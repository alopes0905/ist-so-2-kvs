#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <semaphore.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include "src/common/protocol.h"
#include "src/common/io.h"
#include "subscriptions.h"
#include "src/common/constants.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

typedef struct {
    int active;
    char req_pipe_path[MAX_STRING_SIZE];
    char resp_pipe_path[MAX_STRING_SIZE];
    char notif_pipe_path[MAX_STRING_SIZE];
    int req_fd;
    int resp_fd;
    int notif_fd;
} SessionRequest;

SessionRequest session_buffer[MAX_SESSION_COUNT];
SessionRequest wait_buffer[MAX_SESSION_COUNT];
int buffer_out = 0;

sem_t empty_slots;
sem_t full_slots;
pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;
char *register_fifo_path = NULL;

volatile sig_atomic_t sigusr1_received = 0;

void handle_sigusr1() {
  sigusr1_received = 1;
}

int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

void *host_task() {
    int fifo_fd = open(register_fifo_path, O_RDONLY);
    if (fifo_fd == -1) {
        perror("Failed to open register FIFO");
        return NULL;
    }

    char buffer[MAX_WRITE_SIZE];
    while (1) {
      if (sigusr1_received) {
        handle_signal();
        
        for (int i = 0; i < MAX_SESSION_COUNT; i++) {
          if (session_buffer[i].active) {
            pthread_mutex_lock(&buffer_mutex);
            if (session_buffer[i].resp_fd != -1) {
                close(session_buffer[i].resp_fd);
                session_buffer[i].resp_fd = -1;
            }
            if (session_buffer[i].notif_fd != -1) {
                char response[MAX_STRING_SIZE] = "-1|kill";
                write(session_buffer[i].notif_fd, response, sizeof(response));
                close(session_buffer[i].notif_fd);
                session_buffer[i].notif_fd = -1;
            }
            session_buffer[i].active = 0;
            pthread_mutex_unlock(&buffer_mutex);
          }
        }
        sigusr1_received = 0;
      }

      ssize_t bytes_read = read(fifo_fd, buffer, sizeof(buffer));
      if (bytes_read > 0) {
        int index = 0;
        sem_wait(&empty_slots);
        pthread_mutex_lock(&buffer_mutex);
        sem_getvalue(&full_slots, &index);


        SessionRequest request;
        
        sscanf(buffer + 1, "|%[^|]|%[^|]|%[^|]", request.req_pipe_path, request.resp_pipe_path, request.notif_pipe_path);


        request.req_fd = open(request.req_pipe_path, O_RDONLY);
        request.resp_fd = open(request.resp_pipe_path, O_WRONLY);
        request.notif_fd = open(request.notif_pipe_path, O_WRONLY);

        
        wait_buffer[index] = request;

        pthread_mutex_unlock(&buffer_mutex);
        sem_post(&full_slots);
      }
    }
    close(fifo_fd);
    return NULL;
}

int sessions_receiver(SessionRequest request) {

  char response[2] = {OP_CODE_CONNECT, 0};
  
  if (write(request.resp_fd, response, sizeof(response)) == -1) {
      perror("Failed to write to response pipe");
  }

  if (request.req_fd == -1) {
      perror("Failed to open request pipe");
  }

  char buffer[MAX_WRITE_SIZE];
  char key[MAX_STRING_SIZE];
  while (request.active) {
    
    ssize_t bytes_read = read(request.req_fd, buffer, sizeof(buffer));
    if (bytes_read > 0) {
      int opcode = buffer[0];
      switch (opcode) {
        case OP_CODE_SUBSCRIBE: {
          sscanf(buffer + 1, "|%[^|]", key);
          response[0] = OP_CODE_SUBSCRIBE;
          if (kvs_key_check(key)) {
            response[1] = 1;
            add_subscription(key, request.notif_fd);
          } 
          else {
            response[1] = 0;
          }

          if (request.resp_fd != -1) {
            write(request.resp_fd, response, sizeof(response));
          }
          break;
        }
        case OP_CODE_UNSUBSCRIBE: {
          sscanf(buffer + 1, "|%[^|]", key);
          int subscription_exists = remove_subscription(key, request.notif_fd);

          if (request.resp_fd != -1) {
            response[0] = OP_CODE_UNSUBSCRIBE;
            response[1] = subscription_exists ? 0 : 1;
            write(request.resp_fd, response, sizeof(response));

          }
          break;
        }
        case OP_CODE_DISCONNECT: {
          if (request.resp_fd != -1) {
            response[0] = OP_CODE_DISCONNECT;
            response[1] = 0;
            write(request.resp_fd, response, sizeof(response));
            close(request.resp_fd);
            close(request.req_fd);
            close(request.notif_fd);
          }
          return 1;
        }
        default:
          break;
      }
    } else {
      return 1;
    }
  }
  sem_post(&empty_slots);
  return 1;
} 

void *manager_task() {
  int index;
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  while (1) {
    sem_wait(&full_slots);
    pthread_mutex_lock(&buffer_mutex);
    sem_getvalue(&full_slots, &index);

    SessionRequest request = wait_buffer[index];

    pthread_mutex_unlock(&buffer_mutex);
    sem_post(&empty_slots);

    int session_index = -1;
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      if (!session_buffer[i].active) {
        session_index = i;
        break;
      }
    }
    if (session_index != -1) {
      strncpy(session_buffer[session_index].req_pipe_path, request.req_pipe_path, MAX_PIPE_PATH_LENGTH);
      strncpy(session_buffer[session_index].resp_pipe_path, request.resp_pipe_path, MAX_PIPE_PATH_LENGTH);
      strncpy(session_buffer[session_index].notif_pipe_path, request.notif_pipe_path, MAX_PIPE_PATH_LENGTH);
      session_buffer[session_index].req_fd = request.req_fd;
      session_buffer[session_index].resp_fd = request.resp_fd;
      session_buffer[session_index].notif_fd = request.notif_fd;
      session_buffer[session_index].active = 1;

      sessions_receiver(session_buffer[session_index]);
      session_buffer[session_index].active = 0;
    }
  }
  return NULL;
}

void dispatch_threads(DIR *dir) {
    pthread_t *threads = malloc(max_threads * sizeof(pthread_t));
    
    if (threads == NULL) {
        fprintf(stderr, "Failed to allocate memory for threads\n");
        return;
    }

    struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};
    for (size_t i = 0; i < max_threads; i++) {
        if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) != 0) {
            fprintf(stderr, "Failed to create thread %zu\n", i);
            pthread_mutex_destroy(&thread_data.directory_mutex);
            free(threads);
            return;
        }
    }

    pthread_t host_thread;
    pthread_t manager_threads[MAX_SESSION_COUNT];

    sem_init(&empty_slots, 0, MAX_SESSION_COUNT);
    sem_init(&full_slots, 0, 0);
    

    if (pthread_create(&host_thread, NULL, host_task, NULL) != 0) {
        fprintf(stderr, "Failed to create host thread\n");
        return;
    }

    for (size_t i = 0; i < MAX_SESSION_COUNT; i++) {
        if (pthread_create(&manager_threads[i], NULL, manager_task, NULL) != 0) {
          fprintf(stderr, "Failed to create manager thread %zu\n", i);
          return;
        }
    }

    pthread_join(host_thread, NULL);
    for (size_t i = 0; i < MAX_SESSION_COUNT; i++) {
        pthread_join(manager_threads[i], NULL);
    }

    for (unsigned int i = 0; i < max_threads; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            fprintf(stderr, "Failed to join thread %u\n", i);
            pthread_mutex_destroy(&thread_data.directory_mutex);
            free(threads);
            return;
        }
    }
    if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
        fprintf(stderr, "Failed to destroy directory_mutex\n");
    }
    free(threads);

    sem_destroy(&empty_slots);
    sem_destroy(&full_slots);
}



int main(int argc, char **argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups> \n");
    return 1;
  }

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = handle_sigusr1;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sigaction(SIGUSR1, &sa, NULL);

  jobs_directory = argv[1];
  unlink(argv[4]);
  register_fifo_path = argv[4];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  if (mkfifo(register_fifo_path, 0777) < 0) {
    write_str(STDERR_FILENO, "Failed to create register fifo\n");
    return 1;
  }
  
  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
