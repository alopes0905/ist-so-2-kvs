#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

char const *reqPipePath;
char const *respPipePath;
char const *notifPipePath;
char const *serverPipePath;

int *notifPipe;
pthread_t notif_thread;

void cleanup_pipes() {
  unlink(reqPipePath);
  unlink(respPipePath);
  unlink(notifPipePath);
}

void *notification_handler(void *arg) {
    int notif_pipe = *(int *)arg;
    char buffer[2 * MAX_STRING_SIZE + 2];

    while (1) {
        ssize_t bytes_read = read(notif_pipe, buffer, sizeof(buffer));
        if (bytes_read > 0) {
          char key[MAX_STRING_SIZE];
          char value[MAX_STRING_SIZE];
          sscanf(buffer, "%[^|]|%s", key, value);
          if (strcmp(value, "DELETED") == 0) {
            printf("(%s,DELETED)\n", key);
          } else {
            printf("(%s,%s)\n", key, value);
          }
        }
    }
    return NULL;
}

// Function to create a FIFO and handle reconnection
int create_fifo(const char *fifo_path) {
    if (access(fifo_path, F_OK) == 0) { // Check if FIFO already exists
        if (unlink(fifo_path) == -1) { // Remove the existing FIFO
            perror("Failed to remove existing FIFO");
            return -1;
        }
    }

    // Create the FIFO
    if (mkfifo(fifo_path, 0777) < 0) {
        perror("Failed to create FIFO");
        return -1;
    }
    return 0;
}

void responsePrint(const char *operation, int response) {
  printf("Server returned %d for operation: %s\n", response, operation);
  return;
}

void await_response() {
  int fifo_fd = open(respPipePath, O_RDONLY);

  char buffer[256];
  while (1) {
    ssize_t bytes_read = read(fifo_fd, buffer, sizeof(buffer));
    if (bytes_read > 0) {
      int opcode = buffer[0];
      switch (opcode) {
        case OP_CODE_CONNECT:
          responsePrint("connect", buffer[1]);
          return;

        case OP_CODE_DISCONNECT:
          responsePrint("disconnect", buffer[1]);
          return;

        case OP_CODE_SUBSCRIBE:
          responsePrint("subscribe", buffer[1]);
          return;

        case OP_CODE_UNSUBSCRIBE:
          responsePrint("unsubscribe", buffer[1]);
          return;
        
        default:
          continue;
      }
    }
  }
}
//FIXME - ABRIR TODOS OS PIPES
int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {
  // create pipes and connect             
  reqPipePath = req_pipe_path;
  respPipePath = resp_pipe_path;
  notifPipePath = notif_pipe_path;
  serverPipePath = server_pipe_path;

  //FIXME LOPES ALTERAR
  if (create_fifo(req_pipe_path) == -1) {
    unlink(req_pipe_path);
      return 1;
  }

  if (create_fifo(resp_pipe_path) == -1) {
      unlink(req_pipe_path);
      return 1;
  }

  if (create_fifo(notif_pipe_path) == -1) {
      unlink(resp_pipe_path);
      return 1;
  }

  int server_fd = open(server_pipe_path, O_WRONLY);
  if (server_fd == -1) {
    perror("Failed to open server pipe");
    cleanup_pipes();
    return 1;
  }

  char message[3 * MAX_STRING_SIZE + 2];
  snprintf(message, sizeof(message), "%c|%s|%s|%s", OP_CODE_CONNECT, req_pipe_path, resp_pipe_path, notif_pipe_path);
  if (write(server_fd, message, sizeof(message)) == -1) {
    perror("Failed to write to server pipe");
    close(server_fd);
    cleanup_pipes();
    return 1;
  }

  close(server_fd); //FIXME - CLOSE?
  await_response();

  *notif_pipe = open(notifPipePath, O_RDONLY | O_NONBLOCK);
  if (*notif_pipe == -1) {
    perror("Failed to open notification pipe");
    cleanup_pipes();
    return 1;
  }
  notifPipe = notif_pipe;
  if (pthread_create(&notif_thread, NULL, notification_handler, notifPipe) != 0) {
      perror("Failed to create notification thread");
      cleanup_pipes();
      return 1;
  }
  return 0;
}

int kvs_disconnect(void) {

  int req_fd = open(reqPipePath, O_WRONLY);
  if (req_fd == -1) {
    perror("Failed to open server pipe");
    cleanup_pipes();
    return 1;
  }

  char message[2];
  snprintf(message, sizeof(message), "%c", OP_CODE_DISCONNECT);
  if (write(req_fd, message, sizeof(message)) == -1) {
    perror("Failed to write to server pipe");
    close(req_fd);
    cleanup_pipes();
    return 1;
  }
  
  close(req_fd); //FIXME - CLOSE?
  await_response();
  cleanup_pipes(); //FIXME - ERRORS?
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  int req_fd = open(reqPipePath, O_WRONLY);
  if (req_fd == -1) {
    perror("Failed to open server pipe");
    cleanup_pipes();
    return 1;
  }

  char message[MAX_STRING_SIZE + 3];
  snprintf(message, sizeof(message), "%c|%s", OP_CODE_SUBSCRIBE, key);
  if (write(req_fd, message, sizeof(message)) == -1) {
    perror("Failed to write to server pipe");
    close(req_fd);
    cleanup_pipes();
    return 1;
  }
  
  close(req_fd); //FIXME - CLOSE?
  await_response();
  /*
  int*notif_pipe = open(notifPipePath, O_RDONLY | O_NONBLOCK);
  if (*notif_pipe == -1) {
    perror("Failed to open notification pipe");
    cleanup_pipes();
    return 1;
  }
  */

  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  int req_fd = open(reqPipePath, O_WRONLY);
  if (req_fd == -1) {
    perror("Failed to open server pipe");
    cleanup_pipes();
    return 1;
  }

  char message[MAX_STRING_SIZE + 3];
  snprintf(message, sizeof(message), "%c|%s", OP_CODE_UNSUBSCRIBE, key);
  if (write(req_fd, message, sizeof(message)) == -1) {
    perror("Failed to write to server pipe");
    close(req_fd);
    cleanup_pipes();
    return 1;
  }
  
  close(req_fd); //FIXME - CLOSE?
  await_response();
  return 0;
}
