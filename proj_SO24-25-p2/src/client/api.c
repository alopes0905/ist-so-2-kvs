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

int req_fd;
int resp_fd;
int notif_fd;

void cleanup_pipes() {
  unlink(reqPipePath);
  unlink(respPipePath);
  unlink(notifPipePath);
}


int create_fifo(const char *fifo_path) {
    if (mkfifo(fifo_path, 0777) < 0) {
        perror("Failed to create FIFO");
        return -1;
    }
    return 0;
}

void await_response() {
  char buffer[256];
  while (1) {
    ssize_t bytes_read = read(resp_fd, buffer, sizeof(buffer));
    if (bytes_read > 0) {
      buffer[bytes_read] = '\0';
      int opcode = buffer[0];
      int result = buffer[1];
      switch (opcode) {
        case OP_CODE_CONNECT:
          printf("Server returned %d for operation: connect\n", result);
          return;

        case OP_CODE_DISCONNECT:
          printf("Server returned %d for operation: disconnect\n", result);
          return;

        case OP_CODE_SUBSCRIBE:
          printf("Server returned %d for operation: subscribe\n", result);
          return;

        case OP_CODE_UNSUBSCRIBE:
          printf("Server returned %d for operation: unsubscribe\n", result);
          return;
        
        default:
          continue;
      }
    }
  }
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {
  // create pipes and connect
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);         
  reqPipePath = req_pipe_path;
  respPipePath = resp_pipe_path;
  notifPipePath = notif_pipe_path;
  serverPipePath = server_pipe_path;

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

  close(server_fd);

  req_fd = open(reqPipePath, O_WRONLY);
  if (req_fd == -1) {
    perror("Failed to open server pipe");
    cleanup_pipes();
    return 1;
  }

  resp_fd = open(resp_pipe_path, O_RDONLY);
  if (resp_fd == -1) {
    perror("Failed to open server pipe");
    cleanup_pipes();
    return 1;
  }

  *notif_pipe = open(notif_pipe_path, O_RDONLY);
  if (*notif_pipe == -1) {
    perror("Failed to open server pipe");
    cleanup_pipes();
    return 1;
  }
  notif_fd = *notif_pipe;
  await_response();

  return 0;
}

int kvs_disconnect(void) {
  char message[2];
  snprintf(message, sizeof(message), "%c", OP_CODE_DISCONNECT);
  if (write(req_fd, message, sizeof(message)) == -1) {
    perror("Failed to write to server pipe");
    close(req_fd);
    cleanup_pipes();
    return 1;
  }
  
  await_response();
  close(req_fd);
  close(resp_fd); 
  close(notif_fd); 
  cleanup_pipes();
  return 0;
}

int kvs_subscribe(const char *key) {

  char message[MAX_STRING_SIZE + 3];
  snprintf(message, sizeof(message), "%c|%s", OP_CODE_SUBSCRIBE, key);
  if (write(req_fd, message, sizeof(message)) == -1) {
    perror("Failed to write to server pipe");
    close(req_fd);
    cleanup_pipes();
    return 1;
  }
  await_response();
  return 0;
}

int kvs_unsubscribe(const char *key) {
  char message[MAX_STRING_SIZE + 3];
  snprintf(message, sizeof(message), "%c|%s", OP_CODE_UNSUBSCRIBE, key);
  if (write(req_fd, message, sizeof(message)) == -1) {
    perror("Failed to write to server pipe");
    close(req_fd);
    cleanup_pipes();
    return 1;
  }
  
  await_response();
  return 0;
}

int kvs_kill(void) {
  close(req_fd);
  close(resp_fd); 
  close(notif_fd); 
  cleanup_pipes();
  return 1;
}