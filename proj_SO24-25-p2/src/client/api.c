#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>


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

void cleanup_pipes() {
  unlink(reqPipePath);
  unlink(respPipePath);
  unlink(notifPipePath);
}

//FIXME ALTERAR LOPES
// Function to create a FIFO and handle reconnection
int create_fifo(const char *fifo_path) {
    if (access(fifo_path, F_OK) == 0) { // Check if FIFO already exists
        if (unlink(fifo_path) == -1) { // Remove the existing FIFO
            perror("Failed to remove existing FIFO");
            return -1;
        }
    }

    // Create the FIFO
    if (mkfifo(fifo_path, 0666) == -1) {
        perror("Failed to create FIFO");
        return -1;
    }
    return 0;
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {
  // create pipes and connect             
  reqPipePath = req_pipe_path;
  respPipePath = resp_pipe_path;
  notifPipePath = notif_pipe_path;

  //FIXME LOPES ALTERAR
  if (create_fifo(req_pipe_path) == -1) {
    unlink(req_pipe_path); // Clean up
      return 1;
  }

  if (create_fifo(resp_pipe_path) == -1) {
      unlink(req_pipe_path); // Clean up previously created FIFOs
      return 1;
  }

  if (create_fifo(notif_pipe_path) == -1) {
      unlink(resp_pipe_path); // Clean up
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
  *notif_pipe = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);
  if (*notif_pipe == -1) {
    perror("Failed to open notification pipe");
    cleanup_pipes();
    return 1;
  }
  return 0;

}

int kvs_disconnect(void) {
  cleanup_pipes(); //FIXME - ERRORS?
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  printf("Subscribing to key %s\n", key);
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  printf("Unsubscribing from key %s\n", key);
  return 0;
}
