#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

int notif_pipe;
pthread_t notif_thread;
int signal_catched = 0;

void *notification_handler() {
    char buffer[2 * MAX_STRING_SIZE + 2];

    while (1) {
        ssize_t bytes_read = read(notif_pipe, buffer, sizeof(buffer));
        if (bytes_read > 0) {
          char key[MAX_STRING_SIZE];
          char value[MAX_STRING_SIZE];
          sscanf(buffer, "%[^|]|%s", key, value);
          if (strcmp(value, "kill") == 0 && strcmp(key, "-1") == 0) {
            signal_catched = 1;
            if (kvs_kill()) {
              pthread_exit(NULL);
            }
            return (void *)1;
          }
          if (strcmp(value, "DELETED") == 0) {
            printf("(%s,DELETED)\n", key);
          } else {
            printf("(%s,%s)\n", key, value);
          }
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req_SO";
  char resp_pipe_path[256] = "/tmp/resp_SO";
  char notif_pipe_path[256] = "/tmp/notif_SO";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  if (kvs_connect(req_pipe_path, resp_pipe_path, argv[2], notif_pipe_path, &notif_pipe)) {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }
  if (pthread_create(&notif_thread, NULL, notification_handler, NULL) != 0) {
      perror("Failed to create notification thread");
      return 1;
  }
  while (1) {
    if (signal_catched) {
      printf("Disconnecting...\n");
      return 0;
    }
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_subscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_unsubscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
          printf("Waiting...\n");
          delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          break;
        }
        return 0;
    }
  }
  return 0;
}
