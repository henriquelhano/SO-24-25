#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/server/kvs.h"

void *thread_notif(void* arg) {
  int* notif_fd = (int*)arg;
  
  char notification[2*MAX_STRING_SIZE];
  char key[MAX_STRING_SIZE];
  char value[MAX_STRING_SIZE];

  while(1) {
    ssize_t bytes_read = read(*notif_fd, notification, 2*MAX_STRING_SIZE);
    if (bytes_read == 0) {
      // bytes_read == 0 indica EOF
      fprintf(stderr, "pipe closed\n");
      return NULL;
    } else if (bytes_read == -1) {
      // bytes_read == -1 indica erro
      fprintf(stderr, "read failed: %s\n", strerror(errno));
      exit(EXIT_FAILURE);
    }

    strncpy(key, notification+2, MAX_STRING_SIZE);
    strncpy(value, notification+2, MAX_STRING_SIZE);
    printf("(%s,%s)\n", key, value);

  }
}


int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  char* register_pipe_path = argv[2];

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  // Permite ao cliente receber notificações
  int notif_pipe_fd;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  // TODO open pipes
  if (kvs_connect(req_pipe_path, resp_pipe_path, notif_pipe_path, register_pipe_path, &notif_pipe_fd) != 0) {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }

  pthread_t notif_thread;
  pthread_create(&notif_thread, NULL, thread_notif, &notif_pipe_fd);

  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if (kvs_disconnect() != 0) {
        printf("bananas\n");
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }
      // TODO: end notifications thread
      printf("Disconnected from server\n");
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
      // input should end in a disconnect, or it will loop here forever
      break;
    }
  }
}
