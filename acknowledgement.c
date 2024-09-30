#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_NAME "/test_queue"
#define MAX_SIZE 1024
#define MSG_STOP "exit"
#define MSG_ACK "ack"
#define NUM_GROUPS 3
#define NUM_CONSUMERS_PER_GROUP 2
#define NUM_CONSUMERS (NUM_GROUPS * NUM_CONSUMERS_PER_GROUP)
#define MSGS_PER_PRODUCER 10
#define MAX_GROUP_NAME 20
#define MAX_RETRIES 5
#define RETRY_DELAY_US 100000 // 100 ms

const char *GROUP_NAMES[NUM_GROUPS] = {"analytics", "logging", "notifications"};

typedef struct {
  char group[MAX_GROUP_NAME];
  char text[MAX_SIZE - MAX_GROUP_NAME];
} Message;

int send_with_retry(mqd_t mq, const char *msg_ptr, size_t msg_len,
                    unsigned int msg_prio) {
  int retries = 0;
  while (retries < MAX_RETRIES) {
    if (mq_send(mq, msg_ptr, msg_len, msg_prio) != -1) {
      return 0; // Success
    }
    if (errno != EAGAIN) {
      perror("mq_send");
      return -1; // Unrecoverable error
    }
    usleep(RETRY_DELAY_US);
    retries++;
  }
  fprintf(stderr, "Failed to send message after %d retries\n", MAX_RETRIES);
  return -1;
}

void producer(int group_index, mqd_t mq) {
  Message msg;
  srand(time(NULL) ^ (group_index << 16));

  strncpy(msg.group, GROUP_NAMES[group_index], MAX_GROUP_NAME);
  for (int i = 0; i < MSGS_PER_PRODUCER; i++) {
    snprintf(msg.text, sizeof(msg.text), "Message from producer %s: %d",
             msg.group, i);
    if (send_with_retry(mq, (char *)&msg, sizeof(Message), 0) == -1) {
      fprintf(stderr, "Producer %s failed to send message\n", msg.group);
    }
    usleep(rand() % 1000000); // Sleep up to 1 second
  }
}

void consumer(int id, const char *group, int read_fd, int write_fd) {
  Message msg;
  Message ack_msg;
  int must_stop = 0;

  printf("Consumer %d started (Group %s)\n", id, group);

  while (!must_stop) {
    ssize_t bytes_read = read(read_fd, &msg, sizeof(Message));

    if (bytes_read > 0) {
      if (!strncmp(msg.text, MSG_STOP, strlen(MSG_STOP))) {
        must_stop = 1;
        printf("Consumer %d (Group %s) received stop message\n", id, group);
      } else {
        printf("Consumer %d (Group %s) received: %s\n", id, group, msg.text);
      }

      // Send acknowledgement
      strncpy(ack_msg.group, group, MAX_GROUP_NAME);
      strncpy(ack_msg.text, MSG_ACK, sizeof(ack_msg.text));
      if (write(write_fd, &ack_msg, sizeof(Message)) == -1) {
        perror("write ack message");
      }
    } else if (bytes_read == 0) {
      printf("Consumer %d (Group %s) pipe closed\n", id, group);
      break;
    } else {
      perror("read");
      break;
    }
  }

  close(read_fd);
  close(write_fd);
}

void dispatcher(mqd_t mq,
                int consumer_read_pipes[NUM_GROUPS][NUM_CONSUMERS_PER_GROUP],
                int consumer_write_pipes[NUM_GROUPS][NUM_CONSUMERS_PER_GROUP]) {
  Message msg;
  Message ack_msg;
  int must_stop = 0;
  int current_consumer[NUM_GROUPS] = {
      0}; // Track the current consumer for each group
  fd_set read_fds;
  int max_fd = -1;

  // Find the maximum file descriptor for select()
  for (int i = 0; i < NUM_GROUPS; i++) {
    for (int j = 0; j < NUM_CONSUMERS_PER_GROUP; j++) {
      if (consumer_read_pipes[i][j] > max_fd) {
        max_fd = consumer_read_pipes[i][j];
      }
    }
  }
  // Set stdin to non-blocking mode
  int flags = fcntl(STDIN_FILENO, F_GETFL, 0);
  fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK);

  while (!must_stop) {
    ssize_t bytes_read = mq_receive(mq, (char *)&msg, sizeof(Message), NULL);

    if (bytes_read >= 0) {
      if (!strncmp(msg.text, MSG_STOP, strlen(MSG_STOP))) {
        must_stop = 1;
        printf("Dispatcher received stop message\n");
      } else {
        int group_index = -1;
        for (int i = 0; i < NUM_GROUPS; i++) {
          if (strcmp(msg.group, GROUP_NAMES[i]) == 0) {
            group_index = i;
            break;
          }
        }

        if (group_index != -1) {
          // Send message to the current consumer in the group
          if (write(consumer_write_pipes[group_index]
                                        [current_consumer[group_index]],
                    &msg, sizeof(Message)) == -1) {
            perror("write");
          }

          // Wait for acknowledgement
          FD_ZERO(&read_fds);
          FD_SET(
              consumer_read_pipes[group_index][current_consumer[group_index]],
              &read_fds);

          struct timeval timeout;
          timeout.tv_sec = 5; // 5 seconds timeout
          timeout.tv_usec = 0;

          int ready = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);
          if (ready == -1) {
            perror("select");
          } else if (ready == 0) {
            printf("Timeout waiting for acknowledgement from consumer %d in "
                   "group %s\n",
                   current_consumer[group_index], GROUP_NAMES[group_index]);
            usleep(500000);
          } else {
            if (read(consumer_read_pipes[group_index]
                                        [current_consumer[group_index]],
                     &ack_msg, sizeof(Message)) > 0) {
              if (!strncmp(ack_msg.text, MSG_ACK, strlen(MSG_ACK))) {
                printf(
                    "Received acknowledgement from consumer %d in group %s\n",
                    current_consumer[group_index], GROUP_NAMES[group_index]);
              }
            }
          }

          // Move to the next consumer in the group (round-robin)
          current_consumer[group_index] =
              (current_consumer[group_index] + 1) % NUM_CONSUMERS_PER_GROUP;
        }
      }
    } else if (errno != EAGAIN) {
      perror("mq_receive");
      break;
    }
  }

  // Send stop message to all consumers
  printf("Dispatcher sending stop messages to all consumers\n");
  Message stop_msg;
  strncpy(stop_msg.text, MSG_STOP, sizeof(stop_msg.text));
  for (int i = 0; i < NUM_GROUPS; i++) {
    strncpy(stop_msg.group, GROUP_NAMES[i], MAX_GROUP_NAME);
    for (int j = 0; j < NUM_CONSUMERS_PER_GROUP; j++) {
      if (write(consumer_write_pipes[i][j], &stop_msg, sizeof(Message)) == -1) {
        perror("write stop message");
      }
      // Wait for acknowledgement of stop message
      FD_ZERO(&read_fds);
      FD_SET(consumer_read_pipes[i][j], &read_fds);

      struct timeval timeout;
      timeout.tv_sec = 5; // 5 seconds timeout
      timeout.tv_usec = 0;

      int ready = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);
      if (ready == -1) {
        perror("select");
      } else if (ready == 0) {
        printf("Timeout waiting for stop acknowledgement from consumer %d in "
               "group %s\n",
               j, GROUP_NAMES[i]);
      } else {
        if (read(consumer_read_pipes[i][j], &ack_msg, sizeof(Message)) > 0) {
          if (!strncmp(ack_msg.text, MSG_ACK, strlen(MSG_ACK))) {
            printf(
                "Received stop acknowledgement from consumer %d in group %s\n",
                j, GROUP_NAMES[i]);
          }
        }
      }
    }
  }

  // Close all consumer pipes
  for (int i = 0; i < NUM_GROUPS; i++) {
    for (int j = 0; j < NUM_CONSUMERS_PER_GROUP; j++) {
      close(consumer_read_pipes[i][j]);
      close(consumer_write_pipes[i][j]);
    }
  }
}
mqd_t getMessageQueue() {
  mqd_t mq;
  struct mq_attr attr;

  attr.mq_flags = 0;
  attr.mq_maxmsg = 10;
  attr.mq_msgsize = sizeof(Message);
  attr.mq_curmsgs = 0;

  return mq_open(QUEUE_NAME, O_CREAT | O_RDWR, 0644, &attr);
}
int main() {
  int dispatcher_to_consumer[NUM_GROUPS][NUM_CONSUMERS_PER_GROUP][2];
  int consumer_to_dispatcher[NUM_GROUPS][NUM_CONSUMERS_PER_GROUP][2];
  pid_t pids[NUM_GROUPS + NUM_CONSUMERS + 1]; // +1 for dispatcher

  mqd_t mq = getMessageQueue();
  if (mq == (mqd_t)-1) {
    perror("mq_open");
    exit(1);
  }

  // Create pipes for communication between dispatcher and consumers
  for (int i = 0; i < NUM_GROUPS; i++) {
    for (int j = 0; j < NUM_CONSUMERS_PER_GROUP; j++) {
      if (pipe(dispatcher_to_consumer[i][j]) == -1 ||
          pipe(consumer_to_dispatcher[i][j]) == -1) {
        perror("pipe");
        exit(1);
      }
    }
  }

  // Create consumer processes
  int consumer_count = 0;
  for (int i = 0; i < NUM_GROUPS; i++) {
    for (int j = 0; j < NUM_CONSUMERS_PER_GROUP; j++) {
      pids[consumer_count] = fork();
      if (pids[consumer_count] < 0) {
        perror("fork");
        exit(1);
      } else if (pids[consumer_count] == 0) {
        // Child (consumer) process
        close(dispatcher_to_consumer[i][j][1]); // Close write end
        close(consumer_to_dispatcher[i][j][0]); // Close read end

        // Close all other pipes
        for (int x = 0; x < NUM_GROUPS; x++) {
          for (int y = 0; y < NUM_CONSUMERS_PER_GROUP; y++) {
            if (x != i || y != j) {
              close(dispatcher_to_consumer[x][y][0]);
              close(dispatcher_to_consumer[x][y][1]);
              close(consumer_to_dispatcher[x][y][0]);
              close(consumer_to_dispatcher[x][y][1]);
            }
          }
        }

        consumer(consumer_count, GROUP_NAMES[i],
                 dispatcher_to_consumer[i][j][0],
                 consumer_to_dispatcher[i][j][1]);
        exit(0);
      }
      close(dispatcher_to_consumer[i][j][0]); // Close read end in parent
      close(consumer_to_dispatcher[i][j][1]); // Close write end in parent
      consumer_count++;
    }
  }

  // Create dispatcher process
  pids[NUM_CONSUMERS] = fork();
  if (pids[NUM_CONSUMERS] < 0) {
    perror("fork");
    exit(1);
  } else if (pids[NUM_CONSUMERS] == 0) {
    // Child (dispatcher) process
    int dispatcher_read_pipes[NUM_GROUPS][NUM_CONSUMERS_PER_GROUP];
    int dispatcher_write_pipes[NUM_GROUPS][NUM_CONSUMERS_PER_GROUP];
    for (int i = 0; i < NUM_GROUPS; i++) {
      for (int j = 0; j < NUM_CONSUMERS_PER_GROUP; j++) {
        dispatcher_read_pipes[i][j] = consumer_to_dispatcher[i][j][0];
        dispatcher_write_pipes[i][j] = dispatcher_to_consumer[i][j][1];
      }
    }
    dispatcher(mq, dispatcher_read_pipes, dispatcher_write_pipes);
    exit(0);
  }

  // Create producer processes
  for (int i = 0; i < NUM_GROUPS; i++) {
    pids[NUM_CONSUMERS + 1 + i] = fork();
    if (pids[NUM_CONSUMERS + 1 + i] < 0) {
      perror("fork");
      exit(1);
    } else if (pids[NUM_CONSUMERS + 1 + i] == 0) {
      // Child (producer) process
      producer(i, mq);
      exit(0);
    }
  }

  printf("dispatcher to parent process closed\n");
  // Close all pipes in the parent process
  for (int i = 0; i < NUM_GROUPS; i++) {
    for (int j = 0; j < NUM_CONSUMERS_PER_GROUP; j++) {
      close(dispatcher_to_consumer[i][j][0]);
      close(dispatcher_to_consumer[i][j][1]);
      close(consumer_to_dispatcher[i][j][0]);
      close(consumer_to_dispatcher[i][j][1]);
    }
  }

  // Wait for all producers to finish
  for (int i = 0; i < NUM_GROUPS; i++) {
    waitpid(pids[NUM_CONSUMERS + 1 + i], NULL, 0);
  }

  // Send a single stop message to the queue
  Message stop_msg;
  strncpy(stop_msg.text, MSG_STOP, sizeof(stop_msg.text));
  strncpy(stop_msg.group, "STOP", MAX_GROUP_NAME);
  if (send_with_retry(mq, (char *)&stop_msg, sizeof(Message), 0) == -1) {
    fprintf(stderr, "Failed to send stop message\n");
  }

  // Wait for dispatcher to finish
  waitpid(pids[NUM_CONSUMERS], NULL, 0);

  // Wait for all consumers to finish
  for (int i = 0; i < NUM_CONSUMERS; i++) {
    waitpid(pids[i], NULL, 0);
  }

  // Cleanup
  mq_close(mq);
  mq_unlink(QUEUE_NAME);

  return 0;
}
