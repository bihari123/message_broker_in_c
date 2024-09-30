#include <fcntl.h>
#include <mqueue.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_NAME "/test_queue"
#define MAX_SIZE 1024
#define MSG_STOP "exit"
#define NUM_PRODUCERS 3
#define NUM_CONSUMERS 2
#define MSGS_PER_PRODUCER 5

void producer(int id) {
  mqd_t mq;
  char buffer[MAX_SIZE];

  mq = mq_open(QUEUE_NAME, O_WRONLY);
  if (mq == (mqd_t)-1) {
    perror("mq_open");
    exit(1);
  }

  srand(time(NULL) ^ (id << 16));

  for (int i = 0; i < MSGS_PER_PRODUCER; i++) {
    snprintf(buffer, MAX_SIZE, "Message from producer %d: %d", id, i);
    if (mq_send(mq, buffer, strlen(buffer) + 1, 0) == -1) {
      perror("mq_send");
    }
    usleep(rand() % 1000000); // Sleep up to 1 second
  }

  mq_close(mq);
  exit(0);
}

void consumer(int id) {
  mqd_t mq;
  char buffer[MAX_SIZE + 1];
  int must_stop = 0;

  mq = mq_open(QUEUE_NAME, O_RDONLY);
  if (mq == (mqd_t)-1) {
    perror("mq_open");
    exit(1);
  }

  do {
    ssize_t bytes_read;

    bytes_read = mq_receive(mq, buffer, MAX_SIZE, NULL);

    if (bytes_read >= 0) {
      buffer[bytes_read] = '\0';
      if (!strncmp(buffer, MSG_STOP, strlen(MSG_STOP))) {
        must_stop = 1;
      } else {
        printf("Consumer %d received: %s\n", id, buffer);
      }
    } else {
      perror("mq_receive");
    }
  } while (!must_stop);

  mq_close(mq);
  exit(0);
}

int main() {
  mqd_t mq;
  struct mq_attr attr;
  pid_t pids[NUM_PRODUCERS + NUM_CONSUMERS];
  int i;

  attr.mq_flags = 0;
  attr.mq_maxmsg = 10;
  attr.mq_msgsize = MAX_SIZE;
  attr.mq_curmsgs = 0;

  mq = mq_open(QUEUE_NAME, O_CREAT | O_RDWR, 0644, &attr);
  if (mq == (mqd_t)-1) {
    perror("mq_open");
    exit(1);
  }

  // Create producer processes
  for (i = 0; i < NUM_PRODUCERS; i++) {
    pids[i] = fork();
    if (pids[i] < 0) {
      perror("fork");
      exit(1);
    } else if (pids[i] == 0) {
      producer(i);
    }
  }

  // Create consumer processes
  for (; i < NUM_PRODUCERS + NUM_CONSUMERS; i++) {
    pids[i] = fork();
    if (pids[i] < 0) {
      perror("fork");
      exit(1);
    } else if (pids[i] == 0) {
      consumer(i - NUM_PRODUCERS);
    }
  }

  // Wait for all producers to finish
  for (i = 0; i < NUM_PRODUCERS; i++) {
    waitpid(pids[i], NULL, 0);
  }

  // Send stop messages to consumers
  for (i = 0; i < NUM_CONSUMERS; i++) {
    if (mq_send(mq, MSG_STOP, strlen(MSG_STOP) + 1, 0) == -1) {
      perror("mq_send");
    }
  }

  // Wait for all consumers to finish
  for (i = NUM_PRODUCERS; i < NUM_PRODUCERS + NUM_CONSUMERS; i++) {
    waitpid(pids[i], NULL, 0);
  }

  // Cleanup
  mq_close(mq);
  mq_unlink(QUEUE_NAME);

  return 0;
}
