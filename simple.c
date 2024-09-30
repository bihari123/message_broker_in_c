/*
 *Message queues allow processes to exchange messages - discrete chunks of data.
 They're more structured than pipes and support bidirectional communication.



Use message queues when:

You need to pass discrete messages rather than streams of data.
You want bidirectional communication between unrelated processes.
You need to prioritize messages.
You want persistence of messages even if the receiving process is not running.

Best scenarios:

Task distribution systems where work items can be prioritized.
Event-driven architectures where different types of events need to be handled
differently. Systems where the producer and consumer might not always be running
simultaneously.

Limitations:

More complex to set up compared to pipes.
May have size limitations for individual messages.
 * */
#include <fcntl.h>
#include <mqueue.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define QUEUE_NAME "/test_queue"
#define MAX_SIZE 1024
#define MSG_STOP "exit"

int main() {
  mqd_t mq;
  struct mq_attr attr;
  char buffer[MAX_SIZE + 1];
  int must_stop = 0;

  /* initialize the queue attributes */
  attr.mq_flags = 0;
  attr.mq_maxmsg = 10;
  attr.mq_msgsize = MAX_SIZE;
  attr.mq_curmsgs = 0;

  /* create the message queue */
  mq = mq_open(QUEUE_NAME, O_CREAT | O_RDWR, 0644, &attr);
  if (mq == (mqd_t)-1) {
    perror("mq_open");
    exit(1);
  }

  /* fork to create a child process */
  pid_t pid = fork();

  if (pid < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    /* Child process: write to the queue */
    const char *messages[] = {"Hello", "World", "This",  "Is",
                              "A",     "Test",  MSG_STOP};
    for (int i = 0; i < sizeof(messages) / sizeof(messages[0]); i++) {
      if (mq_send(mq, messages[i], strlen(messages[i]) + 1, 0) == -1) {
        perror("mq_send");
      }
      printf("Sent: %s\n", messages[i]);
      sleep(1); // Wait a bit between messages
    }
    exit(0);
  } else {
    /* Parent process: read from the queue */
    do {
      ssize_t bytes_read;

      /* receive the message */
      bytes_read = mq_receive(mq, buffer, MAX_SIZE, NULL);

      if (bytes_read >= 0) {
        buffer[bytes_read] = '\0';
        if (!strncmp(buffer, MSG_STOP, strlen(MSG_STOP)))
          must_stop = 1;
        else
          printf("Received: %s\n", buffer);
      } else {
        perror("mq_receive");
      }
    } while (!must_stop);

    /* wait for child to finish */
    wait(NULL);

    /* cleanup */
    mq_close(mq);
    mq_unlink(QUEUE_NAME);
  }

  return 0;
}
