/*
 * hello.c - Milestone 1 for Threaded Programming Milestones
 * Purpose: Demonstrates basic pthread_create and pthread_join usage.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

static void *child_routine(void *arg);

int main(void)
{
    pthread_t child_thread;
    int create_status = pthread_create(&child_thread, NULL, child_routine, NULL);
    if (create_status != 0) {
        fprintf(stderr, "Failed to create child thread: %d\n", create_status);
        return EXIT_FAILURE;
    }

    printf("Hello Parent\n");

    int join_status = pthread_join(child_thread, NULL);
    if (join_status != 0) {
        fprintf(stderr, "Failed to join child thread: %d\n", join_status);
        return EXIT_FAILURE;
    }

    return 0;
}

static void *child_routine(void *arg)
{
    (void)arg;
    printf("Hello Child\n");
    return NULL;
}
