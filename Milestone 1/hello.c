/*
 * hello.c - Milestone 1 for Threaded Programming Milestones
 * Purpose: Demonstrates basic pthread_create and pthread_join usage.
 * Notes:
 *  - The parent thread prints "Hello Parent" after creating the child.
 *  - The parent then waits (joins) for the child to finish before exiting.
 *  - Print order between parent/child may vary due to scheduling, but the
 *    process will not exit until the child thread has finished.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

/* Thread start routine: prints a message then exits. */
static void *child_routine(void *arg);

int main(void)
{
    pthread_t child_thread;
    /* Create child thread that runs child_routine (no arguments). */
    int create_status = pthread_create(&child_thread, NULL, child_routine, NULL);
    if (create_status != 0) {
        fprintf(stderr, "Failed to create child thread: %d\n", create_status);
        return EXIT_FAILURE;
    }

    /* Parent work after spawning the child. */
    printf("Milestone 1 Output:\n");
    printf("Hello Parent\n");

    /* Wait for the child to finish before exiting main (synchronization). */
    int join_status = pthread_join(child_thread, NULL);
    if (join_status != 0) {
        fprintf(stderr, "Failed to join child thread: %d\n", join_status);
        return EXIT_FAILURE;
    }

    return 0;
}

static void *child_routine(void *arg)
{
    (void)arg; /* No argument is used for this simple example. */
    printf("Hello Child\n");
    return NULL;
}
