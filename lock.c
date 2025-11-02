/*
 * lock.c - Milestone 2 for Threaded Programming Milestones
 * Purpose: Demonstrates mutex and condition variable synchronization.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUFFER_SIZE 256

struct shared_state {
    char buffer[BUFFER_SIZE];
    int buffer_ready;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

static void *child_routine(void *arg);

int main(void)
{
    struct shared_state state;
    state.buffer_ready = 0;

    int status = pthread_mutex_init(&state.mutex, NULL);
    if (status != 0) {
        fprintf(stderr, "Mutex init failed: %d\n", status);
        return EXIT_FAILURE;
    }

    status = pthread_cond_init(&state.cond, NULL);
    if (status != 0) {
        fprintf(stderr, "Condition variable init failed: %d\n", status);
        pthread_mutex_destroy(&state.mutex);
        return EXIT_FAILURE;
    }

    pthread_t child_thread;
    status = pthread_create(&child_thread, NULL, child_routine, &state);
    if (status != 0) {
        fprintf(stderr, "Failed to create child thread: %d\n", status);
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return EXIT_FAILURE;
    }

    printf("Parent: Enter a line to share with the child thread:\n");
    if (fgets(state.buffer, BUFFER_SIZE, stdin) == NULL) {
        fprintf(stderr, "Failed to read input.\n");
        pthread_mutex_lock(&state.mutex);
        state.buffer_ready = -1;
        pthread_mutex_unlock(&state.mutex);
        pthread_cond_broadcast(&state.cond);
    } else {
        pthread_mutex_lock(&state.mutex);
        state.buffer_ready = 1;
        pthread_mutex_unlock(&state.mutex);
        pthread_cond_signal(&state.cond);
    }

    status = pthread_join(child_thread, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join child thread: %d\n", status);
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return EXIT_FAILURE;
    }

    printf("Parent: Press Enter to exit.\n");
    int ch = getchar();
    (void)ch;

    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.mutex);

    return 0;
}

static void *child_routine(void *arg)
{
    struct shared_state *state = (struct shared_state *)arg;

    int status = pthread_mutex_lock(&state->mutex);
    if (status != 0) {
        fprintf(stderr, "Child failed to lock mutex: %d\n", status);
        return NULL;
    }

    while (state->buffer_ready == 0) {
            /* Guarded wait ensures the child observes a consistent buffer state without races. */
        status = pthread_cond_wait(&state->cond, &state->mutex);
        if (status != 0) {
            fprintf(stderr, "Child failed to wait on condition: %d\n", status);
            pthread_mutex_unlock(&state->mutex);
            return NULL;
        }
    }

    if (state->buffer_ready == -1) {
        pthread_mutex_unlock(&state->mutex);
        return NULL;
    }

    printf("Child received: %s", state->buffer);

    pthread_mutex_unlock(&state->mutex);
    return NULL;
}
