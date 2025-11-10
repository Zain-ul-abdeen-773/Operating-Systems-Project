/*
 * lock.c - Milestone 2 for Threaded Programming Milestones
 * Purpose: Demonstrates mutex + condition variable handoff from parent to child.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define BUFFER_SIZE 256

/* Shared data protected by mutex and coordinated via condition variable. */
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
        /* EOF or read error: treat as no input. */
        fprintf(stderr, "No input received (EOF or error).\n");
        pthread_mutex_lock(&state.mutex);
        state.buffer_ready = -1; /* sentinel for no usable data */
        pthread_mutex_unlock(&state.mutex);
        pthread_cond_broadcast(&state.cond);
    } else {
        /* Strip trailing newline. */
        size_t raw_len = strcspn(state.buffer, "\n");
        state.buffer[raw_len] = '\0';
        /* Trim leading/trailing whitespace to detect empty logical input. */
        char *start = state.buffer;
        while (*start && isspace((unsigned char)*start)) {
            start++;
        }
        char *end = start + strlen(start);
        while (end > start && isspace((unsigned char)*(end - 1))) {
            *(--end) = '\0';

        }
        if (*start == '\0') {
            /* User pressed Enter or entered only whitespace: treat as no input. */
            fprintf(stderr, "Blank input detected. Nothing to share with child.\n");
            pthread_mutex_lock(&state.mutex);
            state.buffer_ready = -1; /* child will just exit */
            pthread_mutex_unlock(&state.mutex);
            pthread_cond_broadcast(&state.cond);
        } else {
            /* If trimming moved the start pointer, shift content to beginning. */
            if (start != state.buffer) {
                memmove(state.buffer, start, strlen(start) + 1);
            }
            /* Signal the child that input is available. */
            pthread_mutex_lock(&state.mutex);
            state.buffer_ready = 1;
            pthread_mutex_unlock(&state.mutex);
            pthread_cond_signal(&state.cond);
        }
    }

    status = pthread_join(child_thread, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join child thread: %d\n", status);
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return EXIT_FAILURE;
    }

    printf("\nParent: Press Enter to exit.\n");
    int ch = getchar();
    (void)ch;

    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.mutex);

    return 0;
}

/* Child waits until the parent indicates the buffer is ready. */
static void *child_routine(void *arg)
{
    struct shared_state *state = (struct shared_state *)arg;

    int status = pthread_mutex_lock(&state->mutex);
    if (status != 0) {
        fprintf(stderr, "Child failed to lock mutex: %d\n", status);
        return NULL;
    }

    while (state->buffer_ready == 0) {
        /* Guarded wait to handle spurious wakeups and race-free checks. */
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
