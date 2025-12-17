/*
 * lock.c - Milestone 2 for Threaded Programming Milestones
 * Purpose: Demonstrates mutex + condition variable handoff from parent to child.
 *
 * Inline comments added to explain behavior and synchronization invariants.
 */

#include <pthread.h>   // pthread_create, pthread_join, mutexes, condvars
#include <stdio.h>     // printf, fprintf, fgets, getchar
#include <stdlib.h>    // EXIT_FAILURE, EXIT_SUCCESS
#include <string.h>    // strlen, memmove, strcspn
#include <ctype.h>     // isspace

#define BUFFER_SIZE 256  // fixed capacity for shared input buffer

/* Shared data protected by mutex and coordinated via condition variable. */
struct shared_state {
    char buffer[BUFFER_SIZE];   // storage for the input string (NUL-terminated)
    int buffer_ready;          // predicate: 0 = not ready, 1 = ready, -1 = sentinel (no data)
    pthread_mutex_t mutex;     // protects buffer and buffer_ready
    pthread_cond_t cond;       // signalled when buffer_ready changes
};

static void *child_routine(void *arg); // child thread function prototype

int main(void)
{
    struct shared_state state;
    state.buffer_ready = 0; // initialize predicate to "not ready"

    /* Initialize mutex with default attributes. */
    int status = pthread_mutex_init(&state.mutex, NULL);
    if (status != 0) {
        fprintf(stderr, "Mutex init failed: %d\n", status);
        return EXIT_FAILURE;
    }

    /* Initialize condition variable with default attributes. */
    status = pthread_cond_init(&state.cond, NULL);
    if (status != 0) {
        fprintf(stderr, "Condition variable init failed: %d\n", status);
        pthread_mutex_destroy(&state.mutex); // cleanup on failure
        return EXIT_FAILURE;
    }

    /* Create child thread, passing pointer to shared state. */
    pthread_t child_thread;
    status = pthread_create(&child_thread, NULL, child_routine, &state);
    if (status != 0) {
        fprintf(stderr, "Failed to create child thread: %d\n", status);
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return EXIT_FAILURE;
    }

    printf("Milestone 2 Output:\n");
    printf("Parent: Enter a line to share with the child thread:\n");

    /* Read a line into state.buffer (bounded by BUFFER_SIZE). */
    if (fgets(state.buffer, BUFFER_SIZE, stdin) == NULL) {
        /* EOF or read error: treat as no input. */
        fprintf(stderr, "No input received (EOF or error).\n");

        /* Update predicate under lock to avoid races. */
        pthread_mutex_lock(&state.mutex);
        state.buffer_ready = -1; /* sentinel for no usable data */
        pthread_mutex_unlock(&state.mutex);

        /* Wake all waiters so they can observe sentinel and exit. */
        pthread_cond_broadcast(&state.cond);
    } else {
        /* Strip trailing newline if present. */
        size_t raw_len = strcspn(state.buffer, "\n");
        state.buffer[raw_len] = '\0';

        /* Trim leading whitespace: advance start past leading spaces. */
        char *start = state.buffer;
        while (*start && isspace((unsigned char)*start)) {
            start++;
        }

        /* Trim trailing whitespace by moving end backward and NUL-terminating. */
        char *end = start + strlen(start);
        while (end > start && isspace((unsigned char)*(end - 1))) {
            *(--end) = '\0';
        }

        /* If trimmed input is empty, treat as no input (sentinel). */
        if (*start == '\0') {
            fprintf(stderr, "Blank input detected. Nothing to share with child.\n");
            pthread_mutex_lock(&state.mutex);
            state.buffer_ready = -1; /* child will just exit */
            pthread_mutex_unlock(&state.mutex);
            pthread_cond_broadcast(&state.cond);
        } else {
            /* If leading whitespace was removed, move trimmed content to buffer start. */
            if (start != state.buffer) {
                memmove(state.buffer, start, strlen(start) + 1); // include NUL
            }

            /* Publish data: set predicate under lock, then signal one waiter. */
            pthread_mutex_lock(&state.mutex);
            state.buffer_ready = 1;   // mark data available
            pthread_mutex_unlock(&state.mutex);

            /* Wake a single waiting consumer (child). */
            pthread_cond_signal(&state.cond);
        }
    }

    /* Wait for child to finish before cleaning up resources. */
    status = pthread_join(child_thread, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join child thread: %d\n", status);
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return EXIT_FAILURE;
    }

    /* Allow user to see output before program exits. */
    printf("\nParent: Press Enter to exit.\n");
    int ch = getchar();
    (void)ch; // suppress unused-variable warning

    /* Destroy synchronization primitives (symmetric cleanup). */
    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.mutex);

    return 0;
}

/* Child waits until the parent indicates the buffer is ready. */
static void *child_routine(void *arg)
{
    struct shared_state *state = (struct shared_state *)arg;

    /* Acquire mutex before checking predicate to maintain invariant. */
    int status = pthread_mutex_lock(&state->mutex);
    if (status != 0) {
        fprintf(stderr, "Child failed to lock mutex: %d\n", status);
        return NULL;
    }

    /* Wait while predicate indicates "not ready".
     * Use while-loop to handle spurious wakeups and re-check predicate after wake.
     * pthread_cond_wait atomically releases mutex and blocks; it re-acquires mutex on wake.
     */
    while (state->buffer_ready == 0) {
        status = pthread_cond_wait(&state->cond, &state->mutex);
        if (status != 0) {
            fprintf(stderr, "Child failed to wait on condition: %d\n", status);
            pthread_mutex_unlock(&state->mutex);
            return NULL;
        }
    }

    /* If parent set sentinel (-1), unlock and exit gracefully. */
    if (state->buffer_ready == -1) {
        pthread_mutex_unlock(&state->mutex);
        return NULL;
    }

    /* At this point buffer_ready == 1 and buffer contents are valid; print while holding mutex
     * to ensure consistent view (optional for printing, but demonstrates safe access).
     */
    printf("Child received: %s", state->buffer);

    /* Release mutex and exit thread. */
    pthread_mutex_unlock(&state->mutex);
    return NULL;
}
