/*
 * randprod.c — Milestone 5: 8-bit Random Number Generator
 *
 * Goal: Solve the Producer/Consumer problem for a random number generator using
 *        Sequencers and Event Counters.
 *
 * Mapping to the assignment steps:
 *  1) Implement producer() and consumer() functions
 *     - See producer_thread() and consumer_thread() below.
 *  2) Ensure producer does not overflow and consumer does not underflow
 *     - We use a blocking ring buffer (rb_put / rb_get) that naturally
 *       blocks on full/empty conditions and prevents overflow/underflow.
 *  3) Use Sequencers and Event Counters for synchronization
 *     - Producer calls sequencer_ticket() per item and advances produced_counter.
 *       Consumer waits with eventcnt_await(produced, next_ticket+1) before rb_get
 *       and advances consumed_counter after processing.
 *  4) Create a test program that spawns producer and consumer threads concurrently
 *     - main() creates both threads and joins them on exit.
 *  5) Add command-line parameters for buffer size and fill level
 *     - Flags: -b <size> and -f <fill> (prefill is clamped to capacity).
 *  6) Add a loop to read user input for printing integers from the buffer
 *     - Interactive loop supports: "print N" to print N consumed integers.
 *  7) Implement an exit command to clean up resources and exit the program
 *     - Type "exit" or send EOF to stop; we wake threads, join, and free resources.

 */

#include "eventcnt.h"
#include "ringbuf.h"
#include "sequencer.h"

#include <errno.h>
#include <inttypes.h>
#include <time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

#define HISTORY_INITIAL_CAPACITY 32

/* Shared state structure containing all synchronization primitives and data
 * shared between producer and consumer threads.
 */
struct shared_state {
    ringbuf_t buffer;                    /* Ring buffer for values (blocking queue) */
    sequencer_t sequencer;               /* Monotonic ticket generator */
    eventcnt_t produced_counter;         /* Tracks number of items produced */
    eventcnt_t consumed_counter;         /* Tracks number of items consumed */
    pthread_mutex_t history_lock;        /* Protects history array */
    uint8_t *history;                    /* Array of consumed values */
    size_t history_size;                 /* Current number of items in history */
    size_t history_capacity;             /* Allocated capacity of history */
    pthread_mutex_t state_lock;          /* Protects stop_requested flag */
    int stop_requested;                  /* Signal for threads to stop */
    int verbose;                         /* Flag to control producer/consumer output */
};

typedef struct shared_state shared_state_t;

/* Function prototypes */
static int shared_state_init(shared_state_t *state, size_t buffer_size, size_t initial_fill, int verbose);
static void shared_state_destroy(shared_state_t *state);
static void request_stop(shared_state_t *state);
static int should_stop(shared_state_t *state);
static int append_history(shared_state_t *state, uint8_t value);

static void *producer_thread(void *arg);
static void *consumer_thread(void *arg);

/* Portable microsecond sleep using nanosleep (handles EINTR). */
static void sleep_us(unsigned int usec)
{
    struct timespec ts;
    ts.tv_sec = usec / 1000000u;
    ts.tv_nsec = (long)(usec % 1000000u) * 1000L;
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
        /* retry */
    }
}

static void print_usage(const char *progname)
{
    fprintf(stderr, "Usage: %s [-b buffer_size] [-f initial_fill] [-v]\n", progname);
    fprintf(stderr, "  -v  Verbose: print producer/consumer operations\n");
    fprintf(stderr, "Commands: print N | exit\n");
}

int main(int argc, char **argv)
{
    size_t buffer_size = 10;
    size_t initial_fill = 0;
    int verbose = 0;  /* By default, suppress producer/consumer output */

    int opt;
    while ((opt = getopt(argc, argv, "b:f:v")) != -1) {
        switch (opt) {
        case 'b': {
            char *end = NULL;
            unsigned long long value = strtoull(optarg, &end, 10);
            if (end == optarg || *end != '\0' || value == 0) {
                fprintf(stderr, "Buffer size must be positive.\n");
                return EXIT_FAILURE;
            }
            buffer_size = (size_t)value;
            break;
        }
        case 'f': {
            char *end = NULL;
            unsigned long long value = strtoull(optarg, &end, 10);
            /* treat non-numeric as error and negatives as invalid */
            if (end == optarg || *end != '\0') {
                fprintf(stderr, "Initial fill must be an integer.\n");
                return EXIT_FAILURE;
            }
            /* strtoull can't represent negative; just check overflow to size_t */
            if (value > (unsigned long long)SIZE_MAX) {
                fprintf(stderr, "Initial fill must be non-negative.\n");
                return EXIT_FAILURE;
            }
            initial_fill = (size_t)value;
            break;
        }
        case 'v':
            verbose = 1;  /* Enable verbose output for producer/consumer */
            break;
        default:
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    shared_state_t state;
    if (shared_state_init(&state, buffer_size, initial_fill, verbose) != 0) {
        fprintf(stderr, "Failed to initialize shared state.\n");
        return EXIT_FAILURE;
    }

    pthread_t producer;
    pthread_t consumer;

    int status = pthread_create(&producer, NULL, producer_thread, &state);
    if (status != 0) {
        fprintf(stderr, "Failed to start producer: %d\n", status);
        shared_state_destroy(&state);
        return EXIT_FAILURE;
    }

    status = pthread_create(&consumer, NULL, consumer_thread, &state);
    if (status != 0) {
        fprintf(stderr, "Failed to start consumer: %d\n", status);
        request_stop(&state);
        pthread_join(producer, NULL);
        shared_state_destroy(&state);
        return EXIT_FAILURE;
    }

    /* Interactive command loop: read user commands to print history or exit */
    size_t print_cursor = 0;  /* Tracks how many values have been printed so far */
    char line[256];

    printf("Commands: print N | exit\n");
    while (1) {
        printf("> ");
        fflush(stdout);  /* Ensure prompt appears immediately */
        
        if (fgets(line, sizeof(line), stdin) == NULL) {
            printf("EOF detected. Exiting.\n");
            request_stop(&state);
            break;
        }

        /* Handle exit command */
        if (strncmp(line, "exit", 4) == 0) {
            request_stop(&state);
            break;
        }

        /* Handle print command: print N consumed values from history */
        size_t requested = 0;
        if (sscanf(line, "print %zu", &requested) == 1) {
            if (requested == 0) {
                printf("Nothing to print.\n");
                continue;
            }

            /* Calculate target number of consumed items needed */
            size_t target = print_cursor + requested;
            
            /* Wait until at least 'target' items have been consumed */
            eventcnt_await(&state.consumed_counter, target);

            /* Lock history and print the requested values */
            int lock_status = pthread_mutex_lock(&state.history_lock);
            if (lock_status != 0) {
                fprintf(stderr, "Failed to lock history: %d\n", lock_status);
                continue;
            }

            while (print_cursor < target && print_cursor < state.history_size) {
                printf("[History] index=%zu value=%u\n", print_cursor, state.history[print_cursor]);
                ++print_cursor;
            }

            lock_status = pthread_mutex_unlock(&state.history_lock);
            if (lock_status != 0) {
                fprintf(stderr, "Failed to unlock history: %d\n", lock_status);
            }
            continue;
        }

        printf("Unknown command. Try: print N | exit\n");
    }

    /* Wait for threads to complete */
    status = pthread_join(producer, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join producer: %d\n", status);
    }

    status = pthread_join(consumer, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join consumer: %d\n", status);
    }

    shared_state_destroy(&state);

    return 0;
}

/* Initialize all shared state including ring buffer, sequencer, event counters,
 * mutexes, and history array. Performs initial prefill if requested.
 */
static int shared_state_init(shared_state_t *state, size_t buffer_size, size_t initial_fill, int verbose)
{
    if (state == NULL) {
        return EINVAL;
    }

    memset(state, 0, sizeof(*state));
    state->verbose = verbose;

    int status = rb_init(&state->buffer, buffer_size);
    if (status != 0) {
        return status;
    }

    status = sequencer_init(&state->sequencer, 0);
    if (status != 0) {
        rb_destroy(&state->buffer);
        return status;
    }

    status = eventcnt_init(&state->produced_counter, 0);
    if (status != 0) {
        sequencer_destroy(&state->sequencer);
        rb_destroy(&state->buffer);
        return status;
    }

    status = eventcnt_init(&state->consumed_counter, 0);
    if (status != 0) {
        eventcnt_destroy(&state->produced_counter);
        sequencer_destroy(&state->sequencer);
        rb_destroy(&state->buffer);
        return status;
    }

    status = pthread_mutex_init(&state->history_lock, NULL);
    if (status != 0) {
        eventcnt_destroy(&state->consumed_counter);
        eventcnt_destroy(&state->produced_counter);
        sequencer_destroy(&state->sequencer);
        rb_destroy(&state->buffer);
        return status;
    }

    status = pthread_mutex_init(&state->state_lock, NULL);
    if (status != 0) {
        pthread_mutex_destroy(&state->history_lock);
        eventcnt_destroy(&state->consumed_counter);
        eventcnt_destroy(&state->produced_counter);
        sequencer_destroy(&state->sequencer);
        rb_destroy(&state->buffer);
        return status;
    }

    state->history_capacity = HISTORY_INITIAL_CAPACITY;
    state->history = (uint8_t *)malloc(state->history_capacity);
    if (state->history == NULL) {
        pthread_mutex_destroy(&state->state_lock);
        pthread_mutex_destroy(&state->history_lock);
        eventcnt_destroy(&state->consumed_counter);
        eventcnt_destroy(&state->produced_counter);
        sequencer_destroy(&state->sequencer);
        rb_destroy(&state->buffer);
        return ENOMEM;
    }

    size_t fill_limit = initial_fill;
    if (fill_limit > buffer_size) {
        printf("Initial fill %zu exceeds buffer capacity %zu. Clamping to capacity.\n", fill_limit, buffer_size);
        fill_limit = buffer_size;
    }

    for (size_t i = 0; i < fill_limit; ++i) {
        uint8_t value = 0;
        int read_status = read_random_byte(&value);
        if (read_status != 0) {
            fprintf(stderr, "Prefill read_random_byte failed: %d\n", read_status);
            break;
        }

        uint64_t ticket = sequencer_ticket(&state->sequencer);
        errno = 0;
        rb_put(&state->buffer, value);
        if (errno == ECANCELED && rb_is_closed(&state->buffer)) {
            break;
        }
        eventcnt_advance(&state->produced_counter, 1);
        printf("[Prefill] ticket=%" PRIu64 " value=%u\n", ticket, value);
    }

    return 0;
}

/* Clean up and destroy all resources in shared state. */
static void shared_state_destroy(shared_state_t *state)
{
    if (state == NULL) {
        return;
    }

    rb_destroy(&state->buffer);
    sequencer_destroy(&state->sequencer);
    eventcnt_destroy(&state->produced_counter);
    eventcnt_destroy(&state->consumed_counter);

    int status = pthread_mutex_destroy(&state->history_lock);
    if (status != 0) {
        fprintf(stderr, "shared_state_destroy: history_lock destroy failed: %d\n", status);
    }

    status = pthread_mutex_destroy(&state->state_lock);
    if (status != 0) {
        fprintf(stderr, "shared_state_destroy: state_lock destroy failed: %d\n", status);
    }

    free(state->history);
    state->history = NULL;
    state->history_capacity = 0;
    state->history_size = 0;
}

/* Request threads to stop by setting the stop flag, closing the ring buffer,
 * and advancing event counters to wake any waiting threads.
 */
static void request_stop(shared_state_t *state)
{
    int status = pthread_mutex_lock(&state->state_lock);
    if (status != 0) {
        fprintf(stderr, "request_stop: lock failed: %d\n", status);
        return;
    }
    state->stop_requested = 1;
    status = pthread_mutex_unlock(&state->state_lock);
    if (status != 0) {
        fprintf(stderr, "request_stop: unlock failed: %d\n", status);
    }

    /* Close ring buffer to unblock any waiting rb_put/rb_get calls */
    rb_close(&state->buffer);
    
    /* Advance event counters to maximum to wake any threads waiting on them */
    uint64_t produced_now = eventcnt_read(&state->produced_counter);
    eventcnt_advance(&state->produced_counter, UINT64_MAX - produced_now);
    uint64_t consumed_now = eventcnt_read(&state->consumed_counter);
    eventcnt_advance(&state->consumed_counter, UINT64_MAX - consumed_now);
}

/* Check if stop has been requested (thread-safe). */
static int should_stop(shared_state_t *state)
{
    int value = 0;
    int status = pthread_mutex_lock(&state->state_lock);
    if (status != 0) {
        fprintf(stderr, "should_stop: lock failed: %d\n", status);
        return 1;
    }
    value = state->stop_requested;
    status = pthread_mutex_unlock(&state->state_lock);
    if (status != 0) {
        fprintf(stderr, "should_stop: unlock failed: %d\n", status);
    }
    return value;
}

/* Append a consumed value to the history array. Grows the array dynamically
 * if capacity is exceeded (thread-safe).
 */
static int append_history(shared_state_t *state, uint8_t value)
{
    int status = pthread_mutex_lock(&state->history_lock);
    if (status != 0) {
        fprintf(stderr, "append_history: lock failed: %d\n", status);
        return status;
    }

    /* Grow history array if at capacity (double the size) */
    if (state->history_size == state->history_capacity) {
        size_t new_capacity = state->history_capacity == 0 ? HISTORY_INITIAL_CAPACITY : state->history_capacity * 2;
        uint8_t *resized = (uint8_t *)realloc(state->history, new_capacity);
        if (resized == NULL) {
            fprintf(stderr, "append_history: realloc failed\n");
            pthread_mutex_unlock(&state->history_lock);
            return ENOMEM;
        }
        state->history = resized;
        state->history_capacity = new_capacity;
    }

    state->history[state->history_size++] = value;

    status = pthread_mutex_unlock(&state->history_lock);
    if (status != 0) {
        fprintf(stderr, "append_history: unlock failed: %d\n", status);
        return status;
    }

    return 0;
}

/* Producer thread: continuously generates random 8-bit values, obtains tickets
 * from the sequencer, places values in the ring buffer, and advances the
 * produced event counter. Only prints output when verbose mode is enabled.
 */
static void *producer_thread(void *arg)
{
    shared_state_t *state = (shared_state_t *)arg;

    while (!should_stop(state)) {
        uint8_t value = 0;
        int status = read_random_byte(&value);
        if (status != 0) {
            fprintf(stderr, "[Producer] read_random_byte failed: %d\n", status);
            sleep_us(100000);
            continue;
        }

        uint64_t ticket = sequencer_ticket(&state->sequencer);
        errno = 0;
        rb_put(&state->buffer, value);
        if (errno == ECANCELED && rb_is_closed(&state->buffer)) {
            break;
        }

        eventcnt_advance(&state->produced_counter, 1);
        
        /* Only print producer output in verbose mode to avoid flooding stdout */
        if (state->verbose) {
            printf("[Producer] ticket=%" PRIu64 " value=%u\n", ticket, value);
        }
    }

    if (state->verbose) {
        printf("[Producer] stopping.\n");
    }
    return NULL;
}

/* Consumer thread: waits for produced items, retrieves values from the ring buffer,
 * appends them to history, and advances the consumed event counter. Only prints
 * output when verbose mode is enabled.
 */
static void *consumer_thread(void *arg)
{
    shared_state_t *state = (shared_state_t *)arg;
    uint64_t next_ticket = 0;

    while (1) {
        if (should_stop(state) && rb_is_closed(&state->buffer)) {
            break;
        }

        /* Wait until at least next_ticket+1 items have been produced */
        eventcnt_await(&state->produced_counter, next_ticket + 1);

        errno = 0;
        uint8_t value = rb_get(&state->buffer);
        if (errno == ECANCELED && rb_is_closed(&state->buffer)) {
            break;
        }

        int status = append_history(state, value);
        if (status != 0) {
            fprintf(stderr, "[Consumer] append_history failed: %d\n", status);
        }

        eventcnt_advance(&state->consumed_counter, 1);
        
        /* Only print consumer output in verbose mode to avoid flooding stdout */
        if (state->verbose) {
            printf("[Consumer] ticket=%" PRIu64 " value=%u (total=%zu)\n",
                   next_ticket, value, state->history_size);
        }
        ++next_ticket;
    }

    if (state->verbose) {
        printf("[Consumer] stopping.\n");
    }
    return NULL;
}
