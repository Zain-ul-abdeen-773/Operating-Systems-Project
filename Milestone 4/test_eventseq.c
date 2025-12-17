/*
 * test_eventseq.c - Milestone 4 Integrated Tool
 *
 * Implements steps 1–10:
 *  1. read_random_byte() from /dev/random (ringbuf.c)
 *  2–3. ringbuf_t with rb_init/rb_destroy and rb_put/rb_get
 *  4. sequencer_t with sequencer_ticket()
 *  5. eventcnt_t with read(), advance(), await()
 *  6. Sequencer + Event Counter synchronization
 *  7. Producer/Consumer via pthreads (dispatch_async alternative)
 *  8. Command-line args: -b (buffer size), -f (initial fill) ,p (no of lines)
 *  9. Interactive loop: print N | stats | exit
 * 10. Clean exit with resource cleanup
 */

#include "eventcnt.h"
#include "ringbuf.h"
#include "sequencer.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

/* --------------------------------------------------------------------------
 *                              Data Structures
 * -------------------------------------------------------------------------- */

#define HISTORY_INITIAL_CAPACITY 32

typedef struct state {
    ringbuf_t buffer;             /* bounded queue of bytes */
    sequencer_t sequencer;        /* assigns increasing tickets */
    eventcnt_t produced_counter;  /* tracks produced items */
    eventcnt_t consumed_counter;  /* tracks consumed items */

    pthread_mutex_t history_lock; /* protects history */
    uint8_t  *history;            /* consumed values */
    uint64_t *history_tickets;    /* sequencer tickets */
    size_t    history_size;
    size_t    history_capacity;

    pthread_mutex_t stop_lock;    /* protects stop flag */
    int stop_requested;
} state_t;

/*
 * state_t invariants:
 * - `buffer` is the shared bounded queue: producers call `rb_put()` and
 *   consumers call `rb_get()`. The ring buffer internally serializes access.
 * - `sequencer` issues monotonically increasing tickets to order produced
 *   elements; tickets are stored alongside history entries for traceability.
 * - `produced_counter` and `consumed_counter` are event counters used to
 *   notify interested threads when production/consumption progresses.
 * - `history` arrays are protected by `history_lock` and record consumed
 *   values and their associated tickets for later inspection/printing.
 * - `stop_lock` serializes access to `stop_requested`; use `request_stop()`
 *   and `should_stop()` helpers to safely set/read the flag.
 */

/* --------------------------------------------------------------------------
 *                              Utility Helpers
 * -------------------------------------------------------------------------- */

/* Portable microsecond sleep using nanosleep */
static void sleep_us(unsigned int usec) {
    struct timespec ts = {
        .tv_sec  = usec / 1000000u,
        .tv_nsec = (long)(usec % 1000000u) * 1000L
    };
    /* Use nanosleep in a loop to handle interrupted sleeps (EINTR). The
     * remaining time is written back into `ts` by nanosleep when interrupted.
     */
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR);
}

static void die(const char *msg, int err) {
    if (err) fprintf(stderr, "%s: %d\n", msg, err);
    else     fprintf(stderr, "%s\n", msg);
    exit(EXIT_FAILURE);
}

/*
 * `die()` is a convenience for fatal errors during initialization. In
 * production code prefer returning error codes and performing cleanup, but
 * for demos a short-circuit exit simplifies error handling.
 */

/* --------------------------------------------------------------------------
 *                              Demo Helpers
 * -------------------------------------------------------------------------- */

static void *demo_event_worker(void *arg) {
    eventcnt_t *ec = (eventcnt_t *)arg;
    for (int i = 0; i < 3; ++i) {
        sleep_us(15000);
        eventcnt_advance(ec, 1);
        printf("Worker advanced counter to %" PRIu64 "\n", eventcnt_read(ec));
    }
    return NULL;
}

static void run_demo(void) {
    printf("=== Sequencer Demo ===\n");
    sequencer_t seq;
    if (sequencer_init(&seq, 0) != 0) die("sequencer_init failed", 0);
    for (int i = 0; i < 5; ++i) {
        uint64_t ticket = sequencer_ticket(&seq);
        uint8_t sample = 0;
        if (read_random_byte(&sample) != 0)
            sample = (uint8_t)(100 + i);
        printf("Ticket %" PRIu64 " -> %u\n", ticket, sample);
    }
    sequencer_destroy(&seq);

    printf("=== Event Counter Demo ===\n");
    eventcnt_t ec;
    if (eventcnt_init(&ec, 0) != 0) die("eventcnt init failed", 0);
    pthread_t w;
    if (pthread_create(&w, NULL, demo_event_worker, &ec) != 0)
        die("pthread_create demo worker", 0);
    pthread_join(w, NULL);
    eventcnt_destroy(&ec);

    printf("=== Ring Buffer Demo ===\n");
    ringbuf_t rb;
    if (rb_init(&rb, 4) != 0) die("rb_init demo failed", 0);
    for (uint8_t v = 1; v <= 3; ++v) {
        rb_put(&rb, v);
        printf("Producer queued value %u\n", v);
    }
    for (int i = 0; i < 3; ++i) {
        uint8_t got = rb_get(&rb);
        printf("Consumer processed value %u\n", got);
    }
    rb_destroy(&rb);
}

/*
 * `run_demo()` exercises the three main primitives (sequencer, eventcnt,
 * ringbuf) in isolation so you can validate correctness before running the
 * integrated producer/consumer system.
 */

/* --------------------------------------------------------------------------
 *                              History Management
 * -------------------------------------------------------------------------- */

static void history_append(state_t *st, uint8_t value, uint64_t ticket) {
    pthread_mutex_lock(&st->history_lock);
    if (st->history_size == st->history_capacity) {
        size_t new_cap = st->history_capacity ? st->history_capacity * 2
                                              : HISTORY_INITIAL_CAPACITY;
        /* Resize both arrays; history stores bytes, tickets stores uint64_t.
         * Note: error handling uses `die()` for simplicity; production code
         * should handle allocation failures more gracefully. */
        st->history = realloc(st->history, new_cap);
        st->history_tickets = realloc(st->history_tickets, new_cap * sizeof(uint64_t));
        if (!st->history || !st->history_tickets)
            die("history realloc failed", 0);
        st->history_capacity = new_cap;
    }
    st->history[st->history_size] = value;
    st->history_tickets[st->history_size] = ticket;
    st->history_size++;
    pthread_mutex_unlock(&st->history_lock);
}

/*
 * Notes on history management:
 * - `history_lock` must be held when accessing `history` arrays to
 *   prevent races between the consumer appending and the main thread
 *   printing values.
 * - The ticket associated with each value preserves ordering assigned by the
 *   `sequencer` and is useful for debugging and validation.
 */

/* --------------------------------------------------------------------------
 *                              Stop Control
 * -------------------------------------------------------------------------- */

static void request_stop(state_t *st) {
    pthread_mutex_lock(&st->stop_lock);
    st->stop_requested = 1;
    pthread_mutex_unlock(&st->stop_lock);

    rb_close(&st->buffer);
    /*
     * To ensure all waiters are unblocked, advance event counters by a large
     * delta (difference to UINT64_MAX). This effectively wakes any threads
     * waiting on `eventcnt_await` without requiring knowledge of current
     * values. `rb_close()` also unblocks ring-buffer waiters.
     */
    eventcnt_advance(&st->produced_counter, UINT64_MAX - eventcnt_read(&st->produced_counter));
    eventcnt_advance(&st->consumed_counter, UINT64_MAX - eventcnt_read(&st->consumed_counter));
}

static int should_stop(state_t *st) {
    pthread_mutex_lock(&st->stop_lock);
    int v = st->stop_requested;
    pthread_mutex_unlock(&st->stop_lock);
    return v;
}

/*
 * `request_stop()` and `should_stop()` provide a thread-safe stop protocol
 * used by both producer and consumer loops to check for shutdown. The stop
 * request is coarse-grained (single flag) because more complex coordinated
 * shutdown isn't required for this demo.
 */

/* --------------------------------------------------------------------------
 *                              Threads
 * -------------------------------------------------------------------------- */

static void *producer_thread(void *arg) {
    state_t *st = arg;
    while (!should_stop(st)) {
        uint8_t value = 0;
        if (read_random_byte(&value) != 0) {
            sleep_us(100000);
            continue;
        }
        sequencer_ticket(&st->sequencer);
        rb_put(&st->buffer, value);
        eventcnt_advance(&st->produced_counter, 1);
    }
    return NULL;
}

/*
 * Producer loop summary:
 * 1) read a random byte (may block or fail briefly) 2) get a ticket from
 *    the sequencer (ordering) 3) put the value into the ring buffer 4)
 *    advance the produced counter to notify consumers.
 * The order of operations matters: the ticket should be acquired before
 * publishing the value to ensure tickets correlate with buffer positions.
 */

static void *consumer_thread(void *arg) {
    state_t *st = arg;
    uint64_t next_ticket = 0;
    while (1) {
        if (should_stop(st) && rb_is_closed(&st->buffer))
            break;
        eventcnt_await(&st->produced_counter, next_ticket + 1);
        uint8_t value = rb_get(&st->buffer);
        history_append(st, value, next_ticket);
        eventcnt_advance(&st->consumed_counter, 1);
        next_ticket++;
    }
    return NULL;
}

/*
 * Consumer loop summary:
 * - Wait until `produced_counter >= next_ticket+1` meaning the producer has
 *   published (or prefilled) the required item. `eventcnt_await()` may block
 *   until the condition is true. - Then `rb_get()` retrieves the value (may
 *   block if buffer empty) and the consumer records it in history. - The
 *   consumed counter is advanced to inform any print/waiting logic.
 */

/* --------------------------------------------------------------------------
 *                              Initialization / Cleanup
 * -------------------------------------------------------------------------- */

static void state_destroy(state_t *st) {
    rb_destroy(&st->buffer);
    sequencer_destroy(&st->sequencer);
    eventcnt_destroy(&st->produced_counter);
    eventcnt_destroy(&st->consumed_counter);
    pthread_mutex_destroy(&st->history_lock);
    pthread_mutex_destroy(&st->stop_lock);
    free(st->history);
    free(st->history_tickets);
}

/*
 * Cleanup notes: all primitives are destroyed in the reverse order from
 * initialization to avoid use-after-free during teardown. The ring buffer
 * and event counters must be destroyed after threads have joined.
 */

static void state_init(state_t *st, size_t buf_size, size_t prefill) {
    memset(st, 0, sizeof(*st));
    if (rb_init(&st->buffer, buf_size) != 0) die("rb_init failed", 0);
    if (sequencer_init(&st->sequencer, 0) != 0) die("sequencer_init failed", 0);
    if (eventcnt_init(&st->produced_counter, 0) != 0) die("produced counter init failed", 0);
    if (eventcnt_init(&st->consumed_counter, 0) != 0) die("consumed counter init failed", 0);
    pthread_mutex_init(&st->history_lock, NULL);
    pthread_mutex_init(&st->stop_lock, NULL);

    if (prefill > buf_size) prefill = buf_size;
    for (size_t i = 0; i < prefill; ++i) {
        uint8_t value = 0;
        if (read_random_byte(&value) != 0) break;
        sequencer_ticket(&st->sequencer);
        rb_put(&st->buffer, value);
        eventcnt_advance(&st->produced_counter, 1);
    }
}

/*
 * Initialization notes:
 * - `state_init()` creates all primitives and prepares optional prefill
 *   values; prefill follows the same code path as live production so tests
 *   exercise the full synchronization stack.
 */

/* --------------------------------------------------------------------------
 *                              Main / CLI
 * -------------------------------------------------------------------------- */

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s [-b buffer_size] [-f initial_fill] [-p print_count] [-d demo] [-I]\n", prog);
    fprintf(stderr, "No arguments → interactive mode (print N | stats | exit)\n");
}

int main(int argc, char **argv) {
    size_t buffer_size = 10;
    size_t initial_fill = 0;
    size_t print_mode_count = 0;
    int demo_mode = 0;
    int force_prompts = 0;

    int opt;
    while ((opt = getopt(argc, argv, "b:f:p:dI")) != -1) {
        switch (opt) {
            case 'b': buffer_size = strtoull(optarg, NULL, 10); break;
            case 'f': initial_fill = strtoull(optarg, NULL, 10); break;
            case 'p': print_mode_count = strtoull(optarg, NULL, 10); break;
            case 'd': demo_mode = 1; break;
            case 'I': force_prompts = 1; break;
            default:  usage(argv[0]); return EXIT_FAILURE;
        }
    }

    if (demo_mode) { run_demo(); return 0; }

    if (print_mode_count == 0 && (isatty(STDIN_FILENO) || force_prompts)) {
        printf("Enter buffer size (default %zu): ", buffer_size);
        char line[64];
        if (fgets(line, sizeof(line), stdin)) {
            long long v; if (sscanf(line, "%lld", &v) == 1 && v > 0) buffer_size = (size_t)v;
        }
        printf("Enter initial fill (default %zu): ", initial_fill);
        if (fgets(line, sizeof(line), stdin)) {
            long long v; if (sscanf(line, "%lld", &v) == 1 && v >= 0) initial_fill = (size_t)v;
        }
    }

    state_t st;
    state_init(&st, buffer_size, initial_fill);

    pthread_t prod, cons;
    pthread_create(&prod, NULL, producer_thread, &st);
    pthread_create(&cons, NULL, consumer_thread, &st);

    if (print_mode_count > 0) {
        eventcnt_await(&st.consumed_counter, print_mode_count);
        pthread_mutex_lock(&st.history_lock);
        size_t limit = print_mode_count < st.history_size ? print_mode_count : st.history_size;
        for (size_t i = 0; i < limit; ++i)
            printf("%u\n", st.history[i]);
        pthread_mutex_unlock(&st.history_lock);
        request_stop(&st);
    } else {
        printf("Commands: print N | stats | exit\n");
        size_t print_cursor = 0;
        char line[256];
        while (1) {
            printf("> ");
            if (!fgets(line, sizeof(line), stdin)) { request_stop(&st); break; }
            if (strncmp(line, "exit", 4) == 0) { request_stop(&st); break; }
            if (strncmp(line, "stats", 5) == 0) {
                uint64_t prod = eventcnt_read(&st.produced_counter);
                uint64_t cons = eventcnt_read(&st.consumed_counter);
                printf("produced=%" PRIu64 ", consumed=%" PRIu64 ", in_flight=%" PRIu64 "\n",
                       prod, cons, prod >= cons ? prod - cons : 0);
                continue;
            }
            size_t n = 0;
            if (sscanf(line, "print %zu", &n) == 1) {
                size_t target = print_cursor + n;
                eventcnt_await(&st.consumed_counter, target);
                pthread_mutex_lock(&st.history_lock);
                while (print_cursor < target && print_cursor < st.history_size)
                    printf("Consumer Received [%zu] %u\n", print_cursor, st.history[print_cursor++]);
                pthread_mutex_unlock(&st.history_lock);
                continue;
            }
            printf("Unknown command. Try: print N | stats | exit\n");
        }
    }

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);
    state_destroy(&st);
    return 0;
}
