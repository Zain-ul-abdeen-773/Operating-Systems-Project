/*
 * test_eventseq.c - Milestone 4 integrated tool
 * Implements steps 1-10 using:
 *  - read_random_byte() from ringbuf.c (Step 1)
 *  - ringbuf_t with rb_init/rb_destroy and rb_put/rb_get (Steps 2-3)
 *  - sequencer_t with sequencer_ticket (Step 4)
 *  - eventcnt_t with read/advance/await (Step 5)
 *  - Producer/consumer synchronized by sequencer + event counters (Step 6)
 *  - pthreads used in place of dispatch_async for portability (Step 7)
 *  - Command line: -b buffer_size, -f initial_fill (Steps 8)
 *  - Interactive loop: print N, exit (Steps 9-10)
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

#define HISTORY_INITIAL_CAPACITY 32

typedef struct state {
    ringbuf_t buffer;             /* bounded queue of bytes */
    sequencer_t sequencer;        /* assigns increasing tickets to items */
    eventcnt_t produced_counter;  /* counts produced items */
    eventcnt_t consumed_counter;  /* counts consumed items */

    pthread_mutex_t history_lock; /* protects history */
    uint8_t *history;             /* consumed values, in order */
    uint64_t *history_tickets;    /* corresponding sequencer tickets */
    size_t history_size;
    size_t history_capacity;

    pthread_mutex_t stop_lock;    /* protects stop flag */
    int stop_requested;
} state_t;

/* --- Utilities ---------------------------------------------------------- */

/* Portable microsecond sleep using nanosleep (handles EINTR). */
static void sleep_us(unsigned int usec)
{
    struct timespec ts;
    ts.tv_sec = usec / 1000000u;
    ts.tv_nsec = (long)(usec % 1000000u) * 1000L;
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
        /* retry with remaining time */
    }
}

static void die(const char *msg, int err)
{
    if (err) fprintf(stderr, "%s: %d\n", msg, err);
    else fprintf(stderr, "%s\n", msg);
    exit(EXIT_FAILURE);
}

/* --- Demo helpers ------------------------------------------------------- */

static void *demo_event_worker(void *arg)
{
    eventcnt_t *ec = (eventcnt_t *)arg;
    /* Simulate work then advance a few times to show progress. */
    for (int i = 0; i < 3; ++i) {
        sleep_us(15000);
        eventcnt_advance(ec, 1);
        printf("Worker advanced counter to %" PRIu64 "\n", eventcnt_read(ec));
    }
    return NULL;
}

static void run_demo(void)
{
    /* Sequencer demo */
    printf("=== Sequencer Demo ===\n");
    sequencer_t seq;
    if (sequencer_init(&seq, 0) != 0) die("sequencer_init failed", 0);
    for (int i = 0; i < 5; ++i) {
        uint64_t ticket = sequencer_ticket(&seq);
        uint8_t sample = 0;
        if (read_random_byte(&sample) != 0) sample = (uint8_t)(100 + i); /* fallback */
        printf("Ticket %" PRIu64 " -> %u\n", ticket, sample);
    }
    sequencer_destroy(&seq);

    /* Event counter demo */
    printf("=== Event Counter Demo ===\n");
    eventcnt_t ec;
    if (eventcnt_init(&ec, 0) != 0) die("eventcnt init failed", 0);
    pthread_t w;
    if (pthread_create(&w, NULL, demo_event_worker, &ec) != 0) die("pthread_create demo worker", 0);
    pthread_join(w, NULL);
    eventcnt_destroy(&ec);

    /* Ring buffer demo */
    printf("=== Ring Buffer Demo ===\n");
    ringbuf_t rb;
    if (rb_init(&rb, 4) != 0) die("rb_init demo failed", 0);
    for (uint8_t v = 1; v <= 3; ++v) {
        errno = 0;
        rb_put(&rb, v);
        printf("Producer queued value %u\n", v);
    }
    for (int i = 0; i < 3; ++i) {
        errno = 0;
        uint8_t got = rb_get(&rb);
        printf("Consumer processed value %u\n", got);
    }
    rb_destroy(&rb);
}

static void history_append(state_t *st, uint8_t value, uint64_t ticket)
{
    int rc = pthread_mutex_lock(&st->history_lock);
    if (rc != 0) die("history lock failed", rc);
    if (st->history_size == st->history_capacity) {
        size_t new_cap = st->history_capacity ? st->history_capacity * 2 : HISTORY_INITIAL_CAPACITY;
        uint8_t *p = (uint8_t *)realloc(st->history, new_cap);
        if (!p) {
            pthread_mutex_unlock(&st->history_lock);
            die("history realloc failed", 0);
        }
        uint64_t *t = (uint64_t *)realloc(st->history_tickets, new_cap * sizeof(uint64_t));
        if (!t) {
            /* keep structures consistent on failure */
            pthread_mutex_unlock(&st->history_lock);
            die("history tickets realloc failed", 0);
        }
        st->history = p;
        st->history_tickets = t;
        st->history_capacity = new_cap;
    }
    st->history[st->history_size++] = value;
    st->history_tickets[st->history_size - 1] = ticket;
    rc = pthread_mutex_unlock(&st->history_lock);
    if (rc != 0) die("history unlock failed", rc);
}

static void request_stop(state_t *st)
{
    int rc = pthread_mutex_lock(&st->stop_lock);
    if (rc != 0) die("stop lock failed", rc);
    st->stop_requested = 1;
    rc = pthread_mutex_unlock(&st->stop_lock);
    if (rc != 0) die("stop unlock failed", rc);

    /* Wake blocked producers/consumers. */
    rb_close(&st->buffer);
    uint64_t p = eventcnt_read(&st->produced_counter);
    eventcnt_advance(&st->produced_counter, UINT64_MAX - p);
    uint64_t c = eventcnt_read(&st->consumed_counter);
    eventcnt_advance(&st->consumed_counter, UINT64_MAX - c);
}

static int should_stop(state_t *st)
{
    int v = 0;
    int rc = pthread_mutex_lock(&st->stop_lock);
    if (rc != 0) die("stop lock failed", rc);
    v = st->stop_requested;
    rc = pthread_mutex_unlock(&st->stop_lock);
    if (rc != 0) die("stop unlock failed", rc);
    return v;
}

/* --- Threads ------------------------------------------------------------ */

static void *producer_thread(void *arg)
{
    state_t *st = (state_t *)arg;
    while (!should_stop(st)) {
        uint8_t value = 0;
        int rs = read_random_byte(&value); /* Step 1 */
        if (rs != 0) {
            fprintf(stderr, "producer: read_random_byte failed: %d\n", rs);
            sleep_us(100000);
            continue;
        }
        uint64_t ticket = sequencer_ticket(&st->sequencer); /* Step 4 */
        (void)ticket; /* ticket can be logged if desired */
        errno = 0;
        rb_put(&st->buffer, value); /* Steps 2-3 */
        if (errno == ECANCELED && rb_is_closed(&st->buffer)) break;
        eventcnt_advance(&st->produced_counter, 1); /* Step 5 */
    }
    return NULL;
}

static void *consumer_thread(void *arg)
{
    state_t *st = (state_t *)arg;
    uint64_t next_ticket = 0;
    while (1) {
        if (should_stop(st) && rb_is_closed(&st->buffer)) break;
        /* Ensure at least next_ticket+1 items produced before attempting get. */
        eventcnt_await(&st->produced_counter, next_ticket + 1); /* Step 6 */
        errno = 0;
        uint8_t value = rb_get(&st->buffer);
        if (errno == ECANCELED && rb_is_closed(&st->buffer)) break;
        history_append(st, value, next_ticket);
        eventcnt_advance(&st->consumed_counter, 1);
        ++next_ticket;
    }
    return NULL;
}

/* --- Initialization / teardown ----------------------------------------- */

static void state_destroy(state_t *st)
{
    rb_destroy(&st->buffer);
    sequencer_destroy(&st->sequencer);
    eventcnt_destroy(&st->produced_counter);
    eventcnt_destroy(&st->consumed_counter);
    pthread_mutex_destroy(&st->history_lock);
    pthread_mutex_destroy(&st->stop_lock);
    free(st->history);
    free(st->history_tickets);
}

static void state_init(state_t *st, size_t buf_size, size_t prefill)
{
    memset(st, 0, sizeof(*st));
    int rc = rb_init(&st->buffer, buf_size);
    if (rc != 0) die("rb_init failed", rc);
    rc = sequencer_init(&st->sequencer, 0);
    if (rc != 0) die("sequencer_init failed", rc);
    rc = eventcnt_init(&st->produced_counter, 0);
    if (rc != 0) die("produced counter init failed", rc);
    rc = eventcnt_init(&st->consumed_counter, 0);
    if (rc != 0) die("consumed counter init failed", rc);
    rc = pthread_mutex_init(&st->history_lock, NULL);
    if (rc != 0) die("history mutex init failed", rc);
    rc = pthread_mutex_init(&st->stop_lock, NULL);
    if (rc != 0) die("stop mutex init failed", rc);

    /* Prefill buffer with random values (up to capacity). */
    if (prefill > buf_size) {
        fprintf(stderr, "Initial fill %zu > capacity %zu; clamping.\n", prefill, buf_size);
        prefill = buf_size;
    }
    for (size_t i = 0; i < prefill; ++i) {
        uint8_t value = 0;
        int rs = read_random_byte(&value);
        if (rs != 0) {
            fprintf(stderr, "prefill: read_random_byte failed: %d\n", rs);
            break;
        }
        (void)sequencer_ticket(&st->sequencer);
        errno = 0;
        rb_put(&st->buffer, value);
        if (errno == ECANCELED && rb_is_closed(&st->buffer)) break;
        eventcnt_advance(&st->produced_counter, 1);
    }
}

/* --- Main (CLI + interactive loop) ------------------------------------- */

static void usage(const char *prog)
{
    fprintf(stderr, "Usage: %s [-b buffer_size] [-f initial_fill] [-p print_count] [-d demo]\n", prog);
    fprintf(stderr, "No arguments: interactive mode (print N | stats | exit).\n");
    fprintf(stderr, "-d demo: run component demo then exit.\n");
}

int main(int argc, char **argv)
{
    size_t buffer_size = 10; /* default */
    size_t initial_fill = 0;
    size_t print_mode_count = 0; /* if >0, run non-interactive and print that many items */
    int demo_mode = 0;

    /* No arguments -> interactive loop by spec (Steps 9-10). */
    int opt;
    while ((opt = getopt(argc, argv, "b:f:p:d")) != -1) {
        switch (opt) {
        case 'b': {
            long long v = atoll(optarg);
            if (v <= 0) die("buffer size must be positive", 0);
            buffer_size = (size_t)v;
            break;
        }
        case 'f': {
            long long v = atoll(optarg);
            if (v < 0) die("initial fill must be non-negative", 0);
            initial_fill = (size_t)v;
            break;
        }
        case 'p': {
            long long v = atoll(optarg);
            if (v < 0) die("print count must be non-negative", 0);
            print_mode_count = (size_t)v;
            break;
        }
        case 'd': {
            demo_mode = 1;
            break;
        }
        default:
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (demo_mode) {
        run_demo();
        return 0;
    }


    state_t st;
    state_init(&st, buffer_size, initial_fill); /* Steps 2-3, 8 */

    pthread_t prod, cons;
    int rc = pthread_create(&prod, NULL, producer_thread, &st); /* Step 7 */
    if (rc != 0) die("pthread_create producer failed", rc);
    rc = pthread_create(&cons, NULL, consumer_thread, &st);     /* Step 7 */
    if (rc != 0) die("pthread_create consumer failed", rc);

    if (print_mode_count > 0) {
        /* Non-interactive direct output mode */
        eventcnt_await(&st.consumed_counter, print_mode_count);
        int lk = pthread_mutex_lock(&st.history_lock);
        if (lk != 0) die("history lock failed", lk);
        size_t limit = print_mode_count <= st.history_size ? print_mode_count : st.history_size;
        for (size_t i = 0; i < limit; ++i) {
            /* Print ticket and value side-by-side */
            printf("%" PRIu64 " %u\n", st.history_tickets[i], st.history[i]);
        }
        lk = pthread_mutex_unlock(&st.history_lock);
        if (lk != 0) die("history unlock failed", lk);
        request_stop(&st);
    } else {
        /* Interactive loop (Step 9) */
        printf("Commands: print N | stats | exit\n");
        size_t print_cursor = 0; /* next history index to display */
        char line[256];
        while (1) {
            printf("> ");
            if (fgets(line, sizeof(line), stdin) == NULL) {
                /* EOF: exit cleanly */
                request_stop(&st);
                break;
            }
            if (strncmp(line, "exit", 4) == 0) {
                request_stop(&st); /* Step 10 */
                break;
            }
            if (strncmp(line, "stats", 5) == 0) {
                uint64_t prod = eventcnt_read(&st.produced_counter);
                uint64_t cons = eventcnt_read(&st.consumed_counter);
                printf("produced=%" PRIu64 ", consumed=%" PRIu64 ", in_flight=%" PRIu64 "\n",
                       prod, cons, (prod >= cons ? prod - cons : 0));
                continue;
            }
            size_t requested = 0;
            if (sscanf(line, "print %zu", &requested) == 1) {
                if (requested == 0) {
                    printf("Nothing to print.\n");
                    continue;
                }
                size_t target = print_cursor + requested;
                /* Wait until at least 'target' items have been consumed. */
                eventcnt_await(&st.consumed_counter, target); /* Step 6/9 */
                int lk2 = pthread_mutex_lock(&st.history_lock);
                if (lk2 != 0) die("history lock failed", lk2);
                while (print_cursor < target && print_cursor < st.history_size) {
                    printf("[History] index=%zu ticket=%" PRIu64 " value=%u\n",
                           print_cursor, st.history_tickets[print_cursor], st.history[print_cursor]);
                    ++print_cursor;
                }
                lk2 = pthread_mutex_unlock(&st.history_lock);
                if (lk2 != 0) die("history unlock failed", lk2);
                continue;
            }
            printf("Unknown command. Try: print N | exit\n");
        }
    }

    /* Join and cleanup (Step 10) */
    rc = pthread_join(prod, NULL);
    if (rc != 0) fprintf(stderr, "join producer failed: %d\n", rc);
    rc = pthread_join(cons, NULL);
    if (rc != 0) fprintf(stderr, "join consumer failed: %d\n", rc);
    state_destroy(&st);
    return 0;
}
