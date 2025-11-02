/*
 * test_eventseq.c - Milestone 4 integration test for Threaded Programming Milestones
 * Purpose: Demonstrates sequencer, event counter, and ring buffer components.
 */

#include "eventcnt.h"
#include "ringbuf.h"
#include "sequencer.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define EVENT_STEPS 3
#define RB_TEST_ITEMS 6

struct event_thread_args {
    eventcnt_t *counter;
};

struct rb_thread_args {
    ringbuf_t *buffer;
};

static void run_sequencer_demo(void);
static void run_event_counter_demo(void);
static void run_ring_buffer_demo(void);
static void *event_advancer(void *arg);
static void *rb_producer(void *arg);
static void *rb_consumer(void *arg);

int main(void)
{
    run_sequencer_demo();
    run_event_counter_demo();
    run_ring_buffer_demo();
    return 0;
}

static void run_sequencer_demo(void)
{
    printf("=== Sequencer Demo ===\n");
    sequencer_t seq;
    int status = sequencer_init(&seq, 1000);
    if (status != 0) {
        fprintf(stderr, "Failed to init sequencer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < 5; ++i) {
        uint64_t ticket = sequencer_ticket(&seq);
        printf("Ticket %d -> %" PRIu64 "\n", i, ticket);
    }

    sequencer_destroy(&seq);
}

static void run_event_counter_demo(void)
{
    printf("\n=== Event Counter Demo ===\n");
    eventcnt_t counter;
    int status = eventcnt_init(&counter, 0);
    if (status != 0) {
        fprintf(stderr, "Failed to init event counter: %d\n", status);
        exit(EXIT_FAILURE);
    }

    pthread_t worker;
    struct event_thread_args args = { .counter = &counter };
    status = pthread_create(&worker, NULL, event_advancer, &args);
    if (status != 0) {
        fprintf(stderr, "Failed to spawn event advancer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    eventcnt_await(&counter, EVENT_STEPS);
    printf("Main observed counter reach %d\n", EVENT_STEPS);

    status = pthread_join(worker, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join event advancer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    eventcnt_destroy(&counter);
}

static void run_ring_buffer_demo(void)
{
    printf("\n=== Ring Buffer Demo ===\n");
    ringbuf_t buffer;
    int status = rb_init(&buffer, 3);
    if (status != 0) {
        fprintf(stderr, "Failed to init ring buffer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    pthread_t producer_thread;
    pthread_t consumer_thread;

    struct rb_thread_args rb_args = { .buffer = &buffer };

    status = pthread_create(&producer_thread, NULL, rb_producer, &rb_args);
    if (status != 0) {
        fprintf(stderr, "Failed to create ring buffer producer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    status = pthread_create(&consumer_thread, NULL, rb_consumer, &rb_args);
    if (status != 0) {
        fprintf(stderr, "Failed to create ring buffer consumer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    status = pthread_join(producer_thread, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join ring buffer producer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    rb_close(&buffer);

    status = pthread_join(consumer_thread, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to join ring buffer consumer: %d\n", status);
        exit(EXIT_FAILURE);
    }

    rb_destroy(&buffer);
}

static void *event_advancer(void *arg)
{
    struct event_thread_args *args = (struct event_thread_args *)arg;
    for (int step = 1; step <= EVENT_STEPS; ++step) {
        sleep(1);
        eventcnt_advance(args->counter, 1);
        printf("Worker advanced counter to %d\n", step);
    }
    return NULL;
}

static void *rb_producer(void *arg)
{
    struct rb_thread_args *args = (struct rb_thread_args *)arg;
    for (int i = 0; i < RB_TEST_ITEMS; ++i) {
        rb_put(args->buffer, (uint8_t)(i + 1));
        printf("Producer queued value %d\n", i + 1);
        usleep(200000);
    }
    return NULL;
}

static void *rb_consumer(void *arg)
{
    struct rb_thread_args *args = (struct rb_thread_args *)arg;
    int received = 0;
    while (received < RB_TEST_ITEMS) {
        uint8_t value = rb_get(args->buffer);
        if (errno == ECANCELED && rb_is_closed(args->buffer)) {
            break;
        }
        printf("Consumer processed value %d\n", value);
        ++received;
        usleep(250000);
    }
    return NULL;
}
