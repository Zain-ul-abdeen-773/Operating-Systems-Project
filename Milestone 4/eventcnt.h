/*
 * eventcnt.h - Milestone 4 event counter interface for Threaded Programming Milestones
 * Monotonic event counter supporting await(target) semantics.
 */

#ifndef EVENTCNT_H
#define EVENTCNT_H

#include <pthread.h>
#include <stdint.h>

/* Counter value guarded by a mutex; waiters block on cond until target reached. */
typedef struct eventcnt {
    uint64_t count;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} eventcnt_t;

/* Initialize to starting value. Returns 0 on success. */
int eventcnt_init(eventcnt_t *e, uint64_t start);
/* Destroy internal resources. Safe to call with NULL (no-op). */
void eventcnt_destroy(eventcnt_t *e);
/* Thread-safe read of current count. */
uint64_t eventcnt_read(eventcnt_t *e);
/* Add delta to count and wake any waiters (broadcast). */
void eventcnt_advance(eventcnt_t *e, uint64_t delta);
/* Block until count >= target. */
void eventcnt_await(eventcnt_t *e, uint64_t target);

#endif /* EVENTCNT_H */
