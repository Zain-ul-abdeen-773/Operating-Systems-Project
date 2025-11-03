/*
 * sema.h - Milestone 3 semaphore interface for Threaded Programming Milestones
 * Counting semaphore built from pthread mutex/cond.
 */

#ifndef SEMA_H
#define SEMA_H

#include <pthread.h>

/* Semaphore with an integer count and wait/signal via condition variable. */
typedef struct semaphore {
    int value;
    pthread_mutex_t mtx;
    pthread_cond_t cv;
} semaphore_t;

/* Initialize with a non-negative initial value. Returns 0 on success. */
int sema_init(semaphore_t *s, int value);
/* Destroy internal resources. Returns 0 on success. */
int sema_destroy(semaphore_t *s);
/* Decrement when value > 0, otherwise block until signaled. */
int sema_wait(semaphore_t *s);
/* Increment and wake one waiter (if any). */
int sema_post(semaphore_t *s);

#endif /* SEMA_H */
