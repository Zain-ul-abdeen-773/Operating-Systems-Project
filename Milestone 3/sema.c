/*
 * sema.c - Milestone 3 semaphore implementation for Threaded Programming Milestones
 * Purpose: Implements a counting semaphore using mutexes and condition variables.
 * Notes:
 *  - wait() blocks while the count is 0; post() increments and signals one waiter.
 *  - All operations are protected by the internal mutex and are thread-safe.
 */

#include "sema.h"

#include <errno.h>

/* Initialize semaphore with non-negative initial value. */
int sema_init(semaphore_t *s, int value)
{
    /* Validate arguments: semaphore pointer must be non-NULL and initial
     * value must be non-negative. Return EINVAL on invalid input. */
    if (s == NULL || value < 0) {
        return EINVAL;
    }

    /* Set the initial counter value. This represents available permits. */
    s->value = value;

    /* Initialize internal mutex protecting the counter. */
    int status = pthread_mutex_init(&s->mtx, NULL);
    if (status != 0) {
        /* Propagate pthread error code to caller. */
        return status;
    }

    /* Initialize condition variable used to block/wake waiters. */
    status = pthread_cond_init(&s->cv, NULL);
    if (status != 0) {
        /* Clean up mutex if cond init fails to avoid resource leak. */
        pthread_mutex_destroy(&s->mtx);
        return status;
    }

    return 0; /* success */
}

/* Destroy semaphore resources. */
int sema_destroy(semaphore_t *s)
{
    /* Validate argument. */
    if (s == NULL) {
        return EINVAL;
    }

    /* Destroy condition variable first. If it is busy (waiters still exist)
     * the destroy will fail; propagate that error to the caller. */
    int status = pthread_cond_destroy(&s->cv);
    if (status != 0) {
        return status;
    }

    /* Then destroy the mutex protecting the semaphore. */
    status = pthread_mutex_destroy(&s->mtx);
    if (status != 0) {
        return status;
    }

    return 0; /* success */
}

/* Acquire one unit; block if none are available. */
int sema_wait(semaphore_t *s)
{
    /* Validate argument. */
    if (s == NULL) {
        return EINVAL;
    }

    /* Acquire internal mutex to examine/modify semaphore counter. */
    int status = pthread_mutex_lock(&s->mtx);
    if (status != 0) {
        return status;
    }

    /* If no permits are available (value == 0), block on condition variable.
     * Use a while-loop to guard against spurious wakeups: on wake we re-check
     * the condition under the same mutex before proceeding. */
    while (s->value == 0) {
        status = pthread_cond_wait(&s->cv, &s->mtx);
        if (status != 0) {
            /* On error, unlock mutex and return the error code. */
            pthread_mutex_unlock(&s->mtx);
            return status;
        }
    }

    /* Consume one permit. This operation is protected by the mutex. */
    s->value--;

    /* Release internal mutex and return. */
    status = pthread_mutex_unlock(&s->mtx);
    if (status != 0) {
        return status;
    }

    return 0; /* success */
}

/* Release one unit and wake a single waiter. */
int sema_post(semaphore_t *s)
{
    /* Validate argument. */
    if (s == NULL) {
        return EINVAL;
    }

    /* Acquire mutex to modify counter atomically. */
    int status = pthread_mutex_lock(&s->mtx);
    if (status != 0) {
        return status;
    }

    /* Increase available permits. */
    s->value++;

    /* Signal one waiter that a permit may now be available. Signal can be
     * called while holding the mutex or after unlocking; calling it while
     * holding the mutex avoids a small race window in some edge cases. */
    status = pthread_cond_signal(&s->cv);
    if (status != 0) {
        /* On error, unlock and propagate. */
        pthread_mutex_unlock(&s->mtx);
        return status;
    }

    /* Release mutex and return success. */
    status = pthread_mutex_unlock(&s->mtx);
    if (status != 0) {
        return status;
    }

    return 0; /* success */
}
