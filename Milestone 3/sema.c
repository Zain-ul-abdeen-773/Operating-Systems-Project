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
    if (s == NULL || value < 0) {
        return EINVAL;
    }

    s->value = value;

    int status = pthread_mutex_init(&s->mtx, NULL);
    if (status != 0) {
        return status;
    }

    status = pthread_cond_init(&s->cv, NULL);
    if (status != 0) {
        pthread_mutex_destroy(&s->mtx);
        return status;
    }

    return 0;
}

/* Destroy semaphore resources. */
int sema_destroy(semaphore_t *s)
{
    if (s == NULL) {
        return EINVAL;
    }

    int status = pthread_cond_destroy(&s->cv);
    if (status != 0) {
        return status;
    }

    status = pthread_mutex_destroy(&s->mtx);
    if (status != 0) {
        return status;
    }

    return 0;
}

/* Acquire one unit; block if none are available. */
int sema_wait(semaphore_t *s)
{
    if (s == NULL) {
        return EINVAL;
    }

    int status = pthread_mutex_lock(&s->mtx);
    if (status != 0) {
        return status;
    }

    while (s->value == 0) {
        status = pthread_cond_wait(&s->cv, &s->mtx);
        if (status != 0) {
            pthread_mutex_unlock(&s->mtx);
            return status;
        }
    }

    s->value--;

    status = pthread_mutex_unlock(&s->mtx);
    if (status != 0) {
        return status;
    }

    return 0;
}

/* Release one unit and wake a single waiter. */
int sema_post(semaphore_t *s)
{
    if (s == NULL) {
        return EINVAL;
    }

    int status = pthread_mutex_lock(&s->mtx);
    if (status != 0) {
        return status;
    }

    s->value++;

    status = pthread_cond_signal(&s->cv);
    if (status != 0) {
        pthread_mutex_unlock(&s->mtx);
        return status;
    }

    status = pthread_mutex_unlock(&s->mtx);
    if (status != 0) {
        return status;
    }

    return 0;
}
