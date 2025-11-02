/*
 * eventcnt.c - Milestone 4 event counter implementation for Threaded Programming Milestones
 * Author: GitHub Copilot (GPT-5 Codex)
 * Purpose: Tracks monotonically increasing event counts with blocking awaits.
 */

#include "eventcnt.h"

#include <errno.h>
#include <stdio.h>

int eventcnt_init(eventcnt_t *e, uint64_t start)
{
    if (e == NULL) {
        return EINVAL;
    }

    e->count = start;

    int status = pthread_mutex_init(&e->lock, NULL);
    if (status != 0) {
        return status;
    }

    status = pthread_cond_init(&e->cond, NULL);
    if (status != 0) {
        pthread_mutex_destroy(&e->lock);
        return status;
    }

    return 0;
}

void eventcnt_destroy(eventcnt_t *e)
{
    if (e == NULL) {
        return;
    }

    int status = pthread_cond_destroy(&e->cond);
    if (status != 0) {
        fprintf(stderr, "eventcnt_destroy: cond destroy failed: %d\n", status);
    }

    status = pthread_mutex_destroy(&e->lock);
    if (status != 0) {
        fprintf(stderr, "eventcnt_destroy: mutex destroy failed: %d\n", status);
    }
}

uint64_t eventcnt_read(eventcnt_t *e)
{
    if (e == NULL) {
        return 0;
    }

    int status = pthread_mutex_lock(&e->lock);
    if (status != 0) {
        fprintf(stderr, "eventcnt_read: lock failed: %d\n", status);
        return 0;
    }
    uint64_t value = e->count;
    status = pthread_mutex_unlock(&e->lock);
    if (status != 0) {
        fprintf(stderr, "eventcnt_read: unlock failed: %d\n", status);
    }
    return value;
}

void eventcnt_advance(eventcnt_t *e, uint64_t delta)
{
    if (e == NULL) {
        return;
    }

    int status = pthread_mutex_lock(&e->lock);
    if (status != 0) {
        fprintf(stderr, "eventcnt_advance: lock failed: %d\n", status);
        return;
    }
    e->count += delta;
    /* Broadcast ensures all waiters observe the atomic count update. */
    status = pthread_cond_broadcast(&e->cond);
    if (status != 0) {
        fprintf(stderr, "eventcnt_advance: broadcast failed: %d\n", status);
    }
    status = pthread_mutex_unlock(&e->lock);
    if (status != 0) {
        fprintf(stderr, "eventcnt_advance: unlock failed: %d\n", status);
    }
}

void eventcnt_await(eventcnt_t *e, uint64_t target)
{
    if (e == NULL) {
        return;
    }

    int status = pthread_mutex_lock(&e->lock);
    if (status != 0) {
        fprintf(stderr, "eventcnt_await: lock failed: %d\n", status);
        return;
    }
    while (e->count < target) {
        status = pthread_cond_wait(&e->cond, &e->lock);
        if (status != 0) {
            fprintf(stderr, "eventcnt_await: wait failed: %d\n", status);
            break;
        }
    }
    status = pthread_mutex_unlock(&e->lock);
    if (status != 0) {
        fprintf(stderr, "eventcnt_await: unlock failed: %d\n", status);
    }
}
