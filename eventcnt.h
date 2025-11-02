/*
 * eventcnt.h - Milestone 4 event counter interface for Threaded Programming Milestones
 * Author: GitHub Copilot (GPT-5 Codex)
 */

#ifndef EVENTCNT_H
#define EVENTCNT_H

#include <pthread.h>
#include <stdint.h>

typedef struct eventcnt {
    uint64_t count;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} eventcnt_t;

int eventcnt_init(eventcnt_t *e, uint64_t start);
void eventcnt_destroy(eventcnt_t *e);
uint64_t eventcnt_read(eventcnt_t *e);
void eventcnt_advance(eventcnt_t *e, uint64_t delta);
void eventcnt_await(eventcnt_t *e, uint64_t target);

#endif /* EVENTCNT_H */
