/*
 * sema.h - Milestone 3 semaphore interface for Threaded Programming Milestones
 * Author: GitHub Copilot (GPT-5 Codex)
 */

#ifndef SEMA_H
#define SEMA_H

#include <pthread.h>

typedef struct semaphore {
    int value;
    pthread_mutex_t mtx;
    pthread_cond_t cv;
} semaphore_t;

int sema_init(semaphore_t *s, int value);
int sema_destroy(semaphore_t *s);
int sema_wait(semaphore_t *s);
int sema_post(semaphore_t *s);

#endif /* SEMA_H */
