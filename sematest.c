/*
 * sematest.c - Milestone 3 test driver for Threaded Programming Milestones
 * Purpose: Demonstrates counting semaphore vs mutex concurrency limits.
 */

#include "sema.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define WORKER_COUNT 5

struct sema_worker_args {
    int id;
    semaphore_t *sema;
};

struct mutex_worker_args {
    int id;
    pthread_mutex_t *mutex;
};

static void *sema_worker(void *arg);
static void *mutex_worker(void *arg);
static void launch_semaphore_demo(void);
static void launch_mutex_demo(void);

int main(void)
{
    printf("=== Semaphore demo (capacity = 2) ===\n");
    launch_semaphore_demo();

    printf("\n=== Mutex demo (capacity = 1) ===\n");
    launch_mutex_demo();

    return 0;
}

static void launch_semaphore_demo(void)
{
    semaphore_t sema;
    int status = sema_init(&sema, 2);
    if (status != 0) {
        fprintf(stderr, "Failed to init semaphore: %d\n", status);
        exit(EXIT_FAILURE);
    }

    pthread_t threads[WORKER_COUNT];
    struct sema_worker_args args[WORKER_COUNT];

    for (int i = 0; i < WORKER_COUNT; ++i) {
        args[i].id = i;
        args[i].sema = &sema;
        status = pthread_create(&threads[i], NULL, sema_worker, &args[i]);
        if (status != 0) {
            fprintf(stderr, "Failed to create semaphore worker %d: %d\n", i, status);
            exit(EXIT_FAILURE);
        }
    }

    for (int i = 0; i < WORKER_COUNT; ++i) {
        status = pthread_join(threads[i], NULL);
        if (status != 0) {
            fprintf(stderr, "Failed to join semaphore worker %d: %d\n", i, status);
            exit(EXIT_FAILURE);
        }
    }

    status = sema_destroy(&sema);
    if (status != 0) {
        fprintf(stderr, "Failed to destroy semaphore: %d\n", status);
        exit(EXIT_FAILURE);
    }
}

static void launch_mutex_demo(void)
{
    pthread_mutex_t mutex;
    int status = pthread_mutex_init(&mutex, NULL);
    if (status != 0) {
        fprintf(stderr, "Failed to init mutex: %d\n", status);
        exit(EXIT_FAILURE);
    }

    pthread_t threads[WORKER_COUNT];
    struct mutex_worker_args args[WORKER_COUNT];

    for (int i = 0; i < WORKER_COUNT; ++i) {
        args[i].id = i;
        args[i].mutex = &mutex;
        status = pthread_create(&threads[i], NULL, mutex_worker, &args[i]);
        if (status != 0) {
            fprintf(stderr, "Failed to create mutex worker %d: %d\n", i, status);
            exit(EXIT_FAILURE);
        }
    }

    for (int i = 0; i < WORKER_COUNT; ++i) {
        status = pthread_join(threads[i], NULL);
        if (status != 0) {
            fprintf(stderr, "Failed to join mutex worker %d: %d\n", i, status);
            exit(EXIT_FAILURE);
        }
    }

    status = pthread_mutex_destroy(&mutex);
    if (status != 0) {
        fprintf(stderr, "Failed to destroy mutex: %d\n", status);
        exit(EXIT_FAILURE);
    }
}

static void *sema_worker(void *arg)
{
    struct sema_worker_args *worker = (struct sema_worker_args *)arg;
    int status = sema_wait(worker->sema);
    if (status != 0) {
        fprintf(stderr, "Worker %d failed to wait on semaphore: %d\n", worker->id, status);
        return NULL;
    }

    printf("[Semaphore] Worker %d entering critical section\n", worker->id);
    sleep(1);
    printf("[Semaphore] Worker %d leaving critical section\n", worker->id);

    status = sema_post(worker->sema);
    if (status != 0) {
        fprintf(stderr, "Worker %d failed to post semaphore: %d\n", worker->id, status);
    }

    return NULL;
}

static void *mutex_worker(void *arg)
{
    struct mutex_worker_args *worker = (struct mutex_worker_args *)arg;
    int status = pthread_mutex_lock(worker->mutex);
    if (status != 0) {
        fprintf(stderr, "Worker %d failed to lock mutex: %d\n", worker->id, status);
        return NULL;
    }

    printf("[Mutex] Worker %d entering critical section\n", worker->id);
    sleep(1);
    printf("[Mutex] Worker %d leaving critical section\n", worker->id);

    status = pthread_mutex_unlock(worker->mutex);
    if (status != 0) {
        fprintf(stderr, "Worker %d failed to unlock mutex: %d\n", worker->id, status);
    }

    return NULL;
}
