/*
 * ringbuf.c - Milestone 4 ring buffer implementation for Threaded Programming Milestones
 * Author: GitHub Copilot (GPT-5 Codex)
 * Purpose: Provides a blocking circular byte queue built with pthread primitives.
 */

#include "ringbuf.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int rb_init(ringbuf_t *rb, size_t capacity)
{
    if (rb == NULL || capacity == 0) {
        return EINVAL;
    }

    memset(rb, 0, sizeof(*rb));
    rb->capacity = capacity;
    rb->data = (uint8_t *)malloc(capacity);
    if (rb->data == NULL) {
        return ENOMEM;
    }

    int status = pthread_mutex_init(&rb->lock, NULL);
    if (status != 0) {
        free(rb->data);
        return status;
    }

    status = pthread_cond_init(&rb->not_full, NULL);
    if (status != 0) {
        pthread_mutex_destroy(&rb->lock);
        free(rb->data);
        return status;
    }

    status = pthread_cond_init(&rb->not_empty, NULL);
    if (status != 0) {
        pthread_cond_destroy(&rb->not_full);
        pthread_mutex_destroy(&rb->lock);
        free(rb->data);
        return status;
    }

    return 0;
}

void rb_destroy(ringbuf_t *rb)
{
    if (rb == NULL) {
        return;
    }

    int status = pthread_cond_destroy(&rb->not_empty);
    if (status != 0) {
        fprintf(stderr, "rb_destroy: not_empty destroy failed: %d\n", status);
    }

    status = pthread_cond_destroy(&rb->not_full);
    if (status != 0) {
        fprintf(stderr, "rb_destroy: not_full destroy failed: %d\n", status);
    }

    status = pthread_mutex_destroy(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_destroy: mutex destroy failed: %d\n", status);
    }

    free(rb->data);
    rb->data = NULL;
    rb->capacity = 0;
}

void rb_put(ringbuf_t *rb, uint8_t value)
{
    if (rb == NULL) {
        return;
    }

    int status = pthread_mutex_lock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_put: lock failed: %d\n", status);
        return;
    }

    while (rb->count == rb->capacity && rb->closed == 0) {
        status = pthread_cond_wait(&rb->not_full, &rb->lock);
        if (status != 0) {
            fprintf(stderr, "rb_put: wait failed: %d\n", status);
            pthread_mutex_unlock(&rb->lock);
            return;
        }
    }

    if (rb->closed) {
        errno = ECANCELED;
        pthread_mutex_unlock(&rb->lock);
        return;
    }

    rb->data[rb->tail] = value;
    rb->tail = (rb->tail + 1) % rb->capacity;
    rb->count++;

    status = pthread_cond_signal(&rb->not_empty);
    if (status != 0) {
        fprintf(stderr, "rb_put: signal failed: %d\n", status);
    }

    status = pthread_mutex_unlock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_put: unlock failed: %d\n", status);
    }
}

uint8_t rb_get(ringbuf_t *rb)
{
    if (rb == NULL) {
        errno = EINVAL;
        return 0;
    }

    int status = pthread_mutex_lock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_get: lock failed: %d\n", status);
        errno = status;
        return 0;
    }

    while (rb->count == 0 && rb->closed == 0) {
        status = pthread_cond_wait(&rb->not_empty, &rb->lock);
        if (status != 0) {
            fprintf(stderr, "rb_get: wait failed: %d\n", status);
            pthread_mutex_unlock(&rb->lock);
            errno = status;
            return 0;
        }
    }

    if (rb->count == 0 && rb->closed) {
        pthread_mutex_unlock(&rb->lock);
        errno = ECANCELED;
        return 0;
    }

    uint8_t value = rb->data[rb->head];
    rb->head = (rb->head + 1) % rb->capacity;
    rb->count--;

    status = pthread_cond_signal(&rb->not_full);
    if (status != 0) {
        fprintf(stderr, "rb_get: signal failed: %d\n", status);
    }

    status = pthread_mutex_unlock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_get: unlock failed: %d\n", status);
    }

    return value;
}

void rb_close(ringbuf_t *rb)
{
    if (rb == NULL) {
        return;
    }

    int status = pthread_mutex_lock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_close: lock failed: %d\n", status);
        return;
    }

    rb->closed = 1;

    status = pthread_cond_broadcast(&rb->not_empty);
    if (status != 0) {
        fprintf(stderr, "rb_close: broadcast not_empty failed: %d\n", status);
    }

    status = pthread_cond_broadcast(&rb->not_full);
    if (status != 0) {
        fprintf(stderr, "rb_close: broadcast not_full failed: %d\n", status);
    }

    status = pthread_mutex_unlock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_close: unlock failed: %d\n", status);
    }
}

int rb_is_closed(ringbuf_t *rb)
{
    if (rb == NULL) {
        return 1;
    }

    int status = pthread_mutex_lock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_is_closed: lock failed: %d\n", status);
        return 1;
    }

    int value = rb->closed;

    status = pthread_mutex_unlock(&rb->lock);
    if (status != 0) {
        fprintf(stderr, "rb_is_closed: unlock failed: %d\n", status);
    }

    return value;
}

int read_random_byte(uint8_t *value)
{
    if (value == NULL) {
        return EINVAL;
    }

    static int random_fd = -1;
    if (random_fd == -1) {
        int fd = open("/dev/random", O_RDONLY);
        if (fd == -1) {
            return errno;
        }
        random_fd = fd;
    }

    ssize_t bytes_read = 0;
    while (bytes_read < 1) {
        ssize_t rc = read(random_fd, value + bytes_read, 1 - bytes_read);
        if (rc > 0) {
            bytes_read += rc;
            continue;
        }
        if (rc == 0) {
            return EIO;
        }
        if (errno == EINTR) {
            continue;
        }
        return errno;
    }

    /*
     * Reading from /dev/random may block until sufficient entropy is available,
     * which makes it easy to visualize backpressure in multi-threaded flows.
     */
    return 0;
}
