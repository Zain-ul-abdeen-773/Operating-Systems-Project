/*
 * ringbuf.h - Milestone 4 ring buffer interface for Threaded Programming Milestones
 * Author: GitHub Copilot (GPT-5 Codex)
 */

#ifndef RINGBUF_H
#define RINGBUF_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

typedef struct ringbuf {
    uint8_t *data;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    int closed;
    pthread_mutex_t lock;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} ringbuf_t;

int rb_init(ringbuf_t *rb, size_t capacity);
void rb_destroy(ringbuf_t *rb);
void rb_put(ringbuf_t *rb, uint8_t value);
uint8_t rb_get(ringbuf_t *rb);
void rb_close(ringbuf_t *rb);
int rb_is_closed(ringbuf_t *rb);
int read_random_byte(uint8_t *value);

#endif /* RINGBUF_H */
