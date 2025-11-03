/*
 * ringbuf.h - Milestone 4 ring buffer interface for Threaded Programming Milestones
 * Blocking circular queue of bytes with close() to wake waiters.
 */

#ifndef RINGBUF_H
#define RINGBUF_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

/* Single-producer/consumer safe under external discipline; internal ops are locked. */
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

/* Allocate internal storage and init sync primitives. */
int rb_init(ringbuf_t *rb, size_t capacity);
/* Free storage and destroy sync primitives. */
void rb_destroy(ringbuf_t *rb);
/* Put one byte; blocks while full. If closed, sets errno=ECANCELED and returns. */
void rb_put(ringbuf_t *rb, uint8_t value);
/* Get one byte; blocks while empty. If closed and empty, sets errno=ECANCELED and returns 0. */
uint8_t rb_get(ringbuf_t *rb);
/* Mark closed and wake all waiters. */
void rb_close(ringbuf_t *rb);
/* Thread-safe check: 1 if closed, else 0. */
int rb_is_closed(ringbuf_t *rb);
/* Read a random byte from /dev/random into value. Returns 0 on success. */
int read_random_byte(uint8_t *value);

#endif /* RINGBUF_H */
