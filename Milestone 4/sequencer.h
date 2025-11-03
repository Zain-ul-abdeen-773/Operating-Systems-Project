/*
 * sequencer.h - Milestone 4 sequencer interface for Threaded Programming Milestones

 * Provides a simple ticket dispenser that hands out increasing numbers.
 */

#ifndef SEQUENCER_H
#define SEQUENCER_H

#include <pthread.h>
#include <stdint.h>

/* Ticket dispenser protected by a mutex for thread safety. */
typedef struct sequencer {
    uint64_t next_ticket;
    pthread_mutex_t lock;
} sequencer_t;

/* Initialize with the starting ticket value. Returns 0 on success. */
int sequencer_init(sequencer_t *s, uint64_t start);
/* Destroy internal mutex. Safe to call with NULL (no-op). */
void sequencer_destroy(sequencer_t *s);
/* Atomically return current ticket and advance to the next. */
uint64_t sequencer_ticket(sequencer_t *s);

#endif /* SEQUENCER_H */
