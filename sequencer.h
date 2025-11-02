/*
 * sequencer.h - Milestone 4 sequencer interface for Threaded Programming Milestones
 * Author: GitHub Copilot (GPT-5 Codex)
 */

#ifndef SEQUENCER_H
#define SEQUENCER_H

#include <pthread.h>
#include <stdint.h>

typedef struct sequencer {
    uint64_t next_ticket;
    pthread_mutex_t lock;
} sequencer_t;

int sequencer_init(sequencer_t *s, uint64_t start);
void sequencer_destroy(sequencer_t *s);
uint64_t sequencer_ticket(sequencer_t *s);

#endif /* SEQUENCER_H */
