/*
 * sequencer.c - Milestone 4 sequencer implementation for Threaded Programming Milestones
 * Author: GitHub Copilot (GPT-5 Codex)
 * Purpose: Generates monotonically increasing sequence numbers safely.
 */

#include "sequencer.h"

#include <errno.h>
#include <stdio.h>

int sequencer_init(sequencer_t *s, uint64_t start)
{
    if (s == NULL) {
        return EINVAL;
    }

    s->next_ticket = start;

    int status = pthread_mutex_init(&s->lock, NULL);
    if (status != 0) {
        return status;
    }

    return 0;
}

void sequencer_destroy(sequencer_t *s)
{
    if (s == NULL) {
        return;
    }

    int status = pthread_mutex_destroy(&s->lock);
    if (status != 0) {
        fprintf(stderr, "sequencer_destroy: destroy failed: %d\n", status);
    }
}

uint64_t sequencer_ticket(sequencer_t *s)
{
    if (s == NULL) {
        return 0;
    }

    int status = pthread_mutex_lock(&s->lock);
    if (status != 0) {
        fprintf(stderr, "sequencer_ticket: lock failed: %d\n", status);
        return 0;
    }

    uint64_t ticket = s->next_ticket;
    s->next_ticket++;

    status = pthread_mutex_unlock(&s->lock);
    if (status != 0) {
        fprintf(stderr, "sequencer_ticket: unlock failed: %d\n", status);
    }

    return ticket;
}
