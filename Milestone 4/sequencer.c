/*
 * sequencer.c - Milestone 4 sequencer implementation for Threaded Programming Milestones
 * Purpose: Thread-safe generator of monotonically increasing tickets.
 * Notes:
 *  - All API calls are safe to invoke concurrently on the same instance.
 *  - Tickets increase by 1 starting from the provided start value.
 */

#include "sequencer.h"

#include <errno.h>
#include <stdio.h>

/* Initialize the sequencer with the next ticket set to 'start'. */
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

/* Destroy internal resources; does not free the struct itself. */
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

/*
 * Atomically fetch-and-increment the ticket value.
 * Returns 0 on error (e.g., NULL input or lock failure).
 */
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

    /* Fetch then increment while holding the mutex. */
    uint64_t ticket = s->next_ticket;
    s->next_ticket++;

    status = pthread_mutex_unlock(&s->lock);
    if (status != 0) {
        fprintf(stderr, "sequencer_ticket: unlock failed: %d\n", status);
    }

    return ticket;
}
