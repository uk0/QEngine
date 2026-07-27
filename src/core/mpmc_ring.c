/* mpmc_ring.c — Vyukov bounded MPMC queue.
 *
 * Each slot carries a seq (uint64) that alternates between "empty"
 * and "full" generations:
 *
 *   Initially  slot[i].seq = i
 *   Producer   reads pos, expects seq == pos; CAS enq_pos++, writes
 *              data, stores seq = pos + 1 (marks "full for this round")
 *   Consumer   reads pos, expects seq == pos + 1; CAS deq_pos++, reads
 *              data, stores seq = pos + cap (marks "empty for next
 *              round")
 *
 * No ABA: seq is 64-bit and monotonically increasing.  No lock on the
 * fast path.  Only the optional parking uses a mutex+cv to avoid
 * spinning when full or empty — a waker takes the PEER direction's
 * mutex, and never while holding its own (see the LOCK ORDER RULE
 * below and in mpmc_ring.h).
 */

#include "mpmc_ring.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

static int is_pow2(uint64_t x) {
    return x >= 2 && ((x & (x - 1)) == 0);
}

int tsdb_mpmc_init(tsdb_mpmc_ring_t *r, uint64_t cap) {
    if (!r || !is_pow2(cap)) return -1;
    memset(r, 0, sizeof(*r));
    r->cap   = cap;
    r->mask  = cap - 1;
    r->slots = calloc(cap, sizeof(tsdb_mpmc_slot_t));
    if (!r->slots) return -1;
    for (uint64_t i = 0; i < cap; i++) {
        atomic_store_explicit(&r->slots[i].seq, i, memory_order_relaxed);
        r->slots[i].data = NULL;
    }
    atomic_store_explicit(&r->enq_pos, 0, memory_order_relaxed);
    atomic_store_explicit(&r->deq_pos, 0, memory_order_relaxed);
    pthread_mutex_init(&r->prod_mu, NULL);
    pthread_mutex_init(&r->cons_mu, NULL);
    pthread_cond_init (&r->space_cv, NULL);
    pthread_cond_init (&r->item_cv,  NULL);
    return 0;
}

void tsdb_mpmc_destroy(tsdb_mpmc_ring_t *r) {
    if (!r) return;
    free(r->slots);
    r->slots = NULL;
    pthread_mutex_destroy(&r->prod_mu);
    pthread_mutex_destroy(&r->cons_mu);
    pthread_cond_destroy (&r->space_cv);
    pthread_cond_destroy (&r->item_cv);
}

/* ---- claim / wake split ---------------------------------------------
 *
 * LOCK ORDER RULE: no thread may hold prod_mu while acquiring cons_mu,
 * nor cons_mu while acquiring prod_mu.  The ring transaction and the
 * peer wake-up are therefore two separate steps:
 *
 *   *_claim()   runs the whole lock-free ring transaction and takes NO
 *               mutex, so a blocking waiter may call it while holding
 *               its own mutex.  It reports through *wake whether a peer
 *               was parked at the moment the item (or the freed slot)
 *               was published.
 *   wake_*()    takes the PEER's mutex to signal.  Every caller has
 *               released its own mutex first, so no thread is ever
 *               holding both — the AB-BA cycle cannot form.
 *
 * The earlier version signalled the peer from inside *_nb(), and the
 * blocking forms called *_nb() from inside their parked loop while
 * holding their own mutex.  A blocking pusher then held prod_mu and
 * wanted cons_mu while a blocking popper held cons_mu and wanted
 * prod_mu — a deadlock reachable with one producer and one consumer.
 */

static int push_claim(tsdb_mpmc_ring_t *r, void *item, int *wake) {
    *wake = 0;
    if (!r) return 0;
    uint64_t pos = atomic_load_explicit(&r->enq_pos, memory_order_relaxed);
    for (;;) {
        tsdb_mpmc_slot_t *slot = &r->slots[pos & r->mask];
        uint64_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
        int64_t  diff = (int64_t)seq - (int64_t)pos;
        if (diff == 0) {
            /* Slot is empty for this round — try to claim. */
            if (atomic_compare_exchange_weak_explicit(
                    &r->enq_pos, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed)) {
                slot->data = item;
                atomic_store_explicit(&slot->seq, pos + 1, memory_order_release);
                /* The item is published before we read parked_consumers,
                 * so a consumer that parks after this point re-checks the
                 * ring under cons_mu and finds it. */
                *wake = atomic_load_explicit(&r->parked_consumers,
                                             memory_order_relaxed) > 0;
                return 1;
            }
            /* CAS failed, another producer got here — pos already
             * reloaded by CAS, retry. */
        } else if (diff < 0) {
            /* Slot full for this round — queue is full. */
            return 0;
        } else {
            /* Slot in use or stale pos; refresh. */
            pos = atomic_load_explicit(&r->enq_pos, memory_order_relaxed);
        }
    }
}

static int pop_claim(tsdb_mpmc_ring_t *r, void **out, int *wake) {
    *wake = 0;
    if (!r) return 0;
    uint64_t pos = atomic_load_explicit(&r->deq_pos, memory_order_relaxed);
    for (;;) {
        tsdb_mpmc_slot_t *slot = &r->slots[pos & r->mask];
        uint64_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
        int64_t  diff = (int64_t)seq - (int64_t)(pos + 1);
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &r->deq_pos, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed)) {
                if (out) *out = slot->data;
                slot->data = NULL;
                atomic_store_explicit(&slot->seq, pos + r->mask + 1,
                                      memory_order_release);
                *wake = atomic_load_explicit(&r->parked_producers,
                                             memory_order_relaxed) > 0;
                return 1;
            }
        } else if (diff < 0) {
            return 0; /* empty */
        } else {
            pos = atomic_load_explicit(&r->deq_pos, memory_order_relaxed);
        }
    }
}

/* Callers MUST hold neither prod_mu nor cons_mu. */
static void wake_consumers(tsdb_mpmc_ring_t *r) {
    pthread_mutex_lock(&r->cons_mu);
    pthread_cond_signal(&r->item_cv);
    pthread_mutex_unlock(&r->cons_mu);
}

static void wake_producers(tsdb_mpmc_ring_t *r) {
    pthread_mutex_lock(&r->prod_mu);
    pthread_cond_signal(&r->space_cv);
    pthread_mutex_unlock(&r->prod_mu);
}

int tsdb_mpmc_push_nb(tsdb_mpmc_ring_t *r, void *item) {
    int wake = 0;
    if (!push_claim(r, item, &wake)) return 0;
    if (wake) wake_consumers(r);   /* holds no mutex here */
    return 1;
}

int tsdb_mpmc_pop_nb(tsdb_mpmc_ring_t *r, void **out) {
    int wake = 0;
    if (!pop_claim(r, out, &wake)) return 0;
    if (wake) wake_producers(r);   /* holds no mutex here */
    return 1;
}

/* Compute a pthread absolute deadline `ms` milliseconds into the future. */
static void deadline_from_ms(struct timespec *ts, int ms) {
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec  += ms / 1000;
    ts->tv_nsec += (long)(ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec  += 1;
        ts->tv_nsec -= 1000000000L;
    }
}

int tsdb_mpmc_push(tsdb_mpmc_ring_t *r, void *item, int timeout_ms) {
    if (tsdb_mpmc_push_nb(r, item)) return 1;
    if (timeout_ms == 0) return 0;

    struct timespec deadline;
    if (timeout_ms > 0) deadline_from_ms(&deadline, timeout_ms);

    pthread_mutex_lock(&r->prod_mu);
    atomic_fetch_add_explicit(&r->parked_producers, 1, memory_order_relaxed);
    for (;;) {
        /* Re-check after taking the mutex — a consumer could have
         * signalled between our non-blocking try and the mutex.  Claim
         * only: taking cons_mu here, under prod_mu, is the AB-BA. */
        int wake = 0;
        if (push_claim(r, item, &wake)) {
            atomic_fetch_sub_explicit(&r->parked_producers, 1, memory_order_relaxed);
            pthread_mutex_unlock(&r->prod_mu);   /* drop ours first ... */
            if (wake) wake_consumers(r);         /* ... then take theirs */
            return 1;
        }
        int rc;
        if (timeout_ms < 0) {
            rc = pthread_cond_wait(&r->space_cv, &r->prod_mu);
        } else {
            rc = pthread_cond_timedwait(&r->space_cv, &r->prod_mu, &deadline);
        }
        if (rc == ETIMEDOUT) {
            atomic_fetch_sub_explicit(&r->parked_producers, 1, memory_order_relaxed);
            pthread_mutex_unlock(&r->prod_mu);
            return 0;
        }
    }
}

int tsdb_mpmc_pop(tsdb_mpmc_ring_t *r, void **out, int timeout_ms) {
    if (tsdb_mpmc_pop_nb(r, out)) return 1;
    if (timeout_ms == 0) return 0;

    struct timespec deadline;
    if (timeout_ms > 0) deadline_from_ms(&deadline, timeout_ms);

    pthread_mutex_lock(&r->cons_mu);
    atomic_fetch_add_explicit(&r->parked_consumers, 1, memory_order_relaxed);
    for (;;) {
        /* Claim only — taking prod_mu here, under cons_mu, is the AB-BA. */
        int wake = 0;
        if (pop_claim(r, out, &wake)) {
            atomic_fetch_sub_explicit(&r->parked_consumers, 1, memory_order_relaxed);
            pthread_mutex_unlock(&r->cons_mu);   /* drop ours first ... */
            if (wake) wake_producers(r);         /* ... then take theirs */
            return 1;
        }
        int rc;
        if (timeout_ms < 0) {
            rc = pthread_cond_wait(&r->item_cv, &r->cons_mu);
        } else {
            rc = pthread_cond_timedwait(&r->item_cv, &r->cons_mu, &deadline);
        }
        if (rc == ETIMEDOUT) {
            atomic_fetch_sub_explicit(&r->parked_consumers, 1, memory_order_relaxed);
            pthread_mutex_unlock(&r->cons_mu);
            return 0;
        }
    }
}

uint64_t tsdb_mpmc_approx_size(const tsdb_mpmc_ring_t *r) {
    if (!r) return 0;
    uint64_t enq = atomic_load_explicit(&r->enq_pos, memory_order_relaxed);
    uint64_t deq = atomic_load_explicit(&r->deq_pos, memory_order_relaxed);
    /* Producers may be ahead of consumers — deq can only trail. */
    return enq >= deq ? enq - deq : 0;
}
