/* mpmc_ring.h — bounded lock-free MPMC ring buffer (Vyukov-style).
 *
 * Multiple producers and multiple consumers push and pop opaque void*
 * items without any mutex on the fast path.  The design is Dmitry
 * Vyukov's sequence-tag scheme: each slot carries a u64 `seq` that
 * encodes a "round" number, letting producers/consumers detect that
 * the slot they want to touch is either already taken or still pending.
 *
 * Invariants:
 *   - cap must be a power of two (enforced at init).
 *   - items are void*; NULL is a legal stored value.
 *   - push/pop are wait-free under the absence of contention and
 *     lock-free under contention — a stalled thread cannot block any
 *     other producer or consumer indefinitely.
 *
 * Use TSDB_MPMC_CACHELINE_PAD to keep head/tail on separate cache lines
 * so producers and consumers don't thrash each other via false sharing.
 *
 * This is a *bounded* queue.  Non-blocking push returns 0 when full;
 * callers should either retry, backoff, or use the blocking variant
 * (push_wait) which parks on a condvar when the ring is saturated.
 */
#ifndef TSDB_CORE_MPMC_RING_H
#define TSDB_CORE_MPMC_RING_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_MPMC_CACHELINE 64

typedef struct {
    _Atomic uint64_t seq;    /* sequence tag, see impl */
    void            *data;
} tsdb_mpmc_slot_t;

typedef struct {
    /* Producers contend only on enq_pos. */
    _Atomic uint64_t enq_pos;
    char _pad0[TSDB_MPMC_CACHELINE - sizeof(uint64_t)];

    /* Consumers contend only on deq_pos. */
    _Atomic uint64_t deq_pos;
    char _pad1[TSDB_MPMC_CACHELINE - sizeof(uint64_t)];

    /* Slots + mask (cap must be pow2, mask = cap - 1). */
    tsdb_mpmc_slot_t *slots;
    uint64_t          mask;
    uint64_t          cap;

    /* Optional parking — each direction uses its OWN mutex so a
     * blocking popper's signal-to-producer path can't deadlock
     * against itself.  parked_* counters are the only cross-mutex
     * hand-off and are atomic. */
    pthread_mutex_t   prod_mu;    /* producers park here when full */
    pthread_cond_t    space_cv;
    pthread_mutex_t   cons_mu;    /* consumers park here when empty */
    pthread_cond_t    item_cv;
    _Atomic int       parked_producers;
    _Atomic int       parked_consumers;
} tsdb_mpmc_ring_t;

/* Initialize a ring with `cap` slots.  cap MUST be a power of two
 * and >= 2.  Returns 0 on success, -1 on invalid cap / allocation
 * failure. */
int  tsdb_mpmc_init(tsdb_mpmc_ring_t *r, uint64_t cap);

/* Release all resources.  Does NOT free items still in the ring — the
 * caller is responsible for draining first. */
void tsdb_mpmc_destroy(tsdb_mpmc_ring_t *r);

/* Non-blocking push.  Returns 1 on success, 0 if full. */
int  tsdb_mpmc_push_nb(tsdb_mpmc_ring_t *r, void *item);

/* Non-blocking pop.  Returns 1 and stores into *out on success, 0 if
 * empty. */
int  tsdb_mpmc_pop_nb (tsdb_mpmc_ring_t *r, void **out);

/* Blocking push.  Parks on space_cv when full.  Returns 1 on success,
 * 0 on timeout (timeout_ms < 0 → wait forever, 0 → return-fast). */
int  tsdb_mpmc_push   (tsdb_mpmc_ring_t *r, void *item, int timeout_ms);

/* Blocking pop.  Parks on item_cv when empty. */
int  tsdb_mpmc_pop    (tsdb_mpmc_ring_t *r, void **out, int timeout_ms);

/* Approximate count (for metrics / backpressure).  Not atomic-coherent
 * — producers/consumers may be mid-stride — but correct to within the
 * number of threads actively pushing+popping. */
uint64_t tsdb_mpmc_approx_size(const tsdb_mpmc_ring_t *r);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CORE_MPMC_RING_H */
