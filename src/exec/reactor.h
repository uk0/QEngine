/* reactor.h — single-producer/single-consumer ring + per-core reactor.
 *
 * Foundation for the ScyllaDB-style shard-per-core evolution (Phase 0).
 * Two primitives, neither yet wired into the storage/cluster hot path:
 *
 *   1. tsdb_spsc_ring_t — a bounded, lock-free ring for ONE producer
 *      thread and ONE consumer thread.  Unlike the Vyukov MPMC ring
 *      (src/core/mpmc_ring.h), SPSC needs only two monotonic counters
 *      with an acquire/release pair: the producer writes `tail` only,
 *      the consumer writes `head` only, so there is no CAS and no
 *      contention between the two ends.  This is the queue a front-end
 *      I/O thread will use to hand work to the single core that owns a
 *      table's shard.
 *
 *   2. tsdb_reactor_t — a single-threaded event loop bound (later) to one
 *      core.  Work is submitted as (fn, arg) closures from a producer
 *      thread; the reactor drains its inbox and runs each closure ON its
 *      own thread.  In the target architecture the closure mutates the
 *      core's owned tables with no locks because the reactor is the sole
 *      accessor.  Phase 0 ships the loop + lifecycle only.
 *
 * Invariants:
 *   - SPSC ring cap MUST be a power of two and >= 2 (enforced at init).
 *   - Exactly one thread may call push (the producer); exactly one thread
 *     may call pop (the consumer).  Violating this is undefined.
 *   - Stored items are void*; NULL is a legal value.
 *   - head/tail sit on separate cache lines to avoid false sharing.
 */
#ifndef TSDB_EXEC_REACTOR_H
#define TSDB_EXEC_REACTOR_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_SPSC_CACHELINE 64

/* ---- SPSC ring: one producer thread, one consumer thread, lock-free. -- */
typedef struct {
    _Atomic uint64_t head;    /* consumer advances; producer reads (acquire) */
    char _pad0[TSDB_SPSC_CACHELINE - sizeof(uint64_t)];
    _Atomic uint64_t tail;    /* producer advances; consumer reads (acquire) */
    char _pad1[TSDB_SPSC_CACHELINE - sizeof(uint64_t)];
    void   **slots;
    uint64_t mask;            /* cap - 1 */
    uint64_t cap;
} tsdb_spsc_ring_t;

/* Initialize with `cap` slots.  cap MUST be a power of two and >= 2.
 * Returns 0 on success, -1 on invalid cap / allocation failure. */
int  tsdb_spsc_init(tsdb_spsc_ring_t *r, uint64_t cap);

/* Release the slot array.  Does NOT free items still in the ring — the
 * caller drains first. */
void tsdb_spsc_destroy(tsdb_spsc_ring_t *r);

/* Producer-only.  Returns 1 on success, 0 if full. */
int  tsdb_spsc_push(tsdb_spsc_ring_t *r, void *item);

/* Consumer-only.  Returns 1 and stores into *out on success, 0 if empty. */
int  tsdb_spsc_pop(tsdb_spsc_ring_t *r, void **out);

/* Approximate occupancy (tail - head).  Safe to call from either end;
 * the value may be stale by the in-flight op of the other thread. */
uint64_t tsdb_spsc_size(const tsdb_spsc_ring_t *r);

/* ---- reactor: a single-threaded, single-owner event loop. ------------- */
typedef void (*tsdb_reactor_task_fn)(void *arg);

typedef struct {
    int               core_id;     /* logical index 0..ncores-1 */
    tsdb_spsc_ring_t  inbox;       /* producer(s) → this core */
    _Atomic int       running;     /* loop flag (0 stop, 1 run) */
    int               started;     /* thread alive? (owner-thread only) */
    pthread_t         thread;
} tsdb_reactor_t;

/* Create a reactor with an inbox of `inbox_cap` slots (pow2, >= 2).  Does
 * NOT start the background thread.  Returns 0 / -1. */
int  tsdb_reactor_init(tsdb_reactor_t *r, int core_id, uint64_t inbox_cap);

/* Stop the thread if running, free any undrained tasks, release the inbox. */
void tsdb_reactor_destroy(tsdb_reactor_t *r);

/* Producer side: enqueue a (fn, arg) closure.  Returns 1 on success, 0 if
 * the inbox is full. */
int  tsdb_reactor_submit(tsdb_reactor_t *r, tsdb_reactor_task_fn fn, void *arg);

/* Consumer side: drain up to `budget` tasks on the CURRENT thread, running
 * each closure.  budget <= 0 drains all currently-queued tasks.  Returns
 * the number processed.  Lets tests drive the loop deterministically
 * without spawning the background thread. */
int  tsdb_reactor_run_once(tsdb_reactor_t *r, int budget);

/* Start / stop the background reactor thread.  start() returns 0 / -1;
 * stop() joins.  After start(), submit() from another thread and the
 * reactor drains in the background.  Do not call run_once concurrently
 * with a started reactor (that would be a second consumer). */
int  tsdb_reactor_start(tsdb_reactor_t *r);
void tsdb_reactor_stop(tsdb_reactor_t *r);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_REACTOR_H */
