/* reactor.h — lock-free SPSC ring + per-core reactor + reactor pool.
 *
 * Foundation for the ScyllaDB-style shard-per-core architecture.  Goal:
 * every table is owned by exactly one core whose single-threaded reactor
 * mutates its memtable/WAL/flush with NO locks, because that reactor is the
 * sole accessor.  Cross-thread hand-off is by lock-free message passing, not
 * shared mutable state under a mutex.
 *
 * Three layers:
 *
 *   1. tsdb_spsc_ring_t — bounded lock-free ring for ONE producer + ONE
 *      consumer (two monotonic counters, acquire/release, no CAS).  The
 *      zero-contention primitive for a future pinned-IO-thread → core path.
 *
 *   2. tsdb_reactor_t — a single-threaded event loop bound (optionally) to
 *      one core.  Its inbox is a lock-free MPMC ring (Vyukov) so any number
 *      of front-end I/O threads can submit work to a core without a lock.
 *      The reactor is the single consumer; it runs each closure on its own
 *      thread.
 *
 *   3. tsdb_reactor_pool_t — N reactors, one per core, each pinned.  A table
 *      name hashes to its owning core (hash(name) % ncores); pool_submit
 *      routes a closure to that core's inbox.  This is the routing layer the
 *      write/query paths will sit on top of (Phase 2+).
 *
 * Everything here is lock-free on the hot path: SPSC (no CAS), MPMC inbox
 * (Vyukov CAS, wait-free uncontended), and stateless hash routing.
 */
#ifndef TSDB_EXEC_REACTOR_H
#define TSDB_EXEC_REACTOR_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#include "../core/mpmc_ring.h"

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

/* cap MUST be a power of two and >= 2.  Returns 0 / -1. */
int  tsdb_spsc_init(tsdb_spsc_ring_t *r, uint64_t cap);
void tsdb_spsc_destroy(tsdb_spsc_ring_t *r);
int  tsdb_spsc_push(tsdb_spsc_ring_t *r, void *item);   /* producer-only; 1 ok, 0 full */
int  tsdb_spsc_pop(tsdb_spsc_ring_t *r, void **out);    /* consumer-only; 1 ok, 0 empty */
uint64_t tsdb_spsc_size(const tsdb_spsc_ring_t *r);

/* ---- reactor: a single-threaded, single-owner event loop. ------------- */
typedef void (*tsdb_reactor_task_fn)(void *arg);

typedef struct {
    int               core_id;     /* logical index 0..ncores-1 */
    int               cpu;         /* CPU to pin to, or -1 for no affinity */
    tsdb_mpmc_ring_t  inbox;       /* lock-free MPMC: any producer → this core */
    _Atomic int       running;
    int               started;     /* owner-thread bookkeeping */
    pthread_t         thread;
} tsdb_reactor_t;

/* inbox_cap: pow2, >= 2.  cpu: CPU id to pin the thread to (Linux), or -1. */
int  tsdb_reactor_init(tsdb_reactor_t *r, int core_id, int cpu, uint64_t inbox_cap);
void tsdb_reactor_destroy(tsdb_reactor_t *r);

/* Producer side (any thread): enqueue a closure.  1 ok, 0 if inbox full. */
int  tsdb_reactor_submit(tsdb_reactor_t *r, tsdb_reactor_task_fn fn, void *arg);

/* Consumer side: drain up to `budget` tasks on the CURRENT thread (budget
 * <= 0 drains all queued).  Returns the count processed.  For deterministic
 * tests without the background thread. */
int  tsdb_reactor_run_once(tsdb_reactor_t *r, int budget);

/* The reactor draining on the CURRENT thread (set on entry to run_once /
 * the background loop), or NULL if this thread is not a reactor.  Lets a
 * task closure learn which core owns it. */
tsdb_reactor_t *tsdb_reactor_current(void);

/* Start / stop the background reactor thread (pins to r->cpu on Linux). */
int  tsdb_reactor_start(tsdb_reactor_t *r);
void tsdb_reactor_stop(tsdb_reactor_t *r);

/* ---- reactor pool: N cores, table→core ownership. --------------------- */
typedef struct {
    tsdb_reactor_t *reactors;
    int             ncores;
} tsdb_reactor_pool_t;

/* Create a pool of `ncores` reactors (clamped to >= 1) each with an inbox of
 * `inbox_cap` slots, start their threads, and pin reactor i to CPU i on
 * Linux.  Returns NULL on failure. */
tsdb_reactor_pool_t *tsdb_reactor_pool_new(int ncores, uint64_t inbox_cap);

/* Stop + join all reactor threads and free the pool. */
void tsdb_reactor_pool_free(tsdb_reactor_pool_t *pool);

int  tsdb_reactor_pool_ncores(const tsdb_reactor_pool_t *pool);

/* Deterministic owner index for a table name: hash(name) % ncores.  Stateless
 * — any thread can compute it with no lock and no shared lookup. */
int  tsdb_reactor_pool_owner_index(const tsdb_reactor_pool_t *pool, const char *name);

/* The reactor that owns `name`. */
tsdb_reactor_t *tsdb_reactor_pool_owner(tsdb_reactor_pool_t *pool, const char *name);

/* Route a closure to the core that owns `name`.  1 ok, 0 if that inbox full. */
int  tsdb_reactor_pool_submit(tsdb_reactor_pool_t *pool, const char *name,
                              tsdb_reactor_task_fn fn, void *arg);

/* ---- synchronous cross-thread call (the Phase 2 write/query bridge) ---- */

/* Submit fn(arg) to reactor `r` and BLOCK the calling thread until the reactor
 * has run it ON its own (owner) thread, then return.  Returns 0 on success,
 * -1 if the work could not be enqueued.  The closure mutates the core's owned
 * state with no data-path lock (single owner); pass inputs + outputs through
 * `arg`.  Only the completion handoff parks the caller — the data path stays
 * lock-free.  Do NOT call from the reactor's own thread (would self-deadlock). */
int  tsdb_reactor_call(tsdb_reactor_t *r, tsdb_reactor_task_fn fn, void *arg);

/* tsdb_reactor_call routed to the core that owns `name`. */
int  tsdb_reactor_pool_call(tsdb_reactor_pool_t *pool, const char *name,
                            tsdb_reactor_task_fn fn, void *arg);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_REACTOR_H */
