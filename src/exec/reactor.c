/* reactor.c — SPSC ring + per-core reactor loop.  See reactor.h. */
#define _POSIX_C_SOURCE 200809L

#include "reactor.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>

/* ---- SPSC ring ------------------------------------------------------- */

static int spsc_is_pow2(uint64_t x) { return x && (x & (x - 1)) == 0; }

int tsdb_spsc_init(tsdb_spsc_ring_t *r, uint64_t cap) {
    if (!r || cap < 2 || !spsc_is_pow2(cap)) return -1;
    r->slots = calloc(cap, sizeof(void *));
    if (!r->slots) return -1;
    atomic_store_explicit(&r->head, 0, memory_order_relaxed);
    atomic_store_explicit(&r->tail, 0, memory_order_relaxed);
    r->mask = cap - 1;
    r->cap  = cap;
    return 0;
}

void tsdb_spsc_destroy(tsdb_spsc_ring_t *r) {
    if (!r) return;
    free(r->slots);
    r->slots = NULL;
    r->mask = r->cap = 0;
}

int tsdb_spsc_push(tsdb_spsc_ring_t *r, void *item) {
    /* Producer owns `tail`; only needs an acquire view of `head` to test
     * fullness.  head/tail are monotonic so (tail - head) is occupancy. */
    uint64_t t = atomic_load_explicit(&r->tail, memory_order_relaxed);
    uint64_t h = atomic_load_explicit(&r->head, memory_order_acquire);
    if (t - h >= r->cap) return 0;                 /* full */
    r->slots[t & r->mask] = item;
    atomic_store_explicit(&r->tail, t + 1, memory_order_release);
    return 1;
}

int tsdb_spsc_pop(tsdb_spsc_ring_t *r, void **out) {
    /* Consumer owns `head`; acquire-loads `tail` so the slot store the
     * producer published before its release-store of tail is visible. */
    uint64_t h = atomic_load_explicit(&r->head, memory_order_relaxed);
    uint64_t t = atomic_load_explicit(&r->tail, memory_order_acquire);
    if (h == t) return 0;                          /* empty */
    *out = r->slots[h & r->mask];
    atomic_store_explicit(&r->head, h + 1, memory_order_release);
    return 1;
}

uint64_t tsdb_spsc_size(const tsdb_spsc_ring_t *r) {
    uint64_t t = atomic_load_explicit(&r->tail, memory_order_acquire);
    uint64_t h = atomic_load_explicit(&r->head, memory_order_acquire);
    return t - h;
}

/* ---- reactor -------------------------------------------------------- */

/* A submitted closure.  Heap-allocated per submit() and freed after the
 * reactor runs it.  Phase 0 is a skeleton, not the hot path; the wired
 * version (Phase 2) will push caller-owned task structs to avoid this
 * per-task allocation. */
typedef struct {
    tsdb_reactor_task_fn fn;
    void                *arg;
} reactor_msg_t;

int tsdb_reactor_init(tsdb_reactor_t *r, int core_id, uint64_t inbox_cap) {
    if (!r) return -1;
    memset(r, 0, sizeof(*r));
    r->core_id = core_id;
    if (tsdb_spsc_init(&r->inbox, inbox_cap) != 0) return -1;
    atomic_store_explicit(&r->running, 0, memory_order_relaxed);
    r->started = 0;
    return 0;
}

int tsdb_reactor_submit(tsdb_reactor_t *r, tsdb_reactor_task_fn fn, void *arg) {
    reactor_msg_t *m = malloc(sizeof(*m));
    if (!m) return 0;
    m->fn  = fn;
    m->arg = arg;
    if (!tsdb_spsc_push(&r->inbox, m)) {
        free(m);
        return 0;                                  /* inbox full */
    }
    return 1;
}

int tsdb_reactor_run_once(tsdb_reactor_t *r, int budget) {
    int n = 0;
    void *item;
    while ((budget <= 0 || n < budget) && tsdb_spsc_pop(&r->inbox, &item)) {
        reactor_msg_t *m = (reactor_msg_t *)item;
        if (m->fn) m->fn(m->arg);
        free(m);
        n++;
    }
    return n;
}

static void *reactor_main(void *arg) {
    tsdb_reactor_t *r = (tsdb_reactor_t *)arg;
    while (atomic_load_explicit(&r->running, memory_order_acquire)) {
        int n = tsdb_reactor_run_once(r, 256);     /* fairness budget */
        if (n == 0) {
            /* Idle: park briefly.  Phase 1 replaces this backoff with
             * proper futex/condvar parking + CPU affinity. */
            struct timespec ts = { 0, 100000 };    /* 100 µs */
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

int tsdb_reactor_start(tsdb_reactor_t *r) {
    if (!r || r->started) return -1;
    atomic_store_explicit(&r->running, 1, memory_order_release);
    if (pthread_create(&r->thread, NULL, reactor_main, r) != 0) {
        atomic_store_explicit(&r->running, 0, memory_order_release);
        return -1;
    }
    r->started = 1;
    return 0;
}

void tsdb_reactor_stop(tsdb_reactor_t *r) {
    if (!r || !r->started) return;
    atomic_store_explicit(&r->running, 0, memory_order_release);
    pthread_join(r->thread, NULL);
    r->started = 0;
}

void tsdb_reactor_destroy(tsdb_reactor_t *r) {
    if (!r) return;
    if (r->started) tsdb_reactor_stop(r);
    /* Free any tasks left undrained (not run). */
    void *item;
    while (tsdb_spsc_pop(&r->inbox, &item)) free(item);
    tsdb_spsc_destroy(&r->inbox);
}
