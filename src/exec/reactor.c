/* reactor.c — SPSC ring + per-core reactor + reactor pool.  See reactor.h. */
#if defined(__linux__)
#  define _GNU_SOURCE
#else
#  define _POSIX_C_SOURCE 200809L
#endif

#include "reactor.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sched.h>
#include <stdatomic.h>

/* ---- SPSC ring (lock-free, single-producer / single-consumer) -------- */

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
    uint64_t t = atomic_load_explicit(&r->tail, memory_order_relaxed);
    uint64_t h = atomic_load_explicit(&r->head, memory_order_acquire);
    if (t - h >= r->cap) return 0;                 /* full */
    r->slots[t & r->mask] = item;
    atomic_store_explicit(&r->tail, t + 1, memory_order_release);
    return 1;
}

int tsdb_spsc_pop(tsdb_spsc_ring_t *r, void **out) {
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

/* A submitted closure.  Heap-allocated per submit(), freed after the reactor
 * runs it.  Phase 0/1 skeleton; the wired hot path (Phase 2) will push
 * caller-owned task structs to avoid this per-task allocation. */
typedef struct {
    tsdb_reactor_task_fn fn;
    void                *arg;
} reactor_msg_t;

int tsdb_reactor_init(tsdb_reactor_t *r, int core_id, int cpu, uint64_t inbox_cap) {
    if (!r) return -1;
    memset(r, 0, sizeof(*r));
    r->core_id = core_id;
    r->cpu     = cpu;
    if (tsdb_mpmc_init(&r->inbox, inbox_cap) != 0) return -1;
    atomic_store_explicit(&r->running, 0, memory_order_relaxed);
    r->started = 0;
    return 0;
}

int tsdb_reactor_submit(tsdb_reactor_t *r, tsdb_reactor_task_fn fn, void *arg) {
    reactor_msg_t *m = malloc(sizeof(*m));
    if (!m) return 0;
    m->fn  = fn;
    m->arg = arg;
    if (!tsdb_mpmc_push_nb(&r->inbox, m)) {        /* lock-free MPMC enqueue */
        free(m);
        return 0;                                  /* inbox full */
    }
    return 1;
}

static __thread tsdb_reactor_t *t_current_reactor = NULL;

tsdb_reactor_t *tsdb_reactor_current(void) { return t_current_reactor; }

int tsdb_reactor_run_once(tsdb_reactor_t *r, int budget) {
    t_current_reactor = r;
    int n = 0;
    void *item;
    while ((budget <= 0 || n < budget) && tsdb_mpmc_pop_nb(&r->inbox, &item)) {
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
            /* Idle backoff.  Phase 2 replaces this with futex parking. */
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
#if defined(__linux__)
    if (r->cpu >= 0) {                             /* best-effort core pinning */
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(r->cpu, &set);
        pthread_setaffinity_np(r->thread, sizeof(set), &set);
    }
#endif
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
    void *item;
    while (tsdb_mpmc_pop_nb(&r->inbox, &item)) free(item);   /* drop undrained */
    tsdb_mpmc_destroy(&r->inbox);
}

/* ---- reactor pool: table → core ownership --------------------------- */

/* FNV-1a 64-bit — the same family used for data-dir / cluster routing, so
 * ownership is a stateless function of the table name (no shared map). */
static uint64_t reactor_fnv1a(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    for (; *s; s++) {
        h ^= (uint64_t)(unsigned char)*s;
        h *= 1099511628211ULL;
    }
    return h;
}

tsdb_reactor_pool_t *tsdb_reactor_pool_new(int ncores, uint64_t inbox_cap) {
    if (ncores < 1) ncores = 1;
    tsdb_reactor_pool_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;
    p->reactors = calloc((size_t)ncores, sizeof(tsdb_reactor_t));
    if (!p->reactors) { free(p); return NULL; }
    p->ncores = ncores;

    for (int i = 0; i < ncores; i++) {
        if (tsdb_reactor_init(&p->reactors[i], i, i, inbox_cap) != 0 ||
            tsdb_reactor_start(&p->reactors[i]) != 0) {
            tsdb_reactor_destroy(&p->reactors[i]);          /* this one */
            for (int j = 0; j < i; j++) tsdb_reactor_destroy(&p->reactors[j]);
            free(p->reactors);
            free(p);
            return NULL;
        }
    }
    return p;
}

void tsdb_reactor_pool_free(tsdb_reactor_pool_t *pool) {
    if (!pool) return;
    for (int i = 0; i < pool->ncores; i++) tsdb_reactor_destroy(&pool->reactors[i]);
    free(pool->reactors);
    free(pool);
}

int tsdb_reactor_pool_ncores(const tsdb_reactor_pool_t *pool) {
    return pool ? pool->ncores : 0;
}

int tsdb_reactor_pool_owner_index(const tsdb_reactor_pool_t *pool, const char *name) {
    if (!pool || pool->ncores <= 0) return 0;
    return (int)(reactor_fnv1a(name) % (uint64_t)pool->ncores);
}

tsdb_reactor_t *tsdb_reactor_pool_owner(tsdb_reactor_pool_t *pool, const char *name) {
    if (!pool) return NULL;
    return &pool->reactors[tsdb_reactor_pool_owner_index(pool, name)];
}

int tsdb_reactor_pool_submit(tsdb_reactor_pool_t *pool, const char *name,
                             tsdb_reactor_task_fn fn, void *arg) {
    if (!pool) return 0;
    return tsdb_reactor_submit(tsdb_reactor_pool_owner(pool, name), fn, arg);
}
