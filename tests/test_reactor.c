/* test_reactor.c — SPSC ring + per-core reactor (Phase 0 of shard-per-core).
 *
 * SPSC ring:
 *   [1] basic FIFO + bounded (single thread)
 *   [2] invalid capacity rejected (0, 1, non-pow2)
 *   [3] threaded SPSC correctness: 1M items, one producer thread + one
 *       consumer thread — every value received exactly once, in strict
 *       FIFO order, sums agree, ring ends empty (no loss/dup/reorder)
 *   [4] saturation at the minimum cap=2: 100k items wrap aggressively,
 *       still FIFO and complete (lock-free progress under back-pressure)
 *
 * Reactor:
 *   [5] run_once dispatch: budget cap + drain-all, tasks run FIFO
 *   [6] background thread: 10k tasks submitted from a producer thread,
 *       all run, clean stop + join
 */
#define _POSIX_C_SOURCE 200809L

#include "../src/exec/reactor.h"

#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define PASS(msg) printf("PASS: %s\n", msg)

/* ---- [1] basic FIFO + bounded -------------------------------------- */

static void t_basic(void) {
    printf("\n[1] SPSC basic push/pop + capacity\n");
    tsdb_spsc_ring_t r;
    assert(tsdb_spsc_init(&r, 4) == 0);

    assert(tsdb_spsc_push(&r, (void *)(uintptr_t)1) == 1);
    assert(tsdb_spsc_push(&r, (void *)(uintptr_t)2) == 1);
    assert(tsdb_spsc_push(&r, (void *)(uintptr_t)3) == 1);
    assert(tsdb_spsc_push(&r, (void *)(uintptr_t)4) == 1);
    assert(tsdb_spsc_size(&r) == 4);
    assert(tsdb_spsc_push(&r, (void *)(uintptr_t)5) == 0);   /* full */

    void *v = NULL;
    assert(tsdb_spsc_pop(&r, &v) == 1 && (uintptr_t)v == 1);
    assert(tsdb_spsc_pop(&r, &v) == 1 && (uintptr_t)v == 2);
    assert(tsdb_spsc_push(&r, (void *)(uintptr_t)5) == 1);
    assert(tsdb_spsc_push(&r, (void *)(uintptr_t)6) == 1);   /* refilled */

    assert(tsdb_spsc_pop(&r, &v) == 1 && (uintptr_t)v == 3);
    assert(tsdb_spsc_pop(&r, &v) == 1 && (uintptr_t)v == 4);
    assert(tsdb_spsc_pop(&r, &v) == 1 && (uintptr_t)v == 5);
    assert(tsdb_spsc_pop(&r, &v) == 1 && (uintptr_t)v == 6);
    assert(tsdb_spsc_pop(&r, &v) == 0);                      /* empty */
    assert(tsdb_spsc_size(&r) == 0);

    tsdb_spsc_destroy(&r);
    PASS("FIFO order + bounded fullness/emptiness");
}

/* ---- [2] invalid capacity ------------------------------------------ */

static void t_invalid_cap(void) {
    printf("\n[2] invalid capacity rejected\n");
    tsdb_spsc_ring_t r;
    assert(tsdb_spsc_init(&r, 0) == -1);
    assert(tsdb_spsc_init(&r, 1) == -1);    /* too small */
    assert(tsdb_spsc_init(&r, 3) == -1);    /* not pow2 */
    assert(tsdb_spsc_init(&r, 6) == -1);    /* not pow2 */
    assert(tsdb_spsc_init(&r, 2) == 0);
    tsdb_spsc_destroy(&r);
    assert(tsdb_spsc_init(&r, 1024) == 0);
    tsdb_spsc_destroy(&r);
    PASS("rejects 0, 1, and non-pow2 caps");
}

/* ---- [3]/[4] threaded SPSC correctness ----------------------------- */

typedef struct {
    tsdb_spsc_ring_t *ring;
    uint64_t          n;
} stream_arg_t;

/* Producer: push 1..n as uintptr_t, spinning while full. */
static void *stream_producer(void *a) {
    stream_arg_t *s = (stream_arg_t *)a;
    for (uint64_t i = 1; i <= s->n; i++) {
        while (!tsdb_spsc_push(s->ring, (void *)(uintptr_t)i))
            sched_yield();
    }
    return NULL;
}

/* One producer thread + consumer on the calling thread.  Asserting that
 * the consumer reads exactly 1,2,3,...,n in order proves, all at once:
 * no loss, no duplication, no reordering, and lock-free progress under
 * back-pressure (the ring repeatedly fills at small caps). */
static void spsc_stream(uint64_t cap, uint64_t n, const char *what) {
    tsdb_spsc_ring_t r;
    assert(tsdb_spsc_init(&r, cap) == 0);

    stream_arg_t s = { &r, n };
    pthread_t prod;
    assert(pthread_create(&prod, NULL, stream_producer, &s) == 0);

    uint64_t sum = 0, expected_sum = 0;
    for (uint64_t i = 1; i <= n; i++) {
        void *v = NULL;
        while (!tsdb_spsc_pop(&r, &v))
            sched_yield();
        assert((uintptr_t)v == (uintptr_t)i);   /* strict FIFO, no gaps */
        sum += (uint64_t)(uintptr_t)v;
        expected_sum += i;
    }
    pthread_join(prod, NULL);

    assert(sum == expected_sum);
    assert(tsdb_spsc_size(&r) == 0);             /* fully drained */
    void *leftover = NULL;
    assert(tsdb_spsc_pop(&r, &leftover) == 0);
    tsdb_spsc_destroy(&r);

    printf("  %s: cap=%llu n=%llu sum=%llu ok\n", what,
           (unsigned long long)cap, (unsigned long long)n,
           (unsigned long long)sum);
}

static void t_stream_1m(void) {
    printf("\n[3] threaded SPSC: 1M items, single producer + consumer\n");
    spsc_stream(1024, 1000000, "1M stream");
    PASS("1M items: exactly-once, in-order, sums agree, ring empty");
}

static void t_stream_mincap(void) {
    printf("\n[4] saturation at cap=2: 100k items, aggressive wrap\n");
    spsc_stream(2, 100000, "min-cap stream");
    PASS("min-cap: FIFO preserved + completes under constant back-pressure");
}

/* ---- [5] reactor run_once ------------------------------------------ */

static void inc_task(void *arg) { (*(int *)arg)++; }

static void t_reactor_run_once(void) {
    printf("\n[5] reactor run_once: budget cap + drain-all\n");
    tsdb_reactor_t r;
    assert(tsdb_reactor_init(&r, 0, -1, 2048) == 0);

    int counter = 0;
    for (int i = 0; i < 1000; i++)
        assert(tsdb_reactor_submit(&r, inc_task, &counter) == 1);

    assert(tsdb_reactor_run_once(&r, 10) == 10);   /* budget caps at 10 */
    assert(counter == 10);
    assert(tsdb_reactor_run_once(&r, 0) == 990);    /* drain the rest */
    assert(counter == 1000);
    assert(tsdb_reactor_run_once(&r, 0) == 0);       /* nothing left */

    tsdb_reactor_destroy(&r);
    PASS("tasks run FIFO, budget honored, drain-all empties inbox");
}

/* ---- [6] reactor background thread --------------------------------- */

static _Atomic int g_bg_counter;
static void bg_inc(void *arg) { (void)arg; atomic_fetch_add(&g_bg_counter, 1); }

static void t_reactor_thread(void) {
    printf("\n[6] reactor background thread: 10k tasks, clean shutdown\n");
    tsdb_reactor_t r;
    assert(tsdb_reactor_init(&r, 3, -1, 1024) == 0);
    atomic_store(&g_bg_counter, 0);
    assert(tsdb_reactor_start(&r) == 0);

    const int N = 10000;
    for (int i = 0; i < N; i++) {
        while (!tsdb_reactor_submit(&r, bg_inc, NULL))   /* producer = main */
            sched_yield();
    }
    /* Wait for the reactor thread to drain (deadline ~10s). */
    for (int spins = 0; atomic_load(&g_bg_counter) < N && spins < 100000; spins++) {
        struct timespec ts = { 0, 100000 };
        nanosleep(&ts, NULL);
    }
    assert(atomic_load(&g_bg_counter) == N);

    tsdb_reactor_stop(&r);          /* idempotent-safe second stop below */
    tsdb_reactor_stop(&r);
    tsdb_reactor_destroy(&r);
    printf("  ran %d tasks on the reactor thread\n", N);
    PASS("background loop drains all tasks + joins cleanly");
}

/* ---- [7] reactor pool: deterministic table→core routing ------------ */

#define POOL_CORES 4
static _Atomic long g_core_runs[POOL_CORES];
static _Atomic long g_route_mismatch;

/* Runs ON a reactor thread; arg = the core the submitter expects to own it.
 * tsdb_reactor_current() reports the core that actually ran it. */
static void route_task(void *arg) {
    int expected = (int)(intptr_t)arg;
    tsdb_reactor_t *self = tsdb_reactor_current();
    int actual = self ? self->core_id : -1;
    if (actual == expected && actual >= 0 && actual < POOL_CORES)
        atomic_fetch_add(&g_core_runs[actual], 1);
    else
        atomic_fetch_add(&g_route_mismatch, 1);
}

static void t_pool_routing(void) {
    printf("\n[7] reactor pool: deterministic table->core routing\n");
    tsdb_reactor_pool_t *pool = tsdb_reactor_pool_new(POOL_CORES, 4096);
    assert(pool && tsdb_reactor_pool_ncores(pool) == POOL_CORES);

    for (int i = 0; i < POOL_CORES; i++) atomic_store(&g_core_runs[i], 0);
    atomic_store(&g_route_mismatch, 0);

    const int NTAB = 400, PER = 50;
    long expected_per_core[POOL_CORES] = {0};
    for (int t = 0; t < NTAB; t++) {
        char name[32]; snprintf(name, sizeof(name), "lt_%d", t);
        int own = tsdb_reactor_pool_owner_index(pool, name);
        assert(own >= 0 && own < POOL_CORES);
        assert(own == tsdb_reactor_pool_owner_index(pool, name));    /* stable */
        for (int k = 0; k < PER; k++)
            while (!tsdb_reactor_pool_submit(pool, name, route_task, (void *)(intptr_t)own))
                sched_yield();
        expected_per_core[own] += PER;
    }

    long total = (long)NTAB * PER;
    for (int spins = 0; spins < 200000; spins++) {
        long done = atomic_load(&g_route_mismatch);
        for (int i = 0; i < POOL_CORES; i++) done += atomic_load(&g_core_runs[i]);
        if (done >= total) break;
        struct timespec ts = { 0, 100000 }; nanosleep(&ts, NULL);
    }

    assert(atomic_load(&g_route_mismatch) == 0);    /* every task ran on its owner core */
    long sum = 0; int nonempty = 0;
    for (int i = 0; i < POOL_CORES; i++) {
        long c = atomic_load(&g_core_runs[i]);
        assert(c == expected_per_core[i]);           /* per-core counts exact */
        sum += c; if (c > 0) nonempty++;
    }
    assert(sum == total);
    assert(nonempty >= 2);                            /* hash actually spread tables */
    printf("  routed %ld tasks, 0 mismatch, spread over %d/%d cores\n",
           sum, nonempty, POOL_CORES);

    tsdb_reactor_pool_free(pool);
    PASS("pool routes each table to hash(name)%ncores and runs it there");
}

/* ---- [8] reactor pool: lock-free MPMC inbox under many producers --- */

static tsdb_reactor_pool_t *g_mp_pool;
static _Atomic long g_mp_runs;
static void mp_task(void *arg) { (void)arg; atomic_fetch_add(&g_mp_runs, 1); }

#define MP_PRODUCERS 4
#define MP_PER 5000

static void *mp_producer(void *arg) {
    uintptr_t id = (uintptr_t)arg;
    for (int i = 0; i < MP_PER; i++) {
        char name[32];
        snprintf(name, sizeof(name), "lt_%lu", (unsigned long)((id * 131 + (unsigned)i) % 256));
        while (!tsdb_reactor_pool_submit(g_mp_pool, name, mp_task, NULL))
            sched_yield();
    }
    return NULL;
}

static void t_pool_multiproducer(void) {
    printf("\n[8] reactor pool: %d producers concurrently submitting\n", MP_PRODUCERS);
    g_mp_pool = tsdb_reactor_pool_new(POOL_CORES, 1024);
    assert(g_mp_pool);
    atomic_store(&g_mp_runs, 0);

    pthread_t prod[MP_PRODUCERS];
    for (uintptr_t i = 0; i < MP_PRODUCERS; i++)
        assert(pthread_create(&prod[i], NULL, mp_producer, (void *)i) == 0);
    for (int i = 0; i < MP_PRODUCERS; i++) pthread_join(prod[i], NULL);

    long total = (long)MP_PRODUCERS * MP_PER;
    for (int spins = 0; spins < 200000 && atomic_load(&g_mp_runs) < total; spins++) {
        struct timespec ts = { 0, 100000 }; nanosleep(&ts, NULL);
    }
    assert(atomic_load(&g_mp_runs) == total);        /* no task lost or duplicated */
    printf("  %d producers x %d submits = %ld tasks all ran (lock-free MPMC inbox)\n",
           MP_PRODUCERS, MP_PER, total);

    tsdb_reactor_pool_free(g_mp_pool);
    PASS("multi-producer routing: no loss/dup through MPMC inboxes");
}

int main(void) {
    printf("=== test_reactor ===\n");
    t_basic();
    t_invalid_cap();
    t_stream_1m();
    t_stream_mincap();
    t_reactor_run_once();
    t_reactor_thread();
    t_pool_routing();
    t_pool_multiproducer();
    printf("\n[PASS] reactor + SPSC ring + pool: all cases passed\n");
    return 0;
}
