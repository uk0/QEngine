/* test_mpmc_ring.c — correctness + stress for the MPMC ring.
 *
 * Correctness:
 *   - single push/pop round-trip (value, order)
 *   - fill to capacity, 1 more push fails, pop frees a slot, push OK
 *   - spsc burst of 10k items
 *
 * Stress:
 *   - 4 producers × 4 consumers each do 50k ops on a 1024-slot ring;
 *     total produced = total consumed, sums agree.
 */

#include "../src/core/mpmc_ring.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PASS(msg) printf("PASS: %s\n", msg)

static void t_basic(void) {
    printf("\n[1] basic push/pop\n");
    tsdb_mpmc_ring_t r;
    assert(tsdb_mpmc_init(&r, 4) == 0);

    assert(tsdb_mpmc_push_nb(&r, (void *)(uintptr_t)1) == 1);
    assert(tsdb_mpmc_push_nb(&r, (void *)(uintptr_t)2) == 1);
    assert(tsdb_mpmc_push_nb(&r, (void *)(uintptr_t)3) == 1);
    assert(tsdb_mpmc_push_nb(&r, (void *)(uintptr_t)4) == 1);
    assert(tsdb_mpmc_push_nb(&r, (void *)(uintptr_t)5) == 0); /* full */

    void *v = NULL;
    assert(tsdb_mpmc_pop_nb(&r, &v) == 1 && (uintptr_t)v == 1);
    assert(tsdb_mpmc_pop_nb(&r, &v) == 1 && (uintptr_t)v == 2);
    assert(tsdb_mpmc_push_nb(&r, (void *)(uintptr_t)5) == 1);
    assert(tsdb_mpmc_push_nb(&r, (void *)(uintptr_t)6) == 1);

    assert(tsdb_mpmc_pop_nb(&r, &v) == 1 && (uintptr_t)v == 3);
    assert(tsdb_mpmc_pop_nb(&r, &v) == 1 && (uintptr_t)v == 4);
    assert(tsdb_mpmc_pop_nb(&r, &v) == 1 && (uintptr_t)v == 5);
    assert(tsdb_mpmc_pop_nb(&r, &v) == 1 && (uintptr_t)v == 6);
    assert(tsdb_mpmc_pop_nb(&r, &v) == 0); /* empty */
    tsdb_mpmc_destroy(&r);
    PASS("push/pop FIFO + capacity");
}

static void t_invalid_cap(void) {
    printf("\n[2] invalid capacity rejected\n");
    tsdb_mpmc_ring_t r;
    assert(tsdb_mpmc_init(&r, 0) == -1);
    assert(tsdb_mpmc_init(&r, 1) == -1);    /* too small */
    assert(tsdb_mpmc_init(&r, 3) == -1);    /* not pow2 */
    assert(tsdb_mpmc_init(&r, 1024) == 0);
    tsdb_mpmc_destroy(&r);
    PASS("rejects non-pow2 and < 2");
}

/* ---- Stress --------------------------------------------------------- */

#define STRESS_PRODUCERS 4
#define STRESS_CONSUMERS 4
#define STRESS_PER_PROD  50000
#define STRESS_RING_CAP  1024

static tsdb_mpmc_ring_t   g_ring;
static _Atomic uint64_t   g_produced_sum;
static _Atomic uint64_t   g_consumed_sum;
static _Atomic uint64_t   g_consumed_count;
static _Atomic int        g_producers_done;

static void *producer(void *arg) {
    uintptr_t tid = (uintptr_t)arg;
    uint64_t local_sum = 0;
    for (int i = 0; i < STRESS_PER_PROD; i++) {
        uint64_t v = (tid << 32) | (uint64_t)(i + 1);
        /* Backoff-spin until there's room. */
        while (!tsdb_mpmc_push_nb(&g_ring, (void *)(uintptr_t)v)) {
            sched_yield();
        }
        local_sum += v;
    }
    atomic_fetch_add(&g_produced_sum, local_sum);
    atomic_fetch_add(&g_producers_done, 1);
    return NULL;
}

static void *consumer(void *arg) {
    (void)arg;
    uint64_t local_sum = 0;
    uint64_t local_count = 0;
    for (;;) {
        void *v = NULL;
        if (tsdb_mpmc_pop_nb(&g_ring, &v)) {
            local_sum   += (uint64_t)(uintptr_t)v;
            local_count += 1;
            continue;
        }
        /* No item right now — check termination. */
        if (atomic_load(&g_producers_done) == STRESS_PRODUCERS) {
            /* Drain race: one more pass. */
            void *drain = NULL;
            if (tsdb_mpmc_pop_nb(&g_ring, &drain)) {
                local_sum   += (uint64_t)(uintptr_t)drain;
                local_count += 1;
                continue;
            }
            break;
        }
        sched_yield();
    }
    atomic_fetch_add(&g_consumed_sum,   local_sum);
    atomic_fetch_add(&g_consumed_count, local_count);
    return NULL;
}

static void t_stress(void) {
    printf("\n[3] stress: %d producers × %d consumers × %d ops\n",
           STRESS_PRODUCERS, STRESS_CONSUMERS, STRESS_PER_PROD);
    assert(tsdb_mpmc_init(&g_ring, STRESS_RING_CAP) == 0);
    atomic_store(&g_produced_sum, 0);
    atomic_store(&g_consumed_sum, 0);
    atomic_store(&g_consumed_count, 0);
    atomic_store(&g_producers_done, 0);

    pthread_t ptids[STRESS_PRODUCERS], ctids[STRESS_CONSUMERS];
    for (uintptr_t i = 0; i < STRESS_PRODUCERS; i++)
        pthread_create(&ptids[i], NULL, producer, (void *)(i + 1));
    for (int i = 0; i < STRESS_CONSUMERS; i++)
        pthread_create(&ctids[i], NULL, consumer, NULL);

    for (int i = 0; i < STRESS_PRODUCERS; i++) pthread_join(ptids[i], NULL);
    for (int i = 0; i < STRESS_CONSUMERS; i++) pthread_join(ctids[i], NULL);

    uint64_t ps = atomic_load(&g_produced_sum);
    uint64_t cs = atomic_load(&g_consumed_sum);
    uint64_t cc = atomic_load(&g_consumed_count);
    uint64_t expected_count = (uint64_t)STRESS_PRODUCERS * STRESS_PER_PROD;
    printf("  produced_sum=%llu consumed_sum=%llu count=%llu (expected %llu)\n",
           (unsigned long long)ps, (unsigned long long)cs,
           (unsigned long long)cc, (unsigned long long)expected_count);
    assert(cc == expected_count);
    assert(cs == ps);
    tsdb_mpmc_destroy(&g_ring);
    PASS("sums match, counts match, no items lost or duplicated");
}

/* Blocking variants: single producer pushes N, single consumer pops N
 * with the blocking API.  Should complete even when producer runs
 * slightly ahead of consumer and fills the ring. */
static tsdb_mpmc_ring_t g_block_ring;
static const int BLOCK_N = 20000;

static void *block_producer(void *arg) {
    (void)arg;
    for (int i = 1; i <= BLOCK_N; i++) {
        int ok = tsdb_mpmc_push(&g_block_ring, (void *)(uintptr_t)i, 1000);
        if (!ok) {
            fprintf(stderr, "block push timed out at %d\n", i);
            abort();
        }
    }
    return NULL;
}

static void t_blocking(void) {
    printf("\n[4] blocking push/pop keeps pace without timeouts\n");
    assert(tsdb_mpmc_init(&g_block_ring, 64) == 0);

    pthread_t tid;
    pthread_create(&tid, NULL, block_producer, NULL);

    /* Consumer on main thread. */
    uint64_t expected = 0;
    for (int i = 1; i <= BLOCK_N; i++) {
        void *v = NULL;
        int ok = tsdb_mpmc_pop(&g_block_ring, &v, 1000);
        assert(ok);
        assert((uintptr_t)v == (uintptr_t)i);
        expected += (uint64_t)i;
    }
    pthread_join(tid, NULL);
    tsdb_mpmc_destroy(&g_block_ring);
    PASS("blocking spsc keeps FIFO order");
}

int main(void) {
    t_basic();
    t_invalid_cap();
    t_blocking();
    t_stress();
    printf("\n=== all mpmc_ring tests passed ===\n");
    return 0;
}
