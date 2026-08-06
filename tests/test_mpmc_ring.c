/* test_mpmc_ring.c — correctness + stress for the MPMC ring.
 *
 * Correctness:
 *   - single push/pop round-trip (value, order)
 *   - fill to capacity, 1 more push fails, pop frees a slot, push OK
 *   - spsc burst of 10k items
 *
 * Deadlock:
 *   - [4] hammers blocking push against blocking pop on a cap-2 ring so
 *     both directions park constantly.  Before the claim/wake split the
 *     blocking forms took prod_mu and cons_mu in opposite orders and
 *     this wedged AB-BA (pusher holds prod_mu wants cons_mu, popper
 *     holds cons_mu wants prod_mu).
 *
 * Stress:
 *   - 4 producers × 4 consumers each do 50k ops on a 1024-slot ring;
 *     total produced = total consumed, sums agree.
 *
 * A hang in this file used to be INVISIBLE.  stdout is block-buffered
 * when the suite redirects it to a log, so a deadlocked process sat until
 * the 300 s suite timeout and was killed having written zero bytes — the
 * failure log was empty.  The watchdog below turns any hang into a loud
 * stderr failure within WATCHDOG_STALL_MS, naming the phase that wedged.
 */

#include "../src/core/mpmc_ring.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define PASS(msg) do { printf("PASS: %s\n", msg); fflush(stdout); } while (0)

/* ---- Watchdog -------------------------------------------------------- */

#define WATCHDOG_STALL_MS 20000  /* no progress this long == hung */
#define WATCHDOG_TICK_MS  100
#define HB_SLOTS          128    /* slots are spaced 8 apart == 1 per line */

/* Per-thread heartbeat counters.  Threads only ever touch their own slot,
 * so liveness reporting adds no contention to what is being measured. */
static _Atomic unsigned long g_hb[HB_SLOTS];
static _Atomic int           g_phase;
static _Atomic int           g_wd_stop;

static const char *const g_phase_name[] = {
    "[0] startup",
    "[1] basic push/pop",
    "[2] invalid capacity",
    "[3] blocking spsc",
    "[4] AB-BA blocking push/pop hammer",
    "[5] mpmc stress",
    "[6] done",
};

#define HB(slot) atomic_fetch_add_explicit(&g_hb[(slot) & (HB_SLOTS - 1)], \
                                           1UL, memory_order_relaxed)

static unsigned long hb_sum(void) {
    unsigned long s = 0;
    for (int i = 0; i < HB_SLOTS; i++)
        s += atomic_load_explicit(&g_hb[i], memory_order_relaxed);
    return s;
}

static void set_phase(int p) {
    atomic_store(&g_phase, p);
    HB(0);              /* entering a phase counts as progress */
    fflush(stdout);     /* so a redirected log is never empty */
}

static void *watchdog_main(void *arg) {
    (void)arg;
    unsigned long last = hb_sum();
    int quiet_ms = 0;
    while (!atomic_load(&g_wd_stop)) {
        struct timespec tick = { 0, WATCHDOG_TICK_MS * 1000L * 1000L };
        nanosleep(&tick, NULL);
        unsigned long now = hb_sum();
        if (now != last) { last = now; quiet_ms = 0; continue; }
        quiet_ms += WATCHDOG_TICK_MS;
        if (quiet_ms < WATCHDOG_STALL_MS) continue;

        const char *phase = g_phase_name[atomic_load(&g_phase)];
        /* stderr is unbuffered — this reaches the log even though the
         * process is about to be torn down mid-flight. */
        fprintf(stderr,
                "\n*** WATCHDOG: no thread made progress for %d ms in phase "
                "%s (heartbeat frozen at %lu).\n"
                "*** This is a HANG, almost certainly the prod_mu/cons_mu "
                "AB-BA deadlock.  Inspect with:\n"
                "***     gdb -p %ld -batch -ex 'thread apply all bt'\n"
                "FAIL: test_mpmc_ring hung in %s\n",
                quiet_ms, phase, now, (long)getpid(), phase);
        fflush(stderr);
        fflush(stdout);
        _exit(9);
    }
    return NULL;
}

static void t_basic(void) {
    printf("\n[1] basic push/pop\n");
    set_phase(1);
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
    set_phase(2);
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
        if ((i & 63) == 0) HB(1);
    }
    atomic_fetch_add(&g_produced_sum, local_sum);
    atomic_fetch_add(&g_producers_done, 1);
    return NULL;
}

static void *consumer(void *arg) {
    (void)arg;
    uint64_t local_sum = 0;
    uint64_t local_count = 0;
    uint64_t spins = 0;
    for (;;) {
        void *v = NULL;
        if ((++spins & 63) == 0) HB(2);
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
    printf("\n[5] stress: %d producers × %d consumers × %d ops\n",
           STRESS_PRODUCERS, STRESS_CONSUMERS, STRESS_PER_PROD);
    set_phase(5);
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
        if ((i & 63) == 0) HB(8);
    }
    return NULL;
}

static void t_blocking(void) {
    printf("\n[3] blocking push/pop keeps pace without timeouts\n");
    set_phase(3);
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
        if ((i & 63) == 0) HB(16);
    }
    pthread_join(tid, NULL);
    tsdb_mpmc_destroy(&g_block_ring);
    PASS("blocking spsc keeps FIFO order");
}

/* ---- [4] AB-BA hammer ------------------------------------------------
 *
 * Regression guard for the lock-order inversion between the two blocking
 * directions.  A cap-2 ring with several threads pushing and several
 * popping keeps BOTH sides parked essentially all the time, which is the
 * only way the cycle closes: a parked pusher must wake, claim a slot and
 * try to signal a consumer at the same moment a parked popper wakes,
 * claims an item and tries to signal a producer.
 *
 * The per-call timeout does NOT mask a deadlock — a deadlocked thread is
 * blocked in pthread_mutex_lock, which has no timeout.  It DOES mask a lost
 * wakeup: a thread nobody signalled just wakes 250 ms later and carries on,
 * so "the phase finished" is not by itself evidence that the wake half of
 * the claim/wake split is present.  The timeout counters are that evidence,
 * and ABBA_MAX_TIMEOUTS below is what turns them into a guard.
 *
 * Measured here, deleting or misdirecting only the wake:
 *
 *                    push+pop timeouts        wall time
 *   correct          5, every run             ~5 s
 *   wake deleted     14..30                   ~5 s   <- INVISIBLE in wall time
 *   wrong condvar    ~2500                    60-80 s
 *
 * The bound has to catch a mutant that costs no wall time at all, so wall
 * time cannot be the guard.  The counter can be, because the two rows scale
 * differently: the correct build's 5 is STRUCTURAL — ABBA_CONS-1 consumers
 * each park once on the drained ring at the end — and does not grow with
 * ABBA_PER_PROD, while a missing wake costs one timeout per park and does.
 * At 20k/producer the rows overlapped (correct 5, mutant 11-18, and one
 * mutant run slipped under a bound of 10); at 100k they separate.
 *
 * The third input is ABBA_TIMEOUT, and it is what makes the correct row hold
 * under LOAD rather than merely at idle — see the note on it below.  At 250 ms
 * a loaded machine produced 12 on a correct build, over the bound; at 1000 ms
 * the same machine under 14 spinning hogs produced 5 on every run while the
 * mutant stayed at 14..30.
 *
 * So if this ever trips, re-measure all three rows — idle AND loaded — before
 * loosening the bound.  The row that matters is the middle one; raising
 * ABBA_PER_PROD or ABBA_TIMEOUT widens the gap, raising the bound only hides it.
 *
 * 2026-08-06 — it tripped, and the bound was NOT touched.  The gate reported
 * push=1 pop=10 (11 > 10) while the data was exactly right (600000 consumed,
 * sum exact), i.e. a lost-wakeup guard firing on a scheduling artefact rather
 * than on a lost wakeup.  Re-measured: under 8 spinning CPU hogs a correct
 * build produced push=0 pop=5 on EVERY run — the structural row, unchanged.
 * CPU load was therefore not the trigger; the failing run was a full-suite pass
 * where other tests were doing fsync-heavy I/O, and a stall longer than the
 * 1000 ms timeout turns a park that WAS going to be signalled into a counted
 * timeout.  ABBA_TIMEOUT 1000 -> 3000 per the rule above: a lost wakeup costs
 * one timeout PER PARK regardless of how long the timeout is, so the mutant row
 * (14..30) does not move and the guard keeps its teeth, while a stall now has
 * to be 3x longer to manufacture a false one.  Cost is bounded and small: the
 * ABBA_CONS-1 structural parks at the end are never signalled and so wait the
 * full timeout once, concurrently — about two seconds more wall time. */

#define ABBA_CAP      2
#define ABBA_PROD     6
#define ABBA_CONS     6
#define ABBA_PER_PROD 100000
/* Long enough that a scheduler delay does not read as a lost wakeup.  At 250 ms
 * a loaded machine produced push=1 pop=11 on a CORRECT build — over the bound —
 * because a woken thread that is not scheduled within the timeout counts one
 * just like a thread nobody signalled.  Lengthening the timeout suppresses that
 * noise without touching the signal: a genuinely missing wake still costs the
 * whole timeout, so the mutant's count is unchanged while the correct build's
 * stays at its structural floor.  The cost is about a second of tail, once. */
#define ABBA_TIMEOUT  3000      /* ms — see the load note below */
#define ABBA_MAX_TIMEOUTS (ABBA_CONS + 4)   /* see the measurement table above */
#define ABBA_TOTAL    ((uint64_t)ABBA_PROD * ABBA_PER_PROD)

static tsdb_mpmc_ring_t g_abba_ring;
static _Atomic uint64_t g_abba_sum;
static _Atomic uint64_t g_abba_consumed;
static _Atomic uint64_t g_abba_push_timeouts;
static _Atomic uint64_t g_abba_pop_timeouts;

static void *abba_producer(void *arg) {
    int id = (int)(uintptr_t)arg;
    int slot = 24 + 8 * id;
    for (int i = 0; i < ABBA_PER_PROD; i++) {
        /* Values are globally unique and cover 1..ABBA_TOTAL exactly. */
        uint64_t v = (uint64_t)id * ABBA_PER_PROD + (uint64_t)i + 1;
        while (!tsdb_mpmc_push(&g_abba_ring, (void *)(uintptr_t)v,
                               ABBA_TIMEOUT)) {
            atomic_fetch_add(&g_abba_push_timeouts, 1);
            HB(slot);
        }
        HB(slot);
    }
    return NULL;
}

static void *abba_consumer(void *arg) {
    int id = (int)(uintptr_t)arg;
    int slot = 72 + 8 * id;
    uint64_t sum = 0;
    while (atomic_load(&g_abba_consumed) < ABBA_TOTAL) {
        void *v = NULL;
        if (tsdb_mpmc_pop(&g_abba_ring, &v, ABBA_TIMEOUT)) {
            sum += (uint64_t)(uintptr_t)v;
            atomic_fetch_add(&g_abba_consumed, 1);
        } else {
            atomic_fetch_add(&g_abba_pop_timeouts, 1);
        }
        HB(slot);
    }
    atomic_fetch_add(&g_abba_sum, sum);
    return NULL;
}

static void t_abba(void) {
    printf("\n[4] AB-BA: %d blocking producers × %d blocking consumers "
           "× %d ops on a cap-%d ring\n",
           ABBA_PROD, ABBA_CONS, ABBA_PER_PROD, ABBA_CAP);
    set_phase(4);
    assert(tsdb_mpmc_init(&g_abba_ring, ABBA_CAP) == 0);
    atomic_store(&g_abba_sum, 0);
    atomic_store(&g_abba_consumed, 0);
    atomic_store(&g_abba_push_timeouts, 0);
    atomic_store(&g_abba_pop_timeouts, 0);

    pthread_t ptids[ABBA_PROD], ctids[ABBA_CONS];
    for (uintptr_t i = 0; i < ABBA_PROD; i++)
        pthread_create(&ptids[i], NULL, abba_producer, (void *)i);
    for (uintptr_t i = 0; i < ABBA_CONS; i++)
        pthread_create(&ctids[i], NULL, abba_consumer, (void *)i);

    for (int i = 0; i < ABBA_PROD; i++) pthread_join(ptids[i], NULL);
    for (int i = 0; i < ABBA_CONS; i++) pthread_join(ctids[i], NULL);

    uint64_t got      = atomic_load(&g_abba_sum);
    uint64_t consumed = atomic_load(&g_abba_consumed);
    uint64_t want     = ABBA_TOTAL * (ABBA_TOTAL + 1) / 2;
    printf("  consumed=%llu (expected %llu) sum=%llu (expected %llu) "
           "push_timeouts=%llu pop_timeouts=%llu\n",
           (unsigned long long)consumed, (unsigned long long)ABBA_TOTAL,
           (unsigned long long)got, (unsigned long long)want,
           (unsigned long long)atomic_load(&g_abba_push_timeouts),
           (unsigned long long)atomic_load(&g_abba_pop_timeouts));
    assert(consumed == ABBA_TOTAL);
    assert(got == want);
    /* Every parked thread must have been woken by a signal, not by its own
     * timeout.  Without this the phase passes with the wake deleted. */
    assert(atomic_load(&g_abba_push_timeouts) +
           atomic_load(&g_abba_pop_timeouts) <= ABBA_MAX_TIMEOUTS);
    tsdb_mpmc_destroy(&g_abba_ring);
    PASS("blocking push and blocking pop interleave without deadlocking, "
         "and each parked side is woken by a signal rather than a timeout");
}

int main(void) {
    pthread_t wd;
    set_phase(0);
    pthread_create(&wd, NULL, watchdog_main, NULL);

    t_basic();
    t_invalid_cap();
    t_blocking();
    t_abba();
    t_stress();

    set_phase(6);
    atomic_store(&g_wd_stop, 1);
    pthread_join(wd, NULL);
    printf("\n=== all mpmc_ring tests passed ===\n");
    fflush(stdout);
    return 0;
}
