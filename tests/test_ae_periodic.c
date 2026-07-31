/* test_ae_periodic.c — anti-entropy has to actually run.
 *
 * THE GAP.  tsdb_resync_startup_thread slept 5 s, swept every table once, and
 * returned.  That single pass was the whole of row-level anti-entropy, while
 * the replication path leans on "anti-entropy will heal it" in at least four
 * places that a running peer reaches every day:
 *
 *   - TSDB_REPLICATION_QUORUM=0 (the deliberate best-effort default) acks a
 *     write before any peer has it;
 *   - replica.c skips a gossip-DEAD peer outright — "it resyncs via
 *     anti-entropy when it rejoins";
 *   - a fanout worker that exhausts MAX_ATTEMPTS gives up — "anti-entropy
 *     heals the gap";
 *   - a SCHEMA_SYNC or WRITE_BATCH the peer refused is simply counted.
 *
 * For a peer that stays UP, none of those ever got a second look: the one
 * sweep had already happened, at t+5 s, before most of them could occur.
 *
 * WHAT THIS PINS (single process, no cluster — cluster_get(db) is NULL so a
 * sweep is the dir walk plus a no-op per table, which is exactly the part
 * under test: does the LOOP run):
 *
 *   [1] the sweep repeats on a period;
 *   [2] TSDB_ANTIENTROPY_PERIOD_MS=0 restores the old one-shot behaviour
 *       exactly — the thread performs one sweep and EXITS;
 *   [3] the shipped default is periodic (not 0) and is the 30 s tick this
 *       codebase already uses for delete-watermark re-assert;
 *   [4] the first sweep still waits out TSDB_ANTIENTROPY_DELAY_MS, so the
 *       startup timing operators depend on is unchanged;
 *   [5] one sweep is bounded by AE_RESYNC_BUDGET_MS per table — the wiring
 *       number, whose enforcement is pinned by tests/test_ae_candidates.c [9].
 *
 * A sweep's real cost against live peers is not measurable in one process;
 * that belongs to the cluster.
 */
#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/server/metrics.h"

#include <dirent.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* tsdb_antientropy_period_ms / _budget_ms / tsdb_cluster_resync_all_tables come
 * from tsdb_cluster.h.  The thread entry point has never been in a header — the
 * node main declares it at its use site the same way. */
extern void *tsdb_resync_startup_thread(void *ud);

static int g_fail = 0;

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
    fprintf(stderr, "FATAL %s:%d rc=%d (%s)\n", __FILE__, __LINE__, _r,   \
            tsdb_errstr(_r)); exit(1); } } while (0)

#define TDIR "/tmp/tsdb_test_ae_periodic"

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* Read one counter out of the Prometheus rendering.  Every metric is preceded
 * by its "# HELP" line, so the sample line always starts after a newline.
 * Absent == 0, which is also what the pre-fix tree reports for a counter it
 * never increments. */
static uint64_t metric(const char *name) {
    size_t len = 0;
    char *text = tsdb_metrics_render(&len);
    if (!text) return 0;
    uint64_t v = 0;
    char needle[160];
    snprintf(needle, sizeof(needle), "\n%s ", name);
    const char *hit = strstr(text, needle);
    if (hit) v = strtoull(hit + strlen(needle), NULL, 10);
    free(text);
    return v;
}

static void msleep(long ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static long mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int main(void) {
    printf("=== test_ae_periodic ===\n");
    tsdb_metrics_init();
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t1", cols, 2, "ts"));
    OK(tsdb_create_table(db, "t2", cols, 2, "ts"));

    /* ── [3] the shipped default ─────────────────────────────────────────── */
    printf("\n[3] the default is periodic, on the 30 s tick\n");
    {
        unsetenv("TSDB_ANTIENTROPY_PERIOD_MS");
        long d = tsdb_antientropy_period_ms();
        printf("  default period = %ld ms\n", d);
        CHECK(d > 0, "row anti-entropy is periodic OUT OF THE BOX — a cluster "
                     "that never sets an env var still re-converges");
        CHECK(d == 30000,
              "and the period is the 30 s tick delwm re-assert already uses");
    }

    /* ── [5] the per-table wall budget the sweep is bounded by ───────────── */
    printf("\n[5] one table's candidate loop is wall-bounded\n");
    {
        long b = tsdb_antientropy_budget_ms();
        printf("  AE_RESYNC_BUDGET_MS = %ld\n", b);
        CHECK(b == 30000,
              "the production driver passes AE_RESYNC_BUDGET_MS (30 s/table); "
              "test_ae_candidates [9] pins that the loop honours it");
    }

    /* ── [1] the sweep repeats ───────────────────────────────────────────── */
    printf("\n[1] the sweep repeats on its period\n");
    {
        setenv("TSDB_ANTIENTROPY_DELAY_MS", "60", 1);
        setenv("TSDB_ANTIENTROPY_PERIOD_MS", "120", 1);

        uint64_t before = metric("qengine_antientropy_sweeps_total");
        pthread_t th;
        if (pthread_create(&th, NULL, tsdb_resync_startup_thread, db) != 0) {
            fprintf(stderr, "FATAL: pthread_create\n");
            return 1;
        }
        /* 60 ms delay + 5 periods of 120 ms = 660 ms; give it 1.2 s and
         * require at least 3 so the assertion cannot pass on timing luck in
         * either direction. */
        msleep(1200);
        uint64_t after = metric("qengine_antientropy_sweeps_total");
        printf("  sweeps: %llu -> %llu over 1.2 s at a 120 ms period\n",
               (unsigned long long)before, (unsigned long long)after);
        CHECK(after - before >= 3,
              "the sweep ran again and again — not once, five seconds after "
              "boot, forever");
        pthread_cancel(th);
        pthread_join(th, NULL);
    }

    /* ── [4] the first sweep still waits out the startup delay ───────────── */
    printf("\n[4] the first sweep still honours TSDB_ANTIENTROPY_DELAY_MS\n");
    {
        setenv("TSDB_ANTIENTROPY_DELAY_MS", "400", 1);
        setenv("TSDB_ANTIENTROPY_PERIOD_MS", "0", 1);

        uint64_t before = metric("qengine_antientropy_sweeps_total");
        long t0 = mono_ms();
        pthread_t th;
        if (pthread_create(&th, NULL, tsdb_resync_startup_thread, db) != 0) {
            fprintf(stderr, "FATAL: pthread_create\n");
            return 1;
        }
        msleep(200);
        uint64_t mid = metric("qengine_antientropy_sweeps_total");
        CHECK(mid == before,
              "nothing swept 200 ms in, with a 400 ms delay configured");

        /* ── [2] period 0 is the old one-shot behaviour, exactly ─────────── */
        printf("\n[2] TSDB_ANTIENTROPY_PERIOD_MS=0 is the old one-shot thread\n");
        void *ret = NULL;
        pthread_join(th, &ret);           /* must TERMINATE, not loop */
        long elapsed = mono_ms() - t0;
        uint64_t after = metric("qengine_antientropy_sweeps_total");
        printf("  thread exited after %ld ms, sweeps %llu -> %llu\n",
               elapsed, (unsigned long long)before, (unsigned long long)after);
        CHECK(after - before == 1, "exactly one sweep");
        CHECK(elapsed >= 400, "and it happened after the configured delay");
    }

    unsetenv("TSDB_ANTIENTROPY_DELAY_MS");
    unsetenv("TSDB_ANTIENTROPY_PERIOD_MS");
    tsdb_close(db);
    rm_rf(TDIR);

    printf("\n=== test_ae_periodic %s ===\n", g_fail ? "FAILED" : "PASSED");
    return g_fail ? 1 : 0;
}
