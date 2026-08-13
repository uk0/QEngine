/* test_flush_hook_lock.c — the per-table compact_mtx must NOT be held across
 * the cluster replication hook.
 *
 * flush_and_clear_ex takes t->compact_mtx and calls flush_and_clear_locked
 * under it; that body fired db->on_replicate — a synchronous cross-node
 * WRITE_BATCH fanout that waits for a remote quorum ack (5 s deadline) — with
 * the lock still held.  compact_mtx is also what the READ path takes around
 * every tsdb_part_open (src/query/exec.c, scan_plan_build_ex: compaction swaps
 * a partition's .col then .idx under it, so a reader must not land mid-swap).
 * One slow or dead peer therefore stalled every SELECT on that table for as
 * long as the fanout took.
 *
 * The observable this test pins is exactly that coupling, with no cluster and
 * no sockets: register an on_replicate hook that just sleeps, make one flush
 * fire it, and time a plain SELECT of the SAME table issued while the hook is
 * provably still inside its sleep.
 *
 * Pre-fix the SELECT blocks for the remainder of the hook's sleep (measured
 * ~2000 ms below); post-fix it completes in single-digit milliseconds while the
 * hook is still sleeping.  Two independent assertions, so this is not a bare
 * wall-clock ratio:
 *   1. the SELECT's own elapsed time is under READ_BUDGET_MS, and
 *   2. g_hook_left is still 0 when the SELECT returns — i.e. the read really
 *      did overtake an in-flight hook rather than merely following it.
 * The broken build can satisfy neither: its SELECT cannot return before the
 * hook releases compact_mtx, which is the same instant the hook sets
 * g_hook_left.
 *
 * The table must own at least one ON-DISK partition before the timed read, or
 * scan_plan_build_ex's partition loop never runs and never takes compact_mtx —
 * the test would then be green either way.  Hence the explicit drain in setup,
 * which also covers TSDB_WAL_ONLY_COMMIT mode where a commit alone writes no
 * partition at all.
 */

#include "../include/tsdb.h"
#include "../src/storage/db.h"  /* tsdb_table_lock_write / tsdb_db_set_hooks —
                                 * internal API, same as test_flush_append_race */

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("%s", #cond); } while (0)

static const char *DIR_PATH = "/tmp/tsdb_test_flush_hook_lock";
static const char *TBL      = "hooklock";

/* How long the fake replication fanout blocks.  Comfortably longer than any
 * plausible SELECT over two tiny partitions, so the two outcomes are orders of
 * magnitude apart rather than adjacent. */
#define HOOK_SLEEP_MS   2000
/* Ceiling for the concurrent read.  Post-fix the read is ~1 ms; pre-fix it is
 * the hook's whole remaining sleep.  The gap is ~1000x, so this is loose on
 * purpose — it is not a throughput assertion. */
#define READ_BUDGET_MS  500
/* Guard so a hook that never fires fails loudly instead of hanging. */
#define WAIT_HOOK_MS    15000

static int g_hook_calls   = 0;   /* only the first call sleeps */
static int g_hook_entered = 0;
static int g_hook_left    = 0;

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void rm_tree(const char *p) {
    char c[512];
    snprintf(c, sizeof(c), "rm -rf %s", p);
    (void)system(c);
}

/* Stands in for cluster_on_replicate's quorum fanout.  It touches nothing —
 * the point is only that it takes a long time, exactly as a fanout to an
 * unresponsive peer does. */
static int slow_hook(void *ud, tsdb_db_t *db, const char *table_name,
                     tsdb_schema_t *schema, tsdb_memtable_t *memtable) {
    (void)ud; (void)db; (void)table_name; (void)schema; (void)memtable;
    if (__atomic_fetch_add(&g_hook_calls, 1, __ATOMIC_SEQ_CST) != 0)
        return TSDB_OK;
    __atomic_store_n(&g_hook_entered, 1, __ATOMIC_SEQ_CST);
    usleep(HOOK_SLEEP_MS * 1000);
    __atomic_store_n(&g_hook_left, 1, __ATOMIC_SEQ_CST);
    return TSDB_OK;
}

typedef struct {
    tsdb_db_t    *db;
    tsdb_table_t *tbl;
    int64_t       ts0;
} writer_arg_t;

/* Writes rows and drains them, which is what fires on_replicate.
 *
 * Two flush shapes, one per durability mode: flush-on-commit drains inside
 * tsdb_batch_commit, deferred-flush (TSDB_WAL_ONLY_COMMIT) leaves the rows in
 * the memtable and needs the explicit tsdb_table_flush.  The other mode's call
 * is a no-op (an empty memtable returns early), so the hook fires exactly once
 * either way — and g_hook_calls makes that independent of mode regardless. */
static void *writer_main(void *ud) {
    writer_arg_t *w = (writer_arg_t *)ud;

    tsdb_table_lock_write(w->tbl);
    tsdb_batch_t *b = NULL;
    if (tsdb_batch_begin(w->tbl, &b) != TSDB_OK) {
        tsdb_table_unlock_write(w->tbl);
        FAIL("batch_begin");
    }
    for (int r = 0; r < 256; r++) {
        int64_t ts = w->ts0 + (int64_t)r * 1000;
        if (tsdb_batch_row_ts(b, (tsdb_ts_t)ts) != TSDB_OK) break;
        (void)tsdb_batch_row_f64(b, 1, (double)r);
        (void)tsdb_batch_row_end(b);
    }
    (void)tsdb_batch_commit(b);
    tsdb_table_unlock_write(w->tbl);

    (void)tsdb_table_flush(w->db, TBL);
    return NULL;
}

int main(void) {
    rm_tree(DIR_PATH);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(DIR_PATH, &db));

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, TBL, cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, TBL, &tbl));

    /* Seed an on-disk partition BEFORE the hook exists, so the timed read has
     * a real tsdb_part_open to do — that open is the only place a reader takes
     * compact_mtx. */
    {
        tsdb_table_lock_write(tbl);
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(tbl, &b));
        for (int r = 0; r < 256; r++) {
            OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(1700000000000000000LL + (int64_t)r * 1000)));
            OK(tsdb_batch_row_f64(b, 1, 1.0));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        tsdb_table_unlock_write(tbl);
    }
    OK(tsdb_table_flush(db, TBL));   /* deferred-flush mode writes nothing until here */

    /* Baseline: with no hook registered the same query is fast.  Establishes
     * that a slow read later is the hook's doing, not the query's cost. */
    int64_t t0 = now_ms();
    {
        tsdb_result_t *res = NULL;
        OK(tsdb_query(db, "SELECT sum(v) FROM hooklock", &res));
        ASSERT(res && tsdb_result_next(res));
        tsdb_result_free(res);
    }
    int64_t baseline_ms = now_ms() - t0;

    tsdb_db_set_hooks(db, slow_hook, NULL, NULL);

    writer_arg_t wa = { db, tbl, 1700000100000000000LL };
    pthread_t th;
    ASSERT(pthread_create(&th, NULL, writer_main, &wa) == 0);

    /* Wait until the hook is provably inside its sleep, so the read below
     * really does overlap it. */
    int64_t deadline = now_ms() + WAIT_HOOK_MS;
    while (!__atomic_load_n(&g_hook_entered, __ATOMIC_SEQ_CST)) {
        if (now_ms() > deadline) FAIL("replication hook never fired");
        usleep(1000);
    }

    int64_t r0 = now_ms();
    {
        tsdb_result_t *res = NULL;
        OK(tsdb_query(db, "SELECT sum(v) FROM hooklock", &res));
        ASSERT(res && tsdb_result_next(res));
        tsdb_result_free(res);
    }
    int64_t read_ms   = now_ms() - r0;
    int     left_when = __atomic_load_n(&g_hook_left, __ATOMIC_SEQ_CST);

    pthread_join(th, NULL);

    printf("=== test_flush_hook_lock ===\n");
    printf("  hook sleep            : %d ms\n", HOOK_SLEEP_MS);
    printf("  baseline SELECT       : %lld ms (no hook in flight)\n",
           (long long)baseline_ms);
    printf("  SELECT during hook    : %lld ms (budget %d ms)\n",
           (long long)read_ms, READ_BUDGET_MS);
    printf("  hook still in flight  : %s\n", left_when ? "no" : "yes");

    if (read_ms >= READ_BUDGET_MS)
        FAIL("SELECT blocked %lld ms behind the replication hook "
             "(compact_mtx held across the hook)", (long long)read_ms);
    if (left_when)
        FAIL("hook had already finished when the SELECT returned — the read "
             "did not overtake it");

    printf("[PASS] replication hook does not block reads of the same table\n");

    tsdb_close(db);
    rm_tree(DIR_PATH);
    return 0;
}
