/* test_flush_append_race.c — memory-safety gate for flush vs. un-serialised
 * append on the same table (task #162).
 *
 * Backstory: the wire WRITE_BATCH handler (src/server/server.c) used to
 * serialise only on the server's own per-name lock pool, while the cluster
 * RPC receiver, the influx path and the anti-entropy re-pull serialise on
 * the table's batch_mu.  The two domains overlap on one table, so a commit
 * flush in one domain ran concurrently with appends from the other.
 * tsdb_part_flush_ex snapshotted nrows, malloc'd its sorted-permutation
 * buffer from that snapshot, then tsdb_memtable_sorted_indices filled
 * indices for the row count it observed under its own lock — writing past
 * the buffer whenever an append landed in between.  On the lvm1 cluster
 * this surfaced as glibc "malloc(): mismatching next->prev_size" on
 * cnode-4 during "[anti-entropy] middle-gap; truncate + full re-pull"
 * repairs concurrent with live writes.
 *
 * The fix is two-sided: server.c joins the batch_mu domain, and the flush
 * sizes the permutation buffer by the memtable's row capacity so even a
 * caller that violates the lock contract cannot make the walk scribble
 * past it.  This test gates the second half: thread A commit-flushes in
 * the batch_mu domain while thread B deliberately appends WITHOUT the
 * table lock (the pre-fix server behaviour).  Row accounting is undefined
 * under that interleave by design — the assertion here is purely that the
 * process stays memory-safe (run under ASan to make violations fatal) and
 * the table remains readable afterwards.
 */

#include "../include/tsdb.h"
#include "../src/storage/db.h"  /* tsdb_table_lock_write / unlock — internal API */

#include <dirent.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("%s", #cond); } while (0)

#define A_BATCHES   400      /* per-row commit-flushes (batch_mu domain) */
#define A_ROWS      700      /* rows per A batch — partial-fill flushes  */
#define B_APPENDS   8000     /* small bulk appends, NO table lock        */
#define B_ROWS      64

static const char *DIR_PATH = "/tmp/tsdb_test_flush_append_race";

static void rm_tree(const char *p) {
    char c[512];
    snprintf(c, sizeof(c), "rm -rf %s", p);
    (void)system(c);
}

typedef struct {
    tsdb_table_t *tbl;
    int           id;
} writer_arg_t;

/* Thread A — the batch_mu domain (anti-entropy re-pull / RPC receiver
 * shape): per-row inserts, commit per batch, table lock held across the
 * whole begin→commit bracket.  Every commit flushes a partial memtable,
 * opening the snapshot→sorted_indices window in tsdb_part_flush_ex. */
static void *writer_locked(void *ud) {
    writer_arg_t *w = (writer_arg_t *)ud;
    int64_t ts = 1000000000LL;
    for (int b = 0; b < A_BATCHES; b++) {
        tsdb_table_lock_write(w->tbl);
        tsdb_batch_t *bt = NULL;
        if (tsdb_batch_begin(w->tbl, &bt) != TSDB_OK) {
            tsdb_table_unlock_write(w->tbl);
            FAIL("batch_begin");
        }
        for (int r = 0; r < A_ROWS; r++) {
            ts += 1000;
            if (tsdb_batch_row_ts(bt, (tsdb_ts_t)ts) != TSDB_OK) break;
            (void)tsdb_batch_row_f64(bt, 1, (double)r);
            (void)tsdb_batch_row_end(bt);
        }
        (void)tsdb_batch_commit(bt);   /* FULL during a race is expected */
        tsdb_table_unlock_write(w->tbl);
    }
    return NULL;
}

/* Thread B — the pre-fix wire-handler shape: bulk append + commit with NO
 * table lock.  Violates the documented contract on purpose so the flush
 * hardening (capacity-sized permutation buffer) is what keeps the process
 * alive.  TSDB_ERR_FULL replies are expected and ignored — the loader
 * retries in production too. */
static void *writer_unlocked(void *ud) {
    writer_arg_t *w = (writer_arg_t *)ud;
    int64_t ts_arr[B_ROWS];
    double  v_arr[B_ROWS];
    int64_t ts = 2000000000LL;
    for (int b = 0; b < B_APPENDS; b++) {
        for (int r = 0; r < B_ROWS; r++) {
            ts += 1000;
            ts_arr[r] = ts;
            v_arr[r]  = (double)b;
        }
        tsdb_batch_t *bt = NULL;
        if (tsdb_batch_begin(w->tbl, &bt) != TSDB_OK) FAIL("batch_begin");
        const void *cols[1]  = { v_arr };
        int         types[1] = { TSDB_TYPE_FLOAT64 };
        int rc = tsdb_batch_append_bulk(bt, ts_arr, cols, types, 1, B_ROWS);
        if (rc == TSDB_OK) (void)tsdb_batch_commit(bt);
        else               tsdb_batch_discard(bt);
    }
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
    OK(tsdb_create_table(db, "race_t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "race_t", &tbl));

    writer_arg_t wa = { tbl, 0 }, wb = { tbl, 1 };
    pthread_t ta, tb;
    ASSERT(pthread_create(&ta, NULL, writer_locked,   &wa) == 0);
    ASSERT(pthread_create(&tb, NULL, writer_unlocked, &wb) == 0);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);

    /* Memory safety is the gate (ASan aborts on violation).  Beyond that,
     * the table must still be coherently readable. */
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db, "SELECT count(*) FROM race_t", &res));
    ASSERT(res && tsdb_result_next(res));
    int64_t n = tsdb_result_i64(res, 0);
    ASSERT(n > 0);
    tsdb_result_free(res);

    printf("=== test_flush_append_race ===\n");
    printf("  A: %d locked per-row batches × %d rows; B: %d unlocked bulk appends × %d rows\n",
           A_BATCHES, A_ROWS, B_APPENDS, B_ROWS);
    printf("  survived concurrent flush/append; count(*)=%lld readable\n",
           (long long)n);
    printf("[PASS] flush vs un-serialised append stays memory-safe\n");

    tsdb_close(db);
    rm_tree(DIR_PATH);
    return 0;
}
