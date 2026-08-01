/* test_batch_rollback.c — a batch whose commit fails must not leave its rows
 * in the memtable, and a retry must land them EXACTLY ONCE (never twice).
 *
 * The bug (TSDB_WAL_ONLY_COMMIT, the mode production runs):
 *   tsdb_batch_commit appends the batch's rows to the memtable, then
 *   redo_log_locked() appends+fsyncs them to the WAL.  On a WAL failure
 *   tsdb_wal_append_durable rolls back the on-disk record, but the ROWS stay in
 *   the memtable, un-acked.  The cluster receiver (rpc_apply_write_batch) then
 *   sends ERR and the sender fanout retries the SAME payload, which re-appends
 *   the batch a SECOND time.  The next successful commit writes BOTH copies to
 *   the WAL — silent duplication that anti-entropy (best_count <= local ->
 *   UP_TO_DATE) never repairs.
 *
 * The fix makes tsdb_batch_discard transactional: it truncates the rows this
 * batch appended back out of the memtable, so the receiver's `commit-failed ->
 * discard` else-branch re-appends a retry from a clean boundary instead of
 * doubling.  The rollback lives in discard, NOT commit, on purpose: a refused
 * commit still RETAINS its rows so a standalone writer drains them on the next
 * flush/close (no silent data loss — test_enospc pins this in BOTH modes).  A
 * cluster receiver discards on ERR; a standalone writer does not.
 *
 * All scenarios are in-process and deterministic — no cluster, no crash:
 *
 *   [wal_only] WAL failure injected via TSDB_WAL_FAIL_APPEND.  A failed commit
 *              (1) returns an error and RETAINS its rows; (2) the receiver's
 *              discard() rolls them back out of the memtable; (3) a re-commit of
 *              the same rows lands them exactly once.
 *
 *   [default]  Flush failure injected via a read-only table dir (like
 *              test_enospc).  The default (flush-on-commit) path also retains
 *              failed-flush rows to drain on the next flush/close — so commit
 *              does NOT roll back — but the receiver's discard() MUST, so a
 *              retry still lands once.  Skips as root (perm injection can't fire).
 *
 *   [discard]  A batch discarded WITHOUT a commit must drop its rows too, and
 *              must leave rows a PRIOR batch put in the memtable untouched.
 *
 * Row visibility: a query scans the memtable, so rows appended via row_end are
 * visible to SELECT before (and after) commit — that is how each rollback is
 * asserted at the query level, no memtable internals needed.
 */

#include "tsdb.h"
#include "../src/storage/memtable.h"   /* tsdb_memtable_truncate_to (unit-level) */
#include "../src/storage/schema.h"
#include "../src/cluster/rpc.h"        /* the receiver apply path (test seam) */

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

/* Day-aligned base timestamps so batches on different "days" flush to distinct
 * partition dirs (matters for the read-only-dir injection: a NEW partition must
 * be mkdir'd, which fails under a 0500 table dir). */
#define DAY1 (1735689600LL * 1000000000LL)               /* 2025-01-01 UTC */
#define DAY2 (DAY1 + 86400LL * 1000000000LL)             /* 2025-01-02 UTC */

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        chmod(q, 0700);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* Count all rows (memtable + partitions) via a full scan. */
static int count_all(tsdb_db_t *db) {
    tsdb_result_t *r = NULL;
    int n = 0;
    if (tsdb_query(db, "SELECT v FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

static void make_table(tsdb_db_t *db) {
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) OK(rc);
}

/* Begin a batch and append n rows (ts=base+i, v=base+i) WITHOUT committing.
 * The rows land in the memtable (visible to SELECT); the caller commits or
 * discards. */
static tsdb_batch_t *append_uncommitted(tsdb_table_t *t, int64_t base, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, base + i));
        OK(tsdb_batch_row_i64(b, 1, base + i));
        OK(tsdb_batch_row_end(b));
    }
    return b;
}

/* Append n rows and commit them; aborts the test on any error. */
static void commit_rows(tsdb_table_t *t, int64_t base, int n) {
    tsdb_batch_t *b = append_uncommitted(t, base, n);
    OK(tsdb_batch_commit(b));
}

/* ---- [wal_only] WAL-failure rollback ------------------------------------ */
static void test_wal_only(void) {
    const char *dir = "/tmp/tsdb_test_batch_rollback_walonly";
    rm_rf(dir);
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    unsetenv("TSDB_WAL_FAIL_APPEND");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    make_table(db);
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));

    /* Batch A: 10 rows committed.  In wal_only mode they sit in the memtable
     * (deferred flush) + WAL. */
    commit_rows(t, DAY1, 10);
    ASSERT(count_all(db) == 10);

    /* Batch B: 5 rows appended, NOT yet committed — visible in the memtable. */
    tsdb_batch_t *B = append_uncommitted(t, DAY2, 5);
    ASSERT(count_all(db) == 15);

    /* Inject a WAL failure and commit B -> redo_log_locked fails. */
    setenv("TSDB_WAL_FAIL_APPEND", "1", 1);
    int rc = tsdb_batch_commit(B);
    unsetenv("TSDB_WAL_FAIL_APPEND");
    ASSERT(rc != TSDB_OK);                 /* (1) commit returns an error */
    /* A refused wal_only commit RETAINS its rows in the memtable — a standalone
     * writer drains them on the next flush/close (the no-silent-loss contract
     * test_enospc phase 11 pins).  The rollback that stops a CLUSTER retry from
     * doubling them lives in discard, which the receiver's ERR-ack branch
     * (rpc_apply_write_batch) calls. */
    ASSERT(count_all(db) == 15);           /* rows retained after failed commit */

    /* Receiver flow: discard the refused batch rolls it back out of the memtable
     * so a fanout retry re-appends from a clean boundary. */
    tsdb_batch_discard(B);
    ASSERT(count_all(db) == 10);           /* (2) discard rolled the batch back */

    /* Retry the SAME 5 rows -> must land exactly once (unfixed: discard leaves
     * B's rows, so the retry doubles them to 20). */
    commit_rows(t, DAY2, 5);
    ASSERT(count_all(db) == 15);           /* (3) landed exactly once */

    /* [discard] A batch discarded without a commit drops its rows and leaves
     * the prior 15 untouched (unfixed: discard would leave count at 18). */
    tsdb_batch_t *C = append_uncommitted(t, DAY2 + 1000, 3);
    ASSERT(count_all(db) == 18);
    tsdb_batch_discard(C);
    ASSERT(count_all(db) == 15);

    /* Durability: reopen replays the WAL -> exactly the 15 committed rows,
     * no dup, no loss (the failed/discarded batches left nothing behind). */
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    ASSERT(count_all(db) == 15);
    tsdb_close(db);

    rm_rf(dir);
    unsetenv("TSDB_WAL_ONLY_COMMIT");
    printf("  [wal_only] commit-fail rollback + retry-once + discard: OK\n");
}

/* ---- [receiver] the REAL WRITE_BATCH apply path, not a hand-coded discard ---
 *
 * The unit tests above call tsdb_batch_discard directly, which proves the
 * primitive but NOT that the receiver wires it in.  A first draft of this fix
 * added truncate_to + discard-rollback and shipped them as dead code: no
 * production caller invoked discard on a COMMIT failure, so the cluster
 * duplication it claimed to fix stayed open.  This case drives
 * rpc_apply_write_batch (via the test seam) so the gate fails if that wiring is
 * ever removed. */
static void test_receiver_commit_fail_no_dup(void) {
    const char *dir = "/tmp/tsdb_test_batch_rollback_receiver";
    rm_rf(dir);
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    unsetenv("TSDB_WAL_FAIL_APPEND");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    make_table(db);                        /* table "t": ts + one i64 col "v" */

    /* Encode a 5-row WRITE_BATCH the way a peer would. */
    int64_t ts[5], v[5];
    for (int i = 0; i < 5; i++) { ts[i] = DAY2 + i * 1000000LL; v[i] = 100 + i; }
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
    const void *col_data[2] = { ts, v };
    uint8_t payload[4096];
    int plen = tsdb_rpc_encode_write_batch(payload, sizeof(payload), "t",
                                           2, col_types, 5, col_data);
    ASSERT(plen > 0);

    /* Deliver it with a WAL failure armed: the receiver's commit fails, it must
     * report ERR (0) AND leave nothing behind — the rows are rolled back. */
    setenv("TSDB_WAL_FAIL_APPEND", "1", 1);
    int ok1 = tsdb_rpc_apply_write_batch_for_test(db, payload, (uint32_t)plen);
    unsetenv("TSDB_WAL_FAIL_APPEND");
    ASSERT(ok1 == 0);                      /* receiver returns ERR */
    ASSERT(count_all(db) == 0);            /* the failed batch left NO rows */

    /* The sender retries the SAME payload.  It must land exactly once, not
     * double.  Pre-fix (discard not wired into the commit-failure branch) this
     * is 10. */
    int ok2 = tsdb_rpc_apply_write_batch_for_test(db, payload, (uint32_t)plen);
    ASSERT(ok2 == 1);                      /* retry lands */
    ASSERT(count_all(db) == 5);            /* exactly once, no dup */

    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    ASSERT(count_all(db) == 5);            /* durable: 5, no dup after replay */
    tsdb_close(db);

    rm_rf(dir);
    unsetenv("TSDB_WAL_ONLY_COMMIT");
    printf("  [receiver] real apply path: commit-fail then retry lands once: OK\n");
}

/* ---- [default] flush-failure: linger on commit, roll back on discard ----- */
static void test_default(void) {
    if (geteuid() == 0) {
        printf("  [default]  SKIP (root — read-only-dir injection cannot fire)\n");
        return;
    }
    const char *dir = "/tmp/tsdb_test_batch_rollback_default";
    rm_rf(dir);
    unsetenv("TSDB_WAL_ONLY_COMMIT");
    unsetenv("TSDB_WAL_FAIL_APPEND");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    make_table(db);
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));

    /* Batch A: 10 rows committed -> flushed to the 20250101 partition (default
     * mode flushes on commit), memtable empty. */
    commit_rows(t, DAY1, 10);
    ASSERT(count_all(db) == 10);

    /* Batch B: 5 rows on a DIFFERENT day, appended, not committed. */
    tsdb_batch_t *B = append_uncommitted(t, DAY2, 5);
    ASSERT(count_all(db) == 15);

    /* Read-only table dir: B's commit-flush must mkdir a NEW partition
     * (20250102) and hits EACCES — the clean errno path ENOSPC also takes. */
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/t", dir);
    if (chmod(tbl_dir, 0500) != 0) FAIL("chmod 0500 failed");

    int rc = tsdb_batch_commit(B);
    ASSERT(rc != TSDB_OK);                 /* (1) commit returns an error */
    /* Default mode retains failed-flush rows in the memtable to drain on the
     * next flush/close (the deliberate contract test_enospc pins) — commit does
     * NOT roll back here. */
    ASSERT(count_all(db) == 15);           /* rows RETAINED after failed commit */

    /* The receiver's else-branch discards; THAT rolls the batch back so a retry
     * cannot double it (unfixed: discard leaves count at 15). */
    tsdb_batch_discard(B);
    ASSERT(count_all(db) == 10);           /* (2) discard rolled the batch back */

    if (chmod(tbl_dir, 0700) != 0) FAIL("chmod 0700 failed");

    /* Retry the SAME 5 rows -> lands exactly once (unfixed: 15 stuck + 5 = 20). */
    commit_rows(t, DAY2, 5);
    ASSERT(count_all(db) == 15);           /* (3) landed exactly once */

    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    ASSERT(count_all(db) == 15);           /* no dup / no loss across reopen */
    tsdb_close(db);

    rm_rf(dir);
    printf("  [default]  commit-linger + discard-rollback + retry-once: OK\n");
}

/* ---- [memtable] truncate_to must rebuild the ts skip-list ---------------- */
static void push_row_mt(tsdb_memtable_t *m, int64_t ts, int64_t v) {
    OK(tsdb_memtable_row_begin(m));
    OK(tsdb_memtable_row_ts(m, (tsdb_ts_t)ts));
    OK(tsdb_memtable_row_i64(m, 1, v));
    OK(tsdb_memtable_row_end(m));
}

static void test_memtable_truncate_direct(void) {
    /* Direct memtable-layer test.  Uses OUT-OF-ORDER timestamps so the ts
     * skip-list — not the identity fast-path — drives sorted_indices.  A
     * truncate that fails to rebuild the skip-list leaves stale nodes from the
     * dropped tail, so the permutation duplicates/omits row positions — caught
     * here by (a) survivors-only + ascending after truncate, and (b) the
     * exact-permutation check after a re-append. */
    const char *dir = "/tmp/tsdb_test_batch_rollback_mt";
    rm_rf(dir);
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) FAIL("mkdir failed");

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    tsdb_schema_t *s = NULL;
    OK(tsdb_schema_create_ex(dir, "t", cols, 2, "ts", TSDB_PARTITION_DAY, 0, &s));
    tsdb_memtable_t *m = NULL;
    OK(tsdb_memtable_new(s, &m));

    /* 10 descending rows (ts 100..91, v == ts). */
    for (int i = 0; i < 10; i++) push_row_mt(m, 100 - i, 100 - i);
    ASSERT(tsdb_memtable_rows(m) == 10);
    ASSERT(!tsdb_memtable_is_sorted(m));            /* out of order */

    /* 5 more descending rows (ts 90..86), then truncate them away. */
    for (int i = 0; i < 5; i++) push_row_mt(m, 90 - i, 90 - i);
    ASSERT(tsdb_memtable_rows(m) == 15);
    OK(tsdb_memtable_truncate_to(m, 10));
    ASSERT(tsdb_memtable_rows(m) == 10);

    /* Rebuilt index must cover ONLY the surviving 10 rows, ascending, each
     * once — no stale node from the dropped tail. */
    {
        size_t idx[10];
        OK(tsdb_memtable_sorted_indices(m, idx));
        const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, 0);
        int seen[256] = {0};
        for (int i = 0; i < 10; i++) {
            ASSERT(idx[i] < 10);
            int64_t ts = ts_buf[idx[i]];
            ASSERT(ts >= 91 && ts <= 100);          /* only survivors */
            ASSERT(!seen[ts]); seen[ts] = 1;        /* no duplicate */
            if (i) ASSERT(ts_buf[idx[i - 1]] <= ts); /* ascending */
        }
    }

    /* Re-append 5 DIFFERENT rows (ts 200..196) on top of the rebuilt index.
     * The combined permutation over all 15 rows must be exact (each position
     * once) and ascending. */
    for (int i = 0; i < 5; i++) push_row_mt(m, 200 - i, 200 - i);
    ASSERT(tsdb_memtable_rows(m) == 15);
    {
        size_t idx[15];
        OK(tsdb_memtable_sorted_indices(m, idx));
        const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, 0);
        int seenpos[15] = {0};
        for (int i = 0; i < 15; i++) {
            ASSERT(idx[i] < 15);
            ASSERT(!seenpos[idx[i]]); seenpos[idx[i]] = 1;      /* a permutation */
            if (i) ASSERT(ts_buf[idx[i - 1]] <= ts_buf[idx[i]]); /* ascending */
        }
    }

    /* truncate_to(0) behaves like clear: empty, trivially sorted; guard rails. */
    OK(tsdb_memtable_truncate_to(m, 0));
    ASSERT(tsdb_memtable_rows(m) == 0);
    ASSERT(tsdb_memtable_is_sorted(m));
    ASSERT(tsdb_memtable_truncate_to(m, 1) == TSDB_ERR_INVAL);  /* target > nrows */

    tsdb_memtable_free(m);
    tsdb_schema_free(s);
    rm_rf(dir);
    printf("  [memtable] truncate_to rebuilds ts skip-list: OK\n");
}

int main(void) {
    printf("=== batch commit/discard rollback (data-loss/duplication) ===\n");
    test_memtable_truncate_direct();
    test_wal_only();
    test_default();
    test_receiver_commit_fail_no_dup();
    printf("=== BATCH ROLLBACK TESTS PASSED ===\n");
    return 0;
}
