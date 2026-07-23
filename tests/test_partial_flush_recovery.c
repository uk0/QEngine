/* test_partial_flush_recovery.c — a crash mid-flush that publishes the non-ts
 * columns but not ts must NOT lose or tear the batch on recovery.
 *
 * THE BUG (pre-fix): tsdb_part_flush_ex2 publishes non-ts columns FIRST and ts
 * LAST (ts is the reader-visibility marker), stamping EVERY column's idx with
 * the flush high-water seq.  tsdb_part_max_seq then takes the MAX checkpoint
 * across columns.  A crash after val publishes (val.idx.max_seq = S) but before
 * ts publishes leaves the recovery cutoff = S, so redo_recover_table SKIPS the
 * WAL records at seq <= S — the whole batch is dropped from ts even though the
 * WAL still holds it.  Result: acked rows vanish (count drops), and val is left
 * with published blocks ts never gets → torn (val reads error / mis-pair).
 *
 * Reproduction: TSDB_TEST_CRASH_BEFORE_TS=1 makes the flush return before the
 * ts column, i.e. exactly "val published, ts not, WAL intact".  The child
 * writes N rows (< block_points → one partition, no auto-flush), forces that
 * faulted flush, then _exit(0) (no clean close).  The parent reopens.
 *
 * Asserts:
 *   1. reopen after the partial-flush crash recovers ALL N rows (not 0/partial).
 *   2. a subsequent CLEAN flush keeps ts/val consistent (no orphan-val dup) and
 *      every value is correctly paired with its ts.
 *   3. a second reopen sees the same consistent N (durable, re-readable).
 */

#include "tsdb.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/* Internal explicit-flush entry (declared here like test_replica_backoff does
 * for its static helpers — avoids pulling the whole storage header). */
extern int tsdb_db_flush_all(tsdb_db_t *db);

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

#define DAY1 (1735689600LL * 1000000000LL)   /* 2025-01-01 UTC ns */

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

static void make_table(const char *dir) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) OK(rc);
    tsdb_close(db);
}

static void write_rows(tsdb_db_t *db, int n, int per_commit) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    int i = 0;
    while (i < n) {
        int m = (n - i < per_commit) ? (n - i) : per_commit;
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int k = 0; k < m; k++) {
            int idx = i + k;
            OK(tsdb_batch_row_ts(b, DAY1 + (int64_t)idx));
            OK(tsdb_batch_row_i64(b, 1, (int64_t)idx));   /* v == row index */
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        i += m;
    }
}

/* count SELECT v rows, and verify each v == (ts - DAY1) (no mis-pairing).
 * *out_wrong = number of mis-paired cells. */
static int count_verify(tsdb_db_t *db, int *out_wrong) {
    tsdb_result_t *r = NULL;
    int n = 0, wrong = 0;
    if (tsdb_query(db, "SELECT ts, v FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            int64_t v  = tsdb_result_i64(r, 1);
            if (v != ts - DAY1) wrong++;
            n++;
        }
        tsdb_result_free(r);
    }
    if (out_wrong) *out_wrong = wrong;
    return n;
}

int main(void) {
    printf("=== test_partial_flush_recovery ===\n");
    const char *dir = "/tmp/tsdb_test_partial_flush";
    rm_rf(dir);
    make_table(dir);

    const int N = 5000;   /* < block_points (8192): one partition, no auto-flush */

    /* Child: write N under budget, then a flush that crashes after val / before
     * ts, then _exit(0) — the memtable dies; val.col + WAL survive. */
    pid_t pid = fork();
    if (pid < 0) { printf("fork unavailable, SKIP\n"); rm_rf(dir); return 0; }
    if (pid == 0) {
        setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) _exit(2);
        write_rows(db, N, 4);
        setenv("TSDB_TEST_CRASH_BEFORE_TS", "1", 1);
        tsdb_db_flush_all(db);          /* publishes val, returns before ts */
        _exit(0);                       /* crash: no tsdb_close */
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
        FAIL("crash child failed (status=%d)", status);

    unsetenv("TSDB_TEST_CRASH_BEFORE_TS");
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);

    /* [1] Reopen: recovery must replay the WAL tail the partial flush did NOT
     * make durable — all N rows, not 0. */
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    int wrong1 = 0;
    int c1 = count_verify(db, &wrong1);
    printf("[1] reopen after partial-flush crash: count=%d (expect %d) wrong=%d\n",
           c1, N, wrong1);
    ASSERT(c1 == N);        /* buggy code loses the batch → 0 */
    ASSERT(wrong1 == 0);    /* and never mis-pairs a value */

    /* [2] A clean flush now must not resurrect the orphan val blocks as a
     * torn tail: ts/val stay consistent, values still correct. */
    OK(tsdb_db_flush_all(db));
    int wrong2 = 0;
    int c2 = count_verify(db, &wrong2);
    printf("[2] after clean re-flush: count=%d (expect %d) wrong=%d\n", c2, N, wrong2);
    ASSERT(c2 == N);
    ASSERT(wrong2 == 0);
    tsdb_close(db);

    /* [3] Reopen the re-flushed partition: still N, still correctly paired. */
    OK(tsdb_open(dir, &db));
    int wrong3 = 0;
    int c3 = count_verify(db, &wrong3);
    printf("[3] reopen re-flushed partition: count=%d (expect %d) wrong=%d\n",
           c3, N, wrong3);
    ASSERT(c3 == N);
    ASSERT(wrong3 == 0);

    /* [4] Append M MORE rows (into the same partition, later ts) so the fresh
     * flush blocks have DIFFERENT boundaries than any orphan val block the
     * partial flush left behind, then flush + reopen.  Total must be N+M with
     * every value paired — catches an orphan-val tail mis-matching by
     * (ts_min,count). */
    {
        const int M = 5000;   /* N+M = 10000 > block_points → multi-block */
        tsdb_table_t *t = NULL;
        OK(tsdb_open_table(db, "t", &t));
        int i = 0;
        while (i < M) {
            int m = (M - i < 7) ? (M - i) : 7;
            tsdb_batch_t *b = NULL;
            OK(tsdb_batch_begin(t, &b));
            for (int k = 0; k < m; k++) {
                int idx = N + i + k;   /* ts continues after the first N */
                OK(tsdb_batch_row_ts(b, DAY1 + (int64_t)idx));
                OK(tsdb_batch_row_i64(b, 1, (int64_t)idx));
                OK(tsdb_batch_row_end(b));
            }
            OK(tsdb_batch_commit(b));
            i += m;
        }
        OK(tsdb_db_flush_all(db));
        int wrong4 = 0, c4 = count_verify(db, &wrong4);
        printf("[4] +%d rows then flush: count=%d (expect %d) wrong=%d\n",
               M, c4, N + M, wrong4);
        ASSERT(c4 == N + M);
        ASSERT(wrong4 == 0);
        tsdb_close(db);
        OK(tsdb_open(dir, &db));
        int wrong5 = 0, c5 = count_verify(db, &wrong5);
        printf("[5] reopen after append: count=%d (expect %d) wrong=%d\n",
               c5, N + M, wrong5);
        ASSERT(c5 == N + M);
        ASSERT(wrong5 == 0);
    }
    tsdb_close(db);

    rm_rf(dir);
    printf("\n=== test_partial_flush_recovery PASSED ===\n");
    return 0;
}
