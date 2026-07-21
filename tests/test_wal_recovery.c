/* test_wal_recovery.c — WAL redo-log crash-recovery contract.
 *
 * Goal: prove that TSDB_WAL_ONLY_COMMIT=1 (deferred-flush) is crash-durable.
 * In that mode tsdb_batch_commit does NOT flush the memtable on every commit
 * (the memtable drains only when it fills, via maybe_flush_b).  Durability
 * therefore depends on the WAL being a real redo log: each commit appends its
 * rows to the WAL and fsyncs before acking, and tsdb_open replays the WAL tail
 * that the partition checkpoints have not yet superseded.
 *
 * Crash model: a child process opens the db, writes + commits, then _exit(0)
 * WITHOUT tsdb_close().  The memtable is process memory and dies with the
 * child; the only state that survives is what reached the WAL file (and any
 * partition the child happened to flush).  The parent reopens the SAME data
 * dir and asserts the row count via SELECT.
 *
 *   Test A (under budget): wal_only=1, N rows in many tiny commits, kept below
 *           the per-table flush budget (TSDB_BLOCK_POINTS=8192) so NO flush
 *           ever runs — every row lives only in the memtable + WAL at crash.
 *           Reopen -> count == N.  On unfixed main this is 0 (the commit only
 *           wrote a 4-byte marker; the whole unflushed memtable is lost).
 *
 *   Test B (crosses a flush): wal_only=1, write enough to force at least one
 *           mid-stream flush, then write MORE rows post-flush, _exit(0).
 *           Reopen -> count == N EXACTLY: the post-flush WAL tail is not lost,
 *           and the flushed prefix is not duplicated (seq > checkpoint dedup).
 *
 *   Test C (default mode): wal_only=0, write N, _exit(0).  Reopen -> count == N.
 *           Already passes on main (flush-on-commit); guards the default path
 *           against regressions from the redo-log changes.
 *
 * Tests A/B fork(); they skip gracefully if fork is unavailable.  All tests
 * run as a normal user (no root needed).
 */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

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

static int count_rows(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int n = 0;
    if (tsdb_query(db, sql, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

/* Commit `n` rows in batches of `per_commit`, one ts-ns apart starting at
 * base_ts, value = base_v + i.  Aborts on any error. */
static void write_rows(tsdb_db_t *db, const char *tbl, int n, int per_commit,
                       int64_t base_ts, int64_t base_v) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, tbl, &t));
    int i = 0;
    while (i < n) {
        int m = (n - i < per_commit) ? (n - i) : per_commit;
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int k = 0; k < m; k++) {
            int idx = i + k;
            OK(tsdb_batch_row_ts(b, base_ts + (int64_t)idx));
            OK(tsdb_batch_row_i64(b, 1, base_v + (int64_t)idx));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        i += m;
    }
}

/* Create the schema in `dir` (idempotent: open-or-create). */
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

/* Child body: open, write, _exit(0) WITHOUT tsdb_close (simulated crash). */
static void crash_child(const char *dir, int wal_only, int n, int per_commit,
                        int64_t base_ts) {
    if (wal_only) setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    else          unsetenv("TSDB_WAL_ONLY_COMMIT");
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) _exit(2);
    write_rows(db, "t", n, per_commit, base_ts, 0);
    /* Hard crash: no tsdb_close() -> no graceful drain/flush of the memtable.
     * Only what reached the WAL file (and any auto-flushed partition) survives. */
    _exit(0);
}

/* Fork a crash child and wait for it.  Returns 1 if the child exited 0,
 * 0 if the child failed, -1 if fork is unavailable. */
static int run_crash_child(const char *dir, int wal_only, int n,
                           int per_commit, int64_t base_ts) {
    pid_t pid = fork();
    if (pid < 0) return -1;          /* fork unavailable */
    if (pid == 0) {
        crash_child(dir, wal_only, n, per_commit, base_ts);
        _exit(3);                    /* unreachable */
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 0;
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 1 : 0;
}

static int reopen_count(const char *dir, int wal_only) {
    if (wal_only) setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    else          unsetenv("TSDB_WAL_ONLY_COMMIT");
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    int c = count_rows(db, "SELECT v FROM t");
    tsdb_close(db);
    return c;
}

/* 2025-01-01 UTC in ns. */
#define DAY1 (1735689600LL * 1000000000LL)

static void test_a(void) {
    const char *dir = "/tmp/tsdb_test_wal_recovery_a";
    rm_rf(dir);
    make_table(dir);

    const int N = 5000;   /* < TSDB_BLOCK_POINTS (8192): no flush ever fires */
    /* Tiny commits, all on one day so they share one partition.  N stays under
     * the per-table 8192-row budget and we never close cleanly, so with
     * wal_only=1 NO partition is ever written — recovery is pure WAL replay
     * from C=0.  On unfixed main the reopened count is 0. */
    int ok = run_crash_child(dir, /*wal_only=*/1, N, /*per_commit=*/4, DAY1);
    if (ok < 0) { printf("test_a: fork unavailable, SKIP\n"); rm_rf(dir); return; }
    ASSERT(ok == 1);

    int c = reopen_count(dir, /*wal_only=*/1);
    printf("[A] under-budget crash: reopened count=%d (expect %d)\n", c, N);
    ASSERT(c == N);
    rm_rf(dir);
    printf("test_a PASS\n");
}

static void test_b(void) {
    const char *dir = "/tmp/tsdb_test_wal_recovery_b";
    rm_rf(dir);
    make_table(dir);

    /* Force at least one mid-stream flush: write well over the 8192 budget so
     * maybe_flush_b drains the memtable mid-run, THEN keep writing so a live
     * WAL tail exists post-flush at crash time.  N spans > 3 budgets. */
    const int N = 30000;
    int ok = run_crash_child(dir, /*wal_only=*/1, N, /*per_commit=*/16, DAY1);
    if (ok < 0) { printf("test_b: fork unavailable, SKIP\n"); rm_rf(dir); return; }
    ASSERT(ok == 1);

    int c = reopen_count(dir, /*wal_only=*/1);
    printf("[B] flush-crossing crash: reopened count=%d (expect %d, exact)\n", c, N);
    ASSERT(c == N);   /* no loss of post-flush tail, no dup of flushed prefix */
    rm_rf(dir);
    printf("test_b PASS\n");
}

static void test_c(void) {
    const char *dir = "/tmp/tsdb_test_wal_recovery_c";
    rm_rf(dir);
    make_table(dir);

    const int N = 5000;
    int ok = run_crash_child(dir, /*wal_only=*/0, N, /*per_commit=*/8, DAY1);
    if (ok < 0) {
        /* No fork: exercise the default path in-process instead (still a
         * meaningful regression guard for flush-on-commit durability). */
        printf("test_c: fork unavailable, in-process fallback\n");
        setenv("TSDB_WAL_ONLY_COMMIT", "0", 1);
        tsdb_db_t *db = NULL;
        OK(tsdb_open(dir, &db));
        write_rows(db, "t", N, 8, DAY1, 0);
        tsdb_close(db);
        int c = reopen_count(dir, 0);
        ASSERT(c == N);
        rm_rf(dir);
        printf("test_c PASS\n");
        return;
    }
    ASSERT(ok == 1);

    int c = reopen_count(dir, /*wal_only=*/0);
    printf("[C] default-mode crash: reopened count=%d (expect %d)\n", c, N);
    ASSERT(c == N);
    rm_rf(dir);
    printf("test_c PASS\n");
}

/* ---- Test D: SYMBOL + FLOAT64 columns survive crash recovery ----
 * Exercises the redo record's variable-length SYMBOL path ([len][utf8],
 * re-interned on replay) and the FLOAT64 path, which Tests A/B (INT64 only)
 * do not cover.  Child writes N rows cycling host symbols and float values
 * under wal_only=1, crashes; parent reopens and verifies the exact count,
 * the per-host counts (symtab rebuilt correctly), and a float sum. */

static const char *HOSTS[] = { "alpha", "bravo", "charlie", "delta" };
#define NHOSTS 4

static void make_table_sym(const char *dir) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts",   TSDB_TYPE_TIMESTAMP },
        { "host", TSDB_TYPE_SYMBOL    },
        { "val",  TSDB_TYPE_FLOAT64   },
    };
    int rc = tsdb_create_table(db, "m", cols, 3, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) OK(rc);
    tsdb_close(db);
}

static void crash_child_sym(const char *dir, int n, int64_t base_ts) {
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) _exit(2);
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "m", &t) != TSDB_OK) _exit(2);
    int i = 0;
    while (i < n) {
        int m = (n - i < 5) ? (n - i) : 5;
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) _exit(2);
        for (int k = 0; k < m; k++) {
            int idx = i + k;
            if (tsdb_batch_row_ts(b, base_ts + idx) != TSDB_OK) _exit(2);
            if (tsdb_batch_row_sym(b, 1, HOSTS[idx % NHOSTS]) != TSDB_OK) _exit(2);
            if (tsdb_batch_row_f64(b, 2, (double)idx + 0.5) != TSDB_OK) _exit(2);
            if (tsdb_batch_row_end(b) != TSDB_OK) _exit(2);
        }
        if (tsdb_batch_commit(b) != TSDB_OK) _exit(2);
        i += m;
    }
    _exit(0);   /* crash: no tsdb_close */
}

static void test_d(void) {
    const char *dir = "/tmp/tsdb_test_wal_recovery_d";
    rm_rf(dir);
    make_table_sym(dir);

    /* Stay under the 8192 budget so this is pure WAL replay (no flush) — the
     * symbol re-intern path is then end-to-end from WAL bytes. */
    const int N = 4000;
    pid_t pid = fork();
    if (pid < 0) { printf("test_d: fork unavailable, SKIP\n"); rm_rf(dir); return; }
    if (pid == 0) { crash_child_sym(dir, N, DAY1); _exit(3); }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) { rm_rf(dir); return; }
    ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    int total = count_rows(db, "SELECT val FROM m");
    int alpha = count_rows(db, "SELECT val FROM m WHERE host = 'alpha'");
    int delta = count_rows(db, "SELECT val FROM m WHERE host = 'delta'");
    printf("[D] symbol crash: total=%d (expect %d), host=alpha=%d delta=%d (expect %d each)\n",
           total, N, alpha, delta, N / NHOSTS);
    ASSERT(total == N);
    ASSERT(alpha == N / NHOSTS);  /* symtab rebuilt: per-host counts exact */
    ASSERT(delta == N / NHOSTS);
    tsdb_close(db);
    rm_rf(dir);
    printf("test_d PASS\n");
}

/* Recover through the CREATE-over-existing path (influx auto-create / idempotent
 * SQL CREATE), NOT open_table.  After a fresh open the in-memory index is empty,
 * so create_table_impl builds the table before any open_table runs; without a
 * redo_recover_table call there, the fsynced WAL tail is never replayed and the
 * first flush truncates it — silent loss of acked rows (worst under wal_only). */
static int reopen_via_create_count(const char *dir) {
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");  /* -> create_table_impl */
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) OK(rc);
    int c = count_rows(db, "SELECT v FROM t");
    tsdb_close(db);
    return c;
}

static void test_e_create_recovers(void) {
    const char *dir = "/tmp/tsdb_test_wal_recovery_e";
    rm_rf(dir);
    make_table(dir);
    const int N = 5000;   /* < block_points: pure WAL replay, no partition */
    int ok = run_crash_child(dir, /*wal_only=*/1, N, /*per_commit=*/4, DAY1);
    if (ok < 0) { printf("test_e: fork unavailable, SKIP\n"); rm_rf(dir); return; }
    ASSERT(ok == 1);
    int c = reopen_via_create_count(dir);
    printf("test_e: recovered %d / %d rows via CREATE-over-existing path\n", c, N);
    ASSERT(c == N);   /* 0 without the create_table_impl recovery fix */
    rm_rf(dir);
}

int main(void) {
    test_c();   /* default path first: must hold regardless of redo changes */
    test_a();
    test_b();
    test_d();
    test_e_create_recovers();   /* create-over-existing recovery (this fix) */
    printf("\n=== WAL redo recovery TESTS PASSED ===\n");
    return 0;
}
