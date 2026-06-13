/* test_enospc.c — disk-full / write-failure durability contract.
 *
 * tsdb has no WAL replay (the WAL is vestigial — see wal.c: tsdb_wal_replay
 * has zero callers and only a 4-byte commit marker is ever appended).
 * Durability instead comes from tsdb_part_flush_ex on every batch_commit:
 * rows are "acked" once the partition flush publishes the ts.idx manifest
 * via atomic temp+rename.  flush_and_clear_locked only clears the memtable
 * and truncates the WAL when that flush returns TSDB_OK.
 *
 * This test forces the partition flush to fail (by making the data/table/
 * partition dir read-only, so col_writer_open's fopen / mkdir / idx
 * temp+rename hit EACCES — the same clean errno path ENOSPC takes) and
 * asserts:
 *   (a) batch_commit returns a clean error (no crash / abort),
 *   (b) data written BEFORE the failure is still fully readable,
 *   (c) the failed-flush rows linger in the memtable and a graceful close
 *       drains them exactly once (no loss, no duplication),
 *   (d) the data survives close + reopen (partition reload),
 *   (e) a FAILED flush leaves the partition .col byte-identical — the
 *       col_writer_abort / failed-close rollback removes the orphan block a
 *       partial append would otherwise leak (regression guard for the fix in
 *       src/storage/part.c).
 *
 * Skips gracefully (exit 0) if run as root, since root bypasses the
 * read-only permission check and the injection would not fire.
 */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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
        /* restore perms so rm_rf can descend even after we chmod'd things */
        chmod(q, 0700);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* Commit one batch of `n` rows starting at base_ts/base_v.  Returns the
 * batch_commit (or earlier) rc — does NOT abort on failure so the caller
 * can assert on the error. */
static int try_insert(tsdb_db_t *db, const char *tbl, int n,
                      int64_t base_ts, int64_t base_v) {
    tsdb_table_t *t = NULL;
    int rc = tsdb_open_table(db, tbl, &t);
    if (rc != TSDB_OK) return rc;
    tsdb_batch_t *b = NULL;
    rc = tsdb_batch_begin(t, &b);
    if (rc != TSDB_OK) return rc;
    for (int i = 0; i < n; i++) {
        rc = tsdb_batch_row_ts(b, base_ts + (int64_t)i);
        if (rc != TSDB_OK) { tsdb_batch_discard(b); return rc; }
        rc = tsdb_batch_row_i64(b, 1, base_v + (int64_t)i);
        if (rc != TSDB_OK) { tsdb_batch_discard(b); return rc; }
        rc = tsdb_batch_row_end(b);
        if (rc != TSDB_OK) { tsdb_batch_discard(b); return rc; }
    }
    return tsdb_batch_commit(b);   /* may flush -> may fail cleanly */
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

static off_t file_size(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? st.st_size : -1;
}

int main(void) {
    if (geteuid() == 0) {
        printf("test_enospc: running as root — read-only injection cannot fire, "
               "SKIP (covered by the Docker loopback test).\n");
        return 0;
    }

    const char *dir = "/tmp/tsdb_test_enospc";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    /* ---- Phase 1: write N rows on one day, commit, verify durable. ---- */
    const int64_t DAY1 = 1735689600LL * 1000000000LL; /* 2025-01-01 UTC */
    const int N1 = 500;
    OK(try_insert(db, "t", N1, DAY1, 100));
    int pre = count_rows(db, "SELECT v FROM t");
    printf("[1] committed N1=%d, count=%d\n", N1, pre);
    ASSERT(pre == N1);

    /* ---- Phase 2: make the TABLE dir read-only, attempt another commit. ----
     * Partition dirs live at <data_dir>/<table>/<YYYYMMDD>, so chmod the
     * table dir (not the data root).  The next commit targets a DIFFERENT
     * day, so the flush must mkdir a brand-new partition dir under a 0500
     * table dir -> EACCES — the same clean errno path an ENOSPC mid-flush
     * takes (col_writer_open's fopen / tsdb_mkdir_p both fail with errno). */
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", dir, "t");
    if (chmod(tbl_dir, 0500) != 0) FAIL("chmod 0500 on table dir failed");

    const int64_t DAY2 = DAY1 + 86400LL * 1000000000LL;
    const int N2 = 500;
    int rc_fail = try_insert(db, "t", N2, DAY2, 200);
    printf("[2] commit under read-only table dir returned rc=%d (%s)\n",
           rc_fail, tsdb_errstr(rc_fail));
    ASSERT(rc_fail != TSDB_OK);   /* clean error, no crash/abort */

    /* ---- Phase 3: no corruption — prior data intact, failed rows linger. ----
     * On a failed flush, flush_and_clear_locked does NOT clear the memtable
     * (rc != TSDB_OK), so the N2 rows survive in RAM and stay visible to a
     * live SELECT.  They are NOT yet durable — that's the safe behaviour
     * (no silent loss; the next successful commit re-flushes them).  The
     * critical correctness property is that the EARLIER durable N1 rows are
     * untouched and nothing is corrupted: count == N1 + N2, never < N1,
     * never garbage. */
    int mid = count_rows(db, "SELECT v FROM t");
    printf("[3] after failed flush, live count=%d (durable N1=%d retained-in-mem N2=%d)\n",
           mid, N1, N2);
    ASSERT(mid >= N1);            /* prior durable data never lost */
    ASSERT(mid == N1 + N2);       /* failed-flush rows retained in memtable */

    /* ---- Phase 4: graceful close drains the retained memtable. ----
     * tsdb_close() flushes pending memtable rows, so once writes are possible
     * again a clean close+reopen surfaces N1 + N2 — the failed-flush rows are
     * NOT lost, they were retried on close.  (The un-acked-rows-lost boundary
     * applies only to a hard CRASH with no graceful close, since there is no
     * WAL replay; that path is covered by the Docker SIGKILL recovery test.)
     * The correctness property here: exactly N1 + N2, no duplication, no
     * corruption. */
    if (chmod(tbl_dir, 0700) != 0) FAIL("chmod 0700 on table dir failed");
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    int durable = count_rows(db, "SELECT v FROM t");
    printf("[4] after restore + graceful close + reopen, count=%d (expect %d)\n",
           durable, N1 + N2);
    ASSERT(durable == N1 + N2);   /* close drained the retained rows; none lost/dup'd */

    /* ---- Phase 5: table not wedged — fresh writes still succeed durably. ---- */
    const int64_t DAY3 = DAY2 + 86400LL * 1000000000LL;
    const int N3 = 300;
    OK(try_insert(db, "t", N3, DAY3, 5000));
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    int post = count_rows(db, "SELECT v FROM t");
    printf("[5] after fresh write + reopen, count=%d (expect %d)\n",
           post, N1 + N2 + N3);
    ASSERT(post == N1 + N2 + N3);

    /* ---- Phase 6: regression guard for the partial-flush .col rollback. ----
     * Fail an APPEND into an EXISTING partition: blocking the partition dir
     * lets col_writer_open's fopen("ab") on the existing .col succeed (and a
     * block gets appended + fflush'd), but col_writer_close's idx temp+rename
     * fails.  Pre-fix, the appended block stayed in the .col as an orphan with
     * no idx entry — dead bytes that accumulate under sustained disk-full
     * retries (consuming the very space that's scarce).  The col_writer_abort
     * / failed-close rollback now truncate the .col back to its pre-flush
     * length, so a FAILED flush leaves the partition byte-identical.
     *
     * Deterministic check: the v.col file size must be unchanged across the
     * failed append. */
    tsdb_table_t *t6 = NULL;
    OK(tsdb_open_table(db, "t", &t6));   /* ensure 20250101 exists on disk */
    char vcol[4096];
    snprintf(vcol, sizeof(vcol), "%s/t/20250101/v.col", dir);
    off_t sz_before = file_size(vcol);
    printf("[6a] v.col size before failed append = %lld\n", (long long)sz_before);
    ASSERT(sz_before > 0);               /* partition really is on disk */

    char p1_dir[4096];
    snprintf(p1_dir, sizeof(p1_dir), "%s/t/20250101", dir);
    const int64_t DAY1b = DAY1 + 5000;   /* same day as N1, distinct ts */
    const int N4 = 300;
    if (chmod(p1_dir, 0500) != 0) FAIL("chmod 0500 on partition dir failed");
    int rc_app = try_insert(db, "t", N4, DAY1b, 90000);   /* append -> flush fails */
    if (chmod(p1_dir, 0700) != 0) FAIL("chmod 0700 on partition dir failed");
    off_t sz_after = file_size(vcol);
    printf("[6b] failed append rc=%d (%s); v.col size after = %lld (expect == %lld)\n",
           rc_app, tsdb_errstr(rc_app), (long long)sz_after, (long long)sz_before);
    ASSERT(rc_app != TSDB_OK);            /* clean error */
    ASSERT(sz_after == sz_before);        /* FIX: failed flush left .col byte-identical */

    /* The failed rows linger in the memtable; a graceful close flushes them
     * exactly once.  Verify no loss and no duplication after reopen. */
    OK(tsdb_open_table(db, "t", &t6));
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(dir, &db));
    int after = count_rows(db, "SELECT v FROM t");
    int n4_rows = count_rows(db, "SELECT v FROM t WHERE v >= 90000 AND v < 90300");
    printf("[6c] after close+reopen: total=%d (expect %d), N4-range=%d (expect %d)\n",
           after, N1 + N2 + N3 + N4, n4_rows, N4);
    ASSERT(after == N1 + N2 + N3 + N4);   /* drained once, no loss, no dup */
    ASSERT(n4_rows == N4);

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== ENOSPC / write-failure durability TESTS PASSED ===\n");
    return 0;
}
