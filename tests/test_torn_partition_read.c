/* test_torn_partition_read.c — regression for the "SELECT returns 0 rows for
 * data durably on disk" bug.
 *
 * On a cluster, an interrupted / racing anti-entropy truncate+re-pull (or a
 * crash mid-flush) can leave a partition's `<ts>.col` file shorter than the
 * block offsets its `<ts>.idx` manifest references.  Before the fix,
 * tsdb_part_open dropped the ENTIRE column on `st.st_size < max_end`, so a
 * SELECT on that table returned 0 rows even though the partition holds millions
 * of durable rows in its intact block prefix.  After the fix the per-block
 * filter recovers the durable prefix and discards only the torn tail.
 *
 * This test reproduces the on-disk condition directly (truncate the ts column
 * of a flushed partition) and asserts:
 *   - intact partition: count == full row count
 *   - torn partition:   count > 0 (durable prefix recovered, not 0)
 *
 * It exercises the same exec/scan resolution the wire SELECT uses (tsdb_query
 * on both the child table and its super-table).
 */

#include "tsdb.h"
#include "../src/catalog/stable.h"
#include "../src/storage/part.h"
#include "../src/storage/schema.h"
#include "../src/storage/memtable.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/resource.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

static int64_t q_count(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK || !r) { if (r) tsdb_result_free(r); return -1; }
    int64_t v = -1;
    if (tsdb_result_next(r)) v = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    return v;
}

/* Find the first "<table_dir>/<YYYYMMDD>/ts.col" path. Returns 1 on success. */
static int find_ts_col(const char *table_dir, char *out, size_t cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s/ts.col", table_dir, e->d_name);
        struct stat st;
        if (stat(p, &st) == 0 && S_ISREG(st.st_mode)) {
            snprintf(out, cap, "%s", p);
            found = 1;
            break;
        }
    }
    closedir(d);
    return found;
}

#define NROWS 20000  /* > block_points (8192) → multiple on-disk blocks */

/* ── fd / mmap exhaustion (EMFILE) regression ──────────────────────────────
 * tsdb_part_open opens two files per column (idx, then col) and mmaps the
 * col.  Before the fix, EVERY open/mmap failure was handled like "column
 * absent" — a bare continue — so running out of file descriptors mid-open
 * silently dropped whole columns: ts unreadable → the partition contributes
 * 0 rows with no log at all; a value column unreadable → the torn-column
 * clamp drives the durable prefix to 0.  Either way tsdb_part_open returned
 * TSDB_OK and scan_plan_build_ex skipped nothing it could SEE, so a wide
 * scan under the default ulimit answered a fraction of the true count with
 * rc=0.  These phases pin the loud-failure contract at both layers:
 *   - tsdb_part_open under EMFILE  → TSDB_ERR_IO (not OK-with-zero-columns)
 *   - SELECT count(*) under EMFILE → non-OK rc   (not a silent short count)
 */

/* Highest open fd.  RLIMIT_NOFILE bounds the fd NUMBER a new open may take,
 * so "current highest + small headroom" is the tightest limit that still
 * lets a few opens succeed before EMFILE fires mid-partition. */
static int fd_highest_open(void) {
    int mx = -1;
    for (int fd = 0; fd < 65536; fd++)
        if (fcntl(fd, F_GETFD) != -1) mx = fd;
    return mx;
}

/* Find "<table_dir>/<YYYYMMDD>" partition dir. Returns 1 on success. */
static int find_part_dir(const char *table_dir, char *out, size_t cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(out, cap, "%s/%s", table_dir, e->d_name);
        found = 1;
        break;
    }
    closedir(d);
    return found;
}

#define FDX_NCOLS 8
#define FDX_ROWS  8192   /* memtable cap: one full block per column */

/* Direct-layer phase: a wide (8-column) flushed partition must refuse to
 * open — TSDB_ERR_IO — when fd exhaustion strikes partway through its
 * column opens, instead of returning TSDB_OK with the unreachable columns
 * recorded as absent. */
static void test_fd_exhaustion_part_open(void) {
    printf("\n-- fd exhaustion: tsdb_part_open must fail loudly --\n");

    const char *root  = "/tmp/tsdb_test_fdx_part";
    const char *table = "wide8";
    rmrf(root);
    mkdir(root, 0755);
    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/%s", root, table);
    mkdir(tdir, 0755);

    tsdb_col_t cols[FDX_NCOLS] = {
        { "ts", TSDB_TYPE_TIMESTAMP }, { "v1", TSDB_TYPE_FLOAT64 },
        { "v2", TSDB_TYPE_FLOAT64 },   { "v3", TSDB_TYPE_FLOAT64 },
        { "v4", TSDB_TYPE_FLOAT64 },   { "v5", TSDB_TYPE_FLOAT64 },
        { "v6", TSDB_TYPE_FLOAT64 },   { "v7", TSDB_TYPE_FLOAT64 },
    };
    tsdb_schema_t *s = NULL;
    if (tsdb_schema_create(tdir, table, cols, FDX_NCOLS, "ts", &s) != TSDB_OK) {
        CHECK(0, "fdx: schema_create");
        return;
    }

    tsdb_memtable_t *m = NULL;
    if (tsdb_memtable_new(s, &m) != TSDB_OK) {
        CHECK(0, "fdx: memtable_new");
        tsdb_schema_free(s);
        return;
    }
    for (int i = 0; i < FDX_ROWS; i++) {
        if (tsdb_memtable_row_begin(m) != TSDB_OK) break;
        tsdb_memtable_row_ts(m, 1000000000000LL + (int64_t)i * 1000000LL);
        for (int c = 1; c < FDX_NCOLS; c++)
            tsdb_memtable_row_f64(m, c, (double)(i + c));
        tsdb_memtable_row_end(m);
    }
    int frc = tsdb_part_flush_ex2(s, m, NULL, table, 0);
    tsdb_memtable_free(m);
    if (frc != TSDB_OK) {
        CHECK(0, "fdx: flush wide partition");
        tsdb_schema_free(s);
        return;
    }

    char pdir[4096];
    if (!find_part_dir(tdir, pdir, sizeof(pdir))) {
        CHECK(0, "fdx: partition dir created");
        tsdb_schema_free(s);
        return;
    }

    /* Sanity: with normal fds the wide partition opens fine. */
    {
        tsdb_part_t *p = NULL;
        CHECK(tsdb_part_open(s, pdir, &p) == TSDB_OK, "fdx: intact wide partition opens");
        if (p) tsdb_part_close(p);
    }

    /* +4 fd slots: enough for the first few idx/col opens (each mapped
     * column KEEPS its col fd), exhausted well before all 8 columns map,
     * so an open inside tsdb_part_open fails with EMFILE mid-loop. */
    struct rlimit old, tight;
    if (getrlimit(RLIMIT_NOFILE, &old) != 0) {
        CHECK(0, "fdx: getrlimit");
        tsdb_schema_free(s);
        return;
    }
    tight = old;
    tight.rlim_cur = (rlim_t)(fd_highest_open() + 1 + 4);
    if (setrlimit(RLIMIT_NOFILE, &tight) != 0) {
        CHECK(0, "fdx: setrlimit lower");
        tsdb_schema_free(s);
        return;
    }

    tsdb_part_t *p = NULL;
    int orc = tsdb_part_open(s, pdir, &p);

    /* Restore BEFORE any assertion so every exit path leaves the process
     * with its original limit. */
    setrlimit(RLIMIT_NOFILE, &old);
    if (p) tsdb_part_close(p);

    printf("  fdx: part_open under EMFILE rc=%d (want TSDB_ERR_IO=%d)\n",
           orc, TSDB_ERR_IO);
    CHECK(orc == TSDB_ERR_IO,
          "fdx: fd-exhausted tsdb_part_open returns TSDB_ERR_IO, not TSDB_OK");

    tsdb_schema_free(s);
    rmrf(root);
}

/* Query-layer phase: a scan that cannot open all its partitions' columns
 * must surface an error, never rc=0 with a fraction of the rows. */
static void test_fd_exhaustion_query(void) {
    printf("\n-- fd exhaustion: SELECT must error, not answer short --\n");

    const char *dir = "/tmp/tsdb_test_fdx_query";
    rmrf(dir);

    enum { FDX_DAYS = 4, FDX_PER_DAY = 6000 };
    const int64_t day_ns = 86400LL * 1000000000LL;

    /* Phase A: 8-column table, one flushed partition per day. */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { CHECK(0, "fdx: open db"); return; }
        tsdb_result_t *r = NULL;
        int crc = tsdb_query(db,
                   "CREATE TABLE wideq (ts TIMESTAMP, v1 FLOAT64, v2 FLOAT64, "
                   "v3 FLOAT64, v4 FLOAT64, v5 FLOAT64, v6 FLOAT64, v7 FLOAT64) "
                   "TIMESTAMP(ts)",
                   &r);
        if (r) { tsdb_result_free(r); r = NULL; }
        if (crc != TSDB_OK) {
            CHECK(0, "fdx: create wideq");
            tsdb_close(db);
            return;
        }
        tsdb_table_t *tbl = NULL;
        if (tsdb_open_table(db, "wideq", &tbl) != TSDB_OK) {
            CHECK(0, "fdx: open wideq");
            tsdb_close(db);
            return;
        }
        for (int d = 0; d < FDX_DAYS; d++) {
            tsdb_batch_t *b = NULL;
            tsdb_batch_begin(tbl, &b);
            for (int i = 0; i < FDX_PER_DAY; i++) {
                tsdb_batch_row_ts(b, 1000000000000LL + (int64_t)d * day_ns
                                     + (int64_t)i * 1000000LL);
                for (int c = 1; c < FDX_NCOLS; c++)
                    tsdb_batch_row_f64(b, c, (double)i);
                tsdb_batch_row_end(b);
            }
            tsdb_batch_commit(b);
            /* Flush per day so each day's rows land in their own on-disk
             * partition — the scan must span several partitions for the
             * mid-plan exhaustion to be visible. */
            tsdb_table_flush(db, "wideq");
        }
        tsdb_close(db);
    }

    /* Phase B: reopen; with normal fds the count is exact. */
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { CHECK(0, "fdx: reopen db"); return; }
    int64_t base = q_count(db, "SELECT count(*) FROM wideq");
    printf("  fdx: baseline count=%lld (want %d)\n",
           (long long)base, FDX_DAYS * FDX_PER_DAY);
    CHECK(base == FDX_DAYS * FDX_PER_DAY, "fdx: baseline count exact");

    /* Phase C: tighten the fd limit and re-run the same count.  +6 slots is
     * far below the 32 col fds the 4-partition scan keeps open, so partition
     * opens fail with EMFILE mid-plan. */
    struct rlimit old, tight;
    if (getrlimit(RLIMIT_NOFILE, &old) != 0) {
        CHECK(0, "fdx: getrlimit");
        tsdb_close(db);
        return;
    }
    tight = old;
    tight.rlim_cur = (rlim_t)(fd_highest_open() + 1 + 6);
    if (setrlimit(RLIMIT_NOFILE, &tight) != 0) {
        CHECK(0, "fdx: setrlimit lower");
        tsdb_close(db);
        return;
    }

    tsdb_result_t *r = NULL;
    int qrc = tsdb_query(db, "SELECT count(*) FROM wideq", &r);
    int64_t nrows = -1;
    if (qrc == TSDB_OK && r && tsdb_result_next(r)) nrows = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);

    /* Restore BEFORE any assertion so every exit path leaves the process
     * with its original limit. */
    setrlimit(RLIMIT_NOFILE, &old);

    printf("  fdx: fd-exhausted count rc=%d rows=%lld\n", qrc, (long long)nrows);
    CHECK(qrc != TSDB_OK, "fdx: fd-exhausted SELECT returns an error rc");
    CHECK(!(qrc == TSDB_OK && nrows < FDX_DAYS * FDX_PER_DAY),
          "fdx: fd-exhausted SELECT must not silently answer short");

    tsdb_close(db);
    rmrf(dir);
}

int main(void) {
    printf("=== test_torn_partition_read ===\n");

    const char *dir = "/tmp/tsdb_test_torn_partition";
    rmrf(dir);

    /* ── Phase 1: create stable + child, write+flush NROWS rows ─────────── */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); return 1; }

        tsdb_result_t *r = NULL;
        tsdb_query(db, "CREATE STABLE cleanA (ts TIMESTAMP, val FLOAT64) TAGS (loc SYMBOL)", &r);
        if (r) { tsdb_result_free(r); r = NULL; }
        tsdb_query(db, "CREATE TABLE lt_0 USING cleanA TAGS ('east')", &r);
        if (r) { tsdb_result_free(r); r = NULL; }

        tsdb_table_t *tbl = NULL;
        if (tsdb_open_table(db, "lt_0", &tbl) != TSDB_OK) { fprintf(stderr, "open lt_0 failed\n"); return 1; }
        tsdb_batch_t *b = NULL;
        tsdb_batch_begin(tbl, &b);
        for (int i = 0; i < NROWS; i++) {
            tsdb_batch_row_ts(b, (tsdb_ts_t)(1000000000000LL + (int64_t)i * 1000000LL));
            tsdb_batch_row_f64(b, 1, (double)i);
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);

        /* Intact baseline: both the child and the super-table read the full set. */
        int64_t c_child  = q_count(db, "SELECT count(*) FROM lt_0");
        int64_t c_stable = q_count(db, "SELECT count(*) FROM cleanA");
        printf("  intact: count(lt_0)=%lld count(cleanA)=%lld\n",
               (long long)c_child, (long long)c_stable);
        CHECK(c_child  == NROWS, "intact child count == NROWS");
        CHECK(c_stable == NROWS, "intact stable count == NROWS");

        tsdb_close(db);
    }

    /* ── Phase 2: tear the partition — truncate ts.col below idx max_end ── */
    char ts_col[4096];
    char table_dir[4096];
    snprintf(table_dir, sizeof(table_dir), "%s/lt_0", dir);
    if (!find_ts_col(table_dir, ts_col, sizeof(ts_col))) {
        fprintf(stderr, "FAIL: could not locate ts.col (was the partition flushed?)\n");
        printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, ++g_fail);
        return 1;
    }
    {
        struct stat st;
        if (stat(ts_col, &st) != 0 || st.st_size < 2) {
            fprintf(stderr, "FAIL: ts.col missing/too small\n");
            printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, ++g_fail);
            return 1;
        }
        off_t half = st.st_size / 2;
        if (truncate(ts_col, half) != 0) {
            fprintf(stderr, "FAIL: truncate(%s) failed\n", ts_col);
            printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, ++g_fail);
            return 1;
        }
        printf("  torn: truncated %s from %lld to %lld bytes\n",
               ts_col, (long long)st.st_size, (long long)half);
    }

    /* ── Phase 3: reopen and read the torn partition ───────────────────── */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen failed\n"); return 1; }

        int64_t c_child  = q_count(db, "SELECT count(*) FROM lt_0");
        int64_t c_stable = q_count(db, "SELECT count(*) FROM cleanA");
        printf("  torn:  count(lt_0)=%lld count(cleanA)=%lld\n",
               (long long)c_child, (long long)c_stable);

        /* The core regression: a torn .col must NOT make a SELECT return 0.
         * The durable block prefix is recovered (some rows < NROWS, but > 0). */
        CHECK(c_child  > 0, "torn child SELECT recovers durable prefix (not 0)");
        CHECK(c_stable > 0, "torn stable SELECT recovers durable prefix (not 0)");
        CHECK(c_child  <= NROWS, "torn child count <= NROWS (tail discarded)");
        CHECK(c_child == c_stable, "child and super-table agree on torn count");

        tsdb_close(db);
    }

    rmrf(dir);

    test_fd_exhaustion_part_open();
    test_fd_exhaustion_query();

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
