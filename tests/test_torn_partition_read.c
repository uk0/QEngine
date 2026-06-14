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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

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

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
