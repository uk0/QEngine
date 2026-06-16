/* test_torn_value_column.c — a torn NON-ts column must never mis-pair rows.
 *
 * Companion to test_torn_partition_read.c, which tears the ts column.  This
 * tears a NON-ts column's `.col` file (shorter than its `.idx` references) while
 * ts.col stays intact — the shape produced by a crash mid-flush or an
 * interrupted/racing anti-entropy truncate+re-pull that hit one column's file.
 *
 * THE BUG (pre-fix): tsdb_part_open opens each column independently.  A torn
 * value column loses its TAIL blocks to the per-block size filter, so it ends
 * up with FEWER readable blocks than ts.  The ALTER-TABLE-ADD-COLUMN alignment
 * pass then assumed the shortfall meant "this column was added later" and
 * PREPENDED zero-fill sentinel blocks at the FRONT — shifting the surviving
 * real blocks to align with the LAST ts blocks.  The block matcher in exec.c
 * pairs blocks by (ts_min, count); the prepended sentinels carry the LEADING ts
 * blocks' (ts_min, count), so a low-ts row matched a zero-fill sentinel instead
 * of its real value.  Result: count(*) stayed correct but every value cell was
 * silently WRONG (row i reported val 0 instead of its real value).  That is
 * worse than the torn-ts case (0 rows): it returns wrong data with no error.
 *
 * THE FIX: distinguish a torn column (idx declares the SAME block count as ts,
 * but the .col tear filtered the tail) from a genuinely later-added column (idx
 * itself has fewer entries).  For a torn column the missing blocks are the TAIL,
 * so the partition is clamped to the durable block PREFIX that every column has
 * — matching the torn-ts behaviour: recover the prefix, drop the torn tail, and
 * NEVER return a mis-paired (wrong-value) row.
 *
 * Asserts:
 *   1. intact:  count == NROWS and every value matches its ts.
 *   2. torn val.col, partial: 0 < rows < NROWS, count(*) agrees with the scan,
 *      and EVERY returned value is correct (no zero-fill misalignment).
 *   3. torn val.col, severe (whole column gone): rows == 0 (no lie), not NROWS
 *      of zero-filled cells.
 *   4. ALTER ADD COLUMN still zero-fills old rows correctly (the fix must not
 *      break the legitimate sentinel-prepend path).
 */

#include "tsdb.h"

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

/* Find "<table_dir>/<YYYYMMDD>/<colfile>". Returns 1 on success. */
static int find_col(const char *table_dir, const char *colfile,
                    char *out, size_t cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s/%s", table_dir, e->d_name, colfile);
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

#define NROWS    30000  /* > block_points (8192) → 4 on-disk blocks */
#define BASE_TS  1000000000000LL
#define STEP_NS  1000000LL

/* val(i) == i + a deterministic fraction in [0,1); floor(val) must equal the
 * row's ts index, so any mis-pairing shows up as floor(val) != ts_index. */
static double val_of(int i) { return (double)i + (double)(i % 997) / 1000.0; }

static void write_dataset(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); exit(1); }
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create rc=%d\n", rc); exit(1); }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "open_table failed\n"); exit(1); }
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    for (int i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * STEP_NS);
        tsdb_batch_row_f64(b, 1, val_of(i));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    tsdb_close(db);
}

/* Scan SELECT ts,val and verify every returned value matches its ts.  Returns
 * the row count via *out_rows and the number of mis-paired cells via *out_wrong. */
static void scan_verify(tsdb_db_t *db, int64_t *out_rows, int *out_wrong) {
    tsdb_result_t *r = NULL;
    int64_t rows = 0; int wrong = 0;
    if (tsdb_query(db, "SELECT ts, val FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            double  v  = tsdb_result_f64(r, 1);
            int64_t idx = (ts - BASE_TS) / STEP_NS;
            if ((int64_t)v != idx) {
                if (wrong < 5)
                    fprintf(stderr, "  MISALIGNED ts_idx=%lld floor(val)=%lld\n",
                            (long long)idx, (long long)(int64_t)v);
                wrong++;
            }
            rows++;
        }
        tsdb_result_free(r);
    }
    *out_rows = rows; *out_wrong = wrong;
}

static int64_t q_count(tsdb_db_t *db) {
    tsdb_result_t *r = NULL; int64_t v = -1;
    if (tsdb_query(db, "SELECT count(*) FROM t", &r) == TSDB_OK && r) {
        if (tsdb_result_next(r)) v = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
    }
    return v;
}

/* ── Phase 1+2: tear a value column to a given fraction, verify recovery ── */
static void test_torn_value(double keep_frac, const char *label,
                            int expect_some_rows) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/tsdb_test_torn_val_%d",
             (int)(keep_frac * 100));
    rmrf(dir);
    write_dataset(dir);

    /* Intact baseline: full count + all values correct. */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }
        int64_t rows = 0; int wrong = 0;
        scan_verify(db, &rows, &wrong);
        int64_t cnt = q_count(db);
        printf("  [%s] intact: rows=%lld count=%lld wrong=%d\n",
               label, (long long)rows, (long long)cnt, wrong);
        CHECK(rows == NROWS && cnt == NROWS && wrong == 0,
              "intact: full count, all values correct");
        tsdb_close(db);
    }

    /* Tear ONLY val.col (ts.col stays intact). */
    char table_dir[256], valcol[4096];
    snprintf(table_dir, sizeof(table_dir), "%s/t", dir);
    if (!find_col(table_dir, "val.col", valcol, sizeof(valcol))) {
        fprintf(stderr, "FAIL: no val.col (not flushed?)\n"); g_fail++; return;
    }
    struct stat st;
    if (stat(valcol, &st) != 0 || st.st_size < 2) {
        fprintf(stderr, "FAIL: val.col missing/small\n"); g_fail++; return;
    }
    off_t keep = (off_t)((double)st.st_size * keep_frac);
    if (truncate(valcol, keep) != 0) {
        fprintf(stderr, "FAIL: truncate failed\n"); g_fail++; return;
    }
    printf("  [%s] torn val.col %lld -> %lld (%.0f%%)\n",
           label, (long long)st.st_size, (long long)keep, keep_frac * 100);

    /* Reopen, scan, verify: count agrees with the scan, NO mis-paired values. */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen torn\n"); exit(1); }
        int64_t rows = 0; int wrong = 0;
        scan_verify(db, &rows, &wrong);
        int64_t cnt = q_count(db);
        printf("  [%s] torn: rows=%lld count=%lld wrong=%d\n",
               label, (long long)rows, (long long)cnt, wrong);

        /* THE CORE REGRESSION: a torn value column must NEVER return a
         * mis-paired (wrong) value cell. */
        CHECK(wrong == 0,
              "torn value column returns NO mis-paired values [core regression]");
        /* count(*) (driven by ts blocks) must agree with the actual scan, i.e.
         * the partition is clamped consistently — no phantom rows. */
        CHECK(cnt == rows, "count(*) agrees with the value scan after tear");
        /* Never more than the truth. */
        CHECK(rows <= NROWS, "torn value scan never exceeds NROWS");
        if (expect_some_rows) {
            CHECK(rows > 0 && rows < NROWS,
                  "partial tear recovers a nonzero durable prefix (< NROWS)");
        } else {
            CHECK(rows == 0,
                  "severe tear yields 0 rows (no zero-fill lie), not NROWS");
        }
        tsdb_close(db);
    }
    rmrf(dir);
}

/* ── Phase 3: ALTER ADD COLUMN still zero-fills pre-existing rows ──
 * The fix gates the sentinel-prepend on idx_decl_count; this guards that a
 * GENUINELY later-added column (its idx really has fewer blocks) still aligns
 * correctly and reads its old rows as zero-fill, not as an error or a clamp. */
static void test_alter_add_still_works(void) {
    const char *dir = "/tmp/tsdb_test_torn_val_alter";
    rmrf(dir);

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open\n"); exit(1); }
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create rc=%d\n", rc); exit(1); }

    /* Write + flush a partition with only ts,val (so 'w' will have ZERO blocks
     * for this partition's existing rows). */
    tsdb_table_t *t = NULL;
    tsdb_open_table(db, "t", &t);
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    const int M = 10000;   /* > block_points → multiple blocks */
    for (int i = 0; i < M; i++) {
        tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * STEP_NS);
        tsdb_batch_row_f64(b, 1, val_of(i));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);

    /* Add a new column AFTER the flush. */
    tsdb_result_t *r = NULL;
    rc = tsdb_query(db, "ALTER TABLE t ADD COLUMN w INT64", &r);
    if (r) { tsdb_result_free(r); r = NULL; }
    CHECK(rc == TSDB_OK, "ALTER ADD COLUMN w succeeded");
    tsdb_close(db);

    /* Reopen and verify: all M rows still present, val still correct, and the
     * pre-existing rows read w as 0 (zero-fill sentinel), not garbage/clamp. */
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }
    int64_t cnt = q_count(db);
    CHECK(cnt == M, "ALTER: all pre-existing rows survive reopen (not clamped)");

    int64_t rows = 0, wsum = 0; int wrong = 0;
    r = NULL;
    if (tsdb_query(db, "SELECT ts, val, w FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            double  v  = tsdb_result_f64(r, 1);
            int64_t w  = tsdb_result_i64(r, 2);
            int64_t idx = (ts - BASE_TS) / STEP_NS;
            if ((int64_t)v != idx) wrong++;
            wsum += w;
            rows++;
        }
        tsdb_result_free(r);
    }
    printf("  [alter] rows=%lld val_wrong=%d w_sum=%lld (w must be all-0 zero-fill)\n",
           (long long)rows, wrong, (long long)wsum);
    CHECK(rows == M, "ALTER: full scan row count");
    CHECK(wrong == 0, "ALTER: val column still correctly paired (sentinel path intact)");
    CHECK(wsum == 0, "ALTER: pre-existing rows read new column as zero-fill");
    tsdb_close(db);
    rmrf(dir);
}

int main(void) {
    printf("=== test_torn_value_column ===\n");

    printf("\n[1] partial tear of a value column (keep 70%%)\n");
    test_torn_value(0.70, "partial", /*expect_some_rows=*/1);

    printf("\n[2] severe tear of a value column (keep 1%% — whole column lost)\n");
    test_torn_value(0.01, "severe", /*expect_some_rows=*/0);

    printf("\n[3] ALTER ADD COLUMN still zero-fills old rows (fix must not break it)\n");
    test_alter_add_still_works();

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
