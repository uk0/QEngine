/* test_window_fns.c — unit tests for DIFF, DERIVATIVE, CSUM, MAVG window fns */
#include "tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); \
} while (0)

#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) \
    FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)

#define EXPECT_CLOSE(got, want, eps, msg) do { \
    double _g = (got), _w = (want); \
    if (!isfinite(_g) || !isfinite(_w) || fabs(_g - _w) > (eps)) \
        FAIL("%s: got %.9f want %.9f", (msg), _g, _w); \
} while (0)

#define EXPECT_NAN(got, msg) do { \
    double _v = (got); \
    if (!isnan(_v)) FAIL("%s: expected NaN, got %.6f", (msg), _v); \
} while (0)

/* ---- helpers ------------------------------------------------------------ */

static void rm_rf(const char *path) {
    DIR *d = opendir(path);
    if (!d) { remove(path); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", path, e->d_name);
        rm_rf(p);
    }
    closedir(d); rmdir(path);
}

/* Open a fresh database at path (wipe if exists). */
static tsdb_db_t *open_fresh_db(const char *path) {
    rm_rf(path);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(path, &db));
    return db;
}

/* Create a simple 2-col table (ts TIMESTAMP, val INT64), return open table. */
static tsdb_table_t *make_table(tsdb_db_t *db, const char *name) {
    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, name, cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, name, &tbl));
    return tbl;
}

/* Insert N rows: ts[i] = i * step_ns, val[i] = val_base + i */
static void insert_uniform(tsdb_table_t *tbl, int n,
                           int64_t step_ns, int64_t val_base) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(tbl, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i * step_ns)));
        OK(tsdb_batch_row_i64(b, 1, val_base + (int64_t)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

/* Insert specific (ts, val) pairs. */
static void insert_rows(tsdb_table_t *tbl,
                        const int64_t *ts_arr,
                        const int64_t *v_arr, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(tbl, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)ts_arr[i]));
        OK(tsdb_batch_row_i64(b, 1, v_arr[i]));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

/* Run a query; abort on error. */
static tsdb_result_t *run_query(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("query failed rc=%d (%s)\n  sql: %s",
                             rc, tsdb_errstr(rc), sql);
    return r;
}

/* Count rows by draining the result. */
static int count_rows(tsdb_result_t *r) {
    int n = 0;
    while (tsdb_result_next(r) > 0) n++;
    return n;
}

/* Materialise up to max_rows float values from col_idx into buf[].
 * Returns actual row count. */
static int collect_f64(tsdb_db_t *db, const char *sql,
                       int col_idx, double *buf, int max_rows) {
    tsdb_result_t *r = run_query(db, sql);
    int n = 0;
    while (tsdb_result_next(r) > 0 && n < max_rows) {
        buf[n++] = tsdb_result_f64(r, col_idx);
    }
    tsdb_result_free(r);
    return n;
}

/* =========================================================================
 * Test 1: diff() basic
 *   10 rows, val=0..9, ts step=1s
 *   row0: NaN; rows 1-9: diff = 1
 * ========================================================================= */
static void test_diff(void) {
    const char *dir = "/tmp/tsdb_wfn_diff";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");
    insert_uniform(tbl, 10, 1000000000LL, 0);

    double vals[10];
    int n = collect_f64(db,
        "SELECT ts, val, diff(val) FROM t ORDER BY ts",
        2, vals, 10);

    if (n != 10) FAIL("diff: expected 10 rows, got %d", n);
    EXPECT_NAN(vals[0], "diff row0");
    for (int i = 1; i < 10; i++)
        EXPECT_CLOSE(vals[i], 1.0, 1e-9, "diff row");

    tsdb_close(db);
    printf("  PASS test_diff\n");
}

/* =========================================================================
 * Test 2: csum()
 *   10 rows, val=0..9
 *   csum[i] = 0+1+...+i = i*(i+1)/2
 * ========================================================================= */
static void test_csum(void) {
    const char *dir = "/tmp/tsdb_wfn_csum";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");
    insert_uniform(tbl, 10, 1000000000LL, 0);

    double vals[10];
    int n = collect_f64(db,
        "SELECT ts, val, csum(val) FROM t ORDER BY ts",
        2, vals, 10);

    if (n != 10) FAIL("csum: expected 10 rows, got %d", n);
    double expected = 0.0;
    for (int i = 0; i < 10; i++) {
        expected += (double)i;
        EXPECT_CLOSE(vals[i], expected, 1e-9, "csum row");
    }
    /* row9: csum = 45 */
    EXPECT_CLOSE(vals[9], 45.0, 1e-9, "csum row9");

    tsdb_close(db);
    printf("  PASS test_csum\n");
}

/* =========================================================================
 * Test 3: derivative()
 *   val=0..9, ts step=1s → each step = 1 val/s
 *   derivative[0] = NaN; derivative[1..9] = 1.0
 * ========================================================================= */
static void test_derivative_uniform(void) {
    const char *dir = "/tmp/tsdb_wfn_deriv";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");
    insert_uniform(tbl, 10, 1000000000LL, 0);

    double vals[10];
    int n = collect_f64(db,
        "SELECT ts, val, derivative(val) FROM t ORDER BY ts",
        2, vals, 10);

    if (n != 10) FAIL("derivative_uniform: expected 10 rows, got %d", n);
    EXPECT_NAN(vals[0], "derivative row0");
    for (int i = 1; i < 10; i++)
        EXPECT_CLOSE(vals[i], 1.0, 1e-9, "derivative row");

    tsdb_close(db);
    printf("  PASS test_derivative_uniform\n");
}

/* =========================================================================
 * Test 4: derivative() non-uniform timestamps
 *   ts = 0, 2s, 5s;  val = 0, 6, 12
 *   derivative[1] = (6-0)/(2e9 ns) * 1e9 = 3.0
 *   derivative[2] = (12-6)/(3e9 ns) * 1e9 = 2.0
 * ========================================================================= */
static void test_derivative_nonuniform(void) {
    const char *dir = "/tmp/tsdb_wfn_deriv_nu";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");

    int64_t ts_arr[3] = {0LL, 2000000000LL, 5000000000LL};
    int64_t v_arr[3]  = {0LL, 6LL, 12LL};
    insert_rows(tbl, ts_arr, v_arr, 3);

    double vals[3];
    int n = collect_f64(db,
        "SELECT ts, val, derivative(val) FROM t ORDER BY ts",
        2, vals, 3);

    if (n != 3) FAIL("derivative_nu: expected 3 rows, got %d", n);
    EXPECT_NAN(vals[0], "derivative_nu row0");
    EXPECT_CLOSE(vals[1], 3.0, 1e-9, "derivative_nu row1");
    EXPECT_CLOSE(vals[2], 2.0, 1e-9, "derivative_nu row2");

    tsdb_close(db);
    printf("  PASS test_derivative_nonuniform\n");
}

/* =========================================================================
 * Test 5: mavg(val, 3)
 *   Input: 0,1,2,3,4,5,6,7,8,9
 *   i=0: 0/1=0.0   (partial)
 *   i=1: (0+1)/2=0.5 (partial)
 *   i=2: (0+1+2)/3=1.0 (full)
 *   i=3: (1+2+3)/3=2.0
 *   …
 *   i=9: (7+8+9)/3=8.0
 * ========================================================================= */
static void test_mavg(void) {
    const char *dir = "/tmp/tsdb_wfn_mavg";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");
    insert_uniform(tbl, 10, 1000000000LL, 0);

    double vals[10];
    int n = collect_f64(db,
        "SELECT ts, val, mavg(val, 3) FROM t ORDER BY ts",
        2, vals, 10);

    if (n != 10) FAIL("mavg3: expected 10 rows, got %d", n);

    double want[10] = {0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0};
    for (int i = 0; i < 10; i++)
        EXPECT_CLOSE(vals[i], want[i], 1e-9, "mavg(3)");

    tsdb_close(db);
    printf("  PASS test_mavg\n");
}

/* =========================================================================
 * Test 6: mavg(val, 1) — identity
 * ========================================================================= */
static void test_mavg_window1(void) {
    const char *dir = "/tmp/tsdb_wfn_mavg1";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");
    insert_uniform(tbl, 5, 1000000000LL, 10);  /* val = 10,11,12,13,14 */

    double vals[5];
    int n = collect_f64(db,
        "SELECT ts, val, mavg(val, 1) FROM t ORDER BY ts",
        2, vals, 5);

    if (n != 5) FAIL("mavg1: expected 5 rows, got %d", n);
    for (int i = 0; i < 5; i++)
        EXPECT_CLOSE(vals[i], (double)(10 + i), 1e-9, "mavg(1) identity");

    tsdb_close(db);
    printf("  PASS test_mavg_window1\n");
}

/* =========================================================================
 * Test 7: combined diff+csum in single query
 *   Spot-check row 0 (NaN diff, csum=0) and row 9 (diff=1, csum=45)
 * ========================================================================= */
static void test_combined(void) {
    const char *dir = "/tmp/tsdb_wfn_combo";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");
    insert_uniform(tbl, 10, 1000000000LL, 0);

    tsdb_result_t *r = run_query(db,
        "SELECT ts, val, diff(val), csum(val) FROM t ORDER BY ts");

    /* row 0 */
    if (tsdb_result_next(r) <= 0) FAIL("combined: no rows");
    double diff0 = tsdb_result_f64(r, 2);
    double csum0 = tsdb_result_f64(r, 3);
    EXPECT_NAN(diff0, "row0 diff should be NaN");
    EXPECT_CLOSE(csum0, 0.0, 1e-9, "row0 csum");

    /* skip rows 1-8 */
    for (int i = 1; i < 9; i++) {
        if (tsdb_result_next(r) <= 0) FAIL("combined: early end at row %d", i);
    }

    /* row 9 */
    if (tsdb_result_next(r) <= 0) FAIL("combined: no row9");
    double diff9 = tsdb_result_f64(r, 2);
    double csum9 = tsdb_result_f64(r, 3);
    EXPECT_CLOSE(diff9, 1.0, 1e-9, "row9 diff");
    EXPECT_CLOSE(csum9, 45.0, 1e-9, "row9 csum");

    tsdb_result_free(r);
    tsdb_close(db);
    printf("  PASS test_combined\n");
}

/* =========================================================================
 * Test 8: error on mixing window fn with aggregate
 * ========================================================================= */
static void test_error_mixed_agg(void) {
    const char *dir = "/tmp/tsdb_wfn_mixerr";
    tsdb_db_t *db = open_fresh_db(dir);
    make_table(db, "t");

    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db,
        "SELECT sum(val), diff(val) FROM t", &r);
    if (rc == TSDB_OK) {
        tsdb_result_free(r);
        FAIL("mixed agg+window should have returned error");
    }
    tsdb_result_free(r);
    tsdb_close(db);
    printf("  PASS test_error_mixed_agg\n");
}

/* =========================================================================
 * Test 9: interp() — linear interpolation over aligned grid
 * ========================================================================= */
static void test_interp(void) {
    const char *dir = "/tmp/tsdb_wfn_interp";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");

    /* Insert 3 points: at 0ns, 2s, 4s with values 0, 2.0, 4.0.
     * Grid interval: 1s = 1000000000 ns.
     * Expected grid points: 0s, 1s, 2s, 3s, 4s → 0, 1, 2, 3, 4. */
    int64_t ts[] = { 0LL, 2000000000LL, 4000000000LL };
    int64_t vs[] = { 0LL, 2LL, 4LL };
    insert_rows(tbl, ts, vs, 3);

    /* SELECT ts, interp(val, 1000000000) FROM t */
    tsdb_result_t *r = run_query(db,
        "SELECT ts, interp(val, 1000000000) FROM t");
    if (tsdb_result_ncols(r) != 2)
        FAIL("interp: expected 2 columns, got %d", tsdb_result_ncols(r));

    /* Should produce 5 rows: ts=0,1s,2s,3s,4s → val=0,1,2,3,4 */
    int row = 0;
    double expected_vals[] = { 0.0, 1.0, 2.0, 3.0, 4.0 };
    while (tsdb_result_next(r)) {
        if (row >= 5) FAIL("interp: too many rows");
        double val = tsdb_result_f64(r, 1);
        EXPECT_CLOSE(val, expected_vals[row], 1e-9, "interp val");
        row++;
    }
    if (row != 5) FAIL("interp: expected 5 rows, got %d", row);

    tsdb_result_free(r);
    tsdb_close(db);
    printf("  PASS test_interp\n");
}

/* =========================================================================
 * Test 10: interp() — sparse data with gap (missing bucket, skip)
 * ========================================================================= */
static void test_interp_gap(void) {
    const char *dir = "/tmp/tsdb_wfn_interp_gap";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");

    /* Only two points: 0ns and 5s. Grid: 1s. Buckets: 0,1,2,3,4,5.
     * 0 and 5 are exact hits; 1,2,3,4 are interpolated.
     * val at 0 = 0, val at 5s = 10 → step per second = 2. */
    int64_t ts[] = { 0LL, 5000000000LL };
    int64_t vs[] = { 0LL, 10LL };
    insert_rows(tbl, ts, vs, 2);

    tsdb_result_t *r = run_query(db,
        "SELECT ts, interp(val, 1000000000) FROM t");

    int row = 0;
    double expected[] = { 0.0, 2.0, 4.0, 6.0, 8.0, 10.0 };
    while (tsdb_result_next(r)) {
        if (row >= 6) FAIL("interp_gap: too many rows");
        double val = tsdb_result_f64(r, 1);
        EXPECT_CLOSE(val, expected[row], 1e-9, "interp_gap val");
        row++;
    }
    if (row != 6) FAIL("interp_gap: expected 6 rows, got %d", row);

    tsdb_result_free(r);
    tsdb_close(db);
    printf("  PASS test_interp_gap\n");
}

/* =========================================================================
 * Test 11: interp() — LIMIT pushdown
 * ========================================================================= */
static void test_interp_limit(void) {
    const char *dir = "/tmp/tsdb_wfn_interp_limit";
    tsdb_db_t *db = open_fresh_db(dir);
    tsdb_table_t *tbl = make_table(db, "t");

    int64_t ts[] = { 0LL, 10000000000LL };
    int64_t vs[] = { 0LL, 10LL };
    insert_rows(tbl, ts, vs, 2);

    tsdb_result_t *r = run_query(db,
        "SELECT ts, interp(val, 1000000000) FROM t LIMIT 3");
    int row = 0;
    while (tsdb_result_next(r)) row++;
    if (row != 3) FAIL("interp_limit: expected 3 rows, got %d", row);

    tsdb_result_free(r);
    tsdb_close(db);
    printf("  PASS test_interp_limit\n");
}

/* =========================================================================
 * Test 12: interp() error — bad arguments
 * ========================================================================= */
static void test_interp_errors(void) {
    const char *dir = "/tmp/tsdb_wfn_interp_err";
    tsdb_db_t *db = open_fresh_db(dir);
    make_table(db, "t");

    /* Missing interval arg. */
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "SELECT interp(val) FROM t", &r);
    if (rc == TSDB_OK) { tsdb_result_free(r); FAIL("interp_errors: should fail with 1 arg"); }
    tsdb_result_free(r); r = NULL;

    /* Combined with aggregate — should fail. */
    rc = tsdb_query(db, "SELECT sum(val), interp(val, 1000000000) FROM t", &r);
    if (rc == TSDB_OK) { tsdb_result_free(r); FAIL("interp_errors: should fail with aggregate"); }
    tsdb_result_free(r);

    tsdb_close(db);
    printf("  PASS test_interp_errors\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */
int main(void) {
    printf("=== test_window_fns ===\n");

    test_diff();
    test_csum();
    test_derivative_uniform();
    test_derivative_nonuniform();
    test_mavg();
    test_mavg_window1();
    test_combined();
    test_error_mixed_agg();
    test_interp();
    test_interp_gap();
    test_interp_limit();
    test_interp_errors();

    printf("=== ALL WINDOW FN TESTS PASSED ===\n");
    return 0;
}
