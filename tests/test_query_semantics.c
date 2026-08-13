/* test_query_semantics.c — break-tests for the QUERY-SEMANTICS unit.
 *
 * Each block below is a "silent wrong answer": the engine returns a number
 * with no error on a query shape users run daily.
 *
 *   QC-13  SAMPLE BY spread()/first()/last() emit a literal 0.0
 *   QC-6   an out-of-order (backfilled) point re-opens a closed SAMPLE BY
 *          bucket, so one logical bucket comes back as two rows
 *   QC-11  min()/max() over an EMPTY selection emit the accumulator identity
 *          (INT64_MAX / INT64_MIN) as if it were a real column value
 *   QC-9   a fractional literal compared against an INT64 column is truncated,
 *          silently moving the boundary
 *
 * Test bodies for these four originate in an earlier run of this unit
 * (trash/tests/test_query_correctness.c.salvage-partial); the ORDER BY cases
 * that shared that file are already fixed and live in tests/test_orderby_topn.c.
 */
#include "tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("assertion failed: %s", #cond); } while (0)

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

/* ---- QC-13: SAMPLE BY spread()/first()/last() must emit real values ------ */
static void test_sampleby_extended_aggs(void) {
    printf("[QC-13] SAMPLE BY spread/first/last emit real values, not 0.0\n");
    const char *dir = "/tmp/tsdb_qs_sb_ext";
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"v",   TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* One 1s bucket, values 10,20,30,40 → spread=30, first=10, last=40. */
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    double vals[] = {10.0, 20.0, 30.0, 40.0};
    for (int i = 0; i < 4; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * 100000000LL)));
        OK(tsdb_batch_row_f64(bk, 1, vals[i]));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), spread(v), first(v), last(v) "
        "FROM t SAMPLE BY 1s", &r));
    ASSERT(tsdb_result_next(r));
    double spread = tsdb_result_f64(r, 1);
    double first  = tsdb_result_f64(r, 2);
    double last   = tsdb_result_f64(r, 3);
    tsdb_result_free(r);
    if (fabs(spread - 30.0) > 1e-9 || fabs(first - 10.0) > 1e-9 || fabs(last - 40.0) > 1e-9)
        FAIL("SAMPLE BY spread=%.3f first=%.3f last=%.3f expected 30/10/40",
             spread, first, last);

    /* An int64 column takes the other accumulator arm; check it too. */
    tsdb_col_t icols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"n",   TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "ti", icols, 2, "ts"));
    tsdb_table_t *itbl = NULL;
    OK(tsdb_open_table(db, "ti", &itbl));
    OK(tsdb_batch_begin(itbl, &bk));
    int64_t ivals[] = {7, 3, 11, 5};
    for (int i = 0; i < 4; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * 100000000LL)));
        OK(tsdb_batch_row_i64(bk, 1, ivals[i]));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));
    r = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), spread(n), first(n), last(n) "
        "FROM ti SAMPLE BY 1s", &r));
    ASSERT(tsdb_result_next(r));
    int64_t isp = tsdb_result_i64(r, 1);
    int64_t ifi = tsdb_result_i64(r, 2);
    int64_t ila = tsdb_result_i64(r, 3);
    tsdb_result_free(r);
    if (isp != 8 || ifi != 7 || ila != 5)
        FAIL("SAMPLE BY int spread=%lld first=%lld last=%lld expected 8/7/5",
             (long long)isp, (long long)ifi, (long long)ila);

    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

/* ---- QC-6: a backfilled (out-of-order) point must not split a bucket ----- */
static void test_sampleby_out_of_order(void) {
    printf("[QC-6] out-of-order point does not re-open / duplicate a bucket\n");
    const char *dir = "/tmp/tsdb_qs_sb_ooo";
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"v",   TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    int64_t bns = 1000000000LL;
    /* Commit 1: buckets 0 and 1, in order. */
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)0));            OK(tsdb_batch_row_f64(bk, 1, 1.0)); OK(tsdb_batch_row_end(bk));
    OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)(bns/2)));      OK(tsdb_batch_row_f64(bk, 1, 1.0)); OK(tsdb_batch_row_end(bk));
    OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)bns));          OK(tsdb_batch_row_f64(bk, 1, 1.0)); OK(tsdb_batch_row_end(bk));
    OK(tsdb_batch_commit(bk));

    /* Commit 2: a BACKFILLED row whose ts falls back inside bucket 0, arriving
     * as a later scan source.  A ts-regression-blind streaming aggregator sees
     * b=0 != cur_bucket(=1), emits bucket 1, then re-opens bucket 0 — so bucket
     * 0 is emitted twice. */
    tsdb_batch_t *bk2 = NULL;
    OK(tsdb_batch_begin(tbl, &bk2));
    OK(tsdb_batch_row_ts(bk2, (tsdb_ts_t)(bns/4)));     OK(tsdb_batch_row_f64(bk2, 1, 1.0)); OK(tsdb_batch_row_end(bk2));
    OK(tsdb_batch_commit(bk2));

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), count(*) FROM t SAMPLE BY 1s", &r));
    int64_t seen[64]; int nseen = 0; int64_t total = 0;
    while (tsdb_result_next(r)) {
        int64_t bkt = (int64_t)tsdb_result_ts(r, 0);
        int64_t cnt = tsdb_result_i64(r, 1);
        for (int i = 0; i < nseen; i++)
            if (seen[i] == bkt)
                FAIL("bucket %lld emitted twice (duplicate/split bucket)", (long long)bkt);
        if (nseen < 64) seen[nseen++] = bkt;
        total += cnt;
    }
    tsdb_result_free(r);
    ASSERT(total == 4);          /* every row counted exactly once (3 + 1) */
    ASSERT(nseen == 2);          /* exactly two distinct buckets */
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

/* ---- QC-11: min()/max() over an empty selection must not emit a sentinel  */
static void test_minmax_empty_selection(void) {
    printf("[QC-11] min()/max() over zero rows do not emit INT64 sentinels as data\n");
    const char *dir = "/tmp/tsdb_qs_minmax_empty";
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"v",   TSDB_TYPE_INT64},
        {"f",   TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 3, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < 100; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * 1000000000LL)));
        OK(tsdb_batch_row_i64(bk, 1, (int64_t)i));
        OK(tsdb_batch_row_f64(bk, 2, (double)i));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    /* WHERE matches nothing.  min/max have no input; they must NOT report
     * INT64_MAX / INT64_MIN as if those were real column values. */
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT min(v), max(v) FROM t WHERE v > 1000000", &r));
    ASSERT(tsdb_result_next(r));
    int64_t mn = tsdb_result_i64(r, 0);
    int64_t mx = tsdb_result_i64(r, 1);
    tsdb_result_free(r);
    if (mn == INT64_MAX || mx == INT64_MIN)
        FAIL("empty min=%lld max=%lld leaked the sentinel as data",
             (long long)mn, (long long)mx);

    /* Same for the float arm: +INF / -INF are the identity, not data. */
    r = NULL;
    OK(tsdb_query(db, "SELECT min(f), max(f) FROM t WHERE v > 1000000", &r));
    ASSERT(tsdb_result_next(r));
    double fmn = tsdb_result_f64(r, 0);
    double fmx = tsdb_result_f64(r, 1);
    tsdb_result_free(r);
    if (isinf(fmn) || isinf(fmx))
        FAIL("empty float min=%f max=%f leaked the identity as data", fmn, fmx);

    /* The non-empty answers must be untouched by whatever guards the empty
     * case — this is the regression the previous attempt at QC-11 caused. */
    r = NULL;
    OK(tsdb_query(db, "SELECT min(v), max(v), min(f), max(f) FROM t", &r));
    ASSERT(tsdb_result_next(r));
    int64_t nmn = tsdb_result_i64(r, 0), nmx = tsdb_result_i64(r, 1);
    double  nfm = tsdb_result_f64(r, 2), nfx = tsdb_result_f64(r, 3);
    tsdb_result_free(r);
    if (nmn != 0 || nmx != 99 || fabs(nfm - 0.0) > 1e-9 || fabs(nfx - 99.0) > 1e-9)
        FAIL("non-empty min/max changed: %lld/%lld %f/%f expected 0/99 0/99",
             (long long)nmn, (long long)nmx, nfm, nfx);

    tsdb_close(db);
    rm_rf(dir);
    printf("  passed (min=%lld max=%lld)\n", (long long)mn, (long long)mx);
}

/* ---- QC-9: fractional literal vs int column must not truncate a boundary  */
static void test_fractional_literal_int_col(void) {
    printf("[QC-9] `v >= 2.5` on an int column keeps the boundary correct\n");
    const char *dir = "/tmp/tsdb_qs_frac";
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"v",   TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));
    /* values 1,2,3,4,5 */
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 1; i <= 5; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * 1000000000LL)));
        OK(tsdb_batch_row_i64(bk, 1, (int64_t)i));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    /* v >= 2.5 must match {3,4,5} == 3 rows.  Truncating 2.5→2 makes it v>=2,
     * which wrongly includes 2 → 4 rows. */
    struct { const char *q; int64_t want; } cases[] = {
        { "SELECT count(*) FROM t WHERE v >= 2.5", 3 },  /* {3,4,5} */
        { "SELECT count(*) FROM t WHERE v >  2.5", 3 },  /* {3,4,5} */
        { "SELECT count(*) FROM t WHERE v <= 2.5", 2 },  /* {1,2}   */
        { "SELECT count(*) FROM t WHERE v <  2.5", 2 },  /* {1,2}   */
        { "SELECT count(*) FROM t WHERE v =  2.5", 0 },  /* no int is 2.5 */
        { "SELECT count(*) FROM t WHERE v != 2.5", 5 },  /* every int differs */
        /* Integral float literals keep their exact meaning. */
        { "SELECT count(*) FROM t WHERE v >= 3.0", 3 },
        { "SELECT count(*) FROM t WHERE v =  3.0", 1 },
        /* Negative fractional: floor(-1.5) == -2, so v > -1.5 is all rows. */
        { "SELECT count(*) FROM t WHERE v > -1.5", 5 },
    };
    for (size_t ci = 0; ci < sizeof(cases)/sizeof(cases[0]); ci++) {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, cases[ci].q, &r));
        ASSERT(tsdb_result_next(r));
        int64_t cnt = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
        if (cnt != cases[ci].want)
            FAIL("%s matched %lld rows, expected %lld (fractional literal truncated)",
                 cases[ci].q, (long long)cnt, (long long)cases[ci].want);
    }
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

int main(int argc, char **argv) {
    printf("=== test_query_semantics ===\n");
    /* Optional single-test selector so each break can be observed failing
     * independently against the unfixed engine. */
    const char *only = (argc > 1) ? argv[1] : NULL;
    if (!only || !strcmp(only, "qc13")) test_sampleby_extended_aggs();
    if (!only || !strcmp(only, "qc6"))  test_sampleby_out_of_order();
    if (!only || !strcmp(only, "qc11")) test_minmax_empty_selection();
    if (!only || !strcmp(only, "qc9"))  test_fractional_literal_int_col();
    printf("\nQUERY-SEMANTICS PASSED\n");
    return 0;
}
