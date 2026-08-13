/* test_bucket_negative_ts.c — Time bucketing must FLOOR, on every path.
 *
 * A bucket is the half-open interval [k*w, (k+1)*w).  Deriving it with C's
 * `/` and `%` does NOT give that: both truncate toward zero, so every negative
 * timestamp that is not an exact multiple of the width lands one bucket too
 * high, and -0.5s shares a bucket with +0.5s.  Pre-1970 timestamps are
 * ordinary here (backfilled history), so this is a plain wrong answer.
 *
 * Worse, the two implementations disagreed with each other:
 * tsdb_bucket_assign's power-of-two fast path uses an arithmetic right shift,
 * which IS floor, while its general path divides.  The same timestamp then got
 * a different bucket depending only on whether the interval happened to be a
 * power of two.
 *
 * Pinned here:
 *   1. tsdb_bucket_assign floors, and the pow2 and general paths agree on
 *      identical effective math for negative input.
 *   2. SAMPLE BY puts -1.5s / -0.5s / +0.5s in three different 1s buckets.
 *   3. The per-row time_bucket() projection (no aggregate) floors likewise.
 *   4. Bucket edges are exact: k*w opens a bucket, k*w-1 belongs to the one
 *      below it.
 */
#include "tsdb.h"
#include "../src/exec/bucket.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) \
    do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) \
    do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s) %s", _rc, tsdb_errstr(_rc), tsdb_last_error()); } while (0)

#define SEC 1000000000LL

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

/* ---- Test 1: tsdb_bucket_assign floors, and both paths agree ------------ */
static void test_assign_floor_and_path_agreement(void) {
    printf("[1] tsdb_bucket_assign: floor semantics, pow2 path == general path\n");

    /* Straddles zero and includes exact multiples of both widths used below. */
    const size_t n = 10;
    int64_t ts[10] = {-9, -8, -5, -4, -1, 0, 1, 3, 4, 7};

    /* Width 4 (a power of two → the right-shift fast path).
     * floor(ts/4), written out rather than recomputed, so the test does not
     * simply restate the implementation. */
    int64_t want4[10] = {-3, -2, -2, -1, -1, 0, 0, 0, 1, 1};
    int64_t fast[10];
    OK(tsdb_bucket_assign(ts, n, 0, 4, fast));
    for (size_t i = 0; i < n; i++)
        if (fast[i] != want4[i])
            FAIL("pow2 path ts=%lld got=%lld want=%lld",
                 (long long)ts[i], (long long)fast[i], (long long)want4[i]);

    /* The SAME effective math, forced down the general (division) path by
     * moving both the timestamps and the origin by one full width:
     * (ts+4 - 4) / 4 == ts / 4.  origin != 0 disables the fast path, so this
     * is the one input for which the two paths are directly comparable. */
    int64_t shifted[10], general[10];
    for (size_t i = 0; i < n; i++) shifted[i] = ts[i] + 4;
    OK(tsdb_bucket_assign(shifted, n, 4, 4, general));
    for (size_t i = 0; i < n; i++)
        if (general[i] != fast[i])
            FAIL("path disagreement ts=%lld pow2=%lld general=%lld",
                 (long long)ts[i], (long long)fast[i], (long long)general[i]);

    /* Non-power-of-two width: only the general path can serve it. */
    int64_t want3[10] = {-3, -3, -2, -2, -1, 0, 0, 1, 1, 2};
    int64_t gen3[10];
    OK(tsdb_bucket_assign(ts, n, 0, 3, gen3));
    for (size_t i = 0; i < n; i++)
        if (gen3[i] != want3[i])
            FAIL("general path w=3 ts=%lld got=%lld want=%lld",
                 (long long)ts[i], (long long)gen3[i], (long long)want3[i]);

    printf("  passed (floor on both paths, paths agree over negatives)\n");
}

/* Open a fresh db with one (ts, v) table and write the given timestamps. */
static tsdb_db_t *make_db(const char *dir, const int64_t *pts, int npts) {
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < npts; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)pts[i]));
        OK(tsdb_batch_row_f64(bk, 1, 1.0));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));
    return db;
}

/* ---- Test 2: SAMPLE BY over pre-1970 timestamps ------------------------- */
static void test_sample_by_negative(void) {
    printf("[2] SAMPLE BY 1s: -1.5s / -0.5s / +0.5s land in three buckets\n");

    const char *dir = "/tmp/tsdb_bucket_negts_t2";
    int64_t pts[3] = {-3 * SEC / 2, -SEC / 2, SEC / 2};
    tsdb_db_t *db = make_db(dir, pts, 3);

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT time_bucket(ts, 1000000000), count(*) FROM t "
                      "SAMPLE BY 1s", &r));

    int64_t want_b[3] = {-2 * SEC, -SEC, 0};
    int n = 0;
    while (tsdb_result_next(r)) {
        int64_t b = (int64_t)tsdb_result_ts(r, 0);
        int64_t c = tsdb_result_i64(r, 1);
        if (n >= 3)
            FAIL("SAMPLE BY emitted more than 3 buckets (extra bucket=%lld)",
                 (long long)b);
        if (b != want_b[n])
            FAIL("bucket %d: got=%lld want=%lld", n, (long long)b,
                 (long long)want_b[n]);
        if (c != 1)
            FAIL("bucket %lld: count=%lld want=1", (long long)b, (long long)c);
        n++;
    }
    if (n != 3) FAIL("expected 3 buckets, got %d", n);

    tsdb_result_free(r);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed (-2s:1, -1s:1, 0s:1)\n");
}

/* ---- Test 3: per-row time_bucket() projection --------------------------- */
/* No aggregate and no SAMPLE BY: a separate copy of the bucket arithmetic in
 * the row-emit loop, which had the same truncation. */
static void test_row_projection_negative(void) {
    printf("[3] per-row time_bucket() projection floors the same way\n");

    const char *dir = "/tmp/tsdb_bucket_negts_t3";
    int64_t pts[3] = {-3 * SEC / 2, -SEC / 2, SEC / 2};
    tsdb_db_t *db = make_db(dir, pts, 3);

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT time_bucket(ts, 1000000000) FROM t", &r));

    int64_t want_b[3] = {-2 * SEC, -SEC, 0};
    int n = 0;
    while (tsdb_result_next(r)) {
        int64_t b = (int64_t)tsdb_result_ts(r, 0);
        if (n >= 3) FAIL("row projection emitted more than 3 rows");
        if (b != want_b[n])
            FAIL("row %d: time_bucket got=%lld want=%lld", n, (long long)b,
                 (long long)want_b[n]);
        n++;
    }
    if (n != 3) FAIL("expected 3 rows, got %d", n);

    tsdb_result_free(r);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

/* ---- Test 4: bucket edges are exact ------------------------------------- */
/* k*w opens bucket k; k*w-1 is the last nanosecond of bucket k-1.  Asserted on
 * both sides of zero so an off-by-one in either direction shows up. */
static void test_bucket_edges(void) {
    printf("[4] bucket edges: k*w opens a bucket, k*w-1 closes the one below\n");

    const char *dir = "/tmp/tsdb_bucket_negts_t4";
    int64_t pts[6] = {
        -2 * SEC,      /* exact edge  → -2s */
        -SEC - 1,      /* 1ns below -1s → -2s */
        -SEC,          /* exact edge  → -1s */
        -1,            /* 1ns below 0 → -1s */
        0,             /* exact edge  →  0s */
        SEC - 1,       /* 1ns below 1s →  0s */
    };
    int64_t want[6] = {-2 * SEC, -2 * SEC, -SEC, -SEC, 0, 0};
    tsdb_db_t *db = make_db(dir, pts, 6);

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT ts, time_bucket(ts, 1000000000) FROM t", &r));
    int n = 0;
    while (tsdb_result_next(r)) {
        int64_t t = (int64_t)tsdb_result_ts(r, 0);
        int64_t b = (int64_t)tsdb_result_ts(r, 1);
        if (n >= 6) FAIL("expected 6 rows, got more");
        if (t != pts[n])
            FAIL("row %d: ts got=%lld want=%lld", n, (long long)t,
                 (long long)pts[n]);
        if (b != want[n])
            FAIL("ts=%lld: bucket got=%lld want=%lld", (long long)t,
                 (long long)b, (long long)want[n]);
        n++;
    }
    if (n != 6) FAIL("expected 6 rows, got %d", n);
    tsdb_result_free(r);

    /* Same six points through SAMPLE BY: three buckets of two rows each. */
    OK(tsdb_query(db, "SELECT time_bucket(ts, 1000000000), count(*) FROM t "
                      "SAMPLE BY 1s", &r));
    int64_t want_b[3] = {-2 * SEC, -SEC, 0};
    n = 0;
    while (tsdb_result_next(r)) {
        int64_t b = (int64_t)tsdb_result_ts(r, 0);
        int64_t c = tsdb_result_i64(r, 1);
        if (n >= 3)
            FAIL("SAMPLE BY emitted more than 3 buckets (extra bucket=%lld)",
                 (long long)b);
        if (b != want_b[n])
            FAIL("bucket %d: got=%lld want=%lld", n, (long long)b,
                 (long long)want_b[n]);
        if (c != 2)
            FAIL("bucket %lld: count=%lld want=2", (long long)b, (long long)c);
        n++;
    }
    if (n != 3) FAIL("expected 3 buckets, got %d", n);

    tsdb_result_free(r);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

int main(void) {
    printf("=== test_bucket_negative_ts ===\n\n");

    test_assign_floor_and_path_agreement();
    test_sample_by_negative();
    test_row_projection_negative();
    test_bucket_edges();

    printf("\n=== 4 passed, 0 failed ===\n");
    return 0;
}
