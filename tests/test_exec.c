/* test_exec.c — Unit tests for SIMD execution primitives.
 *
 * Tests:
 *   1. agg_sum/min/max for f64 and i64, sizes 1–10000
 *   2. filter_f64 EQ/LT/GT producing correct bitmaps
 *   3. bitmap_gather correctness
 *   4. bucket_sum_f64: single-bucket, multi-bucket, sparse-bucket
 *   5. Null-bitmap aware aggregation
 *   6. Performance: 1M double sum throughput in GB/s
 *
 * Build:
 *   cd /Users/firshme/Desktop/work/tsdb && \
 *   clang -std=c11 -O3 -Wall -Wextra -Iinclude -march=native \
 *     src/core/arena.c src/core/symbol.c src/core/util.c \
 *     src/exec/agg.c src/exec/filter.c src/exec/bucket.c \
 *     tests/test_exec.c -o /tmp/tsdb_test_exec -lpthread && \
 *   /tmp/tsdb_test_exec
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <time.h>

#include "tsdb.h"
#include "../src/exec/simd.h"
#include "../src/exec/agg.h"
#include "../src/exec/filter.h"
#include "../src/exec/bucket.h"

/* -----------------------------------------------------------------------
 * Test framework
 * --------------------------------------------------------------------- */
static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do {                           \
    if (cond) { g_pass++; }                             \
    else {                                              \
        g_fail++;                                       \
        fprintf(stderr, "FAIL  %s:%d  %s\n",           \
                __FILE__, __LINE__, (msg));             \
    }                                                   \
} while (0)

#define CHECKF(cond, fmt, ...) do {                     \
    if (cond) { g_pass++; }                             \
    else {                                              \
        g_fail++;                                       \
        fprintf(stderr, "FAIL  %s:%d  " fmt "\n",      \
                __FILE__, __LINE__, __VA_ARGS__);       \
    }                                                   \
} while (0)

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* -----------------------------------------------------------------------
 * Reference scalar implementations (single-pass, trusted)
 * --------------------------------------------------------------------- */

static double ref_sum_f64(const double *v, size_t n) {
    double s = 0.0;
    for (size_t i = 0; i < n; i++) s += v[i];
    return s;
}
static double ref_min_f64(const double *v, size_t n) {
    double m = (double)INFINITY;
    for (size_t i = 0; i < n; i++) if (v[i] < m) m = v[i];
    return m;
}
static double ref_max_f64(const double *v, size_t n) {
    double m = -(double)INFINITY;
    for (size_t i = 0; i < n; i++) if (v[i] > m) m = v[i];
    return m;
}
static int64_t ref_sum_i64(const int64_t *v, size_t n) {
    int64_t s = 0;
    for (size_t i = 0; i < n; i++) s += v[i];
    return s;
}
static int64_t ref_min_i64(const int64_t *v, size_t n) {
    int64_t m = INT64_MAX;
    for (size_t i = 0; i < n; i++) if (v[i] < m) m = v[i];
    return m;
}
static int64_t ref_max_i64(const int64_t *v, size_t n) {
    int64_t m = INT64_MIN;
    for (size_t i = 0; i < n; i++) if (v[i] > m) m = v[i];
    return m;
}

/* -----------------------------------------------------------------------
 * Test 1: Aggregation correctness (f64)
 * --------------------------------------------------------------------- */
static void test_agg_f64(void) {
    /* Test multiple sizes including boundary cases */
    size_t sizes[] = {1, 2, 3, 4, 7, 8, 15, 16, 63, 64, 65,
                      127, 128, 255, 256, 1023, 1024, 8192, 10000};
    size_t nsizes = sizeof(sizes) / sizeof(sizes[0]);
    size_t maxn = 10000;

    double *v = malloc(maxn * sizeof(double));
    /* Fill with deterministic values: v[i] = (i % 100) - 50 + 0.5 */
    for (size_t i = 0; i < maxn; i++)
        v[i] = (double)(i % 100) - 50.0 + 0.5;

    for (size_t si = 0; si < nsizes; si++) {
        size_t n = sizes[si];
        double out_sum, out_min, out_max;
        uint64_t cnt;

        CHECK(tsdb_agg_sum_f64(v, n, NULL, &out_sum, &cnt) == TSDB_OK,
              "agg_sum_f64 ret");
        double ref_s = ref_sum_f64(v, n);
        CHECKF(fabs(out_sum - ref_s) < 1e-6 * (fabs(ref_s) + 1.0),
               "agg_sum_f64 n=%zu got=%.6g want=%.6g", n, out_sum, ref_s);
        CHECKF((size_t)cnt == n, "agg_sum_f64 count n=%zu cnt=%llu",
               n, (unsigned long long)cnt);

        CHECK(tsdb_agg_min_f64(v, n, NULL, &out_min) == TSDB_OK,
              "agg_min_f64 ret");
        CHECKF(out_min == ref_min_f64(v, n),
               "agg_min_f64 n=%zu got=%.6g want=%.6g",
               n, out_min, ref_min_f64(v, n));

        CHECK(tsdb_agg_max_f64(v, n, NULL, &out_max) == TSDB_OK,
              "agg_max_f64 ret");
        CHECKF(out_max == ref_max_f64(v, n),
               "agg_max_f64 n=%zu got=%.6g want=%.6g",
               n, out_max, ref_max_f64(v, n));
    }
    free(v);
}

/* -----------------------------------------------------------------------
 * Test 2: Aggregation correctness (i64)
 * --------------------------------------------------------------------- */
static void test_agg_i64(void) {
    size_t sizes[] = {1, 2, 3, 7, 8, 15, 16, 63, 64, 65, 1023, 1024, 10000};
    size_t nsizes = sizeof(sizes) / sizeof(sizes[0]);
    size_t maxn = 10000;

    int64_t *v = malloc(maxn * sizeof(int64_t));
    for (size_t i = 0; i < maxn; i++)
        v[i] = (int64_t)(i % 100) - 50;

    for (size_t si = 0; si < nsizes; si++) {
        size_t n = sizes[si];
        int64_t out_sum, out_min, out_max;
        uint64_t cnt;

        CHECK(tsdb_agg_sum_i64(v, n, NULL, &out_sum, &cnt) == TSDB_OK,
              "agg_sum_i64 ret");
        CHECKF(out_sum == ref_sum_i64(v, n),
               "agg_sum_i64 n=%zu got=%lld want=%lld",
               n, (long long)out_sum, (long long)ref_sum_i64(v, n));
        CHECKF((size_t)cnt == n,
               "agg_sum_i64 count n=%zu", n);

        CHECK(tsdb_agg_min_i64(v, n, NULL, &out_min) == TSDB_OK,
              "agg_min_i64 ret");
        CHECKF(out_min == ref_min_i64(v, n),
               "agg_min_i64 n=%zu got=%lld want=%lld",
               n, (long long)out_min, (long long)ref_min_i64(v, n));

        CHECK(tsdb_agg_max_i64(v, n, NULL, &out_max) == TSDB_OK,
              "agg_max_i64 ret");
        CHECKF(out_max == ref_max_i64(v, n),
               "agg_max_i64 n=%zu got=%lld want=%lld",
               n, (long long)out_max, (long long)ref_max_i64(v, n));
    }
    free(v);
}

/* -----------------------------------------------------------------------
 * Test 3: Null-bitmap aggregation
 * --------------------------------------------------------------------- */
static void test_agg_null(void) {
    const size_t n = 64;
    double vf[64];
    int64_t vi[64];
    uint8_t bm[8];  /* 64 elements → 8 bytes, MSB-first */

    /* Only even indices are valid (odd = null) */
    memset(bm, 0, sizeof(bm));
    for (size_t i = 0; i < n; i++) {
        vf[i] = (double)(i + 1);
        vi[i] = (int64_t)(i + 1);
        if (i % 2 == 0) {
            /* Set bit i: byte = i/8, bit = 7 - (i%8) */
            bm[i / 8] |= (uint8_t)(1 << (7 - (i % 8)));
        }
    }

    double out_sum; uint64_t cnt;
    CHECK(tsdb_agg_sum_f64(vf, n, bm, &out_sum, &cnt) == TSDB_OK,
          "null agg_sum_f64 ret");
    /* Sum of v[0], v[2], ..., v[62] = 1 + 3 + 5 + ... + 63
     * = sum of odd integers 1..63 = 32^2 = 1024 */
    CHECKF(fabs(out_sum - 1024.0) < 1e-9,
           "null agg_sum_f64 got=%.1f want=1024", out_sum);
    CHECKF(cnt == 32, "null agg_sum_f64 count got=%llu want=32",
           (unsigned long long)cnt);

    int64_t out_sum_i; uint64_t cnti;
    CHECK(tsdb_agg_sum_i64(vi, n, bm, &out_sum_i, &cnti) == TSDB_OK,
          "null agg_sum_i64 ret");
    CHECKF(out_sum_i == 1024LL,
           "null agg_sum_i64 got=%lld want=1024", (long long)out_sum_i);
    CHECKF(cnti == 32, "null agg_sum_i64 count got=%llu want=32",
           (unsigned long long)cnti);

    double out_min;
    CHECK(tsdb_agg_min_f64(vf, n, bm, &out_min) == TSDB_OK,
          "null agg_min_f64 ret");
    CHECKF(out_min == 1.0, "null agg_min_f64 got=%.1f want=1.0", out_min);

    double out_max;
    CHECK(tsdb_agg_max_f64(vf, n, bm, &out_max) == TSDB_OK,
          "null agg_max_f64 ret");
    CHECKF(out_max == 63.0, "null agg_max_f64 got=%.1f want=63.0", out_max);

    /* All-null edge case */
    memset(bm, 0x00, sizeof(bm));
    CHECK(tsdb_agg_sum_f64(vf, n, bm, &out_sum, &cnt) == TSDB_OK,
          "all-null sum ret");
    CHECKF(out_sum == 0.0 && cnt == 0,
           "all-null sum sum=%.1f cnt=%llu", out_sum, (unsigned long long)cnt);
    CHECK(tsdb_agg_min_f64(vf, n, bm, &out_min) == TSDB_OK,
          "all-null min ret");
    CHECK(isinf(out_min) && out_min > 0,
          "all-null min should be +inf");
    CHECK(tsdb_agg_max_f64(vf, n, bm, &out_max) == TSDB_OK,
          "all-null max ret");
    CHECK(isinf(out_max) && out_max < 0,
          "all-null max should be -inf");
}

/* -----------------------------------------------------------------------
 * Test 4: filter_f64 correctness
 * --------------------------------------------------------------------- */

/* Count bits set in bitmap for first n elements */
static size_t count_set(const uint64_t *bm, size_t n) {
    return (size_t)tsdb_bitmap_popcount(bm, n);
}

/* Check if element i passes the filter bitmap */
static int bm_get(const uint64_t *bm, size_t i) {
    return (int)((bm[i / 64] >> (i & 63)) & 1);
}

static void test_filter_f64(void) {
    const size_t n = 200;
    double v[200];
    for (size_t i = 0; i < n; i++) v[i] = (double)i;

    uint64_t bm[4];

    /* EQ: v == 42 → exactly 1 row */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_EQ, 42.0, bm) == TSDB_OK,
          "filter_f64 EQ ret");
    CHECKF(count_set(bm, n) == 1, "filter_f64 EQ count got=%zu want=1",
           count_set(bm, n));
    CHECK(bm_get(bm, 42), "filter_f64 EQ bit 42 should be set");
    CHECK(!bm_get(bm, 41), "filter_f64 EQ bit 41 should be clear");

    /* NE: v != 42 → 199 rows */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_NE, 42.0, bm) == TSDB_OK,
          "filter_f64 NE ret");
    CHECKF(count_set(bm, n) == 199,
           "filter_f64 NE count got=%zu want=199", count_set(bm, n));

    /* LT: v < 50 → 50 rows (0..49) */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_LT, 50.0, bm) == TSDB_OK,
          "filter_f64 LT ret");
    CHECKF(count_set(bm, n) == 50,
           "filter_f64 LT count got=%zu want=50", count_set(bm, n));
    CHECK(bm_get(bm, 49), "filter_f64 LT bit 49 set");
    CHECK(!bm_get(bm, 50), "filter_f64 LT bit 50 clear");

    /* LE: v <= 50 → 51 rows (0..50) */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_LE, 50.0, bm) == TSDB_OK,
          "filter_f64 LE ret");
    CHECKF(count_set(bm, n) == 51,
           "filter_f64 LE count got=%zu want=51", count_set(bm, n));

    /* GT: v > 100 → 99 rows (101..199) */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_GT, 100.0, bm) == TSDB_OK,
          "filter_f64 GT ret");
    CHECKF(count_set(bm, n) == 99,
           "filter_f64 GT count got=%zu want=99", count_set(bm, n));
    CHECK(!bm_get(bm, 100), "filter_f64 GT bit 100 clear");
    CHECK(bm_get(bm, 101), "filter_f64 GT bit 101 set");

    /* GE: v >= 100 → 100 rows (100..199) */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_GE, 100.0, bm) == TSDB_OK,
          "filter_f64 GE ret");
    CHECKF(count_set(bm, n) == 100,
           "filter_f64 GE count got=%zu want=100", count_set(bm, n));

    /* Composed filter: 50 <= v < 100 → 50 rows */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_GE, 50.0, bm) == TSDB_OK,
          "filter_f64 compose GE ret");
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_LT, 100.0, bm) == TSDB_OK,
          "filter_f64 compose LT ret");
    CHECKF(count_set(bm, n) == 50,
           "filter_f64 compose count got=%zu want=50", count_set(bm, n));
    for (size_t i = 0; i < n; i++) {
        int want = (v[i] >= 50.0 && v[i] < 100.0) ? 1 : 0;
        int got  = bm_get(bm, i);
        CHECKF(got == want,
               "filter_f64 compose bit[%zu] got=%d want=%d", i, got, want);
    }

    /* n=1 boundary */
    double v1[1] = {7.0};
    uint64_t bm1[1]; memset(bm1, 0xFF, sizeof(bm1));
    CHECK(tsdb_filter_f64(v1, 1, TSDB_CMP_EQ, 7.0, bm1) == TSDB_OK,
          "filter_f64 n=1 EQ ret");
    CHECK(bm_get(bm1, 0), "filter_f64 n=1 EQ bit 0 set");

    /* n=65: crosses word boundary */
    double v65[65];
    for (int i = 0; i < 65; i++) v65[i] = (double)i;
    uint64_t bm65[2]; memset(bm65, 0xFF, sizeof(bm65));
    CHECK(tsdb_filter_f64(v65, 65, TSDB_CMP_GE, 64.0, bm65) == TSDB_OK,
          "filter_f64 n=65 GE ret");
    CHECKF(count_set(bm65, 65) == 1,
           "filter_f64 n=65 GE count got=%zu want=1", count_set(bm65, 65));
    CHECK(bm_get(bm65, 64), "filter_f64 n=65 GE bit 64 set");
    CHECK(!bm_get(bm65, 63), "filter_f64 n=65 GE bit 63 clear");
}

/* -----------------------------------------------------------------------
 * Test 5: filter_i64 correctness
 * --------------------------------------------------------------------- */
static void test_filter_i64(void) {
    const size_t n = 128;
    int64_t v[128];
    for (size_t i = 0; i < n; i++) v[i] = (int64_t)i - 64;  /* -64..63 */

    uint64_t bm[2];

    /* EQ: v == 0 → 1 row (index 64) */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_i64(v, n, TSDB_CMP_EQ, 0LL, bm) == TSDB_OK,
          "filter_i64 EQ ret");
    CHECKF(count_set(bm, n) == 1,
           "filter_i64 EQ count got=%zu want=1", count_set(bm, n));
    CHECK(bm_get(bm, 64), "filter_i64 EQ bit 64 set");

    /* LT: v < 0 → 64 rows */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_i64(v, n, TSDB_CMP_LT, 0LL, bm) == TSDB_OK,
          "filter_i64 LT ret");
    CHECKF(count_set(bm, n) == 64,
           "filter_i64 LT count got=%zu want=64", count_set(bm, n));

    /* GT: v > 0 → 63 rows (1..63) */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_i64(v, n, TSDB_CMP_GT, 0LL, bm) == TSDB_OK,
          "filter_i64 GT ret");
    CHECKF(count_set(bm, n) == 63,
           "filter_i64 GT count got=%zu want=63", count_set(bm, n));
}

/* -----------------------------------------------------------------------
 * Test 6: bitmap gather
 * --------------------------------------------------------------------- */
static void test_gather(void) {
    const size_t n = 100;
    double vsrc[100];
    int64_t isrc[100];
    for (size_t i = 0; i < n; i++) { vsrc[i] = (double)i; isrc[i] = (int64_t)i; }

    /* Build bitmap: only even indices pass */
    uint64_t bm[2]; memset(bm, 0, sizeof(bm));
    for (size_t i = 0; i < n; i += 2) TSDB_BM_SET(bm, i);

    double fdst[100];
    size_t got = tsdb_bitmap_gather_f64(bm, vsrc, n, fdst);
    CHECKF(got == 50, "gather_f64 count got=%zu want=50", got);
    for (size_t i = 0; i < got; i++) {
        CHECKF(fdst[i] == vsrc[i * 2],
               "gather_f64 [%zu] got=%.0f want=%.0f",
               i, fdst[i], vsrc[i * 2]);
    }

    int64_t idst[100];
    size_t goti = tsdb_bitmap_gather_i64(bm, isrc, n, idst);
    CHECKF(goti == 50, "gather_i64 count got=%zu want=50", goti);
    for (size_t i = 0; i < goti; i++) {
        CHECKF(idst[i] == isrc[i * 2],
               "gather_i64 [%zu] got=%lld want=%lld",
               i, (long long)idst[i], (long long)isrc[i * 2]);
    }

    /* popcount */
    uint64_t pc = tsdb_bitmap_popcount(bm, n);
    CHECKF(pc == 50, "popcount got=%llu want=50", (unsigned long long)pc);
}

/* -----------------------------------------------------------------------
 * Test 7: bucket_assign
 * --------------------------------------------------------------------- */
static void test_bucket_assign(void) {
    /* 10 timestamps 1ns apart, bucket size 3ns */
    const size_t n = 10;
    int64_t ts[10], bid[10];
    for (size_t i = 0; i < n; i++) ts[i] = (int64_t)i;

    CHECK(tsdb_bucket_assign(ts, n, 0, 3, bid) == TSDB_OK,
          "bucket_assign ret");
    int64_t want[] = {0, 0, 0, 1, 1, 1, 2, 2, 2, 3};
    for (size_t i = 0; i < n; i++) {
        CHECKF(bid[i] == want[i],
               "bucket_assign [%zu] got=%lld want=%lld",
               i, (long long)bid[i], (long long)want[i]);
    }

    /* Power-of-two path: bucket_ns = 4 (=2^2) */
    CHECK(tsdb_bucket_assign(ts, n, 0, 4, bid) == TSDB_OK,
          "bucket_assign pow2 ret");
    for (size_t i = 0; i < n; i++) {
        int64_t w = ts[i] / 4;
        CHECKF(bid[i] == w,
               "bucket_assign pow2 [%zu] got=%lld want=%lld",
               i, (long long)bid[i], (long long)w);
    }

    /* With origin */
    CHECK(tsdb_bucket_assign(ts, n, 2, 3, bid) == TSDB_OK,
          "bucket_assign origin ret");
    /* Bucketing FLOORS, so these are floor((ts-origin)/3), not C's `/`.
     * ts[0]=0: floor(-2/3) = -1 — 0 sits in bucket [-1, 2), one below the
     * bucket the origin opens.  Written as literals because C's `/` gives the
     * other answer (0) and would restate the truncation this pins against. */
    CHECK(bid[0] == -1, "bucket_assign origin [0]");
    CHECK(bid[2] ==  0, "bucket_assign origin [2]");
}

/* -----------------------------------------------------------------------
 * Test 8: bucket_sum_f64
 * --------------------------------------------------------------------- */
static void test_bucket_sum(void) {
    /* --- Single bucket --- */
    {
        int64_t bid[]  = {5, 5, 5, 5};
        double  vals[] = {1.0, 2.0, 3.0, 4.0};
        tsdb_bucket_f64_t out[10]; size_t out_n;
        CHECK(tsdb_bucket_sum_f64(bid, vals, 4, out, 10, &out_n) == TSDB_OK,
              "bucket_sum single-bucket ret");
        CHECKF(out_n == 1, "bucket_sum single-bucket out_n got=%zu want=1", out_n);
        CHECKF(out[0].bucket == 5, "bucket_sum single bucket id=%lld", (long long)out[0].bucket);
        CHECKF(out[0].sum == 10.0, "bucket_sum single sum=%.1f", out[0].sum);
        CHECKF(out[0].count == 4,  "bucket_sum single count=%lld", (long long)out[0].count);
    }

    /* --- Consecutive buckets --- */
    {
        int64_t bid[]  = {0, 0, 1, 1, 2, 2, 3};
        double  vals[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0};
        tsdb_bucket_f64_t out[10]; size_t out_n;
        CHECK(tsdb_bucket_sum_f64(bid, vals, 7, out, 10, &out_n) == TSDB_OK,
              "bucket_sum consecutive ret");
        CHECKF(out_n == 4, "bucket_sum consecutive out_n=%zu want=4", out_n);
        CHECKF(out[0].sum == 3.0,  "bucket_sum b0 sum=%.1f", out[0].sum);
        CHECKF(out[1].sum == 7.0,  "bucket_sum b1 sum=%.1f", out[1].sum);
        CHECKF(out[2].sum == 11.0, "bucket_sum b2 sum=%.1f", out[2].sum);
        CHECKF(out[3].sum == 7.0,  "bucket_sum b3 sum=%.1f", out[3].sum);
        CHECKF(out[3].count == 1,  "bucket_sum b3 count=%lld", (long long)out[3].count);
    }

    /* --- Sparse buckets (big gaps) --- */
    {
        int64_t bid[]  = {0, 0, 1000, 1000, 2000000};
        double  vals[] = {10.0, 20.0, 30.0, 40.0, 50.0};
        tsdb_bucket_f64_t out[10]; size_t out_n;
        CHECK(tsdb_bucket_sum_f64(bid, vals, 5, out, 10, &out_n) == TSDB_OK,
              "bucket_sum sparse ret");
        CHECKF(out_n == 3, "bucket_sum sparse out_n=%zu want=3", out_n);
        CHECKF(out[0].sum == 30.0, "bucket_sum sparse b0=%.1f", out[0].sum);
        CHECKF(out[1].sum == 70.0, "bucket_sum sparse b1=%.1f", out[1].sum);
        CHECKF(out[2].sum == 50.0, "bucket_sum sparse b2=%.1f", out[2].sum);
        CHECKF(out[1].bucket == 1000, "bucket_sum sparse b1.bucket=%lld",
               (long long)out[1].bucket);
        CHECKF(out[2].bucket == 2000000, "bucket_sum sparse b2.bucket=%lld",
               (long long)out[2].bucket);
    }

    /* --- Overflow guard --- */
    {
        int64_t bid[]  = {0, 1, 2};
        double  vals[] = {1.0, 2.0, 3.0};
        tsdb_bucket_f64_t out[2]; size_t out_n;
        int rc = tsdb_bucket_sum_f64(bid, vals, 3, out, 2, &out_n);
        CHECKF(rc == TSDB_ERR_OVERFLOW, "bucket_sum overflow rc=%d", rc);
    }

    /* --- n=1 --- */
    {
        int64_t bid[] = {7};
        double  val[] = {99.0};
        tsdb_bucket_f64_t out[1]; size_t out_n;
        CHECK(tsdb_bucket_sum_f64(bid, val, 1, out, 1, &out_n) == TSDB_OK,
              "bucket_sum n=1 ret");
        CHECKF(out_n == 1 && out[0].sum == 99.0,
               "bucket_sum n=1 out_n=%zu sum=%.1f", out_n, out[0].sum);
    }
}

/* -----------------------------------------------------------------------
 * Test 9: u32 filters
 * --------------------------------------------------------------------- */
static void test_filter_u32(void) {
    const size_t n = 64;
    uint32_t v[64];
    for (size_t i = 0; i < n; i++) v[i] = (uint32_t)(i % 8);

    uint64_t bm[1];

    /* EQ == 3: indices 3, 11, 19, 27, 35, 43, 51, 59 → 8 rows */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_u32_eq(v, n, 3u, bm) == TSDB_OK, "u32_eq ret");
    CHECKF(count_set(bm, n) == 8, "u32_eq count got=%zu want=8", count_set(bm, n));
    for (size_t i = 0; i < n; i++) {
        int want = (v[i] == 3u) ? 1 : 0;
        CHECKF(bm_get(bm, i) == want,
               "u32_eq bit[%zu] got=%d want=%d", i, bm_get(bm, i), want);
    }

    /* IN {2, 5}: indices where v[i] == 2 or v[i] == 5 → 16 rows */
    uint32_t set[] = {2u, 5u};
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_u32_in(v, n, set, 2, bm) == TSDB_OK, "u32_in ret");
    CHECKF(count_set(bm, n) == 16,
           "u32_in count got=%zu want=16", count_set(bm, n));
}

/* -----------------------------------------------------------------------
 * Test 10: popcount edge cases
 * --------------------------------------------------------------------- */
static void test_popcount(void) {
    uint64_t bm[2] = {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL};

    /* Exact word boundary */
    CHECKF(tsdb_bitmap_popcount(bm, 64) == 64,
           "popcount 64 got=%llu", (unsigned long long)tsdb_bitmap_popcount(bm, 64));
    /* Partial second word */
    CHECKF(tsdb_bitmap_popcount(bm, 65) == 65,
           "popcount 65 got=%llu", (unsigned long long)tsdb_bitmap_popcount(bm, 65));
    CHECKF(tsdb_bitmap_popcount(bm, 128) == 128,
           "popcount 128 got=%llu", (unsigned long long)tsdb_bitmap_popcount(bm, 128));
    /* Zero */
    uint64_t zbm[1] = {0};
    CHECKF(tsdb_bitmap_popcount(zbm, 64) == 0,
           "popcount zero got=%llu", (unsigned long long)tsdb_bitmap_popcount(zbm, 64));
    /* n_bits=1 */
    uint64_t one[1] = {1ULL};
    CHECKF(tsdb_bitmap_popcount(one, 1) == 1,
           "popcount n=1 got=%llu", (unsigned long long)tsdb_bitmap_popcount(one, 1));
    /* Stale bits above n_bits must be excluded */
    uint64_t stale[1] = {0xFFFFFFFFFFFFFFFFULL};
    CHECKF(tsdb_bitmap_popcount(stale, 3) == 3,
           "popcount stale got=%llu", (unsigned long long)tsdb_bitmap_popcount(stale, 3));
}

/* -----------------------------------------------------------------------
 * Test 11: Performance — 1M double sum throughput
 * --------------------------------------------------------------------- */
static void test_perf_sum(void) {
    const size_t n = 1 << 20;  /* 1M */
    double *v = malloc(n * sizeof(double));
    if (!v) { fprintf(stderr, "  SKIP: malloc failed for perf test\n"); return; }

    for (size_t i = 0; i < n; i++) v[i] = (double)i * 1e-6;

    double out; uint64_t cnt;
    /* Warmup */
    tsdb_agg_sum_f64(v, n, NULL, &out, &cnt);

    const int REPS = 10;
    double t0 = now_sec();
    for (int r = 0; r < REPS; r++)
        tsdb_agg_sum_f64(v, n, NULL, &out, &cnt);
    double elapsed = now_sec() - t0;

    double bytes = (double)n * sizeof(double) * REPS;
    double gbps  = bytes / elapsed / 1e9;
    printf("  PERF  sum 1M f64: %.2f GB/s  (%.3f ms/call, sum=%.3e)\n",
           gbps, elapsed / REPS * 1000.0, out);
    free(v);
}

/* -----------------------------------------------------------------------
 * main
 * --------------------------------------------------------------------- */
int main(void) {
    printf("=== tsdb exec SIMD tests ===\n");
    printf("  SIMD path: %s\n\n", tsdb_simd_name());

    printf("[1] agg_f64 sum/min/max ...\n");
    test_agg_f64();
    printf("[2] agg_i64 sum/min/max ...\n");
    test_agg_i64();
    printf("[3] agg null-bitmap ...\n");
    test_agg_null();
    printf("[4] filter_f64 EQ/NE/LT/LE/GT/GE ...\n");
    test_filter_f64();
    printf("[5] filter_i64 ...\n");
    test_filter_i64();
    printf("[6] filter_u32 eq/in ...\n");
    test_filter_u32();
    printf("[7] bitmap gather ...\n");
    test_gather();
    printf("[8] popcount edge cases ...\n");
    test_popcount();
    printf("[9] bucket_assign ...\n");
    test_bucket_assign();
    printf("[10] bucket_sum_f64 ...\n");
    test_bucket_sum();
    printf("[11] performance ...\n");
    test_perf_sum();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
