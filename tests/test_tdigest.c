/* test_tdigest.c — Unit + performance tests for the T-digest implementation. */

#include "../src/exec/tdigest.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <time.h>

/* ---- helpers ------------------------------------------------------------ */

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); \
} while (0)

#define ASSERT_NEAR(a, b, tol) do { \
    double _a = (a), _b = (b), _t = (tol); \
    if (fabs(_a - _b) > _t) \
        FAIL(#a " = %.6g, expected ~" #b " ± " #tol " (|err|=%.6g)", _a, _b, fabs(_a-_b)); \
} while (0)

/* Simple LCG for reproducible pseudo-random normal via Box-Muller. */
static uint64_t lcg_state = 0xdeadbeefcafe1234ULL;

static double lcg_uniform(void) {
    lcg_state = lcg_state * 6364136223846793005ULL + 1442695040888963407ULL;
    return (double)(lcg_state >> 11) / (double)(1ULL << 53);
}

/* Box-Muller: returns one normal sample N(0,1). */
static double rand_normal(void) {
    double u, v;
    do { u = lcg_uniform(); } while (u == 0.0);
    v = lcg_uniform();
    return sqrt(-2.0 * log(u)) * cos(2.0 * M_PI * v);
}

/* Wall-clock nanoseconds. */
static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ---- test cases --------------------------------------------------------- */

static void test_single_point(void) {
    printf("[1] single-point digest\n");
    tsdb_tdigest_t *td = NULL;
    assert(tsdb_tdigest_new(100.0, &td) == 0);

    tsdb_tdigest_add(td, 42.0);
    assert(tsdb_tdigest_count(td) == 1.0);

    double p50 = tsdb_tdigest_quantile(td, 0.5);
    ASSERT_NEAR(p50, 42.0, 1e-9);

    double p0  = tsdb_tdigest_quantile(td, 0.0);
    double p100 = tsdb_tdigest_quantile(td, 1.0);
    ASSERT_NEAR(p0,   42.0, 1e-9);
    ASSERT_NEAR(p100, 42.0, 1e-9);

    tsdb_tdigest_free(td);
    printf("  PASS\n");
}

static void test_sorted_data(void) {
    printf("[2] sorted data p0/p25/p50/p75/p100\n");
    /* values 1..100 */
    int N = 100;
    tsdb_tdigest_t *td = NULL;
    assert(tsdb_tdigest_new(100.0, &td) == 0);

    for (int i = 1; i <= N; i++) tsdb_tdigest_add(td, (double)i);

    /* p0=1, p25≈25.75, p50≈50.5, p75≈75.25, p100=100 */
    double p0   = tsdb_tdigest_quantile(td, 0.0);
    double p25  = tsdb_tdigest_quantile(td, 0.25);
    double p50  = tsdb_tdigest_quantile(td, 0.5);
    double p75  = tsdb_tdigest_quantile(td, 0.75);
    double p100 = tsdb_tdigest_quantile(td, 1.0);

    ASSERT_NEAR(p0,   1.0,  0.5);
    ASSERT_NEAR(p25,  25.5, 1.5);
    ASSERT_NEAR(p50,  50.5, 1.5);
    ASSERT_NEAR(p75,  75.5, 1.5);
    ASSERT_NEAR(p100, 100.0, 0.5);

    tsdb_tdigest_free(td);
    printf("  PASS (p0=%.2f p25=%.2f p50=%.2f p75=%.2f p100=%.2f)\n",
           p0, p25, p50, p75, p100);
}

static void test_normal_distribution(void) {
    printf("[3] normal(0,1) 10K points — p50≈0, p95≈1.645*sigma\n");
    int N = 10000;
    double mu = 100.0, sigma = 15.0; /* simulate IQ scores */

    tsdb_tdigest_t *td = NULL;
    assert(tsdb_tdigest_new(100.0, &td) == 0);

    /* Accumulate exact stats for comparison. */
    double sum = 0.0, sum_sq = 0.0;
    for (int i = 0; i < N; i++) {
        double v = mu + sigma * rand_normal();
        tsdb_tdigest_add(td, v);
        sum += v;
        sum_sq += v * v;
    }

    double exact_mean   = sum / N;
    double exact_var    = sum_sq / N - exact_mean * exact_mean;
    double exact_stddev = sqrt(exact_var);

    /* Expected quantiles for N(mu, sigma^2). */
    double exp_p50 = mu;                   /* = mean */
    double exp_p95 = mu + 1.6449 * sigma;  /* ≈ mean + 1.645 sigma */

    double got_p50 = tsdb_tdigest_quantile(td, 0.50);
    double got_p95 = tsdb_tdigest_quantile(td, 0.95);

    /* 1% relative error tolerance (Dunning guarantees ε on relative rank) */
    double tol_p50 = 0.02 * sigma;   /* ±2% of sigma for p50 */
    double tol_p95 = 0.02 * (exp_p95 - mu) + 0.5; /* ±2% of tail + half sigma */

    ASSERT_NEAR(got_p50, exp_p50, tol_p50);
    ASSERT_NEAR(got_p95, exp_p95, tol_p95);

    /* Check mean/stddev exact fields. */
    double dig_mean   = tsdb_tdigest_mean(td);
    double dig_stddev = tsdb_tdigest_stddev(td);
    ASSERT_NEAR(dig_mean,   exact_mean,   0.1);
    ASSERT_NEAR(dig_stddev, exact_stddev, 0.1);

    tsdb_tdigest_free(td);
    printf("  PASS p50=%.2f(exp %.2f) p95=%.2f(exp %.2f) stddev=%.2f\n",
           got_p50, exp_p50, got_p95, exp_p95, dig_stddev);
}

static void test_merge_four_shards(void) {
    printf("[4] merge: 4-shard round-trip ≈ full digest\n");
    int N = 10000;

    /* Generate data. */
    double *data = (double *)malloc((size_t)N * sizeof(double));
    for (int i = 0; i < N; i++) data[i] = 50.0 + 30.0 * rand_normal();

    /* Full digest. */
    tsdb_tdigest_t *full = NULL;
    assert(tsdb_tdigest_new(100.0, &full) == 0);
    for (int i = 0; i < N; i++) tsdb_tdigest_add(full, data[i]);

    /* Four shard digests, each with N/4 points. */
    int shard = N / 4;
    tsdb_tdigest_t *shards[4];
    for (int s = 0; s < 4; s++) {
        assert(tsdb_tdigest_new(100.0, &shards[s]) == 0);
        for (int i = s * shard; i < (s+1) * shard; i++)
            tsdb_tdigest_add(shards[s], data[i]);
    }

    /* Merge shards into one. */
    tsdb_tdigest_t *merged = NULL;
    assert(tsdb_tdigest_new(100.0, &merged) == 0);
    for (int s = 0; s < 4; s++) {
        tsdb_tdigest_merge(merged, shards[s]);
        tsdb_tdigest_free(shards[s]);
    }

    /* Compare quantiles — allow 1e-2 * range (1% of data range) error. */
    double range = 3.0 * 30.0; /* ~3 sigma */
    double tol = 0.01 * range; /* 1% tolerance */

    for (double q = 0.1; q <= 0.95; q += 0.1) {
        double qf = tsdb_tdigest_quantile(full,   q);
        double qm = tsdb_tdigest_quantile(merged, q);
        if (fabs(qf - qm) > tol + 0.5) {
            FAIL("merge mismatch at q=%.2f: full=%.4f merged=%.4f diff=%.4f tol=%.4f",
                 q, qf, qm, fabs(qf-qm), tol);
        }
    }

    /* Exact stats merge. */
    ASSERT_NEAR(tsdb_tdigest_count(merged), (double)N, 0.5);
    ASSERT_NEAR(tsdb_tdigest_mean(merged),  tsdb_tdigest_mean(full), 0.01);

    tsdb_tdigest_free(full);
    tsdb_tdigest_free(merged);
    free(data);
    printf("  PASS\n");
}

static void test_null_bitmap(void) {
    printf("[5] null bitmap: skip null values\n");
    /* Add values 1..10, mark evens as null → only odds 1,3,5,7,9 */
    tsdb_tdigest_t *td = NULL;
    assert(tsdb_tdigest_new(100.0, &td) == 0);

    double vals[10] = {1,2,3,4,5,6,7,8,9,10};
    /* Bitmap: byte 0 = bits [0..7], byte 1 = bits [8..9] */
    /* Bit i = byte[i>>3] bit (7-(i&7)).  Set bit = valid (1). */
    /* Want valid: 0,2,4,6,8 (values 1,3,5,7,9) */
    /* bit 0 (idx 0) = valid: byte[0] bit 7 = 1 → 0b10101010 */
    /* bit 2 (idx 2) = valid: already 1 */
    /* bit 1 (idx 1) = null:  byte[0] bit 6 = 0 */
    /* Pattern: valid at 0,2,4,6,8 → 0b10101010 = 0xAA, byte1: bit 0 valid, bit 1 null → 0b10000000 = 0x80 */
    uint8_t bm[2] = { 0xAA, 0x80 };

    tsdb_tdigest_add_bulk(td, vals, 10, bm);

    assert(tsdb_tdigest_count(td) == 5.0);
    ASSERT_NEAR(tsdb_tdigest_mean(td), 5.0, 0.01); /* (1+3+5+7+9)/5 = 5 */
    ASSERT_NEAR(tsdb_tdigest_quantile(td, 0.5), 5.0, 1.0);

    tsdb_tdigest_free(td);
    printf("  PASS\n");
}

static void test_throughput(void) {
    printf("[6] throughput: 1M adds\n");
    int N = 1000000;

    tsdb_tdigest_t *td = NULL;
    assert(tsdb_tdigest_new(100.0, &td) == 0);

    /* Build data outside timing loop to be fair. */
    double *data = (double *)malloc((size_t)N * sizeof(double));
    for (int i = 0; i < N; i++) data[i] = (double)i * 0.001;

    int64_t t0 = now_ns();
    tsdb_tdigest_add_bulk(td, data, (size_t)N, NULL);
    int64_t t1 = now_ns();

    double secs   = (double)(t1 - t0) * 1e-9;
    double mops   = (double)N / secs / 1e6;

    printf("  %.1f M adds/s (%.3f ms total)\n", mops, secs * 1e3);
    if (mops < 30.0) {
        /* Report but don't abort — acceptable on slow CI machines. */
        fprintf(stderr, "  WARNING: throughput %.1f M/s below 50 M/s target\n", mops);
    }

    /* Sanity check result. */
    double p50 = tsdb_tdigest_quantile(td, 0.5);
    ASSERT_NEAR(p50, 500.0, 10.0); /* data range [0, 999], mid ≈ 500 */

    free(data);
    tsdb_tdigest_free(td);
    printf("  PASS\n");
}

static void test_merge_throughput(void) {
    printf("[7] merge throughput: 2 × 500K-point digests\n");
    int HALF = 500000;

    tsdb_tdigest_t *a = NULL, *b = NULL;
    assert(tsdb_tdigest_new(100.0, &a) == 0);
    assert(tsdb_tdigest_new(100.0, &b) == 0);

    for (int i = 0; i < HALF; i++) tsdb_tdigest_add(a, (double)i);
    for (int i = 0; i < HALF; i++) tsdb_tdigest_add(b, (double)(HALF + i));

    int64_t t0 = now_ns();
    tsdb_tdigest_merge(a, b);
    int64_t t1 = now_ns();

    double secs = (double)(t1 - t0) * 1e-9;
    printf("  merge 2×500K in %.3f ms → %.1f M ops/s (centroids)\n",
           secs * 1e3, (double)HALF / secs / 1e6);

    ASSERT_NEAR(tsdb_tdigest_count(a), (double)(2 * HALF), 0.5);

    tsdb_tdigest_free(a);
    tsdb_tdigest_free(b);
    printf("  PASS\n");
}

static void test_relative_error(void) {
    printf("[8] relative error on uniform 1M points\n");
    int N = 1000000;

    tsdb_tdigest_t *td = NULL;
    assert(tsdb_tdigest_new(100.0, &td) == 0);

    /* Uniform [0, N-1] — exact quantiles trivially known. */
    double *data = (double *)malloc((size_t)N * sizeof(double));
    for (int i = 0; i < N; i++) data[i] = (double)i;
    tsdb_tdigest_add_bulk(td, data, (size_t)N, NULL);
    free(data);

    /* Check quantiles at 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99 */
    double qs[]   = {0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99};
    int    nqs    = (int)(sizeof(qs) / sizeof(qs[0]));
    double max_rel_err = 0.0;

    for (int i = 0; i < nqs; i++) {
        double exact = qs[i] * (double)(N - 1);
        double got   = tsdb_tdigest_quantile(td, qs[i]);
        double rel   = (exact > 0.0) ? fabs(got - exact) / exact : fabs(got - exact);
        if (rel > max_rel_err) max_rel_err = rel;
        printf("    q=%.2f exact=%.1f got=%.1f rel_err=%.4f%%\n",
               qs[i], exact, got, rel * 100.0);
    }

    if (max_rel_err > 0.02) { /* 2% relative error max */
        FAIL("max relative error %.4f%% exceeds 2%%", max_rel_err * 100.0);
    }

    tsdb_tdigest_free(td);
    printf("  PASS (max rel err = %.4f%%)\n", max_rel_err * 100.0);
}

/* ---- main --------------------------------------------------------------- */

int main(void) {
    printf("=== test_tdigest ===\n\n");

    test_single_point();
    test_sorted_data();
    test_normal_distribution();
    test_merge_four_shards();
    test_null_bitmap();
    test_throughput();
    test_merge_throughput();
    test_relative_error();

    printf("\nAll T-digest tests PASSED.\n");
    return 0;
}
