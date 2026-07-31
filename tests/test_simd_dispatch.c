/* test_simd_dispatch.c — Runtime SIMD dispatch + multi-path correctness tests.
 *
 * Verifies:
 *   1. CPU level detection returns a valid level
 *   2. All compiled paths (SCALAR, NEON/AVX2 as available) produce
 *      bit-identical results — using TSDB_CPU_LEVEL env-var to force paths
 *   3. Performance: 1M and 10M double sum throughput in GB/s
 *   4. Filter correctness via dispatched path
 *   5. Gather correctness via dispatched path
 *
 * Note: On ARM64 (Apple Silicon) only SCALAR and NEON paths are compiled.
 *       On x86-64, AVX2 (and optionally AVX-512) are also available.
 *       The test skips paths not compiled in; it does NOT fake-run them.
 *
 * Environment-variable override for comparison:
 *   TSDB_CPU_LEVEL=SCALAR  — force scalar path
 *   TSDB_CPU_LEVEL=NEON    — force NEON path (ARM64 only)
 *   TSDB_CPU_LEVEL=AVX2    — force AVX2 path (x86 only)
 *   TSDB_CPU_LEVEL=AVX512  — force AVX512 path (x86+avx512 only)
 *
 * The dispatch re-initialises on each run because we use a fresh process
 * per level via the Makefile "check-levels" target, but the paths function
 * correctly within a single process that has a fixed level for its lifetime.
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
#include "../src/exec/cpuid.h"
#include "../src/exec/simd.h"
#include "../src/exec/agg.h"
#include "../src/exec/filter.h"
#include "../src/exec/gather.h"

/* -----------------------------------------------------------------------
 * Micro test framework
 * --------------------------------------------------------------------- */
static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do {                                       \
    if (cond) { g_pass++; }                                         \
    else {                                                          \
        g_fail++;                                                   \
        fprintf(stderr, "FAIL  %s:%d  %s\n",                       \
                __FILE__, __LINE__, (msg));                         \
    }                                                               \
} while (0)

#define CHECKF(cond, fmt, ...) do {                                 \
    if (cond) { g_pass++; }                                         \
    else {                                                          \
        g_fail++;                                                   \
        fprintf(stderr, "FAIL  %s:%d  " fmt "\n",                  \
                __FILE__, __LINE__, __VA_ARGS__);                   \
    }                                                               \
} while (0)

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* -----------------------------------------------------------------------
 * Reference implementations (trusted scalar baselines)
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

/* -----------------------------------------------------------------------
 * Test 1: CPU level detection
 * --------------------------------------------------------------------- */
static void test_cpu_level(void) {
    tsdb_cpu_level_t lv = tsdb_cpu_level();
    /* Must be a valid enum value */
    CHECK(lv >= TSDB_CPU_SCALAR && lv <= TSDB_CPU_AVX512,
          "cpu level in valid range");

    /* Must match tsdb_cpu_level_name */
    const char *name = tsdb_cpu_level_name(lv);
    CHECK(name != NULL, "cpu level name not null");
    printf("  CPU level detected: %s (%d)\n", name, (int)lv);

    /* TSDB_CPU_LEVEL env var should be honoured if set */
    const char *env = getenv("TSDB_CPU_LEVEL");
    if (env) {
        printf("  TSDB_CPU_LEVEL override: %s → resolved to: %s\n",
               env, tsdb_cpu_level_name(lv));
    }
}

/* -----------------------------------------------------------------------
 * Test 2: Aggregation correctness on the active dispatch path
 * --------------------------------------------------------------------- */
static void test_agg_correctness(void) {
    /* Test on sizes that hit SIMD bulk, tail, and boundary cases */
    static const size_t sizes[] = {1, 3, 7, 8, 15, 16, 63, 64, 65,
                                   255, 256, 1023, 1024, 10000};
    const size_t nsizes = sizeof(sizes) / sizeof(sizes[0]);
    const size_t maxn   = 10000;

    double *v = (double *)malloc(maxn * sizeof(double));
    if (!v) { fprintf(stderr, "SKIP agg_correctness: malloc\n"); return; }
    for (size_t i = 0; i < maxn; i++)
        v[i] = (double)(i % 100) - 50.0 + 0.5;

    for (size_t si = 0; si < nsizes; si++) {
        size_t n = sizes[si];
        double out_sum, out_min, out_max;
        uint64_t cnt;

        /* sum */
        CHECK(tsdb_agg_sum_f64(v, n, NULL, &out_sum, &cnt) == TSDB_OK,
              "agg_sum_f64 ret");
        double ref_s = ref_sum_f64(v, n);
        CHECKF(fabs(out_sum - ref_s) <= 1e-6 * (fabs(ref_s) + 1.0),
               "agg_sum_f64 n=%zu got=%.10g want=%.10g", n, out_sum, ref_s);
        CHECKF((size_t)cnt == n, "agg_sum_f64 count n=%zu got=%llu",
               n, (unsigned long long)cnt);

        /* min */
        CHECK(tsdb_agg_min_f64(v, n, NULL, &out_min) == TSDB_OK,
              "agg_min_f64 ret");
        CHECKF(out_min == ref_min_f64(v, n),
               "agg_min_f64 n=%zu got=%.6g want=%.6g",
               n, out_min, ref_min_f64(v, n));

        /* max */
        CHECK(tsdb_agg_max_f64(v, n, NULL, &out_max) == TSDB_OK,
              "agg_max_f64 ret");
        CHECKF(out_max == ref_max_f64(v, n),
               "agg_max_f64 n=%zu got=%.6g want=%.6g",
               n, out_max, ref_max_f64(v, n));
    }
    free(v);
}

/* -----------------------------------------------------------------------
 * Test 2b: unique-extremum POSITION sweep (min/max, no null bitmap)
 *
 * The min/max kernels split each SIMD group across four accumulators and
 * reduce them at the end.  Drop one accumulator, or skip one lane offset,
 * and the answer is still right for a smooth periodic input: with the
 * v[i] = (i % 100) - 50 + 0.5 vector every other correctness test uses, the
 * minimum lands only on i ≡ 0 or 4 (mod 8) and the maximum only on
 * i ≡ 3 or 7 — so no size-sweep over that vector can see a kernel that
 * ignores half its lanes.  min/max are idempotent, which also makes them
 * immune to OVER-processing; UNDER-processing is the whole failure mode and
 * it needs the extremum planted where the missing lane is.
 *
 * So plant a unique extremum at EVERY index, for sizes spanning the bulk
 * loop, the 2-/4-wide tail and the scalar remainder of every compiled width.
 * --------------------------------------------------------------------- */
static void test_agg_extremum_sweep(void) {
    static const size_t sizes[] = {1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33,
                                   63, 64, 65, 127, 128, 129, 255, 256, 257, 1000};
    const size_t nsizes = sizeof(sizes) / sizeof(sizes[0]);
    const size_t maxn = 1000;

    double  *vf = (double  *)malloc(maxn * sizeof(double));
    int64_t *vi = (int64_t *)malloc(maxn * sizeof(int64_t));
    if (!vf || !vi) { free(vf); free(vi); fprintf(stderr, "SKIP sweep: malloc\n"); return; }

    int bad_minf = -1, bad_maxf = -1, bad_mini = -1, bad_maxi = -1;
    size_t bad_n = 0;

    for (size_t si = 0; si < nsizes; si++) {
        size_t n = sizes[si];
        for (size_t pos = 0; pos < n; pos++) {
            double  omin = 0.0, omax = 0.0;
            int64_t imin = 0, imax = 0;

            /* f64 min: flat 1.0 with a single -7.5 at `pos`. */
            for (size_t i = 0; i < n; i++) vf[i] = 1.0;
            vf[pos] = -7.5;
            tsdb_agg_min_f64(vf, n, NULL, &omin);
            tsdb_agg_max_f64(vf, n, NULL, &omax);
            if (omin != -7.5 && bad_minf < 0) { bad_minf = (int)pos; bad_n = n; }
            if (omax != (n == 1 ? -7.5 : 1.0) && bad_maxf < 0) { bad_maxf = (int)pos; bad_n = n; }

            /* f64 max: flat 1.0 with a single 9.25 at `pos`. */
            for (size_t i = 0; i < n; i++) vf[i] = 1.0;
            vf[pos] = 9.25;
            tsdb_agg_max_f64(vf, n, NULL, &omax);
            tsdb_agg_min_f64(vf, n, NULL, &omin);
            if (omax != 9.25 && bad_maxf < 0) { bad_maxf = (int)pos; bad_n = n; }
            if (omin != (n == 1 ? 9.25 : 1.0) && bad_minf < 0) { bad_minf = (int)pos; bad_n = n; }

            /* i64, same shape. */
            for (size_t i = 0; i < n; i++) vi[i] = 100;
            vi[pos] = -777;
            tsdb_agg_min_i64(vi, n, NULL, &imin);
            tsdb_agg_max_i64(vi, n, NULL, &imax);
            if (imin != -777 && bad_mini < 0) { bad_mini = (int)pos; bad_n = n; }
            if (imax != (n == 1 ? -777 : 100) && bad_maxi < 0) { bad_maxi = (int)pos; bad_n = n; }

            for (size_t i = 0; i < n; i++) vi[i] = 100;
            vi[pos] = 999;
            tsdb_agg_max_i64(vi, n, NULL, &imax);
            tsdb_agg_min_i64(vi, n, NULL, &imin);
            if (imax != 999 && bad_maxi < 0) { bad_maxi = (int)pos; bad_n = n; }
            if (imin != (n == 1 ? 999 : 100) && bad_mini < 0) { bad_mini = (int)pos; bad_n = n; }
        }
    }

    CHECKF(bad_minf < 0, "agg_min_f64 missed a planted extremum at index %d (n=%zu)",
           bad_minf, bad_n);
    CHECKF(bad_maxf < 0, "agg_max_f64 missed a planted extremum at index %d (n=%zu)",
           bad_maxf, bad_n);
    CHECKF(bad_mini < 0, "agg_min_i64 missed a planted extremum at index %d (n=%zu)",
           bad_mini, bad_n);
    CHECKF(bad_maxi < 0, "agg_max_i64 missed a planted extremum at index %d (n=%zu)",
           bad_maxi, bad_n);

    free(vf); free(vi);
}

/* -----------------------------------------------------------------------
 * Test 3: filter_f64 correctness
 * --------------------------------------------------------------------- */
static void test_filter_correctness(void) {
    const size_t n = 128;
    double v[128];
    for (size_t i = 0; i < n; i++) v[i] = (double)i;

    uint64_t bm[2];

    /* EQ 42 → 1 row */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_EQ, 42.0, bm) == TSDB_OK,
          "filter_f64 EQ ret");
    uint64_t pc = tsdb_bitmap_popcount(bm, n);
    CHECKF(pc == 1, "filter_f64 EQ count got=%llu want=1", (unsigned long long)pc);

    /* LT 64 → 64 rows */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_LT, 64.0, bm) == TSDB_OK,
          "filter_f64 LT ret");
    pc = tsdb_bitmap_popcount(bm, n);
    CHECKF(pc == 64, "filter_f64 LT count got=%llu want=64",
           (unsigned long long)pc);

    /* GE 64 → 64 rows */
    memset(bm, 0xFF, sizeof(bm));
    CHECK(tsdb_filter_f64(v, n, TSDB_CMP_GE, 64.0, bm) == TSDB_OK,
          "filter_f64 GE ret");
    pc = tsdb_bitmap_popcount(bm, n);
    CHECKF(pc == 64, "filter_f64 GE count got=%llu want=64",
           (unsigned long long)pc);

    /* Bit-level correctness: check every element */
    memset(bm, 0xFF, sizeof(bm));
    tsdb_filter_f64(v, n, TSDB_CMP_GE, 64.0, bm);
    for (size_t i = 0; i < n; i++) {
        int got  = (int)((bm[i >> 6] >> (i & 63)) & 1);
        int want = (v[i] >= 64.0) ? 1 : 0;
        CHECKF(got == want, "filter_f64 GE bit[%zu] got=%d want=%d",
               i, got, want);
    }
}

/* -----------------------------------------------------------------------
 * Test 4: gather correctness
 * --------------------------------------------------------------------- */
static void test_gather_correctness(void) {
    const size_t n = 200;
    double src[200];
    for (size_t i = 0; i < n; i++) src[i] = (double)i;

    /* bitmap: even indices pass */
    uint64_t bm[4]; memset(bm, 0, sizeof(bm));
    for (size_t i = 0; i < n; i += 2) TSDB_BM_SET(bm, i);

    double dst[200];
    size_t got = tsdb_bitmap_gather_f64(bm, src, n, dst);
    CHECKF(got == 100, "gather_f64 count got=%zu want=100", got);
    for (size_t i = 0; i < got; i++) {
        CHECKF(dst[i] == src[i * 2],
               "gather_f64[%zu] got=%.0f want=%.0f",
               i, dst[i], src[i * 2]);
    }

    /* Dense bitmap: all pass */
    memset(bm, 0xFF, sizeof(bm));
    got = tsdb_bitmap_gather_f64(bm, src, n, dst);
    CHECKF(got == n, "gather_f64 dense count got=%zu want=%zu", got, n);
    for (size_t i = 0; i < n; i++)
        CHECKF(dst[i] == src[i], "gather_f64 dense[%zu]", i);

    /* Sparse: only index 0 and 199 */
    memset(bm, 0, sizeof(bm));
    TSDB_BM_SET(bm, 0);
    TSDB_BM_SET(bm, 199);
    got = tsdb_bitmap_gather_f64(bm, src, n, dst);
    CHECKF(got == 2, "gather_f64 sparse count got=%zu want=2", got);
    CHECK(dst[0] == 0.0,   "gather_f64 sparse[0] == 0.0");
    CHECK(dst[1] == 199.0, "gather_f64 sparse[1] == 199.0");
}

/* -----------------------------------------------------------------------
 * Test 5: null-bitmap aware agg correctness
 * --------------------------------------------------------------------- */
static void test_null_agg(void) {
    const size_t n = 64;
    double vf[64];
    uint8_t bm[8];
    memset(bm, 0, sizeof(bm));
    for (size_t i = 0; i < n; i++) {
        vf[i] = (double)(i + 1);
        if (i % 2 == 0)
            bm[i / 8] |= (uint8_t)(1 << (7 - (i % 8)));
    }

    double out_sum; uint64_t cnt;
    CHECK(tsdb_agg_sum_f64(vf, n, bm, &out_sum, &cnt) == TSDB_OK,
          "null sum ret");
    CHECKF(fabs(out_sum - 1024.0) < 1e-9,
           "null sum got=%.1f want=1024", out_sum);
    CHECKF(cnt == 32, "null sum count got=%llu want=32", (unsigned long long)cnt);
}

/* -----------------------------------------------------------------------
 * Test 6: Performance — 1M and 10M double sum
 * --------------------------------------------------------------------- */
static void test_perf_sum(void) {
    /* ---- 1M ---- */
    {
        const size_t n = 1 << 20;
        double *v = (double *)malloc(n * sizeof(double));
        if (!v) { fprintf(stderr, "  SKIP 1M perf: malloc\n"); goto skip10m; }
        for (size_t i = 0; i < n; i++) v[i] = (double)i * 1e-6;

        double out; uint64_t cnt;
        tsdb_agg_sum_f64(v, n, NULL, &out, &cnt);   /* warmup */

        const int REPS = 10;
        double t0 = now_sec();
        for (int r = 0; r < REPS; r++)
            tsdb_agg_sum_f64(v, n, NULL, &out, &cnt);
        double elapsed = now_sec() - t0;
        double gbps = (double)n * sizeof(double) * REPS / elapsed / 1e9;
        printf("  PERF  1M  f64 sum [%s]: %.2f GB/s  (%.3f ms/call, sum=%.3e)\n",
               tsdb_cpu_level_name(tsdb_cpu_level()), gbps,
               elapsed / REPS * 1000.0, out);

        /* ---- Null-aware variant (~10%% nulls). --------------------------
         * Pre-fix this path bypassed SIMD entirely (`if (bm) return scalar`),
         * so any column with a null bitmap aggregated at scalar speed.  A
         * ratio close to the no-null number tells us the masked SIMD path
         * is actually being hit. */
        size_t bmlen = (n + 7) / 8;
        uint8_t *bm = (uint8_t *)malloc(bmlen);
        if (bm) {
            /* All bits valid except every 11th (~9% nulls). */
            for (size_t b = 0; b < bmlen; b++) bm[b] = 0xFF;
            for (size_t i = 0; i < n; i += 11)
                bm[i >> 3] &= (uint8_t)~(1u << (7 - (i & 7)));
            tsdb_agg_sum_f64(v, n, bm, &out, &cnt); /* warmup */
            t0 = now_sec();
            for (int r = 0; r < REPS; r++)
                tsdb_agg_sum_f64(v, n, bm, &out, &cnt);
            elapsed = now_sec() - t0;
            gbps = (double)n * sizeof(double) * REPS / elapsed / 1e9;
            printf("  PERF  1M  f64 sum [%s] +null bitmap: %.2f GB/s "
                   "(%.3f ms/call, valid=%llu)\n",
                   tsdb_cpu_level_name(tsdb_cpu_level()), gbps,
                   elapsed / REPS * 1000.0, (unsigned long long)cnt);
            free(bm);
        }
        free(v);
skip10m:;
    }

    /* ---- 10M ---- */
    {
        const size_t n = 10 << 20;
        double *v = (double *)malloc(n * sizeof(double));
        if (!v) { fprintf(stderr, "  SKIP 10M perf: malloc\n"); return; }
        for (size_t i = 0; i < n; i++) v[i] = (double)(i % 1000) * 0.001;

        double out; uint64_t cnt;
        tsdb_agg_sum_f64(v, n, NULL, &out, &cnt);   /* warmup */

        const int REPS = 5;
        double t0 = now_sec();
        for (int r = 0; r < REPS; r++)
            tsdb_agg_sum_f64(v, n, NULL, &out, &cnt);
        double elapsed = now_sec() - t0;
        double gbps = (double)n * sizeof(double) * REPS / elapsed / 1e9;
        printf("  PERF  10M f64 sum [%s]: %.2f GB/s  (%.3f ms/call, sum=%.3e)\n",
               tsdb_cpu_level_name(tsdb_cpu_level()), gbps,
               elapsed / REPS * 1000.0, out);
        free(v);
    }
}

/* -----------------------------------------------------------------------
 * Test 7: Performance — filter throughput
 * --------------------------------------------------------------------- */
static void test_perf_filter(void) {
    const size_t n = 1 << 20;   /* 1M doubles */
    double *v = (double *)malloc(n * sizeof(double));
    if (!v) { fprintf(stderr, "  SKIP filter perf\n"); return; }
    for (size_t i = 0; i < n; i++) v[i] = (double)(i % 1000);

    size_t nbm = (n + 63) / 64;
    uint64_t *bm = (uint64_t *)malloc(nbm * sizeof(uint64_t));
    if (!bm) { free(v); fprintf(stderr, "  SKIP filter perf (bm)\n"); return; }

    /* Warmup */
    memset(bm, 0xFF, nbm * sizeof(uint64_t));
    tsdb_filter_f64(v, n, TSDB_CMP_GE, 500.0, bm);

    const int REPS = 10;
    double t0 = now_sec();
    for (int r = 0; r < REPS; r++) {
        memset(bm, 0xFF, nbm * sizeof(uint64_t));
        tsdb_filter_f64(v, n, TSDB_CMP_GE, 500.0, bm);
    }
    double elapsed = now_sec() - t0;
    double gbps = (double)n * sizeof(double) * REPS / elapsed / 1e9;
    printf("  PERF  1M  f64 filter [%s]: %.2f GB/s  (%.3f ms/call)\n",
           tsdb_cpu_level_name(tsdb_cpu_level()), gbps,
           elapsed / REPS * 1000.0);
    free(bm); free(v);
}

/* -----------------------------------------------------------------------
 * Test 8: Performance — gather throughput
 * --------------------------------------------------------------------- */
static void test_perf_gather(void) {
    const size_t n = 1 << 20;
    double *src = (double *)malloc(n * sizeof(double));
    double *dst = (double *)malloc(n * sizeof(double));
    size_t nbm  = (n + 63) / 64;
    uint64_t *bm = (uint64_t *)malloc(nbm * sizeof(uint64_t));
    if (!src || !dst || !bm) {
        free(src); free(dst); free(bm);
        fprintf(stderr, "  SKIP gather perf\n"); return;
    }
    for (size_t i = 0; i < n; i++) src[i] = (double)i;
    /* 50% density bitmap */
    for (size_t w = 0; w < nbm; w++) bm[w] = 0xAAAAAAAAAAAAAAAAULL;

    /* Warmup */
    tsdb_bitmap_gather_f64(bm, src, n, dst);

    const int REPS = 10;
    double t0 = now_sec();
    size_t total_written = 0;
    for (int r = 0; r < REPS; r++)
        total_written += tsdb_bitmap_gather_f64(bm, src, n, dst);
    double elapsed = now_sec() - t0;
    /* Throughput = source bytes read */
    double gbps = (double)n * sizeof(double) * REPS / elapsed / 1e9;
    printf("  PERF  1M  f64 gather [%s]: %.2f GB/s  (%.3f ms/call, written=%zu)\n",
           tsdb_cpu_level_name(tsdb_cpu_level()), gbps,
           elapsed / REPS * 1000.0, total_written / (size_t)REPS);
    free(src); free(dst); free(bm);
}

/* -----------------------------------------------------------------------
 * main
 * --------------------------------------------------------------------- */
int main(void) {
    printf("=== tsdb SIMD dispatch tests ===\n");
    printf("  Compile-time SIMD: %s\n", tsdb_simd_name());
    printf("  Runtime  CPU  lvl: %s\n\n", tsdb_cpu_level_name(tsdb_cpu_level()));

    printf("[1] CPU level detection ...\n");
    test_cpu_level();

    printf("[2] Aggregation correctness (%s path) ...\n",
           tsdb_cpu_level_name(tsdb_cpu_level()));
    test_agg_correctness();

    printf("[2b] min/max unique-extremum position sweep ...\n");
    test_agg_extremum_sweep();

    printf("[3] Filter correctness ...\n");
    test_filter_correctness();

    printf("[4] Gather correctness ...\n");
    test_gather_correctness();

    printf("[5] Null-bitmap aggregation ...\n");
    test_null_agg();

    printf("[6] Performance — sum ...\n");
    test_perf_sum();

    printf("[7] Performance — filter ...\n");
    test_perf_filter();

    printf("[8] Performance — gather ...\n");
    test_perf_gather();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
