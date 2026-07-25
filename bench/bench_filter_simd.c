/* bench_filter_simd.c — isolate the vectorized predicate filters.
 *
 * Why this exists: the AVX2 f64 kernels are compiled out of every deployment
 * image (-DTSDB_NO_AVX2_FILTER in deployment/Dockerfile), because passing a
 * runtime predicate to _mm256_cmp_pd does not build under gcc.  Measuring that
 * through bench_operators is hopeless on a busy host — a full query drags in
 * decode, threads and I/O, and the run-to-run noise swamps the kernel.
 *
 * So call tsdb_filter_* directly on an in-RAM array: no DB, no threads, no
 * disk.  Reports ns/value and Mvalues/s per (type, predicate), plus a
 * checksum of the resulting bitmap so a "fast" kernel that computes the wrong
 * answer cannot masquerade as a win.
 *
 *   ./build/bench/bench_filter_simd [n_values] [iters]
 */
#include "../src/exec/filter.h"
#include "../src/exec/simd.h"
#include "../include/tsdb.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* popcount over the bitmap — catches a kernel that returns wrong bits. */
static uint64_t bm_popcount(const uint64_t *bm, size_t nw) {
    uint64_t c = 0;
    for (size_t i = 0; i < nw; i++) c += (uint64_t)__builtin_popcountll(bm[i]);
    return c;
}

static void bm_reset(uint64_t *bm, size_t nw) { memset(bm, 0xFF, nw * 8); }

static const char *op_name(tsdb_cmp_t op) {
    switch (op) {
    case TSDB_CMP_EQ: return "EQ";
    case TSDB_CMP_NE: return "NE";
    case TSDB_CMP_LT: return "LT";
    case TSDB_CMP_LE: return "LE";
    case TSDB_CMP_GT: return "GT";
    case TSDB_CMP_GE: return "GE";
    default:          return "??";
    }
}

int main(int argc, char **argv) {
    size_t n     = (argc > 1) ? (size_t)strtoull(argv[1], NULL, 10) : 1000000;
    int    iters = (argc > 2) ? atoi(argv[2]) : 200;
    size_t nw    = (n + 63) / 64;

    double   *f = malloc(n * sizeof(double));
    int64_t  *i64 = malloc(n * sizeof(int64_t));
    uint64_t *bm  = malloc(nw * sizeof(uint64_t));
    if (!f || !i64 || !bm) { fprintf(stderr, "oom\n"); return 1; }

    /* Deterministic, no libc rand: a cheap LCG so runs are comparable. */
    uint64_t s = 88172645463325252ULL;
    for (size_t k = 0; k < n; k++) {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17;
        f[k]   = (double)(int64_t)(s % 200000) * 0.01 - 1000.0;   /* [-1000,1000) */
        i64[k] = (int64_t)(s % 1000000);
    }

    printf("=== bench_filter_simd ===\n");
    printf("cpu_level    = %d  (0=scalar 1=neon/sse 2=avx2 3=avx512, see tsdb_cpu_level)\n",
           (int)tsdb_cpu_level());
    printf("values       = %zu   iters = %d\n\n", n, iters);
    printf("  kind   op    ns/value   Mvalues/s     selected\n");
    printf("  ----   --    --------   ---------     --------\n");

    const tsdb_cmp_t ops[] = { TSDB_CMP_LT, TSDB_CMP_LE, TSDB_CMP_GT,
                               TSDB_CMP_GE, TSDB_CMP_EQ, TSDB_CMP_NE };

    for (size_t o = 0; o < sizeof(ops) / sizeof(ops[0]); o++) {
        /* warm */
        bm_reset(bm, nw);
        tsdb_filter_f64(f, n, ops[o], 0.0, bm);
        uint64_t sel = bm_popcount(bm, nw);

        double t0 = now_s();
        for (int it = 0; it < iters; it++) {
            bm_reset(bm, nw);
            tsdb_filter_f64(f, n, ops[o], 0.0, bm);
        }
        double dt = now_s() - t0;
        double nsv = dt * 1e9 / ((double)n * iters);
        printf("  f64    %-2s    %8.3f   %9.1f     %llu\n",
               op_name(ops[o]), nsv, 1000.0 / nsv, (unsigned long long)sel);
    }
    printf("\n");
    for (size_t o = 0; o < sizeof(ops) / sizeof(ops[0]); o++) {
        bm_reset(bm, nw);
        tsdb_filter_i64(i64, n, ops[o], 500000, bm);
        uint64_t sel = bm_popcount(bm, nw);

        double t0 = now_s();
        for (int it = 0; it < iters; it++) {
            bm_reset(bm, nw);
            tsdb_filter_i64(i64, n, ops[o], 500000, bm);
        }
        double dt = now_s() - t0;
        double nsv = dt * 1e9 / ((double)n * iters);
        printf("  i64    %-2s    %8.3f   %9.1f     %llu\n",
               op_name(ops[o]), nsv, 1000.0 / nsv, (unsigned long long)sel);
    }

    free(f); free(i64); free(bm);
    return 0;
}
