/* bench_codec_cpu.c — isolate the float-column compression CPU cost.
 *
 * Builds a representative mixed float workload (price-band, random-walk, sine,
 * full-entropy) as 8192-row blocks and times tsdb_codec_encode_adaptive over
 * many passes.  Reports ns/block, blocks/s, MB/s, and total encoded bytes (so a
 * before/after run also proves the on-disk size is unchanged).
 *
 * Compile against the compress sources directly so it can be built with either
 * the old or new codec.c for an A/B comparison:
 *   cc -O2 -std=c11 -Iinclude -Isrc -o bench_codec_cpu bench/bench_codec_cpu.c \
 *      src/compress/{codec,gorilla,chimp,chimp128,dod,pfor,dict,lzlite,bp128}.c
 */
#define _POSIX_C_SOURCE 200809L  /* clock_gettime/CLOCK_MONOTONIC under -std=c11 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "../src/compress/codec.h"
#include "../include/tsdb.h"

static double now_s(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

#define N      8192          /* rows per block          */
#define BLOCKS 64            /* distinct blocks in mix  */
#define PASSES 400           /* encode passes over mix  */

int main(void) {
    /* Build BLOCKS blocks cycling through 4 realistic float shapes. */
    double (*blk)[N] = malloc(sizeof(double) * N * BLOCKS);
    if (!blk) { perror("malloc"); return 1; }

    uint64_t st = 0x9e3779b97f4a7c15ULL;
    #define NEXT (st = st * 6364136223846793005ULL + 1442695040888963407ULL)
    for (int b = 0; b < BLOCKS; b++) {
        int shape = b & 3;
        if (shape == 0) {                       /* price band [100,150] */
            for (int i = 0; i < N; i++) { NEXT; blk[b][i] = 100.0 + (double)(st >> 11) / (double)(1ULL << 53) * 50.0; }
        } else if (shape == 1) {                /* random walk metric */
            double a = 50.0;
            for (int i = 0; i < N; i++) { NEXT; a += ((double)(st >> 40) / (double)(1ULL << 24) - 0.5) * 2.0; if (a < 0) a = 0; if (a > 100) a = 100; blk[b][i] = a; }
        } else if (shape == 2) {                /* sine (smooth) */
            for (int i = 0; i < N; i++) { double x = 6.28318530717958647692 * (double)i / (double)N, s = 0, term = x, xx = x * x; for (int k = 0; k < 9; k++) { s += term; term *= -xx / (double)((2*k+2)*(2*k+3)); } blk[b][i] = s; }
        } else {                                /* full-entropy random doubles */
            for (int i = 0; i < N; i++) { NEXT; uint64_t bv = st; bv &= ~(0x7FFULL << 52); bv |= ((uint64_t)(512 + (st % 512)) << 52); memcpy(&blk[b][i], &bv, 8); }
        }
    }

    size_t cap = N * 11 + 64;
    uint8_t *out = malloc(cap);
    if (!out) { perror("malloc"); return 1; }

    /* Warm up + capture total bytes (size is workload-deterministic). */
    uint64_t total_bytes = 0;
    for (int b = 0; b < BLOCKS; b++) {
        tsdb_codec_t c; uint16_t f;
        int nb = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, blk[b], N, out, cap, &c, &f);
        if (nb < 0) { fprintf(stderr, "encode failed b=%d rc=%d\n", b, nb); return 1; }
        total_bytes += (uint64_t)nb;
    }

    double t0 = now_s();
    volatile uint64_t sink = 0;
    for (int p = 0; p < PASSES; p++) {
        for (int b = 0; b < BLOCKS; b++) {
            tsdb_codec_t c; uint16_t f;
            int nb = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, blk[b], N, out, cap, &c, &f);
            sink += (uint64_t)nb;
        }
    }
    double dt = now_s() - t0;
    (void)sink;

    long long nblocks = (long long)PASSES * BLOCKS;
    double rows = (double)nblocks * N;
    double raw_mb = rows * 8.0 / 1e6;
    printf("blocks=%lld  rows=%.0f  time=%.3fs\n", nblocks, rows, dt);
    printf("  %.0f ns/block   %.0f blocks/s   %.1f Mrows/s   %.1f MB/s(raw-in)\n",
           dt / nblocks * 1e9, nblocks / dt, rows / dt / 1e6, raw_mb / dt);
    printf("  encoded bytes/mix-pass = %llu  (%.3f B/value over %d-block mix)\n",
           (unsigned long long)total_bytes, (double)total_bytes / (BLOCKS * N), BLOCKS);
    free(blk); free(out);
    return 0;
}
