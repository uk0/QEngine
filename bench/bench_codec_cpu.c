/* bench_codec_cpu.c — isolate the float-column compression CPU cost.
 *
 * Builds a representative mixed float workload (price-band, random-walk, sine,
 * full-entropy) as 8192-row blocks and times tsdb_codec_encode_adaptive over
 * many passes.  Reports ns/block, blocks/s, MB/s, and total encoded bytes (so a
 * before/after run also proves the on-disk size is unchanged).
 *
 * Then times the DECODE side, which is what every scan query pays:
 *   FLOAT64 domain-only (CHIMP/GORILLA — the bitstream reader is the hot loop)
 *   FLOAT64 adaptive     (same, plus the outer-lzlite unwrap a real block has)
 *   TIMESTAMP domain-only (DOD — 1/7/9/12/32-bit reads, a contrast case)
 * Every decode is memcmp'd against the source, so a decode regression fails the
 * bench instead of producing a fast wrong number.
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

    /* ── decode ───────────────────────────────────────────────────────────
     * Pre-encode every block once, then time only the decode loop.  DPASSES is
     * lower than PASSES because decode is the cheaper half and this bench
     * already runs for seconds. */
    #define DPASSES 200
    {
        int64_t (*tsb)[N] = malloc(sizeof(int64_t) * N * BLOCKS);
        uint8_t *encf = malloc(cap * BLOCKS), *enca = malloc(cap * BLOCKS);
        uint8_t *enct = malloc(cap * BLOCKS);
        double  *dblk = malloc(sizeof(double) * N);
        int64_t *dts  = malloc(sizeof(int64_t) * N);
        int nbf[BLOCKS], nba[BLOCKS], nbt[BLOCKS];
        tsdb_codec_t cf[BLOCKS], ca[BLOCKS], ct[BLOCKS];
        uint16_t fla[BLOCKS];
        if (!tsb || !encf || !enca || !enct || !dblk || !dts) { perror("malloc"); return 1; }

        /* Timestamps: nanosecond ticks with jitter — the DOD shape. */
        for (int b = 0; b < BLOCKS; b++) {
            int64_t t = 1700000000000000000LL + (int64_t)b * 1000000000LL;
            for (int i = 0; i < N; i++) {
                NEXT;
                t += 1000000LL + (int64_t)(st % ((b & 1) ? 5 : 250000));
                tsb[b][i] = t;
            }
        }

        for (int b = 0; b < BLOCKS; b++) {
            uint16_t f;
            nbf[b] = tsdb_codec_encode(TSDB_TYPE_FLOAT64, blk[b], N, encf + (size_t)b * cap, cap, &cf[b]);
            nba[b] = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, blk[b], N, enca + (size_t)b * cap, cap, &ca[b], &f);
            fla[b] = f;
            nbt[b] = tsdb_codec_encode(TSDB_TYPE_TIMESTAMP, tsb[b], N, enct + (size_t)b * cap, cap, &ct[b]);
            if (nbf[b] < 0 || nba[b] < 0 || nbt[b] < 0) { fprintf(stderr, "encode failed b=%d\n", b); return 1; }
        }

        /* Correctness gate: every decode must reproduce the input exactly. */
        for (int b = 0; b < BLOCKS; b++) {
            if (tsdb_codec_decode(cf[b], TSDB_TYPE_FLOAT64, encf + (size_t)b * cap, (size_t)nbf[b], dblk, N) < 0
                || memcmp(dblk, blk[b], sizeof(double) * N) != 0) {
                fprintf(stderr, "FLOAT64 domain decode mismatch b=%d\n", b); return 1; }
            if (tsdb_codec_decode_adaptive(ca[b], TSDB_TYPE_FLOAT64, fla[b], enca + (size_t)b * cap, (size_t)nba[b], dblk, N) < 0
                || memcmp(dblk, blk[b], sizeof(double) * N) != 0) {
                fprintf(stderr, "FLOAT64 adaptive decode mismatch b=%d\n", b); return 1; }
            if (tsdb_codec_decode(ct[b], TSDB_TYPE_TIMESTAMP, enct + (size_t)b * cap, (size_t)nbt[b], dts, N) < 0
                || memcmp(dts, tsb[b], sizeof(int64_t) * N) != 0) {
                fprintf(stderr, "TIMESTAMP domain decode mismatch b=%d\n", b); return 1; }
        }

        struct { const char *name; int adaptive; tsdb_type_t type; } phase[3] = {
            { "FLOAT64  domain",   0, TSDB_TYPE_FLOAT64   },
            { "FLOAT64  adaptive", 1, TSDB_TYPE_FLOAT64   },
            { "TIMESTAMP domain",  0, TSDB_TYPE_TIMESTAMP },
        };
        printf("decode (%d passes x %d blocks x %d rows):\n", DPASSES, BLOCKS, N);
        for (int p = 0; p < 3; p++) {
            /* Report which domain codecs the mix actually selected — the bulk
             * refill only helps codecs whose reads are wide. */
            static const char *cname[12] = { "none","dod","gorilla","for","dict",
                                             "raw","chimp","lzlite","chimp128",
                                             "bp128","pfor","f32" };
            unsigned seen = 0;
            char used[64] = "";
            for (int b = 0; b < BLOCKS; b++)
                seen |= 1u << (unsigned)((p == 0) ? cf[b] : (p == 1) ? ca[b] : ct[b]);
            for (unsigned c2 = 0; c2 < 12; c2++) {
                if (!(seen & (1u << c2))) continue;
                if (used[0]) strncat(used, "+", sizeof used - strlen(used) - 1);
                strncat(used, cname[c2], sizeof used - strlen(used) - 1);
            }
            const uint8_t *src = (p == 0) ? encf : (p == 1) ? enca : enct;
            const int     *nbv = (p == 0) ? nbf  : (p == 1) ? nba  : nbt;
            const tsdb_codec_t *cv = (p == 0) ? cf : (p == 1) ? ca : ct;
            void *dst = (p == 2) ? (void *)dts : (void *)dblk;
            double td0 = now_s();
            for (int it = 0; it < DPASSES; it++) {
                for (int b = 0; b < BLOCKS; b++) {
                    if (phase[p].adaptive)
                        tsdb_codec_decode_adaptive(cv[b], phase[p].type, fla[b],
                                                   src + (size_t)b * cap, (size_t)nbv[b], dst, N);
                    else
                        tsdb_codec_decode(cv[b], phase[p].type,
                                          src + (size_t)b * cap, (size_t)nbv[b], dst, N);
                }
            }
            double td = now_s() - td0;
            double drows = (double)DPASSES * BLOCKS * N;
            printf("  %-30s %8.2f ms  %6.2f ns/row  %7.1f Mrows/s  [%s]\n",
                   phase[p].name, td * 1e3, td / drows * 1e9, drows / td / 1e6, used);
        }
        free(tsb); free(encf); free(enca); free(enct); free(dblk); free(dts);
    }

    free(blk); free(out);
    return 0;
}
