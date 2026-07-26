/* test_adaptive.c — adaptive codec selection tests + compression ratio table.
 *
 * Verifies:
 *  1. tsdb_codec_encode_adaptive round-trips correctly for every data
 *     distribution and column type.
 *  2. Result is at most as large as tsdb_codec_encode (no regression).
 *  3. Decode handles legacy blocks (flags=0) identically to old path.
 *  4. TSDB_BF_OUTER_LZ path round-trips correctly.
 *  5. Prints a comparison table: legacy bytes/pt vs adaptive bytes/pt.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <assert.h>

#include "../src/compress/codec.h"
#include "../src/core/types.h"
#include "../include/tsdb.h"

/* ---- helpers ---------------------------------------------------------------- */

#define FAIL(msg) \
    do { fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, (msg)); abort(); } while(0)
#define CHECK(cond, msg) \
    do { if (!(cond)) FAIL(msg); } while(0)

static uint8_t g_enc_buf[1 << 22]; /* 4 MiB — generous for 8192 doubles */
static uint8_t g_enc_legacy[1 << 22];

/* Simple LCG RNG for reproducibility. */
static uint32_t g_rng = 0xdeadbeef;
static inline uint32_t rng_next(void) {
    g_rng = g_rng * 1664525u + 1013904223u;
    return g_rng;
}
static inline double rng_f64(void) {
    /* uniform [0, 1) */
    uint32_t x = rng_next();
    return (double)(x >> 8) / (double)(1u << 24);
}

/* Codec name helper. */
static const char *codec_name(tsdb_codec_t c) {
    switch (c) {
    case TSDB_CODEC_NONE:    return "NONE";
    case TSDB_CODEC_DOD:     return "DOD";
    case TSDB_CODEC_GORILLA: return "GORILLA";
    case TSDB_CODEC_FOR:     return "FOR";
    case TSDB_CODEC_DICT:    return "DICT";
    case TSDB_CODEC_RAW:     return "RAW";
    case TSDB_CODEC_CHIMP:   return "CHIMP";
    case TSDB_CODEC_LZLITE:  return "LZLITE";
    case TSDB_CODEC_CHIMP128:return "CHIMP128";
    case TSDB_CODEC_BP128:   return "BP128";
    case TSDB_CODEC_PFOR:    return "PFOR";
    default:                 return "?";
    }
}

/* ---- per-distribution test routine ----------------------------------------- */

typedef struct {
    const char *label;
    tsdb_type_t type;
    size_t      n;
    void       *values;
    /* Expected outer-LZ decision: 1 = must wrap, 0 = must not, -1 = unpinned.
     * The float gate skips the LZ stage only when the domain output is dense
     * (>= 50% of raw) AND a probe over the raw input finds no byte-level
     * redundancy, so quantized and heterogeneous float shapes must still take
     * the wrapper even though their domain output is dense. */
    int         expect_lz;
} test_case_t;

static void run_case(const test_case_t *tc, int print_header) {
    if (print_header) {
        printf("\n%-30s %-9s %6s  %-8s %8s  %-8s %8s  %7s  %s\n",
               "Distribution", "Type", "N",
               "Lg.Codec", "Lg.B/pt",
               "Ad.Codec", "Ad.B/pt",
               "LZ?", "Status");
        printf("%-30s %-9s %6s  %-8s %8s  %-8s %8s  %7s  %s\n",
               "------------------------------", "---------", "------",
               "--------", "-------",
               "--------", "-------",
               "-------", "------");
    }

    size_t n          = tc->n;
    tsdb_type_t type  = tc->type;
    const void *vals  = tc->values;
    size_t width      = tsdb_type_width(type);
    size_t raw_bytes  = n * width;

    /* --- Legacy encode --- */
    tsdb_codec_t lg_codec = TSDB_CODEC_NONE;
    int lg_bytes = tsdb_codec_encode(type, vals, n,
                                     g_enc_legacy, sizeof(g_enc_legacy),
                                     &lg_codec);
    CHECK(lg_bytes >= 0, "legacy encode failed");

    /* --- Adaptive encode --- */
    tsdb_codec_t ad_codec = TSDB_CODEC_NONE;
    uint16_t     ad_flags = 0;
    int ad_bytes = tsdb_codec_encode_adaptive(type, vals, n,
                                              g_enc_buf, sizeof(g_enc_buf),
                                              &ad_codec, &ad_flags);
    CHECK(ad_bytes >= 0, "adaptive encode failed");

    /* Adaptive must be <= legacy (never worse). */
    CHECK((size_t)ad_bytes <= (size_t)lg_bytes,
          "adaptive produced more bytes than legacy");

    /* No format change: only the pre-existing OUTER_LZ flag bit is ever set. */
    CHECK((ad_flags & ~(uint16_t)TSDB_BF_OUTER_LZ) == 0,
          "adaptive set an unknown block flag");

    if (tc->expect_lz >= 0) {
        int has_lz = (ad_flags & TSDB_BF_OUTER_LZ) ? 1 : 0;
        if (has_lz != tc->expect_lz) {
            fprintf(stderr, "FAIL: %s expected lz=%d got %d (adaptive=%d legacy=%d)\n",
                    tc->label, tc->expect_lz, has_lz, ad_bytes, lg_bytes);
            abort();
        }
        /* When the stage runs, the win must be real, not a 16-byte rounding. */
        if (has_lz)
            CHECK((size_t)ad_bytes * 100 <= (size_t)lg_bytes * 95,
                  "outer LZ accepted for a < 5% gain");
    }

    /* --- Adaptive decode round-trip --- */
    void *decoded = malloc(raw_bytes);
    CHECK(decoded, "alloc decoded");

    int rc = tsdb_codec_decode_adaptive(ad_codec, type, ad_flags,
                                        g_enc_buf, (size_t)ad_bytes,
                                        decoded, n);
    CHECK(rc == TSDB_OK, "adaptive decode failed");

    /* Verify values. */
    if (type == TSDB_TYPE_FLOAT64) {
        const double *orig = (const double *)vals;
        const double *got  = (const double *)decoded;
        for (size_t i = 0; i < n; i++) {
            /* Exact bit-for-bit round-trip required. */
            uint64_t a, b;
            memcpy(&a, &orig[i], 8);
            memcpy(&b, &got[i],  8);
            if (a != b) {
                fprintf(stderr, "FAIL: float mismatch at i=%zu orig=%g got=%g\n",
                        i, orig[i], got[i]);
                abort();
            }
        }
    } else if (type == TSDB_TYPE_TIMESTAMP || type == TSDB_TYPE_INT64) {
        const int64_t *orig = (const int64_t *)vals;
        const int64_t *got  = (const int64_t *)decoded;
        for (size_t i = 0; i < n; i++) {
            if (orig[i] != got[i]) {
                fprintf(stderr, "FAIL: i64 mismatch at i=%zu orig=%lld got=%lld\n",
                        i, (long long)orig[i], (long long)got[i]);
                abort();
            }
        }
    } else { /* SYMBOL */
        const uint32_t *orig = (const uint32_t *)vals;
        const uint32_t *got  = (const uint32_t *)decoded;
        for (size_t i = 0; i < n; i++) {
            if (orig[i] != got[i]) {
                fprintf(stderr, "FAIL: sym mismatch at i=%zu orig=%u got=%u\n",
                        i, orig[i], got[i]);
                abort();
            }
        }
    }
    free(decoded);

    /* --- Legacy decode round-trip (sanity) --- */
    void *decoded_lg = malloc(raw_bytes);
    CHECK(decoded_lg, "alloc decoded_lg");
    rc = tsdb_codec_decode_adaptive(lg_codec, type, /*flags=*/0,
                                    g_enc_legacy, (size_t)lg_bytes,
                                    decoded_lg, n);
    CHECK(rc == TSDB_OK, "legacy decode via adaptive path failed");
    CHECK(memcmp(vals, decoded_lg, raw_bytes) == 0, "legacy decode mismatch");
    free(decoded_lg);

    const char *type_str =
        (type == TSDB_TYPE_TIMESTAMP) ? "TIMESTAMP" :
        (type == TSDB_TYPE_INT64)     ? "INT64"     :
        (type == TSDB_TYPE_FLOAT64)   ? "FLOAT64"   : "SYMBOL";

    printf("%-30s %-9s %6zu  %-8s %7.3f   %-8s %7.3f  %7s  OK\n",
           tc->label, type_str, n,
           codec_name(lg_codec),
           (double)lg_bytes / (double)n,
           codec_name(ad_codec),
           (double)ad_bytes / (double)n,
           (ad_flags & TSDB_BF_OUTER_LZ) ? "YES" : "no");
}

/* ---- distribution builders ------------------------------------------------- */

static void fill_ts_uniform(int64_t *buf, size_t n) {
    /* 1ms tick, starting at a nominal timestamp. */
    buf[0] = 1700000000000000000LL;
    for (size_t i = 1; i < n; i++)
        buf[i] = buf[i-1] + 1000000LL; /* 1 ms in ns */
}

static void fill_ts_jitter(int64_t *buf, size_t n) {
    g_rng = 0xabcdef01;
    buf[0] = 1700000000000000000LL;
    for (size_t i = 1; i < n; i++) {
        int64_t jitter = (int64_t)(rng_next() % 100001) - 50000; /* ±50 µs */
        buf[i] = buf[i-1] + 1000000LL + jitter;
    }
}

static void fill_i64_sequential(int64_t *buf, size_t n) {
    for (size_t i = 0; i < n; i++)
        buf[i] = (int64_t)i * 100;
}

static void fill_i64_random(int64_t *buf, size_t n) {
    g_rng = 0x11223344;
    for (size_t i = 0; i < n; i++) {
        uint64_t hi = rng_next();
        uint64_t lo = rng_next();
        buf[i] = (int64_t)((hi << 32) | lo);
    }
}

static void fill_float_constant(double *buf, size_t n) {
    for (size_t i = 0; i < n; i++)
        buf[i] = 3.14159265358979323846;
}

static void fill_float_sine(double *buf, size_t n) {
    for (size_t i = 0; i < n; i++)
        buf[i] = sin(2.0 * 3.14159265358979323846 * (double)i / (double)n);
}

static void fill_float_random_walk(double *buf, size_t n) {
    /* TSBS-like: CPU metric, starts at ~50, small increments. */
    g_rng = 0xfeedcafe;
    buf[0] = 50.0;
    for (size_t i = 1; i < n; i++) {
        double step = (rng_f64() - 0.5) * 2.0; /* ±1.0 */
        buf[i] = buf[i-1] + step;
        if (buf[i] < 0.0) buf[i] = 0.0;
        if (buf[i] > 100.0) buf[i] = 100.0;
    }
}

static void fill_float_large_range(double *buf, size_t n) {
    /* Wide-range metrics: e.g., memory bytes, spiky values. */
    g_rng = 0xc0ffee00;
    for (size_t i = 0; i < n; i++) {
        buf[i] = rng_f64() * 1e12;
    }
}

/* Quantized telemetry — the shape real TSBS-style agents actually emit.  Its
 * domain output is dense (~80% of raw) so the density half of the float LZ gate
 * arms, but the quantisation leaves repeated byte patterns that lzlite crushes.
 * Without a shape like this in the suite there is no CI signal on that gate. */
static void fill_float_sensor_2dp(double *buf, size_t n) {
    g_rng = 0x5e5c0f2d;
    double a = 50.0;
    for (size_t i = 0; i < n; i++) {
        a += (rng_f64() - 0.5) * 2.0;
        if (a < 0.0) a = 0.0;
        if (a > 100.0) a = 100.0;
        buf[i] = (double)((long long)(a * 100.0 + 0.5)) / 100.0;
    }
}

/* Heterogeneous block: a high-entropy head followed by a quantized tail — one
 * flush spanning a regime change.  A gate that sampled only the head of the
 * block would call this incompressible and throw the tail's win away. */
static void fill_float_het_head(double *buf, size_t n) {
    g_rng = 0x71cb3a95;
    size_t head = n / 16;
    if (head > 512) head = 512;
    for (size_t i = 0; i < head; i++) {
        uint64_t b = ((uint64_t)rng_next() << 32) | rng_next();
        b &= ~(0x7FFULL << 52);
        b |= ((uint64_t)(1000 + (rng_next() % 24)) << 52);
        memcpy(&buf[i], &b, 8);
    }
    double a = 50.0;
    for (size_t i = head; i < n; i++) {
        a += (rng_f64() - 0.5) * 2.0;
        if (a < 0.0) a = 0.0;
        if (a > 100.0) a = 100.0;
        buf[i] = (double)((long long)(a * 100.0 + 0.5)) / 100.0;
    }
}

static void fill_symbol_low_card(uint32_t *buf, size_t n) {
    /* 4 distinct symbols — very compressible. */
    for (size_t i = 0; i < n; i++)
        buf[i] = i % 4;
}

static void fill_symbol_high_card(uint32_t *buf, size_t n) {
    /* Each row a unique symbol — worst case for dict. */
    g_rng = 0xdeadface;
    for (size_t i = 0; i < n; i++)
        buf[i] = rng_next() % 65536;
}

/* ---- LZ wrapper explicit test ---------------------------------------------- */

static void test_lz_roundtrip(void) {
    /* Create a highly repetitive float sequence that domain codec alone
     * leaves with lots of redundancy that LZ can exploit.
     * A repeating block of 64 identical values with a different value every
     * 64th produces a very regular bit-stream for LZ to compress.
     */
    const size_t N = 8192;
    double *vals = malloc(N * sizeof(double));
    CHECK(vals, "alloc");

    /* Repeating sine wave at very low frequency (period >> N): extremely
     * similar adjacent values.  Gorilla will produce many short XOR runs.
     */
    for (size_t i = 0; i < N; i++)
        vals[i] = 1000.0 + 0.001 * (double)i; /* tiny linear ramp */

    tsdb_codec_t codec  = TSDB_CODEC_NONE;
    uint16_t     flags  = 0;
    int ad_bytes = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, vals, N,
                                              g_enc_buf, sizeof(g_enc_buf),
                                              &codec, &flags);
    CHECK(ad_bytes >= 0, "lz ramp encode");

    /* Decode. */
    double *out = malloc(N * sizeof(double));
    CHECK(out, "alloc");
    int rc = tsdb_codec_decode_adaptive(codec, TSDB_TYPE_FLOAT64, flags,
                                        g_enc_buf, (size_t)ad_bytes,
                                        out, N);
    CHECK(rc == TSDB_OK, "lz ramp decode");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &vals[i], 8);
        memcpy(&b, &out[i],  8);
        if (a != b) {
            fprintf(stderr, "FAIL: lz ramp mismatch i=%zu\n", i);
            abort();
        }
    }

    printf("\n[LZ-wrapper explicit] codec=%s lz=%s  %d bytes for N=%zu (%.3f B/pt)\n",
           codec_name(codec),
           (flags & TSDB_BF_OUTER_LZ) ? "YES" : "no",
           ad_bytes, N,
           (double)ad_bytes / (double)N);

    free(vals);
    free(out);
}

/* ---- main ------------------------------------------------------------------ */

int main(void) {
    printf("=== test_adaptive: adaptive codec selection ===\n");

    const size_t N = 8192; /* one full block */

    /* Allocate all value buffers. */
    int64_t  *ts_uniform   = malloc(N * 8);
    int64_t  *ts_jitter    = malloc(N * 8);
    int64_t  *i64_seq      = malloc(N * 8);
    int64_t  *i64_rand     = malloc(N * 8);
    double   *f_const      = malloc(N * 8);
    double   *f_sine       = malloc(N * 8);
    double   *f_rw         = malloc(N * 8);
    double   *f_large      = malloc(N * 8);
    double   *f_2dp        = malloc(N * 8);
    double   *f_het        = malloc(N * 8);
    uint32_t *sym_low      = malloc(N * 4);
    uint32_t *sym_high     = malloc(N * 4);

    CHECK(ts_uniform && ts_jitter && i64_seq && i64_rand &&
          f_const && f_sine && f_rw && f_large && f_2dp && f_het &&
          sym_low && sym_high,
          "alloc test buffers");

    fill_ts_uniform  (ts_uniform,  N);
    fill_ts_jitter   (ts_jitter,   N);
    fill_i64_sequential(i64_seq,   N);
    fill_i64_random  (i64_rand,    N);
    fill_float_constant(f_const,   N);
    fill_float_sine  (f_sine,      N);
    fill_float_random_walk(f_rw,   N);
    fill_float_large_range(f_large, N);
    fill_float_sensor_2dp(f_2dp,   N);
    fill_float_het_head(f_het,     N);
    fill_symbol_low_card(sym_low,  N);
    fill_symbol_high_card(sym_high, N);

    test_case_t cases[] = {
        { "ts:uniform",        TSDB_TYPE_TIMESTAMP, N, ts_uniform, -1 },
        { "ts:jitter",         TSDB_TYPE_TIMESTAMP, N, ts_jitter,  -1 },
        { "i64:sequential",    TSDB_TYPE_INT64,     N, i64_seq,    -1 },
        { "i64:random",        TSDB_TYPE_INT64,     N, i64_rand,   -1 },
        { "f64:constant",      TSDB_TYPE_FLOAT64,   N, f_const,     1 },
        { "f64:sine",          TSDB_TYPE_FLOAT64,   N, f_sine,     -1 },
        { "f64:random-walk",   TSDB_TYPE_FLOAT64,   N, f_rw,       -1 },
        { "f64:large-range",   TSDB_TYPE_FLOAT64,   N, f_large,    -1 },
        { "f64:sensor-2dp",    TSDB_TYPE_FLOAT64,   N, f_2dp,       1 },
        { "f64:het-head+2dp",  TSDB_TYPE_FLOAT64,   N, f_het,       1 },
        { "sym:low-card(4)",   TSDB_TYPE_SYMBOL,    N, sym_low,     1 },
        { "sym:high-card(64k)",TSDB_TYPE_SYMBOL,    N, sym_high,   -1 },
    };
    int ncases = (int)(sizeof(cases) / sizeof(cases[0]));

    for (int i = 0; i < ncases; i++)
        run_case(&cases[i], i == 0);

    test_lz_roundtrip();

    /* ---- Smaller block sizes ---- */
    printf("\n--- Small block (N=128) ---\n");
    const size_t N2 = 128;
    int64_t  *ts_s  = malloc(N2 * 8);
    double   *f_s   = malloc(N2 * 8);
    uint32_t *sym_s = malloc(N2 * 4);
    CHECK(ts_s && f_s && sym_s, "alloc small");

    fill_ts_uniform(ts_s, N2);
    fill_float_random_walk(f_s, N2);
    fill_symbol_low_card(sym_s, N2);

    test_case_t small_cases[] = {
        { "ts:uniform",      TSDB_TYPE_TIMESTAMP, N2, ts_s,  -1 },
        { "f64:random-walk", TSDB_TYPE_FLOAT64,   N2, f_s,   -1 },
        { "sym:low-card",    TSDB_TYPE_SYMBOL,    N2, sym_s, -1 },
    };
    for (int i = 0; i < 3; i++)
        run_case(&small_cases[i], i == 0);

    /* Cleanup. */
    free(ts_uniform); free(ts_jitter);
    free(i64_seq);    free(i64_rand);
    free(f_const);    free(f_sine);    free(f_rw);    free(f_large);
    free(f_2dp);      free(f_het);
    free(sym_low);    free(sym_high);
    free(ts_s);       free(f_s);       free(sym_s);

    printf("\n=== ALL ADAPTIVE TESTS PASSED ===\n");
    return 0;
}
