/* test_lzlite.c -- round-trip and stress tests for the lzlite compressor.
 *
 * Build standalone (expand globs in your shell):
 *   clang -std=c11 -O2 -Wall -Wextra -Iinclude
 *         src/core/SRCS src/compress/SRCS tests/test_lzlite.c
 *         -o /tmp/tsdb_test_lzlite -lpthread
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#include "../src/compress/lzlite.h"
#include "../src/compress/dod.h"
#include "../include/tsdb.h"

/* ------------------------------------------------------------------ helpers */

#define FAIL(msg) \
    do { fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, (msg)); \
         abort(); } while (0)
#define CHECK(cond, msg) do { if (!(cond)) FAIL(msg); } while (0)

/* Simple LCG for reproducible pseudo-random data. */
static uint32_t g_rng = 0xDEADBEEFu;
static uint8_t rng_byte(void)
{
    g_rng = g_rng * 1664525u + 1013904223u;
    return (uint8_t)(g_rng >> 16);
}

/* Encode + decode wrapper; checks round-trip byte-for-byte. */
static void roundtrip(const uint8_t *src, size_t src_n,
                      const char *label)
{
    size_t max_enc = tsdb_lzlite_max_output(src_n);
    uint8_t *enc = (uint8_t *)malloc(max_enc + 1);
    CHECK(enc != NULL, "alloc enc");

    size_t enc_bytes = 0;
    int rc = tsdb_lzlite_encode(src, src_n, enc, max_enc, &enc_bytes);
    CHECK(rc >= 0, "encode failed");
    CHECK((size_t)rc == enc_bytes, "encode return == out_bytes");
    CHECK(enc_bytes <= max_enc, "enc_bytes <= max_enc");

    uint8_t *dec = (uint8_t *)malloc(src_n + 1);
    CHECK(dec != NULL, "alloc dec");

    size_t dec_bytes = 0;
    rc = tsdb_lzlite_decode(enc, enc_bytes, dec, src_n, &dec_bytes);
    CHECK(rc == TSDB_OK, "decode failed");
    CHECK(dec_bytes == src_n, "decoded size mismatch");
    CHECK(memcmp(src, dec, src_n) == 0, "round-trip content mismatch");

    double ratio = src_n > 0 ? (double)src_n / (double)enc_bytes : 1.0;
    printf("[lzlite] %-36s  in=%7zu  out=%7zu  ratio=%.2fx\n",
           label, src_n, enc_bytes, ratio);

    free(enc);
    free(dec);
}

/* ---------------------------------------------------------------- test_random */

static void test_random(void)
{
    const size_t N = 65536;
    uint8_t *buf = (uint8_t *)malloc(N);
    CHECK(buf != NULL, "alloc");
    for (size_t i = 0; i < N; i++)
        buf[i] = rng_byte();

    roundtrip(buf, N, "random 64KB");
    free(buf);
}

/* ---------------------------------------------------------------- test_repeat */

/* 1000 copies of "ABCDEFG" = 7000 bytes. Expect ≤ 50 bytes compressed. */
static void test_repeat(void)
{
    const char *pat = "ABCDEFG";
    const size_t pat_len = 7;
    const size_t reps    = 1000;
    const size_t N       = pat_len * reps; /* 7000 bytes */

    uint8_t *buf = (uint8_t *)malloc(N);
    CHECK(buf != NULL, "alloc");
    for (size_t i = 0; i < reps; i++)
        memcpy(buf + i * pat_len, pat, pat_len);

    size_t max_enc = tsdb_lzlite_max_output(N);
    uint8_t *enc   = (uint8_t *)malloc(max_enc);
    CHECK(enc != NULL, "alloc enc");

    size_t enc_bytes = 0;
    int rc = tsdb_lzlite_encode(buf, N, enc, max_enc, &enc_bytes);
    CHECK(rc >= 0, "encode repeat");

    printf("[lzlite] %-36s  in=%7zu  out=%7zu  ratio=%.2fx\n",
           "repeat 1000x'ABCDEFG'",
           N, enc_bytes, (double)N / (double)enc_bytes);

    /* Verify compression achieves < 50 bytes as required. */
    CHECK(enc_bytes <= 50, "repeat: expected <= 50 bytes");

    /* Decode and verify. */
    uint8_t *dec = (uint8_t *)malloc(N);
    CHECK(dec != NULL, "alloc dec");
    size_t dec_bytes = 0;
    rc = tsdb_lzlite_decode(enc, enc_bytes, dec, N, &dec_bytes);
    CHECK(rc == TSDB_OK, "decode repeat");
    CHECK(dec_bytes == N, "repeat decoded size");
    CHECK(memcmp(buf, dec, N) == 0, "repeat content");

    free(buf); free(enc); free(dec);
}

/* ---------------------------------------------------------------- test_dod_residual */

/* Encode some timestamps with DoD, then compress the DoD output with lzlite. */
static void test_dod_residual(void)
{
    const size_t N_ts = 8192; /* one block worth */
    int64_t *ts_in  = (int64_t *)malloc(N_ts * sizeof(int64_t));
    CHECK(ts_in != NULL, "alloc ts_in");

    /* Uniform 1-ms nanosecond timestamps. */
    ts_in[0] = 1700000000000000000LL;
    for (size_t i = 1; i < N_ts; i++)
        ts_in[i] = ts_in[i - 1] + 1000000LL;

    /* DoD encode. */
    size_t dod_cap  = N_ts * 10; /* generous */
    uint8_t *dod_out = (uint8_t *)malloc(dod_cap);
    CHECK(dod_out != NULL, "alloc dod_out");
    size_t dod_bytes = 0;
    int rc = tsdb_dod_encode(ts_in, N_ts, dod_out, dod_cap, &dod_bytes);
    CHECK(rc == TSDB_OK, "dod_encode");

    /* Now compress the DoD output with lzlite. */
    size_t lz_cap   = tsdb_lzlite_max_output(dod_bytes);
    uint8_t *lz_out  = (uint8_t *)malloc(lz_cap);
    CHECK(lz_out != NULL, "alloc lz_out");
    size_t lz_bytes = 0;
    rc = tsdb_lzlite_encode(dod_out, dod_bytes, lz_out, lz_cap, &lz_bytes);
    CHECK(rc >= 0, "lzlite encode over dod");

    printf("[lzlite] %-36s  dod=%5zu  lz=%5zu  extra=%.2fx\n",
           "DoD residual (8192 uniform ts)",
           dod_bytes, lz_bytes, (double)dod_bytes / (double)lz_bytes);

    /* Decompress and re-verify DoD data is intact. */
    uint8_t *lz_dec = (uint8_t *)malloc(dod_bytes + 1);
    CHECK(lz_dec != NULL, "alloc lz_dec");
    size_t lz_dec_bytes = 0;
    rc = tsdb_lzlite_decode(lz_out, lz_bytes, lz_dec, dod_bytes, &lz_dec_bytes);
    CHECK(rc == TSDB_OK, "lzlite decode over dod");
    CHECK(lz_dec_bytes == dod_bytes, "dod decoded size");
    CHECK(memcmp(dod_out, lz_dec, dod_bytes) == 0, "dod residual content");

    free(ts_in); free(dod_out); free(lz_out); free(lz_dec);
}

/* ---------------------------------------------------------------- test_edge_empty */

static void test_edge_empty(void)
{
    uint8_t out[16];
    size_t out_bytes = 99;

    /* Empty input. */
    int rc = tsdb_lzlite_encode(NULL, 0, out, sizeof(out), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "empty encode");

    size_t dec_bytes = 99;
    rc = tsdb_lzlite_decode(NULL, 0, out, sizeof(out), &dec_bytes);
    CHECK(rc == TSDB_OK && dec_bytes == 0, "empty decode");

    printf("[lzlite] %-36s  OK\n", "edge: empty input");
}

/* ---------------------------------------------------------------- test_edge_n1 */

static void test_edge_n1(void)
{
    uint8_t src[1] = { 0xAB };
    uint8_t enc[64];
    size_t enc_bytes = 0;

    int rc = tsdb_lzlite_encode(src, 1, enc, sizeof(enc), &enc_bytes);
    CHECK(rc >= 0, "n=1 encode");
    CHECK(enc_bytes > 0, "n=1 enc_bytes > 0");

    uint8_t dec[1];
    size_t dec_bytes = 0;
    rc = tsdb_lzlite_decode(enc, enc_bytes, dec, sizeof(dec), &dec_bytes);
    CHECK(rc == TSDB_OK, "n=1 decode");
    CHECK(dec_bytes == 1, "n=1 dec_bytes");
    CHECK(dec[0] == src[0], "n=1 content");

    printf("[lzlite] %-36s  OK\n", "edge: n=1");
}

/* ---------------------------------------------------------------- test_large */

static void test_large(void)
{
    const size_t N = 1 << 20; /* 1 MB */
    uint8_t *src = (uint8_t *)malloc(N);
    CHECK(src != NULL, "alloc large src");

    /* Semi-compressible: alternating structured and random runs. */
    for (size_t i = 0; i < N; i++) {
        if ((i / 256) % 2 == 0)
            src[i] = (uint8_t)(i & 0xFF); /* sequential */
        else
            src[i] = rng_byte();           /* random */
    }

    roundtrip(src, N, "large 1MB (mixed)");
    free(src);
}

/* ---------------------------------------------------------------- test_overflow_buf */

/* Ensure encode returns TSDB_ERR_OVERFLOW when buffer is too small. */
static void test_overflow_buf(void)
{
    /* Random incompressible data, tiny output buffer. */
    const size_t N = 1024;
    uint8_t src[1024];
    for (size_t i = 0; i < N; i++) src[i] = rng_byte();

    uint8_t tiny[8]; /* nowhere near enough */
    size_t out_bytes = 0;
    int rc = tsdb_lzlite_encode(src, N, tiny, sizeof(tiny), &out_bytes);
    CHECK(rc == TSDB_ERR_OVERFLOW, "overflow encode returns OVERFLOW");

    printf("[lzlite] %-36s  OK\n", "overflow: tiny output buffer");
}

/* ---------------------------------------------------------------- test_corrupt */

/* Ensure decoder rejects various forms of corrupt input. */
static void test_corrupt(void)
{
    uint8_t dec[64];
    size_t dec_bytes = 0;

    /* 1. Truncated mid-literal. */
    {
        /* Token says 5 literal bytes, but only 2 follow. */
        uint8_t bad[] = { (uint8_t)(5 << 4), 0xAA, 0xBB };
        int rc = tsdb_lzlite_decode(bad, sizeof(bad), dec, sizeof(dec), &dec_bytes);
        CHECK(rc == TSDB_ERR_CORRUPT, "corrupt: truncated literals");
    }

    /* 2. Back-reference offset == 0 (invalid). */
    {
        /* First emit 4 literal bytes, then a match token with offset=0. */
        uint8_t bad2[] = {
            (uint8_t)(4 << 4),    /* 4 literals, final (no match) */
            0x01, 0x02, 0x03, 0x04,
            /* match sequence: lit=0, match_nib=1 (match_len=5), offset=0 */
            (uint8_t)((0 << 4) | 1),
            0x00, 0x00  /* offset = 0 -- CORRUPT */
        };
        int rc = tsdb_lzlite_decode(bad2, sizeof(bad2), dec, sizeof(dec), &dec_bytes);
        CHECK(rc == TSDB_ERR_CORRUPT, "corrupt: offset == 0");
    }

    /* 3. Back-reference offset beyond current output position. */
    {
        /* Emit 2 literals, then match with offset=100 (past start). */
        uint8_t bad3[] = {
            /* sequence 1: lit=2, match_nib=0 (final seq trick: no offset follows) */
            /* actually we need match_nib != 0 to force a match; use match_nib=1 */
            (uint8_t)((2 << 4) | 1), /* lit=2, match=1 => match_len=5 */
            /* match overflow: none (match_nib < 15) */
            0x64, 0x00,               /* offset = 100 > op (which is 2) */
            0xAA, 0xBB               /* 2 literals */
        };
        int rc = tsdb_lzlite_decode(bad3, sizeof(bad3), dec, sizeof(dec), &dec_bytes);
        CHECK(rc == TSDB_ERR_CORRUPT, "corrupt: offset > op");
    }

    /* 4. Overflow varint runs off end of input. */
    {
        /* lit_nib=15: means we must read overflow bytes ending with <255.
         * But the stream ends right after the 0xFF. */
        uint8_t bad4[] = { (uint8_t)(15 << 4), 0xFF /* no terminator */ };
        int rc = tsdb_lzlite_decode(bad4, sizeof(bad4), dec, sizeof(dec), &dec_bytes);
        CHECK(rc == TSDB_ERR_CORRUPT, "corrupt: truncated overflow varint");
    }

    printf("[lzlite] %-36s  OK\n", "corrupt input detection");
}

/* ---------------------------------------------------------------- test_constant */

/* Single-byte repeated → should be highly compressible. */
static void test_constant(void)
{
    const size_t N = 4096;
    uint8_t *buf = (uint8_t *)malloc(N);
    CHECK(buf != NULL, "alloc");
    memset(buf, 0x5A, N);
    roundtrip(buf, N, "constant 0x5A x4096");
    free(buf);
}

/* ---------------------------------------------------------------- test_binary_zeros */

static void test_binary_zeros(void)
{
    const size_t N = 10000;
    uint8_t *buf = (uint8_t *)calloc(N, 1);
    CHECK(buf != NULL, "alloc");
    roundtrip(buf, N, "zeros x10000");
    free(buf);
}

/* ---------------------------------------------------------------- main */

int main(void)
{
    printf("=== tsdb lzlite compressor tests ===\n\n");

    printf("--- Edge cases ---\n");
    test_edge_empty();
    test_edge_n1();

    printf("\n--- Compression tests ---\n");
    test_constant();
    test_binary_zeros();
    test_repeat();
    test_random();
    test_large();

    printf("\n--- DoD residual ---\n");
    test_dod_residual();

    printf("\n--- Error handling ---\n");
    test_overflow_buf();
    test_corrupt();

    printf("\nAll lzlite tests passed.\n");
    return 0;
}
