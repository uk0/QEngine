/* test_pfor.c — comprehensive tests for bp128 and pfor compression.
 *
 * Tests:
 *   1. bp128 pack/unpack round-trip for b=1..32
 *   2. pfor_encode/decode: uniform, sparse (outliers), random uint32
 *   3. pfor_i64: monotonic counter, jittered sensor int64
 *   4. Boundary: corrupt input → TSDB_ERR_CORRUPT
 *   5. Performance: 1M int64 decode throughput (int/sec)
 *   6. Compression ratio: DoD vs PFOR on three data patterns
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <time.h>
#include <math.h>

#include "../src/compress/bp128.h"
#include "../src/compress/pfor.h"
#include "../src/compress/dod.h"
#include "../src/compress/dict.h"
#include "../include/tsdb.h"
#include "../src/exec/simd.h"

/* -------------------------------------------------------------------------
 * Helpers
 * -------------------------------------------------------------------------*/

#define FAIL(msg) do { \
    fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
    abort(); \
} while(0)

#define CHECK(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)
#define CHECK_EQ(a, b, msg) do { if ((a) != (b)) { \
    fprintf(stderr, "FAIL [%s:%d]: %s (got %lld expected %lld)\n", \
            __FILE__, __LINE__, msg, (long long)(a), (long long)(b)); \
    abort(); \
} } while(0)

static uint8_t g_buf[1 << 22]; /* 4 MiB scratch */
static uint8_t g_buf2[1 << 22];

/* Simple LCG random. */
static unsigned g_rng = 0xDEADBEEF;
static inline uint32_t rand_u32(void) {
    g_rng = g_rng * 1664525u + 1013904223u;
    return g_rng;
}

/* =========================================================================
 * Section 1: bp128 pack/unpack round-trips for b=1..32
 * =========================================================================*/

static void test_bp128_roundtrip(void)
{
    printf("--- bp128 pack/unpack round-trip (b=1..32) ---\n");

    uint32_t src[128];
    uint32_t packed[128]; /* at most 32*16/4 = 128 words */
    uint32_t dst[128];

    for (unsigned b = 1; b <= 32; b++) {
        const uint32_t mask = (b == 32) ? 0xFFFFFFFFu : ((1u << b) - 1u);

        /* Fill with values that fit in b bits (non-zero). */
        for (unsigned i = 0; i < 128; i++) {
            src[i] = (rand_u32() & mask);
        }

        memset(packed, 0, sizeof(packed));
        memset(dst, 0xCC, sizeof(dst));

        int rc = tsdb_bp128_pack(src, packed, b);
        CHECK(rc == TSDB_OK, "bp128_pack failed");

        rc = tsdb_bp128_unpack(packed, dst, b);
        CHECK(rc == TSDB_OK, "bp128_unpack failed");

        for (unsigned i = 0; i < 128; i++) {
            if ((src[i] & mask) != dst[i]) {
                fprintf(stderr, "FAIL: bp128 b=%u i=%u src=0x%08x dst=0x%08x\n",
                        b, i, src[i] & mask, dst[i]);
                abort();
            }
        }
    }
    printf("[bp128] round-trip b=1..32: OK\n");

    /* Verify b=0 and b=33 return TSDB_ERR_INVAL. */
    CHECK(tsdb_bp128_pack(src, packed, 0) == TSDB_ERR_INVAL, "bp128 b=0 should fail");
    CHECK(tsdb_bp128_pack(src, packed, 33) == TSDB_ERR_INVAL, "bp128 b=33 should fail");
    CHECK(tsdb_bp128_unpack(packed, dst, 0) == TSDB_ERR_INVAL, "bp128 unpack b=0 should fail");
    printf("[bp128] invalid b: TSDB_ERR_INVAL OK\n");

    /* Test all-zeros and all-ones. */
    for (unsigned b = 1; b <= 32; b++) {
        const uint32_t mask = (b == 32) ? 0xFFFFFFFFu : ((1u << b) - 1u);
        for (unsigned i = 0; i < 128; i++) src[i] = 0;
        tsdb_bp128_pack(src, packed, b);
        tsdb_bp128_unpack(packed, dst, b);
        for (unsigned i = 0; i < 128; i++) CHECK(dst[i] == 0, "all-zeros");

        for (unsigned i = 0; i < 128; i++) src[i] = mask;
        tsdb_bp128_pack(src, packed, b);
        tsdb_bp128_unpack(packed, dst, b);
        for (unsigned i = 0; i < 128; i++) {
            if (dst[i] != mask) {
                fprintf(stderr, "FAIL: all-ones b=%u i=%u got 0x%08x expected 0x%08x\n",
                        b, i, dst[i], mask);
                abort();
            }
        }
    }
    printf("[bp128] all-zeros, all-ones: OK\n");
    printf("\n");
}

/* =========================================================================
 * Section 2: pfor_encode / pfor_decode
 * =========================================================================*/

static void test_pfor_uniform(void)
{
    /* Uniform values — all the same. */
    const size_t N = 1024;
    uint32_t *src = malloc(N * sizeof(uint32_t));
    uint32_t *dst = malloc(N * sizeof(uint32_t));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++) src[i] = 42u;

    size_t out_bytes = 0;
    int rc = tsdb_pfor_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_encode uniform");
    CHECK(out_bytes > 0, "pfor uniform bytes > 0");

    rc = tsdb_pfor_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "pfor_decode uniform");

    for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "pfor uniform round-trip");

    printf("[PFOR] uniform (all 42)      N=%-6zu  %zu bytes  %.3f bytes/elem\n",
           N, out_bytes, (double)out_bytes / N);
    free(src); free(dst);
}

static void test_pfor_sparse_outliers(void)
{
    /* Sparse pattern: mostly small values (0..15) with occasional large outliers. */
    const size_t N = 1000;
    uint32_t *src = malloc(N * sizeof(uint32_t));
    uint32_t *dst = malloc(N * sizeof(uint32_t));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++) {
        if (i % 50 == 7) {
            src[i] = 0xFFFFFF00u + (uint32_t)(i % 256); /* large outlier */
        } else {
            src[i] = (uint32_t)(i % 16); /* small value, 4 bits */
        }
    }

    size_t out_bytes = 0;
    int rc = tsdb_pfor_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_encode sparse");

    rc = tsdb_pfor_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "pfor_decode sparse");

    for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "pfor sparse round-trip");

    printf("[PFOR] sparse outliers       N=%-6zu  %zu bytes  %.3f bytes/elem\n",
           N, out_bytes, (double)out_bytes / N);
    free(src); free(dst);
}

static void test_pfor_random(void)
{
    /* Random uint32 — worst case for PFOR (requires 32-bit width). */
    const size_t N = 1024;
    uint32_t *src = malloc(N * sizeof(uint32_t));
    uint32_t *dst = malloc(N * sizeof(uint32_t));
    CHECK(src && dst, "alloc");

    g_rng = 0x12345678;
    for (size_t i = 0; i < N; i++) src[i] = rand_u32();

    size_t out_bytes = 0;
    int rc = tsdb_pfor_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_encode random");

    rc = tsdb_pfor_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "pfor_decode random");

    for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "pfor random round-trip");

    printf("[PFOR] random uint32         N=%-6zu  %zu bytes  %.3f bytes/elem\n",
           N, out_bytes, (double)out_bytes / N);
    free(src); free(dst);
}

static void test_pfor_edge_cases(void)
{
    /* n=0 */
    size_t out_bytes = 99;
    int rc = tsdb_pfor_encode(NULL, 0, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "pfor n=0 encode");

    uint32_t dummy = 0;
    rc = tsdb_pfor_decode(g_buf, 0, &dummy, 0);
    CHECK(rc == TSDB_OK, "pfor n=0 decode");

    /* n=1 */
    uint32_t one = 0xDEADBEEFu;
    rc = tsdb_pfor_encode(&one, 1, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor n=1 encode");

    uint32_t got = 0;
    rc = tsdb_pfor_decode(g_buf, out_bytes, &got, 1);
    CHECK(rc == TSDB_OK && got == one, "pfor n=1 decode");

    /* n=127 (partial block) */
    uint32_t vals[127];
    uint32_t vals_out[127];
    for (int i = 0; i < 127; i++) vals[i] = (uint32_t)(i * 3 + 7);
    rc = tsdb_pfor_encode(vals, 127, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor n=127 encode");
    rc = tsdb_pfor_decode(g_buf, out_bytes, vals_out, 127);
    CHECK(rc == TSDB_OK, "pfor n=127 decode");
    for (int i = 0; i < 127; i++) CHECK_EQ(vals[i], vals_out[i], "pfor n=127 round-trip");

    /* n=129 (just over one block) */
    uint32_t v129[129], v129_out[129];
    for (int i = 0; i < 129; i++) v129[i] = (uint32_t)(i * 5 + 3);
    rc = tsdb_pfor_encode(v129, 129, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor n=129 encode");
    rc = tsdb_pfor_decode(g_buf, out_bytes, v129_out, 129);
    CHECK(rc == TSDB_OK, "pfor n=129 decode");
    for (int i = 0; i < 129; i++) CHECK_EQ(v129[i], v129_out[i], "pfor n=129 round-trip");

    /* Corrupt input: truncated bytes → TSDB_ERR_CORRUPT. */
    uint32_t big[256];
    for (int i = 0; i < 256; i++) big[i] = (uint32_t)i;
    rc = tsdb_pfor_encode(big, 256, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor corrupt setup encode");
    uint32_t big_out[256];
    /* Truncate input by half. */
    rc = tsdb_pfor_decode(g_buf, out_bytes / 2, big_out, 256);
    CHECK(rc == TSDB_ERR_CORRUPT, "pfor truncated → CORRUPT");

    /* Wrong count in prologue. */
    uint8_t bad_hdr[64];
    memcpy(bad_hdr, g_buf, 64);
    bad_hdr[0] = 0xFF; bad_hdr[1] = 0xFF; bad_hdr[2] = 0x00; bad_hdr[3] = 0x00; /* count=65535 */
    rc = tsdb_pfor_decode(bad_hdr, 64, big_out, 256);
    CHECK(rc == TSDB_ERR_CORRUPT, "pfor wrong count → CORRUPT");

    printf("[PFOR] edge cases: OK (n=0,1,127,129, corrupt checks)\n");
}

/* =========================================================================
 * Section 3: pfor_i64
 * =========================================================================*/

static void test_pfor_i64_monotonic(void)
{
    /* Monotonic counter (like nanosecond timestamps). */
    const size_t N = 10000;
    int64_t *src = malloc(N * sizeof(int64_t));
    int64_t *dst = malloc(N * sizeof(int64_t));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++) {
        src[i] = (int64_t)1700000000000000000LL + (int64_t)i * 1000000LL;
    }

    size_t out_bytes = 0;
    int rc = tsdb_pfor_encode_i64(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_encode_i64 monotonic");

    rc = tsdb_pfor_decode_i64(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "pfor_decode_i64 monotonic");

    for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "pfor i64 monotonic round-trip");

    /* Compare with DoD. */
    size_t dod_bytes = 0;
    int drc = tsdb_dod_encode(src, N, g_buf2, sizeof(g_buf2), &dod_bytes);
    CHECK(drc == TSDB_OK, "dod_encode for comparison");

    printf("[PFOR-i64] monotonic counter N=%-6zu  pfor=%zu bytes (%.3f B/pt)  "
           "dod=%zu bytes (%.3f B/pt)\n",
           N, out_bytes, (double)out_bytes / N,
           dod_bytes, (double)dod_bytes / N);

    free(src); free(dst);
}

static void test_pfor_i64_jittered(void)
{
    /* Jittered sensor: base delta=1000, ±500 jitter. */
    const size_t N = 10000;
    int64_t *src = malloc(N * sizeof(int64_t));
    int64_t *dst = malloc(N * sizeof(int64_t));
    CHECK(src && dst, "alloc");

    src[0] = 0;
    g_rng = 0xABCDEF01;
    for (size_t i = 1; i < N; i++) {
        int64_t jitter = (int64_t)(rand_u32() % 1001) - 500;
        src[i] = src[i - 1] + 1000 + jitter;
    }

    size_t out_bytes = 0;
    int rc = tsdb_pfor_encode_i64(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_encode_i64 jitter");

    rc = tsdb_pfor_decode_i64(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "pfor_decode_i64 jitter");

    for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "pfor i64 jitter round-trip");

    size_t dod_bytes = 0;
    tsdb_dod_encode(src, N, g_buf2, sizeof(g_buf2), &dod_bytes);

    printf("[PFOR-i64] jittered sensor   N=%-6zu  pfor=%zu bytes (%.3f B/pt)  "
           "dod=%zu bytes (%.3f B/pt)\n",
           N, out_bytes, (double)out_bytes / N,
           dod_bytes, (double)dod_bytes / N);

    free(src); free(dst);
}

static void test_pfor_i64_random(void)
{
    /* Fully random int64 — tests RAW block fallback. */
    const size_t N = 512;
    int64_t *src = malloc(N * sizeof(int64_t));
    int64_t *dst = malloc(N * sizeof(int64_t));
    CHECK(src && dst, "alloc");

    g_rng = 0x11223344;
    for (size_t i = 0; i < N; i++) {
        src[i] = (int64_t)((uint64_t)rand_u32() << 32 | rand_u32());
    }

    size_t out_bytes = 0;
    int rc = tsdb_pfor_encode_i64(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_encode_i64 random");

    rc = tsdb_pfor_decode_i64(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "pfor_decode_i64 random");

    for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "pfor i64 random round-trip");

    printf("[PFOR-i64] random int64      N=%-6zu  pfor=%zu bytes (%.3f B/pt)\n",
           N, out_bytes, (double)out_bytes / N);

    free(src); free(dst);
}

static void test_pfor_i64_edge_cases(void)
{
    /* n=0 */
    size_t out_bytes = 99;
    int rc = tsdb_pfor_encode_i64(NULL, 0, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "pfor_i64 n=0 encode");

    int64_t dummy = 0;
    rc = tsdb_pfor_decode_i64(g_buf, 0, &dummy, 0);
    CHECK(rc == TSDB_OK, "pfor_i64 n=0 decode");

    /* n=1 */
    int64_t one = (int64_t)0xDEADBEEFCAFE1234LL;
    rc = tsdb_pfor_encode_i64(&one, 1, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_i64 n=1 encode");
    int64_t got = 0;
    rc = tsdb_pfor_decode_i64(g_buf, out_bytes, &got, 1);
    CHECK(rc == TSDB_OK && got == one, "pfor_i64 n=1 decode");

    /* Truncated → CORRUPT. */
    int64_t vals[200];
    int64_t vals_out[200];
    for (int i = 0; i < 200; i++) vals[i] = (int64_t)i * 12345678LL;
    rc = tsdb_pfor_encode_i64(vals, 200, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "pfor_i64 corrupt setup");
    rc = tsdb_pfor_decode_i64(g_buf, out_bytes / 2, vals_out, 200);
    CHECK(rc == TSDB_ERR_CORRUPT, "pfor_i64 truncated → CORRUPT");

    printf("[PFOR-i64] edge cases: OK\n");
}

/* =========================================================================
 * Section 4: Compression ratio comparison — DoD vs PFOR
 * =========================================================================*/

static void test_compression_ratio_comparison(void)
{
    printf("\n--- Compression Ratio: DoD vs PFOR-Delta ---\n");
    printf("%-30s  %8s  %8s  %8s  %8s\n",
           "Pattern", "N", "DoD B/pt", "PFOR B/pt", "Winner");

    const struct {
        const char *name;
        size_t n;
        int pattern; /* 0=uniform_ts, 1=jitter, 2=bounded_random */
    } patterns[] = {
        {"uniform ts (1s apart)",     10000, 0},
        {"jittered sensor (+-500)",   10000, 1},
        {"bounded random (+-1M)",      1000, 2},
        {NULL, 0, 0}
    };

    for (int p = 0; patterns[p].name; p++) {
        size_t n = patterns[p].n;
        int64_t *src = malloc(n * sizeof(int64_t));
        int64_t *dst = malloc(n * sizeof(int64_t));
        CHECK(src && dst, "alloc");

        g_rng = 0xFEEDBEEF + (unsigned)p * 7;
        if (patterns[p].pattern == 0) {
            for (size_t i = 0; i < n; i++)
                src[i] = (int64_t)1700000000000000000LL + (int64_t)i * 1000000000LL;
        } else if (patterns[p].pattern == 1) {
            src[0] = 0;
            for (size_t i = 1; i < n; i++) {
                int64_t j = (int64_t)(rand_u32() % 1001) - 500;
                src[i] = src[i-1] + 1000 + j;
            }
        } else {
            /* Bounded random: random walk with deltas in [-1M, +1M].
             * Both DoD and PFOR can handle this without fallback. */
            src[0] = 0;
            for (size_t i = 1; i < n; i++) {
                int64_t delta = (int64_t)(rand_u32() % 2000001) - 1000000;
                src[i] = src[i-1] + delta;
            }
        }

        size_t dod_bytes = 0, pfor_bytes = 0;
        int dod_rc = tsdb_dod_encode(src, n, g_buf, sizeof(g_buf), &dod_bytes);
        CHECK(dod_rc == TSDB_OK, "dod encode comparison");
        CHECK(tsdb_pfor_encode_i64(src, n, g_buf2, sizeof(g_buf2), &pfor_bytes) == TSDB_OK,
              "pfor encode comparison");

        /* Verify round-trips. */
        CHECK(tsdb_dod_decode(g_buf, dod_bytes, dst, n) == TSDB_OK, "dod decode comparison");
        for (size_t i = 0; i < n; i++) CHECK_EQ(src[i], dst[i], "dod comparison rt");
        CHECK(tsdb_pfor_decode_i64(g_buf2, pfor_bytes, dst, n) == TSDB_OK, "pfor decode comparison");
        for (size_t i = 0; i < n; i++) CHECK_EQ(src[i], dst[i], "pfor comparison rt");

        const char *winner = (pfor_bytes < dod_bytes) ? "PFOR" :
                             (dod_bytes < pfor_bytes) ? "DoD" : "tie";
        printf("%-30s  %8zu  %8.3f  %9.3f  %8s\n",
               patterns[p].name, n,
               (double)dod_bytes / n,
               (double)pfor_bytes / n,
               winner);

        free(src); free(dst);
    }

    /* SYMBOL: DICT vs PFOR. */
    printf("\n--- Compression Ratio: DICT vs PFOR (SYMBOL) ---\n");
    printf("%-30s  %8s  %8s  %9s  %8s\n",
           "Pattern", "N", "DICT B/pt", "PFOR B/pt", "Winner");

    const struct {
        const char *name;
        size_t n;
        int cardinality;
    } sym_patterns[] = {
        {"low-card (10 symbols)",  10000, 10},
        {"mid-card (100 symbols)", 10000, 100},
        {"hi-card (1000 symbols)", 10000, 1000},
        {NULL, 0, 0}
    };

    for (int p = 0; sym_patterns[p].name; p++) {
        size_t n = sym_patterns[p].n;
        int card = sym_patterns[p].cardinality;
        uint32_t *src = malloc(n * sizeof(uint32_t));
        uint32_t *dst = malloc(n * sizeof(uint32_t));
        CHECK(src && dst, "alloc sym");

        g_rng = 0xABCD1234 + (unsigned)p;
        for (size_t i = 0; i < n; i++) src[i] = rand_u32() % (unsigned)card;

        size_t dict_bytes = 0, pfor_bytes = 0;
        CHECK(tsdb_dict_encode(src, n, g_buf, sizeof(g_buf), &dict_bytes) == TSDB_OK,
              "dict encode");
        CHECK(tsdb_pfor_encode(src, n, g_buf2, sizeof(g_buf2), &pfor_bytes) == TSDB_OK,
              "pfor encode sym");

        /* Verify round-trips. */
        CHECK(tsdb_dict_decode(g_buf, dict_bytes, dst, n) == TSDB_OK, "dict decode");
        for (size_t i = 0; i < n; i++) CHECK_EQ(src[i], dst[i], "dict rt");
        CHECK(tsdb_pfor_decode(g_buf2, pfor_bytes, dst, n) == TSDB_OK, "pfor decode sym");
        for (size_t i = 0; i < n; i++) CHECK_EQ(src[i], dst[i], "pfor sym rt");

        const char *winner = (pfor_bytes < dict_bytes) ? "PFOR" :
                             (dict_bytes < pfor_bytes) ? "DICT" : "tie";
        printf("%-30s  %8zu  %8.3f  %9.3f  %8s\n",
               sym_patterns[p].name, n,
               (double)dict_bytes / n,
               (double)pfor_bytes / n,
               winner);

        free(src); free(dst);
    }
}

/* =========================================================================
 * Section 5: Performance — 1M int64 decode throughput
 * =========================================================================*/

static double clock_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void test_performance(void)
{
    printf("\n--- Performance: 1M int64 decode throughput ---\n");
    const size_t N = 1 << 20; /* 1M values */

    int64_t *src = malloc(N * sizeof(int64_t));
    int64_t *dst = malloc(N * sizeof(int64_t));
    uint8_t *compressed = malloc(N * 10); /* generous upper bound */
    CHECK(src && dst && compressed, "alloc perf");

    /* Pattern: monotonic counter (best case for PFOR). */
    for (size_t i = 0; i < N; i++) {
        src[i] = (int64_t)1700000000000000000LL + (int64_t)i * 1000000LL;
    }

    /* Encode. */
    size_t pfor_bytes = 0;
    CHECK(tsdb_pfor_encode_i64(src, N, compressed, N * 10, &pfor_bytes) == TSDB_OK,
          "perf encode");

    /* Warmup: 3 passes. */
    for (int w = 0; w < 3; w++) {
        tsdb_pfor_decode_i64(compressed, pfor_bytes, dst, N);
    }

    /* Measure: 5 passes. */
    double t0 = clock_sec();
    const int PASSES = 5;
    for (int p = 0; p < PASSES; p++) {
        int rc = tsdb_pfor_decode_i64(compressed, pfor_bytes, dst, N);
        CHECK(rc == TSDB_OK, "perf decode");
    }
    double elapsed = clock_sec() - t0;

    /* Verify. */
    for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "perf rt");

    double total_ints = (double)N * PASSES;
    double int_per_sec = total_ints / elapsed;
    double bytes_per_sec = (double)pfor_bytes * PASSES / elapsed;

    printf("[PFOR-i64] 1M decode %d passes:  %.0f M int/sec  "
           "(%.1f MB/sec compressed)  [SIMD: %s]\n",
           PASSES, int_per_sec / 1e6, bytes_per_sec / 1e6,
           tsdb_simd_name());
    printf("[PFOR-i64] compressed size: %zu bytes / %zu ints = %.3f bytes/int\n",
           pfor_bytes, N, (double)pfor_bytes / N);

    /* Also benchmark PFOR for uint32 SYMBOL data. */
    uint32_t *u32src = malloc(N * sizeof(uint32_t));
    uint32_t *u32dst = malloc(N * sizeof(uint32_t));
    CHECK(u32src && u32dst, "alloc u32 perf");

    g_rng = 0xCAFE;
    for (size_t i = 0; i < N; i++) u32src[i] = rand_u32() % 100; /* 100 symbols */

    size_t sym_bytes = 0;
    CHECK(tsdb_pfor_encode(u32src, N, compressed, N * 10, &sym_bytes) == TSDB_OK,
          "sym perf encode");

    /* Warmup. */
    for (int w = 0; w < 3; w++) tsdb_pfor_decode(compressed, sym_bytes, u32dst, N);

    t0 = clock_sec();
    for (int p = 0; p < PASSES; p++) {
        int rc = tsdb_pfor_decode(compressed, sym_bytes, u32dst, N);
        CHECK(rc == TSDB_OK, "sym perf decode");
    }
    elapsed = clock_sec() - t0;

    for (size_t i = 0; i < N; i++) CHECK_EQ(u32src[i], u32dst[i], "sym perf rt");

    printf("[PFOR-u32] 1M sym  decode %d passes:  %.0f M int/sec\n",
           PASSES, (double)N * PASSES / elapsed / 1e6);

    free(src); free(dst); free(compressed);
    free(u32src); free(u32dst);
}

/* =========================================================================
 * Section 6: Large N tests (n=1000, 1M round-trip)
 * =========================================================================*/

static void test_large_n(void)
{
    printf("\n--- Large N round-trip tests ---\n");

    /* n=1000 with b=1..32 representative values. */
    {
        const size_t N = 1000;
        uint32_t *src = malloc(N * sizeof(uint32_t));
        uint32_t *dst = malloc(N * sizeof(uint32_t));
        for (size_t i = 0; i < N; i++) src[i] = (uint32_t)(i % 256);
        size_t ob = 0;
        CHECK(tsdb_pfor_encode(src, N, g_buf, sizeof(g_buf), &ob) == TSDB_OK, "large n=1000 enc");
        CHECK(tsdb_pfor_decode(g_buf, ob, dst, N) == TSDB_OK, "large n=1000 dec");
        for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "large n=1000 rt");
        printf("[PFOR] n=1000 round-trip: OK (%zu bytes, %.3f B/pt)\n", ob, (double)ob/N);
        free(src); free(dst);
    }

    /* i64 n=1000. */
    {
        const size_t N = 1000;
        int64_t *src = malloc(N * sizeof(int64_t));
        int64_t *dst = malloc(N * sizeof(int64_t));
        for (size_t i = 0; i < N; i++) src[i] = (int64_t)i * 999983LL;
        size_t ob = 0;
        CHECK(tsdb_pfor_encode_i64(src, N, g_buf, sizeof(g_buf), &ob) == TSDB_OK, "i64 n=1000 enc");
        CHECK(tsdb_pfor_decode_i64(g_buf, ob, dst, N) == TSDB_OK, "i64 n=1000 dec");
        for (size_t i = 0; i < N; i++) CHECK_EQ(src[i], dst[i], "i64 n=1000 rt");
        printf("[PFOR-i64] n=1000 round-trip: OK (%zu bytes, %.3f B/pt)\n", ob, (double)ob/N);
        free(src); free(dst);
    }

    printf("\n");
}

/* =========================================================================
 * Main
 * =========================================================================*/

int main(void)
{
    printf("=== tsdb PFOR / BP128 tests ===\n\n");
    printf("SIMD path: %s\n\n", tsdb_simd_name());

    /* BP128 round-trip. */
    test_bp128_roundtrip();

    /* PFOR uint32. */
    printf("--- PFOR uint32 ---\n");
    test_pfor_uniform();
    test_pfor_sparse_outliers();
    test_pfor_random();
    test_pfor_edge_cases();

    /* PFOR int64. */
    printf("\n--- PFOR int64 ---\n");
    test_pfor_i64_monotonic();
    test_pfor_i64_jittered();
    test_pfor_i64_random();
    test_pfor_i64_edge_cases();

    /* Large N. */
    test_large_n();

    /* Compression ratio. */
    test_compression_ratio_comparison();

    /* Performance. */
    test_performance();

    printf("\n=== All PFOR/BP128 tests passed. ===\n");
    return 0;
}
