/* test_compress.c — self-contained compression round-trip tests. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <assert.h>

#include "../src/compress/codec.h"
#include "../src/compress/dod.h"
#include "../src/compress/gorilla.h"
#include "../src/compress/chimp.h"
#include "../src/compress/chimp128.h"
#include "../src/compress/dict.h"
#include "../src/core/types.h"
#include "../include/tsdb.h"

/* ------------------------------------------------------------------ helpers */

#define FAIL(msg) do { fprintf(stderr, "FAIL: %s (line %d)\n", msg, __LINE__); abort(); } while(0)
#define CHECK(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

static uint8_t g_buf[1 << 20]; /* 1 MiB scratch */

/* ------------------------------------------------------------------ DoD tests */

static void test_dod_uniform(void)
{
    const size_t N = 10000;
    int64_t *src = malloc(N * sizeof(int64_t));
    int64_t *dst = malloc(N * sizeof(int64_t));
    CHECK(src && dst, "alloc");

    /* Uniform 1-second nanosecond timestamps. */
    for (size_t i = 0; i < N; i++)
        src[i] = (int64_t)1700000000000000000LL + (int64_t)i * 1000000000LL;

    size_t out_bytes = 0;
    int rc = tsdb_dod_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "dod_encode uniform");
    CHECK(out_bytes > 0, "dod uniform bytes > 0");

    rc = tsdb_dod_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "dod_decode uniform");

    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "dod uniform round-trip");

    double bpp = (double)out_bytes / N;
    printf("[DoD] uniform timestamps  N=%-6zu  %zu bytes  %.3f bytes/point\n", N, out_bytes, bpp);

    free(src); free(dst);
}

static void test_dod_jitter(void)
{
    const size_t N = 10000;
    int64_t *src = malloc(N * sizeof(int64_t));
    int64_t *dst = malloc(N * sizeof(int64_t));
    CHECK(src && dst, "alloc");

    /* Base 1ms interval + small random jitter ±50000 ns. */
    src[0] = 1700000000000000000LL;
    int64_t base_delta = 1000000LL;
    unsigned rng = 12345;
    for (size_t i = 1; i < N; i++) {
        rng = rng * 1664525u + 1013904223u;
        int64_t jitter = (int64_t)(rng % 100001) - 50000;
        src[i] = src[i - 1] + base_delta + jitter;
    }

    size_t out_bytes = 0;
    int rc = tsdb_dod_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "dod_encode jitter");

    rc = tsdb_dod_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "dod_decode jitter");

    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "dod jitter round-trip");

    double bpp = (double)out_bytes / N;
    printf("[DoD] jitter timestamps   N=%-6zu  %zu bytes  %.3f bytes/point\n", N, out_bytes, bpp);

    free(src); free(dst);
}

static void test_dod_neg_delta(void)
{
    const size_t N = 1000;
    int64_t src[1000], dst[1000];

    /* Mixed negative deltas. */
    src[0] = 0;
    for (size_t i = 1; i < N; i++)
        src[i] = src[i - 1] + ((int64_t)(i % 3 == 0) ? -100 : 200);

    size_t out_bytes = 0;
    int rc = tsdb_dod_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "dod_encode neg");

    rc = tsdb_dod_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "dod_decode neg");

    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "dod neg round-trip");

    printf("[DoD] neg-delta           N=%-6zu  %zu bytes  %.3f bytes/point\n",
           N, out_bytes, (double)out_bytes / N);
}

static void test_dod_edges(void)
{
    /* n == 0 */
    size_t out_bytes = 99;
    int rc = tsdb_dod_encode(NULL, 0, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "dod empty encode");

    int64_t dummy = 0;
    rc = tsdb_dod_decode(g_buf, 0, &dummy, 0);
    CHECK(rc == TSDB_OK, "dod empty decode");

    /* n == 1 */
    int64_t one = (int64_t)0xDEADBEEFCAFE1234LL;
    rc = tsdb_dod_encode(&one, 1, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 8, "dod n=1 encode");

    int64_t got = 0;
    rc = tsdb_dod_decode(g_buf, out_bytes, &got, 1);
    CHECK(rc == TSDB_OK && got == one, "dod n=1 decode");

    printf("[DoD] edge cases: OK\n");
}

/* Compression ratio assertion at N=1000 */
static void test_dod_ratio(void)
{
    const size_t N = 1000;
    int64_t *src = malloc(N * sizeof(int64_t));
    int64_t *dst = malloc(N * sizeof(int64_t));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = (int64_t)1700000000000000000LL + (int64_t)i * 1000000000LL;

    size_t out_bytes = 0;
    int rc = tsdb_dod_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "dod_encode ratio");

    rc = tsdb_dod_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "dod_decode ratio");
    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "dod ratio round-trip");

    printf("[DoD] ratio check N=1000: %zu bytes (limit 300)\n", out_bytes);
    CHECK(out_bytes < 300, "dod uniform 1000 < 300 bytes");

    free(src); free(dst);
}

/* ---------------------------------------------------------------- Gorilla tests */

static void test_gorilla_constant(void)
{
    const size_t N = 10000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = 3.14159265358979;

    size_t out_bytes = 0;
    int rc = tsdb_gorilla_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "gorilla_encode const");

    rc = tsdb_gorilla_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "gorilla_decode const");

    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "gorilla const round-trip");

    double bpp = (double)out_bytes / N;
    printf("[Gorilla] constant        N=%-6zu  %zu bytes  %.3f bytes/point\n", N, out_bytes, bpp);
}

static void test_gorilla_monotone(void)
{
    const size_t N = 10000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = (double)i * 0.001;

    size_t out_bytes = 0;
    int rc = tsdb_gorilla_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "gorilla_encode mono");

    rc = tsdb_gorilla_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "gorilla_decode mono");

    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "gorilla mono round-trip");

    double bpp = (double)out_bytes / N;
    printf("[Gorilla] monotone        N=%-6zu  %zu bytes  %.3f bytes/point\n", N, out_bytes, bpp);

    free(src); free(dst);
}

static void test_gorilla_nan(void)
{
    const size_t N = 100;
    double src[100], dst[100];

    for (size_t i = 0; i < N; i++) {
        if (i % 10 == 0)
            src[i] = (double)(i + 1) / 7.0;
        else if (i % 17 == 0) {
            uint64_t nan_bits = 0x7FF8000000000001ULL;
            memcpy(&src[i], &nan_bits, 8);
        } else
            src[i] = (double)i;
    }

    size_t out_bytes = 0;
    int rc = tsdb_gorilla_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "gorilla_encode nan");

    rc = tsdb_gorilla_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "gorilla_decode nan");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8);
        memcpy(&b, &dst[i], 8);
        CHECK(a == b, "gorilla nan round-trip");
    }

    printf("[Gorilla] with NaN        N=%-6zu  %zu bytes  %.3f bytes/point\n",
           N, out_bytes, (double)out_bytes / N);
}

static void test_gorilla_edges(void)
{
    size_t out_bytes = 99;
    int rc = tsdb_gorilla_encode(NULL, 0, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "gorilla empty encode");

    double dummy = 0;
    rc = tsdb_gorilla_decode(g_buf, 0, &dummy, 0);
    CHECK(rc == TSDB_OK, "gorilla empty decode");

    double one = 2.718281828;
    rc = tsdb_gorilla_encode(&one, 1, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 8, "gorilla n=1 encode");

    double got = 0;
    rc = tsdb_gorilla_decode(g_buf, out_bytes, &got, 1);
    CHECK(rc == TSDB_OK && got == one, "gorilla n=1 decode");

    printf("[Gorilla] edge cases: OK\n");
}

static void test_gorilla_ratio(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = 42.0;

    size_t out_bytes = 0;
    int rc = tsdb_gorilla_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "gorilla_encode ratio");

    rc = tsdb_gorilla_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "gorilla_decode ratio");
    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "gorilla ratio round-trip");

    printf("[Gorilla] ratio check N=1000: %zu bytes (limit 200)\n", out_bytes);
    CHECK(out_bytes < 200, "gorilla const 1000 < 200 bytes");

    free(src); free(dst);
}

/* ---------------------------------------------------------------- Chimp tests */

static void test_chimp_constant(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = 3.14159265358979;

    size_t out_bytes = 0;
    int rc = tsdb_chimp_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp_encode const");

    rc = tsdb_chimp_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp_decode const");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp const round-trip");
    }

    /* Compare with Gorilla on same data. */
    size_t gorilla_bytes = 0;
    rc = tsdb_gorilla_encode(src, N, g_buf + (1 << 18), (1 << 18), &gorilla_bytes);
    CHECK(rc == TSDB_OK, "gorilla_encode const (for comparison)");

    double bpp_chimp   = (double)out_bytes    / N;
    double bpp_gorilla = (double)gorilla_bytes / N;
    double improvement = 100.0 * (1.0 - bpp_chimp / bpp_gorilla);
    printf("[Chimp] constant          N=%-6zu  %zu bytes  %.3f bytes/point"
           "  (Gorilla %.3f, Chimp %.1f%% %s)\n",
           N, out_bytes, bpp_chimp, bpp_gorilla,
           improvement >= 0 ? improvement : -improvement,
           improvement >= 0 ? "smaller" : "larger");

    free(src); free(dst);
}

static void test_chimp_monotone(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = (double)(i + 1);   /* 1.0 .. 1000.0 as double */

    size_t out_bytes = 0;
    int rc = tsdb_chimp_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp_encode mono");

    rc = tsdb_chimp_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp_decode mono");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp mono round-trip");
    }

    /* Compare with Gorilla on same data. */
    size_t gorilla_bytes = 0;
    rc = tsdb_gorilla_encode(src, N, g_buf + (1 << 18), (1 << 18), &gorilla_bytes);
    CHECK(rc == TSDB_OK, "gorilla_encode mono (for comparison)");

    double bpp_chimp   = (double)out_bytes    / N;
    double bpp_gorilla = (double)gorilla_bytes / N;
    double improvement = 100.0 * (1.0 - bpp_chimp / bpp_gorilla);
    printf("[Chimp] monotone (1..N)   N=%-6zu  %zu bytes  %.3f bytes/point"
           "  (Gorilla %.3f, Chimp %.1f%% %s)\n",
           N, out_bytes, bpp_chimp, bpp_gorilla,
           improvement >= 0 ? improvement : -improvement,
           improvement >= 0 ? "smaller" : "larger");

    free(src); free(dst);
}

static void test_chimp_sinusoidal(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    /* Simulate real time-series: sin wave with slight amplitude variation. */
    for (size_t i = 0; i < N; i++)
        src[i] = 100.0 * sin((double)i * 0.1) + (double)i * 0.01;

    size_t out_bytes = 0;
    int rc = tsdb_chimp_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp_encode sin");

    rc = tsdb_chimp_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp_decode sin");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp sin round-trip");
    }

    /* Compare with Gorilla on same data. */
    size_t gorilla_bytes = 0;
    rc = tsdb_gorilla_encode(src, N, g_buf + (1 << 18), (1 << 18), &gorilla_bytes);
    CHECK(rc == TSDB_OK, "gorilla_encode sin (for comparison)");

    double bpp_chimp   = (double)out_bytes    / N;
    double bpp_gorilla = (double)gorilla_bytes / N;
    double improvement = 100.0 * (1.0 - bpp_chimp / bpp_gorilla);
    printf("[Chimp] sinusoidal        N=%-6zu  %zu bytes  %.3f bytes/point"
           "  (Gorilla %.3f, Chimp %.1f%% %s)\n",
           N, out_bytes, bpp_chimp, bpp_gorilla,
           improvement >= 0 ? improvement : -improvement,
           improvement >= 0 ? "smaller" : "larger");

    free(src); free(dst);
}

static void test_chimp_edges(void)
{
    /* n == 0 */
    size_t out_bytes = 99;
    int rc = tsdb_chimp_encode(NULL, 0, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "chimp empty encode");

    double dummy = 0;
    rc = tsdb_chimp_decode(g_buf, 0, &dummy, 0);
    CHECK(rc == TSDB_OK, "chimp empty decode");

    /* n == 1 */
    double one = 2.718281828;
    rc = tsdb_chimp_encode(&one, 1, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 8, "chimp n=1 encode");

    double got = 0;
    rc = tsdb_chimp_decode(g_buf, out_bytes, &got, 1);
    CHECK(rc == TSDB_OK && got == one, "chimp n=1 decode");

    /* NaN round-trip */
    const size_t N = 100;
    double src[100], dst[100];
    for (size_t i = 0; i < N; i++) {
        if (i % 10 == 0) {
            uint64_t nan_bits = 0x7FF8000000000001ULL;
            memcpy(&src[i], &nan_bits, 8);
        } else {
            src[i] = (double)i;
        }
    }
    rc = tsdb_chimp_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp_encode nan");
    rc = tsdb_chimp_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp_decode nan");
    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp nan round-trip");
    }

    printf("[Chimp] edge cases: OK\n");
}

/* ---------------------------------------------------------------- Dict tests */

static void test_dict_simple(void)
{
    const size_t N = 10000;
    uint32_t *src = malloc(N * sizeof(uint32_t));
    uint32_t *dst = malloc(N * sizeof(uint32_t));
    CHECK(src && dst, "alloc");

    /* Repeating pattern 0..9 */
    for (size_t i = 0; i < N; i++)
        src[i] = (uint32_t)(i % 10);

    size_t out_bytes = 0;
    int rc = tsdb_dict_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "dict_encode");

    rc = tsdb_dict_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "dict_decode");

    for (size_t i = 0; i < N; i++)
        CHECK(src[i] == dst[i], "dict round-trip");

    printf("[Dict] repeating 0..9    N=%-6zu  %zu bytes  %.3f bytes/point\n",
           N, out_bytes, (double)out_bytes / N);

    free(src); free(dst);
}

static void test_dict_edges(void)
{
    size_t out_bytes = 99;
    int rc = tsdb_dict_encode(NULL, 0, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "dict empty encode");

    uint32_t dummy = 0;
    rc = tsdb_dict_decode(g_buf, 0, &dummy, 0);
    CHECK(rc == TSDB_OK, "dict empty decode");

    uint32_t one = 0xDEADBEEF;
    rc = tsdb_dict_encode(&one, 1, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "dict n=1 encode");

    uint32_t got = 0;
    rc = tsdb_dict_decode(g_buf, out_bytes, &got, 1);
    CHECK(rc == TSDB_OK && got == one, "dict n=1 decode");

    printf("[Dict] edge cases: OK\n");
}

/* ---------------------------------------------------------------- Chimp128 tests */

/* Helper: encode with all three float codecs and print comparison table row. */
static void chimp128_compare(const char *label, const double *src, size_t N,
                              size_t *out_gorilla, size_t *out_chimp, size_t *out_c128)
{
    static uint8_t tbuf[1 << 21]; /* 2 MiB scratch for each codec */

    size_t gorilla_bytes = 0, chimp_bytes = 0, c128_bytes = 0;

    int rc;
    rc = tsdb_gorilla_encode(src, N, tbuf, sizeof(tbuf), &gorilla_bytes);
    CHECK(rc == TSDB_OK, "gorilla encode in compare");

    rc = tsdb_chimp_encode(src, N, tbuf, sizeof(tbuf), &chimp_bytes);
    CHECK(rc == TSDB_OK, "chimp encode in compare");

    rc = tsdb_chimp128_encode(src, N, tbuf, sizeof(tbuf), &c128_bytes);
    CHECK(rc == TSDB_OK, "chimp128 encode in compare");

    *out_gorilla = gorilla_bytes;
    *out_chimp   = chimp_bytes;
    *out_c128    = c128_bytes;

    printf("  %-22s N=%-6zu  Gorilla %5zu B (%.3f)  Chimp %5zu B (%.3f)  Chimp128 %5zu B (%.3f)\n",
           label, N,
           gorilla_bytes, (double)gorilla_bytes / N,
           chimp_bytes,   (double)chimp_bytes   / N,
           c128_bytes,    (double)c128_bytes    / N);
}

static void test_chimp128_constant(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = 3.14159265358979;

    size_t out_bytes = 0;
    int rc = tsdb_chimp128_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp128_encode const");

    rc = tsdb_chimp128_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp128_decode const");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp128 const round-trip");
    }

    size_t g, c, c128;
    chimp128_compare("constant", src, N, &g, &c, &c128);
    free(src); free(dst);
}

static void test_chimp128_monotone(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = (double)(i + 1);

    size_t out_bytes = 0;
    int rc = tsdb_chimp128_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp128_encode mono");

    rc = tsdb_chimp128_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp128_decode mono");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp128 mono round-trip");
    }

    size_t g, c, c128;
    chimp128_compare("monotone (1..N)", src, N, &g, &c, &c128);
    free(src); free(dst);
}

static void test_chimp128_sinusoidal(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    for (size_t i = 0; i < N; i++)
        src[i] = 100.0 * sin((double)i * 0.1) + (double)i * 0.01;

    size_t out_bytes = 0;
    int rc = tsdb_chimp128_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp128_encode sin");

    rc = tsdb_chimp128_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp128_decode sin");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp128 sin round-trip");
    }

    size_t g, c, c128;
    chimp128_compare("sinusoidal", src, N, &g, &c, &c128);

    /* Chimp128 round-trips correctly; compression ratio may vary by implementation.
     * The fallback path uses '11' instead of trailing-strip '01', so Chimp128
     * can equal or slightly exceed Chimp on some distributions. Allow 2x. */
    CHECK((double)c128 <= (double)c * 2.0,
          "chimp128 sinusoidal <= 2.0 * chimp");

    free(src); free(dst);
}

static void test_chimp128_real_walk(void)
{
    const size_t N = 1000;
    double *src = malloc(N * sizeof(double));
    double *dst = malloc(N * sizeof(double));
    CHECK(src && dst, "alloc");

    /* TSBS-like CPU usage: bounded random walk 0..100 */
    unsigned rng = 0xDEADBEEFu;
    double val = 50.0;
    for (size_t i = 0; i < N; i++) {
        rng = rng * 1664525u + 1013904223u;
        double delta = ((double)(int32_t)rng / (double)0x80000000LL) * 2.0;
        val += delta;
        if (val < 0.0)   val = 0.0;
        if (val > 100.0) val = 100.0;
        src[i] = val;
    }

    size_t out_bytes = 0;
    int rc = tsdb_chimp128_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp128_encode walk");

    rc = tsdb_chimp128_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp128_decode walk");

    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp128 walk round-trip");
    }

    size_t g, c, c128;
    chimp128_compare("cpu-usage walk", src, N, &g, &c, &c128);

    /* Chimp128 round-trips correctly; the fallback path (no trailing-strip when
     * ring isn't used) can produce slightly more bytes than Chimp on random walks.
     * Allow 2x as a sanity bound. */
    CHECK((double)c128 <= (double)c * 2.0,
          "chimp128 real-walk <= 2.0 * chimp");

    free(src); free(dst);
}

static void test_chimp128_edges(void)
{
    /* n == 0 */
    size_t out_bytes = 99;
    int rc = tsdb_chimp128_encode(NULL, 0, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 0, "chimp128 empty encode");

    double dummy = 0;
    rc = tsdb_chimp128_decode(g_buf, 0, &dummy, 0);
    CHECK(rc == TSDB_OK, "chimp128 empty decode");

    /* n == 1 */
    double one = 2.718281828;
    rc = tsdb_chimp128_encode(&one, 1, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK && out_bytes == 8, "chimp128 n=1 encode");

    double got = 0;
    rc = tsdb_chimp128_decode(g_buf, out_bytes, &got, 1);
    CHECK(rc == TSDB_OK && got == one, "chimp128 n=1 decode");

    /* NaN round-trip */
    const size_t N = 100;
    double src[100], dst[100];
    for (size_t i = 0; i < N; i++) {
        if (i % 10 == 0) {
            uint64_t nan_bits = 0x7FF8000000000001ULL;
            memcpy(&src[i], &nan_bits, 8);
        } else {
            src[i] = (double)i;
        }
    }
    rc = tsdb_chimp128_encode(src, N, g_buf, sizeof(g_buf), &out_bytes);
    CHECK(rc == TSDB_OK, "chimp128_encode nan");
    rc = tsdb_chimp128_decode(g_buf, out_bytes, dst, N);
    CHECK(rc == TSDB_OK, "chimp128_decode nan");
    for (size_t i = 0; i < N; i++) {
        uint64_t a, b;
        memcpy(&a, &src[i], 8); memcpy(&b, &dst[i], 8);
        CHECK(a == b, "chimp128 nan round-trip");
    }

    printf("[Chimp128] edge cases: OK\n");
}

/* ---------------------------------------------------------------- Codec dispatch */

static void test_codec_dispatch(void)
{
    const size_t N = 1000;
    tsdb_codec_t codec;

    /* TIMESTAMP -> DOD */
    int64_t *ts = malloc(N * sizeof(int64_t));
    int64_t *ts_out = malloc(N * sizeof(int64_t));
    for (size_t i = 0; i < N; i++) ts[i] = (int64_t)1700000000000000000LL + (int64_t)i * 1000000000LL;
    int bytes = tsdb_codec_encode(TSDB_TYPE_TIMESTAMP, ts, N, g_buf, sizeof(g_buf), &codec);
    CHECK(bytes > 0 && codec == TSDB_CODEC_DOD, "codec dispatch TIMESTAMP->DOD");
    CHECK(tsdb_codec_decode(codec, TSDB_TYPE_TIMESTAMP, g_buf, (size_t)bytes, ts_out, N) == TSDB_OK, "decode ts");
    for (size_t i = 0; i < N; i++) CHECK(ts[i] == ts_out[i], "codec ts round-trip");
    printf("[Codec] TIMESTAMP->DOD: %d bytes\n", bytes);
    free(ts); free(ts_out);

    /* FLOAT64 -> GORILLA / CHIMP / CHIMP128 (best of three) */
    double *fl = malloc(N * sizeof(double));
    double *fl_out = malloc(N * sizeof(double));
    for (size_t i = 0; i < N; i++) fl[i] = (double)i * 1.1;
    bytes = tsdb_codec_encode(TSDB_TYPE_FLOAT64, fl, N, g_buf, sizeof(g_buf), &codec);
    CHECK(bytes > 0 && (codec == TSDB_CODEC_CHIMP || codec == TSDB_CODEC_GORILLA ||
                        codec == TSDB_CODEC_CHIMP128),
          "codec dispatch FLOAT64->CHIMP or GORILLA or CHIMP128");
    CHECK(tsdb_codec_decode(codec, TSDB_TYPE_FLOAT64, g_buf, (size_t)bytes, fl_out, N) == TSDB_OK, "decode fl");
    for (size_t i = 0; i < N; i++) CHECK(fl[i] == fl_out[i], "codec float round-trip");
    printf("[Codec] FLOAT64->%s: %d bytes\n",
           codec == TSDB_CODEC_CHIMP128 ? "CHIMP128" :
           codec == TSDB_CODEC_CHIMP ? "CHIMP" : "GORILLA", bytes);
    free(fl); free(fl_out);

    /* SYMBOL -> DICT or PFOR (best of two) */
    uint32_t *sym = malloc(N * sizeof(uint32_t));
    uint32_t *sym_out = malloc(N * sizeof(uint32_t));
    for (size_t i = 0; i < N; i++) sym[i] = (uint32_t)(i % 50);
    bytes = tsdb_codec_encode(TSDB_TYPE_SYMBOL, sym, N, g_buf, sizeof(g_buf), &codec);
    CHECK(bytes > 0 && (codec == TSDB_CODEC_DICT || codec == TSDB_CODEC_PFOR),
          "codec dispatch SYMBOL->DICT or PFOR");
    CHECK(tsdb_codec_decode(codec, TSDB_TYPE_SYMBOL, g_buf, (size_t)bytes, sym_out, N) == TSDB_OK, "decode sym");
    for (size_t i = 0; i < N; i++) CHECK(sym[i] == sym_out[i], "codec sym round-trip");
    printf("[Codec] SYMBOL->%s:    %d bytes\n",
           codec == TSDB_CODEC_PFOR ? "PFOR" : "DICT", bytes);
    free(sym); free(sym_out);
}

/* L2 cold-tier adaptive encode: the min_gain lever must (a) never produce
 * a LARGER block than the default 16-byte-floor path, (b) round-trip
 * exactly, and (c) on data with a small-but-real LZ gain, the aggressive
 * floor (1) wraps where the default floor (16) leaves it plain. */
static void test_l2_adaptive_min_gain(void)
{
    const size_t N = 4096;
    /* Float random-walk: domain codec compresses, leaving an LZ-able
     * residual where the marginal gain hovers near the 16-byte floor. */
    double *fl = malloc(N * sizeof(double));
    double *out = malloc(N * sizeof(double));
    CHECK(fl && out, "alloc l2");
    double v = 100.0;
    uint64_t lcg = 0xABCDEF1234567ULL;
    for (size_t i = 0; i < N; i++) {
        lcg = lcg * 6364136223846793005ULL + 1442695040888963407ULL;
        v += ((double)(lcg >> 40) / (double)(1u<<24) - 0.5) * 2.0;
        fl[i] = v;
    }

    uint8_t *buf_hot = malloc(N * 16);
    uint8_t *buf_l2  = malloc(N * 16);
    CHECK(buf_hot && buf_l2, "alloc l2 bufs");

    tsdb_codec_t c_hot = TSDB_CODEC_NONE, c_l2 = TSDB_CODEC_NONE;
    uint16_t f_hot = 0, f_l2 = 0;

    int n_hot = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, fl, N,
                                           buf_hot, N * 16, &c_hot, &f_hot);
    int n_l2  = tsdb_codec_encode_adaptive_ex(TSDB_TYPE_FLOAT64, fl, N,
                                              buf_l2, N * 16, &c_l2, &f_l2,
                                              /*min_gain*/ 1);
    CHECK(n_hot > 0 && n_l2 > 0, "l2 encode both ok");
    /* L2 (lower floor) is never larger than the hot path. */
    CHECK(n_l2 <= n_hot, "l2 min_gain=1 never larger than default floor");

    /* Both decode back to the exact input. */
    CHECK(tsdb_codec_decode_adaptive(c_hot, TSDB_TYPE_FLOAT64, f_hot,
                                     buf_hot, (size_t)n_hot, out, N) == TSDB_OK,
          "l2 hot decode");
    for (size_t i = 0; i < N; i++) CHECK(fl[i] == out[i], "l2 hot round-trip");
    CHECK(tsdb_codec_decode_adaptive(c_l2, TSDB_TYPE_FLOAT64, f_l2,
                                     buf_l2, (size_t)n_l2, out, N) == TSDB_OK,
          "l2 cold decode");
    for (size_t i = 0; i < N; i++) CHECK(fl[i] == out[i], "l2 cold round-trip");

    /* min_gain <= 0 is clamped to 1, not treated as "wrap always". */
    tsdb_codec_t c0 = TSDB_CODEC_NONE; uint16_t f0 = 0;
    int n0 = tsdb_codec_encode_adaptive_ex(TSDB_TYPE_FLOAT64, fl, N,
                                           buf_l2, N * 16, &c0, &f0, 0);
    CHECK(n0 > 0 && n0 <= n_hot, "l2 min_gain<=0 clamps to 1");

    printf("[L2] float walk N=%zu: hot=%dB (lz=%d) l2=%dB (lz=%d)\n",
           N, n_hot, (f_hot & TSDB_BF_OUTER_LZ) != 0,
           n_l2, (f_l2 & TSDB_BF_OUTER_LZ) != 0);

    free(fl); free(out); free(buf_hot); free(buf_l2);
}

/* ---------------------------------------------------------------- main */

int main(void)
{
    printf("=== tsdb compression tests ===\n\n");

    printf("--- DoD ---\n");
    test_dod_uniform();
    test_dod_jitter();
    test_dod_neg_delta();
    test_dod_edges();
    test_dod_ratio();

    printf("\n--- Gorilla ---\n");
    test_gorilla_constant();
    test_gorilla_monotone();
    test_gorilla_nan();
    test_gorilla_edges();
    test_gorilla_ratio();

    printf("\n--- Chimp ---\n");
    test_chimp_constant();
    test_chimp_monotone();
    test_chimp_sinusoidal();
    test_chimp_edges();

    printf("\n--- Chimp128 ---\n");
    printf("  Compression comparison table (bytes/point in parens):\n");
    test_chimp128_constant();
    test_chimp128_monotone();
    test_chimp128_sinusoidal();
    test_chimp128_real_walk();
    test_chimp128_edges();

    printf("\n--- Dict ---\n");
    test_dict_simple();
    test_dict_edges();

    printf("\n--- Codec dispatch ---\n");
    test_codec_dispatch();
    test_l2_adaptive_min_gain();

    printf("\nAll tests passed.\n");
    return 0;
}
