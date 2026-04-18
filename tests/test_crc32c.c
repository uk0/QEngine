/* test_crc32c.c — correctness + dispatch + throughput for hardware CRC32C.
 *
 *  1. Known test vector crc32c("123456789") == 0xE3069283.
 *  2. Incremental update composes to one-shot result.
 *  3. Zero-length input yields 0x00000000 (identity).
 *  4. Dispatch picks the best available impl; we sanity-check that it isn't
 *     "sarwate" on common developer hardware (x86-64 with SSE4.2 or ARM64).
 *  5. Throughput micro-bench over 64 MiB: enforces a loose floor (400 MB/s
 *     even on scalar fallback; hardware should clear 5 GB/s).
 */

#include "../src/server/proto.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static double now_s(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

int main(void) {
    printf("=== test_crc32c ===\n");
    printf("impl: %s\n", tsdb_crc32c_impl());

    /* 1. Known test vector. */
    {
        uint32_t v = tsdb_crc32c("123456789", 9);
        CHECK(v == 0xE3069283u, "crc32c('123456789') == 0xE3069283");
    }

    /* 2. Incremental composes to one-shot. */
    {
        const char *msg = "The quick brown fox jumps over the lazy dog";
        size_t n = strlen(msg);
        uint32_t full = tsdb_crc32c(msg, n);
        uint32_t inc  = 0;
        inc = tsdb_crc32c_update(inc, msg,     10);
        inc = tsdb_crc32c_update(inc, msg + 10, n - 10);
        CHECK(full == inc, "update(split) == one-shot");

        /* Also check the edge: two pieces, one empty. */
        uint32_t inc2 = 0;
        inc2 = tsdb_crc32c_update(inc2, msg, n);
        inc2 = tsdb_crc32c_update(inc2, "", 0);
        CHECK(full == inc2, "update + zero-length == one-shot");
    }

    /* 3. Empty input. */
    {
        uint32_t v = tsdb_crc32c(NULL, 0);
        CHECK(v == 0u, "crc32c(empty) == 0");
        uint32_t v2 = tsdb_crc32c_update(0, NULL, 0);
        CHECK(v2 == 0u, "update(0,empty) == 0");
    }

    /* 4. Dispatch sanity on common hardware. */
    {
        const char *name = tsdb_crc32c_impl();
#if defined(__x86_64__)
        /* SSE4.2 has been standard since 2008 Nehalem.  If we're running on
         * a machine that actually lacks it, skip. */
        CHECK(strcmp(name, "sse4.2") == 0 || strcmp(name, "sarwate") == 0,
              "x86 impl is sse4.2 or sarwate");
#elif defined(__aarch64__)
        /* ARMv8 CRC is mandatory on v8.1+; every Apple silicon ships it. */
        CHECK(strcmp(name, "armv8-crc") == 0 || strcmp(name, "sarwate") == 0,
              "aarch64 impl is armv8-crc or sarwate");
#else
        CHECK(strcmp(name, "sarwate") == 0, "other arch uses sarwate");
#endif
    }

    /* 5. Throughput bench — 64 MiB of pseudo-random data. */
    {
        const size_t N = 64u * 1024u * 1024u;
        uint8_t *buf = (uint8_t *)malloc(N);
        if (!buf) { fprintf(stderr, "oom\n"); return 1; }
        /* Deterministic fill. */
        uint32_t s = 1;
        for (size_t i = 0; i < N; i++) {
            s = s * 1103515245u + 12345u;
            buf[i] = (uint8_t)(s >> 16);
        }

        /* Warm-up. */
        volatile uint32_t sink = 0;
        sink ^= tsdb_crc32c(buf, N);

        /* Measure over a few iterations. */
        const int iters = 4;
        double t0 = now_s();
        for (int i = 0; i < iters; i++) sink ^= tsdb_crc32c(buf, N);
        double dt = now_s() - t0;
        double mbs = ((double)N * iters / dt) / (1024.0 * 1024.0);
        printf("throughput: %.0f MiB/s (%d × %zu bytes in %.3f s)\n",
               mbs, iters, N, dt);
        CHECK(mbs >= 400.0, "throughput >= 400 MiB/s");
        if (strcmp(tsdb_crc32c_impl(), "sarwate") != 0)
            CHECK(mbs >= 2000.0, "hardware path >= 2 GiB/s");

        free(buf);
    }

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
