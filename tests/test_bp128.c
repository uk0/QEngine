/* test_bp128.c — exhaustive correctness gate for the SIMD-BP128 bit-packer.
 *
 * BP128 packs 128 uint32 values (4 interleaved lanes x 32) into b bits each.
 * The active kernel is chosen at compile time (scalar / NEON / AVX2).  This
 * test pins correctness REGARDLESS of which path is compiled:
 *
 *   1. round-trip: pack(b) then unpack(b) reproduces the input exactly, for
 *      every bit width b in 1..32 and many random blocks (values masked to b
 *      bits, since BP128 only preserves the low b bits — that is its
 *      contract: PFOR feeds it residuals already < 2^b).
 *   2. bit-identical: the compiled kernel's packed output must byte-match an
 *      INDEPENDENT scalar reference implemented here.  This catches any SIMD
 *      lane-ordering / shift bug — a corrupted pack would diverge from the
 *      reference even if it happens to round-trip.
 *
 * Data integrity is paramount: a silent BP128 bug corrupts every PFOR-encoded
 * INT64 / SYMBOL column.  This gate must pass on the exact build (and the
 * exact -m flags) that ships.
 */

#include "../src/compress/bp128.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

static int g_pass = 0, g_fail = 0;
#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else g_pass++; \
} while (0)

/* Independent scalar reference (separate from the engine's, so a bug in the
 * engine's scalar can't hide behind an identical bug here).  64-bit
 * accumulators per lane; mirrors the BP128 4-lane interleaved layout. */
static void ref_pack(const uint32_t *src, uint32_t *dst, unsigned b) {
    const uint64_t mask = (b == 32) ? 0xFFFFFFFFULL : ((1ULL << b) - 1ULL);
    uint64_t acc[4] = {0,0,0,0};
    unsigned bits = 0, oidx = 0;
    memset(dst, 0, (size_t)b * 16);
    for (unsigned i = 0; i < 32; i++) {
        for (int l = 0; l < 4; l++)
            acc[l] |= ((uint64_t)src[i*4+l] & mask) << bits;
        bits += b;
        while (bits >= 32) {
            for (int l = 0; l < 4; l++) { dst[oidx*4+l] = (uint32_t)acc[l]; acc[l] >>= 32; }
            oidx++; bits -= 32;
        }
    }
    if (bits > 0) for (int l = 0; l < 4; l++) dst[oidx*4+l] = (uint32_t)acc[l];
}

static uint64_t rng_state = 0x9E3779B97F4A7C15ULL;
static uint32_t rng(void) {
    rng_state = rng_state * 6364136223846793005ULL + 1442695040888963407ULL;
    return (uint32_t)(rng_state >> 32);
}

int main(void) {
    printf("=== test_bp128 (exhaustive pack/unpack correctness) ===\n");

    uint32_t in[128], packed_k[64 * 16], packed_ref[64 * 16], out[128];

    for (unsigned b = 1; b <= 32; b++) {
        const uint32_t vmask = (b == 32) ? 0xFFFFFFFFu : ((1u << b) - 1u);
        int rt_ok = 1, bi_ok = 1;

        for (int trial = 0; trial < 200; trial++) {
            for (int i = 0; i < 128; i++) in[i] = rng() & vmask;

            /* engine kernel (scalar/NEON/AVX2 per build) */
            memset(packed_k, 0xCD, sizeof(packed_k));
            CHECK(tsdb_bp128_pack(in, packed_k, b) == TSDB_OK, "pack rc");

            /* independent scalar reference */
            ref_pack(in, packed_ref, b);

            /* bit-identical: kernel output == reference output (b*16 bytes) */
            if (memcmp(packed_k, packed_ref, (size_t)b * 16) != 0) bi_ok = 0;

            /* round-trip */
            memset(out, 0xAB, sizeof(out));
            CHECK(tsdb_bp128_unpack(packed_k, out, b) == TSDB_OK, "unpack rc");
            if (memcmp(in, out, sizeof(in)) != 0) rt_ok = 0;
        }

        CHECK(rt_ok, "round-trip exact");
        CHECK(bi_ok, "kernel bit-identical to scalar reference");
        if (!rt_ok || !bi_ok)
            fprintf(stderr, "  b=%u: round-trip=%d bit-identical=%d\n", b, rt_ok, bi_ok);
    }

    /* Edge: all-zero and all-max blocks at a few widths. */
    for (unsigned b = 1; b <= 32; b++) {
        const uint32_t vmask = (b == 32) ? 0xFFFFFFFFu : ((1u << b) - 1u);
        for (int i = 0; i < 128; i++) in[i] = 0;
        tsdb_bp128_pack(in, packed_k, b); tsdb_bp128_unpack(packed_k, out, b);
        CHECK(memcmp(in, out, sizeof(in)) == 0, "all-zero round-trip");
        for (int i = 0; i < 128; i++) in[i] = vmask;
        tsdb_bp128_pack(in, packed_k, b); tsdb_bp128_unpack(packed_k, out, b);
        CHECK(memcmp(in, out, sizeof(in)) == 0, "all-max round-trip");
    }

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
