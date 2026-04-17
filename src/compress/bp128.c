/* bp128.c — SIMD-accelerated 128-integer bit-packing (Lemire & Boytsov style).
 *
 * Pack layout: 4-way interleaved.  Given 128 values v[0..127]:
 *   Lane k (k=0,1,2,3) holds v[k], v[k+4], v[k+8], ..., v[k+124]  (32 values each).
 *
 * Each lane packs its 32 values at bit-width b using LSB-first bit packing.
 * Each lane's packed output is b 32-bit words.
 * These are interleaved in the output: output word at index (w*4 + lane) holds
 * word w of lane (lane).  Total: b*4 uint32 words = b*16 bytes.
 *
 * Both pack and unpack use 64-bit accumulators to avoid undefined behavior
 * from shifts >= 32.
 *
 * NEON path: loads/stores 4 lanes via uint32x4_t for better throughput.
 * Scalar path: processes all 4 lanes in parallel with uint64_t accumulators.
 */

#include "bp128.h"
#include "../exec/simd.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>

#ifdef TSDB_SIMD_NEON
#include <arm_neon.h>
#endif

/* =========================================================================
 * Core pack/unpack: 4 interleaved lanes, each with 32 values at b bits.
 *
 * Both functions process 4 lanes in parallel using 64-bit accumulators.
 * This handles the case where bits_in_acc can be up to 63, ensuring no UB.
 * =========================================================================*/

/*
 * pack4: Pack 4 interleaved lanes from src[0..127] into dst[0..b*4-1].
 * src layout: src[i*4 + lane] = element i of lane.
 * dst layout: dst[w*4 + lane] = word w of lane's packed output.
 */
static void pack4(const uint32_t *src, uint32_t *dst, unsigned b)
{
    const uint64_t mask = (b == 32) ? 0xFFFFFFFFULL : ((1ULL << b) - 1ULL);
    uint64_t acc[4] = {0, 0, 0, 0};
    unsigned bits_in_acc = 0;
    unsigned out_idx = 0; /* output word group index (each group = 4 words) */

    for (unsigned i = 0; i < 32; i++) {
        /* Load one value per lane. */
        uint64_t v0 = src[i * 4 + 0] & mask;
        uint64_t v1 = src[i * 4 + 1] & mask;
        uint64_t v2 = src[i * 4 + 2] & mask;
        uint64_t v3 = src[i * 4 + 3] & mask;

        /* Accumulate: OR into position bits_in_acc. */
        acc[0] |= v0 << bits_in_acc;
        acc[1] |= v1 << bits_in_acc;
        acc[2] |= v2 << bits_in_acc;
        acc[3] |= v3 << bits_in_acc;
        bits_in_acc += b;

        /* Flush completed 32-bit words. */
        while (bits_in_acc >= 32) {
            /* Write low 32 bits of each accumulator. */
            dst[out_idx * 4 + 0] = (uint32_t)(acc[0]);
            dst[out_idx * 4 + 1] = (uint32_t)(acc[1]);
            dst[out_idx * 4 + 2] = (uint32_t)(acc[2]);
            dst[out_idx * 4 + 3] = (uint32_t)(acc[3]);
            out_idx++;
            /* Shift out the consumed bits. */
            acc[0] >>= 32;
            acc[1] >>= 32;
            acc[2] >>= 32;
            acc[3] >>= 32;
            bits_in_acc -= 32;
        }
    }

    /* Flush any remaining partial word. */
    if (bits_in_acc > 0) {
        dst[out_idx * 4 + 0] = (uint32_t)(acc[0]);
        dst[out_idx * 4 + 1] = (uint32_t)(acc[1]);
        dst[out_idx * 4 + 2] = (uint32_t)(acc[2]);
        dst[out_idx * 4 + 3] = (uint32_t)(acc[3]);
    }
}

/*
 * unpack4: Unpack 4 interleaved lanes from src[0..b*4-1] into dst[0..127].
 * src layout: src[w*4 + lane] = word w of lane's packed input.
 * dst layout: dst[i*4 + lane] = element i of lane.
 */
static void unpack4(const uint32_t *src, uint32_t *dst, unsigned b)
{
    const uint64_t mask = (b == 32) ? 0xFFFFFFFFULL : ((1ULL << b) - 1ULL);
    uint64_t acc[4] = {0, 0, 0, 0};
    unsigned bits_in_acc = 0;
    unsigned in_idx = 0; /* input word group index */

    for (unsigned i = 0; i < 32; i++) {
        /* Refill until we have at least b bits. */
        while (bits_in_acc < b) {
            /* Load next word group: 4 words, one per lane. */
            acc[0] |= (uint64_t)src[in_idx * 4 + 0] << bits_in_acc;
            acc[1] |= (uint64_t)src[in_idx * 4 + 1] << bits_in_acc;
            acc[2] |= (uint64_t)src[in_idx * 4 + 2] << bits_in_acc;
            acc[3] |= (uint64_t)src[in_idx * 4 + 3] << bits_in_acc;
            in_idx++;
            bits_in_acc += 32;
        }

        /* Extract b bits per lane. */
        dst[i * 4 + 0] = (uint32_t)(acc[0] & mask);
        dst[i * 4 + 1] = (uint32_t)(acc[1] & mask);
        dst[i * 4 + 2] = (uint32_t)(acc[2] & mask);
        dst[i * 4 + 3] = (uint32_t)(acc[3] & mask);

        /* Consume b bits. */
        acc[0] >>= b;
        acc[1] >>= b;
        acc[2] >>= b;
        acc[3] >>= b;
        bits_in_acc -= b;
    }
}

/* =========================================================================
 * NEON-optimized pack/unpack using uint32x4_t for load/store throughput.
 * The accumulator arithmetic still uses uint64_t arrays (4 elements).
 * The NEON benefit here is in the interleaved vld1q/vst1q instructions.
 * =========================================================================*/

#ifdef TSDB_SIMD_NEON

static void pack4_neon(const uint32_t *src, uint32_t *dst, unsigned b)
{
    const uint64_t mask = (b == 32) ? 0xFFFFFFFFULL : ((1ULL << b) - 1ULL);
    uint64_t acc[4] = {0, 0, 0, 0};
    unsigned bits_in_acc = 0;
    unsigned out_idx = 0;

    for (unsigned i = 0; i < 32; i++) {
        /* NEON load: 4 lanes simultaneously. */
        uint32x4_t val4 = vld1q_u32(src + i * 4);

        uint64_t v0 = vgetq_lane_u32(val4, 0) & mask;
        uint64_t v1 = vgetq_lane_u32(val4, 1) & mask;
        uint64_t v2 = vgetq_lane_u32(val4, 2) & mask;
        uint64_t v3 = vgetq_lane_u32(val4, 3) & mask;

        acc[0] |= v0 << bits_in_acc;
        acc[1] |= v1 << bits_in_acc;
        acc[2] |= v2 << bits_in_acc;
        acc[3] |= v3 << bits_in_acc;
        bits_in_acc += b;

        while (bits_in_acc >= 32) {
            /* NEON store: 4 lanes simultaneously. */
            uint32x4_t out4 = {
                (uint32_t)(acc[0]),
                (uint32_t)(acc[1]),
                (uint32_t)(acc[2]),
                (uint32_t)(acc[3])
            };
            vst1q_u32(dst + out_idx * 4, out4);
            out_idx++;
            acc[0] >>= 32;
            acc[1] >>= 32;
            acc[2] >>= 32;
            acc[3] >>= 32;
            bits_in_acc -= 32;
        }
    }

    if (bits_in_acc > 0) {
        uint32x4_t out4 = {
            (uint32_t)(acc[0]),
            (uint32_t)(acc[1]),
            (uint32_t)(acc[2]),
            (uint32_t)(acc[3])
        };
        vst1q_u32(dst + out_idx * 4, out4);
    }
}

static void unpack4_neon(const uint32_t *src, uint32_t *dst, unsigned b)
{
    const uint64_t mask = (b == 32) ? 0xFFFFFFFFULL : ((1ULL << b) - 1ULL);
    uint64_t acc[4] = {0, 0, 0, 0};
    unsigned bits_in_acc = 0;
    unsigned in_idx = 0;

    for (unsigned i = 0; i < 32; i++) {
        while (bits_in_acc < b) {
            /* NEON load: 4 lanes simultaneously. */
            uint32x4_t word4 = vld1q_u32(src + in_idx * 4);
            acc[0] |= (uint64_t)vgetq_lane_u32(word4, 0) << bits_in_acc;
            acc[1] |= (uint64_t)vgetq_lane_u32(word4, 1) << bits_in_acc;
            acc[2] |= (uint64_t)vgetq_lane_u32(word4, 2) << bits_in_acc;
            acc[3] |= (uint64_t)vgetq_lane_u32(word4, 3) << bits_in_acc;
            in_idx++;
            bits_in_acc += 32;
        }

        /* Extract b bits and store 4 lanes simultaneously. */
        uint32x4_t out4 = {
            (uint32_t)(acc[0] & mask),
            (uint32_t)(acc[1] & mask),
            (uint32_t)(acc[2] & mask),
            (uint32_t)(acc[3] & mask)
        };
        vst1q_u32(dst + i * 4, out4);

        acc[0] >>= b;
        acc[1] >>= b;
        acc[2] >>= b;
        acc[3] >>= b;
        bits_in_acc -= b;
    }
}

#endif /* TSDB_SIMD_NEON */

/* =========================================================================
 * Public API
 * =========================================================================*/

int tsdb_bp128_pack(const uint32_t *src, uint32_t *dst, unsigned b)
{
    if (!src || !dst) return TSDB_ERR_INVAL;
    if (b == 0 || b > 32) return TSDB_ERR_INVAL;

    /* Zero output: b*4 uint32 words per lane × 4 lanes = b*16 bytes. */
    memset(dst, 0, (size_t)b * 16);

#ifdef TSDB_SIMD_NEON
    pack4_neon(src, dst, b);
#else
    pack4(src, dst, b);
#endif

    return TSDB_OK;
}

int tsdb_bp128_unpack(const uint32_t *src, uint32_t *dst, unsigned b)
{
    if (!src || !dst) return TSDB_ERR_INVAL;
    if (b == 0 || b > 32) return TSDB_ERR_INVAL;

#ifdef TSDB_SIMD_NEON
    unpack4_neon(src, dst, b);
#else
    unpack4(src, dst, b);
#endif

    return TSDB_OK;
}
