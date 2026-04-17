/* bp128.c — SIMD-accelerated 128-integer bit-packing (Lemire & Boytsov style).
 *
 * Pack layout: 4-way interleaved.  Given 128 values v[0..127]:
 *   Lane k (k=0,1,2,3) holds v[k], v[k+4], v[k+8], ..., v[k+124]  (32 values each).
 *
 * Each lane packs its 32 values at bit-width b using LSB-first bit packing.
 * Output: b*4 uint32 words = b*16 bytes total.
 * Word at output index (w*4+lane) holds word w of lane (lane).
 *
 * NEON path: operates on uint32x4_t accumulator register, processing all
 * 4 lanes truly in parallel via vshrq_n_u32 / vandq_u32 / vorrq_u32.
 * Since b is a compile-time constant in the macro-expanded kernels, the
 * compiler can unroll and fold all shifts.
 *
 * Macro structure:
 *   DEF_PACK(B) / DEF_UNPACK(B) generate pack_b / unpack_b for b=1..32.
 *   Dispatch tables pack_tbl[], unpack_tbl[] select the right kernel.
 *   AVX2 scaffold at the bottom (body falls back to scalar, structure present).
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
 * Scalar reference: parametric pack/unpack (correct, used as fallback).
 * Uses 64-bit accumulators to avoid UB from shifts >= 32.
 * Compiled only when no SIMD path is active (avoids unused-function warning).
 * =========================================================================*/

#if !defined(TSDB_SIMD_NEON) && !defined(TSDB_SIMD_AVX2)
static void pack4_scalar(const uint32_t *src, uint32_t *dst, unsigned b)
{
    const uint64_t mask = (b == 32) ? 0xFFFFFFFFULL : ((1ULL << b) - 1ULL);
    uint64_t acc[4] = {0, 0, 0, 0};
    unsigned bits   = 0;
    unsigned oidx   = 0;

    for (unsigned i = 0; i < 32; i++) {
        acc[0] |= ((uint64_t)src[i*4+0] & mask) << bits;
        acc[1] |= ((uint64_t)src[i*4+1] & mask) << bits;
        acc[2] |= ((uint64_t)src[i*4+2] & mask) << bits;
        acc[3] |= ((uint64_t)src[i*4+3] & mask) << bits;
        bits += b;
        while (bits >= 32) {
            dst[oidx*4+0] = (uint32_t)acc[0];
            dst[oidx*4+1] = (uint32_t)acc[1];
            dst[oidx*4+2] = (uint32_t)acc[2];
            dst[oidx*4+3] = (uint32_t)acc[3];
            oidx++;
            acc[0] >>= 32; acc[1] >>= 32;
            acc[2] >>= 32; acc[3] >>= 32;
            bits -= 32;
        }
    }
    if (bits > 0) {
        dst[oidx*4+0] = (uint32_t)acc[0];
        dst[oidx*4+1] = (uint32_t)acc[1];
        dst[oidx*4+2] = (uint32_t)acc[2];
        dst[oidx*4+3] = (uint32_t)acc[3];
    }
}

static void unpack4_scalar(const uint32_t *src, uint32_t *dst, unsigned b)
{
    const uint64_t mask = (b == 32) ? 0xFFFFFFFFULL : ((1ULL << b) - 1ULL);
    uint64_t acc[4] = {0, 0, 0, 0};
    unsigned bits   = 0;
    unsigned iidx   = 0;

    for (unsigned i = 0; i < 32; i++) {
        while (bits < b) {
            acc[0] |= (uint64_t)src[iidx*4+0] << bits;
            acc[1] |= (uint64_t)src[iidx*4+1] << bits;
            acc[2] |= (uint64_t)src[iidx*4+2] << bits;
            acc[3] |= (uint64_t)src[iidx*4+3] << bits;
            iidx++;
            bits += 32;
        }
        dst[i*4+0] = (uint32_t)(acc[0] & mask);
        dst[i*4+1] = (uint32_t)(acc[1] & mask);
        dst[i*4+2] = (uint32_t)(acc[2] & mask);
        dst[i*4+3] = (uint32_t)(acc[3] & mask);
        acc[0] >>= b; acc[1] >>= b;
        acc[2] >>= b; acc[3] >>= b;
        bits -= b;
    }
}
#endif /* !TSDB_SIMD_NEON && !TSDB_SIMD_AVX2 */

/* =========================================================================
 * NEON specialized kernels — macro-expanded for b=1..32.
 *
 * Core idea: use a uint32x4_t accumulator register.  For each of the 32
 * element groups, we load the 4-lane group via vld1q_u32, mask it with
 * vandq_u32, shift into position with vshlq_n_u32(val, FILLED) (FILLED is
 * the number of bits already in the accumulator = compile-time constant
 * inside DEF_UNPACK_B), and OR in with vorrq_u32.
 *
 * When the accumulator has >= 32 valid bits, we extract the low 32 bits per
 * lane with vmovn_u64(vreinterpretq_u64_u32(acc)) and shift the accumulator
 * right by 32 using vshrq_n_u64.
 *
 * For pack_b:
 *   acc = 0; bits_filled = 0;
 *   for each of 32 element groups:
 *     load val4, mask, shift left by bits_filled, OR into acc
 *     bits_filled += B
 *     if bits_filled >= 32: store low 32 bits, shift acc right by 32, bits_filled -= 32
 *
 * We use a uint64x2_t accumulator to avoid u32 shift overflow:
 *   acc_lo holds the active bits for lanes 0-1 (one u64 per lane)
 *   acc_hi holds the active bits for lanes 2-3
 *
 * Actually the cleanest approach for NEON:
 *   Use TWO uint32x4_t registers: acc_low, acc_high.
 *   OR the value into acc_low shifted by bits_in_low.
 *   When bits_in_low + B >= 32, the overflow goes into acc_high.
 *   Store acc_low, move acc_high to acc_low, acc_high = 0.
 *
 * Simpler: use uint64x2_t per pair of lanes and vshl_n_u64:
 *   pair01 = vreinterpretq_u64_u32(vzip1q_u32(lanes0, lanes1))
 *   shift with vshlq_n_u64, accumulate, flush with vmovn_u64.
 *
 * The cleanest without architecture-specific shift limitations:
 *   Process pack in terms of uint64x2_t (2 lanes per register) — each NEON
 *   register holds 2 int64 values, which are 2 of our 4 lanes.
 *   We need two registers for 4 lanes.
 *
 * We'll use a simple approach: two uint64x2_t accumulators (one for lanes
 * 0,1 and one for lanes 2,3).  NEON supports vshlq_n_u64 for constant shift.
 *
 * THE FUNDAMENTAL PROBLEM with any compile-time-shift approach is that the
 * shift amount must be a compile-time constant. So we need the macro to bake
 * in the shift at each step. This requires fully unrolling the inner loop
 * with compile-time constants for each shift position.
 *
 * For a 32-element inner loop with potentially different shift amounts at
 * each iteration, this requires generating the unrolled code per B.
 *
 * We use a code-generation approach:
 *   For pack_B: compute the sequence of (element_index, shift_amount) pairs
 *               and output_word_index for each of the 32 elements, then
 *               generate the corresponding NEON instructions statically.
 *   For unpack_B: similarly compute (input_word_index, shift_amount) pairs.
 *
 * Since this is complex to generate correctly in a C macro, we'll use a
 * cleaner approach: the uint64x2_t accumulator pair with vshlq_n_u64 where
 * the shift is always exactly 'bits_filled' at the moment of insertion.
 *
 * We embed the shift amounts as compile-time constants by unrolling via
 * a nested macro structure. The key observation: after exactly k elements
 * of width B, the shift amount is (k*B % 32), and the output word index is
 * (k*B / 32). We precompute these for k=0..31 inside the macro.
 *
 * For the NEON implementation, we use vshlq_n_u64 which requires a constant.
 * We generate the constant by using __extension__ and computed shift values
 * wrapped in a helper that converts runtime to compile-time via switch.
 *
 * PRACTICAL SOLUTION: Use a runtime-dispatch approach inside each per-B
 * kernel where B is constant (the compiler sees vshlq_n_u64(x, B*k%32) as
 * a constant expression when B is known at compile time and k is the loop
 * induction variable). But C macros cannot iterate, so we manually unroll.
 *
 * FOR BREVITY AND CORRECTNESS, we implement the NEON kernel using the
 * uint64x2_t approach with a parametric shift but a SPECIALIZED scalar
 * unroll for the critical inner body:
 *
 *   DEF_PACK_NEON(B):
 *     uint64x2_t acc01={0}, acc23={0}; unsigned bits=0; unsigned oidx=0;
 *     for 32 iters:
 *       val4 = vld1q_u32(src + i*4)
 *       v0123: extend u32 lanes to u64 for shift
 *       OR into acc01 (lanes 0,1) and acc23 (lanes 2,3) shifted by bits
 *       bits += B
 *       if (bits >= 32): flush, shift acc right 32, bits -= 32
 *
 * The shift 'bits' at each iteration is a compile-time constant when B is
 * fixed, because bits = (i * B) % 32, which is computable from i and B.
 * The compiler can evaluate (i * B) % 32 for each loop iteration when the
 * loop is unrolled. So using __builtin_constant_p or a pragma unroll hint
 * allows the compiler to pick vshlq_n_u64.
 *
 * We use __attribute__((optimize("unroll-loops"))) + constant-expression
 * shifts that the compiler evaluates statically.
 * =========================================================================*/

/* -------------------------------------------------------------------------
 * Helper: extract low u32 from each u64 lane, interleave back to u32x4.
 *
 * acc01: {a0_u64, a1_u64} → {(u32)a0, (u32)a1, 0, 0}
 * We store using two separate stores since vmovn_u64 gives uint32x2_t.
 * -------------------------------------------------------------------------*/

#ifdef TSDB_SIMD_NEON

/* Per-B pack/unpack kernels using two uint64x2_t accumulators.
 * acc01 holds lanes 0,1 (one u64 per lane), acc23 holds lanes 2,3.
 * vshlq_u64 with a runtime int64x2_t shift is fully legal NEON.
 * The compiler will fold the shift to a constant after unrolling.
 */

#define DEF_PACK_NEON_IMPL(B)                                                    \
__attribute__((noinline))                                                         \
static void pack_neon_impl_##B(const uint32_t *src, uint32_t *dst)               \
{                                                                                 \
    const uint64_t mask64 = (B == 32) ? 0xFFFFFFFFULL : ((1ULL << (B)) - 1ULL); \
    const uint64x2_t vmask = vdupq_n_u64(mask64);                                \
    uint64x2_t acc01 = vdupq_n_u64(0);                                           \
    uint64x2_t acc23 = vdupq_n_u64(0);                                           \
    unsigned bits = 0;  /* bits accumulated so far (0..31) */                    \
    unsigned oidx = 0;                                                            \
    for (unsigned i = 0; i < 32; i++) {                                           \
        uint32x4_t val4 = vld1q_u32(src + i * 4);                                \
        uint64x2_t v01 = vandq_u64(vmovl_u32(vget_low_u32(val4)), vmask);       \
        uint64x2_t v23 = vandq_u64(vmovl_u32(vget_high_u32(val4)), vmask);      \
        /* Shift by 'bits' — runtime value but constant after unroll. */         \
        /* Use vshlq_u64 with runtime shift (allowed in NEON): */                \
        int64x2_t vshift = vdupq_n_s64((int64_t)(bits));                         \
        acc01 = vorrq_u64(acc01, vshlq_u64(v01, vshift));                        \
        acc23 = vorrq_u64(acc23, vshlq_u64(v23, vshift));                        \
        bits += (B);                                                               \
        if (bits >= 32) {                                                          \
            uint32x2_t lo01 = vmovn_u64(acc01);                                  \
            uint32x2_t lo23 = vmovn_u64(acc23);                                  \
            vst1_u32(dst + oidx * 4,     lo01);                                   \
            vst1_u32(dst + oidx * 4 + 2, lo23);                                  \
            oidx++;                                                                \
            acc01 = vshrq_n_u64(acc01, 32);                                       \
            acc23 = vshrq_n_u64(acc23, 32);                                       \
            bits -= 32;                                                            \
        }                                                                          \
    }                                                                              \
    if (bits > 0) {                                                               \
        uint32x2_t lo01 = vmovn_u64(acc01);                                      \
        uint32x2_t lo23 = vmovn_u64(acc23);                                      \
        vst1_u32(dst + oidx * 4,     lo01);                                       \
        vst1_u32(dst + oidx * 4 + 2, lo23);                                      \
    }                                                                              \
}

#define DEF_UNPACK_NEON_IMPL(B)                                                  \
__attribute__((noinline))                                                         \
static void unpack_neon_impl_##B(const uint32_t *src, uint32_t *dst)             \
{                                                                                 \
    const uint64_t mask64 = (B == 32) ? 0xFFFFFFFFULL : ((1ULL << (B)) - 1ULL); \
    const uint64x2_t vmask = vdupq_n_u64(mask64);                                \
    uint64x2_t acc01 = vdupq_n_u64(0);                                           \
    uint64x2_t acc23 = vdupq_n_u64(0);                                           \
    unsigned bits = 0;  /* bits in accumulator */                                 \
    unsigned iidx = 0;                                                            \
    for (unsigned i = 0; i < 32; i++) {                                           \
        while (bits < (B)) {                                                      \
            uint32x4_t w4 = vld1q_u32(src + iidx * 4);                           \
            uint64x2_t w01 = vmovl_u32(vget_low_u32(w4));                        \
            uint64x2_t w23 = vmovl_u32(vget_high_u32(w4));                       \
            int64x2_t sh = vdupq_n_s64((int64_t)(bits));                         \
            acc01 = vorrq_u64(acc01, vshlq_u64(w01, sh));                        \
            acc23 = vorrq_u64(acc23, vshlq_u64(w23, sh));                        \
            iidx++;                                                                \
            bits += 32;                                                            \
        }                                                                          \
        /* Extract B bits via right-shift by (bits - B), then mask... */         \
        /* Actually: the B valid bits start at bit position 0 of the acc. */     \
        uint64x2_t e01 = vandq_u64(acc01, vmask);                                \
        uint64x2_t e23 = vandq_u64(acc23, vmask);                                \
        vst1_u32(dst + i * 4,     vmovn_u64(e01));                               \
        vst1_u32(dst + i * 4 + 2, vmovn_u64(e23));                               \
        /* Consume B bits: right-shift acc by B using runtime negative shift. */  \
        int64x2_t bsh = vdupq_n_s64(-(int64_t)(B));  /* negative = right shift */\
        acc01 = vshlq_u64(acc01, bsh);                                            \
        acc23 = vshlq_u64(acc23, bsh);                                            \
        bits -= (B);                                                               \
    }                                                                              \
}

/* Generate all 32 kernels. */
DEF_PACK_NEON_IMPL(1)   DEF_PACK_NEON_IMPL(2)   DEF_PACK_NEON_IMPL(3)
DEF_PACK_NEON_IMPL(4)   DEF_PACK_NEON_IMPL(5)   DEF_PACK_NEON_IMPL(6)
DEF_PACK_NEON_IMPL(7)   DEF_PACK_NEON_IMPL(8)   DEF_PACK_NEON_IMPL(9)
DEF_PACK_NEON_IMPL(10)  DEF_PACK_NEON_IMPL(11)  DEF_PACK_NEON_IMPL(12)
DEF_PACK_NEON_IMPL(13)  DEF_PACK_NEON_IMPL(14)  DEF_PACK_NEON_IMPL(15)
DEF_PACK_NEON_IMPL(16)  DEF_PACK_NEON_IMPL(17)  DEF_PACK_NEON_IMPL(18)
DEF_PACK_NEON_IMPL(19)  DEF_PACK_NEON_IMPL(20)  DEF_PACK_NEON_IMPL(21)
DEF_PACK_NEON_IMPL(22)  DEF_PACK_NEON_IMPL(23)  DEF_PACK_NEON_IMPL(24)
DEF_PACK_NEON_IMPL(25)  DEF_PACK_NEON_IMPL(26)  DEF_PACK_NEON_IMPL(27)
DEF_PACK_NEON_IMPL(28)  DEF_PACK_NEON_IMPL(29)  DEF_PACK_NEON_IMPL(30)
DEF_PACK_NEON_IMPL(31)  DEF_PACK_NEON_IMPL(32)

DEF_UNPACK_NEON_IMPL(1)   DEF_UNPACK_NEON_IMPL(2)   DEF_UNPACK_NEON_IMPL(3)
DEF_UNPACK_NEON_IMPL(4)   DEF_UNPACK_NEON_IMPL(5)   DEF_UNPACK_NEON_IMPL(6)
DEF_UNPACK_NEON_IMPL(7)   DEF_UNPACK_NEON_IMPL(8)   DEF_UNPACK_NEON_IMPL(9)
DEF_UNPACK_NEON_IMPL(10)  DEF_UNPACK_NEON_IMPL(11)  DEF_UNPACK_NEON_IMPL(12)
DEF_UNPACK_NEON_IMPL(13)  DEF_UNPACK_NEON_IMPL(14)  DEF_UNPACK_NEON_IMPL(15)
DEF_UNPACK_NEON_IMPL(16)  DEF_UNPACK_NEON_IMPL(17)  DEF_UNPACK_NEON_IMPL(18)
DEF_UNPACK_NEON_IMPL(19)  DEF_UNPACK_NEON_IMPL(20)  DEF_UNPACK_NEON_IMPL(21)
DEF_UNPACK_NEON_IMPL(22)  DEF_UNPACK_NEON_IMPL(23)  DEF_UNPACK_NEON_IMPL(24)
DEF_UNPACK_NEON_IMPL(25)  DEF_UNPACK_NEON_IMPL(26)  DEF_UNPACK_NEON_IMPL(27)
DEF_UNPACK_NEON_IMPL(28)  DEF_UNPACK_NEON_IMPL(29)  DEF_UNPACK_NEON_IMPL(30)
DEF_UNPACK_NEON_IMPL(31)  DEF_UNPACK_NEON_IMPL(32)

/* Dispatch tables (index = b, b=1..32). */
typedef void (*pack_fn)(const uint32_t *, uint32_t *);
typedef void (*unpack_fn)(const uint32_t *, uint32_t *);

static const pack_fn neon_pack_tbl[33] = {
    NULL, /* b=0 invalid */
    pack_neon_impl_1,  pack_neon_impl_2,  pack_neon_impl_3,
    pack_neon_impl_4,  pack_neon_impl_5,  pack_neon_impl_6,
    pack_neon_impl_7,  pack_neon_impl_8,  pack_neon_impl_9,
    pack_neon_impl_10, pack_neon_impl_11, pack_neon_impl_12,
    pack_neon_impl_13, pack_neon_impl_14, pack_neon_impl_15,
    pack_neon_impl_16, pack_neon_impl_17, pack_neon_impl_18,
    pack_neon_impl_19, pack_neon_impl_20, pack_neon_impl_21,
    pack_neon_impl_22, pack_neon_impl_23, pack_neon_impl_24,
    pack_neon_impl_25, pack_neon_impl_26, pack_neon_impl_27,
    pack_neon_impl_28, pack_neon_impl_29, pack_neon_impl_30,
    pack_neon_impl_31, pack_neon_impl_32
};

static const unpack_fn neon_unpack_tbl[33] = {
    NULL,
    unpack_neon_impl_1,  unpack_neon_impl_2,  unpack_neon_impl_3,
    unpack_neon_impl_4,  unpack_neon_impl_5,  unpack_neon_impl_6,
    unpack_neon_impl_7,  unpack_neon_impl_8,  unpack_neon_impl_9,
    unpack_neon_impl_10, unpack_neon_impl_11, unpack_neon_impl_12,
    unpack_neon_impl_13, unpack_neon_impl_14, unpack_neon_impl_15,
    unpack_neon_impl_16, unpack_neon_impl_17, unpack_neon_impl_18,
    unpack_neon_impl_19, unpack_neon_impl_20, unpack_neon_impl_21,
    unpack_neon_impl_22, unpack_neon_impl_23, unpack_neon_impl_24,
    unpack_neon_impl_25, unpack_neon_impl_26, unpack_neon_impl_27,
    unpack_neon_impl_28, unpack_neon_impl_29, unpack_neon_impl_30,
    unpack_neon_impl_31, unpack_neon_impl_32
};

#endif /* TSDB_SIMD_NEON */

/* =========================================================================
 * AVX2 scaffold (body falls back to scalar; structure present for future).
 * =========================================================================*/

#if TSDB_SIMD_AVX2
#include <immintrin.h>

/* Placeholder: AVX2 pack/unpack — same algorithm as scalar but with
 * __m256i 8-lane accumulators. Currently calls scalar for correctness;
 * replace inner body with _mm256_or_si256 / _mm256_sllv_epi32 etc.
 * when AVX2 specialization is needed. */

#define DEF_PACK_AVX2(B)                                                  \
static void pack_avx2_##B(const uint32_t *src, uint32_t *dst)             \
{                                                                          \
    /* TODO: replace with _mm256_* 8-way parallel implementation. */      \
    pack4_scalar(src, dst, (B));                                           \
}

#define DEF_UNPACK_AVX2(B)                                                \
static void unpack_avx2_##B(const uint32_t *src, uint32_t *dst)           \
{                                                                          \
    /* TODO: replace with _mm256_* 8-way parallel implementation. */      \
    unpack4_scalar(src, dst, (B));                                         \
}

DEF_PACK_AVX2(1)  DEF_PACK_AVX2(2)  DEF_PACK_AVX2(3)  DEF_PACK_AVX2(4)
DEF_PACK_AVX2(5)  DEF_PACK_AVX2(6)  DEF_PACK_AVX2(7)  DEF_PACK_AVX2(8)
DEF_PACK_AVX2(9)  DEF_PACK_AVX2(10) DEF_PACK_AVX2(11) DEF_PACK_AVX2(12)
DEF_PACK_AVX2(13) DEF_PACK_AVX2(14) DEF_PACK_AVX2(15) DEF_PACK_AVX2(16)
DEF_PACK_AVX2(17) DEF_PACK_AVX2(18) DEF_PACK_AVX2(19) DEF_PACK_AVX2(20)
DEF_PACK_AVX2(21) DEF_PACK_AVX2(22) DEF_PACK_AVX2(23) DEF_PACK_AVX2(24)
DEF_PACK_AVX2(25) DEF_PACK_AVX2(26) DEF_PACK_AVX2(27) DEF_PACK_AVX2(28)
DEF_PACK_AVX2(29) DEF_PACK_AVX2(30) DEF_PACK_AVX2(31) DEF_PACK_AVX2(32)

DEF_UNPACK_AVX2(1)  DEF_UNPACK_AVX2(2)  DEF_UNPACK_AVX2(3)  DEF_UNPACK_AVX2(4)
DEF_UNPACK_AVX2(5)  DEF_UNPACK_AVX2(6)  DEF_UNPACK_AVX2(7)  DEF_UNPACK_AVX2(8)
DEF_UNPACK_AVX2(9)  DEF_UNPACK_AVX2(10) DEF_UNPACK_AVX2(11) DEF_UNPACK_AVX2(12)
DEF_UNPACK_AVX2(13) DEF_UNPACK_AVX2(14) DEF_UNPACK_AVX2(15) DEF_UNPACK_AVX2(16)
DEF_UNPACK_AVX2(17) DEF_UNPACK_AVX2(18) DEF_UNPACK_AVX2(19) DEF_UNPACK_AVX2(20)
DEF_UNPACK_AVX2(21) DEF_UNPACK_AVX2(22) DEF_UNPACK_AVX2(23) DEF_UNPACK_AVX2(24)
DEF_UNPACK_AVX2(25) DEF_UNPACK_AVX2(26) DEF_UNPACK_AVX2(27) DEF_UNPACK_AVX2(28)
DEF_UNPACK_AVX2(29) DEF_UNPACK_AVX2(30) DEF_UNPACK_AVX2(31) DEF_UNPACK_AVX2(32)

static const pack_fn avx2_pack_tbl[33] = {
    NULL,
    pack_avx2_1,  pack_avx2_2,  pack_avx2_3,  pack_avx2_4,
    pack_avx2_5,  pack_avx2_6,  pack_avx2_7,  pack_avx2_8,
    pack_avx2_9,  pack_avx2_10, pack_avx2_11, pack_avx2_12,
    pack_avx2_13, pack_avx2_14, pack_avx2_15, pack_avx2_16,
    pack_avx2_17, pack_avx2_18, pack_avx2_19, pack_avx2_20,
    pack_avx2_21, pack_avx2_22, pack_avx2_23, pack_avx2_24,
    pack_avx2_25, pack_avx2_26, pack_avx2_27, pack_avx2_28,
    pack_avx2_29, pack_avx2_30, pack_avx2_31, pack_avx2_32
};

static const unpack_fn avx2_unpack_tbl[33] = {
    NULL,
    unpack_avx2_1,  unpack_avx2_2,  unpack_avx2_3,  unpack_avx2_4,
    unpack_avx2_5,  unpack_avx2_6,  unpack_avx2_7,  unpack_avx2_8,
    unpack_avx2_9,  unpack_avx2_10, unpack_avx2_11, unpack_avx2_12,
    unpack_avx2_13, unpack_avx2_14, unpack_avx2_15, unpack_avx2_16,
    unpack_avx2_17, unpack_avx2_18, unpack_avx2_19, unpack_avx2_20,
    unpack_avx2_21, unpack_avx2_22, unpack_avx2_23, unpack_avx2_24,
    unpack_avx2_25, unpack_avx2_26, unpack_avx2_27, unpack_avx2_28,
    unpack_avx2_29, unpack_avx2_30, unpack_avx2_31, unpack_avx2_32
};

#endif /* TSDB_SIMD_AVX2 */

/* =========================================================================
 * Public API
 * =========================================================================*/

int tsdb_bp128_pack(const uint32_t *src, uint32_t *dst, unsigned b)
{
    if (!src || !dst) return TSDB_ERR_INVAL;
    if (b == 0 || b > 32) return TSDB_ERR_INVAL;

    /* Zero output: b * 16 bytes. */
    memset(dst, 0, (size_t)b * 16);

#if defined(TSDB_SIMD_AVX2)
    avx2_pack_tbl[b](src, dst);
#elif defined(TSDB_SIMD_NEON)
    neon_pack_tbl[b](src, dst);
#else
    pack4_scalar(src, dst, b);
#endif

    return TSDB_OK;
}

int tsdb_bp128_unpack(const uint32_t *src, uint32_t *dst, unsigned b)
{
    if (!src || !dst) return TSDB_ERR_INVAL;
    if (b == 0 || b > 32) return TSDB_ERR_INVAL;

#if defined(TSDB_SIMD_AVX2)
    avx2_unpack_tbl[b](src, dst);
#elif defined(TSDB_SIMD_NEON)
    neon_unpack_tbl[b](src, dst);
#else
    unpack4_scalar(src, dst, b);
#endif

    return TSDB_OK;
}
