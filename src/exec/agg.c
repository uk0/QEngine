/* agg.c — Column aggregate SIMD primitives with runtime CPU dispatch.
 *
 * Architecture layout:
 *   - Scalar kernels (always compiled, any arch)
 *   - NEON  kernels  (aarch64 / ARM_NEON only)
 *   - AVX2  kernels  (x86-64 only)
 *   - AVX-512 kernels (x86-64, per-function target attribute)
 *
 * Runtime dispatch:
 *   tsdb_cpu_level() is consulted once via pthread_once.
 *   Subsequent calls go directly through cached function pointers.
 *
 * Null-bitmap convention (MSB-first byte array):
 *   Bit i = byte[i>>3] bit (7-(i&7)).  NULL if 0, valid if 1.
 *   SIMD fast-paths handle null==NULL only; null-aware path stays scalar.
 *
 * Prefetch: __builtin_prefetch 512 doubles ahead (~4 KB) on long arrays.
 */
#include "agg.h"
#include "cpuid.h"
#include "simd.h"

#include <stddef.h>
#include <stdint.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <string.h>
#include <pthread.h>

/* Prefetch stride: 4096 bytes / 8 bytes per double = 512 doubles ahead */
#define PF_STRIDE 512

/* -----------------------------------------------------------------------
 * Null-bitmap helper
 * --------------------------------------------------------------------- */
static inline int valid(const uint8_t *bm, size_t i) {
    if (!bm) return 1;
    return TSDB_NULL_VALID(bm, i);
}

/* Count of valid (1) bits in a null bitmap covering n elements.  Used by
 * the public sum API to return both sum + valid count without forcing
 * the SIMD kernels to track count themselves. */
static uint64_t popcount_bitmap(const uint8_t *bm, size_t n) {
    uint64_t c = 0;
    size_t full_bytes = n >> 3;
    for (size_t b = 0; b < full_bytes; b++)
        c += (uint64_t)__builtin_popcount(bm[b]);
    for (size_t i = full_bytes << 3; i < n; i++)
        if (TSDB_NULL_VALID(bm, i)) c++;
    return c;
}

/* -----------------------------------------------------------------------
 * === SCALAR KERNELS ===
 * Always compiled; used as fallback and for null-aware paths.
 * --------------------------------------------------------------------- */

static double sum_f64_scalar(const double *v, size_t n,
                              const uint8_t *bm) {
    double s = 0.0;
    for (size_t i = 0; i < n; i++) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        if (valid(bm, i)) s += v[i];
    }
    return s;
}

/* Variant that also fills *cnt (for the sum + count API) */
static double sum_f64_scalar_cnt(const double *v, size_t n,
                                  const uint8_t *bm, uint64_t *cnt) {
    double s = 0.0; *cnt = 0;
    for (size_t i = 0; i < n; i++) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        if (valid(bm, i)) { s += v[i]; (*cnt)++; }
    }
    return s;
}

static double min_f64_scalar(const double *v, size_t n, const uint8_t *bm) {
    double m = (double)INFINITY;
    for (size_t i = 0; i < n; i++)
        if (valid(bm, i) && v[i] < m) m = v[i];
    return m;
}

static double max_f64_scalar(const double *v, size_t n, const uint8_t *bm) {
    double m = -(double)INFINITY;
    for (size_t i = 0; i < n; i++)
        if (valid(bm, i) && v[i] > m) m = v[i];
    return m;
}

static int64_t sum_i64_scalar(const int64_t *v, size_t n,
                               const uint8_t *bm, uint64_t *cnt) {
    int64_t s = 0; *cnt = 0;
    for (size_t i = 0; i < n; i++)
        if (valid(bm, i)) { s += v[i]; (*cnt)++; }
    return s;
}

static int64_t min_i64_scalar(const int64_t *v, size_t n, const uint8_t *bm) {
    int64_t m = INT64_MAX;
    for (size_t i = 0; i < n; i++)
        if (valid(bm, i) && v[i] < m) m = v[i];
    return m;
}

static int64_t max_i64_scalar(const int64_t *v, size_t n, const uint8_t *bm) {
    int64_t m = INT64_MIN;
    for (size_t i = 0; i < n; i++)
        if (valid(bm, i) && v[i] > m) m = v[i];
    return m;
}

/* -----------------------------------------------------------------------
 * === NEON KERNELS ===  (AArch64 only)
 * --------------------------------------------------------------------- */
#if defined(__aarch64__) || defined(__ARM_NEON)
#include <arm_neon.h>

/* MSB-first lane-mask helper.  `two_bits` carries 2 mask bits in MSB-first
 * order: high bit → lane 0, low bit → lane 1.  Returns a uint64x2 where
 * each lane is all-ones (valid) or all-zeros (null), so vbslq_f64 can
 * blend the original value against an identity element. */
static inline uint64x2_t neon_pair_mask(uint8_t two_bits) {
    /* Branchless: shift the bit into position 63 so signed >> sign-extends
     * to all-ones for set bits and all-zeros otherwise.  Using arithmetic
     * shift on int64 gives the lane mask without a select. */
    uint64_t arr[2];
    arr[0] = (uint64_t)(((int64_t)((uint64_t)two_bits << 62)) >> 63);
    arr[1] = (uint64_t)(((int64_t)((uint64_t)two_bits << 63)) >> 63);
    return vld1q_u64(arr);
}

/* 4 × float64x2 accumulators = 8 lanes / iter, breaks 4 dep chains */
static double sum_f64_neon(const double *v, size_t n, const uint8_t *bm) {
    float64x2_t a0 = vdupq_n_f64(0.0);
    float64x2_t a1 = vdupq_n_f64(0.0);
    float64x2_t a2 = vdupq_n_f64(0.0);
    float64x2_t a3 = vdupq_n_f64(0.0);
    size_t i = 0;
    if (!bm) {
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = vaddq_f64(a0, vld1q_f64(v + i + 0));
            a1 = vaddq_f64(a1, vld1q_f64(v + i + 2));
            a2 = vaddq_f64(a2, vld1q_f64(v + i + 4));
            a3 = vaddq_f64(a3, vld1q_f64(v + i + 6));
        }
        for (; i + 1 < n; i += 2)
            a0 = vaddq_f64(a0, vld1q_f64(v + i));
    } else {
        const float64x2_t zero = vdupq_n_f64(0.0);
        /* Process 1 bitmap byte per 8 elements; mask null lanes to 0 so
         * the SIMD add stays branchless. */
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            float64x2_t v0 = vld1q_f64(v + i + 0);
            float64x2_t v1 = vld1q_f64(v + i + 2);
            float64x2_t v2 = vld1q_f64(v + i + 4);
            float64x2_t v3 = vld1q_f64(v + i + 6);
            v0 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 6) & 0x3u)), v0, zero);
            v1 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 4) & 0x3u)), v1, zero);
            v2 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 2) & 0x3u)), v2, zero);
            v3 = vbslq_f64(neon_pair_mask((uint8_t)((b      ) & 0x3u)), v3, zero);
            a0 = vaddq_f64(a0, v0);
            a1 = vaddq_f64(a1, v1);
            a2 = vaddq_f64(a2, v2);
            a3 = vaddq_f64(a3, v3);
        }
    }
    a0 = vaddq_f64(vaddq_f64(a0, a1), vaddq_f64(a2, a3));
    double s = vaddvq_f64(a0);
    /* Tail (1..7 elements that didn't complete a byte). */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) s += v[i];
    }
    return s;
}

static double min_f64_neon(const double *v, size_t n, const uint8_t *bm) {
    if (n == 0) return (double)INFINITY;

    float64x2_t mn = vdupq_n_f64((double)INFINITY);
    size_t i = 0;
    if (!bm) {
        /* Four accumulators, same as sum_f64_neon.  A single accumulator
         * makes this loop latency-bound on the min dependency chain rather
         * than throughput-bound on the loads; measured 3-10x slower than
         * sum on identical resident data. */
        float64x2_t a0 = mn, a1 = mn, a2 = mn, a3 = mn;
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = vminq_f64(a0, vld1q_f64(v + i + 0));
            a1 = vminq_f64(a1, vld1q_f64(v + i + 2));
            a2 = vminq_f64(a2, vld1q_f64(v + i + 4));
            a3 = vminq_f64(a3, vld1q_f64(v + i + 6));
        }
        mn = vminq_f64(vminq_f64(a0, a1), vminq_f64(a2, a3));
        for (; i + 1 < n; i += 2)
            mn = vminq_f64(mn, vld1q_f64(v + i));
    } else {
        const float64x2_t inf = vdupq_n_f64((double)INFINITY);
        /* Replace null lanes with +INF so vminq_f64 is a no-op for them. */
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            float64x2_t v0 = vld1q_f64(v + i + 0);
            float64x2_t v1 = vld1q_f64(v + i + 2);
            float64x2_t v2 = vld1q_f64(v + i + 4);
            float64x2_t v3 = vld1q_f64(v + i + 6);
            v0 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 6) & 0x3u)), v0, inf);
            v1 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 4) & 0x3u)), v1, inf);
            v2 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 2) & 0x3u)), v2, inf);
            v3 = vbslq_f64(neon_pair_mask((uint8_t)((b      ) & 0x3u)), v3, inf);
            mn = vminq_f64(mn, vminq_f64(vminq_f64(v0, v1), vminq_f64(v2, v3)));
        }
    }
    double m = fmin(vgetq_lane_f64(mn, 0), vgetq_lane_f64(mn, 1));
    /* Tail */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) {
            if (v[i] < m) m = v[i];
        }
    }
    return m;
}

static double max_f64_neon(const double *v, size_t n, const uint8_t *bm) {
    if (n == 0) return -(double)INFINITY;

    float64x2_t mx = vdupq_n_f64(-(double)INFINITY);
    size_t i = 0;
    if (!bm) {
        float64x2_t a0 = mx, a1 = mx, a2 = mx, a3 = mx;
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = vmaxq_f64(a0, vld1q_f64(v + i + 0));
            a1 = vmaxq_f64(a1, vld1q_f64(v + i + 2));
            a2 = vmaxq_f64(a2, vld1q_f64(v + i + 4));
            a3 = vmaxq_f64(a3, vld1q_f64(v + i + 6));
        }
        mx = vmaxq_f64(vmaxq_f64(a0, a1), vmaxq_f64(a2, a3));
        for (; i + 1 < n; i += 2)
            mx = vmaxq_f64(mx, vld1q_f64(v + i));
    } else {
        const float64x2_t neg_inf = vdupq_n_f64(-(double)INFINITY);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            float64x2_t v0 = vld1q_f64(v + i + 0);
            float64x2_t v1 = vld1q_f64(v + i + 2);
            float64x2_t v2 = vld1q_f64(v + i + 4);
            float64x2_t v3 = vld1q_f64(v + i + 6);
            v0 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 6) & 0x3u)), v0, neg_inf);
            v1 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 4) & 0x3u)), v1, neg_inf);
            v2 = vbslq_f64(neon_pair_mask((uint8_t)((b >> 2) & 0x3u)), v2, neg_inf);
            v3 = vbslq_f64(neon_pair_mask((uint8_t)((b      ) & 0x3u)), v3, neg_inf);
            mx = vmaxq_f64(mx, vmaxq_f64(vmaxq_f64(v0, v1), vmaxq_f64(v2, v3)));
        }
    }
    double m = fmax(vgetq_lane_f64(mx, 0), vgetq_lane_f64(mx, 1));
    /* Tail */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) {
            if (v[i] > m) m = v[i];
        }
    }
    return m;
}

/* 4 × int64x2 accumulators */
static int64_t sum_i64_neon(const int64_t *v, size_t n,
                             const uint8_t *bm, uint64_t *cnt) {
    int64x2_t a0 = vdupq_n_s64(0);
    int64x2_t a1 = vdupq_n_s64(0);
    int64x2_t a2 = vdupq_n_s64(0);
    int64x2_t a3 = vdupq_n_s64(0);
    size_t i = 0;
    uint64_t valid_count = 0;
    if (!bm) {
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = vaddq_s64(a0, vld1q_s64(v + i + 0));
            a1 = vaddq_s64(a1, vld1q_s64(v + i + 2));
            a2 = vaddq_s64(a2, vld1q_s64(v + i + 4));
            a3 = vaddq_s64(a3, vld1q_s64(v + i + 6));
        }
        for (; i + 1 < n; i += 2)
            a0 = vaddq_s64(a0, vld1q_s64(v + i));
        valid_count = (uint64_t)i;
    } else {
        const int64x2_t zero = vdupq_n_s64(0);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            int64x2_t v0 = vld1q_s64(v + i + 0);
            int64x2_t v1 = vld1q_s64(v + i + 2);
            int64x2_t v2 = vld1q_s64(v + i + 4);
            int64x2_t v3 = vld1q_s64(v + i + 6);
            v0 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 6) & 0x3u)), v0, zero);
            v1 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 4) & 0x3u)), v1, zero);
            v2 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 2) & 0x3u)), v2, zero);
            v3 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b      ) & 0x3u)), v3, zero);
            a0 = vaddq_s64(a0, v0);
            a1 = vaddq_s64(a1, v1);
            a2 = vaddq_s64(a2, v2);
            a3 = vaddq_s64(a3, v3);
            valid_count += (uint64_t)__builtin_popcount(b);
        }
    }
    a0 = vaddq_s64(vaddq_s64(a0, a1), vaddq_s64(a2, a3));
    int64_t s = vgetq_lane_s64(a0, 0) + vgetq_lane_s64(a0, 1);
    /* Tail */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) { s += v[i]; valid_count++; }
    }
    *cnt = valid_count;
    return s;
}

/* ARMv8 NEON has no SMIN.D / SMAX.D for 64-bit signed lanes (SMIN tops out
 * at 32-bit), so emulate with compare-and-select.  Same shape as the
 * AVX2 i64 min/max emulation below. */
#if defined(__aarch64__)
static inline int64x2_t min_s64x2(int64x2_t a, int64x2_t b) {
    /* mask all-ones where a < b → select a, else b */
    uint64x2_t lt = vcltq_s64(a, b);
    return vbslq_s64(lt, a, b);
}
static inline int64x2_t max_s64x2(int64x2_t a, int64x2_t b) {
    uint64x2_t gt = vcgtq_s64(a, b);
    return vbslq_s64(gt, a, b);
}

static int64_t min_i64_neon(const int64_t *v, size_t n, const uint8_t *bm) {
    if (n == 0) return INT64_MAX;
    int64x2_t mn = vdupq_n_s64(INT64_MAX);
    size_t i = 0;
    if (!bm) {
        int64x2_t a0 = mn, a1 = mn, a2 = mn, a3 = mn;
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = min_s64x2(a0, vld1q_s64(v + i + 0));
            a1 = min_s64x2(a1, vld1q_s64(v + i + 2));
            a2 = min_s64x2(a2, vld1q_s64(v + i + 4));
            a3 = min_s64x2(a3, vld1q_s64(v + i + 6));
        }
        mn = min_s64x2(min_s64x2(a0, a1), min_s64x2(a2, a3));
        for (; i + 1 < n; i += 2)
            mn = min_s64x2(mn, vld1q_s64(v + i));
    } else {
        const int64x2_t imax = vdupq_n_s64(INT64_MAX);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            int64x2_t v0 = vld1q_s64(v + i + 0);
            int64x2_t v1 = vld1q_s64(v + i + 2);
            int64x2_t v2 = vld1q_s64(v + i + 4);
            int64x2_t v3 = vld1q_s64(v + i + 6);
            v0 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 6) & 0x3u)), v0, imax);
            v1 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 4) & 0x3u)), v1, imax);
            v2 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 2) & 0x3u)), v2, imax);
            v3 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b      ) & 0x3u)), v3, imax);
            mn = min_s64x2(mn, min_s64x2(min_s64x2(v0, v1), min_s64x2(v2, v3)));
        }
    }
    int64_t m  = vgetq_lane_s64(mn, 0);
    int64_t hi = vgetq_lane_s64(mn, 1);
    if (hi < m) m = hi;
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] < m) m = v[i];
    }
    return m;
}

static int64_t max_i64_neon(const int64_t *v, size_t n, const uint8_t *bm) {
    if (n == 0) return INT64_MIN;
    int64x2_t mx = vdupq_n_s64(INT64_MIN);
    size_t i = 0;
    if (!bm) {
        int64x2_t a0 = mx, a1 = mx, a2 = mx, a3 = mx;
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = max_s64x2(a0, vld1q_s64(v + i + 0));
            a1 = max_s64x2(a1, vld1q_s64(v + i + 2));
            a2 = max_s64x2(a2, vld1q_s64(v + i + 4));
            a3 = max_s64x2(a3, vld1q_s64(v + i + 6));
        }
        mx = max_s64x2(max_s64x2(a0, a1), max_s64x2(a2, a3));
        for (; i + 1 < n; i += 2)
            mx = max_s64x2(mx, vld1q_s64(v + i));
    } else {
        const int64x2_t imin = vdupq_n_s64(INT64_MIN);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            int64x2_t v0 = vld1q_s64(v + i + 0);
            int64x2_t v1 = vld1q_s64(v + i + 2);
            int64x2_t v2 = vld1q_s64(v + i + 4);
            int64x2_t v3 = vld1q_s64(v + i + 6);
            v0 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 6) & 0x3u)), v0, imin);
            v1 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 4) & 0x3u)), v1, imin);
            v2 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b >> 2) & 0x3u)), v2, imin);
            v3 = vbslq_s64((int64x2_t)neon_pair_mask((uint8_t)((b      ) & 0x3u)), v3, imin);
            mx = max_s64x2(mx, max_s64x2(max_s64x2(v0, v1), max_s64x2(v2, v3)));
        }
    }
    int64_t m  = vgetq_lane_s64(mx, 0);
    int64_t hi = vgetq_lane_s64(mx, 1);
    if (hi > m) m = hi;
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] > m) m = v[i];
    }
    return m;
}
#define HAS_NEON_I64_MINMAX 1
#endif /* __aarch64__ */

#define HAS_NEON_KERNELS 1
#endif /* NEON */

/* -----------------------------------------------------------------------
 * === AVX2 / AVX-512 KERNELS ===  (x86-64 only)
 * --------------------------------------------------------------------- */
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>

/* ---------- AVX2 ---------- */

/* 16-entry LUT: a 4-bit nibble (MSB-first, bit 3 = lane 0) maps to a 4-lane
 * uint64 mask where each lane is all-ones (valid) or zero (null).
 * Fits in two 64-byte cache lines (16 × 32 bytes = 512 B).
 *
 * Used by AVX2 null-aware aggregate paths to mask null lanes to the
 * aggregate identity element via _mm256_blendv_pd. */
__attribute__((aligned(64)))
static const uint64_t avx2_nibble_lut[16][4] = {
    /* nibble bits b3 b2 b1 b0 → lanes 0,1,2,3 */
    {       0,        0,        0,        0 }, /* 0000 */
    {       0,        0,        0, ~0ULL    }, /* 0001 */
    {       0,        0, ~0ULL,        0 }, /* 0010 */
    {       0,        0, ~0ULL, ~0ULL    }, /* 0011 */
    {       0, ~0ULL,        0,        0 }, /* 0100 */
    {       0, ~0ULL,        0, ~0ULL    }, /* 0101 */
    {       0, ~0ULL, ~0ULL,        0 }, /* 0110 */
    {       0, ~0ULL, ~0ULL, ~0ULL    }, /* 0111 */
    { ~0ULL,        0,        0,        0 }, /* 1000 */
    { ~0ULL,        0,        0, ~0ULL    }, /* 1001 */
    { ~0ULL,        0, ~0ULL,        0 }, /* 1010 */
    { ~0ULL,        0, ~0ULL, ~0ULL    }, /* 1011 */
    { ~0ULL, ~0ULL,        0,        0 }, /* 1100 */
    { ~0ULL, ~0ULL,        0, ~0ULL    }, /* 1101 */
    { ~0ULL, ~0ULL, ~0ULL,        0 }, /* 1110 */
    { ~0ULL, ~0ULL, ~0ULL, ~0ULL    }, /* 1111 */
};

__attribute__((target("avx2")))
static inline __m256d avx2_nibble_mask(uint8_t nibble) {
    return _mm256_castsi256_pd(
        _mm256_load_si256((const __m256i *)avx2_nibble_lut[nibble & 0xF]));
}

__attribute__((target("avx2")))
static inline double hsum256_pd(__m256d v) {
    __m128d lo  = _mm256_castpd256_pd128(v);
    __m128d hi  = _mm256_extractf128_pd(v, 1);
    __m128d sum = _mm_add_pd(lo, hi);
    __m128d tmp = _mm_unpackhi_pd(sum, sum);
    return _mm_cvtsd_f64(_mm_add_pd(sum, tmp));
}

__attribute__((target("avx2")))
static double sum_f64_avx2(const double *v, size_t n, const uint8_t *bm) {
    __m256d a0 = _mm256_setzero_pd();
    __m256d a1 = _mm256_setzero_pd();
    __m256d a2 = _mm256_setzero_pd();
    __m256d a3 = _mm256_setzero_pd();
    size_t i = 0;
    if (!bm) {
        /* 4 × __m256d = 16 doubles/iteration */
        for (; i + 15 < n; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm256_add_pd(a0, _mm256_loadu_pd(v + i +  0));
            a1 = _mm256_add_pd(a1, _mm256_loadu_pd(v + i +  4));
            a2 = _mm256_add_pd(a2, _mm256_loadu_pd(v + i +  8));
            a3 = _mm256_add_pd(a3, _mm256_loadu_pd(v + i + 12));
        }
        for (; i + 3 < n; i += 4)
            a0 = _mm256_add_pd(a0, _mm256_loadu_pd(v + i));
    } else {
        /* Null-aware: 1 byte → 8 elements (2 × __m256d).  Use the nibble
         * LUT to expand 4 bits into a 4-lane mask, then blend null lanes
         * against zero so the SIMD add stays branchless. */
        const __m256d zero = _mm256_setzero_pd();
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            __m256d v0 = _mm256_loadu_pd(v + i + 0);
            __m256d v1 = _mm256_loadu_pd(v + i + 4);
            v0 = _mm256_blendv_pd(zero, v0, avx2_nibble_mask((uint8_t)((b >> 4) & 0xFu)));
            v1 = _mm256_blendv_pd(zero, v1, avx2_nibble_mask((uint8_t)((b     ) & 0xFu)));
            a0 = _mm256_add_pd(a0, v0);
            a1 = _mm256_add_pd(a1, v1);
        }
    }
    a0 = _mm256_add_pd(_mm256_add_pd(a0, a1), _mm256_add_pd(a2, a3));
    double s = hsum256_pd(a0);
    /* Tail */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) s += v[i];
    }
    return s;
}

__attribute__((target("avx2")))
static double min_f64_avx2(const double *v, size_t n, const uint8_t *bm) {
    if (n == 0) return (double)INFINITY;

    __m256d mn = _mm256_set1_pd((double)INFINITY);
    size_t i = 0;
    if (!bm) {
        /* Four accumulators, same as sum_f64_avx2: _mm256_min_pd has 3-4
         * cycle latency and ~1/cycle throughput, so a single chained
         * accumulator runs at a quarter of the achievable rate. */
        __m256d a0 = mn, a1 = mn, a2 = mn, a3 = mn;
        for (; i + 15 < n; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm256_min_pd(a0, _mm256_loadu_pd(v + i +  0));
            a1 = _mm256_min_pd(a1, _mm256_loadu_pd(v + i +  4));
            a2 = _mm256_min_pd(a2, _mm256_loadu_pd(v + i +  8));
            a3 = _mm256_min_pd(a3, _mm256_loadu_pd(v + i + 12));
        }
        mn = _mm256_min_pd(_mm256_min_pd(a0, a1), _mm256_min_pd(a2, a3));
        for (; i + 3 < n; i += 4)
            mn = _mm256_min_pd(mn, _mm256_loadu_pd(v + i));
    } else {
        const __m256d inf = _mm256_set1_pd((double)INFINITY);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            __m256d v0 = _mm256_loadu_pd(v + i + 0);
            __m256d v1 = _mm256_loadu_pd(v + i + 4);
            v0 = _mm256_blendv_pd(inf, v0, avx2_nibble_mask((uint8_t)((b >> 4) & 0xFu)));
            v1 = _mm256_blendv_pd(inf, v1, avx2_nibble_mask((uint8_t)((b     ) & 0xFu)));
            mn = _mm256_min_pd(mn, _mm256_min_pd(v0, v1));
        }
    }
    __m128d lo = _mm256_castpd256_pd128(mn);
    __m128d hi = _mm256_extractf128_pd(mn, 1);
    lo = _mm_min_pd(lo, hi);
    lo = _mm_min_pd(lo, _mm_unpackhi_pd(lo, lo));
    double m = _mm_cvtsd_f64(lo);
    /* Tail */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) {
            if (v[i] < m) m = v[i];
        }
    }
    return m;
}

__attribute__((target("avx2")))
static double max_f64_avx2(const double *v, size_t n, const uint8_t *bm) {
    if (n == 0) return -(double)INFINITY;

    __m256d mx = _mm256_set1_pd(-(double)INFINITY);
    size_t i = 0;
    if (!bm) {
        __m256d a0 = mx, a1 = mx, a2 = mx, a3 = mx;
        for (; i + 15 < n; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm256_max_pd(a0, _mm256_loadu_pd(v + i +  0));
            a1 = _mm256_max_pd(a1, _mm256_loadu_pd(v + i +  4));
            a2 = _mm256_max_pd(a2, _mm256_loadu_pd(v + i +  8));
            a3 = _mm256_max_pd(a3, _mm256_loadu_pd(v + i + 12));
        }
        mx = _mm256_max_pd(_mm256_max_pd(a0, a1), _mm256_max_pd(a2, a3));
        for (; i + 3 < n; i += 4)
            mx = _mm256_max_pd(mx, _mm256_loadu_pd(v + i));
    } else {
        const __m256d neg_inf = _mm256_set1_pd(-(double)INFINITY);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            __m256d v0 = _mm256_loadu_pd(v + i + 0);
            __m256d v1 = _mm256_loadu_pd(v + i + 4);
            v0 = _mm256_blendv_pd(neg_inf, v0, avx2_nibble_mask((uint8_t)((b >> 4) & 0xFu)));
            v1 = _mm256_blendv_pd(neg_inf, v1, avx2_nibble_mask((uint8_t)((b     ) & 0xFu)));
            mx = _mm256_max_pd(mx, _mm256_max_pd(v0, v1));
        }
    }
    __m128d lo = _mm256_castpd256_pd128(mx);
    __m128d hi = _mm256_extractf128_pd(mx, 1);
    lo = _mm_max_pd(lo, hi);
    lo = _mm_max_pd(lo, _mm_unpackhi_pd(lo, lo));
    double m = _mm_cvtsd_f64(lo);
    /* Tail */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) {
            if (v[i] > m) m = v[i];
        }
    }
    return m;
}

__attribute__((target("avx2")))
static int64_t sum_i64_avx2(const int64_t *v, size_t n,
                              const uint8_t *bm, uint64_t *cnt) {
    __m256i a0 = _mm256_setzero_si256();
    __m256i a1 = _mm256_setzero_si256();
    __m256i a2 = _mm256_setzero_si256();
    __m256i a3 = _mm256_setzero_si256();
    size_t i = 0;
    uint64_t valid_count = 0;
    if (!bm) {
        for (; i + 15 < n; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm256_add_epi64(a0, _mm256_loadu_si256((const __m256i *)(v + i +  0)));
            a1 = _mm256_add_epi64(a1, _mm256_loadu_si256((const __m256i *)(v + i +  4)));
            a2 = _mm256_add_epi64(a2, _mm256_loadu_si256((const __m256i *)(v + i +  8)));
            a3 = _mm256_add_epi64(a3, _mm256_loadu_si256((const __m256i *)(v + i + 12)));
        }
        for (; i + 3 < n; i += 4)
            a0 = _mm256_add_epi64(a0, _mm256_loadu_si256((const __m256i *)(v + i)));
        valid_count = (uint64_t)i;
    } else {
        /* Null-aware: same nibble-LUT mask, but blend against zero in the
         * integer domain.  _mm256_blendv_epi8 selects byte-by-byte under
         * the high bit of each byte; the LUT entries are all-ones / all-
         * zeros so any byte's high bit reflects the lane's validity. */
        const __m256i zero = _mm256_setzero_si256();
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            __m256i v0 = _mm256_loadu_si256((const __m256i *)(v + i + 0));
            __m256i v1 = _mm256_loadu_si256((const __m256i *)(v + i + 4));
            __m256i m0 = _mm256_load_si256((const __m256i *)avx2_nibble_lut[(b >> 4) & 0xF]);
            __m256i m1 = _mm256_load_si256((const __m256i *)avx2_nibble_lut[(b     ) & 0xF]);
            v0 = _mm256_blendv_epi8(zero, v0, m0);
            v1 = _mm256_blendv_epi8(zero, v1, m1);
            a0 = _mm256_add_epi64(a0, v0);
            a1 = _mm256_add_epi64(a1, v1);
            valid_count += (uint64_t)__builtin_popcount(b);
        }
    }
    a0 = _mm256_add_epi64(_mm256_add_epi64(a0, a1), _mm256_add_epi64(a2, a3));
    __m128i lo128 = _mm256_castsi256_si128(a0);
    __m128i hi128 = _mm256_extracti128_si256(a0, 1);
    lo128 = _mm_add_epi64(lo128, hi128);
    int64_t s = (int64_t)(_mm_extract_epi64(lo128, 0) + _mm_extract_epi64(lo128, 1));
    /* Tail */
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) { s += v[i]; valid_count++; }
    }
    *cnt = valid_count;
    return s;
}

/* AVX2 lacks _mm256_min_epi64 / _mm256_max_epi64; emulate via cmpgt+blendv.
 * Same algebra as the NEON helper above, mapped to AVX2 intrinsics. */
__attribute__((target("avx2")))
static inline __m256i min_s64_avx2(__m256i a, __m256i b) {
    /* mask = (a > b) → all-ones lanes where a wins.  We want the smaller,
     * so where mask set keep b, else keep a. */
    __m256i gt = _mm256_cmpgt_epi64(a, b);
    return _mm256_blendv_epi8(a, b, gt);
}
__attribute__((target("avx2")))
static inline __m256i max_s64_avx2(__m256i a, __m256i b) {
    __m256i gt = _mm256_cmpgt_epi64(a, b);
    return _mm256_blendv_epi8(b, a, gt);
}

__attribute__((target("avx2")))
static int64_t min_i64_avx2(const int64_t *v, size_t n, const uint8_t *bm) {
    if (n == 0) return INT64_MAX;
    __m256i mn = _mm256_set1_epi64x(INT64_MAX);
    size_t i = 0;
    if (!bm) {
        /* min_s64_avx2 is cmpgt+blendv — a 2-op chain, so the single
         * accumulator costs even more here than in the f64 kernel. */
        __m256i a0 = mn, a1 = mn, a2 = mn, a3 = mn;
        for (; i + 15 < n; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = min_s64_avx2(a0, _mm256_loadu_si256((const __m256i *)(v + i +  0)));
            a1 = min_s64_avx2(a1, _mm256_loadu_si256((const __m256i *)(v + i +  4)));
            a2 = min_s64_avx2(a2, _mm256_loadu_si256((const __m256i *)(v + i +  8)));
            a3 = min_s64_avx2(a3, _mm256_loadu_si256((const __m256i *)(v + i + 12)));
        }
        mn = min_s64_avx2(min_s64_avx2(a0, a1), min_s64_avx2(a2, a3));
        for (; i + 3 < n; i += 4)
            mn = min_s64_avx2(mn, _mm256_loadu_si256((const __m256i *)(v + i)));
    } else {
        const __m256i imax = _mm256_set1_epi64x(INT64_MAX);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            __m256i v0 = _mm256_loadu_si256((const __m256i *)(v + i + 0));
            __m256i v1 = _mm256_loadu_si256((const __m256i *)(v + i + 4));
            __m256i m0 = _mm256_load_si256((const __m256i *)avx2_nibble_lut[(b >> 4) & 0xF]);
            __m256i m1 = _mm256_load_si256((const __m256i *)avx2_nibble_lut[(b     ) & 0xF]);
            v0 = _mm256_blendv_epi8(imax, v0, m0);
            v1 = _mm256_blendv_epi8(imax, v1, m1);
            mn = min_s64_avx2(mn, min_s64_avx2(v0, v1));
        }
    }
    /* Reduce 4 lanes to 1. */
    __m128i lo = _mm256_castsi256_si128(mn);
    __m128i hi = _mm256_extracti128_si256(mn, 1);
    int64_t a = _mm_extract_epi64(lo, 0);
    int64_t b = _mm_extract_epi64(lo, 1);
    int64_t c = _mm_extract_epi64(hi, 0);
    int64_t d = _mm_extract_epi64(hi, 1);
    int64_t m = a; if (b < m) m = b; if (c < m) m = c; if (d < m) m = d;
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] < m) m = v[i];
    }
    return m;
}

__attribute__((target("avx2")))
static int64_t max_i64_avx2(const int64_t *v, size_t n, const uint8_t *bm) {
    if (n == 0) return INT64_MIN;
    __m256i mx = _mm256_set1_epi64x(INT64_MIN);
    size_t i = 0;
    if (!bm) {
        __m256i a0 = mx, a1 = mx, a2 = mx, a3 = mx;
        for (; i + 15 < n; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = max_s64_avx2(a0, _mm256_loadu_si256((const __m256i *)(v + i +  0)));
            a1 = max_s64_avx2(a1, _mm256_loadu_si256((const __m256i *)(v + i +  4)));
            a2 = max_s64_avx2(a2, _mm256_loadu_si256((const __m256i *)(v + i +  8)));
            a3 = max_s64_avx2(a3, _mm256_loadu_si256((const __m256i *)(v + i + 12)));
        }
        mx = max_s64_avx2(max_s64_avx2(a0, a1), max_s64_avx2(a2, a3));
        for (; i + 3 < n; i += 4)
            mx = max_s64_avx2(mx, _mm256_loadu_si256((const __m256i *)(v + i)));
    } else {
        const __m256i imin = _mm256_set1_epi64x(INT64_MIN);
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            __m256i v0 = _mm256_loadu_si256((const __m256i *)(v + i + 0));
            __m256i v1 = _mm256_loadu_si256((const __m256i *)(v + i + 4));
            __m256i m0 = _mm256_load_si256((const __m256i *)avx2_nibble_lut[(b >> 4) & 0xF]);
            __m256i m1 = _mm256_load_si256((const __m256i *)avx2_nibble_lut[(b     ) & 0xF]);
            v0 = _mm256_blendv_epi8(imin, v0, m0);
            v1 = _mm256_blendv_epi8(imin, v1, m1);
            mx = max_s64_avx2(mx, max_s64_avx2(v0, v1));
        }
    }
    __m128i lo = _mm256_castsi256_si128(mx);
    __m128i hi = _mm256_extracti128_si256(mx, 1);
    int64_t a = _mm_extract_epi64(lo, 0);
    int64_t b = _mm_extract_epi64(lo, 1);
    int64_t c = _mm_extract_epi64(hi, 0);
    int64_t d = _mm_extract_epi64(hi, 1);
    int64_t m = a; if (b > m) m = b; if (c > m) m = c; if (d > m) m = d;
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] > m) m = v[i];
    }
    return m;
}
#define HAS_AVX2_I64_MINMAX 1

/* ---------- AVX-512 (per-function target so the binary stays portable) ---------- */

#if defined(__AVX512F__)

/* Bit-reverse a byte: bitmap is MSB-first (bit 7 = element 0) but
 * AVX-512 mask registers expect LSB-first (mask bit 0 = lane 0).
 * Done with a 3-level shuffle, branchless. */
static inline uint8_t reverse_bits8(uint8_t b) {
    b = (uint8_t)(((b & 0xF0u) >> 4) | ((b & 0x0Fu) << 4));
    b = (uint8_t)(((b & 0xCCu) >> 2) | ((b & 0x33u) << 2));
    b = (uint8_t)(((b & 0xAAu) >> 1) | ((b & 0x55u) << 1));
    return b;
}

__attribute__((target("avx512f,avx512dq")))
static double sum_f64_avx512(const double *v, size_t n, const uint8_t *bm) {
    /* 4 × __m512d = 32 doubles/iteration */
    __m512d a0 = _mm512_setzero_pd();
    __m512d a1 = _mm512_setzero_pd();
    __m512d a2 = _mm512_setzero_pd();
    __m512d a3 = _mm512_setzero_pd();
    size_t i = 0;
    if (!bm) {
        for (; i + 31 < n; i += 32) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm512_add_pd(a0, _mm512_loadu_pd(v + i +  0));
            a1 = _mm512_add_pd(a1, _mm512_loadu_pd(v + i +  8));
            a2 = _mm512_add_pd(a2, _mm512_loadu_pd(v + i + 16));
            a3 = _mm512_add_pd(a3, _mm512_loadu_pd(v + i + 24));
        }
        for (; i + 7 < n; i += 8)
            a0 = _mm512_add_pd(a0, _mm512_loadu_pd(v + i));
    } else {
        /* Native masked add: 1 byte = 8 lanes = 1 __m512d, mask register
         * loaded directly from the (bit-reversed) bitmap byte. */
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            __mmask8 k = (__mmask8)reverse_bits8(bm[i >> 3]);
            a0 = _mm512_mask_add_pd(a0, k, a0, _mm512_loadu_pd(v + i));
        }
    }
    a0 = _mm512_add_pd(_mm512_add_pd(a0, a1), _mm512_add_pd(a2, a3));
    double s = _mm512_reduce_add_pd(a0);
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) s += v[i];
    }
    return s;
}

__attribute__((target("avx512f,avx512dq")))
static double min_f64_avx512(const double *v, size_t n, const uint8_t *bm) {
    if (n == 0) return (double)INFINITY;

    __m512d mn = _mm512_set1_pd((double)INFINITY);
    size_t i = 0;
    if (!bm) {
        __m512d a0 = mn, a1 = mn, a2 = mn, a3 = mn;
        for (; i + 31 < n; i += 32) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm512_min_pd(a0, _mm512_loadu_pd(v + i +  0));
            a1 = _mm512_min_pd(a1, _mm512_loadu_pd(v + i +  8));
            a2 = _mm512_min_pd(a2, _mm512_loadu_pd(v + i + 16));
            a3 = _mm512_min_pd(a3, _mm512_loadu_pd(v + i + 24));
        }
        mn = _mm512_min_pd(_mm512_min_pd(a0, a1), _mm512_min_pd(a2, a3));
        for (; i + 7 < n; i += 8)
            mn = _mm512_min_pd(mn, _mm512_loadu_pd(v + i));
    } else {
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            __mmask8 k = (__mmask8)reverse_bits8(bm[i >> 3]);
            /* mask_min_pd: result[lane] = mask ? min(a,b)[lane] : a[lane] */
            mn = _mm512_mask_min_pd(mn, k, mn, _mm512_loadu_pd(v + i));
        }
    }
    double m = _mm512_reduce_min_pd(mn);
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] < m) m = v[i];
    }
    return m;
}

__attribute__((target("avx512f,avx512dq")))
static double max_f64_avx512(const double *v, size_t n, const uint8_t *bm) {
    if (n == 0) return -(double)INFINITY;

    __m512d mx = _mm512_set1_pd(-(double)INFINITY);
    size_t i = 0;
    if (!bm) {
        __m512d a0 = mx, a1 = mx, a2 = mx, a3 = mx;
        for (; i + 31 < n; i += 32) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm512_max_pd(a0, _mm512_loadu_pd(v + i +  0));
            a1 = _mm512_max_pd(a1, _mm512_loadu_pd(v + i +  8));
            a2 = _mm512_max_pd(a2, _mm512_loadu_pd(v + i + 16));
            a3 = _mm512_max_pd(a3, _mm512_loadu_pd(v + i + 24));
        }
        mx = _mm512_max_pd(_mm512_max_pd(a0, a1), _mm512_max_pd(a2, a3));
        for (; i + 7 < n; i += 8)
            mx = _mm512_max_pd(mx, _mm512_loadu_pd(v + i));
    } else {
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            __mmask8 k = (__mmask8)reverse_bits8(bm[i >> 3]);
            mx = _mm512_mask_max_pd(mx, k, mx, _mm512_loadu_pd(v + i));
        }
    }
    double m = _mm512_reduce_max_pd(mx);
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] > m) m = v[i];
    }
    return m;
}

__attribute__((target("avx512f")))
static int64_t sum_i64_avx512(const int64_t *v, size_t n,
                               const uint8_t *bm, uint64_t *cnt) {
    __m512i a0 = _mm512_setzero_si512();
    __m512i a1 = _mm512_setzero_si512();
    __m512i a2 = _mm512_setzero_si512();
    __m512i a3 = _mm512_setzero_si512();
    size_t i = 0;
    uint64_t valid_count = 0;
    if (!bm) {
        for (; i + 31 < n; i += 32) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm512_add_epi64(a0, _mm512_loadu_si512(v + i +  0));
            a1 = _mm512_add_epi64(a1, _mm512_loadu_si512(v + i +  8));
            a2 = _mm512_add_epi64(a2, _mm512_loadu_si512(v + i + 16));
            a3 = _mm512_add_epi64(a3, _mm512_loadu_si512(v + i + 24));
        }
        for (; i + 7 < n; i += 8)
            a0 = _mm512_add_epi64(a0, _mm512_loadu_si512(v + i));
        valid_count = (uint64_t)i;
    } else {
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint8_t b = bm[i >> 3];
            __mmask8 k = (__mmask8)reverse_bits8(b);
            a0 = _mm512_mask_add_epi64(a0, k, a0, _mm512_loadu_si512(v + i));
            valid_count += (uint64_t)__builtin_popcount(b);
        }
    }
    a0 = _mm512_add_epi64(_mm512_add_epi64(a0, a1), _mm512_add_epi64(a2, a3));
    int64_t s = (int64_t)_mm512_reduce_add_epi64(a0);
    for (; i < n; i++) {
        if (!bm || TSDB_NULL_VALID(bm, i)) { s += v[i]; valid_count++; }
    }
    *cnt = valid_count;
    return s;
}

/* AVX-512F has native _mm512_min_epi64 / _mm512_max_epi64 + masked variants. */
__attribute__((target("avx512f")))
static int64_t min_i64_avx512(const int64_t *v, size_t n, const uint8_t *bm) {
    if (n == 0) return INT64_MAX;
    __m512i mn = _mm512_set1_epi64(INT64_MAX);
    size_t i = 0;
    if (!bm) {
        __m512i a0 = mn, a1 = mn, a2 = mn, a3 = mn;
        for (; i + 31 < n; i += 32) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm512_min_epi64(a0, _mm512_loadu_si512(v + i +  0));
            a1 = _mm512_min_epi64(a1, _mm512_loadu_si512(v + i +  8));
            a2 = _mm512_min_epi64(a2, _mm512_loadu_si512(v + i + 16));
            a3 = _mm512_min_epi64(a3, _mm512_loadu_si512(v + i + 24));
        }
        mn = _mm512_min_epi64(_mm512_min_epi64(a0, a1), _mm512_min_epi64(a2, a3));
        for (; i + 7 < n; i += 8)
            mn = _mm512_min_epi64(mn, _mm512_loadu_si512(v + i));
    } else {
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            __mmask8 k = (__mmask8)reverse_bits8(bm[i >> 3]);
            mn = _mm512_mask_min_epi64(mn, k, mn, _mm512_loadu_si512(v + i));
        }
    }
    int64_t m = (int64_t)_mm512_reduce_min_epi64(mn);
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] < m) m = v[i];
    }
    return m;
}

__attribute__((target("avx512f")))
static int64_t max_i64_avx512(const int64_t *v, size_t n, const uint8_t *bm) {
    if (n == 0) return INT64_MIN;
    __m512i mx = _mm512_set1_epi64(INT64_MIN);
    size_t i = 0;
    if (!bm) {
        __m512i a0 = mx, a1 = mx, a2 = mx, a3 = mx;
        for (; i + 31 < n; i += 32) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            a0 = _mm512_max_epi64(a0, _mm512_loadu_si512(v + i +  0));
            a1 = _mm512_max_epi64(a1, _mm512_loadu_si512(v + i +  8));
            a2 = _mm512_max_epi64(a2, _mm512_loadu_si512(v + i + 16));
            a3 = _mm512_max_epi64(a3, _mm512_loadu_si512(v + i + 24));
        }
        mx = _mm512_max_epi64(_mm512_max_epi64(a0, a1), _mm512_max_epi64(a2, a3));
        for (; i + 7 < n; i += 8)
            mx = _mm512_max_epi64(mx, _mm512_loadu_si512(v + i));
    } else {
        for (; i + 7 < n; i += 8) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            __mmask8 k = (__mmask8)reverse_bits8(bm[i >> 3]);
            mx = _mm512_mask_max_epi64(mx, k, mx, _mm512_loadu_si512(v + i));
        }
    }
    int64_t m = (int64_t)_mm512_reduce_max_epi64(mx);
    for (; i < n; i++) {
        if ((!bm || TSDB_NULL_VALID(bm, i)) && v[i] > m) m = v[i];
    }
    return m;
}
#define HAS_AVX512_I64_MINMAX 1

#define HAS_AVX512_KERNELS 1
#endif /* __AVX512F__ */

#define HAS_X86_KERNELS 1
#endif /* x86 */

/* -----------------------------------------------------------------------
 * === DISPATCH TABLE ===
 * Function pointer types for the three inner signatures we care about.
 * --------------------------------------------------------------------- */

typedef double  (*fn_f64_inner)(const double *, size_t, const uint8_t *);
typedef int64_t (*fn_i64_inner)(const int64_t *, size_t, const uint8_t *, uint64_t *);
typedef int64_t (*fn_i64_minmax)(const int64_t *, size_t, const uint8_t *);

static fn_f64_inner  g_sum_f64 = NULL;
static fn_f64_inner  g_min_f64 = NULL;
static fn_f64_inner  g_max_f64 = NULL;
static fn_i64_inner  g_sum_i64 = NULL;
static fn_i64_minmax g_min_i64 = NULL;
static fn_i64_minmax g_max_i64 = NULL;

static pthread_once_t g_agg_once = PTHREAD_ONCE_INIT;

static void agg_init_once(void) {
    tsdb_cpu_level_t lv = tsdb_cpu_level();

    /* Always default to scalar */
    g_sum_f64 = sum_f64_scalar;
    g_min_f64 = min_f64_scalar;
    g_max_f64 = max_f64_scalar;
    g_sum_i64 = sum_i64_scalar;
    g_min_i64 = min_i64_scalar;
    g_max_i64 = max_i64_scalar;

#if defined(HAS_X86_KERNELS)
    if (lv >= TSDB_CPU_AVX2) {
        g_sum_f64 = sum_f64_avx2;
        g_min_f64 = min_f64_avx2;
        g_max_f64 = max_f64_avx2;
        g_sum_i64 = sum_i64_avx2;
#    if defined(HAS_AVX2_I64_MINMAX)
        g_min_i64 = min_i64_avx2;
        g_max_i64 = max_i64_avx2;
#    endif
    }
#  if defined(HAS_AVX512_KERNELS)
    if (lv >= TSDB_CPU_AVX512) {
        g_sum_f64 = sum_f64_avx512;
        g_min_f64 = min_f64_avx512;
        g_max_f64 = max_f64_avx512;
        g_sum_i64 = sum_i64_avx512;
#    if defined(HAS_AVX512_I64_MINMAX)
        g_min_i64 = min_i64_avx512;
        g_max_i64 = max_i64_avx512;
#    endif
    }
#  endif
#elif defined(HAS_NEON_KERNELS)
    if (lv >= TSDB_CPU_NEON) {
        g_sum_f64 = sum_f64_neon;
        g_min_f64 = min_f64_neon;
        g_max_f64 = max_f64_neon;
        g_sum_i64 = sum_i64_neon;
#    if defined(HAS_NEON_I64_MINMAX)
        g_min_i64 = min_i64_neon;
        g_max_i64 = max_i64_neon;
#    endif
    }
#endif
    (void)lv;  /* suppress unused-var if no SIMD compiled */
}

static inline void ensure_init(void) {
    pthread_once(&g_agg_once, agg_init_once);
}

/* -----------------------------------------------------------------------
 * Public API — float64
 * --------------------------------------------------------------------- */

int tsdb_agg_sum_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;
    ensure_init();
    if (n == 0) { *out = 0.0; *count = 0; return TSDB_OK; }
    *out = g_sum_f64(v, n, null_bitmap);
    /* Count is computed independently via popcount so SIMD sum kernels
     * stay focused on their accumulator hot loop. */
    *count = null_bitmap ? popcount_bitmap(null_bitmap, n) : n;
    return TSDB_OK;
}

int tsdb_agg_min_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    ensure_init();
    if (n == 0) { *out = (double)INFINITY; return TSDB_OK; }
    *out = g_min_f64(v, n, null_bitmap);
    return TSDB_OK;
}

int tsdb_agg_max_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    ensure_init();
    if (n == 0) { *out = -(double)INFINITY; return TSDB_OK; }
    *out = g_max_f64(v, n, null_bitmap);
    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * Public API — int64
 * --------------------------------------------------------------------- */

int tsdb_agg_sum_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;
    ensure_init();
    if (n == 0) { *out = 0; *count = 0; return TSDB_OK; }
    *out = g_sum_i64(v, n, null_bitmap, count);
    return TSDB_OK;
}

/* i64 min/max: SIMD-dispatched per CPU level — AVX2 emulates via cmpgt+blendv,
 * AVX-512 uses native _mm512_min_epi64, NEON emulates via vcltq_s64+vbslq. */
int tsdb_agg_min_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    ensure_init();
    *out = g_min_i64(v, n, null_bitmap);
    return TSDB_OK;
}

int tsdb_agg_max_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    ensure_init();
    *out = g_max_i64(v, n, null_bitmap);
    return TSDB_OK;
}
