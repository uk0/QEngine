/* filter.c — Column filter SIMD primitives.
 *
 * Output bitmap convention: LSB-first uint64 array.
 *   - Element i → bit (i & 63) of word (i >> 6).
 *   - Bit = 1: row passes; bit = 0: row rejected.
 *
 * All tsdb_filter_* functions AND their result into out_bitmap so multiple
 * filters compose naturally.  Callers initialise out_bitmap to all-1s.
 *
 * SIMD strategy:
 *   AVX2 : process 4 doubles per iteration (256-bit), pack 4 mask bits.
 *          64-bit mask words are built up 4 bits at a time.
 *   NEON : process 2 doubles per iteration (128-bit), 2 bits at a time.
 *   Scalar: 1 element per iteration.
 *
 * For each iteration block we collect a partial "pass" mask of TSDB_SIMD_WIDTH
 * bits and OR-shift them into the uint64 output word.  When a full 64-bit word
 * is assembled we AND it into out_bitmap.
 */
#include "filter.h"
#include "simd.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

/* -----------------------------------------------------------------------
 * Scalar helper: compare one double, return 0 or 1
 * --------------------------------------------------------------------- */
static inline int cmp_f64(double a, double b, tsdb_cmp_t op) {
    switch (op) {
    case TSDB_CMP_EQ: return a == b;
    case TSDB_CMP_NE: return a != b;
    case TSDB_CMP_LT: return a <  b;
    case TSDB_CMP_LE: return a <= b;
    case TSDB_CMP_GT: return a >  b;
    case TSDB_CMP_GE: return a >= b;
    default:          return 0;
    }
}

static inline int cmp_i64(int64_t a, int64_t b, tsdb_cmp_t op) {
    switch (op) {
    case TSDB_CMP_EQ: return a == b;
    case TSDB_CMP_NE: return a != b;
    case TSDB_CMP_LT: return a <  b;
    case TSDB_CMP_LE: return a <= b;
    case TSDB_CMP_GT: return a >  b;
    case TSDB_CMP_GE: return a >= b;
    default:          return 0;
    }
}

/* -----------------------------------------------------------------------
 * AVX2 implementation
 * --------------------------------------------------------------------- */
#if defined(TSDB_SIMD_AVX2)

/* Map tsdb_cmp_t → _CMP_* predicate (ordered, non-signaling) */
static inline int avx2_pred(tsdb_cmp_t op) {
    switch (op) {
    case TSDB_CMP_EQ: return _CMP_EQ_OQ;
    case TSDB_CMP_NE: return _CMP_NEQ_OQ;
    case TSDB_CMP_LT: return _CMP_LT_OQ;
    case TSDB_CMP_LE: return _CMP_LE_OQ;
    case TSDB_CMP_GT: return _CMP_GT_OQ;
    case TSDB_CMP_GE: return _CMP_GE_OQ;
    default:          return _CMP_FALSE_OQ;
    }
}

int tsdb_filter_f64(const double *v, size_t n, tsdb_cmp_t op, double rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;

    __m256d r = _mm256_set1_pd(rhs);
    int pred  = avx2_pred(op);
    size_t i  = 0;
    size_t nw = (n + 63) / 64;   /* number of uint64 words */

    /* Accumulate a full 64-bit pass mask, 4 bits per SIMD step */
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;

        for (i = base; i + 3 < end; i += 4) {
            __m256d vv  = _mm256_loadu_pd(v + i);
            __m256d cmp = _mm256_cmp_pd(vv, r, pred);
            int mask    = _mm256_movemask_pd(cmp);   /* 4 bits, LSB = lane 0 */
            pass |= (uint64_t)(unsigned)mask << (i - base);
        }
        /* scalar tail within this word */
        for (; i < end; i++) {
            if (cmp_f64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_i64(const int64_t *v, size_t n, tsdb_cmp_t op, int64_t rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;

    /* AVX2 has pcmpgtq (signed GT) and pcmpeqq (EQ).
     * We compose all 6 comparisons from those two primitives.
     * EQ : pcmpeqq
     * GT : pcmpgtq
     * LT : swap args of GT
     * NE : ~EQ
     * GE : GT | EQ
     * LE : LT | EQ
     */
    __m256i rhs_v = _mm256_set1_epi64x(rhs);
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        for (; i + 3 < end; i += 4) {
            __m256i vv  = _mm256_loadu_si256((const __m256i *)(v + i));
            __m256i eq  = _mm256_cmpeq_epi64(vv, rhs_v);
            __m256i gt  = _mm256_cmpgt_epi64(vv, rhs_v);
            __m256i lt  = _mm256_cmpgt_epi64(rhs_v, vv);
            __m256i sel;
            switch (op) {
            case TSDB_CMP_EQ: sel = eq; break;
            case TSDB_CMP_NE: sel = _mm256_andnot_si256(eq, _mm256_set1_epi64x(-1LL)); break;
            case TSDB_CMP_LT: sel = lt; break;
            case TSDB_CMP_LE: sel = _mm256_or_si256(lt, eq); break;
            case TSDB_CMP_GT: sel = gt; break;
            case TSDB_CMP_GE: sel = _mm256_or_si256(gt, eq); break;
            default:          sel = _mm256_setzero_si256(); break;
            }
            /* movemask on epi64: use 64-bit float cast to get 4 sign bits */
            int mask = _mm256_movemask_pd(_mm256_castsi256_pd(sel));
            pass |= (uint64_t)(unsigned)mask << (i - base);
        }
        for (; i < end; i++) {
            if (cmp_i64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_u32_eq(const uint32_t *v, size_t n, uint32_t rhs,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    __m256i r = _mm256_set1_epi32((int)rhs);
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        for (; i + 7 < end; i += 8) {
            __m256i vv  = _mm256_loadu_si256((const __m256i *)(v + i));
            __m256i eq  = _mm256_cmpeq_epi32(vv, r);
            /* 8 lanes of 32-bit: movemask_epi8 gives 32 sign bits (4 per element).
             * We want one bit per element: take every 4th sign bit. */
            int m32 = _mm256_movemask_epi8(eq); /* 32 bits */
            /* Compact: bits 0,4,8,... of m32 are the lane sign bits */
            uint32_t compact = 0;
            for (int b = 0; b < 8; b++)
                compact |= (uint32_t)((m32 >> (b * 4)) & 1) << b;
            pass |= (uint64_t)compact << (i - base);
        }
        for (; i < end; i++) {
            if (v[i] == rhs)
                pass |= (uint64_t)1 << (i - base);
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_u32_in(const uint32_t *v, size_t n,
                       const uint32_t *set, size_t nset,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;

        for (size_t i = base; i < end; i++) {
            for (size_t j = 0; j < nset; j++) {
                if (v[i] == set[j]) {
                    pass |= (uint64_t)1 << (i - base);
                    break;
                }
            }
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * NEON implementation
 * --------------------------------------------------------------------- */
#elif defined(TSDB_SIMD_NEON)

int tsdb_filter_f64(const double *v, size_t n, tsdb_cmp_t op, double rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;

    float64x2_t r = vdupq_n_f64(rhs);
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        for (; i + 1 < end; i += 2) {
            float64x2_t vv = vld1q_f64(v + i);
            uint64x2_t cmp;
            switch (op) {
            case TSDB_CMP_EQ: cmp = vceqq_f64(vv, r);  break;
            case TSDB_CMP_NE: cmp = vreinterpretq_u64_u32(vmvnq_u32(vreinterpretq_u32_u64(vceqq_f64(vv, r)))); break;
            case TSDB_CMP_LT: cmp = vcltq_f64(vv, r);  break;
            case TSDB_CMP_LE: cmp = vcleq_f64(vv, r);  break;
            case TSDB_CMP_GT: cmp = vcgtq_f64(vv, r);  break;
            case TSDB_CMP_GE: cmp = vcgeq_f64(vv, r);  break;
            default:          cmp = vdupq_n_u64(0);     break;
            }
            /* Extract top bit of each 64-bit lane: bit 63 = all-ones lane */
            uint64_t lane0 = vgetq_lane_u64(cmp, 0) >> 63;
            uint64_t lane1 = vgetq_lane_u64(cmp, 1) >> 63;
            pass |= lane0 << (i - base);
            pass |= lane1 << (i - base + 1);
        }
        /* scalar tail */
        for (; i < end; i++) {
            if (cmp_f64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_i64(const int64_t *v, size_t n, tsdb_cmp_t op, int64_t rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;

    int64x2_t r = vdupq_n_s64(rhs);
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        for (; i + 1 < end; i += 2) {
            int64x2_t  vv  = vld1q_s64(v + i);
            uint64x2_t eq  = vceqq_s64(vv, r);
            uint64x2_t gt  = vcgtq_s64(vv, r);
            uint64x2_t lt  = vcltq_s64(vv, r);
            uint64x2_t cmp;
            switch (op) {
            case TSDB_CMP_EQ: cmp = eq; break;
            case TSDB_CMP_NE: cmp = vreinterpretq_u64_u32(vmvnq_u32(vreinterpretq_u32_u64(eq))); break;
            case TSDB_CMP_LT: cmp = lt; break;
            case TSDB_CMP_LE: cmp = vorrq_u64(lt, eq); break;
            case TSDB_CMP_GT: cmp = gt; break;
            case TSDB_CMP_GE: cmp = vorrq_u64(gt, eq); break;
            default:          cmp = vdupq_n_u64(0); break;
            }
            uint64_t l0 = vgetq_lane_u64(cmp, 0) >> 63;
            uint64_t l1 = vgetq_lane_u64(cmp, 1) >> 63;
            pass |= l0 << (i - base);
            pass |= l1 << (i - base + 1);
        }
        for (; i < end; i++) {
            if (cmp_i64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_u32_eq(const uint32_t *v, size_t n, uint32_t rhs,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    uint32x4_t r = vdupq_n_u32(rhs);
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        for (; i + 3 < end; i += 4) {
            uint32x4_t vv  = vld1q_u32(v + i);
            uint32x4_t eq  = vceqq_u32(vv, r);
            /* Extract one bit per lane: use top bit of each 32-bit lane */
            uint64_t b0 = vgetq_lane_u32(eq, 0) >> 31;
            uint64_t b1 = vgetq_lane_u32(eq, 1) >> 31;
            uint64_t b2 = vgetq_lane_u32(eq, 2) >> 31;
            uint64_t b3 = vgetq_lane_u32(eq, 3) >> 31;
            pass |= (b0 | (b1 << 1) | (b2 << 2) | (b3 << 3)) << (i - base);
        }
        for (; i < end; i++) {
            if (v[i] == rhs)
                pass |= (uint64_t)1 << (i - base);
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_u32_in(const uint32_t *v, size_t n,
                       const uint32_t *set, size_t nset,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++) {
            for (size_t j = 0; j < nset; j++) {
                if (v[i] == set[j]) { pass |= (uint64_t)1 << (i - base); break; }
            }
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * Scalar fallback
 * --------------------------------------------------------------------- */
#else /* TSDB_SIMD_SCALAR */

int tsdb_filter_f64(const double *v, size_t n, tsdb_cmp_t op, double rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++)
            if (cmp_f64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_i64(const int64_t *v, size_t n, tsdb_cmp_t op, int64_t rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++)
            if (cmp_i64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_u32_eq(const uint32_t *v, size_t n, uint32_t rhs,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++)
            if (v[i] == rhs)
                pass |= (uint64_t)1 << (i - base);
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

int tsdb_filter_u32_in(const uint32_t *v, size_t n,
                       const uint32_t *set, size_t nset,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++) {
            for (size_t j = 0; j < nset; j++) {
                if (v[i] == set[j]) { pass |= (uint64_t)1 << (i - base); break; }
            }
        }
        out_bitmap[w] &= pass;
    }
    return TSDB_OK;
}

#endif /* dispatch */

/* -----------------------------------------------------------------------
 * Bitmap utilities — common across all SIMD paths
 * --------------------------------------------------------------------- */

uint64_t tsdb_bitmap_popcount(const uint64_t *bm, size_t n_bits) {
    if (!bm || n_bits == 0) return 0;
    size_t nw    = (n_bits + 63) / 64;
    uint64_t cnt = 0;
    for (size_t w = 0; w + 1 < nw; w++)
        cnt += (uint64_t)__builtin_popcountll(bm[w]);
    /* Mask the last word so bits past n_bits are not counted */
    size_t rem = n_bits & 63;
    uint64_t last = bm[nw - 1];
    if (rem) last &= ((uint64_t)1 << rem) - 1;
    cnt += (uint64_t)__builtin_popcountll(last);
    return cnt;
}

size_t tsdb_bitmap_gather_f64(const uint64_t *bm, const double *src,
                               size_t n, double *dst) {
    if (!bm || !src || !dst || n == 0) return 0;
    size_t out = 0;
    size_t nw  = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        uint64_t word = bm[w];
        /* Only iterate over set bits for density */
        while (word) {
            unsigned bit = (unsigned)__builtin_ctzll(word);
            size_t idx = base + bit;
            if (idx < end) dst[out++] = src[idx];
            word &= word - 1;   /* clear lowest set bit */
        }
    }
    return out;
}

size_t tsdb_bitmap_gather_i64(const uint64_t *bm, const int64_t *src,
                               size_t n, int64_t *dst) {
    if (!bm || !src || !dst || n == 0) return 0;
    size_t out = 0;
    size_t nw  = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        uint64_t word = bm[w];
        while (word) {
            unsigned bit = (unsigned)__builtin_ctzll(word);
            size_t idx = base + bit;
            if (idx < end) dst[out++] = src[idx];
            word &= word - 1;
        }
    }
    return out;
}
