/* filter.c — Column filter SIMD primitives with runtime CPU dispatch.
 *
 * Output bitmap convention: LSB-first uint64 array.
 *   - Element i → bit (i & 63) of word (i >> 6).
 *   - Bit = 1: row passes; bit = 0: row rejected.
 *
 * All tsdb_filter_* functions AND their result into out_bitmap so multiple
 * filters compose naturally.  Callers must initialise out_bitmap to all-1s.
 *
 * Kernel versions per path:
 *   AVX-512 : 16 doubles/cmp per iteration (512-bit), 16-bit mask via _mm512_cmp_pd
 *   AVX2    : 16 doubles per iteration (4 × 4-wide), 4-bit mask per step
 *   NEON    : 4 doubles per iteration (2 × float64x2_t), 2-bit per step
 *   Scalar  : 1 element per iteration
 *
 * Prefetch: 4 KB ahead on long no-null paths.
 *
 * Bitmap utilities (popcount, gather) are common — compiled once at bottom.
 */
#include "filter.h"
#include "cpuid.h"
#include "simd.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

#define PF_STRIDE 512   /* doubles */

/* -----------------------------------------------------------------------
 * Scalar helpers
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
 * === SCALAR KERNELS ===
 * --------------------------------------------------------------------- */

static int filter_f64_scalar(const double *v, size_t n, tsdb_cmp_t op,
                              double rhs, uint64_t *bm) {
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            if (cmp_f64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        }
        bm[w] &= pass;
    }
    return TSDB_OK;
}

static int filter_i64_scalar(const int64_t *v, size_t n, tsdb_cmp_t op,
                              int64_t rhs, uint64_t *bm) {
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++)
            if (cmp_i64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

static int filter_u32_eq_scalar(const uint32_t *v, size_t n, uint32_t rhs,
                                 uint64_t *bm) {
    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        for (size_t i = base; i < end; i++)
            if (v[i] == rhs)
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

static int filter_u32_in_scalar(const uint32_t *v, size_t n,
                                  const uint32_t *set, size_t nset,
                                  uint64_t *bm) {
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
        bm[w] &= pass;
    }
    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * === NEON KERNELS ===  (AArch64 only)
 * --------------------------------------------------------------------- */
#if defined(__aarch64__) || defined(__ARM_NEON)
#include <arm_neon.h>

static int filter_f64_neon(const double *v, size_t n, tsdb_cmp_t op,
                            double rhs, uint64_t *bm) {
    float64x2_t r = vdupq_n_f64(rhs);
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        /* Process 4 doubles at a time (2 × float64x2_t) to improve ILP */
        for (; i + 3 < end; i += 4) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            float64x2_t v0 = vld1q_f64(v + i + 0);
            float64x2_t v1 = vld1q_f64(v + i + 2);
            uint64x2_t cmp0, cmp1;
            switch (op) {
            case TSDB_CMP_EQ:
                cmp0 = vceqq_f64(v0, r); cmp1 = vceqq_f64(v1, r); break;
            case TSDB_CMP_NE:
                cmp0 = vreinterpretq_u64_u32(vmvnq_u32(vreinterpretq_u32_u64(vceqq_f64(v0, r))));
                cmp1 = vreinterpretq_u64_u32(vmvnq_u32(vreinterpretq_u32_u64(vceqq_f64(v1, r)))); break;
            case TSDB_CMP_LT:
                cmp0 = vcltq_f64(v0, r); cmp1 = vcltq_f64(v1, r); break;
            case TSDB_CMP_LE:
                cmp0 = vcleq_f64(v0, r); cmp1 = vcleq_f64(v1, r); break;
            case TSDB_CMP_GT:
                cmp0 = vcgtq_f64(v0, r); cmp1 = vcgtq_f64(v1, r); break;
            case TSDB_CMP_GE:
                cmp0 = vcgeq_f64(v0, r); cmp1 = vcgeq_f64(v1, r); break;
            default:
                cmp0 = cmp1 = vdupq_n_u64(0); break;
            }
            uint64_t b0 = vgetq_lane_u64(cmp0, 0) >> 63;
            uint64_t b1 = vgetq_lane_u64(cmp0, 1) >> 63;
            uint64_t b2 = vgetq_lane_u64(cmp1, 0) >> 63;
            uint64_t b3 = vgetq_lane_u64(cmp1, 1) >> 63;
            pass |= (b0 | (b1 << 1) | (b2 << 2) | (b3 << 3)) << (i - base);
        }
        /* scalar tail */
        for (; i < end; i++)
            if (cmp_f64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

static int filter_i64_neon(const int64_t *v, size_t n, tsdb_cmp_t op,
                            int64_t rhs, uint64_t *bm) {
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
        for (; i < end; i++)
            if (cmp_i64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

static int filter_u32_eq_neon(const uint32_t *v, size_t n, uint32_t rhs,
                               uint64_t *bm) {
    uint32x4_t r = vdupq_n_u32(rhs);
    size_t nw = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        for (; i + 3 < end; i += 4) {
            uint32x4_t vv = vld1q_u32(v + i);
            uint32x4_t eq = vceqq_u32(vv, r);
            uint64_t b0 = vgetq_lane_u32(eq, 0) >> 31;
            uint64_t b1 = vgetq_lane_u32(eq, 1) >> 31;
            uint64_t b2 = vgetq_lane_u32(eq, 2) >> 31;
            uint64_t b3 = vgetq_lane_u32(eq, 3) >> 31;
            pass |= (b0 | (b1 << 1) | (b2 << 2) | (b3 << 3)) << (i - base);
        }
        for (; i < end; i++)
            if (v[i] == rhs)
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

#define HAS_NEON_FILTER 1
#endif /* NEON */

/* -----------------------------------------------------------------------
 * === AVX2 KERNELS ===  (x86-64 only)
 *
 * Compile-out escape hatch: -DTSDB_NO_AVX2_FILTER skips these kernels
 * entirely so the scalar path remains.  Needed on toolchains (alpine
 * clang-17, alpine gcc-13) where constant-propagation through the
 * avx2_pred helper fails and _mm256_cmp_pd rejects the runtime `pred`.
 * --------------------------------------------------------------------- */
#if (defined(__x86_64__) || defined(__i386__)) && !defined(TSDB_NO_AVX2_FILTER)
#include <immintrin.h>

static inline __attribute__((target("avx2"))) int avx2_pred(tsdb_cmp_t op) {
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

/* AVX2 f64: load 16 doubles per word iteration (4 per SIMD op × 4 ops) */
__attribute__((target("avx2")))
static int filter_f64_avx2(const double *v, size_t n, tsdb_cmp_t op,
                            double rhs, uint64_t *bm) {
    __m256d r   = _mm256_set1_pd(rhs);
    int pred    = avx2_pred(op);
    size_t nw   = (n + 63) / 64;

    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        /* Load 16 doubles / iter = 4 × _mm256_cmp_pd → 4 × 4-bit mask */
        for (; i + 15 < end; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            uint64_t m0 = (uint64_t)(unsigned)_mm256_movemask_pd(
                _mm256_cmp_pd(_mm256_loadu_pd(v + i +  0), r, pred));
            uint64_t m1 = (uint64_t)(unsigned)_mm256_movemask_pd(
                _mm256_cmp_pd(_mm256_loadu_pd(v + i +  4), r, pred));
            uint64_t m2 = (uint64_t)(unsigned)_mm256_movemask_pd(
                _mm256_cmp_pd(_mm256_loadu_pd(v + i +  8), r, pred));
            uint64_t m3 = (uint64_t)(unsigned)_mm256_movemask_pd(
                _mm256_cmp_pd(_mm256_loadu_pd(v + i + 12), r, pred));
            pass |= (m0 | (m1 << 4) | (m2 << 8) | (m3 << 12)) << (i - base);
        }
        for (; i + 3 < end; i += 4) {
            uint64_t m = (uint64_t)(unsigned)_mm256_movemask_pd(
                _mm256_cmp_pd(_mm256_loadu_pd(v + i), r, pred));
            pass |= m << (i - base);
        }
        for (; i < end; i++)
            if (cmp_f64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

__attribute__((target("avx2")))
static int filter_i64_avx2(const int64_t *v, size_t n, tsdb_cmp_t op,
                            int64_t rhs, uint64_t *bm) {
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
            uint64_t mask = (uint64_t)(unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(sel));
            pass |= mask << (i - base);
        }
        for (; i < end; i++)
            if (cmp_i64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

__attribute__((target("avx2")))
static int filter_u32_eq_avx2(const uint32_t *v, size_t n, uint32_t rhs,
                               uint64_t *bm) {
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
            int m32 = _mm256_movemask_epi8(eq);   /* 32 bits, 4 per element */
            /* Compact 4-bit→1-bit per element using sign bits */
            uint32_t compact = 0;
            for (int b = 0; b < 8; b++)
                compact |= (uint32_t)((m32 >> (b * 4)) & 1) << b;
            pass |= (uint64_t)compact << (i - base);
        }
        for (; i < end; i++)
            if (v[i] == rhs)
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * AVX-512 f64 filter: 16 doubles/cmp, 16-bit mask
 * --------------------------------------------------------------------- */
#if defined(__AVX512F__) && defined(__AVX512BW__)

__attribute__((target("avx512f,avx512bw,avx512dq")))
static int filter_f64_avx512(const double *v, size_t n, tsdb_cmp_t op,
                              double rhs, uint64_t *bm) {
    __m512d r = _mm512_set1_pd(rhs);
    /* Map op → _MM_CMPINT or _CMP predicate */
    int pred;
    switch (op) {
    case TSDB_CMP_EQ: pred = _CMP_EQ_OQ;  break;
    case TSDB_CMP_NE: pred = _CMP_NEQ_OQ; break;
    case TSDB_CMP_LT: pred = _CMP_LT_OQ;  break;
    case TSDB_CMP_LE: pred = _CMP_LE_OQ;  break;
    case TSDB_CMP_GT: pred = _CMP_GT_OQ;  break;
    case TSDB_CMP_GE: pred = _CMP_GE_OQ;  break;
    default:          pred = _CMP_FALSE_OQ; break;
    }

    size_t nw = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        uint64_t pass = 0;
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        size_t i    = base;

        /* 16 doubles = 1 __m512d, mask is __mmask16 */
        for (; i + 15 < end; i += 16) {
            __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
            __mmask16 mask = _mm512_cmp_pd_mask(_mm512_loadu_pd(v + i), r, pred);
            pass |= (uint64_t)(uint16_t)mask << (i - base);
        }
        for (; i + 3 < end; i += 4) {
            uint64_t m = (uint64_t)(unsigned)_mm256_movemask_pd(
                _mm256_cmp_pd(_mm256_loadu_pd(v + i),
                              _mm256_set1_pd(rhs), pred));
            pass |= m << (i - base);
        }
        for (; i < end; i++)
            if (cmp_f64(v[i], rhs, op))
                pass |= (uint64_t)1 << (i - base);
        bm[w] &= pass;
    }
    return TSDB_OK;
}

#define HAS_AVX512_FILTER 1
#endif /* AVX-512 filter */

#define HAS_X86_FILTER 1
#endif /* x86 */

/* -----------------------------------------------------------------------
 * === DISPATCH TABLE ===
 * --------------------------------------------------------------------- */
typedef int (*fn_filter_f64)(const double *, size_t, tsdb_cmp_t, double, uint64_t *);
typedef int (*fn_filter_i64)(const int64_t *, size_t, tsdb_cmp_t, int64_t, uint64_t *);
typedef int (*fn_filter_u32_eq)(const uint32_t *, size_t, uint32_t, uint64_t *);

static fn_filter_f64    g_filter_f64    = NULL;
static fn_filter_i64    g_filter_i64    = NULL;
static fn_filter_u32_eq g_filter_u32_eq = NULL;

static pthread_once_t g_filter_once = PTHREAD_ONCE_INIT;

static void filter_init_once(void) {
    tsdb_cpu_level_t lv = tsdb_cpu_level();

    g_filter_f64    = filter_f64_scalar;
    g_filter_i64    = filter_i64_scalar;
    g_filter_u32_eq = filter_u32_eq_scalar;

#if defined(HAS_X86_FILTER)
    if (lv >= TSDB_CPU_AVX2) {
        g_filter_f64    = filter_f64_avx2;
        g_filter_i64    = filter_i64_avx2;
        g_filter_u32_eq = filter_u32_eq_avx2;
    }
#  if defined(HAS_AVX512_FILTER)
    if (lv >= TSDB_CPU_AVX512) {
        g_filter_f64 = filter_f64_avx512;
        /* i64 and u32_eq stay AVX2 — avx512 variant not added yet */
    }
#  endif
#elif defined(HAS_NEON_FILTER)
    if (lv >= TSDB_CPU_NEON) {
        g_filter_f64    = filter_f64_neon;
        g_filter_i64    = filter_i64_neon;
        g_filter_u32_eq = filter_u32_eq_neon;
    }
#endif
    (void)lv;
}

static inline void filter_ensure_init(void) {
    pthread_once(&g_filter_once, filter_init_once);
}

/* -----------------------------------------------------------------------
 * Public API
 * --------------------------------------------------------------------- */

int tsdb_filter_f64(const double *v, size_t n, tsdb_cmp_t op, double rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    filter_ensure_init();
    return g_filter_f64(v, n, op, rhs, out_bitmap);
}

int tsdb_filter_i64(const int64_t *v, size_t n, tsdb_cmp_t op, int64_t rhs,
                    uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    filter_ensure_init();
    return g_filter_i64(v, n, op, rhs, out_bitmap);
}

int tsdb_filter_u32_eq(const uint32_t *v, size_t n, uint32_t rhs,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    filter_ensure_init();
    return g_filter_u32_eq(v, n, rhs, out_bitmap);
}

int tsdb_filter_u32_in(const uint32_t *v, size_t n,
                       const uint32_t *set, size_t nset,
                       uint64_t *out_bitmap) {
    if (!v || !out_bitmap) return TSDB_ERR_INVAL;
    return filter_u32_in_scalar(v, n, set, nset, out_bitmap);
}

/* -----------------------------------------------------------------------
 * Bitmap utilities — common across all SIMD paths
 * --------------------------------------------------------------------- */

uint64_t tsdb_bitmap_popcount(const uint64_t *bm, size_t n_bits) {
    if (!bm || n_bits == 0) return 0;
    size_t nw    = (n_bits + 63) / 64;
    uint64_t cnt = 0;
    for (size_t w = 0; w + 1 < nw; w++)
        cnt += (uint64_t)__builtin_popcountll(bm[w]);
    size_t rem = n_bits & 63;
    uint64_t last = bm[nw - 1];
    if (rem) last &= ((uint64_t)1 << rem) - 1;
    cnt += (uint64_t)__builtin_popcountll(last);
    return cnt;
}

/* tsdb_bitmap_gather_f64 / _i64 are now in gather.c */
