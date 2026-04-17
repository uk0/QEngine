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

/* 4 × float64x2 accumulators = 8 lanes / iter, breaks 4 dep chains */
static double sum_f64_neon(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return sum_f64_scalar(v, n, bm);

    float64x2_t a0 = vdupq_n_f64(0.0);
    float64x2_t a1 = vdupq_n_f64(0.0);
    float64x2_t a2 = vdupq_n_f64(0.0);
    float64x2_t a3 = vdupq_n_f64(0.0);
    size_t i = 0;
    for (; i + 7 < n; i += 8) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        a0 = vaddq_f64(a0, vld1q_f64(v + i + 0));
        a1 = vaddq_f64(a1, vld1q_f64(v + i + 2));
        a2 = vaddq_f64(a2, vld1q_f64(v + i + 4));
        a3 = vaddq_f64(a3, vld1q_f64(v + i + 6));
    }
    for (; i + 1 < n; i += 2)
        a0 = vaddq_f64(a0, vld1q_f64(v + i));
    a0 = vaddq_f64(vaddq_f64(a0, a1), vaddq_f64(a2, a3));
    double s = vaddvq_f64(a0);
    if (i < n) s += v[i];
    return s;
}

static double min_f64_neon(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return min_f64_scalar(v, n, bm);
    if (n == 0) return (double)INFINITY;

    float64x2_t mn = vdupq_n_f64((double)INFINITY);
    size_t i = 0;
    for (; i + 1 < n; i += 2) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        mn = vminq_f64(mn, vld1q_f64(v + i));
    }
    double m = fmin(vgetq_lane_f64(mn, 0), vgetq_lane_f64(mn, 1));
    if (i < n && v[i] < m) m = v[i];
    return m;
}

static double max_f64_neon(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return max_f64_scalar(v, n, bm);
    if (n == 0) return -(double)INFINITY;

    float64x2_t mx = vdupq_n_f64(-(double)INFINITY);
    size_t i = 0;
    for (; i + 1 < n; i += 2) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        mx = vmaxq_f64(mx, vld1q_f64(v + i));
    }
    double m = fmax(vgetq_lane_f64(mx, 0), vgetq_lane_f64(mx, 1));
    if (i < n && v[i] > m) m = v[i];
    return m;
}

/* 4 × int64x2 accumulators */
static int64_t sum_i64_neon(const int64_t *v, size_t n,
                             const uint8_t *bm, uint64_t *cnt) {
    if (bm) return sum_i64_scalar(v, n, bm, cnt);

    int64x2_t a0 = vdupq_n_s64(0);
    int64x2_t a1 = vdupq_n_s64(0);
    int64x2_t a2 = vdupq_n_s64(0);
    int64x2_t a3 = vdupq_n_s64(0);
    size_t i = 0;
    for (; i + 7 < n; i += 8) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        a0 = vaddq_s64(a0, vld1q_s64(v + i + 0));
        a1 = vaddq_s64(a1, vld1q_s64(v + i + 2));
        a2 = vaddq_s64(a2, vld1q_s64(v + i + 4));
        a3 = vaddq_s64(a3, vld1q_s64(v + i + 6));
    }
    for (; i + 1 < n; i += 2)
        a0 = vaddq_s64(a0, vld1q_s64(v + i));
    a0 = vaddq_s64(vaddq_s64(a0, a1), vaddq_s64(a2, a3));
    int64_t s = vgetq_lane_s64(a0, 0) + vgetq_lane_s64(a0, 1);
    if (i < n) s += v[i];
    *cnt = n;
    return s;
}

#define HAS_NEON_KERNELS 1
#endif /* NEON */

/* -----------------------------------------------------------------------
 * === AVX2 / AVX-512 KERNELS ===  (x86-64 only)
 * --------------------------------------------------------------------- */
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>

/* ---------- AVX2 ---------- */

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
    if (bm) return sum_f64_scalar(v, n, bm);

    __m256d a0 = _mm256_setzero_pd();
    __m256d a1 = _mm256_setzero_pd();
    __m256d a2 = _mm256_setzero_pd();
    __m256d a3 = _mm256_setzero_pd();
    size_t i = 0;
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
    a0 = _mm256_add_pd(_mm256_add_pd(a0, a1), _mm256_add_pd(a2, a3));
    double s = hsum256_pd(a0);
    for (; i < n; i++) s += v[i];
    return s;
}

__attribute__((target("avx2")))
static double min_f64_avx2(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return min_f64_scalar(v, n, bm);
    if (n == 0) return (double)INFINITY;

    __m256d mn = _mm256_set1_pd((double)INFINITY);
    size_t i = 0;
    for (; i + 3 < n; i += 4) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        mn = _mm256_min_pd(mn, _mm256_loadu_pd(v + i));
    }
    __m128d lo = _mm256_castpd256_pd128(mn);
    __m128d hi = _mm256_extractf128_pd(mn, 1);
    lo = _mm_min_pd(lo, hi);
    lo = _mm_min_pd(lo, _mm_unpackhi_pd(lo, lo));
    double m = _mm_cvtsd_f64(lo);
    for (; i < n; i++) if (v[i] < m) m = v[i];
    return m;
}

__attribute__((target("avx2")))
static double max_f64_avx2(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return max_f64_scalar(v, n, bm);
    if (n == 0) return -(double)INFINITY;

    __m256d mx = _mm256_set1_pd(-(double)INFINITY);
    size_t i = 0;
    for (; i + 3 < n; i += 4) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        mx = _mm256_max_pd(mx, _mm256_loadu_pd(v + i));
    }
    __m128d lo = _mm256_castpd256_pd128(mx);
    __m128d hi = _mm256_extractf128_pd(mx, 1);
    lo = _mm_max_pd(lo, hi);
    lo = _mm_max_pd(lo, _mm_unpackhi_pd(lo, lo));
    double m = _mm_cvtsd_f64(lo);
    for (; i < n; i++) if (v[i] > m) m = v[i];
    return m;
}

__attribute__((target("avx2")))
static int64_t sum_i64_avx2(const int64_t *v, size_t n,
                              const uint8_t *bm, uint64_t *cnt) {
    if (bm) return sum_i64_scalar(v, n, bm, cnt);

    __m256i a0 = _mm256_setzero_si256();
    __m256i a1 = _mm256_setzero_si256();
    __m256i a2 = _mm256_setzero_si256();
    __m256i a3 = _mm256_setzero_si256();
    size_t i = 0;
    for (; i + 15 < n; i += 16) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        a0 = _mm256_add_epi64(a0, _mm256_loadu_si256((const __m256i *)(v + i +  0)));
        a1 = _mm256_add_epi64(a1, _mm256_loadu_si256((const __m256i *)(v + i +  4)));
        a2 = _mm256_add_epi64(a2, _mm256_loadu_si256((const __m256i *)(v + i +  8)));
        a3 = _mm256_add_epi64(a3, _mm256_loadu_si256((const __m256i *)(v + i + 12)));
    }
    for (; i + 3 < n; i += 4)
        a0 = _mm256_add_epi64(a0, _mm256_loadu_si256((const __m256i *)(v + i)));
    a0 = _mm256_add_epi64(_mm256_add_epi64(a0, a1), _mm256_add_epi64(a2, a3));
    __m128i lo128 = _mm256_castsi256_si128(a0);
    __m128i hi128 = _mm256_extracti128_si256(a0, 1);
    lo128 = _mm_add_epi64(lo128, hi128);
    int64_t s = (int64_t)(_mm_extract_epi64(lo128, 0) + _mm_extract_epi64(lo128, 1));
    for (; i < n; i++) s += v[i];
    *cnt = n;
    return s;
}

/* ---------- AVX-512 (per-function target so the binary stays portable) ---------- */

#if defined(__AVX512F__)

__attribute__((target("avx512f,avx512dq")))
static double sum_f64_avx512(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return sum_f64_scalar(v, n, bm);

    /* 4 × __m512d = 32 doubles/iteration */
    __m512d a0 = _mm512_setzero_pd();
    __m512d a1 = _mm512_setzero_pd();
    __m512d a2 = _mm512_setzero_pd();
    __m512d a3 = _mm512_setzero_pd();
    size_t i = 0;
    for (; i + 31 < n; i += 32) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        a0 = _mm512_add_pd(a0, _mm512_loadu_pd(v + i +  0));
        a1 = _mm512_add_pd(a1, _mm512_loadu_pd(v + i +  8));
        a2 = _mm512_add_pd(a2, _mm512_loadu_pd(v + i + 16));
        a3 = _mm512_add_pd(a3, _mm512_loadu_pd(v + i + 24));
    }
    for (; i + 7 < n; i += 8)
        a0 = _mm512_add_pd(a0, _mm512_loadu_pd(v + i));
    a0 = _mm512_add_pd(_mm512_add_pd(a0, a1), _mm512_add_pd(a2, a3));
    double s = _mm512_reduce_add_pd(a0);
    for (; i < n; i++) s += v[i];
    return s;
}

__attribute__((target("avx512f,avx512dq")))
static double min_f64_avx512(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return min_f64_scalar(v, n, bm);
    if (n == 0) return (double)INFINITY;

    __m512d mn = _mm512_set1_pd((double)INFINITY);
    size_t i = 0;
    for (; i + 7 < n; i += 8) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        mn = _mm512_min_pd(mn, _mm512_loadu_pd(v + i));
    }
    double m = _mm512_reduce_min_pd(mn);
    for (; i < n; i++) if (v[i] < m) m = v[i];
    return m;
}

__attribute__((target("avx512f,avx512dq")))
static double max_f64_avx512(const double *v, size_t n, const uint8_t *bm) {
    if (bm) return max_f64_scalar(v, n, bm);
    if (n == 0) return -(double)INFINITY;

    __m512d mx = _mm512_set1_pd(-(double)INFINITY);
    size_t i = 0;
    for (; i + 7 < n; i += 8) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        mx = _mm512_max_pd(mx, _mm512_loadu_pd(v + i));
    }
    double m = _mm512_reduce_max_pd(mx);
    for (; i < n; i++) if (v[i] > m) m = v[i];
    return m;
}

__attribute__((target("avx512f")))
static int64_t sum_i64_avx512(const int64_t *v, size_t n,
                               const uint8_t *bm, uint64_t *cnt) {
    if (bm) return sum_i64_scalar(v, n, bm, cnt);

    __m512i a0 = _mm512_setzero_si512();
    __m512i a1 = _mm512_setzero_si512();
    __m512i a2 = _mm512_setzero_si512();
    __m512i a3 = _mm512_setzero_si512();
    size_t i = 0;
    for (; i + 31 < n; i += 32) {
        __builtin_prefetch(v + i + PF_STRIDE, 0, 0);
        a0 = _mm512_add_epi64(a0, _mm512_loadu_si512(v + i +  0));
        a1 = _mm512_add_epi64(a1, _mm512_loadu_si512(v + i +  8));
        a2 = _mm512_add_epi64(a2, _mm512_loadu_si512(v + i + 16));
        a3 = _mm512_add_epi64(a3, _mm512_loadu_si512(v + i + 24));
    }
    for (; i + 7 < n; i += 8)
        a0 = _mm512_add_epi64(a0, _mm512_loadu_si512(v + i));
    a0 = _mm512_add_epi64(_mm512_add_epi64(a0, a1), _mm512_add_epi64(a2, a3));
    int64_t s = (int64_t)_mm512_reduce_add_epi64(a0);
    for (; i < n; i++) s += v[i];
    *cnt = n;
    return s;
}

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

static fn_f64_inner g_sum_f64 = NULL;
static fn_f64_inner g_min_f64 = NULL;
static fn_f64_inner g_max_f64 = NULL;
static fn_i64_inner g_sum_i64 = NULL;

static pthread_once_t g_agg_once = PTHREAD_ONCE_INIT;

static void agg_init_once(void) {
    tsdb_cpu_level_t lv = tsdb_cpu_level();

    /* Always default to scalar */
    g_sum_f64 = sum_f64_scalar;
    g_min_f64 = min_f64_scalar;
    g_max_f64 = max_f64_scalar;
    g_sum_i64 = sum_i64_scalar;

#if defined(HAS_X86_KERNELS)
    if (lv >= TSDB_CPU_AVX2) {
        g_sum_f64 = sum_f64_avx2;
        g_min_f64 = min_f64_avx2;
        g_max_f64 = max_f64_avx2;
        g_sum_i64 = sum_i64_avx2;
    }
#  if defined(HAS_AVX512_KERNELS)
    if (lv >= TSDB_CPU_AVX512) {
        g_sum_f64 = sum_f64_avx512;
        g_min_f64 = min_f64_avx512;
        g_max_f64 = max_f64_avx512;
        g_sum_i64 = sum_i64_avx512;
    }
#  endif
#elif defined(HAS_NEON_KERNELS)
    if (lv >= TSDB_CPU_NEON) {
        g_sum_f64 = sum_f64_neon;
        g_min_f64 = min_f64_neon;
        g_max_f64 = max_f64_neon;
        g_sum_i64 = sum_i64_neon;
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
    if (!null_bitmap) {
        *out   = g_sum_f64(v, n, NULL);
        *count = n;
    } else {
        *out   = sum_f64_scalar_cnt(v, n, null_bitmap, count);
    }
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

/* i64 min/max: AVX2 lacks _mm256_min_epi64; scalar is safe + fast enough */
int tsdb_agg_min_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    *out = min_i64_scalar(v, n, null_bitmap);
    return TSDB_OK;
}

int tsdb_agg_max_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    *out = max_i64_scalar(v, n, null_bitmap);
    return TSDB_OK;
}
