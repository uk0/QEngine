/* agg.c — Column aggregate SIMD primitives.
 *
 * SIMD dispatch priority: AVX2 → NEON → scalar.
 * All three paths MUST produce bit-identical results on valid data.
 *
 * Design notes:
 *  - null_bitmap is MSB-first: element i valid iff byte[i/8] bit (7-(i&7)) = 1.
 *  - SIMD paths handle the aligned bulk; a scalar tail handles the remainder.
 *  - For min/max on int64: AVX2 lacks _mm256_min_epi64 (it's AVX-512), and
 *    NEON vminq_s64 requires ARMv8.4. We therefore use scalar loops for
 *    min/max i64 even inside the SIMD compilation unit.
 */
#include "agg.h"
#include "simd.h"

#include <stddef.h>
#include <stdint.h>
#include <math.h>    /* INFINITY */
#include <float.h>   /* DBL_MAX */
#include <limits.h>  /* INT64_MAX / INT64_MIN */
#include <string.h>

/* -----------------------------------------------------------------------
 * Helpers
 * --------------------------------------------------------------------- */

static inline int valid(const uint8_t *bm, size_t i) {
    if (!bm) return 1;
    return TSDB_NULL_VALID(bm, i);
}

/* -----------------------------------------------------------------------
 * AVX2 implementation
 * --------------------------------------------------------------------- */
#if defined(TSDB_SIMD_AVX2)

/* Horizontal sum of four f64 lanes in __m256d → scalar double */
static inline double hsum256_pd(__m256d v) {
    __m128d lo  = _mm256_castpd256_pd128(v);
    __m128d hi  = _mm256_extractf128_pd(v, 1);
    __m128d sum = _mm_add_pd(lo, hi);
    __m128d tmp = _mm_unpackhi_pd(sum, sum);
    return _mm_cvtsd_f64(_mm_add_pd(sum, tmp));
}

int tsdb_agg_sum_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;

    /* Fast path: no nulls */
    if (!null_bitmap) {
        __m256d acc0 = _mm256_setzero_pd();
        __m256d acc1 = _mm256_setzero_pd();
        __m256d acc2 = _mm256_setzero_pd();
        __m256d acc3 = _mm256_setzero_pd();
        size_t i = 0;
        /* Unrolled 4× for better ILP */
        for (; i + 15 < n; i += 16) {
            acc0 = _mm256_add_pd(acc0, _mm256_loadu_pd(v + i +  0));
            acc1 = _mm256_add_pd(acc1, _mm256_loadu_pd(v + i +  4));
            acc2 = _mm256_add_pd(acc2, _mm256_loadu_pd(v + i +  8));
            acc3 = _mm256_add_pd(acc3, _mm256_loadu_pd(v + i + 12));
        }
        for (; i + 3 < n; i += 4) {
            acc0 = _mm256_add_pd(acc0, _mm256_loadu_pd(v + i));
        }
        acc0 = _mm256_add_pd(_mm256_add_pd(acc0, acc1),
                             _mm256_add_pd(acc2, acc3));
        double s = hsum256_pd(acc0);
        /* scalar tail */
        for (; i < n; i++) s += v[i];
        *out   = s;
        *count = n;
        return TSDB_OK;
    }

    /* Null-aware path */
    double s = 0.0;
    uint64_t cnt = 0;
    for (size_t i = 0; i < n; i++) {
        if (valid(null_bitmap, i)) { s += v[i]; cnt++; }
    }
    *out   = s;
    *count = cnt;
    return TSDB_OK;
}

int tsdb_agg_min_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    if (n == 0) { *out = (double)INFINITY; return TSDB_OK; }

    if (!null_bitmap) {
        __m256d mn = _mm256_set1_pd((double)INFINITY);
        size_t i = 0;
        for (; i + 3 < n; i += 4)
            mn = _mm256_min_pd(mn, _mm256_loadu_pd(v + i));
        /* horizontal min of 4 lanes */
        __m128d lo = _mm256_castpd256_pd128(mn);
        __m128d hi = _mm256_extractf128_pd(mn, 1);
        lo = _mm_min_pd(lo, hi);
        lo = _mm_min_pd(lo, _mm_unpackhi_pd(lo, lo));
        double m = _mm_cvtsd_f64(lo);
        for (; i < n; i++) if (v[i] < m) m = v[i];
        *out = m;
    } else {
        double m = (double)INFINITY;
        for (size_t i = 0; i < n; i++)
            if (valid(null_bitmap, i) && v[i] < m) m = v[i];
        *out = m;
    }
    return TSDB_OK;
}

int tsdb_agg_max_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    if (n == 0) { *out = -(double)INFINITY; return TSDB_OK; }

    if (!null_bitmap) {
        __m256d mx = _mm256_set1_pd(-(double)INFINITY);
        size_t i = 0;
        for (; i + 3 < n; i += 4)
            mx = _mm256_max_pd(mx, _mm256_loadu_pd(v + i));
        __m128d lo = _mm256_castpd256_pd128(mx);
        __m128d hi = _mm256_extractf128_pd(mx, 1);
        lo = _mm_max_pd(lo, hi);
        lo = _mm_max_pd(lo, _mm_unpackhi_pd(lo, lo));
        double m = _mm_cvtsd_f64(lo);
        for (; i < n; i++) if (v[i] > m) m = v[i];
        *out = m;
    } else {
        double m = -(double)INFINITY;
        for (size_t i = 0; i < n; i++)
            if (valid(null_bitmap, i) && v[i] > m) m = v[i];
        *out = m;
    }
    return TSDB_OK;
}

/* int64 sum: use 256-bit integer arithmetic */
int tsdb_agg_sum_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;

    if (!null_bitmap) {
        __m256i acc = _mm256_setzero_si256();
        size_t i = 0;
        for (; i + 3 < n; i += 4)
            acc = _mm256_add_epi64(acc, _mm256_loadu_si256((const __m256i *)(v + i)));
        /* horizontal sum */
        __m128i lo = _mm256_castsi256_si128(acc);
        __m128i hi = _mm256_extracti128_si256(acc, 1);
        lo = _mm_add_epi64(lo, hi);
        int64_t s = (int64_t)(_mm_extract_epi64(lo, 0) + _mm_extract_epi64(lo, 1));
        for (; i < n; i++) s += v[i];
        *out   = s;
        *count = n;
    } else {
        int64_t s = 0; uint64_t cnt = 0;
        for (size_t i = 0; i < n; i++)
            if (valid(null_bitmap, i)) { s += v[i]; cnt++; }
        *out   = s;
        *count = cnt;
    }
    return TSDB_OK;
}

/* int64 min/max: scalar (no AVX2 instruction for signed i64 min/max) */
int tsdb_agg_min_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    int64_t m = INT64_MAX;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] < m) m = v[i];
    *out = m;
    return TSDB_OK;
}

int tsdb_agg_max_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    int64_t m = INT64_MIN;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] > m) m = v[i];
    *out = m;
    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * NEON implementation
 * --------------------------------------------------------------------- */
#elif defined(TSDB_SIMD_NEON)

/* Horizontal sum of float64x2_t */
static inline double hsum_f64x2(float64x2_t v) {
    return vgetq_lane_f64(v, 0) + vgetq_lane_f64(v, 1);
}

int tsdb_agg_sum_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;

    if (!null_bitmap) {
        float64x2_t acc0 = vdupq_n_f64(0.0);
        float64x2_t acc1 = vdupq_n_f64(0.0);
        float64x2_t acc2 = vdupq_n_f64(0.0);
        float64x2_t acc3 = vdupq_n_f64(0.0);
        size_t i = 0;
        for (; i + 7 < n; i += 8) {
            acc0 = vaddq_f64(acc0, vld1q_f64(v + i + 0));
            acc1 = vaddq_f64(acc1, vld1q_f64(v + i + 2));
            acc2 = vaddq_f64(acc2, vld1q_f64(v + i + 4));
            acc3 = vaddq_f64(acc3, vld1q_f64(v + i + 6));
        }
        for (; i + 1 < n; i += 2)
            acc0 = vaddq_f64(acc0, vld1q_f64(v + i));
        acc0 = vaddq_f64(vaddq_f64(acc0, acc1), vaddq_f64(acc2, acc3));
        double s = hsum_f64x2(acc0);
        if (i < n) s += v[i];   /* tail (odd element) */
        *out   = s;
        *count = n;
    } else {
        double s = 0.0; uint64_t cnt = 0;
        for (size_t i = 0; i < n; i++)
            if (valid(null_bitmap, i)) { s += v[i]; cnt++; }
        *out   = s;
        *count = cnt;
    }
    return TSDB_OK;
}

int tsdb_agg_min_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    if (n == 0) { *out = (double)INFINITY; return TSDB_OK; }

    if (!null_bitmap) {
        float64x2_t mn = vdupq_n_f64((double)INFINITY);
        size_t i = 0;
        for (; i + 1 < n; i += 2)
            mn = vminq_f64(mn, vld1q_f64(v + i));
        double m = fmin(vgetq_lane_f64(mn, 0), vgetq_lane_f64(mn, 1));
        if (i < n && v[i] < m) m = v[i];
        *out = m;
    } else {
        double m = (double)INFINITY;
        for (size_t i = 0; i < n; i++)
            if (valid(null_bitmap, i) && v[i] < m) m = v[i];
        *out = m;
    }
    return TSDB_OK;
}

int tsdb_agg_max_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    if (n == 0) { *out = -(double)INFINITY; return TSDB_OK; }

    if (!null_bitmap) {
        float64x2_t mx = vdupq_n_f64(-(double)INFINITY);
        size_t i = 0;
        for (; i + 1 < n; i += 2)
            mx = vmaxq_f64(mx, vld1q_f64(v + i));
        double m = fmax(vgetq_lane_f64(mx, 0), vgetq_lane_f64(mx, 1));
        if (i < n && v[i] > m) m = v[i];
        *out = m;
    } else {
        double m = -(double)INFINITY;
        for (size_t i = 0; i < n; i++)
            if (valid(null_bitmap, i) && v[i] > m) m = v[i];
        *out = m;
    }
    return TSDB_OK;
}

int tsdb_agg_sum_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;

    if (!null_bitmap) {
        int64x2_t acc0 = vdupq_n_s64(0);
        int64x2_t acc1 = vdupq_n_s64(0);
        size_t i = 0;
        for (; i + 3 < n; i += 4) {
            acc0 = vaddq_s64(acc0, vld1q_s64(v + i + 0));
            acc1 = vaddq_s64(acc1, vld1q_s64(v + i + 2));
        }
        for (; i + 1 < n; i += 2)
            acc0 = vaddq_s64(acc0, vld1q_s64(v + i));
        acc0 = vaddq_s64(acc0, acc1);
        int64_t s = vgetq_lane_s64(acc0, 0) + vgetq_lane_s64(acc0, 1);
        if (i < n) s += v[i];
        *out   = s;
        *count = n;
    } else {
        int64_t s = 0; uint64_t cnt = 0;
        for (size_t i = 0; i < n; i++)
            if (valid(null_bitmap, i)) { s += v[i]; cnt++; }
        *out   = s;
        *count = cnt;
    }
    return TSDB_OK;
}

/* Scalar i64 min/max (vminq_s64 requires ARMv8.4) */
int tsdb_agg_min_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    int64_t m = INT64_MAX;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] < m) m = v[i];
    *out = m;
    return TSDB_OK;
}

int tsdb_agg_max_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    int64_t m = INT64_MIN;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] > m) m = v[i];
    *out = m;
    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * Scalar fallback
 * --------------------------------------------------------------------- */
#else /* TSDB_SIMD_SCALAR */

int tsdb_agg_sum_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;
    double s = 0.0; uint64_t cnt = 0;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i)) { s += v[i]; cnt++; }
    *out = s; *count = cnt;
    return TSDB_OK;
}

int tsdb_agg_min_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    double m = (double)INFINITY;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] < m) m = v[i];
    *out = m;
    return TSDB_OK;
}

int tsdb_agg_max_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    double m = -(double)INFINITY;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] > m) m = v[i];
    *out = m;
    return TSDB_OK;
}

int tsdb_agg_sum_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out, uint64_t *count) {
    if (!v || !out || !count) return TSDB_ERR_INVAL;
    int64_t s = 0; uint64_t cnt = 0;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i)) { s += v[i]; cnt++; }
    *out = s; *count = cnt;
    return TSDB_OK;
}

int tsdb_agg_min_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    int64_t m = INT64_MAX;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] < m) m = v[i];
    *out = m;
    return TSDB_OK;
}

int tsdb_agg_max_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out) {
    if (!v || !out) return TSDB_ERR_INVAL;
    int64_t m = INT64_MIN;
    for (size_t i = 0; i < n; i++)
        if (valid(null_bitmap, i) && v[i] > m) m = v[i];
    *out = m;
    return TSDB_OK;
}

#endif /* dispatch */
