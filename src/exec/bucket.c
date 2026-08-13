/* bucket.c — Time-bucket assignment and aggregation.
 *
 * tsdb_bucket_assign: vectorised where possible (NEON/AVX2), with
 *   a scalar fallback and a power-of-two right-shift fast path.
 *
 * tsdb_bucket_sum_f64: single-pass run-length merge on pre-sorted
 *   bucket IDs.  O(n) time, O(distinct_buckets) space.
 */
#include "bucket.h"
#include "simd.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* -----------------------------------------------------------------------
 * Helpers
 * --------------------------------------------------------------------- */

/* Is x a non-zero power of two? */
static inline int is_pow2(int64_t x) {
    return x > 0 && (x & (x - 1)) == 0;
}

/* Floor division by a POSITIVE divisor.
 *
 * C's `/` truncates toward zero, so -1/4 is 0 while the bucket containing
 * -1 is bucket -1.  A bucket is the half-open interval [k*d, (k+1)*d), which
 * is floor division; the power-of-two path below reaches it with an
 * arithmetic right shift, and this is the same answer for any d.
 *
 * d > 0 is a precondition (tsdb_bucket_assign rejects bucket_ns <= 0), so the
 * remainder's sign is the dividend's: a negative dividend that does not divide
 * exactly truncated UP by one, and is corrected back down here. */
static inline int64_t floor_div_pos(int64_t a, int64_t d) {
    int64_t q = a / d;
    if (a % d != 0 && a < 0) q--;
    return q;
}

/* Count trailing zeros for 64-bit (portable) */
static inline unsigned ctz64(uint64_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return (unsigned)__builtin_ctzll(x);
#else
    unsigned n = 0;
    while (!(x & 1)) { n++; x >>= 1; }
    return n;
#endif
}

/* -----------------------------------------------------------------------
 * tsdb_bucket_assign
 * --------------------------------------------------------------------- */

int tsdb_bucket_assign(const int64_t *ts, size_t n,
                       int64_t origin, int64_t bucket_ns,
                       int64_t *bucket_id) {
    if (!ts || !bucket_id || bucket_ns <= 0) return TSDB_ERR_INVAL;

    /* Fast path: origin == 0 and bucket_ns is a power of two → right shift.
     * >> on a negative signed value is implementation-defined in C11; clang
     * and gcc — the only compilers this project builds with — define it as an
     * arithmetic shift, which rounds toward negative infinity.  That is the
     * same floor the general path below computes. */
    if (origin == 0 && is_pow2(bucket_ns)) {
        unsigned shift = ctz64((uint64_t)bucket_ns);

        for (size_t i = 0; i < n; i++)
            bucket_id[i] = ts[i] >> shift;
        return TSDB_OK;
    }

    /* General path: floor division, so a pre-1970 timestamp gets the same
     * bucket here as it would on the shift path above. */
    for (size_t i = 0; i < n; i++)
        bucket_id[i] = floor_div_pos(ts[i] - origin, bucket_ns);

    return TSDB_OK;
}

/* -----------------------------------------------------------------------
 * tsdb_bucket_sum_f64
 * --------------------------------------------------------------------- */

int tsdb_bucket_sum_f64(const int64_t *bucket_id, const double *values,
                        size_t n,
                        tsdb_bucket_f64_t *out, size_t out_cap,
                        size_t *out_n) {
    if (!bucket_id || !values || !out || !out_n) return TSDB_ERR_INVAL;
    *out_n = 0;
    if (n == 0) return TSDB_OK;

    size_t nb = 0;   /* number of distinct buckets emitted */
    int64_t cur_bid = bucket_id[0];
    double  cur_sum = values[0];
    int64_t cur_cnt = 1;

    for (size_t i = 1; i < n; i++) {
        if (bucket_id[i] == cur_bid) {
            cur_sum += values[i];
            cur_cnt++;
        } else {
            if (nb >= out_cap) return TSDB_ERR_OVERFLOW;
            out[nb].bucket = cur_bid;
            out[nb].sum    = cur_sum;
            out[nb].count  = cur_cnt;
            nb++;
            cur_bid = bucket_id[i];
            cur_sum = values[i];
            cur_cnt = 1;
        }
    }
    /* Flush last bucket */
    if (nb >= out_cap) return TSDB_ERR_OVERFLOW;
    out[nb].bucket = cur_bid;
    out[nb].sum    = cur_sum;
    out[nb].count  = cur_cnt;
    nb++;

    *out_n = nb;
    return TSDB_OK;
}
