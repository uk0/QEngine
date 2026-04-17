/* bucket.h — Time-bucket (SAMPLE BY) primitives.
 *
 * Assigns each timestamp to a bucket and aggregates values per bucket.
 * Assumes input timestamps are monotonically non-decreasing (sorted),
 * which is guaranteed for tick/metric data stored in TSDB.
 */
#ifndef TSDB_EXEC_BUCKET_H
#define TSDB_EXEC_BUCKET_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Assign bucket IDs for an array of timestamps.
 *
 *   bucket_id[i] = (ts[i] - origin) / bucket_ns
 *
 * Fast path: when origin == 0 and bucket_ns is a power of two,
 * computes bucket_id via right-shift.
 *
 * Requires: bucket_ns > 0.
 * Returns TSDB_OK or TSDB_ERR_INVAL.
 */
int tsdb_bucket_assign(const int64_t *ts, size_t n,
                       int64_t origin, int64_t bucket_ns,
                       int64_t *bucket_id);

/* Per-bucket sum result */
typedef struct {
    int64_t bucket;   /* bucket ID */
    double  sum;      /* sum of values in bucket */
    int64_t count;    /* number of valid rows in bucket */
} tsdb_bucket_f64_t;

/* Run-length merge of pre-bucketed (monotone) data.
 *
 * Requires bucket_id to be monotonically non-decreasing (i.e. produced
 * by tsdb_bucket_assign on sorted timestamps).
 *
 * Writes up to out_cap distinct buckets into out[].
 * *out_n receives the number written.
 * Returns TSDB_ERR_OVERFLOW if distinct buckets > out_cap.
 */
int tsdb_bucket_sum_f64(const int64_t *bucket_id, const double *values,
                        size_t n,
                        tsdb_bucket_f64_t *out, size_t out_cap,
                        size_t *out_n);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_BUCKET_H */
