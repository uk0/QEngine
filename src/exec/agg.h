/* agg.h — Column aggregate primitives (sum, min, max, count).
 *
 * All functions accept a null_bitmap (MSB-first byte array).
 *   - null_bitmap == NULL  →  every element is treated as valid.
 *   - Bit i = 1 (VALID), bit i = 0 (NULL).  Null elements are skipped.
 *
 * Outputs are REPLACED (not accumulated): *out and *count receive the
 * result for this call only.  Callers must initialise state to zero before
 * the first block and accumulate across blocks themselves.
 *
 * Conventions for degenerate inputs:
 *   sum   : *out = 0,  *count = 0    when n == 0 or all null
 *   min   : *out = +INFINITY (f64) / INT64_MAX (i64) when all null / n == 0
 *   max   : *out = -INFINITY (f64) / INT64_MIN (i64) when all null / n == 0
 *
 * Returns TSDB_OK (0) on success or a negative tsdb_err_t on bad args.
 */
#ifndef TSDB_EXEC_AGG_H
#define TSDB_EXEC_AGG_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* --- float64 --- */

int tsdb_agg_sum_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out, uint64_t *count);
int tsdb_agg_min_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out);
int tsdb_agg_max_f64(const double *v, size_t n, const uint8_t *null_bitmap,
                     double *out);

/* --- int64 --- */

int tsdb_agg_sum_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out, uint64_t *count);
int tsdb_agg_min_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out);
int tsdb_agg_max_i64(const int64_t *v, size_t n, const uint8_t *null_bitmap,
                     int64_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_AGG_H */
