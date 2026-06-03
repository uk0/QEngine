/* tdigest.h — T-digest approximate quantile sketch (Dunning 2014).
 *
 * Implements the "merging" variant:
 *   - centroids[] sorted by mean; each has a weight.
 *   - Adds to an unmerged buffer; compacts when buffer > COMPRESS_THRESH.
 *   - On compaction: sort all centroids + buffer, then single-pass merge with
 *     the Dunning weight-limit  k(q, delta) = 4 * total * q * (1-q) / delta.
 *
 * Also tracks exact sum / sum_sq / count for cheap mean & stddev.
 *
 * Thread-safety: NONE.  Each thread should own its own digest;
 * use tsdb_tdigest_merge() after parallel passes.
 */
#ifndef TSDB_EXEC_TDIGEST_H
#define TSDB_EXEC_TDIGEST_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_tdigest tsdb_tdigest_t;

/* Create a new digest.
 *   delta   — accuracy/size tradeoff (default 100; smaller = more centroids = more accurate).
 * Returns 0 on success, -1 on allocation failure. */
int  tsdb_tdigest_new(double delta, tsdb_tdigest_t **out);

/* Create a stats-only digest: tracks sum/sum_sq/total for mean & stddev with
 * no centroids (cheap alloc, tight add loop).  Quantiles undefined on it. */
int  tsdb_tdigest_new_stats(tsdb_tdigest_t **out);

/* Create a copy of an existing digest (used for parallel worker init). */
int  tsdb_tdigest_clone(const tsdb_tdigest_t *src, tsdb_tdigest_t **out);

void tsdb_tdigest_free(tsdb_tdigest_t *t);

/* Add a single value. */
void tsdb_tdigest_add(tsdb_tdigest_t *t, double value);

/* Add an array of doubles.
 *   null_bitmap: MSB-first byte array (bit i = byte[i>>3] bit (7-(i&7))).
 *                1 = valid, 0 = null.  NULL pointer means all valid.
 */
void tsdb_tdigest_add_bulk(tsdb_tdigest_t *t, const double *v, size_t n,
                           const uint8_t *null_bitmap);

/* Merge other into self.  other is NOT modified. */
void tsdb_tdigest_merge(tsdb_tdigest_t *self, const tsdb_tdigest_t *other);

/* Compute approximate quantile q in [0, 1].
 * Returns NaN if digest is empty. */
double tsdb_tdigest_quantile(tsdb_tdigest_t *t, double q);

/* Exact statistics (maintained in parallel with centroids). */
double tsdb_tdigest_count(const tsdb_tdigest_t *t);
double tsdb_tdigest_sum(const tsdb_tdigest_t *t);
double tsdb_tdigest_mean(const tsdb_tdigest_t *t);
double tsdb_tdigest_stddev(const tsdb_tdigest_t *t);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_TDIGEST_H */
