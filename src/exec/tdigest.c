/* tdigest.c — Merging T-digest implementation.
 *
 * Algorithm:
 *   Points arrive into an unmerged ring buffer (size BUFFER_CAP).
 *   When the buffer is full (or tsdb_tdigest_quantile is called), we compact:
 *     1. Concatenate centroids[] and unmerged buffer as single centroid list.
 *     2. Sort by mean (qsort).
 *     3. Single forward pass: for each incoming centroid c, if merging c into
 *        the current running centroid would keep total weight ≤ weight_limit
 *        for that quantile position, merge; else start a new centroid.
 *        weight_limit = 4 * total_n * q * (1-q) / delta
 *        where q = cumulative fraction at that point.
 *   After compaction centroids[] is sorted and trimmed.
 *
 * Exact fields (sum, sum_sq, count) are updated on every add and
 * used for mean / stddev — no approximation there.
 */

#include "tdigest.h"

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>

/* ---- tunables ----------------------------------------------------------- */

/* Unmerged input buffer capacity (multiples of delta worth of centroids). */
#define BUFFER_CAP_MULT  10

/* Maximum number of centroids after full compaction.
 * Dunning's analysis: the number of centroids ≤ ceil(delta * pi / 2) ≈ 1.6*delta.
 * We allocate 2*delta as a safe upper bound. */
#define MAX_CENTROIDS_MULT 2

/* Minimum number of buffer entries before we bother compacting. */
#define COMPRESS_MIN 64

/* ---- types -------------------------------------------------------------- */

typedef struct {
    double mean;
    double weight;
} centroid_t;

struct tsdb_tdigest {
    double delta;

    /* Sorted, compacted centroids. */
    centroid_t *centroids;
    int         ncent;
    int         cent_cap;

    /* Unmerged input buffer (unsorted). */
    centroid_t *buf;
    int         nbuf;
    int         buf_cap;

    /* Total weight (= number of points added). */
    double total;

    /* Exact statistics. */
    double sum;
    double sum_sq;
};

/* ---- helpers ------------------------------------------------------------ */

static int cmp_centroid(const void *a, const void *b) {
    const centroid_t *ca = (const centroid_t *)a;
    const centroid_t *cb = (const centroid_t *)b;
    if (ca->mean < cb->mean) return -1;
    if (ca->mean > cb->mean) return  1;
    return 0;
}

/* Compute Dunning weight limit for quantile position q.
 * k(q) = 4 * total * q * (1-q) / delta
 * Clamped to >= 1 so every point gets its own centroid if needed. */
static inline double weight_limit(double total, double q, double delta) {
    double k = 4.0 * total * q * (1.0 - q) / delta;
    return k < 1.0 ? 1.0 : k;
}

/* Merge all buffered points into centroids[].
 * Strategy:
 *  1. Move everything into a temporary array (centroids + buffer).
 *  2. Sort by mean.
 *  3. Forward single-pass merge with weight limit based on current q. */
static void compact(tsdb_tdigest_t *t) {
    if (t->nbuf == 0) return;

    int total_in = t->ncent + t->nbuf;

    /* Allocate temporary merge buffer. */
    centroid_t *tmp = (centroid_t *)malloc((size_t)total_in * sizeof(centroid_t));
    if (!tmp) {
        /* On OOM: drop the buffer points (graceful degradation). */
        t->nbuf = 0;
        return;
    }

    /* Copy centroids + buffer into tmp. */
    memcpy(tmp,            t->centroids, (size_t)t->ncent * sizeof(centroid_t));
    memcpy(tmp + t->ncent, t->buf,       (size_t)t->nbuf  * sizeof(centroid_t));
    t->nbuf = 0;

    /* Sort by mean. */
    qsort(tmp, (size_t)total_in, sizeof(centroid_t), cmp_centroid);

    /* Ensure centroids[] has enough capacity for the result.
     * Upper bound: same number of inputs (worst case no merging). */
    if (total_in > t->cent_cap) {
        centroid_t *nc = (centroid_t *)realloc(t->centroids,
                                               (size_t)total_in * sizeof(centroid_t));
        if (!nc) { free(tmp); return; }
        t->centroids = nc;
        t->cent_cap  = total_in;
    }

    /* Single-pass merge. */
    double cum = 0.0; /* cumulative weight before current running centroid */
    int out = 0;
    t->centroids[0] = tmp[0];
    out = 1;

    for (int i = 1; i < total_in; i++) {
        double q_lo = cum / t->total;
        double q_hi = (cum + t->centroids[out-1].weight + tmp[i].weight) / t->total;
        double q_mid = (q_lo + q_hi) * 0.5;
        double limit = weight_limit(t->total, q_mid, t->delta);

        if (t->centroids[out-1].weight + tmp[i].weight <= limit) {
            /* Merge tmp[i] into current. */
            double w  = t->centroids[out-1].weight + tmp[i].weight;
            double m  = t->centroids[out-1].mean +
                        (tmp[i].mean - t->centroids[out-1].mean) * tmp[i].weight / w;
            t->centroids[out-1].mean   = m;
            t->centroids[out-1].weight = w;
        } else {
            cum += t->centroids[out-1].weight;
            t->centroids[out] = tmp[i];
            out++;
        }
    }
    t->ncent = out;

    free(tmp);
}

/* Ensure buffer has room for one more entry. Compact if full. */
static inline void maybe_compact(tsdb_tdigest_t *t) {
    if (t->nbuf >= t->buf_cap) compact(t);
}

/* ---- public API --------------------------------------------------------- */

int tsdb_tdigest_new(double delta, tsdb_tdigest_t **out) {
    if (delta <= 0.0) delta = 100.0;

    tsdb_tdigest_t *t = (tsdb_tdigest_t *)calloc(1, sizeof(tsdb_tdigest_t));
    if (!t) return -1;

    t->delta = delta;

    int buf_cap  = (int)(BUFFER_CAP_MULT  * delta);
    if (buf_cap  < COMPRESS_MIN) buf_cap  = COMPRESS_MIN;
    int cent_cap = (int)(MAX_CENTROIDS_MULT * delta);
    if (cent_cap < COMPRESS_MIN) cent_cap = COMPRESS_MIN;

    t->buf       = (centroid_t *)malloc((size_t)buf_cap  * sizeof(centroid_t));
    t->centroids = (centroid_t *)malloc((size_t)cent_cap * sizeof(centroid_t));
    if (!t->buf || !t->centroids) {
        free(t->buf); free(t->centroids); free(t);
        return -1;
    }

    t->buf_cap  = buf_cap;
    t->cent_cap = cent_cap;
    *out = t;
    return 0;
}

int tsdb_tdigest_clone(const tsdb_tdigest_t *src, tsdb_tdigest_t **out) {
    tsdb_tdigest_t *t = (tsdb_tdigest_t *)malloc(sizeof(tsdb_tdigest_t));
    if (!t) return -1;

    *t = *src; /* copy scalars */

    /* Deep-copy arrays. */
    t->centroids = (centroid_t *)malloc((size_t)src->cent_cap * sizeof(centroid_t));
    t->buf       = (centroid_t *)malloc((size_t)src->buf_cap  * sizeof(centroid_t));
    if (!t->centroids || !t->buf) {
        free(t->centroids); free(t->buf); free(t);
        return -1;
    }
    memcpy(t->centroids, src->centroids, (size_t)src->ncent * sizeof(centroid_t));
    memcpy(t->buf,       src->buf,       (size_t)src->nbuf  * sizeof(centroid_t));
    *out = t;
    return 0;
}

void tsdb_tdigest_free(tsdb_tdigest_t *t) {
    if (!t) return;
    free(t->centroids);
    free(t->buf);
    free(t);
}

void tsdb_tdigest_add(tsdb_tdigest_t *t, double value) {
    maybe_compact(t);

    t->buf[t->nbuf].mean   = value;
    t->buf[t->nbuf].weight = 1.0;
    t->nbuf++;
    t->total += 1.0;
    t->sum    += value;
    t->sum_sq += value * value;
}

/* null_bitmap: MSB-first, 1 = valid.  NULL means all valid. */
static inline int is_valid(const uint8_t *bm, size_t i) {
    if (!bm) return 1;
    return (bm[i >> 3] >> (7 - (i & 7))) & 1;
}

void tsdb_tdigest_add_bulk(tsdb_tdigest_t *t, const double *v, size_t n,
                           const uint8_t *null_bitmap) {
    /* Fast path: fill buffer in chunks, compacting only when full. */
    for (size_t i = 0; i < n; ) {
        /* Fill as many entries as possible without checking buf_cap each time. */
        int room = t->buf_cap - t->nbuf;
        if (room <= 0) {
            compact(t);
            room = t->buf_cap; /* after compact, buf is empty */
        }

        /* How many values can we batch before next compact? */
        int batch = (int)(n - i);
        if (batch > room) batch = room;

        if (!null_bitmap) {
            /* No nulls — direct copy into buffer. */
            for (int k = 0; k < batch; k++) {
                t->buf[t->nbuf].mean   = v[i + (size_t)k];
                t->buf[t->nbuf].weight = 1.0;
                t->nbuf++;
                t->sum    += v[i + (size_t)k];
                t->sum_sq += v[i + (size_t)k] * v[i + (size_t)k];
            }
            t->total += (double)batch;
            i += (size_t)batch;
        } else {
            /* Null-aware: check each bit. */
            for (int k = 0; k < batch; k++, i++) {
                if (!is_valid(null_bitmap, i)) continue;
                double val = v[i];
                t->buf[t->nbuf].mean   = val;
                t->buf[t->nbuf].weight = 1.0;
                t->nbuf++;
                t->total  += 1.0;
                t->sum    += val;
                t->sum_sq += val * val;
                if (t->nbuf >= t->buf_cap) {
                    compact(t);
                }
            }
        }
    }
}

void tsdb_tdigest_merge(tsdb_tdigest_t *self, const tsdb_tdigest_t *other) {
    /* Flush other's buffer into a temporary compact copy so we iterate
     * over a sorted centroid list. */
    tsdb_tdigest_t *tmp = NULL;
    if (tsdb_tdigest_clone(other, &tmp) != 0) return;
    compact(tmp);  /* ensure tmp->centroids is complete and sorted */

    /* Merge exact stats. */
    self->total  += other->total;
    self->sum    += other->sum;
    self->sum_sq += other->sum_sq;

    /* Add other's centroids into self's buffer. */
    for (int i = 0; i < tmp->ncent; i++) {
        maybe_compact(self);
        self->buf[self->nbuf++] = tmp->centroids[i];
        /* Note: do NOT update total/sum/sum_sq here — already merged above. */
    }
    /* Undo double-counting: we added centroid weights into buf but total
     * was already updated. The centroid weights are from other->total which
     * we already counted above.  Remove the per-centroid increments. */
    /* Actually: tsdb_tdigest_add does NOT go through the buf path for merge —
     * we push raw centroids with their original weights.  The buf entries will
     * be compacted correctly because the weight field reflects the original
     * sub-digest's counts.  We must NOT update total again, which we didn't. */

    tsdb_tdigest_free(tmp);
}

double tsdb_tdigest_quantile(tsdb_tdigest_t *t, double q) {
    if (t->total <= 0.0) return 0.0 / 0.0; /* NaN */
    if (q <= 0.0) q = 0.0;
    if (q >= 1.0) q = 1.0;

    compact(t); /* ensure centroids are up to date */

    if (t->ncent == 0) return 0.0 / 0.0;
    if (t->ncent == 1) return t->centroids[0].mean;

    /* Target cumulative count. */
    double target = q * t->total;

    /* Interpolate between centroid means.
     * Convention: the left edge of centroid[0] is at 0,
     *             the right edge of centroid[0] is at centroids[0].weight/2,
     *             and so on. */

    /* Cumulative weight at left edge of centroid i = sum of weights[0..i-1] + weight[i]/2 */
    double cum = 0.0;

    /* Handle left tail. */
    if (target <= t->centroids[0].weight / 2.0) {
        return t->centroids[0].mean;
    }
    cum = t->centroids[0].weight / 2.0;

    for (int i = 0; i < t->ncent - 1; i++) {
        /* The center of centroid i is at cum;
         * the center of centroid i+1 is at cum + (w[i]+w[i+1])/2. */
        double step = (t->centroids[i].weight + t->centroids[i+1].weight) / 2.0;
        if (target <= cum + step) {
            /* Interpolate between means[i] and means[i+1]. */
            double frac = (target - cum) / step;
            return t->centroids[i].mean +
                   frac * (t->centroids[i+1].mean - t->centroids[i].mean);
        }
        cum += step;
    }

    /* Right tail. */
    return t->centroids[t->ncent - 1].mean;
}

double tsdb_tdigest_count(const tsdb_tdigest_t *t) {
    return t->total;
}

double tsdb_tdigest_sum(const tsdb_tdigest_t *t) {
    return t->sum;
}

double tsdb_tdigest_mean(const tsdb_tdigest_t *t) {
    if (t->total <= 0.0) return 0.0 / 0.0;
    return t->sum / t->total;
}

double tsdb_tdigest_stddev(const tsdb_tdigest_t *t) {
    if (t->total < 2.0) return 0.0;
    /* Population stddev via Welford-equivalent one-pass formula:
     * E[X^2] - (E[X])^2 — but using sum_sq is numerically acceptable
     * for our use case (values are typically in known ranges). */
    double mean  = t->sum / t->total;
    double var   = t->sum_sq / t->total - mean * mean;
    if (var < 0.0) var = 0.0;   /* clamp floating-point rounding */
    return sqrt(var);
}
