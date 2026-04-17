/* filter.h — Column filter primitives producing LSB-first bitmaps.
 *
 * The output bitmap (uint64 array) is LSB-first: element i is represented by
 * bit (i % 64) of word (i / 64).  Bit = 1 means "row passes".
 *
 * Design: every tsdb_filter_* function ANDs its result into out_bitmap.
 * Multiple filters can be composed by calling them in sequence; the
 * caller must memset the bitmap to 0xFF before the first filter.
 *
 * Bitmap size: at least (n + 63) / 64 uint64_t words.
 * Bits past position n are not written; callers must mask them if needed.
 */
#ifndef TSDB_EXEC_FILTER_H
#define TSDB_EXEC_FILTER_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Comparison operators */
typedef enum {
    TSDB_CMP_EQ = 0,  /* == */
    TSDB_CMP_NE = 1,  /* != */
    TSDB_CMP_LT = 2,  /* <  */
    TSDB_CMP_LE = 3,  /* <= */
    TSDB_CMP_GT = 4,  /* >  */
    TSDB_CMP_GE = 5   /* >= */
} tsdb_cmp_t;

/* --- Scalar comparison filters --- */

int tsdb_filter_f64(const double *v, size_t n, tsdb_cmp_t op, double rhs,
                    uint64_t *out_bitmap);
int tsdb_filter_i64(const int64_t *v, size_t n, tsdb_cmp_t op, int64_t rhs,
                    uint64_t *out_bitmap);

/* --- Symbol (uint32) filters --- */

int tsdb_filter_u32_eq(const uint32_t *v, size_t n, uint32_t rhs,
                       uint64_t *out_bitmap);
int tsdb_filter_u32_in(const uint32_t *v, size_t n,
                       const uint32_t *set, size_t nset,
                       uint64_t *out_bitmap);

/* --- Bitmap utilities --- */

/* Count set bits in first n_bits bits of bm.
 * Masks the partial last word so stale high bits are excluded. */
uint64_t tsdb_bitmap_popcount(const uint64_t *bm, size_t n_bits);

/* Gather: copy elements from src where bm bit is 1 into dst (dense).
 * Returns number of elements written. */
size_t tsdb_bitmap_gather_f64(const uint64_t *bm, const double *src,
                               size_t n, double *dst);
size_t tsdb_bitmap_gather_i64(const uint64_t *bm, const int64_t *src,
                               size_t n, int64_t *dst);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_FILTER_H */
