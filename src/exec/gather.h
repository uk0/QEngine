/* gather.h — SIMD-optimised bitmap gather for f64 / i64.
 *
 * Declared separately so other modules can include just gather.h.
 * The tsdb_bitmap_gather_* functions in filter.h now delegate here.
 */
#ifndef TSDB_EXEC_GATHER_H
#define TSDB_EXEC_GATHER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Copy src elements where bm bit is set into dst (dense output).
 * Returns number of elements written.
 * These replace the inline definitions that previously lived in filter.c. */
size_t tsdb_bitmap_gather_f64(const uint64_t *bm, const double *src,
                               size_t n, double *dst);
size_t tsdb_bitmap_gather_i64(const uint64_t *bm, const int64_t *src,
                               size_t n, int64_t *dst);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_GATHER_H */
