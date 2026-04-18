/* memtable.h — in-memory columnar write buffer.
 *
 * Columnar layout: each column has a contiguous array of TSDB_BLOCK_POINTS
 * values. Protected internally by a mutex.
 *
 * Typical usage:
 *   tsdb_memtable_row_begin(m);
 *   tsdb_memtable_row_ts(m, ts);          // must set ts col
 *   tsdb_memtable_row_i64(m, col, v);
 *   tsdb_memtable_row_f64(m, col, v);
 *   tsdb_memtable_row_sym(m, col, s);
 *   tsdb_memtable_row_end(m);             // validates all cols were set
 */
#ifndef TSDB_STORAGE_MEMTABLE_H
#define TSDB_STORAGE_MEMTABLE_H

#include "schema.h"
#include "../../include/tsdb.h"
#include "../core/types.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_memtable tsdb_memtable_t;

/* Create a new empty memtable for the given schema. */
int  tsdb_memtable_new(tsdb_schema_t *s, tsdb_memtable_t **out);

/* Free and release all resources. */
void tsdb_memtable_free(tsdb_memtable_t *m);

/* Begin writing a new row. Must be called before any row_* calls. */
int  tsdb_memtable_row_begin(tsdb_memtable_t *m);

/* Set the timestamp for the current row. */
int  tsdb_memtable_row_ts(tsdb_memtable_t *m, tsdb_ts_t v);

/* Set an INT64 column value. */
int  tsdb_memtable_row_i64(tsdb_memtable_t *m, int col, int64_t v);

/* Set a FLOAT64 column value. */
int  tsdb_memtable_row_f64(tsdb_memtable_t *m, int col, double v);

/* Set a SYMBOL column value (string; will be interned). */
int  tsdb_memtable_row_sym(tsdb_memtable_t *m, int col, const char *s);

/*
 * Finish the current row. All columns must have been set since row_begin.
 * Returns TSDB_ERR_SCHEMA if any column was not set.
 */
int  tsdb_memtable_row_end(tsdb_memtable_t *m);

/* Return the number of committed rows. */
size_t tsdb_memtable_rows(tsdb_memtable_t *m);

/* Returns non-zero if the memtable has reached TSDB_BLOCK_POINTS rows. */
int  tsdb_memtable_is_full(tsdb_memtable_t *m);

/* Clear all rows (prepare for reuse after flush). */
void tsdb_memtable_clear(tsdb_memtable_t *m);

/*
 * Abort an in-progress row (reset in_row and col_set state).
 * Safe to call even when no row is in progress.
 */
void tsdb_memtable_row_abort(tsdb_memtable_t *m);

/*
 * Return a read-only pointer to the contiguous value array for the
 * given column. Valid until next write operation.
 */
const void *tsdb_memtable_col(tsdb_memtable_t *m, int col);

/*
 * Notify the memtable that a new trailing column has been appended to the
 * schema.  Allocates its column buffer and extends col_set.
 *
 * The caller must ensure the memtable is empty (nrows==0) and no row is in
 * progress — otherwise TSDB_ERR_INVAL is returned.
 */
int tsdb_memtable_extend_for_new_column(tsdb_memtable_t *m);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_MEMTABLE_H */
