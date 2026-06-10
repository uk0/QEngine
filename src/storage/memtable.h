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

/*
 * Like tsdb_memtable_rows but reads the committed-row counter with a
 * relaxed atomic load instead of taking m->lock.  The value may be
 * momentarily stale (a concurrent writer's in-progress nrows++ may or
 * may not be observed), so callers must tolerate slack.  Used by the
 * aggregate-memtable budget sum in db.c's maintenance thread, where an
 * O(ntables) per-table mutex-acquire storm under db->lock was the
 * contention source and an approximate sum is sufficient.
 */
size_t tsdb_memtable_rows_relaxed(tsdb_memtable_t *m);

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
 * Atomically snapshot the memtable's per-column data for a concurrent
 * reader.  Holds m->lock for the duration of the copy, so the result
 * is a self-consistent point-in-time view of (nrows, all column data)
 * even while other writers append rows or trigger flush-and-reuse.
 *
 * out_bufs[c] is malloc'd and returned to the caller — caller frees
 * each non-NULL entry when done.  out_bufs[] itself is owned by the
 * caller.  Returns TSDB_OK on success and writes the snapshotted
 * row count to *out_nrows; on failure (alloc), any partial bufs
 * already filled are freed and *out_nrows is set to 0.
 *
 * Caller may pass NULL out_bufs[c] for columns it does not need.
 * Caller's out_bufs[] must have at least m->schema->ncols slots.
 *
 * Used by the query executor's scan_plan_push to take a stable
 * memtable snapshot before iteration — closes the F2 read-during-
 * flush-and-reuse torn-row race documented in
 * docs/tasks/perf-bcd-2026-05-08.md.
 */
int tsdb_memtable_snapshot(tsdb_memtable_t *m,
                            void **out_bufs,
                            size_t *out_nrows);

/*
 * Notify the memtable that a new trailing column has been appended to the
 * schema.  Allocates its column buffer and extends col_set.
 *
 * The caller must ensure the memtable is empty (nrows==0) and no row is in
 * progress — otherwise TSDB_ERR_INVAL is returned.
 */
int tsdb_memtable_extend_for_new_column(tsdb_memtable_t *m);

/*
 * Fill out_idx with the memtable's row positions in ascending ts-order.
 * out_idx must have capacity for at least tsdb_memtable_rows(m) entries.
 * Ties on ts are resolved stably by insertion order.
 *
 * When the skip-list is unavailable (e.g. allocation failed, or the
 * memtable is a legacy variant), falls back to writing insertion order.
 *
 * Returns TSDB_OK on success or negative error on bad arguments.
 */
int tsdb_memtable_sorted_indices(tsdb_memtable_t *m, size_t *out_idx);

/*
 * Fast-path signal: returns non-zero if every insert since the last
 * clear arrived in non-decreasing ts order.  Callers can skip the
 * sorted_indices call entirely in that case and use the identity
 * permutation.  Always returns 1 before the first row is inserted.
 */
int tsdb_memtable_is_sorted(tsdb_memtable_t *m);

/*
 * Bulk columnar append — pushes `n` rows into the memtable in a single
 * critical section.  Replaces ~3-5 lock acquires per row (begin / end +
 * skiplist) with a single one for the whole batch, plus per-column
 * memcpy instead of per-cell scalar writes.  WRITE_BATCH receivers
 * (cluster RPC, public wire proto, federation ingest) call this on
 * the hot path; per-row API stays for legacy callers.
 *
 *   ts_arr    — n int64 timestamps (in schema's ts column position).
 *   col_arrs  — one entry per non-ts column in schema order:
 *                 INT64 / FLOAT64 / TIMESTAMP : n × 8 bytes raw
 *                 SYMBOL                       : packed wire format
 *                                                [u32 total][u16 len][bytes]…
 *   col_types — type per non-ts column (parallel to col_arrs).
 *   ncols_data — length of col_arrs / col_types (= schema->ncols - 1).
 *   n         — number of rows to append.
 *
 * Returns TSDB_OK on success.  TSDB_ERR_FULL if the resulting nrows
 * would exceed block_points.  TSDB_ERR_NOMEM on intern failure.  On
 * any error, no partial rows are visible (atomic-or-nothing).
 */
/* ts_arr is `const void *` so the caller can pass a possibly-misaligned
 * pointer (e.g. wire-protocol slice after a 4-byte header).  We memcpy
 * out int64 values internally; the casts at the callsite are no longer
 * UB on strict-alignment ABIs.  See UBSAN finding 2026-05-08. */
int tsdb_memtable_append_bulk(tsdb_memtable_t *m,
                               const void *ts_arr,
                               const void * const *col_arrs,
                               const int *col_types,
                               int ncols_data,
                               size_t n);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_MEMTABLE_H */
