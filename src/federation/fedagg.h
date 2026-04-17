/* fedagg.h — federation result aggregator.
 *
 * Merges partial tsdb_result_t objects from multiple clusters into a
 * single final tsdb_result_t.
 *
 * Supported merge strategies (auto-detected by column name conventions):
 *   count(*) / count(col) — sum the int64 values
 *   __fed_sum  / __fed_cnt — compute final avg = sum/count
 *   min / max  — element-wise min / max
 *   SAMPLE BY  — key-join on bucket timestamp, then merge per bucket
 *   LIMIT N    — collect all, sort by ts, take first N
 *   raw rows   — union / sort by ts
 */
#ifndef TSDB_FEDAGG_H
#define TSDB_FEDAGG_H

#include "../../include/tsdb.h"
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Merge an array of partial results (from individual clusters) into one.
 * partial[i] may be NULL if cluster i timed out (partial_ok[i] = false).
 *
 * is_sample_by    - true if query has SAMPLE BY (bucket merge by ts key)
 * limit           - 0 = no limit; > 0 = take first N rows after merge
 * has_avg_rewrite - true if AVG was rewritten to __fed_sum + __fed_cnt
 *
 * On success writes *out and returns TSDB_OK.
 * If some partials were NULL, still succeeds but sets *partial_miss_count > 0.
 */
int fedagg_merge(tsdb_result_t **partial, bool *partial_ok,
                 int npartial,
                 bool is_sample_by,
                 int64_t limit,
                 bool has_avg_rewrite,
                 tsdb_result_t **out,
                 int *partial_miss_count);

/*
 * Allocate a new empty tsdb_result_t with the given column schema.
 * col_names / col_types must remain valid until fedagg_result_free().
 */
tsdb_result_t *fedagg_result_alloc(int ncols,
                                   const char **col_names,
                                   const tsdb_type_t *col_types);

/* Append one row (values array, one per col, cast to uint64_t bits). */
int fedagg_result_append(tsdb_result_t *r, const uint64_t *vals);

/* Free a result created by fedagg_result_alloc / fedagg_merge. */
void fedagg_result_free(tsdb_result_t *r);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_FEDAGG_H */
