/* exec.h — QTL query executor.
 *
 * Drives the pipeline from a parsed AST to a materialized result. The result
 * is a column-oriented buffer that the caller iterates row-by-row through the
 * tsdb_result_* API.
 */
#ifndef TSDB_QUERY_EXEC_H
#define TSDB_QUERY_EXEC_H

#include "ast.h"
#include "../../include/tsdb.h"

int tsdb_query_exec(tsdb_db_t *db, qast_query_t *q, tsdb_result_t **out,
                    char *err, size_t errcap);

/* Toggle parallel scan (1=on default, 0=serial). Thread-safe: set before query. */
void tsdb_set_query_parallel(int on);

/* Resize the global query thread pool to n workers (n<=0 = hardware concurrency).
 * Thread-safe. Takes effect for all subsequent queries. */
void tsdb_set_query_pool_size(int n);

#endif
