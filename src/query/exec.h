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

/* Bloom filter block-skip statistics from the most recent serial SELECT.
 * Skipped = blocks where SYMBOL bloom filter said "definitely not here".
 * Total   = disk blocks examined by the serial scan loop (before bloom check). */
uint64_t tsdb_bloom_stats_skipped(void);
uint64_t tsdb_bloom_stats_total(void);

/* Per-query deadline (CLOCK_MONOTONIC nanoseconds).  Set before calling
 * tsdb_query() to bound execution time; the executor checks at block
 * boundaries inside its hot loops and returns TSDB_ERR_TIMEOUT when
 * exceeded.  A value of 0 (default) disables the deadline.
 *
 * Thread-local: the deadline applies only to the calling thread, so
 * server.c can set it per request without leaking into other in-flight
 * queries.  Returns the previous value so server-side code can
 * save/restore around nested queries. */
int64_t tsdb_query_set_deadline_ns(int64_t deadline_monotonic_ns);

/* Check if the deadline has elapsed.  Returns 1 if expired (caller
 * should abort with TSDB_ERR_TIMEOUT), 0 otherwise.  Cheap: clock_gettime
 * + compare; intended to be called at block boundaries (every 8K-ish
 * rows) rather than per row. */
int tsdb_query_deadline_expired(void);

#endif
