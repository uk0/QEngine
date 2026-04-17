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

#endif
