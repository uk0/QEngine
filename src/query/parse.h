/* parse.h — QTL recursive-descent parser.
 *
 * Produces a qast_query_t from a source string (legacy SELECT-only).
 * New entry point qparse_stmt produces a qast_stmt_t for SELECT + DDL.
 * All allocations come from the caller-supplied arena; no reference-counted
 * structures.
 */
#ifndef TSDB_QUERY_PARSE_H
#define TSDB_QUERY_PARSE_H

#include "ast.h"

/* Legacy SELECT-only parser; returns TSDB_ERR_PARSE for non-SELECT. */
int  qparse(const char *src, tsdb_arena_t *a, qast_query_t *out, char *err, size_t errcap);

/* Full statement parser: SELECT + CREATE/DROP/LIST GROUP/DEVICE. */
int  qparse_stmt(const char *src, tsdb_arena_t *a, qast_stmt_t *out,
                 char *err, size_t errcap);

#endif
