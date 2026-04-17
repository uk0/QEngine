/* parse.h — QTL recursive-descent parser.
 *
 * Produces a qast_query_t from a source string. All allocations come from
 * the caller-supplied arena; no reference-counted structures.
 */
#ifndef TSDB_QUERY_PARSE_H
#define TSDB_QUERY_PARSE_H

#include "ast.h"

int  qparse(const char *src, tsdb_arena_t *a, qast_query_t *out, char *err, size_t errcap);

#endif
