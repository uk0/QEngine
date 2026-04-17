/* result_internal.h — internal layout of tsdb_result.
 *
 * This header is NOT part of the public API.  It is included by:
 *   src/query/exec.c  — creates and owns results from local queries
 *   src/federation/fedagg.c — creates results from merged partial results
 *
 * Both share the same struct layout so that tsdb_result_free() (defined once
 * in exec.c) can free either kind of result.
 */
#ifndef TSDB_RESULT_INTERNAL_H
#define TSDB_RESULT_INTERNAL_H

#include "../../include/tsdb.h"
#include "../core/symbol.h"
#include <stddef.h>
#include <sys/types.h>

struct tsdb_result {
    int           ncols;
    char        **col_names;
    tsdb_type_t  *col_types;

    /* For SYMBOL columns, col_symtab[i] is used to decode uint32 → string.
     * Federation-created results always have col_symtab = NULL. */
    tsdb_symtab_t **col_symtab;
    void         **col_data;
    size_t         nrows;
    size_t         cap_rows;

    ssize_t        cur;

    /* Owned symtabs: allocated by DDL (LIST) results; freed by result_free.
     * Regular SELECT results reference schema-owned symtabs and set this NULL. */
    tsdb_symtab_t **owned_symtabs;
    int             n_owned_symtabs;
};

#endif /* TSDB_RESULT_INTERNAL_H */
