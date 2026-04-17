/* schema.h — table schema structure and persistence.
 *
 * Schema is persisted as <dir>/schema.bin.
 * SYMBOL column dictionaries are persisted as <dir>/<col>.sym.
 */
#ifndef TSDB_STORAGE_SCHEMA_H
#define TSDB_STORAGE_SCHEMA_H

#include "../../include/tsdb.h"
#include "../core/types.h"
#include "../core/symbol.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Per-column metadata. */
typedef struct {
    char            name[TSDB_MAX_NAME + 1];
    tsdb_type_t     type;
    tsdb_symtab_t  *symtab;   /* non-NULL only for SYMBOL columns */
} tsdb_col_info_t;

/* Table schema. */
typedef struct {
    char             name[TSDB_MAX_NAME + 1];
    tsdb_col_info_t *cols;
    int              ncols;
    int              ts_col_idx;   /* index of designated timestamp column */
    char            *dir;          /* table directory (heap-allocated) */
} tsdb_schema_t;

/*
 * Create a new schema on disk.
 *
 * dir     - directory for the table (created if absent)
 * name    - table name
 * cols    - column descriptors (from public API)
 * ncols   - number of columns
 * ts_col  - name of the designated timestamp column
 * out     - populated with newly created schema on success
 *
 * Returns TSDB_OK or negative error.
 */
int tsdb_schema_create(const char *dir, const char *name,
                       const tsdb_col_t *cols, int ncols,
                       const char *ts_col,
                       tsdb_schema_t **out);

/*
 * Open an existing schema from disk.
 *
 * dir - table directory (must contain schema.bin)
 * out - populated on success
 *
 * Returns TSDB_OK or negative error.
 */
int tsdb_schema_open(const char *dir, tsdb_schema_t **out);

/* Free a schema object (closes symbol tables, releases memory). */
void tsdb_schema_free(tsdb_schema_t *s);

/* Return column index by name, or -1 if not found. */
int tsdb_schema_col_idx(tsdb_schema_t *s, const char *name);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_SCHEMA_H */
