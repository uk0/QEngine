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

/* Partition granularity. Chosen at CREATE TABLE time; fixed for the life
 * of the table. DAY = YYYYMMDD subdirs, HOUR = YYYYMMDDHH subdirs. */
typedef enum {
    TSDB_PARTITION_DAY  = 0,   /* default; legacy v1 schemas always decode as DAY */
    TSDB_PARTITION_HOUR = 1
} tsdb_partition_unit_t;

/* Table schema. */
typedef struct {
    char                   name[TSDB_MAX_NAME + 1];
    tsdb_col_info_t       *cols;
    int                    ncols;
    int                    ts_col_idx;      /* index of designated timestamp column */
    char                  *dir;              /* table directory (heap-allocated) */
    tsdb_partition_unit_t  partition_unit;   /* DAY or HOUR */
    /* Per-table block size (points/rows per flushed block).  Controls both
     * the memtable capacity before auto-flush and the chunk size in part.c's
     * flush loop.  Valid range: [1024, TSDB_BLOCK_POINTS].  Defaults to
     * TSDB_BLOCK_POINTS for legacy (v1/v2) schemas. */
    int                    block_points;
    /* (d) Block-layout reorder: index of the SYMBOL column whose value
     * pre-sorts each block before encoding.  -1 = off (legacy / default
     * ts-only sort).  >= 0 = on; flush re-permutes the block's row indices
     * by (cols[sort_by_tag_col].value, ts) so per-host runs are contiguous,
     * giving Gorilla / Chimp longer correlated runs at encode time.  Set
     * via _ex2-style entry point or programmatic setter; persisted in the
     * v4 tail of schema.bin (only emitted when value is >= 0, so default
     * tables stay v3 on disk and roll back cleanly to v3-aware binaries).
     * Read path is unchanged — scanners predicate per-row, no within-block
     * ts-monotonicity assumption. */
    int                    sort_by_tag_col;
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

/* Create a schema with explicit partition granularity.  block_points == 0
 * resolves to TSDB_BLOCK_POINTS (the default).  Values are clamped into
 * [1024, TSDB_BLOCK_POINTS]; passing anything outside that range silently
 * snaps to the closest valid boundary. */
int tsdb_schema_create_ex(const char *dir, const char *name,
                          const tsdb_col_t *cols, int ncols,
                          const char *ts_col,
                          tsdb_partition_unit_t partition_unit,
                          int block_points,
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

/*
 * Append a column to an existing schema in place, and re-persist schema.bin.
 *
 * The new column is added at index s->ncols (the previous end) and becomes
 * addressable as the new last index on return.  If type is SYMBOL a fresh
 * symtab is created and its .sym file flushed.
 *
 * Failure modes:
 *   TSDB_ERR_EXISTS   — a column with the same name already exists
 *   TSDB_ERR_INVAL    — name too long or null args
 *   TSDB_ERR_IO       — disk write failed
 *
 * The caller is responsible for making sure no concurrent reader/writer is
 * looking at the schema while this runs.
 */
int tsdb_schema_add_column(tsdb_schema_t *s, const char *col_name,
                            tsdb_type_t col_type);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_SCHEMA_H */
