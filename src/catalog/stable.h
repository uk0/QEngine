/* stable.h — Super Table (STable) + Child Table metadata.
 *
 * The TDengine-style data model:
 *
 *   STable:        schema template + tag schema
 *   ChildTable:    physical data table USING an STable, with tag values
 *                  baked into catalog metadata (not stored per row)
 *
 * Example:
 *
 *   CREATE STABLE meters (ts TIMESTAMP, current FLOAT64, voltage INT64)
 *                 TAGS   (location SYMBOL, groupId INT64);
 *
 *   CREATE TABLE d1001 USING meters TAGS ('us-east-1', 42);
 *   CREATE TABLE d1002 USING meters TAGS ('us-west-2', 17);
 *
 *   INSERT INTO d1001 (ts, current, voltage) VALUES (..., ..., ...);
 *
 *   -- STable query expands to a union over all child tables,
 *   -- with tag filtering pushed down to child-table selection.
 *   SELECT avg(current), location FROM meters WHERE location='us-east-1';
 *
 * Tags live in catalog metadata — queries filter child tables by tag
 * in O(N-tables) before any data scan.
 */
#ifndef TSDB_CATALOG_STABLE_H
#define TSDB_CATALOG_STABLE_H

#include "../../include/tsdb.h"

#define TSDB_STABLE_MAX_COLS     64
#define TSDB_STABLE_MAX_TAG_COLS 16
#define TSDB_STABLE_NAME_MAX     63

typedef struct {
    char        name[TSDB_STABLE_NAME_MAX + 1];
    tsdb_type_t type;
} tsdb_stable_col_t;

typedef struct {
    char               name[TSDB_STABLE_NAME_MAX + 1];
    tsdb_stable_col_t  cols[TSDB_STABLE_MAX_COLS];
    int                ncols;
    int                ts_col_idx;             /* index into cols[] */
    tsdb_stable_col_t  tag_cols[TSDB_STABLE_MAX_TAG_COLS];
    int                ntag_cols;
    tsdb_ts_t          created_at;
} tsdb_stable_t;

/* Tag value slot: one per tag column, union of supported types. */
typedef struct {
    tsdb_type_t type;
    union {
        int64_t  i;
        double   f;
        char     s[64];   /* inlined string for SYMBOL tags */
    } v;
} tsdb_tag_val_t;

typedef struct {
    char             name[TSDB_STABLE_NAME_MAX + 1];
    char             stable_name[TSDB_STABLE_NAME_MAX + 1];
    tsdb_tag_val_t   tags[TSDB_STABLE_MAX_TAG_COLS];
    int              ntags;
    tsdb_ts_t        created_at;
} tsdb_child_table_t;

/* ── catalog-level APIs (defined in stable_catalog.c) ────────────────── */

struct tsdb_catalog;
typedef struct tsdb_catalog tsdb_catalog_t;

int tsdb_stable_create(tsdb_catalog_t *c, const tsdb_stable_t *s);
int tsdb_stable_get   (tsdb_catalog_t *c, const char *name, tsdb_stable_t *out);
int tsdb_stable_exists(tsdb_catalog_t *c, const char *name);
int tsdb_stable_drop  (tsdb_catalog_t *c, const char *name);
int tsdb_stable_list  (tsdb_catalog_t *c, tsdb_stable_t **out_arr, size_t *out_n);

int tsdb_child_table_create(tsdb_catalog_t *c, const tsdb_child_table_t *ct);
int tsdb_child_table_get   (tsdb_catalog_t *c, const char *name,
                             tsdb_child_table_t *out);
int tsdb_child_table_list  (tsdb_catalog_t *c, const char *stable_name_filter,
                             tsdb_child_table_t **out_arr, size_t *out_n);
int tsdb_child_table_drop  (tsdb_catalog_t *c, const char *name);

#endif
