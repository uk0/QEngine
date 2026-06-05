/* catalog_v2.h — unified, ID-keyed metadata catalog (Track B P2).
 *
 * Replaces the v1 4-log, string-FK model (databases/groups/stables/children.log)
 * with one binary, oid-keyed log and a single unified `table` entity:
 *
 *   DATABASE ──db_id──< GROUP ──group_id──< TABLE(kind, parent_id)
 *
 *   TBL_SUPER  parent_id=0   cols[](schema) + tag_cols[](tag schema)  data: none
 *   TBL_CHILD  parent_id=super  inherits parent schema + tag_vals[]    data: own dir
 *   TBL_PLAIN  parent_id=0   cols[](inline schema), no tags            data: own dir
 *
 * A plain/normal table is a first-class TBL_PLAIN — no "shadow stable" hack.
 * Parentage is oids only (catalog_id.h), never copied name strings.  Persisted
 * as <data_dir>/catalog/catalog.log; the v1 logs are untouched (this module is
 * not yet wired into the open path — P4/P6 do that).
 */
#ifndef TSDB_CATALOG_V2_H
#define TSDB_CATALOG_V2_H

#include <stdint.h>
#include <stddef.h>
#include "../../include/catalog_id.h"
#include "../../include/tsdb.h"   /* tsdb_type_t, tsdb_ts_t, TSDB_OK/ERR */

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_CAT2_NAME_MAX  63
#define TSDB_CAT2_MAX_COLS  64
#define TSDB_CAT2_MAX_TAGS  16

typedef enum { TBL_PLAIN = 0, TBL_SUPER = 1, TBL_CHILD = 2 } tsdb_table_kind_t;
typedef enum { CAT2_ENT_DB = 0, CAT2_ENT_GROUP = 1, CAT2_ENT_TABLE = 2 } tsdb_cat2_ent_t;

typedef struct { char name[TSDB_CAT2_NAME_MAX + 1]; tsdb_type_t type; } tsdb_col_def_t;
typedef struct {
    tsdb_type_t type;
    union { int64_t i; double f; char s[TSDB_CAT2_NAME_MAX + 1]; } v;
} tsdb_tagval_t;

typedef struct {
    tsdb_oid_t oid;
    char       name[TSDB_CAT2_NAME_MAX + 1];
    char       description[192];
    int64_t    retention_ns;
    tsdb_ts_t  created_at;
    uint8_t    protected_flag;
} tsdb_db_meta_t;

typedef struct {
    tsdb_oid_t oid;
    tsdb_oid_t db_id;
    char       name[TSDB_CAT2_NAME_MAX + 1];
    char       region[32];
    char       codec_profile[32];
    int64_t    retention_ns;
    int        replica_factor;
    tsdb_ts_t  created_at;
} tsdb_group_meta_t;

typedef struct {
    tsdb_oid_t        oid;
    tsdb_oid_t        db_id;       /* live db (TSDB_OID_DEFAULTDB for unparented plain) */
    tsdb_oid_t        group_id;    /* live group or TSDB_OID_NONE */
    tsdb_oid_t        parent_id;   /* TBL_CHILD → super oid; else TSDB_OID_NONE */
    tsdb_table_kind_t kind;
    char              name[TSDB_CAT2_NAME_MAX + 1];
    tsdb_col_def_t    cols[TSDB_CAT2_MAX_COLS];      int ncols, ts_col_idx; /* PLAIN/SUPER */
    tsdb_col_def_t    tag_cols[TSDB_CAT2_MAX_TAGS];  int ntag_cols;         /* SUPER */
    tsdb_tagval_t     tag_vals[TSDB_CAT2_MAX_TAGS];  int ntag_vals;         /* CHILD */
    tsdb_ts_t         created_at;
} tsdb_table_meta_t;

typedef struct tsdb_catalog_v2 tsdb_catalog_v2_t;

/* Open/close.  Replays catalog.log; materialises `default`(oid 1) + `sysdb`(oid 2)
 * if absent.  node_id seeds the oid allocator. */
int  tsdb_cat2_open(const char *data_dir, uint16_t node_id, tsdb_catalog_v2_t **out);
void tsdb_cat2_close(tsdb_catalog_v2_t *c);

/* Create.  If oid==0 a new one is minted; the assigned oid is written back to
 * the struct.  Persists + indexes.  (RI enforcement + cascade arrive in P3.) */
int  tsdb_cat2_db_create   (tsdb_catalog_v2_t *c, tsdb_db_meta_t    *db);
int  tsdb_cat2_group_create(tsdb_catalog_v2_t *c, tsdb_group_meta_t *g);
int  tsdb_cat2_table_create(tsdb_catalog_v2_t *c, tsdb_table_meta_t *t);

/* Lookup by oid (copies into *out).  Returns TSDB_OK / TSDB_ERR_NOTFOUND. */
int  tsdb_cat2_db_get   (tsdb_catalog_v2_t *c, tsdb_oid_t oid, tsdb_db_meta_t    *out);
int  tsdb_cat2_group_get(tsdb_catalog_v2_t *c, tsdb_oid_t oid, tsdb_group_meta_t *out);
int  tsdb_cat2_table_get(tsdb_catalog_v2_t *c, tsdb_oid_t oid, tsdb_table_meta_t *out);

/* Lookup oid by name.  Databases are global; groups/tables are unique within a
 * database (scoped by db_id).  Returns TSDB_OID_NONE if absent. */
tsdb_oid_t tsdb_cat2_db_by_name   (tsdb_catalog_v2_t *c, const char *name);
tsdb_oid_t tsdb_cat2_group_by_name(tsdb_catalog_v2_t *c, tsdb_oid_t db_id, const char *name);
tsdb_oid_t tsdb_cat2_table_by_name(tsdb_catalog_v2_t *c, tsdb_oid_t db_id, const char *name);

/* Drop a single entity by oid (tombstone).  Cascade is added in P3
 * (tsdb_cat2_drop_subtree). */
int  tsdb_cat2_drop(tsdb_catalog_v2_t *c, tsdb_oid_t oid);

/* Count live entities (for tests/inspection). */
size_t tsdb_cat2_count(tsdb_catalog_v2_t *c);

/* Force a log compaction (also runs at open).  Retains tombstones so the
 * cluster oid-merge can never resurrect a dropped object. */
int  tsdb_cat2_compact(tsdb_catalog_v2_t *c);

#ifdef __cplusplus
}
#endif
#endif /* TSDB_CATALOG_V2_H */
