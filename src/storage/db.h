/* db.h — top-level database object.
 *
 * Implements tsdb_open/close, tsdb_create_table/open_table/drop_table,
 * and tsdb_batch_* from include/tsdb.h.
 *
 * Also exposes internal accessors used by the query executor in src/query/.
 */
#ifndef TSDB_STORAGE_DB_H
#define TSDB_STORAGE_DB_H

#include "../../include/tsdb.h"
#include "schema.h"
#include "memtable.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque table-internal struct is defined in db.c; query code accesses it
 * via these accessors rather than casting through the public handle. */
typedef struct tsdb_table_internal tsdb_table_internal_t;

const char            *tsdb_db_data_dir(tsdb_db_t *db);
tsdb_table_internal_t *tsdb_db_find_table(tsdb_db_t *db, const char *name);

tsdb_schema_t          *tsdb_tbl_schema(tsdb_table_internal_t *t);
tsdb_memtable_t        *tsdb_tbl_memtable(tsdb_table_internal_t *t);
const char             *tsdb_tbl_dir(tsdb_table_internal_t *t);
const char             *tsdb_tbl_name(tsdb_table_internal_t *t);

/* ---- Cluster hooks -------------------------------------------------------
 *
 * These function pointers are set by db_cluster.c when a cluster-enabled DB
 * is opened.  They are called from tsdb_batch_commit() and
 * tsdb_create_table() to trigger replication / schema-sync without
 * introducing a compile-time dependency on cluster headers in db.c.
 *
 * The hooks are stored per-db so standalone (non-cluster) opens work fine
 * with NULL hooks.
 */

/*
 * on_replicate: called BEFORE each memtable flush (auto-flush and commit-flush).
 * This is the hook for cluster replication — it receives the memtable data
 * before the memtable is cleared.
 * Signature: (userdata, db, table_name, schema, memtable)
 *   Returns TSDB_OK or negative error (error is logged but not propagated).
 */
typedef int (*tsdb_on_replicate_fn)(void *ud, tsdb_db_t *db,
                                     const char *table_name,
                                     tsdb_schema_t *schema,
                                     tsdb_memtable_t *memtable);

/*
 * on_create_table: called after table is created locally.
 * Signature: (userdata, db, table_name, schema)
 *   Returns TSDB_OK (errors are logged but not propagated).
 */
typedef int (*tsdb_on_create_fn)(void *ud, tsdb_db_t *db,
                                  const char *table_name,
                                  tsdb_schema_t *schema);

/* Register hooks for a specific db instance. */
void tsdb_db_set_hooks(tsdb_db_t *db,
                        tsdb_on_replicate_fn on_replicate,
                        tsdb_on_create_fn on_create,
                        void *userdata);

/*
 * Mark a batch as local-only (no cluster replication on commit).
 * Used by the RPC server when applying a replicated write from the primary —
 * we must not re-replicate it, or we'd loop forever.
 */
void tsdb_batch_set_local_only(tsdb_batch_t *b);

/*
 * Create a table without triggering the on_create cluster hook.
 * Used by the RPC SCHEMA_SYNC handler so we don't re-sync endlessly.
 */
int tsdb_create_table_local(tsdb_db_t *db,
                             const char *name,
                             const tsdb_col_t *cols, size_t ncols,
                             const char *ts_col);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_DB_H */
