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

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_DB_H */
