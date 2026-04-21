/* database.h — Top-of-hierarchy catalog entity.
 *
 * A Database is the outermost logical namespace that can own Groups
 * (a.k.a. super-tables / "gtables"), and transitively the Devices
 * (child tables) those groups hold.
 *
 * Storage: <data_dir>/catalog/databases.log, append-only, mirrors
 * groups.log format.
 *
 * Hierarchy:
 *      Database                  <-- this file
 *        └── Group (STable)      <-- tsdb_group_t, now owns `database`
 *              └── Table (Device / child table)
 *
 * A stub "default" database is implicit: any group whose `database`
 * field is empty is rendered under it.  Explicit databases created
 * here become first-class entries in the tree.
 */
#ifndef TSDB_CATALOG_DATABASE_H
#define TSDB_CATALOG_DATABASE_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char      name[64];
    char      description[192];
    int64_t   retention_ns;      /* default retention for groups under this db; 0 = none */
    tsdb_ts_t created_at;
    uint8_t   protected_flag;    /* 1 → cannot be dropped (e.g. sysdb) */
} tsdb_database_t;

/* The one system database — auto-created on catalog open if absent.
 * Holds no user tables of its own; surfaces cluster/user/RBAC state
 * through virtual tree entries in the dashboard.  Cannot be dropped. */
#define TSDB_SYSDB_NAME "sysdb"

/* Forward decl — defined in catalog.c */
typedef struct tsdb_catalog tsdb_catalog_t;

int  tsdb_database_create(tsdb_catalog_t *c, const tsdb_database_t *db);
int  tsdb_database_get   (tsdb_catalog_t *c, const char *name, tsdb_database_t *out);
int  tsdb_database_exists(tsdb_catalog_t *c, const char *name);
int  tsdb_database_drop  (tsdb_catalog_t *c, const char *name);

/* Returns a malloc'd array of all databases into *out_arr, count in *out_n.
 * Caller frees with tsdb_database_list_free. */
int  tsdb_database_list  (tsdb_catalog_t *c,
                          tsdb_database_t **out_arr, size_t *out_n);
void tsdb_database_list_free(tsdb_database_t *arr);

#ifdef __cplusplus
}
#endif
#endif /* TSDB_CATALOG_DATABASE_H */
