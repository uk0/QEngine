/* group.h — Group catalog: per-DB device-group metadata.
 *
 * Groups are the top-level organizational unit for IoT devices.
 * Persisted as append-only text log: <data_dir>/catalog/groups.log
 * Each line: one record (no embedded newlines in values — escaping used).
 */
#ifndef TSDB_CATALOG_GROUP_H
#define TSDB_CATALOG_GROUP_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char      name[64];            /* unique across the parent database */
    char      region[32];          /* "us-east-1", "cn-beijing", ... */
    int64_t   retention_ns;        /* 0 = infinite */
    char      codec_profile[32];   /* "default", "chimp128+lz", "raw", ... */
    int       replica_factor;      /* 3 default */
    char      tags[512];           /* flat key=value pairs, comma-separated */
    tsdb_ts_t created_at;
    /* Parent database.  Empty → implicit "default" database. */
    char      database[64];
} tsdb_group_t;

/* Forward declaration — defined in catalog.c */
typedef struct tsdb_catalog tsdb_catalog_t;

/* Open / close the catalog for a data directory.
 * Creates <data_dir>/catalog/ if missing.
 * Replays existing log into memory on open. */
int  tsdb_catalog_open(const char *data_dir, tsdb_catalog_t **out);
void tsdb_catalog_close(tsdb_catalog_t *c);

/* Move every piece of replayed state out of `src` and into `dst`, then destroy
 * the emptied `src`.  `dst` keeps its identity — its address, its mutexes and
 * its compaction thread are untouched — so a reader that is holding `dst`
 * (every read path caches the pointer: `cat = tsdb_db_catalog(db)`) stays
 * valid across a reload.  Swapping the object instead, and freeing the old
 * one, is a use-after-free: readers hold the pointer with no refcount.
 * Both must come from the same data_dir.  dst->lock is held for the swap, so
 * no reader can be inside a catalog call while the old state is freed. */
void tsdb_catalog_adopt(tsdb_catalog_t *dst, tsdb_catalog_t *src);

int  tsdb_group_create(tsdb_catalog_t *c, const tsdb_group_t *g);
int  tsdb_group_get(tsdb_catalog_t *c, const char *name, tsdb_group_t *out);
/* Returns array of groups in *out_arr (caller must call tsdb_group_list_free).
 * *out_n is count. */
int  tsdb_group_list(tsdb_catalog_t *c, tsdb_group_t **out_arr, size_t *out_n);
void tsdb_group_list_free(tsdb_group_t *arr);
int  tsdb_group_drop(tsdb_catalog_t *c, const char *name);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CATALOG_GROUP_H */
