/* multidir.h — multi-directory table placement.
 *
 * When `data_dirs = d1,d2,d3` is configured, tables are placed on one of
 * the dirs according to a strategy (round-robin / least-used / hash).
 * Each placement is persisted in `<primary_data_dir>/tables.idx` so
 * subsequent `tsdb_open_table` calls can locate the right disk.
 */
#ifndef TSDB_STORAGE_MULTIDIR_H
#define TSDB_STORAGE_MULTIDIR_H

#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    TSDB_MD_ROUND_ROBIN = 0,
    TSDB_MD_LEAST_USED  = 1,
    TSDB_MD_HASH        = 2
} tsdb_md_strategy_t;

typedef struct tsdb_multidir tsdb_multidir_t;

/* Build a placement registry from the primary data_dir and an optional
 * list of additional dirs. The primary is always index 0 and is used for
 * the tables.idx registry file.
 *
 * Passing n_extra = 0 means single-directory behaviour — every placement
 * returns the primary. */
int  tsdb_multidir_new(const char *primary,
                       const char *const *extra_dirs, int n_extra,
                       tsdb_md_strategy_t strategy,
                       tsdb_multidir_t **out);

void tsdb_multidir_free(tsdb_multidir_t *md);

/* Pick a disk for a new table. Returns a pointer owned by md (do not free).
 * Side-effect: persists the assignment to `<primary>/tables.idx` so
 * tsdb_multidir_find_table() can locate it on the next open. */
const char *tsdb_multidir_place_table(tsdb_multidir_t *md,
                                      const char *table_name);

/* Look up where a previously-placed table lives.
 * Returns a pointer to an internal string, or NULL if not registered
 * (in which case the caller should scan directories or use the primary). */
const char *tsdb_multidir_find_table(tsdb_multidir_t *md,
                                     const char *table_name);

/* Remove a table's entry from the registry. Called on drop. */
void tsdb_multidir_forget_table(tsdb_multidir_t *md, const char *table_name);

/* Introspection for STATS output. */
int  tsdb_multidir_count(const tsdb_multidir_t *md);
const char *tsdb_multidir_path(const tsdb_multidir_t *md, int idx);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_MULTIDIR_H */
