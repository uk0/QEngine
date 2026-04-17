/* retention.h — background GC: delete partitions older than configured retention.
 *
 * Retention policy is loaded from <data_dir>/retention.conf, format:
 *   table_name  = 30d
 *   metrics.*   = 90d    # glob patterns via fnmatch(3)
 *   logs        = 7d
 *
 * Duration suffixes: d = days, h = hours, m = minutes, s = seconds.
 * First matching rule wins. Tables with no matching rule are skipped.
 * A background pthread wakes every sweep_interval_ns, scans all table
 * directories under data_dir, and removes expired partitions.
 *
 * Safe delete protocol (rename-first):
 *   1. Rename <table>/<YYYYMMDD> → <table>/.gc_<ts>_<YYYYMMDD>
 *   2. Recursively unlink the renamed directory.
 * This ensures readers that have already opened the old path finish safely;
 * the name disappears from the directory listing atomically on rename.
 * Stale .gc_* dirs from prior crashed sweeps are removed at sweep start.
 */
#ifndef TSDB_STORAGE_RETENTION_H
#define TSDB_STORAGE_RETENTION_H

#include "../../include/tsdb.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque GC handle. */
typedef struct tsdb_retention tsdb_retention_t;

/* Options passed at creation time. */
typedef struct {
    int64_t sweep_interval_ns;  /* 0 = use default (1 hour) */
    int     dry_run;            /* 1 = log decisions but skip actual unlink */
} tsdb_retention_opts_t;

/* Counters exposed to callers and tests. */
typedef struct {
    uint64_t sweeps_done;
    uint64_t partitions_deleted;
    uint64_t bytes_reclaimed;
    int64_t  last_sweep_ns;     /* wall clock ns of last completed sweep */
} tsdb_retention_stats_t;

/*
 * Start the background retention GC for a database root.
 *
 * data_dir  - same path passed to tsdb_open()
 * conf_path - path to retention.conf (may be NULL → "<data_dir>/retention.conf")
 * opts      - tuning knobs (may be NULL for defaults)
 * out       - receives opaque handle on success
 *
 * Returns TSDB_OK or negative error.
 * On success, a background thread is running; call tsdb_retention_stop() to
 * shut it down (blocks until current sweep completes).
 */
int  tsdb_retention_start(const char *data_dir,
                           const char *conf_path,
                           const tsdb_retention_opts_t *opts,
                           tsdb_retention_t **out);

/* Stop the background thread and free the handle. */
void tsdb_retention_stop(tsdb_retention_t *r);

/*
 * Execute one synchronous sweep.  For tests and tooling.
 * If deleted_out is non-NULL, *deleted_out receives number of partitions
 * removed (always 0 in dry_run mode even though stats.partitions_deleted
 * is still incremented for observability).
 *
 * Returns TSDB_OK on success or negative error.
 */
int  tsdb_retention_sweep_once(tsdb_retention_t *r, int *deleted_out);

/* Snapshot stats into *out (thread-safe, atomic copy). */
void tsdb_retention_stats(const tsdb_retention_t *r, tsdb_retention_stats_t *out);

/*
 * Convenience integration: attach retention to an open tsdb_db_t.
 * Loads conf from "<db->data_dir>/retention.conf" (or conf_path if given),
 * starts the background thread, and stores the handle inside db so
 * tsdb_close() tears it down automatically.
 *
 * Returns TSDB_OK or negative error.
 */
struct tsdb_db;
int tsdb_db_set_retention(struct tsdb_db *db,
                           const char *conf_path,
                           const tsdb_retention_opts_t *opts);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_RETENTION_H */
