/* compaction.h — background size-tiered block compaction.
 *
 * Strategy: "intra-partition block merge"
 *   - Scans every table's every partition directory.
 *   - For each column file: if block count > threshold, reads all blocks,
 *     concatenates the raw values, and re-encodes into fewer, larger blocks
 *     (COMPACT_BLOCK_POINTS >> TSDB_BLOCK_POINTS).
 *   - Atomically replaces .col + .idx via .tmp files + rename (under lock).
 *
 * Concurrency model:
 *   - A per-table compact_mtx (added in db.c) is held during:
 *       a) the entire tsdb_part_flush_ex call inside flush_and_clear_ex(), and
 *       b) the compactor's rename phase.
 *   - Readers (tsdb_part_open) do NOT acquire compact_mtx; they access the
 *     files directly.  The lock only prevents flush from racing the rename.
 *   - This prevents a concurrent flush from writing into a partially-renamed
 *     file pair (.col new, .idx still old).
 *   - Workers also skip any partition whose mtime is < 60 s (still hot).
 *
 * Output block size:
 *   COMPACT_BLOCK_POINTS = 4 × TSDB_BLOCK_POINTS = 32768
 *   32 × 8192-row input blocks → ceil(32×8192 / 32768) = 8 output blocks.
 *   With variation, tests can assert block_count ≤ 8 reliably.
 */
#ifndef TSDB_STORAGE_COMPACTION_H
#define TSDB_STORAGE_COMPACTION_H

#include <stdint.h>
#include <stddef.h>
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Number of raw-value points per compacted output block. */
#define COMPACT_BLOCK_POINTS  32768

/* Minimum block count in a column before the compactor rewrites it. */
#define COMPACT_THRESHOLD_DEFAULT  16

/* ---- Opaque compactor handle ------------------------------------------- */

typedef struct tsdb_compactor tsdb_compactor_t;

/* ---- Configuration -------------------------------------------------------- */

typedef struct {
    int     min_blocks_to_compact;  /* default COMPACT_THRESHOLD_DEFAULT (16) */
    int     max_tier;               /* unused in current impl (future) */
    int64_t interval_ns;            /* sleep between full scans; default 5e9 (5 s) */
    int     worker_threads;         /* >0 = N workers; 0 = default (1);
                                       <0 = manual (no bg thread, drive via
                                       tsdb_compactor_run_once) */
} tsdb_compactor_opts_t;

/* ---- Statistics ----------------------------------------------------------- */

typedef struct {
    uint64_t compactions_done;  /* number of column files rewritten            */
    uint64_t parts_merged;      /* number of partitions where ≥1 col rewritten */
    uint64_t bytes_written;     /* compressed bytes written in new files        */
    uint64_t bytes_saved;       /* old_file_bytes - new_file_bytes (signed via uint) */
} tsdb_compactor_stats_t;

/* ---- Lifecycle API --------------------------------------------------------- */

/*
 * Start a compactor attached to db.
 * opts may be NULL to use defaults.
 * *out is populated on success.
 * Returns TSDB_OK or negative error.
 */
int  tsdb_compactor_start(tsdb_db_t *db,
                          const tsdb_compactor_opts_t *opts,
                          tsdb_compactor_t **out);

/*
 * Stop the compactor (sets quit flag + joins all workers).
 * Safe to call from any thread; safe to call multiple times.
 */
void tsdb_compactor_stop(tsdb_compactor_t *c);

/*
 * Run a single synchronous compaction pass over ALL tables/partitions.
 * Intended for tests.  Returns TSDB_OK even if nothing was compacted.
 */
int  tsdb_compactor_run_once(tsdb_compactor_t *c);

/*
 * Retrieve accumulated statistics (thread-safe snapshot).
 */
void tsdb_compactor_stats(const tsdb_compactor_t *c, tsdb_compactor_stats_t *out);

/* ---- DB integration helpers (called from db.c) ----------------------------- */

/*
 * Acquire / release the per-table compact mutex.
 * Called by flush_and_clear_ex() in db.c to serialise flush vs. compaction.
 * table is a tsdb_table_internal_t * cast to void * to avoid circular deps.
 */
void tsdb_compact_lock(void *table_internal);
void tsdb_compact_unlock(void *table_internal);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_COMPACTION_H */
