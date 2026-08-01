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
#include "part.h"
#include "wal.h"
#include "iopolicy.h"
#include "../catalog/group.h"
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque table-internal struct is defined in db.c; query code accesses it
 * via these accessors rather than casting through the public handle. */
typedef struct tsdb_table_internal tsdb_table_internal_t;

const char            *tsdb_db_data_dir(tsdb_db_t *db);
/* Multi-dir striping accessors — count is always >= 1 (a non-striped
 * setup behaves identically to count=1).  Iterate via index.  See
 * src/storage/db.c:db_pick_data_dir for the routing rule. */
int                    tsdb_db_data_dir_count(tsdb_db_t *db);
const char            *tsdb_db_data_dir_at(tsdb_db_t *db, int i);
tsdb_table_internal_t *tsdb_db_find_table(tsdb_db_t *db, const char *name);

/* Compactor lifetime guard: acquire marks the table `compacting` (so a
 * concurrent tsdb_drop_table waits before freeing it); release clears it.
 * Bracket every compaction pass that uses the raw table pointer. */
tsdb_table_internal_t *tsdb_db_compact_acquire(tsdb_db_t *db, const char *name);
void                   tsdb_db_compact_release(tsdb_db_t *db, tsdb_table_internal_t *t);

/* Query/scan lifetime guard: acquire marks the table in-use (so a concurrent
 * tsdb_drop_table waits before freeing its schema); release clears it.  Bracket
 * a SELECT's execution (incl. its parallel scan workers) with these. */
tsdb_table_internal_t *tsdb_db_scan_acquire(tsdb_db_t *db, const char *name);
void                   tsdb_db_scan_release(tsdb_db_t *db, tsdb_table_internal_t *t);

/* Snapshot the list of open table names into `out_names` (each a
 * NUL-terminated copy, at most name_cap bytes).  Returns the number of
 * names written (≤ max).  Used by anti-entropy resync to iterate
 * every known table without cloning the catalog lookup path. */
int tsdb_db_list_table_names(tsdb_db_t *db,
                              char (*out_names)[64],
                              int max);

/* Wipe all row data for a table, keeping schema + symbol tables intact.
 * Partition directories under <data_dir>/<name>/ are removed, the memtable
 * is cleared, and the WAL is truncated to zero.  Idempotent — a concurrent
 * writer's in-flight batch is NOT interrupted; callers should quiesce
 * external traffic first for a strict truncation.  Returns TSDB_OK on
 * success, TSDB_ERR_NOTFOUND if the table is unknown. */
int tsdb_truncate_table(tsdb_db_t *db, const char *name);

/* TRUNCATE only if the table is empty when checked UNDER the table lock.
 * *was_empty = 1 if it truncated, 0 if rows were present and nothing was
 * destroyed.  Closes the anti-entropy measure-then-truncate race: the emptiness
 * check and the wipe share one batch_mu + compact_mtx critical section, so a
 * WRITE_BATCH cannot commit between them. */
int tsdb_truncate_table_if_empty(tsdb_db_t *db, const char *name,
                                 int *was_empty);

/* Partition-level time-range delete.
 *
 * cutoff_ns is the boundary in nanoseconds.  op_lt=1 deletes rows with
 * ts < cutoff (or ts <= cutoff when inclusive=1); op_lt=0 deletes rows
 * with ts > cutoff (or ts >= cutoff when inclusive=1).  Only partitions
 * whose full time range satisfies the predicate are removed from disk;
 * boundary partitions containing both kept and deleted rows are left
 * untouched (documented limitation; use per-row DELETE for fine grain,
 * not yet implemented).
 *
 * If out_removed is non-NULL, *out_removed is set to the number of
 * partition directories deleted.  Returns TSDB_OK on success,
 * TSDB_ERR_NOTFOUND if the table is unknown. */
int tsdb_delete_range(tsdb_db_t *db, const char *name,
                      int64_t cutoff_ns, int op_lt, int inclusive,
                      int *out_removed);

/* Persisted per-table delete watermark: every row with ts < (return value) has
 * been deleted (op_lt deletes only).  0 = none.  Used by anti-entropy to
 * re-assert deletes so they can't be resurrected by a peer that missed them. */
int64_t tsdb_table_delwm_load(tsdb_db_t *db, const char *name);

/* Point-in-time recovery (PITR) trim.  Discards every partition whose
 * start timestamp is strictly greater than target_ns across every open
 * table in the db, so the caller can fast-forward a freshly-restored
 * /backup tarball to a specific instant and drop any writes that
 * landed after it.  On success *out_parts_removed is set to the total
 * partition count removed across all tables; pass NULL to ignore. */
int tsdb_pitr_trim_to(tsdb_db_t *db, int64_t target_ns,
                      int *out_parts_removed);

/* Acquire / release the per-table batch serialization lock.
 *
 * Required by any caller running a tsdb_batch_begin → row_*... →
 * tsdb_batch_commit sequence from a context where concurrent writes to
 * the same table are possible (in particular: the cluster RPC handler
 * thread pool on the replica side).  Without this lock, two concurrent
 * batches interleave memtable row_begin / row_ts / ... / row_end on a
 * single table, tearing rows and (with replica pool >1) corrupting the
 * heap on replay.
 *
 * Idempotent for the single-writer path but adds negligible overhead —
 * one uncontended mutex acquire per batch.
 */
void tsdb_table_lock_write  (tsdb_table_t *tbl);
void tsdb_table_unlock_write(tsdb_table_t *tbl);

/* Access the catalog embedded in the db. May return NULL if catalog
 * failed to open (non-fatal for pure SELECT workloads). */
tsdb_catalog_t        *tsdb_db_catalog(tsdb_db_t *db);

/* Diagnose-only consistency scan over the 4-level catalog.  Writes a
 * JSON report into buf (capped at cap bytes) listing referential
 * orphans and on-disk dirs without a catalog row.  Returns bytes
 * written, or negative on error.  Defined in catalog_check.c. */
int                    tsdb_catalog_check(tsdb_db_t *db, char *buf, size_t cap);

/* Backup manifest: per-table (count, max_ts) snapshot used by /backup
 * + restore tooling to verify the round-trip preserved every row.
 * Defined in backup.c. */
int                    tsdb_backup_write_manifest_json(tsdb_db_t *db,
                                                       char *buf, size_t cap);

/* tsdb_backup_emit_manifest_file return codes.
 *
 * The emitter does two things with very different weight, and a single -1
 * flattened them: it flushes every memtable to disk (WITHOUT which the tar the
 * caller is about to take is missing every unflushed row — under
 * TSDB_WAL_ONLY_COMMIT that is most of the recent data), and it writes a
 * verification aid.  The second failing is an inconvenience.  The first
 * failing means the backup would be INCOMPLETE, and the caller has to be able
 * to tell, because the manifest it would otherwise ship is built from
 * tsdb_query — which reads the memtable, so it records counts the tarball does
 * not contain. */
#define TSDB_BACKUP_MANIFEST_FAILED   (-1)  /* aid missing; backup may proceed */
#define TSDB_BACKUP_NOT_ON_DISK       (-2)  /* rows are NOT in the files; do
                                             * not represent a tar of them as a
                                             * backup */
int                    tsdb_backup_emit_manifest_file(tsdb_db_t *db,
                                                     const char *data_dir);

/* Flush every open table's memtable to its on-disk partition so a
 * subsequent on-disk snapshot (e.g. /backup tar) captures all rows.
 * One table's failure never stops the sweep, but the FIRST failure is
 * RETURNED — a caller that asked for its rows on disk has to be able to
 * find out that they are not.  Defined in db.c. */
int                    tsdb_db_flush_all(tsdb_db_t *db);

/* Flush a SINGLE table's memtable to its on-disk partitions.  Required before
 * any operation that reads partition files directly (Parquet export), which
 * would otherwise see nothing at all under deferred-flush mode.  Returns
 * TSDB_OK, or TSDB_ERR_NOTFOUND for an unknown table.  Defined in db.c. */
int                    tsdb_table_flush(tsdb_db_t *db, const char *name);

/* Detected hardware class — set once at open from tsdb_iopolicy_detect().
 * Drives write-path defaults: memtable budget, compactor backoff
 * threshold.  Env vars still override individual knobs. */
tsdb_iopolicy_t        tsdb_db_iopolicy(tsdb_db_t *db);

/* Non-zero when this db defers the partition flush (TSDB_WAL_ONLY_COMMIT=1,
 * or auto-enabled at open for a rotational/sata iopolicy).  The flag moves
 * a commit's durability point: the ack means "redo record fsynced", and the
 * partition write happens later on a size/idle flush.  Anything that needs
 * the rows on DISK rather than merely durable — snapshot, export, a
 * disk-level assertion — must tsdb_db_flush_all() first, and check it. */
int                    tsdb_db_wal_only_commit(tsdb_db_t *db);

/* Raise a table's redo commit sequence so no FUTURE record can be handed a seq
 * a partition on disk already claims as durable.  Monotonic — never lowers.
 *
 * Needed by the restore path (storage/restore.c), which stamps a restored
 * partition's idx with the checkpoint the SOURCE recorded.  Those numbers come
 * from another table's counter, so a target that starts writing afterwards
 * would give its first commits seqs at or below the stamp, and the next open's
 * redo_recover_table would SKIP exactly those records for that partition —
 * acked rows dropped, silently.  Defined in db.c. */
int                    tsdb_db_raise_commit_seq(tsdb_db_t *db,
                                                const char *table,
                                                uint64_t seq);

/* Monotonic flush counter — bumped once per successful memtable→disk
 * flush in flush_and_clear_locked.  Used by the compactor's adaptive
 * pause to back off while writes are hot.  Relaxed atomic; only the
 * delta over a few-second window matters. */
uint64_t               tsdb_db_flush_seq(tsdb_db_t *db);

/* Monotonic table-set generation — bumped under db->lock on every table
 * insert (open/create), drop detach, and ALTER ADD COLUMN.  The steady write
 * path caches an open table handle to skip the db->lock tsdb_open_table takes
 * per WRITE_BATCH, revalidating it against this counter (#168 lever 2).
 * Relaxed atomic. */
uint64_t               tsdb_db_table_set_gen(tsdb_db_t *db);

/* Catalog self-heal: pull the master's catalog log files via
 * TSDB_RPC_CATALOG_DUMP and replay them locally.  Used by data
 * nodes at startup to recover from broadcast misses (crash window
 * during cluster CREATE storms).  Defined in catalog_sync.c. */
int                    tsdb_catalog_pull_from_master(tsdb_db_t *db);

/* Resurrection-safe reverse merge of a CATALOG_DUMP payload: appends only the
 * peer's live `+` records for names absent from the local log (never resurrects
 * a locally-dropped table).  tsdb_catalog_reconcile_from_peers pulls every alive
 * peer's dump and merges it, so a recovered node — including the master — learns
 * tables created during its downtime.  Defined in catalog_sync.c. */
int                    tsdb_catalog_dump_apply_filtered(const char *data_dir,
                                                        const uint8_t *buf, size_t len);
int                    tsdb_catalog_reconcile_from_peers(tsdb_db_t *db);

/* Access the TMQ consumer-group store embedded in the db. Returns NULL if
 * it failed to open (non-fatal for plain workloads). */
struct tsdb_tmq;
struct tsdb_tmq       *tsdb_db_tmq(tsdb_db_t *db);

/* Access the RBAC/auth store embedded in the db. May be NULL if opening
 * failed (non-fatal for plain workloads). */
struct tsdb_auth;
struct tsdb_auth      *tsdb_db_auth(tsdb_db_t *db);

/* Access the UDF (user-defined function) catalog. May be NULL if opening
 * failed. */
struct tsdb_udf_catalog;
struct tsdb_udf_catalog *tsdb_db_udf(tsdb_db_t *db);

/* Access the append-only audit log.  May be NULL if opening failed
 * (read-only fs); callers must tolerate a NULL pointer — the module's
 * public write functions silently no-op in that case. */
struct tsdb_audit;
struct tsdb_audit      *tsdb_db_audit(tsdb_db_t *db);

tsdb_schema_t          *tsdb_tbl_schema(tsdb_table_internal_t *t);
tsdb_memtable_t        *tsdb_tbl_memtable(tsdb_table_internal_t *t);
const char             *tsdb_tbl_dir(tsdb_table_internal_t *t);
const char             *tsdb_tbl_name(tsdb_table_internal_t *t);

/*
 * Return the per-table compaction mutex.  Held by flush_and_clear_ex() and
 * by the compactor's rename phase to prevent reading a half-swapped file pair.
 * Defined in db.c because it accesses the opaque tsdb_table_internal_t layout.
 */
pthread_mutex_t        *tsdb_tbl_compact_mtx(tsdb_table_internal_t *t);

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

/*
 * on_raw_block: called by tsdb_part_flush_ex after each column block is encoded.
 * Opt-in raw-block replication path.  When registered, it fires on every
 * encoded block; set TSDB_REPLICATION_MODE=raw in db_cluster.c to activate
 * this path instead of on_replicate.
 *
 * Signature: (userdata, db, table_name, day_yyyymmdd, col_idx, meta,
 *              block_bytes, block_bytes_len)
 *   block_bytes: compressed data only (NO 32-byte BlockHeader prefix).
 *   meta: populated by part.c (codec, flags, count, ts_min, ts_max).
 *   Returns TSDB_OK or error (logged but not propagated).
 */
typedef int (*tsdb_on_raw_block_fn)(void *ud, tsdb_db_t *db,
                                     const char *table_name,
                                     uint32_t part_day,
                                     uint16_t col_idx,
                                     const tsdb_block_meta_t *meta,
                                     const uint8_t *block_bytes,
                                     size_t block_bytes_len);

/* Register hooks for a specific db instance. */
void tsdb_db_set_hooks(tsdb_db_t *db,
                        tsdb_on_replicate_fn on_replicate,
                        tsdb_on_create_fn on_create,
                        void *userdata);

/* Register the raw-block hook (opt-in; independent from set_hooks).
 * raw_ud is passed as the first argument to on_raw_block. */
void tsdb_db_set_raw_block_hook(tsdb_db_t *db,
                                 tsdb_on_raw_block_fn on_raw_block,
                                 void *raw_ud);

/* Retrieve the registered raw-block hook + userdata.  Called from part.c.
 * Uses void** to avoid a circular typedef between db.h and part.h. */
void tsdb_db_get_raw_block_hook(tsdb_db_t *db,
                                 void **out_fn,
                                 void **out_ud);

/*
 * Mark a batch as local-only (no cluster replication on commit).
 * Used by the RPC server when applying a replicated write from the primary —
 * we must not re-replicate it, or we'd loop forever.
 */
void tsdb_batch_set_local_only(tsdb_batch_t *b);

/*
 * Bulk columnar append on a batch — pushes `n` rows directly via
 * tsdb_memtable_append_bulk, skipping the per-row begin/end + per-cell
 * dispatch.  Use from WRITE_BATCH receivers that already have data laid
 * out in the wire format described on tsdb_memtable_append_bulk.
 *
 * Must NOT be interleaved with tsdb_batch_row_* on the same batch
 * (the per-row API holds the memtable lock for the whole row; the
 * bulk path takes it once for the whole append).  Call tsdb_batch_commit
 * after to flush + WAL-sync as usual.
 *
 * `col_arrs` and `col_types` exclude the timestamp column (n-1
 * entries for an n-column schema).
 */
/* ts_arr is `const void *` so wire-protocol callers (RPC WRITE_BATCH
 * receivers, server bulk path) can pass a possibly-misaligned int64
 * slice without tripping UBSAN's alignment check.  Internally memcpy'd
 * out 8 bytes at a time. */
int tsdb_batch_append_bulk(tsdb_batch_t *b,
                            const void *ts_arr,
                            const void * const *col_arrs,
                            const int *col_types,
                            int ncols_data,
                            size_t n);

/*
 * Create a table without triggering the on_create cluster hook.
 * Used by the RPC SCHEMA_SYNC handler so we don't re-sync endlessly.
 */
int tsdb_create_table_local(tsdb_db_t *db,
                             const char *name,
                             const tsdb_col_t *cols, size_t ncols,
                             const char *ts_col);

/*
 * Same as tsdb_create_table_local, but threads through the schema's
 * partition_unit / block_points / sort_by_tag_col so leader-side schema
 * choices (e.g. PRIMARY TAG sort) replicate to followers via the RPC
 * SCHEMA_SYNC v2-tail payload.  Pre-tail (legacy) senders pass
 * (DAY, 0, -1) which collapses to today's behavior.
 *
 *   partition_unit_int  — TSDB_PARTITION_DAY=0 / HOUR=1
 *   block_points        — 0 means "engine default"
 *   sort_by_tag_col     — -1 = off; >=0 = column index (must be SYMBOL)
 *   incarnation         — the leader's durable table incarnation, stamped
 *                         verbatim so leader and follower agree; 0 (UNKNOWN)
 *                         from a pre-incarnation sender.
 */
int tsdb_create_table_local_ex(tsdb_db_t *db,
                                const char *name,
                                const tsdb_col_t *cols, size_t ncols,
                                const char *ts_col,
                                int partition_unit_int,
                                int block_points,
                                int sort_by_tag_col,
                                uint64_t incarnation);

/* Return the underlying schema for a table handle.  Used by code that
 * needs to read schema metadata (column types, tag-sort flag) or apply
 * a setter (tsdb_schema_set_sort_by_tag_col).  The returned pointer is
 * owned by the table; do not free.  Returns NULL on bad input. */
tsdb_schema_t *tsdb_table_get_schema(tsdb_table_t *t);

/*
 * Enable group-commit for the database.  All subsequent tsdb_batch_commit()
 * calls will route WAL fsync through the group-commit batcher instead of
 * calling fsync() directly.
 *
 * batch_window_ns: maximum time (nanoseconds) a commit waits for coalescing.
 *   0 disables group-commit (reverts to direct fsync on each commit).
 *   Recommended: 500_000 (0.5 ms) to 2_000_000 (2 ms).
 *
 * Returns TSDB_OK or TSDB_ERR_*.
 */
int tsdb_db_set_group_commit(tsdb_db_t *db, int64_t batch_window_ns);

/*
 * Attach a retention GC handle to this db instance.
 * The handle is stopped and freed automatically in tsdb_close().
 * Called by tsdb_db_set_retention() in retention.c.
 */
struct tsdb_retention;
void tsdb_db_attach_retention(tsdb_db_t *db, struct tsdb_retention *r);

/* Return the retention handle currently attached to db (may be NULL
 * if tsdb_db_set_retention was not called).  Used by management
 * endpoints that want to trigger a manual sweep. */
struct tsdb_retention *tsdb_db_retention(tsdb_db_t *db);

/*
 * Return the per-table compaction mutex.  Held by flush_and_clear_ex() and
 * by the compactor's rename phase to prevent reading a half-swapped file pair.
 * Defined in db.c because it accesses the opaque tsdb_table_internal_t layout.
 */
pthread_mutex_t        *tsdb_tbl_compact_mtx(tsdb_table_internal_t *t);

/*
 * Enable group-commit for the database.  All subsequent tsdb_batch_commit()
 * calls route WAL fsync through a background batcher instead of direct fsync.
 *
 * batch_window_ns: max wait time (ns) before forcing an fsync.
 *   0 disables (direct fsync per commit).
 *   Recommended: 500_000 (0.5 ms) to 2_000_000 (2 ms).
 *
 * Returns TSDB_OK or TSDB_ERR_*.
 */
int tsdb_db_set_group_commit(tsdb_db_t *db, int64_t batch_window_ns);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_DB_H */
