/* db.c — top-level database implementation. */

#include "db.h"
#include "schema.h"
#include "memtable.h"
#include "part.h"
#include "wal.h"
#include "../core/symbol.h"
#include "../../include/tsdb.h"
#include "../catalog/group.h"
#include "../catalog/stable.h"
#include "../catalog/device.h"
#include "../catalog/tmq.h"
#include "../catalog/udf.h"
#include "../catalog/user.h"
#include "../catalog/audit.h"
#include "../server/metrics.h"
#include "iopolicy.h"
#include "retention.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <pthread.h>

/* Forward declaration of mkdir_p from schema.c. */
extern int tsdb_mkdir_p(const char *path);

/* Deferred-reclaim helpers (defined below, used by tsdb_open/drop). */
static void *trash_gc_main(void *arg);
static void  trash_or_rm(tsdb_db_t *db, const char *tbl_dir);

/* Forward decl: definitions below. Needed by tsdb_close and
 * tsdb_alter_table_add_column.
 * `_locked` variant assumes the caller already holds t->compact_mtx and is
 * what alter_table / future callers-with-the-lock should use; `_ex` is the
 * public entry point that takes/releases the lock for callers without it. */
static int flush_and_clear_locked(tsdb_table_internal_t *t, int skip_replicate);
static int flush_and_clear_ex(tsdb_table_internal_t *t, int skip_replicate);

/* ---- Table handle ------------------------------------------------------- */

/*
 * Internal table structure.
 * The public tsdb_table_t * aliases this.
 */
typedef struct tsdb_table_internal {
    tsdb_schema_t       *schema;
    tsdb_memtable_t     *memtable;
    tsdb_wal_t          *wal;
    tsdb_group_commit_t *gc;          /* NULL when group-commit disabled */
    char                 name[TSDB_MAX_NAME + 1];
    tsdb_db_t           *db;          /* owning db (set at creation/open) */
    pthread_mutex_t      compact_mtx; /* serialises flush vs. compactor swap */
    /* Serialises an entire batch (begin..commit) for this table.
     * Without this, two concurrent replicate RPCs landing on different
     * RPC-handler threads can interleave row_begin/row_ts/.../row_end
     * on the same memtable, producing torn rows and (with pool>1 peer
     * connections) heap corruption in the replay path. */
    pthread_mutex_t      batch_mu;
    /* Set by the background compactor (under db->lock) for the duration of a
     * compaction pass on this table; tsdb_drop_table waits for it to clear
     * before freeing, since the compactor uses schema/compact_mtx lock-free. */
    volatile int         compacting;
    /* In-flight query/scan count (under db->lock).  A SELECT holds a raw
     * pointer to this table's schema across exec + its parallel scan workers;
     * tsdb_drop_table waits for this to reach 0 before freeing the schema, or
     * a concurrent DROP is a use-after-free (caught by ASan in agg_write /
     * tsdb_part_col_blocks). */
    volatile int         scan_refs;
    /* Set (under db->lock) while ALTER ADD COLUMN reallocs schema->cols.
     * tsdb_db_scan_acquire waits while it is set, so no query starts reading
     * the cols array during the realloc; ALTER also drains scan_refs to 0
     * first, closing the schema-realloc-vs-lockless-reader race. */
    volatile int         altering;

    /* ---- WAL redo log (TSDB_WAL_ONLY_COMMIT crash-durability) ----
     * Only meaningful when db->wal_only_commit is set; zero/untouched in the
     * default flush-on-commit mode.  Guarded by compact_mtx (the same lock the
     * flush takes) so a commit's redo-append and a flush's checkpoint can
     * never interleave.
     *
     *  commit_seq : per-table monotonic sequence, ++ on every redo record
     *               written (one per commit, or per forced mid-batch flush).
     *               The value stamped into a partition's idx on flush is the
     *               commit_seq at flush time; recovery replays only WAL
     *               records with seq > that durable checkpoint.
     *  mem_logged : number of memtable rows already covered by a WAL redo
     *               record.  Reset to 0 whenever the memtable is cleared by a
     *               flush.  A commit serialises memtable rows [mem_logged,
     *               nrows) into the next redo record. */
    uint64_t             commit_seq;
    size_t               mem_logged;
    /* Monotonic per-table memtable-drain counter, bumped under compact_mtx
     * every time flush_and_clear_locked drains the memtable to a partition (or
     * drops it via SKIP_LOCAL).  A batch snapshots it at begin; if it differs at
     * commit/discard time, a flush landed MID-batch, so part of the batch is
     * already durable on disk and a memtable truncate cannot fully roll the
     * batch back (the "straddle" case) — detected so we can fail loudly instead
     * of silently pretending a clean rollback. */
    uint64_t             flush_gen;
    /* idle-flush (tsdb_idle_flush_thread) state — touched only by that thread
     * under db->lock, except mem_has_local which the write path also sets.
     *  idle_prev_rows : memtable row count at the previous idle tick; a table
     *               whose count is unchanged (and non-zero) has gone quiet.
     *  mem_has_local : the memtable holds locally-INGESTED rows (a client write
     *               with local_only=0) that have NOT yet been flushed/replicated
     *               to the shard owners.  Set on a local append, cleared on
     *               every flush.  The idle-flush replicates ONLY such tables —
     *               a memtable holding purely RECEIVED (replicated, local_only=1)
     *               rows must never be re-flushed-with-replicate, or two replica
     *               owners bounce the same rows back and forth forever. */
    size_t               idle_prev_rows;
    int                  mem_has_local;
    /* mem_has_received : the memtable has absorbed REPLICATED (local_only=1)
     * rows since its last flush.  The idle-flush refuses to flush-with-replicate
     * such a memtable even when it also holds local rows (a node that both
     * ingests and owns a table): re-sending the received portion to the other
     * replica would duplicate it (the memtable has no ts-dedup).  The local tail
     * stays visible + WAL-durable locally and drains via the size path. */
    int                  mem_has_received;

    /* ---- WAL redo replay state (compact_mtx-guarded) ----
     *  replaying      : a redo replay is READING this table's WAL right now.
     *               A flush forced from inside the replay (redo_replay_apply's
     *               memtable-overflow guard) must NOT truncate the log the
     *               replay is still iterating: ftruncate(fd,0) makes the
     *               reader's next fread return 0, which replay reads as a clean
     *               EOF, so every record past that point is destroyed AND
     *               reported as a successful replay.
     *  wal_incomplete : the replay ABORTED and the records it never read are
     *               not covered by any partition checkpoint.  Those acked rows
     *               survive ONLY in the log, so the table is frozen: no flush
     *               (hence no checkpoint advance over them), no WAL truncate,
     *               and no new redo commits (a record appended above the
     *               aborting one can never be replayed, so acking it would be
     *               a lie).  Reads are untouched — a table whose tail we cannot
     *               apply is still a table, and refusing to open it would turn
     *               a recoverable state into an outage.  Cleared by TRUNCATE
     *               TABLE, or simply by an operator repairing/removing the log:
     *               the verdict is a pure function of on-disk state. */
    int                  replaying;
    int                  wal_incomplete;
} tsdb_table_internal_t;

/* ---- DB handle ---------------------------------------------------------- */

/* Raised from 128 (a dev-scale cap) to 16384 so a single node can hold
 * the thousands of child tables an industrial time-series workload
 * expects — a mid-size factory with 64 super-tables × 100 devices
 * already blows through the old limit after one commissioning run.
 * The array lives in the struct, so the only cost is ~128 KB of
 * pointer space per open db. */
#define TSDB_DB_MAX_TABLES 16384

/* Maximum number of striped data directories.  Bigger than typical
 * RAID disk counts so an operator can dedicate one TSDB instance
 * across many SSDs without recompiling. */
#define TSDB_MAX_DATA_DIRS 16

struct tsdb_db {
    char             data_dir[4096];     /* primary — catalog/WAL/audit */
    /* Extra striping directories.  When TSDB_DATA_DIRS is set the
     * primary becomes data_dirs[0] and the rest are populated; table
     * data is routed by hash(table_name) % n_data_dirs.  When unset,
     * n_data_dirs == 0 and every callsite falls back to data_dir,
     * preserving single-dir behaviour bit-for-bit. */
    char             data_dirs[TSDB_MAX_DATA_DIRS][4096];
    /* Per-dir health flag (auto-recovery).  1 = healthy, 0 = degraded
     * (mkdir failed at open, statvfs/W_OK probe failed, or a partition
     * flush hit TSDB_ERR_IO).  A degraded dir keeps its SLOT — n_data_dirs
     * never shrinks, so hash placement of every other dir is stable — and
     * only stops receiving NEW tables until trash_gc_main re-probes it
     * healthy (e.g. after a remount).  All-zero via calloc; unused when
     * n_data_dirs <= 1 (single-dir setups never consult it). */
    uint8_t          dir_ok[TSDB_MAX_DATA_DIRS];
    int              n_data_dirs;
    pthread_mutex_t  lock;

    /* Deferred storage reclaim.  DROP renames a table's dir into
     * <its-data-dir>/.trash (an O(1) same-fs rename) instead of an inline
     * recursive rm_rf — a mass DROP DATABASE cascades thousands of these
     * through the single raft apply thread, where an inline rm_rf froze
     * last_applied for tens of seconds and stalled all DDL.  A bg thread
     * reclaims .trash (and drains crash leftovers on open). */
    pthread_t        trash_gc_thread;
    volatile int     trash_gc_running;
    uint64_t         trash_seq;

    /* Aggregate-memtable backpressure.  is_full() caps each table's memtable at
     * block_points ROWS, but with thousands of tables the SUM bloats unbounded
     * (a 2000-ptable stress held ~3.4 GiB on the master).  The trash/maintenance
     * thread periodically sums live memtable rows across all tables and raises
     * this flag when over budget; maybe_flush_b then flushes the table being
     * written (on the writer's own thread — no cross-thread flush) so aggregate
     * memory stays bounded and fast writers get natural backpressure.
     * memtable_budget_rows == 0 disables it (unbounded, legacy behaviour). */
    volatile int     memtable_over_budget;
    uint64_t         memtable_budget_rows;

    /* Monotonic flush counter — bumped once per successful memtable→disk
     * flush in flush_and_clear_locked.  Read by the compactor's adaptive
     * pause logic (compaction.c) to skip cycles while writes are hot, so
     * the compactor stops fighting the writer for the HDD device queue.
     * Relaxed atomic: only the delta over a few-second window matters. */
    volatile uint64_t flush_seq;

    /* Monotonic table-set generation — bumped under db->lock on every table
     * INSERT (open/create), DROP detach, and ALTER ADD COLUMN.  The steady
     * write path caches an already-open table handle to skip the db->lock that
     * tsdb_open_table takes on EVERY WRITE_BATCH; it revalidates the cached
     * handle by comparing this counter, so a drop/alter that could have freed
     * or resized the table invalidates every cache without the writer touching
     * db->lock on the hot path (#168 lever 2).  Relaxed atomic. */
    volatile uint64_t table_set_gen;

    /* Detected storage class — drives write-path defaults (memtable budget,
     * compactor backoff threshold) at open time.  Detection rule lives in
     * src/storage/iopolicy.h; env vars still override individual knobs. */
    tsdb_iopolicy_t iopolicy;

    tsdb_table_internal_t *tables[TSDB_DB_MAX_TABLES];
    int                    ntables;

    /* Open-addressing hash index: table name -> tsdb_table_internal_t*.
     * Lives beside tables[] (which stays the iteration order) and is
     * maintained at every tables[] mutation.  db_find_table probes this
     * instead of the old O(ntables) strcmp walk that ran under db->lock on
     * every read and write — at 2000+ tables that linear walk was the
     * db->lock hold-time source behind master-node query-p99 tail latency.
     * Sized to 2x TSDB_DB_MAX_TABLES (power of two) so the load factor stays
     * <= 0.5 and probe chains stay short.  Stores POINTERS, not indices, so
     * the drop path's array-compaction of tables[] never invalidates it.
     * A NULL slot is empty; TBL_TOMBSTONE marks a deleted slot so linear
     * probing can continue past it. */
    tsdb_table_internal_t *tbl_index[TSDB_DB_MAX_TABLES * 2];

    /* Cluster hooks (NULL for standalone mode). */
    tsdb_on_replicate_fn on_replicate;
    tsdb_on_create_fn    on_create;
    void                *hook_ud;

    /* Raw-block replication hook (opt-in, NULL by default). */
    tsdb_on_raw_block_fn on_raw_block;
    void                *raw_block_ud;

    /* Catalog (Group/Device metadata). Opened in tsdb_open. */
    tsdb_catalog_t      *catalog;

    /* TMQ consumer-group store. Opened in tsdb_open. */
    tsdb_tmq_t          *tmq;

    /* RBAC auth store. Opened in tsdb_open. NULL if open failed. */
    tsdb_auth_t         *auth;

    /* UDF catalog (scalar user-defined functions). NULL if open failed. */
    tsdb_udf_catalog_t  *udf;

    /* Audit log. Best-effort; NULL if the append-only file cannot be
     * created (e.g. read-only filesystem).  Callers must tolerate a
     * NULL pointer; tsdb_audit_write degrades to a no-op. */
    tsdb_audit_t        *audit;

    /* Opt-in enforcement flag.  Default false: tsdb_auth_enforce is a no-op.
     * Flip to true to auto-consult tsdb_auth_check via tsdb_auth_enforce. */
    bool                 auth_enforce;

    /* Retention GC (NULL when not configured). Stopped in tsdb_close(). */
    struct tsdb_retention *retention;

    /* Group-commit window (0 = disabled). Applied to all table WALs. */
    int64_t              group_commit_window_ns;

    /* Default block_points for subsequently-created tables.  0 = library
     * default (TSDB_BLOCK_POINTS).  Tunable via tsdb_db_set_default_block_points. */
    int                  default_block_points;

    /* When set, tsdb_batch_commit only syncs WAL and skips the immediate
     * memtable→partition flush; the memtable flushes naturally when it fills
     * via maybe_flush_b().  Enabled by env TSDB_WAL_ONLY_COMMIT=1.
     *
     * Trade-off: reduces flush count sharply (batches share a memtable), but
     * defers replication (row-level and raw-block hooks fire inside
     * flush_and_clear_ex).  Durability is preserved via WAL fsync; crash
     * recovery replays WAL records back into memtables on open. */
    int                  wal_only_commit;
    /* Set when wal_only_commit was turned on by the non-SSD iopolicy
     * auto-tune (not an explicit env).  A cluster node clears it back to
     * flush-on-commit when it registers replication hooks, so the auto-tune
     * never silently defers replication — operators opt into deferred flush
     * on a cluster via an explicit TSDB_WAL_ONLY_COMMIT=1. */
    int                  wal_only_auto;
};

/* ---- Batch struct ------------------------------------------------------- */

/*
 * A batch is just a thin wrapper holding a pointer to the table
 * (and tracking in-progress row state).
 */
struct tsdb_batch {
    tsdb_db_t             *db;        /* owning database (needed for cluster hook) */
    tsdb_table_internal_t *tbl;
    int                    in_row;
    int                    local_only; /* 1 = skip replication hook (replica recv) */
    /* Rollback anchors captured at tsdb_batch_begin.  base_nrows is the
     * memtable row count before this batch appended anything — the boundary a
     * discard rolls back to (undoes only THIS batch, never rows a prior batch
     * left behind).  begin_flush_gen snapshots tbl->flush_gen so commit/discard
     * can tell whether a flush straddled the batch (see flush_gen). */
    size_t                 base_nrows;
    uint64_t               begin_flush_gen;
};

/* ---- Public per-table batch lock --------------------------------------- */

tsdb_schema_t *tsdb_table_get_schema(tsdb_table_t *tbl) {
    if (!tbl) return NULL;
    return ((tsdb_table_internal_t *)tbl)->schema;
}

void tsdb_table_lock_write(tsdb_table_t *tbl) {
    if (!tbl) return;
    pthread_mutex_lock(&((tsdb_table_internal_t *)tbl)->batch_mu);
}

void tsdb_table_unlock_write(tsdb_table_t *tbl) {
    if (!tbl) return;
    pthread_mutex_unlock(&((tsdb_table_internal_t *)tbl)->batch_mu);
}

/* ---- Path helpers ------------------------------------------------------- */

/* fnv1a — same hash as src/core/symbol.c.  Used by data-dir routing
 * because it's branch-free, alloc-free and delivers a clean
 * distribution on short identifier strings. */
static uint64_t db_fnv1a(const char *s) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (; *s; s++) { h ^= (uint8_t)*s; h *= 0x100000001b3ULL; }
    return h;
}

/* A data dir is writable-healthy when the filesystem answers statvfs AND
 * the dir itself grants write access.  An unmounted mountpoint typically
 * fails one of the two (dir missing, or a root-owned 0755 stub left at
 * the mount path).  Used at open, on flush-error degrade re-probe, and by
 * trash_gc_main's re-adopt pass. */
static int db_dir_healthy(const char *dir) {
    struct statvfs vfs;
    if (statvfs(dir, &vfs) != 0) return 0;
    if (access(dir, W_OK) != 0) return 0;
    return 1;
}

/* Flush hit TSDB_ERR_IO writing under `path` — find which configured data
 * dir it lives in (longest prefix match, so "/d1" never claims "/d10/x")
 * and mark that slot degraded.  New-table placement then routes around it
 * (db_pick_data_dir) while every existing table still resolves via the
 * db_resolve_table_dir probe.  The failed flush's rows stay in the
 * memtable — nothing is moved or dropped here.  No-op on single-dir. */
static void db_mark_dir_degraded(tsdb_db_t *db, const char *path) {
    if (!db || !path || db->n_data_dirs <= 1) return;
    int best = -1;
    size_t best_len = 0;
    for (int i = 0; i < db->n_data_dirs; i++) {
        size_t l = strlen(db->data_dirs[i]);
        if (l > best_len && strncmp(path, db->data_dirs[i], l) == 0 &&
            (path[l] == '/' || path[l] == '\0')) {
            best = i;
            best_len = l;
        }
    }
    if (best < 0 || !db->dir_ok[best]) return;
    db->dir_ok[best] = 0;
    tsdb_metric_inc("qengine_datadir_degraded_total");
    fprintf(stderr, "[db] data dir [%d] %s degraded (flush I/O error) — "
            "routing new tables away until it re-probes healthy\n",
            best, db->data_dirs[best]);
}

/* Pick the data directory a brand-new table should live in.  Hash by
 * name so the same table always routes to the same disk and the
 * distribution stays even across heterogeneous workloads.  Returns the
 * primary data_dir when striping is disabled.
 *
 * The modulus is always the FULL configured n_data_dirs — a degraded dir
 * keeps its slot — so a healthy setup places tables exactly as before.
 * Only when the hashed slot is degraded do we linear-probe forward
 * (wrapping) to the next healthy dir; if none is healthy, fall back to
 * the primary (slot 0).  Existing tables are unaffected: they resolve by
 * on-disk probe in db_resolve_table_dir, not by this hash. */
static const char *db_pick_data_dir(const tsdb_db_t *db, const char *table_name) {
    if (!db || db->n_data_dirs <= 1) return db->data_dir;
    /* Pure stable hash — placement is deterministic and never routes AWAY
     * from a degraded dir.  Rerouting a new table around a temporarily-dark
     * stripe would let a later re-adopt leave the SAME table on two dirs
     * (one copy silently unreachable); refusing the create on the degraded
     * slot (the write simply fails, as before) is the safe choice.  dir_ok
     * is observability + re-adopt only, not a placement input. */
    uint32_t idx = (uint32_t)(db_fnv1a(table_name) % (uint64_t)db->n_data_dirs);
    return db->data_dirs[idx];
}

/* Resolve the dir an EXISTING table currently lives in by probing every
 * configured data directory.  Falls back to the hash-picked dir when
 * the table isn't on disk yet (i.e. we're about to create it).  Caller
 * passes a buffer sized to hold any data_dir + "/" + name + NUL. */
static const char *db_resolve_table_dir(const tsdb_db_t *db, const char *table_name) {
    if (!db || db->n_data_dirs <= 1) return db->data_dir;
    char probe[4200];
    for (int i = 0; i < db->n_data_dirs; i++) {
        snprintf(probe, sizeof(probe), "%s/%s", db->data_dirs[i], table_name);
        struct stat st;
        if (stat(probe, &st) == 0 && S_ISDIR(st.st_mode)) {
            return db->data_dirs[i];
        }
    }
    /* Not yet on disk — return the slot a new table would land in. */
    return db_pick_data_dir(db, table_name);
}

/* table_dir is now a thin wrapper that picks the right base dir.  All
 * existing callers stay source-compatible because they used to take the
 * `db->data_dir` string anyway; we just have them go through the
 * resolver instead. */
static void table_dir(const char *data_dir, const char *table_name, char *out, size_t cap) {
    snprintf(out, cap, "%s/%s", data_dir, table_name);
}

/* Convenience wrapper used everywhere we used to write
 * `table_dir(db->data_dir, name, …)` — picks the right striped dir
 * automatically. */
static void table_dir_db(const tsdb_db_t *db, const char *name, char *out, size_t cap) {
    table_dir(db_resolve_table_dir(db, name), name, out, cap);
}

/* ---- DB open / close ---------------------------------------------------- */

int tsdb_open(const char *data_dir, tsdb_db_t **out) {
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    if (tsdb_mkdir_p(data_dir) < 0) return TSDB_ERR_IO;

    tsdb_db_t *db = calloc(1, sizeof(*db));
    if (!db) return TSDB_ERR_NOMEM;

    snprintf(db->data_dir, sizeof(db->data_dir), "%s", data_dir);
    pthread_mutex_init(&db->lock, NULL);

    /* TSDB_DATA_DIRS = "/disk1/tsdb,/disk2/tsdb,/disk3/tsdb"
     *
     * When set, the data_dir argument acts as the "primary" (still hosts
     * the catalog, WAL, audit, etc) AND is added as the first entry
     * unless already listed.  Tables get distributed across all entries
     * by hash(name) % N.  Each directory is mkdir -p'd so a fresh
     * install just works.  When unset n_data_dirs stays 0 and every
     * code path falls back to the single primary dir (zero behaviour
     * change for existing deployments). */
    {
        const char *dirs_env = getenv("TSDB_DATA_DIRS");
        if (dirs_env && *dirs_env) {
            char buf[16384];
            snprintf(buf, sizeof(buf), "%s", dirs_env);
            int n = 0;
            char *save = NULL;
            /* Reserve slot 0 for the primary so it's always queried first.
             * mkdir_p on it already succeeded above; still health-probe it
             * so a sick primary routes new tables to the other stripes. */
            snprintf(db->data_dirs[n], sizeof(db->data_dirs[0]), "%s", db->data_dir);
            db->dir_ok[n] = db_dir_healthy(db->data_dir) ? 1 : 0;
            n++;
            for (char *tok = strtok_r(buf, ",;", &save);
                 tok && n < TSDB_MAX_DATA_DIRS;
                 tok = strtok_r(NULL, ",;", &save)) {
                /* Trim leading whitespace. */
                while (*tok == ' ' || *tok == '\t') tok++;
                if (!*tok) continue;
                /* Skip duplicates of the primary or earlier entries. */
                int dup = 0;
                for (int i = 0; i < n; i++)
                    if (strcmp(db->data_dirs[i], tok) == 0) { dup = 1; break; }
                if (dup) continue;
                /* Keep EVERY configured dir — a failing one is adopted
                 * DEGRADED (dir_ok=0) instead of dropped, so n_data_dirs
                 * never shrinks and hash placement of the healthy dirs is
                 * identical to a fully-healthy boot.  trash_gc_main
                 * re-adopts it once it probes healthy again. */
                int ok = 1;
                if (tsdb_mkdir_p(tok) < 0) {
                    fprintf(stderr, "[db] TSDB_DATA_DIRS: mkdir %s failed (%s) — "
                            "keeping slot, marked degraded\n",
                            tok, strerror(errno));
                    ok = 0;
                }
                /* Unmounted-mountpoint guard: existing is not enough — the
                 * fs must answer statvfs and grant write access right now,
                 * or the slot starts degraded. */
                if (ok && !db_dir_healthy(tok)) {
                    fprintf(stderr, "[db] TSDB_DATA_DIRS: %s failed statvfs/W_OK "
                            "probe — keeping slot, marked degraded\n", tok);
                    ok = 0;
                }
                snprintf(db->data_dirs[n], sizeof(db->data_dirs[0]), "%s", tok);
                db->dir_ok[n] = (uint8_t)ok;
                n++;
            }
            db->n_data_dirs = n;
            if (n > 1) {
                fprintf(stderr, "[db] data striping enabled across %d dirs:\n", n);
                for (int i = 0; i < n; i++)
                    fprintf(stderr, "      [%d] %s%s\n", i, db->data_dirs[i],
                            db->dir_ok[i] ? "" : "  (degraded)");
            }
        }
    }

    /* Opt-in: commit only syncs WAL, memtable flush deferred to is_full().
     * Remember whether the operator set the env at all — an explicit value
     * (including "0") always wins over the iopolicy auto-tune below. */
    const char *wc = getenv("TSDB_WAL_ONLY_COMMIT");
    int wal_env_set = (wc && wc[0]) ? 1 : 0;
    db->wal_only_commit = (wc && wc[0] && wc[0] != '0') ? 1 : 0;

    /* Open catalog — non-fatal on failure so plain SELECT workloads
     * still work even if catalog dir creation fails. */
    if (tsdb_catalog_open(data_dir, &db->catalog) != TSDB_OK) {
        db->catalog = NULL;
    }

    /* Open TMQ store alongside the catalog. Also non-fatal. */
    if (tsdb_tmq_open(data_dir, &db->tmq) != TSDB_OK) {
        db->tmq = NULL;
    }

    /* Open RBAC auth store. Non-fatal on failure. */
    if (tsdb_auth_open(data_dir, &db->auth) != TSDB_OK) {
        db->auth = NULL;
    }

    /* Open UDF catalog. Non-fatal; dlopen is lazy at first call. */
    if (tsdb_udf_catalog_open(data_dir, &db->udf) != TSDB_OK) {
        db->udf = NULL;
    }

    /* Open append-only audit log.  Non-fatal — the server keeps
     * running with audit disabled if the log file cannot be created
     * (rare in practice: the catalog/ dir already exists by here). */
    if (tsdb_audit_open(data_dir, &db->audit) != 0) {
        db->audit = NULL;
    }

    /* Hardware-adaptive defaults.  Detect storage class once (per the
     * iopolicy.h rules: env override → /sys/block rotational probe → SSD
     * fallback) and use it to pick write-path defaults; env vars below
     * still override individual knobs as an operator escape hatch.
     * Drives:
     *   - memtable_budget_rows (smaller on rotational → trash_gc_main
     *     flushes biggest table earlier, less device-queue contention)
     *   - compactor adaptive-pause threshold (in tsdb_compactor_start) */
    db->iopolicy = tsdb_iopolicy_detect(data_dir);

    /* Aggregate-memtable budget (rows across all tables).  Policy-based:
     *   SSD/NVMe : 32M  — generous; flushes fire at is_full(8192/table),
     *                     no proactive pressure (random I/O is cheap).
     *   SAS/HDD  :  8M  — earlier backpressure; rotational devices pay
     *                     per-flush seek cost so we'd rather flush bigger,
     *                     less often, and not race the compactor.
     * 0 = unbounded (legacy).  Env TSDB_MEMTABLE_BUDGET_ROWS overrides.
     * These fixed values are now only the fallback when the OS won't
     * report physical RAM — see the RAM-sized derivation just below. */
    switch (db->iopolicy) {
    case TSDB_IOPOLICY_HDD:
    case TSDB_IOPOLICY_SAS:
        db->memtable_budget_rows = 8ull * 1000 * 1000;
        break;
    case TSDB_IOPOLICY_SSD:
    default:
        db->memtable_budget_rows = 32ull * 1000 * 1000;
    }
    {
        const char *mb = getenv("TSDB_MEMTABLE_BUDGET_ROWS");
        if (mb && *mb) { long long v = atoll(mb); if (v >= 0) db->memtable_budget_rows = (uint64_t)v; }
    }

    /* Flush-cadence auto-tune: on rotational storage fsync dominates the
     * write path — flush-per-commit measured 69.5k rows/s vs 188.5k with
     * WAL-only commit (2.71x) on a 4.3 MB/s O_DSYNC disk — so default to
     * deferred flush on HDD/SAS.  Guarded three ways:
     *   - explicit TSDB_WAL_ONLY_COMMIT (including "0") always wins;
     *   - SSD keeps flush-on-commit (fsync is cheap there, gap ~11%);
     *   - requires a bounded memtable budget (> 0): deferred flush drains
     *     via memtable_over_budget/maybe_flush_b, which are inert at
     *     budget 0 (TSDB_MEMTABLE_BUDGET_ROWS=0 = legacy unbounded), so
     *     auto-enabling there would grow the memtable without limit. */
    if (!wal_env_set && db->iopolicy != TSDB_IOPOLICY_SSD &&
        db->memtable_budget_rows > 0) {
        db->wal_only_commit = 1;
        db->wal_only_auto   = 1;   /* cleared if this becomes a cluster node */
        fprintf(stderr,
                "[db] auto-enabled wal_only_commit for iopolicy=%s "
                "(fsync-bound); set TSDB_WAL_ONLY_COMMIT=0 to force "
                "flush-on-commit\n",
                tsdb_iopolicy_name(db->iopolicy));
    }
    fprintf(stderr,
            "[db] iopolicy=%s  memtable_budget_rows=%llu  wal_only_commit=%d\n",
            tsdb_iopolicy_name(db->iopolicy),
            (unsigned long long)db->memtable_budget_rows,
            db->wal_only_commit);

    /* Background reclaim of trashed (dropped) table dirs; also drains any
     * leftovers from a crash mid-reclaim. */
    db->trash_gc_running = 1;
    if (pthread_create(&db->trash_gc_thread, NULL, trash_gc_main, db) != 0)
        db->trash_gc_running = 0;

    *out = db;
    return TSDB_OK;
}

void tsdb_close(tsdb_db_t *db) {
    if (!db) return;

    /* Stop the trash GC before tearing the db down. */
    if (db->trash_gc_running) {
        db->trash_gc_running = 0;
        pthread_join(db->trash_gc_thread, NULL);
    }

    pthread_mutex_lock(&db->lock);

    for (int i = 0; i < db->ntables; i++) {
        tsdb_table_internal_t *t = db->tables[i];
        if (!t) continue;

        /* Flush remaining memtable data through the normal flush path, NOT a
         * bare tsdb_part_flush: only flush_and_clear_locked stamps the WAL redo
         * checkpoint (commit_seq) into the partitions it publishes and truncates
         * the log afterwards.  A bare part_flush passes max_seq=0, and
         * tsdb_part_flush_ex2 only ever RAISES a partition's checkpoint, so the
         * rows land on disk while their redo records stay live with
         * seq > checkpoint — the next open replays them ON TOP of the partition
         * close just wrote, and every graceful restart duplicates the tail again.
         * It also persists SYMBOL dictionaries before publishing the blocks that
         * reference them.
         *
         * Lock order is db->lock → compact_mtx, already established by
         * redo_recover_table under tsdb_open_table; nothing in the tree takes
         * compact_mtx and then db->lock (the compactor acquires/releases the
         * table under db->lock strictly outside its compact_mtx swap window),
         * so there is no inversion.  skip_replicate=1 keeps today's semantics:
         * a bare part_flush fired no replication hook. */
        if (t->memtable) {
            pthread_mutex_lock(&t->compact_mtx);
            (void)flush_and_clear_locked(t, /*skip_replicate=*/1);
            pthread_mutex_unlock(&t->compact_mtx);
        }

        /* Stop group-commit bg thread before closing WAL. */
        if (t->gc) { tsdb_group_commit_stop(t->gc); t->gc = NULL; }
        if (t->wal) { tsdb_wal_sync(t->wal); tsdb_wal_close(t->wal); }
        if (t->memtable) tsdb_memtable_free(t->memtable);

        /* Save symbol tables. */
        if (t->schema) {
            for (int ci = 0; ci < t->schema->ncols; ci++) {
                if (t->schema->cols[ci].type == TSDB_TYPE_SYMBOL &&
                    t->schema->cols[ci].symtab) {
                    char sym_path[4096];
                    snprintf(sym_path, sizeof(sym_path), "%s/%s.sym",
                             t->schema->dir, t->schema->cols[ci].name);
                    tsdb_symtab_save(t->schema->cols[ci].symtab, sym_path);
                }
            }
            tsdb_schema_free(t->schema);
        }
        pthread_mutex_destroy(&t->compact_mtx);
        pthread_mutex_destroy(&t->batch_mu);
        free(t);
    }

    if (db->udf)     tsdb_udf_catalog_close(db->udf);
    if (db->audit)   tsdb_audit_close(db->audit);
    if (db->auth)    tsdb_auth_close(db->auth);
    if (db->tmq)     tsdb_tmq_close(db->tmq);
    if (db->catalog) tsdb_catalog_close(db->catalog);

    /* Stop retention GC before destroying the mutex. */
    struct tsdb_retention *ret = db->retention;
    db->retention = NULL;

    pthread_mutex_unlock(&db->lock);

    /* Retention stop may block briefly for the current sweep to finish. */
    if (ret) tsdb_retention_stop(ret);

    pthread_mutex_destroy(&db->lock);
    free(db);
}

/* ---- Catalog hot-reopen ------------------------------------------------- */

/*
 * Rebuild the in-memory catalog from <data_dir>/catalog/*.log.
 *
 * The Raft InstallSnapshot receive path atomically swaps the on-disk
 * catalog files, but without this call the live tsdb_catalog_t hashmap
 * still reflects the pre-snapshot state — the node would need a process
 * restart to pick up the leader's catalog.  The 30 s catalog anti-entropy
 * tick (tsdb_catalog_reconcile_from_peers) calls it too, on any node that
 * learns a record from a peer.
 *
 * Ordering is deliberate: open-new before adopt.  If the fresh catalog
 * fails to open (disk read error, corrupt log, OOM) the live catalog is
 * untouched and the caller gets the open rc back.  Callers decide whether
 * to retry or surface the failure.
 *
 * The fresh state is then MOVED into the live catalog object rather than
 * the object being swapped and the old one freed.  Every read path caches
 * the pointer — `tsdb_catalog_t *cat = tsdb_db_catalog(db);` at exec.c:5849,
 * :6382, :8332, :8609, db.c:841, :1416, db_cluster.c:1924,
 * catalog_check.c:53 — and tsdb_db_catalog() is a bare `return db->catalog`
 * with no refcount and no lock.  Freeing the old object left every one of
 * those holding freed memory; a reconcile tick that landed inside a
 * statement segfaulted the node.  Keeping the object's identity removes the
 * hazard without touching any of those call sites.
 */
int tsdb_db_reload_catalog(tsdb_db_t *db) {
    if (!db) return TSDB_ERR_INVAL;

    tsdb_catalog_t *new_cat = NULL;
    int rc = tsdb_catalog_open(db->data_dir, &new_cat);
    if (rc != TSDB_OK) return rc;

    pthread_mutex_lock(&db->lock);
    tsdb_catalog_t *live = db->catalog;
    if (!live) db->catalog = new_cat;      /* nothing to adopt into */
    pthread_mutex_unlock(&db->lock);

    if (live) tsdb_catalog_adopt(live, new_cat);
    return TSDB_OK;
}

/* ---- Retention GC attachment -------------------------------------------- */

void tsdb_db_attach_retention(tsdb_db_t *db, struct tsdb_retention *r) {
    if (!db) return;
    pthread_mutex_lock(&db->lock);
    struct tsdb_retention *old = db->retention;
    db->retention = r;
    pthread_mutex_unlock(&db->lock);
    if (old) tsdb_retention_stop(old);
}

struct tsdb_retention *tsdb_db_retention(tsdb_db_t *db) {
    return db ? db->retention : NULL;
}

/* ---- Table name index (open addressing, must hold db->lock) ------------- */

/* Capacity (power of two) and mask for tbl_index[].  Load factor never
 * exceeds 0.5 because ntables is capped at TSDB_DB_MAX_TABLES. */
#define TBL_INDEX_CAP  (TSDB_DB_MAX_TABLES * 2)
#define TBL_INDEX_MASK (TBL_INDEX_CAP - 1)

/* Tombstone sentinel for a deleted slot: distinct from NULL (empty) so a
 * linear probe continues past a removed entry instead of stopping early.
 * The address of a static is a stable, never-a-real-table pointer value. */
static tsdb_table_internal_t *const TBL_TOMBSTONE =
    (tsdb_table_internal_t *)(void *)&(char){0};

/* Insert t under its name.  Caller holds db->lock and guarantees the name is
 * not already present (every callsite checks via db_find_table first, or is
 * inserting a freshly-created unique table).  Reuses the first tombstone seen
 * so deleted slots don't permanently lengthen probe chains. */
static void tbl_index_insert(tsdb_db_t *db, tsdb_table_internal_t *t) {
    uint64_t h = db_fnv1a(t->name);
    size_t i = (size_t)(h & TBL_INDEX_MASK);
    size_t first_tomb = TBL_INDEX_CAP;   /* sentinel: none seen yet */
    for (size_t probe = 0; probe < TBL_INDEX_CAP; probe++) {
        tsdb_table_internal_t *slot = db->tbl_index[i];
        if (slot == NULL) {
            db->tbl_index[(first_tomb != TBL_INDEX_CAP) ? first_tomb : i] = t;
            return;
        }
        if (slot == TBL_TOMBSTONE) {
            if (first_tomb == TBL_INDEX_CAP) first_tomb = i;
        }
        i = (i + 1) & TBL_INDEX_MASK;
    }
    /* Unreachable: the map can never be full (load factor <= 0.5). */
}

/* Remove the entry for `name` (a no-op if absent).  Caller holds db->lock.
 * Writes a tombstone, not NULL, so probe chains over this slot survive. */
static void tbl_index_remove(tsdb_db_t *db, const char *name) {
    uint64_t h = db_fnv1a(name);
    size_t i = (size_t)(h & TBL_INDEX_MASK);
    for (size_t probe = 0; probe < TBL_INDEX_CAP; probe++) {
        tsdb_table_internal_t *slot = db->tbl_index[i];
        if (slot == NULL) return;                       /* not present */
        if (slot != TBL_TOMBSTONE && strcmp(slot->name, name) == 0) {
            db->tbl_index[i] = TBL_TOMBSTONE;
            return;
        }
        i = (i + 1) & TBL_INDEX_MASK;
    }
}

/* ---- Table lookup (must hold db->lock) ---------------------------------- */

static tsdb_table_internal_t *db_find_table(tsdb_db_t *db, const char *name) {
    uint64_t h = db_fnv1a(name);
    size_t i = (size_t)(h & TBL_INDEX_MASK);
    for (size_t probe = 0; probe < TBL_INDEX_CAP; probe++) {
        tsdb_table_internal_t *slot = db->tbl_index[i];
        if (slot == NULL) return NULL;                  /* empty slot: miss */
        if (slot != TBL_TOMBSTONE && strcmp(slot->name, name) == 0)
            return slot;
        i = (i + 1) & TBL_INDEX_MASK;
    }
    return NULL;
}

/* ---- Forward declarations ----------------------------------------------- */

/* Defined at end of file; called from create_table_impl and open_table. */
static void maybe_start_gc_for_table(tsdb_db_t *db, tsdb_table_internal_t *t);

/* WAL redo recovery (defined alongside flush): rebuild a freshly-opened
 * table's unflushed WAL tail into its memtable.  Called from tsdb_open_table. */
static void redo_recover_table(tsdb_db_t *db, tsdb_table_internal_t *t);

/* Register a freshly-created plain table into the super-table hierarchy as a
 * data-bearing VTable: a super-table whose own same-named storage IS its data
 * (it has no child rows).  This makes every data table uniformly part of
 * Database→Group→VTable and carries it in stables.log, which cluster
 * catalog-sync replicates.  It registers NO child table — so it never collides
 * with a real `CREATE TABLE x USING st` child named x.  A SELECT on the table
 * dispatches to the super-table executor, which (finding zero children) reads
 * the same-named plain table back via the stable-child guard in exec.c.  Pure
 * catalog metadata: storage, writes, and the plain-table read path are
 * untouched.  Best-effort; skips names/widths beyond the catalog limits. */
/* Set while creating the *storage* for a real child table (CREATE TABLE x USING
 * st): that storage table must NOT be mirrored as its own super-table, or it
 * would (a) shadow its role as a child of st and (b) wrongly accept super-table
 * operations like PARTITION BY tbname.  Defined here, set around the child
 * storage create in exec.c. */
__thread int tsdb_g_creating_child = 0;

static void register_table_as_stable(tsdb_db_t *db, const char *name,
                                      tsdb_schema_t *schema) {
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    if (!cat || !schema) return;
    if (tsdb_g_creating_child) return;                  /* storage of a real child */
    if (strlen(name) > TSDB_STABLE_NAME_MAX) return;    /* must round-trip as a name */
    if (schema->ncols <= 0 || schema->ncols > TSDB_STABLE_MAX_COLS) return;
    if (tsdb_stable_exists(cat, name)) return;          /* already hierarchical */
    tsdb_child_table_t existing_child;
    if (tsdb_child_table_get(cat, name, &existing_child) == TSDB_OK)
        return;                                         /* already a child of some stable */

    tsdb_stable_t st;
    memset(&st, 0, sizeof(st));
    snprintf(st.name, sizeof(st.name), "%s", name);
    st.ncols      = schema->ncols;
    st.ts_col_idx = schema->ts_col_idx;
    st.ntag_cols  = 0;
    st.created_at = tsdb_now_ns();
    for (int i = 0; i < schema->ncols; i++) {
        snprintf(st.cols[i].name, sizeof(st.cols[i].name), "%s", schema->cols[i].name);
        st.cols[i].type = schema->cols[i].type;
    }
    /* Register the VTable only — NO child table.  A data-bearing super-table
     * has zero child rows; its data is the same-named plain table, which the
     * executor reads back via the stable-child fallback.  Registering a child
     * named `name` would collide with a real `CREATE TABLE name USING st`. */
    (void)tsdb_stable_create(cat, &st);
}

/* ---- tsdb_create_table -------------------------------------------------- */

/* Internal: create table with optional hook suppression.
 * block_points == 0 means "use db->default_block_points (or library default)". */
static int create_table_impl(tsdb_db_t *db,
                              const char *name,
                              const tsdb_col_t *cols, size_t ncols,
                              const char *ts_col,
                              tsdb_partition_unit_t part_unit,
                              int block_points,
                              int suppress_hook,
                              uint64_t forced_incarnation)
{
    if (!db || !name || !cols || ncols == 0 || !ts_col) return TSDB_ERR_INVAL;
    if (strlen(name) > TSDB_MAX_NAME) return TSDB_ERR_INVAL;
    if (ncols > TSDB_MAX_COLS) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&db->lock);

    if (db_find_table(db, name)) {
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_EXISTS;
    }

    if (db->ntables >= TSDB_DB_MAX_TABLES) {
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_FULL;
    }

    char dir[4096];
    table_dir_db(db, name, dir, sizeof(dir));

    /* Resolve block_points: explicit arg wins; otherwise fall back to the
     * db-wide default (0 → library default inside schema_create_ex). */
    int bp = (block_points > 0) ? block_points : db->default_block_points;

    /* Table incarnation.  An originating CREATE (hook NOT suppressed) mints a
     * fresh, non-zero value so a DROP+recreate of the same name never reuses
     * the old identity; the fresh value then rides the SCHEMA_SYNC wire to
     * followers via the on_create hook.  A replica apply (suppress_hook=1)
     * stamps the incarnation it was handed — the leader's value for the
     * SCHEMA_SYNC path, or 0 (UNKNOWN) for pre-incarnation senders and other
     * local-create callers — so leader and follower agree and same-incarnation
     * WRITE_BATCHes still match.
     *
     * A CREATE that is a peer's BROADCAST CATALOG DDL being replayed here is
     * NOT an origination, even though it arrives on the unsuppressed path: the
     * catalog fanout ships the DDL as QTL TEXT and every peer re-executes it,
     * so minting here gave each node a DIFFERENT incarnation for the SAME
     * table.  The WRITE_BATCH gate then saw two non-zero values that differ and
     * rejected in BOTH directions — nodes silently stopped replicating to each
     * other and each kept only its own writes (measured live: one table held
     * four distinct incarnations; replicate_incarnation_reject_total climbed
     * while counts split complementarily).  Stamp UNKNOWN(0) on a replay, the
     * same "cannot verify, never reject" value tsdb_create_table_local uses for
     * every other non-SCHEMA_SYNC replica path.  The origin keeps its non-zero
     * value, so a stale batch aimed at the recreated table is still refused
     * there. */
    extern __thread int      tsdb_g_suppress_catalog_broadcast;
    extern __thread uint64_t tsdb_g_raft_apply_seed;
    uint64_t incarnation;
    if (suppress_hook) {
        incarnation = forced_incarnation;
    } else if (!tsdb_g_suppress_catalog_broadcast) {
        incarnation = tsdb_schema_new_incarnation(name, dir);   /* origination */
    } else if (tsdb_g_raft_apply_seed != 0) {
        /* Replaying a COMMITTED RAFT entry.  Derive from the entry's
         * (term, index) — which every node applying this same entry sees
         * identically — mixed with the table name, so all nodes agree on one
         * non-zero value while a DROP+recreate (a different log index) gets a
         * different one.  That restores the gate inside a Raft cluster, where
         * stamping 0 had left it inert. */
        uint64_t h = tsdb_g_raft_apply_seed;
        for (const char *p = name; p && *p; p++) {
            h ^= (unsigned char)*p;
            h *= 1099511628211ULL;          /* FNV-1a mix, same as elsewhere */
        }
        incarnation = h ? h : 1ULL;
    } else {
        incarnation = 0;    /* non-Raft broadcast replay → UNKNOWN, never rejects */
    }

    tsdb_schema_t *schema = NULL;
    int rc = tsdb_schema_create_ex2(dir, name, cols, (int)ncols, ts_col,
                                    part_unit, bp, incarnation, &schema);
    if (rc != TSDB_OK) {
        pthread_mutex_unlock(&db->lock);
        return rc;
    }

    tsdb_table_internal_t *t = calloc(1, sizeof(*t));
    if (!t) {
        tsdb_schema_free(schema);
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_NOMEM;
    }
    strncpy(t->name, name, TSDB_MAX_NAME);
    t->schema = schema;
    t->db     = db;
    pthread_mutex_init(&t->compact_mtx, NULL);
    pthread_mutex_init(&t->batch_mu, NULL);

    rc = tsdb_memtable_new(schema, &t->memtable);
    if (rc != TSDB_OK) {
        pthread_mutex_destroy(&t->compact_mtx);
        pthread_mutex_destroy(&t->batch_mu);
        tsdb_schema_free(schema);
        free(t);
        pthread_mutex_unlock(&db->lock);
        return rc;
    }

    rc = tsdb_wal_open(db->data_dir, name, &t->wal);
    if (rc != TSDB_OK) {
        tsdb_memtable_free(t->memtable);
        tsdb_schema_free(schema);
        free(t);
        pthread_mutex_unlock(&db->lock);
        return rc;
    }

    /* Crash recovery for the create-over-existing path.  After a crash the
     * in-memory table index is empty, so the influx auto-create path (which
     * probes only that index) and idempotent SQL CREATE TABLE re-CREATE an
     * existing on-disk table through here — never through tsdb_open_table,
     * whose redo_recover_table (db.c:~1105) is the only other replay site.
     * Without this, a fsynced-but-unflushed WAL tail is invisible and then
     * destroyed by the first flush's WAL truncate — silent loss of acked
     * rows, and worse under wal_only_commit (larger unflushed tail).  A
     * genuinely new table has an empty WAL so this is a no-op, and the
     * seq<=checkpoint dedup makes it idempotent vs a later open_table. */
    redo_recover_table(db, t);

    db->tables[db->ntables++] = t;
    tbl_index_insert(db, t);   /* keep name index in sync with tables[] */
    __atomic_fetch_add(&db->table_set_gen, 1, __ATOMIC_RELAXED); /* invalidate write-path handle caches */

    /* Start per-table group-commit if db-level GC is active. */
    maybe_start_gc_for_table(db, t);

    /* (d) Tag-sort opt-in via TSDB_BLOCK_SORT_BY_TAG env var.  Applied
     * here BEFORE the cluster on_create hook fires, so the resulting
     * sort_by_tag_col travels in the SCHEMA_SYNC v2 payload tail to all
     * followers — they create their local tables with the same flag
     * without re-reading the env var.  Suppress_hook=1 paths (SCHEMA_SYNC
     * receivers, raft replay) skip this lookup; they already received an
     * explicit sort_by_tag_col from the leader and must not override it. */
    if (!suppress_hook) {
        const char *sort_col_name = getenv("TSDB_BLOCK_SORT_BY_TAG");
        if (sort_col_name && sort_col_name[0]) {
            int found = -1;
            for (int i = 0; i < schema->ncols; i++) {
                if (strcmp(schema->cols[i].name, sort_col_name) == 0
                    && schema->cols[i].type == TSDB_TYPE_SYMBOL) {
                    found = i;
                    break;
                }
            }
            if (found >= 0) {
                /* schema_save inside the setter is best-effort here.
                 * If it fails the in-memory flag still reflects the
                 * intent and writes still tag-sort, but rolling restart
                 * may lose the setting — the setter logs nothing yet, so
                 * surface the rc via stderr for diagnosability. */
                int rc2 = tsdb_schema_set_sort_by_tag_col(schema, found);
                if (rc2 != TSDB_OK) {
                    fprintf(stderr,
                            "[tsdb] TSDB_BLOCK_SORT_BY_TAG=%s: schema_save failed for table %s: rc=%d\n",
                            sort_col_name, name, rc2);
                }
            }
            /* If the named column is missing or not SYMBOL, silently
             * stay at sort_by_tag_col=-1.  This is an opt-in hint, not
             * a hard requirement, so a global env var pointing at a
             * column that doesn't exist on every table is not fatal. */
        }
    }

    /* Snapshot hook pointers under lock, then call after unlock to avoid
     * deadlock if the hook re-enters any db operation. */
    tsdb_on_create_fn on_create = suppress_hook ? NULL : db->on_create;
    void *hook_ud = db->hook_ud;

    pthread_mutex_unlock(&db->lock);

    /* Cluster schema-sync hook (if registered and not suppressed).
     *
     * The rc used to be dropped on the floor, which was harmless only for as
     * long as the receiver ACKed unconditionally — it now reports a schema it
     * failed to create, so this is where that report arrives.  It does NOT
     * fail the create: the local table is already built and durable, and
     * returning an error for a table that exists would make a retry answer
     * TSDB_ERR_EXISTS forever.  What it must not do is stay invisible, because
     * a peer without the schema silently refuses every WRITE_BATCH for it.
     * The periodic anti-entropy sweep (tsdb_resync_startup_thread) is what
     * closes the gap; this is how an operator knows one opened. */
    if (on_create) {
        int hrc = on_create(hook_ud, db, name, schema);
        if (hrc != TSDB_OK) {
            tsdb_metric_inc("qengine_schema_sync_fail_total");
            fprintf(stderr,
                    "[schema-sync] '%s': too few peers took the schema (rc=%d %s)"
                    " — they will refuse writes for it until anti-entropy\n",
                    name, hrc, tsdb_errstr(hrc));
        }
    }

    /* Mirror the table into the super-table hierarchy (local catalog only) —
     * ONLY on the originating (non-suppressed) create.  The suppress_hook=1
     * paths are replicas applying someone else's create: SCHEMA_SYNC receivers
     * (which provision the STORAGE of a real `CREATE TABLE x USING st` child —
     * mirroring those would forge a phantom super-table for the child, since the
     * tsdb_g_creating_child guard is thread-local and unset on the RPC thread),
     * raft replay, and anti-entropy materialize.  Replicas instead receive the
     * mirror VTable through catalog-sync of the originator's stables.log, so the
     * catalog stays consistent without ever mirroring a replicated child. */
    if (!suppress_hook)
        register_table_as_stable(db, name, schema);

    return TSDB_OK;
}

int tsdb_create_table(tsdb_db_t *db,
                      const char *name,
                      const tsdb_col_t *cols, size_t ncols,
                      const char *ts_col)
{
    return create_table_impl(db, name, cols, ncols, ts_col,
                             TSDB_PARTITION_DAY,
                             /*block_points*/ 0,
                             /*suppress_hook*/ 0,
                             /*forced_incarnation*/ 0);
}

int tsdb_create_table_ex(tsdb_db_t *db,
                         const char *name,
                         const tsdb_col_t *cols, size_t ncols,
                         const char *ts_col,
                         tsdb_create_partition_t partition)
{
    tsdb_partition_unit_t unit = (partition == TSDB_CREATE_PART_HOUR)
                                  ? TSDB_PARTITION_HOUR
                                  : TSDB_PARTITION_DAY;
    return create_table_impl(db, name, cols, ncols, ts_col, unit,
                             /*block_points*/ 0,
                             /*suppress_hook*/ 0,
                             /*forced_incarnation*/ 0);
}

int tsdb_create_table_ex2(tsdb_db_t *db,
                          const char *name,
                          const tsdb_col_t *cols, size_t ncols,
                          const char *ts_col,
                          tsdb_create_partition_t partition,
                          int block_points)
{
    tsdb_partition_unit_t unit = (partition == TSDB_CREATE_PART_HOUR)
                                  ? TSDB_PARTITION_HOUR
                                  : TSDB_PARTITION_DAY;
    return create_table_impl(db, name, cols, ncols, ts_col, unit,
                             block_points,
                             /*suppress_hook*/ 0,
                             /*forced_incarnation*/ 0);
}

int tsdb_create_table_local(tsdb_db_t *db,
                             const char *name,
                             const tsdb_col_t *cols, size_t ncols,
                             const char *ts_col)
{
    /* Non-SCHEMA_SYNC replica paths (hierarchy mirror, catalog reconcile) have
     * no incarnation to carry — stamp UNKNOWN (0).  Their tables fall back to
     * "cannot verify" on the WRITE_BATCH apply path, and never reject. */
    return create_table_impl(db, name, cols, ncols, ts_col,
                             TSDB_PARTITION_DAY,
                             /*block_points*/ 0,
                             /*suppress_hook*/ 1,
                             /*forced_incarnation*/ 0);
}

int tsdb_create_table_local_ex(tsdb_db_t *db,
                                const char *name,
                                const tsdb_col_t *cols, size_t ncols,
                                const char *ts_col,
                                int partition_unit_int,
                                int block_points,
                                int sort_by_tag_col,
                                uint64_t incarnation)
{
    tsdb_partition_unit_t unit = (partition_unit_int == (int)TSDB_PARTITION_HOUR)
                                  ? TSDB_PARTITION_HOUR
                                  : TSDB_PARTITION_DAY;
    /* Replica of a peer's CREATE: stamp the leader's incarnation (0 from a
     * pre-incarnation sender) so a follower agrees with the leader and
     * same-incarnation WRITE_BATCHes match instead of being rejected. */
    int rc = create_table_impl(db, name, cols, ncols, ts_col, unit,
                               block_points,
                               /*suppress_hook*/ 1,
                               /*forced_incarnation*/ incarnation);
    if (rc != TSDB_OK) return rc;
    if (sort_by_tag_col >= 0) {
        /* Apply the leader's tag-sort choice to the freshly-created
         * follower schema.  Failures here would mean the follower picked
         * up the table at the wrong layout — surface them so the SCHEMA_SYNC
         * caller can mark this peer as schema-divergent. */
        tsdb_table_t *t = NULL;
        if (tsdb_open_table(db, name, &t) != TSDB_OK || !t) return TSDB_ERR_INTERNAL;
        tsdb_schema_t *s = ((tsdb_table_internal_t *)t)->schema;
        if (!s) return TSDB_ERR_INTERNAL;
        rc = tsdb_schema_set_sort_by_tag_col(s, sort_by_tag_col);
    }
    return rc;
}

/* ---- tsdb_open_table ---------------------------------------------------- */

int tsdb_open_table(tsdb_db_t *db, const char *name, tsdb_table_t **out) {
    if (!db || !name || !out) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&db->lock);

    /* Check if already open. */
    tsdb_table_internal_t *t = db_find_table(db, name);
    if (t) {
        *out = (tsdb_table_t *)t;
        pthread_mutex_unlock(&db->lock);
        return TSDB_OK;
    }

    if (db->ntables >= TSDB_DB_MAX_TABLES) {
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_FULL;
    }

    char dir[4096];
    table_dir_db(db, name, dir, sizeof(dir));

    tsdb_schema_t *schema = NULL;
    int rc = tsdb_schema_open(dir, &schema);
    if (rc != TSDB_OK) {
        pthread_mutex_unlock(&db->lock);
        return rc;
    }

    t = calloc(1, sizeof(*t));
    if (!t) {
        tsdb_schema_free(schema);
        pthread_mutex_unlock(&db->lock);
        return TSDB_ERR_NOMEM;
    }
    strncpy(t->name, name, TSDB_MAX_NAME);
    t->schema = schema;
    t->db     = db;
    pthread_mutex_init(&t->compact_mtx, NULL);
    pthread_mutex_init(&t->batch_mu, NULL);

    rc = tsdb_memtable_new(schema, &t->memtable);
    if (rc != TSDB_OK) {
        pthread_mutex_destroy(&t->compact_mtx);
        pthread_mutex_destroy(&t->batch_mu);
        tsdb_schema_free(schema);
        free(t);
        pthread_mutex_unlock(&db->lock);
        return rc;
    }

    rc = tsdb_wal_open(db->data_dir, name, &t->wal);
    if (rc != TSDB_OK) {
        tsdb_memtable_free(t->memtable);
        tsdb_schema_free(schema);
        free(t);
        pthread_mutex_unlock(&db->lock);
        return rc;
    }

    /* Crash recovery: replay this table's unflushed WAL tail into the memtable
     * (records whose seq a partition checkpoint has not already superseded).
     * Runs on the FIRST open of an existing table this process, before it is
     * exposed.  A no-op when the WAL holds only legacy markers or nothing. */
    redo_recover_table(db, t);

    db->tables[db->ntables++] = t;
    tbl_index_insert(db, t);   /* keep name index in sync with tables[] */
    __atomic_fetch_add(&db->table_set_gen, 1, __ATOMIC_RELAXED); /* invalidate write-path handle caches */

    /* Start per-table group-commit if db-level GC is active. */
    maybe_start_gc_for_table(db, t);

    *out = (tsdb_table_t *)t;
    pthread_mutex_unlock(&db->lock);
    return TSDB_OK;
}

/* ---- tsdb_drop_table ---------------------------------------------------- */

/*
 * Recursively remove a directory and its contents.
 * Only goes one level deep for partition dirs (no recursive descent needed
 * for our layout), but we do a proper recursive walk for correctness.
 */
static void rm_rf(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        /* It may be a file. */
        remove(path);
        return;
    }
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
            continue;
        char sub[4096];
        snprintf(sub, sizeof(sub), "%s/%s", path, ent->d_name);
        rm_rf(sub);
    }
    closedir(d);
    rmdir(path);
}

/* Move a dropped table's storage dir out of the way fast (O(1) rename within
 * the same filesystem) instead of recursively removing it inline.  The bg GC
 * thread reclaims it later.  Falls back to inline rm_rf on a cross-fs / failed
 * rename so behaviour is never worse than before.  is_table_dir() rejects names
 * starting with '.', so .trash is invisible to every table dir-scan. */
static void trash_or_rm(tsdb_db_t *db, const char *tbl_dir) {
    /* Trash dir = <parent-of-tbl_dir>/.trash, keeping the rename same-fs even
     * under TSDB_DATA_DIRS striping. */
    char parent[4096];
    snprintf(parent, sizeof(parent), "%s", tbl_dir);
    char *slash = strrchr(parent, '/');
    const char *base;
    if (slash && slash != parent) { *slash = '\0'; base = slash + 1; }
    else { snprintf(parent, sizeof(parent), "%s", db ? db->data_dir : "."); base = "t"; }

    char trash_dir[4200];
    snprintf(trash_dir, sizeof(trash_dir), "%s/.trash", parent);
    if (tsdb_mkdir_p(trash_dir) < 0) { rm_rf(tbl_dir); return; }

    uint64_t seq = db ? __atomic_add_fetch(&db->trash_seq, 1, __ATOMIC_RELAXED) : 0;
    char dest[4400];
    snprintf(dest, sizeof(dest), "%s/%s.%llu", trash_dir, base, (unsigned long long)seq);
    if (rename(tbl_dir, dest) != 0)
        rm_rf(tbl_dir);   /* cross-fs or other failure → inline recursive remove */
}

/* Reclaim trashed table dirs off the hot path.  Each pass snapshots the .trash
 * entries (in every data dir) into a small batch, then rm_rf's them outside the
 * readdir cursor.  Also drains leftovers from a crash mid-reclaim on the first
 * pass after open. */
static void *trash_gc_main(void *arg) {
    tsdb_db_t *db = (tsdb_db_t *)arg;
    while (db->trash_gc_running) {
        int ndirs = db->n_data_dirs > 0 ? db->n_data_dirs : 1;
        for (int i = 0; i < ndirs && db->trash_gc_running; i++) {
            const char *dd = db->n_data_dirs > 0 ? db->data_dirs[i] : db->data_dir;
            /* Auto-recovery re-adopt: a dir degraded at open (mkdir/probe
             * failure) or by a flush I/O error is re-probed every pass and
             * flipped healthy once its fs answers statvfs + W_OK again
             * (e.g. operator remounted the disk).  Existing data was never
             * moved, so the dir serves reads/writes immediately. */
            if (db->n_data_dirs > 1 && !db->dir_ok[i] && db_dir_healthy(dd)) {
                db->dir_ok[i] = 1;
                fprintf(stderr, "[db] data dir [%d] %s healthy again — "
                        "re-adopted for new-table placement\n", i, dd);
            }
            char trash_dir[4200];
            snprintf(trash_dir, sizeof(trash_dir), "%s/.trash", dd);
            for (;;) {
                /* Pull one batch of names, then remove — avoids mutating the
                 * dir under an open readdir cursor. */
                DIR *d = opendir(trash_dir);
                if (!d) break;
                char batch[16][256];
                int nb = 0;
                struct dirent *e;
                while (nb < 16 && (e = readdir(d)) != NULL) {
                    if (e->d_name[0] == '.') continue;
                    snprintf(batch[nb++], sizeof(batch[0]), "%s", e->d_name);
                }
                closedir(d);
                if (nb == 0) break;
                for (int b = 0; b < nb && db->trash_gc_running; b++) {
                    char p[4500];
                    snprintf(p, sizeof(p), "%s/%s", trash_dir, batch[b]);
                    rm_rf(p);
                }
                if (!db->trash_gc_running) break;
            }
        }

        /* Aggregate-memtable budget: sum live rows across all tables and raise
         * the over-budget flag so the write path flushes under pressure.  Held
         * under db->lock so a concurrent drop can't free a table mid-sum (the
         * lock guards tables[] iteration).  Each per-table count is a relaxed
         * atomic load (tsdb_memtable_rows_relaxed) rather than a per-memtable
         * mutex acquire: the old version took every table's memtable lock under
         * db->lock — an O(ntables) lock-acquire storm every 2s contending with
         * all writers/queries.  The sum only feeds the over-budget heuristic,
         * so slightly stale per-table counts are fine.  O(ntables) and brief. */
        if (db->memtable_budget_rows > 0) {
            uint64_t total = 0;
            pthread_mutex_lock(&db->lock);
            for (int ti = 0; ti < db->ntables && db->trash_gc_running; ti++) {
                tsdb_table_internal_t *t = db->tables[ti];
                if (t && t->memtable) total += tsdb_memtable_rows_relaxed(t->memtable);
            }
            pthread_mutex_unlock(&db->lock);
            db->memtable_over_budget = (total > db->memtable_budget_rows) ? 1 : 0;
        }

        /* Sleep ~2s, waking often to honour shutdown. */
        for (int s = 0; s < 20 && db->trash_gc_running; s++) {
            struct timespec ts = { 0, 100 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

int tsdb_drop_table(tsdb_db_t *db, const char *name) {
    if (!db || !name) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&db->lock);

    int idx = -1;
    for (int i = 0; i < db->ntables; i++) {
        if (db->tables[i] && strcmp(db->tables[i]->name, name) == 0) {
            idx = i;
            break;
        }
    }

    /* Wait for any in-flight compaction pass OR query/scan on this table to
     * finish before freeing it — both hold t->schema (and compact_mtx) by raw
     * pointer with no lock, so freeing underneath them is a use-after-free.
     * Both flags are set/cleared under db->lock, so once we observe them 0
     * while holding the lock they cannot restart (they'd need the lock we
     * hold to re-acquire).
     *
     * Raise the `altering` drain barrier (same one ALTER uses) FIRST so new
     * tsdb_db_scan_acquire callers block instead of taking a fresh ref.  Without
     * it a sustained stream of concurrent DELETE/TRUNCATE keeps scan_refs > 0
     * forever and starves this DROP — a livelock.  With new acquirers held off,
     * in-flight scans drain to 0 and we make bounded progress.  DDL is
     * serialised, so this never races a real ALTER; the barrier dies with the
     * table when we free it below (no clear needed). */
    if (idx >= 0) db->tables[idx]->altering = 1;
    while (idx >= 0 && (db->tables[idx]->compacting ||
                        db->tables[idx]->scan_refs > 0)) {
        pthread_mutex_unlock(&db->lock);
        usleep(2000);
        pthread_mutex_lock(&db->lock);
        idx = -1;
        for (int i = 0; i < db->ntables; i++) {
            if (db->tables[i] && strcmp(db->tables[i]->name, name) == 0) {
                idx = i;
                break;
            }
        }
        if (idx >= 0) db->tables[idx]->altering = 1;  /* keep the barrier up across re-find */
    }

    /* Detach the table from the index + tables[] array UNDER db->lock (O(1)
     * pointer surgery), then free its heavy state AFTER releasing the lock.
     *
     * The slow part of a drop — tsdb_group_commit_stop joins a bg fsync thread
     * (possibly mid-fsync), tsdb_wal_close fsync+close, memtable/schema free —
     * used to run under db->lock.  In a 2000-child DROP DATABASE cascade
     * (exec.c loops tsdb_drop_table per child) that serialised 2000 thread-
     * joins under db->lock, blocking every concurrent WRITE_BATCH's
     * tsdb_open_table for seconds and starving the 10 ms raft tick → election
     * storm (#168).  Detach-then-free keeps the per-child lock hold to a few
     * pointer ops.
     *
     * Safe to free unlocked: the drain loop above guaranteed compacting==0 and
     * scan_refs==0 and raised the `altering` barrier, so no concurrent reader
     * holds `t`; once it is out of tbl_index + tables[] no NEW reader can find
     * it.  `t` is therefore solely owned by this thread from here on. */
    tsdb_table_internal_t *t_detached = NULL;
    if (idx >= 0) {
        t_detached = db->tables[idx];
        /* Drop the name-index entry first (writes a tombstone).  The index
         * stores pointers, so the tables[] array-compaction below never
         * invalidates it — only this dropped name must be removed. */
        tbl_index_remove(db, t_detached->name);

        /* Compact table array. */
        for (int i = idx; i < db->ntables - 1; i++)
            db->tables[i] = db->tables[i + 1];
        db->tables[--db->ntables] = NULL;
        __atomic_fetch_add(&db->table_set_gen, 1, __ATOMIC_RELAXED); /* invalidate write-path handle caches */
    }

    pthread_mutex_unlock(&db->lock);

    /* --- everything below runs WITHOUT db->lock --- */

    if (t_detached) {
        tsdb_table_internal_t *t = t_detached;
        /* Stop group-commit bg thread before closing WAL. */
        if (t->gc)       { tsdb_group_commit_stop(t->gc); t->gc = NULL; }
        if (t->wal)      tsdb_wal_close(t->wal);
        if (t->memtable) tsdb_memtable_free(t->memtable);
        if (t->schema)   tsdb_schema_free(t->schema);
        pthread_mutex_destroy(&t->compact_mtx);
        pthread_mutex_destroy(&t->batch_mu);
        free(t);
    }

    /* Remove WAL file. */
    char wal_path[4096];
    snprintf(wal_path, sizeof(wal_path), "%s/wal/%s.log", db->data_dir, name);
    remove(wal_path);

    /* Remove table directory. */
    char tbl_dir[4096];
    table_dir_db(db, name, tbl_dir, sizeof(tbl_dir));
    trash_or_rm(db, tbl_dir);   /* fast O(1) rename to .trash; bg GC reclaims */

    /* Tear down the super-table + child catalog rows mirrored at create time
     * (after releasing db->lock to keep db→catalog lock ordering clean).
     * Idempotent: a table that predates hierarchy-mirroring has neither. */
    {
        tsdb_catalog_t *cat = tsdb_db_catalog(db);
        if (cat) {
            (void)tsdb_child_table_drop(cat, name);
            /* Drop the mirrored VTable ONLY when it's a data-bearing mirror,
             * i.e. it has NO child tables.  A real super-table that happens to
             * share this name must not be torn down by DROP TABLE — that's DROP
             * STABLE's job, and tsdb_stable_drop cascade-wipes the whole child
             * set.  Use the atomic count+drop so a concurrent CREATE … USING
             * <name> can't race a child in between (a separate list-then-drop
             * would then cascade-wipe it), and a list error can't be mistaken
             * for "0 children". */
            (void)tsdb_stable_drop_if_childless(cat, name);
        }
    }
    return TSDB_OK;
}

/* ---- TRUNCATE TABLE ---------------------------------------------------- */

/* Return 1 if the entry name is metadata we must preserve across truncate:
 * schema.bin (columnar layout) or *.sym (per-column symbol dictionaries). */
static int truncate_keep_entry(const char *name) {
    if (strcmp(name, "schema.bin") == 0) return 1;
    size_t n = strlen(name);
    if (n >= 4 && strcmp(name + n - 4, ".sym") == 0) return 1;
    return 0;
}

/* Count durable partition rows for `tbl_dir` by summing each ts.idx header.
 * Caller holds the table's batch_mu + compact_mtx, so no flush can be
 * publishing a partition underneath the walk. */
static uint64_t table_dir_durable_rows(tsdb_db_t *db, const char *name) {
    char tbl_dir[4096];
    table_dir_db(db, name, tbl_dir, sizeof(tbl_dir));
    uint64_t total = 0;
    DIR *d = opendir(tbl_dir);
    if (!d) return 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        if (truncate_keep_entry(ent->d_name)) continue;   /* schema.bin, *.sym */
        char idx_path[4300];
        snprintf(idx_path, sizeof(idx_path), "%s/%s/ts.idx", tbl_dir, ent->d_name);
        uint64_t rows = 0;
        if (tsdb_part_idx_probe(idx_path, NULL, NULL, NULL, &rows, NULL, NULL, NULL) > 0)
            total += rows;
    }
    closedir(d);
    return total;
}

/* TRUNCATE, but abort atomically if the table is not empty under the lock.
 *
 * Anti-entropy hands out a destructive FULL_PULL only for a table it measured
 * EMPTY, and then truncates and pulls the peer's copy.  The measurement runs
 * OUTSIDE any table lock, so a WRITE_BATCH committing between the measurement
 * and the truncate is destroyed and the pull — sized to the peer — does not
 * bring it back: silent loss of a row no peer holds.  The caller's own
 * re-measure narrows that window but cannot close it, because it, too, runs
 * before batch_mu is taken.
 *
 * This closes it: the emptiness check runs INSIDE the same batch_mu +
 * compact_mtx the truncate holds, so a row_begin (which needs batch_mu) cannot
 * land between the check and the wipe.  *was_empty tells the caller whether the
 * truncate happened (1) or was aborted because rows appeared (0). */
int tsdb_truncate_table_if_empty(tsdb_db_t *db, const char *name,
                                 int *was_empty) {
    if (!db || !name) return TSDB_ERR_INVAL;
    if (was_empty) *was_empty = 0;

    tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, name);
    if (!t) return TSDB_ERR_NOTFOUND;

    pthread_mutex_lock(&t->batch_mu);
    pthread_mutex_lock(&t->compact_mtx);

    /* Emptiness under the lock: memtable rows + durable partition rows.  A
     * WRITE_BATCH commit path holds batch_mu, so it is either already counted
     * here or blocked until we release — it can never slip in during the wipe. */
    uint64_t mem_rows = (t->memtable) ? tsdb_memtable_rows(t->memtable) : 0;
    uint64_t dur_rows = table_dir_durable_rows(db, name);
    /* A FROZEN table (wal_incomplete) is NOT empty, however it measures.  An
     * aborted redo replay leaves acked records in the log that no partition and
     * no memtable covers; when the abort happened on the FIRST record nothing
     * applied at all, so mem_rows and dur_rows are both 0 and the table looks
     * empty here — while its WAL is the ONLY copy of those rows.  Truncating
     * then destroys acked data and this function even cleared wal_incomplete
     * below, erasing the evidence.  That is exactly the state the freeze exists
     * to protect: the abort path promises "no flush, no checkpoint advance, NO
     * TRUNCATE ... until the log is repaired or removed".  Report not-empty so
     * anti-entropy leaves the table alone; explicit TRUNCATE TABLE remains the
     * operator's deliberate way to unfreeze it. */
    if (mem_rows != 0 || dur_rows != 0 || t->wal_incomplete) {
        pthread_mutex_unlock(&t->compact_mtx);
        pthread_mutex_unlock(&t->batch_mu);
        tsdb_db_scan_release(db, t);
        return TSDB_OK;                 /* not empty — nothing destroyed */
    }

    if (t->memtable) tsdb_memtable_clear(t->memtable);
    if (t->wal)     (void)tsdb_wal_truncate(t->wal);
    t->mem_logged = 0;
    t->wal_incomplete = 0;
    /* Table measured empty on disk too, so there is nothing to rm_rf — but run
     * the same sweep the full truncate does, to clear any stray partition a
     * concurrent-but-now-quiesced flush may have left mid-publish. */
    {
        char tbl_dir[4096];
        table_dir_db(db, name, tbl_dir, sizeof(tbl_dir));
        DIR *d = opendir(tbl_dir);
        if (d) {
            struct dirent *ent;
            while ((ent = readdir(d)) != NULL) {
                if (ent->d_name[0] == '.') continue;
                if (truncate_keep_entry(ent->d_name)) continue;
                char sub[4096];
                snprintf(sub, sizeof(sub), "%s/%s", tbl_dir, ent->d_name);
                rm_rf(sub);
            }
            closedir(d);
        }
    }

    pthread_mutex_unlock(&t->compact_mtx);
    pthread_mutex_unlock(&t->batch_mu);
    tsdb_db_scan_release(db, t);
    if (was_empty) *was_empty = 1;
    return TSDB_OK;
}

int tsdb_truncate_table(tsdb_db_t *db, const char *name) {
    if (!db || !name) return TSDB_ERR_INVAL;

    /* scan_acquire so a concurrent DROP TABLE waits for us before freeing the
     * table — we drop db->lock then take batch_mu/compact_mtx, and the
     * scan_refs hold prevents DROP destroying those mutexes in the gap
     * (locked-after-destroy crash under drop+truncate stress). */
    tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, name);
    if (!t) return TSDB_ERR_NOTFOUND;

    /* Hold both locks: compact_mtx so no flush is mid-write, batch_mu so
     * no row_begin is in flight.  Order matches flush_and_clear_ex. */
    pthread_mutex_lock(&t->batch_mu);
    pthread_mutex_lock(&t->compact_mtx);

    if (t->memtable) tsdb_memtable_clear(t->memtable);
    if (t->wal)     (void)tsdb_wal_truncate(t->wal);
    /* The emptied memtable has nothing logged in the emptied WAL.  Leaving the
     * old high-water mark behind makes redo_log_locked skip every commit until
     * the memtable grows back past it, so the first rows written after a
     * TRUNCATE would never reach the redo log at all. */
    t->mem_logged = 0;
    /* Unfreeze a table whose replay aborted (see wal_incomplete): TRUNCATE
     * discards the very rows the freeze was protecting, and the log it was
     * protecting them in has just been emptied — deliberately, by an operator.
     * This is the escape hatch that keeps the freeze from being permanent. */
    t->wal_incomplete = 0;

    /* Remove every entry under the table dir except schema.bin and *.sym.
     * This kills all partition directories verbatim. */
    char tbl_dir[4096];
    table_dir_db(db, name, tbl_dir, sizeof(tbl_dir));
    DIR *d = opendir(tbl_dir);
    if (d) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            if (truncate_keep_entry(ent->d_name)) continue;
            char sub[4096];
            snprintf(sub, sizeof(sub), "%s/%s", tbl_dir, ent->d_name);
            rm_rf(sub);
        }
        closedir(d);
    }

    pthread_mutex_unlock(&t->compact_mtx);
    pthread_mutex_unlock(&t->batch_mu);
    tsdb_db_scan_release(db, t);
    return TSDB_OK;
}

/* ---- DELETE (partition-level time range) ------------------------------- */

/* Parse "YYYYMMDD" or "YYYYMMDDHH" into [start_ns, end_ns] inclusive.
 * Returns 0 on success, -1 if the dirname isn't a partition. */
static int part_dir_to_range(const char *dname, tsdb_partition_unit_t unit,
                             int64_t *out_start, int64_t *out_end)
{
    int y, mo, dy, hh = 0;
    size_t n = strlen(dname);
    if (unit == TSDB_PARTITION_HOUR) {
        if (n != 10) return -1;
        if (sscanf(dname, "%4d%2d%2d%2d", &y, &mo, &dy, &hh) != 4) return -1;
    } else {
        if (n != 8) return -1;
        if (sscanf(dname, "%4d%2d%2d", &y, &mo, &dy) != 3) return -1;
    }
    struct tm tm = {0};
    tm.tm_year = y - 1900;
    tm.tm_mon  = mo - 1;
    tm.tm_mday = dy;
    tm.tm_hour = hh;
    time_t secs = timegm(&tm);
    if (secs == (time_t)-1) return -1;
    int64_t start = (int64_t)secs * 1000000000LL;
    int64_t span  = (unit == TSDB_PARTITION_HOUR) ? 3600000000000LL
                                                  : 86400000000000LL;
    *out_start = start;
    *out_end   = start + span - 1;
    return 0;
}

/* Does a partition spanning [pstart,pend] fall ENTIRELY inside the delete
 * predicate?  DELETE is partition-granular: a partition that straddles the
 * cutoff is left alone (a partial delete would need a per-row rewrite, which
 * is not implemented), so only fully-covered partitions are dropped. */
static int part_fully_covered(int64_t pstart, int64_t pend,
                              int64_t cutoff_ns, int op_lt, int inclusive)
{
    if (op_lt) {
        /* Delete ts < cutoff (or ts <= cutoff when inclusive). */
        return inclusive ? (pend <= cutoff_ns) : (pend <  cutoff_ns);
    }
    /* Delete ts > cutoff (or ts >= cutoff when inclusive). */
    return inclusive ? (pstart >= cutoff_ns) : (pstart > cutoff_ns);
}

/* Inclusive [start,end] ns range of the partition a timestamp flushes into.
 * Mirrors ts_part_index() in part.c — floor division, so pre-epoch
 * timestamps land in the same bucket the flush would put them in. */
static void ts_part_range(int64_t ts_ns, tsdb_partition_unit_t unit,
                          int64_t *out_start, int64_t *out_end)
{
    int64_t span = (unit == TSDB_PARTITION_HOUR) ? 3600000000000LL
                                                 : 86400000000000LL;
    int64_t idx = (ts_ns >= 0) ? (ts_ns / span)
                               : ((ts_ns - span + 1) / span);
    *out_start = idx * span;
    *out_end   = *out_start + span - 1;
}

/* 1 if the memtable currently holds a row that lands in a partition this
 * delete would drop whole.  part_fully_covered is monotone in ts, so the
 * extreme row decides: the minimum ts for a "delete old data" (op_lt)
 * predicate, the maximum for "delete new data".  Caller holds compact_mtx. */
static int memtable_has_covered_row(tsdb_table_internal_t *t,
                                    int64_t cutoff_ns, int op_lt, int inclusive)
{
    if (!t->memtable || !t->schema) return 0;
    size_t n = tsdb_memtable_rows(t->memtable);
    if (n == 0) return 0;
    const int64_t *ts =
        (const int64_t *)tsdb_memtable_col(t->memtable, t->schema->ts_col_idx);
    if (!ts) return 0;

    int64_t extreme = ts[0];
    for (size_t i = 1; i < n; i++) {
        if (op_lt ? (ts[i] < extreme) : (ts[i] > extreme)) extreme = ts[i];
    }
    int64_t pstart = 0, pend = 0;
    ts_part_range(extreme, t->schema->partition_unit, &pstart, &pend);
    return part_fully_covered(pstart, pend, cutoff_ns, op_lt, inclusive);
}

/* Per-table delete watermark: "every row with ts < W has been deleted".
 * Persisted as 8 bytes LE in <tabledir>/_delwm.  Only op_lt deletes (the
 * retention "drop old data" pattern) advance it.  Anti-entropy re-asserts it
 * so a delete can't be resurrected by a peer that missed it.  0 = none. */
static void delwm_path(tsdb_db_t *db, const char *name, char *buf, size_t cap) {
    char tbl_dir[4096];
    table_dir_db(db, name, tbl_dir, sizeof(tbl_dir));
    snprintf(buf, cap, "%s/_delwm", tbl_dir);
}
int64_t tsdb_table_delwm_load(tsdb_db_t *db, const char *name) {
    if (!db || !name) return 0;
    char p[4096]; delwm_path(db, name, p, sizeof(p));
    FILE *f = fopen(p, "rb");
    if (!f) return 0;
    int64_t w = 0;
    if (fread(&w, sizeof(w), 1, f) != 1) w = 0;
    fclose(f);
    return w;
}
/* Advance the watermark to max(existing, W) and persist via tmp+fsync+rename.
 * Best-effort in the sense that a write failure leaves the OLD watermark
 * intact (a later delete re-advances it) — which the previous fopen(p,"wb")
 * did NOT deliver: "wb" truncates _delwm to 0 bytes BEFORE the write is
 * attempted, so a failed (or crash-interrupted) write left a 0-byte file that
 * tsdb_table_delwm_load reads back as "no watermark".  Anti-entropy then stops
 * re-asserting the delete (db_cluster.c gates on wm > 0), a peer that missed
 * the original delete wins the count comparison, and the deleted rows are
 * pulled back cluster-wide and stay back.  The tmp name carries the pid so two
 * concurrent tsdb_delete_range calls cannot share (and tear) one temp file.
 * The load-then-write lost-update race below pre-exists and is unchanged. */
static void delwm_bump(tsdb_db_t *db, const char *name, int64_t w) {
    if (w <= 0) return;
    if (tsdb_table_delwm_load(db, name) >= w) return;
    char p[4096]; delwm_path(db, name, p, sizeof(p));
    char tmp[4200];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%ld", p, (long)getpid());
    FILE *f = fopen(tmp, "wb");
    if (!f) return;
    int ok = (fwrite(&w, sizeof(w), 1, f) == 1);
    if (ok && fflush(f) != 0)         ok = 0;
    if (ok && ferror(f))              ok = 0;
    if (ok && fsync(fileno(f)) != 0)  ok = 0;
    if (fclose(f) != 0)               ok = 0;
    if (!ok)                       { unlink(tmp); return; }
    if (rename(tmp, p) != 0)         unlink(tmp);
}

int tsdb_delete_range(tsdb_db_t *db, const char *name,
                      int64_t cutoff_ns, int op_lt, int inclusive,
                      int *out_removed)
{
    if (!db || !name) return TSDB_ERR_INVAL;

    /* scan_acquire (not a bare find) so a concurrent DROP TABLE waits for us
     * to finish before freeing the table: we drop db->lock and then take
     * t->compact_mtx, and without the scan_refs hold DROP could free t (and
     * destroy compact_mtx) in that gap — a locked-after-destroy crash
     * (pthread ESRCH assertion) observed under drop+delete stress. */
    tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, name);
    if (!t || !t->schema) {
        if (t) tsdb_db_scan_release(db, t);
        return TSDB_ERR_NOTFOUND;
    }

    tsdb_partition_unit_t unit = t->schema->partition_unit;

    pthread_mutex_lock(&t->compact_mtx);

    /* Rows still in the memtable are invisible to the partition scan below.
     * With TSDB_WAL_ONLY_COMMIT (deferred flush — the cluster's configuration,
     * and auto-enabled on rotational disks) a commit leaves rows in the
     * memtable, so DELETE walked the partition dirs, matched nothing and still
     * returned TSDB_OK: the client is told the rows are gone while they are
     * not, and the next size-triggered flush writes them straight back to
     * disk.  Materialise them first so the partition rule below is applied to
     * the whole table, in both durability modes.
     *
     * Only when the memtable actually holds a row inside a partition this
     * delete would drop: the retention / delwm-reassert pattern (drop old data
     * while the memtable buffers recent rows) must not degrade into a forced
     * flush of every table on every 30s re-assert sweep.
     *
     * skip_replicate=1 — an internal materialisation step, not a client write
     * event peers should observe (same reasoning as tsdb_db_flush_all).  It
     * also keeps a peer applying APPLY_DELETE_RANGE from replicating the rows
     * back at the node that just deleted them. */
    int flush_rc = TSDB_OK;
    if (memtable_has_covered_row(t, cutoff_ns, op_lt, inclusive))
        flush_rc = flush_and_clear_locked(t, /*skip_replicate=*/1);

    char tbl_dir[4096];
    table_dir_db(db, name, tbl_dir, sizeof(tbl_dir));
    DIR *d = opendir(tbl_dir);
    int removed = 0;
    if (d) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            if (truncate_keep_entry(ent->d_name)) continue;

            int64_t pstart = 0, pend = 0;
            if (part_dir_to_range(ent->d_name, unit, &pstart, &pend) != 0)
                continue;  /* not a recognisable partition dir */

            if (part_fully_covered(pstart, pend, cutoff_ns, op_lt, inclusive)) {
                char sub[4096];
                snprintf(sub, sizeof(sub), "%s/%s", tbl_dir, ent->d_name);
                rm_rf(sub);
                removed++;
            }
        }
        closedir(d);
    }

    pthread_mutex_unlock(&t->compact_mtx);

    /* Materialisation failed → rows the predicate covers are still buffered.
     * Report that instead of a silent partial delete, and don't record a
     * watermark we did not achieve.  Partitions already dropped stay dropped;
     * delete is idempotent, so the caller (or the re-assert sweep) can retry. */
    if (flush_rc != TSDB_OK) {
        tsdb_db_scan_release(db, t);
        if (out_removed) *out_removed = removed;
        return flush_rc;
    }

    /* Advance the persisted delete watermark for op_lt ("delete old data")
     * deletes, so anti-entropy can re-assert it and a peer that missed this
     * delete (or resurrected it via a refetch) gets the rows re-deleted.
     * Stored as the exclusive upper bound W: every ts < W is deleted. */
    if (op_lt) {
        int64_t w = inclusive ? cutoff_ns + 1 : cutoff_ns;
        delwm_bump(db, name, w);
    }

    tsdb_db_scan_release(db, t);
    if (out_removed) *out_removed = removed;
    return TSDB_OK;
}

/* ---- PITR trim --------------------------------------------------------- *
 * Discard every partition strictly after `target_ns` across every open
 * table in the db.  Intended use: restore a /backup tarball, then call
 * tsdb_pitr_trim_to(target_ns) to roll the state forward to a specific
 * instant — anything written after that is dropped as if it never
 * happened.  Runs through tsdb_delete_range so the same partition-level
 * lock and unlink logic apply; the call is idempotent.
 *
 * Returns TSDB_OK with *out_parts_removed set to the total partition
 * count removed across all tables.  NOT cluster-wide: the caller is
 * expected to either restore the same backup on every node, or invoke
 * this on each node separately. */
int tsdb_pitr_trim_to(tsdb_db_t *db, int64_t target_ns,
                      int *out_parts_removed)
{
    if (!db) return TSDB_ERR_INVAL;

    /* Snapshot the table name list so we don't hold db->lock while
     * tsdb_delete_range re-acquires it.  Names are owned by the
     * tables themselves and outlive this call. */
    pthread_mutex_lock(&db->lock);
    int n = db->ntables;
    const char **names = n > 0 ? calloc((size_t)n, sizeof(*names)) : NULL;
    int k = 0;
    for (int i = 0; i < n; i++) {
        if (db->tables[i] && db->tables[i]->name[0])
            names[k++] = db->tables[i]->name;
    }
    pthread_mutex_unlock(&db->lock);

    int total = 0;
    for (int i = 0; i < k; i++) {
        int removed = 0;
        /* op_lt=0, inclusive=0 → drop partitions whose pstart > target_ns. */
        if (tsdb_delete_range(db, names[i], target_ns,
                              /*op_lt*/ 0, /*inclusive*/ 0,
                              &removed) == TSDB_OK) {
            total += removed;
        }
    }
    free(names);
    if (out_parts_removed) *out_parts_removed = total;
    return TSDB_OK;
}

/* ---- ALTER TABLE ADD COLUMN -------------------------------------------- */

int tsdb_alter_table_add_column(tsdb_db_t *db, const char *table_name,
                                 const char *col_name, tsdb_type_t col_type)
{
    if (!db || !table_name || !col_name) return TSDB_ERR_INVAL;

    /* Find the table, mark it `altering`, and drain in-flight scans to 0 — all
     * under db->lock.  `altering` makes new tsdb_db_scan_acquire callers wait,
     * and draining scan_refs waits out existing queries, so no reader is
     * iterating schema->cols when tsdb_schema_add_column reallocs it below.
     * DDL is serialised on the cluster, so the table can't be dropped here. */
    pthread_mutex_lock(&db->lock);
    tsdb_table_internal_t *t = NULL;
    for (int i = 0; i < db->ntables; i++) {
        if (db->tables[i] && strcmp(db->tables[i]->name, table_name) == 0) {
            t = db->tables[i]; break;
        }
    }
    if (!t) { pthread_mutex_unlock(&db->lock); return TSDB_ERR_NOTFOUND; }
    t->altering = 1;
    while (t->scan_refs > 0) {
        pthread_mutex_unlock(&db->lock);
        usleep(1000);
        pthread_mutex_lock(&db->lock);
    }
    pthread_mutex_unlock(&db->lock);

    /* Serialise with concurrent writes (batch_mu — the cluster ingest/replica
     * paths hold it) and with the compactor/flush (compact_mtx).  Same lock
     * order as the truncate path (batch_mu → compact_mtx) to avoid deadlock. */
    pthread_mutex_lock(&t->batch_mu);
    pthread_mutex_lock(&t->compact_mtx);

    /* Force memtable to disk so we can safely re-size its column buffers.
     * Use the _locked variant since we already hold compact_mtx — calling
     * the public flush_and_clear_ex here would self-deadlock. */
    int rc = flush_and_clear_locked(t, /*skip_replicate=*/1);
    if (rc == TSDB_OK) {
        /* Realloc of schema->cols is now safe: scans are drained + gated by
         * `altering`, writes are excluded by batch_mu, flush/compactor by
         * compact_mtx. */
        rc = tsdb_schema_add_column(t->schema, col_name, col_type);
        if (rc == TSDB_OK) {
            /* Grow the memtable's column buffer to match.  On failure the
             * on-disk schema is updated but the memtable can't ingest the new
             * column until reopen; surface the error unchanged. */
            rc = tsdb_memtable_extend_for_new_column(t->memtable);
        }
    }

    pthread_mutex_unlock(&t->compact_mtx);
    pthread_mutex_unlock(&t->batch_mu);

    pthread_mutex_lock(&db->lock);
    t->altering = 0;
    __atomic_fetch_add(&db->table_set_gen, 1, __ATOMIC_RELAXED); /* schema->cols realloced — drop cached handles */
    pthread_mutex_unlock(&db->lock);
    return rc;
}

/* ---- Batch API ---------------------------------------------------------- */

int tsdb_batch_begin(tsdb_table_t *tbl, tsdb_batch_t **out) {
    if (!tbl || !out) return TSDB_ERR_INVAL;

    tsdb_batch_t *b = calloc(1, sizeof(*b));
    if (!b) return TSDB_ERR_NOMEM;

    tsdb_table_internal_t *ti = (tsdb_table_internal_t *)tbl;
    b->db     = ti->db;
    b->tbl    = ti;
    b->in_row = 0;
    /* Snapshot the rollback anchors.  In wal_only mode this equals mem_logged
     * (steady state: a prior commit advanced mem_logged to nrows, and nothing
     * else mutates the memtable between commits); in default mode it is the
     * post-flush count (0 in steady state).  begin_flush_gen is a same-thread
     * read; only this table's writer bumps flush_gen, and it does so after
     * begin, so the compare at commit/discard is well-defined. */
    b->base_nrows      = tsdb_memtable_rows(ti->memtable);
    b->begin_flush_gen = __atomic_load_n(&ti->flush_gen, __ATOMIC_RELAXED);
    *out = b;
    return TSDB_OK;
}

/*
 * Internal: call replicate hook (if any), flush memtable to partition, clear.
 * Must be called with table's resources available.
 * Only flushes if the memtable has at least one row.
 * skip_replicate=1 suppresses the cluster hook (used for replica-received writes).
 */
/* ---- WAL redo log (TSDB_WAL_ONLY_COMMIT) -------------------------------- *
 *
 * Redo record payload (the CRC32+len framing is added by tsdb_wal_append):
 *
 *   [seq    : u64 LE]
 *   [nrows  : u32 LE]
 *   nrows × row, each row =
 *     [ts : i64 LE]                              -- the designated ts column
 *     then every NON-ts column in ascending schema index order:
 *       TIMESTAMP / INT64 / FLOAT64 / FLOAT32 : 8 bytes LE (raw i64 / double bits)
 *       SYMBOL                                 : [len : u32 LE][utf8 bytes]  (string)
 *
 * SYMBOL columns log the STRING (not the dict code) so replay re-interns via
 * tsdb_memtable_row_sym and the symbol-table ids are rebuilt correctly.
 */

static void redo_put_u32le(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static void redo_put_u64le(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8*i));
}
static uint32_t redo_get_u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static uint64_t redo_get_u64le(const uint8_t *p) {
    uint64_t v = 0; for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8*i); return v;
}

/* Append a growable byte buffer; returns 0 on success, -1 on OOM. */
struct redo_buf { uint8_t *p; size_t n, cap; };
static int redo_buf_reserve(struct redo_buf *b, size_t extra) {
    if (b->n + extra <= b->cap) return 0;
    size_t nc = b->cap ? b->cap * 2 : 256;
    while (nc < b->n + extra) nc *= 2;
    uint8_t *np = realloc(b->p, nc);
    if (!np) return -1;
    b->p = np; b->cap = nc;
    return 0;
}
static int redo_buf_put(struct redo_buf *b, const void *src, size_t n) {
    if (redo_buf_reserve(b, n) != 0) return -1;
    memcpy(b->p + b->n, src, n); b->n += n; return 0;
}

/*
 * Serialise memtable rows [from, to) of table t into a redo record carrying
 * `seq`, returning the malloc'd payload in *out / *out_n.  Caller frees *out.
 * Must be called with the rows stable (compact_mtx held).  Returns TSDB_OK or
 * TSDB_ERR_NOMEM.
 */
static int redo_serialize(tsdb_table_internal_t *t, uint64_t seq,
                          size_t from, size_t to,
                          uint8_t **out, size_t *out_n)
{
    tsdb_schema_t *s = t->schema;
    int ts_ci = s->ts_col_idx;
    struct redo_buf b = {0};
    uint8_t hdr[12];
    redo_put_u64le(hdr + 0, seq);
    redo_put_u32le(hdr + 8, (uint32_t)(to - from));
    if (redo_buf_put(&b, hdr, 12) != 0) { free(b.p); return TSDB_ERR_NOMEM; }

    const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(t->memtable, ts_ci);
    if (!ts_buf) { free(b.p); return TSDB_ERR_INTERNAL; }

    for (size_t r = from; r < to; r++) {
        uint8_t tsb[8];
        redo_put_u64le(tsb, (uint64_t)ts_buf[r]);
        if (redo_buf_put(&b, tsb, 8) != 0) { free(b.p); return TSDB_ERR_NOMEM; }

        for (int ci = 0; ci < s->ncols; ci++) {
            if (ci == ts_ci) continue;
            tsdb_type_t type = s->cols[ci].type;
            const void *col = tsdb_memtable_col(t->memtable, ci);
            if (!col) { free(b.p); return TSDB_ERR_INTERNAL; }
            if (type == TSDB_TYPE_SYMBOL) {
                uint32_t code = ((const uint32_t *)col)[r];
                const char *str = s->cols[ci].symtab
                                    ? tsdb_symtab_str(s->cols[ci].symtab, code) : NULL;
                if (!str) str = "";
                uint32_t len = (uint32_t)strlen(str);
                uint8_t lb[4]; redo_put_u32le(lb, len);
                if (redo_buf_put(&b, lb, 4) != 0 ||
                    redo_buf_put(&b, str, len) != 0) { free(b.p); return TSDB_ERR_NOMEM; }
            } else {
                /* All fixed types are 8 bytes in memory (i64 / double bits). */
                uint64_t bits = ((const uint64_t *)col)[r];
                uint8_t vb[8]; redo_put_u64le(vb, bits);
                if (redo_buf_put(&b, vb, 8) != 0) { free(b.p); return TSDB_ERR_NOMEM; }
            }
        }
    }
    *out = b.p; *out_n = b.n;
    return TSDB_OK;
}

/*
 * Write a redo record for the memtable rows committed since the last record
 * (rows [t->mem_logged, nrows)) and fsync the WAL.  Caller MUST hold
 * t->compact_mtx.  No-op (TSDB_OK) when there are no new rows or no WAL.
 * On success advances t->commit_seq and t->mem_logged.
 */
static int redo_log_locked(tsdb_table_internal_t *t) {
    if (!t->wal) return TSDB_OK;
    /* Frozen log (see wal_incomplete): a record appended here would sit ABOVE
     * the record that aborts every replay, so it could never be read back and
     * — with the flush frozen too — never reach a partition either.  Acking it
     * would be a silent loss; refuse the commit instead. */
    if (t->wal_incomplete) return TSDB_ERR_BUSY;
    size_t nrows = tsdb_memtable_rows(t->memtable);
    if (nrows <= t->mem_logged) return TSDB_OK;

    uint64_t seq = t->commit_seq + 1;
    uint8_t *payload = NULL; size_t plen = 0;
    int rc = redo_serialize(t, seq, t->mem_logged, nrows, &payload, &plen);
    if (rc != TSDB_OK) return rc;

    /* Append + fsync as one unit: commit_seq only advances on success, so a
     * record left live by a half-failed write would have its seq reused by the
     * next commit and replay would apply both.  _durable rolls the record back
     * on either failure. */
    rc = tsdb_wal_append_durable(t->wal, payload, plen);
    free(payload);
    if (rc != TSDB_OK) return rc;

    t->commit_seq = seq;
    t->mem_logged = nrows;
    return TSDB_OK;
}

/* Per-partition durable checkpoint.  On reopen a WAL record is skipped only
 * when its seq is covered by EVERY partition its rows land in, so a completed
 * sibling partition of a multi-partition flush can't mask a torn one (which
 * would drop that partition's acked rows). */
struct redo_part_cp { char name[16]; uint64_t seq; };

/* ts -> partition dir name ("YYYYMMDD" / "YYYYMMDDHH").  Local mirror of
 * part.c's ts_to_part_str, kept here to avoid widening the storage header. */
static void redo_ts_to_part_str(int64_t ts_ns, tsdb_partition_unit_t unit,
                                char *buf, size_t cap) {
    time_t secs = (time_t)(ts_ns / 1000000000LL);
    struct tm tm;
    gmtime_r(&secs, &tm);
    if (unit == TSDB_PARTITION_HOUR)
        snprintf(buf, cap, "%04d%02d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour);
    else
        snprintf(buf, cap, "%04d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

/* Replay-apply context: holds the open table, the global checkpoint (for
 * commit_seq continuation), and the per-partition checkpoint map. */
struct redo_replay_ctx {
    tsdb_table_internal_t *t;
    uint64_t               cutoff;        /* max checkpoint over partitions */
    uint64_t               max_seq;       /* highest seq seen across all records */
    uint64_t               last_complete; /* seq of the last FULLY-applied record
                                           * (a clean checkpoint boundary) */
    int                    applied;       /* 1 if any record was applied */
    const struct redo_part_cp *parts;     /* per-partition durable checkpoints */
    int                    nparts;
    /* Latched by the redo_replay_cb wrapper on the FIRST callback abort, so a
     * decode failure is distinguishable from tsdb_wal_replay's own torn-tail
     * TSDB_ERR_CORRUPT (which travels through the same return channel but means
     * "the bytes past here are not records at all" — they carry no acked seq). */
    int                    abort_rc;
    uint64_t               abort_seq;     /* seq of the record that aborted */
};

/* Durable checkpoint of the partition a row with timestamp `ts` lands in; 0 if
 * that partition has no on-disk checkpoint yet (its rows are not durable and
 * MUST be replayed). */
static uint64_t redo_part_checkpoint(const struct redo_replay_ctx *ctx,
                                     int64_t ts) {
    char name[16];
    redo_ts_to_part_str(ts, ctx->t->schema->partition_unit, name, sizeof(name));
    for (int i = 0; i < ctx->nparts; i++)
        if (strcmp(ctx->parts[i].name, name) == 0) return ctx->parts[i].seq;
    return 0;
}

/*
 * Durable checkpoint covering a whole redo record: the MIN over its rows of the
 * checkpoint of the partition each row lands in (a partition with no checkpoint
 * = 0 = not durable).  A scalar max-over-partitions would let a completed
 * sibling partition of the same flush mask a torn one and drop its acked rows.
 *
 * Sets *ok = 0 when the record cannot be walked under the CURRENT schema — the
 * length checks below are exactly the ones the apply loop makes, plus the
 * requirement that the walk land exactly on `n`, so *ok answers precisely "will
 * the apply loop be able to decode every row of this record?".  The usual cause
 * is benign: an ALTER TABLE ADD COLUMN under an untruncated log leaves records
 * serialised with fewer columns than the schema now has.
 *
 * When *ok is 0 the per-row partition lookup is meaningless past row 0, so the
 * caller must fall back — see redo_rec_cutoff_fallback.
 */
static uint64_t redo_rec_cutoff(const struct redo_replay_ctx *ctx,
                                const uint8_t *p, size_t n, uint32_t nrows,
                                int *ok)
{
    const tsdb_schema_t *s = ctx->t->schema;
    int ts_ci = s->ts_col_idx;
    uint64_t rec_cutoff = UINT64_MAX;
    size_t so = 12;
    *ok = 1;

    for (uint32_t r = 0; r < nrows; r++) {
        if (so + 8 > n) { *ok = 0; return 0; }
        int64_t rts = (int64_t)redo_get_u64le(p + so); so += 8;
        uint64_t cp = redo_part_checkpoint(ctx, rts);
        if (cp < rec_cutoff) rec_cutoff = cp;
        /* No `&& cp` short-circuit here: `so` must advance for EVERY column of
         * EVERY row or the walk desynchronises and later rows resolve garbage
         * timestamps to arbitrary partitions. */
        for (int ci = 0; ci < s->ncols; ci++) {
            if (ci == ts_ci) continue;
            if (s->cols[ci].type == TSDB_TYPE_SYMBOL) {
                if (so + 4 > n) { *ok = 0; return 0; }
                uint32_t sl = redo_get_u32le(p + so); so += 4;
                if (so + sl > n) { *ok = 0; return 0; }
                so += sl;
            } else {
                if (so + 8 > n) { *ok = 0; return 0; }
                so += 8;
            }
        }
    }
    /* A record serialised under a NARROWER schema leaves bytes over; one whose
     * declared nrows outruns its payload was caught above.  Either way the
     * layout does not match, so the per-row lookups cannot be trusted. */
    if (so != n) { *ok = 0; return 0; }
    if (rec_cutoff == UINT64_MAX) rec_cutoff = 0;   /* nrows == 0 */
    return rec_cutoff;
}

/*
 * Durability estimate for a record redo_rec_cutoff could not walk.  Row 0's
 * timestamp sits at a fixed offset (12) and is therefore readable WITHOUT
 * trusting the column layout, so its partition's checkpoint is the one honest
 * datum such a record still offers; that is exactly the value the pre-ALTER
 * code computed for the single-row records ALTER leaves behind.  With no
 * readable timestamp the record counts as not durable (0).
 *
 * Deliberately NOT the global max checkpoint: that would declare a record
 * durable on the strength of a SIBLING partition's flush, which is the very
 * masking the per-partition map exists to prevent.
 */
static uint64_t redo_rec_cutoff_fallback(const struct redo_replay_ctx *ctx,
                                         const uint8_t *p, size_t n,
                                         uint32_t nrows)
{
    if (nrows == 0 || n < 20) return 0;
    return redo_part_checkpoint(ctx, (int64_t)redo_get_u64le(p + 12));
}

/* Decode one redo record and apply rows whose record seq > cutoff into the
 * memtable.  Returns TSDB_OK or an error; see redo_replay_cb for the wrapper
 * that latches an abort. */
static int redo_replay_apply(const void *rec, size_t n,
                             struct redo_replay_ctx *ctx) {
    const uint8_t *p = (const uint8_t *)rec;
    if (n < 12) return TSDB_OK;   /* legacy 4-byte WTMC marker or junk — skip */

    uint64_t seq   = redo_get_u64le(p + 0);
    uint32_t nrows = redo_get_u32le(p + 8);
    if (seq > ctx->max_seq) ctx->max_seq = seq;

    tsdb_table_internal_t *t = ctx->t;
    tsdb_schema_t *s = t->schema;
    int ts_ci = s->ts_col_idx;

    /* Per-partition dedup guard: skip this record only if seq is already
     * durable in EVERY partition its rows land in.  Over-applying an
     * already-durable sibling is harmless: the read model enumerates via ts and
     * pairs non-ts blocks by (ts_min,count), so a re-flushed duplicate block is
     * never double-counted. */
    int cut_ok = 1;
    uint64_t rec_cutoff = redo_rec_cutoff(ctx, p, n, nrows, &cut_ok);
    if (!cut_ok) rec_cutoff = redo_rec_cutoff_fallback(ctx, p, n, nrows);
    if (seq <= rec_cutoff) return TSDB_OK;

    /* Un-walkable AND not covered by a checkpoint: the apply loop below would
     * decode a PREFIX of the rows and then fail, leaving a partial record
     * applied — which the next open would replay again, duplicating it.  A redo
     * record is atomic; refuse the whole thing and let the caller decide what
     * the un-read remainder of the log is worth. */
    if (!cut_ok) return TSDB_ERR_CORRUPT;

    size_t off = 12;

    /* Flush at this CLEAN record boundary if the whole record won't fit the
     * memtable, so we never split one redo record across a partition (which
     * would risk dup/loss of the split record on a second crash).  One record
     * always fits a fresh memtable: the original-write path flushes at the
     * block-points budget, so no single commit's redo spans more rows than
     * that.  Stamp the flush with last_complete — the seq of the last record
     * fully applied — so the partition checkpoint never claims this still-
     * unapplied record.  (In practice each flush truncates the WAL, so the
     * replayed tail fits and this never fires; it only matters if a prior
     * truncate was skipped, which correctness must not depend on.) */
    int bp_cap = (s->block_points > 0 && s->block_points <= TSDB_BLOCK_POINTS)
                    ? s->block_points : TSDB_BLOCK_POINTS;
    if (tsdb_memtable_rows(t->memtable) + nrows > (size_t)bp_cap &&
        tsdb_memtable_rows(t->memtable) > 0) {
        t->commit_seq = (ctx->last_complete > t->commit_seq)
                            ? ctx->last_complete : t->commit_seq;
        /* flush_and_clear_LOCKED: redo_recover_table holds compact_mtx, so the
         * re-locking _ex variant would deadlock. */
        int frc = flush_and_clear_locked(t, /*skip_replicate=*/1);
        if (frc != TSDB_OK) return frc;
        t->mem_logged = 0;
    }

    for (uint32_t r = 0; r < nrows; r++) {
        if (off + 8 > n) return TSDB_ERR_CORRUPT;
        int64_t ts = (int64_t)redo_get_u64le(p + off); off += 8;

        int rc = tsdb_memtable_row_begin(t->memtable);
        if (rc != TSDB_OK) return rc;

        rc = tsdb_memtable_row_ts(t->memtable, ts);
        if (rc != TSDB_OK) { tsdb_memtable_row_abort(t->memtable); return rc; }

        for (int ci = 0; ci < s->ncols; ci++) {
            if (ci == ts_ci) continue;
            tsdb_type_t type = s->cols[ci].type;
            if (type == TSDB_TYPE_SYMBOL) {
                if (off + 4 > n) { tsdb_memtable_row_abort(t->memtable); return TSDB_ERR_CORRUPT; }
                uint32_t len = redo_get_u32le(p + off); off += 4;
                if (off + len > n) { tsdb_memtable_row_abort(t->memtable); return TSDB_ERR_CORRUPT; }
                char stackbuf[256];
                char *str = (len < sizeof(stackbuf)) ? stackbuf : malloc((size_t)len + 1);
                if (!str) { tsdb_memtable_row_abort(t->memtable); return TSDB_ERR_NOMEM; }
                memcpy(str, p + off, len); str[len] = '\0'; off += len;
                rc = tsdb_memtable_row_sym(t->memtable, ci, str);
                if (str != stackbuf) free(str);
                if (rc != TSDB_OK) { tsdb_memtable_row_abort(t->memtable); return rc; }
            } else {
                if (off + 8 > n) { tsdb_memtable_row_abort(t->memtable); return TSDB_ERR_CORRUPT; }
                uint64_t bits = redo_get_u64le(p + off); off += 8;
                if (type == TSDB_TYPE_FLOAT64 || type == TSDB_TYPE_FLOAT32) {
                    double d; memcpy(&d, &bits, 8);
                    rc = tsdb_memtable_row_f64(t->memtable, ci, d);
                } else {
                    rc = tsdb_memtable_row_i64(t->memtable, ci, (int64_t)bits);
                }
                if (rc != TSDB_OK) { tsdb_memtable_row_abort(t->memtable); return rc; }
            }
        }
        rc = tsdb_memtable_row_end(t->memtable);
        if (rc != TSDB_OK) { tsdb_memtable_row_abort(t->memtable); return rc; }
        ctx->applied = 1;
    }
    ctx->last_complete = seq;   /* this record is now fully applied */
    return TSDB_OK;
}

/* tsdb_wal_replay callback.  A wrapper rather than per-return-site bookkeeping
 * so no abort path inside redo_replay_apply can be missed. */
static int redo_replay_cb(const void *rec, size_t n, void *vctx) {
    struct redo_replay_ctx *ctx = (struct redo_replay_ctx *)vctx;
    int rc = redo_replay_apply(rec, n, ctx);
    if (rc != TSDB_OK && ctx->abort_rc == TSDB_OK) {
        ctx->abort_rc  = rc;
        ctx->abort_seq = (n >= 12) ? redo_get_u64le((const uint8_t *)rec) : 0;
    }
    return rc;
}

/*
 * Second, header-only pass over the SAME log, run only after a callback abort.
 * tsdb_wal_replay stops at the aborting record, so everything past it is acked
 * but UN-READ; this pass answers the only two questions that matter about it:
 *
 *   max_seq      — the highest seq anywhere in the log, so the reopened table's
 *                  commit_seq can continue above it and never hand a NEW commit
 *                  a seq that an un-read record already owns.
 *   n_unapplied  — how many un-read records are NOT already covered by a
 *                  partition checkpoint.  Zero means the tail is redundant and
 *                  the table can open, flush and truncate exactly as usual.
 *
 * Durability is judged with the same rule the apply path uses, so the two can
 * never disagree.  This callback NEVER aborts: a record it cannot walk is
 * scored, not fatal.
 */
struct redo_tail_scan {
    const struct redo_replay_ctx *rctx;
    uint64_t from_seq;         /* first UN-READ seq (abort_seq + 1) */
    uint64_t max_seq;
    uint64_t n_unapplied;
    uint64_t first_unapplied;
};

static int redo_tail_scan_cb(const void *rec, size_t n, void *vctx) {
    struct redo_tail_scan *s = (struct redo_tail_scan *)vctx;
    if (n < 12) return TSDB_OK;                  /* legacy WTMC marker */
    const uint8_t *p = (const uint8_t *)rec;
    uint64_t seq   = redo_get_u64le(p + 0);
    uint32_t nrows = redo_get_u32le(p + 8);
    if (seq > s->max_seq) s->max_seq = seq;
    if (seq < s->from_seq) return TSDB_OK;        /* the replay already read it */

    int ok = 1;
    uint64_t cp = redo_rec_cutoff(s->rctx, p, n, nrows, &ok);
    if (!ok) cp = redo_rec_cutoff_fallback(s->rctx, p, n, nrows);
    if (seq > cp) {
        s->n_unapplied++;
        if (s->first_unapplied == 0) s->first_unapplied = seq;
    }
    return TSDB_OK;
}

/*
 * Recover a freshly-opened table's unflushed WAL tail into its memtable.
 * Computes C = max durable commit-seq checkpoint across this table's
 * partitions, replays the per-table WAL, and applies only records with
 * seq > C (records <= C are already durable in partitions).  Sets
 * commit_seq / mem_logged so subsequent commits continue monotonically.
 *
 * Called once, under db->lock, right after the table's wal is opened and
 * before the table is exposed.  Safe to call unconditionally: a WAL with
 * only legacy markers (or none) replays to a no-op.
 */
static void redo_recover_table(tsdb_db_t *db, tsdb_table_internal_t *t) {
    if (!t->wal || !t->schema) return;

    /* Build the per-partition checkpoint map + the global max (the latter for
     * commit_seq continuation).  Replay uses the per-partition map so a
     * completed sibling partition of a torn multi-partition flush can't mask
     * the torn one — see redo_replay_cb. */
    uint64_t cutoff = 0;
    struct redo_part_cp *parts = NULL;
    int nparts = 0, cap = 0;
    char tbl_dir[4096];
    table_dir_db(db, t->name, tbl_dir, sizeof(tbl_dir));
    DIR *d = opendir(tbl_dir);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d)) != NULL) {
            if (e->d_name[0] == '.') continue;   /* skip ., .., .trash */
            char part_dir[5000];
            snprintf(part_dir, sizeof(part_dir), "%s/%s", tbl_dir, e->d_name);
            struct stat st;
            if (stat(part_dir, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
            uint64_t ps = tsdb_part_max_seq(t->schema, part_dir);
            if (ps > cutoff) cutoff = ps;
            if (nparts == cap) {
                int ncap = cap ? cap * 2 : 16;
                struct redo_part_cp *np = realloc(parts, (size_t)ncap * sizeof(*np));
                if (!np) continue;   /* OOM: this dir stays out of the map, so a
                                      * row there looks up 0 = replayed — safe */
                parts = np; cap = ncap;
            }
            snprintf(parts[nparts].name, sizeof(parts[nparts].name), "%s", e->d_name);
            parts[nparts].seq = ps;
            nparts++;
        }
        closedir(d);
    }

    /* Designated: the struct carries abort state now, and a positional
     * initialiser silently mis-assigns the moment it grows again. */
    struct redo_replay_ctx ctx = { .t = t, .cutoff = cutoff,
                                   .last_complete = cutoff,
                                   .parts = parts, .nparts = nparts };
    pthread_mutex_lock(&t->compact_mtx);
    t->replaying = 1;
    int rc = tsdb_wal_replay(db->data_dir, t->name, redo_replay_cb, &ctx);
    /* commit_seq must continue ABOVE every seq that ever existed (durable in
     * partitions OR replayed) so future commits never reuse a seq. */
    uint64_t hi = (ctx.max_seq > cutoff) ? ctx.max_seq : cutoff;

    if (ctx.abort_rc != TSDB_OK) {
        /* The replay stopped on a record it could not apply, so every record
         * after it is acked but UN-READ.  Find out whether that matters before
         * anything is allowed to destroy it. */
        struct redo_tail_scan sc = { .rctx = &ctx, .from_seq = ctx.abort_seq + 1 };
        (void)tsdb_wal_replay(db->data_dir, t->name, redo_tail_scan_cb, &sc);
        if (sc.max_seq > hi) hi = sc.max_seq;
        if (sc.n_unapplied == 0) {
            /* Every un-read record is already durable in the partitions its
             * rows land in.  Opening normally is not just safe, it is required:
             * freezing a table whose data is entirely on disk would turn a
             * recoverable state into an outage. */
            fprintf(stderr,
                    "[db] table '%s': WAL replay aborted at seq %llu (rc=%d); "
                    "every un-read record is already durable - opening normally\n",
                    t->name, (unsigned long long)ctx.abort_seq, ctx.abort_rc);
            tsdb_metric_inc("qengine_wal_replay_aborted_benign_total");
        } else {
            t->wal_incomplete = 1;
            fprintf(stderr,
                    "[db] table '%s': WAL replay ABORTED at seq %llu (rc=%d); "
                    "%llu un-read record(s) from seq %llu up to seq %llu are NOT "
                    "durable in any partition - table stays READABLE but its log "
                    "is frozen (no flush, no checkpoint advance, no truncate, no "
                    "new commits) until %s/wal/%s.log is repaired or removed\n",
                    t->name, (unsigned long long)ctx.abort_seq, ctx.abort_rc,
                    (unsigned long long)sc.n_unapplied,
                    (unsigned long long)sc.first_unapplied,
                    (unsigned long long)sc.max_seq, db->data_dir, t->name);
            tsdb_metric_inc("qengine_wal_replay_incomplete_total");
        }
    } else if (rc != TSDB_OK && rc != TSDB_ERR_CORRUPT) {
        /* tsdb_wal_replay's own TSDB_ERR_CORRUPT means the bytes past the last
         * record are not CRC-valid records (tsdb_wal_open already repaired a
         * torn tail), so they carry no acked seq — expected and safe.  Anything
         * else is logged but non-fatal so the table still opens. */
        fprintf(stderr, "[db] WAL replay for table '%s' returned rc=%d\n",
                t->name, rc);
    }

    t->replaying = 0;
    if (hi > t->commit_seq) t->commit_seq = hi;
    t->mem_logged = tsdb_memtable_rows(t->memtable);
    pthread_mutex_unlock(&t->compact_mtx);
    if (ctx.applied)
        tsdb_metric_inc("qengine_wal_recovered_records_total");
    free(parts);
}

/*
 * The ONE place a flush is allowed to shorten the redo log.  Both callers below
 * reach it after the rows they logged are durable elsewhere, but two states
 * make the truncate destructive rather than merely redundant:
 *
 *   replaying      — tsdb_wal_replay is iterating this very file through its own
 *                    FILE*.  ftruncate(fd,0) makes the reader's next fread
 *                    return 0, which it reads as a clean EOF: the rest of the
 *                    log is destroyed and the replay still returns TSDB_OK.
 *   wal_incomplete — the log holds acked records that no partition covers.
 *
 * Skipping is always safe: the seq > checkpoint skip dedups a re-read prefix,
 * so the truncate only bounds WAL size (see the call sites).
 */
static void wal_truncate_if_safe(tsdb_table_internal_t *t) {
    if (t->wal && !t->replaying && !t->wal_incomplete)
        (void)tsdb_wal_truncate(t->wal);
}

/*
 * Internal variant: caller MUST hold t->compact_mtx.
 * Used by alter_table_add_column and by the public flush_and_clear_ex
 * wrapper.  Splitting the lock acquisition out of the body avoids a
 * deadlock when the caller already needs the lock for related work
 * (schema mutation, etc).
 */
static int flush_and_clear_locked(tsdb_table_internal_t *t, int skip_replicate) {
    if (tsdb_memtable_rows(t->memtable) == 0) return TSDB_OK;
    /* Frozen log: publishing a partition here would stamp it with commit_seq,
     * which covers the un-read records too — the checkpoint would then declare
     * acked rows durable that were never applied, and the next open would skip
     * them for good.  Stamping a LOWER seq is no better: it leaves the flushed
     * rows above the checkpoint, so the next open replays them on top of the
     * partition this flush just wrote.  A single scalar checkpoint cannot
     * express "these rows are durable but those records are not", so the flush
     * has to wait.  Nothing is lost: every row in the memtable came from the
     * log, which wal_truncate_if_safe is keeping intact.
     *
     * This gate also covers tsdb_close, which flushes through this path — and
     * therefore truncates — so without it the first clean shutdown after a
     * frozen open would delete the tail the freeze exists to protect. */
    if (t->wal_incomplete) return TSDB_ERR_BUSY;

    /* Row-level cluster replication: fires BEFORE flush so data still in
     * memtable.  Skipped when raw-block mode is active (on_raw_block != NULL)
     * to avoid double-replication.
     *
     * Phase β.2: when shard mode is on and self is a non-owner, the hook
     * returns TSDB_SKIP_LOCAL after the owners ACK.  We then drop the
     * local copy entirely (clear memtable, truncate WAL) rather than
     * persisting a redundant on-disk shard. */
    int hook_rc = TSDB_OK;
    if (!skip_replicate && t->db && t->db->on_replicate &&
        !t->db->on_raw_block) {
        hook_rc = t->db->on_replicate(t->db->hook_ud, t->db, t->name,
                                       t->schema, t->memtable);
        /* Errors other than the skip signal are logged by the hook but
         * do not abort the local write — falls through to local persist
         * so the row isn't lost on a flaky owner. */
    }
    if (hook_rc == TSDB_SKIP_LOCAL) {
        tsdb_memtable_clear(t->memtable);
        wal_truncate_if_safe(t);
        t->mem_logged = 0;        /* dropped rows: their redo (if any) is gone */
        __atomic_fetch_add(&t->flush_gen, 1, __ATOMIC_RELAXED); /* memtable drained */
        t->mem_has_local = 0;     /* memtable drained: reset content provenance */
        t->mem_has_received = 0;
        tsdb_metric_inc("qengine_shard_local_skipped_total");
        return TSDB_OK;
    }

    /* WAL redo checkpoint: the rows about to be flushed are covered by redo
     * records up to commit_seq, so stamp that seq into every partition idx
     * this flush publishes.  On reopen, recovery skips WAL records with
     * seq <= this checkpoint (already durable here), closing the dup window
     * even if a crash lands between the partition publish and WAL truncate.
     * In the default (non-wal_only) mode commit_seq stays 0, so hwm == 0 and
     * tsdb_part_flush_ex2 writes byte-identical v3 idx headers. */
    uint64_t hwm = t->commit_seq;

    /* Persist any SYMBOL column's dictionary whose codes this flush is about to
     * make durable, BEFORE publishing the blocks that reference them.  Without
     * this a crash leaves durable coded blocks with no matching on-disk dict,
     * and the reopen rebuilds a DIFFERENT one from the WAL tail — silently
     * returning WRONG tag values.  tsdb_symtab_save is a no-op when the dict
     * hasn't grown since the last flush. */
    if (t->schema) {
        for (int ci = 0; ci < t->schema->ncols; ci++) {
            if (t->schema->cols[ci].type == TSDB_TYPE_SYMBOL &&
                t->schema->cols[ci].symtab) {
                char sp[4096];
                snprintf(sp, sizeof(sp), "%s/%s.sym", t->schema->dir,
                         t->schema->cols[ci].name);
                (void)tsdb_symtab_save(t->schema->cols[ci].symtab, sp);
            }
        }
    }

    /* Raw-block replication is triggered from inside tsdb_part_flush_ex
     * after each block encode — pass db + table_name so the hook has context.
     * For replica-received writes (skip_replicate=1) pass NULL to suppress. */
    int rc = tsdb_part_flush_ex2(t->schema, t->memtable,
                                  skip_replicate ? NULL : t->db,
                                  t->name, hwm);
    /* Data-dir failover: an I/O error persisting this table's partitions
     * degrades the striped dir it lives in so NEW tables route elsewhere.
     * The rows below stay in the memtable (only TSDB_OK clears it), so
     * nothing is lost — retries land once the dir re-probes healthy. */
    if (rc == TSDB_ERR_IO && t->db && t->schema && t->schema->dir)
        db_mark_dir_degraded(t->db, t->schema->dir);
    if (rc == TSDB_OK) {
        tsdb_metric_inc("qengine_flushes_total");
        if (t->db) __atomic_fetch_add(&t->db->flush_seq, 1, __ATOMIC_RELAXED);
        tsdb_memtable_clear(t->memtable);
        t->mem_logged = 0;        /* memtable drained: nothing logged yet */
        __atomic_fetch_add(&t->flush_gen, 1, __ATOMIC_RELAXED); /* memtable drained */
        t->mem_has_local = 0;     /* memtable drained: reset content provenance */
        t->mem_has_received = 0;
        /* Truncate WAL after the partition (with its checkpoint) is durably
         * published.  Correctness does NOT depend on this — the seq>checkpoint
         * skip dedups even if truncate is skipped — it only bounds WAL size. */
        wal_truncate_if_safe(t);
    }
    return rc;
}

uint64_t tsdb_db_flush_seq(tsdb_db_t *db) {
    if (!db) return 0;
    return __atomic_load_n(&db->flush_seq, __ATOMIC_RELAXED);
}

uint64_t tsdb_db_table_set_gen(tsdb_db_t *db) {
    if (!db) return 0;
    return __atomic_load_n(&db->table_set_gen, __ATOMIC_RELAXED);
}

tsdb_iopolicy_t tsdb_db_iopolicy(tsdb_db_t *db) {
    if (!db) return TSDB_IOPOLICY_SSD;
    return db->iopolicy;
}

int tsdb_db_wal_only_commit(tsdb_db_t *db) {
    if (!db) return 0;
    return db->wal_only_commit;
}

int tsdb_db_raise_commit_seq(tsdb_db_t *db, const char *table, uint64_t seq) {
    if (!db || !table) return TSDB_ERR_INVAL;
    tsdb_table_internal_t *t = tsdb_db_find_table(db, table);
    if (!t) return TSDB_ERR_NOTFOUND;
    /* Same lock redo_recover_table takes to publish the recovered value. */
    pthread_mutex_lock(&t->compact_mtx);
    if (seq > t->commit_seq) t->commit_seq = seq;
    pthread_mutex_unlock(&t->compact_mtx);
    return TSDB_OK;
}

/* Public wrapper: serialises concurrent batch_commit calls on the same
 * table by wrapping the full flush body (replicate + part_flush + clear)
 * in compact_mtx.  Without this, two callers both pass the rows>0 check,
 * both fire on_replicate (DOUBLE replication), and both call part_flush_ex
 * (DOUBLE on-disk write).  The server-side per-table write lock used to
 * mask this; making the memtable layer self-serialising means the
 * server-side lock is redundant on the WRITE_BATCH wire path. */
static int flush_and_clear_ex(tsdb_table_internal_t *t, int skip_replicate) {
    pthread_mutex_lock(&t->compact_mtx);
    int rc = flush_and_clear_locked(t, skip_replicate);
    pthread_mutex_unlock(&t->compact_mtx);
    return rc;
}

/* Idle-flush maintenance thread.  The size path only flushes a memtable when it
 * fills a block (block_points rows), so the last <block_points rows of a table
 * that stops being written sit unflushed in the INGESTING node's memtable —
 * WAL-durable locally but NOT replicated (on_replicate fires on flush) and NOT
 * visible from other nodes (a non-owner ingester keeps nothing on disk).  This
 * thread drains that tail so it reaches the shard owners and becomes
 * cluster-visible + durable.
 *
 * It flushes a table only when ALL of:
 *   - the memtable is non-empty and its row count is UNCHANGED since the last
 *     tick (writes have paused — a still-growing hot table flushes via the size
 *     path and is left alone, so steady-state write throughput is unchanged), and
 *   - mem_has_local is set: the memtable holds locally-ingested, not-yet-
 *     replicated rows.  A memtable of purely RECEIVED (replicated) rows is NEVER
 *     flushed-with-replicate here — otherwise two replica owners would bounce
 *     the same rows back and forth every tick (the bug the first attempt hit).
 * The flush runs under the table write lock (batch_mu) exactly like the size
 * path and scan_acquire pins the table so a concurrent DROP can't free it.
 * Env: TSDB_IDLE_FLUSH=0 disables it; TSDB_IDLE_FLUSH_MS overrides the 2s tick. */
void *tsdb_idle_flush_thread(void *ud) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return NULL;
    const char *off = getenv("TSDB_IDLE_FLUSH");
    if (off && atoi(off) == 0) return NULL;
    int interval_ms = 2000;
    const char *ev = getenv("TSDB_IDLE_FLUSH_MS");
    if (ev && atoi(ev) > 0) interval_ms = atoi(ev);

    /* Names to flush this tick — heap-allocated once to avoid a 1 MB stack. */
    char (*names)[64] = malloc((size_t)TSDB_DB_MAX_TABLES * 64);
    if (!names) return NULL;

    for (;;) {
        struct timespec ts = { interval_ms / 1000,
                               (long)(interval_ms % 1000) * 1000000 };
        nanosleep(&ts, NULL);

        int nflush = 0;
        pthread_mutex_lock(&db->lock);
        for (int i = 0; i < db->ntables; i++) {
            tsdb_table_internal_t *t = db->tables[i];
            if (!t || !t->memtable) continue;
            size_t r = tsdb_memtable_rows_relaxed(t->memtable);
            if (r > 0 && r == t->idle_prev_rows && t->mem_has_local &&
                !t->mem_has_received && nflush < TSDB_DB_MAX_TABLES)
                snprintf(names[nflush++], 64, "%s", t->name);
            t->idle_prev_rows = r;
        }
        pthread_mutex_unlock(&db->lock);

        for (int i = 0; i < nflush; i++) {
            tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, names[i]);
            if (!t) continue;                       /* dropped since snapshot */
            tsdb_table_lock_write((tsdb_table_t *)t);
            /* Re-check under the write lock: mem_has_local may have cleared (a
             * size-flush drained it) or fresh rows arrived — only replicate a
             * still-local, still-non-empty tail. */
            if (t->memtable && t->mem_has_local && !t->mem_has_received &&
                tsdb_memtable_rows(t->memtable) > 0)
                (void)flush_and_clear_ex(t, 0);     /* skip_replicate=0 → replicate */
            tsdb_table_unlock_write((tsdb_table_t *)t);
            tsdb_db_scan_release(db, t);
        }
    }
    free(names);   /* not reached — loop is infinite */
    return NULL;
}

/*
 * Auto-flush if memtable is full. Called before row_begin.
 * skip_replicate mirrors the batch's local_only flag.
 */
static int maybe_flush_b(tsdb_table_internal_t *t, int skip_replicate) {
    if (tsdb_memtable_is_full(t->memtable)) {
        return flush_and_clear_ex(t, skip_replicate);
    }
    /* Aggregate-memtable backpressure: when total memtable rows across all
     * tables exceed the budget (flag maintained by the maintenance thread),
     * flush THIS table — it's the one being written, already serialised on the
     * writer's thread, so no risky cross-thread flush.  The 2048-row floor
     * avoids tiny fragmenting flushes; substantial tables relieve the pressure
     * and the next maintenance pass clears the flag. */
    if (t->db && t->db->memtable_over_budget &&
        tsdb_memtable_rows(t->memtable) >= 2048) {
        return flush_and_clear_ex(t, skip_replicate);
    }
    return TSDB_OK;
}

int tsdb_batch_row_ts(tsdb_batch_t *b, tsdb_ts_t ts) {
    if (!b) return TSDB_ERR_INVAL;
    tsdb_table_internal_t *t = b->tbl;

    if (!b->in_row) {
        /* Auto-flush if full, then begin row. */
        int rc = maybe_flush_b(t, b->local_only);
        if (rc != TSDB_OK) return rc;

        rc = tsdb_memtable_row_begin(t->memtable);
        if (rc != TSDB_OK) return rc;
        b->in_row = 1;
        /* Record memtable-content provenance for the idle-flush: locally
         * ingested (replicate the tail) vs received (never re-replicate).  Set
         * AFTER maybe_flush_b, which clears both on a size-flush. */
        if (b->local_only) t->mem_has_received = 1;
        else               t->mem_has_local    = 1;
    }
    return tsdb_memtable_row_ts(t->memtable, ts);
}

int tsdb_batch_row_i64(tsdb_batch_t *b, int col_idx, int64_t v) {
    if (!b) return TSDB_ERR_INVAL;
    return tsdb_memtable_row_i64(b->tbl->memtable, col_idx, v);
}

int tsdb_batch_row_f64(tsdb_batch_t *b, int col_idx, double v) {
    if (!b) return TSDB_ERR_INVAL;
    return tsdb_memtable_row_f64(b->tbl->memtable, col_idx, v);
}

int tsdb_batch_row_sym(tsdb_batch_t *b, int col_idx, const char *s) {
    if (!b) return TSDB_ERR_INVAL;
    return tsdb_memtable_row_sym(b->tbl->memtable, col_idx, s);
}

int tsdb_batch_row_end(tsdb_batch_t *b) {
    if (!b) return TSDB_ERR_INVAL;
    int rc = tsdb_memtable_row_end(b->tbl->memtable);
    if (rc == TSDB_OK) b->in_row = 0;
    return rc;
}

int tsdb_batch_commit(tsdb_batch_t *b) {
    if (!b) return TSDB_ERR_INVAL;
    tsdb_table_internal_t *t = b->tbl;

    /* If in a partial row, abort it — don't persist incomplete data. */
    if (b->in_row) {
        tsdb_memtable_row_abort(t->memtable);
        b->in_row = 0;
    }

    if (t->db->wal_only_commit) {
        /* Deferred-flush mode: do NOT flush the memtable here (it drains at
         * budget via maybe_flush_b).  Durability instead comes from the WAL
         * redo log: append THIS batch's freshly-committed rows and fsync
         * before acking.  On a crash the unflushed memtable is rebuilt from
         * these records (those a partition checkpoint has not superseded).
         *
         * Held under compact_mtx so the redo-append + seq/mem_logged bump is
         * atomic w.r.t. a concurrent flush (which captures commit_seq and
         * resets mem_logged).  fsync-in-lock is acceptable: deferring the
         * memtable flush is this mode's whole point, and correctness is the
         * priority over commit throughput. */
        pthread_mutex_lock(&t->compact_mtx);
        int rc = redo_log_locked(t);
        pthread_mutex_unlock(&t->compact_mtx);
        if (rc != TSDB_OK) {
            /* WAL append/fsync failed: the batch's rows stay in the memtable,
             * un-acked.  We deliberately do NOT truncate here.  A standalone
             * writer retains a refused wal_only batch and drains it on the next
             * flush/close, so dropping it would turn "commit returned an error"
             * into silent data loss (test_enospc phase 11 pins exactly this:
             * the refused batch stays in RAM and a clean close drains it once).
             *
             * The cluster's duplication-on-retry is closed on the RECEIVER
             * side instead: rpc_apply_write_batch calls tsdb_batch_discard on an
             * ERR ack, and discard rolls these rows back (see below) so the
             * fanout retry re-appends from a clean boundary rather than doubling
             * them.  Callers that do NOT discard (standalone, influx, server)
             * keep the retain-and-drain-on-close behaviour.  Same split as the
             * default flush-on-commit path, which also retains on failure and
             * rolls back only via discard. */
            return rc;   /* caller either discards (-> rollback) or retains */
        }
        free(b);
        return TSDB_OK;
    }

    /* Default mode: replicate (if cluster hook) + flush remaining rows, then
     * fsync the WAL sync-point.  flush_and_clear_ex skips the hook when
     * local_only=1 (replica-received write). */
    {
        int rc = flush_and_clear_ex(t, b->local_only);
        if (rc != TSDB_OK) return rc;
    }

    /* Sync WAL — direct fsync or via group-commit batcher. */
    if (t->wal) {
        int sync_rc;
        if (t->gc) {
            /* Group-commit path: write a commit marker then wait for batch fsync.
             * The 4-byte magic lets WAL replay skip these control records. */
            uint32_t magic = 0x434D5457u;  /* "WTMC" */
            sync_rc = tsdb_wal_append_sync(t->wal, t->gc, &magic, sizeof(magic));
        } else {
            sync_rc = tsdb_wal_sync(t->wal);
        }
        if (sync_rc != TSDB_OK) return sync_rc;
    }

    free(b);
    return TSDB_OK;
}

void tsdb_batch_discard(tsdb_batch_t *b) {
    if (!b) return;
    tsdb_table_internal_t *t = b->tbl;
    /* Clear any partial (uncommitted) row state first — this also releases the
     * memtable lock that row_begin holds across an open row. */
    if (b->in_row) {
        tsdb_memtable_row_abort(t->memtable);
        b->in_row = 0;
    }
    /* Roll back the rows this batch appended to the memtable but never made
     * durable, so the receiver's "commit-failed -> discard" else-branch (and
     * the append-failure discard) actually undo the batch instead of leaving
     * its rows for the next flush to persist and, in the cluster, for a retry
     * to duplicate.  Truncate to base_nrows — this batch's start boundary — so
     * rows a PRIOR batch left in the memtable (e.g. a default-mode failed-flush
     * batch that is intentionally retained, or earlier wal-durable rows) are
     * preserved.  If a flush straddled this batch, everything now in the
     * memtable is this batch's post-flush tail (the flush drained all prior
     * rows to a partition), so drop it all and warn that the durable prefix
     * cannot be rolled back.  Under compact_mtx, serialised against a flush. */
    pthread_mutex_lock(&t->compact_mtx);
    int straddled = (__atomic_load_n(&t->flush_gen, __ATOMIC_RELAXED)
                         != b->begin_flush_gen);
    (void)tsdb_memtable_truncate_to(t->memtable, straddled ? 0 : b->base_nrows);
    pthread_mutex_unlock(&t->compact_mtx);
    if (straddled)
        fprintf(stderr,
            "[db] table '%s': batch discarded AFTER a mid-batch flush; rows "
            "already persisted to a partition cannot be rolled back\n", t->name);
    free(b);
}

void tsdb_batch_set_local_only(tsdb_batch_t *b) {
    if (b) b->local_only = 1;
}

/* Internal memtable entry point (defined in memtable.c, deliberately
 * not in memtable.h): like tsdb_memtable_append_bulk, but SYMBOL
 * columns are passed as a raw payload pointer + byte length
 * (sym_lens[d]) with no [u32 total] header, so a chunk of a larger
 * batch can be appended without re-packing it into a temp buffer.
 * Mirror of the definition — keep the two in sync. */
int tsdb_memtable_append_bulk_raw(tsdb_memtable_t *m,
                                  const void *ts_arr,
                                  const void * const *col_arrs,
                                  const size_t *sym_lens,
                                  const int *col_types,
                                  int ncols_data,
                                  size_t n);

int tsdb_batch_append_bulk(tsdb_batch_t *b,
                            const void *ts_arr,
                            const void * const *col_arrs,
                            const int *col_types,
                            int ncols_data,
                            size_t n)
{
    if (!b) return TSDB_ERR_INVAL;
    if (b->in_row) return TSDB_ERR_INVAL;
    if (n == 0) return TSDB_OK;
    /* Treat ts_arr as raw bytes to preserve byte-level pointer
     * arithmetic (consumed rows × 8 bytes) without ever forming a
     * misaligned int64_t* — that's the cast UBSAN was flagging. */
    const uint8_t *ts_bytes = (const uint8_t *)ts_arr;

    /* Chunk by block_points so a caller can hand us > bp_cap rows and
     * we transparently flush+continue.  Mirrors what the per-row API
     * gets implicitly via maybe_flush_b on each row_begin.  Without
     * this, a single ingest over the cap (default 8192) would return
     * TSDB_ERR_FULL and the caller would discard the whole batch — the
     * exact bug that hit Influx /write at N=10000 once we routed it
     * through bulk_append. */
    int bp = (b->tbl->schema->block_points > 0 &&
              b->tbl->schema->block_points <= TSDB_BLOCK_POINTS)
                ? b->tbl->schema->block_points : TSDB_BLOCK_POINTS;

    /* Running per-SYMBOL-column read cursors.  Chunk k used to re-walk the
     * variable-length wire format from the start to skip chunks 0..k-1 —
     * O(chunks²) symbol scanning on large multi-block batches (the
     * WRITE_BATCH millions-rows path).  Each cursor starts past the [u32
     * total] header and is advanced once per chunk instead. */
    const uint8_t *sym_cursor[TSDB_MAX_COLS];
    for (int d = 0; d < ncols_data; d++)
        sym_cursor[d] = (col_types[d] == TSDB_TYPE_SYMBOL)
                            ? (const uint8_t *)col_arrs[d] + 4 : NULL;

    size_t consumed = 0;
    while (consumed < n) {
        size_t cur_rows = tsdb_memtable_rows(b->tbl->memtable);
        size_t avail = (cur_rows < (size_t)bp) ? (size_t)bp - cur_rows : 0;

        if (avail == 0) {
            int rc = flush_and_clear_ex(b->tbl, b->local_only);
            if (rc != TSDB_OK) return rc;
            avail = (size_t)bp;
        }

        size_t chunk = (n - consumed) < avail ? (n - consumed) : avail;

        /* Per-column source pointers slid forward by `consumed` rows.
         * 8-byte fixed cols just advance by 8 × consumed; SYMBOL cols
         * resume from their carried cursor. */
        const void *col_arrs_off[TSDB_MAX_COLS];
        size_t      sym_lens_off[TSDB_MAX_COLS];
        for (int d = 0; d < ncols_data; d++) {
            const uint8_t *p = (const uint8_t *)col_arrs[d];
            if (col_types[d] == TSDB_TYPE_SYMBOL) {
                const uint8_t *cur = sym_cursor[d];
                /* Walk the chunk's rows once to find its byte extent,
                 * then hand the payload slice straight to the memtable
                 * (ptr + len, no [u32 total] wrapper copy). */
                const uint8_t *chunk_start = cur;
                size_t chunk_payload = 0;
                for (size_t k = 0; k < chunk; k++) {
                    uint16_t l16;
                    memcpy(&l16, cur, 2);
                    cur += 2 + l16;
                    chunk_payload += 2 + l16;
                }
                sym_cursor[d] = cur;   /* resume point for the next chunk */
                col_arrs_off[d] = chunk_start;
                sym_lens_off[d] = chunk_payload;
            } else {
                col_arrs_off[d] = p + consumed * 8;
                sym_lens_off[d] = 0;
            }
        }

        int rc = tsdb_memtable_append_bulk_raw(b->tbl->memtable,
                                                ts_bytes + consumed * sizeof(int64_t),
                                                col_arrs_off, sym_lens_off,
                                                col_types, ncols_data, chunk);
        if (rc != TSDB_OK) return rc;
        consumed += chunk;
    }
    /* Record content provenance for the idle-flush on whatever tail remains in
     * the memtable (the columnar bulk path — used by influx /write — bypasses
     * the per-row tsdb_batch_row_ts where this is otherwise set). */
    if (tsdb_memtable_rows(b->tbl->memtable) > 0) {
        if (b->local_only) b->tbl->mem_has_received = 1;
        else               b->tbl->mem_has_local    = 1;
    }
    return TSDB_OK;
}

/* ---- Cluster hook registration ----------------------------------------- */

void tsdb_db_set_hooks(tsdb_db_t *db,
                        tsdb_on_replicate_fn on_replicate,
                        tsdb_on_create_fn on_create,
                        void *userdata)
{
    if (!db) return;
    pthread_mutex_lock(&db->lock);
    db->on_replicate = on_replicate;
    db->on_create    = on_create;
    db->hook_ud      = userdata;
    /* This DB is now a cluster node.  If wal_only_commit was enabled only by
     * the iopolicy auto-tune (not an explicit env), revert to flush-on-commit
     * so replication fires per commit instead of being deferred to the flush
     * boundary — deferred replication on a cluster risks replica lag/skew that
     * the auto-tune should not impose silently.  An explicit env is untouched. */
    if (db->wal_only_auto && (on_replicate || on_create)) {
        db->wal_only_commit = 0;
        db->wal_only_auto   = 0;
        fprintf(stderr, "[db] cluster node: reverted auto wal_only_commit to "
                        "flush-on-commit (set TSDB_WAL_ONLY_COMMIT=1 to keep it)\n");
    }
    pthread_mutex_unlock(&db->lock);
}

void tsdb_db_set_raw_block_hook(tsdb_db_t *db,
                                 tsdb_on_raw_block_fn on_raw_block,
                                 void *raw_ud)
{
    if (!db) return;
    pthread_mutex_lock(&db->lock);
    db->on_raw_block = on_raw_block;
    db->raw_block_ud = raw_ud;
    pthread_mutex_unlock(&db->lock);
}

/* Called from part.c to retrieve the raw-block hook.
 * We pass the function pointer via void** to avoid a typedef conflict
 * between db.h's tsdb_on_raw_block_fn and part.c's local part_raw_block_fn_t.
 * The cast is valid since the underlying function signatures are identical. */
void tsdb_db_get_raw_block_hook(tsdb_db_t *db,
                                 void **out_fn,
                                 void **out_ud)
{
    if (!db || !out_fn || !out_ud) return;
    /* Store function pointer as void* via memcpy to satisfy strict-aliasing. */
    tsdb_on_raw_block_fn fn = db->on_raw_block;
    __builtin_memcpy(out_fn, &fn, sizeof(void *));
    *out_ud = db->raw_block_ud;
}

/* ---- Internal accessors for query module ------------------------------- */

const char *tsdb_db_data_dir(tsdb_db_t *db) { return db ? db->data_dir : NULL; }

/* Number of striped data dirs (>=1; 1 means single-dir setup).
 * Callers iterate via tsdb_db_data_dir_at(). */
int tsdb_db_data_dir_count(tsdb_db_t *db) {
    if (!db) return 0;
    return db->n_data_dirs > 0 ? db->n_data_dirs : 1;
}

/* Per-index dir accessor.  i in [0, tsdb_db_data_dir_count()).
 * Returns NULL on out-of-range so callers can simply break the loop. */
const char *tsdb_db_data_dir_at(tsdb_db_t *db, int i) {
    if (!db || i < 0) return NULL;
    if (db->n_data_dirs <= 1) return i == 0 ? db->data_dir : NULL;
    if (i >= db->n_data_dirs) return NULL;
    return db->data_dirs[i];
}

tsdb_table_internal_t *tsdb_db_find_table(tsdb_db_t *db, const char *name) {
    if (!db || !name) return NULL;
    pthread_mutex_lock(&db->lock);
    tsdb_table_internal_t *t = db_find_table(db, name);
    pthread_mutex_unlock(&db->lock);
    return t;
}

/* Compactor lifetime guard.  The background compactor holds the returned raw
 * table pointer (and uses its schema + compact_mtx lock-free) for an entire
 * compaction pass.  Marking the table `compacting` under db->lock makes a
 * concurrent tsdb_drop_table wait until the pass ends before freeing it —
 * closing a use-after-free / double-free seen under drop+compaction stress.
 * Returns NULL (no guard set) if the table is gone. */
tsdb_table_internal_t *tsdb_db_compact_acquire(tsdb_db_t *db, const char *name) {
    if (!db || !name) return NULL;
    pthread_mutex_lock(&db->lock);
    tsdb_table_internal_t *t = db_find_table(db, name);
    if (t) t->compacting = 1;
    pthread_mutex_unlock(&db->lock);
    return t;
}

void tsdb_db_compact_release(tsdb_db_t *db, tsdb_table_internal_t *t) {
    if (!db || !t) return;
    pthread_mutex_lock(&db->lock);
    t->compacting = 0;   /* t is still alive: drop waits while compacting==1 */
    pthread_mutex_unlock(&db->lock);
}

/* Query/scan lifetime guard.  A SELECT (and its parallel scan workers) holds a
 * raw pointer to the table's schema for the duration of execution; marking the
 * table in-use under db->lock makes a concurrent tsdb_drop_table wait until the
 * query finishes before freeing the schema — closing a use-after-free seen in
 * agg_write / tsdb_part_col_blocks under concurrent query+drop.  Find+increment
 * under one lock so it can't race the drop's check+free.  NULL if table gone. */
tsdb_table_internal_t *tsdb_db_scan_acquire(tsdb_db_t *db, const char *name) {
    if (!db || !name) return NULL;
    pthread_mutex_lock(&db->lock);
    tsdb_table_internal_t *t = db_find_table(db, name);
    /* Wait out the `altering` drain barrier: an in-flight ALTER reallocs
     * schema->cols (and we are about to hand the caller a raw pointer it reads
     * lock-free), and a pending DROP raises the same barrier so it can drain
     * scan_refs to 0 without new acquirers starving it.  Re-resolve after each
     * wait: under a DROP the table will vanish and db_find_table returns NULL,
     * so the caller correctly sees the table as gone.  DDL is serialised, so
     * ALTER and DROP never raise the barrier at once. */
    while (t && t->altering) {
        pthread_mutex_unlock(&db->lock);
        usleep(1000);
        pthread_mutex_lock(&db->lock);
        t = db_find_table(db, name);
    }
    if (t) t->scan_refs++;
    pthread_mutex_unlock(&db->lock);
    return t;
}

void tsdb_db_scan_release(tsdb_db_t *db, tsdb_table_internal_t *t) {
    if (!db || !t) return;
    pthread_mutex_lock(&db->lock);
    if (t->scan_refs > 0) t->scan_refs--;
    pthread_mutex_unlock(&db->lock);
}

/* Flush every open table's memtable to its on-disk partition.  Used
 * by /backup to capture an on-disk-consistent snapshot — pre-fix the
 * tarball missed any rows still buffered in memtable, so a 100-row
 * SELECT returned 0 on a freshly-restored sidecar.  Best-effort: a
 * single table's flush failure logs but doesn't abort the whole
 * sweep.
 *
 * skip_replicate=0: replication (row-level on_replicate and the
 * raw-block hook) fires from flush_and_clear_locked, and a memtable
 * flushes exactly once — clearing it destroys the only opportunity to
 * ship those rows.  Under flush-on-commit the memtable is already empty
 * here, so this sweep replicates nothing new; under TSDB_WAL_ONLY_COMMIT
 * (the cluster's mode) the memtable holds up to block_points rows that
 * peers have never seen, and suppressing the hook silently dropped them
 * from replication for good.  A flush that skips replication is only
 * correct when the rows came *from* a peer (batch local_only), which is
 * not this path. */
int tsdb_db_flush_all(tsdb_db_t *db) {
    if (!db) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&db->lock);
    /* Snapshot the table-pointer array so we can drop db->lock before
     * calling flush_and_clear_ex (which takes per-table mutexes that
     * would otherwise re-enter under db->lock). */
    int n = db->ntables;
    tsdb_table_internal_t **tabs = malloc((size_t)n * sizeof(*tabs));
    if (!tabs) { pthread_mutex_unlock(&db->lock); return TSDB_ERR_NOMEM; }
    for (int i = 0; i < n; i++) tabs[i] = db->tables[i];
    pthread_mutex_unlock(&db->lock);

    int first_err = TSDB_OK;
    for (int i = 0; i < n; i++) {
        if (!tabs[i]) continue;
        /* Hold the writer lock across the flush (batch_mu -> compact_mtx, the
         * same order as the idle-flush path).  flush_and_clear_ex snapshots the
         * memtable's sorted-permutation and clears the flushed rows; without
         * this a concurrent out-of-order writer can append between the snapshot
         * and the clear, so the wrong row subset is selected and freshly
         * committed rows are dropped. */
        tsdb_table_lock_write((tsdb_table_t *)tabs[i]);
        int rc = flush_and_clear_ex(tabs[i], /*skip_replicate=*/0);
        tsdb_table_unlock_write((tsdb_table_t *)tabs[i]);
        if (rc != TSDB_OK) {
            fprintf(stderr, "[flush_all] %s rc=%d\n", tabs[i]->name, rc);
            if (first_err == TSDB_OK) first_err = rc;
        }
    }
    free(tabs);
    return first_err;
}

/* Flush ONE table's memtable to its on-disk partitions.  Needed by any
 * operation that reads partition files directly — under deferred-flush mode
 * the freshly committed rows are still buffered and such an operation would
 * otherwise see an empty table.  skip_replicate=0 so the forced drain is
 * indistinguishable from the size/idle flush it front-runs: the cluster hook
 * still observes the rows.  Writer lock held across the flush for the reason
 * spelled out in tsdb_db_flush_all.  TSDB_ERR_NOTFOUND for an unknown table. */
int tsdb_table_flush(tsdb_db_t *db, const char *name) {
    if (!db || !name) return TSDB_ERR_INVAL;
    tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, name);
    if (!t) return TSDB_ERR_NOTFOUND;
    tsdb_table_lock_write((tsdb_table_t *)t);
    int rc = flush_and_clear_ex(t, /*skip_replicate=*/0);
    tsdb_table_unlock_write((tsdb_table_t *)t);
    tsdb_db_scan_release(db, t);
    return rc;
}

int tsdb_db_list_table_names(tsdb_db_t *db, char (*out_names)[64], int max) {
    if (!db || !out_names || max <= 0) return 0;
    pthread_mutex_lock(&db->lock);
    int k = 0;
    for (int i = 0; i < db->ntables && k < max; i++) {
        tsdb_table_internal_t *t = db->tables[i];
        if (!t) continue;
        snprintf(out_names[k], 64, "%s", t->name);
        k++;
    }
    pthread_mutex_unlock(&db->lock);
    return k;
}

tsdb_schema_t   *tsdb_tbl_schema(tsdb_table_internal_t *t)   { return t ? t->schema : NULL; }
tsdb_memtable_t *tsdb_tbl_memtable(tsdb_table_internal_t *t) { return t ? t->memtable : NULL; }
const char      *tsdb_tbl_dir(tsdb_table_internal_t *t)      { return (t && t->schema) ? t->schema->dir : NULL; }
const char      *tsdb_tbl_name(tsdb_table_internal_t *t)     { return t ? t->name : NULL; }
tsdb_catalog_t  *tsdb_db_catalog(tsdb_db_t *db)              { return db ? db->catalog : NULL; }
tsdb_tmq_t      *tsdb_db_tmq(tsdb_db_t *db)                  { return db ? db->tmq : NULL; }
tsdb_auth_t     *tsdb_db_auth(tsdb_db_t *db)                 { return db ? db->auth : NULL; }
tsdb_udf_catalog_t *tsdb_db_udf(tsdb_db_t *db)               { return db ? db->udf  : NULL; }
tsdb_audit_t    *tsdb_db_audit(tsdb_db_t *db)                { return db ? db->audit : NULL; }
pthread_mutex_t *tsdb_tbl_compact_mtx(tsdb_table_internal_t *t) { return t ? &t->compact_mtx : NULL; }

/* ---- RBAC public API (in include/tsdb.h) ------------------------------- */

int tsdb_auth_authenticate(tsdb_db_t *db, const char *username,
                            const char *password, char *out_token,
                            size_t token_cap)
{
    if (!db) return TSDB_ERR_INVAL;
    tsdb_auth_t *a = db->auth;
    if (!a) return TSDB_ERR_INTERNAL;
    int rc = tsdb_auth_login(a, username, password, out_token, token_cap);
    /* Record every login attempt (success or failure) so the operator
     * can detect brute-force or enumeration patterns from the audit
     * trail — the login function itself lives in the catalog layer
     * which has no db handle, so the audit call is hoisted here. */
    tsdb_audit_write(db->audit, username ? username : "", "AUTH",
                     rc == TSDB_OK ? "LOGIN" : "LOGIN_FAILED",
                     "", rc,
                     rc == TSDB_OK ? "" : "invalid credentials");
    return rc;
}

int tsdb_auth_check(tsdb_db_t *db, const char *token,
                     int privilege, const char *resource)
{
    if (!db) return TSDB_ERR_PERMISSION;
    tsdb_auth_t *a = db->auth;
    if (!a) return TSDB_ERR_PERMISSION;
    return tsdb_auth_verify(a, token, privilege, resource);
}

void tsdb_db_set_default_block_points(tsdb_db_t *db, int block_points) {
    if (!db) return;
    int clamped = block_points;
    if (clamped < 0)                   clamped = 0;
    if (clamped > 0 && clamped < 1024) clamped = 1024;
    if (clamped > TSDB_BLOCK_POINTS)   clamped = TSDB_BLOCK_POINTS;
    pthread_mutex_lock(&db->lock);
    db->default_block_points = clamped;
    pthread_mutex_unlock(&db->lock);
}

void tsdb_auth_set_enforce(tsdb_db_t *db, bool enforce) {
    if (!db) return;
    pthread_mutex_lock(&db->lock);
    db->auth_enforce = enforce;
    pthread_mutex_unlock(&db->lock);
}

int tsdb_auth_enforce(tsdb_db_t *db, const char *token,
                       int privilege, const char *resource)
{
    if (!db) return TSDB_ERR_PERMISSION;
    pthread_mutex_lock(&db->lock);
    bool enforce = db->auth_enforce;
    pthread_mutex_unlock(&db->lock);
    if (!enforce) return TSDB_OK;
    return tsdb_auth_check(db, token, privilege, resource);
}

/* ---- Group-commit DB-level API ------------------------------------------ */

/*
 * Internal: start or restart group-commit on a table.
 * Caller must hold db->lock.
 */
static int table_start_gc(tsdb_table_internal_t *t, int64_t window_ns) {
    if (t->gc) {
        tsdb_group_commit_stop(t->gc);
        t->gc = NULL;
    }
    if (window_ns <= 0 || !t->wal) return TSDB_OK;
    return tsdb_group_commit_start(t->wal, window_ns, &t->gc);
}

int tsdb_db_set_group_commit(tsdb_db_t *db, int64_t batch_window_ns) {
    if (!db) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&db->lock);
    db->group_commit_window_ns = batch_window_ns;

    int rc = TSDB_OK;
    for (int i = 0; i < db->ntables && rc == TSDB_OK; i++) {
        if (db->tables[i])
            rc = table_start_gc(db->tables[i], batch_window_ns);
    }
    pthread_mutex_unlock(&db->lock);
    return rc;
}

/*
 * Internal: start group-commit on a newly registered table if db-level
 * GC is active.  Caller must hold db->lock.
 */
static void maybe_start_gc_for_table(tsdb_db_t *db, tsdb_table_internal_t *t) {
    if (db->group_commit_window_ns > 0 && t->wal && !t->gc) {
        tsdb_group_commit_start(t->wal, db->group_commit_window_ns, &t->gc);
    }
}
