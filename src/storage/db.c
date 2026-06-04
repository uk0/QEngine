/* db.c — top-level database implementation. */

#include "db.h"
#include "schema.h"
#include "memtable.h"
#include "part.h"
#include "wal.h"
#include "../../include/tsdb.h"
#include "../catalog/group.h"
#include "../catalog/stable.h"
#include "../catalog/device.h"
#include "../catalog/tmq.h"
#include "../catalog/udf.h"
#include "../catalog/user.h"
#include "../catalog/audit.h"
#include "../server/metrics.h"
#include "retention.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <pthread.h>

/* Forward declaration of mkdir_p from schema.c. */
extern int tsdb_mkdir_p(const char *path);

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
    int              n_data_dirs;
    pthread_mutex_t  lock;

    tsdb_table_internal_t *tables[TSDB_DB_MAX_TABLES];
    int                    ntables;

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

/* Pick the data directory a brand-new table should live in.  Hash by
 * name so the same table always routes to the same disk and the
 * distribution stays even across heterogeneous workloads.  Returns the
 * primary data_dir when striping is disabled. */
static const char *db_pick_data_dir(const tsdb_db_t *db, const char *table_name) {
    if (!db || db->n_data_dirs <= 1) return db->data_dir;
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
            /* Reserve slot 0 for the primary so it's always queried first. */
            snprintf(db->data_dirs[n++], sizeof(db->data_dirs[0]), "%s", db->data_dir);
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
                if (tsdb_mkdir_p(tok) < 0) {
                    fprintf(stderr, "[db] TSDB_DATA_DIRS: mkdir %s failed (%s) — skipping\n",
                            tok, strerror(errno));
                    continue;
                }
                snprintf(db->data_dirs[n++], sizeof(db->data_dirs[0]), "%s", tok);
            }
            db->n_data_dirs = n;
            if (n > 1) {
                fprintf(stderr, "[db] data striping enabled across %d dirs:\n", n);
                for (int i = 0; i < n; i++)
                    fprintf(stderr, "      [%d] %s\n", i, db->data_dirs[i]);
            }
        }
    }

    /* Opt-in: commit only syncs WAL, memtable flush deferred to is_full(). */
    const char *wc = getenv("TSDB_WAL_ONLY_COMMIT");
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

    *out = db;
    return TSDB_OK;
}

void tsdb_close(tsdb_db_t *db) {
    if (!db) return;
    pthread_mutex_lock(&db->lock);

    for (int i = 0; i < db->ntables; i++) {
        tsdb_table_internal_t *t = db->tables[i];
        if (!t) continue;

        /* Flush remaining memtable data. */
        if (t->memtable && tsdb_memtable_rows(t->memtable) > 0) {
            tsdb_part_flush(t->schema, t->memtable);
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
 * restart to pick up the leader's catalog.  This function opens a fresh
 * catalog, swaps it in under db->lock, then closes the old one outside
 * the lock so concurrent queries that already hold the stale pointer
 * finish on their own terms.
 *
 * Ordering is deliberate: open-new before close-old.  If the fresh
 * catalog fails to open (disk read error, corrupt log, OOM) the live
 * catalog is untouched and the caller gets the open rc back.  Callers
 * decide whether to retry or surface the failure.
 */
int tsdb_db_reload_catalog(tsdb_db_t *db) {
    if (!db) return TSDB_ERR_INVAL;

    tsdb_catalog_t *new_cat = NULL;
    int rc = tsdb_catalog_open(db->data_dir, &new_cat);
    if (rc != TSDB_OK) return rc;

    pthread_mutex_lock(&db->lock);
    tsdb_catalog_t *old = db->catalog;
    db->catalog = new_cat;
    pthread_mutex_unlock(&db->lock);

    /* Close outside the lock: catalog_close does fs I/O and holds its
     * own internal mutex; no need to hold db->lock across it.  Any
     * reader that grabbed `old` via tsdb_db_catalog() before our swap
     * is already past db->lock and will finish against the old
     * catalog's still-valid memory until they drop their pointer.  In
     * practice catalog users never cache the pointer past a single
     * query dispatch, so the overlap window is microseconds. */
    if (old) tsdb_catalog_close(old);
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

/* ---- Table lookup (must hold db->lock) ---------------------------------- */

static tsdb_table_internal_t *db_find_table(tsdb_db_t *db, const char *name) {
    for (int i = 0; i < db->ntables; i++) {
        if (db->tables[i] && strcmp(db->tables[i]->name, name) == 0)
            return db->tables[i];
    }
    return NULL;
}

/* ---- Forward declarations ----------------------------------------------- */

/* Defined at end of file; called from create_table_impl and open_table. */
static void maybe_start_gc_for_table(tsdb_db_t *db, tsdb_table_internal_t *t);

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
                              int suppress_hook)
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

    tsdb_schema_t *schema = NULL;
    int rc = tsdb_schema_create_ex(dir, name, cols, (int)ncols, ts_col,
                                    part_unit, bp, &schema);
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

    db->tables[db->ntables++] = t;

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

    /* Cluster schema-sync hook (if registered and not suppressed). */
    if (on_create) {
        on_create(hook_ud, db, name, schema);
    }

    /* Mirror the table into the super-table hierarchy (local catalog only).
     * Runs on every create path — user CREATE, SCHEMA_SYNC apply, raft replay —
     * so every node that has the table also carries its VTable+PTable rows. */
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
                             /*suppress_hook*/ 0);
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
                             /*suppress_hook*/ 0);
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
                             /*suppress_hook*/ 0);
}

int tsdb_create_table_local(tsdb_db_t *db,
                             const char *name,
                             const tsdb_col_t *cols, size_t ncols,
                             const char *ts_col)
{
    return create_table_impl(db, name, cols, ncols, ts_col,
                             TSDB_PARTITION_DAY,
                             /*block_points*/ 0,
                             /*suppress_hook*/ 1);
}

int tsdb_create_table_local_ex(tsdb_db_t *db,
                                const char *name,
                                const tsdb_col_t *cols, size_t ncols,
                                const char *ts_col,
                                int partition_unit_int,
                                int block_points,
                                int sort_by_tag_col)
{
    tsdb_partition_unit_t unit = (partition_unit_int == (int)TSDB_PARTITION_HOUR)
                                  ? TSDB_PARTITION_HOUR
                                  : TSDB_PARTITION_DAY;
    int rc = create_table_impl(db, name, cols, ncols, ts_col, unit,
                               block_points,
                               /*suppress_hook*/ 1);
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

    db->tables[db->ntables++] = t;

    /* Start per-table group-commit if db-level GC is active. */
    maybe_start_gc_for_table(db, t);

    *out = (tsdb_table_t *)t;
    pthread_mutex_unlock(&db->lock);
    return TSDB_OK;
}

/* ---- tsdb_drop_table ---------------------------------------------------- */

/* Forward decl: definitions below. Needed by tsdb_alter_table_add_column.
 * `_locked` variant assumes the caller already holds t->compact_mtx and is
 * what alter_table / future callers-with-the-lock should use; `_ex` is the
 * public entry point that takes/releases the lock for callers without it. */
static int flush_and_clear_locked(tsdb_table_internal_t *t, int skip_replicate);
static int flush_and_clear_ex(tsdb_table_internal_t *t, int skip_replicate);

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

    if (idx >= 0) {
        tsdb_table_internal_t *t = db->tables[idx];
        /* Stop group-commit bg thread before closing WAL. */
        if (t->gc)       { tsdb_group_commit_stop(t->gc); t->gc = NULL; }
        if (t->wal)      tsdb_wal_close(t->wal);
        if (t->memtable) tsdb_memtable_free(t->memtable);
        if (t->schema)   tsdb_schema_free(t->schema);
        pthread_mutex_destroy(&t->compact_mtx);
        pthread_mutex_destroy(&t->batch_mu);
        free(t);

        /* Compact table array. */
        for (int i = idx; i < db->ntables - 1; i++)
            db->tables[i] = db->tables[i + 1];
        db->tables[--db->ntables] = NULL;
    }

    /* Remove WAL file. */
    char wal_path[4096];
    snprintf(wal_path, sizeof(wal_path), "%s/wal/%s.log", db->data_dir, name);
    remove(wal_path);

    /* Remove table directory. */
    char tbl_dir[4096];
    table_dir_db(db, name, tbl_dir, sizeof(tbl_dir));
    rm_rf(tbl_dir);

    pthread_mutex_unlock(&db->lock);

    /* Tear down the super-table + child catalog rows mirrored at create time
     * (after releasing db->lock to keep db→catalog lock ordering clean).
     * Idempotent: a table that predates hierarchy-mirroring has neither. */
    {
        tsdb_catalog_t *cat = tsdb_db_catalog(db);
        if (cat) {
            (void)tsdb_child_table_drop(cat, name);
            (void)tsdb_stable_drop(cat, name);
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
/* Advance the watermark to max(existing, W) and persist.  Best-effort: a
 * write failure leaves the old watermark (a later delete re-advances it). */
static void delwm_bump(tsdb_db_t *db, const char *name, int64_t w) {
    if (w <= 0) return;
    if (tsdb_table_delwm_load(db, name) >= w) return;
    char p[4096]; delwm_path(db, name, p, sizeof(p));
    FILE *f = fopen(p, "wb");
    if (!f) return;
    fwrite(&w, sizeof(w), 1, f);
    fclose(f);
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

            int drop = 0;
            if (op_lt) {
                /* Delete ts < cutoff (or ts <= cutoff when inclusive). */
                drop = inclusive ? (pend <= cutoff_ns) : (pend <  cutoff_ns);
            } else {
                /* Delete ts > cutoff (or ts >= cutoff when inclusive). */
                drop = inclusive ? (pstart >= cutoff_ns) : (pstart > cutoff_ns);
            }
            if (drop) {
                char sub[4096];
                snprintf(sub, sizeof(sub), "%s/%s", tbl_dir, ent->d_name);
                rm_rf(sub);
                removed++;
            }
        }
        closedir(d);
    }

    pthread_mutex_unlock(&t->compact_mtx);

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
    *out = b;
    return TSDB_OK;
}

/*
 * Internal: call replicate hook (if any), flush memtable to partition, clear.
 * Must be called with table's resources available.
 * Only flushes if the memtable has at least one row.
 * skip_replicate=1 suppresses the cluster hook (used for replica-received writes).
 */
/*
 * Internal variant: caller MUST hold t->compact_mtx.
 * Used by alter_table_add_column and by the public flush_and_clear_ex
 * wrapper.  Splitting the lock acquisition out of the body avoids a
 * deadlock when the caller already needs the lock for related work
 * (schema mutation, etc).
 */
static int flush_and_clear_locked(tsdb_table_internal_t *t, int skip_replicate) {
    if (tsdb_memtable_rows(t->memtable) == 0) return TSDB_OK;

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
        if (t->wal) tsdb_wal_truncate(t->wal);
        tsdb_metric_inc("qengine_shard_local_skipped_total");
        return TSDB_OK;
    }

    /* Raw-block replication is triggered from inside tsdb_part_flush_ex
     * after each block encode — pass db + table_name so the hook has context.
     * For replica-received writes (skip_replicate=1) pass NULL to suppress. */
    int rc = tsdb_part_flush_ex(t->schema, t->memtable,
                                 skip_replicate ? NULL : t->db,
                                 t->name);
    if (rc == TSDB_OK) {
        tsdb_metric_inc("qengine_flushes_total");
        tsdb_memtable_clear(t->memtable);
        /* Truncate WAL after successful flush. */
        if (t->wal) tsdb_wal_truncate(t->wal);
    }
    return rc;
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

/*
 * Auto-flush if memtable is full. Called before row_begin.
 * skip_replicate mirrors the batch's local_only flag.
 */
static int maybe_flush_b(tsdb_table_internal_t *t, int skip_replicate) {
    if (tsdb_memtable_is_full(t->memtable)) {
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

    /* Replicate (if cluster hook) + flush any remaining rows.
     * flush_and_clear_ex skips hook when local_only=1 (replica received write).
     *
     * With TSDB_WAL_ONLY_COMMIT, skip the per-commit flush; the memtable
     * drains when it fills via maybe_flush_b().  WAL is still fsynced below
     * so durability is preserved (replay on crash). */
    if (!t->db->wal_only_commit) {
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
    /* Clear any partial row state in the memtable. */
    if (b->in_row) {
        tsdb_memtable_row_abort(b->tbl->memtable);
        b->in_row = 0;
    }
    free(b);
}

void tsdb_batch_set_local_only(tsdb_batch_t *b) {
    if (b) b->local_only = 1;
}

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
         * are variable-length so we walk the wire format to skip the
         * already-consumed prefix.  One-time cost per chunk. */
        const void *col_arrs_off[TSDB_MAX_COLS];
        for (int d = 0; d < ncols_data; d++) {
            const uint8_t *p = (const uint8_t *)col_arrs[d];
            if (col_types[d] == TSDB_TYPE_SYMBOL) {
                /* Skip [u32 total] header, then walk consumed [u16 len][bytes]. */
                const uint8_t *cur = p + 4;
                for (size_t k = 0; k < consumed; k++) {
                    uint16_t l16;
                    memcpy(&l16, cur, 2);
                    cur += 2 + l16;
                }
                /* Build a temp [u32 total][rest] view for the chunk.
                 * Compute new total = sum of [2 + len] for chunk rows. */
                const uint8_t *chunk_start = cur;
                size_t chunk_payload = 0;
                for (size_t k = 0; k < chunk; k++) {
                    uint16_t l16;
                    memcpy(&l16, cur, 2);
                    cur += 2 + l16;
                    chunk_payload += 2 + l16;
                }
                /* memtable_append_bulk expects a [u32 total][...] header.
                 * Allocate a small wrapper buffer for this chunk. */
                uint8_t *tmp = malloc(4 + chunk_payload);
                if (!tmp) return TSDB_ERR_NOMEM;
                uint32_t t32 = (uint32_t)chunk_payload;
                memcpy(tmp, &t32, 4);
                memcpy(tmp + 4, chunk_start, chunk_payload);
                col_arrs_off[d] = tmp;
            } else {
                col_arrs_off[d] = p + consumed * 8;
            }
        }

        int rc = tsdb_memtable_append_bulk(b->tbl->memtable,
                                            ts_bytes + consumed * sizeof(int64_t),
                                            col_arrs_off, col_types,
                                            ncols_data, chunk);

        /* Free the SYMBOL chunk wrappers we malloc'd above. */
        for (int d = 0; d < ncols_data; d++) {
            if (col_types[d] == TSDB_TYPE_SYMBOL) {
                free((void *)col_arrs_off[d]);
            }
        }

        if (rc != TSDB_OK) return rc;
        consumed += chunk;
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
 * sweep.  skip_replicate=1 because the local-flush-for-snapshot path
 * is not a write event peers should observe. */
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

    for (int i = 0; i < n; i++) {
        if (!tabs[i]) continue;
        int rc = flush_and_clear_ex(tabs[i], /*skip_replicate=*/1);
        if (rc != TSDB_OK)
            fprintf(stderr, "[flush_all] %s rc=%d\n", tabs[i]->name, rc);
    }
    free(tabs);
    return TSDB_OK;
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
