/* db.c — top-level database implementation. */

#include "db.h"
#include "schema.h"
#include "memtable.h"
#include "part.h"
#include "wal.h"
#include "../../include/tsdb.h"
#include "../catalog/group.h"
#include "../catalog/device.h"
#include "../catalog/tmq.h"
#include "../catalog/udf.h"
#include "../catalog/user.h"
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
} tsdb_table_internal_t;

/* ---- DB handle ---------------------------------------------------------- */

#define TSDB_DB_MAX_TABLES 128

struct tsdb_db {
    char             data_dir[4096];
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

    /* Opt-in enforcement flag.  Default false: tsdb_auth_enforce is a no-op.
     * Flip to true to auto-consult tsdb_auth_check via tsdb_auth_enforce. */
    bool                 auth_enforce;

    /* Retention GC (NULL when not configured). Stopped in tsdb_close(). */
    struct tsdb_retention *retention;

    /* Group-commit window (0 = disabled). Applied to all table WALs. */
    int64_t              group_commit_window_ns;
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

/* ---- Path helpers ------------------------------------------------------- */

static void table_dir(const char *data_dir, const char *table_name, char *out, size_t cap) {
    snprintf(out, cap, "%s/%s", data_dir, table_name);
}

/* ---- DB open / close ---------------------------------------------------- */

int tsdb_open(const char *data_dir, tsdb_db_t **out) {
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    if (tsdb_mkdir_p(data_dir) < 0) return TSDB_ERR_IO;

    tsdb_db_t *db = calloc(1, sizeof(*db));
    if (!db) return TSDB_ERR_NOMEM;

    snprintf(db->data_dir, sizeof(db->data_dir), "%s", data_dir);
    pthread_mutex_init(&db->lock, NULL);

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
        free(t);
    }

    if (db->udf)     tsdb_udf_catalog_close(db->udf);
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

/* ---- Retention GC attachment -------------------------------------------- */

void tsdb_db_attach_retention(tsdb_db_t *db, struct tsdb_retention *r) {
    if (!db) return;
    pthread_mutex_lock(&db->lock);
    struct tsdb_retention *old = db->retention;
    db->retention = r;
    pthread_mutex_unlock(&db->lock);
    if (old) tsdb_retention_stop(old);
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

/* ---- tsdb_create_table -------------------------------------------------- */

/* Internal: create table with optional hook suppression. */
static int create_table_impl(tsdb_db_t *db,
                              const char *name,
                              const tsdb_col_t *cols, size_t ncols,
                              const char *ts_col,
                              tsdb_partition_unit_t part_unit,
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
    table_dir(db->data_dir, name, dir, sizeof(dir));

    tsdb_schema_t *schema = NULL;
    int rc = tsdb_schema_create_ex(dir, name, cols, (int)ncols, ts_col,
                                    part_unit, &schema);
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

    rc = tsdb_memtable_new(schema, &t->memtable);
    if (rc != TSDB_OK) {
        pthread_mutex_destroy(&t->compact_mtx);
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

    /* Snapshot hook pointers under lock, then call after unlock to avoid
     * deadlock if the hook re-enters any db operation. */
    tsdb_on_create_fn on_create = suppress_hook ? NULL : db->on_create;
    void *hook_ud = db->hook_ud;

    pthread_mutex_unlock(&db->lock);

    /* Cluster schema-sync hook (if registered and not suppressed). */
    if (on_create) {
        on_create(hook_ud, db, name, schema);
    }

    return TSDB_OK;
}

int tsdb_create_table(tsdb_db_t *db,
                      const char *name,
                      const tsdb_col_t *cols, size_t ncols,
                      const char *ts_col)
{
    return create_table_impl(db, name, cols, ncols, ts_col,
                             TSDB_PARTITION_DAY, 0 /* sync */);
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
    return create_table_impl(db, name, cols, ncols, ts_col, unit, 0 /* sync */);
}

int tsdb_create_table_local(tsdb_db_t *db,
                             const char *name,
                             const tsdb_col_t *cols, size_t ncols,
                             const char *ts_col)
{
    return create_table_impl(db, name, cols, ncols, ts_col,
                             TSDB_PARTITION_DAY, 1 /* no sync */);
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
    table_dir(db->data_dir, name, dir, sizeof(dir));

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

    rc = tsdb_memtable_new(schema, &t->memtable);
    if (rc != TSDB_OK) {
        pthread_mutex_destroy(&t->compact_mtx);
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

/* Forward decl: definition below. Needed by tsdb_alter_table_add_column. */
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

    if (idx >= 0) {
        tsdb_table_internal_t *t = db->tables[idx];
        /* Stop group-commit bg thread before closing WAL. */
        if (t->gc)       { tsdb_group_commit_stop(t->gc); t->gc = NULL; }
        if (t->wal)      tsdb_wal_close(t->wal);
        if (t->memtable) tsdb_memtable_free(t->memtable);
        if (t->schema)   tsdb_schema_free(t->schema);
        pthread_mutex_destroy(&t->compact_mtx);
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
    table_dir(db->data_dir, name, tbl_dir, sizeof(tbl_dir));
    rm_rf(tbl_dir);

    pthread_mutex_unlock(&db->lock);
    return TSDB_OK;
}

/* ---- ALTER TABLE ADD COLUMN -------------------------------------------- */

int tsdb_alter_table_add_column(tsdb_db_t *db, const char *table_name,
                                 const char *col_name, tsdb_type_t col_type)
{
    if (!db || !table_name || !col_name) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&db->lock);
    tsdb_table_internal_t *t = NULL;
    for (int i = 0; i < db->ntables; i++) {
        if (db->tables[i] && strcmp(db->tables[i]->name, table_name) == 0) {
            t = db->tables[i]; break;
        }
    }
    pthread_mutex_unlock(&db->lock);

    if (!t) return TSDB_ERR_NOTFOUND;

    /* Serialise with compactor and concurrent flushes. */
    pthread_mutex_lock(&t->compact_mtx);

    /* Force memtable to disk so we can safely re-size its column buffers. */
    int rc = flush_and_clear_ex(t, /*skip_replicate=*/1);
    if (rc != TSDB_OK) { pthread_mutex_unlock(&t->compact_mtx); return rc; }

    /* Persist the schema change first. */
    rc = tsdb_schema_add_column(t->schema, col_name, col_type);
    if (rc != TSDB_OK) { pthread_mutex_unlock(&t->compact_mtx); return rc; }

    /* Grow the memtable's column buffer to match. */
    rc = tsdb_memtable_extend_for_new_column(t->memtable);
    /* If this fails the schema is already updated on disk; we leave the
     * in-memory schema consistent but the memtable cannot ingest new rows
     * for this column until reopen. Surface the error unchanged. */

    pthread_mutex_unlock(&t->compact_mtx);
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
static int flush_and_clear_ex(tsdb_table_internal_t *t, int skip_replicate) {
    if (tsdb_memtable_rows(t->memtable) == 0) return TSDB_OK;

    /* Row-level cluster replication: fires BEFORE flush so data still in
     * memtable.  Skipped when raw-block mode is active (on_raw_block != NULL)
     * to avoid double-replication. */
    if (!skip_replicate && t->db && t->db->on_replicate &&
        !t->db->on_raw_block) {
        t->db->on_replicate(t->db->hook_ud, t->db, t->name,
                            t->schema, t->memtable);
        /* Errors are logged by the hook but do not abort the local write. */
    }

    /* Raw-block replication is triggered from inside tsdb_part_flush_ex
     * after each block encode — pass db + table_name so the hook has context.
     * For replica-received writes (skip_replicate=1) pass NULL to suppress. */
    /* Hold compact_mtx so the compactor cannot swap .col/.idx while
     * we are appending new blocks to them. */
    pthread_mutex_lock(&t->compact_mtx);
    int rc = tsdb_part_flush_ex(t->schema, t->memtable,
                                 skip_replicate ? NULL : t->db,
                                 t->name);
    pthread_mutex_unlock(&t->compact_mtx);
    if (rc == TSDB_OK) {
        tsdb_metric_inc("qengine_flushes_total");
        tsdb_memtable_clear(t->memtable);
        /* Truncate WAL after successful flush. */
        if (t->wal) tsdb_wal_truncate(t->wal);
    }
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
     * flush_and_clear_ex skips hook when local_only=1 (replica received write). */
    int rc = flush_and_clear_ex(t, b->local_only);
    if (rc != TSDB_OK) return rc;

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

tsdb_table_internal_t *tsdb_db_find_table(tsdb_db_t *db, const char *name) {
    if (!db || !name) return NULL;
    pthread_mutex_lock(&db->lock);
    tsdb_table_internal_t *t = db_find_table(db, name);
    pthread_mutex_unlock(&db->lock);
    return t;
}

tsdb_schema_t   *tsdb_tbl_schema(tsdb_table_internal_t *t)   { return t ? t->schema : NULL; }
tsdb_memtable_t *tsdb_tbl_memtable(tsdb_table_internal_t *t) { return t ? t->memtable : NULL; }
const char      *tsdb_tbl_dir(tsdb_table_internal_t *t)      { return (t && t->schema) ? t->schema->dir : NULL; }
const char      *tsdb_tbl_name(tsdb_table_internal_t *t)     { return t ? t->name : NULL; }
tsdb_catalog_t  *tsdb_db_catalog(tsdb_db_t *db)              { return db ? db->catalog : NULL; }
tsdb_tmq_t      *tsdb_db_tmq(tsdb_db_t *db)                  { return db ? db->tmq : NULL; }
tsdb_auth_t     *tsdb_db_auth(tsdb_db_t *db)                 { return db ? db->auth : NULL; }
tsdb_udf_catalog_t *tsdb_db_udf(tsdb_db_t *db)               { return db ? db->udf  : NULL; }
pthread_mutex_t *tsdb_tbl_compact_mtx(tsdb_table_internal_t *t) { return t ? &t->compact_mtx : NULL; }

/* ---- RBAC public API (in include/tsdb.h) ------------------------------- */

int tsdb_auth_authenticate(tsdb_db_t *db, const char *username,
                            const char *password, char *out_token,
                            size_t token_cap)
{
    if (!db) return TSDB_ERR_INVAL;
    tsdb_auth_t *a = db->auth;
    if (!a) return TSDB_ERR_INTERNAL;
    return tsdb_auth_login(a, username, password, out_token, token_cap);
}

int tsdb_auth_check(tsdb_db_t *db, const char *token,
                     int privilege, const char *resource)
{
    if (!db) return TSDB_ERR_PERMISSION;
    tsdb_auth_t *a = db->auth;
    if (!a) return TSDB_ERR_PERMISSION;
    return tsdb_auth_verify(a, token, privilege, resource);
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
