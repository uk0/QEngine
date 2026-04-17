/* db.c — top-level database implementation. */

#include "db.h"
#include "schema.h"
#include "memtable.h"
#include "part.h"
#include "wal.h"
#include "../../include/tsdb.h"
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
    tsdb_schema_t   *schema;
    tsdb_memtable_t *memtable;
    tsdb_wal_t      *wal;
    char             name[TSDB_MAX_NAME + 1];
} tsdb_table_internal_t;

/* ---- DB handle ---------------------------------------------------------- */

#define TSDB_DB_MAX_TABLES 128

struct tsdb_db {
    char             data_dir[4096];
    pthread_mutex_t  lock;

    tsdb_table_internal_t *tables[TSDB_DB_MAX_TABLES];
    int                    ntables;
};

/* ---- Batch struct ------------------------------------------------------- */

/*
 * A batch is just a thin wrapper holding a pointer to the table
 * (and tracking in-progress row state).
 */
struct tsdb_batch {
    tsdb_table_internal_t *tbl;
    int                    in_row;
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
        free(t);
    }

    pthread_mutex_unlock(&db->lock);
    pthread_mutex_destroy(&db->lock);
    free(db);
}

/* ---- Table lookup (must hold db->lock) ---------------------------------- */

static tsdb_table_internal_t *db_find_table(tsdb_db_t *db, const char *name) {
    for (int i = 0; i < db->ntables; i++) {
        if (db->tables[i] && strcmp(db->tables[i]->name, name) == 0)
            return db->tables[i];
    }
    return NULL;
}

/* ---- tsdb_create_table -------------------------------------------------- */

int tsdb_create_table(tsdb_db_t *db,
                      const char *name,
                      const tsdb_col_t *cols, size_t ncols,
                      const char *ts_col)
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
    int rc = tsdb_schema_create(dir, name, cols, (int)ncols, ts_col, &schema);
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

    rc = tsdb_memtable_new(schema, &t->memtable);
    if (rc != TSDB_OK) {
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
    pthread_mutex_unlock(&db->lock);
    return TSDB_OK;
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

    rc = tsdb_memtable_new(schema, &t->memtable);
    if (rc != TSDB_OK) {
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
        if (t->wal)      tsdb_wal_close(t->wal);
        if (t->memtable) tsdb_memtable_free(t->memtable);
        if (t->schema)   tsdb_schema_free(t->schema);
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

/* ---- Batch API ---------------------------------------------------------- */

int tsdb_batch_begin(tsdb_table_t *tbl, tsdb_batch_t **out) {
    if (!tbl || !out) return TSDB_ERR_INVAL;

    tsdb_batch_t *b = calloc(1, sizeof(*b));
    if (!b) return TSDB_ERR_NOMEM;

    b->tbl    = (tsdb_table_internal_t *)tbl;
    b->in_row = 0;
    *out = b;
    return TSDB_OK;
}

/*
 * Internal: flush memtable and clear it.
 * Must be called with table's resources available.
 */
static int flush_and_clear(tsdb_table_internal_t *t) {
    int rc = tsdb_part_flush(t->schema, t->memtable);
    if (rc == TSDB_OK) {
        tsdb_memtable_clear(t->memtable);
        /* Truncate WAL after successful flush. */
        if (t->wal) tsdb_wal_truncate(t->wal);
    }
    return rc;
}

/*
 * Auto-flush if memtable is full. Called before row_begin.
 */
static int maybe_flush(tsdb_table_internal_t *t) {
    if (tsdb_memtable_is_full(t->memtable)) {
        return flush_and_clear(t);
    }
    return TSDB_OK;
}

int tsdb_batch_row_ts(tsdb_batch_t *b, tsdb_ts_t ts) {
    if (!b) return TSDB_ERR_INVAL;
    tsdb_table_internal_t *t = b->tbl;

    if (!b->in_row) {
        /* Auto-flush if full, then begin row. */
        int rc = maybe_flush(t);
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

    /* Flush if full. */
    int rc = maybe_flush(t);
    if (rc != TSDB_OK) return rc;

    /* Sync WAL. */
    if (t->wal) tsdb_wal_sync(t->wal);

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
