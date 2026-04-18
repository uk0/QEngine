/* memtable.c — columnar in-memory write buffer. */

#include "memtable.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/*
 * Each column buffer holds TSDB_BLOCK_POINTS values.
 * Width varies by type (see tsdb_type_width).
 */
struct tsdb_memtable {
    tsdb_schema_t   *schema;
    pthread_mutex_t  lock;

    size_t           nrows;       /* committed rows */

    /* Per-column flat value buffers (malloc'd).
     * Size = TSDB_BLOCK_POINTS * tsdb_type_width(type).
     */
    void           **col_bufs;

    /* Per-row write tracking: bitmask of which cols were set this row.
     * We use a simple uint8_t per column. */
    uint8_t         *col_set;     /* ncols elements, reset on row_begin */
    int              in_row;      /* 1 if between row_begin/row_end */
    int              ts_set;      /* ts was set in current row */
};

int tsdb_memtable_new(tsdb_schema_t *s, tsdb_memtable_t **out) {
    if (!s || !out) return TSDB_ERR_INVAL;

    tsdb_memtable_t *m = calloc(1, sizeof(*m));
    if (!m) return TSDB_ERR_NOMEM;

    m->schema = s;
    pthread_mutex_init(&m->lock, NULL);

    /* Allocate column buffer array. */
    m->col_bufs = calloc((size_t)s->ncols, sizeof(void*));
    if (!m->col_bufs) { free(m); return TSDB_ERR_NOMEM; }

    for (int i = 0; i < s->ncols; i++) {
        size_t w = tsdb_type_width(s->cols[i].type);
        m->col_bufs[i] = malloc(w * TSDB_BLOCK_POINTS);
        if (!m->col_bufs[i]) {
            for (int j = 0; j < i; j++) free(m->col_bufs[j]);
            free(m->col_bufs);
            free(m);
            return TSDB_ERR_NOMEM;
        }
    }

    m->col_set = calloc((size_t)s->ncols, sizeof(uint8_t));
    if (!m->col_set) {
        for (int i = 0; i < s->ncols; i++) free(m->col_bufs[i]);
        free(m->col_bufs);
        free(m);
        return TSDB_ERR_NOMEM;
    }

    *out = m;
    return TSDB_OK;
}

int tsdb_memtable_extend_for_new_column(tsdb_memtable_t *m) {
    if (!m) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    if (m->in_row || m->nrows != 0) {
        pthread_mutex_unlock(&m->lock);
        return TSDB_ERR_INVAL;
    }
    int new_n = m->schema->ncols;           /* schema was already extended */
    int old_n = new_n - 1;

    void **nb = realloc(m->col_bufs, (size_t)new_n * sizeof(void *));
    if (!nb) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_NOMEM; }
    m->col_bufs = nb;

    size_t w = tsdb_type_width(m->schema->cols[old_n].type);
    m->col_bufs[old_n] = malloc(w * TSDB_BLOCK_POINTS);
    if (!m->col_bufs[old_n]) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_NOMEM; }

    uint8_t *ns = realloc(m->col_set, (size_t)new_n * sizeof(uint8_t));
    if (!ns) { free(m->col_bufs[old_n]); pthread_mutex_unlock(&m->lock); return TSDB_ERR_NOMEM; }
    m->col_set = ns;
    m->col_set[old_n] = 0;

    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

void tsdb_memtable_free(tsdb_memtable_t *m) {
    if (!m) return;
    pthread_mutex_lock(&m->lock);
    for (int i = 0; i < m->schema->ncols; i++) {
        free(m->col_bufs[i]);
    }
    free(m->col_bufs);
    free(m->col_set);
    pthread_mutex_unlock(&m->lock);
    pthread_mutex_destroy(&m->lock);
    free(m);
}

int tsdb_memtable_row_begin(tsdb_memtable_t *m) {
    if (!m) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    if (m->in_row) {
        pthread_mutex_unlock(&m->lock);
        return TSDB_ERR_INVAL;
    }
    if (m->nrows >= TSDB_BLOCK_POINTS) {
        /* Memtable is full — caller must flush and clear. */
        pthread_mutex_unlock(&m->lock);
        return TSDB_ERR_FULL;
    }
    memset(m->col_set, 0, (size_t)m->schema->ncols);
    m->in_row  = 1;
    m->ts_set  = 0;
    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

int tsdb_memtable_row_ts(tsdb_memtable_t *m, tsdb_ts_t v) {
    if (!m) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    if (!m->in_row) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }

    int col = m->schema->ts_col_idx;
    int64_t *buf = (int64_t *)m->col_bufs[col];
    buf[m->nrows] = (int64_t)v;
    m->col_set[col] = 1;
    m->ts_set = 1;
    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

int tsdb_memtable_row_i64(tsdb_memtable_t *m, int col, int64_t v) {
    if (!m) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    if (!m->in_row) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }
    if (col < 0 || col >= m->schema->ncols) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }
    if (m->schema->cols[col].type != TSDB_TYPE_INT64) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_SCHEMA; }

    int64_t *buf = (int64_t *)m->col_bufs[col];
    buf[m->nrows] = v;
    m->col_set[col] = 1;
    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

int tsdb_memtable_row_f64(tsdb_memtable_t *m, int col, double v) {
    if (!m) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    if (!m->in_row) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }
    if (col < 0 || col >= m->schema->ncols) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }
    if (m->schema->cols[col].type != TSDB_TYPE_FLOAT64) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_SCHEMA; }

    double *buf = (double *)m->col_bufs[col];
    buf[m->nrows] = v;
    m->col_set[col] = 1;
    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

int tsdb_memtable_row_sym(tsdb_memtable_t *m, int col, const char *s) {
    if (!m || !s) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    if (!m->in_row) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }
    if (col < 0 || col >= m->schema->ncols) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }
    if (m->schema->cols[col].type != TSDB_TYPE_SYMBOL) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_SCHEMA; }
    if (!m->schema->cols[col].symtab) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INTERNAL; }

    uint32_t code = tsdb_symtab_intern(m->schema->cols[col].symtab, s);
    if (code == TSDB_SYMBOL_INVALID) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_NOMEM; }

    uint32_t *buf = (uint32_t *)m->col_bufs[col];
    buf[m->nrows] = code;
    m->col_set[col] = 1;
    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

int tsdb_memtable_row_end(tsdb_memtable_t *m) {
    if (!m) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    if (!m->in_row) { pthread_mutex_unlock(&m->lock); return TSDB_ERR_INVAL; }

    /* Validate all columns were set. */
    for (int i = 0; i < m->schema->ncols; i++) {
        if (!m->col_set[i]) {
            m->in_row = 0;
            pthread_mutex_unlock(&m->lock);
            return TSDB_ERR_SCHEMA;
        }
    }

    m->nrows++;
    m->in_row = 0;
    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

size_t tsdb_memtable_rows(tsdb_memtable_t *m) {
    if (!m) return 0;
    pthread_mutex_lock(&m->lock);
    size_t n = m->nrows;
    pthread_mutex_unlock(&m->lock);
    return n;
}

int tsdb_memtable_is_full(tsdb_memtable_t *m) {
    if (!m) return 0;
    pthread_mutex_lock(&m->lock);
    int full = (m->nrows >= TSDB_BLOCK_POINTS);
    pthread_mutex_unlock(&m->lock);
    return full;
}

void tsdb_memtable_clear(tsdb_memtable_t *m) {
    if (!m) return;
    pthread_mutex_lock(&m->lock);
    m->nrows  = 0;
    m->in_row = 0;
    m->ts_set = 0;
    pthread_mutex_unlock(&m->lock);
}

void tsdb_memtable_row_abort(tsdb_memtable_t *m) {
    if (!m) return;
    pthread_mutex_lock(&m->lock);
    m->in_row = 0;
    m->ts_set = 0;
    memset(m->col_set, 0, (size_t)m->schema->ncols);
    pthread_mutex_unlock(&m->lock);
}

const void *tsdb_memtable_col(tsdb_memtable_t *m, int col) {
    if (!m || col < 0 || col >= m->schema->ncols) return NULL;
    /* No lock needed for read after flush (caller responsibility). */
    return m->col_bufs[col];
}
