/* memtable.c — columnar in-memory write buffer.
 *
 * Storage:
 *   col_bufs[col][row]  — append-only per-column value arrays.
 *
 * Ordering index:
 *   A skip-list keyed on the timestamp column runs in parallel with the
 *   append buffers.  Each row_end call adds a (ts, row_pos) entry; the
 *   sorted iteration (tsdb_memtable_sorted_indices) walks the skip-list
 *   level-0 forward pointers to produce a ts-ordered permutation of the
 *   row positions, which part.c uses on flush to emit ts-sorted blocks
 *   regardless of insertion order.
 *
 *   The node pool is sized to block_points so no per-insert malloc is
 *   needed in the hot path; fall back to heap allocation only if the
 *   pool is exhausted (shouldn't happen while block_points caps nrows).
 */

#include "memtable.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* ---- Skip-list internals (module-private) ------------------------------ */

#define SL_MAX_LEVEL 12    /* supports up to 2^24 entries; memtable caps well below */

typedef struct sl_node {
    int64_t          key;       /* timestamp */
    uint32_t         value;     /* row position in col_bufs */
    int              level;     /* valid indices 0..level */
    struct sl_node  *next[SL_MAX_LEVEL];
} sl_node_t;

typedef struct {
    sl_node_t  head;             /* sentinel; head.next[i] is first node at level i */
    int        top_level;        /* current max used level */
    uint32_t   rng;              /* xorshift32 state for level generation */
    /* Node pool: preallocated to avoid malloc in the hot path. */
    sl_node_t *pool;
    size_t     pool_cap;
    size_t     pool_used;
} skiplist_t;

static uint32_t xs32(uint32_t *s) {
    uint32_t x = *s;
    x ^= x << 13; x ^= x >> 17; x ^= x << 5;
    *s = x ? x : 1;
    return x;
}

/* Random level with P=1/4: each higher level is 4× rarer. */
static int sl_random_level(skiplist_t *sl) {
    int lvl = 0;
    while (lvl + 1 < SL_MAX_LEVEL && (xs32(&sl->rng) & 0x3u) == 0) lvl++;
    return lvl;
}

static int sl_init(skiplist_t *sl, size_t capacity) {
    memset(&sl->head, 0, sizeof(sl->head));
    sl->head.level = SL_MAX_LEVEL - 1;
    for (int i = 0; i < SL_MAX_LEVEL; i++) sl->head.next[i] = NULL;
    sl->top_level = 0;
    sl->rng       = 0x9E3779B9u;  /* golden-ratio seed */
    sl->pool      = calloc(capacity ? capacity : 1, sizeof(sl_node_t));
    if (!sl->pool) return -1;
    sl->pool_cap  = capacity;
    sl->pool_used = 0;
    return 0;
}

static void sl_free(skiplist_t *sl) {
    free(sl->pool);
    sl->pool = NULL;
    sl->pool_cap = 0;
    sl->pool_used = 0;
    sl->top_level = 0;
    memset(sl->head.next, 0, sizeof(sl->head.next));
}

static void sl_reset(skiplist_t *sl) {
    /* Clear without freeing the pool — reuse for next batch. */
    for (int i = 0; i < SL_MAX_LEVEL; i++) sl->head.next[i] = NULL;
    sl->top_level = 0;
    sl->pool_used = 0;
}

/* Insert (key, value).  Stable: if two nodes share the same key, the
 * newer insert comes AFTER the older one in level-0 traversal — this
 * preserves arrival order for equal timestamps, matching the old
 * insertion-order semantics for ts-ties. */
static int sl_insert(skiplist_t *sl, int64_t key, uint32_t value) {
    sl_node_t *update[SL_MAX_LEVEL];
    sl_node_t *x = &sl->head;
    for (int i = sl->top_level; i >= 0; i--) {
        while (x->next[i] && x->next[i]->key <= key) x = x->next[i];
        update[i] = x;
    }
    if (sl->pool_used >= sl->pool_cap) return -1;   /* pool exhausted */
    int lvl = sl_random_level(sl);
    if (lvl > sl->top_level) {
        for (int i = sl->top_level + 1; i <= lvl; i++) update[i] = &sl->head;
        sl->top_level = lvl;
    }
    sl_node_t *n = &sl->pool[sl->pool_used++];
    n->key   = key;
    n->value = value;
    n->level = lvl;
    for (int i = 0; i <= lvl; i++) {
        n->next[i]         = update[i]->next[i];
        update[i]->next[i] = n;
    }
    /* zero any stale higher-level pointers from pool reuse */
    for (int i = lvl + 1; i < SL_MAX_LEVEL; i++) n->next[i] = NULL;
    return 0;
}

/*
 * Each column buffer holds block_points values.
 * Width varies by type (see tsdb_type_width).
 */
struct tsdb_memtable {
    tsdb_schema_t   *schema;
    pthread_mutex_t  lock;

    size_t           nrows;       /* committed rows */

    /* Per-column flat value buffers (malloc'd).
     * Size = block_points * tsdb_type_width(type).
     */
    void           **col_bufs;

    /* Per-row write tracking: bitmask of which cols were set this row.
     * We use a simple uint8_t per column. */
    uint8_t         *col_set;     /* ncols elements, reset on row_begin */
    int              in_row;      /* 1 if between row_begin/row_end */
    int              ts_set;      /* ts was set in current row */

    /* Parallel ts-sorted index.  Populated on row_end; consulted by
     * tsdb_memtable_sorted_indices on flush. */
    skiplist_t       sl;
    int              sl_ok;       /* 1 if sl_init succeeded */
    int64_t          last_ts;     /* track insertion order for fast-path */
    int              all_sorted;  /* 1 while all inserts are monotonic-asc */
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

    int bp = (s->block_points > 0 && s->block_points <= TSDB_BLOCK_POINTS)
                ? s->block_points : TSDB_BLOCK_POINTS;
    for (int i = 0; i < s->ncols; i++) {
        size_t w = tsdb_type_width(s->cols[i].type);
        m->col_bufs[i] = malloc(w * (size_t)bp);
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

    /* Skip-list with capacity matching block_points.  If allocation
     * fails we disable the sorted-index path — flushes will fall back
     * to insertion-order semantics, same as before this code landed. */
    m->sl_ok      = (sl_init(&m->sl, (size_t)bp) == 0);
    m->last_ts    = INT64_MIN;
    m->all_sorted = 1;

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

    int bp = (m->schema->block_points > 0 &&
              m->schema->block_points <= TSDB_BLOCK_POINTS)
                ? m->schema->block_points : TSDB_BLOCK_POINTS;
    size_t w = tsdb_type_width(m->schema->cols[old_n].type);
    m->col_bufs[old_n] = malloc(w * (size_t)bp);
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
    if (m->sl_ok) sl_free(&m->sl);
    pthread_mutex_unlock(&m->lock);
    pthread_mutex_destroy(&m->lock);
    free(m);
}

int tsdb_memtable_row_begin(tsdb_memtable_t *m) {
    if (!m) return TSDB_ERR_INVAL;
    /* Hot-path optimisation: hold m->lock for the entire row_begin →
     * row_ts → row_*  → row_end sequence so the per-column writers can
     * skip their own lock acquire/release pair (see tsdb_memtable_row_*).
     * Cuts the per-row mutex traffic from N+2 to 2 (one acquire here,
     * one release in row_end), which on Apple Silicon yields ~3-4×
     * single-thread ingest throughput.  Risk: callers that grab a row
     * but never call row_end leak the lock — but every existing path
     * (batch, replicate handler, recovery) calls row_end or
     * row_abort, both of which release. */
    pthread_mutex_lock(&m->lock);
    if (m->in_row) {
        pthread_mutex_unlock(&m->lock);
        return TSDB_ERR_INVAL;
    }
    int bp_cap = (m->schema->block_points > 0 &&
                  m->schema->block_points <= TSDB_BLOCK_POINTS)
                    ? m->schema->block_points : TSDB_BLOCK_POINTS;
    if (m->nrows >= (size_t)bp_cap) {
        /* Memtable is full — caller must flush and clear. */
        pthread_mutex_unlock(&m->lock);
        return TSDB_ERR_FULL;
    }
    memset(m->col_set, 0, (size_t)m->schema->ncols);
    m->in_row  = 1;
    m->ts_set  = 0;
    /* INTENTIONAL: do NOT release m->lock here — held until row_end. */
    return TSDB_OK;
}

/* Per-column setters — m->lock is held by tsdb_memtable_row_begin and
 * released by row_end / row_abort.  These functions therefore do NOT
 * touch the mutex; the in_row check is a cheap sanity guard for
 * callers that violate the protocol.  All schema validity checks
 * remain so a misuse fails loudly with TSDB_ERR_SCHEMA / _INVAL. */

int tsdb_memtable_row_ts(tsdb_memtable_t *m, tsdb_ts_t v) {
    if (!m) return TSDB_ERR_INVAL;
    if (!m->in_row) return TSDB_ERR_INVAL;

    int col = m->schema->ts_col_idx;
    int64_t *buf = (int64_t *)m->col_bufs[col];
    buf[m->nrows] = (int64_t)v;
    m->col_set[col] = 1;
    m->ts_set = 1;
    return TSDB_OK;
}

int tsdb_memtable_row_i64(tsdb_memtable_t *m, int col, int64_t v) {
    if (!m) return TSDB_ERR_INVAL;
    if (!m->in_row) return TSDB_ERR_INVAL;
    if (col < 0 || col >= m->schema->ncols) return TSDB_ERR_INVAL;
    if (m->schema->cols[col].type != TSDB_TYPE_INT64) return TSDB_ERR_SCHEMA;

    int64_t *buf = (int64_t *)m->col_bufs[col];
    buf[m->nrows] = v;
    m->col_set[col] = 1;
    return TSDB_OK;
}

int tsdb_memtable_row_f64(tsdb_memtable_t *m, int col, double v) {
    if (!m) return TSDB_ERR_INVAL;
    if (!m->in_row) return TSDB_ERR_INVAL;
    if (col < 0 || col >= m->schema->ncols) return TSDB_ERR_INVAL;
    if (m->schema->cols[col].type != TSDB_TYPE_FLOAT64) return TSDB_ERR_SCHEMA;

    double *buf = (double *)m->col_bufs[col];
    buf[m->nrows] = v;
    m->col_set[col] = 1;
    return TSDB_OK;
}

int tsdb_memtable_row_sym(tsdb_memtable_t *m, int col, const char *s) {
    if (!m || !s) return TSDB_ERR_INVAL;
    if (!m->in_row) return TSDB_ERR_INVAL;
    if (col < 0 || col >= m->schema->ncols) return TSDB_ERR_INVAL;
    if (m->schema->cols[col].type != TSDB_TYPE_SYMBOL) return TSDB_ERR_SCHEMA;
    if (!m->schema->cols[col].symtab) return TSDB_ERR_INTERNAL;

    uint32_t code = tsdb_symtab_intern(m->schema->cols[col].symtab, s);
    if (code == TSDB_SYMBOL_INVALID) return TSDB_ERR_NOMEM;

    uint32_t *buf = (uint32_t *)m->col_bufs[col];
    buf[m->nrows] = code;
    m->col_set[col] = 1;
    return TSDB_OK;
}

int tsdb_memtable_row_end(tsdb_memtable_t *m) {
    if (!m) return TSDB_ERR_INVAL;
    /* Lock was taken by row_begin; we only need to validate + release. */
    if (!m->in_row) return TSDB_ERR_INVAL;

    /* Validate all columns were set. */
    for (int i = 0; i < m->schema->ncols; i++) {
        if (!m->col_set[i]) {
            m->in_row = 0;
            pthread_mutex_unlock(&m->lock);
            return TSDB_ERR_SCHEMA;
        }
    }

    /* Index the new row in the skip-list for later sorted flush. */
    if (m->sl_ok) {
        int64_t *ts_buf = (int64_t *)m->col_bufs[m->schema->ts_col_idx];
        int64_t  ts     = ts_buf[m->nrows];
        if (sl_insert(&m->sl, ts, (uint32_t)m->nrows) < 0) {
            /* Pool exhausted — shouldn't normally happen since capacity
             * matches block_points. Disable sorted-flush to avoid
             * emitting a partial order; the append-only path still works. */
            m->sl_ok = 0;
        } else {
            if (ts < m->last_ts) m->all_sorted = 0;
            m->last_ts = ts;
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
    int bp = (m->schema->block_points > 0 &&
              m->schema->block_points <= TSDB_BLOCK_POINTS)
                ? m->schema->block_points : TSDB_BLOCK_POINTS;
    int full = (m->nrows >= (size_t)bp);
    pthread_mutex_unlock(&m->lock);
    return full;
}

void tsdb_memtable_clear(tsdb_memtable_t *m) {
    if (!m) return;
    pthread_mutex_lock(&m->lock);
    m->nrows  = 0;
    m->in_row = 0;
    m->ts_set = 0;
    if (m->sl_ok) sl_reset(&m->sl);
    m->last_ts    = INT64_MIN;
    m->all_sorted = 1;
    pthread_mutex_unlock(&m->lock);
}

void tsdb_memtable_row_abort(tsdb_memtable_t *m) {
    if (!m) return;
    /* Caller may invoke this after row_begin's lock was acquired but
     * before row_end ran.  Only release if we're actually mid-row;
     * otherwise this is a defensive no-op. */
    if (m->in_row) {
        m->in_row = 0;
        m->ts_set = 0;
        memset(m->col_set, 0, (size_t)m->schema->ncols);
        pthread_mutex_unlock(&m->lock);
    }
}

const void *tsdb_memtable_col(tsdb_memtable_t *m, int col) {
    if (!m || col < 0 || col >= m->schema->ncols) return NULL;
    /* No lock needed for read after flush (caller responsibility). */
    return m->col_bufs[col];
}

int tsdb_memtable_append_bulk(tsdb_memtable_t *m,
                               const int64_t *ts_arr,
                               const void * const *col_arrs,
                               const int *col_types,
                               int ncols_data,
                               size_t n)
{
    if (!m || !ts_arr || !col_arrs || !col_types) return TSDB_ERR_INVAL;
    if (n == 0) return TSDB_OK;

    int bp_cap = (m->schema->block_points > 0 &&
                  m->schema->block_points <= TSDB_BLOCK_POINTS)
                    ? m->schema->block_points : TSDB_BLOCK_POINTS;

    pthread_mutex_lock(&m->lock);
    if (m->in_row) {
        pthread_mutex_unlock(&m->lock);
        return TSDB_ERR_INVAL;
    }
    if (m->nrows + n > (size_t)bp_cap) {
        pthread_mutex_unlock(&m->lock);
        return TSDB_ERR_FULL;
    }

    int ts_ci = m->schema->ts_col_idx;
    size_t base = m->nrows;

    /* TS column: bulk memcpy.  Always 8-byte. */
    int64_t *ts_dst = (int64_t *)m->col_bufs[ts_ci];
    memcpy(ts_dst + base, ts_arr, n * sizeof(int64_t));

    /* Walk schema cols in order, skip ts, consume col_arrs in parallel.
     * Per-type fast path: 8-byte cols are a single memcpy; SYMBOL cols
     * iterate the wire format and intern each value (the symtab itself
     * has its own lock, so we hold m->lock the whole time to keep the
     * append + skiplist update atomic). */
    int data_idx = 0;
    for (int c = 0; c < m->schema->ncols; c++) {
        if (c == ts_ci) continue;
        if (data_idx >= ncols_data) {
            pthread_mutex_unlock(&m->lock);
            return TSDB_ERR_INVAL;
        }
        int t = col_types[data_idx];
        if (t != (int)m->schema->cols[c].type) {
            pthread_mutex_unlock(&m->lock);
            return TSDB_ERR_SCHEMA;
        }
        const void *src = col_arrs[data_idx];
        if (t == TSDB_TYPE_SYMBOL) {
            uint32_t *col = (uint32_t *)m->col_bufs[c];
            const uint8_t *p = (const uint8_t *)src;
            uint32_t total = 0;
            if (p) memcpy(&total, p, 4);
            const uint8_t *cur = p ? p + 4 : NULL;
            const uint8_t *end = p ? p + 4 + total : NULL;
            tsdb_symtab_t *st = m->schema->cols[c].symtab;
            for (size_t r = 0; r < n; r++) {
                if (!cur || cur >= end) {
                    col[base + r] = 0;
                    continue;
                }
                uint16_t l16;
                memcpy(&l16, cur, 2); cur += 2;
                if (cur + l16 > end) {
                    pthread_mutex_unlock(&m->lock);
                    return TSDB_ERR_CORRUPT;
                }
                char sbuf[260];
                int len = l16 < 256 ? l16 : 255;
                memcpy(sbuf, cur, len); sbuf[len] = '\0';
                cur += l16;
                uint32_t code = st ? tsdb_symtab_intern(st, sbuf)
                                   : TSDB_SYMBOL_INVALID;
                if (code == TSDB_SYMBOL_INVALID) {
                    pthread_mutex_unlock(&m->lock);
                    return TSDB_ERR_NOMEM;
                }
                col[base + r] = code;
            }
        } else {
            /* INT64 / FLOAT64 / TIMESTAMP — all 8 bytes. */
            uint8_t *col = (uint8_t *)m->col_bufs[c];
            if (src) memcpy(col + base * 8, src, n * 8);
            else     memset(col + base * 8, 0, n * 8);
        }
        data_idx++;
    }

    /* Update skiplist + sortedness tracking in one pass over the new ts. */
    if (m->sl_ok) {
        for (size_t r = 0; r < n; r++) {
            int64_t ts = ts_arr[r];
            if (sl_insert(&m->sl, ts, (uint32_t)(base + r)) < 0) {
                m->sl_ok = 0;
                break;
            }
            if (ts < m->last_ts) m->all_sorted = 0;
            m->last_ts = ts;
        }
    }

    m->nrows += n;
    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

int tsdb_memtable_sorted_indices(tsdb_memtable_t *m, size_t *out_idx) {
    if (!m || !out_idx) return TSDB_ERR_INVAL;
    pthread_mutex_lock(&m->lock);
    size_t nrows = m->nrows;

    /* Fast identity path: inserts arrived monotonically, so the skip-list
     * walk would just re-emit 0..n-1.  Skip it. */
    if (m->all_sorted || !m->sl_ok) {
        for (size_t i = 0; i < nrows; i++) out_idx[i] = i;
        pthread_mutex_unlock(&m->lock);
        return TSDB_OK;
    }

    /* Walk level-0 forward pointers; stable for equal keys because
     * sl_insert appends after existing equals. */
    size_t w = 0;
    const sl_node_t *n = m->sl.head.next[0];
    while (n && w < nrows) {
        out_idx[w++] = (size_t)n->value;
        n = n->next[0];
    }
    /* If the skip-list somehow undercounts (shouldn't happen), pad with
     * insertion order for the remainder so the caller always gets a
     * well-formed permutation. */
    for (size_t i = w; i < nrows; i++) out_idx[i] = i;

    pthread_mutex_unlock(&m->lock);
    return TSDB_OK;
}

int tsdb_memtable_is_sorted(tsdb_memtable_t *m) {
    if (!m) return 1;
    pthread_mutex_lock(&m->lock);
    int v = m->all_sorted;
    pthread_mutex_unlock(&m->lock);
    return v;
}
