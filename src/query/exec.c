/* exec.c — QTL query executor. */

#include "exec.h"
#include "parse.h"
#include "ast.h"
#include "result_internal.h"
#include "../storage/db.h"
#include "../storage/schema.h"
#include "../storage/memtable.h"
#include "../storage/part.h"
#include "../core/arena.h"
#include "../core/symbol.h"
#include "../exec/agg.h"
#include "../exec/filter.h"
#include "../exec/bucket.h"
#include "../exec/pool.h"
#include "../../include/tsdb.h"
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <ctype.h>
#include <math.h>
#include <pthread.h>

/* ---- Global parallel query pool --------------------------------------- */

static tsdb_pool_t          *g_query_pool  = NULL;
static pthread_once_t        g_pool_once   = PTHREAD_ONCE_INIT;
static volatile int          g_parallel_on = 1; /* toggleable */
static pthread_mutex_t       g_pool_lock   = PTHREAD_MUTEX_INITIALIZER;

static void init_pool(void) {
    tsdb_pool_new(0, &g_query_pool);
}

/* Public toggle: 0 = serial, non-zero = parallel. Thread-safe. */
void tsdb_set_query_parallel(int on) {
    g_parallel_on = on ? 1 : 0;
}

/* Resize the global query pool to n threads.
 * n <= 0 uses hardware concurrency.
 * Thread-safe; tears down and recreates the pool under a mutex. */
void tsdb_set_query_pool_size(int n) {
    pthread_mutex_lock(&g_pool_lock);
    if (g_query_pool) {
        tsdb_pool_free(g_query_pool);
        g_query_pool = NULL;
    }
    tsdb_pool_new(n, &g_query_pool);
    pthread_mutex_unlock(&g_pool_lock);
}

/* struct tsdb_result is defined in result_internal.h (shared with federation). */

/* Forward declarations */
static int exec_select(tsdb_db_t *db, qast_query_t *q, tsdb_result_t *r,
                       char *err, size_t errcap);

/* ---- small helpers ----------------------------------------------------- */

static void eset(char *err, size_t cap, const char *fmt, ...) {
    if (!err || !cap) return;
    va_list ap; va_start(ap, fmt);
    vsnprintf(err, cap, fmt, ap);
    va_end(ap);
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/* ---- Result helpers ---------------------------------------------------- */

static int result_reserve_cols(tsdb_result_t *r, int n) {
    r->ncols = n;
    r->col_names  = calloc((size_t)n, sizeof(char *));
    r->col_types  = calloc((size_t)n, sizeof(tsdb_type_t));
    r->col_symtab = calloc((size_t)n, sizeof(tsdb_symtab_t *));
    r->col_data   = calloc((size_t)n, sizeof(void *));
    if (!r->col_names || !r->col_types || !r->col_data || !r->col_symtab)
        return TSDB_ERR_NOMEM;
    return TSDB_OK;
}

static int result_reserve_rows(tsdb_result_t *r, size_t want) {
    if (want <= r->cap_rows) return TSDB_OK;
    size_t ncap = r->cap_rows ? r->cap_rows : 1024;
    while (ncap < want) ncap *= 2;
    for (int c = 0; c < r->ncols; c++) {
        size_t w = 8; /* all result types are 8 bytes (timestamp/int64/float64/sym→u32 promoted to u64) */
        void *np = realloc(r->col_data[c], w * ncap);
        if (!np) return TSDB_ERR_NOMEM;
        r->col_data[c] = np;
    }
    r->cap_rows = ncap;
    return TSDB_OK;
}

static void result_append_cell(tsdb_result_t *r, int col, uint64_t bits) {
    ((uint64_t *)r->col_data[col])[r->nrows] = bits;
}

/* Set column schema entry. */
static int result_set_col(tsdb_result_t *r, int i, const char *name, tsdb_type_t t,
                          tsdb_symtab_t *sym) {
    r->col_names[i] = strdup(name);
    r->col_types[i] = t;
    r->col_symtab[i] = sym;
    return r->col_names[i] ? TSDB_OK : TSDB_ERR_NOMEM;
}

/* ---- Scan state: list of blocks to read -------------------------------- */

typedef struct {
    /* Per source: either a memtable or a (part, block-index) */
    tsdb_memtable_t  *mem;             /* NULL if from disk */
    tsdb_part_t      *part;            /* NULL if from memtable */
    tsdb_block_meta_t meta;            /* only valid when part != NULL */
    size_t            row_count;       /* rows in this source segment */
    int64_t           ts_min, ts_max;
} scan_src_t;

typedef struct {
    scan_src_t *srcs;
    size_t      nsrcs;
    size_t      cap;
    tsdb_part_t **parts;
    size_t       nparts;
} scan_plan_t;

static void scan_plan_free(scan_plan_t *p) {
    for (size_t i = 0; i < p->nparts; i++) tsdb_part_close(p->parts[i]);
    free(p->parts);
    free(p->srcs);
}

static int scan_plan_push(scan_plan_t *p, scan_src_t s) {
    if (p->nsrcs >= p->cap) {
        size_t ncap = p->cap ? p->cap * 2 : 16;
        scan_src_t *ns = realloc(p->srcs, ncap * sizeof(*ns));
        if (!ns) return TSDB_ERR_NOMEM;
        p->srcs = ns; p->cap = ncap;
    }
    p->srcs[p->nsrcs++] = s;
    return TSDB_OK;
}

static int scan_plan_push_part(scan_plan_t *p, tsdb_part_t *part) {
    tsdb_part_t **np = realloc(p->parts, (p->nparts + 1) * sizeof(*np));
    if (!np) return TSDB_ERR_NOMEM;
    p->parts = np;
    p->parts[p->nparts++] = part;
    return TSDB_OK;
}

/* Collect every partition directory under a table dir, sorted ascending. */
static int list_partitions(const char *table_dir, char ***out, size_t *n_out) {
    DIR *d = opendir(table_dir);
    if (!d) { *out = NULL; *n_out = 0; return TSDB_OK; }
    struct dirent *ent;
    char **names = NULL;
    size_t n = 0, cap = 0;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] < '0' || ent->d_name[0] > '9') continue;
        if (strlen(ent->d_name) != 8) continue; /* YYYYMMDD */
        if (n + 1 > cap) {
            cap = cap ? cap * 2 : 8;
            char **nn = realloc(names, cap * sizeof(char *));
            if (!nn) { closedir(d); return TSDB_ERR_NOMEM; }
            names = nn;
        }
        char full[4096];
        snprintf(full, sizeof(full), "%s/%s", table_dir, ent->d_name);
        names[n++] = strdup(full);
    }
    closedir(d);
    /* sort */
    for (size_t i = 1; i < n; i++) {
        char *x = names[i]; size_t j = i;
        while (j > 0 && strcmp(names[j - 1], x) > 0) { names[j] = names[j - 1]; j--; }
        names[j] = x;
    }
    *out = names; *n_out = n;
    return TSDB_OK;
}

static int scan_plan_build(scan_plan_t *p, tsdb_table_internal_t *t) {
    memset(p, 0, sizeof(*p));

    tsdb_schema_t *s = tsdb_tbl_schema(t);
    const char *dir = tsdb_tbl_dir(t);
    int ts_col = s->ts_col_idx;

    /* Disk partitions first (older data). */
    char **dirs = NULL; size_t nd = 0;
    int rc = list_partitions(dir, &dirs, &nd);
    if (rc != TSDB_OK) return rc;
    for (size_t i = 0; i < nd; i++) {
        tsdb_part_t *part = NULL;
        rc = tsdb_part_open(s, dirs[i], &part);
        if (rc != TSDB_OK) { free(dirs[i]); continue; }
        tsdb_block_meta_t *metas = NULL; size_t nb = 0;
        rc = tsdb_part_col_blocks(part, ts_col, &metas, &nb);
        if (rc != TSDB_OK) { tsdb_part_close(part); free(dirs[i]); continue; }
        scan_plan_push_part(p, part);
        for (size_t b = 0; b < nb; b++) {
            scan_src_t sc = {0};
            sc.part = part;
            sc.meta = metas[b];
            sc.row_count = metas[b].count;
            sc.ts_min = metas[b].ts_min;
            sc.ts_max = metas[b].ts_max;
            scan_plan_push(p, sc);
        }
        free(metas);
        free(dirs[i]);
    }
    free(dirs);

    /* Then memtable (newest data). */
    tsdb_memtable_t *mem = tsdb_tbl_memtable(t);
    size_t nr = mem ? tsdb_memtable_rows(mem) : 0;
    if (nr > 0) {
        const int64_t *tscol = (const int64_t *)tsdb_memtable_col(mem, ts_col);
        scan_src_t sc = {0};
        sc.mem = mem;
        sc.row_count = nr;
        sc.ts_min = tscol[0];
        sc.ts_max = tscol[nr - 1];
        for (size_t i = 0; i < nr; i++) {
            if (tscol[i] < sc.ts_min) sc.ts_min = tscol[i];
            if (tscol[i] > sc.ts_max) sc.ts_max = tscol[i];
        }
        scan_plan_push(p, sc);
    }
    return TSDB_OK;
}

/* ---- Expr evaluation on vectors --------------------------------------- */

typedef struct {
    tsdb_schema_t *schema;
    /* For each column index, point to the decoded buffer for current block. */
    void        **col_bufs;       /* [ncols] */
    size_t        nrows;          /* rows in current block */
    tsdb_symtab_t **col_syms;     /* [ncols] symtab for SYMBOL cols */
    char          err[256];
    int           errored;
} eval_ctx_t;

/* Resolve identifier to a column index; -1 on error. */
static int resolve_col(tsdb_schema_t *s, const char *name) {
    return tsdb_schema_col_idx(s, name);
}

/* Evaluate a *scalar* constant expression (WHERE rhs). */
typedef enum { V_I64, V_F64, V_STR } vkind_t;
typedef struct { vkind_t k; int64_t i; double f; const char *s; } vval_t;

static int eval_const(qast_expr_t *e, vval_t *v) {
    switch (e->kind) {
    case QAST_LIT_INT:
    case QAST_LIT_TS:
        v->k = V_I64; v->i = (e->kind == QAST_LIT_TS) ? e->v.ts : e->v.i;
        return TSDB_OK;
    case QAST_LIT_FLOAT:
        v->k = V_F64; v->f = e->v.f;
        return TSDB_OK;
    case QAST_LIT_STR:
        v->k = V_STR; v->s = e->v.s;
        return TSDB_OK;
    case QAST_NEG: {
        vval_t inner;
        if (eval_const(e->lhs, &inner) != TSDB_OK) return TSDB_ERR_INVAL;
        if (inner.k == V_I64) { v->k = V_I64; v->i = -inner.i; return TSDB_OK; }
        if (inner.k == V_F64) { v->k = V_F64; v->f = -inner.f; return TSDB_OK; }
        return TSDB_ERR_INVAL;
    }
    default:
        return TSDB_ERR_INVAL;
    }
}

/* Apply WHERE bitmap to the current block's rows.
 * The bitmap has LSB-first layout over nrows bits.
 * Returns TSDB_OK on success. bm is caller-allocated and already set to all-1.
 */
static int apply_filter_expr(eval_ctx_t *ctx, qast_expr_t *e, uint64_t *bm) {
    if (!e) return TSDB_OK;

    /* AND / OR: recurse */
    if (e->kind == QAST_AND) {
        int rc = apply_filter_expr(ctx, e->lhs, bm);
        if (rc != TSDB_OK) return rc;
        return apply_filter_expr(ctx, e->rhs, bm);
    }
    if (e->kind == QAST_OR) {
        /* Compute two bitmaps, OR them into bm. */
        size_t nw = (ctx->nrows + 63) / 64;
        uint64_t *a = calloc(nw, sizeof(uint64_t));
        uint64_t *b = calloc(nw, sizeof(uint64_t));
        if (!a || !b) { free(a); free(b); return TSDB_ERR_NOMEM; }
        for (size_t i = 0; i < nw; i++) a[i] = ~(uint64_t)0;
        for (size_t i = 0; i < nw; i++) b[i] = ~(uint64_t)0;
        int rc1 = apply_filter_expr(ctx, e->lhs, a);
        int rc2 = apply_filter_expr(ctx, e->rhs, b);
        if (rc1 == TSDB_OK && rc2 == TSDB_OK) {
            for (size_t i = 0; i < nw; i++) bm[i] &= (a[i] | b[i]);
        }
        free(a); free(b);
        return (rc1 != TSDB_OK) ? rc1 : rc2;
    }
    if (e->kind == QAST_NOT) {
        size_t nw = (ctx->nrows + 63) / 64;
        uint64_t *tmp = calloc(nw, sizeof(uint64_t));
        if (!tmp) return TSDB_ERR_NOMEM;
        for (size_t i = 0; i < nw; i++) tmp[i] = ~(uint64_t)0;
        int rc = apply_filter_expr(ctx, e->lhs, tmp);
        if (rc == TSDB_OK) {
            /* clear bits in bm where tmp is set */
            for (size_t i = 0; i < nw; i++) bm[i] &= ~tmp[i];
            /* mask off tail beyond nrows */
            size_t tail = nw * 64 - ctx->nrows;
            if (tail) {
                uint64_t mask = (~(uint64_t)0) >> tail;
                bm[nw - 1] &= mask;
            }
        }
        free(tmp);
        return rc;
    }

    /* Comparisons */
    if (e->kind != QAST_EQ && e->kind != QAST_NE && e->kind != QAST_LT &&
        e->kind != QAST_LE && e->kind != QAST_GT && e->kind != QAST_GE) {
        eset(ctx->err, sizeof(ctx->err), "unsupported WHERE expr kind %d", e->kind);
        ctx->errored = 1;
        return TSDB_ERR_UNSUPPORTED;
    }

    tsdb_cmp_t op;
    switch (e->kind) {
    case QAST_EQ: op = TSDB_CMP_EQ; break;
    case QAST_NE: op = TSDB_CMP_NE; break;
    case QAST_LT: op = TSDB_CMP_LT; break;
    case QAST_LE: op = TSDB_CMP_LE; break;
    case QAST_GT: op = TSDB_CMP_GT; break;
    case QAST_GE: op = TSDB_CMP_GE; break;
    default: return TSDB_ERR_INTERNAL;
    }

    /* LHS must be an identifier. */
    if (e->lhs->kind != QAST_IDENT) {
        eset(ctx->err, sizeof(ctx->err), "WHERE lhs must be a column name");
        ctx->errored = 1; return TSDB_ERR_UNSUPPORTED;
    }
    int col = resolve_col(ctx->schema, e->lhs->v.s);
    if (col < 0) {
        eset(ctx->err, sizeof(ctx->err), "unknown column '%s'", e->lhs->v.s);
        ctx->errored = 1; return TSDB_ERR_SCHEMA;
    }

    vval_t rhs;
    if (eval_const(e->rhs, &rhs) != TSDB_OK) {
        eset(ctx->err, sizeof(ctx->err), "WHERE rhs must be a constant");
        ctx->errored = 1; return TSDB_ERR_UNSUPPORTED;
    }

    tsdb_type_t ct = ctx->schema->cols[col].type;

    /* Build a temp bitmap = all-1, apply, then AND into bm. */
    size_t nw = (ctx->nrows + 63) / 64;
    uint64_t *tmp = malloc(nw * sizeof(uint64_t));
    if (!tmp) return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < nw; i++) tmp[i] = ~(uint64_t)0;
    int rc = TSDB_OK;

    switch (ct) {
    case TSDB_TYPE_TIMESTAMP:
    case TSDB_TYPE_INT64: {
        int64_t v = (rhs.k == V_I64) ? rhs.i : (int64_t)rhs.f;
        rc = tsdb_filter_i64((const int64_t *)ctx->col_bufs[col], ctx->nrows, op, v, tmp);
        break;
    }
    case TSDB_TYPE_FLOAT64: {
        double v = (rhs.k == V_F64) ? rhs.f : (double)rhs.i;
        rc = tsdb_filter_f64((const double *)ctx->col_bufs[col], ctx->nrows, op, v, tmp);
        break;
    }
    case TSDB_TYPE_SYMBOL: {
        if (rhs.k != V_STR) { free(tmp); return TSDB_ERR_UNSUPPORTED; }
        uint32_t code = tsdb_symtab_lookup(ctx->col_syms[col], rhs.s);
        if (code == TSDB_SYMBOL_INVALID) {
            /* No match at all for EQ; empty bitmap. */
            memset(tmp, (op == TSDB_CMP_EQ) ? 0 : 0xFF, nw * sizeof(uint64_t));
            if (op != TSDB_CMP_EQ && op != TSDB_CMP_NE) rc = TSDB_ERR_UNSUPPORTED;
        } else {
            if (op == TSDB_CMP_EQ) {
                rc = tsdb_filter_u32_eq((const uint32_t *)ctx->col_bufs[col], ctx->nrows, code, tmp);
            } else if (op == TSDB_CMP_NE) {
                /* All - EQ(code) */
                uint64_t *eq = calloc(nw, sizeof(uint64_t));
                rc = tsdb_filter_u32_eq((const uint32_t *)ctx->col_bufs[col], ctx->nrows, code, eq);
                if (rc == TSDB_OK) {
                    for (size_t i = 0; i < nw; i++) tmp[i] = ~eq[i];
                }
                free(eq);
            } else rc = TSDB_ERR_UNSUPPORTED;
        }
        break;
    }
    default:
        rc = TSDB_ERR_UNSUPPORTED;
    }

    if (rc == TSDB_OK) {
        for (size_t i = 0; i < nw; i++) bm[i] &= tmp[i];
        /* Mask tail bits beyond nrows. */
        size_t tail = nw * 64 - ctx->nrows;
        if (tail) {
            uint64_t mask = (~(uint64_t)0) >> tail;
            bm[nw - 1] &= mask;
        }
    }
    free(tmp);
    return rc;
}

/* ---- Projection -------------------------------------------------------- */

/* A "projection" tells how to get values for an output column from a block. */
typedef enum { PROJ_COL, PROJ_TS_BUCKET, PROJ_AGG_SUM, PROJ_AGG_AVG, PROJ_AGG_MIN, PROJ_AGG_MAX, PROJ_AGG_COUNT } proj_kind_t;

typedef struct {
    proj_kind_t kind;
    int         col;                /* source column (for COL / AGG / TS_BUCKET) */
    tsdb_type_t out_type;
    char        name[128];
    /* For TS_BUCKET: */
    int64_t     bucket_ns;
    /* Running aggregate state (used only when whole-query aggregate, no SAMPLE BY) */
    double      agg_sum_f;
    int64_t     agg_sum_i;
    double      agg_min_f, agg_max_f;
    int64_t     agg_min_i, agg_max_i;
    uint64_t    agg_count;
} proj_t;

static int is_agg_call(qast_expr_t *e) {
    if (!e || e->kind != QAST_CALL) return 0;
    const char *n = e->v.s;
    return strcasecmp(n, "sum") == 0 || strcasecmp(n, "avg") == 0 ||
           strcasecmp(n, "min") == 0 || strcasecmp(n, "max") == 0 ||
           strcasecmp(n, "count") == 0;
}

static int build_projections(qast_query_t *q, tsdb_schema_t *s,
                             proj_t **out, int *out_n, int *out_has_agg,
                             char *err, size_t errcap) {
    proj_t *arr = NULL;
    int cap = 0, n = 0;

    int has_agg = 0;

    for (int i = 0; i < q->nsel; i++) {
        qast_sel_item_t *si = &q->sel[i];
        if (si->is_star) {
            /* expand to all columns */
            for (int c = 0; c < s->ncols; c++) {
                if (n + 1 > cap) { cap = cap ? cap * 2 : 8; arr = realloc(arr, cap * sizeof(*arr)); }
                memset(&arr[n], 0, sizeof(arr[n]));
                arr[n].kind = PROJ_COL;
                arr[n].col = c;
                arr[n].out_type = s->cols[c].type;
                snprintf(arr[n].name, sizeof(arr[n].name), "%s", s->cols[c].name);
                n++;
            }
            continue;
        }
        qast_expr_t *e = si->expr;
        if (n + 1 > cap) { cap = cap ? cap * 2 : 8; arr = realloc(arr, cap * sizeof(*arr)); }
        memset(&arr[n], 0, sizeof(arr[n]));

        if (e->kind == QAST_IDENT) {
            int c = resolve_col(s, e->v.s);
            if (c < 0) { eset(err, errcap, "unknown column '%s'", e->v.s); free(arr); return TSDB_ERR_SCHEMA; }
            arr[n].kind = PROJ_COL;
            arr[n].col = c;
            arr[n].out_type = s->cols[c].type;
            snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias ? si->alias : s->cols[c].name);
        } else if (is_agg_call(e)) {
            has_agg = 1;
            const char *name = e->v.s;
            proj_kind_t k = PROJ_AGG_SUM;
            if      (strcasecmp(name, "sum") == 0) k = PROJ_AGG_SUM;
            else if (strcasecmp(name, "avg") == 0) k = PROJ_AGG_AVG;
            else if (strcasecmp(name, "min") == 0) k = PROJ_AGG_MIN;
            else if (strcasecmp(name, "max") == 0) k = PROJ_AGG_MAX;
            else if (strcasecmp(name, "count") == 0) k = PROJ_AGG_COUNT;
            arr[n].kind = k;
            if (e->nargs == 1 && e->args[0]->kind == QAST_IDENT) {
                int c = resolve_col(s, e->args[0]->v.s);
                if (c < 0 && k != PROJ_AGG_COUNT) {
                    eset(err, errcap, "unknown column in aggregate: %s", e->args[0]->v.s);
                    free(arr); return TSDB_ERR_SCHEMA;
                }
                arr[n].col = c;
            } else if (k == PROJ_AGG_COUNT && (e->nargs == 0 || (e->nargs == 1 && e->args[0]->kind == QAST_STAR))) {
                arr[n].col = s->ts_col_idx; /* count uses row count */
            } else {
                eset(err, errcap, "unsupported aggregate argument");
                free(arr); return TSDB_ERR_UNSUPPORTED;
            }
            /* COUNT always int64; AVG always float64; SUM/MIN/MAX take source type */
            if (k == PROJ_AGG_COUNT) arr[n].out_type = TSDB_TYPE_INT64;
            else if (k == PROJ_AGG_AVG) arr[n].out_type = TSDB_TYPE_FLOAT64;
            else arr[n].out_type = (arr[n].col >= 0) ? s->cols[arr[n].col].type : TSDB_TYPE_FLOAT64;
            if (si->alias) snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias);
            else {
                if (arr[n].col >= 0)
                    snprintf(arr[n].name, sizeof(arr[n].name), "%s(%s)", name, s->cols[arr[n].col].name);
                else
                    snprintf(arr[n].name, sizeof(arr[n].name), "%s()", name);
            }
            arr[n].agg_min_f =  INFINITY;
            arr[n].agg_max_f = -INFINITY;
            arr[n].agg_min_i = INT64_MAX;
            arr[n].agg_max_i = INT64_MIN;
        } else if (e->kind == QAST_CALL && strcasecmp(e->v.s, "time_bucket") == 0) {
            /* time_bucket(ts, interval) */
            if (e->nargs != 2 || e->args[0]->kind != QAST_IDENT || e->args[1]->kind != QAST_LIT_INT) {
                eset(err, errcap, "time_bucket requires (col, interval)");
                free(arr); return TSDB_ERR_PARSE;
            }
            int c = resolve_col(s, e->args[0]->v.s);
            if (c < 0) { eset(err, errcap, "unknown column '%s'", e->args[0]->v.s); free(arr); return TSDB_ERR_SCHEMA; }
            arr[n].kind = PROJ_TS_BUCKET;
            arr[n].col = c;
            arr[n].bucket_ns = e->args[1]->v.i;
            arr[n].out_type = TSDB_TYPE_TIMESTAMP;
            if (si->alias) snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias);
            else snprintf(arr[n].name, sizeof(arr[n].name), "time_bucket(%s)", s->cols[c].name);
        } else {
            eset(err, errcap, "unsupported SELECT expression kind %d", e->kind);
            free(arr); return TSDB_ERR_UNSUPPORTED;
        }
        n++;
    }
    *out = arr; *out_n = n; *out_has_agg = has_agg;
    return TSDB_OK;
}

/* ---- Aggregation helpers over a block --------------------------------- */

static void agg_update(proj_t *p, tsdb_schema_t *s, void **bufs, size_t n, const uint64_t *bm) {
    /* Iterate set bits of bm. */
    tsdb_type_t t = (p->col >= 0) ? s->cols[p->col].type : TSDB_TYPE_INT64;

    if (p->kind == PROJ_AGG_COUNT) {
        uint64_t c = tsdb_bitmap_popcount(bm, n);
        p->agg_count += c;
        return;
    }

    if (t == TSDB_TYPE_FLOAT64) {
        const double *v = (const double *)bufs[p->col];
        for (size_t i = 0; i < n; i++) {
            if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
            double x = v[i];
            if (p->kind == PROJ_AGG_SUM || p->kind == PROJ_AGG_AVG) p->agg_sum_f += x;
            if (p->kind == PROJ_AGG_MIN && x < p->agg_min_f) p->agg_min_f = x;
            if (p->kind == PROJ_AGG_MAX && x > p->agg_max_f) p->agg_max_f = x;
            p->agg_count++;
        }
    } else if (t == TSDB_TYPE_INT64 || t == TSDB_TYPE_TIMESTAMP) {
        const int64_t *v = (const int64_t *)bufs[p->col];
        for (size_t i = 0; i < n; i++) {
            if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
            int64_t x = v[i];
            if (p->kind == PROJ_AGG_SUM || p->kind == PROJ_AGG_AVG) p->agg_sum_i += x;
            if (p->kind == PROJ_AGG_MIN && x < p->agg_min_i) p->agg_min_i = x;
            if (p->kind == PROJ_AGG_MAX && x > p->agg_max_i) p->agg_max_i = x;
            p->agg_count++;
        }
    }
}

static void agg_write(proj_t *p, tsdb_schema_t *s, tsdb_result_t *r, int col_idx) {
    tsdb_type_t src_t = (p->col >= 0) ? s->cols[p->col].type : TSDB_TYPE_INT64;
    double out_f = 0.0;
    int64_t out_i = 0;
    int is_int = (p->out_type == TSDB_TYPE_INT64 || p->out_type == TSDB_TYPE_TIMESTAMP);

    switch (p->kind) {
    case PROJ_AGG_COUNT: out_i = (int64_t)p->agg_count; break;
    case PROJ_AGG_SUM:
        if (src_t == TSDB_TYPE_FLOAT64) out_f = p->agg_sum_f;
        else                            out_i = p->agg_sum_i;
        break;
    case PROJ_AGG_AVG:
        if (p->agg_count == 0) out_f = 0.0 / 0.0;
        else if (src_t == TSDB_TYPE_FLOAT64) out_f = p->agg_sum_f / (double)p->agg_count;
        else out_f = (double)p->agg_sum_i / (double)p->agg_count;
        break;
    case PROJ_AGG_MIN:
        if (src_t == TSDB_TYPE_FLOAT64) out_f = p->agg_min_f;
        else                            out_i = p->agg_min_i;
        break;
    case PROJ_AGG_MAX:
        if (src_t == TSDB_TYPE_FLOAT64) out_f = p->agg_max_f;
        else                            out_i = p->agg_max_i;
        break;
    default: return;
    }

    uint64_t bits;
    if (is_int) memcpy(&bits, &out_i, 8);
    else        memcpy(&bits, &out_f, 8);
    ((uint64_t *)r->col_data[col_idx])[r->nrows] = bits;
}

/* ---- Parallel scan task ----------------------------------------------- */

typedef struct {
    /* Input (read-only from worker). */
    scan_src_t    *srcs;
    size_t         nsrcs;
    tsdb_schema_t *schema;
    qast_expr_t   *where;
    int            need_col[TSDB_MAX_COLS];

    /* Private projection state (worker writes). */
    proj_t        *projs;
    int            nprojs;

    /* Error output. */
    int            rc;
    char           err[256];
} par_task_t;

/* Worker function: scan the assigned sources and accumulate into private projs[]. */
void tsdb_par_scan_task(void *arg) {
    par_task_t *t = (par_task_t *)arg;
    t->rc = TSDB_OK;

    for (size_t si = 0; si < t->nsrcs; si++) {
        scan_src_t *src = &t->srcs[si];
        size_t n = src->row_count;

        void **bufs = calloc((size_t)t->schema->ncols, sizeof(void *));
        if (!bufs) { t->rc = TSDB_ERR_NOMEM; return; }
        tsdb_symtab_t **syms = calloc((size_t)t->schema->ncols, sizeof(tsdb_symtab_t *));
        if (!syms) { free(bufs); t->rc = TSDB_ERR_NOMEM; return; }

        int load_rc = TSDB_OK;
        for (int c = 0; c < t->schema->ncols; c++) {
            if (!t->need_col[c]) continue;
            syms[c] = t->schema->cols[c].symtab;
            size_t w = tsdb_type_width(t->schema->cols[c].type);
            if (src->mem) {
                bufs[c] = (void *)tsdb_memtable_col(src->mem, c);
            } else {
                bufs[c] = malloc(w * n);
                if (!bufs[c]) { load_rc = TSDB_ERR_NOMEM; break; }
                tsdb_block_meta_t *metas = NULL; size_t nb = 0;
                load_rc = tsdb_part_col_blocks(src->part, c, &metas, &nb);
                if (load_rc != TSDB_OK) break;
                tsdb_block_meta_t *hit = NULL;
                for (size_t b = 0; b < nb; b++) {
                    if (metas[b].ts_min == src->meta.ts_min && metas[b].count == src->meta.count) {
                        hit = &metas[b]; break;
                    }
                }
                if (!hit) { free(metas); load_rc = TSDB_ERR_CORRUPT; break; }
                load_rc = tsdb_part_read_block(src->part, c, hit, bufs[c]);
                free(metas);
                if (load_rc != TSDB_OK) break;
            }
        }
        if (load_rc != TSDB_OK) {
            for (int c = 0; c < t->schema->ncols; c++)
                if (!src->mem && bufs[c]) free(bufs[c]);
            free(bufs); free(syms);
            t->rc = load_rc;
            return;
        }

        /* Build bitmap. */
        size_t nw = (n + 63) / 64;
        uint64_t *bm = malloc(nw * sizeof(uint64_t));
        if (!bm) {
            for (int c = 0; c < t->schema->ncols; c++)
                if (!src->mem && bufs[c]) free(bufs[c]);
            free(bufs); free(syms);
            t->rc = TSDB_ERR_NOMEM;
            return;
        }
        for (size_t i = 0; i < nw; i++) bm[i] = ~(uint64_t)0;
        size_t tail = nw * 64 - n;
        if (tail) { uint64_t mask = (~(uint64_t)0) >> tail; bm[nw - 1] &= mask; }

        if (t->where) {
            eval_ctx_t ctx = {0};
            ctx.schema = t->schema;
            ctx.col_bufs = bufs;
            ctx.col_syms = syms;
            ctx.nrows = n;
            int frc = apply_filter_expr(&ctx, t->where, bm);
            if (frc != TSDB_OK) {
                if (ctx.err[0]) snprintf(t->err, sizeof(t->err), "%s", ctx.err);
                free(bm);
                for (int c = 0; c < t->schema->ncols; c++)
                    if (!src->mem && bufs[c]) free(bufs[c]);
                free(bufs); free(syms);
                t->rc = frc;
                return;
            }
        }

        /* Update private aggregate state. */
        for (int pi = 0; pi < t->nprojs; pi++) {
            if (t->projs[pi].kind >= PROJ_AGG_SUM && t->projs[pi].kind <= PROJ_AGG_COUNT)
                agg_update(&t->projs[pi], t->schema, bufs, n, bm);
        }

        free(bm);
        for (int c = 0; c < t->schema->ncols; c++)
            if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
    }
}

/* ---- Main select execution ------------------------------------------- */

static int exec_select(tsdb_db_t *db, qast_query_t *q, tsdb_result_t *r,
                       char *err, size_t errcap) {
    tsdb_table_internal_t *tbl = tsdb_db_find_table(db, q->from);
    if (!tbl) {
        /* Try open */
        tsdb_table_t *h = NULL;
        int rc = tsdb_open_table(db, q->from, &h);
        if (rc != TSDB_OK) { eset(err, errcap, "table '%s' not found", q->from); return rc; }
        tbl = tsdb_db_find_table(db, q->from);
        if (!tbl) { eset(err, errcap, "table '%s' not found", q->from); return TSDB_ERR_NOTFOUND; }
    }
    tsdb_schema_t *s = tsdb_tbl_schema(tbl);

    /* Build projections. */
    proj_t *projs = NULL; int nprojs = 0, has_agg = 0;
    int rc = build_projections(q, s, &projs, &nprojs, &has_agg, err, errcap);
    if (rc != TSDB_OK) return rc;

    /* Allocate result columns. */
    rc = result_reserve_cols(r, nprojs);
    if (rc != TSDB_OK) { free(projs); return rc; }
    for (int i = 0; i < nprojs; i++) {
        tsdb_symtab_t *sym = (projs[i].kind == PROJ_COL &&
                              s->cols[projs[i].col].type == TSDB_TYPE_SYMBOL)
                              ? s->cols[projs[i].col].symtab : NULL;
        rc = result_set_col(r, i, projs[i].name, projs[i].out_type, sym);
        if (rc != TSDB_OK) { free(projs); return rc; }
    }

    /* Build scan plan */
    scan_plan_t plan = {0};
    rc = scan_plan_build(&plan, tbl);
    if (rc != TSDB_OK) { free(projs); return rc; }

    /* Special case: SAMPLE BY + has_agg: handle bucket-grouped aggregation. */
    /* For MVP, we don't implement bucket output here; fallback to row-per-bucket
     * via an in-memory bucket map if SAMPLE BY + aggregate; otherwise simple scan. */

    int has_sample = q->has_sample_by;

    /* Bucket output state: only used when has_sample && has_agg */
    typedef struct { int64_t bucket; double sum_f; int64_t sum_i; double min_f, max_f;
                     int64_t min_i, max_i; uint64_t count; } bkt_state_t;
    bkt_state_t *bkts = NULL; size_t nbkt = 0, bkt_cap = 0;
    int64_t cur_bucket = INT64_MIN;

    size_t rows_emitted = 0;
    size_t limit = q->has_limit ? (size_t)q->limit : SIZE_MAX;

    /* ---- Parallel aggregate path --------------------------------------- */
    /* Conditions: agg query, no SAMPLE BY, more than 1 source, parallel enabled.
     * Each worker scans its slice of sources with a private proj_t copy,
     * then main thread merges all partial states. */
    pthread_once(&g_pool_once, init_pool);

    int use_parallel = has_agg && !has_sample && plan.nsrcs > 1
                       && g_parallel_on && g_query_pool != NULL
                       && tsdb_pool_size(g_query_pool) > 1;

    /* Also check TSDB_QUERY_PARALLEL env var (0 = force serial). */
    if (use_parallel) {
        const char *env = getenv("TSDB_QUERY_PARALLEL");
        if (env && env[0] == '0') use_parallel = 0;
    }

    if (use_parallel) {
        int nw = tsdb_pool_size(g_query_pool);

        /* Determine the columns needed (same logic as serial path). */
        int need_col[TSDB_MAX_COLS];
        memset(need_col, 0, sizeof(need_col));
        for (int i = 0; i < nprojs; i++) if (projs[i].col >= 0) need_col[projs[i].col] = 1;
        {
            qast_expr_t *stk[128]; int tp = 0;
            if (q->where) stk[tp++] = q->where;
            while (tp > 0) {
                qast_expr_t *e = stk[--tp];
                if (!e) continue;
                if (e->kind == QAST_IDENT) {
                    int c = resolve_col(s, e->v.s);
                    if (c >= 0) need_col[c] = 1;
                }
                if (e->lhs && tp < 128) stk[tp++] = e->lhs;
                if (e->rhs && tp < 128) stk[tp++] = e->rhs;
                for (int i = 0; i < e->nargs && tp < 128; i++) stk[tp++] = e->args[i];
            }
        }

        /* Per-worker task argument. */
        par_task_t *tasks = calloc((size_t)nw, sizeof(par_task_t));
        if (!tasks) { rc = TSDB_ERR_NOMEM; goto done; }

        /* Distribute sources into contiguous slices, one per worker. */
        size_t nsrcs  = plan.nsrcs;
        size_t slice  = (nsrcs + (size_t)nw - 1) / (size_t)nw;

        int nactive = 0;
        for (int w = 0; w < nw; w++) {
            size_t start = (size_t)w * slice;
            if (start >= nsrcs) break;
            size_t end = start + slice;
            if (end > nsrcs) end = nsrcs;

            par_task_t *t = &tasks[w];
            t->srcs   = &plan.srcs[start];
            t->nsrcs  = end - start;
            t->schema = s;
            t->where  = q->where;
            memcpy(t->need_col, need_col, sizeof(need_col));
            t->nprojs = nprojs;
            t->projs  = malloc((size_t)nprojs * sizeof(proj_t));
            if (!t->projs) {
                for (int j = 0; j < w; j++) free(tasks[j].projs);
                free(tasks);
                rc = TSDB_ERR_NOMEM;
                goto done;
            }
            memcpy(t->projs, projs, (size_t)nprojs * sizeof(proj_t));
            nactive++;
        }

        for (int w = 0; w < nactive; w++)
            tsdb_pool_submit(g_query_pool, tsdb_par_scan_task, &tasks[w]);

        tsdb_pool_wait(g_query_pool);

        /* Check for errors in any worker. */
        for (int w = 0; w < nactive; w++) {
            if (tasks[w].rc != TSDB_OK && rc == TSDB_OK) {
                rc = tasks[w].rc;
                if (tasks[w].err[0] && err && errcap)
                    snprintf(err, errcap, "%s", tasks[w].err);
            }
        }

        if (rc == TSDB_OK) {
            /* Merge per-worker partial states into master projs[]. */
            for (int w = 0; w < nactive; w++) {
                proj_t *wp = tasks[w].projs;
                for (int pi = 0; pi < nprojs; pi++) {
                    proj_t *mp = &projs[pi];
                    proj_t *pp = &wp[pi];
                    switch (mp->kind) {
                    case PROJ_AGG_COUNT:
                        mp->agg_count += pp->agg_count;
                        break;
                    case PROJ_AGG_SUM:
                        mp->agg_sum_f += pp->agg_sum_f;
                        mp->agg_sum_i += pp->agg_sum_i;
                        mp->agg_count += pp->agg_count;
                        break;
                    case PROJ_AGG_AVG:
                        /* Merge sum+count; agg_write divides at end. */
                        mp->agg_sum_f += pp->agg_sum_f;
                        mp->agg_sum_i += pp->agg_sum_i;
                        mp->agg_count += pp->agg_count;
                        break;
                    case PROJ_AGG_MIN:
                        if (pp->agg_min_f < mp->agg_min_f) mp->agg_min_f = pp->agg_min_f;
                        if (pp->agg_min_i < mp->agg_min_i) mp->agg_min_i = pp->agg_min_i;
                        break;
                    case PROJ_AGG_MAX:
                        if (pp->agg_max_f > mp->agg_max_f) mp->agg_max_f = pp->agg_max_f;
                        if (pp->agg_max_i > mp->agg_max_i) mp->agg_max_i = pp->agg_max_i;
                        break;
                    default:
                        break;
                    }
                }
                free(wp);
            }
            free(tasks);

            /* Write merged aggregates to result. */
            rc = result_reserve_rows(r, 1);
            if (rc == TSDB_OK) {
                for (int pi = 0; pi < nprojs; pi++) agg_write(&projs[pi], s, r, pi);
                r->nrows = 1;
            }

            free(projs);
            scan_plan_free(&plan);
            return rc;
        }

        /* Error path: free worker data. */
        for (int w = 0; w < nactive; w++) free(tasks[w].projs);
        free(tasks);
        free(projs);
        scan_plan_free(&plan);
        return rc;
    }
    /* ---- End parallel path ---------------------------------------------- */

    /* Iterate sources. */
    for (size_t si = 0; si < plan.nsrcs && rows_emitted < limit; si++) {
        scan_src_t *src = &plan.srcs[si];
        size_t n = src->row_count;

        /* Allocate per-column decode buffers for this source. */
        void **bufs = calloc((size_t)s->ncols, sizeof(void *));
        if (!bufs) { rc = TSDB_ERR_NOMEM; goto done; }
        tsdb_symtab_t **syms = calloc((size_t)s->ncols, sizeof(tsdb_symtab_t *));
        if (!syms) { free(bufs); rc = TSDB_ERR_NOMEM; goto done; }

        int need_col[TSDB_MAX_COLS] = {0};
        /* Columns needed: from projections + from WHERE */
        for (int i = 0; i < nprojs; i++) if (projs[i].col >= 0) need_col[projs[i].col] = 1;
        /* Scan WHERE expression for idents */
        /* Simple: just load all referenced columns; recursion */
        /* For WHERE, we call a helper that marks column bits. */
        {
            qast_expr_t *stk[128]; int tp = 0;
            if (q->where) stk[tp++] = q->where;
            while (tp > 0) {
                qast_expr_t *e = stk[--tp];
                if (!e) continue;
                if (e->kind == QAST_IDENT) {
                    int c = resolve_col(s, e->v.s);
                    if (c >= 0) need_col[c] = 1;
                }
                if (e->lhs && tp < 128) stk[tp++] = e->lhs;
                if (e->rhs && tp < 128) stk[tp++] = e->rhs;
                for (int i = 0; i < e->nargs && tp < 128; i++) stk[tp++] = e->args[i];
            }
        }

        /* Always load ts col if SAMPLE BY */
        if (has_sample) need_col[s->ts_col_idx] = 1;

        for (int c = 0; c < s->ncols; c++) {
            if (!need_col[c]) continue;
            syms[c] = s->cols[c].symtab;
            size_t w = tsdb_type_width(s->cols[c].type);
            if (src->mem) {
                bufs[c] = (void *)tsdb_memtable_col(src->mem, c);
            } else {
                bufs[c] = malloc(w * n);
                if (!bufs[c]) { rc = TSDB_ERR_NOMEM; break; }
                /* Find this column's block at same index. We stored ts blocks;
                 * assume columns have aligned block counts (our flush does that). */
                tsdb_block_meta_t *metas = NULL; size_t nb = 0;
                rc = tsdb_part_col_blocks(src->part, c, &metas, &nb);
                if (rc != TSDB_OK) break;
                /* match by ts_min == src->meta.ts_min */
                tsdb_block_meta_t *hit = NULL;
                for (size_t b = 0; b < nb; b++) {
                    if (metas[b].ts_min == src->meta.ts_min && metas[b].count == src->meta.count) {
                        hit = &metas[b]; break;
                    }
                }
                if (!hit) { free(metas); rc = TSDB_ERR_CORRUPT; break; }
                rc = tsdb_part_read_block(src->part, c, hit, bufs[c]);
                free(metas);
                if (rc != TSDB_OK) break;
            }
        }
        if (rc != TSDB_OK) {
            for (int c = 0; c < s->ncols; c++)
                if (!src->mem && bufs[c]) free(bufs[c]);
            free(bufs); free(syms); goto done;
        }

        /* Build bitmap: initially all-1 over n rows, then AND filter. */
        size_t nw = (n + 63) / 64;
        uint64_t *bm = malloc(nw * sizeof(uint64_t));
        if (!bm) { for (int c = 0; c < s->ncols; c++) if (!src->mem && bufs[c]) free(bufs[c]); free(bufs); free(syms); rc = TSDB_ERR_NOMEM; goto done; }
        for (size_t i = 0; i < nw; i++) bm[i] = ~(uint64_t)0;
        size_t tail = nw * 64 - n;
        if (tail) {
            uint64_t mask = (~(uint64_t)0) >> tail;
            bm[nw - 1] &= mask;
        }

        if (q->where) {
            eval_ctx_t ctx = {0};
            ctx.schema = s;
            ctx.col_bufs = bufs;
            ctx.col_syms = syms;
            ctx.nrows = n;
            rc = apply_filter_expr(&ctx, q->where, bm);
            if (rc != TSDB_OK) {
                if (ctx.err[0]) eset(err, errcap, "%s", ctx.err);
                free(bm);
                for (int c = 0; c < s->ncols; c++) if (!src->mem && bufs[c]) free(bufs[c]);
                free(bufs); free(syms); goto done;
            }
        }

        /* Execute projections */
        if (has_agg && !has_sample) {
            /* Single-row aggregate over all rows matching. */
            for (int pi = 0; pi < nprojs; pi++) {
                if (projs[pi].kind >= PROJ_AGG_SUM && projs[pi].kind <= PROJ_AGG_COUNT)
                    agg_update(&projs[pi], s, bufs, n, bm);
            }
        } else if (has_agg && has_sample) {
            /* Bucket aggregation over the ts column (int64). */
            const int64_t *tscol = (const int64_t *)bufs[s->ts_col_idx];
            int64_t bnum = q->sample_by.ns;
            if (bnum <= 0) bnum = 1;
            for (size_t i = 0; i < n; i++) {
                if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
                int64_t b = tscol[i] - (tscol[i] % bnum);
                if (b != cur_bucket) {
                    /* flush prior bucket to result */
                    if (nbkt > 0 && bkts[nbkt - 1].count > 0) {
                        /* already in bkts, nothing to do */
                    }
                    /* start new bucket row */
                    if (nbkt >= bkt_cap) {
                        bkt_cap = bkt_cap ? bkt_cap * 2 : 64;
                        bkts = realloc(bkts, bkt_cap * sizeof(bkt_state_t));
                    }
                    memset(&bkts[nbkt], 0, sizeof(bkt_state_t));
                    bkts[nbkt].min_f =  INFINITY;
                    bkts[nbkt].max_f = -INFINITY;
                    bkts[nbkt].min_i = INT64_MAX;
                    bkts[nbkt].max_i = INT64_MIN;
                    bkts[nbkt].bucket = b;
                    nbkt++;
                    cur_bucket = b;
                }
                /* Update this bucket with each projection that needs it */
                for (int pi = 0; pi < nprojs; pi++) {
                    if (projs[pi].kind < PROJ_AGG_SUM || projs[pi].kind > PROJ_AGG_COUNT) continue;
                    int col = projs[pi].col;
                    if (projs[pi].kind == PROJ_AGG_COUNT) { bkts[nbkt - 1].count++; continue; }
                    if (col < 0) continue;
                    tsdb_type_t ct = s->cols[col].type;
                    if (ct == TSDB_TYPE_FLOAT64) {
                        double x = ((const double *)bufs[col])[i];
                        bkts[nbkt - 1].sum_f += x;
                        if (x < bkts[nbkt - 1].min_f) bkts[nbkt - 1].min_f = x;
                        if (x > bkts[nbkt - 1].max_f) bkts[nbkt - 1].max_f = x;
                    } else {
                        int64_t x = ((const int64_t *)bufs[col])[i];
                        bkts[nbkt - 1].sum_i += x;
                        if (x < bkts[nbkt - 1].min_i) bkts[nbkt - 1].min_i = x;
                        if (x > bkts[nbkt - 1].max_i) bkts[nbkt - 1].max_i = x;
                    }
                    bkts[nbkt - 1].count++;
                }
            }
        } else {
            /* Simple row projection. */
            for (size_t i = 0; i < n; i++) {
                if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
                if (rows_emitted >= limit) break;

                result_reserve_rows(r, r->nrows + 1);

                for (int pi = 0; pi < nprojs; pi++) {
                    proj_t *p = &projs[pi];
                    if (p->kind == PROJ_COL) {
                        size_t w = tsdb_type_width(s->cols[p->col].type);
                        uint64_t bits = 0;
                        if (w == 8) bits = ((uint64_t *)bufs[p->col])[i];
                        else if (w == 4) bits = ((uint32_t *)bufs[p->col])[i];
                        result_append_cell(r, pi, bits);
                    } else if (p->kind == PROJ_TS_BUCKET) {
                        int64_t ts = ((int64_t *)bufs[p->col])[i];
                        int64_t b = ts - (ts % p->bucket_ns);
                        uint64_t bits;
                        memcpy(&bits, &b, 8);
                        result_append_cell(r, pi, bits);
                    }
                }
                r->nrows++;
                rows_emitted++;
            }
        }

        free(bm);
        for (int c = 0; c < s->ncols; c++)
            if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
    }

    /* Write aggregates or bucket output. */
    if (has_agg && !has_sample) {
        rc = result_reserve_rows(r, 1);
        if (rc != TSDB_OK) goto done;
        for (int pi = 0; pi < nprojs; pi++) agg_write(&projs[pi], s, r, pi);
        r->nrows = 1;
    } else if (has_agg && has_sample) {
        rc = result_reserve_rows(r, nbkt);
        if (rc != TSDB_OK) { free(bkts); goto done; }
        for (size_t bi = 0; bi < nbkt && r->nrows < limit; bi++) {
            for (int pi = 0; pi < nprojs; pi++) {
                proj_t *p = &projs[pi];
                if (p->kind == PROJ_TS_BUCKET) {
                    uint64_t bits; memcpy(&bits, &bkts[bi].bucket, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_SUM) {
                    double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                               ? bkts[bi].sum_f : (double)bkts[bi].sum_i;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_AVG) {
                    double v = 0;
                    if (bkts[bi].count > 0) {
                        v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                            ? (bkts[bi].sum_f / bkts[bi].count)
                            : ((double)bkts[bi].sum_i / bkts[bi].count);
                    }
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_MIN) {
                    double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                               ? bkts[bi].min_f : (double)bkts[bi].min_i;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_MAX) {
                    double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                               ? bkts[bi].max_f : (double)bkts[bi].max_i;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_COUNT) {
                    int64_t v = (int64_t)bkts[bi].count;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_COL) {
                    /* Not supported in bucket mode, write 0 */
                    result_append_cell(r, pi, 0);
                }
            }
            r->nrows++;
        }
    }

    (void)cmp_u64;
done:
    free(projs);
    free(bkts);
    scan_plan_free(&plan);
    return rc;
}

/* ---- Public API implementations --------------------------------------- */

int tsdb_query(tsdb_db_t *db, const char *qtl, tsdb_result_t **out) {
    if (!db || !qtl || !out) return TSDB_ERR_INVAL;

    tsdb_arena_t a; tsdb_arena_init(&a, 16 * 1024);
    qast_query_t q;
    char err[256];
    int rc = qparse(qtl, &a, &q, err, sizeof(err));
    if (rc != TSDB_OK) {
        tsdb_arena_free(&a);
        return rc;
    }

    tsdb_result_t *r = calloc(1, sizeof(*r));
    if (!r) { tsdb_arena_free(&a); return TSDB_ERR_NOMEM; }
    r->cur = -1;

    rc = exec_select(db, &q, r, err, sizeof(err));
    tsdb_arena_free(&a);
    if (rc != TSDB_OK) {
        tsdb_result_free(r);
        return rc;
    }
    *out = r;
    return TSDB_OK;
}

void tsdb_result_free(tsdb_result_t *r) {
    if (!r) return;
    for (int i = 0; i < r->ncols; i++) {
        free(r->col_names[i]);
        free(r->col_data[i]);
    }
    free(r->col_names);
    free(r->col_types);
    free(r->col_symtab);
    free(r->col_data);
    free(r);
}

int tsdb_result_ncols(tsdb_result_t *r) { return r ? r->ncols : 0; }

const char *tsdb_result_col_name(tsdb_result_t *r, int i) {
    if (!r || i < 0 || i >= r->ncols) return NULL;
    return r->col_names[i];
}

tsdb_type_t tsdb_result_col_type(tsdb_result_t *r, int i) {
    if (!r || i < 0 || i >= r->ncols) return 0;
    return r->col_types[i];
}

int tsdb_result_next(tsdb_result_t *r) {
    if (!r) return 0;
    r->cur++;
    return ((size_t)r->cur < r->nrows) ? 1 : 0;
}

tsdb_ts_t tsdb_result_ts(tsdb_result_t *r, int col) {
    if (!r || col < 0 || col >= r->ncols || r->cur < 0) return 0;
    int64_t v;
    memcpy(&v, &((uint64_t *)r->col_data[col])[r->cur], 8);
    return v;
}

int64_t tsdb_result_i64(tsdb_result_t *r, int col) {
    if (!r || col < 0 || col >= r->ncols || r->cur < 0) return 0;
    int64_t v;
    memcpy(&v, &((uint64_t *)r->col_data[col])[r->cur], 8);
    return v;
}

double tsdb_result_f64(tsdb_result_t *r, int col) {
    if (!r || col < 0 || col >= r->ncols || r->cur < 0) return 0.0;
    double v;
    memcpy(&v, &((uint64_t *)r->col_data[col])[r->cur], 8);
    return v;
}

const char *tsdb_result_sym(tsdb_result_t *r, int col) {
    if (!r || col < 0 || col >= r->ncols || r->cur < 0) return NULL;
    uint32_t code = (uint32_t)((uint64_t *)r->col_data[col])[r->cur];
    if (!r->col_symtab[col]) return NULL;
    return tsdb_symtab_str(r->col_symtab[col], code);
}

bool tsdb_result_is_null(tsdb_result_t *r, int col) {
    (void)r; (void)col; return false; /* Nulls not yet tracked */
}
