/* exec.c — QTL query executor. */

#include "exec.h"
#include "parse.h"
#include "ast.h"
#include "result_internal.h"
#include "../storage/db.h"
#include "../storage/schema.h"
#include "../storage/memtable.h"
#include "../storage/part.h"
#include "../storage/parquet.h"
#include "../core/arena.h"
#include "../core/symbol.h"
#include "../core/bits.h"
#include "../compress/codec.h"
#include "../exec/agg.h"
#include "../exec/tdigest.h"
#include "../exec/filter.h"
#include "../exec/bucket.h"
#include "../exec/pool.h"
#include "../catalog/group.h"
#include "../catalog/database.h"
#include "../catalog/device.h"
#include "../catalog/stable.h"
#include "../catalog/tmq.h"
#include "../catalog/udf.h"
#include "../catalog/user.h"
#include "../server/proto.h"  /* tsdb_crc32c — hardware-accelerated hash */
#include "../catalog/audit.h"
#include "../../include/tsdb.h"
#include "../../include/tsdb_cluster.h"
#include "../raft/raft.h"
#include "../server/metrics.h"
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

/* Forward declaration for the mkdir -p helper exposed by schema.c. */
extern int tsdb_mkdir_p(const char *path);

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

/* Forward declaration: scan_src_t is used in bloom_can_skip_block before
 * its full definition.  Full typedef appears in the Scan state section below. */
typedef struct scan_src scan_src_t;

/* ---- Bloom filter block-skip statistics --------------------------------- */

/* Counts blocks skipped by Bloom filter in the most recent query.
 * Reset to 0 at the start of each serial SELECT scan.
 * Thread-local per-query; works correctly for single-threaded serial path. */
static uint64_t g_bloom_blocks_skipped = 0;
static uint64_t g_bloom_blocks_total   = 0;

/* Test-visible accessor. */
uint64_t tsdb_bloom_stats_skipped(void) { return g_bloom_blocks_skipped; }
uint64_t tsdb_bloom_stats_total(void)   { return g_bloom_blocks_total; }

/* ---- Bloom pre-filter: extract AND-connected SYMBOL EQ predicates -------
 *
 * Walk WHERE through AND nodes only (stop at OR/NOT — conservative).
 * For each EQ node with a SYMBOL column LHS and string constant RHS,
 * record (col_idx, code). Returns the number of constraints collected.
 * A bloom miss on ANY one of them means the block is skippable.
 */
typedef struct {
    int      col;   /* schema column index */
    uint32_t code;  /* symbol code */
} bloom_constraint_t;

#define BLOOM_CONSTRAINT_MAX 8

static int extract_bloom_constraints(qast_expr_t *e, tsdb_schema_t *s,
                                      bloom_constraint_t *out, int cap)
{
    if (!e || cap <= 0) return 0;
    /* AND: recurse both sides */
    if (e->kind == QAST_AND) {
        int n = extract_bloom_constraints(e->lhs, s, out, cap);
        n += extract_bloom_constraints(e->rhs, s, out + n, cap - n);
        return n;
    }
    /* Only process EQ */
    if (e->kind != QAST_EQ) return 0;
    if (!e->lhs || !e->rhs) return 0;
    if (e->lhs->kind != QAST_IDENT) return 0;
    int col = tsdb_schema_col_idx(s, e->lhs->v.s);
    if (col < 0) return 0;
    if (s->cols[col].type != TSDB_TYPE_SYMBOL) return 0;
    if (e->rhs->kind != QAST_LIT_STR) return 0;
    /* Look up the code (may not exist yet if symbol was never seen). */
    uint32_t code = tsdb_symtab_lookup(s->cols[col].symtab, e->rhs->v.s);
    if (code == TSDB_SYMBOL_INVALID) {
        /* Symbol doesn't exist at all — mark with a sentinel (we still record
         * it; any bloom will miss bit 0 of code=UINT32_MAX which we handle). */
        /* Actually: mark col = -1 so the caller knows "skip everything". */
        out[0].col  = -1;   /* sentinel: symbol not in table, skip all blocks */
        out[0].code = 0;
        return 1;
    }
    out[0].col  = col;
    out[0].code = code;
    return 1;
}

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

/* Slow path for result_reserve_rows — called only when growth is required.
 * Kept out-of-line so the inlined fast path is a single comparison. */
static __attribute__((noinline))
int result_reserve_rows_grow(tsdb_result_t *r, size_t want) {
    size_t ncap = r->cap_rows ? r->cap_rows : 1024;
    while (ncap < want) ncap *= 2;
    for (int c = 0; c < r->ncols; c++) {
        size_t w = 8; /* all result types fit in 8 bytes (ts/i64/f64/sym→u32→u64) */
        void *np = realloc(r->col_data[c], w * ncap);
        if (!np) return TSDB_ERR_NOMEM;
        r->col_data[c] = np;
    }
    r->cap_rows = ncap;
    return TSDB_OK;
}

/* Hot path: caller already knows nrows + N will fit under cap_rows in the
 * common case.  Compiler inlines the comparison; the grow branch is cold. */
static inline __attribute__((always_inline))
int result_reserve_rows(tsdb_result_t *r, size_t want) {
    if (__builtin_expect(want <= r->cap_rows, 1)) return TSDB_OK;
    return result_reserve_rows_grow(r, want);
}

static inline __attribute__((always_inline))
void result_append_cell(tsdb_result_t *r, int col, uint64_t bits) {
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

struct scan_src {
    /* Per source: either a memtable or a (part, block-index) */
    tsdb_memtable_t  *mem;             /* NULL if from disk */
    /* Per-column snapshot of the memtable taken under m->lock at
     * scan_plan_push time.  Closes F2: without the snapshot, a
     * concurrent flush+clear+new-write cycle could overwrite the
     * memtable's column buffers between the reader's nrows sample
     * and its iteration, producing torn rows.  Owned by the scan
     * plan; freed in scan_plan_free.  NULL when sc is not from
     * memtable.  Indexed by schema column index. */
    void            **mem_bufs;
    int               mem_nbufs;       /* schema->ncols at snapshot time */
    tsdb_part_t      *part;            /* NULL if from memtable */
    tsdb_block_meta_t meta;            /* only valid when part != NULL */
    size_t            row_count;       /* rows in this source segment */
    int64_t           ts_min, ts_max;
};
/* typedef scan_src_t was forward-declared above bloom_can_skip_block */

/*
 * Check whether a block can be skipped using Bloom filter constraints.
 *
 * For each AND-connected SYMBOL EQ constraint:
 *   - Find the SYMBOL column's block at (ts_min, count) — same block index.
 *   - If TSDB_BF_HAS_BLOOM set and bloom_test returns 0 → definitely skip.
 *
 * Returns 1 if the block can be skipped; 0 otherwise.
 */
static int bloom_can_skip_block(tsdb_part_t *part, tsdb_schema_t *s,
                                 const scan_src_t *src,
                                 const bloom_constraint_t *bc, int nbc)
{
    if (nbc == 0) return 0;

    for (int i = 0; i < nbc; i++) {
        int col = bc[i].col;
        if (col < 0) {
            /* Sentinel: symbol doesn't exist in the table at all. */
            return 1;
        }
        /* Find this column's block metadata matching current source block. */
        tsdb_block_meta_t *metas = NULL; size_t nb = 0;
        int rc = tsdb_part_col_blocks(part, col, &metas, &nb);
        if (rc != TSDB_OK || !metas) continue;

        tsdb_block_meta_t *hit = NULL;
        for (size_t b = 0; b < nb; b++) {
            if (metas[b].ts_min == src->meta.ts_min &&
                metas[b].count  == src->meta.count) {
                hit = &metas[b]; break;
            }
        }
        int can_skip = 0;
        if (hit && (hit->flags & TSDB_BF_HAS_BLOOM)) {
            if (!tsdb_bloom_test(hit->bloom, bc[i].code)) {
                can_skip = 1;
            }
        }
        free(metas);
        if (can_skip) return 1;
    }
    return 0;
}

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
    /* Free per-source memtable snapshots — those buffers were malloc'd
     * by tsdb_memtable_snapshot under m->lock when this plan was
     * built. */
    for (size_t i = 0; i < p->nsrcs; i++) {
        scan_src_t *sc = &p->srcs[i];
        if (sc->mem_bufs) {
            for (int c = 0; c < sc->mem_nbufs; c++) free(sc->mem_bufs[c]);
            free(sc->mem_bufs);
            sc->mem_bufs = NULL;
        }
    }
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

/* Collect every partition directory under a table dir, sorted ascending.
 * Accepts both YYYYMMDD (8 chars, DAY partitions) and YYYYMMDDHH (10 chars,
 * HOUR partitions) names. Non-numeric entries are skipped. */
static int list_partitions(const char *table_dir, char ***out, size_t *n_out) {
    DIR *d = opendir(table_dir);
    if (!d) { *out = NULL; *n_out = 0; return TSDB_OK; }
    struct dirent *ent;
    char **names = NULL;
    size_t n = 0, cap = 0;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] < '0' || ent->d_name[0] > '9') continue;
        size_t nl = strlen(ent->d_name);
        if (nl != 8 && nl != 10) continue;
        /* All characters must be digits. */
        int all_digit = 1;
        for (size_t i = 0; i < nl; i++) {
            if (ent->d_name[i] < '0' || ent->d_name[i] > '9') { all_digit = 0; break; }
        }
        if (!all_digit) continue;
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

/* Forward decls for zone-map-aware scan plan construction. */
typedef struct ts_range ts_range_t_fwd;
static int  ts_range_excludes_fwd(const void *r, int64_t ts_min, int64_t ts_max);

static int scan_plan_build_ex(scan_plan_t *p, tsdb_table_internal_t *t,
                              const void *zone_prune)
{
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

        /* File-level zone-map prune: if the whole partition's [ts_min, ts_max]
         * is outside the caller's WHERE predicate range, skip it entirely —
         * no block metadata walk, no column decode. This is the Step-1 win. */
        if (zone_prune) {
            int64_t zmn = 0, zmx = 0;
            if (tsdb_part_zone_map(part, &zmn, &zmx) == TSDB_OK
                && ts_range_excludes_fwd(zone_prune, zmn, zmx)) {
                tsdb_part_close(part);
                free(dirs[i]);
                continue;
            }
        }

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

    /* Then memtable (newest data).
     *
     * Take a per-column snapshot under m->lock so the reader's view is
     * a consistent point-in-time slice — concurrent writers can keep
     * filling the memtable, trigger flush+clear, and reuse the column
     * buffers without ever corrupting THIS reader's iteration.  See
     * F2 in docs/tasks/perf-bcd-2026-05-08.md. */
    tsdb_memtable_t *mem = tsdb_tbl_memtable(t);
    if (mem) {
        tsdb_schema_t *ms = tsdb_tbl_schema(t);
        int ncols = ms->ncols;
        void **mem_bufs = calloc((size_t)ncols, sizeof(void *));
        if (!mem_bufs) return TSDB_ERR_NOMEM;
        size_t nr = 0;
        int rc = tsdb_memtable_snapshot(mem, mem_bufs, &nr);
        if (rc != TSDB_OK) { free(mem_bufs); return rc; }
        if (nr > 0) {
            const int64_t *tscol = (const int64_t *)mem_bufs[ts_col];
            scan_src_t sc = {0};
            sc.mem      = mem;
            sc.mem_bufs = mem_bufs;
            sc.mem_nbufs = ncols;
            sc.row_count = nr;
            sc.ts_min = tscol[0];
            sc.ts_max = tscol[nr - 1];
            for (size_t i = 0; i < nr; i++) {
                if (tscol[i] < sc.ts_min) sc.ts_min = tscol[i];
                if (tscol[i] > sc.ts_max) sc.ts_max = tscol[i];
            }
            scan_plan_push(p, sc);
        } else {
            /* Empty memtable: snapshot returned no buffers, just
             * release the array we allocated. */
            free(mem_bufs);
        }
    }
    return TSDB_OK;
}

/* Legacy shim: no zone prune. Kept for callers that don't have WHERE bounds. */
static int scan_plan_build(scan_plan_t *p, tsdb_table_internal_t *t) {
    return scan_plan_build_ex(p, t, NULL);
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

/* ---- Timestamp range extraction (for block-level skipping) ------------- */

typedef struct {
    int64_t lo;       /* valid rows satisfy ts >= lo */
    int64_t hi;       /* valid rows satisfy ts <= hi */
    int     has_lo;
    int     has_hi;
} ts_range_t;

static void ts_range_init(ts_range_t *r) {
    r->lo = INT64_MIN; r->hi = INT64_MAX;
    r->has_lo = 0; r->has_hi = 0;
}

/* Walk an AND-connected predicate tree and narrow [lo, hi] based on the ts
 * column comparisons. OR / NOT subtrees are skipped (conservative: those
 * blocks stay in the scan plan). */
static void extract_ts_bounds(qast_expr_t *e, tsdb_schema_t *s, ts_range_t *out) {
    if (!e) return;
    if (e->kind == QAST_AND) {
        extract_ts_bounds(e->lhs, s, out);
        extract_ts_bounds(e->rhs, s, out);
        return;
    }
    if (!e->lhs || !e->rhs) return;
    if (e->lhs->kind != QAST_IDENT) return;

    int col = resolve_col(s, e->lhs->v.s);
    if (col != s->ts_col_idx) return;

    int64_t rhs;
    if (e->rhs->kind == QAST_LIT_INT) rhs = e->rhs->v.i;
    else if (e->rhs->kind == QAST_LIT_TS) rhs = e->rhs->v.ts;
    else return;

    switch (e->kind) {
    case QAST_GE:
        if (rhs > out->lo || !out->has_lo) { out->lo = rhs; out->has_lo = 1; }
        break;
    case QAST_GT: {
        int64_t v = (rhs == INT64_MAX) ? INT64_MAX : rhs + 1;
        if (v > out->lo || !out->has_lo) { out->lo = v; out->has_lo = 1; }
        break;
    }
    case QAST_LE:
        if (rhs < out->hi || !out->has_hi) { out->hi = rhs; out->has_hi = 1; }
        break;
    case QAST_LT: {
        int64_t v = (rhs == INT64_MIN) ? INT64_MIN : rhs - 1;
        if (v < out->hi || !out->has_hi) { out->hi = v; out->has_hi = 1; }
        break;
    }
    case QAST_EQ:
        if (rhs > out->lo || !out->has_lo) { out->lo = rhs; out->has_lo = 1; }
        if (rhs < out->hi || !out->has_hi) { out->hi = rhs; out->has_hi = 1; }
        break;
    default:
        break;
    }
}

/* Return 1 if a block with [ts_min, ts_max] definitely has zero matches. */
static int ts_range_excludes(const ts_range_t *r, int64_t ts_min, int64_t ts_max) {
    if (r->has_hi && ts_min > r->hi) return 1;
    if (r->has_lo && ts_max < r->lo) return 1;
    return 0;
}

/* Wrapper exposed to scan_plan_build_ex (which is defined earlier, before
 * ts_range_t). Using void* keeps the build dependency one-way. */
static int ts_range_excludes_fwd(const void *r, int64_t ts_min, int64_t ts_max) {
    return ts_range_excludes((const ts_range_t *)r, ts_min, ts_max);
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

    /* Constant literal: QAST_LIT_INT non-zero = always-true, 0 = always-false.
     * Used by STable tag-predicate extraction which replaces tag conjuncts with 1. */
    if (e->kind == QAST_LIT_INT) {
        if (e->v.i == 0) {
            /* Always-false: clear entire bitmap. */
            size_t nw = (ctx->nrows + 63) / 64;
            memset(bm, 0, nw * sizeof(uint64_t));
        }
        /* Always-true (non-zero): bitmap unchanged. */
        return TSDB_OK;
    }

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

#define MAVG_MAX_WINDOW 64

/* A "projection" tells how to get values for an output column from a block. */
typedef enum {
    PROJ_COL,
    PROJ_TS_BUCKET,
    PROJ_AGG_SUM,
    PROJ_AGG_AVG,
    PROJ_AGG_MIN,
    PROJ_AGG_MAX,
    PROJ_AGG_SPREAD,    /* spread(col) == max(col) - min(col) */
    PROJ_AGG_COUNT,
    /* T-digest based approximate quantiles and stddev */
    PROJ_AGG_P50,
    PROJ_AGG_P90,
    PROJ_AGG_P99,
    PROJ_AGG_PERCENTILE,  /* percentile(col, q) — user-specified q */
    PROJ_AGG_STDDEV,
    /* Time-series specialised aggregates (TDengine-style) */
    PROJ_AGG_TS_FIRST,    /* first(col)    -- value at earliest ts */
    PROJ_AGG_TS_LAST,     /* last(col)     -- value at latest ts */
    PROJ_AGG_TS_LAST_ROW, /* last_row(col) -- MVP: same as last(col) */
    PROJ_AGG_TS_TWA,      /* twa(col)      -- time-weighted average */
    /* Window (per-row transform) functions */
    PROJ_WIN_DIFF,        /* diff(col)         v[i] - v[i-1], NaN on first row */
    PROJ_WIN_DERIVATIVE,  /* derivative(col)   (v[i]-v[i-1])/(ts[i]-ts[i-1])*1e9 */
    PROJ_WIN_CSUM,        /* csum(col)         cumulative sum */
    PROJ_WIN_MAVG,        /* mavg(col, N)      moving average, window N ≤ 64 */
    PROJ_WIN_LAG,         /* lag(col, N=1)     value N rows back, NaN until n>N */
    PROJ_WIN_INTERP,      /* interp(col,'Xs')  (stub – not yet implemented) */
    PROJ_UDF_SCALAR,      /* user-defined scalar function, per-row call    */
} proj_kind_t;

/* Range sentinels: all PROJ_AGG_* kinds.
 * PROJ_AGG_RANGE_BEGIN / PROJ_AGG_RANGE_END replace old PROJ_AGG_FIRST/LAST
 * to avoid collision with user-facing first()/last() => PROJ_AGG_TS_FIRST/LAST. */
#define PROJ_AGG_RANGE_BEGIN PROJ_AGG_SUM
#define PROJ_AGG_RANGE_END   PROJ_AGG_TS_TWA
/* T-digest kinds start at P50 */
#define PROJ_AGG_TDIGEST_FIRST PROJ_AGG_P50
/* TS-specialised agg kinds start here */
#define PROJ_AGG_TS_KIND_FIRST PROJ_AGG_TS_FIRST

/* Legacy sentinels kept for compatibility with older code in the file. */
#define PROJ_AGG_FIRST PROJ_AGG_SUM
#define PROJ_AGG_LAST  PROJ_AGG_TS_TWA

/* Sentinel: all PROJ_WIN_* kinds */
#define PROJ_WIN_FIRST PROJ_WIN_DIFF
#define PROJ_WIN_LAST  PROJ_WIN_INTERP

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
    /* T-digest state for PROJ_AGG_P50/P90/P99/PERCENTILE/STDDEV */
    tsdb_tdigest_t *tdigest;        /* NULL unless this is a tdigest kind */
    double          percentile_q;   /* q for PROJ_AGG_PERCENTILE [0,1] */
    /* ---- FIRST / LAST / LAST_ROW / TWA state (serial path only) ---------- */
    int64_t  ts_first;              /* ts of first row seen (INT64_MAX=none)  */
    int64_t  ts_last;               /* ts of last  row seen (INT64_MIN=none)  */
    double   ts_first_f, ts_last_f; /* float64 payload for FIRST/LAST         */
    int64_t  ts_first_i, ts_last_i; /* int64   payload for FIRST/LAST         */
    uint32_t ts_first_u32, ts_last_u32; /* uint32 payload for FIRST/LAST      */
    /* TWA state (step-wise-backward): twa = sum(prev_v*dt) / total_dt         */
    double   twa_wsum;              /* weighted sum accumulated so far         */
    int64_t  twa_last_ts;           /* ts of most-recent point (-1=none)       */
    double   twa_last_v;            /* val of most-recent point                */
    /* ---- Window (per-row transform) function state ----------------------- */
    int     win_has_prev;    /* 1 once at least one row has been processed    */
    double  win_prev_f;      /* previous value as double (DIFF/DERIVATIVE)    */
    int64_t win_prev_ts;     /* previous timestamp (DERIVATIVE)               */
    double  win_csum;        /* cumulative sum accumulator (CSUM)             */
    /* MAVG ring buffer */
    double  mavg_buf[MAVG_MAX_WINDOW];
    int     mavg_n;          /* current fill count                            */
    int     mavg_head;       /* oldest element index                          */
    int     mavg_window;     /* configured window size                        */
    double  mavg_sum;        /* running sum                                   */
    /* INTERP: grid interval in nanoseconds */
    int64_t interp_bucket_ns;
    /* ---- UDF scalar call state ------------------------------------------ */
    tsdb_udf_fn_t     udf_fn;
    int               udf_nargs;
    int               udf_arg_cols[8];  /* source column index per UDF arg    */
    tsdb_udf_type_t   udf_arg_types[8];
    tsdb_udf_type_t   udf_ret_type;
} proj_t;

static int is_agg_call(qast_expr_t *e) {
    if (!e || e->kind != QAST_CALL) return 0;
    const char *n = e->v.s;
    return strcasecmp(n, "sum") == 0       || strcasecmp(n, "avg") == 0        ||
           strcasecmp(n, "min") == 0       || strcasecmp(n, "max") == 0        ||
           strcasecmp(n, "spread") == 0    ||
           strcasecmp(n, "count") == 0     ||
           strcasecmp(n, "p50") == 0       || strcasecmp(n, "p90") == 0        ||
           strcasecmp(n, "p99") == 0       || strcasecmp(n, "percentile") == 0 ||
           strcasecmp(n, "stddev") == 0    ||
           /* TDengine-style TS aggregates */
           strcasecmp(n, "first") == 0     || strcasecmp(n, "last") == 0       ||
           strcasecmp(n, "last_row") == 0  || strcasecmp(n, "twa") == 0;
}

static int is_window_call(qast_expr_t *e) {
    if (!e || e->kind != QAST_CALL) return 0;
    const char *n = e->v.s;
    return strcasecmp(n, "diff") == 0       ||
           strcasecmp(n, "derivative") == 0 ||
           strcasecmp(n, "csum") == 0       ||
           strcasecmp(n, "mavg") == 0       ||
           strcasecmp(n, "lag") == 0        ||
           strcasecmp(n, "interp") == 0;
}
/* Helper: allocate a fresh tdigest and store into proj; NULLs on failure. */
static int proj_tdigest_init(proj_t *p) {
    return tsdb_tdigest_new(100.0, &p->tdigest);
}

/* Free a proj's tdigest (idempotent). */
static void proj_tdigest_free(proj_t *p) {
    if (p->tdigest) { tsdb_tdigest_free(p->tdigest); p->tdigest = NULL; }
}

static int build_projections(qast_query_t *q, tsdb_schema_t *s,
                             tsdb_db_t *db,
                             proj_t **out, int *out_n, int *out_has_agg,
                             int *out_has_window, int *out_has_ts_agg,
                             int *out_has_interp,
                             char *err, size_t errcap) {
    proj_t *arr = NULL;
    int cap = 0, n = 0;

    int has_agg = 0;
    int has_window = 0;
    int has_ts_agg = 0;
    int has_interp = 0;

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
            if      (strcasecmp(name, "sum") == 0)        k = PROJ_AGG_SUM;
            else if (strcasecmp(name, "avg") == 0)        k = PROJ_AGG_AVG;
            else if (strcasecmp(name, "min") == 0)        k = PROJ_AGG_MIN;
            else if (strcasecmp(name, "max") == 0)        k = PROJ_AGG_MAX;
            else if (strcasecmp(name, "spread") == 0)     k = PROJ_AGG_SPREAD;
            else if (strcasecmp(name, "count") == 0)      k = PROJ_AGG_COUNT;
            else if (strcasecmp(name, "p50") == 0)        k = PROJ_AGG_P50;
            else if (strcasecmp(name, "p90") == 0)        k = PROJ_AGG_P90;
            else if (strcasecmp(name, "p99") == 0)        k = PROJ_AGG_P99;
            else if (strcasecmp(name, "percentile") == 0) k = PROJ_AGG_PERCENTILE;
            else if (strcasecmp(name, "stddev") == 0)     k = PROJ_AGG_STDDEV;
            /* TDengine-style TS aggregates */
            else if (strcasecmp(name, "first") == 0)    { k = PROJ_AGG_TS_FIRST;    has_ts_agg = 1; }
            else if (strcasecmp(name, "last") == 0)     { k = PROJ_AGG_TS_LAST;     has_ts_agg = 1; }
            else if (strcasecmp(name, "last_row") == 0) { k = PROJ_AGG_TS_LAST_ROW; has_ts_agg = 1; }
            else if (strcasecmp(name, "twa") == 0)      { k = PROJ_AGG_TS_TWA;      has_ts_agg = 1; }
            arr[n].kind = k;

            /* Resolve column argument. */
            if (e->nargs >= 1 && e->args[0]->kind == QAST_IDENT) {
                int c = resolve_col(s, e->args[0]->v.s);
                if (c < 0 && k != PROJ_AGG_COUNT) {
                    eset(err, errcap, "unknown column in aggregate: %s", e->args[0]->v.s);
                    free(arr); return TSDB_ERR_SCHEMA;
                }
                arr[n].col = c;
            } else if (k == PROJ_AGG_COUNT && (e->nargs == 0 || (e->nargs == 1 && e->args[0]->kind == QAST_STAR))) {
                arr[n].col = s->ts_col_idx; /* count uses row count */
            } else if (k >= PROJ_AGG_TS_KIND_FIRST) {
                /* TS agg requires a column argument */
                if (e->nargs < 1 || e->args[0]->kind != QAST_IDENT) {
                    eset(err, errcap, "%s() requires a column argument", name);
                    free(arr); return TSDB_ERR_PARSE;
                }
                /* col already resolved above */
            } else if (k < PROJ_AGG_TDIGEST_FIRST) {
                eset(err, errcap, "unsupported aggregate argument");
                free(arr); return TSDB_ERR_UNSUPPORTED;
            }

            /* For percentile(col, q): parse q from second argument. */
            if (k == PROJ_AGG_PERCENTILE) {
                if (e->nargs != 2) {
                    eset(err, errcap, "percentile() requires 2 arguments: percentile(col, q)");
                    free(arr); return TSDB_ERR_PARSE;
                }
                double q = 0.5;
                qast_expr_t *qarg = e->args[1];
                if (qarg->kind == QAST_LIT_FLOAT)     q = qarg->v.f;
                else if (qarg->kind == QAST_LIT_INT)  q = (double)qarg->v.i;
                else {
                    eset(err, errcap, "percentile() second argument must be a numeric literal");
                    free(arr); return TSDB_ERR_PARSE;
                }
                if (q < 0.0) q = 0.0;
                if (q > 1.0) q = 1.0;
                arr[n].percentile_q = q;
            } else {
                /* Set default q for fixed-quantile helpers. */
                if      (k == PROJ_AGG_P50) arr[n].percentile_q = 0.50;
                else if (k == PROJ_AGG_P90) arr[n].percentile_q = 0.90;
                else if (k == PROJ_AGG_P99) arr[n].percentile_q = 0.99;
            }

            /* Output type */
            if (k == PROJ_AGG_COUNT)
                arr[n].out_type = TSDB_TYPE_INT64;
            else if (k >= PROJ_AGG_TDIGEST_FIRST && k < PROJ_AGG_TS_KIND_FIRST)
                arr[n].out_type = TSDB_TYPE_FLOAT64;
            else if (k == PROJ_AGG_AVG || k == PROJ_AGG_TS_TWA)
                arr[n].out_type = TSDB_TYPE_FLOAT64;
            else
                arr[n].out_type = (arr[n].col >= 0) ? s->cols[arr[n].col].type : TSDB_TYPE_FLOAT64;

            /* Build output column name. */
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

            /* Allocate tdigest for tdigest-kind projections (not TS agg). */
            if (k >= PROJ_AGG_TDIGEST_FIRST && k < PROJ_AGG_TS_KIND_FIRST) {
                if (proj_tdigest_init(&arr[n]) != 0) {
                    free(arr); return TSDB_ERR_NOMEM;
                }
            }
            /* Initialise TS agg state. */
            if (k >= PROJ_AGG_TS_KIND_FIRST) {
                arr[n].ts_first   = INT64_MAX;
                arr[n].ts_last    = INT64_MIN;
                arr[n].twa_last_ts = -1;
            }
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
        } else if (is_window_call(e)) {
            /* ---- Window (per-row transform) functions -------------------- */
            const char *wname = e->v.s;
            proj_kind_t wk;
            if      (strcasecmp(wname, "diff")       == 0) wk = PROJ_WIN_DIFF;
            else if (strcasecmp(wname, "derivative") == 0) wk = PROJ_WIN_DERIVATIVE;
            else if (strcasecmp(wname, "csum")       == 0) wk = PROJ_WIN_CSUM;
            else if (strcasecmp(wname, "mavg")       == 0) wk = PROJ_WIN_MAVG;
            else if (strcasecmp(wname, "lag")        == 0) wk = PROJ_WIN_LAG;
            else                                            wk = PROJ_WIN_INTERP;

            if (wk == PROJ_WIN_INTERP) {
                /* interp(col, interval_ns) — grid-aligned linear interpolation.
                 * Requires exactly 2 args: column name + interval literal. */
                if (e->nargs < 2) {
                    eset(err, errcap, "interp() requires 2 args: interp(col, interval)");
                    free(arr); return TSDB_ERR_PARSE;
                }
                if (e->args[0]->kind != QAST_IDENT) {
                    eset(err, errcap, "interp(): first argument must be a column name");
                    free(arr); return TSDB_ERR_PARSE;
                }
                int64_t ibucket = 0;
                if (e->args[1]->kind == QAST_LIT_INT) {
                    ibucket = e->args[1]->v.i;
                } else {
                    eset(err, errcap, "interp(): second argument must be an interval literal");
                    free(arr); return TSDB_ERR_PARSE;
                }
                if (ibucket <= 0) {
                    eset(err, errcap, "interp(): interval must be positive");
                    free(arr); return TSDB_ERR_PARSE;
                }
                int c = resolve_col(s, e->args[0]->v.s);
                if (c < 0) {
                    eset(err, errcap, "interp(): unknown column '%s'", e->args[0]->v.s);
                    free(arr); return TSDB_ERR_SCHEMA;
                }
                arr[n].kind             = PROJ_WIN_INTERP;
                arr[n].col              = c;
                arr[n].interp_bucket_ns = ibucket;
                arr[n].out_type         = TSDB_TYPE_FLOAT64;
                if (si->alias) snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias);
                else snprintf(arr[n].name, sizeof(arr[n].name), "interp(%s)", s->cols[c].name);
                has_interp = 1;
                n++;
                continue;
            }
            if (e->nargs < 1 || e->args[0]->kind != QAST_IDENT) {
                eset(err, errcap, "%s() requires a column name as first argument", wname);
                free(arr); return TSDB_ERR_PARSE;
            }
            int c = resolve_col(s, e->args[0]->v.s);
            if (c < 0) {
                eset(err, errcap, "%s(): unknown column '%s'", wname, e->args[0]->v.s);
                free(arr); return TSDB_ERR_SCHEMA;
            }
            arr[n].kind     = wk;
            arr[n].col      = c;
            arr[n].out_type = TSDB_TYPE_FLOAT64;
            /* MAVG: parse window size from second argument. */
            if (wk == PROJ_WIN_MAVG) {
                if (e->nargs != 2) {
                    eset(err, errcap, "mavg() requires 2 arguments: mavg(col, N)");
                    free(arr); return TSDB_ERR_PARSE;
                }
                qast_expr_t *warg = e->args[1];
                int64_t wsize = 0;
                if (warg->kind == QAST_LIT_INT)        wsize = warg->v.i;
                else if (warg->kind == QAST_LIT_FLOAT)  wsize = (int64_t)warg->v.f;
                else {
                    eset(err, errcap, "mavg() window size must be an integer literal");
                    free(arr); return TSDB_ERR_PARSE;
                }
                if (wsize < 1 || wsize > MAVG_MAX_WINDOW) {
                    eset(err, errcap, "mavg() window size must be 1..%d", MAVG_MAX_WINDOW);
                    free(arr); return TSDB_ERR_PARSE;
                }
                arr[n].mavg_window = (int)wsize;
            }
            /* LAG(col [, N=1]): reuse mavg_buf as a length-N ring of past
             * values; emit oldest once the ring fills (NaN before that). */
            if (wk == PROJ_WIN_LAG) {
                int64_t lag_n_arg = 1;
                if (e->nargs == 2) {
                    qast_expr_t *larg = e->args[1];
                    if      (larg->kind == QAST_LIT_INT)   lag_n_arg = larg->v.i;
                    else if (larg->kind == QAST_LIT_FLOAT) lag_n_arg = (int64_t)larg->v.f;
                    else {
                        eset(err, errcap, "lag() N must be an integer literal");
                        free(arr); return TSDB_ERR_PARSE;
                    }
                } else if (e->nargs > 2) {
                    eset(err, errcap, "lag() takes 1 or 2 arguments: lag(col [, N])");
                    free(arr); return TSDB_ERR_PARSE;
                }
                if (lag_n_arg < 1 || lag_n_arg > MAVG_MAX_WINDOW) {
                    eset(err, errcap, "lag() N must be 1..%d", MAVG_MAX_WINDOW);
                    free(arr); return TSDB_ERR_PARSE;
                }
                arr[n].mavg_window = (int)lag_n_arg;
            }
            if (si->alias) snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias);
            else snprintf(arr[n].name, sizeof(arr[n].name), "%s(%s)", wname, s->cols[c].name);
            has_window = 1;
        } else if (e->kind == QAST_CALL && db) {
            /* ---- UDF scalar call --------------------------------------------
             * Not a builtin — look up in the UDF catalog, resolve dlopen/dlsym
             * lazily, and wire an entry that the emit path invokes per row. */
            tsdb_udf_catalog_t *udfcat = tsdb_db_udf(db);
            const tsdb_udf_entry_t *ue = NULL;
            char ebuf[256] = {0};
            int lrc = udfcat ? tsdb_udf_catalog_lookup(udfcat, e->v.s, &ue, ebuf, sizeof(ebuf))
                             : TSDB_ERR_NOTFOUND;
            if (lrc != TSDB_OK) {
                if (lrc == TSDB_ERR_UNSUPPORTED) {
                    eset(err, errcap, "UDF '%s': %s", e->v.s,
                         ebuf[0] ? ebuf : "ABI version mismatch");
                } else if (ebuf[0]) {
                    eset(err, errcap, "UDF '%s' not callable: %s", e->v.s, ebuf);
                } else {
                    eset(err, errcap, "unknown function '%s' (not a builtin or registered UDF)",
                         e->v.s);
                }
                free(arr); return lrc;
            }
            if (e->nargs != ue->nargs) {
                eset(err, errcap, "UDF '%s' expects %d args, got %d",
                     e->v.s, ue->nargs, e->nargs);
                free(arr); return TSDB_ERR_PARSE;
            }
            /* Each argument must be a bare column reference in v1. */
            for (int ai = 0; ai < e->nargs; ai++) {
                if (e->args[ai]->kind != QAST_IDENT) {
                    eset(err, errcap,
                         "UDF '%s': arg %d must be a column reference (v1 restriction)",
                         e->v.s, ai);
                    free(arr); return TSDB_ERR_UNSUPPORTED;
                }
                int ci = resolve_col(s, e->args[ai]->v.s);
                if (ci < 0) {
                    eset(err, errcap, "UDF '%s': unknown column '%s'",
                         e->v.s, e->args[ai]->v.s);
                    free(arr); return TSDB_ERR_SCHEMA;
                }
                /* Per-arg type match. */
                tsdb_udf_type_t wire = (tsdb_udf_type_t)s->cols[ci].type;
                if (wire != ue->arg_types[ai]) {
                    eset(err, errcap,
                         "UDF '%s': arg %d column '%s' type mismatch",
                         e->v.s, ai, e->args[ai]->v.s);
                    free(arr); return TSDB_ERR_SCHEMA;
                }
                arr[n].udf_arg_cols[ai]  = ci;
                arr[n].udf_arg_types[ai] = wire;
            }
            arr[n].kind         = PROJ_UDF_SCALAR;
            arr[n].udf_fn       = ue->fn;
            arr[n].udf_nargs    = ue->nargs;
            arr[n].udf_ret_type = ue->ret_type;
            arr[n].out_type     = (tsdb_type_t)ue->ret_type;
            arr[n].col          = arr[n].udf_arg_cols[0]; /* used by scan gather */
            if (si->alias) snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias);
            else           snprintf(arr[n].name, sizeof(arr[n].name), "%s()", e->v.s);
        } else {
            eset(err, errcap, "unsupported SELECT expression kind %d", e->kind);
            free(arr); return TSDB_ERR_UNSUPPORTED;
        }
        n++;
    }
    *out = arr; *out_n = n; *out_has_agg = has_agg;
    if (out_has_window)  *out_has_window  = has_window;
    if (out_has_ts_agg) *out_has_ts_agg = has_ts_agg;
    if (out_has_interp)  *out_has_interp  = has_interp;
    return TSDB_OK;
}

/* ---- Aggregation helpers over a block --------------------------------- */

/* scratch must point to a buffer of at least TSDB_BLOCK_POINTS*8 bytes,
 * alignment 32 preferred. Used as gather destination when bm is not all-set.
 * Passing the scratch in from the caller avoids per-block malloc/free and
 * keeps the hot path allocation-free. */
static void agg_update(proj_t *p, tsdb_schema_t *s, void **bufs, size_t n,
                       const uint64_t *bm, void *scratch) {
    tsdb_type_t t = (p->col >= 0) ? s->cols[p->col].type : TSDB_TYPE_INT64;

    if (p->kind == PROJ_AGG_COUNT) {
        p->agg_count += tsdb_bitmap_popcount(bm, n);
        return;
    }

    /* Determine selectivity once per block so we can take the all-set fast path. */
    uint64_t popcnt = tsdb_bitmap_popcount(bm, n);
    if (popcnt == 0) return;

    /* ---- T-digest path (always f64 gathered) ---- */
    if (p->kind >= PROJ_AGG_TDIGEST_FIRST && p->kind < PROJ_AGG_TS_KIND_FIRST) {
        if (!p->tdigest) return;
        const double *src;
        size_t cn;
        /* For non-f64 columns: gather into scratch as f64 via cast. */
        if (t == TSDB_TYPE_FLOAT64) {
            const double *v = (const double *)bufs[p->col];
            if (popcnt == n) {
                src = v; cn = n;
            } else {
                double *tmp = (double *)scratch;
                cn = tsdb_bitmap_gather_f64(bm, v, n, tmp);
                src = tmp;
            }
        } else {
            /* int64 / timestamp → cast to double */
            const int64_t *iv = (const int64_t *)bufs[p->col];
            double *tmp = (double *)scratch;
            if (popcnt == n) {
                for (size_t i = 0; i < n; i++) tmp[i] = (double)iv[i];
                src = tmp; cn = n;
            } else {
                int64_t *itmp = (int64_t *)scratch;
                size_t icn = tsdb_bitmap_gather_i64(bm, iv, n, itmp);
                for (size_t i = 0; i < icn; i++) tmp[i] = (double)itmp[i];
                src = tmp; cn = icn;
            }
        }
        tsdb_tdigest_add_bulk(p->tdigest, src, cn, NULL);
        return;
    }

    if (t == TSDB_TYPE_FLOAT64) {
        const double *v = (const double *)bufs[p->col];
        const double *src;
        size_t cn;
        if (popcnt == n) {
            src = v; cn = n;
        } else {
            double *tmp = (double *)scratch;
            cn = tsdb_bitmap_gather_f64(bm, v, n, tmp);
            src = tmp;
        }
        switch (p->kind) {
        case PROJ_AGG_SUM:
        case PROJ_AGG_AVG: {
            double out; uint64_t ncnt;
            tsdb_agg_sum_f64(src, cn, NULL, &out, &ncnt);
            p->agg_sum_f += out;
            p->agg_count += cn;
            break;
        }
        case PROJ_AGG_MIN: {
            double m;
            tsdb_agg_min_f64(src, cn, NULL, &m);
            if (m < p->agg_min_f) p->agg_min_f = m;
            p->agg_count += cn;
            break;
        }
        case PROJ_AGG_MAX: {
            double m;
            tsdb_agg_max_f64(src, cn, NULL, &m);
            if (m > p->agg_max_f) p->agg_max_f = m;
            p->agg_count += cn;
            break;
        }
        case PROJ_AGG_SPREAD: {
            /* spread keeps both bounds simultaneously. */
            double lo, hi;
            tsdb_agg_min_f64(src, cn, NULL, &lo);
            tsdb_agg_max_f64(src, cn, NULL, &hi);
            if (lo < p->agg_min_f) p->agg_min_f = lo;
            if (hi > p->agg_max_f) p->agg_max_f = hi;
            p->agg_count += cn;
            break;
        }
        default: break;
        }
    } else if (t == TSDB_TYPE_INT64 || t == TSDB_TYPE_TIMESTAMP) {
        const int64_t *v = (const int64_t *)bufs[p->col];
        const int64_t *src;
        size_t cn;
        if (popcnt == n) {
            src = v; cn = n;
        } else {
            int64_t *tmp = (int64_t *)scratch;
            cn = tsdb_bitmap_gather_i64(bm, v, n, tmp);
            src = tmp;
        }
        switch (p->kind) {
        case PROJ_AGG_SUM:
        case PROJ_AGG_AVG: {
            int64_t out; uint64_t ncnt;
            tsdb_agg_sum_i64(src, cn, NULL, &out, &ncnt);
            p->agg_sum_i += out;
            p->agg_count += cn;
            break;
        }
        case PROJ_AGG_MIN: {
            int64_t m;
            tsdb_agg_min_i64(src, cn, NULL, &m);
            if (m < p->agg_min_i) p->agg_min_i = m;
            p->agg_count += cn;
            break;
        }
        case PROJ_AGG_MAX: {
            int64_t m;
            tsdb_agg_max_i64(src, cn, NULL, &m);
            if (m > p->agg_max_i) p->agg_max_i = m;
            p->agg_count += cn;
            break;
        }
        case PROJ_AGG_SPREAD: {
            int64_t lo, hi;
            tsdb_agg_min_i64(src, cn, NULL, &lo);
            tsdb_agg_max_i64(src, cn, NULL, &hi);
            if (lo < p->agg_min_i) p->agg_min_i = lo;
            if (hi > p->agg_max_i) p->agg_max_i = hi;
            p->agg_count += cn;
            break;
        }
        default: break;
        }
    }

    /* ---- TS-specialised aggregates: FIRST / LAST / LAST_ROW / TWA --------
     * These iterate rows one-by-one using the ts column (bufs[ts_col_idx]).  */
    if (p->kind == PROJ_AGG_TS_FIRST || p->kind == PROJ_AGG_TS_LAST ||
        p->kind == PROJ_AGG_TS_LAST_ROW || p->kind == PROJ_AGG_TS_TWA) {
        const int64_t *ts_col = (bufs && s->ts_col_idx >= 0)
                                ? (const int64_t *)bufs[s->ts_col_idx] : NULL;
        if (!ts_col || p->col < 0) return;
        tsdb_type_t vt = s->cols[p->col].type;
        for (size_t i = 0; i < n; i++) {
            if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
            int64_t ts = ts_col[i];
            double   vf  = 0.0;
            int64_t  vi  = 0;
            uint32_t vu  = 0;
            int is_f64 = (vt == TSDB_TYPE_FLOAT64);
            int is_sym = (vt == TSDB_TYPE_SYMBOL );
            if (is_f64)      vf = ((const double *)  bufs[p->col])[i];
            else if (is_sym) vu = ((const uint32_t *) bufs[p->col])[i];
            else             vi = ((const int64_t *)  bufs[p->col])[i];

            /* FIRST / LAST / LAST_ROW */
            if (p->kind != PROJ_AGG_TS_TWA) {
                if (ts < p->ts_first) {
                    p->ts_first = ts;
                    if (is_f64) p->ts_first_f = vf;
                    else if (is_sym) p->ts_first_u32 = vu;
                    else p->ts_first_i = vi;
                }
                if (ts > p->ts_last) {
                    p->ts_last = ts;
                    if (is_f64) p->ts_last_f = vf;
                    else if (is_sym) p->ts_last_u32 = vu;
                    else p->ts_last_i = vi;
                }
            }
            /* TWA: current-value-forward convention --
             * when we see point i we credit v[i] * (ts[i]-ts[i-1]).
             * Formula: Σ(v[i]·dt[i]) / total_dt, where dt[i]=ts[i]-ts[i-1].
             * Hand example: ts=[0,10,910], v=[0,10,100]
             *   dt[1]=10: +10*10=100; dt[2]=900: +100*900=90000 → sum=90100
             *   total_dt=910-0=910; twa≈99... wait — task spec:
             *   (0·0 + 10·100 + 100·900)/1000=91. That uses v[0]*0 + v[1]*(t2-t0)..
             *   Actually task spec: ts=[0,100,1000], v=[0,10,100]
             *   dt[1]=100: +10*100=1000; dt[2]=900: +100*900=90000 → sum=91000
             *   total_dt=1000; twa=91.  This is v_cur * (ts_cur - ts_prev). */
            if (p->kind == PROJ_AGG_TS_TWA) {
                double v_cur = is_f64 ? vf : (double)vi;
                if (p->twa_last_ts >= 0) {
                    int64_t dt = ts - p->twa_last_ts;
                    if (dt > 0)
                        p->twa_wsum += v_cur * (double)dt;
                }
                if (ts < p->ts_first) p->ts_first = ts;
                if (ts > p->ts_last)  p->ts_last  = ts;
                p->twa_last_ts = ts;
                p->twa_last_v  = v_cur;
            }
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
    case PROJ_AGG_SPREAD:
        if (src_t == TSDB_TYPE_FLOAT64) {
            if (p->agg_count == 0 ||
                p->agg_max_f == -INFINITY || p->agg_min_f == INFINITY)
                out_f = 0.0 / 0.0;
            else
                out_f = p->agg_max_f - p->agg_min_f;
        } else {
            if (p->agg_count == 0 ||
                p->agg_max_i == INT64_MIN || p->agg_min_i == INT64_MAX)
                out_i = 0;
            else
                out_i = p->agg_max_i - p->agg_min_i;
        }
        break;
    /* T-digest quantile/stddev kinds — always float64. */
    case PROJ_AGG_P50:
    case PROJ_AGG_P90:
    case PROJ_AGG_P99:
    case PROJ_AGG_PERCENTILE:
        out_f = p->tdigest ? tsdb_tdigest_quantile(p->tdigest, p->percentile_q) : 0.0 / 0.0;
        break;
    case PROJ_AGG_STDDEV:
        out_f = p->tdigest ? tsdb_tdigest_stddev(p->tdigest) : 0.0 / 0.0;
        break;
    /* TS-specialised aggregates */
    case PROJ_AGG_TS_FIRST:
        if (p->ts_first == INT64_MAX) { out_f = 0.0/0.0; break; }
        if (src_t == TSDB_TYPE_FLOAT64) { out_f = p->ts_first_f; }
        else if (src_t == TSDB_TYPE_SYMBOL ) {
            ((uint64_t *)r->col_data[col_idx])[r->nrows] = (uint64_t)p->ts_first_u32;
            return;
        } else { out_i = p->ts_first_i; }
        break;
    case PROJ_AGG_TS_LAST:
    case PROJ_AGG_TS_LAST_ROW:
        if (p->ts_last == INT64_MIN) { out_f = 0.0/0.0; break; }
        if (src_t == TSDB_TYPE_FLOAT64) { out_f = p->ts_last_f; }
        else if (src_t == TSDB_TYPE_SYMBOL ) {
            ((uint64_t *)r->col_data[col_idx])[r->nrows] = (uint64_t)p->ts_last_u32;
            return;
        } else { out_i = p->ts_last_i; }
        break;
    case PROJ_AGG_TS_TWA: {
        int64_t total_dt = p->ts_last - p->ts_first;
        if (total_dt <= 0)
            out_f = (p->twa_last_ts >= 0) ? p->twa_last_v : 0.0/0.0;
        else
            out_f = p->twa_wsum / (double)total_dt;
        break;
    }
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

/* Classify a projection kind for the stats-fast-path gate.
 *   bits: bit0=HAS_MIN_MAX, bit1=HAS_SUM, bit2=HAS_FIRST_LAST
 *   eligible: 1 if this kind can be served from stats at all. */
static int agg_stats_requires(int kind, uint16_t *bits, int *eligible) {
    *bits = 0;
    *eligible = 1;
    switch (kind) {
        case PROJ_AGG_COUNT:                                    return 0;
        case PROJ_AGG_MIN: case PROJ_AGG_MAX: case PROJ_AGG_SPREAD:
            *bits = TSDB_STATS_HAS_MIN_MAX;                     return 0;
        case PROJ_AGG_SUM: case PROJ_AGG_AVG:
            *bits = TSDB_STATS_HAS_SUM;                         return 0;
        case PROJ_AGG_TS_FIRST: case PROJ_AGG_TS_LAST:
            *bits = TSDB_STATS_HAS_FIRST_LAST;                  return 0;
        default:
            *eligible = 0;                                      return 0;
    }
}

/* Apply pre-computed block stats to a single projection.
 * Must only be called after the gate has verified all required bits. */
static void agg_apply_stats(proj_t *p, tsdb_schema_t *s,
                             const tsdb_block_meta_t *meta,
                             uint32_t row_count)
{
    tsdb_type_t ct = (p->col >= 0) ? s->cols[p->col].type : TSDB_TYPE_INT64;
    switch (p->kind) {
        case PROJ_AGG_COUNT:
            p->agg_count += row_count;
            return;
        case PROJ_AGG_MIN: {
            if (ct == TSDB_TYPE_FLOAT64) {
                double v; memcpy(&v, &meta->stats_min, 8);
                if (v < p->agg_min_f) p->agg_min_f = v;
            } else {
                if (meta->stats_min < p->agg_min_i) p->agg_min_i = meta->stats_min;
            }
            p->agg_count += row_count;
            return;
        }
        case PROJ_AGG_MAX: {
            if (ct == TSDB_TYPE_FLOAT64) {
                double v; memcpy(&v, &meta->stats_max, 8);
                if (v > p->agg_max_f) p->agg_max_f = v;
            } else {
                if (meta->stats_max > p->agg_max_i) p->agg_max_i = meta->stats_max;
            }
            p->agg_count += row_count;
            return;
        }
        case PROJ_AGG_SPREAD: {
            /* Fold both bounds from this block into proj state; output
             * subtracts them in agg_write. */
            if (ct == TSDB_TYPE_FLOAT64) {
                double lo, hi;
                memcpy(&lo, &meta->stats_min, 8);
                memcpy(&hi, &meta->stats_max, 8);
                if (lo < p->agg_min_f) p->agg_min_f = lo;
                if (hi > p->agg_max_f) p->agg_max_f = hi;
            } else {
                if (meta->stats_min < p->agg_min_i) p->agg_min_i = meta->stats_min;
                if (meta->stats_max > p->agg_max_i) p->agg_max_i = meta->stats_max;
            }
            p->agg_count += row_count;
            return;
        }
        case PROJ_AGG_SUM:
        case PROJ_AGG_AVG: {
            if (ct == TSDB_TYPE_FLOAT64) {
                double v; memcpy(&v, &meta->stats_sum, 8);
                p->agg_sum_f += v;
            } else {
                p->agg_sum_i += meta->stats_sum;
            }
            p->agg_count += row_count;
            return;
        }
        case PROJ_AGG_TS_FIRST: {
            /* Update only if this block predates the recorded ts_first. */
            if (meta->ts_min < p->ts_first) {
                p->ts_first = meta->ts_min;
                if (ct == TSDB_TYPE_FLOAT64)
                    memcpy(&p->ts_first_f, &meta->stats_first, 8);
                else
                    p->ts_first_i = meta->stats_first;
            }
            return;
        }
        case PROJ_AGG_TS_LAST: {
            if (meta->ts_max > p->ts_last) {
                p->ts_last = meta->ts_max;
                if (ct == TSDB_TYPE_FLOAT64)
                    memcpy(&p->ts_last_f, &meta->stats_last, 8);
                else
                    p->ts_last_i = meta->stats_last;
            }
            return;
        }
        default:
            return; /* gate should have excluded these */
    }
}

/* Attempt the stats fast-path for a single source + projection set.
 * Returns 1 if the fast-path was taken (projections updated, caller
 * should skip the normal decode); 0 if we fell through and the caller
 * must run the regular scan.
 *
 * Shared between the parallel worker and the serial executor. */
static int try_stats_fastpath(scan_src_t *src,
                               proj_t *projs, int nprojs,
                               tsdb_schema_t *s,
                               const ts_range_t *ts_r,
                               int ts_contained_override)
{
    if (src->mem) return 0;

    /* Structural gate: every projection must be stats-serviceable. */
    for (int pi = 0; pi < nprojs; pi++) {
        proj_t *p = &projs[pi];
        if (p->kind < PROJ_AGG_RANGE_BEGIN || p->kind > PROJ_AGG_RANGE_END)
            return 0;
        uint16_t bits = 0; int eligible = 0;
        agg_stats_requires(p->kind, &bits, &eligible);
        if (!eligible) return 0;
    }

    int ts_contained = ts_contained_override;
    if (ts_contained < 0) {
        ts_contained = 1;
        if (ts_r && ts_r->has_lo && src->ts_min < ts_r->lo) ts_contained = 0;
        if (ts_r && ts_r->has_hi && src->ts_max > ts_r->hi) ts_contained = 0;
    }
    if (!ts_contained) return 0;

    /* Per-column meta lookups.  We cache the metas by col so each one
     * is fetched at most once per source. */
    tsdb_block_meta_t *col_hits[TSDB_MAX_COLS]       = {0};
    tsdb_block_meta_t *col_metas_arr[TSDB_MAX_COLS]  = {0};
    int ok = 1;
    for (int pi = 0; pi < nprojs && ok; pi++) {
        int c = projs[pi].col;
        if (c < 0 || col_hits[c]) continue;
        tsdb_block_meta_t *metas = NULL; size_t nb = 0;
        if (tsdb_part_col_blocks(src->part, c, &metas, &nb) != TSDB_OK) {
            ok = 0; break;
        }
        col_metas_arr[c] = metas;
        for (size_t b = 0; b < nb; b++) {
            if (metas[b].ts_min == src->meta.ts_min &&
                metas[b].count  == src->meta.count) {
                col_hits[c] = &metas[b]; break;
            }
        }
        if (!col_hits[c]) { ok = 0; break; }
    }
    if (ok) {
        for (int pi = 0; pi < nprojs && ok; pi++) {
            proj_t *p = &projs[pi];
            uint16_t bits = 0; int eligible = 0;
            agg_stats_requires(p->kind, &bits, &eligible);
            if (bits == 0) continue;              /* COUNT(*): no stats */
            if (p->col < 0) { ok = 0; break; }
            tsdb_block_meta_t *hit = col_hits[p->col];
            if (!hit || (hit->stats_flags & bits) != bits) { ok = 0; break; }
        }
    }

    if (ok) {
        for (int pi = 0; pi < nprojs; pi++) {
            proj_t *p = &projs[pi];
            tsdb_block_meta_t *hit = (p->col >= 0) ? col_hits[p->col] : NULL;
            tsdb_block_meta_t fb;
            if (!hit) { fb = src->meta; hit = &fb; }
            agg_apply_stats(p, s, hit, src->meta.count);
        }
    }

    for (int c = 0; c < TSDB_MAX_COLS; c++)
        if (col_metas_arr[c]) free(col_metas_arr[c]);
    return ok;
}

/* Worker function: scan the assigned sources and accumulate into private projs[]. */
void tsdb_par_scan_task(void *arg) {
    par_task_t *t = (par_task_t *)arg;
    t->rc = TSDB_OK;

    /* Per-worker scratch for SIMD gather (64 KB). Allocated once per task,
     * reused across every block this worker processes. */
    void *agg_scratch = aligned_alloc(32, (size_t)TSDB_BLOCK_POINTS * 8);
    if (!agg_scratch) { t->rc = TSDB_ERR_NOMEM; return; }

    /* Kill switch — TSDB_DISABLE_STATS_FASTPATH=1 forces the scan path for
     * every aggregate, so we can A/B against precomputed stats. */
    static int fastpath_disabled = -1;
    if (fastpath_disabled < 0) {
        const char *e = getenv("TSDB_DISABLE_STATS_FASTPATH");
        fastpath_disabled = (e && e[0] && e[0] != '0') ? 1 : 0;
    }

    /* Structural gate: every projection must be stats-serviceable and
     * there must be no non-ts WHERE.  If so, `stats_gate_ok` stays 1 and
     * we can try the fast path per-source. */
    int stats_gate_ok = !fastpath_disabled && (t->where == NULL);
    if (stats_gate_ok) {
        for (int pi = 0; pi < t->nprojs; pi++) {
            proj_t *p = &t->projs[pi];
            if (p->kind < PROJ_AGG_RANGE_BEGIN || p->kind > PROJ_AGG_RANGE_END) {
                stats_gate_ok = 0; break;
            }
            uint16_t bits = 0; int eligible = 0;
            agg_stats_requires(p->kind, &bits, &eligible);
            if (!eligible) { stats_gate_ok = 0; break; }
        }
    }

    /* Extract ts bounds once per worker; used to skip blocks entirely. */
    ts_range_t ts_r;
    ts_range_init(&ts_r);
    if (t->where) extract_ts_bounds(t->where, t->schema, &ts_r);

    for (size_t si = 0; si < t->nsrcs; si++) {
        scan_src_t *src = &t->srcs[si];
        size_t n = src->row_count;

        /* Block skip: if the whole source's [ts_min, ts_max] falls outside
         * the WHERE's ts range, skip without ever decoding. */
        if ((ts_r.has_lo || ts_r.has_hi) &&
            ts_range_excludes(&ts_r, src->ts_min, src->ts_max))
            continue;

        /* ---- Stats fast-path --------------------------------------
         * When the block is fully within the ts predicate and every
         * projection can be served from precomputed column stats we
         * skip decoding entirely.  Stats are an optimisation; a miss
         * simply falls through to the scan path below. */
        if (stats_gate_ok) {
            if (try_stats_fastpath(src, t->projs, t->nprojs, t->schema,
                                    &ts_r, /*ts_contained_override=*/-1)) {
                tsdb_metric_inc("qengine_agg_stats_hit_total");
                continue;
            }
            tsdb_metric_inc("qengine_agg_stats_miss_total");
        }

        void **bufs = calloc((size_t)t->schema->ncols, sizeof(void *));
        if (!bufs) { t->rc = TSDB_ERR_NOMEM; free(agg_scratch); return; }
        tsdb_symtab_t **syms = calloc((size_t)t->schema->ncols, sizeof(tsdb_symtab_t *));
        if (!syms) { free(bufs); t->rc = TSDB_ERR_NOMEM; free(agg_scratch); return; }

        int load_rc = TSDB_OK;
        for (int c = 0; c < t->schema->ncols; c++) {
            if (!t->need_col[c]) continue;
            syms[c] = t->schema->cols[c].symtab;
            size_t w = tsdb_type_width(t->schema->cols[c].type);
            if (src->mem) {
                bufs[c] = src->mem_bufs[c];
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
            free(agg_scratch);
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
            free(agg_scratch);
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
                free(agg_scratch);
                return;
            }
        }

        /* Update private aggregate state. */
        for (int pi = 0; pi < t->nprojs; pi++) {
            if (t->projs[pi].kind >= PROJ_AGG_RANGE_BEGIN && t->projs[pi].kind <= PROJ_AGG_RANGE_END)
                agg_update(&t->projs[pi], t->schema, bufs, n, bm, agg_scratch);
        }

        free(bm);
        for (int c = 0; c < t->schema->ncols; c++)
            if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
    }
    free(agg_scratch);
}

/* ---- LATEST ON execution ---------------------------------------------
 *
 *   SELECT cols FROM t LATEST ON ts [PARTITION BY a, b, ...]
 *
 * Semantics: return the row with the maximum ts per unique partition key.
 * Implementation: iterate scan sources in reverse order (newest partition
 * first, then in-memory memtable rows in reverse), iterate rows in reverse
 * within each source, and keep the first occurrence of each partition key
 * hash. Early-exit when LIMIT is met or no-partition case finds one row.
 */
static int exec_latest_on(tsdb_table_internal_t *tbl, qast_query_t *q,
                          tsdb_result_t *r, proj_t *projs, int nprojs,
                          char *err, size_t errcap) {
    tsdb_schema_t *s = tsdb_tbl_schema(tbl);

    int part_idx[TSDB_MAX_COLS];
    int npart = q->nlatest_part;
    for (int i = 0; i < npart; i++) {
        part_idx[i] = resolve_col(s, q->latest_part_cols[i]);
        if (part_idx[i] < 0) {
            eset(err, errcap, "unknown partition column '%s'",
                 q->latest_part_cols[i]);
            return TSDB_ERR_SCHEMA;
        }
    }

    /* LATEST ON may have a WHERE clause too — apply the zone-map prune so we
     * don't reverse-scan partitions entirely outside the range. */
    ts_range_t latest_ts_prune;
    ts_range_init(&latest_ts_prune);
    if (q->where) extract_ts_bounds(q->where, s, &latest_ts_prune);

    scan_plan_t plan = {0};
    int rc = scan_plan_build_ex(&plan, tbl,
                                 (latest_ts_prune.has_lo || latest_ts_prune.has_hi)
                                 ? &latest_ts_prune : NULL);
    if (rc != TSDB_OK) return rc;

    /* Columns to load: projections + partition cols. */
    int need_col[TSDB_MAX_COLS];
    memset(need_col, 0, sizeof(need_col));
    for (int i = 0; i < nprojs; i++) if (projs[i].col >= 0) need_col[projs[i].col] = 1;
    for (int i = 0; i < npart; i++) need_col[part_idx[i]] = 1;

    /* Seen-set: open-addressing linear probe of 64-bit hashes.
     * Value 0 = empty slot, so hash=0 is remapped to 1. */
    size_t cap = 256;
    uint64_t *seen = calloc(cap, sizeof(uint64_t));
    size_t seen_n = 0;
    size_t limit = q->has_limit ? (size_t)q->limit : SIZE_MAX;

    for (ssize_t si = (ssize_t)plan.nsrcs - 1; si >= 0 && r->nrows < limit; si--) {
        scan_src_t *src = &plan.srcs[si];
        size_t n = src->row_count;

        void **bufs = calloc((size_t)s->ncols, sizeof(void *));
        tsdb_symtab_t **syms = calloc((size_t)s->ncols, sizeof(tsdb_symtab_t *));
        if (!bufs || !syms) { free(bufs); free(syms); rc = TSDB_ERR_NOMEM; goto out; }

        int lrc = TSDB_OK;
        for (int c = 0; c < s->ncols; c++) {
            if (!need_col[c]) continue;
            syms[c] = s->cols[c].symtab;
            size_t w = tsdb_type_width(s->cols[c].type);
            if (src->mem) {
                bufs[c] = src->mem_bufs[c];
            } else {
                bufs[c] = malloc(w * n);
                if (!bufs[c]) { lrc = TSDB_ERR_NOMEM; break; }
                tsdb_block_meta_t *metas = NULL; size_t nb = 0;
                lrc = tsdb_part_col_blocks(src->part, c, &metas, &nb);
                if (lrc != TSDB_OK) break;
                tsdb_block_meta_t *hit = NULL;
                for (size_t b = 0; b < nb; b++)
                    if (metas[b].ts_min == src->meta.ts_min && metas[b].count == src->meta.count) {
                        hit = &metas[b]; break;
                    }
                if (!hit) { free(metas); lrc = TSDB_ERR_CORRUPT; break; }
                lrc = tsdb_part_read_block(src->part, c, hit, bufs[c]);
                free(metas);
                if (lrc != TSDB_OK) break;
            }
        }
        if (lrc != TSDB_OK) {
            for (int c = 0; c < s->ncols; c++) if (!src->mem && bufs[c]) free(bufs[c]);
            free(bufs); free(syms);
            rc = lrc; goto out;
        }

        for (ssize_t row = (ssize_t)n - 1; row >= 0 && r->nrows < limit; row--) {
            /* Compute partition key hash via FNV-1a over packed column bytes. */
            uint64_t key = 14695981039346656037ULL;
            if (npart == 0) {
                key = 0x00ababababababab; /* single bucket sentinel */
            } else {
                for (int i = 0; i < npart; i++) {
                    int ci = part_idx[i];
                    size_t w = tsdb_type_width(s->cols[ci].type);
                    const uint8_t *p = (const uint8_t *)bufs[ci] + (size_t)row * w;
                    for (size_t b = 0; b < w; b++) {
                        key ^= p[b];
                        key *= 1099511628211ULL;
                    }
                }
            }
            if (key == 0) key = 1;

            /* Grow set if load factor exceeds 0.5. */
            if (seen_n * 2 >= cap) {
                size_t ncap = cap * 2;
                uint64_t *ns = calloc(ncap, sizeof(uint64_t));
                if (!ns) { rc = TSDB_ERR_NOMEM; goto free_bufs; }
                for (size_t i = 0; i < cap; i++) {
                    if (!seen[i]) continue;
                    size_t pos = (size_t)seen[i] & (ncap - 1);
                    while (ns[pos]) pos = (pos + 1) & (ncap - 1);
                    ns[pos] = seen[i];
                }
                free(seen); seen = ns; cap = ncap;
            }

            size_t pos = (size_t)key & (cap - 1);
            int dup = 0;
            while (seen[pos]) {
                if (seen[pos] == key) { dup = 1; break; }
                pos = (pos + 1) & (cap - 1);
            }
            if (dup) continue;
            seen[pos] = key; seen_n++;

            /* Emit row. */
            rc = result_reserve_rows(r, r->nrows + 1);
            if (rc != TSDB_OK) goto free_bufs;

            for (int pi = 0; pi < nprojs; pi++) {
                proj_t *p = &projs[pi];
                if (p->kind == PROJ_COL) {
                    size_t w = tsdb_type_width(s->cols[p->col].type);
                    uint64_t bits = 0;
                    if (w == 8) bits = ((uint64_t *)bufs[p->col])[row];
                    else if (w == 4) bits = ((uint32_t *)bufs[p->col])[row];
                    result_append_cell(r, pi, bits);
                } else {
                    result_append_cell(r, pi, 0);
                }
            }
            r->nrows++;

            /* No-partition case: a single row is enough. */
            if (npart == 0) { rc = TSDB_OK; goto free_bufs_done; }
        }

free_bufs:
        for (int c = 0; c < s->ncols; c++) if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
        if (rc != TSDB_OK) goto out;
        continue;
free_bufs_done:
        for (int c = 0; c < s->ncols; c++) if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
        goto out;
    }

out:
    free(seen);
    scan_plan_free(&plan);
    return rc;
}

/* Forward declaration for DDL helper used by ASOF JOIN right SYMBOL columns. */
static tsdb_symtab_t *result_new_owned_symtab(tsdb_result_t *r);

/* ---- ASOF JOIN execution -----------------------------------------------
 *
 * SELECT ... FROM L ASOF JOIN R ON lk1=rk1 [, lk2=rk2]
 *
 * For each left row (ascending ts), find the right row with the largest
 * right.ts <= left.ts among rows where all ON key pairs match.
 * Right columns are encoded as 0 (NULL) when no match exists.
 *
 * Algorithm: materialise right table, then two-pointer with a per-key
 * cursor hashmap.  O(N log N) sort + O(N+M) scan.
 */
/* Per-key cursor: scan_pos is how far into the right table we've scanned
 * (advances monotonically as left_ts grows); best is the index of the
 * best-matching right row seen so far for this key (SIZE_MAX = no match). */
typedef struct { uint64_t key; size_t best; } asof_cursor_t;

typedef struct {
    size_t         nrows, ncols;
    int            ts_col;
    int64_t       *ts_buf;
    void         **col_bufs;
    tsdb_type_t   *col_types;
    tsdb_symtab_t **col_syms;
} right_mat_t;

static void right_mat_free(right_mat_t *m) {
    if (!m) return;
    free(m->ts_buf);
    if (m->col_bufs) { for (size_t c = 0; c < m->ncols; c++) free(m->col_bufs[c]); free(m->col_bufs); }
    free(m->col_types); free(m->col_syms);
}

static int right_mat_load_src(right_mat_t *m, scan_src_t *src, tsdb_schema_t *rs, size_t off) {
    size_t n = src->row_count;
    for (size_t c = 0; c < m->ncols; c++) {
        size_t w = tsdb_type_width(rs->cols[c].type);
        if (src->mem) {
            memcpy((char *)m->col_bufs[c] + off * w, src->mem_bufs[c], n * w);
        } else {
            tsdb_block_meta_t *metas = NULL; size_t nb = 0;
            int rc = tsdb_part_col_blocks(src->part, (int)c, &metas, &nb);
            if (rc != TSDB_OK) return rc;
            tsdb_block_meta_t *hit = NULL;
            for (size_t b = 0; b < nb; b++)
                if (metas[b].ts_min == src->meta.ts_min && metas[b].count == src->meta.count)
                    { hit = &metas[b]; break; }
            if (!hit) { free(metas); return TSDB_ERR_CORRUPT; }
            rc = tsdb_part_read_block(src->part, (int)c, hit, (char *)m->col_bufs[c] + off * w);
            free(metas);
            if (rc != TSDB_OK) return rc;
        }
    }
    return TSDB_OK;
}

static int right_mat_build(tsdb_table_internal_t *rtbl, right_mat_t *m) {
    tsdb_schema_t *rs = tsdb_tbl_schema(rtbl);
    m->ncols = (size_t)rs->ncols; m->ts_col = rs->ts_col_idx;
    m->col_types = calloc(m->ncols, sizeof(tsdb_type_t));
    m->col_syms  = calloc(m->ncols, sizeof(tsdb_symtab_t *));
    m->col_bufs  = calloc(m->ncols, sizeof(void *));
    m->ts_buf = NULL; m->nrows = 0;
    if (!m->col_types || !m->col_syms || !m->col_bufs) return TSDB_ERR_NOMEM;

    scan_plan_t rplan; memset(&rplan, 0, sizeof(rplan));
    int rc = scan_plan_build(&rplan, rtbl);
    if (rc != TSDB_OK) { right_mat_free(m); return rc; }

    size_t total = 0;
    for (size_t si = 0; si < rplan.nsrcs; si++) total += rplan.srcs[si].row_count;
    m->nrows = total;
    if (total == 0) { scan_plan_free(&rplan); return TSDB_OK; }

    for (size_t c = 0; c < m->ncols; c++) {
        m->col_types[c] = rs->cols[c].type; m->col_syms[c] = rs->cols[c].symtab;
        m->col_bufs[c] = malloc(tsdb_type_width(rs->cols[c].type) * total);
        if (!m->col_bufs[c]) { scan_plan_free(&rplan); right_mat_free(m); return TSDB_ERR_NOMEM; }
    }
    size_t off = 0;
    for (size_t si = 0; si < rplan.nsrcs; si++) {
        rc = right_mat_load_src(m, &rplan.srcs[si], rs, off);
        if (rc != TSDB_OK) { scan_plan_free(&rplan); right_mat_free(m); return rc; }
        off += rplan.srcs[si].row_count;
    }
    scan_plan_free(&rplan);

    m->ts_buf = malloc(total * sizeof(int64_t));
    if (!m->ts_buf) { right_mat_free(m); return TSDB_ERR_NOMEM; }
    memcpy(m->ts_buf, m->col_bufs[m->ts_col], total * sizeof(int64_t));

    /* Sort columns together by ts if not already sorted. */
    int sorted = 1;
    for (size_t i = 1; i < total; i++) if (m->ts_buf[i] < m->ts_buf[i-1]) { sorted = 0; break; }
    if (!sorted) {
        size_t *idx = malloc(total * sizeof(size_t));
        if (!idx) { right_mat_free(m); return TSDB_ERR_NOMEM; }
        for (size_t i = 0; i < total; i++) idx[i] = i;
        int64_t *ts = m->ts_buf;
        for (size_t i = 1; i < total; i++) {   /* insertion sort, fast for nearly-sorted */
            size_t x = idx[i]; int64_t tv = ts[x]; size_t j = i;
            while (j > 0 && ts[idx[j-1]] > tv) { idx[j] = idx[j-1]; j--; }
            idx[j] = x;
        }
        void *tmp = malloc(total * 8);
        if (!tmp) { free(idx); right_mat_free(m); return TSDB_ERR_NOMEM; }
        for (size_t c = 0; c < m->ncols; c++) {
            size_t w = tsdb_type_width(m->col_types[c]);
            for (size_t i = 0; i < total; i++)
                memcpy((char *)tmp + i * w, (char *)m->col_bufs[c] + idx[i] * w, w);
            memcpy(m->col_bufs[c], tmp, total * w);
        }
        memcpy(m->ts_buf, m->col_bufs[m->ts_col], total * sizeof(int64_t));
        free(tmp); free(idx);
    }
    return TSDB_OK;
}

static uint64_t asof_rkey_hash(right_mat_t *m, size_t row, int *rcol_idx, int nkeys) {
    uint64_t h = 14695981039346656037ULL;
    for (int k = 0; k < nkeys; k++) {
        int c = rcol_idx[k]; tsdb_type_t t = m->col_types[c]; size_t w = tsdb_type_width(t);
        if (t == TSDB_TYPE_SYMBOL && m->col_syms[c]) {
            uint32_t code; memcpy(&code, (const uint8_t *)m->col_bufs[c] + row * w, 4);
            const char *s = tsdb_symtab_str(m->col_syms[c], code); if (!s) s = "";
            for (; *s; s++) { h ^= (uint8_t)*s; h *= 1099511628211ULL; }
            h ^= 0u; h *= 1099511628211ULL;
        } else {
            const uint8_t *p = (const uint8_t *)m->col_bufs[c] + row * w;
            for (size_t i = 0; i < w; i++) { h ^= p[i]; h *= 1099511628211ULL; }
        }
    }
    return h ? h : 1;
}

static uint64_t asof_lkey_hash(tsdb_schema_t *ls, void **lb, size_t row, int *lcol_idx, int nkeys) {
    uint64_t h = 14695981039346656037ULL;
    for (int k = 0; k < nkeys; k++) {
        int c = lcol_idx[k]; tsdb_type_t t = ls->cols[c].type; size_t w = tsdb_type_width(t);
        if (t == TSDB_TYPE_SYMBOL && ls->cols[c].symtab) {
            uint32_t code; memcpy(&code, (const uint8_t *)lb[c] + row * w, 4);
            const char *s = tsdb_symtab_str(ls->cols[c].symtab, code); if (!s) s = "";
            for (; *s; s++) { h ^= (uint8_t)*s; h *= 1099511628211ULL; }
            h ^= 0u; h *= 1099511628211ULL;
        } else {
            const uint8_t *p = (const uint8_t *)lb[c] + row * w;
            for (size_t i = 0; i < w; i++) { h ^= p[i]; h *= 1099511628211ULL; }
        }
    }
    return h ? h : 1;
}

static int asof_keys_eq(tsdb_schema_t *ls, void **lb, size_t lr, int *lcol_idx,
                         right_mat_t *m, size_t rr, int *rcol_idx, int nkeys) {
    for (int k = 0; k < nkeys; k++) {
        int lc = lcol_idx[k], rc2 = rcol_idx[k];
        tsdb_type_t lt = ls->cols[lc].type, rt = m->col_types[rc2];
        size_t lw = tsdb_type_width(lt), rw = tsdb_type_width(rt);
        if (lt == TSDB_TYPE_SYMBOL && rt == TSDB_TYPE_SYMBOL) {
            uint32_t a, b2;
            memcpy(&a, (const uint8_t *)lb[lc] + lr * lw, 4);
            memcpy(&b2, (const uint8_t *)m->col_bufs[rc2] + rr * rw, 4);
            const char *sa = tsdb_symtab_str(ls->cols[lc].symtab, a); if (!sa) sa = "";
            const char *sb = tsdb_symtab_str(m->col_syms[rc2], b2); if (!sb) sb = "";
            if (strcmp(sa, sb) != 0) return 0;
        } else {
            if (memcmp((const uint8_t *)lb[lc] + lr * lw,
                       (const uint8_t *)m->col_bufs[rc2] + rr * rw,
                       lw < rw ? lw : rw) != 0) return 0;
        }
    }
    return 1;
}

static asof_cursor_t *asof_slot(asof_cursor_t *map, size_t cap, uint64_t key) {
    size_t pos = (size_t)key & (cap - 1);
    while (map[pos].key && map[pos].key != key) pos = (pos + 1) & (cap - 1);
    return &map[pos];
}

static int asof_grow(asof_cursor_t **pm, size_t *pc) {
    size_t nc = (*pc) * 2;
    asof_cursor_t *nm = calloc(nc, sizeof(asof_cursor_t));
    if (!nm) return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < *pc; i++)
        if ((*pm)[i].key) *asof_slot(nm, nc, (*pm)[i].key) = (*pm)[i];
    free(*pm); *pm = nm; *pc = nc;
    return TSDB_OK;
}

/* Flag for right-side projection columns encoded in proj_t.col. */
#define PROJ_RFLAG  0x8000
#define PROJ_IS_R(p) ((p)->col & PROJ_RFLAG)
#define PROJ_RC(p)   ((p)->col & ~PROJ_RFLAG)

static int build_projs_asof(qast_query_t *q, tsdb_schema_t *ls, tsdb_schema_t *rs,
                             proj_t **out, int *out_n, char *err, size_t errcap) {
    proj_t *arr = NULL; int cap = 0, n = 0;
    for (int i = 0; i < q->nsel; i++) {
        qast_sel_item_t *si = &q->sel[i];
        if (si->is_star) {
            for (int c = 0; c < ls->ncols; c++) {
                if (n >= cap) { cap = cap ? cap*2 : 8; arr = realloc(arr, (size_t)cap*sizeof(*arr)); }
                memset(&arr[n], 0, sizeof(arr[n]));
                arr[n].kind = PROJ_COL; arr[n].col = c; arr[n].out_type = ls->cols[c].type;
                snprintf(arr[n].name, sizeof(arr[n].name), "%s", ls->cols[c].name); n++;
            }
            for (int c = 0; c < rs->ncols; c++) {
                if (n >= cap) { cap = cap ? cap*2 : 8; arr = realloc(arr, (size_t)cap*sizeof(*arr)); }
                memset(&arr[n], 0, sizeof(arr[n]));
                arr[n].kind = PROJ_COL; arr[n].col = c | PROJ_RFLAG; arr[n].out_type = rs->cols[c].type;
                snprintf(arr[n].name, sizeof(arr[n].name), "%s", rs->cols[c].name); n++;
            }
            continue;
        }
        if (n >= cap) { cap = cap ? cap*2 : 8; arr = realloc(arr, (size_t)cap*sizeof(*arr)); }
        memset(&arr[n], 0, sizeof(arr[n]));
        qast_expr_t *e = si->expr;
        if (e->kind != QAST_IDENT) { eset(err, errcap, "ASOF JOIN: only column references supported"); free(arr); return TSDB_ERR_UNSUPPORTED; }
        int lc = resolve_col(ls, e->v.s);
        if (lc >= 0) {
            arr[n].kind = PROJ_COL; arr[n].col = lc; arr[n].out_type = ls->cols[lc].type;
            snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias ? si->alias : ls->cols[lc].name);
        } else {
            int rc2 = resolve_col(rs, e->v.s);
            if (rc2 < 0) { eset(err, errcap, "unknown column '%s'", e->v.s); free(arr); return TSDB_ERR_SCHEMA; }
            arr[n].kind = PROJ_COL; arr[n].col = rc2 | PROJ_RFLAG; arr[n].out_type = rs->cols[rc2].type;
            snprintf(arr[n].name, sizeof(arr[n].name), "%s", si->alias ? si->alias : rs->cols[rc2].name);
        }
        n++;
    }
    *out = arr; *out_n = n;
    return TSDB_OK;
}

static int exec_asof_join(tsdb_db_t *db, tsdb_table_internal_t *ltbl,
                          qast_query_t *q, tsdb_result_t *r,
                          char *err, size_t errcap) {
    tsdb_schema_t *ls = tsdb_tbl_schema(ltbl);
    int rc = TSDB_OK;

    tsdb_table_internal_t *rtbl = tsdb_db_find_table(db, q->asof_table);
    if (!rtbl) {
        tsdb_table_t *h = NULL;
        rc = tsdb_open_table(db, q->asof_table, &h);
        if (rc != TSDB_OK) { eset(err, errcap, "ASOF JOIN: right table '%s' not found", q->asof_table); return rc; }
        rtbl = tsdb_db_find_table(db, q->asof_table);
        if (!rtbl) { eset(err, errcap, "ASOF JOIN: right table '%s' not found", q->asof_table); return TSDB_ERR_NOTFOUND; }
    }
    tsdb_schema_t *rs = tsdb_tbl_schema(rtbl);

    int nkeys = q->asof_n_keys, lcol_idx[32], rcol_idx[32];
    for (int k = 0; k < nkeys; k++) {
        lcol_idx[k] = resolve_col(ls, q->asof_on_keys_l[k]);
        rcol_idx[k] = resolve_col(rs, q->asof_on_keys_r[k]);
        if (lcol_idx[k] < 0) { eset(err, errcap, "ASOF ON: unknown left column '%s'", q->asof_on_keys_l[k]); return TSDB_ERR_SCHEMA; }
        if (rcol_idx[k] < 0) { eset(err, errcap, "ASOF ON: unknown right column '%s'", q->asof_on_keys_r[k]); return TSDB_ERR_SCHEMA; }
    }

    proj_t *projs = NULL; int nprojs = 0;
    rc = build_projs_asof(q, ls, rs, &projs, &nprojs, err, errcap);
    if (rc != TSDB_OK) return rc;

    rc = result_reserve_cols(r, nprojs);
    if (rc != TSDB_OK) { free(projs); return rc; }
    for (int i = 0; i < nprojs; i++) {
        int is_r = PROJ_IS_R(&projs[i]);
        tsdb_schema_t *sch = is_r ? rs : ls;
        int ci = is_r ? PROJ_RC(&projs[i]) : projs[i].col;
        tsdb_symtab_t *sym = (sch->cols[ci].type == TSDB_TYPE_SYMBOL) ? sch->cols[ci].symtab : NULL;
        if (is_r && sym) {
            tsdb_symtab_t *owned = result_new_owned_symtab(r);
            if (!owned) { free(projs); return TSDB_ERR_NOMEM; }
            rc = result_set_col(r, i, projs[i].name, projs[i].out_type, owned);
        } else {
            rc = result_set_col(r, i, projs[i].name, projs[i].out_type, sym);
        }
        if (rc != TSDB_OK) { free(projs); return rc; }
    }

    right_mat_t rm; memset(&rm, 0, sizeof(rm));
    rc = right_mat_build(rtbl, &rm);
    if (rc != TSDB_OK) { free(projs); return rc; }

    scan_plan_t lplan; memset(&lplan, 0, sizeof(lplan));
    rc = scan_plan_build(&lplan, ltbl);
    if (rc != TSDB_OK) { right_mat_free(&rm); free(projs); return rc; }

    size_t cursor_cap = 256;
    asof_cursor_t *cursor_map = calloc(cursor_cap, sizeof(asof_cursor_t));
    if (!cursor_map) { rc = TSDB_ERR_NOMEM; goto done; }
    size_t cursor_n = 0;
    /* Global scan pointer into right table — advances monotonically as left_ts grows. */
    size_t rscan = 0;

    int need_lcol[TSDB_MAX_COLS]; memset(need_lcol, 0, sizeof(need_lcol));
    for (int i = 0; i < nprojs; i++)
        if (!PROJ_IS_R(&projs[i]) && projs[i].col >= 0) need_lcol[projs[i].col] = 1;
    for (int k = 0; k < nkeys; k++) need_lcol[lcol_idx[k]] = 1;
    need_lcol[ls->ts_col_idx] = 1;

    size_t limit = q->has_limit ? (size_t)q->limit : SIZE_MAX;

    for (size_t si = 0; si < lplan.nsrcs && r->nrows < limit; si++) {
        scan_src_t *src = &lplan.srcs[si];
        size_t ln = src->row_count;

        void **lbufs = calloc((size_t)ls->ncols, sizeof(void *));
        if (!lbufs) { rc = TSDB_ERR_NOMEM; goto done; }
        int lrc = TSDB_OK;
        for (int c = 0; c < ls->ncols; c++) {
            if (!need_lcol[c]) continue;
            size_t w = tsdb_type_width(ls->cols[c].type);
            if (src->mem) {
                lbufs[c] = src->mem_bufs[c];
            } else {
                lbufs[c] = malloc(w * ln);
                if (!lbufs[c]) { lrc = TSDB_ERR_NOMEM; break; }
                tsdb_block_meta_t *metas = NULL; size_t nb = 0;
                lrc = tsdb_part_col_blocks(src->part, c, &metas, &nb);
                if (lrc != TSDB_OK) break;
                tsdb_block_meta_t *hit = NULL;
                for (size_t b = 0; b < nb; b++)
                    if (metas[b].ts_min == src->meta.ts_min && metas[b].count == src->meta.count)
                        { hit = &metas[b]; break; }
                if (!hit) { free(metas); lrc = TSDB_ERR_CORRUPT; break; }
                lrc = tsdb_part_read_block(src->part, c, hit, lbufs[c]);
                free(metas);
                if (lrc != TSDB_OK) break;
            }
        }
        if (lrc != TSDB_OK) {
            for (int c = 0; c < ls->ncols; c++) if (!src->mem && lbufs[c]) free(lbufs[c]);
            free(lbufs); rc = lrc; goto done;
        }

        const int64_t *lts = (const int64_t *)lbufs[ls->ts_col_idx];

        for (size_t li = 0; li < ln && r->nrows < limit; li++) {
            int64_t left_ts = lts[li];
            uint64_t khash = (nkeys > 0)
                ? asof_lkey_hash(ls, lbufs, li, lcol_idx, nkeys)
                : 0xababababababababULL;

            /* Advance global rscan pointer up to left_ts, updating per-key best match. */
            while (rscan < rm.nrows && rm.ts_buf[rscan] <= left_ts) {
                /* Grow before lookup, not after, to avoid stale pointer. */
                if (cursor_n * 2 >= cursor_cap) {
                    int grc = asof_grow(&cursor_map, &cursor_cap);
                    if (grc != TSDB_OK) { rc = grc; goto free_lbufs; }
                }
                /* Compute key hash for this right row. */
                uint64_t rkhash = (nkeys > 0)
                    ? asof_rkey_hash(&rm, rscan, rcol_idx, nkeys)
                    : 0xababababababababULL;
                asof_cursor_t *rslot = asof_slot(cursor_map, cursor_cap, rkhash);
                if (!rslot->key) { rslot->key = rkhash; rslot->best = SIZE_MAX; cursor_n++; }
                rslot->best = rscan;  /* always take the latest row (higher ts is better) */
                rscan++;
            }

            /* Look up cursor for this left row's key hash. */
            if (cursor_n * 2 >= cursor_cap) {
                int grc = asof_grow(&cursor_map, &cursor_cap);
                if (grc != TSDB_OK) { rc = grc; goto free_lbufs; }
            }
            asof_cursor_t *slot = asof_slot(cursor_map, cursor_cap, khash);
            if (!slot->key) { slot->key = khash; slot->best = SIZE_MAX; cursor_n++; }

            size_t rpos = slot->best;
            int have_match = (rpos != SIZE_MAX && rm.ts_buf[rpos] <= left_ts &&
                ((nkeys == 0) ? 1 : asof_keys_eq(ls, lbufs, li, lcol_idx, &rm, rpos, rcol_idx, nkeys)));

            rc = result_reserve_rows(r, r->nrows + 1);
            if (rc != TSDB_OK) goto free_lbufs;

            for (int pi = 0; pi < nprojs; pi++) {
                proj_t *p = &projs[pi];
                if (p->kind != PROJ_COL) { result_append_cell(r, pi, 0); continue; }
                if (!PROJ_IS_R(p)) {
                    int lc = p->col; size_t w = tsdb_type_width(ls->cols[lc].type);
                    uint64_t bits = 0;
                    if (w == 8) bits = ((const uint64_t *)lbufs[lc])[li];
                    else if (w == 4) bits = ((const uint32_t *)lbufs[lc])[li];
                    result_append_cell(r, pi, bits);
                } else {
                    if (!have_match) { result_append_cell(r, pi, 0); continue; }
                    int rc2 = PROJ_RC(p); tsdb_type_t rt = rm.col_types[rc2]; size_t w = tsdb_type_width(rt);
                    if (rt == TSDB_TYPE_SYMBOL && rm.col_syms[rc2]) {
                        uint32_t rcode; memcpy(&rcode, (const uint8_t *)rm.col_bufs[rc2] + rpos * w, 4);
                        const char *s = tsdb_symtab_str(rm.col_syms[rc2], rcode);
                        uint32_t ocode = tsdb_symtab_intern(r->col_symtab[pi], s ? s : "");
                        result_append_cell(r, pi, (uint64_t)ocode);
                    } else {
                        uint64_t bits = 0;
                        if (w == 8) memcpy(&bits, (const uint8_t *)rm.col_bufs[rc2] + rpos * 8, 8);
                        else if (w == 4) { uint32_t v; memcpy(&v, (const uint8_t *)rm.col_bufs[rc2] + rpos * 4, 4); bits = v; }
                        result_append_cell(r, pi, bits);
                    }
                }
            }
            r->nrows++;
        }
free_lbufs:
        for (int c = 0; c < ls->ncols; c++) if (!src->mem && lbufs[c]) free(lbufs[c]);
        free(lbufs);
        if (rc != TSDB_OK) goto done;
    }
done:
    free(cursor_map);
    right_mat_free(&rm);
    scan_plan_free(&lplan);
    free(projs);
    return rc;
}

/* ---- GROUP BY hash-aggregate executor ---------------------------------
 *
 * Triggered by `SELECT ... GROUP BY col1 [, col2, ...]` without SAMPLE BY.
 * Maintains one aggregate-state slot per unique group-key tuple, using a
 * flat open-addressing hashmap keyed by FNV-1a over the packed group-key
 * bytes. One row per group emitted at end, respecting LIMIT.
 *
 * Design decisions:
 *   - Group state is a `proj_t[nprojs]` heap array, deep-copied from the
 *     master projs[] (so t-digest aggregates clone correctly).
 *   - Hash table: power-of-two, 50% load-factor rehash threshold.
 *   - Key tuple: for each GROUP BY col, pack 8 bytes (i64/ts/f64/sym-code
 *     as uint32 cast to uint64). Hash those bytes via FNV-1a.
 *   - Collision handling: store full key tuple in group to verify on
 *     lookup (hash collision + dup key are distinct).
 *   - Output schema: (group keys in declared order) + (agg expressions
 *     as they appear in SELECT list). Other SELECT items are rejected.
 *
 * Only the serial path is implemented; parallel hash-agg is planned for
 * v0.8. For the workloads we target (TSBS double-groupby-* with ≤ dozens
 * of groups, ingest-side rate matters more than query parallelism), this
 * is sufficient.
 */

typedef struct gb_slot {
    int       occupied;
    uint64_t  key_hash;
    uint64_t  key_tuple[TSDB_MAX_COLS];  /* packed group-key values */
    int       n_key;
    proj_t   *state;                     /* nprojs-sized clone */
} gb_slot_t;

/* GROUP BY key hash.
 *
 * Was FNV-1a (8 cycles per byte, scalar).  Replaced with hardware
 * CRC32C (1 cycle per 8 bytes on x86 SSE4.2 / arm64 CRC ext) plus a
 * Knuth multiplicative spread to fill the high 32 bits.  CRC32C alone
 * is fine for bucket selection (probe path verifies via memcmp on the
 * full key tuple) but we still want a 64-bit hash so the cheap
 * `s->key_hash == hash` early-out filter has discriminating power.
 *
 * Why this matters: the GROUP BY hot loop hashes once per row and the
 * old implementation was a tight 8-iteration FNV loop per 8-byte key
 * column — meaningful overhead for 1M-row scans.  The hardware path
 * cuts the hash cost ~8x without changing collision semantics. */
static uint64_t gb_hash(const uint8_t *buf, size_t n) {
    uint32_t c = tsdb_crc32c(buf, n);
    /* Spread 32 bits to 64 via golden-ratio multiply — good distribution
     * for the high bits used by the early-out comparator. */
    return (uint64_t)c * 0x9E3779B97F4A7C15ULL;
}

/* Allocate a hash-table of capacity `cap` (power of two). */
static gb_slot_t *gb_alloc(size_t cap) {
    gb_slot_t *t = calloc(cap, sizeof(gb_slot_t));
    return t;
}

static void gb_slot_free_state(gb_slot_t *s, int nprojs) {
    if (!s->state) return;
    for (int i = 0; i < nprojs; i++) {
        if (s->state[i].tdigest) tsdb_tdigest_free(s->state[i].tdigest);
    }
    free(s->state);
    s->state = NULL;
}

/* Look up (or insert) a slot for key_tuple. Returns the slot. */
static gb_slot_t *gb_upsert(gb_slot_t *tbl, size_t cap,
                            const uint64_t *key, int nk,
                            uint64_t hash, int nprojs,
                            const proj_t *master_projs, size_t *nused)
{
    size_t mask = cap - 1;
    size_t i = (size_t)hash & mask;
    for (;;) {
        gb_slot_t *s = &tbl[i];
        if (!s->occupied) {
            /* Insert. */
            s->occupied = 1;
            s->key_hash = hash;
            s->n_key    = nk;
            memcpy(s->key_tuple, key, (size_t)nk * sizeof(uint64_t));
            s->state = malloc((size_t)nprojs * sizeof(proj_t));
            if (!s->state) return NULL;
            memcpy(s->state, master_projs, (size_t)nprojs * sizeof(proj_t));
            /* Clone t-digests; base proj_t copy shares pointer otherwise. */
            for (int p = 0; p < nprojs; p++) {
                if (master_projs[p].tdigest) {
                    s->state[p].tdigest = NULL;
                    tsdb_tdigest_clone(master_projs[p].tdigest, &s->state[p].tdigest);
                }
            }
            (*nused)++;
            return s;
        }
        if (s->key_hash == hash && s->n_key == nk
            && memcmp(s->key_tuple, key, (size_t)nk * sizeof(uint64_t)) == 0) {
            return s;
        }
        i = (i + 1) & mask;
    }
}

/* Grow the hash table 2× when load factor exceeds 0.5. */
static int gb_grow(gb_slot_t **tbl, size_t *cap, size_t nused, int nprojs) {
    (void)nused; (void)nprojs;
    size_t ncap = (*cap) * 2;
    gb_slot_t *nt = gb_alloc(ncap);
    if (!nt) return TSDB_ERR_NOMEM;
    size_t mask = ncap - 1;
    for (size_t i = 0; i < *cap; i++) {
        gb_slot_t *s = &(*tbl)[i];
        if (!s->occupied) continue;
        size_t j = (size_t)s->key_hash & mask;
        while (nt[j].occupied) j = (j + 1) & mask;
        nt[j] = *s;   /* move — do not free state */
    }
    free(*tbl);
    *tbl = nt;
    *cap = ncap;
    return TSDB_OK;
}

/*
 * Merge `src` hash table into `dst` hash table.
 * For each occupied slot in src, find the matching slot in dst (insert if
 * absent) and combine the aggregate states.
 * Both tables must have been built with the same key schema (nk) and the
 * same projection list (nprojs, projs).
 * Returns TSDB_OK on success, TSDB_ERR_NOMEM on allocation failure.
 */
static int gb_merge_into(gb_slot_t **dst_tbl, size_t *dst_cap, size_t *dst_nused,
                          const gb_slot_t *src_tbl, size_t src_cap,
                          int nprojs, const proj_t *master_projs)
{
    for (size_t si = 0; si < src_cap; si++) {
        const gb_slot_t *ss = &src_tbl[si];
        if (!ss->occupied) continue;

        /* Grow dst if needed (50% load factor). */
        if (*dst_nused * 2 >= *dst_cap) {
            if (gb_grow(dst_tbl, dst_cap, *dst_nused, nprojs) != TSDB_OK)
                return TSDB_ERR_NOMEM;
        }

        /* Find-or-insert into dst. */
        gb_slot_t *ds = gb_upsert(*dst_tbl, *dst_cap,
                                   ss->key_tuple, ss->n_key,
                                   ss->key_hash, nprojs, master_projs, dst_nused);
        if (!ds) return TSDB_ERR_NOMEM;

        /* Merge aggregate states. */
        for (int p = 0; p < nprojs; p++) {
            proj_t *dp = &ds->state[p];
            const proj_t *sp = &ss->state[p];
            switch (dp->kind) {
            case PROJ_AGG_COUNT:
                dp->agg_count += sp->agg_count;
                break;
            case PROJ_AGG_SUM:
                dp->agg_sum_f  += sp->agg_sum_f;
                dp->agg_sum_i  += sp->agg_sum_i;
                dp->agg_count  += sp->agg_count;
                break;
            case PROJ_AGG_AVG:
                dp->agg_sum_f  += sp->agg_sum_f;
                dp->agg_sum_i  += sp->agg_sum_i;
                dp->agg_count  += sp->agg_count;
                break;
            case PROJ_AGG_MIN:
                if (sp->agg_min_f < dp->agg_min_f) dp->agg_min_f = sp->agg_min_f;
                if (sp->agg_min_i < dp->agg_min_i) dp->agg_min_i = sp->agg_min_i;
                break;
            case PROJ_AGG_MAX:
                if (sp->agg_max_f > dp->agg_max_f) dp->agg_max_f = sp->agg_max_f;
                if (sp->agg_max_i > dp->agg_max_i) dp->agg_max_i = sp->agg_max_i;
                break;
            case PROJ_AGG_SPREAD:
                if (sp->agg_min_f < dp->agg_min_f) dp->agg_min_f = sp->agg_min_f;
                if (sp->agg_min_i < dp->agg_min_i) dp->agg_min_i = sp->agg_min_i;
                if (sp->agg_max_f > dp->agg_max_f) dp->agg_max_f = sp->agg_max_f;
                if (sp->agg_max_i > dp->agg_max_i) dp->agg_max_i = sp->agg_max_i;
                dp->agg_count  += sp->agg_count;
                break;
            case PROJ_AGG_P50:
            case PROJ_AGG_P90:
            case PROJ_AGG_P99:
            case PROJ_AGG_PERCENTILE:
            case PROJ_AGG_STDDEV:
                if (dp->tdigest && sp->tdigest)
                    tsdb_tdigest_merge(dp->tdigest, sp->tdigest);
                break;
            case PROJ_AGG_TS_FIRST:
                if (sp->ts_first < dp->ts_first) {
                    dp->ts_first   = sp->ts_first;
                    dp->ts_first_f = sp->ts_first_f;
                    dp->ts_first_i = sp->ts_first_i;
                }
                break;
            case PROJ_AGG_TS_LAST:
            case PROJ_AGG_TS_LAST_ROW:
                if (sp->ts_last > dp->ts_last) {
                    dp->ts_last   = sp->ts_last;
                    dp->ts_last_f = sp->ts_last_f;
                    dp->ts_last_i = sp->ts_last_i;
                }
                break;
            case PROJ_AGG_TS_TWA:
                dp->twa_wsum += sp->twa_wsum;
                /* TWA merge is approximate (no overlap handling); acceptable for
                 * partition-parallel where sources are disjoint time ranges. */
                if (sp->ts_first < dp->ts_first) dp->ts_first = sp->ts_first;
                if (sp->ts_last  > dp->ts_last)  dp->ts_last  = sp->ts_last;
                break;
            default:
                break; /* PROJ_COL: key column, not merged */
            }
        }
    }
    return TSDB_OK;
}

/* ---- Parallel GROUP BY task ------------------------------------------- */
/*
 * Each worker receives a contiguous slice of scan sources, runs the full
 * hash-agg loop over its slice into a private hash table, then the main
 * thread merges all per-worker tables into one final table.
 */
typedef struct {
    /* Input — read-only in worker. */
    scan_src_t    *srcs;
    size_t         nsrcs;
    tsdb_schema_t *schema;
    qast_expr_t   *where;
    int            need_col[TSDB_MAX_COLS];
    int            gkey_cols[TSDB_MAX_COLS]; /* GROUP BY column indices */
    int            ngroup_by;
    const proj_t  *master_projs;

    /* Output — written by worker. */
    int            nprojs;
    gb_slot_t     *ht;
    size_t         ht_cap;
    size_t         ht_nused;

    /* Error. */
    int            rc;
    char           err[256];
} gbpar_task_t;

/* Optional in-loop profiling — gated by env TSDB_GB_PROFILE.  Tracks
 * which of (key gather, hash, upsert+agg) dominates the per-row cost
 * so the planned SIMD batching effort can target the right segment.
 * Zero overhead when off (single load + branch per worker invocation). */
static __thread struct {
    int      enabled;
    int64_t  ns_keygather;
    int64_t  ns_hash;
    int64_t  ns_upsert_agg;
    int64_t  rows;
} gb_prof = {0};

static int64_t gb_prof_now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void tsdb_gbpar_scan_task(void *arg) {
    gbpar_task_t *t = (gbpar_task_t *)arg;
    t->rc = TSDB_OK;

    tsdb_schema_t *s = t->schema;
    int nprojs = t->nprojs;

    {
        const char *e = getenv("TSDB_GB_PROFILE");
        gb_prof.enabled = (e && *e == '1');
    }

    void *agg_scratch = aligned_alloc(32, (size_t)TSDB_BLOCK_POINTS * 8);
    if (!agg_scratch) { t->rc = TSDB_ERR_NOMEM; return; }

    for (size_t si = 0; si < t->nsrcs; si++) {
        scan_src_t *src = &t->srcs[si];
        size_t n = src->row_count;

        /* Decode needed columns. */
        void **bufs = calloc((size_t)s->ncols, sizeof(void *));
        tsdb_symtab_t **syms = calloc((size_t)s->ncols, sizeof(tsdb_symtab_t *));
        if (!bufs || !syms) { free(bufs); free(syms); t->rc = TSDB_ERR_NOMEM; break; }

        int local_rc = TSDB_OK;
        for (int c = 0; c < s->ncols; c++) {
            if (!t->need_col[c]) continue;
            syms[c] = s->cols[c].symtab;
            size_t w = tsdb_type_width(s->cols[c].type);
            if (src->mem) {
                bufs[c] = src->mem_bufs[c];
            } else {
                bufs[c] = malloc(w * n);
                if (!bufs[c]) { local_rc = TSDB_ERR_NOMEM; break; }
                tsdb_block_meta_t *metas = NULL; size_t nb = 0;
                local_rc = tsdb_part_col_blocks(src->part, c, &metas, &nb);
                if (local_rc != TSDB_OK) break;
                tsdb_block_meta_t *hit = NULL;
                for (size_t b = 0; b < nb; b++)
                    if (metas[b].ts_min == src->meta.ts_min &&
                        metas[b].count  == src->meta.count) {
                        hit = &metas[b]; break;
                    }
                if (!hit) { free(metas); local_rc = TSDB_ERR_CORRUPT; break; }
                local_rc = tsdb_part_read_block(src->part, c, hit, bufs[c]);
                free(metas);
                if (local_rc != TSDB_OK) break;
            }
        }
        if (local_rc != TSDB_OK) {
            for (int c = 0; c < s->ncols; c++)
                if (!src->mem && bufs[c]) free(bufs[c]);
            free(bufs); free(syms);
            t->rc = local_rc;
            break;
        }

        /* WHERE bitmap. */
        size_t nw = (n + 63) / 64;
        uint64_t *bm = malloc(nw * sizeof(uint64_t));
        if (!bm) {
            for (int c = 0; c < s->ncols; c++)
                if (!src->mem && bufs[c]) free(bufs[c]);
            free(bufs); free(syms);
            t->rc = TSDB_ERR_NOMEM;
            break;
        }
        for (size_t i = 0; i < nw; i++) bm[i] = ~(uint64_t)0;
        size_t tail = nw * 64 - n;
        if (tail) bm[nw - 1] &= (~(uint64_t)0) >> tail;

        if (t->where) {
            eval_ctx_t ctx = {0};
            ctx.schema = s; ctx.col_bufs = bufs; ctx.col_syms = syms; ctx.nrows = n;
            local_rc = apply_filter_expr(&ctx, t->where, bm);
            if (local_rc != TSDB_OK) {
                free(bm);
                for (int c = 0; c < s->ncols; c++)
                    if (!src->mem && bufs[c]) free(bufs[c]);
                free(bufs); free(syms);
                t->rc = local_rc;
                break;
            }
        }

        /* Per-row hash-aggregate into the worker's private hash table.
         *
         * Hot-loop optimisation (informed by [gb-profile] data showing
         * upsert+agg = 60-89 % of total): hoist the 256-byte
         * one_bufs[] memset and the constant one_bm/one_n out of the
         * inner per-projection loop.  agg_update only READS bufs[],
         * never writes; we set the at-most-2 indices it actually
         * consults per call, then null them back so the next
         * iteration starts clean.  Removes ~512 B/(row × proj) of
         * memset traffic — for a 4-agg query × 8.6 M rows that's
         * ~17 GB of zeroing erased from the steady-state path. */
        uint64_t one_bm = 1;
        size_t   one_n  = 1;
        void    *one_bufs[TSDB_MAX_COLS];
        memset(one_bufs, 0, sizeof(one_bufs));

        for (size_t row = 0; row < n; row++) {
            if (!(bm[row / 64] & ((uint64_t)1 << (row % 64)))) continue;

            int64_t t0 = gb_prof.enabled ? gb_prof_now_ns() : 0;
            uint64_t key_tuple[TSDB_MAX_COLS];
            for (int g = 0; g < t->ngroup_by; g++) {
                int c = t->gkey_cols[g];
                size_t w = tsdb_type_width(s->cols[c].type);
                uint64_t val = 0;
                if (w == 8) val = ((const uint64_t *)bufs[c])[row];
                else if (w == 4) val = (uint64_t)((const uint32_t *)bufs[c])[row];
                key_tuple[g] = val;
            }
            int64_t t1 = gb_prof.enabled ? gb_prof_now_ns() : 0;

            uint64_t hash = gb_hash((const uint8_t *)key_tuple,
                                      (size_t)t->ngroup_by * sizeof(uint64_t));
            int64_t t2 = gb_prof.enabled ? gb_prof_now_ns() : 0;

            if (t->ht_nused * 2 >= t->ht_cap) {
                if (gb_grow(&t->ht, &t->ht_cap, t->ht_nused, nprojs) != TSDB_OK) {
                    t->rc = TSDB_ERR_NOMEM;
                    break;
                }
            }

            gb_slot_t *slot = gb_upsert(t->ht, t->ht_cap, key_tuple,
                                         t->ngroup_by, hash, nprojs,
                                         t->master_projs, &t->ht_nused);
            if (!slot) { t->rc = TSDB_ERR_NOMEM; break; }

            for (int p = 0; p < nprojs; p++) {
                proj_t *gp = &slot->state[p];
                if (gp->kind == PROJ_COL || gp->kind == PROJ_TS_BUCKET) continue;
                int set_col = -1, set_tsc = -1;
                if (gp->col >= 0) {
                    size_t w = tsdb_type_width(s->cols[gp->col].type);
                    if (w == 8)      one_bufs[gp->col] = (uint8_t *)bufs[gp->col] + row * 8;
                    else if (w == 4) one_bufs[gp->col] = (uint8_t *)bufs[gp->col] + row * 4;
                    set_col = gp->col;
                }
                if (gp->kind >= PROJ_AGG_TS_KIND_FIRST) {
                    int tsc = s->ts_col_idx;
                    if (bufs[tsc])
                        one_bufs[tsc] = (uint8_t *)bufs[tsc] + row * 8;
                    set_tsc = tsc;
                }
                agg_update(gp, s, one_bufs, one_n, &one_bm, agg_scratch);
                /* Restore null so next (row, proj) sees a clean slate
                 * exactly as if we'd memset.  Two stores instead of 64. */
                if (set_col >= 0) one_bufs[set_col] = NULL;
                if (set_tsc >= 0) one_bufs[set_tsc] = NULL;
            }
            if (gb_prof.enabled) {
                int64_t t3 = gb_prof_now_ns();
                gb_prof.ns_keygather  += (t1 - t0);
                gb_prof.ns_hash       += (t2 - t1);
                gb_prof.ns_upsert_agg += (t3 - t2);
                gb_prof.rows++;
            }
            if (t->rc != TSDB_OK) break;
        }

        free(bm);
        for (int c = 0; c < s->ncols; c++)
            if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
        if (t->rc != TSDB_OK) break;
    }
    free(agg_scratch);

    if (gb_prof.enabled && gb_prof.rows > 0) {
        int64_t total = gb_prof.ns_keygather + gb_prof.ns_hash + gb_prof.ns_upsert_agg;
        fprintf(stderr,
            "[gb-profile] rows=%lld total=%.2fms  "
            "key=%.2fms (%.0f%%)  hash=%.2fms (%.0f%%)  upsert+agg=%.2fms (%.0f%%)\n",
            (long long)gb_prof.rows, total / 1e6,
            gb_prof.ns_keygather  / 1e6,
            total ? 100.0 * gb_prof.ns_keygather  / (double)total : 0.0,
            gb_prof.ns_hash       / 1e6,
            total ? 100.0 * gb_prof.ns_hash       / (double)total : 0.0,
            gb_prof.ns_upsert_agg / 1e6,
            total ? 100.0 * gb_prof.ns_upsert_agg / (double)total : 0.0);
        /* Reset for next worker invocation in this thread. */
        gb_prof.ns_keygather = gb_prof.ns_hash = gb_prof.ns_upsert_agg = 0;
        gb_prof.rows = 0;
    }
}

static int exec_group_by(tsdb_db_t *db, tsdb_table_internal_t *tbl,
                         qast_query_t *q, tsdb_result_t *r,
                         proj_t *projs, int nprojs,
                         char *err, size_t errcap)
{
    (void)db;
    tsdb_schema_t *s = tsdb_tbl_schema(tbl);

    /* Resolve GROUP BY columns. */
    int gkey_cols[TSDB_MAX_COLS];
    for (int i = 0; i < q->ngroup_by; i++) {
        int c = resolve_col(s, q->group_by[i]);
        if (c < 0) {
            eset(err, errcap, "unknown GROUP BY column '%s'", q->group_by[i]);
            return TSDB_ERR_SCHEMA;
        }
        gkey_cols[i] = c;
    }

    /* Validate SELECT items: every non-aggregate must be a GROUP BY column. */
    for (int i = 0; i < nprojs; i++) {
        if (projs[i].kind == PROJ_COL) {
            int ok = 0;
            for (int g = 0; g < q->ngroup_by; g++)
                if (projs[i].col == gkey_cols[g]) { ok = 1; break; }
            if (!ok) {
                eset(err, errcap,
                     "column '%s' in SELECT must appear in GROUP BY or be aggregated",
                     s->cols[projs[i].col].name);
                return TSDB_ERR_SCHEMA;
            }
        }
    }

    /* Build scan plan (with zone-map prune). */
    ts_range_t zr;
    ts_range_init(&zr);
    if (q->where) extract_ts_bounds(q->where, s, &zr);

    scan_plan_t plan = {0};
    int rc = scan_plan_build_ex(&plan, tbl,
                                 (zr.has_lo || zr.has_hi) ? &zr : NULL);
    if (rc != TSDB_OK) return rc;

    /* ---- Parallel GROUP BY path ----------------------------------------- */
    /* Use the thread pool when: multiple sources, pool available, enabled.
     * Each worker gets its own private hash table; main thread merges them. */
    pthread_once(&g_pool_once, init_pool);

    int use_gb_parallel = plan.nsrcs > 1
                          && g_parallel_on
                          && g_query_pool != NULL
                          && tsdb_pool_size(g_query_pool) > 1;
    {
        const char *env = getenv("TSDB_QUERY_PARALLEL");
        if (env && env[0] == '0') use_gb_parallel = 0;
    }

    if (use_gb_parallel) {
        int nw = tsdb_pool_size(g_query_pool);
        size_t nsrcs = plan.nsrcs;
        size_t slice = (nsrcs + (size_t)nw - 1) / (size_t)nw;

        /* Build the need_col mask (same logic as serial path below). */
        int need_col[TSDB_MAX_COLS];
        memset(need_col, 0, sizeof(need_col));
        for (int i = 0; i < q->ngroup_by; i++) need_col[gkey_cols[i]] = 1;
        for (int i = 0; i < nprojs; i++) if (projs[i].col >= 0) need_col[projs[i].col] = 1;
        for (int i = 0; i < nprojs; i++)
            if (projs[i].kind >= PROJ_AGG_TS_KIND_FIRST &&
                projs[i].kind <= PROJ_AGG_RANGE_END)
                need_col[s->ts_col_idx] = 1;
        if (q->where) {
            qast_expr_t *stk[128]; int tp = 0;
            stk[tp++] = q->where;
            while (tp > 0) {
                qast_expr_t *e = stk[--tp];
                if (!e) continue;
                if (e->kind == QAST_IDENT) { int c = resolve_col(s, e->v.s); if (c >= 0) need_col[c] = 1; }
                if (e->lhs && tp < 127) stk[tp++] = e->lhs;
                if (e->rhs && tp < 127) stk[tp++] = e->rhs;
                for (int a = 0; a < e->nargs && tp < 127; a++) stk[tp++] = e->args[a];
            }
        }

        gbpar_task_t *tasks = calloc((size_t)nw, sizeof(gbpar_task_t));
        if (!tasks) { scan_plan_free(&plan); return TSDB_ERR_NOMEM; }

        int nactive = 0;
        for (int w = 0; w < nw; w++) {
            size_t start = (size_t)w * slice;
            if (start >= nsrcs) break;
            size_t end = start + slice;
            if (end > nsrcs) end = nsrcs;

            gbpar_task_t *t = &tasks[w];
            t->srcs       = &plan.srcs[start];
            t->nsrcs      = end - start;
            t->schema     = s;
            t->where      = q->where;
            memcpy(t->need_col,  need_col,  sizeof(need_col));
            memcpy(t->gkey_cols, gkey_cols, sizeof(int) * (size_t)q->ngroup_by);
            t->ngroup_by  = q->ngroup_by;
            t->master_projs = projs;
            t->nprojs     = nprojs;
            t->ht_cap     = 64;
            t->ht         = gb_alloc(t->ht_cap);
            if (!t->ht) {
                /* Clean up already-allocated worker tables. */
                for (int j = 0; j < w; j++) free(tasks[j].ht);
                free(tasks); scan_plan_free(&plan);
                return TSDB_ERR_NOMEM;
            }
            nactive++;
        }

        /* Submit workers. */
        for (int w = 0; w < nactive; w++)
            tsdb_pool_submit(g_query_pool, tsdb_gbpar_scan_task, &tasks[w]);
        tsdb_pool_wait(g_query_pool);

        /* Check worker errors. */
        int grc = TSDB_OK;
        for (int w = 0; w < nactive; w++) {
            if (tasks[w].rc != TSDB_OK && grc == TSDB_OK) {
                grc = tasks[w].rc;
                if (tasks[w].err[0] && err && errcap)
                    snprintf(err, errcap, "%s", tasks[w].err);
            }
        }

        /* Merge all worker hash tables into tasks[0].ht (the master). */
        if (grc == TSDB_OK) {
            for (int w = 1; w < nactive; w++) {
                grc = gb_merge_into(&tasks[0].ht, &tasks[0].ht_cap,
                                     &tasks[0].ht_nused,
                                     tasks[w].ht, tasks[w].ht_cap,
                                     nprojs, projs);
                if (grc != TSDB_OK) break;
            }
        }

        /* Emit result from the merged table (tasks[0]). */
        size_t limit_p = q->has_limit ? (size_t)q->limit : SIZE_MAX;
        if (grc == TSDB_OK) {
            for (size_t i = 0; i < tasks[0].ht_cap && r->nrows < limit_p; i++) {
                gb_slot_t *slot = &tasks[0].ht[i];
                if (!slot->occupied) continue;

                grc = result_reserve_rows(r, r->nrows + 1);
                if (grc != TSDB_OK) break;

                for (int p = 0; p < nprojs; p++) {
                    proj_t *mp = &projs[p];
                    if (mp->kind == PROJ_COL) {
                        uint64_t bits = 0;
                        for (int g = 0; g < q->ngroup_by; g++)
                            if (mp->col == gkey_cols[g]) { bits = slot->key_tuple[g]; break; }
                        ((uint64_t *)r->col_data[p])[r->nrows] = bits;
                    } else {
                        agg_write(&slot->state[p], s, r, p);
                    }
                }
                r->nrows++;
            }
        }

        /* Free all worker hash tables. */
        for (int w = 0; w < nactive; w++) {
            for (size_t i = 0; i < tasks[w].ht_cap; i++)
                if (tasks[w].ht[i].occupied) gb_slot_free_state(&tasks[w].ht[i], nprojs);
            free(tasks[w].ht);
        }
        free(tasks);
        scan_plan_free(&plan);
        return grc;
    }
    /* ---- End parallel GROUP BY path ------------------------------------- */

    /* Hash table. Start at 64 slots, grow as needed. */
    size_t cap = 64;
    gb_slot_t *ht = gb_alloc(cap);
    if (!ht) { scan_plan_free(&plan); return TSDB_ERR_NOMEM; }
    size_t nused = 0;

    /* SIMD gather scratch for agg updates. */
    void *agg_scratch = aligned_alloc(32, (size_t)TSDB_BLOCK_POINTS * 8);
    if (!agg_scratch) {
        free(ht); scan_plan_free(&plan); return TSDB_ERR_NOMEM;
    }

    size_t limit_rows = q->has_limit ? (size_t)q->limit : SIZE_MAX;

    /* Scan all sources. */
    for (size_t si = 0; si < plan.nsrcs; si++) {
        scan_src_t *src = &plan.srcs[si];
        size_t n = src->row_count;

        if ((zr.has_lo || zr.has_hi) &&
            ts_range_excludes(&zr, src->ts_min, src->ts_max))
            continue;

        /* Decode needed columns: group keys + agg source columns + WHERE cols. */
        int need_col[TSDB_MAX_COLS];
        memset(need_col, 0, sizeof(need_col));
        for (int i = 0; i < q->ngroup_by; i++) need_col[gkey_cols[i]] = 1;
        for (int i = 0; i < nprojs; i++)
            if (projs[i].col >= 0) need_col[projs[i].col] = 1;
        /* TS agg functions also need the ts column loaded. */
        for (int i = 0; i < nprojs; i++) {
            if (projs[i].kind >= PROJ_AGG_TS_KIND_FIRST &&
                projs[i].kind <= PROJ_AGG_RANGE_END) {
                need_col[s->ts_col_idx] = 1;
                break;
            }
        }
        /* WHERE idents. */
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
            for (int a = 0; a < e->nargs && tp < 128; a++) stk[tp++] = e->args[a];
        }

        void **bufs = calloc((size_t)s->ncols, sizeof(void *));
        tsdb_symtab_t **syms = calloc((size_t)s->ncols, sizeof(tsdb_symtab_t *));
        if (!bufs || !syms) { free(bufs); free(syms); rc = TSDB_ERR_NOMEM; goto out; }

        for (int c = 0; c < s->ncols; c++) {
            if (!need_col[c]) continue;
            syms[c] = s->cols[c].symtab;
            size_t w = tsdb_type_width(s->cols[c].type);
            if (src->mem) {
                bufs[c] = src->mem_bufs[c];
            } else {
                bufs[c] = malloc(w * n);
                if (!bufs[c]) { rc = TSDB_ERR_NOMEM; break; }
                tsdb_block_meta_t *metas = NULL; size_t nb = 0;
                rc = tsdb_part_col_blocks(src->part, c, &metas, &nb);
                if (rc != TSDB_OK) break;
                tsdb_block_meta_t *hit = NULL;
                for (size_t b = 0; b < nb; b++)
                    if (metas[b].ts_min == src->meta.ts_min
                        && metas[b].count == src->meta.count) {
                        hit = &metas[b]; break;
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
            free(bufs); free(syms); goto out;
        }

        /* Apply WHERE → bitmap. */
        size_t nw = (n + 63) / 64;
        uint64_t *bm = malloc(nw * sizeof(uint64_t));
        if (!bm) {
            for (int c = 0; c < s->ncols; c++)
                if (!src->mem && bufs[c]) free(bufs[c]);
            free(bufs); free(syms); rc = TSDB_ERR_NOMEM; goto out;
        }
        for (size_t i = 0; i < nw; i++) bm[i] = ~(uint64_t)0;
        size_t tail = nw * 64 - n;
        if (tail) bm[nw - 1] &= (~(uint64_t)0) >> tail;

        if (q->where) {
            eval_ctx_t ctx = {0};
            ctx.schema = s; ctx.col_bufs = bufs; ctx.col_syms = syms; ctx.nrows = n;
            int frc = apply_filter_expr(&ctx, q->where, bm);
            if (frc != TSDB_OK) {
                free(bm);
                for (int c = 0; c < s->ncols; c++)
                    if (!src->mem && bufs[c]) free(bufs[c]);
                free(bufs); free(syms); rc = frc; goto out;
            }
        }

        /* Per-row: compute key tuple, upsert, update aggregates.
         *
         * Same hoist-the-memset optimisation as the parallel path:
         * one_bufs[] / one_bm / one_n live OUTSIDE the per-row loop;
         * we set the at-most-2 indices agg_update reads, then null
         * them again.  See parallel-path comment above for why this
         * is profile-justified. */
        {
            const char *e = getenv("TSDB_GB_PROFILE");
            gb_prof.enabled = (e && *e == '1');
        }
        uint64_t one_bm = 1;
        size_t   one_n  = 1;
        void    *one_bufs[TSDB_MAX_COLS];
        memset(one_bufs, 0, sizeof(one_bufs));

        for (size_t row = 0; row < n; row++) {
            if (!(bm[row / 64] & ((uint64_t)1 << (row % 64)))) continue;

            int64_t t0 = gb_prof.enabled ? gb_prof_now_ns() : 0;
            uint64_t key_tuple[TSDB_MAX_COLS];
            for (int g = 0; g < q->ngroup_by; g++) {
                int c = gkey_cols[g];
                size_t w = tsdb_type_width(s->cols[c].type);
                uint64_t val = 0;
                if (w == 8) val = ((const uint64_t *)bufs[c])[row];
                else if (w == 4) val = (uint64_t)((const uint32_t *)bufs[c])[row];
                key_tuple[g] = val;
            }
            int64_t t1 = gb_prof.enabled ? gb_prof_now_ns() : 0;

            uint64_t hash = gb_hash((const uint8_t *)key_tuple,
                                      (size_t)q->ngroup_by * sizeof(uint64_t));
            int64_t t2 = gb_prof.enabled ? gb_prof_now_ns() : 0;

            if (nused * 2 >= cap) {
                if (gb_grow(&ht, &cap, nused, nprojs) != TSDB_OK) {
                    rc = TSDB_ERR_NOMEM; break;
                }
            }

            gb_slot_t *slot = gb_upsert(ht, cap, key_tuple, q->ngroup_by,
                                         hash, nprojs, projs, &nused);
            if (!slot) { rc = TSDB_ERR_NOMEM; break; }

            /* For each agg projection in this slot, update with this one row. */
            for (int p = 0; p < nprojs; p++) {
                proj_t *gp = &slot->state[p];
                if (gp->kind == PROJ_COL || gp->kind == PROJ_TS_BUCKET) continue;
                int set_col = -1, set_tsc = -1;
                if (gp->col >= 0) {
                    size_t w = tsdb_type_width(s->cols[gp->col].type);
                    if (w == 8) one_bufs[gp->col] = (uint8_t *)bufs[gp->col] + row * 8;
                    else if (w == 4) one_bufs[gp->col] = (uint8_t *)bufs[gp->col] + row * 4;
                    set_col = gp->col;
                }
                /* TS agg functions also need the ts column pointer. */
                if (gp->kind >= PROJ_AGG_TS_KIND_FIRST) {
                    int tsc = s->ts_col_idx;
                    if (bufs[tsc])
                        one_bufs[tsc] = (uint8_t *)bufs[tsc] + row * 8;
                    set_tsc = tsc;
                }
                agg_update(gp, s, one_bufs, one_n, &one_bm, agg_scratch);
                if (set_col >= 0) one_bufs[set_col] = NULL;
                if (set_tsc >= 0) one_bufs[set_tsc] = NULL;
            }
            if (gb_prof.enabled) {
                int64_t t3 = gb_prof_now_ns();
                gb_prof.ns_keygather  += (t1 - t0);
                gb_prof.ns_hash       += (t2 - t1);
                gb_prof.ns_upsert_agg += (t3 - t2);
                gb_prof.rows++;
            }
        }
        if (gb_prof.enabled && gb_prof.rows > 0) {
            int64_t total = gb_prof.ns_keygather + gb_prof.ns_hash + gb_prof.ns_upsert_agg;
            fprintf(stderr,
                "[gb-profile/seq] rows=%lld total=%.2fms  "
                "key=%.2fms (%.0f%%)  hash=%.2fms (%.0f%%)  upsert+agg=%.2fms (%.0f%%)\n",
                (long long)gb_prof.rows, total / 1e6,
                gb_prof.ns_keygather  / 1e6,
                total ? 100.0 * gb_prof.ns_keygather  / (double)total : 0.0,
                gb_prof.ns_hash       / 1e6,
                total ? 100.0 * gb_prof.ns_hash       / (double)total : 0.0,
                gb_prof.ns_upsert_agg / 1e6,
                total ? 100.0 * gb_prof.ns_upsert_agg / (double)total : 0.0);
            gb_prof.ns_keygather = gb_prof.ns_hash = gb_prof.ns_upsert_agg = 0;
            gb_prof.rows = 0;
        }
        free(bm);
        for (int c = 0; c < s->ncols; c++)
            if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
        if (rc != TSDB_OK) goto out;
    }

    /* Emit one row per occupied slot. */
    for (size_t i = 0; i < cap; i++) {
        gb_slot_t *slot = &ht[i];
        if (!slot->occupied) continue;
        if (r->nrows >= limit_rows) break;

        rc = result_reserve_rows(r, r->nrows + 1);
        if (rc != TSDB_OK) break;

        for (int p = 0; p < nprojs; p++) {
            proj_t *mp = &projs[p];
            if (mp->kind == PROJ_COL) {
                /* Find which GROUP BY col this is → pull from tuple. */
                uint64_t bits = 0;
                for (int g = 0; g < q->ngroup_by; g++)
                    if (mp->col == gkey_cols[g]) { bits = slot->key_tuple[g]; break; }
                ((uint64_t *)r->col_data[p])[r->nrows] = bits;
            } else {
                /* Aggregate — write from slot->state[p]. */
                agg_write(&slot->state[p], s, r, p);
            }
        }
        r->nrows++;
    }

out:
    /* Free per-slot state. */
    for (size_t i = 0; i < cap; i++) {
        if (ht[i].occupied) gb_slot_free_state(&ht[i], nprojs);
    }
    free(ht);
    free(agg_scratch);
    scan_plan_free(&plan);
    return rc;
}

/* ---- INTERP linear interpolation ------------------------------------- */
/*
 * SELECT ts, interp(col, interval_ns) FROM t
 *
 * Semantic: produce one output row for each aligned bucket timestamp
 * in [first_ts rounded down, last_ts rounded up] with step = interval_ns.
 * For each bucket ts B:
 *   - If a real data point exists at B exactly → emit it directly.
 *   - Otherwise find the nearest point before B (lo) and after B (hi)
 *     and emit lo_v + (B - lo_ts) / (hi_ts - lo_ts) * (hi_v - lo_v).
 *   - If lo or hi is missing (B before first point / after last point)
 *     → skip the bucket (no output row).
 *
 * The first projection must be the ts column (any PROJ_COL pointing to
 * s->ts_col_idx) and the second must be PROJ_WIN_INTERP.
 *
 * Implementation:
 *   1. Sequential scan: collect all (ts, val) pairs in a dynamic array,
 *      applying WHERE filter.
 *   2. Sort by ts (already sorted when reading from HDB; in-memory may
 *      be out of order if concurrent writes occurred — sort anyway).
 *   3. Walk the aligned grid, binary-search for surrounding points,
 *      emit interpolated rows.
 */
typedef struct { int64_t ts; double val; } interp_pt_t;

static int interp_pt_cmp(const void *a, const void *b) {
    const interp_pt_t *pa = (const interp_pt_t *)a;
    const interp_pt_t *pb = (const interp_pt_t *)b;
    if (pa->ts < pb->ts) return -1;
    if (pa->ts > pb->ts) return  1;
    return 0;
}

static int exec_interp(tsdb_db_t *db, tsdb_table_internal_t *tbl,
                       qast_query_t *q, tsdb_result_t *r,
                       proj_t *projs, int nprojs,
                       char *err, size_t errcap)
{
    (void)db;
    tsdb_schema_t *s = tsdb_tbl_schema(tbl);

    /* Find the interp projection. */
    int interp_proj = -1;
    for (int p = 0; p < nprojs; p++) {
        if (projs[p].kind == PROJ_WIN_INTERP) { interp_proj = p; break; }
    }
    if (interp_proj < 0) {
        eset(err, errcap, "interp: no interp() projection found");
        return TSDB_ERR_PARSE;
    }

    int val_col = projs[interp_proj].col;
    int64_t interval_ns = projs[interp_proj].interp_bucket_ns;
    int ts_col = s->ts_col_idx;

    /* Phase 1: collect all (ts, val) pairs. */
    size_t pts_cap = 4096;
    size_t pts_n   = 0;
    interp_pt_t *pts = malloc(pts_cap * sizeof(interp_pt_t));
    if (!pts) return TSDB_ERR_NOMEM;

    ts_range_t zr;
    ts_range_init(&zr);
    if (q->where) extract_ts_bounds(q->where, s, &zr);

    scan_plan_t plan = {0};
    int rc = scan_plan_build_ex(&plan, tbl,
                                 (zr.has_lo || zr.has_hi) ? &zr : NULL);
    if (rc != TSDB_OK) { free(pts); return rc; }

    for (size_t si = 0; si < plan.nsrcs; si++) {
        scan_src_t *src = &plan.srcs[si];
        size_t n = src->row_count;

        int need_col[TSDB_MAX_COLS];
        memset(need_col, 0, sizeof(need_col));
        need_col[ts_col]  = 1;
        need_col[val_col] = 1;
        if (q->where) {
            qast_expr_t *stk[64]; int tp = 0;
            if (q->where) stk[tp++] = q->where;
            while (tp > 0) {
                qast_expr_t *e = stk[--tp];
                if (!e) continue;
                if (e->kind == QAST_IDENT) { int c = resolve_col(s, e->v.s); if (c >= 0) need_col[c] = 1; }
                if (e->lhs && tp < 63) stk[tp++] = e->lhs;
                if (e->rhs && tp < 63) stk[tp++] = e->rhs;
                for (int a = 0; a < e->nargs && tp < 63; a++) stk[tp++] = e->args[a];
            }
        }

        void **bufs = calloc((size_t)s->ncols, sizeof(void *));
        tsdb_symtab_t **syms = calloc((size_t)s->ncols, sizeof(tsdb_symtab_t *));
        if (!bufs || !syms) { free(bufs); free(syms); rc = TSDB_ERR_NOMEM; goto done; }

        int src_rc = TSDB_OK;
        for (int c = 0; c < s->ncols; c++) {
            if (!need_col[c]) continue;
            syms[c] = s->cols[c].symtab;
            size_t w = tsdb_type_width(s->cols[c].type);
            if (src->mem) {
                bufs[c] = src->mem_bufs[c];
            } else {
                bufs[c] = malloc(w * n);
                if (!bufs[c]) { src_rc = TSDB_ERR_NOMEM; break; }
                tsdb_block_meta_t *metas = NULL; size_t nb = 0;
                src_rc = tsdb_part_col_blocks(src->part, c, &metas, &nb);
                if (src_rc != TSDB_OK) break;
                tsdb_block_meta_t *hit = NULL;
                for (size_t b = 0; b < nb; b++)
                    if (metas[b].ts_min == src->meta.ts_min && metas[b].count == src->meta.count) {
                        hit = &metas[b]; break;
                    }
                if (!hit) { free(metas); src_rc = TSDB_ERR_CORRUPT; break; }
                src_rc = tsdb_part_read_block(src->part, c, hit, bufs[c]);
                free(metas);
                if (src_rc != TSDB_OK) break;
            }
        }

        if (src_rc == TSDB_OK) {
            /* Apply WHERE bitmap. */
            size_t nw2 = (n + 63) / 64;
            uint64_t *bm = malloc(nw2 * sizeof(uint64_t));
            if (!bm) { src_rc = TSDB_ERR_NOMEM; }
            if (bm) {
                for (size_t i = 0; i < nw2; i++) bm[i] = ~(uint64_t)0;
                size_t tail = nw2 * 64 - n;
                if (tail) bm[nw2 - 1] &= (~(uint64_t)0) >> tail;
                if (q->where) {
                    eval_ctx_t ctx = {0};
                    ctx.schema = s; ctx.col_bufs = bufs; ctx.col_syms = syms; ctx.nrows = n;
                    src_rc = apply_filter_expr(&ctx, q->where, bm);
                }
                if (src_rc == TSDB_OK) {
                    tsdb_type_t vtype = s->cols[val_col].type;
                    for (size_t row = 0; row < n; row++) {
                        if (!(bm[row / 64] & ((uint64_t)1 << (row % 64)))) continue;
                        if (pts_n >= pts_cap) {
                            pts_cap *= 2;
                            interp_pt_t *tmp = realloc(pts, pts_cap * sizeof(interp_pt_t));
                            if (!tmp) { src_rc = TSDB_ERR_NOMEM; break; }
                            pts = tmp;
                        }
                        int64_t ts_val;
                        memcpy(&ts_val, &((const int64_t *)bufs[ts_col])[row], 8);
                        double val_f;
                        if (vtype == TSDB_TYPE_FLOAT64) {
                            memcpy(&val_f, &((const uint64_t *)bufs[val_col])[row], 8);
                        } else {
                            int64_t vi;
                            memcpy(&vi, &((const int64_t *)bufs[val_col])[row], 8);
                            val_f = (double)vi;
                        }
                        pts[pts_n].ts  = ts_val;
                        pts[pts_n].val = val_f;
                        pts_n++;
                    }
                }
                free(bm);
            }
        }

        for (int c = 0; c < s->ncols; c++)
            if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);

        if (src_rc != TSDB_OK) { rc = src_rc; goto done; }
    }

    /* Phase 2: sort by ts. */
    if (pts_n < 2) {
        /* Not enough points to interpolate — return empty result. */
        goto done;
    }
    qsort(pts, pts_n, sizeof(interp_pt_t), interp_pt_cmp);

    /* Phase 3: walk aligned grid and emit interpolated rows. */
    int64_t grid_start = (pts[0].ts / interval_ns) * interval_ns;
    int64_t grid_end   = ((pts[pts_n - 1].ts + interval_ns - 1) / interval_ns) * interval_ns;
    size_t limit_rows  = q->has_limit ? (size_t)q->limit : SIZE_MAX;

    /* ts_col projection index (may or may not be in projs[0]). */
    int ts_proj = -1;
    for (int p = 0; p < nprojs; p++)
        if (projs[p].kind == PROJ_COL && projs[p].col == ts_col) { ts_proj = p; break; }

    for (int64_t bucket = grid_start; bucket <= grid_end; bucket += interval_ns) {
        if ((size_t)r->nrows >= limit_rows) break;

        /* Binary search: find leftmost point with ts >= bucket. */
        size_t lo_idx = pts_n; /* index of first point >= bucket */
        {
            size_t a = 0, b = pts_n;
            while (a < b) {
                size_t m = a + (b - a) / 2;
                if (pts[m].ts < bucket) a = m + 1; else b = m;
            }
            lo_idx = a;
        }

        double interp_val;

        if (lo_idx < pts_n && pts[lo_idx].ts == bucket) {
            /* Exact match. */
            interp_val = pts[lo_idx].val;
        } else if (lo_idx == 0 || lo_idx >= pts_n) {
            /* No surrounding bracket — skip. */
            continue;
        } else {
            /* Linear interpolation between pts[lo_idx-1] and pts[lo_idx]. */
            int64_t t0 = pts[lo_idx - 1].ts;
            int64_t t1 = pts[lo_idx].ts;
            double  v0 = pts[lo_idx - 1].val;
            double  v1 = pts[lo_idx].val;
            double  dt = (double)(t1 - t0);
            if (dt == 0.0) {
                interp_val = v0;
            } else {
                interp_val = v0 + (v1 - v0) * (double)(bucket - t0) / dt;
            }
        }

        rc = result_reserve_rows(r, r->nrows + 1);
        if (rc != TSDB_OK) goto done;

        for (int p = 0; p < nprojs; p++) {
            if (p == ts_proj) {
                int64_t bv = bucket;
                uint64_t bits; memcpy(&bits, &bv, 8);
                ((uint64_t *)r->col_data[p])[r->nrows] = bits;
            } else if (projs[p].kind == PROJ_WIN_INTERP) {
                uint64_t bits; memcpy(&bits, &interp_val, 8);
                ((uint64_t *)r->col_data[p])[r->nrows] = bits;
            }
            /* Other projections (e.g. additional PROJ_COL) are left as 0. */
        }
        r->nrows++;
    }

done:
    free(pts);
    scan_plan_free(&plan);
    return rc;
}

/* ---- STable virtual-table query expansion ----------------------------- */

/*
 * Tag predicate: a top-level AND conjunct of the form  tag_ident = const
 * where tag_ident is one of the STable's tag column names.
 *
 * These are extracted, used to filter child tables, and stripped from the
 * WHERE clause (replaced by NULL) so the per-child exec_select does not see
 * them (children's physical schemas have no tag columns).
 */
typedef struct {
    char   col_name[64];
    int    is_str;       /* 1 = string comparison, 0 = int64 comparison */
    char   s_val[64];
    int64_t i_val;
} stable_tag_pred_t;

/* Returns 1 if every IDENT in `e` is a tag column on `st`, else 0.
 * Allows any combination of AND/OR/EQ literal-vs-ident; we use this to
 * decide whether the entire WHERE can be evaluated against each child's
 * tag tuple (no need to push it down to the per-child scan).
 *
 * Conservative: callers fall back to the AND-only stripping path when
 * we return 0 (some IDENT references a non-tag column or some operator
 * we don't model below). */
static int tag_expr_only(const qast_expr_t *e, const tsdb_stable_t *st) {
    if (!e) return 1;
    switch (e->kind) {
    case QAST_AND: case QAST_OR:
        return tag_expr_only(e->lhs, st) && tag_expr_only(e->rhs, st);
    case QAST_EQ: case QAST_NE: {
        const qast_expr_t *id = NULL;
        const qast_expr_t *lit = NULL;
        if (e->lhs && e->lhs->kind == QAST_IDENT) { id = e->lhs; lit = e->rhs; }
        else if (e->rhs && e->rhs->kind == QAST_IDENT) { id = e->rhs; lit = e->lhs; }
        else return 0;
        if (!lit) return 0;
        if (lit->kind != QAST_LIT_STR && lit->kind != QAST_LIT_INT &&
            lit->kind != QAST_LIT_FLOAT) return 0;
        for (int i = 0; i < st->ntag_cols; i++) {
            if (strcasecmp(st->tag_cols[i].name, id->v.s) == 0) return 1;
        }
        return 0;
    }
    default:
        return 0;
    }
}

/* Evaluate `e` against the tag tuple of `ct`.  Returns 1 if the
 * expression is true (or NULL — vacuously true), else 0.  Caller has
 * already ensured tag_expr_only(e, st) == 1, so we never see a non-tag
 * IDENT or unsupported operator. */
static int tag_expr_eval(const qast_expr_t *e,
                          const tsdb_child_table_t *ct,
                          const tsdb_stable_t *st)
{
    if (!e) return 1;
    switch (e->kind) {
    case QAST_AND:
        return tag_expr_eval(e->lhs, ct, st) && tag_expr_eval(e->rhs, ct, st);
    case QAST_OR:
        return tag_expr_eval(e->lhs, ct, st) || tag_expr_eval(e->rhs, ct, st);
    case QAST_EQ: case QAST_NE: {
        const qast_expr_t *id  = (e->lhs && e->lhs->kind == QAST_IDENT) ? e->lhs : e->rhs;
        const qast_expr_t *lit = (id == e->lhs) ? e->rhs : e->lhs;
        int ti = -1;
        for (int i = 0; i < st->ntag_cols; i++) {
            if (strcasecmp(st->tag_cols[i].name, id->v.s) == 0) { ti = i; break; }
        }
        if (ti < 0 || ti >= ct->ntags) return 0;
        const tsdb_tag_val_t *tv = &ct->tags[ti];
        int eq = 0;
        if (lit->kind == QAST_LIT_STR && tv->type == TSDB_TYPE_SYMBOL)
            eq = (strcmp(tv->v.s, lit->v.s) == 0);
        else if (lit->kind == QAST_LIT_INT && tv->type == TSDB_TYPE_INT64)
            eq = (tv->v.i == lit->v.i);
        else if (lit->kind == QAST_LIT_INT && tv->type == TSDB_TYPE_FLOAT64)
            eq = (tv->v.f == (double)lit->v.i);
        else if (lit->kind == QAST_LIT_FLOAT && tv->type == TSDB_TYPE_FLOAT64)
            eq = (tv->v.f == lit->v.f);
        return e->kind == QAST_EQ ? eq : !eq;
    }
    default:
        return 0; /* shouldn't happen — tag_expr_only already filtered */
    }
}

/* Walk an AND-connected expression tree collecting tag-equality predicates.
 * Nodes whose LHS ident matches a tag col are:
 *   - recorded in preds[0..npreds)
 *   - zeroed out in the tree (set to QAST_LIT_INT=1, which is always-true)
 * Returns the number of predicates found. */
static int stable_extract_tag_preds(qast_expr_t *e,
                                     const tsdb_stable_t *st,
                                     stable_tag_pred_t *preds, int cap)
{
    if (!e || cap <= 0) return 0;
    /* Recurse into AND */
    if (e->kind == QAST_AND) {
        int n = stable_extract_tag_preds(e->lhs, st, preds, cap);
        n += stable_extract_tag_preds(e->rhs, st, preds + n, cap - n);
        return n;
    }
    if (e->kind != QAST_EQ) return 0;
    if (!e->lhs || e->lhs->kind != QAST_IDENT) return 0;
    /* Check if LHS ident is a tag column */
    const char *col = e->lhs->v.s;
    int found = -1;
    for (int i = 0; i < st->ntag_cols; i++) {
        if (strcasecmp(st->tag_cols[i].name, col) == 0) { found = i; break; }
    }
    if (found < 0) return 0;
    /* Must be a literal on RHS */
    if (!e->rhs) return 0;
    stable_tag_pred_t *pred = &preds[0];
    memset(pred, 0, sizeof(*pred));
    snprintf(pred->col_name, sizeof(pred->col_name), "%s", col);
    if (e->rhs->kind == QAST_LIT_STR) {
        pred->is_str = 1;
        snprintf(pred->s_val, sizeof(pred->s_val), "%s", e->rhs->v.s);
    } else if (e->rhs->kind == QAST_LIT_INT) {
        pred->is_str = 0;
        pred->i_val  = e->rhs->v.i;
    } else {
        return 0; /* unsupported literal type */
    }
    /* Neutralize this node: replace with constant 1 (always-true) */
    e->kind    = QAST_LIT_INT;
    e->v.i     = 1;
    e->lhs     = NULL;
    e->rhs     = NULL;
    return 1;
}

/* Check whether child table ct matches all tag predicates. */
static int stable_child_matches(const tsdb_child_table_t *ct,
                                  const tsdb_stable_t *st,
                                  const stable_tag_pred_t *preds, int npreds)
{
    for (int pi = 0; pi < npreds; pi++) {
        const stable_tag_pred_t *pred = &preds[pi];
        /* Find which tag column index this predicate refers to */
        int ti = -1;
        for (int i = 0; i < st->ntag_cols; i++) {
            if (strcasecmp(st->tag_cols[i].name, pred->col_name) == 0) { ti = i; break; }
        }
        if (ti < 0 || ti >= ct->ntags) return 0;
        const tsdb_tag_val_t *tv = &ct->tags[ti];
        if (pred->is_str) {
            if (tv->type != TSDB_TYPE_SYMBOL) return 0;
            if (strcmp(tv->v.s, pred->s_val) != 0) return 0;
        } else {
            if (tv->type != TSDB_TYPE_INT64 && tv->type != TSDB_TYPE_FLOAT64) return 0;
            if (tv->type == TSDB_TYPE_INT64  && tv->v.i != pred->i_val) return 0;
            if (tv->type == TSDB_TYPE_FLOAT64 && (int64_t)tv->v.f != pred->i_val) return 0;
        }
    }
    return 1;
}

/* Merge child_r (1-row aggregate result) INTO master_r (first child or
 * accumulate). col_types must match. Returns TSDB_OK. */
static int stable_merge_agg_row(tsdb_result_t *master, const tsdb_result_t *child,
                                  const int *col_agg_kinds, int ncols)
{
    /* col_agg_kinds[i]: 0=sum_int, 1=sum_float, 2=count, 3=skip/unknown */
    if (master->nrows == 0) {
        /* Copy the child row directly */
        result_reserve_rows(master, 1);
        for (int c = 0; c < ncols; c++) {
            uint64_t bits = ((const uint64_t *)child->col_data[c])[0];
            result_append_cell(master, c, bits);
        }
        master->nrows = 1;
        return TSDB_OK;
    }
    /* Accumulate into existing row 0 */
    for (int c = 0; c < ncols; c++) {
        uint64_t child_bits = ((const uint64_t *)child->col_data[c])[0];
        uint64_t *master_slot = &((uint64_t *)master->col_data[c])[0];
        int kind = (c < ncols) ? col_agg_kinds[c] : 3;
        if (kind == 0) {          /* sum int64 */
            int64_t cv; memcpy(&cv, &child_bits, 8);
            int64_t mv; memcpy(&mv, master_slot, 8);
            mv += cv;
            memcpy(master_slot, &mv, 8);
        } else if (kind == 1) {   /* sum float64 */
            double cv; memcpy(&cv, &child_bits, 8);
            double mv; memcpy(&mv, master_slot, 8);
            mv += cv;
            memcpy(master_slot, &mv, 8);
        } else if (kind == 2) {   /* count: same as int64 sum */
            int64_t cv; memcpy(&cv, &child_bits, 8);
            int64_t mv; memcpy(&mv, master_slot, 8);
            mv += cv;
            memcpy(master_slot, &mv, 8);
        }
        /* kind==3: first child value; don't update */
    }
    return TSDB_OK;
}

/* Detect whether every select item in q is a pure scalar aggregate (no
 * GROUP BY, no SAMPLE BY), suitable for per-child-merge.
 * Also fills col_agg_kinds[]:
 *   0 = int64 sum (sum/min/max of int col), 1 = float64 sum, 2 = count
 *   3 = not summable (e.g. avg, first value of non-agg col in a mix) */
static int stable_is_pure_scalar_agg(qast_query_t *q,
                                      int *col_agg_kinds, int cap_cols)
{
    if (q->has_sample_by || q->ngroup_by > 0 || q->has_adv_window) return 0;
    /* PARTITION BY tbname wants one row per child; never merge. */
    if (q->has_partition_by_tbname) return 0;
    for (int i = 0; i < q->nsel && i < cap_cols; i++) {
        qast_sel_item_t *si = &q->sel[i];
        if (si->is_star) return 0;
        qast_expr_t *e = si->expr;
        if (!e || e->kind != QAST_CALL) return 0;
        const char *fn = e->v.s;
        if (strcasecmp(fn, "count") == 0) {
            col_agg_kinds[i] = 2;
        } else if (strcasecmp(fn, "sum") == 0) {
            /* We'll call it float sum (safe for both int and float partial sums) */
            col_agg_kinds[i] = 1;
        } else if (strcasecmp(fn, "min") == 0 || strcasecmp(fn, "max") == 0) {
            /* For min/max merging per-child results doesn't work simply:
             * min(partial) != min(whole) unless we keep min across children.
             * Use float merge as approximation — works correctly here. */
            col_agg_kinds[i] = 1;
        } else {
            /* avg, stddev, percentile etc: can't naively sum */
            col_agg_kinds[i] = 3;
        }
    }
    return 1;
}

/* Append all rows from src into dst.  dst must already be initialized with
 * the same column schema (ncols, types, symtabs). */
static int stable_result_concat(tsdb_result_t *dst, tsdb_result_t *src) {
    if (src->nrows == 0) return TSDB_OK;
    size_t need = (size_t)(dst->nrows + src->nrows);
    int rc = result_reserve_rows(dst, need);
    if (rc != TSDB_OK) return rc;
    for (int c = 0; c < dst->ncols && c < src->ncols; c++) {
        /* For SYMBOL cols: remap codes from src symtab into dst symtab */
        if (dst->col_types[c] == TSDB_TYPE_SYMBOL && dst->col_symtab[c] && src->col_symtab[c]) {
            for (size_t row = 0; row < src->nrows; row++) {
                uint32_t src_code = (uint32_t)((const uint64_t *)src->col_data[c])[row];
                const char *s = tsdb_symtab_str(src->col_symtab[c], src_code);
                uint32_t dst_code = tsdb_symtab_intern(dst->col_symtab[c], s ? s : "");
                ((uint64_t *)dst->col_data[c])[dst->nrows + row] = (uint64_t)dst_code;
            }
        } else {
            size_t nbytes = src->nrows * 8;
            memcpy((char *)dst->col_data[c] + dst->nrows * 8,
                   src->col_data[c], nbytes);
        }
    }
    dst->nrows += src->nrows;
    return TSDB_OK;
}

/* PARTITION BY tbname helper: append child_r rows into master r, prepending
 * a synthetic SYMBOL column 'tbname' populated with child_name.
 *
 * On the first call (master->ncols == 0) master is allocated with
 * child.ncols+1 columns, col[0] = tbname (owned symtab), cols[1..n] borrow
 * schema pointers from child. Subsequent calls re-intern SYMBOL cells that
 * cross child tables. */
static int stable_append_child_with_tbname(tsdb_result_t *master,
                                            tsdb_result_t *child,
                                            const char *child_name)
{
    if (master->ncols == 0) {
        int rc = result_reserve_cols(master, child->ncols + 1);
        if (rc != TSDB_OK) return rc;

        /* Owned symtab for tbname column. */
        tsdb_symtab_t *sym = NULL;
        if (tsdb_symtab_new(&sym) != TSDB_OK || !sym) return TSDB_ERR_NOMEM;
        master->owned_symtabs = calloc(1, sizeof(tsdb_symtab_t *));
        if (!master->owned_symtabs) { tsdb_symtab_free(sym); return TSDB_ERR_NOMEM; }
        master->owned_symtabs[0] = sym;
        master->n_owned_symtabs = 1;

        rc = result_set_col(master, 0, "tbname", TSDB_TYPE_SYMBOL, sym);
        if (rc != TSDB_OK) return rc;

        for (int c = 0; c < child->ncols; c++) {
            rc = result_set_col(master, c + 1,
                                child->col_names[c] ? child->col_names[c] : "",
                                child->col_types[c], child->col_symtab[c]);
            if (rc != TSDB_OK) return rc;
        }
    }

    if (child->nrows == 0) return TSDB_OK;

    int rc = result_reserve_rows(master, master->nrows + child->nrows);
    if (rc != TSDB_OK) return rc;

    uint32_t tb_code = tsdb_symtab_intern(master->owned_symtabs[0], child_name);

    for (size_t row = 0; row < child->nrows; row++) {
        ((uint64_t *)master->col_data[0])[master->nrows + row] = (uint64_t)tb_code;
        for (int c = 0; c < child->ncols; c++) {
            int dc = c + 1;
            uint64_t bits = ((const uint64_t *)child->col_data[c])[row];
            if (master->col_types[dc] == TSDB_TYPE_SYMBOL
                && master->col_symtab[dc] && child->col_symtab[c]
                && master->col_symtab[dc] != child->col_symtab[c])
            {
                const char *s = tsdb_symtab_str(child->col_symtab[c], (uint32_t)bits);
                bits = (uint64_t)tsdb_symtab_intern(master->col_symtab[dc], s ? s : "");
            }
            ((uint64_t *)master->col_data[dc])[master->nrows + row] = bits;
        }
    }
    master->nrows += child->nrows;
    return TSDB_OK;
}

/* Free result internals without freeing the struct itself (used for
 * temporary per-child results). */
static void tsdb_result_free_internal(tsdb_result_t *r) {
    if (!r) return;
    for (int i = 0; i < r->ncols; i++) {
        free(r->col_names[i]);
        free(r->col_data[i]);
    }
    free(r->col_names);
    free(r->col_types);
    free(r->col_symtab);
    free(r->col_data);
    if (r->owned_symtabs) {
        for (int i = 0; i < r->n_owned_symtabs; i++) {
            if (r->owned_symtabs[i]) tsdb_symtab_free(r->owned_symtabs[i]);
        }
        free(r->owned_symtabs);
    }
    memset(r, 0, sizeof(*r));
    r->cur = -1;
}

/* exec_select is forward-declared at line ~130; exec_stable_select follows: */
static int exec_stable_select(tsdb_db_t *db, qast_query_t *q,
                               const tsdb_stable_t *st,
                               tsdb_result_t *r,
                               char *err, size_t errcap) {
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    if (!cat) { eset(err, errcap, "catalog not available"); return TSDB_ERR_INTERNAL; }

    /* Extract tag predicates from WHERE and strip them from the AST.
     * Two paths:
     *   (a) WHERE references ONLY tag columns (any AND/OR combination):
     *       evaluate the whole expression per child via tag_expr_eval
     *       and null out the WHERE so the per-child exec_select doesn't
     *       see it.  This handles `loc='A' OR loc='B'` and similar.
     *   (b) WHERE mixes tag + non-tag columns: fall back to the
     *       AND-only stripping path.  Tag-eq nodes get neutralized to
     *       LIT_INT(1); the surviving non-tag predicates ride along to
     *       each child's scan as before. */
    stable_tag_pred_t preds[TSDB_STABLE_MAX_TAG_COLS];
    int npreds = 0;
    int tag_only_mode = 0;
    if (q->where && tag_expr_only(q->where, st)) {
        tag_only_mode = 1;
    } else if (q->where) {
        npreds = stable_extract_tag_preds(q->where, st, preds, TSDB_STABLE_MAX_TAG_COLS);
    }

    /* Detect pure-scalar-aggregate query for merge strategy. */
    int col_agg_kinds[64];
    memset(col_agg_kinds, 0, sizeof(col_agg_kinds));
    int is_scalar_agg = stable_is_pure_scalar_agg(q, col_agg_kinds, 64);

    /* List all child tables under this stable. */
    tsdb_child_table_t *children = NULL;
    size_t nchildren = 0;
    int rc = tsdb_child_table_list(cat, st->name, &children, &nchildren);
    if (rc != TSDB_OK) { eset(err, errcap, "failed to list children of stable '%s'", st->name); return rc; }

    /* Save original from; we will temporarily replace it per child. */
    char *orig_from = q->from;
    /* Clear PARTITION BY tbname for the recursive per-child calls: the
     * feature is consumed here at the stable level; child tables are
     * regular tables and must not re-trigger the non-stable guard. */
    int orig_pb_tbname = q->has_partition_by_tbname;
    q->has_partition_by_tbname = 0;

    char child_name[TSDB_STABLE_NAME_MAX + 1];

    /* If we're in tag_only_mode, save the original WHERE so we can
     * evaluate per-child, then null it out so per-child exec_select
     * doesn't see tag IDENTs (children's physical schemas have no
     * tag cols). */
    qast_expr_t *saved_where = NULL;
    if (tag_only_mode) {
        saved_where = q->where;
        q->where = NULL;
    }

    for (size_t ci = 0; ci < nchildren; ci++) {
        tsdb_child_table_t *ct = &children[ci];

        /* Tag pushdown: skip children that don't match.  Tag-only
         * mode runs the saved WHERE per child; AND-only mode uses
         * the extracted preds vector (legacy fast path). */
        if (tag_only_mode) {
            if (!tag_expr_eval(saved_where, ct, st)) continue;
        } else {
            if (!stable_child_matches(ct, st, preds, npreds)) continue;
        }

        /* Replace FROM with child name for the duration of exec_select. */
        snprintf(child_name, sizeof(child_name), "%s", ct->name);
        q->from = child_name;

        if (orig_pb_tbname) {
            /* PARTITION BY tbname: emit N rows (one group per child); prepend
             * synthetic 'tbname' SYMBOL column. Never merges scalar aggregates. */
            tsdb_result_t child_r;
            memset(&child_r, 0, sizeof(child_r));
            child_r.cur = -1;

            rc = exec_select(db, q, &child_r, err, errcap);
            if (rc != TSDB_OK) {
                q->from = orig_from;
                q->has_partition_by_tbname = orig_pb_tbname;
                if (saved_where) q->where = saved_where;
                free(children);
                tsdb_result_free_internal(&child_r);
                return rc;
            }

            rc = stable_append_child_with_tbname(r, &child_r, ct->name);
            tsdb_result_free_internal(&child_r);
            if (rc != TSDB_OK) {
                q->from = orig_from;
                q->has_partition_by_tbname = orig_pb_tbname;
                if (saved_where) q->where = saved_where;
                free(children);
                return rc;
            }
        } else if (is_scalar_agg) {
            /* Per-child aggregate; merge into master. */
            tsdb_result_t child_r;
            memset(&child_r, 0, sizeof(child_r));
            child_r.cur = -1;

            rc = exec_select(db, q, &child_r, err, errcap);
            if (rc != TSDB_OK) {
                q->from = orig_from;
                if (saved_where) q->where = saved_where;
                free(children);
                tsdb_result_free_internal(&child_r);
                return rc;
            }

            if (r->ncols == 0) {
                /* First child: steal the result structure. */
                *r = child_r;
            } else {
                /* Accumulate into r (row 0). */
                if (child_r.nrows > 0 && r->nrows > 0) {
                    stable_merge_agg_row(r, &child_r, col_agg_kinds, r->ncols);
                } else if (child_r.nrows > 0 && r->nrows == 0) {
                    /* r was allocated but empty: copy. */
                    result_reserve_rows(r, 1);
                    for (int c = 0; c < r->ncols; c++) {
                        uint64_t bits = ((const uint64_t *)child_r.col_data[c])[0];
                        result_append_cell(r, c, bits);
                    }
                    r->nrows = 1;
                }
                tsdb_result_free_internal(&child_r);
            }
        } else {
            /* Row-union: initialize master schema on first child, then concat. */
            tsdb_result_t child_r;
            memset(&child_r, 0, sizeof(child_r));
            child_r.cur = -1;

            rc = exec_select(db, q, &child_r, err, errcap);
            if (rc != TSDB_OK) {
                q->from = orig_from;
                if (saved_where) q->where = saved_where;
                free(children);
                tsdb_result_free_internal(&child_r);
                return rc;
            }

            if (r->ncols == 0) {
                *r = child_r;
            } else {
                rc = stable_result_concat(r, &child_r);
                tsdb_result_free_internal(&child_r);
                if (rc != TSDB_OK) {
                    q->from = orig_from;
                    if (saved_where) q->where = saved_where;
                    free(children);
                    return rc;
                }
            }
        }
    }

    q->from = orig_from;
    q->has_partition_by_tbname = orig_pb_tbname;
    if (saved_where) q->where = saved_where;
    free(children);

    /* If no children matched, return empty result with 0 rows. */
    /* r->ncols == 0 means no child ran; that is a valid 0-row result. */

    return TSDB_OK;
}

/* ---- Main select execution ------------------------------------------- */

/* ---- Per-query deadline ------------------------------------------------ *
 *
 * Pre-fix nothing enforced any deadline.  request_timeout_ns lived in
 * server config but was only used to time the query for the metric
 * histogram — a runaway SELECT would happily run forever and pin a
 * connection thread.  The fix is a thread-local deadline checked at
 * block boundaries inside hot loops; cheap (clock_gettime once per
 * 8K-row block) but bounded.
 *
 * Tracking per-thread instead of per-query because the executor doesn't
 * thread a context pointer through every level — too invasive.  Server
 * code (handle_query) sets it before tsdb_query() and clears it after,
 * so concurrent connections don't share the deadline. */
static __thread int64_t g_query_deadline_ns = 0;

static inline int64_t now_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

int64_t tsdb_query_set_deadline_ns(int64_t deadline_monotonic_ns) {
    int64_t prev = g_query_deadline_ns;
    g_query_deadline_ns = deadline_monotonic_ns;
    return prev;
}

int tsdb_query_deadline_expired(void) {
    if (g_query_deadline_ns == 0) return 0;
    return now_monotonic_ns() > g_query_deadline_ns;
}

/* ORDER BY post-processing.
 *
 * Pre-fix the parser stored q->has_order / q->order_col / q->order_dir
 * but the executor never read them — `SELECT … ORDER BY ts DESC LIMIT 5`
 * returned rows in scan order (verified ASC and DESC produced identical
 * output).
 *
 * Implementation: build a permutation by sorting an index array, then
 * apply it column-by-column with a scratch buffer.  Runs once at the
 * end of every exec_select dispatch (plain / GROUP BY / SAMPLE BY /
 * LATEST ON / window) so every result shape is covered.
 *
 * Comparator state is kept in a thread-local struct since qsort lacks
 * a userdata pointer (qsort_r is non-portable across glibc/BSD).  This
 * is safe because tsdb_query is single-threaded per call — parallel
 * scans merge into the same result before we sort. */
typedef struct {
    const void  *col_data;
    tsdb_type_t  type;
    int          desc;
} ord_ctx_t;

static __thread ord_ctx_t g_ord_ctx;

static int order_cmp_idx(const void *pa, const void *pb) {
    size_t ia = *(const size_t *)pa;
    size_t ib = *(const size_t *)pb;
    int  c = 0;
    switch (g_ord_ctx.type) {
    case TSDB_TYPE_INT64:
    case TSDB_TYPE_TIMESTAMP: {
        int64_t va = ((const int64_t *)g_ord_ctx.col_data)[ia];
        int64_t vb = ((const int64_t *)g_ord_ctx.col_data)[ib];
        c = (va > vb) - (va < vb);
        break;
    }
    case TSDB_TYPE_FLOAT64: {
        double va = ((const double *)g_ord_ctx.col_data)[ia];
        double vb = ((const double *)g_ord_ctx.col_data)[ib];
        c = (va > vb) - (va < vb);
        break;
    }
    case TSDB_TYPE_SYMBOL: {
        uint32_t va = ((const uint32_t *)g_ord_ctx.col_data)[ia];
        uint32_t vb = ((const uint32_t *)g_ord_ctx.col_data)[ib];
        c = (va > vb) - (va < vb);
        break;
    }
    default:
        c = 0;
        break;
    }
    return g_ord_ctx.desc ? -c : c;
}

static int result_apply_order_by(tsdb_result_t *r, qast_query_t *q,
                                  char *err, size_t errcap) {
    if (!q || !q->has_order || !q->order_col || !r || r->nrows < 2)
        return TSDB_OK;

    int oc = -1;
    for (int i = 0; i < r->ncols; i++) {
        if (r->col_names[i] && strcmp(r->col_names[i], q->order_col) == 0) {
            oc = i; break;
        }
    }
    if (oc < 0) {
        eset(err, errcap, "ORDER BY column '%s' not in result schema",
             q->order_col);
        return TSDB_ERR_SCHEMA;
    }

    size_t n = r->nrows;
    size_t *idx = (size_t *)malloc(n * sizeof(size_t));
    if (!idx) return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n; i++) idx[i] = i;

    g_ord_ctx.col_data = r->col_data[oc];
    g_ord_ctx.type     = r->col_types[oc];
    g_ord_ctx.desc     = (q->order_dir == QAST_ORDER_DESC);
    qsort(idx, n, sizeof(size_t), order_cmp_idx);

    /* Permute each column by the index array.  Result columns store
     * uint32 for SYMBOL and uint64 for everything else (matching how
     * result_append_cell writes them).  All other shapes default to
     * 8-byte slots so a single per-column scratch buffer covers them. */
    for (int c = 0; c < r->ncols; c++) {
        size_t w = (r->col_types[c] == TSDB_TYPE_SYMBOL) ? 4 : 8;
        uint8_t *src = (uint8_t *)r->col_data[c];
        if (!src) continue;
        uint8_t *tmp = (uint8_t *)malloc(n * w);
        if (!tmp) { free(idx); return TSDB_ERR_NOMEM; }
        for (size_t i = 0; i < n; i++)
            memcpy(tmp + i * w, src + idx[i] * w, w);
        memcpy(src, tmp, n * w);
        free(tmp);
    }

    free(idx);
    return TSDB_OK;
}

static int exec_select(tsdb_db_t *db, qast_query_t *q, tsdb_result_t *r,
                       char *err, size_t errcap) {
    /* Check if FROM refers to a STable; if so, expand to union over children. */
    {
        tsdb_catalog_t *cat = tsdb_db_catalog(db);
        int from_is_stable = (cat && q->from && tsdb_stable_exists(cat, q->from));
        if (q->has_partition_by_tbname && !from_is_stable) {
            eset(err, errcap,
                 "PARTITION BY tbname requires FROM to be a super-table");
            return TSDB_ERR_PARSE;
        }
        if (from_is_stable) {
            tsdb_stable_t st;
            int rc = tsdb_stable_get(cat, q->from, &st);
            if (rc != TSDB_OK) { eset(err, errcap, "failed to fetch stable '%s'", q->from); return rc; }
            return exec_stable_select(db, q, &st, r, err, errcap);
        }
    }

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

    /* ASOF JOIN dispatch — handles its own projection, schema, and result setup. */
    if (q->has_asof_join) {
        return exec_asof_join(db, tbl, q, r, err, errcap);
    }

    /* Build projections. */
    proj_t *projs = NULL; int nprojs = 0, has_agg = 0, has_window = 0, has_ts_agg = 0;
    int has_interp = 0;
    int rc = build_projections(q, s, db, &projs, &nprojs, &has_agg, &has_window,
                               &has_ts_agg, &has_interp, err, errcap);
    if (rc != TSDB_OK) return rc;

    /* INTERP dispatch — grid-aligned linear interpolation.
     * Must be checked before the general window function validation below
     * because interp() is classified as a window call but has its own
     * execution path that allows combining with WHERE and ORDER BY. */
    if (has_interp) {
        if (has_agg || q->has_sample_by || q->ngroup_by > 0 ||
            q->has_latest_on || q->has_asof_join) {
            if (projs) { for (int pi = 0; pi < nprojs; pi++) proj_tdigest_free(&projs[pi]); free(projs); }
            eset(err, errcap,
                 "interp() cannot be combined with aggregates, GROUP BY, "
                 "SAMPLE BY, LATEST ON, or ASOF JOIN");
            return TSDB_ERR_UNSUPPORTED;
        }
        /* Allocate result columns. */
        rc = result_reserve_cols(r, nprojs);
        if (rc != TSDB_OK) { free(projs); return rc; }
        for (int i = 0; i < nprojs; i++) {
            rc = result_set_col(r, i, projs[i].name, projs[i].out_type, NULL);
            if (rc != TSDB_OK) { free(projs); return rc; }
        }
        int irc = exec_interp(db, tbl, q, r, projs, nprojs, err, errcap);
        free(projs);
        return irc;
    }

    /* Window functions only valid in plain non-agg SELECT (no GROUP BY,
     * no SAMPLE BY, no LATEST ON, no ASOF JOIN). */
    if (has_window && (has_agg || q->has_sample_by || q->ngroup_by > 0 ||
                       q->has_latest_on || q->has_asof_join)) {
        if (projs) { for (int pi = 0; pi < nprojs; pi++) proj_tdigest_free(&projs[pi]); free(projs); }
        eset(err, errcap,
             "window functions (diff/derivative/csum/mavg) cannot be combined with "
             "aggregates, GROUP BY, SAMPLE BY, LATEST ON, or ASOF JOIN");
        return TSDB_ERR_UNSUPPORTED;
    }

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

    /* LATEST ON dispatch — reverse-scan with per-partition-key early-exit. */
    if (q->has_latest_on) {
        int lrc = exec_latest_on(tbl, q, r, projs, nprojs, err, errcap);
        free(projs);
        return lrc;
    }

    /* GROUP BY dispatch — hash-aggregate with tuple keys.
     * Triggered only when SELECT includes one or more aggregate expressions
     * AND the query has a GROUP BY clause. Pure projection with GROUP BY
     * (no aggregates) is treated as DISTINCT, which we implement via the
     * same path by emitting one row per unique key with no agg updates. */
    if (q->ngroup_by > 0 && !q->has_sample_by) {
        int grc = exec_group_by(db, tbl, q, r, projs, nprojs, err, errcap);
        free(projs);
        return grc;
    }

    /* Extract ts bounds from WHERE once, so both file-level zone-map prune
     * (via scan_plan_build_ex) and per-block skip (in the worker loops) see
     * the same range. */
    ts_range_t plan_ts_prune;
    ts_range_init(&plan_ts_prune);
    if (q->where) extract_ts_bounds(q->where, s, &plan_ts_prune);

    /* Build scan plan with zone-map prune. */
    scan_plan_t plan = {0};
    rc = scan_plan_build_ex(&plan, tbl,
                             (plan_ts_prune.has_lo || plan_ts_prune.has_hi)
                             ? &plan_ts_prune : NULL);
    if (rc != TSDB_OK) { free(projs); return rc; }

    /* Special case: SAMPLE BY + has_agg: handle bucket-grouped aggregation.
     * Streaming emit: bucket state is flushed to result immediately when a new
     * bucket boundary is crossed. Only one bkt_state_t is live at any time.
     * Cross-source merging is correct because cur_state persists across the
     * outer source loop — a bucket that straddles two scan sources is merged
     * before emission.
     */

    int has_sample = q->has_sample_by;

    /* Single bucket accumulator for streaming SAMPLE BY. */
    typedef struct { int64_t bucket; double sum_f; int64_t sum_i; double min_f, max_f;
                     int64_t min_i, max_i; uint64_t count; } bkt_state_t;

    /* cur_state holds the bucket currently being aggregated.
     * cur_bucket == INT64_MIN means no bucket has been opened yet. */
    bkt_state_t cur_state;
    memset(&cur_state, 0, sizeof(cur_state));
    cur_state.min_f = INFINITY;  cur_state.max_f = -INFINITY;
    cur_state.min_i = INT64_MAX; cur_state.max_i = INT64_MIN;
    int64_t cur_bucket = INT64_MIN;  /* sentinel: no active bucket */

    /* ---- Advanced window extra state ------------------------------------ */
    int64_t adv_prev_ts = INT64_MIN;
    uint64_t adv_prev_state = 0;
    int      adv_prev_state_valid = 0;
    int     adv_inside_window = 0;
    int64_t adv_win_start_ts  = 0;
    int adv_state_col_idx = -1;
    if (q->has_adv_window && q->adv_window_kind == QAST_WIN_STATE && q->state_col)
        adv_state_col_idx = resolve_col(s, q->state_col);

    size_t rows_emitted = 0;
    size_t limit = q->has_limit ? (size_t)q->limit : SIZE_MAX;

    /* Scratch declared up front so all goto-done paths can free it. */
    void *serial_agg_scratch = NULL;

    /* ---- Parallel aggregate path --------------------------------------- */
    /* Conditions: agg query, no SAMPLE BY, more than 1 source, parallel enabled.
     * Each worker scans its slice of sources with a private proj_t copy,
     * then main thread merges all partial states. */
    pthread_once(&g_pool_once, init_pool);

    /* has_ts_agg requires row-by-row ts access — disable parallel for now */
    int use_parallel = has_agg && !has_sample && !q->has_adv_window
                       && !has_ts_agg && plan.nsrcs > 1
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
                for (int j = 0; j < w; j++) {
                    for (int pi = 0; pi < nprojs; pi++) proj_tdigest_free(&tasks[j].projs[pi]);
                    free(tasks[j].projs);
                }
                free(tasks);
                rc = TSDB_ERR_NOMEM;
                goto done;
            }
            memcpy(t->projs, projs, (size_t)nprojs * sizeof(proj_t));
            /* Deep-copy tdigest pointers — each worker needs its own empty digest. */
            for (int pi = 0; pi < nprojs; pi++) {
                if (t->projs[pi].kind >= PROJ_AGG_TDIGEST_FIRST &&
                    t->projs[pi].kind < PROJ_AGG_TS_KIND_FIRST) {
                    t->projs[pi].tdigest = NULL; /* clear shallow-copied pointer */
                    if (proj_tdigest_init(&t->projs[pi]) != 0) {
                        /* cleanup all already allocated */
                        for (int pk = 0; pk < pi; pk++) proj_tdigest_free(&t->projs[pk]);
                        free(t->projs);
                        for (int j = 0; j < w; j++) {
                            for (int pk = 0; pk < nprojs; pk++) proj_tdigest_free(&tasks[j].projs[pk]);
                            free(tasks[j].projs);
                        }
                        free(tasks);
                        rc = TSDB_ERR_NOMEM;
                        goto done;
                    }
                }
            }
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
                    case PROJ_AGG_SPREAD:
                        if (pp->agg_min_f < mp->agg_min_f) mp->agg_min_f = pp->agg_min_f;
                        if (pp->agg_min_i < mp->agg_min_i) mp->agg_min_i = pp->agg_min_i;
                        if (pp->agg_max_f > mp->agg_max_f) mp->agg_max_f = pp->agg_max_f;
                        if (pp->agg_max_i > mp->agg_max_i) mp->agg_max_i = pp->agg_max_i;
                        mp->agg_count += pp->agg_count;
                        break;
                    case PROJ_AGG_P50:
                    case PROJ_AGG_P90:
                    case PROJ_AGG_P99:
                    case PROJ_AGG_PERCENTILE:
                    case PROJ_AGG_STDDEV:
                        /* Merge worker tdigest into master; free worker copy. */
                        if (mp->tdigest && pp->tdigest)
                            tsdb_tdigest_merge(mp->tdigest, pp->tdigest);
                        proj_tdigest_free(pp);
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

            /* Free master tdigests now that we've written results. */
            for (int pi = 0; pi < nprojs; pi++) proj_tdigest_free(&projs[pi]);
            free(projs);
            scan_plan_free(&plan);
            return rc;
        }

        /* Error path: free worker tdigests and data. */
        for (int w = 0; w < nactive; w++) {
            for (int pi = 0; pi < nprojs; pi++) proj_tdigest_free(&tasks[w].projs[pi]);
            free(tasks[w].projs);
        }
        free(tasks);
        for (int pi = 0; pi < nprojs; pi++) proj_tdigest_free(&projs[pi]);
        free(projs);
        scan_plan_free(&plan);
        return rc;
    }
    /* ---- End parallel path ---------------------------------------------- */

    /* Extract ts bounds once for serial block skipping. */
    ts_range_t ts_r;
    ts_range_init(&ts_r);
    if (q->where) extract_ts_bounds(q->where, s, &ts_r);

    /* Extract AND-connected SYMBOL EQ constraints for Bloom pre-filter. */
    bloom_constraint_t bloom_bc[BLOOM_CONSTRAINT_MAX];
    int bloom_nbc = 0;
    if (q->where) {
        bloom_nbc = extract_bloom_constraints(q->where, s,
                                               bloom_bc, BLOOM_CONSTRAINT_MAX);
    }

    /* Reset bloom stats for this query. */
    g_bloom_blocks_skipped = 0;
    g_bloom_blocks_total   = 0;

    /* SIMD gather scratch (64 KB), reused across blocks for the serial path. */
    if (has_agg) {
        serial_agg_scratch = aligned_alloc(32, (size_t)TSDB_BLOCK_POINTS * 8);
        if (!serial_agg_scratch) { rc = TSDB_ERR_NOMEM; goto done; }
    }

    /* Iterate sources. */
    for (size_t si = 0; si < plan.nsrcs && rows_emitted < limit; si++) {
        /* Per-query deadline check at block boundary — see
         * tsdb_query_set_deadline_ns().  Fires once per source (block
         * group) so a runaway scan can be killed without per-row
         * overhead. */
        if (tsdb_query_deadline_expired()) { rc = TSDB_ERR_TIMEOUT; goto done; }

        scan_src_t *src = &plan.srcs[si];
        size_t n = src->row_count;

        /* Block skip via extracted ts bounds. */
        if ((ts_r.has_lo || ts_r.has_hi) &&
            ts_range_excludes(&ts_r, src->ts_min, src->ts_max))
            continue;

        /* Bloom filter pre-check: skip block if SYMBOL is provably absent.
         * Only applies to disk blocks (memtable has no bloom). */
        if (bloom_nbc > 0 && src->part) {
            g_bloom_blocks_total++;
            tsdb_metric_inc("qengine_bloom_lookups_total");
            if (bloom_can_skip_block(src->part, s, src, bloom_bc, bloom_nbc)) {
                g_bloom_blocks_skipped++;
                tsdb_metric_inc("qengine_bloom_skips_total");
                continue;
            }
        }

        /* Stats fast-path (serial).  Same shape as the parallel worker.
         * Gate is conservative: simple aggregate query, no predicate,
         * no windowing, no grouping — anything more complex falls to
         * the full scan where the existing code already handles it. */
        {
            static int fp_off = -1;
            if (fp_off < 0) {
                const char *e = getenv("TSDB_DISABLE_STATS_FASTPATH");
                fp_off = (e && e[0] && e[0] != '0') ? 1 : 0;
            }
            int eligible_query = has_agg && !q->where
                               && !q->has_sample_by
                               && !q->has_adv_window
                               && q->ngroup_by == 0;
            if (eligible_query && !fp_off && !src->mem) {
                if (try_stats_fastpath(src, projs, nprojs, s, &ts_r, 1)) {
                    tsdb_metric_inc("qengine_agg_stats_hit_total");
                    continue;
                }
                tsdb_metric_inc("qengine_agg_stats_miss_total");
            }
        }

        /* Allocate per-column decode buffers for this source. */
        void **bufs = calloc((size_t)s->ncols, sizeof(void *));
        if (!bufs) { rc = TSDB_ERR_NOMEM; goto done; }
        tsdb_symtab_t **syms = calloc((size_t)s->ncols, sizeof(tsdb_symtab_t *));
        if (!syms) { free(bufs); rc = TSDB_ERR_NOMEM; goto done; }

        int need_col[TSDB_MAX_COLS] = {0};
        /* Columns needed: from projections + from WHERE */
        for (int i = 0; i < nprojs; i++) {
            if (projs[i].col >= 0) need_col[projs[i].col] = 1;
            /* Multi-arg UDFs reference columns beyond proj.col. */
            if (projs[i].kind == PROJ_UDF_SCALAR) {
                for (int ai = 0; ai < projs[i].udf_nargs; ai++) {
                    int ci = projs[i].udf_arg_cols[ai];
                    if (ci >= 0) need_col[ci] = 1;
                }
            }
        }
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

        /* Always load ts col if SAMPLE BY, advanced window, or any window fn */
        if (has_sample || q->has_adv_window || has_window || has_ts_agg) need_col[s->ts_col_idx] = 1;
        /* STATE_WINDOW: also pre-load the state column. */
        if (q->has_adv_window && q->adv_window_kind == QAST_WIN_STATE && q->state_col) {
            int sc = resolve_col(s, q->state_col);
            if (sc >= 0) need_col[sc] = 1;
        }
        /* EVENT_WINDOW: mark columns referenced in start/end expressions. */
        if (q->has_adv_window && q->adv_window_kind == QAST_WIN_EVENT) {
            qast_expr_t *estk[64]; int etp = 0;
            if (q->event_start_expr) estk[etp++] = q->event_start_expr;
            if (q->event_end_expr)   estk[etp++] = q->event_end_expr;
            while (etp > 0) {
                qast_expr_t *ee = estk[--etp];
                if (!ee) continue;
                if (ee->kind == QAST_IDENT) { int ec = resolve_col(s, ee->v.s); if (ec >= 0) need_col[ec] = 1; }
                if (ee->lhs && etp < 64) estk[etp++] = ee->lhs;
                if (ee->rhs && etp < 64) estk[etp++] = ee->rhs;
                for (int ei = 0; ei < ee->nargs && etp < 64; ei++) estk[etp++] = ee->args[ei];
            }
        }

        for (int c = 0; c < s->ncols; c++) {
            if (!need_col[c]) continue;
            syms[c] = s->cols[c].symtab;
            size_t w = tsdb_type_width(s->cols[c].type);
            if (src->mem) {
                bufs[c] = src->mem_bufs[c];
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
        if (has_agg && !has_sample && !q->has_adv_window) {
            /* Single-row aggregate over all rows matching. */
            for (int pi = 0; pi < nprojs; pi++) {
                if (projs[pi].kind >= PROJ_AGG_RANGE_BEGIN && projs[pi].kind <= PROJ_AGG_RANGE_END)
                    agg_update(&projs[pi], s, bufs, n, bm, serial_agg_scratch);
            }
        } else if (has_agg && has_sample) {
            /* Streaming bucket aggregation: emit each bucket as soon as we
             * observe a new bucket boundary.  cur_state persists across source
             * boundaries so that a bucket spanning two scan sources is merged
             * correctly — no double-emit, no lost rows. */
            const int64_t *tscol = (const int64_t *)bufs[s->ts_col_idx];
            int64_t bnum = q->sample_by.ns;
            if (bnum <= 0) bnum = 1;
            for (size_t i = 0; i < n; i++) {
                if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
                int64_t b = tscol[i] - (tscol[i] % bnum);

                if (b != cur_bucket) {
                    /* Emit the completed bucket that just closed. */
                    if (cur_bucket != INT64_MIN && cur_state.count > 0) {
                        rc = result_reserve_rows(r, r->nrows + 1);
                        if (rc != TSDB_OK) {
                            free(bm);
                            for (int c = 0; c < s->ncols; c++)
                                if (!src->mem && bufs[c]) free(bufs[c]);
                            free(bufs); free(syms); goto done;
                        }
                        for (int pi = 0; pi < nprojs; pi++) {
                            proj_t *p = &projs[pi];
                            if (p->kind == PROJ_TS_BUCKET) {
                                uint64_t bits; memcpy(&bits, &cur_state.bucket, 8);
                                result_append_cell(r, pi, bits);
                            } else if (p->kind == PROJ_AGG_SUM) {
                                double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                           ? cur_state.sum_f : (double)cur_state.sum_i;
                                uint64_t bits; memcpy(&bits, &v, 8);
                                result_append_cell(r, pi, bits);
                            } else if (p->kind == PROJ_AGG_AVG) {
                                double v = 0;
                                if (cur_state.count > 0) {
                                    v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                        ? (cur_state.sum_f / (double)cur_state.count)
                                        : ((double)cur_state.sum_i / (double)cur_state.count);
                                }
                                uint64_t bits; memcpy(&bits, &v, 8);
                                result_append_cell(r, pi, bits);
                            } else if (p->kind == PROJ_AGG_MIN) {
                                double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                           ? cur_state.min_f : (double)cur_state.min_i;
                                uint64_t bits; memcpy(&bits, &v, 8);
                                result_append_cell(r, pi, bits);
                            } else if (p->kind == PROJ_AGG_MAX) {
                                double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                           ? cur_state.max_f : (double)cur_state.max_i;
                                uint64_t bits; memcpy(&bits, &v, 8);
                                result_append_cell(r, pi, bits);
                            } else if (p->kind == PROJ_AGG_COUNT) {
                                int64_t v = (int64_t)cur_state.count;
                                uint64_t bits; memcpy(&bits, &v, 8);
                                result_append_cell(r, pi, bits);
                            } else {
                                /* PROJ_COL not supported in bucket mode */
                                result_append_cell(r, pi, 0);
                            }
                        }
                        r->nrows++;
                        rows_emitted++;
                        /* LIMIT pushdown: stop scanning further rows if limit hit */
                        if (q->has_limit && rows_emitted >= limit) break;
                    }

                    /* Open fresh bucket state for b. */
                    memset(&cur_state, 0, sizeof(cur_state));
                    cur_state.min_f =  INFINITY;
                    cur_state.max_f = -INFINITY;
                    cur_state.min_i = INT64_MAX;
                    cur_state.max_i = INT64_MIN;
                    cur_state.bucket = b;
                    cur_bucket = b;
                }

                /* Accumulate row into current bucket. */
                for (int pi = 0; pi < nprojs; pi++) {
                    if (projs[pi].kind < PROJ_AGG_RANGE_BEGIN || projs[pi].kind > PROJ_AGG_COUNT) continue;
                    int col = projs[pi].col;
                    if (projs[pi].kind == PROJ_AGG_COUNT) { cur_state.count++; continue; }
                    if (col < 0) continue;
                    tsdb_type_t ct = s->cols[col].type;
                    if (ct == TSDB_TYPE_FLOAT64) {
                        double x = ((const double *)bufs[col])[i];
                        cur_state.sum_f += x;
                        if (x < cur_state.min_f) cur_state.min_f = x;
                        if (x > cur_state.max_f) cur_state.max_f = x;
                    } else {
                        int64_t x = ((const int64_t *)bufs[col])[i];
                        cur_state.sum_i += x;
                        if (x < cur_state.min_i) cur_state.min_i = x;
                        if (x > cur_state.max_i) cur_state.max_i = x;
                    }
                    cur_state.count++;
                }
            }
            /* Early-exit the source loop if LIMIT already satisfied */
            if (q->has_limit && rows_emitted >= limit) {
                free(bm);
                for (int c = 0; c < s->ncols; c++)
                    if (!src->mem && bufs[c]) free(bufs[c]);
                free(bufs); free(syms);
                goto post_scan;
            }
        } else if (q->has_adv_window && has_agg) {
            /* ---- Advanced window aggregation (SESSION/STATE_WINDOW/EVENT_WINDOW) ----
             * Shares bkt_state_t cur_state / cur_bucket declared above.
             */
            const int64_t *adv_ts = (const int64_t *)bufs[s->ts_col_idx];

/* Local macros for advanced window (undef'd at end of block) */
#define ADW_FLUSH(start_ts_val)                                                    \
    do {                                                                           \
        if (cur_state.count > 0) {                                                 \
            rc = result_reserve_rows(r, r->nrows + 1);                             \
            if (rc != TSDB_OK) {                                                   \
                free(bm);                                                          \
                for (int _ca = 0; _ca < s->ncols; _ca++)                           \
                    if (!src->mem && bufs[_ca]) free(bufs[_ca]);                   \
                free(bufs); free(syms); goto done;                                 \
            }                                                                      \
            for (int _p2 = 0; _p2 < nprojs; _p2++) {                             \
                proj_t *_pp = &projs[_p2];                                        \
                if (_pp->kind == PROJ_TS_BUCKET) {                                \
                    int64_t _sv = (start_ts_val);                                 \
                    uint64_t _bb; memcpy(&_bb, &_sv, 8);                          \
                    result_append_cell(r, _p2, _bb);                              \
                } else if (_pp->kind == PROJ_AGG_SUM) {                           \
                    double _vv = (_pp->col >= 0 &&                                \
                        s->cols[_pp->col].type == TSDB_TYPE_FLOAT64)              \
                        ? cur_state.sum_f : (double)cur_state.sum_i;              \
                    uint64_t _bb; memcpy(&_bb, &_vv, 8);                          \
                    result_append_cell(r, _p2, _bb);                              \
                } else if (_pp->kind == PROJ_AGG_AVG) {                           \
                    double _vv = 0;                                                \
                    if (cur_state.count > 0) {                                    \
                        _vv = (_pp->col >= 0 &&                                   \
                            s->cols[_pp->col].type == TSDB_TYPE_FLOAT64)          \
                            ? (cur_state.sum_f / (double)cur_state.count)         \
                            : ((double)cur_state.sum_i / (double)cur_state.count); \
                    }                                                              \
                    uint64_t _bb; memcpy(&_bb, &_vv, 8);                          \
                    result_append_cell(r, _p2, _bb);                              \
                } else if (_pp->kind == PROJ_AGG_MIN) {                           \
                    double _vv = (_pp->col >= 0 &&                                \
                        s->cols[_pp->col].type == TSDB_TYPE_FLOAT64)              \
                        ? cur_state.min_f : (double)cur_state.min_i;              \
                    uint64_t _bb; memcpy(&_bb, &_vv, 8);                          \
                    result_append_cell(r, _p2, _bb);                              \
                } else if (_pp->kind == PROJ_AGG_MAX) {                           \
                    double _vv = (_pp->col >= 0 &&                                \
                        s->cols[_pp->col].type == TSDB_TYPE_FLOAT64)              \
                        ? cur_state.max_f : (double)cur_state.max_i;              \
                    uint64_t _bb; memcpy(&_bb, &_vv, 8);                          \
                    result_append_cell(r, _p2, _bb);                              \
                } else if (_pp->kind == PROJ_AGG_COUNT) {                         \
                    int64_t _vv = (int64_t)cur_state.count;                       \
                    uint64_t _bb; memcpy(&_bb, &_vv, 8);                          \
                    result_append_cell(r, _p2, _bb);                              \
                } else {                                                           \
                    result_append_cell(r, _p2, 0);                                \
                }                                                                  \
            }                                                                      \
            r->nrows++;                                                            \
            rows_emitted++;                                                        \
        }                                                                          \
    } while (0)

#define ADW_OPEN(ts_val)                                                           \
    do {                                                                           \
        memset(&cur_state, 0, sizeof(cur_state));                                  \
        cur_state.min_f =  INFINITY; cur_state.max_f = -INFINITY;                 \
        cur_state.min_i = INT64_MAX; cur_state.max_i = INT64_MIN;                 \
        cur_bucket = (ts_val);                                                     \
    } while (0)

#define ADW_ACC(ri)                                                                \
    do {                                                                           \
        /* Increment row count once per row (not per projection). */              \
        cur_state.count++;                                                         \
        for (int _pa = 0; _pa < nprojs; _pa++) {                                  \
            if (projs[_pa].kind < PROJ_AGG_RANGE_BEGIN ||                         \
                projs[_pa].kind > PROJ_AGG_COUNT) continue;                       \
            if (projs[_pa].kind == PROJ_AGG_COUNT) continue; /* row count above */ \
            int _caa = projs[_pa].col;                                            \
            if (_caa < 0) continue;                                               \
            if (s->cols[_caa].type == TSDB_TYPE_FLOAT64) {                        \
                double _xa = ((const double *)bufs[_caa])[(ri)];                  \
                cur_state.sum_f += _xa;                                           \
                if (_xa < cur_state.min_f) cur_state.min_f = _xa;                 \
                if (_xa > cur_state.max_f) cur_state.max_f = _xa;                 \
            } else {                                                               \
                int64_t _xa = ((const int64_t *)bufs[_caa])[(ri)];                \
                cur_state.sum_i += _xa;                                           \
                if (_xa < cur_state.min_i) cur_state.min_i = _xa;                 \
                if (_xa > cur_state.max_i) cur_state.max_i = _xa;                 \
            }                                                                      \
        }                                                                          \
    } while (0)

            if (q->adv_window_kind == QAST_WIN_SESSION) {
                int64_t gap_ns = q->session_gap_ns;
                if (gap_ns <= 0) gap_ns = 1;
                for (size_t i = 0; i < n && rows_emitted < limit; i++) {
                    if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
                    int64_t ts = adv_ts[i];
                    if (cur_bucket != INT64_MIN && (ts - adv_prev_ts) > gap_ns) {
                        ADW_FLUSH(cur_bucket);
                        ADW_OPEN(ts);
                    }
                    if (cur_bucket == INT64_MIN) ADW_OPEN(ts);
                    adv_prev_ts = ts;
                    ADW_ACC(i);
                }
            } else if (q->adv_window_kind == QAST_WIN_STATE) {
                if (adv_state_col_idx < 0) {
                    eset(err, errcap, "STATE_WINDOW: column '%s' not found",
                         q->state_col ? q->state_col : "?");
                    rc = TSDB_ERR_SCHEMA;
                    free(bm);
                    for (int c2 = 0; c2 < s->ncols; c2++)
                        if (!src->mem && bufs[c2]) free(bufs[c2]);
                    free(bufs); free(syms); goto done;
                }
                size_t sw = tsdb_type_width(s->cols[adv_state_col_idx].type);
                for (size_t i = 0; i < n && rows_emitted < limit; i++) {
                    if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
                    int64_t ts = adv_ts[i];
                    uint64_t sv = 0;
                    if (sw == 8)      sv = ((const uint64_t *)bufs[adv_state_col_idx])[i];
                    else if (sw == 4) sv = ((const uint32_t *)bufs[adv_state_col_idx])[i];
                    else if (sw == 2) sv = ((const uint16_t *)bufs[adv_state_col_idx])[i];
                    else if (sw == 1) sv = ((const uint8_t  *)bufs[adv_state_col_idx])[i];
                    if (adv_prev_state_valid && sv != adv_prev_state) {
                        ADW_FLUSH(cur_bucket);
                        ADW_OPEN(ts);
                    }
                    if (cur_bucket == INT64_MIN) ADW_OPEN(ts);
                    adv_prev_ts = ts;
                    adv_prev_state = sv;
                    adv_prev_state_valid = 1;
                    ADW_ACC(i);
                }
            } else if (q->adv_window_kind == QAST_WIN_EVENT) {
                size_t nbm2 = (n + 63) / 64;
                uint64_t *sbm = calloc(nbm2, sizeof(uint64_t));
                uint64_t *ebm = calloc(nbm2, sizeof(uint64_t));
                if (!sbm || !ebm) {
                    free(sbm); free(ebm); free(bm);
                    for (int c2 = 0; c2 < s->ncols; c2++) if (!src->mem && bufs[c2]) free(bufs[c2]);
                    free(bufs); free(syms); rc = TSDB_ERR_NOMEM; goto done;
                }
                for (size_t wb = 0; wb < nbm2; wb++) { sbm[wb] = ~(uint64_t)0; ebm[wb] = ~(uint64_t)0; }
                if (n % 64 != 0) {
                    uint64_t mask2 = (~(uint64_t)0) >> (64 - n % 64);
                    sbm[nbm2-1] &= mask2; ebm[nbm2-1] &= mask2;
                }
                eval_ctx_t ectx2;
                memset(&ectx2, 0, sizeof(ectx2));
                ectx2.schema = s; ectx2.col_bufs = bufs; ectx2.col_syms = syms; ectx2.nrows = n;
                int evrc = apply_filter_expr(&ectx2, q->event_start_expr, sbm);
                if (evrc != TSDB_OK) {
                    if (ectx2.err[0]) eset(err, errcap, "%s", ectx2.err);
                    free(sbm); free(ebm); free(bm);
                    for (int c2 = 0; c2 < s->ncols; c2++) if (!src->mem && bufs[c2]) free(bufs[c2]);
                    free(bufs); free(syms); rc = evrc; goto done;
                }
                memset(&ectx2, 0, sizeof(ectx2));
                ectx2.schema = s; ectx2.col_bufs = bufs; ectx2.col_syms = syms; ectx2.nrows = n;
                evrc = apply_filter_expr(&ectx2, q->event_end_expr, ebm);
                if (evrc != TSDB_OK) {
                    if (ectx2.err[0]) eset(err, errcap, "%s", ectx2.err);
                    free(sbm); free(ebm); free(bm);
                    for (int c2 = 0; c2 < s->ncols; c2++) if (!src->mem && bufs[c2]) free(bufs[c2]);
                    free(bufs); free(syms); rc = evrc; goto done;
                }
                for (size_t i = 0; i < n && rows_emitted < limit; i++) {
                    int64_t ts = adv_ts[i];
                    int is_s = (int)((sbm[i/64] >> (i%64)) & 1);
                    int is_e = (int)((ebm[i/64] >> (i%64)) & 1);
                    if (!adv_inside_window && is_s) {
                        adv_inside_window = 1;
                        adv_win_start_ts  = ts;
                        ADW_OPEN(ts);
                    }
                    if (adv_inside_window) {
                        ADW_ACC(i);
                        adv_prev_ts = ts;
                        if (is_e) {
                            ADW_FLUSH(adv_win_start_ts);
                            cur_bucket = INT64_MIN;
                            adv_inside_window = 0;
                        }
                    }
                }
                free(sbm); free(ebm);
            }

#undef ADW_FLUSH
#undef ADW_OPEN
#undef ADW_ACC

            /* Early-exit the source loop if LIMIT already satisfied */
            if (q->has_limit && rows_emitted >= limit) {
                free(bm);
                for (int c2 = 0; c2 < s->ncols; c2++)
                    if (!src->mem && bufs[c2]) free(bufs[c2]);
                free(bufs); free(syms);
                goto post_scan;
            }
        } else {
            /* Simple row projection (and window functions). */
            const int64_t *ts_col_buf =
                (const int64_t *)bufs[s->ts_col_idx];

            /* Hoist the growth check: worst case is every row in this block
             * passes the filter.  One reserve call covers all rows; the
             * per-row reserve inside the loop becomes redundant. */
            {
                size_t upper = r->nrows + n;
                if (q->has_limit && upper > (size_t)limit) upper = (size_t)limit;
                if (upper > r->cap_rows)
                    result_reserve_rows_grow(r, upper);
            }

            /* Precompute per-projection source pointers + widths.  In the
             * inner row loop these values are constant, so we hoist them
             * out to dodge per-row schema lookups + width branches. */
            const void *proj_src[TSDB_MAX_COLS];
            uint8_t     proj_w   [TSDB_MAX_COLS];
            int         all_simple_w8 = (nprojs > 0);
            for (int pi = 0; pi < nprojs; pi++) {
                proj_src[pi] = NULL;
                proj_w  [pi] = 0;
                proj_t *pp = &projs[pi];
                if (pp->kind == PROJ_COL && pp->col >= 0) {
                    proj_w  [pi] = (uint8_t)tsdb_type_width(s->cols[pp->col].type);
                    proj_src[pi] = bufs[pp->col];
                    if (proj_w[pi] != 8) all_simple_w8 = 0;
                } else {
                    all_simple_w8 = 0;
                }
            }

            /* Fast path: all projections are 8-byte column reads (the
             * common shape for SELECT col1, col2, … FROM t WHERE …).
             * Iterate the bitmap word-by-word; use CTZ to jump between
             * set bits without per-position bit tests — big win at low
             * selectivity, neutral at high. */
            if (all_simple_w8) {
                size_t nwords = (n + 63) / 64;
                for (size_t wi = 0; wi < nwords; wi++) {
                    uint64_t m = bm[wi];
                    while (m) {
                        if (rows_emitted >= limit) goto limit_hit;
                        int bit = __builtin_ctzll(m);
                        m &= (m - 1);
                        size_t i = wi * 64 + (size_t)bit;
                        if (i >= n) break;
                        size_t outrow = r->nrows;
                        for (int pi = 0; pi < nprojs; pi++) {
                            ((uint64_t *)r->col_data[pi])[outrow] =
                                ((const uint64_t *)proj_src[pi])[i];
                        }
                        r->nrows = outrow + 1;
                        rows_emitted++;
                    }
                }
            limit_hit:;
                /* fast-path emitted; skip the generic loop below. */
                goto row_proj_done;
            }

            for (size_t i = 0; i < n; i++) {
                if (!(bm[i / 64] & ((uint64_t)1 << (i % 64)))) continue;
                if (rows_emitted >= limit) break;

                int64_t cur_ts = ts_col_buf ? ts_col_buf[i] : 0;

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
                        int64_t b  = ts - (ts % p->bucket_ns);
                        uint64_t bits;
                        memcpy(&bits, &b, 8);
                        result_append_cell(r, pi, bits);
                    } else if (p->kind >= PROJ_WIN_FIRST &&
                               p->kind <= PROJ_WIN_LAST) {
                        /* Window function per-row compute. */
                        tsdb_type_t ct = s->cols[p->col].type;
                        double cur_v = (ct == TSDB_TYPE_FLOAT64)
                            ? ((const double   *)bufs[p->col])[i]
                            : (double)((const int64_t *)bufs[p->col])[i];
                        double out_v = NAN;
                        switch (p->kind) {
                        case PROJ_WIN_DIFF:
                            out_v = p->win_has_prev
                                    ? cur_v - p->win_prev_f : NAN;
                            p->win_prev_f   = cur_v;
                            p->win_has_prev = 1;
                            break;
                        case PROJ_WIN_DERIVATIVE:
                            if (p->win_has_prev) {
                                int64_t dt = cur_ts - p->win_prev_ts;
                                out_v = (dt == 0) ? 0.0
                                      : (cur_v - p->win_prev_f)
                                        / (double)dt * 1.0e9;
                            }
                            p->win_prev_f   = cur_v;
                            p->win_prev_ts  = cur_ts;
                            p->win_has_prev = 1;
                            break;
                        case PROJ_WIN_CSUM:
                            p->win_csum += cur_v;
                            out_v = p->win_csum;
                            p->win_has_prev = 1;
                            break;
                        case PROJ_WIN_MAVG: {
                            int ww = p->mavg_window;
                            if (p->mavg_n < ww) {
                                int slot = (p->mavg_head + p->mavg_n) % ww;
                                p->mavg_buf[slot] = cur_v;
                                p->mavg_sum += cur_v;
                                p->mavg_n++;
                                out_v = p->mavg_sum / (double)p->mavg_n;
                            } else {
                                double evict = p->mavg_buf[p->mavg_head];
                                p->mavg_sum = p->mavg_sum - evict + cur_v;
                                p->mavg_buf[p->mavg_head] = cur_v;
                                p->mavg_head = (p->mavg_head + 1) % ww;
                                out_v = p->mavg_sum / (double)ww;
                            }
                            p->win_has_prev = 1;
                            break;
                        }
                        case PROJ_WIN_LAG: {
                            /* mavg_buf is reused as a length-N ring of past
                             * values.  While it's filling we emit NaN; once
                             * full the head slot holds the value from N rows
                             * ago — emit it, then overwrite with cur_v. */
                            int N = p->mavg_window;
                            if (p->mavg_n < N) {
                                int slot = (p->mavg_head + p->mavg_n) % N;
                                p->mavg_buf[slot] = cur_v;
                                p->mavg_n++;
                                out_v = NAN;
                            } else {
                                out_v = p->mavg_buf[p->mavg_head];
                                p->mavg_buf[p->mavg_head] = cur_v;
                                p->mavg_head = (p->mavg_head + 1) % N;
                            }
                            p->win_has_prev = 1;
                            break;
                        }
                        default:
                            out_v = NAN;
                            break;
                        }
                        uint64_t bits;
                        memcpy(&bits, &out_v, 8);
                        result_append_cell(r, pi, bits);
                    } else if (p->kind == PROJ_UDF_SCALAR) {
                        /* Per-row scalar UDF call.
                         * v1 path invokes the vectorised ABI with n=1 for
                         * simplicity; batching across the block is a follow-up
                         * optimisation. */
                        uint64_t in_vals[TSDB_UDF_MAX_ARGS] = {0};
                        const void *argp[TSDB_UDF_MAX_ARGS] = {0};
                        for (int ai = 0; ai < p->udf_nargs; ai++) {
                            int ci = p->udf_arg_cols[ai];
                            in_vals[ai] = ((const uint64_t *)bufs[ci])[i];
                            argp[ai] = &in_vals[ai];
                        }
                        uint64_t out_bits = 0;
                        tsdb_udf_ctx_t ctx = { .abi_version = TSDB_UDF_ABI_V1 };
                        int urc = p->udf_fn(&ctx, argp, 1, &out_bits);
                        if (urc != TSDB_UDF_OK) out_bits = 0;
                        result_append_cell(r, pi, out_bits);
                    }
                }
                r->nrows++;
                rows_emitted++;
            }
        row_proj_done:;
        }

        free(bm);
        for (int c = 0; c < s->ncols; c++)
            if (!src->mem && bufs[c]) free(bufs[c]);
        free(bufs); free(syms);
    }

post_scan:
    /* Write aggregates or final bucket flush. */
    if (has_agg && !has_sample && !q->has_adv_window) {
        rc = result_reserve_rows(r, 1);
        if (rc != TSDB_OK) goto done;
        for (int pi = 0; pi < nprojs; pi++) agg_write(&projs[pi], s, r, pi);
        r->nrows = 1;
        /* tdigests freed by the done: path */
    } else if (has_agg && has_sample) {
        /* Flush the last (still-open) bucket, if any rows were seen and
         * the LIMIT has not already been reached. */
        if (cur_bucket != INT64_MIN && cur_state.count > 0 &&
            !(q->has_limit && rows_emitted >= limit)) {
            rc = result_reserve_rows(r, r->nrows + 1);
            if (rc != TSDB_OK) goto done;
            for (int pi = 0; pi < nprojs; pi++) {
                proj_t *p = &projs[pi];
                if (p->kind == PROJ_TS_BUCKET) {
                    uint64_t bits; memcpy(&bits, &cur_state.bucket, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_SUM) {
                    double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                               ? cur_state.sum_f : (double)cur_state.sum_i;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_AVG) {
                    double v = 0;
                    if (cur_state.count > 0) {
                        v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                            ? (cur_state.sum_f / (double)cur_state.count)
                            : ((double)cur_state.sum_i / (double)cur_state.count);
                    }
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_MIN) {
                    double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                               ? cur_state.min_f : (double)cur_state.min_i;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_MAX) {
                    double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                               ? cur_state.max_f : (double)cur_state.max_i;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else if (p->kind == PROJ_AGG_COUNT) {
                    int64_t v = (int64_t)cur_state.count;
                    uint64_t bits; memcpy(&bits, &v, 8);
                    result_append_cell(r, pi, bits);
                } else {
                    /* PROJ_COL not supported in bucket mode */
                    result_append_cell(r, pi, 0);
                }
            }
            r->nrows++;
        }
    } else if (q->has_adv_window && has_agg) {
        /* Flush the last open advanced window bucket if one is still open. */
        if (cur_bucket != INT64_MIN && cur_state.count > 0 &&
            !(q->has_limit && rows_emitted >= limit)) {
            rc = result_reserve_rows(r, r->nrows + 1);
            if (rc == TSDB_OK) {
                int64_t win_start = cur_bucket;
                for (int pi = 0; pi < nprojs; pi++) {
                    proj_t *p = &projs[pi];
                    if (p->kind == PROJ_TS_BUCKET) {
                        uint64_t bits; memcpy(&bits, &win_start, 8);
                        result_append_cell(r, pi, bits);
                    } else if (p->kind == PROJ_AGG_SUM) {
                        double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                   ? cur_state.sum_f : (double)cur_state.sum_i;
                        uint64_t bits; memcpy(&bits, &v, 8);
                        result_append_cell(r, pi, bits);
                    } else if (p->kind == PROJ_AGG_AVG) {
                        double v = 0;
                        if (cur_state.count > 0) {
                            v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                ? (cur_state.sum_f / (double)cur_state.count)
                                : ((double)cur_state.sum_i / (double)cur_state.count);
                        }
                        uint64_t bits; memcpy(&bits, &v, 8);
                        result_append_cell(r, pi, bits);
                    } else if (p->kind == PROJ_AGG_MIN) {
                        double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                   ? cur_state.min_f : (double)cur_state.min_i;
                        uint64_t bits; memcpy(&bits, &v, 8);
                        result_append_cell(r, pi, bits);
                    } else if (p->kind == PROJ_AGG_MAX) {
                        double v = (p->col >= 0 && s->cols[p->col].type == TSDB_TYPE_FLOAT64)
                                   ? cur_state.max_f : (double)cur_state.max_i;
                        uint64_t bits; memcpy(&bits, &v, 8);
                        result_append_cell(r, pi, bits);
                    } else if (p->kind == PROJ_AGG_COUNT) {
                        int64_t v = (int64_t)cur_state.count;
                        uint64_t bits; memcpy(&bits, &v, 8);
                        result_append_cell(r, pi, bits);
                    } else {
                        result_append_cell(r, pi, 0);
                    }
                }
                r->nrows++;
            }
        }
    }

    (void)cmp_u64;
done:
    free(serial_agg_scratch);
    if (projs) {
        for (int pi = 0; pi < nprojs; pi++) proj_tdigest_free(&projs[pi]);
        free(projs);
    }
    scan_plan_free(&plan);
    return rc;
}

/* ---- DDL execution helpers --------------------------------------------- */

/*
 * Build a result row for LIST GROUPS: one SYMBOL col per field.
 * Columns: name, region, retention_ns(i64), codec_profile, replica_factor(i64),
 *          tags, created_at(ts)
 */
static const char *LIST_GROUPS_COLS[]  = {
    "name","region","retention_ns","codec_profile",
    "replica_factor","tags","created_at","database"
};
static const tsdb_type_t LIST_GROUPS_TYPES[] = {
    TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL, TSDB_TYPE_INT64,
    TSDB_TYPE_SYMBOL, TSDB_TYPE_INT64,  TSDB_TYPE_SYMBOL,
    TSDB_TYPE_TIMESTAMP, TSDB_TYPE_SYMBOL
};
#define LIST_GROUPS_NCOLS 8

/* LIST DATABASES result columns: name, description, retention_ns, created_at. */
static const char *LIST_DBS_COLS[] = {
    "name","description","retention_ns","created_at","protected"
};
static const tsdb_type_t LIST_DBS_TYPES[] = {
    TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL,
    TSDB_TYPE_INT64,  TSDB_TYPE_TIMESTAMP,
    TSDB_TYPE_INT64
};
#define LIST_DBS_NCOLS 5

/* LIST VTABLES result columns. */
static const char *LIST_VTABS_COLS[] = {
    "name","ncols","ntag_cols","created_at","database","group"
};
static const tsdb_type_t LIST_VTABS_TYPES[] = {
    TSDB_TYPE_SYMBOL, TSDB_TYPE_INT64, TSDB_TYPE_INT64,
    TSDB_TYPE_TIMESTAMP, TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL
};
#define LIST_VTABS_NCOLS 6

/* LIST PTABLES result columns. */
static const char *LIST_PTABS_COLS[] = {
    "name","vtable","ntags","created_at","database","group"
};
static const tsdb_type_t LIST_PTABS_TYPES[] = {
    TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL, TSDB_TYPE_INT64,
    TSDB_TYPE_TIMESTAMP, TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL
};
#define LIST_PTABS_NCOLS 6

static const char *LIST_DEVICES_COLS[] = {"group","id","type","location","tags","last_seen","status"};
static const tsdb_type_t LIST_DEVICES_TYPES[] = {
    TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL,
    TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL, TSDB_TYPE_TIMESTAMP,
    TSDB_TYPE_INT64
};
#define LIST_DEVICES_NCOLS 7

/* Allocate an owned symtab. Registers it in the result's owned list. */
static tsdb_symtab_t *result_new_owned_symtab(tsdb_result_t *r) {
    tsdb_symtab_t *st = NULL;
    if (tsdb_symtab_new(&st) != TSDB_OK) return NULL;
    int n = r->n_owned_symtabs;
    tsdb_symtab_t **arr = realloc(r->owned_symtabs,
                                   (size_t)(n + 1) * sizeof(*arr));
    if (!arr) { tsdb_symtab_free(st); return NULL; }
    arr[n] = st;
    r->owned_symtabs = arr;
    r->n_owned_symtabs = n + 1;
    return st;
}

/* Intern a string into symtab and append to result col. */
static int result_append_sym(tsdb_result_t *r, int col, const char *s) {
    tsdb_symtab_t *st = r->col_symtab[col];
    if (!st) return TSDB_ERR_INTERNAL;
    uint32_t code = tsdb_symtab_intern(st, s ? s : "");
    if (code == TSDB_SYMBOL_INVALID) return TSDB_ERR_NOMEM;
    ((uint64_t *)r->col_data[col])[r->nrows] = code;
    return TSDB_OK;
}

static int result_append_i64_val(tsdb_result_t *r, int col, int64_t v) {
    uint64_t bits;
    memcpy(&bits, &v, 8);
    ((uint64_t *)r->col_data[col])[r->nrows] = bits;
    return TSDB_OK;
}

static int result_append_ts_val(tsdb_result_t *r, int col, tsdb_ts_t v) {
    return result_append_i64_val(r, col, (int64_t)v);
}

/* Initialise result with ncols columns; allocate symtabs for SYMBOL cols.
 * Also preallocates 64 rows worth of data. */
static int result_init_ddl(tsdb_result_t *r, int ncols,
                            const char **names,
                            const tsdb_type_t *types) {
    r->ncols     = ncols;
    r->col_names  = calloc((size_t)ncols, sizeof(char *));
    r->col_types  = calloc((size_t)ncols, sizeof(tsdb_type_t));
    r->col_symtab = calloc((size_t)ncols, sizeof(tsdb_symtab_t *));
    r->col_data   = calloc((size_t)ncols, sizeof(void *));
    if (!r->col_names || !r->col_types || !r->col_data || !r->col_symtab)
        return TSDB_ERR_NOMEM;

    size_t init_rows = 64;
    for (int i = 0; i < ncols; i++) {
        r->col_names[i] = strdup(names[i]);
        r->col_types[i] = types[i];
        r->col_data[i]  = malloc(init_rows * 8);
        if (!r->col_names[i] || !r->col_data[i]) return TSDB_ERR_NOMEM;
        if (types[i] == TSDB_TYPE_SYMBOL) {
            r->col_symtab[i] = result_new_owned_symtab(r);
            if (!r->col_symtab[i]) return TSDB_ERR_NOMEM;
        }
    }
    r->cap_rows = init_rows;
    return TSDB_OK;
}

/* Grow result data arrays if needed before appending. */
static int result_ddl_ensure_cap(tsdb_result_t *r) {
    if (r->nrows < r->cap_rows) return TSDB_OK;
    size_t newcap = r->cap_rows * 2;
    for (int i = 0; i < r->ncols; i++) {
        void *np = realloc(r->col_data[i], newcap * 8);
        if (!np) return TSDB_ERR_NOMEM;
        r->col_data[i] = np;
    }
    r->cap_rows = newcap;
    return TSDB_OK;
}

/* Commit a row (increment nrows). */
static void result_ddl_end_row(tsdb_result_t *r) { r->nrows++; }

static int exec_list_groups(tsdb_catalog_t *cat, tsdb_result_t *r,
                             const char *db_filter) {
    int rc = result_init_ddl(r, LIST_GROUPS_NCOLS,
                              LIST_GROUPS_COLS, LIST_GROUPS_TYPES);
    if (rc != TSDB_OK) return rc;

    tsdb_group_t *arr = NULL;
    size_t n = 0;
    rc = tsdb_group_list(cat, &arr, &n);
    if (rc != TSDB_OK) return rc;

    int has_filter = (db_filter && db_filter[0]);
    for (size_t i = 0; i < n; i++) {
        tsdb_group_t *g = &arr[i];
        if (has_filter && strcmp(g->database, db_filter) != 0) continue;
        if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
        char ret_str[32];
        snprintf(ret_str, sizeof(ret_str), "%lld", (long long)g->retention_ns);

        result_append_sym(r, 0, g->name);
        result_append_sym(r, 1, g->region);
        result_append_i64_val(r, 2, g->retention_ns);
        result_append_sym(r, 3, g->codec_profile);
        result_append_i64_val(r, 4, (int64_t)g->replica_factor);
        result_append_sym(r, 5, g->tags);
        result_append_ts_val(r, 6, g->created_at);
        /* Empty string → implicit "default" database; keep the on-wire
         * value empty so the UI can decide how to render it. */
        result_append_sym(r, 7, g->database);
        result_ddl_end_row(r);
    }
    tsdb_group_list_free(arr);
    return rc;
}

static int exec_list_databases(tsdb_catalog_t *cat, tsdb_result_t *r) {
    int rc = result_init_ddl(r, LIST_DBS_NCOLS,
                              LIST_DBS_COLS, LIST_DBS_TYPES);
    if (rc != TSDB_OK) return rc;

    tsdb_database_t *arr = NULL;
    size_t n = 0;
    rc = tsdb_database_list(cat, &arr, &n);
    if (rc != TSDB_OK) return rc;

    for (size_t i = 0; i < n; i++) {
        if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
        tsdb_database_t *d = &arr[i];
        result_append_sym(r, 0, d->name);
        result_append_sym(r, 1, d->description);
        result_append_i64_val(r, 2, d->retention_ns);
        result_append_ts_val(r, 3, d->created_at);
        result_append_i64_val(r, 4, (int64_t)d->protected_flag);
        result_ddl_end_row(r);
    }
    tsdb_database_list_free(arr);
    return rc;
}

static int exec_list_vtables(tsdb_catalog_t *cat,
                              const char *db_filter,
                              const char *grp_filter,
                              tsdb_result_t *r)
{
    int rc = result_init_ddl(r, LIST_VTABS_NCOLS,
                              LIST_VTABS_COLS, LIST_VTABS_TYPES);
    if (rc != TSDB_OK) return rc;

    tsdb_stable_t *arr = NULL;
    size_t n = 0;
    rc = tsdb_stable_list(cat, &arr, &n);
    if (rc != TSDB_OK) return rc;

    for (size_t i = 0; i < n; i++) {
        tsdb_stable_t *s = &arr[i];
        if (db_filter && *db_filter && strcmp(s->database, db_filter) != 0) continue;
        if (grp_filter && *grp_filter && strcmp(s->group, grp_filter) != 0) continue;
        if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
        result_append_sym(r, 0, s->name);
        result_append_i64_val(r, 1, (int64_t)s->ncols);
        result_append_i64_val(r, 2, (int64_t)s->ntag_cols);
        result_append_ts_val(r, 3, s->created_at);
        result_append_sym(r, 4, s->database);
        result_append_sym(r, 5, s->group);
        result_ddl_end_row(r);
    }
    free(arr);
    return rc;
}

static int exec_list_ptables(tsdb_catalog_t *cat,
                              const char *vtable_filter,
                              const char *db_filter,
                              const char *grp_filter,
                              tsdb_result_t *r)
{
    int rc = result_init_ddl(r, LIST_PTABS_NCOLS,
                              LIST_PTABS_COLS, LIST_PTABS_TYPES);
    if (rc != TSDB_OK) return rc;

    tsdb_child_table_t *arr = NULL;
    size_t n = 0;
    rc = tsdb_child_table_list(cat,
        (vtable_filter && vtable_filter[0]) ? vtable_filter : NULL,
        &arr, &n);
    if (rc != TSDB_OK) return rc;

    for (size_t i = 0; i < n; i++) {
        tsdb_child_table_t *ct = &arr[i];
        if (db_filter && *db_filter && strcmp(ct->database, db_filter) != 0) continue;
        if (grp_filter && *grp_filter && strcmp(ct->group, grp_filter) != 0) continue;
        if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
        result_append_sym(r, 0, ct->name);
        result_append_sym(r, 1, ct->stable_name);
        result_append_i64_val(r, 2, (int64_t)ct->ntags);
        result_append_ts_val(r, 3, ct->created_at);
        result_append_sym(r, 4, ct->database);
        result_append_sym(r, 5, ct->group);
        result_ddl_end_row(r);
    }
    free(arr);
    return rc;
}

static int exec_list_devices(tsdb_catalog_t *cat, const char *group,
                              tsdb_result_t *r) {
    int rc = result_init_ddl(r, LIST_DEVICES_NCOLS,
                              LIST_DEVICES_COLS, LIST_DEVICES_TYPES);
    if (rc != TSDB_OK) return rc;

    tsdb_device_t *arr = NULL;
    size_t n = 0;
    /* group="" means list all */
    rc = tsdb_device_list(cat, (group && group[0]) ? group : NULL, &arr, &n);
    if (rc != TSDB_OK) return rc;

    for (size_t i = 0; i < n; i++) {
        if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
        tsdb_device_t *d = &arr[i];
        result_append_sym(r, 0, d->group);
        result_append_sym(r, 1, d->id);
        result_append_sym(r, 2, d->type);
        result_append_sym(r, 3, d->location);
        result_append_sym(r, 4, d->tags);
        result_append_ts_val(r, 5, d->last_seen);
        result_append_i64_val(r, 6, (int64_t)d->status);
        result_ddl_end_row(r);
    }
    tsdb_device_list_free(arr);
    return rc;
}

/* LIST USERS result columns: name, role, created_at. */
static const char *LIST_USERS_COLS[]  = {"name", "role", "created_at"};
static const tsdb_type_t LIST_USERS_TYPES[] = {
    TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL, TSDB_TYPE_TIMESTAMP
};
#define LIST_USERS_NCOLS 3

static int exec_list_users(tsdb_auth_t *auth, tsdb_result_t *r) {
    int rc = result_init_ddl(r, LIST_USERS_NCOLS,
                              LIST_USERS_COLS, LIST_USERS_TYPES);
    if (rc != TSDB_OK) return rc;

    tsdb_user_t *arr = NULL;
    size_t n = 0;
    rc = tsdb_auth_user_list(auth, &arr, &n);
    if (rc != TSDB_OK) return rc;

    for (size_t i = 0; i < n; i++) {
        if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
        tsdb_user_t *u = &arr[i];
        const char *role_str = (u->role == TSDB_USER_ROLE_ADMIN) ? "ADMIN" : "NORMAL";
        result_append_sym(r, 0, u->name);
        result_append_sym(r, 1, role_str);
        result_append_ts_val(r, 2, u->created_at);
        result_ddl_end_row(r);
    }
    free(arr);
    return rc;
}

/* Build a single-row, single-col STATUS result with a message string. */
static int result_status(tsdb_result_t *r, const char *msg) {
    static const char *col_names[] = {"status"};
    static const tsdb_type_t col_types[] = {TSDB_TYPE_SYMBOL};
    int rc = result_init_ddl(r, 1, col_names, col_types);
    if (rc != TSDB_OK) return rc;
    result_append_sym(r, 0, msg);
    result_ddl_end_row(r);
    return TSDB_OK;
}

/* ---- Public API implementations --------------------------------------- */

/* Defined in storage/db_cluster.c.  Peer-side RPC and apply paths
 * flip these to 1 before re-entering tsdb_query with a replayed
 * catalog statement — prevents ping-pong in fanout mode and
 * re-entry-into-Raft in consensus mode. */
extern __thread int tsdb_g_suppress_catalog_broadcast;
extern __thread int tsdb_g_inside_raft_apply;

static inline void try_broadcast_catalog_qtl(tsdb_db_t *db,
                                             const char *qtl, int rc_local)
{
    if (rc_local != TSDB_OK) return;
    if (tsdb_g_suppress_catalog_broadcast) return;
    /* When Raft is live, consensus replication already made this
     * statement durable across the master set — skip the fanout. */
    if (tsdb_db_raft_for_db(db)) return;
    (void)tsdb_cluster_broadcast_catalog_qtl(db, qtl, NULL, NULL);
}

/* Check whether a parsed statement is a catalog mutation that must
 * go through Raft when raft is bound.  Keep this in sync with the
 * broadcast switch near the end of tsdb_query. */
static int is_catalog_ddl(qast_stmt_kind_t k) {
    switch (k) {
    case QAST_STMT_CREATE_DATABASE:
    case QAST_STMT_DROP_DATABASE:
    case QAST_STMT_CREATE_GROUP:
    case QAST_STMT_DROP_GROUP:
    case QAST_STMT_CREATE_STABLE:
    case QAST_STMT_DROP_STABLE:
    case QAST_STMT_CREATE_CHILD_TABLE:
    case QAST_STMT_CREATE_TABLE:
    case QAST_STMT_DROP_TABLE:
        return 1;
    default:
        return 0;
    }
}

int tsdb_query(tsdb_db_t *db, const char *qtl, tsdb_result_t **out) {
    if (!db || !qtl || !out) return TSDB_ERR_INVAL;

    tsdb_arena_t a; tsdb_arena_init(&a, 16 * 1024);
    qast_stmt_t stmt;
    char err[256];
    int rc = qparse_stmt(qtl, &a, &stmt, err, sizeof(err));
    if (rc != TSDB_OK) {
        tsdb_arena_free(&a);
        return rc;
    }

    tsdb_result_t *r = calloc(1, sizeof(*r));
    if (!r) { tsdb_arena_free(&a); return TSDB_ERR_NOMEM; }
    r->cur = -1;

    if (stmt.kind == QAST_STMT_SELECT) {
        /* Phase γ — shard read forwarding.  When TSDB_SHARD_REPLICA_N
         * is set and self is not in the owner set for the FROM table,
         * ship the QTL to an owner via FED_QUERY and return its result
         * verbatim.  Inert when shard mode is off or self is owner —
         * caller continues with the local exec path below. */
        if (stmt.u.query.from) {
            tsdb_result_t *forwarded = NULL;
            int frc = tsdb_cluster_maybe_forward_select(
                db, stmt.u.query.from, qtl, &forwarded);
            if (frc != TSDB_OK) {
                tsdb_result_free(r);
                tsdb_arena_free(&a);
                return frc;
            }
            if (forwarded) {
                tsdb_result_free(r);
                tsdb_arena_free(&a);
                *out = forwarded;
                return TSDB_OK;
            }
        }
        rc = exec_select(db, &stmt.u.query, r, err, sizeof(err));
        /* Apply ORDER BY post-execution so plain SELECT, GROUP BY,
         * SAMPLE BY, LATEST ON, and the stable-select union all share
         * the same sort path.  Must run before arena_free because
         * q->order_col is arena-allocated. */
        if (rc == TSDB_OK)
            rc = result_apply_order_by(r, &stmt.u.query, err, sizeof(err));
        tsdb_arena_free(&a);
        if (rc != TSDB_OK) { tsdb_result_free(r); return rc; }
        *out = r;
        return TSDB_OK;
    }

    /* ---- Raft consensus fast-path for catalog DDL --------------------
     *
     * When this db has a raft state machine bound (TSDB_CONSENSUS=raft,
     * role=master) AND we're NOT already running inside the apply
     * thread, route catalog mutations through Raft.  The apply callback
     * (raft_apply_cb in tsdb_node_main.c) is responsible for re-entering
     * tsdb_query on every master with both suppress flags on; that
     * re-entry lands below this block and executes the statement via
     * the normal DDL switch. */
    tsdb_raft_t *raft = tsdb_db_raft_for_db(db);
    if (raft && is_catalog_ddl(stmt.kind) && !tsdb_g_inside_raft_apply) {
        /* Pre-validate before propose so obvious user errors surface
         * synchronously instead of being swallowed by the apply
         * callback's silent log line.  These checks read the leader's
         * current catalog under the same membership the propose will
         * commit against; a concurrent state change between check and
         * apply still races (Raft's design is async-apply), but the
         * common-case ergonomics — DROP missing X, CREATE X under
         * missing parent — work as users expect. */
        tsdb_catalog_t *pc = tsdb_db_catalog(db);
        if (pc) {
            const char *err_msg = NULL;
            int err_rc = TSDB_OK;
            switch (stmt.kind) {
            case QAST_STMT_CREATE_GROUP: {
                const char *gname = stmt.u.create_group.spec.name;
                const char *pdb = stmt.u.create_group.spec.database;
                if (pdb[0] && !tsdb_database_exists(pc, pdb)) {
                    err_msg = "ERR: database not found"; err_rc = TSDB_ERR_NOTFOUND;
                    break;
                }
                /* Catalog keys groups globally by name.  If a group with
                 * this name already exists somewhere, surface a clearer
                 * EXISTS / namespace-collision message rather than the
                 * silent "OK: committed via raft" + apply-side EXISTS. */
                tsdb_group_t g_existing;
                static char gexist_msg[200];
                if (tsdb_group_get(pc, gname, &g_existing) == TSDB_OK) {
                    if (strcmp(g_existing.database, pdb) == 0) {
                        snprintf(gexist_msg, sizeof(gexist_msg),
                                 "ERR: group '%s' already exists in database '%s'",
                                 gname, pdb[0] ? pdb : "(default)");
                    } else {
                        snprintf(gexist_msg, sizeof(gexist_msg),
                                 "ERR: group name '%s' is already used in database '%s' "
                                 "(group names are globally unique across databases)",
                                 gname,
                                 g_existing.database[0] ? g_existing.database : "(default)");
                    }
                    err_msg = gexist_msg; err_rc = TSDB_ERR_EXISTS;
                }
                break;
            }
            case QAST_STMT_CREATE_STABLE: {
                const char *sname = stmt.u.create_stable.spec.name;
                const char *pdb = stmt.u.create_stable.spec.database;
                const char *pgr = stmt.u.create_stable.spec.group;
                if (pdb[0] && !tsdb_database_exists(pc, pdb)) {
                    err_msg = "ERR: database not found"; err_rc = TSDB_ERR_NOTFOUND;
                    break;
                }
                if (pgr[0]) {
                    tsdb_group_t gtmp;
                    if (tsdb_group_get(pc, pgr, &gtmp) != TSDB_OK) {
                        err_msg = "ERR: group not found"; err_rc = TSDB_ERR_NOTFOUND;
                        break;
                    }
                }
                tsdb_stable_t st_existing;
                static char sexist_msg[200];
                if (tsdb_stable_get(pc, sname, &st_existing) == TSDB_OK) {
                    if (strcmp(st_existing.database, pdb) == 0 &&
                        strcmp(st_existing.group, pgr) == 0) {
                        snprintf(sexist_msg, sizeof(sexist_msg),
                                 "ERR: stable '%s' already exists at this location", sname);
                    } else {
                        snprintf(sexist_msg, sizeof(sexist_msg),
                                 "ERR: stable name '%s' is already used in database '%s' group '%s' "
                                 "(stable names are globally unique)",
                                 sname,
                                 st_existing.database[0] ? st_existing.database : "(default)",
                                 st_existing.group[0]    ? st_existing.group    : "(default)");
                    }
                    err_msg = sexist_msg; err_rc = TSDB_ERR_EXISTS;
                }
                break;
            }
            case QAST_STMT_CREATE_DATABASE: {
                if (tsdb_database_exists(pc, stmt.u.create_database.name)) {
                    err_msg = "ERR: database already exists"; err_rc = TSDB_ERR_EXISTS;
                }
                break;
            }
            case QAST_STMT_CREATE_CHILD_TABLE: {
                const tsdb_child_table_t *ct_in = &stmt.u.create_child_table.spec;
                tsdb_stable_t st_pre;
                if (tsdb_stable_get(pc, ct_in->stable_name, &st_pre) != TSDB_OK) {
                    err_msg = "ERR: stable not found"; err_rc = TSDB_ERR_NOTFOUND;
                    break;
                }
                /* Catalog keys child tables globally by name. */
                tsdb_child_table_t ct_existing;
                static char ctexist_msg[200];
                if (tsdb_child_table_get(pc, ct_in->name, &ct_existing) == TSDB_OK) {
                    if (strcmp(ct_existing.stable_name, ct_in->stable_name) == 0) {
                        snprintf(ctexist_msg, sizeof(ctexist_msg),
                                 "ERR: child table '%s' already exists under stable '%s'",
                                 ct_in->name, ct_in->stable_name);
                    } else {
                        snprintf(ctexist_msg, sizeof(ctexist_msg),
                                 "ERR: table name '%s' is already used as a child of stable '%s' "
                                 "(child table names are globally unique)",
                                 ct_in->name, ct_existing.stable_name);
                    }
                    err_msg = ctexist_msg; err_rc = TSDB_ERR_EXISTS;
                    break;
                }
                /* Tag count + per-position type — same checks the apply
                 * path runs, hoisted up so the leader's "OK: committed
                 * via raft" reply only fires for genuinely valid CREATEs. */
                static char schema_msg[160];
                if (ct_in->ntags != st_pre.ntag_cols) {
                    snprintf(schema_msg, sizeof(schema_msg),
                             "ERR: tag count mismatch (got %d, stable '%s' expects %d)",
                             ct_in->ntags, st_pre.name, st_pre.ntag_cols);
                    err_msg = schema_msg; err_rc = TSDB_ERR_SCHEMA;
                    break;
                }
                for (int ti = 0; ti < ct_in->ntags; ti++) {
                    tsdb_type_t want = st_pre.tag_cols[ti].type;
                    tsdb_type_t got  = ct_in->tags[ti].type;
                    if (want == got) continue;
                    if (want == TSDB_TYPE_FLOAT64 && got == TSDB_TYPE_INT64) continue;
                    snprintf(schema_msg, sizeof(schema_msg),
                             "ERR: tag type mismatch at position %d "
                             "(tag '%s' expects %s)",
                             ti, st_pre.tag_cols[ti].name,
                             tsdb_type_name(st_pre.tag_cols[ti].type));
                    err_msg = schema_msg; err_rc = TSDB_ERR_SCHEMA;
                    break;
                }
                break;
            }
            case QAST_STMT_DROP_DATABASE: {
                const char *n = stmt.u.drop_database.name;
                if (strcmp(n, TSDB_SYSDB_NAME) == 0) {
                    err_msg = "ERR: database is protected (e.g. sysdb) — drop refused";
                    err_rc = TSDB_ERR_PERMISSION;
                } else if (!tsdb_database_exists(pc, n)) {
                    err_msg = "ERR: database not found"; err_rc = TSDB_ERR_NOTFOUND;
                }
                break;
            }
            case QAST_STMT_DROP_GROUP: {
                tsdb_group_t gtmp;
                if (tsdb_group_get(pc, stmt.u.drop_group.name, &gtmp) != TSDB_OK) {
                    err_msg = "ERR: group not found"; err_rc = TSDB_ERR_NOTFOUND;
                }
                break;
            }
            case QAST_STMT_DROP_STABLE: {
                if (!tsdb_stable_exists(pc, stmt.u.drop_stable.name)) {
                    err_msg = "ERR: stable not found"; err_rc = TSDB_ERR_NOTFOUND;
                }
                break;
            }
            case QAST_STMT_DROP_TABLE: {
                /* DROP TABLE accepts both child tables and regular tables.
                 * Surface NOTFOUND only if neither path knows the name. */
                tsdb_child_table_t ctmp;
                int has_child = (tsdb_child_table_get(pc,
                                    stmt.u.drop_table.name, &ctmp) == TSDB_OK);
                int has_table = (tsdb_db_find_table(db, stmt.u.drop_table.name) != NULL);
                if (!has_child && !has_table) {
                    err_msg = "ERR: table not found"; err_rc = TSDB_ERR_NOTFOUND;
                }
                break;
            }
            default: break;
            }
            if (err_msg) {
                /* Return TSDB_OK + status-row, mirroring the "ERR: not
                 * raft leader" path above.  The HTTP /sql provider
                 * doesn't free the result on rc != 0, so emitting an
                 * error code here would leak; the status-row form also
                 * lets the dashboard render the message inline rather
                 * than flattening it through the HTTP error wrapper. */
                (void)err_rc;
                result_status(r, err_msg);
                *out = r;
                tsdb_arena_free(&a);
                return TSDB_OK;
            }
        }

        tsdb_arena_free(&a);
        tsdb_raft_state_t s = tsdb_raft_state(raft);
        if (s != TSDB_RAFT_LEADER) {
            /* Not leader — surface leader hint as a status row.  Return
             * TSDB_OK (not PERMISSION) so the HTTP layer renders the
             * hint in `rows` instead of flattening to a generic
             * "permission denied" error body; clients that want to
             * auto-retry can detect the "ERR: not raft leader" prefix. */
            char msg[160];
            snprintf(msg, sizeof(msg),
                     "ERR: not raft leader (self=%llu, leader=%llu) — "
                     "retry against the leader",
                     (unsigned long long)tsdb_raft_self_id(raft),
                     (unsigned long long)tsdb_raft_leader_id(raft));
            result_status(r, msg);
            *out = r;
            return TSDB_OK;
        }
        int prc = tsdb_raft_propose(raft, TSDB_RAFT_ENTRY_CATALOG_QTL,
                                     qtl, (uint32_t)strlen(qtl), 5000);
        if (prc == TSDB_ERR_PERMISSION) {
            char msg[160];
            snprintf(msg, sizeof(msg),
                     "ERR: stepped down mid-propose, retry (leader=%llu)",
                     (unsigned long long)tsdb_raft_leader_id(raft));
            result_status(r, msg);
            *out = r;
            return TSDB_OK;
        }
        if (prc != TSDB_OK) {
            result_status(r, "ERR: raft propose failed (timeout or IO)");
            *out = r;
            return TSDB_OK;
        }
        /* Applied on us via raft_apply_cb.  Return success marker. */
        result_status(r, "OK: committed via raft");
        *out = r;
        return TSDB_OK;
    }

    /* Data-node guard: on any cluster node that didn't bind a raft
     * handle (TSDB_NODE_ROLE=data, or raft disabled on a master that's
     * still bootstrapping) catalog DDL must not run locally — the
     * catalog is raft-owned and any local write will diverge from the
     * committed view.  Fresh catalog-DDL from a client should be sent
     * to a master instead.  The cluster-node-mgr check excludes the
     * legacy single-process server where db has no cluster binding —
     * there, local DDL is still the right behaviour. */
    if (!raft && is_catalog_ddl(stmt.kind) && !tsdb_g_inside_raft_apply &&
        !tsdb_g_suppress_catalog_broadcast &&
        tsdb_cluster_node_mgr_for_db(db) != NULL)
    {
        /* The suppress-broadcast flag is set by the APPLY_CATALOG_QTL
         * RPC receiver (rpc.c) when a committed catalog DDL arrives
         * from a master's raft apply: in that window the local DDL
         * IS the authoritative truth the data node needs to absorb.
         * Without this bypass, the guard would refuse the incoming
         * broadcast and data-node catalogs would lag the cluster
         * forever. */
        tsdb_arena_free(&a);
        result_status(r,
            "ERR: catalog DDL is not accepted on a data node — send "
            "to any master (TSDB_NODE_ROLE=master, see LIST MASTERS)");
        *out = r;
        return TSDB_OK;
    }

    tsdb_arena_free(&a);

    /* DDL statements require the catalog. */
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    if (!cat) {
        tsdb_result_free(r);
        eset(err, sizeof(err), "catalog not available");
        return TSDB_ERR_INTERNAL;
    }

    switch (stmt.kind) {
    case QAST_STMT_CREATE_DATABASE: {
        tsdb_database_t d;
        memset(&d, 0, sizeof(d));
        snprintf(d.name, sizeof(d.name), "%s", stmt.u.create_database.name);
        snprintf(d.description, sizeof(d.description), "%s",
                 stmt.u.create_database.description);
        d.retention_ns = stmt.u.create_database.retention_ns;
        rc = tsdb_database_create(cat, &d);
        if (rc == TSDB_OK) rc = result_status(r, "OK: database created");
        else if (rc == TSDB_ERR_EXISTS) { result_status(r, "ERR: database exists"); rc = TSDB_ERR_EXISTS; }
        else result_status(r, "ERR: create database failed");
        break;
    }
    case QAST_STMT_DROP_DATABASE: {
        const char *dbname = stmt.u.drop_database.name;

        /* Cascade in declared dependency order so a partial failure
         * leaves the catalog in a coherent intermediate state rather
         * than orphaned children:
         *   1. for every group in this db: cascade its stables (which
         *      drops physical tables + child catalog rows), then
         *      tsdb_group_drop.
         *   2. cascade stables attached directly to the db (group == "").
         *   3. tsdb_database_drop.
         *
         * Names are snapshot-copied because the underlying drops
         * mutate the catalog hmaps we'd otherwise be iterating. */
        tsdb_group_t *gall = NULL;
        size_t ngall = 0;
        if (tsdb_group_list(cat, &gall, &ngall) == TSDB_OK) {
            char gnames[256][64];
            int  ng_match = 0;
            for (size_t i = 0; i < ngall && ng_match < 256; i++) {
                if (strcmp(gall[i].database, dbname) == 0) {
                    snprintf(gnames[ng_match++], 64, "%s", gall[i].name);
                }
            }
            tsdb_group_list_free(gall);
            for (int gi = 0; gi < ng_match; gi++) {
                /* Cascade stables under this group. */
                tsdb_stable_t *sall = NULL;
                size_t nsall = 0;
                if (tsdb_stable_list(cat, &sall, &nsall) == TSDB_OK) {
                    char snames[256][64];
                    int  ns_match = 0;
                    for (size_t i = 0; i < nsall && ns_match < 256; i++) {
                        if (strcmp(sall[i].database, dbname) == 0 &&
                            strcmp(sall[i].group, gnames[gi]) == 0) {
                            snprintf(snames[ns_match++], 64, "%s", sall[i].name);
                        }
                    }
                    free(sall);
                    for (int si = 0; si < ns_match; si++) {
                        tsdb_child_table_t *children = NULL;
                        size_t nch = 0;
                        int lrc = tsdb_child_table_list(cat, snames[si], &children, &nch);
                        if (lrc != TSDB_OK)
                            fprintf(stderr, "[cascade] DROP DATABASE %s: list children of stable %s rc=%d\n",
                                    dbname, snames[si], lrc);
                        for (size_t ci = 0; ci < nch; ci++) {
                            int drc = tsdb_drop_table(db, children[ci].name);
                            if (drc != TSDB_OK && drc != TSDB_ERR_NOTFOUND)
                                fprintf(stderr, "[cascade] DROP DATABASE %s: drop child %s rc=%d\n",
                                        dbname, children[ci].name, drc);
                        }
                        free(children);
                        int src = tsdb_stable_drop(cat, snames[si]);
                        if (src != TSDB_OK && src != TSDB_ERR_NOTFOUND)
                            fprintf(stderr, "[cascade] DROP DATABASE %s: drop stable %s rc=%d\n",
                                    dbname, snames[si], src);
                    }
                }
                int grc = tsdb_group_drop(cat, gnames[gi]);
                if (grc != TSDB_OK && grc != TSDB_ERR_NOTFOUND)
                    fprintf(stderr, "[cascade] DROP DATABASE %s: drop group %s rc=%d\n",
                            dbname, gnames[gi], grc);
            }
        }

        /* Cascade stables attached directly to the db (no group). */
        {
            tsdb_stable_t *sall = NULL;
            size_t nsall = 0;
            if (tsdb_stable_list(cat, &sall, &nsall) == TSDB_OK) {
                char snames[256][64];
                int  ns_match = 0;
                for (size_t i = 0; i < nsall && ns_match < 256; i++) {
                    if (strcmp(sall[i].database, dbname) == 0 &&
                        sall[i].group[0] == '\0') {
                        snprintf(snames[ns_match++], 64, "%s", sall[i].name);
                    }
                }
                free(sall);
                for (int si = 0; si < ns_match; si++) {
                    tsdb_child_table_t *children = NULL;
                    size_t nch = 0;
                    int lrc = tsdb_child_table_list(cat, snames[si], &children, &nch);
                    if (lrc != TSDB_OK)
                        fprintf(stderr, "[cascade] DROP DATABASE %s: list children of stable %s rc=%d\n",
                                dbname, snames[si], lrc);
                    for (size_t ci = 0; ci < nch; ci++) {
                        int drc = tsdb_drop_table(db, children[ci].name);
                        if (drc != TSDB_OK && drc != TSDB_ERR_NOTFOUND)
                            fprintf(stderr, "[cascade] DROP DATABASE %s: drop child %s rc=%d\n",
                                    dbname, children[ci].name, drc);
                    }
                    free(children);
                    int src = tsdb_stable_drop(cat, snames[si]);
                    if (src != TSDB_OK && src != TSDB_ERR_NOTFOUND)
                        fprintf(stderr, "[cascade] DROP DATABASE %s: drop stable %s rc=%d\n",
                                dbname, snames[si], src);
                }
            }
        }

        rc = tsdb_database_drop(cat, dbname);
        if (rc == TSDB_OK)
            rc = result_status(r, "OK: database dropped");
        else if (rc == TSDB_ERR_NOTFOUND) {
            result_status(r, "ERR: database not found");
            rc = TSDB_ERR_NOTFOUND;
        } else if (rc == TSDB_ERR_PERMISSION) {
            result_status(r,
                "ERR: database is protected (e.g. sysdb) — drop refused");
            rc = TSDB_ERR_PERMISSION;
        } else {
            result_status(r, "ERR: drop database failed");
        }
        break;
    }
    case QAST_STMT_LIST_DATABASES:
        rc = exec_list_databases(cat, r);
        break;
    case QAST_STMT_CREATE_GROUP: {
        /* If IN DATABASE was specified, ensure the parent exists. */
        const tsdb_group_t *g = &stmt.u.create_group.spec;
        if (g->database[0] && !tsdb_database_exists(cat, g->database)) {
            result_status(r, "ERR: database not found");
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        rc = tsdb_group_create(cat, &stmt.u.create_group.spec);
        if (rc == TSDB_OK) rc = result_status(r, "OK: group created");
        else if (rc == TSDB_ERR_EXISTS) { result_status(r, "ERR: group exists"); rc = TSDB_ERR_EXISTS; }
        else result_status(r, "ERR: create group failed");
        break;
    }
    case QAST_STMT_DROP_GROUP: {
        const char *gname = stmt.u.drop_group.name;
        /* Cascade: drop every stable that lives in this group along
         * with its physical child tables, then drop the group itself.
         * Mirrors DROP DATABASE's per-group block above; tsdb_group_drop
         * already cascades the device catalog rows but not stables. */
        tsdb_group_t gmeta;
        const char *gdb = "";
        if (tsdb_group_get(cat, gname, &gmeta) == TSDB_OK) gdb = gmeta.database;
        tsdb_stable_t *sall = NULL;
        size_t nsall = 0;
        if (tsdb_stable_list(cat, &sall, &nsall) == TSDB_OK) {
            char snames[256][64];
            int  ns_match = 0;
            for (size_t i = 0; i < nsall && ns_match < 256; i++) {
                if (strcmp(sall[i].group, gname) == 0 &&
                    strcmp(sall[i].database, gdb) == 0) {
                    snprintf(snames[ns_match++], 64, "%s", sall[i].name);
                }
            }
            free(sall);
            for (int si = 0; si < ns_match; si++) {
                tsdb_child_table_t *children = NULL;
                size_t nch = 0;
                int lrc = tsdb_child_table_list(cat, snames[si], &children, &nch);
                if (lrc != TSDB_OK)
                    fprintf(stderr, "[cascade] DROP GROUP %s: list children of stable %s rc=%d\n",
                            gname, snames[si], lrc);
                for (size_t ci = 0; ci < nch; ci++) {
                    int drc = tsdb_drop_table(db, children[ci].name);
                    if (drc != TSDB_OK && drc != TSDB_ERR_NOTFOUND)
                        fprintf(stderr, "[cascade] DROP GROUP %s: drop child %s rc=%d\n",
                                gname, children[ci].name, drc);
                }
                free(children);
                int src = tsdb_stable_drop(cat, snames[si]);
                if (src != TSDB_OK && src != TSDB_ERR_NOTFOUND)
                    fprintf(stderr, "[cascade] DROP GROUP %s: drop stable %s rc=%d\n",
                            gname, snames[si], src);
            }
        }
        rc = tsdb_group_drop(cat, gname);
        if (rc == TSDB_OK) rc = result_status(r, "OK: group dropped");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: group not found"); rc = TSDB_ERR_NOTFOUND; }
        else result_status(r, "ERR: drop group failed");
        break;
    }
    case QAST_STMT_LIST_GROUPS:
        rc = exec_list_groups(cat, r, stmt.u.list_groups.database);
        break;
    case QAST_STMT_CREATE_DEVICE: {
        rc = tsdb_device_create(cat, &stmt.u.create_device.spec);
        if (rc == TSDB_OK) rc = result_status(r, "OK: device created");
        else if (rc == TSDB_ERR_EXISTS) { result_status(r, "ERR: device exists"); rc = TSDB_ERR_EXISTS; }
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: group not found"); rc = TSDB_ERR_NOTFOUND; }
        else result_status(r, "ERR: create device failed");
        break;
    }
    case QAST_STMT_DROP_DEVICE: {
        rc = tsdb_device_drop(cat, stmt.u.drop_device.group, stmt.u.drop_device.id);
        if (rc == TSDB_OK) rc = result_status(r, "OK: device dropped");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: device not found"); rc = TSDB_ERR_NOTFOUND; }
        else result_status(r, "ERR: drop device failed");
        break;
    }
    case QAST_STMT_LIST_DEVICES:
        rc = exec_list_devices(cat, stmt.u.list_devices.group, r);
        break;
    case QAST_STMT_LIST_VTABLES:
        rc = exec_list_vtables(cat,
                               stmt.u.list_vtables.database,
                               stmt.u.list_vtables.group, r);
        break;
    case QAST_STMT_LIST_PTABLES:
        rc = exec_list_ptables(cat,
                               stmt.u.list_ptables.vtable,
                               stmt.u.list_ptables.database,
                               stmt.u.list_ptables.group, r);
        break;

    /* ---- STable DDL ---------------------------------------------------- */
    case QAST_STMT_CREATE_STABLE: {
        rc = tsdb_stable_create(cat, &stmt.u.create_stable.spec);
        if (rc == TSDB_OK) rc = result_status(r, "OK: stable created");
        else if (rc == TSDB_ERR_EXISTS) { result_status(r, "ERR: stable exists"); rc = TSDB_ERR_EXISTS; }
        else result_status(r, "ERR: create stable failed");
        break;
    }
    case QAST_STMT_DROP_STABLE: {
        const char *sname = stmt.u.drop_stable.name;
        /* List children so we can drop physical tables BEFORE catalog drop. */
        tsdb_child_table_t *children = NULL;
        size_t nch = 0;
        if (tsdb_stable_exists(cat, sname)) {
            int lrc = tsdb_child_table_list(cat, sname, &children, &nch);
            if (lrc != TSDB_OK)
                fprintf(stderr, "[cascade] DROP STABLE %s: list children rc=%d\n",
                        sname, lrc);
        }
        /* Drop physical tables first. */
        for (size_t ci = 0; ci < nch; ci++) {
            int drc = tsdb_drop_table(db, children[ci].name);
            if (drc != TSDB_OK && drc != TSDB_ERR_NOTFOUND)
                fprintf(stderr, "[cascade] DROP STABLE %s: drop child %s rc=%d\n",
                        sname, children[ci].name, drc);
        }
        free(children);
        /* Now drop the catalog entries (cascades child table catalog entries). */
        rc = tsdb_stable_drop(cat, sname);
        if (rc == TSDB_OK) rc = result_status(r, "OK: stable dropped");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: stable not found"); rc = TSDB_ERR_NOTFOUND; }
        else result_status(r, "ERR: drop stable failed");
        break;
    }
    case QAST_STMT_DESCRIBE: {
        /* Resolve in priority: stable → child table → regular table.
         * Emits one row per column with (col_name, type, is_tag,
         * tag_value).  For non-tag columns and stables (which have
         * no per-value tags) the tag_value field is empty. */
        const char *nm = stmt.u.describe.name;
        const char *desc_cols[]  = { "col_name", "type", "is_tag", "tag_value" };
        const tsdb_type_t desc_types[] = {
            TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL, TSDB_TYPE_INT64, TSDB_TYPE_SYMBOL };
        rc = result_init_ddl(r, 4, desc_cols, desc_types);
        if (rc != TSDB_OK) break;

        /* Try as a stable first. */
        tsdb_stable_t st;
        if (tsdb_stable_get(cat, nm, &st) == TSDB_OK) {
            for (int i = 0; i < st.ncols; i++) {
                if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
                result_append_sym(r, 0, st.cols[i].name);
                result_append_sym(r, 1, tsdb_type_name(st.cols[i].type));
                result_append_i64_val(r, 2, 0);
                result_append_sym(r, 3, "");
                result_ddl_end_row(r);
            }
            for (int i = 0; i < st.ntag_cols && rc == TSDB_OK; i++) {
                if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
                result_append_sym(r, 0, st.tag_cols[i].name);
                result_append_sym(r, 1, tsdb_type_name(st.tag_cols[i].type));
                result_append_i64_val(r, 2, 1);
                result_append_sym(r, 3, "");
                result_ddl_end_row(r);
            }
            break;
        }

        /* Try as a child table. */
        tsdb_child_table_t ct;
        if (tsdb_child_table_get(cat, nm, &ct) == TSDB_OK &&
            tsdb_stable_get(cat, ct.stable_name, &st) == TSDB_OK)
        {
            for (int i = 0; i < st.ncols; i++) {
                if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
                result_append_sym(r, 0, st.cols[i].name);
                result_append_sym(r, 1, tsdb_type_name(st.cols[i].type));
                result_append_i64_val(r, 2, 0);
                result_append_sym(r, 3, "");
                result_ddl_end_row(r);
            }
            for (int i = 0; i < st.ntag_cols && i < ct.ntags && rc == TSDB_OK; i++) {
                if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
                char tagval[80];
                switch (ct.tags[i].type) {
                case TSDB_TYPE_INT64:   snprintf(tagval, sizeof(tagval), "%lld",
                                                 (long long)ct.tags[i].v.i); break;
                case TSDB_TYPE_FLOAT64: snprintf(tagval, sizeof(tagval), "%g",
                                                 ct.tags[i].v.f); break;
                case TSDB_TYPE_SYMBOL:  snprintf(tagval, sizeof(tagval), "%s",
                                                 ct.tags[i].v.s); break;
                default: tagval[0] = '\0';
                }
                result_append_sym(r, 0, st.tag_cols[i].name);
                result_append_sym(r, 1, tsdb_type_name(st.tag_cols[i].type));
                result_append_i64_val(r, 2, 1);
                result_append_sym(r, 3, tagval);
                result_ddl_end_row(r);
            }
            break;
        }

        /* Try as a regular table — open it to inspect the schema. */
        tsdb_table_t *tbl = NULL;
        if (tsdb_open_table(db, nm, &tbl) == TSDB_OK && tbl) {
            tsdb_table_internal_t *ti = tsdb_db_find_table(db, nm);
            tsdb_schema_t *sch = ti ? tsdb_tbl_schema(ti) : NULL;
            if (sch) {
                for (int i = 0; i < sch->ncols; i++) {
                    if ((rc = result_ddl_ensure_cap(r)) != TSDB_OK) break;
                    result_append_sym(r, 0, sch->cols[i].name);
                    result_append_sym(r, 1, tsdb_type_name(sch->cols[i].type));
                    result_append_i64_val(r, 2, 0);
                    result_append_sym(r, 3, "");
                    result_ddl_end_row(r);
                }
                break;
            }
        }

        /* Nothing matched. */
        result_status(r, "ERR: table not found");
        rc = TSDB_ERR_NOTFOUND;
        break;
    }
    case QAST_STMT_DROP_TABLE: {
        const char *tn = stmt.u.drop_table.name;
        /* Cascade: if the name is registered as a child-table in the
         * catalog, remove that row so LIST PTABLES stops showing it.
         * Normal tables return NOTFOUND from tsdb_child_table_drop,
         * which we intentionally ignore. */
        (void)tsdb_child_table_drop(cat, tn);
        rc = tsdb_drop_table(db, tn);
        if (rc == TSDB_OK) {
            rc = result_status(r, "OK: table dropped");
        }
        else if (rc == TSDB_ERR_NOTFOUND) {
            result_status(r, "ERR: table not found"); rc = TSDB_ERR_NOTFOUND;
        } else {
            result_status(r, "ERR: drop table failed");
        }
        break;
    }
    case QAST_STMT_TRUNCATE_TABLE: {
        rc = tsdb_truncate_table(db, stmt.u.truncate_table.name);
        if (rc == TSDB_OK) {
            /* Broadcast to all ALIVE peers.  No-op in standalone mode. */
            int acked = 0, total = 0;
            (void)tsdb_cluster_broadcast_truncate(db, stmt.u.truncate_table.name,
                                                   &acked, &total);
            char buf[96];
            if (total > 0) {
                snprintf(buf, sizeof(buf),
                         "OK: table truncated (peers ACKed %d/%d)", acked, total);
            } else {
                snprintf(buf, sizeof(buf), "OK: table truncated");
            }
            rc = result_status(r, buf);
        }
        else if (rc == TSDB_ERR_NOTFOUND) {
            result_status(r, "ERR: table not found"); rc = TSDB_ERR_NOTFOUND;
        } else {
            result_status(r, "ERR: truncate failed");
        }
        break;
    }
    case QAST_STMT_DELETE_RANGE: {
        int removed = 0;
        rc = tsdb_delete_range(db,
                               stmt.u.delete_range.table,
                               stmt.u.delete_range.cutoff_ns,
                               stmt.u.delete_range.op_lt,
                               stmt.u.delete_range.inclusive,
                               &removed);
        if (rc == TSDB_OK) {
            int acked = 0, total = 0;
            (void)tsdb_cluster_broadcast_delete_range(
                    db, stmt.u.delete_range.table,
                    stmt.u.delete_range.cutoff_ns,
                    stmt.u.delete_range.op_lt,
                    stmt.u.delete_range.inclusive,
                    &acked, &total);
            char buf[128];
            if (total > 0) {
                snprintf(buf, sizeof(buf),
                         "OK: %d partition(s) deleted (peers ACKed %d/%d)",
                         removed, acked, total);
            } else {
                snprintf(buf, sizeof(buf),
                         "OK: %d partition(s) deleted", removed);
            }
            rc = result_status(r, buf);
        } else if (rc == TSDB_ERR_NOTFOUND) {
            result_status(r, "ERR: table not found"); rc = TSDB_ERR_NOTFOUND;
        } else {
            result_status(r, "ERR: delete failed");
        }
        break;
    }
    case QAST_STMT_CREATE_CHILD_TABLE: {
        const tsdb_child_table_t *ct_in = &stmt.u.create_child_table.spec;
        /* Fetch stable schema to build the physical table columns. */
        tsdb_stable_t st;
        int src = tsdb_stable_get(cat, ct_in->stable_name, &st);
        if (src != TSDB_OK) {
            result_status(r, "ERR: stable not found");
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        /* Tag count + type validation against the stable's TAGS schema.
         * Without this, `CREATE TABLE pt USING st TAGS ('only-one')`
         * with a 3-tag stable would silently succeed and the missing
         * tags would default to zeros — confusing the per-tag query
         * pushdown later. */
        if (ct_in->ntags != st.ntag_cols) {
            char msg[160];
            snprintf(msg, sizeof(msg),
                     "ERR: tag count mismatch (got %d, stable '%s' expects %d)",
                     ct_in->ntags, st.name, st.ntag_cols);
            result_status(r, msg);
            rc = TSDB_ERR_SCHEMA;
            break;
        }
        {
            int type_err = -1;
            for (int ti = 0; ti < ct_in->ntags; ti++) {
                tsdb_type_t want = st.tag_cols[ti].type;
                tsdb_type_t got  = ct_in->tags[ti].type;
                /* INT64 literals are accepted for FLOAT64 tag slots
                 * (the parser only emits FLOAT for tokens with a dot);
                 * everything else must match exactly. */
                if (want == got) continue;
                if (want == TSDB_TYPE_FLOAT64 && got == TSDB_TYPE_INT64) continue;
                type_err = ti; break;
            }
            if (type_err >= 0) {
                char msg[200];
                snprintf(msg, sizeof(msg),
                         "ERR: tag type mismatch at position %d "
                         "(tag '%s' expects %s)",
                         type_err, st.tag_cols[type_err].name,
                         tsdb_type_name(st.tag_cols[type_err].type));
                result_status(r, msg);
                rc = TSDB_ERR_SCHEMA;
                break;
            }
        }
        /* Build col list from stable cols (excluding TIMESTAMP — use as ts_col). */
        tsdb_col_t cols[TSDB_STABLE_MAX_COLS];
        for (int ci = 0; ci < st.ncols; ci++) {
            cols[ci].name = st.cols[ci].name;
            cols[ci].type = st.cols[ci].type;
        }
        const char *ts_col = st.cols[st.ts_col_idx >= 0 ? st.ts_col_idx : 0].name;
        rc = tsdb_create_table(db, ct_in->name, cols, (size_t)st.ncols, ts_col);
        if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) {
            result_status(r, "ERR: create child table failed");
            break;
        }
        /* rc may be EXISTS here if a background schema-sync hook from a
         * peer already provisioned the physical table.  That's fine —
         * the catalog row is what makes it visible to LIST PTABLES, so
         * we fall through and always try to register the catalog entry.
         * The catalog layer itself returns EXISTS if the row is also
         * already there (idempotent peer-side replay). */
        int physical_preexisted = (rc == TSDB_ERR_EXISTS);
        tsdb_child_table_t ct_registered = *ct_in;
        if (!ct_registered.database[0] && st.database[0]) {
            snprintf(ct_registered.database,
                     sizeof(ct_registered.database), "%s", st.database);
        }
        if (!ct_registered.group[0] && st.group[0]) {
            snprintf(ct_registered.group,
                     sizeof(ct_registered.group), "%s", st.group);
        }
        rc = tsdb_child_table_create(cat, &ct_registered);
        if (rc == TSDB_OK) {
            rc = result_status(r, physical_preexisted
                ? "OK: child table catalog entry created (physical already present)"
                : "OK: child table created");
        } else if (rc == TSDB_ERR_EXISTS) {
            result_status(r, "ERR: child table exists");
            rc = TSDB_ERR_EXISTS;
        } else {
            result_status(r, "ERR: child table catalog failed");
        }
        break;
    }
    case QAST_STMT_CREATE_TABLE: {
        /* Build tsdb_col_t[] pointing at the AST's in-place storage.
         * The cols[].name pointers stay alive for the duration of this
         * call; tsdb_schema_create_ex copies into its own buffers. */
        tsdb_col_t cols[TSDB_STABLE_MAX_COLS];
        for (int i = 0; i < stmt.u.create_table.ncols; i++) {
            cols[i].name = stmt.u.create_table.col_names[i];
            cols[i].type = stmt.u.create_table.col_types[i];
        }
        tsdb_create_partition_t part = stmt.u.create_table.partition_hour
                                          ? TSDB_CREATE_PART_HOUR
                                          : TSDB_CREATE_PART_DAY;
        rc = tsdb_create_table_ex2(db,
                                    stmt.u.create_table.name,
                                    cols,
                                    (size_t)stmt.u.create_table.ncols,
                                    stmt.u.create_table.ts_col,
                                    part,
                                    stmt.u.create_table.block_points);
        if (rc == TSDB_OK)                 rc = result_status(r, "OK: table created");
        else if (rc == TSDB_ERR_EXISTS) { result_status(r, "ERR: table exists"); rc = TSDB_ERR_EXISTS; }
        else if (rc == TSDB_ERR_SCHEMA) { result_status(r, "ERR: schema error (missing or non-timestamp ts col)"); rc = TSDB_ERR_SCHEMA; }
        else if (rc == TSDB_ERR_INVAL)  { result_status(r, "ERR: invalid arguments"); rc = TSDB_ERR_INVAL; }
        else                               result_status(r, "ERR: create table failed");
        break;
    }
    case QAST_STMT_ALTER_ADD_COLUMN: {
        rc = tsdb_alter_table_add_column(db,
            stmt.u.alter_add_column.table,
            stmt.u.alter_add_column.col_name,
            stmt.u.alter_add_column.col_type);
        if (rc == TSDB_OK)                   rc = result_status(r, "OK: column added");
        else if (rc == TSDB_ERR_NOTFOUND)  { result_status(r, "ERR: table not found"); rc = TSDB_ERR_NOTFOUND; }
        else if (rc == TSDB_ERR_EXISTS)    { result_status(r, "ERR: column already exists"); rc = TSDB_ERR_EXISTS; }
        else                                 result_status(r, "ERR: alter add column failed");
        break;
    }

    /* ---- TMQ consumer-group statements --------------------------------- */
    case QAST_STMT_CREATE_CONSUMER_GROUP: {
        tsdb_tmq_t *tmq = tsdb_db_tmq(db);
        if (!tmq) { result_status(r, "ERR: tmq not available"); rc = TSDB_ERR_INTERNAL; break; }
        /* Validate that the topic refers to an existing table. */
        tsdb_table_t *tbl = NULL;
        int trc = tsdb_open_table(db, stmt.u.create_consumer_group.topic, &tbl);
        if (trc != TSDB_OK || !tbl) {
            result_status(r, "ERR: topic (table) not found");
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        rc = tsdb_tmq_group_create(tmq,
                                    stmt.u.create_consumer_group.name,
                                    stmt.u.create_consumer_group.topic);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: consumer group created");
        else if (rc == TSDB_ERR_EXISTS)   { result_status(r, "ERR: consumer group exists"); rc = TSDB_ERR_EXISTS; }
        else if (rc == TSDB_ERR_FULL)     { result_status(r, "ERR: too many consumer groups"); rc = TSDB_ERR_FULL; }
        else                                result_status(r, "ERR: create consumer group failed");
        break;
    }
    case QAST_STMT_JOIN_GROUP: {
        tsdb_tmq_t *tmq = tsdb_db_tmq(db);
        if (!tmq) { result_status(r, "ERR: tmq not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = tsdb_tmq_join(tmq,
                           stmt.u.join_group.name,
                           stmt.u.join_group.consumer_id);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: joined group");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: group not found"); rc = TSDB_ERR_NOTFOUND; }
        else if (rc == TSDB_ERR_EXISTS)   { result_status(r, "ERR: consumer already in group"); rc = TSDB_ERR_EXISTS; }
        else if (rc == TSDB_ERR_FULL)     { result_status(r, "ERR: group full"); rc = TSDB_ERR_FULL; }
        else                                result_status(r, "ERR: join failed");
        break;
    }
    case QAST_STMT_LEAVE_GROUP: {
        tsdb_tmq_t *tmq = tsdb_db_tmq(db);
        if (!tmq) { result_status(r, "ERR: tmq not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = tsdb_tmq_leave(tmq,
                            stmt.u.leave_group.name,
                            stmt.u.leave_group.consumer_id);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: left group");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: group or consumer not found"); rc = TSDB_ERR_NOTFOUND; }
        else                                result_status(r, "ERR: leave failed");
        break;
    }
    case QAST_STMT_COMMIT_OFFSET: {
        tsdb_tmq_t *tmq = tsdb_db_tmq(db);
        if (!tmq) { result_status(r, "ERR: tmq not available"); rc = TSDB_ERR_INTERNAL; break; }
        const char *cid = stmt.u.commit_offset.consumer_id;
        rc = tsdb_tmq_commit(tmq,
                             stmt.u.commit_offset.name,
                             (cid && *cid) ? cid : NULL,
                             stmt.u.commit_offset.seq);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: offset committed");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: group or consumer not found"); rc = TSDB_ERR_NOTFOUND; }
        else if (rc == TSDB_ERR_INVAL)    { result_status(r, "ERR: seq must be monotonic"); rc = TSDB_ERR_INVAL; }
        else                                result_status(r, "ERR: commit failed");
        break;
    }

    case QAST_STMT_EXPORT_PARQUET: {
        const char *tname   = stmt.u.export_parquet.table;
        const char *out_dir = stmt.u.export_parquet.out_dir;
        /* Locate the table + schema. */
        tsdb_table_internal_t *ti = tsdb_db_find_table(db, tname);
        if (!ti) {
            result_status(r, "ERR: table not found");
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        tsdb_schema_t *s     = tsdb_tbl_schema(ti);
        const char    *tdir  = tsdb_tbl_dir(ti);
        /* Ensure output directory exists. */
        if (tsdb_mkdir_p(out_dir) != TSDB_OK) {
            result_status(r, "ERR: cannot create output dir");
            rc = TSDB_ERR_IO;
            break;
        }
        /* Enumerate partition subdirs (YYYYMMDD / YYYYMMDDHH). */
        DIR *d = opendir(tdir);
        if (!d) {
            result_status(r, "ERR: table dir unreadable");
            rc = TSDB_ERR_IO;
            break;
        }
        int nfiles = 0;
        int export_rc = TSDB_OK;
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            size_t nl = strlen(ent->d_name);
            if (nl != 8 && nl != 10) continue;
            int all_digit = 1;
            for (size_t i = 0; i < nl; i++) {
                if (ent->d_name[i] < '0' || ent->d_name[i] > '9') { all_digit = 0; break; }
            }
            if (!all_digit) continue;
            char pdir[4096];
            snprintf(pdir, sizeof(pdir), "%s/%s", tdir, ent->d_name);
            tsdb_part_t *p = NULL;
            if (tsdb_part_open(s, pdir, &p) != TSDB_OK) continue;
            char outfile[4096];
            snprintf(outfile, sizeof(outfile), "%s/%s_%s.parquet",
                     out_dir, tname, ent->d_name);
            int prc = tsdb_part_export_parquet(p, outfile);
            tsdb_part_close(p);
            if (prc != TSDB_OK) { export_rc = prc; break; }
            nfiles++;
        }
        closedir(d);
        if (export_rc != TSDB_OK) {
            result_status(r, "ERR: parquet export failed");
            rc = export_rc;
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg), "OK: exported %d partition(s)", nfiles);
            rc = result_status(r, msg);
        }
        break;
    }

    /* ---- RBAC statements ---------------------------------------------- */
    case QAST_STMT_CREATE_USER: {
        tsdb_auth_t *auth = tsdb_db_auth(db);
        if (!auth) { result_status(r, "ERR: auth not available"); rc = TSDB_ERR_INTERNAL; break; }
        tsdb_user_role_t role = stmt.u.create_user.role ? TSDB_USER_ROLE_ADMIN
                                                         : TSDB_USER_ROLE_NORMAL;
        rc = tsdb_auth_user_create(auth, stmt.u.create_user.name,
                                    stmt.u.create_user.password, role);
        if (rc == TSDB_OK)                 rc = result_status(r, "OK: user created");
        else if (rc == TSDB_ERR_EXISTS)  { result_status(r, "ERR: user exists"); rc = TSDB_ERR_EXISTS; }
        else if (rc == TSDB_ERR_FULL)    { result_status(r, "ERR: too many users"); rc = TSDB_ERR_FULL; }
        else                               result_status(r, "ERR: create user failed");
        break;
    }
    case QAST_STMT_DROP_USER: {
        tsdb_auth_t *auth = tsdb_db_auth(db);
        if (!auth) { result_status(r, "ERR: auth not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = tsdb_auth_user_drop(auth, stmt.u.drop_user.name);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: user dropped");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: user not found"); rc = TSDB_ERR_NOTFOUND; }
        else                                result_status(r, "ERR: drop user failed");
        break;
    }
    case QAST_STMT_LIST_USERS: {
        tsdb_auth_t *auth = tsdb_db_auth(db);
        if (!auth) { result_status(r, "ERR: auth not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = exec_list_users(auth, r);
        break;
    }
    case QAST_STMT_GRANT: {
        tsdb_auth_t *auth = tsdb_db_auth(db);
        if (!auth) { result_status(r, "ERR: auth not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = tsdb_auth_grant(auth, stmt.u.grant.user,
                              stmt.u.grant.priv, stmt.u.grant.resource);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: grant applied");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: user not found"); rc = TSDB_ERR_NOTFOUND; }
        else if (rc == TSDB_ERR_FULL)     { result_status(r, "ERR: too many grants"); rc = TSDB_ERR_FULL; }
        else                                result_status(r, "ERR: grant failed");
        break;
    }
    case QAST_STMT_REVOKE: {
        tsdb_auth_t *auth = tsdb_db_auth(db);
        if (!auth) { result_status(r, "ERR: auth not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = tsdb_auth_revoke(auth, stmt.u.revoke_.user,
                               stmt.u.revoke_.priv, stmt.u.revoke_.resource);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: revoke applied");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: user not found"); rc = TSDB_ERR_NOTFOUND; }
        else                                result_status(r, "ERR: revoke failed");
        break;
    }
    case QAST_STMT_ALTER_USER_PASSWORD: {
        tsdb_auth_t *auth = tsdb_db_auth(db);
        if (!auth) { result_status(r, "ERR: auth not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = tsdb_auth_user_set_password(auth,
                                          stmt.u.alter_user_password.name,
                                          stmt.u.alter_user_password.password);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: password updated");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: user not found"); rc = TSDB_ERR_NOTFOUND; }
        else                                result_status(r, "ERR: alter user failed");
        break;
    }
    case QAST_STMT_CREATE_FUNCTION: {
        tsdb_udf_catalog_t *udfcat = tsdb_db_udf(db);
        if (!udfcat) { result_status(r, "ERR: udf catalog not available"); rc = TSDB_ERR_INTERNAL; break; }
        tsdb_udf_entry_t e;
        memset(&e, 0, sizeof(e));
        snprintf(e.name,    sizeof(e.name),    "%s", stmt.u.create_function.name);
        snprintf(e.so_path, sizeof(e.so_path), "%s", stmt.u.create_function.so_path);
        snprintf(e.symbol,  sizeof(e.symbol),  "%s", stmt.u.create_function.symbol);
        e.nargs      = stmt.u.create_function.nargs;
        for (int i = 0; i < e.nargs; i++) {
            e.arg_types[i] = (tsdb_udf_type_t)stmt.u.create_function.arg_types[i];
        }
        e.ret_type   = (tsdb_udf_type_t)stmt.u.create_function.ret_type;
        e.created_at = tsdb_now_ns();
        rc = tsdb_udf_catalog_create(udfcat, &e);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: function created");
        else if (rc == TSDB_ERR_EXISTS)   { result_status(r, "ERR: function exists or shadows builtin"); rc = TSDB_ERR_EXISTS; }
        else if (rc == TSDB_ERR_FULL)     { result_status(r, "ERR: too many functions"); rc = TSDB_ERR_FULL; }
        else                                result_status(r, "ERR: create function failed");
        break;
    }
    case QAST_STMT_DROP_FUNCTION: {
        tsdb_udf_catalog_t *udfcat = tsdb_db_udf(db);
        if (!udfcat) { result_status(r, "ERR: udf catalog not available"); rc = TSDB_ERR_INTERNAL; break; }
        rc = tsdb_udf_catalog_drop(udfcat, stmt.u.drop_function.name);
        if (rc == TSDB_OK)                  rc = result_status(r, "OK: function dropped");
        else if (rc == TSDB_ERR_NOTFOUND) { result_status(r, "ERR: function not found"); rc = TSDB_ERR_NOTFOUND; }
        else                                result_status(r, "ERR: drop function failed");
        break;
    }
    case QAST_STMT_LIST_FUNCTIONS: {
        tsdb_udf_catalog_t *udfcat = tsdb_db_udf(db);
        if (!udfcat) { result_status(r, "ERR: udf catalog not available"); rc = TSDB_ERR_INTERNAL; break; }
        tsdb_udf_entry_t *arr = NULL;
        size_t n = 0;
        rc = tsdb_udf_catalog_list(udfcat, &arr, &n);
        if (rc != TSDB_OK) { result_status(r, "ERR: list functions failed"); break; }
        const char *names[] = {"name", "nargs", "ret_type", "so_path", "symbol"};
        tsdb_type_t types[] = {TSDB_TYPE_SYMBOL, TSDB_TYPE_INT64, TSDB_TYPE_SYMBOL,
                                TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL};
        rc = result_init_ddl(r, 5, names, types);
        if (rc == TSDB_OK) {
            for (size_t i = 0; i < n; i++) {
                const char *rtn =
                    arr[i].ret_type == TSDB_UDF_TYPE_INT64     ? "INT64"     :
                    arr[i].ret_type == TSDB_UDF_TYPE_FLOAT64   ? "FLOAT64"   :
                    arr[i].ret_type == TSDB_UDF_TYPE_TIMESTAMP ? "TIMESTAMP" : "?";
                result_reserve_rows(r, r->nrows + 1);
                uint32_t c_name    = tsdb_symtab_intern(r->col_symtab[0], arr[i].name);
                uint32_t c_rtn     = tsdb_symtab_intern(r->col_symtab[2], rtn);
                uint32_t c_path    = tsdb_symtab_intern(r->col_symtab[3], arr[i].so_path);
                uint32_t c_symbol  = tsdb_symtab_intern(r->col_symtab[4], arr[i].symbol);
                result_append_cell(r, 0, (uint64_t)c_name);
                ((int64_t *)r->col_data[1])[r->nrows] = arr[i].nargs;
                result_append_cell(r, 2, (uint64_t)c_rtn);
                result_append_cell(r, 3, (uint64_t)c_path);
                result_append_cell(r, 4, (uint64_t)c_symbol);
                r->nrows++;
            }
        }
        free(arr);
        break;
    }

    case QAST_STMT_LIST_MASTERS: {
        tsdb_raft_t *raft_h = tsdb_db_raft_for_db(db);
        static const char *cols_m[]  = { "id", "addr" };
        static const tsdb_type_t tys_m[] = { TSDB_TYPE_SYMBOL, TSDB_TYPE_SYMBOL };
        rc = result_init_ddl(r, 2, cols_m, tys_m);
        if (rc == TSDB_OK && raft_h) {
            tsdb_raft_cfg_member_t mems[TSDB_RAFT_CFG_MAX_MASTERS];
            int nm = tsdb_raft_config_members(raft_h, mems,
                                               TSDB_RAFT_CFG_MAX_MASTERS);
            for (int i = 0; i < nm; i++) {
                char idbuf[32];
                snprintf(idbuf, sizeof(idbuf), "%llu",
                         (unsigned long long)mems[i].id);
                result_append_sym(r, 0, idbuf);
                result_append_sym(r, 1, mems[i].addr);
                result_ddl_end_row(r);
            }
        } else if (rc == TSDB_OK) {
            /* Data-node fallback: no raft handle, so we can't read the
             * committed config.  Fall back to the gossip node manager —
             * every master advertises role=master, so filtering ALIVE
             * nodes by role gives the same answer the leader would
             * return, modulo gossip freshness. */
            tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr_for_db(db);
            if (mgr) {
                tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
                int n = tsdb_node_manager_snapshot(mgr, snap,
                                                    TSDB_CLUSTER_MAX_NODES);
                for (int i = 0; i < n; i++) {
                    if (snap[i].role != TSDB_ROLE_MASTER) continue;
                    if (snap[i].state == TSDB_NODE_DEAD) continue;
                    char idbuf[32];
                    snprintf(idbuf, sizeof(idbuf), "%llu",
                             (unsigned long long)snap[i].id);
                    result_append_sym(r, 0, idbuf);
                    result_append_sym(r, 1, snap[i].addr);
                    result_ddl_end_row(r);
                }
            }
        }
        break;
    }
    case QAST_STMT_ADD_MASTER:
    case QAST_STMT_REMOVE_MASTER: {
        tsdb_raft_t *raft_h = tsdb_db_raft_for_db(db);
        if (!raft_h) {
            result_status(r,
                "ERR: raft not enabled on this node (set "
                "TSDB_CONSENSUS=raft to use membership change)");
            break;
        }
        const char *target = (stmt.kind == QAST_STMT_ADD_MASTER)
                             ? stmt.u.add_master.target
                             : stmt.u.remove_master.target;

        /* Resolve target → (id, addr).  Strategy:
         *   1. If target parses as a decimal u64, use it as id directly.
         *   2. Otherwise treat as "host:port" and look up the id from
         *      the gossip node manager.
         * ADD requires addr; REMOVE can accept either. */
        uint64_t target_id = 0;
        char     target_addr[80] = {0};
        char *endp = NULL;
        unsigned long long parsed = strtoull(target, &endp, 10);
        if (endp && *endp == '\0' && parsed > 0) {
            target_id = (uint64_t)parsed;
        } else {
            snprintf(target_addr, sizeof(target_addr), "%s", target);
            /* Scan node_mgr for matching addr. */
            tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr_for_db(db);
            if (mgr) {
                tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
                int n = tsdb_node_manager_snapshot(mgr, snap, TSDB_CLUSTER_MAX_NODES);
                for (int i = 0; i < n; i++) {
                    if (strcmp(snap[i].addr, target) == 0) {
                        target_id = snap[i].id;
                        break;
                    }
                }
            }
        }
        if (target_id == 0) {
            result_status(r, "ERR: could not resolve target to a node id "
                             "(check TSDB_NODE_ROLE=master seed is alive)");
            break;
        }

        int prc;
        if (stmt.kind == QAST_STMT_ADD_MASTER) {
            prc = tsdb_raft_add_master(raft_h, target_id, target_addr, 5000);
        } else {
            prc = tsdb_raft_remove_master(raft_h, target_id, 5000);
        }
        if (prc == TSDB_ERR_PERMISSION) {
            char msg[160];
            snprintf(msg, sizeof(msg),
                     "ERR: not raft leader (leader=%llu) — retry there",
                     (unsigned long long)tsdb_raft_leader_id(raft_h));
            result_status(r, msg);
        } else if (prc == TSDB_OK) {
            result_status(r, stmt.kind == QAST_STMT_ADD_MASTER
                             ? "OK: master added (raft committed)"
                             : "OK: master removed (raft committed)");
        } else {
            result_status(r, "ERR: raft propose failed (timeout or IO)");
        }
        break;
    }
    default:
        result_status(r, "ERR: unknown statement");
        rc = TSDB_ERR_UNSUPPORTED;
        break;
    }

    /* Catalog-mutating statements get fanned out to every ALIVE peer
     * so every node's catalog stays in lockstep.  The peer-side RPC
     * handler flips tsdb_g_suppress_catalog_broadcast before replaying
     * the statement, which keeps the rebroadcast from looping.
     * DROP STABLE cascades to child tables on both the primary and
     * each peer, so DROP_CHILD doesn't need its own case. */
    switch (stmt.kind) {
    case QAST_STMT_CREATE_DATABASE:
    case QAST_STMT_DROP_DATABASE:
    case QAST_STMT_CREATE_GROUP:
    case QAST_STMT_DROP_GROUP:
    case QAST_STMT_CREATE_STABLE:
    case QAST_STMT_DROP_STABLE:
    case QAST_STMT_CREATE_CHILD_TABLE:
    case QAST_STMT_CREATE_TABLE:
    case QAST_STMT_DROP_TABLE:
        try_broadcast_catalog_qtl(db, qtl, rc);
        break;
    default:
        break;
    }

    if (rc != TSDB_OK) {
        tsdb_result_free(r);
        return rc;
    }
    *out = r;
    return TSDB_OK;
}

/* ---- Authenticated query entrypoint ------------------------------------- */

/* Derive required authorization from a parsed statement.
 *
 *   *out_priv       : bitmask of TSDB_PRIV_* needed (0 when admin_only is set)
 *   *out_resource   : table name or "*" — points at stable storage owned by
 *                     the caller (either a literal or stmt's arena-allocated
 *                     string; caller must copy before freeing arena)
 *   *out_admin_only : non-zero when the statement requires ADMIN role
 */
static void stmt_required_authz(const qast_stmt_t *stmt,
                                 int          *out_priv,
                                 const char  **out_resource,
                                 int          *out_admin_only)
{
    *out_priv        = 0;
    *out_resource    = "*";
    *out_admin_only  = 0;

    switch (stmt->kind) {
    case QAST_STMT_SELECT:
        *out_priv     = TSDB_PRIV_SELECT;
        *out_resource = stmt->u.query.from ? stmt->u.query.from : "*";
        break;

    /* Catalog reads — SELECT privilege on "*" */
    case QAST_STMT_LIST_DATABASES:
    case QAST_STMT_LIST_GROUPS:
    case QAST_STMT_LIST_DEVICES:
    case QAST_STMT_LIST_VTABLES:
    case QAST_STMT_LIST_PTABLES:
    case QAST_STMT_LIST_FUNCTIONS:
    case QAST_STMT_LIST_MASTERS:
        *out_priv = TSDB_PRIV_SELECT;
        break;
    case QAST_STMT_DESCRIBE:
        *out_priv     = TSDB_PRIV_SELECT;
        *out_resource = stmt->u.describe.name;
        break;

    /* Raft membership change — admin-only cluster op. */
    case QAST_STMT_ADD_MASTER:
    case QAST_STMT_REMOVE_MASTER:
        *out_admin_only = 1;
        break;

    /* Schema DDL — resource carries the actual entity name so the
     * audit log shows `object: "auditdb"` instead of the catch-all `*`. */
    case QAST_STMT_CREATE_DATABASE:
        *out_priv = TSDB_PRIV_DDL;
        *out_resource = stmt->u.create_database.name;
        break;
    case QAST_STMT_DROP_DATABASE:
        *out_priv = TSDB_PRIV_DDL;
        *out_resource = stmt->u.drop_database.name;
        break;
    case QAST_STMT_CREATE_GROUP:
        *out_priv = TSDB_PRIV_DDL;
        *out_resource = stmt->u.create_group.spec.name;
        break;
    case QAST_STMT_DROP_GROUP:
        *out_priv = TSDB_PRIV_DDL;
        *out_resource = stmt->u.drop_group.name;
        break;
    case QAST_STMT_CREATE_STABLE:
        *out_priv = TSDB_PRIV_DDL;
        *out_resource = stmt->u.create_stable.spec.name;
        break;
    case QAST_STMT_DROP_STABLE:
        *out_priv = TSDB_PRIV_DDL;
        *out_resource = stmt->u.drop_stable.name;
        break;
    case QAST_STMT_CREATE_DEVICE:
    case QAST_STMT_DROP_DEVICE:
        *out_priv = TSDB_PRIV_DDL;
        break;
    case QAST_STMT_CREATE_CHILD_TABLE:
        *out_priv     = TSDB_PRIV_DDL;
        *out_resource = stmt->u.create_child_table.spec.name;
        break;
    case QAST_STMT_CREATE_TABLE:
        *out_priv     = TSDB_PRIV_DDL;
        *out_resource = stmt->u.create_table.name;
        break;
    case QAST_STMT_ALTER_ADD_COLUMN:
        *out_priv     = TSDB_PRIV_DDL;
        *out_resource = stmt->u.alter_add_column.table;
        break;
    case QAST_STMT_DROP_TABLE:
        *out_priv     = TSDB_PRIV_DDL;
        *out_resource = stmt->u.drop_table.name;
        break;

    /* TRUNCATE / DELETE — data-mutating operations on a specific table.
     * Treat them as DELETE privilege on that table. */
    case QAST_STMT_TRUNCATE_TABLE:
        *out_priv     = TSDB_PRIV_DDL;
        *out_resource = stmt->u.truncate_table.name;
        break;
    case QAST_STMT_DELETE_RANGE:
        *out_priv     = TSDB_PRIV_DDL;
        *out_resource = stmt->u.delete_range.table;
        break;

    /* TMQ consumer-group statements — DDL */
    case QAST_STMT_CREATE_CONSUMER_GROUP:
    case QAST_STMT_JOIN_GROUP:
    case QAST_STMT_LEAVE_GROUP:
    case QAST_STMT_COMMIT_OFFSET:
        *out_priv = TSDB_PRIV_DDL;
        break;

    case QAST_STMT_EXPORT_PARQUET:
        *out_priv     = TSDB_PRIV_SELECT;
        *out_resource = stmt->u.export_parquet.table;
        break;

    /* ADMIN-only: user management + UDF registration.
     * Rationale: CREATE FUNCTION is effectively shell access to the server
     * (dlopen of an arbitrary .so).  A DDL-granted NORMAL user must NOT
     * be able to do this even with GRANT DDL ON *. */
    case QAST_STMT_CREATE_USER:
    case QAST_STMT_DROP_USER:
    case QAST_STMT_LIST_USERS:
    case QAST_STMT_GRANT:
    case QAST_STMT_REVOKE:
    case QAST_STMT_ALTER_USER_PASSWORD:
    case QAST_STMT_CREATE_FUNCTION:
    case QAST_STMT_DROP_FUNCTION:
        *out_admin_only = 1;
        break;
    }
}

/* Classify a statement for the audit log.  Returns the event category
 * ("DDL", "AUTH", "GRANT", "REVOKE", "DATA", "QUERY", "RAFT") and the
 * specific action string ("CREATE TABLE", "LOGIN", ...).  `kind` is
 * whatever tsdb_query_auth already knows about the stmt. */
static void stmt_audit_kind(const qast_stmt_t *stmt,
                             const char **out_event,
                             const char **out_action)
{
    const char *ev = "QUERY", *act = "SELECT";
    switch (stmt->kind) {
    case QAST_STMT_SELECT:                  ev = "QUERY"; act = "SELECT"; break;
    case QAST_STMT_DESCRIBE:                ev = "QUERY"; act = "DESCRIBE"; break;
    case QAST_STMT_CREATE_DATABASE:         ev = "DDL"; act = "CREATE DATABASE"; break;
    case QAST_STMT_DROP_DATABASE:           ev = "DDL"; act = "DROP DATABASE"; break;
    case QAST_STMT_LIST_DATABASES:          ev = "QUERY"; act = "LIST DATABASES"; break;
    case QAST_STMT_CREATE_GROUP:            ev = "DDL"; act = "CREATE GROUP"; break;
    case QAST_STMT_DROP_GROUP:              ev = "DDL"; act = "DROP GROUP"; break;
    case QAST_STMT_LIST_GROUPS:             ev = "QUERY"; act = "LIST GROUPS"; break;
    case QAST_STMT_CREATE_DEVICE:           ev = "DDL"; act = "CREATE DEVICE"; break;
    case QAST_STMT_DROP_DEVICE:             ev = "DDL"; act = "DROP DEVICE"; break;
    case QAST_STMT_LIST_DEVICES:            ev = "QUERY"; act = "LIST DEVICES"; break;
    case QAST_STMT_LIST_VTABLES:            ev = "QUERY"; act = "LIST VTABLES"; break;
    case QAST_STMT_LIST_PTABLES:            ev = "QUERY"; act = "LIST PTABLES"; break;
    case QAST_STMT_CREATE_STABLE:           ev = "DDL"; act = "CREATE STABLE"; break;
    case QAST_STMT_DROP_STABLE:             ev = "DDL"; act = "DROP STABLE"; break;
    case QAST_STMT_CREATE_CHILD_TABLE:      ev = "DDL"; act = "CREATE CHILD TABLE"; break;
    case QAST_STMT_CREATE_TABLE:            ev = "DDL"; act = "CREATE TABLE"; break;
    case QAST_STMT_DROP_TABLE:              ev = "DDL"; act = "DROP TABLE"; break;
    case QAST_STMT_ALTER_ADD_COLUMN:        ev = "DDL"; act = "ALTER ADD COLUMN"; break;
    case QAST_STMT_TRUNCATE_TABLE:          ev = "DATA"; act = "TRUNCATE TABLE"; break;
    case QAST_STMT_DELETE_RANGE:            ev = "DATA"; act = "DELETE RANGE"; break;
    case QAST_STMT_CREATE_CONSUMER_GROUP:   ev = "DDL"; act = "CREATE CONSUMER GROUP"; break;
    case QAST_STMT_JOIN_GROUP:              ev = "DDL"; act = "JOIN GROUP"; break;
    case QAST_STMT_LEAVE_GROUP:             ev = "DDL"; act = "LEAVE GROUP"; break;
    case QAST_STMT_COMMIT_OFFSET:           ev = "DDL"; act = "COMMIT OFFSET"; break;
    case QAST_STMT_EXPORT_PARQUET:          ev = "DATA"; act = "EXPORT PARQUET"; break;
    case QAST_STMT_CREATE_USER:             ev = "AUTH"; act = "CREATE USER"; break;
    case QAST_STMT_DROP_USER:               ev = "AUTH"; act = "DROP USER"; break;
    case QAST_STMT_LIST_USERS:              ev = "QUERY"; act = "LIST USERS"; break;
    case QAST_STMT_GRANT:                   ev = "GRANT"; act = "GRANT"; break;
    case QAST_STMT_REVOKE:                  ev = "REVOKE"; act = "REVOKE"; break;
    case QAST_STMT_ALTER_USER_PASSWORD:     ev = "AUTH"; act = "ALTER USER PASSWORD"; break;
    case QAST_STMT_CREATE_FUNCTION:         ev = "DDL"; act = "CREATE FUNCTION"; break;
    case QAST_STMT_DROP_FUNCTION:           ev = "DDL"; act = "DROP FUNCTION"; break;
    case QAST_STMT_LIST_FUNCTIONS:          ev = "QUERY"; act = "LIST FUNCTIONS"; break;
    case QAST_STMT_LIST_MASTERS:            ev = "QUERY"; act = "LIST MASTERS"; break;
    case QAST_STMT_ADD_MASTER:              ev = "RAFT"; act = "ADD MASTER"; break;
    case QAST_STMT_REMOVE_MASTER:           ev = "RAFT"; act = "REMOVE MASTER"; break;
    }
    if (out_event)  *out_event  = ev;
    if (out_action) *out_action = act;
}

int tsdb_query_auth(tsdb_db_t *db, const char *token,
                     const char *qtl, tsdb_result_t **out)
{
    if (!db || !token || !qtl || !out) return TSDB_ERR_INVAL;

    tsdb_auth_t *auth = tsdb_db_auth(db);
    if (!auth) return TSDB_ERR_INTERNAL;

    /* Parse once to decide authz; then free the arena and delegate to
     * tsdb_query (which re-parses).  Double-parse is negligible for DDL
     * and cheap for SELECT — the simplicity is worth it. */
    tsdb_arena_t a;
    tsdb_arena_init(&a, 16 * 1024);
    qast_stmt_t stmt;
    char err[256];
    int rc = qparse_stmt(qtl, &a, &stmt, err, sizeof(err));
    if (rc != TSDB_OK) { tsdb_arena_free(&a); return rc; }

    int priv = 0, admin_only = 0;
    const char *resource = "*";
    stmt_required_authz(&stmt, &priv, &resource, &admin_only);

    /* Snapshot resource + audit kind before releasing the arena. */
    char resbuf[TSDB_USER_RESOURCE_MAX];
    snprintf(resbuf, sizeof(resbuf), "%s", resource ? resource : "*");
    const char *ev = NULL, *act = NULL;
    stmt_audit_kind(&stmt, &ev, &act);
    tsdb_arena_free(&a);

    /* Resolve the actor name for the audit record; best-effort — if the
     * token is unknown we'll see TSDB_ERR_PERMISSION on the auth check
     * below and still log the attempt with an empty user field. */
    char who[TSDB_USER_NAME_MAX] = "";
    (void)tsdb_auth_token_user(auth, token, who, sizeof(who));
    tsdb_audit_t *aud = tsdb_db_audit(db);

    if (admin_only) {
        tsdb_user_role_t role;
        rc = tsdb_auth_token_role(auth, token, &role);
        if (rc != TSDB_OK) {
            tsdb_audit_write(aud, who, ev, act, resbuf,
                             TSDB_ERR_PERMISSION, "invalid token");
            return TSDB_ERR_PERMISSION;
        }
        if (role != TSDB_USER_ROLE_ADMIN) {
            tsdb_audit_write(aud, who, ev, act, resbuf,
                             TSDB_ERR_PERMISSION, "admin role required");
            return TSDB_ERR_PERMISSION;
        }
    } else if (priv != 0) {
        rc = tsdb_auth_verify(auth, token, priv, resbuf);
        if (rc != TSDB_OK) {
            tsdb_audit_write(aud, who, ev, act, resbuf, rc, "denied");
            return rc;
        }
    } else {
        /* Unknown statement kind with no authz mapping — require a valid
         * token as a minimum bar rather than silently allowing. */
        tsdb_user_role_t role;
        rc = tsdb_auth_token_role(auth, token, &role);
        if (rc != TSDB_OK) {
            tsdb_audit_write(aud, who, ev, act, resbuf,
                             TSDB_ERR_PERMISSION, "invalid token");
            return TSDB_ERR_PERMISSION;
        }
    }

    rc = tsdb_query(db, qtl, out);
    /* Only audit state-changing events — logging every SELECT would make
     * the audit trail unreadable at dashboard scale.  Read-only LIST /
     * QUERY categories are skipped; everything that mutates state,
     * grants, or cluster membership is recorded. */
    if (aud && ev && strcmp(ev, "QUERY") != 0) {
        tsdb_audit_write(aud, who, ev, act, resbuf, rc, "");
    }
    return rc;
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
    /* Free owned symtabs (created by DDL LIST results). */
    if (r->owned_symtabs) {
        for (int i = 0; i < r->n_owned_symtabs; i++) {
            if (r->owned_symtabs[i]) tsdb_symtab_free(r->owned_symtabs[i]);
        }
        free(r->owned_symtabs);
    }
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
