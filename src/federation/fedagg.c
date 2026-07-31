/* fedagg.c — federation result aggregator.
 *
 * Creates and merges tsdb_result_t objects produced by multiple clusters.
 *
 * NOTE: struct tsdb_result is defined in src/query/result_internal.h.
 * The public tsdb_result_* accessor functions are defined ONCE in exec.c.
 * This file only creates/allocates results via fedagg_result_alloc/append/free.
 */

#include "fedagg.h"
#include "../query/result_internal.h"
#include "../../include/tsdb.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <stdint.h>

/* ---- Result builder helpers ----------------------------------------------- */

tsdb_result_t *fedagg_result_alloc(int ncols,
                                   const char **col_names,
                                   const tsdb_type_t *col_types)
{
    tsdb_result_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;

    r->ncols     = ncols;
    r->col_names  = calloc((size_t)ncols, sizeof(char *));
    r->col_types  = calloc((size_t)ncols, sizeof(tsdb_type_t));
    r->col_symtab = calloc((size_t)ncols, sizeof(void *)); /* always NULL */
    r->col_data   = calloc((size_t)ncols, sizeof(void *));
    r->cur        = -1;

    if (!r->col_names || !r->col_types || !r->col_data || !r->col_symtab) {
        fedagg_result_free(r);
        return NULL;
    }

    for (int i = 0; i < ncols; i++) {
        r->col_names[i] = col_names ? strdup(col_names[i]) : strdup("");
        r->col_types[i] = col_types ? col_types[i] : TSDB_TYPE_INT64;
    }
    return r;
}

int fedagg_result_append(tsdb_result_t *r, const uint64_t *vals) {
    if (!r || !vals) return -1;

    if (r->nrows >= r->cap_rows) {
        size_t ncap = r->cap_rows ? r->cap_rows * 2 : 256;
        for (int c = 0; c < r->ncols; c++) {
            void *np = realloc(r->col_data[c], ncap * 8);
            if (!np) return -1;
            r->col_data[c] = np;
        }
        r->cap_rows = ncap;
    }

    for (int c = 0; c < r->ncols; c++) {
        ((uint64_t *)r->col_data[c])[r->nrows] = vals[c];
    }
    r->nrows++;
    return 0;
}

void fedagg_result_free(tsdb_result_t *r) {
    if (!r) return;
    if (r->col_names) {
        for (int i = 0; i < r->ncols; i++) free(r->col_names[i]);
    }
    if (r->col_data) {
        for (int i = 0; i < r->ncols; i++) free(r->col_data[i]);
    }
    free(r->col_names);
    free(r->col_types);
    free(r->col_symtab);
    free(r->col_data);
    /* Decoded federation results own a symtab per SYMBOL column (see the
     * dictionary section in fedrpc.c).  tsdb_result_free() already released
     * these; this path did not, so every fedrpc_decode_result error unwind
     * leaked one table per column decoded so far. */
    if (r->owned_symtabs) {
        for (int i = 0; i < r->n_owned_symtabs; i++) {
            if (r->owned_symtabs[i]) tsdb_symtab_free(r->owned_symtabs[i]);
        }
        free(r->owned_symtabs);
    }
    free(r);
}

/* ---- Detect column intent ------------------------------------------------- */

static int is_count_col(const char *name) {
    return name && strncasecmp(name, "count", 5) == 0;
}
static int is_fed_sum(const char *name) {
    return name && strncmp(name, "__fed_sum", 9) == 0;
}
static int is_fed_cnt(const char *name) {
    return name && strncmp(name, "__fed_cnt", 9) == 0;
}
static int is_min_col(const char *name) {
    return name && strncasecmp(name, "min", 3) == 0;
}
static int is_max_col(const char *name) {
    return name && strncasecmp(name, "max", 3) == 0;
}
static int is_sum_col(const char *name) {
    return name && strncasecmp(name, "sum", 3) == 0;
}

/* ---- Scalar aggregate merge --------------------------------------------- */

static tsdb_result_t *merge_scalar(tsdb_result_t **partial, bool *partial_ok,
                                   int npartial, bool has_avg_rewrite,
                                   int *miss_out)
{
    tsdb_result_t *schema_r = NULL;
    for (int i = 0; i < npartial; i++) {
        if (partial_ok[i] && partial[i] && partial[i]->nrows > 0) {
            schema_r = partial[i];
            break;
        }
    }
    if (!schema_r) return NULL;

    int ncols = schema_r->ncols;
    const char **names = (const char **)schema_r->col_names;
    tsdb_result_t *out = fedagg_result_alloc(ncols, names, schema_r->col_types);
    if (!out) return NULL;

    uint64_t acc[128]     = {0};
    uint64_t min_acc[128];
    uint64_t max_acc[128];
    int      has_first[128] = {0};
    for (int c = 0; c < ncols && c < 128; c++) {
        min_acc[c] = UINT64_MAX;
        max_acc[c] = 0;
    }

    int merged = 0;
    for (int i = 0; i < npartial; i++) {
        if (!partial_ok[i] || !partial[i] || partial[i]->nrows == 0) {
            if (miss_out) (*miss_out)++;
            continue;
        }
        tsdb_result_t *p = partial[i];
        /* Iterate all rows (may be >1 for raw aggregates). */
        for (size_t row = 0; row < p->nrows; row++) {
            for (int c = 0; c < ncols && c < 128; c++) {
                uint64_t v = ((uint64_t *)p->col_data[c])[row];
                const char *cname = p->col_names[c];

                if (is_count_col(cname) || is_fed_cnt(cname)) {
                    int64_t sv, sv2;
                    memcpy(&sv, &acc[c], 8);
                    memcpy(&sv2, &v, 8);
                    sv += sv2;
                    memcpy(&acc[c], &sv, 8);
                } else if (is_fed_sum(cname) || is_sum_col(cname)) {
                    double sf, sv;
                    memcpy(&sf, &acc[c], 8);
                    memcpy(&sv, &v, 8);
                    sf += sv;
                    memcpy(&acc[c], &sf, 8);
                } else if (is_min_col(cname)) {
                    double cur, nv;
                    memcpy(&cur, &min_acc[c], 8);
                    memcpy(&nv, &v, 8);
                    if (!has_first[c] || nv < cur) {
                        min_acc[c] = v;
                        has_first[c] = 1;
                    }
                } else if (is_max_col(cname)) {
                    double cur, nv;
                    memcpy(&cur, &max_acc[c], 8);
                    memcpy(&nv, &v, 8);
                    if (!has_first[c] || nv > cur) {
                        max_acc[c] = v;
                        has_first[c] = 1;
                    }
                } else {
                    /* Default: sum for numeric. */
                    if (schema_r->col_types[c] == TSDB_TYPE_INT64 ||
                        schema_r->col_types[c] == TSDB_TYPE_TIMESTAMP) {
                        int64_t sv, sv2;
                        memcpy(&sv, &acc[c], 8);
                        memcpy(&sv2, &v, 8);
                        sv += sv2;
                        memcpy(&acc[c], &sv, 8);
                    } else {
                        double df, dv;
                        memcpy(&df, &acc[c], 8);
                        memcpy(&dv, &v, 8);
                        df += dv;
                        memcpy(&acc[c], &df, 8);
                    }
                }
            }
        }
        merged++;
    }

    if (merged == 0) { fedagg_result_free(out); return NULL; }

    /* Post-process: fix min/max columns. */
    for (int c = 0; c < ncols && c < 128; c++) {
        const char *cname = schema_r->col_names[c];
        if (is_min_col(cname)) acc[c] = has_first[c] ? min_acc[c] : 0;
        else if (is_max_col(cname)) acc[c] = has_first[c] ? max_acc[c] : 0;
    }

    /* AVG rewrite: __fed_sum_X + __fed_cnt_X → avg_X. */
    int drop[128] = {0};
    if (has_avg_rewrite) {
        for (int c = 0; c < ncols && c < 128; c++) {
            if (!is_fed_sum(schema_r->col_names[c])) continue;
            const char *suffix = schema_r->col_names[c] + 9;
            int cnt_col = -1;
            for (int d = 0; d < ncols && d < 128; d++) {
                if (is_fed_cnt(schema_r->col_names[d])) {
                    if (strcmp(schema_r->col_names[d] + 9, suffix) == 0) {
                        cnt_col = d;
                        break;
                    }
                }
            }
            if (cnt_col >= 0) {
                double s;
                int64_t n_i64;
                memcpy(&s,     &acc[c],      8);
                memcpy(&n_i64, &acc[cnt_col], 8);
                double n = (double)n_i64;   /* int64 → double conversion */
                double avg = (n > 0.0) ? s / n : 0.0;
                memcpy(&acc[c], &avg, 8);
                /* Rename sum col to avg col, mark cnt col for drop. */
                free(out->col_names[c]);
                char newname[128];
                snprintf(newname, sizeof(newname), "avg%s", suffix);
                out->col_names[c] = strdup(newname);
                out->col_types[c] = TSDB_TYPE_FLOAT64;
                drop[cnt_col] = 1;
            }
        }
    }

    /* Build output, skip dropped columns. */
    int out_cols[128]; int nout = 0;
    for (int c = 0; c < ncols && c < 128; c++) {
        if (!drop[c]) out_cols[nout++] = c;
    }

    if (nout < ncols) {
        const char *new_names[128];
        tsdb_type_t new_types[128];
        for (int i = 0; i < nout; i++) {
            new_names[i] = out->col_names[out_cols[i]];
            new_types[i] = out->col_types[out_cols[i]];
        }
        tsdb_result_t *r2 = fedagg_result_alloc(nout, new_names, new_types);
        fedagg_result_free(out);
        if (!r2) return NULL;
        uint64_t row[128];
        for (int i = 0; i < nout; i++) row[i] = acc[out_cols[i]];
        fedagg_result_append(r2, row);
        r2->cur = -1;
        return r2;
    }

    fedagg_result_append(out, acc);
    out->cur = -1;
    return out;
}

/* ---- SAMPLE BY bucket merge ---------------------------------------------- */

typedef struct {
    uint64_t ts;
    uint64_t vals[127];
    int      ncols;
} bucket_row_t;

static int cmp_bucket_row(const void *a, const void *b) {
    const bucket_row_t *ra = (const bucket_row_t *)a;
    const bucket_row_t *rb = (const bucket_row_t *)b;
    if (ra->ts < rb->ts) return -1;
    if (ra->ts > rb->ts) return 1;
    return 0;
}

static tsdb_result_t *merge_sample_by(tsdb_result_t **partial, bool *partial_ok,
                                      int npartial, bool has_avg_rewrite,
                                      int64_t limit, int *miss_out)
{
    tsdb_result_t *schema_r = NULL;
    for (int i = 0; i < npartial; i++) {
        if (partial_ok[i] && partial[i]) { schema_r = partial[i]; break; }
    }
    if (!schema_r) return NULL;

    int ncols = schema_r->ncols;
    if (ncols > 128) ncols = 128;

    size_t total_rows = 0;
    for (int i = 0; i < npartial; i++) {
        if (partial_ok[i] && partial[i]) total_rows += partial[i]->nrows;
        else if (miss_out) (*miss_out)++;
    }
    if (total_rows == 0) return NULL;

    bucket_row_t *rows = calloc(total_rows, sizeof(bucket_row_t));
    if (!rows) return NULL;

    size_t ri = 0;
    for (int i = 0; i < npartial; i++) {
        if (!partial_ok[i] || !partial[i]) continue;
        tsdb_result_t *p = partial[i];
        for (size_t row = 0; row < p->nrows; row++) {
            rows[ri].ncols = (ncols - 1 < 127) ? (ncols - 1) : 127;
            rows[ri].ts = ((uint64_t *)p->col_data[0])[row];
            for (int c = 1; c < ncols && c - 1 < 127; c++) {
                rows[ri].vals[c - 1] = ((uint64_t *)p->col_data[c])[row];
            }
            ri++;
        }
    }

    qsort(rows, ri, sizeof(bucket_row_t), cmp_bucket_row);

    const char **out_names = (const char **)schema_r->col_names;
    tsdb_result_t *out = fedagg_result_alloc(ncols, out_names, schema_r->col_types);
    if (!out) { free(rows); return NULL; }

    size_t k = 0;
    while (k < ri) {
        uint64_t cur_ts = rows[k].ts;
        double   sums[127]  = {0};
        double   mins[127];
        double   maxs[127];
        int64_t  cnts[127]  = {0};
        int      has_f[127] = {0};

        for (int c = 0; c < ncols - 1 && c < 127; c++) {
            double v; memcpy(&v, &rows[k].vals[c], 8);
            mins[c] = v; maxs[c] = v;
        }

        while (k < ri && rows[k].ts == cur_ts) {
            for (int c = 0; c < ncols - 1 && c < 127; c++) {
                uint64_t raw = rows[k].vals[c];
                /* Determine if this column is integer type (count/fed_cnt). */
                int col_idx = c + 1; /* schema col index */
                int is_int_col = (schema_r->col_types[col_idx] == TSDB_TYPE_INT64);
                double v;
                if (is_int_col) {
                    int64_t iv; memcpy(&iv, &raw, 8);
                    v = (double)iv;
                } else {
                    memcpy(&v, &raw, 8);
                }
                sums[c] += v;
                if (!has_f[c] || v < mins[c]) mins[c] = v;
                if (!has_f[c] || v > maxs[c]) maxs[c] = v;
                cnts[c]++;
                has_f[c] = 1;
            }
            k++;
        }

        uint64_t outrow[128];
        outrow[0] = cur_ts;
        for (int c = 1; c < ncols && c - 1 < 127; c++) {
            const char *cname = schema_r->col_names[c];
            int col_is_int = (schema_r->col_types[c] == TSDB_TYPE_INT64);
            if (is_count_col(cname) || is_fed_cnt(cname)) {
                /* Count/fed_cnt: sums[] holds the accumulated double-converted int. */
                int64_t cnt_i = (int64_t)sums[c - 1];
                memcpy(&outrow[c], &cnt_i, 8);
            } else if (is_min_col(cname)) {
                if (col_is_int) { int64_t iv = (int64_t)mins[c-1]; memcpy(&outrow[c], &iv, 8); }
                else { memcpy(&outrow[c], &mins[c - 1], 8); }
            } else if (is_max_col(cname)) {
                if (col_is_int) { int64_t iv = (int64_t)maxs[c-1]; memcpy(&outrow[c], &iv, 8); }
                else { memcpy(&outrow[c], &maxs[c - 1], 8); }
            } else {
                /* sum or fed_sum: store as float64. */
                memcpy(&outrow[c], &sums[c - 1], 8);
            }
        }

        /* AVG rewrite per bucket: __fed_sum / __fed_cnt.
         * __fed_sum is float64 (sum of floats), stored in outrow as float64 bits.
         * __fed_cnt is int64 (count), stored in outrow as int64 bits. */
        if (has_avg_rewrite) {
            for (int c = 1; c < ncols; c++) {
                if (!is_fed_sum(schema_r->col_names[c])) continue;
                const char *suffix = schema_r->col_names[c] + 9;
                for (int d = 1; d < ncols; d++) {
                    if (is_fed_cnt(schema_r->col_names[d]) &&
                        strcmp(schema_r->col_names[d] + 9, suffix) == 0) {
                        double s;
                        int64_t n_i64;
                        memcpy(&s,     &outrow[c], 8);
                        memcpy(&n_i64, &outrow[d], 8);
                        double n = (double)n_i64;
                        double avg = (n > 0.0) ? s / n : 0.0;
                        memcpy(&outrow[c], &avg, 8);
                        int64_t zero_i = 0;
                        memcpy(&outrow[d], &zero_i, 8);
                        break;
                    }
                }
            }
        }

        fedagg_result_append(out, outrow);
        if (limit > 0 && (int64_t)out->nrows >= limit) break;
    }

    free(rows);
    out->cur = -1;

    /* Rename __fed_sum → avg in schema. */
    if (has_avg_rewrite) {
        for (int c = 0; c < out->ncols; c++) {
            if (is_fed_sum(out->col_names[c])) {
                const char *suffix = out->col_names[c] + 9;
                free(out->col_names[c]);
                char newname[128];
                snprintf(newname, sizeof(newname), "avg%s", suffix);
                out->col_names[c] = strdup(newname);
                out->col_types[c] = TSDB_TYPE_FLOAT64;
            }
            if (is_fed_cnt(out->col_names[c])) {
                free(out->col_names[c]);
                out->col_names[c] = strdup("__cnt");
            }
        }
    }
    return out;
}

/* ---- Main merge entry point ----------------------------------------------- */

int fedagg_merge(tsdb_result_t **partial, bool *partial_ok,
                 int npartial,
                 bool is_sample_by,
                 int64_t limit,
                 bool has_avg_rewrite,
                 tsdb_result_t **out,
                 int *partial_miss_count)
{
    if (!partial || !out) return TSDB_ERR_INVAL;
    if (partial_miss_count) *partial_miss_count = 0;

    tsdb_result_t *merged = NULL;

    if (is_sample_by) {
        merged = merge_sample_by(partial, partial_ok, npartial,
                                 has_avg_rewrite, limit, partial_miss_count);
    } else {
        merged = merge_scalar(partial, partial_ok, npartial,
                              has_avg_rewrite, partial_miss_count);
    }

    if (!merged) return TSDB_ERR_IO;

    if (!is_sample_by && limit > 0 && (int64_t)merged->nrows > limit) {
        merged->nrows = (size_t)limit;
    }

    *out = merged;
    return TSDB_OK;
}
