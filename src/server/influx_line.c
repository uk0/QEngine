/* influx_line.c — InfluxDB Line Protocol parser + HTTP ingest for tsdb.
 *
 * Three public functions:
 *   tsdb_influx_line_parse()  — parse one line into structured output
 *   tsdb_influx_ingest()      — ingest a batch of LP lines into the DB
 *   tsdb_influx_http_start()  — start a minimal HTTP server on a given bind addr
 */

#include "influx_line.h"
#include "../../include/tsdb.h"
#include "../storage/db.h"
#include "../storage/schema.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ---- Escape-aware character scanner -------------------------------------- */

/*
 * Context for which character set to treat as delimiters.
 * In measurement:   stop on ','  ' '  (but not '=')
 * In tag key/val:   stop on ','  '='  ' '
 * In field key:     stop on ','  '='  ' '
 * In field value:   stop on ','  ' '  (not '=')
 */

/*
 * Copy a backslash-escape-aware token from src[*pos..] into dst[0..dsz-1].
 * Stops at any un-escaped character in stop_chars, or end of src.
 * Advances *pos past the token (but NOT past the stop character).
 * Returns number of bytes written to dst (not counting NUL).
 */
static inline void build_stop_mask(const char *stop_chars, uint8_t mask[256]) {
    for (int i = 0; i < 256; i++) mask[i] = 0;
    /* Mark backslash with bit 1 (escape marker) and stop chars with
     * bit 0.  One indexed load decides both per-byte branches in the
     * scan loop, replacing the prior strchr-style inner loop. */
    mask[(unsigned char)'\\'] = 2;
    for (const char *s = stop_chars; *s; s++) {
        mask[(unsigned char)*s] |= 1;
    }
}

static size_t scan_token(const char *src, size_t srclen, size_t *pos,
                          const char *stop_chars,
                          char *dst, size_t dsz) {
    /* O(N×K) → O(N): build a 256-byte stop-char bitmap once, then
     * scan with a single indexed load per byte.  Gain on Influx
     * line-protocol ingest is multiplicative because every tag /
     * field / value goes through this. */
    uint8_t mask[256];
    build_stop_mask(stop_chars, mask);

    size_t dpos = 0;
    size_t i = *pos;
    const size_t cap = dsz - 1;
    while (i < srclen && dpos < cap) {
        unsigned char c = (unsigned char)src[i];
        uint8_t m = mask[c];
        if (m & 2) {
            /* Backslash escape — copy the next char literally. */
            if (i + 1 < srclen) {
                dst[dpos++] = src[i + 1];
                i += 2;
            } else {
                i++;
            }
            continue;
        }
        if (m & 1) break;       /* stop char — leave for caller */
        dst[dpos++] = (char)c;
        i++;
    }
    dst[dpos] = '\0';
    *pos = i;
    return dpos;
}

/*
 * Scan a quoted string field value (opening '"' already consumed).
 * Handles \" and \\ inside the string.
 * Advances *pos past the closing '"'.
 */
static size_t scan_quoted(const char *src, size_t srclen, size_t *pos,
                           char *dst, size_t dsz) {
    size_t dpos = 0;
    while (*pos < srclen && dpos < dsz - 1) {
        char c = src[*pos];
        if (c == '"') { (*pos)++; break; }  /* closing quote */
        if (c == '\\' && *pos + 1 < srclen) {
            (*pos)++;
            char nxt = src[*pos];
            dst[dpos++] = (nxt == '"' || nxt == '\\') ? nxt : nxt;
            (*pos)++;
        } else {
            dst[dpos++] = c;
            (*pos)++;
        }
    }
    dst[dpos] = '\0';
    return dpos;
}

/* ---- Parse a single field value ----------------------------------------- */

static int parse_field_value(const char *src, size_t srclen, size_t *pos,
                               tsdb_influx_val_t *val,
                               tsdb_influx_ftype_t *ftype,
                               char *str_buf, size_t str_cap,
                               char **str_out) {
    if (*pos >= srclen) return TSDB_ERR_PARSE;

    char c = src[*pos];

    /* Quoted string. */
    if (c == '"') {
        (*pos)++;
        scan_quoted(src, srclen, pos, str_buf, str_cap);
        *ftype   = TSDB_INFLUX_STRING;
        *str_out = str_buf;
        val->i64 = 0;
        return TSDB_OK;
    }

    /* Boolean: t/T/true/TRUE/f/F/false/FALSE */
    if (c == 't' || c == 'T' || c == 'f' || c == 'F') {
        /* Collect the boolean token. */
        char tmp[16];
        size_t tpos = *pos;
        scan_token(src, srclen, &tpos, ", \n\r", tmp, sizeof(tmp));
        if (!strcasecmp(tmp, "t") || !strcasecmp(tmp, "true")) {
            *ftype   = TSDB_INFLUX_BOOL;
            val->i64 = 1;
            *pos     = tpos;
            *str_out = NULL;
            return TSDB_OK;
        }
        if (!strcasecmp(tmp, "f") || !strcasecmp(tmp, "false")) {
            *ftype   = TSDB_INFLUX_BOOL;
            val->i64 = 0;
            *pos     = tpos;
            *str_out = NULL;
            return TSDB_OK;
        }
        /* Fall through to numeric parse if token didn't match a bool. */
    }

    /* Numeric: collect until comma, space, or end. */
    char numbuf[64];
    size_t npos = *pos;
    scan_token(src, srclen, &npos, ", \n\r", numbuf, sizeof(numbuf));
    if (numbuf[0] == '\0') return TSDB_ERR_PARSE;

    size_t nlen = strlen(numbuf);
    char last   = numbuf[nlen - 1];

    /* Integer: trailing 'i' or 'u'. */
    if (last == 'i' || last == 'u') {
        numbuf[nlen - 1] = '\0';
        char *end = NULL;
        errno = 0;
        int64_t iv = (int64_t)strtoll(numbuf, &end, 10);
        if (errno || !end || *end != '\0') return TSDB_ERR_PARSE;
        *ftype   = TSDB_INFLUX_INT;
        val->i64 = iv;
        *pos     = npos;
        *str_out = NULL;
        return TSDB_OK;
    }

    /* Float (includes plain integers without suffix per LP spec). */
    char *end = NULL;
    errno = 0;
    double dv = strtod(numbuf, &end);
    if (errno || !end || *end != '\0') return TSDB_ERR_PARSE;
    *ftype   = TSDB_INFLUX_FLOAT;
    val->f64 = dv;
    *pos     = npos;
    *str_out = NULL;
    return TSDB_OK;
}

/* ---- Main line parser ---------------------------------------------------- */

int tsdb_influx_line_parse(const char *line, size_t len,
                            char *scratch, size_t scap,
                            char **out_measurement,
                            char **out_tag_keys,   char **out_tag_vals,   int *out_ntags,
                            char **out_field_keys,
                            tsdb_influx_val_t *out_field_vals,
                            tsdb_influx_ftype_t *out_field_types,
                            char **out_field_strs,
                            int *out_nfields,
                            tsdb_ts_t *out_ts) {
    if (!line || len == 0 || !scratch) return TSDB_ERR_PARSE;

    /* Strip trailing newline / CR. */
    while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) len--;
    if (len == 0) return TSDB_ERR_PARSE;

    /* Skip comment lines. */
    if (line[0] == '#') return TSDB_ERR_PARSE;

    size_t spos = 0; /* scratch write position */
    size_t pos  = 0; /* source read position */

    /* Helper: allocate NUL-terminated token in scratch. */
#define SCRATCH_ALLOC(var, need) \
    do { \
        if (spos + (need) + 1 > scap) return TSDB_ERR_NOMEM; \
        (var) = scratch + spos; \
        spos += (need) + 1; \
    } while (0)

    /* ---- Measurement name ---- */
    char *meas;
    SCRATCH_ALLOC(meas, len); /* upper bound */
    size_t mlen = scan_token(line, len, &pos, ", ", meas, scap - spos + len + 1);
    /* Adjust spos to actual length used. */
    spos = (size_t)(meas - scratch) + mlen + 1;
    if (mlen == 0) return TSDB_ERR_PARSE;
    *out_measurement = meas;

    /* ---- Tags (optional) ---- */
    *out_ntags = 0;
    while (pos < len && line[pos] == ',') {
        pos++; /* skip comma */
        int ti = *out_ntags;
        if (ti >= TSDB_INFLUX_MAX_TAGS) return TSDB_ERR_PARSE;

        char *tk;
        SCRATCH_ALLOC(tk, len);
        size_t tklen = scan_token(line, len, &pos, "=,", tk, scap - spos + len + 1);
        spos = (size_t)(tk - scratch) + tklen + 1;
        if (tklen == 0) return TSDB_ERR_PARSE;

        if (pos >= len || line[pos] != '=') return TSDB_ERR_PARSE;
        pos++; /* skip '=' */

        char *tv;
        SCRATCH_ALLOC(tv, len);
        size_t tvlen = scan_token(line, len, &pos, ", ", tv, scap - spos + len + 1);
        spos = (size_t)(tv - scratch) + tvlen + 1;
        if (tvlen == 0) return TSDB_ERR_PARSE;

        out_tag_keys[ti] = tk;
        out_tag_vals[ti] = tv;
        (*out_ntags)++;
    }

    /* ---- Separator: space between tags/measurement and fields ---- */
    if (pos >= len || line[pos] != ' ') return TSDB_ERR_PARSE;
    pos++; /* skip space */
    /* Skip additional spaces (non-standard but common). */
    while (pos < len && line[pos] == ' ') pos++;

    /* ---- Fields (one or more, comma-separated) ---- */
    *out_nfields = 0;
    while (pos < len) {
        int fi = *out_nfields;
        if (fi >= TSDB_INFLUX_MAX_FIELDS) break;

        /* Field key. */
        char *fk;
        SCRATCH_ALLOC(fk, len);
        size_t fklen = scan_token(line, len, &pos, "=,", fk, scap - spos + len + 1);
        spos = (size_t)(fk - scratch) + fklen + 1;
        if (fklen == 0) break;

        if (pos >= len || line[pos] != '=') return TSDB_ERR_PARSE;
        pos++; /* skip '=' */

        /* Field value — needs a string buffer for quoted strings. */
        char *fstr_buf;
        SCRATCH_ALLOC(fstr_buf, len);
        tsdb_influx_val_t fval;
        tsdb_influx_ftype_t ftype;
        char *fstr_out = NULL;
        if (parse_field_value(line, len, &pos, &fval, &ftype,
                               fstr_buf, scap - spos + len + 1,
                               &fstr_out) != TSDB_OK) {
            return TSDB_ERR_PARSE;
        }
        if (fstr_out) {
            size_t fslen = strlen(fstr_buf);
            spos = (size_t)(fstr_buf - scratch) + fslen + 1;
        } else {
            /* String buffer not used; reclaim the scratch space. */
            spos = (size_t)(fstr_buf - scratch);
        }

        out_field_keys[fi]  = fk;
        out_field_vals[fi]  = fval;
        out_field_types[fi] = ftype;
        out_field_strs[fi]  = fstr_out;
        (*out_nfields)++;

        /* Comma → another field; space → timestamp or end. */
        if (pos < len && line[pos] == ',') {
            pos++;
        } else {
            break;
        }
    }

    if (*out_nfields == 0) return TSDB_ERR_PARSE;

    /* ---- Optional timestamp ---- */
    *out_ts = 0;
    /* Skip exactly one space separator if present. */
    if (pos < len && line[pos] == ' ') {
        pos++;
        /* Skip extra spaces. */
        while (pos < len && line[pos] == ' ') pos++;
        if (pos < len && (isdigit((unsigned char)line[pos]) || line[pos] == '-')) {
            char *end = NULL;
            *out_ts = (tsdb_ts_t)strtoll(line + pos, &end, 10);
        }
    }
    /* Default to server time if not present or zero. */
    if (*out_ts == 0) {
        *out_ts = tsdb_now_ns();
    }

#undef SCRATCH_ALLOC
    return TSDB_OK;
}

/* ---- Schema inspection & auto-create table ------------------------------ */

/*
 * Check if a table exists.  Uses internal API to find it without opening.
 */
static int table_exists(tsdb_db_t *db, const char *name) {
    tsdb_table_internal_t *t = tsdb_db_find_table(db, name);
    return (t != NULL) ? 1 : 0;
}

/*
 * Map influx field type to tsdb column type.
 */
static tsdb_type_t influx_to_tsdb_type(tsdb_influx_ftype_t ft) {
    switch (ft) {
    case TSDB_INFLUX_FLOAT:  return TSDB_TYPE_FLOAT64;
    case TSDB_INFLUX_INT:    return TSDB_TYPE_INT64;
    case TSDB_INFLUX_BOOL:   return TSDB_TYPE_INT64;
    case TSDB_INFLUX_STRING: return TSDB_TYPE_SYMBOL;
    default:                  return TSDB_TYPE_FLOAT64;
    }
}

/*
 * Auto-create table from measurement + first line's schema.
 * Columns: ts TIMESTAMP, tag_1..tag_N SYMBOL, field_1..field_M <type>
 */
static int auto_create_table(tsdb_db_t *db, const char *name,
                              char **tag_keys, int ntags,
                              char **field_keys, tsdb_influx_ftype_t *ftypes, int nfields) {
    /* Max columns: 1 ts + ntags + nfields. */
    int total = 1 + ntags + nfields;
    if (total > 64) return TSDB_ERR_INVAL;

    tsdb_col_t cols[64];
    char col_name_store[64][128];
    int ci = 0;

    /* ts column */
    snprintf(col_name_store[ci], 128, "ts");
    cols[ci].name = col_name_store[ci];
    cols[ci].type = TSDB_TYPE_TIMESTAMP;
    ci++;

    /* Tag columns (SYMBOL). */
    for (int i = 0; i < ntags && ci < 64; i++) {
        snprintf(col_name_store[ci], 128, "%s", tag_keys[i]);
        cols[ci].name = col_name_store[ci];
        cols[ci].type = TSDB_TYPE_SYMBOL;
        ci++;
    }

    /* Field columns. */
    for (int i = 0; i < nfields && ci < 64; i++) {
        snprintf(col_name_store[ci], 128, "%s", field_keys[i]);
        cols[ci].name = col_name_store[ci];
        cols[ci].type = influx_to_tsdb_type(ftypes[i]);
        ci++;
    }

    int rc = tsdb_create_table(db, name, cols, (size_t)ci, "ts");
    if (rc == TSDB_ERR_EXISTS) rc = TSDB_OK; /* idempotent */
    return rc;
}

/*
 * Find column index in schema by name; returns -1 if not found.
 */
static int schema_find_col(tsdb_schema_t *schema, const char *name) {
    for (int i = 0; i < schema->ncols; i++) {
        if (strcmp(schema->cols[i].name, name) == 0) return i;
    }
    return -1;
}

/* ---- Auto-create serialisation ------------------------------------------
 *
 * Was: a single module-wide mutex held across the entire pass-2 loop
 * (auto-create + per-table batch_begin/commit).  That serialised every
 * Influx writer through one critical section, capping multi-writer
 * throughput to ~80–200 k rows/s on the 4-node cluster.
 *
 * Now: g_create_mu only protects the auto-create-on-first-row check
 * (so two writers don't race a CREATE TABLE on the same measurement).
 * Per-table batches use the existing tsdb_table_lock_write, which
 * already serialises against replication receives — so lock work
 * scales per-table, not globally.
 */
static pthread_mutex_t g_create_mu = PTHREAD_MUTEX_INITIALIZER;

/* ---- Parsed row record (for two-pass batching) --------------------------- */

/*
 * Parsed representation of one line, heap-allocated scratch owned by `scratch_buf`.
 * We collect all rows, group by measurement, then issue one batch per measurement
 * so there is only one WAL fsync per ingest call (not one per row).
 */
typedef struct influx_row {
    char                 *measurement;    /* points into scratch_buf */
    char                 *tag_keys[TSDB_INFLUX_MAX_TAGS];
    char                 *tag_vals[TSDB_INFLUX_MAX_TAGS];
    int                   ntags;
    char                 *field_keys[TSDB_INFLUX_MAX_FIELDS];
    tsdb_influx_val_t     field_vals[TSDB_INFLUX_MAX_FIELDS];
    tsdb_influx_ftype_t   field_types[TSDB_INFLUX_MAX_FIELDS];
    char                 *field_strs[TSDB_INFLUX_MAX_FIELDS];
    int                   nfields;
    tsdb_ts_t             ts;
    char                 *scratch_buf;   /* owns the string storage; free this */
} influx_row_t;

/*
 * Write a single pre-parsed row into an open batch.
 * Returns TSDB_OK on success.
 *
 * Kept for non-bulk callers; the main pass-2 ingest now goes through
 * write_rows_columnar which packs into the bulk_append wire format
 * once per measurement instead of N row_begin/end pairs per batch.
 */
static int write_row_to_batch(tsdb_batch_t *batch, tsdb_schema_t *schema,
                               const influx_row_t *row) {
    tsdb_batch_row_ts(batch, row->ts);

    for (int i = 0; i < row->ntags; i++) {
        int ci = schema_find_col(schema, row->tag_keys[i]);
        if (ci < 0) continue;
        tsdb_batch_row_sym(batch, ci, row->tag_vals[i]);
    }

    for (int i = 0; i < row->nfields; i++) {
        int ci = schema_find_col(schema, row->field_keys[i]);
        if (ci < 0) continue;
        switch (row->field_types[i]) {
        case TSDB_INFLUX_FLOAT:
            tsdb_batch_row_f64(batch, ci, row->field_vals[i].f64);
            break;
        case TSDB_INFLUX_INT:
        case TSDB_INFLUX_BOOL:
            tsdb_batch_row_i64(batch, ci, row->field_vals[i].i64);
            break;
        case TSDB_INFLUX_STRING:
            if (row->field_strs[i])
                tsdb_batch_row_sym(batch, ci, row->field_strs[i]);
            break;
        }
    }

    return tsdb_batch_row_end(batch);
}

/* --- Bulk pass-2 helper -----------------------------------------------------
 * Pack every row matching `meas` into columnar wire format and append in one
 * critical section via tsdb_batch_append_bulk.  Replaces the per-row dispatch
 * loop on the hot ingest path.
 *
 * Marks each consumed row's measurement to NULL so the outer loop skips
 * already-processed entries.
 */
static int write_rows_columnar(tsdb_batch_t *batch, tsdb_schema_t *schema,
                                influx_row_t *rows, size_t nrows_total,
                                const char *meas)
{
    int ts_ci   = schema->ts_col_idx;
    int n_data  = schema->ncols - 1;

    /* Count matching rows up front so we can size buffers exactly. */
    size_t n = 0;
    for (size_t j = 0; j < nrows_total; j++) {
        if (rows[j].measurement && strcmp(rows[j].measurement, meas) == 0) n++;
    }
    if (n == 0) return TSDB_OK;

    int64_t *ts_arr = (int64_t *)malloc(n * sizeof(int64_t));
    if (!ts_arr) return TSDB_ERR_NOMEM;

    /* Per-data-col working state.  fixed_buf points to a contiguous
     * n×8 array for INT64/FLOAT64; sym_buf is a growable [u32 total]
     * [u16 len][bytes]… buffer for SYMBOL cols. */
    typedef struct {
        int      schema_idx;   /* index into schema->cols[] */
        int      col_type;     /* TSDB_TYPE_* */
        uint8_t *fixed_buf;    /* INT64 / FLOAT64: n × 8 bytes */
        uint8_t *sym_buf;      /* SYMBOL: starts with [u32 total], grows */
        size_t   sym_used;     /* bytes consumed in sym_buf (>=4 for header) */
        size_t   sym_cap;
    } col_state_t;
    col_state_t *cs = (col_state_t *)calloc((size_t)n_data, sizeof(*cs));
    if (!cs) { free(ts_arr); return TSDB_ERR_NOMEM; }

    int data_idx = 0;
    for (int c = 0; c < schema->ncols; c++) {
        if (c == ts_ci) continue;
        cs[data_idx].schema_idx = c;
        cs[data_idx].col_type   = (int)schema->cols[c].type;
        if (cs[data_idx].col_type == TSDB_TYPE_SYMBOL) {
            cs[data_idx].sym_cap  = 4 + n * (2 + 16);
            cs[data_idx].sym_buf  = (uint8_t *)malloc(cs[data_idx].sym_cap);
            cs[data_idx].sym_used = 4;
            if (!cs[data_idx].sym_buf) {
                for (int k = 0; k < data_idx; k++) {
                    free(cs[k].fixed_buf);
                    free(cs[k].sym_buf);
                }
                free(cs); free(ts_arr);
                return TSDB_ERR_NOMEM;
            }
        } else {
            cs[data_idx].fixed_buf = (uint8_t *)calloc(n, 8);
            if (!cs[data_idx].fixed_buf) {
                for (int k = 0; k < data_idx; k++) {
                    free(cs[k].fixed_buf);
                    free(cs[k].sym_buf);
                }
                free(cs); free(ts_arr);
                return TSDB_ERR_NOMEM;
            }
        }
        data_idx++;
    }

    /* Walk rows once.  For each col we scan the row's tag_keys[] then
     * field_keys[] for the matching name (same lookup write_row_to_batch
     * did per-cell, just inlined).  Unset cols get default values: 0 for
     * numeric, empty string for SYMBOL — matches the row-end "all cols
     * set" check on the per-row path which was actually never enforced
     * here either (schema_find_col returning -1 was silently skipped). */
    size_t r = 0;
    for (size_t j = 0; j < nrows_total; j++) {
        if (!rows[j].measurement || strcmp(rows[j].measurement, meas) != 0)
            continue;
        const influx_row_t *row = &rows[j];
        ts_arr[r] = (int64_t)row->ts;

        for (int d = 0; d < n_data; d++) {
            const char *col_name = schema->cols[cs[d].schema_idx].name;
            if (cs[d].col_type == TSDB_TYPE_SYMBOL) {
                const char *sval = NULL;
                for (int t = 0; t < row->ntags; t++) {
                    if (strcmp(row->tag_keys[t], col_name) == 0) {
                        sval = row->tag_vals[t]; break;
                    }
                }
                if (!sval) {
                    for (int f = 0; f < row->nfields; f++) {
                        if (strcmp(row->field_keys[f], col_name) == 0 &&
                            row->field_types[f] == TSDB_INFLUX_STRING)
                        {
                            sval = row->field_strs[f]; break;
                        }
                    }
                }
                if (!sval) sval = "";
                size_t slen = strlen(sval);
                if (slen > 65535) slen = 65535;
                if (cs[d].sym_used + 2 + slen > cs[d].sym_cap) {
                    size_t nc = (cs[d].sym_used + 2 + slen) * 2;
                    uint8_t *nb = (uint8_t *)realloc(cs[d].sym_buf, nc);
                    if (!nb) {
                        for (int k = 0; k < n_data; k++) {
                            free(cs[k].fixed_buf); free(cs[k].sym_buf);
                        }
                        free(cs); free(ts_arr);
                        return TSDB_ERR_NOMEM;
                    }
                    cs[d].sym_buf = nb;
                    cs[d].sym_cap = nc;
                }
                uint16_t l16 = (uint16_t)slen;
                memcpy(cs[d].sym_buf + cs[d].sym_used, &l16, 2);
                memcpy(cs[d].sym_buf + cs[d].sym_used + 2, sval, slen);
                cs[d].sym_used += 2 + slen;
            } else {
                int64_t v = 0;
                double  fv = 0;
                int found = 0;
                for (int f = 0; f < row->nfields; f++) {
                    if (strcmp(row->field_keys[f], col_name) == 0) {
                        switch (row->field_types[f]) {
                        case TSDB_INFLUX_FLOAT:
                            fv = row->field_vals[f].f64;
                            memcpy(cs[d].fixed_buf + r * 8, &fv, 8);
                            break;
                        case TSDB_INFLUX_INT:
                        case TSDB_INFLUX_BOOL:
                            v = row->field_vals[f].i64;
                            memcpy(cs[d].fixed_buf + r * 8, &v, 8);
                            break;
                        case TSDB_INFLUX_STRING:
                            /* Type mismatch — keep default 0. */
                            break;
                        }
                        found = 1; break;
                    }
                }
                if (!found) {
                    /* Default 0 already from calloc — explicit memset
                     * just to make the intent obvious for non-Influx
                     * schemas where col was added later. */
                    memset(cs[d].fixed_buf + r * 8, 0, 8);
                }
            }
        }

        rows[j].measurement = NULL; /* mark consumed */
        r++;
    }

    /* Patch each SYMBOL col's [u32 total] header. */
    for (int d = 0; d < n_data; d++) {
        if (cs[d].col_type == TSDB_TYPE_SYMBOL) {
            uint32_t total = (uint32_t)(cs[d].sym_used - 4);
            memcpy(cs[d].sym_buf, &total, 4);
        }
    }

    /* Hand pointers to the bulk path. */
    const void *col_arrs[TSDB_MAX_COLS];
    int         col_types_arr[TSDB_MAX_COLS];
    for (int d = 0; d < n_data; d++) {
        col_arrs[d]      = (cs[d].col_type == TSDB_TYPE_SYMBOL)
                              ? (const void *)cs[d].sym_buf
                              : (const void *)cs[d].fixed_buf;
        col_types_arr[d] = cs[d].col_type;
    }
    int rc = tsdb_batch_append_bulk(batch, ts_arr, col_arrs, col_types_arr,
                                     n_data, n);

    for (int d = 0; d < n_data; d++) {
        free(cs[d].fixed_buf);
        free(cs[d].sym_buf);
    }
    free(cs);
    free(ts_arr);
    return rc;
}

/* ---- Core ingest function ------------------------------------------------ */

int tsdb_influx_ingest(tsdb_db_t *db, const char *body, size_t n,
                        size_t *out_lines, size_t *out_errors) {
    if (!db || !body) {
        if (out_lines)  *out_lines  = 0;
        if (out_errors) *out_errors = 0;
        return TSDB_OK;
    }

    size_t lines  = 0;
    size_t errors = 0;

    /* ---- Pass 1: parse all lines ----------------------------------------- */

    /* Count data lines first to size the row array. */
    size_t max_rows = 0;
    {
        const char *q = body, *qend = body + n;
        while (q < qend) {
            const char *nl = memchr(q, '\n', (size_t)(qend - q));
            const char *le = nl ? nl : qend;
            size_t ll = (size_t)(le - q);
            if (ll > 0 && q[0] != '#' && q[0] != '\r') max_rows++;
            q = le + (nl ? 1 : 0);
        }
    }

    if (max_rows == 0) {
        if (out_lines)  *out_lines  = 0;
        if (out_errors) *out_errors = 0;
        return TSDB_OK;
    }

    influx_row_t *rows = (influx_row_t *)calloc(max_rows, sizeof(influx_row_t));
    if (!rows) return TSDB_OK;

    /* Single arena for ALL line scratch buffers in this batch.  Was a
     * per-line malloc/free pair which dominated CPU on large
     * payloads (5000 lines = 5000 mallocs).  Arena size is bounded by
     * 4× body_len which leaves headroom for the worst-case escape
     * expansion.  One alloc, one free, zero per-line allocator hit. */
    size_t arena_cap = (size_t)n * 4 + (max_rows * 256);
    char  *arena     = (char *)malloc(arena_cap);
    if (!arena) { free(rows); return TSDB_OK; }
    size_t arena_used = 0;

    size_t nrows = 0;
    const char *p   = body;
    const char *end = body + n;

    while (p < end) {
        const char *nl      = memchr(p, '\n', (size_t)(end - p));
        const char *line_end = nl ? nl : end;
        size_t linelen       = (size_t)(line_end - p);

        if (linelen == 0 || p[0] == '#' || p[0] == '\r') {
            p = line_end + (nl ? 1 : 0);
            continue;
        }

        lines++;

        size_t scap   = linelen * 4 + 256;
        if (arena_used + scap > arena_cap) {
            errors++;
            p = line_end + (nl ? 1 : 0);
            continue;
        }
        char *scratch = arena + arena_used;
        arena_used += scap;
        if (!scratch) {
            errors++;
            p = line_end + (nl ? 1 : 0);
            continue;
        }

        influx_row_t *row = &rows[nrows];
        int rc = tsdb_influx_line_parse(
            p, linelen, scratch, scap,
            &row->measurement,
            row->tag_keys, row->tag_vals, &row->ntags,
            row->field_keys, row->field_vals, row->field_types, row->field_strs,
            &row->nfields, &row->ts);

        if (rc != TSDB_OK) {
            /* `scratch` is an interior pointer into the arena now;
             * the arena owns the storage and is freed once below. */
            errors++;
            p = line_end + (nl ? 1 : 0);
            continue;
        }

        row->scratch_buf = scratch;
        nrows++;
        p = line_end + (nl ? 1 : 0);
    }

    /* ---- Pass 2: group by measurement and write one batch per table ------- */

    /* For each unique measurement, find all rows and write them in one batch. */
    for (size_t i = 0; i < nrows; i++) {
        influx_row_t *first = &rows[i];
        if (!first->measurement) continue; /* already processed */

        const char *meas = first->measurement;

        /* Auto-create the destination table the first time we see a
         * given measurement.  The narrow create_mu guards only this
         * step so two concurrent writers on the same measurement
         * don't both try to create it; once the table exists every
         * subsequent writer can fan out without contention. */
        if (!table_exists(db, meas)) {
            pthread_mutex_lock(&g_create_mu);
            int needs_create = !table_exists(db, meas);
            int rc = TSDB_OK;
            if (needs_create) {
                rc = auto_create_table(db, meas,
                                        first->tag_keys,   first->ntags,
                                        first->field_keys, first->field_types, first->nfields);
            }
            pthread_mutex_unlock(&g_create_mu);
            if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) {
                /* Mark all rows for this measurement as errors. */
                for (size_t j = i; j < nrows; j++) {
                    if (rows[j].measurement && strcmp(rows[j].measurement, meas) == 0) {
                        errors++;
                        rows[j].measurement = NULL;
                    }
                }
                continue;
            }
        }

        tsdb_table_t *tbl = NULL;
        int rc = tsdb_open_table(db, meas, &tbl);
        if (rc != TSDB_OK || !tbl) {
            for (size_t j = i; j < nrows; j++) {
                if (rows[j].measurement && strcmp(rows[j].measurement, meas) == 0) {
                    errors++;
                    rows[j].measurement = NULL;
                }
            }
            continue;
        }

        tsdb_table_internal_t *ti = tsdb_db_find_table(db, meas);
        tsdb_schema_t *schema = ti ? tsdb_tbl_schema(ti) : NULL;
        if (!schema) {
            for (size_t j = i; j < nrows; j++) {
                if (rows[j].measurement && strcmp(rows[j].measurement, meas) == 0) {
                    errors++;
                    rows[j].measurement = NULL;
                }
            }
            continue;
        }

        /* Serialize with any concurrent replication receives on the
         * same table.  The WRITE_BATCH RPC handler takes
         * tsdb_table_lock_write before appending; without matching
         * serialization here, a concurrent influx ingest and a
         * replication receive can both mutate the memtable at once
         * and step on each other's in-flight row — observed as
         * cross-node row drift under the 4-writer stress. */
        tsdb_table_lock_write(tbl);
        tsdb_batch_t *batch = NULL;
        if (tsdb_batch_begin(tbl, &batch) != TSDB_OK) {
            tsdb_table_unlock_write(tbl);
            for (size_t j = i; j < nrows; j++) {
                if (rows[j].measurement && strcmp(rows[j].measurement, meas) == 0) {
                    errors++;
                    rows[j].measurement = NULL;
                }
            }
            continue;
        }

        /* Bulk-pack every row matching this measurement into columnar
         * wire format and append in one critical section.  Hot path for
         * Influx ingest — replaces the per-row begin/setter/end loop
         * that used to dominate single-writer throughput. */
        rc = write_rows_columnar(batch, schema, rows, nrows, meas);
        if (rc != TSDB_OK) {
            tsdb_batch_discard(batch);
            errors++;
        } else {
            if (tsdb_batch_commit(batch) != TSDB_OK) errors++;
        }
        tsdb_table_unlock_write(tbl);
    }

    /* Single free for all scratch buffers (now an arena). */
    free(arena);
    free(rows);

    if (out_lines)  *out_lines  = lines;
    if (out_errors) *out_errors = errors;
    return TSDB_OK;
}

/* ---- Minimal HTTP server ------------------------------------------------- */

typedef struct {
    int         fd;
    tsdb_db_t  *db;
} http_conn_ctx_t;

static void http_send(int fd, const char *resp, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t r = write(fd, resp + sent, len - sent);
        if (r <= 0) break;
        sent += (size_t)r;
    }
}

static void http_send_str(int fd, const char *s) {
    http_send(fd, s, strlen(s));
}

static int read_line_from_fd(int fd, char *buf, size_t cap) {
    size_t pos = 0;
    while (pos < cap - 1) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r <= 0) return -1;
        buf[pos++] = c;
        if (c == '\n') break;
    }
    buf[pos] = '\0';
    return (int)pos;
}

static void *http_conn_handler(void *arg) {
    http_conn_ctx_t *ctx = (http_conn_ctx_t *)arg;
    int fd = ctx->fd;
    tsdb_db_t *db = ctx->db;
    free(ctx);

    /* --- Parse request line: "POST /write HTTP/1.1" --- */
    char req_line[512];
    if (read_line_from_fd(fd, req_line, sizeof(req_line)) <= 0) goto done;

    /* Strip \r\n */
    size_t rlen = strlen(req_line);
    while (rlen > 0 && (req_line[rlen-1] == '\n' || req_line[rlen-1] == '\r'))
        req_line[--rlen] = '\0';

    /* Must be POST /write ... */
    int is_write = (strncmp(req_line, "POST /write", 11) == 0);
    if (!is_write) {
        http_send_str(fd, "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
        goto done;
    }

    /* --- Parse headers; find Content-Length --- */
    long content_length = -1;
    for (;;) {
        char hdr_line[1024];
        int n = read_line_from_fd(fd, hdr_line, sizeof(hdr_line));
        if (n <= 0) goto done;
        /* Empty line = end of headers. */
        if (hdr_line[0] == '\r' || hdr_line[0] == '\n') break;

        /* Case-insensitive match for Content-Length. */
        if (strncasecmp(hdr_line, "Content-Length:", 15) == 0) {
            content_length = atol(hdr_line + 15);
        }
        /* Reject chunked encoding (unsupported in MVP). */
        if (strncasecmp(hdr_line, "Transfer-Encoding:", 18) == 0) {
            http_send_str(fd,
                "HTTP/1.1 400 Bad Request\r\n"
                "Content-Length: 28\r\n\r\n"
                "chunked encoding not supported");
            goto done;
        }
    }

    if (content_length < 0 || content_length > 64 * 1024 * 1024) {
        http_send_str(fd,
            "HTTP/1.1 400 Bad Request\r\n"
            "Content-Length: 21\r\n\r\n"
            "missing content-length");
        goto done;
    }

    /* --- Read body --- */
    char *body = (char *)malloc((size_t)content_length + 1);
    if (!body) {
        http_send_str(fd,
            "HTTP/1.1 500 Internal Server Error\r\n"
            "Content-Length: 3\r\n\r\n"
            "OOM");
        goto done;
    }

    size_t got = 0;
    while (got < (size_t)content_length) {
        ssize_t r = read(fd, body + got, (size_t)content_length - got);
        if (r <= 0) { free(body); goto done; }
        got += (size_t)r;
    }
    body[content_length] = '\0';

    /* --- Ingest --- */
    size_t nlines = 0, nerrors = 0;
    tsdb_influx_ingest(db, body, (size_t)content_length, &nlines, &nerrors);
    free(body);

    /* --- Response --- */
    if (nerrors == nlines && nlines > 0) {
        /* All lines failed. */
        http_send_str(fd,
            "HTTP/1.1 400 Bad Request\r\n"
            "Content-Length: 18\r\n\r\n"
            "all lines rejected");
    } else {
        /* 204 No Content per InfluxDB write API. */
        http_send_str(fd,
            "HTTP/1.1 204 No Content\r\n"
            "Content-Length: 0\r\n\r\n");
    }

done:
    close(fd);
    return NULL;
}

/* Global HTTP server state.
 *
 * Multi-accept: N listener sockets, all bound to the same port via
 * SO_REUSEPORT, each with its own accept thread.  The kernel hashes
 * incoming SYN packets across the listening sockets so concurrent
 * clients don't serialize on a single accept queue.  Tunable via
 * TSDB_INFLUX_ACCEPT_THREADS (default 4, clamped to [1, 32]).
 *
 * On Linux SO_REUSEPORT is the load-balancing variant we want; on
 * macOS the same socket option exists but lacks the kernel-side
 * hashing, so the extra listeners just share the queue — harmless
 * but not faster.  Either way the API is stable. */
#define TSDB_HTTP_MAX_ACCEPT 32
static volatile int g_http_running = 0;
static int          g_http_listen_fds[TSDB_HTTP_MAX_ACCEPT];
static int          g_http_n_listeners = 0;
static pthread_t    g_http_threads[TSDB_HTTP_MAX_ACCEPT];
static tsdb_db_t   *g_http_db = NULL;

static void *http_accept_loop(void *arg) {
    int my_fd = (int)(intptr_t)arg;
    while (g_http_running) {
        struct pollfd pfd = { my_fd, POLLIN, 0 };
        int r = poll(&pfd, 1, 200);
        if (r <= 0) continue;
        if (!(pfd.revents & POLLIN)) continue;

        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(my_fd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }

        http_conn_ctx_t *ctx = (http_conn_ctx_t *)malloc(sizeof(*ctx));
        if (!ctx) { close(cfd); continue; }
        ctx->fd = cfd;
        ctx->db = g_http_db;

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, http_conn_handler, ctx) != 0) {
            close(cfd);
            free(ctx);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

int tsdb_influx_http_start(const char *bind_addr, tsdb_db_t *db) {
    if (!bind_addr || !db) return -1;

    /* Parse host:port. */
    char host[128] = "0.0.0.0";
    int  port = 28091;
    const char *colon = strrchr(bind_addr, ':');
    if (colon) {
        size_t hlen = (size_t)(colon - bind_addr);
        if (hlen < sizeof(host)) {
            memcpy(host, bind_addr, hlen);
            host[hlen] = '\0';
        }
        port = atoi(colon + 1);
    }

    /* TSDB_INFLUX_ACCEPT_THREADS — number of listener sockets bound to
     * the same port via SO_REUSEPORT, each with its own accept thread.
     * Default 4; clamp to [1, TSDB_HTTP_MAX_ACCEPT].  Concurrent
     * clients no longer serialize on a single accept queue. */
    int n_listeners = 4;
    {
        const char *e = getenv("TSDB_INFLUX_ACCEPT_THREADS");
        if (e && *e) n_listeners = atoi(e);
        if (n_listeners < 1) n_listeners = 1;
        if (n_listeners > TSDB_HTTP_MAX_ACCEPT) n_listeners = TSDB_HTTP_MAX_ACCEPT;
    }

    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_aton(host, &sa.sin_addr) == 0) return -1;

    g_http_db        = db;
    g_http_running   = 1;
    g_http_n_listeners = 0;

    for (int i = 0; i < n_listeners; i++) {
        int lfd = socket(AF_INET, SOCK_STREAM, 0);
        if (lfd < 0) goto fail;

        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
        setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

        if (bind(lfd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
            close(lfd);
            goto fail;
        }
        if (listen(lfd, 256) < 0) {
            close(lfd);
            goto fail;
        }
        g_http_listen_fds[i] = lfd;

        if (pthread_create(&g_http_threads[i], NULL,
                           http_accept_loop,
                           (void *)(intptr_t)lfd) != 0) {
            close(lfd);
            goto fail;
        }
        g_http_n_listeners++;
    }

    fprintf(stderr, "[influx] HTTP write endpoint listening on %s:%d (%d accept threads)\n",
            host, port, g_http_n_listeners);
    return 0;

fail:
    g_http_running = 0;
    for (int i = 0; i < g_http_n_listeners; i++) {
        shutdown(g_http_listen_fds[i], SHUT_RDWR);
        close(g_http_listen_fds[i]);
        pthread_join(g_http_threads[i], NULL);
    }
    g_http_n_listeners = 0;
    return -1;
}

void tsdb_influx_http_stop(void) {
    if (!g_http_running) return;
    g_http_running = 0;
    for (int i = 0; i < g_http_n_listeners; i++) {
        if (g_http_listen_fds[i] >= 0) {
            shutdown(g_http_listen_fds[i], SHUT_RDWR);
            close(g_http_listen_fds[i]);
            g_http_listen_fds[i] = -1;
        }
    }
    for (int i = 0; i < g_http_n_listeners; i++) {
        pthread_join(g_http_threads[i], NULL);
    }
    g_http_n_listeners = 0;
    g_http_db = NULL;
}
