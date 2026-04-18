/* parquet.c — minimal Parquet v1 writer (PLAIN / UNCOMPRESSED).
 *
 * See parquet.h for scope. The implementation is intentionally tight:
 * everything is buffered in memory, then written out as one file.
 *
 * Correctness references:
 *   https://parquet.apache.org/docs/file-format/
 *   https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
 *   Thrift Compact Protocol (field headers, zigzag varint, list headers).
 */

#include "parquet.h"
#include "part.h"
#include "schema.h"
#include "../core/symbol.h"
#include "../core/types.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <sys/stat.h>

/* ---- Parquet enum values (from parquet.thrift) ------------------------- */
#define PQ_TYPE_BOOLEAN              0
#define PQ_TYPE_INT32                1
#define PQ_TYPE_INT64                2
#define PQ_TYPE_FLOAT                4
#define PQ_TYPE_DOUBLE               5
#define PQ_TYPE_BYTE_ARRAY           6

#define PQ_ENCODING_PLAIN            0
#define PQ_ENCODING_RLE              3

#define PQ_COMPRESSION_UNCOMPRESSED  0

#define PQ_PAGE_TYPE_DATA_PAGE       0

#define PQ_REPTYPE_REQUIRED          0
/* #define PQ_REPTYPE_OPTIONAL       1   -- not used in MVP */

#define PQ_CONVERTED_TYPE_UTF8       0

/* ---- Thrift-compact protocol type codes -------------------------------- */
#define TC_STOP        0
#define TC_TRUE        1
#define TC_FALSE       2
#define TC_BYTE        3
#define TC_I16         4
#define TC_I32         5
#define TC_I64         6
#define TC_DOUBLE      7
#define TC_BINARY      8   /* also used for string */
#define TC_LIST        9
#define TC_SET        10
#define TC_MAP        11
#define TC_STRUCT     12

/* ---- Dynamic byte buffer ----------------------------------------------- */

typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} buf_t;

static int buf_reserve(buf_t *b, size_t need) {
    if (b->len + need <= b->cap) return 0;
    size_t nc = b->cap ? b->cap : 256;
    while (nc < b->len + need) nc *= 2;
    uint8_t *nb = realloc(b->data, nc);
    if (!nb) return -1;
    b->data = nb;
    b->cap  = nc;
    return 0;
}

static int buf_put(buf_t *b, const void *p, size_t n) {
    if (buf_reserve(b, n) < 0) return -1;
    memcpy(b->data + b->len, p, n);
    b->len += n;
    return 0;
}

static int buf_putc(buf_t *b, uint8_t c) {
    if (buf_reserve(b, 1) < 0) return -1;
    b->data[b->len++] = c;
    return 0;
}

static void buf_free(buf_t *b) { free(b->data); b->data = NULL; b->len = b->cap = 0; }

/* ---- Little-endian fixed-width writers (for the data pages, not thrift) */

static int buf_put_u32le(buf_t *b, uint32_t v) {
    uint8_t t[4];
    t[0] = (uint8_t)v; t[1] = (uint8_t)(v >> 8);
    t[2] = (uint8_t)(v >> 16); t[3] = (uint8_t)(v >> 24);
    return buf_put(b, t, 4);
}

static int buf_put_i64le(buf_t *b, int64_t v) {
    uint64_t u = (uint64_t)v;
    uint8_t t[8];
    for (int i = 0; i < 8; i++) t[i] = (uint8_t)(u >> (i * 8));
    return buf_put(b, t, 8);
}

static int buf_put_f64le(buf_t *b, double v) {
    uint64_t u;
    memcpy(&u, &v, 8);
    uint8_t t[8];
    for (int i = 0; i < 8; i++) t[i] = (uint8_t)(u >> (i * 8));
    return buf_put(b, t, 8);
}

/* ---- Thrift compact primitives ----------------------------------------- */

/* Unsigned varint (LEB128). */
static int tc_uvarint(buf_t *b, uint64_t v) {
    while (v >= 0x80) {
        if (buf_putc(b, (uint8_t)(v | 0x80)) < 0) return -1;
        v >>= 7;
    }
    return buf_putc(b, (uint8_t)v);
}

/* Zigzag-encoded signed varint (for i32/i64 fields). */
static int tc_zigzag_i32(buf_t *b, int32_t v) {
    uint32_t z = ((uint32_t)v << 1) ^ (uint32_t)(v >> 31);
    return tc_uvarint(b, z);
}

static int tc_zigzag_i64(buf_t *b, int64_t v) {
    uint64_t z = ((uint64_t)v << 1) ^ (uint64_t)(v >> 63);
    return tc_uvarint(b, z);
}

/*
 * Thrift-compact struct writer with cursor for field-id delta encoding.
 *
 *   last_fid: last emitted field id (start at 0 per struct nesting level).
 * For each write_* call, pass the absolute field id; the helper computes
 * the delta and updates last_fid.
 */

typedef struct {
    int16_t last_fid;
} tc_ctx_t;

static void tc_init(tc_ctx_t *c) { c->last_fid = 0; }

/* Field header for field id `fid`, thrift compact type code `type`.
 * Uses short form (delta encoded in upper nibble) when delta fits in [1..15]. */
static int tc_field_hdr(buf_t *b, tc_ctx_t *c, int16_t fid, uint8_t type) {
    int16_t delta = (int16_t)(fid - c->last_fid);
    if (delta >= 1 && delta <= 15) {
        if (buf_putc(b, (uint8_t)((delta << 4) | (type & 0x0F))) < 0) return -1;
    } else {
        if (buf_putc(b, (uint8_t)(type & 0x0F)) < 0) return -1;
        if (tc_zigzag_i32(b, fid) < 0) return -1;
    }
    c->last_fid = fid;
    return 0;
}

static int tc_stop(buf_t *b) { return buf_putc(b, TC_STOP); }

/* ---- High-level field writers (struct-internal) ------------------------ */

static int tc_write_bool(buf_t *b, tc_ctx_t *c, int16_t fid, int v) {
    /* Short form: pack true/false into the type nibble. */
    uint8_t tcode = v ? TC_TRUE : TC_FALSE;
    return tc_field_hdr(b, c, fid, tcode);
}

static int tc_write_i32(buf_t *b, tc_ctx_t *c, int16_t fid, int32_t v) {
    if (tc_field_hdr(b, c, fid, TC_I32) < 0) return -1;
    return tc_zigzag_i32(b, v);
}

static int tc_write_i64(buf_t *b, tc_ctx_t *c, int16_t fid, int64_t v) {
    if (tc_field_hdr(b, c, fid, TC_I64) < 0) return -1;
    return tc_zigzag_i64(b, v);
}

static int tc_write_binary(buf_t *b, tc_ctx_t *c, int16_t fid,
                            const void *data, size_t n) {
    if (tc_field_hdr(b, c, fid, TC_BINARY) < 0) return -1;
    if (tc_uvarint(b, (uint64_t)n) < 0) return -1;
    if (n > 0 && buf_put(b, data, n) < 0) return -1;
    return 0;
}

static int tc_write_string(buf_t *b, tc_ctx_t *c, int16_t fid, const char *s) {
    return tc_write_binary(b, c, fid, s, s ? strlen(s) : 0);
}

/* List header: short form `(size << 4) | elem_type` when size < 15;
 * long form `(0xF << 4) | elem_type` + uvarint(size) otherwise. */
static int tc_write_list_hdr(buf_t *b, tc_ctx_t *c, int16_t fid,
                              uint8_t elem_type, size_t count) {
    if (tc_field_hdr(b, c, fid, TC_LIST) < 0) return -1;
    if (count < 15) {
        return buf_putc(b, (uint8_t)((count << 4) | (elem_type & 0x0F)));
    }
    if (buf_putc(b, (uint8_t)(0xF0 | (elem_type & 0x0F))) < 0) return -1;
    return tc_uvarint(b, (uint64_t)count);
}

static int tc_write_struct_hdr(buf_t *b, tc_ctx_t *c, int16_t fid) {
    return tc_field_hdr(b, c, fid, TC_STRUCT);
}

/* ---- Parquet-level writers --------------------------------------------- */

/*
 * Serialize a PageHeader for a DATA_PAGE with fields:
 *   1: type = DATA_PAGE
 *   2: uncompressed_page_size (i32)
 *   3: compressed_page_size   (i32)
 *   5: data_page_header {
 *          1: num_values (i32)
 *          2: encoding = PLAIN (i32)
 *          3: definition_level_encoding = RLE (i32)
 *          4: repetition_level_encoding = RLE (i32)
 *      }
 */
static int write_page_header(buf_t *out, int32_t uncompressed_size,
                              int32_t compressed_size, int32_t num_values)
{
    tc_ctx_t c; tc_init(&c);
    if (tc_write_i32(out, &c, 1, PQ_PAGE_TYPE_DATA_PAGE) < 0) return -1;
    if (tc_write_i32(out, &c, 2, uncompressed_size) < 0) return -1;
    if (tc_write_i32(out, &c, 3, compressed_size) < 0) return -1;
    if (tc_write_struct_hdr(out, &c, 5) < 0) return -1;
    {
        tc_ctx_t dc; tc_init(&dc);
        if (tc_write_i32(out, &dc, 1, num_values) < 0) return -1;
        if (tc_write_i32(out, &dc, 2, PQ_ENCODING_PLAIN) < 0) return -1;
        if (tc_write_i32(out, &dc, 3, PQ_ENCODING_RLE) < 0) return -1;
        if (tc_write_i32(out, &dc, 4, PQ_ENCODING_RLE) < 0) return -1;
        if (tc_stop(out) < 0) return -1;
    }
    return tc_stop(out);
}

/* ---- Column gathering --------------------------------------------------- */

/*
 * Decompress all blocks for a column into a single contiguous buffer of raw
 * values (the memory layout for SYMBOL is an array of uint32 codes).
 *
 * Returns TSDB_OK with *out_raw (heap-allocated, caller frees) and
 * *out_count filled, or a negative error.
 */
static int gather_column_raw(tsdb_part_t *part, int col_idx,
                              tsdb_type_t type,
                              void **out_raw, size_t *out_count)
{
    *out_raw = NULL;
    *out_count = 0;

    tsdb_block_meta_t *metas = NULL;
    size_t nblocks = 0;
    int rc = tsdb_part_col_blocks(part, col_idx, &metas, &nblocks);
    if (rc != TSDB_OK) return rc;
    if (nblocks == 0) { free(metas); return TSDB_OK; }

    size_t total = 0;
    for (size_t i = 0; i < nblocks; i++) total += metas[i].count;

    size_t width = tsdb_type_width(type);
    if (width == 0) { free(metas); return TSDB_ERR_UNSUPPORTED; }

    uint8_t *buf = malloc(total * width);
    if (!buf) { free(metas); return TSDB_ERR_NOMEM; }

    size_t off = 0;
    for (size_t i = 0; i < nblocks; i++) {
        rc = tsdb_part_read_block(part, col_idx, &metas[i], buf + off);
        if (rc != TSDB_OK) { free(buf); free(metas); return rc; }
        off += (size_t)metas[i].count * width;
    }
    free(metas);

    *out_raw   = buf;
    *out_count = total;
    return TSDB_OK;
}

/* ---- PLAIN encoders ----------------------------------------------------- */

static int encode_plain_int64(buf_t *out, const int64_t *vals, size_t n) {
    if (buf_reserve(out, n * 8) < 0) return -1;
    for (size_t i = 0; i < n; i++) {
        if (buf_put_i64le(out, vals[i]) < 0) return -1;
    }
    return 0;
}

static int encode_plain_double(buf_t *out, const double *vals, size_t n) {
    if (buf_reserve(out, n * 8) < 0) return -1;
    for (size_t i = 0; i < n; i++) {
        if (buf_put_f64le(out, vals[i]) < 0) return -1;
    }
    return 0;
}

static int encode_plain_byte_array_from_symbols(buf_t *out,
                                                 const uint32_t *codes,
                                                 size_t n,
                                                 tsdb_symtab_t *symtab)
{
    for (size_t i = 0; i < n; i++) {
        const char *s = symtab ? tsdb_symtab_str(symtab, codes[i]) : NULL;
        uint32_t slen = s ? (uint32_t)strlen(s) : 0u;
        if (buf_put_u32le(out, slen) < 0) return -1;
        if (slen && buf_put(out, s, slen) < 0) return -1;
    }
    return 0;
}

/* ---- SchemaElement emission -------------------------------------------- */

/*
 * Root schema element: just a name + num_children. No type, no
 * repetition_type (root has no repetition_type per spec).
 */
static int write_schema_root(buf_t *out, int32_t num_children, const char *name) {
    tc_ctx_t c; tc_init(&c);
    /* 4: name (required), 5: num_children */
    if (tc_write_string(out, &c, 4, name && *name ? name : "schema") < 0) return -1;
    if (tc_write_i32(out, &c, 5, num_children) < 0) return -1;
    return tc_stop(out);
}

/*
 * Leaf schema element: type, repetition_type=REQUIRED, name, converted_type
 * only when needed (UTF8 for SYMBOL).
 */
static int write_schema_leaf(buf_t *out,
                              int32_t type,
                              int32_t rep_type,
                              const char *name,
                              int has_converted,
                              int32_t converted_type)
{
    tc_ctx_t c; tc_init(&c);
    /* 1: type */
    if (tc_write_i32(out, &c, 1, type) < 0) return -1;
    /* 3: repetition_type */
    if (tc_write_i32(out, &c, 3, rep_type) < 0) return -1;
    /* 4: name */
    if (tc_write_string(out, &c, 4, name) < 0) return -1;
    /* 6: converted_type (optional) */
    if (has_converted) {
        if (tc_write_i32(out, &c, 6, converted_type) < 0) return -1;
    }
    return tc_stop(out);
}

/* ---- Per-column temp state --------------------------------------------- */

typedef struct {
    /* Cached schema info. */
    const char   *name;
    tsdb_type_t   tsdb_type;
    int32_t       pq_type;
    int           is_symbol;

    /* Encoded PLAIN page bytes. */
    buf_t         plain;

    /* Row count in this column (must match across all columns). */
    size_t        nvals;

    /* Offsets recorded as we write the file body. */
    uint64_t      page_header_offset;   /* absolute file offset of page header */
    uint64_t      page_header_size;     /* bytes in the page header */
    uint64_t      total_size;           /* page_header_size + plain.len */
} col_state_t;

/*
 * Serialize one DataPage (header + PLAIN bytes) into `body`. Records the
 * column's absolute file offset.
 *
 * body_base_offset is the file offset at which `body` begins (i.e. 4, since
 * the magic "PAR1" precedes it). cs->page_header_offset is recorded as an
 * absolute file offset.
 */
static int serialize_column_page(buf_t *body, col_state_t *cs,
                                  uint64_t body_base_offset)
{
    cs->page_header_offset = body_base_offset + body->len;

    buf_t hdr = {0};
    int32_t page_bytes = (int32_t)cs->plain.len;
    if (write_page_header(&hdr, page_bytes, page_bytes, (int32_t)cs->nvals) < 0) {
        buf_free(&hdr);
        return -1;
    }
    cs->page_header_size = hdr.len;

    if (buf_put(body, hdr.data, hdr.len) < 0) { buf_free(&hdr); return -1; }
    buf_free(&hdr);

    if (cs->plain.len > 0 &&
        buf_put(body, cs->plain.data, cs->plain.len) < 0) return -1;

    cs->total_size = cs->page_header_size + cs->plain.len;
    return 0;
}

/* ---- ColumnChunk + ColumnMetaData -------------------------------------- */

/*
 * Emit ColumnMetaData for one column.
 *
 * Fields we set:
 *   1: type (i32)
 *   2: encodings = [PLAIN, RLE]              (RLE included because most
 *                                             readers expect it even when
 *                                             def/rep levels are absent)
 *   3: path_in_schema = [ name ]
 *   4: codec = UNCOMPRESSED
 *   5: num_values (i64)
 *   6: total_uncompressed_size (i64) = page_header_size + page_bytes
 *   7: total_compressed_size   (i64) = same (uncompressed)
 *   9: data_page_offset (i64)  = absolute file offset of the PageHeader
 */
static int write_column_meta(buf_t *out, const col_state_t *cs)
{
    tc_ctx_t c; tc_init(&c);

    /* 1: type */
    if (tc_write_i32(out, &c, 1, cs->pq_type) < 0) return -1;

    /* 2: encodings = list<Encoding> of 2 elements: PLAIN, RLE */
    {
        if (tc_write_list_hdr(out, &c, 2, TC_I32, 2) < 0) return -1;
        if (tc_zigzag_i32(out, PQ_ENCODING_PLAIN) < 0) return -1;
        if (tc_zigzag_i32(out, PQ_ENCODING_RLE)   < 0) return -1;
    }

    /* 3: path_in_schema = list<string> of 1 */
    {
        if (tc_write_list_hdr(out, &c, 3, TC_BINARY, 1) < 0) return -1;
        size_t nlen = strlen(cs->name);
        if (tc_uvarint(out, (uint64_t)nlen) < 0) return -1;
        if (nlen && buf_put(out, cs->name, nlen) < 0) return -1;
    }

    /* 4: codec */
    if (tc_write_i32(out, &c, 4, PQ_COMPRESSION_UNCOMPRESSED) < 0) return -1;

    /* 5: num_values */
    if (tc_write_i64(out, &c, 5, (int64_t)cs->nvals) < 0) return -1;

    /* 6: total_uncompressed_size */
    if (tc_write_i64(out, &c, 6, (int64_t)cs->total_size) < 0) return -1;

    /* 7: total_compressed_size */
    if (tc_write_i64(out, &c, 7, (int64_t)cs->total_size) < 0) return -1;

    /* 9: data_page_offset */
    if (tc_write_i64(out, &c, 9, (int64_t)cs->page_header_offset) < 0) return -1;

    return tc_stop(out);
}

/*
 * ColumnChunk:
 *   2: file_offset (i64, required) — set to 0 per spec (deprecated).
 *   3: meta_data (struct, optional-but-required-in-practice)
 */
static int write_column_chunk(buf_t *out, const col_state_t *cs) {
    tc_ctx_t c; tc_init(&c);
    if (tc_write_i64(out, &c, 2, 0) < 0) return -1;
    if (tc_write_struct_hdr(out, &c, 3) < 0) return -1;
    if (write_column_meta(out, cs) < 0) return -1;
    return tc_stop(out);
}

/*
 * RowGroup:
 *   1: columns = list<ColumnChunk>
 *   2: total_byte_size (i64) = sum of column total sizes (uncompressed)
 *   3: num_rows (i64)
 */
static int write_row_group(buf_t *out, const col_state_t *cols, int ncols,
                            int64_t total_byte_size, int64_t num_rows)
{
    tc_ctx_t c; tc_init(&c);
    /* 1: list<ColumnChunk> */
    if (tc_write_list_hdr(out, &c, 1, TC_STRUCT, (size_t)ncols) < 0) return -1;
    for (int i = 0; i < ncols; i++) {
        if (write_column_chunk(out, &cols[i]) < 0) return -1;
    }
    /* 2: total_byte_size */
    if (tc_write_i64(out, &c, 2, total_byte_size) < 0) return -1;
    /* 3: num_rows */
    if (tc_write_i64(out, &c, 3, num_rows) < 0) return -1;
    return tc_stop(out);
}

/*
 * FileMetaData:
 *   1: version (i32) = 1
 *   2: schema = list<SchemaElement> [root, col1, col2, ...]
 *   3: num_rows (i64)
 *   4: row_groups = list<RowGroup> of 1
 *   6: created_by (string, optional) = "tsdb parquet writer"
 */
static int write_file_metadata(buf_t *out,
                                const col_state_t *cols, int ncols,
                                int64_t total_byte_size, int64_t num_rows,
                                const char *table_name)
{
    tc_ctx_t c; tc_init(&c);

    /* 1: version */
    if (tc_write_i32(out, &c, 1, 1) < 0) return -1;

    /* 2: schema = list of (1 root + ncols leaves) */
    {
        size_t total_elems = (size_t)ncols + 1;
        if (tc_write_list_hdr(out, &c, 2, TC_STRUCT, total_elems) < 0) return -1;
        if (write_schema_root(out, ncols, table_name) < 0) return -1;
        for (int i = 0; i < ncols; i++) {
            int has_conv = cols[i].is_symbol;
            int32_t conv = PQ_CONVERTED_TYPE_UTF8;
            if (write_schema_leaf(out, cols[i].pq_type, PQ_REPTYPE_REQUIRED,
                                   cols[i].name, has_conv, conv) < 0) return -1;
        }
    }

    /* 3: num_rows */
    if (tc_write_i64(out, &c, 3, num_rows) < 0) return -1;

    /* 4: row_groups = [one row group] */
    {
        if (tc_write_list_hdr(out, &c, 4, TC_STRUCT, 1) < 0) return -1;
        if (write_row_group(out, cols, ncols, total_byte_size, num_rows) < 0) return -1;
    }

    /* 6: created_by */
    if (tc_write_string(out, &c, 6, "tsdb parquet writer") < 0) return -1;

    return tc_stop(out);
}

/* ---- Top-level export -------------------------------------------------- */

static void col_state_free(col_state_t *cs, int ncols) {
    for (int i = 0; i < ncols; i++) buf_free(&cs[i].plain);
    free(cs);
}

int tsdb_part_export_parquet(tsdb_part_t *part, const char *out_path) {
    if (!part || !out_path) return TSDB_ERR_INVAL;

    /* Access schema via the tsdb_part's internal field; parquet.c lives
     * alongside part.c in src/storage, so we peek at the same struct layout.
     * But tsdb_part is opaque — use the provided accessors where possible.
     * Read the schema via a helper block: read blocks for each column. */

    /* We need the schema to know column names/types. tsdb_part_t is opaque,
     * but tsdb_part_col_blocks + tsdb_part_read_block cover the data path.
     * For schema we rely on the caller passing a part that was opened with
     * the schema they want; we peek at schema via a sibling helper. */

    /* Use an extern accessor we provide in part.c (see tsdb_part_schema). */
    extern tsdb_schema_t *tsdb_part_schema(const tsdb_part_t *p);
    tsdb_schema_t *s = tsdb_part_schema(part);
    if (!s) return TSDB_ERR_INVAL;

    int ncols = s->ncols;
    if (ncols <= 0) return TSDB_ERR_INVAL;

    col_state_t *cs = calloc((size_t)ncols, sizeof(*cs));
    if (!cs) return TSDB_ERR_NOMEM;

    /* Cache column metadata + encode PLAIN pages. */
    size_t row_count = 0;
    int    row_count_set = 0;
    int    rc = TSDB_OK;
    for (int i = 0; i < ncols; i++) {
        cs[i].name      = s->cols[i].name;
        cs[i].tsdb_type = s->cols[i].type;
        cs[i].is_symbol = 0;

        switch (s->cols[i].type) {
        case TSDB_TYPE_TIMESTAMP:
        case TSDB_TYPE_INT64:
            cs[i].pq_type = PQ_TYPE_INT64;
            break;
        case TSDB_TYPE_FLOAT64:
            cs[i].pq_type = PQ_TYPE_DOUBLE;
            break;
        case TSDB_TYPE_SYMBOL:
            cs[i].pq_type   = PQ_TYPE_BYTE_ARRAY;
            cs[i].is_symbol = 1;
            break;
        default:
            rc = TSDB_ERR_UNSUPPORTED;
            goto done;
        }

        void  *raw    = NULL;
        size_t nvals  = 0;
        rc = gather_column_raw(part, i, s->cols[i].type, &raw, &nvals);
        if (rc != TSDB_OK) goto done;

        if (!row_count_set) { row_count = nvals; row_count_set = 1; }
        else if (nvals != row_count) {
            free(raw);
            rc = TSDB_ERR_CORRUPT;
            goto done;
        }

        cs[i].nvals = nvals;

        if (nvals > 0) {
            int enc = 0;
            switch (s->cols[i].type) {
            case TSDB_TYPE_TIMESTAMP:
            case TSDB_TYPE_INT64:
                enc = encode_plain_int64(&cs[i].plain, (const int64_t *)raw, nvals);
                break;
            case TSDB_TYPE_FLOAT64:
                enc = encode_plain_double(&cs[i].plain, (const double *)raw, nvals);
                break;
            case TSDB_TYPE_SYMBOL:
                enc = encode_plain_byte_array_from_symbols(
                        &cs[i].plain, (const uint32_t *)raw, nvals,
                        s->cols[i].symtab);
                break;
            default:
                enc = -1;
                break;
            }
            free(raw);
            if (enc < 0) { rc = TSDB_ERR_NOMEM; goto done; }
        } else {
            free(raw);
        }
    }

    /* Empty partition: return OK without creating a file. */
    if (row_count == 0) { rc = TSDB_OK; goto done; }

    /* Assemble file body: "PAR1" + per-column (PageHeader || PLAIN bytes). */
    buf_t body = {0};
    const uint8_t MAGIC[4] = { 'P', 'A', 'R', '1' };

    if (buf_put(&body, MAGIC, 4) < 0) { buf_free(&body); rc = TSDB_ERR_NOMEM; goto done; }

    int64_t total_col_bytes = 0;
    for (int i = 0; i < ncols; i++) {
        /* body_base_offset == 0 means absolute-file-offset == body->len. */
        if (serialize_column_page(&body, &cs[i], 0) < 0) {
            buf_free(&body); rc = TSDB_ERR_NOMEM; goto done;
        }
        total_col_bytes += (int64_t)cs[i].total_size;
    }

    /* Footer: FileMetaData + u32 LE length + "PAR1" */
    buf_t footer = {0};
    if (write_file_metadata(&footer, cs, ncols,
                             total_col_bytes, (int64_t)row_count,
                             s->name) < 0) {
        buf_free(&footer); buf_free(&body);
        rc = TSDB_ERR_NOMEM; goto done;
    }
    uint32_t footer_len = (uint32_t)footer.len;

    /* Write out the file. */
    FILE *fp = fopen(out_path, "wb");
    if (!fp) { buf_free(&footer); buf_free(&body); rc = TSDB_ERR_IO; goto done; }

    int wrote_ok = 1;
    if (fwrite(body.data, 1, body.len, fp) != body.len) wrote_ok = 0;
    if (wrote_ok && fwrite(footer.data, 1, footer.len, fp) != footer.len) wrote_ok = 0;
    if (wrote_ok) {
        uint8_t lbuf[4];
        lbuf[0] = (uint8_t)footer_len;
        lbuf[1] = (uint8_t)(footer_len >> 8);
        lbuf[2] = (uint8_t)(footer_len >> 16);
        lbuf[3] = (uint8_t)(footer_len >> 24);
        if (fwrite(lbuf, 1, 4, fp) != 4) wrote_ok = 0;
    }
    if (wrote_ok && fwrite(MAGIC, 1, 4, fp) != 4) wrote_ok = 0;
    if (fflush(fp) != 0) wrote_ok = 0;
    fclose(fp);

    buf_free(&body);
    buf_free(&footer);

    if (!wrote_ok) { rc = TSDB_ERR_IO; goto done; }

done:
    col_state_free(cs, ncols);
    return rc;
}
