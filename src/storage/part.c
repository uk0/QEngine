/* part.c — disk partition flush and read. */

#include "part.h"
#include "iopolicy.h"
#include "../compress/codec.h"
#include "../core/bits.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

/*
 * Raw-block hook type local to part.c (mirrors db.h's tsdb_on_raw_block_fn,
 * but avoids a circular include since db.h includes part.h).
 * tsdb_db_get_raw_block_hook is defined in db.c.
 */
typedef int (*part_raw_block_fn_t)(void *ud, struct tsdb_db *db,
                                    const char *table_name,
                                    uint32_t part_day,
                                    uint16_t col_idx,
                                    const tsdb_block_meta_t *meta,
                                    const uint8_t *block_bytes,
                                    size_t block_bytes_len);
/* out_fn is void** (function pointer via void*) to avoid typedef mismatch. */
extern void tsdb_db_get_raw_block_hook(struct tsdb_db *db,
                                        void **out_fn,
                                        void **out_ud);

/* Forward declaration of mkdir_p from schema.c. */
extern int tsdb_mkdir_p(const char *path);

/* ---- Byte-level serialization helpers ----------------------------------- */

static inline void put_u8(uint8_t *p, uint8_t v)    { p[0] = v; }
static inline void put_u16le(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static inline void put_u32le(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static inline void put_u64le(uint8_t *p, uint64_t v) {
    put_u32le(p,   (uint32_t)v);
    put_u32le(p+4, (uint32_t)(v>>32));
}
static inline void put_i64le(uint8_t *p, int64_t v)  { put_u64le(p, (uint64_t)v); }

static inline uint32_t get_u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static inline uint64_t get_u64le(const uint8_t *p) {
    return (uint64_t)get_u32le(p) | ((uint64_t)get_u32le(p+4)<<32);
}
static inline int64_t  get_i64le(const uint8_t *p)  { return (int64_t)get_u64le(p); }
static inline uint16_t get_u16le(const uint8_t *p)  { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }

/* ---- Partition directory name from timestamp ----------------------------- */

/*
 * Stringify a timestamp into its partition directory name, honouring the
 * schema's partition_unit:
 *   DAY  -> "YYYYMMDD"        (9 bytes incl. NUL)
 *   HOUR -> "YYYYMMDDHH"     (11 bytes incl. NUL)
 * buf must be at least 11 bytes.
 */
static void ts_to_part_str(int64_t ts_ns, tsdb_partition_unit_t unit, char *buf) {
    time_t secs = (time_t)(ts_ns / 1000000000LL);
    struct tm tm;
    gmtime_r(&secs, &tm);
    if (unit == TSDB_PARTITION_HOUR) {
        snprintf(buf, 11, "%04d%02d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour);
    } else {
        snprintf(buf, 11, "%04d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    }
}

/*
 * Partition index = unique bucket number covering one row's timestamp.
 * DAY  -> days since 1970-01-01   (negative OK)
 * HOUR -> hours since 1970-01-01 00:00 UTC
 */
static int64_t ts_part_index(int64_t ts_ns, tsdb_partition_unit_t unit) {
    int64_t ns_per_unit = (unit == TSDB_PARTITION_HOUR)
                          ? 3600000000000LL
                          : 86400000000000LL;
    if (ts_ns >= 0) return ts_ns / ns_per_unit;
    return (ts_ns - ns_per_unit + 1) / ns_per_unit;
}

/*
 * Pack a partition string into a uint32 integer identifier used by the
 * cluster's raw-block hook (opaque to consumers). YYYYMMDD fits into
 * u32. YYYYMMDDHH overflows u32 — for HOUR partitions we compute
 * day*100+hour*1 packed differently; we encode as (year*1000000 +
 * month*10000 + day*100 + hour) which fits within u32 through year 4294.
 */
static uint32_t part_str_to_int(const char *s, tsdb_partition_unit_t unit) {
    unsigned y = 0, mo = 0, dy = 0, hh = 0;
    if (unit == TSDB_PARTITION_HOUR) {
        sscanf(s, "%4u%2u%2u%2u", &y, &mo, &dy, &hh);
        return (uint32_t)(y * 1000000u + mo * 10000u + dy * 100u + hh);
    }
    sscanf(s, "%4u%2u%2u", &y, &mo, &dy);
    return (uint32_t)(y * 10000u + mo * 100u + dy);
}

/* ---- BlockHeader helpers ------------------------------------------------ */

/*
 * BlockHeader layout (32 bytes, little-endian):
 *   [0..3]   magic   u32 = TSDB_BLOCK_MAGIC
 *   [4]      codec   u8
 *   [5]      _pad    u8
 *   [6..7]   flags   u16
 *   [8..11]  count   u32
 *   [12..19] ts_min  i64
 *   [20..27] ts_max  i64
 *   [28..31] size    u32  (compressed data bytes following this header)
 */
static size_t write_block_header(uint8_t *buf,
                                 uint8_t codec, uint16_t flags,
                                 uint32_t count,
                                 int64_t ts_min, int64_t ts_max,
                                 uint32_t data_size)
{
    put_u32le(buf + 0,  TSDB_BLOCK_MAGIC);
    put_u8   (buf + 4,  codec);
    put_u8   (buf + 5,  0);
    put_u16le(buf + 6,  flags);
    put_u32le(buf + 8,  count);
    put_i64le(buf + 12, ts_min);
    put_i64le(buf + 20, ts_max);
    put_u32le(buf + 28, data_size);
    return TSDB_BLOCK_HEADER_SIZE;
}

static int read_block_header(const uint8_t *buf,
                             uint8_t *out_codec, uint16_t *out_flags,
                             uint32_t *out_count,
                             int64_t *out_ts_min, int64_t *out_ts_max,
                             uint32_t *out_size)
{
    uint32_t magic = get_u32le(buf);
    if (magic != TSDB_BLOCK_MAGIC) return TSDB_ERR_CORRUPT;
    *out_codec  = buf[4];
    *out_flags  = get_u16le(buf + 6);
    *out_count  = get_u32le(buf + 8);
    *out_ts_min = get_i64le(buf + 12);
    *out_ts_max = get_i64le(buf + 20);
    *out_size   = get_u32le(buf + 28);
    return TSDB_OK;
}

/*
 * IdxHeader v2 layout (36 bytes, little-endian):
 *   [0..3]    magic        u32 = TSDB_IDX_MAGIC
 *   [4..7]    count        u32
 *   [8..9]    version      u16 = 2
 *   [10..11]  _pad         u16
 *   [12..19]  total_rows   u64
 *   [20..27]  file_ts_min  i64   (file-level zone map — new in v2)
 *   [28..35]  file_ts_max  i64
 */
static size_t write_idx_header(uint8_t *buf, uint32_t count, uint64_t total_rows,
                               int64_t file_ts_min, int64_t file_ts_max) {
    put_u32le(buf + 0,  TSDB_IDX_MAGIC);
    put_u32le(buf + 4,  count);
    put_u16le(buf + 8,  TSDB_IDX_VERSION);
    put_u16le(buf + 10, 0);
    put_u64le(buf + 12, total_rows);
    put_i64le(buf + 20, file_ts_min);
    put_i64le(buf + 28, file_ts_max);
    return TSDB_IDX_HEADER_SIZE;
}

/*
 * Decode an IdxHeader supporting both v1 and v2.
 * Returns the number of header bytes consumed (20 for v1, 36 for v2),
 * or -1 on corruption.
 */
static int read_idx_header(const uint8_t *buf, size_t avail,
                           uint32_t *out_count, uint16_t *out_version,
                           uint64_t *out_total_rows,
                           int64_t  *out_file_ts_min, int64_t *out_file_ts_max)
{
    if (avail < TSDB_IDX_HEADER_SIZE_V1) return -1;
    if (get_u32le(buf) != TSDB_IDX_MAGIC) return -1;
    *out_count      = get_u32le(buf + 4);
    *out_version    = get_u16le(buf + 8);
    *out_total_rows = get_u64le(buf + 12);

    if (*out_version == 1) {
        /* Zone map unknown — caller must fall back. */
        *out_file_ts_min = INT64_MAX;
        *out_file_ts_max = INT64_MIN;
        return (int)TSDB_IDX_HEADER_SIZE_V1;
    }
    if (*out_version == 2) {
        if (avail < TSDB_IDX_HEADER_SIZE) return -1;
        *out_file_ts_min = get_i64le(buf + 20);
        *out_file_ts_max = get_i64le(buf + 28);
        return (int)TSDB_IDX_HEADER_SIZE;
    }
    return -1;   /* unknown version */
}

/*
 * BlockIndexEntry layout (40 bytes, little-endian):
 *   [0..7]    offset    u64
 *   [8..11]   size      u32
 *   [12..15]  count     u32
 *   [16..23]  ts_min    i64
 *   [24..31]  ts_max    i64
 *   [32..39]  _reserved u64
 */
static size_t write_idx_entry(uint8_t *buf,
                              uint64_t offset, uint32_t size, uint32_t count,
                              int64_t ts_min, int64_t ts_max,
                              uint64_t bloom)
{
    put_u64le(buf + 0,  offset);
    put_u32le(buf + 8,  size);
    put_u32le(buf + 12, count);
    put_i64le(buf + 16, ts_min);
    put_i64le(buf + 24, ts_max);
    put_u64le(buf + 32, bloom);
    return TSDB_IDX_ENTRY_SIZE;
}

/* ---- Per-partition, per-column writer ----------------------------------- */

/*
 * Maximum compressed output per block (2× raw + overhead).
 */
#define MAX_COMPRESSED_BLOCK (TSDB_BLOCK_POINTS * 8 * 2 + 256)

typedef struct {
    FILE    *col_fp;
    uint64_t col_offset;
    uint32_t block_count;
    uint64_t total_rows;

    /* File-level zone map accumulated during this flush session.
     * Seeded from existing v2 idx header if present, else from
     * per-block metadata in the existing v1 idx. */
    int64_t  file_ts_min;
    int64_t  file_ts_max;
    int      has_zone;      /* 1 if file_ts_min/max are initialized */

    uint8_t *idx_entries;  /* accumulated new entries for this flush session */
    size_t   idx_cap;
    size_t   idx_n;

    char     idx_path[4096];

    /* Raw-block hook (optional, set by tsdb_part_flush_ex). */
    part_raw_block_fn_t  raw_block_fn;
    void                *raw_block_ud;
    struct tsdb_db      *raw_block_db;
    const char          *raw_block_table;
    uint32_t             raw_block_day;   /* YYYYMMDD or YYYYMMDDHH packed */
    int                  col_idx;
} col_writer_t;

static int col_writer_open(col_writer_t *w, const char *part_dir,
                           const char *col_name)
{
    memset(w, 0, sizeof(*w));
    char col_path[4096];

    snprintf(col_path,    sizeof(col_path),    "%s/%s.col", part_dir, col_name);
    snprintf(w->idx_path, sizeof(w->idx_path), "%s/%s.idx", part_dir, col_name);

    /* Open .col for appending. */
    w->col_fp = fopen(col_path, "ab");
    if (!w->col_fp) return TSDB_ERR_IO;

    /* On HDD, coalesce write syscalls with a larger stdio buffer.  Each
     * block-header+payload is typically 10–100 KiB; a 256 KiB buffer
     * flushes 2–25 blocks per write(), amortising seek + metadata cost.
     * On SSD, the default 4–8 KiB buffer is fine. */
    size_t wbuf = tsdb_iopolicy_write_buf_bytes(
        tsdb_iopolicy_detect(part_dir));
    if (wbuf > 0) {
        /* setvbuf may fail silently if called after I/O; we've only just
         * opened, so this is the legal moment.  Failure is non-fatal —
         * the stream stays on its default buffering. */
        (void)setvbuf(w->col_fp, NULL, _IOFBF, wbuf);
    }

    fseek(w->col_fp, 0, SEEK_END);
    w->col_offset = (uint64_t)ftell(w->col_fp);

    /* Read existing idx header to prime block_count, total_rows, and the
     * file-level zone map (v2 only). */
    w->has_zone    = 0;
    w->file_ts_min = INT64_MAX;
    w->file_ts_max = INT64_MIN;
    {
        FILE *idx_r = fopen(w->idx_path, "rb");
        if (idx_r) {
            uint8_t hdr[TSDB_IDX_HEADER_SIZE];
            size_t n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_r);
            uint32_t cnt = 0;
            uint16_t ver = 0;
            uint64_t tot = 0;
            int64_t  fmn = INT64_MAX, fmx = INT64_MIN;
            int hsz = read_idx_header(hdr, n, &cnt, &ver, &tot, &fmn, &fmx);
            if (hsz > 0) {
                w->block_count = cnt;
                w->total_rows  = tot;
                if (ver >= 2) {
                    w->file_ts_min = fmn;
                    w->file_ts_max = fmx;
                    w->has_zone    = (cnt > 0);
                } else if (cnt > 0) {
                    /* v1 idx: derive zone from per-block entries. */
                    if (fseek(idx_r, (long)hsz, SEEK_SET) == 0) {
                        for (uint32_t b = 0; b < cnt; b++) {
                            uint8_t e[TSDB_IDX_ENTRY_SIZE];
                            if (fread(e, 1, TSDB_IDX_ENTRY_SIZE, idx_r)
                                != TSDB_IDX_ENTRY_SIZE) break;
                            int64_t mn = get_i64le(e + 16);
                            int64_t mx = get_i64le(e + 24);
                            if (mn < w->file_ts_min) w->file_ts_min = mn;
                            if (mx > w->file_ts_max) w->file_ts_max = mx;
                        }
                        w->has_zone = 1;
                    }
                }
            }
            fclose(idx_r);
        }
    }

    w->idx_cap = 16;
    w->idx_entries = malloc(w->idx_cap * TSDB_IDX_ENTRY_SIZE);
    if (!w->idx_entries) { fclose(w->col_fp); return TSDB_ERR_NOMEM; }
    return TSDB_OK;
}

static int col_writer_write_block(col_writer_t *w,
                                  tsdb_type_t type,
                                  const void *raw_vals, size_t count,
                                  int64_t ts_min, int64_t ts_max)
{
    uint8_t *comp_buf = malloc(MAX_COMPRESSED_BLOCK);
    if (!comp_buf) return TSDB_ERR_NOMEM;

    tsdb_codec_t codec_used = TSDB_CODEC_NONE;
    uint16_t     blk_flags  = 0;
    int comp_bytes = tsdb_codec_encode_adaptive(type, raw_vals, count,
                                                comp_buf, MAX_COMPRESSED_BLOCK,
                                                &codec_used, &blk_flags);
    if (comp_bytes < 0) { free(comp_buf); return TSDB_ERR_INTERNAL; }

    /* For SYMBOL columns: build a 64-bit Bloom filter over all codes in this
     * block. The filter lives in BlockIndexEntry._reserved (bytes 32-39). */
    uint64_t bloom = 0;
    if (type == TSDB_TYPE_SYMBOL) {
        const uint32_t *codes = (const uint32_t *)raw_vals;
        for (size_t k = 0; k < count; k++) {
            bloom = tsdb_bloom_add(bloom, codes[k]);
        }
        blk_flags |= TSDB_BF_HAS_BLOOM;
    }

    uint8_t hdr[TSDB_BLOCK_HEADER_SIZE];
    write_block_header(hdr, (uint8_t)codec_used, blk_flags,
                       (uint32_t)count, ts_min, ts_max,
                       (uint32_t)comp_bytes);

    if (fwrite(hdr, 1, TSDB_BLOCK_HEADER_SIZE, w->col_fp) != TSDB_BLOCK_HEADER_SIZE) {
        free(comp_buf); return TSDB_ERR_IO;
    }
    if ((size_t)comp_bytes > 0 &&
        fwrite(comp_buf, 1, (size_t)comp_bytes, w->col_fp) != (size_t)comp_bytes) {
        free(comp_buf); return TSDB_ERR_IO;
    }

    /* Raw-block replication hook: fire BEFORE freeing comp_buf. */
    if (w->raw_block_fn) {
        tsdb_block_meta_t meta;
        meta.offset = w->col_offset;  /* offset of this block's header */
        meta.size   = (uint32_t)comp_bytes;
        meta.count  = (uint32_t)count;
        meta.ts_min = ts_min;
        meta.ts_max = ts_max;
        meta.codec  = (uint8_t)codec_used;
        meta.flags  = blk_flags;
        w->raw_block_fn(w->raw_block_ud, w->raw_block_db,
                        w->raw_block_table, w->raw_block_day,
                        (uint16_t)w->col_idx, &meta,
                        comp_buf, (size_t)comp_bytes);
        /* Errors are intentionally ignored here — local write must proceed. */
    }

    free(comp_buf);

    /* Grow idx_entries buffer if needed. */
    if (w->idx_n >= w->idx_cap) {
        size_t nc = w->idx_cap * 2;
        uint8_t *nb = realloc(w->idx_entries, nc * TSDB_IDX_ENTRY_SIZE);
        if (!nb) return TSDB_ERR_NOMEM;
        w->idx_entries = nb;
        w->idx_cap = nc;
    }
    uint8_t *entry = w->idx_entries + w->idx_n * TSDB_IDX_ENTRY_SIZE;
    write_idx_entry(entry, w->col_offset, (uint32_t)comp_bytes,
                    (uint32_t)count, ts_min, ts_max, bloom);
    w->idx_n++;
    w->col_offset  += TSDB_BLOCK_HEADER_SIZE + (uint64_t)comp_bytes;
    w->total_rows  += count;
    w->block_count++;

    /* Extend file-level zone map. */
    if (!w->has_zone) {
        w->file_ts_min = ts_min;
        w->file_ts_max = ts_max;
        w->has_zone = 1;
    } else {
        if (ts_min < w->file_ts_min) w->file_ts_min = ts_min;
        if (ts_max > w->file_ts_max) w->file_ts_max = ts_max;
    }
    return TSDB_OK;
}

static int col_writer_close(col_writer_t *w) {
    int rc = TSDB_OK;

    if (w->col_fp) {
        if (fflush(w->col_fp) != 0) rc = TSDB_ERR_IO;
        fclose(w->col_fp);
        w->col_fp = NULL;
    }

    if (w->idx_n > 0 && rc == TSDB_OK) {
        /* Read all existing entries from old idx (if any). Handles both
         * v1 (20-byte) and v2 (36-byte) headers transparently. */
        uint8_t *old_entries = NULL;
        uint32_t old_count   = 0;

        {
            FILE *idx_r = fopen(w->idx_path, "rb");
            if (idx_r) {
                uint8_t hdr[TSDB_IDX_HEADER_SIZE];
                size_t n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_r);
                uint32_t cnt = 0;
                uint16_t ver = 0;
                uint64_t tot = 0;
                int64_t  fmn = 0, fmx = 0;
                int hsz = read_idx_header(hdr, n, &cnt, &ver, &tot, &fmn, &fmx);
                if (hsz > 0) {
                    old_count = cnt;
                    if (old_count > 0) {
                        /* Seek past whatever header size the old file has. */
                        if (fseek(idx_r, (long)hsz, SEEK_SET) == 0) {
                            old_entries = malloc((size_t)old_count * TSDB_IDX_ENTRY_SIZE);
                            if (old_entries) {
                                if (fread(old_entries, 1,
                                          (size_t)old_count * TSDB_IDX_ENTRY_SIZE, idx_r)
                                    != (size_t)old_count * TSDB_IDX_ENTRY_SIZE) {
                                    free(old_entries);
                                    old_entries = NULL;
                                    old_count   = 0;
                                }
                            }
                        }
                    }
                }
                fclose(idx_r);
            }
        }

        /* Rewrite idx atomically (always in v2 format). */
        FILE *idx_w = fopen(w->idx_path, "wb");
        if (!idx_w) {
            free(old_entries);
            rc = TSDB_ERR_IO;
        } else {
            uint32_t total_count = old_count + (uint32_t)w->idx_n;
            int64_t  fmn = w->has_zone ? w->file_ts_min : 0;
            int64_t  fmx = w->has_zone ? w->file_ts_max : 0;
            uint8_t  hdr[TSDB_IDX_HEADER_SIZE];
            write_idx_header(hdr, total_count, w->total_rows, fmn, fmx);
            fwrite(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_w);
            if (old_count > 0 && old_entries) {
                fwrite(old_entries, 1, (size_t)old_count * TSDB_IDX_ENTRY_SIZE, idx_w);
            }
            fwrite(w->idx_entries, 1, w->idx_n * TSDB_IDX_ENTRY_SIZE, idx_w);
            fflush(idx_w);
            fclose(idx_w);
            free(old_entries);
        }
    }

    free(w->idx_entries);
    w->idx_entries = NULL;
    return rc;
}

/* ---- tsdb_part_flush / tsdb_part_flush_ex --------------------------------- */

int tsdb_part_flush(tsdb_schema_t *s, tsdb_memtable_t *m) {
    return tsdb_part_flush_ex(s, m, NULL, NULL);
}

int tsdb_part_flush_ex(tsdb_schema_t *s, tsdb_memtable_t *m,
                       struct tsdb_db *db, const char *table_name)
{
    if (!s || !m) return TSDB_ERR_INVAL;

    /* Retrieve raw-block hook (may be NULL for standalone mode). */
    part_raw_block_fn_t raw_fn = NULL;
    void *raw_ud = NULL;
    if (db) {
        void *fn_ptr = NULL;
        tsdb_db_get_raw_block_hook(db, &fn_ptr, &raw_ud);
        __builtin_memcpy(&raw_fn, &fn_ptr, sizeof(raw_fn));
    }

    size_t nrows = tsdb_memtable_rows(m);
    if (nrows == 0) return TSDB_OK;

    const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, s->ts_col_idx);
    if (!ts_buf) return TSDB_ERR_INTERNAL;

    /*
     * Collect unique day indices from the memtable.
     * With TSDB_BLOCK_POINTS=8192 max rows we only expect 1-2 days.
     * Dynamic array handles edge cases gracefully.
     */
    int64_t *days = NULL;
    int ndays = 0, days_cap = 4;
    days = malloc((size_t)days_cap * sizeof(int64_t));
    if (!days) return TSDB_ERR_NOMEM;

    const tsdb_partition_unit_t part_unit = s->partition_unit;

    for (size_t r = 0; r < nrows; r++) {
        int64_t bucket = ts_part_index(ts_buf[r], part_unit);
        int found = 0;
        for (int d = 0; d < ndays; d++) {
            if (days[d] == bucket) { found = 1; break; }
        }
        if (!found) {
            if (ndays >= days_cap) {
                days_cap *= 2;
                int64_t *nb = realloc(days, (size_t)days_cap * sizeof(int64_t));
                if (!nb) { free(days); return TSDB_ERR_NOMEM; }
                days = nb;
            }
            days[ndays++] = bucket;
        }
    }

    for (int d = 0; d < ndays; d++) {
        int64_t bucket = days[d];

        /* Collect row indices for this partition bucket. */
        size_t *row_idx = malloc(nrows * sizeof(size_t));
        if (!row_idx) { free(days); return TSDB_ERR_NOMEM; }
        size_t day_nrows = 0;
        for (size_t r = 0; r < nrows; r++) {
            if (ts_part_index(ts_buf[r], part_unit) == bucket) row_idx[day_nrows++] = r;
        }
        if (day_nrows == 0) { free(row_idx); continue; }

        /* Build partition directory name (DAY: 8 chars, HOUR: 10 chars). */
        char part_name[11];
        ts_to_part_str(ts_buf[row_idx[0]], part_unit, part_name);

        /* Pack partition identifier for the cluster raw-block hook. */
        uint32_t part_day_int = part_str_to_int(part_name, part_unit);

        char part_dir[4096];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", s->dir, part_name);
        if (tsdb_mkdir_p(part_dir) < 0) { free(row_idx); free(days); return TSDB_ERR_IO; }

        /* Write each column. */
        for (int ci = 0; ci < s->ncols; ci++) {
            tsdb_type_t   type  = s->cols[ci].type;
            size_t        width = tsdb_type_width(type);
            const uint8_t *col_buf = (const uint8_t *)tsdb_memtable_col(m, ci);
            if (!col_buf) { free(row_idx); free(days); return TSDB_ERR_INTERNAL; }

            col_writer_t w;
            int rc = col_writer_open(&w, part_dir, s->cols[ci].name);
            if (rc != TSDB_OK) { free(row_idx); free(days); return rc; }

            /* Wire up raw-block hook if available. */
            w.raw_block_fn    = raw_fn;
            w.raw_block_ud    = raw_ud;
            w.raw_block_db    = db;
            w.raw_block_table = table_name;
            w.raw_block_day   = part_day_int;
            w.col_idx         = ci;

            /* Write in chunks of TSDB_BLOCK_POINTS. */
            size_t base = 0;
            while (base < day_nrows) {
                size_t chunk = day_nrows - base;
                if (chunk > TSDB_BLOCK_POINTS) chunk = TSDB_BLOCK_POINTS;

                uint8_t *chunk_buf = malloc(chunk * width);
                if (!chunk_buf) {
                    col_writer_close(&w);
                    free(row_idx); free(days);
                    return TSDB_ERR_NOMEM;
                }

                /* Extract rows and compute ts_min/ts_max. */
                int64_t ts_min = INT64_MAX, ts_max = INT64_MIN;
                const int64_t *ts_col_data = (const int64_t *)tsdb_memtable_col(m, s->ts_col_idx);
                for (size_t k = 0; k < chunk; k++) {
                    size_t ri = row_idx[base + k];
                    memcpy(chunk_buf + k * width, col_buf + ri * width, width);
                    int64_t t = ts_col_data[ri];
                    if (t < ts_min) ts_min = t;
                    if (t > ts_max) ts_max = t;
                }
                /* For the timestamp column itself, use the actual values. */
                if (type == TSDB_TYPE_TIMESTAMP) {
                    ts_min = ((const int64_t *)chunk_buf)[0];
                    ts_max = ts_min;
                    for (size_t k = 0; k < chunk; k++) {
                        int64_t t = ((const int64_t *)chunk_buf)[k];
                        if (t < ts_min) ts_min = t;
                        if (t > ts_max) ts_max = t;
                    }
                }

                rc = col_writer_write_block(&w, type, chunk_buf, chunk,
                                            ts_min, ts_max);
                free(chunk_buf);
                if (rc != TSDB_OK) {
                    col_writer_close(&w);
                    free(row_idx); free(days);
                    return rc;
                }
                base += chunk;
            }

            rc = col_writer_close(&w);
            if (rc != TSDB_OK) { free(row_idx); free(days); return rc; }
        }
        free(row_idx);
    }
    free(days);
    return TSDB_OK;
}

/* ---- tsdb_part_t (read side) ------------------------------------------- */

struct tsdb_part {
    tsdb_schema_t *schema;
    char           dir[4096];

    struct {
        int      fd;
        uint8_t *map;
        size_t   map_size;
    } col_maps[TSDB_MAX_COLS];

    tsdb_block_meta_t *col_metas[TSDB_MAX_COLS];
    size_t             col_meta_n[TSDB_MAX_COLS];

    /* File-level zone map aggregated across all columns of this partition.
     * Populated on open from v2 idx headers; falls back to per-block
     * ts column metadata for v1 idx files. */
    int64_t            zone_ts_min;
    int64_t            zone_ts_max;
    int                zone_valid;
};

int tsdb_part_open(tsdb_schema_t *s, const char *partition_dir, tsdb_part_t **out) {
    if (!s || !partition_dir || !out) return TSDB_ERR_INVAL;

    tsdb_part_t *p = calloc(1, sizeof(*p));
    if (!p) return TSDB_ERR_NOMEM;

    p->schema = s;
    snprintf(p->dir, sizeof(p->dir), "%s", partition_dir);
    p->zone_ts_min = INT64_MAX;
    p->zone_ts_max = INT64_MIN;
    p->zone_valid  = 0;

    for (int i = 0; i < s->ncols; i++)
        p->col_maps[i].fd = -1;

    for (int ci = 0; ci < s->ncols; ci++) {
        char col_path[4096];
        snprintf(col_path, sizeof(col_path), "%s/%s.col",
                 partition_dir, s->cols[ci].name);

        int fd = open(col_path, O_RDONLY);
        if (fd < 0) continue;

        struct stat st;
        if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); continue; }

        void *map = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (map == MAP_FAILED) { close(fd); continue; }

        /* Hint the kernel about expected access pattern.  Forward scan +
         * block-skip benefits from SEQUENTIAL on HDD (aggressive readahead
         * + early page eviction) and WILLNEED on SSD (warm page cache
         * without forward prefetch). */
        tsdb_iopolicy_advise_read(tsdb_iopolicy_detect(partition_dir),
                                  map, (size_t)st.st_size);

        p->col_maps[ci].fd       = fd;
        p->col_maps[ci].map      = (uint8_t *)map;
        p->col_maps[ci].map_size = (size_t)st.st_size;

        /* Read idx. */
        char idx_path[4096];
        snprintf(idx_path, sizeof(idx_path), "%s/%s.idx",
                 partition_dir, s->cols[ci].name);

        FILE *idx_f = fopen(idx_path, "rb");
        if (!idx_f) continue;

        uint8_t hdr[TSDB_IDX_HEADER_SIZE];
        size_t hdr_n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_f);
        uint32_t block_count  = 0;
        uint16_t idx_version  = 0;
        uint64_t total_rows_u = 0;
        int64_t  fmn = 0, fmx = 0;
        int      hdr_size = read_idx_header(hdr, hdr_n,
                                            &block_count, &idx_version,
                                            &total_rows_u, &fmn, &fmx);
        if (hdr_size < 0) { fclose(idx_f); continue; }
        if (block_count == 0) { fclose(idx_f); continue; }

        /* Seek past whichever header size the file actually uses. */
        if (fseek(idx_f, (long)hdr_size, SEEK_SET) != 0) {
            fclose(idx_f); continue;
        }

        /* Merge v2 file-level zone map into the per-partition aggregate.
         * All columns cover the same rows (and thus same row timestamps),
         * so taking the union is safe and idempotent. */
        if (idx_version >= 2) {
            if (fmn < p->zone_ts_min) p->zone_ts_min = fmn;
            if (fmx > p->zone_ts_max) p->zone_ts_max = fmx;
            p->zone_valid = 1;
        }

        p->col_metas[ci] = malloc((size_t)block_count * sizeof(tsdb_block_meta_t));
        if (!p->col_metas[ci]) { fclose(idx_f); tsdb_part_close(p); return TSDB_ERR_NOMEM; }

        for (uint32_t b = 0; b < block_count; b++) {
            uint8_t entry[TSDB_IDX_ENTRY_SIZE];
            if (fread(entry, 1, TSDB_IDX_ENTRY_SIZE, idx_f) != TSDB_IDX_ENTRY_SIZE) break;
            p->col_metas[ci][b].offset = get_u64le(entry + 0);
            p->col_metas[ci][b].size   = get_u32le(entry + 8);
            p->col_metas[ci][b].count  = get_u32le(entry + 12);
            p->col_metas[ci][b].ts_min = get_i64le(entry + 16);
            p->col_metas[ci][b].ts_max = get_i64le(entry + 24);
            p->col_metas[ci][b].bloom  = get_u64le(entry + 32);
            p->col_metas[ci][b].codec  = TSDB_CODEC_NONE;
            p->col_metas[ci][b].flags  = 0;
            p->col_meta_n[ci]++;
        }
        fclose(idx_f);

        /* Back-fill codec from block headers. */
        for (size_t b = 0; b < p->col_meta_n[ci]; b++) {
            uint64_t off = p->col_metas[ci][b].offset;
            if (off + TSDB_BLOCK_HEADER_SIZE <= p->col_maps[ci].map_size) {
                uint8_t  codec = 0;
                uint16_t flags = 0;
                uint32_t count = 0, dsz = 0;
                int64_t  tmin = 0, tmax = 0;
                if (read_block_header(p->col_maps[ci].map + off,
                                      &codec, &flags, &count,
                                      &tmin, &tmax, &dsz) == TSDB_OK) {
                    p->col_metas[ci][b].codec = codec;
                    p->col_metas[ci][b].flags = flags;
                }
            }
        }
    }

    /* ─── ALTER TABLE ADD COLUMN support ───────────────────────────────────
     * Columns added after earlier flushes have fewer (or zero) blocks than
     * the TS column for this partition. Pad the front with synthetic
     * block-meta records so readers that walk TS-aligned blocks always
     * find a matching entry. Sentinel: offset=0, codec=TSDB_CODEC_NONE,
     * no col file mapped for the synthesised block range.
     * tsdb_part_read_block zero-fills when it sees the sentinel. */
    int ts_ci = s->ts_col_idx;
    if (ts_ci >= 0 && ts_ci < s->ncols && p->col_meta_n[ts_ci] > 0) {
        size_t nb_ts = p->col_meta_n[ts_ci];
        for (int ci = 0; ci < s->ncols; ci++) {
            if (ci == ts_ci)                  continue;
            size_t nb_col = p->col_meta_n[ci];
            if (nb_col >= nb_ts)              continue;   /* already aligned */

            size_t nmiss = nb_ts - nb_col;
            tsdb_block_meta_t *merged = malloc(nb_ts * sizeof(*merged));
            if (!merged) { tsdb_part_close(p); return TSDB_ERR_NOMEM; }

            /* Synthesise the leading blocks that pre-date the column. */
            for (size_t b = 0; b < nmiss; b++) {
                memset(&merged[b], 0, sizeof(merged[b]));
                merged[b].offset = UINT64_MAX;                /* sentinel */
                merged[b].count  = p->col_metas[ts_ci][b].count;
                merged[b].ts_min = p->col_metas[ts_ci][b].ts_min;
                merged[b].ts_max = p->col_metas[ts_ci][b].ts_max;
                merged[b].codec  = TSDB_CODEC_NONE;
            }
            /* Append existing real blocks after the synthetic prefix. */
            if (nb_col > 0 && p->col_metas[ci]) {
                memcpy(&merged[nmiss], p->col_metas[ci],
                       nb_col * sizeof(*merged));
                free(p->col_metas[ci]);
            }
            p->col_metas[ci]  = merged;
            p->col_meta_n[ci] = nb_ts;
        }
    }

    *out = p;
    return TSDB_OK;
}

void tsdb_part_close(tsdb_part_t *p) {
    if (!p) return;
    for (int i = 0; i < p->schema->ncols; i++) {
        if (p->col_maps[i].map) munmap(p->col_maps[i].map, p->col_maps[i].map_size);
        if (p->col_maps[i].fd >= 0) close(p->col_maps[i].fd);
        free(p->col_metas[i]);
    }
    free(p);
}

int tsdb_part_col_blocks(tsdb_part_t *p, int col_idx,
                         tsdb_block_meta_t **out_arr, size_t *out_n)
{
    if (!p || col_idx < 0 || col_idx >= p->schema->ncols || !out_arr || !out_n)
        return TSDB_ERR_INVAL;

    size_t n = p->col_meta_n[col_idx];
    *out_n = n;
    if (n == 0) { *out_arr = NULL; return TSDB_OK; }

    *out_arr = malloc(n * sizeof(tsdb_block_meta_t));
    if (!*out_arr) return TSDB_ERR_NOMEM;
    memcpy(*out_arr, p->col_metas[col_idx], n * sizeof(tsdb_block_meta_t));
    return TSDB_OK;
}

int tsdb_part_read_block(tsdb_part_t *p, int col_idx,
                         const tsdb_block_meta_t *meta, void *out_buf)
{
    if (!p || col_idx < 0 || col_idx >= p->schema->ncols || !meta || !out_buf)
        return TSDB_ERR_INVAL;

    /* ALTER TABLE ADD COLUMN sentinel: offset == UINT64_MAX marks a block
     * that pre-dates the column's creation — zero-fill with the type's
     * default value. Works whether the column file is mapped or not. */
    if (meta->offset == UINT64_MAX) {
        tsdb_type_t type = p->schema->cols[col_idx].type;
        size_t w = tsdb_type_width(type);
        memset(out_buf, 0, (size_t)meta->count * w);
        return TSDB_OK;
    }

    if (!p->col_maps[col_idx].map) return TSDB_ERR_NOTFOUND;

    uint64_t off    = meta->offset;
    size_t   map_sz = p->col_maps[col_idx].map_size;

    if (off + TSDB_BLOCK_HEADER_SIZE > map_sz) return TSDB_ERR_CORRUPT;

    const uint8_t *hdr_ptr = p->col_maps[col_idx].map + off;
    uint8_t  codec = 0;
    uint16_t flags = 0;
    uint32_t count = 0, data_size = 0;
    int64_t  ts_min = 0, ts_max = 0;
    int rc = read_block_header(hdr_ptr, &codec, &flags, &count,
                               &ts_min, &ts_max, &data_size);
    (void)ts_min; (void)ts_max;
    if (rc != TSDB_OK) return rc;

    if (off + TSDB_BLOCK_HEADER_SIZE + data_size > map_sz) return TSDB_ERR_CORRUPT;

    const uint8_t *data_ptr = hdr_ptr + TSDB_BLOCK_HEADER_SIZE;
    tsdb_type_t type = p->schema->cols[col_idx].type;

    return tsdb_codec_decode_adaptive((tsdb_codec_t)codec, type, flags,
                                      data_ptr, data_size,
                                      out_buf, count);
}

void tsdb_part_col_map(const tsdb_part_t *p, int col_idx,
                        const uint8_t **out_map, size_t *out_len)
{
    if (!p || !out_map || !out_len) return;
    *out_map = NULL;
    *out_len = 0;
    if (col_idx < 0 || col_idx >= p->schema->ncols) return;
    *out_map = p->col_maps[col_idx].map;
    *out_len = p->col_maps[col_idx].map_size;
}

/* Schema accessor for the Parquet exporter (see src/storage/parquet.c). */
tsdb_schema_t *tsdb_part_schema(const tsdb_part_t *p) {
    return p ? p->schema : NULL;
}

int tsdb_part_zone_map(tsdb_part_t *p, int64_t *out_ts_min, int64_t *out_ts_max) {
    if (!p || !out_ts_min || !out_ts_max) return TSDB_ERR_INVAL;

    if (p->zone_valid) {
        *out_ts_min = p->zone_ts_min;
        *out_ts_max = p->zone_ts_max;
        return TSDB_OK;
    }

    /* v1-idx fallback: compute from the ts column's per-block metadata. */
    int ts_col = p->schema->ts_col_idx;
    if (ts_col < 0 || ts_col >= p->schema->ncols) return TSDB_ERR_NOTFOUND;
    if (p->col_meta_n[ts_col] == 0) return TSDB_ERR_NOTFOUND;

    int64_t mn = INT64_MAX, mx = INT64_MIN;
    for (size_t b = 0; b < p->col_meta_n[ts_col]; b++) {
        if (p->col_metas[ts_col][b].ts_min < mn) mn = p->col_metas[ts_col][b].ts_min;
        if (p->col_metas[ts_col][b].ts_max > mx) mx = p->col_metas[ts_col][b].ts_max;
    }
    /* Memoize for subsequent calls. */
    p->zone_ts_min = mn;
    p->zone_ts_max = mx;
    p->zone_valid  = 1;

    *out_ts_min = mn;
    *out_ts_max = mx;
    return TSDB_OK;
}
