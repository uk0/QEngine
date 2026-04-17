/* part.c — disk partition flush and read. */

#include "part.h"
#include "../compress/codec.h"
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

/* Convert a nanosecond timestamp to YYYYMMDD string (UTC). buf must be >= 9 bytes. */
static void ts_to_day_str(int64_t ts_ns, char *buf) {
    time_t secs = (time_t)(ts_ns / 1000000000LL);
    struct tm tm;
    gmtime_r(&secs, &tm);
    snprintf(buf, 9, "%04d%02d%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

/* Day index (days since epoch) from nanosecond timestamp. */
static int64_t ts_day_index(int64_t ts_ns) {
    int64_t ns_per_day = 86400000000000LL;
    if (ts_ns >= 0) return ts_ns / ns_per_day;
    return (ts_ns - ns_per_day + 1) / ns_per_day;
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
 * IdxHeader layout (20 bytes, little-endian):
 *   [0..3]    magic      u32 = TSDB_IDX_MAGIC
 *   [4..7]    count      u32
 *   [8..9]    version    u16
 *   [10..11]  _pad       u16
 *   [12..19]  total_rows u64
 */
static size_t write_idx_header(uint8_t *buf, uint32_t count, uint64_t total_rows) {
    put_u32le(buf + 0,  TSDB_IDX_MAGIC);
    put_u32le(buf + 4,  count);
    put_u16le(buf + 8,  TSDB_IDX_VERSION);
    put_u16le(buf + 10, 0);
    put_u64le(buf + 12, total_rows);
    return TSDB_IDX_HEADER_SIZE;
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
                              int64_t ts_min, int64_t ts_max)
{
    put_u64le(buf + 0,  offset);
    put_u32le(buf + 8,  size);
    put_u32le(buf + 12, count);
    put_i64le(buf + 16, ts_min);
    put_i64le(buf + 24, ts_max);
    put_u64le(buf + 32, 0);
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

    uint8_t *idx_entries;  /* accumulated new entries for this flush session */
    size_t   idx_cap;
    size_t   idx_n;

    char     idx_path[4096];
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
    fseek(w->col_fp, 0, SEEK_END);
    w->col_offset = (uint64_t)ftell(w->col_fp);

    /* Read existing idx to prime block_count and total_rows. */
    {
        FILE *idx_r = fopen(w->idx_path, "rb");
        if (idx_r) {
            uint8_t hdr[TSDB_IDX_HEADER_SIZE];
            if (fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_r) == TSDB_IDX_HEADER_SIZE
                && get_u32le(hdr) == TSDB_IDX_MAGIC) {
                w->block_count = get_u32le(hdr + 4);
                w->total_rows  = get_u64le(hdr + 12);
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
                    (uint32_t)count, ts_min, ts_max);
    w->idx_n++;
    w->col_offset  += TSDB_BLOCK_HEADER_SIZE + (uint64_t)comp_bytes;
    w->total_rows  += count;
    w->block_count++;
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
        /* Read all existing entries from old idx (if any). */
        uint8_t *old_entries = NULL;
        uint32_t old_count   = 0;

        {
            FILE *idx_r = fopen(w->idx_path, "rb");
            if (idx_r) {
                uint8_t hdr[TSDB_IDX_HEADER_SIZE];
                if (fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_r) == TSDB_IDX_HEADER_SIZE
                    && get_u32le(hdr) == TSDB_IDX_MAGIC) {
                    old_count = get_u32le(hdr + 4);
                    if (old_count > 0) {
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
                fclose(idx_r);
            }
        }

        /* Rewrite idx atomically. */
        FILE *idx_w = fopen(w->idx_path, "wb");
        if (!idx_w) {
            free(old_entries);
            rc = TSDB_ERR_IO;
        } else {
            uint32_t total_count = old_count + (uint32_t)w->idx_n;
            uint8_t  hdr[TSDB_IDX_HEADER_SIZE];
            write_idx_header(hdr, total_count, w->total_rows);
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

/* ---- tsdb_part_flush ---------------------------------------------------- */

int tsdb_part_flush(tsdb_schema_t *s, tsdb_memtable_t *m) {
    if (!s || !m) return TSDB_ERR_INVAL;

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

    for (size_t r = 0; r < nrows; r++) {
        int64_t day = ts_day_index(ts_buf[r]);
        int found = 0;
        for (int d = 0; d < ndays; d++) {
            if (days[d] == day) { found = 1; break; }
        }
        if (!found) {
            if (ndays >= days_cap) {
                days_cap *= 2;
                int64_t *nb = realloc(days, (size_t)days_cap * sizeof(int64_t));
                if (!nb) { free(days); return TSDB_ERR_NOMEM; }
                days = nb;
            }
            days[ndays++] = day;
        }
    }

    for (int d = 0; d < ndays; d++) {
        int64_t day = days[d];

        /* Collect row indices for this day. */
        size_t *row_idx = malloc(nrows * sizeof(size_t));
        if (!row_idx) { free(days); return TSDB_ERR_NOMEM; }
        size_t day_nrows = 0;
        for (size_t r = 0; r < nrows; r++) {
            if (ts_day_index(ts_buf[r]) == day) row_idx[day_nrows++] = r;
        }
        if (day_nrows == 0) { free(row_idx); continue; }

        /* Build partition directory. */
        char day_str[9];
        ts_to_day_str(ts_buf[row_idx[0]], day_str);

        char part_dir[4096];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", s->dir, day_str);
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
};

int tsdb_part_open(tsdb_schema_t *s, const char *partition_dir, tsdb_part_t **out) {
    if (!s || !partition_dir || !out) return TSDB_ERR_INVAL;

    tsdb_part_t *p = calloc(1, sizeof(*p));
    if (!p) return TSDB_ERR_NOMEM;

    p->schema = s;
    snprintf(p->dir, sizeof(p->dir), "%s", partition_dir);

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
        if (fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_f) != TSDB_IDX_HEADER_SIZE
            || get_u32le(hdr) != TSDB_IDX_MAGIC) {
            fclose(idx_f);
            continue;
        }

        uint32_t block_count = get_u32le(hdr + 4);
        if (block_count == 0) { fclose(idx_f); continue; }

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
            p->col_metas[ci][b].codec  = TSDB_CODEC_NONE;
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
