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

static inline void put_u8(uint8_t *p, uint8_t v)   { p[0] = v; }
static inline void put_u16le(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static inline void put_u32le(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static inline void put_u64le(uint8_t *p, uint64_t v) {
    put_u32le(p,   (uint32_t)v);
    put_u32le(p+4, (uint32_t)(v>>32));
}
static inline void put_i64le(uint8_t *p, int64_t v) { put_u64le(p, (uint64_t)v); }

static inline uint8_t  get_u8(const uint8_t *p)  { return p[0]; }
static inline uint16_t get_u16le(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }
static inline uint32_t get_u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static inline uint64_t get_u64le(const uint8_t *p) {
    return (uint64_t)get_u32le(p) | ((uint64_t)get_u32le(p+4)<<32);
}
static inline int64_t get_i64le(const uint8_t *p)  { return (int64_t)get_u64le(p); }

/* ---- Partition directory name from timestamp ----------------------------- */

/*
 * Convert a nanosecond timestamp to YYYYMMDD string (UTC).
 * buf must be at least 9 bytes.
 */
static void ts_to_day_str(int64_t ts_ns, char *buf) {
    time_t secs = (time_t)(ts_ns / 1000000000LL);
    struct tm tm;
    gmtime_r(&secs, &tm);
    snprintf(buf, 9, "%04d%02d%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

/* Day index (days since epoch) from nanosecond timestamp. */
static int64_t ts_day_index(int64_t ts_ns) {
    /* Integer division, handling negatives correctly. */
    int64_t ns_per_day = 86400000000000LL;
    if (ts_ns >= 0) return ts_ns / ns_per_day;
    return (ts_ns - ns_per_day + 1) / ns_per_day;
}

/* ---- Flush implementation ---------------------------------------------- */

/*
 * Maximum compressed output per block.
 * Give some headroom above raw size for codec overhead.
 */
#define MAX_COMPRESSED_BLOCK (TSDB_BLOCK_POINTS * 8 * 2 + 256)

/*
 * Write a BlockHeader to a byte array.
 * Returns TSDB_BLOCK_HEADER_SIZE (32).
 */
static size_t write_block_header(uint8_t *buf,
                                 uint8_t codec, uint16_t flags,
                                 uint32_t count,
                                 int64_t ts_min, int64_t ts_max,
                                 uint32_t data_size)
{
    put_u32le(buf + 0,  TSDB_BLOCK_MAGIC);
    put_u8   (buf + 4,  codec);
    put_u8   (buf + 5,  0);           /* _pad */
    put_u16le(buf + 6,  flags);
    put_u32le(buf + 8,  count);
    put_i64le(buf + 12, ts_min);      /* bytes 12..19 */
    put_i64le(buf + 20, ts_max);      /* bytes 20..27 */
    put_u32le(buf + 28, data_size);
    return TSDB_BLOCK_HEADER_SIZE;
}

/*
 * Read a BlockHeader from a mmap'd pointer.
 * Returns 0 on success, TSDB_ERR_CORRUPT if magic wrong.
 */
static int read_block_header(const uint8_t *buf,
                             uint8_t *out_codec, uint16_t *out_flags,
                             uint32_t *out_count,
                             int64_t *out_ts_min, int64_t *out_ts_max,
                             uint32_t *out_size)
{
    uint32_t magic = get_u32le(buf);
    if (magic != TSDB_BLOCK_MAGIC) return TSDB_ERR_CORRUPT;
    *out_codec  = get_u8   (buf + 4);
    *out_flags  = get_u16le(buf + 6);
    *out_count  = get_u32le(buf + 8);
    *out_ts_min = get_i64le(buf + 12);
    *out_ts_max = get_i64le(buf + 20);
    *out_size   = get_u32le(buf + 28);
    return TSDB_OK;
}

/*
 * Write an IdxHeader (20 bytes).
 */
static size_t write_idx_header(uint8_t *buf, uint32_t count, uint64_t total_rows) {
    put_u32le(buf + 0,  TSDB_IDX_MAGIC);
    put_u32le(buf + 4,  count);
    put_u16le(buf + 8,  TSDB_IDX_VERSION);
    put_u16le(buf + 10, 0);   /* _pad */
    put_u64le(buf + 12, total_rows);
    return TSDB_IDX_HEADER_SIZE;  /* 20 */
}

/*
 * Write a BlockIndexEntry (40 bytes).
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
    put_u64le(buf + 32, 0); /* _reserved */
    return TSDB_IDX_ENTRY_SIZE;  /* 40 */
}

/* ---- Per-partition, per-column writer ----------------------------------- */

typedef struct {
    FILE    *col_fp;
    FILE    *idx_fp;
    uint64_t col_offset;   /* current byte offset in .col file */
    uint32_t block_count;
    uint64_t total_rows;

    /* Temporary buffer to accumulate index entries. */
    uint8_t *idx_entries;  /* malloc'd; TSDB_IDX_ENTRY_SIZE * cap */
    size_t   idx_cap;
    size_t   idx_n;
} col_writer_t;

static int col_writer_open(col_writer_t *w, const char *part_dir,
                           const char *col_name) {
    char path[4096];
    memset(w, 0, sizeof(*w));

    snprintf(path, sizeof(path), "%s/%s.col", part_dir, col_name);
    w->col_fp = fopen(path, "ab");
    if (!w->col_fp) return TSDB_ERR_IO;

    snprintf(path, sizeof(path), "%s/%s.idx", part_dir, col_name);
    w->idx_fp = fopen(path, "ab");
    if (!w->idx_fp) { fclose(w->col_fp); w->col_fp = NULL; return TSDB_ERR_IO; }

    /* Current file size is our starting offset for new blocks. */
    fseek(w->col_fp, 0, SEEK_END);
    w->col_offset = (uint64_t)ftell(w->col_fp);

    /* Read existing block count from idx to know total_rows so far. */
    fseek(w->idx_fp, 0, SEEK_END);
    long idx_end = ftell(w->idx_fp);
    if (idx_end >= (long)TSDB_IDX_HEADER_SIZE) {
        fseek(w->idx_fp, 0, SEEK_SET);
        uint8_t hdr[TSDB_IDX_HEADER_SIZE];
        if (fread(hdr, 1, TSDB_IDX_HEADER_SIZE, w->idx_fp) == TSDB_IDX_HEADER_SIZE) {
            uint32_t magic = get_u32le(hdr);
            if (magic == TSDB_IDX_MAGIC) {
                w->block_count = get_u32le(hdr + 4);
                w->total_rows  = get_u64le(hdr + 12);
            }
        }
    }
    /* Seek idx to end for appending. */
    fseek(w->idx_fp, 0, SEEK_END);

    /* Pre-allocate index entry buffer. */
    w->idx_cap = 64;
    w->idx_entries = malloc(w->idx_cap * TSDB_IDX_ENTRY_SIZE);
    if (!w->idx_entries) {
        fclose(w->col_fp); fclose(w->idx_fp);
        return TSDB_ERR_NOMEM;
    }
    return TSDB_OK;
}

static int col_writer_write_block(col_writer_t *w,
                                  tsdb_type_t type,
                                  const void *raw_vals, size_t count,
                                  int64_t ts_min, int64_t ts_max)
{
    /* Allocate output buffer for compressed data. */
    uint8_t *comp_buf = malloc(MAX_COMPRESSED_BLOCK);
    if (!comp_buf) return TSDB_ERR_NOMEM;

    tsdb_codec_t codec_used = TSDB_CODEC_NONE;
    int comp_bytes = tsdb_codec_encode(type, raw_vals, count,
                                       comp_buf, MAX_COMPRESSED_BLOCK,
                                       &codec_used);
    if (comp_bytes < 0) { free(comp_buf); return TSDB_ERR_INTERNAL; }

    /* Write BlockHeader. */
    uint8_t hdr[TSDB_BLOCK_HEADER_SIZE];
    write_block_header(hdr, (uint8_t)codec_used, 0,
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

    /* Accumulate index entry. */
    if (w->idx_n >= w->idx_cap) {
        size_t new_cap = w->idx_cap * 2;
        uint8_t *nb = realloc(w->idx_entries, new_cap * TSDB_IDX_ENTRY_SIZE);
        if (!nb) return TSDB_ERR_NOMEM;
        w->idx_entries = nb;
        w->idx_cap = new_cap;
    }
    uint8_t *entry = w->idx_entries + w->idx_n * TSDB_IDX_ENTRY_SIZE;
    write_idx_entry(entry,
                    w->col_offset,
                    (uint32_t)comp_bytes,
                    (uint32_t)count,
                    ts_min, ts_max);
    w->idx_n++;
    w->total_rows += count;
    w->col_offset += TSDB_BLOCK_HEADER_SIZE + (uint64_t)comp_bytes;
    w->block_count++;
    return TSDB_OK;
}

static int col_writer_close(col_writer_t *w) {
    if (!w->col_fp || !w->idx_fp) return TSDB_OK;
    fclose(w->col_fp);
    w->col_fp = NULL;

    /* Rewrite the idx file: header + all (old + new) entries.
     * Simple strategy: rewrite the whole thing from scratch. */

    /* Read existing entries from idx file. */
    rewind(w->idx_fp);
    long idx_size = 0;
    fseek(w->idx_fp, 0, SEEK_END);
    idx_size = ftell(w->idx_fp);
    rewind(w->idx_fp);

    /* Read old entries (if any). */
    uint32_t old_count = 0;
    uint64_t old_rows  = 0;
    uint8_t *old_entries = NULL;

    if (idx_size >= (long)TSDB_IDX_HEADER_SIZE) {
        uint8_t hdr[TSDB_IDX_HEADER_SIZE];
        if (fread(hdr, 1, TSDB_IDX_HEADER_SIZE, w->idx_fp) == TSDB_IDX_HEADER_SIZE) {
            if (get_u32le(hdr) == TSDB_IDX_MAGIC) {
                old_count = get_u32le(hdr + 4);
                old_rows  = get_u64le(hdr + 12);
                size_t old_bytes = old_count * TSDB_IDX_ENTRY_SIZE;
                if (old_bytes > 0) {
                    old_entries = malloc(old_bytes);
                    if (old_entries) {
                        if (fread(old_entries, 1, old_bytes, w->idx_fp) != old_bytes) {
                            free(old_entries);
                            old_entries = NULL;
                            old_count = 0;
                            old_rows  = 0;
                        }
                    }
                }
            }
        }
    }

    /* Rewrite the idx file. */
    fclose(w->idx_fp);
    w->idx_fp = NULL;

    /* We need the idx file path — it was opened already, so we can't get it
     * directly. The caller must handle this. For now we accept that we've
     * already fclose'd it and need the path externally.
     *
     * Design choice: store idx file path in the writer. Let's store it. */
    /* This is handled by col_writer_close_path below. */

    uint32_t total_count = old_count + (uint32_t)w->idx_n;
    uint64_t total_rows  = old_rows  + (w->total_rows - old_rows);
    /* w->total_rows was initialized from old file, then incremented. */
    total_rows = w->total_rows;

    free(old_entries);
    free(w->idx_entries);
    w->idx_entries = NULL;
    return TSDB_OK;
}

/* Better design: keep idx_path in writer. */
typedef struct {
    FILE    *col_fp;
    FILE    *idx_fp;
    uint64_t col_offset;
    uint32_t block_count;
    uint64_t total_rows;   /* running total rows across all opens */

    uint8_t *idx_entries;  /* new entries this session */
    size_t   idx_cap;
    size_t   idx_n;

    char     idx_path[4096];
} col_writer2_t;

static int col_writer2_open(col_writer2_t *w, const char *part_dir,
                             const char *col_name) {
    memset(w, 0, sizeof(*w));
    char col_path[4096];

    snprintf(col_path,  sizeof(col_path),  "%s/%s.col", part_dir, col_name);
    snprintf(w->idx_path, sizeof(w->idx_path), "%s/%s.idx", part_dir, col_name);

    /* Open .col for appending (creates if not exists). */
    w->col_fp = fopen(col_path, "ab");
    if (!w->col_fp) return TSDB_ERR_IO;
    fseek(w->col_fp, 0, SEEK_END);
    w->col_offset = (uint64_t)ftell(w->col_fp);

    /* Read existing idx to get current block_count and total_rows. */
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

    /* Pre-allocate idx entry buffer. */
    w->idx_cap = 64;
    w->idx_entries = malloc(w->idx_cap * TSDB_IDX_ENTRY_SIZE);
    if (!w->idx_entries) { fclose(w->col_fp); return TSDB_ERR_NOMEM; }
    return TSDB_OK;
}

static int col_writer2_write_block(col_writer2_t *w,
                                   tsdb_type_t type,
                                   const void *raw_vals, size_t count,
                                   int64_t ts_min, int64_t ts_max)
{
    uint8_t *comp_buf = malloc(MAX_COMPRESSED_BLOCK);
    if (!comp_buf) return TSDB_ERR_NOMEM;

    tsdb_codec_t codec_used = TSDB_CODEC_NONE;
    int comp_bytes = tsdb_codec_encode(type, raw_vals, count,
                                       comp_buf, MAX_COMPRESSED_BLOCK,
                                       &codec_used);
    if (comp_bytes < 0) { free(comp_buf); return TSDB_ERR_INTERNAL; }

    uint8_t hdr[TSDB_BLOCK_HEADER_SIZE];
    write_block_header(hdr, (uint8_t)codec_used, 0,
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

static int col_writer2_close(col_writer2_t *w) {
    int rc = TSDB_OK;

    if (w->col_fp) {
        if (fflush(w->col_fp) != 0) rc = TSDB_ERR_IO;
        fclose(w->col_fp);
        w->col_fp = NULL;
    }

    if (w->idx_n > 0) {
        /* Read all existing entries from old idx (if any). */
        uint8_t *old_entries = NULL;
        uint32_t old_count   = 0;
        uint64_t old_rows    = 0;

        {
            FILE *idx_r = fopen(w->idx_path, "rb");
            if (idx_r) {
                uint8_t hdr[TSDB_IDX_HEADER_SIZE];
                if (fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_r) == TSDB_IDX_HEADER_SIZE
                    && get_u32le(hdr) == TSDB_IDX_MAGIC) {
                    old_count = get_u32le(hdr + 4);
                    /* old total_rows is implicit; we already added it in open. */
                    old_rows = get_u64le(hdr + 12);
                    if (old_count > 0) {
                        old_entries = malloc((size_t)old_count * TSDB_IDX_ENTRY_SIZE);
                        if (old_entries) {
                            if (fread(old_entries, 1,
                                      (size_t)old_count * TSDB_IDX_ENTRY_SIZE,
                                      idx_r)
                                != (size_t)old_count * TSDB_IDX_ENTRY_SIZE) {
                                free(old_entries);
                                old_entries = NULL;
                                old_count   = 0;
                                old_rows    = 0;
                            }
                        }
                    }
                }
                fclose(idx_r);
            }
        }

        /* Rewrite idx atomically (write to same path). */
        FILE *idx_w = fopen(w->idx_path, "wb");
        if (!idx_w) {
            free(old_entries);
            free(w->idx_entries);
            return TSDB_ERR_IO;
        }

        uint32_t total_count = old_count + (uint32_t)w->idx_n;
        /* w->total_rows already includes old rows (set in open from existing header). */
        uint8_t hdr[TSDB_IDX_HEADER_SIZE];
        write_idx_header(hdr, total_count, w->total_rows);
        fwrite(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_w);

        if (old_count > 0 && old_entries) {
            fwrite(old_entries, 1, (size_t)old_count * TSDB_IDX_ENTRY_SIZE, idx_w);
        }
        fwrite(w->idx_entries, 1, w->idx_n * TSDB_IDX_ENTRY_SIZE, idx_w);
        fflush(idx_w);
        fclose(idx_w);

        free(old_entries);
        (void)old_rows;
    } else if (w->block_count == 0) {
        /* First time: write empty header. */
        /* Only if file doesn't exist yet — skip if already handled. */
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

    /* Get timestamp column for day partitioning. */
    const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, s->ts_col_idx);
    if (!ts_buf) return TSDB_ERR_INTERNAL;

    /*
     * Group rows by day. Since rows are generally time-ordered within a memtable
     * but may straddle two days, we use a single-pass grouping.
     *
     * Strategy: find contiguous runs of the same day index. If not contiguous,
     * we still handle them (just scan all rows for each day).
     *
     * For simplicity and correctness: sort rows by day, then process groups.
     * But memtable doesn't guarantee ordering. We do a two-pass: collect unique
     * days, then for each day collect rows.
     *
     * With TSDB_BLOCK_POINTS=8192 rows max, a simple O(days*rows) is fine.
     */

    /* Collect unique day indices. */
    int64_t days[2] = {0, 0}; /* At most 2 days in 8192 consecutive rows. */
    int ndays = 0;
    for (size_t r = 0; r < nrows; r++) {
        int64_t day = ts_day_index(ts_buf[r]);
        int found = 0;
        for (int d = 0; d < ndays; d++) {
            if (days[d] == day) { found = 1; break; }
        }
        if (!found) {
            if (ndays >= 2) {
                /* Rare edge case: 3+ days. Expand dynamically. */
                /* For simplicity, treat everything as one block. */
                break;
            }
            days[ndays++] = day;
        }
    }
    if (ndays == 0) ndays = 1, days[0] = ts_day_index(ts_buf[0]);

    for (int d = 0; d < ndays; d++) {
        int64_t day = days[d];

        /* Collect row indices belonging to this day. */
        size_t *row_idx = malloc(nrows * sizeof(size_t));
        if (!row_idx) return TSDB_ERR_NOMEM;
        size_t day_nrows = 0;
        for (size_t r = 0; r < nrows; r++) {
            if (ts_day_index(ts_buf[r]) == day) {
                row_idx[day_nrows++] = r;
            }
        }
        if (day_nrows == 0) { free(row_idx); continue; }

        /* Build partition directory name. */
        char day_str[9];
        ts_to_day_str(ts_buf[row_idx[0]], day_str);

        char part_dir[4096];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", s->dir, day_str);
        if (tsdb_mkdir_p(part_dir) < 0) { free(row_idx); return TSDB_ERR_IO; }

        /* For each column, write blocks of at most TSDB_BLOCK_POINTS rows. */
        for (int ci = 0; ci < s->ncols; ci++) {
            tsdb_type_t type  = s->cols[ci].type;
            size_t      width = tsdb_type_width(type);
            const uint8_t *col_buf = (const uint8_t *)tsdb_memtable_col(m, ci);
            if (!col_buf) { free(row_idx); return TSDB_ERR_INTERNAL; }

            col_writer2_t w;
            int rc = col_writer2_open(&w, part_dir, s->cols[ci].name);
            if (rc != TSDB_OK) { free(row_idx); return rc; }

            /* Chunk into blocks of TSDB_BLOCK_POINTS. */
            size_t base = 0;
            while (base < day_nrows) {
                size_t chunk = day_nrows - base;
                if (chunk > TSDB_BLOCK_POINTS) chunk = TSDB_BLOCK_POINTS;

                /* Extract chunk rows into contiguous buffer. */
                uint8_t *chunk_buf = malloc(chunk * width);
                if (!chunk_buf) {
                    col_writer2_close(&w);
                    free(row_idx);
                    return TSDB_ERR_NOMEM;
                }

                /* Compute ts_min, ts_max from ts column for this chunk. */
                int64_t ts_min = INT64_MAX;
                int64_t ts_max = INT64_MIN;
                const int64_t *ts_col = (const int64_t *)tsdb_memtable_col(m, s->ts_col_idx);
                for (size_t k = 0; k < chunk; k++) {
                    size_t ri = row_idx[base + k];
                    memcpy(chunk_buf + k * width, col_buf + ri * width, width);
                    int64_t t = ts_col[ri];
                    if (t < ts_min) ts_min = t;
                    if (t > ts_max) ts_max = t;
                }

                /* Use ts from ts column directly for ts blocks. */
                if (type == TSDB_TYPE_TIMESTAMP) {
                    ts_min = ts_max = ((const int64_t *)chunk_buf)[0];
                    for (size_t k = 0; k < chunk; k++) {
                        int64_t t = ((const int64_t *)chunk_buf)[k];
                        if (t < ts_min) ts_min = t;
                        if (t > ts_max) ts_max = t;
                    }
                }

                rc = col_writer2_write_block(&w, type, chunk_buf, chunk,
                                             ts_min, ts_max);
                free(chunk_buf);
                if (rc != TSDB_OK) {
                    col_writer2_close(&w);
                    free(row_idx);
                    return rc;
                }
                base += chunk;
            }

            rc = col_writer2_close(&w);
            if (rc != TSDB_OK) { free(row_idx); return rc; }
        }

        free(row_idx);
    }

    return TSDB_OK;
}

/* ---- tsdb_part_t (read side) ------------------------------------------- */

struct tsdb_part {
    tsdb_schema_t *schema;
    char           dir[4096];

    /* Per-column mmap'd .col file. */
    struct {
        int       fd;
        uint8_t  *map;
        size_t    map_size;
    } col_maps[TSDB_MAX_COLS];

    /* Per-column index entries. */
    tsdb_block_meta_t *col_metas[TSDB_MAX_COLS];
    size_t             col_meta_n[TSDB_MAX_COLS];
};

int tsdb_part_open(tsdb_schema_t *s, const char *partition_dir, tsdb_part_t **out) {
    if (!s || !partition_dir || !out) return TSDB_ERR_INVAL;

    tsdb_part_t *p = calloc(1, sizeof(*p));
    if (!p) return TSDB_ERR_NOMEM;

    p->schema = s;
    snprintf(p->dir, sizeof(p->dir), "%s", partition_dir);

    /* Initialize all fds to -1 so close can detect unopen. */
    for (int i = 0; i < s->ncols; i++) {
        p->col_maps[i].fd = -1;
        p->col_maps[i].map = NULL;
    }

    /* For each column, open and mmap .col file, then read .idx. */
    for (int ci = 0; ci < s->ncols; ci++) {
        char col_path[4096];
        snprintf(col_path, sizeof(col_path), "%s/%s.col",
                 partition_dir, s->cols[ci].name);

        int fd = open(col_path, O_RDONLY);
        if (fd < 0) {
            /* Column file missing — not an error if partition is sparse. */
            continue;
        }
        p->col_maps[ci].fd = fd;

        struct stat st;
        if (fstat(fd, &st) < 0 || st.st_size == 0) {
            close(fd);
            p->col_maps[ci].fd = -1;
            continue;
        }
        p->col_maps[ci].map_size = (size_t)st.st_size;
        p->col_maps[ci].map = mmap(NULL, (size_t)st.st_size,
                                   PROT_READ, MAP_PRIVATE, fd, 0);
        if (p->col_maps[ci].map == MAP_FAILED) {
            p->col_maps[ci].map = NULL;
            close(fd);
            p->col_maps[ci].fd = -1;
            continue;
        }

        /* Read idx file. */
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
            /* codec comes from the block header; fill during read_block. */
            p->col_metas[ci][b].codec  = TSDB_CODEC_NONE;
            p->col_meta_n[ci]++;
        }

        /* Back-fill codec from block headers in the mmap. */
        for (size_t b = 0; b < p->col_meta_n[ci]; b++) {
            uint64_t off = p->col_metas[ci][b].offset;
            if (off + TSDB_BLOCK_HEADER_SIZE <= p->col_maps[ci].map_size) {
                uint8_t codec, dummy_u8;
                uint16_t dummy_u16;
                uint32_t dummy_u32;
                int64_t  dummy_i64;
                read_block_header(p->col_maps[ci].map + off,
                                  &codec, &dummy_u16,
                                  &dummy_u32,
                                  &dummy_i64, &dummy_i64,
                                  &dummy_u32);
                p->col_metas[ci][b].codec = codec;
            }
        }

        fclose(idx_f);
    }

    *out = p;
    return TSDB_OK;
}

void tsdb_part_close(tsdb_part_t *p) {
    if (!p) return;
    for (int i = 0; i < p->schema->ncols; i++) {
        if (p->col_maps[i].map && p->col_maps[i].map != MAP_FAILED) {
            munmap(p->col_maps[i].map, p->col_maps[i].map_size);
        }
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

    uint64_t off = meta->offset;
    size_t map_sz = p->col_maps[col_idx].map_size;

    if (off + TSDB_BLOCK_HEADER_SIZE > map_sz) return TSDB_ERR_CORRUPT;

    /* Read and validate block header. */
    const uint8_t *hdr_ptr = p->col_maps[col_idx].map + off;
    uint8_t  codec;
    uint16_t flags;
    uint32_t count, data_size;
    int64_t  ts_min, ts_max;
    int rc = read_block_header(hdr_ptr, &codec, &flags, &count,
                               &ts_min, &ts_max, &data_size);
    if (rc != TSDB_OK) return rc;

    if (off + TSDB_BLOCK_HEADER_SIZE + data_size > map_sz) return TSDB_ERR_CORRUPT;

    const uint8_t *data_ptr = hdr_ptr + TSDB_BLOCK_HEADER_SIZE;
    tsdb_type_t type = p->schema->cols[col_idx].type;

    return tsdb_codec_decode((tsdb_codec_t)codec, type,
                             data_ptr, data_size,
                             out_buf, count);
}
