/* compaction.c — background size-tiered intra-partition block compaction.
 *
 * See compaction.h for the design narrative.
 *
 * Implementation outline
 * ──────────────────────
 * 1.  compactor_run_once_impl()
 *       for each table → for each partition dir → compact_partition()
 *
 * 2.  compact_partition(table, part_dir)
 *       for each column → compact_column_file(schema, col_idx, part_dir)
 *
 * 3.  compact_column_file(schema, col_idx, part_dir, stats)
 *       a.  Read .col completely into RAM (mmap or read)
 *       b.  Decode all blocks → one big flat array per column type
 *       c.  Re-encode in COMPACT_BLOCK_POINTS chunks → .col.tmp + .idx.tmp
 *       d.  Acquire compact_mtx for the table, rename both .tmp → live, release
 *
 * 4.  Worker threads call compactor_run_once_impl() then sleep interval_ns.
 */

#define _POSIX_C_SOURCE 200809L

#include "compaction.h"
#include "db.h"
#include "schema.h"
#include "part.h"
#include "../compress/codec.h"
#include "../core/types.h"
#include "../../include/tsdb.h"
#include "../server/metrics.h"
#include "../server/proto.h"   /* tsdb_crc32c — block trailer parity with flush path */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <math.h>
#include <time.h>

#include <pthread.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/resource.h>   /* setpriority */
#if defined(__linux__)
#include <sys/syscall.h>    /* SYS_gettid, SYS_ioprio_set */
#endif

/* ---- Serialisation helpers (little-endian) -------------------------------- */

static inline uint32_t get_u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static inline uint64_t get_u64le(const uint8_t *p) {
    return (uint64_t)get_u32le(p) | ((uint64_t)get_u32le(p+4)<<32);
}
static inline int64_t  get_i64le(const uint8_t *p)  { return (int64_t)get_u64le(p); }
static inline uint16_t get_u16le(const uint8_t *p)  { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }

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

/* ---- On-disk format constants (mirrors part.c) --------------------------- */

#define BLK_MAGIC       0x314B4C42u
#define IDX_MAGIC       0x31584449u
#define IDX_VERSION     3u
#define BLK_HDR_SZ      32u
#define IDX_HDR_SZ_V1   20u
#define IDX_HDR_SZ_V2   36u
#define IDX_HDR_SZ      40u   /* V3 */
#define IDX_ENTRY_SZ_V2 40u
#define IDX_ENTRY_SZ    88u   /* V3 (prefix 40 + stats 48) */

/* Max compressed output for one COMPACT_BLOCK_POINTS chunk. */
#define MAX_COMP_OUT  (COMPACT_BLOCK_POINTS * 8 * 2 + 512)

/* L2 cold-tier outer-lzlite gain threshold (resolved once).  Compaction
 * only runs on aged (>=60s) blocks, so we recompress at a lower wrap
 * threshold than the hot path's 16-byte floor: any positive byte gain is
 * kept, trading a hair of decode speed for tighter cold storage.  This is
 * always safe (never larger than before) and never changes the block
 * layout — only whether the lzlite wrapper is applied at the margin.
 *   TSDB_L2_COMPRESS=0   → 16 (revert to hot-path floor)
 *   TSDB_L2_MIN_GAIN=<n> → explicit floor
 *   default              → 1
 *
 * NOTE: a larger-cold-block tier (which measurements show would shrink
 * DICT/SYMBOL columns ~11x) is NOT shipped here — the read path matches
 * columns block-by-block by (ts_min,count) and assumes every column shares
 * one block size (exec.c), so a per-type or larger block would break
 * column alignment and corrupt reads.  Raising the block-size ceiling is a
 * separate, correctness-gated change. */
static int l2_min_gain(void) {
    static int cached = -1;
    if (cached >= 0) return cached;
    const char *en = getenv("TSDB_L2_COMPRESS");
    if (en && en[0] == '0') { cached = 16; return cached; }
    const char *mg = getenv("TSDB_L2_MIN_GAIN");
    if (mg && mg[0]) { int v = atoi(mg); cached = v > 0 ? v : 1; return cached; }
    cached = 1;
    return cached;
}

/* ---- Partition directory name classification ------------------------------ */

/*
 * Return 1 if the directory entry name looks like a valid partition:
 *   YYYYMMDD   (8 digits)  — DAY
 *   YYYYMMDDHH (10 digits) — HOUR
 */
static int is_partition_dir(const char *name) {
    size_t n = strlen(name);
    if (n != 8 && n != 10) return 0;
    for (size_t i = 0; i < n; i++) {
        if (name[i] < '0' || name[i] > '9') return 0;
    }
    return 1;
}

/* ---- Block I/O helpers ---------------------------------------------------- */

/*
 * Write a v3 IdxHeader into buf (exactly IDX_HDR_SZ bytes).
 * Mirrors the layout in src/storage/part.c.
 */
static void write_idx_hdr(uint8_t *buf, uint32_t nblocks, uint64_t total_rows,
                          int64_t ts_min, int64_t ts_max)
{
    memset(buf, 0, IDX_HDR_SZ);
    put_u32le(buf + 0,  IDX_MAGIC);
    put_u32le(buf + 4,  nblocks);
    put_u16le(buf + 8,  (uint16_t)IDX_VERSION);
    put_u16le(buf + 10, 0);
    put_u64le(buf + 12, total_rows);
    put_i64le(buf + 20, ts_min);
    put_i64le(buf + 28, ts_max);
    put_u16le(buf + 36, (uint16_t)IDX_ENTRY_SZ);   /* entry_size */
    put_u16le(buf + 38, 0);                         /* stats_variant */
}

/*
 * Write a BlockIndexEntry (IDX_ENTRY_SZ bytes) into buf.
 * Compaction currently does not compute per-column stats (it works on
 * raw bytes of already-compressed blocks), so the stats tail is zeroed
 * — stats_flags==0 signals absent to readers.  A future pass can fold
 * stats in during the decode/re-encode step.
 */
static void write_idx_entry(uint8_t *buf,
                            uint64_t offset, uint32_t size,
                            uint32_t count,
                            int64_t ts_min, int64_t ts_max)
{
    memset(buf, 0, IDX_ENTRY_SZ);
    put_u64le(buf + 0,  offset);
    put_u32le(buf + 8,  size);
    put_u32le(buf + 12, count);
    put_i64le(buf + 16, ts_min);
    put_i64le(buf + 24, ts_max);
    put_u64le(buf + 32, 0);
    /* bytes 40..87 already zero — stats_flags=0 means "absent" */
}

/*
 * Write a BlockHeader (BLK_HDR_SZ bytes) into buf.
 */
static void write_blk_hdr(uint8_t *buf,
                          uint8_t codec, uint16_t flags,
                          uint32_t count,
                          int64_t ts_min, int64_t ts_max,
                          uint32_t data_size)
{
    put_u32le(buf + 0,  BLK_MAGIC);
    put_u8   (buf + 4,  codec);
    put_u8   (buf + 5,  0);
    put_u16le(buf + 6,  flags);
    put_u32le(buf + 8,  count);
    put_i64le(buf + 12, ts_min);
    put_i64le(buf + 20, ts_max);
    put_u32le(buf + 28, data_size);
}

/* ---- Column-file compaction ----------------------------------------------- */

/*
 * Describes one existing block found while parsing the old .col file.
 */
typedef struct {
    uint64_t offset;    /* byte offset of BlockHeader in .col */
    uint32_t data_sz;   /* compressed data bytes (NOT including header) */
    uint32_t count;     /* number of values */
    int64_t  ts_min;
    int64_t  ts_max;
    uint8_t  codec;
    uint16_t flags;
} block_info_t;

/*
 * Fwrite wrapper: returns 0 on success, -1 on short write.
 */
static int safe_write(FILE *fp, const void *buf, size_t n) {
    if (n == 0) return 0;
    return (fwrite(buf, 1, n, fp) == n) ? 0 : -1;
}

/*
 * compact_column_file — core routine.
 *
 * Reads all blocks from <part_dir>/<col_name>.col/.idx,
 * decodes them to flat arrays, re-encodes in COMPACT_BLOCK_POINTS chunks,
 * writes .col.tmp / .idx.tmp, then (under compact_mtx) renames into place.
 *
 * table_lock_fn / table_unlock_fn / lock_ud provide the mutex callbacks
 * so we don't need to know the tsdb_table_internal_t layout here.
 *
 * Returns TSDB_OK on success, TSDB_OK (no-op) if no compaction needed,
 * or negative on I/O error (caller logs; compaction continues to next col).
 */
typedef void (*lock_fn_t)(void *ud);

/* Read the current block count from a column's live .idx header.
 * Returns UINT32_MAX if the file is missing/short/unrecognised — callers treat
 * that as "changed" and conservatively skip the swap. */
static uint32_t live_idx_block_count(const char *part_dir, const char *col_name) {
    char idx_path[4096];
    snprintf(idx_path, sizeof(idx_path), "%s/%s.idx", part_dir, col_name);
    FILE *f = fopen(idx_path, "rb");
    if (!f) return UINT32_MAX;
    uint8_t hdr[8];
    size_t n = fread(hdr, 1, sizeof(hdr), f);
    fclose(f);
    if (n < 8 || get_u32le(hdr) != IDX_MAGIC) return UINT32_MAX;
    return get_u32le(hdr + 4);
}

static int compact_column_file(const char *part_dir,
                               const char *col_name,
                               tsdb_type_t col_type,
                               int         threshold,
                               uint64_t   *out_bytes_written,
                               uint64_t   *out_bytes_saved,
                               int        *out_produced,
                               uint32_t   *out_src_blocks)
{
    char col_path[4096], idx_path[4096];
    char col_tmp[4096],  idx_tmp[4096];

    if (out_produced) *out_produced = 0;
    if (out_src_blocks) *out_src_blocks = 0;

    snprintf(col_path, sizeof(col_path), "%s/%s.col", part_dir, col_name);
    snprintf(idx_path, sizeof(idx_path), "%s/%s.idx", part_dir, col_name);
    snprintf(col_tmp,  sizeof(col_tmp),  "%s/%s.col.tmp", part_dir, col_name);
    snprintf(idx_tmp,  sizeof(idx_tmp),  "%s/%s.idx.tmp", part_dir, col_name);

    /* ---- 1. mmap the existing .col file ----------------------------------- */

    int fd = open(col_path, O_RDONLY);
    if (fd < 0) return TSDB_OK;   /* column doesn't exist yet — nothing to do */

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); return TSDB_OK; }

    size_t   map_len = (size_t)st.st_size;
    uint64_t old_bytes = (uint64_t)map_len;

    uint8_t *col_map = mmap(NULL, map_len, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (col_map == MAP_FAILED) return TSDB_ERR_IO;

    /* ---- 2. Read .idx header to get block count --------------------------- */

    FILE *idx_f = fopen(idx_path, "rb");
    if (!idx_f) { munmap(col_map, map_len); return TSDB_OK; }

    uint8_t hdr_buf[IDX_HDR_SZ];
    size_t  hdr_n = fread(hdr_buf, 1, IDX_HDR_SZ, idx_f);
    if (hdr_n < IDX_HDR_SZ_V1 ||
        get_u32le(hdr_buf) != IDX_MAGIC) {
        fclose(idx_f);
        munmap(col_map, map_len);
        return TSDB_OK;
    }
    uint32_t block_count = get_u32le(hdr_buf + 4);
    if (out_src_blocks) *out_src_blocks = block_count;
    uint16_t idx_ver     = get_u16le(hdr_buf + 8);
    uint64_t total_rows  = get_u64le(hdr_buf + 12);

    /* Not enough blocks to bother compacting. */
    if ((int)block_count < threshold) {
        fclose(idx_f);
        munmap(col_map, map_len);
        return TSDB_OK;
    }

    /* Already compacted: re-compacting a partition whose block count is
     * already at (or below) what a fresh compaction would produce just
     * re-encodes the same rows and reacquires the swap lock on every scan —
     * an infinite re-compaction loop (observed burning a core and churning
     * files on a steady-state cluster).  ceil(total_rows / COMPACT_BLOCK_POINTS)
     * is the post-compaction block count; skip once we are already there. */
    if (total_rows > 0) {
        uint64_t expected = (total_rows + COMPACT_BLOCK_POINTS - 1)
                            / (uint64_t)COMPACT_BLOCK_POINTS;
        if ((uint64_t)block_count <= expected) {
            fclose(idx_f);
            munmap(col_map, map_len);
            return TSDB_OK;
        }
    }

    /* Decide header / entry sizes based on on-disk version. */
    int    hdr_sz   = (idx_ver == 1) ? (int)IDX_HDR_SZ_V1
                    : (idx_ver == 2) ? (int)IDX_HDR_SZ_V2
                                     : (int)IDX_HDR_SZ;
    size_t entry_sz = (idx_ver == 3 && hdr_n >= IDX_HDR_SZ)
                     ? (size_t)get_u16le(hdr_buf + 36)
                     : (size_t)IDX_ENTRY_SZ_V2;
    if (entry_sz == 0) entry_sz = IDX_ENTRY_SZ_V2;

    /* ---- 3. Read all BlockIndexEntry records ------------------------------- */

    block_info_t *infos = malloc((size_t)block_count * sizeof(block_info_t));
    if (!infos) {
        fclose(idx_f);
        munmap(col_map, map_len);
        return TSDB_ERR_NOMEM;
    }

    if (fseek(idx_f, hdr_sz, SEEK_SET) != 0) {
        free(infos);
        fclose(idx_f);
        munmap(col_map, map_len);
        return TSDB_ERR_IO;
    }

    uint8_t *ebuf = malloc(entry_sz);
    if (!ebuf) {
        free(infos); fclose(idx_f);
        munmap(col_map, map_len);
        return TSDB_ERR_NOMEM;
    }
    for (uint32_t b = 0; b < block_count; b++) {
        if (fread(ebuf, 1, entry_sz, idx_f) != entry_sz) {
            free(ebuf); free(infos);
            fclose(idx_f);
            munmap(col_map, map_len);
            return TSDB_ERR_IO;
        }
        infos[b].offset = get_u64le(ebuf + 0);
        infos[b].data_sz= get_u32le(ebuf + 8);
        infos[b].count  = get_u32le(ebuf + 12);
        infos[b].ts_min = get_i64le(ebuf + 16);
        infos[b].ts_max = get_i64le(ebuf + 24);
        infos[b].codec  = 0;
        infos[b].flags  = 0;
    }
    free(ebuf);
    fclose(idx_f);

    /* The .col was mmap'd BEFORE the .idx was read, so a flush that appended a
     * block in that gap grew the .col file past our stale map while the idx we
     * just read already references it — the bounds checks below would then
     * silently DROP that newest block and the swap would lose it.  If the .col
     * FILE has grown since our map, re-map it at its current size so those
     * bytes are visible (the flush publishes .col bytes before the .idx, so an
     * idx-referenced block is on disk).  Size to the real file, NOT to idx
     * offsets — a lock-free idx read can hold garbage offsets for entries the
     * decode's bounds check skips anyway.  If the file did not grow this is a
     * no-op, so the common (race-free) path is unchanged. */
    {
        int rfd = open(col_path, O_RDONLY);
        struct stat rst;
        if (rfd >= 0 && fstat(rfd, &rst) == 0 &&
            (uint64_t)rst.st_size > (uint64_t)map_len) {
            uint8_t *nm = mmap(NULL, (size_t)rst.st_size, PROT_READ,
                               MAP_PRIVATE, rfd, 0);
            if (nm != MAP_FAILED) {
                munmap(col_map, map_len);
                col_map = nm;
                map_len = (size_t)rst.st_size;
            }
        }
        if (rfd >= 0) close(rfd);
    }

    /* Back-fill codec and flags from BlockHeaders in the mmap. */
    for (uint32_t b = 0; b < block_count; b++) {
        uint64_t off = infos[b].offset;
        if (off + BLK_HDR_SZ > map_len) continue;
        const uint8_t *h = col_map + off;
        if (get_u32le(h) != BLK_MAGIC) continue;
        infos[b].codec = h[4];
        infos[b].flags = get_u16le(h + 6);
    }

    /* ---- 4. Decode all blocks into a flat buffer -------------------------- */

    size_t val_width = tsdb_type_width(col_type);
    size_t total_vals = (size_t)total_rows;

    uint8_t *raw_buf = malloc(total_vals * val_width);
    if (!raw_buf) {
        free(infos);
        munmap(col_map, map_len);
        return TSDB_ERR_NOMEM;
    }

    size_t out_pos = 0;
    for (uint32_t b = 0; b < block_count; b++) {
        uint64_t off     = infos[b].offset;
        uint32_t data_sz = infos[b].data_sz;
        uint32_t cnt     = infos[b].count;

        if (off + BLK_HDR_SZ + data_sz > map_len || cnt == 0) continue;

        /* Bound the WRITE into raw_buf.  raw_buf is sized to total_vals (the
         * idx header's total_rows).  If the per-block counts sum to MORE than
         * that — which happens when a concurrent flush leaves the header and
         * the block entries momentarily inconsistent (the compactor reads the
         * .idx lock-free) — decoding the next block would write past raw_buf
         * and corrupt the heap (observed as a glibc sysmalloc abort under
         * drop+write+compaction stress).  Bail on the column; it stays as-is
         * and a later compaction pass picks up the settled, consistent file. */
        if (out_pos + (size_t)cnt > total_vals) {
            free(raw_buf);
            free(infos);
            munmap(col_map, map_len);
            return TSDB_OK;
        }

        const uint8_t *data_ptr = col_map + off + BLK_HDR_SZ;

        int rc = tsdb_codec_decode_adaptive(
            (tsdb_codec_t)infos[b].codec,
            col_type,
            infos[b].flags,
            data_ptr,
            (size_t)data_sz,
            raw_buf + out_pos * val_width,
            (size_t)cnt);

        if (rc != TSDB_OK) {
            /* Corruption in one block: abort column compaction, leave as-is. */
            free(raw_buf);
            free(infos);
            munmap(col_map, map_len);
            return TSDB_OK;
        }
        out_pos += cnt;
    }

    munmap(col_map, map_len);

    /* Adjust total_vals to what we actually decoded (could differ if some
     * blocks were skipped due to map corruption). */
    total_vals = out_pos;

    /* ---- 5. Re-encode into .col.tmp / .idx.tmp ---------------------------- */

    FILE *new_col = fopen(col_tmp, "wb");
    FILE *new_idx = fopen(idx_tmp, "wb");
    if (!new_col || !new_idx) {
        if (new_col) fclose(new_col);
        if (new_idx) fclose(new_idx);
        free(raw_buf);
        free(infos);
        return TSDB_ERR_IO;
    }

    /* We need to know the block count upfront for the idx header; collect
     * index entries first, then write the header at the end by rewinding. */
    uint32_t new_block_count = 0;
    uint64_t col_offset      = 0;
    uint64_t new_total_rows  = 0;
    int64_t  file_ts_min     = INT64_MAX;
    int64_t  file_ts_max     = INT64_MIN;

    /* Pre-compute: how many output blocks? */
    uint32_t n_out_blocks = (uint32_t)((total_vals + COMPACT_BLOCK_POINTS - 1)
                                       / COMPACT_BLOCK_POINTS);

    /* Write placeholder idx header (will be rewritten). */
    {
        uint8_t placeholder[IDX_HDR_SZ];
        memset(placeholder, 0, IDX_HDR_SZ);
        if (safe_write(new_idx, placeholder, IDX_HDR_SZ) < 0) goto io_err;
    }

    /* Allocate idx-entry accumulation buffer. */
    uint8_t *idx_entries = malloc((size_t)n_out_blocks * IDX_ENTRY_SZ);
    uint8_t *comp_buf    = malloc(MAX_COMP_OUT);
    if (!idx_entries || !comp_buf) {
        free(idx_entries);
        free(comp_buf);
        goto nomem_err;
    }

    for (size_t base = 0; base < total_vals; base += COMPACT_BLOCK_POINTS) {
        size_t chunk = total_vals - base;
        if (chunk > COMPACT_BLOCK_POINTS) chunk = COMPACT_BLOCK_POINTS;

        const void *chunk_ptr = raw_buf + base * val_width;

        /* Compute ts range for this chunk (only meaningful for TIMESTAMP col;
         * for other cols we re-use the range from the source idx entries). */
        int64_t blk_ts_min, blk_ts_max;
        if (col_type == TSDB_TYPE_TIMESTAMP) {
            const int64_t *ts_vals = (const int64_t *)chunk_ptr;
            blk_ts_min = ts_vals[0];
            blk_ts_max = ts_vals[0];
            for (size_t k = 1; k < chunk; k++) {
                if (ts_vals[k] < blk_ts_min) blk_ts_min = ts_vals[k];
                if (ts_vals[k] > blk_ts_max) blk_ts_max = ts_vals[k];
            }
        } else {
            /* For non-timestamp columns, we borrow ts_min/ts_max from the
             * corresponding source block(s).  Since we're concatenating in
             * order, find which source block covers position `base`. */
            size_t src_base = 0;
            blk_ts_min = INT64_MAX;
            blk_ts_max = INT64_MIN;
            for (uint32_t b = 0; b < block_count; b++) {
                size_t bcnt = (size_t)infos[b].count;
                size_t src_end = src_base + bcnt;
                /* Check if source block [src_base, src_end) overlaps [base, base+chunk). */
                if (src_end > base && src_base < base + chunk) {
                    if (infos[b].ts_min < blk_ts_min) blk_ts_min = infos[b].ts_min;
                    if (infos[b].ts_max > blk_ts_max) blk_ts_max = infos[b].ts_max;
                }
                src_base = src_end;
            }
            if (blk_ts_min == INT64_MAX) { blk_ts_min = 0; blk_ts_max = 0; }
        }

        /* Encode at the L2 cold tier (lower outer-lzlite wrap threshold). */
        tsdb_codec_t codec_used = TSDB_CODEC_NONE;
        uint16_t     blk_flags  = 0;
        int comp_bytes = tsdb_codec_encode_adaptive_ex(
            col_type, chunk_ptr, chunk,
            comp_buf, MAX_COMP_OUT,
            &codec_used, &blk_flags,
            l2_min_gain());
        if (comp_bytes < 0) {
            free(idx_entries);
            free(comp_buf);
            goto io_err;
        }

        /* Write BlockHeader + compressed data + CRC32C trailer.  Mirrors
         * the flush-path layout in part.c so the reader's CRC verifier
         * doesn't reject compacted blocks. */
        blk_flags |= TSDB_BLOCK_FLAG_HAS_CRC;
        uint8_t blk_hdr[BLK_HDR_SZ];
        write_blk_hdr(blk_hdr, (uint8_t)codec_used, blk_flags,
                      (uint32_t)chunk,
                      blk_ts_min, blk_ts_max,
                      (uint32_t)comp_bytes);
        if (safe_write(new_col, blk_hdr,  BLK_HDR_SZ)   < 0 ||
            safe_write(new_col, comp_buf, (size_t)comp_bytes) < 0) {
            free(idx_entries);
            free(comp_buf);
            goto io_err;
        }
        {
            uint32_t crc = tsdb_crc32c(blk_hdr, BLK_HDR_SZ);
            if ((size_t)comp_bytes > 0)
                crc = tsdb_crc32c_update(crc, comp_buf, (size_t)comp_bytes);
            uint8_t trailer[TSDB_BLOCK_CRC_TRAILER_SIZE];
            trailer[0] = (uint8_t)(crc      );
            trailer[1] = (uint8_t)(crc >>  8);
            trailer[2] = (uint8_t)(crc >> 16);
            trailer[3] = (uint8_t)(crc >> 24);
            if (safe_write(new_col, trailer, TSDB_BLOCK_CRC_TRAILER_SIZE) < 0) {
                free(idx_entries); free(comp_buf); goto io_err;
            }
        }

        /* Accumulate idx entry. */
        uint8_t *ep = idx_entries + new_block_count * IDX_ENTRY_SZ;
        write_idx_entry(ep, col_offset, (uint32_t)comp_bytes,
                        (uint32_t)chunk, blk_ts_min, blk_ts_max);
        new_block_count++;

        col_offset    += BLK_HDR_SZ + (uint64_t)comp_bytes
                          + TSDB_BLOCK_CRC_TRAILER_SIZE;
        new_total_rows += chunk;

        if (blk_ts_min < file_ts_min) file_ts_min = blk_ts_min;
        if (blk_ts_max > file_ts_max) file_ts_max = blk_ts_max;
    }

    free(comp_buf);

    /* Write all idx entries. */
    if (new_block_count > 0 &&
        safe_write(new_idx, idx_entries,
                   (size_t)new_block_count * IDX_ENTRY_SZ) < 0) {
        free(idx_entries);
        goto io_err;
    }
    free(idx_entries);

    /* Rewind idx and write the real header. */
    if (fseek(new_idx, 0, SEEK_SET) != 0) goto io_err;
    {
        uint8_t real_hdr[IDX_HDR_SZ];
        if (file_ts_min == INT64_MAX) { file_ts_min = 0; file_ts_max = 0; }
        write_idx_hdr(real_hdr, new_block_count, new_total_rows,
                      file_ts_min, file_ts_max);
        if (safe_write(new_idx, real_hdr, IDX_HDR_SZ) < 0) goto io_err;
    }

    /* fsync both tmp files before rename. */
    fflush(new_col);
    fflush(new_idx);
    {
        int fc = fileno(new_col);
        int fi = fileno(new_idx);
        if (fc >= 0) fsync(fc);
        if (fi >= 0) fsync(fi);
    }
    fclose(new_col); new_col = NULL;
    fclose(new_idx); new_idx = NULL;

    free(raw_buf);
    free(infos);

    /* ---- 6. Leave .col.tmp / .idx.tmp on disk for the caller to swap -------
     * The rename is NOT done here.  compact_partition swaps every column of
     * the partition together, under one hold of the compact lock, so a
     * concurrent reader (which takes the same lock around tsdb_part_open)
     * never observes a partition with some columns compacted (large blocks)
     * and others not — a mix that breaks the scan's cross-column block
     * alignment and yields wrong results. */
    if (out_bytes_written) *out_bytes_written += col_offset;
    if (out_bytes_saved)   *out_bytes_saved   += (col_offset < old_bytes)
                                                  ? (old_bytes - col_offset) : 0;
    if (out_produced) *out_produced = 1;
    return TSDB_OK;

io_err:
    if (new_col) fclose(new_col);
    if (new_idx) fclose(new_idx);
    remove(col_tmp);
    remove(idx_tmp);
    free(raw_buf);
    free(infos);
    return TSDB_ERR_IO;

nomem_err:
    fclose(new_col);
    fclose(new_idx);
    remove(col_tmp);
    remove(idx_tmp);
    free(raw_buf);
    free(infos);
    return TSDB_ERR_NOMEM;
}

/* ---- compact_partition ---------------------------------------------------- */

/*
 * Compact one partition directory for the given table (schema + lock).
 */
static int compact_partition(tsdb_schema_t   *schema,
                             const char      *part_dir,
                             int              threshold,
                             lock_fn_t        lock_fn,
                             lock_fn_t        unlock_fn,
                             void            *lock_ud,
                             tsdb_compactor_stats_t *stats)
{
    int any_compacted = 0;

    int *produced = calloc((size_t)schema->ncols, sizeof(int));
    uint32_t *src_blocks = calloc((size_t)schema->ncols, sizeof(uint32_t));
    if (!produced || !src_blocks) { free(produced); free(src_blocks); return TSDB_ERR_NOMEM; }

    /* Phase 1 — re-encode each eligible column to .col.tmp/.idx.tmp.  No lock:
     * this only reads the live files and writes new .tmp files.  This is the
     * expensive step (decode + best-of-N re-encode) and is deliberately kept
     * off the compact lock so readers are never blocked during it.  We record
     * the source block count each column was compacted from so phase 2 can
     * detect a concurrent flush that appended blocks in the meantime. */
    for (int ci = 0; ci < schema->ncols; ci++) {
        uint64_t bw = 0, bs = 0;
        int prod = 0;
        uint32_t srcb = 0;
        int rc = compact_column_file(part_dir,
                                     schema->cols[ci].name,
                                     schema->cols[ci].type,
                                     threshold,
                                     &bw, &bs, &prod, &srcb);
        if (rc == TSDB_OK && prod) {
            produced[ci]   = 1;
            src_blocks[ci] = srcb;
            any_compacted = 1;
            tsdb_metric_inc("qengine_compactions_total");
            if (stats) {
                stats->bytes_written += bw;
                stats->bytes_saved   += bs;
                stats->compactions_done++;
            }
        }
        /* Non-fatal errors: continue with next column (its .tmp, if any, was
         * already removed by compact_column_file). */
    }

    /* Phase 2 — swap EVERY produced column atomically under one lock hold, so
     * a concurrent reader (taking the same lock around tsdb_part_open) sees
     * the partition as either entirely pre- or entirely post-compaction.
     * Doing this per-column would expose a window where the ts (enumerator)
     * column is compacted but a value column is not, misaligning block
     * boundaries and corrupting the scan's results. */
    if (any_compacted) {
        if (lock_fn) lock_fn(lock_ud);

        /* Staleness guard: if ANY produced column's live .idx grew since we
         * snapshotted it in phase 1 (a flush appended blocks to this — now no
         * longer cold — partition), our .tmp is a stale prefix.  Swapping it in
         * would clobber the appended block and silently drop those rows (until
         * anti-entropy heals).  Abort the WHOLE partition swap to keep the
         * cross-column block alignment consistent; a later pass compacts the
         * settled files.  flush holds compact_mtx for its append, so reading
         * the live counts here (under the same lock) is a consistent check. */
        int stale = 0;
        for (int ci = 0; ci < schema->ncols && !stale; ci++) {
            if (!produced[ci]) continue;
            if (live_idx_block_count(part_dir, schema->cols[ci].name) != src_blocks[ci])
                stale = 1;
        }

        /* Crash-atomicity journal.  The per-column renames below are atomic
         * individually but NOT as a set: a crash (or this engine's fast-exit
         * shutdown) between them leaves some columns compacted and others on
         * the old block layout — a desynced partition that reads as corrupt.
         * Persist the produced-column list BEFORE the first rename (file
         * fsync + directory fsync, so no rename can become durable without
         * the marker also being durable); tsdb_part_open's recovery then
         * rolls any interrupted swap forward by renaming the remaining .tmp
         * pairs.  The marker is unlinked only after the renames are fsynced. */
        char swap_marker[4096];
        snprintf(swap_marker, sizeof(swap_marker), "%s/.compact_swap", part_dir);
        if (!stale) {
            FILE *mf = fopen(swap_marker, "w");
            if (mf) {
                for (int ci = 0; ci < schema->ncols; ci++)
                    if (produced[ci]) fprintf(mf, "%s\n", schema->cols[ci].name);
                fflush(mf);
                fsync(fileno(mf));
                fclose(mf);
                int dfd = open(part_dir, O_RDONLY);
                if (dfd >= 0) { fsync(dfd); close(dfd); }
            } else {
                /* Cannot journal — do not risk a torn swap; leave the .tmp
                 * files for a later pass and keep the old (consistent) set. */
                stale = 1;
            }
        }

        for (int ci = 0; ci < schema->ncols; ci++) {
            if (!produced[ci]) continue;
            char col_path[4096], idx_path[4096], col_tmp[4096], idx_tmp[4096];
            snprintf(col_path, sizeof(col_path), "%s/%s.col", part_dir, schema->cols[ci].name);
            snprintf(idx_path, sizeof(idx_path), "%s/%s.idx", part_dir, schema->cols[ci].name);
            snprintf(col_tmp,  sizeof(col_tmp),  "%s/%s.col.tmp", part_dir, schema->cols[ci].name);
            snprintf(idx_tmp,  sizeof(idx_tmp),  "%s/%s.idx.tmp", part_dir, schema->cols[ci].name);
            if (stale) { remove(col_tmp); remove(idx_tmp); continue; }
            /* Test-only: widen the in-swap window (some columns already
             * renamed, others not) so the concurrent-read test deterministically
             * stresses the reader-blocking path.  Under the lock → readers wait
             * and never observe the mixed state.  Never set in production. */
            const char *rd = getenv("TSDB_TEST_COMPACT_RENAME_DELAY_MS");
            if (rd && *rd) {
                int ms = atoi(rd);
                if (ms > 0) {
                    struct timespec dts = { ms / 1000, (long)(ms % 1000) * 1000000L };
                    nanosleep(&dts, NULL);
                }
            }
            rename(col_tmp, col_path);
            rename(idx_tmp, idx_path);
        }
        if (!stale) {
            /* Make the renames durable, then retire the journal.  Order
             * matters: if the marker outlives a crash the recovery is a
             * no-op (no .tmp files remain); if renames were lost with it,
             * recovery replays them from the still-present .tmp files. */
            int dfd = open(part_dir, O_RDONLY);
            if (dfd >= 0) { fsync(dfd); close(dfd); }
            unlink(swap_marker);
        }
        if (unlock_fn) unlock_fn(lock_ud);
    }

    free(produced);
    free(src_blocks);
    if (any_compacted && stats) {
        stats->parts_merged++;
    }
    return TSDB_OK;
}

/* ---- Compactor internals -------------------------------------------------- */

/* Per-table lock/unlock callbacks stored in the compactor. */
typedef struct {
    tsdb_table_internal_t *tbl;   /* weak reference — protected by db->lock */
    pthread_mutex_t       *mtx;   /* points at tbl->compact_mtx */
} tbl_lock_entry_t;

struct tsdb_compactor {
    tsdb_db_t     *db;

    /* Options. */
    int     threshold;
    int64_t interval_ns;
    int     nworkers;

    /* Workers. */
    pthread_t      *workers;
    volatile int    quit;

    /* Adaptive backoff — skip this cycle if recent flush rate exceeds the
     * threshold so the writer's I/O isn't fighting compactor I/O for the
     * device queue.  Threshold derived from iopolicy at start: SSD=0
     * (never pause), SAS=100/s, HDD=50/s.  Env override
     * TSDB_COMPACTION_BUSY_THRESHOLD; 0 disables. */
    int             busy_threshold;
    int64_t         last_check_ns;
    uint64_t        last_flush_seq;

    /* Per-table mtime memo (kdb+ style "immutable HDB" inspired): a table none
     * of whose PARTITION dirs has moved since we last processed it has had no
     * new flush → no compaction possible.  Keyed on max(partition-dir mtime),
     * not the table-dir mtime: an append into an existing partition bumps that
     * partition dir (via the <col>.idx temp+rename) but never the table dir, so
     * a table-dir key would wrongly skip a steadily-written table.  Skipping an
     * idle table saves the inner partition-walk syscalls (open+getdents+stat per
     * partition) which dominate compactor CPU at 2000 tables.  Compactor's own
     * writes DO bump partition mtimes, so the next cycle still re-checks any
     * table we just touched. The memo is reset (size set to 0) on disable via
     * TSDB_COMPACTION_MEMO=0 — useful for safety drills. */
    int                       memo_enabled;
    pthread_mutex_t           memo_mtx;
    struct tsdb_compact_memo *memo;       /* heap-grown table → mtime map */
    int                       memo_n;
    int                       memo_cap;

    /* Stats (protected by stats_mtx). */
    pthread_mutex_t         stats_mtx;
    tsdb_compactor_stats_t  stats;
};

struct tsdb_compact_memo {
    char    name[64];
    time_t  last_mtime;
};

/* lock/unlock callbacks passed into compact_partition. */
static void cpt_lock(void *ud) {
    pthread_mutex_t *m = (pthread_mutex_t *)ud;
    if (m) pthread_mutex_lock(m);
}
static void cpt_unlock(void *ud) {
    pthread_mutex_t *m = (pthread_mutex_t *)ud;
    if (m) pthread_mutex_unlock(m);
}

/* ---- compactor_run_once_impl ---------------------------------------------- */

static int64_t compactor_now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* True if the writer has flushed faster than busy_threshold over the last
 * sampling window — compactor backs off this round so the writer's flushes
 * get exclusive use of the device queue.  The single-sampler-per-cycle
 * design keeps the cost to one atomic load per compactor wake-up, far
 * cheaper than tracking a sliding window. */
/* mtime memo lookup — returns the stored mtime, or 0 if name unknown.
 * Hot path under memo_mtx: O(N) linear over N=#tables ever seen.
 * For 2000 tables that's ~2000 strcmps (~64 KiB compares) per cycle —
 * cheap vs. the per-partition syscalls we'd do without the skip. */
static time_t compactor_memo_get(tsdb_compactor_t *c, const char *name) {
    pthread_mutex_lock(&c->memo_mtx);
    time_t out = 0;
    for (int i = 0; i < c->memo_n; i++) {
        if (strcmp(c->memo[i].name, name) == 0) { out = c->memo[i].last_mtime; break; }
    }
    pthread_mutex_unlock(&c->memo_mtx);
    return out;
}
static void compactor_memo_set(tsdb_compactor_t *c, const char *name, time_t mtime) {
    pthread_mutex_lock(&c->memo_mtx);
    for (int i = 0; i < c->memo_n; i++) {
        if (strcmp(c->memo[i].name, name) == 0) { c->memo[i].last_mtime = mtime; pthread_mutex_unlock(&c->memo_mtx); return; }
    }
    if (c->memo_n >= c->memo_cap) {
        int newcap = c->memo_cap ? c->memo_cap * 2 : 64;
        struct tsdb_compact_memo *nm = realloc(c->memo, (size_t)newcap * sizeof(*nm));
        if (!nm) { pthread_mutex_unlock(&c->memo_mtx); return; }
        c->memo = nm; c->memo_cap = newcap;
    }
    snprintf(c->memo[c->memo_n].name, sizeof(c->memo[c->memo_n].name), "%s", name);
    c->memo[c->memo_n].last_mtime = mtime;
    c->memo_n++;
    pthread_mutex_unlock(&c->memo_mtx);
}

static int compactor_should_pause(tsdb_compactor_t *c) {
    if (c->busy_threshold <= 0) return 0;
    int64_t now = compactor_now_ns();
    uint64_t cur = tsdb_db_flush_seq(c->db);
    if (c->last_check_ns == 0) {
        c->last_check_ns = now;
        c->last_flush_seq = cur;
        return 0;
    }
    int64_t dt = now - c->last_check_ns;
    if (dt < 1000000000LL) return 0;   /* < 1 s — not enough signal */
    double rate = (double)(cur - c->last_flush_seq) * 1e9 / (double)dt;
    int pause = rate > (double)c->busy_threshold;
    c->last_check_ns = now;
    c->last_flush_seq = cur;
    return pause;
}

/* Newest mtime across a table's partition subdirs.  Keying the compaction memo
 * on this (not the table-dir mtime) is what makes an append into an EXISTING
 * partition bust the memo: flush publishes <col>.idx via temp+rename, which
 * bumps the owning partition dir's mtime — but never the table dir's.  Returns 0
 * if the dir is unreadable or has no partitions (caller then never memo-skips).
 * Stats partition dirs only, not the per-column files — cheap. */
static time_t table_max_part_mtime(const char *tbl_dir) {
    DIR *td = opendir(tbl_dir);
    if (!td) return 0;
    time_t maxm = 0;
    struct dirent *pe;
    while ((pe = readdir(td)) != NULL) {
        if (!is_partition_dir(pe->d_name)) continue;
        char part_dir[4096];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", tbl_dir, pe->d_name);
        struct stat ps;
        if (stat(part_dir, &ps) == 0 && ps.st_mtime > maxm) maxm = ps.st_mtime;
    }
    closedir(td);
    return maxm;
}

/* One compaction pass over the table subdirs of a single data dir.  Split
 * out of compactor_run_once_impl() so every striped data dir gets the same
 * pass; the per-table logic is unchanged. */
static int compactor_scan_dir(tsdb_compactor_t *c, tsdb_db_t *db, const char *data_dir) {
    DIR *dd = opendir(data_dir);
    if (!dd) return TSDB_ERR_IO;

    struct dirent *de;
    while ((de = readdir(dd)) != NULL) {
        if (de->d_name[0] == '.') continue;   /* skip . .. .gitkeep etc. */

        /* Skip known non-table dirs. */
        if (strcmp(de->d_name, "wal") == 0) continue;
        if (strcmp(de->d_name, "catalog") == 0) continue;

        /* Check it's actually a directory. */
        char tbl_dir[4096];
        snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", data_dir, de->d_name);
        struct stat tst;
        if (stat(tbl_dir, &tst) < 0 || !S_ISDIR(tst.st_mode)) continue;

        /* mtime memo: skip the entire per-partition scan if none of this
         * table's PARTITION dirs has moved since we last processed it.  Keyed on
         * max(partition-dir mtime), NOT the table-dir mtime — a flush that
         * appends into an existing partition bumps that partition dir (via the
         * <col>.idx temp+rename) but never the table dir, so a table-dir key
         * would wrongly skip a steadily-written table forever.  Compaction
         * itself rewrites partition files and bumps their mtime, so the NEXT
         * cycle still re-checks any table we just touched.  Big win on
         * wide-cardinality workloads where most tables are idle in any given
         * 10 s window.  part_mtime is recorded pre-walk (below) so an append
         * during this cycle is re-checked next cycle. */
        time_t part_mtime = 0;
        if (c->memo_enabled) {
            part_mtime = table_max_part_mtime(tbl_dir);
            time_t cached = compactor_memo_get(c, de->d_name);
            if (part_mtime != 0 && cached == part_mtime) {
                tsdb_metric_inc("qengine_compaction_memo_skipped_total");
                continue;
            }
        }

        /* Acquire the table for compaction: marks it `compacting` so a
         * concurrent DROP TABLE waits before freeing it (we use its raw
         * schema/compact_mtx pointers lock-free below).  Must be released on
         * every path out of this iteration. */
        tsdb_table_internal_t *tbl = tsdb_db_compact_acquire(db, de->d_name);
        if (!tbl) continue;   /* table not currently open — skip */

        tsdb_schema_t *schema = tsdb_tbl_schema(tbl);
        if (!schema) { tsdb_db_compact_release(db, tbl); continue; }

        /* Retrieve the per-table compact mutex. */
        pthread_mutex_t *cmtx = tsdb_tbl_compact_mtx(tbl);

        /* Enumerate partition subdirs under the table dir. */
        DIR *td = opendir(tbl_dir);
        if (!td) { tsdb_db_compact_release(db, tbl); continue; }

        struct dirent *pe;
        while ((pe = readdir(td)) != NULL) {
            if (!is_partition_dir(pe->d_name)) continue;

            char part_dir[4096];
            snprintf(part_dir, sizeof(part_dir), "%s/%s", tbl_dir, pe->d_name);

            /* Skip partitions modified in the last 60 seconds (still hot). */
            struct stat ps;
            if (stat(part_dir, &ps) == 0) {
                struct timespec now;
                clock_gettime(CLOCK_REALTIME, &now);
                int64_t age_s = (int64_t)now.tv_sec - (int64_t)ps.st_mtime;
                if (age_s < 60) continue;
            }

            tsdb_compactor_stats_t local_stats;
            memset(&local_stats, 0, sizeof(local_stats));

            compact_partition(schema, part_dir,
                              c->threshold,
                              cpt_lock, cpt_unlock, (void *)cmtx,
                              &local_stats);

            /* Merge local_stats into c->stats under lock. */
            if (local_stats.compactions_done > 0) {
                pthread_mutex_lock(&c->stats_mtx);
                c->stats.compactions_done += local_stats.compactions_done;
                c->stats.parts_merged     += local_stats.parts_merged;
                c->stats.bytes_written    += local_stats.bytes_written;
                c->stats.bytes_saved      += local_stats.bytes_saved;
                pthread_mutex_unlock(&c->stats_mtx);
            }

            if (c->quit) break;
        }
        closedir(td);
        tsdb_db_compact_release(db, tbl);

        /* Memo: record the max partition mtime we observed pre-walk so the next
         * cycle can fast-skip this table if no flush has touched any partition.
         * Recorded even when no compaction happened — equally valid signal. */
        if (c->memo_enabled) compactor_memo_set(c, de->d_name, part_mtime);

        if (c->quit) break;
    }
    closedir(dd);
    return TSDB_OK;
}

static int compactor_run_once_impl(tsdb_compactor_t *c) {
    if (compactor_should_pause(c)) {
        tsdb_metric_inc("qengine_compaction_paused_total");
        return TSDB_OK;
    }
    /* Snapshot the db's table list under db->lock. */
    tsdb_db_t *db = c->db;

    /* We need to enumerate tables.  Use the public accessor. */
    /* Walk EVERY striped data dir for table subdirs — tables are routed
     * onto one of the TSDB_DATA_DIRS stripes by name hash, so scanning
     * only the primary would never compact striped tables.  Single-dir
     * setups have count==1 → one pass over data_dir, exactly as before. */

    int ndirs = tsdb_db_data_dir_count(db);
    if (ndirs <= 0) ndirs = 1;

    int rc = TSDB_ERR_IO;
    for (int di = 0; di < ndirs; di++) {
        const char *data_dir = tsdb_db_data_dir_at(db, di);
        if (!data_dir || !data_dir[0]) {
            if (di == 0) data_dir = tsdb_db_data_dir(db);
            if (!data_dir) continue;
        }
        if (compactor_scan_dir(c, db, data_dir) == TSDB_OK) rc = TSDB_OK;
        if (c->quit) break;
    }
    return rc;
}

/* ---- Worker thread -------------------------------------------------------- */

/* Drop the calling thread to IDLE I/O class + nice 19.  On Linux the block
 * layer's CFQ/BFQ schedulers only dispatch IDLE-class requests when no other
 * I/O is pending, so the writer's WAL fsync + memtable flushes preempt
 * compactor reads/writes on the same HDD.  Eliminates the wide-cardinality
 * sustained-write cliff (measured: ~0.8M r/s → 1.4M r/s sustained, 8 min run,
 * 2000 tables).  Opt-out with TSDB_COMPACTION_IDLE_IO=0.  Linux only — other
 * systems silently skip. */
static void compactor_demote_io_priority(void) {
#if defined(__linux__)
    const char *e = getenv("TSDB_COMPACTION_IDLE_IO");
    if (e && e[0] == '0') return;
    /* ioprio_set: who=PROCESS(1) targets a TID; class IDLE=3. */
    pid_t tid = (pid_t)syscall(SYS_gettid);
    (void)syscall(SYS_ioprio_set, /*IOPRIO_WHO_PROCESS=*/1, (int)tid,
                  (3 /*IOPRIO_CLASS_IDLE*/ << 13) | 0);
    /* nice 19 — compactor never needs to be timely under write load. */
    (void)setpriority(PRIO_PROCESS, (id_t)tid, 19);
#endif
}

static void *compactor_worker(void *arg) {
    tsdb_compactor_t *c = (tsdb_compactor_t *)arg;

    compactor_demote_io_priority();

    while (!c->quit) {
        compactor_run_once_impl(c);
        if (c->quit) break;

        /* Sleep interval_ns in small increments so quit is checked promptly. */
        int64_t remaining = c->interval_ns;
        while (remaining > 0 && !c->quit) {
            int64_t slice = remaining > 100000000LL ? 100000000LL : remaining; /* 100ms */
            struct timespec ts = {
                .tv_sec  = (time_t)(slice / 1000000000LL),
                .tv_nsec = (long)(slice % 1000000000LL)
            };
            nanosleep(&ts, NULL);
            remaining -= slice;
        }
    }
    return NULL;
}

/* ---- Public API ------------------------------------------------------------ */

int tsdb_compactor_start(tsdb_db_t *db,
                         const tsdb_compactor_opts_t *opts,
                         tsdb_compactor_t **out)
{
    if (!db || !out) return TSDB_ERR_INVAL;

    tsdb_compactor_t *c = calloc(1, sizeof(*c));
    if (!c) return TSDB_ERR_NOMEM;

    c->db         = db;
    c->threshold  = COMPACT_THRESHOLD_DEFAULT;
    c->interval_ns= 5000000000LL;  /* 5 s */
    c->nworkers   = 1;
    c->quit       = 0;

    if (opts) {
        if (opts->min_blocks_to_compact > 0)
            c->threshold  = opts->min_blocks_to_compact;
        if (opts->interval_ns > 0)
            c->interval_ns = opts->interval_ns;
        if (opts->worker_threads > 0)
            c->nworkers   = opts->worker_threads;
        else if (opts->worker_threads < 0)
            c->nworkers   = 0;        /* manual mode: no background workers; the
                                         caller drives tsdb_compactor_run_once
                                         (deterministic, race-free for tests) */
    }

    /* Per-table mtime memo — kdb+ inspired "stable data stays stable": if
     * none of a table's partition dirs' mtimes has changed (max-partition-mtime
     * key, which a flush into an existing partition DOES bump), there's no new
     * flush so no work to do, skip the inner enumeration entirely.  Default ON;
     * env TSDB_COMPACTION_MEMO=0 disables (useful when chasing a bug to force
     * a full re-scan every cycle). */
    {
        const char *e = getenv("TSDB_COMPACTION_MEMO");
        c->memo_enabled = !(e && e[0] == '0');
        pthread_mutex_init(&c->memo_mtx, NULL);
        c->memo = NULL; c->memo_n = 0; c->memo_cap = 0;
    }

    /* Adaptive backoff threshold from storage class.  Numbers tuned from
     * the 4-node lvm1 cluster (HDD-backed) where a writer steady-state
     * sustained ~44 flushes/s (2000 tables × ~22 s per table to fill 8192
     * rows) — 50 is high enough that idle compactor cycles still fire,
     * low enough to prevent contention during bursts.  Env override:
     * TSDB_COMPACTION_BUSY_THRESHOLD=0 → never pause. */
    {
        tsdb_iopolicy_t pol = tsdb_db_iopolicy(db);
        switch (pol) {
        case TSDB_IOPOLICY_HDD: c->busy_threshold = 50;  break;
        case TSDB_IOPOLICY_SAS: c->busy_threshold = 100; break;
        case TSDB_IOPOLICY_SSD:
        default:                c->busy_threshold = 0;   break;
        }
        const char *e = getenv("TSDB_COMPACTION_BUSY_THRESHOLD");
        if (e && *e) c->busy_threshold = atoi(e);
    }

    pthread_mutex_init(&c->stats_mtx, NULL);

    c->workers = c->nworkers ? calloc((size_t)c->nworkers, sizeof(pthread_t)) : NULL;
    if (c->nworkers && !c->workers) {
        pthread_mutex_destroy(&c->stats_mtx);
        free(c);
        return TSDB_ERR_NOMEM;
    }

    for (int i = 0; i < c->nworkers; i++) {
        if (pthread_create(&c->workers[i], NULL, compactor_worker, c) != 0) {
            /* Stop already-started workers. */
            c->quit = 1;
            for (int j = 0; j < i; j++) pthread_join(c->workers[j], NULL);
            free(c->workers);
            pthread_mutex_destroy(&c->stats_mtx);
            free(c);
            return TSDB_ERR_IO;
        }
    }

    *out = c;
    return TSDB_OK;
}

void tsdb_compactor_stop(tsdb_compactor_t *c) {
    if (!c) return;

    c->quit = 1;
    if (c->workers) {
        for (int i = 0; i < c->nworkers; i++) {
            pthread_join(c->workers[i], NULL);
        }
        free(c->workers);
        c->workers = NULL;
    }

    pthread_mutex_destroy(&c->stats_mtx);
    pthread_mutex_destroy(&c->memo_mtx);
    free(c->memo);
    free(c);
}

int tsdb_compactor_run_once(tsdb_compactor_t *c) {
    if (!c) return TSDB_ERR_INVAL;
    return compactor_run_once_impl(c);
}

void tsdb_compactor_stats(const tsdb_compactor_t *c, tsdb_compactor_stats_t *out) {
    if (!c || !out) return;
    /* Cast away const for the mutex — we only read the protected data. */
    tsdb_compactor_t *nc = (tsdb_compactor_t *)c;
    pthread_mutex_lock(&nc->stats_mtx);
    *out = c->stats;
    pthread_mutex_unlock(&nc->stats_mtx);
}

/* ---- DB integration ------------------------------------------------------- */
/*
 * NOTE: there is no tsdb_db_set_compactor() wrapper.
 * Callers must use tsdb_compactor_start() / tsdb_compactor_stop() directly
 * so the handle is not lost and background threads can be joined on teardown.
 * See the test in tests/test_compaction.c for the canonical usage pattern.
 */

void tsdb_compact_lock(void *table_internal) {
    if (!table_internal) return;
    tsdb_table_internal_t *t = (tsdb_table_internal_t *)table_internal;
    pthread_mutex_t *m = tsdb_tbl_compact_mtx(t);
    if (m) pthread_mutex_lock(m);
}

void tsdb_compact_unlock(void *table_internal) {
    if (!table_internal) return;
    tsdb_table_internal_t *t = (tsdb_table_internal_t *)table_internal;
    pthread_mutex_t *m = tsdb_tbl_compact_mtx(t);
    if (m) pthread_mutex_unlock(m);
}
