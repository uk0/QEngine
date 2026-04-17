/* part.h — on-disk partition management.
 *
 * A partition corresponds to one calendar day (UTC) by default, or one
 * calendar hour (UTC) when the table schema declares HOUR granularity.
 * Layout: <schema->dir>/<PARTSTR>/<colname>.col  — compressed data blocks
 *         <schema->dir>/<PARTSTR>/<colname>.idx  — block index
 *
 *   PARTSTR = "YYYYMMDD"   (DAY,  length 8)
 *           | "YYYYMMDDHH" (HOUR, length 10)
 *
 * BlockHeader (32 bytes, little-endian, no padding):
 *   magic   u32  = 'BLK1' (0x314B4C42)
 *   codec   u8
 *   _pad    u8
 *   flags   u16
 *   count   u32
 *   ts_min  i64
 *   ts_max  i64
 *   size    u32   (compressed bytes following this header)
 *   [CompressedData: size bytes]
 *
 * IdxHeader v1 (20 bytes, legacy):
 *   magic       u32  = 'IDX1' (0x31584449)
 *   count       u32  (number of BlockIndexEntry records)
 *   version     u16  = 1
 *   _pad        u16
 *   total_rows  u64
 *
 * IdxHeader v2 (36 bytes, current writer — 2026-04):
 *   ... v1 fields ...
 *   file_ts_min i64  (zone map — rows < this definitely absent)
 *   file_ts_max i64  (zone map — rows > this definitely absent)
 *   Enables whole-partition skipping in the query planner without
 *   walking every block's BlockIndexEntry. Reader accepts both v1 and
 *   v2; v1 callers fall back to per-block ts_min/ts_max computation.
 *
 * BlockIndexEntry (40 bytes, little-endian):
 *   offset    u64
 *   size      u32
 *   count     u32
 *   ts_min    i64
 *   ts_max    i64
 *   _reserved u64
 */
#ifndef TSDB_STORAGE_PART_H
#define TSDB_STORAGE_PART_H

#include "schema.h"
#include "memtable.h"
#include "../../include/tsdb.h"
#include "../core/types.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Magic constants. */
#define TSDB_BLOCK_MAGIC  0x314B4C42u  /* "BLK1" LE */
#define TSDB_IDX_MAGIC    0x31584449u  /* "IDX1" LE */
#define TSDB_IDX_VERSION  2            /* writer version (adds file-level zone map) */

/* Block header on-disk size. */
#define TSDB_BLOCK_HEADER_SIZE  32u

/* Index header sizes. V1 = legacy (no zone map); V2 = current writer. */
#define TSDB_IDX_HEADER_SIZE_V1  20u
#define TSDB_IDX_HEADER_SIZE     36u

/* Block index entry on-disk size. */
#define TSDB_IDX_ENTRY_SIZE     40u

/* Block metadata returned to query layer. */
typedef struct {
    uint64_t offset;   /* byte offset of BlockHeader in .col file */
    uint32_t size;     /* compressed data bytes */
    uint32_t count;    /* number of values */
    int64_t  ts_min;
    int64_t  ts_max;
    uint8_t  codec;
    uint16_t flags;    /* TSDB_BF_* bitmask from block header */
} tsdb_block_meta_t;

/*
 * Flush the memtable to disk.
 * Creates partition subdirectories as needed; groups rows by day.
 * Each group is chunked into blocks of at most TSDB_BLOCK_POINTS points.
 *
 * db / table_name are only used for the on_raw_block hook.
 * Pass (NULL, NULL) for standalone (non-cluster) usage.
 */
int tsdb_part_flush(tsdb_schema_t *s, tsdb_memtable_t *m);
int tsdb_part_flush_ex(tsdb_schema_t *s, tsdb_memtable_t *m,
                       struct tsdb_db *db, const char *table_name);

/* Opaque partition handle (for reading). */
typedef struct tsdb_part tsdb_part_t;

/* Open a partition directory for reading. */
int  tsdb_part_open(tsdb_schema_t *s, const char *partition_dir, tsdb_part_t **out);

/* Close a partition handle. */
void tsdb_part_close(tsdb_part_t *p);

/*
 * Return all block metadata for a column.
 * *out_arr is malloc'd by callee; caller must free(*out_arr).
 */
int tsdb_part_col_blocks(tsdb_part_t *p, int col_idx,
                         tsdb_block_meta_t **out_arr, size_t *out_n);

/*
 * Decompress one block into out_buf.
 * out_buf must hold at least (meta->count * tsdb_type_width(col_type)) bytes.
 */
int tsdb_part_read_block(tsdb_part_t *p, int col_idx,
                         const tsdb_block_meta_t *meta, void *out_buf);

/*
 * Return a read-only pointer to the mmap'd .col file for a column.
 * *out_map / *out_len are set to NULL/0 if the column file is not mapped.
 * Caller must NOT free the returned pointer — it is owned by the partition.
 */
void tsdb_part_col_map(const tsdb_part_t *p, int col_idx,
                        const uint8_t **out_map, size_t *out_len);

/*
 * File-level zone map: the min/max timestamp covering every row in this
 * partition, across every column. Derived from the IdxHeader v2 `file_ts_*`
 * fields when available, otherwise computed from per-block metadata of
 * the ts column as a one-time fallback.
 *
 * Returns TSDB_OK and fills out_ts_min, out_ts_max on success.
 * Returns TSDB_ERR_NOTFOUND if no data has been written to this partition.
 */
int tsdb_part_zone_map(tsdb_part_t *p, int64_t *out_ts_min, int64_t *out_ts_max);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_PART_H */
