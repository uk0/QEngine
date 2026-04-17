/* part.h — on-disk partition management.
 *
 * A partition corresponds to one calendar day (UTC) of data.
 * Layout: <schema->dir>/<YYYYMMDD>/<colname>.col  — compressed data blocks
 *         <schema->dir>/<YYYYMMDD>/<colname>.idx  — block index
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
 * IdxHeader (20 bytes, little-endian):
 *   magic      u32  = 'IDX1' (0x31584449)
 *   count      u32  (number of BlockIndexEntry records)
 *   version    u16
 *   _pad       u16
 *   total_rows u64
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
#define TSDB_IDX_VERSION  1

/* Block header on-disk size. */
#define TSDB_BLOCK_HEADER_SIZE  32u

/* Index header on-disk size. */
#define TSDB_IDX_HEADER_SIZE    20u

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
} tsdb_block_meta_t;

/*
 * Flush the memtable to disk.
 * Creates partition subdirectories as needed; groups rows by day.
 * Each group is chunked into blocks of at most TSDB_BLOCK_POINTS points.
 */
int tsdb_part_flush(tsdb_schema_t *s, tsdb_memtable_t *m);

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

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_PART_H */
