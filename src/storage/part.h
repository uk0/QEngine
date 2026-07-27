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
#define TSDB_IDX_VERSION  4            /* writer version: v4 adds durable max commit-seq
                                        * (WAL redo checkpoint); v3 added per-block stats */

/* Block header on-disk size. */
#define TSDB_BLOCK_HEADER_SIZE  32u

/* Block flag bits (bytes 6..7 of the BlockHeader, little-endian u16).
 *
 * Bits 0..2 are owned by the codec layer (TSDB_BF_OUTER_LZ / NOT_NULL /
 * HAS_BLOOM in compress/codec.h).  Block-layer flags start at bit 3. */
#define TSDB_BLOCK_FLAG_HAS_CRC   (1u << 3)  /* compressed data is followed by
                                                4 bytes of CRC32C; reader must
                                                verify before decoding.  Old
                                                blocks have flag=0 and no
                                                trailer (forward-compat). */
#define TSDB_BLOCK_CRC_TRAILER_SIZE 4u

/* IN-MEMORY ONLY (never written to a block header, never read from one).
 * Set by tsdb_part_open on a synthesised meta whose offset is UINT64_MAX to
 * distinguish the two kinds of "this column has no block here":
 *   flag clear → ALTER TABLE ADD COLUMN: the rows pre-date the column, so
 *                zero IS the value and the readers zero-fill;
 *   flag set   → HOLE: the column lost a block ts still has (a dropped
 *                raw-block replication push, a half-finished migration), so
 *                the value is UNKNOWN and the readers return
 *                TSDB_ERR_CORRUPT rather than fabricating one. */
#define TSDB_BLOCK_FLAG_HOLE      (1u << 4)

/* Index header sizes.
 *   V1 = legacy (no zone map)
 *   V2 = adds file-level ts zone map
 *   V3 = adds explicit entry_size + stats_variant so stats layout can
 *        evolve without another header bump. */
#define TSDB_IDX_HEADER_SIZE_V1  20u
#define TSDB_IDX_HEADER_SIZE_V2  36u
#define TSDB_IDX_HEADER_SIZE_V3  40u
#define TSDB_IDX_HEADER_SIZE     48u   /* V4: V3 + max_seq u64 at [40..47] */

/* Block index entry on-disk sizes.
 *   V1/V2 entries = 40 bytes (offset,size,count,ts_min,ts_max,bloom).
 *   V3 entries    = 88 bytes (V1/V2 prefix + 48 bytes of column stats). */
#define TSDB_IDX_ENTRY_SIZE_V2  40u
#define TSDB_IDX_ENTRY_SIZE     88u   /* V3 */

/* Stats payload (bytes 40..87 of a V3 BlockIndexEntry) — see comment at
 * the top of part.c for full layout. */
#define TSDB_IDX_STATS_BYTES  48u

/* Column-stats flag bits (bytes 80..81 of a V3 entry, little-endian u16). */
#define TSDB_STATS_HAS_MIN_MAX    0x0001u
#define TSDB_STATS_HAS_SUM        0x0002u
#define TSDB_STATS_HAS_FIRST_LAST 0x0004u

/* Block metadata returned to query layer. */
typedef struct {
    uint64_t offset;   /* byte offset of BlockHeader in .col file */
    uint32_t size;     /* compressed data bytes */
    uint32_t count;    /* number of values */
    int64_t  ts_min;
    int64_t  ts_max;
    uint8_t  codec;
    uint16_t flags;    /* TSDB_BF_* bitmask from block header */
    uint64_t bloom;    /* 64-bit bloom filter (TSDB_BF_HAS_BLOOM must be set); 0 otherwise */

    /* Per-column precomputed statistics (V3 stats payload).
     *
     * Interpretation depends on the column type:
     *   TIMESTAMP/INT64 → min/max/sum/first/last are int64 values
     *                     stored in the 8-byte fields below.
     *   FLOAT64         → same fields hold IEEE 754 doubles (reinterpret).
     *   SYMBOL          → stats_flags == 0 (absent); use bloom instead.
     *
     * stats_flags indicates which fields are valid.  0 means "absent"
     * (V1/V2 entry, a SYMBOL column, or a widened legacy entry). */
    int64_t  stats_min;
    int64_t  stats_max;
    int64_t  stats_sum;
    int64_t  stats_first;
    int64_t  stats_last;
    uint16_t stats_flags;
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
/*
 * Like tsdb_part_flush_ex but stamps each written partition's idx with a
 * durable max commit-sequence (the WAL redo checkpoint).  Every column idx
 * published by this flush records max(existing_max_seq, max_seq), so on
 * reopen the recovery path can skip WAL records whose seq <= that value
 * (already durable in the partition).  Pass max_seq == 0 to leave the
 * checkpoint untouched (used by the non-redo / default flush path).
 */
int tsdb_part_flush_ex2(tsdb_schema_t *s, tsdb_memtable_t *m,
                        struct tsdb_db *db, const char *table_name,
                        uint64_t max_seq);

/*
 * Read the maximum durable commit-sequence checkpoint recorded across all
 * columns of a single partition directory (the max over every <col>.idx
 * v4 header).  Returns 0 if the partition predates v4 (no checkpoint) or
 * has no idx files.  Used by recovery to compute C for the WAL seq>C skip.
 */
uint64_t tsdb_part_max_seq(tsdb_schema_t *s, const char *partition_dir);

/*
 * The ONE canonical idx-header encoder, shared by the flush path and the
 * raw-block replication path so both stamp byte-identical headers and select
 * V3 (max_seq==0, 40 bytes) vs V4 (max_seq>0, 48 bytes) the same way.  `buf`
 * must hold at least TSDB_IDX_HEADER_SIZE bytes.  Returns the header size.
 */
size_t tsdb_part_write_idx_header(uint8_t *buf, uint32_t count,
                                  uint64_t total_rows,
                                  int64_t file_ts_min, int64_t file_ts_max,
                                  uint64_t max_seq);

/*
 * Probe an existing idx file's header (version, entry size, zone map, total
 * rows, durable max_seq), applying the mixed-writer header-size recovery so a
 * V3/V4-mongrel reports values consistent with where its entries live.
 * Returns the (recovered) header size, 0 if absent/short, -1 if corrupt.
 * Out-params may be NULL.  The raw-block writer uses this to PRESERVE a
 * partition's idx version and carry its max_seq forward (no silent downgrade).
 */
int tsdb_part_idx_probe(const char *idx_path,
                        uint16_t *out_version, uint32_t *out_count,
                        uint32_t *out_entry_size, uint64_t *out_total_rows,
                        int64_t *out_file_ts_min, int64_t *out_file_ts_max,
                        uint64_t *out_max_seq);

/*
 * Serialise the read-modify-write publish of any <col>.idx inside ONE
 * partition directory.  Striped by a hash of part_dir over a fixed mutex
 * table, so different partitions never contend.
 *
 * Every <col>.idx publish is a full read-modify-write (read all old entries,
 * append, rewrite via tmp+fsync+rename).  Two of them running concurrently on
 * the same file both read N and both write N+1: one entry is silently lost.
 * When the loser is a NON-ts column and ts survives, that is exactly the
 * multi-column hole this lock exists to prevent.  Both writers of an idx take
 * it: col_writer_close (flush/compaction) and tsdb_rawblock_apply_ex
 * (replication/migration).
 *
 * LOCK ORDER: innermost.  Never acquire compact_mtx, db->lock or batch_mu
 * while holding it.  It is deliberately NOT compact_mtx: the raw-block hook
 * fires from inside tsdb_part_flush_ex2 while the SENDER holds its own
 * compact_mtx across a blocking RPC, so making the RECEIVER's applier take
 * compact_mtx would let two nodes that flush-and-push to each other stall for
 * the full replication timeout and then drop the block — a self-inflicted
 * version of the very failure being fixed.  The sender never holds this lock
 * during the hook (it is taken only around the idx RMW), so no cycle exists.
 */
void tsdb_part_idx_lock(const char *part_dir);
void tsdb_part_idx_unlock(const char *part_dir);

/*
 * Commit test for the partition's visibility marker.
 *
 * Answers: may a ts block with key (ts_min, count) be published into this
 * partition without creating a block the reader cannot pair?  The key is
 * exactly the one exec.c's block matcher uses, and the accept/reject rule is
 * exactly the classification tsdb_part_open's alignment pass applies, so a
 * writer can never publish something the reader would refuse.
 *
 *   TSDB_OK        every non-ts column already carries this key, or has no
 *                  blocks here at all in a partition that ts has already
 *                  published into (ALTER TABLE ADD COLUMN — tsdb_part_open
 *                  zero-fills those by design).
 *   TSDB_ERR_BUSY  at least one column HAS blocks here but not this one, or a
 *                  column has NO blocks in a partition ts has not published
 *                  into either (a whole column's group never arrived, which
 *                  reads back as fabricated zeros).  The caller must not
 *                  publish.  out_missing_col (may be NULL) names the column.
 *   TSDB_ERR_IO    a probe failed; treat as not-ready.
 *
 * Caller MUST hold tsdb_part_idx_lock(part_dir).  Returns TSDB_OK immediately
 * for a single-column schema.
 */
int tsdb_part_ts_publish_ready(tsdb_schema_t *s, const char *part_dir,
                               int64_t ts_min, uint32_t count,
                               char *out_missing_col, size_t cap);

/*
 * Repair an ALREADY-torn partition: lower <ts>.idx to the longest block
 * PREFIX every column can pair with, republished via tmp+fsync+rename with the
 * idx version and max_seq preserved (never downgrades V4).
 *
 * Forward-only and idempotent: a second call retracts 0.  Deletes NOTHING —
 * the .col bytes and the non-ts idx entries beyond the new count stay on disk,
 * so a later push re-lands the missing block through the existing dedup and
 * the partition heals upward.  Lowering the visibility marker is what makes
 * the gap visible to anti-entropy, which compares (count, max_ts) and is
 * otherwise structurally blind to a missing value column.
 *
 * A column with ZERO entries in this partition is treated as an ALTER-added
 * column (the same call the reader makes) and never forces a retraction.
 *
 * NOT called from tsdb_part_open: reads must not mutate.  Call it from a
 * restart-time repair sweep or an operator tool.  *out_retracted (may be NULL)
 * receives the number of ts blocks dropped.
 */
int tsdb_part_ts_retract_unpaired(tsdb_schema_t *s, const char *part_dir,
                                  uint32_t *out_retracted);

/* Opaque partition handle (for reading). */
typedef struct tsdb_part tsdb_part_t;

/* Open a partition directory for reading. */
/* Compute the V3 per-block stats payload (min/max/sum/first/last/flags) for a
 * decoded column chunk.  Exposed so compaction stamps the SAME stats a flush
 * would; SYMBOL and count==0 yield an all-zero payload (stats_flags == 0,
 * meaning "absent"). */
void tsdb_part_compute_block_stats(tsdb_type_t type, const void *raw_vals,
                                   size_t count, tsdb_block_meta_t *out);

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
 * Zero-copy variant of tsdb_part_read_block.
 *
 * On success *out_data points at meta->count decoded values:
 *   - *out_owned == NULL: the block was served ZERO-COPY — *out_data points
 *     straight into the partition's PROT_READ mmap of the .col file (RAW
 *     codec, no outer-LZ wrap, payload naturally aligned for the column
 *     type).  The pointer is read-only and valid only until
 *     tsdb_part_close(p); the caller must keep the partition open across
 *     consumption and must NOT write through or free it.
 *   - *out_owned != NULL: the block needed decoding (or the zero-copy gate
 *     rejected it); *out_owned == *out_data is a malloc'd buffer the caller
 *     frees after use.
 *
 * Integrity checks (header cross-check, CRC trailer) are identical to
 * tsdb_part_read_block.  The fast path is disabled process-wide by setting
 * env TSDB_ZEROCOPY_READ=0 (latched at tsdb_part_open time; default ON).
 */
int tsdb_part_read_block_ref(tsdb_part_t *p, int col_idx,
                             const tsdb_block_meta_t *meta,
                             const void **out_data, void **out_owned);

/*
 * Process-wide zero-copy read counters (monotonic, relaxed): blocks served
 * as direct mmap pointers vs. blocks that fell back to a decode/copy.
 * Test/observability aid; either out-param may be NULL.
 */
void tsdb_part_zerocopy_stats(uint64_t *out_hits, uint64_t *out_fallbacks);

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

/* Accessor: the schema this partition was opened against. */
tsdb_schema_t *tsdb_part_schema(const tsdb_part_t *p);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_PART_H */
