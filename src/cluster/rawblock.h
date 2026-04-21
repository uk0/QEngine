/* rawblock.h — Raw compressed-block replication.
 *
 * On flush, the primary encodes each column block once and sends the
 * compressed bytes verbatim to replicas via TSDB_RPC_RAW_BLOCK_PUSH.
 * Replicas write the bytes directly to .col / .idx files — no decode,
 * no re-compress.  This replaces the row-level WRITE_BATCH path when
 * TSDB_REPLICATION_MODE=raw is set.
 *
 * Wire payload (RAW_BLOCK_PUSH, little-endian):
 *   [table_len u8][table utf8]
 *   [part_day u32]          -- YYYYMMDD
 *   [col_idx u16]
 *   [codec u8] [flags u16]
 *   [count u32] [ts_min i64] [ts_max i64]
 *   [block_bytes_len u32] [block_bytes]   -- compressed data only, NO BlockHeader
 */
#ifndef TSDB_CLUSTER_RAWBLOCK_H
#define TSDB_CLUSTER_RAWBLOCK_H

#include "../storage/part.h"
#include "../storage/db.h"
#include "cluster.h"
#include "../../include/tsdb.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Struct for one raw-block push message. */
typedef struct {
    char      table[64];
    uint32_t  part_day;          /* YYYYMMDD */
    uint16_t  col_idx;
    uint8_t   codec;
    uint16_t  flags;
    uint32_t  count;
    int64_t   ts_min;
    int64_t   ts_max;
    /* Per-column block stats (forwarded to the replica so its index
     * mirrors the primary's V3 entry exactly).  Fields carry the same
     * semantics as the `stats_*` fields on tsdb_block_meta_t.
     * stats_flags == 0 means the primary did not compute stats for this
     * block (e.g. SYMBOL column) — the replica writes zeros. */
    int64_t   stats_min;
    int64_t   stats_max;
    int64_t   stats_sum;
    int64_t   stats_first;
    int64_t   stats_last;
    uint16_t  stats_flags;
    uint32_t  block_bytes_len;
    uint8_t  *block_bytes;       /* caller-owned; NOT freed by parse/serialize */
} tsdb_rawblock_push_t;

/*
 * Serialize into a single heap-allocated buffer ready for RPC payload.
 * *buf is malloc'd by callee; caller must free(*buf).
 */
int tsdb_rawblock_serialize(const tsdb_rawblock_push_t *r,
                             uint8_t **buf, size_t *len);

/*
 * Parse a RAW_BLOCK_PUSH payload buffer.
 * out->block_bytes points into buf (not a copy); keep buf alive as long
 * as you use out->block_bytes.
 */
int tsdb_rawblock_parse(const uint8_t *buf, size_t len,
                         tsdb_rawblock_push_t *out);

/*
 * Primary calls this from the flush-path on_raw_block hook.
 * Fans out to all alive peers concurrently; waits for quorum_w ACKs
 * (including the already-written local copy which counts as 1).
 */
int tsdb_rawblock_replicate(tsdb_cluster_t *c,
                             const char *table, uint32_t day, uint16_t col_idx,
                             const tsdb_block_meta_t *meta,
                             const uint8_t *block_bytes, size_t block_len,
                             int quorum_w);

/*
 * Replica-side handler: write the block verbatim to
 *   <db_data_dir>/<table>/<day_str>/<col_name>.col + .idx
 *
 * The col name is resolved from the replica's local schema (which must
 * already exist via prior SCHEMA_SYNC).
 */
int tsdb_rawblock_apply(tsdb_db_t *db, const tsdb_rawblock_push_t *r);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_RAWBLOCK_H */
