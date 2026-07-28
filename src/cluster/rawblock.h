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
 *   [issuer u64]                          -- OPTIONAL tail, see tsdb_rawblock_push_t
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
    /* The block's PARTITION-LOCAL ordinal, carried in the six padding bytes the
     * stats area already had (wire [42..47] == idx entry [82..87] — the areas
     * are byte-for-byte the same layout, so this needs no wire-length change).
     *
     * It is the identity: (size, count, ts_min, ts_max) is NOT unique — a run of
     * rows carrying one timestamp yields several distinct blocks whose ts
     * payloads are byte-identical — so the applier could neither tell a retried
     * push from a genuinely different block nor place an out-of-order one.
     *
     * mark == 0 (the memset default) means the sender predates the field; the
     * applier then falls back to its historical append + tail-compare. */
    tsdb_block_ord_t ord;
    uint32_t  block_bytes_len;
    uint8_t  *block_bytes;       /* caller-owned; NOT freed by parse/serialize */
    /* WHO issued `ord`.  An ordinal names a block group inside the partition
     * that issued it, so it means nothing without the issuer: two nodes flushing
     * the same timestamp grid into one table+day both start at ordinal 0 and
     * their ts blocks are byte-identical, so a receiver keying its translation
     * on the ordinal alone collapses the two groups onto one local ordinal —
     * ACKing and discarding the second sender's ts block, and answering the
     * survivor with the second sender's values.
     *
     * Carried as 8 trailing bytes AFTER block_bytes, which is additive in both
     * directions: tsdb_rawblock_parse has always ignored trailing bytes, so an
     * older receiver skips it, and a payload without it parses as issuer 0.
     * 0 means "unknown / this node's own stream" — what the migration importer,
     * the restore replay and the tests produce. */
    uint64_t  issuer;
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

/*
 * Enforce tsdb_part_ts_publish_ready before publishing a TS block: refuse to
 * advance the partition's visibility marker past a group whose other columns
 * have not all landed here.
 */
#define TSDB_RB_VERIFY_TS  0x1u

/*
 * As tsdb_rawblock_apply, with control over that commit test.
 *
 * The RPC receiver (rpc.c, TSDB_RPC_RAW_BLOCK_PUSH) passes TSDB_RB_VERIFY_TS.
 * That is the ONLY path where blocks arrive from an untrusted sender whose
 * pushes can fail independently, and it is the only path that can be given a
 * ts block for a group whose siblings will never arrive.
 *
 * tsdb_rawblock_apply (flags == 0) stays the raw primitive for LOCAL bulk
 * callers — the migration importer and the tests — which deliberately land
 * blocks in stream order (a v2 stream emits ts FIRST) and reconcile at the end
 * of the run.  Enforcing the test inline there would reject every ts block of
 * an existing stream file and break a documented feature.
 *
 * Returns TSDB_ERR_BUSY when the test defers a ts publish; NOTHING is written
 * in that case, so the caller may simply retry once the sibling lands.
 */
int tsdb_rawblock_apply_ex(tsdb_db_t *db, const tsdb_rawblock_push_t *r,
                           uint32_t flags);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_RAWBLOCK_H */
