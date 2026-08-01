/* merkle.h — 4-level Merkle tree over one partition's column blocks.
 *
 * Tree structure (bound: 4096 blocks per partition per column):
 *   level 0: root (1 node)
 *   level 1: 16 children of root
 *   level 2: 256 children per l1 bucket  (16 × 16 = 256 nodes)
 *   level 3: leaves — one per block (up to 16 × 256 = 4096)
 *
 * Hashing: XXH64 (64-bit, fast non-crypto).
 *   leaf[i]  = xxh64(block_bytes of block i, 0)
 *   l2[j]    = xxh64(concat(leaves[j*16 .. j*16+15]), 0)  — fill missing with 0
 *   l1[k]    = xxh64(concat(l2[k*16 .. k*16+15]), 0)
 *   root     = xxh64(concat(l1[0..15]), 0)
 *
 * If nleaves < 4096 the excess entries are 0; the tree is still valid
 * because empty nodes hash as 0 and bubble up.
 *
 * LIMIT: 4096 blocks per (partition, column).  With TSDB_BLOCK_POINTS=8192
 * rows this caps a single partition at ~33M rows.  Returns TSDB_ERR_OVERFLOW
 * if exceeded.
 */
#ifndef TSDB_CLUSTER_MERKLE_H
#define TSDB_CLUSTER_MERKLE_H

#include "../storage/part.h"
#include "../../include/tsdb.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_MERKLE_MAX_LEAVES  4096  /* hard cap: 16 * 256 */
#define TSDB_MERKLE_L1_SIZE      16
#define TSDB_MERKLE_L2_SIZE     256   /* 16 per l1 × 16 l1 = 256 */

typedef struct {
    int64_t  root;
    int64_t  l1[TSDB_MERKLE_L1_SIZE];
    int64_t  l2[TSDB_MERKLE_L2_SIZE];
    int64_t  leaves[TSDB_MERKLE_MAX_LEAVES];
    int      nleaves;
} tsdb_merkle_t;

/*
 * Build a Merkle tree from all blocks of one column in a partition.
 * p       : opened partition (tsdb_part_open)
 * col_idx : column index
 * out     : populated on success
 *
 * Returns TSDB_OK, TSDB_ERR_OVERFLOW (too many blocks), or other error.
 */
int tsdb_merkle_build(const tsdb_part_t *p, int col_idx, tsdb_merkle_t *out);

/*
 * Build a Merkle tree from raw .col file bytes without an open partition.
 * col_map     : mmap'd (or malloc'd) .col file bytes
 * col_map_size: byte length
 * metas       : block metadata array (from .idx)
 * nmetas      : number of entries
 * out         : populated on success
 */
int tsdb_merkle_build_raw(const uint8_t *col_map, size_t col_map_size,
                           const tsdb_block_meta_t *metas, size_t nmetas,
                           tsdb_merkle_t *out);

/*
 * Compute the diff: which leaf indices (block indices) differ between
 * a local and a remote tree.
 *
 * Both trees must have been built from the same (table, partition, col).
 * diff_indices: malloc'd by callee; caller must free(*diff_indices).
 * *ndiff      : number of differing leaf indices.
 *
 * Returns TSDB_OK.
 */
int tsdb_merkle_diff(const tsdb_merkle_t *local, const tsdb_merkle_t *remote,
                     uint16_t **diff_indices, size_t *ndiff);

/* ==== Row-range digest (re-blocking-invariant leaf) ======================= *
 *
 * The Merkle LEAF above hashes a block's BYTES (xxh64 over col_map+data_off).
 * Two byte-identical-CONTENT replicas that compacted independently have
 * DIFFERENT block bytes, so that leaf reports them divergent — a repair storm,
 * which is why tsdb_node_main.c says the merkle block-sync path is not wired.
 *
 * The row-range digest replaces that leaf.  It keys on ROW CONTENT, not block
 * identity: rows are bucketed by a fixed ts window (the caller's `span`, epoch-
 * aligned exactly like time_bucket / the UTC partition boundaries) and each
 * bucket carries (count, hsum, hxor) — an ORDER-INDEPENDENT multiset
 * fingerprint of its rows' canonical (ts, values) tuples.  A compacted replica
 * and an un-compacted one that hold the SAME ROWS therefore produce the SAME
 * per-bucket digest, and two replicas that each miss a DIFFERENT interior batch
 * (equal count, equal max_ts — the anti-entropy blind spot) produce DIFFERENT
 * per-bucket digests that name exactly the divergent bucket(s).
 *
 * Bucket count bound: production keys on the table's partition span, so buckets
 * align 1:1 with partitions and the count is bounded by the same retention that
 * bounds partition count; TSDB_ROWDIGEST_MAX_BUCKETS (>= AEBF_MAX_BUCKETS) caps
 * it and the builder returns TSDB_ERR_OVERFLOW past that, so the caller degrades
 * to the count/max_ts-only decision rather than emitting a truncated vector.
 */
#define TSDB_ROWDIGEST_MAX_BUCKETS 4096

typedef struct {
    int64_t  bstart;   /* bucket start ts, epoch-aligned to `span`            */
    uint64_t count;    /* rows whose ts falls in [bstart, bstart+span)        */
    uint64_t hsum;     /* sum  (mod 2^64) of the bucket's per-row hashes       */
    uint64_t hxor;     /* xor           of the bucket's per-row hashes         */
} tsdb_rowdigest_bucket_t;

/* Canonical, node-portable content hash of the result-set row at the cursor.
 *
 * Hashes every column in schema order: int/timestamp by value, float by its
 * IEEE bits, SYMBOL by its RESOLVED STRING (never the node-local dictionary
 * code, which differs across replicas), NULL by a distinct sentinel.  Two rows
 * with identical (ts, values) hash equal on any node; a single differing field
 * changes the hash.  `ncols` must be tsdb_result_ncols(res).  Does NOT advance
 * the cursor. */
uint64_t tsdb_rowdigest_row_hash(tsdb_result_t *res, int ncols);

/* Build the per-bucket digest vector by consuming `res` (walks it to the end
 * with tsdb_result_next).  `span` is the bucket width in ns (> 0).  On success
 * *out is a malloc'd vector of *out_n buckets sorted ascending by bstart (caller
 * frees).  Returns TSDB_OK, TSDB_ERR_INVAL, TSDB_ERR_NOMEM, or TSDB_ERR_OVERFLOW
 * if the bucket count would exceed TSDB_ROWDIGEST_MAX_BUCKETS. */
int tsdb_rowdigest_from_result(tsdb_result_t *res, int64_t span,
                               tsdb_rowdigest_bucket_t **out, size_t *out_n);

/* Divergent buckets between two digest vectors (each sorted ascending by
 * bstart).  A bucket diverges when it is present in one vector and absent from
 * the other, OR present in both with a differing (count, hsum, hxor).  Divergent
 * bstart values are written to out_bstarts[0..min(*out_ndiv,cap)-1]; *out_ndiv
 * is the TOTAL divergent count (may exceed cap).  Returns TSDB_OK. */
int tsdb_rowdigest_diff(const tsdb_rowdigest_bucket_t *a, size_t na,
                        const tsdb_rowdigest_bucket_t *b, size_t nb,
                        int64_t *out_bstarts, size_t cap, size_t *out_ndiv);

/* Wire size of a digest response: a u32 bucket count + up to the cap of
 * 32-byte {bstart,count,hsum,hxor} records.  The RPC caller sizes its receive
 * buffer to this so a full vector never truncates. */
#define TSDB_ROWDIGEST_REC_SIZE 32
#define TSDB_ROWDIGEST_WIRE_MAX (4 + TSDB_ROWDIGEST_MAX_BUCKETS * TSDB_ROWDIGEST_REC_SIZE)

/* Serialize `v[0..n)` to the wire body (nbuckets u32, then n 32-byte records,
 * host byte order — the cluster is homogeneous, matching every other RPC body).
 * Returns the number of bytes written (>0), or -1 if it would exceed `cap`. */
int tsdb_rowdigest_serialize(const tsdb_rowdigest_bucket_t *v, size_t n,
                             uint8_t *buf, size_t cap);

/* Parse a body produced by tsdb_rowdigest_serialize.  On success *out is a
 * malloc'd vector of *out_n buckets (caller frees; NULL/0 for an empty vector).
 * Returns TSDB_OK, or TSDB_ERR_CORRUPT if the body is short or inconsistent. */
int tsdb_rowdigest_deserialize(const uint8_t *buf, uint32_t len,
                               tsdb_rowdigest_bucket_t **out, size_t *out_n);

/* Compute THIS node's row-range digest for `table_name` over [ts_lo, ts_hi]
 * (INT64_MIN / INT64_MAX = open), bucketed by `span` ns.  Implemented in
 * db_cluster.c.  Reads LOCAL storage only: the SELECT runs in scatter-local
 * mode so a mirrored plain table never scatters and answers with another
 * node's rows (the same trap tsdb_cluster_local_table_stats avoids).  Both the
 * requester and the LOCAL_TABLE_DIGEST RPC handler call this, so the two sides
 * bucket and hash identically.  *out is malloc'd (caller frees).  Returns
 * TSDB_OK, TSDB_ERR_OVERFLOW (bucket count over the cap), or TSDB_ERR_*. */
int tsdb_cluster_local_table_digest(tsdb_db_t *db, const char *table_name,
                                    int64_t span, int64_t ts_lo, int64_t ts_hi,
                                    tsdb_rowdigest_bucket_t **out, size_t *out_n);

/* ---- Digest repair: content-dedup bucket merge (implemented in db_cluster.c) *
 *
 * Split from the RPC/conn plumbing so the merge is testable in-process against
 * two local DBs — one standing in for the peer — with no live cluster. */

/* Sorted content-hash set of the rows THIS node holds in [bstart, bend]
 * (local-only read).  *out is malloc'd (caller frees; NULL/0 when empty).
 * Returns TSDB_OK or TSDB_ERR_*. */
int tsdb_ae_local_bucket_hashes(tsdb_db_t *db, const char *table,
                                int64_t bstart, int64_t bend,
                                uint64_t **out, size_t *out_n);

/* Insert (local_only) every row of `peer_res` whose canonical content hash is
 * NOT in the sorted set lset[0..ln).  Never deletes a local row, never inserts
 * a content-duplicate.  *out_inserted (may be NULL) = rows added.  Returns
 * TSDB_OK or TSDB_ERR_*. */
int tsdb_ae_merge_result_dedup(tsdb_db_t *db, const char *table,
                               tsdb_result_t *peer_res,
                               const uint64_t *lset, size_t ln,
                               int *out_inserted);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_MERKLE_H */
