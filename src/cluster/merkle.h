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

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_MERKLE_H */
