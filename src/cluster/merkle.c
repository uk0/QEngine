/* merkle.c — 4-level Merkle tree for cross-cluster block diffing.
 *
 * Uses an embedded ~50-line XXH64 implementation (public domain, adapted from
 * the reference XXH spec) to avoid external dependencies.
 */

#include "merkle.h"
#include "../../include/tsdb.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* ---- XXH64 (single-file embedding) --------------------------------------- */

#define XXH_PRIME1 11400714785074694791ULL
#define XXH_PRIME2 14029467366897019727ULL
#define XXH_PRIME3  1609587929392839161ULL
#define XXH_PRIME4  9650029242287828579ULL
#define XXH_PRIME5  2870177450012600261ULL

static inline uint64_t xxh_rotl64(uint64_t v, int r) {
    return (v << r) | (v >> (64 - r));
}
static inline uint64_t xxh_round(uint64_t acc, uint64_t in) {
    acc += in * XXH_PRIME2;
    acc  = xxh_rotl64(acc, 31);
    acc *= XXH_PRIME1;
    return acc;
}
static inline uint64_t xxh_merge(uint64_t acc, uint64_t val) {
    val  = xxh_round(0, val);
    acc ^= val;
    acc  = acc * XXH_PRIME1 + XXH_PRIME4;
    return acc;
}

static uint64_t xxh64(const void *input, size_t len, uint64_t seed) {
    const uint8_t *p   = (const uint8_t *)input;
    const uint8_t *end = p + len;
    uint64_t h64;

    if (len >= 32) {
        const uint8_t *limit = end - 32;
        uint64_t v1 = seed + XXH_PRIME1 + XXH_PRIME2;
        uint64_t v2 = seed + XXH_PRIME2;
        uint64_t v3 = seed;
        uint64_t v4 = seed - XXH_PRIME1;

        do {
            uint64_t lane;
            memcpy(&lane, p, 8); v1 = xxh_round(v1, lane); p += 8;
            memcpy(&lane, p, 8); v2 = xxh_round(v2, lane); p += 8;
            memcpy(&lane, p, 8); v3 = xxh_round(v3, lane); p += 8;
            memcpy(&lane, p, 8); v4 = xxh_round(v4, lane); p += 8;
        } while (p <= limit);

        h64  = xxh_rotl64(v1, 1) + xxh_rotl64(v2, 7)
             + xxh_rotl64(v3, 12) + xxh_rotl64(v4, 18);
        h64  = xxh_merge(h64, v1);
        h64  = xxh_merge(h64, v2);
        h64  = xxh_merge(h64, v3);
        h64  = xxh_merge(h64, v4);
    } else {
        h64 = seed + XXH_PRIME5;
    }

    h64 += (uint64_t)len;

    while (p + 8 <= end) {
        uint64_t k1;
        memcpy(&k1, p, 8);
        k1  = xxh_round(0, k1);
        h64 ^= k1;
        h64  = xxh_rotl64(h64, 27) * XXH_PRIME1 + XXH_PRIME4;
        p   += 8;
    }
    if (p + 4 <= end) {
        uint32_t k1;
        memcpy(&k1, p, 4);
        h64 ^= (uint64_t)k1 * XXH_PRIME1;
        h64  = xxh_rotl64(h64, 23) * XXH_PRIME2 + XXH_PRIME3;
        p   += 4;
    }
    while (p < end) {
        h64 ^= (uint64_t)(*p) * XXH_PRIME5;
        h64  = xxh_rotl64(h64, 11) * XXH_PRIME1;
        p++;
    }

    h64 ^= h64 >> 33;
    h64 *= XXH_PRIME2;
    h64 ^= h64 >> 29;
    h64 *= XXH_PRIME3;
    h64 ^= h64 >> 32;
    return h64;
}

/* ---- Merkle tree construction helpers ------------------------------------- */

/*
 * Hash a concatenation of 16 int64_t values as a single 128-byte buffer.
 */
static int64_t hash_node_array(const int64_t *arr, int count) {
    /* Pad to 16 entries with zeros. */
    uint64_t buf[16] = {0};
    int n = count < 16 ? count : 16;
    for (int i = 0; i < n; i++) buf[i] = (uint64_t)arr[i];
    return (int64_t)xxh64(buf, 16 * sizeof(uint64_t), 0);
}

static int tsdb_merkle_build_from_metas(const uint8_t *col_map,
                                         size_t col_map_size,
                                         const tsdb_block_meta_t *metas,
                                         size_t nmetas,
                                         tsdb_merkle_t *out)
{
    memset(out, 0, sizeof(*out));

    if (nmetas > TSDB_MERKLE_MAX_LEAVES)
        return TSDB_ERR_OVERFLOW;

    out->nleaves = (int)nmetas;

    /* Build leaves: hash each block's compressed bytes. */
    for (size_t i = 0; i < nmetas; i++) {
        uint64_t off  = metas[i].offset;
        uint32_t size = metas[i].size;
        /* Block on disk: 32-byte header + size bytes of compressed data.
         * We hash the compressed data only (same bytes sent over the wire). */
        size_t data_off = off + 32; /* TSDB_BLOCK_HEADER_SIZE */
        if (col_map && data_off + size <= col_map_size) {
            out->leaves[i] = (int64_t)xxh64(col_map + data_off, size, 0);
        } else {
            /* Block bytes not available; use size+count as proxy. */
            uint64_t proxy = ((uint64_t)size << 32) | (uint64_t)metas[i].count;
            out->leaves[i] = (int64_t)xxh64(&proxy, sizeof(proxy), 0);
        }
    }

    /* Build l2 (256 nodes, each covers 16 leaves). */
    for (int j = 0; j < TSDB_MERKLE_L2_SIZE; j++) {
        out->l2[j] = hash_node_array(&out->leaves[j * 16], 16);
    }

    /* Build l1 (16 nodes, each covers 16 l2 nodes). */
    for (int k = 0; k < TSDB_MERKLE_L1_SIZE; k++) {
        out->l1[k] = hash_node_array(&out->l2[k * 16], 16);
    }

    /* Root. */
    out->root = hash_node_array(out->l1, TSDB_MERKLE_L1_SIZE);

    return TSDB_OK;
}

/* ---- Public API ----------------------------------------------------------- */

int tsdb_merkle_build(const tsdb_part_t *p, int col_idx, tsdb_merkle_t *out)
{
    if (!p || !out) return TSDB_ERR_INVAL;

    tsdb_block_meta_t *metas = NULL;
    size_t nmetas = 0;
    int rc = tsdb_part_col_blocks((tsdb_part_t *)p, col_idx, &metas, &nmetas);
    if (rc != TSDB_OK) return rc;

    /* Use the mmap'd .col file bytes for accurate block hashing.
     * tsdb_part_col_map() exposes the read-only mmap without exposing
     * the opaque tsdb_part_t internals. */
    const uint8_t *col_map = NULL;
    size_t         col_map_size = 0;
    tsdb_part_col_map(p, col_idx, &col_map, &col_map_size);

    rc = tsdb_merkle_build_from_metas(col_map, col_map_size, metas, nmetas, out);
    free(metas);
    return rc;
}

int tsdb_merkle_build_raw(const uint8_t *col_map, size_t col_map_size,
                           const tsdb_block_meta_t *metas, size_t nmetas,
                           tsdb_merkle_t *out)
{
    if (!out) return TSDB_ERR_INVAL;
    return tsdb_merkle_build_from_metas(col_map, col_map_size, metas, nmetas, out);
}

int tsdb_merkle_diff(const tsdb_merkle_t *local, const tsdb_merkle_t *remote,
                     uint16_t **diff_indices, size_t *ndiff)
{
    if (!local || !remote || !diff_indices || !ndiff) return TSDB_ERR_INVAL;

    *diff_indices = NULL;
    *ndiff = 0;

    /* Fast path: same root → no diff. */
    if (local->root == remote->root) return TSDB_OK;

    /* Walk the tree top-down to find differing leaves efficiently. */
    uint16_t *result = malloc(TSDB_MERKLE_MAX_LEAVES * sizeof(uint16_t));
    if (!result) return TSDB_ERR_NOMEM;

    size_t n = 0;

    /* Check l1 nodes. */
    for (int k = 0; k < TSDB_MERKLE_L1_SIZE; k++) {
        if (local->l1[k] == remote->l1[k]) continue;

        /* Check l2 nodes within this l1 bucket. */
        for (int j = 0; j < 16; j++) {
            int l2_idx = k * 16 + j;
            if (local->l2[l2_idx] == remote->l2[l2_idx]) continue;

            /* Check individual leaves within this l2 bucket. */
            for (int i = 0; i < 16; i++) {
                int leaf_idx = l2_idx * 16 + i;
                if (leaf_idx >= TSDB_MERKLE_MAX_LEAVES) break;

                int64_t loc = (leaf_idx < local->nleaves)  ? local->leaves[leaf_idx]  : 0;
                int64_t rem = (leaf_idx < remote->nleaves) ? remote->leaves[leaf_idx] : 0;

                /* Also emit indices present in one tree but not the other. */
                int in_local  = leaf_idx < local->nleaves;
                int in_remote = leaf_idx < remote->nleaves;

                if (loc != rem || in_local != in_remote) {
                    if (n < TSDB_MERKLE_MAX_LEAVES)
                        result[n++] = (uint16_t)leaf_idx;
                }
            }
        }
    }

    if (n == 0) {
        free(result);
        *diff_indices = NULL;
    } else {
        *diff_indices = result;
    }
    *ndiff = n;
    return TSDB_OK;
}
