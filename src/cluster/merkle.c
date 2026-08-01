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

/* ==== Row-range digest (re-blocking-invariant leaf) ======================= */

/* Hard cap on columns per row for the fixed-size hash scratch (== TSDB_MAX_COLS,
 * the table-schema limit, so a real row never exceeds it). */
enum { RD_MAX_COLS = 256 };

uint64_t tsdb_rowdigest_row_hash(tsdb_result_t *res, int ncols)
{
    /* Two words per column: a (position | type) tag and the value word.  The
     * tag makes column POSITION and TYPE part of the identity, so two rows that
     * differ only by which column holds a value still hash apart. */
    uint64_t words[2 * RD_MAX_COLS];
    int n = ncols;
    if (n < 0) n = 0;
    if (n > RD_MAX_COLS) n = RD_MAX_COLS;

    int w = 0;
    for (int i = 0; i < n; i++) {
        uint64_t tag = (uint64_t)(unsigned)i << 8;
        uint64_t cw;
        if (tsdb_result_is_null(res, i)) {
            tag |= 0x00;                       /* NULL is its own type tag   */
            cw = 0x9E3779B97F4A7C15ULL;        /* fixed non-value sentinel   */
        } else {
            switch (tsdb_result_col_type(res, i)) {
            case TSDB_TYPE_TIMESTAMP:
                tag |= 0x01; cw = (uint64_t)tsdb_result_ts(res, i); break;
            case TSDB_TYPE_INT64:
                tag |= 0x02; cw = (uint64_t)tsdb_result_i64(res, i); break;
            case TSDB_TYPE_FLOAT64:
            case TSDB_TYPE_FLOAT32: {
                /* Hash the IEEE bits.  Replication copies the bytes verbatim,
                 * so both replicas hash identical bits for the same value; the
                 * only normalisation is -0.0 -> +0.0 (they compare equal). */
                double d = tsdb_result_f64(res, i);
                uint64_t bits;
                memcpy(&bits, &d, 8);
                if (d == 0.0) bits = 0;
                tag |= 0x03; cw = bits; break;
            }
            case TSDB_TYPE_SYMBOL: {
                /* Hash the RESOLVED STRING, never the node-local dictionary
                 * code — codes are independent across replicas (the same
                 * reason pull_table_delta re-interns by string). */
                const char *s = tsdb_result_sym(res, i);
                tag |= 0x04;
                cw = s ? xxh64(s, strlen(s), 0x53594d424f554cULL) : 0ULL;
                break;
            }
            default:
                tag |= 0x0F; cw = 0ULL; break;
            }
        }
        words[w++] = tag;
        words[w++] = cw;
    }
    return xxh64(words, (size_t)w * sizeof(uint64_t), 0x524f57444947ULL);
}

int tsdb_rowdigest_from_result(tsdb_result_t *res, int64_t span,
                               tsdb_rowdigest_bucket_t **out, size_t *out_n)
{
    if (!res || span <= 0 || !out || !out_n) return TSDB_ERR_INVAL;
    *out = NULL;
    *out_n = 0;

    int ncols = tsdb_result_ncols(res);
    int ts_ci = -1;
    for (int i = 0; i < ncols; i++)
        if (tsdb_result_col_type(res, i) == TSDB_TYPE_TIMESTAMP) { ts_ci = i; break; }
    if (ts_ci < 0) return TSDB_ERR_INVAL;      /* no ts column → cannot bucket */

    size_t cap = 16, n = 0;
    tsdb_rowdigest_bucket_t *v = malloc(cap * sizeof(*v));
    if (!v) return TSDB_ERR_NOMEM;
    size_t last = (size_t)-1;                  /* index of the last-touched bucket */

    while (tsdb_result_next(res)) {
        int64_t ts = (int64_t)tsdb_result_ts(res, ts_ci);
        int64_t m  = ts % span;
        if (m < 0) m += span;                  /* floor-align even a negative ts */
        int64_t bstart = ts - m;
        uint64_t rh = tsdb_rowdigest_row_hash(res, ncols);

        size_t idx = (size_t)-1;
        if (last != (size_t)-1 && v[last].bstart == bstart) {
            idx = last;                        /* hot path: same partition run */
        } else {
            for (size_t k = 0; k < n; k++)
                if (v[k].bstart == bstart) { idx = k; break; }
        }
        if (idx == (size_t)-1) {
            if (n >= TSDB_ROWDIGEST_MAX_BUCKETS) { free(v); return TSDB_ERR_OVERFLOW; }
            if (n == cap) {
                size_t nc = cap * 2;
                tsdb_rowdigest_bucket_t *nv = realloc(v, nc * sizeof(*v));
                if (!nv) { free(v); return TSDB_ERR_NOMEM; }
                v = nv; cap = nc;
            }
            idx = n++;
            v[idx].bstart = bstart;
            v[idx].count = 0; v[idx].hsum = 0; v[idx].hxor = 0;
        }
        v[idx].count++;
        v[idx].hsum += rh;                     /* order-independent (commutative) */
        v[idx].hxor ^= rh;
        last = idx;
    }

    /* Sort ascending by bstart so tsdb_rowdigest_diff can merge-walk.  Input is
     * near-sorted (partition-ordered), so insertion sort is effectively O(n). */
    for (size_t i = 1; i < n; i++) {
        tsdb_rowdigest_bucket_t x = v[i];
        size_t j = i;
        while (j > 0 && v[j - 1].bstart > x.bstart) { v[j] = v[j - 1]; j--; }
        v[j] = x;
    }

    *out = v;
    *out_n = n;
    return TSDB_OK;
}

int tsdb_rowdigest_diff(const tsdb_rowdigest_bucket_t *a, size_t na,
                        const tsdb_rowdigest_bucket_t *b, size_t nb,
                        int64_t *out_bstarts, size_t cap, size_t *out_ndiv)
{
    if (!out_ndiv) return TSDB_ERR_INVAL;
    *out_ndiv = 0;
    if ((na && !a) || (nb && !b)) return TSDB_ERR_INVAL;

    size_t i = 0, j = 0, nd = 0;
    while (i < na || j < nb) {
        int emit;
        int64_t bs;
        if (i < na && (j >= nb || a[i].bstart < b[j].bstart)) {
            bs = a[i].bstart; emit = 1; i++;                   /* only in a */
        } else if (j < nb && (i >= na || b[j].bstart < a[i].bstart)) {
            bs = b[j].bstart; emit = 1; j++;                   /* only in b */
        } else {                                               /* in both   */
            bs = a[i].bstart;
            emit = (a[i].count != b[j].count) ||
                   (a[i].hsum  != b[j].hsum)  ||
                   (a[i].hxor  != b[j].hxor);
            i++; j++;
        }
        if (emit) {
            if (out_bstarts && nd < cap) out_bstarts[nd] = bs;
            nd++;
        }
    }
    *out_ndiv = nd;
    return TSDB_OK;
}

int tsdb_rowdigest_serialize(const tsdb_rowdigest_bucket_t *v, size_t n,
                             uint8_t *buf, size_t cap)
{
    if (!buf) return -1;
    if (n > TSDB_ROWDIGEST_MAX_BUCKETS) return -1;
    size_t need = 4 + n * TSDB_ROWDIGEST_REC_SIZE;
    if (need > cap) return -1;

    uint32_t nb = (uint32_t)n;
    memcpy(buf, &nb, 4);
    uint8_t *p = buf + 4;
    for (size_t i = 0; i < n; i++) {
        memcpy(p,      &v[i].bstart, 8);
        memcpy(p + 8,  &v[i].count,  8);
        memcpy(p + 16, &v[i].hsum,   8);
        memcpy(p + 24, &v[i].hxor,   8);
        p += TSDB_ROWDIGEST_REC_SIZE;
    }
    return (int)need;
}

int tsdb_rowdigest_deserialize(const uint8_t *buf, uint32_t len,
                               tsdb_rowdigest_bucket_t **out, size_t *out_n)
{
    if (!buf || !out || !out_n) return TSDB_ERR_INVAL;
    *out = NULL;
    *out_n = 0;
    if (len < 4) return TSDB_ERR_CORRUPT;

    uint32_t nb = 0;
    memcpy(&nb, buf, 4);
    if (nb > TSDB_ROWDIGEST_MAX_BUCKETS) return TSDB_ERR_CORRUPT;
    if ((uint64_t)len < 4 + (uint64_t)nb * TSDB_ROWDIGEST_REC_SIZE)
        return TSDB_ERR_CORRUPT;
    if (nb == 0) return TSDB_OK;                    /* well-formed empty vector */

    tsdb_rowdigest_bucket_t *v = malloc((size_t)nb * sizeof(*v));
    if (!v) return TSDB_ERR_NOMEM;
    const uint8_t *p = buf + 4;
    for (uint32_t i = 0; i < nb; i++) {
        memcpy(&v[i].bstart, p,      8);
        memcpy(&v[i].count,  p + 8,  8);
        memcpy(&v[i].hsum,   p + 16, 8);
        memcpy(&v[i].hxor,   p + 24, 8);
        p += TSDB_ROWDIGEST_REC_SIZE;
    }
    *out = v;
    *out_n = nb;
    return TSDB_OK;
}
