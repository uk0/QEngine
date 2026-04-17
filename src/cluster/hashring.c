/* hashring.c — consistent hash ring implementation.
 *
 * Uses FNV-1a 64-bit as the ring hash.  Virtual nodes are placed at
 * hash(node_id_bytes || vn_idx_bytes).  The ring is kept sorted for
 * O(log N) lookup.
 */

#include "hashring.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ---- FNV-1a 64-bit ------------------------------------------------------- */

#define FNV_OFFSET 14695981039346656037ULL
#define FNV_PRIME  1099511628211ULL

static uint64_t fnv1a_64(const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t h = FNV_OFFSET;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= FNV_PRIME;
    }
    return h;
}

/* ---- Virtual node -------------------------------------------------------- */

typedef struct {
    uint64_t    hash;       /* ring position */
    tsdb_node_id_t node_id;
} vnode_t;

static int vnode_cmp(const void *a, const void *b) {
    const vnode_t *va = (const vnode_t *)a;
    const vnode_t *vb = (const vnode_t *)b;
    if (va->hash < vb->hash) return -1;
    if (va->hash > vb->hash) return  1;
    return 0;
}

/* ---- Ring struct --------------------------------------------------------- */

struct tsdb_hashring {
    /* sorted array of virtual nodes */
    vnode_t     vnodes[TSDB_RING_MAX_NODES * TSDB_RING_VN];
    int         nvnodes;

    /* physical nodes we know about (for dedup) */
    tsdb_node_id_t nodes[TSDB_RING_MAX_NODES];
    int            nnodes;

    int         sorted; /* whether vnodes[] is sorted */
};

/* ---- Public API ---------------------------------------------------------- */

tsdb_hashring_t *tsdb_hashring_new(void) {
    tsdb_hashring_t *r = calloc(1, sizeof(*r));
    return r;
}

void tsdb_hashring_free(tsdb_hashring_t *ring) {
    free(ring);
}

int tsdb_hashring_add(tsdb_hashring_t *ring, tsdb_node_id_t node_id) {
    if (!ring) return -1;

    /* Idempotent: check if already present. */
    for (int i = 0; i < ring->nnodes; i++) {
        if (ring->nodes[i] == node_id) return 0;
    }

    if (ring->nnodes >= TSDB_RING_MAX_NODES) return -1;

    ring->nodes[ring->nnodes++] = node_id;

    /* Place TSDB_RING_VN virtual nodes. */
    for (int v = 0; v < TSDB_RING_VN; v++) {
        uint8_t buf[16];
        uint64_t nid = node_id;
        uint32_t vidx = (uint32_t)v;
        memcpy(buf,     &nid,  8);
        memcpy(buf + 8, &vidx, 4);
        uint64_t h = fnv1a_64(buf, 12);

        ring->vnodes[ring->nvnodes].hash    = h;
        ring->vnodes[ring->nvnodes].node_id = node_id;
        ring->nvnodes++;
    }

    ring->sorted = 0;
    return 0;
}

void tsdb_hashring_remove(tsdb_hashring_t *ring, tsdb_node_id_t node_id) {
    if (!ring) return;

    /* Remove from physical node list. */
    int found = 0;
    for (int i = 0; i < ring->nnodes; i++) {
        if (ring->nodes[i] == node_id) {
            ring->nodes[i] = ring->nodes[--ring->nnodes];
            found = 1;
            break;
        }
    }
    if (!found) return;

    /* Remove virtual nodes belonging to this physical node. */
    int w = 0;
    for (int i = 0; i < ring->nvnodes; i++) {
        if (ring->vnodes[i].node_id != node_id) {
            ring->vnodes[w++] = ring->vnodes[i];
        }
    }
    ring->nvnodes = w;
    ring->sorted = 0;
}

int tsdb_hashring_node_count(const tsdb_hashring_t *ring) {
    return ring ? ring->nnodes : 0;
}

/* Ensure the ring is sorted. */
static void ensure_sorted(tsdb_hashring_t *ring) {
    if (!ring->sorted) {
        qsort(ring->vnodes, ring->nvnodes, sizeof(vnode_t), vnode_cmp);
        ring->sorted = 1;
    }
}

/* Binary search: find first vnode with hash >= h. */
static int ring_find_gte(const tsdb_hashring_t *ring, uint64_t h) {
    int lo = 0, hi = ring->nvnodes;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (ring->vnodes[mid].hash < h)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo; /* may equal nvnodes (wrap around) */
}

int tsdb_hashring_owner(const tsdb_hashring_t *ring,
                        const void *key, size_t key_len,
                        int replica_count,
                        tsdb_node_id_t *out_nodes)
{
    if (!ring || !key || !out_nodes || replica_count <= 0) return 0;
    if (ring->nvnodes == 0) return 0;

    ensure_sorted((tsdb_hashring_t *)ring);

    uint64_t h = fnv1a_64(key, key_len);
    int pos = ring_find_gte(ring, h);
    if (pos >= ring->nvnodes) pos = 0; /* wrap */

    /* Walk the ring, collecting distinct physical nodes. */
    int found = 0;
    int max_walk = ring->nvnodes;
    for (int i = 0; i < max_walk && found < replica_count && found < ring->nnodes; i++) {
        int idx = (pos + i) % ring->nvnodes;
        tsdb_node_id_t nid = ring->vnodes[idx].node_id;

        /* Dedup: check if already in out_nodes. */
        int dup = 0;
        for (int j = 0; j < found; j++) {
            if (out_nodes[j] == nid) { dup = 1; break; }
        }
        if (!dup) {
            out_nodes[found++] = nid;
        }
    }
    return found;
}

int tsdb_hashring_owner_str(const tsdb_hashring_t *ring,
                            const char *key,
                            int replica_count,
                            tsdb_node_id_t *out_nodes)
{
    if (!key) return 0;
    return tsdb_hashring_owner(ring, key, strlen(key), replica_count, out_nodes);
}
