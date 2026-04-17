/* hashring.h — consistent hash ring for shard routing.
 *
 * Each node gets TSDB_RING_VN virtual nodes spread uniformly around a
 * 64-bit ring.  tsdb_hashring_owner() returns up to replica_count node IDs
 * responsible for the given key: index 0 is the primary.
 */
#ifndef TSDB_CLUSTER_HASHRING_H
#define TSDB_CLUSTER_HASHRING_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Number of virtual nodes per physical node. */
#define TSDB_RING_VN 256

/* Maximum nodes supported in one ring. */
#define TSDB_RING_MAX_NODES 64

typedef uint64_t tsdb_node_id_t;

/* Opaque ring handle. */
typedef struct tsdb_hashring tsdb_hashring_t;

/* Allocate an empty ring. Returns NULL on OOM. */
tsdb_hashring_t *tsdb_hashring_new(void);

/* Free the ring. */
void tsdb_hashring_free(tsdb_hashring_t *ring);

/* Add a node to the ring.  Idempotent if already present.
 * Returns 0 on success, -1 on capacity exceeded. */
int tsdb_hashring_add(tsdb_hashring_t *ring, tsdb_node_id_t node_id);

/* Remove a node from the ring.  No-op if absent. */
void tsdb_hashring_remove(tsdb_hashring_t *ring, tsdb_node_id_t node_id);

/* Return how many physical nodes are currently in the ring. */
int tsdb_hashring_node_count(const tsdb_hashring_t *ring);

/*
 * Find the replica_count nodes responsible for the given byte key.
 * out_nodes must have room for at least replica_count entries.
 * Returns the actual number of nodes written (≤ replica_count,
 * limited by how many physical nodes exist).
 * out_nodes[0] is the primary.
 */
int tsdb_hashring_owner(const tsdb_hashring_t *ring,
                        const void *key, size_t key_len,
                        int replica_count,
                        tsdb_node_id_t *out_nodes);

/* Hash a null-terminated string key. */
int tsdb_hashring_owner_str(const tsdb_hashring_t *ring,
                            const char *key,
                            int replica_count,
                            tsdb_node_id_t *out_nodes);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_HASHRING_H */
