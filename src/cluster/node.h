/* node.h — cluster node identity and membership view.
 *
 * tsdb_node_manager_t maintains the local view of all cluster nodes:
 * their addresses, states, and heartbeat timestamps.
 * It is updated by the gossip layer and consulted by the RPC / replica layers.
 */
#ifndef TSDB_CLUSTER_NODE_H
#define TSDB_CLUSTER_NODE_H

#include "hashring.h"
#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum address string length, including port. */
#define TSDB_ADDR_MAX 64

/* Node state in the SWIM-lite failure detector. */
typedef enum {
    TSDB_NODE_JOINING  = 0,
    TSDB_NODE_ALIVE    = 1,
    TSDB_NODE_SUSPECT  = 2,
    TSDB_NODE_DEAD     = 3
} tsdb_node_state_t;

/* Information we track per cluster member. */
typedef struct {
    tsdb_node_id_t  id;
    char            addr[TSDB_ADDR_MAX];   /* "host:port" (RPC TCP port) */
    char            gossip_addr[TSDB_ADDR_MAX]; /* "host:port" (UDP gossip port) */
    tsdb_node_state_t state;
    uint64_t        version;               /* monotonic generation / incarnation */
    int64_t         last_heartbeat_ns;     /* CLOCK_MONOTONIC nanoseconds */
    int             suspect_count;         /* consecutive probe failures */
    /* Local-view only (never on-wire): when we first upserted this peer.
     * Used by /cluster to report "known_for_s" so operators can see how
     * long a peer has been part of our local membership view. */
    int64_t         first_seen_ns;         /* CLOCK_MONOTONIC nanoseconds */
} tsdb_node_info_t;

/* Maximum cluster size. */
#define TSDB_CLUSTER_MAX_NODES 64

/* Opaque node manager. */
typedef struct tsdb_node_manager tsdb_node_manager_t;

/*
 * Create a node manager for this local node.
 * local_id    - unique ID for this node
 * local_addr  - "host:port" (RPC TCP)
 * gossip_addr - "host:port" (UDP gossip)
 */
tsdb_node_manager_t *tsdb_node_manager_new(tsdb_node_id_t local_id,
                                           const char *local_addr,
                                           const char *gossip_addr);

/* Free the node manager. */
void tsdb_node_manager_free(tsdb_node_manager_t *mgr);

/* Return local node ID. */
tsdb_node_id_t tsdb_node_manager_local_id(tsdb_node_manager_t *mgr);

/* Return local RPC address. */
const char *tsdb_node_manager_local_addr(tsdb_node_manager_t *mgr);

/* Upsert a node in the membership table.
 * If the incoming version > stored version, update state + heartbeat. */
void tsdb_node_manager_upsert(tsdb_node_manager_t *mgr,
                              const tsdb_node_info_t *info);

/* SWIM refute — called when a peer's state_sync claims the local node
 * is SUSPECT or DEAD.  Bumps the local version strictly past peer_ver
 * and marks self ALIVE.  Next STATE_SYNC fanout carries the new
 * (ver, ALIVE) tuple which the higher-version-wins merge accepts
 * everywhere, restoring our membership. */
void tsdb_node_manager_refute(tsdb_node_manager_t *mgr, uint64_t peer_ver);

/* Mark a node as SUSPECT (failed probe). */
void tsdb_node_manager_suspect(tsdb_node_manager_t *mgr, tsdb_node_id_t id);

/* Mark a node DEAD (suspect timeout expired). */
void tsdb_node_manager_dead(tsdb_node_manager_t *mgr, tsdb_node_id_t id);

/* Mark a node ALIVE again (received a heartbeat from it). */
void tsdb_node_manager_alive(tsdb_node_manager_t *mgr, tsdb_node_id_t id,
                             uint64_t new_version);

/* Get a snapshot of all known nodes (including self).
 * out_buf must hold at least TSDB_CLUSTER_MAX_NODES entries.
 * Returns number of entries written. */
int tsdb_node_manager_snapshot(tsdb_node_manager_t *mgr,
                               tsdb_node_info_t *out_buf, int buf_cap);

/* Get the info for one specific node. Returns 0 if found, -1 if not found. */
int tsdb_node_manager_get(tsdb_node_manager_t *mgr, tsdb_node_id_t id,
                          tsdb_node_info_t *out);

/* Select up to `count` random ALIVE nodes (excluding local).
 * Returns how many were written. */
int tsdb_node_manager_random_alive(tsdb_node_manager_t *mgr,
                                   tsdb_node_id_t *out, int count);

/* Return the shared hashring (updated whenever membership changes). */
tsdb_hashring_t *tsdb_node_manager_ring(tsdb_node_manager_t *mgr);

/*
 * Thread-safe ring lookup: holds mgr->lock while calling tsdb_hashring_owner_str.
 * Returns the number of replica node IDs written to out_nodes (up to max_replicas).
 */
int tsdb_node_manager_ring_owner(tsdb_node_manager_t *mgr,
                                  const char *key,
                                  int max_replicas,
                                  tsdb_node_id_t *out_nodes);

/* Return number of ALIVE nodes (including self). */
int tsdb_node_manager_alive_count(tsdb_node_manager_t *mgr);

/* Serialise membership for a STATE_SYNC gossip payload.
 * Writes binary records into buf (capacity cap bytes).
 * Returns bytes written, or -1 if buffer too small. */
int tsdb_node_manager_encode(tsdb_node_manager_t *mgr, uint8_t *buf, int cap);

/* Deserialise and merge STATE_SYNC payload. */
void tsdb_node_manager_decode(tsdb_node_manager_t *mgr,
                              const uint8_t *buf, int len);

/* Update heartbeat for local node (called by gossip ticker). */
void tsdb_node_manager_heartbeat(tsdb_node_manager_t *mgr);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_NODE_H */
