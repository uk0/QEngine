/* cluster.h — top-level cluster object tying together node manager,
 * gossip engine, RPC server, and replica manager.
 *
 * A tsdb_db_t holds a pointer to tsdb_cluster_t (may be NULL for
 * standalone mode). The cluster routes writes through the hashring
 * and replication layer.
 */
#ifndef TSDB_CLUSTER_H
#define TSDB_CLUSTER_H

#include "node.h"
#include "gossip.h"
#include "rpc.h"
#include "replica.h"
#include "hashring.h"
#include "autobalance.h"
#include "../../include/tsdb.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque cluster object. */
typedef struct tsdb_cluster tsdb_cluster_t;

/*
 * Create and start a cluster node.
 *
 * db           - owning database (used by RPC server for local applies)
 * local_id     - unique 64-bit node identifier
 * rpc_addr     - "host:port" for TCP RPC (default port 28081)
 * gossip_addr  - "host:port" for UDP gossip (default port 28080)
 * seeds        - comma-separated "host:gossip_port" strings (may be NULL)
 * local_role   - TSDB_ROLE_MASTER (default) or TSDB_ROLE_DATA.
 *                MASTERs participate in Raft; DATA nodes forward DDL.
 */
tsdb_cluster_t *tsdb_cluster_new(tsdb_db_t *db,
                                 tsdb_node_id_t local_id,
                                 const char *rpc_addr,
                                 const char *gossip_addr,
                                 const char *seeds,
                                 tsdb_node_role_t local_role);

/* Stop gossip/RPC threads and free resources. */
void tsdb_cluster_free(tsdb_cluster_t *c);

/* Return local node ID. */
tsdb_node_id_t tsdb_cluster_local_id(tsdb_cluster_t *c);

/* Return the shared node manager. */
tsdb_node_manager_t *tsdb_cluster_node_mgr(tsdb_cluster_t *c);

/* Return the hashring. */
tsdb_hashring_t *tsdb_cluster_ring(tsdb_cluster_t *c);

/* Return the replica manager. */
tsdb_replica_mgr_t *tsdb_cluster_replica_mgr(tsdb_cluster_t *c);

/* Return the auto-balance controller (may be NULL if not started). */
tsdb_autobalance_t *tsdb_cluster_autobalance(tsdb_cluster_t *c);

/* ---- Sharding routing API (Phase α) ---------------------------------
 *
 * Computes which N node IDs should own a given shard key.  Used by the
 * sharded write path (Phase β) and by clients that want to skip the
 * coordinator hop.  Today the cluster still fan-outs to every node
 * (full-replica), so this API is read-only — callers can ask "where
 * WOULD a shard live" without changing where the data actually goes.
 *
 *   table_name  — used together with key to derive the shard hash
 *   key         — partition key (e.g. host tag value, device id);
 *                 may be empty/NULL → just hash the table name
 *   replica_n   — how many distinct nodes to return (clamped to ring
 *                 node count).  Typical values: 2 or 3.
 *   out_ids     — caller-supplied buffer; filled with up to replica_n
 *                 unique node IDs in primary-first order.
 *
 * Returns the count actually written, 0 on empty ring, negative on
 * invalid args.  Idempotent: same key always returns the same nodes
 * until ring membership changes.
 */
int tsdb_cluster_route(tsdb_cluster_t *c,
                       const char *table_name,
                       const char *key,
                       int replica_n,
                       tsdb_node_id_t *out_ids);

/*
 * Called by db.c batch commit when cluster is active.
 *
 * table_name  - target table
 * ncols       - number of columns
 * col_types   - column types (tsdb_type_t)
 * nrows       - row count
 * col_data    - per-column data pointers (columnar)
 *
 * If this node is the primary for the shard:
 *   - writes locally (already done by caller via local memtable)
 *   - replicates to W-1 more replicas
 * If this node is a replica:
 *   - applies locally (already done)
 * If this node is not involved:
 *   - TODO: forward to primary (not implemented; all N=3 nodes host all shards)
 *
 * Returns TSDB_OK or error.
 */
int tsdb_cluster_write(tsdb_cluster_t *c,
                       const char *table_name,
                       int ncols, const int *col_types,
                       int nrows, const void **col_data,
                       int *out_remote_acks, uint64_t incarnation);

/*
 * Propagate a CREATE TABLE to cluster peers.
 * Called from tsdb_create_table when cluster is active.
 * nodes / nnodes: target node IDs (all ALIVE peers if NULL is passed —
 * implementation picks all ALIVE nodes automatically).
 *
 * partition_unit / block_points / sort_by_tag_col travel in the
 * SCHEMA_SYNC v2 payload tail.  Pass the leader's actual schema
 * values so the follower's local schema matches exactly.
 */
int tsdb_cluster_sync_schema(tsdb_cluster_t *c,
                             const char *table_name,
                             int ncols, const char **col_names,
                             const int *col_types, int ts_col_idx,
                             int partition_unit, int block_points,
                             int sort_by_tag_col,
                             const tsdb_node_id_t *nodes, int nnodes,
                             uint64_t incarnation);

/*
 * Fill buf with cluster status JSON-ish string (for tsdb_cluster_stats).
 * Returns bytes written.
 */
int tsdb_cluster_stats_str(tsdb_cluster_t *c, char *buf, size_t cap);

/* Wait until at least `min_alive` nodes are visible (for test readiness).
 * Internal function — the public API is tsdb_cluster_wait_ready(tsdb_db_t*, ...)
 * in db.c / include/tsdb.h. */
void tsdb_cluster_wait_alive(tsdb_cluster_t *c, int min_alive, int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_H */
