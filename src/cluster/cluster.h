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
 */
tsdb_cluster_t *tsdb_cluster_new(tsdb_db_t *db,
                                 tsdb_node_id_t local_id,
                                 const char *rpc_addr,
                                 const char *gossip_addr,
                                 const char *seeds);

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
                       int nrows, const void **col_data);

/*
 * Propagate a CREATE TABLE to cluster peers.
 * Called from tsdb_create_table when cluster is active.
 * nodes / nnodes: target node IDs (all ALIVE peers if NULL is passed —
 * implementation picks all ALIVE nodes automatically).
 */
int tsdb_cluster_sync_schema(tsdb_cluster_t *c,
                             const char *table_name,
                             int ncols, const char **col_names,
                             const int *col_types, int ts_col_idx,
                             const tsdb_node_id_t *nodes, int nnodes);

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
