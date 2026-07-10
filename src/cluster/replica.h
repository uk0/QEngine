/* replica.h — synchronous quorum replication.
 *
 * Write path (primary):
 *   1. Write locally (local_only path that bypasses re-replication).
 *   2. Encode the batch as a WRITE_BATCH RPC payload.
 *   3. Fan out to all replicas (async send).
 *   4. Wait for W=2 ACKs total (including local write = 1).
 *   5. Return success once quorum reached; reply with error if timeout.
 *
 * The caller must tell us which nodes are replicas (from hashring lookup).
 * This layer does NOT decide primary/replica roles; that is done by
 * the cluster layer based on hashring_owner().
 */
#ifndef TSDB_CLUSTER_REPLICA_H
#define TSDB_CLUSTER_REPLICA_H

#include "node.h"
#include "rpc.h"
#include "../../include/tsdb.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Quorum write count (including local). */
#define TSDB_WRITE_QUORUM 2

/* Replica manager: holds RPC connections to cluster peers. */
typedef struct tsdb_replica_mgr tsdb_replica_mgr_t;

/*
 * Create a replica manager.
 * node_mgr    - shared membership state
 */
tsdb_replica_mgr_t *tsdb_replica_mgr_new(tsdb_node_manager_t *node_mgr);

/* Free and close all connections. */
void tsdb_replica_mgr_free(tsdb_replica_mgr_t *rmgr);

/* Install an optional cross-DC async forwarder.  Every batch payload
 * produced by tsdb_replica_push_row_batch is copied into the
 * forwarder's ring buffer for async delivery to a remote DC.  Pass
 * NULL to detach.  See src/federation/dr_forwarder.h. */
struct tsdb_dr_forwarder;
void tsdb_replica_mgr_set_dr(tsdb_replica_mgr_t *rmgr,
                              struct tsdb_dr_forwarder *fw);

/*
 * Replicate a write batch to the given set of replica nodes.
 *
 * table_name  - target table
 * ncols       - number of columns (including timestamp)
 * col_types   - array of tsdb_type_t for each column
 * nrows       - number of rows
 * col_data    - array of pointers, one per column (columnar layout)
 * replicas    - node IDs to replicate to (PRIMARY already wrote locally)
 * nreplicas   - length of replicas array
 * w_quorum    - number of ACKs needed (including the already-written local)
 *               pass TSDB_WRITE_QUORUM (2) for normal operation
 *
 * Returns TSDB_OK when quorum achieved, TSDB_ERR_IO if not enough ACKs.
 */
int tsdb_replica_write(tsdb_replica_mgr_t *rmgr,
                       const char *table_name,
                       int ncols, const int *col_types,
                       int nrows, const void **col_data,
                       const tsdb_node_id_t *replicas, int nreplicas,
                       int w_quorum);

/*
 * Sync schema (SCHEMA_SYNC RPC) to all given nodes.
 * Returns TSDB_OK when at least (quorum-1) remote nodes ACK.
 *
 * partition_unit / block_points / sort_by_tag_col travel in the v2
 * payload tail so leader-side schema choices replicate verbatim.
 */
int tsdb_replica_sync_schema(tsdb_replica_mgr_t *rmgr,
                             const char *table_name,
                             int ncols, const char **col_names,
                             const int *col_types, int ts_col_idx,
                             int partition_unit, int block_points,
                             int sort_by_tag_col,
                             const tsdb_node_id_t *nodes, int nnodes,
                             int quorum);

/*
 * Broadcast a TRUNCATE-TABLE apply to the given peer list (best-effort).
 * Local apply must already have happened.  Always returns TSDB_OK;
 * *out_acked_peers gets the count of peers that ACKed (0 = all failed).
 */
int tsdb_replica_broadcast_truncate(tsdb_replica_mgr_t *rmgr,
                                     const char *table_name,
                                     const tsdb_node_id_t *peers, int npeers,
                                     int *out_acked_peers);

/*
 * Broadcast a DELETE-RANGE apply (best-effort).  Same semantics as
 * broadcast_truncate.  cutoff_ns / op_lt / inclusive match the
 * tsdb_delete_range signature.
 */
int tsdb_replica_broadcast_delete_range(tsdb_replica_mgr_t *rmgr,
                                         const char *table_name,
                                         int64_t cutoff_ns,
                                         int op_lt, int inclusive,
                                         const tsdb_node_id_t *peers, int npeers,
                                         int *out_acked_peers);

/*
 * Broadcast a catalog-mutating QTL statement to every ALIVE peer.
 * Best-effort; every peer runs the statement locally via tsdb_query.
 * out_acked_peers gets the count that successfully applied (EXISTS is
 * treated as success — idempotent on peer catch-up).
 */
int tsdb_replica_broadcast_catalog_qtl(tsdb_replica_mgr_t *rmgr,
                                        const char *qtl,
                                        const tsdb_node_id_t *peers, int npeers,
                                        int *out_acked_peers);

/*
 * Get or create a persistent RPC connection to the given node.
 * Returns NULL if unable to connect.
 */
tsdb_rpc_conn_t *tsdb_replica_mgr_get_conn(tsdb_replica_mgr_t *rmgr,
                                            tsdb_node_id_t node_id);

/*
 * Evict a failing conn from its pool slot so the next get_conn redials.
 * The conn object is intentionally NOT closed (in-flight callers may hold
 * its lock); the bounded leak is documented at evict_one_conn.
 */
void tsdb_replica_mgr_evict_conn(tsdb_replica_mgr_t *rmgr,
                                  tsdb_node_id_t node_id,
                                  tsdb_rpc_conn_t *bad);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_REPLICA_H */
