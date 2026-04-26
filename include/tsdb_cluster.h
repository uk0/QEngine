/* tsdb_cluster.h — cluster extension API for tsdb.
 *
 * Include this in addition to tsdb.h when using cluster features.
 * The functions declared here are implemented in src/storage/db_cluster.c.
 */
#ifndef TSDB_CLUSTER_H_PUBLIC
#define TSDB_CLUSTER_H_PUBLIC

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Open a cluster-enabled database instance.
 *
 * data_dir      - local storage root (created if missing)
 * bind_addr     - TCP RPC bind address "host:port" (e.g. "0.0.0.0:28081")
 * gossip_seeds  - comma-separated "host:gossip_port" for peer discovery,
 *                 or NULL for standalone bootstrap node.
 *                 Gossip port = RPC port - 1 on the same host by convention.
 * out           - populated on success
 *
 * Returns TSDB_OK or a negative error code.
 */
int tsdb_open_cluster(const char *data_dir,
                      const char *bind_addr,
                      const char *gossip_seeds,
                      tsdb_db_t **out);

/*
 * Return cluster status as a JSON-ish string.
 * buf - caller buffer, cap - buffer capacity.
 * Returns bytes written.
 */
int tsdb_cluster_stats(tsdb_db_t *db, char *buf, size_t cap);

/*
 * Return the number of ALIVE cluster nodes (including self).
 * Returns 1 for standalone mode.
 */
int tsdb_cluster_alive_count(tsdb_db_t *db);

/*
 * Block until at least min_nodes ALIVE members are visible,
 * or timeout_ms milliseconds have elapsed.
 */
void tsdb_cluster_wait_ready(tsdb_db_t *db, int min_nodes, int timeout_ms);

/*
 * Stop cluster threads (gossip + RPC) without closing the DB.
 * tsdb_close() will still work normally after this call.
 */
void tsdb_close_cluster(tsdb_db_t *db);

/*
 * Cluster-wide broadcast of a TRUNCATE TABLE that has already been applied
 * locally.  Sends TSDB_RPC_APPLY_TRUNCATE to every ALIVE peer except self,
 * best-effort.  *out_total_peers gets the number of peers contacted;
 * *out_acked_peers gets how many replied with ACK.  Both may be NULL.
 *
 * In standalone mode (no cluster) returns TSDB_OK with both counts = 0.
 */
int tsdb_cluster_broadcast_truncate(tsdb_db_t *db,
                                     const char *table_name,
                                     int *out_acked_peers,
                                     int *out_total_peers);

/*
 * Cluster-wide broadcast of a partition-level DELETE that has already
 * been applied locally.  Sends TSDB_RPC_APPLY_DELETE_RANGE to every
 * ALIVE peer except self.
 *
 * op_lt / inclusive match the semantics of tsdb_delete_range.
 */
int tsdb_cluster_broadcast_delete_range(tsdb_db_t *db,
                                         const char *table_name,
                                         int64_t cutoff_ns,
                                         int op_lt, int inclusive,
                                         int *out_acked_peers,
                                         int *out_total_peers);

/*
 * Cluster-wide broadcast of a catalog-mutating QTL (CREATE DATABASE,
 * CREATE VTABLE, CREATE TABLE, CREATE GROUP, DROP *).  The primary has
 * already applied locally; this fans out to every ALIVE peer so each
 * maintains an identical catalog snapshot.  Standalone → no-op.
 *
 * A thread-local flag (`tsdb_g_suppress_catalog_broadcast`) keeps the
 * peer's re-entered tsdb_query from broadcasting again.
 */
int tsdb_cluster_broadcast_catalog_qtl(tsdb_db_t *db,
                                        const char *qtl,
                                        int *out_acked_peers,
                                        int *out_total_peers);

/* Variant that targets data nodes only — used by the Raft apply
 * callback on the master side.  Other masters replicate the same
 * log entry through Raft's own AppendEntries, so they must NOT be
 * broadcast to (that would cause a duplicate apply).  Data nodes
 * don't run Raft and have no other path to see catalog-only DDL
 * like CREATE DATABASE / CREATE GROUP; without this fanout, their
 * local catalog lags the cluster forever. */
int tsdb_cluster_broadcast_catalog_qtl_to_data(tsdb_db_t *db,
                                                 const char *qtl,
                                                 int *out_acked_peers,
                                                 int *out_total_peers);

/* Anti-entropy resync.  Pulls missing rows (ts > local_max_ts) from
 * the peer that holds the most up-to-date copy of `table_name`.  The
 * pulled rows are applied with the local-only flag so they do not
 * re-replicate (that would amplify traffic and could loop if two
 * peers both try to catch up at once).  Covers the common case of a
 * data node being offline during writes — on restart the node can
 * call this for every table in its catalog to close the gap.
 *
 * Returns TSDB_OK on success (including the "already up-to-date"
 * case) and sets *out_rows_pulled to the number of rows inserted.
 * Returns TSDB_ERR_IO only when the transport / peer schema path
 * actively fails; callers that want best-effort catch-up can
 * safely retry next interval. */
int tsdb_cluster_resync_table(tsdb_db_t *db,
                               const char *table_name,
                               int *out_rows_pulled);

/* Phase γ — read-side shard forwarding.
 *
 * If TSDB_SHARD_REPLICA_N > 0 and this node is NOT in the owner set
 * for `table_name`, forwards `qtl` to the first owner via FED_QUERY
 * and returns the result in *out.  Otherwise leaves *out=NULL and
 * returns TSDB_OK so the caller proceeds with local execution.
 * Re-entry on the owner side is guarded by a thread-local so a
 * forwarded query never re-forwards.
 *
 * `table_name` must be the table the query reads from (typically
 * the FROM clause).  When NULL, the caller is signalling "no shard
 * decision possible" and forwarding is skipped. */
int tsdb_cluster_maybe_forward_select(tsdb_db_t *db,
                                       const char *table_name,
                                       const char *qtl,
                                       tsdb_result_t **out);

/* Bridges into the cluster layer for modules that live above it
 * (e.g. raft.c) without exposing the full tsdb_cluster struct.  Each
 * returns NULL / 0 if the db is in standalone mode. */
struct tsdb_node_manager;
typedef struct tsdb_node_manager tsdb_node_manager_t;
struct tsdb_replica_mgr;
typedef struct tsdb_replica_mgr  tsdb_replica_mgr_t;

tsdb_node_manager_t *tsdb_cluster_node_mgr_for_db   (tsdb_db_t *db);
tsdb_replica_mgr_t  *tsdb_cluster_replica_mgr_for_db(tsdb_db_t *db);
uint64_t             tsdb_cluster_local_id_for_db   (tsdb_db_t *db);

/* Attach/lookup a raft instance for a given db.  The node main hands
 * its raft_h to the db once the state machine is open; exec.c then
 * consults it on every catalog-mutating QTL to decide between the
 * Raft consensus path and the legacy fanout path.  Pass NULL to
 * detach (during shutdown). */
struct tsdb_raft;
typedef struct tsdb_raft tsdb_raft_t;

void           tsdb_db_bind_raft (tsdb_db_t *db, tsdb_raft_t *raft);
tsdb_raft_t   *tsdb_db_raft_for_db(tsdb_db_t *db);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_H_PUBLIC */
