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

/* Follower → leader catalog-DDL forward over the trusted RPC channel.
 * On an all-master Raft cluster a CREATE/DROP that lands on a FOLLOWER must
 * transparently reach the LEADER instead of bouncing the client with a
 * "not raft leader" hint.  exec.c calls this when it holds a non-zero
 * leader hint (tsdb_raft_leader_id); the helper resolves leader_id to an
 * RPC addr from membership and ships the QTL via TSDB_RPC_DDL_FORWARD,
 * copying the leader's status text into out_status.
 *
 * Returns TSDB_OK with out_status set when the leader RAN the statement.
 * Returns TSDB_ERR_NOTFOUND when leader_id is 0 (mid-election) or unknown/down
 * in membership — the caller should then fall back to the retry hint.  Other
 * negative codes signal that the leader refused it or that the round-trip
 * failed; out_status is set to the leader's own message when the refusal came
 * WITH one, and left EMPTY when no answer arrived at all.  That is the
 * distinction the caller needs: an empty status means "ask someone else", a
 * populated one means "the leader ran this and it did not work — showing the
 * retry hint instead would be a lie".  (Before 2026-07-31 a refusal came back
 * as TSDB_OK carrying the error text, so no caller could tell at all.)
 * No forward loop: the leader's state==LEADER so it executes locally and never
 * re-forwards. */
int tsdb_cluster_forward_ddl_to_leader(tsdb_db_t *db,
                                        uint64_t leader_id,
                                        const char *qtl,
                                        char *out_status, size_t cap);

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

/* ONE anti-entropy sweep: open every on-disk table, materialize any
 * data-bearing table the catalog knows and storage does not, then
 * tsdb_cluster_resync_table each of them.  Bumps
 * qengine_antientropy_sweeps_total.  This is the body
 * tsdb_resync_startup_thread ran exactly once, 5 s after boot; it now runs it
 * on a period, because everything in the replication path that says
 * "anti-entropy will heal it" is depending on it running again. */
void tsdb_cluster_resync_all_tables(tsdb_db_t *db);

/* Milliseconds between sweeps (TSDB_ANTIENTROPY_PERIOD_MS, default 30000 —
 * the same tick delete-watermark re-assert uses).  0 = one shot at startup and
 * never again, the pre-2026-07-31 behaviour. */
long tsdb_antientropy_period_ms(void);

/* Wall budget ONE table's candidate loop gets inside a sweep
 * (AE_RESYNC_BUDGET_MS).  Exposed so a test can pin the number the production
 * driver passes; tests/test_ae_candidates.c pins that the loop honours it. */
long tsdb_antientropy_budget_ms(void);

/* Anti-entropy reconcile decision (exposed for unit testing).
 *
 * Given the local (count, max_ts) and the best peer's (count, max_ts), decide
 * what recovery action is SAFE.  The hard invariant: anti-entropy must never
 * reduce a node's durable row count for a table based solely on a peer count
 * comparison — so a populated local table is never truncated here. */
typedef enum {
    TSDB_AE_UP_TO_DATE = 0, /* peer adds nothing — no action          */
    TSDB_AE_TAIL_PULL,      /* peer has newer rows — pull ts > local   */
    TSDB_AE_FULL_PULL,      /* local empty — safe truncate + full pull */
    TSDB_AE_SKIP_UNSAFE     /* would shrink durable data — refuse      */
} tsdb_ae_action_t;

tsdb_ae_action_t tsdb_antientropy_decide(uint64_t local_count,
                                         int64_t  local_max_ts,
                                         uint64_t best_count,
                                         int64_t  best_max_ts);

/* Is node id `x` in the `n`-element set — the co-replica membership test that
 * confines the row-digest to a table's replica set (exposed for unit testing). */
int tsdb_ae_node_in_set(uint64_t x, const uint64_t *set, int n);

/* Hard anti-runaway pull budget for the row-digest: the most rows ONE sweep may
 * merge for a table = peer_hi_count (the union of two replicas is at most
 * local + peer_hi, so no legitimate heal needs more than the fullest peer HOLDS).
 * NOT (peer_hi - local): that is 0 at equal counts, i.e. exactly the middle hole
 * the digest exists to heal.  Exposed for unit testing. */
uint64_t tsdb_ae_pull_budget(uint64_t local_before, uint64_t peer_hi_count);

/* Rows THIS node holds for `table_name`, and the newest ts among them
 * (exposed for unit testing).
 *
 * Anti-entropy must not measure itself with `SELECT count(*) FROM <t>`: plain
 * tables are mirrored as childless super-tables, so that query scatters and a
 * node with no local rows gets back the CLUSTER-WIDE count — it then believes
 * it is up to date and never pulls.  This counts partitions (ts.idx headers)
 * plus an atomic memtable snapshot.  An empty table yields (0, INT64_MIN).
 * Out-params may be NULL.  Returns TSDB_OK, or TSDB_ERR_* if the measurement
 * could not be taken — callers must not guess on failure. */
int tsdb_cluster_local_table_stats(tsdb_db_t *db, const char *table_name,
                                   uint64_t *out_count, int64_t *out_max_ts);

/* The PEER half of the same measurement (exposed for unit testing).
 *
 * `conn` is an open connection to the peer (struct tsdb_rpc_conn, i.e. the
 * tsdb_rpc_conn_t of src/cluster/rpc.h — forward-declared here so the public
 * header keeps no dependency on the cluster wire layer).
 *
 * Asks the peer what IT ITSELF holds via TSDB_RPC_LOCAL_TABLE_STATS, and only
 * if the peer's binary is too old to know that opcode falls back to the legacy
 * `SELECT count(*), max(ts) FROM <t>` probe.  Returns 0 with *out_count /
 * *out_max_ts populated, -1 if the peer gave no usable answer. */
struct tsdb_rpc_conn;
int tsdb_cluster_peer_table_stats_conn(struct tsdb_rpc_conn *conn,
                                       const char *table_name,
                                       uint64_t *out_count,
                                       int64_t  *out_max_ts);

/* Partition-level backfill primitive (exposed for unit testing).
 *
 * Materialises `res` — every row of ONE partition, as pulled from a fuller
 * peer via `SELECT * FROM t WHERE ts >= pstart AND ts <= pend` — into a
 * temp partition under <table>/.aebf_tmp/ (normal col-writer flush path,
 * SYMBOL codes interned through the live table's dictionaries), then swaps
 * the .col/.idx pairs over the live partition atomically under the table's
 * compaction mutex.
 *
 * `part_name` is the partition dir name ("YYYYMMDD" for DAY tables,
 * "YYYYMMDDHH" for HOUR); every row in `res` must fall inside its range.
 * `expected_local_rows` is the caller's snapshot of the live partition's
 * ts.idx total_rows; if the live value moved by swap time (a concurrent
 * flush/delete touched the partition) the swap aborts with TSDB_ERR_BUSY
 * and the table is left untouched.  Refuses (TSDB_ERR_INVAL) unless the
 * pulled copy is STRICTLY fuller than expected_local_rows — backfill never
 * reduces durable rows.  On success *out_rows_written is the new partition
 * row count.  Local-unique rows in the swapped partition are replaced by
 * the peer copy (MVP limitation; logged loudly by the resync caller). */
int tsdb_cluster_backfill_partition_from_result(tsdb_db_t *db,
                                                const char *table_name,
                                                const char *part_name,
                                                tsdb_result_t *res,
                                                uint64_t expected_local_rows,
                                                uint64_t *out_rows_written);

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

/* ---- Cluster-wide super-table aggregation (task #175) -----------------
 *
 * Scatter-local mode flag.  Defined in query/exec.c; set by the
 * FED_QUERY_LOCAL RPC handler (and around the coordinator's own local
 * partial leg).  While non-zero:
 *   - exec_stable_select never starts a new scatter (anti-recursion),
 *   - the per-child expansion covers only children whose primary ALIVE
 *     hashring owner is this node (all children when standalone),
 *   - tsdb_cluster_maybe_forward_select is inert (a scattered partial
 *     must read local data, never bounce to another node). */
extern __thread int tsdb_g_scatter_local_mode;

/* Physical-local read for the anti-entropy row digest.  Superset of scatter-
 * local: the childless-data-bearing self-child is covered by physical presence
 * rather than hash-ownership, so a replica digests the rows it HOLDS, not only
 * the ones it OWNS.  Set only by tsdb_cluster_local_table_digest, alongside
 * scatter-local; see the definition in query/exec.c for the full rationale. */
extern __thread int tsdb_g_ae_physical_local;

/* Scatter-read child assignment: 1 when `child_name`'s primary alive
 * hashring owner is this node — walks tsdb_cluster_route() preference
 * order (TSDB_SHARD_REPLICA_N entries when sharding, every node
 * otherwise), skips non-ALIVE peers, compares the first alive owner
 * against self.  1 in standalone mode or when routing fails (fall back
 * to reading locally rather than dropping a child everywhere). */
int tsdb_cluster_child_assigned_to_self(tsdb_db_t *db, const char *child_name);

/* Coordinator fan-out: send `qtl` via FED_QUERY_LOCAL to every ALIVE peer
 * sequentially (timeout_ms per peer), collecting one decoded partial
 * result per peer into results[0..*out_n).  Returns TSDB_OK when every
 * alive peer answered.  On any peer failure returns TSDB_ERR_IO with
 * *out_failed_peer set and every already-collected result freed — a
 * missing partial would silently under-count the aggregate, so the
 * caller must surface the failure instead of merging what remains. */
int tsdb_cluster_scatter_stable_agg(tsdb_db_t *db,
                                     const char *qtl,
                                     int timeout_ms,
                                     tsdb_result_t **results, int cap,
                                     int *out_n,
                                     uint64_t *out_failed_peer);

/* Membership census: peers in this node's view (self excluded) split into
 * "known" (any state) and "alive" (what the scatter above actually dials),
 * plus the id of the first non-ALIVE peer.  Returns 1 when clustered, 0 when
 * standalone.  SHOW uses the known-vs-alive gap to refuse to call a catalog
 * listing cluster-complete while a node it cannot reach might hold more. */
int tsdb_cluster_peer_census(tsdb_db_t *db, int *out_known, int *out_alive,
                              uint64_t *out_missing_id);

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
