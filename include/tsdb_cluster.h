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

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_H_PUBLIC */
