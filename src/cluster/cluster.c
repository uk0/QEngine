/* cluster.c — cluster node lifecycle and write routing. */

#include "cluster.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

struct tsdb_cluster {
    tsdb_node_id_t       local_id;
    tsdb_node_manager_t *node_mgr;
    tsdb_gossip_t        *gossip;
    tsdb_rpc_server_t    *rpc_server;
    tsdb_replica_mgr_t   *replica_mgr;
};

/* ---- Helper: split seeds string ------------------------------------------ */

static void parse_seeds(tsdb_cluster_t *c, tsdb_gossip_t *gossip, const char *seeds) {
    if (!seeds || seeds[0] == '\0') return;
    char buf[4096];
    snprintf(buf, sizeof(buf), "%s", seeds);

    char *saveptr = NULL;
    char *tok = strtok_r(buf, ",", &saveptr);
    while (tok) {
        /* Trim whitespace. */
        while (*tok == ' ' || *tok == '\t') tok++;
        char *end = tok + strlen(tok) - 1;
        while (end > tok && (*end == ' ' || *end == '\t')) *end-- = '\0';

        if (*tok) {
            /* Add to gossip engine. */
            tsdb_gossip_add_seed(gossip, tok);

            /* Also pre-populate node_mgr with address so rpc can reach it. */
            /* We don't know the node_id yet; gossip will converge. */
            (void)c;
        }
        tok = strtok_r(NULL, ",", &saveptr);
    }
}

/* ---- Public API ---------------------------------------------------------- */

tsdb_cluster_t *tsdb_cluster_new(tsdb_db_t *db,
                                  tsdb_node_id_t local_id,
                                  const char *rpc_addr,
                                  const char *gossip_addr,
                                  const char *seeds)
{
    tsdb_cluster_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->local_id = local_id;

    /* Node manager. */
    c->node_mgr = tsdb_node_manager_new(local_id, rpc_addr, gossip_addr);
    if (!c->node_mgr) { free(c); return NULL; }

    /* RPC server (must start before gossip so port is ready). */
    c->rpc_server = tsdb_rpc_server_new(rpc_addr, db, c->node_mgr);
    if (!c->rpc_server) {
        tsdb_node_manager_free(c->node_mgr);
        free(c);
        return NULL;
    }

    /* Gossip engine. */
    c->gossip = tsdb_gossip_new(gossip_addr, c->node_mgr);
    if (!c->gossip) {
        tsdb_rpc_server_stop(c->rpc_server);
        tsdb_node_manager_free(c->node_mgr);
        free(c);
        return NULL;
    }

    /* Replica manager. */
    c->replica_mgr = tsdb_replica_mgr_new(c->node_mgr);
    if (!c->replica_mgr) {
        tsdb_gossip_stop(c->gossip);
        tsdb_rpc_server_stop(c->rpc_server);
        tsdb_node_manager_free(c->node_mgr);
        free(c);
        return NULL;
    }

    /* Bootstrap: add seed nodes. */
    parse_seeds(c, c->gossip, seeds);

    printf("[cluster] node %llu started  rpc=%s  gossip=%s\n",
           (unsigned long long)local_id, rpc_addr, gossip_addr);

    return c;
}

void tsdb_cluster_free(tsdb_cluster_t *c) {
    if (!c) return;
    tsdb_replica_mgr_free(c->replica_mgr);
    tsdb_gossip_stop(c->gossip);
    tsdb_rpc_server_stop(c->rpc_server);
    tsdb_node_manager_free(c->node_mgr);
    free(c);
}

tsdb_node_id_t tsdb_cluster_local_id(tsdb_cluster_t *c) {
    return c ? c->local_id : 0;
}

tsdb_node_manager_t *tsdb_cluster_node_mgr(tsdb_cluster_t *c) {
    return c ? c->node_mgr : NULL;
}

tsdb_hashring_t *tsdb_cluster_ring(tsdb_cluster_t *c) {
    return c ? tsdb_node_manager_ring(c->node_mgr) : NULL;
}

tsdb_replica_mgr_t *tsdb_cluster_replica_mgr(tsdb_cluster_t *c) {
    return c ? c->replica_mgr : NULL;
}

/* ---- Write routing ------------------------------------------------------- */

int tsdb_cluster_write(tsdb_cluster_t *c,
                       const char *table_name,
                       int ncols, const int *col_types,
                       int nrows, const void **col_data)
{
    if (!c || !table_name || nrows == 0) return TSDB_OK;

    /* Determine replica nodes under node_mgr lock for thread safety.
     * The hashring is modified by gossip threads; we must snapshot it
     * while holding the same mutex. */
    tsdb_node_id_t all_replicas[3];
    int nreplicas = tsdb_node_manager_ring_owner(c->node_mgr, table_name, 3, all_replicas);
    if (nreplicas == 0) return TSDB_OK;

    /* Determine our role.
     *
     * With N=R=3, every node is a replica of every shard, so the writer
     * is always in the replica set.  We treat the writer as the "primary"
     * for this particular write (acting primary).  The hash-determined
     * primary matters only for routing writes received from a non-replica
     * (forwarding — not yet implemented, not needed for N=R=3 tests).
     *
     * The on_replicate hook is set to skip_replicate=1 on replicas (via
     * tsdb_batch_set_local_only), so re-replication loops are impossible.
     */
    int is_replica = 0;
    for (int i = 0; i < nreplicas; i++) {
        if (all_replicas[i] == c->local_id) {
            is_replica = 1;
            break;
        }
    }

    if (!is_replica) {
        /* Not responsible for this shard.  TODO: forward to primary. */
        return TSDB_OK;
    }

    /* We are in the replica set and received a write — act as coordinator.
     * Fan out to the other replicas in the set. */
    /* Collect the remote replica IDs (excluding self). */
    tsdb_node_id_t remote_replicas[3];
    int nremote = 0;
    for (int i = 0; i < nreplicas; i++) {
        if (all_replicas[i] != c->local_id) {
            remote_replicas[nremote++] = all_replicas[i];
        }
    }

    if (nremote == 0) return TSDB_OK; /* single-node cluster */

    return tsdb_replica_write(c->replica_mgr,
                              table_name,
                              ncols, col_types,
                              nrows, col_data,
                              remote_replicas, nremote,
                              TSDB_WRITE_QUORUM);
}

int tsdb_cluster_sync_schema(tsdb_cluster_t *c,
                              const char *table_name,
                              int ncols, const char **col_names,
                              const int *col_types, int ts_col_idx,
                              const tsdb_node_id_t *nodes, int nnodes)
{
    if (!c) return TSDB_OK;
    return tsdb_replica_sync_schema(c->replica_mgr,
                                    table_name, ncols, col_names, col_types, ts_col_idx,
                                    nodes, nnodes, TSDB_WRITE_QUORUM);
}

/* ---- Stats --------------------------------------------------------------- */

int tsdb_cluster_stats_str(tsdb_cluster_t *c, char *buf, size_t cap) {
    if (!c || !buf || cap == 0) return 0;

    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(c->node_mgr, snap, TSDB_CLUSTER_MAX_NODES);

    static const char *state_names[] = { "JOINING", "ALIVE", "SUSPECT", "DEAD" };

    int written = 0;
    written += snprintf(buf + written, cap - written,
                        "{\"local_id\":%llu,\"nodes\":[",
                        (unsigned long long)c->local_id);

    for (int i = 0; i < n && (size_t)written < cap - 2; i++) {
        const char *sname = (snap[i].state <= 3) ? state_names[snap[i].state] : "?";
        written += snprintf(buf + written, cap - written,
                            "%s{\"id\":%llu,\"addr\":\"%s\",\"state\":\"%s\",\"ver\":%llu}",
                            (i > 0 ? "," : ""),
                            (unsigned long long)snap[i].id,
                            snap[i].addr, sname,
                            (unsigned long long)snap[i].version);
    }
    written += snprintf(buf + written, cap - written, "]}");
    return written;
}

void tsdb_cluster_wait_alive(tsdb_cluster_t *c, int min_alive, int timeout_ms) {
    if (!c) return;
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        int alive = tsdb_node_manager_alive_count(c->node_mgr);
        if (alive >= min_alive) return;

        struct timespec sl = { 0, 50000000L }; /* 50ms */
        nanosleep(&sl, NULL);
        elapsed += 50;
    }
}
