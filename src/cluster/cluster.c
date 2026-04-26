/* cluster.c — cluster node lifecycle and write routing. */

#include "cluster.h"
#include "autobalance.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stddef.h>

struct tsdb_cluster {
    tsdb_node_id_t       local_id;
    tsdb_node_manager_t *node_mgr;
    tsdb_gossip_t        *gossip;
    tsdb_rpc_server_t    *rpc_server;
    tsdb_replica_mgr_t   *replica_mgr;
    tsdb_autobalance_t   *autobalance;
    char                  data_dir[4096]; /* for autobalance storage scan */
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

/* Signature must stay in sync with cluster.h — adds local_role for
 * master/data split introduced alongside the Raft consensus layer. */
tsdb_cluster_t *tsdb_cluster_new(tsdb_db_t *db,
                                  tsdb_node_id_t local_id,
                                  const char *rpc_addr,
                                  const char *gossip_addr,
                                  const char *seeds,
                                  tsdb_node_role_t local_role)
{
    tsdb_cluster_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->local_id = local_id;

    /* Node manager. */
    c->node_mgr = tsdb_node_manager_new(local_id, rpc_addr, gossip_addr, local_role);
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

    /* Auto-balance controller — data_dir defaults to empty (storage scan disabled). */
    const char *data_dir = getenv("TSDB_DATA_DIR");
    if (data_dir) snprintf(c->data_dir, sizeof(c->data_dir), "%s", data_dir);
    c->autobalance = tsdb_autobalance_new(c->node_mgr, local_id,
                                          c->data_dir[0] ? c->data_dir : NULL);
    /* Non-fatal if autobalance fails (e.g. in unit tests). */

    printf("[cluster] node %llu started  rpc=%s  gossip=%s\n",
           (unsigned long long)local_id, rpc_addr, gossip_addr);

    return c;
}

void tsdb_cluster_free(tsdb_cluster_t *c) {
    if (!c) return;
    tsdb_autobalance_free(c->autobalance);
    tsdb_replica_mgr_free(c->replica_mgr);
    tsdb_gossip_stop(c->gossip);
    tsdb_rpc_server_stop(c->rpc_server);
    tsdb_node_manager_free(c->node_mgr);
    free(c);
}

tsdb_autobalance_t *tsdb_cluster_autobalance(tsdb_cluster_t *c) {
    return c ? c->autobalance : NULL;
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

    /* Replicate to every alive non-self node.
     *
     * The previous implementation used a consistent-hash ring to pick
     * max-R replicas (R=3 by default) and only replicated when the
     * writer happened to be in that set.  That was fine when N (cluster
     * size) == R because every writer was always a replica — but once
     * the cluster grew beyond R, writes landing on a non-replica were
     * silently dropped from the replication path: the row stayed on
     * whichever node the client hit, and queries against any other
     * node returned empty.  Fanout to every alive peer is the right
     * default for the "any node should answer any query" model the
     * rest of the system (DDL via Raft, dashboard load balancing)
     * already assumes.  Sharding/forwarding can be reintroduced when
     * the read path learns to combine partial results.
     *
     * The hook on the receiver side marks the batch local-only, so no
     * secondary re-replication loop is possible. */
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int ntotal = tsdb_node_manager_snapshot(c->node_mgr, snap,
                                             TSDB_CLUSTER_MAX_NODES);
    tsdb_node_id_t remote_replicas[TSDB_CLUSTER_MAX_NODES];
    int nremote = 0;
    for (int i = 0; i < ntotal && nremote < TSDB_CLUSTER_MAX_NODES; i++) {
        if (snap[i].id == c->local_id) continue;
        if (snap[i].state == TSDB_NODE_DEAD) continue;
        remote_replicas[nremote++] = snap[i].id;
    }

    /* Track write load for the auto-balancer. */
    tsdb_autobalance_record_write(c->autobalance, (uint64_t)nrows);

    if (nremote == 0) return TSDB_OK; /* single-node cluster */

    /* Quorum tunable.  TSDB_REPLICATION_QUORUM env:
     *   1 (default) — wait for ≥1 remote ACK before commit returns.
     *                 Strongest "one peer has it durable" guarantee.
     *   0           — async fan-out: spawn worker threads, return
     *                 immediately.  ~3-10× higher commit throughput
     *                 on networked clusters but commit OK no longer
     *                 implies any peer has the row durable yet.
     *                 Anti-entropy still closes the gap.
     *   N>1         — full Raft-style quorum of N remote ACKs.  Use
     *                 only if writes must survive the simultaneous
     *                 loss of (N_total - N) machines. */
    static int cached_quorum = -1;
    if (cached_quorum < 0) {
        const char *q = getenv("TSDB_REPLICATION_QUORUM");
        cached_quorum = q && *q ? atoi(q) : 1;
        if (cached_quorum < 0) cached_quorum = 0;
    }
    int q = cached_quorum;
    if (q > nremote) q = nremote;

    return tsdb_replica_write(c->replica_mgr,
                              table_name,
                              ncols, col_types,
                              nrows, col_data,
                              remote_replicas, nremote,
                              q);
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

/* ---- Sharding routing API (Phase α) ------------------------------------- */

int tsdb_cluster_route(tsdb_cluster_t *c,
                       const char *table_name,
                       const char *key,
                       int replica_n,
                       tsdb_node_id_t *out_ids)
{
    if (!c || !table_name || !out_ids || replica_n <= 0) return -1;
    tsdb_hashring_t *ring = tsdb_node_manager_ring(c->node_mgr);
    if (!ring) return 0;

    /* Compose a stable hash key: "<table>\1<key>".  The separator is
     * an explicit byte (not NUL, which would truncate strlen-based
     * helpers).  The table name acts as a salt so distinct tables
     * don't share the same primary for an empty user key. */
    char buf[256];
    size_t tlen = strnlen(table_name, sizeof(buf) - 2);
    if (tlen >= sizeof(buf) - 2) return -1;
    memcpy(buf, table_name, tlen);
    buf[tlen++] = '\1';
    size_t klen = key ? strnlen(key, sizeof(buf) - tlen - 1) : 0;
    if (klen) memcpy(buf + tlen, key, klen);
    size_t total = tlen + klen;

    /* Cap replica_n to the live ring size so callers don't get
     * duplicate IDs back when N_node < replica_n. */
    int rn = tsdb_hashring_node_count(ring);
    if (replica_n > rn) replica_n = rn;
    if (replica_n <= 0) return 0;

    return tsdb_hashring_owner(ring, buf, total, replica_n, out_ids);
}

/* ---- Stats --------------------------------------------------------------- */

int tsdb_cluster_stats_str(tsdb_cluster_t *c, char *buf, size_t cap) {
    if (!c || !buf || cap == 0) return 0;

    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(c->node_mgr, snap, TSDB_CLUSTER_MAX_NODES);

    static const char *state_names[] = { "JOINING", "ALIVE", "SUSPECT", "DEAD" };
    static const char *role_names[]  = { "master", "data" };

    /* Snapshot monotonic clock once so per-node ages agree. */
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t now = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;

    int written = 0;
    /* Emit ids as STRINGS.  tsdb_node_id_t is uint64_t; any value above
     * 2^53 (≈ 9e15) silently loses precision when JSON.parse rounds it
     * into a JS Number — the dashboard saw ids ending in "…000" because
     * of that round trip.  Strings preserve every digit and also match
     * how the dashboard already keys its join tables. */
    written += snprintf(buf + written, cap - written,
                        "{\"local_id\":\"%llu\",\"nodes\":[",
                        (unsigned long long)c->local_id);

    for (int i = 0; i < n && (size_t)written < cap - 2; i++) {
        const char *sname = (snap[i].state <= 3) ? state_names[snap[i].state] : "?";
        /* hb_age_ms: wall time since we last observed a heartbeat from
         *            this node (ms).  >3000ms is already suspicious.
         * known_for_s: seconds since first upsert into local node_mgr.
         *              Proxy for per-peer uptime as observed by this node.
         * Both are local-clock based — peers may have different wall
         * clocks, so computing on the server side is the right place. */
        int64_t hb_age_ms = 0;
        if (snap[i].last_heartbeat_ns > 0 && now > snap[i].last_heartbeat_ns)
            hb_age_ms = (now - snap[i].last_heartbeat_ns) / 1000000LL;
        int64_t known_for_s = 0;
        if (snap[i].first_seen_ns > 0 && now > snap[i].first_seen_ns)
            known_for_s = (now - snap[i].first_seen_ns) / 1000000000LL;

        const char *rname = (snap[i].role <= 1) ? role_names[snap[i].role] : "?";
        written += snprintf(buf + written, cap - written,
                            "%s{\"id\":\"%llu\",\"addr\":\"%s\",\"state\":\"%s\","
                            "\"role\":\"%s\","
                            "\"ver\":%llu,\"hb_age_ms\":%lld,\"known_for_s\":%lld,"
                            "\"suspect_count\":%d}",
                            (i > 0 ? "," : ""),
                            (unsigned long long)snap[i].id,
                            snap[i].addr, sname, rname,
                            (unsigned long long)snap[i].version,
                            (long long)hb_age_ms,
                            (long long)known_for_s,
                            snap[i].suspect_count);
    }
    written += snprintf(buf + written, cap - written, "]");

    /* Append autobalance stats if available. */
    if (c->autobalance && (size_t)written < cap - 2) {
        char ab_buf[1024];
        int ab_len = tsdb_autobalance_stats_str(c->autobalance, ab_buf, sizeof(ab_buf));
        if (ab_len > 0) {
            written += snprintf(buf + written, cap - written,
                                ",\"autobalance\":%s", ab_buf);
        }
    }

    if ((size_t)written < cap - 1)
        written += snprintf(buf + written, cap - written, "}");
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
