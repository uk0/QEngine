/* db_cluster.c — Cluster extension entry points.
 *
 * Provides the tsdb_open_cluster / tsdb_cluster_* public API.
 * Uses db hooks (tsdb_db_set_hooks) so cluster replication fires from
 * tsdb_batch_commit and tsdb_create_table without modifying db.c's logic.
 */

/* clock_gettime + CLOCK_REALTIME are POSIX.1-2001; musl requires the
 * feature-test macro to unhide them. glibc uses _DEFAULT_SOURCE. */
#ifndef _POSIX_C_SOURCE
#  define _POSIX_C_SOURCE 200809L
#endif
#ifndef _DEFAULT_SOURCE
#  define _DEFAULT_SOURCE 1
#endif

#include "db.h"
#include "schema.h"
#include "memtable.h"
#include "../cluster/cluster.h"
#include "../cluster/node.h"
#include "../cluster/rawblock.h"
#include "../../include/tsdb_cluster.h"
#include "../core/types.h"
#include "../server/metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

/* ---- Cluster registry ---------------------------------------------------- */

#define MAX_CLUSTER_ENTRIES 16

typedef struct {
    tsdb_db_t      *db;
    tsdb_cluster_t *cluster;
} cluster_entry_t;

static cluster_entry_t g_cluster_map[MAX_CLUSTER_ENTRIES];
static int             g_nentries = 0;
static pthread_mutex_t g_cluster_lock = PTHREAD_MUTEX_INITIALIZER;

static tsdb_cluster_t *cluster_get(tsdb_db_t *db) {
    pthread_mutex_lock(&g_cluster_lock);
    for (int i = 0; i < g_nentries; i++) {
        if (g_cluster_map[i].db == db) {
            tsdb_cluster_t *c = g_cluster_map[i].cluster;
            pthread_mutex_unlock(&g_cluster_lock);
            return c;
        }
    }
    pthread_mutex_unlock(&g_cluster_lock);
    return NULL;
}

static void cluster_set(tsdb_db_t *db, tsdb_cluster_t *cluster) {
    pthread_mutex_lock(&g_cluster_lock);
    for (int i = 0; i < g_nentries; i++) {
        if (g_cluster_map[i].db == db) {
            g_cluster_map[i].cluster = cluster;
            pthread_mutex_unlock(&g_cluster_lock);
            return;
        }
    }
    if (g_nentries < MAX_CLUSTER_ENTRIES) {
        g_cluster_map[g_nentries].db      = db;
        g_cluster_map[g_nentries].cluster = cluster;
        g_nentries++;
    }
    pthread_mutex_unlock(&g_cluster_lock);
}

static void cluster_remove(tsdb_db_t *db) {
    pthread_mutex_lock(&g_cluster_lock);
    for (int i = 0; i < g_nentries; i++) {
        if (g_cluster_map[i].db == db) {
            g_cluster_map[i] = g_cluster_map[--g_nentries];
            break;
        }
    }
    pthread_mutex_unlock(&g_cluster_lock);
}

/* ---- Hook implementations ------------------------------------------------ */

/*
 * on_replicate: called from flush_and_clear (before each local memtable flush).
 * userdata is the tsdb_cluster_t *.
 * We extract columnar data from the memtable and fan-out via tsdb_cluster_write.
 */
static int cluster_on_replicate(void *ud, tsdb_db_t *db,
                                  const char *table_name,
                                  tsdb_schema_t *schema,
                                  tsdb_memtable_t *memtable)
{
    (void)db;
    tsdb_cluster_t *c = (tsdb_cluster_t *)ud;
    if (!c || !schema || !memtable) return TSDB_OK;

    int nrows = (int)tsdb_memtable_rows(memtable);
    if (nrows == 0) return TSDB_OK;

    int ncols = schema->ncols;

    /* Build col_types and col_data arrays from schema + memtable. */
    int col_types[TSDB_MAX_COLS];
    const void *col_data[TSDB_MAX_COLS];

    for (int c_idx = 0; c_idx < ncols; c_idx++) {
        col_types[c_idx] = (int)schema->cols[c_idx].type;
        col_data[c_idx]  = tsdb_memtable_col(memtable, c_idx);
    }

    return tsdb_cluster_write(c, table_name,
                              ncols, col_types,
                              nrows, col_data);
}

/*
 * on_create: called from tsdb_create_table after local success.
 * Fans schema out to all other known ALIVE nodes.
 */
static int cluster_on_create(void *ud, tsdb_db_t *db,
                               const char *table_name,
                               tsdb_schema_t *schema)
{
    (void)db;
    tsdb_cluster_t *c = (tsdb_cluster_t *)ud;
    if (!c || !schema) return TSDB_OK;

    int ncols = schema->ncols;
    const char *col_names[TSDB_MAX_COLS];
    int col_types[TSDB_MAX_COLS];
    int ts_col_idx = 0;

    for (int i = 0; i < ncols; i++) {
        col_names[i] = schema->cols[i].name;
        col_types[i] = (int)schema->cols[i].type;
        if (schema->cols[i].type == TSDB_TYPE_TIMESTAMP) ts_col_idx = i;
    }

    /* Get all alive nodes except self. */
    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr(c);
    tsdb_node_info_t nodes[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(mgr, nodes, TSDB_CLUSTER_MAX_NODES);

    tsdb_node_id_t target_ids[TSDB_CLUSTER_MAX_NODES];
    int ntargets = 0;
    tsdb_node_id_t local_id = tsdb_cluster_local_id(c);

    for (int i = 0; i < n; i++) {
        if (nodes[i].id != local_id && nodes[i].state == TSDB_NODE_ALIVE) {
            target_ids[ntargets++] = nodes[i].id;
        }
    }

    if (ntargets == 0) return TSDB_OK;

    return tsdb_cluster_sync_schema(c, table_name,
                                    ncols, col_names, col_types, ts_col_idx,
                                    target_ids, ntargets);
}

/* ---- Raw-block replication hook ------------------------------------------ */

/*
 * cluster_on_raw_block: called by tsdb_part_flush_ex after each block encode.
 * Activated when TSDB_REPLICATION_MODE=raw.
 * Forwards the compressed block bytes to all ALIVE peers via
 * TSDB_RPC_RAW_BLOCK_PUSH, one block at a time, concurrently.
 */
static int cluster_on_raw_block(void *ud, tsdb_db_t *db,
                                  const char *table_name,
                                  uint32_t part_day,
                                  uint16_t col_idx,
                                  const tsdb_block_meta_t *meta,
                                  const uint8_t *block_bytes,
                                  size_t block_bytes_len)
{
    (void)db;
    tsdb_cluster_t *c = (tsdb_cluster_t *)ud;
    if (!c || !table_name || !meta) return TSDB_OK;

    tsdb_metric_inc("qengine_rawblock_pushes_total");

    /* quorum_w=2: local write counts as 1, need 1 remote ACK. */
    return tsdb_rawblock_replicate(c, table_name, part_day, col_idx,
                                   meta, block_bytes, block_bytes_len, 2);
}

/* ---- Node ID generation -------------------------------------------------- *
 *
 * Persist the id to <data_dir>/node_id.  Same data_dir (same container
 * volume, same physical host) keeps the same id across restarts, so
 * peer gossip state doesn't accumulate a trail of ghost DEAD entries
 * every time a node is recreated.  First boot generates a fresh id
 * seeded by bind address + current time; subsequent boots read the file.
 */
static tsdb_node_id_t generate_node_id(const char *data_dir, const char *addr) {
    char path[4096];
    if (data_dir) {
        snprintf(path, sizeof(path), "%s/node_id", data_dir);
        FILE *f = fopen(path, "r");
        if (f) {
            uint64_t v = 0;
            if (fscanf(f, "%llu", (unsigned long long *)&v) == 1 && v != 0) {
                fclose(f);
                return (tsdb_node_id_t)v;
            }
            fclose(f);
        }
    }

    /* Fresh id — FNV-1a of (addr || time_ns). */
    uint64_t h = 14695981039346656037ULL;
    if (addr) {
        for (const char *p = addr; *p; p++) {
            h ^= (uint8_t)*p;
            h *= 1099511628211ULL;
        }
    }
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t t = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    h ^= t;
    h *= 1099511628211ULL;
    if (h == 0) h = 1ULL;

    /* Persist it atomically: write to tmp + rename.  Best-effort — if
     * the write fails we just return the id and next restart will
     * generate a fresh one (degrading gracefully to the old behaviour). */
    if (data_dir) {
        char tmp[4096];
        snprintf(tmp, sizeof(tmp), "%s/node_id.tmp", data_dir);
        FILE *f = fopen(tmp, "w");
        if (f) {
            fprintf(f, "%llu\n", (unsigned long long)h);
            fflush(f);
            fclose(f);
            (void)rename(tmp, path);
        }
    }
    return h;
}

/* ---- Public cluster API -------------------------------------------------- */

int tsdb_open_cluster(const char *data_dir,
                      const char *bind_addr,
                      const char *gossip_seeds,
                      tsdb_db_t **out)
{
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(data_dir, &db);
    if (rc != TSDB_OK) return rc;

    /* Derive gossip addr: same host, rpc_port - 1. */
    char gossip_addr[64];
    {
        char host[128] = "0.0.0.0";
        int  rpc_port  = 28081;
        if (bind_addr) {
            const char *colon = strrchr(bind_addr, ':');
            if (colon) {
                size_t hlen = (size_t)(colon - bind_addr);
                if (hlen < sizeof(host)) {
                    memcpy(host, bind_addr, hlen);
                    host[hlen] = '\0';
                }
                rpc_port = atoi(colon + 1);
            }
        }
        snprintf(gossip_addr, sizeof(gossip_addr), "%s:%d", host, rpc_port - 1);
    }

    tsdb_node_id_t node_id = generate_node_id(data_dir, bind_addr);

    tsdb_cluster_t *cluster = tsdb_cluster_new(db, node_id,
                                               bind_addr  ? bind_addr  : "0.0.0.0:28081",
                                               gossip_addr,
                                               gossip_seeds);
    if (!cluster) {
        tsdb_close(db);
        return TSDB_ERR_IO;
    }

    /* Register hooks so tsdb_batch_commit and tsdb_create_table trigger
     * replication without db.c needing to know about the cluster module.
     *
     * Replication mode selection:
     *   TSDB_REPLICATION_MODE=raw  → raw-block path (encode-once, send bytes)
     *   TSDB_REPLICATION_MODE=row  → row-level WRITE_BATCH (default)
     */
    {
        const char *rep_mode = getenv("TSDB_REPLICATION_MODE");
        int use_raw = rep_mode && (rep_mode[0] == 'r' && rep_mode[1] == 'a');

        /* Always register on_create for schema sync. */
        tsdb_db_set_hooks(db,
                          use_raw ? NULL : cluster_on_replicate,
                          cluster_on_create,
                          cluster);

        if (use_raw) {
            tsdb_db_set_raw_block_hook(db, cluster_on_raw_block, cluster);
        }
    }

    cluster_set(db, cluster);
    *out = db;
    return TSDB_OK;
}

int tsdb_cluster_stats(tsdb_db_t *db, char *buf, size_t cap) {
    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return 0;
    return tsdb_cluster_stats_str(c, buf, cap);
}

int tsdb_cluster_alive_count(tsdb_db_t *db) {
    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return 1;
    return tsdb_node_manager_alive_count(tsdb_cluster_node_mgr(c));
}

void tsdb_cluster_wait_ready(tsdb_db_t *db, int min_nodes, int timeout_ms) {
    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return;
    tsdb_cluster_wait_alive(c, min_nodes, timeout_ms);
}

void tsdb_close_cluster(tsdb_db_t *db) {
    tsdb_cluster_t *c = cluster_get(db);
    if (c) {
        cluster_remove(db);
        tsdb_cluster_free(c);
    }
}

/* ---- Broadcast helpers (called after local DELETE/TRUNCATE success) ---- */

/* Collect ALIVE peer IDs (excluding self). */
static int collect_alive_peers(tsdb_cluster_t *c,
                                tsdb_node_id_t *out, int cap)
{
    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr(c);
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(mgr, snap, TSDB_CLUSTER_MAX_NODES);
    tsdb_node_id_t self = tsdb_cluster_local_id(c);

    int k = 0;
    for (int i = 0; i < n && k < cap; i++) {
        if (snap[i].id == self) continue;
        if (snap[i].state != TSDB_NODE_ALIVE) continue;
        out[k++] = snap[i].id;
    }
    return k;
}

int tsdb_cluster_broadcast_truncate(tsdb_db_t *db,
                                     const char *table_name,
                                     int *out_acked_peers,
                                     int *out_total_peers)
{
    if (out_acked_peers) *out_acked_peers = 0;
    if (out_total_peers) *out_total_peers = 0;
    if (!db || !table_name) return TSDB_ERR_INVAL;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_OK;  /* standalone — nothing to do */

    tsdb_node_id_t peers[TSDB_CLUSTER_MAX_NODES];
    int npeers = collect_alive_peers(c, peers, TSDB_CLUSTER_MAX_NODES);
    if (out_total_peers) *out_total_peers = npeers;
    if (npeers == 0) return TSDB_OK;

    int acked = 0;
    int rc = tsdb_replica_broadcast_truncate(tsdb_cluster_replica_mgr(c),
                                             table_name,
                                             peers, npeers,
                                             &acked);
    if (out_acked_peers) *out_acked_peers = acked;
    return rc;
}

int tsdb_cluster_broadcast_delete_range(tsdb_db_t *db,
                                         const char *table_name,
                                         int64_t cutoff_ns,
                                         int op_lt, int inclusive,
                                         int *out_acked_peers,
                                         int *out_total_peers)
{
    if (out_acked_peers) *out_acked_peers = 0;
    if (out_total_peers) *out_total_peers = 0;
    if (!db || !table_name) return TSDB_ERR_INVAL;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_OK;

    tsdb_node_id_t peers[TSDB_CLUSTER_MAX_NODES];
    int npeers = collect_alive_peers(c, peers, TSDB_CLUSTER_MAX_NODES);
    if (out_total_peers) *out_total_peers = npeers;
    if (npeers == 0) return TSDB_OK;

    int acked = 0;
    int rc = tsdb_replica_broadcast_delete_range(tsdb_cluster_replica_mgr(c),
                                                  table_name,
                                                  cutoff_ns, op_lt, inclusive,
                                                  peers, npeers,
                                                  &acked);
    if (out_acked_peers) *out_acked_peers = acked;
    return rc;
}

/* Thread-local re-entry guard.  Set on the peer side (RPC handler)
 * before nested tsdb_query so the resulting catalog writes don't
 * re-broadcast and ping-pong forever. */
__thread int tsdb_g_suppress_catalog_broadcast = 0;

int tsdb_cluster_broadcast_catalog_qtl(tsdb_db_t *db,
                                        const char *qtl,
                                        int *out_acked_peers,
                                        int *out_total_peers)
{
    if (out_acked_peers) *out_acked_peers = 0;
    if (out_total_peers) *out_total_peers = 0;
    if (!db || !qtl) return TSDB_ERR_INVAL;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_OK;

    tsdb_node_id_t peers[TSDB_CLUSTER_MAX_NODES];
    int npeers = collect_alive_peers(c, peers, TSDB_CLUSTER_MAX_NODES);
    if (out_total_peers) *out_total_peers = npeers;
    if (npeers == 0) return TSDB_OK;

    int acked = 0;
    int rc = tsdb_replica_broadcast_catalog_qtl(tsdb_cluster_replica_mgr(c),
                                                 qtl, peers, npeers, &acked);
    if (out_acked_peers) *out_acked_peers = acked;
    return rc;
}
