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
#include <strings.h> /* strcasecmp for TSDB_NODE_ROLE parsing */
#include "../cluster/node.h"
#include "../cluster/rawblock.h"
#include "../cluster/replica.h"
#include "../cluster/rpc.h"
#include "../federation/fedrpc.h"
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

/* Public accessor — modules outside db_cluster.c (e.g. catalog_sync)
 * need the cluster handle to enumerate peers / pick a master. */
struct tsdb_cluster *tsdb_db_cluster(tsdb_db_t *db) {
    return cluster_get(db);
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

/* Forward decl for the Phase γ helper used in cluster_on_replicate's
 * Phase β.2 owner check (definition is further down in the file). */
static int shard_replica_n_cached(void);

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

    /* Build col_types[] + col_data[] arrays from schema + memtable.
     *
     * Symbol columns can't ship as raw codes — every node has its
     * own symtab so the receiver's interpretation of a code differs
     * from the sender's.  Instead we resolve each row's code to the
     * source string here and pack a wire-friendly buffer:
     *
     *   [u32 total_bytes][u16 len_0][bytes_0]…[u16 len_n-1][bytes_n-1]
     *
     * The encoder ships this verbatim; the receiver re-interns the
     * strings into its own symtab.  Buffers are heap-allocated and
     * freed after cluster_write returns. */
    int col_types[TSDB_MAX_COLS];
    const void *col_data[TSDB_MAX_COLS];
    void *resolved_buf[TSDB_MAX_COLS] = {0};

    for (int c_idx = 0; c_idx < ncols; c_idx++) {
        col_types[c_idx] = (int)schema->cols[c_idx].type;
        if (schema->cols[c_idx].type == TSDB_TYPE_SYMBOL) {
            const uint32_t *codes =
                (const uint32_t *)tsdb_memtable_col(memtable, c_idx);
            tsdb_symtab_t *st = schema->cols[c_idx].symtab;
            /* Pre-size: assume max symbol length 64 bytes in the worst
             * case; resize if a longer symbol shows up. */
            size_t cap = 4 + (size_t)nrows * (2 + 32);
            uint8_t *buf = malloc(cap);
            if (!buf) goto oom;
            size_t off = 4;  /* skip u32 total header for now */
            for (int r = 0; r < nrows; r++) {
                const char *s = (codes && st)
                    ? tsdb_symtab_str(st, codes[r]) : NULL;
                if (!s) s = "";
                size_t slen = strlen(s);
                if (slen > 65535) slen = 65535;
                if (off + 2 + slen > cap) {
                    cap = (off + 2 + slen) * 2;
                    uint8_t *nb = realloc(buf, cap);
                    if (!nb) { free(buf); goto oom; }
                    buf = nb;
                }
                uint16_t l16 = (uint16_t)slen;
                memcpy(buf + off, &l16, 2);          off += 2;
                memcpy(buf + off, s,    slen);       off += slen;
            }
            uint32_t total = (uint32_t)(off - 4);
            memcpy(buf, &total, 4);
            resolved_buf[c_idx] = buf;
            col_data[c_idx]     = buf;
        } else {
            col_data[c_idx] = tsdb_memtable_col(memtable, c_idx);
        }
    }

    int rc = tsdb_cluster_write(c, table_name, ncols, col_types,
                                nrows, col_data);

    for (int i = 0; i < ncols; i++) free(resolved_buf[i]);

    /* Phase β.2 — drop the local copy on non-owner nodes once the
     * owners have ACKed the rows.  Cheap-ish to check (one route
     * lookup per flush) and turning it into TSDB_SKIP_LOCAL upstream
     * lets db.c clear the memtable + truncate the WAL instead of
     * persisting a redundant on-disk shard.  Conservative: only
     * activates when the cluster_write call returned OK (ownership
     * verified by the per-peer ACKs that fanout_wait_quorum gates
     * on); on failure we fall through to local persist so the row
     * isn't lost on flaky owners. */
    if (rc == TSDB_OK && shard_replica_n_cached() > 0) {
        tsdb_node_id_t owners[TSDB_CLUSTER_MAX_NODES];
        int got = tsdb_cluster_route(c, table_name, "",
                                     shard_replica_n_cached(), owners);
        if (got > 0) {
            tsdb_node_id_t self = tsdb_cluster_local_id(c);
            int self_is_owner = 0;
            for (int i = 0; i < got; i++) {
                if (owners[i] == self) { self_is_owner = 1; break; }
            }
            if (!self_is_owner) return TSDB_SKIP_LOCAL;
        }
    }
    return rc;

oom:
    for (int i = 0; i < ncols; i++) free(resolved_buf[i]);
    return TSDB_ERR_NOMEM;
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
                                    (int)schema->partition_unit,
                                    schema->block_points,
                                    schema->sort_by_tag_col,
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

    /* Pick up the master/data role from the env.  Default is MASTER so
     * every pre-existing data dir keeps accepting DDL via the legacy
     * fanout path until the operator explicitly opts into `data`. */
    tsdb_node_role_t local_role = TSDB_ROLE_MASTER;
    {
        const char *r = getenv("TSDB_NODE_ROLE");
        if (r && (!strcasecmp(r, "data") || !strcasecmp(r, "dnode"))) {
            local_role = TSDB_ROLE_DATA;
        }
    }

    tsdb_cluster_t *cluster = tsdb_cluster_new(db, node_id,
                                               bind_addr  ? bind_addr  : "0.0.0.0:28081",
                                               gossip_addr,
                                               gossip_seeds,
                                               local_role);
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

/* Collect alive non-master peers — used by the Raft apply callback on
 * the master side to broadcast catalog DDL (CREATE DATABASE, DROP
 * DATABASE, …) to data nodes.  Other masters apply the same entry
 * via Raft so they don't need the broadcast; data nodes don't run
 * Raft at all and would otherwise never see these statements. */
static int collect_alive_data_peers(tsdb_cluster_t *c,
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
        if (snap[i].role == TSDB_ROLE_MASTER) continue;
        out[k++] = snap[i].id;
    }
    return k;
}

int tsdb_cluster_broadcast_catalog_qtl_to_data(tsdb_db_t *db,
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
    int npeers = collect_alive_data_peers(c, peers, TSDB_CLUSTER_MAX_NODES);
    if (out_total_peers) *out_total_peers = npeers;
    if (npeers == 0) return TSDB_OK;

    int acked = 0;
    int rc = tsdb_replica_broadcast_catalog_qtl(tsdb_cluster_replica_mgr(c),
                                                 qtl, peers, npeers, &acked);
    if (out_acked_peers) *out_acked_peers = acked;
    return rc;
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

/* Thread-local re-entry guards.
 *
 * tsdb_g_suppress_catalog_broadcast
 *   Set on the peer side of the legacy fanout RPC handler before
 *   nested tsdb_query.  Prevents ping-pong in fanout mode.
 *
 * tsdb_g_inside_raft_apply
 *   Set on the Raft apply thread before replaying a committed QTL.
 *   Tells exec.c "you're already on the consensus-blessed path,
 *   skip the propose-and-wait and just execute locally."  Both
 *   flags are set together during apply — Raft's serialization is
 *   the only fan-out that happens, the local execution is quiet. */
__thread int tsdb_g_suppress_catalog_broadcast = 0;
__thread int tsdb_g_inside_raft_apply          = 0;

/* ---- Bridges for modules that live above the cluster layer ----------
 * raft.c wants node_mgr + replica_mgr + local_id but has no reason to
 * pull in the full cluster.h (which would pull gossip / autobalance).
 * These thin helpers live here because db_cluster.c already owns the
 * db->cluster mapping. */
tsdb_node_manager_t *tsdb_cluster_node_mgr_for_db(tsdb_db_t *db) {
    tsdb_cluster_t *c = cluster_get(db);
    return c ? tsdb_cluster_node_mgr(c) : NULL;
}
tsdb_replica_mgr_t *tsdb_cluster_replica_mgr_for_db(tsdb_db_t *db) {
    tsdb_cluster_t *c = cluster_get(db);
    return c ? tsdb_cluster_replica_mgr(c) : NULL;
}
uint64_t tsdb_cluster_local_id_for_db(tsdb_db_t *db) {
    tsdb_cluster_t *c = cluster_get(db);
    return c ? (uint64_t)tsdb_cluster_local_id(c) : 0;
}

/* ---- Raft binding -----------------------------------------------------
 * Map tsdb_db_t* → tsdb_raft_t* so exec.c can reach the state machine
 * without pulling raft.h into every call site.  Parallels cluster_get
 * but in a small private map because raft is optional. */

#define RAFT_MAP_CAP 32
typedef struct {
    tsdb_db_t  *db;
    tsdb_raft_t *raft;
} raft_entry_t;

static raft_entry_t   g_raft_map[RAFT_MAP_CAP];
static int            g_raft_nentries = 0;
static pthread_mutex_t g_raft_lock = PTHREAD_MUTEX_INITIALIZER;

void tsdb_db_bind_raft(tsdb_db_t *db, tsdb_raft_t *raft) {
    if (!db) return;
    pthread_mutex_lock(&g_raft_lock);
    for (int i = 0; i < g_raft_nentries; i++) {
        if (g_raft_map[i].db == db) {
            g_raft_map[i].raft = raft;
            pthread_mutex_unlock(&g_raft_lock);
            return;
        }
    }
    if (g_raft_nentries < RAFT_MAP_CAP) {
        g_raft_map[g_raft_nentries].db   = db;
        g_raft_map[g_raft_nentries].raft = raft;
        g_raft_nentries++;
    }
    pthread_mutex_unlock(&g_raft_lock);
}

tsdb_raft_t *tsdb_db_raft_for_db(tsdb_db_t *db) {
    if (!db) return NULL;
    pthread_mutex_lock(&g_raft_lock);
    tsdb_raft_t *r = NULL;
    for (int i = 0; i < g_raft_nentries; i++) {
        if (g_raft_map[i].db == db) { r = g_raft_map[i].raft; break; }
    }
    pthread_mutex_unlock(&g_raft_lock);
    return r;
}

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

/* ---- Anti-entropy resync ------------------------------------------------ */

/* Ask peer `peer_id` for (count, max_ts) of `table_name`.  Returns 0
 * on success with out_count / out_max_ts populated; -1 on failure. */
static int peer_table_stats(tsdb_cluster_t *c,
                             tsdb_node_id_t peer_id,
                             const char *table_name,
                             uint64_t *out_count,
                             int64_t  *out_max_ts)
{
    if (!c || !table_name) return -1;

    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return -1;

    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(rmgr, peer_id);
    if (!conn) return -1;

    char qtl[256];
    snprintf(qtl, sizeof(qtl),
             "SELECT count(*), max(ts) FROM %s", table_name);

    tsdb_result_t *res = NULL;
    int rc = fedrpc_query(conn, qtl, 5000, &res);
    if (rc != TSDB_OK || !res) {
        if (res) tsdb_result_free(res);
        return -1;
    }

    int ok = 0;
    if (tsdb_result_next(res)) {
        *out_count  = (uint64_t)tsdb_result_i64(res, 0);
        *out_max_ts = tsdb_result_ts(res, 1);
        ok = 1;
    }
    tsdb_result_free(res);
    return ok ? 0 : -1;
}

/* Pull every row at ts > since_ts from `peer_id` and insert locally
 * with local_only = 1 so the write does not re-fan-out.  Returns the
 * number of rows inserted, or -1 on transport / schema failure. */
static int pull_table_delta(tsdb_db_t *db,
                             tsdb_cluster_t *c,
                             tsdb_node_id_t peer_id,
                             const char *table_name,
                             int64_t since_ts)
{
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return -1;

    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(rmgr, peer_id);
    if (!conn) return -1;

    char qtl[256];
    snprintf(qtl, sizeof(qtl),
             "SELECT * FROM %s WHERE ts > %lld",
             table_name, (long long)since_ts);

    tsdb_result_t *res = NULL;
    int rc = fedrpc_query(conn, qtl, 60000, &res);
    if (rc != TSDB_OK || !res) {
        if (res) tsdb_result_free(res);
        return -1;
    }

    int ncols = tsdb_result_ncols(res);
    if (ncols < 1) { tsdb_result_free(res); return 0; }

    tsdb_table_t *tbl = NULL;
    if (tsdb_open_table(db, table_name, &tbl) != TSDB_OK || !tbl) {
        tsdb_result_free(res);
        return -1;
    }

    int ts_ci = -1;
    for (int i = 0; i < ncols; i++) {
        if (tsdb_result_col_type(res, i) == TSDB_TYPE_TIMESTAMP) {
            ts_ci = i; break;
        }
    }
    if (ts_ci < 0) { tsdb_result_free(res); return -1; }

    tsdb_table_lock_write(tbl);
    tsdb_batch_t *batch = NULL;
    if (tsdb_batch_begin(tbl, &batch) != TSDB_OK) {
        tsdb_table_unlock_write(tbl);
        tsdb_result_free(res);
        return -1;
    }
    tsdb_batch_set_local_only(batch);

    int pulled = 0;
    while (tsdb_result_next(res)) {
        tsdb_batch_row_ts(batch, tsdb_result_ts(res, ts_ci));
        for (int i = 0; i < ncols; i++) {
            if (i == ts_ci) continue;
            switch (tsdb_result_col_type(res, i)) {
            case TSDB_TYPE_INT64:
                tsdb_batch_row_i64(batch, i, tsdb_result_i64(res, i));
                break;
            case TSDB_TYPE_FLOAT64:
                tsdb_batch_row_f64(batch, i, tsdb_result_f64(res, i));
                break;
            case TSDB_TYPE_SYMBOL: {
                /* Resolve the symbol via the source's symtab (already
                 * decoded in tsdb_result_sym) and re-intern locally.
                 * Without this case the column would never be set
                 * and row_end would fail with TSDB_ERR_SCHEMA — every
                 * pulled row silently dropped on tables that have a
                 * symbol column (i.e. most IoT / metrics tables). */
                const char *s = tsdb_result_sym(res, i);
                tsdb_batch_row_sym(batch, i, s ? s : "");
                break;
            }
            default:
                break;
            }
        }
        tsdb_batch_row_end(batch);
        pulled++;
    }

    if (tsdb_batch_commit(batch) != TSDB_OK) {
        tsdb_table_unlock_write(tbl);
        tsdb_result_free(res);
        return -1;
    }
    tsdb_table_unlock_write(tbl);
    tsdb_result_free(res);
    return pulled;
}

int tsdb_cluster_resync_table(tsdb_db_t *db,
                               const char *table_name,
                               int *out_rows_pulled)
{
    if (out_rows_pulled) *out_rows_pulled = 0;
    if (!db || !table_name) return TSDB_ERR_INVAL;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_OK;

    uint64_t local_count = 0;
    int64_t  local_max_ts = 0;
    {
        char qtl[256];
        snprintf(qtl, sizeof(qtl),
                 "SELECT count(*), max(ts) FROM %s", table_name);
        tsdb_result_t *res = NULL;
        if (tsdb_query(db, qtl, &res) == TSDB_OK && res &&
            tsdb_result_next(res))
        {
            local_count  = (uint64_t)tsdb_result_i64(res, 0);
            local_max_ts = tsdb_result_ts(res, 1);
        }
        if (res) tsdb_result_free(res);
    }

    /* Find the peer with the highest (count, max_ts). */
    tsdb_node_id_t peers[TSDB_CLUSTER_MAX_NODES];
    int npeers = collect_alive_peers(c, peers, TSDB_CLUSTER_MAX_NODES);
    tsdb_node_id_t best = 0;
    uint64_t best_count = local_count;
    int64_t  best_max_ts = local_max_ts;

    for (int i = 0; i < npeers; i++) {
        uint64_t pc = 0; int64_t pmax = 0;
        if (peer_table_stats(c, peers[i], table_name, &pc, &pmax) != 0) continue;
        if (pc > best_count ||
            (pc == best_count && pmax > best_max_ts))
        {
            best = peers[i];
            best_count = pc;
            best_max_ts = pmax;
        }
    }

    if (best == 0) return TSDB_OK; /* already up-to-date */

    /* Gap classification:
     *
     *   tail gap   — best_max_ts > local_max_ts.  We missed recent
     *                writes; pulling everything past local_max_ts
     *                catches up cheaply.
     *
     *   middle gap — best_max_ts == local_max_ts but best_count >
     *                local_count.  We were down for some interval in
     *                the middle of the burst (e.g. chaos kill+restart),
     *                missed those rows, but were back in time to receive
     *                later batches that include the highest-ts row.
     *                A tail-only pull (ts > local_max_ts) returns 0 rows
     *                and the gap stays open forever — observed during
     *                consistency_audit phase 3.
     *
     * For the middle-gap case fall back to a simple but correct
     * recovery: truncate the local table and pull the entire dataset
     * from the peer.  This is cheap on the test scale and converges
     * deterministically; for production-scale tables a partition-level
     * Merkle is the right answer (tracked separately). */
    int pulled = 0;
    if (best_max_ts > local_max_ts) {
        pulled = pull_table_delta(db, c, best, table_name, local_max_ts);
    } else {
        /* Middle gap — tail pull won't catch this.  Reset + full pull. */
        fprintf(stderr,
                "[anti-entropy] %s: middle-gap detected "
                "(local=%llu/peer=%llu, equal max_ts %lld); "
                "truncate + full re-pull\n",
                table_name,
                (unsigned long long)local_count,
                (unsigned long long)best_count,
                (long long)local_max_ts);
        if (tsdb_truncate_table(db, table_name) != TSDB_OK) {
            return TSDB_ERR_IO;
        }
        /* since_ts = 0 catches every row in this DB — nano-second epoch
         * timestamps are always > 0, so the > 0 predicate is equivalent
         * to "all rows". */
        pulled = pull_table_delta(db, c, best, table_name, 0);
    }

    if (pulled < 0) return TSDB_ERR_IO;
    if (out_rows_pulled) *out_rows_pulled = pulled;
    tsdb_metric_add("qengine_antientropy_rows_pulled_total", (uint64_t)pulled);
    return TSDB_OK;
}

/* ---- Anti-entropy startup thread ---------------------------------------- */

#include <pthread.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

/* True for directories that represent a user table (excludes dot-
 * prefixed files, "catalog", "wal", "raft" plumbing dirs). */
static int resync_is_table_dir(const char *name) {
    if (!name || name[0] == '.' || name[0] == '_') return 0;
    if (strcmp(name, "catalog") == 0 ||
        strcmp(name, "wal")     == 0 ||
        strcmp(name, "raft")    == 0) return 0;
    return 1;
}

/* Detached worker: sleep long enough for gossip + replication to settle,
 * then walk the data_dir to populate db->tables[] (which is otherwise
 * empty after a fresh restart), and for each known table call
 * tsdb_cluster_resync_table to pull any missing tail from a peer. */
void *tsdb_resync_startup_thread(void *ud) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return NULL;

    int delay_ms = 5000;
    const char *envd = getenv("TSDB_ANTIENTROPY_DELAY_MS");
    if (envd && *envd) delay_ms = atoi(envd);
    struct timespec ts = { .tv_sec = delay_ms / 1000,
                           .tv_nsec = (long)(delay_ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);

    /* Pre-open every on-disk table across every striped data dir so
     * db->tables[] reflects them.  Falls back to a single-dir scan
     * when striping is disabled.  tsdb_open_table itself routes the
     * (later) opens to the correct dir via db_resolve_table_dir(). */
    int ndirs = tsdb_db_data_dir_count(db);
    for (int di = 0; di < ndirs; di++) {
        const char *dd = tsdb_db_data_dir_at(db, di);
        if (!dd) continue;
        DIR *d = opendir(dd);
        if (!d) continue;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (!resync_is_table_dir(de->d_name)) continue;
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s", dd, de->d_name);
            struct stat st;
            if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
            tsdb_table_t *tbl = NULL;
            (void)tsdb_open_table(db, de->d_name, &tbl);
        }
        closedir(d);
    }

    char names[TSDB_CLUSTER_MAX_NODES * 8][64];
    int max_names = (int)(sizeof(names) / sizeof(names[0]));
    int n = tsdb_db_list_table_names(db, names, max_names);
    fprintf(stderr, "[anti-entropy] scanning %d tables for gaps\n", n);

    int total = 0;
    for (int i = 0; i < n; i++) {
        int pulled = 0;
        if (tsdb_cluster_resync_table(db, names[i], &pulled) == TSDB_OK &&
            pulled > 0)
        {
            total += pulled;
            fprintf(stderr, "[anti-entropy] %s: pulled %d rows\n",
                    names[i], pulled);
        }
    }
    fprintf(stderr, "[anti-entropy] startup catch-up: %d rows across %d tables\n",
            total, n);
    return NULL;
}

/* ---- Phase γ — read-side forwarding for shard mode --------------------
 *
 * When TSDB_SHARD_REPLICA_N is set and this node is NOT in the owner
 * set for a table, forward the SELECT QTL to the first owner via the
 * existing FED_QUERY RPC and return the encoded result.  The owner
 * runs the query locally and ships rows back; the caller stitches the
 * result into the standard tsdb_result_t pipeline.
 *
 * The thread-local guard tsdb_g_inside_shard_forward prevents the
 * forwarded query from re-entering this code path on the owner side
 * (which would loop indefinitely if route ever returns the wrong set
 * — defensive only; the route is idempotent under stable membership).
 *
 * Returns:
 *   TSDB_OK     + *out=NULL → caller continues with local exec
 *   TSDB_OK     + *out set  → forward succeeded, return *out
 *   error code              → forward failed, surface the error
 */
__thread int tsdb_g_inside_shard_forward = 0;

static int shard_replica_n_cached(void) {
    static int cached = -1;
    if (cached < 0) {
        const char *s = getenv("TSDB_SHARD_REPLICA_N");
        cached = s && *s ? atoi(s) : 0;
        if (cached < 0) cached = 0;
    }
    return cached;
}

int tsdb_cluster_maybe_forward_select(tsdb_db_t *db,
                                       const char *table_name,
                                       const char *qtl,
                                       tsdb_result_t **out)
{
    if (out) *out = NULL;
    if (!db || !table_name || !qtl || !out) return TSDB_OK;

    /* Re-entry guard: a forwarded query landing here would loop. */
    if (tsdb_g_inside_shard_forward) return TSDB_OK;

    int n = shard_replica_n_cached();
    if (n <= 0) return TSDB_OK;  /* shard mode off → run local */

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_OK;

    tsdb_node_id_t owners[TSDB_CLUSTER_MAX_NODES];
    int got = tsdb_cluster_route(c, table_name, "", n, owners);
    if (got <= 0) return TSDB_OK;  /* routing failed → fall back to local */

    tsdb_node_id_t self = tsdb_cluster_local_id(c);
    for (int i = 0; i < got; i++) {
        if (owners[i] == self) return TSDB_OK;  /* I'm an owner → run local */
    }

    /* Self is non-owner.  Pick the first owner and forward.  Future
     * work: load-balance across owners or fan-out + merge for
     * partition-pruning gains. */
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return TSDB_OK;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(rmgr, owners[0]);
    if (!conn) return TSDB_ERR_IO;

    tsdb_g_inside_shard_forward = 1;
    int rc = fedrpc_query(conn, qtl, 5000, out);
    tsdb_g_inside_shard_forward = 0;
    if (rc != TSDB_OK) {
        if (*out) { tsdb_result_free(*out); *out = NULL; }
        return rc;
    }
    return TSDB_OK;
}
