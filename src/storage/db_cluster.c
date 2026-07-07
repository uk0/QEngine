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
/* timegm() is a BSD extension; the strict _POSIX_C_SOURCE above would hide
 * it on Darwin without this (glibc/musl unhide it via _DEFAULT_SOURCE). */
#ifndef _DARWIN_C_SOURCE
#  define _DARWIN_C_SOURCE 1
#endif

#include "db.h"
#include "../catalog/stable.h"
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
#include <dirent.h>
#include <sys/stat.h>

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

    /* Retry until every currently-alive peer ACKs (bounded attempts).  A
     * delete the broadcast fails to deliver leaves that peer with the rows and
     * a higher count, which anti-entropy would pull back onto the deleting
     * node (resurrection).  The delete is idempotent (partition-granular), so
     * re-broadcasting to peers that already applied is a no-op.  Peers that
     * stay down are caught later by the anti-entropy watermark re-assert. */
    int acked = 0, rc = TSDB_OK;
    for (int attempt = 0; attempt < 3; attempt++) {
        acked = 0;
        rc = tsdb_replica_broadcast_delete_range(tsdb_cluster_replica_mgr(c),
                                                 table_name,
                                                 cutoff_ns, op_lt, inclusive,
                                                 peers, npeers,
                                                 &acked);
        if (rc == TSDB_OK && acked >= npeers) break;
        struct timespec bk = { 0, 50 * 1000 * 1000 };  /* 50ms backoff */
        nanosleep(&bk, NULL);
        npeers = collect_alive_peers(c, peers, TSDB_CLUSTER_MAX_NODES);
        if (out_total_peers) *out_total_peers = npeers;
        if (npeers == 0) break;
    }
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

/* Forward a catalog DDL from a raft FOLLOWER to the current raft LEADER over
 * the trusted node-to-node RPC channel (TSDB_RPC_DDL_FORWARD).  The leader's
 * apply path runs the QTL through the full local query (raft propose + catalog
 * broadcast) and returns its status text, which we copy into out_status.
 *
 * leader_id is the raft leader hint (tsdb_raft_leader_id) the caller already
 * holds; we resolve it to an RPC address via the membership snapshot — the
 * same master-by-master walk proxy_sql_to_master uses for the raft-less data
 * node, kept here so the raft-bound follower path reuses one resolution.
 *
 * Returns TSDB_OK with out_status populated on a successful round-trip; a
 * negative tsdb_err_t if the leader is unknown to membership, unreachable, or
 * answered ERR.  The leader executes locally (its state==LEADER), so a
 * forwarded DDL never re-forwards — no ping-pong. */
int tsdb_cluster_forward_ddl_to_leader(tsdb_db_t *db,
                                        uint64_t leader_id,
                                        const char *qtl,
                                        char *out_status, size_t cap)
{
    if (!db || !qtl || !out_status || cap == 0) return TSDB_ERR_INVAL;
    if (leader_id == 0) return TSDB_ERR_NOTFOUND;   /* mid-election */

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_ERR_INTERNAL;

    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr(c);
    if (!mgr) return TSDB_ERR_INTERNAL;

    /* Resolve leader_id → RPC addr from the membership snapshot. */
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(mgr, snap, TSDB_CLUSTER_MAX_NODES);
    const char *leader_addr = NULL;
    for (int i = 0; i < n; i++) {
        if (snap[i].id == (tsdb_node_id_t)leader_id) {
            if (snap[i].state == TSDB_NODE_DEAD) break;  /* known but down */
            leader_addr = snap[i].addr;
            break;
        }
    }
    if (!leader_addr || !leader_addr[0]) return TSDB_ERR_NOTFOUND;

    size_t qlen = strlen(qtl);
    if (qlen >= 4096) return TSDB_ERR_INVAL;
    uint8_t payload[4608];
    int plen = tsdb_rpc_encode_catalog_qtl(payload, sizeof(payload), qtl);
    if (plen <= 0) return TSDB_ERR_INTERNAL;

    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(leader_addr, 3000);
    if (!conn) return TSDB_ERR_IO;

    uint8_t resp[512];
    uint32_t rlen = 0;
    int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_DDL_FORWARD,
                                payload, (uint32_t)plen,
                                resp, sizeof(resp) - 1, &rlen);
    tsdb_rpc_conn_close(conn);
    if (rc != TSDB_OK) return rc;

    resp[rlen] = '\0';
    snprintf(out_status, cap, "%s", (const char *)resp);
    return TSDB_OK;
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

    /* Read count / max_ts by COLUMN NAME, not positional index.  The peer's
     * result (especially across a mixed old/new binary cluster, or any path
     * that emits aggregate columns in a different order than this SELECT) is
     * NOT guaranteed to put count at index 0.  Reading by index there once
     * delivered a max_ts-scale value as "count" (~1.3e12 vs a 3.6M row count),
     * which the anti-entropy reconcile then mistook for a peer that strictly
     * dominates us and truncated durable local data.  Match the count column
     * by the substring "count" (covers "count(ts)" / "count(*)" / "count")
     * and the max(ts) column by "max(ts)".  Fall back to the timestamp-typed
     * column for max, and to positional index only if naming fails entirely. */
    int ok = 0;
    if (tsdb_result_next(res)) {
        int ci_count = tsdb_result_col_index_by_name(res, "count");
        int ci_max   = tsdb_result_col_index_by_name(res, "max(ts)");
        if (ci_max < 0) ci_max = tsdb_result_col_index_by_name(res, "max");
        if (ci_max < 0) {
            /* No name match — take the first TIMESTAMP column as max_ts. */
            int n = tsdb_result_ncols(res);
            for (int i = 0; i < n; i++) {
                if (tsdb_result_col_type(res, i) == TSDB_TYPE_TIMESTAMP) { ci_max = i; break; }
            }
        }
        if (ci_count < 0) ci_count = 0;          /* last-resort positional */
        if (ci_max   < 0) ci_max   = 1;

        *out_count  = (uint64_t)tsdb_result_i64(res, ci_count);
        *out_max_ts = tsdb_result_ts(res, ci_max);
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

/* ---- Partition-level backfill (middle-gap convergence, env-gated) --------
 *
 * A "middle gap" (equal max_ts, higher peer count) is never resolved by the
 * table-level truncate+re-pull (that was the data-loss bug).  Instead, when
 * TSDB_AE_PARTITION_BACKFILL=1, we converge ONE cold divergent partition per
 * resync tick: pull the peer's full copy of that partition, build a temp
 * partition through the normal col-writer flush path, and atomically swap it
 * in under the table's compact mutex — the same phase-1/phase-2 pattern
 * compaction uses, including the staleness guard. */

/* Duplicate of db.c's static part_dir_to_range: parse "YYYYMMDD" /
 * "YYYYMMDDHH" into an inclusive [start_ns, end_ns] UTC range. */
static int aebf_part_dir_to_range(const char *dname, tsdb_partition_unit_t unit,
                                  int64_t *out_start, int64_t *out_end)
{
    int y, mo, dy, hh = 0;
    size_t n = strlen(dname);
    if (unit == TSDB_PARTITION_HOUR) {
        if (n != 10) return -1;
        if (sscanf(dname, "%4d%2d%2d%2d", &y, &mo, &dy, &hh) != 4) return -1;
    } else {
        if (n != 8) return -1;
        if (sscanf(dname, "%4d%2d%2d", &y, &mo, &dy) != 3) return -1;
    }
    struct tm tm = {0};
    tm.tm_year = y - 1900;
    tm.tm_mon  = mo - 1;
    tm.tm_mday = dy;
    tm.tm_hour = hh;
    time_t secs = timegm(&tm);
    if (secs == (time_t)-1) return -1;
    int64_t start = (int64_t)secs * 1000000000LL;
    int64_t span  = (unit == TSDB_PARTITION_HOUR) ? 3600000000000LL
                                                  : 86400000000000LL;
    *out_start = start;
    *out_end   = start + span - 1;
    return 0;
}

/* Best-effort removal of the scratch partition build area.  Only the files
 * this module creates (one .col/.idx pair per schema column, plus the idx
 * writer's transient .tmp) are unlinked; rmdir then fails harmlessly if
 * anything unexpected is present.  The scratch dir is dot-prefixed, so
 * scans, resync and the compactor never see it. */
static void aebf_scratch_remove(const tsdb_schema_t *s,
                                const char *scratch_part, const char *scratch)
{
    for (int ci = 0; ci < s->ncols; ci++) {
        char p[4200];
        snprintf(p, sizeof(p), "%s/%s.col", scratch_part, s->cols[ci].name);
        remove(p);
        snprintf(p, sizeof(p), "%s/%s.idx", scratch_part, s->cols[ci].name);
        remove(p);
        snprintf(p, sizeof(p), "%s/%s.idx.tmp", scratch_part, s->cols[ci].name);
        remove(p);
    }
    remove(scratch_part);
    remove(scratch);
}

extern int tsdb_mkdir_p(const char *path);

/* Core backfill primitive (exposed for unit testing): materialise `res`
 * (every row of one partition, pulled from a fuller peer) as a fresh
 * partition and atomically swap it over the live one.
 *
 * Phase 1 (no lock): replay the rows through a standalone memtable bound to
 * a schema clone whose dir points at <table>/.aebf_tmp — SYMBOL columns
 * intern through the LIVE table's shared symtabs, so the emitted codes stay
 * valid — and flush through the normal col-writer path (CRC, stats, zone
 * maps).  The live partition's durable max_seq checkpoint is carried
 * forward so WAL redo never regresses.
 *
 * Phase 2 (under compact_mtx): re-probe the live ts idx; if its total_rows
 * moved off `expected_local_rows` (a concurrent flush/truncate/delete
 * touched the partition) abort with TSDB_ERR_BUSY and leave the table
 * untouched.  Otherwise rename every .col/.idx pair into place, ts column
 * last (flush's visibility order).  scan_refs are held throughout, so
 * ALTER/DROP wait for us like they do for any scan.
 *
 * Hard invariant: refuses to swap unless the pulled row count STRICTLY
 * exceeds expected_local_rows — anti-entropy never shrinks durable data. */
int tsdb_cluster_backfill_partition_from_result(tsdb_db_t *db,
                                                const char *table_name,
                                                const char *part_name,
                                                tsdb_result_t *res,
                                                uint64_t expected_local_rows,
                                                uint64_t *out_rows_written)
{
    if (out_rows_written) *out_rows_written = 0;
    if (!db || !table_name || !part_name || !res) return TSDB_ERR_INVAL;

    tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, table_name);
    if (!t) return TSDB_ERR_NOTFOUND;
    tsdb_schema_t *s   = tsdb_tbl_schema(t);
    const char    *dir = tsdb_tbl_dir(t);
    if (!s || !dir) { tsdb_db_scan_release(db, t); return TSDB_ERR_INTERNAL; }

    int rc = TSDB_OK;
    tsdb_memtable_t *mt = NULL;

    int64_t pstart = 0, pend = 0;
    if (aebf_part_dir_to_range(part_name, s->partition_unit,
                               &pstart, &pend) != 0) {
        tsdb_db_scan_release(db, t);
        return TSDB_ERR_INVAL;
    }

    /* Map result columns to schema columns by EXACT name (the substring
     * matcher would confuse "v" with "val") and validate the types. */
    int ncols = s->ncols;
    int rmap[TSDB_MAX_COLS];
    if (ncols > TSDB_MAX_COLS || tsdb_result_ncols(res) != ncols) {
        tsdb_db_scan_release(db, t);
        return TSDB_ERR_SCHEMA;
    }
    for (int ci = 0; ci < ncols; ci++) {
        rmap[ci] = -1;
        for (int i = 0; i < ncols; i++) {
            const char *nm = tsdb_result_col_name(res, i);
            if (nm && strcmp(nm, s->cols[ci].name) == 0) { rmap[ci] = i; break; }
        }
        if (rmap[ci] < 0) { tsdb_db_scan_release(db, t); return TSDB_ERR_SCHEMA; }
        tsdb_type_t st = s->cols[ci].type;
        tsdb_type_t rt = tsdb_result_col_type(res, rmap[ci]);
        int type_ok = (st == rt) ||
                      /* FLOAT32 is normalised to FLOAT64 on result wires. */
                      (st == TSDB_TYPE_FLOAT32 && rt == TSDB_TYPE_FLOAT64);
        if (!type_ok || (ci != s->ts_col_idx && st == TSDB_TYPE_TIMESTAMP)) {
            tsdb_db_scan_release(db, t);
            return type_ok ? TSDB_ERR_UNSUPPORTED : TSDB_ERR_SCHEMA;
        }
    }

    char live_part[4096], scratch[4096], scratch_part[4096], ts_idx_live[4200];
    snprintf(live_part,    sizeof(live_part),    "%s/%s", dir, part_name);
    snprintf(scratch,      sizeof(scratch),      "%s/.aebf_tmp", dir);
    snprintf(scratch_part, sizeof(scratch_part), "%s/%s", scratch, part_name);
    snprintf(ts_idx_live,  sizeof(ts_idx_live),  "%s/%s.idx",
             live_part, s->cols[s->ts_col_idx].name);

    /* Clear any crash residue; if the scratch partition still exists after
     * that, something unexpected lives there — refuse to build on top. */
    aebf_scratch_remove(s, scratch_part, scratch);
    struct stat sst;
    if (stat(scratch_part, &sst) == 0) {
        tsdb_db_scan_release(db, t);
        return TSDB_ERR_IO;
    }

    /* Carry the live partition's durable WAL checkpoint into the rebuilt
     * idx headers so redo recovery never replays already-durable rows. */
    uint64_t keep_seq = tsdb_part_max_seq(s, live_part);

    /* Schema clone: identical layout + SHARED symtabs, dir → scratch. */
    tsdb_schema_t tmp_s = *s;
    tmp_s.dir = scratch;

    if (tsdb_memtable_new(&tmp_s, &mt) != TSDB_OK) {
        tsdb_db_scan_release(db, t);
        return TSDB_ERR_NOMEM;
    }

    size_t chunk_cap = (s->block_points > 0 &&
                        s->block_points <= TSDB_BLOCK_POINTS)
                         ? (size_t)s->block_points : (size_t)TSDB_BLOCK_POINTS;
    uint64_t rows = 0;
    size_t inmem = 0;
    while (tsdb_result_next(res)) {
        int64_t ts = tsdb_result_ts(res, rmap[s->ts_col_idx]);
        if (ts < pstart || ts > pend) { rc = TSDB_ERR_INVAL; goto fail; }
        if (tsdb_memtable_row_begin(mt) != TSDB_OK ||
            tsdb_memtable_row_ts(mt, ts) != TSDB_OK) {
            rc = TSDB_ERR_INTERNAL; goto fail;
        }
        for (int ci = 0; ci < ncols; ci++) {
            if (ci == s->ts_col_idx) continue;
            int rci = rmap[ci];
            switch (s->cols[ci].type) {
            case TSDB_TYPE_INT64:
                tsdb_memtable_row_i64(mt, ci, tsdb_result_i64(res, rci));
                break;
            case TSDB_TYPE_FLOAT64:
            case TSDB_TYPE_FLOAT32:
                tsdb_memtable_row_f64(mt, ci, tsdb_result_f64(res, rci));
                break;
            case TSDB_TYPE_SYMBOL: {
                const char *sym = tsdb_result_sym(res, rci);
                tsdb_memtable_row_sym(mt, ci, sym ? sym : "");
                break;
            }
            default:
                rc = TSDB_ERR_UNSUPPORTED; goto fail;
            }
        }
        if (tsdb_memtable_row_end(mt) != TSDB_OK) { rc = TSDB_ERR_SCHEMA; goto fail; }
        rows++;
        if (++inmem >= chunk_cap) {
            rc = tsdb_part_flush_ex2(&tmp_s, mt, NULL, NULL, keep_seq);
            if (rc != TSDB_OK) goto fail;
            tsdb_memtable_clear(mt);
            inmem = 0;
        }
    }
    if (inmem > 0) {
        rc = tsdb_part_flush_ex2(&tmp_s, mt, NULL, NULL, keep_seq);
        if (rc != TSDB_OK) goto fail;
    }

    /* Never shrink durable data: the peer copy must be strictly fuller. */
    if (rows == 0 || rows <= expected_local_rows) { rc = TSDB_ERR_INVAL; goto fail; }

    /* Newly interned symbols exist only in memory until close; persist the
     * dictionaries now so the swapped codes survive an unclean shutdown. */
    for (int ci = 0; ci < ncols; ci++) {
        if (s->cols[ci].type == TSDB_TYPE_SYMBOL && s->cols[ci].symtab) {
            char sym_path[4200];
            snprintf(sym_path, sizeof(sym_path), "%s/%s.sym",
                     dir, s->cols[ci].name);
            tsdb_symtab_save(s->cols[ci].symtab, sym_path);
        }
    }

    /* Phase 2 — swap under the compaction lock with the staleness guard. */
    {
        pthread_mutex_t *cmtx = tsdb_tbl_compact_mtx(t);
        pthread_mutex_lock(cmtx);

        uint64_t cur = 0;
        int prc = tsdb_part_idx_probe(ts_idx_live, NULL, NULL, NULL,
                                      &cur, NULL, NULL, NULL);
        if (prc < 0 || cur != expected_local_rows) {
            pthread_mutex_unlock(cmtx);
            rc = TSDB_ERR_BUSY;
            goto fail;
        }

        (void)tsdb_mkdir_p(live_part);   /* no-op when already present */

        /* Rename every column pair, ts LAST — same visibility order as the
         * flush path (readers enumerate blocks via ts.idx).  Failures are
         * logged and skipped like compaction's renames; the torn-column
         * prefix clamp keeps a partially-swapped partition readable. */
        for (int pass = 0; pass < 2; pass++) {
            for (int ci = 0; ci < ncols; ci++) {
                int is_ts = (ci == s->ts_col_idx);
                if ((pass == 0 && is_ts) || (pass == 1 && !is_ts)) continue;
                char from[4200], to[4200];
                snprintf(from, sizeof(from), "%s/%s.col", scratch_part, s->cols[ci].name);
                snprintf(to,   sizeof(to),   "%s/%s.col", live_part,    s->cols[ci].name);
                if (rename(from, to) != 0)
                    fprintf(stderr, "[anti-entropy] %s: backfill rename %s failed\n",
                            table_name, from);
                snprintf(from, sizeof(from), "%s/%s.idx", scratch_part, s->cols[ci].name);
                snprintf(to,   sizeof(to),   "%s/%s.idx", live_part,    s->cols[ci].name);
                if (rename(from, to) != 0)
                    fprintf(stderr, "[anti-entropy] %s: backfill rename %s failed\n",
                            table_name, from);
            }
        }
        pthread_mutex_unlock(cmtx);
    }

    aebf_scratch_remove(s, scratch_part, scratch);
    tsdb_memtable_free(mt);
    tsdb_db_scan_release(db, t);
    if (out_rows_written) *out_rows_written = rows;
    return TSDB_OK;

fail:
    if (mt) {
        tsdb_memtable_row_abort(mt);
        tsdb_memtable_free(mt);
    }
    aebf_scratch_remove(s, scratch_part, scratch);
    tsdb_db_scan_release(db, t);
    return rc;
}

/* One resync-tick partition backfill attempt against peer `best`.
 * Returns the number of rows swapped in (> 0) on success, 0 otherwise
 * (no qualifying partition / transport failure / staleness abort). */
#define AEBF_MAX_PARTS   1024
#define AEBF_MAX_BUCKETS 4096

static int ae_partition_backfill(tsdb_db_t *db, tsdb_cluster_t *c,
                                 tsdb_node_id_t best, const char *table_name,
                                 int64_t delwm)
{
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return 0;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(rmgr, best);
    if (!conn) return 0;

    tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, table_name);
    if (!t) return 0;
    tsdb_schema_t *s   = tsdb_tbl_schema(t);
    const char    *dir = tsdb_tbl_dir(t);
    if (!s || !dir) { tsdb_db_scan_release(db, t); return 0; }

    tsdb_partition_unit_t unit = s->partition_unit;
    int64_t span = (unit == TSDB_PARTITION_HOUR) ? 3600000000000LL
                                                 : 86400000000000LL;
    size_t name_len = (unit == TSDB_PARTITION_HOUR) ? 10 : 8;
    const char *ts_col_name = s->cols[s->ts_col_idx].name;

    /* Local per-partition map: dir name → [pstart,pend], ts.idx total_rows
     * (header probe only — no blocks opened), dir mtime. */
    typedef struct {
        char     name[12];
        int64_t  pstart, pend;
        uint64_t rows;
        time_t   mtime;
    } aebf_part_t;
    aebf_part_t *parts    = malloc(sizeof(*parts) * AEBF_MAX_PARTS);
    int64_t     *pb_start = malloc(sizeof(*pb_start) * AEBF_MAX_BUCKETS);
    uint64_t    *pb_count = malloc(sizeof(*pb_count) * AEBF_MAX_BUCKETS);
    if (!parts || !pb_start || !pb_count) {
        free(parts); free(pb_start); free(pb_count);
        tsdb_db_scan_release(db, t);
        return 0;
    }
    int nparts = 0;

    DIR *d = opendir(dir);
    if (!d) {
        free(parts); free(pb_start); free(pb_count);
        tsdb_db_scan_release(db, t);
        return 0;
    }
    struct dirent *de;
    while ((de = readdir(d)) != NULL && nparts < AEBF_MAX_PARTS) {
        size_t nl = strlen(de->d_name);
        if (nl != name_len) continue;
        int all_digit = 1;
        for (size_t i = 0; i < nl; i++) {
            if (de->d_name[i] < '0' || de->d_name[i] > '9') { all_digit = 0; break; }
        }
        if (!all_digit) continue;

        aebf_part_t p;
        snprintf(p.name, sizeof(p.name), "%s", de->d_name);
        if (aebf_part_dir_to_range(p.name, unit, &p.pstart, &p.pend) != 0)
            continue;

        char path[4200];
        struct stat st;
        snprintf(path, sizeof(path), "%s/%s", dir, p.name);
        if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        p.mtime = st.st_mtime;

        p.rows = 0;
        snprintf(path, sizeof(path), "%s/%s/%s.idx", dir, p.name, ts_col_name);
        if (tsdb_part_idx_probe(path, NULL, NULL, NULL,
                                &p.rows, NULL, NULL, NULL) < 0)
            continue;   /* corrupt idx — leave it to the torn-read clamp */

        parts[nparts++] = p;
    }
    closedir(d);
    int ret = 0;
    if (nparts == 0) goto out;

    /* Oldest-first, deterministic. */
    for (int i = 1; i < nparts; i++) {
        aebf_part_t x = parts[i];
        int j = i;
        while (j > 0 && parts[j - 1].pstart > x.pstart) { parts[j] = parts[j - 1]; j--; }
        parts[j] = x;
    }

    /* Peer per-partition map in ONE query.  time_bucket(ts, span) starts
     * are epoch-aligned, exactly like the UTC partition boundaries. */
    int nbuckets = 0;
    {
        char qtl[512];
        snprintf(qtl, sizeof(qtl),
                 "SELECT time_bucket(ts, %lld), count(*) FROM %s SAMPLE BY %s",
                 (long long)span, table_name,
                 unit == TSDB_PARTITION_HOUR ? "1h" : "1d");
        tsdb_result_t *pres = NULL;
        if (fedrpc_query(conn, qtl, 10000, &pres) != TSDB_OK || !pres) {
            if (pres) tsdb_result_free(pres);
            goto out;
        }
        int ci_b = tsdb_result_col_index_by_name(pres, "time_bucket");
        int ci_c = tsdb_result_col_index_by_name(pres, "count");
        if (ci_b < 0 || ci_c < 0) {
            tsdb_result_free(pres);
            goto out;
        }
        while (tsdb_result_next(pres) && nbuckets < AEBF_MAX_BUCKETS) {
            pb_start[nbuckets] = tsdb_result_ts(pres, ci_b);
            pb_count[nbuckets] = (uint64_t)tsdb_result_i64(pres, ci_c);
            nbuckets++;
        }
        tsdb_result_free(pres);
    }
    if (nbuckets == 0) goto out;

    /* Pick THE first qualifying partition (one per resync tick, bounding
     * the fedrpc frame and the temp-build memory):
     *   - at or above the delete watermark,
     *   - COLD: not the current bucket AND dir mtime older than 60s,
     *   - peer strictly fuller, with a plausible (non-timestamp-scale) count. */
    const uint64_t IMPLAUSIBLE_COUNT = 100000000000ULL; /* 1e11, as decide() */
    int64_t now_ns   = tsdb_now_ns();
    int64_t cur_bkt  = now_ns - (now_ns % span);
    time_t  now_s    = time(NULL);
    int pick = -1;
    uint64_t pick_peer_rows = 0;
    for (int i = 0; i < nparts; i++) {
        if (delwm > 0 && parts[i].pstart < delwm) continue;
        if (parts[i].pstart == cur_bkt) continue;
        if (now_s - parts[i].mtime <= 60) continue;
        uint64_t pc = 0;
        for (int b = 0; b < nbuckets; b++) {
            if (pb_start[b] >= parts[i].pstart && pb_start[b] <= parts[i].pend)
                pc += pb_count[b];
        }
        if (pc <= parts[i].rows || pc >= IMPLAUSIBLE_COUNT) continue;
        pick = i;
        pick_peer_rows = pc;
        break;
    }
    if (pick < 0) goto out;

    /* Pull the peer's full copy of exactly this partition. */
    {
        tsdb_result_t *rows_res = NULL;
        char qtl[512];
        snprintf(qtl, sizeof(qtl),
                 "SELECT * FROM %s WHERE ts >= %lld AND ts <= %lld",
                 table_name,
                 (long long)parts[pick].pstart, (long long)parts[pick].pend);
        if (fedrpc_query(conn, qtl, 60000, &rows_res) != TSDB_OK || !rows_res) {
            if (rows_res) tsdb_result_free(rows_res);
            goto out;
        }

        uint64_t written = 0;
        int rc = tsdb_cluster_backfill_partition_from_result(db, table_name,
                                                             parts[pick].name,
                                                             rows_res,
                                                             parts[pick].rows,
                                                             &written);
        tsdb_result_free(rows_res);

        if (rc != TSDB_OK) {
            fprintf(stderr,
                    "[anti-entropy] %s: partition %s backfill %s "
                    "(local=%llu peer_map=%llu rc=%d)\n",
                    table_name, parts[pick].name,
                    rc == TSDB_ERR_BUSY ? "aborted by staleness guard "
                                          "(concurrent write; retry next tick)"
                                        : "failed",
                    (unsigned long long)parts[pick].rows,
                    (unsigned long long)pick_peer_rows, rc);
            goto out;
        }

        fprintf(stderr,
                "[anti-entropy] %s: backfilled partition %s local=%llu -> "
                "peer=%llu rows (local-unique rows in this partition, if any, "
                "are replaced by the peer copy - MVP limitation)\n",
                table_name, parts[pick].name,
                (unsigned long long)parts[pick].rows,
                (unsigned long long)written);
        tsdb_metric_inc("qengine_antientropy_partition_backfills_total");
        ret = (int)written;
    }

out:
    free(parts);
    free(pb_start);
    free(pb_count);
    tsdb_db_scan_release(db, t);
    return ret;
}

/* Pure anti-entropy reconcile decision (exposed for unit testing).
 *
 * INVARIANT enforced here: anti-entropy must NEVER reduce a node's durable row
 * count for a table based solely on a peer count comparison.  The old recovery
 * truncated the local table and re-pulled from the peer on any "middle gap"
 * (equal max_ts, higher peer count) — so a bogus peer count, or a peer that
 * genuinely held fewer rows, permanently destroyed durable local data.  A
 * corrupt peer count (the count column read as a max_ts-scale value across a
 * mixed old/new binary cluster) turned every reconcile into a destructive
 * wipe.  Decisions:
 *
 *   - best_max_ts > local_max_ts        -> TAIL_PULL  (pull ts > local_max_ts)
 *   - best_count >= 1e11                 -> SKIP_UNSAFE (timestamp-scale count
 *                                          is corrupt; never act on it)
 *   - equal max_ts, best_count>local,
 *       local_count > 0                  -> SKIP_UNSAFE (peer not provably a
 *                                          strict superset; refuse to wipe)
 *   - equal max_ts, best_count>local,
 *       local_count == 0                 -> FULL_PULL  (nothing to lose)
 *   - otherwise                          -> UP_TO_DATE */
tsdb_ae_action_t tsdb_antientropy_decide(uint64_t local_count,
                                         int64_t  local_max_ts,
                                         uint64_t best_count,
                                         int64_t  best_max_ts)
{
    /* A plausible row count can never reach timestamp scale (ns-epoch values
     * are ~1.7e18 today; even ms-epoch is ~1.7e12). */
    const uint64_t IMPLAUSIBLE_COUNT = 100000000000ULL; /* 1e11 */

    if (best_max_ts > local_max_ts) return TSDB_AE_TAIL_PULL;

    /* From here best_max_ts == local_max_ts (peers with a lower max_ts never
     * become "best").  Only a strictly higher peer count is a candidate gap. */
    if (best_count <= local_count) return TSDB_AE_UP_TO_DATE;

    if (best_count >= IMPLAUSIBLE_COUNT) return TSDB_AE_SKIP_UNSAFE;
    if (local_count > 0)                 return TSDB_AE_SKIP_UNSAFE;
    return TSDB_AE_FULL_PULL;
}

int tsdb_cluster_resync_table(tsdb_db_t *db,
                               const char *table_name,
                               int *out_rows_pulled)
{
    if (out_rows_pulled) *out_rows_pulled = 0;
    if (!db || !table_name) return TSDB_ERR_INVAL;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_OK;

    /* Delete-watermark re-assert (makes deletes durable vs anti-entropy).
     * If this table has a persisted delete watermark W ("all ts < W deleted"),
     * (1) re-apply it locally — undoing any rows a prior refetch resurrected —
     * and (2) re-broadcast it to alive peers BEFORE the count comparison, so a
     * peer that missed the original delete (or came back with stale rows)
     * deletes them and can't poison the pull below.  Both ops are idempotent.
     * Skip while replaying a peer's delete to avoid broadcast recursion. */
    int64_t wm = tsdb_table_delwm_load(db, table_name);
    if (wm > 0) {
        int rmv = 0;
        tsdb_delete_range(db, table_name, wm, /*op_lt=*/1, /*inclusive=*/0, &rmv);
        int a = 0, tot = 0;
        tsdb_cluster_broadcast_delete_range(db, table_name, wm, 1, 0, &a, &tot);
    }

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

    /* Gap classification + safety guard (see tsdb_antientropy_decide):
     *
     *   tail gap   — best_max_ts > local_max_ts.  We missed recent writes;
     *                pulling everything past local_max_ts catches up cheaply.
     *
     *   middle gap — equal max_ts but a higher peer count.  A tail-only pull
     *                returns 0 rows, so the old code truncated + full re-pulled.
     *                That was the data-loss bug: a bogus/smaller peer count
     *                wiped durable local rows.  We now refuse to truncate a
     *                populated table (and refuse a timestamp-scale "count"
     *                outright); only an EMPTY local table is safely full-pulled.
     *                A true middle-gap backfill needs row-level reconciliation
     *                (partition Merkle), tracked separately — it must not be
     *                faked with a destructive wipe. */
    int pulled = 0;
    tsdb_ae_action_t act = tsdb_antientropy_decide(local_count, local_max_ts,
                                                   best_count, best_max_ts);
    switch (act) {
    case TSDB_AE_TAIL_PULL:
        pulled = pull_table_delta(db, c, best, table_name, local_max_ts);
        break;

    case TSDB_AE_FULL_PULL:
        /* local empty — truncate is a no-op; full pull lets a fresh node
         * converge.  since_ts = 0 catches every row (ns-epoch ts are > 0). */
        fprintf(stderr,
                "[anti-entropy] %s: empty local, full pull from peer "
                "(peer=%llu, max_ts %lld)\n",
                table_name,
                (unsigned long long)best_count,
                (long long)local_max_ts);
        if (tsdb_truncate_table(db, table_name) != TSDB_OK) {
            return TSDB_ERR_IO;
        }
        pulled = pull_table_delta(db, c, best, table_name, 0);
        break;

    case TSDB_AE_SKIP_UNSAFE: {
        /* Opt-in convergence: swap ONE cold divergent partition from the
         * fuller peer (temp build + atomic rename under compact_mtx with a
         * staleness guard).  Never reduces durable rows — the swap helper
         * refuses unless the pulled copy is strictly fuller.  Default OFF. */
        const char *bfe = getenv("TSDB_AE_PARTITION_BACKFILL");
        if (bfe && strcmp(bfe, "1") == 0 && best_count < 100000000000ULL) {
            int bf = ae_partition_backfill(db, c, best, table_name, wm);
            if (bf > 0) { pulled = bf; break; }
        }
        /* Would shrink durable data on a peer-count comparison — refuse. */
        fprintf(stderr,
                "[anti-entropy] %s: middle-gap (local=%llu/peer=%llu, "
                "equal max_ts %lld) — peer not provably a superset%s; "
                "skipping destructive truncate to preserve local rows\n",
                table_name,
                (unsigned long long)local_count,
                (unsigned long long)best_count,
                (long long)local_max_ts,
                best_count >= 100000000000ULL ? " (timestamp-scale count)" : "");
        if (out_rows_pulled) *out_rows_pulled = 0;
        return TSDB_OK;
    }

    case TSDB_AE_UP_TO_DATE:
    default:
        if (out_rows_pulled) *out_rows_pulled = 0;
        return TSDB_OK;
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

/* For each data-bearing super-table (a mirrored plain table — zero child
 * tables) the catalog knows but that has no local storage yet, create its
 * plain-table backing from the stable schema (suppress-hook, no re-broadcast).
 * Lets a node that just learned a table via tsdb_catalog_reconcile_from_peers
 * receive its rows through the normal anti-entropy pull.  Real super-tables
 * (>=1 child) are skipped — their data lives in the children. */
static void materialize_missing_stables(tsdb_db_t *db) {
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    if (!cat) return;
    tsdb_stable_t *stbl = NULL; size_t ns = 0;
    if (tsdb_stable_list(cat, &stbl, &ns) != TSDB_OK || !stbl) return;
    for (size_t i = 0; i < ns; i++) {
        tsdb_stable_t *s = &stbl[i];
        if (tsdb_db_find_table(db, s->name)) continue;          /* already local */
        tsdb_child_table_t *ch = NULL; size_t nch = 0;
        if (tsdb_child_table_list(cat, s->name, &ch, &nch) == TSDB_OK) free(ch);
        if (nch > 0) continue;                                  /* real super-table */
        if (s->ncols <= 0 || s->ts_col_idx < 0 || s->ts_col_idx >= s->ncols) continue;
        tsdb_col_t cols[TSDB_STABLE_MAX_COLS];
        for (int k = 0; k < s->ncols; k++) {
            cols[k].name = s->cols[k].name;
            cols[k].type = s->cols[k].type;
        }
        const char *ts_col = s->cols[s->ts_col_idx].name;
        if (tsdb_create_table_local(db, s->name, cols, (size_t)s->ncols, ts_col) == TSDB_OK)
            fprintf(stderr, "[catalog-reconcile] materialized learned table '%s'\n", s->name);
    }
    free(stbl);
}

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

    /* Catalog reconcile FIRST: pull every alive peer's catalog and merge it
     * resurrection-safe, so this node — master included — learns any table
     * created while it was down (e.g. an SDK table that landed in stables.log
     * on the peers).  Then materialize storage for the data-bearing ones so the
     * per-table row pull below fills them in. */
    (void)tsdb_catalog_reconcile_from_peers(db);

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

    /* Create storage for any reconciled-but-unmaterialized data-bearing table,
     * so it joins db->tables[] and gets its rows pulled by the loop below. */
    materialize_missing_stables(db);

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

/* Re-assert every table's persisted delete watermark to all alive peers.
 * Cheap (only tables that have had an op_lt delete; just an idempotent
 * broadcast, no count/pull), so it can run on a short period.  This is what
 * makes a delete durable against a peer that was down when it happened: once
 * the peer is back, the next re-assert sweep re-broadcasts the watermark and
 * the peer deletes its stale rows — converging without that peer ever having
 * to learn the watermark by itself. */
void tsdb_delwm_reassert_all(tsdb_db_t *db) {
    if (!db) return;
    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return;
    char names[TSDB_CLUSTER_MAX_NODES * 8][64];
    int max_names = (int)(sizeof(names) / sizeof(names[0]));
    int n = tsdb_db_list_table_names(db, names, max_names);
    for (int i = 0; i < n; i++) {
        int64_t wm = tsdb_table_delwm_load(db, names[i]);
        if (wm <= 0) continue;
        int a = 0, tot = 0;
        tsdb_cluster_broadcast_delete_range(db, names[i], wm, 1, 0, &a, &tot);
    }
}

/* Background thread: re-assert delete watermarks every 30s. */
void *tsdb_delwm_reassert_thread(void *ud) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return NULL;
    for (;;) {
        struct timespec ts = { 30, 0 };
        nanosleep(&ts, NULL);
        tsdb_delwm_reassert_all(db);

        /* Periodic catalog anti-entropy.  The catalog-broadcast fan-out
         * (raft-apply → tsdb_cluster_broadcast_catalog_qtl_to_data) is
         * best-effort with a single retry; a data node that misses both
         * attempts for a table/stable created AFTER its one-shot startup
         * reconcile stays diverged until its next restart.  A missed stable
         * record is worse than missing rows: SELECT count/sum/... FROM that
         * stable resolves via the plain-table path and silently under-answers
         * instead of scattering (observed live: a node with the table's
         * schema.bin but no stables.log record returns 0 for cluster data).
         * Pulling missing catalog records from peers here on the same 30s tick
         * closes that residual gap — the "catalog-level anti-entropy" the
         * broadcast path explicitly defers to.  Safe to repeat: the apply is
         * filtered + resurrection-safe (respects tombstones, adds only missing
         * records) and a no-op that logs nothing when the node is already in
         * sync.  Cost is one CATALOG_DUMP RPC per alive peer per tick. */
        (void)tsdb_catalog_reconcile_from_peers(db);
    }
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

    /* Scatter-local partials must read LOCAL data only — the stable-agg
     * coordinator already fanned the query out to every node, so a
     * second hop here would double-count (or loop). */
    if (tsdb_g_scatter_local_mode) return TSDB_OK;

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

/* ---- Cluster-wide super-table aggregation (task #175) ------------------ */

/* Scatter-read child assignment.  Deterministic single-assignee rule so a
 * scattered stable aggregate counts every child's data exactly once
 * cluster-wide: the child belongs to the FIRST ALIVE node in its hashring
 * preference order.  With TSDB_SHARD_REPLICA_N set, the preference list is
 * the owner set (the only nodes holding the child's data — if all owners
 * are down the data is unreachable and every node skips the child); with
 * sharding off, data is fully replicated, so the walk covers every node
 * and always lands on some alive assignee.  Standalone / routing failure
 * → 1 (read locally rather than dropping the child everywhere). */
int tsdb_cluster_child_assigned_to_self(tsdb_db_t *db, const char *child_name) {
    if (!db || !child_name) return 1;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return 1;                       /* cluster inactive → all local */

    int npref = shard_replica_n_cached();
    if (npref <= 0) npref = TSDB_CLUSTER_MAX_NODES;

    tsdb_node_id_t pref[TSDB_CLUSTER_MAX_NODES];
    int got = tsdb_cluster_route(c, child_name, "", npref, pref);
    if (got <= 0) return 1;                 /* routing failed → run local */

    tsdb_node_id_t self = tsdb_cluster_local_id(c);

    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr(c);
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = mgr ? tsdb_node_manager_snapshot(mgr, snap, TSDB_CLUSTER_MAX_NODES) : 0;

    for (int i = 0; i < got; i++) {
        if (pref[i] == self) return 1;      /* self is first alive owner */
        int alive = 0;
        for (int j = 0; j < n; j++) {
            if (snap[j].id == pref[i]) {
                alive = (snap[j].state == TSDB_NODE_ALIVE);
                break;
            }
        }
        if (alive) return 0;                /* an earlier owner is alive */
    }
    return 0;                               /* no owner alive (and self not
                                             * an owner) — data unreachable */
}

/* Coordinator fan-out for the stable-aggregation scatter.  Sequential
 * FED_QUERY_LOCAL to every ALIVE peer (MVP; typical cluster is 3 nodes),
 * hard per-peer timeout.  All-or-nothing: one missing partial would
 * silently under-count the aggregate, so any peer failure aborts. */
int tsdb_cluster_scatter_stable_agg(tsdb_db_t *db,
                                     const char *qtl,
                                     int timeout_ms,
                                     tsdb_result_t **results, int cap,
                                     int *out_n,
                                     uint64_t *out_failed_peer)
{
    if (out_n) *out_n = 0;
    if (out_failed_peer) *out_failed_peer = 0;
    if (!db || !qtl || !results || cap <= 0 || !out_n) return TSDB_ERR_INVAL;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return TSDB_OK;                 /* standalone — nothing to ask */

    tsdb_node_id_t peers[TSDB_CLUSTER_MAX_NODES];
    int npeers = collect_alive_peers(c, peers, TSDB_CLUSTER_MAX_NODES);
    if (npeers == 0) return TSDB_OK;
    if (npeers > cap) npeers = cap;

    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);

    int n = 0;
    for (int i = 0; i < npeers; i++) {
        tsdb_rpc_conn_t *conn =
            rmgr ? tsdb_replica_mgr_get_conn(rmgr, peers[i]) : NULL;
        tsdb_result_t *res = NULL;
        int rc = conn ? fedrpc_query_local(conn, qtl, timeout_ms, &res)
                      : TSDB_ERR_IO;
        if (rc != TSDB_OK || !res) {
            if (res) tsdb_result_free(res);
            for (int k = 0; k < n; k++) tsdb_result_free(results[k]);
            *out_n = 0;
            if (out_failed_peer) *out_failed_peer = (uint64_t)peers[i];
            return TSDB_ERR_IO;
        }
        results[n++] = res;
    }
    *out_n = n;
    return TSDB_OK;
}
