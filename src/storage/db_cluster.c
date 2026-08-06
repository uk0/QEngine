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
#include "part.h"    /* tsdb_part_idx_probe — local row/ts measurement */
#include "antientropy.h" /* pull-candidate ranking + bounded retry driver */
#include "../cluster/cluster.h"
#include <strings.h> /* strcasecmp for TSDB_NODE_ROLE parsing */
#include "../cluster/node.h"
#include "../cluster/rawblock.h"
#include "../cluster/replica.h"
#include "../cluster/rpc.h"
#include "../cluster/merkle.h" /* row-range digest: middle-hole detection */
#include "../federation/fedrpc.h"
#include "../../include/tsdb_cluster.h"
#include "../core/types.h"
#include "../server/metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>    /* open() — fsync the partition dir after the backfill swap */
#include <unistd.h>

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

    int remote_acks = 0;
    int rc = tsdb_cluster_write(c, table_name, ncols, col_types,
                                nrows, col_data, &remote_acks,
                                schema->incarnation);

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
    /* Phase 1 (owner-ACK-gated SKIP_LOCAL): only drop this non-owner's local
     * copy once at least one REMOTE owner has DURABLY confirmed the rows.
     * The old test `rc == TSDB_OK` was unsafe under TSDB_REPLICATION_QUORUM=0:
     * async fan-out returns OK with zero remote ACKs, so the memtable was
     * cleared and the WAL truncated (db.c) while the rows still lived only in
     * a volatile fan-out queue — a crash there lost them.  With quorum=0
     * (remote_acks stays 0) we now KEEP the local copy (durable) and let
     * anti-entropy converge; the sharded-storage drop still fires whenever a
     * remote owner ACK was actually obtained (quorum>=1). */
    if (rc == TSDB_OK && remote_acks >= 1 && shard_replica_n_cached() > 0) {
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
                                    target_ids, ntargets,
                                    schema->incarnation);
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

    /* Persist it atomically AND DURABLY: write to tmp, fsync the tmp so its
     * bytes are on the device before the rename names it, rename, then fsync
     * the directory so the rename itself survives a crash.
     *
     * The node id is the replication ISSUER — it is the identity the block
     * ordinal's .ordmap keys on and the natural stream identity for write-batch
     * dedup.  If it is not durable, a crash between the rename and the metadata
     * flush loses the file, the next restart mints a FRESH id, and every batch
     * this node already replicated looks like it came from a new sender: old
     * blocks appear new, dedup state is defeated.  fflush+fclose+rename (the
     * old path) guarantees none of that — fclose only flushes stdio into the
     * page cache, and the rename's dirent is not durable until the dir is
     * fsynced.  Still best-effort on the error paths: a write failure returns
     * the in-memory id and degrades to the old regenerate-on-restart behaviour,
     * which is no worse than before. */
    if (data_dir) {
        char tmp[4096];
        snprintf(tmp, sizeof(tmp), "%s/node_id.tmp", data_dir);
        FILE *f = fopen(tmp, "w");
        if (f) {
            fprintf(f, "%llu\n", (unsigned long long)h);
            int ok = (fflush(f) == 0) && !ferror(f);
            if (ok) {
                int fd = fileno(f);
                if (fd >= 0 && fsync(fd) != 0) ok = 0;   /* bytes -> device */
            }
            if (fclose(f) != 0) ok = 0;
            if (ok && rename(tmp, path) == 0) {
                int dfd = open(data_dir, O_RDONLY | O_DIRECTORY);
                if (dfd >= 0) { (void)fsync(dfd); close(dfd); }  /* rename -> durable */
            } else {
                (void)unlink(tmp);
            }
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

/* tsdb_g_raft_apply_seed
 *   The committed Raft entry's (term, index), mixed into one non-zero word, set
 *   by the apply callback for the duration of one QTL.  A CREATE replayed from
 *   Raft must NOT mint a fresh table incarnation — every node replays the SAME
 *   entry, so minting gave each node a different value for the same table and
 *   the WRITE_BATCH incarnation gate then rejected replication in both
 *   directions (fixed in 7cc45e1 by stamping UNKNOWN(0), which is safe but
 *   leaves the gate inert in a Raft cluster).  (term, index) is consensus-agreed
 *   and unique per creation event, so deriving the incarnation from it gives
 *   every node the SAME non-zero value AND changes on a DROP+recreate — the
 *   gate works again without any node disagreeing.  0 = not applying a Raft
 *   entry. */
__thread uint64_t tsdb_g_raft_apply_seed = 0;

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
    /* Clear BEFORE any other early return.  The caller (exec.c) now branches on
     * "is out_status non-empty" to tell "the leader refused this statement" from
     * "nobody answered", and its buffer is an uninitialised stack array — every
     * path out of here has to leave a defined answer in it. */
    out_status[0] = '\0';
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

    /* Copy the body FIRST, whatever the verdict.  The leader now answers ERR
     * for a statement its query engine rejected (it used to ACK and put the
     * failure text in the body for us to string-match), and that text is the
     * only description of what actually went wrong — losing it on the way out
     * would trade one silent failure for another.  out_status is left EMPTY
     * when the peer sent no body, which is how the caller tells "the leader
     * ran it and refused" from "we never got an answer".
     *
     * rlen is the WIRE payload length, not the number of bytes copied into
     * resp — tsdb_rpc_call_recv reports resp.payload_len and copies at most
     * resp_cap.  `resp[rlen] = 0` on a longer-than-511-byte reply is a stack
     * write past the array; today no responder emits one, but this path is
     * newly reachable on ERR and must not depend on that. */
    if (rlen >= sizeof(resp)) rlen = (uint32_t)sizeof(resp) - 1;
    if (rlen > 0) {
        resp[rlen] = '\0';
        snprintf(out_status, cap, "%s", (const char *)resp);
    }
    return rc;
}

/* ---- Anti-entropy resync ------------------------------------------------ */

/* Ask the peer behind `conn` for (count, max_ts) of `table_name`.  Returns 0
 * on success with out_count / out_max_ts populated; -1 on failure.
 * Exposed (via include/tsdb_cluster.h) so the probe can be unit-tested
 * against a real RPC server and against a stub that models an old peer.
 *
 * PREFERRED PATH — TSDB_RPC_LOCAL_TABLE_STATS: the peer measures ITSELF with
 * tsdb_cluster_local_table_stats and hands back its own (count, max_ts).
 *
 * Why that matters: `SELECT count(*), max(ts) FROM <t>` is NOT a measurement
 * of the peer.  Hierarchy mirroring registers every plain table as a childless
 * data-bearing super-table, so exec routes the query through exec_stable_select
 * and, on a peer that holds NO local rows, the cluster-agg coordinator fires
 * and the peer answers with the CLUSTER-WIDE aggregate — byte-identical to
 * what the real holder reports.  The selection loop below then cannot tell the
 * two apart, and if it lands on the empty one the follow-up
 * `SELECT * FROM <t> WHERE ts > N` (not a scalar agg, so no coordinator)
 * returns nothing: the resync reports success having converged zero rows, and
 * the startup resync runs exactly once, so that is permanent.  The tree
 * already fixed the LOCAL half of this comparison for the same reason
 * (tsdb_cluster_local_table_stats, tests/test_ae_local_stats.c); this is the
 * symmetric peer half.
 *
 * FALLBACK — a peer running an older binary has no such opcode: its dispatch
 * hits `default:` and ACKs with an empty body, which the client helper reports
 * as TSDB_ERR_UNSUPPORTED.  We then run exactly the query this function has
 * always run, so a mixed-version cluster keeps behaving as it does today
 * rather than erroring.  A transport failure degrades the same way (the legacy
 * path gets one more chance on the same pooled conn).
 *
 * NOT a fallback: TSDB_ERR_INTERNAL, i.e. the peer knows the opcode and could
 * not answer.  That is a peer which failed to measure itself, and guessing on
 * its behalf with a query that can return the cluster's numbers is what this
 * change exists to stop; treat it as "no answer" so the peer is skipped. */
int tsdb_cluster_peer_table_stats_conn(struct tsdb_rpc_conn *conn,
                                       const char *table_name,
                                       uint64_t *out_count,
                                       int64_t  *out_max_ts)
{
    if (!conn || !table_name || !out_count || !out_max_ts) return -1;

    uint64_t lc = 0;
    int64_t  lm = 0;
    int prc = tsdb_rpc_local_table_stats(conn, table_name, &lc, &lm);
    if (prc == TSDB_OK) {
        *out_count  = lc;
        *out_max_ts = lm;
        return 0;
    }
    if (prc == TSDB_ERR_INTERNAL) return -1;   /* understood, could not answer */

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

/* Resolve peer_id to a pooled conn and probe it.  Returns 0 on success,
 * -1 if the peer is undialable or did not answer. */
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

    return tsdb_cluster_peer_table_stats_conn(conn, table_name,
                                              out_count, out_max_ts);
}

/* Pull every row at ts > since_ts from `peer_id` and insert locally
 * with local_only = 1 so the write does not re-fan-out.  Returns the
 * number of rows inserted, or -1 on transport / schema failure. */
/* Defined further down; used by the tail pull for content-dedup. */
int tsdb_ae_local_bucket_hashes(tsdb_db_t *db, const char *table,
                                int64_t bstart, int64_t bend,
                                uint64_t **out, size_t *out_n);
int tsdb_ae_merge_result_dedup(tsdb_db_t *db, const char *table,
                               tsdb_result_t *peer_res,
                               const uint64_t *lset, size_t ln,
                               int *out_inserted);

static int pull_table_delta(tsdb_db_t *db,
                             tsdb_cluster_t *c,
                             tsdb_node_id_t peer_id,
                             const char *table_name,
                             int64_t since_ts)
{
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return -1;
    if (since_ts == INT64_MAX) return 0;    /* nothing can be newer */

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

    /* CONTENT-DEDUP the tail pull.  This used to insert every fetched row
     * unconditionally, which duplicates rows whenever ANOTHER repair path
     * delivers the same range concurrently — and that is the normal case right
     * after a peer reconnects: anti-entropy pulls the gap while the sender's
     * replication catch-up pushes the very same rows.  Measured live: a node
     * cut off for 5 batches came back holding those 250 rows TWICE
     * (count 1250 vs 1000, sum 644375 vs 500500, every v in [451,700] present
     * exactly twice) — a silent over-count with rc=OK.
     *
     * Reuse the digest merge's tested primitive: hash what we already hold in
     * the pulled range, then insert only rows whose content is absent.  The set
     * is built AFTER the network fetch so the race window excludes the whole
     * round trip, and in the common case it is EMPTY (nothing local can be
     * newer than the max_ts we pulled from) — so this costs nothing and only
     * bites in exactly the raced overlap.  It also fixes two latent bugs in the
     * old loop: FLOAT32 columns were silently dropped (`default: break` wrote
     * no value), and an unknown column type produced a partial row instead of a
     * loud failure.
     *
     * Trade-off, inherited from the same primitive the digest uses: a row whose
     * content is byte-identical to one we already hold is not inserted, so a
     * legitimately-duplicated identical row is not re-created.  Preferring that
     * over silently doubling a reconnect's worth of rows. */
    uint64_t *lset = NULL;
    size_t ln = 0;
    if (tsdb_ae_local_bucket_hashes(db, table_name, since_ts + 1, INT64_MAX,
                                    &lset, &ln) != TSDB_OK) {
        tsdb_result_free(res);
        return -1;
    }

    int inserted = 0;
    int mrc = tsdb_ae_merge_result_dedup(db, table_name, res, lset, ln,
                                         &inserted);
    tsdb_result_free(res);
    free(lset);
    return mrc == TSDB_OK ? inserted : -1;
}

/* ---- Row-range digest repair: content-dedup bucket merge ----------------- */

static int rd_u64_cmp(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}
static int rd_u64_member(const uint64_t *sorted, size_t n, uint64_t key) {
    return n && bsearch(&key, sorted, n, sizeof(uint64_t), rd_u64_cmp) != NULL;
}

/* Sorted content-hash set of the rows THIS node holds in [bstart, bend], read
 * local-only (same scatter-local guard as the digest).  The dedup key for a
 * bucket merge.  Exposed (with tsdb_ae_merge_result_dedup) so the merge is
 * testable in-process without a live peer.  *out malloc'd; caller frees. */
int tsdb_ae_local_bucket_hashes(tsdb_db_t *db, const char *table,
                                int64_t bstart, int64_t bend,
                                uint64_t **out, size_t *out_n)
{
    if (!db || !table || !out || !out_n) return TSDB_ERR_INVAL;
    *out = NULL;
    *out_n = 0;

    char qtl[320];
    snprintf(qtl, sizeof(qtl),
             "SELECT * FROM %s WHERE ts >= %lld AND ts <= %lld",
             table, (long long)bstart, (long long)bend);

    /* PHYSICAL-local, exactly like tsdb_cluster_local_table_digest.  This set is
     * "what rows do I already hold" — the dedup key that stops the merge from
     * re-inserting rows this node physically has.  Reading it scatter-local ALONE
     * (ownership-filtered) returns EMPTY on a node that physically holds rows it
     * does not OWN, so the dedup set was empty, EVERY pulled row looked missing,
     * and the merge duplicated the whole bucket — the real engine of the
     * 2026-08-02 runaway (a co-replica pulled 1000 rows onto its own 600 and
     * became 1600).  tsdb_g_ae_physical_local lifts the self-child's gate to
     * physical presence so the set reflects what is HELD, not what is OWNED. */
    tsdb_result_t *lres = NULL;
    int prev  = tsdb_g_scatter_local_mode;
    int pprev = tsdb_g_ae_physical_local;
    tsdb_g_scatter_local_mode = 1;
    tsdb_g_ae_physical_local  = 1;
    int rc = tsdb_query(db, qtl, &lres);
    tsdb_g_scatter_local_mode = prev;
    tsdb_g_ae_physical_local  = pprev;
    if (rc != TSDB_OK || !lres) {
        if (lres) tsdb_result_free(lres);
        return rc == TSDB_OK ? TSDB_ERR_INTERNAL : rc;
    }

    uint64_t *lset = NULL;
    size_t ln = 0, lcap = 0;
    int lncols = tsdb_result_ncols(lres);
    while (tsdb_result_next(lres)) {
        if (ln == lcap) {
            size_t nc = lcap ? lcap * 2 : 64;
            uint64_t *nn = realloc(lset, nc * sizeof(uint64_t));
            if (!nn) { free(lset); tsdb_result_free(lres); return TSDB_ERR_NOMEM; }
            lset = nn; lcap = nc;
        }
        lset[ln++] = tsdb_rowdigest_row_hash(lres, lncols);
    }
    tsdb_result_free(lres);
    if (ln > 1) qsort(lset, ln, sizeof(uint64_t), rd_u64_cmp);
    *out = lset;
    *out_n = ln;
    return TSDB_OK;
}

/* Insert (local_only) every row of `peer_res` whose canonical content hash is
 * NOT already in the sorted set lset[0..ln).  Never deletes a local row; never
 * inserts a content-duplicate — so pulling a whole bucket adds only the rows
 * this node was missing, and a replica legitimately AHEAD in the bucket keeps
 * its extra rows.  *out_inserted (may be NULL) = rows added. */
int tsdb_ae_merge_result_dedup(tsdb_db_t *db, const char *table,
                               tsdb_result_t *peer_res,
                               const uint64_t *lset, size_t ln,
                               int *out_inserted)
{
    if (out_inserted) *out_inserted = 0;
    if (!db || !table || !peer_res) return TSDB_ERR_INVAL;

    int ncols = tsdb_result_ncols(peer_res);
    if (ncols < 1) return TSDB_OK;             /* nothing to merge */
    int ts_ci = -1;
    for (int i = 0; i < ncols; i++)
        if (tsdb_result_col_type(peer_res, i) == TSDB_TYPE_TIMESTAMP) { ts_ci = i; break; }
    if (ts_ci < 0) return TSDB_ERR_INVAL;

    tsdb_table_t *tbl = NULL;
    if (tsdb_open_table(db, table, &tbl) != TSDB_OK || !tbl) return TSDB_ERR_INTERNAL;
    tsdb_table_lock_write(tbl);
    tsdb_batch_t *batch = NULL;
    if (tsdb_batch_begin(tbl, &batch) != TSDB_OK) {
        tsdb_table_unlock_write(tbl);
        return TSDB_ERR_INTERNAL;
    }
    tsdb_batch_set_local_only(batch);

    int inserted = 0;
    while (tsdb_result_next(peer_res)) {
        /* Skip content this node already holds — the merge never duplicates. */
        if (rd_u64_member(lset, ln, tsdb_rowdigest_row_hash(peer_res, ncols)))
            continue;
        tsdb_batch_row_ts(batch, tsdb_result_ts(peer_res, ts_ci));
        int reproducible = 1;
        for (int i = 0; i < ncols; i++) {
            if (i == ts_ci) continue;
            switch (tsdb_result_col_type(peer_res, i)) {
            case TSDB_TYPE_INT64:
                tsdb_batch_row_i64(batch, i, tsdb_result_i64(peer_res, i)); break;
            case TSDB_TYPE_FLOAT64:
            case TSDB_TYPE_FLOAT32:
                tsdb_batch_row_f64(batch, i, tsdb_result_f64(peer_res, i)); break;
            case TSDB_TYPE_SYMBOL: {
                const char *s = tsdb_result_sym(peer_res, i);
                tsdb_batch_row_sym(batch, i, s ? s : ""); break;
            }
            default:
                /* A column type the row hash accounts for but this merge cannot
                 * faithfully rebuild (a non-primary TIMESTAMP column, or a NULL
                 * once null-tracking lands — both unreachable today, see the
                 * digest reviewer note).  Reproducing it wrong would make the
                 * merged row hash differ from the peer's, so the bucket would
                 * never converge and would re-pull every sweep — an unbounded
                 * repair loop.  Abandon the whole merge loudly instead: discard
                 * the batch and return an error so this sweep does no partial,
                 * hash-diverging insert.  The bucket stays a visible mismatch;
                 * a later sweep retries.  Guards a future schema/null change;
                 * unreachable and free today. */
                reproducible = 0;
                break;
            }
            if (!reproducible) break;
        }
        if (!reproducible) {
            tsdb_batch_discard(batch);
            tsdb_table_unlock_write(tbl);
            return TSDB_ERR_UNSUPPORTED;
        }
        tsdb_batch_row_end(batch);
        inserted++;
    }

    int crc = tsdb_batch_commit(batch);
    tsdb_table_unlock_write(tbl);
    if (crc != TSDB_OK) return TSDB_ERR_IO;
    if (out_inserted) *out_inserted = inserted;
    return TSDB_OK;
}

/* Content-dedup merge of ONE divergent ts bucket [bstart, bend] from `peer_id`.
 *
 * A digest mismatch at EQUAL (count, max_ts) means each replica holds interior
 * rows the other lacks — neither is provably a superset, so the bucket must NOT
 * be truncated and re-pulled (that is the data-loss bug this whole path exists
 * to avoid).  Thin wrapper over the two testable halves: build the local dedup
 * set, pull the peer's copy of the bucket, merge the rows we are missing.
 * Returns rows inserted (>= 0), or -1 on a transport/schema failure. */
static int pull_bucket_merge(tsdb_db_t *db, tsdb_cluster_t *c,
                             tsdb_node_id_t peer_id, const char *table_name,
                             int64_t bstart, int64_t bend)
{
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return -1;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(rmgr, peer_id);
    if (!conn) return -1;

    uint64_t *lset = NULL;
    size_t ln = 0;
    if (tsdb_ae_local_bucket_hashes(db, table_name, bstart, bend,
                                    &lset, &ln) != TSDB_OK)
        return -1;

    char qtl[320];
    snprintf(qtl, sizeof(qtl),
             "SELECT * FROM %s WHERE ts >= %lld AND ts <= %lld",
             table_name, (long long)bstart, (long long)bend);
    tsdb_result_t *res = NULL;
    if (fedrpc_query(conn, qtl, 60000, &res) != TSDB_OK || !res) {
        if (res) tsdb_result_free(res);
        free(lset);
        return -1;
    }

    int inserted = 0;
    int rc = tsdb_ae_merge_result_dedup(db, table_name, res, lset, ln, &inserted);
    tsdb_result_free(res);
    free(lset);
    return rc == TSDB_OK ? inserted : -1;
}

/* Bucket width for the row-range digest: the table's own partition span, so
 * buckets align 1:1 with partitions (the granularity ae_partition_backfill and
 * the rawblock idempotency comment already reason in).  0 = table unknown. */
static int64_t ae_table_span(tsdb_db_t *db, const char *table) {
    tsdb_table_internal_t *t = tsdb_db_scan_acquire(db, table);
    if (!t) return 0;
    tsdb_schema_t *s = tsdb_tbl_schema(t);
    int64_t span = 0;
    if (s) span = (s->partition_unit == TSDB_PARTITION_HOUR)
                      ? 3600000000000LL : 86400000000000LL;
    tsdb_db_scan_release(db, t);
    return span;
}

int tsdb_ae_node_in_set(uint64_t x, const uint64_t *set, int n) {
    for (int i = 0; i < n; i++) if (set[i] == x) return 1;
    return 0;
}

/* Anti-runaway pull budget for the row-digest: the most rows ONE sweep may merge
 * for a table.  A sweep pulls only rows a peer holds and this node lacks, so it
 * can never legitimately need to insert MORE than the fullest peer's entire
 * table — the union of two replicas is at most local + peer_hi.  The budget is
 * therefore peer_hi_count.  Anything beyond that is the merge duplicating rather
 * than converging (the 257M-row runaway), so the merge stops and says so.
 *
 * It deliberately is NOT (peer_hi - local).  That difference is 0 whenever the
 * counts are EQUAL — which is precisely the middle hole the digest exists to
 * heal (two replicas, equal counts, each missing an interior row the other has)
 * — and 0 whenever this node legitimately holds rows a peer lacks.  Using the
 * difference silently neutered the heal: the live cluster logged "hit the pull
 * budget (0 rows)" every sweep on divergent tables while merging nothing.  A
 * node holding MORE than a peer is not "ahead and done"; it can still be missing
 * that peer's interior rows. */
uint64_t tsdb_ae_pull_budget(uint64_t local_before, uint64_t peer_hi_count) {
    (void)local_before;   /* deliberately unused — see above */
    return peer_hi_count;
}

/* Row-range digest verification for the tables the (count,max_ts) probe cannot
 * distinguish.  CONFINED to co-replicas by the caller (eqpeers holds only
 * replica-set members) and hard-bounded by `budget` rows total this sweep, so a
 * merge can never runaway past one peer's worth even if the confinement is
 * wrong.  For each peer, exchange the per-bucket digest, and for every bucket
 * they disagree on, content-dedup merge the peer's copy (never truncate — see
 * pull_bucket_merge).  Returns total rows merged (>= 0).
 *
 * A converged table (digests match) costs one local digest + one digest RPC per
 * equal peer and merges nothing.  The local digest is recomputed only after an
 * actual merge, so an already-in-sync table never rescans its own rows twice. */
static int ae_digest_verify_and_merge(tsdb_db_t *db, tsdb_cluster_t *c,
                                      const char *table, int64_t span,
                                      const tsdb_node_id_t *eqpeers, int neq,
                                      uint64_t budget)
{
    tsdb_rowdigest_bucket_t *lv = NULL;
    size_t ln = 0;
    if (tsdb_cluster_local_table_digest(db, table, span, INT64_MIN, INT64_MAX,
                                        &lv, &ln) != TSDB_OK)
        return 0;                       /* over the bucket cap / error → degrade */

    uint8_t *rbuf = malloc(TSDB_ROWDIGEST_WIRE_MAX);
    if (!rbuf) { free(lv); return 0; }

    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    int total_merged = 0;

    for (int pi = 0; pi < neq; pi++) {
        tsdb_rpc_conn_t *conn = rmgr ? tsdb_replica_mgr_get_conn(rmgr, eqpeers[pi])
                                     : NULL;
        if (!conn) continue;

        uint32_t rlen = 0;
        /* Old peer (ERR_UNSUPPORTED), a vector that overflowed the buffer, or a
         * refusal all degrade to "skip this peer" — the count/max_ts decision
         * already ran and a mixed-version cluster must keep working. */
        if (tsdb_rpc_local_table_digest(conn, table, span, INT64_MIN, INT64_MAX,
                                        rbuf, TSDB_ROWDIGEST_WIRE_MAX,
                                        &rlen) != TSDB_OK)
            continue;

        tsdb_rowdigest_bucket_t *pv = NULL;
        size_t pn = 0;
        if (tsdb_rowdigest_deserialize(rbuf, rlen, &pv, &pn) != TSDB_OK) continue;

        int64_t divs[512];
        size_t ndiv = 0;
        tsdb_rowdigest_diff(lv, ln, pv, pn, divs,
                            sizeof(divs) / sizeof(divs[0]), &ndiv);
        free(pv);
        if (ndiv == 0) continue;                    /* converged with this peer */

        size_t nmerge = ndiv < (sizeof(divs) / sizeof(divs[0]))
                            ? ndiv : (sizeof(divs) / sizeof(divs[0]));
        int merged_here = 0;
        for (size_t di = 0; di < nmerge; di++) {
            /* Hard anti-runaway bound: never pull past one peer's worth in a
             * sweep.  A correct heal converges WELL within this; tripping it
             * means the merge is duplicating (the reverted runaway), so stop and
             * make it loud rather than pull 257M rows.  Count merged_here too —
             * it is not folded into total_merged until the peer finishes, so a
             * single peer with many divergent buckets must be bounded mid-loop. */
            if ((uint64_t)(total_merged + merged_here) >= budget) {
                tsdb_metric_inc("qengine_antientropy_runaway_averted_total");
                fprintf(stderr,
                        "[anti-entropy] %s: row-digest hit the pull budget "
                        "(%llu rows, fullest peer's worth) — stopping to avoid a "
                        "runaway merge; a legitimate heal never reaches this\n",
                        table, (unsigned long long)budget);
                goto done;
            }
            tsdb_metric_inc("qengine_antientropy_digest_mismatch_total");
            int m = pull_bucket_merge(db, c, eqpeers[pi], table,
                                      divs[di], divs[di] + span - 1);
            if (m > 0) merged_here += m;
        }
        total_merged += merged_here;

        /* Our rows changed — recompute the local digest so the NEXT peer diffs
         * against the merged state and we do not re-pull the same rows. */
        if (merged_here > 0) {
            free(lv);
            lv = NULL; ln = 0;
            if (tsdb_cluster_local_table_digest(db, table, span,
                                                INT64_MIN, INT64_MAX,
                                                &lv, &ln) != TSDB_OK)
                break;                              /* cannot re-measure → stop */
        }
    }

done:
    free(rbuf);
    free(lv);
    return total_merged;
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

        /* THE ONE WRITER THAT RENUMBERS THE PARTITION'S ORDINAL SPACE DOWNWARD.
         *
         * Phase 1 flushed into an EMPTY scratch dir, so part_ord_base started at
         * 0 and the rebuilt blocks are stamped 0..k-1 — numbers this partition
         * has already bound to other rows.  Everything else that allocates an
         * ordinal (the flush, compaction, the replication applier) goes through
         * tsdb_part_next_ordinal precisely so the space stays monotone for the
         * partition's LIFETIME; this path replaces the rows instead of extending
         * them, and only the .col/.idx pairs are swapped.
         *
         * The remote-ordinal map is not rebuilt by that swap and would keep
         * mapping every sender's group onto the OLD numbering, where local
         * ordinal N now names a different peer's rows.  Drop it: the next
         * delivery of each group then allocates a fresh ordinal and lands as a
         * new group, which is what this path did before ordinals existed. */
        tsdb_part_ord_reset(live_part);

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
        /* Make the directory entries the swap just rewrote — the renames AND the
         * .ordmap removal — durable together.  A crash that kept the renames but
         * lost the unlink would resurrect a map onto the pre-rebuild numbering. */
        {
            int dfd = open(live_part, O_RDONLY);
            if (dfd >= 0) { (void)fsync(dfd); close(dfd); }
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

/*
 * Count the rows THIS node actually holds for `table_name`, plus the newest
 * timestamp among them.
 *
 * Anti-entropy cannot use `SELECT count(*), max(ts) FROM <t>` for this: every
 * plain table is mirrored as a childless data-bearing super-table, so exec
 * routes that query through exec_stable_select, and on a node holding no local
 * rows the cluster-agg coordinator fires and returns the CLUSTER-WIDE total.
 * The node would compare the cluster's number against its peers', find nothing
 * higher, and never pull — staying empty forever while believing it is current.
 *
 * Disk rows come from each partition's ts.idx header (total_rows + file_ts_max,
 * the same fields the reader trusts).  Memtable rows are added from an atomic
 * snapshot, so an un-flushed tail counts too and max_ts is never understated —
 * understating it would make the tail pull re-fetch rows we already hold.
 *
 * An empty table yields (0, INT64_MIN), which tsdb_antientropy_decide maps to
 * TAIL_PULL/FULL_PULL against any populated peer.  Returns TSDB_ERR_* only when
 * the measurement itself could not be taken; callers must not guess on failure.
 */
int tsdb_cluster_local_table_stats(tsdb_db_t *db, const char *table_name,
                                   uint64_t *out_count, int64_t *out_max_ts)
{
    if (!db || !table_name) return TSDB_ERR_INVAL;

    uint64_t total = 0;
    int64_t  maxts = INT64_MIN;

    /* ---- durable rows: one ts.idx header per partition ---- */
    int ndirs = tsdb_db_data_dir_count(db);
    for (int i = 0; i < ndirs; i++) {
        const char *dd = tsdb_db_data_dir_at(db, i);
        if (!dd) continue;
        char tdir[4096];
        snprintf(tdir, sizeof(tdir), "%s/%s", dd, table_name);
        DIR *d = opendir(tdir);
        if (!d) continue;
        struct dirent *e;
        while ((e = readdir(d)) != NULL) {
            if (e->d_name[0] == '.') continue;      /* ., .., .trash */
            char idx_path[5200];
            snprintf(idx_path, sizeof(idx_path), "%s/%s/ts.idx", tdir, e->d_name);
            uint64_t rows = 0;
            int64_t  fmax = INT64_MIN;
            /* Returns the header size on success; 0 = absent/short, -1 =
             * corrupt.  Skip anything that is not a readable partition idx
             * (schema.bin and friends land here too). */
            if (tsdb_part_idx_probe(idx_path, NULL, NULL, NULL, &rows,
                                    NULL, &fmax, NULL) <= 0)
                continue;
            total += rows;
            if (rows > 0 && fmax > maxts) maxts = fmax;
        }
        closedir(d);
    }

    /* ---- un-flushed rows still in the memtable ---- */
    tsdb_table_internal_t *ti = tsdb_db_find_table(db, table_name);
    if (ti) {
        tsdb_memtable_t *mt = tsdb_tbl_memtable(ti);
        tsdb_schema_t   *ms = tsdb_tbl_schema(ti);
        if (mt && ms && ms->ncols > 0) {
            void **bufs = calloc((size_t)ms->ncols, sizeof(void *));
            if (!bufs) return TSDB_ERR_NOMEM;
            size_t nr = 0;
            if (tsdb_memtable_snapshot(mt, bufs, &nr) == TSDB_OK && nr > 0) {
                const int64_t *ts = (const int64_t *)bufs[0];   /* ts is col 0 */
                total += (uint64_t)nr;
                /* Scan rather than take ts[nr-1]: a memtable accepting
                 * out-of-order rows is not necessarily ts-sorted. */
                if (ts) {
                    for (size_t k = 0; k < nr; k++)
                        if (ts[k] > maxts) maxts = ts[k];
                }
            }
            for (int c = 0; c < ms->ncols; c++) free(bufs[c]);
            free(bufs);
        }
    }

    if (out_count)  *out_count  = total;
    if (out_max_ts) *out_max_ts = maxts;
    return TSDB_OK;
}

/* THIS node's row-range digest for `table_name` over [ts_lo, ts_hi], bucketed
 * by `span` ns.  See merkle.h.  Reads local storage only via a scatter-local
 * SELECT — the same guard tsdb_cluster_local_table_stats needs so a mirrored
 * plain table does not answer with the cluster-wide rows. */
int tsdb_cluster_local_table_digest(tsdb_db_t *db, const char *table_name,
                                    int64_t span, int64_t ts_lo, int64_t ts_hi,
                                    tsdb_rowdigest_bucket_t **out, size_t *out_n)
{
    if (!db || !table_name || span <= 0 || !out || !out_n) return TSDB_ERR_INVAL;
    *out = NULL;
    *out_n = 0;

    char qtl[320];
    if (ts_lo == INT64_MIN && ts_hi == INT64_MAX)
        snprintf(qtl, sizeof(qtl), "SELECT * FROM %s", table_name);
    else
        snprintf(qtl, sizeof(qtl),
                 "SELECT * FROM %s WHERE ts >= %lld AND ts <= %lld",
                 table_name, (long long)ts_lo, (long long)ts_hi);

    /* Physical-local read.  A mirrored plain table is a childless data-bearing
     * super-table, so a bare SELECT can scatter and gather the CLUSTER-WIDE
     * rows — the digest would then describe everyone's data, not this node's,
     * and two divergent replicas would look identical.  Scatter-local alone is
     * not enough either: it covers the self-child only when this node OWNS it by
     * hash, so a replica holding rows it does not own reads EMPTY and the digest
     * cannot fingerprint the divergence it exists to heal.  tsdb_g_ae_physical_local
     * lifts the self-child's ownership gate to physical presence; scatter-local
     * still suppresses the cluster re-scatter.  See exec.c for the rationale. */
    tsdb_result_t *res = NULL;
    int prev  = tsdb_g_scatter_local_mode;
    int pprev = tsdb_g_ae_physical_local;
    tsdb_g_scatter_local_mode = 1;
    tsdb_g_ae_physical_local  = 1;   /* self-child by physical presence, not ownership */
    int rc = tsdb_query(db, qtl, &res);
    tsdb_g_scatter_local_mode = prev;
    tsdb_g_ae_physical_local  = pprev;
    if (rc != TSDB_OK || !res) {
        if (res) tsdb_result_free(res);
        return rc == TSDB_OK ? TSDB_ERR_INTERNAL : rc;
    }

    rc = tsdb_rowdigest_from_result(res, span, out, out_n);
    tsdb_result_free(res);
    return rc;
}

tsdb_ae_action_t tsdb_antientropy_decide(uint64_t local_count,
                                         int64_t  local_max_ts,
                                         uint64_t best_count,
                                         int64_t  best_max_ts)
{
    /* A plausible row count can never reach timestamp scale (ns-epoch values
     * are ~1.7e18 today; even ms-epoch is ~1.7e12). */
    const uint64_t IMPLAUSIBLE_COUNT = 100000000000ULL; /* 1e11 */

    if (best_max_ts > local_max_ts) return TSDB_AE_TAIL_PULL;

    /* From here best_max_ts <= local_max_ts.  A peer BEHIND us in time can
     * reach this point — the caller admits any peer with a strictly higher
     * count, which is what the pre-candidate-list selection loop did too —
     * and it still cannot obtain anything destructive: the only truncating
     * branch below needs local_count == 0, and a table that measures 0 rows
     * measures local_max_ts == INT64_MIN (tsdb_cluster_local_table_stats),
     * which no peer's max_ts can be below.  Only a strictly higher peer count
     * is a candidate gap. */
    if (best_count <= local_count) return TSDB_AE_UP_TO_DATE;

    if (best_count >= IMPLAUSIBLE_COUNT) return TSDB_AE_SKIP_UNSAFE;
    if (local_count > 0)                 return TSDB_AE_SKIP_UNSAFE;
    return TSDB_AE_FULL_PULL;
}

/* ---- Pull-candidate ranking + bounded retry ------------------------------
 *
 * See src/storage/antientropy.h for why a single "best" peer is not enough. */

/* Strict "a ranks before b" (see the header for why count leads). */
static int ae_cand_better(const tsdb_ae_cand_t *a, const tsdb_ae_cand_t *b) {
    if (a->count  != b->count)  return a->count  > b->count;
    if (a->max_ts != b->max_ts) return a->max_ts > b->max_ts;
    return a->node_id < b->node_id;
}

void tsdb_antientropy_rank_candidates(tsdb_ae_cand_t *c, int n) {
    if (!c || n < 2) return;
    for (int i = 1; i < n; i++) {           /* insertion sort; n <= 64 peers */
        tsdb_ae_cand_t x = c[i];
        int j = i;
        while (j > 0 && ae_cand_better(&x, &c[j - 1])) { c[j] = c[j - 1]; j--; }
        c[j] = x;
    }
}

int tsdb_antientropy_run_candidates(const tsdb_ae_cand_t *c, int n,
                                    const tsdb_ae_driver_t *drv,
                                    tsdb_ae_run_t *out)
{
    if (!out) return TSDB_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!c || n <= 0 || !drv || !drv->local_stats || !drv->attempt)
        return TSDB_ERR_INVAL;

    long start_ms = drv->now_ms ? drv->now_ms(drv->ctx) : 0;

    for (int i = 0; i < n; i++) {
        /* Wall budget: without it the worst case is n (<= 64) x the 60 s
         * delta-pull timeout, serially, for every table in the catalog, on the
         * one startup pass.  Checked here so candidate 0 is always tried. */
        if (drv->budget_ms > 0 && drv->now_ms &&
            drv->now_ms(drv->ctx) - start_ms >= drv->budget_ms) {
            out->budget_hit = 1;
            break;
        }

        /* Re-measure before EVERY decision.  The loop mutates local storage,
         * so a stale measurement would let a later candidate be classified
         * against rows we no longer (or now do) hold.  It also means the
         * destructive gate below always sees the CURRENT state, which is a
         * fresher input than the pre-change code used — that took one
         * measurement before any peer round-trip and truncated on it. */
        uint64_t before = 0; int64_t before_ts = 0;
        if (drv->local_stats(drv->ctx, &before, &before_ts) != 0) {
            out->hard_err = 1;      /* cannot measure ourselves — never guess */
            break;
        }

        tsdb_ae_action_t act = tsdb_antientropy_decide(before, before_ts,
                                                       c[i].count, c[i].max_ts);
        if (act == TSDB_AE_UP_TO_DATE) continue;   /* not even contacted */

        int64_t since = (act == TSDB_AE_FULL_PULL) ? 0 : before_ts;
        int rc = drv->attempt(drv->ctx, &c[i], act, since);
        out->visited++;
        if (act == TSDB_AE_TAIL_PULL || act == TSDB_AE_FULL_PULL) out->pulls++;
        if (rc < 0) out->hard_err = 1;

        /* Progress is MEASURED, not reported.  The delta pull counts rows the
         * peer handed back and discards tsdb_batch_row_end's status, so "it
         * returned 10000" is not "10000 landed" — schema drift or a symbol it
         * could not intern drops every one of them.  Compare the local row
         * count instead; that is the only thing that means convergence. */
        uint64_t after = 0; int64_t after_ts = 0;
        if (drv->local_stats(drv->ctx, &after, &after_ts) != 0) {
            out->hard_err = 1;
            break;
        }
        if (after > before) {
            out->rows   = after - before;
            out->chosen = c[i].node_id;
            break;
        }
        /* Reported a gap, delivered nothing — advance to the next candidate. */
    }
    return TSDB_OK;
}

/* Wall budget for ONE table's candidate loop. */
#define AE_RESYNC_BUDGET_MS  30000L

/* Row-range digest verification is far heavier than the (count,max_ts) probe
 * (it reads and hashes the equal peer's rows), and a CONVERGED cluster has an
 * equal peer for most tables — so running it every sweep of every healthy table
 * would roughly double sweep CPU (the exact regression the design warns of).
 * It is therefore throttled to one sweep in AE_DIGEST_SWEEP_EVERY (~4 min at the
 * 30 s default); a middle hole is permanent until repaired, so detecting it in
 * minutes rather than seconds costs nothing.  `% == 1` fires on the FIRST sweep
 * so a fresh node verifies promptly.  A stale-cache alternative was rejected: a
 * digest cache that served a stale entry could MASK the very hole this detects. */
#define AE_DIGEST_SWEEP_EVERY 8L

/* Defined further down with the scatter helpers. */
static long mono_ms(void);

/* Production wiring for tsdb_ae_driver_t. */
typedef struct {
    tsdb_db_t      *db;
    tsdb_cluster_t *c;
    const char     *table;
    int64_t         delwm;
    int             bf_attempted;   /* partition backfill: once per resync */
} ae_resync_ctx_t;

static int ae_local_stats_cb(void *vctx, uint64_t *count, int64_t *max_ts) {
    ae_resync_ctx_t *x = (ae_resync_ctx_t *)vctx;
    return tsdb_cluster_local_table_stats(x->db, x->table, count, max_ts)
             == TSDB_OK ? 0 : -1;
}

static long ae_now_ms_cb(void *vctx) { (void)vctx; return mono_ms(); }

/* ---- Periodic-sweep hardening ------------------------------------------- *
 *
 * Reached only from the single anti-entropy sweep thread
 * (tsdb_resync_startup_thread -> resync_all_tables -> resync_table -> here),
 * so the per-table registries below need no locking.  Each is bounded and
 * fails OPEN (logs rather than suppresses) when full — a first occurrence is
 * never lost.
 */

/* Monotonic sweep counter, bumped once per full sweep in
 * tsdb_cluster_resync_all_tables.  Read by the middle-gap throttle. */
static long g_ae_sweep_no = 0;

/* Defect 1 — deterministic race-window injection seam (NULL in production). */
static void (*g_ae_prewrite_hook)(void *) = NULL;
static void  *g_ae_prewrite_arg = NULL;

void tsdb_ae_set_test_prewrite_hook(void (*fn)(void *), void *arg) {
    g_ae_prewrite_hook = fn;
    g_ae_prewrite_arg  = arg;
}

/* Defect 1 — the destructive half of FULL_PULL, isolated so the truncate-race
 * guard is testable without a live peer.  See antientropy.h. */
static int ae_full_pull_truncate_guarded(tsdb_db_t *db, const char *table)
{
    /* Test seam: fire a racing write EXACTLY where the production race exposes
     * one — after FULL_PULL was decided on an empty measurement, before the
     * truncate.  No-op in production. */
    if (g_ae_prewrite_hook) g_ae_prewrite_hook(g_ae_prewrite_arg);

    /* Truncate ONLY if the table is still empty when checked under the table
     * lock.  FULL_PULL was handed out for a table that measured EMPTY outside
     * any lock; if a WRITE_BATCH has committed since, truncating would destroy
     * rows no peer holds and the pull (sized to the peer) would not bring them
     * back.  tsdb_truncate_table_if_empty runs the emptiness check inside the
     * same batch_mu it truncates under, so a write cannot slip between the two
     * — closing the race an outside-the-lock re-measure could only narrow. */
    int was_empty = 0;
    if (tsdb_truncate_table_if_empty(db, table, &was_empty) != TSDB_OK)
        return -1;                      /* cannot verify emptiness — never guess */
    if (!was_empty) {
        tsdb_metric_inc("qengine_antientropy_truncate_averted_total");
        fprintf(stderr,
                "[anti-entropy] %s: row(s) committed since the empty "
                "measurement that gated this full pull — aborted destructive "
                "truncate under the table lock to preserve locally-committed "
                "rows; next sweep re-decides\n", table);
        return 0;
    }
    return 1;
}

int tsdb_ae_full_pull_truncate_guarded_for_test(struct tsdb_db *db,
                                                const char *table) {
    return ae_full_pull_truncate_guarded(db, table);
}

/* Defect 2 — middle-gap log throttle registry. */
#define AE_MIDGAP_TAB_CAP    512
#define AE_MIDGAP_LOG_EVERY  60      /* sweeps between repeats (60 x 30 s = 30 min) */

typedef struct {
    char name[64];
    long last_gap_sweep;   /* newest sweep this table was seen middle-gapped */
    long last_log_sweep;   /* newest sweep we emitted the human line         */
} ae_midgap_ent_t;

static ae_midgap_ent_t g_ae_midgap[AE_MIDGAP_TAB_CAP];
static int             g_ae_midgap_n = 0;

int tsdb_ae_midgap_should_log(const char *table, long sweep_no, long every)
{
    if (!table) return 1;                      /* fail open */
    ae_midgap_ent_t *e = NULL;
    for (int i = 0; i < g_ae_midgap_n; i++)
        if (strcmp(g_ae_midgap[i].name, table) == 0) { e = &g_ae_midgap[i]; break; }
    if (!e) {
        if (g_ae_midgap_n >= AE_MIDGAP_TAB_CAP) return 1;   /* full — never suppress */
        e = &g_ae_midgap[g_ae_midgap_n++];
        snprintf(e->name, sizeof(e->name), "%s", table);
        e->last_gap_sweep = -1;        /* never seen -> first sight is a transition */
        e->last_log_sweep = LONG_MIN;  /* and clears the throttle window            */
    }
    long prev_gap = e->last_gap_sweep;
    /* Transition = the gap was ABSENT in the immediately-preceding sweep (which
     * also makes a same-sweep repeat across several divergent peers a non-
     * transition, so one table logs at most once per sweep). */
    int transition = (prev_gap < sweep_no - 1);
    /* `last_log_sweep <= sweep_no - every` rather than `sweep_no - last >= every`
     * so the never-logged sentinel LONG_MIN does not underflow the subtraction
     * (UB that `make debug`'s -fsanitize=undefined would abort on); sweep_no and
     * every are small, so sweep_no - every cannot overflow. */
    int throttle   = (every > 0) && (e->last_log_sweep <= sweep_no - every);
    int should     = transition || throttle;
    e->last_gap_sweep = sweep_no;
    if (should) e->last_log_sweep = sweep_no;
    return should;
}

/* Defect 4 — over-count persistence registry. */
#define AE_OVERCOUNT_TAB_CAP   512
#define AE_OVERCOUNT_PERSIST_N 3       /* consecutive suspect sweeps before we shout */

typedef struct {
    char name[64];
    long streak;                       /* consecutive suspect sweeps */
} ae_overcount_ent_t;

static ae_overcount_ent_t g_ae_overcount[AE_OVERCOUNT_TAB_CAP];
static int                g_ae_overcount_n = 0;

long tsdb_ae_overcount_note(const char *table, int suspect)
{
    if (!table) return 0;
    ae_overcount_ent_t *e = NULL;
    for (int i = 0; i < g_ae_overcount_n; i++)
        if (strcmp(g_ae_overcount[i].name, table) == 0) { e = &g_ae_overcount[i]; break; }
    if (!e) {
        if (g_ae_overcount_n >= AE_OVERCOUNT_TAB_CAP) return suspect ? 1 : 0;
        e = &g_ae_overcount[g_ae_overcount_n++];
        snprintf(e->name, sizeof(e->name), "%s", table);
        e->streak = 0;
    }
    e->streak = suspect ? e->streak + 1 : 0;
    return e->streak;
}

static int ae_attempt_cb(void *vctx, const tsdb_ae_cand_t *cand,
                         tsdb_ae_action_t act, int64_t since_ts)
{
    ae_resync_ctx_t *x = (ae_resync_ctx_t *)vctx;
    tsdb_node_id_t peer = (tsdb_node_id_t)cand->node_id;

    switch (act) {
    case TSDB_AE_TAIL_PULL:
        return pull_table_delta(x->db, x->c, peer, x->table, since_ts);

    case TSDB_AE_FULL_PULL: {
        /* The ONLY destructive step in anti-entropy.  tsdb_antientropy_decide
         * hands it out only when the table measured EMPTY — but that
         * measurement was taken microseconds earlier, before this call, and a
         * WRITE_BATCH committing in the window would be destroyed by the
         * truncate and NOT restored by the pull (since_ts = 0 catches every
         * row the PEER holds, and the peer never had the raced write).  The
         * guard re-measures immediately before the truncate and aborts if a
         * row landed, so a locally-committed row no peer has is never wiped. */
        fprintf(stderr,
                "[anti-entropy] %s: empty local, full pull from peer %llu "
                "(peer count=%llu, max_ts %lld)\n",
                x->table, (unsigned long long)cand->node_id,
                (unsigned long long)cand->count, (long long)cand->max_ts);
        int g = ae_full_pull_truncate_guarded(x->db, x->table);
        if (g < 0)  return -1;
        if (g == 0) return 0;   /* raced write preserved; next sweep re-decides */
        return pull_table_delta(x->db, x->c, peer, x->table, since_ts);
    }

    case TSDB_AE_SKIP_UNSAFE: {
        /* Opt-in convergence: swap ONE cold divergent partition from the
         * fuller peer (temp build + atomic rename under compact_mtx with a
         * staleness guard).  Never reduces durable rows — the swap helper
         * refuses unless the pulled copy is strictly fuller.  Default OFF.
         *
         * Capped to ONE attempt per resync even though several candidates can
         * now classify SKIP_UNSAFE: each attempt is a full local partition
         * scan plus a SAMPLE BY against the peer plus a possible temp build,
         * and running that once per candidate would amplify the most expensive
         * path in this file N-fold.  The cap lands on the highest-count
         * candidate because the list is already ranked. */
        const char *bfe = getenv("TSDB_AE_PARTITION_BACKFILL");
        if (bfe && strcmp(bfe, "1") == 0 && !x->bf_attempted &&
            cand->count < 100000000000ULL) {
            x->bf_attempted = 1;
            int bf = ae_partition_backfill(x->db, x->c, peer, x->table, x->delwm);
            if (bf > 0) return bf;
        }
        /* Would shrink durable data on a peer-count comparison — refuse.
         *
         * Refusing is right, but the gap it refuses to close is PERMANENT and
         * until now the only trace of it was this line on stderr.  There is no
         * periodic row anti-entropy — tsdb_resync_startup_thread runs once, 5 s
         * after boot — so nothing revisits the decision, and the count(*) the
         * two nodes answer stays different with no error on either.  Measured
         * live: cnode-1 12000, cnode-3 9000, identical max(ts).  Count it so an
         * operator can see divergence exists without reading container logs. */
        /* The counter is the scrapeable signal — bump it EVERY occurrence.  The
         * human line is throttled: once on the TRANSITION into middle-gap, then
         * at most once per AE_MIDGAP_LOG_EVERY sweeps while it persists.  Before
         * this, a table middle-gapped against three peers logged 3 lines every
         * 30 s forever (~8,600/day/table, no rate limit). */
        tsdb_metric_inc("qengine_antientropy_middle_gap_total");
        if (tsdb_ae_midgap_should_log(x->table, g_ae_sweep_no, AE_MIDGAP_LOG_EVERY))
            fprintf(stderr,
                    "[anti-entropy] %s: middle-gap vs peer %llu (peer=%llu rows, "
                    "max_ts %lld) — peer not provably a superset%s; "
                    "skipping destructive truncate to preserve local rows\n",
                    x->table, (unsigned long long)cand->node_id,
                    (unsigned long long)cand->count, (long long)cand->max_ts,
                    cand->count >= 100000000000ULL ? " (timestamp-scale count)" : "");
        return 0;
    }

    case TSDB_AE_UP_TO_DATE:
    default:
        return 0;
    }
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

    /* Measure OUR OWN rows, not the cluster's.  `SELECT count(*) FROM <t>` is
     * not a local measurement here: hierarchy mirroring registers every plain
     * table as a childless data-bearing super-table, so exec routes it through
     * exec_stable_select, and for a node with no local rows the cluster-agg
     * coordinator fires and answers with the CLUSTER-WIDE total.  The node then
     * compares that against its peers, concludes it is already up to date, and
     * never pulls — the table stays empty here forever.  Anti-entropy has to
     * count what is actually on THIS node. */
    uint64_t local_count = 0;
    int64_t  local_max_ts = 0;
    if (tsdb_cluster_local_table_stats(db, table_name,
                                       &local_count, &local_max_ts) != TSDB_OK)
        return TSDB_OK;   /* cannot measure ourselves — never guess, never pull */

    /* Ask EVERY alive peer for its (count, max_ts) and KEEP EVERY ANSWER.
     *
     * Collapsing them to one "best" is what stalls convergence.  Peers can be
     * indistinguishable at the sort key and yet only one of them can hand the
     * rows over — most importantly a peer on an OLDER binary, which does not
     * know TSDB_RPC_LOCAL_TABLE_STATS, so the probe falls back to
     * `SELECT count(*), max(ts) FROM <t>`; on a peer holding nothing that
     * resolves through the childless data-bearing mirror and exec's cluster-agg
     * coordinator answers with the CLUSTER-WIDE aggregate — the holder's exact
     * numbers.  The old first-wins tie-break then picked whichever of the two
     * gossip happened to list first, and when that was the empty peer,
     * `SELECT * FROM <t> WHERE ts > N` handed back nothing and the resync
     * reported success having converged zero rows — permanently, because the
     * startup resync runs exactly once. */
    tsdb_node_id_t peers[TSDB_CLUSTER_MAX_NODES];
    int npeers = collect_alive_peers(c, peers, TSDB_CLUSTER_MAX_NODES);

    /* Shard-aware confinement for the row-digest (the 2026-08-02 runaway fix).
     * A childless-data-bearing table is owned by a FIXED replica set
     * (tsdb_cluster_route); every one of its rows belongs to that set.  A node
     * NOT in the set only ever holds TRANSIENT non-owner write copies pending
     * SKIP_LOCAL — its physical storage is not authoritative.  The reverted
     * digest let such a node fingerprint its full physical storage, compare it
     * to a co-replica's, and merge toward the cross-node UNION; with
     * SHARD_REPLICA_N < node-count that cross-contaminates shards and
     * re-replicates without bound (a 3000-row table reached 15000 and 257M rows
     * were pulled before it was reverted).  The digest is therefore confined to
     * run ONLY between CO-REPLICAS: this node must be in the set, and only peers
     * in the set are digest-verified.  route-fail / standalone (rsn==0) keeps the
     * old whole-cluster behaviour, which is correct when nothing is sharded. */
    tsdb_node_id_t rset[TSDB_CLUSTER_MAX_NODES];
    int rsn = 0;
    int self_is_replica = 1;
    {
        int rn = shard_replica_n_cached();
        if (rn > 0) {
            rsn = tsdb_cluster_route(c, table_name, "", rn, rset);
            if (rsn > 0) {
                tsdb_node_id_t self = tsdb_cluster_local_id(c);
                self_is_replica = 0;
                for (int i = 0; i < rsn; i++)
                    if (rset[i] == self) { self_is_replica = 1; break; }
            }
        }
    }

    tsdb_ae_cand_t cand[TSDB_CLUSTER_MAX_NODES];
    int ncand = 0;
    /* Peers that answered EQUAL count AND EQUAL max_ts — the (count,max_ts)
     * probe's blind spot.  They are NOT pull candidates (nothing is provably
     * behind), but they are exactly the peers a row-range digest must verify:
     * two replicas each missing a different interior batch tie here. */
    tsdb_node_id_t eqpeers[TSDB_CLUSTER_MAX_NODES];
    int neq = 0;
    uint64_t peer_hi_count = 0;         /* fullest peer that answered           */
    int64_t  peer_hi_ts    = INT64_MIN; /* newest ts any peer that answered has */
    int      peers_answered = 0;
    for (int i = 0; i < npeers; i++) {
        uint64_t pc = 0; int64_t pmax = 0;
        /* A peer that cannot answer the probe never becomes a candidate, so no
         * delta pull is ever aimed at a peer we already know is unreachable. */
        if (peer_table_stats(c, peers[i], table_name, &pc, &pmax) != 0) continue;

        /* Over-count watch (defect 4): remember the fullest peer and the newest
         * peer ts across EVERY answer — admitted as a pull candidate or not. */
        peers_answered++;
        if (pc   > peer_hi_count) peer_hi_count = pc;
        if (pmax > peer_hi_ts)    peer_hi_ts    = pmax;

        /* Row-range digest candidate.  The original case is an EXACT
         * (count,max_ts) tie — the middle-hole blind spot: two replicas each
         * missing a different interior batch tie here and the count/max_ts probe
         * cannot see it.
         *
         * WIDENED to also verify a peer whose count DIFFERS at an EQUAL max_ts.
         * That is the shape a replication drop leaves — a peer holds rows we
         * lack (or vice versa) with the same newest timestamp — and the old
         * path handles it badly: if the peer has MORE it becomes a `cand` and a
         * `SELECT ts > local_max_ts` pull brings back nothing (max_ts already
         * matches, the gap is interior); if the peer has FEWER, nothing looks at
         * it at all.  Both leave a permanent divergence (observed live on the
         * cluster: stress_t/phole stuck at unequal counts, never healing).  The
         * digest diff is per-bucket and count-agnostic, and the merge only ever
         * inserts the rows a bucket lacks (content-dedup, never truncates), so
         * running it here pulls-and-merges toward the union in BOTH directions
         * and is safe for a legitimately-ahead replica.
         *
         * Still gated on EQUAL max_ts: a peer with a strictly newer max_ts is
         * genuinely ahead in time and the cheap TAIL_PULL already covers it —
         * no need to digest the whole table for a tail.  A peer strictly behind
         * in time is behind, not diverged, and its own sweep pulls from us. */
        /* CO-REPLICA confinement: only a peer in this table's replica set is
         * digest-verified.  A peer outside the set holds at most transient
         * non-owner copies; merging against it crosses shards and runs away. */
        if (pmax == local_max_ts && neq < TSDB_CLUSTER_MAX_NODES &&
            (rsn == 0 || tsdb_ae_node_in_set((uint64_t)peers[i], rset, rsn)))
            eqpeers[neq++] = peers[i];

        /* Admission is EXACTLY the predicate the single-`best` loop used: that
         * loop seeded `best` with the LOCAL stats and replaced it only on a
         * strictly greater (count, max_ts), so a peer could be selected iff it
         * was strictly greater than local in that order.  Keeping the same
         * predicate means NO candidate reaches tsdb_antientropy_decide that
         * could not reach it before — the retry widens how many peers we try,
         * never what any one of them is allowed to do. */
        if (!(pc > local_count || (pc == local_count && pmax > local_max_ts)))
            continue;

        cand[ncand].node_id = (uint64_t)peers[i];
        cand[ncand].count   = pc;
        cand[ncand].max_ts  = pmax;
        ncand++;
    }

    /* Over-count watch (defect 4).  We hold MORE rows than the fullest peer at
     * an EQUAL newest-ts horizon.  A replica that is merely async-AHEAD took the
     * write first and so has a strictly NEWER max_ts; equal max_ts + higher
     * local count is instead the signature of a duplicated WRITE_BATCH counted
     * twice.  Count alone cannot PROVE a duplicate and a transient ahead-state
     * at equal ts resolves within a sweep or two, so we NEVER truncate here —
     * we track persistence and make a real (permanent) over-count visible.
     * A legitimately-ahead replica clears (streak resets) before N sweeps. */
    if (peers_answered > 0) {
        int suspect = (local_count > peer_hi_count) &&
                      (peer_hi_ts == local_max_ts);
        long streak = tsdb_ae_overcount_note(table_name, suspect);
        if (suspect && streak >= AE_OVERCOUNT_PERSIST_N) {
            tsdb_metric_inc("qengine_antientropy_overcount_persistent_total");
            if (streak == AE_OVERCOUNT_PERSIST_N)
                fprintf(stderr,
                        "[anti-entropy] %s: local holds %llu rows vs peer max %llu "
                        "at equal max_ts for %ld sweeps — possible duplicate "
                        "over-count (NOT auto-repaired; a legitimately-ahead "
                        "replica would have cleared)\n",
                        table_name, (unsigned long long)local_count,
                        (unsigned long long)peer_hi_count, streak);
        }
    }

    /* Candidates present (a peer is strictly ahead): the unchanged count/max_ts
     * pull path.  ncand == 0 (every peer ties or is behind) falls straight
     * through to the row-range digest verify below. */
    if (ncand > 0) {
        tsdb_antientropy_rank_candidates(cand, ncand);

        /* Try candidates best-first until one actually DELIVERS rows.  The gap
         * classification and its safety guard are unchanged (see
         * tsdb_antientropy_decide): a tail gap pulls everything past our max_ts;
         * a middle gap (equal max_ts, higher peer count) is REFUSED rather than
         * healed with the truncate + re-pull that once destroyed durable rows.
         * The driver only sequences — it re-measures this node before every
         * decision and after every attempt, so the destructive branch sees
         * fresher state than the pre-change code did and "delivered rows" means
         * the local row count actually went up. */
        ae_resync_ctx_t ctx = { db, c, table_name, wm, 0 };
        tsdb_ae_driver_t drv = {
            .ctx         = &ctx,
            .local_stats = ae_local_stats_cb,
            .attempt     = ae_attempt_cb,
            .now_ms      = ae_now_ms_cb,
            .budget_ms   = AE_RESYNC_BUDGET_MS,
        };
        tsdb_ae_run_t run;
        if (tsdb_antientropy_run_candidates(cand, ncand, &drv, &run) != TSDB_OK)
            return TSDB_ERR_INVAL;

        if (run.rows > 0) {
            int pulled = run.rows > (uint64_t)INT_MAX ? INT_MAX : (int)run.rows;
            if (out_rows_pulled) *out_rows_pulled = pulled;
            tsdb_metric_add("qengine_antientropy_rows_pulled_total", run.rows);
            /* A pull moved us: (count,max_ts) is now different, so re-decide on
             * the next sweep rather than also digesting a stale equal-peer set. */
            return TSDB_OK;
        }
        if (run.hard_err) return TSDB_ERR_IO;   /* preserve the TSDB_ERR_IO contract */
        if (run.pulls > 0) {
            /* Every peer that claimed to be ahead delivered nothing.  Do not
             * truncate, do not spin, do not record the table as converged: leave
             * it byte-for-byte as it is, make the condition loud and scrapeable,
             * and let the next resync retry against a fresh membership snapshot. */
            fprintf(stderr,
                    "[anti-entropy] %s: %d peer(s) reported rows but delivered "
                    "none%s — local still %llu rows; no source found\n",
                    table_name, run.pulls,
                    run.budget_hit ? " (wall budget expired)" : "",
                    (unsigned long long)local_count);
            tsdb_metric_inc("qengine_antientropy_no_source_total");
        }
    }

    /* Row-range digest verify — the divergence safety net for CO-REPLICAS.
     * Checks every co-replica peer at an EQUAL max_ts (whether or not its count
     * matches ours): an exact tie is a middle hole, an unequal count at equal
     * max_ts is a replication drop the count/max_ts probe heals badly or not at
     * all.  Gated on: self is in the table's replica set (self_is_replica — a
     * non-replica holds only transient copies and must never merge), a non-empty
     * table, one sweep in AE_DIGEST_SWEEP_EVERY, and a hard per-sweep pull budget
     * of one peer's worth.  OFF by default after the 2026-08-02 runaway — set
     * TSDB_AE_ROW_DIGEST=1 to opt in.  A mismatch names the divergent ts
     * bucket(s) and pull-merges the peer's copy (content-dedup; never a
     * truncate), so co-replicas converge to the union — see pull_bucket_merge. */
    if (self_is_replica && neq > 0 && local_count > 0) {
        const char *de = getenv("TSDB_AE_ROW_DIGEST");
        int enabled = de && strcmp(de, "1") == 0;   /* default OFF; opt-in only */
        int due     = (g_ae_sweep_no % AE_DIGEST_SWEEP_EVERY) == 1;
        if (enabled && due) {
            int64_t span = ae_table_span(db, table_name);
            if (span > 0) {
                uint64_t budget = tsdb_ae_pull_budget(local_count, peer_hi_count);
                int merged = ae_digest_verify_and_merge(db, c, table_name, span,
                                                        eqpeers, neq, budget);
                if (merged > 0) {
                    if (out_rows_pulled) *out_rows_pulled += merged;
                    tsdb_metric_add("qengine_antientropy_rows_pulled_total",
                                    (uint64_t)merged);
                    fprintf(stderr,
                            "[anti-entropy] %s: row-range digest merged %d row(s) "
                            "from a peer at equal max_ts — a divergence the "
                            "count/max_ts probe heals badly or not at all\n",
                            table_name, merged);
                }
            }
        }
    }
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
        strcmp(name, "raft")    == 0 ||
        /* Core dumps live in $TSDB_DATA_DIR/cores (deployment/entrypoint.sh
         * puts them there so a crash cannot fill the data dir itself).  It is
         * plumbing, not a table: the sweep was opening it as one every tick,
         * and the orphan report named it as a table the catalog forgot. */
        strcmp(name, "cores")   == 0) return 0;
    return 1;
}

/* Count table directories on disk that the catalog knows NOTHING about, and
 * make them visible.  REPORT ONLY — nothing is deleted or moved here.
 *
 * A DROP removes the catalog row cluster-wide and the storage on every node that
 * RUNS it (tsdb_drop_table has no not-found path; it always reaps the WAL and
 * trash_or_rm's the dir).  A node that was DOWN for the DROP never runs it: the
 * catalog tombstone correctly keeps the table out of its catalog when it
 * rejoins, but its local directory stays forever, and DROP cannot reach it
 * either — exec.c refuses to propose a DROP for a name the LEADER does not know.
 * Measured live 2026-08-04: one node alone held five such directories while the
 * catalog listed only the real tables.
 *
 * That matters beyond disk: hierarchy mirroring registers every plain table as a
 * childless data-bearing super-table and the sweep calls
 * materialize_missing_stables, so an orphan directory is a resurrection
 * candidate.  Reaping it automatically would be a delete decided by this code —
 * refused deliberately (see the design options on task #8); surfacing it is not.
 *
 * Cheap: one readdir per data dir per sweep plus two catalog lookups per entry.
 * The name list is logged only when the COUNT CHANGES, so a steady state costs
 * one gauge update and no log lines. */
static void ae_report_orphan_storage(tsdb_db_t *db) {
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    if (!cat) return;

    long  n_orphan = 0;
    char  names[8][128];
    int   nnames = 0;
    int   ndirs = tsdb_db_data_dir_count(db);

    for (int di = 0; di < ndirs; di++) {
        const char *dd = tsdb_db_data_dir_at(db, di);
        if (!dd) continue;
        DIR *d = opendir(dd);
        if (!d) continue;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (!resync_is_table_dir(de->d_name)) continue;
            char path[4600];
            snprintf(path, sizeof(path), "%s/%s", dd, de->d_name);
            struct stat st;
            if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;

            /* Known to the catalog in EITHER role → not an orphan.  A plain
             * table is mirrored as a childless super-table, so the stable
             * lookup covers the ordinary case. */
            tsdb_stable_t       stmp;
            tsdb_child_table_t  ctmp;
            if (tsdb_stable_get(cat, de->d_name, &stmp) == TSDB_OK)      continue;
            if (tsdb_child_table_get(cat, de->d_name, &ctmp) == TSDB_OK) continue;

            n_orphan++;
            if (nnames < 8)
                snprintf(names[nnames++], sizeof(names[0]), "%s", de->d_name);
        }
        closedir(d);
    }

    tsdb_metric_gauge_set("qengine_orphan_table_dirs", (double)n_orphan);

    static long last_reported = -1;
    if (n_orphan == last_reported) return;
    last_reported = n_orphan;
    if (n_orphan <= 0) return;

    char list[1100];
    size_t off = 0;
    for (int i = 0; i < nnames && off < sizeof(list) - 1; i++)
        off += (size_t)snprintf(list + off, sizeof(list) - off,
                                "%s%s", i ? ", " : "", names[i]);
    fprintf(stderr,
            "[storage] %ld table director(ies) on disk are unknown to the "
            "catalog (%s%s) — most likely dropped while this node was down, so "
            "the tombstone kept them out of the catalog but nothing reaped the "
            "files; DROP cannot reach them either.  Nothing was deleted.\n",
            n_orphan, list, n_orphan > nnames ? ", ..." : "");
}

/* The wall budget one table's candidate loop is allowed, exported so a test can
 * pin the number the production driver actually passes (its enforcement is
 * pinned by tests/test_ae_candidates.c).  This, times the table count, is the
 * worst case of ONE sweep — and only for tables that have a candidate peer at
 * all: a converged table costs one LOCAL_TABLE_STATS round-trip per alive peer
 * and stops at `ncand == 0`. */
long tsdb_antientropy_budget_ms(void) { return AE_RESYNC_BUDGET_MS; }

/* Milliseconds between anti-entropy sweeps.  0 = one shot at startup and never
 * again, which is what this node did unconditionally until 2026-07-31.
 *
 * 30 s by default: the same tick tsdb_delwm_reassert_thread already runs on, so
 * the cadence an operator has to reason about does not multiply.  Deliberately
 * NOT cached in a static — the sweep loop reads it once per iteration, which
 * costs one getenv per 30 s and lets a test drive two periods in one process. */
long tsdb_antientropy_period_ms(void) {
    const char *e = getenv("TSDB_ANTIENTROPY_PERIOD_MS");
    long ms = (e && *e) ? atol(e) : 30000;
    return ms > 0 ? ms : 0;
}

/* ONE anti-entropy sweep: learn what tables exist, then close each one's row
 * gap against the alive peers.  Extracted verbatim from the body of the
 * startup thread so the periodic tick runs the code that was already there and
 * already tested — the only new thing is that it runs more than once. */
void tsdb_cluster_resync_all_tables(tsdb_db_t *db) {
    if (!db) return;

    /* One monotonic tick per sweep — drives the middle-gap log throttle so a
     * persistent gap logs once, not once per divergent peer per 30 s. */
    g_ae_sweep_no++;

    /* Report-only: surface table dirs the catalog knows nothing about.  Runs
     * BEFORE the pre-open below, which would otherwise register them in
     * db->tables[] and hide the discrepancy. */
    ae_report_orphan_storage(db);

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
    tsdb_metric_inc("qengine_antientropy_sweeps_total");
    if (total > 0)
        fprintf(stderr, "[anti-entropy] sweep: %d rows across %d tables\n",
                total, n);
}

/* Detached worker: sleep long enough for gossip + replication to settle, then
 * sweep — and keep sweeping.
 *
 * This thread used to run ONE sweep and return, and that single pass was the
 * whole of row-level anti-entropy.  Every "anti-entropy will heal it" in the
 * replication path is a promise made to this thread, and for a peer that stays
 * up the thread had already finished before any of them could be called in:
 *
 *   - TSDB_REPLICATION_QUORUM=0 acks a write no peer has yet (cluster.c);
 *   - a gossip-DEAD peer is skipped outright — "it resyncs via anti-entropy
 *     when it rejoins" (replica.c replica_peer_dialable);
 *   - a fanout worker that burns MAX_ATTEMPTS gives up — "anti-entropy heals
 *     the gap" (replica.c fanout_worker);
 *   - a SCHEMA_SYNC the peer refused (rpc.c, above) leaves it unable to accept
 *     writes for that table at all;
 *   - a fanout send dropped under the in-flight byte cap (replica.c).
 *
 * The loop is the same body as before, run again every
 * TSDB_ANTIENTROPY_PERIOD_MS.  Cost per sweep on a converged cluster is one
 * LOCAL_TABLE_STATS round-trip per table per alive peer — the candidate list
 * comes back empty and tsdb_cluster_resync_table returns before contacting
 * anyone.  A sweep that does find work is bounded by AE_RESYNC_BUDGET_MS
 * (30 s) per table, and sweeps cannot overlap or pile up: the period is a gap
 * BETWEEN sweeps on one thread, not a rate. */
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
     * per-table row pull below fills them in.
     *
     * Startup only.  tsdb_delwm_reassert_thread already runs exactly this
     * reconcile every 30 s (see its comment), so repeating it inside the sweep
     * loop below would double the CATALOG_DUMP traffic to buy nothing —
     * materialize_missing_stables inside the sweep still picks up whatever
     * that thread learned. */
    (void)tsdb_catalog_reconcile_from_peers(db);

    long period_ms = tsdb_antientropy_period_ms();
    fprintf(stderr, "[anti-entropy] first sweep after %d ms, then every %ld ms\n",
            delay_ms, period_ms);

    for (;;) {
        tsdb_cluster_resync_all_tables(db);

        /* Re-read each iteration so the knob is not frozen at process start
         * (and so a test can drive two periods in one process).  0 means the
         * pre-2026-07-31 behaviour exactly: one sweep, then this thread ends. */
        period_ms = tsdb_antientropy_period_ms();
        if (period_ms <= 0) break;
        struct timespec p = { .tv_sec = period_ms / 1000,
                              .tv_nsec = (period_ms % 1000) * 1000000L };
        nanosleep(&p, NULL);
    }
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
    /* A pooled conn can be stale: the owner restarted since it was dialed
     * (e.g. a peer restarts under this node during a rolling upgrade),
     * leaving a half-open socket, so the first fedrpc fails fast.  Without
     * eviction the SAME poisoned conn is handed back on every subsequent
     * query and this node answers "I/O error" PERMANENTLY until it is
     * restarted.  Evict so the pool redials a fresh conn, then retry the
     * forward once — mirrors the stable-scatter coordinator's recovery. */
    if (rc != TSDB_OK) {
        if (*out) { tsdb_result_free(*out); *out = NULL; }
        tsdb_replica_mgr_evict_conn(rmgr, owners[0], conn);
        conn = tsdb_replica_mgr_get_conn(rmgr, owners[0]);
        if (conn) rc = fedrpc_query(conn, qtl, 5000, out);
    }
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

/* Monotonic milliseconds for wall-clock budgets. */
static long mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Membership census for the catalog-introspection path (SHOW ...).
 *
 * The scatter below only dials ALIVE peers, so "every alive peer answered"
 * is NOT the same claim as "every node in this cluster answered": a cluster
 * with a dead node would report a clean fan-out over a smaller set.  SHOW
 * has to tell those apart before it calls a listing cluster-complete, so it
 * needs both numbers.
 *
 *   *out_known — peers in this node's membership view, any state, self excluded
 *   *out_alive — the TSDB_NODE_ALIVE subset (exactly who the scatter dials)
 *   *out_missing_id — id of the first non-ALIVE peer, 0 when there is none
 *
 * Returns 1 when this node is clustered, 0 when standalone (counters 0). */
int tsdb_cluster_peer_census(tsdb_db_t *db, int *out_known, int *out_alive,
                              uint64_t *out_missing_id)
{
    if (out_known) *out_known = 0;
    if (out_alive) *out_alive = 0;
    if (out_missing_id) *out_missing_id = 0;
    if (!db) return 0;

    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return 0;

    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr(c);
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(mgr, snap, TSDB_CLUSTER_MAX_NODES);
    tsdb_node_id_t self = tsdb_cluster_local_id(c);

    int known = 0, alive = 0;
    for (int i = 0; i < n; i++) {
        if (snap[i].id == self) continue;
        known++;
        if (snap[i].state == TSDB_NODE_ALIVE) {
            alive++;
        } else if (out_missing_id && *out_missing_id == 0) {
            *out_missing_id = (uint64_t)snap[i].id;
        }
    }
    if (out_known) *out_known = known;
    if (out_alive) *out_alive = alive;
    return 1;
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
    long start_ms = mono_ms();
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

    /* Whole-scatter wall budget: the sequential peer loop plus the
     * evict/redial retries below must never sum to a long client-visible
     * stall — each leg is bounded, but N legs x retry x timeout adds up. */
    long total_budget_ms = (timeout_ms > 0) ? 3L * (long)timeout_ms : 0;

    int n = 0;
    for (int i = 0; i < npeers; i++) {
        if (total_budget_ms > 0 && mono_ms() - start_ms >= total_budget_ms) {
            for (int k = 0; k < n; k++) tsdb_result_free(results[k]);
            *out_n = 0;
            if (out_failed_peer) *out_failed_peer = (uint64_t)peers[i];
            return TSDB_ERR_IO;
        }
        tsdb_rpc_conn_t *conn =
            rmgr ? tsdb_replica_mgr_get_conn(rmgr, peers[i]) : NULL;
        tsdb_result_t *res = NULL;
        int rc = conn ? fedrpc_query_local(conn, qtl, timeout_ms, &res)
                      : TSDB_ERR_IO;
        if ((rc != TSDB_OK || !res) && rmgr &&
            !(total_budget_ms > 0 && mono_ms() - start_ms >= total_budget_ms)) {
            /* The pooled conn may be stale (peer restarted since it was
             * dialed; half-open socket).  Without eviction every future
             * scatter gets the same poisoned conn back and the failure is
             * PERMANENT (observed live as a node answering "I/O error"
             * forever while its peers were healthy).  Evict so the pool
             * redials, and retry this leg once — also when the first
             * get_conn returned NULL (a transient dial/membership blip),
             * which the fresh attempt may succeed on. */
            if (res) { tsdb_result_free(res); res = NULL; }
            if (conn) tsdb_replica_mgr_evict_conn(rmgr, peers[i], conn);
            conn = tsdb_replica_mgr_get_conn(rmgr, peers[i]);
            rc = conn ? fedrpc_query_local(conn, qtl, timeout_ms, &res)
                      : TSDB_ERR_IO;
        }
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
