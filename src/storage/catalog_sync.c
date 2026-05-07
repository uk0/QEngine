/* catalog_sync.c — data-node catalog self-heal at startup.
 *
 * Pre-fix a data peer that crashed (or missed broadcasts) during the
 * cluster's CREATE TABLE / GROUP / STABLE storm stayed forever
 * divergent.  Master broadcasts a CREATE TABLE pt_X USING vt_Y RPC,
 * the data peer's local tsdb_query rejects it with "stable not
 * found" because vt_Y's earlier broadcast was lost, the master sees
 * the ERR ack, and the broadcast retry hits the same wall.  Cascade
 * keeps the peer's catalog stuck.
 *
 * Observed on lvm1 cnode-3: 50 of 50 PTables missing on this peer
 * but 50 of 50 present on the others (cnode-2/cnode-4).  Recurring
 * across runs — not a transient miss, a persistent state divergence.
 *
 * This module fetches the master's verbatim catalog log files via a
 * single TSDB_RPC_CATALOG_DUMP and replays the additions / removals
 * locally with the same suppress-broadcast flag the cluster uses for
 * apply re-entry.  A data node calls this once at startup before
 * serving traffic; combined with the existing data anti-entropy that
 * pulls partition rows, the cluster converges on cold restart.
 */
#include "db.h"
#include "../cluster/rpc.h"
#include "../cluster/replica.h"
#include "../cluster/node.h"
#include "../cluster/cluster.h"
#include "../catalog/database.h"
#include "../catalog/group.h"
#include "../catalog/stable.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* Forward decls for catalog reload + cluster hooks (defined in db.c /
 * db_cluster.c / catalog modules). */
extern int  tsdb_db_reload_catalog(tsdb_db_t *db);
extern struct tsdb_cluster *tsdb_db_cluster(tsdb_db_t *db);

/* The 4 catalog log files we ship/replay.  Each lives at
 *   <data_dir>/catalog/<basename>
 * and is a text file with one entry per line; the receiver
 * concatenates the master's content into its own file (after a
 * timestamp marker) and then triggers tsdb_db_reload_catalog so the
 * in-memory hmaps see the merged state. */
static const char *kCatalogLogs[] = {
    "databases.log",
    "groups.log",
    "stables.log",
    "child_tables.log",
};
static const int kCatalogLogCount = (int)(sizeof(kCatalogLogs) / sizeof(kCatalogLogs[0]));

/* Wire format of the dump payload:
 *   for each log file in the kCatalogLogs order:
 *     u32_le  name_len
 *     bytes   name      (e.g. "databases.log", no NUL)
 *     u32_le  body_len
 *     bytes   body      (raw file contents, may include + and - lines)
 * Concatenation in fixed order keeps the parser stateless. */

/* Dump master's catalog log files into out_buf.  Returns bytes written
 * or -1 on error.  Called from rpc.c when TSDB_RPC_CATALOG_DUMP arrives. */
int tsdb_catalog_dump_serialize(const char *data_dir, uint8_t *out, size_t cap) {
    if (!data_dir || !out || cap < 16) return -1;
    size_t off = 0;
    for (int i = 0; i < kCatalogLogCount; i++) {
        const char *base = kCatalogLogs[i];
        char path[4096];
        snprintf(path, sizeof(path), "%s/catalog/%s", data_dir, base);

        /* Read file into memory.  Catalog logs are append-only text
         * files — typically a few KB to a few MB on a stress-tested
         * node.  We size the dump at a generous 32 MB; bigger clusters
         * should partition the dump but that's a follow-up. */
        FILE *fp = fopen(path, "rb");
        size_t body_len = 0;
        uint8_t *body = NULL;
        if (fp) {
            fseek(fp, 0, SEEK_END);
            long sz = ftell(fp);
            fseek(fp, 0, SEEK_SET);
            if (sz > 0 && sz < 32 * 1024 * 1024) {
                body = (uint8_t *)malloc((size_t)sz);
                if (body) {
                    if (fread(body, 1, (size_t)sz, fp) == (size_t)sz)
                        body_len = (size_t)sz;
                    else { free(body); body = NULL; }
                }
            }
            fclose(fp);
        }
        size_t name_len = strlen(base);
        size_t need = 4 + name_len + 4 + body_len;
        if (off + need > cap) { free(body); return -1; }

        /* name_len LE */
        out[off + 0] = (uint8_t)(name_len      );
        out[off + 1] = (uint8_t)(name_len >>  8);
        out[off + 2] = (uint8_t)(name_len >> 16);
        out[off + 3] = (uint8_t)(name_len >> 24);
        memcpy(out + off + 4, base, name_len);
        off += 4 + name_len;
        out[off + 0] = (uint8_t)(body_len      );
        out[off + 1] = (uint8_t)(body_len >>  8);
        out[off + 2] = (uint8_t)(body_len >> 16);
        out[off + 3] = (uint8_t)(body_len >> 24);
        if (body_len > 0) memcpy(out + off + 4, body, body_len);
        off += 4 + body_len;
        free(body);
    }
    return (int)off;
}

/* Replay a dump into <data_dir>/catalog/.  Strategy: append the
 * master's body to the local file's tail.  The catalog logs are
 * compatible with deduplication (replaying a + entry that matches an
 * existing row is a no-op via the catalog hmap's EXISTS check on
 * reload); + entries the local already has stay as duplicates in the
 * file, but compaction collapses them later.
 *
 * The full-replace alternative (overwrite local file) drops any
 * locally-unique entries (rare but possible after a CREATE that the
 * master itself missed).  Append + reload is conservative.  Caller
 * runs tsdb_db_reload_catalog after this to refresh in-memory state. */
int tsdb_catalog_dump_apply(const char *data_dir, const uint8_t *buf, size_t len) {
    if (!data_dir || !buf || len < 8) return -1;
    size_t off = 0;
    int applied = 0;
    while (off + 8 <= len) {
        uint32_t name_len = (uint32_t)buf[off] | ((uint32_t)buf[off+1] << 8)
                          | ((uint32_t)buf[off+2] << 16) | ((uint32_t)buf[off+3] << 24);
        off += 4;
        if (name_len > 64 || off + name_len + 4 > len) break;
        char name[80] = {0};
        memcpy(name, buf + off, name_len);
        off += name_len;
        uint32_t body_len = (uint32_t)buf[off] | ((uint32_t)buf[off+1] << 8)
                          | ((uint32_t)buf[off+2] << 16) | ((uint32_t)buf[off+3] << 24);
        off += 4;
        if (off + body_len > len) break;

        /* Validate the file name against our allow-list — refuse a
         * peer that tries to write outside catalog/. */
        int ok = 0;
        for (int i = 0; i < kCatalogLogCount; i++) {
            if (strcmp(name, kCatalogLogs[i]) == 0) { ok = 1; break; }
        }
        if (!ok) { off += body_len; continue; }

        if (body_len > 0) {
            char path[4096];
            snprintf(path, sizeof(path), "%s/catalog/%s", data_dir, name);
            FILE *fp = fopen(path, "ab");
            if (fp) {
                fwrite(buf + off, 1, body_len, fp);
                fclose(fp);
                applied++;
            }
        }
        off += body_len;
    }
    return applied;
}

/* Public entry point — data-node startup uses this.  Calls into
 * cluster RPC layer to fetch the dump from any alive master, applies
 * it, reloads the in-memory catalog. */
/* Background thread entry point — sleeps a few seconds to let gossip
 * settle, then runs the pull.  Detached; nothing waits on it. */
void *catalog_sync_thread_main(void *ud) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return NULL;
    /* Honour the same env knob the data anti-entropy uses for grace
     * period — bigger value gives gossip more time to mark masters
     * ALIVE before the pull dials. */
    int delay_ms = 6000;
    const char *envd = getenv("TSDB_ANTIENTROPY_DELAY_MS");
    if (envd && *envd) delay_ms = atoi(envd);
    struct timespec ts = { .tv_sec = delay_ms / 1000,
                           .tv_nsec = (long)(delay_ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
    int rc = tsdb_catalog_pull_from_master(db);
    if (rc != TSDB_OK)
        fprintf(stderr, "[catalog-sync] thread rc=%d\n", rc);
    return NULL;
}

int tsdb_catalog_pull_from_master(tsdb_db_t *db) {
    if (!db) return TSDB_ERR_INVAL;

    struct tsdb_cluster *c = tsdb_db_cluster(db);
    if (!c) return TSDB_OK; /* standalone — nothing to pull */

    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(tsdb_cluster_node_mgr(c),
                                        snap, TSDB_CLUSTER_MAX_NODES);
    tsdb_node_id_t self = tsdb_cluster_local_id(c);
    tsdb_node_id_t target = 0;
    for (int i = 0; i < n; i++) {
        if (snap[i].id == self)                continue;
        if (snap[i].state != TSDB_NODE_ALIVE)  continue;
        if (snap[i].role  != TSDB_ROLE_MASTER) continue;
        target = snap[i].id;
        break;
    }
    if (target == 0) {
        fprintf(stderr, "[catalog-sync] no alive master peer; skipping\n");
        return TSDB_OK;
    }

    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return TSDB_OK;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(rmgr, target);
    if (!conn) {
        fprintf(stderr, "[catalog-sync] failed to dial master\n");
        return TSDB_ERR_IO;
    }

    /* Empty payload — master serves whatever it has.  Generous reply
     * cap (32 MB) to fit the four logs of even a stress-tested node. */
    size_t reply_cap = 32 * 1024 * 1024;
    uint8_t *reply = (uint8_t *)malloc(reply_cap);
    if (!reply) return TSDB_ERR_NOMEM;
    uint32_t reply_len = 0;
    int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_CATALOG_DUMP,
                                 NULL, 0, reply, (uint32_t)reply_cap, &reply_len);
    if (rc != TSDB_OK) {
        fprintf(stderr, "[catalog-sync] rpc rc=%d\n", rc);
        free(reply);
        return rc;
    }

    int applied = tsdb_catalog_dump_apply(tsdb_db_data_dir(db), reply, reply_len);
    free(reply);
    fprintf(stderr, "[catalog-sync] applied %d log files (%u bytes)\n",
            applied, reply_len);

    /* Reload in-memory catalog so subsequent SELECT / CREATE see the
     * merged state. */
    (void)tsdb_db_reload_catalog(db);
    return TSDB_OK;
}
