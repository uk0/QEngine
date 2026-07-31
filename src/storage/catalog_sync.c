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
#include "../server/metrics.h"
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

/* ---- Resurrection-safe reverse merge (a recovered MASTER learns tables that
 * were created while it was down) ----------------------------------------
 *
 * tsdb_catalog_dump_apply above appends a peer's whole log verbatim — correct
 * data<-master, where the master log is authoritative.  The reverse direction
 * (master<-data, to learn an outage-window CREATE) needs a SAFE merge that can
 * only ADD genuinely-new tables and can never resurrect one this node
 * created-then-dropped (whose log carries a `-name` tombstone).  Rule: append a
 * peer `+NAME` record IFF
 *     NAME is in the peer's LIVE set (peer's last op for NAME is `+`)   AND
 *     NAME appears NOWHERE in our local log for that file.
 * Peer `-` lines are ignored outright: a stale peer must never drop a table we
 * legitimately hold (DROP still flows the authoritative master->data path). */

/* Name token = text between the 1st and 2nd TAB of a catalog log line. */
static int catlog_line_name(const char *p, size_t n, char *out, size_t cap) {
    size_t i = 0;
    while (i < n && p[i] != '\t' && p[i] != '\n') i++;
    if (i >= n || p[i] != '\t') return 0;
    i++;
    size_t j = 0;
    while (i < n && p[i] != '\t' && p[i] != '\n' && j + 1 < cap) out[j++] = p[i++];
    out[j] = 0;
    return j > 0;
}

/* Does `buf` contain, at/after byte `from`, a line whose op-char is `op` and
 * whose name token equals `name`? */
static int body_has_line(const char *buf, size_t len, size_t from,
                         char op, const char *name) {
    size_t i = from;
    while (i < len) {
        size_t ls = i;
        while (i < len && buf[i] != '\n') i++;
        size_t ll = i - ls;
        if (i < len) i++;
        if (ll < 2 || buf[ls] != op) continue;
        char nm[80];
        if (catlog_line_name(buf + ls, ll, nm, sizeof(nm)) && !strcmp(nm, name))
            return 1;
    }
    return 0;
}

/* Whole-file membership: does `lbuf` (local log) mention `name` on any line? */
static int local_mentions(const char *lbuf, size_t llen, const char *name) {
    size_t i = 0;
    while (i < llen) {
        size_t ls = i;
        while (i < llen && lbuf[i] != '\n') i++;
        size_t ll = i - ls;
        if (i < llen) i++;
        char nm[80];
        if (ll >= 2 && catlog_line_name(lbuf + ls, ll, nm, sizeof(nm)) && !strcmp(nm, name))
            return 1;
    }
    return 0;
}

/* Extract the Nth (0-based) TAB-separated field of a log line into out[]. */
static int catlog_line_field(const char *p, size_t n, int idx, char *out, size_t cap) {
    size_t i = 0; int f = 0;
    while (f < idx) {
        while (i < n && p[i] != '\t' && p[i] != '\n') i++;
        if (i >= n || p[i] != '\t') return 0;
        i++; f++;
    }
    size_t j = 0;
    while (i < n && p[i] != '\t' && p[i] != '\n' && j + 1 < cap) out[j++] = p[i++];
    out[j] = 0;
    return j > 0;
}

/* Is `name` a LIVE stable in <catalog_dir>/stables.log (its last op is '+')?
 * Used to refuse re-learning a child whose parent super-table is already gone —
 * otherwise reconcile resurrects orphan child rows that nothing ever cascades. */
static int local_stable_live(const char *catalog_dir, const char *name) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/stables.log", catalog_dir);
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    int live = 0;
    char line[8192];
    while (fgets(line, sizeof(line), f)) {
        char nm[80];
        if (catlog_line_name(line, strlen(line), nm, sizeof(nm)) && !strcmp(nm, name))
            live = (line[0] == '+');   /* last op for this name wins */
    }
    fclose(f);
    return live;
}

/* Append, to <catalog_dir>/<fname>, the peer body's `+` records for names that
 * are live on the peer and absent from the local log.  For child_tables.log a
 * `+child` is ALSO skipped unless its parent stable is live locally (the dump
 * applies stables.log before child_tables.log, so it already reflects this
 * dump's stables).  Returns # records added. */
static int filtered_append_log(const char *catalog_dir, const char *fname,
                               const char *body, size_t blen) {
    int is_child = (strcmp(fname, "child_tables.log") == 0);
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", catalog_dir, fname);
    char *lbuf = NULL; size_t llen = 0;
    FILE *lf = fopen(path, "rb");
    if (lf) {
        fseek(lf, 0, SEEK_END); long sz = ftell(lf); fseek(lf, 0, SEEK_SET);
        if (sz > 0 && sz < 64L * 1024 * 1024) {
            lbuf = malloc((size_t)sz);
            if (lbuf && fread(lbuf, 1, (size_t)sz, lf) == (size_t)sz) llen = (size_t)sz;
            else { free(lbuf); lbuf = NULL; }
        }
        fclose(lf);
    }
    char *added = NULL; size_t added_len = 0, added_cap = 0;   /* "\x01name\x01..." dedup */
    FILE *out = NULL;
    int count = 0;
    size_t i = 0;
    while (i < blen) {
        size_t ls = i;
        while (i < blen && body[i] != '\n') i++;
        size_t ll = i - ls;
        size_t next = (i < blen) ? i + 1 : i;
        if (i < blen) i++;
        if (ll < 2 || body[ls] != '+') continue;
        char nm[80];
        if (!catlog_line_name(body + ls, ll, nm, sizeof(nm))) continue;
        if (lbuf && local_mentions(lbuf, llen, nm)) continue;        /* known locally */
        char key[88]; int kl = snprintf(key, sizeof(key), "\x01%s\x01", nm);
        if (added && strstr(added, key)) continue;                  /* already added */
        if (body_has_line(body, blen, next, '-', nm)) continue;     /* peer dropped it later */
        if (is_child) {
            /* `+child\t<name>\t<stable_name>\t...` — refuse to re-learn a child
             * whose parent super-table isn't live locally (dropped/never seen),
             * which would otherwise resurrect an orphan that nothing cascades. */
            char parent[80];
            if (!catlog_line_field(body + ls, ll, 2, parent, sizeof(parent)) ||
                !local_stable_live(catalog_dir, parent))
                continue;
        }
        if (!out) { out = fopen(path, "ab"); if (!out) break; }
        fwrite(body + ls, 1, ll, out); fputc('\n', out);
        count++;
        if (added_len + (size_t)kl + 1 > added_cap) {
            added_cap = (added_cap ? added_cap * 2 : 1024) + (size_t)kl;
            char *na = realloc(added, added_cap);
            if (na) added = na; else continue;
        }
        memcpy(added + added_len, key, (size_t)kl); added_len += (size_t)kl; added[added_len] = 0;
    }
    if (out) fclose(out);
    free(lbuf); free(added);
    return count;
}

/* Apply a CATALOG_DUMP payload with the resurrection-safe filter (each of the
 * 4 logs).  Returns total records added across files. */
int tsdb_catalog_dump_apply_filtered(const char *data_dir,
                                     const uint8_t *buf, size_t len) {
    if (!data_dir || !buf || len < 8) return 0;
    size_t off = 0; int added = 0;
    while (off + 8 <= len) {
        uint32_t name_len = (uint32_t)buf[off] | ((uint32_t)buf[off+1] << 8)
                          | ((uint32_t)buf[off+2] << 16) | ((uint32_t)buf[off+3] << 24);
        off += 4;
        if (name_len > 64 || off + name_len + 4 > len) break;
        char name[80] = {0};
        memcpy(name, buf + off, name_len); off += name_len;
        uint32_t body_len = (uint32_t)buf[off] | ((uint32_t)buf[off+1] << 8)
                          | ((uint32_t)buf[off+2] << 16) | ((uint32_t)buf[off+3] << 24);
        off += 4;
        if (off + body_len > len) break;
        int ok = 0;
        for (int k = 0; k < kCatalogLogCount; k++)
            if (!strcmp(name, kCatalogLogs[k])) { ok = 1; break; }
        if (ok && body_len > 0) {
            char catalog_dir[4096];
            snprintf(catalog_dir, sizeof(catalog_dir), "%s/catalog", data_dir);
            added += filtered_append_log(catalog_dir, name,
                                         (const char *)(buf + off), body_len);
        }
        off += body_len;
    }
    return added;
}

/* Pull each alive peer's catalog dump and merge it resurrection-safe, so a
 * recovered node (incl. the master) learns tables created during its absence.
 * Returns the number of catalog records learned; reloads the in-memory catalog
 * if anything changed. */
int tsdb_catalog_reconcile_from_peers(tsdb_db_t *db) {
    if (!db) return 0;
    struct tsdb_cluster *c = tsdb_db_cluster(db);
    if (!c) return 0;
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) return 0;

    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(tsdb_cluster_node_mgr(c), snap, TSDB_CLUSTER_MAX_NODES);
    tsdb_node_id_t self = tsdb_cluster_local_id(c);

    size_t cap = 32 * 1024 * 1024;
    uint8_t *reply = malloc(cap);
    if (!reply) return 0;

    int total = 0;
    for (int i = 0; i < n; i++) {
        if (snap[i].id == self)               continue;
        if (snap[i].state != TSDB_NODE_ALIVE) continue;
        tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(rmgr, snap[i].id);
        if (!conn) continue;
        uint32_t rlen = 0; int rtrunc = 0;
        int rc = tsdb_rpc_call_recv_ex(conn, TSDB_RPC_CATALOG_DUMP,
                                       NULL, 0, reply, (uint32_t)cap, &rlen, &rtrunc);
        if (rc == TSDB_OK && rtrunc) {
            /* rlen is only a PREFIX of a dump that overran our 32 MB buffer, so
             * its tail record is cut mid-way.  Applying it would parse a
             * partial record into the catalog — skip this peer and surface the
             * short read instead of corrupting the merge. */
            tsdb_metric_inc("qengine_catalog_dump_truncated_total");
            fprintf(stderr, "[catalog-reconcile] peer %llu dump exceeded %zu-byte "
                    "buffer — skipped (truncated)\n",
                    (unsigned long long)snap[i].id, cap);
            continue;
        }
        if (rc == TSDB_OK && rlen > 0)
            total += tsdb_catalog_dump_apply_filtered(tsdb_db_data_dir(db), reply, rlen);
    }
    free(reply);

    if (total > 0) {
        (void)tsdb_db_reload_catalog(db);
        fprintf(stderr, "[catalog-reconcile] learned %d missing catalog record(s) from peers\n",
                total);
    }
    return total;
}

/* Public entry point — data-node startup uses this.  Calls into
 * cluster RPC layer to fetch the dump from any alive master, applies
 * it, reloads the in-memory catalog. */
/* Background thread entry point — polls gossip until an alive master
 * appears, then runs the pull.  Detached; nothing waits on it.
 *
 * Pre-fix this slept a fixed 6 s and quit silently if no master had
 * been discovered yet.  Gossip discovery on lvm1's 4-node cluster
 * actually takes 10-60 s after a force-recreate (each node mints a
 * fresh node_id and the others don't trust it until enough heartbeats
 * land), so the one-shot pull always saw alive_nodes=1 and bailed
 * with no log line.  Now the thread polls every 2 s for up to 60 s. */
void *catalog_sync_thread_main(void *ud) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return NULL;

    int initial_delay_ms = 4000;  /* let socket binds settle */
    int total_budget_ms  = 60000; /* hard cap so a stuck cluster doesn't pin
                                     this thread forever */
    int poll_interval_ms = 2000;

    const char *envd = getenv("TSDB_ANTIENTROPY_DELAY_MS");
    if (envd && *envd) initial_delay_ms = atoi(envd);

    struct timespec ts = { .tv_sec = initial_delay_ms / 1000,
                           .tv_nsec = (long)(initial_delay_ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);

    int waited = initial_delay_ms;
    while (waited < total_budget_ms) {
        /* Probe gossip for an alive master.  tsdb_catalog_pull_from_master
         * itself returns OK when no master is found (treats as no-op),
         * so we loop until the pull observably succeeds — the function
         * logs `[catalog-sync] applied N log files` on success. */
        struct tsdb_cluster *c = tsdb_db_cluster(db);
        if (c) {
            tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
            int nn = tsdb_node_manager_snapshot(tsdb_cluster_node_mgr(c),
                                                 snap, TSDB_CLUSTER_MAX_NODES);
            tsdb_node_id_t self = tsdb_cluster_local_id(c);
            int found_master = 0;
            for (int i = 0; i < nn; i++) {
                if (snap[i].id == self)                continue;
                if (snap[i].state != TSDB_NODE_ALIVE)  continue;
                if (snap[i].role  != TSDB_ROLE_MASTER) continue;
                found_master = 1;
                break;
            }
            if (found_master) {
                int rc = tsdb_catalog_pull_from_master(db);
                if (rc == TSDB_OK) return NULL;  /* done — pull ran */
                fprintf(stderr, "[catalog-sync] retry: pull rc=%d\n", rc);
            }
        }
        struct timespec p = { .tv_sec = poll_interval_ms / 1000,
                              .tv_nsec = (long)(poll_interval_ms % 1000) * 1000000 };
        nanosleep(&p, NULL);
        waited += poll_interval_ms;
    }
    fprintf(stderr,
            "[catalog-sync] gave up after %d ms — no alive master found\n",
            waited);
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
        /* BUSY (not OK) so the polling caller keeps retrying — gossip
         * may discover a master in a later poll.  Pre-fix this returned
         * TSDB_OK and the polling loop exited as if the pull had run. */
        fprintf(stderr, "[catalog-sync] no alive master peer yet — will retry\n");
        return TSDB_ERR_BUSY;
    }

    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);
    if (!rmgr) {
        /* Same silent-OK trap: replica mgr can be momentarily NULL
         * during early startup before the cluster wires it up. */
        fprintf(stderr, "[catalog-sync] replica mgr not ready — will retry\n");
        return TSDB_ERR_BUSY;
    }
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
    uint32_t reply_len = 0; int reply_trunc = 0;
    int rc = tsdb_rpc_call_recv_ex(conn, TSDB_RPC_CATALOG_DUMP,
                                    NULL, 0, reply, (uint32_t)reply_cap, &reply_len,
                                    &reply_trunc);
    if (rc != TSDB_OK) {
        fprintf(stderr, "[catalog-sync] rpc rc=%d\n", rc);
        free(reply);
        return rc;
    }
    if (reply_trunc) {
        /* The master's dump overran our buffer; reply_len bytes are a prefix
         * whose last record is cut.  Applying it would publish a half-parsed
         * catalog — fail the pull so the caller retries rather than install a
         * partial record. */
        tsdb_metric_inc("qengine_catalog_dump_truncated_total");
        fprintf(stderr, "[catalog-sync] master dump exceeded %zu-byte buffer — "
                "aborting (truncated)\n", reply_cap);
        free(reply);
        return TSDB_ERR_IO;
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
