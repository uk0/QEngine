/* tsdb_node_main.c — CLI entry point for a standalone cluster node.
 *
 * Usage:
 *   tsdb_node --data-dir /tmp/node1 --rpc 127.0.0.1:28081 \
 *             [--seeds 127.0.0.1:28080,127.0.0.1:28082]
 *
 * Starts a tsdb cluster node and blocks until SIGTERM/SIGINT.
 */

/* Must include tsdb.h via full path since src/cluster/ is not include root. */
#include "../../include/tsdb.h"
#include "../../include/tsdb_cluster.h"
#include "../server/server.h"
#include "../server/metrics_server.h"
#include "../server/metrics.h"
#include "../server/influx_line.h"
#include "../raft/raft.h"
#include "disk_weight.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <inttypes.h>

static volatile int g_running = 1;
static time_t       g_node_start_epoch = 0;   /* set in main() */
static char         g_local_role[16] = "master"; /* TSDB_NODE_ROLE, set in main() */
char                g_local_data_dir[4096] = {0};  /* defined here (non-static)
                                                      so snapshot callbacks
                                                      can extern-declare it. */

/* Forward decl for the /raft JSON callback — full body lives next to
 * the other *_json_cb helpers further down. */
static int raft_json_cb(void *ud, char *buf, size_t cap);

/* Thread-local flags re-used by exec.c to keep the Raft apply path
 * from re-entering itself.  Declared extern here so we can flip them
 * on the apply thread before replaying the QTL. */
extern __thread int tsdb_g_suppress_catalog_broadcast;
extern __thread int tsdb_g_inside_raft_apply;

/* Forward decl: g_local_data_dir is defined further down but used by
 * the snapshot callbacks that live up here (they need to be visible
 * before raft_apply_cb). */
extern char g_local_data_dir[];
static int  tsdb_mkdir_p_noerr(const char *path);

/* ---- Catalog snapshot callbacks ----------------------------------------
 *
 * raft_snapshot_write_cb serialises every .log file under
 * <data_dir>/catalog/ into a contiguous byte stream.  The body shape:
 *   u32 num_files
 *   for each file:
 *     u32 name_len       (bytes including NUL, max 255)
 *     name[name_len]
 *     u64 content_len
 *     content[content_len]
 *
 * raft_snapshot_restore_cb parses the same format and atomically
 * swaps the catalog/.log files on disk.  It does NOT hot-reopen the
 * in-memory catalog — that requires db.c surgery and is tracked as a
 * follow-up.  A restart applies the snapshot contents; until then the
 * node's state machine sits on the prior catalog (but at least the
 * raft log+markers are now in sync with the leader, so it stops
 * spinning on AE mismatches). */

static int raft_snapshot_write_cb(void *ud, uint8_t **out, uint32_t *out_len) {
    (void)ud;
    *out = NULL; *out_len = 0;
    if (!g_local_data_dir[0]) return -1;

    char cat_dir[4200];
    snprintf(cat_dir, sizeof(cat_dir), "%s/catalog", g_local_data_dir);
    DIR *d = opendir(cat_dir);
    if (!d) return -1;

    /* First pass: collect file names + sizes. */
    struct { char name[128]; size_t sz; } files[32];
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL && n < 32) {
        const char *ext = strrchr(de->d_name, '.');
        if (!ext || strcmp(ext, ".log") != 0) continue;
        if (strlen(de->d_name) >= sizeof(files[0].name)) continue;
        char path[4400];
        snprintf(path, sizeof(path), "%s/%s", cat_dir, de->d_name);
        struct stat st;
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        snprintf(files[n].name, sizeof(files[n].name), "%s", de->d_name);
        files[n].sz = (size_t)st.st_size;
        n++;
    }
    closedir(d);
    if (n == 0) return -1;

    /* Second pass: allocate buffer + copy. */
    size_t total = 4; /* num_files */
    for (int i = 0; i < n; i++) {
        size_t nm = strlen(files[i].name) + 1;
        total += 4 + nm + 8 + files[i].sz;
    }
    uint8_t *buf = malloc(total);
    if (!buf) return -1;

    size_t off = 0;
    uint32_t nf = (uint32_t)n;
    memcpy(buf + off, &nf, 4); off += 4;
    for (int i = 0; i < n; i++) {
        uint32_t nm = (uint32_t)(strlen(files[i].name) + 1);
        memcpy(buf + off, &nm, 4); off += 4;
        memcpy(buf + off, files[i].name, nm); off += nm;
        uint64_t sz = files[i].sz;
        memcpy(buf + off, &sz, 8); off += 8;
        if (sz == 0) continue;
        char path[4400];
        snprintf(path, sizeof(path), "%s/%s", cat_dir, files[i].name);
        FILE *fp = fopen(path, "rb");
        if (!fp) { free(buf); return -1; }
        size_t got = fread(buf + off, 1, sz, fp);
        fclose(fp);
        if (got != sz) { free(buf); return -1; }
        off += sz;
    }

    *out = buf;
    *out_len = (uint32_t)off;
    return 0;
}

static int raft_snapshot_restore_cb(void *ud,
                                     const uint8_t *data, uint32_t data_len)
{
    tsdb_db_t *db = (tsdb_db_t *)ud;   /* may be NULL on a minimal setup */
    if (!data || data_len < 4 || !g_local_data_dir[0]) return -1;

    char cat_dir[4200];
    snprintf(cat_dir, sizeof(cat_dir), "%s/catalog", g_local_data_dir);

    /* Parse and write into a staging dir so we don't clobber the live
     * catalog mid-restore.  Then atomic-rename each file over. */
    char stage[4232];
    snprintf(stage, sizeof(stage), "%s/snapshot-stage", cat_dir);
    (void)tsdb_mkdir_p_noerr(stage);  /* helper below */

    size_t off = 0;
    uint32_t nf = 0;
    memcpy(&nf, data + off, 4); off += 4;
    if (nf > 32) return -1;

    for (uint32_t i = 0; i < nf; i++) {
        if (off + 4 > data_len) return -1;
        uint32_t nm = 0;
        memcpy(&nm, data + off, 4); off += 4;
        if (nm == 0 || nm > 128 || off + nm + 8 > data_len) return -1;
        char name[128];
        memcpy(name, data + off, nm);
        name[nm - 1] = '\0';   /* incoming carries its own NUL but
                                  be defensive in case the sender was
                                  off by one */
        off += nm;
        uint64_t sz = 0;
        memcpy(&sz, data + off, 8); off += 8;
        if (sz > data_len || off + sz > data_len) return -1;
        char spath[4360];
        snprintf(spath, sizeof(spath), "%s/%s", stage, name);
        FILE *fp = fopen(spath, "wb");
        if (!fp) return -1;
        if (sz > 0 && fwrite(data + off, 1, sz, fp) != sz) {
            fclose(fp); return -1;
        }
        fflush(fp);
        fsync(fileno(fp));
        fclose(fp);
        off += sz;

        /* Atomic swap over the live file. */
        char dpath[4232];
        snprintf(dpath, sizeof(dpath), "%s/%s", cat_dir, name);
        if (rename(spath, dpath) != 0) return -1;
    }

    /* Hot-reopen the in-memory catalog so subsequent LIST / query /
     * apply paths see the leader's state without a process restart.
     * When db is NULL (legacy callers that didn't plumb it) we fall
     * back to the old "restart required" behaviour — disk state is
     * correct, only the live hashmap is stale. */
    if (db) {
        int rrc = tsdb_db_reload_catalog(db);
        if (rrc != TSDB_OK) {
            fprintf(stderr,
                    "[raft-snap] restored %u file(s) (%u B) but catalog "
                    "reload failed rc=%d — node may serve stale data "
                    "until restart\n", nf, data_len, rrc);
            return 0;
        }
        fprintf(stderr,
                "[raft-snap] restored %u catalog file(s) from leader "
                "(%u bytes); in-memory catalog reloaded\n", nf, data_len);
    } else {
        fprintf(stderr,
                "[raft-snap] restored %u catalog file(s) from leader "
                "(%u bytes); node restart required for in-memory "
                "catalog to reflect the snapshot\n", nf, data_len);
    }
    return 0;
}

/* Helper: mkdir -p, ignore EEXIST.  db.c has tsdb_mkdir_p but it's
 * not on the public surface — duplicate here to avoid coupling. */
static int tsdb_mkdir_p_noerr(const char *path) {
    char tmp[4232]; snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') { *p = 0; mkdir(tmp, 0755); *p = '/'; }
    }
    mkdir(tmp, 0755);
    return 0;
}

/* raft_apply_cb — invoked on every committed log entry.  Replays the
 * QTL locally (with both suppression flags on) so every master ends
 * up with the same catalog state.  Failures are logged but never
 * roll back — Raft guarantees apply is idempotent across nodes. */
static int raft_apply_cb(void *ud, const tsdb_raft_entry_t *e) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db || !e) return 0;
    if (e->type != TSDB_RAFT_ENTRY_CATALOG_QTL) return 0;
    if (e->payload_len == 0 || !e->payload) return 0;

    /* Copy payload to a NUL-terminated buffer for tsdb_query.  Cap to
     * 16 KB; catalog statements are tiny in practice. */
    if (e->payload_len > 16 * 1024) {
        fprintf(stderr, "[raft-apply] oversized QTL (%u bytes), skipping\n",
                e->payload_len);
        return 0;
    }
    char *qtl = malloc(e->payload_len + 1);
    if (!qtl) return 0;
    memcpy(qtl, e->payload, e->payload_len);
    qtl[e->payload_len] = '\0';

    tsdb_g_suppress_catalog_broadcast = 1;
    tsdb_g_inside_raft_apply          = 1;
    tsdb_result_t *res = NULL;
    int qrc = tsdb_query(db, qtl, &res);
    tsdb_g_inside_raft_apply          = 0;
    tsdb_g_suppress_catalog_broadcast = 0;

    if (res) tsdb_result_free(res);
    if (qrc != TSDB_OK && qrc != TSDB_ERR_EXISTS) {
        /* Log, don't fail — Raft can't roll back apply.  An EXISTS
         * here is normal on peer catch-up where the DB already has
         * the catalog row from a prior broadcast. */
        fprintf(stderr, "[raft-apply] tsdb_query rc=%d qtl=%s\n", qrc, qtl);
    }

    /* Fan the committed QTL out to data-node peers so their local
     * catalog stays in sync.  Masters already apply this entry
     * through their own Raft state machine and must be skipped
     * (broadcasting to them would cause a duplicate DDL attempt
     * and spurious EXISTS errors).  If qrc was neither OK nor
     * EXISTS, skip the broadcast — we don't want to propagate a
     * partial/failed state machine transition to observers. */
    if (qrc == TSDB_OK || qrc == TSDB_ERR_EXISTS) {
        (void)tsdb_cluster_broadcast_catalog_qtl_to_data(db, qtl, NULL, NULL);
    }
    free(qtl);
    return 0;
}

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --data-dir <dir> --rpc <host:port>\n"
        "       [--seeds <seed,...>] [--bind <host:port>]\n"
        "       [--metrics-bind <host:port>]\n"
        "\n"
        "  --data-dir       Local storage directory (created if absent)\n"
        "  --rpc            TCP RPC bind address (e.g. 0.0.0.0:28081)\n"
        "  --seeds          Comma-separated gossip seed addresses\n"
        "  --bind           Wire-protocol client bind (default 0.0.0.0:28090)\n"
        "                   — exposes the same API as tsdb-server.\n"
        "  --metrics-bind   /metrics + /health + / dashboard bind\n"
        "                   (default 0.0.0.0:28094)\n"
        "\n"
        "Gossip UDP port = RPC port - 1.\n",
        prog);
}

/* raft_json_cb — serialise the local Raft state machine for /raft.
 * Small, stable JSON consumed by tests/raft/lib.sh + the dashboard.
 * NULL `ud` → {} (raft not running on this node). */
static int raft_json_cb(void *ud, char *buf, size_t cap) {
    tsdb_raft_t *r = (tsdb_raft_t *)ud;
    if (!r || cap < 8) return 0;
    static const char *state_names[] = { "follower", "candidate", "leader" };
    tsdb_raft_state_t st = tsdb_raft_state(r);
    const char *sname = (st <= 2) ? state_names[st] : "?";
    int n = snprintf(buf, cap,
                     "{\"self_id\":\"%llu\","
                     "\"role\":\"%s\","
                     "\"current_term\":%llu,"
                     "\"leader_id\":\"%llu\","
                     "\"commit_index\":%llu,"
                     "\"last_applied\":%llu,"
                     "\"last_index\":%llu,"
                     "\"snapshot_index\":%llu}",
                     (unsigned long long)tsdb_raft_self_id(r),
                     sname,
                     (unsigned long long)tsdb_raft_current_term(r),
                     (unsigned long long)tsdb_raft_leader_id(r),
                     (unsigned long long)tsdb_raft_commit_index(r),
                     (unsigned long long)tsdb_raft_last_applied(r),
                     (unsigned long long)tsdb_raft_last_index(r),
                     (unsigned long long)tsdb_raft_snapshot_index(r));
    return (n > 0 && (size_t)n < cap) ? n : 0;
}

/* cluster_json_cb — provide /cluster HTTP payload.
 *
 * tsdb_cluster_stats returns gossip-level node state (id / addr /
 * state / ver) plus autobalance stats.  Gossip does NOT carry per-node
 * disk capacity today, so this wrapper also appends a "local" object
 * with THIS node's statvfs-derived total_bytes / free_bytes / vn_weight
 * — the dashboard uses it to annotate the self row.  Peer disk is
 * shown as "-" until the gossip protocol grows the field (separate
 * change, involves on-wire schema bump).
 */
/* g_local_data_dir is now defined above (non-static) so the
 * snapshot callbacks and cluster_json_cb share the same storage. */

static int cluster_json_cb(void *ud, char *buf, size_t cap) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return 0;
    int n = tsdb_cluster_stats(db, buf, cap);
    if (n <= 0 || (size_t)n >= cap - 1) return n;

    /* Splice a "local" field in right before the trailing '}'.
     * tsdb_cluster_stats always ends with '}'; we remove that, append
     * our field, and re-terminate. */
    while (n > 0 && (buf[n-1] == '\n' || buf[n-1] == '\0' || buf[n-1] == ' '))
        n--;
    if (n == 0 || buf[n-1] != '}') return n;
    n--;

    uint64_t total = 0, free_b = 0;
    int vn = 0;
    if (g_local_data_dir[0]) {
        vn = tsdb_disk_weight_detail(g_local_data_dir,
                                      TSDB_DISK_WEIGHT_DEFAULT_PER_TB,
                                      &total, &free_b);
    }
    /* used% with one decimal (caller renders as-is).  We emit used_x10
     * as integer to dodge any locale-specific '%f' rendering ambiguity
     * — JS side divides by 10. */
    int used_x10 = 0;
    if (total > 0 && total >= free_b)
        used_x10 = (int)(((total - free_b) * 1000ULL) / total);

    char host[128] = "self";
    gethostname(host, sizeof(host) - 1);
    host[sizeof(host) - 1] = '\0';

    long uptime_s = 0;
    if (g_node_start_epoch > 0) {
        time_t nowt = time(NULL);
        if (nowt > g_node_start_epoch) uptime_s = (long)(nowt - g_node_start_epoch);
    }

    int add = snprintf(buf + n, cap - (size_t)n,
        ",\"local\":{"
            "\"host\":\"%s\","
            "\"pid\":%d,"
            "\"uptime_s\":%ld,"
            "\"disk\":{\"total_bytes\":%llu,\"free_bytes\":%llu,"
                      "\"used_x10\":%d,\"data_dir\":\"%s\",\"vn_weight\":%d}"
        "}}",
        host, (int)getpid(), uptime_s,
        (unsigned long long)total, (unsigned long long)free_b,
        used_x10, g_local_data_dir, vn);
    if (add < 0) return n + 1;   /* should be unreachable */
    return n + add;
}

/* ---- /tree provider ------------------------------------------------------
 *
 * Walks <data_dir>/ and reports every subdirectory as a table.  Each table
 * row reports the column count (by opening the table and reading schema).
 * Stable/device hierarchy is gathered by piggy-backing on LIST GROUPS /
 * LIST DEVICES QTL statements — if the result set is non-empty we nest
 * devices under a synthesised "_groups" virtual table.
 *
 * Errors are treated as "skip this entry" — the dashboard shows what's
 * reachable instead of failing loud on a half-open table. */
static int j_escape_str(char *dst, size_t cap, const char *s) {
    size_t w = 0;
    for (size_t i = 0; s && s[i] && w + 2 < cap; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '"' || c == '\\') { dst[w++] = '\\'; dst[w++] = (char)c; }
        else if (c == '\n')        { dst[w++] = '\\'; dst[w++] = 'n'; }
        else if (c == '\r')        { dst[w++] = '\\'; dst[w++] = 'r'; }
        else if (c == '\t')        { dst[w++] = '\\'; dst[w++] = 't'; }
        else if (c < 0x20)         { /* drop */ }
        else                       { dst[w++] = (char)c; }
    }
    if (w < cap) dst[w] = '\0';
    return (int)w;
}

/* Return 1 if the name looks like a valid table directory (not ".", "..",
 * not starting with '.', not the catalog dir). */
static int is_table_dir(const char *name) {
    if (!name || name[0] == '.' || name[0] == '_') return 0;
    if (strcmp(name, "catalog") == 0) return 0;
    if (strcmp(name, "wal")     == 0) return 0;
    return 1;
}

/* Recursively sum file sizes under a directory.  Used to surface the
 * data_dir's disk footprint in the dashboard's Database header. */
static uint64_t dir_bytes_recursive(const char *path, int depth) {
    if (depth > 6) return 0;          /* guard against pathological layouts */
    DIR *d = opendir(path);
    if (!d) return 0;
    uint64_t total = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char full[4096];
        snprintf(full, sizeof(full), "%s/%s", path, de->d_name);
        struct stat st;
        if (lstat(full, &st) != 0) continue;
        if (S_ISREG(st.st_mode))      total += (uint64_t)st.st_size;
        else if (S_ISDIR(st.st_mode)) total += dir_bytes_recursive(full, depth + 1);
    }
    closedir(d);
    return total;
}

static int tree_json_cb(void *ud, char *buf, size_t cap) {
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db || !g_local_data_dir[0]) return 0;

    DIR *d = opendir(g_local_data_dir);
    if (!d) return 0;

    /* Derive db name = basename(data_dir).  Fallback to full path. */
    const char *db_name = strrchr(g_local_data_dir, '/');
    db_name = (db_name && db_name[1]) ? db_name + 1 : g_local_data_dir;

    char host[128] = "self";
    gethostname(host, sizeof(host) - 1);
    host[sizeof(host) - 1] = '\0';

    long uptime_s = 0;
    if (g_node_start_epoch > 0) {
        time_t nowt = time(NULL);
        if (nowt > g_node_start_epoch) uptime_s = (long)(nowt - g_node_start_epoch);
    }

    uint64_t disk_bytes = dir_bytes_recursive(g_local_data_dir, 0);

    int w = 0;
    char esc[1024];
    char name_esc[256];
    j_escape_str(esc, sizeof(esc), g_local_data_dir);
    j_escape_str(name_esc, sizeof(name_esc), db_name);
    w += snprintf(buf + w, cap - (size_t)w,
                  "{\"db\":{\"name\":\"%s\",\"path\":\"%s\","
                   "\"host\":\"%s\",\"uptime_s\":%ld,"
                   "\"disk_bytes\":%llu,\"role\":\"%s\"},\"tables\":[",
                   name_esc, esc, host, uptime_s,
                   (unsigned long long)disk_bytes, g_local_role);

    /* Legacy "tables" section — a flat on-disk dir scan.  In an
     * industrial catalog (thousands of PTables) this duplicates what
     * `ptables:[]` below already exposes via the catalog, and walking
     * each dir with dir_bytes_recursive() is expensive.  Cap the list
     * at a handful so the dashboard still shows something on blank
     * deployments, but punt large catalogs through the cleaner
     * databases/vtables/ptables sections. */
    int first = 1;
    int tables_emitted = 0;
    const int TABLES_SECTION_CAP = 32;
    struct dirent *de;
    while ((de = readdir(d)) != NULL && (size_t)w < cap - 256
           && tables_emitted < TABLES_SECTION_CAP) {
        if (!is_table_dir(de->d_name)) continue;
        /* Must be a directory. */
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s", g_local_data_dir, de->d_name);
        struct stat st;
        if (stat(path, &st) != 0) continue;
        if (!S_ISDIR(st.st_mode)) continue;

        tsdb_table_t *tbl = NULL;
        int rc = tsdb_open_table(db, de->d_name, &tbl);
        if (rc != 0) continue;

        char tbl_name_esc[256];
        j_escape_str(tbl_name_esc, sizeof(tbl_name_esc), de->d_name);
        uint64_t tbl_bytes = dir_bytes_recursive(path, 0);
        w += snprintf(buf + w, cap - (size_t)w,
                      "%s{\"name\":\"%s\",\"kind\":\"table\","
                       "\"bytes\":%llu}",
                      first ? "" : ",", tbl_name_esc,
                      (unsigned long long)tbl_bytes);
        first = 0;
        tables_emitted++;
        (void)tbl;
    }
    closedir(d);

    /* DATABASES section — top-level logical namespace. */
    w += snprintf(buf + w, cap - (size_t)w, "],\"databases\":[");
    {
        tsdb_result_t *res = NULL;
        if (tsdb_query(db, "LIST DATABASES;", &res) == 0 && res) {
            int ncols = tsdb_result_ncols(res);
            int first_db = 1;
            while (tsdb_result_next(res) > 0 && (size_t)w < cap - 256) {
                const char *dn = NULL, *ddesc = NULL;
                int prot = 0;
                for (int c = 0; c < ncols; c++) {
                    const char *cn = tsdb_result_col_name(res, c);
                    if      (cn && strcmp(cn, "name") == 0)        dn    = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "description") == 0) ddesc = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "protected") == 0)   prot  = (int)tsdb_result_i64(res, c);
                }
                if (!dn) continue;
                char ne[128], de[256];
                j_escape_str(ne, sizeof(ne), dn);
                j_escape_str(de, sizeof(de), ddesc ? ddesc : "");
                w += snprintf(buf + w, cap - (size_t)w,
                              "%s{\"name\":\"%s\",\"description\":\"%s\","
                              "\"protected\":%d}",
                              first_db ? "" : ",", ne, de, prot);
                first_db = 0;
            }
            tsdb_result_free(res);
        }
    }

    /* VTABLES section (super-tables).  4-level path: DB → Group → VTable. */
    w += snprintf(buf + w, cap - (size_t)w, "],\"vtables\":[");
    {
        tsdb_result_t *res = NULL;
        if (tsdb_query(db, "LIST VTABLES;", &res) == 0 && res) {
            int ncols = tsdb_result_ncols(res);
            int first_v = 1;
            while (tsdb_result_next(res) > 0 && (size_t)w < cap - 256) {
                const char *vn = NULL, *vdb = NULL, *vgrp = NULL;
                int ncols_v = 0, ntags_v = 0;
                for (int c = 0; c < ncols; c++) {
                    const char *cn = tsdb_result_col_name(res, c);
                    if      (cn && strcmp(cn, "name")       == 0) vn      = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "database")   == 0) vdb     = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "group")      == 0) vgrp    = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "ncols")      == 0) ncols_v = (int)tsdb_result_i64(res, c);
                    else if (cn && strcmp(cn, "ntag_cols")  == 0) ntags_v = (int)tsdb_result_i64(res, c);
                }
                if (!vn) continue;
                char ne[128], dbe[128], gre[128];
                j_escape_str(ne,  sizeof(ne),  vn);
                j_escape_str(dbe, sizeof(dbe), vdb  ? vdb  : "");
                j_escape_str(gre, sizeof(gre), vgrp ? vgrp : "");
                w += snprintf(buf + w, cap - (size_t)w,
                              "%s{\"name\":\"%s\",\"database\":\"%s\","
                              "\"group\":\"%s\",\"ncols\":%d,\"ntags\":%d}",
                              first_v ? "" : ",", ne, dbe, gre, ncols_v, ntags_v);
                first_v = 0;
            }
            tsdb_result_free(res);
        }
    }

    /* PTABLES section (child tables).  Inherits ancestry from parent VT. */
    w += snprintf(buf + w, cap - (size_t)w, "],\"ptables\":[");
    {
        tsdb_result_t *res = NULL;
        if (tsdb_query(db, "LIST PTABLES;", &res) == 0 && res) {
            int ncols = tsdb_result_ncols(res);
            int first_p = 1;
            while (tsdb_result_next(res) > 0 && (size_t)w < cap - 256) {
                const char *pn = NULL, *pv = NULL, *pdb = NULL, *pgrp = NULL;
                for (int c = 0; c < ncols; c++) {
                    const char *cn = tsdb_result_col_name(res, c);
                    if      (cn && strcmp(cn, "name")     == 0) pn   = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "vtable")   == 0) pv   = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "database") == 0) pdb  = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "group")    == 0) pgrp = tsdb_result_sym(res, c);
                }
                if (!pn) continue;
                char ne[128], ve[128], dbe[128], gre[128];
                j_escape_str(ne,  sizeof(ne),  pn);
                j_escape_str(ve,  sizeof(ve),  pv   ? pv   : "");
                j_escape_str(dbe, sizeof(dbe), pdb  ? pdb  : "");
                j_escape_str(gre, sizeof(gre), pgrp ? pgrp : "");
                w += snprintf(buf + w, cap - (size_t)w,
                              "%s{\"name\":\"%s\",\"vtable\":\"%s\","
                              "\"database\":\"%s\",\"group\":\"%s\"}",
                              first_p ? "" : ",", ne, ve, dbe, gre);
                first_p = 0;
            }
            tsdb_result_free(res);
        }
    }

    /* GROUPS section — each carries the parent `database`. */
    w += snprintf(buf + w, cap - (size_t)w, "],\"groups\":[");
    {
        tsdb_result_t *res = NULL;
        if (tsdb_query(db, "LIST GROUPS;", &res) == 0 && res) {
            int ncols = tsdb_result_ncols(res);
            int gfirst = 1;
            while (tsdb_result_next(res) > 0 && (size_t)w < cap - 256) {
                const char *gname = NULL, *gdb = NULL;
                for (int c = 0; c < ncols; c++) {
                    const char *cn = tsdb_result_col_name(res, c);
                    if      (cn && strcmp(cn, "name") == 0)     gname = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "database") == 0) gdb   = tsdb_result_sym(res, c);
                }
                if (!gname) continue;
                char ge[256], dbe[128];
                j_escape_str(ge,  sizeof(ge),  gname);
                j_escape_str(dbe, sizeof(dbe), gdb ? gdb : "");
                w += snprintf(buf + w, cap - (size_t)w,
                              "%s{\"name\":\"%s\",\"database\":\"%s\"}",
                              gfirst ? "" : ",", ge, dbe);
                gfirst = 0;
            }
            tsdb_result_free(res);
        }
    }

    /* DEVICES section — each carries the parent `group`. */
    w += snprintf(buf + w, cap - (size_t)w, "],\"devices\":[");
    {
        tsdb_result_t *res = NULL;
        if (tsdb_query(db, "LIST DEVICES;", &res) == 0 && res) {
            int ncols = tsdb_result_ncols(res);
            int first = 1;
            while (tsdb_result_next(res) > 0 && (size_t)w < cap - 256) {
                const char *dgroup = NULL, *did = NULL;
                for (int c = 0; c < ncols; c++) {
                    const char *cn = tsdb_result_col_name(res, c);
                    if      (cn && strcmp(cn, "group") == 0) dgroup = tsdb_result_sym(res, c);
                    else if (cn && strcmp(cn, "id")    == 0) did    = tsdb_result_sym(res, c);
                }
                if (!did) continue;
                char gee[128], idee[128];
                j_escape_str(gee,  sizeof(gee),  dgroup ? dgroup : "");
                j_escape_str(idee, sizeof(idee), did);
                w += snprintf(buf + w, cap - (size_t)w,
                              "%s{\"name\":\"%s\",\"group\":\"%s\"}",
                              first ? "" : ",", idee, gee);
                first = 0;
            }
            tsdb_result_free(res);
        }
    }
    w += snprintf(buf + w, cap - (size_t)w, "]}\n");
    return w;
}

/* ---- /sql provider -------------------------------------------------------
 *
 * Execute a single QTL statement, stream the result into a JSON object of
 * shape: {"cols":[…],"types":[…],"rows":[[…]],"nrows":N,"truncated":bool,
 *        "ms":X}.  Row limit is enforced here at 1000; caller-supplied
 * LIMIT in the query is respected naturally. */
#define SQL_ROW_CAP  1000

static int write_json_cell(char *buf, size_t cap, tsdb_result_t *res,
                           int col, tsdb_type_t ty)
{
    if (tsdb_result_is_null(res, col))
        return snprintf(buf, cap, "null");
    switch (ty) {
        case TSDB_TYPE_TIMESTAMP:
            return snprintf(buf, cap, "%" PRId64,
                            (int64_t)tsdb_result_ts(res, col));
        case TSDB_TYPE_INT64:
            return snprintf(buf, cap, "%" PRId64,
                            tsdb_result_i64(res, col));
        case TSDB_TYPE_FLOAT64: {
            double v = tsdb_result_f64(res, col);
            /* JSON has no NaN/Inf — emit null. */
            if (v != v || v > 1e308 || v < -1e308)
                return snprintf(buf, cap, "null");
            return snprintf(buf, cap, "%.17g", v);
        }
        case TSDB_TYPE_SYMBOL: {
            const char *s = tsdb_result_sym(res, col);
            if (!s) return snprintf(buf, cap, "null");
            int w = 0;
            if (cap > 2) { buf[w++] = '"'; }
            char esc[4096];
            j_escape_str(esc, sizeof(esc), s);
            int add = snprintf(buf + w, cap - (size_t)w, "%s", esc);
            if (add > 0) w += add;
            if ((size_t)w + 1 < cap) buf[w++] = '"';
            if ((size_t)w < cap) buf[w] = '\0';
            return w;
        }
    }
    return snprintf(buf, cap, "null");
}

static const char *type_name(tsdb_type_t t) {
    switch (t) {
        case TSDB_TYPE_TIMESTAMP: return "TIMESTAMP";
        case TSDB_TYPE_INT64:     return "INT64";
        case TSDB_TYPE_FLOAT64:   return "FLOAT64";
        case TSDB_TYPE_SYMBOL:    return "SYMBOL";
    }
    return "UNKNOWN";
}

/* Minimal TCP-HTTP client just good enough to forward POST /sql from
 * a data node to a master dashboard.  No keepalive, no pipelining,
 * one request per connection; blocking I/O with a generous overall
 * timeout.  Every cluster node exposes the dashboard API on the same
 * conventional port (28094), so we swap the port in the master's
 * gossip-advertised host:RPC address. */
#include <sys/socket.h>
#include <netdb.h>
#include <sys/time.h>
#include <errno.h>

#define TSDB_METRICS_DEFAULT_PORT 28094

static int http_post_sql(const char *host, int port,
                          const char *q, size_t qlen,
                          char *body_buf, size_t body_cap)
{
    struct addrinfo hints = {0}, *ai = NULL;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);
    if (getaddrinfo(host, portstr, &hints, &ai) != 0) return -1;

    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) { freeaddrinfo(ai); return -1; }

    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
        close(fd); freeaddrinfo(ai); return -1;
    }
    freeaddrinfo(ai);

    /* Build body {"q":"…"} with minimal escaping for quote + backslash. */
    size_t esc_cap = qlen * 2 + 16;
    char *esc = malloc(esc_cap);
    if (!esc) { close(fd); return -1; }
    size_t ew = 0;
    esc[ew++] = '{'; esc[ew++] = '"'; esc[ew++] = 'q'; esc[ew++] = '"';
    esc[ew++] = ':'; esc[ew++] = '"';
    for (size_t i = 0; i < qlen && ew + 2 < esc_cap; i++) {
        char c = q[i];
        if (c == '"' || c == '\\') esc[ew++] = '\\';
        esc[ew++] = c;
    }
    if (ew + 2 >= esc_cap) { free(esc); close(fd); return -1; }
    esc[ew++] = '"'; esc[ew++] = '}';

    char hdr[512];
    int hlen = snprintf(hdr, sizeof(hdr),
        "POST /sql HTTP/1.1\r\nHost: %s\r\n"
        "Content-Type: application/json\r\nContent-Length: %zu\r\n"
        "Connection: close\r\n\r\n", host, ew);
    if (hlen <= 0 || send(fd, hdr, (size_t)hlen, 0) != hlen ||
        send(fd, esc, ew, 0) != (ssize_t)ew) {
        free(esc); close(fd); return -1;
    }
    free(esc);

    /* Read full HTTP response into a local buffer, then slice body. */
    size_t rcap = 64 * 1024, rlen = 0;
    char *raw = malloc(rcap);
    if (!raw) { close(fd); return -1; }
    for (;;) {
        if (rlen + 4096 > rcap) {
            size_t nc = rcap * 2;
            if (nc > 4 * 1024 * 1024) break;
            char *nr = realloc(raw, nc);
            if (!nr) break;
            raw = nr; rcap = nc;
        }
        ssize_t n = recv(fd, raw + rlen, rcap - rlen - 1, 0);
        if (n <= 0) break;
        rlen += (size_t)n;
    }
    close(fd);
    raw[rlen] = '\0';

    /* Locate body after "\r\n\r\n". */
    char *body = strstr(raw, "\r\n\r\n");
    if (!body) { free(raw); return -1; }
    body += 4;
    size_t blen = rlen - (size_t)(body - raw);
    if (blen >= body_cap) blen = body_cap - 1;
    memcpy(body_buf, body, blen);
    body_buf[blen] = '\0';
    free(raw);
    return (int)blen;
}

static int proxy_sql_to_master(tsdb_db_t *db,
                                const char *q, size_t qlen,
                                char *buf, size_t cap)
{
    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr_for_db(db);
    if (!mgr) return -1;

    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(mgr, snap, TSDB_CLUSTER_MAX_NODES);
    for (int i = 0; i < n; i++) {
        if (snap[i].role != TSDB_ROLE_MASTER) continue;
        if (snap[i].state == TSDB_NODE_DEAD) continue;

        /* addr is "host:RPC_PORT"; split on ':'. */
        char host[128];
        snprintf(host, sizeof(host), "%s", snap[i].addr);
        char *colon = strchr(host, ':');
        if (!colon) continue;
        *colon = '\0';

        int rc = http_post_sql(host, TSDB_METRICS_DEFAULT_PORT,
                                q, qlen, buf, cap);
        if (rc > 0) return rc;
    }
    return -1;
}

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Peek the first keyword (ASCII, case-insensitive) to decide whether a
 * query is a catalog DDL the data-node must forward to a master.  We
 * intentionally stay keyword-based — the full parse happens on the
 * master; the data node doesn't need a second parser. */
static int query_is_catalog_ddl(const char *q, size_t qlen) {
    size_t i = 0;
    while (i < qlen && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' ||
                        q[i] == '\r')) i++;
    const char *p = q + i;
    size_t left = qlen - i;
    static const char *kw[] = {
        "CREATE", "DROP", "ALTER", "ADD ", "REMOVE ", NULL
    };
    for (int k = 0; kw[k]; k++) {
        size_t kl = strlen(kw[k]);
        if (left >= kl && strncasecmp(p, kw[k], kl) == 0) return 1;
    }
    return 0;
}

static int sql_exec_cb(void *ud, const char *q, size_t qlen,
                       char *buf, size_t cap)
{
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return snprintf(buf, cap, "{\"error\":\"db not ready\"}");

    /* Data-node UX fix: when the dashboard lands on a node without a
     * Raft handle AND the statement is a catalog DDL, transparently
     * forward to a master so CREATE DB / GROUP / TABLE just work.
     * Clients still get the same JSON shape they would see if they
     * had hit the master directly. */
    if (!tsdb_db_raft_for_db(db) && query_is_catalog_ddl(q, qlen)) {
        int prc = proxy_sql_to_master(db, q, qlen, buf, cap);
        if (prc > 0) return prc;
        /* else fall through — local exec will surface the guard msg */
    }

    /* Ensure q is NUL-terminated; we copy because the HTTP parser
     * already left it NUL-terminated but belt-and-suspenders. */
    char *stmt = malloc(qlen + 1);
    if (!stmt) return snprintf(buf, cap, "{\"error\":\"oom\"}");
    memcpy(stmt, q, qlen);
    stmt[qlen] = '\0';

    int64_t t0 = now_ms();
    tsdb_result_t *res = NULL;
    int rc = tsdb_query(db, stmt, &res);
    if (rc != 0 || !res) {
        const char *e = tsdb_errstr(rc);
        char esc[512]; j_escape_str(esc, sizeof(esc), e ? e : "unknown error");
        int w = snprintf(buf, cap,
                         "{\"error\":\"%s\",\"ms\":%" PRId64 "}",
                         esc, now_ms() - t0);
        free(stmt);
        return w;
    }

    int ncols = tsdb_result_ncols(res);

    /* Header: "cols":[…],"types":[…]. */
    int w = 0;
    w += snprintf(buf + w, cap - (size_t)w, "{\"cols\":[");
    for (int c = 0; c < ncols && (size_t)w < cap - 64; c++) {
        const char *n = tsdb_result_col_name(res, c);
        char esc[256]; j_escape_str(esc, sizeof(esc), n ? n : "");
        w += snprintf(buf + w, cap - (size_t)w, "%s\"%s\"",
                      c > 0 ? "," : "", esc);
    }
    w += snprintf(buf + w, cap - (size_t)w, "],\"types\":[");
    for (int c = 0; c < ncols && (size_t)w < cap - 64; c++) {
        tsdb_type_t t = tsdb_result_col_type(res, c);
        w += snprintf(buf + w, cap - (size_t)w, "%s\"%s\"",
                      c > 0 ? "," : "", type_name(t));
    }

    /* Rows. */
    w += snprintf(buf + w, cap - (size_t)w, "],\"rows\":[");
    int nrows = 0, truncated = 0;
    while (tsdb_result_next(res) > 0) {
        if (nrows >= SQL_ROW_CAP) { truncated = 1; break; }
        /* Reserve at least 16 KiB per row before emitting; if we're too
         * close to the buffer end, stop and mark truncated. */
        if ((size_t)w > cap - 16 * 1024) { truncated = 1; break; }

        w += snprintf(buf + w, cap - (size_t)w, "%s[",
                      nrows > 0 ? "," : "");
        for (int c = 0; c < ncols; c++) {
            if (c > 0 && (size_t)w < cap)
                buf[w++] = ',';
            tsdb_type_t ty = tsdb_result_col_type(res, c);
            int add = write_json_cell(buf + w, cap - (size_t)w, res, c, ty);
            if (add > 0) w += add;
        }
        if ((size_t)w < cap) buf[w++] = ']';
        nrows++;
    }

    w += snprintf(buf + w, cap - (size_t)w,
                  "],\"nrows\":%d,\"truncated\":%s,\"ms\":%" PRId64 "}",
                  nrows, truncated ? "true" : "false", now_ms() - t0);

    tsdb_result_free(res);
    free(stmt);
    return w;
}

int main(int argc, char **argv) {
    const char *data_dir     = NULL;
    const char *rpc_addr     = NULL;
    const char *seeds        = NULL;
    const char *client_bind  = getenv("TSDB_BIND");
    const char *metrics_bind = getenv("TSDB_METRICS_BIND");

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--rpc") == 0 && i + 1 < argc) {
            rpc_addr = argv[++i];
        } else if (strcmp(argv[i], "--seeds") == 0 && i + 1 < argc) {
            seeds = argv[++i];
        } else if (strcmp(argv[i], "--bind") == 0 && i + 1 < argc) {
            client_bind = argv[++i];
        } else if (strcmp(argv[i], "--metrics-bind") == 0 && i + 1 < argc) {
            metrics_bind = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    if (!data_dir || !rpc_addr) {
        usage(argv[0]);
        return 1;
    }
    /* Sensible defaults so `docker run` with just seeds works. */
    if (!client_bind  || !*client_bind)  client_bind  = "0.0.0.0:28090";
    if (!metrics_bind || !*metrics_bind) metrics_bind = "0.0.0.0:28094";

    g_node_start_epoch = time(NULL);

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    /* TCP writes to a peer that closed their end must not kill us.
     * Without this, client-side disconnect during a write_batch causes
     * the handler thread to trap SIGPIPE → default action is terminate
     * the whole process → cluster node flaps.  Ignoring makes send()
     * return EPIPE/-1 which the RPC / wire-protocol layers surface as
     * an ordinary I/O error. */
    signal(SIGPIPE, SIG_IGN);

    tsdb_db_t *db = NULL;
    int rc = tsdb_open_cluster(data_dir, rpc_addr, seeds, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "tsdb_open_cluster failed: %s\n", tsdb_errstr(rc));
        return 1;
    }

    printf("[node] cluster opened  rpc=%s  data=%s\n", rpc_addr, data_dir);
    fflush(stdout);

    /* Wait for cluster to form. */
    tsdb_cluster_wait_ready(db, 1, 5000);

    char stats_buf[4096];
    tsdb_cluster_stats(db, stats_buf, sizeof(stats_buf));
    printf("[node] cluster view: %s\n", stats_buf);
    fflush(stdout);

    /* Start the wire-protocol client server so external clients (Go SDK,
     * JDBC, CLI) can reach this node the same way they reach tsdb-server. */
    tsdb_metrics_init();
    tsdb_server_opts_t sopts = {
        .bind_addr    = client_bind,
        .max_conns    = 1024,
        .db           = db,
    };
    tsdb_server_t *srv = NULL;
    rc = tsdb_server_start(&sopts, &srv);
    if (rc != TSDB_OK) {
        fprintf(stderr, "tsdb_server_start(%s) failed: %s\n",
                client_bind, tsdb_errstr(rc));
        tsdb_close(db);
        return 1;
    }
    printf("[node] client bind=%s\n", client_bind);

    /* /metrics + /health + dashboard + /cluster — latter wired to the
     * live cluster view via the provider callback.  Stash the data_dir
     * for the callback so it can report local disk capacity. */
    tsdb_metrics_server_t *ms = NULL;
    tsdb_metrics_server_set_data_dir(data_dir);
    snprintf(g_local_data_dir, sizeof(g_local_data_dir), "%s", data_dir);
    /* TSDB_NODE_ROLE — mirror the same env var db_cluster.c reads, so
     * /tree can advertise this node's role without reaching into the
     * cluster struct.  Accept "data" / "dnode" as aliases. */
    {
        const char *r = getenv("TSDB_NODE_ROLE");
        if (r && (!strcasecmp(r, "data") || !strcasecmp(r, "dnode")))
            snprintf(g_local_role, sizeof(g_local_role), "data");
        else
            snprintf(g_local_role, sizeof(g_local_role), "master");
    }
    tsdb_metrics_server_set_cluster_provider(cluster_json_cb, db);
    tsdb_metrics_server_set_tree_provider(tree_json_cb, db);
    tsdb_metrics_server_set_sql_provider(sql_exec_cb, db);

    /* Optional Raft consensus for the master set.  Gate:
     *   TSDB_NODE_ROLE == master (default)   AND
     *   TSDB_CONSENSUS == raft   (explicit opt-in; fanout stays the
     *                             default until the implementation is
     *                             exercised under the acceptance suite). */
    tsdb_raft_t *raft_h = NULL;
    {
        const char *cons = getenv("TSDB_CONSENSUS");
        int want_raft = cons && !strcasecmp(cons, "raft");
        if (want_raft && !strcmp(g_local_role, "master")) {
            tsdb_node_manager_t *mgr  = tsdb_cluster_node_mgr_for_db(db);
            tsdb_replica_mgr_t  *rmgr = tsdb_cluster_replica_mgr_for_db(db);
            uint64_t self_id          = tsdb_cluster_local_id_for_db(db);
            if (mgr && rmgr && self_id) {
                raft_h = tsdb_raft_open(data_dir, self_id, mgr, rmgr,
                                         raft_apply_cb, db);
                if (raft_h) {
                    tsdb_db_bind_raft(db, raft_h);
                    tsdb_raft_set_snapshot_handlers(raft_h,
                                                     raft_snapshot_write_cb,
                                                     raft_snapshot_restore_cb,
                                                     db);
                    tsdb_metrics_server_set_raft_provider(raft_json_cb,
                                                           raft_h);
                    printf("[node] raft consensus ENABLED (data=%s/raft)\n",
                           data_dir);
                } else {
                    fprintf(stderr,
                            "[node] raft open failed — falling back to fanout\n");
                }
            }
        }
    }

    rc = tsdb_metrics_server_start(metrics_bind, &ms);
    if (rc == 0 && ms) {
        printf("[node] metrics  bind=%s\n", metrics_bind);
    } else {
        fprintf(stderr, "[node] metrics server start(%s) failed\n", metrics_bind);
    }

    /* Influx Line Protocol ingest on a separate port — turning this on
     * for cluster nodes lets benchmarks & perf tests push rows over a
     * widely-supported HTTP endpoint instead of the bespoke wire
     * protocol.  Convention: TSDB_INFLUX_BIND, default 0.0.0.0:28092. */
    {
        const char *influx_bind = getenv("TSDB_INFLUX_BIND");
        if (!influx_bind || !*influx_bind) influx_bind = "0.0.0.0:28092";
        if (tsdb_influx_http_start(influx_bind, db) == 0) {
            printf("[node] influx  bind=%s\n", influx_bind);
        } else {
            fprintf(stderr, "[node] influx start(%s) failed\n", influx_bind);
        }
    }

    /* Anti-entropy catch-up: a node that was down during writes
     * rejoins with a stale local state.  Ask each peer for every
     * known table's (count, max_ts), and pull any rows we're
     * missing.  Runs on a short-lived background thread started
     * after cluster-ready so the main loop stays responsive; off
     * by default during the first 5 s so gossip can converge. */
    {
        pthread_t resync_thr;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        extern void *tsdb_resync_startup_thread(void *ud);
        if (pthread_create(&resync_thr, &attr,
                           tsdb_resync_startup_thread, db) == 0) {
            printf("[node] anti-entropy catch-up armed\n");
        }
        pthread_attr_destroy(&attr);
    }
    fflush(stdout);

    /* Main loop: optionally print stats every 5s (gated by env). */
    int verbose = getenv("TSDB_VERBOSE") != NULL;
    int tick = 0;
    while (g_running) {
        sleep(1);
        if (verbose && ++tick % 5 == 0) {
            tsdb_cluster_stats(db, stats_buf, sizeof(stats_buf));
            printf("[node] alive=%d  %s\n",
                   tsdb_cluster_alive_count(db), stats_buf);
            fflush(stdout);
        }
    }

    printf("[node] shutting down...\n");
    if (ms)  tsdb_metrics_server_stop(ms);
    if (srv) tsdb_server_stop(srv);
    tsdb_close(db);
    printf("[node] done.\n");
    return 0;
}
