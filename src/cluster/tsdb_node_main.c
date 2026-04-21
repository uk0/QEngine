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
static char g_local_data_dir[4096] = {0};

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

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int sql_exec_cb(void *ud, const char *q, size_t qlen,
                       char *buf, size_t cap)
{
    tsdb_db_t *db = (tsdb_db_t *)ud;
    if (!db) return snprintf(buf, cap, "{\"error\":\"db not ready\"}");

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
    rc = tsdb_metrics_server_start(metrics_bind, &ms);
    if (rc == 0 && ms) {
        printf("[node] metrics  bind=%s\n", metrics_bind);
    } else {
        fprintf(stderr, "[node] metrics server start(%s) failed\n", metrics_bind);
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
