/* metrics_server.c — Minimal HTTP/1.1 server for GET /metrics.
 *
 * Threading model: one accept thread + per-connection threads (detached).
 * Each connection:
 *   1. Reads until "\r\n\r\n" (end of HTTP headers).
 *   2. If "GET /metrics", render and send 200 OK.
 *   3. Otherwise send 404.
 *   4. Close connection (Connection: close).
 *
 * No keep-alive, no chunked encoding — simple enough for Prometheus scrape.
 */

#define _POSIX_C_SOURCE 200809L

#include "metrics_server.h"
#include "metrics.h"
#include "../cluster/disk_weight.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <poll.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

/* Process start time — set on first tsdb_metrics_server_start call.
 * Used by /health to report uptime in seconds. */
static time_t g_start_epoch = 0;
static pthread_once_t g_once = PTHREAD_ONCE_INIT;
static void record_start(void) { g_start_epoch = time(NULL); }

/* /cluster provider registry (see header).  Atomic-store via pointer
 * writes, which are word-sized on every platform we target. */
static tsdb_cluster_json_fn g_cluster_fn  = NULL;
static void                *g_cluster_ud  = NULL;
static tsdb_tree_json_fn    g_tree_fn     = NULL;
static void                *g_tree_ud     = NULL;
static tsdb_sql_exec_fn     g_sql_fn      = NULL;
static void                *g_sql_ud      = NULL;
static char                 g_data_dir[4096] = {0};

void tsdb_metrics_server_set_cluster_provider(tsdb_cluster_json_fn fn,
                                               void *userdata) {
    g_cluster_ud = userdata;
    g_cluster_fn = fn;
}

void tsdb_metrics_server_set_tree_provider(tsdb_tree_json_fn fn,
                                            void *userdata) {
    g_tree_ud = userdata;
    g_tree_fn = fn;
}

void tsdb_metrics_server_set_sql_provider(tsdb_sql_exec_fn fn,
                                           void *userdata) {
    g_sql_ud = userdata;
    g_sql_fn = fn;
}

void tsdb_metrics_server_set_data_dir(const char *path) {
    if (!path) { g_data_dir[0] = '\0'; return; }
    snprintf(g_data_dir, sizeof(g_data_dir), "%s", path);
}

/* Build the default synthetic /cluster JSON used when no provider is
 * registered.  Reports the single local node with disk capacity. */
static int build_standalone_cluster_json(char *buf, size_t cap) {
    long uptime = (long)(time(NULL) - g_start_epoch);
    if (uptime < 0) uptime = 0;
    uint64_t total = 0, free_b = 0;
    int vn = 0;
    if (g_data_dir[0]) {
        vn = tsdb_disk_weight_detail(g_data_dir,
                                      TSDB_DISK_WEIGHT_DEFAULT_PER_TB,
                                      &total, &free_b);
    }
    int used_x10 = 0;
    if (total > 0 && total >= free_b)
        used_x10 = (int)(((total - free_b) * 1000ULL) / total);
    char host[128] = "self";
    gethostname(host, sizeof(host) - 1);
    host[sizeof(host) - 1] = '\0';
    return snprintf(buf, cap,
        "{\"mode\":\"standalone\","
        "\"local_id\":\"0\","
        "\"nodes\":[{"
            "\"id\":\"0\","
            "\"addr\":\"%s\","
            "\"state\":\"ALIVE\","
            "\"ver\":0,"
            "\"hb_age_ms\":0,"
            "\"known_for_s\":%ld,"
            "\"suspect_count\":0"
        "}],"
        "\"local\":{"
            "\"host\":\"%s\","
            "\"pid\":%d,"
            "\"uptime_s\":%ld,"
            "\"disk\":{\"total_bytes\":%llu,\"free_bytes\":%llu,"
                      "\"used_x10\":%d,\"data_dir\":\"%s\",\"vn_weight\":%d}"
        "}}\n",
        host, uptime,
        host, (int)getpid(), uptime,
        (unsigned long long)total, (unsigned long long)free_b,
        used_x10, g_data_dir, vn);
}

/* ---- Server struct -------------------------------------------------------- */

struct tsdb_metrics_server {
    int          listen_fd;
    int          port;
    volatile int running;
    pthread_t    accept_thread;
};

/* ---- Connection handler -------------------------------------------------- */

static void write_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, buf + sent, len - sent);
        if (n <= 0) break;
        sent += (size_t)n;
    }
}

/* Parse Content-Length from a buffer containing HTTP headers terminated
 * by CRLFCRLF / LFLF.  Returns -1 if absent or malformed. */
static long parse_content_length(const char *hdrs, size_t hdr_len) {
    const char *key = "Content-Length:";
    size_t klen = strlen(key);
    /* Case-insensitive match at start of a line. */
    for (size_t i = 0; i + klen < hdr_len; i++) {
        if (i > 0 && hdrs[i-1] != '\n') continue;
        if (strncasecmp(hdrs + i, key, klen) != 0) continue;
        const char *v = hdrs + i + klen;
        while (*v == ' ' || *v == '\t') v++;
        char *end = NULL;
        long x = strtol(v, &end, 10);
        if (end != v && x >= 0) return x;
        return -1;
    }
    return -1;
}

static void *handle_connection(void *arg) {
    int fd = (int)(intptr_t)arg;

    /* 15-second read + write deadline — SQL queries (including server-side
     * execution) can exceed the former 5 s.  Pre-dashboard era was 5s;
     * that's fine for Prometheus scraping but cuts off slower /sql calls. */
    struct timeval tv = { .tv_sec = 15, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    /* Read request headers + up to first chunk of body into a heap buffer.
     * SQL POST bodies can be tens of KB; headers are bounded to 8 KB. */
    size_t  cap = 16 * 1024;
    char   *req = malloc(cap);
    if (!req) { close(fd); return NULL; }
    size_t pos = 0;
    int    got_end = 0;
    size_t hdr_end = 0;

    while (pos < cap - 1) {
        ssize_t n = read(fd, req + pos, cap - 1 - pos);
        if (n <= 0) break;
        pos += (size_t)n;
        req[pos] = '\0';
        char *m = strstr(req, "\r\n\r\n");
        if (m)      { hdr_end = (size_t)(m - req) + 4; got_end = 1; break; }
        m = strstr(req, "\n\n");
        if (m)      { hdr_end = (size_t)(m - req) + 2; got_end = 1; break; }
    }

    if (!got_end) {
        free(req);
        close(fd);
        return NULL;
    }

    /* If Content-Length indicates a body longer than what we already read,
     * keep reading.  Cap body at 1 MiB — /sql queries should never exceed. */
    long clen = parse_content_length(req, hdr_end);
    if (clen > 0) {
        const size_t BODY_CAP = 1024 * 1024;
        size_t want = hdr_end + (size_t)clen;
        if (want > BODY_CAP + hdr_end) want = BODY_CAP + hdr_end;
        if (want > cap) {
            size_t ncap = want + 1;
            char *nb = realloc(req, ncap);
            if (!nb) { free(req); close(fd); return NULL; }
            req = nb; cap = ncap;
        }
        while (pos < want) {
            ssize_t n = read(fd, req + pos, want - pos);
            if (n <= 0) break;
            pos += (size_t)n;
        }
        req[pos < cap ? pos : cap - 1] = '\0';
    }

    const char *body     = req + hdr_end;
    size_t      body_len = (pos > hdr_end) ? (pos - hdr_end) : 0;

    /* Parse first line: "METHOD <path> HTTP/1.x". */
    int route_metrics = 0;
    int route_health  = 0;
    int route_dash    = 0;
    int route_cluster = 0;
    int route_tree    = 0;
    int route_sql     = 0;
    if (strncmp(req, "GET /metrics", 12) == 0)           route_metrics = 1;
    else if (strncmp(req, "GET /health", 11) == 0)       route_health  = 1;
    else if (strncmp(req, "GET /cluster", 12) == 0)      route_cluster = 1;
    else if (strncmp(req, "GET /tree", 9) == 0)          route_tree    = 1;
    else if (strncmp(req, "POST /sql", 9) == 0 ||
             strncmp(req, "GET /sql",  8) == 0)          route_sql     = 1;
    else if (strncmp(req, "GET / ", 6) == 0 ||
             strncmp(req, "GET /index", 10) == 0)        route_dash    = 1;

    if (route_metrics) {
        size_t body_len = 0;
        char *body = tsdb_metrics_render(&body_len);
        if (!body) {
            const char *err =
                "HTTP/1.1 500 Internal Server Error\r\n"
                "Content-Length: 0\r\n"
                "Connection: close\r\n\r\n";
            write_all(fd, err, strlen(err));
        } else {
            char hdr[256];
            int hlen = snprintf(hdr, sizeof(hdr),
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                "Content-Length: %zu\r\n"
                "Connection: close\r\n\r\n",
                body_len);
            write_all(fd, hdr, (size_t)hlen);
            write_all(fd, body, body_len);
            free(body);
        }
    } else if (route_cluster) {
        /* Cluster topology.  If the cluster module has registered a
         * provider we defer to it (full member list + autobalance);
         * otherwise we synthesise a single-node JSON so the dashboard
         * has something consistent to render. */
        char body_c[8192];
        int blen = 0;
        if (g_cluster_fn) blen = g_cluster_fn(g_cluster_ud, body_c, sizeof(body_c));
        if (blen <= 0)    blen = build_standalone_cluster_json(body_c, sizeof(body_c));
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n\r\n",
            blen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body_c, (size_t)blen);
    } else if (route_tree) {
        /* Catalog tree for the dashboard left-hand navigator. */
        char body_t[64 * 1024];
        int blen = 0;
        if (g_tree_fn) blen = g_tree_fn(g_tree_ud, body_t, sizeof(body_t));
        if (blen <= 0) {
            blen = snprintf(body_t, sizeof(body_t),
                            "{\"db\":\"%s\",\"tables\":[],"
                            "\"note\":\"tree provider not installed\"}\n",
                            g_data_dir);
        }
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n\r\n",
            blen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body_t, (size_t)blen);
    } else if (route_sql) {
        /* Execute a SQL / QTL statement.
         *
         * POST body is JSON: {"q":"<sql>"}; we extract the q value with a
         * forgiving parser (no nested JSON, no escaped quotes in q is
         * expected — the dashboard always sends a clean plaintext
         * query).  GET fallback accepts ?q=<url-encoded sql> so it's
         * curl-friendly. */
        char *q_start = NULL;
        size_t q_len = 0;
        char  *q_copy = NULL;

        if (strncmp(req, "POST", 4) == 0 && body_len > 0) {
            /* Look for "q":" pattern. */
            const char *p = strstr(body, "\"q\"");
            if (!p) p = strstr(body, "'q'");
            if (p) {
                p = strchr(p, ':');
                if (p) { p++;
                    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
                    if (*p == '"' || *p == '\'') {
                        char quote = *p++;
                        const char *e = p;
                        while (*e && *e != quote) {
                            if (*e == '\\' && e[1]) e += 2;
                            else e++;
                        }
                        size_t n = (size_t)(e - p);
                        q_copy = malloc(n + 1);
                        if (q_copy) {
                            /* Unescape \", \\, \n, \r, \t only — enough for
                             * the dashboard's client-side JSON encoder. */
                            size_t w = 0;
                            for (size_t r = 0; r < n; r++) {
                                char c = p[r];
                                if (c == '\\' && r + 1 < n) {
                                    char nx = p[++r];
                                    if      (nx == 'n') c = '\n';
                                    else if (nx == 'r') c = '\r';
                                    else if (nx == 't') c = '\t';
                                    else                c = nx;
                                }
                                q_copy[w++] = c;
                            }
                            q_copy[w] = '\0';
                            q_start = q_copy;
                            q_len   = w;
                        }
                    }
                }
            }
        } else {
            /* GET /sql?q=... — find '?q=' in the request line. */
            const char *p = strstr(req, "?q=");
            if (p) {
                p += 3;
                const char *e = p;
                while (*e && *e != ' ' && *e != '&' && *e != '\r' && *e != '\n') e++;
                size_t n = (size_t)(e - p);
                q_copy = malloc(n + 1);
                if (q_copy) {
                    /* Minimal URL decode (%xx + '+'). */
                    size_t w = 0;
                    for (size_t r = 0; r < n; r++) {
                        char c = p[r];
                        if (c == '+') c = ' ';
                        else if (c == '%' && r + 2 < n) {
                            char h1 = p[++r], h2 = p[++r];
                            int v1 = (h1 <= '9') ? h1 - '0'
                                   : (h1 <= 'F') ? h1 - 'A' + 10
                                                 : h1 - 'a' + 10;
                            int v2 = (h2 <= '9') ? h2 - '0'
                                   : (h2 <= 'F') ? h2 - 'A' + 10
                                                 : h2 - 'a' + 10;
                            c = (char)((v1 << 4) | v2);
                        }
                        q_copy[w++] = c;
                    }
                    q_copy[w] = '\0';
                    q_start = q_copy;
                    q_len   = w;
                }
            }
        }

        /* Allocate result body buffer.  Upper bound: 4 MiB — more than
         * enough for 1000 rows × ~50 cols × 80 chars. */
        const size_t RES_CAP = 4 * 1024 * 1024;
        char *res = malloc(RES_CAP);
        int   rlen = 0;
        if (!res) {
            const char *oom = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            write_all(fd, oom, strlen(oom));
            free(q_copy);
            goto done;
        }
        if (!q_start || q_len == 0) {
            rlen = snprintf(res, RES_CAP,
                            "{\"error\":\"empty query\"}\n");
        } else if (!g_sql_fn) {
            rlen = snprintf(res, RES_CAP,
                            "{\"error\":\"sql provider not installed\"}\n");
        } else {
            rlen = g_sql_fn(g_sql_ud, q_start, q_len, res, RES_CAP);
            if (rlen <= 0)
                rlen = snprintf(res, RES_CAP,
                                "{\"error\":\"provider returned %d\"}\n", rlen);
        }
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n\r\n",
            rlen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, res, (size_t)rlen);
        free(res);
        free(q_copy);
    } else if (route_health) {
        /* Minimal liveness payload — kept JSON-only so k8s / curl
         * integrations parse it trivially.  Uptime is seconds since the
         * first metrics_server_start call this process. */
        time_t now = time(NULL);
        long uptime = (long)(now - g_start_epoch);
        if (uptime < 0) uptime = 0;
        char body[256];
        int blen = snprintf(body, sizeof(body),
            "{\"status\":\"ok\",\"uptime_s\":%ld,\"pid\":%d}\n",
            uptime, (int)getpid());
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n\r\n",
            blen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body, (size_t)blen);
    } else if (route_dash) {
        /* Single-page HTML dashboard.  No external assets; fetches
         * /health + /metrics from the same origin via XHR every 2 s.
         * Renders: (1) cumulative stat cards, (2) derived-rate cards,
         * (3) SVG sparklines for queries/s and rows/s, (4) an events
         * log for auth + flush events, (5) server-identification panel. */
        static const char DASH[] =
"<!DOCTYPE html><html lang=en><head><meta charset=utf-8>"
"<meta name=viewport content=\"width=device-width,initial-scale=1\">"
"<title>QEngine — health</title>"
"<style>"
":root{--fg:#0f172a;--mu:#64748b;--bd:#e2e8f0;--bg:#f8fafc;--card:#fff;"
"--ok:#22c55e;--bad:#ef4444;--warn:#f59e0b;--accent:#2563eb}"
"*{box-sizing:border-box}"
"body{font:14px/1.4 -apple-system,system-ui,sans-serif;color:var(--fg);"
"background:var(--bg);margin:0;padding:24px;max-width:1400px;margin:auto}"
"header{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin-bottom:4px}"
"h1{font-size:20px;margin:0}"
".sub{color:var(--mu);font-size:12px}"
".badge{display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;"
"color:#fff;background:var(--ok);font-weight:500;letter-spacing:.02em}"
".badge.bad{background:var(--bad)}.badge.warn{background:var(--warn)}"
".badge.mode{background:#4338ca;font-weight:600;letter-spacing:.05em}"
".badge.mode.standalone{background:#64748b}"
"section{margin-top:20px}"
".secttl{font-size:11px;color:var(--mu);text-transform:uppercase;letter-spacing:.08em;"
"margin-bottom:8px;font-weight:600}"
".grid{display:grid;gap:10px}"
".grid.cards{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}"
".grid.wide{grid-template-columns:repeat(auto-fit,minmax(340px,1fr))}"
".card{background:var(--card);border:1px solid var(--bd);border-radius:8px;padding:12px 14px}"
".k{color:var(--mu);font-size:10px;text-transform:uppercase;letter-spacing:.05em}"
".v{font-size:22px;font-weight:600;margin-top:2px;font-variant-numeric:tabular-nums}"
".vs{color:var(--mu);font-size:11px;margin-top:2px;font-variant-numeric:tabular-nums}"
".spark{display:block;width:100%;height:60px;margin-top:8px}"
".spark path{fill:none;stroke:var(--accent);stroke-width:1.5}"
".spark .area{fill:var(--accent);fill-opacity:.10;stroke:none}"
".idtbl{width:100%;font-size:12px;border-collapse:collapse}"
".idtbl td{padding:4px 0;color:var(--mu)}"
".idtbl td.val{color:var(--fg);font-variant-numeric:tabular-nums;text-align:right}"
".idtbl thead td{color:var(--mu);font-size:10px;text-transform:uppercase;"
"letter-spacing:.05em;font-weight:600}"
".idtbl tbody td{padding:4px 8px;border-top:1px solid var(--bd)}"
".pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:10px;font-weight:500}"
".pill.alive{background:#dcfce7;color:#166534}"
".pill.suspect{background:#fef3c7;color:#92400e}"
".pill.dead{background:#fee2e2;color:#991b1b}"
".pill.joining{background:#dbeafe;color:#1e40af}"
".pill.ok{background:#dcfce7;color:#166534}"
".pill.warn{background:#fef3c7;color:#92400e}"
".pill.bad{background:#fee2e2;color:#991b1b}"
".bar{display:inline-block;height:6px;width:60px;vertical-align:middle;"
"background:#e2e8f0;border-radius:3px;overflow:hidden;margin-right:6px}"
".bar>span{display:block;height:100%;background:var(--accent)}"
".bar.warn>span{background:var(--warn)}.bar.bad>span{background:var(--bad)}"
/* SQL console + tree */
".qshell{display:grid;grid-template-columns:240px 1fr;gap:10px;align-items:stretch}"
".tree{background:var(--card);border:1px solid var(--bd);border-radius:8px;"
"padding:8px 10px;min-height:300px;max-height:480px;overflow:auto;font-size:12px}"
".tree .grp{font-weight:600;color:var(--mu);text-transform:uppercase;"
"letter-spacing:.06em;font-size:10px;margin-top:8px;margin-bottom:4px}"
".tree .grp:first-child{margin-top:0}"
".tree .item{padding:3px 6px;border-radius:4px;cursor:pointer;"
"white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
".tree .item:hover{background:#f1f5f9}"
".tree .item.active{background:#dbeafe;color:#1e3a8a}"
".tree .empty{color:var(--mu);font-style:italic;padding:3px 6px}"
".qpanel{display:flex;flex-direction:column;gap:8px}"
".qeditor{background:#0f172a;color:#e2e8f0;border:1px solid var(--bd);"
"border-radius:8px;padding:10px 12px;font-family:ui-monospace,SFMono-Regular,"
"Menlo,monospace;font-size:13px;line-height:1.45;min-height:140px;"
"resize:vertical;width:100%}"
".qbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}"
".qbtn{background:var(--accent);color:#fff;border:none;border-radius:6px;"
"padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer}"
".qbtn:hover{background:#1d4ed8}.qbtn:disabled{opacity:.5;cursor:wait}"
".qbtn.alt{background:#f1f5f9;color:var(--fg)}"
".qbtn.alt:hover{background:#e2e8f0}"
".qstatus{font-size:11px;color:var(--mu);font-variant-numeric:tabular-nums}"
".qresult{background:var(--card);border:1px solid var(--bd);border-radius:8px;"
"overflow:auto;max-height:420px}"
".qresult table{width:100%;border-collapse:collapse;font-size:12px}"
".qresult th,.qresult td{padding:4px 10px;text-align:left;"
"border-bottom:1px solid var(--bd);white-space:nowrap}"
".qresult th{background:#f8fafc;position:sticky;top:0;font-weight:600;"
"font-size:10px;text-transform:uppercase;letter-spacing:.04em;color:var(--mu)}"
".qresult th .ty{font-weight:400;color:#94a3b8;margin-left:4px}"
".qresult td.num{text-align:right;font-variant-numeric:tabular-nums}"
".qresult td.null{color:#94a3b8;font-style:italic}"
".qresult .err{padding:14px;color:var(--bad);font-family:ui-monospace,"
"SFMono-Regular,Menlo,monospace;font-size:12px}"
".log{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;"
"max-height:240px;overflow:auto}"
".log .row{display:flex;gap:8px;padding:4px 0;border-bottom:1px dotted var(--bd)}"
".log .t{color:var(--mu);white-space:nowrap}"
".log .m{flex:1}"
".log .tag{padding:0 6px;border-radius:4px;font-size:10px;text-transform:uppercase}"
".log .tag.auth{background:#dbeafe;color:#1e40af}"
".log .tag.deny{background:#fee2e2;color:#991b1b}"
".log .tag.flush{background:#dcfce7;color:#166534}"
".log .tag.err{background:#fef3c7;color:#92400e}"
"footer{color:var(--mu);font-size:11px;margin-top:28px;padding-top:12px;"
"border-top:1px solid var(--bd)}"
"</style></head><body>"
"<header>"
"<h1>QEngine</h1>"
"<span id=modebadge class=\"badge mode\">…</span>"
"<span id=st class=badge>loading</span>"
"<span class=sub id=host></span>"
"</header>"
"<div class=sub id=uptime></div>"

"<section><div class=secttl>Realtime rates</div>"
"<div class=\"grid wide\">"
"<div class=card>"
"<div class=k>Queries / sec</div>"
"<div class=v id=rq>—</div>"
"<div class=vs id=rqmeta>p50 - · p99 -</div>"
"<svg class=spark id=sq viewBox=\"0 0 100 20\" preserveAspectRatio=none></svg>"
"</div>"
"<div class=card>"
"<div class=k>Rows written / sec</div>"
"<div class=v id=rw>—</div>"
"<div class=vs id=rwmeta>avg batch - rows</div>"
"<svg class=spark id=sw viewBox=\"0 0 100 20\" preserveAspectRatio=none></svg>"
"</div>"
"</div></section>"

"<section><div class=secttl>Performance</div>"
"<div class=\"grid cards\">"
"<div class=card><div class=k>Memtable rows</div><div class=v id=pm>-</div></div>"
"<div class=card><div class=k>On-disk bytes</div><div class=v id=pd>-</div></div>"
"<div class=card><div class=k>Flushes / min</div><div class=v id=pf>-</div></div>"
"<div class=card><div class=k>Compactions / min</div><div class=v id=pc>-</div></div>"
"<div class=card><div class=k>Bloom skip rate</div><div class=v id=pb>-</div></div>"
"<div class=card><div class=k>Cluster nodes alive</div><div class=v id=pa>-</div></div>"
"</div></section>"

"<section><div class=secttl>Counters</div>"
"<div class=\"grid cards\" id=g></div></section>"

"<section><div class=secttl>Cluster topology</div>"
"<div class=card>"
"<div class=sub id=cmode style=\"margin-bottom:6px\">-</div>"
"<table class=idtbl id=ctbl><thead>"
"<tr><td>id</td><td>addr</td><td>state</td><td>disk free/total</td>"
"<td class=val>used</td><td class=val>vn</td>"
"<td class=val>uptime</td><td class=val>last hb</td></tr></thead>"
"<tbody id=cbody></tbody></table></div></section>"

"<section><div class=secttl>Query console · Ctrl+Enter to run · double-click a table on the left</div>"
"<div class=qshell>"
"<div class=tree id=tree><div class=empty>loading…</div></div>"
"<div class=qpanel>"
"<textarea class=qeditor id=qed placeholder=\"SELECT * FROM ... LIMIT 100\" spellcheck=false>"
"SELECT 1</textarea>"
"<div class=qbar>"
"<button class=qbtn id=qrun>Run</button>"
"<button class=\"qbtn alt\" id=qreload>Reload tree</button>"
"<span class=qstatus id=qstatus>ready</span>"
"</div>"
"<div class=qresult id=qres></div>"
"</div>"
"</div></section>"

"<section><div class=secttl>Events (last 50)</div>"
"<div class=card><div class=log id=log></div></div></section>"

"<section><div class=secttl>Server identification</div>"
"<div class=card>"
"<table class=idtbl id=idt><tr><td>host</td><td class=val id=i_host>-</td></tr>"
"<tr><td>pid</td><td class=val id=i_pid>-</td></tr>"
"<tr><td>uptime</td><td class=val id=i_up>-</td></tr>"
"<tr><td>crc32c impl</td><td class=val>hardware-dispatched</td></tr>"
"</table></div></section>"

"<footer>polling /health + /metrics every 2s · last refresh "
"<span id=t>-</span></footer>"

"<script>"
"const CARDS=["
"['qengine_connections_active','Active conns',''],"
"['qengine_queries_total','Queries total',''],"
"['qengine_rows_written_total','Rows written',''],"
"['qengine_bytes_written_total','Bytes written','B'],"
"['qengine_query_errors_total','Query errors',''],"
"['qengine_flushes_total','Flushes',''],"
"['qengine_bloom_skips_total','Bloom skips',''],"
"['qengine_auth_logins_total','Auth logins',''],"
"['qengine_auth_denied_total','Auth denied',''],"
"['qengine_connections_total','Conns lifetime',''],"
"['qengine_replicate_sent_total','Replicate sent',''],"
"['qengine_replicate_ack_total','Replicate ack',''],"
"['qengine_replicate_fail_total','Replicate fail',''],"
"['qengine_replica_dial_total','Peer conn dials','']"
"];"
/* Keys we care about — everything else in /metrics can be skipped
 * during parsing.  Cheaper than parseFloat-ing hundreds of histogram
 * buckets every 2s on a busy cluster. */
"const WANT=new Set(["
"'qengine_connections_active','qengine_queries_total','qengine_rows_written_total',"
"'qengine_bytes_written_total','qengine_query_errors_total','qengine_flushes_total',"
"'qengine_compactions_total','qengine_bloom_skips_total','qengine_bloom_lookups_total',"
"'qengine_auth_logins_total','qengine_auth_denied_total','qengine_connections_total',"
"'qengine_replicate_sent_total','qengine_replicate_ack_total','qengine_replicate_fail_total',"
"'qengine_replica_dial_total','qengine_memtable_rows','qengine_disk_bytes',"
"'qengine_cluster_nodes_alive','qengine_ingest_batch_size_count','qengine_ingest_batch_size_sum'"
"]);"
/* We also consume query_duration_ms histogram buckets + count/sum.
 * Keep those by a prefix rule so we can compute p50/p99 below. */
"const WANT_PFX=['qengine_query_duration_ms'];"
"const QD_BOUNDS=[0.5,1,5,10,50,100,500,1000,Infinity];"
"const MAXPTS=60;"
"const state={qHist:[],wHist:[],prev:null,events:[],lastCBody:'',lastCKey:''};"
"function fmt(v,u){if(v>=1e9)return (v/1e9).toFixed(1)+'G'+u;"
"if(v>=1e6)return (v/1e6).toFixed(1)+'M'+u;"
"if(v>=1e3)return (v/1e3).toFixed(1)+'k'+u;"
"return (Math.round(v*10)/10)+u;}"
"function fmtSec(s){s=s|0;if(s<60)return s+'s';"
"if(s<3600)return Math.floor(s/60)+'m '+(s%60)+'s';"
"const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h+'h '+m+'m';}"
"function fmtAge(ms){if(ms==null)return '-';"
"if(ms<1000)return ms+'ms';return fmtSec(Math.floor(ms/1000));}"
"function fmtBytes(b){if(!b)return '-';const u=['B','K','M','G','T','P'];"
"let i=0,v=b;while(v>=1024&&i<u.length-1){v/=1024;i++;}"
"return (Math.round(v*10)/10)+u[i];}"
"function hbClass(ms){if(ms==null)return '';"
"if(ms<3000)return 'ok';if(ms<10000)return 'warn';return 'bad';}"
"function diskClass(x10){if(x10==null)return '';"
"if(x10<800)return '';if(x10<950)return 'warn';return 'bad';}"
"function spark(id,pts){const el=document.getElementById(id);"
"if(pts.length<2){el.innerHTML='';return;}"
"let max=0;for(let i=0;i<pts.length;i++)if(pts[i]>max)max=pts[i];"
"if(max<1)max=1;const n=pts.length;let d='';"
"for(let i=0;i<n;i++){const x=(i/(n-1))*100,y=20-(pts[i]/max)*18-1;"
"d+=(i?'L':'M')+x.toFixed(1)+' '+y.toFixed(1)+' ';}"
"el.innerHTML='<path class=area d=\"'+d+'L100 20 L0 20 Z\"/><path d=\"'+d+'\"/>';}"
"function pushEv(tag,txt){"
"const t=new Date().toLocaleTimeString();"
"state.events.unshift({t,tag,txt});"
"if(state.events.length>50)state.events.length=50;"
"document.getElementById('log').innerHTML=state.events.map(e=>"
"`<div class=row><span class=t>${e.t}</span>"
"<span class=\"tag ${e.tag}\">${e.tag}</span>"
"<span class=m>${e.txt}</span></div>`).join('');}"
/* Streaming-style parser: newline split + indexOf, skip '#' and keys
 * not in WANT/WANT_PFX.  Avoids building an intermediate 300-entry map. */
"function parseMetrics(text){const m={};const lines=text.split('\\n');"
"for(let i=0;i<lines.length;i++){const l=lines[i];"
"if(!l||l.charCodeAt(0)===35)continue;"
"const p=l.indexOf(' ');if(p<0)continue;"
"const k=l.substring(0,p);const bc=k.indexOf('{');"
"const base=bc>=0?k.substring(0,bc):k;"
"if(!WANT.has(base)){let ok=false;"
"for(let j=0;j<WANT_PFX.length;j++)if(base.indexOf(WANT_PFX[j])===0){ok=true;break;}"
"if(!ok)continue;}"
"const v=+l.substring(p+1);if(!Number.isNaN(v))m[k]=v;}return m;}"
/* p50/p99 from Prometheus-style cumulative histogram buckets.
 * m has keys like qengine_query_duration_ms_bucket{le=\"5\"} etc. */
"function quantile(m,pfx,q){const cnt=m[pfx+'_count']||0;"
"if(cnt<=0)return null;const target=cnt*q;"
"for(let i=0;i<QD_BOUNDS.length;i++){"
"const le=QD_BOUNDS[i]===Infinity?'+Inf':String(QD_BOUNDS[i]);"
"const c=m[pfx+'_bucket{le=\"'+le+'\"}'];"
"if(c!=null&&c>=target)return QD_BOUNDS[i];}"
"return null;}"
"async function tick(){try{"
" const [h,r]=await Promise.all(["
"  fetch('/health').then(x=>x.json()),"
"  fetch('/metrics').then(x=>x.text())]);"
" document.getElementById('st').textContent='ok';"
" document.getElementById('st').className='badge';"
" document.getElementById('i_host').textContent=location.host;"
" document.getElementById('i_pid').textContent=h.pid;"
" document.getElementById('i_up').textContent=fmtSec(h.uptime_s);"
" document.getElementById('host').textContent=location.host;"
" document.getElementById('uptime').textContent='uptime '+fmtSec(h.uptime_s);"
" const m=parseMetrics(r);const now=Date.now();"
" if(state.prev){"
"  const dt=(now-state.prev.ts)/1000;"
"  const pm=state.prev.m;"
"  const dq=((m.qengine_queries_total||0)-(pm.qengine_queries_total||0))/dt;"
"  const dw=((m.qengine_rows_written_total||0)-(pm.qengine_rows_written_total||0))/dt;"
"  state.qHist.push(Math.max(0,dq));if(state.qHist.length>MAXPTS)state.qHist.shift();"
"  state.wHist.push(Math.max(0,dw));if(state.wHist.length>MAXPTS)state.wHist.shift();"
"  document.getElementById('rq').textContent=fmt(dq,'/s');"
"  document.getElementById('rw').textContent=fmt(dw,'/s');"
"  const p50=quantile(m,'qengine_query_duration_ms',0.5);"
"  const p99=quantile(m,'qengine_query_duration_ms',0.99);"
"  document.getElementById('rqmeta').textContent="
"    'p50 '+(p50!=null?p50+'ms':'-')+' · p99 '+(p99!=null?p99+'ms':'-');"
"  const bcnt=m['qengine_ingest_batch_size_count']||0;"
"  const bsum=m['qengine_ingest_batch_size_sum']||0;"
"  const avgB=bcnt>0?(bsum/bcnt):0;"
"  document.getElementById('rwmeta').textContent='avg batch '+Math.round(avgB)+' rows';"
"  requestAnimationFrame(()=>{spark('sq',state.qHist);spark('sw',state.wHist);});"
"  const dt_min=dt/60;"
"  const dFl=(m.qengine_flushes_total||0)-(pm.qengine_flushes_total||0);"
"  const dCp=(m.qengine_compactions_total||0)-(pm.qengine_compactions_total||0);"
"  const dBL=(m.qengine_bloom_lookups_total||0)-(pm.qengine_bloom_lookups_total||0);"
"  const dBS=(m.qengine_bloom_skips_total||0)-(pm.qengine_bloom_skips_total||0);"
"  document.getElementById('pf').textContent=fmt(dFl/Math.max(dt_min,0.001),'');"
"  document.getElementById('pc').textContent=fmt(dCp/Math.max(dt_min,0.001),'');"
"  document.getElementById('pb').textContent="
"    dBL>0?(100*dBS/dBL).toFixed(1)+'%':'-';"
"  const dAuth=(m.qengine_auth_logins_total||0)-(pm.qengine_auth_logins_total||0);"
"  if(dAuth>0)pushEv('auth',`+${dAuth} login(s)`);"
"  const dDeny=(m.qengine_auth_denied_total||0)-(pm.qengine_auth_denied_total||0);"
"  if(dDeny>0)pushEv('deny',`+${dDeny} auth denial(s)`);"
"  if(dFl>0)pushEv('flush',`+${dFl} flush(es)`);"
"  const dErr=(m.qengine_query_errors_total||0)-(pm.qengine_query_errors_total||0);"
"  if(dErr>0)pushEv('err',`+${dErr} query error(s)`);"
"  const dRF=(m.qengine_replicate_fail_total||0)-(pm.qengine_replicate_fail_total||0);"
"  if(dRF>0)pushEv('err',`+${dRF} replicate fail(s)`);"
"  const dDial=(m.qengine_replica_dial_total||0)-(pm.qengine_replica_dial_total||0);"
"  if(dDial>0)pushEv('info',`+${dDial} peer conn(s) dialled`);}"
" state.prev={ts:now,m};"
" document.getElementById('pm').textContent=fmt(m.qengine_memtable_rows||0,'');"
" document.getElementById('pd').textContent=fmtBytes(m.qengine_disk_bytes||0);"
" document.getElementById('pa').textContent="
"   (m.qengine_cluster_nodes_alive!=null?m.qengine_cluster_nodes_alive:'-');"
" document.getElementById('g').innerHTML=CARDS.map(c=>{"
"  const v=m[c[0]]??0;"
"  return `<div class=card><div class=k>${c[1]}</div>"
"<div class=v>${fmt(v,c[2])}</div></div>`;}).join('');"
" document.getElementById('t').textContent=new Date().toLocaleTimeString();"
"}catch(e){"
" document.getElementById('st').textContent='unreachable';"
" document.getElementById('st').className='badge bad';}}"
"async function tickCluster(){try{"
" const c=await (await fetch('/cluster')).json();"
" const ab=c.autobalance||{};"
" const abByID={};(ab.nodes||[]).forEach(n=>abByID[n.id]=n);"
" const isStandalone=c.mode==='standalone';"
" const nodes=c.nodes||[];"
" const aliveCount=nodes.filter(n=>(n.state||'ALIVE')==='ALIVE').length;"
" const mb=document.getElementById('modebadge');"
" if(isStandalone){mb.textContent='STANDALONE';mb.className='badge mode standalone';}"
" else{mb.textContent='CLUSTER · '+aliveCount+'/'+nodes.length+' nodes';"
"      mb.className='badge mode';}"
" let clusterWrites=0;(ab.nodes||[]).forEach(n=>{clusterWrites+=(n.writes_sec||0);});"
" document.getElementById('cmode').innerHTML='local_id: '+(c.local_id||0)"
"   +(isStandalone?'':('   ·   cluster write rate: '+clusterWrites+' rows/sec'))"
"   +(ab.ema_writes_sec!=null?('   ·   ema_local: '+ab.ema_writes_sec.toFixed(1)):'');"
/* Build a cheap key to detect cluster-level churn so we can skip the
 * innerHTML write when nothing visible changed (biggest render cost). */
" const key=nodes.map(n=>n.id+':'+n.state+':'+(n.hb_age_ms||0)+':'+(n.known_for_s||0)).join('|')"
"   +'#'+(c.local&&c.local.disk?c.local.disk.used_x10:'-');"
" if(key===state.lastCKey)return;state.lastCKey=key;"
" const rows=nodes.map(n=>{"
"  const s=(n.state||'ALIVE').toLowerCase();"
"  const isLocal=(String(n.id)===String(c.local_id));"
"  const localDisk=(isLocal&&c.local&&c.local.disk)?c.local.disk:(n.disk||{});"
"  const abNode=abByID[n.id]||{};"
"  const abStorage=abNode.storage_mb?(abNode.storage_mb*1024*1024):0;"
"  const totalB=localDisk.total_bytes||0;"
"  const freeB=localDisk.free_bytes||0;"
"  const cap=totalB?(fmtBytes(freeB)+' / '+fmtBytes(totalB))"
"    :(abStorage?('used '+fmtBytes(abStorage)):(isStandalone?'-':'peer'));"
"  let usedCell='-';"
"  if(localDisk.used_x10!=null){"
"    const pct=(localDisk.used_x10/10).toFixed(1)+'%';"
"    const bc=diskClass(localDisk.used_x10);"
"    const w=Math.max(2,Math.min(100,localDisk.used_x10/10));"
"    usedCell=`<span class=\"bar ${bc}\"><span style=\"width:${w}%\"></span></span>${pct}`;"
"  }else if(abStorage){usedCell=fmtBytes(abStorage);}"
"  const vn=localDisk.vn_weight||abNode.vn||'-';"
"  const up=(isLocal&&c.local&&c.local.uptime_s)?fmtSec(c.local.uptime_s)"
"    :(n.known_for_s?fmtSec(n.known_for_s):'-');"
"  const hbAge=n.hb_age_ms;"
"  const hbCell=hbAge==null?'-'"
"    :`<span class=\"pill ${hbClass(hbAge)}\">${fmtAge(hbAge)}</span>`;"
"  const badge=isLocal?' <span class=pill ok style=\"margin-left:6px\">this</span>':'';"
"  return `<tr><td>${n.id}${badge}</td><td>${n.addr||'-'}</td>"
"<td><span class=\"pill ${s}\">${(n.state||'ALIVE')}</span></td>"
"<td>${cap}</td><td class=val>${usedCell}</td>"
"<td class=val>${vn}</td>"
"<td class=val>${up}</td>"
"<td class=val>${hbCell}</td></tr>`;}).join('');"
" document.getElementById('cbody').innerHTML=rows||'<tr><td colspan=8>no nodes</td></tr>';"
"}catch(e){"
" document.getElementById('cbody').innerHTML="
"   '<tr><td colspan=8 class=sub>/cluster unreachable: '+e.message+'</td></tr>';}}"
"tick();setInterval(tick,2000);"
"tickCluster();setInterval(tickCluster,3000);"

/* ---- Query console + catalog tree ---------------------------------- */
"function esc(s){return String(s).replace(/[&<>]/g,c=>(c==='&'?'&amp;':c==='<'?'&lt;':'&gt;'));}"
"function fmtCell(v,ty){"
"if(v===null||v===undefined)return '<td class=null>NULL</td>';"
"if(ty==='TIMESTAMP'){const n=Number(v);if(!isFinite(n))return `<td>${esc(v)}</td>`;"
"const d=new Date(n/1e6);const iso=d.toISOString().replace('T',' ').replace('Z','');"
"return `<td title='${n}'>${iso}</td>`;}"
"if(ty==='INT64'||ty==='FLOAT64')return `<td class=num>${esc(v)}</td>`;"
"return `<td>${esc(v)}</td>`;}"
"async function loadTree(){"
"const el=document.getElementById('tree');"
"el.innerHTML='<div class=empty>loading…</div>';"
"try{const t=await (await fetch('/tree')).json();"
"let h='';"
"h+='<div class=grp>Tables ('+((t.tables||[]).length)+')</div>';"
"if(!t.tables||!t.tables.length)h+='<div class=empty>no tables</div>';"
"else (t.tables||[]).forEach(x=>{"
"h+=`<div class=item data-tbl='${esc(x.name)}' title='double-click to query first 1000 rows'>`"
"+esc(x.name)+'</div>';});"
"if(t.groups&&t.groups.length){"
"h+='<div class=grp>Groups ('+t.groups.length+')</div>';"
"t.groups.forEach(g=>{h+=`<div class=item data-grp='${esc(g.name)}'>${esc(g.name)}</div>`;});}"
"el.innerHTML=h;"
"el.querySelectorAll('.item[data-tbl]').forEach(node=>{"
"node.addEventListener('dblclick',()=>{"
"const n=node.dataset.tbl;"
"document.getElementById('qed').value=`SELECT * FROM ${n} LIMIT 1000`;"
"runSql();});"
"node.addEventListener('click',()=>{"
"el.querySelectorAll('.item.active').forEach(x=>x.classList.remove('active'));"
"node.classList.add('active');});});"
"el.querySelectorAll('.item[data-grp]').forEach(node=>{"
"node.addEventListener('dblclick',()=>{"
"document.getElementById('qed').value=`LIST DEVICES IN GROUP ${node.dataset.grp}`;"
"runSql();});});"
"}catch(e){el.innerHTML='<div class=empty>error: '+esc(e.message)+'</div>';}}"
"async function runSql(){"
"const ed=document.getElementById('qed');"
"const btn=document.getElementById('qrun');"
"const st=document.getElementById('qstatus');"
"const out=document.getElementById('qres');"
"const q=ed.value.trim();"
"if(!q){st.textContent='empty query';return;}"
"btn.disabled=true;st.textContent='running…';"
"const t0=performance.now();"
"try{const r=await fetch('/sql',{method:'POST',"
"headers:{'Content-Type':'application/json'},body:JSON.stringify({q})});"
"const j=await r.json();"
"const dt=Math.round(performance.now()-t0);"
"if(j.error){out.innerHTML=`<div class=err>${esc(j.error)}</div>`;"
"st.textContent='error in '+dt+'ms';return;}"
"const cols=j.cols||[],types=j.types||[],rows=j.rows||[];"
"let h='<table><thead><tr>';"
"cols.forEach((c,i)=>{h+=`<th>${esc(c)}<span class=ty>${esc(types[i]||'')}</span></th>`;});"
"h+='</tr></thead><tbody>';"
"rows.forEach(row=>{h+='<tr>';"
"row.forEach((v,i)=>{h+=fmtCell(v,types[i]);});"
"h+='</tr>';});"
"h+='</tbody></table>';"
"if(!rows.length)h='<div class=empty style=\"padding:14px\">no rows</div>';"
"out.innerHTML=h;"
"st.textContent=`${j.nrows} row${j.nrows===1?'':'s'}`"
"+(j.truncated?' (truncated at 1000)':'')"
"+` · server ${j.ms}ms · wire ${dt}ms`;"
"}catch(e){out.innerHTML=`<div class=err>${esc(e.message)}</div>`;"
"st.textContent='network error';}"
"finally{btn.disabled=false;}}"
"document.getElementById('qrun').addEventListener('click',runSql);"
"document.getElementById('qreload').addEventListener('click',loadTree);"
"document.getElementById('qed').addEventListener('keydown',e=>{"
"if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)){e.preventDefault();runSql();}});"
"loadTree();"
"</script></body></html>";
        size_t body_len = sizeof(DASH) - 1;
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %zu\r\n"
            "Connection: close\r\n\r\n",
            body_len);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, DASH, body_len);
    } else {
        const char *not_found =
            "HTTP/1.1 404 Not Found\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        write_all(fd, not_found, strlen(not_found));
    }

done:
    free(req);
    close(fd);
    return NULL;
}

/* ---- Accept loop --------------------------------------------------------- */

static void *accept_loop(void *arg) {
    struct tsdb_metrics_server *ms = (struct tsdb_metrics_server *)arg;

    while (ms->running) {
        struct pollfd pfd = { ms->listen_fd, POLLIN, 0 };
        int r = poll(&pfd, 1, 200);
        if (r <= 0) continue;
        if (!(pfd.revents & POLLIN)) continue;

        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(ms->listen_fd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }

        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, handle_connection, (void *)(intptr_t)cfd) != 0) {
            close(cfd);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

/* ---- Public API ---------------------------------------------------------- */

static int parse_bind(const char *addr, char *host_out, size_t hcap, int *port_out) {
    if (!addr || addr[0] == '\0') return -1;
    const char *colon = strrchr(addr, ':');
    if (!colon) return -1;
    size_t hlen = (size_t)(colon - addr);
    if (hlen >= hcap) hlen = hcap - 1;
    memcpy(host_out, addr, hlen);
    host_out[hlen] = '\0';
    *port_out = atoi(colon + 1);
    return 0;
}

int tsdb_metrics_server_start(const char *bind_addr, tsdb_metrics_server_t **out) {
    *out = NULL;
    if (!bind_addr || bind_addr[0] == '\0') return 0; /* disabled */

    pthread_once(&g_once, record_start);

    char host[128];
    int  port;
    if (parse_bind(bind_addr, host, sizeof(host), &port) != 0) return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) <= 0) sa.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }
    /* Larger backlog: when several dashboard tabs + Prometheus scrape
     * the same node, connection arrivals burst.  32 was enough for
     * scrapes-only but gets noisy with the embedded dashboard polling
     * /health + /metrics + /cluster every 2–5 s per tab. */
    if (listen(fd, 512) < 0) {
        close(fd);
        return -1;
    }

    /* Get actual port (if port was 0). */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);
    int actual_port = ntohs(bound.sin_port);

    struct tsdb_metrics_server *ms =
        (struct tsdb_metrics_server *)calloc(1, sizeof(*ms));
    if (!ms) { close(fd); return -1; }

    ms->listen_fd = fd;
    ms->port      = actual_port;
    ms->running   = 1;

    if (pthread_create(&ms->accept_thread, NULL, accept_loop, ms) != 0) {
        close(fd);
        free(ms);
        return -1;
    }

    *out = ms;
    return 0;
}

void tsdb_metrics_server_stop(tsdb_metrics_server_t *ms) {
    if (!ms) return;
    ms->running = 0;
    close(ms->listen_fd);
    pthread_join(ms->accept_thread, NULL);
    free(ms);
}

int tsdb_metrics_server_port(const tsdb_metrics_server_t *ms) {
    if (!ms) return -1;
    return ms->port;
}
