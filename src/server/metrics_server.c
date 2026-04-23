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

#define _GNU_SOURCE          /* for strcasestr */
#include "metrics_server.h"
#include "metrics.h"
#include "../cluster/disk_weight.h"
#include "../../include/tsdb.h"   /* for TSDB_OK, TSDB_ERR_PERMISSION */

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
static tsdb_retention_sweep_fn g_ret_sweep_fn = NULL;
static void                *g_ret_sweep_ud = NULL;
static tsdb_audit_tail_fn   g_audit_tail_fn = NULL;
static void                *g_audit_tail_ud = NULL;
static tsdb_pitr_trim_fn    g_pitr_fn       = NULL;
static void                *g_pitr_ud       = NULL;
static tsdb_auth_login_fn   g_auth_login_fn = NULL;
static tsdb_auth_check_fn   g_auth_check_fn = NULL;
static void                *g_auth_ud       = NULL;
static tsdb_raft_json_fn    g_raft_fn     = NULL;
static void                *g_raft_ud     = NULL;
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

void tsdb_metrics_server_set_retention_sweep_provider(
    tsdb_retention_sweep_fn fn, void *userdata)
{
    g_ret_sweep_fn = fn;
    g_ret_sweep_ud = userdata;
}

void tsdb_metrics_server_set_audit_provider(tsdb_audit_tail_fn fn,
                                              void *userdata)
{
    g_audit_tail_fn = fn;
    g_audit_tail_ud = userdata;
}

void tsdb_metrics_server_set_pitr_provider(tsdb_pitr_trim_fn fn,
                                             void *userdata)
{
    g_pitr_fn = fn;
    g_pitr_ud = userdata;
}

void tsdb_metrics_server_set_auth_provider(tsdb_auth_login_fn login,
                                             tsdb_auth_check_fn check,
                                             void *userdata)
{
    g_auth_login_fn = login;
    g_auth_check_fn = check;
    g_auth_ud       = userdata;
}

void tsdb_metrics_server_set_raft_provider(tsdb_raft_json_fn fn,
                                            void *userdata) {
    g_raft_ud = userdata;
    g_raft_fn = fn;
}

void tsdb_metrics_server_set_data_dir(const char *path) {
    if (!path) { g_data_dir[0] = '\0'; return; }
    snprintf(g_data_dir, sizeof(g_data_dir), "%s", path);
}

/* Forward decl — defined below; static-file helper above needs it. */
static void write_all(int fd, const char *buf, size_t len);

/* ──────────────────────────────────────────────────────────────────────────
 * Optional static-file serving.  When TSDB_DASHBOARD_DIR is set at process
 * start, GET / and GET /<path> read files from that directory instead of
 * returning the embedded HTML.  Intended use: the Vite+React dashboard
 * under repo root `dashboard/` is built into `dashboard/dist/`, the
 * operator points TSDB_DASHBOARD_DIR at it, and every request is served
 * as a static asset with its MIME type.  Missing env or missing index
 * file → falls through to the embedded HTML so nothing breaks.
 * ────────────────────────────────────────────────────────────────────── */
static char g_dashboard_dir[4096] = {0};
static int  g_dashboard_dir_probed = 0;

static const char *dashboard_dir(void) {
    if (!g_dashboard_dir_probed) {
        g_dashboard_dir_probed = 1;
        const char *e = getenv("TSDB_DASHBOARD_DIR");
        if (e && e[0]) {
            snprintf(g_dashboard_dir, sizeof(g_dashboard_dir), "%s", e);
        }
    }
    return g_dashboard_dir[0] ? g_dashboard_dir : NULL;
}

static const char *mime_of(const char *path) {
    const char *dot = strrchr(path, '.');
    if (!dot) return "application/octet-stream";
    dot++;
    if (!strcasecmp(dot, "html") || !strcasecmp(dot, "htm")) return "text/html; charset=utf-8";
    if (!strcasecmp(dot, "js")  || !strcasecmp(dot, "mjs")) return "application/javascript; charset=utf-8";
    if (!strcasecmp(dot, "css")) return "text/css; charset=utf-8";
    if (!strcasecmp(dot, "json")) return "application/json; charset=utf-8";
    if (!strcasecmp(dot, "svg")) return "image/svg+xml";
    if (!strcasecmp(dot, "png")) return "image/png";
    if (!strcasecmp(dot, "jpg") || !strcasecmp(dot, "jpeg")) return "image/jpeg";
    if (!strcasecmp(dot, "gif")) return "image/gif";
    if (!strcasecmp(dot, "woff2")) return "font/woff2";
    if (!strcasecmp(dot, "woff")) return "font/woff";
    if (!strcasecmp(dot, "ttf")) return "font/ttf";
    if (!strcasecmp(dot, "ico")) return "image/x-icon";
    if (!strcasecmp(dot, "txt") || !strcasecmp(dot, "map")) return "text/plain; charset=utf-8";
    return "application/octet-stream";
}

/* Resolve a request path (e.g. "/" or "/assets/main.js") against the
 * configured dashboard dir, blocking "../" traversal.  Returns the
 * mmap-safe absolute path on success, "" on reject. */
static int resolve_static_path(const char *urlpath, char *out, size_t cap) {
    const char *root = dashboard_dir();
    if (!root) return -1;

    /* "/" → "/index.html" */
    const char *rel = urlpath && urlpath[0] ? urlpath : "/";
    if (rel[0] != '/') return -1;
    if (rel[1] == '\0') rel = "/index.html";

    /* Block ../ traversal, double slashes, and absolute paths. */
    for (const char *s = rel; *s; s++) {
        if (s[0] == '.' && s[1] == '.') return -1;
        if (s[0] == '/' && s[1] == '/') return -1;
    }

    int n = snprintf(out, cap, "%s%s", root, rel);
    if (n <= 0 || (size_t)n >= cap) return -1;
    return 0;
}

static int try_serve_static(int fd, const char *urlpath) {
    char path[4200];
    if (resolve_static_path(urlpath, path, sizeof(path)) != 0) return 0;

    FILE *f = fopen(path, "rb");
    if (!f) return 0;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return 0; }
    long len = ftell(f);
    if (len < 0) { fclose(f); return 0; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return 0; }

    char hdr[512];
    const char *ct = mime_of(path);
    /* Hashed Vite assets live under /assets/ and are safe to cache for a
     * year; the top-level index.html must stay fresh so dashboard pushes
     * propagate on reload. */
    const char *cc = strstr(urlpath, "/assets/") == urlpath
                       ? "public, max-age=31536000, immutable"
                       : "no-store";
    int hn = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: %s\r\n"
        "Cache-Control: %s\r\n"
        "Content-Length: %ld\r\n"
        "Connection: close\r\n\r\n",
        ct, cc, len);
    write_all(fd, hdr, (size_t)hn);

    /* Heap-allocate — a 64 KiB stack buffer overflowed a tight worker
     * thread stack on macOS and fread() returned 0 despite feof=0.
     * Holding the buffer on the heap sidesteps that. */
    const size_t BUFCAP = 64 * 1024;
    char *buf = malloc(BUFCAP);
    if (!buf) { fclose(f); return 0; }
    while (len > 0) {
        size_t take = len > (long)BUFCAP ? BUFCAP : (size_t)len;
        size_t got  = fread(buf, 1, take, f);
        if (got == 0) break;
        write_all(fd, buf, got);
        len -= (long)got;
    }
    free(buf);
    fclose(f);
    return 1;
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
    int route_raft    = 0;
    int route_backup  = 0;
    int route_ret_sweep = 0;
    int route_login   = 0;
    int route_logout  = 0;
    int route_audit   = 0;
    int route_pitr    = 0;
    if (strncmp(req, "GET /metrics", 12) == 0)           route_metrics = 1;
    else if (strncmp(req, "GET /login", 10) == 0)        route_login = 1;
    else if (strncmp(req, "POST /login", 11) == 0)       route_login = 2; /* 2 = POST */
    else if (strncmp(req, "GET /logout", 11) == 0)       route_logout = 1;
    else if (strncmp(req, "GET /health", 11) == 0)       route_health  = 1;
    else if (strncmp(req, "GET /cluster", 12) == 0)      route_cluster = 1;
    else if (strncmp(req, "GET /tree", 9) == 0)          route_tree    = 1;
    else if (strncmp(req, "GET /raft", 9) == 0)          route_raft    = 1;
    else if (strncmp(req, "GET /backup", 11) == 0)       route_backup  = 1;
    else if (strncmp(req, "GET /audit", 10) == 0)        route_audit   = 1;
    else if (strncmp(req, "POST /pitr", 10) == 0)        route_pitr    = 1;
    else if (strncmp(req, "POST /retention/sweep", 21) == 0 ||
             strncmp(req, "GET /retention/sweep",  20) == 0)
                                                          route_ret_sweep = 1;
    else if (strncmp(req, "POST /sql", 9) == 0 ||
             strncmp(req, "GET /sql",  8) == 0)          route_sql     = 1;
    else if (strncmp(req, "GET / ", 6) == 0 ||
             strncmp(req, "GET /index", 10) == 0)        route_dash    = 1;
    /* GET /assets/*, /favicon.ico and /vite.svg are served from the
     * static bundle when TSDB_DASHBOARD_DIR is set — gated by the
     * dashboard auth cookie like the dashboard proper. */
    int route_static = 0;
    if (strncmp(req, "GET /assets/", 12) == 0 ||
        strncmp(req, "GET /favicon", 12) == 0 ||
        strncmp(req, "GET /vite.svg", 13) == 0) route_static = 1;
    /* Extract just the URL path (request-target) for static-file handlers
     * that need to know which file to read.  `req` points at the full
     * request line; the path starts after "GET " and ends at SP/?. */
    char req_path[512] = "/";
    {
        const char *s = strchr(req, ' ');
        if (s) {
            s++;
            const char *e = s;
            while (*e && *e != ' ' && *e != '?' && *e != '\r' && *e != '\n') e++;
            size_t plen = (size_t)(e - s);
            if (plen >= sizeof(req_path)) plen = sizeof(req_path) - 1;
            memcpy(req_path, s, plen);
            req_path[plen] = '\0';
        }
    }

    /* ---- Optional dashboard login gate ----------------------------------
     * When TSDB_DASHBOARD_AUTH is set at process start, every route that
     * mutates state or exposes internal data requires a valid session
     * cookie.  /health, /metrics, /login, /logout stay public so
     * Prometheus / k8s probes and the login flow itself keep working.
     * The cookie is issued by POST /login and checked via the registered
     * auth provider (which calls into tsdb_auth_check). */
    static int g_auth_enabled = -1;
    if (g_auth_enabled < 0) {
        const char *e = getenv("TSDB_DASHBOARD_AUTH");
        g_auth_enabled = (e && *e && *e != '0') ? 1 : 0;
    }
    /* /raft stays public: it's read-only telemetry consumed by both
     * the dashboard and the Raft acceptance scenarios over plain curl.
     * Every gated route below mutates state or exposes schema/catalog. */
    int route_needs_auth = g_auth_enabled &&
        (route_dash || route_cluster || route_tree ||
         route_backup || route_ret_sweep || route_sql ||
         route_audit || route_pitr || route_static);

    /* Extract tsdb_auth token from Cookie header (first match wins).
     * HTTP headers are case-insensitive per RFC 7230; most clients use
     * "Cookie:" but we accept "cookie:" too. */
    char cookie_tok[65] = {0};
    if (route_needs_auth) {
        const char *ch = strstr(req, "\nCookie:");
        if (!ch) ch = strstr(req, "\ncookie:");
        if (!ch) ch = strstr(req, "\r\nCookie:");
        if (!ch) ch = strstr(req, "\r\ncookie:");
        if (ch) {
            ch = strstr(ch, "tsdb_auth=");
            if (ch) {
                ch += 10;
                size_t w = 0;
                while (*ch && *ch != ';' && *ch != '\r' && *ch != '\n' &&
                       w < sizeof(cookie_tok) - 1) {
                    cookie_tok[w++] = *ch++;
                }
                cookie_tok[w] = '\0';
            }
        }

        int authed = 0;
        if (cookie_tok[0] && g_auth_check_fn &&
            g_auth_check_fn(g_auth_ud, cookie_tok) == TSDB_OK) authed = 1;

        if (!authed) {
            /* Redirect browsers (dashboard GET) to /login; return 401
             * for XHR (POST /sql etc) so client-side JS can handle. */
            if (route_dash) {
                const char *r =
                    "HTTP/1.1 302 Found\r\nLocation: /login\r\n"
                    "Content-Length: 0\r\nConnection: close\r\n\r\n";
                write_all(fd, r, strlen(r));
            } else {
                const char *r =
                    "HTTP/1.1 401 Unauthorized\r\n"
                    "Content-Type: application/json\r\n"
                    "Content-Length: 32\r\nConnection: close\r\n\r\n"
                    "{\"error\":\"login required\"}\n";
                write_all(fd, r, strlen(r));
            }
            goto done;
        }
    }

    if (route_login == 1) {
        /* GET /login — minimal form that POSTs back to /login. */
        static const char FORM[] =
            "<!doctype html><html><head><meta charset=utf-8>"
            "<title>tsdb login</title>"
            "<style>body{font:14px/1.5 system-ui,sans-serif;display:flex;"
            "align-items:center;justify-content:center;min-height:100vh;"
            "margin:0;background:#f1f5f9}"
            ".card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;"
            "padding:24px 28px;width:320px;box-shadow:0 2px 6px rgba(0,0,0,.04)}"
            ".card h1{margin:0 0 4px;font-size:18px}"
            ".card p{margin:0 0 18px;color:#64748b;font-size:12px}"
            ".card label{display:block;font-size:12px;color:#334155;margin:10px 0 4px}"
            ".card input{width:100%;padding:8px 10px;border:1px solid #cbd5e1;"
            "border-radius:6px;font-size:13px;box-sizing:border-box}"
            ".card button{width:100%;margin-top:16px;padding:9px;border:none;"
            "border-radius:6px;background:#6366f1;color:#fff;font-size:13px;"
            "cursor:pointer}"
            ".err{margin-top:12px;padding:8px;border-radius:6px;background:#fee2e2;"
            "color:#991b1b;font-size:12px;display:none}"
            ".err.on{display:block}"
            "</style></head><body><div class=card>"
            "<h1>tsdb dashboard</h1>"
            "<p>Login required — default root / 123456 for fresh installs.</p>"
            "<form id=f method=POST action=/login>"
            "<label>Username</label><input name=user autofocus value=root>"
            "<label>Password</label><input name=pass type=password value=123456>"
            "<button type=submit>Login</button>"
            "<div class=err id=e>invalid credentials</div>"
            "</form>"
            "<script>"
            "if(location.search.indexOf('bad')>=0)document.getElementById('e').classList.add('on');"
            "</script></div></body></html>";
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
            "Content-Length: %zu\r\nCache-Control: no-store\r\n"
            "Connection: close\r\n\r\n", sizeof(FORM) - 1);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, FORM, sizeof(FORM) - 1);
        goto done;
    }

    if (route_login == 2) {
        /* POST /login — body is form-urlencoded: user=...&pass=... */
        char user[64] = {0};
        char pass[128] = {0};
        if (body_len > 0) {
            const char *u = strstr(body, "user=");
            if (u) {
                u += 5;
                size_t w = 0;
                while (*u && *u != '&' && w < sizeof(user) - 1 &&
                       (size_t)(u - body) < body_len) {
                    user[w++] = (*u == '+') ? ' ' : *u;
                    u++;
                }
            }
            const char *p = strstr(body, "pass=");
            if (p) {
                p += 5;
                size_t w = 0;
                while (*p && *p != '&' && w < sizeof(pass) - 1 &&
                       (size_t)(p - body) < body_len) {
                    pass[w++] = (*p == '+') ? ' ' : *p;
                    p++;
                }
            }
        }

        char tok[65] = {0};
        int rc = g_auth_login_fn
            ? g_auth_login_fn(g_auth_ud, user, pass, tok, sizeof(tok))
            : TSDB_ERR_PERMISSION;

        if (rc == TSDB_OK && tok[0]) {
            char hdr[512];
            int hlen = snprintf(hdr, sizeof(hdr),
                "HTTP/1.1 302 Found\r\n"
                "Set-Cookie: tsdb_auth=%s; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400\r\n"
                "Location: /\r\nContent-Length: 0\r\n"
                "Connection: close\r\n\r\n", tok);
            write_all(fd, hdr, (size_t)hlen);
        } else {
            const char *r =
                "HTTP/1.1 302 Found\r\nLocation: /login?bad=1\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            write_all(fd, r, strlen(r));
        }
        goto done;
    }

    if (route_logout) {
        const char *r =
            "HTTP/1.1 302 Found\r\n"
            "Set-Cookie: tsdb_auth=; Path=/; HttpOnly; Max-Age=0\r\n"
            "Location: /login\r\nContent-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        write_all(fd, r, strlen(r));
        goto done;
    }

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
    } else if (route_raft) {
        /* Small, stable JSON — fine on the stack. */
        char body_r[1024] = "{}";
        int blen = 2;
        if (g_raft_fn) {
            int n = g_raft_fn(g_raft_ud, body_r, sizeof(body_r));
            if (n > 0) blen = n;
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
        write_all(fd, body_r, (size_t)blen);
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
        /* Catalog tree for the dashboard left-hand navigator.
         *
         * Size: 8 MB on the heap (not stack).  A 4-level industrial
         * catalog (1 DB × 8 Groups × 64 VTables × 5000 PTables) emits
         * ~400 KB already; reserve enough headroom that future fleets
         * with 50 K+ tables still render instead of getting truncated
         * right before the `databases:[]` / `vtables:[]` sections. */
        size_t cap = 8u * 1024u * 1024u;
        char *body_t = malloc(cap);
        int blen = 0;
        if (body_t) {
            if (g_tree_fn) blen = g_tree_fn(g_tree_ud, body_t, cap);
            if (blen <= 0) {
                blen = snprintf(body_t, cap,
                                "{\"db\":\"%s\",\"tables\":[],"
                                "\"note\":\"tree provider not installed\"}\n",
                                g_data_dir);
            }
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
        if (body_t) {
            write_all(fd, body_t, (size_t)blen);
            free(body_t);
        }
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
            rlen = g_sql_fn(g_sql_ud, q_start, q_len,
                            cookie_tok, res, RES_CAP);
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
    } else if (route_backup) {
        /* Stream a tar.gz of g_data_dir so an operator can snapshot a
         * single node for disaster recovery.  Uses popen(tar …) and
         * forwards bytes to the HTTP client with chunked transfer
         * encoding.  Caveats:
         *   - read-only best-effort: the memtable/flush path is not
         *     quiesced, so an actively-written table may show torn
         *     partitions in the tarball.  Ingest pause + /backup is
         *     the safe pattern for production.
         *   - no auth on this endpoint today — same security posture
         *     as /metrics and /sql, rely on network isolation.
         * Restore: stop node, extract the tarball into the node's
         * data_dir, restart.  The anti-entropy thread will close any
         * remaining gap against live peers automatically. */
        tsdb_metric_inc("qengine_backup_requests_total");
        if (!g_data_dir[0]) {
            const char *err = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 22\r\nConnection: close\r\n\r\ndata_dir not configured";
            write_all(fd, err, strlen(err));
            goto done;
        }

        /* tar -cz -C <parent> <leaf>: gives a relative tree.  We
         * use --ignore-failed-read so in-flight tmp/gc files don't
         * abort the whole stream. */
        char parent[4096];
        snprintf(parent, sizeof(parent), "%s", g_data_dir);
        char *slash = strrchr(parent, '/');
        const char *leaf = g_data_dir;
        if (slash) { *slash = '\0'; leaf = slash + 1; }
        else       { snprintf(parent, sizeof(parent), "."); }

        char cmd[8192];
        snprintf(cmd, sizeof(cmd),
                 "tar --ignore-failed-read -cz -C '%s' '%s' 2>/dev/null",
                 parent, leaf);
        FILE *tar = popen(cmd, "r");
        if (!tar) {
            const char *err = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 10\r\nConnection: close\r\n\r\ntar failed";
            write_all(fd, err, strlen(err));
            goto done;
        }

        char fname[256];
        snprintf(fname, sizeof(fname), "%s-backup.tar.gz",
                 leaf[0] ? leaf : "tsdb");
        char hdr[512];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/gzip\r\n"
            "Content-Disposition: attachment; filename=\"%s\"\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n\r\n",
            fname);
        write_all(fd, hdr, (size_t)hlen);

        uint8_t chunk[64 * 1024];
        uint64_t total = 0;
        for (;;) {
            size_t n = fread(chunk, 1, sizeof(chunk), tar);
            if (n == 0) break;
            char size_hdr[32];
            int slen = snprintf(size_hdr, sizeof(size_hdr), "%zx\r\n", n);
            write_all(fd, size_hdr, (size_t)slen);
            write_all(fd, (char *)chunk, n);
            write_all(fd, "\r\n", 2);
            total += n;
        }
        write_all(fd, "0\r\n\r\n", 5);
        pclose(tar);
        tsdb_metric_add("qengine_backup_bytes_streamed_total", total);
    } else if (route_ret_sweep) {
        int deleted = -1;
        if (g_ret_sweep_fn) deleted = g_ret_sweep_fn(g_ret_sweep_ud);
        char body[128];
        int blen = snprintf(body, sizeof(body),
            "{\"ok\":%s,\"partitions_deleted\":%d}\n",
            deleted >= 0 ? "true" : "false", deleted);
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 %s\r\nContent-Type: application/json\r\n"
            "Content-Length: %d\r\nConnection: close\r\n\r\n",
            deleted >= 0 ? "200 OK" : "503 Service Unavailable", blen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body, (size_t)blen);
    } else if (route_audit) {
        /* Tail the audit log.  Query-string "n=<N>" picks how many
         * rows to return; default 200, cap at 2000 so a huge log can't
         * blow the 2 MiB response buffer. */
        int nrows = 200;
        const char *q = strchr(req, '?');
        if (q) {
            const char *n = strstr(q, "n=");
            if (n) { int v = atoi(n + 2); if (v > 0) nrows = v; }
        }
        if (nrows > 2000) nrows = 2000;

        const size_t AUD_CAP = 2 * 1024 * 1024;
        char *body = malloc(AUD_CAP);
        if (!body) {
            const char *oom = "HTTP/1.1 500 Internal Server Error\r\n"
                              "Content-Length: 0\r\nConnection: close\r\n\r\n";
            write_all(fd, oom, strlen(oom)); goto done;
        }
        int n = snprintf(body, AUD_CAP, "{\"rows\":[");
        int wrote = 0;
        if (g_audit_tail_fn) {
            char raw[1024 * 1024];
            int got = g_audit_tail_fn(g_audit_tail_ud, nrows,
                                       raw, sizeof(raw));
            if (got > 0) {
                /* raw is newline-separated JSONL; rewrap as JSON array. */
                int first = 1;
                char *p = raw, *line_end;
                raw[got < (int)sizeof(raw) ? got : (int)sizeof(raw) - 1] = '\0';
                while ((line_end = strchr(p, '\n')) != NULL) {
                    *line_end = '\0';
                    if (*p) {
                        int add = snprintf(body + n, AUD_CAP - (size_t)n,
                                           "%s%s", first ? "" : ",", p);
                        if (add > 0 && (size_t)n + (size_t)add + 32 < AUD_CAP) {
                            n += add; wrote++; first = 0;
                        } else {
                            break;
                        }
                    }
                    p = line_end + 1;
                }
            }
        }
        n += snprintf(body + n, AUD_CAP - (size_t)n,
                      "],\"nrows\":%d}\n", wrote);
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Length: %d\r\nConnection: close\r\n\r\n", n);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body, (size_t)n);
        free(body);
    } else if (route_pitr) {
        /* Point-in-time recovery trim.  POST /pitr?ts=<ns> drops every
         * partition whose start timestamp is > ts across every open
         * table.  Intended to run immediately after extracting a
         * /backup tarball so the operator can roll the restored node
         * forward to an exact instant. */
        int64_t ts_ns = 0;
        const char *q = strchr(req, '?');
        if (q) {
            const char *t = strstr(q, "ts=");
            if (t) ts_ns = strtoll(t + 3, NULL, 10);
        }
        int removed = -1;
        if (ts_ns <= 0) {
            const char *r = "HTTP/1.1 400 Bad Request\r\n"
                            "Content-Type: application/json\r\n"
                            "Content-Length: 38\r\nConnection: close\r\n\r\n"
                            "{\"error\":\"missing ?ts=<ns> parameter\"}\n";
            write_all(fd, r, strlen(r));
            goto done;
        }
        if (g_pitr_fn) removed = g_pitr_fn(g_pitr_ud, ts_ns);

        char body[160];
        int blen = snprintf(body, sizeof(body),
            "{\"ok\":%s,\"target_ns\":%lld,\"partitions_removed\":%d}\n",
            removed >= 0 ? "true" : "false",
            (long long)ts_ns, removed);
        char hdr[256];
        int hlen = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 %s\r\nContent-Type: application/json\r\n"
            "Content-Length: %d\r\nConnection: close\r\n\r\n",
            removed >= 0 ? "200 OK" : "503 Service Unavailable", blen);
        write_all(fd, hdr, (size_t)hlen);
        write_all(fd, body, (size_t)blen);
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
    } else if (route_static) {
        /* Vite asset bundle — /assets/*, /favicon.ico, /vite.svg.
         * Served verbatim from TSDB_DASHBOARD_DIR with immutable caching
         * on hashed /assets/ paths.  404 when the dir isn't configured
         * or the file doesn't exist, so a stale bookmark can't confuse
         * the embedded HTML fallback path. */
        if (!try_serve_static(fd, req_path)) {
            const char *r = "HTTP/1.1 404 Not Found\r\n"
                            "Content-Length: 0\r\nConnection: close\r\n\r\n";
            write_all(fd, r, strlen(r));
        }
    } else if (route_dash) {
        /* Dashboard — prefer the Vite+React build from TSDB_DASHBOARD_DIR
         * when the env var is set and dashboard/dist/index.html exists;
         * otherwise fall through to the single-file embedded HTML below.
         * The embedded version remains the canonical minimal-config
         * fallback so every tsdb install has a working dashboard even
         * without bundling the Vite artefacts. */
        if (try_serve_static(fd, "/index.html")) goto done;
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
/* Language switcher — pushed to the right edge of the header. */
".langsw{margin-left:auto;display:inline-flex;gap:0;"
"border:1px solid var(--bd);border-radius:14px;overflow:hidden;"
"font-size:11px;background:var(--card)}"
".langsw button{background:transparent;border:0;color:var(--mu);"
"padding:3px 10px;cursor:pointer;font-weight:500;letter-spacing:.03em}"
".langsw button:hover{background:#f1f5f9}"
".langsw button.on{background:var(--accent);color:#fff}"
".langsw button + button{border-left:1px solid var(--bd)}"
".langsw button.on + button,.langsw button + button.on{border-left-color:var(--accent)}"
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
".qshell{display:grid;grid-template-columns:360px 1fr;gap:10px;align-items:stretch}"
".tree{background:var(--card);border:1px solid var(--bd);border-radius:8px;"
"padding:8px 10px;min-height:300px;max-height:520px;overflow:auto;font-size:12px}"
".tree .dbhdr{border-bottom:1px solid var(--bd);padding:4px 2px 8px 2px;"
"margin-bottom:6px}"
".tree .dbhdr .dbname{font-weight:700;color:var(--fg);font-size:13px;"
"display:flex;align-items:center;gap:6px;word-break:break-all}"
".tree .dbhdr .dbname>span:last-child{flex:1;overflow-wrap:anywhere}"
".tree .dbhdr .dbname .ic{background:#e0e7ff;color:#1e3a8a;border-radius:4px;"
"padding:1px 5px;font-size:10px;font-weight:600;letter-spacing:.04em}"
".tree .dbhdr .dbmeta{color:var(--mu);font-size:10px;margin-top:3px;"
"font-variant-numeric:tabular-nums;word-break:break-all}"
".tree .tbar{display:flex;gap:4px;margin:6px 0 8px 0;flex-wrap:wrap}"
".tree .tbar button{font-size:10px;padding:2px 8px;border-radius:4px;"
"border:1px solid var(--bd);background:#f8fafc;color:var(--fg);cursor:pointer}"
".tree .tbar button:hover{background:var(--accent);color:#fff;"
"border-color:var(--accent)}"
".tree .grp{font-weight:600;color:var(--mu);text-transform:uppercase;"
"letter-spacing:.06em;font-size:10px;margin-top:8px;margin-bottom:4px;"
"display:flex;justify-content:space-between;align-items:center}"
".tree .grp:first-child{margin-top:0}"
".tree .item{padding:3px 6px;border-radius:4px;cursor:pointer;"
"white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
"display:flex;align-items:center;gap:6px}"
/* DB + Group rows show full name (wrap) so operators can read the
 * entire identifier even when nested deeply; deeper rows (VTable /
 * PTable) keep the single-line ellipsis so the tree stays compact. */
".tree .item.db,.tree .item.gr{white-space:normal;word-break:break-word}"
".tree .item.db>.nm,.tree .item.gr>.nm{white-space:normal;overflow:visible;"
"text-overflow:clip;word-break:break-word;overflow-wrap:anywhere}"
".tree .item:hover{background:#f1f5f9}"
".tree .item.active{background:#dbeafe;color:#1e3a8a}"
".tree .item .nm{flex:1;overflow:hidden;text-overflow:ellipsis}"
".tree .item .sz{color:var(--mu);font-size:10px;"
"font-variant-numeric:tabular-nums}"
".tree .item .x{opacity:0;color:#94a3b8;cursor:pointer;padding:0 2px;"
"font-size:12px;border-radius:3px}"
".tree .item:hover .x{opacity:1}"
".tree .item .x:hover{background:#fee2e2;color:#991b1b}"
".tree .empty{color:var(--mu);font-style:italic;padding:3px 6px}"
/* Collapsible nested rows. */
".tree .node{cursor:pointer;user-select:none}"
".tree .node::before{content:'▸';display:inline-block;width:10px;"
"color:var(--mu);font-size:10px;transition:transform .1s}"
".tree .node.open::before{transform:rotate(90deg)}"
".tree .kids{margin-left:14px;display:none}"
".tree .node.open + .kids{display:block}"
".tree .db{font-weight:600;color:#1e3a8a}"
".tree .db .ic{background:#e0e7ff;color:#1e3a8a;border-radius:4px;"
"padding:0 5px;font-size:9px;font-weight:700;margin-right:4px}"
".tree .gr .ic{background:#dcfce7;color:#166534;border-radius:4px;"
"padding:0 5px;font-size:9px;font-weight:700;margin-right:4px}"
".tree .dev{color:var(--fg);padding-left:2px}"
".tree .dev::before{content:'•';color:var(--mu);margin-right:6px}"
/* Tiny modal for CREATE TABLE / CREATE GROUP prompts. */
".mask{position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:100;"
"display:flex;align-items:center;justify-content:center}"
".mask .box{background:var(--card);border-radius:10px;padding:18px 20px;"
"min-width:320px;max-width:90vw;box-shadow:0 8px 28px rgba(0,0,0,.18)}"
".mask .box h3{margin:0 0 10px 0;font-size:14px;color:var(--fg)}"
".mask .box label{display:block;font-size:11px;color:var(--mu);margin-top:8px}"
".mask .box input,.mask .box textarea{width:100%;padding:6px 8px;"
"font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;"
"border:1px solid var(--bd);border-radius:5px;box-sizing:border-box}"
".mask .box textarea{min-height:80px;resize:vertical}"
".mask .box .row{display:flex;gap:8px;justify-content:flex-end;margin-top:14px}"
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
/* QTL example library — a collapsible reference card */
".qex{background:var(--card);border:1px solid var(--bd);border-radius:8px;"
"padding:10px 14px;margin-bottom:10px}"
".qex > summary{cursor:pointer;font-weight:600;color:var(--fg);font-size:12px;"
"list-style:none;outline:none;user-select:none}"
".qex > summary::-webkit-details-marker{display:none}"
".qex > summary::before{content:'▸ ';color:var(--mu);font-weight:400}"
".qex[open] > summary::before{content:'▾ '}"
".qex > summary .hint{color:var(--mu);font-weight:400;font-size:11px;"
"margin-left:6px}"
".qex-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));"
"gap:8px;margin-top:10px}"
".qex-cat{display:flex;flex-direction:column;gap:4px}"
".qex-cat h4{font-size:10px;text-transform:uppercase;letter-spacing:.06em;"
"color:var(--mu);margin:0 0 2px 0;font-weight:600}"
".qex-item{border:1px solid var(--bd);border-radius:6px;padding:6px 8px;"
"cursor:pointer;background:#f8fafc;transition:background .08s}"
".qex-item:hover{background:#e0e7ff;border-color:#818cf8}"
".qex-item .t{font-size:11px;color:var(--fg);margin-bottom:2px;font-weight:500}"
".qex-item pre{margin:0;font:11px/1.35 ui-monospace,SFMono-Regular,Menlo,monospace;"
"color:#334155;white-space:pre-wrap;word-break:break-all}"
".qex-item .d{margin-top:3px;font-size:10px;color:var(--mu);line-height:1.35}"
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
"<span id=st class=badge data-i18n=status.loading>loading</span>"
"<span class=sub id=host></span>"
"<div class=langsw>"
"<button id=lang_en type=button>EN</button>"
"<button id=lang_zh type=button>中</button>"
"</div>"
"</header>"
"<div class=sub id=uptime></div>"

"<section><div class=secttl data-i18n=sec.rates>Realtime rates</div>"
"<div class=\"grid wide\">"
"<div class=card>"
"<div class=k data-i18n=card.qps>Queries / sec</div>"
"<div class=v id=rq>—</div>"
"<div class=vs id=rqmeta>p50 - · p99 -</div>"
"<svg class=spark id=sq viewBox=\"0 0 100 20\" preserveAspectRatio=none></svg>"
"</div>"
"<div class=card>"
"<div class=k data-i18n=card.rps>Rows written / sec</div>"
"<div class=v id=rw>—</div>"
"<div class=vs id=rwmeta>avg batch - rows</div>"
"<svg class=spark id=sw viewBox=\"0 0 100 20\" preserveAspectRatio=none></svg>"
"</div>"
"</div></section>"

"<section><div class=secttl data-i18n=sec.perf>Performance</div>"
"<div class=\"grid cards\">"
"<div class=card><div class=k data-i18n=perf.mem>Memtable rows</div>"
"<div class=v id=pm>-</div></div>"
"<div class=card><div class=k data-i18n=perf.disk>On-disk bytes</div>"
"<div class=v id=pd>-</div></div>"
"<div class=card><div class=k data-i18n=perf.flush>Flushes / min</div>"
"<div class=v id=pf>-</div></div>"
"<div class=card><div class=k data-i18n=perf.compact>Compactions / min</div>"
"<div class=v id=pc>-</div></div>"
"<div class=card><div class=k data-i18n=perf.bloom>Bloom skip rate</div>"
"<div class=v id=pb>-</div></div>"
"<div class=card><div class=k data-i18n=perf.alive>Cluster nodes alive</div>"
"<div class=v id=pa>-</div></div>"
"</div></section>"

"<section><div class=secttl data-i18n=sec.counters>Counters</div>"
"<div class=\"grid cards\" id=g></div></section>"

"<section><div class=secttl data-i18n=sec.topo>Cluster topology</div>"
"<div class=card>"
"<div class=sub id=cmode style=\"margin-bottom:6px\">-</div>"
"<div class=sub id=rolebar style=\"margin-bottom:6px\">-</div>"
"<div class=sub id=raftbar style=\"margin-bottom:6px\">-</div>"
"<table class=idtbl id=ctbl><thead>"
"<tr>"
"<td data-i18n=thead.id>id</td>"
"<td data-i18n=thead.addr>addr</td>"
"<td data-i18n=thead.role>role</td>"
"<td data-i18n=thead.state>state</td>"
"<td data-i18n=thead.disk>disk free/total</td>"
"<td class=val data-i18n=thead.used>used</td>"
"<td class=val data-i18n=thead.vn>vn</td>"
"<td class=val data-i18n=thead.uptime>uptime</td>"
"<td class=val data-i18n=thead.hb>last hb</td>"
"</tr></thead>"
"<tbody id=cbody></tbody></table></div></section>"

"<section><div class=secttl><span data-i18n=sec.console>Query console</span>"
" · <span data-i18n=sec.console_hint>"
"Ctrl+Enter to run · double-click a table on the left</span></div>"
"<details class=qex id=qexdet>"
"<summary><span data-i18n=qex.title>QTL examples</span>"
" <span class=hint data-i18n=qex.hint>(click any to load into editor)</span></summary>"
"<div class=qex-grid id=qexgrid></div>"
"</details>"
"<div class=qshell>"
"<div class=tree id=tree><div class=empty data-i18n=common.loading>loading…</div></div>"
"<div class=qpanel>"
"<textarea class=qeditor id=qed placeholder=\"SELECT * FROM ... LIMIT 100\" spellcheck=false>"
"SELECT 1</textarea>"
"<div class=qbar>"
"<button class=qbtn id=qrun data-i18n=btn.run>Run</button>"
"<button class=\"qbtn alt\" id=qreload data-i18n=btn.reload>Reload tree</button>"
"<span class=qstatus id=qstatus data-i18n=status.ready>ready</span>"
"</div>"
"<div class=qresult id=qres></div>"
"</div>"
"</div></section>"

"<section><div class=secttl data-i18n=sec.events>Events (last 50)</div>"
"<div class=card><div class=log id=log></div></div></section>"

"<section><div class=secttl data-i18n=sec.ident>Server identification</div>"
"<div class=card>"
"<table class=idtbl id=idt>"
"<tr><td data-i18n=ident.host>host</td><td class=val id=i_host>-</td></tr>"
"<tr><td data-i18n=ident.pid>pid</td><td class=val id=i_pid>-</td></tr>"
"<tr><td data-i18n=ident.uptime>uptime</td><td class=val id=i_up>-</td></tr>"
"<tr><td data-i18n=ident.crc>crc32c impl</td>"
"<td class=val data-i18n=ident.crcv>hardware-dispatched</td></tr>"
"</table></div></section>"

"<footer><span data-i18n=footer.poll>"
"polling /health + /metrics every 2s · last refresh</span> "
"<span id=t>-</span></footer>"

"<script>"
/* ---- i18n dictionary ---------------------------------------------- */
"const I18N={"
"en:{"
 "'sec.rates':'Realtime rates',"
 "'sec.perf':'Performance',"
 "'sec.counters':'Counters',"
 "'sec.topo':'Cluster topology',"
 "'sec.console':'Query console',"
 "'sec.console_hint':'Ctrl+Enter to run · double-click a table on the left',"
 "'sec.events':'Events (last 50)',"
 "'sec.ident':'Server identification',"
 "'card.qps':'Queries / sec','card.rps':'Rows written / sec',"
 "'perf.mem':'Memtable rows','perf.disk':'On-disk bytes',"
 "'perf.flush':'Flushes / min','perf.compact':'Compactions / min',"
 "'perf.bloom':'Bloom skip rate','perf.alive':'Cluster nodes alive',"
 "'cnt.active':'Active conns','cnt.qtot':'Queries total',"
 "'cnt.rwrt':'Rows written','cnt.bwrt':'Bytes written',"
 "'cnt.qerr':'Query errors','cnt.flush':'Flushes',"
 "'cnt.bloom':'Bloom skips','cnt.auth_ok':'Auth logins',"
 "'cnt.auth_no':'Auth denied','cnt.conns_life':'Conns lifetime',"
 "'cnt.rep_sent':'Replicate sent','cnt.rep_ack':'Replicate ack',"
 "'cnt.rep_fail':'Replicate fail','cnt.dial':'Peer conn dials',"
 "'cnt.stats_hit':'Agg stats hit','cnt.stats_miss':'Agg stats miss',"
 "'thead.id':'id','thead.addr':'addr','thead.state':'state','thead.role':'role',"
 "'thead.disk':'disk free/total','thead.used':'used','thead.vn':'vn',"
 "'thead.uptime':'uptime','thead.hb':'last hb',"
 "'role.master':'MASTER','role.data':'DATA',"
 "'topo.roles_fmt':'Masters {m} · Data {d}',"
 "'topo.raft_off':'Raft: off (fanout)',"
 "'topo.raft_state_fmt':'Raft: {role} · term {term} · leader {leader} · "
    "commit {commit} · applied {applied} · last {last}',"
 "'topo.leader_badge':'leader',"
 "'qex.title':'QTL examples','qex.hint':'(click any to load into editor)',"
 "'qex.cat.db':'Databases',"
 "'qex.cat.stable':'STables (vtables + child)',"
 "'qex.cat.group':'Groups & devices',"
 "'qex.cat.ddl':'Tables — DDL',"
 "'qex.cat.select':'SELECT & filter',"
 "'qex.cat.agg':'Aggregates',"
 "'qex.cat.win':'Time-bucket / window',"
 "'qex.cat.join':'Join / latest-on',"
 "'qex.cat.rbac':'Users & RBAC',"
 "'qex.cat.tmq':'TMQ (pub/sub)',"
 "'qex.cat.udf':'UDF',"
 "'qex.cat.export':'Export',"
 "'qex.cat.cluster_admin':'Cluster admin',"
 "'qex.cat.http':'HTTP / management endpoints',"
 "'qex.cat.cluster':'Cluster catalog',"
 "'btn.run':'Run','btn.reload':'Reload tree',"
 "'status.ready':'ready','status.loading':'loading',"
 "'status.loaded':'loaded · Ctrl+Enter to run',"
 "'status.running':'running…','status.empty':'empty query',"
 "'status.net_err':'network error','status.unreach':'unreachable',"
 "'common.loading':'loading…','common.no_tables':'no tables',"
 "'common.no_rows':'no rows','common.rows':'row(s)',"
 "'common.truncated':'truncated at 1000','common.this':'this',"
 "'common.server_ms':'server','common.wire_ms':'wire',"
 "'common.tables':'Tables','common.groups':'Groups','common.devices':'Devices',"
 "'common.click_load':'click to load',"
 "'db.host':'host','db.path':'path','db.disk':'disk',"
 "'db.uptime':'up','db.tables':'tables',"
 "'tree.btn_newdb':'+ DB','tree.btn_newgrp':'+ Group',"
 "'tree.btn_newtbl':'+ Table',"
 "'tree.btn_reload':'↻',"
 "'tree.drop_confirm':'Drop',"
 "'tree.drop_ok':'Dropped',"
 "'tree.drop_fail':'Drop failed',"
 "'tree.databases':'Databases','tree.nodb':'(no database)',"
 "'tree.nogrp':'(no group)','tree.no_dbs':'no databases',"
 "'tree.no_grps':'no groups','tree.no_devs':'no devices',"
 "'tree.lbl_groups':'groups','tree.lbl_devs':'devices',"
 "'tree.vtables':'VTables','tree.ptables':'PTables',"
 "'tree.lbl_vtables':'vtables','tree.lbl_ptables':'ptables',"
 "'tree.lbl_cols':'cols','tree.no_vtables':'no vtables',"
 "'tree.no_ptables':'no ptables',"
 "'tree.sysdb_lock':'system · read-only',"
 "'modal.new_database':'New database',"
 "'modal.db_name':'Database name (letters / digits / underscore)',"
 "'modal.db_desc':'Description (optional)',"
 "'modal.new_group':'New group',"
 "'modal.group_name':'Group name',"
 "'modal.group_db':'Parent database (optional)',"
 "'modal.new_table':'New table',"
 "'modal.table_sql':'CREATE TABLE statement',"
 "'modal.ok':'Create','modal.cancel':'Cancel',"
 "'ident.host':'host',"
 "'ident.pid':'pid','ident.uptime':'uptime','ident.crc':'crc32c impl',"
 "'ident.crcv':'hardware-dispatched',"
 "'mode.stand':'STANDALONE','mode.cluster':'CLUSTER',"
 "'mode.nodes':'nodes','mode.alive':'alive','status.ok':'ok',"
 "'cmode.local':'local_id','cmode.rate':'cluster write rate',"
 "'cmode.rpsec':'rows/sec','cmode.ema':'ema_local',"
 "'footer.poll':'polling /health + /metrics every 2s · last refresh',"
 "'ev.auth_add':'login(s)','ev.deny_add':'auth denial(s)',"
 "'ev.flush_add':'flush(es)','ev.qerr_add':'query error(s)',"
 "'ev.repfail_add':'replicate fail(s)','ev.dial_add':'peer conn(s) dialled',"
 "'tree.error':'error'"
"},"
"zh:{"
 "'sec.rates':'实时速率',"
 "'sec.perf':'性能',"
 "'sec.counters':'计数器',"
 "'sec.topo':'集群拓扑',"
 "'sec.console':'查询控制台',"
 "'sec.console_hint':'Ctrl+Enter 运行 · 双击左侧表名快速查询',"
 "'sec.events':'事件（最近 50 条）',"
 "'sec.ident':'服务器信息',"
 "'card.qps':'每秒查询','card.rps':'每秒写入行',"
 "'perf.mem':'内存表行数','perf.disk':'磁盘字节',"
 "'perf.flush':'每分钟 Flush','perf.compact':'每分钟 Compaction',"
 "'perf.bloom':'Bloom 跳过率','perf.alive':'存活节点数',"
 "'cnt.active':'活跃连接','cnt.qtot':'查询总数',"
 "'cnt.rwrt':'已写入行','cnt.bwrt':'已写入字节',"
 "'cnt.qerr':'查询错误','cnt.flush':'Flush 次数',"
 "'cnt.bloom':'Bloom 跳过','cnt.auth_ok':'认证登录',"
 "'cnt.auth_no':'认证拒绝','cnt.conns_life':'累计连接',"
 "'cnt.rep_sent':'复制发送','cnt.rep_ack':'复制 ACK',"
 "'cnt.rep_fail':'复制失败','cnt.dial':'对端连接建立',"
 "'cnt.stats_hit':'聚合 stats 命中','cnt.stats_miss':'聚合 stats 未命中',"
 "'thead.id':'ID','thead.addr':'地址','thead.state':'状态','thead.role':'角色',"
 "'thead.disk':'磁盘 剩余/总量','thead.used':'使用率',"
 "'thead.vn':'VN 权重','thead.uptime':'运行时长','thead.hb':'最近心跳',"
 "'role.master':'主节点','role.data':'数据节点',"
 "'topo.roles_fmt':'主节点 {m} · 数据节点 {d}',"
 "'topo.raft_off':'Raft：未启用（fanout 模式）',"
 "'topo.raft_state_fmt':'Raft：{role} · term {term} · leader {leader} · "
    "commit {commit} · applied {applied} · last {last}',"
 "'topo.leader_badge':'主节点',"
 "'qex.title':'QTL 示例','qex.hint':'（点击卡片加载到编辑器）',"
 "'qex.cat.db':'数据库',"
 "'qex.cat.stable':'超级表 (STable / 子表)',"
 "'qex.cat.group':'分组与设备',"
 "'qex.cat.ddl':'普通表 — DDL',"
 "'qex.cat.select':'SELECT 与过滤',"
 "'qex.cat.agg':'聚合函数',"
 "'qex.cat.win':'时间桶 / 窗口',"
 "'qex.cat.join':'Join / Latest-On',"
 "'qex.cat.rbac':'用户与 RBAC',"
 "'qex.cat.tmq':'TMQ 消息（发布/订阅）',"
 "'qex.cat.udf':'UDF 用户函数',"
 "'qex.cat.export':'导出',"
 "'qex.cat.cluster_admin':'集群管理（Raft 成员变更）',"
 "'qex.cat.http':'HTTP / 管理端点',"
 "'qex.cat.cluster':'集群目录',"
 "'btn.run':'运行','btn.reload':'刷新树',"
 "'status.ready':'就绪','status.loading':'加载中',"
 "'status.loaded':'已加载 · Ctrl+Enter 运行',"
 "'status.running':'运行中…','status.empty':'空查询',"
 "'status.net_err':'网络错误','status.unreach':'不可达',"
 "'common.loading':'加载中…','common.no_tables':'暂无数据表',"
 "'common.no_rows':'无数据','common.rows':'行',"
 "'common.truncated':'已截断至 1000 条','common.this':'本机',"
 "'common.server_ms':'服务端','common.wire_ms':'网络',"
 "'common.tables':'数据表','common.groups':'分组','common.devices':'设备',"
 "'common.click_load':'点击加载',"
 "'db.host':'主机','db.path':'路径','db.disk':'磁盘',"
 "'db.uptime':'运行','db.tables':'表',"
 "'tree.btn_newdb':'+ 数据库','tree.btn_newgrp':'+ 分组',"
 "'tree.btn_newtbl':'+ 建表',"
 "'tree.btn_reload':'↻',"
 "'tree.drop_confirm':'删除',"
 "'tree.drop_ok':'已删除',"
 "'tree.drop_fail':'删除失败',"
 "'tree.databases':'数据库','tree.nodb':'（未归属数据库）',"
 "'tree.nogrp':'（未归属分组）','tree.no_dbs':'暂无数据库',"
 "'tree.no_grps':'暂无分组','tree.no_devs':'暂无设备',"
 "'tree.lbl_groups':'个分组','tree.lbl_devs':'个设备',"
 "'tree.vtables':'虚拟表（VTable）','tree.ptables':'物理表（PTable）',"
 "'tree.lbl_vtables':'个虚表','tree.lbl_ptables':'个物理表',"
 "'tree.lbl_cols':'列','tree.no_vtables':'暂无虚拟表',"
 "'tree.no_ptables':'暂无物理表',"
 "'tree.sysdb_lock':'系统数据库 · 只读',"
 "'modal.new_database':'创建数据库',"
 "'modal.db_name':'数据库名（字母 / 数字 / 下划线）',"
 "'modal.db_desc':'描述（可选）',"
 "'modal.new_group':'创建分组',"
 "'modal.group_name':'分组名',"
 "'modal.group_db':'所属数据库（可选）',"
 "'modal.new_table':'创建表',"
 "'modal.table_sql':'CREATE TABLE 语句',"
 "'modal.ok':'创建','modal.cancel':'取消',"
 "'ident.host':'主机',"
 "'ident.pid':'PID','ident.uptime':'运行时长','ident.crc':'CRC32C 实现',"
 "'ident.crcv':'硬件加速',"
 "'mode.stand':'单机','mode.cluster':'集群',"
 "'mode.nodes':'节点','mode.alive':'存活','status.ok':'正常',"
 "'cmode.local':'本机 ID','cmode.rate':'集群写入速率',"
 "'cmode.rpsec':'行/秒','cmode.ema':'本机 EMA',"
 "'footer.poll':'每 2 秒轮询 /health + /metrics · 最后刷新',"
 "'ev.auth_add':'次登录','ev.deny_add':'次认证拒绝',"
 "'ev.flush_add':'次 flush','ev.qerr_add':'次查询错误',"
 "'ev.repfail_add':'次复制失败','ev.dial_add':'次对端新连接',"
 "'tree.error':'错误'"
"}};"
"let LANG=localStorage.getItem('qengine.lang');"
"if(!LANG)LANG=(navigator.language||'en').toLowerCase().startsWith('zh')"
"         ?'zh':'en';"
"if(!I18N[LANG])LANG='en';"
"function t(k){const d=I18N[LANG];return (d&&d[k])||I18N.en[k]||k;}"
"function applyStaticI18n(){"
"document.querySelectorAll('[data-i18n]').forEach(el=>{"
"el.textContent=t(el.getAttribute('data-i18n'));});"
"document.documentElement.lang=(LANG==='zh')?'zh-CN':'en';"
"document.getElementById('lang_en').classList.toggle('on',LANG==='en');"
"document.getElementById('lang_zh').classList.toggle('on',LANG==='zh');}"
"function setLang(l){if(!I18N[l]||l===LANG)return;LANG=l;"
"localStorage.setItem('qengine.lang',l);applyStaticI18n();refreshDynamic();}"

/* Counter cards keyed by i18n so switching language is a re-render
 * instead of a new tab reload. */
"const CARDS=["
"['qengine_connections_active','cnt.active',''],"
"['qengine_queries_total','cnt.qtot',''],"
"['qengine_rows_written_total','cnt.rwrt',''],"
"['qengine_bytes_written_total','cnt.bwrt','B'],"
"['qengine_query_errors_total','cnt.qerr',''],"
"['qengine_flushes_total','cnt.flush',''],"
"['qengine_bloom_skips_total','cnt.bloom',''],"
"['qengine_auth_logins_total','cnt.auth_ok',''],"
"['qengine_auth_denied_total','cnt.auth_no',''],"
"['qengine_connections_total','cnt.conns_life',''],"
"['qengine_replicate_sent_total','cnt.rep_sent',''],"
"['qengine_replicate_ack_total','cnt.rep_ack',''],"
"['qengine_replicate_fail_total','cnt.rep_fail',''],"
"['qengine_replica_dial_total','cnt.dial',''],"
"['qengine_agg_stats_hit_total','cnt.stats_hit',''],"
"['qengine_agg_stats_miss_total','cnt.stats_miss','']"
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
"const state={qHist:[],wHist:[],prev:null,events:[],"
"lastCBody:'',lastCKey:'',lastM:null,lastCluster:null,lastTree:null,"
"raftLeader:'',raftSelf:'',raftRole:''};"
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
" document.getElementById('st').textContent=t('status.ok');"
" document.getElementById('st').className='badge';"
" document.getElementById('i_host').textContent=location.host;"
" document.getElementById('i_pid').textContent=h.pid;"
" document.getElementById('i_up').textContent=fmtSec(h.uptime_s);"
" document.getElementById('host').textContent=location.host;"
" document.getElementById('uptime').textContent="
"   t('ident.uptime')+' '+fmtSec(h.uptime_s);"
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
"  document.getElementById('rwmeta').textContent="
"    (LANG==='zh'?'平均批 '+Math.round(avgB)+' 行'"
"               :'avg batch '+Math.round(avgB)+' rows');"
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
"  if(dAuth>0)pushEv('auth',`+${dAuth} `+t('ev.auth_add'));"
"  const dDeny=(m.qengine_auth_denied_total||0)-(pm.qengine_auth_denied_total||0);"
"  if(dDeny>0)pushEv('deny',`+${dDeny} `+t('ev.deny_add'));"
"  if(dFl>0)pushEv('flush',`+${dFl} `+t('ev.flush_add'));"
"  const dErr=(m.qengine_query_errors_total||0)-(pm.qengine_query_errors_total||0);"
"  if(dErr>0)pushEv('err',`+${dErr} `+t('ev.qerr_add'));"
"  const dRF=(m.qengine_replicate_fail_total||0)-(pm.qengine_replicate_fail_total||0);"
"  if(dRF>0)pushEv('err',`+${dRF} `+t('ev.repfail_add'));"
"  const dDial=(m.qengine_replica_dial_total||0)-(pm.qengine_replica_dial_total||0);"
"  if(dDial>0)pushEv('info',`+${dDial} `+t('ev.dial_add'));}"
" state.prev={ts:now,m};state.lastM=m;"
" document.getElementById('pm').textContent=fmt(m.qengine_memtable_rows||0,'');"
" document.getElementById('pd').textContent=fmtBytes(m.qengine_disk_bytes||0);"
" document.getElementById('pa').textContent="
"   (m.qengine_cluster_nodes_alive!=null?m.qengine_cluster_nodes_alive:'-');"
" renderCounters(m);"
" document.getElementById('t').textContent=new Date().toLocaleTimeString();"
"}catch(e){"
" document.getElementById('st').textContent=t('status.unreach');"
" document.getElementById('st').className='badge bad';}}"

/* Re-render counter grid with current language. */
"function renderCounters(m){"
"if(!m)m=(state.prev?state.prev.m:{})||{};"
"document.getElementById('g').innerHTML=CARDS.map(c=>{"
" const v=m[c[0]]??0;"
" return `<div class=card><div class=k>${esc(t(c[1]))}</div>"
"<div class=v>${fmt(v,c[2])}</div></div>`;}).join('');}"
"async function tickCluster(){try{"
" const c=await (await fetch('/cluster')).json();"
" const ab=c.autobalance||{};"
" const abByID={};(ab.nodes||[]).forEach(n=>abByID[n.id]=n);"
" const isStandalone=c.mode==='standalone';"
" const nodes=c.nodes||[];"
" const aliveCount=nodes.filter(n=>(n.state||'ALIVE')==='ALIVE').length;"
" const mb=document.getElementById('modebadge');"
" if(isStandalone){mb.textContent=t('mode.stand');mb.className='badge mode standalone';}"
" else{mb.textContent=t('mode.cluster')+' · '+aliveCount+'/'"
"      +nodes.length+' '+t('mode.nodes');"
"      mb.className='badge mode';}"
" let clusterWrites=0;(ab.nodes||[]).forEach(n=>{clusterWrites+=(n.writes_sec||0);});"
" state.lastCluster=c;"
" document.getElementById('cmode').innerHTML="
"   esc(t('cmode.local'))+': '+(c.local_id||0)"
"   +(isStandalone?'':('   ·   '+esc(t('cmode.rate'))+': '+clusterWrites+' '+esc(t('cmode.rpsec'))))"
"   +(ab.ema_writes_sec!=null?('   ·   '+esc(t('cmode.ema'))+': '+ab.ema_writes_sec.toFixed(1)):'');"
/* Role tally bar — Masters N · Data M.  Sourced from n.role which the
 * /cluster JSON now carries per-node (master or data; default master
 * preserves back-compat with older peers that didn't advertise). */
" const mCnt=nodes.filter(n=>((n.role||'master')==='master')).length;"
" const dCnt=nodes.length-mCnt;"
" document.getElementById('rolebar').textContent="
"   t('topo.roles_fmt').replace('{m}',mCnt).replace('{d}',dCnt);"
/* Build a cheap key to detect cluster-level churn so we can skip the
 * innerHTML write when nothing visible changed (biggest render cost). */
/* Keep this cache key timer-free — hb_age_ms / known_for_s advance
 * every poll tick, so including them forces a full innerHTML rewrite
 * every 3 s.  The visible result was a cluster table that looked
 * like it "switched" between nodes on every refresh.  Include only
 * real state signals (id / alive-state / role / disk pct / language
 * / raft leader id); the hb + uptime columns re-paint within the
 * cached rows via DOM text replacement on the last-painted row — if
 * they drift we're OK, and if real state changes the whole table
 * rebuilds at that moment. */
" const key=nodes.map(n=>n.id+':'+n.state+':'+(n.role||'')).join('|')"
"   +'#'+(c.local&&c.local.disk?c.local.disk.used_x10:'-')+'#'+LANG"
"   +'#leader='+(state.raftLeader||'');"
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
"    :(abStorage?(esc(t('thead.used'))+' '+fmtBytes(abStorage))"
"               :(isStandalone?'-':esc(t('mode.nodes'))));"
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
"  const badge=isLocal?' <span class=pill ok style=\"margin-left:6px\">'"
"    +esc(t('common.this'))+'</span>':'';"
"  const role=(n.role||'master');"
"  const roleClass=(role==='master'?'ok':'warn');"
"  const roleLabel=(role==='master'?t('role.master'):t('role.data'));"
"  const isRaftLeader=(state.raftLeader&&String(n.id)===state.raftLeader);"
"  const leaderPill=isRaftLeader?"
"    ` <span class=\"pill ok\" title=raft>👑 ${esc(t('topo.leader_badge'))}</span>`:'';"
"  return `<tr><td>${n.id}${badge}${leaderPill}</td><td>${n.addr||'-'}</td>"
"<td><span class=\"pill ${roleClass}\">${esc(roleLabel)}</span></td>"
"<td><span class=\"pill ${s}\">${(n.state||'ALIVE')}</span></td>"
"<td>${cap}</td><td class=val>${usedCell}</td>"
"<td class=val>${vn}</td>"
"<td class=val>${up}</td>"
"<td class=val>${hbCell}</td></tr>`;}).join('');"
" document.getElementById('cbody').innerHTML=rows||"
"   '<tr><td colspan=9>'+esc(t('common.no_rows'))+'</td></tr>';"
"}catch(e){"
" document.getElementById('cbody').innerHTML="
"   '<tr><td colspan=9 class=sub>/cluster '+esc(t('status.unreach'))"
"   +': '+e.message+'</td></tr>';}}"
"tick();setInterval(tick,2000);"
"tickCluster();setInterval(tickCluster,3000);"

/* Raft self-state tick.  Hits /raft on *this* node once per 2 s and
 * paints the one-line status bar above the topology table plus a
 * "leader" pill on whichever row matches leader_id.  /raft returns
 * {} when raft isn't bound (TSDB_CONSENSUS unset or role=data), in
 * which case we show the fallback "Raft: off" string. */
"async function tickRaft(){"
"try{const r=await fetch('/raft');const j=await r.json();"
"const bar=document.getElementById('raftbar');"
"if(!j||!j.self_id){"
" bar.textContent=t('topo.raft_off');"
" state.raftLeader='';state.raftSelf='';"
" return;"
"}"
"state.raftLeader=String(j.leader_id||'');"
"state.raftSelf=String(j.self_id||'');"
"state.raftRole=j.role||'-';"
"bar.textContent=t('topo.raft_state_fmt')"
"  .replace('{role}',j.role||'-')"
"  .replace('{term}',j.current_term||0)"
"  .replace('{leader}',j.leader_id||'-')"
"  .replace('{commit}',j.commit_index||0)"
"  .replace('{applied}',j.last_applied||0)"
"  .replace('{last}',j.last_index||0);"
"}catch(e){}}"
"tickRaft();setInterval(tickRaft,2000);"

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
"el.innerHTML='<div class=empty>'+esc(t('common.loading'))+'</div>';"
"try{const tr=await (await fetch('/tree')).json();"
"state.lastTree=tr;"
"renderTree();"
"}catch(e){document.getElementById('tree').innerHTML="
"'<div class=empty>'+esc(t('tree.error'))+': '+esc(e.message)+'</div>';}}"

/* Render tree from cached state.lastTree — 4-level hierarchy:
 *   Database → Group → VTable (super-table) → PTable (child table).
 * Legacy catalog Groups that own IoT Devices (no VTables) still render
 * their device list inside the group.  Orphan tables (not bound to any
 * VTable) land in a flat "Tables" section at the bottom. */
"function renderTree(){"
"const el=document.getElementById('tree');"
"const tr=state.lastTree;"
"if(!tr){el.innerHTML='<div class=empty>'+esc(t('common.loading'))+'</div>';return;}"
"const dbs=tr.databases||[];"
"const grs=tr.groups||[];"
"const dvs=tr.devices||[];"
"const tbs=tr.tables||[];"
"const vts=tr.vtables||[];"
"const pts=tr.ptables||[];"
"let h='';"
/* ---- Header card ---- */
"const dbi=tr.db||{};"
"const dbnm=esc(dbi.name||'-');"
"const dbpath=esc(dbi.path||'');"
"const dbhost=esc(dbi.host||'');"
"const dbUp=dbi.uptime_s?fmtSec(dbi.uptime_s):'-';"
"const dbDisk=dbi.disk_bytes?fmtBytes(dbi.disk_bytes):'-';"
"h+='<div class=dbhdr>';"
"h+=`<div class=dbname><span class=ic>NODE</span><span>${dbnm}</span></div>`;"
"h+='<div class=dbmeta>'+esc(t('db.host'))+': '+dbhost"
"  +' · '+esc(t('db.path'))+': '+dbpath+'</div>';"
"h+='<div class=dbmeta>'+esc(t('db.disk'))+': '+dbDisk"
"  +' · '+esc(t('db.uptime'))+': '+dbUp"
"  +' · '+esc(t('db.tables'))+': '+tbs.length+'</div>';"
"h+='<div class=tbar>"
"<button id=btn_newdb title=\"CREATE DATABASE\">'+esc(t('tree.btn_newdb'))+'</button>"
"<button id=btn_newgrp title=\"CREATE GROUP\">'+esc(t('tree.btn_newgrp'))+'</button>"
"<button id=btn_newtbl title=\"CREATE TABLE\">'+esc(t('tree.btn_newtbl'))+'</button>"
"<button id=btn_rld title=\"reload\">'+esc(t('tree.btn_reload'))+'</button>"
"</div>';"
"h+='</div>';"
/* ---- Buckets ----
 * 4-level hierarchy demands two VTable indexes:
 *   vtByDb      — DB roll-up counter
 *   vtByDbGrp   — exact key used when rendering a group's VTables
 * Group keys collapse "" → "(no group)" so ungrouped VTables surface. */
"const grpsByDb={};"
"grs.forEach(g=>{const k=g.database||'';(grpsByDb[k]=grpsByDb[k]||[]).push(g);});"
"const devsByGrp={};"
"dvs.forEach(d=>{const k=d.group||'';(devsByGrp[k]=devsByGrp[k]||[]).push(d);});"
"const vtByDb={};"
"const vtByDbGrp={};"
"vts.forEach(v=>{"
"  const dk=v.database||'';"
"  (vtByDb[dk]=vtByDb[dk]||[]).push(v);"
"  const k=dk+'\\u0001'+(v.group||'');"
"  (vtByDbGrp[k]=vtByDbGrp[k]||[]).push(v);"
"});"
"const ptByVt={};"
"pts.forEach(p=>{const k=p.vtable||'';(ptByVt[k]=ptByVt[k]||[]).push(p);});"
/* ---- Databases section ---- */
"h+='<div class=grp><span>'+esc(t('tree.databases'))+' ('+(dbs.length+((grpsByDb['']||[]).length||(vtByDb['']||[]).length?1:0))+')</span></div>';"
"function renderPtables(vtableName){"
" const ps=ptByVt[vtableName]||[];"
" if(!ps.length)return '<div class=empty>'+esc(t('tree.no_ptables'))+'</div>';"
" let ph='';"
" ps.forEach(p=>{"
"   ph+=`<div class=\"item dev\" data-dev='${esc(p.name)}' title='${esc(p.name)}'>`"
"     +`<span class=nm>${esc(p.name)}</span>`"
"     +`<span class=x data-dropptbl='${esc(p.name)}' title='DROP PTABLE'>✕</span>`"
"     +'</div>';});"
" return ph;"
"}"
"function renderVtable(v,idx){"
" const ps=ptByVt[v.name]||[];"
" let vh=`<div class=\"item gr node\" data-nodeid=vt_${idx} title='${esc(v.name)}'>`"
"   +'<span class=ic>V</span>'"
"   +`<span class=nm>${esc(v.name)}</span>`"
"   +`<span class=sz>${v.ncols} ${esc(t('tree.lbl_cols'))} · ${ps.length} ${esc(t('tree.lbl_ptables'))}</span>`"
"   +`<span class=x data-dropvt='${esc(v.name)}' title='DROP VTABLE'>✕</span>`"
"   +'</div><div class=kids>';"
" vh+=renderPtables(v.name);"
" vh+='</div>';"
" return vh;"
"}"
/* Group node — VTables (+ legacy Devices) inside one DB.
 * If gName is empty string we render the "(no group)" bucket without
 * a DROP handle (nothing to drop — it's a logical fallback). */
"function renderDbGroup(dbName,gName,idx){"
" const vs=vtByDbGrp[(dbName||'')+'\\u0001'+(gName||'')]||[];"
" const devs=gName?(devsByGrp[gName]||[]):[];"
" const tot=vs.length+devs.length;"
" let gh=`<div class=\"item gr node\" data-nodeid=grp_${idx}>`"
"   +'<span class=ic>G</span>'"
"   +`<span class=nm>${esc(gName||t('tree.nogrp'))}</span>`"
"   +`<span class=sz>${vs.length} ${esc(t('tree.lbl_vtables'))} · ${devs.length} ${esc(t('tree.lbl_devs'))}</span>`"
"   +(gName?`<span class=x data-dropgrp='${esc(gName)}' title='DROP GROUP'>✕</span>`:'')"
"   +'</div><div class=kids>';"
" if(!tot){gh+='<div class=empty>'+esc(t('tree.no_vtables'))+'</div>';}"
" if(vs.length){"
"   gh+='<div class=grp><span>'+esc(t('tree.vtables'))+' ('+vs.length+')</span></div>';"
"   vs.forEach((v,vi)=>{gh+=renderVtable(v,idx*1000+vi);});"
" }"
" if(devs.length){"
"   gh+='<div class=grp><span>'+esc(t('common.devices'))+' ('+devs.length+')</span></div>';"
"   devs.forEach(d=>{"
"     gh+=`<div class=\"item dev\" data-dev='${esc(d.name)}' data-devgrp='${esc(d.group)}'>`"
"       +`<span class=nm>${esc(d.name)}</span>`"
"       +`<span class=x data-dropdev='${esc(d.group)}|${esc(d.name)}' title='DROP DEVICE'>✕</span>`"
"       +'</div>';});"
" }"
" gh+='</div>';"
" return gh;"
"}"
"function renderDb(dbName,desc,idx,isOrphan,isProtected){"
" const vs=vtByDb[dbName]||[];"
" const gs=grpsByDb[dbName]||[];"
" const grpNames=new Set();"
" gs.forEach(g=>grpNames.add(g.name));"
" vs.forEach(v=>grpNames.add(v.group||''));"
" const allGrps=[...grpNames].filter(n=>n).sort();"
" const hasUngrouped=grpNames.has('')||grpNames.has(undefined);"
" let dh=`<div class=\"item db node\" data-nodeid=db_${idx}>`"
"   +'<span class=ic>DB</span>'"
"   +`<span class=nm>${esc(dbName||t('tree.nodb'))}</span>`"
"   +(isProtected?`<span class=sz title='${esc(t('tree.sysdb_lock'))}'>🔒</span>`"
"                :`<span class=sz>${allGrps.length+(hasUngrouped?1:0)} ${esc(t('tree.lbl_groups'))}"
"                   · ${vs.length} ${esc(t('tree.lbl_vtables'))}</span>`)"
"   +((isOrphan||isProtected)?''"
"     :`<span class=x data-dropdb='${esc(dbName)}' title='DROP DATABASE'>✕</span>`)"
"   +'</div><div class=kids>';"
/* sysdb exposes virtual shortcuts — click to populate editor. */
" if(isProtected){"
"   dh+='<div class=grp><span>'+esc(t('sec.ident'))+'</span></div>';"
"   const shortcuts=["
"     ['users','LIST USERS'],"
"     ['groups','LIST GROUPS'],"
"     ['cluster','-- open /cluster in a new tab for full topology'],"
"     ['load','-- open /metrics for Prometheus-format counters']"
"   ];"
"   shortcuts.forEach(s=>{"
"     dh+=`<div class=\"item dev\" data-sqlshortcut='${esc(s[1])}'>`"
"       +`<span class=nm>${esc(s[0])}</span></div>`;});"
" }else{"
"   dh+='<div class=grp><span>'+esc(t('common.groups'))+' ('+(allGrps.length+(hasUngrouped?1:0))+')</span></div>';"
"   if(!allGrps.length && !hasUngrouped)"
"     dh+='<div class=empty>'+esc(t('tree.no_grps'))+'</div>';"
"   allGrps.forEach((gn,gi)=>{dh+=renderDbGroup(dbName,gn,idx*10000+gi);});"
"   if(hasUngrouped)dh+=renderDbGroup(dbName,'',idx*10000+9999);"
" }"
" dh+='</div>';"
" return dh;"
"}"
"if(!dbs.length && !((grpsByDb['']||[]).length) && !((vtByDb['']||[]).length) && !tbs.length)"
"h+='<div class=empty>'+esc(t('tree.no_dbs'))+'</div>';"
"else{"
" dbs.forEach((d,i)=>{h+=renderDb(d.name,d.description,i+1,false,!!d.protected);});"
" if((grpsByDb['']||[]).length || (vtByDb['']||[]).length)"
"   h+=renderDb('',null,0,true,false);"
/* ---- Default-DB bucket ----
 * Tables created via plain CREATE TABLE (no vtable, no group) are not
 * orphans at the node level — the whole node IS a database.  Render
 * them under a single DB-header named after the node's data_dir so
 * the 4-level hierarchy (DB → Group → VTable → PTable) is never
 * broken by a flat list at the bottom.  PTables that already show up
 * under a VTable in the catalog are filtered out so they don't appear
 * twice (once under their VTable and once here). */
" const ptNames=new Set();"
" pts.forEach(p=>ptNames.add(p.name));"
" const orphanTbs=tbs.filter(x=>!ptNames.has(x.name));"
" if(orphanTbs.length){"
"   const ddName=(dbi.name||'default');"
"   let bh=`<div class=\"item db node open\" data-nodeid=db_default>`"
"     +'<span class=ic>DB</span>'"
"     +`<span class=nm>${esc(ddName)}</span>`"
"     +`<span class=sz>${orphanTbs.length} ${esc(t('db.tables'))}</span>`"
"     +'</div><div class=kids>';"
"   bh+='<div class=grp><span>'+esc(t('common.tables'))+' ('+orphanTbs.length+')</span></div>';"
"   orphanTbs.forEach(x=>{"
"     const sz=x.bytes?fmtBytes(x.bytes):'';"
"     bh+=`<div class=item data-tbl='${esc(x.name)}' title='${esc(t('common.click_load'))}'>`"
"       +`<span class=nm>${esc(x.name)}</span>`"
"       +(sz?`<span class=sz>${sz}</span>`:'')"
"       +`<span class=x data-drop='${esc(x.name)}' title='DROP TABLE'>✕</span>`"
"       +'</div>';});"
"   bh+='</div>';"
"   h+=bh;"
" }"
"}"
"el.innerHTML=h;"
/* ---- Wire listeners ---- */
/* Collapsible node toggle */
"el.querySelectorAll('.node').forEach(n=>{"
"n.addEventListener('click',ev=>{"
"if(ev.target.classList.contains('x'))return;"
"n.classList.toggle('open');});});"
"el.querySelectorAll('.item[data-tbl]').forEach(node=>{"
"node.addEventListener('dblclick',()=>{"
"const n=node.dataset.tbl;"
"document.getElementById('qed').value=`SELECT * FROM ${n} LIMIT 1000`;"
"runSql();});});"
"el.querySelectorAll('.item[data-dev]').forEach(node=>{"
"node.addEventListener('dblclick',()=>{"
"const n=node.dataset.dev;"
"document.getElementById('qed').value=`SELECT * FROM ${n} LIMIT 1000`;"
"runSql();});});"
/* DROP TABLE */
"el.querySelectorAll('.x[data-drop]').forEach(x=>{"
"x.addEventListener('click',async ev=>{"
"ev.stopPropagation();"
"const n=x.dataset.drop;"
"if(!confirm(t('tree.drop_confirm')+' \"'+n+'\" ?'))return;"
"await dropAndReload('DROP TABLE '+n,n);});});"
/* DROP GROUP */
"el.querySelectorAll('.x[data-dropgrp]').forEach(x=>{"
"x.addEventListener('click',async ev=>{"
"ev.stopPropagation();"
"const n=x.dataset.dropgrp;"
"if(!confirm(t('tree.drop_confirm')+' GROUP \"'+n+'\" ?'))return;"
"await dropAndReload('DROP GROUP '+n,n);});});"
/* DROP DATABASE */
"el.querySelectorAll('.x[data-dropdb]').forEach(x=>{"
"x.addEventListener('click',async ev=>{"
"ev.stopPropagation();"
"const n=x.dataset.dropdb;"
"if(!confirm(t('tree.drop_confirm')+' DATABASE \"'+n+'\" ?'))return;"
"await dropAndReload('DROP DATABASE '+n,n);});});"
/* DROP DEVICE */
"el.querySelectorAll('.x[data-dropdev]').forEach(x=>{"
"x.addEventListener('click',async ev=>{"
"ev.stopPropagation();"
"const [g,d]=x.dataset.dropdev.split('|');"
"if(!confirm(t('tree.drop_confirm')+' DEVICE \"'+d+'\" ?'))return;"
"await dropAndReload('DROP DEVICE '+d+' IN GROUP '+g,d);});});"
/* DROP VTABLE */
"el.querySelectorAll('.x[data-dropvt]').forEach(x=>{"
"x.addEventListener('click',async ev=>{"
"ev.stopPropagation();"
"const n=x.dataset.dropvt;"
"if(!confirm(t('tree.drop_confirm')+' VTABLE \"'+n+'\" ?'))return;"
"await dropAndReload('DROP VTABLE '+n,n);});});"
/* DROP PTABLE */
"el.querySelectorAll('.x[data-dropptbl]').forEach(x=>{"
"x.addEventListener('click',async ev=>{"
"ev.stopPropagation();"
"const n=x.dataset.dropptbl;"
"if(!confirm(t('tree.drop_confirm')+' PTABLE \"'+n+'\" ?'))return;"
"await dropAndReload('DROP TABLE '+n,n);});});"
/* sysdb virtual shortcut — load SQL into editor */
"el.querySelectorAll('[data-sqlshortcut]').forEach(el=>{"
"el.addEventListener('click',ev=>{"
"ev.stopPropagation();"
"const sql=el.getAttribute('data-sqlshortcut');"
"document.getElementById('qed').value=sql;"
"document.getElementById('qstatus').textContent=t('status.loaded');});});"
/* Toolbar */
"const btnDB=document.getElementById('btn_newdb');"
"if(btnDB)btnDB.addEventListener('click',openNewDatabase);"
"const btnT=document.getElementById('btn_newtbl');"
"if(btnT)btnT.addEventListener('click',openNewTable);"
"const btnG=document.getElementById('btn_newgrp');"
"if(btnG)btnG.addEventListener('click',openNewGroup);"
"const btnR=document.getElementById('btn_rld');"
"if(btnR)btnR.addEventListener('click',loadTree);"
"}"

"async function dropAndReload(sql,label){"
"try{const r=await fetch('/sql',{method:'POST',"
"headers:{'Content-Type':'application/json'},"
"body:JSON.stringify({q:sql})});"
"const j=await r.json();"
"if(j.error){alert(t('tree.drop_fail')+': '+j.error);return;}"
"document.getElementById('qstatus').textContent=t('tree.drop_ok')+': '+label;"
"loadTree();"
"}catch(e){alert(t('tree.drop_fail')+': '+e.message);}}"

/* ---- Modal helpers ---------------------------------------------- */
"function closeModal(){"
"const m=document.getElementById('qengine_modal');"
"if(m)m.remove();}"
"function showModal(title,bodyHtml,onOk){"
"closeModal();"
"const m=document.createElement('div');"
"m.id='qengine_modal';m.className='mask';"
"m.innerHTML=`<div class=box>"
"<h3>${esc(title)}</h3>${bodyHtml}"
"<div class=row>"
"<button class=\"qbtn alt\" id=m_cancel>${esc(t('modal.cancel'))}</button>"
"<button class=qbtn id=m_ok>${esc(t('modal.ok'))}</button>"
"</div></div>`;"
"document.body.appendChild(m);"
"document.getElementById('m_cancel').addEventListener('click',closeModal);"
"document.getElementById('m_ok').addEventListener('click',onOk);"
"m.addEventListener('click',ev=>{if(ev.target===m)closeModal();});"
"setTimeout(()=>{const f=m.querySelector('input,textarea');if(f)f.focus();},10);}"
"async function runSilent(sql){"
"const r=await fetch('/sql',{method:'POST',"
"headers:{'Content-Type':'application/json'},"
"body:JSON.stringify({q:sql})});"
"return r.json();}"
"function openNewDatabase(){"
"showModal(t('modal.new_database'),"
"`<label>${esc(t('modal.db_name'))}</label>"
"<input id=m_in placeholder='iot_core'>`,"
"async()=>{"
"const v=document.getElementById('m_in').value.trim();"
"if(!v)return;"
"const j=await runSilent('CREATE DATABASE '+v);"
"if(j.error){alert(j.error);return;}"
"closeModal();loadTree();"
"document.getElementById('qstatus').textContent='OK: database '+v;"
"});}"
"function openNewGroup(){"
"const dbs=(state.lastTree&&state.lastTree.databases)||[];"
"let dbOpts='<option value=\"\">'+esc(t('tree.nodb'))+'</option>';"
"dbs.forEach(d=>{dbOpts+=`<option value='${esc(d.name)}'>${esc(d.name)}</option>`;});"
"showModal(t('modal.new_group'),"
"`<label>${esc(t('modal.group_name'))}</label>"
"<input id=m_in placeholder='factory_a'>"
"<label>${esc(t('modal.group_db'))}</label>"
"<select id=m_db style='width:100%;padding:6px 8px;border:1px solid var(--bd);"
"border-radius:5px;font-size:12px;box-sizing:border-box'>${dbOpts}</select>`,"
"async()=>{"
"const v=document.getElementById('m_in').value.trim();"
"if(!v)return;"
"const db=document.getElementById('m_db').value.trim();"
"const sql=db?'CREATE GROUP '+v+' IN DATABASE '+db:'CREATE GROUP '+v;"
"const j=await runSilent(sql);"
"if(j.error){alert(j.error);return;}"
"closeModal();loadTree();"
"document.getElementById('qstatus').textContent='OK: group '+v;"
"});}"
"function openNewTable(){"
"showModal(t('modal.new_table'),"
"`<label>${esc(t('modal.table_sql'))}</label>"
"<textarea id=m_in>CREATE TABLE my_table (ts TIMESTAMP, val FLOAT64) TIMESTAMP(ts)</textarea>`,"
"async()=>{"
"const v=document.getElementById('m_in').value.trim();"
"if(!v)return;"
"const j=await runSilent(v);"
"if(j.error){alert(j.error);return;}"
"closeModal();loadTree();"
"document.getElementById('qstatus').textContent='OK: '+"
"((j.rows&&j.rows[0]&&j.rows[0][0])||'table created');"
"});}"
"async function runSql(){"
"const ed=document.getElementById('qed');"
"const btn=document.getElementById('qrun');"
"const stE=document.getElementById('qstatus');"
"const out=document.getElementById('qres');"
"const q=ed.value.trim();"
"if(!q){stE.textContent=t('status.empty');return;}"
"btn.disabled=true;stE.textContent=t('status.running');"
"const t0=performance.now();"
"try{const r=await fetch('/sql',{method:'POST',"
"headers:{'Content-Type':'application/json'},body:JSON.stringify({q})});"
"const j=await r.json();"
"const dt=Math.round(performance.now()-t0);"
"if(j.error){out.innerHTML=`<div class=err>${esc(j.error)}</div>`;"
"stE.textContent=(LANG==='zh'?'错误':'error')+' in '+dt+'ms';return;}"
"const cols=j.cols||[],types=j.types||[],rows=j.rows||[];"
"let h='<table><thead><tr>';"
"cols.forEach((c,i)=>{h+=`<th>${esc(c)}<span class=ty>${esc(types[i]||'')}</span></th>`;});"
"h+='</tr></thead><tbody>';"
"rows.forEach(row=>{h+='<tr>';"
"row.forEach((v,i)=>{h+=fmtCell(v,types[i]);});"
"h+='</tr>';});"
"h+='</tbody></table>';"
"if(!rows.length)h='<div class=empty style=\"padding:14px\">'"
"+esc(t('common.no_rows'))+'</div>';"
"out.innerHTML=h;"
"stE.textContent=j.nrows+' '+t('common.rows')"
"+(j.truncated?' ('+t('common.truncated')+')':'')"
"+' · '+t('common.server_ms')+' '+j.ms+'ms · '"
"+t('common.wire_ms')+' '+dt+'ms';"
"}catch(e){out.innerHTML=`<div class=err>${esc(e.message)}</div>`;"
"stE.textContent=t('status.net_err');}"
"finally{btn.disabled=false;}}"
"document.getElementById('qrun').addEventListener('click',runSql);"
"document.getElementById('qreload').addEventListener('click',loadTree);"
"document.getElementById('qed').addEventListener('keydown',e=>{"
"if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)){e.preventDefault();runSql();}});"
/* ---- QTL examples ---------------------------------------------------
 * Complete reference derived from src/query/ast.h (every QAST_STMT_*
 * kind) plus the aggregate / window / TS functions recognised by
 * is_agg_call and is_window_call.  Each entry carries:
 *   [0] { en, zh }   — short title shown in the card header
 *   [1] 'SQL string' — loaded into the editor on click
 *   [2] { en, zh }   — one-line description below the SQL (optional)
 *
 * Categories follow the i18n dictionary qex.cat.*.  Keep the ordering
 * database → stable/group → ptable → SELECT → aggregate → window →
 * join → RBAC → TMQ → UDF → export → cluster-admin so newcomers see
 * the ddl path first. */
"const QEX=[{c:'qex.cat.db',i:["
"[{en:'create database',zh:'建数据库'},'CREATE DATABASE iot_core',"
"{en:'Create a catalog namespace. Propagated via Raft to every master; broadcast to data nodes on apply.',zh:'建目录命名空间；由 Raft 在主节点提交，落到数据节点。'}],"
"[{en:'create + retention',zh:'建库带保留期'},'CREATE DATABASE logs_30d (retention=\\'30d\\')',"
"{en:'Retention window passed as an option. The retention sweeper enforces it on each table of the DB.',zh:'retention 选项指定 TTL，retention sweeper 会在每个表上触发清理。'}],"
"[{en:'list databases',zh:'列数据库'},'LIST DATABASES',"
"{en:'Returns name, description, retention_ns, created_at, protected flag.',zh:'返回名称、描述、保留期(ns)、创建时间、是否受保护。'}],"
"[{en:'drop database',zh:'删数据库'},'DROP DATABASE iot_core',"
"{en:'Drop an entire database and all its tables. sysdb is protected and cannot be dropped.',zh:'删除整个库及其下所有表；sysdb 为受保护系统库，不可删。'}],"
"]},{c:'qex.cat.stable',i:["
"[{en:'create stable',zh:'建超级表'},'CREATE STABLE meters (ts TIMESTAMP, volt FLOAT64) TAGS (region SYMBOL, unit INT64)',"
"{en:'Super-table template: columns are the value schema; TAGS index slices into lightweight child tables.',zh:'超级表模板：列是量测字段；TAGS 作为索引维度切分出轻量子表。'}],"
"[{en:'create child',zh:'创建子表'},'CREATE TABLE d1 USING meters TAGS (\\'east\\', 42)',"
"{en:'Materialise a child table from a STable with concrete tag values — no per-table column definition needed.',zh:'基于 STable 按具体标签值创建子表，无需再写列定义。'}],"
"[{en:'list vtables',zh:'列超级表'},'LIST VTABLES',"
"{en:'Alias LIST STABLES also works. Returns every super-table (a.k.a. vtable) in the catalog.',zh:'别名 LIST STABLES 亦可。返回目录中全部超级表(vtable)。'}],"
"[{en:'list ptables of stable',zh:'列子表'},'LIST PTABLES USING meters',"
"{en:'Enumerate child tables belonging to a given STable. Omit USING to list every ptable in the catalog.',zh:'枚举某 STable 下的所有子表；不加 USING 则列全部 ptable。'}],"
"[{en:'drop stable',zh:'删超级表'},'DROP STABLE meters',"
"{en:'Drops the STable and every child table that descends from it. Irreversible.',zh:'删除 STable 及全部衍生子表，不可逆。'}],"
"]},{c:'qex.cat.group',i:["
"[{en:'create group',zh:'创建分组'},'CREATE GROUP factory_a (region=\\'us-east-1\\', retention=\\'30d\\')',"
"{en:'Logical grouping with region + retention overrides. Used for multi-tenant labelling & per-region policy.',zh:'逻辑分组，可设 region/retention，供多租户和分区域策略使用。'}],"
"[{en:'create device',zh:'创建设备'},'CREATE DEVICE sensor_001 IN GROUP factory_a (type=\\'temperature\\')',"
"{en:'Register a device inside a group. The type tag is metadata only — data still lands in normal tables.',zh:'在分组中注册设备；type 属性仅为元数据，实际数据仍入表。'}],"
"[{en:'list groups',zh:'列分组'},'LIST GROUPS',"
"{en:'Returns every group across every database with its region, retention, codec_profile, replica_factor.',zh:'跨所有库列分组，返回 region、retention、codec_profile、replica_factor。'}],"
"[{en:'list devices',zh:'列设备'},'LIST DEVICES IN GROUP factory_a',"
"{en:'Scope the list to one group. Omit IN GROUP to enumerate every device cluster-wide.',zh:'限定到某一分组；省略 IN GROUP 则全局枚举。'}],"
"[{en:'drop device',zh:'删设备'},'DROP DEVICE sensor_001 IN GROUP factory_a',"
"{en:'Remove a device entry. Does NOT delete any data already written — use TRUNCATE/DELETE for that.',zh:'仅移除设备元数据，不会清理已写入的数据；如需清数据用 TRUNCATE/DELETE。'}],"
"[{en:'drop group',zh:'删分组'},'DROP GROUP factory_a',"
"{en:'Deletes the group and its device memberships. Tables themselves stay until dropped explicitly.',zh:'删除分组及其设备归属；表本身保留，须显式删除。'}],"
"]},{c:'qex.cat.ddl',i:["
"[{en:'create table',zh:'建表'},'CREATE TABLE sensor (ts TIMESTAMP, dev SYMBOL, v FLOAT64) TIMESTAMP(ts)',"
"{en:'Standard column schema. The TIMESTAMP(col) suffix tells the engine which column is the time axis.',zh:'标准列定义；TIMESTAMP(col) 后缀指明时间轴列，用于分区和时序优化。'}],"
"[{en:'hourly partition',zh:'按小时分区'},'CREATE TABLE sensor_h (ts TIMESTAMP, v INT64) TIMESTAMP(ts) WITH (PARTITION=\\'hour\\')',"
"{en:'Choose hour-level partitioning for sub-day ingest rates. Default is PARTITION=day.',zh:'小时级分区，适合高频写入场景；默认 PARTITION=day。'}],"
"[{en:'block tuning',zh:'块参数'},'CREATE TABLE m (ts TIMESTAMP, v FLOAT64) TIMESTAMP(ts) WITH (BLOCK_POINTS=8192, PARTITION=\\'day\\')',"
"{en:'BLOCK_POINTS controls how many samples per column block. Larger blocks compress better; smaller scans faster.',zh:'BLOCK_POINTS 控制列块点数，越大压缩越好，越小扫描越快。'}],"
"[{en:'list tables',zh:'列表'},'LIST TABLES',"
"{en:'Alias LIST PTABLES. Returns name, parent vtable, tag count, created_at, database, group.',zh:'别名 LIST PTABLES。返回表名、所属 vtable、tag 数、创建时间、库、分组。'}],"
"[{en:'alter add col',zh:'加列'},'ALTER TABLE sensor ADD COLUMN temp FLOAT64',"
"{en:'Append a new column. Pre-existing rows see NULL for this column until new writes land.',zh:'追加新列；已有行在该列取 NULL，直到新写入到来。'}],"
"[{en:'truncate',zh:'清空'},'TRUNCATE TABLE sensor',"
"{en:'Keep schema + symbol table, wipe partition dirs + memtable + WAL. Much faster than DELETE for full-table purge.',zh:'保留 schema 和符号表，清空分区目录、memtable、WAL；比 DELETE 快得多。'}],"
"[{en:'delete range',zh:'按范围删'},'DELETE FROM sensor WHERE ts < \\'2026-01-01\\'',"
"{en:'Partition-aligned delete. Only ts < | <= | > | >= constants allowed (so whole partitions can be dropped cleanly).',zh:'按分区边界删除。只允许 ts 与常量的比较，确保整个分区可以原子丢弃。'}],"
"[{en:'drop table',zh:'删表'},'DROP TABLE sensor',"
"{en:'Irreversibly deletes schema + all data + partition dirs + WAL.',zh:'永久删除 schema、全部数据、分区目录与 WAL。'}],"
"]},{c:'qex.cat.select',i:["
"[{en:'all rows',zh:'全部行'},'SELECT * FROM trades LIMIT 100',"
"{en:'Full-row scan; LIMIT caps memory. Without WHERE, every partition is read — prefer time-range predicates on large tables.',zh:'整行扫描；LIMIT 限制内存。无 WHERE 时会扫全部分区，大表建议加时间谓词。'}],"
"[{en:'project cols',zh:'选列'},'SELECT ts, symbol, price FROM trades LIMIT 100',"
"{en:'Column pruning — only the listed columns are decoded from the columnar store. Huge speedup on wide tables.',zh:'列裁剪，只解码用到的列；宽表可获显著加速。'}],"
"[{en:'where range',zh:'时间范围'},'SELECT * FROM trades WHERE ts BETWEEN \\'2026-01-01\\' AND \\'2026-01-02\\' LIMIT 100',"
"{en:'Partition pruning via time predicate. The engine skips entire partition dirs outside the range.',zh:'时间谓词触发分区裁剪，完全跳过范围外的分区目录。'}],"
"[{en:'where eq',zh:'按值过滤'},'SELECT ts, price FROM trades WHERE symbol = \\'AAPL\\' LIMIT 100',"
"{en:'Equality filter on a SYMBOL column uses the per-partition bloom filter to skip blocks.',zh:'SYMBOL 等值过滤会命中分区布隆过滤器，可跳过大量列块。'}],"
"[{en:'distinct',zh:'去重'},'SELECT DISTINCT symbol FROM trades',"
"{en:'Unique values of a column. Hash-based, runs in a single pass.',zh:'去重查询，基于哈希单遍扫描。'}],"
"[{en:'limit + order',zh:'排序 + 限制'},'SELECT * FROM trades ORDER BY ts DESC LIMIT 10',"
"{en:'Top-N with reverse order. Leverages the time-indexed sort already present on disk.',zh:'Top-N 查询，利用磁盘上已按时间排序的数据反向取 10 行。'}],"
"]},{c:'qex.cat.agg',i:["
"[{en:'count + sum + avg',zh:'计数 & 求和 & 平均'},'SELECT count(*), sum(volume), avg(price) FROM trades',"
"{en:'Three SIMD-accelerated aggregates in one pass. Precomputed per-block stats short-circuit when the whole block qualifies.',zh:'一次扫描同时计算 count/sum/avg，块级预计算在整块命中时直接跳过解码。'}],"
"[{en:'min/max/spread',zh:'最小/最大/极差'},'SELECT min(price), max(price), spread(price) FROM trades',"
"{en:'spread(col) = max - min. Useful for range-based anomaly detection.',zh:'spread = max-min，常用于基于范围的异常检测。'}],"
"[{en:'first & last & twa',zh:'首值/末值/时间加权'},'SELECT first(price), last(price), twa(price) FROM trades',"
"{en:'first()/last() are time-ordered endpoints. twa() is time-weighted average — integrates value over duration.',zh:'first/last 按时间取端点；twa 为时间加权均值，按持续时长加权积分。'}],"
"[{en:'quantiles',zh:'分位数'},'SELECT p50(price), p90(price), p99(price), stddev(price) FROM trades',"
"{en:'p50/p90/p99 use a compact t-digest sketch — tunable accuracy/memory trade-off.',zh:'p50/p90/p99 基于 t-digest 近似，精度与内存可调。'}],"
"[{en:'percentile(col,q)',zh:'任意分位'},'SELECT percentile(price, 0.95) FROM trades',"
"{en:'Arbitrary quantile q ∈ (0,1). Slower than the p50/p90/p99 fast paths.',zh:'任意分位 q ∈ (0,1)，比 p50/p90/p99 稍慢。'}],"
"[{en:'last_row',zh:'最后一行'},'SELECT last_row(price) FROM trades',"
"{en:'Return the latest row — ts, and all columns. Unlike last(price) which returns one value, last_row returns the whole tuple.',zh:'返回最新一行，包含 ts 与全部列；不同于 last(price) 只取单值。'}],"
"[{en:'group by',zh:'分组聚合'},'SELECT symbol, avg(price), count(*) FROM trades GROUP BY symbol',"
"{en:'Hash-based group-by. SIMD-accelerated and parallelised across partitions.',zh:'基于哈希的分组聚合，跨分区并行且 SIMD 加速。'}],"
"[{en:'stddev',zh:'标准差'},'SELECT stddev(price), stddev(volume) FROM trades',"
"{en:'Welford single-pass variance. Identical answer to the two-pass formula but no round-trip over data.',zh:'Welford 单遍求方差，与两遍公式结果一致但只扫一次数据。'}],"
"]},{c:'qex.cat.win',i:["
"[{en:'sample by 1m',zh:'按 1 分钟采样'},'SELECT time_bucket(ts, 1m), avg(price) FROM trades SAMPLE BY 1m LIMIT 60',"
"{en:'Fixed-width downsampling. Buckets align to wall-clock boundaries so adjacent queries stitch cleanly.',zh:'等宽降采样，桶按墙钟对齐，便于相邻查询拼接。'}],"
"[{en:'session window',zh:'会话窗口'},'SELECT count(*) FROM trades SESSION(ts, 3s)',"
"{en:'Gap-based window: a new window starts whenever the idle gap exceeds 3 s.',zh:'基于间隔的窗口：空闲超过 3 秒即开启新窗。'}],"
"[{en:'state window',zh:'状态窗口'},'SELECT count(*) FROM trades STATE_WINDOW(status)',"
"{en:'A window is the maximal run of rows sharing the same value in the named column.',zh:'按指定列的连续相同值切窗，同值视为同一窗口。'}],"
"[{en:'event window',zh:'事件窗口'},'SELECT count(*) FROM trades EVENT_WINDOW(temp > 20, temp < 15)',"
"{en:'Opens on the start predicate, closes on the end predicate. Ideal for threshold-based episodes.',zh:'起始谓词触发开窗、结束谓词触发关窗，适合阈值事件段。'}],"
"[{en:'diff',zh:'差分'},'SELECT ts, diff(price) FROM trades LIMIT 100',"
"{en:'x[i] - x[i-1]. First row is NULL. Useful for rate-of-change or tick-to-tick deltas.',zh:'相邻差分 x[i]-x[i-1]，首行为空，常用于差分和 tick 级变化。'}],"
"[{en:'derivative',zh:'导数'},'SELECT ts, derivative(price) FROM trades LIMIT 100',"
"{en:'Time-normalised diff: (x[i]-x[i-1]) / (ts[i]-ts[i-1]). Units: per-second.',zh:'时间归一化差分 (Δx/Δt)，单位为每秒。'}],"
"[{en:'cumulative sum',zh:'累积求和'},'SELECT ts, csum(volume) FROM trades LIMIT 100',"
"{en:'Running total. Each row emits the sum of every value seen so far.',zh:'累积求和，当前行输出此前所有值的和。'}],"
"[{en:'moving avg',zh:'移动平均'},'SELECT ts, mavg(price, 10) FROM trades LIMIT 100',"
"{en:'Simple moving average over the last N points. NULL for the first N-1 rows.',zh:'简单移动平均，前 N-1 行为 NULL。'}],"
"]},{c:'qex.cat.join',i:["
"[{en:'latest-on',zh:'最新一行'},'SELECT * FROM trades LATEST ON ts',"
"{en:'Return only the most recent row by ts. Equivalent to ORDER BY ts DESC LIMIT 1 but uses the index directly.',zh:'仅返回时间最新的一行，等价于 ORDER BY ts DESC LIMIT 1，但直接走索引。'}],"
"[{en:'latest per group',zh:'按分组取最新'},'SELECT * FROM trades LATEST ON ts PARTITION BY symbol',"
"{en:'Most recent row per partition key. Classic \"last value per sensor\" pattern.',zh:'每个分区键取最新一行，即「每个设备的最新值」。'}],"
"[{en:'asof join',zh:'asof 连接'},'SELECT * FROM trades ASOF JOIN quotes ON sym = sym LIMIT 10',"
"{en:'Left-join each trade with the most recent preceding quote where sym matches. Core time-series correlation primitive.',zh:'左表每行取右表 sym 相等且时间不晚于自身的最近一行；时序关联基础算子。'}],"
"]},{c:'qex.cat.rbac',i:["
"[{en:'create user',zh:'创建用户'},'CREATE USER alice IDENTIFIED BY \\'secret\\' ROLE normal',"
"{en:'Password is hashed (bcrypt) before persist. ROLE is normal | admin.',zh:'密码会被 bcrypt 哈希后持久化；ROLE 可为 normal 或 admin。'}],"
"[{en:'create admin',zh:'创建管理员'},'CREATE USER root IDENTIFIED BY \\'pw\\' ROLE admin',"
"{en:'Admins bypass GRANT checks — any DDL / DML on any table works.',zh:'admin 绕过 GRANT 检查，对任意表的 DDL/DML 均放行。'}],"
"[{en:'grant select',zh:'授读权限'},'GRANT SELECT ON sensor TO alice',"
"{en:'Privilege set: SELECT | INSERT | DDL | ALL. Target: specific table name, or * for cluster-wide.',zh:'权限：SELECT/INSERT/DDL/ALL；目标：表名或 * 代表集群范围。'}],"
"[{en:'grant ddl',zh:'授 DDL'},'GRANT DDL ON * TO alice',"
"{en:'Allows CREATE / DROP / ALTER / TRUNCATE on every table.',zh:'授予对所有表的 CREATE/DROP/ALTER/TRUNCATE 权限。'}],"
"[{en:'grant all',zh:'授所有权限'},'GRANT ALL ON * TO alice',"
"{en:'Equivalent to SELECT + INSERT + DDL on every target.',zh:'等价于 SELECT + INSERT + DDL 合并后作用于所有目标。'}],"
"[{en:'revoke',zh:'撤销'},'REVOKE INSERT ON sensor FROM alice',"
"{en:'Removes a previously-granted privilege. Idempotent — revoking something not granted is a no-op.',zh:'撤销先前授予的权限；对未授予的权限撤销是幂等的空操作。'}],"
"[{en:'change pw',zh:'改密码'},'ALTER USER alice SET PASSWORD \\'newsecret\\'',"
"{en:'Rehashes the new password. Existing sessions stay authenticated until their token expires.',zh:'新密码重新哈希；已建连接持有的 token 过期前仍可用。'}],"
"[{en:'drop user',zh:'删用户'},'DROP USER alice',"
"{en:'Removes the user and all associated grants. Active sessions for this user get disconnected on next request.',zh:'删除用户及其全部授权；活跃会话在下一次请求时断开。'}],"
"[{en:'list users',zh:'列用户'},'LIST USERS',"
"{en:'Returns name, role, created_at. Never emits the password hash.',zh:'返回名、角色、创建时间；不输出密码哈希。'}],"
"]},{c:'qex.cat.tmq',i:["
"[{en:'create cg',zh:'建消费组'},'CREATE CONSUMER GROUP cg ON sensor',"
"{en:'Pub-sub style subscription on a table. Offsets are stored per consumer within the group.',zh:'在表上建发布-订阅消费组；offset 按消费组内每个消费者分别持久化。'}],"
"[{en:'join',zh:'加入'},'JOIN GROUP cg AS consumer_1',"
"{en:'Register a consumer inside the group. After joining, the consumer gets its own offset cursor.',zh:'在消费组中注册消费者；加入后获得独立 offset 游标。'}],"
"[{en:'commit offset',zh:'提交 offset'},'COMMIT OFFSET cg AT 1000 AS consumer_1',"
"{en:'Advance the consumer\\'s committed offset so reconnects resume past this point.',zh:'提交 offset，消费者断线重连将从此 offset 之后继续消费。'}],"
"[{en:'leave',zh:'离开'},'LEAVE GROUP cg AS consumer_1',"
"{en:'Tear down this consumer. Its offset is retained; rejoin under the same id resumes.',zh:'退出此消费者；offset 保留，同 id 重新 JOIN 可续消费。'}],"
"]},{c:'qex.cat.udf',i:["
"[{en:'create udf',zh:'注册 UDF'},'CREATE FUNCTION my_double(FLOAT64) RETURNS FLOAT64 AS \\'/path/to/udf.so\\' SYMBOL \\'double_fn\\'',"
"{en:'Load a C-ABI function from a shared object. SYMBOL is the exported name inside the .so.',zh:'从 .so 加载 C-ABI 函数；SYMBOL 是该库中导出符号名。'}],"
"[{en:'use udf',zh:'调用 UDF'},'SELECT ts, my_double(price) FROM trades LIMIT 10',"
"{en:'Call a registered UDF as if it were a built-in scalar function.',zh:'注册后可像内建标量函数一样使用。'}],"
"[{en:'list udf',zh:'列 UDF'},'LIST FUNCTIONS',"
"{en:'Returns name, arg types, return type, lib path, symbol.',zh:'返回名、参数类型、返回类型、动态库路径与符号。'}],"
"[{en:'drop udf',zh:'删 UDF'},'DROP FUNCTION my_double',"
"{en:'Unregister. Running queries that already resolved the symbol keep working until finish.',zh:'注销函数；已解析符号的运行中查询可继续执行直到完成。'}],"
"]},{c:'qex.cat.export',i:["
"[{en:'parquet',zh:'导出 Parquet'},'EXPORT TABLE trades TO PARQUET \\'/tmp/trades.parquet\\'',"
"{en:'Columnar export to Parquet. Preserves types end-to-end (TIMESTAMP → INT64 nanos, SYMBOL → dictionary).',zh:'列式导出 Parquet，类型完整保留（TIMESTAMP→INT64 纳秒，SYMBOL→字典）。'}],"
"]},{c:'qex.cat.cluster_admin',i:["
"[{en:'list masters',zh:'列主节点'},'LIST MASTERS',"
"{en:'Returns id + addr of every node in the committed Raft config. Falls back to gossip on data nodes.',zh:'返回 Raft 已提交配置中所有主节点的 id 与地址；数据节点走 gossip 回退。'}],"
"[{en:'add master',zh:'增主节点'},'ADD MASTER \\'qengine-cnode-5:28081\\'',"
"{en:'Single-server membership change (§6). Refuses with ERR_BUSY if a prior CONFIG entry is still uncommitted.',zh:'单成员变更(§6)；若上一条 CONFIG 未提交则以 ERR_BUSY 拒绝。'}],"
"[{en:'remove master',zh:'减主节点'},'REMOVE MASTER \\'2873018462191719611\\'',"
"{en:'Takes node id or host:port. Committed REMOVE of self causes the current leader to step down.',zh:'参数接 node id 或 host:port；提交删除自身时当前 leader 会主动 stepdown。'}],"
"]},{c:'qex.cat.http',i:["
"[{en:'GET /health',zh:'健康检查'},'# curl GET http://<node>:28094/health',"
"{en:'Returns uptime + pid. Non-QTL — use curl / kubelet probes to call directly.',zh:'返回运行时长与 pid；非 QTL，直接用 curl 或 kubelet 探针调用。'}],"
"[{en:'GET /metrics',zh:'Prometheus 指标'},'# curl GET http://<node>:28094/metrics',"
"{en:'Prometheus text-format dump of every qengine_* counter (writes, replication, retention, anti-entropy, backup, raft).',zh:'Prometheus 文本格式，输出所有 qengine_* 指标 (写入/复制/保留/补齐/备份/Raft)。'}],"
"[{en:'POST /retention/sweep',zh:'强制 GC 清理'},'# curl -X POST http://<node>:28094/retention/sweep',"
"{en:'Force-run the retention GC once. Responds {\"ok\":true,\"partitions_deleted\":N}.',zh:'强制触发一次保留策略清理；返回 {ok, partitions_deleted}。'}],"
"[{en:'GET /backup',zh:'备份打包下载'},'# curl -o backup.tgz http://<node>:28094/backup',"
"{en:'Streams a gzip tarball of the node\\'s data_dir via chunked transfer. Restore = stop node, untar, start (anti-entropy closes the gap).',zh:'流式下载 data_dir 的 gzip tar；恢复：停节点、解压、启动（anti-entropy 自动补齐差异）。'}],"
"[{en:'GET /tree',zh:'目录树 JSON'},'# curl http://<node>:28094/tree',"
"{en:'JSON snapshot of databases / tables / vtables / groups / devices used by the dashboard tree panel.',zh:'仪表盘左侧目录树所用 JSON 快照：库/表/超级表/分组/设备。'}],"
"[{en:'POST /sql',zh:'HTTP 查询'},'# curl -X POST http://<node>:28094/sql -H \\'Content-Type: application/json\\' -d \\'{\"q\":\"LIST DATABASES\"}\\'',"
"{en:'Dashboard and CLI-less query entry-point. Body is {\"q\":\"...\"}; response is the same JSON shape as this editor shows.',zh:'仪表盘与无 CLI 查询入口；body={q:...}，响应 JSON 结构与此编辑器相同。'}],"
"]}];"
"function renderQex(){"
"const g=document.getElementById('qexgrid');"
"let h='';QEX.forEach(cat=>{"
"h+='<div class=qex-cat><h4>'+esc(t(cat.c))+'</h4>';"
"cat.i.forEach(it=>{"
"const ttl=(it[0]&&it[0][LANG])||(it[0]&&it[0].en)||'';"
"const doc=(it[2]&&it[2][LANG])||(it[2]&&it[2].en)||'';"
"h+=`<div class=qex-item data-sql='${esc(it[1]).replace(/'/g,'&#39;')}' title='${esc(t('common.click_load'))}'>`"
"+`<div class=t>${esc(ttl)}</div>`"
"+`<pre>${esc(it[1])}</pre>`"
"+(doc?`<div class=d>${esc(doc)}</div>`:'')"
"+'</div>';});"
"h+='</div>';});"
"g.innerHTML=h;"
"g.querySelectorAll('.qex-item').forEach(el=>{"
"el.addEventListener('click',()=>{"
"const sql=el.getAttribute('data-sql').replace(/&#39;/g,\"'\");"
"const ed=document.getElementById('qed');"
"ed.value=sql;ed.focus();"
"document.getElementById('qstatus').textContent=t('status.loaded');"
"});});}"

/* Re-render everything derived from a translation: static nodes, the
 * counters grid, cluster table, tree, QTL examples.  Values mixed with
 * labels (uptime "X s", rates "/s", etc.) naturally refresh on the
 * next 2-second tick so we don't touch them here. */
"function refreshDynamic(){"
"applyStaticI18n();"
"renderCounters(state.lastM);"
"state.lastCKey=null;"
"if(state.lastCluster)tickCluster().catch(()=>{});"
"if(state.lastTree)renderTree();"
"renderQex();"
"}"
"document.getElementById('lang_en').addEventListener('click',()=>setLang('en'));"
"document.getElementById('lang_zh').addEventListener('click',()=>setLang('zh'));"
"applyStaticI18n();"
"renderQex();"
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
