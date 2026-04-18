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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

static void *handle_connection(void *arg) {
    int fd = (int)(intptr_t)arg;

    /* Read request headers (stop at \r\n\r\n or buffer full). */
    char req[2048];
    size_t pos = 0;
    int got_end = 0;

    while (pos < sizeof(req) - 1) {
        ssize_t n = read(fd, req + pos, sizeof(req) - 1 - pos);
        if (n <= 0) break;
        pos += (size_t)n;
        req[pos] = '\0';
        if (strstr(req, "\r\n\r\n") || strstr(req, "\n\n")) {
            got_end = 1;
            break;
        }
    }

    if (!got_end) {
        close(fd);
        return NULL;
    }

    /* Parse first line: "GET <path> HTTP/1.x". */
    int route_metrics = 0;
    int route_health  = 0;
    int route_dash    = 0;
    if (strncmp(req, "GET /metrics", 12) == 0)           route_metrics = 1;
    else if (strncmp(req, "GET /health", 11) == 0)       route_health  = 1;
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
         * /health + /metrics from the same origin via XHR every 2 s
         * and displays headline numbers. */
        static const char DASH[] =
"<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
"<title>QEngine — health</title>"
"<style>"
"body{font:14px/1.4 -apple-system,system-ui,sans-serif;color:#222;background:#f6f7f9;margin:0;padding:24px}"
"h1{font-size:18px;margin:0 0 16px;display:flex;align-items:center;gap:8px}"
".badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;color:#fff;background:#22c55e}"
".badge.bad{background:#ef4444}.badge.warn{background:#f59e0b}"
".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}"
".card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px}"
".k{color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.04em}"
".v{font-size:22px;font-weight:600;margin-top:4px;font-variant-numeric:tabular-nums}"
".sub{color:#6b7280;font-size:11px;margin-top:2px}"
"footer{color:#6b7280;font-size:11px;margin-top:24px}"
"</style></head><body>"
"<h1>QEngine <span id=\"st\" class=\"badge\">loading</span></h1>"
"<div class=\"grid\" id=\"g\"></div>"
"<footer>polling /health + /metrics every 2 s  ·  refreshed <span id=\"t\">-</span></footer>"
"<script>"
"const cells=["
"['uptime_s','Uptime','s'],"
"['qengine_connections_active','Active connections',''],"
"['qengine_queries_total','Queries total',''],"
"['qengine_rows_written_total','Rows written',''],"
"['qengine_bytes_written_total','Bytes written','B'],"
"['qengine_query_errors_total','Query errors',''],"
"['qengine_flushes_total','Flushes',''],"
"['qengine_bloom_skips_total','Bloom skips',''],"
"['qengine_auth_denied_total','Auth denied',''],"
"['qengine_auth_logins_total','Auth logins','']"
"];"
"function fmt(v,u){if(v>=1e9)return (v/1e9).toFixed(1)+'G'+u;"
"if(v>=1e6)return (v/1e6).toFixed(1)+'M'+u;if(v>=1e3)return (v/1e3).toFixed(1)+'k'+u;"
"return v+u;}"
"async function tick(){try{"
" const h=await (await fetch('/health')).json();"
" document.getElementById('st').textContent='ok';"
" document.getElementById('st').className='badge';"
" const r=await (await fetch('/metrics')).text();"
" const m={};r.split('\\n').forEach(l=>{"
"  if(l.startsWith('#'))return;const p=l.indexOf(' ');"
"  if(p<0)return;const k=l.substring(0,p);const v=parseFloat(l.substring(p+1));"
"  if(!Number.isNaN(v))m[k]=v;});"
" m.uptime_s=h.uptime_s;"
" document.getElementById('g').innerHTML=cells.map(c=>{"
"  const v=m[c[0]]??0;"
"  return `<div class=card><div class=k>${c[1]}</div><div class=v>${fmt(v,c[2])}</div><div class=sub>${c[0]}</div></div>`;"
" }).join('');"
" document.getElementById('t').textContent=new Date().toLocaleTimeString();"
"}catch(e){"
" document.getElementById('st').textContent='unreachable';"
" document.getElementById('st').className='badge bad';"
"}}"
"tick();setInterval(tick,2000);"
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
    if (listen(fd, 32) < 0) {
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
