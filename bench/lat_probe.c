/* lat_probe.c — serial round-trip latency probe for the qengine wire path.
 *
 * Measures three levels of "responsiveness" against a host-mapped wire port,
 * each as a serial request→response RTT (no pipelining), then reports
 * p50/p90/p99/p99.9/max:
 *
 *   PING    — MSG_PING → PONG.  Pure server event-loop turnaround; no data
 *             touched.  The truest "kernel response speed" number.
 *   WRITE   — single-row MSG_WRITE_BATCH → WRITE_ACK on an existing table.
 *             The append hot path (WAL + memtable + replication fan-out).
 *   QUERY   — "SELECT count(*) FROM <table>" → FIN.  The read/plan path.
 *
 *   gcc -O2 -pthread lat_probe.c tsdb_wire.c -o lat_probe
 *
 * Env (all optional):
 *   LAT_PORT    wire port                     default 29301
 *   LAT_TABLE   existing table to probe       default lt_0
 *   LAT_N       samples per phase             default 2000
 *   LAT_WARM    warmup ops per phase          default 100
 *   LAT_AUTH    "user:pass" for AUTH_LOGIN    default none
 *   LAT_HOST    server host                   default 127.0.0.1
 */
#define _DEFAULT_SOURCE 1
#define _POSIX_C_SOURCE 200809L
#include "tsdb_wire.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>

static char g_host[256] = "127.0.0.1";
static char g_user[64] = {0}, g_pass[64] = {0};
static int  g_auth = 0;

static double now_us(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec * 1e-3;
}

static int node_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET; sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, g_host, &sa.sin_addr) != 1) sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) { close(fd); return -1; }
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct timeval tv = { .tv_sec = 30, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    return fd;
}

static int handshake(tsdb_conn_t *c) {
    uint8_t buf[160], *p = buf;
    *p++ = TSDB_WIRE_VER;
    const char *cid = "latprobe"; *p++ = (uint8_t)strlen(cid);
    memcpy(p, cid, strlen(cid)); p += strlen(cid); *p++ = 0;
    if (frame_send(c, MSG_HELLO, 0, c->next_req_id++, buf, (uint32_t)(p - buf)) < 0) return -1;
    tsdb_msg_t r = {0}; int rc = frame_recv(c, &r);
    int ok = (rc == 0 && r.hdr.type == MSG_HELLO_OK); msg_free(&r);
    if (!ok) return -1;
    if (g_auth) {
        uint8_t a[160], *q = a; size_t ul = strlen(g_user), pl = strlen(g_pass);
        *q++ = (uint8_t)ul; memcpy(q, g_user, ul); q += ul;
        *q++ = (uint8_t)pl; memcpy(q, g_pass, pl); q += pl;
        if (frame_send(c, MSG_AUTH_LOGIN, 0, c->next_req_id++, a, (uint32_t)(q - a)) < 0) return -1;
        tsdb_msg_t ar = {0}; rc = frame_recv(c, &ar);
        ok = (rc == 0 && ar.hdr.type == MSG_AUTH_OK); msg_free(&ar);
        if (!ok) return -1;
    }
    return 0;
}

static int do_ping(tsdb_conn_t *c) {
    if (frame_send(c, MSG_PING, 0, c->next_req_id++, NULL, 0) < 0) return -1;
    tsdb_msg_t m = {0}; int rc = frame_recv(c, &m); msg_free(&m);
    return rc == 0 ? 0 : -1;
}

static size_t build_one_row(uint8_t *buf, const char *tbl, int64_t ts_ns, double val) {
    uint8_t *p = buf; size_t tl = strlen(tbl);
    *p++ = (uint8_t)tl; memcpy(p, tbl, tl); p += tl;
    put_u16(p, 2); p += 2; put_u32(p, 1); p += 4;                 /* 2 cols, 1 row */
    *p++ = 2; memcpy(p, "ts", 2); p += 2; *p++ = 1; *p++ = 0; put_u32(p, 8); p += 4;
    put_u64(p, (uint64_t)ts_ns); p += 8;
    *p++ = 3; memcpy(p, "val", 3); p += 3; *p++ = 3; *p++ = 0; put_u32(p, 8); p += 4;
    uint64_t bits; memcpy(&bits, &val, 8); put_u64(p, bits); p += 8;
    return (size_t)(p - buf);
}

static int do_write(tsdb_conn_t *c, const char *tbl, int64_t ts_ns) {
    uint8_t buf[256];
    size_t n = build_one_row(buf, tbl, ts_ns, 1.0 + (double)(ts_ns & 0xff) * 0.001);
    if (frame_send(c, MSG_WRITE_BATCH, 0, c->next_req_id++, buf, (uint32_t)n) < 0) return -1;
    tsdb_msg_t r = {0}; int rc = frame_recv(c, &r);
    int ok = (rc == 0 && r.hdr.type == MSG_WRITE_ACK); msg_free(&r);
    return ok ? 0 : -1;
}

static int do_query(tsdb_conn_t *c, const char *qtl) {
    size_t ql = strlen(qtl); uint8_t buf[256]; if (ql + 2 > sizeof(buf)) return -1;
    put_u16(buf, (uint16_t)ql); memcpy(buf + 2, qtl, ql);
    if (frame_send(c, MSG_QUERY, 0, c->next_req_id++, buf, (uint32_t)(ql + 2)) < 0) return -1;
    for (;;) {
        tsdb_msg_t m = {0}; int rc = frame_recv(c, &m);
        if (rc < 0) { msg_free(&m); return -1; }
        int fin = (m.hdr.flags & TSDB_FLAG_FIN) != 0, err = (m.hdr.type == MSG_ERROR);
        msg_free(&m);
        if (err) return -1;
        if (fin) break;
    }
    return 0;
}

static int cmp_d(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : (x > y ? 1 : 0);
}
static double pct(const double *a, int n, double p) {
    int i = (int)(p * (n - 1) + 0.5); if (i < 0) i = 0; if (i >= n) i = n - 1; return a[i];
}
static void report(const char *name, double *lat, int n, int errs) {
    if (n <= 0) { printf("  %-6s: no samples (errs=%d)\n", name, errs); return; }
    qsort(lat, (size_t)n, sizeof(double), cmp_d);
    double sum = 0; for (int i = 0; i < n; i++) sum += lat[i];
    printf("  %-6s n=%d errs=%d  mean=%.1f  p50=%.1f  p90=%.1f  p99=%.1f  p99.9=%.1f  max=%.1f  (us)\n",
           name, n, errs, sum / n, pct(lat, n, 0.50), pct(lat, n, 0.90),
           pct(lat, n, 0.99), pct(lat, n, 0.999), lat[n - 1]);
}

int main(void) {
    int port = getenv("LAT_PORT") ? atoi(getenv("LAT_PORT")) : 29301;
    int N    = getenv("LAT_N")    ? atoi(getenv("LAT_N"))    : 2000;
    int WARM = getenv("LAT_WARM") ? atoi(getenv("LAT_WARM")) : 100;
    const char *tbl = getenv("LAT_TABLE") ? getenv("LAT_TABLE") : "lt_0";
    { const char *v = getenv("LAT_HOST"); if (v) snprintf(g_host, sizeof(g_host), "%s", v); }
    { const char *v = getenv("LAT_AUTH"); if (v) { const char *c = strchr(v, ':');
        if (c) { size_t ul = (size_t)(c - v); if (ul < sizeof(g_user)) { memcpy(g_user, v, ul); g_user[ul] = 0; }
                 snprintf(g_pass, sizeof(g_pass), "%s", c + 1); g_auth = 1; } } }

    printf("[lat] host=%s port=%d table=%s N=%d warm=%d auth=%s\n",
           g_host, port, tbl, N, WARM, g_auth ? g_user : "(none)");

    int fd = node_connect(port);
    if (fd < 0) { fprintf(stderr, "connect failed\n"); return 1; }
    tsdb_conn_t c = { .fd = fd, .timeout_ms = 30000, .next_req_id = 1 };
    if (handshake(&c) < 0) { fprintf(stderr, "handshake failed\n"); return 1; }

    char qbuf[128]; snprintf(qbuf, sizeof(qbuf), "SELECT count(*) FROM %s", tbl);

    double *lat = malloc((size_t)N * sizeof(double));
    int64_t base = 2000000000000LL;   /* far-future ts so writes stay monotonic */

    /* PING */
    for (int i = 0; i < WARM; i++) do_ping(&c);
    int errs = 0;
    for (int i = 0; i < N; i++) { double t = now_us(); if (do_ping(&c) == 0) lat[i - errs] = now_us() - t; else errs++; }
    report("PING", lat, N - errs, errs);

    /* WRITE */
    for (int i = 0; i < WARM; i++) do_write(&c, tbl, base + (int64_t)i * 1000000LL);
    errs = 0;
    for (int i = 0; i < N; i++) { int64_t ts = base + (int64_t)(WARM + i) * 1000000LL;
        double t = now_us(); if (do_write(&c, tbl, ts) == 0) lat[i - errs] = now_us() - t; else errs++; }
    report("WRITE", lat, N - errs, errs);

    /* QUERY */
    for (int i = 0; i < WARM; i++) do_query(&c, qbuf);
    errs = 0;
    for (int i = 0; i < N; i++) { double t = now_us(); if (do_query(&c, qbuf) == 0) lat[i - errs] = now_us() - t; else errs++; }
    report("QUERY", lat, N - errs, errs);

    free(lat);
    tsdb_conn_close(&c);
    return 0;
}
