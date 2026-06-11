/* load_cluster.c — sustained wire-protocol write loader for the qengine
 * cluster.  Runs ON the host, talks TCP wire v1 to host-mapped wire ports,
 * writes until a target logical byte volume is reached OR the data filesystem
 * crosses a usage cap (safety stop on a shared disk).
 *
 *   gcc -O2 -pthread load_cluster.c tsdb_wire.c -o load_cluster
 *
 * Logical bytes = rows * 16 (ts u64 + val f64, raw).  On-disk is compressed +
 * replicated, so physical footprint differs — the disk cap guards that.
 *
 * Env (all optional):
 *   TSDB_LOAD_GB         target logical GiB (rows*16)        default 100
 *   TSDB_LOAD_THREADS    total worker threads                default 16
 *   TSDB_LOAD_TABLES     child tables under the stable       default 2000
 *   TSDB_LOAD_BATCH      rows per WRITE_BATCH (<=8192)        default 4096
 *   TSDB_LOAD_PORTS      comma list of wire ports            default 29301
 *   TSDB_LOAD_STABLE     stable name                         default loadst
 *   TSDB_LOAD_DISK_PATH  fs to watch for the cap             default /home/nas
 *   TSDB_LOAD_DISK_CAP   stop when fs used% >= this           default 90
 *   TSDB_LOAD_MAX_SEC    wall-clock safety cap (0=off)        default 0
 *   TSDB_LOAD_AUTH       "user:pass" to AUTH_LOGIN (optional) default none
 *   TSDB_LOAD_NOCREATE   1 = skip CREATE (tables already up)  default 0
 *   TSDB_LOAD_PIPELINE   WRITE_BATCH frames in flight/conn    default 1
 *
 * SIGINT/SIGTERM → graceful stop (workers drain the in-flight batch).
 */
#define _DEFAULT_SOURCE 1
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1      /* macOS: un-hide INADDR_LOOPBACK et al. */
#include "tsdb_wire.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>

#define ROW_BYTES 16ULL   /* ts u64 + val f64, raw logical accounting */

static int      g_ports[16]; static int g_nports = 0;
static char     g_auth_user[64] = {0}, g_auth_pass[64] = {0}; static int g_auth = 0;
static char     g_stable[64] = "loadst";
static uint64_t g_target_bytes;
static int      g_tables = 2000, g_batch = 4096, g_threads = 16, g_nocreate = 0;
static int      g_pipeline = 1;
static double   g_disk_cap = 90.0, g_max_sec = 0.0;
static char     g_disk_path[256] = "/home/nas";

static atomic_uint_least64_t g_bytes = 0, g_rows = 0;
static atomic_int g_stop = 0, g_errs = 0;

static void on_sig(int s) { (void)s; atomic_store(&g_stop, 1); }

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}
static uint64_t splitmix(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

static int node_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET; sa.sin_port = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) { close(fd); return -1; }
    struct timeval tv = { .tv_sec = 30, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    return fd;
}

static int handshake(tsdb_conn_t *c) {
    uint8_t buf[160], *p = buf;
    *p++ = TSDB_WIRE_VER;
    const char *cid = "loadgen"; *p++ = (uint8_t)strlen(cid);
    memcpy(p, cid, strlen(cid)); p += strlen(cid); *p++ = 0;
    if (frame_send(c, MSG_HELLO, 0, c->next_req_id++, buf, (uint32_t)(p - buf)) < 0) return -1;
    tsdb_msg_t r = {0}; int rc = frame_recv(c, &r);
    int ok = (rc == 0 && r.hdr.type == MSG_HELLO_OK); msg_free(&r);
    if (!ok) return -1;
    if (g_auth) {
        uint8_t a[160], *q = a; size_t ul = strlen(g_auth_user), pl = strlen(g_auth_pass);
        *q++ = (uint8_t)ul; memcpy(q, g_auth_user, ul); q += ul;
        *q++ = (uint8_t)pl; memcpy(q, g_auth_pass, pl); q += pl;
        if (frame_send(c, MSG_AUTH_LOGIN, 0, c->next_req_id++, a, (uint32_t)(q - a)) < 0) return -1;
        tsdb_msg_t ar = {0}; rc = frame_recv(c, &ar);
        ok = (rc == 0 && ar.hdr.type == MSG_AUTH_OK); msg_free(&ar);
        if (!ok) return -1;
    }
    return 0;
}

/* Run a QTL statement, drain until FIN.  Returns 0, or -1 on MSG_ERROR/IO. */
static int exec_qtl(tsdb_conn_t *c, const char *qtl) {
    size_t ql = strlen(qtl); uint8_t buf[1024]; if (ql + 2 > sizeof(buf)) return -1;
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

/* 2-col batch: ts ascending (1ms step) + val random-walk (≈2-3× compressible). */
static size_t build_batch(uint8_t *buf, const char *tbl, uint32_t nrows,
                          int64_t base_ts, uint64_t seed, double *walk) {
    uint8_t *p = buf; size_t tl = strlen(tbl);
    *p++ = (uint8_t)tl; memcpy(p, tbl, tl); p += tl;
    put_u16(p, 2); p += 2; put_u32(p, nrows); p += 4;
    *p++ = 2; memcpy(p, "ts", 2); p += 2; *p++ = 1; *p++ = 0; put_u32(p, nrows * 8u); p += 4;
    for (uint32_t i = 0; i < nrows; i++) { int64_t ts = base_ts + (int64_t)i * 1000000LL; put_u64(p, (uint64_t)ts); p += 8; }
    *p++ = 3; memcpy(p, "val", 3); p += 3; *p++ = 3; *p++ = 0; put_u32(p, nrows * 8u); p += 4;
    double v = *walk;
    for (uint32_t i = 0; i < nrows; i++) {
        uint64_t h = splitmix(seed + i);
        v += ((double)(h % 201) - 100.0) * 0.01;
        uint64_t bits; memcpy(&bits, &v, 8); put_u64(p, bits); p += 8;
    }
    *walk = v;
    return (size_t)(p - buf);
}

static int send_batch(tsdb_conn_t *c, const char *tbl, uint32_t nrows,
                      int64_t base_ts, uint64_t seed, double *walk) {
    static __thread uint8_t buf[1 << 18];   /* 256 KiB; fits batch<=8192 */
    size_t n = build_batch(buf, tbl, nrows, base_ts, seed, walk);
    return frame_send(c, MSG_WRITE_BATCH, 0, c->next_req_id++, buf, (uint32_t)n) < 0 ? -1 : 0;
}

/* Reap one reply.  0 = WRITE_ACK; -1 = non-ACK frame (server error);
 * -2 = transport failure (timeout/CRC/reset) — connection unusable. */
static int recv_ack(tsdb_conn_t *c) {
    tsdb_msg_t r = {0}; int rc = frame_recv(c, &r);
    if (rc != 0) { msg_free(&r); return -2; }
    int ok = (r.hdr.type == MSG_WRITE_ACK); msg_free(&r);
    return ok ? 0 : -1;
}

typedef struct { int port, tbl_lo, tbl_hi, wid; } job_t;

static void *worker(void *arg) {
    job_t *j = (job_t *)arg;
    int fd = node_connect(j->port);
    if (fd < 0) { atomic_fetch_add(&g_errs, 1); return NULL; }
    tsdb_conn_t c = { .fd = fd, .timeout_ms = 30000, .next_req_id = 1 };
    if (handshake(&c) < 0) { atomic_fetch_add(&g_errs, 1); close(fd); return NULL; }

    int ntab = j->tbl_hi - j->tbl_lo; if (ntab <= 0) ntab = 1;
    int64_t *ts = calloc((size_t)ntab, sizeof(int64_t));
    double  *walk = calloc((size_t)ntab, sizeof(double));
    const int64_t base = 1000000000000LL;   /* 1e12 ns origin */
    uint32_t k = 0;
    int in_flight = 0;
    while (!atomic_load(&g_stop)) {
        int ti = (int)(k++ % (uint32_t)ntab);
        int tno = j->tbl_lo + ti;
        char tbl[40]; snprintf(tbl, sizeof(tbl), "lt_%d", tno);
        int64_t bts = base + ts[ti] * 1000000LL;
        int rc = send_batch(&c, tbl, (uint32_t)g_batch, bts,
                            ((uint64_t)tno << 20) ^ (uint64_t)ts[ti], &walk[ti]);
        if (rc == 0) {
            in_flight++;
            if (g_pipeline > 1) {           /* pipelined: count at send time */
                ts[ti] += g_batch;
                atomic_fetch_add(&g_rows, (uint64_t)g_batch);
                atomic_fetch_add(&g_bytes, (uint64_t)g_batch * ROW_BYTES);
            }
            if (in_flight >= g_pipeline) {  /* pipe full — reap one ACK */
                rc = recv_ack(&c) < 0 ? -1 : 0;
                if (rc == 0) {
                    in_flight--;
                    if (g_pipeline <= 1) {  /* unpipelined: count after ACK */
                        ts[ti] += g_batch;
                        atomic_fetch_add(&g_rows, (uint64_t)g_batch);
                        atomic_fetch_add(&g_bytes, (uint64_t)g_batch * ROW_BYTES);
                    }
                }
            }
        }
        if (rc < 0) {
            atomic_fetch_add(&g_errs, 1);
            close(fd); usleep(20 * 1000);
            in_flight = 0;      /* conn gone — outstanding ACKs went with it */
            fd = node_connect(j->port);
            if (fd < 0) break;
            c.fd = fd; c.next_req_id = 1;
            if (handshake(&c) < 0) break;
            continue;   /* retry same table next loop; ts gap is harmless */
        }
    }
    while (in_flight > 0) {     /* drain ACKs still in flight at stop */
        int rc = recv_ack(&c);
        if (rc < 0) atomic_fetch_add(&g_errs, 1);
        if (rc == -2) break;    /* transport dead — nothing more to drain */
        in_flight--;
    }
    free(ts); free(walk); close(fd);
    return NULL;
}

static double fs_used_pct(const char *path) {
    struct statvfs s; if (statvfs(path, &s) != 0) return -1;
    double total = (double)s.f_blocks * (double)s.f_frsize;
    double avail = (double)s.f_bavail * (double)s.f_frsize;
    if (total <= 0) return -1;
    return (total - avail) * 100.0 / total;
}

static int envi(const char *k, int d) { const char *v = getenv(k); return v ? atoi(v) : d; }
static double envf(const char *k, double d) { const char *v = getenv(k); return v ? atof(v) : d; }

int main(void) {
    double gb = envf("TSDB_LOAD_GB", 100.0);
    g_target_bytes = (uint64_t)(gb * (double)(1ULL << 30));
    g_threads  = envi("TSDB_LOAD_THREADS", 16);
    g_tables   = envi("TSDB_LOAD_TABLES", 2000);
    g_batch    = envi("TSDB_LOAD_BATCH", 4096); if (g_batch > 8192) g_batch = 8192; if (g_batch < 1) g_batch = 1;
    g_disk_cap = envf("TSDB_LOAD_DISK_CAP", 90.0);
    g_max_sec  = envf("TSDB_LOAD_MAX_SEC", 0.0);
    g_nocreate = envi("TSDB_LOAD_NOCREATE", 0);
    g_pipeline = envi("TSDB_LOAD_PIPELINE", 1); if (g_pipeline < 1) g_pipeline = 1;
    { const char *v = getenv("TSDB_LOAD_DISK_PATH"); if (v) snprintf(g_disk_path, sizeof(g_disk_path), "%s", v); }
    { const char *v = getenv("TSDB_LOAD_STABLE"); if (v) snprintf(g_stable, sizeof(g_stable), "%s", v); }
    { char pb[128]; const char *v = getenv("TSDB_LOAD_PORTS");
      snprintf(pb, sizeof(pb), "%s", v ? v : "29301");
      for (char *t = strtok(pb, ","); t && g_nports < 16; t = strtok(NULL, ",")) g_ports[g_nports++] = atoi(t); }
    { const char *au = getenv("TSDB_LOAD_AUTH");
      if (au && strchr(au, ':')) { g_auth = 1; sscanf(au, "%63[^:]:%63s", g_auth_user, g_auth_pass); } }
    if (g_nports == 0) { g_ports[0] = 29301; g_nports = 1; }

    signal(SIGINT, on_sig); signal(SIGTERM, on_sig); signal(SIGPIPE, SIG_IGN);

    printf("[load] target=%.1f GiB  threads=%d  tables=%d  batch=%d  pipeline=%d  ports=%d  stable=%s  disk_cap=%.0f%% (%s)  auth=%s\n",
           gb, g_threads, g_tables, g_batch, g_pipeline, g_nports, g_stable, g_disk_cap, g_disk_path, g_auth ? g_auth_user : "(none)");

    int fd = node_connect(g_ports[0]);
    if (fd < 0) { fprintf(stderr, "[load] connect :%d failed\n", g_ports[0]); return 1; }
    tsdb_conn_t cc = { .fd = fd, .timeout_ms = 30000, .next_req_id = 1 };
    if (handshake(&cc) < 0) { fprintf(stderr, "[load] handshake failed (wire auth required? set TSDB_LOAD_AUTH=user:pass)\n"); return 1; }

    if (!g_nocreate) {
        double tc0 = now_sec();
        char q[160];
        snprintf(q, sizeof(q), "DROP STABLE %s", g_stable); (void)exec_qtl(&cc, q);  /* idempotent reset */
        snprintf(q, sizeof(q), "CREATE STABLE %s (ts TIMESTAMP, val FLOAT64) TAGS (tid SYMBOL)", g_stable);
        if (exec_qtl(&cc, q) < 0) { fprintf(stderr, "[load] CREATE STABLE %s failed\n", g_stable); close(fd); return 1; }
        int made = 0;
        for (int i = 0; i < g_tables; i++) {
            snprintf(q, sizeof(q), "CREATE TABLE lt_%d USING %s TAGS ('%d')", i, g_stable, i);
            if (exec_qtl(&cc, q) == 0) made++;
        }
        printf("[load] stable %s + %d/%d child tables in %.2fs\n", g_stable, made, g_tables, now_sec() - tc0);
        fflush(stdout);
        sleep(5);   /* let DDL replicate to followers before writes fan in */
    }
    close(fd);

    pthread_t *tid = calloc((size_t)g_threads, sizeof(*tid));
    job_t *jobs = calloc((size_t)g_threads, sizeof(*jobs));
    int per = g_tables / g_threads; if (per < 1) per = 1;
    for (int w = 0; w < g_threads; w++) {
        jobs[w].port = g_ports[w % g_nports];
        jobs[w].tbl_lo = w * per;
        jobs[w].tbl_hi = (w == g_threads - 1) ? g_tables : (w + 1) * per;
        if (jobs[w].tbl_lo >= g_tables) jobs[w].tbl_lo = g_tables - 1;
        if (jobs[w].tbl_hi > g_tables)  jobs[w].tbl_hi = g_tables;
        jobs[w].wid = w;
        pthread_create(&tid[w], NULL, worker, &jobs[w]);
    }

    double t0 = now_sec(), last = t0; uint64_t lastb = 0;
    while (!atomic_load(&g_stop)) {
        usleep(2000 * 1000);
        uint64_t b = atomic_load(&g_bytes), r = atomic_load(&g_rows);
        double now = now_sec(), dt = now - last, inst = (double)(b - lastb) / (dt > 0 ? dt : 1);
        double up = fs_used_pct(g_disk_path);
        printf("[load] %.2f/%.0f GiB  rows=%" PRIu64 "  %.0f MB/s  avg=%.2fM r/s  disk=%.1f%%  t=%.0fs  errs=%d\n",
               (double)b / (double)(1ULL << 30), gb, r, inst / 1e6,
               (double)r / (now - t0) / 1e6, up, now - t0, atomic_load(&g_errs));
        fflush(stdout);
        last = now; lastb = b;
        if (b >= g_target_bytes) { printf("[load] target reached\n"); atomic_store(&g_stop, 1); }
        if (up >= 0 && up >= g_disk_cap) { printf("[load] DISK CAP %.1f%% >= %.0f%% — stopping\n", up, g_disk_cap); atomic_store(&g_stop, 1); }
        if (g_max_sec > 0 && (now - t0) >= g_max_sec) { printf("[load] max_sec %.0f reached\n", g_max_sec); atomic_store(&g_stop, 1); }
    }
    for (int w = 0; w < g_threads; w++) pthread_join(tid[w], NULL);
    double el = now_sec() - t0;
    printf("[load] DONE  %.2f GiB  %" PRIu64 " rows  %.1fs  %.2fM r/s  errs=%d\n",
           (double)atomic_load(&g_bytes) / (double)(1ULL << 30), atomic_load(&g_rows),
           el, (double)atomic_load(&g_rows) / (el > 0 ? el : 1) / 1e6, atomic_load(&g_errs));
    free(tid); free(jobs);
    return 0;
}
