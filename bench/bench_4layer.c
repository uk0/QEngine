/* bench_4layer.c — end-to-end ingest benchmark against a running
 * tsdb cluster, exercising the full 4-level hierarchy
 *
 *   Database → Group → VTable (STable) → PTable (child table)
 *
 * The hierarchy is generated randomly at start-up (N DBs × G groups ×
 * V vtables × P ptables, with VTable column counts uniformly sampled
 * in [cols_min, cols_max]).  Writes use the dashboard Influx Line
 * Protocol endpoint (default port 28092 inside the container, mapped
 * to 2921N / 2929N on a compose stack) so the bench runs with plain
 * TCP and no SDK dependency.
 *
 * Default target is 100M rows; override with --rows to match a laptop
 * or a production-scale host.  Each writer thread picks a random
 * PTable per flush and spreads rows across measurement names so that
 * the memtable flush paths exercise every VTable.  If multiple --host
 * arguments are supplied the writers round-robin across them, which
 * gives realistic fan-in on a multi-node cluster.
 *
 * Build:     make bench        picks up via bench/<name>.c wildcard
 * Single run:
 *   build/bench/bench_4layer --host 127.0.0.1 --rows 1000000 --threads 4
 *
 * Cluster run (auth ON, 4-node lvm1 compose):
 *   build/bench/bench_4layer \
 *     --host 10.88.51.102 --port-dash 29311 --port-influx 29321 \
 *     --host 10.88.51.102 --port-dash 29312 --port-influx 29322 \
 *     --host 10.88.51.102 --port-dash 29313 --port-influx 29323 \
 *     --host 10.88.51.102 --port-dash 29314 --port-influx 29324 \
 *     --rows 100000000 --threads 16 --auth-user root --auth-pass 123456
 *
 * Exits 0 only if every setup DDL succeeded and the final row count
 * across all writers matched what the bench intended to send.
 */

#define _POSIX_C_SOURCE 200809L
#if defined(__APPLE__)
#  define _DARWIN_C_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

/* ---- config ------------------------------------------------------------- */

#define MAX_HOSTS           16
#define MAX_PTABLES_TOTAL   65536     /* D × G × V × P upper bound         */
#define MAX_VTABLES_TOTAL   4096
#define MAX_FIELD_COLS      16        /* hard cap; bench generator picks
                                         a random count in [cmin, cmax]    */
#define BATCH_ROWS          1024      /* rows per HTTP POST /write          */
#define HTTP_SEND_BUF       (256 * 1024)
#define HTTP_RECV_BUF       4096

typedef struct {
    char host[128];
    int  dash_port;      /* HTTP port for /login + /sql (default 28094)   */
    int  influx_port;    /* HTTP port for /write      (default 28092)     */
    char cookie[80];     /* tsdb_auth=<hex32>; set after login; empty OK  */
} host_t;

typedef struct {
    char  name[64];
    char  parent_vtable[64];      /* the STable it USING                  */
    int   ncols;                  /* fields (excluding ts)                */
    /* field name tags drawn from a small dictionary keyed by vtable     */
    int   vtable_idx;
} ptable_t;

typedef struct {
    char  name[64];
    char  database[64];
    char  group[64];
    int   ncols;                                    /* random 2..8       */
    char  field_names[MAX_FIELD_COLS][32];
    int   field_is_int[MAX_FIELD_COLS];             /* 1 = INT64, else FLOAT64 */
} vtable_t;

typedef struct {
    /* Cluster addressing. */
    host_t hosts[MAX_HOSTS];
    int    n_hosts;

    /* Target counts. */
    long long total_rows;
    int       n_threads;
    int       n_databases;
    int       n_groups_per_db;
    int       n_vtables_per_group;
    int       n_ptables_per_vtable;
    int       cols_min;
    int       cols_max;

    /* Generated structure. */
    vtable_t  vtables[MAX_VTABLES_TOTAL];
    int       n_vtables;
    ptable_t  ptables[MAX_PTABLES_TOTAL];
    int       n_ptables;

    /* Auth (optional). */
    char      auth_user[64];
    char      auth_pass[64];

    /* Shared counters. */
    atomic_llong rows_written;
    atomic_llong rows_failed;
    atomic_int   dispatch_host_rr;
} bench_t;

/* ---- tiny HTTP client -------------------------------------------------- */

static int dial(const char *host, int port) {
    struct addrinfo hints = {0}, *ai = NULL;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char ps[16]; snprintf(ps, sizeof(ps), "%d", port);
    if (getaddrinfo(host, ps, &hints, &ai) != 0) return -1;
    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) { freeaddrinfo(ai); return -1; }
    struct timeval tv = { .tv_sec = 30, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
        close(fd); freeaddrinfo(ai); return -1;
    }
    freeaddrinfo(ai);
    return fd;
}

static int send_all(int fd, const char *buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t k = send(fd, buf + sent, n - sent, 0);
        if (k <= 0) return -1;
        sent += (size_t)k;
    }
    return 0;
}

/* Issue POST with JSON body to /sql and return response JSON in out. */
static int http_post_json(const host_t *h, const char *path,
                           const char *body, size_t blen,
                           char *out, size_t out_cap)
{
    int fd = dial(h->host, h->dash_port);
    if (fd < 0) return -1;

    char hdr[512];
    int len;
    if (h->cookie[0]) {
        len = snprintf(hdr, sizeof(hdr),
            "POST %s HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\n"
            "Content-Length: %zu\r\nCookie: tsdb_auth=%s\r\n"
            "Connection: close\r\n\r\n",
            path, h->host, blen, h->cookie);
    } else {
        len = snprintf(hdr, sizeof(hdr),
            "POST %s HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\n"
            "Content-Length: %zu\r\n"
            "Connection: close\r\n\r\n",
            path, h->host, blen);
    }
    if (send_all(fd, hdr, (size_t)len) < 0 ||
        send_all(fd, body, blen) < 0) { close(fd); return -1; }

    size_t r = 0;
    while (r < out_cap - 1) {
        ssize_t k = recv(fd, out + r, out_cap - 1 - r, 0);
        if (k <= 0) break;
        r += (size_t)k;
    }
    out[r] = '\0';
    close(fd);
    return (int)r;
}

/* Form-urlencoded POST used only by /login. */
static int http_post_form(const host_t *h, const char *path,
                           const char *body, size_t blen,
                           char *out, size_t out_cap)
{
    int fd = dial(h->host, h->dash_port);
    if (fd < 0) return -1;
    char hdr[512];
    int len = snprintf(hdr, sizeof(hdr),
        "POST %s HTTP/1.1\r\nHost: %s\r\n"
        "Content-Type: application/x-www-form-urlencoded\r\n"
        "Content-Length: %zu\r\nConnection: close\r\n\r\n",
        path, h->host, blen);
    if (send_all(fd, hdr, (size_t)len) < 0 ||
        send_all(fd, body, blen) < 0) { close(fd); return -1; }
    size_t r = 0;
    while (r < out_cap - 1) {
        ssize_t k = recv(fd, out + r, out_cap - 1 - r, 0);
        if (k <= 0) break;
        r += (size_t)k;
    }
    out[r] = '\0';
    close(fd);
    return (int)r;
}

/* POST body to /write on the Influx port; uses plain Content-Length. */
static int http_post_influx(const host_t *h, const char *body, size_t blen) {
    int fd = dial(h->host, h->influx_port);
    if (fd < 0) return -1;
    char hdr[256];
    int len = snprintf(hdr, sizeof(hdr),
        "POST /write HTTP/1.1\r\nHost: %s\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %zu\r\nConnection: close\r\n\r\n",
        h->host, blen);
    if (send_all(fd, hdr, (size_t)len) < 0 ||
        send_all(fd, body, blen) < 0) { close(fd); return -1; }
    /* Read enough to find the HTTP status line, discard the rest. */
    char resp[256];
    ssize_t k = recv(fd, resp, sizeof(resp) - 1, 0);
    close(fd);
    if (k <= 0) return -1;
    resp[k] = '\0';
    return (strstr(resp, "200 OK") || strstr(resp, "204 No")) ? 0 : -1;
}

/* ---- auth login ------------------------------------------------------- */

static int login(host_t *h, const char *user, const char *pass) {
    if (!user || !*user) { h->cookie[0] = '\0'; return 0; }
    char body[256], resp[4096];
    int bl = snprintf(body, sizeof(body), "user=%s&pass=%s", user, pass);
    int n  = http_post_form(h, "/login", body, (size_t)bl, resp, sizeof(resp));
    if (n <= 0) return -1;
    /* Extract hex32 from Set-Cookie: tsdb_auth=…;  */
    char *p = strstr(resp, "Set-Cookie:");
    if (!p) p = strstr(resp, "set-cookie:");
    if (!p) return -1;
    char *t = strstr(p, "tsdb_auth=");
    if (!t) return -1;
    t += 10;
    size_t w = 0;
    while (*t && *t != ';' && *t != '\r' && *t != '\n' &&
           w < sizeof(h->cookie) - 1) {
        h->cookie[w++] = *t++;
    }
    h->cookie[w] = '\0';
    return h->cookie[0] ? 0 : -1;
}

/* ---- JSON-escape a QTL for /sql body ---------------------------------- */

static size_t json_escape(const char *s, char *dst, size_t dcap) {
    size_t w = 0;
    for (const char *p = s; *p && w + 2 < dcap; p++) {
        if (*p == '"' || *p == '\\') dst[w++] = '\\';
        dst[w++] = *p;
    }
    dst[w] = '\0';
    return w;
}

static int run_qtl(const host_t *h, const char *qtl) {
    char esc[4096], body[4096], resp[4096];
    size_t el = json_escape(qtl, esc, sizeof(esc));
    int bl = snprintf(body, sizeof(body), "{\"q\":\"%.*s\"}", (int)el, esc);
    int n  = http_post_json(h, "/sql", body, (size_t)bl, resp, sizeof(resp));
    if (n <= 0) return -1;
    return (strstr(resp, "\"error\"") && !strstr(resp, "EXISTS")) ? -1 : 0;
}

/* ---- schema generation ------------------------------------------------ */

static void rand_field_name(int idx, int is_int, char *out, size_t cap) {
    static const char *prefixes[] = {
        "v","val","x","y","temp","volt","watt","pressure","flow","sig"
    };
    int pi = rand() % (int)(sizeof(prefixes)/sizeof(prefixes[0]));
    snprintf(out, cap, "%s%d%s", prefixes[pi], idx, is_int ? "i" : "");
}

static void build_hierarchy(bench_t *b) {
    b->n_vtables = 0;
    b->n_ptables = 0;
    for (int d = 0; d < b->n_databases; d++) {
        for (int g = 0; g < b->n_groups_per_db; g++) {
            for (int v = 0; v < b->n_vtables_per_group; v++) {
                if (b->n_vtables >= MAX_VTABLES_TOTAL) return;
                vtable_t *vt = &b->vtables[b->n_vtables];
                snprintf(vt->database, sizeof(vt->database), "bench_db%d", d);
                snprintf(vt->group,    sizeof(vt->group),    "grp_%d_%d", d, g);
                snprintf(vt->name,     sizeof(vt->name),
                         "vt_d%d_g%d_v%d", d, g, v);
                int span = b->cols_max - b->cols_min + 1;
                vt->ncols = b->cols_min + (span > 0 ? rand() % span : 0);
                if (vt->ncols < 1) vt->ncols = 1;
                if (vt->ncols > MAX_FIELD_COLS) vt->ncols = MAX_FIELD_COLS;
                for (int f = 0; f < vt->ncols; f++) {
                    vt->field_is_int[f] = rand() % 3 == 0; /* ~33% INT */
                    rand_field_name(f, vt->field_is_int[f],
                                    vt->field_names[f],
                                    sizeof(vt->field_names[f]));
                }
                b->n_vtables++;

                /* Child ptables. */
                for (int p = 0; p < b->n_ptables_per_vtable; p++) {
                    if (b->n_ptables >= MAX_PTABLES_TOTAL) break;
                    ptable_t *pt = &b->ptables[b->n_ptables];
                    snprintf(pt->parent_vtable, sizeof(pt->parent_vtable),
                             "%s", vt->name);
                    snprintf(pt->name, sizeof(pt->name),
                             "pt_d%d_g%d_v%d_p%d", d, g, v, p);
                    pt->ncols      = vt->ncols;
                    pt->vtable_idx = b->n_vtables - 1;
                    b->n_ptables++;
                }
            }
        }
    }
}

/* Send CREATE statements for the whole hierarchy against host[0]. */
static int create_hierarchy(bench_t *b) {
    host_t *h = &b->hosts[0];
    char qtl[1024];

    for (int d = 0; d < b->n_databases; d++) {
        snprintf(qtl, sizeof(qtl), "CREATE DATABASE bench_db%d", d);
        if (run_qtl(h, qtl) != 0)
            fprintf(stderr, "[setup] %s failed (continuing)\n", qtl);
    }
    for (int i = 0; i < b->n_vtables; i++) {
        vtable_t *vt = &b->vtables[i];
        snprintf(qtl, sizeof(qtl),
                 "CREATE GROUP %s IN DATABASE %s",
                 vt->group, vt->database);
        (void)run_qtl(h, qtl);
    }
    for (int i = 0; i < b->n_vtables; i++) {
        vtable_t *vt = &b->vtables[i];
        size_t w = 0;
        w += (size_t)snprintf(qtl + w, sizeof(qtl) - w,
                              "CREATE STABLE %s (ts TIMESTAMP", vt->name);
        for (int f = 0; f < vt->ncols; f++) {
            w += (size_t)snprintf(qtl + w, sizeof(qtl) - w,
                                  ", %s %s", vt->field_names[f],
                                  vt->field_is_int[f] ? "INT64" : "FLOAT64");
        }
        w += (size_t)snprintf(qtl + w, sizeof(qtl) - w,
                              ") TAGS (region SYMBOL, unit INT64)");
        if (run_qtl(h, qtl) != 0)
            fprintf(stderr, "[setup] CREATE STABLE %s failed\n", vt->name);
    }
    static const char *REGIONS[] = { "east", "west", "north", "south" };
    for (int i = 0; i < b->n_ptables; i++) {
        ptable_t *pt = &b->ptables[i];
        snprintf(qtl, sizeof(qtl),
                 "CREATE TABLE %s USING %s TAGS ('%s', %d)",
                 pt->name, pt->parent_vtable,
                 REGIONS[rand() % 4], (rand() % 100) + 1);
        (void)run_qtl(h, qtl);
    }
    return 0;
}

/* ---- writer thread ---------------------------------------------------- */

typedef struct {
    bench_t   *ctx;
    int        tid;
    long long  rows_target;
} writer_arg_t;

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void *writer_main(void *arg) {
    writer_arg_t *wa = (writer_arg_t *)arg;
    bench_t *b = wa->ctx;

    /* Per-thread RNG seed: stable results across runs if desired. */
    unsigned seed = (unsigned)(wa->tid * 2654435761u ^ (unsigned)now_ns());

    char *batch = malloc(HTTP_SEND_BUF);
    if (!batch) return NULL;

    long long emitted = 0;
    int64_t base_ts = 1700000000000000000LL + (int64_t)wa->tid * 10000000000LL;

    while (emitted < wa->rows_target) {
        int batch_rows = 0;
        size_t bw = 0;

        /* Pick a random ptable for this batch so the server's memtable
         * locks get exercised across many tables simultaneously. */
        ptable_t *pt = &b->ptables[rand_r(&seed) % b->n_ptables];
        vtable_t *vt = &b->vtables[pt->vtable_idx];

        while (batch_rows < BATCH_ROWS &&
               emitted + batch_rows < wa->rows_target &&
               bw + 512 < HTTP_SEND_BUF)
        {
            int64_t ts = base_ts + ((int64_t)emitted + batch_rows) * 1000000LL;
            bw += (size_t)snprintf(batch + bw, HTTP_SEND_BUF - bw,
                                    "%s ", pt->name);
            for (int f = 0; f < vt->ncols; f++) {
                if (f) batch[bw++] = ',';
                int v = (int)(rand_r(&seed) % 1000);
                if (vt->field_is_int[f]) {
                    bw += (size_t)snprintf(batch + bw, HTTP_SEND_BUF - bw,
                                           "%s=%di", vt->field_names[f], v);
                } else {
                    bw += (size_t)snprintf(batch + bw, HTTP_SEND_BUF - bw,
                                           "%s=%d.%d", vt->field_names[f],
                                           v, v % 10);
                }
            }
            bw += (size_t)snprintf(batch + bw, HTTP_SEND_BUF - bw,
                                    " %" PRId64 "\n", ts);
            batch_rows++;
        }

        int hi = atomic_fetch_add(&b->dispatch_host_rr, 1) % b->n_hosts;
        int rc = http_post_influx(&b->hosts[hi], batch, bw);
        if (rc == 0) {
            atomic_fetch_add(&b->rows_written, batch_rows);
        } else {
            atomic_fetch_add(&b->rows_failed, batch_rows);
            usleep(5 * 1000);      /* small backoff on transport fail  */
        }
        emitted += batch_rows;
    }

    free(batch);
    return NULL;
}

/* ---- progress printer ------------------------------------------------- */

static volatile int g_done = 0;
static void *progress_main(void *arg) {
    bench_t *b = (bench_t *)arg;
    int64_t t0 = now_ns();
    long long last = 0;
    while (!g_done) {
        sleep(2);
        long long cur = atomic_load(&b->rows_written);
        long long failed = atomic_load(&b->rows_failed);
        double elapsed = (double)(now_ns() - t0) / 1e9;
        double rate = elapsed > 0 ? (double)cur / elapsed : 0;
        double inst = (double)(cur - last) / 2.0;
        double pct  = (double)cur * 100.0 / (double)b->total_rows;
        fprintf(stderr,
            "\r[bench] %.2f%%  rows=%lld  failed=%lld  avg=%.0f/s  inst=%.0f/s  elapsed=%.1fs   ",
            pct, cur, failed, rate, inst, elapsed);
        fflush(stderr);
        last = cur;
    }
    fprintf(stderr, "\n");
    return NULL;
}

/* ---- argv parsing ----------------------------------------------------- */

static void usage(const char *prog) {
    fprintf(stderr,
      "Usage: %s [--host H] [--port-dash N] [--port-influx N] ...\n"
      "  --host HOST              host to hit (repeatable; writes round-robin)\n"
      "  --port-dash PORT         dashboard HTTP port for /login + /sql (default 28094)\n"
      "  --port-influx PORT       influx HTTP port for /write           (default 28092)\n"
      "  --rows N                 total rows to insert            (default 100000000)\n"
      "  --threads T              writer threads                  (default 16)\n"
      "  --databases D            level 1 count                   (default 4)\n"
      "  --groups-per-db G        level 2 count                   (default 2)\n"
      "  --vtables-per-group V    level 3 count                   (default 4)\n"
      "  --ptables-per-vtable P   level 4 count                   (default 16)\n"
      "  --cols-min / --cols-max  random field-column span        (default 2 / 8)\n"
      "  --auth-user / --auth-pass   dashboard login               (default root/123456)\n"
      "  --seed S                 PRNG seed                        (default time)\n"
      "  --no-login               skip login (auth disabled clusters)\n"
      "  --skip-setup             assume hierarchy + STables already exist\n"
      "\nExample: full 100M run against 4-node lvm1 stack\n"
      "  %s --host 10.88.51.102 --port-dash 29311 --port-influx 29321 \\\n"
      "     --host 10.88.51.102 --port-dash 29312 --port-influx 29322 \\\n"
      "     --host 10.88.51.102 --port-dash 29313 --port-influx 29323 \\\n"
      "     --host 10.88.51.102 --port-dash 29314 --port-influx 29324 \\\n"
      "     --rows 100000000 --threads 32\n",
      prog, prog);
}

int main(int argc, char **argv) {
    /* bench_t holds two large arrays (MAX_PTABLES_TOTAL × ptable_t +
     * MAX_VTABLES_TOTAL × vtable_t) that together exceed the default
     * 8 MB pthread stack on Linux.  Heap-allocate so the initial
     * zero-initialisation doesn't overflow. */
    bench_t *bp = calloc(1, sizeof(*bp));
    if (!bp) { fprintf(stderr, "oom\n"); return 1; }
    bench_t b_storage; /* unused placeholder — keep legacy code below
                         readable while we mutate through *bp via &b */
    (void)b_storage;
#define b (*bp)
    b.total_rows         = 100000000LL;
    b.n_threads          = 16;
    b.n_databases        = 4;
    b.n_groups_per_db    = 2;
    b.n_vtables_per_group = 4;
    b.n_ptables_per_vtable = 16;
    b.cols_min           = 2;
    b.cols_max           = 8;
    snprintf(b.auth_user, sizeof(b.auth_user), "root");
    snprintf(b.auth_pass, sizeof(b.auth_pass), "123456");

    unsigned seed = (unsigned)time(NULL);
    int skip_setup = 0;
    int no_login   = 0;

    /* Pending host under construction. */
    host_t pending = { .dash_port = 28094, .influx_port = 28092 };
    int pending_has_host = 0;

    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        const char *v = (i + 1 < argc) ? argv[i + 1] : NULL;
        #define NEED(x) do { if (!v) { fprintf(stderr, \
            "missing value for %s\n", x); usage(argv[0]); return 1; } \
            i++; } while (0)

        if (!strcmp(a, "--host")) {
            NEED("--host");
            if (pending_has_host && b.n_hosts < MAX_HOSTS) {
                b.hosts[b.n_hosts++] = pending;
                pending = (host_t){ .dash_port = 28094, .influx_port = 28092 };
            }
            snprintf(pending.host, sizeof(pending.host), "%s", v);
            pending_has_host = 1;
        } else if (!strcmp(a, "--port-dash"))    { NEED("--port-dash");    pending.dash_port   = atoi(v); }
        else if (!strcmp(a, "--port-influx"))  { NEED("--port-influx");  pending.influx_port = atoi(v); }
        else if (!strcmp(a, "--rows"))         { NEED("--rows");         b.total_rows = atoll(v); }
        else if (!strcmp(a, "--threads"))      { NEED("--threads");      b.n_threads = atoi(v); }
        else if (!strcmp(a, "--databases"))    { NEED("--databases");    b.n_databases = atoi(v); }
        else if (!strcmp(a, "--groups-per-db")){ NEED("--groups-per-db"); b.n_groups_per_db = atoi(v); }
        else if (!strcmp(a, "--vtables-per-group")){ NEED("--vtables-per-group"); b.n_vtables_per_group = atoi(v); }
        else if (!strcmp(a, "--ptables-per-vtable")){ NEED("--ptables-per-vtable"); b.n_ptables_per_vtable = atoi(v); }
        else if (!strcmp(a, "--cols-min"))     { NEED("--cols-min");     b.cols_min = atoi(v); }
        else if (!strcmp(a, "--cols-max"))     { NEED("--cols-max");     b.cols_max = atoi(v); }
        else if (!strcmp(a, "--auth-user"))    { NEED("--auth-user");    snprintf(b.auth_user, sizeof(b.auth_user), "%s", v); }
        else if (!strcmp(a, "--auth-pass"))    { NEED("--auth-pass");    snprintf(b.auth_pass, sizeof(b.auth_pass), "%s", v); }
        else if (!strcmp(a, "--seed"))         { NEED("--seed");         seed = (unsigned)atoi(v); }
        else if (!strcmp(a, "--skip-setup"))   skip_setup = 1;
        else if (!strcmp(a, "--no-login"))     no_login = 1;
        else if (!strcmp(a, "--help") || !strcmp(a, "-h")) { usage(argv[0]); return 0; }
        else { fprintf(stderr, "unknown arg: %s\n", a); usage(argv[0]); return 1; }
        #undef NEED
    }
    if (pending_has_host && b.n_hosts < MAX_HOSTS) b.hosts[b.n_hosts++] = pending;

    if (b.n_hosts == 0) {
        host_t h = { .dash_port = 28094, .influx_port = 28092 };
        snprintf(h.host, sizeof(h.host), "127.0.0.1");
        b.hosts[0] = h; b.n_hosts = 1;
    }
    if (b.cols_max < b.cols_min) b.cols_max = b.cols_min;
    srand(seed);
    atomic_init(&b.rows_written, 0);
    atomic_init(&b.rows_failed,  0);
    atomic_init(&b.dispatch_host_rr, 0);

    /* Login every host if auth is requested. */
    if (!no_login) {
        for (int i = 0; i < b.n_hosts; i++) {
            if (login(&b.hosts[i], b.auth_user, b.auth_pass) != 0) {
                fprintf(stderr,
                    "[warn] login failed on %s:%d (proceeding, assume auth disabled)\n",
                    b.hosts[i].host, b.hosts[i].dash_port);
            }
        }
    }

    build_hierarchy(&b);
    printf("[bench] hierarchy: %d DB × %d grp × %d vtable × %d ptable = %d vtables / %d ptables\n",
           b.n_databases, b.n_groups_per_db, b.n_vtables_per_group,
           b.n_ptables_per_vtable, b.n_vtables, b.n_ptables);
    printf("[bench] fields per vtable ∈ [%d, %d], types FLOAT64/INT64\n",
           b.cols_min, b.cols_max);
    printf("[bench] target %lld rows × %d writers → ~%.1fM rows/thread\n",
           b.total_rows, b.n_threads,
           (double)b.total_rows / b.n_threads / 1e6);
    printf("[bench] hosts:\n");
    for (int i = 0; i < b.n_hosts; i++) {
        printf("  [%d] %s  dash=%d  influx=%d  cookie=%s\n", i,
               b.hosts[i].host, b.hosts[i].dash_port, b.hosts[i].influx_port,
               b.hosts[i].cookie[0] ? "yes" : "no");
    }

    if (!skip_setup) {
        printf("[bench] issuing DDL on %s:%d ...\n",
               b.hosts[0].host, b.hosts[0].dash_port);
        int64_t t0 = now_ns();
        if (create_hierarchy(&b) != 0) {
            fprintf(stderr, "[bench] setup failed\n");
            return 1;
        }
        printf("[bench] DDL done in %.2fs\n", (now_ns() - t0) / 1e9);
    }

    /* Launch writer threads. */
    pthread_t *tids = calloc((size_t)b.n_threads, sizeof(pthread_t));
    writer_arg_t *args = calloc((size_t)b.n_threads, sizeof(*args));
    long long base = b.total_rows / b.n_threads;
    long long rem  = b.total_rows % b.n_threads;
    int64_t wall_t0 = now_ns();
    for (int i = 0; i < b.n_threads; i++) {
        args[i].ctx = &b;
        args[i].tid = i;
        args[i].rows_target = base + (i < rem ? 1 : 0);
        if (pthread_create(&tids[i], NULL, writer_main, &args[i]) != 0) {
            fprintf(stderr, "pthread_create failed: %s\n", strerror(errno));
            return 1;
        }
    }
    pthread_t prog_thr;
    pthread_create(&prog_thr, NULL, progress_main, &b);

    for (int i = 0; i < b.n_threads; i++) pthread_join(tids[i], NULL);
    g_done = 1;
    pthread_join(prog_thr, NULL);

    double elapsed = (now_ns() - wall_t0) / 1e9;
    long long written = atomic_load(&b.rows_written);
    long long failed  = atomic_load(&b.rows_failed);

    printf("\n[bench] done\n");
    printf("  wrote        %lld rows\n", written);
    printf("  failed       %lld rows\n", failed);
    printf("  wall         %.2f s\n", elapsed);
    printf("  throughput   %.0f rows/s  (%.2f MB/s estimated @ 32 B/row)\n",
           elapsed > 0 ? written / elapsed : 0,
           elapsed > 0 ? (double)written * 32.0 / elapsed / (1024.0 * 1024.0) : 0);

    free(tids);
    free(args);
    int rc = (written + failed >= b.total_rows) ? 0 : 1;
#undef b
    free(bp);
    return rc;
}
