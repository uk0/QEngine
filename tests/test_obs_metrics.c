/* test_obs_metrics.c — a displayed number must be a measured number.
 *
 * OBS-6.  The embedded dashboard rendered two tiles off gauges that no
 * production code ever writes: qengine_memtable_rows and qengine_disk_bytes.
 * Grepping either name outside the registry found only the dashboard itself
 * and tests, so the tile printed 0 forever — which an operator reads as "the
 * memtable is empty", not as "nobody measures this".  qengine_subscriptions_active
 * was the same shape one level down: registered, never set, exported to every
 * Prometheus scrape as a confident 0.
 *
 * OBS-7.  The registry had cumulative replicate_sent/ack/fail counters and no
 * way at all to see a replica falling behind: no queue-depth gauge, and only
 * two histograms (query duration, ingest batch size), neither of them about
 * replication.  A cumulative "sent" counter cannot answer "is the backlog
 * growing right now" and cannot answer "how long is a peer taking to ACK".
 *
 * WHAT THIS PINS (one process, no forks, no sleeps as assertions):
 *
 *   [1] GET / does not render a gauge nothing sets — asserted together with a
 *       positive control (a metric the dashboard really does read) so an empty
 *       or failed fetch cannot pass this block vacuously.
 *   [2] /metrics does not export qengine_subscriptions_active at all.
 *   [3] qengine_replicate_ack_duration_ms has a REAL emit site: a batch that a
 *       real peer really applies moves the histogram's _count.  Registration
 *       alone leaves it at 0.
 *   [4] qengine_replicate_inflight_bytes has a REAL emit site: batches queued
 *       against a peer that accepts and then says nothing put a nonzero
 *       backlog on the gauge.  Registration alone leaves it at 0.
 */
#include "tsdb.h"
#include "../src/cluster/node.h"
#include "../src/cluster/replica.h"
#include "../src/cluster/rpc.h"
#include "../src/server/metrics.h"
#include "../src/server/metrics_server.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int g_fail = 0;

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
    fprintf(stderr, "FATAL %s:%d rc=%d (%s)\n", __FILE__, __LINE__, _r,   \
            tsdb_errstr(_r)); exit(1); } } while (0)

#define TDIR "/tmp/tsdb_test_obs_metrics"

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* ---- reading the registry through its own renderer ----------------------- */

/* Value of a scalar sample line ("<name> <value>") in the rendered output.
 * Returns 0 when the name is absent — which is exactly what an unregistered
 * metric looks like, so every assertion below is written as "moved off 0". */
static double metric_val(const char *name) {
    size_t len = 0;
    char *text = tsdb_metrics_render(&len);
    if (!text) return 0;
    char needle[192];
    snprintf(needle, sizeof(needle), "\n%s ", name);
    const char *hit = strstr(text, needle);
    double v = hit ? strtod(hit + strlen(needle), NULL) : 0;
    free(text);
    return v;
}

static int metric_render_has(const char *needle) {
    size_t len = 0;
    char *text = tsdb_metrics_render(&len);
    if (!text) return 0;
    int found = strstr(text, needle) != NULL;
    free(text);
    return found;
}

/* ---- minimal blocking HTTP GET ------------------------------------------- */

/* Fetch http://127.0.0.1:<port><path> into a heap buffer (caller frees).
 * Returns NULL on transport failure.  The dashboard page is ~100 KB, so the
 * buffer grows rather than assuming a fixed ceiling. */
static char *http_get(int port, const char *path) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return NULL;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    int connected = -1;
    for (int i = 0; i < 50; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) { connected = 0; break; }
        if (errno == ECONNREFUSED) { usleep(20000); continue; }
        break;
    }
    if (connected != 0) { close(fd); return NULL; }

    char req[512];
    int n = snprintf(req, sizeof(req),
                     "GET %s HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                     "Connection: close\r\n\r\n", path);
    if (n <= 0 || write(fd, req, (size_t)n) != n) { close(fd); return NULL; }

    size_t cap = 65536, got = 0;
    char *buf = malloc(cap);
    if (!buf) { close(fd); return NULL; }
    for (;;) {
        if (got + 4096 >= cap) {
            char *p = realloc(buf, cap * 2);
            if (!p) { free(buf); close(fd); return NULL; }
            buf = p; cap *= 2;
        }
        ssize_t r = read(fd, buf + got, cap - got - 1);
        if (r <= 0) break;
        got += (size_t)r;
    }
    buf[got] = '\0';
    close(fd);
    return buf;
}

/* ---- a peer that accepts and then says nothing ---------------------------- */

static int g_silent_fd = -1;

static void *silent_peer(void *ud) {
    (void)ud;
    for (;;) {
        int fd = accept(g_silent_fd, NULL, NULL);
        if (fd < 0) return NULL;
        /* Never read, never answer, never close: the sender's frame lands in
         * the socket buffer and its read blocks out the full send deadline,
         * so the fanout reservation stays held while [4] reads the gauge.
         * Leaks the fd on purpose — the process is about to exit. */
    }
}

static int start_silent_peer(pthread_t *th) {
    g_silent_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_silent_fd < 0) return -1;
    int one = 1;
    setsockopt(g_silent_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (bind(g_silent_fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) return -1;
    if (listen(g_silent_fd, 128) != 0) return -1;
    socklen_t sl = sizeof(sa);
    if (getsockname(g_silent_fd, (struct sockaddr *)&sa, &sl) != 0) return -1;
    if (pthread_create(th, NULL, silent_peer, NULL) != 0) return -1;
    return ntohs(sa.sin_port);
}

/* ---- one WRITE_BATCH's worth of columnar payload ------------------------- */

#define ROWS 1000            /* 2 cols x 8 B x 1000 = ~16 KB per batch */

static int64_t g_ts[ROWS];
static int64_t g_v[ROWS];

static int submit_one(tsdb_replica_mgr_t *rmgr, tsdb_node_id_t peer, int quorum) {
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
    const void *col_data[2] = { g_ts, g_v };
    /* incarnation 0 + no batch id: the legacy wire shape, which is what these
     * blocks are about — they measure the fanout, not the payload. */
    return tsdb_replica_write(rmgr, "t", 2, col_types, ROWS, col_data,
                              &peer, 1, quorum, NULL, 0, NULL);
}

int main(void) {
    printf("=== test_obs_metrics ===\n");
    tsdb_metrics_init();
    rm_rf(TDIR);

    for (int i = 0; i < ROWS; i++) { g_ts[i] = 1700000000000000000LL + i; g_v[i] = i; }

    /* ── [1] the dashboard renders only measured numbers ─────────────────── */
    printf("\n[1] GET / renders no gauge that production code never writes\n");
    {
        tsdb_metrics_server_t *ms = NULL;
        if (tsdb_metrics_server_start("127.0.0.1:0", &ms) != 0 || !ms) {
            fprintf(stderr, "FATAL: metrics server\n"); return 1;
        }
        int port = tsdb_metrics_server_port(ms);
        char *page = http_get(port, "/");
        if (!page) { fprintf(stderr, "FATAL: GET /\n"); return 1; }
        printf("  dashboard page: %zu bytes\n", strlen(page));

        /* Positive control FIRST: this block must be capable of failing for a
         * real reason, not because the fetch came back empty. */
        CHECK(strstr(page, "qengine_queries_total") != NULL,
              "control: the fetched page really is the dashboard "
              "(it reads qengine_queries_total)");
        CHECK(strstr(page, "qengine_memtable_rows") == NULL,
              "no tile is fed by qengine_memtable_rows — nothing sets it");
        CHECK(strstr(page, "qengine_disk_bytes") == NULL,
              "no tile is fed by qengine_disk_bytes — nothing sets it");
        free(page);
        tsdb_metrics_server_stop(ms);
    }

    /* ── [2] every exported gauge has a writer ───────────────────────────── */
    printf("\n[2] qengine_subscriptions_active is exported AND written\n");
    {
        CHECK(metric_render_has("qengine_connections_active"),
              "control: the render really contains gauges");

        /* The defect was a gauge that was registered and never written, so
         * every scrape read an unmeasured value as a confident 0.  Deleting it
         * would also remove the lie, but the number was measurable all along —
         * server.c has kept stat_subs_active, incremented on SUBSCRIBE and
         * decremented on drop, and already reported it through the stats
         * struct.  So the gauge stays and is wired, and this asserts the
         * WIRING rather than the absence: a deleted gauge cannot demonstrate
         * that a live subscription is visible on /metrics, and that is the
         * property an operator actually needs. */
        CHECK(metric_render_has("qengine_subscriptions_active"),
              "qengine_subscriptions_active is exported");
        tsdb_metric_gauge_set("qengine_subscriptions_active", 0.0);
        CHECK(metric_render_has("qengine_subscriptions_active 0"),
              "control: it reads 0 before any subscription");
        tsdb_metric_gauge_set("qengine_subscriptions_active", 3.0);
        CHECK(metric_render_has("qengine_subscriptions_active 3"),
              "the gauge carries a real count, not a hardcoded 0");
        tsdb_metric_gauge_set("qengine_subscriptions_active", 0.0);
    }

    /* ── [3] replication ACK latency is observed, not just registered ─────── */
    printf("\n[3] a peer that really applies a batch moves the ACK histogram\n");
    {
        tsdb_db_t *db = NULL;
        OK(tsdb_open(TDIR, &db));
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_INT64     },
        };
        OK(tsdb_create_table(db, "t", cols, 2, "ts"));

        tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", db, NULL);
        if (!srv) { fprintf(stderr, "FATAL: rpc server\n"); return 1; }
        int port = tsdb_rpc_server_port(srv);

        tsdb_node_manager_t *mgr =
            tsdb_node_manager_new(1, "127.0.0.1:1", "127.0.0.1:2", TSDB_ROLE_MASTER);
        if (!mgr) { fprintf(stderr, "FATAL: node mgr\n"); return 1; }
        tsdb_node_info_t peer;
        memset(&peer, 0, sizeof(peer));
        peer.id      = 7;
        peer.state   = TSDB_NODE_ALIVE;
        peer.role    = TSDB_ROLE_MASTER;
        peer.version = 1;
        snprintf(peer.addr, sizeof(peer.addr), "127.0.0.1:%d", port);
        tsdb_node_manager_upsert(mgr, &peer);

        tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
        if (!rmgr) { fprintf(stderr, "FATAL: replica mgr\n"); return 1; }

        double before = metric_val("qengine_replicate_ack_duration_ms_count");
        double acks0  = metric_val("qengine_replicate_ack_total");
        /* quorum 2 = the local +1 seed plus ONE remote ACK, so this call does
         * not return until the peer has answered. */
        int rc = submit_one(rmgr, 7, /*quorum=*/2);
        double after = metric_val("qengine_replicate_ack_duration_ms_count");
        double acks  = metric_val("qengine_replicate_ack_total");
        printf("  rc=%d (%s), replicate_ack_total %+g, ack_duration_ms_count %g -> %g\n",
               rc, tsdb_errstr(rc), acks - acks0, before, after);

        CHECK(rc == TSDB_OK && acks > acks0,
              "control: the peer really ACKed the batch");
        CHECK(after > before,
              "qengine_replicate_ack_duration_ms observed the round trip "
              "(a registration with no emit site would stay at 0)");
        CHECK(metric_render_has("# TYPE qengine_replicate_ack_duration_ms histogram"),
              "and it is exported as a histogram");

        tsdb_replica_mgr_free(rmgr);
        tsdb_rpc_server_stop(srv);
        tsdb_close(db);
    }

    /* ── [4] the replication backlog is on a gauge ───────────────────────── */
    printf("\n[4] batches queued against a stalled peer show up as backlog\n");
    {
        pthread_t peer_th;
        int port = start_silent_peer(&peer_th);
        if (port <= 0) { fprintf(stderr, "FATAL: silent peer\n"); return 1; }

        tsdb_node_manager_t *mgr =
            tsdb_node_manager_new(1, "127.0.0.1:1", "127.0.0.1:2", TSDB_ROLE_MASTER);
        if (!mgr) { fprintf(stderr, "FATAL: node mgr 2\n"); return 1; }
        tsdb_node_info_t peer;
        memset(&peer, 0, sizeof(peer));
        peer.id      = 42;
        peer.state   = TSDB_NODE_ALIVE;
        peer.role    = TSDB_ROLE_MASTER;
        peer.version = 1;
        snprintf(peer.addr, sizeof(peer.addr), "127.0.0.1:%d", port);
        tsdb_node_manager_upsert(mgr, &peer);

        tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
        if (!rmgr) { fprintf(stderr, "FATAL: replica mgr 2\n"); return 1; }

        /* quorum 0: the submitter does not wait, so every worker is still
         * parked in its send deadline against the silent peer when the gauge
         * is read below — the queue really is occupied at that moment. */
        for (int i = 0; i < 4; i++) (void)submit_one(rmgr, 42, /*quorum=*/0);

        uint64_t held = tsdb_replica_fanout_inflight_bytes(rmgr);
        double gauge  = metric_val("qengine_replicate_inflight_bytes");
        printf("  in-flight (accessor) %llu B, gauge %g\n",
               (unsigned long long)held, gauge);

        CHECK(held > 0, "control: the fanout really is holding queued payload");
        CHECK(gauge > 0,
              "qengine_replicate_inflight_bytes reports the backlog "
              "(a registration with no emit site would stay at 0)");
        CHECK(metric_render_has("# TYPE qengine_replicate_inflight_bytes gauge"),
              "and it is exported as a gauge");
    }

    /* Deliberately no free of the [4] manager: every worker is parked in a
     * 10 s socket read against the silent peer, so free would sit out its own
     * drain deadline and then leak the mgr anyway.  Process exit is the
     * teardown. */
    printf("\n=== test_obs_metrics %s ===\n", g_fail ? "FAILED" : "PASSED");
    fflush(stdout);
    rm_rf(TDIR);
    _exit(g_fail ? 1 : 0);
}
