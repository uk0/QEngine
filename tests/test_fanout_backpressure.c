/* test_fanout_backpressure.c — a slow peer must not become unbounded RSS.
 *
 * THE GAP.  Under TSDB_REPLICATION_QUORUM=0 (the cluster's setting) the
 * submitter does not wait: fanout_wait_quorum_ex reaches ack_count(1) >=
 * quorum(0) and returns immediately, leaving one worker per peer behind.  Each
 * of those workers owns a ref on a fanout_ctx_t that owns the whole encoded
 * WRITE_BATCH payload, and each one can sit in tsdb_rpc_call_recv_to for up to
 * TSDB_REPL_SEND_TIMEOUT_MS (10 s) against a peer that accepted the socket and
 * stopped answering.  The queue they wait in is tsdb_pool's linked list, which
 * has no depth cap and whose submit cannot fail on depth — and when the pool is
 * unavailable the code falls back to a bare pthread_create, so capping the pool
 * itself would not have helped.  Ingest that outruns a stalled peer therefore
 * grows the sender's heap by (payload bytes x queued batches) with nothing
 * anywhere to stop it.
 *
 * WHAT THIS PINS (one process; the peer is a socket that accepts and then goes
 * silent, so the workers block exactly the way they do against a wedged node):
 *
 *   [1] in-flight fanout bytes stay under the cap no matter how much is
 *       submitted;
 *   [2] the overflow is DROPPED and COUNTED, not queued — a dropped best-effort
 *       write is defensible, a silent one is not;
 *   [3] the drop does not hang the submitter or corrupt the wait: with a real
 *       quorum the caller still gets a definite answer;
 *   [4] the cap is off by setting the env to 0, so an operator who wants the
 *       old unbounded behaviour can have it.
 */
#include "tsdb.h"
#include "../src/cluster/node.h"
#include "../src/cluster/replica.h"
#include "../src/server/metrics.h"

#include <arpa/inet.h>
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

static uint64_t metric(const char *name) {
    size_t len = 0;
    char *text = tsdb_metrics_render(&len);
    if (!text) return 0;
    uint64_t v = 0;
    char needle[160];
    snprintf(needle, sizeof(needle), "\n%s ", name);
    const char *hit = strstr(text, needle);
    if (hit) v = strtoull(hit + strlen(needle), NULL, 10);
    free(text);
    return v;
}

/* ---- a peer that accepts and then says nothing ---------------------------- */

static int g_listen_fd = -1;

static void *silent_peer(void *ud) {
    (void)ud;
    for (;;) {
        int fd = accept(g_listen_fd, NULL, NULL);
        if (fd < 0) return NULL;
        /* Never read, never answer, never close: the sender's writev lands in
         * the socket buffer and its read blocks out the full send deadline.
         * Leaks the fd on purpose — the process is about to exit. */
    }
}

static int start_silent_peer(pthread_t *th) {
    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) return -1;
    int one = 1;
    setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (bind(g_listen_fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) return -1;
    if (listen(g_listen_fd, 128) != 0) return -1;
    socklen_t sl = sizeof(sa);
    if (getsockname(g_listen_fd, (struct sockaddr *)&sa, &sl) != 0) return -1;
    if (pthread_create(th, NULL, silent_peer, NULL) != 0) return -1;
    return ntohs(sa.sin_port);
}

/* ---- one WRITE_BATCH's worth of columnar payload ------------------------- */

#define ROWS 4000            /* 2 cols x 8 B x 4000 = 64 KB per batch */

static int64_t g_ts[ROWS];
static int64_t g_v[ROWS];

static int submit_one(tsdb_replica_mgr_t *rmgr, tsdb_node_id_t peer, int quorum) {
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64 };
    const void *col_data[2] = { g_ts, g_v };
    /* incarnation 0 + no batch id: the legacy wire shape, which is what this
     * backpressure test is about — it measures the fanout, not the payload. */
    return tsdb_replica_write(rmgr, "t", 2, col_types, ROWS, col_data,
                              &peer, 1, quorum, NULL, 0, NULL);
}

int main(void) {
    printf("=== test_fanout_backpressure ===\n");
    tsdb_metrics_init();

    for (int i = 0; i < ROWS; i++) { g_ts[i] = 1700000000000000000LL + i; g_v[i] = i; }

    pthread_t peer_th;
    int port = start_silent_peer(&peer_th);
    if (port <= 0) { fprintf(stderr, "FATAL: peer\n"); return 1; }

    /* One connection per peer (the live cluster's setting) and one pool
     * thread, so a single stalled round-trip is enough to back the queue up. */
    setenv("TSDB_REPLICA_CONNS_PER_PEER", "1", 1);
    setenv("TSDB_FANOUT_POOL_SIZE", "1", 1);
    /* 1 MiB cap: ~16 of the 64 KB batches below fit, so 120 submissions
     * overrun it decisively without the test depending on exact arithmetic. */
    setenv("TSDB_FANOUT_MAX_INFLIGHT_BYTES", "1048576", 1);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(1, "127.0.0.1:1", "127.0.0.1:2", TSDB_ROLE_MASTER);
    if (!mgr) { fprintf(stderr, "FATAL: node mgr\n"); return 1; }

    tsdb_node_info_t peer;
    memset(&peer, 0, sizeof(peer));
    peer.id    = 42;
    peer.state = TSDB_NODE_ALIVE;
    peer.role  = TSDB_ROLE_MASTER;
    peer.version = 1;
    snprintf(peer.addr, sizeof(peer.addr), "127.0.0.1:%d", port);
    tsdb_node_manager_upsert(mgr, &peer);

    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    if (!rmgr) { fprintf(stderr, "FATAL: replica mgr\n"); return 1; }

    /* ── [1]/[2] the cap holds and the overflow is counted ───────────────── */
    printf("\n[1] in-flight fanout bytes stay bounded under a stalled peer\n");
    {
        uint64_t dropped0 = metric("qengine_fanout_dropped_total");
        uint64_t peak = 0;
        const int N = 120;
        for (int i = 0; i < N; i++) {
            (void)submit_one(rmgr, 42, /*quorum=*/0);
            uint64_t cur = tsdb_replica_fanout_inflight_bytes(rmgr);
            if (cur > peak) peak = cur;
        }
        uint64_t dropped = metric("qengine_fanout_dropped_total") - dropped0;
        printf("  %d submissions of ~%d B: peak in-flight %llu B, dropped %llu\n",
               N, ROWS * 16, (unsigned long long)peak,
               (unsigned long long)dropped);

        /* One batch may be admitted while already at the cap (the check is
         * "room for one more", not "would still fit"), so allow the payload
         * size on top of the configured ceiling. */
        CHECK(peak <= 1048576ULL + (uint64_t)ROWS * 16 + 4096,
              "in-flight payload bytes never exceeded the cap");
        CHECK(dropped > 0,
              "[2] the overflow was dropped AND counted "
              "(qengine_fanout_dropped_total)");
    }

    /* ── [3] a real quorum still gets a definite answer ──────────────────── */
    printf("\n[3] the submitter still returns a verdict, it does not hang\n");
    {
        /* quorum 2 = local + one peer; the peer never answers, and the queue
         * is already over the cap, so this must come back FAILED rather than
         * block forever or report success off a dropped send. */
        setenv("TSDB_FANOUT_WAIT_TIMEOUT_MS", "300", 1);
        int rc = submit_one(rmgr, 42, /*quorum=*/2);
        printf("  quorum=2 against a silent peer -> rc=%d (%s)\n",
               rc, tsdb_errstr(rc));
        CHECK(rc != TSDB_OK,
              "a write that reached no peer is not reported as replicated");
    }

    /* ── [4] the cap is opt-outable ──────────────────────────────────────── */
    printf("\n[4] TSDB_FANOUT_MAX_INFLIGHT_BYTES=0 restores unbounded queueing\n");
    {
        /* The cap is fixed at mgr_new (like conns_per_peer), so the opt-out
         * needs its own manager — which is also the honest test: this is how
         * an operator sets it, once, at process start. */
        setenv("TSDB_FANOUT_MAX_INFLIGHT_BYTES", "0", 1);
        tsdb_replica_mgr_t *uncapped = tsdb_replica_mgr_new(mgr);
        if (!uncapped) { fprintf(stderr, "FATAL: replica mgr 2\n"); return 1; }

        uint64_t dropped0 = metric("qengine_fanout_dropped_total");
        for (int i = 0; i < 60; i++) (void)submit_one(uncapped, 42, /*quorum=*/0);
        uint64_t dropped = metric("qengine_fanout_dropped_total") - dropped0;
        uint64_t infl = tsdb_replica_fanout_inflight_bytes(uncapped);
        printf("  60 submissions on an uncapped mgr: dropped %llu, "
               "in-flight %llu B\n",
               (unsigned long long)dropped, (unsigned long long)infl);
        CHECK(dropped == 0, "nothing is dropped when the cap is disabled");
        CHECK(infl > 1048576ULL,
              "and the queue grows past the capped manager's ceiling — the "
              "operator really did opt out");
    }

    /* Deliberately no tsdb_replica_mgr_free: every worker is parked in a 10 s
     * socket read against the silent peer, so free would sit out its own 10 s
     * drain deadline and then leak the mgr anyway.  Process exit is the
     * teardown. */
    printf("\n=== test_fanout_backpressure %s ===\n", g_fail ? "FAILED" : "PASSED");
    fflush(stdout);
    _exit(g_fail ? 1 : 0);
}
