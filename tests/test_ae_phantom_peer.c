/* test_ae_phantom_peer.c — end-to-end guard for the anti-entropy peer probe.
 *
 * THE BUG.  Anti-entropy asked a peer "how much do you have?" with
 * `SELECT count(*), max(ts) FROM <t>` over FED_QUERY.  Hierarchy mirroring
 * registers every plain table as a childless data-bearing super-table, so on a
 * peer holding NO local rows exec's cluster-agg coordinator fires and the peer
 * answers with the CLUSTER-WIDE aggregate — the SAME (count, max_ts) the real
 * holder reports.  The resync's selection loop keeps the first reporter, so
 * gossip order decided the winner; when it was the empty peer, the follow-up
 * `SELECT * FROM <t> WHERE ts > N` (not a scalar agg, so no coordinator) handed
 * back nothing.  The node logged success having converged zero rows — and the
 * startup resync runs exactly once, so it stayed empty for the life of the
 * process.
 *
 * THE FIX.  TSDB_RPC_LOCAL_TABLE_STATS: the peer measures ITSELF with
 * tsdb_cluster_local_table_stats.  No query engine on that path, so no scatter
 * to hijack — a peer holding nothing reports (0, INT64_MIN) and can never be
 * mistaken for the holder.
 *
 * TOPOLOGY (full replication — TSDB_SHARD_REPLICA_N unset; sharding masks the
 * bug because a non-owner forwards SELECT * to the owner and does deliver rows):
 *
 *   node1  boots ALONE, creates `pt`, writes NROWS rows.   -> the sole HOLDER
 *   node2  boots after, creates `pt` locally, zero rows.   -> the PHANTOM
 *   node0  in-process here, creates `pt` locally, zero rows -> must converge
 *
 * ASSERTIONS (all deterministic — none depends on gossip order or on where the
 * hashring happens to place the table):
 *   [1] probing the HOLDER reports its real (count, max_ts)
 *   [2] probing the PHANTOM reports (0, INT64_MIN) — the whole point
 *   [3] node0's resync pulls NROWS and the rows are readable afterwards
 *
 * The legacy FED_QUERY answer from the phantom is PRINTED, never asserted: it
 * only equals the cluster total when the ring assignee for `pt` is the holder,
 * and this test must not fail on a hashring draw.  Pre-fix, assertion [2] fails
 * whenever that draw goes that way and assertion [3] is a coin flip on gossip
 * order; post-fix both hold unconditionally.
 *
 * Forks processes and binds 127.0.0.1 ports -> CLUSTER_TEST_SRCS, run by
 * `make test-cluster`, NOT part of the socket-free default suite.
 */

#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"
#include "../src/cluster/rpc.h"
#include "../src/federation/fedrpc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define N_CHILDREN 2
#define N_NODES    3
#define NROWS      5000
#define TABLE      "pt"

#define PORT0 28181   /* node0 — in-process (empty)   */
#define PORT1 28182   /* node1 — holder              */
#define PORT2 28183   /* node2 — phantom             */

static pid_t g_child_pids[N_CHILDREN];
static int   g_n_children = 0;
static char  g_tmp_dirs[N_NODES][256];

static void test_cleanup(void) {
    for (int i = 0; i < g_n_children; i++) if (g_child_pids[i] > 0) kill(g_child_pids[i], SIGTERM);
    for (int i = 0; i < g_n_children; i++) if (g_child_pids[i] > 0) { int st; waitpid(g_child_pids[i], &st, 0); g_child_pids[i] = 0; }
    for (int i = 0; i < N_NODES; i++) if (g_tmp_dirs[i][0]) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", g_tmp_dirs[i]); system(c); }
}

#define ASSERT(cond) do { if (!(cond)) { fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #cond, __FILE__, __LINE__); test_cleanup(); exit(1); } } while (0)
#define ASSERT_OK(rc) do { int _rc=(rc); if (_rc!=TSDB_OK){ fprintf(stderr,"ASSERT_OK: %s == %d (%s)  [%s:%d]\n",#rc,_rc,tsdb_errstr(_rc),__FILE__,__LINE__); test_cleanup(); exit(1);} } while (0)

static void sig_cleanup(int sig) { (void)sig; test_cleanup(); exit(0); }

static int64_t test_ts(int row) {
    return (int64_t)1743465600LL * 1000000000LL + (int64_t)row * 1000000LL;
}

static tsdb_col_t g_cols[] = {
    { "ts",    TSDB_TYPE_TIMESTAMP },
    { "value", TSDB_TYPE_FLOAT64   },
    { "n",     TSDB_TYPE_INT64     },
};

static void create_table(tsdb_db_t *db) {
    int rc = tsdb_create_table(db, TABLE, g_cols, 3, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) ASSERT_OK(rc);
}

static void write_rows(tsdb_db_t *db, int count) {
    tsdb_table_t *tbl = NULL; ASSERT_OK(tsdb_open_table(db, TABLE, &tbl));
    tsdb_batch_t *b = NULL;   ASSERT_OK(tsdb_batch_begin(tbl, &b));
    for (int i = 0; i < count; i++) {
        ASSERT_OK(tsdb_batch_row_ts(b, test_ts(i)));
        ASSERT_OK(tsdb_batch_row_f64(b, 1, 100.0 + (i % 1000) * 0.01));
        ASSERT_OK(tsdb_batch_row_i64(b, 2, 1000L + i));
        ASSERT_OK(tsdb_batch_row_end(b));
    }
    ASSERT_OK(tsdb_batch_commit(b));
}

/* Child node: open, optionally create + fill the table, then park. */
typedef struct { int idx; int holder; char dir[256]; char rpc[64]; char seeds[256]; } node_cfg_t;

static void run_node(const node_cfg_t *cfg) {
    tsdb_db_t *db = NULL;
    int rc = tsdb_open_cluster(cfg->dir, cfg->rpc, cfg->seeds, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "[node%d] open_cluster: %s\n", cfg->idx, tsdb_errstr(rc));
        exit(1);
    }
    create_table(db);
    if (cfg->holder) write_rows(db, NROWS);
    printf("[node%d] up rpc=%s holder=%d pid=%d\n",
           cfg->idx, cfg->rpc, cfg->holder, (int)getpid());
    fflush(stdout);
    sigset_t ss; sigemptyset(&ss); sigsuspend(&ss);
    tsdb_close(db);
    exit(0);
}

static int tcp_can_connect(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return 0;
    struct sockaddr_in sa; memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET; sa.sin_port = htons((uint16_t)port);
    inet_aton(host, &sa.sin_addr);
    int flags = fcntl(fd, F_GETFL); fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    struct pollfd pfd = { fd, POLLOUT, 0 };
    int r = poll(&pfd, 1, timeout_ms), ok = 0;
    if (r > 0 && (pfd.revents & POLLOUT)) {
        int err = 0; socklen_t el = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el); ok = (err == 0);
    }
    close(fd); return ok;
}

static void wait_for_port(const char *host, int port, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        if (tcp_can_connect(host, port, 100)) return;
        usleep(100000); elapsed += 100;
    }
    fprintf(stderr, "Timeout waiting for %s:%d\n", host, port);
    test_cleanup(); exit(1);
}

static tsdb_rpc_conn_t *dial(int port) {
    char addr[64]; snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    return tsdb_rpc_connect(addr, 3000);
}

/* Rows physically readable from a data dir, via a fresh PLAIN (non-cluster)
 * open — no peers reachable, so nothing can be answered on our behalf. */
static int64_t plain_row_count(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) return -1;
    char qtl[128]; snprintf(qtl, sizeof(qtl), "SELECT ts FROM %s", TABLE);
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, qtl, &r) != TSDB_OK) { tsdb_close(db); return -2; }
    int64_t n = 0; while (tsdb_result_next(r) == 1) n++;
    tsdb_result_free(r); tsdb_close(db);
    return n;
}

int main(void) {
    signal(SIGINT, sig_cleanup); signal(SIGTERM, sig_cleanup);
    printf("=== test_ae_phantom_peer: anti-entropy must not pick an empty peer ===\n");

    /* FULL replication.  With TSDB_SHARD_REPLICA_N set, a non-owner forwards
     * SELECT * to the owner, so even the phantom delivers rows and the bug is
     * masked — the failure needs the default, fully-replicated mode. */
    unsetenv("TSDB_SHARD_REPLICA_N");
    /* Keep the background startup resync out of the way; this test drives
     * tsdb_cluster_resync_table explicitly. */
    setenv("TSDB_ANTIENTROPY_DELAY_MS", "600000", 1);

    for (int i = 0; i < N_NODES; i++)
        snprintf(g_tmp_dirs[i], sizeof(g_tmp_dirs[i]),
                 "/tmp/tsdb_ae_phantom_%d_%d", (int)getpid(), i);

    char seeds[256];
    snprintf(seeds, sizeof(seeds), "127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d",
             PORT0 - 1, PORT1 - 1, PORT2 - 1);

    /* --- node1 boots ALONE and becomes the sole holder ------------------- */
    node_cfg_t c1 = { .idx = 1, .holder = 1 };
    snprintf(c1.dir,  sizeof(c1.dir),  "%s", g_tmp_dirs[1]);
    snprintf(c1.rpc,  sizeof(c1.rpc),  "127.0.0.1:%d", PORT1);
    snprintf(c1.seeds, sizeof(c1.seeds), "%s", seeds);
    pid_t p1 = fork();
    if (p1 < 0) { perror("fork"); test_cleanup(); exit(1); }
    if (p1 == 0) { signal(SIGINT, SIG_DFL); signal(SIGTERM, SIG_DFL); run_node(&c1); }
    g_child_pids[g_n_children++] = p1;
    wait_for_port("127.0.0.1", PORT1, 15000);
    sleep(2);   /* let node1 finish creating + writing before anyone joins */

    /* --- node2 joins: schema only, ZERO rows: the phantom ---------------- */
    node_cfg_t c2 = { .idx = 2, .holder = 0 };
    snprintf(c2.dir,  sizeof(c2.dir),  "%s", g_tmp_dirs[2]);
    snprintf(c2.rpc,  sizeof(c2.rpc),  "127.0.0.1:%d", PORT2);
    snprintf(c2.seeds, sizeof(c2.seeds), "%s", seeds);
    pid_t p2 = fork();
    if (p2 < 0) { perror("fork"); test_cleanup(); exit(1); }
    if (p2 == 0) { signal(SIGINT, SIG_DFL); signal(SIGTERM, SIG_DFL); run_node(&c2); }
    g_child_pids[g_n_children++] = p2;
    wait_for_port("127.0.0.1", PORT2, 15000);

    /* --- node0 in-process: schema only, ZERO rows, must converge --------- */
    char addr0[64]; snprintf(addr0, sizeof(addr0), "127.0.0.1:%d", PORT0);
    tsdb_db_t *db0 = NULL;
    ASSERT_OK(tsdb_open_cluster(g_tmp_dirs[0], addr0, seeds, &db0));
    create_table(db0);
    tsdb_cluster_wait_ready(db0, 3, 15000);
    printf("[test] alive nodes seen by node0: %d\n", tsdb_cluster_alive_count(db0));
    ASSERT(tsdb_cluster_alive_count(db0) >= 3);

    uint64_t lc = 0; int64_t lm = 0;
    ASSERT_OK(tsdb_cluster_local_table_stats(db0, TABLE, &lc, &lm));
    printf("[test] node0 local stats before resync: count=%llu max_ts=%lld\n",
           (unsigned long long)lc, (long long)lm);
    ASSERT(lc == 0);           /* node0 really is empty */

    /* --- [1] probing the HOLDER --------------------------------------- */
    tsdb_rpc_conn_t *ch = dial(PORT1);
    ASSERT(ch != NULL);
    uint64_t hc = 0; int64_t hm = 0;
    int hrc = tsdb_cluster_peer_table_stats_conn(ch, TABLE, &hc, &hm);
    printf("[test] holder  probe: rc=%d count=%llu max_ts=%lld\n",
           hrc, (unsigned long long)hc, (long long)hm);
    ASSERT(hrc == 0);
    ASSERT(hc == (uint64_t)NROWS);
    ASSERT(hm == test_ts(NROWS - 1));
    printf("[test] PASS: the holder reports its real rows\n");

    /* --- [2] probing the PHANTOM -------------------------------------- */
    tsdb_rpc_conn_t *cp = dial(PORT2);
    ASSERT(cp != NULL);

    /* Diagnostic only: what the LEGACY probe would have believed.  Equals the
     * cluster total exactly when the ring assignee for `pt` is the holder, so
     * it is printed, never asserted — a hashring draw must not fail a build. */
    tsdb_result_t *legacy = NULL;
    char q[128]; snprintf(q, sizeof(q), "SELECT count(*), max(ts) FROM %s", TABLE);
    if (fedrpc_query(cp, q, 5000, &legacy) == TSDB_OK && legacy &&
        tsdb_result_next(legacy))
    {
        long long lcount = (long long)tsdb_result_i64(legacy, 0);
        printf("[test] phantom via legacy `SELECT count(*)`: %lld  %s\n", lcount,
               lcount >= NROWS
                 ? "<== PHANTOM CONFIRMED (peer holds nothing, echoes the cluster)"
                 : "(answered locally this run — ring placement; not exercised)");
    } else {
        printf("[test] phantom legacy query did not answer (diagnostic only)\n");
    }
    if (legacy) tsdb_result_free(legacy);

    uint64_t pc = 12345; int64_t pm = 999;
    int prc = tsdb_cluster_peer_table_stats_conn(cp, TABLE, &pc, &pm);
    printf("[test] phantom probe: rc=%d count=%llu max_ts=%lld\n",
           prc, (unsigned long long)pc, (long long)pm);
    ASSERT(prc == 0);
    ASSERT(pc == 0);              /* it holds nothing, and now says so */
    ASSERT(pm == INT64_MIN);
    printf("[test] PASS: the empty peer reports (0, INT64_MIN)\n");

    /* Being seeded with the local numbers, the resync only accepts a peer
     * STRICTLY ahead — so a truthful empty peer can never be chosen. */
    ASSERT(tsdb_antientropy_decide(lc, lm, pc, pm) == TSDB_AE_UP_TO_DATE);
    ASSERT(tsdb_antientropy_decide(lc, lm, hc, hm) == TSDB_AE_TAIL_PULL);

    /* --- [3] node0 converges, whatever order gossip lists the peers in -- */
    int pulled = 0;
    int rrc = tsdb_cluster_resync_table(db0, TABLE, &pulled);
    printf("[test] resync: rc=%d pulled=%d\n", rrc, pulled);
    ASSERT(rrc == TSDB_OK);
    ASSERT(pulled == NROWS);

    tsdb_rpc_conn_close(ch);
    tsdb_rpc_conn_close(cp);

    lc = 0; lm = 0;
    ASSERT_OK(tsdb_cluster_local_table_stats(db0, TABLE, &lc, &lm));
    printf("[test] node0 local stats after resync: count=%llu max_ts=%lld\n",
           (unsigned long long)lc, (long long)lm);
    ASSERT(lc == (uint64_t)NROWS);

    tsdb_close_cluster(db0);
    tsdb_close(db0);

    int64_t on_disk = plain_row_count(g_tmp_dirs[0]);
    printf("[test] node0 rows readable from a fresh plain open: %lld\n",
           (long long)on_disk);
    ASSERT(on_disk == NROWS);
    printf("[test] PASS: node0 converged to %d rows\n", NROWS);

    test_cleanup();
    printf("\n=== test_ae_phantom_peer: ALL PASS ===\n");
    return 0;
}
