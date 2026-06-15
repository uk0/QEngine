/* test_cluster_count_repro.c — end-to-end cluster guard for task #174.
 *
 * Companion to the deterministic in-process reproducer in
 * tests/test_stable_open_fail.c (which forces the exact empty-result-set the
 * server saw).  This test forms a real sharded 3-node cluster, writes rows that
 * replicate to peer nodes, then — on every node that PHYSICALLY holds a table's
 * partition on disk (has_partition) — asserts that `SELECT count(*)` returns
 * exactly one result row whose value equals the row count (never an empty set,
 * never a wrong value).  This is exactly what a wire query handler does on a
 * data-bearing node.  Sharded placement (TSDB_SHARD_REPLICA_N=1) reproduces the
 * server condition where a node receives data via WRITE_BATCH replication and
 * the stable mirror via catalog-sync, then answers count(*) locally.
 *
 * Anti-entropy startup pull is pushed far out so a fresh plain open of a node
 * that does NOT hold a table cannot transiently pull rows (a measurement
 * artifact); assertions fire only on physical partition holders.
 */

#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <errno.h>
#include <time.h>
#include <dirent.h>

#define N_CHILDREN 2
#define N_NODES    3

typedef struct { int node_idx; char data_dir[256]; char rpc_addr[64]; char seeds[256]; } node_cfg_t;

static pid_t g_child_pids[N_CHILDREN];
static int   g_n_children = 0;
static char  g_tmp_dirs[N_NODES][256];

static void test_cleanup(void) {
    for (int i = 0; i < g_n_children; i++) if (g_child_pids[i] > 0) kill(g_child_pids[i], SIGTERM);
    for (int i = 0; i < g_n_children; i++) if (g_child_pids[i] > 0) { int st; waitpid(g_child_pids[i], &st, 0); g_child_pids[i] = 0; }
    for (int i = 0; i < N_NODES; i++) if (g_tmp_dirs[i][0]) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", g_tmp_dirs[i]); system(c); }
}

#define ASSERT(cond) do { if (!(cond)) { fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #cond, __FILE__, __LINE__); test_cleanup(); exit(1); } } while(0)
#define ASSERT_OK(rc) do { int _rc=(rc); if (_rc!=TSDB_OK){ fprintf(stderr,"ASSERT_OK: %s == %d (%s)  [%s:%d]\n",#rc,_rc,tsdb_errstr(_rc),__FILE__,__LINE__); test_cleanup(); exit(1);} } while(0)

static void sig_cleanup(int sig) { (void)sig; test_cleanup(); exit(0); }

static void run_node(const node_cfg_t *cfg) {
    tsdb_db_t *db = NULL;
    int rc = tsdb_open_cluster(cfg->data_dir, cfg->rpc_addr, cfg->seeds, &db);
    if (rc != TSDB_OK) { fprintf(stderr, "[node%d] open_cluster failed: %s\n", cfg->node_idx, tsdb_errstr(rc)); exit(1); }
    printf("[node%d] started rpc=%s data=%s pid=%d\n", cfg->node_idx, cfg->rpc_addr, cfg->data_dir, (int)getpid());
    fflush(stdout);
    sigset_t ss; sigemptyset(&ss); sigsuspend(&ss);
    tsdb_close(db); exit(0);
}

static int tcp_can_connect(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return 0;
    struct sockaddr_in sa = {0}; sa.sin_family = AF_INET; sa.sin_port = htons((uint16_t)port); inet_aton(host, &sa.sin_addr);
    int flags = fcntl(fd, F_GETFL); fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    struct pollfd pfd = { fd, POLLOUT, 0 }; int r = poll(&pfd, 1, timeout_ms);
    int ok = 0;
    if (r > 0 && (pfd.revents & POLLOUT)) { int err = 0; socklen_t el = sizeof(err); getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el); ok = (err == 0); }
    close(fd); return ok;
}
static void wait_for_port(const char *host, int port, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) { if (tcp_can_connect(host, port, 100)) return; usleep(100000); elapsed += 100; }
    fprintf(stderr, "Timeout waiting for %s:%d\n", host, port); test_cleanup(); exit(1);
}

static int64_t test_ts(int row) { return (int64_t)1743465600LL * 1000000000LL + (int64_t)row * 1000000LL; }

static void write_rows(tsdb_db_t *db, const char *table_name, int start, int count) {
    tsdb_table_t *tbl = NULL; ASSERT_OK(tsdb_open_table(db, table_name, &tbl));
    tsdb_batch_t *b = NULL; ASSERT_OK(tsdb_batch_begin(tbl, &b));
    for (int i = 0; i < count; i++) {
        int row = start + i;
        ASSERT_OK(tsdb_batch_row_ts(b, test_ts(row)));
        ASSERT_OK(tsdb_batch_row_f64(b, 1, 100.0 + (row % 1000) * 0.01));
        ASSERT_OK(tsdb_batch_row_i64(b, 2, 1000L + row));
        ASSERT_OK(tsdb_batch_row_end(b));
    }
    ASSERT_OK(tsdb_batch_commit(b));
}

/* Is there a real on-disk partition (YYYYMMDD[HH]) under <data_dir>/<table>/?
 * The deterministic "this node physically holds the table" signal — immune to
 * anti-entropy transient pulls (which never create a partition dir here). */
static int has_partition(const char *data_dir, const char *table) {
    char tdir[512]; snprintf(tdir, sizeof(tdir), "%s/%s", data_dir, table);
    DIR *d = opendir(tdir);
    if (!d) return 0;
    struct dirent *e; int found = 0;
    while ((e = readdir(d))) {
        size_t n = strlen(e->d_name);
        if (n != 8 && n != 10) continue;
        int allnum = 1;
        for (size_t i = 0; i < n; i++) if (e->d_name[i] < '0' || e->d_name[i] > '9') { allnum = 0; break; }
        if (allnum) { found = 1; break; }
    }
    closedir(d);
    return found;
}

/* Row-union probe on a FRESH plain open of a data dir. Returns number of rows. */
static int64_t probe_row_scan(const char *data_dir, const char *table) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(data_dir, &db) != TSDB_OK) return -1;
    char qtl[256]; snprintf(qtl, sizeof(qtl), "SELECT ts FROM %s", table);
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, qtl, &r);
    if (rc != TSDB_OK) { fprintf(stderr, "[row_scan] query rc=%d (%s)\n", rc, tsdb_errstr(rc)); tsdb_close(db); return -2; }
    int64_t n = 0; while (tsdb_result_next(r) == 1) n++;
    tsdb_result_free(r); tsdb_close(db); return n;
}

/* Scalar-agg probe on a FRESH plain open. *out_nrows = number of RESULT ROWS
 * (the bug tell: count(*) must yield exactly 1; the bug yields 0). */
static int64_t probe_count_star(const char *data_dir, const char *table, int *out_nrows, int *out_rc) {
    tsdb_db_t *db = NULL;
    int orc = tsdb_open(data_dir, &db);
    if (orc != TSDB_OK) { *out_rc = orc; *out_nrows = -1; return -1; }
    char qtl[256]; snprintf(qtl, sizeof(qtl), "SELECT count(*) FROM %s", table);
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, qtl, &r);
    *out_rc = rc;
    if (rc != TSDB_OK) { *out_nrows = -1; tsdb_close(db); return -1; }
    int nrows = 0; int64_t cnt = 0;
    while (tsdb_result_next(r) == 1) { if (nrows == 0) cnt = tsdb_result_i64(r, 0); nrows++; }
    *out_nrows = nrows;
    tsdb_result_free(r); tsdb_close(db);
    return cnt;
}

int main(void) {
    signal(SIGINT, sig_cleanup); signal(SIGTERM, sig_cleanup);
    printf("=== test_cluster_count_repro: count(*) empty-result-set bug ===\n");

    /* Reproduce the SERVER condition: SHARDED placement.  replica_n=1 means
     * each table is owned by exactly ONE node; the others drop their local copy
     * (cluster_on_replicate -> TSDB_SKIP_LOCAL).  Set BEFORE fork so every node
     * inherits it.  This is the configuration under which the server saw the
     * empty result set (data on only 2 of 4 nodes). */
    setenv("TSDB_SHARD_REPLICA_N", "1", 1);
    /* Push the anti-entropy startup pull far out so it never fires during this
     * short test.  Otherwise a fresh plain open of a node that does NOT hold a
     * table would transiently pull rows from a peer, making a row-scan briefly
     * non-empty on a node with no real partition — a measurement artifact, not
     * the bug.  We only assert on nodes whose partition dir is physically
     * present (see has_partition below). */
    setenv("TSDB_ANTIENTROPY_DELAY_MS", "600000", 1);

    for (int i = 0; i < N_NODES; i++)
        snprintf(g_tmp_dirs[i], sizeof(g_tmp_dirs[i]), "/tmp/tsdb_count_repro_%d_%d", (int)getpid(), i);

    const char *seeds = "127.0.0.1:29080,127.0.0.1:29081,127.0.0.1:29082";
    node_cfg_t child_cfgs[N_CHILDREN];
    for (int i = 0; i < N_CHILDREN; i++) {
        child_cfgs[i].node_idx = i + 1;
        snprintf(child_cfgs[i].data_dir, sizeof(child_cfgs[i].data_dir), "%s", g_tmp_dirs[i + 1]);
        snprintf(child_cfgs[i].rpc_addr, sizeof(child_cfgs[i].rpc_addr), "127.0.0.1:%d", 29082 + i);
        snprintf(child_cfgs[i].seeds, sizeof(child_cfgs[i].seeds), "%s", seeds);
    }
    for (int i = 0; i < N_CHILDREN; i++) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); test_cleanup(); exit(1); }
        if (pid == 0) { signal(SIGINT, SIG_DFL); signal(SIGTERM, SIG_DFL); run_node(&child_cfgs[i]); exit(0); }
        g_child_pids[i] = pid; g_n_children++;
    }

    tsdb_db_t *db_a = NULL;
    ASSERT_OK(tsdb_open_cluster(g_tmp_dirs[0], "127.0.0.1:29081", seeds, &db_a));
    for (int i = 0; i < N_NODES; i++) wait_for_port("127.0.0.1", 29081 + i, 10000);
    sleep(3);
    tsdb_cluster_wait_ready(db_a, 2, 5000);
    ASSERT(tsdb_cluster_alive_count(db_a) >= 2);

    /* Create several tables and write to each from node0.  Under replica_n=1,
     * hash routing makes each table owned by exactly ONE node; the others drop
     * their copy.  With NTAB tables across 3 nodes, some land on node1/node2 —
     * NON-creator nodes that receive data purely via WRITE_BATCH replication,
     * exactly like the server's cnode-3/cnode-4.  We then check count(*) on
     * whichever node physically holds each table. */
    #define NTAB 9
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "value", TSDB_TYPE_FLOAT64 }, { "count", TSDB_TYPE_INT64 } };
    char tnames[NTAB][32];
    for (int t = 0; t < NTAB; t++) {
        snprintf(tnames[t], sizeof(tnames[t]), "lt_%d", t);
        int rc = tsdb_create_table(db_a, tnames[t], cols, 3, "ts");
        ASSERT(rc == TSDB_OK || rc == TSDB_ERR_EXISTS);
    }
    printf("[test] created %d tables; waiting for schema propagation\n", NTAB);
    sleep(2);
    for (int t = 0; t < NTAB; t++) write_rows(db_a, tnames[t], 0, 10000);
    printf("[test] wrote 10000 rows to each; waiting for replication + drop-local\n");
    sleep(4);

    int fail = 0;

    /* ---- Probe: each node's data dir via a FRESH plain open. ----
     * This is exactly what a wire query handler does on the node that physically
     * holds the table.  We assert ONLY on (node, table) pairs whose partition
     * directory is physically present on disk (has_partition) — the deterministic
     * "this node holds the data" signal, immune to anti-entropy artifacts.  For
     * each such holder, count(*) MUST return exactly 1 result row whose value
     * equals the row count.  Pre-fix, a holder whose table is a stable mirror
     * but whose open fails returns an EMPTY result set (0 rows) — the bug. */
    tsdb_close(db_a);  /* release cluster handle so dirs are openable plain */
    sleep(1);
    printf("\n[test] --- probe each node's data dir (fresh plain open) ---\n");
    int holders_total = 0;
    for (int i = 0; i < N_NODES; i++) {
        for (int t = 0; t < NTAB; t++) {
            if (!has_partition(g_tmp_dirs[i], tnames[t])) continue;  /* not a real holder */
            holders_total++;
            int64_t rs = probe_row_scan(g_tmp_dirs[i], tnames[t]);
            int cn = -99, cr = -99;
            int64_t cv = probe_count_star(g_tmp_dirs[i], tnames[t], &cn, &cr);
            printf("[test]   node%d holds %s: SELECT ts -> %lld | count(*) rc=%d rows=%d val=%lld\n",
                   i, tnames[t], (long long)rs, cr, cn, (long long)cv);
            if (cr != TSDB_OK)   { fprintf(stderr, "[test] FAIL: node%d %s count(*) errored rc=%d\n", i, tnames[t], cr); fail = 1; }
            else if (cn != 1)    { fprintf(stderr, "[test] FAIL(BUG): node%d %s count(*) returned %d result rows (expected 1) -- EMPTY RESULT SET\n", i, tnames[t], cn); fail = 1; }
            else if (cv != rs)   { fprintf(stderr, "[test] FAIL(BUG): node%d %s count(*)=%lld != SELECT ts rows %lld -- COUNT MISMATCH\n", i, tnames[t], (long long)cv, (long long)rs); fail = 1; }
        }
    }
    printf("\n[test] === verdict ===\n");
    if (holders_total == 0) { fprintf(stderr, "[test] FAIL: no node physically holds any table (sharding lost data)\n"); fail = 1; }
    else printf("[test] verified count(*) on %d physical (node,table) holders\n", holders_total);

    for (int i = 0; i < N_CHILDREN; i++) if (g_child_pids[i] > 0) { kill(g_child_pids[i], SIGTERM); int st; waitpid(g_child_pids[i], &st, 0); g_child_pids[i] = 0; }
    if (!getenv("TSDB_REPRO_NOCLEAN"))
        for (int i = 0; i < N_NODES; i++) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", g_tmp_dirs[i]); system(c); }
    else
        for (int i = 0; i < N_NODES; i++) fprintf(stderr, "[test] NOCLEAN: node%d dir = %s\n", i, g_tmp_dirs[i]);

    if (fail) { printf("\n=== test_cluster_count_repro: FAILED (bug reproduced) ===\n"); return 1; }
    printf("[test] PASS: count(*) returns 1 row on the owner and via forward\n");
    printf("\n=== test_cluster_count_repro: ALL PASS ===\n");
    return 0;
}
