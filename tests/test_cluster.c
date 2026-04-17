/* test_cluster.c — end-to-end cluster test.
 *
 * Starts 3 tsdb nodes using fork() on localhost ports:
 *   Node A: RPC 28081, Gossip 28080
 *   Node B: RPC 28082, Gossip 28081 (UDP/TCP separate namespaces, ok)
 *   Node C: RPC 28083, Gossip 28082
 *
 * Test sequence:
 *   1. Fork 3 child processes, each runs a cluster node.
 *   2. Parent waits until all 3 nodes can accept TCP connections.
 *   3. Node A: create table, insert 10000 rows.
 *   4. Wait for replication to propagate.
 *   5. Node B: query row count, verify >= 10000.
 *   6. Kill Node C.
 *   7. Write another 1000 rows on A (W=2 should succeed with A+B).
 *   8. Node C restart + backfill: TODO (mark as skipped).
 *   9. Cleanup.
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
#include <assert.h>
#include <time.h>

/* ---- Test utilities ------------------------------------------------------- */

#define ASSERT(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", \
                    #cond, __FILE__, __LINE__); \
            test_cleanup(); \
            exit(1); \
        } \
    } while(0)

#define ASSERT_OK(rc) \
    do { \
        int _rc = (rc); \
        if (_rc != TSDB_OK) { \
            fprintf(stderr, "ASSERT_OK: %s == %d (%s)  [%s:%d]\n", \
                    #rc, _rc, tsdb_errstr(_rc), __FILE__, __LINE__); \
            test_cleanup(); \
            exit(1); \
        } \
    } while(0)

/* ---- Node configuration -------------------------------------------------- */

#define N_NODES 3

typedef struct {
    int    node_idx;
    char   data_dir[256];
    char   rpc_addr[64];
    char   gossip_addr[64];
    char   seeds[256];
} node_cfg_t;

/* ---- Global child PIDs for cleanup --------------------------------------- */

static pid_t g_child_pids[N_NODES];
static int   g_n_children = 0;
static char  g_tmp_dirs[N_NODES][256];

static void test_cleanup(void) {
    for (int i = 0; i < g_n_children; i++) {
        if (g_child_pids[i] > 0) {
            kill(g_child_pids[i], SIGTERM);
        }
    }
    /* Wait for children. */
    for (int i = 0; i < g_n_children; i++) {
        if (g_child_pids[i] > 0) {
            int st;
            waitpid(g_child_pids[i], &st, 0);
            g_child_pids[i] = 0;
        }
    }
    /* Remove temp directories. */
    for (int i = 0; i < N_NODES; i++) {
        if (g_tmp_dirs[i][0]) {
            char cmd[512];
            snprintf(cmd, sizeof(cmd), "rm -rf %s", g_tmp_dirs[i]);
            system(cmd);
        }
    }
}

static void sig_cleanup(int sig) {
    (void)sig;
    test_cleanup();
    exit(0);
}

/* ---- Child node process -------------------------------------------------- */

static void run_node(const node_cfg_t *cfg) {
    /* This runs in the child process. */
    tsdb_db_t *db = NULL;
    int rc = tsdb_open_cluster(cfg->data_dir, cfg->rpc_addr, cfg->seeds, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "[node%d] open_cluster failed: %s\n",
                cfg->node_idx, tsdb_errstr(rc));
        exit(1);
    }

    printf("[node%d] started  rpc=%s  data=%s  pid=%d\n",
           cfg->node_idx, cfg->rpc_addr, cfg->data_dir, (int)getpid());
    fflush(stdout);

    /* Wait for signals. */
    sigset_t ss;
    sigemptyset(&ss);
    for (;;) {
        sigsuspend(&ss);
        /* We'll only be woken by a signal; clean exit on SIGTERM. */
        break;
    }

    tsdb_close(db);
    exit(0);
}

/* ---- TCP readiness probe ------------------------------------------------- */

static int tcp_can_connect(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;

    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton(host, &sa.sin_addr);

    /* Non-blocking connect. */
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    connect(fd, (struct sockaddr *)&sa, sizeof(sa));

    struct pollfd pfd = { fd, POLLOUT, 0 };
    int r = poll(&pfd, 1, timeout_ms);

    int ok = 0;
    if (r > 0 && (pfd.revents & POLLOUT)) {
        int err = 0;
        socklen_t el = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el);
        ok = (err == 0);
    }
    close(fd);
    return ok;
}

static void wait_for_port(const char *host, int port, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        if (tcp_can_connect(host, port, 100)) return;
        usleep(100000); /* 100ms */
        elapsed += 100;
    }
    fprintf(stderr, "Timeout waiting for %s:%d\n", host, port);
    test_cleanup();
    exit(1);
}

/* ---- Timestamp helpers --------------------------------------------------- */

static int64_t test_ts(int row) {
    /* 2026-04-01 UTC: 1743465600 seconds */
    return (int64_t)1743465600LL * 1000000000LL + (int64_t)row * 1000000LL;
}

/* ---- Test: write N rows on a DB ------------------------------------------ */

static void write_rows(tsdb_db_t *db, const char *table_name, int start, int count) {
    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, table_name, &tbl));

    tsdb_batch_t *b = NULL;
    ASSERT_OK(tsdb_batch_begin(tbl, &b));

    for (int i = 0; i < count; i++) {
        int row = start + i;
        ASSERT_OK(tsdb_batch_row_ts(b, test_ts(row)));
        ASSERT_OK(tsdb_batch_row_f64(b, 1, 100.0 + (row % 1000) * 0.01));
        ASSERT_OK(tsdb_batch_row_i64(b, 2, 1000L + row));
        ASSERT_OK(tsdb_batch_row_end(b));
    }
    ASSERT_OK(tsdb_batch_commit(b));
}

/* ---- Count rows via query ------------------------------------------------ */

static int64_t count_rows(tsdb_db_t *db, const char *table_name) {
    char qtl[256];
    snprintf(qtl, sizeof(qtl), "SELECT ts FROM %s", table_name);
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, qtl, &r);
    if (rc != TSDB_OK) return -1;

    int64_t n = 0;
    while (tsdb_result_next(r) == 1) n++;
    tsdb_result_free(r);
    return n;
}

/* ---- Main test ------------------------------------------------------------ */

int main(void) {
    signal(SIGINT,  sig_cleanup);
    signal(SIGTERM, sig_cleanup);

    printf("=== test_cluster: 3-node gossip + replication ===\n");

    /* Set up temp directories. */
    for (int i = 0; i < N_NODES; i++) {
        snprintf(g_tmp_dirs[i], sizeof(g_tmp_dirs[i]),
                 "/tmp/tsdb_test_cluster_%d_%d", (int)getpid(), i);
    }

    /* Node configurations. */
    node_cfg_t cfgs[N_NODES];
    /* Gossip port = RPC port - 1:
     *   Node0: rpc=28081 gossip=28080
     *   Node1: rpc=28082 gossip=28081 (UDP, so no conflict with TCP 28081)
     *   Node2: rpc=28083 gossip=28082
     */
    for (int i = 0; i < N_NODES; i++) {
        cfgs[i].node_idx = i;
        snprintf(cfgs[i].data_dir,    sizeof(cfgs[i].data_dir),    "%s", g_tmp_dirs[i]);
        snprintf(cfgs[i].rpc_addr,    sizeof(cfgs[i].rpc_addr),    "127.0.0.1:%d", 28081 + i);
        snprintf(cfgs[i].gossip_addr, sizeof(cfgs[i].gossip_addr), "127.0.0.1:%d", 28080 + i);
        /* All nodes seed to node 0's gossip port. */
        snprintf(cfgs[i].seeds, sizeof(cfgs[i].seeds),
                 "127.0.0.1:28080,127.0.0.1:28081,127.0.0.1:28082");
    }

    /* Fork 3 child processes. */
    for (int i = 0; i < N_NODES; i++) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            test_cleanup();
            exit(1);
        }
        if (pid == 0) {
            /* Child process: run the node. */
            /* Reset signal handlers. */
            signal(SIGINT, SIG_DFL);
            signal(SIGTERM, SIG_DFL);
            run_node(&cfgs[i]);
            /* run_node never returns. */
            exit(0);
        }
        g_child_pids[i] = pid;
        g_n_children++;
        printf("[test] forked node%d  pid=%d  rpc=%s\n",
               i, (int)pid, cfgs[i].rpc_addr);
    }

    /* Wait for all 3 RPC ports to be ready. */
    printf("[test] waiting for cluster nodes to start...\n");
    for (int i = 0; i < N_NODES; i++) {
        wait_for_port("127.0.0.1", 28081 + i, 10000);
        printf("[test] node%d RPC port ready\n", i);
    }

    /* Let gossip converge (~2s). */
    printf("[test] waiting for gossip convergence (2s)...\n");
    sleep(2);

    /* ---- Open node A (local) for writes. ---------------------------------- */
    printf("[test] opening node A database for writes...\n");
    tsdb_db_t *db_a = NULL;
    ASSERT_OK(tsdb_open_cluster(g_tmp_dirs[0],
                                "127.0.0.1:28081",
                                "127.0.0.1:28080,127.0.0.1:28081,127.0.0.1:28082",
                                &db_a));

    /* Wait until node A sees at least 1 other node. */
    tsdb_cluster_wait_ready(db_a, 2, 5000);

    int alive_a = tsdb_cluster_alive_count(db_a);
    printf("[test] node A sees %d alive node(s)\n", alive_a);

    char stats_buf[4096];
    tsdb_cluster_stats(db_a, stats_buf, sizeof(stats_buf));
    printf("[test] cluster view: %s\n", stats_buf);

    /* ---- Create table on node A (schema sync to peers). ------------------- */
    printf("[test] creating table 'metrics' on node A...\n");
    tsdb_col_t cols[] = {
        { "ts",     TSDB_TYPE_TIMESTAMP },
        { "value",  TSDB_TYPE_FLOAT64   },
        { "count",  TSDB_TYPE_INT64     },
    };
    int rc = tsdb_create_table(db_a, "metrics", cols, 3, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) {
        fprintf(stderr, "create_table failed: %s\n", tsdb_errstr(rc));
        test_cleanup();
        exit(1);
    }

    /* Allow schema to propagate. */
    sleep(1);

    /* ---- Write 10000 rows on node A. -------------------------------------- */
    printf("[test] writing 10000 rows on node A...\n");
    int64_t t0 = (int64_t)time(NULL);
    write_rows(db_a, "metrics", 0, 10000);
    int64_t t1 = (int64_t)time(NULL);
    printf("[test] 10000 rows written in %llds\n", (long long)(t1 - t0));

    /* ---- Wait for replication to propagate. ------------------------------- */
    printf("[test] waiting 2s for replication...\n");
    sleep(2);

    /* ---- Open node B and query. ------------------------------------------- */
    printf("[test] opening node B for queries...\n");
    tsdb_db_t *db_b = NULL;
    ASSERT_OK(tsdb_open_cluster(g_tmp_dirs[1],
                                "127.0.0.1:28082",
                                "127.0.0.1:28080,127.0.0.1:28081,127.0.0.1:28082",
                                &db_b));

    /* Wait for node B to see the table. */
    sleep(1);

    int64_t rows_b = count_rows(db_b, "metrics");
    printf("[test] node B row count: %lld (expected ~10000)\n", (long long)rows_b);
    ASSERT(rows_b >= 10000);
    printf("[test] PASS: node B has replicated data\n");

    /* ---- Kill node C. ----------------------------------------------------- */
    printf("[test] killing node C (pid=%d)...\n", (int)g_child_pids[2]);
    kill(g_child_pids[2], SIGTERM);
    {
        int st;
        waitpid(g_child_pids[2], &st, 0);
    }
    g_child_pids[2] = 0;
    sleep(1);

    /* ---- Write 1000 more rows on A (W=2 with A+B should succeed). --------- */
    printf("[test] writing 1000 more rows on A with C dead (W=2 test)...\n");
    write_rows(db_a, "metrics", 10000, 1000);
    printf("[test] PASS: W=2 write succeeded with one node down\n");

    /* ---- Verify total on node B. ------------------------------------------ */
    sleep(1);
    int64_t rows_b2 = count_rows(db_b, "metrics");
    printf("[test] node B row count after second write: %lld (expected ~11000)\n",
           (long long)rows_b2);
    ASSERT(rows_b2 >= 11000);
    printf("[test] PASS: replication after node C failure\n");

    /* ---- TODO: restart C and verify backfill. ----------------------------- */
    printf("[test] TODO: node C restart + backfill not implemented\n");

    /* ---- Cluster stats. --------------------------------------------------- */
    tsdb_cluster_stats(db_a, stats_buf, sizeof(stats_buf));
    printf("[test] final cluster view: %s\n", stats_buf);

    /* ---- Cleanup. --------------------------------------------------------- */
    tsdb_close(db_a);
    tsdb_close(db_b);

    /* Kill remaining children. */
    for (int i = 0; i < N_NODES; i++) {
        if (g_child_pids[i] > 0) {
            kill(g_child_pids[i], SIGTERM);
            int st;
            waitpid(g_child_pids[i], &st, 0);
            g_child_pids[i] = 0;
        }
    }
    /* Remove temp data. */
    for (int i = 0; i < N_NODES; i++) {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", g_tmp_dirs[i]);
        system(cmd);
    }

    printf("\n=== test_cluster: ALL PASS ===\n");
    return 0;
}
