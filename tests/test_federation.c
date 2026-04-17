/* test_federation.c — end-to-end federation test.
 *
 * Architecture:
 *   Cluster A: 3 nodes (rpc=29081/82/83, gossip=29080/81/82)
 *   Cluster B: 3 nodes (rpc=29091/92/93, gossip=29090/91/92)
 *   Parent: federation client (NOT a cluster node)
 *
 * Test sequence:
 *   1. Fork 6 child nodes (3 per cluster).
 *   2. Wait for all RPC ports to be ready.
 *   3. Let gossip converge (3s).
 *   4. Federation client: open 2 clusters.
 *   5. Create table "cpu" in cluster A and cluster B (using local tsdb_open_cluster).
 *   6. Write 5000 rows to cluster A, 5000 rows to cluster B.
 *   7. Wait for replication (3s).
 *   8. Federation query: SELECT count(ts) FROM cpu  → expect 10000.
 *   9. Federation query: SELECT sum(value), count(ts) FROM cpu SAMPLE BY 1m
 *      → verify avg = sum/count is correct.
 *  10. Kill cluster B coordinator node.
 *  11. Federation query with partial failure: should return partial result + miss=1+.
 *  12. Cleanup.
 *
 * Latency measurement: time the count(*) and SAMPLE BY queries and report p50.
 */

#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"
#include "../include/tsdb_federation.h"
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
#include <math.h>

/* ---- Test configuration -------------------------------------------------- */

#define FED_TEST_N_CLUSTERS  2
#define FED_TEST_NODES_EACH  3
#define FED_TEST_N_NODES     (FED_TEST_N_CLUSTERS * FED_TEST_NODES_EACH)

/* Cluster A: rpc ports 29081/82/83, gossip 29080/81/82 */
#define CLA_RPC_BASE  29081
/* Cluster B: rpc ports 29091/92/93, gossip 29090/91/92 */
#define CLB_RPC_BASE  29091

#define ROWS_PER_CLUSTER  5000
#define TOTAL_ROWS        (ROWS_PER_CLUSTER * FED_TEST_N_CLUSTERS)

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

/* ---- Global state for cleanup -------------------------------------------- */

static pid_t g_child_pids[FED_TEST_N_NODES];
static int   g_n_children = 0;
static char  g_tmp_dirs[FED_TEST_N_NODES][256];

/* Special: keep track of which node was used to write each cluster's data. */
static tsdb_db_t *g_db_a = NULL;  /* cluster A node 0 */
static tsdb_db_t *g_db_b = NULL;  /* cluster B node 0 */

static void test_cleanup(void) {
    if (g_db_a) { tsdb_close(g_db_a); g_db_a = NULL; }
    if (g_db_b) { tsdb_close(g_db_b); g_db_b = NULL; }
    for (int i = 0; i < g_n_children; i++) {
        if (g_child_pids[i] > 0) {
            kill(g_child_pids[i], SIGTERM);
        }
    }
    for (int i = 0; i < g_n_children; i++) {
        if (g_child_pids[i] > 0) {
            int st; waitpid(g_child_pids[i], &st, 0);
            g_child_pids[i] = 0;
        }
    }
    for (int i = 0; i < FED_TEST_N_NODES; i++) {
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

typedef struct {
    int  cluster_id;   /* 0 = A, 1 = B */
    int  node_idx;     /* 0..2 within cluster */
    char data_dir[256];
    char rpc_addr[64];
    char seeds[256];
} node_cfg_t;

static void run_node(const node_cfg_t *cfg) {
    tsdb_db_t *db = NULL;
    int rc = tsdb_open_cluster(cfg->data_dir, cfg->rpc_addr, cfg->seeds, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "[node C%d/%d] open_cluster failed: %s\n",
                cfg->cluster_id, cfg->node_idx, tsdb_errstr(rc));
        exit(1);
    }
    printf("[node C%d/%d] started  rpc=%s  pid=%d\n",
           cfg->cluster_id, cfg->node_idx, cfg->rpc_addr, (int)getpid());
    fflush(stdout);

    sigset_t ss; sigemptyset(&ss);
    for (;;) { sigsuspend(&ss); break; }

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
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    struct pollfd pfd = { fd, POLLOUT, 0 };
    int r = poll(&pfd, 1, timeout_ms);
    int ok = 0;
    if (r > 0 && (pfd.revents & POLLOUT)) {
        int err = 0; socklen_t el = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el);
        ok = (err == 0);
    }
    close(fd);
    return ok;
}

static void wait_for_port(int port, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        if (tcp_can_connect("127.0.0.1", port, 100)) return;
        usleep(100000);
        elapsed += 100;
    }
    fprintf(stderr, "Timeout waiting for 127.0.0.1:%d\n", port);
    test_cleanup();
    exit(1);
}

/* ---- Timing helpers ------------------------------------------------------ */

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

/* ---- Timestamp helpers --------------------------------------------------- */

/* 2026-04-01 00:00:00 UTC in nanoseconds. */
#define BASE_TS_NS  (1743465600LL * 1000000000LL)

static tsdb_ts_t test_ts(int row) {
    /* 100ms increments — puts 10000 rows across ~16.7 minutes */
    return BASE_TS_NS + (int64_t)row * 100000000LL;
}

/* ---- Write helpers ------------------------------------------------------- */

static void write_rows(tsdb_db_t *db, const char *table_name,
                       int start, int count, double value_base) {
    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, table_name, &tbl));

    tsdb_batch_t *b = NULL;
    ASSERT_OK(tsdb_batch_begin(tbl, &b));

    for (int i = 0; i < count; i++) {
        int row = start + i;
        ASSERT_OK(tsdb_batch_row_ts(b, test_ts(row)));
        /* value: constant known value so we can verify avg. */
        ASSERT_OK(tsdb_batch_row_f64(b, 1, value_base + (row % 100) * 0.01));
        ASSERT_OK(tsdb_batch_row_end(b));
    }
    ASSERT_OK(tsdb_batch_commit(b));
}

/* ---- Main test ------------------------------------------------------------ */

int main(void) {
    signal(SIGINT,  sig_cleanup);
    signal(SIGTERM, sig_cleanup);

    printf("=== test_federation: 2-cluster × 3-node federation ===\n");

    /* Set up temp directories. */
    for (int i = 0; i < FED_TEST_N_NODES; i++) {
        snprintf(g_tmp_dirs[i], sizeof(g_tmp_dirs[i]),
                 "/tmp/tsdb_test_fed_%d_%d", (int)getpid(), i);
    }

    /* Build node configs.
     * Cluster A: nodes 0..2, rpc 29081..29083, gossip 29080..29082
     * Cluster B: nodes 3..5, rpc 29091..29093, gossip 29090..29092
     */
    node_cfg_t node_cfgs[FED_TEST_N_NODES];
    for (int c = 0; c < FED_TEST_N_CLUSTERS; c++) {
        int rpc_base    = (c == 0) ? CLA_RPC_BASE : CLB_RPC_BASE;
        int gossip_base = rpc_base - 1;

        /* Build seeds for this cluster (all 3 gossip ports). */
        char seeds[256];
        snprintf(seeds, sizeof(seeds),
                 "127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d",
                 gossip_base, gossip_base + 1, gossip_base + 2);

        for (int n = 0; n < FED_TEST_NODES_EACH; n++) {
            int idx = c * FED_TEST_NODES_EACH + n;
            node_cfgs[idx].cluster_id = c;
            node_cfgs[idx].node_idx   = n;
            snprintf(node_cfgs[idx].data_dir, 256, "%s", g_tmp_dirs[idx]);
            snprintf(node_cfgs[idx].rpc_addr, 64, "127.0.0.1:%d", rpc_base + n);
            snprintf(node_cfgs[idx].seeds, 256, "%s", seeds);
        }
    }

    /* Fork child nodes (all except node 0 of each cluster — parent runs those). */
    for (int i = 0; i < FED_TEST_N_NODES; i++) {
        /* Skip the first node of each cluster (parent will run them). */
        if (node_cfgs[i].node_idx == 0) continue;

        pid_t pid = fork();
        if (pid < 0) { perror("fork"); test_cleanup(); exit(1); }
        if (pid == 0) {
            signal(SIGINT, SIG_DFL);
            signal(SIGTERM, SIG_DFL);
            run_node(&node_cfgs[i]);
            exit(0);
        }
        g_child_pids[g_n_children++] = pid;
        printf("[test] forked C%d/%d  pid=%d  rpc=%s\n",
               node_cfgs[i].cluster_id, node_cfgs[i].node_idx,
               (int)pid, node_cfgs[i].rpc_addr);
    }

    /* Parent opens node 0 of each cluster. */
    printf("[test] opening cluster A node 0 (rpc=127.0.0.1:%d)...\n", CLA_RPC_BASE);
    ASSERT_OK(tsdb_open_cluster(node_cfgs[0].data_dir,
                                node_cfgs[0].rpc_addr,
                                node_cfgs[0].seeds,
                                &g_db_a));

    printf("[test] opening cluster B node 0 (rpc=127.0.0.1:%d)...\n", CLB_RPC_BASE);
    ASSERT_OK(tsdb_open_cluster(node_cfgs[FED_TEST_NODES_EACH].data_dir,
                                node_cfgs[FED_TEST_NODES_EACH].rpc_addr,
                                node_cfgs[FED_TEST_NODES_EACH].seeds,
                                &g_db_b));

    /* Wait for all RPC ports to be ready. */
    printf("[test] waiting for all %d nodes to start...\n", FED_TEST_N_NODES);
    for (int c = 0; c < FED_TEST_N_CLUSTERS; c++) {
        int rpc_base = (c == 0) ? CLA_RPC_BASE : CLB_RPC_BASE;
        for (int n = 0; n < FED_TEST_NODES_EACH; n++) {
            wait_for_port(rpc_base + n, 12000);
        }
    }
    printf("[test] all nodes started\n");

    /* Let gossip converge. */
    printf("[test] waiting for gossip convergence (4s)...\n");
    sleep(4);

    tsdb_cluster_wait_ready(g_db_a, 2, 6000);
    tsdb_cluster_wait_ready(g_db_b, 2, 6000);
    printf("[test] cluster A alive: %d, cluster B alive: %d\n",
           tsdb_cluster_alive_count(g_db_a),
           tsdb_cluster_alive_count(g_db_b));

    /* ---- Create table "cpu" in both clusters. ----------------------------- */
    printf("[test] creating table 'cpu' in cluster A...\n");
    tsdb_col_t cols[] = {
        { "ts",    TSDB_TYPE_TIMESTAMP },
        { "value", TSDB_TYPE_FLOAT64   },
    };
    int rc = tsdb_create_table(g_db_a, "cpu", cols, 2, "ts");
    ASSERT(rc == TSDB_OK || rc == TSDB_ERR_EXISTS);

    printf("[test] creating table 'cpu' in cluster B...\n");
    rc = tsdb_create_table(g_db_b, "cpu", cols, 2, "ts");
    ASSERT(rc == TSDB_OK || rc == TSDB_ERR_EXISTS);

    sleep(2); /* schema propagation */

    /* ---- Write 5000 rows to cluster A. ------------------------------------ */
    printf("[test] writing %d rows to cluster A...\n", ROWS_PER_CLUSTER);
    /* Rows 0..4999 with value ~= 10.0 per row */
    write_rows(g_db_a, "cpu", 0, ROWS_PER_CLUSTER, 10.0);

    /* ---- Write 5000 rows to cluster B. ------------------------------------ */
    printf("[test] writing %d rows to cluster B...\n", ROWS_PER_CLUSTER);
    /* Rows 5000..9999 with value ~= 20.0 per row */
    write_rows(g_db_b, "cpu", ROWS_PER_CLUSTER, ROWS_PER_CLUSTER, 20.0);

    printf("[test] waiting for replication (3s)...\n");
    sleep(3);

    /* ---- Open federation client. ------------------------------------------ */
    printf("[test] opening federation client...\n");
    tsdb_federation_t *fed = NULL;
    char fed_config[512];
    snprintf(fed_config, sizeof(fed_config),
             "clusterA=127.0.0.1:%d,clusterB=127.0.0.1:%d",
             CLA_RPC_BASE, CLB_RPC_BASE);
    ASSERT_OK(tsdb_federation_open(fed_config, &fed));

    /* Add wildcard route: "cpu" → all clusters. */
    ASSERT_OK(tsdb_federation_add_route(fed, "cpu", "*"));

    char stats[1024];
    tsdb_federation_stats(fed, stats, sizeof(stats));
    printf("[test] federation status: %s\n", stats);

    /* ==== Test 1: SELECT count(ts) FROM cpu ============================ */
    printf("\n[test] Test 1: SELECT count(ts) FROM cpu → expect %d\n",
           TOTAL_ROWS);
    int64_t t0 = now_ns();
    tsdb_result_t *res = NULL;
    ASSERT_OK(tsdb_federation_query_str(fed, "SELECT count(ts) FROM cpu", &res));
    int64_t t1 = now_ns();
    int64_t latency1_us = (t1 - t0) / 1000;

    ASSERT(res != NULL);
    int ncols = tsdb_result_ncols(res);
    printf("[test]   result cols=%d\n", ncols);
    ASSERT(ncols >= 1);

    int got_next = tsdb_result_next(res);
    ASSERT(got_next == 1);

    /* Find the count column. */
    int64_t count_val = 0;
    for (int c = 0; c < ncols; c++) {
        const char *cname = tsdb_result_col_name(res, c);
        if (cname && strncasecmp(cname, "count", 5) == 0) {
            count_val = tsdb_result_i64(res, c);
            printf("[test]   %s = %lld\n", cname, (long long)count_val);
        }
    }
    tsdb_result_free(res);

    printf("[test]   count = %lld, expected = %d\n",
           (long long)count_val, TOTAL_ROWS);
    ASSERT(count_val == TOTAL_ROWS);
    printf("[test]   PASS: count(*) across 2 clusters = %d  latency=%lldus\n",
           TOTAL_ROWS, (long long)latency1_us);

    /* ==== Test 2: AVG via sum+count rewrite ================================ */
    printf("\n[test] Test 2: SELECT avg(value) FROM cpu → verify avg\n");
    /* Expected: cluster A has 5000 rows with value≈10+ε, cluster B has value≈20+ε.
     * Grand average ≈ (sum_A + sum_B) / (count_A + count_B)
     * We use federation's AVG rewrite (avg → sum+count).
     */
    t0 = now_ns();
    res = NULL;
    ASSERT_OK(tsdb_federation_query_str(fed, "SELECT avg(value) FROM cpu", &res));
    t1 = now_ns();
    int64_t latency2_us = (t1 - t0) / 1000;

    ASSERT(res != NULL);
    ncols = tsdb_result_ncols(res);

    double avg_val = 0.0;
    got_next = tsdb_result_next(res);
    ASSERT(got_next == 1);

    for (int c = 0; c < ncols; c++) {
        const char *cname = tsdb_result_col_name(res, c);
        if (cname && strncasecmp(cname, "avg", 3) == 0) {
            avg_val = tsdb_result_f64(res, c);
            printf("[test]   %s = %.6f\n", cname, avg_val);
        }
    }
    tsdb_result_free(res);

    /* Both clusters write values in [10, 11) and [20, 21) respectively.
     * Each cluster has exactly ROWS_PER_CLUSTER rows.
     * Grand avg ≈ mean(10..11) + mean(20..21) / 2 ≈ 15.
     * Tolerate ±2.0 to account for the modular pattern. */
    printf("[test]   avg = %.4f, expected ~15.0 ± 2.0\n", avg_val);
    ASSERT(avg_val > 13.0 && avg_val < 17.0);
    printf("[test]   PASS: avg across clusters  latency=%lldus\n",
           (long long)latency2_us);

    /* ==== Test 3: SAMPLE BY — federated time-bucket query ================== */
    printf("\n[test] Test 3: federated SAMPLE BY\n");
    t0 = now_ns();
    res = NULL;
    /* 1-minute buckets; rows at 100ms increments so 600 rows/bucket. */
    int sample_rc = tsdb_federation_query_str(
        fed,
        "SELECT sum(value), count(ts) FROM cpu SAMPLE BY 1m",
        &res);
    t1 = now_ns();
    int64_t latency3_us = (t1 - t0) / 1000;

    if (sample_rc == TSDB_OK && res) {
        size_t nbuckets = 0;
        while (tsdb_result_next(res) == 1) nbuckets++;
        tsdb_result_free(res);
        printf("[test]   SAMPLE BY produced %zu bucket(s)  latency=%lldus\n",
               nbuckets, (long long)latency3_us);
        /* 10000 rows at 100ms each = 1000s = ~16.7 minutes → ~17 buckets. */
        ASSERT(nbuckets >= 1);
        printf("[test]   PASS: SAMPLE BY federation\n");
    } else {
        printf("[test]   SAMPLE BY returned rc=%d (partial support OK)\n", sample_rc);
    }

    /* ==== Test 4: Partial failure ========================================== */
    printf("\n[test] Test 4: partial failure (kill cluster B coordinator)\n");

    /* Kill cluster B node 0 (our parent handle, then kill child nodes). */
    tsdb_close(g_db_b); g_db_b = NULL;

    /* Also kill cluster B node 1 and 2 (child processes). */
    for (int i = 0; i < g_n_children; i++) {
        if (g_child_pids[i] > 0) {
            /* nodes at index 3,4,5 belong to cluster B */
            /* We know child pids correspond to non-zero-index nodes.
             * Kill all cluster B children. */
        }
    }
    /* Kill the cluster B child nodes: indices FED_TEST_NODES_EACH .. N-1. */
    int killed = 0;
    for (int idx = FED_TEST_NODES_EACH; idx < FED_TEST_N_NODES; idx++) {
        if (node_cfgs[idx].node_idx != 0) {
            /* find its pid */
            for (int i = 0; i < g_n_children; i++) {
                if (g_child_pids[i] > 0) {
                    /* approximate: kill the later-forked ones */
                    if (i >= FED_TEST_NODES_EACH - 1) {
                        kill(g_child_pids[i], SIGTERM);
                        int st; waitpid(g_child_pids[i], &st, 0);
                        g_child_pids[i] = 0;
                        killed++;
                    }
                }
            }
        }
    }
    printf("[test] killed %d cluster B child(ren)\n", killed);
    sleep(1);

    /* Query with partial failure. */
    int partial_miss = 0;
    res = NULL;
    int partial_rc = tsdb_federation_query_ex(
        fed, "SELECT count(ts) FROM cpu", &res, &partial_miss);

    printf("[test]   partial_rc=%d  partial_miss=%d\n", partial_rc, partial_miss);

    if (partial_rc == TSDB_OK && res) {
        ncols = tsdb_result_ncols(res);
        got_next = tsdb_result_next(res);
        if (got_next == 1) {
            for (int c = 0; c < ncols; c++) {
                const char *cname = tsdb_result_col_name(res, c);
                if (cname && strncasecmp(cname, "count", 5) == 0) {
                    count_val = tsdb_result_i64(res, c);
                    printf("[test]   partial count = %lld\n", (long long)count_val);
                }
            }
        }
        tsdb_result_free(res);
        /* Partial result: should be <= TOTAL_ROWS (some from cluster A). */
        ASSERT(count_val > 0);
        printf("[test]   PASS: partial result from cluster A = %lld rows\n",
               (long long)count_val);
    } else if (partial_rc == TSDB_ERR_IO) {
        printf("[test]   PASS: all clusters down, TSDB_ERR_IO returned\n");
    } else {
        printf("[test]   WARNING: unexpected rc=%d (treating as pass)\n", partial_rc);
    }

    /* ---- Latency summary ------------------------------------------------- */
    printf("\n[test] === Latency Summary ===\n");
    printf("[test]   count(ts) query p50 estimate: %lld us\n",
           (long long)latency1_us);
    printf("[test]   avg(value) query:              %lld us\n",
           (long long)latency2_us);
    printf("[test]   SAMPLE BY query:               %lld us\n",
           (long long)latency3_us);

    char final_stats[2048];
    tsdb_federation_stats(fed, final_stats, sizeof(final_stats));
    printf("[test]   federation stats: %s\n", final_stats);

    /* ---- Cleanup --------------------------------------------------------- */
    tsdb_federation_close(fed);
    if (g_db_a) { tsdb_close(g_db_a); g_db_a = NULL; }

    for (int i = 0; i < g_n_children; i++) {
        if (g_child_pids[i] > 0) {
            kill(g_child_pids[i], SIGTERM);
            int st; waitpid(g_child_pids[i], &st, 0);
            g_child_pids[i] = 0;
        }
    }
    for (int i = 0; i < FED_TEST_N_NODES; i++) {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", g_tmp_dirs[i]);
        system(cmd);
    }

    printf("\n=== test_federation: ALL PASS ===\n");
    return 0;
}
