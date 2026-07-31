/* test_cluster_show.c — SHOW's cluster scope, on four live nodes.
 *
 * This is the half of SHOW a single-process test cannot reach: what the
 * statement does when the replicas DISAGREE, and what it does when it cannot
 * ask one of them.
 *
 * Topology mirrors the 4-node fixture cluster: DATABASE bench > GROUP
 * g_sensors/g_meters > STABLE env/power > env_0..3, pw_0..1, plus the plain
 * table `dup`.  Every node is seeded with that catalog BEFORE it joins, and
 * node2 is given one extra child table, `env_solo`, that no other node has
 * heard of — the divergence src/storage/catalog_sync.c documents, where one
 * peer's catalog silently holds less (or more) than its neighbours'.
 *
 *   node0 (parent) rpc 28181   node1 rpc 28182
 *   node2 rpc 28183 (+env_solo) node3 rpc 28184
 *
 *  1. LIST TABLES on node0 sees 6 of the cluster's 7 tables and says nothing
 *     about the seventh.  That is the whole reason SHOW exists.
 *  2. SHOW TABLES run ON ALL FOUR NODES returns the same 7 rows, and marks
 *     env_solo "1/4" while the shared tables are "4/4".  Nodes agree, and
 *     the operator can see which row only one replica holds.
 *  3. SHOW STABLES / DATABASES / GROUPS agree across all four the same way.
 *  4. A SHOW that arrives as somebody else's scatter leg answers locally
 *     instead of fanning out again.
 *  5. Kill node3.  SHOW must FAIL on every surviving node rather than
 *     quietly return the 6 rows they still hold as the cluster's catalog —
 *     once while gossip still calls the peer ALIVE (the fan-out leg fails)
 *     and again after it is demoted (the fan-out skips it, and only the
 *     membership census is left).  LIST keeps working, node-locally.
 *
 * Run: make test-cluster (excluded from the default suite — it binds ports
 * and forks, like the other tests/test_cluster*.c).
 */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE
#define _DEFAULT_SOURCE

#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"
#include "../src/cluster/rpc.h"
#include "../src/federation/fedrpc.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define NNODES 4
#define RPC0   28181

static pid_t g_child[NNODES];          /* [0] unused: node0 is the parent */
static char  g_dir[NNODES][64];

static void cleanup(void) {
    for (int i = 1; i < NNODES; i++) {
        if (g_child[i] > 0) { kill(g_child[i], SIGKILL); waitpid(g_child[i], NULL, 0); g_child[i] = 0; }
    }
    for (int i = 0; i < NNODES; i++) {
        if (!g_dir[i][0]) continue;
        char cmd[160];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", g_dir[i]);
        (void)!system(cmd);
    }
}

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    cleanup(); exit(1); \
} while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

static void run_ddl(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("DDL rc=%d (%s): %s", rc, tsdb_errstr(rc), sql);
    if (r) tsdb_result_free(r);
}

/* Seed a data dir offline, so nothing is broadcast and each node's catalog is
 * exactly what we put in it. */
static void seed(const char *dir, int with_solo) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    run_ddl(db, "CREATE DATABASE bench");
    run_ddl(db, "CREATE GROUP g_sensors IN DATABASE bench");
    run_ddl(db, "CREATE GROUP g_meters IN DATABASE bench");
    run_ddl(db, "CREATE STABLE env (ts TIMESTAMP, t FLOAT64) TAGS (loc SYMBOL) "
                "IN DATABASE bench GROUP g_sensors");
    run_ddl(db, "CREATE STABLE power (ts TIMESTAMP, w FLOAT64) TAGS (loc SYMBOL) "
                "IN DATABASE bench GROUP g_meters");
    char sql[192];
    for (int i = 0; i < 4; i++) {
        snprintf(sql, sizeof(sql), "CREATE TABLE env_%d USING env TAGS ('s%d')", i, i);
        run_ddl(db, sql);
    }
    for (int i = 0; i < 2; i++) {
        snprintf(sql, sizeof(sql), "CREATE TABLE pw_%d USING power TAGS ('m%d')", i, i);
        run_ddl(db, sql);
    }
    run_ddl(db, "CREATE TABLE dup (ts TIMESTAMP, v INT64) TIMESTAMP(ts)");
    if (with_solo)
        run_ddl(db, "CREATE TABLE env_solo USING env TAGS ('solo')");
    tsdb_close(db);
}

static int tcp_ready(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    int fl = fcntl(fd, F_GETFL); fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    struct pollfd p = { fd, POLLOUT, 0 };
    int ok = 0;
    if (poll(&p, 1, 200) > 0 && (p.revents & POLLOUT)) {
        int e = 0; socklen_t el = sizeof(e);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &e, &el);
        ok = (e == 0);
    }
    close(fd);
    return ok;
}

/* ---- Rendering a listing to a comparable string -------------------------- */

/* "<name> <on_nodes>\n" per row, sorted.  Returns row count, or -1 when the
 * statement answered with an "ERR: ..." status row instead of a listing. */
static int render(tsdb_result_t *r, char *out, size_t cap, char *errtext, size_t errcap) {
    if (out && cap) out[0] = '\0';
    if (errtext && errcap) errtext[0] = '\0';
    if (!r) { if (errtext) snprintf(errtext, errcap, "no result"); return -1; }

    int ncols = tsdb_result_ncols(r);
    const char *c0 = tsdb_result_col_name(r, 0);
    if (ncols == 1 && c0 && !strcmp(c0, "status")) {
        while (tsdb_result_next(r) == 1) {
            const char *s = tsdb_result_sym(r, 0);
            if (s && !strncmp(s, "ERR:", 4) && errtext) snprintf(errtext, errcap, "%s", s);
        }
        return -1;
    }
    char rows[64][160];
    int n = 0;
    while (tsdb_result_next(r) == 1 && n < 64) {
        const char *nm = tsdb_result_sym(r, 0);
        const char *hd = tsdb_result_sym(r, ncols - 1);
        snprintf(rows[n++], sizeof(rows[0]), "%s %s", nm ? nm : "(null)", hd ? hd : "(null)");
    }
    for (int i = 1; i < n; i++) {
        char tmp[160]; snprintf(tmp, sizeof(tmp), "%s", rows[i]);
        int j = i - 1;
        while (j >= 0 && strcmp(rows[j], tmp) > 0) { snprintf(rows[j+1], sizeof(rows[0]), "%s", rows[j]); j--; }
        snprintf(rows[j+1], sizeof(rows[0]), "%s", tmp);
    }
    size_t used = 0;
    for (int i = 0; i < n; i++) used += (size_t)snprintf(out + used, cap - used, "%s\n", rows[i]);
    return n;
}

static int show_local(tsdb_db_t *db, const char *sql, char *out, size_t cap,
                      char *err, size_t errcap) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) {
        if (r) tsdb_result_free(r);
        if (err) snprintf(err, errcap, "rc=%d (%s)", rc, tsdb_errstr(rc));
        return -1;
    }
    int n = render(r, out, cap, err, errcap);
    tsdb_result_free(r);
    return n;
}

/* Run `sql` ON node `idx` through plain FED_QUERY.  FED_QUERY (unlike
 * FED_QUERY_LOCAL) does NOT arm scatter-local mode, so the remote node
 * executes SHOW with its full cluster scope — this is how the statement is
 * exercised on each of the other three nodes. */
static int show_on(int idx, const char *sql, char *out, size_t cap,
                   char *err, size_t errcap) {
    char addr[32];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", RPC0 + idx);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 3000);
    if (!conn) { if (err) snprintf(err, errcap, "dial %s failed", addr); return -2; }
    tsdb_result_t *r = NULL;
    int rc = fedrpc_query(conn, sql, 10000, &r);
    if (rc != TSDB_OK) {
        tsdb_rpc_conn_close(conn);
        if (err) snprintf(err, errcap, "fed rc=%d (%s)", rc, tsdb_errstr(rc));
        /* The peer replies RPC_ERR for a failed query; a SHOW that refuses
         * does so with an OK status row, so this really is a transport or
         * execution failure. */
        return -2;
    }
    int n = render(r, out, cap, err, errcap);
    tsdb_result_free(r);
    tsdb_rpc_conn_close(conn);
    return n;
}

/* Every node must return the same listing for `sql`. */
static void all_agree(tsdb_db_t *db0, const char *sql, int live_nodes,
                      char *out, size_t cap) {
    char err[512];
    int n0 = show_local(db0, sql, out, cap, err, sizeof(err));
    if (n0 < 0) FAIL("`%s` on node0: %s", sql, err);
    for (int i = 1; i < live_nodes; i++) {
        char other[4096];
        int ni = show_on(i, sql, other, sizeof(other), err, sizeof(err));
        if (ni < 0) FAIL("`%s` on node%d: %s", sql, i, err);
        if (strcmp(out, other) != 0)
            FAIL("`%s` disagrees between node0 and node%d:\n--- node0 ---\n%s"
                 "--- node%d ---\n%s", sql, i, out, i, other);
    }
    printf("     all %d nodes agree on `%s` (%d rows)\n", live_nodes, sql, n0);
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    for (int i = 0; i < NNODES; i++) snprintf(g_dir[i], sizeof(g_dir[i]), "/tmp/tsdb_show_c%d", i);
    cleanup();

    for (int i = 0; i < NNODES; i++) seed(g_dir[i], i == 2);

    char seeds[256] = "";
    for (int i = 0; i < NNODES; i++) {
        char one[32];
        snprintf(one, sizeof(one), "%s127.0.0.1:%d", i ? "," : "", RPC0 - 1 + i);
        strncat(seeds, one, sizeof(seeds) - strlen(seeds) - 1);
    }

    for (int i = 1; i < NNODES; i++) {
        g_child[i] = fork();
        if (g_child[i] == 0) {
            char addr[32];
            snprintf(addr, sizeof(addr), "127.0.0.1:%d", RPC0 + i);
            tsdb_db_t *db = NULL;
            if (tsdb_open_cluster(g_dir[i], addr, seeds, &db) != TSDB_OK) _exit(1);
            for (;;) pause();
        }
        ASSERT(g_child[i] > 0);
    }

    char addr0[32];
    snprintf(addr0, sizeof(addr0), "127.0.0.1:%d", RPC0);
    tsdb_db_t *db0 = NULL;
    OK(tsdb_open_cluster(g_dir[0], addr0, seeds, &db0));

    for (int i = 1; i < NNODES; i++) {
        for (int k = 0; k < 150 && !tcp_ready(RPC0 + i); k++) usleep(100000);
        if (!tcp_ready(RPC0 + i)) FAIL("node%d never bound rpc %d", i, RPC0 + i);
    }
    tsdb_cluster_wait_ready(db0, NNODES, 20000);
    for (int k = 0; k < 300 && tsdb_cluster_alive_count(db0) < NNODES; k++) usleep(100000);
    if (tsdb_cluster_alive_count(db0) != NNODES)
        FAIL("gossip never converged: alive=%d want %d",
             tsdb_cluster_alive_count(db0), NNODES);
    printf("[test] %d nodes alive\n", NNODES);

    char buf[4096], err[512];

    /* ── 1: LIST is node-local, and here it is short ────────────────────── */
    int n = show_local(db0, "LIST TABLES", buf, sizeof(buf), err, sizeof(err));
    if (n != 6) FAIL("LIST TABLES on node0 -> %d rows, want 6 (%s)", n, err);
    if (strstr(buf, "env_solo")) FAIL("LIST TABLES saw node2's private table");
    printf("[1] PASS: LIST TABLES = 6 rows, node-local, no env_solo\n");

    /* ── 2: SHOW on all four nodes, same answer, divergence labelled ────── */
    all_agree(db0, "SHOW TABLES", NNODES, buf, sizeof(buf));
    if (!strstr(buf, "env_solo 1/4"))
        FAIL("SHOW TABLES did not report env_solo as held by 1 of 4:\n%s", buf);
    if (!strstr(buf, "env_0 4/4") || !strstr(buf, "pw_1 4/4"))
        FAIL("SHOW TABLES did not report the shared tables as 4/4:\n%s", buf);
    {
        int rows = 0;
        for (const char *p = buf; *p; p++) if (*p == '\n') rows++;
        if (rows != 7) FAIL("SHOW TABLES -> %d rows, want 7:\n%s", rows, buf);
    }
    printf("[2] PASS: SHOW TABLES = 7 rows on every node; env_solo 1/4, shared 4/4\n%s", buf);

    /* ── 3: the other three layers agree too ────────────────────────────── */
    all_agree(db0, "SHOW STABLES",   NNODES, buf, sizeof(buf));
    if (!strstr(buf, "env 4/4") || !strstr(buf, "power 4/4") || !strstr(buf, "dup 4/4"))
        FAIL("SHOW STABLES missing an expected 4/4 row:\n%s", buf);
    printf("%s", buf);
    all_agree(db0, "SHOW DATABASES", NNODES, buf, sizeof(buf));
    if (!strstr(buf, "bench 4/4")) FAIL("SHOW DATABASES missing bench 4/4:\n%s", buf);
    printf("%s", buf);
    all_agree(db0, "SHOW GROUPS",    NNODES, buf, sizeof(buf));
    if (!strstr(buf, "g_sensors 4/4") || !strstr(buf, "g_meters 4/4"))
        FAIL("SHOW GROUPS missing an expected 4/4 row:\n%s", buf);
    printf("%s", buf);
    all_agree(db0, "SHOW TABLES USING env", NNODES, buf, sizeof(buf));
    if (!strstr(buf, "env_solo 1/4")) FAIL("filtered SHOW lost env_solo:\n%s", buf);
    printf("[3] PASS: STABLES / DATABASES / GROUPS / filtered TABLES agree on 4 nodes\n");

    /* ── 4: a SHOW that arrives as a scatter leg stays local ────────────── */
    {
        char a2[32];
        snprintf(a2, sizeof(a2), "127.0.0.1:%d", RPC0 + 2);
        tsdb_rpc_conn_t *conn = tsdb_rpc_connect(a2, 3000);
        ASSERT(conn != NULL);
        tsdb_result_t *r = NULL;
        OK(fedrpc_query_local(conn, "SHOW TABLES", 5000, &r));
        ASSERT(r);
        if (tsdb_result_ncols(r) != 6)
            FAIL("SHOW inside a scatter leg returned %d columns, want 6 "
                 "(it re-scattered instead of answering locally)",
                 tsdb_result_ncols(r));
        int rows = 0;
        while (tsdb_result_next(r) == 1) { ASSERT(tsdb_result_sym(r, 0) != NULL); rows++; }
        if (rows != 7) FAIL("node2 scatter leg -> %d rows, want its own 7", rows);
        tsdb_result_free(r);
        tsdb_rpc_conn_close(conn);
        printf("[4] PASS: SHOW served as a scatter leg answers node-locally\n");
    }

    /* ── 5: a node we cannot ask is an error, not a shorter list ────────── */
    kill(g_child[3], SIGKILL);
    waitpid(g_child[3], NULL, 0);
    g_child[3] = 0;

    /* 5a — node3 is still ALIVE in gossip, so the fan-out dials it and that
     * leg fails.  Only the "every alive peer answered" check stands here. */
    int refused = 0;
    for (int i = 0; i < 300; i++) {
        n = show_local(db0, "SHOW TABLES", buf, sizeof(buf), err, sizeof(err));
        if (n == -1) { refused = 1; break; }
        if (n != 7)
            FAIL("SHOW TABLES returned %d rows with a node down — a partial "
                 "listing presented as the cluster's catalog", n);
        usleep(100000);
    }
    if (!refused)
        FAIL("SHOW TABLES never refused after node3 died — it kept reporting a "
             "cluster-wide answer it could no longer certify");
    printf("[5a] PASS: node0 refused while the peer was still ALIVE:\n     %s\n", err);

    /* 5b — once gossip demotes node3 the fan-out SKIPS it, so every peer it
     * dialled answered and the check above is satisfied.  A node that is
     * known but unreachable is exactly when a listing silently shrinks, so
     * this is the case the membership census exists for. */
    for (int i = 0; i < 900 && tsdb_cluster_alive_count(db0) > NNODES - 1; i++) usleep(100000);
    if (tsdb_cluster_alive_count(db0) > NNODES - 1)
        FAIL("gossip never demoted the dead node in 90s — cannot exercise the "
             "membership census");
    n = show_local(db0, "SHOW TABLES", buf, sizeof(buf), err, sizeof(err));
    if (n != -1)
        FAIL("with node3 known-but-not-alive, SHOW TABLES returned %d rows "
             "instead of refusing — a node's catalog was dropped silently", n);
    printf("[5b] PASS: node0 refused with the peer demoted:\n     %s\n", err);

    /* 5c — the surviving peers refuse too, not just the node we asked from. */
    for (int i = 1; i <= 2; i++) {
        n = show_on(i, "SHOW TABLES", buf, sizeof(buf), err, sizeof(err));
        if (n >= 0)
            FAIL("node%d returned %d rows for SHOW TABLES with node3 down", i, n);
    }
    printf("[5c] PASS: surviving peers refuse as well\n");

    /* 5d — LIST keeps its node-local meaning and keeps working. */
    n = show_local(db0, "LIST TABLES", buf, sizeof(buf), err, sizeof(err));
    if (n != 6) FAIL("LIST TABLES after the kill -> %d rows, want 6", n);
    printf("[5d] PASS: LIST TABLES still answers node-locally (%d rows)\n", n);

    tsdb_close(db0);
    cleanup();
    printf("\n=== ALL CLUSTER SHOW TESTS PASSED ===\n");
    return 0;
}
