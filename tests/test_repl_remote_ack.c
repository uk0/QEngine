/* test_repl_remote_ack.c — a configured write quorum must be met by REMOTE
 * ACKs, never by the local write alone (REPL-1), and the tsd.conf cluster
 * keys must reach their engine readers (OBS-2).
 *
 * THE BUG (REPL-1).  tsdb_cluster_write read TSDB_REPLICATION_QUORUM
 * (default 1, documented "wait for >=1 remote ACK"), clamped it to the
 * remote-peer count, and passed it straight to tsdb_replica_write.  The
 * fanout seeds ack_count = 1 for the already-written LOCAL copy, so a
 * quorum of 1 was satisfied before any worker even dialed a peer: the wait
 * loop (`ack_count < quorum`) was vacuously false and the write returned
 * TSDB_OK with the rows on exactly one node.  On a 2-node cluster the
 * clamp made this the ONLY reachable configuration — lose the ingesting
 * node before anti-entropy runs and the acked write is gone.
 *
 * THE INVARIANT.  TSDB_OK from tsdb_cluster_write under a configured
 * quorum q > 0 means min(q, nremote) REMOTE peers durably ACKed the batch.
 * The local +1 seed exists only so "wait for all" broadcasts can count it;
 * the configured quorum must be translated to q+1 on the wire.
 *
 * THE BUG (OBS-2).  tsd.conf parses cluster_write_quorum and
 * cluster_replica_factor into tsdb_config_t and then NOTHING in the engine
 * reads them — the only live knobs were the env vars.  tsdb_open_cluster
 * now bridges explicitly-set file keys onto the env readers (overwrite=0,
 * so a real env var stays authoritative), and — critically — does NOT
 * bridge the parser DEFAULTS: replica_factor defaults to 3, and exporting
 * that would silently switch every cluster with a config file into shard
 * mode.
 *
 * Single process, one real cluster node on 127.0.0.1, plus a stub peer
 * that ACCEPTS connections and never replies — the shape of a peer that is
 * up but cannot durably apply.  No forks.
 *
 *   [1] OBS-2: an explicit cluster_write_quorum=2 in tsd.conf reaches the
 *       engine as TSDB_REPLICATION_QUORUM=1 (remote-ack convention), and
 *       the ABSENT cluster_replica_factor key is NOT bridged from its
 *       default (TSDB_SHARD_REPLICA_N stays unset).
 *   [2] REPL-1: with one ALIVE peer that never ACKs, tsdb_cluster_write
 *       must NOT return TSDB_OK, must report 0 remote ACKs, and must have
 *       actually waited for the (test-shortened) fanout deadline rather
 *       than returning instantly on the local +1.
 *   [3] With the peer marked DEAD (nremote == 0, the single-node shape),
 *       the write returns TSDB_OK immediately — the gate is about remote
 *       copies that were PROMISED, not about standalone operation.
 */
#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"
#include "../src/cluster/cluster.h"
#include "../src/cluster/node.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int g_fail = 0;

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define TDIR     "/tmp/tsdb_test_repl_remote_ack"
#define CONF     "/tmp/tsdb_test_repl_remote_ack.conf"
#define RPC_ADDR "127.0.0.1:28471"
#define STUB_ID  ((tsdb_node_id_t)42424242ULL)

/* Defined in src/storage/db_cluster.c; not exported through a public header. */
extern struct tsdb_cluster *tsdb_db_cluster(tsdb_db_t *db);

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

static long mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* ---- stub peer: accepts, reads, never replies ---------------------------- */

typedef struct {
    int listen_fd;
    int client_fds[16];
    int n_clients;
    pthread_mutex_t mu;
    int stop;
} stub_t;

static void *stub_accept_loop(void *ud) {
    stub_t *s = (stub_t *)ud;
    for (;;) {
        int cfd = accept(s->listen_fd, NULL, NULL);
        if (cfd < 0) return NULL;
        pthread_mutex_lock(&s->mu);
        if (s->stop) { pthread_mutex_unlock(&s->mu); close(cfd); return NULL; }
        if (s->n_clients < 16) s->client_fds[s->n_clients++] = cfd;
        else close(cfd);
        pthread_mutex_unlock(&s->mu);
        /* Deliberately never read a full frame, never write a byte: the
         * peer is "up" at the TCP level and silent at the RPC level. */
    }
}

static int stub_start(stub_t *s, pthread_t *th) {
    memset(s, 0, sizeof(*s));
    pthread_mutex_init(&s->mu, NULL);
    s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (s->listen_fd < 0) return -1;
    int one = 1;
    setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (bind(s->listen_fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) return -1;
    if (listen(s->listen_fd, 8) != 0) return -1;
    socklen_t sl = sizeof(sa);
    if (getsockname(s->listen_fd, (struct sockaddr *)&sa, &sl) != 0) return -1;
    if (pthread_create(th, NULL, stub_accept_loop, s) != 0) return -1;
    return ntohs(sa.sin_port);
}

static void stub_shutdown(stub_t *s) {
    pthread_mutex_lock(&s->mu);
    s->stop = 1;
    for (int i = 0; i < s->n_clients; i++) close(s->client_fds[i]);
    s->n_clients = 0;
    pthread_mutex_unlock(&s->mu);
    /* Closing the listen fd unblocks accept(). */
    close(s->listen_fd);
}

int main(void) {
    printf("=== test_repl_remote_ack ===\n");
    rm_rf(TDIR);

    /* Keep the failing-quorum wait short so the test is quick; the
     * assertion below still proves a real wait happened. */
    setenv("TSDB_FANOUT_WAIT_TIMEOUT_MS", "1500", 1);
    unsetenv("TSDB_REPLICATION_QUORUM");
    unsetenv("TSDB_SHARD_REPLICA_N");

    /* tsd.conf with an EXPLICIT write quorum and NO replica factor. */
    {
        FILE *f = fopen(CONF, "w");
        if (!f) { fprintf(stderr, "FATAL: cannot write %s\n", CONF); return 1; }
        fprintf(f, "cluster_write_quorum = 2\n");
        fclose(f);
        setenv("TSDB_CONFIG", CONF, 1);
    }

    tsdb_db_t *db = NULL;
    int orc = tsdb_open_cluster(TDIR, RPC_ADDR, "", &db);
    if (orc != TSDB_OK || !db) {
        fprintf(stderr, "FATAL: tsdb_open_cluster rc=%d\n", orc);
        return 1;
    }
    tsdb_cluster_t *c = tsdb_db_cluster(db);
    if (!c) { fprintf(stderr, "FATAL: no cluster handle\n"); return 1; }

    printf("\n[1] OBS-2: explicit tsd.conf keys reach the engine env readers\n");
    {
        const char *q = getenv("TSDB_REPLICATION_QUORUM");
        const char *n = getenv("TSDB_SHARD_REPLICA_N");
        printf("  TSDB_REPLICATION_QUORUM=%s  TSDB_SHARD_REPLICA_N=%s\n",
               q ? q : "(unset)", n ? n : "(unset)");
        CHECK(q != NULL && strcmp(q, "1") == 0,
              "cluster_write_quorum=2 (total) bridged as 1 remote ACK");
        CHECK(n == NULL,
              "absent cluster_replica_factor NOT bridged from its default");
    }

    /* Stub peer, registered ALIVE in the local membership view. */
    stub_t stub;
    pthread_t stub_th;
    int stub_port = stub_start(&stub, &stub_th);
    if (stub_port < 0) { fprintf(stderr, "FATAL: stub\n"); return 1; }
    {
        tsdb_node_info_t info;
        memset(&info, 0, sizeof(info));
        info.id = STUB_ID;
        snprintf(info.addr, sizeof(info.addr), "127.0.0.1:%d", stub_port);
        snprintf(info.gossip_addr, sizeof(info.gossip_addr), "127.0.0.1:1");
        info.state   = TSDB_NODE_ALIVE;
        info.version = 1;
        tsdb_node_manager_upsert(tsdb_cluster_node_mgr(c), &info);
    }

    int64_t ts_col[4] = { 1000, 2000, 3000, 4000 };
    int col_types[1]  = { TSDB_TYPE_TIMESTAMP };
    const void *col_data[1] = { ts_col };

    printf("\n[2] REPL-1: quorum=1 must wait for a REMOTE ACK, not the local +1\n");
    {
        int remote_acks = -1;
        long t0 = mono_ms();
        int rc = tsdb_cluster_write(c, "qt", 1, col_types, 4, col_data,
                                    &remote_acks, 0, NULL);
        long dt = mono_ms() - t0;
        printf("  rc=%d remote_acks=%d elapsed=%ldms\n", rc, remote_acks, dt);
        CHECK(rc != TSDB_OK,
              "no remote ACK -> commit MUST NOT return TSDB_OK (got rc=%d)", rc);
        CHECK(remote_acks == 0, "remote ack count is 0 (got %d)", remote_acks);
        CHECK(dt >= 700,
              "the write waited for the fanout deadline (%ldms >= 700ms), "
              "not returned instantly on the local write", dt);
    }

    printf("\n[3] a DEAD peer leaves the single-node fast path intact\n");
    {
        tsdb_node_manager_dead(tsdb_cluster_node_mgr(c), STUB_ID);
        int remote_acks = -1;
        long t0 = mono_ms();
        int rc = tsdb_cluster_write(c, "qt", 1, col_types, 4, col_data,
                                    &remote_acks, 0, NULL);
        long dt = mono_ms() - t0;
        printf("  rc=%d remote_acks=%d elapsed=%ldms\n", rc, remote_acks, dt);
        CHECK(rc == TSDB_OK, "no remote peers -> local write commits (rc=%d)", rc);
        CHECK(dt < 700, "and does so without a quorum wait (%ldms)", dt);
    }

    stub_shutdown(&stub);
    pthread_join(stub_th, NULL);
    tsdb_close_cluster(db);
    remove(CONF);
    rm_rf(TDIR);

    printf("\n%s\n", g_fail ? "FAILED" : "OK");
    return g_fail ? 1 : 0;
}
