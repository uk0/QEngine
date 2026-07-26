/* test_ae_peer_stats.c — anti-entropy must measure a PEER truthfully.
 *
 * THE BUG.  192e7e5 fixed the half of the comparison where a node measured
 * ITSELF with a cluster-scoped query (tsdb_cluster_local_table_stats, pinned by
 * tests/test_ae_local_stats.c).  The peer half was left asking
 * `SELECT count(*), max(ts) FROM <t>` over FED_QUERY.  That is not a
 * measurement of the peer: hierarchy mirroring registers every plain table as
 * a childless data-bearing super-table, so exec routes the query through
 * exec_stable_select and, on a peer holding NO local rows, the cluster-agg
 * coordinator fires and the peer answers with the CLUSTER-WIDE aggregate —
 * byte-for-byte the same (count, max_ts) the real holder reports.  The resync's
 * selection loop cannot tell them apart; when it lands on the empty one, the
 * follow-up `SELECT * FROM <t> WHERE ts > N` (not a scalar agg, so no
 * coordinator) hands back nothing.  The node reports success having converged
 * zero rows, and the startup resync runs exactly once, so it stays empty for
 * the life of the process.
 *
 * THE FIX.  TSDB_RPC_LOCAL_TABLE_STATS: the peer runs
 * tsdb_cluster_local_table_stats on itself and returns its own (count,max_ts).
 * There is no query engine on that path, so there is no scatter to hijack —
 * an empty peer reports (0, INT64_MIN) and never becomes a pull candidate.
 *
 * WHAT THIS PINS (single process; a real RPC server on 127.0.0.1:0 plus a
 * hand-rolled stub peer, no forks, no sleeps, no cluster):
 *
 *   [1] The opcode reports the SERVER's own storage — rows in the memtable,
 *       rows after a flush, and (0, INT64_MIN) for a table it does not have.
 *   [2] Reading the probe through the anti-entropy entry point
 *       (tsdb_cluster_peer_table_stats_conn) gives the same numbers, i.e. the
 *       resync path is actually wired to the new opcode.  A table the peer has
 *       never heard of is the discriminator: the new path answers "0 rows",
 *       the old FED_QUERY path errored (the peer's query fails → ERR) and the
 *       peer was dropped instead of being recorded as empty.
 *   [3] OLD-PEER FALLBACK.  A stub that models a pre-opcode binary — its
 *       dispatch `default:` ACKs with an EMPTY body — must be detected as
 *       TSDB_ERR_UNSUPPORTED, and the anti-entropy probe must then fall back
 *       to FED_QUERY and return the legacy answer rather than failing.
 *   [4] A peer that knows the opcode and replies ERR is NOT silently retried
 *       through the query path; it is reported as "no answer" so the resync
 *       skips it.
 *   [5] The truthful empty-peer reading is exactly the one that keeps a
 *       phantom out of the candidate set (tsdb_antientropy_decide agrees).
 *
 * The peer half of an actual two-node convergence needs live nodes and lives
 * with the cluster tests.
 */
#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/cluster/rpc.h"
#include "../src/storage/db.h"    /* tsdb_db_flush_all */

#include <arpa/inet.h>
#include <dirent.h>
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

#define TDIR "/tmp/tsdb_test_ae_peer_stats"
#define BASE 1700000000000000000LL

/* Sentinels the stub "old peer" answers FED_QUERY with — deliberately not
 * values any real table in this test could produce, so seeing them proves the
 * legacy path ran. */
#define STUB_COUNT  777777LL
#define STUB_MAXTS  4242LL

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

/* ---- stub peer ---------------------------------------------------------- */

/* How the stub answers TSDB_RPC_LOCAL_TABLE_STATS. */
typedef enum {
    STUB_OLD_BINARY = 0,  /* ACK with an EMPTY body — the pre-opcode
                           * dispatch `default:`, byte-for-byte           */
    STUB_ERR        = 1   /* ERR — knows the opcode, cannot answer        */
} stub_mode_t;

typedef struct {
    int         listen_fd;
    stub_mode_t mode;
    int         saw_fed_query;   /* set when the client fell back         */
} stub_t;

/* Serve exactly one connection, framing by hand so the stub owes nothing to
 * the server implementation under test. */
static void *stub_serve(void *ud) {
    stub_t *s = (stub_t *)ud;
    int cfd = accept(s->listen_fd, NULL, NULL);
    if (cfd < 0) return NULL;

    for (;;) {
        uint8_t hdr[TSDB_RPC_HDR_SIZE];
        size_t got = 0;
        while (got < sizeof(hdr)) {
            ssize_t n = read(cfd, hdr + got, sizeof(hdr) - got);
            if (n <= 0) { close(cfd); return NULL; }
            got += (size_t)n;
        }
        uint8_t  rpc_id = hdr[5];
        uint32_t req_id, plen;
        memcpy(&req_id, hdr + 6, 4);
        memcpy(&plen,   hdr + 10, 4);

        uint8_t *pl = plen ? (uint8_t *)malloc(plen) : NULL;
        for (uint32_t off = 0; off < plen; ) {
            ssize_t n = read(cfd, pl + off, plen - off);
            if (n <= 0) { free(pl); close(cfd); return NULL; }
            off += (uint32_t)n;
        }
        free(pl);

        uint8_t body[128];
        uint32_t blen = 0;
        tsdb_rpc_type_t rtype = TSDB_RPC_ACK;

        if (rpc_id == TSDB_RPC_LOCAL_TABLE_STATS) {
            /* STUB_OLD_BINARY: empty ACK, exactly what a binary without this
             * case in its switch sends from `default:`.
             * STUB_ERR: the peer understood and refused. */
            if (s->mode == STUB_ERR) rtype = TSDB_RPC_ERR;
            blen = 0;
        } else if (rpc_id == TSDB_RPC_FED_QUERY) {
            /* Canned fedrpc result: 1 row, cols "count" INT64 and "max(ts)"
             * TIMESTAMP.  Wire layout per src/federation/fedrpc.h. */
            s->saw_fed_query = 1;
            uint8_t *p = body;
            *p++ = 2;                                   /* ncols            */
            *p++ = 5;  memcpy(p, "count",   5); p += 5; /* col 0 name       */
            *p++ = (uint8_t)TSDB_TYPE_INT64;
            *p++ = 7;  memcpy(p, "max(ts)", 7); p += 7; /* col 1 name       */
            *p++ = (uint8_t)TSDB_TYPE_TIMESTAMP;
            uint32_t nrows = 1;
            memcpy(p, &nrows, 4); p += 4;
            int64_t v = STUB_COUNT;  memcpy(p, &v, 8); p += 8;
            v = STUB_MAXTS;          memcpy(p, &v, 8); p += 8;
            blen = (uint32_t)(p - body);
        } else {
            blen = 0;   /* everything else: bare ACK */
        }

        uint8_t frame[TSDB_RPC_HDR_SIZE + sizeof(body)];
        int fn = tsdb_rpc_encode(frame, sizeof(frame), rtype, req_id,
                                 blen ? body : NULL, blen);
        if (fn <= 0) { close(cfd); return NULL; }
        for (int off = 0; off < fn; ) {
            ssize_t n = write(cfd, frame + off, (size_t)(fn - off));
            if (n <= 0) { close(cfd); return NULL; }
            off += (int)n;
        }
    }
}

/* Bind 127.0.0.1:0 and start the stub; returns the chosen port or -1. */
static int stub_start(stub_t *s, pthread_t *th, stub_mode_t mode) {
    memset(s, 0, sizeof(*s));
    s->mode = mode;
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
    if (listen(s->listen_fd, 4) != 0) return -1;
    socklen_t sl = sizeof(sa);
    if (getsockname(s->listen_fd, (struct sockaddr *)&sa, &sl) != 0) return -1;
    if (pthread_create(th, NULL, stub_serve, s) != 0) return -1;
    return ntohs(sa.sin_port);
}

static tsdb_rpc_conn_t *dial(int port) {
    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    return tsdb_rpc_connect(addr, 2000);
}

/* ---- data ---------------------------------------------------------------- */

static void insert_rows(tsdb_table_t *t, int64_t first, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, BASE + first + i));
        OK(tsdb_batch_row_i64(b, 1, first + i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

int main(void) {
    printf("=== test_ae_peer_stats ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    insert_rows(t, 0, 100);

    tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", db, NULL);
    if (!srv) { fprintf(stderr, "FATAL: rpc server\n"); return 1; }
    int port = tsdb_rpc_server_port(srv);
    tsdb_rpc_conn_t *conn = dial(port);
    if (!conn) { fprintf(stderr, "FATAL: dial\n"); return 1; }

    /* ── [1] the opcode reports the SERVER's own storage ─────────────────── */
    printf("\n[1] TSDB_RPC_LOCAL_TABLE_STATS reads the peer's own storage\n");
    {
        uint64_t lc = 0; int64_t lm = 0;
        uint64_t rc_ = 0; int64_t rm = 0;
        OK(tsdb_cluster_local_table_stats(db, "t", &lc, &lm));
        int rc = tsdb_rpc_local_table_stats(conn, "t", &rc_, &rm);
        printf("  local=(%llu,%lld) over-rpc=(%llu,%lld) rc=%d\n",
               (unsigned long long)lc, (long long)lm,
               (unsigned long long)rc_, (long long)rm, rc);
        CHECK(rc == TSDB_OK, "probe succeeds against a current peer");
        CHECK(rc_ == 100 && rc_ == lc, "count over the wire == the peer's own count");
        CHECK(rm == BASE + 99 && rm == lm, "max_ts over the wire == the peer's own max_ts");

        /* A flush moves rows memtable → partition; the wire answer must not
         * care which side of the flush they are on. */
        OK(tsdb_db_flush_all(db));
        rc_ = 0; rm = 0;
        CHECK(tsdb_rpc_local_table_stats(conn, "t", &rc_, &rm) == TSDB_OK &&
              rc_ == 100 && rm == BASE + 99,
              "unchanged across a flush (100, BASE+99)");

        /* THE PHANTOM READING.  A table this peer does not hold is EMPTY —
         * not the cluster's total, and not an error. */
        rc_ = 12345; rm = 999;
        rc = tsdb_rpc_local_table_stats(conn, "absent_here", &rc_, &rm);
        CHECK(rc == TSDB_OK && rc_ == 0 && rm == INT64_MIN,
              "a table the peer does not hold reads (0, INT64_MIN)");
    }

    /* ── [2] the anti-entropy probe is wired to the opcode ───────────────── */
    printf("\n[2] tsdb_cluster_peer_table_stats_conn uses it\n");
    {
        uint64_t pc = 0; int64_t pm = 0;
        CHECK(tsdb_cluster_peer_table_stats_conn(conn, "t", &pc, &pm) == 0 &&
              pc == 100 && pm == BASE + 99,
              "populated peer probed as (100, BASE+99)");

        /* Discriminator: on the legacy FED_QUERY path the peer's query for an
         * unknown table FAILS, the peer replies ERR, and the probe returns -1
         * — the peer is dropped rather than recorded as holding nothing.
         * Wired to the new opcode it is recorded truthfully as empty. */
        pc = 12345; pm = 999;
        int prc = tsdb_cluster_peer_table_stats_conn(conn, "absent_here",
                                                     &pc, &pm);
        printf("  absent table: rc=%d count=%llu max_ts=%lld\n",
               prc, (unsigned long long)pc, (long long)pm);
        CHECK(prc == 0 && pc == 0 && pm == INT64_MIN,
              "empty peer measured as (0, INT64_MIN), not dropped");
    }

    /* ── [5] and that reading keeps a phantom out of the candidate set ───── */
    printf("\n[3] an empty peer can never win the pull selection\n");
    {
        /* The resync seeds its search with the local numbers and only accepts
         * a peer that is strictly ahead:
         *     pc > best_count || (pc == best_count && pmax > best_max_ts)
         * A truthful empty peer ties the empty local node and loses; the real
         * holder wins.  Before the fix both reported the SAME cluster-wide
         * aggregate and the winner was gossip order. */
        uint64_t local_count = 0; int64_t local_max = INT64_MIN;
        uint64_t empty_c = 0;     int64_t empty_m   = INT64_MIN;
        uint64_t holder_c = 100;  int64_t holder_m  = BASE + 99;

        int empty_wins  = (empty_c > local_count) ||
                          (empty_c == local_count && empty_m > local_max);
        int holder_wins = (holder_c > local_count) ||
                          (holder_c == local_count && holder_m > local_max);
        CHECK(!empty_wins, "empty peer never displaces the local seed");
        CHECK(holder_wins, "the real holder does");
        CHECK(tsdb_antientropy_decide(local_count, local_max,
                                      empty_c, empty_m) == TSDB_AE_UP_TO_DATE,
              "decide(local empty, peer empty) = UP_TO_DATE (no truncate)");
        CHECK(tsdb_antientropy_decide(local_count, local_max,
                                      holder_c, holder_m) == TSDB_AE_TAIL_PULL,
              "decide(local empty, real holder) = TAIL_PULL");
    }

    tsdb_rpc_conn_close(conn);
    tsdb_rpc_server_stop(srv);

    /* ── [3] OLD PEER: must degrade, not error ───────────────────────────── */
    printf("\n[4] a peer too old for the opcode degrades to FED_QUERY\n");
    {
        stub_t s; pthread_t th;
        int sport = stub_start(&s, &th, STUB_OLD_BINARY);
        if (sport < 0) { fprintf(stderr, "FATAL: stub\n"); return 1; }
        tsdb_rpc_conn_t *sc = dial(sport);
        if (!sc) { fprintf(stderr, "FATAL: dial stub\n"); return 1; }

        uint64_t c = 0; int64_t m = 0;
        int rc = tsdb_rpc_local_table_stats(sc, "t", &c, &m);
        CHECK(rc == TSDB_ERR_UNSUPPORTED,
              "empty ACK is reported as TSDB_ERR_UNSUPPORTED (rc=%d)", rc);

        c = 0; m = 0;
        int prc = tsdb_cluster_peer_table_stats_conn(sc, "t", &c, &m);
        printf("  old peer: rc=%d count=%llu max_ts=%lld fed_query_seen=%d\n",
               prc, (unsigned long long)c, (long long)m, s.saw_fed_query);
        CHECK(prc == 0, "the probe still succeeds against an old peer");
        CHECK(s.saw_fed_query == 1, "it fell back to FED_QUERY");
        CHECK(c == (uint64_t)STUB_COUNT && m == STUB_MAXTS,
              "and returned the legacy answer verbatim");

        tsdb_rpc_conn_close(sc);
        pthread_join(th, NULL);
        close(s.listen_fd);
    }

    /* ── [4] a peer that refuses is skipped, not re-asked over the query ── */
    printf("\n[5] a peer that knows the opcode and refuses is skipped\n");
    {
        stub_t s; pthread_t th;
        int sport = stub_start(&s, &th, STUB_ERR);
        if (sport < 0) { fprintf(stderr, "FATAL: stub\n"); return 1; }
        tsdb_rpc_conn_t *sc = dial(sport);
        if (!sc) { fprintf(stderr, "FATAL: dial stub\n"); return 1; }

        uint64_t c = 12345; int64_t m = 999;
        int prc = tsdb_cluster_peer_table_stats_conn(sc, "t", &c, &m);
        CHECK(prc == -1, "reported as no-answer (rc=%d)", prc);
        CHECK(s.saw_fed_query == 0,
              "not re-asked with the query that can return the cluster total");

        tsdb_rpc_conn_close(sc);
        pthread_join(th, NULL);
        close(s.listen_fd);
    }

    tsdb_close(db);
    rm_rf(TDIR);

    if (g_fail) {
        printf("\n=== test_ae_peer_stats FAILED (%d) ===\n", g_fail);
        return 1;
    }
    printf("\n=== test_ae_peer_stats PASSED ===\n");
    return 0;
}
