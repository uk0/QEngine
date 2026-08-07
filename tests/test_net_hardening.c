/* test_net_hardening.c — connection-stability + FQDN hardening for the
 * inter-node RPC dialer (src/cluster/rpc.c tsdb_rpc_connect), plus
 * wire-server (src/server/server.c) accept-loop and frame-guard hardening.
 *
 * Test A — keepalive: a connected dialer fd must carry SO_KEEPALIVE=1 (so a
 *   silently-dead peer is reaped instead of wedging an RPC slot).  On Linux the
 *   idle probe threshold (TCP_KEEPIDLE) must be the configured 30 s default.
 *
 * Test B — dual-stack resolve: dialing the FQDN "localhost" must succeed.  The
 *   old AF_INET + first-result-only path could pick a v6 "localhost" address
 *   and fail; AF_UNSPEC + iterate-all tries every result until one connects.
 *
 * Test C — WRITE_BATCH symbol-total guard must not wrap: a SYMBOL column
 *   declaring [u32 total] = 0xFFFFFFFD inside an 8-byte column must be
 *   answered ERROR/TSDB_ERR_INVAL.  A guard written as `total + 4 > col_size`
 *   does the addition in uint32, wraps to 1 for totals near UINT32_MAX, and
 *   admits the bogus extent into the bulk-append path.
 *
 * Test D — the wire accept loop must survive transient accept() errors: the
 *   accept thread is spawned exactly once at server start and nothing ever
 *   restarts it, so treating EMFILE as fatal converts momentary fd exhaustion
 *   into a permanent endpoint outage — the listen fd stays open, the kernel
 *   keeps completing handshakes into the backlog, and clients see connects
 *   succeed with replies that never come.  The test pins RLIMIT_NOFILE just
 *   above the process's live fd count, wakes the accept thread into EMFILE,
 *   releases everything, and then requires a fresh connect + HELLO + write +
 *   query to succeed.
 *
 * tsdb_rpc_conn is opaque; its first member is `int fd` (see struct
 * tsdb_rpc_conn in rpc.c), so the connected fd is *(int *)conn.
 */

#include "../src/cluster/rpc.h"
#include "../src/server/server.h"
#include "../src/server/proto.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

/* Minimal accept loop: accept one connection, hold it briefly, then close. */
static void *accept_one(void *arg) {
    int listen_fd = *(int *)arg;
    struct sockaddr_in cli;
    socklen_t clen = sizeof(cli);
    int cfd = accept(listen_fd, (struct sockaddr *)&cli, &clen);
    if (cfd >= 0) {
        usleep(50 * 1000);   /* let the dialer finish setsockopt + assert */
        close(cfd);
    }
    return NULL;
}

/* Bind 127.0.0.1:0, listen, return the fd; *out_port gets the chosen port. */
static int listen_ephemeral(int *out_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) { close(fd); return -1; }
    if (listen(fd, 4) != 0) { close(fd); return -1; }
    socklen_t sl = sizeof(sa);
    if (getsockname(fd, (struct sockaddr *)&sa, &sl) != 0) { close(fd); return -1; }
    *out_port = ntohs(sa.sin_port);
    return fd;
}

/* Test A: keepalive on the dialer fd (+ TCP_KEEPIDLE==30 on Linux). */
static void test_keepalive(void) {
    int port = 0;
    int lfd = listen_ephemeral(&port);
    CHECK(lfd >= 0, "ephemeral listener on 127.0.0.1");
    if (lfd < 0) return;

    pthread_t th;
    pthread_create(&th, NULL, accept_one, &lfd);

    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    CHECK(conn != NULL, "tsdb_rpc_connect(127.0.0.1) succeeds");

    if (conn) {
        int fd = *(int *)conn;   /* first member of struct tsdb_rpc_conn */
        int ka = 0; socklen_t kl = sizeof(ka);
        int rc = getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &ka, &kl);
        /* getsockopt returns the option's stored value, which is 1 on Linux but
         * the SO_KEEPALIVE flag bit (nonzero, e.g. 8) on macOS/BSD — assert
         * "enabled", not a literal 1. */
        CHECK(rc == 0 && ka != 0, "dialer fd has SO_KEEPALIVE enabled");

#ifdef __linux__
        int idle = 0; socklen_t il = sizeof(idle);
        int irc = getsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, &il);
        CHECK(irc == 0 && idle == 30, "dialer fd TCP_KEEPIDLE == 30 (Linux)");
#endif
        tsdb_rpc_conn_close(conn);
    }

    pthread_join(th, NULL);
    close(lfd);
}

/* Test B: dual-stack iterate-all resolves and connects via "localhost". */
static void test_dualstack_localhost(void) {
    int port = 0;
    int lfd = listen_ephemeral(&port);
    CHECK(lfd >= 0, "ephemeral listener for localhost test");
    if (lfd < 0) return;

    pthread_t th;
    pthread_create(&th, NULL, accept_one, &lfd);

    char addr[64];
    snprintf(addr, sizeof(addr), "localhost:%d", port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    CHECK(conn != NULL, "tsdb_rpc_connect(localhost) succeeds (dual-stack)");
    if (conn) tsdb_rpc_conn_close(conn);

    pthread_join(th, NULL);
    close(lfd);
}

/* ==== Tests C & D — wire-server hardening ================================= */

static tsdb_server_t *g_nh_srv  = NULL;
static tsdb_db_t     *g_nh_db   = NULL;
static int            g_nh_port = 0;
static char           g_nh_dir[256];

static int nh_setup_server(void) {
    /* One listener ⇒ one accept thread.  Test D must prove THE accept thread
     * survives EMFILE, which is only deterministic when there is exactly one
     * thread to kill (with several SO_REUSEPORT listeners a surviving sibling
     * could mask a dead one). */
    setenv("TSDB_WIRE_ACCEPT_THREADS", "1", 1);
    snprintf(g_nh_dir, sizeof(g_nh_dir), "/tmp/tsdb_test_nethard_%d", (int)getpid());

    if (tsdb_open(g_nh_dir, &g_nh_db) != TSDB_OK) return -1;
    tsdb_server_opts_t opts = {
        .bind_addr = "127.0.0.1:0",   /* port 0 = OS assigns */
        .max_conns = 64,
        .db        = g_nh_db,
    };
    if (tsdb_server_start(&opts, &g_nh_srv) != TSDB_OK) return -1;
    g_nh_port = tsdb_server_port(g_nh_srv);
    usleep(10000); /* give the accept thread time to start */
    return 0;
}

static void nh_teardown_server(void) {
    if (g_nh_srv) { tsdb_server_stop(g_nh_srv); g_nh_srv = NULL; }
    if (g_nh_db)  { tsdb_close(g_nh_db); g_nh_db = NULL; }
    char cmd[300];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_nh_dir);
    (void)system(cmd);
}

static int nh_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    for (int i = 0; i < 50; i++) {
        if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) return fd;
        if (errno == ECONNREFUSED) { usleep(20000); continue; }
        break;
    }
    close(fd);
    return -1;
}

/* Poll-gated frame receive: FAILS instead of hanging when the server never
 * answers — which is the exact symptom of a dead accept thread (the kernel
 * completed our handshake into the backlog, so connect()/send() succeed and
 * only the missing reply betrays the outage). */
static int nh_recv_deadline(int fd, tsdb_frame_hdr_t *hdr, uint8_t **out, int ms) {
    struct pollfd pfd = { fd, POLLIN, 0 };
    int r = poll(&pfd, 1, ms);
    if (r <= 0) return -1000;   /* timeout (or poll error): no reply */
    return tsdb_proto_recv(fd, hdr, out);
}

/* HELLO round-trip with a reply deadline; 0 on success. */
static int nh_hello(int fd, int ms) {
    if (tsdb_proto_send(fd, TSDB_MT_HELLO, 0, 1, NULL, 0) != TSDB_OK) return -1;
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    int rc = nh_recv_deadline(fd, &hdr, &pl, ms);
    free(pl);
    if (rc != TSDB_OK) return rc;
    return (hdr.type == TSDB_MT_HELLO_OK) ? 0 : -2;
}

/* Test C: symbol [u32 total] near UINT32_MAX must be rejected, not wrapped. */
static void test_symbol_total_overflow(int port) {
    int fd = nh_connect(port);
    CHECK(fd >= 0, "connect for symbol-guard test");
    if (fd < 0) return;
    CHECK(nh_hello(fd, 3000) == 0, "HELLO before symbol-guard write");

    /* CREATE TABLE nh_sym (ts TIMESTAMP, sym SYMBOL) */
    {
        uint8_t b[64]; uint8_t *p = b;
        *p++ = 6; memcpy(p, "nh_sym", 6); p += 6;      /* table name  */
        *p++ = 2; memcpy(p, "ts", 2); p += 2;          /* ts col name */
        *p++ = 2;                                      /* ncols       */
        *p++ = 2; memcpy(p, "ts", 2); p += 2;  *p++ = TSDB_TYPE_TIMESTAMP;
        *p++ = 3; memcpy(p, "sym", 3); p += 3; *p++ = TSDB_TYPE_SYMBOL;
        tsdb_proto_send(fd, TSDB_MT_CREATE_TABLE, 0, 2, b, (size_t)(p - b));
        tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
        int rc = nh_recv_deadline(fd, &hdr, &pl, 3000);
        CHECK(rc == TSDB_OK && hdr.type == TSDB_MT_HELLO_OK, "CREATE nh_sym ok");
        free(pl);
    }

    /* WRITE_BATCH, 1 row.  The SYMBOL column is 8 bytes and declares
     * [u32 total] = 0xFFFFFFFD; the honest check "4 + total <= col_size" is
     * false by ~4 GiB, but computed as uint32 `total + 4` it wraps to 1 and
     * passes.  The row entry itself ([u16 len=2]"AB") is well-formed, so an
     * unfixed server ACKs the frame instead of crashing — the failure
     * signature is deterministic: WRITE_ACK where ERROR is required. */
    {
        uint8_t b[128]; uint8_t *p = b;
        *p++ = 6; memcpy(p, "nh_sym", 6); p += 6;
        uint16_t nc = 2; memcpy(p, &nc, 2); p += 2;
        uint32_t nr = 1;  memcpy(p, &nr, 4); p += 4;
        /* ts col */
        *p++ = 2; memcpy(p, "ts", 2); p += 2;
        *p++ = TSDB_TYPE_TIMESTAMP; *p++ = 0;          /* codec RAW */
        uint32_t csz = 8; memcpy(p, &csz, 4); p += 4;
        int64_t ts = 1000000000000000000LL; memcpy(p, &ts, 8); p += 8;
        /* sym col: [u32 total=0xFFFFFFFD][u16 len=2]['A']['B'] */
        *p++ = 3; memcpy(p, "sym", 3); p += 3;
        *p++ = TSDB_TYPE_SYMBOL; *p++ = 0;
        csz = 8; memcpy(p, &csz, 4); p += 4;
        uint32_t total = 0xFFFFFFFDu; memcpy(p, &total, 4); p += 4;
        uint16_t sl = 2; memcpy(p, &sl, 2); p += 2;
        *p++ = 'A'; *p++ = 'B';
        tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, 3, b, (size_t)(p - b));

        tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
        int rc = nh_recv_deadline(fd, &hdr, &pl, 3000);
        CHECK(rc == TSDB_OK, "reply to wrapping-total WRITE_BATCH");
        CHECK(rc == TSDB_OK && hdr.type == TSDB_MT_ERROR,
              "wrapping symbol total answered ERROR, not WRITE_ACK");
        if (rc == TSDB_OK && hdr.type == TSDB_MT_ERROR && pl && hdr.payload_len >= 4) {
            int32_t ec; memcpy(&ec, pl, 4);
            CHECK(ec == TSDB_ERR_INVAL, "error code is TSDB_ERR_INVAL");
        }
        free(pl);
    }
    close(fd);
}

/* Test D helper: the post-recovery end-to-end proof — create a fresh table,
 * write 10 rows, read count(*) back.  Returns 1 when every step round-trips
 * inside its deadline. */
static int nh_write_query_roundtrip(int fd) {
    tsdb_frame_hdr_t hdr; uint8_t *pl = NULL;
    uint8_t b[512]; uint8_t *p = b;

    /* CREATE TABLE nh_c (ts TIMESTAMP, v INT64) */
    *p++ = 4; memcpy(p, "nh_c", 4); p += 4;
    *p++ = 2; memcpy(p, "ts", 2); p += 2;
    *p++ = 2;
    *p++ = 2; memcpy(p, "ts", 2); p += 2; *p++ = TSDB_TYPE_TIMESTAMP;
    *p++ = 1; *p++ = 'v';                 *p++ = TSDB_TYPE_INT64;
    tsdb_proto_send(fd, TSDB_MT_CREATE_TABLE, 0, 4, b, (size_t)(p - b));
    if (nh_recv_deadline(fd, &hdr, &pl, 5000) != TSDB_OK) return 0;
    int t = hdr.type; free(pl); pl = NULL;
    if (t != TSDB_MT_HELLO_OK) return 0;

    /* WRITE_BATCH: 10 rows */
    p = b;
    *p++ = 4; memcpy(p, "nh_c", 4); p += 4;
    uint16_t nc = 2; memcpy(p, &nc, 2); p += 2;
    uint32_t nr = 10; memcpy(p, &nr, 4); p += 4;
    *p++ = 2; memcpy(p, "ts", 2); p += 2;
    *p++ = TSDB_TYPE_TIMESTAMP; *p++ = 0;
    uint32_t csz = 80; memcpy(p, &csz, 4); p += 4;
    for (int i = 0; i < 10; i++) {
        int64_t ts = 5000000000000000000LL + (int64_t)i * 1000000LL;
        memcpy(p, &ts, 8); p += 8;
    }
    *p++ = 1; *p++ = 'v';
    *p++ = TSDB_TYPE_INT64; *p++ = 0;
    csz = 80; memcpy(p, &csz, 4); p += 4;
    for (int i = 0; i < 10; i++) { int64_t v = i; memcpy(p, &v, 8); p += 8; }
    tsdb_proto_send(fd, TSDB_MT_WRITE_BATCH, 0, 5, b, (size_t)(p - b));
    if (nh_recv_deadline(fd, &hdr, &pl, 5000) != TSDB_OK) return 0;
    t = hdr.type; free(pl); pl = NULL;
    if (t != TSDB_MT_WRITE_ACK) return 0;

    /* QUERY count(*) — expect HDR, then ROWS(count=10) ... FIN */
    const char *q = "SELECT count(*) FROM nh_c";
    tsdb_proto_send(fd, TSDB_MT_QUERY, 0, 6, (const uint8_t *)q, strlen(q));
    if (nh_recv_deadline(fd, &hdr, &pl, 5000) != TSDB_OK) return 0;
    t = hdr.type; free(pl); pl = NULL;
    if (t != TSDB_MT_QUERY_RESULT_HDR) return 0;
    int64_t cnt = -1; int fin = 0;
    while (!fin) {
        if (nh_recv_deadline(fd, &hdr, &pl, 5000) != TSDB_OK) return 0;
        if (hdr.type != TSDB_MT_QUERY_RESULT_ROWS) { free(pl); return 0; }
        if (pl && hdr.payload_len >= 6 + 8) {
            uint32_t rn; memcpy(&rn, pl, 4);
            if (rn == 1) memcpy(&cnt, pl + 6, 8);
        }
        fin = (hdr.flags & TSDB_FLAG_FIN) != 0;
        free(pl); pl = NULL;
    }
    return cnt == 10;
}

/* Test D: exhaust the process fd table so accept() returns EMFILE, release,
 * then require the endpoint to still answer. */
static void test_accept_survives_emfile(int port) {
    /* Sanity: the endpoint answers before the exhaustion cycle. */
    int fd = nh_connect(port);
    CHECK(fd >= 0, "pre-exhaustion connect");
    if (fd < 0) return;
    CHECK(nh_hello(fd, 3000) == 0, "pre-exhaustion HELLO answered");
    close(fd);
    usleep(150 * 1000);   /* let the handler thread notice EOF and free its fd */

    struct rlimit old_rl;
    if (getrlimit(RLIMIT_NOFILE, &old_rl) != 0) { CHECK(0, "getrlimit"); return; }

    /* Baseline from THIS process's live fd table — never an assumed count.
     * The new soft limit sits a small headroom above the highest open fd; the
     * fill loop below consumes that headroom (plus any holes) exactly. */
    int max_fd = -1;
    long scan_hi = (old_rl.rlim_cur != RLIM_INFINITY && old_rl.rlim_cur < 65536)
                       ? (long)old_rl.rlim_cur : 65536;
    for (long i = 0; i < scan_hi; i++)
        if (fcntl((int)i, F_GETFD) != -1) max_fd = (int)i;
    CHECK(max_fd >= 2, "fd baseline scan found the stdio fds");

    struct rlimit low_rl = old_rl;
    low_rl.rlim_cur = (rlim_t)(max_fd + 1 + 10);
    CHECK(setrlimit(RLIMIT_NOFILE, &low_rl) == 0, "lower RLIMIT_NOFILE");

    /* Client sockets created BEFORE the fill: connect() allocates no new fd,
     * so these can complete handshakes while the table is full and wake the
     * accept thread straight into accept() == EMFILE. */
    int pc[3];
    for (int i = 0; i < 3; i++) pc[i] = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(pc[0] >= 0 && pc[1] >= 0 && pc[2] >= 0, "pre-created client sockets");

    int fill[512], nfill = 0;
    errno = 0;
    while (nfill < 512) {
        int d = dup(2);
        if (d < 0) break;
        fill[nfill++] = d;
    }
    /* Darwin reports allocation-at-the-limit as EBADF where POSIX (and
     * Linux) say EMFILE — the same vocabulary accept() uses, which is why
     * the server's transient set must cover both.  What matters here is
     * that the loop ended on exhaustion, not on the array cap. */
    int fill_errno = errno;
    int exhausted = nfill < 512 &&
        (fill_errno == EMFILE || fill_errno == ENFILE || fill_errno == EBADF);
    if (!exhausted)
        fprintf(stderr, "  fd-fill diagnostic: nfill=%d errno=%d (%s)\n",
                nfill, fill_errno, strerror(fill_errno));
    CHECK(exhausted, "fd table filled to exhaustion (EMFILE/ENFILE/Darwin EBADF)");

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    int nconn = 0;
    for (int i = 0; i < 3; i++)
        if (pc[i] >= 0 && connect(pc[i], (struct sockaddr *)&sa, sizeof(sa)) == 0)
            nconn++;
    CHECK(nconn == 3, "handshakes completed into the backlog with fd table full");

    /* ≥3 accept-loop poll cycles (200 ms each): the accept thread definitely
     * woke up and took accept() == EMFILE at least once. */
    usleep(800 * 1000);

    /* Release EVERYTHING on this one shared path — CHECK never aborts, so
     * every branch above falls through to here and a poisoned rlimit cannot
     * leak into later tests in this binary. */
    for (int i = 0; i < nfill; i++) close(fill[i]);
    for (int i = 0; i < 3; i++) if (pc[i] >= 0) close(pc[i]);
    CHECK(setrlimit(RLIMIT_NOFILE, &old_rl) == 0, "restore RLIMIT_NOFILE");

    /* The proof.  A dead accept thread still completes handshakes (the listen
     * fd is open and the backlog has room), so this connect succeeds either
     * way — only the deadline-gated reply separates alive from dead. */
    fd = nh_connect(port);
    CHECK(fd >= 0, "post-exhaustion connect");
    if (fd < 0) return;
    int hello_ok = (nh_hello(fd, 5000) == 0);
    CHECK(hello_ok, "accept thread survived EMFILE: HELLO answered after recovery");
    if (hello_ok)
        CHECK(nh_write_query_roundtrip(fd),
              "post-exhaustion write+query round-trip (count == 10)");
    close(fd);
}

int main(void) {
    printf("=== test_net_hardening ===\n");

    printf("\n[A] TCP keepalive on the RPC dialer fd\n");
    test_keepalive();

    printf("\n[B] dual-stack getaddrinfo: connect via localhost\n");
    test_dualstack_localhost();

    if (nh_setup_server() != 0) {
        fprintf(stderr, "FAIL: could not start in-process wire server\n");
        g_fail++;
    } else {
        printf("\n[C] wire symbol-total guard rejects wrapping totals\n");
        test_symbol_total_overflow(g_nh_port);

        printf("\n[D] accept loop survives EMFILE fd exhaustion\n");
        test_accept_survives_emfile(g_nh_port);
    }
    nh_teardown_server();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
