/* test_rpc_hardening.c — the inter-node RPC read path must survive a hostile
 * peer that has proved nothing.
 *
 * THE BUGS.  The auth handshake added in the previous round gates DISPATCH, but
 * everything below it runs BEFORE any credential check, on a socket anyone who
 * can reach the port may open:
 *
 *   [1] recv_frame took the 32-bit payload_len straight off the wire and called
 *       malloc(TSDB_RPC_HDR_SIZE + plen) with no ceiling.  One forged 18-byte
 *       header asks the node for a ~4 GiB allocation; a handful of them are an
 *       out-of-memory kill, unauthenticated and free.
 *   [2] The frame's version byte was read and then discarded
 *       (`uint8_t ver = buf[4]; (void)ver;`), so a frame claiming a version
 *       this build does not implement was parsed field-by-field as if it were
 *       version 1 rather than refused.
 *
 * THE FIX.  Both are header-level checks in recv_frame, before the allocation:
 * an unknown version and a payload_len above TSDB_RPC_MAX_PAYLOAD each drop the
 * connection.  A body read is additionally bounded by a receive timeout that is
 * armed only once a header has arrived and cleared right after, so a peer that
 * stalls mid-frame cannot pin a handler thread while idle pooled connections —
 * which legitimately sit for minutes between requests — keep blocking as before.
 *
 * WHAT THIS PINS (single process, a real RPC server on 127.0.0.1:0):
 *   [1] A forged payload_len of 200 MiB makes the server CLOSE the connection
 *       rather than sit in read_full waiting for a body it has already
 *       allocated that memory for.  Prompt EOF is the whole assertion:
 *       "no reply" alone does NOT separate fixed from broken, because the
 *       broken node also never replies — it just waits, holding the memory.
 *       (200 MiB and not 4 GiB deliberately: a 4 GiB malloc simply fails on
 *       most hosts, so the unfixed node drops the connection too and the case
 *       proves nothing.  See the note at the call site.)
 *   [2] A legitimate peer still completes HEARTBEAT afterwards — the cap
 *       rejects nothing real and the node survived the attack.
 *
 * NOT TESTED, and honestly so: the version gate in recv_frame.  The frame CRC
 * already covers the version byte, so a frame with a tampered version fails the
 * checksum first and is refused for that reason both before and after the fix —
 * an assertion on it would be green on the unfixed tree and prove nothing.  The
 * gate matters only for a genuine future peer that sends an unknown version
 * with a CORRECT checksum, which no code in this repo can currently emit
 * (crc32 is static to rpc.c), and reimplementing the checksum here just to
 * manufacture that frame would make the test lie the moment the two diverge.
 */
#include "tsdb.h"
#include "../src/cluster/rpc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static int g_fail = 0;

#define CHECK(cond, msg) do {                                            \
    if (!(cond)) { fprintf(stderr, "FAIL %s:%d: %s\n",                   \
                           __FILE__, __LINE__, msg); g_fail++; }         \
    else printf("  PASS: %s\n", msg);                                    \
} while (0)

#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) {                  \
    fprintf(stderr, "FATAL %s:%d rc=%d\n", __FILE__, __LINE__, _r);      \
    exit(1); } } while (0)

#define TDIR "/tmp/tsdb_test_rpc_hardening"
#define BASE 1700000000000000000LL

static void rm_rf(const char *p) {
    char c[512];
    snprintf(c, sizeof(c), "rm -rf %s", p);
    (void)system(c);
}

static int raw_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_aton("127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) { close(fd); return -1; }
    /* Bound our own reads: a refused frame closes the connection, and we must
     * observe that as EOF rather than hanging the test if it does not. */
    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return fd;
}

static int write_all(int fd, const uint8_t *b, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, b + off, n - off);
        if (w <= 0) return -1;
        off += (size_t)w;
    }
    return 0;
}

/* A well-formed header whose payload_len field is then overwritten with a lie.
 * Magic and version still pass, so the frame reaches exactly the length check
 * this test is about. */
static int send_forged_len(int fd, uint32_t plen) {
    uint8_t hdr[TSDB_RPC_HDR_SIZE];
    int fn = tsdb_rpc_encode(hdr, sizeof(hdr), TSDB_RPC_CATALOG_DUMP, 1, NULL, 0);
    if (fn <= 0) return -1;
    memcpy(hdr + 10, &plen, 4);
    return write_all(fd, hdr, (size_t)fn);
}

/* Distinguish the THREE outcomes, because "no reply" alone does not separate
 * fixed from broken: a node that trusts the forged length also sends no reply —
 * it is sitting in read_full waiting for a body that will never arrive, with the
 * allocation already made.  Only prompt EOF proves the frame was refused.
 *   RES_FRAME(0)  peer answered with a frame
 *   RES_EOF(1)    peer closed  -> refused before committing to the body
 *   RES_OPEN(2)   nothing, connection still open -> peer is waiting for a body */
#define RES_FRAME 0
#define RES_EOF   1
#define RES_OPEN  2

static int read_outcome(int fd) {
    uint8_t hdr[TSDB_RPC_HDR_SIZE];
    size_t off = 0;
    while (off < sizeof(hdr)) {
        ssize_t r = read(fd, hdr + off, sizeof(hdr) - off);
        if (r == 0) return RES_EOF;                 /* clean close */
        if (r < 0)  return RES_OPEN;                /* SO_RCVTIMEO fired */
        off += (size_t)r;
    }
    return RES_FRAME;
}

int main(void) {
    printf("=== test_rpc_hardening ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_INT64} };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    {
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < 10; i++) {
            OK(tsdb_batch_row_ts(b, BASE + i));
            OK(tsdb_batch_row_i64(b, 1, i));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }

    tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", db, NULL);
    if (!srv) { fprintf(stderr, "FATAL: rpc server\n"); return 1; }
    int port = tsdb_rpc_server_port(srv);

    /* ── [1] a forged payload_len must not become an allocation ─────────── */
    printf("\n[1] a forged oversize payload_len is refused, not allocated\n");
    {
        int fd = raw_connect(port);
        CHECK(fd >= 0, "raw connect to the RPC port");
        /* 200 MiB, deliberately NOT 4 GiB.  A 4 GiB request simply fails to
         * allocate on most hosts, so the unfixed node also drops the
         * connection and the case proves nothing.  200 MiB is comfortably
         * allocatable yet far above TSDB_RPC_MAX_PAYLOAD, so it separates the
         * two: it is also the more realistic attack, since a few dozen
         * connections each parking 200 MiB exhaust a node far more reliably
         * than one oversized malloc that fails outright. */
        CHECK(send_forged_len(fd, 200u * 1024u * 1024u) == 0,
              "sent a header declaring a 200 MiB body");
        /* Pre-fix: the length is trusted, the 200 MiB is allocated, and the
         * handler blocks in read_full waiting for a body that never comes
         * -> RES_OPEN.
         * Post-fix: the length is rejected against TSDB_RPC_MAX_PAYLOAD before
         * the allocation and the connection is dropped -> RES_EOF. */
        int outcome = read_outcome(fd);
        CHECK(outcome == RES_EOF,
              outcome == RES_OPEN
                ? "REFUSED: server closed instead of parking memory for the body"
                : "REFUSED: server closed rather than serving the forged frame");
        close(fd);
    }

    /* ── [2] the node is still alive and still serves real traffic ──────── */
    printf("\n[2] legitimate RPC still works after the attack\n");
    {
        char addr[64];
        snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
        tsdb_rpc_conn_t *c = tsdb_rpc_connect(addr, 2000);
        CHECK(c != NULL, "connect succeeds after the attacks");
        if (c) {
            CHECK(tsdb_rpc_call(c, TSDB_RPC_HEARTBEAT, NULL, 0) == TSDB_OK,
                  "HEARTBEAT still answered");
            tsdb_rpc_conn_close(c);
        }
    }

    tsdb_rpc_server_stop(srv);
    tsdb_close(db);
    rm_rf(TDIR);

    if (g_fail) { printf("\n%d FAILED\n", g_fail); return 1; }
    printf("\nALL PASS\n");
    return 0;
}
