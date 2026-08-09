/* test_rpc_auth.c — the inter-node RPC plane must authenticate the peer.
 *
 * THE BUG.  connection_handler (src/cluster/rpc.c) dispatched EVERY opcode the
 * instant a frame arrived, with no credential check — the only guard was an
 * "already mutually trusted" comment.  DDL_FORWARD runs arbitrary DDL/SELECT,
 * CATALOG_DUMP ships the whole catalog, WRITE_BATCH ingests.  RPC-level TLS is
 * opt-in and off by default, so any host that can reach the port owned the
 * data.  A raw socket that never proves anything could pull the catalog.
 *
 * THE FIX.  When TSDB_RPC_SECRET is set, the server challenges each freshly
 * accepted peer with a random nonce and refuses to dispatch a single opcode
 * until the peer returns HMAC-SHA256(secret, nonce); tsdb_rpc_connect answers
 * the same challenge so intra-cluster RPC still works.  Secret unset preserves
 * the legacy plaintext plane (byte-identical wire) with a one-time warning.
 *
 * WHAT THIS PINS (single process, a real RPC server on 127.0.0.1:0):
 *   [1] With a secret set, a raw connection that skips the handshake and sends
 *       CATALOG_DUMP is NOT answered with an ACK — no catalog is exfiltrated.
 *       (Pre-fix this returned an ACK carrying the catalog.)
 *   [2] A properly-authenticated peer (tsdb_rpc_connect) still works end to
 *       end: HEARTBEAT and CATALOG_DUMP both succeed.
 *   [3] A raw peer that answers the challenge with a WRONG tag is refused —
 *       the server does not dispatch its follow-up CATALOG_DUMP.
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

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
    fprintf(stderr, "FATAL %s:%d rc=%d (%s)\n", __FILE__, __LINE__, _r,   \
            tsdb_errstr(_r)); exit(1); } } while (0)

#define TDIR   "/tmp/tsdb_test_rpc_auth"
#define SECRET "shared-cluster-secret-9f3a"
#define BASE   1700000000000000000LL

/* Same opcode numbers the fixed server uses for the handshake (rpc.h). */
#define AUTH_CHALLENGE 26
#define AUTH_RESPONSE  27

static void rm_rf(const char *p) {
    char cmd[4096];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best-effort cleanup */ }
}

/* ---- raw client (no libtsdb framing help beyond tsdb_rpc_encode) --------- */

/* Blocking loopback socket with a short read timeout so a refused handshake
 * surfaces as a fast error, never a hang. */
static int raw_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = htons((uint16_t)port);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) { close(fd); return -1; }
    struct timeval tv = { .tv_sec = 3, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return fd;
}

static int write_all(int fd, const uint8_t *b, size_t n) {
    for (size_t off = 0; off < n; ) {
        ssize_t w = write(fd, b + off, n - off);
        if (w <= 0) return -1;
        off += (size_t)w;
    }
    return 0;
}

/* Read exactly n bytes; -1 on EOF/timeout/error. */
static int read_all(int fd, uint8_t *b, size_t n) {
    for (size_t off = 0; off < n; ) {
        ssize_t r = read(fd, b + off, n - off);
        if (r <= 0) return -1;
        off += (size_t)r;
    }
    return 0;
}

/* Send one framed opcode with no payload. */
static int raw_send_op(int fd, int type, uint32_t req_id) {
    uint8_t frame[TSDB_RPC_HDR_SIZE];
    int fn = tsdb_rpc_encode(frame, sizeof(frame),
                             (tsdb_rpc_type_t)type, req_id, NULL, 0);
    if (fn <= 0) return -1;
    return write_all(fd, frame, (size_t)fn);
}

/* Send one framed opcode with a payload. */
static int raw_send_op_body(int fd, int type, uint32_t req_id,
                            const uint8_t *body, uint32_t blen) {
    uint8_t frame[TSDB_RPC_HDR_SIZE + 64];
    if (blen > 64) return -1;
    int fn = tsdb_rpc_encode(frame, sizeof(frame),
                             (tsdb_rpc_type_t)type, req_id, body, blen);
    if (fn <= 0) return -1;
    return write_all(fd, frame, (size_t)fn);
}

/* Read one frame header; returns 0 and fills type/plen (payload drained),
 * -1 on EOF/timeout. */
static int raw_read_frame(int fd, int *type, uint32_t *plen) {
    uint8_t hdr[TSDB_RPC_HDR_SIZE];
    if (read_all(fd, hdr, sizeof(hdr)) != 0) return -1;
    *type = hdr[5];
    memcpy(plen, hdr + 10, 4);
    /* Drain the payload so the next read aligns on a header. */
    uint32_t left = *plen;
    uint8_t scratch[4096];
    while (left > 0) {
        uint32_t chunk = left > sizeof(scratch) ? (uint32_t)sizeof(scratch) : left;
        if (read_all(fd, scratch, chunk) != 0) return -1;
        left -= chunk;
    }
    return 0;
}

int main(void) {
    printf("=== test_rpc_auth ===\n");
    rm_rf(TDIR);

    /* Secret set BEFORE the server starts — this is the "require auth" mode. */
    setenv("TSDB_RPC_SECRET", SECRET, 1);
    unsetenv("TSDB_RPC_TLS");   /* plaintext + authenticated: the vuln case */

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
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

    /* ── [1] unauthenticated opcode is refused (the exfil the fix closes) ── */
    printf("\n[1] a raw peer that skips the handshake cannot CATALOG_DUMP\n");
    {
        int fd = raw_connect(port);
        CHECK(fd >= 0, "raw connect to the RPC port");
        /* Skip the handshake entirely; go straight for the catalog. */
        CHECK(raw_send_op(fd, TSDB_RPC_CATALOG_DUMP, 1) == 0,
              "sent CATALOG_DUMP with no credentials");
        int type = -1; uint32_t plen = 0;
        int got = raw_read_frame(fd, &type, &plen);
        /* Fixed: first frame is AUTH_CHALLENGE (not an ACK), or the peer is
         * simply closed.  Pre-fix: first frame is an ACK carrying the catalog. */
        printf("  first server frame: got=%d type=%d plen=%u\n", got, type, plen);
        CHECK(!(got == 0 && type == TSDB_RPC_ACK),
              "unauthenticated CATALOG_DUMP is NOT answered with an ACK "
              "(no catalog exfiltration)");
        if (fd >= 0) close(fd);
    }

    /* ── [2] an authenticated peer still works end to end ────────────────── */
    printf("\n[2] tsdb_rpc_connect completes the handshake; RPC works\n");
    {
        char addr[64];
        snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
        tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
        CHECK(conn != NULL, "authenticated dial succeeds");
        if (conn) {
            CHECK(tsdb_rpc_call(conn, TSDB_RPC_HEARTBEAT, NULL, 0) == TSDB_OK,
                  "HEARTBEAT over an authenticated conn returns OK");
            uint8_t buf[64 * 1024];
            uint32_t rlen = 0;
            int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_CATALOG_DUMP, NULL, 0,
                                        buf, sizeof(buf), &rlen);
            printf("  CATALOG_DUMP rc=%d bytes=%u\n", rc, rlen);
            CHECK(rc == TSDB_OK, "authenticated CATALOG_DUMP returns OK");
            tsdb_rpc_conn_close(conn);
        }
    }

    /* ── [3] a wrong tag is refused (HMAC verification, not just presence) ── */
    printf("\n[3] a raw peer that answers with a wrong tag is refused\n");
    {
        int fd = raw_connect(port);
        CHECK(fd >= 0, "raw connect to the RPC port");
        int type = -1; uint32_t plen = 0;
        int got = raw_read_frame(fd, &type, &plen);
        if (got == 0 && type == AUTH_CHALLENGE && plen == 32) {
            /* Answer with 32 zero bytes — a wrong HMAC. */
            uint8_t bad[32] = {0};
            raw_send_op_body(fd, AUTH_RESPONSE, 0, bad, sizeof(bad));
            /* Now try to read the catalog; the server must have refused. */
            raw_send_op(fd, TSDB_RPC_CATALOG_DUMP, 2);
            int t2 = -1; uint32_t p2 = 0;
            int g2 = raw_read_frame(fd, &t2, &p2);
            printf("  after wrong tag: got=%d type=%d plen=%u\n", g2, t2, p2);
            CHECK(!(g2 == 0 && t2 == TSDB_RPC_ACK && p2 > 0),
                  "wrong-tag peer gets no catalog ACK");
        } else {
            /* Pre-fix server never challenges; [1] already recorded the fail. */
            printf("  server did not issue a challenge (pre-fix build)\n");
            CHECK(0, "server issued an AUTH_CHALLENGE");
        }
        if (fd >= 0) close(fd);
    }

    tsdb_rpc_server_stop(srv);
    tsdb_close(db);
    rm_rf(TDIR);

    printf("\n%s\n", g_fail ? "FAILED" : "ALL PASS");
    return g_fail ? 1 : 0;
}
