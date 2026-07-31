/* test_rpc_resp_len_copied.c — *resp_len must be the bytes COPIED, not the wire
 * payload_len, and an oversized frame must raise a distinct truncation signal.
 *
 * THE BUG.  call_recv_locked set *resp_len = resp.payload_len (the frame's
 * declared length) but only memcpy'd min(payload_len, resp_cap) bytes into the
 * caller's buffer.  A caller that trusted *resp_len (catalog_sync's 32 MB
 * CATALOG_DUMP, fedrpc's result buffer, the fixed-size raft reply buffers)
 * then read past what was written — the uninitialised buffer tail parsed as
 * data.  And because the reported length capped silently at resp_cap once the
 * SOURCE was fixed, a caller that needed the WHOLE frame could not tell an
 * exact fit from a clamp.
 *
 * THE FIX under test: *resp_len is always the bytes copied (min(payload_len,
 * resp_cap)); tsdb_rpc_call_recv_ex additionally sets *truncated = 1 iff the
 * frame carried more than fit.  The socket is still fully drained either way,
 * so the connection stays usable.
 *
 * A real loopback peer speaks the TSRP frame format, so nothing here depends on
 * cluster state or timing.
 */
#include "../src/cluster/rpc.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static int g_fail = 0;
#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define REPLY_PLEN 100                 /* every ACK carries a 100-byte body */
static uint8_t g_payload[REPLY_PLEN];

static int read_full_fd(int fd, uint8_t *b, size_t n) {
    size_t done = 0;
    while (done < n) { ssize_t r = read(fd, b + done, n - done);
        if (r <= 0) return -1; done += (size_t)r; }
    return 0;
}
static int write_full_fd(int fd, const uint8_t *b, size_t n) {
    size_t done = 0;
    while (done < n) { ssize_t w = write(fd, b + done, n - done);
        if (w <= 0) return -1; done += (size_t)w; }
    return 0;
}

typedef struct { int listen_fd; int port; volatile int frames; } peer_t;

/* Read one TSRP frame header (+ drain its body); return req_id or -1. */
static int peer_read_frame(int fd, uint32_t *req_id) {
    uint8_t hdr[TSDB_RPC_HDR_SIZE];
    if (read_full_fd(fd, hdr, sizeof(hdr)) < 0) return -1;
    uint32_t magic; memcpy(&magic, hdr, 4);
    if (magic != TSDB_RPC_MAGIC) return -1;
    uint32_t rid;  memcpy(&rid,  hdr + 6,  4);
    uint32_t plen; memcpy(&plen, hdr + 10, 4);
    if (plen > 0) {
        uint8_t *body = malloc(plen);
        if (!body || read_full_fd(fd, body, plen) < 0) { free(body); return -1; }
        free(body);
    }
    *req_id = rid;
    return 0;
}

/* Reply ACK carrying the 100-byte payload, echoing the request's id. */
static int peer_send_ack_payload(int fd, uint32_t req_id) {
    uint8_t buf[TSDB_RPC_HDR_SIZE + REPLY_PLEN];
    int n = tsdb_rpc_encode(buf, sizeof(buf), TSDB_RPC_ACK, req_id,
                            g_payload, REPLY_PLEN);
    if (n <= 0) return -1;
    return write_full_fd(fd, buf, (size_t)n);
}

static void *peer_thread(void *arg) {
    peer_t *p = (peer_t *)arg;
    int fd = accept(p->listen_fd, NULL, NULL);
    if (fd < 0) return NULL;
    for (;;) {
        uint32_t rid = 0;
        if (peer_read_frame(fd, &rid) < 0) break;
        p->frames++;
        if (peer_send_ack_payload(fd, rid) < 0) break;
    }
    close(fd);
    return NULL;
}

static int peer_start(peer_t *p) {
    memset(p, 0, sizeof(*p));
    p->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (p->listen_fd < 0) return -1;
    int one = 1;
    setsockopt(p->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(p->listen_fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) return -1;
    if (listen(p->listen_fd, 4) < 0) return -1;
    struct sockaddr_in bound; socklen_t bl = sizeof(bound);
    getsockname(p->listen_fd, (struct sockaddr *)&bound, &bl);
    p->port = ntohs(bound.sin_port);
    return 0;
}

int main(void) {
    printf("=== test_rpc_resp_len_copied ===\n");
    for (int i = 0; i < REPLY_PLEN; i++) g_payload[i] = (uint8_t)(i * 7 + 1);

    peer_t peer;
    if (peer_start(&peer) != 0) { fprintf(stderr, "peer start failed\n"); return 1; }
    pthread_t tid;
    if (pthread_create(&tid, NULL, peer_thread, &peer) != 0) { return 1; }

    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", peer.port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    if (!conn) { fprintf(stderr, "connect failed\n"); return 1; }

    uint8_t req[4] = { 1, 2, 3, 4 };

    /* [1] resp_cap SMALLER than the frame: report the copied length, flag the
     *     truncation, and never touch the buffer tail past what was written. */
    printf("\n[1] a frame bigger than the buffer reports COPIED length + truncated\n");
    {
        uint8_t buf[8];
        memset(buf, 0xEE, sizeof(buf));   /* sentinel: nothing beyond copy is set */
        uint32_t rlen = 0; int trunc = -1;
        int rc = tsdb_rpc_call_recv_ex(conn, TSDB_RPC_WRITE_BATCH,
                                       req, sizeof(req),
                                       buf, sizeof(buf), &rlen, &trunc);
        CHECK(rc == TSDB_OK, "call succeeded (ACK)");
        CHECK(rlen == sizeof(buf),
              "*resp_len is the 8 bytes COPIED, not the 100-byte wire length");
        CHECK(trunc == 1, "the truncation signal is raised for the short buffer");
        CHECK(memcmp(buf, g_payload, sizeof(buf)) == 0,
              "the 8 copied bytes are the frame's first 8 bytes (no corruption)");
    }

    /* [2] resp_cap LARGER than the frame: full length, no false truncation. */
    printf("\n[2] a frame that fits reports the full length and no truncation\n");
    {
        uint8_t buf[128];
        memset(buf, 0xEE, sizeof(buf));
        uint32_t rlen = 0; int trunc = -1;
        int rc = tsdb_rpc_call_recv_ex(conn, TSDB_RPC_WRITE_BATCH,
                                       req, sizeof(req),
                                       buf, sizeof(buf), &rlen, &trunc);
        CHECK(rc == TSDB_OK, "call succeeded");
        CHECK(rlen == REPLY_PLEN, "*resp_len is the full 100 bytes");
        CHECK(trunc == 0, "no false truncation when the frame fits exactly");
        CHECK(memcmp(buf, g_payload, REPLY_PLEN) == 0, "all 100 bytes intact");
        CHECK(buf[REPLY_PLEN] == 0xEE, "the byte past the frame is untouched");
    }

    /* [3] the plain (non-_ex) entry point is now safe too — its *resp_len is the
     *     copied length, so an unaudited caller no longer over-reads. */
    printf("\n[3] the plain tsdb_rpc_call_recv also reports the copied length\n");
    {
        uint8_t buf[8];
        uint32_t rlen = 999;
        int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_WRITE_BATCH,
                                    req, sizeof(req), buf, sizeof(buf), &rlen);
        CHECK(rc == TSDB_OK, "call succeeded");
        CHECK(rlen == sizeof(buf),
              "*resp_len clamps to the buffer — no caller reads past it");
    }

    /* [4] the connection is still in lock-step: the socket was fully drained
     *     each time regardless of resp_cap, so a further call still pairs. */
    printf("\n[4] truncation does not desynchronise the stream\n");
    {
        uint8_t buf[128]; uint32_t rlen = 0; int trunc = -1;
        int rc = tsdb_rpc_call_recv_ex(conn, TSDB_RPC_WRITE_BATCH,
                                       req, sizeof(req),
                                       buf, sizeof(buf), &rlen, &trunc);
        CHECK(rc == TSDB_OK && rlen == REPLY_PLEN,
              "a 4th call still gets a clean, fully-paired reply");
    }

    tsdb_rpc_conn_close(conn);
    pthread_join(tid, NULL);
    close(peer.listen_fd);

    printf("\n=== test_rpc_resp_len_copied: %s ===\n", g_fail ? "FAILED" : "ALL PASS");
    return g_fail ? 1 : 0;
}
