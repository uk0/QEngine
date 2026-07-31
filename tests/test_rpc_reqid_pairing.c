/* test_rpc_reqid_pairing.c — a response must answer the request it carries the
 * id of.
 *
 * rpc.h documents req_id as "monotonic request ID for request/response
 * matching".  tsdb_rpc_encode writes it, tsdb_rpc_decode parses it into
 * tsdb_rpc_msg_t.req_id — and call_recv_locked never looked at it.  The client
 * took whatever frame arrived next as the answer to whatever it had just sent.
 *
 * That is only a documentation gap while the stream stays in lock-step.  It
 * stops being one the moment a call abandons a response mid-flight, which the
 * replication path does by design: tsdb_rpc_call_recv_to arms SO_RCVTIMEO
 * (TSDB_REPL_SEND_TIMEOUT_MS = 10 s on the fanout and raw-block senders), and a
 * peer that takes longer than that to apply one batch leaves its ACK sitting in
 * the socket.  The request was fully written; the peer WILL answer it.  With
 * TSDB_REPLICA_CONNS_PER_PEER=1 — the live cluster's setting — every fanout
 * worker for that peer serialises on that one socket, so the next worker in
 * line sends ITS batch and reads the previous batch's reply.  From then on the
 * connection is permanently off by one:
 *
 *      client                              peer
 *      -- WRITE_BATCH #1 (req 1) ------->  applying, slow
 *      (10 s deadline fires, ERR_IO)
 *                                <-------  ACK req 1
 *      -- WRITE_BATCH #2 (req 2) ------->  fails to apply
 *      <-- reads ACK req 1 as its own      ERR req 2
 *
 * Batch #2 is counted as replicated (qengine_replicate_ack_total++, ack_count++)
 * against a reply that says nothing about it — and the peer's real answer for
 * #2 was ERR.  Acked and not landed, with no error anywhere.  Under
 * TSDB_REPLICATION_QUORUM=0 the ack count is also what gates SKIP_LOCAL in
 * db_cluster.c, i.e. whether this node may drop its own copy of those rows.
 *
 * The test drives exactly that sequence against a fake peer that speaks the
 * TSRP frame format, so nothing here depends on cluster state or timing luck.
 *
 * [P1] the answer to a request must carry that request's id.
 * [P2] a connection that has already been desynchronised must not keep
 *      answering TSDB_OK — one abandoned response poisons it for good, so it
 *      has to fail closed rather than pair every later call off by one.
 */

#include "../src/cluster/rpc.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

/* ---- Fake peer ----------------------------------------------------------- */

/* Reads two frames.  Answers the first one LATE (after the client's deadline
 * has already fired) with an ACK, and the second one immediately with an ERR.
 * A correct client sees: call 1 timed out, call 2 answered ERR. */

typedef struct {
    int      listen_fd;
    int      port;
    int      slow_ms;        /* how long to sit on the first reply */
    volatile int frames_read;
    volatile int ready;
} peer_t;

static int read_full_fd(int fd, uint8_t *b, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, b + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

/* Read one TSRP frame; returns its req_id, or (uint32_t)-1 on EOF/error. */
static int peer_read_frame(int fd, uint32_t *out_req_id) {
    uint8_t hdr[TSDB_RPC_HDR_SIZE];
    if (read_full_fd(fd, hdr, sizeof(hdr)) < 0) return -1;
    uint32_t magic; memcpy(&magic, hdr, 4);
    if (magic != TSDB_RPC_MAGIC) return -1;
    uint32_t rid;  memcpy(&rid,  hdr + 6,  4);
    uint32_t plen; memcpy(&plen, hdr + 10, 4);
    if (plen > 0) {
        uint8_t *body = malloc(plen);
        if (!body) return -1;
        if (read_full_fd(fd, body, plen) < 0) { free(body); return -1; }
        free(body);
    }
    *out_req_id = rid;
    return 0;
}

static int peer_send_reply(int fd, tsdb_rpc_type_t type, uint32_t req_id) {
    uint8_t buf[TSDB_RPC_HDR_SIZE];
    int n = tsdb_rpc_encode(buf, sizeof(buf), type, req_id, NULL, 0);
    if (n <= 0) return -1;
    size_t done = 0;
    while (done < (size_t)n) {
        ssize_t w = write(fd, buf + done, (size_t)n - done);
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    return 0;
}

static void msleep(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static void *peer_thread(void *arg) {
    peer_t *p = (peer_t *)arg;
    int fd = accept(p->listen_fd, NULL, NULL);
    if (fd < 0) return NULL;

    uint32_t rid1 = 0, rid2 = 0;

    /* Frame 1: read it, then stall past the client's deadline before ACKing.
     * This is the peer that took longer than TSDB_REPL_SEND_TIMEOUT_MS to
     * apply a batch — the request landed, the answer is merely late. */
    if (peer_read_frame(fd, &rid1) < 0) { close(fd); return NULL; }
    p->frames_read++;
    msleep(p->slow_ms);
    peer_send_reply(fd, TSDB_RPC_ACK, rid1);

    /* Frame 2: answer it truthfully and at once — this batch FAILED here. */
    if (peer_read_frame(fd, &rid2) < 0) { close(fd); return NULL; }
    p->frames_read++;
    peer_send_reply(fd, TSDB_RPC_ERR, rid2);

    /* Frame 3 (if the client keeps going): also an error. */
    uint32_t rid3 = 0;
    if (peer_read_frame(fd, &rid3) == 0) {
        p->frames_read++;
        peer_send_reply(fd, TSDB_RPC_ERR, rid3);
    }

    /* Hold the socket open so the client's failures are protocol-level, not
     * "the peer hung up". */
    msleep(300);
    close(fd);
    return NULL;
}

static int peer_start(peer_t *p, int slow_ms) {
    memset(p, 0, sizeof(*p));
    p->slow_ms = slow_ms;
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

/* ---- [P1] / [P2] --------------------------------------------------------- */

static void test_late_reply_is_not_the_next_calls_answer(void) {
    peer_t peer;
    if (peer_start(&peer, /*slow_ms=*/700) != 0) {
        fprintf(stderr, "FAIL: could not start fake peer\n");
        g_fail++;
        return;
    }

    pthread_t tid;
    if (pthread_create(&tid, NULL, peer_thread, &peer) != 0) {
        fprintf(stderr, "FAIL: could not start peer thread\n");
        g_fail++;
        return;
    }

    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", peer.port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    if (!conn) {
        fprintf(stderr, "FAIL: could not connect to fake peer\n");
        g_fail++;
        pthread_join(tid, NULL);
        return;
    }

    uint8_t body[64];
    memset(body, 0xA5, sizeof(body));
    uint8_t ack[1]; uint32_t alen = 0;

    /* Call 1 — the slow batch.  Deadline well under the peer's stall, so this
     * abandons a reply that is still coming. */
    int rc1 = tsdb_rpc_call_recv_to(conn, TSDB_RPC_WRITE_BATCH,
                                    body, sizeof(body),
                                    ack, sizeof(ack), &alen, 200);
    CHECK(rc1 != TSDB_OK,
          "[P1] call 1 reports failure when its deadline fires");

    /* Let the peer's late ACK for call 1 land in our socket buffer. */
    msleep(900);

    /* Call 2 — a DIFFERENT batch, which this peer answers with ERR.  The reply
     * sitting in the buffer belongs to call 1.  Taking it is the defect. */
    int rc2 = tsdb_rpc_call_recv_to(conn, TSDB_RPC_WRITE_BATCH,
                                    body, sizeof(body),
                                    ack, sizeof(ack), &alen, 3000);
    CHECK(rc2 != TSDB_OK,
          "[P1] call 2 does NOT return OK off the previous call's ACK");

    /* Call 3 — nothing on this connection may report success again; the stream
     * offset can no longer be recovered from. */
    int rc3 = tsdb_rpc_call_recv_to(conn, TSDB_RPC_WRITE_BATCH,
                                    body, sizeof(body),
                                    ack, sizeof(ack), &alen, 3000);
    CHECK(rc3 != TSDB_OK,
          "[P2] a desynchronised connection keeps failing instead of "
          "pairing every later call off by one");

    tsdb_rpc_conn_close(conn);
    pthread_join(tid, NULL);
    close(peer.listen_fd);

    printf("      (peer read %d frames)\n", peer.frames_read);
}

/* A healthy in-order exchange must still work — the id check may not cost the
 * normal path anything. */
static void *echo_peer_thread(void *arg) {
    peer_t *p = (peer_t *)arg;
    int fd = accept(p->listen_fd, NULL, NULL);
    if (fd < 0) return NULL;
    for (;;) {
        uint32_t rid = 0;
        if (peer_read_frame(fd, &rid) < 0) break;
        p->frames_read++;
        if (peer_send_reply(fd, TSDB_RPC_ACK, rid) < 0) break;
    }
    close(fd);
    return NULL;
}

static void test_healthy_exchange_still_acks(void) {
    peer_t peer;
    if (peer_start(&peer, 0) != 0) { g_fail++; return; }

    pthread_t tid;
    if (pthread_create(&tid, NULL, echo_peer_thread, &peer) != 0) { g_fail++; return; }

    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", peer.port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    if (!conn) { g_fail++; pthread_join(tid, NULL); return; }

    uint8_t body[32];
    memset(body, 0x11, sizeof(body));
    uint8_t ack[1]; uint32_t alen = 0;

    int all_ok = 1;
    for (int i = 0; i < 16; i++) {
        int rc = tsdb_rpc_call_recv_to(conn, TSDB_RPC_WRITE_BATCH,
                                       body, sizeof(body),
                                       ack, sizeof(ack), &alen, 3000);
        if (rc != TSDB_OK) { all_ok = 0; break; }
    }
    CHECK(all_ok, "16 in-order round-trips all ACK (id check costs the "
                  "healthy path nothing)");

    /* And the plain (no-deadline) entry point too. */
    int rc = tsdb_rpc_call(conn, TSDB_RPC_HEARTBEAT, body, sizeof(body));
    CHECK(rc == TSDB_OK, "tsdb_rpc_call still ACKs on a healthy connection");

    tsdb_rpc_conn_close(conn);
    pthread_join(tid, NULL);
    close(peer.listen_fd);
}

int main(void) {
    printf("=== rpc req_id pairing ===\n");
    test_late_reply_is_not_the_next_calls_answer();
    test_healthy_exchange_still_acks();
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
