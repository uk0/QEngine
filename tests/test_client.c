/* test_client.c — protocol round-trip tests with a mini mock server.
 *
 * Spawns a POSIX thread that runs an in-process mock server, then drives
 * the client framing code through 5+ message types:
 *
 *   1. HELLO / HELLO_OK
 *   2. QUERY  / QUERY_RESULT_HDR + QUERY_RESULT_ROWS (FIN)
 *   3. WRITE_BATCH / WRITE_ACK
 *   4. LIST_GROUPS / (text response)
 *   5. CLUSTER_STATS / (kv response)
 *   6. WRITE_STREAM_OPEN + WRITE_STREAM_DATA + WRITE_STREAM_END / WRITE_ACK
 *
 * Verifies:
 *   - MAGIC present and correct
 *   - CRC32C valid on every frame
 *   - Response types match expectations
 */

#define _POSIX_C_SOURCE 200809L
/* INADDR_LOOPBACK and friends require BSD definitions on macOS */
#if defined(__APPLE__)
#  define _DARWIN_C_SOURCE
#endif

#include "../cli/tsdb_wire.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <assert.h>
#include <time.h>

/* ─── Test helpers ─────────────────────────────────────────────────────────── */
static int g_tests_run    = 0;
static int g_tests_passed = 0;

#define ASSERT(cond, msg) do { \
    g_tests_run++; \
    if (cond) { g_tests_passed++; printf("  [PASS] %s\n", msg); } \
    else { printf("  [FAIL] %s  (line %d)\n", msg, __LINE__); } \
} while(0)

/* ─── Mini mock server state ───────────────────────────────────────────────── */

typedef struct {
    int listen_fd;
    int port;
} mock_ctx_t;

/*
 * Build and send a frame from the mock server side.
 * Reuses frame_send/frame_recv from tsdb_wire.c.
 */
static int mock_send(tsdb_conn_t *c, uint8_t type, uint16_t flags,
                     uint64_t req_id, const uint8_t *payload, uint32_t plen) {
    return frame_send(c, type, flags, req_id, payload, plen);
}

/* ─── Individual mock handlers ─────────────────────────────────────────────── */

static void handle_hello(tsdb_conn_t *c, const tsdb_msg_t *req) {
    /* HELLO_OK: [server_ver_len u8][server_ver utf8] */
    const char *sv = "tsdb-server v0.1-mock";
    uint8_t svl = (uint8_t)strlen(sv);
    uint8_t buf[64];
    buf[0] = svl;
    memcpy(buf + 1, sv, svl);
    mock_send(c, MSG_HELLO_OK, 0, req->hdr.req_id, buf, 1 + svl);
}

static void handle_query(tsdb_conn_t *c, const tsdb_msg_t *req) {
    /* Return a 2-column (ts:TIMESTAMP, price:FLOAT64) result with 2 rows */

    /* HDR */
    uint8_t hdr_buf[32];
    uint8_t *p = hdr_buf;
    put_u16(p, 2); p += 2;                      /* ncols */
    /* col 0: ts, TIMESTAMP */
    const char *cn0 = "ts";
    *p++ = (uint8_t)strlen(cn0);
    memcpy(p, cn0, strlen(cn0)); p += strlen(cn0);
    *p++ = TSDB_TYPE_TIMESTAMP;
    /* col 1: price, FLOAT64 */
    const char *cn1 = "price";
    *p++ = (uint8_t)strlen(cn1);
    memcpy(p, cn1, strlen(cn1)); p += strlen(cn1);
    *p++ = TSDB_TYPE_FLOAT64;
    mock_send(c, MSG_QUERY_RESULT_HDR, 0, req->hdr.req_id, hdr_buf, (uint32_t)(p - hdr_buf));

    /* ROWS: this MUST match the production server's emit_query_chunk()
     * (src/server/server.c).  Layout:
     *   [nrows u32][ncols u16]
     *   per column, by ColType (the reader already has types from the HDR):
     *     numeric: nrows * 8 bytes (LE, 8-byte stride)
     *     SYMBOL : [blocklen u32] then nrows * ([len u16][bytes])
     * No table name, no per-column name/type/codec/csz inside the chunk —
     * those live only in the HDR.  A drift here (or in decode_query_rows)
     * regresses the empty-result bug fixed in this change. */
    uint8_t rows_buf[256];
    p = rows_buf;
    put_u32(p, 2); p += 4;    /* nrows */
    put_u16(p, 2); p += 2;    /* ncols */

    /* col 0: ts — 2 timestamps, raw 8-byte stride */
    int64_t ts0 = 1735689600000000000LL; /* 2026-01-01 */
    int64_t ts1 = ts0 + 1000000000LL;
    put_u64(p, (uint64_t)ts0); p += 8;
    put_u64(p, (uint64_t)ts1); p += 8;

    /* col 1: price — 2 floats, raw 8-byte stride */
    double f0 = 100.5, f1 = 101.0;
    uint64_t raw0, raw1;
    memcpy(&raw0, &f0, 8); memcpy(&raw1, &f1, 8);
    put_u64(p, raw0); p += 8;
    put_u64(p, raw1); p += 8;

    mock_send(c, MSG_QUERY_RESULT_ROWS, TSDB_FLAG_FIN, req->hdr.req_id,
              rows_buf, (uint32_t)(p - rows_buf));
}

static void handle_write_batch(tsdb_conn_t *c, const tsdb_msg_t *req) {
    /* Parse payload to extract nrows */
    const uint8_t *p = req->payload;
    const uint8_t *end = p + req->hdr.payload_len;
    uint32_t nrows = 0;
    if (p < end) {
        uint8_t tl = *p++;
        p += tl;
        if (p + 6 <= end) {
            /* ncols */ get_u16(p); p += 2;
            nrows = get_u32(p);
        }
    }
    /* WRITE_ACK: [rows_accepted u32] (canonical server shape) */
    uint8_t ack[4];
    put_u32(ack, nrows);
    mock_send(c, MSG_WRITE_ACK, 0, req->hdr.req_id, ack, 4);
}

static void handle_list_groups(tsdb_conn_t *c, const tsdb_msg_t *req) {
    const char *body = "factory_a\nfactory_b\n";
    mock_send(c, MSG_SCHEMA, 0, req->hdr.req_id,
              (const uint8_t*)body, (uint32_t)strlen(body));
}

static void handle_cluster_stats(tsdb_conn_t *c, const tsdb_msg_t *req) {
    /* [kv_count u16] [k_len u8][k utf8][v_len u8][v utf8] ... */
    uint8_t buf[256];
    uint8_t *p = buf;
    /* 2 kv pairs */
    put_u16(p, 2); p += 2;
    const char *k1 = "nodes", *v1 = "1";
    *p++ = (uint8_t)strlen(k1); memcpy(p, k1, strlen(k1)); p += strlen(k1);
    *p++ = (uint8_t)strlen(v1); memcpy(p, v1, strlen(v1)); p += strlen(v1);
    const char *k2 = "tables", *v2 = "3";
    *p++ = (uint8_t)strlen(k2); memcpy(p, k2, strlen(k2)); p += strlen(k2);
    *p++ = (uint8_t)strlen(v2); memcpy(p, v2, strlen(v2)); p += strlen(v2);
    mock_send(c, MSG_HELLO_OK, 0, req->hdr.req_id, buf, (uint32_t)(p - buf));
}

static void handle_write_stream_open(tsdb_conn_t *c, const tsdb_msg_t *req) {
    /* No ack for OPEN; just record and proceed */
    (void)c; (void)req;
    /* In a real server we'd start a stream session */
}

static void handle_write_stream_data(tsdb_conn_t *c, const tsdb_msg_t *req) {
    /* Accumulate silently; no per-chunk ack */
    (void)c; (void)req;
}

static void handle_write_stream_end(tsdb_conn_t *c, const tsdb_msg_t *req) {
    uint8_t ack[4];
    put_u32(ack, 42);  /* rows_accepted */
    mock_send(c, MSG_WRITE_ACK, 0, req->hdr.req_id, ack, 4);
}

/* ─── Mock server connection handler ───────────────────────────────────────── */

static void mock_handle_conn(int fd) {
    tsdb_conn_t c = {0};
    c.fd = fd;
    c.timeout_ms = 5000;
    c.next_req_id = 100; /* server-side counter unused but needed for struct */

    for (;;) {
        tsdb_msg_t msg = {0};
        int rc = frame_recv(&c, &msg);
        if (rc < 0) break; /* connection closed or error */

        switch (msg.hdr.type) {
        case MSG_HELLO:             handle_hello(&c, &msg);               break;
        case MSG_QUERY:             handle_query(&c, &msg);               break;
        case MSG_WRITE_BATCH:       handle_write_batch(&c, &msg);         break;
        case MSG_LIST_GROUPS:       handle_list_groups(&c, &msg);         break;
        case MSG_CLUSTER_STATS:     handle_cluster_stats(&c, &msg);       break;
        case MSG_WRITE_STREAM_OPEN: handle_write_stream_open(&c, &msg);   break;
        case MSG_WRITE_STREAM_DATA: handle_write_stream_data(&c, &msg);   break;
        case MSG_WRITE_STREAM_END:  handle_write_stream_end(&c, &msg);    break;
        default:
            /* Send ERROR for unknown types */
            {
                const char *errmsg = "unknown msg";
                uint16_t msglen = (uint16_t)strlen(errmsg);
                uint8_t ebuf[64];
                put_u32(ebuf, (uint32_t)-1);
                put_u16(ebuf + 4, msglen);
                memcpy(ebuf + 6, errmsg, msglen);
                mock_send(&c, MSG_ERROR, 0, msg.hdr.req_id, ebuf, 6 + msglen);
            }
            break;
        }
        msg_free(&msg);
    }
    close(fd);
}

static void *mock_server_thread(void *arg) {
    mock_ctx_t *ctx = (mock_ctx_t *)arg;
    /* Accept exactly one connection (our test) */
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    int fd = accept(ctx->listen_fd, (struct sockaddr *)&addr, &addrlen);
    if (fd < 0) return NULL;
    mock_handle_conn(fd);
    return NULL;
}

/* ─── Mock server startup ───────────────────────────────────────────────────── */

static int start_mock_server(mock_ctx_t *ctx) {
    ctx->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->listen_fd < 0) return -1;

    int one = 1;
    setsockopt(ctx->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0; /* OS picks port */

    if (bind(ctx->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        return -1;
    if (listen(ctx->listen_fd, 1) < 0) return -1;

    /* Get assigned port */
    socklen_t sl = sizeof(addr);
    getsockname(ctx->listen_fd, (struct sockaddr *)&addr, &sl);
    ctx->port = ntohs(addr.sin_port);
    return 0;
}

/* ─── Client-side test helpers ──────────────────────────────────────────────── */

static tsdb_conn_t g_conn;

static int client_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons((uint16_t)port);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd); return -1;
    }
    memset(&g_conn, 0, sizeof(g_conn));
    g_conn.fd = fd;
    g_conn.timeout_ms = 5000;
    g_conn.next_req_id = 1;
    return 0;
}

/* ─── CRC self-test ─────────────────────────────────────────────────────────── */

static void test_crc32c(void) {
    printf("\n=== CRC32C self-test ===\n");
    /* Known CRC32C values */
    /* crc32c("") = 0x00000000 */
    uint32_t c0 = crc32c(0, NULL, 0);
    ASSERT(c0 == 0x00000000u, "crc32c('') == 0x00000000");

    /* crc32c("123456789") = 0xE3069283 */
    const uint8_t *s = (const uint8_t*)"123456789";
    uint32_t c1 = crc32c(0, s, 9);
    ASSERT(c1 == 0xE3069283u, "crc32c('123456789') == 0xE3069283");

    /* Incremental should equal one-shot */
    uint32_t ca = crc32c(0, s, 4);
    uint32_t cb = crc32c(ca, s + 4, 5);
    ASSERT(cb == c1, "incremental crc32c matches one-shot");
}

/* ─── Frame encode/decode self-test (no network) ────────────────────────────── */

static void test_frame_loopback(void) {
    printf("\n=== Frame loopback (socketpair) ===\n");
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        printf("  [SKIP] socketpair not available\n");
        return;
    }

    tsdb_conn_t ca = {.fd = sv[0], .timeout_ms = 2000, .next_req_id = 1};
    tsdb_conn_t cb = {.fd = sv[1], .timeout_ms = 2000, .next_req_id = 1};

    const uint8_t payload[] = {0x01, 0x02, 0x03, 0xAB, 0xCD};
    uint64_t req_id = 42;

    int r = frame_send(&ca, MSG_QUERY, 0, req_id, payload, sizeof(payload));
    ASSERT(r == 0, "frame_send returns 0");

    tsdb_msg_t msg = {0};
    r = frame_recv(&cb, &msg);
    ASSERT(r == 0, "frame_recv returns 0");
    ASSERT(msg.hdr.magic == TSDB_WIRE_MAGIC, "magic correct");
    ASSERT(msg.hdr.ver == TSDB_WIRE_VER, "ver correct");
    ASSERT(msg.hdr.type == MSG_QUERY, "type matches");
    ASSERT(msg.hdr.req_id == req_id, "req_id echoed");
    ASSERT(msg.hdr.payload_len == sizeof(payload), "payload_len matches");
    ASSERT(memcmp(msg.payload, payload, sizeof(payload)) == 0, "payload bytes match");
    msg_free(&msg);

    close(sv[0]); close(sv[1]);
}

/* ─── CRC tamper test ────────────────────────────────────────────────────────── */

static void test_crc_tamper(void) {
    printf("\n=== CRC tamper detection ===\n");
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) { return; }

    tsdb_conn_t ca = {.fd = sv[0], .timeout_ms = 2000, .next_req_id = 1};
    /* sv[1] used for raw read only — no tsdb_conn_t needed */

    /* Send a valid frame */
    const uint8_t payload[] = {0xDE, 0xAD};
    frame_send(&ca, MSG_PING, 0, 1, payload, 2);

    /* Intercept the raw bytes: read (HDR + payload + CRC) and flip a bit */
    uint8_t raw[TSDB_WIRE_HDR_SIZE + 2 + 4];
    ssize_t n = read(sv[1], raw, sizeof(raw));
    ASSERT(n == (ssize_t)sizeof(raw), "raw read correct size");

    /* Flip a bit in the payload area */
    raw[TSDB_WIRE_HDR_SIZE] ^= 0xFF;

    /* Write the corrupted frame to a new pair */
    int sv2[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, sv2);
    tsdb_conn_t cc = {.fd = sv2[1], .timeout_ms = 2000, .next_req_id = 1};
    write(sv2[0], raw, sizeof(raw));
    close(sv2[0]);

    tsdb_msg_t msg = {0};
    int rc = frame_recv(&cc, &msg);
    ASSERT(rc == -2, "CRC mismatch detected (rc=-2)");
    msg_free(&msg);

    close(sv[0]); close(sv[1]);
    close(sv2[1]);
}

/* ─── End-to-end mock server tests ─────────────────────────────────────────── */

static void test_hello(void) {
    printf("\n--- test HELLO / HELLO_OK ---\n");
    uint8_t buf[64];
    uint8_t *p = buf;
    *p++ = TSDB_WIRE_VER;
    const char *cid = "test-client";
    *p++ = (uint8_t)strlen(cid);
    memcpy(p, cid, strlen(cid)); p += strlen(cid);
    *p++ = 0; /* no token */

    tsdb_msg_t resp = {0};
    int rc = send_recv(&g_conn, MSG_HELLO, 0, buf, (uint32_t)(p - buf),
                        MSG_HELLO_OK, &resp);
    ASSERT(rc == 0, "HELLO→HELLO_OK success");
    ASSERT(resp.hdr.type == MSG_HELLO_OK, "response type is HELLO_OK");
    ASSERT(resp.hdr.payload_len >= 1, "HELLO_OK has payload");
    /* Check server version string */
    if (rc == 0 && resp.hdr.payload_len >= 1) {
        uint8_t svl = resp.payload[0];
        ASSERT(svl > 0, "server_ver_len > 0");
        ASSERT(svl + 1 <= resp.hdr.payload_len, "version string within payload");
    }
    msg_free(&resp);
}

static void test_query(void) {
    printf("\n--- test QUERY / QUERY_RESULT_HDR + ROWS ---\n");
    const char *qtl = "SELECT ts, price FROM trades";
    uint8_t buf[256];
    put_u16(buf, (uint16_t)strlen(qtl));
    memcpy(buf + 2, qtl, strlen(qtl));

    uint64_t req_id = g_conn.next_req_id++;
    int rc = frame_send(&g_conn, MSG_QUERY, 0, req_id, buf, (uint32_t)(2 + strlen(qtl)));
    ASSERT(rc == 0, "QUERY frame sent");

    /* Expect HDR then ROWS(FIN) */
    tsdb_msg_t hdr_msg = {0}, rows_msg = {0};
    rc = frame_recv(&g_conn, &hdr_msg);
    ASSERT(rc == 0, "QUERY_RESULT_HDR received");
    ASSERT(hdr_msg.hdr.type == MSG_QUERY_RESULT_HDR, "type is QUERY_RESULT_HDR");
    /* Decode ncols */
    if (rc == 0 && hdr_msg.hdr.payload_len >= 2) {
        uint16_t nc = get_u16(hdr_msg.payload);
        ASSERT(nc == 2, "ncols == 2");
    }
    msg_free(&hdr_msg);

    rc = frame_recv(&g_conn, &rows_msg);
    ASSERT(rc == 0, "QUERY_RESULT_ROWS received");
    ASSERT(rows_msg.hdr.type == MSG_QUERY_RESULT_ROWS, "type is QUERY_RESULT_ROWS");
    ASSERT((rows_msg.hdr.flags & TSDB_FLAG_FIN) != 0, "FIN flag set on last rows frame");

    /* Decode the chunk exactly as the production client (decode_query_rows)
     * does and verify the row VALUES round-trip.  The previous client used a
     * [tlen][table][ncols u16][nrows u32]+per-col-meta layout that did not
     * match emit_query_chunk's [nrows u32][ncols u16]+raw layout, so a count(*)
     * over real data decoded to zero rows on cluster nodes.  These value
     * asserts are the regression guard. */
    {
        const uint8_t *rp  = rows_msg.payload;
        const uint8_t *rend = rp + rows_msg.hdr.payload_len;
        ASSERT(rp + 6 <= rend, "rows chunk has nrows+ncols header");
        uint32_t nrows = get_u32(rp); rp += 4;
        uint16_t ncols = get_u16(rp); rp += 2;
        ASSERT(nrows == 2, "decoded nrows == 2");
        ASSERT(ncols == 2, "decoded ncols == 2");
        /* col 0: TIMESTAMP, 2*8 raw */
        const uint8_t *ts_col = rp; rp += (size_t)nrows * 8;
        /* col 1: FLOAT64, 2*8 raw */
        const uint8_t *px_col = rp; rp += (size_t)nrows * 8;
        ASSERT(rp == rend, "rows chunk fully consumed");
        int64_t got_ts0 = (int64_t)get_u64(ts_col);
        int64_t got_ts1 = (int64_t)get_u64(ts_col + 8);
        ASSERT(got_ts0 == 1735689600000000000LL, "row0 ts matches");
        ASSERT(got_ts1 == 1735689600000000000LL + 1000000000LL, "row1 ts matches");
        double got_px0, got_px1;
        uint64_t b0 = get_u64(px_col), b1 = get_u64(px_col + 8);
        memcpy(&got_px0, &b0, 8); memcpy(&got_px1, &b1, 8);
        ASSERT(got_px0 == 100.5, "row0 price matches");
        ASSERT(got_px1 == 101.0, "row1 price matches");
    }
    msg_free(&rows_msg);
}

static void test_write_batch(void) {
    printf("\n--- test WRITE_BATCH / WRITE_ACK ---\n");
    /* Build a minimal columnar payload: 1 col, 2 rows, RAW codec */
    uint8_t buf[256];
    uint8_t *p = buf;
    const char *tbl = "prices";
    *p++ = (uint8_t)strlen(tbl); memcpy(p, tbl, strlen(tbl)); p += strlen(tbl);
    put_u16(p, 1); p += 2;  /* ncols */
    put_u32(p, 2); p += 4;  /* nrows */
    /* col: val, FLOAT64, RAW */
    const char *cn = "val";
    *p++ = (uint8_t)strlen(cn); memcpy(p, cn, strlen(cn)); p += strlen(cn);
    *p++ = TSDB_TYPE_FLOAT64;
    *p++ = TSDB_CODEC_RAW;
    put_u32(p, 16); p += 4;  /* 2 * 8 */
    double v0 = 1.0, v1 = 2.0;
    uint64_t r0, r1;
    memcpy(&r0, &v0, 8); memcpy(&r1, &v1, 8);
    put_u64(p, r0); p += 8;
    put_u64(p, r1); p += 8;

    tsdb_msg_t resp = {0};
    int rc = send_recv(&g_conn, MSG_WRITE_BATCH, 0, buf, (uint32_t)(p - buf),
                        MSG_WRITE_ACK, &resp);
    ASSERT(rc == 0, "WRITE_BATCH→WRITE_ACK success");
    if (rc == 0 && resp.hdr.payload_len >= 4) {
        uint32_t rows_acc = get_u32(resp.payload);
        ASSERT(rows_acc == 2, "server accepted 2 rows");
    }
    msg_free(&resp);
}

static void test_list_groups(void) {
    printf("\n--- test LIST_GROUPS ---\n");
    uint8_t buf[1] = {0}; /* empty glob */
    tsdb_msg_t resp = {0};
    int rc = send_recv(&g_conn, MSG_LIST_GROUPS, 0, buf, 1, 0, &resp);
    ASSERT(rc == 0, "LIST_GROUPS response received");
    ASSERT(resp.hdr.payload_len > 0, "LIST_GROUPS payload non-empty");
    msg_free(&resp);
}

static void test_cluster_stats(void) {
    printf("\n--- test CLUSTER_STATS ---\n");
    tsdb_msg_t resp = {0};
    int rc = send_recv(&g_conn, MSG_CLUSTER_STATS, 0, NULL, 0, 0, &resp);
    ASSERT(rc == 0, "CLUSTER_STATS response received");
    if (rc == 0 && resp.hdr.payload_len >= 2) {
        uint16_t nkv = get_u16(resp.payload);
        ASSERT(nkv == 2, "cluster stats has 2 kv pairs");
    }
    msg_free(&resp);
}

static void test_write_stream(void) {
    printf("\n--- test WRITE_STREAM (OPEN+DATA+END) ---\n");

    /* WRITE_STREAM_OPEN */
    const char *tbl = "readings";
    uint8_t obuf[64];
    obuf[0] = (uint8_t)strlen(tbl);
    memcpy(obuf + 1, tbl, strlen(tbl));
    uint64_t open_id = g_conn.next_req_id++;
    int rc = frame_send(&g_conn, MSG_WRITE_STREAM_OPEN, 0, open_id, obuf, 1 + (uint32_t)strlen(tbl));
    ASSERT(rc == 0, "WRITE_STREAM_OPEN sent");

    /* WRITE_STREAM_DATA: 1 col, 3 rows */
    uint8_t dbuf[256];
    uint8_t *p = dbuf;
    *p++ = (uint8_t)strlen(tbl); memcpy(p, tbl, strlen(tbl)); p += strlen(tbl);
    put_u16(p, 1); p += 2;
    put_u32(p, 3); p += 4;
    const char *cn = "temp";
    *p++ = (uint8_t)strlen(cn); memcpy(p, cn, strlen(cn)); p += strlen(cn);
    *p++ = TSDB_TYPE_FLOAT64; *p++ = TSDB_CODEC_RAW;
    put_u32(p, 24); p += 4;
    double vs[3] = {20.0, 21.5, 22.0};
    for (int i = 0; i < 3; i++) {
        uint64_t rv; memcpy(&rv, &vs[i], 8); put_u64(p, rv); p += 8;
    }
    uint64_t data_id = g_conn.next_req_id++;
    rc = frame_send(&g_conn, MSG_WRITE_STREAM_DATA, 0, data_id, dbuf, (uint32_t)(p - dbuf));
    ASSERT(rc == 0, "WRITE_STREAM_DATA sent");

    /* WRITE_STREAM_END + expect WRITE_ACK */
    tsdb_msg_t resp = {0};
    uint64_t end_id = g_conn.next_req_id++;
    rc = frame_send(&g_conn, MSG_WRITE_STREAM_END, TSDB_FLAG_FIN, end_id, NULL, 0);
    ASSERT(rc == 0, "WRITE_STREAM_END sent");
    rc = frame_recv(&g_conn, &resp);
    ASSERT(rc == 0, "WRITE_ACK received");
    ASSERT(resp.hdr.type == MSG_WRITE_ACK, "type is WRITE_ACK");
    if (rc == 0 && resp.hdr.payload_len >= 4) {
        uint32_t rows_acc = get_u32(resp.payload);
        ASSERT(rows_acc == 42, "mock server acked 42 rows");
    }
    msg_free(&resp);
}

/* ─── main ──────────────────────────────────────────────────────────────────── */

int main(void) {
    printf("=== tsdb wire protocol tests ===\n");

    /* ── Pure unit tests (no network) ── */
    test_crc32c();
    test_frame_loopback();
    test_crc_tamper();

    /* ── Mock server tests ── */
    mock_ctx_t ctx = {0};
    if (start_mock_server(&ctx) < 0) {
        perror("start_mock_server");
        return 1;
    }

    /* Start mock server thread */
    pthread_t tid;
    pthread_create(&tid, NULL, mock_server_thread, &ctx);

    /* Give the server a moment to enter accept() */
    struct timespec ts = {0, 10000000}; /* 10ms */
    nanosleep(&ts, NULL);

    /* Connect client */
    if (client_connect(ctx.port) < 0) {
        perror("client_connect");
        return 1;
    }
    printf("\n=== Mock server tests (port %d) ===\n", ctx.port);

    /* Run all 5+ message type tests */
    test_hello();
    test_query();
    test_write_batch();
    test_list_groups();
    test_cluster_stats();
    test_write_stream();

    close(g_conn.fd);
    close(ctx.listen_fd);
    pthread_join(tid, NULL);

    /* ── Summary ── */
    printf("\n=== Results: %d/%d passed ===\n", g_tests_passed, g_tests_run);
    return g_tests_passed == g_tests_run ? 0 : 1;
}
