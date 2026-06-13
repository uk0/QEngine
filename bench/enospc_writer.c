/* enospc_writer.c — empirical DISK-FULL (ENOSPC) probe for the tsdb wire server.
 *
 * Connects, HELLO, creates a table via SQL (QUERY), then loops WRITE_BATCH
 * frames (8192 rows: ts + i64 + f64, RAW codec) until the server returns an
 * error.  On the first write error it:
 *   - records the last successfully-acked row count,
 *   - sends SELECT count(*) and reports what the server says is durable,
 *   - reports whether the server is still responsive (PING + a final query).
 *
 * Exit codes:
 *   0  = got a clean write error AND server stayed responsive AND count sane
 *   2  = server hung / connection died on write (no clean error)
 *   3  = server crashed / unresponsive after the error
 *   4  = setup failure (connect / hello / create table)
 *   5  = data integrity violation (post-ENOSPC count < acked, or query garbage)
 *
 * Build:  cc -O2 -o enospc_writer bench/enospc_writer.c cli/tsdb_wire.c -Iinclude -Icli
 */
#include "tsdb_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <time.h>

/* ---- minimal TCP connect (mirrors cli/tsdb_client.c tcp_connect) ---- */
static int tcp_connect(const char *host, int port, int timeout_ms) {
    char ports[16];
    snprintf(ports, sizeof(ports), "%d", port);
    struct addrinfo hints, *res = NULL, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, ports, &hints, &res) != 0) return -1;
    int fd = -1;
    for (rp = res; rp; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(fd); fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct timeval tv = { timeout_ms / 1000, (timeout_ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    return fd;
}

static int do_hello(tsdb_conn_t *c) {
    uint8_t buf[256]; uint8_t *p = buf;
    *p++ = TSDB_WIRE_VER;
    const char *cid = "enospc-probe";
    *p++ = (uint8_t)strlen(cid);
    memcpy(p, cid, strlen(cid)); p += strlen(cid);
    *p++ = 0; /* empty token */
    tsdb_msg_t resp = {0};
    int rc = send_recv(c, MSG_HELLO, 0, buf, (uint32_t)(p - buf), MSG_HELLO_OK, &resp);
    msg_free(&resp);
    return rc;
}

/* Send a SQL statement via QUERY. Returns 0 if frames came back without a
 * server ERROR, -1 on transport error / server ERROR. For count(*) we parse
 * the first row's first INT64 cell into *count_out (if non-NULL). */
static int do_query(tsdb_conn_t *c, const char *sql, long long *count_out) {
    size_t qlen = strlen(sql);
    uint8_t *buf = malloc(qlen + 2);
    if (!buf) return -1;
    put_u16(buf, (uint16_t)qlen);
    memcpy(buf + 2, sql, qlen);
    uint64_t req_id = ++c->next_req_id;
    int rc = frame_send(c, MSG_QUERY, 0, req_id, buf, (uint32_t)(qlen + 2));
    free(buf);
    if (rc != 0) return -1;

    int got_count = 0;
    for (int guard = 0; guard < 4096; guard++) {
        tsdb_msg_t msg = {0};
        if (frame_recv(c, &msg) != 0) { msg_free(&msg); return -1; }
        if (msg.hdr.type == MSG_ERROR) { msg_free(&msg); return -1; }
        if (msg.hdr.type == MSG_QUERY_RESULT_ROWS && count_out && !got_count) {
            /* QUERY_RESULT_ROWS chunk layout (server emit_query_chunk):
             *   [nrows u32][ncols u16] then raw fixed-stride column data
             *   (8 bytes/value for TS/INT64/FLOAT64).  The reader already
             *   has column types from QUERY_RESULT_HDR.  count(*) is a
             *   single INT64 column, 1 row -> first 8 bytes after the
             *   6-byte header. */
            const uint8_t *pp = msg.payload;
            const uint8_t *e  = msg.payload + msg.hdr.payload_len;
            if (pp + 6 <= e) {
                uint32_t nr; memcpy(&nr, pp, 4);
                uint16_t nc; memcpy(&nc, pp + 4, 2);
                if (nr >= 1 && nc >= 1 && pp + 6 + 8 <= e) {
                    int64_t v; memcpy(&v, pp + 6, 8);
                    *count_out = (long long)v;
                    got_count = 1;
                }
            }
        }
        int fin = (msg.hdr.flags & TSDB_FLAG_FIN) != 0;
        msg_free(&msg);
        if (fin) break;
    }
    return 0;
}

/* Build + send one WRITE_BATCH of `rows` rows.
 * Columns: ts(TIMESTAMP), v(INT64), f(FLOAT64), all RAW (8 bytes/value).
 * Returns: 0 on WRITE_ACK, 1 on server MSG_ERROR (clean), -1 on transport. */
static int do_write_batch(tsdb_conn_t *c, const char *table, int rows,
                          int64_t base_ts, int64_t base_val) {
    const char *cols[3] = { "ts", "v", "f" };
    uint8_t types[3] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_INT64, TSDB_TYPE_FLOAT64 };
    uint32_t tnlen = (uint32_t)strlen(table);
    size_t cap = 1 + tnlen + 2 + 4;
    for (int i = 0; i < 3; i++) cap += 1 + strlen(cols[i]) + 1 + 1 + 4 + (size_t)rows * 8;
    uint8_t *buf = malloc(cap);
    if (!buf) return -1;
    uint8_t *p = buf;
    *p++ = (uint8_t)tnlen;
    memcpy(p, table, tnlen); p += tnlen;
    put_u16(p, 3); p += 2;
    put_u32(p, (uint32_t)rows); p += 4;
    for (int ci = 0; ci < 3; ci++) {
        uint8_t cl = (uint8_t)strlen(cols[ci]);
        *p++ = cl; memcpy(p, cols[ci], cl); p += cl;
        *p++ = types[ci];
        *p++ = TSDB_CODEC_RAW;
        put_u32(p, (uint32_t)(rows * 8)); p += 4;
        for (int r = 0; r < rows; r++) {
            if (ci == 0)      put_u64(p, (uint64_t)(base_ts + r));
            else if (ci == 1) put_u64(p, (uint64_t)(base_val + r));
            else {
                /* High-entropy float bits so no codec can compress the
                 * column away — keeps the on-disk partition growing at
                 * ~8 bytes/row so a small volume hits ENOSPC quickly.
                 * xorshift64 seeded by the absolute row index. */
                uint64_t x = (uint64_t)(base_val + r) + 0x9E3779B97F4A7C15ULL;
                x ^= x >> 30; x *= 0xBF58476D1CE4E5B9ULL;
                x ^= x >> 27; x *= 0x94D049BB133111EBULL;
                x ^= x >> 31;
                double d; memcpy(&d, &x, 8);
                if (d != d) d = 1.0;            /* avoid NaN poisoning min/max */
                uint64_t bits; memcpy(&bits, &d, 8); put_u64(p, bits);
            }
            p += 8;
        }
    }
    uint64_t req_id = ++c->next_req_id;
    int rc = frame_send(c, MSG_WRITE_BATCH, 0, req_id, buf, (uint32_t)(p - buf));
    free(buf);
    if (rc != 0) return -1;

    tsdb_msg_t resp = {0};
    if (frame_recv(c, &resp) != 0) { msg_free(&resp); return -1; }
    int t = resp.hdr.type;
    msg_free(&resp);
    if (t == MSG_WRITE_ACK) return 0;
    if (t == MSG_ERROR)     return 1;
    return -1; /* unexpected frame */
}

static int do_ping(tsdb_conn_t *c) {
    uint64_t req_id = ++c->next_req_id;
    if (frame_send(c, MSG_PING, 0, req_id, NULL, 0) != 0) return -1;
    tsdb_msg_t resp = {0};
    if (frame_recv(c, &resp) != 0) { msg_free(&resp); return -1; }
    int ok = (resp.hdr.type == MSG_PING);
    msg_free(&resp);
    return ok ? 0 : -1;
}

int main(int argc, char **argv) {
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port        = (argc > 2) ? atoi(argv[2]) : 39301;
    const char *table = (argc > 3) ? argv[3] : "enospc";
    int rows_per_batch = (argc > 4) ? atoi(argv[4]) : 8192;
    int max_batches    = (argc > 5) ? atoi(argv[5]) : 2000000;

    tsdb_conn_t c = {0};
    c.timeout_ms = 15000;
    c.next_req_id = 0;
    c.fd = tcp_connect(host, port, c.timeout_ms);
    if (c.fd < 0) { fprintf(stderr, "FATAL connect %s:%d failed: %s\n", host, port, strerror(errno)); return 4; }

    if (do_hello(&c) != 0) { fprintf(stderr, "FATAL hello failed\n"); return 4; }

    /* Count-only mode: `enospc_writer HOST PORT TABLE 0 0 count` connects,
     * runs SELECT count(*) FROM TABLE, prints COUNT=<n>, exits.  Used by the
     * harness to read the durable count over the wire (the image's tsdb-cli
     * is a network client, not an embedded REPL). */
    if (argc > 6 && strcmp(argv[6], "count") == 0) {
        long long cnt = -1;
        char q[256]; snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
        int qrc = do_query(&c, q, &cnt);
        printf("COUNT=%lld (qrc=%d)\n", cnt, qrc);
        tsdb_conn_close(&c);
        return (qrc == 0 && cnt >= 0) ? 0 : 1;
    }

    char ddl[512];
    snprintf(ddl, sizeof(ddl),
        "CREATE TABLE %s (ts TIMESTAMP, v INT64, f FLOAT64) TIMESTAMP(ts)", table);
    if (do_query(&c, ddl, NULL) != 0) {
        fprintf(stderr, "FATAL create table failed\n"); return 4;
    }
    printf("SETUP ok: table=%s rows/batch=%d\n", table, rows_per_batch);
    fflush(stdout);

    long long acked_rows = 0;
    int err_batch = -1;
    int got_clean_err = 0;
    int transport_fail = 0;
    int b;
    for (b = 0; b < max_batches; b++) {
        int rc = do_write_batch(&c, table, rows_per_batch,
                                1000000000LL + (int64_t)b * rows_per_batch,
                                (int64_t)b * rows_per_batch);
        if (rc == 0) {
            acked_rows += rows_per_batch;
            if ((b % 20) == 0) { printf("  acked batch %d (rows=%lld)\n", b, acked_rows); fflush(stdout); }
            continue;
        } else if (rc == 1) {
            got_clean_err = 1; err_batch = b;
            printf("WRITE_ERROR clean at batch %d after %lld acked rows\n", b, acked_rows);
            fflush(stdout);
            break;
        } else {
            transport_fail = 1; err_batch = b;
            printf("WRITE_TRANSPORT_FAIL at batch %d after %lld acked rows\n", b, acked_rows);
            fflush(stdout);
            break;
        }
    }
    (void)err_batch;
    if (b >= max_batches) {
        printf("NO_ENOSPC: wrote all %d batches (%lld rows) without error\n", max_batches, acked_rows);
        tsdb_conn_close(&c);
        return 0;
    }

    if (transport_fail) {
        printf("VERDICT: transport failure on write (possible hang/crash)\n");
        tsdb_conn_close(&c);
        tsdb_conn_t c2 = {0}; c2.timeout_ms = 8000;
        c2.fd = tcp_connect(host, port, c2.timeout_ms);
        if (c2.fd < 0 || do_hello(&c2) != 0) {
            printf("SERVER_DEAD: cannot reconnect after transport fail\n");
            if (c2.fd >= 0) tsdb_conn_close(&c2);
            return 3;
        }
        long long cnt = -1;
        char q[256]; snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
        do_query(&c2, q, &cnt);
        printf("RECONNECT_OK durable_count=%lld acked=%lld\n", cnt, acked_rows);
        tsdb_conn_close(&c2);
        return 2;
    }

    /* got_clean_err path: server returned MSG_ERROR. Probe responsiveness. */
    int ping_ok = (do_ping(&c) == 0);
    printf("PING after error: %s\n", ping_ok ? "OK (responsive)" : "FAILED");
    fflush(stdout);

    long long durable = -1;
    char q[256]; snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
    int qrc = do_query(&c, q, &durable);
    printf("SELECT count(*) after ENOSPC: rc=%d count=%lld (acked=%lld)\n",
           qrc, durable, acked_rows);
    fflush(stdout);

    /* Semantics: a LIVE SELECT after the failed batch may legitimately
     * return count >= acked, because the failed batch's rows linger in the
     * memtable (flush failed -> memtable not cleared -> visible but NOT yet
     * durable).  So count in [acked, acked + one_batch] is expected and
     * fine.  Real corruption signals are: count == 0 with acked > 0
     * (total loss), count < acked (lost acked-durable rows), garbage (<0),
     * or count far above acked + a single batch (duplication). The DURABLE
     * count is proven by the harness's restart leg (post-restart == acked). */
    long long upper = acked_rows + (long long)rows_per_batch + 1;
    int verdict = 0;
    if (!ping_ok)            { printf("VERDICT: server unresponsive after error\n"); verdict = 3; }
    else if (qrc != 0)       { printf("VERDICT: query failed after error\n");        verdict = 3; }
    else if (durable < 0)    { printf("VERDICT: query returned garbage\n");           verdict = 5; }
    else if (acked_rows > 0 && durable == 0) {
        printf("VERDICT: DATA LOSS — acked %lld rows but count==0\n", acked_rows);
        verdict = 5;
    }
    else if (durable < acked_rows) {
        printf("VERDICT: count %lld < acked %lld — lost acked-durable rows\n", durable, acked_rows);
        verdict = 5;
    }
    else if (durable > upper) {
        printf("VERDICT: count %lld >> acked %lld + 1 batch — duplication/corruption\n",
               durable, acked_rows);
        verdict = 5;
    }
    else {
        printf("VERDICT: clean ENOSPC — no crash, clean error, count=%lld "
               "(acked=%lld durable + up to one failed-batch retained in memtable)\n",
               durable, acked_rows);
        verdict = 0;
    }
    tsdb_conn_close(&c);
    return verdict;
}
