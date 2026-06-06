/* verify_count.c — does count(*) include memtable rows under deferred-flush?
 *
 *  Writes EXACTLY N rows (N < TSDB_BLOCK_POINTS = 8192) in a single batch,
 *  then immediately queries count(*).  Under deferred-flush + sub-threshold N,
 *  the rows MUST be in the memtable (no flush has occurred).
 *
 *   count==N → memtable IS counted by count(*) (anti-entropy semantics safe).
 *   count==0 → memtable EXCLUDED (anti-entropy under-reports → middle-gap
 *              truncate-and-refill triggered spuriously under deferred-flush).
 *
 *   gcc -O2 -pthread verify_count.c tsdb_wire.c -o verify_count
 */
#define _DEFAULT_SOURCE 1
#define _POSIX_C_SOURCE 200809L
#include "tsdb_wire.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#define N_ROWS 500

static int connect_node(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return -1;
    struct sockaddr_in sa = {0}; sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port); sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) { close(fd); return -1; }
    return fd;
}
static int handshake(tsdb_conn_t *c) {
    uint8_t b[64], *p = b;
    *p++ = TSDB_WIRE_VER; const char *cid = "verify";
    *p++ = (uint8_t)strlen(cid); memcpy(p, cid, strlen(cid)); p += strlen(cid); *p++ = 0;
    if (frame_send(c, MSG_HELLO, 0, c->next_req_id++, b, (uint32_t)(p - b)) < 0) return -1;
    tsdb_msg_t r = {0}; int rc = frame_recv(c, &r);
    int ok = (rc == 0 && r.hdr.type == MSG_HELLO_OK); msg_free(&r); return ok ? 0 : -1;
}
static int exec_qtl(tsdb_conn_t *c, const char *qtl) {
    size_t ql = strlen(qtl); uint8_t b[1024]; put_u16(b, (uint16_t)ql); memcpy(b + 2, qtl, ql);
    if (frame_send(c, MSG_QUERY, 0, c->next_req_id++, b, (uint32_t)(ql + 2)) < 0) return -1;
    for (;;) { tsdb_msg_t m = {0}; int rc = frame_recv(c, &m); if (rc<0){msg_free(&m);return -1;}
        int fin = (m.hdr.flags & TSDB_FLAG_FIN) != 0, err = (m.hdr.type == MSG_ERROR);
        msg_free(&m); if (err) return -1; if (fin) break; }
    return 0;
}
/* Query expected to return 1 row, 1 i64 col → fill *out and -1 on error. */
static int query_i64(tsdb_conn_t *c, const char *qtl, int64_t *out) {
    size_t ql = strlen(qtl); uint8_t b[1024]; put_u16(b, (uint16_t)ql); memcpy(b + 2, qtl, ql);
    if (frame_send(c, MSG_QUERY, 0, c->next_req_id++, b, (uint32_t)(ql + 2)) < 0) return -1;
    int got = 0;
    for (;;) {
        tsdb_msg_t m = {0}; int rc = frame_recv(c, &m); if (rc<0){msg_free(&m);return -1;}
        int fin = (m.hdr.flags & TSDB_FLAG_FIN) != 0;
        if (m.hdr.type == MSG_QUERY_RESULT_ROWS && m.hdr.payload_len >= 6) {
            /* payload: nrows u32, then 1 col with raw i64 layout: name_len u8,name,type,codec,bytes_u32,data.
             * Easier: skip to the last 8 bytes (since 1 row × 1 i64 col = 8 bytes raw at the end). */
            const uint8_t *p = m.payload;
            uint32_t nr = (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
            (void)nr;
            if (m.hdr.payload_len >= 8 && !got) {
                int64_t v; memcpy(&v, m.payload + m.hdr.payload_len - 8, 8);
                *out = v; got = 1;
            }
        }
        if (m.hdr.type == MSG_ERROR) { msg_free(&m); return -1; }
        msg_free(&m); if (fin) break;
    }
    return got ? 0 : -1;
}
static size_t build_batch(uint8_t *buf, const char *tbl, uint32_t nrows) {
    uint8_t *p = buf; size_t tl = strlen(tbl);
    *p++ = (uint8_t)tl; memcpy(p, tbl, tl); p += tl;
    put_u16(p, 2); p += 2; put_u32(p, nrows); p += 4;
    *p++ = 2; memcpy(p, "ts", 2); p += 2; *p++ = 1; *p++ = 0; put_u32(p, nrows * 8u); p += 4;
    for (uint32_t i = 0; i < nrows; i++) { int64_t ts = 1000000000000LL + (int64_t)i * 1000000LL; put_u64(p, (uint64_t)ts); p += 8; }
    *p++ = 3; memcpy(p, "val", 3); p += 3; *p++ = 3; *p++ = 0; put_u32(p, nrows * 8u); p += 4;
    for (uint32_t i = 0; i < nrows; i++) { double v = (double)i; uint64_t bits; memcpy(&bits, &v, 8); put_u64(p, bits); p += 8; }
    return (size_t)(p - buf);
}

int main(int argc, char **argv) {
    int port = argc > 1 ? atoi(argv[1]) : 29301;
    int fd = connect_node(port);
    if (fd < 0) { perror("connect"); return 1; }
    tsdb_conn_t c = { .fd = fd, .timeout_ms = 10000, .next_req_id = 1 };
    if (handshake(&c) < 0) { fprintf(stderr, "handshake failed\n"); return 1; }

    /* Fresh slate */
    (void)exec_qtl(&c, "DROP STABLE vst");
    if (exec_qtl(&c, "CREATE STABLE vst (ts TIMESTAMP, val FLOAT64) TAGS (t SYMBOL)") < 0) {
        fprintf(stderr, "CREATE STABLE failed\n"); return 1; }
    if (exec_qtl(&c, "CREATE TABLE vct USING vst TAGS ('v')") < 0) {
        fprintf(stderr, "CREATE TABLE failed\n"); return 1; }

    /* 500 rows in 1 batch → well under TSDB_BLOCK_POINTS=8192 → fully in memtable. */
    uint8_t buf[1 << 16]; size_t n = build_batch(buf, "vct", N_ROWS);
    if (frame_send(&c, MSG_WRITE_BATCH, 0, c.next_req_id++, buf, (uint32_t)n) < 0) {
        fprintf(stderr, "WRITE_BATCH send failed\n"); return 1; }
    tsdb_msg_t ack = {0};
    if (frame_recv(&c, &ack) < 0 || ack.hdr.type != MSG_WRITE_ACK) {
        fprintf(stderr, "WRITE_ACK not received (got type=%d)\n", ack.hdr.type); msg_free(&ack); return 1; }
    uint64_t acked = ack.hdr.payload_len >= 8 ? get_u64(ack.payload) : 0;
    msg_free(&ack);
    printf("[verify] wrote %d rows; ACK = %llu\n", N_ROWS, (unsigned long long)acked);

    int64_t cnt = -1, mn = -1, mx = -1;
    if (query_i64(&c, "SELECT count(*) FROM vct", &cnt) < 0) { fprintf(stderr, "count(*) query failed\n"); return 1; }
    if (query_i64(&c, "SELECT min(ts)  FROM vct", &mn)  < 0) mn = -1;
    if (query_i64(&c, "SELECT max(ts)  FROM vct", &mx)  < 0) mx = -1;
    printf("[verify] count(*)=%lld  min(ts)=%lld  max(ts)=%lld  expected_count=%d  expected_max_ts=%lld\n",
           (long long)cnt, (long long)mn, (long long)mx,
           N_ROWS, (long long)(1000000000000LL + (int64_t)(N_ROWS - 1) * 1000000LL));

    printf("[verify] RESULT: count(*) %s memtable rows  (%lld vs %d)\n",
           cnt == N_ROWS ? "DOES count" : (cnt == 0 ? "DOES NOT count" : "PARTIAL — anomalous"),
           (long long)cnt, N_ROWS);

    /* Idle 3s, retry — does memtable get auto-flushed by maintenance? */
    sleep(3);
    if (query_i64(&c, "SELECT count(*) FROM vct", &cnt) == 0)
        printf("[verify] count(*) after 3s idle = %lld\n", (long long)cnt);

    close(fd);
    return 0;
}
