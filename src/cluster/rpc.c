/* rpc.c — TCP binary RPC implementation.
 *
 * Server model: one accept loop thread; per-connection handler thread.
 * Client model: blocking synchronous calls with mutex-protected socket.
 */

#include "rpc.h"
#include "rawblock.h"
#include "../storage/db.h"
#include "../storage/schema.h"
#include "../storage/memtable.h"
#include "../federation/fedrpc.h"
#include "../../include/tsdb_cluster.h"  /* tsdb_g_scatter_local_mode */
#include "../server/metrics.h"
#include "../server/tls.h"
#include "../compress/lzlite.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

/* ---- CRC32 (IEEE polynomial, software) ----------------------------------- */

static uint32_t crc32_table[256];
static int crc32_init_done = 0;

static void crc32_init(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_init_done = 1;
}

static uint32_t crc32(const uint8_t *data, size_t len) {
    if (!crc32_init_done) crc32_init();
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        c = crc32_table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

/* ---- Wire encode / decode ------------------------------------------------ */

int tsdb_rpc_encode(uint8_t *buf, size_t buf_cap,
                    tsdb_rpc_type_t type, uint32_t req_id,
                    const uint8_t *payload, uint32_t payload_len)
{
    if (buf_cap < TSDB_RPC_HDR_SIZE + payload_len) return -1;

    uint8_t *p = buf;

    /* magic */
    uint32_t magic = TSDB_RPC_MAGIC;
    memcpy(p, &magic, 4); p += 4;

    /* ver */
    *p++ = TSDB_RPC_VER;

    /* rpc_id */
    *p++ = (uint8_t)type;

    /* req_id */
    memcpy(p, &req_id, 4); p += 4;

    /* payload_len */
    memcpy(p, &payload_len, 4); p += 4;

    /* crc32: covers ver(1) + rpc_id(1) + req_id(4) + payload_len(4) + payload */
    uint32_t crc = 0;
    {
        uint8_t tmp[10];
        tmp[0] = TSDB_RPC_VER;
        tmp[1] = (uint8_t)type;
        memcpy(tmp + 2, &req_id, 4);
        memcpy(tmp + 6, &payload_len, 4);
        crc = crc32(tmp, 10);
        if (payload && payload_len > 0)
            crc = crc32(payload, payload_len) ^ crc; /* simple chain */
    }
    memcpy(p, &crc, 4); p += 4;

    /* payload */
    if (payload && payload_len > 0) {
        memcpy(p, payload, payload_len);
        p += payload_len;
    }

    return (int)(p - buf);
}

int tsdb_rpc_decode(const uint8_t *buf, size_t len, tsdb_rpc_msg_t *msg) {
    if (len < TSDB_RPC_HDR_SIZE) return 0; /* need more */

    uint32_t magic;
    memcpy(&magic, buf, 4);
    if (magic != TSDB_RPC_MAGIC) return -1; /* corrupt */

    uint8_t ver = buf[4];
    (void)ver;
    uint8_t rpc_id = buf[5];

    uint32_t req_id;
    memcpy(&req_id, buf + 6, 4);

    uint32_t payload_len;
    memcpy(&payload_len, buf + 10, 4);

    uint32_t stored_crc;
    memcpy(&stored_crc, buf + 14, 4);

    if (len < TSDB_RPC_HDR_SIZE + payload_len) return 0; /* need more */

    /* Verify CRC. */
    uint32_t computed = 0;
    {
        uint8_t tmp[10];
        tmp[0] = buf[4];
        tmp[1] = buf[5];
        memcpy(tmp + 2, buf + 6,  4);
        memcpy(tmp + 6, buf + 10, 4);
        computed = crc32(tmp, 10);
        if (payload_len > 0)
            computed = crc32(buf + TSDB_RPC_HDR_SIZE, payload_len) ^ computed;
    }
    if (computed != stored_crc) return -1; /* checksum mismatch */

    msg->type        = (tsdb_rpc_type_t)rpc_id;
    msg->req_id      = req_id;
    msg->payload_len = payload_len;
    msg->payload     = (uint8_t *)(buf + TSDB_RPC_HDR_SIZE);

    return (int)(TSDB_RPC_HDR_SIZE + payload_len);
}

/* ---- I/O helpers --------------------------------------------------------- */

/* Parse "host:port" into host string + port int. */
static int parse_addr(const char *addr, char *host_out, size_t host_cap, int *port_out) {
    if (!addr) return -1;
    const char *colon = strrchr(addr, ':');
    if (!colon) return -1;
    size_t hlen = (size_t)(colon - addr);
    if (hlen >= host_cap) return -1;
    memcpy(host_out, addr, hlen);
    host_out[hlen] = '\0';
    *port_out = atoi(colon + 1);
    return 0;
}

/* ---- Optional mutual TLS for the inter-node RPC channel ------------------
 *
 * Off by default: when TSDB_RPC_TLS is unset the int-fd plaintext path below
 * runs unchanged (byte-identical to before this feature).  When set, each
 * accepted peer fd is wrapped server-side (require + verify the client cert
 * against the CA) and each tsdb_rpc_connect wraps its fd client-side,
 * presenting the configured cert.  Reuses src/server/tls.c primitives.
 *
 *   TSDB_RPC_TLS=1                 enable
 *   TSDB_RPC_TLS_CERT=<pem>        this node's certificate (chain)
 *   TSDB_RPC_TLS_KEY=<pem>         matching private key
 *   TSDB_RPC_TLS_CA=<pem>          CA that signed peer certs (mutual verify)
 *   TSDB_RPC_TLS_SKIP_VERIFY=1     client skips cert verification (bootstrap)
 */
static int rpc_tls_enabled(void) {
    const char *e = getenv("TSDB_RPC_TLS");
    return (e && *e && e[0] != '0') ? 1 : 0;
}

static pthread_once_t   rpc_tls_srv_once = PTHREAD_ONCE_INIT;
static pthread_once_t   rpc_tls_cli_once = PTHREAD_ONCE_INIT;
static tsdb_tls_ctx_t  *rpc_tls_srv_ctx  = NULL;
static tsdb_tls_ctx_t  *rpc_tls_cli_ctx  = NULL;

static void rpc_tls_srv_init(void) {
    const char *cert = getenv("TSDB_RPC_TLS_CERT");
    const char *key  = getenv("TSDB_RPC_TLS_KEY");
    const char *ca   = getenv("TSDB_RPC_TLS_CA");
    if (!cert || !*cert || !key || !*key) {
        fprintf(stderr, "[rpc] TSDB_RPC_TLS set but CERT/KEY missing\n");
        return;
    }
    /* ca non-NULL → server requires + verifies the client cert (mutual). */
    if (tsdb_tls_server_ctx(cert, key, ca, &rpc_tls_srv_ctx) != 0)
        rpc_tls_srv_ctx = NULL;
}

static void rpc_tls_cli_init(void) {
    const char *cert = getenv("TSDB_RPC_TLS_CERT");
    const char *key  = getenv("TSDB_RPC_TLS_KEY");
    const char *ca   = getenv("TSDB_RPC_TLS_CA");
    const char *sv   = getenv("TSDB_RPC_TLS_SKIP_VERIFY");
    int skip = (sv && *sv && sv[0] != '0') ? 1 : 0;
    if (tsdb_tls_client_ctx(ca, skip, cert, key, &rpc_tls_cli_ctx) != 0)
        rpc_tls_cli_ctx = NULL;
}

static tsdb_tls_ctx_t *rpc_tls_server_ctx(void) {
    pthread_once(&rpc_tls_srv_once, rpc_tls_srv_init);
    return rpc_tls_srv_ctx;
}

static tsdb_tls_ctx_t *rpc_tls_client_ctx(void) {
    pthread_once(&rpc_tls_cli_once, rpc_tls_cli_init);
    return rpc_tls_cli_ctx;
}

/* Full read: keep going until all bytes received or error.
 * tls != NULL routes through the TLS connection; NULL is the plain fd path. */
static int read_full(int fd, tsdb_tls_conn_t *tls, uint8_t *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t r = tls ? tsdb_tls_read(tls, buf + done, len - done)
                        : read(fd, buf + done, len - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

/* Full write: keep going until all bytes sent or error.
 * tls != NULL routes through the TLS connection; NULL is the plain fd path. */
static int write_full(int fd, tsdb_tls_conn_t *tls, const uint8_t *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t w = tls ? tsdb_tls_write(tls, buf + done, len - done)
                        : write(fd, buf + done, len - done);
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    return 0;
}

/* Full writev: drain all iovecs, advancing past whatever a short write
 * consumed.  EINTR-safe (retries), same error contract as write_full.
 *
 * TLS has no scatter-write primitive, so when tls != NULL each iov segment
 * is sent in order via write_full (TLS framing makes the on-wire bytes
 * differ from plaintext writev anyway — the decoded frame is identical).
 * tls == NULL keeps the exact single-writev plaintext path. */
static int writev_all(int fd, tsdb_tls_conn_t *tls, struct iovec *iov, int iovcnt) {
    if (tls) {
        for (int i = 0; i < iovcnt; i++) {
            if (write_full(fd, tls, (const uint8_t *)iov[i].iov_base,
                           iov[i].iov_len) < 0)
                return -1;
        }
        return 0;
    }
    while (iovcnt > 0) {
        ssize_t w = writev(fd, iov, iovcnt);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) return -1;
        size_t adv = (size_t)w;
        while (iovcnt > 0 && adv >= iov->iov_len) {
            adv -= iov->iov_len;
            iov++;
            iovcnt--;
        }
        if (iovcnt > 0 && adv > 0) {
            iov->iov_base = (uint8_t *)iov->iov_base + adv;
            iov->iov_len -= adv;
        }
    }
    return 0;
}

/* Encode just the 18-byte frame header into hdr (byte-identical to the
 * header tsdb_rpc_encode emits, including the CRC that chains the payload),
 * so the body can be referenced in place and sent via writev instead of
 * being copied into a contiguous send buffer. */
static void encode_header(uint8_t hdr[TSDB_RPC_HDR_SIZE],
                          tsdb_rpc_type_t type, uint32_t req_id,
                          const uint8_t *payload, uint32_t payload_len)
{
    uint8_t *p = hdr;
    uint32_t magic = TSDB_RPC_MAGIC;
    memcpy(p, &magic, 4); p += 4;
    *p++ = TSDB_RPC_VER;
    *p++ = (uint8_t)type;
    memcpy(p, &req_id, 4); p += 4;
    memcpy(p, &payload_len, 4); p += 4;

    uint32_t crc = 0;
    {
        uint8_t tmp[10];
        tmp[0] = TSDB_RPC_VER;
        tmp[1] = (uint8_t)type;
        memcpy(tmp + 2, &req_id, 4);
        memcpy(tmp + 6, &payload_len, 4);
        crc = crc32(tmp, 10);
        if (payload && payload_len > 0)
            crc = crc32(payload, payload_len) ^ crc; /* simple chain */
    }
    memcpy(p, &crc, 4);
}

/* ---- Per-connection handler ---------------------------------------------- */

/* Recv a complete frame into caller-allocated buffer. */
static int recv_frame(int fd, tsdb_tls_conn_t *tls, uint8_t *hdr_buf,
                      uint8_t **payload_out,
                      uint32_t *payload_len_out, tsdb_rpc_msg_t *msg)
{
    if (read_full(fd, tls, hdr_buf, TSDB_RPC_HDR_SIZE) < 0) return -1;

    uint32_t magic;
    memcpy(&magic, hdr_buf, 4);
    if (magic != TSDB_RPC_MAGIC) return -1;

    uint32_t plen;
    memcpy(&plen, hdr_buf + 10, 4);

    /* One contiguous allocation for header+payload; read the body straight
     * in after the header so tsdb_rpc_decode sees a single buffer without a
     * second malloc + full-payload memcpy. */
    size_t total = TSDB_RPC_HDR_SIZE + plen;
    uint8_t *combined = malloc(total);
    if (!combined) return -1;
    memcpy(combined, hdr_buf, TSDB_RPC_HDR_SIZE);
    if (plen > 0 && read_full(fd, tls, combined + TSDB_RPC_HDR_SIZE, plen) < 0) {
        free(combined);
        return -1;
    }

    int consumed = tsdb_rpc_decode(combined, total, msg);
    if (consumed <= 0) { free(combined); return -1; }

    /* Caller owns combined. */
    *payload_out     = combined;
    *payload_len_out = plen;
    return 0;
}

/* Send a simple ACK or ERR response.
 *
 * Pre-fix this used a fixed 64-byte stack body cap.  Most RPCs reply
 * with no body or a tiny ack, so 64 was fine in practice — until
 * TSDB_RPC_CATALOG_DUMP (rpc id 19) started replying with the
 * verbatim contents of the master's catalog log files (tens of KB).
 * tsdb_rpc_encode returned -1 on the size mismatch, write_full never
 * fired, the master closed the connection without sending anything,
 * and the data peer's tsdb_rpc_call_recv saw a 0-byte read and
 * returned TSDB_ERR_IO (-3).  Observed end-to-end as cnode-3
 * cold-boot self-heal looping forever on `rpc rc=-3`.
 *
 * Heap-allocate when the body doesn't fit the stack buffer; fall back
 * to a silent no-op on malloc failure so a memory-pressured master
 * doesn't double-fault writing a partial frame. */
static void send_reply(int fd, tsdb_tls_conn_t *tls, tsdb_rpc_type_t type,
                       uint32_t req_id,
                       const uint8_t *body, uint32_t body_len) {
    uint8_t small[TSDB_RPC_HDR_SIZE + 64];
    size_t  cap = TSDB_RPC_HDR_SIZE + body_len;
    uint8_t *buf = (cap <= sizeof(small)) ? small : malloc(cap);
    if (!buf) return;
    int n = tsdb_rpc_encode(buf, cap, type, req_id, body, body_len);
    if (n > 0) write_full(fd, tls, buf, (size_t)n);
    if (buf != small) free(buf);
}

/* Server handler args. */
typedef struct {
    int                  fd;
    tsdb_tls_conn_t     *tls;   /* NULL → plaintext (default) */
    tsdb_db_t           *db;
    tsdb_node_manager_t *node_mgr;
    struct tsdb_rpc_server *srv;   /* for live-connection tracking */
} handler_args_t;

/* Forward decls — registry lives in struct tsdb_rpc_server below. */
static void rpc_conn_track_add(struct tsdb_rpc_server *s, int fd);
static void rpc_conn_track_remove(struct tsdb_rpc_server *s, int fd);

/* Handle one client connection in its own thread. */
/* Decode a raw WRITE_BATCH payload and apply it locally (local_only, so it
 * doesn't re-enter the cluster fanout).  Returns 1 if rows landed, 0 on any
 * decode/open/append/commit failure.  Shared by the WRITE_BATCH (raw) and
 * WRITE_BATCH_LZ (post-decompress) receive paths.  Extracted from the
 * inline handler so the lz path doesn't duplicate the 80-line apply chain. */
static int rpc_apply_write_batch(tsdb_db_t *db,
                                 const uint8_t *payload, uint32_t payload_len) {
    if (!db || payload_len == 0) return 0;

    char table_name[64] = {0};
    int ncols = 0, nrows = 0;
    int col_types[TSDB_MAX_COLS];
    uint8_t *col_data[TSDB_MAX_COLS] = {0};

    int rc = tsdb_rpc_decode_write_batch(payload, payload_len,
                                         table_name, sizeof(table_name),
                                         &ncols, col_types, &nrows,
                                         (uint8_t **)col_data);
    if (rc != 0 || nrows <= 0) return 0;

    tsdb_table_t *tbl = NULL;
    if (tsdb_open_table(db, table_name, &tbl) != TSDB_OK || !tbl) return 0;

    int write_ok = 0;
    tsdb_table_lock_write(tbl);
    tsdb_batch_t *batch = NULL;
    if (tsdb_batch_begin(tbl, &batch) == TSDB_OK) {
        tsdb_batch_set_local_only(batch);

        /* Wire layout: non-symbol cols are nrows×8 raw bytes, symbol cols are
         * [u32 total][u16 len][bytes]…  Compute per-column offsets into the
         * single contiguous payload and hand pointers to bulk-append. */
        int ts_ci = -1;
        int base[TSDB_MAX_COLS];
        int boff = 0;
        int sizing_ok = 1;
        for (int c = 0; c < ncols; c++) {
            base[c] = boff;
            if (col_types[c] == TSDB_TYPE_TIMESTAMP) ts_ci = c;
            if (col_types[c] == TSDB_TYPE_SYMBOL) {
                uint32_t total = 0;
                memcpy(&total, col_data[0] + boff, 4);
                boff += 4 + (int)total;
            } else {
                boff += 8 * nrows;
            }
            if ((uint32_t)boff > payload_len) { sizing_ok = 0; break; }
        }
        const void *col_arrs[TSDB_MAX_COLS];
        int data_types[TSDB_MAX_COLS];
        int n_data = 0;
        for (int c = 0; c < ncols && sizing_ok; c++) {
            if (c == ts_ci) continue;
            col_arrs[n_data]   = col_data[0] + base[c];
            data_types[n_data] = col_types[c];
            n_data++;
        }
        const int64_t *ts_arr = (ts_ci >= 0)
            ? (const int64_t *)(col_data[0] + base[ts_ci]) : NULL;
        int64_t *ts_synth = NULL;
        if (!ts_arr) ts_synth = calloc(nrows, sizeof(int64_t));
        int append_rc = sizing_ok
            ? tsdb_batch_append_bulk(batch, ts_arr ? ts_arr : ts_synth,
                                     col_arrs, data_types, n_data, (size_t)nrows)
            : TSDB_ERR_CORRUPT;
        free(ts_synth);
        if (append_rc == TSDB_OK) {
            if (tsdb_batch_commit(batch) == TSDB_OK) write_ok = 1;
        } else {
            tsdb_batch_discard(batch);
        }
    }
    tsdb_table_unlock_write(tbl);
    return write_ok;
}

/* Shared body of the FED_QUERY / FED_QUERY_LOCAL receive paths: decode the
 * QTL payload, run it through tsdb_query, encode the result, reply.
 * local_mode != 0 arms tsdb_g_scatter_local_mode (defined in query/exec.c)
 * for the duration of the query — scatter-local semantics for the
 * cluster-wide stable aggregation, and the anti-recursion guard that keeps
 * a scattered partial from scattering again. */
static void handle_fed_query(tsdb_db_t *db, int fd, tsdb_tls_conn_t *tls,
                             const tsdb_rpc_msg_t *msg, int local_mode)
{
    if (!db || msg->payload_len < 2) {
        send_reply(fd, tls, TSDB_RPC_ERR, msg->req_id, NULL, 0);
        return;
    }
    uint16_t qlen;
    memcpy(&qlen, msg->payload, 2);
    if ((uint32_t)qlen + 2 > msg->payload_len || qlen >= 4096) {
        send_reply(fd, tls, TSDB_RPC_ERR, msg->req_id, NULL, 0);
        return;
    }
    char qtl[4096];
    memcpy(qtl, msg->payload + 2, qlen);
    qtl[qlen] = '\0';

    tsdb_result_t *qr = NULL;
    int prev_mode = tsdb_g_scatter_local_mode;
    if (local_mode) tsdb_g_scatter_local_mode = 1;
    int qrc = tsdb_query(db, qtl, &qr);
    tsdb_g_scatter_local_mode = prev_mode;

    if (qrc != TSDB_OK || !qr) {
        send_reply(fd, tls, TSDB_RPC_ERR, msg->req_id, NULL, 0);
        return;
    }

    /* Encode result into a response buffer. */
    uint32_t rbufcap = 64 * 1024 * 1024; /* 64 MB */
    uint8_t *rbuf = malloc(rbufcap);
    if (rbuf) {
        int encoded = fedrpc_encode_result(rbuf, rbufcap, qr);
        if (encoded > 0) {
            /* Send ACK with payload = encoded result. */
            uint8_t *frame = malloc((size_t)encoded + TSDB_RPC_HDR_SIZE);
            if (frame) {
                int fn = tsdb_rpc_encode(frame,
                                         (size_t)encoded + TSDB_RPC_HDR_SIZE,
                                         TSDB_RPC_ACK,
                                         msg->req_id,
                                         rbuf, (uint32_t)encoded);
                if (fn > 0) write_full(fd, tls, frame, (size_t)fn);
                free(frame);
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg->req_id, NULL, 0);
            }
        } else {
            send_reply(fd, tls, TSDB_RPC_ERR, msg->req_id, NULL, 0);
        }
        free(rbuf);
    } else {
        send_reply(fd, tls, TSDB_RPC_ERR, msg->req_id, NULL, 0);
    }
    tsdb_result_free(qr);
}

static void *connection_handler(void *arg) {
    handler_args_t *ha = (handler_args_t *)arg;
    int fd = ha->fd;
    tsdb_tls_conn_t *tls = ha->tls;
    tsdb_db_t *db = ha->db;
    tsdb_node_manager_t *node_mgr = ha->node_mgr;
    struct tsdb_rpc_server *srv = ha->srv;
    free(ha);
    rpc_conn_track_add(srv, fd);

    uint8_t hdr_buf[TSDB_RPC_HDR_SIZE];
    for (;;) {
        uint8_t *combined = NULL;
        uint32_t plen     = 0;
        tsdb_rpc_msg_t msg = {0};

        if (recv_frame(fd, tls, hdr_buf, &combined, &plen, &msg) < 0) break;

        switch (msg.type) {
        case TSDB_RPC_HEARTBEAT:
            /* Update node state. */
            if (node_mgr && msg.payload_len >= 8) {
                tsdb_node_id_t nid;
                memcpy(&nid, msg.payload, 8);
                uint64_t ver = 0;
                if (msg.payload_len >= 16) memcpy(&ver, msg.payload + 8, 8);
                tsdb_node_manager_alive(node_mgr, nid, ver);
            }
            send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
            break;

        case TSDB_RPC_SCHEMA_SYNC:
            if (db && msg.payload_len > 0) {
                char table_name[64] = {0};
                int ncols = 0, ts_col_idx = 0;
                /* Sized for TSDB_MAX_COLS so 128-col IoT devices replicate
                 * without trashing adjacent stack.  64 KiB (names) + 2 KiB
                 * (types) is well under the default 8 MiB pthread stack. */
                char col_names[TSDB_MAX_COLS][64];
                int col_types[TSDB_MAX_COLS];
                int part_unit = 0, block_points = 0, sort_by_tag_col = -1;

                int rc = tsdb_rpc_decode_schema(msg.payload, msg.payload_len,
                                                table_name, sizeof(table_name),
                                                &ncols, col_names, col_types, &ts_col_idx,
                                                &part_unit, &block_points, &sort_by_tag_col);
                if (rc == 0 && ncols > 0 && ncols <= TSDB_MAX_COLS) {
                    tsdb_col_t cols[TSDB_MAX_COLS];
                    for (int i = 0; i < ncols; i++) {
                        cols[i].name = col_names[i];
                        cols[i].type = (tsdb_type_t)col_types[i];
                    }
                    const char *ts_name = (ts_col_idx >= 0 && ts_col_idx < ncols)
                                         ? col_names[ts_col_idx] : col_names[0];
                    /* Use _local_ex variant: avoids re-syncing AND threads
                     * partition_unit / block_points / sort_by_tag_col so the
                     * follower's local schema matches the leader's choice
                     * exactly.  Pre-tail (legacy) senders feed the defaults
                     * (DAY, 0, -1) which collapses to today's behavior. */
                    tsdb_create_table_local_ex(db, table_name, cols, ncols, ts_name,
                                                part_unit, block_points, sort_by_tag_col);
                }
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

        case TSDB_RPC_WRITE_BATCH: {
            /* Reflect the TRUE decode→open→begin→commit outcome in the reply.
             * ACK-without-landing used to drop rows silently (sender counts a
             * phantom replica) — the apply helper returns the real result. */
            int write_ok = rpc_apply_write_batch(db, msg.payload, msg.payload_len);
            if (write_ok) {
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_replicate_recv_ok_total");
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_replicate_recv_err_total");
            }
            break;
        }

        case TSDB_RPC_WRITE_BATCH_LZ: {
            /* Compressed WRITE_BATCH: [u32 orig_len LE][lzlite bytes].
             * Decompress into a scratch buffer, then apply via the shared
             * helper exactly as a raw WRITE_BATCH. */
            int write_ok = 0;
            if (db && msg.payload_len > 4) {
                uint32_t orig_len = 0;
                memcpy(&orig_len, msg.payload, 4);
                /* Guard against a malformed/huge orig_len (cap at 256 MiB). */
                if (orig_len > 0 && orig_len <= (256u << 20)) {
                    uint8_t *raw = malloc(orig_len);
                    if (raw) {
                        size_t got = 0;
                        int drc = tsdb_lzlite_decode(msg.payload + 4,
                                                     msg.payload_len - 4,
                                                     raw, orig_len, &got);
                        if (drc == TSDB_OK && got == orig_len) {
                            write_ok = rpc_apply_write_batch(db, raw, (uint32_t)got);
                        }
                        free(raw);
                    }
                }
            }
            if (write_ok) {
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_replicate_recv_ok_total");
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_replicate_recv_err_total");
            }
            break;
        }

        case TSDB_RPC_FED_INGEST: {
            /* Cross-DC ingest — payload is identical to WRITE_BATCH.
             * We apply it locally with the local_only flag set so the
             * batch does NOT re-enter the intra-cluster replication
             * loop.  Separate counters make it possible to tell "DC
             * fan-in" volume apart from "cluster fanout" on /metrics. */
            int write_ok = 0;
            if (db && msg.payload_len > 0) {
                char table_name[64] = {0};
                int ncols = 0, nrows = 0;
                int col_types[TSDB_MAX_COLS];
                uint8_t *col_data[TSDB_MAX_COLS] = {0};
                int rc = tsdb_rpc_decode_write_batch(
                    msg.payload, msg.payload_len,
                    table_name, sizeof(table_name),
                    &ncols, col_types, &nrows,
                    (uint8_t **)col_data);
                if (rc == 0 && nrows > 0) {
                    tsdb_table_t *tbl = NULL;
                    if (tsdb_open_table(db, table_name, &tbl) == TSDB_OK && tbl) {
                        tsdb_table_lock_write(tbl);
                        tsdb_batch_t *batch = NULL;
                        if (tsdb_batch_begin(tbl, &batch) == TSDB_OK) {
                            tsdb_batch_set_local_only(batch);
                            /* Same wire layout as WRITE_BATCH — hand it
                             * straight to bulk_append (see receiver above
                             * for format details). */
                            int ts_ci = -1;
                            int base[TSDB_MAX_COLS];
                            int boff = 0;
                            int sizing_ok = 1;
                            for (int c = 0; c < ncols; c++) {
                                base[c] = boff;
                                if (col_types[c] == TSDB_TYPE_TIMESTAMP) ts_ci = c;
                                if (col_types[c] == TSDB_TYPE_SYMBOL) {
                                    uint32_t total = 0;
                                    memcpy(&total, col_data[0] + boff, 4);
                                    boff += 4 + (int)total;
                                } else {
                                    boff += 8 * nrows;
                                }
                                if ((uint32_t)boff > msg.payload_len) {
                                    sizing_ok = 0;
                                    break;
                                }
                            }
                            const void *col_arrs[TSDB_MAX_COLS];
                            int data_types[TSDB_MAX_COLS];
                            int n_data = 0;
                            for (int c = 0; c < ncols && sizing_ok; c++) {
                                if (c == ts_ci) continue;
                                col_arrs[n_data]   = col_data[0] + base[c];
                                data_types[n_data] = col_types[c];
                                n_data++;
                            }
                            const int64_t *ts_arr = (ts_ci >= 0)
                                ? (const int64_t *)(col_data[0] + base[ts_ci])
                                : NULL;
                            int64_t *ts_synth = NULL;
                            if (!ts_arr) ts_synth = calloc(nrows, sizeof(int64_t));
                            int append_rc = sizing_ok
                                ? tsdb_batch_append_bulk(batch, ts_arr ? ts_arr : ts_synth,
                                                          col_arrs, data_types, n_data,
                                                          (size_t)nrows)
                                : TSDB_ERR_CORRUPT;
                            free(ts_synth);
                            if (append_rc == TSDB_OK) {
                                if (tsdb_batch_commit(batch) == TSDB_OK) write_ok = 1;
                            } else {
                                tsdb_batch_discard(batch);
                            }
                        }
                        tsdb_table_unlock_write(tbl);
                    }
                }
            }
            if (write_ok) {
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_dr_recv_ok_total");
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_dr_recv_err_total");
            }
            break;
        }

        case TSDB_RPC_FED_QUERY:
            /* Federation query: run QTL on local DB, encode result, send back. */
            handle_fed_query(db, fd, tls, &msg, 0 /* full local semantics */);
            break;

        case TSDB_RPC_FED_QUERY_LOCAL:
            /* Scatter-local query: same wire contract as FED_QUERY, but the
             * QTL executes with tsdb_g_scatter_local_mode set, so a stable
             * read never re-scatters and covers only the children this node
             * is the primary alive owner for.  Sent by the cluster-wide
             * stable-aggregation coordinator. */
            handle_fed_query(db, fd, tls, &msg, 1 /* scatter-local */);
            break;

        case TSDB_RPC_RAW_BLOCK_PUSH:
            /* Replica receive: parse block, write verbatim to .col/.idx. */
            if (db && msg.payload_len > 0) {
                tsdb_rawblock_push_t rb;
                int rc = tsdb_rawblock_parse(msg.payload, msg.payload_len, &rb);
                if (rc == TSDB_OK) {
                    rc = tsdb_rawblock_apply(db, &rb);
                }
                if (rc == TSDB_OK)
                    send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                else
                    send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

        case TSDB_RPC_APPLY_TRUNCATE:
            /* Peer-side: apply TRUNCATE locally.  Call the storage API
             * directly (not QTL) so we never re-enter exec.c and can't loop. */
            if (db && msg.payload_len > 0) {
                char tbl[128] = {0};
                int rc = tsdb_rpc_decode_truncate(msg.payload, msg.payload_len,
                                                  tbl, sizeof(tbl));
                if (rc == 0) {
                    int trc = tsdb_truncate_table(db, tbl);
                    /* Treat "table not found" as a successful no-op — this
                     * peer simply wasn't a replica for that table.  Only
                     * real I/O errors count as failure. */
                    if (trc == TSDB_OK || trc == TSDB_ERR_NOTFOUND)
                        send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                    else
                        send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                } else {
                    send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

        case TSDB_RPC_APPLY_DELETE_RANGE:
            if (db && msg.payload_len > 0) {
                char tbl[128] = {0};
                int64_t cutoff = 0;
                int op_lt = 0, inclusive = 0;
                int rc = tsdb_rpc_decode_delete_range(msg.payload, msg.payload_len,
                                                      tbl, sizeof(tbl),
                                                      &cutoff, &op_lt, &inclusive);
                if (rc == 0) {
                    int removed = 0;
                    int trc = tsdb_delete_range(db, tbl, cutoff,
                                                op_lt, inclusive, &removed);
                    if (trc == TSDB_OK || trc == TSDB_ERR_NOTFOUND)
                        send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                    else
                        send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                } else {
                    send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

        case TSDB_RPC_APPLY_CATALOG_QTL:
            /* Peer side: catalog mutation broadcast from a primary.
             * We re-run the QTL locally so DB / Group / VTable / PTable
             * registrations land in this peer's catalog.  The thread-
             * local guard prevents the nested tsdb_query from
             * re-broadcasting and ping-ponging forever. */
            if (db && msg.payload_len > 0) {
                char qtl[4096] = {0};
                int rc = tsdb_rpc_decode_catalog_qtl(msg.payload,
                                                     msg.payload_len,
                                                     qtl, sizeof(qtl));
                if (rc == 0) {
                    extern __thread int tsdb_g_suppress_catalog_broadcast;
                    tsdb_g_suppress_catalog_broadcast = 1;
                    tsdb_result_t *qr = NULL;
                    int qrc = tsdb_query(db, qtl, &qr);
                    tsdb_g_suppress_catalog_broadcast = 0;
                    if (qr) tsdb_result_free(qr);
                    /* EXISTS is a common idempotent case on re-broadcast
                     * or stale peer catch-up — treat as success. */
                    if (qrc == TSDB_OK || qrc == TSDB_ERR_EXISTS)
                        send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                    else
                        send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                } else {
                    send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

        case TSDB_RPC_DDL_FORWARD:
            /* Data-node → master DDL forward: run the QTL through the FULL
             * local query path (raft propose + catalog broadcast — no
             * suppress flag, unlike APPLY_CATALOG_QTL) and return the
             * status text as the ACK payload.  Peers on the RPC port are
             * already mutually trusted; this replaces the HTTP /sql proxy
             * that the dashboard auth gate rejected (cookies are
             * node-local). */
            if (db && msg.payload_len > 0) {
                char qtl[4096] = {0};
                if (tsdb_rpc_decode_catalog_qtl(msg.payload, msg.payload_len,
                                                qtl, sizeof(qtl)) == 0) {
                    tsdb_result_t *qr = NULL;
                    int qrc = tsdb_query(db, qtl, &qr);
                    const char *txt = NULL;
                    if (qr && tsdb_result_next(qr) == 1)
                        txt = tsdb_result_sym(qr, 0);   /* status row */
                    if (!txt) txt = (qrc == TSDB_OK) ? "OK" : tsdb_errstr(qrc);
                    send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id,
                               (const uint8_t *)txt, (uint32_t)strlen(txt));
                    if (qr) tsdb_result_free(qr);
                } else {
                    send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

        case TSDB_RPC_CATALOG_DUMP: {
            /* Master serves a snapshot of its catalog log files so a
             * data peer that missed earlier broadcasts (typical after
             * a crash recovery window) can self-heal at startup.
             * Defined in src/storage/catalog_sync.c. */
            extern int tsdb_catalog_dump_serialize(const char *data_dir,
                                                    uint8_t *out, size_t cap);
            extern const char *tsdb_db_data_dir(tsdb_db_t *db);
            if (!db) {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                break;
            }
            const char *dd = tsdb_db_data_dir(db);
            size_t cap = 32 * 1024 * 1024;
            uint8_t *body = (uint8_t *)malloc(cap);
            if (!body) {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                break;
            }
            int n = tsdb_catalog_dump_serialize(dd, body, cap);
            if (n < 0) {
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                free(body);
                break;
            }
            send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, body, (uint32_t)n);
            free(body);
            break;
        }

        case TSDB_RPC_RAFT_REQUEST_VOTE: {
            /* Raft candidate requesting a vote — hand off to the raft
             * state machine via the registered handler.  If no raft is
             * running on this node (role=data or feature off) we reply
             * ERR so the candidate treats this peer as absent. */
            uint8_t rb[64];
            uint32_t rn = 0;
            extern int tsdb_raft_rpc_handle_vote(const uint8_t *, uint32_t,
                                                 uint8_t *, uint32_t, uint32_t *);
            if (tsdb_raft_rpc_handle_vote(msg.payload, msg.payload_len,
                                          rb, sizeof(rb), &rn) == 0)
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            break;
        }

        case TSDB_RPC_RAFT_APPEND_ENTRIES: {
            /* Leader heartbeat or log replication — same plumbing as
             * REQUEST_VOTE above.  Response buffer sized for the 17-byte
             * resp struct; no further growth since responses never carry
             * entries. */
            uint8_t rb[64];
            uint32_t rn = 0;
            extern int tsdb_raft_rpc_handle_append(const uint8_t *, uint32_t,
                                                   uint8_t *, uint32_t, uint32_t *);
            if (tsdb_raft_rpc_handle_append(msg.payload, msg.payload_len,
                                            rb, sizeof(rb), &rn) == 0)
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            break;
        }

        case TSDB_RPC_RAFT_INSTALL_SNAPSHOT: {
            /* Far-behind follower receiving a full snapshot body.
             * Response is tiny (8 B) — just an ACK with the current term. */
            uint8_t rb[32];
            uint32_t rn = 0;
            extern int tsdb_raft_rpc_handle_install(const uint8_t *, uint32_t,
                                                    uint8_t *, uint32_t,
                                                    uint32_t *);
            if (tsdb_raft_rpc_handle_install(msg.payload, msg.payload_len,
                                             rb, sizeof(rb), &rn) == 0)
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            break;
        }

        case TSDB_RPC_RAFT_PRE_VOTE: {
            /* Pre-vote probe (§9.6): same payload as REQUEST_VOTE but
             * answered without touching persistent term/vote — a
             * partitioned returning node can't disrupt the cluster
             * just by announcing a higher term. */
            uint8_t rb[64];
            uint32_t rn = 0;
            extern int tsdb_raft_rpc_handle_pre_vote(const uint8_t *, uint32_t,
                                                     uint8_t *, uint32_t,
                                                     uint32_t *);
            if (tsdb_raft_rpc_handle_pre_vote(msg.payload, msg.payload_len,
                                               rb, sizeof(rb), &rn) == 0)
                send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, tls, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            break;
        }

        default:
            send_reply(fd, tls, TSDB_RPC_ACK, msg.req_id, NULL, 0);
            break;
        }

        free(combined);
    }

    rpc_conn_track_remove(srv, fd);   /* deregister before close (fd reuse) */
    if (tls) tsdb_tls_close(tls);     /* sends close_notify + closes fd */
    else     close(fd);
    return NULL;
}

/* ---- Server -------------------------------------------------------------- */

struct tsdb_rpc_server {
    int                  listen_fd;
    int                  port;
    tsdb_db_t           *db;
    tsdb_node_manager_t *node_mgr;
    pthread_t            accept_thread;
    volatile int         running;

    /* Live-connection registry — mirrors the wire server's drain: stop()
     * kicks every blocked recv (shutdown) and waits for the detached
     * handlers to finish before the caller tears down the db they use. */
    pthread_mutex_t conn_mu;
    pthread_cond_t  conn_cv;
    int            *conn_fds;
    int             conn_n, conn_cap;
    int             conn_inflight;
};

static void rpc_conn_track_add(struct tsdb_rpc_server *s, int fd) {
    if (!s) return;
    pthread_mutex_lock(&s->conn_mu);
    if (s->conn_n >= s->conn_cap) {
        int ncap = s->conn_cap ? s->conn_cap * 2 : 64;
        int *nf = realloc(s->conn_fds, (size_t)ncap * sizeof(int));
        if (nf) { s->conn_fds = nf; s->conn_cap = ncap; }
    }
    if (s->conn_n < s->conn_cap) s->conn_fds[s->conn_n++] = fd;
    s->conn_inflight++;
    pthread_mutex_unlock(&s->conn_mu);
}

static void rpc_conn_track_remove(struct tsdb_rpc_server *s, int fd) {
    if (!s) return;
    pthread_mutex_lock(&s->conn_mu);
    for (int i = 0; i < s->conn_n; i++) {
        if (s->conn_fds[i] == fd) { s->conn_fds[i] = s->conn_fds[--s->conn_n]; break; }
    }
    if (--s->conn_inflight == 0) pthread_cond_broadcast(&s->conn_cv);
    pthread_mutex_unlock(&s->conn_mu);
}

/* ---- TCP keepalive ------------------------------------------------------- */

/* Enable SO_KEEPALIVE + per-platform idle/intvl/cnt on a connected fd so a
 * peer that dies silently (power loss, severed link) is detected and the
 * socket torn down, instead of a half-open connection wedging an RPC slot.
 * Gated on TSDB_TCP_KEEPALIVE (default on; "0" disables); idle/intvl/cnt come
 * from TSDB_TCP_KEEPALIVE_IDLE_S / _INTVL_S / _CNT.  Per-option setsockopt
 * failures are non-fatal — an older kernel/SDK may lack a given knob. */
static void set_tcp_keepalive(int fd) {
    const char *en = getenv("TSDB_TCP_KEEPALIVE");
    if (en && strcmp(en, "0") == 0) return;

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) != 0) return;

    const char *ei = getenv("TSDB_TCP_KEEPALIVE_IDLE_S");
    const char *ev = getenv("TSDB_TCP_KEEPALIVE_INTVL_S");
    const char *ec = getenv("TSDB_TCP_KEEPALIVE_CNT");
    int idle  = (ei && *ei) ? atoi(ei) : 30;
    int intvl = (ev && *ev) ? atoi(ev) : 10;
    int cnt   = (ec && *ec) ? atoi(ec) : 3;

#ifdef __linux__
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE,  &idle,  sizeof(idle));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT,   &cnt,   sizeof(cnt));
#elif defined(__APPLE__)
#  ifdef TCP_KEEPALIVE
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &idle,  sizeof(idle));
#  endif
#  ifdef TCP_KEEPINTVL
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
#  endif
#  ifdef TCP_KEEPCNT
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT,   &cnt,   sizeof(cnt));
#  endif
#else
    (void)idle; (void)intvl; (void)cnt;
#endif
}

static void *accept_loop(void *arg) {
    tsdb_rpc_server_t *srv = (tsdb_rpc_server_t *)arg;

    while (srv->running) {
        struct pollfd pfd = { srv->listen_fd, POLLIN, 0 };
        int r = poll(&pfd, 1, 200);
        if (r <= 0) continue;

        struct sockaddr_in client_addr;
        socklen_t clen = sizeof(client_addr);
        int client_fd = accept(srv->listen_fd,
                               (struct sockaddr *)&client_addr, &clen);
        if (client_fd < 0) continue;

        /* Disable Nagle for low-latency. */
        int one = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        set_tcp_keepalive(client_fd);

        /* Iter 10: enlarge the receive buffer on the replication-accept
         * side so a peer can absorb a burst of WRITE_BATCH frames without
         * forcing the sender to stall.  Mirrors the SO_SNDBUF bump on the
         * connect side.  Env TSDB_RPC_RCVBUF_KB overrides (0 = autotune). */
        {
            const char *e = getenv("TSDB_RPC_RCVBUF_KB");
            int kb = e ? atoi(e) : 4096;
            if (kb > 0) { int b = kb * 1024; setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &b, sizeof(b)); }
        }

        /* Optional mutual TLS: wrap the accepted fd (synchronous handshake,
         * requires + verifies the peer cert) before handing it off.  Off by
         * default — tls stays NULL and the handler runs the plaintext path. */
        tsdb_tls_conn_t *tls = NULL;
        if (rpc_tls_enabled()) {
            tsdb_tls_ctx_t *sc = rpc_tls_server_ctx();
            if (!sc || tsdb_tls_server_wrap(sc, client_fd, &tls) != 0) {
                close(client_fd);
                continue;
            }
        }

        handler_args_t *ha = malloc(sizeof(*ha));
        if (!ha) {
            if (tls) tsdb_tls_close(tls); else close(client_fd);
            continue;
        }
        ha->fd       = client_fd;
        ha->tls      = tls;
        ha->db       = srv->db;
        ha->node_mgr = srv->node_mgr;
        ha->srv      = srv;

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, connection_handler, ha) != 0) {
            free(ha);
            if (tls) tsdb_tls_close(tls); else close(client_fd);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

tsdb_rpc_server_t *tsdb_rpc_server_new(const char *bind_addr,
                                        tsdb_db_t *db,
                                        tsdb_node_manager_t *node_mgr)
{
    char host[128] = "0.0.0.0";
    int  port = 28081;
    if (bind_addr) parse_addr(bind_addr, host, sizeof(host), &port);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return NULL;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in sa = {0};
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons((uint16_t)port);
    inet_aton(host, &sa.sin_addr);

    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd); return NULL;
    }
    if (listen(fd, 64) < 0) { close(fd); return NULL; }

    /* Get actual bound port. */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);

    tsdb_rpc_server_t *srv = calloc(1, sizeof(*srv));
    if (!srv) { close(fd); return NULL; }
    srv->listen_fd = fd;
    srv->port      = ntohs(bound.sin_port);
    srv->db        = db;
    srv->node_mgr  = node_mgr;
    srv->running   = 1;
    pthread_mutex_init(&srv->conn_mu, NULL);
    pthread_cond_init(&srv->conn_cv, NULL);

    pthread_create(&srv->accept_thread, NULL, accept_loop, srv);
    return srv;
}

void tsdb_rpc_server_stop(tsdb_rpc_server_t *srv) {
    if (!srv) return;
    srv->running = 0;
    pthread_join(srv->accept_thread, NULL);
    close(srv->listen_fd);

    /* Drain detached peer handlers: kick blocked recvs, wait for zero
     * inflight.  On timeout leak srv (and let the caller's db outlive us)
     * rather than freeing under a live handler. */
    pthread_mutex_lock(&srv->conn_mu);
    for (int i = 0; i < srv->conn_n; i++)
        shutdown(srv->conn_fds[i], SHUT_RDWR);
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    int drained = 1;
    while (srv->conn_inflight > 0) {
        if (pthread_cond_timedwait(&srv->conn_cv, &srv->conn_mu, &dl) == ETIMEDOUT) {
            drained = 0;
            break;
        }
    }
    int leftover = srv->conn_inflight;
    pthread_mutex_unlock(&srv->conn_mu);
    if (!drained) {
        fprintf(stderr, "[rpc] stop: %d peer handler(s) still in flight after "
                        "10s — leaking server state instead of freeing\n",
                leftover);
        return;
    }

    pthread_mutex_destroy(&srv->conn_mu);
    pthread_cond_destroy(&srv->conn_cv);
    free(srv->conn_fds);
    free(srv);
}

int tsdb_rpc_server_port(tsdb_rpc_server_t *srv) {
    return srv ? srv->port : -1;
}

/* ---- Client connection --------------------------------------------------- */

struct tsdb_rpc_conn {
    int              fd;
    tsdb_tls_conn_t *tls;   /* NULL → plaintext (default) */
    pthread_mutex_t  lock;
    uint32_t         next_req_id;
    char             addr[TSDB_ADDR_MAX];
};

tsdb_rpc_conn_t *tsdb_rpc_connect(const char *addr, int timeout_ms) {
    char host[128];
    int  port;
    if (parse_addr(addr, host, sizeof(host), &port) < 0) return NULL;

    if (timeout_ms <= 0) timeout_ms = 2000;

    /* Dual-stack: AF_UNSPEC lets getaddrinfo return both v4 and v6 results;
     * iterate and try each until one connects, applying the non-blocking
     * connect + poll timeout per attempt (mirrors the CLI tcp_connect). */
    struct addrinfo hints = {0}, *res = NULL;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);
    if (getaddrinfo(host, portstr, &hints, &res) != 0) return NULL;

    int fd = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;

        /* Set non-blocking for connect with timeout. */
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int cr = connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (cr < 0 && errno != EINPROGRESS) {
            close(fd); fd = -1; continue;
        }
        if (cr != 0) {
            struct pollfd pfd = { fd, POLLOUT, 0 };
            if (poll(&pfd, 1, timeout_ms) <= 0 || !(pfd.revents & POLLOUT)) {
                close(fd); fd = -1; continue;
            }
            int err = 0;
            socklen_t errlen = sizeof(err);
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errlen);
            if (err) { close(fd); fd = -1; continue; }
        }
        /* Restore blocking. */
        fcntl(fd, F_SETFL, flags);
        break;
    }
    freeaddrinfo(res);
    if (fd < 0) return NULL;

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    /* Bounded I/O: a peer that accepts a request and never responds must
     * not wedge this connection forever.  A blocked read here holds
     * conn->lock, which cascades: catalog-broadcast fanout workers queue
     * on the same mutex, which wedges the raft apply thread, which times
     * out every DDL propose cluster-wide (observed live: one lost fed-
     * query response during a rolling restart froze DDL on all nodes).
     * 30 s default — far above any healthy LAN RPC, small enough to
     * self-heal; env TSDB_RPC_IO_TIMEOUT_MS overrides (0 = unbounded). */
    {
        static int io_tmo_ms = -1;
        if (io_tmo_ms < 0) {
            const char *e = getenv("TSDB_RPC_IO_TIMEOUT_MS");
            io_tmo_ms = (e && *e) ? atoi(e) : 30000;
        }
        if (io_tmo_ms > 0) {
            struct timeval tv = { .tv_sec  = io_tmo_ms / 1000,
                                  .tv_usec = (io_tmo_ms % 1000) * 1000 };
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        }
    }

    set_tcp_keepalive(fd);

    /* Iter 10: enlarge the send buffer on the leader→peer replication
     * link.  perf on a virgin cluster put tcp_sendmsg at 7.27% of leader
     * CPU; a 4 MiB SO_SNDBUF lets the kernel absorb many 64 KiB WRITE_BATCH
     * frames before the writer blocks, cutting push-cycle churn on this
     * high-throughput LAN path.  Env TSDB_RPC_SNDBUF_KB overrides (0 =
     * leave kernel autotuning alone). */
    {
        const char *e = getenv("TSDB_RPC_SNDBUF_KB");
        int kb = e ? atoi(e) : 4096;
        if (kb > 0) { int b = kb * 1024; setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &b, sizeof(b)); }
    }

    /* Optional mutual TLS: wrap the connected fd client-side, presenting our
     * cert so a mutual-TLS peer accepts us.  Off by default — tls stays NULL
     * and the int-fd plaintext path runs unchanged. */
    tsdb_tls_conn_t *tls = NULL;
    if (rpc_tls_enabled()) {
        tsdb_tls_ctx_t *cc = rpc_tls_client_ctx();
        if (!cc || tsdb_tls_client_wrap(cc, fd, host, &tls) != 0) {
            close(fd); return NULL;
        }
    }

    tsdb_rpc_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) {
        if (tls) tsdb_tls_close(tls); else close(fd);
        return NULL;
    }
    conn->fd  = fd;
    conn->tls = tls;
    pthread_mutex_init(&conn->lock, NULL);
    conn->next_req_id = 1;
    snprintf(conn->addr, sizeof(conn->addr), "%s", addr);

    return conn;
}

void tsdb_rpc_conn_close(tsdb_rpc_conn_t *conn) {
    if (!conn) return;
    if (conn->tls) tsdb_tls_close(conn->tls);   /* closes underlying fd */
    else           close(conn->fd);
    pthread_mutex_destroy(&conn->lock);
    free(conn);
}

int tsdb_rpc_call(tsdb_rpc_conn_t *conn,
                  tsdb_rpc_type_t type,
                  const uint8_t *payload, uint32_t payload_len)
{
    uint8_t dummy[1];
    uint32_t resp_len = 0;
    return tsdb_rpc_call_recv(conn, type, payload, payload_len,
                              dummy, 1, &resp_len);
}

/* Lock-free request/response body shared by tsdb_rpc_call_recv and
 * tsdb_rpc_call_recv_to.  Caller holds conn->lock. */
static int call_recv_locked(tsdb_rpc_conn_t *conn,
                            tsdb_rpc_type_t type,
                            const uint8_t *payload, uint32_t payload_len,
                            uint8_t *resp_buf, uint32_t resp_cap,
                            uint32_t *resp_len)
{
    uint32_t req_id = conn->next_req_id++;

    /* Header on the stack + payload referenced in place; a single writev
     * ships [header][payload] without a malloc'd send buffer or a
     * full-payload memcpy.  Frame bytes are identical to tsdb_rpc_encode. */
    uint8_t sendhdr[TSDB_RPC_HDR_SIZE];
    encode_header(sendhdr, type, req_id, payload, payload_len);

    struct iovec iov[2];
    int iovcnt = 0;
    iov[iovcnt].iov_base = sendhdr;
    iov[iovcnt].iov_len  = TSDB_RPC_HDR_SIZE;
    iovcnt++;
    if (payload && payload_len > 0) {
        iov[iovcnt].iov_base = (void *)payload;
        iov[iovcnt].iov_len  = payload_len;
        iovcnt++;
    }

    if (writev_all(conn->fd, conn->tls, iov, iovcnt) < 0) {
        return TSDB_ERR_IO;
    }

    /* Read response header. */
    uint8_t hdr[TSDB_RPC_HDR_SIZE];
    if (read_full(conn->fd, conn->tls, hdr, TSDB_RPC_HDR_SIZE) < 0) {
        return TSDB_ERR_IO;
    }

    uint32_t rlen;
    memcpy(&rlen, hdr + 10, 4);

    /* One contiguous allocation for header+payload; read the body straight
     * in after the header (single malloc, no second buffer + memcpy). */
    size_t total = TSDB_RPC_HDR_SIZE + rlen;
    uint8_t *combined = malloc(total);
    if (!combined) { return TSDB_ERR_NOMEM; }
    memcpy(combined, hdr, TSDB_RPC_HDR_SIZE);
    if (rlen > 0 && read_full(conn->fd, conn->tls, combined + TSDB_RPC_HDR_SIZE, rlen) < 0) {
        free(combined);
        return TSDB_ERR_IO;
    }

    tsdb_rpc_msg_t resp = {0};
    int consumed = tsdb_rpc_decode(combined, total, &resp);
    if (consumed < 0) { free(combined); return TSDB_ERR_CORRUPT; }

    if (resp_len) *resp_len = resp.payload_len;
    if (resp_buf && resp_cap > 0 && resp.payload_len > 0) {
        uint32_t copy = resp.payload_len < resp_cap ? resp.payload_len : resp_cap;
        memcpy(resp_buf, resp.payload, copy);
    }

    int result = (resp.type == TSDB_RPC_ACK) ? TSDB_OK : TSDB_ERR_INTERNAL;
    free(combined);
    return result;
}

int tsdb_rpc_call_recv(tsdb_rpc_conn_t *conn,
                       tsdb_rpc_type_t type,
                       const uint8_t *payload, uint32_t payload_len,
                       uint8_t *resp_buf, uint32_t resp_cap,
                       uint32_t *resp_len)
{
    if (!conn) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&conn->lock);
    int rc = call_recv_locked(conn, type, payload, payload_len,
                              resp_buf, resp_cap, resp_len);
    pthread_mutex_unlock(&conn->lock);
    return rc;
}

/* Arm (timeout_ms > 0) or clear (timeout_ms == 0) the socket I/O
 * deadline.  0 restores the blocking default the data path relies on. */
static void conn_set_io_timeout(int fd, int timeout_ms) {
    struct timeval tv = { 0, 0 };
    if (timeout_ms > 0) {
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
    }
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

/* Acquire conn->lock but give up after timeout_ms so a hard-deadline RPC
 * can never park unbounded behind a no-timeout data-path round-trip on
 * the same pooled (CONNS_PER_PEER=1) socket.  Returns 0 on lock, -1 on
 * timeout.  Portable: pthread_mutex_timedlock where available (Linux),
 * else a trylock + short nanosleep spin to the deadline (macOS has no
 * pthread_mutex_timedlock). */
static int conn_lock_deadline(pthread_mutex_t *m, int timeout_ms) {
    if (timeout_ms <= 0) {                  /* unbounded — plain lock */
        pthread_mutex_lock(m);
        return 0;
    }
#if defined(__linux__)
    /* timedlock takes an absolute CLOCK_REALTIME deadline. */
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec  += timeout_ms / 1000;
    ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
    for (;;) {
        int rc = pthread_mutex_timedlock(m, &ts);
        if (rc == 0) return 0;
        if (rc == EINTR) continue;
        return -1;                          /* ETIMEDOUT (or hard error) */
    }
#else
    struct timespec dl;
    clock_gettime(CLOCK_MONOTONIC, &dl);
    dl.tv_sec  += timeout_ms / 1000;
    dl.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (dl.tv_nsec >= 1000000000L) { dl.tv_sec++; dl.tv_nsec -= 1000000000L; }
    for (;;) {
        int rc = pthread_mutex_trylock(m);
        if (rc == 0) return 0;
        if (rc != EBUSY) return -1;
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec > dl.tv_sec ||
            (now.tv_sec == dl.tv_sec && now.tv_nsec >= dl.tv_nsec))
            return -1;
        struct timespec nap = { 0, 1000000L };  /* 1 ms */
        nanosleep(&nap, NULL);
    }
#endif
}

/* tsdb_rpc_call_recv with a hard per-call socket deadline.
 *
 * Pool conns are blocking sockets with no recv timeout — right for the
 * data path (large streamed payloads under disk pressure), fatal for
 * the raft tick thread: a peer that dies silently (container stopped,
 * its IP simply vanishes, so no RST ever arrives) leaves write()
 * buffering into the void and read() parked forever.  Observed live as
 * three survivors frozen mid PreVote round after the leader was
 * stopped — no election ever fired.  The deadline is armed and cleared
 * under conn->lock so a concurrent data-path call never runs with our
 * timeout in effect. */
int tsdb_rpc_call_recv_to(tsdb_rpc_conn_t *conn,
                          tsdb_rpc_type_t type,
                          const uint8_t *payload, uint32_t payload_len,
                          uint8_t *resp_buf, uint32_t resp_cap,
                          uint32_t *resp_len, int timeout_ms)
{
    if (!conn) return TSDB_ERR_INVAL;

    if (conn_lock_deadline(&conn->lock, timeout_ms) != 0) return TSDB_ERR_IO;
    conn_set_io_timeout(conn->fd, timeout_ms);
    int rc = call_recv_locked(conn, type, payload, payload_len,
                              resp_buf, resp_cap, resp_len);
    conn_set_io_timeout(conn->fd, 0);
    pthread_mutex_unlock(&conn->lock);
    return rc;
}

/* ---- Write-batch payload encode / decode --------------------------------- */

int tsdb_rpc_encode_write_batch(uint8_t *buf, uint32_t cap,
                                const char *table_name,
                                int ncols, const int *col_types,
                                int nrows, const void **col_data)
{
    if (!buf || !table_name || !col_types || !col_data) return -1;

    uint8_t *p = buf;
    uint8_t *end = buf + cap;

#define CHECK(n) if (p + (n) > end) return -1

    /* table_name */
    uint8_t tnlen = (uint8_t)strlen(table_name);
    CHECK(1 + tnlen);
    *p++ = tnlen;
    memcpy(p, table_name, tnlen); p += tnlen;

    /* nrows, ncols */
    uint32_t nr = (uint32_t)nrows;
    CHECK(4 + 1);
    memcpy(p, &nr, 4); p += 4;
    *p++ = (uint8_t)ncols;

    /* col types */
    CHECK(ncols);
    for (int c = 0; c < ncols; c++) *p++ = (uint8_t)col_types[c];

    /* col data: columnar layout.
     * Symbol columns are length-prefixed strings to survive cross-
     * node symtab independence; the caller hands us a buffer that
     * starts with `[u32 total_bytes][u16 len][bytes]…`.  Other types
     * are still nrows * 8 bytes of raw codes. */
    for (int c = 0; c < ncols; c++) {
        if (col_types[c] == TSDB_TYPE_SYMBOL) {
            uint32_t total = 0;
            if (col_data[c]) memcpy(&total, col_data[c], 4);
            size_t sz = 4 + total;
            CHECK((int)sz);
            if (col_data[c]) memcpy(p, col_data[c], sz);
            else             memset(p, 0, 4); /* empty col */
            p += sz;
        } else {
            size_t sz = (size_t)nrows * 8;
            CHECK((int)sz);
            if (col_data[c]) memcpy(p, col_data[c], sz);
            else             memset(p, 0, sz);
            p += sz;
        }
    }
#undef CHECK

    return (int)(p - buf);
}

int tsdb_rpc_decode_write_batch(const uint8_t *buf, uint32_t len,
                                char *out_table, int table_cap,
                                int *out_ncols, int *out_col_types,
                                int *out_nrows, uint8_t **out_col_data)
{
    if (!buf || !out_table || !out_ncols || !out_col_types ||
        !out_nrows || !out_col_data) return -1;

    const uint8_t *p = buf;
    const uint8_t *end = buf + len;

#define NEED(n) if (p + (n) > end) return -1

    NEED(1);
    uint8_t tnlen = *p++;
    NEED(tnlen);
    int copy = tnlen < (uint8_t)(table_cap - 1) ? tnlen : table_cap - 1;
    memcpy(out_table, p, copy);
    out_table[copy] = '\0';
    p += tnlen;

    NEED(4 + 1);
    uint32_t nrows; memcpy(&nrows, p, 4); p += 4;
    uint8_t ncols = *p++;

    *out_nrows = (int)nrows;
    *out_ncols = (int)ncols;

    NEED(ncols);
    for (int c = 0; c < ncols; c++) {
        out_col_types[c] = *p++;
    }

    /* col_data points directly into payload buffer (no copy). */
    *out_col_data = (uint8_t *)p;

#undef NEED
    return 0;
}

/* ---- Schema-sync payload encode / decode --------------------------------- */

int tsdb_rpc_encode_schema(uint8_t *buf, uint32_t cap,
                           const char *table_name,
                           int ncols, const char **col_names,
                           const int *col_types, int ts_col_idx,
                           int partition_unit, int block_points,
                           int sort_by_tag_col)
{
    if (!buf || !table_name || !col_names || !col_types) return -1;

    uint8_t *p = buf;
    uint8_t *end = buf + cap;

#define CHECK(n) if (p + (n) > end) return -1

    uint8_t tnlen = (uint8_t)strlen(table_name);
    CHECK(1 + tnlen + 1 + 1);
    *p++ = tnlen;
    memcpy(p, table_name, tnlen); p += tnlen;
    *p++ = (uint8_t)ncols;
    *p++ = (uint8_t)(ts_col_idx >= 0 ? ts_col_idx : 0);

    for (int c = 0; c < ncols; c++) {
        uint8_t nlen = col_names[c] ? (uint8_t)strlen(col_names[c]) : 0;
        CHECK(1 + nlen + 1);
        *p++ = nlen;
        if (nlen > 0) { memcpy(p, col_names[c], nlen); p += nlen; }
        *p++ = (uint8_t)col_types[c];
    }

    /* v2 wire tail: partition_unit + block_points + sort_by_tag_col.
     * Always emitted by new encoders.  Old decoders ignore trailing
     * bytes after the per-col loop, so the addition is wire-safe. */
    CHECK(1 + 4 + 4);
    *p++ = (uint8_t)partition_unit;
    {
        uint32_t bp = (uint32_t)(block_points > 0 ? block_points : 0);
        memcpy(p, &bp, 4); p += 4;
    }
    {
        int32_t sc = (int32_t)sort_by_tag_col;
        memcpy(p, &sc, 4); p += 4;
    }

#undef CHECK
    return (int)(p - buf);
}

int tsdb_rpc_decode_schema(const uint8_t *buf, uint32_t len,
                           char *out_table, int table_cap,
                           int *out_ncols, char out_col_names[][64],
                           int *out_col_types, int *out_ts_col_idx,
                           int *out_partition_unit, int *out_block_points,
                           int *out_sort_by_tag_col)
{
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;

    /* v2 tail defaults — applied if the payload has no tail (legacy sender). */
    if (out_partition_unit)  *out_partition_unit  = 0;   /* TSDB_PARTITION_DAY */
    if (out_block_points)    *out_block_points    = 0;   /* engine default */
    if (out_sort_by_tag_col) *out_sort_by_tag_col = -1;  /* off */

#define NEED(n) if (p + (n) > end) return -1

    NEED(1);
    uint8_t tnlen = *p++;
    NEED(tnlen);
    int copy = tnlen < (uint8_t)(table_cap - 1) ? tnlen : table_cap - 1;
    memcpy(out_table, p, copy);
    out_table[copy] = '\0';
    p += tnlen;

    NEED(2);
    uint8_t ncols = *p++;
    uint8_t ts_idx = *p++;
    *out_ncols     = (int)ncols;
    *out_ts_col_idx = (int)ts_idx;

    for (int c = 0; c < ncols; c++) {
        NEED(1);
        uint8_t nlen = *p++;
        NEED(nlen + 1);
        int cn = nlen < 63 ? nlen : 63;
        memcpy(out_col_names[c], p, cn);
        out_col_names[c][cn] = '\0';
        p += nlen;
        out_col_types[c] = *p++;
    }

    /* v2 tail: present iff at least 9 bytes remain. */
    if ((size_t)(end - p) >= 1 + 4 + 4) {
        uint8_t pu = *p++;
        if (out_partition_unit) *out_partition_unit = (int)pu;
        uint32_t bp = 0;
        memcpy(&bp, p, 4); p += 4;
        if (out_block_points) *out_block_points = (int)bp;
        int32_t sc = -1;
        memcpy(&sc, p, 4); p += 4;
        if (out_sort_by_tag_col) *out_sort_by_tag_col = (int)sc;
    }
#undef NEED
    return 0;
}

/* ---- APPLY_TRUNCATE / APPLY_DELETE_RANGE encode & decode ----------------- */

int tsdb_rpc_encode_truncate(uint8_t *buf, uint32_t cap, const char *table_name) {
    if (!buf || !table_name) return -1;
    uint8_t tnlen = (uint8_t)strlen(table_name);
    if ((uint32_t)tnlen + 1 > cap) return -1;
    buf[0] = tnlen;
    memcpy(buf + 1, table_name, tnlen);
    return 1 + tnlen;
}

int tsdb_rpc_decode_truncate(const uint8_t *buf, uint32_t len,
                             char *out_table, int table_cap) {
    if (!buf || len < 1 || !out_table || table_cap <= 0) return -1;
    uint8_t tnlen = buf[0];
    if ((uint32_t)tnlen + 1 > len) return -1;
    int copy = tnlen < (uint8_t)(table_cap - 1) ? tnlen : table_cap - 1;
    memcpy(out_table, buf + 1, copy);
    out_table[copy] = '\0';
    return 0;
}

int tsdb_rpc_encode_delete_range(uint8_t *buf, uint32_t cap,
                                 const char *table_name,
                                 int64_t cutoff_ns,
                                 int op_lt, int inclusive)
{
    if (!buf || !table_name) return -1;
    uint8_t tnlen = (uint8_t)strlen(table_name);
    uint32_t need = 1u + tnlen + 8u + 1u + 1u;
    if (need > cap) return -1;
    buf[0] = tnlen;
    memcpy(buf + 1, table_name, tnlen);
    memcpy(buf + 1 + tnlen, &cutoff_ns, 8);
    buf[1 + tnlen + 8] = (uint8_t)(op_lt ? 1 : 0);
    buf[1 + tnlen + 9] = (uint8_t)(inclusive ? 1 : 0);
    return (int)need;
}

int tsdb_rpc_decode_delete_range(const uint8_t *buf, uint32_t len,
                                 char *out_table, int table_cap,
                                 int64_t *out_cutoff_ns,
                                 int *out_op_lt, int *out_inclusive)
{
    if (!buf || len < 1 || !out_table || table_cap <= 0) return -1;
    uint8_t tnlen = buf[0];
    uint32_t need = 1u + tnlen + 8u + 1u + 1u;
    if (need > len) return -1;
    int copy = tnlen < (uint8_t)(table_cap - 1) ? tnlen : table_cap - 1;
    memcpy(out_table, buf + 1, copy);
    out_table[copy] = '\0';
    int64_t cutoff;
    memcpy(&cutoff, buf + 1 + tnlen, 8);
    if (out_cutoff_ns)  *out_cutoff_ns  = cutoff;
    if (out_op_lt)      *out_op_lt      = buf[1 + tnlen + 8] ? 1 : 0;
    if (out_inclusive)  *out_inclusive  = buf[1 + tnlen + 9] ? 1 : 0;
    return 0;
}

/* APPLY_CATALOG_QTL — carries a whole QTL statement so the peer can
 * replay it into its own catalog (e.g. CREATE DATABASE, CREATE VTABLE).
 * Payload layout: qtl_len u32 LE + raw UTF-8 bytes, no trailing NUL.
 */
int tsdb_rpc_encode_catalog_qtl(uint8_t *buf, uint32_t cap, const char *qtl) {
    if (!buf || !qtl) return -1;
    size_t qlen = strlen(qtl);
    if (qlen > 0xFFFFFFFFu) return -1;
    uint32_t need = 4u + (uint32_t)qlen;
    if (need > cap) return -1;
    uint32_t q32 = (uint32_t)qlen;
    memcpy(buf, &q32, 4);
    memcpy(buf + 4, qtl, qlen);
    return (int)need;
}

int tsdb_rpc_decode_catalog_qtl(const uint8_t *buf, uint32_t len,
                                char *out_qtl, int qtl_cap)
{
    if (!buf || len < 4 || !out_qtl || qtl_cap <= 0) return -1;
    uint32_t qlen;
    memcpy(&qlen, buf, 4);
    if (4u + qlen > len) return -1;
    int copy = (int)qlen < (qtl_cap - 1) ? (int)qlen : (qtl_cap - 1);
    memcpy(out_qtl, buf + 4, (size_t)copy);
    out_qtl[copy] = '\0';
    return 0;
}
