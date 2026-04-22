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
#include "../server/metrics.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
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

/* Full read: keep going until all bytes received or error. */
static int read_full(int fd, uint8_t *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t r = read(fd, buf + done, len - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

/* Full write: keep going until all bytes sent or error. */
static int write_full(int fd, const uint8_t *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t w = write(fd, buf + done, len - done);
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    return 0;
}

/* ---- Per-connection handler ---------------------------------------------- */

/* Recv a complete frame into caller-allocated buffer. */
static int recv_frame(int fd, uint8_t *hdr_buf, uint8_t **payload_out,
                      uint32_t *payload_len_out, tsdb_rpc_msg_t *msg)
{
    if (read_full(fd, hdr_buf, TSDB_RPC_HDR_SIZE) < 0) return -1;

    uint32_t magic;
    memcpy(&magic, hdr_buf, 4);
    if (magic != TSDB_RPC_MAGIC) return -1;

    uint32_t plen;
    memcpy(&plen, hdr_buf + 10, 4);

    uint8_t *payload = NULL;
    if (plen > 0) {
        payload = malloc(plen);
        if (!payload) return -1;
        if (read_full(fd, payload, plen) < 0) { free(payload); return -1; }
    }

    /* Build a contiguous buffer for tsdb_rpc_decode. */
    size_t total = TSDB_RPC_HDR_SIZE + plen;
    uint8_t *combined = malloc(total);
    if (!combined) { free(payload); return -1; }
    memcpy(combined, hdr_buf, TSDB_RPC_HDR_SIZE);
    if (plen > 0) memcpy(combined + TSDB_RPC_HDR_SIZE, payload, plen);
    free(payload);

    int consumed = tsdb_rpc_decode(combined, total, msg);
    if (consumed <= 0) { free(combined); return -1; }

    /* Caller owns combined. */
    *payload_out     = combined;
    *payload_len_out = plen;
    return 0;
}

/* Send a simple ACK or ERR response. */
static void send_reply(int fd, tsdb_rpc_type_t type, uint32_t req_id,
                       const uint8_t *body, uint32_t body_len) {
    uint8_t buf[TSDB_RPC_HDR_SIZE + 64];
    int n = tsdb_rpc_encode(buf, sizeof(buf), type, req_id, body, body_len);
    if (n > 0) write_full(fd, buf, (size_t)n);
}

/* Server handler args. */
typedef struct {
    int                  fd;
    tsdb_db_t           *db;
    tsdb_node_manager_t *node_mgr;
} handler_args_t;

/* Handle one client connection in its own thread. */
static void *connection_handler(void *arg) {
    handler_args_t *ha = (handler_args_t *)arg;
    int fd = ha->fd;
    tsdb_db_t *db = ha->db;
    tsdb_node_manager_t *node_mgr = ha->node_mgr;
    free(ha);

    uint8_t hdr_buf[TSDB_RPC_HDR_SIZE];
    for (;;) {
        uint8_t *combined = NULL;
        uint32_t plen     = 0;
        tsdb_rpc_msg_t msg = {0};

        if (recv_frame(fd, hdr_buf, &combined, &plen, &msg) < 0) break;

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
            send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
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

                int rc = tsdb_rpc_decode_schema(msg.payload, msg.payload_len,
                                                table_name, sizeof(table_name),
                                                &ncols, col_names, col_types, &ts_col_idx);
                if (rc == 0 && ncols > 0 && ncols <= TSDB_MAX_COLS) {
                    tsdb_col_t cols[TSDB_MAX_COLS];
                    for (int i = 0; i < ncols; i++) {
                        cols[i].name = col_names[i];
                        cols[i].type = (tsdb_type_t)col_types[i];
                    }
                    const char *ts_name = (ts_col_idx >= 0 && ts_col_idx < ncols)
                                         ? col_names[ts_col_idx] : col_names[0];
                    /* Use _local variant to avoid re-syncing to other nodes. */
                    tsdb_create_table_local(db, table_name, cols, ncols, ts_name);
                }
                send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
            } else {
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

        case TSDB_RPC_WRITE_BATCH: {
            /* Track whether the entire decode→open→begin→commit chain
             * actually landed the rows so we can reflect the truth in
             * the RPC reply — previously the receiver unconditionally
             * ACKed even when the table was missing or batch_commit
             * failed, so the sender counted it as a successful replica
             * and dropped the rows silently.  Under concurrent writes
             * this is the smoking gun for the 3-8% row-loss we've
             * observed: send-side ack_count climbs, receive-side
             * memtable stays empty. */
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
                        /* Serialize concurrent replicate RPCs on the
                         * same table.  See tsdb_table_lock_write() doc. */
                        tsdb_table_lock_write(tbl);
                        tsdb_batch_t *batch = NULL;
                        if (tsdb_batch_begin(tbl, &batch) == TSDB_OK) {
                            /* Mark as local-only to prevent re-replication loop. */
                            tsdb_batch_set_local_only(batch);
                            for (int row = 0; row < nrows; row++) {
                                /* Compute per-col offsets. */
                                int col_off[TSDB_MAX_COLS];
                                int off = 0;
                                for (int c = 0; c < ncols; c++) {
                                    col_off[c] = off;
                                    switch (col_types[c]) {
                                    case TSDB_TYPE_SYMBOL: off += 4 * nrows; break;
                                    default:               off += 8 * nrows; break;
                                    }
                                }
                                (void)col_off; /* resolved below */

                                /* ts column first. */
                                int64_t ts_val = 0;
                                int ts_ci = -1;
                                for (int c = 0; c < ncols; c++) {
                                    if (col_types[c] == TSDB_TYPE_TIMESTAMP) {
                                        ts_ci = c;
                                        break;
                                    }
                                }

                                /* Calculate offsets for each column. */
                                int base[TSDB_MAX_COLS];
                                {
                                    int boff = 0;
                                    for (int c = 0; c < ncols; c++) {
                                        base[c] = boff;
                                        int w = (col_types[c] == TSDB_TYPE_SYMBOL) ? 4 : 8;
                                        boff += w * nrows;
                                    }
                                }

                                if (ts_ci >= 0) {
                                    memcpy(&ts_val,
                                           col_data[0] + base[ts_ci] + row * 8, 8);
                                }
                                tsdb_batch_row_ts(batch, ts_val);

                                for (int c = 0; c < ncols; c++) {
                                    if (col_types[c] == TSDB_TYPE_TIMESTAMP) continue;
                                    int stride = (col_types[c] == TSDB_TYPE_SYMBOL) ? 4 : 8;
                                    uint8_t *ptr = col_data[0] + base[c] + row * stride;
                                    switch (col_types[c]) {
                                    case TSDB_TYPE_INT64: {
                                        int64_t v; memcpy(&v, ptr, 8);
                                        tsdb_batch_row_i64(batch, c, v);
                                        break;
                                    }
                                    case TSDB_TYPE_FLOAT64: {
                                        double v; memcpy(&v, ptr, 8);
                                        tsdb_batch_row_f64(batch, c, v);
                                        break;
                                    }
                                    default: break;
                                    }
                                }
                                tsdb_batch_row_end(batch);
                            }
                            if (tsdb_batch_commit(batch) == TSDB_OK) write_ok = 1;
                        }
                        tsdb_table_unlock_write(tbl);
                    }
                }
            }
            if (write_ok) {
                send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_replicate_recv_ok_total");
            } else {
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                tsdb_metric_inc("qengine_replicate_recv_err_total");
            }
            break;
        }

        case TSDB_RPC_FED_QUERY:
            /* Federation query: run QTL on local DB, encode result, send back. */
            if (db && msg.payload_len >= 2) {
                uint16_t qlen;
                memcpy(&qlen, msg.payload, 2);
                if ((uint32_t)qlen + 2 <= msg.payload_len && qlen < 4096) {
                    char qtl[4096];
                    memcpy(qtl, msg.payload + 2, qlen);
                    qtl[qlen] = '\0';

                    tsdb_result_t *qr = NULL;
                    int qrc = tsdb_query(db, qtl, &qr);

                    if (qrc == TSDB_OK && qr) {
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
                                                             msg.req_id,
                                                             rbuf, (uint32_t)encoded);
                                    if (fn > 0) write_full(fd, frame, (size_t)fn);
                                    free(frame);
                                }
                            } else {
                                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                            }
                            free(rbuf);
                        } else {
                            send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                        }
                        tsdb_result_free(qr);
                    } else {
                        send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                    }
                } else {
                    send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
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
                    send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                else
                    send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            } else {
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
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
                        send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                    else
                        send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                } else {
                    send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
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
                        send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                    else
                        send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                } else {
                    send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
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
                        send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
                    else
                        send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                } else {
                    send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
                }
            } else {
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            }
            break;

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
                send_reply(fd, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
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
                send_reply(fd, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
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
                send_reply(fd, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
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
                send_reply(fd, TSDB_RPC_ACK, msg.req_id, rb, rn);
            else
                send_reply(fd, TSDB_RPC_ERR, msg.req_id, NULL, 0);
            break;
        }

        default:
            send_reply(fd, TSDB_RPC_ACK, msg.req_id, NULL, 0);
            break;
        }

        free(combined);
    }

    close(fd);
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
};

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

        handler_args_t *ha = malloc(sizeof(*ha));
        if (!ha) { close(client_fd); continue; }
        ha->fd       = client_fd;
        ha->db       = srv->db;
        ha->node_mgr = srv->node_mgr;

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, connection_handler, ha) != 0) {
            free(ha);
            close(client_fd);
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

    pthread_create(&srv->accept_thread, NULL, accept_loop, srv);
    return srv;
}

void tsdb_rpc_server_stop(tsdb_rpc_server_t *srv) {
    if (!srv) return;
    srv->running = 0;
    pthread_join(srv->accept_thread, NULL);
    close(srv->listen_fd);
    free(srv);
}

int tsdb_rpc_server_port(tsdb_rpc_server_t *srv) {
    return srv ? srv->port : -1;
}

/* ---- Client connection --------------------------------------------------- */

struct tsdb_rpc_conn {
    int             fd;
    pthread_mutex_t lock;
    uint32_t        next_req_id;
    char            addr[TSDB_ADDR_MAX];
};

tsdb_rpc_conn_t *tsdb_rpc_connect(const char *addr, int timeout_ms) {
    char host[128];
    int  port;
    if (parse_addr(addr, host, sizeof(host), &port) < 0) return NULL;

    if (timeout_ms <= 0) timeout_ms = 2000;

    struct addrinfo hints = {0}, *res = NULL;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);
    if (getaddrinfo(host, portstr, &hints, &res) != 0) return NULL;

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { freeaddrinfo(res); return NULL; }

    /* Set non-blocking for connect with timeout. */
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);

    connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);

    struct pollfd pfd = { fd, POLLOUT, 0 };
    if (poll(&pfd, 1, timeout_ms) <= 0 || !(pfd.revents & POLLOUT)) {
        close(fd); return NULL;
    }

    /* Check for connect error. */
    int err = 0;
    socklen_t errlen = sizeof(err);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errlen);
    if (err) { close(fd); return NULL; }

    /* Restore blocking. */
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) & ~O_NONBLOCK);

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    tsdb_rpc_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) { close(fd); return NULL; }
    conn->fd = fd;
    pthread_mutex_init(&conn->lock, NULL);
    conn->next_req_id = 1;
    snprintf(conn->addr, sizeof(conn->addr), "%s", addr);

    return conn;
}

void tsdb_rpc_conn_close(tsdb_rpc_conn_t *conn) {
    if (!conn) return;
    close(conn->fd);
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

int tsdb_rpc_call_recv(tsdb_rpc_conn_t *conn,
                       tsdb_rpc_type_t type,
                       const uint8_t *payload, uint32_t payload_len,
                       uint8_t *resp_buf, uint32_t resp_cap,
                       uint32_t *resp_len)
{
    if (!conn) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&conn->lock);

    uint32_t req_id = conn->next_req_id++;
    size_t buf_size = TSDB_RPC_HDR_SIZE + payload_len;
    uint8_t *sendbuf = malloc(buf_size);
    if (!sendbuf) { pthread_mutex_unlock(&conn->lock); return TSDB_ERR_NOMEM; }

    int n = tsdb_rpc_encode(sendbuf, buf_size, type, req_id, payload, payload_len);
    if (n < 0) {
        free(sendbuf);
        pthread_mutex_unlock(&conn->lock);
        return TSDB_ERR_INTERNAL;
    }

    if (write_full(conn->fd, sendbuf, (size_t)n) < 0) {
        free(sendbuf);
        pthread_mutex_unlock(&conn->lock);
        return TSDB_ERR_IO;
    }
    free(sendbuf);

    /* Read response header. */
    uint8_t hdr[TSDB_RPC_HDR_SIZE];
    if (read_full(conn->fd, hdr, TSDB_RPC_HDR_SIZE) < 0) {
        pthread_mutex_unlock(&conn->lock);
        return TSDB_ERR_IO;
    }

    uint32_t rlen;
    memcpy(&rlen, hdr + 10, 4);

    /* Read response payload. */
    uint8_t *rbuf = NULL;
    if (rlen > 0) {
        rbuf = malloc(rlen);
        if (!rbuf) { pthread_mutex_unlock(&conn->lock); return TSDB_ERR_NOMEM; }
        if (read_full(conn->fd, rbuf, rlen) < 0) {
            free(rbuf);
            pthread_mutex_unlock(&conn->lock);
            return TSDB_ERR_IO;
        }
    }

    /* Parse complete response frame. */
    size_t total = TSDB_RPC_HDR_SIZE + rlen;
    uint8_t *combined = malloc(total);
    if (!combined) { free(rbuf); pthread_mutex_unlock(&conn->lock); return TSDB_ERR_NOMEM; }
    memcpy(combined, hdr, TSDB_RPC_HDR_SIZE);
    if (rlen > 0) memcpy(combined + TSDB_RPC_HDR_SIZE, rbuf, rlen);
    free(rbuf);

    tsdb_rpc_msg_t resp = {0};
    int consumed = tsdb_rpc_decode(combined, total, &resp);
    if (consumed < 0) { free(combined); pthread_mutex_unlock(&conn->lock); return TSDB_ERR_CORRUPT; }

    if (resp_len) *resp_len = resp.payload_len;
    if (resp_buf && resp_cap > 0 && resp.payload_len > 0) {
        uint32_t copy = resp.payload_len < resp_cap ? resp.payload_len : resp_cap;
        memcpy(resp_buf, resp.payload, copy);
    }

    int result = (resp.type == TSDB_RPC_ACK) ? TSDB_OK : TSDB_ERR_INTERNAL;
    free(combined);
    pthread_mutex_unlock(&conn->lock);
    return result;
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

    /* col data: columnar layout */
    for (int c = 0; c < ncols; c++) {
        int stride = (col_types[c] == TSDB_TYPE_SYMBOL) ? 4 : 8;
        size_t sz = (size_t)nrows * stride;
        CHECK((int)sz);
        if (col_data[c]) memcpy(p, col_data[c], sz);
        else             memset(p, 0, sz);
        p += sz;
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
                           const int *col_types, int ts_col_idx)
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
#undef CHECK
    return (int)(p - buf);
}

int tsdb_rpc_decode_schema(const uint8_t *buf, uint32_t len,
                           char *out_table, int table_cap,
                           int *out_ncols, char out_col_names[][64],
                           int *out_col_types, int *out_ts_col_idx)
{
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
