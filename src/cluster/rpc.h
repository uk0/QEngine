/* rpc.h — binary TCP RPC for cluster communication.
 *
 * Frame format (all little-endian):
 *   magic     u32  = 'TSRP'  (0x50525354)
 *   ver       u8   = 1
 *   rpc_id    u8   (RPC type)
 *   req_id    u32  (monotonic request ID for request/response matching)
 *   payload_len u32
 *   crc32     u32  (covers ver..payload)
 *   payload   [payload_len bytes]
 *
 * Total header = 4+1+1+4+4+4 = 18 bytes.
 *
 * RPC types:
 *   WRITE_BATCH   - primary → replica: replicate a batch of rows
 *   READ_BLOCK    - query forwarding (future)
 *   QUERY         - query forwarding (future)
 *   HEARTBEAT     - cluster keep-alive
 *   SCHEMA_SYNC   - create-table propagation
 *   ACK           - generic positive response
 *   ERR           - generic error response
 */
#ifndef TSDB_CLUSTER_RPC_H
#define TSDB_CLUSTER_RPC_H

#include "node.h"
#include "../../include/tsdb.h"
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Magic number. */
#define TSDB_RPC_MAGIC  0x50525354u  /* "TSRP" LE */
#define TSDB_RPC_VER    1
#define TSDB_RPC_HDR_SIZE 18u

/* RPC type IDs. */
typedef enum {
    TSDB_RPC_WRITE_BATCH  = 1,
    TSDB_RPC_READ_BLOCK   = 2,
    TSDB_RPC_QUERY        = 3,
    TSDB_RPC_HEARTBEAT    = 4,
    TSDB_RPC_SCHEMA_SYNC  = 5,
    TSDB_RPC_ACK          = 6,
    TSDB_RPC_ERR          = 7,
    TSDB_RPC_FED_QUERY       = 8,  /* federation query: QTL → encoded result */
    TSDB_RPC_RAW_BLOCK_PUSH  = 9,  /* raw compressed block replica sync */
    TSDB_RPC_RAW_BLOCK_ACK   = 10, /* per-block ack */
    TSDB_RPC_APPLY_TRUNCATE    = 11, /* cluster broadcast: apply TRUNCATE TABLE locally */
    TSDB_RPC_APPLY_DELETE_RANGE = 12, /* cluster broadcast: apply partition-level DELETE locally */
    TSDB_RPC_APPLY_CATALOG_QTL  = 13, /* cluster broadcast: run a catalog QTL statement locally */
    TSDB_RPC_RAFT_REQUEST_VOTE  = 14, /* candidate asks peer for a vote   */
    TSDB_RPC_RAFT_APPEND_ENTRIES = 15, /* leader replicates log to follower */
    TSDB_RPC_RAFT_INSTALL_SNAPSHOT = 16, /* leader ships snapshot bytes to a
                                          far-behind follower */
    TSDB_RPC_RAFT_PRE_VOTE      = 17, /* PreVote probe (§9.6): hypothetical
                                         RequestVote that doesn't mutate
                                         currentTerm on either side. */
    TSDB_RPC_FED_INGEST         = 18, /* cross-DC async replication — same
                                         payload as WRITE_BATCH, but the
                                         receiver applies with local_only
                                         and bumps qengine_dr_recv_*.
                                         See src/federation/dr_forwarder.c */
    TSDB_RPC_CATALOG_DUMP       = 19  /* data-node startup pull: receiver
                                         (master) returns the verbatim
                                         contents of databases.log +
                                         groups.log + stables.log +
                                         child_tables.log so a data peer
                                         that missed broadcasts during a
                                         crash window can self-heal.  See
                                         tsdb_catalog_pull_from_master() in
                                         storage/catalog_sync.c. */,
    TSDB_RPC_WRITE_BATCH_LZ     = 20  /* WRITE_BATCH with an lzlite-compressed
                                         payload: [u32 orig_len LE][lzlite bytes].
                                         Receiver decompresses then applies
                                         exactly as WRITE_BATCH.  Sender emits
                                         this (instead of type 1) only when
                                         compression saved >10% and the payload
                                         cleared the size threshold; gated by
                                         TSDB_REPLICATION_COMPRESS (default on).
                                         Kafka-style batch compression — cuts
                                         tcp_sendmsg bytes + LAN/WAN bandwidth. */,
    TSDB_RPC_DDL_FORWARD        = 21  /* data-node → master: run this catalog
                                         QTL through the master's FULL query
                                         path (raft propose + broadcast) and
                                         return the status text as the ACK
                                         payload.  Replaces the old HTTP
                                         /sql proxy, which could not pass the
                                         dashboard auth gate (session cookies
                                         are node-local).  Payload format is
                                         the same as APPLY_CATALOG_QTL. */,
    TSDB_RPC_FED_QUERY_LOCAL    = 22, /* FED_QUERY variant executed in
                                         scatter-local mode: the handler sets
                                         tsdb_g_scatter_local_mode around
                                         tsdb_query so a super-table read (a)
                                         never re-scatters and (b) covers only
                                         the children whose primary alive
                                         hashring owner is the receiving node.
                                         Used by the cluster-wide stable
                                         aggregation coordinator; wire format
                                         identical to FED_QUERY. */
    TSDB_RPC_LOCAL_TABLE_STATS  = 23  /* anti-entropy peer probe: "how many
                                         rows do YOU YOURSELF hold for this
                                         table, and what is your newest ts?".
                                         The receiver answers from its own
                                         storage (tsdb_cluster_local_table_stats)
                                         — no query engine, no scatter, no
                                         hashring filter.

                                         Request payload:
                                           name_len u8
                                           name     [name_len bytes]
                                         Response: ACK with a FIXED 16-byte body
                                           count  u64 LE
                                           max_ts i64 LE   (INT64_MIN = empty)

                                         A receiver that predates this opcode
                                         falls through the dispatch switch to
                                         `default:`, which on THOSE builds ACKs
                                         with an EMPTY body — so "ACK whose body
                                         is not 16 bytes" is the unambiguous
                                         too-old signal and the caller degrades
                                         to the legacy FED_QUERY probe.  Builds
                                         from 2026-07-31 answer an unknown
                                         opcode with ERR_UNSUPPORTED instead;
                                         that maps to the same
                                         TSDB_ERR_UNSUPPORTED and the same
                                         fallback. */,
    TSDB_RPC_ERR_UNSUPPORTED    = 24  /* reply: "this build does not implement
                                         that opcode".

                                         The dispatch `default:` used to ACK
                                         with an empty body, i.e. tell a peer
                                         on a NEWER build that whatever it just
                                         asked for had been done.  Nothing on
                                         this transport negotiates versions —
                                         tsdb_rpc_decode reads `ver` and
                                         immediately (void)s it — so that arm
                                         was the entire forward-compatibility
                                         story, answering "yes" by default.

                                         It has to be an error, and a
                                         DISTINGUISHABLE one: the
                                         LOCAL_TABLE_STATS probe has to keep
                                         telling "too old for this opcode"
                                         (fall back to the legacy query) apart
                                         from "knows it, could not answer"
                                         (skip the peer).  A plain
                                         TSDB_RPC_ERR would collapse the two.
                                         Clients map this to
                                         TSDB_ERR_UNSUPPORTED; a client too old
                                         to know the code sees "not ACK" and
                                         fails, which is the safe direction. */
} tsdb_rpc_type_t;

/* Parsed RPC message (received side). */
typedef struct {
    tsdb_rpc_type_t type;
    uint32_t        req_id;
    uint32_t        payload_len;
    uint8_t        *payload;   /* points into recv buffer, NOT owned */
} tsdb_rpc_msg_t;

/* ---- Server -------------------------------------------------------------- */

/* Forward declaration of DB handle. */
typedef struct tsdb_db tsdb_db_t;

/* Opaque RPC server. */
typedef struct tsdb_rpc_server tsdb_rpc_server_t;

/*
 * Create and start a listening RPC server on `bind_addr` ("host:port").
 * db is used to service WRITE_BATCH and SCHEMA_SYNC requests.
 * node_mgr is consulted / updated on HEARTBEAT messages.
 */
tsdb_rpc_server_t *tsdb_rpc_server_new(const char *bind_addr,
                                       tsdb_db_t *db,
                                       tsdb_node_manager_t *node_mgr);

/* Stop accepting connections and free resources. */
void tsdb_rpc_server_stop(tsdb_rpc_server_t *srv);

/* Return the port the server is listening on (useful when port was 0). */
int tsdb_rpc_server_port(tsdb_rpc_server_t *srv);

/* ---- Client connection --------------------------------------------------- */

/* Opaque client connection handle (one per remote node, persistent). */
typedef struct tsdb_rpc_conn tsdb_rpc_conn_t;

/*
 * Open a client connection to `addr` ("host:port").
 * Blocks until connected or timeout_ms elapsed (0 = use default 2000ms).
 * Returns NULL on failure.
 */
tsdb_rpc_conn_t *tsdb_rpc_connect(const char *addr, int timeout_ms);

/* Close and free a connection. */
void tsdb_rpc_conn_close(tsdb_rpc_conn_t *conn);

/* 1 once this conn has been retired — a reply arrived that did not pair with
 * the request that was sent, so the stream is out of step and every later call
 * on it fails closed.  Pool owners must not hand a retired conn back out; the
 * flag is sticky and only a fresh connect clears it. */
int  tsdb_rpc_conn_is_desynced(const tsdb_rpc_conn_t *conn);

/*
 * Send an RPC request and wait synchronously for an ACK/ERR response.
 * payload / payload_len: request body.
 * Returns TSDB_OK on ACK, TSDB_ERR_IO on timeout/socket error,
 * TSDB_ERR_UNSUPPORTED if the remote does not implement the opcode,
 * TSDB_ERR_INTERNAL if remote returned ERR,
 * TSDB_ERR_CORRUPT on a bad frame or a reply carrying another request's id.
 */
int tsdb_rpc_call(tsdb_rpc_conn_t *conn,
                  tsdb_rpc_type_t type,
                  const uint8_t *payload, uint32_t payload_len);

/*
 * Like tsdb_rpc_call but also returns the response payload.
 * resp_buf / resp_cap: caller-supplied buffer.
 * *resp_len set to actual bytes on success.
 */
int tsdb_rpc_call_recv(tsdb_rpc_conn_t *conn,
                       tsdb_rpc_type_t type,
                       const uint8_t *payload, uint32_t payload_len,
                       uint8_t *resp_buf, uint32_t resp_cap,
                       uint32_t *resp_len);

/*
 * Same as tsdb_rpc_call_recv but with a hard per-call socket deadline
 * (SO_RCVTIMEO/SO_SNDTIMEO armed for the duration of the exchange,
 * cleared after).  Used by the raft senders so a silently-dead peer
 * cannot park the tick thread in a blocking read.  timeout_ms <= 0
 * behaves like the plain variant.
 */
int tsdb_rpc_call_recv_to(tsdb_rpc_conn_t *conn,
                          tsdb_rpc_type_t type,
                          const uint8_t *payload, uint32_t payload_len,
                          uint8_t *resp_buf, uint32_t resp_cap,
                          uint32_t *resp_len, int timeout_ms);

/* Bound for the write-replication sends (WRITE_BATCH fanout + RAW_BLOCK_PUSH).
 * These hold the single per-peer pooled conn->lock (CONNS_PER_PEER=1) for a
 * whole round-trip; a no-timeout send against a peer wedged on its own
 * batch_mu pinned that lock forever and leaked the sender thread.  Generous
 * (well above a normal apply) but far below the fanout-wait deadline so a
 * real ring resolves in seconds, not tens of seconds. */
#define TSDB_REPL_SEND_TIMEOUT_MS 10000

/* ---- Wire helpers (used by gossip and replica layers) -------------------- */

/*
 * Encode a frame into buf.  buf must hold at least
 * TSDB_RPC_HDR_SIZE + payload_len bytes.
 * Returns total bytes written (header + payload).
 */
int tsdb_rpc_encode(uint8_t *buf, size_t buf_cap,
                    tsdb_rpc_type_t type, uint32_t req_id,
                    const uint8_t *payload, uint32_t payload_len);

/*
 * Attempt to parse a frame from buf[0..len).
 * On success fills *msg and returns the total bytes consumed.
 * Returns 0 if need more data, -1 on corrupt frame.
 * msg->payload points into buf — copy before next call.
 */
int tsdb_rpc_decode(const uint8_t *buf, size_t len, tsdb_rpc_msg_t *msg);

/* ---- Write-batch payload helpers ---------------------------------------- */

/*
 * Encode a WRITE_BATCH payload.
 * Format:
 *   table_name_len  u8
 *   table_name      [table_name_len bytes]
 *   nrows           u32  (number of rows)
 *   ncols           u8
 *   per-col-type    [ncols bytes, tsdb_type_t values]
 *   per-row data:   for each row: [ncols × (4 or 8 bytes depending on type)]
 *     TIMESTAMP/INT64/FLOAT64 → 8 bytes LE
 *     SYMBOL                  → 4 bytes LE (dict code)
 *     NOTE: symbol strings are sent as u32 codes; schema must be in sync.
 *
 * Returns bytes written, or -1 if buffer too small.
 */
int tsdb_rpc_encode_write_batch(uint8_t *buf, uint32_t cap,
                                const char *table_name,
                                int ncols, const int *col_types,
                                int nrows, const void **col_data);

/*
 * Decode WRITE_BATCH payload into column pointers.
 * Caller provides pre-allocated out_col_data[ncols] void* array.
 * Returns 0 on success, -1 on parse error.
 */
int tsdb_rpc_decode_write_batch(const uint8_t *buf, uint32_t len,
                                char *out_table, int table_cap,
                                int *out_ncols, int *out_col_types,
                                int *out_nrows, uint8_t **out_col_data);

/* ---- Schema-sync payload helpers ---------------------------------------- */

/*
 * Encode a SCHEMA_SYNC payload.
 *
 * v1 wire (legacy):
 *   table_name_len u8
 *   table_name     [...]
 *   ncols          u8
 *   ts_col_idx     u8
 *   per-col: name_len u8, name [...], type u8
 *
 * v2 wire tail (2026-05; written by encoder, optional on decode):
 *   partition_unit  u8     (TSDB_PARTITION_DAY=0 / HOUR=1)
 *   block_points    u32    (per-table block size; 0 → engine default)
 *   sort_by_tag_col i32    (-1 = off; >=0 = column index)
 *
 * The v2 tail closes a latent replication gap: pre-tail receivers
 * silently used (DAY, default block_points, sort_by_tag_col=-1) even
 * if the leader's schema diverged.  New encoders emit the tail
 * unconditionally; new decoders treat its absence as the legacy
 * defaults so old senders remain compatible.  Old receivers ignore
 * the trailing bytes (loop terminates after the per-col block, p<end
 * is not validated), so new senders also remain compatible with old
 * receivers.
 *
 * Returns bytes written, or -1.
 */
int tsdb_rpc_encode_schema(uint8_t *buf, uint32_t cap,
                           const char *table_name,
                           int ncols, const char **col_names,
                           const int *col_types, int ts_col_idx,
                           int partition_unit, int block_points,
                           int sort_by_tag_col);

/* Decode SCHEMA_SYNC payload.
 *
 * out_partition_unit / out_block_points / out_sort_by_tag_col may be
 * NULL if caller doesn't care; defaults are filled when the payload
 * has no v2 tail (DAY / 0 / -1).
 *
 * Returns 0 on success. */
int tsdb_rpc_decode_schema(const uint8_t *buf, uint32_t len,
                           char *out_table, int table_cap,
                           int *out_ncols, char out_col_names[][64],
                           int *out_col_types, int *out_ts_col_idx,
                           int *out_partition_unit, int *out_block_points,
                           int *out_sort_by_tag_col);

/* ---- Apply-truncate / apply-delete-range payload helpers ----------------
 *
 * APPLY_TRUNCATE payload:
 *   name_len u8
 *   name     [name_len bytes]
 *
 * APPLY_DELETE_RANGE payload:
 *   name_len  u8
 *   name      [name_len bytes]
 *   cutoff_ns i64 LE (8 bytes)
 *   op_lt     u8  (1 = ts <, 0 = ts >)
 *   inclusive u8  (1 = inclusive boundary)
 */
int tsdb_rpc_encode_truncate(uint8_t *buf, uint32_t cap,
                             const char *table_name);
int tsdb_rpc_decode_truncate(const uint8_t *buf, uint32_t len,
                             char *out_table, int table_cap);

int tsdb_rpc_encode_delete_range(uint8_t *buf, uint32_t cap,
                                 const char *table_name,
                                 int64_t cutoff_ns,
                                 int op_lt, int inclusive);
int tsdb_rpc_decode_delete_range(const uint8_t *buf, uint32_t len,
                                 char *out_table, int table_cap,
                                 int64_t *out_cutoff_ns,
                                 int *out_op_lt, int *out_inclusive);

/* APPLY_CATALOG_QTL payload:
 *   qtl_len u32 LE
 *   qtl     [qtl_len bytes]  UTF-8 (no NUL)
 */
int tsdb_rpc_encode_catalog_qtl(uint8_t *buf, uint32_t cap, const char *qtl);
int tsdb_rpc_decode_catalog_qtl(const uint8_t *buf, uint32_t len,
                                char *out_qtl, int qtl_cap);

/* ---- Anti-entropy peer probe (TSDB_RPC_LOCAL_TABLE_STATS) ---------------- */

/*
 * Ask the peer behind `conn` how many rows IT ITSELF holds for `table_name`
 * and what its newest ts is.  The peer answers straight from its storage, so
 * unlike `SELECT count(*), max(ts) FROM <t>` over FED_QUERY the answer can
 * never be the CLUSTER-WIDE aggregate: a peer holding nothing reports
 * (0, INT64_MIN) instead of echoing back the numbers of whichever node
 * actually has the data.
 *
 * Returns:
 *   TSDB_OK              — *out_count / *out_max_ts filled from the peer.
 *   TSDB_ERR_UNSUPPORTED — the peer's binary predates this opcode (it ACKed
 *                          with an empty body from the dispatch `default:`).
 *                          The caller must fall back to the legacy probe.
 *   TSDB_ERR_INTERNAL    — the peer knows the opcode and could not answer.
 *   TSDB_ERR_IO / TSDB_ERR_INVAL — transport / argument failure.
 */
int tsdb_rpc_local_table_stats(tsdb_rpc_conn_t *conn, const char *table_name,
                               uint64_t *out_count, int64_t *out_max_ts);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_RPC_H */
