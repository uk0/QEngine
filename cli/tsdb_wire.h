/* tsdb_wire.h — client-side wire protocol definitions for tsdb TCP protocol v1.
 *
 * Frame layout (all little-endian):
 *
 *   MAGIC      u32  = 0x42445354  ('TSDB' LE)
 *   VER        u8   = 1
 *   TYPE       u8   (message type from table below)
 *   FLAGS      u16  (bit 0=FIN, bit 1=COMPRESS_LZLITE, bit 2=STREAM, bit 3=PONG)
 *   RESERVED   u32  (set to 0 on send, ignore on recv)
 *   REQ_ID     u64  (monotonically increasing; echoed in response)
 *   PAYLOAD_LEN u32
 *   --- total header = 4+1+1+2+4+8+4 = 24 bytes ---
 *   PAYLOAD    [PAYLOAD_LEN bytes]
 *   CRC32C     u32  (Castagnoli CRC over header[4..23] + payload)
 *
 * NOTE: CRC covers bytes starting at VER (offset 4) through end of PAYLOAD.
 *       The MAGIC field itself is NOT included in the CRC window.
 */
#ifndef TSDB_WIRE_H
#define TSDB_WIRE_H

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ─── Constants ──────────────────────────────────────────────────────────── */

#define TSDB_WIRE_MAGIC     0x42445354u   /* 'TSDB' little-endian */
#define TSDB_WIRE_VER       1
#define TSDB_WIRE_HDR_SIZE  24u           /* header before payload */
#define TSDB_WIRE_TRAIL     4u            /* CRC32C trailer */

/* Frame flags (bits in the FLAGS field) */
#define TSDB_FLAG_FIN              (1u << 0)
#define TSDB_FLAG_COMPRESS_LZLITE (1u << 1)
#define TSDB_FLAG_STREAM           (1u << 2)
#define TSDB_FLAG_PONG             (1u << 3)

/* ─── Message type IDs ───────────────────────────────────────────────────── */
typedef enum {
    MSG_HELLO             =  1,
    MSG_HELLO_OK          =  2,
    MSG_ERROR             =  3,
    MSG_PING              =  4,   /* PONG = same id, FLAG_PONG set */
    MSG_AUTH_LOGIN        =  5,   /* payload: [u8 ulen][user][u8 plen][pass] */
    MSG_AUTH_OK           =  6,   /* payload: 32-byte hex token (no NUL) */
    MSG_CREATE_GROUP      = 10,
    MSG_LIST_GROUPS       = 11,
    MSG_DROP_GROUP        = 12,
    MSG_CREATE_DEVICE     = 13,
    MSG_LIST_DEVICES      = 14,
    MSG_DROP_DEVICE       = 15,
    MSG_CREATE_TABLE      = 20,
    MSG_DROP_TABLE        = 21,
    MSG_SCHEMA            = 22,
    MSG_WRITE_BATCH       = 30,
    MSG_WRITE_STREAM_OPEN = 31,
    MSG_WRITE_STREAM_DATA = 32,
    MSG_WRITE_STREAM_END  = 33,
    MSG_WRITE_ACK         = 34,
    MSG_QUERY             = 40,
    MSG_QUERY_RESULT_HDR  = 41,
    MSG_QUERY_RESULT_ROWS = 42,
    MSG_SUBSCRIBE         = 50,
    MSG_SUB_EVENT         = 51,
    MSG_UNSUBSCRIBE       = 52,
    MSG_CLUSTER_STATS     = 60,
} tsdb_msg_type_t;

/* ─── Column types (also used in wire payload schemas) ───────────────────── */
#define TSDB_TYPE_TIMESTAMP  1
#define TSDB_TYPE_INT64      2
#define TSDB_TYPE_FLOAT64    3
#define TSDB_TYPE_SYMBOL     4

/* ─── Codec IDs (columnar write) ─────────────────────────────────────────── */
#define TSDB_CODEC_RAW      0
#define TSDB_CODEC_GORILLA  1
#define TSDB_CODEC_DOD      2

/* ─── Payload layout notes (as agreed by client+server) ─────────────────── */
/*
 * HELLO payload:
 *   [ver u8] [client_id_len u8] [client_id utf8] [token_len u8] [token utf8]
 *   ver = TSDB_WIRE_VER
 *
 * HELLO_OK payload:
 *   [server_ver_len u8] [server_ver utf8]
 *   e.g. "tsdb-server v0.1"
 *
 * ERROR payload:
 *   [err_code i32 LE] [msg_len u16 LE] [msg utf8]
 *
 * CREATE_GROUP payload:
 *   [name_len u8] [name utf8] [nprops u8]
 *   for each prop: [key_len u8][key utf8][val_len u8][val utf8]
 *
 * LIST_GROUPS payload:
 *   [glob_len u8] [glob utf8]   (0 = list all)
 *
 * CREATE_DEVICE payload:
 *   [group_len u8] [group utf8] [id_len u8] [id utf8] [ntags u8]
 *   for each tag: [key_len u8][key utf8][val_len u8][val utf8]
 *
 * LIST_DEVICES payload:
 *   [group_len u8] [group utf8]
 *
 * AUTH_LOGIN payload (type 5):
 *   [user_len u8] [user utf8] [pass_len u8] [pass utf8]
 *   Client sends this right after HELLO when the server is configured with
 *   require_auth=true.  On success the server replies AUTH_OK; on failure
 *   it replies ERROR with err_code = TSDB_ERR_PERMISSION (-13).
 *
 * AUTH_OK payload (type 6):
 *   32 bytes of lowercase hex (no NUL) — the session token.  The server
 *   associates this token with the underlying connection; subsequent
 *   QUERY frames on the same connection are executed as the authenticated
 *   user.  On connection close the token is discarded.
 *
 * QUERY payload:
 *   [qtl_len u16 LE] [qtl utf8]
 *
 * QUERY_RESULT_HDR payload:
 *   [ncols u16 LE]
 *   for each col: [name_len u8][name utf8][type u8]
 *
 * QUERY_RESULT_ROWS payload: columnar chunk (same format as WRITE_BATCH).
 *   Last chunk has FLAG_FIN set.
 *
 * WRITE_BATCH / WRITE_STREAM_DATA columnar payload:
 *   [table_name_len u8] [table_name utf8]
 *   [ncols u16 LE] [nrows u32 LE]
 *   for each col:
 *     [col_name_len u8][col_name utf8][col_type u8][codec u8]
 *     [compressed_size u32 LE][compressed bytes]
 *
 * WRITE_STREAM_OPEN payload:
 *   [table_name_len u8] [table_name utf8]
 *
 * WRITE_STREAM_END payload: empty (nrows=0 signal)
 *
 * WRITE_ACK payload:
 *   [rows_accepted u64 LE] [last_seq u64 LE]
 *
 * SUBSCRIBE payload:
 *   [table_len u8] [table utf8] [filter_len u16 LE] [filter utf8] (0 = no filter)
 *
 * SUB_EVENT payload: same columnar format as WRITE_BATCH payload
 *
 * UNSUBSCRIBE payload:
 *   [sub_req_id u64 LE]   (the req_id used in the original SUBSCRIBE)
 *
 * CLUSTER_STATS payload (request): empty
 * CLUSTER_STATS response (MSG_SCHEMA type = 22 or MSG_HELLO_OK): text kv pairs
 *   [kv_count u16 LE] for each: [k_len u8][k utf8][v_len u8][v utf8]
 */

/* ─── Frame header struct (in-memory, not wire order) ────────────────────── */
typedef struct {
    uint32_t magic;       /* always TSDB_WIRE_MAGIC */
    uint8_t  ver;
    uint8_t  type;
    uint16_t flags;
    uint32_t reserved;
    uint64_t req_id;
    uint32_t payload_len;
} tsdb_frame_hdr_t;

/* ─── TLS opaque pointer (only valid when tsdb_tls_available() == 1) ────── */
/*
 * Forward-declared so that tsdb_conn_t can hold a pointer without pulling in
 * the full TLS headers.  Cast to tsdb_tls_conn_t* when using.
 * When TLS is not compiled in, this field is always NULL.
 */
struct tsdb_cli_tls_conn;

/* ─── Connection context ─────────────────────────────────────────────────── */
typedef struct {
    int      fd;
    uint64_t next_req_id;
    char     host[256];
    int      port;
    int      timeout_ms;   /* recv/send timeout, default 10000 */
    /* TLS state — NULL for plaintext connections */
    struct tsdb_cli_tls_conn *tls;
    /* Auto-reconnect state — retained so a dropped session can re-dial the
     * same endpoint with the same credentials.  auto_reconnect=0 disables it. */
    const char *token;          /* auth token (may be NULL) */
    int         use_tls;        /* re-dial over TLS */
    const char *tls_ca;         /* PEM CA bundle (TLS only) */
    int         tls_insecure;   /* skip cert verification (TLS only) */
    int         auto_reconnect; /* 1 = re-dial idempotent ops on conn death */
} tsdb_conn_t;

/* ─── Received message ───────────────────────────────────────────────────── */
typedef struct {
    tsdb_frame_hdr_t hdr;
    uint8_t         *payload;  /* malloc'd, caller must free */
    uint32_t         crc_recv; /* crc as received */
} tsdb_msg_t;

/* ─── CRC32C (Castagnoli) ────────────────────────────────────────────────── */
/*
 * Software CRC32C using the Castagnoli polynomial 0x82F63B78 (reflected).
 * In production code, prefer hardware-accelerated variants:
 *   x86: _mm_crc32_u* intrinsics (SSE4.2)
 *   ARM: __crc32cb/__crc32cw (armv8.1-a)
 * Both GCC and clang emit crc32 instructions when -mcrc is in effect.
 * This fallback is intentionally portable C11 and runs ~500 MB/s.
 */
uint32_t crc32c(uint32_t crc, const uint8_t *buf, size_t len);

/* ─── Frame encode/decode ────────────────────────────────────────────────── */

/*
 * Write a complete frame to `fd`:
 *   24-byte header + payload + 4-byte CRC32C trailer.
 * crc is computed over header[4..23] + payload.
 * Returns 0 on success, -1 on error (errno set).
 */
int frame_send(tsdb_conn_t *c, uint8_t type, uint16_t flags,
               uint64_t req_id, const uint8_t *payload, uint32_t plen);

/*
 * Read one frame from `fd` with a per-read poll timeout of c->timeout_ms.
 * Allocates msg->payload (caller must free).
 * Returns 0 on success, -1 on I/O error, -2 on CRC mismatch, -3 on bad magic.
 */
int frame_recv(tsdb_conn_t *c, tsdb_msg_t *msg);

/* Free payload of a received message. */
static inline void msg_free(tsdb_msg_t *m) {
    if (m && m->payload) { free(m->payload); m->payload = NULL; }
}

/* ─── TLS-aware connect ──────────────────────────────────────────────────── */

/*
 * Connect with TLS.
 *   ca_path      — PEM CA bundle to verify server cert; NULL = system store
 *   skip_verify  — non-zero disables certificate verification (insecure)
 * The fd is taken from an already-connected socket (use tsdb_conn_connect or
 * fill conn->fd yourself), then wrapped.  Alternatively, pass a ready fd < 0
 * to have this function create and connect the socket.
 *
 * Returns 0 on success, -1 on error (errno set).
 */
int tsdb_conn_connect_tls(const char *host, int port,
                          const char *ca_path, int skip_verify,
                          tsdb_conn_t *conn);

/*
 * Close a connection, freeing TLS resources if applicable.
 * Safe to call even if conn->tls == NULL (plain close).
 */
void tsdb_conn_close(tsdb_conn_t *conn);

/* ─── High-level request/response ────────────────────────────────────────── */

/*
 * Send a request and wait for a response of expected type resp_type.
 * On server ERROR response, prints the error message and returns -1.
 * Returns 0 on success (msg filled), caller must msg_free(msg).
 */
int send_recv(tsdb_conn_t *c,
              uint8_t req_type, uint16_t flags,
              const uint8_t *payload, uint32_t plen,
              uint8_t resp_type, tsdb_msg_t *msg);

/* ─── Little-endian helpers (safe, no UB) ───────────────────────────────── */
static inline void put_u8 (uint8_t *b, uint8_t  v) { b[0] = v; }
static inline void put_u16(uint8_t *b, uint16_t v) {
    b[0] = (uint8_t)v; b[1] = (uint8_t)(v >> 8);
}
static inline void put_u32(uint8_t *b, uint32_t v) {
    b[0] = (uint8_t)v;        b[1] = (uint8_t)(v >>  8);
    b[2] = (uint8_t)(v >> 16); b[3] = (uint8_t)(v >> 24);
}
static inline void put_u64(uint8_t *b, uint64_t v) {
    put_u32(b,     (uint32_t)v);
    put_u32(b + 4, (uint32_t)(v >> 32));
}

static inline uint8_t  get_u8 (const uint8_t *b) { return b[0]; }
static inline uint16_t get_u16(const uint8_t *b) {
    return (uint16_t)b[0] | ((uint16_t)b[1] << 8);
}
static inline uint32_t get_u32(const uint8_t *b) {
    return (uint32_t)b[0]        | ((uint32_t)b[1] <<  8) |
           ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
}
static inline uint64_t get_u64(const uint8_t *b) {
    return (uint64_t)get_u32(b) | ((uint64_t)get_u32(b + 4) << 32);
}

#ifdef __cplusplus
}
#endif
#endif /* TSDB_WIRE_H */
