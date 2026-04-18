/* proto.h — wire-protocol frame encode / decode for tsdb TCP server.
 *
 * Wire protocol v1 — little-endian throughout.
 *
 * Frame layout (24-byte fixed header + variable payload + 4-byte CRC32C tail):
 *
 *   [0..4)   magic    = 0x42445354  ('TSDB' LE)
 *   [4]      ver      = 1
 *   [5]      type     = tsdb_msg_type_t (uint8)
 *   [6..8)   flags    (uint16 LE)
 *   [8..12)  reserved (uint32, write 0, ignore on read)
 *   [12..20) req_id   (uint64 LE)
 *   [20..24) payload_len (uint32 LE)
 *   [24..24+payload_len) payload
 *   [24+payload_len..+4) CRC32C of bytes [4..24+payload_len)
 *                         i.e. covers VER..end-of-payload, not the magic or CRC itself
 *
 * See docs/design/wire-protocol.md for full spec.
 */
#ifndef TSDB_SERVER_PROTO_H
#define TSDB_SERVER_PROTO_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- Constants ------------------------------------------------------------ */

#define TSDB_PROTO_MAGIC      0x42445354u    /* 'TSDB' little-endian */
#define TSDB_PROTO_VER        1
#define TSDB_PROTO_HDR_SIZE   24             /* bytes, fixed */
#define TSDB_PROTO_MAX_PAYLOAD (16u * 1024u * 1024u)  /* 16 MiB */

/* Flags (16-bit) */
#define TSDB_FLAG_FIN          0x0001u   /* last frame in a multi-frame response */
#define TSDB_FLAG_COMPRESS_LZ  0x0002u   /* payload is lzlite-compressed */
#define TSDB_FLAG_STREAM       0x0004u   /* frame is part of a stream */
#define TSDB_FLAG_PONG         0x0008u   /* PING response */

/* ---- Message types ------------------------------------------------------- */

typedef enum {
    /* Session */
    TSDB_MT_HELLO              = 1,
    TSDB_MT_HELLO_OK           = 2,
    TSDB_MT_ERROR              = 3,
    TSDB_MT_PING               = 4,
    /* AUTH_LOGIN payload: [u8 ulen][user][u8 plen][pass]
     * AUTH_OK    payload: 32-byte lowercase hex token (no NUL) */
    TSDB_MT_AUTH_LOGIN         = 5,
    TSDB_MT_AUTH_OK            = 6,

    /* Group / device management */
    TSDB_MT_CREATE_GROUP       = 10,
    TSDB_MT_LIST_GROUPS        = 11,
    TSDB_MT_DROP_GROUP         = 12,
    TSDB_MT_CREATE_DEVICE      = 13,
    TSDB_MT_LIST_DEVICES       = 14,
    TSDB_MT_DROP_DEVICE        = 15,

    /* Table DDL */
    TSDB_MT_CREATE_TABLE       = 20,
    TSDB_MT_DROP_TABLE         = 21,
    TSDB_MT_SCHEMA             = 22,

    /* Write */
    TSDB_MT_WRITE_BATCH        = 30,
    TSDB_MT_WRITE_STREAM_OPEN  = 31,
    TSDB_MT_WRITE_STREAM_DATA  = 32,
    TSDB_MT_WRITE_STREAM_END   = 33,
    TSDB_MT_WRITE_ACK          = 34,

    /* Query */
    TSDB_MT_QUERY              = 40,
    TSDB_MT_QUERY_RESULT_HDR   = 41,
    TSDB_MT_QUERY_RESULT_ROWS  = 42,

    /* Subscription */
    TSDB_MT_SUBSCRIBE          = 50,
    TSDB_MT_SUB_EVENT          = 51,
    TSDB_MT_UNSUBSCRIBE        = 52,

    /* Cluster */
    TSDB_MT_CLUSTER_STATS      = 60,

    /* Raw block replication (internal) */
    TSDB_MT_RAW_BLOCK_PUSH     = 70,
    TSDB_MT_RAW_BLOCK_ACK      = 71,
    TSDB_MT_MERKLE_DIFF_REQ    = 72,
    TSDB_MT_MERKLE_DIFF_RESP   = 73,
} tsdb_msg_type_t;

/* ---- Frame header -------------------------------------------------------- */

typedef struct {
    uint32_t magic;
    uint8_t  ver;
    uint8_t  type;
    uint16_t flags;
    /* 4 bytes reserved — not in struct, handled in serialise/parse */
    uint64_t req_id;
    uint32_t payload_len;
} tsdb_frame_hdr_t;

/* Serialise header into exactly 24 bytes (little-endian). */
void tsdb_frame_write_hdr(const tsdb_frame_hdr_t *h, uint8_t out[24]);

/* Parse 24 bytes → header.
 * Returns TSDB_OK (0) or TSDB_ERR_CORRUPT (-4) on bad magic/ver. */
int  tsdb_frame_read_hdr(const uint8_t in[24], tsdb_frame_hdr_t *out);

/* ---- CRC32C (Castagnoli / iSCSI polynomial) ------------------------------ */

/* One-shot CRC32C over arbitrary bytes.  Dispatches at runtime to the best
 * available implementation (SSE4.2 / ARMv8 CRC / Sarwate fallback). */
uint32_t tsdb_crc32c(const void *data, size_t n);

/* Incremental variant: feed multiple chunks, carrying the running CRC in
 * its finalised wire form.  Start the chain with 0. */
uint32_t tsdb_crc32c_update(uint32_t crc, const void *data, size_t n);

/* Returns the name of the currently-selected CRC32C implementation.  One of
 * "sse4.2", "armv8-crc", or "sarwate".  Intended for diagnostics. */
const char *tsdb_crc32c_impl(void);

/* ---- Frame I/O ----------------------------------------------------------- */

/*
 * Send a complete frame (header + payload + CRC32C tail) over fd.
 * Handles short writes (loops until all bytes sent).
 * Returns TSDB_OK or TSDB_ERR_IO.
 */
int tsdb_proto_send(int fd, uint8_t type, uint16_t flags, uint64_t req_id,
                    const void *payload, size_t n);

/*
 * Receive one complete frame from fd.
 *   hdr         — populated with the parsed header
 *   out_payload — malloc'd buffer containing the payload; caller must free().
 *                 NULL if payload_len == 0.
 *
 * Returns TSDB_OK, TSDB_ERR_IO (connection closed / read error),
 *         or TSDB_ERR_CORRUPT (bad magic / CRC mismatch).
 */
int tsdb_proto_recv(int fd, tsdb_frame_hdr_t *hdr, uint8_t **out_payload);

/* ---- TLS-aware I/O variants --------------------------------------------- */
/*
 * Same as tsdb_proto_send / tsdb_proto_recv but use a tsdb_io_t for the
 * underlying read/write operations.  When io->tls == NULL these degrade to
 * the plain-fd versions above.  Include tls.h before proto.h to use these.
 */
#ifdef TSDB_SERVER_TLS_H
int tsdb_proto_send_io(tsdb_io_t *io, uint8_t type, uint16_t flags,
                       uint64_t req_id, const void *payload, size_t n);
int tsdb_proto_recv_io(tsdb_io_t *io, tsdb_frame_hdr_t *hdr,
                       uint8_t **out_payload);
#endif

#ifdef __cplusplus
}
#endif

#endif /* TSDB_SERVER_PROTO_H */
