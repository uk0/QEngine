/* proto.c — wire-protocol frame encode / decode.
 *
 * CRC32C (Castagnoli) polynomial: 0x82F63B78 (reflected).
 * Known test vector: crc32c("123456789") == 0xE3069283.
 */

#include "proto.h"
#include "../../include/tsdb.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

/* ---- CRC32C (software, 8-bytes-at-a-time Sarwate) ----------------------- */

/* Castagnoli (iSCSI) reflected polynomial. */
#define CRC32C_POLY 0x82F63B78u

static uint32_t crc32c_table[8][256];
static pthread_once_t crc32c_once = PTHREAD_ONCE_INIT;

static void crc32c_init_table(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (CRC32C_POLY ^ (c >> 1)) : (c >> 1);
        crc32c_table[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = crc32c_table[0][i];
        for (int s = 1; s < 8; s++) {
            c = crc32c_table[0][c & 0xFF] ^ (c >> 8);
            crc32c_table[s][i] = c;
        }
    }
}

uint32_t tsdb_crc32c(const void *data, size_t n) {
    pthread_once(&crc32c_once, crc32c_init_table);

    const uint8_t *p = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFFu;

    /* Process 8 bytes at a time. */
    while (n >= 8) {
        uint32_t w0, w1;
        memcpy(&w0, p,     4);
        memcpy(&w1, p + 4, 4);
        uint32_t c0 = crc ^ w0;
        uint32_t c1 = w1;
        crc = crc32c_table[7][ c0        & 0xFF]
            ^ crc32c_table[6][(c0 >>  8) & 0xFF]
            ^ crc32c_table[5][(c0 >> 16) & 0xFF]
            ^ crc32c_table[4][ c0 >> 24        ]
            ^ crc32c_table[3][ c1        & 0xFF]
            ^ crc32c_table[2][(c1 >>  8) & 0xFF]
            ^ crc32c_table[1][(c1 >> 16) & 0xFF]
            ^ crc32c_table[0][ c1 >> 24        ];
        p += 8;
        n -= 8;
    }
    /* Remaining bytes. */
    while (n-- > 0)
        crc = crc32c_table[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);

    return crc ^ 0xFFFFFFFFu;
}

/* ---- Header serialise / parse ------------------------------------------- */

/*
 * Wire layout (24 bytes, all LE):
 *   [0..4)   magic
 *   [4]      ver
 *   [5]      type
 *   [6..8)   flags
 *   [8..12)  reserved (0)
 *   [12..20) req_id
 *   [20..24) payload_len
 */
void tsdb_frame_write_hdr(const tsdb_frame_hdr_t *h, uint8_t out[24]) {
    memset(out, 0, 24);
    /* magic */
    out[0] = (uint8_t)(h->magic);
    out[1] = (uint8_t)(h->magic >> 8);
    out[2] = (uint8_t)(h->magic >> 16);
    out[3] = (uint8_t)(h->magic >> 24);
    /* ver, type */
    out[4] = h->ver;
    out[5] = h->type;
    /* flags (LE) */
    out[6] = (uint8_t)(h->flags);
    out[7] = (uint8_t)(h->flags >> 8);
    /* reserved [8..12) = 0 */
    /* req_id (LE) */
    out[12] = (uint8_t)(h->req_id);
    out[13] = (uint8_t)(h->req_id >> 8);
    out[14] = (uint8_t)(h->req_id >> 16);
    out[15] = (uint8_t)(h->req_id >> 24);
    out[16] = (uint8_t)(h->req_id >> 32);
    out[17] = (uint8_t)(h->req_id >> 40);
    out[18] = (uint8_t)(h->req_id >> 48);
    out[19] = (uint8_t)(h->req_id >> 56);
    /* payload_len (LE) */
    out[20] = (uint8_t)(h->payload_len);
    out[21] = (uint8_t)(h->payload_len >> 8);
    out[22] = (uint8_t)(h->payload_len >> 16);
    out[23] = (uint8_t)(h->payload_len >> 24);
}

int tsdb_frame_read_hdr(const uint8_t in[24], tsdb_frame_hdr_t *out) {
    uint32_t magic = (uint32_t)in[0]
                   | ((uint32_t)in[1] << 8)
                   | ((uint32_t)in[2] << 16)
                   | ((uint32_t)in[3] << 24);
    if (magic != TSDB_PROTO_MAGIC)
        return TSDB_ERR_CORRUPT;

    out->magic   = magic;
    out->ver     = in[4];
    out->type    = in[5];
    out->flags   = (uint16_t)in[6] | ((uint16_t)in[7] << 8);
    /* reserved in[8..12) ignored */
    out->req_id  = (uint64_t)in[12]
                 | ((uint64_t)in[13] << 8)
                 | ((uint64_t)in[14] << 16)
                 | ((uint64_t)in[15] << 24)
                 | ((uint64_t)in[16] << 32)
                 | ((uint64_t)in[17] << 40)
                 | ((uint64_t)in[18] << 48)
                 | ((uint64_t)in[19] << 56);
    out->payload_len = (uint32_t)in[20]
                     | ((uint32_t)in[21] << 8)
                     | ((uint32_t)in[22] << 16)
                     | ((uint32_t)in[23] << 24);
    return TSDB_OK;
}

/* ---- I/O helpers --------------------------------------------------------- */

static int write_all(int fd, const uint8_t *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, buf + done, n - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            return TSDB_ERR_IO;
        }
        if (w == 0) return TSDB_ERR_IO;
        done += (size_t)w;
    }
    return TSDB_OK;
}

static int read_all(int fd, uint8_t *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, buf + done, n - done);
        if (r < 0) {
            if (errno == EINTR) continue;
            return TSDB_ERR_IO;
        }
        if (r == 0) return TSDB_ERR_IO;  /* EOF */
        done += (size_t)r;
    }
    return TSDB_OK;
}

/* ---- Public send / recv -------------------------------------------------- */

/*
 * Frame on wire:
 *   24-byte header
 *   payload_len bytes of payload
 *   4-byte CRC32C LE — computed over bytes [4..24+payload_len)
 *                       i.e. ver + type + flags + reserved + req_id + payload_len + payload
 */
int tsdb_proto_send(int fd, uint8_t type, uint16_t flags, uint64_t req_id,
                    const void *payload, size_t n)
{
    if (n > TSDB_PROTO_MAX_PAYLOAD)
        return TSDB_ERR_INVAL;

    tsdb_frame_hdr_t hdr = {
        .magic       = TSDB_PROTO_MAGIC,
        .ver         = TSDB_PROTO_VER,
        .type        = type,
        .flags       = flags,
        .req_id      = req_id,
        .payload_len = (uint32_t)n,
    };

    uint8_t raw_hdr[TSDB_PROTO_HDR_SIZE];
    tsdb_frame_write_hdr(&hdr, raw_hdr);

    /* Compute CRC32C incrementally over header[4..24) + payload. */
    pthread_once(&crc32c_once, crc32c_init_table);
    uint32_t running = 0xFFFFFFFFu;
    {
        const uint8_t *hp = raw_hdr + 4;
        for (size_t i = 0; i < (TSDB_PROTO_HDR_SIZE - 4); i++)
            running = crc32c_table[0][(running ^ hp[i]) & 0xFF] ^ (running >> 8);
    }
    if (payload && n > 0) {
        const uint8_t *pp = (const uint8_t *)payload;
        for (size_t i = 0; i < n; i++)
            running = crc32c_table[0][(running ^ pp[i]) & 0xFF] ^ (running >> 8);
    }
    uint32_t crc = running ^ 0xFFFFFFFFu;

    uint8_t crc_bytes[4];
    crc_bytes[0] = (uint8_t)(crc);
    crc_bytes[1] = (uint8_t)(crc >> 8);
    crc_bytes[2] = (uint8_t)(crc >> 16);
    crc_bytes[3] = (uint8_t)(crc >> 24);

    int rc;
    if ((rc = write_all(fd, raw_hdr, TSDB_PROTO_HDR_SIZE)) != TSDB_OK) return rc;
    if (n > 0 && payload) {
        if ((rc = write_all(fd, (const uint8_t *)payload, n)) != TSDB_OK) return rc;
    }
    return write_all(fd, crc_bytes, 4);
}

int tsdb_proto_recv(int fd, tsdb_frame_hdr_t *hdr, uint8_t **out_payload) {
    uint8_t raw_hdr[TSDB_PROTO_HDR_SIZE];
    int rc;

    if ((rc = read_all(fd, raw_hdr, TSDB_PROTO_HDR_SIZE)) != TSDB_OK)
        return rc;

    if ((rc = tsdb_frame_read_hdr(raw_hdr, hdr)) != TSDB_OK)
        return rc;

    if (hdr->payload_len > TSDB_PROTO_MAX_PAYLOAD)
        return TSDB_ERR_CORRUPT;

    uint8_t *payload = NULL;
    if (hdr->payload_len > 0) {
        payload = (uint8_t *)malloc(hdr->payload_len);
        if (!payload) return TSDB_ERR_NOMEM;
        if ((rc = read_all(fd, payload, hdr->payload_len)) != TSDB_OK) {
            free(payload);
            return rc;
        }
    }

    /* Read and verify CRC32C tail. */
    uint8_t crc_bytes[4];
    if ((rc = read_all(fd, crc_bytes, 4)) != TSDB_OK) {
        free(payload);
        return rc;
    }

    uint32_t stored_crc = (uint32_t)crc_bytes[0]
                        | ((uint32_t)crc_bytes[1] << 8)
                        | ((uint32_t)crc_bytes[2] << 16)
                        | ((uint32_t)crc_bytes[3] << 24);

    /* Compute expected CRC over header[4..24) + payload. */
    pthread_once(&crc32c_once, crc32c_init_table);
    uint32_t running = 0xFFFFFFFFu;
    const uint8_t *hp = raw_hdr + 4;
    for (size_t i = 0; i < (TSDB_PROTO_HDR_SIZE - 4); i++)
        running = crc32c_table[0][(running ^ hp[i]) & 0xFF] ^ (running >> 8);
    if (payload && hdr->payload_len > 0) {
        for (size_t i = 0; i < hdr->payload_len; i++)
            running = crc32c_table[0][(running ^ payload[i]) & 0xFF] ^ (running >> 8);
    }
    uint32_t computed_crc = running ^ 0xFFFFFFFFu;

    if (computed_crc != stored_crc) {
        free(payload);
        return TSDB_ERR_CORRUPT;
    }

    *out_payload = payload;
    return TSDB_OK;
}
