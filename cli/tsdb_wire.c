/* tsdb_wire.c — wire protocol framing implementation.
 *
 * CRC32C (Castagnoli, poly 0x1EDC6F41 / reflected 0x82F63B78).
 * Frame layout: see tsdb_wire.h header comment.
 */

#define _POSIX_C_SOURCE 200809L

#include "tsdb_wire.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdint.h>

/* ─── CRC32C (Castagnoli) table-driven implementation ────────────────────── */
/*
 * Reflected polynomial: 0x82F63B78
 * Production note: on x86 with SSE4.2, use _mm_crc32_u8/u32/u64 intrinsics.
 * On ARMv8.1-a, use __crc32cb/__crc32cw/__crc32cd GCC built-ins.
 * This software path is ~500 MB/s on modern hardware, sufficient for CLI use.
 */

static uint32_t crc32c_table[256];
static int      crc32c_table_init = 0;

static void build_crc32c_table(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++)
            crc = (crc >> 1) ^ ((crc & 1) ? 0x82F63B78u : 0);
        crc32c_table[i] = crc;
    }
    crc32c_table_init = 1;
}

uint32_t crc32c(uint32_t crc, const uint8_t *buf, size_t len) {
    if (!crc32c_table_init) build_crc32c_table();
    crc = ~crc;
    while (len--)
        crc = (crc >> 8) ^ crc32c_table[(crc ^ *buf++) & 0xFF];
    return ~crc;
}

/* ─── Reliable I/O helpers ───────────────────────────────────────────────── */

/* Write exactly n bytes to fd, retrying on EINTR. */
static int write_all(int fd, const uint8_t *buf, size_t n) {
    while (n > 0) {
        ssize_t r = write(fd, buf, n);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) { errno = ECONNRESET; return -1; }
        buf += r; n -= (size_t)r;
    }
    return 0;
}

/* Read exactly n bytes from fd with poll-based timeout (ms). */
static int read_all(int fd, uint8_t *buf, size_t n, int timeout_ms) {
    while (n > 0) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int pr = poll(&pfd, 1, timeout_ms);
        if (pr < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (pr == 0) { errno = ETIMEDOUT; return -1; }
        /* POLLERR or POLLNVAL without POLLIN means no data coming */
        if ((pfd.revents & (POLLERR | POLLNVAL)) &&
            !(pfd.revents & POLLIN)) {
            errno = ECONNRESET; return -1;
        }
        /* POLLHUP with no POLLIN: peer closed, no data left */
        if ((pfd.revents & POLLHUP) && !(pfd.revents & POLLIN)) {
            errno = ECONNRESET; return -1;
        }
        ssize_t r = read(fd, buf, n);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) { errno = ECONNRESET; return -1; }
        buf += r; n -= (size_t)r;
    }
    return 0;
}

/* ─── Frame serialisation ────────────────────────────────────────────────── */

/*
 * Wire header layout (24 bytes, all LE):
 *   [0..3]  MAGIC u32
 *   [4]     VER   u8
 *   [5]     TYPE  u8
 *   [6..7]  FLAGS u16
 *   [8..11] RESERVED u32
 *   [12..19] REQ_ID u64
 *   [20..23] PAYLOAD_LEN u32
 */

int frame_send(tsdb_conn_t *c, uint8_t type, uint16_t flags,
               uint64_t req_id, const uint8_t *payload, uint32_t plen) {
    uint8_t hdr[TSDB_WIRE_HDR_SIZE];
    put_u32(hdr + 0,  TSDB_WIRE_MAGIC);
    put_u8 (hdr + 4,  TSDB_WIRE_VER);
    put_u8 (hdr + 5,  type);
    put_u16(hdr + 6,  flags);
    put_u32(hdr + 8,  0); /* reserved */
    put_u64(hdr + 12, req_id);
    put_u32(hdr + 20, plen);

    /* CRC over hdr[4..23] + payload */
    uint32_t crc = 0;
    crc = crc32c(crc, hdr + 4, TSDB_WIRE_HDR_SIZE - 4);
    if (plen > 0 && payload)
        crc = crc32c(crc, payload, plen);

    uint8_t trail[4];
    put_u32(trail, crc);

    if (write_all(c->fd, hdr, TSDB_WIRE_HDR_SIZE) < 0) return -1;
    if (plen > 0 && payload && write_all(c->fd, payload, plen) < 0) return -1;
    if (write_all(c->fd, trail, 4) < 0) return -1;
    return 0;
}

int frame_recv(tsdb_conn_t *c, tsdb_msg_t *msg) {
    memset(msg, 0, sizeof(*msg));

    uint8_t hdr[TSDB_WIRE_HDR_SIZE];
    if (read_all(c->fd, hdr, TSDB_WIRE_HDR_SIZE, c->timeout_ms) < 0)
        return -1;

    uint32_t magic = get_u32(hdr + 0);
    if (magic != TSDB_WIRE_MAGIC) return -3;

    msg->hdr.magic       = magic;
    msg->hdr.ver         = get_u8 (hdr + 4);
    msg->hdr.type        = get_u8 (hdr + 5);
    msg->hdr.flags       = get_u16(hdr + 6);
    msg->hdr.reserved    = get_u32(hdr + 8);
    msg->hdr.req_id      = get_u64(hdr + 12);
    msg->hdr.payload_len = get_u32(hdr + 20);

    uint32_t plen = msg->hdr.payload_len;
    if (plen > 16 * 1024 * 1024) {
        /* Reject absurdly large payloads (16 MiB limit from spec) */
        errno = EMSGSIZE; return -1;
    }

    uint8_t *payload = NULL;
    if (plen > 0) {
        payload = malloc(plen);
        if (!payload) return -1;
        if (read_all(c->fd, payload, plen, c->timeout_ms) < 0) {
            free(payload); return -1;
        }
    }

    /* Read 4-byte CRC trailer */
    uint8_t trail[4];
    if (read_all(c->fd, trail, 4, c->timeout_ms) < 0) {
        free(payload); return -1;
    }
    uint32_t crc_recv = get_u32(trail);

    /* Verify CRC */
    uint32_t crc = 0;
    crc = crc32c(crc, hdr + 4, TSDB_WIRE_HDR_SIZE - 4);
    if (plen > 0) crc = crc32c(crc, payload, plen);
    if (crc != crc_recv) {
        free(payload);
        msg->crc_recv = crc_recv;
        return -2;
    }

    msg->payload  = payload;
    msg->crc_recv = crc_recv;
    return 0;
}

/* ─── High-level send_recv ───────────────────────────────────────────────── */

int send_recv(tsdb_conn_t *c,
              uint8_t req_type, uint16_t flags,
              const uint8_t *payload, uint32_t plen,
              uint8_t resp_type, tsdb_msg_t *msg) {
    uint64_t req_id = c->next_req_id++;
    if (frame_send(c, req_type, flags, req_id, payload, plen) < 0)
        return -1;

    int rc = frame_recv(c, msg);
    if (rc < 0) return rc;

    if (msg->hdr.type == MSG_ERROR) {
        /* Decode error message and print it */
        const uint8_t *p = msg->payload;
        uint32_t mlen = msg->hdr.payload_len;
        if (mlen >= 6) {
            /* [err_code i32 LE] [msg_len u16 LE] [msg utf8] */
            int32_t err_code = (int32_t)get_u32(p);
            uint16_t ml = get_u16(p + 4);
            if (ml + 6 <= mlen) {
                fprintf(stderr, "server error %d: %.*s\n", err_code, (int)ml, (const char*)(p + 6));
            } else {
                fprintf(stderr, "server error %d\n", err_code);
            }
        } else {
            fprintf(stderr, "server error (empty)\n");
        }
        msg_free(msg);
        return -1;
    }

    if (resp_type != 0 && msg->hdr.type != resp_type) {
        fprintf(stderr, "unexpected response type %d (expected %d)\n",
                msg->hdr.type, resp_type);
        msg_free(msg);
        return -1;
    }

    return 0;
}
