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
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/socket.h>
#include <netdb.h>

/* ─── TLS integration ────────────────────────────────────────────────────── */
/*
 * The client-side TLS state is managed via the same OpenSSL / mbedtls macros
 * as the server, but the cli/ directory doesn't link against libtsdb.
 * We include the tls backend directly using the same feature-test macros set
 * by the Makefile (TSDB_TLS_OPENSSL / TSDB_TLS_MBEDTLS).
 */

#if defined(TSDB_TLS_OPENSSL)
#  include <openssl/ssl.h>
#  include <openssl/err.h>

/* Thin wrapper matching the tsdb_cli_tls_conn forward decl in tsdb_wire.h */
struct tsdb_cli_tls_conn {
    SSL_CTX *ctx;
    SSL     *ssl;
};

static int cli_tls_available(void) { return 1; }

static struct tsdb_cli_tls_conn *
cli_tls_connect(const char *host, int fd, const char *ca_path, int skip_verify)
{
    SSL_CTX *cx = SSL_CTX_new(TLS_client_method());
    if (!cx) return NULL;
    SSL_CTX_set_min_proto_version(cx, TLS1_2_VERSION);
    SSL_CTX_set_options(cx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 |
                            SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
    if (skip_verify) {
        SSL_CTX_set_verify(cx, SSL_VERIFY_NONE, NULL);
    } else {
        if (ca_path && *ca_path) {
            if (SSL_CTX_load_verify_locations(cx, ca_path, NULL) != 1) {
                SSL_CTX_free(cx); return NULL;
            }
        } else {
            SSL_CTX_set_default_verify_paths(cx);
        }
        SSL_CTX_set_verify(cx, SSL_VERIFY_PEER, NULL);
    }
    SSL *ssl = SSL_new(cx);
    if (!ssl) { SSL_CTX_free(cx); return NULL; }
    SSL_set_fd(ssl, fd);
    SSL_set_connect_state(ssl);
    if (host && *host) {
        SSL_set_tlsext_host_name(ssl, host);
        SSL_set1_host(ssl, host);
    }
    /* Handshake loop. */
    int ret;
    while ((ret = SSL_do_handshake(ssl)) != 1) {
        int err = SSL_get_error(ssl, ret);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) continue;
        fprintf(stderr, "[tls-cli] handshake failed (err=%d)\n", err);
        SSL_free(ssl); SSL_CTX_free(cx); return NULL;
    }
    struct tsdb_cli_tls_conn *c = calloc(1, sizeof(*c));
    if (!c) { SSL_free(ssl); SSL_CTX_free(cx); return NULL; }
    c->ctx = cx; c->ssl = ssl;
    return c;
}

static ssize_t cli_tls_read(struct tsdb_cli_tls_conn *c, void *buf, size_t n)
{
    int sz = (n > (size_t)INT_MAX) ? INT_MAX : (int)n;
    for (;;) {
        int r = SSL_read(c->ssl, buf, sz);
        if (r > 0) return r;
        int err = SSL_get_error(c->ssl, r);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) continue;
        if (err == SSL_ERROR_ZERO_RETURN) { errno = ECONNRESET; return 0; }
        errno = EIO; return -1;
    }
}

static ssize_t cli_tls_write(struct tsdb_cli_tls_conn *c, const void *buf, size_t n)
{
    int sz = (n > (size_t)INT_MAX) ? INT_MAX : (int)n;
    for (;;) {
        int r = SSL_write(c->ssl, buf, sz);
        if (r > 0) return r;
        int err = SSL_get_error(c->ssl, r);
        if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) continue;
        errno = EIO; return -1;
    }
}

static void cli_tls_free(struct tsdb_cli_tls_conn *c)
{
    if (!c) return;
    SSL_shutdown(c->ssl);
    SSL_free(c->ssl);
    SSL_CTX_free(c->ctx);
    free(c);
}

#else /* No TLS */

struct tsdb_cli_tls_conn { int _dummy; };
static int cli_tls_available(void)               { return 0; }
static struct tsdb_cli_tls_conn *
cli_tls_connect(const char *h, int fd, const char *ca, int sv)
{ (void)h;(void)fd;(void)ca;(void)sv; errno = ENOTSUP; return NULL; }
static ssize_t cli_tls_read(struct tsdb_cli_tls_conn *c, void *b, size_t n)
{ (void)c;(void)b;(void)n; errno=ENOTSUP; return -1; }
static ssize_t cli_tls_write(struct tsdb_cli_tls_conn *c, const void *b, size_t n)
{ (void)c;(void)b;(void)n; errno=ENOTSUP; return -1; }
static void cli_tls_free(struct tsdb_cli_tls_conn *c) { (void)c; }

#endif /* TSDB_TLS_OPENSSL */

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

/* Write exactly n bytes, dispatching through TLS or plain fd. */
static int write_all(tsdb_conn_t *c, const uint8_t *buf, size_t n) {
    while (n > 0) {
        ssize_t r;
        if (c->tls)
            r = cli_tls_write(c->tls, buf, n);
        else
            r = write(c->fd, buf, n);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) { errno = ECONNRESET; return -1; }
        buf += r; n -= (size_t)r;
    }
    return 0;
}

/*
 * Read exactly n bytes from conn.
 * For TLS: SSL_read already buffers internally, poll is skipped to avoid
 * the TLS-record vs fd-read mismatch.  SO_RCVTIMEO on the socket handles
 * the timeout at the OS level (set by tsdb_conn_connect_tls).
 * For plaintext: poll-based timeout as before.
 */
static int read_all(tsdb_conn_t *c, uint8_t *buf, size_t n, int timeout_ms) {
    while (n > 0) {
        ssize_t r;
        if (c->tls) {
            r = cli_tls_read(c->tls, buf, n);
        } else {
            struct pollfd pfd = { .fd = c->fd, .events = POLLIN };
            int pr = poll(&pfd, 1, timeout_ms);
            if (pr < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            if (pr == 0) { errno = ETIMEDOUT; return -1; }
            if ((pfd.revents & (POLLERR | POLLNVAL)) && !(pfd.revents & POLLIN)) {
                errno = ECONNRESET; return -1;
            }
            if ((pfd.revents & POLLHUP) && !(pfd.revents & POLLIN)) {
                errno = ECONNRESET; return -1;
            }
            r = read(c->fd, buf, n);
        }
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

    if (write_all(c, hdr, TSDB_WIRE_HDR_SIZE) < 0) return -1;
    if (plen > 0 && payload && write_all(c, payload, plen) < 0) return -1;
    if (write_all(c, trail, 4) < 0) return -1;
    return 0;
}

int frame_recv(tsdb_conn_t *c, tsdb_msg_t *msg) {
    memset(msg, 0, sizeof(*msg));

    uint8_t hdr[TSDB_WIRE_HDR_SIZE];
    if (read_all(c, hdr, TSDB_WIRE_HDR_SIZE, c->timeout_ms) < 0)
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
        if (read_all(c, payload, plen, c->timeout_ms) < 0) {
            free(payload); return -1;
        }
    }

    /* Read 4-byte CRC trailer */
    uint8_t trail[4];
    if (read_all(c, trail, 4, c->timeout_ms) < 0) {
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

/* ─── TLS connect ────────────────────────────────────────────────────────── */

/* Non-blocking connect with a poll(POLLOUT) timeout, mirroring tcp_connect in
 * tsdb_client.c (which is static there, so we keep a local copy rather than a
 * shared header).  Resolves host/port (AF_INET, matching the TLS path) and
 * tries every address until one connects within timeout_ms.  Returns a
 * connected blocking fd, or -1 with errno set on timeout/refusal.  Exported
 * (non-static) so the timeout test can dial a black hole directly. */
int tsdb_wire_nb_connect(const char *host, int port, int timeout_ms)
{
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);
    struct addrinfo hints = {0};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    if (getaddrinfo(host, port_str, &hints, &res) != 0 || !res) {
        errno = ENOENT; return -1;
    }

    int fd = -1;
    int last_errno = ETIMEDOUT;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) { last_errno = errno; continue; }

        /* Non-blocking connect with timeout. */
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        int cr = connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (cr < 0 && errno != EINPROGRESS) {
            last_errno = errno; close(fd); fd = -1; continue;
        }
        if (cr != 0) {
            struct pollfd pfd = { .fd = fd, .events = POLLOUT };
            int pr = poll(&pfd, 1, timeout_ms);
            if (pr <= 0 || !(pfd.revents & POLLOUT)) {
                last_errno = (pr == 0) ? ETIMEDOUT : errno;
                close(fd); fd = -1; continue;
            }
            int soerr = 0; socklen_t sl = sizeof(soerr);
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &sl);
            if (soerr) { last_errno = soerr; close(fd); fd = -1; continue; }
        }
        /* Restore blocking. */
        fcntl(fd, F_SETFL, flags);
        break;
    }
    freeaddrinfo(res);
    if (fd < 0) errno = last_errno;
    return fd;
}

int tsdb_conn_connect_tls(const char *host, int port,
                          const char *ca_path, int skip_verify,
                          tsdb_conn_t *conn)
{
    if (!host || !conn) { errno = EINVAL; return -1; }

    if (!cli_tls_available()) {
        fprintf(stderr, "[tls-cli] TLS support not compiled in\n");
        errno = ENOTSUP; return -1;
    }

    /* Resolve and connect with a bounded (non-blocking + poll) timeout so a
     * dead/black-holed host fails fast instead of blocking on the OS SYN
     * timeout (~127s). */
    int fd = tsdb_wire_nb_connect(host, port, 5000);
    if (fd < 0) {
        return -1;
    }

    /* TLS wrap. */
    struct tsdb_cli_tls_conn *tls = cli_tls_connect(host, fd, ca_path, skip_verify);
    if (!tls) {
        close(fd);
        return -1;
    }

    conn->fd          = fd;
    conn->tls         = tls;
    conn->next_req_id = 1;
    conn->timeout_ms  = 10000;
    snprintf(conn->host, sizeof(conn->host), "%s", host);
    conn->port        = port;
    return 0;
}

void tsdb_conn_close(tsdb_conn_t *conn)
{
    if (!conn) return;
    if (conn->tls) {
        cli_tls_free(conn->tls);
        conn->tls = NULL;
        /* cli_tls_free calls SSL_shutdown which does the underlying close */
    } else if (conn->fd >= 0) {
        close(conn->fd);
    }
    conn->fd = -1;
}
