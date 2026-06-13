/* tls.h — opt-in TLS wrapping for tsdb TCP connections.
 *
 * Compiled in only when TSDB_TLS_OPENSSL or TSDB_TLS_MBEDTLS is defined.
 * When neither is defined every public function is a no-op stub (see below).
 *
 * Design:
 *   - tsdb_tls_ctx_t   : per-process context (cert, key, CA chain).
 *   - tsdb_tls_conn_t  : per-connection TLS state (wraps an existing TCP fd).
 *   - tsdb_io_t        : thin I/O abstraction used by proto.c so that read/write
 *                        dispatch to either the plain fd or the TLS connection.
 *
 * Lifecycle (server side):
 *   1. tsdb_tls_server_ctx(cert, key, ca) → ctx
 *   2. For each accepted fd: tsdb_tls_server_wrap(ctx, fd) → conn
 *   3. Use tsdb_tls_read / tsdb_tls_write in place of read / write.
 *   4. tsdb_tls_close(conn)  on disconnect.
 *   5. tsdb_tls_free(ctx)    on server shutdown.
 *
 * Lifecycle (client side):
 *   1. tsdb_tls_client_ctx(ca, skip_verify, cert, key) → ctx
 *   2. tsdb_tls_client_wrap(ctx, fd, hostname) → conn
 *   3. Use tsdb_tls_read / tsdb_tls_write.
 *   4. tsdb_tls_close(conn)
 *   5. tsdb_tls_free(ctx)
 */

#ifndef TSDB_SERVER_TLS_H
#define TSDB_SERVER_TLS_H

#include <stddef.h>
#include <sys/types.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque types ─────────────────────────────────────────────────────────── */

typedef struct tsdb_tls_ctx  tsdb_tls_ctx_t;
typedef struct tsdb_tls_conn tsdb_tls_conn_t;

/* ── I/O abstraction (used by proto.c) ────────────────────────────────────── */

/*
 * tsdb_io_t is the single read/write unit passed into tsdb_proto_send_io /
 * tsdb_proto_recv_io.  When tls == NULL, falls back to plain read(fd)/write(fd).
 */
typedef struct {
    int              fd;     /* underlying TCP socket */
    tsdb_tls_conn_t *tls;   /* NULL → plaintext */
    /* Per-connection session state.  Populated by TSDB_MT_AUTH_LOGIN.
     * 32 hex chars + NUL; empty string means "not authenticated yet". */
    char             auth_token[33];
} tsdb_io_t;

/* Convenient initialiser macros.  Designated initialisers zero-fill unlisted
 * fields (C99 guarantee), so auth_token starts empty. */
#define TSDB_IO_PLAIN(fd_)   ((tsdb_io_t){ .fd = (fd_), .tls = NULL })
#define TSDB_IO_TLS(fd_, c_) ((tsdb_io_t){ .fd = (fd_), .tls = (c_) })

/* Generic read/write that dispatch through TLS or plain fd. */
ssize_t tsdb_io_read (tsdb_io_t *io, void *buf, size_t n);
ssize_t tsdb_io_write(tsdb_io_t *io, const void *buf, size_t n);

/* ── TLS context ──────────────────────────────────────────────────────────── */

/*
 * Create a server-side TLS context.
 *   cert_path  — PEM certificate file (required)
 *   key_path   — PEM private key file (required)
 *   ca_path    — PEM CA bundle for mutual TLS (NULL = no client auth)
 * Returns 0 on success, -1 on error (error logged to stderr).
 */
int tsdb_tls_server_ctx(const char *cert_path,
                        const char *key_path,
                        const char *ca_path,
                        tsdb_tls_ctx_t **out);

/*
 * Create a client-side TLS context.
 *   ca_path      — PEM CA bundle (NULL = use system default)
 *   skip_verify  — non-zero to disable certificate verification (insecure)
 *   cert_path    — PEM client certificate to present (NULL = none); required
 *                  to authenticate against a peer running mutual TLS.
 *   key_path     — PEM private key matching cert_path (NULL = none).
 * Returns 0 on success, -1 on error.
 */
int tsdb_tls_client_ctx(const char *ca_path,
                        int         skip_verify,
                        const char *cert_path,
                        const char *key_path,
                        tsdb_tls_ctx_t **out);

/* Free a TLS context.  Safe to call with NULL. */
void tsdb_tls_free(tsdb_tls_ctx_t *ctx);

/* ── Per-connection wrap ──────────────────────────────────────────────────── */

/*
 * Wrap an accepted TCP fd with TLS (server side).
 * Performs the TLS handshake synchronously.
 * Returns 0 on success, -1 on error.
 */
int tsdb_tls_server_wrap(tsdb_tls_ctx_t *ctx, int fd,
                         tsdb_tls_conn_t **out);

/*
 * Wrap a connected TCP fd with TLS (client side).
 * Performs the TLS handshake synchronously.
 *   hostname — used for SNI and certificate verification; may be NULL.
 * Returns 0 on success, -1 on error.
 */
int tsdb_tls_client_wrap(tsdb_tls_ctx_t *ctx, int fd,
                         const char *hostname,
                         tsdb_tls_conn_t **out);

/* ── TLS I/O ──────────────────────────────────────────────────────────────── */

ssize_t tsdb_tls_read (tsdb_tls_conn_t *c, void *buf, size_t n);
ssize_t tsdb_tls_write(tsdb_tls_conn_t *c, const void *buf, size_t n);

/*
 * Send TLS close_notify and close the underlying fd.
 * Safe to call with NULL.
 */
void tsdb_tls_close(tsdb_tls_conn_t *c);

/* ── Availability query ───────────────────────────────────────────────────── */

/*
 * Returns 1 if TLS support was compiled in, 0 otherwise.
 * Used by tests/CLI to decide whether to attempt TLS connections.
 */
int tsdb_tls_available(void);

#ifdef __cplusplus
}
#endif
#endif /* TSDB_SERVER_TLS_H */
