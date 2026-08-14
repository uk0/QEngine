/* tls.c — TLS wrapping for tsdb TCP connections.
 *
 * Primary backend: OpenSSL 3.x  (TSDB_TLS_OPENSSL)
 * Fallback backend: mbedtls 4.x (TSDB_TLS_MBEDTLS)  -- stubbed (not yet ported)
 * No TLS: both tsdb_io_read/write fall through to plain read/write.
 *
 * The mbedtls 4.0 path is guarded by TSDB_TLS_MBEDTLS but the actual
 * implementation is not provided because mbedtls 4.0 changed its RNG/PSA
 * API incompatibly from 3.x.  The #ifdef chain falls through to the no-op
 * stubs, producing a compile-time warning only.  OpenSSL is the active path.
 */

#define _POSIX_C_SOURCE 200809L
#include "tls.h"
#include "metrics.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>

/* ────────────────────────────────────────────────────────────────────────── */
/*  I/O abstraction — always compiled regardless of TLS backend              */
/* ────────────────────────────────────────────────────────────────────────── */

ssize_t tsdb_io_read(tsdb_io_t *io, void *buf, size_t n)
{
    if (!io) { errno = EINVAL; return -1; }
    if (io->tls) return tsdb_tls_read(io->tls, buf, n);
    /* Plain fd — retry EINTR. */
    for (;;) {
        ssize_t r = read(io->fd, buf, n);
        if (r < 0 && errno == EINTR) continue;
        return r;
    }
}

ssize_t tsdb_io_write(tsdb_io_t *io, const void *buf, size_t n)
{
    if (!io) { errno = EINVAL; return -1; }
    if (io->tls) return tsdb_tls_write(io->tls, buf, n);
    /* Plain fd — retry EINTR. */
    for (;;) {
        ssize_t w = write(io->fd, buf, n);
        if (w < 0 && errno == EINTR) continue;
        return w;
    }
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  OpenSSL backend                                                           */
/* ────────────────────────────────────────────────────────────────────────── */

#if defined(TSDB_TLS_OPENSSL)

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

int tsdb_tls_available(void) { return 1; }

/* ── Context ─────────────────────────────────────────────────────────────── */

struct tsdb_tls_ctx {
    SSL_CTX *ctx;
};

static void log_ssl_errors(const char *label)
{
    unsigned long e;
    while ((e = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(e, buf, sizeof(buf));
        fprintf(stderr, "[tls] %s: %s\n", label, buf);
    }
}

int tsdb_tls_server_ctx(const char *cert_path,
                        const char *key_path,
                        const char *ca_path,
                        tsdb_tls_ctx_t **out)
{
    if (!cert_path || !key_path || !out) { errno = EINVAL; return -1; }

    SSL_CTX *cx = SSL_CTX_new(TLS_server_method());
    if (!cx) { log_ssl_errors("SSL_CTX_new"); return -1; }

    /* Load server certificate and private key. */
    if (SSL_CTX_use_certificate_chain_file(cx, cert_path) != 1) {
        log_ssl_errors("use_certificate_chain_file");
        SSL_CTX_free(cx); return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(cx, key_path, SSL_FILETYPE_PEM) != 1) {
        log_ssl_errors("use_PrivateKey_file");
        SSL_CTX_free(cx); return -1;
    }
    if (SSL_CTX_check_private_key(cx) != 1) {
        log_ssl_errors("check_private_key");
        SSL_CTX_free(cx); return -1;
    }

    /* Optional mutual TLS: load CA chain for client cert verification. */
    if (ca_path && *ca_path) {
        if (SSL_CTX_load_verify_locations(cx, ca_path, NULL) != 1) {
            log_ssl_errors("load_verify_locations (CA)");
            SSL_CTX_free(cx); return -1;
        }
        SSL_CTX_set_verify(cx,
            SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
    } else {
        SSL_CTX_set_verify(cx, SSL_VERIFY_NONE, NULL);
    }

    /* Security options: disable old protocol versions. */
    SSL_CTX_set_options(cx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 |
                            SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
    SSL_CTX_set_min_proto_version(cx, TLS1_2_VERSION);

    tsdb_tls_ctx_t *t = calloc(1, sizeof(*t));
    if (!t) { SSL_CTX_free(cx); return -1; }
    t->ctx = cx;
    *out   = t;
    return 0;
}

int tsdb_tls_client_ctx(const char *ca_path,
                        int         skip_verify,
                        const char *cert_path,
                        const char *key_path,
                        tsdb_tls_ctx_t **out)
{
    if (!out) { errno = EINVAL; return -1; }

    SSL_CTX *cx = SSL_CTX_new(TLS_client_method());
    if (!cx) { log_ssl_errors("SSL_CTX_new"); return -1; }

    SSL_CTX_set_min_proto_version(cx, TLS1_2_VERSION);
    SSL_CTX_set_options(cx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 |
                            SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);

    /* Optional client certificate — presented when the peer runs mutual TLS. */
    if (cert_path && *cert_path && key_path && *key_path) {
        if (SSL_CTX_use_certificate_chain_file(cx, cert_path) != 1) {
            log_ssl_errors("use_certificate_chain_file (client)");
            SSL_CTX_free(cx); return -1;
        }
        if (SSL_CTX_use_PrivateKey_file(cx, key_path, SSL_FILETYPE_PEM) != 1) {
            log_ssl_errors("use_PrivateKey_file (client)");
            SSL_CTX_free(cx); return -1;
        }
        if (SSL_CTX_check_private_key(cx) != 1) {
            log_ssl_errors("check_private_key (client)");
            SSL_CTX_free(cx); return -1;
        }
    }

    if (skip_verify) {
        /* LOUD BY POLICY.  SSL_VERIFY_NONE makes this client accept whatever
         * certificate the peer sends, so the peer is not authenticated at all
         * and a man in the middle is indistinguishable from the real server.
         * The hatch is kept — a bootstrap legitimately needs it — but no
         * process may run this way without saying so, or a cluster stays
         * unverified for months and only an incident reveals it.  Announced
         * at the one site that installs SSL_VERIFY_NONE, so every caller is
         * covered; src/cluster/rpc.c (TSDB_RPC_TLS_SKIP_VERIFY) is the only
         * caller in src/ today and it builds its client context once per
         * process, so this is one banner per process, not per connection. */
        fprintf(stderr,
            "[tls] ==========================================================\n"
            "[tls] == PEER CERTIFICATE VERIFICATION IS DISABLED            ==\n"
            "[tls] == This client accepts ANY certificate.  Traffic is     ==\n"
            "[tls] == encrypted but the peer is NOT authenticated, so a    ==\n"
            "[tls] == man in the middle can read and rewrite it unseen.    ==\n"
            "[tls] ==========================================================\n");
        /* Same fact, scrapeable.  See the registration in metrics.c for why a
         * banner alone is not enough. */
        tsdb_metric_gauge_set("qengine_tls_peer_verification_disabled", 1.0);
        SSL_CTX_set_verify(cx, SSL_VERIFY_NONE, NULL);
    } else {
        /* Load CA bundle: custom path or system default. */
        if (ca_path && *ca_path) {
            if (SSL_CTX_load_verify_locations(cx, ca_path, NULL) != 1) {
                log_ssl_errors("load_verify_locations (client CA)");
                SSL_CTX_free(cx); return -1;
            }
        } else {
            SSL_CTX_set_default_verify_paths(cx);
        }
        SSL_CTX_set_verify(cx, SSL_VERIFY_PEER, NULL);
    }

    tsdb_tls_ctx_t *t = calloc(1, sizeof(*t));
    if (!t) { SSL_CTX_free(cx); return -1; }
    t->ctx = cx;
    *out   = t;
    return 0;
}

void tsdb_tls_free(tsdb_tls_ctx_t *ctx)
{
    if (!ctx) return;
    SSL_CTX_free(ctx->ctx);
    free(ctx);
}

/* ── Per-connection ──────────────────────────────────────────────────────── */

struct tsdb_tls_conn {
    SSL *ssl;
    int  fd;
};

static int do_handshake(SSL *ssl, const char *side)
{
    int ret, err;
    while ((ret = SSL_do_handshake(ssl)) != 1) {
        err = SSL_get_error(ssl, ret);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE)
            continue;
        fprintf(stderr, "[tls] %s handshake failed (err=%d)\n", side, err);
        log_ssl_errors(side);
        return -1;
    }
    return 0;
}

int tsdb_tls_server_wrap(tsdb_tls_ctx_t *ctx, int fd,
                         tsdb_tls_conn_t **out)
{
    if (!ctx || fd < 0 || !out) { errno = EINVAL; return -1; }

    SSL *ssl = SSL_new(ctx->ctx);
    if (!ssl) { log_ssl_errors("SSL_new"); return -1; }

    SSL_set_fd(ssl, fd);
    SSL_set_accept_state(ssl);

    if (do_handshake(ssl, "server") != 0) {
        SSL_free(ssl); return -1;
    }

    tsdb_tls_conn_t *c = calloc(1, sizeof(*c));
    if (!c) { SSL_free(ssl); return -1; }
    c->ssl = ssl;
    c->fd  = fd;
    *out   = c;
    return 0;
}

int tsdb_tls_client_wrap(tsdb_tls_ctx_t *ctx, int fd,
                         const char *hostname,
                         tsdb_tls_conn_t **out)
{
    if (!ctx || fd < 0 || !out) { errno = EINVAL; return -1; }

    SSL *ssl = SSL_new(ctx->ctx);
    if (!ssl) { log_ssl_errors("SSL_new"); return -1; }

    SSL_set_fd(ssl, fd);
    SSL_set_connect_state(ssl);

    /* SNI + hostname verification. */
    if (hostname && *hostname) {
        SSL_set_tlsext_host_name(ssl, hostname);
        SSL_set1_host(ssl, hostname);
    }

    if (do_handshake(ssl, "client") != 0) {
        SSL_free(ssl); return -1;
    }

    tsdb_tls_conn_t *c = calloc(1, sizeof(*c));
    if (!c) { SSL_free(ssl); return -1; }
    c->ssl = ssl;
    c->fd  = fd;
    *out   = c;
    return 0;
}

/* ── I/O ─────────────────────────────────────────────────────────────────── */

ssize_t tsdb_tls_read(tsdb_tls_conn_t *c, void *buf, size_t n)
{
    if (!c) { errno = EINVAL; return -1; }
    /* OpenSSL SSL_read returns int, cap at INT_MAX. */
    int sz = (n > (size_t)INT_MAX) ? INT_MAX : (int)n;
    int ret;
    for (;;) {
        ret = SSL_read(c->ssl, buf, sz);
        if (ret > 0) return (ssize_t)ret;
        int err = SSL_get_error(c->ssl, ret);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE)
            continue;
        if (err == SSL_ERROR_ZERO_RETURN) { /* clean shutdown */
            errno = ECONNRESET;
            return 0;
        }
        errno = EIO;
        return -1;
    }
}

ssize_t tsdb_tls_write(tsdb_tls_conn_t *c, const void *buf, size_t n)
{
    if (!c) { errno = EINVAL; return -1; }
    int sz = (n > (size_t)INT_MAX) ? INT_MAX : (int)n;
    int ret;
    for (;;) {
        ret = SSL_write(c->ssl, buf, sz);
        if (ret > 0) return (ssize_t)ret;
        int err = SSL_get_error(c->ssl, ret);
        if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ)
            continue;
        errno = EIO;
        return -1;
    }
}

void tsdb_tls_close(tsdb_tls_conn_t *c)
{
    if (!c) return;
    SSL_shutdown(c->ssl);
    SSL_free(c->ssl);
    close(c->fd);
    free(c);
}

/* ────────────────────────────────────────────────────────────────────────── */
#elif defined(TSDB_TLS_MBEDTLS)
/*
 * mbedtls 4.0 backend — NOT YET IMPLEMENTED.
 * mbedtls 4.0 replaced the ctr_drbg/entropy RNG wiring with PSA Crypto and
 * removed mbedtls_ssl_conf_defaults / mbedtls_ssl_conf_rng.
 * The API requires significant porting work beyond the scope of this change.
 * The Makefile falls back to OpenSSL when mbedtls is the only option available;
 * this path is left as a compile-time stub that emits a diagnostic.
 */
#warning "TSDB_TLS_MBEDTLS: mbedtls 4.0 backend not yet implemented; falling back to no-op stubs"

int tsdb_tls_available(void) { return 0; }

struct tsdb_tls_ctx  { int _dummy; };
struct tsdb_tls_conn { int _dummy; };

int tsdb_tls_server_ctx(const char *c, const char *k, const char *ca,
                        tsdb_tls_ctx_t **o)
{ (void)c;(void)k;(void)ca;(void)o; fprintf(stderr,"[tls] mbedtls not available\n"); return -1; }
int tsdb_tls_client_ctx(const char *ca, int sv, const char *cert, const char *key,
                        tsdb_tls_ctx_t **o)
{ (void)ca;(void)sv;(void)cert;(void)key;(void)o; fprintf(stderr,"[tls] mbedtls not available\n"); return -1; }
void tsdb_tls_free(tsdb_tls_ctx_t *ctx) { (void)ctx; }
int tsdb_tls_server_wrap(tsdb_tls_ctx_t *x, int fd, tsdb_tls_conn_t **o)
{ (void)x;(void)fd;(void)o; return -1; }
int tsdb_tls_client_wrap(tsdb_tls_ctx_t *x, int fd, const char *h, tsdb_tls_conn_t **o)
{ (void)x;(void)fd;(void)h;(void)o; return -1; }
ssize_t tsdb_tls_read (tsdb_tls_conn_t *c, void *b, size_t n)  { (void)c;(void)b;(void)n; errno=ENOTSUP; return -1; }
ssize_t tsdb_tls_write(tsdb_tls_conn_t *c, const void *b, size_t n) { (void)c;(void)b;(void)n; errno=ENOTSUP; return -1; }
void    tsdb_tls_close(tsdb_tls_conn_t *c) { (void)c; }

/* ────────────────────────────────────────────────────────────────────────── */
#else
/* ── No TLS compiled in — all stubs ─────────────────────────────────────── */

int tsdb_tls_available(void) { return 0; }

struct tsdb_tls_ctx  { int _dummy; };
struct tsdb_tls_conn { int _dummy; };

int tsdb_tls_server_ctx(const char *c, const char *k, const char *ca,
                        tsdb_tls_ctx_t **o)
{ (void)c;(void)k;(void)ca;(void)o; errno=ENOTSUP; return -1; }
int tsdb_tls_client_ctx(const char *ca, int sv, const char *cert, const char *key,
                        tsdb_tls_ctx_t **o)
{ (void)ca;(void)sv;(void)cert;(void)key;(void)o; errno=ENOTSUP; return -1; }
void tsdb_tls_free(tsdb_tls_ctx_t *ctx) { (void)ctx; }
int tsdb_tls_server_wrap(tsdb_tls_ctx_t *x, int fd, tsdb_tls_conn_t **o)
{ (void)x;(void)fd;(void)o; errno=ENOTSUP; return -1; }
int tsdb_tls_client_wrap(tsdb_tls_ctx_t *x, int fd, const char *h, tsdb_tls_conn_t **o)
{ (void)x;(void)fd;(void)h;(void)o; errno=ENOTSUP; return -1; }
ssize_t tsdb_tls_read (tsdb_tls_conn_t *c, void *b, size_t n)  { (void)c;(void)b;(void)n; errno=ENOTSUP; return -1; }
ssize_t tsdb_tls_write(tsdb_tls_conn_t *c, const void *b, size_t n) { (void)c;(void)b;(void)n; errno=ENOTSUP; return -1; }
void    tsdb_tls_close(tsdb_tls_conn_t *c) { (void)c; }

#endif /* TSDB_TLS_OPENSSL / TSDB_TLS_MBEDTLS / no-TLS */
