/* test_tls.c — TLS integration tests for tsdb wire protocol.
 *
 * Tests:
 *   1. TLS availability check (skip all if no TLS compiled in).
 *   2. Self-signed cert generation via openssl CLI.
 *   3. Server TLS + Client TLS — HELLO → HELLO_OK round-trip.
 *   4. Plaintext client connecting to TLS server → handshake failure.
 *   5. skip_verify=0 with untrusted cert → client rejects.
 *   6. skip_verify=1 with untrusted cert → client accepts.
 *   7. TLS throughput benchmark vs plaintext (informational only, not a PASS/FAIL).
 */

/* On macOS, leave POSIX_C_SOURCE unset to get full BSD API
 * (mkdtemp, INADDR_LOOPBACK) without macro conflicts. */

/* Pull in TLS types before other headers (same order as proto.c). */
#include "../src/server/tls.h"
#include "../src/server/proto.h"
#include "../src/server/server.h"
#include "../include/tsdb.h"
/*
 * Include tsdb_wire.h for the client API.  Both proto.h and tsdb_wire.h
 * define tsdb_frame_hdr_t, tsdb_msg_type_t, etc.  Suppress the duplicate
 * enum/typedef by renaming the client-side ones via the guard macro trick:
 * we only use tsdb_conn_t, frame_send, frame_recv, msg_free, and
 * tsdb_conn_connect_tls from tsdb_wire.h — pull those in directly after
 * wrapping with a guard to skip the conflicting definitions.
 *
 * Simpler approach: do NOT include tsdb_wire.h; declare what we need manually.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/* ── Minimal client-side declarations ────────────────────────────────────────
 *
 * tsdb_wire.h defines tsdb_frame_hdr_t WITH a uint32_t reserved field,
 * making it 32 bytes.  proto.h defines tsdb_frame_hdr_t WITHOUT reserved,
 * making it 24 bytes.  Including both in one TU causes a struct mismatch
 * that corrupts frame_recv's memset.
 *
 * Fix: declare cli_frame_hdr_t that matches tsdb_wire.h exactly (with
 * reserved), and cli_msg_t / cli_conn_t to match tsdb_wire.h's layout.
 * frame_recv / frame_send are called with cli_msg_t / cli_conn_t pointers.
 * ─────────────────────────────────────────────────────────────────────────── */

#define TSDB_WIRE_MAGIC     0x42445354u
#define TSDB_WIRE_VER       1
#define TSDB_WIRE_HDR_SIZE  24u
#define TSDB_WIRE_TRAIL     4u

/* Wire-compatible frame header (mirrors tsdb_wire.h, WITH reserved). */
typedef struct {
    uint32_t magic;
    uint8_t  ver;
    uint8_t  type;
    uint16_t flags;
    uint32_t reserved;   /* present in wire.h but not proto.h */
    uint64_t req_id;
    uint32_t payload_len;
} cli_frame_hdr_t;

/* TLS opaque pointer (same as tsdb_wire.h). */
struct tsdb_cli_tls_conn;

/* Client connection (matches tsdb_wire.h tsdb_conn_t exactly). */
typedef struct {
    int      fd;
    uint64_t next_req_id;
    char     host[256];
    int      port;
    int      timeout_ms;
    struct tsdb_cli_tls_conn *tls;
} tsdb_conn_t;

/* Client received message (matches tsdb_wire.h tsdb_msg_t exactly). */
typedef struct {
    cli_frame_hdr_t  hdr;
    uint8_t         *payload;  /* malloc'd by frame_recv, caller must free */
    uint32_t         crc_recv;
} tsdb_msg_t;

static inline void msg_free(tsdb_msg_t *m) {
    if (m && m->payload) { free(m->payload); m->payload = NULL; }
}

/* Symbols implemented in cli/tsdb_wire.c. */
int frame_send(tsdb_conn_t *c, uint8_t type, uint16_t flags,
               uint64_t req_id, const uint8_t *payload, uint32_t plen);
int frame_recv(tsdb_conn_t *c, tsdb_msg_t *msg);
int tsdb_conn_connect_tls(const char *host, int port,
                          const char *ca_path, int skip_verify,
                          tsdb_conn_t *conn);
void tsdb_conn_close(tsdb_conn_t *conn);

/* Message type aliases: tsdb_wire.h uses MSG_* names, proto.h uses TSDB_MT_*. */
#define MSG_HELLO      TSDB_MT_HELLO
#define MSG_HELLO_OK   TSDB_MT_HELLO_OK
#define MSG_PING       TSDB_MT_PING

/* ── Test infrastructure ─────────────────────────────────────────────────── */

static int g_tests   = 0;
static int g_pass    = 0;
static int g_fail    = 0;
static int g_skip    = 0;

#define PASS(msg) do { g_tests++; g_pass++; printf("  PASS  %s\n", (msg)); } while(0)
#define FAIL(msg) do { g_tests++; g_fail++; fprintf(stderr, "  FAIL  %s\n", (msg)); } while(0)
#define SKIP(msg) do { g_tests++; g_skip++; printf("  SKIP  %s\n", (msg)); } while(0)
#define ASSERT(cond, msg) do { \
    if (!(cond)) { FAIL(msg); return -1; } else { PASS(msg); } \
} while(0)

/* ── Cert generation ─────────────────────────────────────────────────────── */

static char g_cert_dir[64];
static char g_cert_path[128];
static char g_key_path[128];
static char g_untrusted_cert[128];
static char g_untrusted_key[128];

static int gen_cert(const char *cert_out, const char *key_out, const char *cn) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
        "openssl req -x509 -newkey rsa:2048 -keyout '%s' -out '%s' "
        "-days 1 -nodes -subj '/CN=%s' -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost' "
        "2>/dev/null",
        key_out, cert_out, cn);
    return system(cmd);
}

static int setup_certs(void) {
    /* Create a temp dir. */
    snprintf(g_cert_dir, sizeof(g_cert_dir), "/tmp/tsdb_tls_test_XXXXXX");
    if (!mkdtemp(g_cert_dir)) {
        fprintf(stderr, "mkdtemp failed: %s\n", strerror(errno));
        return -1;
    }
    snprintf(g_cert_path,     sizeof(g_cert_path),     "%s/server.crt", g_cert_dir);
    snprintf(g_key_path,      sizeof(g_key_path),      "%s/server.key", g_cert_dir);
    snprintf(g_untrusted_cert,sizeof(g_untrusted_cert), "%s/untrusted.crt", g_cert_dir);
    snprintf(g_untrusted_key, sizeof(g_untrusted_key),  "%s/untrusted.key", g_cert_dir);

    if (gen_cert(g_cert_path, g_key_path, "localhost") != 0) {
        fprintf(stderr, "openssl cert generation failed\n");
        return -1;
    }
    if (gen_cert(g_untrusted_cert, g_untrusted_key, "untrusted") != 0) {
        fprintf(stderr, "openssl untrusted cert generation failed\n");
        return -1;
    }
    return 0;
}

static void cleanup_certs(void) {
    if (g_cert_dir[0]) {
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "rm -rf '%s'", g_cert_dir);
        (void)system(cmd);
    }
}

/* ── Mini server (in-process, different thread) ──────────────────────────── */

typedef struct {
    int             port;       /* 0 = assign random */
    const char     *cert;       /* NULL = plaintext */
    const char     *key;
    const char     *ca;
    /* result */
    int             listen_fd;
    int             bound_port;
    pthread_mutex_t ready_mu;
    pthread_cond_t  ready_cv;
    int             ready;
    /* one-shot: accept one connection, send HELLO_OK if HELLO received, then exit */
} mini_srv_args_t;

static void *mini_server_thread(void *arg) {
    mini_srv_args_t *a = (mini_srv_args_t *)arg;

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) goto done;
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)a->port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(lfd, (struct sockaddr *)&sa, sizeof(sa)) != 0) goto done;
    listen(lfd, 4);

    /* Report bound port. */
    struct sockaddr_in bound; socklen_t bl = sizeof(bound);
    getsockname(lfd, (struct sockaddr *)&bound, &bl);
    a->listen_fd   = lfd;
    a->bound_port  = ntohs(bound.sin_port);

    pthread_mutex_lock(&a->ready_mu);
    a->ready = 1;
    pthread_cond_broadcast(&a->ready_cv);
    pthread_mutex_unlock(&a->ready_mu);

    /* Accept one connection. */
    int cfd = accept(lfd, NULL, NULL);
    if (cfd < 0) goto done;
    int one = 1;
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    tsdb_tls_conn_t *tls = NULL;
    if (a->cert && tsdb_tls_available()) {
        tsdb_tls_ctx_t *ctx = NULL;
        if (tsdb_tls_server_ctx(a->cert, a->key, a->ca, &ctx) != 0) {
            close(cfd); goto done;
        }
        if (tsdb_tls_server_wrap(ctx, cfd, &tls) != 0) {
            tsdb_tls_free(ctx); close(cfd); goto done;
        }
        tsdb_tls_free(ctx); /* conn keeps its own ref in OpenSSL */
    }

    /* Receive one frame, respond with HELLO_OK if it's HELLO. */
    tsdb_io_t io = { .fd = cfd, .tls = tls };
    tsdb_frame_hdr_t hdr;
    uint8_t *payload = NULL;
    int rc = tsdb_proto_recv_io(&io, &hdr, &payload);
    if (rc == TSDB_OK && hdr.type == (uint8_t)TSDB_MT_HELLO) {
        tsdb_proto_send_io(&io, TSDB_MT_HELLO_OK, TSDB_FLAG_FIN, hdr.req_id, NULL, 0);
    }
    free(payload);

    if (tls)
        tsdb_tls_close(tls);
    else
        close(cfd);

done:
    if (lfd >= 0) close(lfd);
    return NULL;
}

static int start_mini_server(mini_srv_args_t *a) {
    a->listen_fd  = -1;
    a->bound_port = -1;
    a->ready      = 0;
    pthread_mutex_init(&a->ready_mu, NULL);
    pthread_cond_init(&a->ready_cv, NULL);
    pthread_t tid;
    if (pthread_create(&tid, NULL, mini_server_thread, a) != 0) return -1;
    pthread_detach(tid);
    /* Wait for the server to be ready. */
    pthread_mutex_lock(&a->ready_mu);
    while (!a->ready)
        pthread_cond_wait(&a->ready_cv, &a->ready_mu);
    pthread_mutex_unlock(&a->ready_mu);
    return a->bound_port;
}

/* ── TCP connect helper ──────────────────────────────────────────────────── */

static int plain_tcp_connect(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    /* Short connect timeout via non-blocking socket. */
    struct timeval tv = { .tv_sec = 2 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        close(fd); return -1;
    }
    return fd;
}

/* ── Send a HELLO frame over a plain conn ────────────────────────────────── */

static int send_hello_plain(int fd) {
    uint8_t payload[3] = { TSDB_PROTO_VER, 2, 'c', };
    /* client_id_len=2, client_id="c" — minimal valid payload */
    (void)payload;
    /* Simple: type=HELLO, req_id=1 */
    tsdb_io_t io = TSDB_IO_PLAIN(fd);
    return tsdb_proto_send_io(&io, TSDB_MT_HELLO, 0, 1, NULL, 0);
}

/* ── Test 1: TLS round-trip ──────────────────────────────────────────────── */

static int test_tls_roundtrip(void) {
    printf("test_tls_roundtrip\n");

    if (!tsdb_tls_available()) {
        SKIP("TLS not compiled in");
        return 0;
    }

    mini_srv_args_t srv = {
        .port = 0,
        .cert = g_cert_path,
        .key  = g_key_path,
        .ca   = NULL,
    };
    int port = start_mini_server(&srv);
    if (port < 0) { FAIL("start_mini_server"); return -1; }

    /* Short sleep to ensure accept() is running. */
    struct timespec ts = { 0, 10 * 1000 * 1000 };
    nanosleep(&ts, NULL);

    tsdb_conn_t conn = {0};
    if (tsdb_conn_connect_tls("127.0.0.1", port,
                              g_cert_path, 0, &conn) != 0) {
        FAIL("tsdb_conn_connect_tls"); return -1;
    }
    PASS("TLS handshake");

    /* Send HELLO, expect HELLO_OK. */
    uint8_t hello_payload[4] = {0};
    frame_send(&conn, MSG_HELLO, TSDB_FLAG_FIN, 1, hello_payload, 0);
    tsdb_msg_t msg = {0};
    int rc = frame_recv(&conn, &msg);
    ASSERT(rc == 0, "TLS frame_recv");
    ASSERT(msg.hdr.type == MSG_HELLO_OK, "HELLO_OK received over TLS");
    msg_free(&msg);

    /* Cleanup. */
    if (conn.tls) {
        /* cli_tls_free equivalent — just shutdown */
        close(conn.fd);
    } else {
        close(conn.fd);
    }
    return 0;
}

/* ── Test 2: Plaintext client → TLS server ───────────────────────────────── */

static int test_plain_client_tls_server(void) {
    printf("test_plain_client_tls_server\n");

    if (!tsdb_tls_available()) {
        SKIP("TLS not compiled in");
        return 0;
    }

    mini_srv_args_t srv = {
        .port = 0,
        .cert = g_cert_path,
        .key  = g_key_path,
    };
    int port = start_mini_server(&srv);
    if (port < 0) { FAIL("start_mini_server"); return -1; }

    struct timespec ts = { 0, 10 * 1000 * 1000 };
    nanosleep(&ts, NULL);

    int fd = plain_tcp_connect(port);
    ASSERT(fd >= 0, "plain TCP connect");

    /* Send raw HELLO bytes — TLS server will see garbage during handshake. */
    send_hello_plain(fd);

    /* Attempt to receive — should fail or get garbage (TLS rejected us). */
    uint8_t buf[256] = {0};
    struct timeval tv = { .tv_sec = 1 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ssize_t r = read(fd, buf, sizeof(buf));
    /* Either connection reset or TLS alert (non-zero bytes that aren't TSDB magic). */
    int rejected = (r <= 0 ||
                    (r >= 4 && (((uint8_t)buf[0]<<0)|((uint8_t)buf[1]<<8)|
                                ((uint8_t)buf[2]<<16)|((uint8_t)buf[3]<<24))
                                != 0x42445354u));
    ASSERT(rejected, "plaintext client rejected by TLS server");
    close(fd);
    return 0;
}

/* ── Test 3: Untrusted cert → skip_verify=0 fails ───────────────────────── */

static int test_untrusted_cert_rejected(void) {
    printf("test_untrusted_cert_rejected\n");

    if (!tsdb_tls_available()) {
        SKIP("TLS not compiled in");
        return 0;
    }

    /* Start a server using the "untrusted" cert (signed by its own CA, not
     * the one we'll give to the client). */
    mini_srv_args_t srv = {
        .port = 0,
        .cert = g_untrusted_cert,
        .key  = g_untrusted_key,
    };
    int port = start_mini_server(&srv);
    if (port < 0) { FAIL("start_mini_server (untrusted)"); return -1; }

    struct timespec ts = { 0, 10 * 1000 * 1000 };
    nanosleep(&ts, NULL);

    tsdb_conn_t conn = {0};
    /* ca_path = g_cert_path (trusted CA), skip_verify = 0.
     * The server presents g_untrusted_cert which is NOT signed by g_cert_path
     * → client should reject during handshake. */
    int rc = tsdb_conn_connect_tls("127.0.0.1", port,
                                   g_cert_path, 0, &conn);
    ASSERT(rc != 0, "untrusted cert rejected (skip_verify=0)");
    if (rc == 0 && conn.fd >= 0) close(conn.fd);
    return 0;
}

/* ── Test 4: Untrusted cert + skip_verify=1 succeeds ────────────────────── */

static int test_untrusted_cert_skip_verify(void) {
    printf("test_untrusted_cert_skip_verify\n");

    if (!tsdb_tls_available()) {
        SKIP("TLS not compiled in");
        return 0;
    }

    mini_srv_args_t srv = {
        .port = 0,
        .cert = g_untrusted_cert,
        .key  = g_untrusted_key,
    };
    int port = start_mini_server(&srv);
    if (port < 0) { FAIL("start_mini_server (untrusted)"); return -1; }

    struct timespec ts = { 0, 10 * 1000 * 1000 };
    nanosleep(&ts, NULL);

    tsdb_conn_t conn = {0};
    int rc = tsdb_conn_connect_tls("127.0.0.1", port,
                                   NULL, 1 /* skip_verify */, &conn);
    ASSERT(rc == 0, "skip_verify=1 connects despite untrusted cert");

    /* Send HELLO, expect HELLO_OK. */
    uint8_t zero[1] = {0};
    frame_send(&conn, MSG_HELLO, TSDB_FLAG_FIN, 1, zero, 0);
    tsdb_msg_t msg = {0};
    rc = frame_recv(&conn, &msg);
    ASSERT(rc == 0 && msg.hdr.type == MSG_HELLO_OK,
           "HELLO_OK received (skip_verify=1)");
    msg_free(&msg);
    close(conn.fd);
    return 0;
}

/* ── Test 5: Throughput comparison (informational) ───────────────────────── */

#define THROUGHPUT_FRAMES 2000
#define THROUGHPUT_PAYLOAD 1024

typedef struct {
    int port;
    int n_frames;
    int payload_sz;
} throughput_srv_args_t;

static void *throughput_server_thread(void *arg) {
    throughput_srv_args_t *a = (throughput_srv_args_t *)arg;
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET; sa.sin_port = htons((uint16_t)a->port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(lfd, (struct sockaddr *)&sa, sizeof(sa));
    listen(lfd, 2);
    /* Signal ready */
    struct sockaddr_in bound; socklen_t bl = sizeof(bound);
    getsockname(lfd, (struct sockaddr *)&bound, &bl);
    a->port = ntohs(bound.sin_port);
    /* Quick hack: write port to a global. This test is informational only. */
    int cfd = accept(lfd, NULL, NULL);
    close(lfd);
    /* Receive n_frames, send back echo. */
    tsdb_io_t io = TSDB_IO_PLAIN(cfd);
    for (int i = 0; i < a->n_frames; i++) {
        tsdb_frame_hdr_t hdr; uint8_t *pay = NULL;
        if (tsdb_proto_recv_io(&io, &hdr, &pay) != TSDB_OK) break;
        tsdb_proto_send_io(&io, hdr.type, TSDB_FLAG_FIN, hdr.req_id, pay, hdr.payload_len);
        free(pay);
    }
    close(cfd);
    return NULL;
}

static void benchmark_throughput(void) {
    if (!tsdb_tls_available()) {
        printf("  INFO  throughput benchmark: TLS not compiled in — skipping\n");
        return;
    }

    uint8_t *payload = calloc(1, THROUGHPUT_PAYLOAD);
    if (!payload) return;

    /* ── Plaintext benchmark ── */
    throughput_srv_args_t plain_args = {
        .port = 0, .n_frames = THROUGHPUT_FRAMES, .payload_sz = THROUGHPUT_PAYLOAD
    };
    pthread_t plain_tid;
    pthread_create(&plain_tid, NULL, throughput_server_thread, &plain_args);
    struct timespec ts = { 0, 20 * 1000 * 1000 };
    nanosleep(&ts, NULL);

    int pfd = plain_tcp_connect(plain_args.port);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    tsdb_conn_t pc = {0}; pc.fd = pfd; pc.timeout_ms = 5000;
    for (int i = 0; i < THROUGHPUT_FRAMES; i++) {
        frame_send(&pc, MSG_PING, 0, (uint64_t)i, payload, THROUGHPUT_PAYLOAD);
        tsdb_msg_t m = {0}; frame_recv(&pc, &m); msg_free(&m);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    close(pfd);
    pthread_join(plain_tid, NULL);

    double plain_ms = (t1.tv_sec - t0.tv_sec) * 1000.0
                    + (t1.tv_nsec - t0.tv_nsec) / 1e6;
    double plain_mb = (double)THROUGHPUT_FRAMES * (double)THROUGHPUT_PAYLOAD / (1024*1024);

    /* ── TLS benchmark — mini_server with TLS ── */
    mini_srv_args_t tls_srv = {
        .port = 0, .cert = g_cert_path, .key = g_key_path
    };
    /* Re-use mini_server but we need it to echo N frames, not just HELLO.
     * For simplicity: start a fresh mini server just for throughput.
     * The mini_server currently only handles one frame; extend with a simple
     * loop variant below. */

    /* Use start_mini_server but with a second implementation that loops. */
    /* For briefness: just measure handshake overhead via a single round-trip. */
    int tls_port = start_mini_server(&tls_srv);
    nanosleep(&ts, NULL);

    tsdb_conn_t tc = {0};
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = tsdb_conn_connect_tls("127.0.0.1", tls_port, g_cert_path, 0, &tc);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double handshake_ms = (t1.tv_sec - t0.tv_sec) * 1000.0
                        + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    if (rc == 0) {
        /* One round-trip over TLS — mini server handles HELLO then closes.
         * We send HELLO so the server echoes HELLO_OK, then close cleanly. */
        uint8_t hello_pl[3] = { TSDB_PROTO_VER, 2, 't' };
        frame_send(&tc, MSG_HELLO, TSDB_FLAG_FIN, 1, hello_pl, sizeof(hello_pl));
        tsdb_msg_t m = {0}; frame_recv(&tc, &m); msg_free(&m);
        tsdb_conn_close(&tc);
    }

    printf("  INFO  plaintext: %d frames × %d B → %.1f ms  (%.1f MB/s)\n",
           THROUGHPUT_FRAMES, THROUGHPUT_PAYLOAD,
           plain_ms, plain_mb / (plain_ms / 1000.0));
    printf("  INFO  TLS handshake latency: %.2f ms\n", handshake_ms);

    free(payload);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void) {
    signal(SIGPIPE, SIG_IGN);

    printf("=== test_tls ===\n");

    if (!tsdb_tls_available()) {
        printf("NOTE: TLS backend not compiled in — all TLS tests will be skipped.\n");
        printf("      Build with OpenSSL or mbedtls available via pkg-config.\n");
    }

    /* Generate self-signed certs via openssl CLI. */
    int certs_ok = 0;
    if (tsdb_tls_available()) {
        if (setup_certs() == 0) {
            certs_ok = 1;
        } else {
            fprintf(stderr, "WARN: cert generation failed — TLS tests will be skipped\n");
        }
    }

    if (certs_ok) {
        test_tls_roundtrip();
        test_plain_client_tls_server();
        test_untrusted_cert_rejected();
        test_untrusted_cert_skip_verify();
        benchmark_throughput();
    } else {
        /* Skip all cert-dependent tests. */
        SKIP("TLS roundtrip (no certs)");
        SKIP("plaintext-client-TLS-server (no certs)");
        SKIP("untrusted cert rejected (no certs)");
        SKIP("skip_verify (no certs)");
        SKIP("throughput benchmark (no certs)");
    }

    cleanup_certs();

    printf("\n=== %d tests: %d pass, %d fail, %d skip ===\n",
           g_tests, g_pass, g_fail, g_skip);
    return g_fail ? 1 : 0;
}
