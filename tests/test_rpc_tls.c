/* test_rpc_tls.c — mutual TLS for the inter-node cluster RPC channel.
 *
 * The peer RPC port (src/cluster/rpc.c) is trust-by-network-boundary by
 * default.  This test exercises the OPTIONAL mutual-TLS mode gated by
 * TSDB_RPC_TLS=1 (+ _CERT/_KEY/_CA/_SKIP_VERIFY).  Default-off behavior is
 * covered by the rest of the suite (no other test sets TSDB_RPC_TLS).
 *
 * Process model: the RPC server runs in its OWN forked child (server env +
 * the once-cached server TLS ctx live there), the client runs in another
 * forked child (its own env + client TLS ctx) — exactly the two-process
 * shape of a real cluster, and the only way to give the two roles distinct
 * env / cert material without the lazy per-process ctx cache colliding.  The
 * server child publishes its bound port over a pipe.
 *
 * Scenarios:
 *   1. TLS round-trip:  server TLS on + client TLS on → HEARTBEAT ACK.
 *   2. plaintext client → TLS server  → rejected (no valid RPC reply).
 *   3. TLS client with NO client cert → mutual-TLS server → connect fails.
 *
 * Cert material: an ephemeral self-signed CA plus a server cert and a client
 * cert both signed by that CA (SAN covers loopback), generated under a /tmp
 * mkdtemp dir via the openssl CLI — the chained extension of the self-signed
 * pattern in tests/test_tls.c.
 */

#include "../src/cluster/rpc.h"
#include "../src/server/tls.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/* ── Test infra ──────────────────────────────────────────────────────────── */

static int g_tests = 0, g_pass = 0, g_fail = 0, g_skip = 0;
#define PASS(m) do { g_tests++; g_pass++; printf("  PASS  %s\n", (m)); } while (0)
#define FAIL(m) do { g_tests++; g_fail++; fprintf(stderr, "  FAIL  %s\n", (m)); } while (0)
#define SKIP(m) do { g_tests++; g_skip++; printf("  SKIP  %s\n", (m)); } while (0)

/* ── Cert generation: self-signed CA + CA-signed server & client certs ────── */

static char g_dir[64];
static char g_ca_cert[128],  g_ca_key[128];
static char g_srv_cert[128], g_srv_key[128];
static char g_cli_cert[128], g_cli_key[128];

static int run(const char *cmd) { return system(cmd); }

static int gen_ca(void) {
    char c[1024];
    snprintf(c, sizeof(c),
        "openssl req -x509 -newkey rsa:2048 -keyout '%s' -out '%s' "
        "-days 1 -nodes -subj '/CN=tsdb-test-ca' 2>/dev/null",
        g_ca_key, g_ca_cert);
    return run(c);
}

/* Key + CSR for `cn`, signed by the test CA.  SAN covers the loopback the
 * client uses for hostname verification.  Uses an extfile on disk (no bash
 * process substitution), so /bin/sh suffices. */
static int gen_signed(const char *cert, const char *key, const char *cn) {
    char ext[160];
    snprintf(ext, sizeof(ext), "%s/%s.ext", g_dir, cn);
    FILE *f = fopen(ext, "w");
    if (!f) return -1;
    fputs("subjectAltName=IP:127.0.0.1,DNS:localhost\n", f);
    fclose(f);

    char csr[160];
    snprintf(csr, sizeof(csr), "%s/%s.csr", g_dir, cn);

    char c[2048];
    snprintf(c, sizeof(c),
        "openssl req -newkey rsa:2048 -keyout '%s' -out '%s' -nodes "
        "-subj '/CN=%s' 2>/dev/null && "
        "openssl x509 -req -in '%s' -CA '%s' -CAkey '%s' -CAcreateserial "
        "-out '%s' -days 1 -extfile '%s' 2>/dev/null",
        key, csr, cn, csr, g_ca_cert, g_ca_key, cert, ext);
    return run(c);
}

static int setup_certs(void) {
    snprintf(g_dir, sizeof(g_dir), "/tmp/tsdb_rpc_tls_XXXXXX");
    if (!mkdtemp(g_dir)) { fprintf(stderr, "mkdtemp: %s\n", strerror(errno)); return -1; }
    snprintf(g_ca_cert,  sizeof(g_ca_cert),  "%s/ca.crt",  g_dir);
    snprintf(g_ca_key,   sizeof(g_ca_key),   "%s/ca.key",  g_dir);
    snprintf(g_srv_cert, sizeof(g_srv_cert), "%s/srv.crt", g_dir);
    snprintf(g_srv_key,  sizeof(g_srv_key),  "%s/srv.key", g_dir);
    snprintf(g_cli_cert, sizeof(g_cli_cert), "%s/cli.crt", g_dir);
    snprintf(g_cli_key,  sizeof(g_cli_key),  "%s/cli.key", g_dir);

    if (gen_ca() != 0) { fprintf(stderr, "gen CA failed\n"); return -1; }
    if (gen_signed(g_srv_cert, g_srv_key, "server") != 0) {
        fprintf(stderr, "gen server cert failed\n"); return -1;
    }
    if (gen_signed(g_cli_cert, g_cli_key, "client") != 0) {
        fprintf(stderr, "gen client cert failed\n"); return -1;
    }
    return 0;
}

static void cleanup_certs(void) {
    if (g_dir[0]) {
        char c[256];
        snprintf(c, sizeof(c), "rm -rf '%s'", g_dir);
        (void)run(c);
    }
}

/* ── Server child: mutual-TLS RPC server, publishes its port over a pipe ──── */

/* Fork a server child that sets mutual-TLS server env, starts the RPC server,
 * writes the bound port (int) to the pipe, then blocks serving until killed.
 * Returns the child pid; *port_out receives the bound port. */
static pid_t spawn_tls_server(int *port_out) {
    int pfd[2];
    if (pipe(pfd) != 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(pfd[0]); close(pfd[1]); return -1; }

    if (pid == 0) {
        close(pfd[0]);
        signal(SIGPIPE, SIG_IGN);
        setenv("TSDB_RPC_TLS", "1", 1);
        setenv("TSDB_RPC_TLS_CA",   g_ca_cert, 1);
        setenv("TSDB_RPC_TLS_CERT", g_srv_cert, 1);
        setenv("TSDB_RPC_TLS_KEY",  g_srv_key, 1);

        tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", NULL, NULL);
        int port = srv ? tsdb_rpc_server_port(srv) : -1;
        ssize_t w = write(pfd[1], &port, sizeof(port)); (void)w;
        if (!srv) _exit(2);
        /* Serve until the parent kills us. */
        for (;;) pause();
        _exit(0);
    }

    close(pfd[1]);
    int port = -1;
    ssize_t r = read(pfd[0], &port, sizeof(port));
    close(pfd[0]);
    if (r != (ssize_t)sizeof(port) || port <= 0) {
        kill(pid, SIGKILL); waitpid(pid, NULL, 0);
        return -1;
    }
    *port_out = port;
    return pid;
}

static void kill_server(pid_t pid) {
    if (pid > 0) { kill(pid, SIGKILL); waitpid(pid, NULL, 0); }
}

/* ── Client children (each forked so its env + once-cached client ctx is fresh) */

/* Scenario 1: full mutual-TLS client → HEARTBEAT round-trip. */
static int cli_roundtrip(int port) {
    setenv("TSDB_RPC_TLS", "1", 1);
    setenv("TSDB_RPC_TLS_CA",   g_ca_cert, 1);
    setenv("TSDB_RPC_TLS_CERT", g_cli_cert, 1);
    setenv("TSDB_RPC_TLS_KEY",  g_cli_key, 1);

    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    if (!conn) { fprintf(stderr, "tls connect failed\n"); return 3; }

    uint8_t pl[16] = {0};
    uint64_t nid = 7, ver = 1;
    memcpy(pl, &nid, 8); memcpy(pl + 8, &ver, 8);
    int rc = tsdb_rpc_call(conn, TSDB_RPC_HEARTBEAT, pl, sizeof(pl));
    tsdb_rpc_conn_close(conn);
    return (rc == TSDB_OK) ? 0 : 4;
}

/* Scenario 2: raw plaintext TCP client → must NOT get a valid RPC reply. */
static int cli_plaintext(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 2;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    struct timeval tv = { .tv_sec = 2 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) { close(fd); return 2; }

    /* Well-formed plaintext HEARTBEAT.  The TLS server reads it as a bogus
     * ClientHello, the handshake fails, the fd is dropped. */
    uint8_t frame[64], pl[16] = {0};
    int n = tsdb_rpc_encode(frame, sizeof(frame), TSDB_RPC_HEARTBEAT, 1, pl, sizeof(pl));
    if (n > 0) { ssize_t w = write(fd, frame, (size_t)n); (void)w; }

    uint8_t buf[64];
    ssize_t r = read(fd, buf, sizeof(buf));
    int got_valid_rpc = 0;
    if (r >= (ssize_t)4) {
        uint32_t magic;
        memcpy(&magic, buf, 4);
        if (magic == TSDB_RPC_MAGIC) got_valid_rpc = 1;   /* server spoke RPC → leak */
    }
    close(fd);
    return got_valid_rpc ? 5 : 0;     /* 0 = correctly rejected */
}

/* Scenario 3: TLS client with CA but NO client cert → mutual server rejects.
 *
 * Under TLS 1.3 the client's handshake can report success before the server
 * processes the (absent) client Certificate and sends its alert, so the
 * connect may return a handle; the rejection then surfaces on first use.  The
 * meaningful invariant is therefore "a no-client-cert peer cannot complete an
 * RPC" — connect fails OR the first call fails. */
static int cli_no_cert(int port) {
    setenv("TSDB_RPC_TLS", "1", 1);
    setenv("TSDB_RPC_TLS_CA", g_ca_cert, 1);
    unsetenv("TSDB_RPC_TLS_CERT");
    unsetenv("TSDB_RPC_TLS_KEY");

    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    if (!conn) return 0;              /* rejected at handshake — correct */

    uint8_t pl[16] = {0};
    int rc = tsdb_rpc_call(conn, TSDB_RPC_HEARTBEAT, pl, sizeof(pl));
    tsdb_rpc_conn_close(conn);
    return (rc == TSDB_OK) ? 6 : 0;  /* 0 = call correctly rejected */
}

/* ── Drive one scenario: spawn server child, run client in its own child ──── */

static void run_scenario(const char *name, int (*client)(int port)) {
    int port = -1;
    pid_t srv = spawn_tls_server(&port);
    if (srv < 0) { FAIL(name); return; }

    fflush(stdout); fflush(stderr);
    pid_t cli = fork();
    if (cli < 0) { kill_server(srv); FAIL(name); return; }
    if (cli == 0) {
        signal(SIGPIPE, SIG_IGN);
        _exit(client(port));
    }

    int st = 0;
    waitpid(cli, &st, 0);
    kill_server(srv);

    if (WIFEXITED(st) && WEXITSTATUS(st) == 0) PASS(name);
    else {
        char m[160];
        snprintf(m, sizeof(m), "%s (client exit=%d)", name,
                 WIFEXITED(st) ? WEXITSTATUS(st) : -1);
        FAIL(m);
    }
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    printf("=== test_rpc_tls ===\n");

    if (!tsdb_tls_available()) {
        printf("NOTE: TLS backend not compiled in — all RPC-TLS tests skipped.\n");
        SKIP("rpc TLS round-trip (no TLS backend)");
        SKIP("rpc plaintext rejected (no TLS backend)");
        SKIP("rpc no-client-cert rejected (no TLS backend)");
        printf("\n=== %d tests: %d pass, %d fail, %d skip ===\n",
               g_tests, g_pass, g_fail, g_skip);
        return 0;
    }

    if (setup_certs() != 0) {
        fprintf(stderr, "WARN: cert generation failed — RPC-TLS tests skipped\n");
        SKIP("rpc TLS round-trip (no certs)");
        SKIP("rpc plaintext rejected (no certs)");
        SKIP("rpc no-client-cert rejected (no certs)");
        cleanup_certs();
        printf("\n=== %d tests: %d pass, %d fail, %d skip ===\n",
               g_tests, g_pass, g_fail, g_skip);
        return 0;
    }

    run_scenario("rpc TLS round-trip (HEARTBEAT ACK)",       cli_roundtrip);
    run_scenario("plaintext client rejected by TLS server",  cli_plaintext);
    run_scenario("no-client-cert rejected by mutual server", cli_no_cert);

    cleanup_certs();

    printf("\n=== %d tests: %d pass, %d fail, %d skip ===\n",
           g_tests, g_pass, g_fail, g_skip);
    return g_fail ? 1 : 0;
}
