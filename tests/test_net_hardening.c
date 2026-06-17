/* test_net_hardening.c — connection-stability + FQDN hardening for the
 * inter-node RPC dialer (src/cluster/rpc.c tsdb_rpc_connect).
 *
 * Test A — keepalive: a connected dialer fd must carry SO_KEEPALIVE=1 (so a
 *   silently-dead peer is reaped instead of wedging an RPC slot).  On Linux the
 *   idle probe threshold (TCP_KEEPIDLE) must be the configured 30 s default.
 *
 * Test B — dual-stack resolve: dialing the FQDN "localhost" must succeed.  The
 *   old AF_INET + first-result-only path could pick a v6 "localhost" address
 *   and fail; AF_UNSPEC + iterate-all tries every result until one connects.
 *
 * tsdb_rpc_conn is opaque; its first member is `int fd` (see struct
 * tsdb_rpc_conn in rpc.c), so the connected fd is *(int *)conn.
 */

#include "../src/cluster/rpc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

/* Minimal accept loop: accept one connection, hold it briefly, then close. */
static void *accept_one(void *arg) {
    int listen_fd = *(int *)arg;
    struct sockaddr_in cli;
    socklen_t clen = sizeof(cli);
    int cfd = accept(listen_fd, (struct sockaddr *)&cli, &clen);
    if (cfd >= 0) {
        usleep(50 * 1000);   /* let the dialer finish setsockopt + assert */
        close(cfd);
    }
    return NULL;
}

/* Bind 127.0.0.1:0, listen, return the fd; *out_port gets the chosen port. */
static int listen_ephemeral(int *out_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) { close(fd); return -1; }
    if (listen(fd, 4) != 0) { close(fd); return -1; }
    socklen_t sl = sizeof(sa);
    if (getsockname(fd, (struct sockaddr *)&sa, &sl) != 0) { close(fd); return -1; }
    *out_port = ntohs(sa.sin_port);
    return fd;
}

/* Test A: keepalive on the dialer fd (+ TCP_KEEPIDLE==30 on Linux). */
static void test_keepalive(void) {
    int port = 0;
    int lfd = listen_ephemeral(&port);
    CHECK(lfd >= 0, "ephemeral listener on 127.0.0.1");
    if (lfd < 0) return;

    pthread_t th;
    pthread_create(&th, NULL, accept_one, &lfd);

    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    CHECK(conn != NULL, "tsdb_rpc_connect(127.0.0.1) succeeds");

    if (conn) {
        int fd = *(int *)conn;   /* first member of struct tsdb_rpc_conn */
        int ka = 0; socklen_t kl = sizeof(ka);
        int rc = getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &ka, &kl);
        /* getsockopt returns the option's stored value, which is 1 on Linux but
         * the SO_KEEPALIVE flag bit (nonzero, e.g. 8) on macOS/BSD — assert
         * "enabled", not a literal 1. */
        CHECK(rc == 0 && ka != 0, "dialer fd has SO_KEEPALIVE enabled");

#ifdef __linux__
        int idle = 0; socklen_t il = sizeof(idle);
        int irc = getsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, &il);
        CHECK(irc == 0 && idle == 30, "dialer fd TCP_KEEPIDLE == 30 (Linux)");
#endif
        tsdb_rpc_conn_close(conn);
    }

    pthread_join(th, NULL);
    close(lfd);
}

/* Test B: dual-stack iterate-all resolves and connects via "localhost". */
static void test_dualstack_localhost(void) {
    int port = 0;
    int lfd = listen_ephemeral(&port);
    CHECK(lfd >= 0, "ephemeral listener for localhost test");
    if (lfd < 0) return;

    pthread_t th;
    pthread_create(&th, NULL, accept_one, &lfd);

    char addr[64];
    snprintf(addr, sizeof(addr), "localhost:%d", port);
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
    CHECK(conn != NULL, "tsdb_rpc_connect(localhost) succeeds (dual-stack)");
    if (conn) tsdb_rpc_conn_close(conn);

    pthread_join(th, NULL);
    close(lfd);
}

int main(void) {
    printf("=== test_net_hardening ===\n");

    printf("\n[A] TCP keepalive on the RPC dialer fd\n");
    test_keepalive();

    printf("\n[B] dual-stack getaddrinfo: connect via localhost\n");
    test_dualstack_localhost();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
