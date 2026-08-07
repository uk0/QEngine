/* test_replica_backoff.c — unit tests for replica.c connection-stability
 * helpers: gossip-DEAD peer skip + exponential connect/RPC backoff.
 *
 * The two helpers (replica_peer_dialable / replica_backoff_ns) are static
 * inside src/cluster/replica.c.  That TU exposes non-static *_test shims
 * under -DTSDB_TEST; this test links a -DTSDB_TEST build of replica.o (see
 * the dedicated Makefile rule) and calls them through these externs.
 *
 * Asserts:
 *   1. replica_peer_dialable: DEAD -> 0; ALIVE / SUSPECT / JOINING -> 1.
 *   2. replica_backoff_ns: starts at the base (20 ms), is monotonic
 *      non-decreasing across attempts, and saturates at the cap (500 ms)
 *      for large attempts.
 *   3. tsdb_replica_mgr_get_conn evicting a retired (desynced) conn leaks
 *      the object instead of closing it — the pool hands out raw pointers
 *      with no refcount, so a close there frees memory (and the conn->lock
 *      mutex) other callers may still be using.  Only sharp under ASan.
 */

#include "../src/cluster/node.h"   /* tsdb_node_state_t */
#include "../src/cluster/replica.h"
#include "../src/cluster/rpc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* Shims defined in src/cluster/replica.c under -DTSDB_TEST. */
extern int  replica_peer_dialable_test(tsdb_node_state_t state);
extern long replica_backoff_ns_test(int attempt);

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

#define BASE_NS  (20L * 1000000L)   /* 20 ms */
#define CAP_NS   (500L * 1000000L)  /* 500 ms */

static void test_dialable(void) {
    CHECK(replica_peer_dialable_test(TSDB_NODE_DEAD) == 0,
          "DEAD peer is NOT dialable");
    CHECK(replica_peer_dialable_test(TSDB_NODE_ALIVE) == 1,
          "ALIVE peer is dialable");
    CHECK(replica_peer_dialable_test(TSDB_NODE_SUSPECT) == 1,
          "SUSPECT peer is still dialable (only hard-DEAD is skipped)");
    CHECK(replica_peer_dialable_test(TSDB_NODE_JOINING) == 1,
          "JOINING peer is dialable");
}

static void test_backoff(void) {
    /* Starts at the base. */
    CHECK(replica_backoff_ns_test(0) == BASE_NS,
          "backoff attempt 0 == base (20 ms)");

    /* Monotonic non-decreasing across a wide attempt range. */
    int mono = 1;
    long prev = replica_backoff_ns_test(0);
    for (int a = 1; a <= 12; a++) {
        long cur = replica_backoff_ns_test(a);
        if (cur < prev) { mono = 0; break; }
        prev = cur;
    }
    CHECK(mono, "backoff is monotonic non-decreasing across attempts 0..12");

    /* Strictly increasing while below the cap (20 -> 40 -> 80 ms). */
    CHECK(replica_backoff_ns_test(0) < replica_backoff_ns_test(1) &&
          replica_backoff_ns_test(1) < replica_backoff_ns_test(2),
          "backoff strictly grows below the cap (attempt 0 < 1 < 2)");

    /* Saturates at the cap for large attempts and never exceeds it. */
    CHECK(replica_backoff_ns_test(100) == CAP_NS,
          "backoff saturates at the cap (500 ms) for a large attempt");
    CHECK(replica_backoff_ns_test(12) == CAP_NS,
          "backoff is already capped by attempt 12");

    /* Negative attempt is clamped to the base, not garbage. */
    CHECK(replica_backoff_ns_test(-1) == BASE_NS,
          "backoff clamps a negative attempt to the base");
}

/* get_conn hands out raw conn pointers with no refcount, so evicting a
 * retired (desynced) conn from its pool slot must LEAK the object, never
 * tsdb_rpc_conn_close() it: an earlier caller may still be mid-call on the
 * very same pointer (fanout workers queued on conn->lock, the anti-entropy
 * probe).  Reproduce the peer-stop shape single-threaded and deterministic:
 * dial an in-test listener that slams the accepted socket shut, fail one
 * call so read_full marks the conn desynced, then call get_conn again for
 * the same node id and read the retired conn afterwards.  Before the fix
 * the second get_conn tsdb_rpc_conn_close()d it and the read below was a
 * heap-use-after-free (ASan aborts, naming get_conn's close as the free
 * site); the assertion is only sharp under scripts/asan-build.sh — without
 * ASan the freed byte may still happen to read back as 1. */
static void test_retired_conn_leaked_not_closed(void) {
    /* The client writes onto a socket whose peer already closed; the kernel
     * answers RST and the next write raises SIGPIPE, which by default kills
     * the test.  Production ignores it too (tsdb_node_main.c). */
    signal(SIGPIPE, SIG_IGN);

    /* One conn per peer so the second get_conn lands on the same slot the
     * retired conn occupies (rr % 1 == 0).  Read at mgr_new time. */
    setenv("TSDB_REPLICA_CONNS_PER_PEER", "1", 1);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;   /* ephemeral */
    socklen_t sl = sizeof sa;
    int lok = lfd >= 0 &&
              bind(lfd, (struct sockaddr *)&sa, sizeof sa) == 0 &&
              listen(lfd, 4) == 0 &&
              getsockname(lfd, (struct sockaddr *)&sa, &sl) == 0;
    CHECK(lok, "in-test listener bound on an ephemeral loopback port");
    if (!lok) return;

    tsdb_node_manager_t *nm =
        tsdb_node_manager_new(1, "127.0.0.1:1", "127.0.0.1:1",
                              TSDB_ROLE_MASTER);
    tsdb_node_info_t peer;
    memset(&peer, 0, sizeof peer);
    peer.id      = 2;
    peer.state   = TSDB_NODE_ALIVE;
    peer.version = 1;
    snprintf(peer.addr, sizeof peer.addr, "127.0.0.1:%u",
             (unsigned)ntohs(sa.sin_port));
    tsdb_node_manager_upsert(nm, &peer);

    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(nm);

    tsdb_rpc_conn_t *c1 = tsdb_replica_mgr_get_conn(rmgr, 2);
    CHECK(c1 != NULL, "first get_conn dials the in-test listener");

    /* Server side: take the queued connection and slam it shut so the
     * client's next call fails in read_full (EOF/RST). */
    int sfd = accept(lfd, NULL, NULL);
    if (sfd >= 0) close(sfd);

    if (c1) {
        uint8_t pl[4] = {0};
        int rc = tsdb_rpc_call(c1, TSDB_RPC_HEARTBEAT, pl, sizeof pl);
        CHECK(rc != 0, "call on the slammed conn fails");
        CHECK(tsdb_rpc_conn_is_desynced(c1) == 1,
              "failed call retired (desynced) c1");

        /* Second get_conn for the same node sees the retired conn in its
         * slot and must evict it leak-and-clear. */
        (void)tsdb_replica_mgr_get_conn(rmgr, 2);

        /* THE assertion: c1 must still be intact — reading it must not be
         * a use-after-free.  ASan is the real judge here. */
        CHECK(tsdb_rpc_conn_is_desynced(c1) == 1,
              "retired conn evicted from the pool is leaked intact, not freed");
    }

    close(lfd);
    tsdb_replica_mgr_free(rmgr);
    tsdb_node_manager_free(nm);
}

int main(void) {
    printf("=== test_replica_backoff ===\n");
    test_dialable();
    test_backoff();
    test_retired_conn_leaked_not_closed();
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
