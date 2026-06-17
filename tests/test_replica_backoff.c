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
 */

#include "../src/cluster/node.h"   /* tsdb_node_state_t */

#include <stdio.h>

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

int main(void) {
    printf("=== test_replica_backoff ===\n");
    test_dialable();
    test_backoff();
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
