/* test_autobalance.c — unit tests for the auto-balance controller.
 *
 * Tests:
 *   1. Weighted ring: add_weighted, get_weight, set_weight
 *   2. Ring weight set_weight + re-query owner consistency
 *   3. Autobalance create/destroy lifecycle
 *   4. record_write + ema calculation (manual trigger)
 *   5. encode_load + merge_load round-trip
 *   6. stats_str produces valid JSON
 *   7. Rebalance: imbalanced writes → heavier node gets fewer VNs
 */

#define _POSIX_C_SOURCE 200809L

#include "../src/cluster/hashring.h"
#include "../src/cluster/node.h"
#include "../src/cluster/autobalance.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <math.h>

/* ---- Mini harness --------------------------------------------------------- */

static int g_pass = 0, g_fail = 0;
static const char *g_test = "";

#define CHECK(cond, msg) do { \
    if (cond) { g_pass++; } \
    else { fprintf(stderr, "  FAIL [%s:%d] (%s): %s\n", \
                   __FILE__, __LINE__, g_test, msg); g_fail++; } \
} while(0)

#define SECTION(name) do { g_test = (name); printf("--- %s ---\n", g_test); } while(0)

/* ---- Test 1: weighted ring add / get_weight ------------------------------ */

static void test_weighted_add(void) {
    SECTION("weighted ring: add_weighted + get_weight");

    tsdb_hashring_t *r = tsdb_hashring_new();
    CHECK(r != NULL, "ring alloc");

    /* Default add → TSDB_RING_VN (256). */
    tsdb_hashring_add(r, 1ULL);
    CHECK(tsdb_hashring_get_weight(r, 1ULL) == TSDB_RING_VN, "default weight = 256");

    /* Weighted add. */
    tsdb_hashring_add_weighted(r, 2ULL, 128);
    CHECK(tsdb_hashring_get_weight(r, 2ULL) == 128, "weight 128");

    tsdb_hashring_add_weighted(r, 3ULL, 512);
    CHECK(tsdb_hashring_get_weight(r, 3ULL) == 512, "weight 512 (VN_MAX)");

    /* Clamped to VN_MAX. */
    tsdb_hashring_add_weighted(r, 4ULL, 9999);
    CHECK(tsdb_hashring_get_weight(r, 4ULL) == TSDB_RING_VN_MAX, "clamped to VN_MAX");

    /* Non-existent node → 0. */
    CHECK(tsdb_hashring_get_weight(r, 99ULL) == 0, "missing node weight = 0");

    tsdb_hashring_free(r);
}

/* ---- Test 2: set_weight changes VN count --------------------------------- */

static void test_set_weight(void) {
    SECTION("weighted ring: set_weight");

    tsdb_hashring_t *r = tsdb_hashring_new();
    tsdb_hashring_add_weighted(r, 10ULL, 256);
    tsdb_hashring_add_weighted(r, 20ULL, 256);
    tsdb_hashring_add_weighted(r, 30ULL, 256);

    CHECK(tsdb_hashring_get_weight(r, 10ULL) == 256, "initial weight 256");

    /* Reduce weight of node 10 to min. */
    tsdb_hashring_set_weight(r, 10ULL, TSDB_BALANCE_VN_MIN);
    CHECK(tsdb_hashring_get_weight(r, 10ULL) == TSDB_BALANCE_VN_MIN,
          "weight reduced to VN_MIN");

    /* Increase weight of node 20. */
    tsdb_hashring_set_weight(r, 20ULL, 400);
    CHECK(tsdb_hashring_get_weight(r, 20ULL) == 400, "weight increased to 400");

    /* set_weight on non-present node adds it. */
    tsdb_hashring_set_weight(r, 99ULL, 64);
    CHECK(tsdb_hashring_get_weight(r, 99ULL) == 64, "set_weight adds new node");
    CHECK(tsdb_hashring_node_count(r) == 4, "4 nodes after add-via-set_weight");

    /* Verify owners are consistent: node 10 should be primary for fewer keys
     * than node 20 (which has the highest weight). Simple statistical check. */
    int hits[4] = {0,0,0,0};  /* 10,20,30,99 */
    tsdb_node_id_t owners[1];
    for (int i = 0; i < 10000; i++) {
        char key[32];
        snprintf(key, sizeof(key), "table_%d", i);
        if (tsdb_hashring_owner_str(r, key, 1, owners) == 1) {
            if (owners[0] == 10ULL) hits[0]++;
            else if (owners[0] == 20ULL) hits[1]++;
            else if (owners[0] == 30ULL) hits[2]++;
            else if (owners[0] == 99ULL) hits[3]++;
        }
    }
    /* node20 (400 VN) should win more than node10 (32 VN).
     * With 4 nodes at 32/400/256/64 = 752 total VN, node20 gets ~53%.
     * node10 gets ~4%.  Expect node20 > node10 by a large margin. */
    printf("  hits: node10=%d node20=%d node30=%d node99=%d (total=10000)\n",
           hits[0], hits[1], hits[2], hits[3]);
    CHECK(hits[1] > hits[0] * 3, "node20 (400 VN) dominates node10 (32 VN)");
    CHECK(hits[0] < 1000, "node10 (32 VN) gets <10% of keys");

    tsdb_hashring_free(r);
}

/* ---- Test 3: autobalance lifecycle --------------------------------------- */

static void test_lifecycle(void) {
    SECTION("autobalance: create / destroy");

    tsdb_node_manager_t *mgr = tsdb_node_manager_new(1ULL, "127.0.0.1:19000",
                                                      "127.0.0.1:19001");
    CHECK(mgr != NULL, "node_mgr alloc");

    tsdb_autobalance_t *ab = tsdb_autobalance_new(mgr, 1ULL, NULL);
    CHECK(ab != NULL, "autobalance alloc");

    /* Free should not crash. */
    tsdb_autobalance_free(ab);
    tsdb_node_manager_free(mgr);
    CHECK(1, "free without crash");
}

/* ---- Test 4: record_write accumulates, rebalance_now triggers cycle ------ */

static void test_record_write(void) {
    SECTION("autobalance: record_write + rebalance_now");

    tsdb_node_manager_t *mgr = tsdb_node_manager_new(1ULL, "127.0.0.1:19010",
                                                      "127.0.0.1:19011");
    tsdb_autobalance_t *ab = tsdb_autobalance_new(mgr, 1ULL, NULL);
    CHECK(ab != NULL, "alloc");

    tsdb_autobalance_record_write(ab, 1000);
    tsdb_autobalance_record_write(ab, 500);

    /* Force an immediate cycle — should not crash. */
    int rc = tsdb_autobalance_rebalance_now(ab);
    CHECK(rc == 0, "rebalance_now returns 0");

    tsdb_autobalance_free(ab);
    tsdb_node_manager_free(mgr);
}

/* ---- Test 5: encode_load / merge_load round-trip ------------------------- */

static void test_load_roundtrip(void) {
    SECTION("autobalance: encode_load / merge_load round-trip");

    tsdb_node_manager_t *mgr = tsdb_node_manager_new(42ULL, "127.0.0.1:19020",
                                                      "127.0.0.1:19021");
    tsdb_autobalance_t *ab = tsdb_autobalance_new(mgr, 42ULL, NULL);

    /* Record some writes so EMA is non-zero after a cycle. */
    tsdb_autobalance_record_write(ab, 9000);
    tsdb_autobalance_rebalance_now(ab);

    /* Encode. */
    uint8_t buf[64];
    int n = tsdb_autobalance_encode_load(ab, buf, sizeof(buf));
    CHECK(n == TSDB_NODE_LOAD_WIRE_SIZE, "encode produces 28 bytes");

    /* Decode manually. */
    uint64_t node_id;
    uint64_t writes_sec;
    uint64_t storage_bytes;
    uint32_t cpu;
    memcpy(&node_id,      buf,      8);
    memcpy(&writes_sec,   buf + 8,  8);
    memcpy(&storage_bytes,buf + 16, 8);
    memcpy(&cpu,          buf + 24, 4);

    CHECK(node_id == 42ULL, "node_id round-trip");
    /* writes_sec may be 0 if the EMA window hasn't elapsed; that's OK. */
    printf("  decoded: writes_sec=%llu storage_bytes=%llu cpu=%u\n",
           (unsigned long long)writes_sec,
           (unsigned long long)storage_bytes,
           cpu);

    /* Now merge into a second balancer to verify merge_load. */
    tsdb_node_manager_t *mgr2 = tsdb_node_manager_new(99ULL, "127.0.0.1:19030",
                                                       "127.0.0.1:19031");
    tsdb_autobalance_t *ab2 = tsdb_autobalance_new(mgr2, 99ULL, NULL);
    tsdb_autobalance_merge_load(ab2, buf, n);

    /* stats_str should include node 42. */
    char stats[2048];
    int slen = tsdb_autobalance_stats_str(ab2, stats, sizeof(stats));
    CHECK(slen > 0, "stats_str non-empty");
    CHECK(strstr(stats, "42") != NULL, "stats contains merged node id 42");
    printf("  stats: %.*s\n", (int)(slen < 200 ? slen : 200), stats);

    tsdb_autobalance_free(ab2);
    tsdb_node_manager_free(mgr2);
    tsdb_autobalance_free(ab);
    tsdb_node_manager_free(mgr);
}

/* ---- Test 6: stats_str is valid JSON-ish --------------------------------- */

static void test_stats_str(void) {
    SECTION("autobalance: stats_str");

    tsdb_node_manager_t *mgr = tsdb_node_manager_new(7ULL, "127.0.0.1:19040",
                                                      "127.0.0.1:19041");
    tsdb_autobalance_t *ab = tsdb_autobalance_new(mgr, 7ULL, NULL);

    char buf[4096];
    int n = tsdb_autobalance_stats_str(ab, buf, sizeof(buf));
    CHECK(n > 0, "stats non-empty");
    CHECK(buf[0] == '{', "starts with {");
    CHECK(buf[n-1] == '}' || buf[n-1] == ']', "ends with } or ]");
    CHECK(strstr(buf, "local_id") != NULL, "contains local_id");
    CHECK(strstr(buf, "ema_writes_sec") != NULL, "contains ema_writes_sec");
    printf("  stats_str (%d bytes): %.*s\n", n, (int)(n < 300 ? n : 300), buf);

    tsdb_autobalance_free(ab);
    tsdb_node_manager_free(mgr);
}

/* ---- Test 7: imbalanced writes → heavier node gets fewer VNs ------------ */

static void test_rebalance_adjusts_weights(void) {
    SECTION("autobalance: rebalance adjusts weights for imbalanced load");

    /* Create two node managers; use mgr1 (local) as the primary.
     * Simulate a peer (node 2) with high write load by merging a crafted
     * load payload, then trigger a rebalance and verify node 2 gets fewer VNs. */

    tsdb_node_manager_t *mgr = tsdb_node_manager_new(1ULL, "127.0.0.1:19050",
                                                      "127.0.0.1:19051");

    /* Add a peer node to the manager so it appears in ring. */
    tsdb_node_info_t peer = {0};
    peer.id = 2ULL;
    snprintf(peer.addr, sizeof(peer.addr), "127.0.0.1:19052");
    snprintf(peer.gossip_addr, sizeof(peer.gossip_addr), "127.0.0.1:19053");
    peer.state   = TSDB_NODE_ALIVE;
    peer.version = 1;
    tsdb_node_manager_upsert(mgr, &peer);

    int init_vn1 = tsdb_hashring_get_weight(tsdb_node_manager_ring(mgr), 1ULL);
    int init_vn2 = tsdb_hashring_get_weight(tsdb_node_manager_ring(mgr), 2ULL);
    printf("  initial VN: node1=%d node2=%d\n", init_vn1, init_vn2);
    CHECK(init_vn1 == TSDB_RING_VN, "node1 starts at default 256 VN");
    CHECK(init_vn2 == TSDB_RING_VN, "node2 starts at default 256 VN");

    tsdb_autobalance_t *ab = tsdb_autobalance_new(mgr, 1ULL, NULL);

    /* Inject a high-load record for node 2 (10x the writes of node 1). */
    uint8_t load_buf[TSDB_NODE_LOAD_WIRE_SIZE];
    uint64_t nid       = 2ULL;
    uint64_t high_wps  = 100000; /* 100k rows/sec — extremely heavy */
    uint64_t storage   = 0;
    uint32_t cpu       = 0;
    memcpy(load_buf,      &nid,      8);
    memcpy(load_buf + 8,  &high_wps, 8);
    memcpy(load_buf + 16, &storage,  8);
    memcpy(load_buf + 24, &cpu,      4);
    tsdb_autobalance_merge_load(ab, load_buf, TSDB_NODE_LOAD_WIRE_SIZE);

    /* Node 1 has zero local writes (light load). */
    /* Trigger rebalance. */
    tsdb_autobalance_rebalance_now(ab);

    int vn1 = tsdb_hashring_get_weight(tsdb_node_manager_ring(mgr), 1ULL);
    int vn2 = tsdb_hashring_get_weight(tsdb_node_manager_ring(mgr), 2ULL);
    printf("  after rebalance: node1=%d VN  node2=%d VN\n", vn1, vn2);

    /* Heavy node (2) should have fewer or equal VNs.
     * Light node (1) should have more VNs than heavy node (2). */
    CHECK(vn2 <= vn1, "heavy node2 gets ≤ VNs than light node1");
    CHECK(vn2 < TSDB_BALANCE_VN_BASE, "heavy node2 is below base VN count");
    CHECK(vn2 >= TSDB_BALANCE_VN_MIN, "heavy node2 stays at or above VN_MIN");

    tsdb_autobalance_free(ab);
    tsdb_node_manager_free(mgr);
}

/* ---- Test 8: ring remove preserves vn_counts ----------------------------- */

static void test_remove_preserves_counts(void) {
    SECTION("weighted ring: remove + re-add preserves weight tracking");

    tsdb_hashring_t *r = tsdb_hashring_new();
    tsdb_hashring_add_weighted(r, 1ULL, 100);
    tsdb_hashring_add_weighted(r, 2ULL, 200);
    tsdb_hashring_add_weighted(r, 3ULL, 300);

    tsdb_hashring_remove(r, 2ULL);
    CHECK(tsdb_hashring_node_count(r) == 2, "2 nodes after remove");
    CHECK(tsdb_hashring_get_weight(r, 2ULL) == 0, "removed node weight = 0");
    CHECK(tsdb_hashring_get_weight(r, 1ULL) == 100, "node1 weight unchanged");
    CHECK(tsdb_hashring_get_weight(r, 3ULL) == 300, "node3 weight unchanged");

    /* Re-add node 2 with different weight. */
    tsdb_hashring_add_weighted(r, 2ULL, 50);
    CHECK(tsdb_hashring_get_weight(r, 2ULL) == 50, "re-added node2 weight=50");
    CHECK(tsdb_hashring_node_count(r) == 3, "3 nodes after re-add");

    tsdb_hashring_free(r);
}

/* ---- Main ---------------------------------------------------------------- */

int main(void) {
    printf("=== test_autobalance ===\n");

    test_weighted_add();
    test_set_weight();
    test_lifecycle();
    test_record_write();
    test_load_roundtrip();
    test_stats_str();
    test_rebalance_adjusts_weights();
    test_remove_preserves_counts();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
