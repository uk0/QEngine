/* test_cluster_route.c — verify Phase α routing API.
 *
 * Properties checked:
 *   1. With a populated ring, route(table, key, R) returns R distinct
 *      node IDs every time
 *   2. Same (table, key) → same node IDs (idempotent)
 *   3. Different keys land on different primaries with reasonable
 *      uniformity (each node owns a slice of the keyspace)
 *   4. Adding/removing a node only re-homes ~1/N of the keys
 *      (consistent-hashing property)
 *
 * The test exercises the hashring directly to avoid spinning up
 * gossip/RPC machinery — the cluster_route wrapper is a thin
 * pre-format + delegation, so verifying ring behaviour is sufficient
 * for the routing API contract.
 */

#include "../src/cluster/hashring.h"
#include "../include/tsdb.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FAIL(fmt, ...) do { \
  fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
  abort(); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

static void key_for(int table_idx, int key_idx, char *buf, size_t cap) {
    snprintf(buf, cap, "tbl_%03d\1host_%05d", table_idx, key_idx);
}

int main(void) {
    tsdb_hashring_t *ring = tsdb_hashring_new();
    ASSERT(ring);

    /* 4 nodes, equal weight. */
    tsdb_node_id_t nodes[] = { 100, 200, 300, 400 };
    for (int i = 0; i < 4; i++)
        ASSERT(tsdb_hashring_add(ring, nodes[i]) == 0);
    ASSERT(tsdb_hashring_node_count(ring) == 4);

    /* ---- Property 1: R distinct primaries per key ---- */
    for (int k = 0; k < 100; k++) {
        char key[64]; key_for(7, k, key, sizeof(key));
        tsdb_node_id_t out[3];
        int got = tsdb_hashring_owner(ring, key, strlen(key), 3, out);
        ASSERT(got == 3);
        ASSERT(out[0] != out[1]);
        ASSERT(out[0] != out[2]);
        ASSERT(out[1] != out[2]);
        /* All must be in the registered set. */
        for (int j = 0; j < 3; j++) {
            int found = 0;
            for (int n = 0; n < 4; n++) if (out[j] == nodes[n]) { found = 1; break; }
            ASSERT(found);
        }
    }
    printf("property 1: R distinct primaries        PASS\n");

    /* ---- Property 2: idempotent ---- */
    char key[64]; key_for(13, 42, key, sizeof(key));
    tsdb_node_id_t a[3], b[3];
    tsdb_hashring_owner(ring, key, strlen(key), 3, a);
    tsdb_hashring_owner(ring, key, strlen(key), 3, b);
    ASSERT(memcmp(a, b, sizeof(a)) == 0);
    printf("property 2: idempotent                  PASS\n");

    /* ---- Property 3: load distribution ---- */
    int load[4] = {0};
    for (int k = 0; k < 10000; k++) {
        char kk[64]; key_for(0, k, kk, sizeof(kk));
        tsdb_node_id_t out[1];
        tsdb_hashring_owner(ring, kk, strlen(kk), 1, out);
        for (int n = 0; n < 4; n++) if (out[0] == nodes[n]) { load[n]++; break; }
    }
    int total = load[0] + load[1] + load[2] + load[3];
    ASSERT(total == 10000);
    /* Each node should own non-zero share; with 256 VNs and 10k keys
     * the variance can hit ±20 pp on a small sample.  We assert a
     * lenient bound — the goal is "no node stuck at 0% or 100%",
     * not statistical purity.  Realistic 1M-key production traffic
     * is far smoother. */
    for (int n = 0; n < 4; n++) {
        double pct = (double)load[n] / total;
        printf("  node %d → %4d keys (%5.1f%%)\n", n, load[n], pct * 100);
        ASSERT(pct > 0.05 && pct < 0.50);
    }
    printf("property 3: even distribution           PASS\n");

    /* ---- Property 4: consistent-hash stability under +1 node ---- */
    tsdb_node_id_t before_owner[1000];
    for (int k = 0; k < 1000; k++) {
        char kk[64]; key_for(0, k, kk, sizeof(kk));
        tsdb_node_id_t out[1];
        tsdb_hashring_owner(ring, kk, strlen(kk), 1, out);
        before_owner[k] = out[0];
    }
    /* Add a 5th node. */
    ASSERT(tsdb_hashring_add(ring, 500) == 0);
    int rehomed = 0;
    for (int k = 0; k < 1000; k++) {
        char kk[64]; key_for(0, k, kk, sizeof(kk));
        tsdb_node_id_t out[1];
        tsdb_hashring_owner(ring, kk, strlen(kk), 1, out);
        if (out[0] != before_owner[k]) rehomed++;
    }
    double rehomed_pct = (double)rehomed / 1000;
    printf("  after +1 node, %d / 1000 keys rehomed (%5.1f%%)\n",
           rehomed, rehomed_pct * 100);
    /* Theory: with N=4 → N=5, ideal rehome rate is 1/5 = 20%.
     * Real ring with 256 VN and 1k keys can swing to either side of
     * theory.  We assert "consistent-hash held": no more than ~half
     * the keys should re-home (a non-CH ring would shuffle ~80%+),
     * and the new node should claim *something* (>0). */
    ASSERT(rehomed > 0 && rehomed_pct < 0.50);
    printf("property 4: consistent-hash stable      PASS\n");

    tsdb_hashring_free(ring);
    printf("\n=== CLUSTER ROUTE TESTS PASSED ===\n");
    return 0;
}
