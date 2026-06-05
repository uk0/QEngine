/* test_catalog_id.c — node-prefixed OID allocator (Track B P1).
 *
 * Pins: monotonic unique ids within a node, correct node prefix, no reuse
 * across a reopen, zero collisions across distinct node_ids, and observe()
 * advancing the high-water.
 */
#include "../include/catalog_id.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_oid";

int main(void) {
    printf("=== test_catalog_id ===\n");
    char cmd[256], dir[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", TMP, TMP); (void)system(cmd);
    snprintf(dir, sizeof(dir), "%s", TMP);

    /* 1M ids from node 7: strictly monotonic seq, prefix==7, all >= FIRST_USR. */
    tsdb_oid_alloc_t a;
    ASSERT(tsdb_oid_alloc_open(&a, dir, 7) == TSDB_OK);
    const int N = 1000000;
    uint64_t last = 0;
    for (int i = 0; i < N; i++) {
        tsdb_oid_t o = tsdb_oid_next(&a);
        ASSERT(o != TSDB_OID_NONE);
        ASSERT(tsdb_oid_node(o) == 7);
        uint64_t s = tsdb_oid_seq(o);
        ASSERT(s >= TSDB_OID_FIRST_USR);
        ASSERT(s > last || (i == 0 && s >= TSDB_OID_FIRST_USR)); /* strictly increasing */
        if (i > 0) ASSERT(s == last + 1);                        /* contiguous within a run */
        last = s;
    }
    uint64_t max_before = last;
    printf("  1M ids node=7 monotonic, top seq=%llu\n", (unsigned long long)max_before);
    tsdb_oid_alloc_close(&a);

    /* Reopen: must resume PAST the persisted reservation — no id reused. */
    ASSERT(tsdb_oid_alloc_open(&a, dir, 7) == TSDB_OK);
    tsdb_oid_t o2 = tsdb_oid_next(&a);
    ASSERT(tsdb_oid_node(o2) == 7);
    ASSERT(tsdb_oid_seq(o2) > max_before);   /* strictly past the last block, no reuse */
    printf("  after reopen, next seq=%llu > %llu (no reuse)\n",
           (unsigned long long)tsdb_oid_seq(o2), (unsigned long long)max_before);
    tsdb_oid_alloc_close(&a);

    /* Four distinct node_ids never collide: different high 16 bits. */
    tsdb_oid_t seen[4][16];
    for (int n = 0; n < 4; n++) {
        char d[256]; snprintf(d, sizeof(d), "%s/n%d", TMP, n);
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", d); (void)system(cmd);
        tsdb_oid_alloc_t na;
        ASSERT(tsdb_oid_alloc_open(&na, d, (uint16_t)(n + 1)) == TSDB_OK);
        for (int k = 0; k < 16; k++) {
            seen[n][k] = tsdb_oid_next(&na);
            ASSERT(tsdb_oid_node(seen[n][k]) == (uint16_t)(n + 1));
        }
        tsdb_oid_alloc_close(&na);
    }
    /* Cross-product: every pair of ids across the four nodes is distinct. */
    for (int n1 = 0; n1 < 4; n1++)
        for (int k1 = 0; k1 < 16; k1++)
            for (int n2 = 0; n2 < 4; n2++)
                for (int k2 = 0; k2 < 16; k2++)
                    if (!(n1 == n2 && k1 == k2))
                        ASSERT(seen[n1][k1] != seen[n2][k2]);
    printf("  4 node_ids x 16 ids: 0 collisions (distinct prefixes)\n");

    /* observe() advances the high-water so a future mint can't reissue. */
    {
        char d[256]; snprintf(d, sizeof(d), "%s/obs", TMP);
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", d); (void)system(cmd);
        tsdb_oid_alloc_t oa;
        ASSERT(tsdb_oid_alloc_open(&oa, d, 3) == TSDB_OK);
        tsdb_oid_t high = tsdb_oid_make(3, 5000000);
        tsdb_oid_observe(&oa, high);
        tsdb_oid_observe(&oa, tsdb_oid_make(9, 9000000)); /* other node — ignored */
        tsdb_oid_t after = tsdb_oid_next(&oa);
        ASSERT(tsdb_oid_seq(after) > 5000000);
        tsdb_oid_alloc_close(&oa);
        printf("  observe advanced past seq 5000000 (next=%llu)\n",
               (unsigned long long)tsdb_oid_seq(after));
    }

    snprintf(cmd, sizeof(cmd), "rm -rf %s", TMP); (void)system(cmd);
    printf("[PASS] OID allocator: monotonic, no reuse on reopen, collision-free across nodes\n");
    return 0;
}
