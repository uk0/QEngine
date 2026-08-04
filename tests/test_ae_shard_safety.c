/* test_ae_shard_safety.c — the two guards that keep the anti-entropy row-digest
 * from running away on a SHARDED cluster (the 2026-08-02 incident).
 *
 * The reverted digest let a node that was NOT a replica of a table fingerprint
 * its full physical storage, compare it to a co-replica's, and merge toward the
 * cross-node UNION.  With TSDB_SHARD_REPLICA_N < node-count each node
 * legitimately holds a DIFFERENT subset, so that merge cross-contaminates shards
 * and re-replicates without bound — a 3000-row table reached 15000 and 257M rows
 * were pulled before it was reverted.
 *
 * The fix is two pure, independently-verifiable guards:
 *   1. CO-REPLICA confinement (tsdb_ae_node_in_set): the digest runs ONLY
 *      between members of a table's replica set; a non-replica is filtered out.
 *   2. Anti-runaway pull budget (tsdb_ae_pull_budget): a sweep may pull at most
 *      one fullest-peer's worth, because AE only copies rows IN and cannot
 *      legitimately raise local past the source's count.
 *
 * Either guard alone stops the runaway; together they are defence in depth. */

#include "../include/tsdb_cluster.h"

#include <stdio.h>
#include <stdint.h>

static int g_pass = 0, g_fail = 0;

#define CHECK(cond, ...) do { \
    if (!(cond)) { fprintf(stderr, "FAIL [%s:%d]: ", __FILE__, __LINE__); \
                   fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); g_fail++; } \
    else { printf("PASS: "); printf(__VA_ARGS__); printf("\n"); g_pass++; } \
} while (0)

int main(void) {
    printf("=== test_ae_shard_safety ===\n");

    /* ── Guard 1: co-replica confinement ─────────────────────────────────── */
    printf("\n[1] co-replica confinement (tsdb_ae_node_in_set)\n");
    {
        /* Table replica set (SHARD_REPLICA_N=2) across a 4-node cluster. */
        uint64_t rset[2] = { 1001, 1002 };

        CHECK(tsdb_ae_node_in_set(1001, rset, 2) == 1,
              "a replica (n1) IS in the set -> digest-verified");
        CHECK(tsdb_ae_node_in_set(1002, rset, 2) == 1,
              "the other replica (n2) IS in the set -> digest-verified");

        /* n3/n4 are NOT replicas of this table.  They may physically hold
         * transient non-owner write copies, but merging against them is exactly
         * what crossed shards and ran away — they MUST be filtered out. */
        CHECK(tsdb_ae_node_in_set(1003, rset, 2) == 0,
              "a NON-replica (n3, transient copies) is filtered out -> no cross-shard merge");
        CHECK(tsdb_ae_node_in_set(1004, rset, 2) == 0,
              "a NON-replica (n4) is filtered out");

        /* An empty set means routing failed / standalone — the caller then keeps
         * the whole-cluster behaviour, which is correct when nothing is sharded. */
        CHECK(tsdb_ae_node_in_set(1003, rset, 0) == 0,
              "empty set contains nothing (caller special-cases rsn==0)");
    }

    /* ── Guard 2: anti-runaway pull budget ───────────────────────────────── */
    printf("\n[2] anti-runaway pull budget (tsdb_ae_pull_budget)\n");
    {
        /* The bound is the fullest peer's TOTAL: a heal can never need to insert
         * more rows than the source holds (union <= local + peer_hi). */
        CHECK(tsdb_ae_pull_budget(2400, 3000) == 3000,
              "local 2400, fullest peer 3000 -> budget 3000 (a peer's worth)");

        /* THE REGRESSION THIS PINS.  At EQUAL counts the budget must still be
         * positive: equal-count-different-content IS the middle hole the digest
         * exists to heal.  The earlier (peer_hi - local) form gave 0 here and
         * silently neutered the heal — the live cluster logged "hit the pull
         * budget (0 rows)" every sweep while merging nothing. */
        CHECK(tsdb_ae_pull_budget(3000, 3000) == 3000,
              "EQUAL counts (the middle hole) -> budget POSITIVE, heal not neutered");

        /* Holding MORE than a peer does not mean "done": this node can still be
         * missing that peer's interior rows. */
        CHECK(tsdb_ae_pull_budget(3000, 2400) == 2400,
              "local ahead in COUNT can still lack interior rows -> budget 2400");

        /* Still hard-bounded: one sweep can never merge past a peer's worth, so
         * the 257M-row explosion is impossible however wrong the diff is. */
        CHECK(tsdb_ae_pull_budget(15000, 3000) == 3000,
              "runaway state is capped at one peer's worth (3000), never 257M");

        /* A peer that holds nothing can contribute nothing. */
        CHECK(tsdb_ae_pull_budget(1000, 0) == 0,
              "empty peer -> budget 0 (nothing to copy from)");
    }

    /* ── Composed: the incident cannot recur ─────────────────────────────── */
    printf("\n[3] composed — a non-replica in the runaway state is doubly stopped\n");
    {
        uint64_t rset[2] = { 1001, 1002 };
        uint64_t non_replica = 1003;   /* the node that ran away */
        uint64_t local_junk  = 15000;  /* its over-counted storage */
        uint64_t peer_hi     = 3000;   /* the real fullest co-replica */

        int confined_out = (tsdb_ae_node_in_set(non_replica, rset, 2) == 0);
        uint64_t budget  = tsdb_ae_pull_budget(local_junk, peer_hi);

        CHECK(confined_out,
              "guard 1: the runaway node is not a replica -> it never digests");
        CHECK(budget <= peer_hi,
              "guard 2: even if it did, one sweep merges at most a peer's worth (%llu)",
              (unsigned long long)budget);
        CHECK(confined_out && budget <= peer_hi && budget < local_junk,
              "both guards bound the 257M-row runaway (cap %llu << runaway 15000)",
              (unsigned long long)budget);
    }

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
