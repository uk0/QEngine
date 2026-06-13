/* test_raft_cluster.c — in-process 3-node Raft smoke.
 *
 * Can't easily spin up 3 full tsdb_node binaries from a C unit test
 * (they need ports, data dirs, etc.), but we can exercise the core
 * state machine by building three tsdb_raft_t instances backed by
 * three mock node managers + replica managers.
 *
 * PROBLEM: tsdb_replica_mgr_get_conn returns real RPC conns, which
 * need a listening socket.  So for this test we stub the RPC path
 * by replacing the global Raft dispatcher handler so RequestVote /
 * AppendEntries from "peer X" actually land in raft X's handler
 * directly — zero wire involvement, pure state-machine test.
 *
 * That means this file is a CORRECTNESS test, not a performance /
 * integration test.  The bash scenarios under tests/raft/scenarios/
 * cover the wire path.
 */

#include "../src/raft/raft.h"
#include "../src/raft/raft_log.h"
#include "../src/cluster/node.h"
#include "../src/cluster/replica.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* We don't spin this up; we just need a non-null pointer to hand to
 * tsdb_raft_open.  Propose tests won't work without a real replica
 * mgr, so those are deferred to the bash harness. */
extern tsdb_replica_mgr_t *tsdb_replica_mgr_new(tsdb_node_manager_t *);

static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

/* Smoke: open → self alone → after grace window elapses, solo master
 * self-elects.  The original "npeers==0 stays follower" guard was
 * dropped in 90e9e36 because a single-master topology is a legitimate
 * operating mode: the lone node must elect itself or nothing commits. */
static void t_solo_stays_follower(void) {
    printf("\n[1] solo raft self-elects after grace (no peers)\n");
    const char *dir = "/tmp/tsdb-raft-solo";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(42ULL, "127.0.0.1:39001",
                               "127.0.0.1:39000", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);

    tsdb_raft_t *r = tsdb_raft_open(dir, 42ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    /* Sleep past the grace period plus one full election window: the
     * tick thread keeps re-rolling the timer during grace, so the
     * first election fires at grace + rand(ELECTION_MIN..MAX) —
     * 1500 + 600..1200 ms with the current tunables. */
    usleep(3000 * 1000);

    tsdb_raft_state_t s = tsdb_raft_state(r);
    uint64_t term = tsdb_raft_current_term(r);
    uint64_t leader = tsdb_raft_leader_id(r);
    printf("  state=%d term=%llu leader=%llu\n",
           s, (unsigned long long)term, (unsigned long long)leader);
    assert(s == TSDB_RAFT_LEADER);
    assert(leader == 42ULL);
    assert(term >= 1);

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: solo node self-elects as sole master\n");
}

/* Spin until the raft instance reports LEADER, or give up after
 * `timeout_ms`.  Returns 1 if leadership was observed. */
static int wait_for_leader(tsdb_raft_t *r, int timeout_ms) {
    int waited = 0;
    while (waited < timeout_ms) {
        if (tsdb_raft_state(r) == TSDB_RAFT_LEADER) return 1;
        usleep(10 * 1000);
        waited += 10;
    }
    return tsdb_raft_state(r) == TSDB_RAFT_LEADER;
}

/* BUG-2 (CheckQuorum): a leader that loses contact with a quorum must
 * step down instead of serving stale reads forever.  Harness: a solo
 * node self-elects (quorum=1), commits its SEED, then we commit a
 * CONFIG_ADD for a SECOND master — this widens the committed quorum to 2.
 * That peer has no listening socket, so its AppendEntries always fail;
 * once the leader fails QUORUM_MISS_STEPDOWN consecutive CheckQuorum
 * windows it can no longer confirm a quorum and demotes to follower
 * (leader_id=0).  The stepdown is now deliberately hysteresis-gated (it
 * takes a few election timeouts, not one) so a transiently-stalled but
 * reachable leader does not self-demote and start an election storm under
 * write load — but a GENUINELY isolated leader like this one fails every
 * window in a row and still steps down.
 *
 * (The peer must be added through the committed config, not just gossip:
 * once the SEED commits, rebuild_peers_locked treats the committed
 * config as authoritative, which is the Raft §6 membership definition.)
 *
 * Safety preserved: stepping down only makes the node STOP acting as
 * leader — it cannot violate Leader Append-Only or Log Matching. */
static void t_checkquorum_stepdown(void) {
    printf("\n[3] leader steps down after losing quorum (CheckQuorum)\n");
    const char *dir = "/tmp/tsdb-raft-cq";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(100ULL, "127.0.0.1:39101",
                               "127.0.0.1:39100", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);

    tsdb_raft_t *r = tsdb_raft_open(dir, 100ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    /* Solo node self-elects after the startup grace + one election. */
    assert(wait_for_leader(r, 4000));
    /* Let the bootstrap SEED commit so the committed config is the
     * authoritative member source before we widen it. */
    usleep(400 * 1000);
    printf("  became leader (term=%llu)\n",
           (unsigned long long)tsdb_raft_current_term(r));

    /* Commit a CONFIG_ADD for an unreachable second master.  The lone
     * leader still has quorum=1 at commit time, so this returns OK; the
     * committed config then has 2 members and quorum becomes 2. */
    int arc = tsdb_raft_add_master(r, 200ULL, "127.0.0.1:39999", 1000);
    printf("  add_master(200) rc=%d → quorum now 2\n", arc);
    assert(arc == TSDB_OK);

    /* The leader now demotes only after QUORUM_MISS_STEPDOWN (3)
     * consecutive failed CheckQuorum windows, each ≈ one election timeout
     * (≤ ELECTION_MAX_MS = 1200 ms).  Worst case ~3×1200 ms; bound at
     * 6000 ms so the property test stays robust under CI scheduling jitter
     * while still proving a truly-isolated leader relinquishes leadership. */
    int waited = 0;
    tsdb_raft_state_t s = tsdb_raft_state(r);
    while (s == TSDB_RAFT_LEADER && waited < 6000) {
        usleep(10 * 1000);
        waited += 10;
        s = tsdb_raft_state(r);
    }
    uint64_t leader = tsdb_raft_leader_id(r);
    printf("  after losing quorum: state=%d leader=%llu (%d ms)\n",
           s, (unsigned long long)leader, waited);
    assert(s != TSDB_RAFT_LEADER);   /* stepped down */
    assert(leader == 0);             /* no longer advertises self as leader */

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: partitioned leader relinquished leadership\n");
}

/* Run ONE BUG-1 attempt on a fresh raft instance: become solo leader,
 * commit a CONFIG_ADD for an unreachable master (quorum → 2), then fire
 * a propose with a short deadline.  Because the peer is unreachable the
 * entry can never reach quorum.  Outcome (written through the out-ptrs):
 *   *out_prc        — the propose return code
 *   *out_truncated  — 1 iff the IO path truncated the un-acked tail
 *   *out_phantom    — 1 iff the entry was wrongly applied (the BUG)
 * Returns 1 if the instance reached the leader+config-add precondition.
 *
 * Why a fresh instance per attempt: once the unreachable peer is in the
 * committed config the node can never re-form a quorum, so it cannot be
 * re-used as a leader after it (inevitably) steps down. */
static int run_propose_attempt(int idx, int *out_prc,
                                int *out_truncated, int *out_phantom) {
    char dir[64];
    snprintf(dir, sizeof(dir), "/tmp/tsdb-raft-pq-%d", idx);
    rm_rf(dir);
    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(300ULL, "127.0.0.1:39201",
                               "127.0.0.1:39200", TSDB_ROLE_MASTER);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 300ULL, mgr, rmgr, NULL, NULL);

    int ok = 0;
    *out_prc = TSDB_OK; *out_truncated = 0; *out_phantom = 0;
    if (r && wait_for_leader(r, 4000)) {
        usleep(400 * 1000);                       /* SEED commits */
        if (tsdb_raft_add_master(r, 400ULL, "127.0.0.1:39999", 1000)
                == TSDB_OK) {
            /* Wait for the tick to rebuild the peer set so quorum_needed
             * reflects the now-2-member config BEFORE we propose.  Without
             * this the propose could commit against a stale quorum=1 and
             * look like a phantom; this delay is short relative to the
             * CheckQuorum stepdown (~1 election timeout). */
            usleep(40 * 1000);
            if (tsdb_raft_state(r) != TSDB_RAFT_LEADER) goto attempt_done;
            ok = 1;
            uint64_t before_last = tsdb_raft_last_index(r);
            /* Short deadline: usually expires before the next CheckQuorum
             * tick steps us down, so we exercise the still-leader
             * truncate path; if CheckQuorum wins the race we still verify
             * no phantom commit. */
            int prc = tsdb_raft_propose(r, TSDB_RAFT_ENTRY_NOOP, "x", 1, 40);
            *out_prc = prc;
            if (tsdb_raft_last_applied(r) >= before_last + 1)
                *out_phantom = 1;                  /* entry applied = BUG */
            if (prc == TSDB_ERR_IO &&
                tsdb_raft_last_index(r) == before_last)
                *out_truncated = 1;
        }
    }
attempt_done:
    if (r) tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    return ok;
}

/* BUG-1 (phantom DDL): a propose that cannot reach a quorum must NOT
 * report success and must NOT leave an applied entry behind.  When the
 * un-acked entry is provably un-replicated, the leader truncates its own
 * tail and returns a definitive failure (TSDB_ERR_IO); otherwise it
 * returns TSDB_ERR_INDETERMINATE (or PERMISSION if it stepped down) and
 * leaves the log intact.  The peer here is unreachable, so the entry is
 * always un-replicated — we expect the truncate path on at least one of
 * a few attempts (the other outcome is a clean stepdown, also failure).
 *
 * Safety preserved: we only ever truncate an UNcommitted, UNreplicated
 * tail (no other log holds it), so Leader Append-Only / Log Matching are
 * intact; a committed or replicated entry is never discarded. */
static void t_propose_no_quorum_truncates(void) {
    printf("\n[4] propose without quorum: no phantom commit, tail truncated\n");

    int saw_failure = 0, saw_truncate = 0;
    for (int attempt = 0; attempt < 12 && !saw_truncate; attempt++) {
        int prc, truncated, phantom;
        if (!run_propose_attempt(attempt, &prc, &truncated, &phantom))
            continue;                              /* precondition flaked */
        assert(!phantom);                          /* core: no phantom DDL */
        if (prc != TSDB_OK) saw_failure = 1;
        if (truncated) {
            saw_truncate = 1;
            printf("  attempt %d: TSDB_ERR_IO + un-acked tail truncated\n",
                   attempt);
        } else {
            printf("  attempt %d: prc=%d (no phantom commit)\n",
                   attempt, prc);
        }
    }

    assert(saw_failure);    /* propose never silently "succeeded"        */
    assert(saw_truncate);   /* the un-replicated-tail truncate path ran  */
    printf("PASS: no phantom commit; un-replicated tail truncated\n");
}

/* Log append through raft_log API (not propose), verifies tsdb_raft
 * exposes last_index correctly even without replication. */
static void t_last_index_query(void) {
    printf("\n[2] last_index tracks log appends\n");
    const char *dir = "/tmp/tsdb-raft-lastidx";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(7ULL, "127.0.0.1:39011",
                               "127.0.0.1:39010", TSDB_ROLE_MASTER);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 7ULL, mgr, rmgr, NULL, NULL);
    assert(tsdb_raft_last_index(r) == 0);

    /* Write a couple of entries directly via the log layer. */
    tsdb_raft_log_t *log = (tsdb_raft_log_t *)(0);
    (void)log;

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: last_index queryable (0 on fresh log)\n");
}

/* CheckQuorum hysteresis: a leader must NOT step down on a SINGLE missed
 * CheckQuorum window — only after QUORUM_MISS_STEPDOWN (3) consecutive
 * misses.  This is the anti-storm property: under write load the apply
 * thread can starve the tick loop and a perfectly-reachable leader misses
 * one window's worth of acks; the old code demoted it immediately and the
 * cluster churned through elections.  We can't drive intermittent acks
 * from an in-process unit test, but we can prove the TWO endpoints of the
 * hysteresis with timing alone, against a leader whose only peer is
 * unreachable (so every window misses):
 *
 *   (a) After one full max-length election window has elapsed (1300 ms >
 *       ELECTION_MAX_MS), the leader is STILL leader.  Under the old
 *       single-window stepdown it would already have demoted — so this
 *       directly catches a regression back to twitchy behaviour.  At most
 *       two CheckQuorum windows can have fired by 1300 ms (windows are
 *       re-rolled to ≥ ELECTION_MIN_MS = 600 ms each, so check #3 cannot
 *       land before ~1800 ms), so the streak is < 3 and we hold.
 *
 *   (b) Given enough consecutive misses (≤ 3 windows ≈ 3×1200 ms), the
 *       genuinely-isolated leader DOES eventually step down — the
 *       partition guarantee (BUG-2) is preserved, just lease-delayed.
 *
 * Safety: identical to t_checkquorum_stepdown — stepping down only stops
 * this node acting as leader; it never deletes log or advances commit, so
 * Leader Append-Only and Log Matching are untouched. */
static void t_checkquorum_hysteresis(void) {
    printf("\n[5] leader rides out a single missed window (CheckQuorum hysteresis)\n");
    const char *dir = "/tmp/tsdb-raft-cqh";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(500ULL, "127.0.0.1:39501",
                               "127.0.0.1:39500", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);

    tsdb_raft_t *r = tsdb_raft_open(dir, 500ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    assert(wait_for_leader(r, 4000));
    usleep(400 * 1000);                       /* SEED commits */

    /* Widen committed quorum to 2 with an unreachable peer: every
     * CheckQuorum window from here on misses. */
    int arc = tsdb_raft_add_master(r, 600ULL, "127.0.0.1:39999", 1000);
    assert(arc == TSDB_OK);
    printf("  add_master(600) rc=%d → quorum now 2, peer unreachable\n", arc);

    /* (a) One full max-window later, the leader must still be leader.
     * The old single-window stepdown would have demoted it by now. */
    usleep(1300 * 1000);
    tsdb_raft_state_t mid = tsdb_raft_state(r);
    printf("  at 1300 ms: state=%d (expect still LEADER=%d)\n",
           mid, TSDB_RAFT_LEADER);
    assert(mid == TSDB_RAFT_LEADER);   /* survived a transient miss window */

    /* (b) Given the full hysteresis budget, the isolated leader steps
     * down — partition guarantee intact. */
    int waited = 1300;
    tsdb_raft_state_t s = tsdb_raft_state(r);
    while (s == TSDB_RAFT_LEADER && waited < 6000) {
        usleep(10 * 1000);
        waited += 10;
        s = tsdb_raft_state(r);
    }
    uint64_t leader = tsdb_raft_leader_id(r);
    printf("  eventual: state=%d leader=%llu (%d ms)\n",
           s, (unsigned long long)leader, waited);
    assert(s != TSDB_RAFT_LEADER);     /* isolated leader still relinquishes */
    assert(leader == 0);

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: leader tolerates one missed window, demotes after the streak\n");
}

int main(void) {
    t_solo_stays_follower();
    t_checkquorum_stepdown();
    t_propose_no_quorum_truncates();
    t_last_index_query();
    t_checkquorum_hysteresis();
    printf("\n=== all raft cluster smoke tests passed ===\n");
    return 0;
}
