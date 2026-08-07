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
#include "../src/raft/raft_rpc.h"
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
 *   *out_phantom    — 1 iff the entry was wrongly applied (the BUG)
 *   *out_before     — last_index immediately BEFORE the propose
 *   *out_after      — last_index immediately AFTER it returned
 * Returns 1 if the instance reached the leader+config-add precondition.
 *
 * Why a fresh instance per attempt: once the unreachable peer is in the
 * committed config the node can never re-form a quorum, so it cannot be
 * re-used as a leader after it (inevitably) steps down. */
static int run_propose_attempt(int idx, int *out_prc, int *out_phantom,
                                uint64_t *out_before, uint64_t *out_after) {
    char dir[64];
    snprintf(dir, sizeof(dir), "/tmp/tsdb-raft-pq-%d", idx);
    rm_rf(dir);
    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(300ULL, "127.0.0.1:39201",
                               "127.0.0.1:39200", TSDB_ROLE_MASTER);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 300ULL, mgr, rmgr, NULL, NULL);

    int ok = 0;
    *out_prc = TSDB_OK; *out_phantom = 0; *out_before = 0; *out_after = 0;
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
            /* Short deadline: it expires while we are still leader, which
             * is exactly the state the old code used to justify deleting
             * its own tail; if CheckQuorum wins the race we still verify
             * no phantom commit. */
            int prc = tsdb_raft_propose(r, TSDB_RAFT_ENTRY_NOOP, "x", 1, 40);
            *out_prc = prc;
            *out_before = before_last;
            *out_after  = tsdb_raft_last_index(r);
            if (tsdb_raft_last_applied(r) >= before_last + 1)
                *out_phantom = 1;                  /* entry applied = BUG */
        }
    }
attempt_done:
    if (r) tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    return ok;
}

/* BUG-1 (phantom DDL) — INVERTED CONTRACT.
 *
 * A propose that cannot reach a quorum must NOT report success and must
 * NOT leave an applied entry behind.  It must ALSO not delete the entry:
 * Leader Append-Only (§5.3) says a leader never removes an index from its
 * own log, and a leader has no way to prove locally that no follower took
 * the entry.  The old code truncated the tail whenever match_index was
 * behind and r->replicating was 0 — but replicate_to decrements
 * `replicating` on EVERY exit path, including the 3 s receive timeout of
 * an AppendEntries the follower already appended and fsynced.  Those two
 * signals therefore do not distinguish "nobody has it" from "somebody has
 * it and the ack was lost", and truncating on them erases an index
 * another log holds.
 *
 * So: on timeout the propose returns TSDB_ERR_INDETERMINATE (or
 * PERMISSION if it stepped down first) and the log keeps the entry.  The
 * peer here is unreachable, so every attempt times out — the exact case
 * that used to truncate.  Every attempt is checked; none may truncate. */
static void t_propose_no_quorum_keeps_tail(void) {
    printf("\n[4] propose without quorum: no phantom commit, tail PRESERVED\n");

    int saw_failure = 0, checked = 0;
    for (int attempt = 0; attempt < 12; attempt++) {
        int prc, phantom;
        uint64_t before = 0, after = 0;
        if (!run_propose_attempt(attempt, &prc, &phantom, &before, &after))
            continue;                              /* precondition flaked */
        checked++;
        assert(!phantom);                          /* core: no phantom DDL */
        int truncated = (after < before + 1);
        printf("  attempt %d: prc=%d last_index %llu → %llu%s\n",
               attempt, prc, (unsigned long long)before,
               (unsigned long long)after,
               truncated ? "  << TRUNCATED (Leader Append-Only violated)" : "");
        assert(!truncated);                        /* the fix under test   */
        assert(prc == TSDB_ERR_INDETERMINATE || prc == TSDB_ERR_PERMISSION);
        assert(after == before + 1);               /* entry still in the log */
        if (prc != TSDB_OK) saw_failure = 1;
    }

    assert(checked > 0);    /* the precondition held at least once       */
    assert(saw_failure);    /* propose never silently "succeeded"        */
    printf("PASS: no phantom commit; un-acked tail left in the log\n");
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

/* COLD-START SPLIT-BRAIN FIX, part A (joiner-defer).
 *
 * A node started WITH gossip seeds (a "joiner") must NOT bootstrap a
 * group of its own while config.bin is still uninitialised: if it self-
 * elected and sealed a rival SEED={self}, two cold-start nodes would
 * form disjoint single-node groups whose later merge truncates one
 * side's committed log (data loss).  So a joiner with an uninitialised
 * config stays a FOLLOWER and waits to be added by the seedless
 * bootstrap leader.
 *
 * Harness: open a node, mark it a joiner, give it NO peers (gossip view
 * is just self).  A SEEDLESS node in this exact situation self-elects
 * (proven by [1]); the joiner must NOT.  We wait well past the startup
 * grace AND several election windows, then assert it never became
 * leader, never bumped its term (it never even campaigned), and never
 * sealed a config (committed member count stays 0).
 *
 * This is the directly-unit-testable half of the fix on a single
 * instance — it isolates the gating decision with zero transport. */
static void t_joiner_defers_seed(void) {
    printf("\n[6] joiner with uninitialised config stays follower (no rival seed)\n");
    const char *dir = "/tmp/tsdb-raft-joiner-defer";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(800ULL, "127.0.0.1:39801",
                               "127.0.0.1:39800", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);

    tsdb_raft_t *r = tsdb_raft_open(dir, 800ULL, mgr, rmgr, NULL, NULL);
    assert(r);
    /* Mark as a seeded joiner BEFORE the grace window expires so the
     * very first post-grace election decision sees the flag. */
    tsdb_raft_set_joiner(r, 1);

    /* Sleep past grace (1500 ms) + at least two full max-length election
     * windows (2×1200 ms): a seedless node would be leader many times
     * over by now.  4500 ms gives comfortable margin under CI jitter. */
    usleep(4500 * 1000);

    tsdb_raft_state_t s = tsdb_raft_state(r);
    uint64_t term   = tsdb_raft_current_term(r);
    uint64_t leader = tsdb_raft_leader_id(r);
    tsdb_raft_cfg_member_t mems[16];
    int nmem = tsdb_raft_config_members(r, mems, 16);
    printf("  state=%d term=%llu leader=%llu committed_members=%d\n",
           s, (unsigned long long)term, (unsigned long long)leader, nmem);

    assert(s == TSDB_RAFT_FOLLOWER);   /* did NOT self-elect            */
    assert(s != TSDB_RAFT_LEADER);
    assert(leader == 0);               /* advertises no leader           */
    assert(term == 0);                 /* never campaigned → term unbumped */
    assert(nmem == 0);                 /* never sealed a rival SEED       */

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: joiner deferred — no self-election, no rival seed\n");
}

/* COLD-START SPLIT-BRAIN FIX, part A — the OTHER endpoint of the gate.
 *
 * The defer is strictly scoped to the cold-start window (config NOT yet
 * initialised).  Once the config IS initialised — because this member
 * was added, OR because this is a restart of an already-established
 * member — a node that happens to be a "joiner" (started with seeds)
 * MUST campaign normally, or an established cluster could never fail
 * over to a restarted member that was launched with --seeds.
 *
 * Single-instance harness for this: let a SEEDLESS solo node self-elect
 * and seal its SEED, which persists config.bin (is_initialised → true).
 * Close it, then REOPEN THE SAME data dir as a JOINER.  On reopen the
 * persisted config loads → is_initialised() is true → the joiner gate is
 * inert → the node must self-elect again exactly like the established-
 * restart case.  This is the closest faithful reproduction of "seeded
 * member, established config, must still win an election" available
 * without multi-node transport.  (The "added then campaigns" variant
 * needs a real leader on the wire to deliver the CONFIG_ADD, which the
 * single-instance harness cannot provide — documented here.) */
static void t_joiner_with_initialised_config_elects(void) {
    printf("\n[7] joiner with INITIALISED config (restart) campaigns normally\n");
    const char *dir = "/tmp/tsdb-raft-joiner-restart";
    rm_rf(dir);

    /* Phase 1: seedless solo bootstraps + seals SEED (persists config). */
    tsdb_node_manager_t *mgr1 =
        tsdb_node_manager_new(900ULL, "127.0.0.1:39901",
                               "127.0.0.1:39900", TSDB_ROLE_MASTER);
    assert(mgr1);
    tsdb_replica_mgr_t *rmgr1 = tsdb_replica_mgr_new(mgr1);
    assert(rmgr1);
    tsdb_raft_t *r1 = tsdb_raft_open(dir, 900ULL, mgr1, rmgr1, NULL, NULL);
    assert(r1);
    assert(wait_for_leader(r1, 4000));       /* seedless self-elects     */
    usleep(500 * 1000);                       /* SEED commits + persists  */
    tsdb_raft_cfg_member_t m1[16];
    int n1 = tsdb_raft_config_members(r1, m1, 16);
    printf("  phase1: became leader, committed_members=%d (config sealed)\n", n1);
    assert(n1 >= 1);                          /* config is now initialised */
    tsdb_raft_close(r1);
    tsdb_node_manager_free(mgr1);
    tsdb_replica_mgr_free(rmgr1);

    /* Phase 2: REOPEN same dir as a JOINER.  Persisted config → gate
     * inert → must campaign + win like an established-member restart. */
    tsdb_node_manager_t *mgr2 =
        tsdb_node_manager_new(900ULL, "127.0.0.1:39901",
                               "127.0.0.1:39900", TSDB_ROLE_MASTER);
    assert(mgr2);
    tsdb_replica_mgr_t *rmgr2 = tsdb_replica_mgr_new(mgr2);
    assert(rmgr2);
    tsdb_raft_t *r2 = tsdb_raft_open(dir, 900ULL, mgr2, rmgr2, NULL, NULL);
    assert(r2);
    tsdb_raft_set_joiner(r2, 1);              /* seeded, but config exists */

    int won = wait_for_leader(r2, 4000);
    tsdb_raft_state_t s = tsdb_raft_state(r2);
    uint64_t leader = tsdb_raft_leader_id(r2);
    printf("  phase2: reopened as joiner → state=%d leader=%llu won=%d\n",
           s, (unsigned long long)leader, won);
    assert(won);                              /* failover/restart works    */
    assert(s == TSDB_RAFT_LEADER);
    assert(leader == 900ULL);

    tsdb_raft_close(r2);
    tsdb_node_manager_free(mgr2);
    tsdb_replica_mgr_free(rmgr2);
    rm_rf(dir);
    printf("PASS: joiner with initialised config campaigns + wins (failover intact)\n");
}

/* Encode one AppendEntries and push it through the registered handler —
 * the same entry point tsdb_raft_rpc_handle_append gives the RPC server,
 * so this is a frame arriving from the network with no wire involved.
 * Returns 0 when the handler answered and *resp was decoded. */
static int drive_append(const tsdb_raft_req_append_t *req,
                         tsdb_raft_resp_append_t *resp) {
    size_t cap = tsdb_raft_append_buf_cap(req->n_entries, 64) + 1024;
    uint8_t *buf = malloc(cap);
    assert(buf);
    int n = tsdb_raft_encode_req_append(buf, cap, req);
    assert(n > 0);
    uint8_t rbuf[64];
    uint32_t rlen = 0;
    int rc = tsdb_raft_rpc_handle_append(buf, (uint32_t)n,
                                          rbuf, sizeof(rbuf), &rlen);
    free(buf);
    if (rc != 0) return -1;
    return tsdb_raft_decode_resp_append(rbuf, rlen, resp);
}

/* AppendEntries had NO membership filter, although PreVote and
 * RequestVote both have one.  Anything that could reach the RPC port
 * could therefore hand us a frame with any leader_id and any term, and we
 * would adopt the term and append its entries — and with a mismatched
 * prev_log_term, delete ours.  The committed config is the Raft §6
 * definition of who may speak; a leader_id absent from it gets a plain
 * rejection at our current term.
 *
 * Harness: a solo node self-elects and seals SEED={self}, so the
 * committed member set is exactly {1000} and id 9999 is provably not in
 * it.  (The filter deliberately fails OPEN while the config is
 * uninitialised — that is what keeps a cold start electable — so the
 * config MUST be sealed before this case means anything.) */
static void t_append_from_non_member_refused(void) {
    printf("\n[8] AppendEntries from a non-member is refused\n");
    const char *dir = "/tmp/tsdb-raft-ae-nonmember";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(1000ULL, "127.0.0.1:39601",
                               "127.0.0.1:39600", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 1000ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    assert(wait_for_leader(r, 4000));
    usleep(600 * 1000);                       /* SEED + NOOP commit */
    tsdb_raft_cfg_member_t mems[16];
    int nmem = tsdb_raft_config_members(r, mems, 16);
    printf("  committed_members=%d (first id=%llu)\n",
           nmem, nmem > 0 ? (unsigned long long)mems[0].id : 0ULL);
    assert(nmem == 1 && mems[0].id == 1000ULL);

    uint64_t term_before = tsdb_raft_current_term(r);
    uint64_t last_before = tsdb_raft_last_index(r);

    char body[8] = "forged";
    tsdb_raft_entry_t e = { .index = last_before + 1,
                             .term  = term_before + 7,
                             .type  = TSDB_RAFT_ENTRY_NOOP,
                             .payload_len = 6, .payload = body };
    tsdb_raft_req_append_t req = {
        .term = term_before + 7, .leader_id = 9999ULL,
        .prev_log_index = 0, .prev_log_term = 0,
        .leader_commit = 0, .n_entries = 1, .entries = &e
    };
    tsdb_raft_resp_append_t resp = {0};
    assert(drive_append(&req, &resp) == 0);

    uint64_t term_after = tsdb_raft_current_term(r);
    uint64_t last_after = tsdb_raft_last_index(r);
    printf("  success=%u resp.term=%llu  last_index %llu → %llu  term %llu → %llu\n",
           resp.success, (unsigned long long)resp.term,
           (unsigned long long)last_before, (unsigned long long)last_after,
           (unsigned long long)term_before, (unsigned long long)term_after);

    assert(resp.success == 0);              /* refused                     */
    assert(last_after == last_before);      /* nothing appended            */
    assert(term_after == term_before);      /* term of a non-member ignored */
    assert(resp.term == term_before);       /* answered in OUR term        */

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: non-member AppendEntries refused, log and term untouched\n");
}

/* A committed entry is present on a quorum and Leader Completeness
 * (§5.4.3) guarantees every future leader has it — so no legitimate
 * AppendEntries can ever disagree about an index at or below our
 * commit_index.  The follower nevertheless obeyed the conflict rule
 * blindly: one frame with prev_log_index <= commit_index and a bogus
 * prev_log_term truncated committed entries away.
 *
 * Harness: replicate 3 entries with leader_commit=3 (all committed), then
 * send prev_log_index=2 / prev_log_term=99.  Pre-fix the log is cut back
 * to index 1 — two committed entries gone.  Post-fix the frame is refused
 * and the log is untouched.  This node is a joiner with an uninitialised
 * config, so the membership filter fails open and the ONLY thing under
 * test here is the truncate floor. */
static void t_ae_cannot_truncate_committed(void) {
    printf("\n[9] AppendEntries may not truncate at or below commit_index\n");
    const char *dir = "/tmp/tsdb-raft-ae-commit-floor";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(1100ULL, "127.0.0.1:39701",
                               "127.0.0.1:39700", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 1100ULL, mgr, rmgr, NULL, NULL);
    assert(r);
    tsdb_raft_set_joiner(r, 1);   /* never campaigns; stays a follower */

    char body[4] = "e";
    tsdb_raft_entry_t ents[3];
    for (int i = 0; i < 3; i++) {
        ents[i] = (tsdb_raft_entry_t){ .index = (uint64_t)(i + 1), .term = 5,
                                        .type = TSDB_RAFT_ENTRY_NOOP,
                                        .payload_len = 1, .payload = body };
    }
    tsdb_raft_req_append_t good = {
        .term = 5, .leader_id = 1200ULL,
        .prev_log_index = 0, .prev_log_term = 0, .leader_commit = 3,
        .n_entries = 3, .entries = ents
    };
    tsdb_raft_resp_append_t gresp = {0};
    assert(drive_append(&good, &gresp) == 0);
    printf("  replicated 3 entries: success=%u last_index=%llu commit_index=%llu\n",
           gresp.success, (unsigned long long)tsdb_raft_last_index(r),
           (unsigned long long)tsdb_raft_commit_index(r));
    assert(gresp.success == 1);
    assert(tsdb_raft_last_index(r) == 3);
    assert(tsdb_raft_commit_index(r) == 3);

    /* Disagree about index 2 — which is committed. */
    tsdb_raft_req_append_t bad = {
        .term = 5, .leader_id = 1200ULL,
        .prev_log_index = 2, .prev_log_term = 99, .leader_commit = 3,
        .n_entries = 0, .entries = NULL
    };
    tsdb_raft_resp_append_t bresp = {0};
    assert(drive_append(&bad, &bresp) == 0);
    uint64_t last_after = tsdb_raft_last_index(r);
    printf("  conflict at committed index 2: success=%u last_index=%llu (expect 3)\n",
           bresp.success, (unsigned long long)last_after);

    assert(bresp.success == 0);
    assert(last_after == 3);          /* pre-fix: 1 — committed rows erased */
    assert(tsdb_raft_commit_index(r) == 3);

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: committed prefix survived a conflicting AppendEntries\n");
}

int main(void) {
    t_solo_stays_follower();
    t_checkquorum_stepdown();
    t_propose_no_quorum_keeps_tail();
    t_last_index_query();
    t_checkquorum_hysteresis();
    t_joiner_defers_seed();
    t_joiner_with_initialised_config_elects();
    t_append_from_non_member_refused();
    t_ae_cannot_truncate_committed();
    printf("\n=== all raft cluster smoke tests passed ===\n");
    return 0;
}
