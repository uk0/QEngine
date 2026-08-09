/* test_raft_liveness.c — RAFT-04/05/06/07: replication liveness and
 * membership coupling.
 *
 * Same single-instance harness as test_raft_cluster.c: one real
 * tsdb_raft_t, peers injected through the gossip node manager, no
 * second live node.  Each case is a property a multi-node deployment
 * relies on, reduced to what one instance can prove:
 *
 *  [A] RAFT-07 — a leader must NOT accept a configuration change until
 *      its current-term NOOP has committed (the §5.4.2 gate the
 *      single-server membership-change correction requires).  We catch
 *      the window between winning the election and the NOOP commit and
 *      demand TSDB_ERR_BUSY.
 *
 *  [B] RAFT-05 — a gossip-visible master must NOT be auto-promoted into
 *      the voting set at match_index 0.  An unreachable "master" is
 *      injected into gossip; the committed config must not grow (the
 *      learner can never catch up), and the leader must keep committing
 *      (quorum arithmetic counts voting members only).
 *
 *  [C] RAFT-06 — data-plane ownership must follow the COMMITTED config:
 *      once REMOVE MASTER commits, the node leaves the hashring and
 *      gossip (which still sees it ALIVE) must not re-seat it as an
 *      owner.  A committed ADD re-admits it.
 *
 *  [D] RAFT-04 — replication must not block the caller on a slow peer.
 *      A peer that accepts TCP but never answers stalls AppendEntries
 *      for the full 3 s RPC deadline; a propose with a 300 ms commit
 *      deadline must return in ~300 ms, not after the wire timeout.
 */

#include "../src/raft/raft.h"
#include "../src/cluster/node.h"
#include "../src/cluster/replica.h"
#include "../include/tsdb.h"

#include <assert.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* Test-only introspection defined in raft.c (no public header slot):
 * 1 while this node is leader and its current-term NOOP has not yet
 * committed+applied. */
extern int tsdb_raft_noop_pending(tsdb_raft_t *r);

static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static int wait_for_leader(tsdb_raft_t *r, int timeout_ms) {
    int waited = 0;
    while (waited < timeout_ms) {
        if (tsdb_raft_state(r) == TSDB_RAFT_LEADER) return 1;
        usleep(10 * 1000);
        waited += 10;
    }
    return tsdb_raft_state(r) == TSDB_RAFT_LEADER;
}

/* Inject a gossip record for an ALIVE master peer, the way a SWIM
 * STATE_SYNC merge would. */
static void gossip_master(tsdb_node_manager_t *mgr, uint64_t id,
                           const char *addr, uint64_t ver) {
    tsdb_node_info_t info = {0};
    info.id = id;
    snprintf(info.addr, sizeof(info.addr), "%s", addr);
    snprintf(info.gossip_addr, sizeof(info.gossip_addr), "%s", addr);
    info.state   = TSDB_NODE_ALIVE;
    info.role    = TSDB_ROLE_MASTER;
    info.version = ver;
    tsdb_node_manager_upsert(mgr, &info);
}

static int config_has(tsdb_raft_t *r, uint64_t id) {
    tsdb_raft_cfg_member_t mems[16];
    int n = tsdb_raft_config_members(r, mems, 16);
    for (int i = 0; i < n; i++) {
        if (mems[i].id == id) return 1;
    }
    return 0;
}

static int ring_has(tsdb_node_manager_t *mgr, uint64_t id) {
    tsdb_node_id_t out[8] = {0};
    int n = tsdb_node_manager_ring_owner(mgr, "probe-shard-key", 8, out);
    for (int i = 0; i < n; i++) {
        if (out[i] == id) return 1;
    }
    return 0;
}

/* ---- [A] RAFT-07: config change refused until current-term NOOP commits ---
 *
 * One attempt: fresh instance, poll tightly for the LEADER transition,
 * then fire ADD MASTER inside the pre-NOOP window.  become_leader sets
 * the pending-NOOP mark under the lock and the tick thread cannot
 * propose the NOOP for at least one 10 ms tick, so a sub-millisecond
 * poll reliably lands inside the window.
 *
 * Outcomes per attempt:
 *   1  window hit, gate held (BUSY)          → property proven
 *  -1  window hit, config change WENT THROUGH → the RAFT-07 bug
 *   0  window missed (NOOP already committed) → retry on a fresh dir
 */
static int noop_gate_attempt(int idx) {
    char dir[64];
    snprintf(dir, sizeof(dir), "/tmp/tsdb-raft-live-a-%d", idx);
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(42ULL, "127.0.0.1:39311",
                               "127.0.0.1:39310", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 42ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    int verdict = 0;

    /* Tight poll for the leader transition (grace 1500 ms + election). */
    int64_t give_up = now_ms() + 6000;
    while (now_ms() < give_up && tsdb_raft_state(r) != TSDB_RAFT_LEADER)
        usleep(200);
    if (tsdb_raft_state(r) != TSDB_RAFT_LEADER) goto out;

    {
        int pend_before = tsdb_raft_noop_pending(r);
        int arc = tsdb_raft_add_master(r, 55ULL, "127.0.0.1:39998", 300);
        int pend_after = tsdb_raft_noop_pending(r);
        printf("  attempt %d: pend_before=%d add rc=%d pend_after=%d\n",
               idx, pend_before, arc, pend_after);
        if (!pend_before) goto out;              /* window missed */
        if (arc == TSDB_ERR_BUSY) {
            /* Gate held.  It must lift once the NOOP commits: the same
             * ADD must eventually be accepted. */
            int64_t lift = now_ms() + 4000;
            int arc2 = TSDB_ERR_BUSY;
            while (now_ms() < lift) {
                arc2 = tsdb_raft_add_master(r, 55ULL, "127.0.0.1:39998", 500);
                if (arc2 == TSDB_OK) break;
                usleep(50 * 1000);
            }
            printf("  attempt %d: post-NOOP add rc=%d\n", idx, arc2);
            verdict = (arc2 == TSDB_OK) ? 1 : 0;
            goto out;
        }
        /* The call went through (or failed some other way) while the
         * NOOP was pending.  If the NOOP was STILL pending after the
         * call returned, the config change was accepted strictly inside
         * the forbidden window — the RAFT-07 defect. */
        if (arc == TSDB_OK && pend_after) verdict = -1;
        /* else: NOOP committed mid-call — ambiguous, retry. */
    }

out:
    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    return verdict;
}

static void t_config_change_gated_on_noop(void) {
    printf("\n[A] RAFT-07: config change before current-term NOOP commit is refused\n");
    int proven = 0;
    for (int i = 0; i < 6 && !proven; i++) {
        int v = noop_gate_attempt(i);
        assert(v != -1);   /* config change accepted pre-NOOP = the bug */
        if (v == 1) proven = 1;
    }
    assert(proven);        /* the window was hit and the gate held */
    printf("PASS: pre-NOOP config change refused (BUSY), accepted after commit\n");
}

/* ---- [B] RAFT-05: no auto-promotion at match_index 0 -------------------- */
static void t_no_autopromote_uncaught_master(void) {
    printf("\n[B] RAFT-05: gossip master is NOT auto-added before catching up\n");
    const char *dir = "/tmp/tsdb-raft-live-b";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(42ULL, "127.0.0.1:39321",
                               "127.0.0.1:39320", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 42ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    assert(wait_for_leader(r, 4000));
    usleep(600 * 1000);                    /* SEED + NOOP commit */
    assert(config_has(r, 42ULL));

    /* An ALIVE master appears in gossip.  Nothing listens on its
     * address, so its log can never catch up (match_index stays 0). */
    gossip_master(mgr, 77ULL, "127.0.0.1:39997", 1);

    /* Give the leader many ticks.  The old code auto-added the peer to
     * the committed config within one tick, jumping quorum to 2 and
     * wedging the cluster on an empty-logged, unreachable "member". */
    usleep(1500 * 1000);

    tsdb_raft_cfg_member_t mems[16];
    int nmem = tsdb_raft_config_members(r, mems, 16);
    printf("  committed members after 1.5 s: %d (has77=%d)\n",
           nmem, config_has(r, 77ULL));
    assert(!config_has(r, 77ULL));   /* never promoted at match 0 */
    assert(nmem == 1);

    /* Quorum arithmetic must still count VOTING members only: the
     * solo leader stays leader and keeps committing. */
    assert(tsdb_raft_state(r) == TSDB_RAFT_LEADER);
    int prc = tsdb_raft_propose(r, TSDB_RAFT_ENTRY_NOOP, "x", 1, 2000);
    printf("  propose with uncaught learner present rc=%d\n", prc);
    assert(prc == TSDB_OK);

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: uncaught-up master stays out of the voting set; leader unaffected\n");
}

/* ---- [C] RAFT-06: hashring follows the committed config ----------------- */
static void t_ring_follows_committed_config(void) {
    printf("\n[C] RAFT-06: committed REMOVE evicts data ownership; gossip cannot re-seat it\n");
    const char *dir = "/tmp/tsdb-raft-live-c";
    rm_rf(dir);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(42ULL, "127.0.0.1:39331",
                               "127.0.0.1:39330", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 42ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    assert(wait_for_leader(r, 4000));
    usleep(600 * 1000);                    /* SEED + NOOP commit */
    assert(ring_has(mgr, 42ULL));
    assert(!ring_has(mgr, 77ULL));

    /* Gossip discovers node 77 → it becomes a data owner. */
    gossip_master(mgr, 77ULL, "127.0.0.1:39996", 1);
    assert(ring_has(mgr, 77ULL));

    /* Operator removes it through Raft.  Once the REMOVE commits, 77
     * must stop owning shards even though gossip still says ALIVE. */
    int rrc = TSDB_ERR_BUSY;
    int64_t give_up = now_ms() + 4000;
    while (now_ms() < give_up) {
        rrc = tsdb_raft_remove_master(r, 77ULL, 1000);
        if (rrc != TSDB_ERR_BUSY) break;
        usleep(50 * 1000);
    }
    printf("  remove_master(77) rc=%d\n", rrc);
    assert(rrc == TSDB_OK);
    int in_ring = ring_has(mgr, 77ULL);
    printf("  ring_has(77) after committed REMOVE: %d\n", in_ring);
    assert(!in_ring);                 /* ownership follows committed config */

    /* Gossip keeps seeing it ALIVE — a fresh upsert and an alive-mark
     * must NOT re-seat a removed member as an owner. */
    gossip_master(mgr, 77ULL, "127.0.0.1:39996", 50);
    tsdb_node_manager_alive(mgr, 77ULL, 60);
    assert(!ring_has(mgr, 77ULL));

    /* A committed ADD re-admits it (the coupling is two-way, keyed on
     * the committed config, not a one-way ban). */
    int arc = TSDB_ERR_BUSY;
    give_up = now_ms() + 4000;
    while (now_ms() < give_up) {
        arc = tsdb_raft_add_master(r, 77ULL, "127.0.0.1:39996", 1000);
        if (arc != TSDB_ERR_BUSY) break;
        usleep(50 * 1000);
    }
    printf("  re-add_master(77) rc=%d ring_has=%d\n", arc, ring_has(mgr, 77ULL));
    assert(arc == TSDB_OK);
    assert(ring_has(mgr, 77ULL));

    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: hashring tracks committed membership, not bare gossip\n");
}

/* ---- [D] RAFT-04: replication must not block on a slow peer ------------- */

/* A TCP endpoint that accepts connections (via the listen backlog) but
 * never reads or writes: connect() succeeds on loopback, the AE frame
 * lands in the socket buffer, and the reply never comes — the RPC call
 * burns its full receive deadline (3 s for AppendEntries). */
static int open_silent_listener(uint16_t *out_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0 ||
        listen(fd, 16) != 0) {
        close(fd);
        return -1;
    }
    socklen_t sl = sizeof(sa);
    if (getsockname(fd, (struct sockaddr *)&sa, &sl) != 0) {
        close(fd);
        return -1;
    }
    *out_port = ntohs(sa.sin_port);
    return fd;
}

static void t_propose_not_blocked_by_slow_peer(void) {
    printf("\n[D] RAFT-04: propose returns on its own deadline despite a stalled peer\n");
    const char *dir = "/tmp/tsdb-raft-live-d";
    rm_rf(dir);

    uint16_t port = 0;
    int lfd = open_silent_listener(&port);
    assert(lfd >= 0);
    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%u", (unsigned)port);
    printf("  silent peer at %s\n", addr);

    tsdb_node_manager_t *mgr =
        tsdb_node_manager_new(42ULL, "127.0.0.1:39341",
                               "127.0.0.1:39340", TSDB_ROLE_MASTER);
    assert(mgr);
    tsdb_replica_mgr_t *rmgr = tsdb_replica_mgr_new(mgr);
    assert(rmgr);
    tsdb_raft_t *r = tsdb_raft_open(dir, 42ULL, mgr, rmgr, NULL, NULL);
    assert(r);

    assert(wait_for_leader(r, 4000));
    usleep(600 * 1000);                    /* SEED + NOOP commit */

    /* Make the silent endpoint a committed member (explicit operator
     * add), so the leader replicates to it and quorum becomes 2. */
    gossip_master(mgr, 88ULL, addr, 1);
    int arc = TSDB_ERR_BUSY;
    int64_t give_up = now_ms() + 4000;
    while (now_ms() < give_up) {
        arc = tsdb_raft_add_master(r, 88ULL, addr, 2000);
        if (arc != TSDB_ERR_BUSY) break;
        usleep(50 * 1000);
    }
    printf("  add_master(88) rc=%d\n", arc);
    give_up = now_ms() + 3000;
    while (now_ms() < give_up && !config_has(r, 88ULL))
        usleep(20 * 1000);
    assert(config_has(r, 88ULL));

    /* Let the heartbeat sweep engage the stalled peer. */
    usleep(300 * 1000);

    /* The commit deadline is 300 ms and can never be met (quorum 2,
     * peer silent).  The call must come back on THAT deadline; the old
     * serial replicate path first sat out the peer's full 3 s
     * AppendEntries timeout on this very thread. */
    int64_t t0 = now_ms();
    int prc = tsdb_raft_propose(r, TSDB_RAFT_ENTRY_NOOP, "x", 1, 300);
    int64_t dt = now_ms() - t0;
    printf("  propose rc=%d elapsed=%lld ms (deadline 300)\n",
           prc, (long long)dt);
    assert(prc == TSDB_ERR_INDETERMINATE || prc == TSDB_ERR_PERMISSION);
    assert(dt < 2000);   /* pre-fix: >= 3000 (one full AE wire timeout) */

    close(lfd);
    tsdb_raft_close(r);
    tsdb_node_manager_free(mgr);
    tsdb_replica_mgr_free(rmgr);
    rm_rf(dir);
    printf("PASS: slow peer cannot hold the propose path hostage\n");
}

int main(int argc, char **argv) {
    /* Optional selector (A/B/C/D) so one case can be reproduced alone. */
    const char *only = argc > 1 ? argv[1] : NULL;
    if (!only || strcmp(only, "A") == 0) t_config_change_gated_on_noop();
    if (!only || strcmp(only, "B") == 0) t_no_autopromote_uncaught_master();
    if (!only || strcmp(only, "C") == 0) t_ring_follows_committed_config();
    if (!only || strcmp(only, "D") == 0) t_propose_not_blocked_by_slow_peer();
    printf("\n=== all raft liveness/membership tests passed ===\n");
    return 0;
}
