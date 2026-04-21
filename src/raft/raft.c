/* raft.c — Raft state machine (election + log replication).
 *
 * Single mutex (`r->lock`) protects every field of the state.  All
 * RPC payload handling, tick events, and proposals acquire the lock
 * first; the design deliberately trades contention for easy
 * correctness review.
 *
 * Outgoing RPCs (RequestVote, AppendEntries) are issued BY THE TICK
 * THREAD while briefly holding no lock — the tick pulls the state
 * slice it needs, drops the lock, calls the peer, then reacquires
 * the lock to merge the response.  This keeps any one slow peer from
 * stalling the whole state machine.
 *
 * Timers:
 *  - Election timeout is randomized in [electionMin_ms, electionMax_ms)
 *    to avoid split votes.  Reset every time we hear a valid leader.
 *  - Heartbeat timer fires at heartbeat_ms while leader.
 *
 * Omitted from this first pass (tracked as follow-ups):
 *  - snapshots + log compaction
 *  - membership change (add/remove master)
 *  - leader lease / read-index optimisation (reads still go through
 *    the local tsdb_query path, which is fine for catalog semantics)
 */

#include "raft.h"
#include "raft_rpc.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../cluster/rpc.h"
#include "../../include/tsdb.h"

/* --- Tunables --------------------------------------------------------- */

/* Election timeout window (ms).  Randomised per-node to avoid split
 * votes.  Must be comfortably larger than heartbeat_ms × 3. */
static const int ELECTION_MIN_MS = 150;
static const int ELECTION_MAX_MS = 300;
/* Heartbeat interval (ms) while leader. */
static const int HEARTBEAT_MS    = 50;

/* Max masters we track at once.  Catalog consensus clusters are
 * almost always 3 or 5 nodes — picking something well above typical
 * without bloating the state. */
#define MAX_PEERS 31

/* --- Forward decls ---------------------------------------------------- */

typedef struct {
    uint64_t id;
    /* Per-peer replication state (leader only).  For followers we
     * still track id so the leader has a stable index.  next_index
     * starts at last_log_index+1 after winning; shrinks on mismatch. */
    uint64_t next_index;
    uint64_t match_index;
} peer_t;

struct tsdb_raft {
    pthread_mutex_t   lock;
    pthread_cond_t    commit_cv;   /* wake proposer on commit advance */

    uint64_t          self_id;
    tsdb_node_manager_t *node_mgr;
    tsdb_replica_mgr_t  *replica_mgr;
    tsdb_raft_log_t     *log;

    /* Volatile Raft state. */
    tsdb_raft_state_t  state;
    uint64_t           leader_id;
    uint64_t           commit_index;
    uint64_t           last_applied;

    /* Volatile candidate/election state. */
    int                votes_granted;    /* #votes received this election */

    /* Peer table (rebuilt from node_mgr each tick) */
    peer_t             peers[MAX_PEERS];
    int                npeers;

    /* Timers, all measured against CLOCK_MONOTONIC. */
    int64_t            election_deadline_ns;
    int64_t            next_heartbeat_ns;
    int                election_timeout_ms; /* rolled per election */
    int64_t            startup_grace_until_ns; /* no elections before this */

    /* Apply callback. */
    tsdb_raft_apply_fn apply_fn;
    void              *apply_ud;

    /* Tick thread control. */
    pthread_t          tick_thread;
    pthread_t          apply_thread;
    int                tick_running;

    /* RNG for election timeout randomisation. */
    unsigned int       rng_seed;
};

/* Max entries packed into one AppendEntries call.  Keeps the RPC
 * bounded while leaving room for burst replication after a leader
 * change. */
#define MAX_ENTRIES_PER_AE 64

/* --- Time helpers ----------------------------------------------------- */

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int64_t ms_to_ns(int ms) { return (int64_t)ms * 1000000LL; }

/* Roll a fresh random election timeout and set the deadline. */
static void reset_election_timer(tsdb_raft_t *r) {
    int span = ELECTION_MAX_MS - ELECTION_MIN_MS;
    r->election_timeout_ms =
        ELECTION_MIN_MS + (int)(rand_r(&r->rng_seed) % (unsigned)span);
    r->election_deadline_ns = now_ns() + ms_to_ns(r->election_timeout_ms);
}

/* Startup grace period before the tick thread will consider an
 * election.  SWIM-lite gossip takes a moment to propagate master
 * roles across a fresh cluster; if we fire an election before peers
 * are visible, we self-elect against a quorum of 1 and every node
 * ends up thinking it's leader. */
static const int RAFT_STARTUP_GRACE_MS = 1500;

static void bump_heartbeat_deadline(tsdb_raft_t *r) {
    r->next_heartbeat_ns = now_ns() + ms_to_ns(HEARTBEAT_MS);
}

/* --- Peer table ------------------------------------------------------- */

/* Rebuild peer[] from node_mgr: every ALIVE or SUSPECT master except
 * self.  Called under r->lock. */
static void rebuild_peers_locked(tsdb_raft_t *r) {
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(r->node_mgr, snap, TSDB_CLUSTER_MAX_NODES);
    int k = 0;
    for (int i = 0; i < n && k < MAX_PEERS; i++) {
        if (snap[i].id == r->self_id) continue;
        if (snap[i].role != TSDB_ROLE_MASTER) continue;
        if (snap[i].state == TSDB_NODE_DEAD) continue;
        /* Already in table? keep next_index / match_index. */
        int found = -1;
        for (int j = 0; j < r->npeers; j++) {
            if (r->peers[j].id == snap[i].id) { found = j; break; }
        }
        if (found >= 0) {
            r->peers[k++] = r->peers[found];
        } else {
            r->peers[k].id          = snap[i].id;
            r->peers[k].next_index  = tsdb_raft_log_last_index(r->log) + 1;
            r->peers[k].match_index = 0;
            k++;
        }
    }
    r->npeers = k;
}

/* Quorum across (peers + self).  Simple majority. */
static int quorum_needed(const tsdb_raft_t *r) {
    int total = r->npeers + 1; /* include self */
    return total / 2 + 1;
}

/* --- State transitions (all under r->lock) --------------------------- */

static void become_follower_locked(tsdb_raft_t *r, uint64_t new_term,
                                    uint64_t leader_id)
{
    uint64_t cur = tsdb_raft_log_current_term(r->log);
    if (new_term > cur) {
        (void)tsdb_raft_log_set_term(r->log, new_term);
    }
    r->state     = TSDB_RAFT_FOLLOWER;
    r->leader_id = leader_id;
    reset_election_timer(r);
}

static void become_candidate_locked(tsdb_raft_t *r) {
    uint64_t new_term = tsdb_raft_log_current_term(r->log) + 1;
    (void)tsdb_raft_log_set_term(r->log, new_term);
    (void)tsdb_raft_log_set_voted_for(r->log, r->self_id);
    r->state         = TSDB_RAFT_CANDIDATE;
    r->leader_id     = 0;
    r->votes_granted = 1; /* vote for self */
    reset_election_timer(r);
}

static void become_leader_locked(tsdb_raft_t *r) {
    r->state     = TSDB_RAFT_LEADER;
    r->leader_id = r->self_id;
    /* Reset per-peer replication state. */
    uint64_t last = tsdb_raft_log_last_index(r->log);
    for (int i = 0; i < r->npeers; i++) {
        r->peers[i].next_index  = last + 1;
        r->peers[i].match_index = 0;
    }
    bump_heartbeat_deadline(r);
}

/* --- RPC handlers (called from rpc.c via the dispatcher) ------------- */

static int on_request_vote(void *ud,
                            const tsdb_raft_req_vote_t *req,
                            tsdb_raft_resp_vote_t *resp)
{
    tsdb_raft_t *r = (tsdb_raft_t *)ud;
    if (!r) return -1;
    pthread_mutex_lock(&r->lock);

    uint64_t cur_term  = tsdb_raft_log_current_term(r->log);
    uint64_t voted_for = tsdb_raft_log_voted_for(r->log);

    /* Reject stale terms. */
    if (req->term < cur_term) {
        resp->term         = cur_term;
        resp->vote_granted = 0;
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    /* Higher term → step down + forget prior vote. */
    if (req->term > cur_term) {
        become_follower_locked(r, req->term, 0);
        cur_term  = req->term;
        voted_for = tsdb_raft_log_voted_for(r->log); /* now 0 */
    }

    /* Grant vote iff we haven't voted (or voted for same candidate)
     * AND the candidate's log is at least as up-to-date (§5.4.1). */
    int log_ok = 0;
    uint64_t our_last_idx  = tsdb_raft_log_last_index(r->log);
    uint64_t our_last_term = tsdb_raft_log_last_term(r->log);
    if (req->last_log_term > our_last_term) log_ok = 1;
    else if (req->last_log_term == our_last_term &&
             req->last_log_index >= our_last_idx) log_ok = 1;

    int granted = 0;
    if (log_ok && (voted_for == 0 || voted_for == req->candidate_id)) {
        (void)tsdb_raft_log_set_voted_for(r->log, req->candidate_id);
        reset_election_timer(r);
        granted = 1;
    }

    resp->term         = tsdb_raft_log_current_term(r->log);
    resp->vote_granted = (uint8_t)granted;
    pthread_mutex_unlock(&r->lock);
    return 0;
}

static int on_append_entries(void *ud,
                              const tsdb_raft_req_append_t *req,
                              tsdb_raft_resp_append_t *resp)
{
    tsdb_raft_t *r = (tsdb_raft_t *)ud;
    if (!r) return -1;
    pthread_mutex_lock(&r->lock);

    uint64_t cur_term = tsdb_raft_log_current_term(r->log);

    /* Reply false if term < currentTerm. */
    if (req->term < cur_term) {
        resp->term        = cur_term;
        resp->success     = 0;
        resp->match_index = 0;
        pthread_mutex_unlock(&r->lock);
        return 0;
    }

    /* Any valid AppendEntries resets our election timer — we heard
     * from the leader, so don't start an election for another round. */
    if (req->term > cur_term) {
        become_follower_locked(r, req->term, req->leader_id);
        cur_term = req->term;
    } else {
        /* Same term — whoever is sending AppendEntries is the leader.
         * If we were a candidate, step down. */
        if (r->state == TSDB_RAFT_CANDIDATE) {
            r->state = TSDB_RAFT_FOLLOWER;
        }
        r->leader_id = req->leader_id;
        reset_election_timer(r);
    }

    /* Log matching: if prev_log_index > 0, the entry at that index
     * must exist with the right term. */
    if (req->prev_log_index > 0) {
        uint64_t our_last = tsdb_raft_log_last_index(r->log);
        if (req->prev_log_index > our_last) {
            resp->term        = cur_term;
            resp->success     = 0;
            resp->match_index = our_last;
            pthread_mutex_unlock(&r->lock);
            return 0;
        }
        uint64_t our_term = tsdb_raft_log_term_at(r->log, req->prev_log_index);
        if (our_term != req->prev_log_term) {
            /* Conflict — truncate from prev_log_index (inclusive). */
            (void)tsdb_raft_log_truncate(r->log, req->prev_log_index);
            resp->term        = cur_term;
            resp->success     = 0;
            resp->match_index = req->prev_log_index - 1;
            pthread_mutex_unlock(&r->lock);
            return 0;
        }
    }

    /* Append new entries starting at prev_log_index + 1.  Truncate
     * any existing entries that conflict (same index, different term). */
    uint64_t insert_at = req->prev_log_index + 1;
    for (uint32_t i = 0; i < req->n_entries; i++) {
        const tsdb_raft_entry_t *e = &req->entries[i];
        uint64_t our_last = tsdb_raft_log_last_index(r->log);
        if (e->index <= our_last) {
            uint64_t our_term = tsdb_raft_log_term_at(r->log, e->index);
            if (our_term == e->term) continue; /* already have it */
            /* Conflict — truncate from this index. */
            (void)tsdb_raft_log_truncate(r->log, e->index);
        }
        tsdb_raft_entry_t copy = *e;
        if (tsdb_raft_log_append(r->log, &copy) != 0) {
            resp->term        = cur_term;
            resp->success     = 0;
            resp->match_index = tsdb_raft_log_last_index(r->log);
            pthread_mutex_unlock(&r->lock);
            return 0;
        }
        (void)insert_at;
    }

    /* Advance commit index.  leader_commit may be ahead of our log;
     * cap at our last_index. */
    if (req->leader_commit > r->commit_index) {
        uint64_t our_last = tsdb_raft_log_last_index(r->log);
        uint64_t new_commit = req->leader_commit < our_last
                                 ? req->leader_commit
                                 : our_last;
        if (new_commit > r->commit_index) {
            r->commit_index = new_commit;
            pthread_cond_broadcast(&r->commit_cv);
        }
    }

    resp->term        = cur_term;
    resp->success     = 1;
    resp->match_index = tsdb_raft_log_last_index(r->log);
    pthread_mutex_unlock(&r->lock);
    return 0;
}

/* --- Outgoing election: send RequestVote to every peer -------------- */

/* Send RequestVote to peer_id and merge the response.  May cause us
 * to step down if the peer has a higher term.  Returns 1 if the peer
 * granted us a vote, 0 otherwise. */
static int send_request_vote(tsdb_raft_t *r, uint64_t peer_id,
                              uint64_t term, uint64_t last_idx, uint64_t last_term)
{
    tsdb_raft_req_vote_t req = {
        .term = term, .candidate_id = r->self_id,
        .last_log_index = last_idx, .last_log_term = last_term
    };
    uint8_t req_buf[32];
    int rn = tsdb_raft_encode_req_vote(req_buf, sizeof(req_buf), &req);
    if (rn <= 0) return 0;

    /* Grab a connection from the replica pool (shared with data path). */
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(r->replica_mgr, peer_id);
    if (!conn) return 0;

    uint8_t resp_buf[32];
    uint32_t resp_len = 0;
    int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_RAFT_REQUEST_VOTE,
                                 req_buf, (uint32_t)rn,
                                 resp_buf, sizeof(resp_buf), &resp_len);
    if (rc != TSDB_OK) return 0;

    tsdb_raft_resp_vote_t resp = {0};
    if (tsdb_raft_decode_resp_vote(resp_buf, resp_len, &resp) != 0) return 0;

    pthread_mutex_lock(&r->lock);
    if (resp.term > tsdb_raft_log_current_term(r->log)) {
        become_follower_locked(r, resp.term, 0);
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    pthread_mutex_unlock(&r->lock);
    return resp.vote_granted ? 1 : 0;
}

/* Run an election: send RequestVote to each known master in parallel.
 * Keep it simple: serial for now — with 3-5 masters the wire cost is
 * microseconds; fan-out lets a slow peer block nobody.
 *
 * Called by the tick thread with r->lock already held.  Drops the
 * lock while the RPCs fly and reacquires before touching state. */
static void run_election_unlocked(tsdb_raft_t *r) {
    /* Take a snapshot of what we need for RequestVote. */
    pthread_mutex_lock(&r->lock);
    uint64_t term     = tsdb_raft_log_current_term(r->log);
    uint64_t last_idx = tsdb_raft_log_last_index(r->log);
    uint64_t last_trm = tsdb_raft_log_last_term(r->log);
    uint64_t peers[MAX_PEERS];
    int np = r->npeers;
    for (int i = 0; i < np; i++) peers[i] = r->peers[i].id;
    pthread_mutex_unlock(&r->lock);

    int granted = 1; /* self-vote already counted */
    for (int i = 0; i < np; i++) {
        granted += send_request_vote(r, peers[i], term, last_idx, last_trm);
    }

    pthread_mutex_lock(&r->lock);
    /* Still candidate AND same term? Count the votes. */
    if (r->state == TSDB_RAFT_CANDIDATE &&
        tsdb_raft_log_current_term(r->log) == term &&
        granted >= quorum_needed(r))
    {
        become_leader_locked(r);
    }
    pthread_mutex_unlock(&r->lock);
}

/* --- Commit advance (leader only) ------------------------------------
 *
 * After any matchIndex update, sweep the range [commit_index+1,
 * last_index] to see whether a majority of matchIndex[i] has caught
 * up.  Raft §5.4.2: only commit entries whose term == currentTerm.
 * Called under r->lock. */
static void maybe_advance_commit_locked(tsdb_raft_t *r) {
    if (r->state != TSDB_RAFT_LEADER) return;
    uint64_t last    = tsdb_raft_log_last_index(r->log);
    uint64_t current = tsdb_raft_log_current_term(r->log);
    int quorum = quorum_needed(r);

    for (uint64_t N = last; N > r->commit_index; N--) {
        if (tsdb_raft_log_term_at(r->log, N) != current) continue;
        int count = 1; /* self */
        for (int i = 0; i < r->npeers; i++) {
            if (r->peers[i].match_index >= N) count++;
        }
        if (count >= quorum) {
            r->commit_index = N;
            pthread_cond_broadcast(&r->commit_cv);
            return;
        }
    }
}

/* --- Replication (leader only) ---------------------------------------
 *
 * One AppendEntries sweep to peer_id.  Packs up to MAX_ENTRIES_PER_AE
 * entries starting at the peer's next_index.  An empty call (peer is
 * fully caught up) doubles as a heartbeat.
 *
 * Success → bumps peer.match_index and kicks the commit-advance check.
 * Term-conflict failure → rewinds peer.next_index using the peer's
 * match_index hint so the next AE re-tries from a safer point. */
static void replicate_to(tsdb_raft_t *r, uint64_t peer_id) {
    /* Snapshot the state we need while holding the lock, then drop
     * it for the blocking RPC.  Entries are copied into a local
     * buffer so the peer's log can race freely. */
    pthread_mutex_lock(&r->lock);
    if (r->state != TSDB_RAFT_LEADER) {
        pthread_mutex_unlock(&r->lock);
        return;
    }
    int peer_idx = -1;
    for (int i = 0; i < r->npeers; i++) {
        if (r->peers[i].id == peer_id) { peer_idx = i; break; }
    }
    if (peer_idx < 0) {
        pthread_mutex_unlock(&r->lock);
        return;
    }

    uint64_t term       = tsdb_raft_log_current_term(r->log);
    uint64_t commit     = r->commit_index;
    uint64_t last_idx   = tsdb_raft_log_last_index(r->log);
    uint64_t next_idx   = r->peers[peer_idx].next_index;
    uint64_t prev_idx   = next_idx > 0 ? next_idx - 1 : 0;
    uint64_t prev_term  = prev_idx > 0 ? tsdb_raft_log_term_at(r->log, prev_idx) : 0;

    /* How many entries to pack? */
    uint32_t n_send = 0;
    if (last_idx >= next_idx) {
        uint64_t span = last_idx - next_idx + 1;
        n_send = span > MAX_ENTRIES_PER_AE ? MAX_ENTRIES_PER_AE : (uint32_t)span;
    }

    tsdb_raft_entry_t *entries = NULL;
    if (n_send > 0) {
        entries = calloc(n_send, sizeof(tsdb_raft_entry_t));
        if (!entries) { pthread_mutex_unlock(&r->lock); return; }
        for (uint32_t i = 0; i < n_send; i++) {
            uint64_t idx = next_idx + i;
            tsdb_raft_entry_t tmp = {0};
            if (tsdb_raft_log_read(r->log, idx, &tmp) != 0) {
                for (uint32_t j = 0; j < i; j++) free(entries[j].payload);
                free(entries);
                pthread_mutex_unlock(&r->lock);
                return;
            }
            entries[i] = tmp; /* payload malloc'd by log_read */
        }
    }
    pthread_mutex_unlock(&r->lock);

    tsdb_raft_req_append_t req = {
        .term = term, .leader_id = r->self_id,
        .prev_log_index = prev_idx, .prev_log_term = prev_term,
        .leader_commit  = commit,
        .n_entries = n_send, .entries = entries
    };

    /* Size the RPC buffer generously: header + entries + 1 KB slack. */
    size_t bcap = tsdb_raft_append_buf_cap(n_send, 4096) + 1024;
    uint8_t *req_buf = malloc(bcap);
    if (!req_buf) goto done;
    int rn = tsdb_raft_encode_req_append(req_buf, bcap, &req);
    if (rn <= 0) { free(req_buf); goto done; }

    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(r->replica_mgr, peer_id);
    if (!conn) { free(req_buf); goto done; }

    uint8_t resp_buf[32];
    uint32_t resp_len = 0;
    int rc = tsdb_rpc_call_recv(conn, TSDB_RPC_RAFT_APPEND_ENTRIES,
                                 req_buf, (uint32_t)rn,
                                 resp_buf, sizeof(resp_buf), &resp_len);
    free(req_buf);
    if (rc != TSDB_OK) goto done;

    tsdb_raft_resp_append_t resp = {0};
    if (tsdb_raft_decode_resp_append(resp_buf, resp_len, &resp) != 0) goto done;

    pthread_mutex_lock(&r->lock);
    if (resp.term > tsdb_raft_log_current_term(r->log)) {
        become_follower_locked(r, resp.term, 0);
        pthread_mutex_unlock(&r->lock);
        goto done;
    }
    /* Re-find the peer in case rebuild_peers shuffled the table. */
    int new_idx = -1;
    for (int i = 0; i < r->npeers; i++) {
        if (r->peers[i].id == peer_id) { new_idx = i; break; }
    }
    if (new_idx < 0 || r->state != TSDB_RAFT_LEADER) {
        pthread_mutex_unlock(&r->lock);
        goto done;
    }
    if (resp.success) {
        uint64_t new_match = prev_idx + n_send;
        if (new_match > r->peers[new_idx].match_index)
            r->peers[new_idx].match_index = new_match;
        r->peers[new_idx].next_index = new_match + 1;
        maybe_advance_commit_locked(r);
    } else {
        /* Roll back: follower's hint is its last matching index. */
        uint64_t hint = resp.match_index;
        if (hint + 1 < r->peers[new_idx].next_index)
            r->peers[new_idx].next_index = hint + 1;
        else if (r->peers[new_idx].next_index > 1)
            r->peers[new_idx].next_index--;
    }
    pthread_mutex_unlock(&r->lock);

done:
    if (entries) {
        for (uint32_t i = 0; i < n_send; i++) free(entries[i].payload);
        free(entries);
    }
}

static void replicate_all(tsdb_raft_t *r) {
    pthread_mutex_lock(&r->lock);
    uint64_t peers[MAX_PEERS];
    int np = r->npeers;
    for (int i = 0; i < np; i++) peers[i] = r->peers[i].id;
    pthread_mutex_unlock(&r->lock);
    for (int i = 0; i < np; i++) replicate_to(r, peers[i]);
}

/* --- Tick thread ----------------------------------------------------- */

static void *tick_thread_main(void *arg) {
    tsdb_raft_t *r = (tsdb_raft_t *)arg;
    while (r->tick_running) {
        /* Rebuild peer set from the latest gossip view. */
        pthread_mutex_lock(&r->lock);
        rebuild_peers_locked(r);
        tsdb_raft_state_t st   = r->state;
        int64_t ed             = r->election_deadline_ns;
        int64_t hb             = r->next_heartbeat_ns;
        int     np             = r->npeers;
        int64_t grace_until    = r->startup_grace_until_ns;
        pthread_mutex_unlock(&r->lock);

        int64_t now = now_ns();
        if (st == TSDB_RAFT_LEADER) {
            if (now >= hb) {
                /* One AppendEntries sweep per peer — carries entries
                 * when the peer is behind, empty payload otherwise
                 * (so it also serves as a heartbeat). */
                replicate_all(r);
                pthread_mutex_lock(&r->lock);
                bump_heartbeat_deadline(r);
                pthread_mutex_unlock(&r->lock);
            }
        } else {
            /* Don't race into an election during the startup grace
             * period, and don't elect ourselves when no peer is known
             * yet — doing so gives us a 1-node quorum and every node
             * trivially wins its own term.  In both cases just reset
             * the timer and hope the next tick sees peers. */
            if (now < grace_until || np == 0) {
                pthread_mutex_lock(&r->lock);
                reset_election_timer(r);
                pthread_mutex_unlock(&r->lock);
            } else if (now >= ed) {
                pthread_mutex_lock(&r->lock);
                become_candidate_locked(r);
                pthread_mutex_unlock(&r->lock);
                run_election_unlocked(r);
            }
        }

        /* Sleep a small tick; 10ms gives us <5% jitter on a 150ms
         * election timeout and is easy on CPU. */
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 10 * 1000000 };
        nanosleep(&ts, NULL);
    }
    return NULL;
}

/* Forward decl — apply thread body is defined below tsdb_raft_propose
 * because it's conceptually part of the propose/apply pipeline.  We
 * only need the symbol up here for pthread_create. */
static void *apply_thread_main(void *arg);

/* --- Public API ----------------------------------------------------- */

tsdb_raft_t *tsdb_raft_open(const char *data_dir,
                             uint64_t local_id,
                             tsdb_node_manager_t *node_mgr,
                             tsdb_replica_mgr_t  *replica_mgr,
                             tsdb_raft_apply_fn   apply_fn,
                             void                *apply_ud)
{
    if (!data_dir || !node_mgr || !replica_mgr) return NULL;
    tsdb_raft_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;
    pthread_mutex_init(&r->lock, NULL);
    pthread_cond_init(&r->commit_cv, NULL);

    r->self_id     = local_id;
    r->node_mgr    = node_mgr;
    r->replica_mgr = replica_mgr;
    r->apply_fn    = apply_fn;
    r->apply_ud    = apply_ud;
    r->state       = TSDB_RAFT_FOLLOWER;
    r->rng_seed    = (unsigned)(local_id ^ (uint64_t)now_ns());

    r->log = tsdb_raft_log_open(data_dir);
    if (!r->log) { free(r); return NULL; }

    r->startup_grace_until_ns = now_ns() + ms_to_ns(RAFT_STARTUP_GRACE_MS);
    reset_election_timer(r);
    /* Push the first election deadline past the grace window so an
     * unusually short random timeout can't sneak an election in before
     * gossip has had time to run. */
    if (r->election_deadline_ns < r->startup_grace_until_ns) {
        r->election_deadline_ns = r->startup_grace_until_ns;
    }

    /* Register RPC handlers before we start ticking, so a peer's
     * RequestVote on a cold cluster always lands in our state machine. */
    tsdb_raft_rpc_set_handlers(on_request_vote, on_append_entries, r);

    r->tick_running = 1;
    if (pthread_create(&r->tick_thread, NULL, tick_thread_main, r) != 0) {
        tsdb_raft_log_close(r->log);
        free(r);
        return NULL;
    }
    if (pthread_create(&r->apply_thread, NULL, apply_thread_main, r) != 0) {
        r->tick_running = 0;
        pthread_join(r->tick_thread, NULL);
        tsdb_raft_log_close(r->log);
        free(r);
        return NULL;
    }
    return r;
}

void tsdb_raft_close(tsdb_raft_t *r) {
    if (!r) return;
    pthread_mutex_lock(&r->lock);
    r->tick_running = 0;
    pthread_cond_broadcast(&r->commit_cv); /* unstick apply thread */
    pthread_mutex_unlock(&r->lock);
    pthread_join(r->tick_thread, NULL);
    pthread_join(r->apply_thread, NULL);
    tsdb_raft_rpc_set_handlers(NULL, NULL, NULL);
    tsdb_raft_log_close(r->log);
    pthread_cond_destroy(&r->commit_cv);
    pthread_mutex_destroy(&r->lock);
    free(r);
}

uint64_t tsdb_raft_self_id(tsdb_raft_t *r)      { return r ? r->self_id : 0; }

tsdb_raft_state_t tsdb_raft_state(tsdb_raft_t *r) {
    if (!r) return TSDB_RAFT_FOLLOWER;
    pthread_mutex_lock(&r->lock);
    tsdb_raft_state_t s = r->state;
    pthread_mutex_unlock(&r->lock);
    return s;
}

uint64_t tsdb_raft_current_term(tsdb_raft_t *r) {
    return r ? tsdb_raft_log_current_term(r->log) : 0;
}
uint64_t tsdb_raft_leader_id(tsdb_raft_t *r) {
    if (!r) return 0;
    pthread_mutex_lock(&r->lock);
    uint64_t id = r->leader_id;
    pthread_mutex_unlock(&r->lock);
    return id;
}
uint64_t tsdb_raft_commit_index(tsdb_raft_t *r) {
    if (!r) return 0;
    pthread_mutex_lock(&r->lock);
    uint64_t c = r->commit_index;
    pthread_mutex_unlock(&r->lock);
    return c;
}
uint64_t tsdb_raft_last_applied(tsdb_raft_t *r) {
    if (!r) return 0;
    pthread_mutex_lock(&r->lock);
    uint64_t la = r->last_applied;
    pthread_mutex_unlock(&r->lock);
    return la;
}
uint64_t tsdb_raft_last_index(tsdb_raft_t *r) {
    return r ? tsdb_raft_log_last_index(r->log) : 0;
}

/* --- Apply thread -----------------------------------------------------
 *
 * Drains committed-but-unapplied entries into the state machine via
 * apply_fn.  Decoupled from the RPC path so a slow apply (e.g. a big
 * catalog log replay) can't stall heartbeats. */
static void *apply_thread_main(void *arg) {
    tsdb_raft_t *r = (tsdb_raft_t *)arg;
    while (r->tick_running) {
        pthread_mutex_lock(&r->lock);
        while (r->tick_running && r->last_applied >= r->commit_index) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;  /* wake up at least every second to check
                                tick_running during shutdown */
            pthread_cond_timedwait(&r->commit_cv, &r->lock, &ts);
        }
        if (!r->tick_running) { pthread_mutex_unlock(&r->lock); break; }
        uint64_t to_apply = r->last_applied + 1;
        uint64_t commit   = r->commit_index;
        pthread_mutex_unlock(&r->lock);

        for (uint64_t idx = to_apply; idx <= commit; idx++) {
            tsdb_raft_entry_t e = {0};
            if (tsdb_raft_log_read(r->log, idx, &e) != 0) break;
            if (r->apply_fn) {
                (void)r->apply_fn(r->apply_ud, &e);
            }
            free(e.payload);
            pthread_mutex_lock(&r->lock);
            r->last_applied = idx;
            pthread_cond_broadcast(&r->commit_cv); /* wake proposers */
            pthread_mutex_unlock(&r->lock);
        }
    }
    return NULL;
}

/* --- Propose --------------------------------------------------------
 *
 * Append locally, then wait for commit_index to catch up.  We reuse
 * the tick thread's replicate_all() sweep to push the entry to peers
 * (fires every HEARTBEAT_MS ≈ 50 ms).  For a tighter RTT the caller
 * could kick a condvar that speeds up the next sweep — left as a
 * future optimisation; current latency is bounded by heartbeat_ms. */
int tsdb_raft_propose(tsdb_raft_t *r,
                       tsdb_raft_entry_type_t type,
                       const void *payload, uint32_t payload_len,
                       int timeout_ms)
{
    if (!r) return TSDB_ERR_INVAL;

    pthread_mutex_lock(&r->lock);
    if (r->state != TSDB_RAFT_LEADER) {
        pthread_mutex_unlock(&r->lock);
        return TSDB_ERR_PERMISSION;
    }

    /* Append entry at (last_index + 1) in the current term. */
    tsdb_raft_entry_t entry = {
        .term        = tsdb_raft_log_current_term(r->log),
        .type        = (uint32_t)type,
        .payload_len = payload_len,
        .payload     = (void *)payload, /* log.append copies */
    };
    if (tsdb_raft_log_append(r->log, &entry) != 0) {
        pthread_mutex_unlock(&r->lock);
        return TSDB_ERR_IO;
    }
    uint64_t my_index = entry.index;
    pthread_mutex_unlock(&r->lock);

    /* Kick the replicate sweep right away so we don't wait a full
     * heartbeat_ms for propagation. */
    replicate_all(r);

    /* Wait until commit_index catches up (and apply too, so the caller
     * sees the state machine effect of their write). */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    int tmo = timeout_ms > 0 ? timeout_ms : 5000;
    deadline.tv_sec  += tmo / 1000;
    deadline.tv_nsec += (long)(tmo % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec  += 1;
        deadline.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&r->lock);
    int rc = TSDB_OK;
    while (r->last_applied < my_index) {
        if (r->state != TSDB_RAFT_LEADER) { rc = TSDB_ERR_PERMISSION; break; }
        int w = pthread_cond_timedwait(&r->commit_cv, &r->lock, &deadline);
        if (w == ETIMEDOUT) { rc = TSDB_ERR_IO; break; }
    }
    pthread_mutex_unlock(&r->lock);
    return rc;
}
