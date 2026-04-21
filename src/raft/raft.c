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
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

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

    char              data_dir[4096]; /* retained for chunked snapshot
                                          staging under raft/snapshot/ */
    uint64_t          self_id;
    tsdb_node_manager_t *node_mgr;
    tsdb_replica_mgr_t  *replica_mgr;
    tsdb_raft_log_t     *log;
    tsdb_raft_config_t  *config;      /* persistent master set */

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

    /* Snapshot callbacks (optional). */
    tsdb_raft_snapshot_write_fn   snap_write_fn;
    tsdb_raft_snapshot_restore_fn snap_restore_fn;
    void                         *snap_ud;

    /* Tick thread control. */
    pthread_t          tick_thread;
    pthread_t          apply_thread;
    int                tick_running;

    /* RNG for election timeout randomisation. */
    unsigned int       rng_seed;

    /* Membership-bootstrap plumbing.  When we win leadership on a
     * cluster whose config.bin hasn't been initialised yet, we set
     * `pending_seed = 1` under the lock.  The tick thread picks it up,
     * drops the lock, and proposes a single TSDB_RAFT_CFG_OP_SEED log
     * entry containing the current ALIVE-master snapshot.  That entry
     * replicates to every peer via normal log replay; on commit the
     * apply thread calls tsdb_raft_config_set() everywhere, so LIST
     * MASTERS converges cluster-wide.  pending_seed is cleared once
     * a propose call completes (success OR stale-leader failure) so
     * we don't spin proposing the same entry. */
    int                pending_seed;
};

/* Max entries packed into one AppendEntries call.  Keeps the RPC
 * bounded while leaving room for burst replication after a leader
 * change. */
#define MAX_ENTRIES_PER_AE 64

/* Upper bound on the InstallSnapshot request header.  Matches the
 * codec layout in raft_rpc.c (INSTALL_HDR_SIZE = 41) with a small
 * safety margin so the send buffer never truncates an encoded chunk
 * header even if a future field is bolted on. */
#define INSTALL_HDR_MAX 128u

/* Auto-compaction threshold.  Once the apply thread has applied this
 * many entries past the current snapshot boundary, we discard the
 * subsumed prefix so the log doesn't grow without bound.  On a
 * three-master cluster issuing DDL every few seconds this produces
 * one compaction every ~15 s in the worst case, which is fine.
 *
 * Trade-off: a follower that falls more than SNAP_COMPACT_STRIDE
 * entries behind can no longer catch up from the tail alone; it needs
 * InstallSnapshot (tracked as a follow-up).  For a stable 3-master
 * set this is rare, but the threshold is deliberately generous. */
#define SNAP_COMPACT_STRIDE 256

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
 * self.  This MVP keeps using gossip for both peer discovery AND
 * quorum — the persisted r->config is advisory only (populated by
 * ADD MASTER / REMOVE MASTER log entries for audit + LIST MASTERS).
 * Swapping to a config-gated quorum is the "real" Raft membership
 * change and is tracked as a follow-up; it needs PreVote +
 * stepdown-on-self-removal + joint consensus handling. */
static void rebuild_peers_locked(tsdb_raft_t *r) {
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(r->node_mgr, snap, TSDB_CLUSTER_MAX_NODES);
    int k = 0;
    for (int i = 0; i < n && k < MAX_PEERS; i++) {
        if (snap[i].id == r->self_id) continue;
        if (snap[i].role != TSDB_ROLE_MASTER) continue;
        if (snap[i].state == TSDB_NODE_DEAD) continue;
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

static int quorum_needed(const tsdb_raft_t *r) {
    int total = r->npeers + 1;
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
    r->pending_seed = 0;   /* follower doesn't seed; if we win again later
                              and config is still uninit, we'll set it again. */
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

    /* If the config hasn't been initialised yet (fresh cluster, no
     * prior SEED entry in any peer's log), mark that we need to emit
     * one.  The tick thread owns the actual propose call because it
     * needs to drop r->lock before calling tsdb_raft_propose (which
     * re-acquires the lock internally).  See tick_thread_main. */
    if (r->config && !tsdb_raft_config_is_initialised(r->config)) {
        r->pending_seed = 1;
    }
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

/* --- InstallSnapshot handler (follower side) -------------------------
 *
 * The leader streams the body in 64 KB chunks.  We buffer partials to
 *   <data_dir>/raft/snapshot/incoming.bin.tmp
 * and only hand the fully-reassembled buffer to snap_restore_fn when
 * the `done` flag arrives.  Each chunk carries its own byte offset so
 * duplicates (benign retries on RPC timeout) overwrite in place.
 *
 * A mid-transfer crash leaves the tmp file on disk; we nuke it on
 * raft_open so the next transfer re-starts at offset=0 without us
 * having to teach the wire to resume.
 */

/* Build <data_dir>/raft/snapshot/; return 0 on success. */
static int snap_stage_dir(const tsdb_raft_t *r, char *out, size_t cap) {
    int n = snprintf(out, cap, "%s/raft/snapshot", r->data_dir);
    if (n <= 0 || (size_t)n >= cap) return -1;
    /* mkdir -p without pulling in shell utilities.  Ignore EEXIST. */
    char tmp[4200];
    snprintf(tmp, sizeof(tmp), "%s", out);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') { *p = 0; (void)mkdir(tmp, 0755); *p = '/'; }
    }
    (void)mkdir(tmp, 0755);
    return 0;
}

/* Path of the in-progress chunk buffer. */
static int snap_stage_path(const tsdb_raft_t *r, char *out, size_t cap) {
    int n = snprintf(out, cap, "%s/raft/snapshot/incoming.bin.tmp",
                     r->data_dir);
    return (n > 0 && (size_t)n < cap) ? 0 : -1;
}

/* Append `data_len` bytes at byte position `offset` in the stage file.
 * Opens with O_CREAT | O_WRONLY; trusts the caller to do seek+write
 * atomically.  Returns 0 on success. */
static int snap_stage_write(const char *path, uint32_t offset,
                             const uint8_t *data, uint32_t data_len)
{
    int flags = O_WRONLY;
    struct stat st;
    if (offset == 0) {
        /* Fresh transfer — truncate so a prior aborted one doesn't
         * leave stale tail bytes past our final `done` offset. */
        flags |= O_CREAT | O_TRUNC;
    } else {
        flags |= O_CREAT;
    }
    int fd = open(path, flags, 0644);
    if (fd < 0) return -1;
    if (data_len > 0) {
        if (lseek(fd, offset, SEEK_SET) != (off_t)offset) {
            close(fd); return -1;
        }
        ssize_t w = write(fd, data, data_len);
        if (w < 0 || (uint32_t)w != data_len) { close(fd); return -1; }
    }
    /* Keep the file around until done=1; fsync is deferred to the
     * restore handler which does its own atomic-rename on the body. */
    (void)st;
    close(fd);
    return 0;
}

static int on_install_snapshot(void *ud,
                                const tsdb_raft_req_install_t *req,
                                tsdb_raft_resp_install_t *resp)
{
    tsdb_raft_t *r = (tsdb_raft_t *)ud;
    if (!r || !req || !resp) return -1;
    pthread_mutex_lock(&r->lock);

    uint64_t cur_term = tsdb_raft_log_current_term(r->log);

    /* Stale leader? ignore. */
    if (req->term < cur_term) {
        resp->term = cur_term;
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    if (req->term > cur_term) {
        become_follower_locked(r, req->term, req->leader_id);
        cur_term = req->term;
    } else {
        /* Same term — sender is the legit leader; reset election timer. */
        if (r->state == TSDB_RAFT_CANDIDATE) r->state = TSDB_RAFT_FOLLOWER;
        r->leader_id = req->leader_id;
        reset_election_timer(r);
    }

    /* If we've already applied past this snapshot index there's
     * nothing to do — ack with our term. */
    if (req->last_included_index <= tsdb_raft_log_snapshot_index(r->log)) {
        resp->term = cur_term;
        pthread_mutex_unlock(&r->lock);
        return 0;
    }

    /* Drop the lock across any filesystem I/O (stage write or final
     * restore hook).  Re-acquire to mutate log markers at the end. */
    tsdb_raft_snapshot_restore_fn fn = r->snap_restore_fn;
    void *sud = r->snap_ud;
    pthread_mutex_unlock(&r->lock);

    char stage_dir[4300];
    char stage_path[4400];
    if (snap_stage_dir(r, stage_dir, sizeof(stage_dir)) != 0) {
        pthread_mutex_lock(&r->lock);
        resp->term = tsdb_raft_log_current_term(r->log);
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    (void)snap_stage_path(r, stage_path, sizeof(stage_path));

    /* Stage this chunk to disk.  Single-threaded per follower (the RPC
     * server handler fires these in order from one leader connection),
     * so we don't need a lock here. */
    if (snap_stage_write(stage_path, req->offset,
                          req->data, req->data_len) != 0) {
        pthread_mutex_lock(&r->lock);
        resp->term = tsdb_raft_log_current_term(r->log);
        pthread_mutex_unlock(&r->lock);
        return 0;
    }

    if (!req->done) {
        /* More chunks coming.  Ack with our term so the leader keeps
         * going; do NOT advance log markers yet. */
        pthread_mutex_lock(&r->lock);
        resp->term = tsdb_raft_log_current_term(r->log);
        pthread_mutex_unlock(&r->lock);
        return 0;
    }

    /* Final chunk arrived — slurp the whole file in and hand it to the
     * restore hook.  Size = req->offset + req->data_len (computed from
     * the actual chunk headers rather than stat() so a stale tail can't
     * leak into the body). */
    uint32_t body_len = req->offset + req->data_len;
    uint8_t *body = NULL;
    int rrc = 0;
    if (body_len > 0) {
        body = malloc(body_len);
        if (!body) rrc = -1;
        else {
            int bfd = open(stage_path, O_RDONLY);
            if (bfd < 0) { rrc = -1; }
            else {
                uint32_t got = 0;
                while (got < body_len) {
                    ssize_t n = read(bfd, body + got, body_len - got);
                    if (n <= 0) { rrc = -1; break; }
                    got += (uint32_t)n;
                }
                close(bfd);
            }
        }
    }

    if (rrc == 0 && fn) {
        rrc = fn(sud, body, body_len);
    }
    free(body);
    /* Stage file is no longer needed — unlink opportunistically.
     * On failure the leader will retry at offset=0 which truncates. */
    (void)unlink(stage_path);

    pthread_mutex_lock(&r->lock);
    if (rrc == 0) {
        /* Advance log markers so future AEs with prev_log_index ==
         * last_included_index pass the consistency check. */
        (void)tsdb_raft_log_compact(r->log,
                                    req->last_included_index,
                                    req->last_included_term);
        if (r->last_applied < req->last_included_index)
            r->last_applied = req->last_included_index;
        if (r->commit_index < req->last_included_index)
            r->commit_index = req->last_included_index;
    }
    resp->term = tsdb_raft_log_current_term(r->log);
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
    uint64_t snap_idx   = tsdb_raft_log_snapshot_index(r->log);
    uint64_t next_idx   = r->peers[peer_idx].next_index;

    /* Peer is far enough behind that it needs entries we already
     * compacted away.  If we have a snapshot-write hook, stream the
     * body via InstallSnapshot in 64 KB chunks.  Otherwise fall back
     * to the old behaviour: bump next_index forward and let the peer
     * coast on heartbeats (stale catalog, but at least not election
     * storm). */
    if (next_idx <= snap_idx) {
        if (r->snap_write_fn) {
            /* Drop the lock across the (potentially slow) write hook
             * and the wire RPC. */
            uint64_t snap_term = tsdb_raft_log_snapshot_term(r->log);
            tsdb_raft_snapshot_write_fn wfn = r->snap_write_fn;
            void *sud = r->snap_ud;
            uint64_t self_id = r->self_id;
            pthread_mutex_unlock(&r->lock);

            uint8_t *body = NULL;
            uint32_t body_len = 0;
            if (wfn(sud, &body, &body_len) != 0 || !body) {
                goto skip_snap;
            }

            /* Chunked transfer.  64 KB balances per-RPC overhead
             * against peer-side heartbeat starvation (each chunk
             * monopolises the peer's RPC slot until ACK).  An empty
             * body is legitimate on a fresh cluster with a snapshot
             * marker but no catalog content — send a single done=1
             * chunk so the follower still advances its log markers. */
            const uint32_t CHUNK_MAX = 64u * 1024u;
            uint8_t *sbuf = malloc(INSTALL_HDR_MAX + CHUNK_MAX);
            if (!sbuf) { free(body); goto skip_snap; }

            int aborted = 0;
            uint32_t off = 0;
            do {
                uint32_t remain = body_len - off;
                uint32_t take   = remain > CHUNK_MAX ? CHUNK_MAX : remain;
                uint8_t done    = (take == remain);

                tsdb_raft_req_install_t sreq = {
                    .term = term,
                    .leader_id = self_id,
                    .last_included_index = snap_idx,
                    .last_included_term  = snap_term,
                    .offset              = off,
                    .done                = done,
                    .data_len            = take,
                    .data                = (take > 0) ? (body + off) : NULL
                };
                int sn = tsdb_raft_encode_req_install(
                             sbuf, INSTALL_HDR_MAX + take, &sreq);
                if (sn <= 0) { aborted = 1; break; }

                tsdb_rpc_conn_t *conn2 =
                    tsdb_replica_mgr_get_conn(r->replica_mgr, peer_id);
                if (!conn2) { aborted = 1; break; }

                uint8_t sresp_buf[32];
                uint32_t sresp_len = 0;
                int rc2 = tsdb_rpc_call_recv(conn2,
                                              TSDB_RPC_RAFT_INSTALL_SNAPSHOT,
                                              sbuf, (uint32_t)sn,
                                              sresp_buf, sizeof(sresp_buf),
                                              &sresp_len);
                if (rc2 != TSDB_OK) { aborted = 1; break; }

                tsdb_raft_resp_install_t sresp = {0};
                if (tsdb_raft_decode_resp_install(sresp_buf, sresp_len,
                                                   &sresp) != 0) {
                    aborted = 1; break;
                }
                /* Stale-leader: abort the whole transfer. */
                pthread_mutex_lock(&r->lock);
                if (sresp.term > tsdb_raft_log_current_term(r->log)) {
                    become_follower_locked(r, sresp.term, 0);
                    pthread_mutex_unlock(&r->lock);
                    aborted = 1; break;
                }
                pthread_mutex_unlock(&r->lock);

                off += take;
                if (take == 0) break; /* defensive, empty-body 1-shot */
            } while (off < body_len);

            free(sbuf);
            free(body);

            if (!aborted) {
                /* All chunks ack'd.  Peer is now caught up through
                 * snap_idx; AEs from snap_idx+1 will drain the tail. */
                pthread_mutex_lock(&r->lock);
                for (int i = 0; i < r->npeers; i++) {
                    if (r->peers[i].id == peer_id) {
                        r->peers[i].next_index = snap_idx + 1;
                        if (r->peers[i].match_index < snap_idx)
                            r->peers[i].match_index = snap_idx;
                        break;
                    }
                }
                pthread_mutex_unlock(&r->lock);
            }
            /* Either way, next heartbeat retries; abort on transient
             * failure is cheaper than an infinite re-try storm here. */
            return;

skip_snap:
            pthread_mutex_lock(&r->lock);
            /* fall through to the heartbeat-only path below */
        }
        r->peers[peer_idx].next_index = snap_idx + 1;
        next_idx = snap_idx + 1;
    }

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

/* Forward decl — propose is defined later; the tick thread needs it
 * to drive the leader-seeded config bootstrap. */
int tsdb_raft_propose(tsdb_raft_t *r,
                       tsdb_raft_entry_type_t type,
                       const void *payload, uint32_t payload_len,
                       int timeout_ms);

/* Encode a TSDB_RAFT_CFG_OP_SEED payload:
 *   u8  op = TSDB_RAFT_CFG_OP_SEED
 *   u8  count
 *   [ u64 id | u8 addr_len | char addr[addr_len] ] * count
 *
 * Returns encoded bytes on success, 0 on cap overflow. */
static uint32_t encode_cfg_seed(const tsdb_raft_cfg_member_t *members,
                                 int n,
                                 uint8_t *buf, uint32_t cap)
{
    if (n < 0 || n > 255) return 0;
    if (cap < 2) return 0;
    uint32_t off = 0;
    buf[off++] = TSDB_RAFT_CFG_OP_SEED;
    buf[off++] = (uint8_t)n;
    for (int i = 0; i < n; i++) {
        uint8_t al = (uint8_t)strnlen(members[i].addr,
                                       sizeof(members[i].addr));
        if (al > 79) al = 79;
        if ((uint32_t)(off + 8 + 1 + al) > cap) return 0;
        memcpy(buf + off, &members[i].id, 8);
        off += 8;
        buf[off++] = al;
        if (al > 0) {
            memcpy(buf + off, members[i].addr, al);
            off += al;
        }
    }
    return off;
}

/* Called by the tick thread OUTSIDE r->lock when the leader won
 * election on a fresh cluster (pending_seed set).  Builds a SEED
 * payload from ALIVE master peers + self and proposes it.  On success
 * or on "not leader any more" outcome, pending_seed is cleared so we
 * don't re-propose.  Temporary failures (timeout, no quorum yet)
 * leave the flag set so the next tick retries. */
static void drive_pending_seed(tsdb_raft_t *r) {
    /* Snapshot everything we need to build the payload under the
     * lock; release it before calling propose (which acquires). */
    pthread_mutex_lock(&r->lock);
    if (r->state != TSDB_RAFT_LEADER || !r->pending_seed) {
        pthread_mutex_unlock(&r->lock);
        return;
    }
    /* If config somehow got populated while we weren't looking
     * (e.g. apply thread replayed a SEED from the log on a quick
     * reboot), clear the flag and bail — no need to propose. */
    if (r->config && tsdb_raft_config_is_initialised(r->config)) {
        r->pending_seed = 0;
        pthread_mutex_unlock(&r->lock);
        return;
    }

    /* Snapshot gossip-ALIVE masters (including self). */
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int ns = tsdb_node_manager_snapshot(r->node_mgr, snap,
                                          TSDB_CLUSTER_MAX_NODES);
    tsdb_raft_cfg_member_t members[TSDB_RAFT_CFG_MAX_MASTERS];
    int nm = 0;
    for (int i = 0; i < ns && nm < TSDB_RAFT_CFG_MAX_MASTERS; i++) {
        if (snap[i].role != TSDB_ROLE_MASTER) continue;
        if (snap[i].state == TSDB_NODE_DEAD) continue;
        members[nm].id = snap[i].id;
        snprintf(members[nm].addr, sizeof(members[nm].addr), "%s",
                 snap[i].addr);
        nm++;
    }
    /* Ensure self is in the seed (gossip may not list self depending
     * on the impl; defensive). */
    int have_self = 0;
    for (int i = 0; i < nm; i++) {
        if (members[i].id == r->self_id) { have_self = 1; break; }
    }
    if (!have_self && nm < TSDB_RAFT_CFG_MAX_MASTERS) {
        const char *addr = tsdb_node_manager_local_addr(r->node_mgr);
        members[nm].id = r->self_id;
        snprintf(members[nm].addr, sizeof(members[nm].addr), "%s",
                 addr ? addr : "");
        nm++;
    }
    pthread_mutex_unlock(&r->lock);

    if (nm < 1) return; /* retry next tick */

    uint8_t payload[2 + TSDB_RAFT_CFG_MAX_MASTERS * (8 + 1 + 80)];
    uint32_t plen = encode_cfg_seed(members, nm, payload, sizeof(payload));
    if (plen == 0) return;

    /* Short timeout — keeps the tick thread responsive.  If the
     * propose doesn't commit within 500 ms the next tick retries.
     * That also covers the "cluster lost quorum mid-propose" case. */
    int prc = tsdb_raft_propose(r, TSDB_RAFT_ENTRY_CONFIG, payload, plen, 500);

    pthread_mutex_lock(&r->lock);
    if (prc == TSDB_OK) {
        r->pending_seed = 0;
    } else if (prc == TSDB_ERR_PERMISSION) {
        /* Lost leadership mid-propose.  become_follower_locked already
         * cleared pending_seed; belt-and-suspenders. */
        r->pending_seed = 0;
    }
    /* Any other error (IO / timeout): leave flag set, next tick retries. */
    pthread_mutex_unlock(&r->lock);
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

        /* Leader-seeded config bootstrap.  Every node used to seed
         * config.bin independently right after the startup grace
         * window — but that's a race: early starters saw fewer
         * masters than late starters, so LIST MASTERS diverged
         * across the cluster on a fresh boot.  Now only the LEADER
         * proposes a SEED log entry (TSDB_RAFT_CFG_OP_SEED) with the
         * master snapshot it sees; followers pick it up through
         * normal replication.  See become_leader_locked() for the
         * flag that kicks this off. */
        if (st == TSDB_RAFT_LEADER) {
            drive_pending_seed(r);

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
             * period: on a fresh multi-master cluster, gossip needs a
             * moment to deliver peer addresses and without the grace
             * every master would elect itself (1-node quorum) and we'd
             * split-brain.  After grace, np==0 is legitimate — it
             * means the gossip view really has no other masters, which
             * is the normal shape of a 1-master topology — and the
             * lone master must elect itself to make progress. */
            if (now < grace_until) {
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
    snprintf(r->data_dir, sizeof(r->data_dir), "%s", data_dir);

    /* Wipe any stale snapshot-chunk staging file left by a mid-
     * transfer crash.  Leader always re-starts at offset=0 so there's
     * nothing worth resuming. */
    {
        char stale[4200];
        int n = snprintf(stale, sizeof(stale),
                         "%s/raft/snapshot/incoming.bin.tmp", data_dir);
        if (n > 0 && (size_t)n < sizeof(stale)) (void)unlink(stale);
    }

    r->log = tsdb_raft_log_open(data_dir);
    if (!r->log) { free(r); return NULL; }
    r->config = tsdb_raft_config_open(data_dir);
    /* config==NULL is OK; the module falls back to gossip-derived
     * quorum counts so existing clusters keep functioning. */

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
    tsdb_raft_rpc_set_handlers(on_request_vote, on_append_entries,
                               on_install_snapshot, r);

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
    tsdb_raft_rpc_set_handlers(NULL, NULL, NULL, NULL);
    tsdb_raft_config_close(r->config);
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
uint64_t tsdb_raft_snapshot_index(tsdb_raft_t *r) {
    return r ? tsdb_raft_log_snapshot_index(r->log) : 0;
}

int tsdb_raft_config_members(tsdb_raft_t *r,
                              tsdb_raft_cfg_member_t *out, int cap)
{
    if (!r || !out || cap <= 0) return 0;
    return r->config ? tsdb_raft_config_snapshot(r->config, out, cap) : 0;
}

/* Build a CONFIG-entry payload: op | id | addr_len | addr. */
static uint32_t encode_cfg_payload(uint8_t op, uint64_t id, const char *addr,
                                    uint8_t *buf, uint32_t cap)
{
    uint8_t al = addr ? (uint8_t)strlen(addr) : 0;
    if (al > 79) al = 79;
    if ((uint32_t)(10u + al) > cap) return 0;
    buf[0] = op;
    memcpy(buf + 1, &id, 8);
    buf[9] = al;
    if (al > 0) memcpy(buf + 10, addr, al);
    return 10u + al;
}

int tsdb_raft_add_master(tsdb_raft_t *r,
                          uint64_t id, const char *addr, int timeout_ms)
{
    if (!r) return TSDB_ERR_INVAL;
    uint8_t payload[128];
    uint32_t plen = encode_cfg_payload(TSDB_RAFT_CFG_OP_ADD, id, addr,
                                        payload, sizeof(payload));
    if (plen == 0) return TSDB_ERR_INVAL;
    return tsdb_raft_propose(r, TSDB_RAFT_ENTRY_CONFIG,
                              payload, plen, timeout_ms);
}

int tsdb_raft_remove_master(tsdb_raft_t *r, uint64_t id, int timeout_ms) {
    if (!r) return TSDB_ERR_INVAL;
    uint8_t payload[16];
    uint32_t plen = encode_cfg_payload(TSDB_RAFT_CFG_OP_REMOVE, id, NULL,
                                        payload, sizeof(payload));
    if (plen == 0) return TSDB_ERR_INVAL;
    return tsdb_raft_propose(r, TSDB_RAFT_ENTRY_CONFIG,
                              payload, plen, timeout_ms);
}

void tsdb_raft_set_snapshot_handlers(tsdb_raft_t *r,
                                      tsdb_raft_snapshot_write_fn   write_fn,
                                      tsdb_raft_snapshot_restore_fn restore_fn,
                                      void *ud)
{
    if (!r) return;
    pthread_mutex_lock(&r->lock);
    r->snap_write_fn   = write_fn;
    r->snap_restore_fn = restore_fn;
    r->snap_ud         = ud;
    pthread_mutex_unlock(&r->lock);
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
            if (e.type == TSDB_RAFT_ENTRY_CONFIG) {
                /* Raft owns CONFIG entries — apply directly to the
                 * persistent config file.  Three op codes:
                 *   ADD    → u8 op | u64 id | u8 addr_len | char addr[]
                 *   REMOVE → u8 op | u64 id | u8 addr_len | char addr[]
                 *                                 (addr ignored)
                 *   SEED   → u8 op | u8 count | [u64 id | u8 addr_len |
                 *                                 char addr[]] * count
                 *
                 * SEED is emitted exactly once per cluster life by the
                 * first elected leader against a pristine config.bin;
                 * we replay it with tsdb_raft_config_set() so every
                 * node converges on the same LIST MASTERS view. */
                if (e.payload_len >= 1 && r->config) {
                    uint8_t *p = (uint8_t *)e.payload;
                    uint8_t op = p[0];
                    if (op == TSDB_RAFT_CFG_OP_ADD ||
                        op == TSDB_RAFT_CFG_OP_REMOVE) {
                        if (e.payload_len >= 10) {
                            uint64_t id;
                            memcpy(&id, p + 1, 8);
                            uint8_t al = p[9];
                            char addr[80] = {0};
                            if ((uint32_t)(10 + al) <= e.payload_len &&
                                al < sizeof(addr)) {
                                memcpy(addr, p + 10, al);
                                addr[al] = '\0';
                            }
                            if (op == TSDB_RAFT_CFG_OP_ADD)
                                (void)tsdb_raft_config_add(r->config, id, addr);
                            else
                                (void)tsdb_raft_config_remove(r->config, id);
                        }
                    } else if (op == TSDB_RAFT_CFG_OP_SEED) {
                        if (e.payload_len >= 2) {
                            uint8_t count = p[1];
                            uint32_t off = 2;
                            tsdb_raft_cfg_member_t members[TSDB_RAFT_CFG_MAX_MASTERS];
                            int nm = 0;
                            int valid = 1;
                            for (int m = 0; m < count &&
                                 nm < TSDB_RAFT_CFG_MAX_MASTERS; m++) {
                                if (off + 9 > e.payload_len) { valid = 0; break; }
                                uint64_t id;
                                memcpy(&id, p + off, 8);
                                off += 8;
                                uint8_t al = p[off++];
                                if (off + al > e.payload_len) { valid = 0; break; }
                                char addr[80] = {0};
                                uint8_t use_al = al < 79 ? al : 79;
                                if (use_al > 0) memcpy(addr, p + off, use_al);
                                addr[use_al] = '\0';
                                off += al;
                                members[nm].id = id;
                                snprintf(members[nm].addr,
                                          sizeof(members[nm].addr), "%s", addr);
                                nm++;
                            }
                            if (valid && nm >= 1) {
                                (void)tsdb_raft_config_set(r->config,
                                                            members, nm);
                            }
                        }
                    }
                }
            } else if (r->apply_fn) {
                (void)r->apply_fn(r->apply_ud, &e);
            }
            uint64_t entry_term = e.term;
            free(e.payload);
            pthread_mutex_lock(&r->lock);
            r->last_applied = idx;
            pthread_cond_broadcast(&r->commit_cv); /* wake proposers */
            pthread_mutex_unlock(&r->lock);

            /* Auto-compact: once we're SNAP_COMPACT_STRIDE past the
             * current snapshot boundary, roll it forward.  Keeps the
             * log bounded regardless of DDL workload.  Entry terms are
             * stable after apply so we can compact up to `idx`. */
            uint64_t snap = tsdb_raft_log_snapshot_index(r->log);
            if (idx >= snap + SNAP_COMPACT_STRIDE) {
                (void)tsdb_raft_log_compact(r->log, idx, entry_term);
            }
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
    /* On a single-master cluster (npeers==0) no AppendEntries response
     * ever comes back, so the usual path through replicate_to →
     * maybe_advance_commit_locked never fires — the leader would hang
     * forever waiting for its own entry to commit.  The leader's own
     * last_index counts toward the quorum; kick the advance here so a
     * lone leader makes immediate progress.  For multi-node clusters
     * this is a cheap no-op: quorum isn't satisfied until peers ack. */
    maybe_advance_commit_locked(r);
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
