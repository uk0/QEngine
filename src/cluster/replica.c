/* replica.c — synchronous quorum replication implementation.
 *
 * Fan-out uses detached worker threads and a refcounted context so the
 * caller can return as soon as quorum is reached, without waiting for
 * slow peers.  Workers decrement the refcount on exit; the last unref
 * frees the context and payload.
 */

#include "replica.h"
#include "node.h"
#include "rpc.h"
#include "../server/metrics.h"
#include "../federation/dr_forwarder.h"
#include "../exec/pool.h"
#include "../compress/lzlite.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <errno.h>

/* ---- Connection-stability helpers ---------------------------------------- *
 *
 * Tiny pure functions kept near the top so they are unit-testable in
 * isolation (see tests/test_replica_backoff.c, which links them via the
 * TSDB_TEST wrapper at the bottom of this file).
 */

/*
 * Whether we should bother dialing a peer in the given gossip state.
 *
 * A hard-DEAD peer is skipped: gossip's failure detector has already
 * concluded it is down, so a connect attempt only burns the connect
 * timeout and never succeeds.  It resyncs via anti-entropy once it
 * rejoins, so dropping its replica writes here is safe.  SUSPECT stays
 * dialable — a suspected peer may merely have missed a probe and is
 * worth one more try; only hard-DEAD is filtered.
 */
static int replica_peer_dialable(tsdb_node_state_t state)
{
    return state == TSDB_NODE_DEAD ? 0 : 1;
}

/*
 * Exponential backoff between connect/RPC retries against a single peer.
 *
 * Schedule (base 20 ms, doubling, capped at 500 ms), returned in
 * nanoseconds:
 *
 *   attempt 0 ->  20 ms
 *   attempt 1 ->  40 ms
 *   attempt 2 ->  80 ms
 *   attempt 3 -> 160 ms
 *   attempt 4 -> 320 ms
 *   attempt 5+-> 500 ms (cap)
 *
 * Replaces the old fixed 5 ms / 50 ms sleeps so a flapping or
 * slow-to-listen peer is retried with progressively longer gaps rather
 * than a tight fixed-delay loop.
 */
static long replica_backoff_ns(int attempt)
{
    const long base_ms = 20;
    const long cap_ms  = 500;
    if (attempt < 0) attempt = 0;
    /* Clamp the shift so 1L << attempt can't overflow; 20 ms << 5 already
     * exceeds the cap, so anything past that clamps to cap_ms anyway. */
    if (attempt > 5) attempt = 5;
    long ms = base_ms * (1L << attempt);
    if (ms > cap_ms) ms = cap_ms;
    return ms * 1000000L;
}

/* ---- Connection pool ----------------------------------------------------- */

/*
 * Per-peer connection pool.  Each peer gets N connections (env
 * TSDB_REPLICA_CONNS_PER_PEER, default 8).  Round-robin dispatch via an
 * atomic counter: callers may still block on a per-conn mutex inside
 * tsdb_rpc_call if two calls land on the same slot, but the peer-side
 * RPC server spawns one handler thread per incoming connection, so N
 * connections == N concurrent peer handlers.
 */
#define TSDB_REPLICA_CONNS_PER_PEER_MAX  32
#define TSDB_REPLICA_CONNS_PER_PEER_DEF  8
#define MAX_PEERS                        TSDB_CLUSTER_MAX_NODES

typedef struct {
    tsdb_node_id_t   node_id;     /* 0 = empty slot */
    tsdb_rpc_conn_t *conns[TSDB_REPLICA_CONNS_PER_PEER_MAX];
    atomic_uint      rr;          /* round-robin dispatch counter */
} peer_slot_t;

struct tsdb_replica_mgr {
    tsdb_node_manager_t *node_mgr;
    pthread_mutex_t      lock;     /* protects peers[].node_id / conns[] insertion */
    peer_slot_t          peers[MAX_PEERS];
    int                  conns_per_peer;  /* fixed at mgr_new time */
    /* Optional cross-DC forwarder.  Non-NULL means every outbound
     * row-batch payload is also copied to the forwarder's ring for
     * async delivery to the remote DC.  Transparent to cluster RPC
     * fanout — see src/federation/dr_forwarder.c. */
    struct tsdb_dr_forwarder *dr;

    /* In-flight fanout workers (pool tasks holding ctx->rmgr).  mgr_free
     * must wait for this to drain before freeing — ASan caught a worker
     * reading peers[] after tsdb_close_cluster freed the mgr at shutdown.
     * Guarded by `lock`; signaled on zero via infl_cv. */
    int            fanout_inflight;
    pthread_cond_t infl_cv;
};

static int env_conns_per_peer(void) {
    const char *v = getenv("TSDB_REPLICA_CONNS_PER_PEER");
    if (!v || !*v) return TSDB_REPLICA_CONNS_PER_PEER_DEF;
    int n = atoi(v);
    if (n < 1) n = 1;
    if (n > TSDB_REPLICA_CONNS_PER_PEER_MAX) n = TSDB_REPLICA_CONNS_PER_PEER_MAX;
    return n;
}

tsdb_replica_mgr_t *tsdb_replica_mgr_new(tsdb_node_manager_t *node_mgr) {
    tsdb_replica_mgr_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;
    r->node_mgr = node_mgr;
    r->conns_per_peer = env_conns_per_peer();
    pthread_mutex_init(&r->lock, NULL);
    pthread_cond_init(&r->infl_cv, NULL);
    return r;
}

void tsdb_replica_mgr_set_dr(tsdb_replica_mgr_t *rmgr, struct tsdb_dr_forwarder *fw) {
    if (!rmgr) return;
    rmgr->dr = fw;
}

void tsdb_replica_mgr_free(tsdb_replica_mgr_t *rmgr) {
    if (!rmgr) return;

    /* Wait for in-flight fanout workers (they read peers[]/conns via
     * ctx->rmgr).  On timeout, LEAK the mgr — a worker still running
     * means free() here is a use-after-free. */
    pthread_mutex_lock(&rmgr->lock);
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    int drained = 1;
    while (rmgr->fanout_inflight > 0) {
        if (pthread_cond_timedwait(&rmgr->infl_cv, &rmgr->lock, &dl) == ETIMEDOUT) {
            drained = 0;
            break;
        }
    }
    int leftover = rmgr->fanout_inflight;
    pthread_mutex_unlock(&rmgr->lock);
    if (!drained) {
        fprintf(stderr, "[replica] free: %d fanout worker(s) still in flight "
                        "after 10s — leaking mgr instead of freeing\n", leftover);
        return;
    }

    for (int i = 0; i < MAX_PEERS; i++) {
        peer_slot_t *p = &rmgr->peers[i];
        if (p->node_id == 0) continue;
        for (int k = 0; k < rmgr->conns_per_peer; k++) {
            if (p->conns[k]) {
                tsdb_rpc_conn_close(p->conns[k]);
                p->conns[k] = NULL;
            }
        }
    }
    pthread_mutex_destroy(&rmgr->lock);
    free(rmgr);
}

/* Find the slot for node_id; return NULL if absent.  Caller must hold lock. */
static peer_slot_t *find_slot_locked(tsdb_replica_mgr_t *rmgr,
                                      tsdb_node_id_t node_id)
{
    for (int i = 0; i < MAX_PEERS; i++) {
        if (rmgr->peers[i].node_id == node_id)
            return &rmgr->peers[i];
    }
    return NULL;
}

/* Claim an empty slot for node_id; return it.  Caller must hold lock. */
static peer_slot_t *claim_slot_locked(tsdb_replica_mgr_t *rmgr,
                                       tsdb_node_id_t node_id)
{
    for (int i = 0; i < MAX_PEERS; i++) {
        if (rmgr->peers[i].node_id == 0) {
            rmgr->peers[i].node_id = node_id;
            return &rmgr->peers[i];
        }
    }
    return NULL;
}

tsdb_rpc_conn_t *tsdb_replica_mgr_get_conn(tsdb_replica_mgr_t *rmgr,
                                             tsdb_node_id_t node_id)
{
    if (!rmgr) return NULL;

    pthread_mutex_lock(&rmgr->lock);
    peer_slot_t *p = find_slot_locked(rmgr, node_id);
    if (!p) p = claim_slot_locked(rmgr, node_id);
    if (!p) { pthread_mutex_unlock(&rmgr->lock); return NULL; }

    /* Pick a round-robin slot.  Atomic counter is cheap and gives us
     * uniform dispatch without tracking per-conn load. */
    unsigned idx = atomic_fetch_add_explicit(&p->rr, 1, memory_order_relaxed)
                   % (unsigned)rmgr->conns_per_peer;
    tsdb_rpc_conn_t *c = p->conns[idx];
    if (c) { pthread_mutex_unlock(&rmgr->lock); return c; }

    /* Slot empty — need to dial.  Release lock while we connect so other
     * threads can use already-established conns in the same peer. */
    tsdb_node_info_t info = {0};
    if (tsdb_node_manager_get(rmgr->node_mgr, node_id, &info) < 0) {
        pthread_mutex_unlock(&rmgr->lock);
        return NULL;
    }
    /* Gossip says this peer is hard-DEAD — don't dial.  tsdb_rpc_connect
     * would otherwise sit on the full 2 s connect timeout for nothing; the
     * peer resyncs via anti-entropy when it rejoins. */
    if (!replica_peer_dialable(info.state)) {
        pthread_mutex_unlock(&rmgr->lock);
        return NULL;
    }
    pthread_mutex_unlock(&rmgr->lock);

    tsdb_rpc_conn_t *newc = tsdb_rpc_connect(info.addr, 2000);
    if (!newc) {
        tsdb_metric_inc("qengine_replica_dial_fail_total");
        return NULL;
    }
    tsdb_metric_inc("qengine_replica_dial_total");

    pthread_mutex_lock(&rmgr->lock);
    /* Slot or peer may have been recycled while we dialed; re-find. */
    peer_slot_t *p2 = find_slot_locked(rmgr, node_id);
    if (!p2) {
        /* Peer vanished while we dialed (unlikely); drop the new conn. */
        pthread_mutex_unlock(&rmgr->lock);
        tsdb_rpc_conn_close(newc);
        return NULL;
    }
    if (p2->conns[idx]) {
        /* Another thread filled the slot; keep the earlier one. */
        tsdb_rpc_conn_t *existing = p2->conns[idx];
        pthread_mutex_unlock(&rmgr->lock);
        tsdb_rpc_conn_close(newc);
        return existing;
    }
    p2->conns[idx] = newc;
    pthread_mutex_unlock(&rmgr->lock);
    return newc;
}

/*
 * Evict a single failing conn from its slot.  Does NOT close the conn
 * object: other threads may still be mid-call on it under conn->lock,
 * and closing it there would free the mutex they are holding → UAF →
 * glibc heap abort.  Instead we clear the slot so future get_conn calls
 * redial, and intentionally leak the old conn's socket and struct.
 *
 * The leak is bounded: at most (nreplicas * conns_per_peer) per cluster
 * lifetime; a few KB of sockets if half the pool ever goes bad.  Closing
 * the conn at process exit is the cluster mgr's responsibility — we
 * accept the leak during runtime in exchange for memory safety across
 * concurrent RPCs.
 */
static void evict_one_conn(tsdb_replica_mgr_t *rmgr,
                           tsdb_node_id_t node_id,
                           tsdb_rpc_conn_t *bad)
{
    if (!rmgr || !bad) return;
    pthread_mutex_lock(&rmgr->lock);
    peer_slot_t *p = find_slot_locked(rmgr, node_id);
    if (p) {
        for (int k = 0; k < rmgr->conns_per_peer; k++) {
            if (p->conns[k] == bad) {
                p->conns[k] = NULL;   /* next get_conn will redial */
                break;
            }
        }
    }
    pthread_mutex_unlock(&rmgr->lock);
    /* DO NOT tsdb_rpc_conn_close(bad). */
}

/* Public wrapper: callers outside the replication fan-out (e.g. the stable
 * scatter coordinator) hit the same stale-cached-conn failure mode after a
 * peer restart, and without eviction every retry gets the same poisoned
 * socket back — the failure becomes permanent instead of a one-shot blip. */
void tsdb_replica_mgr_evict_conn(tsdb_replica_mgr_t *rmgr,
                                  tsdb_node_id_t node_id,
                                  tsdb_rpc_conn_t *bad)
{
    evict_one_conn(rmgr, node_id, bad);
}

/* ---- Replication --------------------------------------------------------- */

/*
 * Shared fan-out context.  Caller holds 1 ref; each worker thread holds 1
 * ref.  Workers increment ack_count on success, signal the condvar when
 * quorum is met or when all workers have finished.  Last unref frees the
 * context (including the encoded payload).
 */
typedef struct {
    atomic_int            refcount;
    pthread_mutex_t       mu;
    pthread_cond_t        cv;

    int                   ack_count;       /* includes local write = 1 */
    int                   done_count;      /* threads that have finished */
    int                   nreplicas;
    int                   quorum;

    uint8_t              *payload;         /* owned: freed on last unref */
    uint32_t              payload_len;
    tsdb_replica_mgr_t   *rmgr;
    int                   rpc_kind;        /* TSDB_RPC_WRITE_BATCH or SCHEMA_SYNC */
} fanout_ctx_t;

typedef struct {
    fanout_ctx_t  *ctx;
    tsdb_node_id_t node_id;
} worker_arg_t;

static fanout_ctx_t *fanout_ctx_new(tsdb_replica_mgr_t *rmgr,
                                     uint8_t *payload, uint32_t plen,
                                     int quorum, int rpc_kind)
{
    fanout_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    atomic_init(&ctx->refcount, 1);  /* caller's ref */
    pthread_mutex_init(&ctx->mu, NULL);
    pthread_cond_init(&ctx->cv, NULL);
    ctx->ack_count   = 1;  /* local already written */
    ctx->quorum      = quorum;
    ctx->payload     = payload;
    ctx->payload_len = plen;
    ctx->rmgr        = rmgr;
    ctx->rpc_kind    = rpc_kind;
    return ctx;
}

static void fanout_ctx_unref(fanout_ctx_t *ctx) {
    if (atomic_fetch_sub_explicit(&ctx->refcount, 1, memory_order_acq_rel) == 1) {
        pthread_mutex_destroy(&ctx->mu);
        pthread_cond_destroy(&ctx->cv);
        free(ctx->payload);
        free(ctx);
    }
}

/* Iter 6: shared fanout worker pool — replaces per-batch pthread_create
 * which spawned one detached thread PER replica PER WRITE_BATCH.  Under
 * sustained writes (8000-16000 batches/s × ~1-2 replicas each, QUORUM=0
 * leaving workers waiting on peer ACK in the background) thread count
 * piled into the thousands.  A fixed-size pool (default 64) reuses
 * threads across batches: lower context-switch overhead, bounded RSS,
 * predictable scheduling.  Env TSDB_FANOUT_POOL_SIZE overrides.
 *
 * Lazy init under pthread_once so the pool only materialises if fanout
 * ever runs (single-node tests don't pay for it).  Falls back to
 * pthread_create if the pool can't be created or submit fails — same
 * observable behaviour as before, just bounded by the pool's queue. */
static void *fanout_worker(void *arg);   /* forward */

static pthread_once_t g_fanout_pool_once = PTHREAD_ONCE_INIT;
static tsdb_pool_t   *g_fanout_pool      = NULL;

static void fanout_pool_init(void) {
    int n = 64;
    const char *e = getenv("TSDB_FANOUT_POOL_SIZE");
    if (e && *e) { int v = atoi(e); if (v > 0 && v <= 4096) n = v; }
    if (tsdb_pool_new(n, &g_fanout_pool) != TSDB_OK) g_fanout_pool = NULL;
}

/* Adapter: pool callback returns void; the underlying worker returns void*. */
static void fanout_worker_pool_cb(void *arg) {
    (void)fanout_worker(arg);
}

static void *fanout_worker(void *arg) {
    worker_arg_t *wa = (worker_arg_t *)arg;
    fanout_ctx_t *ctx = wa->ctx;
    tsdb_node_id_t nid = wa->node_id;
    /* Capture before fanout_ctx_unref can free ctx; the submitter took our
     * inflight slot before submit, so rmgr outlives this function. */
    tsdb_replica_mgr_t *rmgr_local = ctx->rmgr;
    free(wa);

    /* Retry fanout against a single peer up to MAX_ATTEMPTS times
     * before giving up.  Two distinct failure modes:
     *
     *   - Dial failure (conn == NULL): peer's RPC server isn't
     *     accepting yet — typical during a cluster cold start where
     *     gossip marks the peer ALIVE a moment before the listen
     *     socket binds, or right after a container restart.  Back off
     *     with replica_backoff_ns() so the listener has time to come up
     *     before the next try.  Without this, bench startups show dozens
     *     to hundreds of dial_fails on the first fanout wave that
     *     actually landed cleanly on retry.
     *
     *   - RPC-level failure (rc != OK): the socket was good but the
     *     peer's handler returned TSDB_RPC_ERR (e.g. receive-side
     *     batch_commit failed) or the call timed out.  Evict the
     *     (possibly poisoned) conn from the pool and back off (same
     *     schedule) so a peer's in-flight table-lock has a chance to
     *     drain.
     *
     * MAX_ATTEMPTS stays small so a permanently-dead peer doesn't
     * wedge the dispatcher forever — the quorum waiter still
     * returns as soon as enough other peers have acked. */
    const int MAX_ATTEMPTS = 3;
    int rc = TSDB_ERR_IO;

    /* Skip a gossip-DEAD peer entirely: get_conn would already refuse to
     * dial it (returns NULL), so spending all MAX_ATTEMPTS + backoff here
     * only delays this worker's done_count bump and the quorum waiter.
     * One up-front state check; ALIVE/SUSPECT peers fall through and retry
     * with backoff exactly as before. */
    {
        tsdb_node_info_t pinfo = {0};
        if (tsdb_node_manager_get(ctx->rmgr->node_mgr, nid, &pinfo) == 0 &&
            !replica_peer_dialable(pinfo.state)) {
            goto done;
        }
    }

    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(ctx->rmgr, nid);
        if (!conn) {
            rc = TSDB_ERR_IO;
            if (attempt + 1 < MAX_ATTEMPTS) {
                struct timespec ts = { .tv_sec = 0,
                                       .tv_nsec = replica_backoff_ns(attempt) };
                nanosleep(&ts, NULL);
            }
            continue;
        }
        rc = tsdb_rpc_call(conn, ctx->rpc_kind, ctx->payload, ctx->payload_len);
        if (rc == TSDB_OK) break;
        evict_one_conn(ctx->rmgr, nid, conn);
        if (attempt + 1 < MAX_ATTEMPTS) {
            struct timespec ts = { .tv_sec = 0,
                                   .tv_nsec = replica_backoff_ns(attempt) };
            nanosleep(&ts, NULL);
        }
    }
done:
    if (rc == TSDB_OK) {
        tsdb_metric_inc("qengine_replicate_ack_total");
    } else {
        tsdb_metric_inc("qengine_replicate_fail_total");
    }

    pthread_mutex_lock(&ctx->mu);
    if (rc == TSDB_OK) ctx->ack_count++;
    ctx->done_count++;
    /* Wake the waiter on either quorum-reached or everyone-finished. */
    if (ctx->ack_count >= ctx->quorum || ctx->done_count >= ctx->nreplicas) {
        pthread_cond_broadcast(&ctx->cv);
    }
    pthread_mutex_unlock(&ctx->mu);

    fanout_ctx_unref(ctx);

    /* Release the inflight slot the submitter reserved for us. */
    pthread_mutex_lock(&rmgr_local->lock);
    if (--rmgr_local->fanout_inflight == 0)
        pthread_cond_broadcast(&rmgr_local->infl_cv);
    pthread_mutex_unlock(&rmgr_local->lock);
    return NULL;
}

/*
 * Fan out `payload` to `replicas[]` and block until quorum ACKs arrive or
 * all workers finish.  On success, caller returns TSDB_OK without waiting
 * for slow replicas — they finish in background and drop their refs on the
 * shared context.
 *
 * When `out_acks` is non-NULL, the total ack count (including the local
 * +1 baseline) is written out on return; only meaningful in the
 * wait-for-all case (quorum > 1 + nreplicas forces the loop to wait until
 * every worker finishes).
 */
static int fanout_wait_quorum_ex(tsdb_replica_mgr_t *rmgr,
                                 uint8_t *payload, uint32_t plen,
                                 const tsdb_node_id_t *replicas, int nreplicas,
                                 int quorum, int rpc_kind,
                                 int *out_acks)
{
    /* Caller's local write counts as 1 ack; if even launching every
     * replica can't reach quorum, fail fast. */
    if (1 + nreplicas < quorum) { free(payload); return TSDB_ERR_IO; }

    fanout_ctx_t *ctx = fanout_ctx_new(rmgr, payload, plen, quorum, rpc_kind);
    if (!ctx) { free(payload); return TSDB_ERR_NOMEM; }
    ctx->nreplicas = nreplicas;

    /* Bump refcount once per worker *before* creating any thread, so a
     * worker that exits before the loop ends can't drop refcount to 0. */
    atomic_fetch_add_explicit(&ctx->refcount, nreplicas, memory_order_acq_rel);

    /* Reserve an mgr-inflight slot per worker while rmgr is guaranteed
     * alive (we're on the submitter's thread).  Workers release theirs at
     * exit; mgr_free waits for zero before freeing. */
    pthread_mutex_lock(&rmgr->lock);
    rmgr->fanout_inflight += nreplicas;
    pthread_mutex_unlock(&rmgr->lock);

    tsdb_metric_add("qengine_replicate_sent_total", (uint64_t)nreplicas);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    int launched = 0;
    for (int i = 0; i < nreplicas; i++) {
        worker_arg_t *wa = malloc(sizeof(*wa));
        if (!wa) {
            /* Can't launch this worker — release its pre-reserved ref
             * and count it as "done" so the waiter doesn't hang. */
            pthread_mutex_lock(&ctx->mu);
            ctx->done_count++;
            if (ctx->done_count >= ctx->nreplicas)
                pthread_cond_broadcast(&ctx->cv);
            pthread_mutex_unlock(&ctx->mu);
            fanout_ctx_unref(ctx);
            pthread_mutex_lock(&rmgr->lock);
            if (--rmgr->fanout_inflight == 0)
                pthread_cond_broadcast(&rmgr->infl_cv);
            pthread_mutex_unlock(&rmgr->lock);
            continue;
        }
        wa->ctx     = ctx;
        wa->node_id = replicas[i];

        /* Iter 6: prefer the shared pool over a per-batch pthread_create.
         * Lazily materialise the pool on first fanout. */
        pthread_once(&g_fanout_pool_once, fanout_pool_init);
        int submitted = 0;
        if (g_fanout_pool && tsdb_pool_submit(g_fanout_pool,
                                              fanout_worker_pool_cb, wa) == TSDB_OK) {
            submitted = 1;
        } else {
            /* Pool unavailable or full — fall back to legacy detached thread
             * so a transient pool issue can never wedge writes. */
            pthread_t tid;
            if (pthread_create(&tid, &attr, fanout_worker, wa) == 0) submitted = 1;
        }
        if (submitted) {
            launched++;
        } else {
            /* Worker never ran — same handling as the malloc-fail path. */
            free(wa);
            pthread_mutex_lock(&ctx->mu);
            ctx->done_count++;
            if (ctx->done_count >= ctx->nreplicas)
                pthread_cond_broadcast(&ctx->cv);
            pthread_mutex_unlock(&ctx->mu);
            fanout_ctx_unref(ctx);
            pthread_mutex_lock(&rmgr->lock);
            if (--rmgr->fanout_inflight == 0)
                pthread_cond_broadcast(&rmgr->infl_cv);
            pthread_mutex_unlock(&rmgr->lock);
        }
    }
    pthread_attr_destroy(&attr);
    (void)launched;

    /* Wait for quorum OR for every worker to finish — with a hard deadline.
     * This waiter can be the raft apply thread (catalog broadcast); an
     * unbounded wait there froze last_applied and timed out every DDL
     * propose cluster-wide when one worker wedged on a dead peer conn.
     * Workers hold their own ctx refs, so leaving them behind is safe —
     * fanout_ctx refcounting already supports workers outliving us. */
    static int wait_tmo_ms = -1;
    if (wait_tmo_ms < 0) {
        const char *e = getenv("TSDB_FANOUT_WAIT_TIMEOUT_MS");
        wait_tmo_ms = (e && *e) ? atoi(e) : 45000;
    }
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec  += wait_tmo_ms / 1000;
    dl.tv_nsec += (long)(wait_tmo_ms % 1000) * 1000000L;
    if (dl.tv_nsec >= 1000000000L) { dl.tv_sec++; dl.tv_nsec -= 1000000000L; }

    pthread_mutex_lock(&ctx->mu);
    while (ctx->ack_count < ctx->quorum && ctx->done_count < ctx->nreplicas) {
        if (wait_tmo_ms > 0) {
            if (pthread_cond_timedwait(&ctx->cv, &ctx->mu, &dl) == ETIMEDOUT) {
                tsdb_metric_inc("qengine_fanout_wait_timeout_total");
                break;
            }
        } else {
            pthread_cond_wait(&ctx->cv, &ctx->mu);
        }
    }
    int ok = (ctx->ack_count >= ctx->quorum);
    int final_acks = ctx->ack_count;
    pthread_mutex_unlock(&ctx->mu);

    if (out_acks) *out_acks = final_acks;

    fanout_ctx_unref(ctx);  /* drop caller's ref; workers may outlive us */
    return ok ? TSDB_OK : TSDB_ERR_IO;
}

/* Thin wrapper: legacy callers don't need ack counts. */
static int fanout_wait_quorum(tsdb_replica_mgr_t *rmgr,
                              uint8_t *payload, uint32_t plen,
                              const tsdb_node_id_t *replicas, int nreplicas,
                              int quorum, int rpc_kind)
{
    return fanout_wait_quorum_ex(rmgr, payload, plen, replicas, nreplicas,
                                 quorum, rpc_kind, NULL);
}

int tsdb_replica_write(tsdb_replica_mgr_t *rmgr,
                       const char *table_name,
                       int ncols, const int *col_types,
                       int nrows, const void **col_data,
                       const tsdb_node_id_t *replicas, int nreplicas,
                       int w_quorum)
{
    if (!rmgr || nreplicas == 0) return TSDB_OK;

    /* Encode the WRITE_BATCH payload once.
     *
     * Sizing must include the actual SYMBOL byte count: the caller packs
     * each symbol column as `[u32 total_bytes][u16 len][bytes]…` which is
     * variable-length, NOT 4 bytes per row.  Underallocating here used to
     * silently fail tsdb_rpc_encode_write_batch (returns -1 on overrun)
     * and the whole replication call returned ERR_INTERNAL with no
     * counter ticking — peers stayed empty for any batch with non-trivial
     * symbol payloads (e.g. 100-row Influx writes with a host tag). */
    size_t fixed_per_row = 0;   /* non-symbol columns: 8 bytes each */
    size_t sym_bytes_total = 0; /* sum of [4 + total] across SYMBOL cols */
    for (int c = 0; c < ncols; c++) {
        if (col_types[c] == TSDB_TYPE_SYMBOL) {
            uint32_t total = 0;
            if (col_data[c]) memcpy(&total, col_data[c], 4);
            sym_bytes_total += 4 + (size_t)total;
        } else {
            fixed_per_row += 8;
        }
    }
    size_t payload_cap = 8 + (size_t)ncols * 2
                       + (size_t)nrows * fixed_per_row
                       + sym_bytes_total
                       + 256;  /* table_name + headers + safety margin */
    uint8_t *payload = malloc(payload_cap);
    if (!payload) return TSDB_ERR_NOMEM;

    int plen = tsdb_rpc_encode_write_batch(payload, (uint32_t)payload_cap,
                                           table_name,
                                           ncols, col_types,
                                           nrows, col_data);
    if (plen < 0) { free(payload); return TSDB_ERR_INTERNAL; }

    /* Cross-DC fan-out piggybacks on the intra-cluster payload so we
     * only pay the encode cost once.  The forwarder copies the buffer
     * internally (and FED_INGEST decodes RAW), so enqueue BEFORE the
     * lzlite step below; drops are reported on the sender's /metrics. */
    if (rmgr->dr) {
        tsdb_dr_forwarder_enqueue(rmgr->dr, payload, (size_t)plen);
    }

    /* Kafka-style batch compression: lzlite-wrap the WRITE_BATCH payload
     * for the intra-cluster fan-out.  The raw payload is columnar (ts is a
     * +Δ arithmetic run, vals are a correlated walk) so it carries real
     * residual redundancy — measured 38-39% on the loader's data.
     *
     * Default OFF, opt-in via TSDB_REPLICATION_COMPRESS=1.  Rationale
     * (verified A/B, 2026-06-08): the win is BANDWIDTH, not CPU.  On a
     * NIC-separated / WAN cluster (1-10 GbE, cross-DC DR) cutting 38% of
     * replication bytes is a clear gain.  But on a single-host cluster
     * where peers talk over loopback, bandwidth is ~free and the extra
     * lzlite CPU on the flush critical path costs ~40% throughput.  So
     * the operator enables it only when replication is bandwidth-bound.
     * Same-binary A/B (single-host loopback cluster, 480 s cold):
     *   compress=0 → 1.38M r/s @480s ; compress=1 → 0.81M r/s @480s.
     * Either way the payload is lossless (unit test + errs=0 both runs). */
    int rpc_kind = TSDB_RPC_WRITE_BATCH;
    {
        static int compress_enabled = -1;
        if (compress_enabled < 0) {
            const char *e = getenv("TSDB_REPLICATION_COMPRESS");
            compress_enabled = (e && e[0] == '1') ? 1 : 0;
        }
        tsdb_metric_add("qengine_replicate_bytes_raw_total", (uint64_t)plen);
        if (compress_enabled && plen >= 1024) {
            size_t cap = 4 + tsdb_lzlite_max_output((size_t)plen);
            uint8_t *lz = malloc(cap);
            if (lz) {
                size_t lzn = 0;
                /* encode returns bytes written (>=0) on success, neg on error;
                 * lzn is set to the same count on success. */
                int erc = tsdb_lzlite_encode(payload, (size_t)plen,
                                             lz + 4, cap - 4, &lzn);
                if (erc >= 0 && (4 + lzn) < (size_t)plen * 9 / 10) {
                    uint32_t orig = (uint32_t)plen;
                    memcpy(lz, &orig, 4);
                    free(payload);
                    payload  = lz;
                    plen     = (int)(4 + lzn);
                    rpc_kind = TSDB_RPC_WRITE_BATCH_LZ;
                } else {
                    free(lz);   /* no win — ship raw */
                }
            }
        }
        tsdb_metric_add("qengine_replicate_bytes_wire_total", (uint64_t)plen);
    }

    return fanout_wait_quorum(rmgr, payload, (uint32_t)plen,
                              replicas, nreplicas,
                              w_quorum, rpc_kind);
}

/* ---- Schema sync --------------------------------------------------------- */

int tsdb_replica_sync_schema(tsdb_replica_mgr_t *rmgr,
                              const char *table_name,
                              int ncols, const char **col_names,
                              const int *col_types, int ts_col_idx,
                              int partition_unit, int block_points,
                              int sort_by_tag_col,
                              const tsdb_node_id_t *nodes, int nnodes,
                              int quorum)
{
    if (!rmgr || nnodes == 0) return TSDB_OK;

    uint8_t stack_buf[2048];
    int plen = tsdb_rpc_encode_schema(stack_buf, sizeof(stack_buf),
                                      table_name, ncols, col_names,
                                      col_types, ts_col_idx,
                                      partition_unit, block_points,
                                      sort_by_tag_col);
    if (plen < 0) return TSDB_ERR_INTERNAL;

    /* Move onto heap so fan-out can own it past this call's return. */
    uint8_t *payload = malloc((size_t)plen);
    if (!payload) return TSDB_ERR_NOMEM;
    memcpy(payload, stack_buf, (size_t)plen);

    return fanout_wait_quorum(rmgr, payload, (uint32_t)plen,
                              nodes, nnodes,
                              quorum, TSDB_RPC_SCHEMA_SYNC);
}

/* ---- Broadcast: TRUNCATE / DELETE RANGE ---------------------------------
 *
 * Best-effort broadcast to all ALIVE peers.  Unlike write-path quorum,
 * we wait for every worker to finish and report how many ACKed.  Callers
 * (exec.c) surface the counts to the user as "X/Y peers ACKed".  The local
 * apply already happened before this is called; peers that were never
 * replicas simply reply with ACK (not-found is a no-op on the peer side).
 */
int tsdb_replica_broadcast_truncate(tsdb_replica_mgr_t *rmgr,
                                     const char *table_name,
                                     const tsdb_node_id_t *peers, int npeers,
                                     int *out_acked_peers)
{
    if (out_acked_peers) *out_acked_peers = 0;
    if (!rmgr || !table_name || npeers <= 0) return TSDB_OK;

    uint8_t stack_buf[256];
    int plen = tsdb_rpc_encode_truncate(stack_buf, sizeof(stack_buf), table_name);
    if (plen < 0) return TSDB_ERR_INTERNAL;

    uint8_t *payload = malloc((size_t)plen);
    if (!payload) return TSDB_ERR_NOMEM;
    memcpy(payload, stack_buf, (size_t)plen);

    /* quorum = 1 + npeers is the maximum achievable count (local +1
     * plus every peer ACK).  It passes fanout's fail-fast check (which
     * rejects unreachable quorums) and still forces the loop to wait
     * for every worker since done_count >= nreplicas exits the loop
     * regardless of ack_count.  Best-effort result. */
    int acks = 0;
    (void)fanout_wait_quorum_ex(rmgr, payload, (uint32_t)plen,
                                peers, npeers,
                                1 + npeers, TSDB_RPC_APPLY_TRUNCATE,
                                &acks);
    /* fanout_wait_quorum_ex counts local as +1; subtract to get peer acks. */
    int peer_acks = acks - 1;
    if (peer_acks < 0) peer_acks = 0;
    if (out_acked_peers) *out_acked_peers = peer_acks;
    return TSDB_OK;
}

int tsdb_replica_broadcast_catalog_qtl(tsdb_replica_mgr_t *rmgr,
                                        const char *qtl,
                                        const tsdb_node_id_t *peers, int npeers,
                                        int *out_acked_peers)
{
    if (out_acked_peers) *out_acked_peers = 0;
    if (!rmgr || !qtl || npeers <= 0) return TSDB_OK;

    size_t qlen = strlen(qtl);
    /* 4 B length prefix + statement body.  Catalog statements are short
     * (CREATE VTABLE ... cols ... tags) so 8 KB is ample even with a
     * generous margin. */
    size_t cap = 16u + qlen;
    uint8_t *payload = malloc(cap);
    if (!payload) return TSDB_ERR_NOMEM;
    int plen = tsdb_rpc_encode_catalog_qtl(payload, (uint32_t)cap, qtl);
    if (plen < 0) { free(payload); return TSDB_ERR_INTERNAL; }

    int acks = 0;
    (void)fanout_wait_quorum_ex(rmgr, payload, (uint32_t)plen,
                                peers, npeers,
                                1 + npeers, TSDB_RPC_APPLY_CATALOG_QTL,
                                &acks);
    int peer_acks = acks - 1;
    if (peer_acks < 0) peer_acks = 0;
    if (out_acked_peers) *out_acked_peers = peer_acks;
    return TSDB_OK;
}

int tsdb_replica_broadcast_delete_range(tsdb_replica_mgr_t *rmgr,
                                         const char *table_name,
                                         int64_t cutoff_ns,
                                         int op_lt, int inclusive,
                                         const tsdb_node_id_t *peers, int npeers,
                                         int *out_acked_peers)
{
    if (out_acked_peers) *out_acked_peers = 0;
    if (!rmgr || !table_name || npeers <= 0) return TSDB_OK;

    uint8_t stack_buf[256];
    int plen = tsdb_rpc_encode_delete_range(stack_buf, sizeof(stack_buf),
                                            table_name, cutoff_ns,
                                            op_lt, inclusive);
    if (plen < 0) return TSDB_ERR_INTERNAL;

    uint8_t *payload = malloc((size_t)plen);
    if (!payload) return TSDB_ERR_NOMEM;
    memcpy(payload, stack_buf, (size_t)plen);

    int acks = 0;
    (void)fanout_wait_quorum_ex(rmgr, payload, (uint32_t)plen,
                                peers, npeers,
                                1 + npeers, TSDB_RPC_APPLY_DELETE_RANGE,
                                &acks);
    int peer_acks = acks - 1;
    if (peer_acks < 0) peer_acks = 0;
    if (out_acked_peers) *out_acked_peers = peer_acks;
    return TSDB_OK;
}

#ifdef TSDB_TEST
/* Non-static shims so tests/test_replica_backoff.c can exercise the
 * connection-stability helpers above without #include-ing this whole TU
 * (which would clash with the libtsdb objects the test also links). */
int  replica_peer_dialable_test(tsdb_node_state_t state) {
    return replica_peer_dialable(state);
}
long replica_backoff_ns_test(int attempt) {
    return replica_backoff_ns(attempt);
}
#endif /* TSDB_TEST */
