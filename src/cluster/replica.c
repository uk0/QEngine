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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>

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
    return r;
}

void tsdb_replica_mgr_free(tsdb_replica_mgr_t *rmgr) {
    if (!rmgr) return;
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

static void *fanout_worker(void *arg) {
    worker_arg_t *wa = (worker_arg_t *)arg;
    fanout_ctx_t *ctx = wa->ctx;
    tsdb_node_id_t nid = wa->node_id;
    free(wa);

    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(ctx->rmgr, nid);
    int rc = TSDB_ERR_IO;
    if (conn) {
        rc = tsdb_rpc_call(conn, ctx->rpc_kind, ctx->payload, ctx->payload_len);
        if (rc != TSDB_OK) evict_one_conn(ctx->rmgr, nid, conn);
    }
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
            continue;
        }
        wa->ctx     = ctx;
        wa->node_id = replicas[i];

        pthread_t tid;
        if (pthread_create(&tid, &attr, fanout_worker, wa) == 0) {
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
        }
    }
    pthread_attr_destroy(&attr);
    (void)launched;

    /* Wait for quorum OR for every worker to finish (whichever first). */
    pthread_mutex_lock(&ctx->mu);
    while (ctx->ack_count < ctx->quorum && ctx->done_count < ctx->nreplicas) {
        pthread_cond_wait(&ctx->cv, &ctx->mu);
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

    /* Encode the WRITE_BATCH payload once. */
    size_t row_size = 0;
    for (int c = 0; c < ncols; c++) {
        row_size += (col_types[c] == TSDB_TYPE_SYMBOL) ? 4 : 8;
    }
    size_t payload_cap = 8 + ncols * 2 + (size_t)nrows * row_size + 64;
    uint8_t *payload = malloc(payload_cap);
    if (!payload) return TSDB_ERR_NOMEM;

    int plen = tsdb_rpc_encode_write_batch(payload, (uint32_t)payload_cap,
                                           table_name,
                                           ncols, col_types,
                                           nrows, col_data);
    if (plen < 0) { free(payload); return TSDB_ERR_INTERNAL; }

    return fanout_wait_quorum(rmgr, payload, (uint32_t)plen,
                              replicas, nreplicas,
                              w_quorum, TSDB_RPC_WRITE_BATCH);
}

/* ---- Schema sync --------------------------------------------------------- */

int tsdb_replica_sync_schema(tsdb_replica_mgr_t *rmgr,
                              const char *table_name,
                              int ncols, const char **col_names,
                              const int *col_types, int ts_col_idx,
                              const tsdb_node_id_t *nodes, int nnodes,
                              int quorum)
{
    if (!rmgr || nnodes == 0) return TSDB_OK;

    uint8_t stack_buf[2048];
    int plen = tsdb_rpc_encode_schema(stack_buf, sizeof(stack_buf),
                                      table_name, ncols, col_names,
                                      col_types, ts_col_idx);
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
