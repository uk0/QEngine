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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>

/* ---- Connection pool ----------------------------------------------------- */

#define MAX_CONN_POOL 64

typedef struct {
    tsdb_node_id_t  node_id;
    tsdb_rpc_conn_t *conn;
} conn_entry_t;

struct tsdb_replica_mgr {
    tsdb_node_manager_t *node_mgr;
    pthread_mutex_t      lock;
    conn_entry_t         pool[MAX_CONN_POOL];
    int                  nconns;
};

tsdb_replica_mgr_t *tsdb_replica_mgr_new(tsdb_node_manager_t *node_mgr) {
    tsdb_replica_mgr_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;
    r->node_mgr = node_mgr;
    pthread_mutex_init(&r->lock, NULL);
    return r;
}

void tsdb_replica_mgr_free(tsdb_replica_mgr_t *rmgr) {
    if (!rmgr) return;
    for (int i = 0; i < rmgr->nconns; i++) {
        if (rmgr->pool[i].conn) tsdb_rpc_conn_close(rmgr->pool[i].conn);
    }
    pthread_mutex_destroy(&rmgr->lock);
    free(rmgr);
}

tsdb_rpc_conn_t *tsdb_replica_mgr_get_conn(tsdb_replica_mgr_t *rmgr,
                                             tsdb_node_id_t node_id)
{
    if (!rmgr) return NULL;
    pthread_mutex_lock(&rmgr->lock);

    /* Look up existing connection. */
    for (int i = 0; i < rmgr->nconns; i++) {
        if (rmgr->pool[i].node_id == node_id && rmgr->pool[i].conn) {
            tsdb_rpc_conn_t *c = rmgr->pool[i].conn;
            pthread_mutex_unlock(&rmgr->lock);
            return c;
        }
    }

    /* Need to create new connection. */
    tsdb_node_info_t info = {0};
    if (tsdb_node_manager_get(rmgr->node_mgr, node_id, &info) < 0) {
        pthread_mutex_unlock(&rmgr->lock);
        return NULL;
    }

    if (rmgr->nconns >= MAX_CONN_POOL) {
        pthread_mutex_unlock(&rmgr->lock);
        return NULL;
    }

    pthread_mutex_unlock(&rmgr->lock);

    /* Connect outside lock to avoid blocking other threads. */
    tsdb_rpc_conn_t *conn = tsdb_rpc_connect(info.addr, 2000);
    if (!conn) return NULL;

    pthread_mutex_lock(&rmgr->lock);
    /* Check if another thread already inserted while we were connecting. */
    for (int i = 0; i < rmgr->nconns; i++) {
        if (rmgr->pool[i].node_id == node_id) {
            /* Use the one already inserted; discard ours. */
            tsdb_rpc_conn_close(conn);
            conn = rmgr->pool[i].conn;
            pthread_mutex_unlock(&rmgr->lock);
            return conn;
        }
    }

    if (rmgr->nconns < MAX_CONN_POOL) {
        rmgr->pool[rmgr->nconns].node_id = node_id;
        rmgr->pool[rmgr->nconns].conn    = conn;
        rmgr->nconns++;
    }
    pthread_mutex_unlock(&rmgr->lock);
    return conn;
}

/* Remove a dead connection from the pool. */
static void evict_conn(tsdb_replica_mgr_t *rmgr, tsdb_node_id_t node_id) {
    pthread_mutex_lock(&rmgr->lock);
    for (int i = 0; i < rmgr->nconns; i++) {
        if (rmgr->pool[i].node_id == node_id) {
            if (rmgr->pool[i].conn) tsdb_rpc_conn_close(rmgr->pool[i].conn);
            rmgr->pool[i] = rmgr->pool[--rmgr->nconns];
            break;
        }
    }
    pthread_mutex_unlock(&rmgr->lock);
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
        if (rc != TSDB_OK) evict_conn(ctx->rmgr, nid);
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
 */
static int fanout_wait_quorum(tsdb_replica_mgr_t *rmgr,
                              uint8_t *payload, uint32_t plen,
                              const tsdb_node_id_t *replicas, int nreplicas,
                              int quorum, int rpc_kind)
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

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    int launched = 0;
    for (int i = 0; i < nreplicas; i++) {
        worker_arg_t *wa = malloc(sizeof(*wa));
        if (!wa) continue;
        wa->ctx     = ctx;
        wa->node_id = replicas[i];

        pthread_t tid;
        if (pthread_create(&tid, &attr, fanout_worker, wa) == 0) {
            launched++;
        } else {
            /* Worker never ran — release its pre-reserved ref and record
             * the slot as done so the waiter isn't stuck waiting for it. */
            free(wa);
            pthread_mutex_lock(&ctx->mu);
            ctx->done_count++;
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
    pthread_mutex_unlock(&ctx->mu);

    fanout_ctx_unref(ctx);  /* drop caller's ref; workers may outlive us */
    return ok ? TSDB_OK : TSDB_ERR_IO;
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
