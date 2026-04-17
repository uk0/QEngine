/* replica.c — synchronous quorum replication implementation. */

#include "replica.h"
#include "node.h"
#include "rpc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

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

/* Per-replica send argument for concurrent fan-out. */
typedef struct {
    tsdb_replica_mgr_t *rmgr;
    tsdb_node_id_t      node_id;
    uint8_t            *payload;
    uint32_t            payload_len;
    volatile int       *ack_count;
    pthread_mutex_t    *ack_lock;
} send_arg_t;

static void *send_to_replica(void *arg) {
    send_arg_t *sa = (send_arg_t *)arg;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(sa->rmgr, sa->node_id);
    if (!conn) {
        free(sa);
        return NULL;
    }
    int rc = tsdb_rpc_call(conn, TSDB_RPC_WRITE_BATCH, sa->payload, sa->payload_len);
    if (rc == TSDB_OK) {
        pthread_mutex_lock(sa->ack_lock);
        (*sa->ack_count)++;
        pthread_mutex_unlock(sa->ack_lock);
    } else {
        /* Evict bad connection so next call reconnects. */
        evict_conn(sa->rmgr, sa->node_id);
    }
    free(sa);
    return NULL;
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
    /* Estimate buffer size. */
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

    /* Local write already done; ack_count starts at 1. */
    volatile int ack_count = 1;
    pthread_mutex_t ack_lock;
    pthread_mutex_init(&ack_lock, NULL);

    /* Fan out to replicas concurrently. */
    pthread_t tids[TSDB_CLUSTER_MAX_NODES];
    int nthreads = 0;

    for (int i = 0; i < nreplicas && nthreads < TSDB_CLUSTER_MAX_NODES; i++) {
        send_arg_t *sa = malloc(sizeof(*sa));
        if (!sa) continue;
        sa->rmgr        = rmgr;
        sa->node_id     = replicas[i];
        sa->payload     = payload;
        sa->payload_len = (uint32_t)plen;
        sa->ack_count   = &ack_count;
        sa->ack_lock    = &ack_lock;

        if (pthread_create(&tids[nthreads], NULL, send_to_replica, sa) == 0)
            nthreads++;
        else
            free(sa);
    }

    /* Wait for all threads then check quorum. */
    for (int i = 0; i < nthreads; i++) pthread_join(tids[i], NULL);

    pthread_mutex_destroy(&ack_lock);
    free(payload);

    return (ack_count >= w_quorum) ? TSDB_OK : TSDB_ERR_IO;
}

/* ---- Schema sync --------------------------------------------------------- */

typedef struct {
    tsdb_replica_mgr_t *rmgr;
    tsdb_node_id_t      node_id;
    uint8_t            *payload;
    uint32_t            payload_len;
    volatile int       *ack_count;
    pthread_mutex_t    *ack_lock;
} schema_arg_t;

static void *send_schema(void *arg) {
    schema_arg_t *sa = (schema_arg_t *)arg;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(sa->rmgr, sa->node_id);
    if (!conn) { free(sa); return NULL; }

    int rc = tsdb_rpc_call(conn, TSDB_RPC_SCHEMA_SYNC, sa->payload, sa->payload_len);
    if (rc == TSDB_OK) {
        pthread_mutex_lock(sa->ack_lock);
        (*sa->ack_count)++;
        pthread_mutex_unlock(sa->ack_lock);
    } else {
        evict_conn(sa->rmgr, sa->node_id);
    }
    free(sa);
    return NULL;
}

int tsdb_replica_sync_schema(tsdb_replica_mgr_t *rmgr,
                              const char *table_name,
                              int ncols, const char **col_names,
                              const int *col_types, int ts_col_idx,
                              const tsdb_node_id_t *nodes, int nnodes,
                              int quorum)
{
    if (!rmgr || nnodes == 0) return TSDB_OK;

    uint8_t payload[2048];
    int plen = tsdb_rpc_encode_schema(payload, sizeof(payload),
                                      table_name, ncols, col_names,
                                      col_types, ts_col_idx);
    if (plen < 0) return TSDB_ERR_INTERNAL;

    volatile int ack_count = 1; /* local is implicitly done */
    pthread_mutex_t ack_lock;
    pthread_mutex_init(&ack_lock, NULL);

    pthread_t tids[TSDB_CLUSTER_MAX_NODES];
    int nthreads = 0;

    for (int i = 0; i < nnodes && nthreads < TSDB_CLUSTER_MAX_NODES; i++) {
        schema_arg_t *sa = malloc(sizeof(*sa));
        if (!sa) continue;
        sa->rmgr        = rmgr;
        sa->node_id     = nodes[i];
        sa->payload     = malloc(plen);
        if (!sa->payload) { free(sa); continue; }
        memcpy(sa->payload, payload, plen);
        sa->payload_len = (uint32_t)plen;
        sa->ack_count   = &ack_count;
        sa->ack_lock    = &ack_lock;

        if (pthread_create(&tids[nthreads], NULL, send_schema, sa) == 0)
            nthreads++;
        else { free(sa->payload); free(sa); }
    }

    for (int i = 0; i < nthreads; i++) pthread_join(tids[i], NULL);
    pthread_mutex_destroy(&ack_lock);

    return (ack_count >= quorum) ? TSDB_OK : TSDB_ERR_IO;
}
