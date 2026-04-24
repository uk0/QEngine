/* dr_forwarder.c — see dr_forwarder.h. */

#include "dr_forwarder.h"

#include "../../include/tsdb.h"
#include "../cluster/rpc.h"
#include "../server/metrics.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Ring buffer ───────────────────────────────────────────────────── */

typedef struct dr_item {
    uint8_t *payload;
    size_t   len;
} dr_item_t;

struct tsdb_dr_forwarder {
    char             remote[128];
    dr_item_t       *ring;
    int              cap;
    int              head;       /* next write slot */
    int              tail;       /* next read slot */
    int              size;       /* items currently in ring */
    pthread_mutex_t  mu;
    pthread_cond_t   cv;
    pthread_t        thread;
    int              running;    /* 0 signals the drain thread to exit */

    _Atomic uint64_t enqueued;
    _Atomic uint64_t sent;
    _Atomic uint64_t ack;
    _Atomic uint64_t fail;
    _Atomic uint64_t dropped;
    _Atomic int      remote_alive;
};

/* ── Background drain thread ───────────────────────────────────────── */

static void sleep_ms(int ms) {
    struct timespec ts = { .tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
}

static void *drain_thread_main(void *arg) {
    tsdb_dr_forwarder_t *fw = (tsdb_dr_forwarder_t *)arg;
    int backoff_ms = 200;      /* exponential backoff ceiling */

    while (fw->running) {
        /* Pop one item. */
        dr_item_t item;
        int have = 0;
        pthread_mutex_lock(&fw->mu);
        while (fw->running && fw->size == 0)
            pthread_cond_wait(&fw->cv, &fw->mu);
        if (fw->size > 0) {
            item = fw->ring[fw->tail];
            fw->tail = (fw->tail + 1) % fw->cap;
            fw->size--;
            have = 1;
        }
        pthread_mutex_unlock(&fw->mu);
        if (!fw->running) break;
        if (!have) continue;

        /* Try to send.  rpc_dial is blocking with a small timeout,
         * rpc_call retries on bad connections once.  If the remote is
         * unreachable we bounce-back with exponential backoff to avoid
         * hammering a downed DC. */
        tsdb_rpc_conn_t *conn = tsdb_rpc_connect(fw->remote, 2000);
        int rc = conn ? TSDB_OK : TSDB_ERR_IO;
        if (!conn) {
            atomic_store_explicit(&fw->remote_alive, 0, memory_order_relaxed);
            atomic_fetch_add_explicit(&fw->fail, 1, memory_order_relaxed);
            tsdb_metric_inc("qengine_dr_fail_total");
            /* Re-queue at head — simplest way to preserve order until
             * the remote recovers.  Caller dropped new inserts while we
             * were backed off, so ring never grows unbounded. */
            pthread_mutex_lock(&fw->mu);
            int slot = (fw->tail - 1 + fw->cap) % fw->cap;
            fw->ring[slot] = item;
            fw->tail = slot;
            fw->size++;
            pthread_mutex_unlock(&fw->mu);

            sleep_ms(backoff_ms);
            if (backoff_ms < 5000) backoff_ms *= 2;
            continue;
        }
        backoff_ms = 200;
        atomic_store_explicit(&fw->remote_alive, 1, memory_order_relaxed);

        atomic_fetch_add_explicit(&fw->sent, 1, memory_order_relaxed);
        tsdb_metric_inc("qengine_dr_sent_total");
        rc = tsdb_rpc_call(conn, TSDB_RPC_FED_INGEST, item.payload, (uint32_t)item.len);
        if (rc == TSDB_OK) {
            atomic_fetch_add_explicit(&fw->ack, 1, memory_order_relaxed);
            tsdb_metric_inc("qengine_dr_ack_total");
        } else {
            atomic_fetch_add_explicit(&fw->fail, 1, memory_order_relaxed);
            tsdb_metric_inc("qengine_dr_fail_total");
        }
        tsdb_rpc_conn_close(conn);
        free(item.payload);
    }

    /* Drain on shutdown — free queued items. */
    pthread_mutex_lock(&fw->mu);
    while (fw->size > 0) {
        free(fw->ring[fw->tail].payload);
        fw->tail = (fw->tail + 1) % fw->cap;
        fw->size--;
    }
    pthread_mutex_unlock(&fw->mu);
    return NULL;
}

/* ── Public API ────────────────────────────────────────────────────── */

int tsdb_dr_forwarder_new(const char *remote_addr, int ring_cap,
                           tsdb_dr_forwarder_t **out) {
    if (!remote_addr || !*remote_addr || !out) return TSDB_ERR_INVAL;
    if (ring_cap < 16) ring_cap = 16;

    tsdb_dr_forwarder_t *fw = calloc(1, sizeof(*fw));
    if (!fw) return TSDB_ERR_NOMEM;
    snprintf(fw->remote, sizeof(fw->remote), "%s", remote_addr);
    fw->ring = calloc((size_t)ring_cap, sizeof(dr_item_t));
    if (!fw->ring) { free(fw); return TSDB_ERR_NOMEM; }
    fw->cap = ring_cap;
    pthread_mutex_init(&fw->mu, NULL);
    pthread_cond_init(&fw->cv, NULL);
    fw->running = 1;
    if (pthread_create(&fw->thread, NULL, drain_thread_main, fw) != 0) {
        pthread_mutex_destroy(&fw->mu);
        pthread_cond_destroy(&fw->cv);
        free(fw->ring);
        free(fw);
        return TSDB_ERR_INTERNAL;
    }
    fprintf(stderr, "[dr] forwarder started → %s (ring=%d)\n",
            fw->remote, fw->cap);
    *out = fw;
    return TSDB_OK;
}

void tsdb_dr_forwarder_free(tsdb_dr_forwarder_t *fw) {
    if (!fw) return;
    pthread_mutex_lock(&fw->mu);
    fw->running = 0;
    pthread_cond_broadcast(&fw->cv);
    pthread_mutex_unlock(&fw->mu);
    pthread_join(fw->thread, NULL);
    pthread_mutex_destroy(&fw->mu);
    pthread_cond_destroy(&fw->cv);
    free(fw->ring);
    free(fw);
}

int tsdb_dr_forwarder_enqueue(tsdb_dr_forwarder_t *fw,
                               const uint8_t *payload, size_t len) {
    if (!fw || !payload || len == 0) return TSDB_ERR_INVAL;

    /* Copy the payload so the caller can free theirs. */
    uint8_t *copy = malloc(len);
    if (!copy) {
        atomic_fetch_add_explicit(&fw->dropped, 1, memory_order_relaxed);
        tsdb_metric_inc("qengine_dr_dropped_total");
        return TSDB_ERR_NOMEM;
    }
    memcpy(copy, payload, len);

    pthread_mutex_lock(&fw->mu);
    if (fw->size >= fw->cap) {
        pthread_mutex_unlock(&fw->mu);
        free(copy);
        atomic_fetch_add_explicit(&fw->dropped, 1, memory_order_relaxed);
        tsdb_metric_inc("qengine_dr_dropped_total");
        return TSDB_ERR_NOMEM;
    }
    fw->ring[fw->head].payload = copy;
    fw->ring[fw->head].len = len;
    fw->head = (fw->head + 1) % fw->cap;
    fw->size++;
    atomic_fetch_add_explicit(&fw->enqueued, 1, memory_order_relaxed);
    pthread_cond_signal(&fw->cv);
    pthread_mutex_unlock(&fw->mu);
    return TSDB_OK;
}

void tsdb_dr_forwarder_stats(tsdb_dr_forwarder_t *fw, tsdb_dr_stats_t *out) {
    if (!fw || !out) return;
    out->enqueued     = atomic_load_explicit(&fw->enqueued, memory_order_relaxed);
    out->sent         = atomic_load_explicit(&fw->sent,     memory_order_relaxed);
    out->ack          = atomic_load_explicit(&fw->ack,      memory_order_relaxed);
    out->fail         = atomic_load_explicit(&fw->fail,     memory_order_relaxed);
    out->dropped      = atomic_load_explicit(&fw->dropped,  memory_order_relaxed);
    out->remote_alive = atomic_load_explicit(&fw->remote_alive, memory_order_relaxed);
    pthread_mutex_lock(&fw->mu);
    out->backlog = fw->size;
    pthread_mutex_unlock(&fw->mu);
}
