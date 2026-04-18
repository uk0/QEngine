/* autobalance.c — Auto-balance controller implementation. */

#define _POSIX_C_SOURCE 200809L

#include "autobalance.h"
#include "node.h"
#include "hashring.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <math.h>

/* ---- Configuration -------------------------------------------------------- */

#define DEFAULT_ALPHA         0.6   /* writes weight */
#define DEFAULT_BETA          0.4   /* storage weight */
#define DEFAULT_DAMPEN        0.5   /* fraction of score applied */
#define DEFAULT_INTERVAL_MS   30000 /* 30 seconds */

static double env_double(const char *name, double def) {
    const char *v = getenv(name);
    if (!v) return def;
    char *end;
    double d = strtod(v, &end);
    return (end != v) ? d : def;
}

static int env_int(const char *name, int def) {
    const char *v = getenv(name);
    if (!v) return def;
    char *end;
    long l = strtol(v, &end, 10);
    return (end != v) ? (int)l : def;
}

/* ---- Helpers -------------------------------------------------------------- */

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

/* Compute total bytes under a directory tree (non-recursive, one level). */
static uint64_t dir_bytes(const char *path) {
    if (!path || path[0] == '\0') return 0;
    DIR *d = opendir(path);
    if (!d) return 0;
    uint64_t total = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char full[4096];
        snprintf(full, sizeof(full), "%s/%s", path, de->d_name);
        struct stat st;
        if (stat(full, &st) == 0) total += (uint64_t)st.st_size;
    }
    closedir(d);
    return total;
}

/* ---- Per-node load table ------------------------------------------------- */

#define LOAD_TABLE_MAX  64

typedef struct {
    tsdb_node_load_t  loads[LOAD_TABLE_MAX];
    int               nloads;
    pthread_mutex_t   lock;
} load_table_t;

static void lt_init(load_table_t *lt) {
    memset(lt, 0, sizeof(*lt));
    pthread_mutex_init(&lt->lock, NULL);
}

static void lt_destroy(load_table_t *lt) {
    pthread_mutex_destroy(&lt->lock);
}

/* Upsert a load record; always replaces if node_id matches. */
static void lt_upsert(load_table_t *lt, const tsdb_node_load_t *r) {
    pthread_mutex_lock(&lt->lock);
    for (int i = 0; i < lt->nloads; i++) {
        if (lt->loads[i].node_id == r->node_id) {
            lt->loads[i] = *r;
            pthread_mutex_unlock(&lt->lock);
            return;
        }
    }
    if (lt->nloads < LOAD_TABLE_MAX)
        lt->loads[lt->nloads++] = *r;
    pthread_mutex_unlock(&lt->lock);
}

/* ---- Main struct ---------------------------------------------------------- */

struct tsdb_autobalance {
    tsdb_node_manager_t *node_mgr;
    tsdb_node_id_t       local_id;
    char                 data_dir[4096];

    /* Config. */
    double   alpha;
    double   beta;
    double   dampen;
    int      interval_ms;

    /* Local write counter (updated by record_write). */
    atomic_uint_fast64_t write_counter;
    uint64_t             write_counter_prev;
    int64_t              last_ema_ts_ms;
    double               ema_writes_sec;    /* EMA of rows/sec */

    /* Load table (self + peers via gossip). */
    load_table_t load_table;

    /* Min-at-VN consecutive cycles tracker (per node). */
    int min_cycles[TSDB_CLUSTER_MAX_NODES]; /* parallel to node_mgr nodes */

    /* Background thread. */
    pthread_t        thread;
    volatile int     running;
    pthread_mutex_t  cycle_mu; /* serialises manual trigger + done signal */
    pthread_cond_t   cycle_cv; /* wakes thread to rebalance */
    pthread_cond_t   done_cv;  /* thread signals here when cycle completes */
    volatile int     cycle_requested;
    volatile int     cycle_done;
};

/* ---- Wire encode / decode ------------------------------------------------- */

/* 28-byte LE record: node_id(8) writes_sec(8) storage_bytes(8) cpu_pct(4) */
int tsdb_autobalance_encode_load(tsdb_autobalance_t *ab,
                                  uint8_t *buf, int cap)
{
    if (!ab || !buf || cap < TSDB_NODE_LOAD_WIRE_SIZE) return -1;

    /* Compute current storage bytes on the fly. */
    uint64_t storage = dir_bytes(ab->data_dir);

    /* Build record. */
    uint64_t writes_sec = (uint64_t)(ab->ema_writes_sec < 0 ? 0 : ab->ema_writes_sec);
    uint32_t cpu = 0; /* placeholder — real CPU measurement would use getrusage */

    uint8_t *p = buf;
    memcpy(p, &ab->local_id,  8); p += 8;
    memcpy(p, &writes_sec,    8); p += 8;
    memcpy(p, &storage,       8); p += 8;
    memcpy(p, &cpu,           4); p += 4;
    return (int)(p - buf);
}

void tsdb_autobalance_merge_load(tsdb_autobalance_t *ab,
                                  const uint8_t *buf, int len)
{
    if (!ab || !buf || len < TSDB_NODE_LOAD_WIRE_SIZE) return;
    int nrecs = len / TSDB_NODE_LOAD_WIRE_SIZE;
    for (int i = 0; i < nrecs; i++) {
        const uint8_t *p = buf + i * TSDB_NODE_LOAD_WIRE_SIZE;
        tsdb_node_load_t r = {0};
        memcpy(&r.node_id,      p,      8); p += 8;
        memcpy(&r.writes_sec,   p,      8); p += 8;
        memcpy(&r.storage_bytes,p,      8); p += 8;
        memcpy(&r.cpu_pct_x100, p,      4);
        lt_upsert(&ab->load_table, &r);
    }
}

/* ---- EMA update ---------------------------------------------------------- */

/*
 * Update the local exponential moving average of writes/sec.
 * α_ema = 0.2 (smoothing factor, 20% weight on newest sample).
 */
static void update_ema(tsdb_autobalance_t *ab) {
    int64_t now = now_ms();
    int64_t dt_ms = now - ab->last_ema_ts_ms;
    if (dt_ms <= 0) return;

    uint64_t cur = atomic_load(&ab->write_counter);
    uint64_t delta = cur - ab->write_counter_prev;
    ab->write_counter_prev = cur;
    ab->last_ema_ts_ms = now;

    double instant = (double)delta / ((double)dt_ms / 1000.0);
    double alpha_ema = 0.2;
    ab->ema_writes_sec = alpha_ema * instant + (1.0 - alpha_ema) * ab->ema_writes_sec;
}

/* ---- Rebalance cycle ------------------------------------------------------ */

static void do_rebalance(tsdb_autobalance_t *ab) {
    update_ema(ab);

    /* Upsert our own load into the table. */
    {
        uint8_t self_buf[TSDB_NODE_LOAD_WIRE_SIZE];
        if (tsdb_autobalance_encode_load(ab, self_buf, sizeof(self_buf)) > 0)
            tsdb_autobalance_merge_load(ab, self_buf, TSDB_NODE_LOAD_WIRE_SIZE);
    }

    /* Snapshot current alive nodes. */
    tsdb_node_info_t snap[TSDB_CLUSTER_MAX_NODES];
    int nsnap = tsdb_node_manager_snapshot(ab->node_mgr, snap, TSDB_CLUSTER_MAX_NODES);
    if (nsnap <= 1) return; /* single node, nothing to balance */

    /* Collect load records for alive nodes only. */
    pthread_mutex_lock(&ab->load_table.lock);

    double writes_arr[TSDB_CLUSTER_MAX_NODES];
    double storage_arr[TSDB_CLUSTER_MAX_NODES];
    int    node_idx[TSDB_CLUSTER_MAX_NODES];
    int    nactive = 0;

    for (int i = 0; i < nsnap; i++) {
        if (snap[i].state != TSDB_NODE_ALIVE) continue;
        writes_arr[nactive]  = 0;
        storage_arr[nactive] = 0;
        node_idx[nactive]    = i;

        /* Find load entry. */
        for (int j = 0; j < ab->load_table.nloads; j++) {
            if (ab->load_table.loads[j].node_id == snap[i].id) {
                writes_arr[nactive]  = (double)ab->load_table.loads[j].writes_sec;
                storage_arr[nactive] = (double)ab->load_table.loads[j].storage_bytes;
                break;
            }
        }
        nactive++;
    }
    pthread_mutex_unlock(&ab->load_table.lock);

    if (nactive <= 1) return;

    /* Normalise each metric to [0, 1]. */
    double max_w = 1.0, max_s = 1.0;
    for (int i = 0; i < nactive; i++) {
        if (writes_arr[i]  > max_w) max_w = writes_arr[i];
        if (storage_arr[i] > max_s) max_s = storage_arr[i];
    }

    /* Compute target VN counts and apply. */
    double base = (double)TSDB_BALANCE_VN_BASE;
    for (int i = 0; i < nactive; i++) {
        double wn  = writes_arr[i]  / max_w;   /* [0,1] */
        double sn  = storage_arr[i] / max_s;   /* [0,1] */
        double score = ab->alpha * wn + ab->beta * sn;
        /* High score → overloaded → fewer VNs. */
        int target = (int)(base * (1.0 - ab->dampen * score) + 0.5);
        if (target < TSDB_BALANCE_VN_MIN) target = TSDB_BALANCE_VN_MIN;
        if (target > TSDB_BALANCE_VN_MAX) target = TSDB_BALANCE_VN_MAX;

        tsdb_node_id_t nid = snap[node_idx[i]].id;
        tsdb_hashring_t *ring = tsdb_node_manager_ring(ab->node_mgr);
        int cur_vn = tsdb_hashring_get_weight(ring, nid);

        if (cur_vn != target) {
            tsdb_hashring_set_weight(ring, nid, target);
            if (getenv("TSDB_LOG_AUTOBALANCE")) {
                fprintf(stderr, "[autobalance] node %llu: VN %d → %d  (score=%.3f)\n",
                        (unsigned long long)nid, cur_vn, target, score);
            }
        }
    }
}

/* ---- Background thread --------------------------------------------------- */

static void *balance_thread(void *arg) {
    tsdb_autobalance_t *ab = (tsdb_autobalance_t *)arg;
    pthread_mutex_lock(&ab->cycle_mu);
    while (ab->running) {
        /* Wait for interval_ms or manual trigger. */
        if (!ab->cycle_requested) {
            struct timespec deadline;
            clock_gettime(CLOCK_REALTIME, &deadline);
            long add_ns = (long)ab->interval_ms * 1000000L;
            deadline.tv_sec  += add_ns / 1000000000L;
            deadline.tv_nsec += add_ns % 1000000000L;
            if (deadline.tv_nsec >= 1000000000L) {
                deadline.tv_sec++;
                deadline.tv_nsec -= 1000000000L;
            }
            pthread_cond_timedwait(&ab->cycle_cv, &ab->cycle_mu, &deadline);
        }
        if (!ab->running) break;
        ab->cycle_requested = 0;
        /* Release lock during heavy work so rebalance_now can signal. */
        pthread_mutex_unlock(&ab->cycle_mu);
        do_rebalance(ab);
        pthread_mutex_lock(&ab->cycle_mu);
        ab->cycle_done = 1;
        pthread_cond_broadcast(&ab->done_cv);
    }
    pthread_mutex_unlock(&ab->cycle_mu);
    return NULL;
}

/* ---- Public API ---------------------------------------------------------- */

tsdb_autobalance_t *tsdb_autobalance_new(tsdb_node_manager_t *node_mgr,
                                          tsdb_node_id_t local_id,
                                          const char *db_data_dir)
{
    if (!node_mgr) return NULL;

    tsdb_autobalance_t *ab = calloc(1, sizeof(*ab));
    if (!ab) return NULL;

    ab->node_mgr = node_mgr;
    ab->local_id = local_id;
    if (db_data_dir)
        snprintf(ab->data_dir, sizeof(ab->data_dir), "%s", db_data_dir);

    ab->alpha       = env_double("TSDB_BALANCE_ALPHA",       DEFAULT_ALPHA);
    ab->beta        = env_double("TSDB_BALANCE_BETA",        DEFAULT_BETA);
    ab->dampen      = env_double("TSDB_BALANCE_DAMPEN",      DEFAULT_DAMPEN);
    ab->interval_ms = env_int  ("TSDB_BALANCE_INTERVAL_MS",  DEFAULT_INTERVAL_MS);

    atomic_init(&ab->write_counter, 0);
    ab->last_ema_ts_ms = now_ms();

    lt_init(&ab->load_table);
    pthread_mutex_init(&ab->cycle_mu, NULL);
    pthread_cond_init (&ab->cycle_cv, NULL);
    pthread_cond_init (&ab->done_cv,  NULL);

    ab->running = 1;
    if (pthread_create(&ab->thread, NULL, balance_thread, ab) != 0) {
        lt_destroy(&ab->load_table);
        pthread_mutex_destroy(&ab->cycle_mu);
        pthread_cond_destroy (&ab->cycle_cv);
        free(ab);
        return NULL;
    }

    return ab;
}

void tsdb_autobalance_free(tsdb_autobalance_t *ab) {
    if (!ab) return;
    pthread_mutex_lock(&ab->cycle_mu);
    ab->running = 0;
    pthread_cond_signal(&ab->cycle_cv);
    pthread_mutex_unlock(&ab->cycle_mu);
    pthread_join(ab->thread, NULL);

    lt_destroy(&ab->load_table);
    pthread_mutex_destroy(&ab->cycle_mu);
    pthread_cond_destroy (&ab->cycle_cv);
    pthread_cond_destroy (&ab->done_cv);
    free(ab);
}

void tsdb_autobalance_record_write(tsdb_autobalance_t *ab, uint64_t row_count) {
    if (!ab) return;
    atomic_fetch_add(&ab->write_counter, row_count);
}

int tsdb_autobalance_rebalance_now(tsdb_autobalance_t *ab) {
    if (!ab) return -1;
    pthread_mutex_lock(&ab->cycle_mu);
    ab->cycle_requested = 1;
    ab->cycle_done = 0;
    pthread_cond_signal(&ab->cycle_cv);
    /* Wait until the cycle completes (with 2s timeout). */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 2;
    while (!ab->cycle_done && ab->running) {
        int rc = pthread_cond_timedwait(&ab->done_cv, &ab->cycle_mu, &deadline);
        if (rc != 0) break; /* timeout or error */
    }
    pthread_mutex_unlock(&ab->cycle_mu);
    return 0;
}

int tsdb_autobalance_stats_str(tsdb_autobalance_t *ab, char *buf, int cap) {
    if (!ab || !buf || cap <= 0) return 0;

    update_ema(ab);

    pthread_mutex_lock(&ab->load_table.lock);
    int n = 0;
    n += snprintf(buf + n, (size_t)(cap - n),
                  "{\"local_id\":%llu,\"ema_writes_sec\":%.1f,\"interval_ms\":%d"
                  ",\"alpha\":%.2f,\"beta\":%.2f,\"dampen\":%.2f,\"nodes\":[",
                  (unsigned long long)ab->local_id,
                  ab->ema_writes_sec,
                  ab->interval_ms,
                  ab->alpha, ab->beta, ab->dampen);

    tsdb_hashring_t *ring = tsdb_node_manager_ring(ab->node_mgr);
    for (int i = 0; i < ab->load_table.nloads && n < cap - 2; i++) {
        const tsdb_node_load_t *r = &ab->load_table.loads[i];
        int vn = tsdb_hashring_get_weight(ring, r->node_id);
        n += snprintf(buf + n, (size_t)(cap - n),
                      "%s{\"id\":%llu,\"writes_sec\":%llu,\"storage_mb\":%.1f"
                      ",\"cpu_pct\":%.2f,\"vn\":%d}",
                      (i > 0 ? "," : ""),
                      (unsigned long long)r->node_id,
                      (unsigned long long)r->writes_sec,
                      (double)r->storage_bytes / (1024.0 * 1024.0),
                      (double)r->cpu_pct_x100 / 100.0,
                      vn);
    }
    pthread_mutex_unlock(&ab->load_table.lock);

    if (n < cap - 2)
        n += snprintf(buf + n, (size_t)(cap - n), "]}");
    return n;
}
