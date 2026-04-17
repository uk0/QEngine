/* federation.c — multi-cluster federation coordinator.
 *
 * Implements tsdb_federation_t: connects to N cluster coordinators,
 * routes queries via fedrouter, fan-outs via fedrpc, and merges via fedagg.
 */

#include "federation.h"
#include "fedrouter.h"
#include "fedrpc.h"
#include "fedagg.h"
#include "../../include/tsdb.h"
#include "../cluster/rpc.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>
#include <ctype.h>

#define FED_MAX_CLUSTERS_INTERNAL  16
#define FED_CONNECT_TIMEOUT_MS     5000
#define FED_QUERY_TIMEOUT_MS       30000

/* ---- Per-cluster state ---------------------------------------------------- */

typedef struct {
    tsdb_fed_cluster_t  cfg;
    tsdb_rpc_conn_t    *conn;       /* persistent RPC connection; NULL if down */
    pthread_mutex_t     lock;       /* protect conn reconnect. */
    int                 failures;   /* consecutive failure count */
} fed_cluster_state_t;

/* ---- Federation handle ---------------------------------------------------- */

struct tsdb_federation {
    fed_cluster_state_t  clusters[FED_MAX_CLUSTERS_INTERNAL];
    int                  nclusters;
    fed_router_t        *router;
    pthread_mutex_t      stats_lock;
    /* Stats counters. */
    uint64_t             total_queries;
    uint64_t             total_partial;
    /* Last query duration (ns). */
    int64_t              last_query_ns;
    int64_t              p50_ns;          /* rolling estimate */
};

/* ---- Lifecycle ------------------------------------------------------------ */

int tsdb_federation_new(const tsdb_fed_cluster_t *clusters, int n,
                        tsdb_federation_t **out)
{
    if (!clusters || n <= 0 || !out) return TSDB_ERR_INVAL;
    if (n > FED_MAX_CLUSTERS_INTERNAL) n = FED_MAX_CLUSTERS_INTERNAL;

    tsdb_federation_t *f = calloc(1, sizeof(*f));
    if (!f) return TSDB_ERR_NOMEM;

    pthread_mutex_init(&f->stats_lock, NULL);
    f->nclusters = n;

    /* Build cluster name list for the router. */
    const char *names[FED_MAX_CLUSTERS_INTERNAL];
    for (int i = 0; i < n; i++) {
        f->clusters[i].cfg = clusters[i];
        pthread_mutex_init(&f->clusters[i].lock, NULL);
        f->clusters[i].conn = NULL;
        names[i] = clusters[i].name;

        /* Attempt initial connection (non-fatal if unreachable at creation). */
        f->clusters[i].conn = tsdb_rpc_connect(clusters[i].coordinator_addr,
                                                FED_CONNECT_TIMEOUT_MS);
        if (!f->clusters[i].conn) {
            fprintf(stderr, "[federation] warn: cannot connect to %s (%s)\n",
                    clusters[i].name, clusters[i].coordinator_addr);
        }
    }

    f->router = fed_router_new(NULL, 0, names, n);
    if (!f->router) {
        tsdb_federation_free(f);
        return TSDB_ERR_NOMEM;
    }

    *out = f;
    return TSDB_OK;
}

void tsdb_federation_free(tsdb_federation_t *f) {
    if (!f) return;
    for (int i = 0; i < f->nclusters; i++) {
        if (f->clusters[i].conn) tsdb_rpc_conn_close(f->clusters[i].conn);
        pthread_mutex_destroy(&f->clusters[i].lock);
    }
    fed_router_free(f->router);
    pthread_mutex_destroy(&f->stats_lock);
    free(f);
}

/* ---- Routing rules ------------------------------------------------------- */

int tsdb_federation_add_route(tsdb_federation_t *f,
                              const char *table_pattern,
                              const char *target_cluster)
{
    if (!f || !table_pattern || !target_cluster) return TSDB_ERR_INVAL;
    return fed_router_add(f->router, table_pattern, target_cluster) == 0
           ? TSDB_OK : TSDB_ERR_FULL;
}

/* ---- Query rewrite helpers ----------------------------------------------- */

/*
 * Detect SAMPLE BY in the QTL string.
 * Simple string search: "SAMPLE BY" case-insensitive.
 */
static int has_sample_by(const char *qtl) {
    const char *p = qtl;
    while (*p) {
        if (strncasecmp(p, "sample", 6) == 0) {
            const char *q = p + 6;
            while (*q == ' ' || *q == '\t') q++;
            if (strncasecmp(q, "by", 2) == 0) return 1;
        }
        p++;
    }
    return 0;
}

/*
 * Detect LIMIT N in the QTL string.
 * Returns the limit value, or 0 if not found.
 */
static int64_t detect_limit(const char *qtl) {
    const char *p = qtl;
    while (*p) {
        if (strncasecmp(p, "limit", 5) == 0) {
            const char *q = p + 5;
            while (*q == ' ' || *q == '\t') q++;
            if (*q >= '0' && *q <= '9') return (int64_t)atoll(q);
        }
        p++;
    }
    return 0;
}

/*
 * Extract table name from "FROM <table>".
 * Writes to out_table (null-terminated).  Returns 0 on success.
 */
static int extract_table(const char *qtl, char *out_table, size_t cap) {
    const char *p = qtl;
    while (*p) {
        if (strncasecmp(p, "from", 4) == 0 &&
            (p == qtl || !isalnum((unsigned char)*(p - 1)))) {
            const char *q = p + 4;
            while (*q == ' ' || *q == '\t') q++;
            size_t i = 0;
            while (*q && !isspace((unsigned char)*q) &&
                   *q != ';' && *q != ')' && i + 1 < cap) {
                out_table[i++] = *q++;
            }
            out_table[i] = '\0';
            return 0;
        }
        p++;
    }
    return -1;
}

/*
 * AVG rewrite: replace avg(X) with __fed_sum(X) AS __fed_sum_X, count(X) AS __fed_cnt_X
 * in a copy of the QTL string.
 *
 * Simple approach: scan SELECT clause for avg(...), replace in buffer.
 * Returns 1 if rewrite happened, 0 otherwise.
 */
static int rewrite_avg(const char *qtl, char *out, size_t outcap, int *rewritten) {
    if (rewritten) *rewritten = 0;

    const char *p   = qtl;
    char *dst       = out;
    char *end       = out + outcap - 1;

    while (*p && dst < end) {
        /* Case-insensitive scan for "avg(" */
        if (strncasecmp(p, "avg(", 4) == 0) {
            /* Find matching closing paren. */
            const char *arg_start = p + 4;
            int depth = 1;
            const char *q = arg_start;
            while (*q && depth > 0) {
                if (*q == '(') depth++;
                else if (*q == ')') depth--;
                q++;
            }
            /* q now points one past the closing ')'. */
            /* arg is arg_start..q-2 (excluding the closing paren). */
            size_t arg_len = (size_t)(q - 1 - arg_start);
            char arg[256];
            if (arg_len >= sizeof(arg)) arg_len = sizeof(arg) - 1;
            memcpy(arg, arg_start, arg_len);
            arg[arg_len] = '\0';

            /* Emit: sum(arg) AS __fed_sum_arg, count(arg) AS __fed_cnt_arg */
            int written = snprintf(dst, (size_t)(end - dst),
                                   "sum(%s) AS __fed_sum_%s, count(%s) AS __fed_cnt_%s",
                                   arg, arg, arg, arg);
            if (written > 0) dst += written;

            p = q;  /* skip past the original avg(...) */
            if (rewritten) *rewritten = 1;
        } else {
            *dst++ = *p++;
        }
    }
    *dst = '\0';
    return 0;
}

/*
 * Also rewrite count(*) → count(ts) since some cluster executors don't
 * handle count(*) natively.
 * Also remove "AS count_expr" aliases that would trip up the parser.
 */
static void rewrite_count_star(char *qtl) {
    char *p = qtl;
    while ((p = strcasestr(p, "count(*)"))) {
        /* Replace "count(*)" with "count(ts)" in-place (same length? no, shorter).
         * Since "count(*)" = 8 chars and "count(ts)" = 9 chars, we need to shift.
         * Use memmove. */
        size_t rest = strlen(p + 8);
        memmove(p + 9, p + 8, rest + 1);
        memcpy(p, "count(ts)", 9);
        p += 9;
    }
}

/* ---- Per-cluster query task (for parallelism via pthreads) --------------- */

typedef struct {
    tsdb_federation_t  *f;
    int                 cluster_idx;
    const char         *qtl;          /* rewritten query */
    tsdb_result_t      *result;       /* out */
    bool                ok;
    int                 rc;
} fed_query_task_t;

static void *fed_query_worker(void *arg) {
    fed_query_task_t *task = (fed_query_task_t *)arg;
    fed_cluster_state_t *cs = &task->f->clusters[task->cluster_idx];

    pthread_mutex_lock(&cs->lock);

    /* Reconnect if needed. */
    if (!cs->conn) {
        cs->conn = tsdb_rpc_connect(cs->cfg.coordinator_addr,
                                    FED_CONNECT_TIMEOUT_MS);
    }

    if (!cs->conn) {
        pthread_mutex_unlock(&cs->lock);
        task->ok = false;
        task->rc = TSDB_ERR_IO;
        return NULL;
    }

    int rc = fedrpc_query(cs->conn, task->qtl,
                          FED_QUERY_TIMEOUT_MS, &task->result);
    if (rc != TSDB_OK) {
        /* Close connection on error — will reconnect next time. */
        tsdb_rpc_conn_close(cs->conn);
        cs->conn = NULL;
        cs->failures++;
        task->ok = false;
        task->rc = rc;
    } else {
        cs->failures = 0;
        task->ok = true;
        task->rc = TSDB_OK;
    }

    pthread_mutex_unlock(&cs->lock);
    return NULL;
}

/* ---- Main query entry point ----------------------------------------------- */

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

int tsdb_federation_query_ex(tsdb_federation_t *f, const char *qtl,
                             tsdb_result_t **out, int *partial_miss_out)
{
    if (!f || !qtl || !out) return TSDB_ERR_INVAL;

    int64_t t_start = now_ns();

    /* Extract table name for routing. */
    char table_name[128] = {0};
    extract_table(qtl, table_name, sizeof(table_name));

    /* Resolve target clusters. */
    int cluster_indices[FED_MAX_CLUSTERS_INTERNAL];
    int ntargets = fed_router_resolve(f->router, table_name,
                                     f->nclusters, cluster_indices);
    if (ntargets == 0) {
        /* No targets: fan-out all. */
        ntargets = f->nclusters;
        for (int i = 0; i < ntargets; i++) cluster_indices[i] = i;
    }

    /* Detect query shape. */
    int  sampleby = has_sample_by(qtl);
    int64_t limit  = detect_limit(qtl);

    /* AVG rewrite. */
    char rewritten_qtl[4096];
    int  did_avg_rewrite = 0;
    rewrite_avg(qtl, rewritten_qtl, sizeof(rewritten_qtl), &did_avg_rewrite);
    rewrite_count_star(rewritten_qtl);

    /* Launch parallel queries. */
    fed_query_task_t tasks[FED_MAX_CLUSTERS_INTERNAL];
    pthread_t        threads[FED_MAX_CLUSTERS_INTERNAL];
    memset(tasks, 0, sizeof(tasks));

    for (int t = 0; t < ntargets; t++) {
        tasks[t].f           = f;
        tasks[t].cluster_idx = cluster_indices[t];
        tasks[t].qtl         = rewritten_qtl;
        tasks[t].result      = NULL;
        tasks[t].ok          = false;
        tasks[t].rc          = TSDB_ERR_IO;

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        pthread_create(&threads[t], &attr, fed_query_worker, &tasks[t]);
        pthread_attr_destroy(&attr);
    }

    /* Wait for all. */
    for (int t = 0; t < ntargets; t++) {
        pthread_join(threads[t], NULL);
    }

    /* Collect partial results. */
    tsdb_result_t *partials[FED_MAX_CLUSTERS_INTERNAL];
    bool           partial_ok[FED_MAX_CLUSTERS_INTERNAL];
    int            ngood = 0;

    for (int t = 0; t < ntargets; t++) {
        partials[t]    = tasks[t].result;
        partial_ok[t]  = tasks[t].ok;
        if (tasks[t].ok) ngood++;
    }

    if (ngood == 0) {
        /* All failed. */
        for (int t = 0; t < ntargets; t++) {
            if (partials[t]) fedagg_result_free(partials[t]);
        }
        return TSDB_ERR_IO;
    }

    /* Merge. */
    int miss = 0;
    tsdb_result_t *merged = NULL;
    int rc = fedagg_merge(partials, partial_ok, ntargets,
                          (bool)sampleby, limit, (bool)did_avg_rewrite,
                          &merged, &miss);

    /* Free partial results (fedagg_merge made copies). */
    for (int t = 0; t < ntargets; t++) {
        if (partials[t]) fedagg_result_free(partials[t]);
    }

    if (rc != TSDB_OK) return rc;

    if (partial_miss_out) *partial_miss_out = miss;

    /* Update stats. */
    int64_t elapsed = now_ns() - t_start;
    pthread_mutex_lock(&f->stats_lock);
    f->total_queries++;
    f->total_partial += (uint64_t)miss;
    f->last_query_ns  = elapsed;
    /* Exponentially-weighted moving average for p50 estimate. */
    f->p50_ns = f->p50_ns == 0 ? elapsed
                                : (f->p50_ns * 9 + elapsed) / 10;
    pthread_mutex_unlock(&f->stats_lock);

    *out = merged;
    return TSDB_OK;
}

int tsdb_federation_query(tsdb_federation_t *f, const char *qtl,
                          tsdb_result_t **out)
{
    return tsdb_federation_query_ex(f, qtl, out, NULL);
}

/* ---- Stats --------------------------------------------------------------- */

int tsdb_federation_stats_str(tsdb_federation_t *f, char *buf, size_t cap) {
    if (!f || !buf || !cap) return 0;

    pthread_mutex_lock(&f->stats_lock);
    int n = snprintf(buf, cap,
                     "{\"clusters\":%d,\"total_queries\":%llu,"
                     "\"total_partial\":%llu,\"last_query_us\":%lld,"
                     "\"p50_us\":%lld",
                     f->nclusters,
                     (unsigned long long)f->total_queries,
                     (unsigned long long)f->total_partial,
                     (long long)(f->last_query_ns / 1000),
                     (long long)(f->p50_ns / 1000));
    pthread_mutex_unlock(&f->stats_lock);

    /* Append per-cluster info. */
    if (n < (int)cap - 2) {
        char *p = buf + n;
        int rem = (int)cap - n;
        int w = snprintf(p, (size_t)rem, ",\"cluster_status\":[");
        p += w; rem -= w;
        for (int i = 0; i < f->nclusters && rem > 32; i++) {
            fed_cluster_state_t *cs = &f->clusters[i];
            w = snprintf(p, (size_t)rem,
                         "%s{\"name\":\"%s\",\"addr\":\"%s\",\"up\":%s,\"failures\":%d}",
                         i ? "," : "",
                         cs->cfg.name,
                         cs->cfg.coordinator_addr,
                         cs->conn ? "true" : "false",
                         cs->failures);
            p += w; rem -= w;
        }
        if (rem > 4) { *p++ = ']'; *p++ = '}'; *p = '\0'; }
        n = (int)(p - buf);
    }

    return n;
}
