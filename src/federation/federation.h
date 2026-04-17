/* federation.h — multi-cluster federation coordinator.
 *
 * A tsdb_federation_t groups N independent tsdb clusters under one
 * query interface.  Queries are fan-out to the appropriate clusters,
 * partial results are collected, and the federation coordinator merges
 * them into a single tsdb_result_t.
 */
#ifndef TSDB_FEDERATION_H
#define TSDB_FEDERATION_H

#include "../../include/tsdb.h"
#include "fedrouter.h"
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Per-cluster descriptor. */
typedef struct {
    char  name[64];              /* "cluster-east-1" */
    char  coordinator_addr[128]; /* "1.2.3.4:28081" */
    int   region_priority;       /* lower = closer / preferred */
} tsdb_fed_cluster_t;

/* Opaque federation handle. */
typedef struct tsdb_federation tsdb_federation_t;

/* ---- Lifecycle ------------------------------------------------------------ */

/*
 * Create a federation from an explicit cluster list.
 * clusters[0..n-1] describe each independent cluster.
 */
int tsdb_federation_new(const tsdb_fed_cluster_t *clusters, int n,
                        tsdb_federation_t **out);

/* Release all resources (closes RPC connections). */
void tsdb_federation_free(tsdb_federation_t *f);

/* ---- Routing rules ------------------------------------------------------- */

/*
 * Register a routing rule.
 * table_pattern: exact table name, or "*" for all tables.
 * target_cluster: cluster name registered in federation_new, or "*" = fan-out all.
 */
int tsdb_federation_add_route(tsdb_federation_t *f,
                              const char *table_pattern,
                              const char *target_cluster);

/* ---- Query --------------------------------------------------------------- */

/*
 * Execute a QTL query across all matching clusters.
 * The federation coordinator:
 *   1. Routes by table name / routing rules.
 *   2. Rewrites AVG(x) → __fed_sum(x) + __fed_cnt(x) before dispatch.
 *   3. Fan-outs to matching clusters in parallel.
 *   4. Merges partial results (fedagg).
 *   5. Returns merged tsdb_result_t.
 *
 * If a cluster times out or returns an error, it is treated as a partial
 * failure: the result is still returned (with data from other clusters),
 * and *partial_miss_out is set to the number of failed clusters.
 * Pass NULL for partial_miss_out to ignore.
 *
 * Returns TSDB_OK on full success, TSDB_ERR_IO if ALL clusters failed.
 */
int tsdb_federation_query(tsdb_federation_t *f,
                          const char *qtl,
                          tsdb_result_t **out);

int tsdb_federation_query_ex(tsdb_federation_t *f,
                             const char *qtl,
                             tsdb_result_t **out,
                             int *partial_miss_out);

/* ---- Statistics ---------------------------------------------------------- */

/*
 * Fill buf with JSON-ish federation status.
 * Returns bytes written.
 */
int tsdb_federation_stats_str(tsdb_federation_t *f, char *buf, size_t cap);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_FEDERATION_H */
