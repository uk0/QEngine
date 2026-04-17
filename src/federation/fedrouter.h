/* fedrouter.h — federation query router.
 *
 * Decides which clusters a query should be dispatched to, based on
 * registered routing rules (table pattern → cluster name) and
 * optional time-range shard hints.
 */
#ifndef TSDB_FEDROUTER_H
#define TSDB_FEDROUTER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum clusters in a federation. */
#define FED_MAX_CLUSTERS  16
/* Maximum routing rules. */
#define FED_MAX_ROUTES    64

/* One routing rule: table_pattern → cluster_name.
 * Patterns: exact name, or "*" (wildcard matches any table).
 */
typedef struct {
    char table_pattern[128];  /* "cpu", "trades.*", "*" */
    char cluster_name[64];    /* "east" or "*" = all clusters */
} fed_route_t;

/* Opaque router handle — embedded in tsdb_federation_t. */
typedef struct fed_router fed_router_t;

/* Create a router with the given rules array. */
fed_router_t *fed_router_new(const fed_route_t *rules, int nrules,
                             const char * const *cluster_names, int nclusters);
void fed_router_free(fed_router_t *r);

/* Add a single route at runtime. Returns 0 on success, -1 if full. */
int fed_router_add(fed_router_t *r,
                   const char *table_pattern,
                   const char *cluster_name);

/*
 * Resolve which clusters should handle a query on `table_name`.
 * out_indices: caller-supplied array of at least FED_MAX_CLUSTERS ints.
 * Returns number of cluster indices written (indices into federation's
 * cluster array).  Returns nclusters (fan-out all) if table_pattern = "*".
 */
int fed_router_resolve(fed_router_t *r,
                       const char *table_name,
                       int nclusters,
                       int *out_indices);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_FEDROUTER_H */
