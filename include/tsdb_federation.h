/* tsdb_federation.h — multi-cluster federation API for tsdb.
 *
 * Include this in addition to tsdb.h when using federation features.
 * A federation client connects to N independent tsdb clusters and provides
 * a unified query interface across all of them.
 *
 * Config JSON format (simplified):
 *   {
 *     "clusters": [
 *       {"name":"east","coordinator":"127.0.0.1:28081","priority":1},
 *       {"name":"west","coordinator":"127.0.0.1:28091","priority":2}
 *     ],
 *     "routes": [
 *       {"table":"trades","cluster":"east"},
 *       {"table":"cpu","cluster":"*"}
 *     ]
 *   }
 *
 * Simplified key=value alternative:
 *   "east=127.0.0.1:28081,west=127.0.0.1:28091|trades->east|cpu->*"
 */
#ifndef TSDB_FEDERATION_H_PUBLIC
#define TSDB_FEDERATION_H_PUBLIC

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque federation handle (defined in src/federation/federation.h). */
typedef struct tsdb_federation tsdb_federation_t;

/*
 * Open a federation client from a config string.
 *
 * config_json — JSON config string (see above), or NULL to read from
 *               environment variable TSDB_FEDERATION_CONFIG.
 *               Also accepts simplified pipe-delimited format:
 *                 "east=addr,west=addr|table->cluster|..."
 *
 * Returns TSDB_OK on success.
 */
int tsdb_federation_open(const char *config_json, tsdb_federation_t **out);

/* Close and free a federation client. */
void tsdb_federation_close(tsdb_federation_t *f);

/*
 * Execute a QTL query across all matching clusters.
 *
 * qtl — query string (same syntax as tsdb_query).
 * out — on success, caller-owned result (free with tsdb_result_free).
 *
 * Returns TSDB_OK on full success.
 * Returns TSDB_ERR_IO if ALL clusters failed.
 * If only some clusters fail, returns TSDB_OK with partial data; use
 * tsdb_federation_stats() to check partial miss count.
 */
int tsdb_federation_query_str(tsdb_federation_t *f, const char *qtl,
                              tsdb_result_t **out);

/*
 * Like tsdb_federation_query_str but also returns the number of clusters
 * that failed to respond (partial miss count).
 */
int tsdb_federation_query_ex(tsdb_federation_t *f, const char *qtl,
                             tsdb_result_t **out, int *partial_miss_out);

/*
 * Fill buf with JSON-ish federation status (cluster health, query stats).
 * Returns bytes written.
 */
int tsdb_federation_stats(tsdb_federation_t *f, char *buf, size_t cap);

/*
 * Add a routing rule after open.
 * table_pattern — exact table name or "*" for all tables.
 * target_cluster — cluster name registered in config, or "*" = fan-out all.
 */
int tsdb_federation_add_route(tsdb_federation_t *f,
                              const char *table_pattern,
                              const char *target_cluster);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_FEDERATION_H_PUBLIC */
