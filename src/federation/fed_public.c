/* fed_public.c — public federation API: tsdb_federation_open/close/query/stats.
 *
 * Implements the tsdb_federation.h public API.
 * Includes a minimal JSON parser for the cluster config format.
 */

#include "../../include/tsdb_federation.h"
#include "../../include/tsdb.h"
#include "federation.h"
#include "fedagg.h"
#include "fedrouter.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

#define FED_CFG_MAX_CLUSTERS  16
#define FED_CFG_MAX_ROUTES    64

/* ---- Minimal JSON parser -------------------------------------------------- */
/*
 * We only parse our specific schema:
 *   {"clusters":[{"name":"...","coordinator":"...","priority":N},...],
 *    "routes":[{"table":"...","cluster":"..."},...]}
 *
 * We use a simple linear scan, not a general parser.
 */

/* Skip whitespace. */
static const char *skip_ws(const char *p) {
    while (*p && isspace((unsigned char)*p)) p++;
    return p;
}

/* Extract a JSON string value (between quotes).
 * Returns pointer past closing quote on success, NULL on error.
 * Writes at most `cap-1` chars to `out`. */
static const char *json_str(const char *p, char *out, size_t cap) {
    p = skip_ws(p);
    if (*p != '"') return NULL;
    p++;
    size_t i = 0;
    while (*p && *p != '"') {
        if (*p == '\\') { p++; if (!*p) return NULL; }
        if (i + 1 < cap) out[i++] = *p;
        p++;
    }
    out[i] = '\0';
    if (*p == '"') p++;
    return p;
}

/* Find the value of a JSON key (string value) in the buffer near position p.
 * Looks for "key":"value".  Returns pointer past the closing quote. */
static const char *json_get_str(const char *buf, const char *key,
                                 char *out, size_t cap,
                                 const char *search_start)
{
    const char *p = search_start ? search_start : buf;
    size_t klen = strlen(key);
    char kpat[128];
    snprintf(kpat, sizeof(kpat), "\"%s\"", key);
    size_t kplen = klen + 2;

    while (*p) {
        if (strncmp(p, kpat, kplen) == 0) {
            const char *q = p + kplen;
            q = skip_ws(q);
            if (*q == ':') {
                q++;
                q = skip_ws(q);
                const char *end = json_str(q, out, cap);
                return end;
            }
        }
        p++;
    }
    return NULL;
}

/* Find the value of a JSON key (integer) near p. */
static const char *json_get_int(const char *search_start, const char *key,
                                 int *out)
{
    const char *p = search_start;
    size_t klen = strlen(key);
    char kpat[128];
    snprintf(kpat, sizeof(kpat), "\"%s\"", key);
    size_t kplen = klen + 2;

    while (*p) {
        if (strncmp(p, kpat, kplen) == 0) {
            const char *q = p + kplen;
            q = skip_ws(q);
            if (*q == ':') {
                q++;
                q = skip_ws(q);
                if (*q == '-' || isdigit((unsigned char)*q)) {
                    *out = atoi(q);
                    while (*q && (isdigit((unsigned char)*q) || *q == '-')) q++;
                    return q;
                }
            }
        }
        p++;
    }
    return NULL;
}

/* Parse JSON config into cluster array + route array. */
static int parse_json_config(const char *json,
                              tsdb_fed_cluster_t *clusters, int *nclusters,
                              char route_tables[][128], char route_clusters[][64],
                              int *nroutes)
{
    *nclusters = 0;
    *nroutes   = 0;

    /* Find "clusters" array. */
    const char *clusters_start = strstr(json, "\"clusters\"");
    if (clusters_start) {
        clusters_start = strchr(clusters_start, '[');
        if (clusters_start) {
        clusters_start++;

        const char *p = clusters_start;
        while (*p && *nclusters < FED_CFG_MAX_CLUSTERS) {
            p = skip_ws(p);
            if (*p == ']') break;
            if (*p != '{') { p++; continue; }
            /* Inside a cluster object. */
            const char *obj_end = strchr(p, '}');
            if (!obj_end) break;

            char name[64] = {0}, coord[128] = {0};
            int  prio = 1;

            json_get_str(p, "name",        name,  sizeof(name),  p);
            json_get_str(p, "coordinator", coord, sizeof(coord), p);
            json_get_int(p, "priority", &prio);

            if (name[0] && coord[0]) {
                snprintf(clusters[*nclusters].name, 64, "%s", name);
                snprintf(clusters[*nclusters].coordinator_addr, 128, "%s", coord);
                clusters[*nclusters].region_priority = prio;
                (*nclusters)++;
            }
            p = obj_end + 1;
            p = skip_ws(p);
            if (*p == ',') p++;
        }
        } /* if (clusters_start) */
    }

    /* Find "routes" array. */
    {
    const char *routes_start = strstr(json, "\"routes\"");
    if (routes_start) {
        routes_start = strchr(routes_start, '[');
        if (!routes_start) { return 0; }
        routes_start++;

        const char *p = routes_start;
        while (*p && *nroutes < FED_CFG_MAX_ROUTES) {
            p = skip_ws(p);
            if (*p == ']') break;
            if (*p != '{') { p++; continue; }
            const char *obj_end = strchr(p, '}');
            if (!obj_end) break;

            char tbl[128] = {0}, cname[64] = {0};
            json_get_str(p, "table",   tbl,   sizeof(tbl),   p);
            json_get_str(p, "cluster", cname, sizeof(cname), p);

            if (tbl[0] && cname[0]) {
                snprintf(route_tables[*nroutes],   128, "%s", tbl);
                snprintf(route_clusters[*nroutes],  64, "%s", cname);
                (*nroutes)++;
            }
            p = obj_end + 1;
            p = skip_ws(p);
            if (*p == ',') p++;
        }
    }
    } /* end routes block */
    return 0;
}

/*
 * Parse simplified pipe-delimited format:
 *   "east=127.0.0.1:28081,west=127.0.0.1:28091|trades->east|cpu->*"
 *
 * First segment (before '|') = comma-separated name=addr pairs.
 * Subsequent segments = table->cluster routing rules.
 */
static int parse_kv_config(const char *config,
                            tsdb_fed_cluster_t *clusters, int *nclusters,
                            char route_tables[][128], char route_clusters[][64],
                            int *nroutes)
{
    *nclusters = 0;
    *nroutes   = 0;

    char buf[4096];
    snprintf(buf, sizeof(buf), "%s", config);

    /* Split on first '|'. */
    char *pipe = strchr(buf, '|');
    char *cluster_part = buf;
    char *route_part   = NULL;
    if (pipe) { *pipe = '\0'; route_part = pipe + 1; }

    /* Parse clusters: "name=addr,name=addr,...". */
    char *tok = strtok(cluster_part, ",");
    while (tok && *nclusters < FED_CFG_MAX_CLUSTERS) {
        char *eq = strchr(tok, '=');
        if (eq) {
            *eq = '\0';
            snprintf(clusters[*nclusters].name, 64, "%s", tok);
            snprintf(clusters[*nclusters].coordinator_addr, 128, "%s", eq + 1);
            clusters[*nclusters].region_priority = *nclusters + 1;
            (*nclusters)++;
        }
        tok = strtok(NULL, ",");
    }

    /* Parse routes: "table->cluster|table->cluster|..." */
    if (route_part) {
        char rbuf[2048];
        snprintf(rbuf, sizeof(rbuf), "%s", route_part);
        tok = strtok(rbuf, "|");
        while (tok && *nroutes < FED_CFG_MAX_ROUTES) {
            char *arrow = strstr(tok, "->");
            if (arrow) {
                *arrow = '\0';
                snprintf(route_tables[*nroutes],   128, "%s", tok);
                snprintf(route_clusters[*nroutes],  64, "%s", arrow + 2);
                (*nroutes)++;
            }
            tok = strtok(NULL, "|");
        }
    }
    return 0;
}

/* ---- Public API ---------------------------------------------------------- */

int tsdb_federation_open(const char *config_json, tsdb_federation_t **out) {
    if (!out) return TSDB_ERR_INVAL;

    /* Fall back to environment variable if config_json is NULL. */
    const char *cfg = config_json;
    if (!cfg) {
        cfg = getenv("TSDB_FEDERATION_CONFIG");
    }
    if (!cfg) return TSDB_ERR_INVAL;

    tsdb_fed_cluster_t clusters[FED_CFG_MAX_CLUSTERS];
    char route_tables[FED_CFG_MAX_ROUTES][128];
    char route_clusters[FED_CFG_MAX_ROUTES][64];
    int  nclusters = 0, nroutes = 0;

    /* Detect format. */
    const char *trimmed = cfg;
    while (*trimmed && isspace((unsigned char)*trimmed)) trimmed++;

    int rc;
    if (*trimmed == '{') {
        rc = parse_json_config(cfg, clusters, &nclusters,
                               route_tables, route_clusters, &nroutes);
    } else {
        rc = parse_kv_config(cfg, clusters, &nclusters,
                             route_tables, route_clusters, &nroutes);
    }
    if (rc != 0 || nclusters == 0) return TSDB_ERR_INVAL;

    tsdb_federation_t *f = NULL;
    rc = tsdb_federation_new(clusters, nclusters, &f);
    if (rc != TSDB_OK) return rc;

    /* Register routes. */
    for (int i = 0; i < nroutes; i++) {
        tsdb_federation_add_route(f, route_tables[i], route_clusters[i]);
    }

    *out = f;
    return TSDB_OK;
}

void tsdb_federation_close(tsdb_federation_t *f) {
    tsdb_federation_free(f);
}

int tsdb_federation_query_str(tsdb_federation_t *f, const char *qtl,
                              tsdb_result_t **out)
{
    return tsdb_federation_query(f, qtl, out);
}

int tsdb_federation_stats(tsdb_federation_t *f, char *buf, size_t cap) {
    return tsdb_federation_stats_str(f, buf, cap);
}

/* Note: tsdb_federation_add_route is declared in federation.h (internal) and
 * in tsdb_federation.h (public).  The public header uses the same name.
 * The implementation in federation.c IS the canonical implementation.
 * fed_public.c re-exports it through the public header — no wrapper needed
 * since the function body is in federation.c and the linker resolves it. */
