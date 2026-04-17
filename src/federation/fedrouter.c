/* fedrouter.c — federation query router implementation. */

#include "fedrouter.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct fed_router {
    fed_route_t  rules[FED_MAX_ROUTES];
    int          nrules;
    /* cluster name table (pointers to caller-supplied strings, or copied). */
    char         cluster_names[FED_MAX_CLUSTERS][64];
    int          nclusters;
};

fed_router_t *fed_router_new(const fed_route_t *rules, int nrules,
                             const char * const *cluster_names, int nclusters)
{
    fed_router_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;

    r->nclusters = nclusters < FED_MAX_CLUSTERS ? nclusters : FED_MAX_CLUSTERS;
    for (int i = 0; i < r->nclusters; i++) {
        snprintf(r->cluster_names[i], 64, "%s", cluster_names[i]);
    }

    r->nrules = nrules < FED_MAX_ROUTES ? nrules : FED_MAX_ROUTES;
    if (rules) {
        for (int i = 0; i < r->nrules; i++) {
            r->rules[i] = rules[i];
        }
    }
    return r;
}

void fed_router_free(fed_router_t *r) {
    free(r);
}

int fed_router_add(fed_router_t *r, const char *table_pattern,
                   const char *cluster_name)
{
    if (!r || r->nrules >= FED_MAX_ROUTES) return -1;
    snprintf(r->rules[r->nrules].table_pattern, 128, "%s", table_pattern);
    snprintf(r->rules[r->nrules].cluster_name,   64, "%s", cluster_name);
    r->nrules++;
    return 0;
}

/*
 * Simple pattern match:
 *   - exact match
 *   - "prefix.*" matches if table starts with "prefix."
 *   - "*" matches anything
 */
static int pattern_match(const char *pattern, const char *table) {
    if (strcmp(pattern, "*") == 0) return 1;
    size_t plen = strlen(pattern);
    if (plen >= 2 && pattern[plen - 1] == '*' && pattern[plen - 2] == '.') {
        /* prefix.* style */
        size_t prefix_len = plen - 2;
        if (strncmp(pattern, table, prefix_len) == 0 &&
            table[prefix_len] == '.') return 1;
    }
    return strcmp(pattern, table) == 0;
}

int fed_router_resolve(fed_router_t *r, const char *table_name,
                       int nclusters, int *out_indices)
{
    if (!r || !table_name || !out_indices) return 0;

    /* Scan rules in order; first match wins. */
    for (int i = 0; i < r->nrules; i++) {
        if (!pattern_match(r->rules[i].table_pattern, table_name)) continue;

        const char *cname = r->rules[i].cluster_name;
        if (strcmp(cname, "*") == 0) {
            /* Fan-out all. */
            for (int c = 0; c < nclusters; c++) out_indices[c] = c;
            return nclusters;
        }
        /* Find the named cluster. */
        for (int c = 0; c < r->nclusters; c++) {
            if (strcmp(r->cluster_names[c], cname) == 0) {
                out_indices[0] = c;
                return 1;
            }
        }
        /* Named cluster not found — fall through to next rule. */
    }

    /* No rule matched: fan-out all by default. */
    for (int c = 0; c < nclusters; c++) out_indices[c] = c;
    return nclusters;
}
