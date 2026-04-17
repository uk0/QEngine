/* db_cluster.c — Cluster extension entry points.
 *
 * Provides the tsdb_open_cluster / tsdb_cluster_* public API.
 * Uses a global registry (db_ptr -> cluster_ptr) to avoid modifying
 * the tsdb_db struct defined in db.c.
 */

#include "db.h"
#include "../cluster/cluster.h"
#include "../../include/tsdb_cluster.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

/* ---- Cluster registry ---------------------------------------------------- */

#define MAX_CLUSTER_ENTRIES 16

typedef struct {
    tsdb_db_t      *db;
    tsdb_cluster_t *cluster;
} cluster_entry_t;

static cluster_entry_t g_cluster_map[MAX_CLUSTER_ENTRIES];
static int             g_nentries = 0;
static pthread_mutex_t g_cluster_lock = PTHREAD_MUTEX_INITIALIZER;

static tsdb_cluster_t *cluster_get(tsdb_db_t *db) {
    pthread_mutex_lock(&g_cluster_lock);
    for (int i = 0; i < g_nentries; i++) {
        if (g_cluster_map[i].db == db) {
            tsdb_cluster_t *c = g_cluster_map[i].cluster;
            pthread_mutex_unlock(&g_cluster_lock);
            return c;
        }
    }
    pthread_mutex_unlock(&g_cluster_lock);
    return NULL;
}

static void cluster_set(tsdb_db_t *db, tsdb_cluster_t *cluster) {
    pthread_mutex_lock(&g_cluster_lock);
    for (int i = 0; i < g_nentries; i++) {
        if (g_cluster_map[i].db == db) {
            g_cluster_map[i].cluster = cluster;
            pthread_mutex_unlock(&g_cluster_lock);
            return;
        }
    }
    if (g_nentries < MAX_CLUSTER_ENTRIES) {
        g_cluster_map[g_nentries].db      = db;
        g_cluster_map[g_nentries].cluster = cluster;
        g_nentries++;
    }
    pthread_mutex_unlock(&g_cluster_lock);
}

static void cluster_remove(tsdb_db_t *db) {
    pthread_mutex_lock(&g_cluster_lock);
    for (int i = 0; i < g_nentries; i++) {
        if (g_cluster_map[i].db == db) {
            g_cluster_map[i] = g_cluster_map[--g_nentries];
            break;
        }
    }
    pthread_mutex_unlock(&g_cluster_lock);
}

/* ---- Node ID generation -------------------------------------------------- */

static tsdb_node_id_t generate_node_id(const char *addr) {
    uint64_t h = 14695981039346656037ULL;
    if (addr) {
        for (const char *p = addr; *p; p++) {
            h ^= (uint8_t)*p;
            h *= 1099511628211ULL;
        }
    }
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t t = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    h ^= t;
    h *= 1099511628211ULL;
    return h ? h : 1ULL;
}

/* ---- Public cluster API -------------------------------------------------- */

int tsdb_open_cluster(const char *data_dir,
                      const char *bind_addr,
                      const char *gossip_seeds,
                      tsdb_db_t **out)
{
    if (!data_dir || !out) return TSDB_ERR_INVAL;

    tsdb_db_t *db = NULL;
    int rc = tsdb_open(data_dir, &db);
    if (rc != TSDB_OK) return rc;

    /* Derive gossip addr: same host, rpc_port - 1. */
    char gossip_addr[64];
    {
        char host[128] = "0.0.0.0";
        int  rpc_port  = 28081;
        if (bind_addr) {
            const char *colon = strrchr(bind_addr, ':');
            if (colon) {
                size_t hlen = (size_t)(colon - bind_addr);
                if (hlen < sizeof(host)) {
                    memcpy(host, bind_addr, hlen);
                    host[hlen] = '\0';
                }
                rpc_port = atoi(colon + 1);
            }
        }
        snprintf(gossip_addr, sizeof(gossip_addr), "%s:%d", host, rpc_port - 1);
    }

    tsdb_node_id_t node_id = generate_node_id(bind_addr);

    tsdb_cluster_t *cluster = tsdb_cluster_new(db, node_id,
                                               bind_addr  ? bind_addr  : "0.0.0.0:28081",
                                               gossip_addr,
                                               gossip_seeds);
    if (!cluster) {
        tsdb_close(db);
        return TSDB_ERR_IO;
    }

    cluster_set(db, cluster);
    *out = db;
    return TSDB_OK;
}

int tsdb_cluster_stats(tsdb_db_t *db, char *buf, size_t cap) {
    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return 0;
    return tsdb_cluster_stats_str(c, buf, cap);
}

int tsdb_cluster_alive_count(tsdb_db_t *db) {
    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return 1;
    return tsdb_node_manager_alive_count(tsdb_cluster_node_mgr(c));
}

void tsdb_cluster_wait_ready(tsdb_db_t *db, int min_nodes, int timeout_ms) {
    tsdb_cluster_t *c = cluster_get(db);
    if (!c) return;
    tsdb_cluster_wait_alive(c, min_nodes, timeout_ms);
}

void tsdb_close_cluster(tsdb_db_t *db) {
    tsdb_cluster_t *c = cluster_get(db);
    if (c) {
        cluster_remove(db);
        tsdb_cluster_free(c);
    }
}
