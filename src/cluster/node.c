/* node.c — cluster membership management. */

#include "node.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

/* ---- Helpers ------------------------------------------------------------- */

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Simple xorshift64 for random node selection. */
static uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

/* ---- Node manager struct ------------------------------------------------- */

struct tsdb_node_manager {
    pthread_mutex_t   lock;

    tsdb_node_id_t    local_id;
    char              local_addr[TSDB_ADDR_MAX];
    char              local_gossip_addr[TSDB_ADDR_MAX];

    tsdb_node_info_t  nodes[TSDB_CLUSTER_MAX_NODES];
    int               nnodes;

    tsdb_hashring_t  *ring;

    uint64_t          rng_state;
};

/* ---- Init / free --------------------------------------------------------- */

tsdb_node_manager_t *tsdb_node_manager_new(tsdb_node_id_t local_id,
                                           const char *local_addr,
                                           const char *gossip_addr)
{
    tsdb_node_manager_t *mgr = calloc(1, sizeof(*mgr));
    if (!mgr) return NULL;

    pthread_mutex_init(&mgr->lock, NULL);
    mgr->local_id = local_id;
    snprintf(mgr->local_addr,        sizeof(mgr->local_addr),        "%s", local_addr   ? local_addr   : "");
    snprintf(mgr->local_gossip_addr, sizeof(mgr->local_gossip_addr), "%s", gossip_addr  ? gossip_addr  : "");

    mgr->ring = tsdb_hashring_new();
    if (!mgr->ring) { free(mgr); return NULL; }

    mgr->rng_state = (uint64_t)local_id ^ (uint64_t)now_ns();

    /* Register self. */
    tsdb_node_info_t self = {0};
    self.id = local_id;
    snprintf(self.addr,         sizeof(self.addr),        "%s", local_addr  ? local_addr  : "");
    snprintf(self.gossip_addr,  sizeof(self.gossip_addr), "%s", gossip_addr ? gossip_addr : "");
    self.state   = TSDB_NODE_ALIVE;
    self.version = 1;
    self.last_heartbeat_ns = now_ns();

    mgr->nodes[mgr->nnodes++] = self;
    tsdb_hashring_add(mgr->ring, local_id);

    return mgr;
}

void tsdb_node_manager_free(tsdb_node_manager_t *mgr) {
    if (!mgr) return;
    tsdb_hashring_free(mgr->ring);
    pthread_mutex_destroy(&mgr->lock);
    free(mgr);
}

tsdb_node_id_t tsdb_node_manager_local_id(tsdb_node_manager_t *mgr) {
    return mgr ? mgr->local_id : 0;
}

const char *tsdb_node_manager_local_addr(tsdb_node_manager_t *mgr) {
    return mgr ? mgr->local_addr : NULL;
}

/* ---- Internal lookup (must hold lock) ------------------------------------ */

static int find_node(tsdb_node_manager_t *mgr, tsdb_node_id_t id) {
    for (int i = 0; i < mgr->nnodes; i++) {
        if (mgr->nodes[i].id == id) return i;
    }
    return -1;
}

/* ---- Public membership ops ----------------------------------------------- */

void tsdb_node_manager_upsert(tsdb_node_manager_t *mgr,
                               const tsdb_node_info_t *info)
{
    if (!mgr || !info) return;
    pthread_mutex_lock(&mgr->lock);

    int idx = find_node(mgr, info->id);
    if (idx < 0) {
        if (mgr->nnodes >= TSDB_CLUSTER_MAX_NODES) {
            pthread_mutex_unlock(&mgr->lock);
            return;
        }
        idx = mgr->nnodes++;
        mgr->nodes[idx] = *info;
        /* Add to hashring only if ALIVE. */
        if (info->state == TSDB_NODE_ALIVE || info->state == TSDB_NODE_JOINING) {
            tsdb_hashring_add(mgr->ring, info->id);
        }
    } else {
        if (info->version > mgr->nodes[idx].version) {
            tsdb_node_state_t old_state = mgr->nodes[idx].state;
            mgr->nodes[idx] = *info;

            /* Sync ring membership. */
            int was_in_ring = (old_state == TSDB_NODE_ALIVE || old_state == TSDB_NODE_JOINING);
            int now_in_ring = (info->state == TSDB_NODE_ALIVE || info->state == TSDB_NODE_JOINING);
            if (!was_in_ring && now_in_ring) {
                tsdb_hashring_add(mgr->ring, info->id);
            } else if (was_in_ring && !now_in_ring) {
                tsdb_hashring_remove(mgr->ring, info->id);
            }
        }
    }

    pthread_mutex_unlock(&mgr->lock);
}

void tsdb_node_manager_suspect(tsdb_node_manager_t *mgr, tsdb_node_id_t id) {
    if (!mgr) return;
    pthread_mutex_lock(&mgr->lock);
    int idx = find_node(mgr, id);
    if (idx >= 0 && mgr->nodes[idx].state == TSDB_NODE_ALIVE) {
        mgr->nodes[idx].state = TSDB_NODE_SUSPECT;
        mgr->nodes[idx].suspect_count++;
        mgr->nodes[idx].version++;
        tsdb_hashring_remove(mgr->ring, id);
    }
    pthread_mutex_unlock(&mgr->lock);
}

void tsdb_node_manager_dead(tsdb_node_manager_t *mgr, tsdb_node_id_t id) {
    if (!mgr) return;
    pthread_mutex_lock(&mgr->lock);
    int idx = find_node(mgr, id);
    if (idx >= 0) {
        mgr->nodes[idx].state = TSDB_NODE_DEAD;
        mgr->nodes[idx].version++;
        tsdb_hashring_remove(mgr->ring, id);
    }
    pthread_mutex_unlock(&mgr->lock);
}

void tsdb_node_manager_alive(tsdb_node_manager_t *mgr, tsdb_node_id_t id,
                              uint64_t new_version)
{
    if (!mgr) return;
    pthread_mutex_lock(&mgr->lock);
    int idx = find_node(mgr, id);
    if (idx >= 0) {
        tsdb_node_state_t old_state = mgr->nodes[idx].state;
        mgr->nodes[idx].state = TSDB_NODE_ALIVE;
        mgr->nodes[idx].suspect_count = 0;
        if (new_version > mgr->nodes[idx].version) {
            mgr->nodes[idx].version = new_version;
        }
        mgr->nodes[idx].last_heartbeat_ns = now_ns();
        if (old_state != TSDB_NODE_ALIVE) {
            tsdb_hashring_add(mgr->ring, id);
        }
    }
    pthread_mutex_unlock(&mgr->lock);
}

void tsdb_node_manager_heartbeat(tsdb_node_manager_t *mgr) {
    if (!mgr) return;
    pthread_mutex_lock(&mgr->lock);
    int idx = find_node(mgr, mgr->local_id);
    if (idx >= 0) {
        mgr->nodes[idx].last_heartbeat_ns = now_ns();
        mgr->nodes[idx].version++;
    }
    pthread_mutex_unlock(&mgr->lock);
}

int tsdb_node_manager_snapshot(tsdb_node_manager_t *mgr,
                                tsdb_node_info_t *out_buf, int buf_cap)
{
    if (!mgr || !out_buf || buf_cap <= 0) return 0;
    pthread_mutex_lock(&mgr->lock);
    int n = mgr->nnodes < buf_cap ? mgr->nnodes : buf_cap;
    memcpy(out_buf, mgr->nodes, n * sizeof(tsdb_node_info_t));
    pthread_mutex_unlock(&mgr->lock);
    return n;
}

int tsdb_node_manager_get(tsdb_node_manager_t *mgr, tsdb_node_id_t id,
                           tsdb_node_info_t *out)
{
    if (!mgr || !out) return -1;
    pthread_mutex_lock(&mgr->lock);
    int idx = find_node(mgr, id);
    if (idx >= 0) *out = mgr->nodes[idx];
    pthread_mutex_unlock(&mgr->lock);
    return idx >= 0 ? 0 : -1;
}

int tsdb_node_manager_random_alive(tsdb_node_manager_t *mgr,
                                    tsdb_node_id_t *out, int count)
{
    if (!mgr || !out || count <= 0) return 0;
    pthread_mutex_lock(&mgr->lock);

    /* Collect ALIVE nodes excluding self. */
    tsdb_node_id_t alive[TSDB_CLUSTER_MAX_NODES];
    int nalive = 0;
    for (int i = 0; i < mgr->nnodes; i++) {
        if (mgr->nodes[i].id != mgr->local_id &&
            mgr->nodes[i].state == TSDB_NODE_ALIVE) {
            alive[nalive++] = mgr->nodes[i].id;
        }
    }

    /* Fisher-Yates partial shuffle to pick `count` random entries. */
    int picked = 0;
    for (int i = 0; i < nalive && picked < count; i++) {
        uint64_t r = xorshift64(&mgr->rng_state) % (uint64_t)(nalive - i);
        int j = i + (int)r;
        tsdb_node_id_t tmp = alive[i];
        alive[i] = alive[j];
        alive[j] = tmp;
        out[picked++] = alive[i];
    }

    pthread_mutex_unlock(&mgr->lock);
    return picked;
}

tsdb_hashring_t *tsdb_node_manager_ring(tsdb_node_manager_t *mgr) {
    return mgr ? mgr->ring : NULL;
}

int tsdb_node_manager_ring_owner(tsdb_node_manager_t *mgr,
                                  const char *key,
                                  int max_replicas,
                                  tsdb_node_id_t *out_nodes)
{
    if (!mgr || !key || !out_nodes || max_replicas <= 0) return 0;
    pthread_mutex_lock(&mgr->lock);
    int n = tsdb_hashring_owner_str(mgr->ring, key, max_replicas, out_nodes);
    pthread_mutex_unlock(&mgr->lock);
    return n;
}

int tsdb_node_manager_alive_count(tsdb_node_manager_t *mgr) {
    if (!mgr) return 0;
    pthread_mutex_lock(&mgr->lock);
    int n = 0;
    for (int i = 0; i < mgr->nnodes; i++) {
        if (mgr->nodes[i].state == TSDB_NODE_ALIVE) n++;
    }
    pthread_mutex_unlock(&mgr->lock);
    return n;
}

/* ---- Binary encode / decode for STATE_SYNC ------------------------------- */

/*
 * On-wire node record (fixed 80 bytes LE):
 *   id            u64
 *   addr          48 bytes (null-padded)
 *   gossip_addr   48 bytes (null-padded) -- reuse addr field space
 *   -- packed in 80: id(8)+addr(48)+state(1)+_pad(3)+version(8)+hb_ns(8)+suspct(4)
 * Wait, 8+48+1+3+8+8+4 = 80. Perfect.
 */
#define NODE_WIRE_SIZE 80

int tsdb_node_manager_encode(tsdb_node_manager_t *mgr, uint8_t *buf, int cap) {
    if (!mgr || !buf) return -1;
    pthread_mutex_lock(&mgr->lock);

    int needed = mgr->nnodes * NODE_WIRE_SIZE;
    if (needed > cap) {
        pthread_mutex_unlock(&mgr->lock);
        return -1;
    }

    uint8_t *p = buf;
    for (int i = 0; i < mgr->nnodes; i++) {
        const tsdb_node_info_t *n = &mgr->nodes[i];
        memset(p, 0, NODE_WIRE_SIZE);

        uint64_t id = n->id;
        memcpy(p, &id, 8); p += 8;

        /* Pack addr + gossip_addr into 48 bytes total: 24 + 24. */
        int alen = (int)strlen(n->addr);
        if (alen > 23) alen = 23;
        memcpy(p, n->addr, alen); p += 24;

        int glen = (int)strlen(n->gossip_addr);
        if (glen > 23) glen = 23;
        memcpy(p, n->gossip_addr, glen); p += 24;

        p[0] = (uint8_t)n->state; p++;
        p[0] = 0; p[1] = 0; p[2] = 0; p += 3; /* pad */

        uint64_t ver = n->version;
        memcpy(p, &ver, 8); p += 8;

        int64_t hb = n->last_heartbeat_ns;
        memcpy(p, &hb, 8); p += 8;

        int32_t sc = n->suspect_count;
        memcpy(p, &sc, 4); p += 4;
    }

    int written = (int)(p - buf);
    pthread_mutex_unlock(&mgr->lock);
    return written;
}

void tsdb_node_manager_decode(tsdb_node_manager_t *mgr,
                               const uint8_t *buf, int len)
{
    if (!mgr || !buf || len < NODE_WIRE_SIZE) return;

    int nrecs = len / NODE_WIRE_SIZE;
    for (int i = 0; i < nrecs; i++) {
        const uint8_t *p = buf + i * NODE_WIRE_SIZE;
        tsdb_node_info_t info = {0};

        memcpy(&info.id, p, 8); p += 8;

        /* Skip local node updates from gossip. */
        if (info.id == mgr->local_id) continue;

        memcpy(info.addr,        p, 24); info.addr[23]        = '\0'; p += 24;
        memcpy(info.gossip_addr, p, 24); info.gossip_addr[23] = '\0'; p += 24;

        info.state = (tsdb_node_state_t)p[0]; p++;
        p += 3; /* pad */

        memcpy(&info.version, p, 8); p += 8;
        memcpy(&info.last_heartbeat_ns, p, 8); p += 8;
        memcpy(&info.suspect_count, p, 4); p += 4;

        tsdb_node_manager_upsert(mgr, &info);
    }
}
