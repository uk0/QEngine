/* catalog_id.c — node-prefixed OID allocator (Track B P1).  See catalog_id.h. */
#include "../../include/catalog_id.h"
#include "../../include/tsdb.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define OID_ALLOC_BLOCK ((uint64_t)4096)   /* ids reserved (and fsync'd) per block */

/* Overwrite OIDSEQ with `high` and fsync, so a restart resumes at or past it. */
static int oid_persist(tsdb_oid_alloc_t *a, uint64_t high) {
    char tmp[4128];
    snprintf(tmp, sizeof(tmp), "%s.tmp", a->path);
    FILE *f = fopen(tmp, "w");
    if (!f) return TSDB_ERR_IO;
    int ok = (fprintf(f, "%llu\n", (unsigned long long)high) > 0);
    if (fflush(f) != 0) ok = 0;
    if (ok) fsync(fileno(f));
    fclose(f);
    if (!ok) { unlink(tmp); return TSDB_ERR_IO; }
    if (rename(tmp, a->path) != 0) { unlink(tmp); return TSDB_ERR_IO; }
    return TSDB_OK;
}

int tsdb_oid_alloc_open(tsdb_oid_alloc_t *a, const char *catalog_dir, uint16_t node_id) {
    if (!a || !catalog_dir) return TSDB_ERR_INVAL;
    memset(a, 0, sizeof(*a));
    a->node_id = node_id;
    pthread_mutex_init(&a->lock, NULL);
    a->inited = 1;
    snprintf(a->path, sizeof(a->path), "%s/OIDSEQ", catalog_dir);

    uint64_t persisted = 0;
    FILE *f = fopen(a->path, "r");
    if (f) {
        unsigned long long v = 0;
        if (fscanf(f, "%llu", &v) == 1) persisted = (uint64_t)v;
        fclose(f);
    }
    if (persisted < TSDB_OID_FIRST_USR) persisted = TSDB_OID_FIRST_USR;
    /* Resume from the persisted high-water; the previous run reserved up to here
     * and may have left some ids in its final block unused — skipping them keeps
     * the no-reuse guarantee (ids need not be contiguous). */
    a->next     = persisted;
    a->reserved = persisted;   /* nothing reserved beyond this yet */
    return TSDB_OK;
}

tsdb_oid_t tsdb_oid_next(tsdb_oid_alloc_t *a) {
    if (!a || !a->inited) return TSDB_OID_NONE;
    pthread_mutex_lock(&a->lock);
    if (a->next >= a->reserved) {
        uint64_t newhigh = a->next + OID_ALLOC_BLOCK;
        if (oid_persist(a, newhigh) != TSDB_OK) {
            pthread_mutex_unlock(&a->lock);
            return TSDB_OID_NONE;
        }
        a->reserved = newhigh;
    }
    uint64_t seq = a->next++;
    uint16_t node = a->node_id;
    pthread_mutex_unlock(&a->lock);
    return tsdb_oid_make(node, seq);
}

void tsdb_oid_observe(tsdb_oid_alloc_t *a, tsdb_oid_t oid) {
    if (!a || !a->inited) return;
    if (tsdb_oid_node(oid) != a->node_id) return;   /* other node's id — can't collide */
    uint64_t seq = tsdb_oid_seq(oid);
    pthread_mutex_lock(&a->lock);
    if (seq >= a->next) {
        a->next = seq + 1;
        if (a->next > a->reserved) {
            uint64_t newhigh = a->next + OID_ALLOC_BLOCK;
            if (oid_persist(a, newhigh) == TSDB_OK) a->reserved = newhigh;
        }
    }
    pthread_mutex_unlock(&a->lock);
}

void tsdb_oid_alloc_close(tsdb_oid_alloc_t *a) {
    if (!a || !a->inited) return;
    pthread_mutex_destroy(&a->lock);
    a->inited = 0;
}
