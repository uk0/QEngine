/* catalog_id.h — stable object identity for the metadata catalog (Track B P1).
 *
 * The v2 catalog replaces denormalized string foreign keys with stable 64-bit
 * object ids (OIDs).  An OID is identity; a name is a mutable attribute.  The
 * id space is partitioned by node so any node mints globally-unique ids locally
 * with no consensus round-trip on the CREATE hot path:
 *
 *     oid = (node_id << 48) | local_seq        node_id: 16 bits   seq: 48 bits
 *
 * Distinct node_ids never collide (different high 16 bits); within a node the
 * monotonic local_seq is unique.  A force-recreated node with a fresh node_id
 * simply opens a non-overlapping sub-range, so even a split-brain dual-create
 * yields two distinct ids rather than a silent alias.
 *
 * Reserved low ids are FIXED cluster-wide (node_id 0) so every node agrees on
 * them with zero coordination — the `default` and `sysdb` databases are then
 * byte-identical everywhere.
 */
#ifndef TSDB_CATALOG_ID_H
#define TSDB_CATALOG_ID_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t tsdb_oid_t;

#define TSDB_OID_NONE       ((tsdb_oid_t)0)   /* null / root / no-parent */
#define TSDB_OID_DEFAULTDB  ((tsdb_oid_t)1)   /* the materialized "default" database */
#define TSDB_OID_SYSDB      ((tsdb_oid_t)2)   /* the system database */
#define TSDB_OID_FIRST_USR  ((tsdb_oid_t)1024)/* first id handed to a user object */

#define TSDB_OID_SEQ_BITS   48
#define TSDB_OID_SEQ_MASK   (((tsdb_oid_t)1 << TSDB_OID_SEQ_BITS) - 1)

/* Decompose an oid. */
static inline uint16_t   tsdb_oid_node(tsdb_oid_t o) { return (uint16_t)(o >> TSDB_OID_SEQ_BITS); }
static inline uint64_t   tsdb_oid_seq (tsdb_oid_t o) { return (uint64_t)(o & TSDB_OID_SEQ_MASK); }
static inline tsdb_oid_t tsdb_oid_make(uint16_t node, uint64_t seq) {
    return ((tsdb_oid_t)node << TSDB_OID_SEQ_BITS) | (seq & TSDB_OID_SEQ_MASK);
}

/* Allocator: hands out monotonic local_seq for one node, persisting a high-water
 * to <catalog_dir>/OIDSEQ so ids are never reused across restarts.  Reservation
 * is block-batched (a fsync every OID_ALLOC_BLOCK ids, not per id) — a restart
 * resumes from the last reserved high-water, skipping any unused ids in the
 * final block (ids need not be contiguous). */
typedef struct {
    uint16_t        node_id;
    uint64_t        next;        /* next local_seq to hand out */
    uint64_t        reserved;    /* persisted high-water; hand out while next < reserved */
    char            path[4096];  /* <catalog_dir>/OIDSEQ */
    pthread_mutex_t lock;
    int             inited;
} tsdb_oid_alloc_t;

/* Open the allocator for `catalog_dir` as `node_id`.  Creates/reads OIDSEQ.
 * Returns TSDB_OK or a negative error. */
int        tsdb_oid_alloc_open(tsdb_oid_alloc_t *a, const char *catalog_dir, uint16_t node_id);

/* Mint the next globally-unique oid for this node.  Thread-safe.  Returns
 * TSDB_OID_NONE only on a persistence failure. */
tsdb_oid_t tsdb_oid_next(tsdb_oid_alloc_t *a);

/* Note that an oid was observed (e.g. replayed from a peer/log): if it belongs
 * to THIS node and its seq >= next, advance past it so a future mint cannot
 * reissue it.  No-op for other nodes' ids. */
void       tsdb_oid_observe(tsdb_oid_alloc_t *a, tsdb_oid_t oid);

void       tsdb_oid_alloc_close(tsdb_oid_alloc_t *a);

#ifdef __cplusplus
}
#endif
#endif /* TSDB_CATALOG_ID_H */
