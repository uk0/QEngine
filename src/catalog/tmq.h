/* tmq.h — TMQ consumer-group metadata (Kafka-compatible semantics).
 *
 * Scope (local only):
 *   - Consumer groups are per-node; no cross-node rebalance.
 *   - A "topic" is an existing pub/sub table name.
 *   - Each group has a fixed partition count (TSDB_TMQ_NPARTITIONS).
 *   - Rebalance is simple round-robin on join/leave.
 *   - Offsets are monotone int64; commit rejects decreasing values.
 *
 * Persistence:
 *   <data_dir>/catalog/consumer_groups.log  (append-only, percent-encoded).
 *
 *   Record ops (tab-separated):
 *     +group   <name> <topic> <npart> <created_at>
 *     -group   <name>
 *     +member  <group> <consumer_id> <joined_at>
 *     -member  <group> <consumer_id>
 *     ~offset  <group> <partition> <seq>
 *
 *   On open, log is replayed top-to-bottom; tombstones remove entries.
 */
#ifndef TSDB_CATALOG_TMQ_H
#define TSDB_CATALOG_TMQ_H

#include "../../include/tsdb.h"
#include "../core/types.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Fixed partition count per group. */
#define TSDB_TMQ_NPARTITIONS   4
#define TSDB_TMQ_NAME_MAX      64
#define TSDB_TMQ_ID_MAX        64
#define TSDB_TMQ_TOPIC_MAX     64
#define TSDB_TMQ_MAX_MEMBERS   64
#define TSDB_TMQ_MAX_GROUPS    64

typedef struct {
    char        consumer_id[TSDB_TMQ_ID_MAX];
    tsdb_ts_t   joined_at;
} tsdb_tmq_member_t;

typedef struct {
    char               name[TSDB_TMQ_NAME_MAX];
    char               topic[TSDB_TMQ_TOPIC_MAX];
    int                npart;    /* equal to TSDB_TMQ_NPARTITIONS for now */
    tsdb_ts_t          created_at;

    tsdb_tmq_member_t  members[TSDB_TMQ_MAX_MEMBERS];
    int                nmembers;

    /* partition_owner[p] = index into members[] (or -1 if unowned). */
    int                partition_owner[TSDB_TMQ_NPARTITIONS];

    /* committed_offsets[p] — seq per partition (shared across members).
     * Survives member handover so Kafka-like consumer-group semantics hold:
     * the partition's committed position persists when ownership changes. */
    int64_t            committed_offsets[TSDB_TMQ_NPARTITIONS];
} tsdb_tmq_group_t;

typedef struct tsdb_tmq tsdb_tmq_t;

/* ---- Lifecycle --------------------------------------------------------- */

/* Open the consumer-group store rooted at <data_dir>/catalog/. Replays
 * consumer_groups.log if present. Returns TSDB_OK on success. */
int  tsdb_tmq_open(const char *data_dir, tsdb_tmq_t **out);

void tsdb_tmq_close(tsdb_tmq_t *t);

/* ---- Group lifecycle --------------------------------------------------- */

/* Create a new consumer group bound to <topic>.
 *   TSDB_ERR_EXISTS  — group already exists
 *   TSDB_ERR_INVAL   — bad name / topic
 *   TSDB_ERR_FULL    — too many groups
 */
int tsdb_tmq_group_create(tsdb_tmq_t *t,
                          const char *name,
                          const char *topic);

/* Drop a group (tombstone). Returns TSDB_ERR_NOTFOUND if absent. */
int tsdb_tmq_group_drop(tsdb_tmq_t *t, const char *name);

/* Snapshot group state into *out. Returns TSDB_ERR_NOTFOUND if absent. */
int tsdb_tmq_group_get(tsdb_tmq_t *t, const char *name, tsdb_tmq_group_t *out);

/* ---- Membership -------------------------------------------------------- */

/* Add consumer to group, re-running rebalance. Rejected if member exists. */
int tsdb_tmq_join(tsdb_tmq_t *t,
                  const char *group,
                  const char *consumer_id);

/* Remove consumer from group, re-running rebalance. */
int tsdb_tmq_leave(tsdb_tmq_t *t,
                   const char *group,
                   const char *consumer_id);

/* ---- Offsets ----------------------------------------------------------- */

/* Commit a seq.
 *   consumer_id == NULL → apply seq to every partition of the group
 *                         (matches the spec's `COMMIT OFFSET <name> AT <seq>`).
 *   consumer_id != NULL → apply seq only to partitions currently owned
 *                         by that member.
 * Rejects decreasing seq (returns TSDB_ERR_INVAL). */
int tsdb_tmq_commit(tsdb_tmq_t *t,
                    const char *group,
                    const char *consumer_id,   /* may be NULL */
                    int64_t seq);

/* Read the committed offset for a partition. Offsets live on the group,
 * so consumer_id is accepted for convenience but ignored (pass NULL).
 * Returns TSDB_ERR_NOTFOUND if group missing.                            */
int tsdb_tmq_get_offset(tsdb_tmq_t *t,
                        const char *group,
                        const char *consumer_id,   /* may be NULL */
                        int partition,
                        int64_t *out_seq);

/* ---- Enumeration (for tests and introspection) ------------------------ */

/* List all groups. Caller frees the returned array with free(). */
int tsdb_tmq_list(tsdb_tmq_t *t,
                  tsdb_tmq_group_t **out_arr,
                  size_t *out_n);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CATALOG_TMQ_H */
