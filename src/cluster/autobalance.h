/* autobalance.h — Auto-balance controller for tsdb cluster.
 *
 * The balancer monitors per-node load (writes/sec, storage bytes)
 * and adjusts each node's virtual-node count in the consistent hash ring
 * so that overloaded nodes receive fewer new shards over time.
 *
 * Algorithm:
 *   1. Each node tracks its own writes_sec and storage_bytes via atomic
 *      counters updated by the write path.
 *   2. Every 30 s the balancer:
 *       a. Collects load stats for all ALIVE nodes (from gossip-propagated
 *          tsdb_node_load_t piggybacked on STATE_SYNC).
 *       b. Computes a load score: score = alpha*writes_norm + beta*storage_norm
 *          where *_norm is each metric normalised to [0,1] across all nodes.
 *       c. Derives target VN count: VN_target(i) = BASE_VN * (1 - dampen * score_i)
 *          clamped to [VN_MIN, VN_MAX].
 *       d. Applies tsdb_hashring_set_weight() for each node.
 *   3. When a key's primary owner changes (detected by comparing current vs.
 *      previous ring snapshot), the *old* primary migrates the partition data
 *      to the *new* primary via rawblock push (best-effort, async).
 *
 * Back-pressure: if a node's ring weight drops to VN_MIN for 3 consecutive
 * cycles, it emits a WARN log but does not drop below VN_MIN.
 *
 * Configuration via environment variables (all optional):
 *   TSDB_BALANCE_ALPHA    weight of writes_sec in score    (default 0.6)
 *   TSDB_BALANCE_BETA     weight of storage_bytes in score (default 0.4)
 *   TSDB_BALANCE_DAMPEN   fraction of score applied to VN (default 0.5)
 *   TSDB_BALANCE_INTERVAL_MS  rebalance interval ms        (default 30000)
 */
#ifndef TSDB_CLUSTER_AUTOBALANCE_H
#define TSDB_CLUSTER_AUTOBALANCE_H

#include "node.h"
#include "hashring.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Load snapshot for one node, propagated via gossip. */
typedef struct {
    tsdb_node_id_t node_id;
    uint64_t       writes_sec;     /* exponential-moving-average rows/sec */
    uint64_t       storage_bytes;  /* current on-disk bytes */
    uint32_t       cpu_pct_x100;   /* CPU usage * 100 (e.g. 7525 = 75.25%) */
} tsdb_node_load_t;

/* Wire size for one tsdb_node_load_t record in gossip payload. */
#define TSDB_NODE_LOAD_WIRE_SIZE  28  /* 8+8+8+4 */

/* VN count bounds for weighted ring. */
#define TSDB_BALANCE_VN_BASE  256
#define TSDB_BALANCE_VN_MIN    32
#define TSDB_BALANCE_VN_MAX   512

/* Opaque balancer object. */
typedef struct tsdb_autobalance tsdb_autobalance_t;

/*
 * Create and start the auto-balance controller.
 *
 * node_mgr   - shared membership + ring state
 * local_id   - this node's ID
 * db_data_dir - path to on-disk data (for storage_bytes calculation)
 *
 * Starts a background pthread that runs every interval_ms milliseconds.
 * Returns NULL on failure.
 */
tsdb_autobalance_t *tsdb_autobalance_new(tsdb_node_manager_t *node_mgr,
                                         tsdb_node_id_t local_id,
                                         const char *db_data_dir);

/* Stop the background thread and free resources. */
void tsdb_autobalance_free(tsdb_autobalance_t *ab);

/*
 * Called by the write path on every committed batch.
 * row_count is added to the internal writes counter; the balancer uses
 * this to compute EMA writes/sec.
 */
void tsdb_autobalance_record_write(tsdb_autobalance_t *ab, uint64_t row_count);

/*
 * Merge a gossip-received load payload into the local load table.
 * Called by gossip when it receives a TSDB_GOSSIP_LOAD_SYNC message.
 * buf / len: raw wire bytes (array of TSDB_NODE_LOAD_WIRE_SIZE records).
 */
void tsdb_autobalance_merge_load(tsdb_autobalance_t *ab,
                                  const uint8_t *buf, int len);

/*
 * Serialise this node's current load stats into buf for gossip piggybacking.
 * Returns bytes written or -1.
 */
int tsdb_autobalance_encode_load(tsdb_autobalance_t *ab,
                                  uint8_t *buf, int cap);

/*
 * Return a human-readable status string (JSON-like).
 * Fills buf up to cap bytes.  Returns bytes written.
 */
int tsdb_autobalance_stats_str(tsdb_autobalance_t *ab, char *buf, int cap);

/*
 * Force an immediate rebalance cycle (for testing / CLI STATS command).
 * Returns 0 on success.
 */
int tsdb_autobalance_rebalance_now(tsdb_autobalance_t *ab);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_CLUSTER_AUTOBALANCE_H */
