/* raft_config.h — persistent master-set tracking for Raft membership.
 *
 * MVP scope: just an ordered list of (node_id, "addr") pairs that
 * describes the current consensus membership.  This list feeds
 * quorum_needed() so a single node can be added/removed via a
 * CONFIG log entry without every node ever computing a stale
 * majority.
 *
 * Bootstrapping: if config.bin doesn't exist at open(), fall back to
 * the gossip-derived snapshot (all ALIVE masters at the moment of
 * first call).  That's "implicit bootstrap" — good enough for a
 * cluster that was already running pre-Raft-config, and correct
 * because every node agrees on the gossip snapshot at boot.
 */
#ifndef TSDB_RAFT_CONFIG_H
#define TSDB_RAFT_CONFIG_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_RAFT_CFG_MAX_MASTERS 16

typedef struct {
    uint64_t id;
    char     addr[80];    /* rpc "host:port" */
} tsdb_raft_cfg_member_t;

typedef struct tsdb_raft_config tsdb_raft_config_t;

/* Open (or create) the config rooted at <data_dir>/raft/.  Returns
 * NULL on I/O failure. */
tsdb_raft_config_t *tsdb_raft_config_open(const char *data_dir);
void                tsdb_raft_config_close(tsdb_raft_config_t *cfg);

/* Returns 1 iff the on-disk config existed at open (vs. needing
 * implicit bootstrap). */
int tsdb_raft_config_is_initialised(const tsdb_raft_config_t *cfg);

/* Populate from a list (used by bootstrap + by InstallSnapshot).
 * Persists + fsyncs atomically.  Returns 0 / -1. */
int tsdb_raft_config_set(tsdb_raft_config_t *cfg,
                          const tsdb_raft_cfg_member_t *members, int n);

/* Add/remove one member; persists.  Idempotent — adding an existing
 * member or removing a missing one is a no-op. */
int tsdb_raft_config_add   (tsdb_raft_config_t *cfg,
                             uint64_t id, const char *addr);
int tsdb_raft_config_remove(tsdb_raft_config_t *cfg, uint64_t id);

/* Snapshot the current membership into caller-supplied buffer.
 * Returns the count actually written (<=cap). */
int tsdb_raft_config_snapshot(tsdb_raft_config_t *cfg,
                               tsdb_raft_cfg_member_t *out, int cap);

/* Count of configured members. */
int tsdb_raft_config_count(tsdb_raft_config_t *cfg);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_RAFT_CONFIG_H */
