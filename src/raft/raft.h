/* raft.h — consensus state machine for the catalog layer.
 *
 * Scope:
 *   - Raft as described in Ongaro/Ousterhout, but restricted to a
 *     static master set (no membership change RPC yet) and no
 *     snapshots.  Both are planned as follow-ups once election +
 *     log replication are stable under the acceptance harness.
 *
 * Threading model:
 *   - One tick thread drives election timeout + heartbeat scheduling.
 *   - RPC threads call tsdb_raft_on_request_vote / _on_append_entries
 *     concurrently; all shared state is behind `r->lock`.
 *   - Client-facing tsdb_raft_propose runs on the caller thread,
 *     grabs the log, appends, waits on a condvar until committed.
 *
 * This module owns NOTHING about the catalog schema — it just stores
 * and replicates opaque byte payloads.  The apply callback provided
 * by the caller is where those bytes become catalog state.
 */
#ifndef TSDB_RAFT_H
#define TSDB_RAFT_H

#include <stdint.h>
#include <stddef.h>

#include "raft_log.h"
#include "raft_config.h"
#include "../cluster/node.h"
#include "../cluster/replica.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_raft tsdb_raft_t;

/* Callback invoked (from the apply thread, under r->lock released)
 * when a committed log entry should be applied to the state machine.
 * Return 0 on success, non-zero to signal a hard failure — the raft
 * module does NOT roll back on apply failure; the entry is considered
 * applied regardless (consistent with Raft's state-machine contract). */
typedef int (*tsdb_raft_apply_fn)(void *ud, const tsdb_raft_entry_t *entry);

/* Called when a peer must SNAPSHOT its state machine for shipment to
 * a far-behind follower.  Returns 0 on success, with *out pointing at
 * a malloc'd buffer the caller will free(); *out_len is the byte
 * count.  Returning -1 or leaving out=NULL tells the raft module
 * "can't snapshot right now, stay with AppendEntries". */
typedef int (*tsdb_raft_snapshot_write_fn)(void *ud,
                                            uint8_t **out,
                                            uint32_t *out_len);

/* Called when this node is a follower that just received a full
 * snapshot body from the leader.  The implementation is responsible
 * for replacing its local state machine state with the snapshot
 * contents.  Returns 0 on success, -1 to reject (leader will retry). */
typedef int (*tsdb_raft_snapshot_restore_fn)(void *ud,
                                              const uint8_t *data,
                                              uint32_t data_len);

/* Create a raft instance rooted at <data_dir>/raft/ for this node.
 *
 *  local_id    — this node's 64-bit id
 *  node_mgr    — used to enumerate current MASTER peers each tick
 *  replica_mgr — shared RPC connection pool for outbound calls
 *  apply_fn    — called when an entry is committed (may be NULL)
 *
 * The returned handle owns a tick thread that starts immediately.
 * Safe to call tsdb_raft_close during shutdown; joins the tick
 * thread + flushes state before returning. */
tsdb_raft_t *tsdb_raft_open(const char *data_dir,
                             uint64_t local_id,
                             tsdb_node_manager_t *node_mgr,
                             tsdb_replica_mgr_t  *replica_mgr,
                             tsdb_raft_apply_fn   apply_fn,
                             void                *apply_ud);

/* Attach optional snapshot callbacks.  Without these the log-only
 * compaction from task #41 still works; with them, InstallSnapshot
 * RPC can ship a catalog body to a far-behind follower. */
void tsdb_raft_set_snapshot_handlers(tsdb_raft_t *r,
                                      tsdb_raft_snapshot_write_fn   write_fn,
                                      tsdb_raft_snapshot_restore_fn restore_fn,
                                      void *ud);

void tsdb_raft_close(tsdb_raft_t *r);

/* ---- Introspection (for /raft JSON and tests) --------------------------- */

typedef enum {
    TSDB_RAFT_FOLLOWER  = 0,
    TSDB_RAFT_CANDIDATE = 1,
    TSDB_RAFT_LEADER    = 2
} tsdb_raft_state_t;

uint64_t          tsdb_raft_self_id      (tsdb_raft_t *r);
tsdb_raft_state_t tsdb_raft_state        (tsdb_raft_t *r);
uint64_t          tsdb_raft_current_term (tsdb_raft_t *r);
uint64_t          tsdb_raft_leader_id    (tsdb_raft_t *r);
uint64_t          tsdb_raft_commit_index (tsdb_raft_t *r);
uint64_t          tsdb_raft_last_applied (tsdb_raft_t *r);
uint64_t          tsdb_raft_last_index   (tsdb_raft_t *r);
uint64_t          tsdb_raft_snapshot_index(tsdb_raft_t *r);

/* Snapshot of the current master set.  `out` must have room for at
 * least TSDB_RAFT_CFG_MAX_MASTERS entries; returns the count
 * actually written. */
int               tsdb_raft_config_members(tsdb_raft_t *r,
                                            tsdb_raft_cfg_member_t *out,
                                            int cap);

/* Leader-only: propose an ADD / REMOVE MASTER.  Appends a config
 * entry to the Raft log; caller waits for commit via the normal
 * propose/apply path.  Returns TSDB_OK when applied cluster-wide. */
int               tsdb_raft_add_master(tsdb_raft_t *r,
                                        uint64_t id, const char *addr,
                                        int timeout_ms);
int               tsdb_raft_remove_master(tsdb_raft_t *r,
                                           uint64_t id,
                                           int timeout_ms);

/* ---- Public client API -------------------------------------------------- */

/* Submit `payload` as a new log entry of `type`.  Must be called on a
 * leader; returns TSDB_OK once the entry is committed AND applied, or:
 *   TSDB_ERR_PERMISSION  not leader — caller should redirect to
 *                        tsdb_raft_leader_id()
 *   TSDB_ERR_IO          log append or quorum replication failed
 *   TSDB_ERR_INVAL       null handle / payload too big */
int tsdb_raft_propose(tsdb_raft_t *r,
                       tsdb_raft_entry_type_t type,
                       const void *payload, uint32_t payload_len,
                       int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_RAFT_H */
