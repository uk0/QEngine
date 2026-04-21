/* raft_log.h — persistent Raft state (currentTerm / votedFor) + log.
 *
 * Minimal, single-process interface.  The higher-level state machine
 * in raft.c drives term/vote updates and log appends through this
 * module; all durability (fsync) lives here.
 *
 * On-disk layout under <data_dir>/raft/:
 *
 *   meta.bin             16 B  magic(4) + version(4) + currentTerm(u64)
 *   vote.bin             16 B  magic(4) + version(4) + votedFor(u64)
 *   log/seg-<start>.bin  append-only stream of entries
 *
 * We keep term and vote in two separate files so a crash between
 * "bumped term" and "cast vote" can never yield a "voted without
 * bumping" inconsistency.  Both files are rewritten whole + fsynced.
 *
 * Log entries are length-prefixed and carry their own term so a peer
 * synchronising from an empty log can always read back without
 * out-of-band metadata.  Truncation (for conflicting AppendEntries)
 * rewrites the tail segment in-place — no holes.
 */
#ifndef TSDB_RAFT_LOG_H
#define TSDB_RAFT_LOG_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_raft_log tsdb_raft_log_t;

/* Log entry types.  CATALOG_QTL carries the raw QTL string so peers
 * replay via tsdb_query; NOOP is what a fresh leader appends right
 * after winning an election so followers commit everything from the
 * previous term (Raft §5.4.2). */
typedef enum {
    TSDB_RAFT_ENTRY_NOOP        = 0,
    TSDB_RAFT_ENTRY_CATALOG_QTL = 1
} tsdb_raft_entry_type_t;

typedef struct {
    uint64_t index;        /* 1-based Raft log index */
    uint64_t term;         /* term in which leader created the entry */
    uint32_t type;         /* tsdb_raft_entry_type_t */
    uint32_t payload_len;  /* bytes in payload[] */
    void    *payload;      /* owned by caller of read; caller of append
                              copies into internal buffers */
} tsdb_raft_entry_t;

/* Open (or create) the raft persistence rooted at data_dir/raft/.
 * Creates the directory, the two metadata files, and the log segment
 * dir if missing.  Loads currentTerm / votedFor into memory.
 *
 * Returns NULL on I/O failure.  The caller holds ownership — call
 * tsdb_raft_log_close to release. */
tsdb_raft_log_t *tsdb_raft_log_open(const char *data_dir);

void tsdb_raft_log_close(tsdb_raft_log_t *rl);

/* ---- term / vote --------------------------------------------------------- */

uint64_t tsdb_raft_log_current_term(tsdb_raft_log_t *rl);
uint64_t tsdb_raft_log_voted_for  (tsdb_raft_log_t *rl);

/* Write currentTerm=term to disk, fsync, update in-memory state.  Also
 * resets votedFor to 0 since a higher term invalidates any prior vote.
 * Returns 0 on success, -1 on I/O failure (term is not updated). */
int tsdb_raft_log_set_term(tsdb_raft_log_t *rl, uint64_t term);

/* Record that we voted for `node_id` in the current term.  Persists
 * atomically before returning.  Returns 0 or -1. */
int tsdb_raft_log_set_voted_for(tsdb_raft_log_t *rl, uint64_t node_id);

/* ---- log ----------------------------------------------------------------- */

/* The 1-based index of the highest entry on disk (0 if the log is empty). */
uint64_t tsdb_raft_log_last_index(tsdb_raft_log_t *rl);

/* Term of the last on-disk entry (0 if empty). */
uint64_t tsdb_raft_log_last_term (tsdb_raft_log_t *rl);

/* Look up the term at a specific 1-based index.  Returns 0 if the
 * index is out of range (past the end or before the first kept entry). */
uint64_t tsdb_raft_log_term_at(tsdb_raft_log_t *rl, uint64_t index);

/* Append `entry` at index (last_index + 1).  The caller's `entry.index`
 * is ignored on input; on return it is set to the assigned index.
 * Term, type, payload_len, payload must all be set by the caller.
 * Payload bytes are copied into the log's own buffer before fsync.
 * Returns 0 on success, -1 on I/O failure. */
int tsdb_raft_log_append(tsdb_raft_log_t *rl, tsdb_raft_entry_t *entry);

/* Read the entry at `index` into `out`.  Allocates out->payload with
 * malloc; the caller must free it.  Returns 0 if found, -1 otherwise. */
int tsdb_raft_log_read(tsdb_raft_log_t *rl, uint64_t index,
                        tsdb_raft_entry_t *out);

/* Truncate the log so that the highest remaining entry is (index - 1).
 * Used when an AppendEntries conflict forces us to throw away our
 * tail.  Idempotent: truncating past the end is a no-op.  Returns 0
 * on success, -1 on I/O failure. */
int tsdb_raft_log_truncate(tsdb_raft_log_t *rl, uint64_t index);

/* ---- Snapshot support ---------------------------------------------------
 *
 * A snapshot captures state up through (last_snapshot_index,
 * last_snapshot_term).  The log can physically discard entries with
 * index <= last_snapshot_index because they're subsumed.  The
 * snapshot BODY (the catalog tarball) lives in a separate file
 * managed by the higher-level raft module; the log only records the
 * marker so AE consistency checks can still answer "term at
 * snapshot_index" after the entries themselves are gone.
 */

uint64_t tsdb_raft_log_snapshot_index(tsdb_raft_log_t *rl);
uint64_t tsdb_raft_log_snapshot_term (tsdb_raft_log_t *rl);

/* Mark (index, term) as covered by a snapshot, then drop every log
 * entry whose index is <= `index`.  Idempotent for indices that
 * don't advance the marker.  Returns 0 on success, -1 on I/O
 * failure. */
int tsdb_raft_log_compact(tsdb_raft_log_t *rl,
                           uint64_t index, uint64_t term);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_RAFT_LOG_H */
