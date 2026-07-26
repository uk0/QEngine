/* wal.h — Write-Ahead Log.
 *
 * Each record is: [crc32 u32][len u32][payload u8[len]]
 * File path: <db_dir>/wal/<table_name>.log
 * fsync is explicit via tsdb_wal_sync().
 *
 * Group-commit
 * ============
 * tsdb_group_commit_t implements a background-thread fsync batcher.
 * Multiple callers of tsdb_wal_append_sync() block on a condition variable
 * after appending their record.  A single background thread wakes every
 * batch_window_ns (or when nwaiters >= TSDB_GC_EAGER_WAITERS) and issues
 * one fsync for all pending records, then broadcasts to unblock waiters.
 *
 * Callers share a monotonically increasing write_seq; the bg thread bumps
 * fsynced_seq to the latest write_seq after each fsync.  A waiter returns
 * as soon as fsynced_seq >= its captured seq.
 *
 * All append_sync calls on a single tsdb_wal_t that share the same
 * tsdb_group_commit_t are serialized by the group-commit mutex — do NOT
 * mix tsdb_wal_append() and tsdb_wal_append_sync() on the same fd.
 */
#ifndef TSDB_STORAGE_WAL_H
#define TSDB_STORAGE_WAL_H

#include "../../include/tsdb.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_wal tsdb_wal_t;

/*
 * Open (or create) a WAL file for the given table.
 * db_dir/wal/<table_name>.log is created if absent.
 */
int  tsdb_wal_open(const char *db_dir, const char *table_name, tsdb_wal_t **out);

/* Close the WAL file. Does not sync or truncate. */
void tsdb_wal_close(tsdb_wal_t *w);

/*
 * Append a record to the WAL.
 * rec/n is the raw payload. CRC is computed internally.
 */
int  tsdb_wal_append(tsdb_wal_t *w, const void *rec, size_t n);

/* fsync the WAL file to durable storage. */
int  tsdb_wal_sync(tsdb_wal_t *w);

/*
 * Append a record AND make it durable as one all-or-nothing unit: on any
 * failure the log is left byte-identical to what it was before the call.
 * tsdb_wal_append already rolls back its own short/failed write; this adds
 * the same rollback to a fsync that fails AFTER the bytes reached the file
 * (the normal disk-full shape on a delayed-allocation filesystem: the
 * writev lands in the page cache and only fsync reports ENOSPC).
 *
 * Callers that key a record's identity on a sequence number they only
 * advance on success MUST use this: a record left live under a seq that is
 * then reused makes replay apply both records, duplicating the rows they
 * overlap on.  Serialised by the caller, exactly like tsdb_wal_append.
 */
int  tsdb_wal_append_durable(tsdb_wal_t *w, const void *rec, size_t n);

/* Truncate the WAL file to zero (called after successful memtable flush). */
int  tsdb_wal_truncate(tsdb_wal_t *w);

/*
 * Repair a torn tail left by a crash/power cut: truncate the log to the end of
 * its last CRC-valid record.  tsdb_wal_open() calls this before it returns the
 * handle, so an appending fd can never run past a torn tail; it is exported so
 * tests can exercise it directly.
 * *discarded_out (may be NULL) receives the number of bytes dropped (0 = the
 * log was already clean).  Returns TSDB_OK, or TSDB_ERR_IO if the log could
 * not be read or truncated — in that case the file is left EXACTLY as it was.
 */
int  tsdb_wal_repair(tsdb_wal_t *w, uint64_t *discarded_out);

/*
 * Replay all records in the WAL file.
 * For each record the callback is invoked with the payload.
 * Returns TSDB_OK, or TSDB_ERR_CORRUPT if a record has bad CRC, or
 * any non-zero value returned by the callback.
 */
int tsdb_wal_replay(const char *db_dir, const char *table_name,
                    int (*cb)(const void *rec, size_t n, void *ctx), void *ctx);

/* ---- Group-commit API ---------------------------------------------------- */

/*
 * Number of waiting callers that triggers an immediate fsync without waiting
 * for the full batch_window_ns to expire.
 */
#define TSDB_GC_EAGER_WAITERS 8

typedef struct tsdb_group_commit tsdb_group_commit_t;

/*
 * Start a group-commit background thread attached to WAL w.
 * batch_window_ns: maximum time (nanoseconds) a commit waits for batching.
 *                  Typical values: 500_000 (0.5 ms) - 2_000_000 (2 ms).
 * Returns TSDB_OK on success; *out is valid until tsdb_group_commit_stop().
 */
int  tsdb_group_commit_start(tsdb_wal_t *w, int64_t batch_window_ns,
                              tsdb_group_commit_t **out);

/*
 * Stop the group-commit background thread.
 * Issues a final fsync and wakes all blocked waiters before joining.
 * Safe to call with g == NULL (no-op).
 */
void tsdb_group_commit_stop(tsdb_group_commit_t *g);

/*
 * Append a record to the WAL and wait until it has been fsync'd to disk.
 * Under group-commit the actual fsync is performed by the background thread,
 * potentially coalescing this write with other concurrent writers.
 *
 * Returns TSDB_OK when the data is durable.
 * Thread-safe: multiple threads may call this concurrently.
 * Must NOT be mixed with plain tsdb_wal_append() / tsdb_wal_sync() on the
 * same tsdb_wal_t instance.
 */
int  tsdb_wal_append_sync(tsdb_wal_t *w, tsdb_group_commit_t *g,
                           const void *rec, size_t n);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_WAL_H */
