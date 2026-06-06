/* io_async.h — thin io_uring wrapper for async disk I/O.
 *
 * Phase 1 of the multi-iteration architecture upgrade (kdb+/Kafka/Scylla/
 * PostgreSQL-flavored, see docs/tasks/stress-100g.md).  Modern Linux
 * (5.1+) ships io_uring; submit-and-wait amortises syscall cost across
 * many ops and lets the kernel pipeline disk I/O without per-op blocking.
 * Sync fsync()/write() are the syscall-CPU hotspot perf flagged on the
 * leader during the 480 s cliff test.
 *
 * Build:
 *   - Linux + liburing-dev: define TSDB_USE_IOURING at compile time and
 *     link with -luring.  The Makefile detects liburing via pkg-config.
 *   - Anything else: this module compiles into a stub that returns
 *     "not available" so callers fall back to plain syscalls.  No new
 *     dependency on non-Linux platforms.
 *
 * Threading:
 *   - One tsdb_io_async_t per logical "submitter".  Rings are NOT
 *     thread-safe, so do not share an instance across threads without
 *     external serialisation.  Typical use: one instance per worker
 *     thread (thread-local) or per db (under db->lock).
 *
 * Lifecycle:
 *   - tsdb_io_async_create() at startup → use → tsdb_io_async_destroy()
 *     at shutdown.  Setup mmaps SQ/CQ rings (~16 KiB on default depth);
 *     not free, so amortise per long-lived submitter.
 */
#ifndef TSDB_STORAGE_IO_ASYNC_H
#define TSDB_STORAGE_IO_ASYNC_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_io_async tsdb_io_async_t;

/* Compile-time capability check.  Returns 1 if this build supports real
 * io_uring (i.e., TSDB_USE_IOURING was set and liburing was linked), 0
 * otherwise.  Doesn't try to probe the running kernel — that happens at
 * tsdb_io_async_create() time. */
int  tsdb_io_async_available(void);

/* Create an instance with SQ depth `entries` (clamped to [8, 4096], default
 * 64 if 0).  Returns 0 + sets *out, or -1 on error with errno set.
 *
 * Non-uring build: succeeds, returns a stub instance that does sync
 * fallback on each submit. */
int  tsdb_io_async_create(unsigned entries, tsdb_io_async_t **out);

/* Tear down an instance.  Safe on NULL. */
void tsdb_io_async_destroy(tsdb_io_async_t *io);

/* Submit an fsync for fd to the ring.  user_data is echoed back on the
 * completion entry; pick any uint64_t tag (request seq, pointer, etc).
 * Returns 0 on success, -1 if the SQ is full or fd invalid.
 *
 * Stub build: invokes fsync(fd) inline and pretends a completion is
 * waiting (drain returns immediately). */
int  tsdb_io_async_submit_fsync(tsdb_io_async_t *io, int fd, uint64_t user_data);

/* Submit any queued SQEs to the kernel.  Returns the number actually
 * submitted (may be less than what was prepared if errors).  Required
 * before drain() can see any completions.
 *
 * Stub build: returns 0 (nothing queued — submit_fsync already did it). */
int  tsdb_io_async_submit(tsdb_io_async_t *io);

/* Block until at least min_completions completions are ready, then drain
 * up to max_n of them.  Fills out_user_data[] with the corresponding tags
 * (in completion order, NOT submission order) and out_errors with the
 * count of completions whose res field was negative.  Either array may
 * be NULL.
 *
 * Returns the number of completions delivered.
 *
 * Stub build: returns min_completions (or max_n if less), filling
 * out_user_data with stub-tracked tags and out_errors with 0. */
int  tsdb_io_async_drain(tsdb_io_async_t *io,
                          int min_completions,
                          uint64_t *out_user_data, int max_n,
                          int *out_errors);

/* Convenience helper: submit_fsync + submit + drain(1) in one call.
 * Equivalent to plain fsync(fd) semantically but goes through io_uring
 * on supported builds.  Used by integration points that want async
 * infra without batching yet. */
int  tsdb_io_async_fsync_sync(tsdb_io_async_t *io, int fd);

#ifdef __cplusplus
}
#endif
#endif /* TSDB_STORAGE_IO_ASYNC_H */
