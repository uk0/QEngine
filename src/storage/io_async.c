/* io_async.c — io_uring wrapper, see header for contract. */
#define _POSIX_C_SOURCE 200809L

#include "io_async.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

#if defined(__linux__) && defined(TSDB_USE_IOURING)
#  include <liburing.h>
#  define HAS_IOURING 1
#else
#  define HAS_IOURING 0
#endif

struct tsdb_io_async {
#if HAS_IOURING
    struct io_uring  ring;
    unsigned         depth;
    int              alive;        /* 1 = ring initialised */
    int              pending_subs; /* prepared but not yet io_uring_submit'd */
#else
    /* Stub mode: track in-flight submits in a tiny ring so drain() can
     * return them in submission order.  Real fsync ran inline on submit. */
    uint64_t  inflight[16];
    int       head, tail;
#endif
};

int tsdb_io_async_available(void) {
    return HAS_IOURING;
}

int tsdb_io_async_create(unsigned entries, tsdb_io_async_t **out) {
    if (!out) { errno = EINVAL; return -1; }
    if (entries == 0) entries = 64;
    if (entries < 8)    entries = 8;
    if (entries > 4096) entries = 4096;

    tsdb_io_async_t *io = calloc(1, sizeof(*io));
    if (!io) { errno = ENOMEM; return -1; }

#if HAS_IOURING
    int rc = io_uring_queue_init(entries, &io->ring, 0);
    if (rc < 0) {
        free(io);
        errno = -rc;
        return -1;
    }
    io->depth = entries;
    io->alive = 1;
    io->pending_subs = 0;
#else
    io->head = io->tail = 0;
#endif

    *out = io;
    return 0;
}

void tsdb_io_async_destroy(tsdb_io_async_t *io) {
    if (!io) return;
#if HAS_IOURING
    if (io->alive) io_uring_queue_exit(&io->ring);
#endif
    free(io);
}

int tsdb_io_async_submit_fsync(tsdb_io_async_t *io, int fd, uint64_t user_data) {
    if (!io) { errno = EINVAL; return -1; }

#if HAS_IOURING
    if (!io->alive) { errno = EINVAL; return -1; }
    struct io_uring_sqe *sqe = io_uring_get_sqe(&io->ring);
    if (!sqe) { errno = EAGAIN; return -1; }     /* SQ full — caller should drain */
    /* Iter 8: IORING_FSYNC_DATASYNC = fdatasync semantics.  Skips the
     * inode metadata journal entry when file size hasn't changed, which
     * (per perf at the 4-min cliff regime) was responsible for 3.84% in
     * `__lock_text_start` blocked behind jbd2_log_wait_commit.  For our
     * WAL + partition idx files, the metadata we care about (size) IS
     * synced — durability semantics preserved.  Opt out at compile time
     * by undefining TSDB_USE_FDATASYNC. */
#  ifndef TSDB_NO_FDATASYNC
    io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
#  else
    io_uring_prep_fsync(sqe, fd, 0);
#  endif
    /* io_uring_sqe_set_data64 isn't in every liburing version; use the
     * field directly which works back to the earliest releases. */
    sqe->user_data = user_data;
    io->pending_subs++;
    return 0;
#else
    /* Stub: do the fdatasync inline, record the tag so drain() can return it. */
    int next = (io->tail + 1) & 15;
    if (next == io->head) { errno = EAGAIN; return -1; }
#  if defined(__linux__) && !defined(TSDB_NO_FDATASYNC)
    if (fdatasync(fd) < 0) return -1;
#  else
    if (fsync(fd) < 0) return -1;
#  endif
    io->inflight[io->tail] = user_data;
    io->tail = next;
    return 0;
#endif
}

int tsdb_io_async_submit(tsdb_io_async_t *io) {
    if (!io) return 0;
#if HAS_IOURING
    if (!io->alive) return 0;
    if (io->pending_subs == 0) return 0;
    int rc = io_uring_submit(&io->ring);
    if (rc >= 0) io->pending_subs = 0;
    return rc;
#else
    return 0;   /* fsyncs already ran in submit_fsync */
#endif
}

int tsdb_io_async_drain(tsdb_io_async_t *io,
                         int min_completions,
                         uint64_t *out_user_data, int max_n,
                         int *out_errors)
{
    int errs = 0, got = 0;
    if (!io) goto done;

#if HAS_IOURING
    if (!io->alive) goto done;
    while (got < min_completions || (got < max_n &&
           io_uring_cq_ready(&io->ring) > 0))
    {
        struct io_uring_cqe *cqe = NULL;
        int rc;
        if (got < min_completions) {
            rc = io_uring_wait_cqe(&io->ring, &cqe);
        } else {
            rc = io_uring_peek_cqe(&io->ring, &cqe);
        }
        if (rc < 0 || !cqe) break;
        if (out_user_data && got < max_n) out_user_data[got] = cqe->user_data;
        if (cqe->res < 0) errs++;
        io_uring_cqe_seen(&io->ring, cqe);
        got++;
        if (got >= max_n) break;
    }
#else
    while (got < min_completions || (got < max_n && io->head != io->tail)) {
        if (io->head == io->tail) break;       /* drained */
        if (out_user_data && got < max_n) out_user_data[got] = io->inflight[io->head];
        io->head = (io->head + 1) & 15;
        got++;
    }
#endif

done:
    if (out_errors) *out_errors = errs;
    return got;
}

int tsdb_io_async_fsync_sync(tsdb_io_async_t *io, int fd) {
    if (tsdb_io_async_submit_fsync(io, fd, 0) < 0) {
        /* SQ full or other error — fall back to plain fsync. */
        return fsync(fd) == 0 ? 0 : -1;
    }
    (void)tsdb_io_async_submit(io);
    int errs = 0;
    int got = tsdb_io_async_drain(io, 1, NULL, 1, &errs);
    if (got != 1 || errs != 0) return -1;
    return 0;
}
