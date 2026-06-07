/* test_io_async_robust.c — edge-case coverage for the io_async wrapper.
 *
 * Complements test_io_async.c with cases the basic test doesn't reach:
 * depth clamping, submit-beyond-capacity + full drain + reuse, repeated
 * fsync_sync, destroy-with-pending, NULL-safety, and available() stability.
 *
 * Assertions hold on both the real io_uring build and the sync-fallback
 * stub: completion ORDER is never assumed (tags matched as a multiset),
 * and capacity floors use the create() clamp ([8,4096]) which both paths
 * honour (the stub's 16-slot ring delivers >= 8 in flight regardless).
 */
#define _POSIX_C_SOURCE 200809L

#include "../src/storage/io_async.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>

#define ASSERT(cond) do { if (!(cond)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #cond, __FILE__, __LINE__); abort(); } } while (0)

static const char *TMP = "/tmp/tsdb_test_io_async_robust.bin";

/* Count how many times `want` appears in tags[0..n). */
static int multiset_count(const uint64_t *tags, int n, uint64_t want) {
    int c = 0;
    for (int i = 0; i < n; i++) if (tags[i] == want) c++;
    return c;
}

int main(void) {
    printf("=== test_io_async_robust ===\n");
    printf("  build supports io_uring: %s\n",
           tsdb_io_async_available() ? "yes" : "no (using sync stub)");

    unlink(TMP);
    int fd = open(TMP, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT(fd >= 0);
    ASSERT(write(fd, "seed", 4) == 4);

    /* [1] Depth clamping: 0 (→default 64), 1 (→min 8), 9999 (→max 4096)
     * all create successfully and destroy cleanly. */
    {
        const unsigned reqs[] = { 0u, 1u, 9999u };
        for (size_t i = 0; i < sizeof(reqs) / sizeof(reqs[0]); i++) {
            tsdb_io_async_t *io = NULL;
            ASSERT(tsdb_io_async_create(reqs[i], &io) == 0);
            ASSERT(io != NULL);
            /* A clamped instance must be usable: one fsync round-trips. */
            ASSERT(tsdb_io_async_fsync_sync(io, fd) == 0);
            tsdb_io_async_destroy(io);
        }
        printf("  [1] depth clamping (0/1/9999): all create+fsync+destroy OK\n");
    }

    /* [2] Submit beyond capacity, then drain everything, then reuse.
     * Loop submit_fsync until it returns -1 (SQ full / EAGAIN); at least
     * the create-depth floor (8) must have been accepted.  Drained count
     * must equal accepted count, with zero errors, and every accepted tag
     * must come back exactly once (order-independent).  Then confirm the
     * ring accepts new work after draining. */
    {
        tsdb_io_async_t *io = NULL;
        ASSERT(tsdb_io_async_create(8, &io) == 0);

        enum { CAP = 4096 };
        uint64_t tags[CAP] = {0};
        int accepted = 0;
        for (int i = 0; i < CAP; i++) {
            int r = tsdb_io_async_submit_fsync(io, fd, (uint64_t)(0x2000 + i));
            if (r != 0) break;       /* SQ full */
            accepted++;
        }
        ASSERT(accepted >= 8);       /* at least the clamped depth's worth */
        ASSERT(accepted < CAP);      /* the ring DID report full at some point */

        int submitted = tsdb_io_async_submit(io);
        ASSERT(submitted >= 0);

        int errs = -1;
        int drained = tsdb_io_async_drain(io, accepted, tags, CAP, &errs);
        ASSERT(drained == accepted);
        ASSERT(errs == 0);
        for (int i = 0; i < accepted; i++) {
            ASSERT(multiset_count(tags, drained, (uint64_t)(0x2000 + i)) == 1);
        }

        /* Ring is empty again — a fresh submit must be accepted. */
        ASSERT(tsdb_io_async_submit_fsync(io, fd, 0xBEEF) == 0);
        ASSERT(tsdb_io_async_submit(io) >= 0);
        uint64_t t2[2] = {0};
        int e2 = -1;
        int d2 = tsdb_io_async_drain(io, 1, t2, 2, &e2);
        ASSERT(d2 == 1);
        ASSERT(e2 == 0);
        ASSERT(t2[0] == 0xBEEF);

        printf("  [2] over-capacity: %d accepted, %d drained (errs=%d), reuse OK\n",
               accepted, drained, errs);

        tsdb_io_async_destroy(io);
    }

    /* [3] fsync_sync called many times back-to-back on a real fd; all 0. */
    {
        tsdb_io_async_t *io = NULL;
        ASSERT(tsdb_io_async_create(0, &io) == 0);

        const int ROUNDS = 64;
        for (int i = 0; i < ROUNDS; i++) {
            char b[24];
            int n = snprintf(b, sizeof(b), "fsync-round-%d\n", i);
            ASSERT(write(fd, b, (size_t)n) == n);
            ASSERT(tsdb_io_async_fsync_sync(io, fd) == 0);
        }
        printf("  [3] fsync_sync x%d on real fd: all returned 0\n", ROUNDS);

        tsdb_io_async_destroy(io);
    }

    /* [4] Destroy with pending, un-drained submits must not crash. */
    {
        tsdb_io_async_t *io = NULL;
        ASSERT(tsdb_io_async_create(8, &io) == 0);

        for (int i = 0; i < 4; i++) {
            ASSERT(tsdb_io_async_submit_fsync(io, fd, (uint64_t)(0x300 + i)) == 0);
        }
        (void)tsdb_io_async_submit(io);   /* queued but never drained */
        tsdb_io_async_destroy(io);        /* must be clean */
        printf("  [4] destroy with pending submits: no crash\n");
    }

    /* [5] NULL-safety, per the .c contracts:
     *   - destroy(NULL): no-op.
     *   - submit_fsync(NULL,..): returns -1 (errno EINVAL).
     *   - submit(NULL): returns 0.
     *   - drain(NULL,..): returns 0; if out_errors given, set to 0.
     *   - fsync_sync(NULL,..): submit_fsync(NULL) fails, falls back to
     *     plain fsync(fd) on the real fd → 0. */
    {
        tsdb_io_async_destroy(NULL);                       /* no-op */
        ASSERT(tsdb_io_async_submit_fsync(NULL, fd, 7) == -1);
        ASSERT(tsdb_io_async_submit(NULL) == 0);

        int errs = 12345;
        uint64_t junk[2] = {0};
        ASSERT(tsdb_io_async_drain(NULL, 3, junk, 2, &errs) == 0);
        ASSERT(errs == 0);
        /* out_errors == NULL must also be tolerated. */
        ASSERT(tsdb_io_async_drain(NULL, 1, NULL, 1, NULL) == 0);

        ASSERT(tsdb_io_async_fsync_sync(NULL, fd) == 0);   /* plain-fsync fallback */
        printf("  [5] NULL-safety: destroy/submit_fsync/submit/drain/fsync_sync OK\n");
    }

    /* [6] available() is a compile-time constant — stable across calls. */
    {
        int a = tsdb_io_async_available();
        ASSERT(a == 0 || a == 1);
        for (int i = 0; i < 8; i++) ASSERT(tsdb_io_async_available() == a);
        printf("  [6] available() stable: %d across calls\n", a);
    }

    close(fd);
    unlink(TMP);
    printf("[PASS] io_async robust edge cases\n");
    return 0;
}
