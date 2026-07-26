/* test_io_async.c — basic io_uring wrapper sanity test.
 *
 * Submits N fsyncs to a temp file, verifies all completions return with
 * the right user_data tag and no errors.  Works on both real-uring builds
 * and the sync-fallback stub (verifies the stub also delivers tags).
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

static const char *TMP = "/tmp/tsdb_test_io_async.bin";

int main(void) {
    printf("=== test_io_async ===\n");
    printf("  build supports io_uring: %s\n",
           tsdb_io_async_available() ? "yes" : "no (using sync stub)");

    /* Prepare a temp file we can fsync against. */
    unlink(TMP);
    int fd = open(TMP, O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT(fd >= 0);

    /* Single-shot path via the convenience helper. */
    {
        tsdb_io_async_t *io = NULL;
        ASSERT(tsdb_io_async_create(0, &io) == 0);

        const char buf[] = "hello io_uring";
        ASSERT(write(fd, buf, sizeof(buf)) == (ssize_t)sizeof(buf));
        ASSERT(tsdb_io_async_fsync_sync(io, fd) == 0);
        printf("  [1] fsync_sync round-trip: OK\n");

        tsdb_io_async_destroy(io);
    }

    /* Batched path: prepare N fsyncs, submit, drain.  Tags returned in
     * completion order (uring) or submission order (stub). */
    {
        tsdb_io_async_t *io = NULL;
        ASSERT(tsdb_io_async_create(8, &io) == 0);

        /* enum, not `const int`: in C a const int is not a constant
         * expression, so `uint64_t tags[N + 4] = {0}` below became a VLA —
         * which clang initialises as an extension but gcc rejects outright,
         * breaking the whole test build on the deployment compiler. */
        enum { N = 8 };
        for (int i = 0; i < N; i++) {
            char b[8]; snprintf(b, sizeof(b), "row%d\n", i);
            ASSERT(write(fd, b, strlen(b)) > 0);
            ASSERT(tsdb_io_async_submit_fsync(io, fd, (uint64_t)(0x1000 + i)) == 0);
        }
        int submitted = tsdb_io_async_submit(io);
        ASSERT(submitted >= 0);

        uint64_t tags[N + 4] = {0};
        int errs = 0;
        int got = tsdb_io_async_drain(io, N, tags, N + 4, &errs);
        ASSERT(got == N);
        ASSERT(errs == 0);

        /* Each submitted tag must appear exactly once in completions. */
        for (int i = 0; i < N; i++) {
            uint64_t want = (uint64_t)(0x1000 + i);
            int found = 0;
            for (int j = 0; j < got; j++) if (tags[j] == want) { found = 1; break; }
            ASSERT(found);
        }
        printf("  [2] %d submit+drain: all tags delivered (errs=%d)\n", N, errs);

        tsdb_io_async_destroy(io);
    }

    /* SQ-full path: depth=8, submit 9 → 9th should err with EAGAIN. */
    {
        tsdb_io_async_t *io = NULL;
        ASSERT(tsdb_io_async_create(8, &io) == 0);

        int ok = 0;
        for (int i = 0; i < 20; i++) {
            int r = tsdb_io_async_submit_fsync(io, fd, (uint64_t)i);
            if (r == 0) ok++;
            else break;          /* SQ said full */
        }
        ASSERT(ok >= 8);         /* at least depth's worth went in */
        printf("  [3] SQ-full path: %d submits accepted before EAGAIN\n", ok);

        (void)tsdb_io_async_submit(io);
        int errs = 0;
        int drained = tsdb_io_async_drain(io, ok, NULL, ok, &errs);
        ASSERT(drained == ok);
        ASSERT(errs == 0);

        tsdb_io_async_destroy(io);
    }

    close(fd);
    unlink(TMP);
    printf("[PASS] io_async basic sanity\n");
    return 0;
}
