/* test_wal_fsync_fail.c — an interval-mode WAL whose fsync FAILS must stop
 * acking writes.
 *
 * In TSDB_WAL_SYNC_MS mode tsdb_wal_sync() acks immediately and a background
 * flusher owes the fsync.  When that fsync fails there is no caller left to
 * tell: the rows were already acked.  Re-marking the WAL dirty and fsyncing
 * again is not a recovery — on Linux the second fsync reports SUCCESS without
 * the pages the first one lost ever reaching the device (post-4.13 fsync error
 * semantics) — so a silent retry turns a durability failure into a clean-looking
 * commit.  The only honest response is to stop promising durability on that
 * WAL, which is what this pins:
 *
 *   [1] the ack before any flusher tick is OK (the window is real, not denied)
 *   [2] once the flusher's fsync has failed, tsdb_wal_sync() returns TSDB_ERR_IO
 *   [3] and keeps returning it — the refusal is latched, not one-shot
 *
 * TSDB_WAL_FAIL_FSYNC is the deterministic fault injection this needs; it is the
 * same test-only knob shape wal.c already uses for TSDB_WAL_FAIL_APPEND.  No
 * portable fd makes a real fsync fail (verified here: fsync succeeds on
 * /dev/null and on a FIFO under this platform), so the error path cannot be
 * reached from a black box without it.
 *
 * Both env vars are latched on first WAL use, so this binary runs ONLY in the
 * failing-interval configuration.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/storage/wal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d", _r); } while (0)

static const char *DIR = "/tmp/tsdb_test_wal_fsync_fail";

static void rm_tree(const char *p) {
    char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c);
}

static void msleep(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

int main(void) {
    printf("=== test_wal_fsync_fail ===\n");

    /* Latched on first WAL use — must be set before tsdb_wal_open. */
    setenv("TSDB_WAL_SYNC_MS",   "20", 1);
    setenv("TSDB_WAL_FAIL_FSYNC", "1", 1);

    rm_tree(DIR);

    tsdb_wal_t *w = NULL;
    OK(tsdb_wal_open(DIR, "t", &w));

    uint8_t rec[64];
    memset(rec, 0xA5, sizeof(rec));
    OK(tsdb_wal_append(w, rec, sizeof(rec)));

    /* [1] The WAL is not dirty until this call, so no flusher tick can have
     * fsynced yet: the ack is unconditionally OK here.  That is the window the
     * mode trades away, and it is honest. */
    OK(tsdb_wal_sync(w));
    printf("  phase 1: first ack OK (interval window open)\n");

    /* Give the 20 ms flusher many chances to run and fail. */
    msleep(400);

    /* [2] The owed fsync failed.  Nothing can make the acked bytes durable now,
     * so the WAL must refuse to ack anything further instead of reporting a
     * clean commit over an unknown durability state. */
    OK(tsdb_wal_append(w, rec, sizeof(rec)));
    int rc2 = tsdb_wal_sync(w);
    printf("  phase 2: sync after failed interval fsync -> rc=%d\n", rc2);
    ASSERT(rc2 == TSDB_ERR_IO);

    /* [3] Latched: a later call must not drift back to OK.  A one-shot error
     * would let the very next commit ack again over the same lost pages. */
    msleep(100);
    int rc3 = tsdb_wal_sync(w);
    printf("  phase 3: sync still refused -> rc=%d\n", rc3);
    ASSERT(rc3 == TSDB_ERR_IO);

    tsdb_wal_close(w);
    rm_tree(DIR);

    printf("PASS test_wal_fsync_fail\n");
    return 0;
}
