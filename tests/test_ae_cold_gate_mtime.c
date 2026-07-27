/* test_ae_cold_gate_mtime.c — the anti-entropy COLD gate's input.
 *
 * db_cluster.c's partition backfill picks a partition to overwrite with a
 * peer's copy, and the ONLY guard keeping a partition under active LOCAL write
 * off that list is a directory mtime:
 *
 *     if (parts[i].pstart == cur_bkt) continue;          // not the current bucket
 *     if (now_s - parts[i].mtime <= 60) continue;        // COLD gate
 *
 * Its own success log says "local-unique rows in this partition, if any, are
 * replaced by the peer copy - MVP limitation", so defeating that gate is data
 * loss, not a stale-stats nuisance.  It is aggravated by the pick comparing the
 * peer count against ts.idx total_rows — DURABLE rows only, memtable excluded —
 * so an actively-written partition understates itself exactly when the gate has
 * been defeated.  The exposed case is a NON-current-bucket partition still
 * receiving writes: late/out-of-order data, replication catch-up, backfill, or
 * an hour-partitioned table just after the hour rolls.
 *
 * The gate works only because every flush moves the partition dir's mtime.
 * temp+rename did that for free by creating and removing a dirent inside the
 * dir; the in-place append publish (col_idx_append_publish) has to do it
 * explicitly, and when it stopped, a measured 66 s of continuous flushes into
 * one partition took dir_age from 0 s to 66 s and the gate went ELIGIBLE with
 * writes still live.
 *
 * Waiting out 60 s here would make the suite 60 s slower to assert something
 * the arithmetic already gives us, so this pins the INPUT instead: after a
 * flush into an existing partition, that partition's dir mtime is at or after
 * the moment the flush began, hence dir_age == 0 and the gate reads SKIP.  A
 * frozen mtime fails this immediately, and it is the same freeze that crosses
 * 60 s once the writes have run long enough.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define ASSERT(cond) do { if (!(cond)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #cond, __FILE__, __LINE__); abort(); } } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) { \
    fprintf(stderr, "rc=%d at %s:%d\n", _r, __FILE__, __LINE__); abort(); } } while (0)

static const char *DIR_ = "/tmp/tsdb_test_ae_cold_gate_mtime";

/* 2025-01-01T00:00:00Z — a fixed past day, never the current bucket, so the
 * `pstart == cur_bkt` guard above it can never be what protects this. */
#define DAY1  (1735689600LL * 1000000000LL)
#define PART  "20250101"

static void rm_tree(const char *p) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c); }

static void write_and_flush(tsdb_db_t *db, int base) {
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, "x", &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 50; i++) {
        long long g = (long long)base + i;
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(DAY1 + g * 1000000LL)));
        OK(tsdb_batch_row_i64(b, 1, g));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));
}

/* st_mtime is second-granular: a second flush inside the same wall second as
 * the first would be indistinguishable from no flush at all. */
static void wait_past_second(time_t sec) {
    struct timespec nap = { 0, 20 * 1000 * 1000 };   /* 20 ms */
    while (time(NULL) <= sec) nanosleep(&nap, NULL);
}

int main(void) {
    printf("=== test_ae_cold_gate_mtime ===\n");
    rm_tree(DIR_);

    tsdb_db_t *db = NULL; OK(tsdb_open(DIR_, &db));
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_INT64} };
    OK(tsdb_create_table(db, "x", cols, 2, "ts"));

    char part_dir[640], ts_idx[700];
    snprintf(part_dir, sizeof(part_dir), "%s/x/%s", DIR_, PART);
    snprintf(ts_idx,   sizeof(ts_idx),   "%s/ts.idx", part_dir);

    write_and_flush(db, 0);                      /* creates the partition */

    struct stat d0, i0;
    ASSERT(stat(part_dir, &d0) == 0);            /* partition dir must exist */
    ASSERT(stat(ts_idx,   &i0) == 0);
    printf("  created  %s  dir_mtime=%ld  ts.idx=%lld bytes\n",
           PART, (long)d0.st_mtime, (long long)i0.st_size);

    /* Two more flushes into that SAME already-existing partition — the
     * "still receiving writes" state the gate exists to exclude. */
    for (int round = 1; round <= 2; round++) {
        struct stat dprev; ASSERT(stat(part_dir, &dprev) == 0);
        wait_past_second(dprev.st_mtime);

        time_t flush_at = time(NULL);
        write_and_flush(db, round * 50);

        struct stat d, i;
        ASSERT(stat(part_dir, &d) == 0);
        ASSERT(stat(ts_idx,   &i) == 0);
        long age = (long)(time(NULL) - d.st_mtime);

        printf("  flush #%d  ts.idx %lld->%lld bytes (inode %s)  dir_mtime %ld->%ld"
               "  dir_age=%lds  gate=%s\n",
               round, (long long)i0.st_size, (long long)i.st_size,
               i0.st_ino == i.st_ino ? "SAME, in-place" : "CHANGED, temp+rename",
               (long)dprev.st_mtime, (long)d.st_mtime, age,
               age <= 60 ? "SKIP (protected)" : "ELIGIBLE (treated as settled)");

        ASSERT(i.st_size > i0.st_size);          /* the append really landed */
        i0 = i;

        /* The invariant the gate rests on: a partition that just took a flush
         * carries an mtime at or after the moment that flush began.  Without
         * the publish bumping the dir this is d0.st_mtime forever, and every
         * further second of writing walks dir_age toward the 60 s cliff. */
        ASSERT(d.st_mtime >= flush_at);
        ASSERT(d.st_mtime >  dprev.st_mtime);

        /* ...which is exactly what makes the gate say SKIP. */
        ASSERT(age <= 60);
    }

    tsdb_close(db);
    rm_tree(DIR_);
    printf("[PASS] a flush into an existing partition keeps it HOT for the "
           "anti-entropy COLD gate\n");
    return 0;
}
