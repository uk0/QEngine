/* test_multidir_dirok_race.c — multi-data-dir fixture for the db->dir_ok[]
 * health flags.
 *
 * WHAT THIS IS.  db->dir_ok[] is written by the flush path (db_mark_dir_degraded,
 * under the table's batch_mu) and read AND written by the trash-GC thread's
 * re-adopt pass (trash_gc_main), which holds no lock at all.  The two threads
 * share no mutex, so the pair is a data race.  Nothing in the suite formed that
 * pair before: almost every test runs one data dir, where the `n_data_dirs > 1`
 * guard short-circuits before dir_ok[] is touched at all, and the one that does
 * configure striping (test_multi_data_dirs) never fails a flush, so the write
 * side never fires and the GC's read has nothing to race with.  This fixture
 * supplies the missing half: a failing flush (degrade: read + write) against the
 * GC's re-adopt (read + write) with nothing synchronising them.
 *
 * THIS IS A FIXTURE, NOT A BREAK TEST — it is green on the unfixed tree too,
 * and that is stated here so nobody mistakes it for proof.  On macOS the race
 * cannot be surfaced even with the multi-dir configuration in place: the GC
 * writes dir_ok[i] only AFTER db_dir_healthy(dd), whose statvfs/access go
 * through libsystem locks that ThreadSanitizer models as synchronisation, so it
 * orders that write against the flush thread.  Measured directly on this tree:
 * a probe byte written at the SAME two sites is reported as a race when the GC
 * side sits BEFORE db_dir_healthy and is NOT reported when it is moved after —
 * nothing else changed.  The race is real under C11 (no lock is common to the
 * two threads); TSan on this platform simply cannot see it.
 *
 * What the fixture is still worth: it is the only coverage in the suite for the
 * degrade + re-adopt path itself, and on a platform whose probe syscalls do not
 * synchronise it would surface the race under scripts/tsan-build.sh.
 *
 * MECHANICS.  Two data dirs, so n_data_dirs == 2 and the guard opens.  The
 * table's OWN directory is made unwritable, which fails its flush (degrade
 * writes dir_ok[i] = 0) while the parent data dir still probes healthy, so the
 * GC's next pass re-adopts it (writes dir_ok[i] = 1).  TSDB_MEMTABLE_BUDGET_ROWS=0
 * keeps the GC pass off db->lock, so no incidental lock ordering hides the pair.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d", _r); } while (0)

static const char *PRIMARY = "/tmp/tsdb_dirok_race_primary";
static const char *STRIPE  = "/tmp/tsdb_dirok_race_stripe";

static void rm_tree(const char *p) {
    char c[512]; snprintf(c, sizeof(c), "chmod -R u+rwx %s 2>/dev/null; rm -rf %s", p, p);
    (void)system(c);
}

static void msleep(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static int dir_exists(const char *p) {
    struct stat st;
    return stat(p, &st) == 0 && S_ISDIR(st.st_mode);
}

int main(void) {
    printf("=== test_multidir_dirok_race ===\n");

    rm_tree(PRIMARY); rm_tree(STRIPE);

    setenv("TSDB_DATA_DIRS", STRIPE, 1);
    /* Keep the GC pass off db->lock: the aggregate-budget block is the only
     * lock it takes, and a lock the writer also takes could introduce an
     * incidental happens-before edge that masks the pair under test. */
    setenv("TSDB_MEMTABLE_BUDGET_ROWS", "0", 1);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(PRIMARY, &db));

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "rt", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "rt", &t));

    /* Locate the table's own dir; either stripe is fine, both are configured
     * slots so the n_data_dirs > 1 guard is open for whichever it landed in. */
    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/rt", PRIMARY);
    if (!dir_exists(tdir)) snprintf(tdir, sizeof(tdir), "%s/rt", STRIPE);
    ASSERT(dir_exists(tdir));
    printf("  table dir: %s\n", tdir);

    /* Drive degrade-vs-re-adopt for longer than the GC's ~2 s pass period so
     * the two threads touch dir_ok[] repeatedly with nothing between them. */
    int64_t ts = 1735689600000000000LL;
    for (int round = 0; round < 3; round++) {
        ASSERT(chmod(tdir, 0500) == 0);          /* r-x: writes under it fail */

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int r = 0; r < 64; r++) {
            OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(ts++)));
            OK(tsdb_batch_row_i64(b, 1, r));
            OK(tsdb_batch_row_end(b));
        }
        /* Either mode: the commit may or may not flush, so force it. */
        (void)tsdb_batch_commit(b);
        (void)tsdb_table_flush(db, "rt");        /* -> degrade writes dir_ok[] */

        ASSERT(chmod(tdir, 0700) == 0);          /* -> GC re-adopt writes it back */
        msleep(1000);
    }

    (void)tsdb_table_flush(db, "rt");
    tsdb_close(db);
    rm_tree(PRIMARY); rm_tree(STRIPE);

    printf("PASS test_multidir_dirok_race\n");
    return 0;
}
