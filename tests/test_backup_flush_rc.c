/* test_backup_flush_rc.c — a backup must not report success while holding
 * less than the database.
 *
 * tsdb_db_flush_all already propagates the first table's failure (db.c), and
 * db.h already says so: "the FIRST failure is RETURNED — a caller that asked
 * for its rows on disk has to be able to find out that they are not."  One
 * caller then threw it away:
 *
 *     (void)tsdb_db_flush_all(db);      backup.c, tsdb_backup_emit_manifest_file
 *
 * That call is the ONLY thing putting the memtable on disk before /backup runs
 * `tar -cz` over the data dir.  Under TSDB_WAL_ONLY_COMMIT — the cluster's mode
 * — a commit is a redo append plus a memtable insert, so a failed flush means
 * those rows are in the WAL and RAM and NOT in any partition file.  The
 * manifest is built from tsdb_query, which reads the memtable too, so it
 * cheerfully recorded counts the tarball does not contain, and /backup streamed
 * 200 OK.  restore.c's own comment names this exact call as the thing a BACKUP
 * must not imitate.
 *
 * INJECTION: chmod 0500 on the table directory, the same clean-errno path
 * tests/test_enospc.c uses.  The flush must mkdir a partition dir under it and
 * gets EACCES.
 *
 *   [1] flush_all itself reports the failure (the property db.h promises).
 *   [2] the manifest emitter reports it too, with a code distinct from "the
 *       manifest file could not be written" — the route has to tell "your
 *       verification aid is missing" from "your rows are not on disk".
 *   [3] with the table dir writable again, both succeed and the manifest
 *       lands.
 *
 * Only meaningful under deferred flush; under flush-on-commit the memtable is
 * already empty at this point and the sweep has nothing to fail on, so the
 * test asserts the honest outcome for whichever mode it runs in.
 */
#include "tsdb.h"
#include "../src/storage/db.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int g_fail = 0;

#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)

#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
    fprintf(stderr, "FATAL %s:%d rc=%d (%s)\n", __FILE__, __LINE__, _r,   \
            tsdb_errstr(_r)); exit(1); } } while (0)

#define TDIR "/tmp/tsdb_test_backup_flush_rc"
#define DAY1 1700000000000000000LL
#define DAY_NS 86400000000000LL

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        struct stat st;
        if (stat(q, &st) == 0 && S_ISDIR(st.st_mode)) chmod(q, 0755);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static void insert(tsdb_db_t *db, int64_t base, int n) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, base + i));
        OK(tsdb_batch_row_i64(b, 1, i));
        OK(tsdb_batch_row_end(b));
    }
    (void)tsdb_batch_commit(b);
}

int main(void) {
    printf("=== test_backup_flush_rc ===\n");
    rm_rf(TDIR);

    const char *wo = getenv("TSDB_WAL_ONLY_COMMIT");
    int deferred = (wo && wo[0] == '1');
    printf("  mode: %s\n", deferred ? "deferred flush" : "flush-on-commit");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    /* Establish a partition so the table dir exists, then flush it clean. */
    insert(db, DAY1, 200);
    OK(tsdb_db_flush_all(db));

    /* Rows for a NEW day: the flush must mkdir a fresh partition dir. */
    insert(db, DAY1 + DAY_NS, 200);

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/t", TDIR);
    if (chmod(tbl_dir, 0500) != 0) { fprintf(stderr, "FATAL: chmod\n"); return 1; }

    printf("\n[1] tsdb_db_flush_all reports the failure\n");
    int frc = tsdb_db_flush_all(db);
    printf("  flush_all rc=%d (%s)\n", frc, tsdb_errstr(frc));
    if (deferred) {
        CHECK(frc != TSDB_OK, "the partition write failed and it is reported");
    } else {
        CHECK(frc == TSDB_OK,
              "flush-on-commit already wrote through — nothing left to fail");
    }

    printf("\n[2] the manifest emitter does not paper over it\n");
    {
        int mrc = tsdb_backup_emit_manifest_file(db, TDIR);
        printf("  emit_manifest rc=%d\n", mrc);
        if (deferred) {
            CHECK(mrc != 0,
                  "a manifest emitted over a database that is NOT on disk is "
                  "not a success");
            CHECK(mrc == TSDB_BACKUP_NOT_ON_DISK,
                  "and it says WHY — distinct from a manifest that could not "
                  "be written, which stays best-effort");
        } else {
            CHECK(mrc == 0, "flush-on-commit: nothing failed, manifest emitted");
        }
    }

    printf("\n[3] with the failure cleared, both succeed\n");
    {
        if (chmod(tbl_dir, 0755) != 0) { fprintf(stderr, "FATAL: chmod\n"); return 1; }
        CHECK(tsdb_db_flush_all(db) == TSDB_OK, "flush_all succeeds again");
        CHECK(tsdb_backup_emit_manifest_file(db, TDIR) == 0,
              "and the manifest is emitted");
        char mpath[4096];
        snprintf(mpath, sizeof(mpath), "%s/_backup_manifest.json", TDIR);
        struct stat st;
        CHECK(stat(mpath, &st) == 0 && st.st_size > 0,
              "_backup_manifest.json is on disk and non-empty");
    }

    tsdb_close(db);
    rm_rf(TDIR);
    printf("\n=== test_backup_flush_rc %s ===\n", g_fail ? "FAILED" : "PASSED");
    return g_fail ? 1 : 0;
}
