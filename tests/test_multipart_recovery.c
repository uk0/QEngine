/* test_multipart_recovery.c — a crash mid-flush that publishes one partition
 * of a multi-partition flush but not its sibling must NOT lose the sibling's
 * acked rows on recovery.
 *
 * THE BUG (pre-fix): one flush stamps the same hwm into every partition it
 * touches sequentially, and redo_recover_table took a scalar MAX checkpoint
 * over all partitions.  If DAY1 published (ts.idx max_seq = S covering ALL the
 * flush's records) but DAY2's publish failed / crashed, recovery's cutoff = S
 * and every WAL record seq <= S is skipped — DAY2's acked rows are permanently
 * dropped though the WAL still holds them.
 *
 * THE FIX: redo_recover_table builds a per-partition checkpoint map and
 * redo_replay_cb skips a record only when its seq is covered by EVERY
 * partition its rows land in (MIN over touched partitions, missing = 0).
 *
 * Reproduction (public API + a dir-permission fault, == the persistent disk
 * state of a kill -9 between the two partition publishes): write N1 DAY1 rows
 * + N2 DAY2 rows (all acked under wal_only_commit), block DAY2's dir, force a
 * flush (DAY1 publishes, DAY2 fails), _exit(0), heal the dir, reopen.
 */
#include "tsdb.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern int tsdb_db_flush_all(tsdb_db_t *db);

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define DAY1 (1735689600LL * 1000000000LL)   /* 2025-01-01 UTC ns */
#define DAY2 (DAY1 + 86400LL * 1000000000LL) /* 2025-01-02 UTC ns */
#define N1 3000
#define N2 2000
#define TDIR "/tmp/tsdb_test_multipart"

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        chmod(q, 0700);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static void write_range(tsdb_table_t *t, int64_t base, int n) {
    int i = 0;
    while (i < n) {
        int m = (n - i < 4) ? (n - i) : 4;
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int k = 0; k < m; k++) {
            int idx = i + k;
            OK(tsdb_batch_row_ts(b, base + (int64_t)idx));
            OK(tsdb_batch_row_i64(b, 1, (int64_t)(base % 1000000007LL) + idx));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));   /* durable ACK under wal_only_commit */
        i += m;
    }
}

static void count_days(tsdb_db_t *db, int *d1, int *d2, int *wrong) {
    *d1 = *d2 = *wrong = 0;
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, "SELECT ts, v FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            int64_t v  = tsdb_result_i64(r, 1);
            int64_t base = (ts >= DAY2) ? DAY2 : DAY1;
            if (v != (base % 1000000007LL) + (ts - base)) (*wrong)++;
            if (ts >= DAY2) (*d2)++; else (*d1)++;
        }
        tsdb_result_free(r);
    }
}

int main(int argc, char **argv) {
    if (argc > 1 && !strcmp(argv[1], "child")) {
        setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
        tsdb_db_t *db = NULL;
        if (tsdb_open(TDIR, &db) != TSDB_OK) _exit(2);
        tsdb_table_t *t = NULL;
        if (tsdb_open_table(db, "t", &t) != TSDB_OK) _exit(3);
        write_range(t, DAY1, N1);          /* DAY1 first -> publishes first */
        write_range(t, DAY2, N2);
        /* Crash after the first partition (DAY1) publishes but before DAY2 —
         * root bypasses dir permissions, so use the flush fault hook. */
        setenv("TSDB_TEST_CRASH_AFTER_PART", "1", 1);
        (void)tsdb_db_flush_all(db);       /* DAY1 publishes, DAY2 not */
        _exit(0);                          /* crash: memtable dies */
    }

    printf("=== test_multipart_recovery ===\n");
    rm_rf(TDIR);
    {
        tsdb_db_t *db = NULL;
        OK(tsdb_open(TDIR, &db));
        tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
        int rc = tsdb_create_table(db, "t", cols, 2, "ts");
        if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) OK(rc);
        tsdb_close(db);
    }

    /* crashing child */
    char cmd[4600];
    snprintf(cmd, sizeof(cmd), "%s child", argv[0]);
    int st = system(cmd);
    if (st == -1 || !WIFEXITED(st) || WEXITSTATUS(st) != 0)
        FAIL("child failed st=%d", st);

    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    int d1 = 0, d2 = 0, wrong = 0;
    count_days(db, &d1, &d2, &wrong);
    printf("[reopen] DAY1=%d (expect %d)  DAY2=%d (expect %d)  mispaired=%d\n",
           d1, N1, d2, N2, wrong);
    tsdb_close(db);
    rm_rf(TDIR);

    if (d1 != N1 || d2 != N2 || wrong != 0)
        FAIL("lost/mispaired acked rows: DAY2 %d/%d, DAY1 %d/%d, wrong=%d",
             d2, N2, d1, N1, wrong);
    printf("\n=== test_multipart_recovery PASSED ===\n");
    return 0;
}
