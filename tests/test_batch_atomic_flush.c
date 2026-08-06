/* test_batch_atomic_flush.c — a bulk batch must not be SPLIT across a flush.
 *
 * tsdb_batch_append_bulk chunks by block_points and flushes when the memtable
 * fills.  A batch that starts on a partly-full memtable was therefore split: a
 * prefix went durable into a partition and the rest stayed in memory.  Nothing
 * can undo that prefix — tsdb_batch_discard only truncates the memtable — so
 * the receiver's commit-failure rollback left HALF the batch applied, and a
 * sender retry then duplicated only the other half.  It is also the missing
 * foundation for WRITE_BATCH dedup, which needs "apply this batch exactly once,
 * atomically" (prerequisite 5 of the dedup design).
 *
 * The fix flushes BEFORE appending any row of a batch that would fit in an
 * empty memtable, and re-anchors the rollback point, so the batch lands in one
 * memtable generation and a discard undoes ALL of it.
 *
 * The measurement: block_points 1024, 600 rows already resident, then a
 * 600-row bulk batch (600+600 > 1024, but 600 <= 1024) that is DISCARDED.
 *   fixed  -> flush first, batch atomic, discard undoes it: 600 rows survive.
 *   broken -> mid-batch flush persists 600 + 424 of the batch, discard drops
 *             only the 176-row tail: 1024 rows survive — 424 of a discarded
 *             batch made durable.
 */

#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int g_fail = 0;

#define CHECK(c, ...) do { \
    if (!(c)) { fprintf(stderr, "FAIL [%s:%d]: ", __FILE__, __LINE__); \
                fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); g_fail++; } \
    else { printf("  ok: "); printf(__VA_ARGS__); printf("\n"); } \
} while (0)

#define BASE 1700000000000000000LL
#define BP   1024
#define PRE  600
#define BAT  600

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

/* Partition directories under <dir>/t — 0 means nothing has been flushed. */
static int npartitions(const char *dir) {
    char p[4096];
    snprintf(p, sizeof(p), "%s/t", dir);
    DIR *d = opendir(p);
    if (!d) return 0;
    struct dirent *e;
    int n = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char q[4600]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        struct stat st;
        if (stat(q, &st) == 0 && S_ISDIR(st.st_mode)) n++;
    }
    closedir(d);
    return n;
}

static int count_rows(tsdb_db_t *db) {
    tsdb_result_t *r = NULL;
    int n = 0;
    if (tsdb_query(db, "SELECT v FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

int main(void) {
    printf("=== test_batch_atomic_flush ===\n");
    const char *dir = "/tmp/tsdb_test_batch_atomic_flush";
    rm_rf(dir);

    /* Deferred flush is what leaves rows RESIDENT after a commit — the only
     * state in which a following bulk batch can straddle a flush.  Under
     * flush-on-commit the memtable is empty when the bulk batch starts and
     * there is nothing to test, so pin the mode here rather than depend on how
     * the suite was invoked. */
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    setenv("TSDB_IDLE_FLUSH", "0", 1);   /* no background flush racing us */

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK || !db) { fprintf(stderr, "FATAL open\n"); return 1; }

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    if (tsdb_create_table_ex2(db, "t", cols, 2, "ts",
                              TSDB_CREATE_PART_DAY, BP) != TSDB_OK) {
        fprintf(stderr, "FATAL create\n"); return 1;
    }
    tsdb_table_t *tbl = NULL;
    if (tsdb_open_table(db, "t", &tbl) != TSDB_OK || !tbl) {
        fprintf(stderr, "FATAL open_table\n"); return 1;
    }

    /* Resident rows a PRIOR batch left in the memtable. */
    {
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(tbl, &b) != TSDB_OK) { fprintf(stderr, "FATAL begin\n"); return 1; }
        for (int i = 0; i < PRE; i++) {
            tsdb_batch_row_ts(b, BASE + i);
            tsdb_batch_row_i64(b, 1, i);
            tsdb_batch_row_end(b);
        }
        if (tsdb_batch_commit(b) != TSDB_OK) { fprintf(stderr, "FATAL commit\n"); return 1; }
    }
    CHECK(count_rows(db) == PRE, "%d rows written before the bulk batch", PRE);
    /* The precondition the whole test rests on: nothing has been flushed yet,
     * so those rows are still RESIDENT and the bulk batch below starts on a
     * partly-full memtable.  (Under flush-on-commit this would be 1 and the
     * straddle could never happen — hence the pinned mode above.) */
    CHECK(npartitions(dir) == 0,
          "nothing flushed yet (%d partitions), so the rows are RESIDENT and a "
          "straddle is possible", npartitions(dir));

    /* A bulk batch that fits in an EMPTY memtable but not in what is left.
     * Then DISCARD it: nothing of it may survive. */
    {
        int64_t ts[BAT], v[BAT];
        for (int i = 0; i < BAT; i++) { ts[i] = BASE + 100000 + i; v[i] = 900000 + i; }
        const void *arrs[1] = { v };
        int types[1] = { TSDB_TYPE_INT64 };

        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(tbl, &b) != TSDB_OK) { fprintf(stderr, "FATAL begin2\n"); return 1; }
        int rc = tsdb_batch_append_bulk(b, ts, arrs, types, 1, BAT);
        CHECK(rc == TSDB_OK, "bulk append of %d rows accepted (rc=%d)", BAT, rc);
        tsdb_batch_discard(b);
    }

    int after = count_rows(db);
    CHECK(after == PRE,
          "a DISCARDED bulk batch leaves nothing behind: %d rows (want %d; "
          "%d would mean %d of its rows were flushed and cannot be undone)",
          after, PRE, PRE + (BP - PRE), BP - PRE);

    /* None of the batch's own values may be present. */
    {
        tsdb_result_t *r = NULL;
        int seen = 0;
        if (tsdb_query(db, "SELECT v FROM t WHERE v >= 900000", &r) == TSDB_OK && r) {
            while (tsdb_result_next(r) > 0) seen++;
            tsdb_result_free(r);
        }
        CHECK(seen == 0, "no row of the discarded batch is readable (found %d)", seen);
    }

    /* And the table still works: a later batch commits and is visible, so the
     * pre-flush + re-anchor did not leave the batch state wedged. */
    {
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(tbl, &b) != TSDB_OK) { fprintf(stderr, "FATAL begin3\n"); return 1; }
        for (int i = 0; i < 10; i++) {
            tsdb_batch_row_ts(b, BASE + 200000 + i);
            tsdb_batch_row_i64(b, 1, 5000 + i);
            tsdb_batch_row_end(b);
        }
        int rc = tsdb_batch_commit(b);
        CHECK(rc == TSDB_OK, "a later batch still commits (rc=%d)", rc);
        CHECK(count_rows(db) == PRE + 10,
              "and is visible: %d rows", count_rows(db));
    }

    tsdb_close(db);
    rm_rf(dir);
    printf(g_fail ? "\n=== FAILED (%d) ===\n" : "\n=== PASSED ===\n", g_fail);
    return g_fail ? 1 : 0;
}
