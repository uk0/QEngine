/* test_pitr.c — point-in-time recovery trim.
 *
 *   1. open DB + insert 6 rows spread across 6 days (partition_unit=DAY
 *      so one partition per row).
 *   2. tsdb_pitr_trim_to(target) where target = day-3 mid-day.
 *   3. Verify partitions with pstart > target are gone, earlier ones
 *      survive, and a SELECT returns only the kept rows.
 *   4. Re-run trim with the same target → idempotent (0 new removals).
 */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/storage/db.h" /* tsdb_pitr_trim_to */

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* Insert one row at ts via the public batch API. */
static void ins(tsdb_db_t *db, const char *tbl, int64_t ts, int64_t v) {
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, tbl, &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    OK(tsdb_batch_row_ts(b, ts));
    OK(tsdb_batch_row_i64(b, 1, v));
    OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_commit(b));
}

static int count_select(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int n = 0;
    if (tsdb_query(db, sql, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_pitr";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    /* Six rows on consecutive days — daily partitioning is default. */
    const int64_t DAY_NS = 86400LL * 1000000000LL;
    const int64_t base   = 1735689600LL * 1000000000LL; /* 2025-01-01 UTC */
    for (int i = 0; i < 6; i++) {
        ins(db, "t", base + (int64_t)i * DAY_NS, 100 + i);
    }

    /* Pre-PITR: all 6 rows visible. */
    int pre = count_select(db, "SELECT v FROM t");
    printf("[1] pre-PITR rows=%d\n", pre);
    ASSERT(pre == 6);

    /* Trim to day 3 (noon) → keep days 0..2 (3 partitions), drop 3..5. */
    int64_t target = base + 3 * DAY_NS + DAY_NS / 2;
    int removed = -1;
    OK(tsdb_pitr_trim_to(db, target, &removed));
    printf("[2] pitr_trim_to(target=+3.5d) removed=%d\n", removed);
    /* partition start must be strictly > target; day 3's partition has
     * pstart == base+3*DAY_NS, which is < target, so it survives. Days
     * 4 and 5 strictly exceed target ⇒ 2 partitions dropped. */
    ASSERT(removed == 2);

    int post = count_select(db, "SELECT v FROM t");
    printf("[3] post-PITR rows=%d\n", post);
    ASSERT(post == 4);

    /* Idempotent. */
    int removed2 = -1;
    OK(tsdb_pitr_trim_to(db, target, &removed2));
    printf("[4] second trim removed=%d (expect 0)\n", removed2);
    ASSERT(removed2 == 0);

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== PITR TESTS PASSED ===\n");
    return 0;
}
