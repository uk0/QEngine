/* test_sum_overflow.c — sum(int64) overflow must error, not wrap.
 *
 * Two INT64_MAX-ish values split across two scan sources (one flushed
 * partition + the memtable) so the per-source block sums fold into the
 * running total — the fold is overflow-checked and must abort the query
 * with TSDB_ERR_OVERFLOW instead of returning a wrapped negative sum.
 */

#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <dirent.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); \
} while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

static void rm_rf(const char *path) {
    DIR *d = opendir(path);
    if (!d) { remove(path); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", path, e->d_name);
        rm_rf(p);
    }
    closedir(d); rmdir(path);
}

static void write_one(tsdb_table_t *t, int64_t ts, int64_t v) {
    tsdb_batch_t *b;
    OK(tsdb_batch_begin(t, &b));
    OK(tsdb_batch_row_ts(b, (tsdb_ts_t)ts));
    OK(tsdb_batch_row_i64(b, 1, v));
    OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_commit(b));
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_sum_overflow";
    rm_rf(dir);

    printf("=== sum(int64) overflow tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "ovf", cols, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "ovf", &t));

    /* Row 1 → flush to its own on-disk partition, row 2 → memtable. */
    write_one(t, 1000000000LL, INT64_MAX - 1);
    OK(tsdb_db_flush_all(db));
    write_one(t, 2000000000LL, INT64_MAX - 2);

    /* [1] sum() must error with OVERFLOW, not return a wrapped value. */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT sum(v) FROM ovf", &r);
        ASSERT(rc == TSDB_ERR_OVERFLOW);
        if (r) tsdb_result_free(r);
        printf("[1] PASS: sum(v) rc=%d (OVERFLOW), not wraparound\n", rc);
    }

    /* [2] avg() shares the int64 sum accumulator — same error. */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT avg(v) FROM ovf", &r);
        ASSERT(rc == TSDB_ERR_OVERFLOW);
        if (r) tsdb_result_free(r);
        printf("[2] PASS: avg(v) rc=%d (OVERFLOW)\n", rc);
    }

    /* [3] Control: a non-overflowing sum still works, split across the
     * same disk-partition + memtable shape. */
    {
        OK(tsdb_create_table(db, "small", cols, 2, "ts"));
        tsdb_table_t *t2; OK(tsdb_open_table(db, "small", &t2));
        write_one(t2, 1000000000LL, 40);
        OK(tsdb_db_flush_all(db));
        write_one(t2, 2000000000LL, 2);

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT sum(v) FROM small", &r));
        ASSERT(tsdb_result_next(r));
        int64_t sum = tsdb_result_i64(r, 0);
        ASSERT(sum == 42);
        tsdb_result_free(r);
        printf("[3] PASS: control sum == 42\n");
    }

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== ALL SUM-OVERFLOW TESTS PASSED ===\n");
    return 0;
}
