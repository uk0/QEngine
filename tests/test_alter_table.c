/* test_alter_table.c — ALTER TABLE ADD COLUMN end-to-end.
 *
 * Phases:
 *  1. Create table, insert 3 rows
 *  2. ALTER TABLE ... ADD COLUMN (via QTL), verify catalog
 *  3. Insert 2 more rows that supply the new column
 *  4. Query all 5 rows: old rows return default value for new col
 *  5. Reopen DB: schema + new column still visible, data consistent
 *  6. ALTER TABLE on unknown table → error
 *  7. ALTER TABLE ADD COLUMN with duplicate name → TSDB_ERR_EXISTS
 */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc)    do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
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

static void run_sql_ok(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("sql '%s' rc=%d (%s)", sql, rc, tsdb_errstr(rc));
    if (r) tsdb_result_free(r);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_alter_table";
    rm_rf(dir);
    printf("=== tsdb ALTER TABLE ADD COLUMN tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    /* Phase 1 — create + initial rows */
    printf("\n[1] CREATE TABLE m + 3 rows with (ts, v)\n");
    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "m", cols, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "m", &t));
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 3; i++) {
        OK(tsdb_batch_row_ts (b, (tsdb_ts_t)((i + 1) * 1000000000LL)));
        OK(tsdb_batch_row_i64(b, 1, 100 + i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    printf("  PASS: table + 3 rows\n");

    /* Phase 2 — ADD COLUMN w via QTL */
    printf("\n[2] ALTER TABLE m ADD COLUMN w FLOAT64\n");
    run_sql_ok(db, "ALTER TABLE m ADD COLUMN w FLOAT64;");
    printf("  PASS: QTL ALTER dispatched\n");

    /* Phase 3 — insert rows that supply the new column */
    printf("\n[3] insert 2 rows with ts, v, w\n");
    {
        tsdb_table_t *t2;
        OK(tsdb_open_table(db, "m", &t2));
        tsdb_batch_t *b2; OK(tsdb_batch_begin(t2, &b2));
        for (int i = 0; i < 2; i++) {
            OK(tsdb_batch_row_ts (b2, (tsdb_ts_t)((i + 10) * 1000000000LL)));
            OK(tsdb_batch_row_i64(b2, 1, 500 + i));
            OK(tsdb_batch_row_f64(b2, 2, 3.14 + i));
            OK(tsdb_batch_row_end(b2));
        }
        OK(tsdb_batch_commit(b2));
    }
    printf("  PASS: 2 rows with new column\n");

    /* Phase 4 — scan: 5 rows, old rows w==0.0 */
    printf("\n[4] SELECT ts, v, w FROM m — 5 rows, old rows w==0.0\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT ts, v, w FROM m;", &r));
        ASSERT(r != NULL);
        ASSERT(tsdb_result_ncols(r) == 3);

        int rows = 0, zero_w = 0, nonzero_w = 0;
        while (tsdb_result_next(r)) {
            int64_t v  = tsdb_result_i64(r, 1);
            double  w  = tsdb_result_f64(r, 2);
            if (v < 500) { ASSERT(w == 0.0); zero_w++; }
            else         { ASSERT(w > 3.0);  nonzero_w++; }
            rows++;
        }
        tsdb_result_free(r);
        ASSERT(rows == 5);
        ASSERT(zero_w == 3);
        ASSERT(nonzero_w == 2);
        printf("  PASS: 5 rows, 3 with default w=0.0, 2 with real w\n");
    }

    /* Phase 5 — reopen, verify persistence */
    printf("\n[5] tsdb_close + tsdb_open — schema survives reopen\n");
    tsdb_close(db);
    OK(tsdb_open(dir, &db));
    OK(tsdb_open_table(db, "m", &t));
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM m;", &r));
        ASSERT(tsdb_result_next(r));
        int64_t cnt = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
        ASSERT(cnt == 5);
        printf("  PASS: count(*) = 5 after reopen\n");
    }
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT w FROM m;", &r));
        int rows = 0;
        while (tsdb_result_next(r)) rows++;
        tsdb_result_free(r);
        ASSERT(rows == 5);
        printf("  PASS: new column 'w' scannable after reopen\n");
    }

    /* Phase 6 — ALTER TABLE on unknown table → error */
    printf("\n[6] ALTER TABLE on missing table → error\n");
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "ALTER TABLE nope ADD COLUMN x INT64;", &r);
        ASSERT(rc != TSDB_OK);
        if (r) tsdb_result_free(r);
        printf("  PASS: rejected with rc=%d\n", rc);
    }

    /* Phase 7 — duplicate name rejected */
    printf("\n[7] ALTER TABLE m ADD COLUMN w (dup) → TSDB_ERR_EXISTS\n");
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "ALTER TABLE m ADD COLUMN w INT64;", &r);
        ASSERT(rc == TSDB_ERR_EXISTS);
        if (r) tsdb_result_free(r);
        printf("  PASS: duplicate rejected rc=%d\n", rc);
    }

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== ALL ALTER TABLE TESTS PASSED ===\n");
    return 0;
}
