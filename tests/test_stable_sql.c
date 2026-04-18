/* test_stable_sql.c — End-to-end QTL DDL + virtual STable query tests.
 *
 * Tests:
 *  1. CREATE STABLE meters (...)  TAGS (...)
 *  2. CREATE TABLE d1 USING meters TAGS ('east', 42)
 *  3. CREATE TABLE d2 USING meters TAGS ('west', 17)
 *  4. INSERT rows into d1 and d2 via batch API
 *  5. SELECT count(*) FROM meters  — expects d1+d2 total
 *  6. SELECT count(*) FROM meters WHERE location = 'east'  — only d1 rows
 *  7. DROP STABLE meters  — d1, d2 catalog + physical tables removed
 */

#include "tsdb.h"
#include "../src/catalog/stable.h"

struct tsdb_catalog;
struct tsdb_catalog *tsdb_db_catalog(tsdb_db_t *db);

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); \
} while (0)

#define OK(rc) do { \
    int _rc = (rc); \
    if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); \
} while (0)

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

/* Helper: run a DDL/query string and return the first status column string.
 * If expect_ok != 0, asserts the status starts with "OK". */
static void run_ddl(tsdb_db_t *db, const char *sql, int expect_ok) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (expect_ok) {
        if (rc != TSDB_OK) FAIL("query '%s' failed: rc=%d (%s)", sql, rc, tsdb_errstr(rc));
        ASSERT(r != NULL);
        if (tsdb_result_next(r) && tsdb_result_ncols(r) >= 1) {
            const char *s = tsdb_result_sym(r, 0);
            if (s && strncmp(s, "OK", 2) != 0)
                FAIL("Expected OK status for '%s', got '%s'", sql, s ? s : "(null)");
        }
    }
    if (r) tsdb_result_free(r);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_stable_sql";
    rm_rf(dir);

    printf("=== tsdb STable SQL end-to-end tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    /* ─ Phase 1: CREATE STABLE ─────────────────────────────────────────── */
    printf("\n[1] CREATE STABLE meters\n");
    run_ddl(db,
        "CREATE STABLE meters ("
        "  ts TIMESTAMP,"
        "  current FLOAT64,"
        "  voltage INT64"
        ") TAGS ("
        "  location SYMBOL,"
        "  groupId INT64"
        ");",
        1 /* expect OK */);

    struct tsdb_catalog *cat = tsdb_db_catalog(db);
    ASSERT(cat != NULL);
    ASSERT(tsdb_stable_exists(cat, "meters"));
    printf("  PASS: stable 'meters' registered in catalog\n");

    /* ─ Phase 2: CREATE TABLE child tables ─────────────────────────────── */
    printf("\n[2] CREATE TABLE d1 and d2 USING meters\n");
    run_ddl(db,
        "CREATE TABLE d1 USING meters TAGS ('east', 42);",
        1);
    run_ddl(db,
        "CREATE TABLE d2 USING meters TAGS ('west', 17);",
        1);

    /* Verify catalog entries */
    tsdb_child_table_t ct;
    OK(tsdb_child_table_get(cat, "d1", &ct));
    ASSERT(strcmp(ct.stable_name, "meters") == 0);
    ASSERT(strcmp(ct.tags[0].v.s, "east") == 0);
    ASSERT(ct.tags[1].v.i == 42);

    OK(tsdb_child_table_get(cat, "d2", &ct));
    ASSERT(strcmp(ct.tags[0].v.s, "west") == 0);
    ASSERT(ct.tags[1].v.i == 17);
    printf("  PASS: d1 (east, 42), d2 (west, 17) registered\n");

    /* ─ Phase 3: INSERT rows ────────────────────────────────────────────── */
    printf("\n[3] INSERT rows into d1 (3 rows) and d2 (2 rows)\n");
    {
        tsdb_table_t *tbl = NULL;
        OK(tsdb_open_table(db, "d1", &tbl));
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(tbl, &b));
        for (int i = 0; i < 3; i++) {
            OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(1000000000LL * (i + 1))));
            OK(tsdb_batch_row_f64(b, 1, 10.0 + i));  /* current */
            OK(tsdb_batch_row_i64(b, 2, 220 + i));    /* voltage */
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }
    {
        tsdb_table_t *tbl = NULL;
        OK(tsdb_open_table(db, "d2", &tbl));
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(tbl, &b));
        for (int i = 0; i < 2; i++) {
            OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(1000000000LL * (i + 10))));
            OK(tsdb_batch_row_f64(b, 1, 20.0 + i));  /* current */
            OK(tsdb_batch_row_i64(b, 2, 380 + i));    /* voltage */
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }
    printf("  PASS: 3 rows → d1, 2 rows → d2\n");

    /* ─ Phase 3.5: Non-aggregate row scan (exercises stable_result_concat) ─ */
    printf("\n[3.5] SELECT voltage FROM meters  (expect 5 rows via row-concat)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT voltage FROM meters;", &r));
        ASSERT(r != NULL);
        int nrows = 0;
        while (tsdb_result_next(r)) nrows++;
        printf("  row-concat rows = %d\n", nrows);
        ASSERT(nrows == 5);
        tsdb_result_free(r);
        printf("  PASS: non-aggregate union row count = 5\n");
    }

    /* ─ Phase 4: SELECT count(*) FROM meters (union all) ───────────────── */
    printf("\n[4] SELECT count(*) FROM meters  (expect 5 = 3+2)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM meters;", &r));
        ASSERT(r != NULL);
        ASSERT(tsdb_result_next(r));
        int64_t total = tsdb_result_i64(r, 0);
        printf("  count(*) = %lld\n", (long long)total);
        ASSERT(total == 5);
        ASSERT(!tsdb_result_next(r)); /* Only one row expected */
        tsdb_result_free(r);
        printf("  PASS: union count = 5\n");
    }

    /* ─ Phase 5: SELECT count(*) FROM meters WHERE location='east' ──────── */
    printf("\n[5] SELECT count(*) FROM meters WHERE location = 'east'  (expect 3)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM meters WHERE location = 'east';", &r));
        ASSERT(r != NULL);
        ASSERT(tsdb_result_next(r));
        int64_t east_cnt = tsdb_result_i64(r, 0);
        printf("  count(*) where location='east' = %lld\n", (long long)east_cnt);
        ASSERT(east_cnt == 3);
        tsdb_result_free(r);
        printf("  PASS: tag-pushdown count = 3\n");
    }

    /* ─ Phase 5.5: PARTITION BY tbname (one row per child) ──────────────── */
    printf("\n[5.5] SELECT count(*) FROM meters PARTITION BY tbname  (expect 2 rows)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM meters PARTITION BY tbname;", &r));
        ASSERT(r != NULL);
        ASSERT(tsdb_result_ncols(r) == 2);  /* tbname + count */
        int found_d1 = 0, found_d2 = 0;
        int nrows = 0;
        while (tsdb_result_next(r)) {
            const char *name = tsdb_result_sym(r, 0);
            int64_t  cnt  = tsdb_result_i64(r, 1);
            printf("  tbname=%s count=%lld\n", name ? name : "(null)", (long long)cnt);
            if (name && strcmp(name, "d1") == 0) { ASSERT(cnt == 3); found_d1 = 1; }
            if (name && strcmp(name, "d2") == 0) { ASSERT(cnt == 2); found_d2 = 1; }
            nrows++;
        }
        ASSERT(nrows == 2);
        ASSERT(found_d1 && found_d2);
        tsdb_result_free(r);
        printf("  PASS: PARTITION BY tbname emits 2 rows with distinct tbname\n");
    }

    /* Guard: PARTITION BY tbname on non-stable table must error. */
    printf("\n[5.6] PARTITION BY tbname on non-stable FROM must reject\n");
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT count(*) FROM d1 PARTITION BY tbname;", &r);
        ASSERT(rc != TSDB_OK);
        if (r) tsdb_result_free(r);
        printf("  PASS: rejected with rc=%d\n", rc);
    }

    /* ─ Phase 6: DROP STABLE meters ─────────────────────────────────────── */
    printf("\n[6] DROP STABLE meters\n");
    run_ddl(db, "DROP STABLE meters;", 1);

    ASSERT(!tsdb_stable_exists(cat, "meters"));
    tsdb_child_table_t probe;
    ASSERT(tsdb_child_table_get(cat, "d1", &probe) != TSDB_OK);
    ASSERT(tsdb_child_table_get(cat, "d2", &probe) != TSDB_OK);
    printf("  PASS: stable + children removed from catalog\n");

    /* ─ Phase 7: Verify physical tables are also gone ───────────────────── */
    printf("\n[7] Verify physical tables d1, d2 are dropped\n");
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT count(*) FROM d1;", &r);
        /* Should fail since table no longer exists */
        ASSERT(rc != TSDB_OK);
        if (r) tsdb_result_free(r);
        printf("  PASS: d1 is gone (query returned error %d)\n", rc);
    }
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT count(*) FROM d2;", &r);
        ASSERT(rc != TSDB_OK);
        if (r) tsdb_result_free(r);
        printf("  PASS: d2 is gone (query returned error %d)\n", rc);
    }

    tsdb_close(db);
    rm_rf(dir);

    /* ─ Phase 8: Performance probe — 10 child tables, union query ──────── */
    printf("\n[8] Performance: 10 child tables union query latency\n");
    {
        const char *perf_dir = "/tmp/tsdb_test_stable_perf";
        rm_rf(perf_dir);
        tsdb_db_t *pdb = NULL;
        OK(tsdb_open(perf_dir, &pdb));

        /* Create stable */
        run_ddl(pdb,
            "CREATE STABLE sensors ("
            "  ts TIMESTAMP, val FLOAT64"
            ") TAGS (region SYMBOL, gid INT64);",
            1);

        /* Create 10 child tables and insert 100 rows each */
        char sql[512];
        for (int i = 0; i < 10; i++) {
            snprintf(sql, sizeof(sql),
                "CREATE TABLE sensor%d USING sensors TAGS ('r%d', %d);",
                i, i, i);
            run_ddl(pdb, sql, 1);

            char tname[32]; snprintf(tname, sizeof(tname), "sensor%d", i);
            tsdb_table_t *tbl = NULL;
            OK(tsdb_open_table(pdb, tname, &tbl));
            tsdb_batch_t *b = NULL;
            OK(tsdb_batch_begin(tbl, &b));
            for (int row = 0; row < 100; row++) {
                OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(1000000000LL * (i * 100 + row + 1))));
                OK(tsdb_batch_row_f64(b, 1, (double)row));
                OK(tsdb_batch_row_end(b));
            }
            OK(tsdb_batch_commit(b));
        }

        /* Time the union query */
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        tsdb_result_t *r = NULL;
        OK(tsdb_query(pdb, "SELECT count(*) FROM sensors;", &r));
        ASSERT(r != NULL);
        ASSERT(tsdb_result_next(r));
        int64_t total = tsdb_result_i64(r, 0);
        tsdb_result_free(r);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        int64_t elapsed_us = ((int64_t)(t1.tv_sec - t0.tv_sec) * 1000000LL +
                              (t1.tv_nsec - t0.tv_nsec) / 1000LL);

        printf("  10 child tables × 100 rows = %lld total rows\n", (long long)total);
        printf("  union query latency: %lld µs\n", (long long)elapsed_us);
        ASSERT(total == 1000);

        tsdb_close(pdb);
        rm_rf(perf_dir);
        printf("  PASS: 10-child union in %lld µs\n", (long long)elapsed_us);
    }

    printf("\n=== ALL STABLE SQL TESTS PASSED ===\n");
    return 0;
}
