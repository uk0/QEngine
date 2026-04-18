/* test_group_by.c — multi-key GROUP BY hash-aggregate executor tests. */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("%s", #cond); } while (0)

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

static tsdb_ts_t ts_at(int64_t sec) {
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    return base + sec * 1000000000LL;
}

static void test_single_key_group_by(tsdb_db_t *db) {
    printf("\n[1] single-key GROUP BY\n");
    tsdb_table_t *t; OK(tsdb_open_table(db, "trades", &t));

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT sym, count(*), avg(price), sum(volume) "
                      "FROM trades GROUP BY sym", &r));
    ASSERT(tsdb_result_ncols(r) == 4);

    int rows = 0;
    int64_t total_count = 0;
    int64_t total_volume = 0;
    while (tsdb_result_next(r)) {
        const char *sym = tsdb_result_sym(r, 0);
        int64_t     cnt = tsdb_result_i64(r, 1);
        double      avg = tsdb_result_f64(r, 2);
        int64_t     vol = tsdb_result_i64(r, 3);
        printf("  sym=%-8s  count=%lld  avg=%.2f  sum(vol)=%lld\n",
               sym ? sym : "?", (long long)cnt, avg, (long long)vol);
        ASSERT(sym != NULL);
        ASSERT(cnt > 0);
        total_count  += cnt;
        total_volume += vol;
        rows++;
    }
    tsdb_result_free(r);

    ASSERT(rows == 3);            /* 3 distinct symbols */
    ASSERT(total_count == 900);   /* 300 per symbol × 3 */
    ASSERT(total_volume == 900 * 100); /* vol=100 each row */

    printf("  PASS: %d groups, totals consistent\n", rows);
}

static void test_multi_key_group_by(tsdb_db_t *db) {
    printf("\n[2] two-key GROUP BY (sym, rgn)\n");

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT sym, rgn, count(*) "
                      "FROM trades GROUP BY sym, rgn", &r));
    ASSERT(tsdb_result_ncols(r) == 3);

    int rows = 0;
    int64_t total_count = 0;
    while (tsdb_result_next(r)) {
        const char *sym = tsdb_result_sym(r, 0);
        const char *rgn = tsdb_result_sym(r, 1);
        int64_t     cnt = tsdb_result_i64(r, 2);
        printf("  sym=%-6s rgn=%-6s count=%lld\n",
               sym ? sym : "?", rgn ? rgn : "?", (long long)cnt);
        ASSERT(cnt > 0);
        total_count += cnt;
        rows++;
    }
    tsdb_result_free(r);

    /* 3 syms × 2 rgns = up to 6 groups. */
    ASSERT(rows <= 6 && rows >= 3);
    ASSERT(total_count == 900);
    printf("  PASS: %d groups, rows accounted for (%lld)\n", rows, (long long)total_count);
}

static void test_group_by_with_filter(tsdb_db_t *db) {
    printf("\n[3] GROUP BY with WHERE\n");

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT sym, count(*) FROM trades "
                      "WHERE price > 105.0 GROUP BY sym", &r));
    int rows = 0;
    while (tsdb_result_next(r)) {
        const char *sym = tsdb_result_sym(r, 0);
        int64_t     cnt = tsdb_result_i64(r, 1);
        printf("  sym=%-6s count=%lld\n", sym ? sym : "?", (long long)cnt);
        ASSERT(cnt > 0);
        rows++;
    }
    tsdb_result_free(r);

    ASSERT(rows >= 1 && rows <= 3);
    printf("  PASS: %d groups after WHERE filter\n", rows);
}

static void test_group_by_rejects_invalid(tsdb_db_t *db) {
    printf("\n[4] non-aggregated, non-group SELECT column is rejected\n");

    tsdb_result_t *r;
    int rc = tsdb_query(db, "SELECT price FROM trades GROUP BY sym", &r);
    ASSERT(rc != TSDB_OK);
    printf("  PASS: got rc=%d for invalid GROUP BY\n", rc);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_group_by";
    rm_rf(dir);

    printf("=== tsdb GROUP BY tests ===\n");

    tsdb_db_t *db; OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"sym",    TSDB_TYPE_SYMBOL},
        {"rgn", TSDB_TYPE_SYMBOL},
        {"price",  TSDB_TYPE_FLOAT64},
        {"volume", TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "trades", cols, 5, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "trades", &t));

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    const char *syms[]    = {"AAPL","MSFT","GOOG"};
    const char *rgns[] = {"us-east","us-west"};
    for (int i = 0; i < 900; i++) {
        OK(tsdb_batch_row_ts (b, ts_at(i)));
        OK(tsdb_batch_row_sym(b, 1, syms[i % 3]));
        OK(tsdb_batch_row_sym(b, 2, rgns[i % 2]));
        OK(tsdb_batch_row_f64(b, 3, 100.0 + (double)(i % 20)));
        OK(tsdb_batch_row_i64(b, 4, 100));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    test_single_key_group_by(db);
    test_multi_key_group_by(db);
    test_group_by_with_filter(db);
    test_group_by_rejects_invalid(db);

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== ALL GROUP BY TESTS PASSED ===\n");
    return 0;
}
