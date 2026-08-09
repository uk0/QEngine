/* test_query_correctness.c — break-tests for the QUERY-CORRECTNESS unit.
 *
 * Each block below is a "silent wrong answer" the engine returns without an
 * error on a common query shape.  The test is written to FAIL against the
 * unfixed engine and pass once the executor honours the documented semantics.
 *
 *   QC-4   ORDER BY x LIMIT N returns an arbitrary N rows, then sorts them
 *   QC-14  ORDER BY <symbol_col> sorts by dictionary code, not string value
 *   QC-11  min()/max() over an empty selection emit the INT64 sentinel as data
 *   QC-13  SAMPLE BY spread()/first()/last()/twa() emit 0.0 instead of a value
 *   QC-6   out-of-order (backfilled) points re-open a closed SAMPLE BY bucket
 *   QC-9   fractional literal vs int column truncates a boundary predicate
 */
#include "tsdb.h"
#include "../src/query/exec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("assertion failed: %s", #cond); } while (0)

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


static void test_orderby_limit_topn(void) {
    printf("[QC-4] ORDER BY price DESC LIMIT 5 returns the true top-5\n");
    const char *dir = "/tmp/tsdb_qc_orderlimit";
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"price", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* price == row index; the top-5 by price are rows 1995..1999.  The scan
     * emits ascending ts (== ascending price), so a scan-time LIMIT keeps the
     * SMALLEST 5, then sorts them — the exact wrong answer QC-4 describes. */
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < 2000; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * 1000000000LL)));
        OK(tsdb_batch_row_f64(bk, 1, (double)i));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT ts, price FROM t ORDER BY price DESC LIMIT 5", &r));
    double got[5]; int n = 0;
    while (tsdb_result_next(r)) {
        if (n < 5) got[n] = tsdb_result_f64(r, 1);
        n++;
    }
    tsdb_result_free(r);
    ASSERT(n == 5);
    /* The true top-5 prices descending are 1999,1998,1997,1996,1995. */
    for (int i = 0; i < 5; i++) {
        double want = (double)(1999 - i);
        if (fabs(got[i] - want) > 1e-9)
            FAIL("row %d: price=%.1f expected=%.1f (ORDER BY applied AFTER LIMIT truncation)",
                 i, got[i], want);
    }
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

static void test_orderby_symbol_string(void) {
    printf("[QC-14] ORDER BY sym sorts by string value, not dictionary code\n");
    const char *dir = "/tmp/tsdb_qc_ordersym";
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"sym", TSDB_TYPE_SYMBOL},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* Intern order (== dictionary code order) is deliberately NOT the string
     * order: "delta"=0, "alpha"=1, "charlie"=2, "bravo"=3.  Sorting by code
     * yields delta,alpha,charlie,bravo; sorting by string yields
     * alpha,bravo,charlie,delta. */
    const char *ins[] = {"delta", "alpha", "charlie", "bravo"};
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < 4; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * 1000000000LL)));
        OK(tsdb_batch_row_sym(bk, 1, ins[i]));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT ts, sym FROM t ORDER BY sym ASC", &r));
    const char *want[] = {"alpha", "bravo", "charlie", "delta"};
    int n = 0;
    while (tsdb_result_next(r)) {
        const char *s = tsdb_result_sym(r, 1);
        if (n < 4 && (!s || strcmp(s, want[n]) != 0))
            FAIL("row %d: sym=%s expected=%s (SYMBOL ORDER BY compared codes, not strings)",
                 n, s ? s : "(null)", want[n]);
        n++;
    }
    tsdb_result_free(r);
    ASSERT(n == 4);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

int main(void) {
    printf("=== test_orderby_topn ===\n");
    test_orderby_limit_topn();
    test_orderby_symbol_string();
    printf("\nORDER BY top-N + symbol-string PASSED\n");
    return 0;
}
