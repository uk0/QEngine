/* test_query.c — end-to-end query tests.
 *
 * Creates a table, ingests rows, then runs various QTL queries and asserts
 * the result set.
 */
#include "tsdb.h"
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

static tsdb_ts_t ts_at(int day, int sec) {
    /* 2026-01-01 00:00:00 UTC + day days + sec seconds, nanosecond resolution */
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    return base + ((tsdb_ts_t)day * 86400LL + sec) * 1000000000LL;
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_query";
    rm_rf(dir);

    printf("=== tsdb query tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"symbol", TSDB_TYPE_SYMBOL},
        {"price",  TSDB_TYPE_FLOAT64},
        {"volume", TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "trades", cols, 4, "ts"));

    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "trades", &t));

    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    const char *syms[] = {"AAPL", "MSFT", "GOOG", "AMZN", "META"};
    for (int i = 0; i < 5000; i++) {
        OK(tsdb_batch_row_ts(b, ts_at(0, i)));
        OK(tsdb_batch_row_sym(b, 1, syms[i % 5]));
        OK(tsdb_batch_row_f64(b, 2, 100.0 + (double)i / 10.0));
        OK(tsdb_batch_row_i64(b, 3, 1000 + i));
        OK(tsdb_batch_row_end(b));
    }
    for (int i = 0; i < 5000; i++) {
        OK(tsdb_batch_row_ts(b, ts_at(1, i)));
        OK(tsdb_batch_row_sym(b, 1, syms[i % 5]));
        OK(tsdb_batch_row_f64(b, 2, 200.0 + (double)i / 10.0));
        OK(tsdb_batch_row_i64(b, 3, 2000 + i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    /* --- Test 1: SELECT * LIMIT --- */
    printf("\n[1] SELECT * FROM trades LIMIT 3\n");
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT * FROM trades LIMIT 3", &r));
    assert(tsdb_result_ncols(r) == 4);
    int nrows = 0;
    while (tsdb_result_next(r)) {
        printf("  ts=%lld symbol=%s price=%.2f volume=%lld\n",
               (long long)tsdb_result_ts(r, 0),
               tsdb_result_sym(r, 1),
               tsdb_result_f64(r, 2),
               (long long)tsdb_result_i64(r, 3));
        nrows++;
    }
    assert(nrows == 3);
    tsdb_result_free(r);

    /* --- Test 2: SELECT col list with WHERE --- */
    printf("\n[2] SELECT ts, price WHERE symbol = 'AAPL' LIMIT 3\n");
    OK(tsdb_query(db, "SELECT ts, price FROM trades WHERE symbol = 'AAPL' LIMIT 3", &r));
    assert(tsdb_result_ncols(r) == 2);
    nrows = 0;
    while (tsdb_result_next(r)) {
        printf("  ts=%lld price=%.2f\n", (long long)tsdb_result_ts(r, 0), tsdb_result_f64(r, 1));
        nrows++;
    }
    assert(nrows == 3);
    tsdb_result_free(r);

    /* --- Test 3: count(*) --- */
    printf("\n[3] SELECT count(*) FROM trades\n");
    OK(tsdb_query(db, "SELECT count(*) FROM trades", &r));
    assert(tsdb_result_ncols(r) == 1);
    assert(tsdb_result_next(r));
    int64_t cnt = tsdb_result_i64(r, 0);
    printf("  count=%lld\n", (long long)cnt);
    assert(cnt == 10000);
    tsdb_result_free(r);

    /* --- Test 4: aggregations --- */
    printf("\n[4] SELECT sum(volume), avg(price), min(price), max(price) FROM trades WHERE symbol='AAPL'\n");
    OK(tsdb_query(db, "SELECT sum(volume), avg(price), min(price), max(price) FROM trades WHERE symbol = 'AAPL'", &r));
    assert(tsdb_result_ncols(r) == 4);
    assert(tsdb_result_next(r));
    int64_t sum_vol = tsdb_result_i64(r, 0);
    double avg_p = tsdb_result_f64(r, 1);
    double min_p = tsdb_result_f64(r, 2);
    double max_p = tsdb_result_f64(r, 3);
    printf("  sum_vol=%lld  avg=%.3f  min=%.2f  max=%.2f\n",
           (long long)sum_vol, avg_p, min_p, max_p);
    /* AAPL is at i=0,5,10,...,4995. Day 0 vol=1000+i; day 1 vol=2000+i.
     * sum_vol = 1000*1000 + sum(0,5,...,4995) + 1000*2000 + sum(0,5,...,4995)
     *         = 1000000 + 2497500 + 2000000 + 2497500 = 7995000 */
    assert(sum_vol == 7995000);
    assert(min_p >= 100.0 && min_p <= 100.001);
    assert(max_p >= 699.0 && max_p <= 700.0);
    tsdb_result_free(r);

    /* --- Test 5: time range filter --- */
    printf("\n[5] SELECT count(*) WHERE ts >= '2026-01-02' AND symbol = 'MSFT'\n");
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE ts >= '2026-01-02' AND symbol = 'MSFT'", &r));
    assert(tsdb_result_next(r));
    cnt = tsdb_result_i64(r, 0);
    printf("  count=%lld\n", (long long)cnt);
    /* Day 1: 5000 rows, MSFT is i=1,6,...,4996 → 1000 rows */
    assert(cnt == 1000);
    tsdb_result_free(r);

    /* --- Test 6: SAMPLE BY aggregation --- */
    printf("\n[6] SELECT time_bucket(ts, 1m), avg(price) FROM trades WHERE symbol='AAPL' SAMPLE BY 1m LIMIT 5\n");
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 60000000000), avg(price) FROM trades "
        "WHERE symbol = 'AAPL' SAMPLE BY 1m LIMIT 5", &r));
    assert(tsdb_result_ncols(r) == 2);
    nrows = 0;
    while (tsdb_result_next(r)) {
        tsdb_ts_t bkt = tsdb_result_ts(r, 0);
        double avg = tsdb_result_f64(r, 1);
        printf("  bucket=%lld avg=%.3f\n", (long long)bkt, avg);
        nrows++;
    }
    assert(nrows == 5);
    tsdb_result_free(r);

    /* --- Test 7: parse errors --- */
    printf("\n[7] parse error tests\n");
    assert(tsdb_query(db, "SELECT FROM trades", &r) < 0);
    assert(tsdb_query(db, "SELECT * FROM nonexistent", &r) < 0);
    printf("  parse errors rejected as expected\n");

    /* --- Test 8: LATEST ON without partition (single latest row) --- */
    printf("\n[8] SELECT * FROM trades LATEST ON ts\n");
    OK(tsdb_query(db, "SELECT * FROM trades LATEST ON ts", &r));
    int rows = 0;
    tsdb_ts_t latest_ts = 0;
    while (tsdb_result_next(r)) {
        latest_ts = tsdb_result_ts(r, 0);
        rows++;
    }
    assert(rows == 1);
    /* Newest row is day1 i=4999 → ts_at(1, 4999) */
    assert(latest_ts == ts_at(1, 4999));
    printf("  latest_ts=%lld (expected=%lld)  PASS\n",
           (long long)latest_ts, (long long)ts_at(1, 4999));
    tsdb_result_free(r);

    /* --- Test 9: LATEST ON with PARTITION BY symbol (one row per symbol) --- */
    printf("\n[9] SELECT * FROM trades LATEST ON ts PARTITION BY symbol\n");
    OK(tsdb_query(db, "SELECT * FROM trades LATEST ON ts PARTITION BY symbol", &r));
    rows = 0;
    while (tsdb_result_next(r)) {
        printf("  symbol=%s ts=%lld price=%.2f\n",
               tsdb_result_sym(r, 1),
               (long long)tsdb_result_ts(r, 0),
               tsdb_result_f64(r, 2));
        rows++;
    }
    assert(rows == 5); /* 5 distinct symbols */
    tsdb_result_free(r);

    /* --- Test 10: block skipping via ts range filter --- */
    printf("\n[10] block-skip: WHERE ts < '2026-01-02'\n");
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE ts < '2026-01-02'", &r));
    assert(tsdb_result_next(r));
    int64_t c_day0 = tsdb_result_i64(r, 0);
    printf("  day0 rows = %lld\n", (long long)c_day0);
    assert(c_day0 == 5000);
    tsdb_result_free(r);

    tsdb_close(db);
    printf("\n=== All query tests PASSED ===\n");
    rm_rf(dir);
    return 0;
}
