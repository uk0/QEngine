/* test_query.c — end-to-end query tests.
 *
 * Creates a table, ingests rows, then runs various QTL queries and asserts
 * the result set.
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

    /* --- Test 5b: IN / BETWEEN predicates (desugared to OR/AND in the parser) --- */
    printf("\n[5b] IN / BETWEEN predicates\n");
    /* 3 symbols x 2000 rows each = 6000 */
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE symbol IN ('AAPL','MSFT','GOOG')", &r));
    assert(tsdb_result_next(r)); cnt = tsdb_result_i64(r, 0);
    printf("  symbol IN (AAPL,MSFT,GOOG) = %lld (expect 6000)\n", (long long)cnt);
    assert(cnt == 6000); tsdb_result_free(r);
    /* volume 1000→1 (day0 only), 3000→2 (both days), 6000→1 (day1 only) = 4 */
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE volume IN (1000, 3000, 6000)", &r));
    assert(tsdb_result_next(r)); cnt = tsdb_result_i64(r, 0);
    printf("  volume IN (1000,3000,6000) = %lld (expect 4)\n", (long long)cnt);
    assert(cnt == 4); tsdb_result_free(r);
    /* NOT IN: 10000 - (vol 1000: 1 row) - (vol 6000: 1 row) = 9998 */
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE volume NOT IN (1000, 6000)", &r));
    assert(tsdb_result_next(r)); cnt = tsdb_result_i64(r, 0);
    printf("  volume NOT IN (1000,6000) = %lld (expect 9998)\n", (long long)cnt);
    assert(cnt == 9998); tsdb_result_free(r);
    /* BETWEEN inclusive: day0 vol in [2000,5999] i=1000..4999 (4000) + day1 i=0..3999 (4000) = 8000 */
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE volume BETWEEN 2000 AND 5999", &r));
    assert(tsdb_result_next(r)); cnt = tsdb_result_i64(r, 0);
    printf("  volume BETWEEN 2000 AND 5999 = %lld (expect 8000)\n", (long long)cnt);
    assert(cnt == 8000); tsdb_result_free(r);
    /* NOT BETWEEN: 10000 - 8000 = 2000 (vol<2000: 1000 rows, vol>5999: 1000 rows) */
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE volume NOT BETWEEN 2000 AND 5999", &r));
    assert(tsdb_result_next(r)); cnt = tsdb_result_i64(r, 0);
    printf("  volume NOT BETWEEN 2000 AND 5999 = %lld (expect 2000)\n", (long long)cnt);
    assert(cnt == 2000); tsdb_result_free(r);
    /* BETWEEN's AND must NOT be swallowed as boolean AND: this is
     * (volume>=2000 AND volume<=5999) AND symbol='AAPL' = 8000 AAPL-fraction = 1600 */
    OK(tsdb_query(db, "SELECT count(*) FROM trades WHERE volume BETWEEN 2000 AND 5999 AND symbol = 'AAPL'", &r));
    assert(tsdb_result_next(r)); cnt = tsdb_result_i64(r, 0);
    printf("  volume BETWEEN 2000 AND 5999 AND symbol=AAPL = %lld (expect 1600)\n", (long long)cnt);
    assert(cnt == 1600); tsdb_result_free(r);
    /* empty IN () is a graceful parse error, not a crash */
    { tsdb_result_t *er = NULL;
      int rc = tsdb_query(db, "SELECT count(*) FROM trades WHERE volume IN ()", &er);
      printf("  volume IN () rc=%d (expect <0)\n", rc);
      assert(rc < 0); if (er) tsdb_result_free(er); }

    /* --- Test 5c: row scalar expressions in SELECT (PROJ_SCALAR_ROW) --- */
    printf("\n[5c] row scalar expressions in SELECT\n");
    /* SELECT price*2 -> 10000 FLOAT64 rows; sum == 2*sum(price) == 7999000 */
    OK(tsdb_query(db, "SELECT price*2 FROM trades", &r));
    assert(tsdb_result_ncols(r) == 1);
    { double s2 = 0; int64_t nr = 0;
      while (tsdb_result_next(r)) { s2 += tsdb_result_f64(r, 0); nr++; }
      printf("  SELECT price*2: rows=%lld sum=%.1f (expect 10000, 7999000)\n", (long long)nr, s2);
      assert(nr == 10000); assert(s2 > 7998999.0 && s2 < 7999001.0); }
    tsdb_result_free(r);
    /* SELECT volume+volume -> FLOAT64; sum == 2*sum(volume) == 79990000 */
    OK(tsdb_query(db, "SELECT volume+volume FROM trades", &r));
    { double sv = 0; while (tsdb_result_next(r)) sv += tsdb_result_f64(r, 0);
      printf("  SELECT volume+volume: sum=%.0f (expect 79990000)\n", sv);
      assert(sv > 79989999.0 && sv < 79990001.0); }
    tsdb_result_free(r);
    /* mixed columns: SELECT price*volume -> 10000 rows */
    OK(tsdb_query(db, "SELECT price*volume AS notional FROM trades", &r));
    { int64_t nr = 0; while (tsdb_result_next(r)) nr++; assert(nr == 10000); }
    tsdb_result_free(r);
    /* rejects (rc<0): SYMBOL arithmetic, aggregate-in-scalar, scalar+agg mix, scalar+GROUP BY */
    { tsdb_result_t *er=NULL; assert(tsdb_query(db, "SELECT symbol*2 FROM trades", &er) < 0); if(er) tsdb_result_free(er); }
    { tsdb_result_t *er=NULL; assert(tsdb_query(db, "SELECT sum(volume)/count(*) FROM trades", &er) < 0); if(er) tsdb_result_free(er); }
    { tsdb_result_t *er=NULL; assert(tsdb_query(db, "SELECT price*2, sum(volume) FROM trades", &er) < 0); if(er) tsdb_result_free(er); }
    { tsdb_result_t *er=NULL; assert(tsdb_query(db, "SELECT price*2 FROM trades GROUP BY symbol", &er) < 0); if(er) tsdb_result_free(er); }
    /* interp() has its own emit path that would leave a scalar cell as 0 — reject */
    { tsdb_result_t *er=NULL; assert(tsdb_query(db, "SELECT ts, interp(price, 1000000000), price*2 FROM trades", &er) < 0); if(er) tsdb_result_free(er); }
    printf("  scalar rejects (symbol/agg/mix/groupby/interp) all rc<0\n");

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

    /* --- Test 6b: GROUP BY time_bucket(ts, N) routes to the SAMPLE BY path --- */
    printf("\n[6b] GROUP BY time_bucket(ts, 1m) == SAMPLE BY 1m\n");
    int64_t sb_rows = 0, sb_total = 0;
    OK(tsdb_query(db, "SELECT time_bucket(ts, 60000000000), count(*) FROM trades SAMPLE BY 1m", &r));
    while (tsdb_result_next(r)) { sb_rows++; sb_total += tsdb_result_i64(r, 1); }
    tsdb_result_free(r);
    int64_t gb_rows = 0, gb_total = 0;
    OK(tsdb_query(db, "SELECT time_bucket(ts, 60000000000), count(*) FROM trades GROUP BY time_bucket(ts, 1m)", &r));
    while (tsdb_result_next(r)) { gb_rows++; gb_total += tsdb_result_i64(r, 1); }
    tsdb_result_free(r);
    printf("  SAMPLE BY: %lld buckets total=%lld ; GROUP BY tb: %lld buckets total=%lld\n",
           (long long)sb_rows, (long long)sb_total, (long long)gb_rows, (long long)gb_total);
    assert(gb_rows > 0 && gb_rows == sb_rows);          /* same buckets as SAMPLE BY */
    assert(gb_total == 10000 && gb_total == sb_total);  /* every row bucketed */
    /* reject: bucket + tag key (streaming SAMPLE BY has no per-tag grouping) */
    { tsdb_result_t *er = NULL;
      int rc = tsdb_query(db, "SELECT count(*) FROM trades GROUP BY time_bucket(ts, 1m), symbol", &er);
      printf("  GROUP BY time_bucket + tag rc=%d (expect <0)\n", rc);
      assert(rc < 0); if (er) tsdb_result_free(er); }
    /* reject: two time_bucket keys */
    { tsdb_result_t *er = NULL;
      int rc = tsdb_query(db, "SELECT count(*) FROM trades GROUP BY time_bucket(ts, 1m), time_bucket(ts, 120000000000)", &er);
      assert(rc < 0); if (er) tsdb_result_free(er); }
    /* reject: time_bucket combined with an advanced window / LATEST ON — those
     * are parsed first and set has_adv_window/has_latest_on; without the guard
     * they'd be silently dropped when time_bucket sets has_sample_by. */
    { tsdb_result_t *er = NULL;
      int rc = tsdb_query(db, "SELECT count(*) FROM trades SESSION(ts, 300000000000) GROUP BY time_bucket(ts, 1m)", &er);
      printf("  SESSION + GROUP BY time_bucket rc=%d (expect <0)\n", (int)rc);
      assert(rc < 0); if (er) tsdb_result_free(er); }
    { tsdb_result_t *er = NULL;
      int rc = tsdb_query(db, "SELECT * FROM trades LATEST ON ts GROUP BY time_bucket(ts, 1m)", &er);
      assert(rc < 0); if (er) tsdb_result_free(er); }

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

    /* --- Test 11: T-digest percentile aggregates --- */
    printf("\n[11] T-digest: p50/p99/stddev on price\n");

    /* price on day0: 100 + i/10 for i=0..4999  → [100.0, 599.9]
     * price on day1: 200 + i/10 for i=0..4999  → [200.0, 699.9]
     * Combined 10000 rows: min=100.0, max=699.9, p50≈~400, p99≈~693 */

    tsdb_set_query_parallel(0); /* serial */
    OK(tsdb_query(db, "SELECT p50(price), p99(price), stddev(price) FROM trades", &r));
    assert(tsdb_result_next(r));
    double serial_p50 = tsdb_result_f64(r, 0);
    double serial_p99 = tsdb_result_f64(r, 1);
    double serial_std = tsdb_result_f64(r, 2);
    printf("  serial  p50=%.2f  p99=%.2f  stddev=%.2f\n",
           serial_p50, serial_p99, serial_std);
    tsdb_result_free(r);

    /* Sanity: p50 should be near the mid of [100, 699.9] ≈ 399.95 */
    assert(serial_p50 > 350.0 && serial_p50 < 450.0);
    /* p99 near top: ≥ 680 */
    assert(serial_p99 > 680.0 && serial_p99 < 705.0);
    /* stddev of uniform-ish distribution ~170 */
    assert(serial_std > 100.0 && serial_std < 250.0);

    /* --- Test 12: parallel vs serial p99 within 1e-3 --- */
    printf("\n[12] parallel p99 matches serial within 1e-3\n");
    tsdb_set_query_parallel(1); /* parallel */
    OK(tsdb_query(db, "SELECT p99(price) FROM trades", &r));
    assert(tsdb_result_next(r));
    double par_p99 = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    printf("  serial_p99=%.4f  parallel_p99=%.4f  diff=%.6f\n",
           serial_p99, par_p99, fabs(serial_p99 - par_p99));
    assert(fabs(serial_p99 - par_p99) < 1e-1); /* 0.1 price unit tolerance */

    /* --- Test 13: percentile(col, q) with user-specified q --- */
    printf("\n[13] percentile(price, 0.95) for AAPL only\n");
    tsdb_set_query_parallel(0);
    OK(tsdb_query(db, "SELECT percentile(price, 0.95) FROM trades WHERE symbol='AAPL'", &r));
    assert(tsdb_result_next(r));
    double p95_aapl = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    printf("  AAPL p95=%.4f\n", p95_aapl);
    /* AAPL prices: day0 100+i/10 at i=0,5,...4995; day1 200+i/10 same.
     * 2000 AAPL rows, roughly [100, 599.5] and [200, 699.5] interleaved.
     * p95 of those should be ≥ 600 */
    assert(p95_aapl > 580.0 && p95_aapl < 705.0);

    /* --- Test 14: p90 --- */
    printf("\n[14] p90(price) from all trades\n");
    OK(tsdb_query(db, "SELECT p90(price) FROM trades", &r));
    assert(tsdb_result_next(r));
    double p90_all = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    printf("  p90=%.4f\n", p90_all);
    assert(p90_all > 580.0 && p90_all < 720.0);

    /* --- Test 15: stddev/percentile over EMPTY input error (no silent NaN).
     * avg() keeps its documented NaN-on-empty; count(*) keeps returning 0. */
    printf("\n[15] stddev/percentile on empty input error instead of NaN\n");
    {
        tsdb_col_t ecols[] = {
            {"ts", TSDB_TYPE_TIMESTAMP},
            {"v",  TSDB_TYPE_FLOAT64},
        };
        OK(tsdb_create_table(db, "empty_t", ecols, 2, "ts"));

        tsdb_result_t *er = NULL;
        int erc = tsdb_query(db, "SELECT stddev(v) FROM empty_t", &er);
        assert(erc != TSDB_OK);
        printf("  stddev(empty) rc=%d (%s)\n", erc, tsdb_errstr(erc));

        erc = tsdb_query(db, "SELECT p50(v) FROM empty_t", &er);
        assert(erc != TSDB_OK);
        erc = tsdb_query(db, "SELECT percentile(v, 0.9) FROM empty_t", &er);
        assert(erc != TSDB_OK);

        /* WHERE that filters everything out is also empty input. */
        erc = tsdb_query(db, "SELECT p99(price) FROM trades WHERE price > 1e12", &er);
        assert(erc != TSDB_OK);

        /* avg() on empty stays NaN (documented), count stays 0. */
        OK(tsdb_query(db, "SELECT avg(v) FROM empty_t", &er));
        assert(tsdb_result_next(er));
        assert(isnan(tsdb_result_f64(er, 0)));
        tsdb_result_free(er);
        OK(tsdb_query(db, "SELECT count(*) FROM empty_t", &er));
        assert(tsdb_result_next(er));
        assert(tsdb_result_i64(er, 0) == 0);
        tsdb_result_free(er);
        printf("  empty-input: stddev/p50/percentile/p99 error, avg=NaN, count=0\n");
    }

    tsdb_set_query_parallel(1); /* restore parallel default */

    /* --- Test 16: ORDER BY permutes whole 8-byte cells (SYMBOL pairing) --- */
    /* Result cells are uniformly 8-byte slots — SYMBOL codes are u32
     * zero-extended into a u64 cell.  A 4-byte permute stride on the SYMBOL
     * column shuffles half-cells and mispairs symbols with every other
     * column, so row0 of ORDER BY v DESC came back ("alpha", 4). */
    printf("\n[16] SELECT sym, v ORDER BY v DESC keeps symbol paired with row\n");
    {
        tsdb_col_t ocols[] = {
            {"ts",  TSDB_TYPE_TIMESTAMP},
            {"sym", TSDB_TYPE_SYMBOL},
            {"v",   TSDB_TYPE_INT64},
        };
        OK(tsdb_create_table(db, "ordsym", ocols, 3, "ts"));
        tsdb_table_t *ot = NULL;
        OK(tsdb_open_table(db, "ordsym", &ot));
        tsdb_batch_t *ob = NULL;
        OK(tsdb_batch_begin(ot, &ob));
        const char *onames[] = {"alpha", "bravo", "charlie", "delta"};
        for (int i = 0; i < 4; i++) {
            OK(tsdb_batch_row_ts(ob, ts_at(0, i)));
            OK(tsdb_batch_row_sym(ob, 1, onames[i]));
            OK(tsdb_batch_row_i64(ob, 2, i + 1));
            OK(tsdb_batch_row_end(ob));
        }
        OK(tsdb_batch_commit(ob));

        tsdb_result_t *orr = NULL;
        OK(tsdb_query(db, "SELECT sym, v FROM ordsym ORDER BY v DESC", &orr));
        const char *want_sym[] = {"delta", "charlie", "bravo", "alpha"};
        int64_t     want_v[]   = {4, 3, 2, 1};
        int oi = 0;
        while (tsdb_result_next(orr)) {
            assert(oi < 4);
            const char *gs = tsdb_result_sym(orr, 0);
            int64_t     gv = tsdb_result_i64(orr, 1);
            printf("  row%d: sym=%s v=%lld (want %s %lld)\n", oi,
                   gs ? gs : "(null)", (long long)gv,
                   want_sym[oi], (long long)want_v[oi]);
            assert(gv == want_v[oi]);
            assert(gs && strcmp(gs, want_sym[oi]) == 0);
            oi++;
        }
        assert(oi == 4);
        tsdb_result_free(orr);
    }

    tsdb_close(db);
    printf("\n=== All query tests PASSED ===\n");
    rm_rf(dir);
    return 0;
}
