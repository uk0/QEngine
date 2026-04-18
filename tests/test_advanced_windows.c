/* test_advanced_windows.c — SESSION / STATE_WINDOW / EVENT_WINDOW tests.
 *
 * Tests three advanced windowing modes that mirror TDengine's API:
 *
 *  1. SESSION window  — gap-based: new window when consecutive ts gap > threshold
 *  2. STATE_WINDOW    — value-change: new window when a state column changes
 *  3. EVENT_WINDOW    — expression boundary: open on start_expr, close on end_expr
 */
#include "tsdb.h"
#include "../src/query/exec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) \
    do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) \
    do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)
#define ASSERT(cond) \
    do { if (!(cond)) FAIL("assertion failed: %s", #cond); } while (0)

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

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* =========================================================================
 * Test 1: SESSION window
 *
 * Data: 5 rows with timestamps 0, 1s, 2s, 10s, 11s (values 1, 2, 3, 4, 5)
 * Gap threshold: 3s (= 3_000_000_000 ns)
 *
 * Row 0 ts=0  val=1  → window1 opens
 * Row 1 ts=1s val=2  → gap 1s <= 3s → same window1
 * Row 2 ts=2s val=3  → gap 1s <= 3s → same window1
 * Row 3 ts=10s val=4 → gap 8s > 3s  → flush window1, open window2
 * Row 4 ts=11s val=5 → gap 1s <= 3s → same window2
 *
 * Expected: 2 windows
 *   window1: count=3, avg=2.0  (vals 1,2,3)
 *   window2: count=2, avg=4.5  (vals 4,5)
 * =========================================================================
 */
static void test_session_window(void) {
    printf("[1] SESSION window: gap-based windowing\n");

    const char *dir = "/tmp/tsdb_adv_session";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    int64_t S = 1000000000LL; /* 1 second in ns */
    int64_t timestamps[] = { 0, 1*S, 2*S, 10*S, 11*S };
    double  values[]     = { 1.0, 2.0, 3.0, 4.0, 5.0 };

    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < 5; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)timestamps[i]));
        OK(tsdb_batch_row_f64(bk, 1, values[i]));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    /* Query: SESSION(ts, 3s) — gap > 3s triggers new window */
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT avg(val), count(*) FROM t "
        "SESSION(ts, 3s)",
        &res));

    int nrows = 0;
    double avgs[8];
    int64_t counts[8];
    while (tsdb_result_next(res)) {
        if (nrows >= 8) FAIL("too many rows");
        avgs[nrows]   = tsdb_result_f64(res, 0);
        counts[nrows] = tsdb_result_i64(res, 1);
        nrows++;
    }
    tsdb_result_free(res);

    ASSERT(nrows == 2);
    /* window1: avg=2.0, count=3 */
    if (fabs(avgs[0] - 2.0) > 0.001)
        FAIL("window1 avg: expected 2.0, got %.3f", avgs[0]);
    if (counts[0] != 3)
        FAIL("window1 count: expected 3, got %lld", (long long)counts[0]);
    /* window2: avg=4.5, count=2 */
    if (fabs(avgs[1] - 4.5) > 0.001)
        FAIL("window2 avg: expected 4.5, got %.3f", avgs[1]);
    if (counts[1] != 2)
        FAIL("window2 count: expected 2, got %lld", (long long)counts[1]);

    printf("  OK: 2 windows, avgs=[%.1f, %.1f], counts=[%lld, %lld]\n",
           avgs[0], avgs[1], (long long)counts[0], (long long)counts[1]);

    tsdb_close(db);
    rm_rf(dir);
}

/* =========================================================================
 * Test 2: STATE_WINDOW
 *
 * Data: 6 rows, status column = A,A,B,B,A,C (encoded as ints for simplicity)
 *   ts: 0, 1s, 2s, 3s, 4s, 5s
 *   status: 1, 1, 2, 2, 1, 3
 *
 * Expected: 4 windows
 *   status=1(A): count=2  (rows 0,1)
 *   status=2(B): count=2  (rows 2,3)
 *   status=1(A): count=1  (row 4)
 *   status=3(C): count=1  (row 5)
 * =========================================================================
 */
static void test_state_window(void) {
    printf("[2] STATE_WINDOW: value-change windowing\n");

    const char *dir = "/tmp/tsdb_adv_state";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"status", TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    int64_t S = 1000000000LL;
    int64_t statuses[] = { 1, 1, 2, 2, 1, 3 };

    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < 6; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * S)));
        OK(tsdb_batch_row_i64(bk, 1, statuses[i]));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT count(*) FROM t "
        "STATE_WINDOW(status)",
        &res));

    int nrows = 0;
    int64_t counts[8];
    while (tsdb_result_next(res)) {
        if (nrows >= 8) FAIL("too many rows");
        counts[nrows] = tsdb_result_i64(res, 0);
        nrows++;
    }
    tsdb_result_free(res);

    ASSERT(nrows == 4);
    if (counts[0] != 2) FAIL("win1 count: expected 2, got %lld", (long long)counts[0]);
    if (counts[1] != 2) FAIL("win2 count: expected 2, got %lld", (long long)counts[1]);
    if (counts[2] != 1) FAIL("win3 count: expected 1, got %lld", (long long)counts[2]);
    if (counts[3] != 1) FAIL("win4 count: expected 1, got %lld", (long long)counts[3]);

    printf("  OK: 4 windows, counts=[%lld, %lld, %lld, %lld]\n",
           (long long)counts[0], (long long)counts[1],
           (long long)counts[2], (long long)counts[3]);

    tsdb_close(db);
    rm_rf(dir);
}

/* =========================================================================
 * Test 3: EVENT_WINDOW
 *
 * Data: temperature sequence (ts=0..7s): 10, 15, 25, 30, 20, 8, 12, 28
 *
 * start_expr: temp > 20
 * end_expr:   temp < 15
 *
 * Trace (inclusive end semantics — end row is included in window):
 *   row0  ts=0  temp=10  start? no  end? yes (10<15) — not in window, no effect
 *   row1  ts=1s temp=15  start? no  end? no
 *   row2  ts=2s temp=25  start? YES → open window1, win1 start ts=2s
 *                        end? no (25 not <15)
 *   row3  ts=3s temp=30  in window1, end? no
 *   row4  ts=4s temp=20  in window1, end? no
 *   row5  ts=5s temp=8   in window1, end? YES (8<15) → accumulate 8 then flush
 *                        window1: rows={25,30,20,8}, count=4, avg=20.75
 *   row6  ts=6s temp=12  start? no  end? yes (12<15) — not in window
 *   row7  ts=7s temp=28  start? YES → open window2, win2 start ts=7s
 *                        end? no → scan ends, flush window2
 *                        window2: rows={28}, count=1, avg=28.0
 *
 * Expected: 2 windows
 *   window1: avg=20.75, count=4
 *   window2: avg=28.0,  count=1
 * =========================================================================
 */
static void test_event_window(void) {
    printf("[3] EVENT_WINDOW: expression boundary windowing\n");

    const char *dir = "/tmp/tsdb_adv_event";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",   TSDB_TYPE_TIMESTAMP},
        {"temp", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    int64_t S = 1000000000LL;
    double temps[] = { 10.0, 15.0, 25.0, 30.0, 20.0, 8.0, 12.0, 28.0 };

    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < 8; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)i * S)));
        OK(tsdb_batch_row_f64(bk, 1, temps[i]));
        OK(tsdb_batch_row_end(bk));
    }
    OK(tsdb_batch_commit(bk));

    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT avg(temp), count(*) FROM t "
        "EVENT_WINDOW(temp > 20, temp < 15)",
        &res));

    int nrows = 0;
    double avgs[8];
    int64_t counts[8];
    while (tsdb_result_next(res)) {
        if (nrows >= 8) FAIL("too many rows");
        avgs[nrows]   = tsdb_result_f64(res, 0);
        counts[nrows] = tsdb_result_i64(res, 1);
        nrows++;
    }
    tsdb_result_free(res);

    ASSERT(nrows == 2);
    /* window1: {25, 30, 20, 8} → avg=20.75, count=4 */
    if (fabs(avgs[0] - 20.75) > 0.01)
        FAIL("window1 avg: expected 20.75, got %.4f", avgs[0]);
    if (counts[0] != 4)
        FAIL("window1 count: expected 4, got %lld", (long long)counts[0]);
    /* window2: {28} → avg=28.0, count=1 */
    if (fabs(avgs[1] - 28.0) > 0.01)
        FAIL("window2 avg: expected 28.0, got %.4f", avgs[1]);
    if (counts[1] != 1)
        FAIL("window2 count: expected 1, got %lld", (long long)counts[1]);

    printf("  OK: 2 windows, avgs=[%.2f, %.2f], counts=[%lld, %lld]\n",
           avgs[0], avgs[1], (long long)counts[0], (long long)counts[1]);

    tsdb_close(db);
    rm_rf(dir);
}

/* =========================================================================
 * Test 4: SESSION window performance — 10M rows with irregular timestamps
 * =========================================================================
 */
static void test_session_perf(void) {
    printf("[4] SESSION window performance: 10M rows\n");

    const char *dir = "/tmp/tsdb_adv_perf";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* Generate 10M rows with irregular ts: mostly 1ms apart, occasional 10s gaps
     * to create ~1000 sessions. */
    int64_t N = 10000000;
    int64_t ts = 0;
    int64_t one_ms  = 1000000LL;
    int64_t gap_10s = 10000000000LL;

    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int64_t i = 0; i < N; i++) {
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)ts));
        OK(tsdb_batch_row_f64(bk, 1, (double)(i % 100)));
        OK(tsdb_batch_row_end(bk));
        /* Every ~10000 rows inject a >5s gap to create a new session */
        if ((i + 1) % 10000 == 0) ts += gap_10s;
        else ts += one_ms;

        /* Flush periodically to avoid huge memtable */
        if ((i + 1) % 500000 == 0) {
            OK(tsdb_batch_commit(bk));
            OK(tsdb_batch_begin(tbl, &bk));
        }
    }
    OK(tsdb_batch_commit(bk));

    int64_t t0 = now_ns();
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT count(*) FROM t "
        "SESSION(ts, 5s)",
        &res));

    int64_t nwin = 0;
    while (tsdb_result_next(res)) nwin++;
    tsdb_result_free(res);

    int64_t elapsed = now_ns() - t0;

    /* We expect ~1000 sessions (one gap per 10000 rows, 10M/10000 = 1000) */
    printf("  Windows: %lld, elapsed: %.1fms\n",
           (long long)nwin, (double)elapsed / 1e6);
    if (nwin < 500 || nwin > 2000)
        FAIL("expected ~1000 sessions, got %lld", (long long)nwin);
    if (elapsed > 10000000000LL) /* 10 seconds max */
        FAIL("SESSION 10M rows too slow: %.1fms", (double)elapsed / 1e6);

    printf("  OK: ~1000 windows in %.1fms\n", (double)elapsed / 1e6);

    tsdb_close(db);
    rm_rf(dir);
}

/* =========================================================================
 * Test 5: Existing SAMPLE BY still works (regression check)
 * =========================================================================
 */
static void test_sample_by_regression(void) {
    printf("[5] SAMPLE BY regression: existing queries unaffected\n");

    const char *dir = "/tmp/tsdb_adv_regression";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* 30 rows across 3 one-second buckets */
    int64_t S = 1000000000LL;
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int b = 0; b < 3; b++) {
        for (int r = 0; r < 10; r++) {
            OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)((int64_t)b * S + r * 100000000LL)));
            OK(tsdb_batch_row_f64(bk, 1, (double)(b * 10 + r)));
            OK(tsdb_batch_row_end(bk));
        }
    }
    OK(tsdb_batch_commit(bk));

    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT avg(val) FROM t SAMPLE BY 1s",
        &res));

    int nrows = 0;
    double avgs[8];
    while (tsdb_result_next(res)) {
        if (nrows >= 8) FAIL("too many rows");
        avgs[nrows] = tsdb_result_f64(res, 0);
        nrows++;
    }
    tsdb_result_free(res);

    ASSERT(nrows == 3);
    /* bucket 0: vals 0..9 → avg=4.5 */
    if (fabs(avgs[0] - 4.5) > 0.001) FAIL("bucket0 avg: expected 4.5, got %.3f", avgs[0]);
    /* bucket 1: vals 10..19 → avg=14.5 */
    if (fabs(avgs[1] - 14.5) > 0.001) FAIL("bucket1 avg: expected 14.5, got %.3f", avgs[1]);
    /* bucket 2: vals 20..29 → avg=24.5 */
    if (fabs(avgs[2] - 24.5) > 0.001) FAIL("bucket2 avg: expected 24.5, got %.3f", avgs[2]);

    printf("  OK: 3 buckets, avgs=[%.1f, %.1f, %.1f]\n",
           avgs[0], avgs[1], avgs[2]);

    tsdb_close(db);
    rm_rf(dir);
}

/* =========================================================================
 * main
 * =========================================================================
 */
int main(void) {
    printf("=== Advanced Window Tests ===\n");
    test_session_window();
    test_state_window();
    test_event_window();
    test_session_perf();
    test_sample_by_regression();
    printf("=== All tests passed ===\n");
    return 0;
}
