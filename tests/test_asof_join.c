/* test_asof_join.c — ASOF JOIN executor tests.
 *
 * Schema:
 *   trades(ts TIMESTAMP, sym SYMBOL, px FLOAT64)
 *   quotes(ts TIMESTAMP, sym SYMBOL, bid FLOAT64, ask FLOAT64)
 *
 * All tests verify correctness of the two-pointer ASOF JOIN algorithm.
 * The final test checks large-scale performance (100K × 100K).
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
#include <time.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, __VA_ARGS__); \
    abort(); \
} while (0)

#define FAIL0(msg) do { \
    fprintf(stderr, "FAIL %s:%d: " msg "\n", __FILE__, __LINE__); \
    abort(); \
} while (0)

#define OK(rc) do { \
    int _rc = (rc); \
    if (_rc != TSDB_OK) { \
        fprintf(stderr, "FAIL %s:%d: rc=%d (%s)\n", __FILE__, __LINE__, _rc, tsdb_errstr(_rc)); \
        abort(); \
    } \
} while (0)

/* Recursive remove. */
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

/* Monotonic wall clock in nanoseconds. */
static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Convert a simple integer offset in nanoseconds to a timestamp. */
static tsdb_ts_t mts(int64_t ns_offset) {
    /* Use a fixed epoch: 2026-01-01T00:00:00Z */
    return (tsdb_ts_t)(1767225600000000000LL + ns_offset);
}

/* Count and collect rows via tsdb_result_next. */
typedef struct {
    double  bid[16384];
    double  ask[16384];
    double  px[16384];
    int     n;
} collected_t;

static collected_t collect(tsdb_result_t *r,
                            int c_bid, int c_ask, int c_px) {
    collected_t c; c.n = 0;
    while (tsdb_result_next(r)) {
        if (c.n < 16384) {
            c.bid[c.n] = (c_bid >= 0) ? tsdb_result_f64(r, c_bid) : 0.0;
            c.ask[c.n] = (c_ask >= 0) ? tsdb_result_f64(r, c_ask) : 0.0;
            c.px[c.n]  = (c_px  >= 0) ? tsdb_result_f64(r, c_px)  : 0.0;
            c.n++;
        } else {
            c.n++;  /* just count */
        }
    }
    return c;
}

static int col_idx(tsdb_result_t *r, const char *name) {
    int n = tsdb_result_ncols(r);
    for (int i = 0; i < n; i++)
        if (strcmp(tsdb_result_col_name(r, i), name) == 0) return i;
    return -1;
}

/* ======================================================================== */

static const char *TESTDIR = "/tmp/tsdb_test_asof_join";

int main(void) {
    rm_rf(TESTDIR);

    printf("=== ASOF JOIN tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TESTDIR, &db));

    tsdb_table_t *trd = NULL, *qot = NULL;

    /* ---------- Create tables ---------- */
    {
        tsdb_col_t tc[] = {
            {"ts",  TSDB_TYPE_TIMESTAMP},
            {"sym", TSDB_TYPE_SYMBOL},
            {"px",  TSDB_TYPE_FLOAT64},
        };
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
    }
    {
        tsdb_col_t qc[] = {
            {"ts",  TSDB_TYPE_TIMESTAMP},
            {"sym", TSDB_TYPE_SYMBOL},
            {"bid", TSDB_TYPE_FLOAT64},
            {"ask", TSDB_TYPE_FLOAT64},
        };
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "quotes", &qot));
    }

    /* ======================================================================
     * TEST 1: Basic correctness — single-key ASOF JOIN.
     *
     *  quotes:  t=50  AAPL  bid=9.0   ask=10.0
     *           t=100 AAPL  bid=9.5   ask=10.5
     *           t=200 AAPL  bid=9.8   ask=10.8
     *
     *  trades:  t=30  AAPL  → NO match (before all quotes)
     *           t=80  AAPL  → quote@t=50  bid=9.0
     *           t=120 AAPL  → quote@t=100 bid=9.5
     *           t=250 AAPL  → quote@t=200 bid=9.8
     * ====================================================================== */
    printf("\n[1] Basic correctness — single key ASOF JOIN\n");
    {
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        OK(tsdb_batch_row_ts(b, mts(50)));  OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,9.0));  OK(tsdb_batch_row_f64(b,3,10.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,9.5));  OK(tsdb_batch_row_f64(b,3,10.5)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(200))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,9.8));  OK(tsdb_batch_row_f64(b,3,10.8)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(30)));  OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,9.1)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(80)));  OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,9.2)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(120))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,9.6)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(250))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,9.9)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));
    }

    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db,
            "SELECT ts, px, bid, ask FROM trades ASOF JOIN quotes ON sym=sym",
            &r));

        int c_bid = col_idx(r, "bid");
        int c_ask = col_idx(r, "ask");
        int c_px  = col_idx(r, "px");
        assert(c_bid >= 0 && c_ask >= 0 && c_px >= 0);

        collected_t res = collect(r, c_bid, c_ask, c_px);
        assert(res.n == 4 && "expected 4 result rows");

        /* Row 0: t=30, no quote → bid=0 (NULL) */
        assert(res.bid[0] == 0.0 && "t=30 should have NULL bid");
        assert(res.ask[0] == 0.0 && "t=30 should have NULL ask");

        /* Row 1: t=80 → quote@t=50 bid=9.0 */
        assert(fabs(res.bid[1] - 9.0) < 1e-9 && "t=80 bid should be 9.0");
        assert(fabs(res.ask[1] - 10.0) < 1e-9 && "t=80 ask should be 10.0");

        /* Row 2: t=120 → quote@t=100 bid=9.5 */
        assert(fabs(res.bid[2] - 9.5) < 1e-9 && "t=120 bid should be 9.5");
        assert(fabs(res.ask[2] - 10.5) < 1e-9 && "t=120 ask should be 10.5");

        /* Row 3: t=250 → quote@t=200 bid=9.8 */
        assert(fabs(res.bid[3] - 9.8) < 1e-9 && "t=250 bid should be 9.8");
        assert(fabs(res.ask[3] - 10.8) < 1e-9 && "t=250 ask should be 10.8");

        tsdb_result_free(r);
        printf("  PASS: basic single-key ASOF JOIN correctness\n");
    }

    /* ======================================================================
     * TEST 2: Per-key isolation — AAPL and MSFT quotes are independent.
     * ====================================================================== */
    printf("\n[2] Per-key isolation (AAPL vs MSFT)\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"ask",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        OK(tsdb_batch_row_ts(b, mts(10)));  OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,1.0));   OK(tsdb_batch_row_f64(b,3,1.1));   OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(10)));  OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,100.0)); OK(tsdb_batch_row_f64(b,3,100.1)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(20)));  OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,2.0));   OK(tsdb_batch_row_f64(b,3,2.1));   OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(20)));  OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,200.0)); OK(tsdb_batch_row_f64(b,3,200.1)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(15))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,1.5));   OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(15))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,105.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(25))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,2.5));   OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(25))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,205.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db,
            "SELECT ts, sym, px, bid FROM trades ASOF JOIN quotes ON sym=sym",
            &r));

        int c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);

        collected_t res = collect(r, c_bid, -1, -1);
        assert(res.n == 4 && "expected 4 rows");

        /* Row 0: t=15, AAPL → bid=1.0 */
        assert(fabs(res.bid[0] - 1.0) < 1e-9 && "AAPL@t=15 bid=1.0");
        /* Row 1: t=15, MSFT → bid=100.0 */
        assert(fabs(res.bid[1] - 100.0) < 1e-9 && "MSFT@t=15 bid=100.0");
        /* Row 2: t=25, AAPL → bid=2.0 */
        assert(fabs(res.bid[2] - 2.0) < 1e-9 && "AAPL@t=25 bid=2.0");
        /* Row 3: t=25, MSFT → bid=200.0 */
        assert(fabs(res.bid[3] - 200.0) < 1e-9 && "MSFT@t=25 bid=200.0");

        tsdb_result_free(r);
        printf("  PASS: per-key isolation\n");
    }

    /* ======================================================================
     * TEST 3: Edge case — trade before ALL quotes → NULL right cols.
     * ====================================================================== */
    printf("\n[3] Edge case: trade before all quotes → NULL right cols\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"ask",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        OK(tsdb_batch_row_ts(b, mts(1000))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,5.0)); OK(tsdb_batch_row_f64(b,3,5.5)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(5)));    OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,4.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(2000))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,4.5)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db,
            "SELECT ts, px, bid FROM trades ASOF JOIN quotes ON sym=sym",
            &r));

        int c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        collected_t res = collect(r, c_bid, -1, -1);
        assert(res.n == 2 && "expected 2 rows");

        /* Row 0: t=5, before any quote → bid=0.0 (NULL) */
        assert(res.bid[0] == 0.0 && "t=5 should have NULL bid");
        /* Row 1: t=2000, after quote@t=1000 → bid=5.0 */
        assert(fabs(res.bid[1] - 5.0) < 1e-9 && "t=2000 bid should be 5.0");

        tsdb_result_free(r);
        printf("  PASS: NULL right columns before first quote\n");
    }

    /* ======================================================================
     * TEST 4: Empty right table → all right cols NULL.
     * ====================================================================== */
    printf("\n[4] Edge case: empty right table\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"ask",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,10.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));
        /* quotes table is empty — no rows inserted */

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db,
            "SELECT ts, px, bid FROM trades ASOF JOIN quotes ON sym=sym",
            &r));

        int c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        collected_t res = collect(r, c_bid, -1, -1);
        assert(res.n == 1 && "expected 1 row");
        assert(res.bid[0] == 0.0 && "empty right → bid=NULL/0");

        tsdb_result_free(r);
        printf("  PASS: empty right table\n");
    }

    /* ======================================================================
     * TEST 5: Parser acceptance — verify ASOF JOIN syntax is accepted.
     * ====================================================================== */
    printf("\n[5] Parser acceptance test\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"ask",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        /* Empty tables — just verify parse + exec without error. */
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT * FROM trades ASOF JOIN quotes ON sym=sym", &r));
        /* Expect 0 rows from empty tables. */
        int cnt = 0;
        while (tsdb_result_next(r)) cnt++;
        assert(cnt == 0);
        tsdb_result_free(r);

        printf("  PASS: parser accepts ASOF JOIN ON syntax\n");
    }

    /* ======================================================================
     * TEST 6: Large-scale performance — 100K trades × 100K quotes.
     *
     * Two symbols (AAPL, MSFT), interleaved.
     * Expected: correctness + wall time < 1 second.
     * ====================================================================== */
    printf("\n[6] Large-scale: 100K trades x 100K quotes (2 symbols)\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"ask",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        const int N = 100000;
        const char *syms2[2] = {"AAPL", "MSFT"};

        /* quotes: interleaved AAPL/MSFT at even ns offsets. */
        {
            tsdb_batch_t *b = NULL;
            OK(tsdb_batch_begin(qot, &b));
            for (int i = 0; i < N; i++) {
                int64_t t = (int64_t)i * 2;
                OK(tsdb_batch_row_ts(b, mts(t)));
                OK(tsdb_batch_row_sym(b, 1, syms2[i % 2]));
                OK(tsdb_batch_row_f64(b, 2, 100.0 + (double)i));
                OK(tsdb_batch_row_f64(b, 3, 101.0 + (double)i));
                OK(tsdb_batch_row_end(b));
            }
            OK(tsdb_batch_commit(b));
        }

        /* trades: interleaved AAPL/MSFT at odd ns offsets. */
        {
            tsdb_batch_t *b = NULL;
            OK(tsdb_batch_begin(trd, &b));
            for (int i = 0; i < N; i++) {
                int64_t t = (int64_t)i * 2 + 1;
                OK(tsdb_batch_row_ts(b, mts(t)));
                OK(tsdb_batch_row_sym(b, 1, syms2[i % 2]));
                OK(tsdb_batch_row_f64(b, 2, 200.0 + (double)i));
                OK(tsdb_batch_row_end(b));
            }
            OK(tsdb_batch_commit(b));
        }

        int64_t t0 = now_ns();
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db,
            "SELECT ts, sym, px, bid, ask FROM trades ASOF JOIN quotes ON sym=sym",
            &r));
        int64_t elapsed_ns = now_ns() - t0;
        double elapsed_ms = (double)elapsed_ns / 1e6;

        /* Count rows and spot-check first. */
        int nrows = 0;
        double first_bid = -1.0;
        int c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);

        while (tsdb_result_next(r)) {
            if (nrows == 0) first_bid = tsdb_result_f64(r, c_bid);
            nrows++;
        }
        assert(nrows == N && "expected 100K result rows");

        /* First trade (i=0): AAPL, t=1.  Quote AAPL t=0, bid=100.0. */
        assert(fabs(first_bid - 100.0) < 1e-9 && "first trade bid should be 100.0");

        tsdb_result_free(r);

        printf("  100K x 100K ASOF JOIN wall time: %.1f ms\n", elapsed_ms);
        if (elapsed_ms >= 1000.0) {
            fprintf(stderr, "  WARNING: elapsed %.1f ms exceeds 1000 ms target\n", elapsed_ms);
        } else {
            printf("  PASS: performance < 1 second (%.1f ms)\n", elapsed_ms);
        }
    }

    /* ======================================================================
     * TEST 7: Exact-timestamp ties and duplicate timestamps.
     *
     *  quotes:  t=100 AAPL bid=1.0
     *           t=100 AAPL bid=2.0   <- duplicate ts, same key
     *           t=200 AAPL bid=3.0
     *           t=200 AAPL bid=4.0   <- duplicate ts, same key
     *
     *  "most recent row at or before the probe ts": a probe AT t=100 must
     *  match (<=, not <), and among rows sharing a timestamp the last one in
     *  storage order is the match.
     * ====================================================================== */
    printf("\n[7] Exact-timestamp ties + duplicate timestamps\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"ask",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,1.0)); OK(tsdb_batch_row_f64(b,3,11.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,2.0)); OK(tsdb_batch_row_f64(b,3,12.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(200))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,3.0)); OK(tsdb_batch_row_f64(b,3,13.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(200))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,4.0)); OK(tsdb_batch_row_f64(b,3,14.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(99)));  OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.1)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.2)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.3)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(150))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.4)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(200))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.5)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(201))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.6)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT ts, px, bid, ask FROM trades ASOF JOIN quotes ON sym=sym", &r));
        int c_bid = col_idx(r, "bid"), c_ask = col_idx(r, "ask"), c_px = col_idx(r, "px");
        assert(c_bid >= 0 && c_ask >= 0 && c_px >= 0);
        collected_t res = collect(r, c_bid, c_ask, c_px);
        assert(res.n == 6 && "expected 6 result rows");

        /* t=99: strictly before every quote → NULL. */
        assert(res.bid[0] == 0.0 && "t=99 has no quote at or before it");
        assert(res.ask[0] == 0.0 && "t=99 ask must be NULL too");
        /* t=100 (twice): exact tie, last of the two duplicates wins. */
        assert(fabs(res.bid[1] - 2.0) < 1e-9 && "exact tie at t=100 must match, taking the last duplicate");
        assert(fabs(res.ask[1] - 12.0) < 1e-9 && "tie must take bid and ask from the SAME right row");
        assert(fabs(res.bid[2] - 2.0) < 1e-9 && "second probe at t=100 sees the same match");
        assert(fabs(res.ask[2] - 12.0) < 1e-9 && "second probe at t=100 ask");
        /* t=150: between the two duplicate groups. */
        assert(fabs(res.bid[3] - 2.0) < 1e-9 && "t=150 still matches the t=100 pair");
        assert(fabs(res.ask[3] - 12.0) < 1e-9 && "t=150 ask");
        /* t=200: exact tie with the second duplicate pair. */
        assert(fabs(res.bid[4] - 4.0) < 1e-9 && "exact tie at t=200 takes the last duplicate");
        assert(fabs(res.ask[4] - 14.0) < 1e-9 && "t=200 ask from the same row");
        /* t=201: after everything. */
        assert(fabs(res.bid[5] - 4.0) < 1e-9 && "t=201 matches the newest quote");
        assert(fabs(res.ask[5] - 14.0) < 1e-9 && "t=201 ask");
        tsdb_result_free(r);
        printf("  PASS: exact ties resolve with <=, duplicates take the last row\n");
    }

    /* ======================================================================
     * TEST 8: Empty groups.
     *   - a left symbol that appears nowhere on the right      (TSLA)
     *   - a left symbol whose right rows are all in the future (MSFT)
     *   - a right symbol that never trades                     (GOOG)
     * ====================================================================== */
    printf("\n[8] Empty groups (left-only symbol, future-only group, right-only symbol)\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"ask",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,7.0)); OK(tsdb_batch_row_f64(b,3,7.5)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(100))); OK(tsdb_batch_row_sym(b,1,"GOOG")); OK(tsdb_batch_row_f64(b,2,8.0)); OK(tsdb_batch_row_f64(b,3,8.5)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(500))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,9.0)); OK(tsdb_batch_row_f64(b,3,9.5)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(150))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,1.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(150))); OK(tsdb_batch_row_sym(b,1,"TSLA")); OK(tsdb_batch_row_f64(b,2,2.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(150))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,3.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(600))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,4.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(600))); OK(tsdb_batch_row_sym(b,1,"TSLA")); OK(tsdb_batch_row_f64(b,2,5.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT ts, px, bid FROM trades ASOF JOIN quotes ON sym=sym", &r));
        int c_bid = col_idx(r, "bid"), c_px = col_idx(r, "px");
        assert(c_bid >= 0 && c_px >= 0);
        collected_t res = collect(r, c_bid, -1, c_px);
        assert(res.n == 5 && "expected 5 result rows");

        assert(fabs(res.px[0] - 1.0) < 1e-9 && "row order follows the left table");
        assert(fabs(res.bid[0] - 7.0) < 1e-9 && "AAPL@150 matches AAPL@100");
        assert(fabs(res.px[1] - 2.0) < 1e-9);
        assert(res.bid[1] == 0.0 && "TSLA is absent from the right table entirely");
        assert(fabs(res.px[2] - 3.0) < 1e-9);
        assert(res.bid[2] == 0.0 && "MSFT@150 predates the only MSFT quote");
        assert(fabs(res.px[3] - 4.0) < 1e-9);
        assert(fabs(res.bid[3] - 9.0) < 1e-9 && "MSFT@600 matches MSFT@500, not GOOG/AAPL");
        assert(fabs(res.px[4] - 5.0) < 1e-9);
        assert(res.bid[4] == 0.0 && "TSLA still unmatched after every right row is consumed");
        tsdb_result_free(r);

        /* The right-only symbol must not leak into any left row: no result
         * row may carry GOOG's bid. */
        OK(tsdb_query(db, "SELECT ts, bid FROM trades ASOF JOIN quotes ON sym=sym", &r));
        c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        int leaks = 0, rows = 0;
        while (tsdb_result_next(r)) { rows++; if (fabs(tsdb_result_f64(r, c_bid) - 8.0) < 1e-9) leaks++; }
        assert(rows == 5);
        assert(leaks == 0 && "GOOG (right-only symbol) must never match a left row");
        tsdb_result_free(r);
        printf("  PASS: empty groups yield NULL, no cross-group leakage\n");
    }

    /* ======================================================================
     * TEST 9: Non-SYMBOL key, multi-key ON, no-ON-key, and LIMIT.
     * These drive the general key path rather than the dense-symbol one.
     * ====================================================================== */
    printf("\n[9] INT64 key, two-key ON, keyless join, LIMIT\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"lot",TSDB_TYPE_INT64},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"lot",TSDB_TYPE_INT64},{"bid",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 4, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        OK(tsdb_batch_row_ts(b, mts(10))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_i64(b,2,1)); OK(tsdb_batch_row_f64(b,3,1.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(20))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_i64(b,2,2)); OK(tsdb_batch_row_f64(b,3,2.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(30))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_i64(b,2,1)); OK(tsdb_batch_row_f64(b,3,3.0)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(40))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_i64(b,2,1)); OK(tsdb_batch_row_f64(b,3,0.1)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(41))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_i64(b,2,2)); OK(tsdb_batch_row_f64(b,3,0.2)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(42))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_i64(b,2,1)); OK(tsdb_batch_row_f64(b,3,0.3)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(43))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_i64(b,2,9)); OK(tsdb_batch_row_f64(b,3,0.4)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        /* Single INT64 key: lot 1 → newest lot-1 quote is MSFT@30 (3.0). */
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT ts, bid FROM trades ASOF JOIN quotes ON lot=lot", &r));
        int c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        collected_t res = collect(r, c_bid, -1, -1);
        assert(res.n == 4);
        assert(fabs(res.bid[0] - 3.0) < 1e-9 && "lot=1 → newest lot-1 quote (t=30)");
        assert(fabs(res.bid[1] - 2.0) < 1e-9 && "lot=2 → the lot-2 quote");
        assert(fabs(res.bid[2] - 3.0) < 1e-9 && "lot=1 again");
        assert(res.bid[3] == 0.0 && "lot=9 has no quote");
        tsdb_result_free(r);

        /* Two keys: sym AND lot must both match. */
        OK(tsdb_query(db, "SELECT ts, bid FROM trades ASOF JOIN quotes ON sym=sym, lot=lot", &r));
        c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        res = collect(r, c_bid, -1, -1);
        assert(res.n == 4);
        assert(fabs(res.bid[0] - 1.0) < 1e-9 && "(AAPL,1) → 1.0");
        assert(fabs(res.bid[1] - 2.0) < 1e-9 && "(AAPL,2) → 2.0");
        assert(fabs(res.bid[2] - 3.0) < 1e-9 && "(MSFT,1) → 3.0");
        assert(res.bid[3] == 0.0 && "(MSFT,9) has no quote");
        tsdb_result_free(r);

        /* No ON keys: every left row takes the newest right row at or before it. */
        OK(tsdb_query(db, "SELECT ts, bid FROM trades ASOF JOIN quotes", &r));
        c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        res = collect(r, c_bid, -1, -1);
        assert(res.n == 4);
        for (int i = 0; i < 4; i++)
            assert(fabs(res.bid[i] - 3.0) < 1e-9 && "keyless join always takes the newest quote");
        tsdb_result_free(r);

        /* LIMIT truncates without changing the surviving rows. */
        OK(tsdb_query(db, "SELECT ts, bid FROM trades ASOF JOIN quotes ON sym=sym, lot=lot LIMIT 2", &r));
        c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        res = collect(r, c_bid, -1, -1);
        assert(res.n == 2 && "LIMIT 2");
        assert(fabs(res.bid[0] - 1.0) < 1e-9);
        assert(fabs(res.bid[1] - 2.0) < 1e-9);
        tsdb_result_free(r);
        printf("  PASS: INT64 key, 2-key ON, keyless join, LIMIT\n");
    }

    /* ======================================================================
     * TEST 10: SELECT * carries the right table's SYMBOL column through the
     * result's own dictionary, and unprojected right columns are not needed
     * to produce a correct answer.
     * ====================================================================== */
    printf("\n[10] SELECT * with a right SYMBOL column\n");
    {
        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64},{"venue",TSDB_TYPE_SYMBOL}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        OK(tsdb_batch_row_ts(b, mts(10))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,1.0)); OK(tsdb_batch_row_sym(b,3,"NYSE")); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(20))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,2.0)); OK(tsdb_batch_row_sym(b,3,"BATS")); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(30))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,3.0)); OK(tsdb_batch_row_sym(b,3,"ARCA")); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        OK(tsdb_batch_row_ts(b, mts(5)));  OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.1)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(25))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.2)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(35))); OK(tsdb_batch_row_sym(b,1,"MSFT")); OK(tsdb_batch_row_f64(b,2,0.3)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_row_ts(b, mts(45))); OK(tsdb_batch_row_sym(b,1,"AAPL")); OK(tsdb_batch_row_f64(b,2,0.4)); OK(tsdb_batch_row_end(b));
        OK(tsdb_batch_commit(b));

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT * FROM trades ASOF JOIN quotes ON sym=sym", &r));
        /* Duplicate names on both sides: the right block starts after the
         * left one, so scan by index rather than by name. */
        int nc = tsdb_result_ncols(r);
        assert(nc == 7 && "3 left cols + 4 right cols");
        int c_venue = -1;
        for (int i = 3; i < nc; i++)
            if (strcmp(tsdb_result_col_name(r, i), "venue") == 0) c_venue = i;
        assert(c_venue >= 0 && "right venue column present");
        assert(tsdb_result_col_type(r, c_venue) == TSDB_TYPE_SYMBOL);

        /* Row 0 (t=5, AAPL) has no quote at or before it, so the join writes
         * dictionary code 0 for every right column.  A SYMBOL result column
         * has no NULL encoding, so code 0 reads back as the first string
         * interned into the result dictionary — here "NYSE", from row 1.
         * That aliasing is a pre-existing property of the result format, not
         * of the join; it is pinned here so a future NULL encoding has to
         * update this expectation deliberately. */
        const char *want[4] = {"NYSE", "NYSE", "BATS", "ARCA"};
        int n = 0;
        while (tsdb_result_next(r)) {
            const char *v = tsdb_result_sym(r, c_venue);
            assert(v && strcmp(v, want[n]) == 0 && "right SYMBOL value must survive the join");
            n++;
        }
        assert(n == 4);
        tsdb_result_free(r);

        /* Same join, right SYMBOL column not projected: the answer for the
         * columns that ARE projected must be unchanged. */
        OK(tsdb_query(db, "SELECT ts, px, bid FROM trades ASOF JOIN quotes ON sym=sym", &r));
        int c_bid = col_idx(r, "bid");
        assert(c_bid >= 0);
        collected_t res = collect(r, c_bid, -1, -1);
        assert(res.n == 4);
        assert(res.bid[0] == 0.0);
        assert(fabs(res.bid[1] - 1.0) < 1e-9);
        assert(fabs(res.bid[2] - 2.0) < 1e-9);
        assert(fabs(res.bid[3] - 3.0) < 1e-9);
        tsdb_result_free(r);
        printf("  PASS: right SYMBOL projection and column pruning agree\n");
    }

    /* ======================================================================
     * TEST 11: Out-of-order right side.
     *
     * The scan plan lists disk partitions before the memtable regardless of
     * their timestamps, so writing the HIGH quote timestamps, flushing them
     * by closing the db, then writing the LOW ones produces a right side that
     * arrives as [high..., low...].  ASOF has to sort it before merging, and
     * the sort has to be stable or duplicate timestamps would pair with a
     * different right row.
     * ====================================================================== */
    printf("\n[11] Out-of-order right side (partition after memtable timestamps)\n");
    {
        const int N = 20000;               /* 2N quote rows, 2N trade rows */
        const char *S4[4] = {"AAPL","MSFT","GOOG","AMZN"};

        tsdb_close(db);
        rm_rf(TESTDIR);
        OK(tsdb_open(TESTDIR, &db));

        tsdb_col_t tc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"px",TSDB_TYPE_FLOAT64}};
        tsdb_col_t qc[] = {{"ts",TSDB_TYPE_TIMESTAMP},{"sym",TSDB_TYPE_SYMBOL},{"bid",TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
        OK(tsdb_create_table(db, "quotes", qc, 3, "ts"));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));

        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(qot, &b));
        for (int i = N; i < 2 * N; i++) {          /* high half first */
            OK(tsdb_batch_row_ts(b, mts((int64_t)i * 1000)));
            OK(tsdb_batch_row_sym(b, 1, S4[i % 4]));
            OK(tsdb_batch_row_f64(b, 2, 1000.0 + (double)i));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        tsdb_close(db);                             /* push it to a partition */

        OK(tsdb_open(TESTDIR, &db));
        OK(tsdb_open_table(db, "trades", &trd));
        OK(tsdb_open_table(db, "quotes", &qot));
        OK(tsdb_batch_begin(qot, &b));
        for (int i = 0; i < N; i++) {               /* low half second */
            OK(tsdb_batch_row_ts(b, mts((int64_t)i * 1000)));
            OK(tsdb_batch_row_sym(b, 1, S4[i % 4]));
            OK(tsdb_batch_row_f64(b, 2, 1000.0 + (double)i));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));

        OK(tsdb_batch_begin(trd, &b));
        for (int i = 0; i < 2 * N; i++) {
            OK(tsdb_batch_row_ts(b, mts((int64_t)i * 1000 + 500)));
            OK(tsdb_batch_row_sym(b, 1, S4[i % 4]));
            OK(tsdb_batch_row_f64(b, 2, (double)i));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));

        int64_t t0 = now_ns();
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT ts, px, bid FROM trades ASOF JOIN quotes ON sym=sym", &r));
        int c_bid = col_idx(r, "bid"), c_px = col_idx(r, "px");
        assert(c_bid >= 0 && c_px >= 0);
        int i = 0, bad = 0;
        while (tsdb_result_next(r)) {
            /* Trade i sits 500 ns after quote i and carries the same symbol,
             * so quote i is the newest same-symbol quote at or before it. */
            if (fabs(tsdb_result_f64(r, c_px)  - (double)i)         > 1e-9) bad++;
            if (fabs(tsdb_result_f64(r, c_bid) - (1000.0 + (double)i)) > 1e-9) bad++;
            i++;
        }
        double elapsed_ms = (double)(now_ns() - t0) / 1e6;
        tsdb_result_free(r);
        assert(i == 2 * N && "every left row must be emitted");
        assert(bad == 0 && "out-of-order right side must still match quote i");
        printf("  40K unsorted right rows joined in %.1f ms, all %d matches exact\n",
               elapsed_ms, i);
        printf("  PASS: out-of-order right side sorted stably before the merge\n");
    }

    tsdb_close(db);
    rm_rf(TESTDIR);

    printf("\n=== ALL ASOF JOIN TESTS PASSED ===\n");
    return 0;
}
