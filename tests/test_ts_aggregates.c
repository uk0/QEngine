/* test_ts_aggregates.c — Tests for TDengine-style TS aggregates:
 *   first(col), last(col), last_row(col), twa(col)
 *
 * Test cases:
 *   1. first/last on 100-row table with ascending ts + val
 *   2. twa on uniform ts + linear val (step-wise-backward convention)
 *   3. twa on non-uniform ts — hand-computed value
 *   4. WHERE filter + last()
 *   5. SELECT first(val), last(val), twa(val) — 3 cols in one query
 *   6. last_row(val) behaves like last(val) (MVP)
 *   7. Performance: 1M rows first/last/twa latency
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
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); \
} while (0)

#define OK(rc) do { \
    int _rc = (rc); \
    if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); \
} while (0)

#define CHECK(cond, msg) do { \
    if (cond) { g_pass++; printf("  PASS  %s\n", (msg)); } \
    else { g_fail++; fprintf(stderr, "  FAIL  %s\n", (msg)); } \
} while (0)

#define CHECKF(cond, fmt, ...) do { \
    if (cond) { g_pass++; } \
    else { g_fail++; fprintf(stderr, "  FAIL  " fmt "\n", ##__VA_ARGS__); } \
} while (0)

static int g_pass = 0, g_fail = 0;

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

static tsdb_ts_t base_ts(int64_t offset_ns) {
    return tsdb_parse_ts("2026-01-01 00:00:00") + offset_ns;
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_ts_agg";
    rm_rf(dir);

    printf("=== tsdb TS-aggregate tests ===\n\n");

    /* ----------------------------------------------------------------
     * Setup: table t1 with 100 rows: ts evenly spaced 1s, val=1..100
     * ---------------------------------------------------------------- */
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t1", cols, 2, "ts"));

    tsdb_table_t *t1h = NULL;
    OK(tsdb_open_table(db, "t1", &t1h));

    {
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t1h, &b));
        for (int i = 0; i < 100; i++) {
            OK(tsdb_batch_row_ts(b,  base_ts((int64_t)i * 1000000000LL)));
            OK(tsdb_batch_row_f64(b, 1, (double)(i + 1)));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }

    /* ----------------------------------------------------------------
     * Test 1: first(val) / last(val) — basic correctness
     * ---------------------------------------------------------------- */
    printf("[1] first(val), last(val) on 100-row ascending table\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT first(val), last(val) FROM t1", &r));
        CHECK(tsdb_result_ncols(r) == 2, "ncols==2");
        int rows = 0;
        double fv = 0.0, lv = 0.0;
        while (tsdb_result_next(r)) {
            fv = tsdb_result_f64(r, 0);
            lv = tsdb_result_f64(r, 1);
            rows++;
        }
        CHECK(rows == 1, "nrows==1");
        CHECKF(fv == 1.0,   "first(val)=1.0   got %.2f", fv);
        CHECKF(lv == 100.0, "last(val)=100.0  got %.2f", lv);
        tsdb_result_free(r);
    }

    /* ----------------------------------------------------------------
     * Test 2: twa(val) uniform ts + linear val
     *   step-wise-backward: for N=100 points 1s apart, val=1..100:
     *   twa = sum(v[i] * dt[i+1]) / total_dt
     *       = (1+2+...+99)*1 / 99  = 4950/99 = 50.0
     * ---------------------------------------------------------------- */
    printf("[2] twa(val) uniform ts, linear val (step-wise-backward)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT twa(val) FROM t1", &r));
        double tv = 0.0;
        while (tsdb_result_next(r)) tv = tsdb_result_f64(r, 0);
        tsdb_result_free(r);
        CHECKF(fabs(tv - 50.0) < 0.01,
               "twa(val)~50.0 (step-wise-backward) got %.4f", tv);
    }

    /* ----------------------------------------------------------------
     * Test 3: twa non-uniform ts hand-computed
     *   Points: (ts=0, v=0), (ts=100, v=10), (ts=1000, v=100)
     *   step-wise-backward:
     *     point 0 (v=0) weights gap to next = 100: 0*100 = 0
     *     point 1 (v=10) weights gap to next = 900: 10*900 = 9000
     *     point 2 (v=100) last point, weight 0
     *     twa = 9000 / 1000 = 9.0
     *   avg = (0+10+100)/3 = 36.67 — significantly different
     * ---------------------------------------------------------------- */
    printf("[3] twa non-uniform ts (hand-computed: 9.0)\n");
    {
        const char *dir2 = "/tmp/tsdb_test_ts_agg_t2";
        rm_rf(dir2);
        tsdb_db_t *db2 = NULL;
        OK(tsdb_open(dir2, &db2));
        tsdb_col_t c2[] = {{"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(db2, "t2", c2, 2, "ts"));
        tsdb_table_t *th2 = NULL;
        OK(tsdb_open_table(db2, "t2", &th2));

        {
            tsdb_batch_t *b2 = NULL;
            OK(tsdb_batch_begin(th2, &b2));
            int64_t ts0 = base_ts(0);
            OK(tsdb_batch_row_ts(b2, ts0 + 0));    OK(tsdb_batch_row_f64(b2, 1, 0.0));   OK(tsdb_batch_row_end(b2));
            OK(tsdb_batch_row_ts(b2, ts0 + 100));  OK(tsdb_batch_row_f64(b2, 1, 10.0));  OK(tsdb_batch_row_end(b2));
            OK(tsdb_batch_row_ts(b2, ts0 + 1000)); OK(tsdb_batch_row_f64(b2, 1, 100.0)); OK(tsdb_batch_row_end(b2));
            OK(tsdb_batch_commit(b2));
        }

        tsdb_result_t *r = NULL;
        OK(tsdb_query(db2, "SELECT twa(v) FROM t2", &r));
        double tv = 0.0;
        while (tsdb_result_next(r)) tv = tsdb_result_f64(r, 0);
        tsdb_result_free(r);
        CHECKF(fabs(tv - 9.0) < 0.01,
               "twa non-uniform=9.0 (step-wise-backward) got %.4f", tv);

        /* avg = 36.67, markedly different */
        OK(tsdb_query(db2, "SELECT avg(v) FROM t2", &r));
        double av = 0.0;
        while (tsdb_result_next(r)) av = tsdb_result_f64(r, 0);
        tsdb_result_free(r);
        CHECKF(fabs(av - 36.666) < 0.01, "avg=36.67 got %.4f", av);
        CHECKF(fabs(tv - av) > 20.0,
               "twa != avg (non-uniform): tv=%.2f av=%.2f", tv, av);

        tsdb_close(db2);
        rm_rf(dir2);
    }

    /* ----------------------------------------------------------------
     * Test 4: WHERE + last(val)
     *   WHERE val < 50.5  (selects val 1..50)  →  last = 50.0
     * ---------------------------------------------------------------- */
    printf("[4] WHERE val < 50.5 + last(val)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT last(val) FROM t1 WHERE val < 50.5", &r));
        double lv = 0.0;
        while (tsdb_result_next(r)) lv = tsdb_result_f64(r, 0);
        tsdb_result_free(r);
        CHECKF(lv == 50.0, "last(val) WHERE val<50.5 = 50.0, got %.2f", lv);
    }

    /* ----------------------------------------------------------------
     * Test 5: SELECT first(val), last(val), twa(val) — 3 columns
     * ---------------------------------------------------------------- */
    printf("[5] SELECT first(val), last(val), twa(val) — 3 output cols\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT first(val), last(val), twa(val) FROM t1", &r));
        CHECK(tsdb_result_ncols(r) == 3, "ncols==3");
        int rows = 0;
        double fv = 0.0, lv = 0.0, tv = 0.0;
        while (tsdb_result_next(r)) {
            fv = tsdb_result_f64(r, 0);
            lv = tsdb_result_f64(r, 1);
            tv = tsdb_result_f64(r, 2);
            rows++;
        }
        CHECK(rows == 1, "nrows==1");
        CHECKF(fv == 1.0,   "first(val)=1.0   got %.2f", fv);
        CHECKF(lv == 100.0, "last(val)=100.0  got %.2f", lv);
        CHECKF(fabs(tv - 50.0) < 0.01, "twa(val)~50.0    got %.4f", tv);
        tsdb_result_free(r);
    }

    /* ----------------------------------------------------------------
     * Test 6: last_row(val) == last(val) (MVP)
     * ---------------------------------------------------------------- */
    printf("[6] last_row(val) == last(val)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT last_row(val) FROM t1", &r));
        double lrv = 0.0;
        while (tsdb_result_next(r)) lrv = tsdb_result_f64(r, 0);
        tsdb_result_free(r);
        CHECKF(lrv == 100.0, "last_row(val)=100.0 got %.2f", lrv);
    }

    /* ----------------------------------------------------------------
     * Test 7: Performance — 1M rows, first/last/twa latency
     * ---------------------------------------------------------------- */
    printf("[7] Performance: 1M rows first/last/twa\n");
    {
        const char *pdir = "/tmp/tsdb_test_ts_agg_perf";
        rm_rf(pdir);
        tsdb_db_t *pdb = NULL;
        OK(tsdb_open(pdir, &pdb));
        tsdb_col_t pc[] = {{"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64}};
        OK(tsdb_create_table(pdb, "perf", pc, 2, "ts"));
        tsdb_table_t *pt = NULL;
        OK(tsdb_open_table(pdb, "perf", &pt));

        int N = 1000000;
        {
            tsdb_batch_t *pb = NULL;
            OK(tsdb_batch_begin(pt, &pb));
            for (int i = 0; i < N; i++) {
                OK(tsdb_batch_row_ts(pb,  base_ts((int64_t)i * 1000000LL)));
                OK(tsdb_batch_row_f64(pb, 1, (double)(i + 1)));
                OK(tsdb_batch_row_end(pb));
            }
            OK(tsdb_batch_commit(pb));
        }

        struct timespec t0, t1;
        tsdb_result_t *r = NULL;
        double dummy = 0.0;

        /* first */
        clock_gettime(CLOCK_MONOTONIC, &t0);
        OK(tsdb_query(pdb, "SELECT first(v) FROM perf", &r));
        clock_gettime(CLOCK_MONOTONIC, &t1);
        while (tsdb_result_next(r)) dummy = tsdb_result_f64(r, 0);
        double ms_first = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                          (t1.tv_nsec - t0.tv_nsec) / 1e6;
        CHECKF(dummy == 1.0, "first(v) perf=1.0 got %.1f", dummy);
        tsdb_result_free(r);
        printf("    first(v) on 1M rows: %.2f ms\n", ms_first);

        /* last */
        clock_gettime(CLOCK_MONOTONIC, &t0);
        OK(tsdb_query(pdb, "SELECT last(v) FROM perf", &r));
        clock_gettime(CLOCK_MONOTONIC, &t1);
        while (tsdb_result_next(r)) dummy = tsdb_result_f64(r, 0);
        double ms_last = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                         (t1.tv_nsec - t0.tv_nsec) / 1e6;
        CHECKF(dummy == (double)N, "last(v) perf=%d got %.1f", N, dummy);
        tsdb_result_free(r);
        printf("    last(v)  on 1M rows: %.2f ms\n", ms_last);

        /* twa */
        clock_gettime(CLOCK_MONOTONIC, &t0);
        OK(tsdb_query(pdb, "SELECT twa(v) FROM perf", &r));
        clock_gettime(CLOCK_MONOTONIC, &t1);
        while (tsdb_result_next(r)) dummy = tsdb_result_f64(r, 0);
        double ms_twa = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                        (t1.tv_nsec - t0.tv_nsec) / 1e6;
        (void)dummy;
        tsdb_result_free(r);
        printf("    twa(v)   on 1M rows: %.2f ms\n", ms_twa);

        tsdb_close(pdb);
        rm_rf(pdir);
    }

    /* ----------------------------------------------------------------
     * Summary
     * ---------------------------------------------------------------- */
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    tsdb_close(db);
    rm_rf(dir);
    return g_fail ? 1 : 0;
}
