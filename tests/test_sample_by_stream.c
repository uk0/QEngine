/* test_sample_by_stream.c — Streaming SAMPLE BY + LIMIT pushdown tests.
 *
 * Verifies:
 *  1. Correctness: SAMPLE BY + LIMIT returns exactly N buckets in order.
 *  2. Cross-source merge: a bucket straddling two partitions is aggregated
 *     correctly — no double-emit, no lost rows.
 *  3. Performance: 1M rows across 1000 buckets, LIMIT 10 completes in < 50ms.
 *  4. Edge cases: LIMIT > total buckets, zero matching rows.
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

/* Monotonic wall clock in nanoseconds. */
static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ---- Test 1: basic LIMIT correctness ----------------------------------- */
static void test_limit_correctness(void) {
    printf("[1] SAMPLE BY + LIMIT returns exactly N buckets in ascending order\n");

    const char *dir = "/tmp/tsdb_sbstream_t1";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"value", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* Write 100 buckets of 10 rows each (1s bucket = 1e9 ns). */
    int64_t bucket_ns = 1000000000LL;  /* 1 second */
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int b = 0; b < 100; b++) {
        for (int r = 0; r < 10; r++) {
            int64_t ts = (int64_t)b * bucket_ns + r * 100000000LL;  /* 0.1s apart */
            OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)ts));
            OK(tsdb_batch_row_f64(bk, 1, (double)(b * 10 + r)));
            OK(tsdb_batch_row_end(bk));
        }
    }
    OK(tsdb_batch_commit(bk));

    /* Query: LIMIT 7 buckets */
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), sum(value) FROM t "
        "SAMPLE BY 1s LIMIT 7", &res));

    int n = 0;
    int64_t prev_ts = INT64_MIN;
    double expected_sum[] = {45.0, 145.0, 245.0, 345.0, 445.0, 545.0, 645.0};
    /* bucket b: values b*10, b*10+1, ..., b*10+9 → sum = b*100 + 45 */
    while (tsdb_result_next(res)) {
        tsdb_ts_t bkt = tsdb_result_ts(res, 0);
        double    s   = tsdb_result_f64(res, 1);
        ASSERT(bkt > prev_ts);  /* ascending order */
        ASSERT(n < 7);
        double exp = expected_sum[n];
        if (fabs(s - exp) > 0.5)
            FAIL("bucket %d: sum=%.1f expected=%.1f", n, s, exp);
        prev_ts = (int64_t)bkt;
        n++;
    }
    ASSERT(n == 7);
    tsdb_result_free(res);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed (7 buckets, correct sums, ascending order)\n");
}

/* ---- Test 2: cross-source merge ---------------------------------------- */
/* A bucket spanning two partitions must yield the same aggregate as the
 * full-data result — no double-emit, no lost rows. */
static void test_cross_partition_merge(void) {
    printf("[2] Cross-partition bucket merge: same aggregate as full-scan\n");

    const char *dir = "/tmp/tsdb_sbstream_t2";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"value", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* Write rows that straddle partition boundaries.
     * Use a 2-second bucket (2e9 ns). Write rows in two separate commits so
     * the storage may split across scan sources, then write rows on both the
     * first and second second such that they share a 2s bucket.
     *
     * Rows:
     *   commit 1: ts = 0..999 ms  (in bucket 0)
     *   commit 2: ts = 1000..1999 ms  (same 2s bucket 0: 0..1999 ms)
     *
     * Both commits fall within the same 2-second window [0, 2e9).
     */
    int64_t bucket_ns2 = 2000000000LL;  /* 2 seconds */

    double expected_sum = 0;
    tsdb_batch_t *bk = NULL;

    /* First batch: rows at 0..999 ms (1000 rows) */
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 0; i < 1000; i++) {
        int64_t ts = (int64_t)i * 1000000LL;  /* 0..999 ms in ns */
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)ts));
        OK(tsdb_batch_row_f64(bk, 1, (double)i));
        OK(tsdb_batch_row_end(bk));
        expected_sum += (double)i;
    }
    OK(tsdb_batch_commit(bk));

    /* Second batch: rows at 1000..1999 ms (same 2s bucket) */
    OK(tsdb_batch_begin(tbl, &bk));
    for (int i = 1000; i < 2000; i++) {
        int64_t ts = (int64_t)i * 1000000LL;  /* 1000..1999 ms in ns */
        OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)ts));
        OK(tsdb_batch_row_f64(bk, 1, (double)i));
        OK(tsdb_batch_row_end(bk));
        expected_sum += (double)i;
    }
    OK(tsdb_batch_commit(bk));
    (void)bucket_ns2;

    /* Query with a 2-second bucket — all 2000 rows should merge into 1 bucket. */
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 2000000000), sum(value) FROM t "
        "SAMPLE BY 2s", &res));

    int n = 0;
    double got_sum = 0;
    while (tsdb_result_next(res)) {
        got_sum = tsdb_result_f64(res, 1);
        n++;
    }
    if (n != 1)
        FAIL("expected 1 merged bucket, got %d (expected_sum=%.0f)", n, expected_sum);
    if (fabs(got_sum - expected_sum) > 0.5)
        FAIL("cross-source sum: got=%.1f expected=%.1f", got_sum, expected_sum);

    tsdb_result_free(res);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed (1 merged bucket, sum=%.0f correct)\n", got_sum);
}

/* ---- Test 3: performance ------------------------------------------------ */
static void test_performance(void) {
    printf("[3] Performance: 1M rows, 1000 buckets, LIMIT 10 < 50ms\n");

    const char *dir = "/tmp/tsdb_sbstream_t3";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"value", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* 1M rows across 1000 buckets (1000 rows per bucket).
     * bucket = 1 second, rows 1ms apart within a bucket. */
    int64_t bucket_ns  = 1000000000LL;   /* 1 second */
    int64_t row_step   =   1000000LL;    /* 1 ms */
    int     nbuckets   = 1000;
    int     rows_per_b = 1000;
    int     total_rows = nbuckets * rows_per_b;

    /* Write in chunks to avoid excessive batch memory. */
    int chunk = 10000;
    double row_val = 1.0;
    tsdb_batch_t *bk = NULL;
    int written = 0;
    for (int b = 0; b < nbuckets; b++) {
        if (written == 0 || written % chunk == 0) {
            if (bk) { OK(tsdb_batch_commit(bk)); bk = NULL; }
            OK(tsdb_batch_begin(tbl, &bk));
        }
        for (int r = 0; r < rows_per_b; r++) {
            int64_t ts = (int64_t)b * bucket_ns + (int64_t)r * row_step;
            OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)ts));
            OK(tsdb_batch_row_f64(bk, 1, row_val));
            OK(tsdb_batch_row_end(bk));
            written++;
        }
    }
    if (bk) { OK(tsdb_batch_commit(bk)); }
    printf("  ingested %d rows\n", total_rows);

    /* Warm up: run one query to page in data. */
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), sum(value) FROM t "
        "SAMPLE BY 1s LIMIT 10", &res));
    tsdb_result_free(res);

    /* Timed run (median of 5). */
    int64_t times[5];
    for (int run = 0; run < 5; run++) {
        int64_t t0 = now_ns();
        OK(tsdb_query(db,
            "SELECT time_bucket(ts, 1000000000), sum(value) FROM t "
            "SAMPLE BY 1s LIMIT 10", &res));
        int64_t t1 = now_ns();

        int cnt = 0;
        while (tsdb_result_next(res)) cnt++;
        ASSERT(cnt == 10);
        tsdb_result_free(res);
        times[run] = t1 - t0;
    }

    /* Sort to find median. */
    for (int i = 0; i < 4; i++)
        for (int j = i + 1; j < 5; j++)
            if (times[j] < times[i]) { int64_t tmp = times[i]; times[i] = times[j]; times[j] = tmp; }
    int64_t median_ns = times[2];
    double  median_ms = (double)median_ns / 1e6;

    printf("  latency (median of 5): %.2f ms\n", median_ms);
    printf("  latency range: %.2f–%.2f ms\n",
           (double)times[0] / 1e6, (double)times[4] / 1e6);

    if (median_ms >= 50.0)
        FAIL("p50 latency %.2f ms exceeds 50ms threshold", median_ms);

    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

/* ---- Test 4: LIMIT > total buckets (no early exit, flush last) ---------- */
static void test_limit_larger_than_buckets(void) {
    printf("[4] LIMIT > total buckets: all buckets returned, no truncation\n");

    const char *dir = "/tmp/tsdb_sbstream_t4";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"value", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* 5 buckets, 3 rows each. */
    int64_t bucket_ns = 1000000000LL;
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int b = 0; b < 5; b++) {
        for (int r = 0; r < 3; r++) {
            int64_t ts = (int64_t)b * bucket_ns + (int64_t)r * 100000000LL;
            OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)ts));
            OK(tsdb_batch_row_f64(bk, 1, 1.0));
            OK(tsdb_batch_row_end(bk));
        }
    }
    OK(tsdb_batch_commit(bk));

    /* LIMIT 100 >> 5 actual buckets. */
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), count(*) FROM t "
        "SAMPLE BY 1s LIMIT 100", &res));

    int n = 0;
    while (tsdb_result_next(res)) {
        int64_t cnt = tsdb_result_i64(res, 1);
        if (cnt != 3) FAIL("bucket %d count=%lld expected 3", n, (long long)cnt);
        n++;
    }
    if (n != 5)
        FAIL("expected 5 buckets, got %d", n);

    tsdb_result_free(res);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed (5 buckets returned for LIMIT 100)\n");
}

/* ---- Test 5: zero matching rows ---------------------------------------- */
static void test_zero_rows(void) {
    printf("[5] Zero matching rows: result is empty\n");

    const char *dir = "/tmp/tsdb_sbstream_t5";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"value", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    /* Empty table — no rows written. */

    tsdb_result_t *res = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), sum(value) FROM t "
        "SAMPLE BY 1s LIMIT 10", &res));

    int n = 0;
    while (tsdb_result_next(res)) n++;
    if (n != 0)
        FAIL("expected 0 rows from empty table, got %d", n);

    tsdb_result_free(res);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed\n");
}

/* ---- Test 6: correctness cross-check — LIMIT 10 matches full-scan ------ */
static void test_limit_correctness_vs_full(void) {
    printf("[6] LIMIT 10 results match first 10 buckets of full-scan\n");

    const char *dir = "/tmp/tsdb_sbstream_t6";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"value", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "t", &tbl));

    /* 30 buckets of 5 rows each. */
    int64_t bucket_ns = 1000000000LL;
    tsdb_batch_t *bk = NULL;
    OK(tsdb_batch_begin(tbl, &bk));
    for (int b = 0; b < 30; b++) {
        for (int r = 0; r < 5; r++) {
            int64_t ts = (int64_t)b * bucket_ns + (int64_t)r * 100000000LL;
            OK(tsdb_batch_row_ts(bk, (tsdb_ts_t)ts));
            OK(tsdb_batch_row_f64(bk, 1, (double)(b * 5 + r + 1)));
            OK(tsdb_batch_row_end(bk));
        }
    }
    OK(tsdb_batch_commit(bk));

    /* Full-scan: collect first 10 bucket sums. */
    tsdb_result_t *full = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), sum(value) FROM t "
        "SAMPLE BY 1s", &full));
    double full_sums[10];
    tsdb_ts_t full_bkts[10];
    int fi = 0;
    while (tsdb_result_next(full) && fi < 10) {
        full_bkts[fi] = tsdb_result_ts(full, 0);
        full_sums[fi] = tsdb_result_f64(full, 1);
        fi++;
    }
    tsdb_result_free(full);
    ASSERT(fi == 10);

    /* LIMIT 10 scan. */
    tsdb_result_t *lim = NULL;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 1000000000), sum(value) FROM t "
        "SAMPLE BY 1s LIMIT 10", &lim));
    int li = 0;
    while (tsdb_result_next(lim)) {
        tsdb_ts_t bkt = tsdb_result_ts(lim, 0);
        double    s   = tsdb_result_f64(lim, 1);
        if (li >= 10) FAIL("LIMIT 10 returned more than 10 rows");
        if (bkt != full_bkts[li])
            FAIL("bucket %d: ts mismatch: LIMIT=%lld full=%lld",
                 li, (long long)bkt, (long long)full_bkts[li]);
        if (fabs(s - full_sums[li]) > 0.5)
            FAIL("bucket %d: sum mismatch: LIMIT=%.1f full=%.1f", li, s, full_sums[li]);
        li++;
    }
    if (li != 10)
        FAIL("LIMIT 10 returned %d rows", li);
    tsdb_result_free(lim);
    tsdb_close(db);
    rm_rf(dir);
    printf("  passed (first 10 bucket sums match full-scan)\n");
}

int main(void) {
    printf("=== test_sample_by_stream ===\n\n");

    test_limit_correctness();
    test_cross_partition_merge();
    test_performance();
    test_limit_larger_than_buckets();
    test_zero_rows();
    test_limit_correctness_vs_full();

    printf("\n=== 6 passed, 0 failed ===\n");
    return 0;
}
