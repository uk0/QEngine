/* test_parallel.c — parallel query execution tests.
 *
 * Tests:
 *  1. Insert 1M rows, verify serial count == parallel count.
 *  2. Verify serial sum == parallel sum (within 1e-9 relative tolerance).
 *  3. 4-thread vs 1-thread sum throughput comparison.
 *  4. Stress: 200 threads each issuing 50 queries = 10000 total concurrent
 *     tsdb_query() calls against the same db; verify correctness.
 */
#include "tsdb.h"
#include "../src/query/exec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)

/* ---- utilities ---------------------------------------------------------- */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

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

/* ---- shared db handle for stress test ----------------------------------- */

typedef struct {
    tsdb_db_t *db;
    int        queries_per_thread;
    int        errors;
    int64_t    expected_count;
} stress_arg_t;

static void *stress_worker(void *arg) {
    stress_arg_t *a = (stress_arg_t *)arg;
    for (int i = 0; i < a->queries_per_thread; i++) {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(a->db, "SELECT count(*) FROM stress", &r);
        if (rc != TSDB_OK) { a->errors++; continue; }
        if (!tsdb_result_next(r)) { a->errors++; tsdb_result_free(r); continue; }
        int64_t cnt = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
        if (cnt != a->expected_count) a->errors++;
    }
    return NULL;
}

/* ---- main --------------------------------------------------------------- */

int main(void) {
    printf("=== test_parallel ===\n\n");

    /* ------------------------------------------------------------------ */
    /* Setup: insert 1M rows into a table that flushes to disk.            */
    /* ------------------------------------------------------------------ */
    const char *dir = "/tmp/tsdb_test_parallel";
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"value", TSDB_TYPE_FLOAT64},
        {"iv",    TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "par", cols, 3, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "par", &tbl));

    int64_t N = 1000000;
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_ts_t step = 1000000LL; /* 1ms */

    printf("[setup] inserting %lldM rows...\n", (long long)N / 1000000);
    double t0 = now_sec();
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(tbl, &b));
    for (int64_t i = 0; i < N; i++) {
        OK(tsdb_batch_row_ts(b, base + i * step));
        OK(tsdb_batch_row_f64(b, 1, 1.0 + (double)(i % 1000)));
        OK(tsdb_batch_row_i64(b, 2, i + 1));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    printf("[setup] ingest done in %.2fs\n", now_sec() - t0);

    /* Warm up table cache. */
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM par", &r));
        tsdb_result_next(r); tsdb_result_free(r);
    }

    /* ------------------------------------------------------------------ */
    /* Test 1: serial count == parallel count                              */
    /* ------------------------------------------------------------------ */
    printf("\n[1] count(*) serial vs parallel\n");

    tsdb_set_query_parallel(0);
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT count(*) FROM par", &r));
    assert(tsdb_result_next(r));
    int64_t cnt_serial = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    printf("  serial  count = %lld\n", (long long)cnt_serial);

    tsdb_set_query_parallel(1);
    OK(tsdb_query(db, "SELECT count(*) FROM par", &r));
    assert(tsdb_result_next(r));
    int64_t cnt_parallel = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    printf("  parallel count = %lld\n", (long long)cnt_parallel);

    if (cnt_serial != cnt_parallel)
        FAIL("count mismatch: serial=%lld parallel=%lld",
             (long long)cnt_serial, (long long)cnt_parallel);
    assert(cnt_serial == N);
    printf("  PASS: counts match (%lld)\n", (long long)N);

    /* ------------------------------------------------------------------ */
    /* Test 2: serial sum == parallel sum                                  */
    /* ------------------------------------------------------------------ */
    printf("\n[2] sum(iv) serial vs parallel\n");

    tsdb_set_query_parallel(0);
    OK(tsdb_query(db, "SELECT sum(iv) FROM par", &r));
    assert(tsdb_result_next(r));
    int64_t sum_serial = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    printf("  serial  sum = %lld\n", (long long)sum_serial);

    tsdb_set_query_parallel(1);
    OK(tsdb_query(db, "SELECT sum(iv) FROM par", &r));
    assert(tsdb_result_next(r));
    int64_t sum_parallel = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    printf("  parallel sum = %lld\n", (long long)sum_parallel);

    /* sum(1..N) = N*(N+1)/2 */
    int64_t expected_sum = (int64_t)N * ((int64_t)N + 1) / 2;
    if (sum_serial != expected_sum)
        FAIL("serial sum wrong: got %lld want %lld", (long long)sum_serial, (long long)expected_sum);
    if (sum_parallel != expected_sum)
        FAIL("parallel sum wrong: got %lld want %lld", (long long)sum_parallel, (long long)expected_sum);
    printf("  PASS: sums match (%lld)\n", (long long)expected_sum);

    /* ------------------------------------------------------------------ */
    /* Test 3: sum(value) float — relative tolerance 1e-9                  */
    /* ------------------------------------------------------------------ */
    printf("\n[3] sum(value) float serial vs parallel\n");

    tsdb_set_query_parallel(0);
    OK(tsdb_query(db, "SELECT sum(value) FROM par", &r));
    assert(tsdb_result_next(r));
    double fsum_serial = tsdb_result_f64(r, 0);
    tsdb_result_free(r);

    tsdb_set_query_parallel(1);
    OK(tsdb_query(db, "SELECT sum(value) FROM par", &r));
    assert(tsdb_result_next(r));
    double fsum_parallel = tsdb_result_f64(r, 0);
    tsdb_result_free(r);

    double rel_err = fabs(fsum_serial - fsum_parallel) / (fabs(fsum_serial) + 1e-30);
    printf("  serial  sum(value) = %.6f\n", fsum_serial);
    printf("  parallel sum(value) = %.6f\n", fsum_parallel);
    printf("  relative error = %.2e\n", rel_err);
    if (rel_err > 1e-9)
        FAIL("float sum relative error %.2e exceeds 1e-9", rel_err);
    printf("  PASS: float sums within tolerance\n");

    /* ------------------------------------------------------------------ */
    /* Test 4: throughput comparison — sum at 1 vs N_CPU threads           */
    /* ------------------------------------------------------------------ */
    printf("\n[4] throughput: 1-thread vs pool-thread sum(iv)\n");
    int iters = 10;

    tsdb_set_query_parallel(0);
    double t_serial_start = now_sec();
    for (int i = 0; i < iters; i++) {
        OK(tsdb_query(db, "SELECT sum(iv) FROM par", &r));
        tsdb_result_next(r); tsdb_result_free(r);
    }
    double t_serial_end = now_sec();
    double t_serial_total = (t_serial_end - t_serial_start) / iters * 1000.0;

    tsdb_set_query_parallel(1);
    double t_par_start = now_sec();
    for (int i = 0; i < iters; i++) {
        OK(tsdb_query(db, "SELECT sum(iv) FROM par", &r));
        tsdb_result_next(r); tsdb_result_free(r);
    }
    double t_par_end = now_sec();
    double t_par_total = (t_par_end - t_par_start) / iters * 1000.0;

    double speedup = (t_par_total > 0) ? t_serial_total / t_par_total : 0.0;
    printf("  serial  avg=%.2fms\n", t_serial_total);
    printf("  parallel avg=%.2fms\n", t_par_total);
    printf("  speedup=%.2fx\n", speedup);
    /* Sanity: parallel should not be dramatically slower than serial. */
    if (t_par_total > t_serial_total * 5.0)
        FAIL("parallel is >5x slower than serial (%.2fx slowdown)", t_par_total / t_serial_total);
    printf("  PASS\n");

    /* ------------------------------------------------------------------ */
    /* Test 5: stress — 200 threads × 50 queries = 10000 concurrent calls  */
    /* ------------------------------------------------------------------ */
    printf("\n[5] stress test: 10000 concurrent tsdb_query() calls\n");

    /* Create dedicated stress table. */
    tsdb_col_t scols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "stress", scols, 2, "ts"));
    tsdb_table_t *stbl = NULL;
    OK(tsdb_open_table(db, "stress", &stbl));

    int64_t SROWS = 100000;
    tsdb_batch_t *sb = NULL;
    OK(tsdb_batch_begin(stbl, &sb));
    for (int64_t i = 0; i < SROWS; i++) {
        OK(tsdb_batch_row_ts(sb, base + i * step));
        OK(tsdb_batch_row_i64(sb, 1, i + 1));
        OK(tsdb_batch_row_end(sb));
    }
    OK(tsdb_batch_commit(sb));

    /* Warm up. */
    {
        tsdb_result_t *wr = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM stress", &wr));
        assert(tsdb_result_next(wr));
        int64_t wc = tsdb_result_i64(wr, 0);
        tsdb_result_free(wr);
        if (wc != SROWS) FAIL("stress table count wrong: %lld", (long long)wc);
    }

    tsdb_set_query_parallel(1);

    int nthreads = 200;
    int queries_per = 50;
    stress_arg_t *args = calloc((size_t)nthreads, sizeof(stress_arg_t));
    pthread_t *tids = malloc((size_t)nthreads * sizeof(pthread_t));

    for (int i = 0; i < nthreads; i++) {
        args[i].db = db;
        args[i].queries_per_thread = queries_per;
        args[i].errors = 0;
        args[i].expected_count = SROWS;
        pthread_create(&tids[i], NULL, stress_worker, &args[i]);
    }
    for (int i = 0; i < nthreads; i++)
        pthread_join(tids[i], NULL);

    int total_errors = 0;
    for (int i = 0; i < nthreads; i++) total_errors += args[i].errors;
    free(args); free(tids);

    printf("  %d threads × %d queries = %d total\n",
           nthreads, queries_per, nthreads * queries_per);
    if (total_errors > 0)
        FAIL("stress test: %d errors out of %d queries",
             total_errors, nthreads * queries_per);
    printf("  PASS: 0 errors\n");

    /* ------------------------------------------------------------------ */
    /* Cleanup                                                             */
    /* ------------------------------------------------------------------ */
    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== All parallel tests PASSED ===\n");
    return 0;
}
