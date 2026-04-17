/* bench_query.c — query latency benchmark with parallel vs serial comparison.
 *
 * Seeds a table with N rows and measures p50 of:
 *   Q1: SELECT count(*) FROM trades
 *   Q2: SELECT avg(price) FROM trades WHERE symbol = 'AAPL'
 *   Q3: SELECT sum(volume) FROM trades WHERE price > 500.0
 *   Q4: SAMPLE BY 1s (serial-only path)
 *
 * Thread-scaling table (1/2/4/8 threads × count/sum) is printed at the end.
 */
#include "tsdb.h"
#include "../src/query/exec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

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

static int cmp_dbl(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* Run query iters times, return median latency in ms. */
static double run_query_lat(tsdb_db_t *db, const char *q, int iters) {
    double *lat = malloc((size_t)iters * sizeof(double));
    for (int i = 0; i < iters; i++) {
        tsdb_result_t *r = NULL;
        double t0 = now_sec();
        if (tsdb_query(db, q, &r) != TSDB_OK) {
            fprintf(stderr, "Q fail: %s\n", q);
            free(lat);
            return -1.0;
        }
        while (tsdb_result_next(r)) { /* drain */ }
        tsdb_result_free(r);
        lat[i] = (now_sec() - t0) * 1000.0; /* ms */
    }
    qsort(lat, (size_t)iters, sizeof(double), cmp_dbl);
    double p50 = lat[iters / 2];
    free(lat);
    return p50;
}

static void bench_serial_parallel(tsdb_db_t *db, const char *q, int iters, const char *label) {
    /* Serial run. */
    tsdb_set_query_parallel(0);
    double t_serial = run_query_lat(db, q, iters);

    /* Parallel run. */
    tsdb_set_query_parallel(1);
    double t_parallel = run_query_lat(db, q, iters);

    double speedup = (t_parallel > 0) ? t_serial / t_parallel : 0.0;
    printf("  %-28s  serial=%6.2fms  parallel=%6.2fms  speedup=%.2fx\n",
           label, t_serial, t_parallel, speedup);
}

static void run_query_print(tsdb_db_t *db, const char *q, int iters, const char *label) {
    double p50 = run_query_lat(db, q, iters);
    printf("  %-28s  p50=%.2fms\n", label, p50);
}

/* Build and populate a fresh DB at `dir` with N rows of the trades schema.
 * Returns open db handle. */
static tsdb_db_t *build_db(const char *dir, int64_t N) {
    tsdb_db_t *db;
    tsdb_open(dir, &db);
    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"symbol", TSDB_TYPE_SYMBOL},
        {"price",  TSDB_TYPE_FLOAT64},
        {"volume", TSDB_TYPE_INT64},
    };
    tsdb_create_table(db, "trades", cols, 4, "ts");
    tsdb_table_t *t; tsdb_open_table(db, "trades", &t);

    const char *syms[] = {"AAPL","MSFT","GOOG","AMZN","META","NVDA","TSLA","NFLX"};
    const int nsyms = 8;
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_ts_t step = 1000000LL; /* 1 ms */

    tsdb_batch_t *b; tsdb_batch_begin(t, &b);
    for (int64_t i = 0; i < N; i++) {
        tsdb_batch_row_ts(b, base + i * step);
        tsdb_batch_row_sym(b, 1, syms[i % nsyms]);
        tsdb_batch_row_f64(b, 2, 100.0 + (double)(i % 10000) * 0.01);
        tsdb_batch_row_i64(b, 3, 1000 + (i % 100000));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    return db;
}

/* ----------------------------------------------------------------------- */
/* Thread-scaling table                                                      */
/* ----------------------------------------------------------------------- */

static void print_scaling_table(int64_t N, int iters) {
    const char *dir = "/tmp/tsdb_bench_scale";
    rm_rf(dir);

    printf("\n=== Thread-scaling table (N=%lld, p50 over %d iters) ===\n",
           (long long)N, iters);
    printf("%-8s  %-16s  %-16s\n", "Threads", "count(*) ms", "sum(volume) ms");
    printf("%-8s  %-16s  %-16s\n", "-------", "-----------", "--------------");

    double t0 = now_sec();
    tsdb_db_t *db = build_db(dir, N);
    printf("  (ingest %.2fs)\n", now_sec() - t0);

    const int thread_counts[] = {1, 2, 4, 8};
    const int ntc = (int)(sizeof(thread_counts) / sizeof(thread_counts[0]));

    tsdb_set_query_parallel(1);

    for (int ti = 0; ti < ntc; ti++) {
        int nthreads = thread_counts[ti];
        tsdb_set_query_pool_size(nthreads);

        double lat_count = run_query_lat(db, "SELECT count(*) FROM trades", iters);
        double lat_sum   = run_query_lat(db, "SELECT sum(volume) FROM trades", iters);

        printf("%-8d  %-16.3f  %-16.3f\n", nthreads, lat_count, lat_sum);
    }

    tsdb_close(db);
    rm_rf(dir);
}

int main(int argc, char **argv) {
    int64_t N = 5000000;  /* default 5M rows */
    if (argc > 1) N = strtoll(argv[1], NULL, 10);

    const char *dir = "/tmp/tsdb_bench_query";
    rm_rf(dir);

    printf("=== tsdb query bench (N=%lld) ===\n", (long long)N);
    printf("ingesting...\n");
    double t0 = now_sec();
    tsdb_db_t *db = build_db(dir, N);
    double t1 = now_sec();
    printf("ingest: %.2fs  (%.2fM rows/s)\n", t1 - t0, (double)N / (t1 - t0) / 1e6);

    int iters = 20;

    printf("\n--- Serial vs Parallel comparison (agg queries) ---\n");
    bench_serial_parallel(db, "SELECT count(*) FROM trades", iters, "Q1 count(*)");
    bench_serial_parallel(db, "SELECT avg(price) FROM trades WHERE symbol = 'AAPL'", iters, "Q2 avg/filter");
    bench_serial_parallel(db, "SELECT sum(volume) FROM trades WHERE price > 500.0", iters, "Q3 sum/range");

    printf("\n--- Serial-only paths (unchanged) ---\n");
    tsdb_set_query_parallel(1); /* restore default */
    run_query_print(db, "SELECT time_bucket(ts, 1s), avg(price) FROM trades WHERE symbol = 'AAPL' SAMPLE BY 1s LIMIT 100",
                    iters, "Q4 sample by 1s");

    tsdb_close(db);
    rm_rf(dir);

    /* ---- Thread-scaling tables ---- */
    print_scaling_table(1000000LL,  iters);
    print_scaling_table(5000000LL,  iters);

    return 0;
}
