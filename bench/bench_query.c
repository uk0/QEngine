/* bench_query.c — query latency benchmark.
 *
 * Seeds a table with N rows and measures p50/p99 of:
 *   Q1: SELECT count(*) FROM t
 *   Q2: SELECT avg(price) FROM t WHERE symbol = 'AAPL'
 *   Q3: SELECT time_bucket(ts, 1s), avg(price) FROM t WHERE symbol='AAPL' SAMPLE BY 1s LIMIT 100
 */
#include "tsdb.h"
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

static void run_query(tsdb_db_t *db, const char *q, int iters, const char *label) {
    double *lat = malloc(iters * sizeof(double));
    for (int i = 0; i < iters; i++) {
        tsdb_result_t *r = NULL;
        double t0 = now_sec();
        if (tsdb_query(db, q, &r) != TSDB_OK) { fprintf(stderr, "Q fail: %s\n", q); exit(1); }
        while (tsdb_result_next(r)) { /* drain */ }
        tsdb_result_free(r);
        lat[i] = (now_sec() - t0) * 1000.0; /* ms */
    }
    qsort(lat, iters, sizeof(double), cmp_dbl);
    double p50 = lat[iters / 2];
    double p99 = lat[(int)(iters * 0.99)];
    double pmin = lat[0], pmax = lat[iters - 1];
    printf("  %-20s  iters=%d  p50=%.2fms  p99=%.2fms  min=%.2fms  max=%.2fms\n",
           label, iters, p50, p99, pmin, pmax);
    free(lat);
}

int main(int argc, char **argv) {
    int64_t N = 5000000;  /* default 5M rows */
    if (argc > 1) N = strtoll(argv[1], NULL, 10);

    const char *dir = "/tmp/tsdb_bench_query";
    rm_rf(dir);

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

    printf("=== tsdb query bench (N=%lld) ===\n", (long long)N);
    printf("ingesting...\n");
    double t0 = now_sec();
    tsdb_batch_t *b; tsdb_batch_begin(t, &b);
    for (int64_t i = 0; i < N; i++) {
        tsdb_batch_row_ts(b, base + i * step);
        tsdb_batch_row_sym(b, 1, syms[i % nsyms]);
        tsdb_batch_row_f64(b, 2, 100.0 + (double)(i % 10000) * 0.01);
        tsdb_batch_row_i64(b, 3, 1000 + (i % 100000));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    double t1 = now_sec();
    printf("ingest: %.2fs  (%.2fM rows/s)\n", t1 - t0, N / (t1 - t0) / 1e6);

    int iters = 20;
    printf("\nquery latency:\n");
    run_query(db, "SELECT count(*) FROM trades", iters, "Q1 count(*)");
    run_query(db, "SELECT avg(price) FROM trades WHERE symbol = 'AAPL'", iters, "Q2 avg filter");
    run_query(db, "SELECT sum(volume) FROM trades WHERE price > 500.0", iters, "Q3 range filter");
    run_query(db, "SELECT time_bucket(ts, 1s), avg(price) FROM trades WHERE symbol = 'AAPL' SAMPLE BY 1s LIMIT 100", iters, "Q4 sample by 1s");

    tsdb_close(db);
    rm_rf(dir);
    return 0;
}
