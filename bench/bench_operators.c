/* bench_operators.c — per-operator compute-time benchmark across data volumes.
 *
 * Seeds `trades` (ts, symbol, price, volume) + `quotes` (ts, symbol, bid) with
 * N rows of deterministic data, then measures the p50 wall time of every query
 * operator the engine supports.  Run it at several N (e.g. 10000 / 100000 /
 * 1000000) to see how each operator scales.
 *
 *   ./build/bench/bench_operators 1000000
 *
 * Reports, per operator: p50 latency (ms) and scan throughput (Mrows/s).
 * An operator whose query the dialect rejects is printed as "n/a" rather than
 * aborting the run, so the table is always complete.
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

/* Run query `iters` times; return p50 latency (ms), or -1.0 if it fails. */
static double q_p50_ms(tsdb_db_t *db, const char *q, int iters) {
    double *lat = malloc((size_t)iters * sizeof(double));
    if (!lat) return -1.0;
    for (int i = 0; i < iters; i++) {
        tsdb_result_t *r = NULL;
        double t0 = now_sec();
        if (tsdb_query(db, q, &r) != TSDB_OK || !r) {
            if (r) tsdb_result_free(r);
            free(lat);
            return -1.0;
        }
        while (tsdb_result_next(r)) { /* drain */ }
        tsdb_result_free(r);
        lat[i] = (now_sec() - t0) * 1000.0;
    }
    qsort(lat, (size_t)iters, sizeof(double), cmp_dbl);
    double p50 = lat[iters / 2];
    free(lat);
    return p50;
}

/* Print one operator row: p50 ms + Mrows/s (N scanned / p50). */
static void row(tsdb_db_t *db, const char *label, const char *q,
                int iters, int64_t N) {
    double p50 = q_p50_ms(db, q, iters);
    if (p50 < 0) {
        printf("  %-22s  %12s  %12s\n", label, "n/a", "n/a");
        return;
    }
    double mrps = (p50 > 0) ? ((double)N / (p50 / 1000.0) / 1e6) : 0.0;
    printf("  %-22s  %10.3f    %10.1f\n", label, p50, mrps);
}

static tsdb_db_t *build_db(const char *dir, int64_t N, double *out_ingest_s) {
    tsdb_db_t *db;
    tsdb_open(dir, &db);

    tsdb_col_t tcols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"symbol", TSDB_TYPE_SYMBOL},
        {"price",  TSDB_TYPE_FLOAT64},
        {"volume", TSDB_TYPE_INT64},
    };
    tsdb_create_table(db, "trades", tcols, 4, "ts");
    tsdb_col_t qcols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"symbol", TSDB_TYPE_SYMBOL},
        {"bid",    TSDB_TYPE_FLOAT64},
    };
    tsdb_create_table(db, "quotes", qcols, 3, "ts");

    tsdb_table_t *tt; tsdb_open_table(db, "trades", &tt);
    tsdb_table_t *qt; tsdb_open_table(db, "quotes", &qt);

    const char *syms[] = {"AAPL","MSFT","GOOG","AMZN","META","NVDA","TSLA","NFLX"};
    const int nsyms = 8;
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_ts_t step = 1000000LL; /* 1 ms */

    double t0 = now_sec();
    tsdb_batch_t *b; tsdb_batch_begin(tt, &b);
    for (int64_t i = 0; i < N; i++) {
        tsdb_batch_row_ts(b, base + i * step);
        tsdb_batch_row_sym(b, 1, syms[i % nsyms]);
        tsdb_batch_row_f64(b, 2, 100.0 + (double)(i % 10000) * 0.01);
        tsdb_batch_row_i64(b, 3, 1000 + (i % 100000));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    double t1 = now_sec();
    if (out_ingest_s) *out_ingest_s = t1 - t0;

    /* quotes: same cadence, a bid column. */
    tsdb_batch_t *qb; tsdb_batch_begin(qt, &qb);
    for (int64_t i = 0; i < N; i++) {
        tsdb_batch_row_ts(qb, base + i * step);
        tsdb_batch_row_sym(qb, 1, syms[i % nsyms]);
        tsdb_batch_row_f64(qb, 2, 99.5 + (double)(i % 10000) * 0.01);
        tsdb_batch_row_end(qb);
    }
    tsdb_batch_commit(qb);
    return db;
}

static void run_volume(int64_t N) {
    const char *dir = "/tmp/tsdb_bench_ops";
    rm_rf(dir);

    double ingest_s = 0;
    tsdb_db_t *db = build_db(dir, N, &ingest_s);

    /* More iterations for small N so p50 is stable; fewer for big N. */
    int iters = (int)(20000000LL / (N > 0 ? N : 1));
    if (iters < 8)   iters = 8;
    if (iters > 200) iters = 200;

    printf("\n======================================================================\n");
    printf(" N = %lld rows   (ingest %.3fs = %.2f Mrows/s)   iters=%d\n",
           (long long)N, ingest_s, (double)N / ingest_s / 1e6, iters);
    printf("======================================================================\n");
    printf("  %-22s  %12s  %12s\n", "operator", "p50 (ms)", "Mrows/s");
    printf("  %-22s  %12s  %12s\n", "--------", "--------", "-------");

    tsdb_set_query_parallel(1);

    /* Full-scan aggregates. */
    row(db, "count(*)",            "SELECT count(*) FROM trades", iters, N);
    row(db, "sum(volume)",         "SELECT sum(volume) FROM trades", iters, N);
    row(db, "avg(price)",          "SELECT avg(price) FROM trades", iters, N);
    row(db, "min(price)",          "SELECT min(price) FROM trades", iters, N);
    row(db, "max(price)",          "SELECT max(price) FROM trades", iters, N);
    row(db, "first(price)",        "SELECT first(price) FROM trades", iters, N);
    row(db, "last(price)",         "SELECT last(price) FROM trades", iters, N);
    row(db, "stddev(price)",       "SELECT stddev(price) FROM trades", iters, N);
    row(db, "spread(price)",       "SELECT spread(price) FROM trades", iters, N);

    /* Filters (predicate evaluation over the scan). */
    row(db, "WHERE range(f64)",    "SELECT count(*) FROM trades WHERE price > 500.0", iters, N);
    row(db, "WHERE symbol eq",     "SELECT count(*) FROM trades WHERE symbol = 'AAPL'", iters, N);
    row(db, "WHERE int range",     "SELECT count(*) FROM trades WHERE volume > 50000", iters, N);
    row(db, "filter + avg",        "SELECT avg(price) FROM trades WHERE symbol = 'AAPL'", iters, N);

    /* Grouping / windowing. */
    row(db, "GROUP BY symbol",     "SELECT symbol, avg(price) FROM trades GROUP BY symbol", iters, N);
    row(db, "GROUP BY 2-agg",      "SELECT symbol, sum(volume), avg(price) FROM trades GROUP BY symbol", iters, N);
    row(db, "SAMPLE BY 1s",        "SELECT avg(price) FROM trades SAMPLE BY 1s", iters, N);
    row(db, "SAMPLE BY 1m",        "SELECT sum(volume), avg(price) FROM trades SAMPLE BY 1m", iters, N);

    /* Latest / join. */
    row(db, "LATEST ON (partby)",  "SELECT * FROM trades LATEST ON ts PARTITION BY symbol", iters, N);
    row(db, "ASOF JOIN",           "SELECT ts, price, bid FROM trades ASOF JOIN quotes ON symbol=symbol", iters, N);

    /* Projection / full materialize (scan + emit all columns). */
    row(db, "project 3 cols",      "SELECT ts, price, volume FROM trades", iters > 20 ? 20 : iters, N);

    tsdb_close(db);
    rm_rf(dir);
}

int main(int argc, char **argv) {
    /* Volumes: default 10k / 100k / 1M (1w / 10w / 100w). Override: pass Ns. */
    if (argc > 1) {
        for (int i = 1; i < argc; i++) run_volume(strtoll(argv[i], NULL, 10));
    } else {
        run_volume(10000);
        run_volume(100000);
        run_volume(1000000);
    }
    return 0;
}
