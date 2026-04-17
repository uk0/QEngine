/* bench_ingest.c — write throughput benchmark.
 *
 * Ingests N synthetic rows into a trades-like table and reports:
 *   - rows/sec  (single-threaded batch insert)
 *   - bytes/sec uncompressed
 *   - bytes/point on disk (compression ratio)
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

static uint64_t dir_size(const char *path) {
    DIR *d = opendir(path);
    if (!d) { struct stat st; return (stat(path, &st) == 0) ? (uint64_t)st.st_size : 0; }
    uint64_t total = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", path, e->d_name);
        total += dir_size(p);
    }
    closedir(d);
    return total;
}

int main(int argc, char **argv) {
    int64_t N = 1000000;
    if (argc > 1) N = strtoll(argv[1], NULL, 10);

    const char *dir = "/tmp/tsdb_bench_ingest";
    rm_rf(dir);

    tsdb_db_t *db;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open fail\n"); return 1; }

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"symbol", TSDB_TYPE_SYMBOL},
        {"price",  TSDB_TYPE_FLOAT64},
        {"volume", TSDB_TYPE_INT64},
    };
    tsdb_create_table(db, "trades", cols, 4, "ts");
    tsdb_table_t *t; tsdb_open_table(db, "trades", &t);

    const char *syms[] = {"AAPL","MSFT","GOOG","AMZN","META","NVDA","TSLA","NFLX"};
    const int nsyms = sizeof(syms) / sizeof(syms[0]);

    /* base timestamp = 2026-01-01T00:00:00, step 100us so N points cover 100 ms each 1K */
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_ts_t step = 100000LL; /* 100 microseconds */

    tsdb_batch_t *b; tsdb_batch_begin(t, &b);
    double t0 = now_sec();
    for (int64_t i = 0; i < N; i++) {
        tsdb_batch_row_ts(b, base + i * step);
        tsdb_batch_row_sym(b, 1, syms[i % nsyms]);
        tsdb_batch_row_f64(b, 2, 100.0 + (double)(i % 10000) * 0.01);
        tsdb_batch_row_i64(b, 3, 1000 + (i % 100000));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    tsdb_close(db);
    double t1 = now_sec();

    double rps = N / (t1 - t0);
    uint64_t bytes_raw  = (uint64_t)N * (8 + 4 + 8 + 8);    /* in-memory width */
    uint64_t bytes_disk = dir_size(dir);

    printf("=== tsdb ingest bench ===\n");
    printf("rows         = %lld\n", (long long)N);
    printf("wall time    = %.3f s\n", t1 - t0);
    printf("throughput   = %.2f M rows/s\n", rps / 1e6);
    printf("raw bytes    = %.2f MB\n", bytes_raw / 1e6);
    printf("on-disk      = %.2f MB\n", bytes_disk / 1e6);
    printf("bytes/point  = %.2f B\n", (double)bytes_disk / (double)N);
    printf("compression  = %.2fx\n", (double)bytes_raw / (double)bytes_disk);

    rm_rf(dir);
    return 0;
}
