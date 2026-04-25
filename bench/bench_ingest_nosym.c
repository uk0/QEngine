/* bench_ingest_nosym.c — ingest bench WITHOUT symbol column.
 *
 * Isolates the cost of the symtab lookup path so we can quantify how
 * much speed the per-row rwlock_rdlock costs us, and decide whether
 * an inline cache is worth implementing.
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

int main(int argc, char **argv) {
    int64_t N = (argc > 1) ? strtoll(argv[1], NULL, 10) : 5000000;
    const char *dir = "/tmp/tsdb_bench_nosym";
    rm_rf(dir);

    tsdb_db_t *db; tsdb_open(dir, &db);
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"price", TSDB_TYPE_FLOAT64},
        {"volume",TSDB_TYPE_INT64},
    };
    tsdb_create_table(db, "ticks", cols, 3, "ts");
    tsdb_table_t *t; tsdb_open_table(db, "ticks", &t);

    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_batch_t *b; tsdb_batch_begin(t, &b);
    double t0 = now_sec();
    for (int64_t i = 0; i < N; i++) {
        tsdb_batch_row_ts(b, base + i * 100000LL);
        tsdb_batch_row_f64(b, 1, 100.0 + (double)(i % 10000) * 0.01);
        tsdb_batch_row_i64(b, 2, 1000 + (i % 100000));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    tsdb_close(db);
    double t1 = now_sec();

    printf("rows=%lld  time=%.3fs  rate=%.2f M rows/s\n",
           (long long)N, t1 - t0, (N / (t1 - t0)) / 1e6);
    rm_rf(dir);
    return 0;
}
