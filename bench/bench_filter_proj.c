/* bench_filter_proj.c — profile the current filter + projection path.
 *
 * The advisor flagged that src/exec/filter.c already emits bitmap keep
 * masks, so the executor is already late-materialised.  Before committing
 * to a fusion rewrite, this bench measures whether the path is
 * memory-bandwidth-bound (fusion won't help) or CPU-bound on bitmap
 * iteration (narrow fusion might help).
 *
 * Workload: SELECT v FROM bench WHERE v > K on a single table of 1M
 * INT64 rows, across selectivity levels 1%, 10%, 50%, 90%.  Raw bytes
 * touched vs wall time gives an effective throughput.
 */

#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

static double now_s(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

static void rmrf(const char *p) {
    char cmd[1024]; snprintf(cmd, sizeof(cmd), "rm -rf %s", p); (void)system(cmd);
}

/* Populate table with N INT64 rows: v[i] = i (deterministic so the WHERE
 * predicate cuts at a known threshold for each selectivity level). */
static void populate(tsdb_db_t *db, size_t N) {
    tsdb_table_t *t = NULL;
    tsdb_open_table(db, "bench", &t);
    tsdb_batch_t *b; tsdb_batch_begin(t, &b);
    for (size_t i = 0; i < N; i++) {
        tsdb_batch_row_ts (b, (tsdb_ts_t)((int64_t)i * 1000000LL));
        tsdb_batch_row_i64(b, 1, (int64_t)i);
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
}

/* Run <qtl>, drain all rows, return (nrows, wall_seconds). */
static double run_drain(tsdb_db_t *db, const char *qtl, size_t *out_rows) {
    tsdb_result_t *r = NULL;
    double t0 = now_s();
    int rc = tsdb_query(db, qtl, &r);
    if (rc != TSDB_OK || !r) {
        fprintf(stderr, "query failed rc=%d (%s)\n", rc, tsdb_errstr(rc));
        if (r) tsdb_result_free(r);
        *out_rows = 0;
        return 0.0;
    }
    size_t n = 0;
    while (tsdb_result_next(r)) n++;
    double t1 = now_s();
    tsdb_result_free(r);
    *out_rows = n;
    return t1 - t0;
}

int main(void) {
    const char *dir = "/tmp/tsdb_bench_filter_proj";
    rmrf(dir);

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open\n"); return 1; }

    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_INT64} };
    tsdb_create_table(db, "bench", cols, 2, "ts");

    const size_t N = 1000000;  /* 1M rows */
    printf("Populating %zu rows...\n", N);
    double tp0 = now_s();
    populate(db, N);
    printf("ingest: %.3f s (%.1f M rows/sec)\n",
           now_s() - tp0, (double)N / (now_s() - tp0) / 1e6);

    /* Warm the page cache with one scan. */
    size_t wn = 0;
    (void)run_drain(db, "SELECT v FROM bench", &wn);

    struct { const char *label; int64_t thresh; double expected_sel; } tests[] = {
        { "sel=1%",  (int64_t)(N - N/100),  0.01 },
        { "sel=10%", (int64_t)(N - N/10),   0.10 },
        { "sel=50%", (int64_t)(N/2),        0.50 },
        { "sel=90%", (int64_t)(N/10),       0.90 },
        { NULL, 0, 0 }
    };

    printf("\nColumn storage: 8 B / row × %zu rows = %.1f MiB raw\n",
           N, (double)(N * 8) / (1024.0 * 1024.0));
    printf("Bench: SELECT v FROM bench WHERE v > <threshold>\n\n");
    printf("%-12s %10s %10s %10s %12s\n",
           "selectivity", "rows_out", "wall_ms", "MiB/s_scan", "ns/row_out");

    for (int i = 0; tests[i].label; i++) {
        char qtl[160];
        snprintf(qtl, sizeof(qtl),
                 "SELECT v FROM bench WHERE v > %lld",
                 (long long)tests[i].thresh);

        /* Median of 3 runs to smooth noise. */
        double ts[3];
        size_t rows = 0;
        for (int k = 0; k < 3; k++) ts[k] = run_drain(db, qtl, &rows);
        /* Pick min (the cleanest run). */
        double best = ts[0]; if (ts[1] < best) best = ts[1]; if (ts[2] < best) best = ts[2];

        double mibps     = ((double)(N * 8) / (1024.0 * 1024.0)) / best;
        double ns_per_o  = rows > 0 ? (best * 1e9 / (double)rows) : 0.0;
        printf("%-12s %10zu %10.2f %10.0f %12.1f\n",
               tests[i].label, rows, best * 1000.0, mibps, ns_per_o);
    }

    tsdb_close(db);
    rmrf(dir);
    return 0;
}
