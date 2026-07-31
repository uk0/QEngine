/* bench_meta_e2e.c — end-to-end query latency over a MANY-BLOCK partition.
 *
 * Companion to bench_block_meta, which isolates the metadata-fetch cost.  This
 * one measures whether removing that cost moves a whole query, and it uses
 * NOTHING but the public API so the identical source compiles against a
 * pre-change tree.  Run it in both, compare.
 *
 * The partition is deliberately large in BLOCKS, because the copying fetch
 * costs O(nb) per block and so O(nb^2) per scan: at a few dozen blocks it is
 * invisible, at a few thousand it is not.
 *
 * Usage:  bench_meta_e2e [nblocks]      (default 3000 -> 24.6M rows)
 */

#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#define BP       8192                    /* TSDB_BLOCK_POINTS */
#define STEP_NS  1000000LL               /* 1 ms */
#define BASE_TS  1699920000000000000LL   /* day-aligned */
#define REPS     5

static int64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static volatile uint64_t g_sink;

int main(int argc, char **argv) {
    size_t nb = (argc > 1) ? (size_t)strtoul(argv[1], NULL, 10) : 3000;
    size_t rows = nb * BP;

    const char *dir = getenv("TSDB_BM_DIR");
    if (!dir) dir = "/tmp/tsdb_bench_meta_e2e";
    char cmd[4200]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);

    printf("=== bench_meta_e2e ===\n");
    printf("one partition: %zu blocks, %.2fM rows\n\n", nb, (double)rows / 1e6);

    int64_t t0 = now_ns();
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open\n"); return 1; }
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_FLOAT64 },
            { "k",  TSDB_TYPE_INT64 },     { "g", TSDB_TYPE_INT64 },
        };
        int rc = tsdb_create_table(db, "t", cols, 4, "ts");
        if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create %d\n", rc); return 1; }
        tsdb_table_t *t = NULL;
        if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "open_table\n"); return 1; }
        tsdb_batch_t *b = NULL;
        tsdb_batch_begin(t, &b);
        for (size_t i = 0; i < rows; i++) {
            tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * STEP_NS);
            tsdb_batch_row_f64(b, 1, (double)(i % 977));
            tsdb_batch_row_i64(b, 2, (int64_t)i);
            tsdb_batch_row_i64(b, 3, (int64_t)(i % 4));
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);
        tsdb_close(db);
    }
    printf("ingest %.1f s\n\n", (double)(now_ns() - t0) / 1e9);

    struct { const char *sql; const char *site; } qs[] = {
        { "SELECT count(*), min(v), max(v), sum(v) FROM t", "try_stats_fastpath" },
        { "SELECT sum(v) FROM t WHERE g = 1",               "scan_load_col_block" },
        { "SELECT g, count(*), sum(v) FROM t GROUP BY g",   "scan_load_col_block" },
        { "SELECT ts, v, k, g FROM t WHERE k > 24575990",   "exec_select row path" },
    };

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); return 1; }
    printf("%-52s %-22s %10s %10s\n", "query", "site", "best ms", "ns/row");
    for (unsigned qi = 0; qi < sizeof(qs) / sizeof(qs[0]); qi++) {
        tsdb_result_t *r = NULL;
        if (tsdb_query(db, qs[qi].sql, &r) == TSDB_OK && r) {   /* warm */
            while (tsdb_result_next(r)) g_sink++;
            tsdb_result_free(r);
        }
        int64_t best = 0;
        for (int rep = 0; rep < REPS; rep++) {
            int64_t a = now_ns();
            r = NULL;
            if (tsdb_query(db, qs[qi].sql, &r) == TSDB_OK && r) {
                while (tsdb_result_next(r)) g_sink++;
                tsdb_result_free(r);
            }
            int64_t e = now_ns() - a;
            if (!best || e < best) best = e;
        }
        printf("%-52s %-22s %10.2f %10.3f\n", qs[qi].sql, qs[qi].site,
               (double)best / 1e6, (double)best / (double)rows);
        fflush(stdout);
    }
    tsdb_close(db);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
    printf("\nsink=%llu\n", (unsigned long long)g_sink);
    return 0;
}
