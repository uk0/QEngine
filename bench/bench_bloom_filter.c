/* bench_bloom_filter.c — Bloom filter block-skip performance benchmark.
 *
 * Measures WHERE sym='dev_042' query latency on 1000-device, large dataset
 * with and without bloom filter pruning.
 *
 * Configuration:
 *   - 1000 distinct symbols (dev_000 .. dev_999)
 *   - TOTAL_ROWS = 1M rows spread sequentially across devices
 *     (each device gets TOTAL_ROWS/N_DEVS rows = 1000 rows/device)
 *   - One target: dev_042
 *   - Query: SELECT count(*) FROM bench_bloom WHERE sym = 'dev_042'
 *
 * Metrics:
 *   - Wall time with bloom (default build)
 *   - Block skipped / total
 *   - Throughput
 */

#include "../include/tsdb.h"
#include "../src/query/exec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>

#define N_DEVS      1000
#define ROWS_PER_DEV  1000
#define TOTAL_ROWS    (N_DEVS * ROWS_PER_DEV)   /* 1,000,000 */
#define BASE_NS       1776211200000000000LL       /* 2026-04-15 00:00:00 UTC */
#define TARGET_DEV    "dev_042"
#define TABLE_NAME    "bench_bloom"

static double wall_seconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void fill_dev_name(char *buf, int idx) {
    snprintf(buf, 16, "dev_%03d", idx);
}

int main(void) {
    printf("=== Bloom filter benchmark ===\n");
    printf("  Devices   : %d\n", N_DEVS);
    printf("  Rows/dev  : %d\n", ROWS_PER_DEV);
    printf("  Total rows: %d\n", TOTAL_ROWS);
    printf("  Target    : %s\n", TARGET_DEV);

    /* Setup temporary directory. */
    char data_dir[] = "/tmp/tsdb_bloom_bench_XXXXXX";
    if (!mkdtemp(data_dir)) {
        fprintf(stderr, "mkdtemp failed\n");
        return 1;
    }

    tsdb_db_t *db = NULL;
    if (tsdb_open(data_dir, &db) != TSDB_OK) {
        fprintf(stderr, "tsdb_open failed\n");
        return 1;
    }

    /* Create table. */
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "sym", TSDB_TYPE_SYMBOL    },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    tsdb_create_table(db, TABLE_NAME, cols, 3, "ts");
    tsdb_table_t *tbl = NULL;
    tsdb_open_table(db, TABLE_NAME, &tbl);

    printf("\n[1/3] Inserting %d rows sequentially (dev_000 x %d, dev_001 x %d, ...)...\n",
           TOTAL_ROWS, ROWS_PER_DEV, ROWS_PER_DEV);

    double t0 = wall_seconds();

    /* Insert in batches of ROWS_PER_DEV per device (sequential by device).
     * This ensures each TSDB_BLOCK_POINTS=8192 block is dominated by
     * ~8 distinct device symbols, keeping the bloom selective. */
    char devname[16];
    tsdb_batch_t *batch = NULL;
    tsdb_batch_begin(tbl, &batch);
    for (int d = 0; d < N_DEVS; d++) {
        fill_dev_name(devname, d);
        for (int r = 0; r < ROWS_PER_DEV; r++) {
            int64_t ts = BASE_NS + (int64_t)((d * ROWS_PER_DEV + r)) * 1000000LL;
            tsdb_batch_row_ts(batch, ts);
            tsdb_batch_row_sym(batch, 1, devname);
            tsdb_batch_row_f64(batch, 2, (double)(d * 1000 + r));
            tsdb_batch_row_end(batch);
        }
    }
    tsdb_batch_commit(batch);

    double insert_s = wall_seconds() - t0;
    printf("    Insert: %.2f s  (%.0f rows/s)\n",
           insert_s, TOTAL_ROWS / insert_s);

    /* Force serial query for deterministic bloom stats. */
    tsdb_set_query_parallel(0);

    printf("\n[2/3] Query WITH bloom filter:\n");

    /* Warm-up. */
    tsdb_result_t *r = NULL;
    tsdb_query(db, "SELECT count(*) FROM " TABLE_NAME " WHERE sym = 'dev_042'", &r);
    tsdb_result_free(r);

    /* Timed run. */
#define N_RUNS  10
    double times[N_RUNS];
    int64_t result_count = 0;

    for (int run = 0; run < N_RUNS; run++) {
        double t_start = wall_seconds();
        r = NULL;
        tsdb_query(db, "SELECT count(*) FROM " TABLE_NAME " WHERE sym = 'dev_042'", &r);
        times[run] = wall_seconds() - t_start;
        if (tsdb_result_next(r) == 1) result_count = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
    }

    uint64_t skipped = tsdb_bloom_stats_skipped();
    uint64_t total   = tsdb_bloom_stats_total();

    /* Statistics. */
    double sum = 0, mn = times[0], mx = times[0];
    for (int i = 0; i < N_RUNS; i++) {
        sum += times[i];
        if (times[i] < mn) mn = times[i];
        if (times[i] > mx) mx = times[i];
    }
    double avg = sum / N_RUNS;

    printf("    Result count : %lld (expected %d)\n",
           (long long)result_count, ROWS_PER_DEV);
    printf("    Blocks total : %llu\n", (unsigned long long)total);
    printf("    Blocks skip  : %llu (%.1f%%)\n",
           (unsigned long long)skipped,
           total ? 100.0 * skipped / total : 0.0);
    printf("    Latency avg  : %.3f ms\n", avg * 1000.0);
    printf("    Latency min  : %.3f ms\n", mn * 1000.0);
    printf("    Latency max  : %.3f ms\n", mx * 1000.0);
    printf("    Throughput   : %.0f rows/s\n",
           result_count / avg);

    /* Non-existent symbol benchmark. */
    printf("\n[3/3] Query non-existent symbol (expect all blocks skipped):\n");
    t0 = wall_seconds();
    r = NULL;
    tsdb_query(db, "SELECT count(*) FROM " TABLE_NAME " WHERE sym = 'dev_NONEXISTENT'", &r);
    double ne_time = wall_seconds() - t0;
    int64_t ne_count = 0;
    if (tsdb_result_next(r) == 1) ne_count = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    uint64_t ne_skip = tsdb_bloom_stats_skipped();
    uint64_t ne_tot  = tsdb_bloom_stats_total();
    printf("    Result count : %lld\n", (long long)ne_count);
    printf("    Blocks skip  : %llu / %llu (%.1f%%)\n",
           (unsigned long long)ne_skip, (unsigned long long)ne_tot,
           ne_tot ? 100.0 * ne_skip / ne_tot : 0.0);
    printf("    Latency      : %.3f ms\n", ne_time * 1000.0);

    tsdb_close(db);

    printf("\n=== Benchmark complete ===\n");
    return 0;
}
