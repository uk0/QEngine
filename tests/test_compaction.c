/* test_compaction.c — integration test for size-tiered block compaction.
 *
 * Tests:
 *  1. Write 32 × 8192-row batches → 32 blocks per column.
 *  2. Call tsdb_compactor_run_once().
 *  3. Assert block count per column drops to ≤ 8.
 *  4. Assert count(*) and sum(value) are identical before and after.
 *  5. Print wall time + bytes written + bytes saved.
 */

#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/schema.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

/* ---- Helpers -------------------------------------------------------------- */

#define ASSERT(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", \
                    #cond, __FILE__, __LINE__); \
            abort(); \
        } \
    } while(0)

#define ASSERT_OK(rc) \
    do { \
        int _rc = (rc); \
        if (_rc != TSDB_OK) { \
            fprintf(stderr, "ASSERT_OK FAILED: %s == %d (%s)  [%s:%d]\n", \
                    #rc, _rc, tsdb_errstr(_rc), __FILE__, __LINE__); \
            abort(); \
        } \
    } while(0)

static double wall_time_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/*
 * Recursively remove a directory tree.
 */
static void rm_rf(const char *path) {
    DIR *d = opendir(path);
    if (!d) { remove(path); return; }
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
        char sub[4096];
        snprintf(sub, sizeof(sub), "%s/%s", path, e->d_name);
        rm_rf(sub);
    }
    closedir(d);
    rmdir(path);
}

/*
 * Count blocks in a .idx file.  Returns 0 if file absent.
 */
static uint32_t count_blocks_in_idx(const char *idx_path) {
    FILE *f = fopen(idx_path, "rb");
    if (!f) return 0;
    uint8_t hdr[36];
    size_t n = fread(hdr, 1, sizeof(hdr), f);
    fclose(f);
    if (n < 8) return 0;
    /* magic check */
    uint32_t magic = (uint32_t)hdr[0] | ((uint32_t)hdr[1]<<8) |
                     ((uint32_t)hdr[2]<<16) | ((uint32_t)hdr[3]<<24);
    if (magic != 0x31584449u) return 0;
    return (uint32_t)hdr[4] | ((uint32_t)hdr[5]<<8) |
           ((uint32_t)hdr[6]<<16) | ((uint32_t)hdr[7]<<24);
}

/*
 * Walk <table_dir>/<partition>/<col>.idx and return max block count
 * across all partitions for that column.
 */
static uint32_t max_block_count_for_col(const char *table_dir, const char *col_name) {
    uint32_t mx = 0;
    DIR *td = opendir(table_dir);
    if (!td) return 0;
    struct dirent *pe;
    while ((pe = readdir(td)) != NULL) {
        if (pe->d_name[0] == '.') continue;
        size_t n = strlen(pe->d_name);
        if (n != 8 && n != 10) continue;
        char idx_path[4096];
        snprintf(idx_path, sizeof(idx_path), "%s/%s/%s.idx",
                 table_dir, pe->d_name, col_name);
        uint32_t bc = count_blocks_in_idx(idx_path);
        if (bc > mx) mx = bc;
    }
    closedir(td);
    return mx;
}

/* ---- Main test ------------------------------------------------------------ */

static const char *TMP_DIR = "/tmp/tsdb_test_compaction";
static const char *TABLE    = "metrics";

/* Schema: ts (TIMESTAMP), value (FLOAT64), tag (INT64) */
static tsdb_col_t COLS[] = {
    { "ts",    TSDB_TYPE_TIMESTAMP },
    { "value", TSDB_TYPE_FLOAT64   },
    { "tag",   TSDB_TYPE_INT64     },
};
static const int NCOLS = 3;

#define BATCH_ROWS    8192    /* exactly one block's worth per flush */
#define N_BATCHES     32      /* → 32 blocks per column before compaction */

/* All rows land on 2026-04-01 UTC (within the first partition). */
#define BASE_TS_NS   ((int64_t)1743465600LL * 1000000000LL)  /* 2026-04-01 00:00 UTC */
#define TS_STEP_NS   1000000LL                                /* 1 ms */

int main(void) {
    printf("[TEST] compaction: 32-block → merge via run_once\n");

    /* Clean slate. */
    rm_rf(TMP_DIR);

    /* 1. Open DB, create table. */
    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(TMP_DIR, &db));

    ASSERT_OK(tsdb_create_table(db, TABLE, COLS, (size_t)NCOLS, "ts"));

    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, TABLE, &tbl));

    /* 2. Write N_BATCHES × BATCH_ROWS rows, each batch is a separate commit
     *    so we get one block per batch per column (BATCH_ROWS == TSDB_BLOCK_POINTS).
     */
    int64_t  global_row = 0;
    int64_t  expected_count = (int64_t)N_BATCHES * BATCH_ROWS;
    double   expected_value_sum = 0.0;
    int64_t  expected_tag_sum   = 0;

    for (int b = 0; b < N_BATCHES; b++) {
        tsdb_batch_t *batch = NULL;
        ASSERT_OK(tsdb_batch_begin(tbl, &batch));

        for (int r = 0; r < BATCH_ROWS; r++) {
            int64_t ts = BASE_TS_NS + global_row * TS_STEP_NS;
            double  val = (double)(global_row % 1000) * 0.01 + 1.0;
            int64_t tag = global_row % 256;

            ASSERT_OK(tsdb_batch_row_ts(batch, ts));
            ASSERT_OK(tsdb_batch_row_f64(batch, 1, val));
            ASSERT_OK(tsdb_batch_row_i64(batch, 2, tag));
            ASSERT_OK(tsdb_batch_row_end(batch));

            expected_value_sum += val;
            expected_tag_sum   += tag;
            global_row++;
        }

        ASSERT_OK(tsdb_batch_commit(batch));
    }
    printf("  wrote %lld rows across %d batches\n",
           (long long)expected_count, N_BATCHES);

    /* 3. Record pre-compaction block count. */
    char table_dir[4096];
    snprintf(table_dir, sizeof(table_dir), "%s/%s", TMP_DIR, TABLE);

    uint32_t pre_ts    = max_block_count_for_col(table_dir, "ts");
    uint32_t pre_value = max_block_count_for_col(table_dir, "value");
    uint32_t pre_tag   = max_block_count_for_col(table_dir, "tag");
    printf("  pre-compaction  block counts: ts=%u  value=%u  tag=%u\n",
           pre_ts, pre_value, pre_tag);
    ASSERT(pre_ts    >= 1u);
    ASSERT(pre_value >= 1u);
    ASSERT(pre_tag   >= 1u);

    /* 4. Start compactor with low threshold (4) so even moderate block counts
     *    are rewritten. */
    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns           = 5000000000LL;  /* 5 s (irrelevant — we call run_once) */
    opts.worker_threads        = 1;

    tsdb_compactor_t *cpt = NULL;
    ASSERT_OK(tsdb_compactor_start(db, &opts, &cpt));

    /* 5. Make partitions look "cold" by back-dating mtime by 120 s.
     *    The compactor skips partitions modified in the last 60 s. */
    {
        DIR *td = opendir(table_dir);
        if (td) {
            struct dirent *pe;
            while ((pe = readdir(td)) != NULL) {
                if (pe->d_name[0] == '.') continue;
                size_t nl = strlen(pe->d_name);
                if (nl != 8 && nl != 10) continue;
                char part_dir[4096];
                snprintf(part_dir, sizeof(part_dir), "%s/%s", table_dir, pe->d_name);
                struct stat st;
                if (stat(part_dir, &st) == 0) {
                    /* Back-date mtime by 120 s so the compactor treats the
                     * partition as cold (it skips those < 60 s old). */
                    struct utimbuf ut;
                    ut.actime  = st.st_atime;
                    ut.modtime = st.st_mtime - 120;
                    utime(part_dir, &ut);
                }
            }
            closedir(td);
        }
    }

    double t0 = wall_time_s();
    ASSERT_OK(tsdb_compactor_run_once(cpt));
    double elapsed = wall_time_s() - t0;

    tsdb_compactor_stats_t stats;
    tsdb_compactor_stats(cpt, &stats);

    tsdb_compactor_stop(cpt);

    /* 6. Record post-compaction block count. */
    uint32_t post_ts    = max_block_count_for_col(table_dir, "ts");
    uint32_t post_value = max_block_count_for_col(table_dir, "value");
    uint32_t post_tag   = max_block_count_for_col(table_dir, "tag");
    printf("  post-compaction block counts: ts=%u  value=%u  tag=%u\n",
           post_ts, post_value, post_tag);

    /* COMPACT_BLOCK_POINTS = 32768 = 4 × 8192.
     * 32 × 8192 = 262144 rows.  ceil(262144/32768) = 8. */
    ASSERT(post_ts    <= 8u);
    ASSERT(post_value <= 8u);
    ASSERT(post_tag   <= 8u);
    ASSERT(post_ts    < pre_ts);
    ASSERT(post_value < pre_value);
    ASSERT(post_tag   < pre_tag);

    /* 7. Verify data integrity via the query engine. */
    tsdb_result_t *res = NULL;
    int qrc = tsdb_query(db,
        "SELECT count(ts), sum(value), sum(tag) FROM metrics", &res);
    ASSERT_OK(qrc);

    int got_row = tsdb_result_next(res);
    ASSERT(got_row == 1);

    int64_t got_count = tsdb_result_i64(res, 0);
    double  got_vsum  = tsdb_result_f64(res, 1);
    /* sum(tag) over INT64 column returns INT64. */
    int64_t got_tsum  = tsdb_result_i64(res, 2);
    tsdb_result_free(res);

    printf("  count: expected=%lld  got=%lld\n",
           (long long)expected_count, (long long)got_count);
    printf("  value_sum: expected=%.4f  got=%.4f\n",
           expected_value_sum, got_vsum);
    printf("  tag_sum  : expected=%lld  got=%lld\n",
           (long long)expected_tag_sum, (long long)got_tsum);

    ASSERT(got_count == expected_count);
    /* Allow 0.1% relative tolerance for floating-point accumulation. */
    {
        double rel_err = (got_vsum - expected_value_sum);
        if (rel_err < 0) rel_err = -rel_err;
        ASSERT(rel_err < expected_value_sum * 0.001 + 1e-3);
    }
    ASSERT(got_tsum == expected_tag_sum);

    /* 8. Print stats. */
    printf("  compactions_done : %llu\n", (unsigned long long)stats.compactions_done);
    printf("  parts_merged     : %llu\n", (unsigned long long)stats.parts_merged);
    printf("  bytes_written    : %llu\n", (unsigned long long)stats.bytes_written);
    printf("  bytes_saved      : %llu\n", (unsigned long long)stats.bytes_saved);
    printf("  run_once wall time: %.3f ms\n", elapsed * 1000.0);

    ASSERT(stats.compactions_done >= 1);

    /* 9. Close DB. */
    tsdb_close(db);

    /* Cleanup. */
    rm_rf(TMP_DIR);

    printf("[PASS] compaction test\n");
    return 0;
}
