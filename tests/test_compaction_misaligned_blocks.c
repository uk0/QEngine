/* test_compaction_misaligned_blocks.c — compaction must keep non-ts columns
 * pairable when the source block size does not tile COMPACT_BLOCK_POINTS.
 *
 * THE BUG: the ts column's output block took its range from the actual decoded
 * timestamps, but every OTHER column borrowed min/max across all SOURCE blocks
 * overlapping the output chunk.  COMPACT_BLOCK_POINTS is 32768; when the source
 * block size does not divide it, a source block straddles the output boundary,
 * so the non-ts column's block N got a ts_min strictly BELOW the ts column's
 * block N.  Reads pair non-ts columns to ts blocks by first-match on
 * (ts_min,count), that match then failed, and every non-ts read of the
 * partition returned "data corrupt" — while count(*), served from the ts
 * column, kept answering correctly, so nothing looked wrong.
 *
 * Under the default flush-on-commit mode the flush block size IS the batch
 * size, so any table written in batches that do not divide 32768 hit this the
 * first time the compactor touched the partition.
 *
 * THE FIX: compact the ts column first, keep its decoded values, and derive
 * every other column's block range from them, so all columns of a partition
 * stamp identical (ts_min,count).
 *
 * 1000 rows per batch, 200 batches, all in one day partition: 1000 does not
 * divide 32768, so output block 0 (32768 rows) ends inside source block 33.
 */
#include "tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define TDIR    "/tmp/tsdb_test_compact_misalign"
#define BATCH   1000            /* 32768 % 1000 != 0 — that is the point */
#define NBATCH  200
#define NROWS   (BATCH * NBATCH)
#define DAY1    1700000000000000000LL

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* The compactor skips partitions touched in the last 60 s. */
static void backdate(const char *tbl_dir) {
    DIR *td = opendir(tbl_dir);
    if (!td) return;
    struct dirent *pe;
    while ((pe = readdir(td))) {
        if (pe->d_name[0] == '.') continue;
        char pd[4096]; snprintf(pd, sizeof(pd), "%s/%s", tbl_dir, pe->d_name);
        struct stat st;
        if (stat(pd, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        DIR *dd = opendir(pd);
        if (dd) {
            struct dirent *fe;
            while ((fe = readdir(dd))) {
                if (fe->d_name[0] == '.') continue;
                char fp[8192]; snprintf(fp, sizeof(fp), "%s/%s", pd, fe->d_name);
                struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
                utime(fp, &tb);
            }
            closedir(dd);
        }
        struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
        utime(pd, &tb);
    }
    closedir(td);
}

/* Sum the non-ts column. Returns -1 if the query failed outright. */
static long long sum_v(tsdb_db_t *db, int64_t *out_rows) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "SELECT ts, v FROM m", &r);
    if (rc != TSDB_OK || !r) {
        fprintf(stderr, "  SELECT ts,v failed: rc=%d (%s)\n", rc, tsdb_errstr(rc));
        if (r) tsdb_result_free(r);
        return -1;
    }
    long long s = 0; int64_t n = 0;
    while (tsdb_result_next(r) > 0) { s += tsdb_result_i64(r, 1); n++; }
    tsdb_result_free(r);
    *out_rows = n;
    return s;
}

static long long count_star(tsdb_db_t *db) {
    tsdb_result_t *r = NULL; long long n = -1;
    if (tsdb_query(db, "SELECT count(*) FROM m", &r) == TSDB_OK && r &&
        tsdb_result_next(r) > 0) n = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);
    return n;
}

int main(void) {
    printf("=== test_compaction_misaligned_blocks ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "m", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));

    long long expect_sum = 0;
    for (int b = 0; b < NBATCH; b++) {
        tsdb_batch_t *bt = NULL;
        OK(tsdb_batch_begin(t, &bt));
        for (int k = 0; k < BATCH; k++) {
            int64_t i = (int64_t)b * BATCH + k;
            OK(tsdb_batch_row_ts(bt, DAY1 + i * 1000000LL));
            OK(tsdb_batch_row_i64(bt, 1, i));
            OK(tsdb_batch_row_end(bt));
            expect_sum += i;
        }
        OK(tsdb_batch_commit(bt));
    }
    OK(tsdb_db_flush_all(db));
    printf("[setup] %d rows in %d blocks of %d (32768 %% %d = %d)\n",
           NROWS, NBATCH, BATCH, BATCH, 32768 % BATCH);

    int64_t rows = 0;
    long long before = sum_v(db, &rows);
    printf("[before] rows=%lld sum(v)=%lld  count(*)=%lld\n",
           (long long)rows, before, count_star(db));
    if (before != expect_sum) FAIL("pre-compaction sum %lld != %lld", before, expect_sum);

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/m", TDIR);
    backdate(tbl_dir);

    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns = 5000000000LL;
    opts.worker_threads = 1;
    tsdb_compactor_t *cpt = NULL;
    OK(tsdb_compactor_start(db, &opts, &cpt));
    OK(tsdb_compactor_run_once(cpt));
    tsdb_compactor_stop(cpt);

    /* count(*) comes from the ts column and stayed right even with the bug —
     * check it so a failure here is clearly distinguished from the real one. */
    long long cnt = count_star(db);
    rows = 0;
    long long after = sum_v(db, &rows);
    printf("[after]  rows=%lld sum(v)=%lld  count(*)=%lld\n",
           (long long)rows, after, cnt);

    tsdb_close(db);
    rm_rf(TDIR);

    if (cnt != NROWS)
        FAIL("count(*) %lld != %d — the ts column itself was damaged", cnt, NROWS);
    if (after < 0)
        FAIL("the non-ts column became unreadable after compaction "
             "(count(*) still answered %lld, which is why this hid in production)", cnt);
    if (rows != NROWS)
        FAIL("post-compaction row count %lld != %d", (long long)rows, NROWS);
    if (after != expect_sum)
        FAIL("post-compaction sum(v) %lld != %lld", after, expect_sum);

    printf("\n=== test_compaction_misaligned_blocks PASSED ===\n");
    return 0;
}
