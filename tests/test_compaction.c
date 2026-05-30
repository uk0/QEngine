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
#include <pthread.h>

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

/* Order-sensitive FNV-1a fold over the full result set of SELECT ts,value,tag.
 * Captures every stored value bit-for-bit, in row order.  Compaction
 * re-encodes losslessly and preserves row order, so the digest and the row
 * count must be byte-identical before and after a compaction pass. */
static void scan_digest(tsdb_db_t *db, const char *table,
                        uint64_t *out_h, int64_t *out_n) {
    char qtl[128];
    snprintf(qtl, sizeof(qtl), "SELECT ts, value, tag FROM %s", table);
    tsdb_result_t *r = NULL;
    uint64_t h = 1469598103934665603ULL;
    int64_t  n = 0;
    if (tsdb_query(db, qtl, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r)) {
            int64_t ts = tsdb_result_ts(r, 0);
            double  v  = tsdb_result_f64(r, 1);
            int64_t tg = tsdb_result_i64(r, 2);
            uint64_t vb; memcpy(&vb, &v, sizeof(vb));
            h = (h ^ (uint64_t)ts) * 1099511628211ULL;
            h = (h ^ vb)           * 1099511628211ULL;
            h = (h ^ (uint64_t)tg) * 1099511628211ULL;
            n++;
        }
    }
    if (r) tsdb_result_free(r);
    *out_h = h; *out_n = n;
}

/* Back-date every partition's mtime so the compactor (which skips
 * partitions touched in the last 60 s) treats them as cold. */
static void backdate_partitions(const char *table_dir) {
    DIR *td = opendir(table_dir);
    if (!td) return;
    struct dirent *pe;
    while ((pe = readdir(td)) != NULL) {
        if (pe->d_name[0] == '.') continue;
        size_t nl = strlen(pe->d_name);
        if (nl != 8 && nl != 10) continue;
        char part_dir[4096];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", table_dir, pe->d_name);
        struct stat st;
        if (stat(part_dir, &st) == 0) {
            struct utimbuf ut = { st.st_atime, st.st_mtime - 120 };
            utime(part_dir, &ut);
        }
    }
    closedir(td);
}

/* ---- T2: concurrent read during compaction -----------------------------
 * A reader that opens a partition while the compactor swaps its .col/.idx
 * must see a consistent (idx,col) pair.  The rename window is deliberately
 * widened (TSDB_TEST_COMPACT_RENAME_DELAY_MS) so the race is hit on every
 * run: with the reader holding compact_mtx around tsdb_part_open it blocks
 * through the swap and always reads correct data; without it, it would pair
 * an old idx with a freshly-compacted col → wrong/failed query. */
typedef struct {
    tsdb_db_t *db;
    int64_t    expect_count;
    int64_t    expect_tag_sum;
    int        iters;
    volatile int stop;
    int        errors;   /* wrong-result or query-error count */
} reader_arg_t;

static void *reader_thread(void *ud) {
    reader_arg_t *a = (reader_arg_t *)ud;
    for (int i = 0; i < a->iters && !a->stop; i++) {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(a->db,
            "SELECT count(ts), sum(tag) FROM ccmetrics WHERE tag >= 128", &r);
        if (rc != TSDB_OK || !r) { a->errors++; if (r) tsdb_result_free(r); continue; }
        if (!tsdb_result_next(r)) { a->errors++; tsdb_result_free(r); continue; }
        int64_t c = tsdb_result_i64(r, 0);
        int64_t s = tsdb_result_i64(r, 1);
        if (c != a->expect_count || s != a->expect_tag_sum) a->errors++;
        tsdb_result_free(r);
    }
    return NULL;
}

static void test_concurrent_read_during_compaction(void) {
    printf("\n[TEST] concurrent read during compaction (torn-read race)\n");
    const char *DIR2 = "/tmp/tsdb_test_compaction_cc";
    const char *T2   = "ccmetrics";
    rm_rf(DIR2);

    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(DIR2, &db));
    ASSERT_OK(tsdb_create_table(db, T2, COLS, (size_t)NCOLS, "ts"));
    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, T2, &tbl));

    const int NB = 20;  /* 20 flush blocks → compactor (min_blocks=4) merges */
    int64_t gr = 0, exp_count = 0, exp_tagsum = 0;
    for (int b = 0; b < NB; b++) {
        tsdb_batch_t *batch = NULL;
        ASSERT_OK(tsdb_batch_begin(tbl, &batch));
        for (int r = 0; r < BATCH_ROWS; r++) {
            int64_t ts = BASE_TS_NS + gr * TS_STEP_NS;
            double  val = (double)(gr % 1000) * 0.01 + 1.0;
            int64_t tag = gr % 256;
            ASSERT_OK(tsdb_batch_row_ts(batch, ts));
            ASSERT_OK(tsdb_batch_row_f64(batch, 1, val));
            ASSERT_OK(tsdb_batch_row_i64(batch, 2, tag));
            ASSERT_OK(tsdb_batch_row_end(batch));
            if (tag >= 128) { exp_count++; exp_tagsum += tag; }
            gr++;
        }
        ASSERT_OK(tsdb_batch_commit(batch));
    }

    char table_dir[4096];
    snprintf(table_dir, sizeof(table_dir), "%s/%s", DIR2, T2);
    backdate_partitions(table_dir);

    /* Widen the rename window so the reader reliably overlaps the swap. */
    setenv("TSDB_TEST_COMPACT_RENAME_DELAY_MS", "25", 1);

    /* Start readers first, then trigger the compaction swap underneath them. */
    reader_arg_t ra = { .db = db, .expect_count = exp_count,
                        .expect_tag_sum = exp_tagsum, .iters = 4000,
                        .stop = 0, .errors = 0 };
    pthread_t readers[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&readers[i], NULL, reader_thread, &ra);

    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns = 5000000000LL;
    opts.worker_threads = 1;
    tsdb_compactor_t *cpt = NULL;
    ASSERT_OK(tsdb_compactor_start(db, &opts, &cpt));
    ASSERT_OK(tsdb_compactor_run_once(cpt));   /* the swap (with delay) */
    tsdb_compactor_stop(cpt);

    ra.stop = 1;
    for (int i = 0; i < 4; i++) pthread_join(readers[i], NULL);

    unsetenv("TSDB_TEST_COMPACT_RENAME_DELAY_MS");

    printf("  reader errors (wrong/failed results during swap): %d\n", ra.errors);
    ASSERT(ra.errors == 0);

    tsdb_close(db);
    rm_rf(DIR2);
    printf("[PASS] concurrent read during compaction\n");
}

/* ---- T3: DROP TABLE during an active compaction pass --------------------
 * The background compactor holds a raw table pointer (schema + compact_mtx)
 * lock-free for a whole pass.  A concurrent DROP TABLE that frees the table
 * underneath it is a use-after-free / double-free (observed crashing a node
 * under drop+compaction stress).  tsdb_drop_table must wait for the pass to
 * finish.  The rename window is widened so the compactor is reliably mid-pass
 * when the drop arrives; under ASan this must complete with no UAF. */
static void test_drop_during_compaction(void) {
    printf("\n[TEST] DROP TABLE during active compaction (lifetime race)\n");
    const char *DIR3 = "/tmp/tsdb_test_compaction_dc";
    rm_rf(DIR3);

    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(DIR3, &db));
    ASSERT_OK(tsdb_create_table(db, "dctbl", COLS, (size_t)NCOLS, "ts"));
    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, "dctbl", &tbl));

    int64_t gr = 0;
    for (int b = 0; b < 12; b++) {          /* 12 blocks > min_blocks 4 */
        tsdb_batch_t *batch = NULL;
        ASSERT_OK(tsdb_batch_begin(tbl, &batch));
        for (int r = 0; r < BATCH_ROWS; r++) {
            ASSERT_OK(tsdb_batch_row_ts(batch, BASE_TS_NS + gr * TS_STEP_NS));
            ASSERT_OK(tsdb_batch_row_f64(batch, 1, 1.0));
            ASSERT_OK(tsdb_batch_row_i64(batch, 2, gr % 7));
            ASSERT_OK(tsdb_batch_row_end(batch));
            gr++;
        }
        ASSERT_OK(tsdb_batch_commit(batch));
    }
    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/%s", DIR3, "dctbl");
    backdate_partitions(tdir);

    /* Wide swap window so the worker is mid-pass (compacting=1) at drop time. */
    setenv("TSDB_TEST_COMPACT_RENAME_DELAY_MS", "300", 1);
    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns = 10000000LL;   /* 10ms — worker runs almost immediately */
    opts.worker_threads = 1;
    tsdb_compactor_t *cpt = NULL;
    ASSERT_OK(tsdb_compactor_start(db, &opts, &cpt));

    usleep(100000);   /* 100ms: worker is now inside dctbl's compaction pass */
    /* Without the lifetime guard this frees the table under the compactor and
     * ASan reports a use-after-free / double-free; with it, drop blocks until
     * the pass ends, then frees cleanly. */
    ASSERT_OK(tsdb_drop_table(db, "dctbl"));

    unsetenv("TSDB_TEST_COMPACT_RENAME_DELAY_MS");
    tsdb_compactor_stop(cpt);

    /* Table is gone — a query either errors or returns nothing, never crashes. */
    tsdb_result_t *r = NULL;
    int qrc = tsdb_query(db, "SELECT count(*) FROM dctbl", &r);
    printf("  post-drop query rc=%d (table dropped)\n", qrc);
    if (r) tsdb_result_free(r);

    tsdb_close(db);
    rm_rf(DIR3);
    printf("[PASS] DROP during compaction (no use-after-free)\n");
}

/* ---- T4: append to a partition while it is being compacted --------------
 * The compactor snapshots a partition's block count in phase 1, then swaps in
 * phase 2.  If a flush appends a block in between, swapping the stale .tmp
 * would clobber the appended block (silent row loss).  The staleness guard
 * aborts the swap when the live .idx grew.  This test asserts the INVARIANT —
 * no rows lost — which must hold whether the append lands before the snapshot
 * (compacted in) or after it (swap aborted). */
static tsdb_compactor_t *g_t4_cpt;
static void *t4_compact_thread(void *ud) {
    (void)ud;
    tsdb_compactor_run_once(g_t4_cpt);   /* compacts, then delays in the swap */
    return NULL;
}
static void test_append_during_compaction(void) {
    printf("\n[TEST] append to partition during compaction (no row loss)\n");
    const char *DIR4 = "/tmp/tsdb_test_compaction_ad";
    rm_rf(DIR4);

    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(DIR4, &db));
    ASSERT_OK(tsdb_create_table(db, "adtbl", COLS, (size_t)NCOLS, "ts"));
    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, "adtbl", &tbl));

    int64_t gr = 0, expected = 0;
    for (int b = 0; b < 12; b++) {        /* 12 blocks → compactor (min 4) merges */
        tsdb_batch_t *batch = NULL;
        ASSERT_OK(tsdb_batch_begin(tbl, &batch));
        for (int r = 0; r < BATCH_ROWS; r++) {
            ASSERT_OK(tsdb_batch_row_ts(batch, BASE_TS_NS + gr * TS_STEP_NS));
            ASSERT_OK(tsdb_batch_row_f64(batch, 1, 1.0));
            ASSERT_OK(tsdb_batch_row_i64(batch, 2, 1));
            ASSERT_OK(tsdb_batch_row_end(batch));
            gr++; expected++;
        }
        ASSERT_OK(tsdb_batch_commit(batch));
    }
    char tdir[4096]; snprintf(tdir, sizeof(tdir), "%s/%s", DIR4, "adtbl");
    backdate_partitions(tdir);

    /* Long swap delay so the append below reliably overlaps the compaction. */
    setenv("TSDB_TEST_COMPACT_RENAME_DELAY_MS", "1200", 1);
    tsdb_compactor_opts_t opts; memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4; opts.interval_ns = 10000000LL; opts.worker_threads = 1;
    ASSERT_OK(tsdb_compactor_start(db, &opts, &g_t4_cpt));

    pthread_t th; pthread_create(&th, NULL, t4_compact_thread, NULL);
    usleep(400000);   /* let phase-1 encode finish, land the append in the swap window */

    /* Append a fresh batch into the SAME partition (continues the ts sequence,
     * same day) — forces a flush that appends a block to the live .idx. */
    tsdb_batch_t *ab = NULL;
    ASSERT_OK(tsdb_batch_begin(tbl, &ab));
    for (int r = 0; r < BATCH_ROWS; r++) {
        ASSERT_OK(tsdb_batch_row_ts(ab, BASE_TS_NS + gr * TS_STEP_NS));
        ASSERT_OK(tsdb_batch_row_f64(ab, 1, 1.0));
        ASSERT_OK(tsdb_batch_row_i64(ab, 2, 1));
        ASSERT_OK(tsdb_batch_row_end(ab));
        gr++; expected++;
    }
    ASSERT_OK(tsdb_batch_commit(ab));

    pthread_join(th, NULL);
    tsdb_compactor_stop(g_t4_cpt);
    unsetenv("TSDB_TEST_COMPACT_RENAME_DELAY_MS");

    /* Invariant: every row is still present (count == seeded + appended). */
    tsdb_result_t *res = NULL;
    ASSERT_OK(tsdb_query(db, "SELECT count(ts) FROM adtbl", &res));
    ASSERT(tsdb_result_next(res));
    int64_t got = tsdb_result_i64(res, 0);
    tsdb_result_free(res);
    printf("  expected=%lld got=%lld (no rows lost to the swap)\n",
           (long long)expected, (long long)got);
    ASSERT(got == expected);

    tsdb_close(db);
    rm_rf(DIR4);
    printf("[PASS] append during compaction (no row loss)\n");
}

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
    /* References for a partial-filter aggregate (tag >= 128) — exercises the
     * gather-into-scratch path over the post-compaction 32768-row blocks. */
    int64_t  expected_count_filt = 0;
    int64_t  expected_tag_sum_filt = 0;
    double   expected_value_sum_filt = 0.0;

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
            if (tag >= 128) {
                expected_count_filt++;
                expected_tag_sum_filt   += tag;
                expected_value_sum_filt += val;
            }
            global_row++;
        }

        ASSERT_OK(tsdb_batch_commit(batch));
    }
    printf("  wrote %lld rows across %d batches\n",
           (long long)expected_count, N_BATCHES);

    /* Full-fidelity digest BEFORE compaction (every row/col, in order). */
    uint64_t pre_digest = 0;  int64_t pre_digest_n = 0;
    scan_digest(db, TABLE, &pre_digest, &pre_digest_n);

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

    /* 5b. Loop guard: a partition already at its post-compaction block count
     * (ceil(rows / COMPACT_BLOCK_POINTS)) must NOT be re-compacted — doing so
     * is a no-op that loops forever, re-encoding the same data and burning CPU
     * (observed on the live cluster).  Re-cool the partition, then start a
     * FRESH compactor (counter from 0, so we don't race the first one's worker)
     * and confirm it performs ZERO compactions on the already-compacted table. */
    backdate_partitions(table_dir);
    tsdb_compactor_opts_t opts2 = opts;
    tsdb_compactor_t *cpt2 = NULL;
    ASSERT_OK(tsdb_compactor_start(db, &opts2, &cpt2));
    sleep(1);   /* let cpt2's worker scan the cold, already-compacted table */
    tsdb_compactor_stats_t guard;
    tsdb_compactor_stats(cpt2, &guard);
    printf("  re-compaction guard: fresh compactor did %llu compactions (must be 0)\n",
           (unsigned long long)guard.compactions_done);
    ASSERT(guard.compactions_done == 0);
    tsdb_compactor_stop(cpt2);

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

    /* 7b. Partial-filter aggregate over the 32768-row compacted blocks.
     * A WHERE that selects a SUBSET (popcnt != block row-count) forces the
     * aggregate gather-into-scratch path.  Post-compaction blocks hold 32768
     * rows — 4x the legacy 8192-element scratch — so a scratch sized to the
     * compile-time TSDB_BLOCK_POINTS overflows here.  Data integrity is
     * paramount: the filtered result must be exact. */
    tsdb_result_t *res2 = NULL;
    int qrc2 = tsdb_query(db,
        "SELECT count(ts), sum(tag), sum(value) FROM metrics WHERE tag >= 128",
        &res2);
    ASSERT_OK(qrc2);
    ASSERT(tsdb_result_next(res2) == 1);
    int64_t f_count = tsdb_result_i64(res2, 0);
    int64_t f_tsum  = tsdb_result_i64(res2, 1);
    double  f_vsum  = tsdb_result_f64(res2, 2);
    tsdb_result_free(res2);

    printf("  [filter tag>=128] count    : expected=%lld  got=%lld\n",
           (long long)expected_count_filt, (long long)f_count);
    printf("  [filter tag>=128] tag_sum  : expected=%lld  got=%lld\n",
           (long long)expected_tag_sum_filt, (long long)f_tsum);
    printf("  [filter tag>=128] value_sum: expected=%.4f  got=%.4f\n",
           expected_value_sum_filt, f_vsum);

    ASSERT(f_count == expected_count_filt);
    ASSERT(f_tsum  == expected_tag_sum_filt);
    {
        double rel_err = f_vsum - expected_value_sum_filt;
        if (rel_err < 0) rel_err = -rel_err;
        ASSERT(rel_err < expected_value_sum_filt * 0.001 + 1e-3);
    }

    /* 7c. Full-fidelity: every row/column bit-identical after compaction.
     * Compaction re-encodes losslessly and preserves row order, so the
     * order-sensitive digest and row count must match exactly.  This also
     * pins the anti-entropy safety contract (count(*)/max(ts) invariant —
     * the cluster reconciles on those, so compaction must not perturb them). */
    uint64_t post_digest = 0; int64_t post_digest_n = 0;
    scan_digest(db, TABLE, &post_digest, &post_digest_n);
    printf("  full-fidelity digest: pre=%016llx/%lld  post=%016llx/%lld\n",
           (unsigned long long)pre_digest, (long long)pre_digest_n,
           (unsigned long long)post_digest, (long long)post_digest_n);
    ASSERT(post_digest_n == pre_digest_n);
    ASSERT(post_digest   == pre_digest);

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

    /* T2: concurrent read while the compactor swaps files underneath. */
    test_concurrent_read_during_compaction();

    /* T3: DROP TABLE while the compactor is actively compacting it. */
    test_drop_during_compaction();

    /* T4: append to a partition mid-compaction must not lose rows. */
    test_append_during_compaction();

    return 0;
}
