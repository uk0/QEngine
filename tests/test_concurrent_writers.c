/* test_concurrent_writers.c — multi-writer correctness gate for the
 * per-table batch_mu ingest path.
 *
 * Backstory: docs/perf-baseline-2026-05-08.md shows the 4-node lvm1
 * cluster scaling badly when multiple writers hit the same table:
 * 4 writers / 1 table = 308 K rows/s, 8 writers / 1 table = 197 K
 * rows/s, 4+4 writers / 2 tables = 313 K rows/s.  Diagnosis: the
 * per-table tsdb_table_lock_write (src/storage/db.c:147-155) is
 * the contention.  Any future patch that loosens this lock must
 * preserve write-correctness invariants — none of which the existing
 * test suite covered.  This is that gate.
 *
 * What we assert under N=8 writers × M=512 batches × 16 rows/batch
 * into the same table:
 *
 *   1. Total row count matches (writer_id, row_id) cardinality.
 *   2. Every (writer_id, row_id) pair appears EXACTLY ONCE — no
 *      lost writes, no torn rows where wrong column values pair
 *      up across writers.
 *   3. Per-writer values are coherent: row_id k written by writer
 *      w must read back with value = k * 100 + w (the encoded
 *      tuple).  A torn-row bug shows up as one column from writer
 *      A and another from writer B in the same row.
 *   4. count(*) returned by SQL matches the iterator total.
 *
 * Failure of any of these means the per-table lock is doing real
 * work and a "loosen the lock" patch broke an invariant.  Run
 * this clean before any (b) perf change lands, and again after.
 */

#include "../include/tsdb.h"
#include "../src/storage/db.h"  /* tsdb_table_lock_write / unlock — internal API */

#include <assert.h>
#include <dirent.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("%s", #cond); } while (0)

#define N_WRITERS    8
#define M_BATCHES    512
#define ROWS_PER_BAT 16
#define TOTAL_ROWS   ((int64_t)N_WRITERS * M_BATCHES * ROWS_PER_BAT)

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

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

typedef struct {
    tsdb_db_t       *db;
    int              writer_id;
    atomic_uint_fast64_t *errors;
    /* Each writer claims a disjoint timestamp range: ts =
     * writer_id * TOTAL_ROWS + row_index.  Combined with the
     * (writer_id, row_id) tuple encoded into the value column,
     * we can deterministically verify every row's identity on
     * read-back.  No two writers ever produce the same ts. */
} writer_arg_t;

static void *writer_main(void *arg) {
    writer_arg_t *wa = (writer_arg_t *)arg;
    tsdb_table_t *t;
    if (tsdb_open_table(wa->db, "concw", &t) != TSDB_OK) {
        atomic_fetch_add(wa->errors, 1);
        return NULL;
    }

    int64_t base_ts = (int64_t)wa->writer_id * TOTAL_ROWS;
    for (int batch = 0; batch < M_BATCHES; batch++) {
        /* Public batch-API contract: tsdb_table_lock_write must be held
         * across the entire batch_begin → row_*  → batch_commit cycle.
         * The hot paths (influx_line, replica receive RPC) all wrap
         * their writes this way; the bare batch_* functions are NOT
         * internally serialised against each other.  The point of this
         * test is to verify that the lock-protected path is correct
         * under contention — losing rows here would mean even the
         * existing serialised path leaks rows. */
        tsdb_table_lock_write(t);

        tsdb_batch_t *b;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) {
            atomic_fetch_add(wa->errors, 1);
            tsdb_table_unlock_write(t);
            return NULL;
        }
        for (int r = 0; r < ROWS_PER_BAT; r++) {
            int64_t row_id = (int64_t)batch * ROWS_PER_BAT + r;
            int64_t ts     = base_ts + row_id;
            /* Every column carries enough state that a torn row
             * (column-mismatched values) is detectable: writer_id
             * goes in `w`, row_id in `r_id`, and the verification
             * relation `v == row_id * 100 + writer_id` ties the
             * three INT64 columns together. */
            int64_t r_id = row_id;
            int64_t w    = (int64_t)wa->writer_id;
            int64_t v    = row_id * 100 + (int64_t)wa->writer_id;
            if (tsdb_batch_row_ts (b, ts) != TSDB_OK ||
                tsdb_batch_row_i64(b, 1, w) != TSDB_OK ||
                tsdb_batch_row_i64(b, 2, r_id) != TSDB_OK ||
                tsdb_batch_row_i64(b, 3, v) != TSDB_OK ||
                tsdb_batch_row_end(b) != TSDB_OK)
            {
                atomic_fetch_add(wa->errors, 1);
                /* Don't break — try to drain the batch. */
            }
        }
        if (tsdb_batch_commit(b) != TSDB_OK) atomic_fetch_add(wa->errors, 1);

        tsdb_table_unlock_write(t);
    }
    return NULL;
}

static void run_concurrent_writers(tsdb_db_t *db) {
    printf("\n[1] %d writers × %d batches × %d rows = %lld rows into one table\n",
           N_WRITERS, M_BATCHES, ROWS_PER_BAT, (long long)TOTAL_ROWS);

    pthread_t threads[N_WRITERS];
    writer_arg_t args[N_WRITERS];
    atomic_uint_fast64_t errors;
    atomic_init(&errors, 0);

    double t0 = now_sec();
    for (int i = 0; i < N_WRITERS; i++) {
        args[i].db        = db;
        args[i].writer_id = i;
        args[i].errors    = &errors;
        if (pthread_create(&threads[i], NULL, writer_main, &args[i]) != 0)
            FAIL("pthread_create");
    }
    for (int i = 0; i < N_WRITERS; i++) pthread_join(threads[i], NULL);
    double t1 = now_sec();

    uint64_t e = atomic_load(&errors);
    if (e != 0) FAIL("writer threads logged %llu errors", (unsigned long long)e);

    double mb_rows = (double)TOTAL_ROWS / 1.0e6;
    double sec     = t1 - t0;
    printf("  ingest: %.3f s  (%.2f M rows/s, single-table contended path)\n",
           sec, mb_rows / sec);
}

/* Read every row, build a presence bitmap keyed on (writer_id, row_id),
 * verify each cell appears exactly once with coherent column values. */
static void verify_no_lost_no_torn(tsdb_db_t *db) {
    printf("\n[2] read-back: every (writer, row_id) appears exactly once + columns coherent\n");

    /* Bitmap: one bit per (writer, row_id) pair.  Total bits =
     * N_WRITERS × M_BATCHES × ROWS_PER_BAT.  At 8×512×16 = 65 536
     * bits = 8 KB — trivial. */
    const int64_t per_writer = (int64_t)M_BATCHES * ROWS_PER_BAT;
    const int64_t nbits      = (int64_t)N_WRITERS * per_writer;
    uint8_t *seen = calloc((size_t)((nbits + 7) / 8), 1);
    ASSERT(seen != NULL);

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT w, r_id, v FROM concw", &r));
    int64_t rows_seen = 0;
    while (tsdb_result_next(r)) {
        int64_t w    = tsdb_result_i64(r, 0);
        int64_t r_id = tsdb_result_i64(r, 1);
        int64_t v    = tsdb_result_i64(r, 2);
        if (w < 0 || w >= N_WRITERS)              FAIL("w out of range: %lld", (long long)w);
        if (r_id < 0 || r_id >= per_writer)       FAIL("r_id out of range: %lld", (long long)r_id);
        /* Coherence check: catches torn rows where columns from two
         * different writers got combined. */
        int64_t expected_v = r_id * 100 + w;
        if (v != expected_v)
            FAIL("torn row: w=%lld r_id=%lld v=%lld expected_v=%lld",
                 (long long)w, (long long)r_id, (long long)v, (long long)expected_v);

        int64_t bit_idx = w * per_writer + r_id;
        if (seen[bit_idx >> 3] & (1u << (bit_idx & 7)))
            FAIL("duplicate: w=%lld r_id=%lld already seen", (long long)w, (long long)r_id);
        seen[bit_idx >> 3] |= (1u << (bit_idx & 7));
        rows_seen++;
    }
    tsdb_result_free(r);

    /* Lost-rows check: bits with no row are missing writes. */
    int64_t lost = 0;
    for (int64_t i = 0; i < nbits; i++) {
        if (!(seen[i >> 3] & (1u << (i & 7)))) lost++;
    }
    free(seen);

    if (rows_seen != TOTAL_ROWS)
        FAIL("rows_seen=%lld want=%lld", (long long)rows_seen, (long long)TOTAL_ROWS);
    if (lost != 0)
        FAIL("%lld rows lost out of %lld", (long long)lost, (long long)TOTAL_ROWS);

    printf("  PASS: %lld rows, every cell exactly once, all columns coherent\n",
           (long long)rows_seen);
}

static void verify_count_star(tsdb_db_t *db) {
    printf("\n[3] SELECT count(*) from SQL must agree with iterator total\n");
    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT count(*) FROM concw", &r));
    ASSERT(tsdb_result_next(r));
    int64_t cnt = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    if (cnt != TOTAL_ROWS)
        FAIL("count(*)=%lld want=%lld", (long long)cnt, (long long)TOTAL_ROWS);
    printf("  PASS: count(*) = %lld\n", (long long)cnt);
}

static void verify_per_writer_aggregates(tsdb_db_t *db) {
    printf("\n[4] GROUP BY w: each writer must report its exact row count\n");
    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT w, count(*) FROM concw GROUP BY w", &r));
    int seen[N_WRITERS] = {0};
    int64_t per_writer = (int64_t)M_BATCHES * ROWS_PER_BAT;
    while (tsdb_result_next(r)) {
        int64_t w   = tsdb_result_i64(r, 0);
        int64_t cnt = tsdb_result_i64(r, 1);
        if (w < 0 || w >= N_WRITERS) FAIL("w out of range");
        if (cnt != per_writer)
            FAIL("writer %lld got count=%lld expected=%lld",
                 (long long)w, (long long)cnt, (long long)per_writer);
        seen[w]++;
    }
    tsdb_result_free(r);
    for (int i = 0; i < N_WRITERS; i++)
        if (seen[i] != 1) FAIL("writer %d appeared %d times in GROUP BY w", i, seen[i]);
    printf("  PASS: %d writers × %lld rows each\n", N_WRITERS, (long long)per_writer);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_concurrent_writers";
    rm_rf(dir);

    printf("=== tsdb concurrent-writer correctness gate ===\n");

    tsdb_db_t *db; OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",   TSDB_TYPE_TIMESTAMP},
        {"w",    TSDB_TYPE_INT64},
        {"r_id", TSDB_TYPE_INT64},
        {"v",    TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "concw", cols, 4, "ts"));

    run_concurrent_writers(db);
    verify_no_lost_no_torn(db);
    verify_count_star(db);
    verify_per_writer_aggregates(db);

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== ALL CONCURRENT-WRITER TESTS PASSED ===\n");
    return 0;
}
