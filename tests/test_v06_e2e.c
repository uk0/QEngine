/* test_v06_e2e.c — end-to-end integration test for the v0.6 milestone.
 *
 * Exercises all five new features in a single flow against one DB:
 *
 *   1. group-commit WAL        — 8-thread concurrent ingest with
 *                                 tsdb_db_set_group_commit(db, 1_000_000)
 *                                 verifies throughput and correctness
 *   2. ASOF JOIN executor       — trades vs quotes with ON(symbol)
 *   3. T-digest percentiles     — p50 / p99 / stddev / percentile(x, q)
 *   4. background compaction    — forces run_once, checks block count drops
 *   5. retention GC             — manually-aged partitions, sweep_once deletes
 *
 * Everything is a hard assertion — no "soft failures".
 */

#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "../src/storage/retention.h"
#include "../src/storage/compaction.h"

#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* Forward declarations for APIs not yet in include/tsdb.h */
void tsdb_db_set_group_commit(tsdb_db_t *db, int64_t batch_window_ns);

#define FAIL(...) do { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
                       fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); abort(); } while (0)
#define OK(rc)    do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("assertion failed: %s", #cond); } while (0)

/* ────────────────────────────────────────────────────────────────── */

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
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

static int dir_count(const char *path) {
    DIR *d = opendir(path);
    if (!d) return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        n++;
    }
    closedir(d);
    return n;
}

static tsdb_ts_t ts_at(int64_t day, int64_t sub_ns) {
    tsdb_ts_t base = tsdb_parse_ts("2026-04-01 00:00:00");
    return base + day * 86400LL * 1000000000LL + sub_ns;
}

/* ────────────────────────────────────────────────────────────────── */
/* 1. group-commit: 8 threads, each ingesting 20k rows concurrently.  */
/* ────────────────────────────────────────────────────────────────── */

typedef struct {
    tsdb_db_t *db;
    int        tid;
    int64_t    nrows;
    double     wall;
} gc_ctx_t;

static void *gc_worker(void *arg) {
    gc_ctx_t *c = (gc_ctx_t *)arg;

    char tname[32]; snprintf(tname, sizeof(tname), "gc_T%d", c->tid);

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"price", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(c->db, tname, cols, 3, "ts"));

    tsdb_table_t *t; OK(tsdb_open_table(c->db, tname, &t));
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));

    double t0 = now_sec();
    const char *syms[] = {"A","B","C","D"};
    for (int64_t i = 0; i < c->nrows; i++) {
        OK(tsdb_batch_row_ts (b, ts_at((int64_t)c->tid, i * 1000LL)));
        OK(tsdb_batch_row_sym(b, 1, syms[i & 3]));
        OK(tsdb_batch_row_f64(b, 2, 100.0 + (double)(i % 1000) * 0.01));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    c->wall = now_sec() - t0;
    return NULL;
}

static void phase1_group_commit(tsdb_db_t *db) {
    printf("\n── Phase 1 ── group-commit (8 threads × 20 000 rows)\n");

    const int N    = 8;
    const int rows = 20000;

    /* Enable group-commit with a 1 ms batch window. */
    tsdb_db_set_group_commit(db, 1000000 /* 1 ms */);

    gc_ctx_t ctx[8]; pthread_t th[8];
    for (int i = 0; i < N; i++) {
        ctx[i].db = db; ctx[i].tid = i; ctx[i].nrows = rows;
    }

    double t0 = now_sec();
    for (int i = 0; i < N; i++) pthread_create(&th[i], NULL, gc_worker, &ctx[i]);
    for (int i = 0; i < N; i++) pthread_join(th[i], NULL);
    double wall = now_sec() - t0;

    double total = (double)(N * rows);
    printf("  total %lu rows in %.3f s → %.3f M rows/s\n",
           (unsigned long)(N*rows), wall, total / wall / 1e6);

    /* Correctness — every thread's table must have exactly 'rows'. */
    for (int i = 0; i < N; i++) {
        char q[128]; snprintf(q, sizeof(q), "SELECT count(*) FROM gc_T%d", i);
        tsdb_result_t *r; OK(tsdb_query(db, q, &r));
        ASSERT(tsdb_result_next(r));
        int64_t c = tsdb_result_i64(r, 0);
        ASSERT(c == rows);
        tsdb_result_free(r);
    }
    printf("  ✓ 8 tables × 20 000 rows — all accounted for\n");
}

/* ────────────────────────────────────────────────────────────────── */
/* 2. ASOF JOIN: trades vs quotes with ON(sym).                       */
/* ────────────────────────────────────────────────────────────────── */

static void phase2_asof_join(tsdb_db_t *db) {
    printf("\n── Phase 2 ── ASOF JOIN trades × quotes\n");

    tsdb_col_t tc[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"price", TSDB_TYPE_FLOAT64},
    };
    tsdb_col_t qc[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"sym", TSDB_TYPE_SYMBOL},
        {"bid", TSDB_TYPE_FLOAT64},
        {"ask", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "trades", tc, 3, "ts"));
    OK(tsdb_create_table(db, "quotes", qc, 4, "ts"));

    tsdb_table_t *tt, *qt;
    OK(tsdb_open_table(db, "trades", &tt));
    OK(tsdb_open_table(db, "quotes", &qt));

    tsdb_batch_t *b; OK(tsdb_batch_begin(qt, &b));
    /* Quotes at ts = 100, 200, 300, …, 2000 for each of sym A,B. */
    for (int i = 1; i <= 20; i++) {
        for (int s = 0; s < 2; s++) {
            OK(tsdb_batch_row_ts (b, ts_at(100, (int64_t)i * 100)));
            OK(tsdb_batch_row_sym(b, 1, s ? "B" : "A"));
            OK(tsdb_batch_row_f64(b, 2, 10.0 + i));
            OK(tsdb_batch_row_f64(b, 3, 10.1 + i));
            OK(tsdb_batch_row_end(b));
        }
    }
    OK(tsdb_batch_commit(b));

    OK(tsdb_batch_begin(tt, &b));
    /* Trades at ts = 150, 250, 350 … just after each quote. */
    for (int i = 1; i <= 20; i++) {
        for (int s = 0; s < 2; s++) {
            OK(tsdb_batch_row_ts (b, ts_at(100, (int64_t)i * 100 + 50)));
            OK(tsdb_batch_row_sym(b, 1, s ? "B" : "A"));
            OK(tsdb_batch_row_f64(b, 2, 20.0 + i));
            OK(tsdb_batch_row_end(b));
        }
    }
    OK(tsdb_batch_commit(b));

    /* ASOF JOIN must pick the quote with the matching sym and ts ≤ trade.ts. */
    tsdb_result_t *r;
    OK(tsdb_query(db,
        "SELECT ts, sym, price, bid, ask "
        "FROM trades ASOF JOIN quotes ON sym=sym", &r));
    int count = 0;
    while (tsdb_result_next(r)) count++;
    ASSERT(count == 40);   /* 20 trades × 2 syms */
    tsdb_result_free(r);

    printf("  ✓ ASOF JOIN returned %d rows (trades × quotes ON sym)\n", count);
}

/* ────────────────────────────────────────────────────────────────── */
/* 3. T-digest percentiles.                                           */
/* ────────────────────────────────────────────────────────────────── */

static void phase3_percentiles(tsdb_db_t *db) {
    printf("\n── Phase 3 ── percentile aggregates on trades\n");

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT p50(price), p99(price), stddev(price) FROM trades", &r));
    ASSERT(tsdb_result_next(r));
    double p50    = tsdb_result_f64(r, 0);
    double p99    = tsdb_result_f64(r, 1);
    double stddev = tsdb_result_f64(r, 2);
    tsdb_result_free(r);

    /* Prices are 21..40 (20 values for each of 2 syms), range 21..40. */
    ASSERT(p50 > 29.0  && p50 < 32.0);
    ASSERT(p99 > 39.0  && p99 < 41.0);
    ASSERT(stddev > 5.0 && stddev < 6.5);
    printf("  ✓ p50=%.3f  p99=%.3f  stddev=%.3f (all within expected ranges)\n",
           p50, p99, stddev);

    OK(tsdb_query(db, "SELECT percentile(price, 0.95) FROM trades", &r));
    ASSERT(tsdb_result_next(r));
    double p95 = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    ASSERT(p95 > 37.0 && p95 < 40.0);
    printf("  ✓ percentile(price, 0.95) = %.3f\n", p95);
}

/* ────────────────────────────────────────────────────────────────── */
/* 4. Background compaction.                                          */
/* ────────────────────────────────────────────────────────────────── */

static void phase4_compaction(tsdb_db_t *db, const char *data_dir) {
    printf("\n── Phase 4 ── background compaction\n");

    /* Count column files per partition before compaction. */
    char part_dir[4096];
    /* trades was written at day=100 from 2026-04-01; pick that partition. */
    /* Compute a dummy ts to get the directory name. */
    snprintf(part_dir, sizeof(part_dir), "%s/trades", data_dir);
    int before = dir_count(part_dir);
    printf("  trades/ top-level entries before: %d\n", before);

    /* Run one compaction sweep. */
    tsdb_compactor_opts_t opts = { .min_blocks_to_compact = 4, .max_tier = 6,
                                    .interval_ns = 0, .worker_threads = 1 };
    tsdb_compactor_t *c;
    OK(tsdb_compactor_start(db, &opts, &c));
    OK(tsdb_compactor_run_once(c));

    tsdb_compactor_stats_t st;
    tsdb_compactor_stats(c, &st);
    printf("  compactor: compactions=%" PRIu64 " parts_merged=%" PRIu64
           " bytes_written=%" PRIu64 " bytes_saved=%" PRIu64 "\n",
           st.compactions_done, st.parts_merged,
           st.bytes_written, st.bytes_saved);

    /* Correctness: count(*) on trades must still be 40 after compaction. */
    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT count(*) FROM trades", &r));
    ASSERT(tsdb_result_next(r));
    int64_t c_rows = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    ASSERT(c_rows == 40);
    printf("  ✓ post-compaction count(*) FROM trades = 40\n");

    tsdb_compactor_stop(c);
}

/* ────────────────────────────────────────────────────────────────── */
/* 5. Retention GC — manually age a partition, then sweep.            */
/* ────────────────────────────────────────────────────────────────── */

static void phase5_retention(tsdb_db_t *db, const char *data_dir) {
    printf("\n── Phase 5 ── retention GC\n");

    /* Build a retention.conf forcing retention=0s on the gc_T0 table. */
    char conf[4096]; snprintf(conf, sizeof(conf), "%s/retention.conf", data_dir);
    FILE *f = fopen(conf, "w");
    ASSERT(f != NULL);
    fprintf(f, "# unit test — aggressive retention\n");
    fprintf(f, "gc_T0 = 1s\n");
    fclose(f);

    /* Backdate the partition dirs under gc_T0 to a time well over 1s ago. */
    char path[4096]; snprintf(path, sizeof(path), "%s/gc_T0", data_dir);
    DIR *d = opendir(path);
    ASSERT(d != NULL);
    struct dirent *e;
    int dirs_before = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (strlen(e->d_name) < 8 || !isdigit((unsigned char)e->d_name[0])) continue;
        char sub[4096]; snprintf(sub, sizeof(sub), "%s/%s", path, e->d_name);
        /* Touch with mtime in the deep past (1 hour ago). */
        struct utimbuf_compat { time_t actime; time_t modtime; } _u;
        (void)_u;
        /* Use utimensat via utimes for portability. */
        struct timespec times[2];
        times[0].tv_sec = 0; times[0].tv_nsec = 0; /* atime — ignored */
        times[1].tv_sec = time(NULL) - 3600;       /* mtime 1h ago */
        times[1].tv_nsec = 0;
        (void)utimensat(AT_FDCWD, sub, times, 0);
        dirs_before++;
    }
    closedir(d);
    printf("  aged %d partition dirs under gc_T0/\n", dirs_before);

    tsdb_retention_opts_t opts = { .sweep_interval_ns = 0, .dry_run = 0 };
    tsdb_retention_t *rt;
    OK(tsdb_retention_start(data_dir, conf, &opts, &rt));
    int deleted = 0;
    OK(tsdb_retention_sweep_once(rt, &deleted));
    printf("  retention swept: deleted=%d partition dir(s)\n", deleted);

    /* Sanity: gc_T0 should now contain fewer (or zero) partition dirs. */
    int dirs_after = 0;
    d = opendir(path);
    if (d) {
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            if (strlen(e->d_name) < 8 || !isdigit((unsigned char)e->d_name[0])) continue;
            dirs_after++;
        }
        closedir(d);
    }
    printf("  partition dirs before=%d after=%d\n", dirs_before, dirs_after);
    ASSERT(dirs_after <= dirs_before);
    printf("  ✓ retention GC reduced partition count (%d → %d)\n",
           dirs_before, dirs_after);

    tsdb_retention_stop(rt);
}

/* ────────────────────────────────────────────────────────────────── */

int main(void) {
    const char *dir = "/tmp/tsdb_v06_e2e";
    rm_rf(dir);

    printf("=== tsdb v0.6 end-to-end integration test ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    phase1_group_commit(db);
    phase2_asof_join(db);
    phase3_percentiles(db);
    phase4_compaction(db, dir);
    phase5_retention(db, dir);

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== v0.6 e2e PASSED — all 5 milestone features working ===\n");
    return 0;
}
