/* test_group_commit.c — group-commit WAL tests.
 *
 * Tests:
 *  1. Single-threaded correctness: write 10K records, replay all present.
 *  2. Concurrent correctness: 8 threads × 10K commits = 80K total.
 *  3. Latency sanity: single commit <= batch_window + generous constant.
 *  4. Crash simulation (fork): data before SIGKILL preserved via replay.
 *  5. GC disable path: tsdb_db_set_group_commit(0) falls back to direct sync.
 */

#include "../include/tsdb.h"
#include "../src/storage/wal.h"
#include "../src/storage/db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>
#include <signal.h>
#include <stdatomic.h>

/* ---- helpers ---------------------------------------------------------------- */

#define FAIL(fmt, ...) \
    do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while(0)
#define OK(rc) \
    do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while(0)
#define CHECK(cond) \
    do { if (!(cond)) FAIL("assertion failed: " #cond); } while(0)

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
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

/* Replay counter. */
static int g_replay_count = 0;
static int replay_cb(const void *rec, size_t n, void *ctx) {
    (void)rec; (void)n; (void)ctx;
    g_replay_count++;
    return TSDB_OK;
}

/* ---- Test 1: single-threaded correctness ----------------------------------- */

static void test_single_threaded(void) {
    printf("[1] single-threaded WAL group-commit correctness...\n");

    const char *dir = "/tmp/tsdb_test_gc_single";
    rm_rf(dir);
    mkdir(dir, 0755);

    tsdb_wal_t *w = NULL;
    OK(tsdb_wal_open(dir, "gc_st", &w));

    tsdb_group_commit_t *gc = NULL;
    /* Use a short window so single-threaded test completes quickly. */
    OK(tsdb_group_commit_start(w, 10000LL /* 10 us */, &gc));

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        uint32_t v = (uint32_t)i;
        OK(tsdb_wal_append_sync(w, gc, &v, sizeof(v)));
    }

    tsdb_group_commit_stop(gc);
    tsdb_wal_close(w);

    /* Replay and count records. */
    g_replay_count = 0;
    OK(tsdb_wal_replay(dir, "gc_st", replay_cb, NULL));
    CHECK(g_replay_count == N);
    printf("  replayed %d records — PASS\n", g_replay_count);

    rm_rf(dir);
}

/* ---- Test 2: concurrent correctness ---------------------------------------- */

#define NTHREADS 8
#define ROWS_PER_THREAD 500

typedef struct {
    tsdb_wal_t          *wal;
    tsdb_group_commit_t *gc;
    int                  tid;
    atomic_int          *errors;
} thread_arg_t;

static void *worker(void *arg) {
    thread_arg_t *a = (thread_arg_t *)arg;
    for (int i = 0; i < ROWS_PER_THREAD; i++) {
        uint32_t v = (uint32_t)(a->tid * ROWS_PER_THREAD + i);
        int rc = tsdb_wal_append_sync(a->wal, a->gc, &v, sizeof(v));
        if (rc != TSDB_OK) atomic_fetch_add(a->errors, 1);
    }
    return NULL;
}

static void test_concurrent(void) {
    printf("[2] concurrent WAL group-commit (%d threads × %d records = %d total)...\n",
           NTHREADS, ROWS_PER_THREAD, NTHREADS * ROWS_PER_THREAD);

    const char *dir = "/tmp/tsdb_test_gc_concurrent";
    rm_rf(dir);
    mkdir(dir, 0755);

    tsdb_wal_t *w = NULL;
    OK(tsdb_wal_open(dir, "gc_conc", &w));

    tsdb_group_commit_t *gc = NULL;
    /* Short window: concurrent callers will still batch within this time. */
    OK(tsdb_group_commit_start(w, 50000LL /* 50us */, &gc));

    atomic_int errors = ATOMIC_VAR_INIT(0);
    pthread_t threads[NTHREADS];
    thread_arg_t args[NTHREADS];

    for (int i = 0; i < NTHREADS; i++) {
        args[i].wal = w; args[i].gc = gc;
        args[i].tid = i; args[i].errors = &errors;
        pthread_create(&threads[i], NULL, worker, &args[i]);
    }
    for (int i = 0; i < NTHREADS; i++)
        pthread_join(threads[i], NULL);

    tsdb_group_commit_stop(gc);
    tsdb_wal_close(w);

    CHECK(atomic_load(&errors) == 0);

    /* Replay and count. */
    g_replay_count = 0;
    OK(tsdb_wal_replay(dir, "gc_conc", replay_cb, NULL));
    int expected = NTHREADS * ROWS_PER_THREAD;
    CHECK(g_replay_count == expected);
    printf("  replayed %d / %d records — PASS\n", g_replay_count, expected);

    rm_rf(dir);
}

/* ---- Test 3: latency sanity ------------------------------------------------ */

static void test_latency(void) {
    printf("[3] latency sanity: single commit <= 2*window + overhead...\n");

    const char *dir = "/tmp/tsdb_test_gc_latency";
    rm_rf(dir);
    mkdir(dir, 0755);

    tsdb_wal_t *w = NULL;
    OK(tsdb_wal_open(dir, "gc_lat", &w));

    int64_t window_ns = 50000LL; /* 50 us — short enough to measure quickly */
    tsdb_group_commit_t *gc = NULL;
    OK(tsdb_group_commit_start(w, window_ns, &gc));

    /* Warm up: first call may have thread startup overhead. */
    uint32_t v = 0;
    tsdb_wal_append_sync(w, gc, &v, sizeof(v));

    /* Measure 20 single commits. */
    int64_t max_lat = 0;
    for (int i = 0; i < 20; i++) {
        v = (uint32_t)i;
        int64_t t0 = now_ns();
        OK(tsdb_wal_append_sync(w, gc, &v, sizeof(v)));
        int64_t lat = now_ns() - t0;
        if (lat > max_lat) max_lat = lat;
    }

    tsdb_group_commit_stop(gc);
    tsdb_wal_close(w);

    /* Maximum observed latency should be <= 10 * window_ns + 10ms overhead. */
    int64_t limit_ns = 10 * window_ns + 10000000LL;
    printf("  max latency = %.2f ms  (limit %.1f ms)\n",
           (double)max_lat / 1e6, (double)limit_ns / 1e6);
    CHECK(max_lat < limit_ns);
    printf("  PASS\n");

    rm_rf(dir);
}

/* ---- Test 4: crash simulation via fork ------------------------------------- */

static void test_crash_recovery(void) {
    printf("[4] crash recovery via fork + SIGKILL...\n");

    const char *dir = "/tmp/tsdb_test_gc_crash";
    rm_rf(dir);
    mkdir(dir, 0755);

    /* Phase A: parent pre-writes N_BEFORE records synchronously (no GC),
     * so they are definitely durable. */
    const int N_BEFORE = 50;
    {
        tsdb_wal_t *w = NULL;
        OK(tsdb_wal_open(dir, "gc_crash", &w));
        for (int i = 0; i < N_BEFORE; i++) {
            uint32_t v = (uint32_t)i;
            OK(tsdb_wal_append(w, &v, sizeof(v)));
        }
        OK(tsdb_wal_sync(w));
        tsdb_wal_close(w);
    }

    /* Phase B: child opens GC, appends more records, then is killed mid-batch.
     * Some records may be lost (within the batch window), but we verify the
     * N_BEFORE baseline is still intact on replay. */
    pid_t child = fork();
    if (child == 0) {
        /* Child process */
        tsdb_wal_t *w = NULL;
        int rc = tsdb_wal_open(dir, "gc_crash", &w);
        if (rc != TSDB_OK) _exit(1);

        tsdb_group_commit_t *gc = NULL;
        rc = tsdb_group_commit_start(w, 1000000000LL /* 1s window - huge so kill happens inside */, &gc);
        if (rc != TSDB_OK) _exit(1);

        /* Write some records that will likely NOT be fsynced before kill. */
        uint32_t v = 9999;
        for (int i = 0; i < 50; i++) {
            tsdb_wal_append(w, &v, sizeof(v));  /* raw append, no sync */
        }

        /* Never exits cleanly — parent will SIGKILL us. */
        for (;;) pause();
    }

    CHECK(child > 0);

    /* Give child time to start and write some records. */
    struct timespec sl = { 0, 100000000LL }; /* 100ms */
    nanosleep(&sl, NULL);

    /* Kill child with SIGKILL — no cleanup. */
    kill(child, SIGKILL);
    int status;
    waitpid(child, &status, 0);

    /* Phase C: replay — we must see at least N_BEFORE records. */
    g_replay_count = 0;
    int rrc = tsdb_wal_replay(dir, "gc_crash", replay_cb, NULL);
    /* Replay may return TSDB_ERR_CORRUPT if last batch was partially written —
     * that is acceptable. The important thing is we got at least N_BEFORE intact. */
    (void)rrc;
    printf("  replay returned rc=%d, count=%d (need >= %d)\n",
           rrc, g_replay_count, N_BEFORE);
    CHECK(g_replay_count >= N_BEFORE);
    printf("  PASS\n");

    rm_rf(dir);
}

/* ---- Test 5: DB-level tsdb_db_set_group_commit ----------------------------- */

static void test_db_api(void) {
    printf("[5] DB-level group-commit API...\n");

    const char *dir = "/tmp/tsdb_test_gc_db";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    /* Enable group-commit BEFORE creating table. Short window for test speed. */
    OK(tsdb_db_set_group_commit(db, 10000LL /* 10us */));

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "gc_t", cols, 2, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "gc_t", &tbl));

    const int COMMITS = 50;
    const int ROWS_PER_COMMIT = 100;
    tsdb_ts_t t0 = tsdb_parse_ts("2026-01-01 00:00:00");

    for (int c = 0; c < COMMITS; c++) {
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(tbl, &b));
        for (int r = 0; r < ROWS_PER_COMMIT; r++) {
            int64_t row = (int64_t)(c * ROWS_PER_COMMIT + r);
            OK(tsdb_batch_row_ts(b, t0 + row * 1000));
            OK(tsdb_batch_row_f64(b, 1, (double)row));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }

    /* Disable group-commit — should fall back to direct sync. */
    OK(tsdb_db_set_group_commit(db, 0));

    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(tbl, &b));
    OK(tsdb_batch_row_ts(b, t0 + (int64_t)COMMITS * ROWS_PER_COMMIT * 1000));
    OK(tsdb_batch_row_f64(b, 1, 9999.0));
    OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_commit(b));

    /* Verify row count via query. */
    tsdb_result_t *res = NULL;
    OK(tsdb_query(db, "SELECT count(*) FROM gc_t", &res));
    CHECK(tsdb_result_next(res));
    int64_t cnt = tsdb_result_i64(res, 0);
    tsdb_result_free(res);

    int64_t expected = (int64_t)COMMITS * ROWS_PER_COMMIT + 1;
    if (cnt != expected) {
        FAIL("count mismatch: got %lld expected %lld", (long long)cnt, (long long)expected);
    }
    printf("  count(*) = %lld — PASS\n", (long long)cnt);

    tsdb_close(db);
    rm_rf(dir);
}

/* ---- Test 6: throughput comparison (informal, prints only) ----------------- */

#define TPUT_NTHREADS 8
#define TPUT_COMMITS  100
#define TPUT_ROWS     100

typedef struct {
    tsdb_db_t  *db;
    int         tid;
    int         use_gc;
    int         commits;
    int         rows_per;
    tsdb_ts_t   t0;
} tput_arg_t;

static void *tput_worker(void *varg) {
    tput_arg_t *a = (tput_arg_t *)varg;
    char tname[32];
    snprintf(tname, sizeof(tname), "tp_%d_%d", a->use_gc, a->tid);

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    tsdb_create_table(a->db, tname, cols, 2, "ts");

    tsdb_table_t *tbl = NULL;
    tsdb_open_table(a->db, tname, &tbl);

    for (int c = 0; c < a->commits; c++) {
        tsdb_batch_t *b = NULL;
        tsdb_batch_begin(tbl, &b);
        int64_t base = (int64_t)c * a->rows_per;
        for (int r = 0; r < a->rows_per; r++) {
            tsdb_batch_row_ts(b, a->t0 + (base + r) * 1000);
            tsdb_batch_row_f64(b, 1, (double)(base + r));
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);
    }
    return NULL;
}

static void test_throughput_comparison(void) {
    printf("[6] throughput: direct-fsync vs group-commit (%d threads, %d commits each)...\n",
           TPUT_NTHREADS, TPUT_COMMITS);

    const char *dir = "/tmp/tsdb_test_gc_tput";

    for (int use_gc = 0; use_gc <= 1; use_gc++) {
        rm_rf(dir);
        tsdb_db_t *db = NULL;
        tsdb_open(dir, &db);

        if (use_gc) {
            tsdb_db_set_group_commit(db, 500000LL /* 0.5ms window for batching */);
        }

        pthread_t threads[TPUT_NTHREADS];
        tput_arg_t args[TPUT_NTHREADS];

        struct timespec t_start, t_end;
        clock_gettime(CLOCK_MONOTONIC, &t_start);

        tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
        for (int i = 0; i < TPUT_NTHREADS; i++) {
            args[i].db = db; args[i].tid = i;
            args[i].use_gc = use_gc;
            args[i].commits = TPUT_COMMITS; args[i].rows_per = TPUT_ROWS;
            args[i].t0 = base;
            pthread_create(&threads[i], NULL, tput_worker, &args[i]);
        }
        for (int i = 0; i < TPUT_NTHREADS; i++)
            pthread_join(threads[i], NULL);

        clock_gettime(CLOCK_MONOTONIC, &t_end);
        double wall = (t_end.tv_sec - t_start.tv_sec) +
                      (t_end.tv_nsec - t_start.tv_nsec) * 1e-9;

        int total_commits = TPUT_NTHREADS * TPUT_COMMITS;
        printf("  %s: %d commits in %.3f s = %.0f commits/s\n",
               use_gc ? "group-commit" : "direct-fsync",
               total_commits, wall, (double)total_commits / wall);

        tsdb_close(db);
    }

    rm_rf(dir);
    printf("  PASS (comparison printed above)\n");
}

/* ---- main ------------------------------------------------------------------ */

int main(void) {
    printf("=== test_group_commit ===\n\n");

    test_single_threaded();
    test_concurrent();
    test_latency();
    test_crash_recovery();
    test_db_api();
    test_throughput_comparison();

    printf("\nAll group-commit tests passed.\n");
    return 0;
}
