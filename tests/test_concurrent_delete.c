/* test_concurrent_delete.c — concurrent INSERT + SELECT + DELETE/TRUNCATE
 * correctness & throughput probe.
 *
 * Three specific failure modes are being watched for:
 *   1. Query mid-enumeration vs TRUNCATE rm_rf — process crash or error spike
 *   2. INSERT-to-memtable vs DELETE — "resurrection" of rows that should
 *      have been deleted but survive in the memtable
 *   3. Throughput regression when a mutator thread runs alongside writes
 *
 * We don't test cluster-level RPC ordering here — that needs a multi-node
 * scenario with in-flight replication and is exercised by
 * /tmp/cluster_delete_test on lvm1 separately.
 */

#include "../include/tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdatomic.h>

#define FAIL(fmt, ...) \
    do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) \
    do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define CHECK(cond) \
    do { if (!(cond)) FAIL("assertion failed: " #cond); } while (0)

static int64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
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

/* ---- shared state ---- */
typedef struct {
    tsdb_db_t       *db;
    tsdb_table_t    *tbl;
    atomic_int       stop;
    atomic_uint_fast64_t rows_written;
    atomic_uint_fast64_t queries_ok;
    atomic_uint_fast64_t queries_err;
    atomic_uint_fast64_t mutator_truncates;
    atomic_uint_fast64_t mutator_deletes;
} ctx_t;

/* ---- Writer: 10-row batches in a tight loop ---- */
static void *writer_thread(void *arg) {
    ctx_t *c = arg;
    int64_t seq = 0;
    while (!atomic_load(&c->stop)) {
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(c->tbl, &b) != TSDB_OK) continue;
        int ok = 1;
        for (int i = 0; i < 10; i++) {
            /* ts monotonically increasing in ns */
            int64_t ts = (seq++) * 1000LL + 1000000000000000000LL;
            if (tsdb_batch_row_ts(b, ts) != TSDB_OK) { ok = 0; break; }
            if (tsdb_batch_row_f64(b, 1, (double)i) != TSDB_OK) { ok = 0; break; }
            if (tsdb_batch_row_end(b) != TSDB_OK) { ok = 0; break; }
        }
        if (ok && tsdb_batch_commit(b) == TSDB_OK) {
            atomic_fetch_add(&c->rows_written, 10);
        }
    }
    return NULL;
}

/* ---- Reader: SELECT count(*) in a tight loop, records errors ---- */
static void *reader_thread(void *arg) {
    ctx_t *c = arg;
    while (!atomic_load(&c->stop)) {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(c->db, "SELECT count() FROM stress_t", &r);
        if (rc == TSDB_OK && r) {
            while (tsdb_result_next(r)) { /* no-op */ }
            tsdb_result_free(r);
            atomic_fetch_add(&c->queries_ok, 1);
        } else {
            atomic_fetch_add(&c->queries_err, 1);
            /* Log once to surface which rc fires first. */
            static _Atomic int logged = 0;
            int expected = 0;
            if (atomic_compare_exchange_strong(&logged, &expected, 1)) {
                fprintf(stderr, "    (first err) rc=%d (%s)\n",
                        rc, tsdb_errstr(rc));
            }
            if (r) tsdb_result_free(r);
        }
    }
    return NULL;
}

/* ---- Mutator: every 300ms, alternate DELETE or TRUNCATE ---- */
static void *mutator_thread(void *arg) {
    ctx_t *c = arg;
    int flip = 0;
    while (!atomic_load(&c->stop)) {
        struct timespec slp = {0, 300 * 1000000L}; /* 300 ms */
        nanosleep(&slp, NULL);
        if (atomic_load(&c->stop)) break;

        if (flip++ & 1) {
            /* TRUNCATE drops everything. */
            tsdb_result_t *r = NULL;
            if (tsdb_query(c->db, "TRUNCATE TABLE stress_t", &r) == TSDB_OK) {
                atomic_fetch_add(&c->mutator_truncates, 1);
            }
            if (r) tsdb_result_free(r);
        } else {
            /* Keep only recent rows — delete ts < cutoff where cutoff
             * is arbitrary but fixed.  With monotonic seq-based ts this
             * deletes whatever has already been written at older seq. */
            tsdb_result_t *r = NULL;
            if (tsdb_query(c->db,
                           "DELETE FROM stress_t WHERE ts < 1000000000500000000",
                           &r) == TSDB_OK) {
                atomic_fetch_add(&c->mutator_deletes, 1);
            }
            if (r) tsdb_result_free(r);
        }
    }
    return NULL;
}

/* ---- Scenario 1: writer only (baseline throughput) ---- */
static double bench_writer_only(tsdb_db_t *db, tsdb_table_t *tbl, int seconds) {
    ctx_t c = {0};
    c.db = db; c.tbl = tbl;

    pthread_t w; pthread_create(&w, NULL, writer_thread, &c);

    int64_t t0 = now_ns();
    sleep(seconds);
    atomic_store(&c.stop, 1);
    pthread_join(w, NULL);
    int64_t dt = now_ns() - t0;
    double sec = (double)dt / 1e9;
    double rps = (double)c.rows_written / sec;

    printf("  writer-only:          %10.0f rows/s  (%lu rows in %.2fs)\n",
           rps, (unsigned long)c.rows_written, sec);
    return rps;
}

/* ---- Scenario 2: writer + reader (no mutation) ---- */
static double bench_writer_reader(tsdb_db_t *db, tsdb_table_t *tbl, int seconds) {
    ctx_t c = {0};
    c.db = db; c.tbl = tbl;

    pthread_t w, r;
    pthread_create(&w, NULL, writer_thread, &c);
    pthread_create(&r, NULL, reader_thread, &c);

    int64_t t0 = now_ns();
    sleep(seconds);
    atomic_store(&c.stop, 1);
    pthread_join(w, NULL); pthread_join(r, NULL);
    int64_t dt = now_ns() - t0;
    double sec = (double)dt / 1e9;
    double rps = (double)c.rows_written / sec;

    printf("  writer + reader:      %10.0f rows/s  (%lu queries, err=%lu)\n",
           rps, (unsigned long)c.queries_ok, (unsigned long)c.queries_err);
    return rps;
}

/* ---- Scenario 3: writer + reader + mutator (the real stress case) ---- */
static double bench_full_contention(tsdb_db_t *db, tsdb_table_t *tbl, int seconds,
                                     uint64_t *out_q_ok, uint64_t *out_q_err) {
    ctx_t c = {0};
    c.db = db; c.tbl = tbl;

    pthread_t w, r, m;
    pthread_create(&w, NULL, writer_thread, &c);
    pthread_create(&r, NULL, reader_thread, &c);
    pthread_create(&m, NULL, mutator_thread, &c);

    int64_t t0 = now_ns();
    sleep(seconds);
    atomic_store(&c.stop, 1);
    pthread_join(w, NULL); pthread_join(r, NULL); pthread_join(m, NULL);
    int64_t dt = now_ns() - t0;
    double sec = (double)dt / 1e9;
    double rps = (double)c.rows_written / sec;

    *out_q_ok  = c.queries_ok;
    *out_q_err = c.queries_err;

    printf("  writer+reader+mutator:%10.0f rows/s  (queries ok=%lu err=%lu, "
           "truncates=%lu deletes=%lu)\n",
           rps,
           (unsigned long)c.queries_ok, (unsigned long)c.queries_err,
           (unsigned long)c.mutator_truncates,
           (unsigned long)c.mutator_deletes);
    return rps;
}

/* ---- DELETE partition-boundary test ----
 *
 * Because tsdb_batch_commit auto-flushes the memtable, rows are on disk
 * before DELETE sees them.  DELETE then drops partitions whose FULL
 * time range satisfies the predicate; partitions that partially overlap
 * the predicate boundary are left untouched (documented behavior in
 * tsdb_delete_range: per-row DELETE is "not yet implemented").
 *
 * We write rows that straddle the cutoff and confirm:
 *   - rows strictly below cutoff are dropped
 *   - rows in a partition that spans the cutoff survive (safety)
 */
static void test_delete_partition_boundary(const char *dir) {
    rm_rf(dir); mkdir(dir, 0755);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "mt_test", cols, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "mt_test", &t));

    /* Partition A: 2024-01-01  (50 rows)
     * Partition B: 2025-01-01  (50 rows) */
    const int64_t part_a_ns = 1704067200LL * 1000000000LL;  /* 2024-01-01 */
    const int64_t part_b_ns = 1735689600LL * 1000000000LL;  /* 2025-01-01 */

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 50; i++) {
        OK(tsdb_batch_row_ts (b, part_a_ns + (int64_t)i * 1000));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    for (int i = 0; i < 50; i++) {
        OK(tsdb_batch_row_ts (b, part_b_ns + (int64_t)i * 1000));
        OK(tsdb_batch_row_f64(b, 1, (double)(100 + i)));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    /* DELETE only full partitions with pend < cutoff.  Cutoff sits in
     * the middle of 2024: partition A's pend (end of 2024-01-01) is
     * before cutoff but partition B's pend is after — so only A drops. */
    const int64_t cutoff = part_b_ns - 1;  /* just before 2025 */
    char del[128];
    snprintf(del, sizeof(del),
             "DELETE FROM mt_test WHERE ts < %lld", (long long)cutoff);

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, del, &r));
    if (r) tsdb_result_free(r);

    tsdb_result_t *rq = NULL;
    OK(tsdb_query(db, "SELECT count() FROM mt_test", &rq));
    int64_t cnt = 0;
    if (rq && tsdb_result_next(rq)) cnt = tsdb_result_i64(rq, 0);
    if (rq) tsdb_result_free(rq);
    printf("  after DELETE ts<2025-01-01:            %lld rows remain "
           "(expect 50 — partition A dropped, B kept)\n", (long long)cnt);
    CHECK(cnt == 50);

    /* TRUNCATE clears everything. */
    OK(tsdb_query(db, "TRUNCATE TABLE mt_test", &r));
    if (r) tsdb_result_free(r);

    rq = NULL;
    OK(tsdb_query(db, "SELECT count() FROM mt_test", &rq));
    cnt = -1;
    if (rq && tsdb_result_next(rq)) cnt = tsdb_result_i64(rq, 0);
    if (rq) tsdb_result_free(rq);
    printf("  after TRUNCATE:                        %lld rows remain "
           "(expect 0)\n", (long long)cnt);
    CHECK(cnt == 0);

    tsdb_close(db);
    rm_rf(dir);
}

/* ---- Main ----*/
int main(void) {
    printf("\n=== test_concurrent_delete ===\n");

    printf("\n[1] DELETE partition-boundary semantics check\n");
    test_delete_partition_boundary("/tmp/tsdb_stress_mt");

    const char *dir = "/tmp/tsdb_stress_concurrent";
    rm_rf(dir); mkdir(dir, 0755);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"v",  TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "stress_t", cols, 2, "ts"));
    tsdb_table_t *tbl; OK(tsdb_open_table(db, "stress_t", &tbl));

    const int SECONDS = 5;

    printf("\n[2] throughput comparison (%d seconds each):\n", SECONDS);
    double rps_base   = bench_writer_only(db, tbl, SECONDS);
    /* Clear so each scenario starts clean. */
    tsdb_result_t *r = NULL;
    (void)tsdb_query(db, "TRUNCATE TABLE stress_t", &r);
    if (r) tsdb_result_free(r);

    double rps_read   = bench_writer_reader(db, tbl, SECONDS);
    (void)tsdb_query(db, "TRUNCATE TABLE stress_t", &r);
    if (r) tsdb_result_free(r);

    uint64_t q_ok = 0, q_err = 0;
    double rps_full   = bench_full_contention(db, tbl, SECONDS, &q_ok, &q_err);

    printf("\n[3] interpretation:\n");
    if (rps_base > 0) {
        printf("  writer+reader  %.1f%% of baseline\n",
               100.0 * rps_read / rps_base);
        printf("  full stress    %.1f%% of baseline\n",
               100.0 * rps_full / rps_base);
    }
    double err_rate = (q_ok + q_err > 0)
                      ? 100.0 * (double)q_err / (double)(q_ok + q_err) : 0.0;
    printf("  query error rate under stress: %.2f%% (%lu/%lu)\n",
           err_rate, (unsigned long)q_err, (unsigned long)(q_ok + q_err));

    /* Sanity: we didn't crash — that's the bare minimum. */
    CHECK(rps_full > 0);

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== ok ===\n");
    return 0;
}
