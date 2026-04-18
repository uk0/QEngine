/* test_v09_e2e.c — v0.9 milestone end-to-end integration test.
 *
 * Exercises every v0.9 feature in a single flow, using production
 * entry points (no internal shortcuts):
 *
 *   Phase 1 — STable DDL via QTL + PARTITION BY tbname
 *   Phase 2 — InfluxDB Line Protocol ingest (parse + SQL count)
 *   Phase 3 — PromQL subset compilation
 *   Phase 4 — Prometheus /metrics self-telemetry (counter + histogram)
 *   Phase 5 — Parallel GROUP BY hash-aggregate
 *   Phase 6 — INTERP linear interpolation
 *
 * All phases assert-hard; the test exits non-zero on any failure.
 */

#include "tsdb.h"
#include "../src/catalog/stable.h"
#include "../src/server/influx_line.h"
#include "../src/server/promql.h"
#include "../src/server/metrics.h"

struct tsdb_catalog;
struct tsdb_catalog *tsdb_db_catalog(tsdb_db_t *db);

#include <assert.h>
#include <dirent.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FAIL(...) do { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
                       fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); abort(); } while (0)
#define OK(rc)    do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("assertion failed: %s", #c); } while (0)

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

/* Minimal SQL helper: run and discard result, assert OK. */
static void run_sql(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("sql '%s' failed: rc=%d (%s)", sql, rc, tsdb_errstr(rc));
    if (r) tsdb_result_free(r);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 1 — STable DDL + PARTITION BY tbname                         */
/* ────────────────────────────────────────────────────────────────── */

static void phase1_stable_ddl(tsdb_db_t *db) {
    printf("\n── Phase 1 ── STable DDL + PARTITION BY tbname\n");

    run_sql(db,
        "CREATE STABLE meters ("
        "  ts TIMESTAMP, current FLOAT64, voltage INT64"
        ") TAGS (loc SYMBOL, gid INT64);");
    run_sql(db, "CREATE TABLE d1 USING meters TAGS ('east', 1);");
    run_sql(db, "CREATE TABLE d2 USING meters TAGS ('west', 2);");

    /* Bulk-insert via batch API (no INSERT DML yet in QTL). */
    const char *dev[] = {"d1", "d2"};
    int rows_per[] = {5, 3};
    for (int i = 0; i < 2; i++) {
        tsdb_table_t *t; OK(tsdb_open_table(db, dev[i], &t));
        tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
        for (int k = 0; k < rows_per[i]; k++) {
            OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(1000000000LL * (i * 100 + k + 1))));
            OK(tsdb_batch_row_f64(b, 1, 10.0 + k));
            OK(tsdb_batch_row_i64(b, 2, 220 + k));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }

    /* Union query: total rows 5+3=8. */
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT count(*) FROM meters;", &r));
    ASSERT(tsdb_result_next(r));
    int64_t total = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    ASSERT(total == 8);

    /* PARTITION BY tbname: 2 rows, one per child. */
    OK(tsdb_query(db, "SELECT count(*) FROM meters PARTITION BY tbname;", &r));
    int n = 0; int saw_d1 = 0, saw_d2 = 0;
    while (tsdb_result_next(r)) {
        const char *name = tsdb_result_sym(r, 0);
        int64_t cnt = tsdb_result_i64(r, 1);
        if (name && !strcmp(name, "d1")) { ASSERT(cnt == 5); saw_d1 = 1; }
        if (name && !strcmp(name, "d2")) { ASSERT(cnt == 3); saw_d2 = 1; }
        n++;
    }
    tsdb_result_free(r);
    ASSERT(n == 2 && saw_d1 && saw_d2);
    printf("  ✓ stable union=%" PRId64 ", PARTITION BY tbname → 2 rows (d1=5, d2=3)\n", total);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 2 — InfluxDB Line Protocol ingest                            */
/* ────────────────────────────────────────────────────────────────── */

static void phase2_influx_lp(tsdb_db_t *db) {
    printf("\n── Phase 2 ── InfluxDB Line Protocol ingest\n");

    const char *body =
        "cpu,host=a,region=us usage=0.42 1234567890000000000\n"
        "cpu,host=b,region=us usage=0.71 1234567890000000001\n"
        "cpu,host=a,region=eu usage=0.55 1234567890000000002\n";

    size_t lines = 0, errors = 0;
    OK(tsdb_influx_ingest(db, body, strlen(body), &lines, &errors));
    ASSERT(lines == 3);
    ASSERT(errors == 0);

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT count(*) FROM cpu;", &r));
    ASSERT(tsdb_result_next(r));
    int64_t cnt = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    ASSERT(cnt == 3);
    printf("  ✓ 3 LP lines → cpu table, count=%" PRId64 "\n", cnt);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 3 — PromQL subset compilation                                */
/* ────────────────────────────────────────────────────────────────── */

static void phase3_promql(void) {
    printf("\n── Phase 3 ── PromQL subset compilation\n");
    char err[256] = {0};

    /* bare metric */
    char *qtl = tsdb_promql_compile("cpu", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl && strstr(qtl, "SELECT ts, value FROM cpu"));
    free(qtl);

    /* rate() → derivative */
    qtl = tsdb_promql_compile("rate(cpu[5m])", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl && strstr(qtl, "derivative(value)"));
    free(qtl);

    /* sum by (label) → GROUP BY */
    qtl = tsdb_promql_compile("sum by (region) (cpu)", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl && strstr(qtl, "GROUP BY region"));
    free(qtl);

    /* labels → WHERE */
    qtl = tsdb_promql_compile("cpu{host=\"a\"}", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl && strstr(qtl, "WHERE host = 'a'"));
    free(qtl);
    printf("  ✓ bare metric, rate(), sum by(), label → QTL\n");
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 4 — Prometheus /metrics self-telemetry                       */
/* ────────────────────────────────────────────────────────────────── */

static void phase4_metrics(void) {
    printf("\n── Phase 4 ── Prometheus /metrics self-telemetry\n");
    tsdb_metrics_init();

    for (int i = 0; i < 50; i++) tsdb_metric_inc("qengine_queries_total");
    tsdb_metric_add("qengine_rows_written_total", 1000);
    tsdb_metric_gauge_set("qengine_memtable_rows", 4242);
    for (int i = 1; i <= 10; i++) tsdb_metric_observe("qengine_query_duration_ms", (double)i);

    size_t len = 0;
    char *out = tsdb_metrics_render(&len);
    ASSERT(out && len > 0);
    ASSERT(strstr(out, "qengine_queries_total 50"));
    ASSERT(strstr(out, "qengine_rows_written_total 1000"));
    ASSERT(strstr(out, "qengine_memtable_rows 4242"));
    ASSERT(strstr(out, "qengine_query_duration_ms_bucket"));
    ASSERT(strstr(out, "# HELP"));
    ASSERT(strstr(out, "# TYPE"));
    free(out);
    printf("  ✓ counters, gauge, histogram, HELP+TYPE lines present\n");
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 5 — Parallel GROUP BY                                        */
/* ────────────────────────────────────────────────────────────────── */

static void phase5_parallel_groupby(tsdb_db_t *db) {
    printf("\n── Phase 5 ── Parallel GROUP BY hash-aggregate\n");

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"rgn", TSDB_TYPE_SYMBOL},
        {"val",    TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "obs", cols, 3, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "obs", &t));

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    const char *regs[] = {"us", "eu", "ap"};
    int per = 5000;
    for (int r = 0; r < 3; r++) {
        for (int i = 0; i < per; i++) {
            OK(tsdb_batch_row_ts (b, (tsdb_ts_t)(1000000000LL * (r * per + i + 1))));
            OK(tsdb_batch_row_sym(b, 1, regs[r]));
            OK(tsdb_batch_row_f64(b, 2, (double)i));
            OK(tsdb_batch_row_end(b));
        }
    }
    OK(tsdb_batch_commit(b));

    tsdb_result_t *r = NULL;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    OK(tsdb_query(db, "SELECT rgn, count(*), sum(val) FROM obs GROUP BY rgn;", &r));
    clock_gettime(CLOCK_MONOTONIC, &t1);

    int groups = 0; int64_t tot = 0;
    while (tsdb_result_next(r)) {
        groups++;
        tot += tsdb_result_i64(r, 1);
    }
    tsdb_result_free(r);

    int64_t us = (t1.tv_sec - t0.tv_sec) * 1000000LL + (t1.tv_nsec - t0.tv_nsec) / 1000LL;
    ASSERT(groups == 3);
    ASSERT(tot == 3LL * per);
    printf("  ✓ 3 groups, sum(count)=%" PRId64 ", 15K rows in %" PRId64 " µs\n", tot, us);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 6 — INTERP linear interpolation                              */
/* ────────────────────────────────────────────────────────────────── */

static void phase6_interp(tsdb_db_t *db) {
    printf("\n── Phase 6 ── INTERP linear interpolation\n");

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "ip", cols, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "ip", &t));

    /* Sparse points: t=0 v=0 ; t=4s v=40 ; t=8s v=80. Interp to 1s grid → 9 rows. */
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    OK(tsdb_batch_row_ts(b, (tsdb_ts_t)0));                    OK(tsdb_batch_row_f64(b, 1, 0.0));  OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_row_ts(b, (tsdb_ts_t)4000000000LL));         OK(tsdb_batch_row_f64(b, 1, 40.0)); OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_row_ts(b, (tsdb_ts_t)8000000000LL));         OK(tsdb_batch_row_f64(b, 1, 80.0)); OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_commit(b));

    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, "SELECT ts, interp(val, 1000000000) FROM ip;", &r));
    int rows = 0;
    double first_interp = -1, mid_interp = -1, last_interp = -1;
    while (tsdb_result_next(r)) {
        double v = tsdb_result_f64(r, 1);
        if (rows == 0) first_interp = v;
        if (rows == 4) mid_interp = v;   /* t=4s sample point */
        last_interp = v;
        rows++;
    }
    tsdb_result_free(r);

    ASSERT(rows == 9);
    ASSERT(fabs(first_interp - 0.0)  < 1e-6);
    ASSERT(fabs(mid_interp   - 40.0) < 1e-6);
    ASSERT(fabs(last_interp  - 80.0) < 1e-6);
    printf("  ✓ interp grid 1s → 9 rows, endpoints 0/40/80 exact\n");
}

/* ────────────────────────────────────────────────────────────────── */

int main(void) {
    const char *dir = "/tmp/tsdb_v09_e2e";
    rm_rf(dir);

    printf("=== tsdb v0.9 end-to-end integration test ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    phase1_stable_ddl(db);
    phase2_influx_lp(db);
    phase3_promql();
    phase4_metrics();
    phase5_parallel_groupby(db);
    phase6_interp(db);

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== v0.9 e2e PASSED — six milestone features verified ===\n");
    return 0;
}
