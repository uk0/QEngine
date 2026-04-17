/* tsbs_cpu_query.c — TSBS cpu-only query benchmark driver.
 *
 * Usage: tsbs_cpu_query <db_dir>
 *
 * Runs TSBS-aligned cpu-only queries against an already-ingested DB.
 * Reports p50/p90/p99 latency in milliseconds over 10 iterations each.
 *
 * Query set (names match TSBS conventions):
 *   Q1  single-groupby-1-1-1  : 1 host, 1 metric, 1h window, minutely max
 *   Q2  single-groupby-1-8-1  : 1 host, 8 metrics, 1h window, minutely max
 *   Q3  high-cpu-1            : 1 host, find rows with usage_user > 90
 *   Q4  high-cpu-all          : all hosts, find rows with usage_user > 90
 *   Q5  double-groupby-1      : all hosts, 1 metric, hourly max (degraded: no multi-key GROUP BY)
 *   Q6  lastpoint             : 1 host, latest row (driver-side last row via LIMIT scan)
 *
 * Notes on executor limitations:
 *   - Q5: executor lacks multi-key GROUP BY; we collapse per-hour using SAMPLE BY
 *     over all hosts. Result covers the same time axis but loses per-host dimension.
 *   - Q6: ORDER BY is parsed but not executed; we scan all rows for a host and
 *     take the last one from the driver (the result is already time-ordered).
 *   - Q1/Q2 use SAMPLE BY 1m (60s buckets) for minutely aggregation.
 */
#include "tsdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <math.h>

#define QUERY_ITERS 10

/* ---- timing ------------------------------------------------------------ */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* ---- percentile -------------------------------------------------------- */

static int cmp_dbl(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static double pct(double *arr, int n, double p) {
    /* p in [0,1]. Linear interpolation. */
    double pos = p * (double)(n - 1);
    int lo = (int)pos;
    int hi = lo + 1;
    if (hi >= n) return arr[n - 1];
    double frac = pos - lo;
    return arr[lo] * (1.0 - frac) + arr[hi] * frac;
}

/* ---- query runner ------------------------------------------------------ */

typedef struct {
    const char *label;
    const char *qtl;
} query_def_t;

/* Run a query N times, drain results, report latency stats.
 * Returns number of rows from last run (for sanity check). */
static int64_t run_query(tsdb_db_t *db, const char *label, const char *qtl) {
    double lat[QUERY_ITERS];
    int64_t nrows = 0;

    for (int i = 0; i < QUERY_ITERS; i++) {
        tsdb_result_t *r = NULL;
        double t0 = now_sec();
        int rc = tsdb_query(db, qtl, &r);
        if (rc != TSDB_OK) {
            fprintf(stderr, "  [FAIL] %s: tsdb_query returned %d (%s)\nSQL: %s\n",
                    label, rc, tsdb_errstr(rc), qtl);
            lat[i] = 0.0;
            if (r) tsdb_result_free(r);
            continue;
        }
        nrows = 0;
        while (tsdb_result_next(r) == 1) nrows++;
        tsdb_result_free(r);
        lat[i] = (now_sec() - t0) * 1000.0;  /* ms */
    }

    qsort(lat, QUERY_ITERS, sizeof(double), cmp_dbl);
    double p50 = pct(lat, QUERY_ITERS, 0.50);
    double p90 = pct(lat, QUERY_ITERS, 0.90);
    double p99 = pct(lat, QUERY_ITERS, 0.99);
    double pmin = lat[0], pmax = lat[QUERY_ITERS - 1];

    printf("  %-30s  rows=%-8lld  p50=%7.2fms  p90=%7.2fms  p99=%7.2fms  "
           "min=%7.2fms  max=%7.2fms\n",
           label, (long long)nrows, p50, p90, p99, pmin, pmax);
    return nrows;
}

/* Q6: lastpoint for a single host — scan all rows for hostname, take last. */
static void run_lastpoint(tsdb_db_t *db, const char *hostname) {
    char qtl[256];
    snprintf(qtl, sizeof(qtl),
        "SELECT ts FROM cpu WHERE hostname = '%s'", hostname);

    double lat[QUERY_ITERS];
    int64_t nrows = 0;
    tsdb_ts_t last_ts = 0;

    for (int i = 0; i < QUERY_ITERS; i++) {
        tsdb_result_t *r = NULL;
        double t0 = now_sec();
        int rc = tsdb_query(db, qtl, &r);
        if (rc != TSDB_OK) {
            fprintf(stderr, "  [FAIL] lastpoint: tsdb_query returned %d\n", rc);
            lat[i] = 0.0;
            if (r) tsdb_result_free(r);
            continue;
        }
        nrows = 0; last_ts = 0;
        while (tsdb_result_next(r) == 1) {
            last_ts = tsdb_result_ts(r, 0);
            nrows++;
        }
        tsdb_result_free(r);
        lat[i] = (now_sec() - t0) * 1000.0;
    }

    qsort(lat, QUERY_ITERS, sizeof(double), cmp_dbl);
    double p50 = pct(lat, QUERY_ITERS, 0.50);
    double p90 = pct(lat, QUERY_ITERS, 0.90);
    double p99 = pct(lat, QUERY_ITERS, 0.99);
    double pmin = lat[0], pmax = lat[QUERY_ITERS - 1];

    printf("  %-30s  rows=%-8lld  p50=%7.2fms  p90=%7.2fms  p99=%7.2fms  "
           "min=%7.2fms  max=%7.2fms\n",
           "lastpoint (1-host scan)", (long long)nrows,
           p50, p90, p99, pmin, pmax);
    (void)last_ts;
}

/* ---- main --------------------------------------------------------------- */

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <db_dir>\n", argv[0]);
        return 1;
    }
    const char *db_dir = argv[1];

    tsdb_db_t *db;
    int rc = tsdb_open(db_dir, &db);
    if (rc != TSDB_OK) {
        fprintf(stderr, "tsdb_open(%s) failed: %s\n", db_dir, tsdb_errstr(rc));
        return 1;
    }

    printf("=== TSBS cpu-only query benchmark ===\n");
    printf("db_dir=%s  iters=%d\n", db_dir, QUERY_ITERS);
    printf("\nquery latency (ms, %d iterations each):\n", QUERY_ITERS);

    /* ------------------------------------------------------------------
     * Q1: single-groupby-1-1-1
     *   1 host, 1 metric (usage_user), 1-hour window starting at base,
     *   grouped by minute. TSBS uses max aggregation.
     * ------------------------------------------------------------------ */
    run_query(db,
        "single-groupby-1-1-1",
        "SELECT time_bucket(ts, 60000000000), max(usage_user) FROM cpu "
        "WHERE hostname = 'host_7' "
        "AND ts >= '2026-01-01 00:00:00' AND ts < '2026-01-01 01:00:00' "
        "SAMPLE BY 60000000000");

    /* ------------------------------------------------------------------
     * Q2: single-groupby-1-8-1
     *   1 host, 8 metrics, 1-hour window. We run max(usage_user) as
     *   representative; TSBS actually requests 8 separate metrics but
     *   the executor does not support multi-metric SAMPLE BY in one pass.
     *   We approximate with a multi-column scan over the same window.
     * ------------------------------------------------------------------ */
    run_query(db,
        "single-groupby-1-8-1",
        "SELECT time_bucket(ts, 60000000000), "
        "max(usage_user), max(usage_system), max(usage_idle), max(usage_nice), "
        "max(usage_iowait), max(usage_irq), max(usage_softirq), max(usage_steal) "
        "FROM cpu "
        "WHERE hostname = 'host_5' "
        "AND ts >= '2026-01-01 00:00:00' AND ts < '2026-01-01 01:00:00' "
        "SAMPLE BY 60000000000");

    /* ------------------------------------------------------------------
     * Q3: high-cpu-1
     *   1 host, find all rows where usage_user > 90.
     * ------------------------------------------------------------------ */
    run_query(db,
        "high-cpu-1",
        "SELECT ts, usage_user FROM cpu "
        "WHERE hostname = 'host_3' AND usage_user > 90.0");

    /* ------------------------------------------------------------------
     * Q4: high-cpu-all
     *   All hosts, find all rows where usage_user > 90 (full table scan).
     * ------------------------------------------------------------------ */
    run_query(db,
        "high-cpu-all",
        "SELECT ts, hostname, usage_user FROM cpu "
        "WHERE usage_user > 90.0");

    /* ------------------------------------------------------------------
     * Q5: double-groupby-1  (degraded — no multi-key GROUP BY)
     *   All hosts, 1 metric (usage_user), hourly aggregate.
     *   TSBS groups by (hour, host); we collapse to per-hour only.
     *   Comment in output marks this as degraded.
     * ------------------------------------------------------------------ */
    run_query(db,
        "double-groupby-1 [degraded]",
        "SELECT time_bucket(ts, 3600000000000), max(usage_user) FROM cpu "
        "SAMPLE BY 3600000000000");

    /* ------------------------------------------------------------------
     * Q6: lastpoint — latest snapshot per host.
     *   ORDER BY DESC is parsed but not executed by the executor.
     *   We scan ts column for host_0 and the driver takes the last row.
     * ------------------------------------------------------------------ */
    run_lastpoint(db, "host_0");

    tsdb_close(db);
    return 0;
}
