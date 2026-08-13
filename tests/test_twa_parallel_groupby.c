/* test_twa_parallel_groupby.c — twa() under GROUP BY must not change answer
 * when the query runs in parallel.
 *
 * THE BUG.  exec_group_by's parallel gate had no exclusion for time-weighted
 * aggregates.  Each worker accumulated twa_wsum over its own slice of scan
 * sources and the merge simply added them together:
 *
 *     case PROJ_AGG_TS_TWA:
 *         dp->twa_wsum += sp->twa_wsum;
 *         / * TWA merge is approximate (no overlap handling) * /
 *
 * A time-weighted average is Σ(value · dt) / total_dt.  Splitting the rows
 * across workers drops every dt that BRIDGES two slices — the interval from the
 * last row of one partition to the first row of the next contributes nothing —
 * while the divisor still spans the full range.  The result is therefore
 * systematically LOW by exactly the share of total time that falls in those
 * inter-partition gaps: small for dense data (this test, 200 rows/day, measures
 * 99.4996 against a true 100.0) and arbitrarily large for sparse series where
 * the gaps dominate — so a metering job validated on one dense partition can
 * bill from a badly wrong twa in production.  The whole-query parallel gate
 * already refused ts-aggregates for exactly this reason (`!has_ts_agg`); the
 * GROUP BY gate did not.
 *
 * THE FIX.  Exclude twa from the GROUP BY parallel gate.  Only twa: first(),
 * last() and last_row() merge by comparing ts_first/ts_last and pick the true
 * extreme, so they stay exact under the split and stay parallel.
 *
 * WHAT THIS PINS.  The engine must agree with ITSELF: the same query, over data
 * spanning several day-partitions, must return the same twa with parallelism on
 * as with TSDB_QUERY_PARALLEL=0.  That is the observable the bug cannot fake —
 * a broken build returns a strictly smaller value from the parallel run
 * (measured: 99.499583 vs 100.000000 on this fixture).
 */
#include "tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

static int g_fail = 0;

#define FATAL(fmt, ...) do { \
    fprintf(stderr, "FATAL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    exit(1); } while (0)

#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) \
    FATAL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define CHECK(cond, msg) do { \
    if (cond) printf("  PASS: %s\n", (msg)); \
    else { g_fail++; fprintf(stderr, "  FAIL: %s\n", (msg)); } \
} while (0)

#define TDIR "/tmp/tsdb_test_twa_parallel"
#define DAY_NS 86400000000000LL
#define BASE   1700000000000000000LL

static void rm_rf(const char *p) {
    char c[512];
    snprintf(c, sizeof(c), "rm -rf %s", p);
    (void)system(c);
}

/* Run "SELECT twa(v) FROM t GROUP BY host" and return the twa of the first
 * group, or NAN if the query produced nothing. */
static double twa_first_group(tsdb_db_t *db) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "SELECT host, twa(v) FROM t GROUP BY host", &r);
    if (rc != TSDB_OK) { if (r) tsdb_result_free(r); return NAN; }
    double out = NAN;
    if (tsdb_result_next(r)) out = tsdb_result_f64(r, 1);
    tsdb_result_free(r);
    return out;
}

int main(void) {
    printf("=== test_twa_parallel_groupby ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));

    tsdb_col_t cols[] = {
        { "ts",   TSDB_TYPE_TIMESTAMP },
        { "host", TSDB_TYPE_SYMBOL    },
        { "v",    TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, "t", cols, 3, "ts"));

    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));

    /* Six days, so the scan plan has several sources to split across workers.
     * A constant value makes the expected twa exactly that constant regardless
     * of how the interval is carved up — which is what lets a wrong answer be
     * recognised as wrong rather than merely different. */
    const int DAYS = 6, PER_DAY = 200;
    {
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int d = 0; d < DAYS; d++) {
            for (int i = 0; i < PER_DAY; i++) {
                int64_t ts = BASE + (int64_t)d * DAY_NS
                           + (int64_t)i * (DAY_NS / PER_DAY);
                OK(tsdb_batch_row_ts(b, ts));
                OK(tsdb_batch_row_sym(b, 1, "h1"));
                OK(tsdb_batch_row_f64(b, 2, 100.0));
                OK(tsdb_batch_row_end(b));
            }
        }
        OK(tsdb_batch_commit(b));
    }
    OK(tsdb_db_flush_all(db));

    /* Serial is the reference: the whole-query path has always refused to
     * parallelise ts-aggregates, so this number is the engine's own answer. */
    setenv("TSDB_QUERY_PARALLEL", "0", 1);
    double serial = twa_first_group(db);
    printf("  serial   twa = %.6f\n", serial);

    unsetenv("TSDB_QUERY_PARALLEL");
    double parallel = twa_first_group(db);
    printf("  parallel twa = %.6f\n", parallel);

    CHECK(!isnan(serial) && serial > 0.0, "serial twa is a real value");

    /* The value is constant at 100.0, so twa must be 100.0 whichever path runs.
     * Pre-fix the parallel run loses every inter-partition interval and comes
     * back far below it. */
    CHECK(fabs(serial - 100.0) < 0.5, "serial twa matches the constant value");
    CHECK(fabs(parallel - 100.0) < 0.5, "parallel twa matches the constant value");
    CHECK(fabs(serial - parallel) < 1e-6, "parallel twa agrees with serial");

    tsdb_close(db);
    rm_rf(TDIR);

    if (g_fail) { printf("\n%d FAILED\n", g_fail); return 1; }
    printf("\nALL PASS\n");
    return 0;
}
