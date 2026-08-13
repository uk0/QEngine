/* test_bucket_groupby_reject.c — time_bucket() in SELECT with a column GROUP BY
 * must be refused, not answered with uninitialised heap.
 *
 * THE BUG.  exec_group_by validated only PROJ_COL projections against the
 * GROUP BY keys, so a PROJ_TS_BUCKET projection passed the check.  At emission
 * agg_write has no case for that kind and hits `default: return;`, leaving the
 * cell unwritten — and result columns are grown with realloc, which does not
 * zero.  So
 *
 *     SELECT time_bucket(ts, 1s), count(*) FROM t GROUP BY host
 *
 * returned rc=0 with whatever heap bytes happened to sit at that offset,
 * presented to the caller as a timestamp.  A user migrating from a database
 * where that syntax means "group by bucket AND host" gets one row per host
 * whose bucket column is stale memory, silently charted.
 *
 * WHY REFUSING IS THE RIGHT ANSWER.  A group keyed by a tag spans arbitrarily
 * many buckets, so there is no single bucket value to report.  The legitimate
 * form — time_bucket in the GROUP BY — never reaches this executor at all: the
 * parser routes it to the SAMPLE BY bucketing path, and already refuses to mix
 * it with a tag key.  So every query that arrives here with a bucket projection
 * is the unanswerable shape.
 *
 * WHAT THIS PINS.  The query is rejected with a schema error.  The assertion is
 * on the RETURN CODE, not on the value: checking the bucket cell would be a
 * vacuous test, because uninitialised heap can hold anything — including, on a
 * lucky run, something that looks plausible.
 */
#include "tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

#define TDIR "/tmp/tsdb_test_bucket_gb_reject"
#define BASE 1700000000000000000LL

static void rm_rf(const char *p) {
    char c[512];
    snprintf(c, sizeof(c), "rm -rf %s", p);
    (void)system(c);
}

int main(void) {
    printf("=== test_bucket_groupby_reject ===\n");
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
    {
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < 40; i++) {
            OK(tsdb_batch_row_ts(b, BASE + (int64_t)i * 100000000LL));
            OK(tsdb_batch_row_sym(b, 1, (i % 2) ? "h1" : "h2"));
            OK(tsdb_batch_row_f64(b, 2, (double)i));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }
    OK(tsdb_db_flush_all(db));

    /* The unanswerable shape: bucket projection, tag grouping. */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db,
            "SELECT time_bucket(ts, 1000000000), count(*) FROM t GROUP BY host", &r);
        if (r) tsdb_result_free(r);
        printf("  bucket-in-select + GROUP BY host -> rc=%d\n", rc);
        CHECK(rc != TSDB_OK, "refused instead of returning an unwritten cell");
    }

    /* Everything else must still work: the guard must not catch legitimate
     * queries.  A plain tag GROUP BY, and the bucketing form that routes to the
     * SAMPLE BY path, both have to keep answering. */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT host, count(*) FROM t GROUP BY host", &r);
        int rows = 0;
        if (rc == TSDB_OK && r) while (tsdb_result_next(r)) rows++;
        if (r) tsdb_result_free(r);
        CHECK(rc == TSDB_OK && rows == 2, "plain tag GROUP BY still works");
    }
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db,
            "SELECT time_bucket(ts, 1000000000), count(*) FROM t "
            "GROUP BY time_bucket(ts, 1000000000)", &r);
        int rows = 0;
        if (rc == TSDB_OK && r) while (tsdb_result_next(r)) rows++;
        if (r) tsdb_result_free(r);
        CHECK(rc == TSDB_OK && rows > 0, "GROUP BY time_bucket still buckets");
    }

    tsdb_close(db);
    rm_rf(TDIR);

    if (g_fail) { printf("\n%d FAILED\n", g_fail); return 1; }
    printf("\nALL PASS\n");
    return 0;
}
