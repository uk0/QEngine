/* test_stable_scatter.c — task #175: stable scalar-aggregate merge + scatter.
 *
 * Pins the multi-child merge semantics that back the cluster-wide stable
 * aggregation:
 *
 *  1. count/sum/min/max/avg over a 3-child stable (float + int columns)
 *     are CORRECT — the old merge summed min/max partials and kept avg
 *     from the first child only.
 *  2. An empty child is a merge no-op (its min/max partials are the
 *     identity sentinels, not zeros).
 *  3. Tag-filtered aggregates only cover matching children.
 *  4. Aliases survive the merge path; avg's derived column is named
 *     avg(col).
 *  5. Non-mergeable aggregates (stddev/...) on a multi-child stable fail
 *     loudly instead of returning the first child's value; single-child
 *     stables keep the passthrough.
 *  6. Scatter-local mode with the cluster inactive covers ALL children
 *     (ownership filter defaults to local).
 *  7. FED_QUERY_LOCAL end-to-end over a real RPC server: the handler arms
 *     scatter-local mode, executes, and ships the encoded result back —
 *     values match the plain FED_QUERY of the same query.
 *
 * The live multi-node scatter (coordinator merging partials from real
 * peers) needs a running cluster and is verified after deploy; here the
 * coordinator is inert (alive_count == 1).
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/cluster/rpc.h"
#include "../src/federation/fedrpc.h"

#include <assert.h>
#include <dirent.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); \
} while (0)

#define OK(rc) do { \
    int _rc = (rc); \
    if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); \
} while (0)

#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

#define ASSERT_F64_EQ(a, b) do { \
    double _a = (a), _b = (b); \
    if (fabs(_a - _b) > 1e-9) FAIL("%s: got %.12g want %.12g", #a, _a, _b); \
} while (0)

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

static void run_ok(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("'%s' rc=%d (%s)", sql, rc, tsdb_errstr(rc));
    tsdb_result_free(r);
}

/* Insert one (ts, v FLOAT64, w INT64) row batch into a child table. */
static void insert_rows(tsdb_db_t *db, const char *tbl,
                        const int64_t *ts, const double *v,
                        const int64_t *w, int n)
{
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, tbl, &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)ts[i]));
        OK(tsdb_batch_row_f64(b, 1, v[i]));
        OK(tsdb_batch_row_i64(b, 2, w[i]));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

/* Run `sql`, assert exactly one row, return the result (caller frees). */
static tsdb_result_t *one_row(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("'%s' rc=%d (%s)", sql, rc, tsdb_errstr(rc));
    ASSERT(r != NULL);
    if (!tsdb_result_next(r)) FAIL("'%s': no rows", sql);
    return r;
}

static void check_totals(tsdb_db_t *db, const char *label) {
    tsdb_result_t *r = one_row(db,
        "SELECT count(*), sum(v), min(v), max(v), avg(v) FROM m");
    ASSERT(tsdb_result_ncols(r) == 5);
    ASSERT(tsdb_result_i64(r, 0) == 6);
    ASSERT_F64_EQ(tsdb_result_f64(r, 1), 24.0);
    ASSERT_F64_EQ(tsdb_result_f64(r, 2), -1.0);
    ASSERT_F64_EQ(tsdb_result_f64(r, 3), 10.0);
    ASSERT_F64_EQ(tsdb_result_f64(r, 4),  4.0);
    /* Original column names, original order. */
    ASSERT(tsdb_result_col_index_by_name(r, "count(ts)") == 0);
    ASSERT(tsdb_result_col_index_by_name(r, "sum(v)")    == 1);
    ASSERT(tsdb_result_col_index_by_name(r, "min(v)")    == 2);
    ASSERT(tsdb_result_col_index_by_name(r, "max(v)")    == 3);
    ASSERT(tsdb_result_col_index_by_name(r, "avg(v)")    == 4);
    tsdb_result_free(r);

    r = one_row(db, "SELECT sum(w), min(w), max(w), avg(w) FROM m");
    ASSERT(tsdb_result_i64(r, 0) == 135);
    ASSERT(tsdb_result_i64(r, 1) == -5);
    ASSERT(tsdb_result_i64(r, 2) == 100);
    ASSERT_F64_EQ(tsdb_result_f64(r, 3), 22.5);
    tsdb_result_free(r);

    /* spread(x) == max(x) - min(x), merged via min+max across children;
     * float spread stays float, int spread stays int. */
    r = one_row(db, "SELECT spread(v), spread(w) FROM m");
    ASSERT(tsdb_result_ncols(r) == 2);
    ASSERT_F64_EQ(tsdb_result_f64(r, 0), 11.0);   /* 10 - (-1) */
    ASSERT(tsdb_result_i64(r, 1) == 105);          /* 100 - (-5) */
    ASSERT(tsdb_result_col_index_by_name(r, "spread(v)") == 0);
    ASSERT(tsdb_result_col_index_by_name(r, "spread(w)") == 1);
    tsdb_result_free(r);
    printf("  PASS: totals correct (%s)\n", label);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_stable_scatter";
    rm_rf(dir);

    printf("=== stable scalar-agg merge + scatter tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    /* ── Setup: 3 data-bearing children + 1 empty child ────────────────── */
    run_ok(db, "CREATE STABLE m (ts TIMESTAMP, v FLOAT64, w INT64) "
               "TAGS (loc SYMBOL)");
    run_ok(db, "CREATE TABLE c1 USING m TAGS ('a')");
    run_ok(db, "CREATE TABLE c2 USING m TAGS ('b')");
    run_ok(db, "CREATE TABLE c3 USING m TAGS ('a')");
    run_ok(db, "CREATE TABLE c4 USING m TAGS ('b')");   /* stays empty */

    {
        int64_t ts1[] = { 1000000000LL, 2000000000LL };
        double  v1 [] = { 1.5, 2.5 };
        int64_t w1 [] = { 10, 20 };
        insert_rows(db, "c1", ts1, v1, w1, 2);

        int64_t ts2[] = { 3000000000LL };
        double  v2 [] = { 10.0 };
        int64_t w2 [] = { -5 };
        insert_rows(db, "c2", ts2, v2, w2, 1);

        int64_t ts3[] = { 4000000000LL, 5000000000LL, 6000000000LL };
        double  v3 [] = { 4.0, 7.0, -1.0 };
        int64_t w3 [] = { 7, 100, 3 };
        insert_rows(db, "c3", ts3, v3, w3, 3);
    }
    /* Totals: n=6  sum(v)=24  min(v)=-1  max(v)=10  avg(v)=4
     *         sum(w)=135  min(w)=-5  max(w)=100  avg(w)=22.5 */

    /* ── 1+2: multi-child merge correct; empty child c4 is a no-op ─────── */
    printf("\n[1] multi-child count/sum/min/max/avg (with an empty child)\n");
    check_totals(db, "local multi-child merge");

    /* ── 3: tag-filtered subset (children c1+c3, loc='a') ──────────────── */
    printf("\n[2] tag-filtered aggregate covers matching children only\n");
    {
        tsdb_result_t *r = one_row(db,
            "SELECT count(*), sum(v), min(v), max(v), avg(v) FROM m "
            "WHERE loc = 'a'");
        ASSERT(tsdb_result_i64(r, 0) == 5);
        ASSERT_F64_EQ(tsdb_result_f64(r, 1), 14.0);
        ASSERT_F64_EQ(tsdb_result_f64(r, 2), -1.0);
        ASSERT_F64_EQ(tsdb_result_f64(r, 3),  7.0);
        ASSERT_F64_EQ(tsdb_result_f64(r, 4),  2.8);
        tsdb_result_free(r);
        printf("  PASS: WHERE loc='a' → 5 rows, avg=2.8\n");
    }

    /* ── 4: alias survives the merge path ──────────────────────────────── */
    printf("\n[3] aliases + avg column naming\n");
    {
        tsdb_result_t *r = one_row(db,
            "SELECT avg(v) AS mv, count(*) AS n FROM m");
        ASSERT(tsdb_result_col_index_by_name(r, "mv") == 0);
        ASSERT(tsdb_result_col_index_by_name(r, "n")  == 1);
        ASSERT_F64_EQ(tsdb_result_f64(r, 0), 4.0);
        ASSERT(tsdb_result_i64(r, 1) == 6);
        tsdb_result_free(r);
        printf("  PASS: 'avg(v) AS mv, count(*) AS n' aliased + correct\n");
    }

    /* ── 5: non-mergeable aggregate on multi-child stable errors ───────── */
    printf("\n[4] stddev/mixed on multi-child stable → loud error\n");
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT stddev(v) FROM m", &r);
        ASSERT(rc != TSDB_OK);
        if (r) tsdb_result_free(r);

        r = NULL;
        rc = tsdb_query(db, "SELECT count(*), stddev(v) FROM m", &r);
        ASSERT(rc != TSDB_OK);
        if (r) tsdb_result_free(r);
        printf("  PASS: stddev over multi-child stable rejected (rc!=OK)\n");
    }

    /* single-child stable keeps the passthrough (stddev still works) */
    {
        run_ok(db, "CREATE STABLE s1 (ts TIMESTAMP, v FLOAT64, w INT64) "
                   "TAGS (loc SYMBOL)");
        run_ok(db, "CREATE TABLE only1 USING s1 TAGS ('x')");
        int64_t ts[] = { 1000000000LL, 2000000000LL, 3000000000LL };
        double  v [] = { 1.0, 2.0, 3.0 };
        int64_t w [] = { 1, 2, 3 };
        insert_rows(db, "only1", ts, v, w, 3);

        tsdb_result_t *r = one_row(db, "SELECT stddev(v), avg(v) FROM s1");
        ASSERT_F64_EQ(tsdb_result_f64(r, 1), 2.0);
        tsdb_result_free(r);
        printf("  PASS: single-child stable stddev passthrough intact\n");
    }

    /* ── 6: scatter-local mode, cluster inactive → all children local ──── */
    printf("\n[5] scatter-local mode with cluster inactive\n");
    {
        ASSERT(tsdb_g_scatter_local_mode == 0);
        tsdb_g_scatter_local_mode = 1;
        check_totals(db, "scatter-local, standalone");
        ASSERT(tsdb_cluster_child_assigned_to_self(db, "c1") == 1);
        tsdb_g_scatter_local_mode = 0;
    }

    /* ── 7: FED_QUERY_LOCAL end-to-end over a real RPC server ──────────── */
    printf("\n[6] FED_QUERY_LOCAL RPC round-trip\n");
    {
        tsdb_rpc_server_t *srv = tsdb_rpc_server_new("127.0.0.1:0", db, NULL);
        ASSERT(srv != NULL);
        int port = tsdb_rpc_server_port(srv);
        ASSERT(port > 0);

        char addr[64];
        snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
        tsdb_rpc_conn_t *conn = tsdb_rpc_connect(addr, 2000);
        ASSERT(conn != NULL);

        const char *sql = "SELECT count(*), sum(v), avg(v) FROM m";

        tsdb_result_t *rl = NULL;
        OK(fedrpc_query_local(conn, sql, 5000, &rl));
        ASSERT(rl && tsdb_result_next(rl));
        ASSERT(tsdb_result_i64(rl, 0) == 6);
        ASSERT_F64_EQ(tsdb_result_f64(rl, 1), 24.0);
        ASSERT_F64_EQ(tsdb_result_f64(rl, 2),  4.0);

        /* Plain FED_QUERY of the same statement must agree (standalone:
         * scatter-local filtering is a no-op). */
        tsdb_result_t *rq = NULL;
        OK(fedrpc_query(conn, sql, 5000, &rq));
        ASSERT(rq && tsdb_result_next(rq));
        ASSERT(tsdb_result_i64(rq, 0) == tsdb_result_i64(rl, 0));
        ASSERT_F64_EQ(tsdb_result_f64(rq, 1), tsdb_result_f64(rl, 1));
        ASSERT_F64_EQ(tsdb_result_f64(rq, 2), tsdb_result_f64(rl, 2));

        tsdb_result_free(rl);
        tsdb_result_free(rq);
        tsdb_rpc_conn_close(conn);
        tsdb_rpc_server_stop(srv);
        printf("  PASS: FED_QUERY_LOCAL == FED_QUERY on standalone node\n");
    }

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== ALL STABLE SCATTER TESTS PASSED ===\n");
    return 0;
}
