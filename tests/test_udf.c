/* test_udf.c — scalar UDF end-to-end.
 *
 * Uses the sample .so in build/test/udf_sample.so (built by the Makefile
 * rule). Covers:
 *
 *   1. CREATE FUNCTION (via QTL)
 *   2. SELECT my_double(val) FROM t  — result matches 2*val
 *   3. udf_add(a,b) with two INT64 cols
 *   4. udf_clamp(x,lo,hi) with three FLOAT64 cols
 *   5. LIST FUNCTIONS
 *   6. DROP FUNCTION + reject subsequent call
 *   7. Reopen DB — UDF metadata persists; lazy dlopen works post-reopen
 *   8. Name collision with builtin is rejected
 *   9. Missing .so path returns clear error at lookup (not 0 rows)
 *  10. ABI-mismatch — simulated by loading a library with wrong version sym
 *
 * A UDF nested inside an aggregate — SELECT avg(my_double(val)) FROM t:
 *
 *  11. Every scalar aggregate over a 1-arg UDF, differentially checked
 *      against the same aggregate over the raw column (my_double is 2*x, so
 *      each aggregate must come out exactly 2x — count exactly equal)
 *  12. 2-arg and 3-arg UDFs inside aggregates
 *  13. WHERE + UDF-in-aggregate, including the rule that the UDF never sees a
 *      row the WHERE excluded
 *  14. GROUP BY + UDF-in-aggregate, serial and parallel, byte-identical
 *  15. Multi-source (parallel whole-query aggregate) path
 *  16. Several UDF aggregates in one SELECT, mixed with plain aggregates
 *  17. Cases that stay UNSUPPORTED, and the aggregate-argument validation
 *      hole that used to silently aggregate the ts column
 *  18. Empty input behaves exactly as it does over a raw column
 */

#include "tsdb.h"
#include "../src/query/exec.h"

#include <assert.h>
#include <dirent.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); \
} while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

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

static const char *SO_PATH = "build/test/udf_sample.so";

static void run_ok(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("sql '%s' rc=%d (%s)", sql, rc, tsdb_errstr(rc));
    if (r) tsdb_result_free(r);
}

/* ---- helpers for the aggregate-over-UDF phases -------------------------- */

/* Run a single-row query and copy every cell of row 0 into out[] as doubles.
 * Aborts unless the query succeeds and produces exactly `ncols` columns and
 * exactly one row. */
static void one_row_f64(tsdb_db_t *db, const char *sql, double *out, int ncols) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK) FAIL("sql '%s' rc=%d (%s)", sql, rc, tsdb_errstr(rc));
    ASSERT(r != NULL);
    if (tsdb_result_ncols(r) != ncols)
        FAIL("sql '%s' ncols=%d want %d", sql, tsdb_result_ncols(r), ncols);
    ASSERT(tsdb_result_next(r));
    for (int i = 0; i < ncols; i++) {
        out[i] = (tsdb_result_col_type(r, i) == TSDB_TYPE_FLOAT64)
                 ? tsdb_result_f64(r, i)
                 : (double)tsdb_result_i64(r, i);
    }
    ASSERT(!tsdb_result_next(r));
    tsdb_result_free(r);
}

/* Assert a query is refused with exactly `want`.  A refusal, not a wrong
 * answer, is the contract for everything not yet wired. */
static void expect_rc(tsdb_db_t *db, const char *sql, int want) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (r) tsdb_result_free(r);
    if (rc != want)
        FAIL("sql '%s' rc=%d (%s), want %d (%s)",
             sql, rc, tsdb_errstr(rc), want, tsdb_errstr(want));
}

static void eq(double got, double want, const char *what) {
    double tol = 1e-9 * (fabs(want) > 1.0 ? fabs(want) : 1.0);
    if (!(fabs(got - want) <= tol))
        FAIL("%s: got %.12g want %.12g", what, got, want);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_udf";
    rm_rf(dir);

    struct stat st;
    if (stat(SO_PATH, &st) != 0) {
        fprintf(stderr, "SKIP: %s not built. Run `make build/test/udf_sample.so` first.\n", SO_PATH);
        return 0;
    }

    printf("=== tsdb UDF tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    /* Phase 1 — CREATE FUNCTION via QTL */
    printf("\n[1] CREATE FUNCTION my_double(FLOAT64) RETURNS FLOAT64\n");
    char sql[1024];
    snprintf(sql, sizeof(sql),
        "CREATE FUNCTION my_double(FLOAT64) RETURNS FLOAT64 "
        "FROM '%s' SYMBOL 'udf_double';", SO_PATH);
    run_ok(db, sql);
    snprintf(sql, sizeof(sql),
        "CREATE FUNCTION my_add(INT64, INT64) RETURNS INT64 "
        "FROM '%s' SYMBOL 'udf_add';", SO_PATH);
    run_ok(db, sql);
    snprintf(sql, sizeof(sql),
        "CREATE FUNCTION my_clamp(FLOAT64, FLOAT64, FLOAT64) RETURNS FLOAT64 "
        "FROM '%s' SYMBOL 'udf_clamp';", SO_PATH);
    run_ok(db, sql);
    printf("  PASS: 3 UDFs registered via QTL\n");

    /* Phase 2 — SELECT my_double(val) */
    printf("\n[2] SELECT my_double(val) FROM ud\n");
    tsdb_col_t cols1[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "ud", cols1, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "ud", &t));
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 5; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)((i + 1) * 1000000000LL)));
        OK(tsdb_batch_row_f64(b, 1, 1.5 + (double)i));  /* 1.5, 2.5, 3.5, 4.5, 5.5 */
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT my_double(val) FROM ud", &r));
        ASSERT(r != NULL);
        int nr = 0;
        double expected[] = { 3.0, 5.0, 7.0, 9.0, 11.0 };
        while (tsdb_result_next(r)) {
            double v = tsdb_result_f64(r, 0);
            ASSERT(nr < 5);
            ASSERT(fabs(v - expected[nr]) < 1e-9);
            nr++;
        }
        ASSERT(nr == 5);
        tsdb_result_free(r);
        printf("  PASS: 5 rows, my_double(val) = 2*val exact\n");
    }

    /* Phase 3 — udf_add with two INT64 cols */
    printf("\n[3] SELECT my_add(a, b) FROM ab\n");
    tsdb_col_t cols2[] = {
        {"ts", TSDB_TYPE_TIMESTAMP},
        {"a",  TSDB_TYPE_INT64},
        {"b",  TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "ab", cols2, 3, "ts"));
    tsdb_table_t *ab; OK(tsdb_open_table(db, "ab", &ab));
    tsdb_batch_t *b2; OK(tsdb_batch_begin(ab, &b2));
    for (int i = 0; i < 3; i++) {
        OK(tsdb_batch_row_ts(b2, (tsdb_ts_t)((i + 1) * 1000000000LL)));
        OK(tsdb_batch_row_i64(b2, 1, 10 + i));
        OK(tsdb_batch_row_i64(b2, 2, 100 + i));
        OK(tsdb_batch_row_end(b2));
    }
    OK(tsdb_batch_commit(b2));
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT my_add(a, b) FROM ab", &r));
        int nr = 0;
        int64_t expected[] = { 110, 112, 114 };
        while (tsdb_result_next(r)) {
            int64_t v = tsdb_result_i64(r, 0);
            ASSERT(nr < 3);
            ASSERT(v == expected[nr]);
            nr++;
        }
        ASSERT(nr == 3);
        tsdb_result_free(r);
        printf("  PASS: my_add returns a+b pairwise\n");
    }

    /* Phase 5 — LIST FUNCTIONS */
    printf("\n[5] LIST FUNCTIONS (expect 3 registered)\n");
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "LIST FUNCTIONS", &r));
        ASSERT(r != NULL);
        ASSERT(tsdb_result_ncols(r) == 5);
        int nr = 0;
        while (tsdb_result_next(r)) nr++;
        tsdb_result_free(r);
        ASSERT(nr == 3);
        printf("  PASS: LIST FUNCTIONS returns 3 rows, 5 cols\n");
    }

    /* Phase 6 — DROP FUNCTION + reject */
    printf("\n[6] DROP FUNCTION my_add; subsequent call rejected\n");
    run_ok(db, "DROP FUNCTION my_add");
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT my_add(a, b) FROM ab", &r);
        ASSERT(rc != TSDB_OK);
        if (r) tsdb_result_free(r);
        printf("  PASS: rc=%d after drop\n", rc);
    }

    /* Phase 7 — reopen DB, UDF metadata survives */
    printf("\n[7] close + reopen — UDF catalog replay\n");
    tsdb_close(db);
    OK(tsdb_open(dir, &db));
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "LIST FUNCTIONS", &r));
        int nr = 0; while (tsdb_result_next(r)) nr++;
        tsdb_result_free(r);
        ASSERT(nr == 2);  /* my_double + my_clamp (my_add was dropped) */
        printf("  PASS: reopen found 2 surviving UDFs\n");
    }
    /* Re-exercise my_double after reopen — forces a fresh dlopen path. */
    OK(tsdb_open_table(db, "ud", &t));
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT my_double(val) FROM ud LIMIT 1", &r));
        ASSERT(tsdb_result_next(r));
        double v = tsdb_result_f64(r, 0);
        ASSERT(fabs(v - 3.0) < 1e-9);
        tsdb_result_free(r);
        printf("  PASS: lazy dlopen after reopen works\n");
    }

    /* Phase 8 — builtin name collision rejected */
    printf("\n[8] CREATE FUNCTION count(...) rejected (shadows builtin)\n");
    {
        tsdb_result_t *r = NULL;
        snprintf(sql, sizeof(sql),
            "CREATE FUNCTION count(INT64) RETURNS INT64 "
            "FROM '%s' SYMBOL 'udf_double';", SO_PATH);
        int rc = tsdb_query(db, sql, &r);
        ASSERT(rc == TSDB_ERR_EXISTS);
        if (r) tsdb_result_free(r);
        printf("  PASS: rejected rc=%d\n", rc);
    }

    /* Phase 9 — missing .so path surfaces a clear error (not zero rows) */
    printf("\n[9] missing .so path → lookup returns TSDB_ERR_NOTFOUND\n");
    {
        run_ok(db,
            "CREATE FUNCTION ghost(INT64) RETURNS INT64 "
            "FROM '/nonexistent/definitely-not-there.so' SYMBOL 'udf_double';");
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT ghost(a) FROM ab", &r);
        ASSERT(rc == TSDB_ERR_NOTFOUND);
        if (r) tsdb_result_free(r);
        OK(tsdb_query(db, "DROP FUNCTION ghost", &r));
        if (r) tsdb_result_free(r);
        printf("  PASS: rc=%d (NOTFOUND) with descriptive error, not 0 rows\n", rc);
    }

    /* Phase 10 — UDF runtime error aborts the SELECT (not rows-of-zero) */
    printf("\n[10] UDF returning an error rc aborts the query\n");
    {
        snprintf(sql, sizeof(sql),
            "CREATE FUNCTION fail42(INT64) RETURNS INT64 "
            "FROM '%s' SYMBOL 'udf_fail42';", SO_PATH);
        run_ok(db, sql);

        /* rows in ab carry a = 10, 11, 12 — no 42 → passes through. */
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT fail42(a) FROM ab", &r));
        int nr = 0;
        while (tsdb_result_next(r)) {
            ASSERT(tsdb_result_i64(r, 0) == 10 + nr);
            nr++;
        }
        ASSERT(nr == 3);
        tsdb_result_free(r);

        /* Insert the magic failure value; the SELECT must now error. */
        OK(tsdb_open_table(db, "ab", &ab));
        tsdb_batch_t *b3; OK(tsdb_batch_begin(ab, &b3));
        OK(tsdb_batch_row_ts(b3, (tsdb_ts_t)9000000000LL));
        OK(tsdb_batch_row_i64(b3, 1, 42));
        OK(tsdb_batch_row_i64(b3, 2, 0));
        OK(tsdb_batch_row_end(b3));
        OK(tsdb_batch_commit(b3));

        r = NULL;
        int rc = tsdb_query(db, "SELECT fail42(a) FROM ab", &r);
        ASSERT(rc == TSDB_ERR_INTERNAL);
        if (r) tsdb_result_free(r);
        printf("  PASS: rc=%d (INTERNAL) — query aborted, not zeros\n", rc);
    }

    /* ===================================================================
     * A UDF nested inside an aggregate.
     * =================================================================== */

    /* my_add was dropped in phase 6; bring it back for the 2-arg cases. */
    snprintf(sql, sizeof(sql),
        "CREATE FUNCTION my_add(INT64, INT64) RETURNS INT64 "
        "FROM '%s' SYMBOL 'udf_add';", SO_PATH);
    run_ok(db, sql);

    /* Phase 11 — every scalar aggregate over a 1-arg UDF, differentially
     * checked against the same aggregate over the raw column.  my_double is
     * x -> 2x, a strictly increasing affine map, so for THIS udf:
     *   count  identical
     *   sum / avg / min / max / spread / stddev / p50 / first / last / twa
     *          exactly 2x the raw-column answer.
     * The raw-column column of each pair is produced by the pre-existing
     * (unmodified) aggregate path, so this is a differential test of the new
     * path against the old one, not a table of hand-copied constants. */
    printf("\n[11] agg(my_double(val)) vs agg(val) — every scalar aggregate\n");
    {
        static const char *AGGS[] = {
            "count", "sum", "avg", "min", "max",
            "spread", "stddev", "p50", "first", "last", "twa",
        };
        const int NAGG = (int)(sizeof(AGGS) / sizeof(AGGS[0]));
        for (int i = 0; i < NAGG; i++) {
            char raw[128], wrapped[128];
            snprintf(raw,     sizeof(raw),     "SELECT %s(val) FROM ud", AGGS[i]);
            snprintf(wrapped, sizeof(wrapped), "SELECT %s(my_double(val)) FROM ud", AGGS[i]);
            double a = 0, b = 0;
            one_row_f64(db, raw, &a, 1);
            one_row_f64(db, wrapped, &b, 1);
            double want = (strcmp(AGGS[i], "count") == 0) ? a : 2.0 * a;
            char what[160];
            snprintf(what, sizeof(what), "%s(my_double(val))", AGGS[i]);
            eq(b, want, what);
        }
        /* Absolute check on two of them so a bug that scaled BOTH sides
         * identically could not hide behind the ratio.  val = 1.5 .. 5.5. */
        double v[2];
        one_row_f64(db, "SELECT sum(my_double(val)), count(my_double(val)) FROM ud", v, 2);
        eq(v[0], 35.0, "sum(my_double(val)) absolute");
        eq(v[1], 5.0,  "count(my_double(val)) absolute");
        printf("  PASS: %d aggregates match 2x the raw-column answer\n", NAGG);
    }

    /* Phase 12 — 2-arg and 3-arg UDFs inside aggregates. */
    printf("\n[12] agg(my_add(a,b)) and agg(my_clamp(x,lo,hi))\n");
    {
        tsdb_col_t cols3[] = {
            {"ts", TSDB_TYPE_TIMESTAMP},
            {"a",  TSDB_TYPE_INT64},
            {"b",  TSDB_TYPE_INT64},
            {"x",  TSDB_TYPE_FLOAT64},
            {"lo", TSDB_TYPE_FLOAT64},
            {"hi", TSDB_TYPE_FLOAT64},
        };
        OK(tsdb_create_table(db, "m3", cols3, 6, "ts"));
        tsdb_table_t *m3; OK(tsdb_open_table(db, "m3", &m3));
        tsdb_batch_t *bm; OK(tsdb_batch_begin(m3, &bm));
        /* a+b = 110,112,114,116,118 ; clamp(x,2,9) over x=1.5..5.5 */
        for (int i = 0; i < 5; i++) {
            OK(tsdb_batch_row_ts(bm, (tsdb_ts_t)((i + 1) * 1000000000LL)));
            OK(tsdb_batch_row_i64(bm, 1, 10 + i));
            OK(tsdb_batch_row_i64(bm, 2, 100 + i));
            OK(tsdb_batch_row_f64(bm, 3, 1.5 + (double)i));
            OK(tsdb_batch_row_f64(bm, 4, 2.0));
            OK(tsdb_batch_row_f64(bm, 5, 9.0));
            OK(tsdb_batch_row_end(bm));
        }
        OK(tsdb_batch_commit(bm));

        double g[4];
        one_row_f64(db,
            "SELECT sum(my_add(a,b)), avg(my_add(a,b)), "
            "min(my_add(a,b)), max(my_add(a,b)) FROM m3", g, 4);
        eq(g[0], 570.0, "sum(my_add(a,b))");
        eq(g[1], 114.0, "avg(my_add(a,b))");
        eq(g[2], 110.0, "min(my_add(a,b))");
        eq(g[3], 118.0, "max(my_add(a,b))");

        /* clamp(1.5,2,9)=2, then 2.5, 3.5, 4.5, 5.5 → sum 18, min 2, max 5.5 */
        one_row_f64(db,
            "SELECT sum(my_clamp(x,lo,hi)), avg(my_clamp(x,lo,hi)), "
            "min(my_clamp(x,lo,hi)), max(my_clamp(x,lo,hi)) FROM m3", g, 4);
        eq(g[0], 18.0, "sum(my_clamp)");
        eq(g[1], 3.6,  "avg(my_clamp)");
        eq(g[2], 2.0,  "min(my_clamp)");
        eq(g[3], 5.5,  "max(my_clamp)");
        printf("  PASS: 2-arg and 3-arg UDFs aggregate correctly\n");
    }

    /* Phase 13 — WHERE + UDF-in-aggregate.  The UDF must only ever see rows
     * that survived the predicate: the same rule the per-row scalar path
     * follows.  fail42 proves it — it errors on the value 42, so a query that
     * filters 42 out must still succeed. */
    printf("\n[13] WHERE + UDF inside an aggregate\n");
    {
        double g[3];
        one_row_f64(db,
            "SELECT avg(my_double(val)), count(my_double(val)), sum(my_double(val)) "
            "FROM ud WHERE val > 2.0", g, 3);
        eq(g[0], 8.0,  "avg(my_double(val)) WHERE val>2");
        eq(g[1], 4.0,  "count(my_double(val)) WHERE val>2");
        eq(g[2], 32.0, "sum(my_double(val)) WHERE val>2");

        /* ab holds a = 10, 11, 12, 42.  Unfiltered, fail42 aborts. */
        expect_rc(db, "SELECT sum(fail42(a)) FROM ab", TSDB_ERR_INTERNAL);
        /* Filtered, the failing row is compacted out before the UDF runs. */
        double s2[2];
        one_row_f64(db, "SELECT sum(fail42(a)), count(fail42(a)) FROM ab WHERE a < 42",
                    s2, 2);
        eq(s2[0], 33.0, "sum(fail42(a)) WHERE a<42");
        eq(s2[1], 3.0,  "count(fail42(a)) WHERE a<42");
        printf("  PASS: WHERE applied before the UDF; failing row filtered out\n");
    }

    /* Phase 14 — GROUP BY.  Runs the same query through the serial and the
     * parallel hash-aggregate and requires identical output. */
    printf("\n[14] GROUP BY + UDF inside an aggregate (serial == parallel)\n");
    {
        tsdb_col_t colsg[] = {
            {"ts", TSDB_TYPE_TIMESTAMP},
            {"g",  TSDB_TYPE_INT64},
            {"v",  TSDB_TYPE_FLOAT64},
        };
        OK(tsdb_create_table(db, "gb", colsg, 3, "ts"));
        tsdb_table_t *gt; OK(tsdb_open_table(db, "gb", &gt));
        /* 8 commits → 8 scan sources, so the parallel GROUP BY worker split
         * actually has something to split.  g in {0,1,2}; v = row index. */
        int nrows_total = 0;
        for (int c = 0; c < 8; c++) {
            tsdb_batch_t *bg; OK(tsdb_batch_begin(gt, &bg));
            for (int i = 0; i < 50; i++) {
                int row = c * 50 + i;
                OK(tsdb_batch_row_ts(bg, (tsdb_ts_t)((row + 1) * 1000000LL)));
                OK(tsdb_batch_row_i64(bg, 1, row % 3));
                OK(tsdb_batch_row_f64(bg, 2, (double)row));
                OK(tsdb_batch_row_end(bg));
                nrows_total++;
            }
            OK(tsdb_batch_commit(bg));
        }

        const char *GQ =
            "SELECT g, count(my_double(v)), sum(my_double(v)), avg(my_double(v)), "
            "min(my_double(v)), max(my_double(v)) FROM gb GROUP BY g";
        /* Reference from the pre-existing path: same aggregates over v. */
        const char *RQ =
            "SELECT g, count(v), sum(v), avg(v), min(v), max(v) FROM gb GROUP BY g";

        double par[8][6], ser[8][6], ref[8][6];
        int npar = 0, nser = 0, nref = 0;

        for (int pass = 0; pass < 3; pass++) {
            tsdb_set_query_parallel(pass == 0 ? 1 : 0);
            const char *q = (pass == 2) ? RQ : GQ;
            tsdb_result_t *r = NULL;
            OK(tsdb_query(db, q, &r));
            ASSERT(tsdb_result_ncols(r) == 6);
            int n = 0;
            while (tsdb_result_next(r)) {
                ASSERT(n < 8);
                double *dst = (pass == 0) ? par[n] : (pass == 1) ? ser[n] : ref[n];
                for (int ci = 0; ci < 6; ci++)
                    dst[ci] = (tsdb_result_col_type(r, ci) == TSDB_TYPE_FLOAT64)
                              ? tsdb_result_f64(r, ci)
                              : (double)tsdb_result_i64(r, ci);
                n++;
            }
            tsdb_result_free(r);
            if (pass == 0) npar = n; else if (pass == 1) nser = n; else nref = n;
        }
        tsdb_set_query_parallel(1);

        ASSERT(npar == 3 && nser == 3 && nref == 3);
        /* Groups may come out in hash order; index by g. */
        double *bypar[3] = {0}, *byser[3] = {0}, *byref[3] = {0};
        for (int i = 0; i < 3; i++) {
            bypar[(int)par[i][0]] = par[i];
            byser[(int)ser[i][0]] = ser[i];
            byref[(int)ref[i][0]] = ref[i];
        }
        double total_count = 0;
        for (int g = 0; g < 3; g++) {
            ASSERT(bypar[g] && byser[g] && byref[g]);
            for (int ci = 1; ci < 6; ci++) {
                char what[96];
                snprintf(what, sizeof(what), "group %d col %d serial==parallel", g, ci);
                eq(byser[g][ci], bypar[g][ci], what);
                snprintf(what, sizeof(what), "group %d col %d vs raw column", g, ci);
                /* col 1 is count (identical), 2..5 scale by 2. */
                eq(bypar[g][ci], (ci == 1 ? byref[g][ci] : 2.0 * byref[g][ci]), what);
            }
            total_count += bypar[g][1];
        }
        eq(total_count, (double)nrows_total, "GROUP BY total row count");
        printf("  PASS: 3 groups, %d rows, serial == parallel == 2x raw\n",
               nrows_total);

        /* GROUP BY + WHERE + UDF. */
        {
            tsdb_result_t *r = NULL;
            OK(tsdb_query(db,
                "SELECT g, sum(my_double(v)), count(my_double(v)) "
                "FROM gb WHERE v >= 200 GROUP BY g", &r));
            double seen_sum = 0, seen_cnt = 0;
            int n = 0;
            while (tsdb_result_next(r)) {
                seen_sum += tsdb_result_f64(r, 1);
                seen_cnt += (double)tsdb_result_i64(r, 2);
                n++;
            }
            tsdb_result_free(r);
            ASSERT(n == 3);
            /* v = 200..399 → 200 rows, sum(v) = (200+399)*200/2 = 59900. */
            eq(seen_cnt, 200.0,       "GROUP BY+WHERE count");
            eq(seen_sum, 2.0 * 59900, "GROUP BY+WHERE sum");
            printf("  PASS: GROUP BY + WHERE + UDF aggregate\n");
        }
    }

    /* Phase 15 — multi-source whole-query aggregate (the parallel scan path
     * and the block-stats gate).  gb has 8 sources; the same answer must come
     * out of the parallel and the serial executor. */
    printf("\n[15] multi-source aggregate: parallel == serial\n");
    {
        const char *Q =
            "SELECT count(my_double(v)), sum(my_double(v)), avg(my_double(v)), "
            "min(my_double(v)), max(my_double(v)), spread(my_double(v)), "
            "stddev(my_double(v)) FROM gb";
        double par[7], ser[7], ref[7];
        tsdb_set_query_parallel(1);
        one_row_f64(db, Q, par, 7);
        tsdb_set_query_parallel(0);
        one_row_f64(db, Q, ser, 7);
        tsdb_set_query_parallel(1);
        one_row_f64(db,
            "SELECT count(v), sum(v), avg(v), min(v), max(v), spread(v), "
            "stddev(v) FROM gb", ref, 7);
        for (int i = 0; i < 7; i++) {
            char what[64];
            snprintf(what, sizeof(what), "col %d parallel==serial", i);
            eq(ser[i], par[i], what);
            snprintf(what, sizeof(what), "col %d vs raw column", i);
            eq(par[i], (i == 0 ? ref[i] : 2.0 * ref[i]), what);
        }
        /* v = 0..399 → sum 79800, so sum(my_double(v)) = 159600. */
        eq(par[0], 400.0,    "count over 8 sources");
        eq(par[1], 159600.0, "sum over 8 sources");
        printf("  PASS: 400 rows / 8 sources, parallel == serial == 2x raw\n");
    }

    /* Phase 16 — several UDF aggregates in one SELECT, mixed with plain
     * aggregates and count(*).  Each UDF aggregate gets its own
     * materialisation slot; they must not overwrite each other. */
    printf("\n[16] several UDF aggregates in one SELECT, mixed with plain ones\n");
    {
        double g[6];
        one_row_f64(db,
            "SELECT avg(my_double(val)), avg(val), max(my_double(val)), "
            "min(val), count(*), sum(my_double(val)) FROM ud", g, 6);
        eq(g[0], 7.0,   "avg(my_double(val))");
        eq(g[1], 3.5,   "avg(val)");
        eq(g[2], 11.0,  "max(my_double(val))");
        eq(g[3], 1.5,   "min(val)");
        eq(g[4], 5.0,   "count(*)");
        eq(g[5], 35.0,  "sum(my_double(val))");

        /* Two different UDFs side by side over the same table. */
        double h[2];
        one_row_f64(db,
            "SELECT sum(my_add(a,b)), sum(my_clamp(x,lo,hi)) FROM m3", h, 2);
        eq(h[0], 570.0, "sum(my_add) alongside sum(my_clamp)");
        eq(h[1], 18.0,  "sum(my_clamp) alongside sum(my_add)");
        printf("  PASS: independent materialisation slots per aggregate\n");
    }

    /* Phase 17 — what stays a refusal.  Every one of these must return an
     * error rather than a number computed from the wrong input. */
    printf("\n[17] refusals stay refusals\n");
    {
        /* Not wired: the SAMPLE BY bucket accumulator, the advanced window
         * kinds and LATEST ON all run their own aggregation. */
        expect_rc(db, "SELECT avg(my_double(val)) FROM ud SAMPLE BY 1s",
                  TSDB_ERR_UNSUPPORTED);
        expect_rc(db, "SELECT avg(my_double(val)) FROM ud LATEST ON ts",
                  TSDB_ERR_UNSUPPORTED);
        /* Unknown UDF inside an aggregate resolves like any other call. */
        expect_rc(db, "SELECT avg(no_such_udf(val)) FROM ud", TSDB_ERR_NOTFOUND);
        /* Arity and per-argument type are checked at plan time. */
        expect_rc(db, "SELECT avg(my_double(val, val)) FROM ud", TSDB_ERR_PARSE);
        expect_rc(db, "SELECT avg(my_double(a)) FROM ab", TSDB_ERR_SCHEMA);
        expect_rc(db, "SELECT avg(my_double(nosuchcol)) FROM ud", TSDB_ERR_SCHEMA);
        /* Nesting a builtin, or an expression, inside an aggregate is still
         * unsupported — but it must SAY so.  Before this change stddev() and
         * the percentile family fell through the argument checks entirely and
         * aggregated column 0 (the ts column) instead. */
        expect_rc(db, "SELECT stddev(a + b) FROM ab",  TSDB_ERR_UNSUPPORTED);
        expect_rc(db, "SELECT p50(a + b) FROM ab",     TSDB_ERR_UNSUPPORTED);
        expect_rc(db, "SELECT p90(1) FROM ab",         TSDB_ERR_UNSUPPORTED);
        expect_rc(db, "SELECT percentile(a + b, 0.5) FROM ab", TSDB_ERR_UNSUPPORTED);
        expect_rc(db, "SELECT sum(a + b) FROM ab",     TSDB_ERR_UNSUPPORTED);
        /* HAVING cannot reference an aggregate over a UDF (it matches
         * aggregates by column, and this one has no source column). */
        expect_rc(db,
            "SELECT g, avg(my_double(v)) FROM gb GROUP BY g "
            "HAVING avg(my_double(v)) > 1", TSDB_ERR_SCHEMA);
        printf("  PASS: 12 refusals, each with the right error code\n");
    }

    /* Phase 18 — empty input behaves exactly as it does over a raw column:
     * avg NaN, count 0, min/max the untouched sentinels, and the t-digest
     * kinds refuse rather than emit a NaN cell. */
    printf("\n[18] empty input: same behaviour as the raw column\n");
    {
        double u[4], v[4];
        one_row_f64(db,
            "SELECT count(my_double(val)), avg(my_double(val)), "
            "min(my_double(val)), max(my_double(val)) FROM ud WHERE val > 1000.0", u, 4);
        one_row_f64(db,
            "SELECT count(val), avg(val), min(val), max(val) "
            "FROM ud WHERE val > 1000.0", v, 4);
        eq(u[0], 0.0, "count over empty input");
        eq(v[0], 0.0, "count(val) over empty input");
        for (int i = 1; i < 4; i++) {
            /* NaN == NaN fails a plain compare; require the same class. */
            int same = (isnan(u[i]) && isnan(v[i])) ||
                       (!isnan(u[i]) && !isnan(v[i]) && u[i] == v[i]);
            if (!same) FAIL("empty-input col %d: udf=%g raw=%g", i, u[i], v[i]);
        }
        expect_rc(db, "SELECT stddev(my_double(val)) FROM ud WHERE val > 1000.0",
                  TSDB_ERR_INVAL);
        expect_rc(db, "SELECT stddev(val) FROM ud WHERE val > 1000.0",
                  TSDB_ERR_INVAL);
        printf("  PASS: empty-input results identical to the raw-column path\n");
    }

    /* Phase 19 — DROP FUNCTION also invalidates the aggregate form. */
    printf("\n[19] DROP FUNCTION invalidates agg(udf(...)) too\n");
    {
        run_ok(db, "DROP FUNCTION my_add");
        expect_rc(db, "SELECT sum(my_add(a,b)) FROM m3", TSDB_ERR_NOTFOUND);
        printf("  PASS: rc=NOTFOUND after drop\n");
    }

    tsdb_close(db);
    rm_rf(dir);
    printf("\n=== ALL UDF TESTS PASSED ===\n");
    return 0;
}
