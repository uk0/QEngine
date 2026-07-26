/* bench_udf.c — what does a user-defined function actually cost?
 *
 * The UDF ABI is batched and columnar (one call per block, n values in, n out),
 * so the interesting number is not "is a UDF slow" but how close it lands to
 * the equivalent built-in — i.e. how much of the cost is dispatch/marshalling
 * versus the user's own arithmetic.
 *
 * Measures, over the same data:
 *   projection      SELECT val               — the floor: scan + materialise
 *   native scale    SELECT val * 2.0         — built-in arithmetic
 *   udf 1-arg       SELECT my_double(val)
 *   udf 2-arg       SELECT my_add(a, b)      — INT64, two input columns
 *   udf 3-arg       SELECT my_clamp(val,l,h) — three input columns
 *   udf + aggregate SELECT avg(my_double(val))  — no row materialisation
 *   native aggregate SELECT avg(val)         — the aggregate floor
 *   client-side     SELECT my_double(val) drained and averaged by the caller,
 *                   which is what the aggregate form replaces
 *
 * Reports ns/row and Mrows/s for each, plus the UDF/native ratio, so a
 * regression in the dispatch path is visible rather than buried in the scan.
 *
 *   ./build/bench/bench_udf [rows] [iters]
 */
#include "../include/tsdb.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <unistd.h>

#define DIRP "/tmp/tsdb_bench_udf"
#define SO   "build/test/udf_sample.so"

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* Run `sql` `iters` times; return best-of ns/row and drain every row so
 * materialisation is included where the query returns rows. */
static double timed(tsdb_db_t *db, const char *sql, int iters, int64_t nrows,
                    int64_t *out_seen)
{
    double best = 1e30;
    int64_t seen = 0;
    for (int it = 0; it < iters; it++) {
        tsdb_result_t *r = NULL;
        double t0 = now_s();
        int rc = tsdb_query(db, sql, &r);
        int64_t n = 0;
        if (rc == TSDB_OK && r) { while (tsdb_result_next(r) > 0) n++; }
        double dt = now_s() - t0;
        if (r) tsdb_result_free(r);
        if (rc != TSDB_OK) { *out_seen = -1; return -1.0; }
        if (dt < best) best = dt;
        seen = n;
    }
    *out_seen = seen;
    return best * 1e9 / (double)nrows;
}

static void row(const char *label, double nsrow, int64_t seen, double baseline) {
    if (nsrow < 0) { printf("  %-26s   QUERY FAILED\n", label); return; }
    printf("  %-26s %8.2f  %9.1f   %8lld", label, nsrow, 1000.0 / nsrow,
           (long long)seen);
    if (baseline > 0) printf("   %5.2fx", nsrow / baseline);
    printf("\n");
}

int main(int argc, char **argv) {
    int64_t N     = (argc > 1) ? strtoll(argv[1], NULL, 10) : 2000000;
    int     iters = (argc > 2) ? atoi(argv[2]) : 5;

    rm_rf(DIRP);
    tsdb_db_t *db = NULL;
    if (tsdb_open(DIRP, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); return 1; }

    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
        { "lo",  TSDB_TYPE_FLOAT64   },
        { "hi",  TSDB_TYPE_FLOAT64   },
        { "a",   TSDB_TYPE_INT64     },
        { "b",   TSDB_TYPE_INT64     },
    };
    if (tsdb_create_table(db, "u", cols, 6, "ts") != TSDB_OK) {
        fprintf(stderr, "create failed\n"); return 1;
    }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "u", &t) != TSDB_OK) { fprintf(stderr, "open tbl\n"); return 1; }

    double t0 = now_s();
    int64_t i = 0;
    while (i < N) {
        int64_t m = (N - i < 4096) ? (N - i) : 4096;
        tsdb_batch_t *bt = NULL;
        if (tsdb_batch_begin(t, &bt) != TSDB_OK) return 1;
        for (int64_t k = 0; k < m; k++) {
            int64_t g = i + k;
            tsdb_batch_row_ts(bt, 1700000000000000000LL + g * 1000000LL);
            tsdb_batch_row_f64(bt, 1, (double)(g % 1000) * 0.5 - 100.0);
            tsdb_batch_row_f64(bt, 2, -50.0);
            tsdb_batch_row_f64(bt, 3,  50.0);
            tsdb_batch_row_i64(bt, 4, g);
            tsdb_batch_row_i64(bt, 5, g * 2);
            tsdb_batch_row_end(bt);
        }
        if (tsdb_batch_commit(bt) != TSDB_OK) return 1;
        i += m;
    }
    double ingest = now_s() - t0;

    /* Register the sample UDFs. */
    const char *ddl[] = {
        "CREATE FUNCTION my_double(FLOAT64) RETURNS FLOAT64 FROM '" SO "' SYMBOL 'udf_double';",
        "CREATE FUNCTION my_add(INT64, INT64) RETURNS INT64 FROM '" SO "' SYMBOL 'udf_add';",
        "CREATE FUNCTION my_clamp(FLOAT64, FLOAT64, FLOAT64) RETURNS FLOAT64 FROM '" SO "' SYMBOL 'udf_clamp';",
    };
    for (size_t k = 0; k < sizeof(ddl) / sizeof(ddl[0]); k++) {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, ddl[k], &r);
        if (r) tsdb_result_free(r);
        if (rc != TSDB_OK) {
            fprintf(stderr, "CREATE FUNCTION failed (rc=%d %s)\n"
                    "  need %s — run `make build/test/udf_sample.so` first\n",
                    rc, tsdb_errstr(rc), SO);
            return 1;
        }
    }

    printf("=== bench_udf ===\n");
    printf("rows = %lld   iters = %d   ingest %.2fs (%.2f Mrows/s)\n\n",
           (long long)N, iters, ingest, (double)N / ingest / 1e6);
    printf("  query                       ns/row     Mrows/s        rows   vs native\n");
    printf("  -----                       ------     -------        ----   ---------\n");

    int64_t seen = 0;
    double proj   = timed(db, "SELECT val FROM u",                        iters, N, &seen);
    row("projection (floor)", proj, seen, 0);
    double nscale = timed(db, "SELECT val * 2.0 FROM u",                  iters, N, &seen);
    row("native  val * 2.0", nscale, seen, 0);
    double u1     = timed(db, "SELECT my_double(val) FROM u",             iters, N, &seen);
    row("udf     my_double(val)", u1, seen, nscale);
    double u2     = timed(db, "SELECT my_add(a, b) FROM u",               iters, N, &seen);
    row("udf     my_add(a,b)", u2, seen, nscale);
    double u3     = timed(db, "SELECT my_clamp(val, lo, hi) FROM u",      iters, N, &seen);
    row("udf     my_clamp(v,lo,hi)", u3, seen, nscale);

    double uwh = timed(db, "SELECT my_double(val) FROM u WHERE val > 0.0", iters, N, &seen);
    row("udf     + WHERE filter", uwh, seen, nscale);

    printf("\n  aggregate form\n");
    double nagg = timed(db, "SELECT avg(val) FROM u",                     iters, N, &seen);
    row("native  avg(val)", nagg, seen, 0);

    double uagg = timed(db, "SELECT avg(my_double(val)) FROM u",           iters, N, &seen);
    row("udf     avg(my_double(val))", uagg, seen, nagg);
    double uaggw = timed(db, "SELECT avg(my_double(val)) FROM u WHERE val > 0.0",
                          iters, N, &seen);
    row("udf     + WHERE filter", uaggw, seen, nagg);

    /* What the aggregate form replaces: with no server-side composition the
     * client has to pull every my_double(val) row and average it itself. */
    {
        double best = 1e30;
        for (int it = 0; it < iters; it++) {
            tsdb_result_t *r = NULL;
            double t0 = now_s();
            int rc = tsdb_query(db, "SELECT my_double(val) FROM u", &r);
            double sum = 0; int64_t n = 0;
            if (rc == TSDB_OK && r) {
                while (tsdb_result_next(r) > 0) { sum += tsdb_result_f64(r, 0); n++; }
            }
            double dt = now_s() - t0;
            if (r) tsdb_result_free(r);
            if (rc != TSDB_OK) { n = -1; break; }
            if (dt < best) best = dt;
            seen = n;
        }
        row("  same, averaged client-side", best * 1e9 / (double)N, seen, nagg);
        if (uagg > 0 && best > 0)
            printf("\n  aggregating the UDF in the engine instead of at the client:"
                   " %.2fx\n", (best * 1e9 / (double)N) / uagg);
    }

    if (proj > 0 && u1 > 0)
        printf("\n  UDF dispatch overhead above the projection floor: %.2f ns/row\n",
               u1 - proj);

    tsdb_close(db);
    rm_rf(DIRP);
    return 0;
}
