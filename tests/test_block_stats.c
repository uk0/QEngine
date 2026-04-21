/* test_block_stats.c — V3 block stats fast-path parity + counter tests.
 *
 * For each covered aggregate, the answer must match what a full scan
 * produces.  We exercise both paths directly by flipping
 * TSDB_DISABLE_STATS_FASTPATH via setenv between runs, rebuilding a
 * fresh tsdb_result_t each time.
 *
 * The fast-path counter (qengine_agg_stats_hit_total) is also asserted
 * to increment so we know the optimisation actually fired — without
 * this the test would silently regress to "scan always".
 */

#include "../include/tsdb.h"
#include "../src/server/metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <sys/stat.h>
#include <math.h>

#define ASSERT(cond) do { \
    if (!(cond)) { fprintf(stderr, "ASSERT: %s [%s:%d]\n", #cond, __FILE__, __LINE__); \
                   abort(); } } while (0)

#define NEAR(a, b, eps) (fabs((double)(a) - (double)(b)) <= (eps))

static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

/* Run a single aggregate query against (db, table) and return the
 * first row's cells.  Caller provides out_sum/out_min/out_max/out_cnt
 * pointers for the four fields the query yields. */
static void run_agg(tsdb_db_t *db, const char *q,
                    double *out_sum, double *out_min, double *out_max,
                    int64_t *out_cnt)
{
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, q, &r);
    if (rc != 0 || !r) { fprintf(stderr, "query [%s] rc=%d\n", q, rc); abort(); }
    ASSERT(tsdb_result_ncols(r) == 4);
    ASSERT(tsdb_result_next(r) > 0);
    *out_sum = tsdb_result_f64(r, 0);
    *out_min = tsdb_result_f64(r, 1);
    *out_max = tsdb_result_f64(r, 2);
    *out_cnt = tsdb_result_i64(r, 3);
    tsdb_result_free(r);
}

static int64_t gauge_counter_total(const char *name) {
    size_t len = 0;
    char *body = tsdb_metrics_render(&len);
    if (!body) return -1;
    int64_t out = -1;
    /* Parse text exposition: "<name> <value>" on one line. */
    char needle[128]; snprintf(needle, sizeof(needle), "\n%s ", name);
    char *p = strstr(body, needle);
    if (p) {
        p += strlen(needle);
        out = (int64_t)strtoll(p, NULL, 10);
    }
    free(body);
    return out;
}

int main(void) {
    tsdb_metrics_init();
    const char *dir = "/tmp/test_block_stats_db";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    ASSERT(tsdb_open(dir, &db) == TSDB_OK);

    /* CREATE TABLE met (ts, val FLOAT64) */
    tsdb_col_t cols[] = {
        { .name = "ts",  .type = TSDB_TYPE_TIMESTAMP },
        { .name = "val", .type = TSDB_TYPE_FLOAT64   },
    };
    ASSERT(tsdb_create_table(db, "met", cols, 2, "ts") == TSDB_OK);

    tsdb_table_t *tbl = NULL;
    ASSERT(tsdb_open_table(db, "met", &tbl) == TSDB_OK);

    /* 2 × 2048-row blocks — enough to force a flush. */
    const int N = 4096;
    const int64_t base = 1700000000LL * 1000000000LL;  /* ns */
    double expect_sum = 0, expect_min = 1e18, expect_max = -1e18;

    tsdb_batch_t *b = NULL;
    ASSERT(tsdb_batch_begin(tbl, &b) == TSDB_OK);
    for (int i = 0; i < N; i++) {
        double v = (double)i + 0.25;
        int64_t ts = base + (int64_t)i * 1000000LL;
        tsdb_batch_row_ts(b, ts);
        tsdb_batch_row_f64(b, 1, v);
        tsdb_batch_row_end(b);
        expect_sum += v;
        if (v < expect_min) expect_min = v;
        if (v > expect_max) expect_max = v;
    }
    ASSERT(tsdb_batch_commit(b) == TSDB_OK);
    tsdb_close(db);

    /* Reopen so the on-disk blocks are the only source. */
    ASSERT(tsdb_open(dir, &db) == TSDB_OK);

    const char *Q =
        "SELECT sum(val), min(val), max(val), count(*) FROM met";

    /* Pass 1 — fast-path enabled. */
    unsetenv("TSDB_DISABLE_STATS_FASTPATH");
    int64_t hits_before = gauge_counter_total("qengine_agg_stats_hit_total");
    double s1, mn1, mx1; int64_t c1;
    run_agg(db, Q, &s1, &mn1, &mx1, &c1);
    int64_t hits_after = gauge_counter_total("qengine_agg_stats_hit_total");
    printf("[fast]  sum=%.2f min=%.2f max=%.2f count=%lld  hits %lld→%lld\n",
           s1, mn1, mx1, (long long)c1,
           (long long)hits_before, (long long)hits_after);
    ASSERT(hits_after > hits_before);

    /* Pass 2 — fast-path disabled. */
    setenv("TSDB_DISABLE_STATS_FASTPATH", "1", 1);
    /* The flag is resolved once per worker (static cache).  Since each
     * tsdb_query spawns fresh workers, a new setenv takes effect on the
     * next query — but we still need a path that doesn't reuse the
     * initial cached value.  Close+reopen forces new workers. */
    tsdb_close(db);
    ASSERT(tsdb_open(dir, &db) == TSDB_OK);

    double s2, mn2, mx2; int64_t c2;
    run_agg(db, Q, &s2, &mn2, &mx2, &c2);
    printf("[scan]  sum=%.2f min=%.2f max=%.2f count=%lld\n",
           s2, mn2, mx2, (long long)c2);

    /* Parity: fast-path ≡ scan-path, within float rounding. */
    ASSERT(NEAR(s1, s2, 1e-6));
    ASSERT(NEAR(mn1, mn2, 1e-9));
    ASSERT(NEAR(mx1, mx2, 1e-9));
    ASSERT(c1 == c2);

    /* Expected values (computed during insert). */
    ASSERT(NEAR(s1, expect_sum, 1e-6));
    ASSERT(NEAR(mn1, expect_min, 1e-9));
    ASSERT(NEAR(mx1, expect_max, 1e-9));
    ASSERT(c1 == N);

    /* ---- spread(val) — dedicated check (stats-served) ---- */
    unsetenv("TSDB_DISABLE_STATS_FASTPATH");
    tsdb_close(db);
    ASSERT(tsdb_open(dir, &db) == TSDB_OK);
    {
        tsdb_result_t *r = NULL;
        ASSERT(tsdb_query(db, "SELECT spread(val) FROM met", &r) == 0);
        ASSERT(tsdb_result_next(r) > 0);
        double sp = tsdb_result_f64(r, 0);
        printf("[fast]  spread=%.4f (expected %.4f)\n",
               sp, expect_max - expect_min);
        ASSERT(NEAR(sp, expect_max - expect_min, 1e-9));
        tsdb_result_free(r);
    }

    tsdb_close(db);
    rm_rf(dir);
    unsetenv("TSDB_DISABLE_STATS_FASTPATH");

    printf("=== test_block_stats OK ===\n");
    return 0;
}
