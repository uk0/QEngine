/* test_sql_having.c — GROUP BY ... HAVING post-aggregation filter.
 *
 * Grammar under test:
 *   SELECT ... FROM t [WHERE ...] GROUP BY cols HAVING <cond> [LIMIT n]
 *
 * Data: 6 rows, 3 symbols:
 *   AAPL: price 10, 20, 30   (count=3, sum=60,  avg=20)
 *   MSFT: price  5,  5       (count=2, sum=10,  avg=5)
 *   GOOG: price 100          (count=1, sum=100, avg=100)
 *
 * Covers:
 *   1. HAVING sum(price) > 20        → AAPL, GOOG
 *   2. HAVING count(*) >= 2          → AAPL, MSFT
 *   3. HAVING avg(price) < 10        → MSFT
 *   4. HAVING <grouped SYMBOL> = 's' → single group
 *   5. HAVING via projection alias
 *   6. AND-combined conditions
 *   7. HAVING + LIMIT (limit applies after the filter)
 *   8. error: HAVING without GROUP BY
 *   9. error: HAVING references a non-projected aggregate
 *  10. error: HAVING references an unknown column
 */

#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static int g_pass = 0, g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

#define OK(rc) do { \
    int _rc = (rc); \
    if (_rc != TSDB_OK) { \
        fprintf(stderr, "FATAL %s:%d: rc=%d (%s)\n", __FILE__, __LINE__, _rc, tsdb_errstr(_rc)); \
        exit(1); \
    } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    (void)system(cmd);
}

/* Run a (sym, agg) 2-col group query; return #rows and record which of
 * AAPL/MSFT/GOOG appeared (bitmask bit0/1/2). */
static int q_groups(tsdb_db_t *db, const char *sql, int *out_mask) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc != TSDB_OK || !r) return -1;
    int n = 0, mask = 0;
    while (tsdb_result_next(r) > 0) {
        const char *sym = tsdb_result_sym(r, 0);
        if (sym) {
            if (strcmp(sym, "AAPL") == 0) mask |= 1;
            if (strcmp(sym, "MSFT") == 0) mask |= 2;
            if (strcmp(sym, "GOOG") == 0) mask |= 4;
        }
        n++;
    }
    tsdb_result_free(r);
    if (out_mask) *out_mask = mask;
    return n;
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_sql_having";
    rmrf(dir);

    printf("=== test_sql_having ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"price", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "trades", cols, 3, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "trades", &t));

    {
        static const struct { const char *sym; double price; } rows[] = {
            {"AAPL", 10}, {"AAPL", 20}, {"AAPL", 30},
            {"MSFT", 5},  {"MSFT", 5},
            {"GOOG", 100},
        };
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < 6; i++) {
            OK(tsdb_batch_row_ts(b, 1000 + i));
            OK(tsdb_batch_row_sym(b, 1, rows[i].sym));
            OK(tsdb_batch_row_f64(b, 2, rows[i].price));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }

    int mask = 0, n;

    /* [1] sum threshold */
    n = q_groups(db, "SELECT sym, sum(price) FROM trades GROUP BY sym "
                     "HAVING sum(price) > 20", &mask);
    CHECK(n == 2 && mask == (1 | 4), "HAVING sum(price) > 20 keeps AAPL+GOOG");

    /* [2] count threshold */
    n = q_groups(db, "SELECT sym, count(*) FROM trades GROUP BY sym "
                     "HAVING count(*) >= 2", &mask);
    CHECK(n == 2 && mask == (1 | 2), "HAVING count(*) >= 2 keeps AAPL+MSFT");

    /* [3] avg threshold */
    n = q_groups(db, "SELECT sym, avg(price) FROM trades GROUP BY sym "
                     "HAVING avg(price) < 10", &mask);
    CHECK(n == 1 && mask == 2, "HAVING avg(price) < 10 keeps MSFT only");

    /* [4] grouped SYMBOL column comparison */
    n = q_groups(db, "SELECT sym, count(*) FROM trades GROUP BY sym "
                     "HAVING sym = 'GOOG'", &mask);
    CHECK(n == 1 && mask == 4, "HAVING sym = 'GOOG' keeps GOOG only");
    n = q_groups(db, "SELECT sym, count(*) FROM trades GROUP BY sym "
                     "HAVING sym != 'GOOG'", &mask);
    CHECK(n == 2 && mask == (1 | 2), "HAVING sym != 'GOOG' keeps AAPL+MSFT");

    /* [5] alias reference */
    n = q_groups(db, "SELECT sym, sum(price) AS total FROM trades GROUP BY sym "
                     "HAVING total > 20", &mask);
    CHECK(n == 2 && mask == (1 | 4), "HAVING via alias 'total' works");

    /* [6] AND-combined */
    n = q_groups(db, "SELECT sym, sum(price), count(*) FROM trades GROUP BY sym "
                     "HAVING sum(price) > 20 AND count(*) >= 3", &mask);
    CHECK(n == 1 && mask == 1, "HAVING a AND b keeps AAPL only");

    /* [7] LIMIT applies to the filtered output */
    n = q_groups(db, "SELECT sym, count(*) FROM trades GROUP BY sym "
                     "HAVING count(*) >= 1 LIMIT 2", &mask);
    CHECK(n == 2, "HAVING ... LIMIT 2 emits 2 rows");

    /* [8] HAVING without GROUP BY errors cleanly */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT sum(price) FROM trades "
                                "HAVING sum(price) > 1", &r);
        CHECK(rc != TSDB_OK, "HAVING without GROUP BY errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
        rc = tsdb_query(db, "SELECT price FROM trades HAVING price > 1", &r);
        CHECK(rc != TSDB_OK, "HAVING on plain SELECT errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [9] non-projected aggregate reference errors */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT sym, count(*) FROM trades GROUP BY sym "
                                "HAVING sum(price) > 20", &r);
        CHECK(rc != TSDB_OK, "HAVING sum(price) errors when sum not projected");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [10] unknown column reference errors */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT sym, count(*) FROM trades GROUP BY sym "
                                "HAVING bogus > 1", &r);
        CHECK(rc != TSDB_OK, "HAVING on unknown column errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    tsdb_close(db);
    rmrf(dir);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
