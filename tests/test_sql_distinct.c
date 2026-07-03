/* test_sql_distinct.c — SELECT DISTINCT col[, col...].
 *
 * Grammar under test:
 *   SELECT DISTINCT col[, col...] FROM t [WHERE ...] [LIMIT n]
 *
 * Implemented as a parser-level rewrite onto the GROUP BY hash-aggregate
 * path (DISTINCT a, b == GROUP BY a, b projecting only the keys).
 *
 * Data: 6 rows → sym in {a,a,b,b,c}, val in {1,1,2,2,3,3}:
 *   (a,1) (a,1) (b,2) (b,2) (c,3) (c,3)
 *
 * Covers:
 *   1. single-column DISTINCT on SYMBOL collapses dupes
 *   2. single-column DISTINCT on INT64 collapses dupes
 *   3. multi-column DISTINCT emits unique tuples
 *   4. DISTINCT + WHERE
 *   5. DISTINCT + LIMIT
 *   6. error: DISTINCT with an aggregate
 *   7. error: DISTINCT *
 *   8. error: DISTINCT combined with GROUP BY
 */

#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

/* Row count of a query, or -1 on error. */
static int q_nrows(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, sql, &r) != TSDB_OK || !r) return -1;
    int n = 0;
    while (tsdb_result_next(r) > 0) n++;
    tsdb_result_free(r);
    return n;
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_sql_distinct";
    rmrf(dir);

    printf("=== test_sql_distinct ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"sym", TSDB_TYPE_SYMBOL},
        {"val", TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "ev", cols, 3, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "ev", &t));

    {
        static const struct { const char *sym; int64_t val; } rows[] = {
            {"a", 1}, {"a", 1}, {"b", 2}, {"b", 2}, {"c", 3}, {"c", 3},
        };
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < 6; i++) {
            OK(tsdb_batch_row_ts(b, 1000 + i));
            OK(tsdb_batch_row_sym(b, 1, rows[i].sym));
            OK(tsdb_batch_row_i64(b, 2, rows[i].val));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }

    /* [1] single SYMBOL column */
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT DISTINCT sym FROM ev", &r));
        int n = 0, mask = 0;
        CHECK(tsdb_result_ncols(r) == 1, "DISTINCT sym projects 1 column");
        while (tsdb_result_next(r) > 0) {
            const char *sv = tsdb_result_sym(r, 0);
            if (sv) {
                if (strcmp(sv, "a") == 0) mask |= 1;
                if (strcmp(sv, "b") == 0) mask |= 2;
                if (strcmp(sv, "c") == 0) mask |= 4;
            }
            n++;
        }
        tsdb_result_free(r);
        CHECK(n == 3 && mask == 7, "DISTINCT sym collapses 6 rows to {a,b,c}");
    }

    /* [2] single INT64 column */
    CHECK(q_nrows(db, "SELECT DISTINCT val FROM ev") == 3,
          "DISTINCT val collapses to 3 values");

    /* [3] multi-column */
    CHECK(q_nrows(db, "SELECT DISTINCT sym, val FROM ev") == 3,
          "DISTINCT sym, val emits 3 unique tuples");

    /* [4] with WHERE */
    CHECK(q_nrows(db, "SELECT DISTINCT sym FROM ev WHERE val >= 2") == 2,
          "DISTINCT + WHERE keeps {b,c}");

    /* [5] with LIMIT */
    CHECK(q_nrows(db, "SELECT DISTINCT sym FROM ev LIMIT 2") == 2,
          "DISTINCT + LIMIT 2 emits 2 rows");

    /* [6] aggregate is rejected */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT DISTINCT count(sym) FROM ev", &r);
        CHECK(rc != TSDB_OK, "DISTINCT with aggregate errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [7] star is rejected */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT DISTINCT * FROM ev", &r);
        CHECK(rc != TSDB_OK, "DISTINCT * errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [8] DISTINCT + GROUP BY is rejected */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "SELECT DISTINCT sym FROM ev GROUP BY sym", &r);
        CHECK(rc != TSDB_OK, "DISTINCT combined with GROUP BY errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    tsdb_close(db);
    rmrf(dir);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
