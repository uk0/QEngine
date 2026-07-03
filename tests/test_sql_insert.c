/* test_sql_insert.c — INSERT INTO ... VALUES through the SQL layer.
 *
 * Grammar under test:
 *   INSERT INTO <table> [(col1,col2,...)] VALUES (v1,v2,...)[,(...)...]
 *
 * Covers:
 *   1. multi-tuple INSERT in schema order, verified via count(*)/max()/sum()
 *   2. explicit column list in a reordered order
 *   3. negative literals
 *   4. error: unknown table
 *   5. error: wrong arity (tuple width != column count)
 *   6. error: type mismatch (string into INT64, int into SYMBOL)
 *   7. error: column list that does not cover all columns
 *   8. failed INSERTs leave the row count unchanged (all-or-nothing)
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

static int64_t q_i64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, sql, &r) != TSDB_OK || !r) return INT64_MIN;
    int64_t v = INT64_MIN;
    if (tsdb_result_next(r) > 0) v = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    return v;
}

static double q_f64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, sql, &r) != TSDB_OK || !r) return NAN;
    double v = NAN;
    if (tsdb_result_next(r) > 0) v = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    return v;
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_sql_insert";
    rmrf(dir);

    printf("=== test_sql_insert ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db,
            "CREATE TABLE m (ts TIMESTAMP, v INT64, p FLOAT64, tag SYMBOL) "
            "TIMESTAMP(ts)", &r));
        tsdb_result_free(r);
    }

    /* [1] multi-tuple INSERT in schema order */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db,
            "INSERT INTO m VALUES "
            "(1000, 1, 1.5, 'a'), (2000, 2, 2.5, 'b'), (3000, 3, 3.5, 'a')", &r);
        CHECK(rc == TSDB_OK, "3-tuple INSERT returns OK");
        if (rc == TSDB_OK && r) {
            const char *st = NULL;
            if (tsdb_result_next(r) > 0) st = tsdb_result_sym(r, 0);
            CHECK(st && strncmp(st, "OK: inserted 3", 14) == 0,
                  "status row reports 3 rows inserted");
            tsdb_result_free(r);
        }
        CHECK(q_i64(db, "SELECT count(*) FROM m") == 3, "count(*)==3 after INSERT");
        CHECK(q_i64(db, "SELECT max(v) FROM m") == 3, "max(v)==3");
        CHECK(fabs(q_f64(db, "SELECT sum(p) FROM m") - 7.5) < 1e-9, "sum(p)==7.5");
        CHECK(q_i64(db, "SELECT max(ts) FROM m") == 3000, "max(ts)==3000 (bare-int ns)");
    }

    /* [2] explicit column list, reordered */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db,
            "INSERT INTO m (v, ts, tag, p) VALUES (4, 4000, 'c', 4.5)", &r);
        CHECK(rc == TSDB_OK, "column-list INSERT (reordered) returns OK");
        if (rc == TSDB_OK) tsdb_result_free(r);
        CHECK(q_i64(db, "SELECT count(*) FROM m") == 4, "count(*)==4 after column-list INSERT");
        CHECK(q_i64(db, "SELECT max(v) FROM m") == 4, "max(v)==4");
        CHECK(q_i64(db, "SELECT count(*) FROM m WHERE tag='c'") == 1,
              "reordered SYMBOL landed in the right column");
    }

    /* [3] negative literals */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db,
            "INSERT INTO m VALUES (5000, -5, -2.5, 'neg')", &r);
        CHECK(rc == TSDB_OK, "negative int/float literals accepted");
        if (rc == TSDB_OK) tsdb_result_free(r);
        CHECK(q_i64(db, "SELECT min(v) FROM m") == -5, "min(v)==-5");
    }

    /* [4] error: unknown table */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "INSERT INTO nosuch VALUES (1, 2, 3.0, 'x')", &r);
        CHECK(rc != TSDB_OK, "INSERT into unknown table errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [5] error: wrong arity */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "INSERT INTO m VALUES (6000, 6)", &r);
        CHECK(rc != TSDB_OK, "2-value tuple into 4-column table errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
        rc = tsdb_query(db, "INSERT INTO m VALUES (6000, 6, 6.5, 'z', 99)", &r);
        CHECK(rc != TSDB_OK, "5-value tuple into 4-column table errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
        /* mixed-width tuples are a parse error */
        rc = tsdb_query(db, "INSERT INTO m VALUES (1,2,3.0,'a'), (1,2)", &r);
        CHECK(rc != TSDB_OK, "mixed-width tuples error");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [6] error: type mismatch */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "INSERT INTO m VALUES (7000, 'notint', 7.5, 'd')", &r);
        CHECK(rc != TSDB_OK, "string into INT64 errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
        rc = tsdb_query(db, "INSERT INTO m VALUES (7000, 7, 7.5, 42)", &r);
        CHECK(rc != TSDB_OK, "int into SYMBOL errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
        rc = tsdb_query(db, "INSERT INTO m VALUES (7000, 7, 'notfloat', 'd')", &r);
        CHECK(rc != TSDB_OK, "string into FLOAT64 errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
        /* FLOAT64 accepts an int literal */
        rc = tsdb_query(db, "INSERT INTO m VALUES (7000, 7, 7, 'd')", &r);
        CHECK(rc == TSDB_OK, "int literal into FLOAT64 accepted");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [7] error: partial column list */
    {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "INSERT INTO m (ts, v) VALUES (8000, 8)", &r);
        CHECK(rc != TSDB_OK, "column list not covering all columns errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
        rc = tsdb_query(db, "INSERT INTO m (ts, v, p, p) VALUES (8000, 8, 8.5, 8.5)", &r);
        CHECK(rc != TSDB_OK, "duplicate column in list errors");
        if (rc == TSDB_OK) tsdb_result_free(r);
    }

    /* [8] failed INSERTs left the data untouched */
    CHECK(q_i64(db, "SELECT count(*) FROM m") == 6,
          "count(*)==6 — failed INSERTs wrote nothing");

    tsdb_close(db);
    rmrf(dir);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
