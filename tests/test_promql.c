/* test_promql.c — PromQL subset translation and formatting tests.
 *
 * Tests:
 *  1. Bare metric → QTL SELECT
 *  2. Metric + label filter → WHERE clause
 *  3. Multiple label filters
 *  4. rate(metric[5m]) → derivative
 *  5. avg_over_time(metric[5m]) → SAMPLE BY
 *  6. sum(metric) → scalar aggregate
 *  7. sum by (label) (metric) → GROUP BY
 *  8. avg(metric), count(metric)
 *  9. Unsupported expression → NULL + errbuf message
 * 10. format_result: execute QTL, verify JSON structure
 */

#include "tsdb.h"
#include "../src/server/promql.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); \
} while (0)

#define ASSERT(cond) do { \
    if (!(cond)) FAIL("assertion failed: %s", #cond); \
} while (0)

#define OK(rc) do { \
    int _rc = (rc); \
    if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); \
} while (0)

/* ---- Helpers ------------------------------------------------------------- */

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

/* Check that qtl contains needle as a substring. */
static void assert_contains(const char *qtl, const char *needle) {
    if (!qtl)               FAIL("QTL is NULL, expected to contain: %s", needle);
    if (!strstr(qtl, needle)) FAIL("QTL '%s' does not contain '%s'", qtl, needle);
}

/* Check that qtl matches exactly. */
static void assert_eq(const char *qtl, const char *expected) {
    if (!qtl) FAIL("QTL is NULL, expected: %s", expected);
    if (strcmp(qtl, expected) != 0)
        FAIL("QTL mismatch:\n  got:      '%s'\n  expected: '%s'", qtl, expected);
}

/* ---- Test 1: bare metric ------------------------------------------------- */
static void test_bare_metric(void) {
    printf("[1] bare metric → SELECT ts, value\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("http_requests_total", 0, 0, 0, err, sizeof(err));
    assert_eq(qtl, "SELECT ts, value FROM http_requests_total ORDER BY ts");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 2: metric + single label --------------------------------------- */
static void test_single_label(void) {
    printf("[2] metric{k=\"v\"} → WHERE k='v'\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("cpu{host=\"a\"}", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "FROM cpu");
    assert_contains(qtl, "WHERE host = 'a'");
    assert_contains(qtl, "ORDER BY ts");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 3: multiple label filters -------------------------------------- */
static void test_multi_label(void) {
    printf("[3] metric{k1=\"v1\",k2=\"v2\"} → WHERE ... AND ...\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("net_bytes{job=\"api\",region=\"us-east\"}", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "WHERE job = 'api'");
    assert_contains(qtl, "AND region = 'us-east'");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 4: rate() → derivative ---------------------------------------- */
static void test_rate(void) {
    printf("[4] rate(cpu[5m]) → derivative(value)\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("rate(cpu[5m])", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "derivative(value)");
    assert_contains(qtl, "FROM cpu");
    assert_contains(qtl, "ORDER BY ts");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 5: avg_over_time() → SAMPLE BY -------------------------------- */
static void test_avg_over_time(void) {
    printf("[5] avg_over_time(metric[5m]) → SAMPLE BY 5m\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("avg_over_time(cpu[5m])", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "avg(value)");
    assert_contains(qtl, "FROM cpu");
    assert_contains(qtl, "SAMPLE BY 5m");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 6: sum(metric) ------------------------------------------------- */
static void test_sum(void) {
    printf("[6] sum(metric) → SELECT sum(value) FROM metric\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("sum(cpu)", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "sum(value)");
    assert_contains(qtl, "FROM cpu");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 7: sum by (label) (metric) ------------------------------------- */
static void test_sum_by(void) {
    printf("[7] sum by (host) (cpu) → GROUP BY host\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("sum by (host) (cpu)", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "host, sum(value)");
    assert_contains(qtl, "FROM cpu");
    assert_contains(qtl, "GROUP BY host");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 8: avg and count aggregates ------------------------------------ */
static void test_avg_count(void) {
    printf("[8] avg(metric) / count(metric)\n");
    char err[256] = "";

    char *q1 = tsdb_promql_compile("avg(mem)", 0, 0, 0, err, sizeof(err));
    ASSERT(q1 != NULL);
    assert_contains(q1, "avg(value)");
    assert_contains(q1, "FROM mem");
    free(q1);

    char *q2 = tsdb_promql_compile("count(events)", 0, 0, 0, err, sizeof(err));
    ASSERT(q2 != NULL);
    assert_contains(q2, "count(*)");
    assert_contains(q2, "FROM events");
    free(q2);

    printf("    PASS\n");
}

/* ---- Test 9: unsupported expression → NULL + errbuf --------------------- */
static void test_unsupported(void) {
    printf("[9] unsupported → NULL + errbuf message\n");

    /* Regex matcher should fail */
    char err[256] = "";
    char *qtl = tsdb_promql_compile("cpu{host=~\"node.*\"}", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl == NULL);
    ASSERT(err[0] != '\0');
    printf("    regex → NULL, err='%s'\n", err);

    /* Trailing garbage */
    memset(err, 0, sizeof(err));
    qtl = tsdb_promql_compile("cpu @@ garbage", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl == NULL);
    printf("    trailing garbage → NULL, err='%s'\n", err);

    printf("    PASS\n");
}

/* ---- Test 10: format_result JSON structure ------------------------------- */
static void test_format_result(void) {
    printf("[10] format_result: JSON structure well-formed\n");

    const char *dir = "/tmp/tsdb_test_promql";
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    /* Create a simple metric table */
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"host",  TSDB_TYPE_SYMBOL},
        {"value", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "cpu", cols, 3, "ts"));

    tsdb_table_t *tbl = NULL;
    OK(tsdb_open_table(db, "cpu", &tbl));

    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(tbl, &b));

    /* Insert 5 rows for host=A and 5 for host=B */
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    for (int i = 0; i < 5; i++) {
        tsdb_ts_t ts = base + (tsdb_ts_t)i * 1000000000LL;
        OK(tsdb_batch_row_ts(b, ts));
        OK(tsdb_batch_row_sym(b, 1, "A"));
        OK(tsdb_batch_row_f64(b, 2, 10.0 + i));
        OK(tsdb_batch_row_end(b));
    }
    for (int i = 0; i < 5; i++) {
        tsdb_ts_t ts = base + (tsdb_ts_t)i * 1000000000LL;
        OK(tsdb_batch_row_ts(b, ts));
        OK(tsdb_batch_row_sym(b, 1, "B"));
        OK(tsdb_batch_row_f64(b, 2, 20.0 + i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    /* Test 10a: bare metric query */
    printf("  [10a] bare metric JSON\n");
    {
        char *qtl = tsdb_promql_compile("cpu", 0, 0, 0, NULL, 0);
        ASSERT(qtl != NULL);
        size_t jlen = 0;
        char *json = tsdb_promql_format_result(db, qtl, "cpu", NULL, &jlen);
        ASSERT(json != NULL);
        ASSERT(jlen > 0);
        /* Verify structure */
        assert_contains(json, "\"status\":\"success\"");
        assert_contains(json, "\"resultType\":\"matrix\"");
        assert_contains(json, "\"__name__\":\"cpu\"");
        assert_contains(json, "\"values\":[");
        printf("    JSON (%zu bytes): %.200s...\n", jlen, json);
        free(json);
        free(qtl);
    }

    /* Test 10b: sum by (host) — grouped result */
    printf("  [10b] sum by (host) grouped JSON\n");
    {
        char err[256] = "";
        char *qtl = tsdb_promql_compile("sum by (host) (cpu)", 0, 0, 0, err, sizeof(err));
        ASSERT(qtl != NULL);
        printf("    QTL: %s\n", qtl);
        size_t jlen = 0;
        char *json = tsdb_promql_format_result(db, qtl, "cpu", "host", &jlen);
        ASSERT(json != NULL);
        /* Both groups should appear */
        assert_contains(json, "\"host\":\"A\"");
        assert_contains(json, "\"host\":\"B\"");
        printf("    JSON (%zu bytes): %.300s...\n", jlen, json);
        free(json);
        free(qtl);
    }

    /* Test 10c: format_result with NULL db → NULL return */
    printf("  [10c] NULL db → NULL\n");
    {
        size_t jlen = 0;
        char *json = tsdb_promql_format_result(NULL, "SELECT ts, value FROM cpu ORDER BY ts", "cpu", NULL, &jlen);
        ASSERT(json == NULL);
    }

    tsdb_close(db);
    rm_rf(dir);
    printf("    PASS\n");
}

/* ---- Test 11: rate() with label filter ----------------------------------- */
static void test_rate_with_labels(void) {
    printf("[11] rate(cpu{host=\"A\"}[5m]) → WHERE + derivative\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("rate(cpu{host=\"A\"}[5m])", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "derivative(value)");
    assert_contains(qtl, "WHERE host = 'A'");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 12: avg_over_time with 1h window ------------------------------- */
static void test_avg_over_time_1h(void) {
    printf("[12] avg_over_time(metric[1h]) → SAMPLE BY 1h\n");
    char err[256] = "";
    char *qtl = tsdb_promql_compile("avg_over_time(mem[1h])", 0, 0, 0, err, sizeof(err));
    ASSERT(qtl != NULL);
    assert_contains(qtl, "SAMPLE BY 1h");
    free(qtl);
    printf("    PASS\n");
}

/* ---- Test 13: min / max aggregates --------------------------------------- */
static void test_min_max(void) {
    printf("[13] min/max aggregates\n");
    char err[256] = "";
    char *q1 = tsdb_promql_compile("min(disk)", 0, 0, 0, err, sizeof(err));
    ASSERT(q1 != NULL);
    assert_contains(q1, "min(value)");
    free(q1);
    char *q2 = tsdb_promql_compile("max(disk)", 0, 0, 0, err, sizeof(err));
    ASSERT(q2 != NULL);
    assert_contains(q2, "max(value)");
    free(q2);
    printf("    PASS\n");
}

/* =========================================================================
 * main
 * ========================================================================= */
int main(void) {
    printf("=== tsdb PromQL translation tests ===\n\n");

    test_bare_metric();
    test_single_label();
    test_multi_label();
    test_rate();
    test_avg_over_time();
    test_sum();
    test_sum_by();
    test_avg_count();
    test_unsupported();
    test_format_result();
    test_rate_with_labels();
    test_avg_over_time_1h();
    test_min_max();

    printf("\n=== ALL PASS ===\n");
    return 0;
}
