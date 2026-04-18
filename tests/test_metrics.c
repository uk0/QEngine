/* test_metrics.c — Unit tests for src/server/metrics.c
 *
 * Tests:
 *  1. counter inc/add basic correctness
 *  2. gauge set / inc / dec
 *  3. histogram observe + bucket counts
 *  4. render: HELP, TYPE lines present; histogram bucket lines present
 *  5. concurrency: 10 threads each inc 10K times → total == 100K
 */

#include "../src/server/metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <assert.h>
#include <inttypes.h>

/* ---- Test harness --------------------------------------------------------- */

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
        g_fail++; \
    } else { \
        printf("PASS: %s\n", msg); \
        g_pass++; \
    } \
} while(0)

#define CHECK_EQ_U64(a, b, msg) do { \
    uint64_t _a = (uint64_t)(a), _b = (uint64_t)(b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL [%s:%d]: %s (got %" PRIu64 " want %" PRIu64 ")\n", \
                __FILE__, __LINE__, msg, _a, _b); \
        g_fail++; \
    } else { \
        printf("PASS: %s\n", msg); \
        g_pass++; \
    } \
} while(0)

/* ---- Test 1: counter ------------------------------------------------------- */

static void test_counter(void) {
    printf("\n--- Test 1: counter ---\n");
    tsdb_metrics_init();

    /* inc once */
    tsdb_metric_inc("qengine_queries_total");
    /* add 99 more */
    tsdb_metric_add("qengine_queries_total", 99);

    /* render and verify the value appears */
    size_t len;
    char *out = tsdb_metrics_render(&len);
    CHECK(out != NULL, "render returns non-NULL");

    /* Find "qengine_queries_total 100" in output. */
    int found = (strstr(out, "qengine_queries_total 100") != NULL);
    CHECK(found, "counter value 100 present in render");
    free(out);

    /* rows_written_total: add 500 */
    tsdb_metric_add("qengine_rows_written_total", 500);
    out = tsdb_metrics_render(&len);
    CHECK(out != NULL, "render non-NULL (rows)");
    CHECK(strstr(out, "qengine_rows_written_total 500") != NULL,
          "rows_written_total 500 in render");
    free(out);
}

/* ---- Test 2: gauge --------------------------------------------------------- */

static void test_gauge(void) {
    printf("\n--- Test 2: gauge ---\n");

    tsdb_metric_gauge_set("qengine_memtable_rows", 42.0);
    size_t len;
    char *out = tsdb_metrics_render(&len);
    CHECK(out != NULL, "render non-NULL (gauge)");
    CHECK(strstr(out, "qengine_memtable_rows 42") != NULL,
          "gauge value 42 in render");
    free(out);

    tsdb_metric_gauge_set("qengine_connections_active", 3.0);
    tsdb_metric_gauge_inc("qengine_connections_active");
    /* should be 4 */
    out = tsdb_metrics_render(&len);
    CHECK(out != NULL, "render non-NULL (gauge inc)");
    CHECK(strstr(out, "qengine_connections_active 4") != NULL,
          "gauge inc to 4");
    free(out);

    tsdb_metric_gauge_dec("qengine_connections_active");
    out = tsdb_metrics_render(&len);
    CHECK(strstr(out, "qengine_connections_active 3") != NULL,
          "gauge dec to 3");
    free(out);
}

/* ---- Test 3: histogram ----------------------------------------------------- */

static void test_histogram(void) {
    printf("\n--- Test 3: histogram ---\n");

    /* Observe 10 samples:
     *   5 at 0.1 ms  → falls in le=0.5 and all higher buckets
     *   3 at 2.0 ms  → falls in le=5 and higher (NOT le=0.5, NOT le=1)
     *   2 at 200 ms  → falls in le=500 and higher
     *
     * Expected bucket counts:
     *   le=0.5:    5   (only the 0.1 ms ones)
     *   le=1:      5   (same)
     *   le=5:      8   (5 + 3)
     *   le=10:     8
     *   le=50:     8
     *   le=100:    8
     *   le=500:   10   (5 + 3 + 2)
     *   le=1000:  10
     *   le=+Inf:  10
     *   _count:   10
     */
    for (int i = 0; i < 5; i++) tsdb_metric_observe("qengine_query_duration_ms", 0.1);
    for (int i = 0; i < 3; i++) tsdb_metric_observe("qengine_query_duration_ms", 2.0);
    for (int i = 0; i < 2; i++) tsdb_metric_observe("qengine_query_duration_ms", 200.0);

    size_t len;
    char *out = tsdb_metrics_render(&len);
    CHECK(out != NULL, "render non-NULL (histogram)");

    /* Verify specific bucket lines. */
    CHECK(strstr(out, "qengine_query_duration_ms_bucket{le=\"0.5\"} 5") != NULL,
          "histogram le=0.5 bucket=5");
    CHECK(strstr(out, "qengine_query_duration_ms_bucket{le=\"1\"} 5") != NULL,
          "histogram le=1 bucket=5");
    CHECK(strstr(out, "qengine_query_duration_ms_bucket{le=\"5\"} 8") != NULL,
          "histogram le=5 bucket=8");
    CHECK(strstr(out, "qengine_query_duration_ms_bucket{le=\"500\"} 10") != NULL,
          "histogram le=500 bucket=10");
    CHECK(strstr(out, "qengine_query_duration_ms_bucket{le=\"+Inf\"} 10") != NULL,
          "histogram le=+Inf bucket=10");
    CHECK(strstr(out, "qengine_query_duration_ms_count 10") != NULL,
          "histogram count=10");

    /* _sum: 5*0.1 + 3*2.0 + 2*200.0 = 0.5 + 6.0 + 400.0 = 406.5 */
    CHECK(strstr(out, "qengine_query_duration_ms_sum") != NULL,
          "histogram sum line present");

    free(out);
}

/* ---- Test 4: render format ------------------------------------------------- */

static void test_render_format(void) {
    printf("\n--- Test 4: render format ---\n");

    size_t len;
    char *out = tsdb_metrics_render(&len);
    CHECK(out != NULL, "render non-NULL");
    CHECK(len > 0, "render length > 0");

    /* Every metric must have # HELP and # TYPE lines. */
    CHECK(strstr(out, "# HELP qengine_connections_total") != NULL,
          "HELP line present for connections_total");
    CHECK(strstr(out, "# TYPE qengine_connections_total counter") != NULL,
          "TYPE counter line present");
    CHECK(strstr(out, "# HELP qengine_connections_active") != NULL,
          "HELP line present for connections_active");
    CHECK(strstr(out, "# TYPE qengine_connections_active gauge") != NULL,
          "TYPE gauge line present");
    CHECK(strstr(out, "# HELP qengine_query_duration_ms") != NULL,
          "HELP line present for query_duration_ms");
    CHECK(strstr(out, "# TYPE qengine_query_duration_ms histogram") != NULL,
          "TYPE histogram line present");
    CHECK(strstr(out, "qengine_query_duration_ms_sum") != NULL,
          "histogram _sum line");
    CHECK(strstr(out, "qengine_query_duration_ms_count") != NULL,
          "histogram _count line");

    /* Content-Type string: not in the payload itself, but verify
     * text/plain version 0.0.4 format cues (bucket labels). */
    CHECK(strstr(out, "{le=\"+Inf\"}") != NULL, "+Inf bucket present in output");

    free(out);
}

/* ---- Test 5: concurrency --------------------------------------------------- */

#define CONCURRENT_THREADS   10
#define INC_PER_THREAD    10000

static void *concurrent_inc(void *arg) {
    (void)arg;
    for (int i = 0; i < INC_PER_THREAD; i++) {
        tsdb_metric_inc("qengine_flushes_total");
    }
    return NULL;
}

static void test_concurrency(void) {
    printf("\n--- Test 5: concurrency ---\n");

    pthread_t threads[CONCURRENT_THREADS];
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
        pthread_create(&threads[i], NULL, concurrent_inc, NULL);
    }
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    size_t len;
    char *out = tsdb_metrics_render(&len);
    CHECK(out != NULL, "render non-NULL (concurrent)");

    /* 10 threads × 10K incs = 100K flushes_total */
    uint64_t expected = (uint64_t)CONCURRENT_THREADS * INC_PER_THREAD;
    char expected_str[64];
    snprintf(expected_str, sizeof(expected_str),
             "qengine_flushes_total %" PRIu64, expected);
    int found = (strstr(out, expected_str) != NULL);

    if (!found) {
        /* Print what we got for debugging. */
        fprintf(stderr, "  Expected to find: \"%s\"\n", expected_str);
        /* Find the actual line for flushes_total. */
        const char *p = strstr(out, "qengine_flushes_total ");
        if (p) {
            char line[128];
            int j = 0;
            while (p[j] && p[j] != '\n' && j < 127) { line[j] = p[j]; j++; }
            line[j] = '\0';
            fprintf(stderr, "  Actual line: \"%s\"\n", line);
        }
    }
    CHECK(found, "concurrent 100K flushes counted correctly");

    free(out);
}

/* ---- Main ------------------------------------------------------------------ */

int main(void) {
    printf("=== test_metrics ===\n");
    tsdb_metrics_init();

    test_counter();
    test_gauge();
    test_histogram();
    test_render_format();
    test_concurrency();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return (g_fail > 0) ? 1 : 0;
}
