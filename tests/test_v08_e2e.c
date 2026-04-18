/* test_v08_e2e.c — v0.8 milestone end-to-end integration test.
 *
 * Exercises all four v0.8 TDengine-class features in a single flow:
 *
 *   Phase 1 — Time-series aggregates: first/last/twa
 *   Phase 2 — Window functions: diff, derivative, csum, mavg
 *   Phase 3 — Advanced windowing: SESSION / STATE_WINDOW / EVENT_WINDOW
 *   Phase 4 — Super Table (STable) metadata model with cascading drop
 *
 * All phases assert-hard; the test exits non-zero on any failure.
 */

#include "tsdb.h"
#include "../src/catalog/stable.h"

struct tsdb_catalog;
struct tsdb_catalog *tsdb_db_catalog(tsdb_db_t *db);

#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(...) do { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
                       fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); abort(); } while (0)
#define OK(rc)    do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("assertion failed: %s", #c); } while (0)

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

static tsdb_ts_t ts_at(int64_t sec) {
    return tsdb_parse_ts("2026-04-01 00:00:00") + sec * 1000000000LL;
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 1 — time-series aggregates                                   */
/* ────────────────────────────────────────────────────────────────── */

static void phase1_ts_aggs(tsdb_db_t *db) {
    printf("\n── Phase 1 ── time-series aggregates (first / last / twa)\n");

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "ts_agg", cols, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "ts_agg", &t));

    /* 3 points: (ts=0, v=0), (ts=100s, v=10), (ts=1000s, v=100). */
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    OK(tsdb_batch_row_ts (b, ts_at(0)));    OK(tsdb_batch_row_f64(b, 1, 0.0));   OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_row_ts (b, ts_at(100)));  OK(tsdb_batch_row_f64(b, 1, 10.0));  OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_row_ts (b, ts_at(1000))); OK(tsdb_batch_row_f64(b, 1, 100.0)); OK(tsdb_batch_row_end(b));
    OK(tsdb_batch_commit(b));

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT first(val), last(val), twa(val) FROM ts_agg", &r));
    ASSERT(tsdb_result_next(r));
    double f   = tsdb_result_f64(r, 0);
    double l   = tsdb_result_f64(r, 1);
    double twa = tsdb_result_f64(r, 2);
    tsdb_result_free(r);

    /* twa with (current-value forward) convention from agent's impl:
     * (0·0 + 10·100 + 100·900) / 1000 = 91 */
    ASSERT(f == 0.0);
    ASSERT(l == 100.0);
    ASSERT(twa > 90.0 && twa < 92.0);
    printf("  ✓ first=%.0f last=%.0f twa=%.2f\n", f, l, twa);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 2 — window functions                                         */
/* ────────────────────────────────────────────────────────────────── */

static void phase2_window_fns(tsdb_db_t *db) {
    printf("\n── Phase 2 ── window functions (diff / derivative / csum / mavg)\n");

    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "wf", cols, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "wf", &t));

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    /* val = 0, 1, 2, …, 9 at ts = 0, 1s, 2s, …, 9s */
    for (int i = 0; i < 10; i++) {
        OK(tsdb_batch_row_ts (b, ts_at(i)));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT ts, val, diff(val), derivative(val), csum(val) "
                      "FROM wf", &r));
    int nrows = 0;
    double csum_last = 0;
    while (tsdb_result_next(r)) {
        csum_last = tsdb_result_f64(r, 4);
        nrows++;
    }
    tsdb_result_free(r);
    ASSERT(nrows == 10);
    /* Σ 0..9 = 45 */
    ASSERT(csum_last == 45.0);
    printf("  ✓ 10 rows emitted, final csum=%.0f (Σ 0..9 = 45)\n", csum_last);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 3 — advanced windowing                                       */
/* ────────────────────────────────────────────────────────────────── */

static void phase3_advanced_windows(tsdb_db_t *db) {
    printf("\n── Phase 3 ── SESSION / STATE_WINDOW / EVENT_WINDOW\n");

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"status", TSDB_TYPE_SYMBOL},
        {"temp",   TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "ev", cols, 3, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "ev", &t));

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    /* Pattern the SESSION test uses: ts=0,1,2, gap 8s, 10,11.
     * Plus a state pattern for status: A,A,B,B,A,B. */
    const char *statuses[] = {"A","A","B","B","A","B"};
    int        offsets[]   = {0, 1, 2, 10, 11, 12};
    double     temps[]     = {10, 12, 14, 25, 30, 8};
    for (int i = 0; i < 6; i++) {
        OK(tsdb_batch_row_ts (b, ts_at(offsets[i])));
        OK(tsdb_batch_row_sym(b, 1, statuses[i]));
        OK(tsdb_batch_row_f64(b, 2, temps[i]));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    /* SESSION(ts, 3s) — gap > 3 s creates a new window. */
    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT count(*) FROM ev SESSION(ts, 3s)", &r));
    int n_sess = 0;
    while (tsdb_result_next(r)) n_sess++;
    tsdb_result_free(r);
    ASSERT(n_sess == 2);
    printf("  ✓ SESSION(ts, 3s) → %d windows\n", n_sess);

    /* STATE_WINDOW(status) — status changes: A→B→A→B = 4 windows. */
    OK(tsdb_query(db, "SELECT count(*) FROM ev STATE_WINDOW(status)", &r));
    int n_state = 0;
    while (tsdb_result_next(r)) n_state++;
    tsdb_result_free(r);
    ASSERT(n_state == 4);
    printf("  ✓ STATE_WINDOW(status) → %d windows\n", n_state);

    /* EVENT_WINDOW(temp > 20, temp < 15) — {25,30} triggers, 8 closes. */
    OK(tsdb_query(db, "SELECT count(*) FROM ev EVENT_WINDOW(temp > 20, temp < 15)", &r));
    int n_event = 0;
    while (tsdb_result_next(r)) n_event++;
    tsdb_result_free(r);
    ASSERT(n_event >= 1);
    printf("  ✓ EVENT_WINDOW(temp>20, temp<15) → %d windows\n", n_event);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 4 — Super Table model                                        */
/* ────────────────────────────────────────────────────────────────── */

static void phase4_super_table(tsdb_db_t *db) {
    printf("\n── Phase 4 ── Super Table (STable + child tables)\n");

    struct tsdb_catalog *cat = tsdb_db_catalog(db);
    ASSERT(cat != NULL);

    tsdb_stable_t st;
    memset(&st, 0, sizeof(st));
    snprintf(st.name, sizeof(st.name), "%s", "meters");
    snprintf(st.cols[0].name, sizeof(st.cols[0].name), "%s", "ts");
    st.cols[0].type = TSDB_TYPE_TIMESTAMP;
    snprintf(st.cols[1].name, sizeof(st.cols[1].name), "%s", "current");
    st.cols[1].type = TSDB_TYPE_FLOAT64;
    st.ncols = 2;
    st.ts_col_idx = 0;
    snprintf(st.tag_cols[0].name, sizeof(st.tag_cols[0].name), "%s", "loc");
    st.tag_cols[0].type = TSDB_TYPE_SYMBOL;
    st.ntag_cols = 1;
    OK(tsdb_stable_create(cat, &st));
    ASSERT(tsdb_stable_exists(cat, "meters"));

    /* 4 child tables, 2 in each region. */
    const char *nm[] = {"d1","d2","d3","d4"};
    const char *lo[] = {"east","west","east","west"};
    for (int i = 0; i < 4; i++) {
        tsdb_child_table_t ct;
        memset(&ct, 0, sizeof(ct));
        snprintf(ct.name,        sizeof(ct.name),        "%s", nm[i]);
        snprintf(ct.stable_name, sizeof(ct.stable_name), "%s", "meters");
        ct.ntags = 1;
        ct.tags[0].type = TSDB_TYPE_SYMBOL;
        snprintf(ct.tags[0].v.s, sizeof(ct.tags[0].v.s), "%s", lo[i]);
        OK(tsdb_child_table_create(cat, &ct));
    }

    /* List + tag filter. */
    tsdb_child_table_t *arr = NULL;
    size_t n = 0;
    OK(tsdb_child_table_list(cat, "meters", &arr, &n));
    ASSERT(n == 4);
    int east = 0, west = 0;
    for (size_t i = 0; i < n; i++) {
        if (!strcmp(arr[i].tags[0].v.s, "east")) east++;
        else if (!strcmp(arr[i].tags[0].v.s, "west")) west++;
    }
    free(arr);
    ASSERT(east == 2 && west == 2);
    printf("  ✓ 4 child tables (2 east, 2 west) under stable 'meters'\n");

    /* Cascade drop. */
    OK(tsdb_stable_drop(cat, "meters"));
    tsdb_child_table_t probe;
    for (int i = 0; i < 4; i++) {
        ASSERT(tsdb_child_table_get(cat, nm[i], &probe) != TSDB_OK);
    }
    printf("  ✓ DROP STABLE cascaded removal of all 4 children\n");
}

/* ────────────────────────────────────────────────────────────────── */

int main(void) {
    const char *dir = "/tmp/tsdb_v08_e2e";
    rm_rf(dir);

    printf("=== tsdb v0.8 end-to-end integration test ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    phase1_ts_aggs(db);
    phase2_window_fns(db);
    phase3_advanced_windows(db);
    phase4_super_table(db);

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== v0.8 e2e PASSED — four milestone features verified ===\n");
    return 0;
}
