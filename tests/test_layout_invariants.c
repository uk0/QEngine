/* test_layout_invariants.c — gate for the (d) compression layout reorder.
 *
 * Single TSBS-shape cpu table (one wide table, multiple hosts).
 * Asserts four query invariants against expected values computed
 * independently from the data we generate.  Runs the gate twice:
 *
 *   Mode A: sort_by_tag_col = -1   (default ts-sort flush)
 *   Mode B: sort_by_tag_col = host (counting-sort by host within block)
 *
 * Both modes MUST produce identical answers; physical layout differs,
 * query semantics do not.  XOR-of-bits multiset hash is order-independent
 * and exact, so any value drift in mode B is a real bug.
 *
 * Invariants:
 *   [A] SELECT count(*) FROM cpu                      — total rows
 *   [B] SELECT v multiset XOR                         — per-value bits
 *   [C] SELECT count(*) FROM cpu WHERE host='host_5'  — per-host count
 *   [D] SELECT count(*) FROM cpu WHERE ts in [a,b)    — ts-range count
 */

#include "tsdb.h"
#include "../src/storage/schema.h"
#include "../src/storage/db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)

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

static inline uint64_t xor_f64(uint64_t acc, double v) {
    uint64_t bits;
    memcpy(&bits, &v, 8);
    return acc ^ bits;
}

#define N_HOSTS  16
#define N_TICKS  800
#define TICK_NS  (10LL * 1000000000LL)

/* trigger ∈ {"explicit-setter", "env-var", "off"} selects how the flag
 * gets onto the schema; sort_by_tag_col in [-1, ncols) is the value the
 * test expects to find on the schema after the trigger fires. */
static int run_mode(const char *label,
                    const char *trigger,
                    int sort_by_tag_col,
                    int64_t *out_count, uint64_t *out_v_xor,
                    int64_t *out_host5, int64_t *out_range)
{
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/tsdb_test_layout_invariants_%s", label);
    rm_rf(dir);

    printf("--- mode %s (trigger=%s sort_by_tag_col=%d) ---\n",
           label, trigger, sort_by_tag_col);

    if (strcmp(trigger, "env-var") == 0 && sort_by_tag_col >= 0) {
        setenv("TSDB_BLOCK_SORT_BY_TAG", "host", 1);
    } else {
        unsetenv("TSDB_BLOCK_SORT_BY_TAG");
    }

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    tsdb_col_t cols[] = {
        {"ts",   TSDB_TYPE_TIMESTAMP},
        {"host", TSDB_TYPE_SYMBOL},
        {"v",    TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "cpu", cols, 3, "ts"));

    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "cpu", &t));

    /* Trigger paths:
     *   "explicit-setter": white-box — call the setter directly.  Used
     *                      to prove the engine-level invariants.
     *   "env-var":         opt-in — TSDB_BLOCK_SORT_BY_TAG=host was set
     *                      before tsdb_create_table, so the schema must
     *                      already reflect sort_by_tag_col after the
     *                      DDL returns.  Verify that, then write data.
     *   "off":             nothing set; flag remains -1.
     */
    tsdb_schema_t *s = tsdb_table_get_schema(t);
    assert(s != NULL);

    if (strcmp(trigger, "explicit-setter") == 0 && sort_by_tag_col >= 0) {
        OK(tsdb_schema_set_sort_by_tag_col(s, sort_by_tag_col));
    } else if (strcmp(trigger, "env-var") == 0) {
        if (s->sort_by_tag_col != sort_by_tag_col)
            FAIL("env-var trigger: expected schema flag=%d got=%d",
                 sort_by_tag_col, s->sort_by_tag_col);
        unsetenv("TSDB_BLOCK_SORT_BY_TAG");  /* don't leak to next mode */
    }

    double walk[N_HOSTS];
    for (int h = 0; h < N_HOSTS; h++) walk[h] = 50.0 + (double)h * 0.125;
    uint64_t lcg = 0xC001D00DULL;

    tsdb_ts_t base_ts = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_ts_t sub_step = TICK_NS / N_HOSTS;

    int64_t  expected_count       = 0;
    uint64_t expected_v_multiset  = 0;
    int64_t  expected_host5_count = 0;
    const int range_tick_lo = 100;
    const int range_tick_hi = 105;
    tsdb_ts_t ts_lo = base_ts + (tsdb_ts_t)range_tick_lo * TICK_NS;
    tsdb_ts_t ts_hi = base_ts + (tsdb_ts_t)range_tick_hi * TICK_NS;
    int64_t expected_range_count = 0;

    char host_str[N_HOSTS][16];
    for (int h = 0; h < N_HOSTS; h++) snprintf(host_str[h], sizeof(host_str[h]), "host_%d", h);

    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));

    for (int tick = 0; tick < N_TICKS; tick++) {
        tsdb_ts_t tick_base = base_ts + (tsdb_ts_t)tick * TICK_NS;
        for (int h = 0; h < N_HOSTS; h++) {
            tsdb_ts_t ts = tick_base + (tsdb_ts_t)h * sub_step;
            lcg = lcg * 6364136223846793005ULL + 1442695040888963407ULL;
            double step = ((double)(lcg >> 32) / (double)UINT32_MAX - 0.5) * 5.0;
            walk[h] += step;
            if (walk[h] < 0.0)   walk[h] = 0.0;
            if (walk[h] > 100.0) walk[h] = 100.0;
            double v = walk[h];

            OK(tsdb_batch_row_ts(b, ts));
            OK(tsdb_batch_row_sym(b, 1, host_str[h]));
            OK(tsdb_batch_row_f64(b, 2, v));
            OK(tsdb_batch_row_end(b));

            expected_count++;
            expected_v_multiset = xor_f64(expected_v_multiset, v);
            if (h == 5) expected_host5_count++;
            if (ts >= ts_lo && ts < ts_hi) expected_range_count++;
        }
    }
    OK(tsdb_batch_commit(b));

    printf("  expected: count=%lld  v_xor=%016llx  host5=%lld  range[%d,%d)=%lld\n",
           (long long)expected_count,
           (unsigned long long)expected_v_multiset,
           (long long)expected_host5_count,
           range_tick_lo, range_tick_hi,
           (long long)expected_range_count);

    {   /* [A] */
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM cpu", &r));
        assert(tsdb_result_next(r) == 1);
        int64_t got = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
        if (got != expected_count) FAIL("[A] count: got=%lld expected=%lld", (long long)got, (long long)expected_count);
        printf("  [A] count(*)                       = %lld  ✓\n", (long long)got);
    }
    {   /* [B] */
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT v FROM cpu", &r));
        uint64_t got = 0;
        int64_t n = 0;
        while (tsdb_result_next(r) == 1) {
            got = xor_f64(got, tsdb_result_f64(r, 0));
            n++;
        }
        tsdb_result_free(r);
        if (n != expected_count) FAIL("[B] projection count: got=%lld expected=%lld", (long long)n, (long long)expected_count);
        if (got != expected_v_multiset) FAIL("[B] v multiset xor: got=%016llx expected=%016llx", (unsigned long long)got, (unsigned long long)expected_v_multiset);
        printf("  [B] v multiset xor                 = %016llx  ✓\n", (unsigned long long)got);
    }
    {   /* [C] */
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM cpu WHERE host = 'host_5'", &r));
        assert(tsdb_result_next(r) == 1);
        int64_t got = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
        if (got != expected_host5_count) FAIL("[C] host=host_5 count: got=%lld expected=%lld", (long long)got, (long long)expected_host5_count);
        printf("  [C] count(*) WHERE host='host_5'   = %lld  ✓\n", (long long)got);
    }
    {   /* [D] */
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT count(*) FROM cpu WHERE ts >= %lld AND ts < %lld",
                 (long long)ts_lo, (long long)ts_hi);
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, sql, &r));
        assert(tsdb_result_next(r) == 1);
        int64_t got = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
        if (got != expected_range_count) FAIL("[D] ts-range count: got=%lld expected=%lld", (long long)got, (long long)expected_range_count);
        printf("  [D] count(*) WHERE ts in [%d,%d) = %lld  ✓\n", range_tick_lo, range_tick_hi, (long long)got);
    }

    *out_count = expected_count;
    *out_v_xor = expected_v_multiset;
    *out_host5 = expected_host5_count;
    *out_range = expected_range_count;

    tsdb_close(db);
    rm_rf(dir);
    return 0;
}

int main(void) {
    printf("=== layout invariants gate ===\n");
    printf("config: N_HOSTS=%d N_TICKS=%d total_rows=%d\n",
           N_HOSTS, N_TICKS, N_HOSTS * N_TICKS);

    int64_t  cA, c5A, rA, cB, c5B, rB, cC, c5C, rC;
    uint64_t xA, xB, xC;

    run_mode("off",     "off",              -1, &cA, &xA, &c5A, &rA);
    run_mode("on",      "explicit-setter",   1, &cB, &xB, &c5B, &rB);
    run_mode("env-var", "env-var",           1, &cC, &xC, &c5C, &rC);

    if (cA != cB || xA != xB || c5A != c5B || rA != rB
     || cA != cC || xA != xC || c5A != c5C || rA != rC) {
        FAIL("cross-mode mismatch:\n"
             "  off   (%lld %016llx %lld %lld)\n"
             "  on    (%lld %016llx %lld %lld)\n"
             "  envvar(%lld %016llx %lld %lld)",
             (long long)cA, (unsigned long long)xA, (long long)c5A, (long long)rA,
             (long long)cB, (unsigned long long)xB, (long long)c5B, (long long)rB,
             (long long)cC, (unsigned long long)xC, (long long)c5C, (long long)rC);
    }

    printf("\nALL PASS — all three triggers (off / explicit-setter / env-var) "
           "produce identical query answers.\n");
    return 0;
}
