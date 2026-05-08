/* test_layout_invariants.c — Pre-Session-2 gate for the (d) compression
 * layout reorder.  Documents the four query invariants that MUST hold
 * regardless of within-block row ordering, so a future tag-sort flush
 * (sort_by_tag_col >= 0) can be validated against this baseline by
 * re-running the same test.
 *
 * Single TSBS-shape cpu table (one wide table, multiple hosts), small
 * enough that every assertion is cheap but big enough to span block
 * boundaries.  Expected values are computed in this file from the data
 * we generate — never read back from the engine — so the gate stays
 * authoritative even when the engine layout changes.
 *
 * Invariants asserted:
 *   A. SELECT count(*) FROM cpu                        — total row count
 *   B. SELECT v multiset                               — XOR of all f64 bits
 *   C. SELECT count(*) FROM cpu WHERE host='host_5'    — per-host row count
 *   D. SELECT count(*) FROM cpu WHERE ts >= a AND ts < b — ts-range row count
 *
 * (A) and (C) cover row-level integrity; (B) covers value-level integrity
 * via an order-independent multiset hash; (D) covers the block-level
 * zone-map cull behavior under any row order.
 *
 * Today (sort_by_tag_col=-1, default): test passes.
 * Session 2 (sort_by_tag_col>=0):       test must still pass — same expected
 *                                       values, different physical layout.
 */

#include "tsdb.h"
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

/* Bit-exact XOR multiset hash for doubles — order-independent, exact,
 * detects any single-bit change.  Reinterpret each f64 as uint64 so
 * NaN payload bits (if any) are not silently normalised away. */
static inline uint64_t xor_f64(uint64_t acc, double v) {
    uint64_t bits;
    memcpy(&bits, &v, 8);
    return acc ^ bits;
}

#define N_HOSTS  16
#define N_TICKS  800
#define TICK_NS  (10LL * 1000000000LL)

int main(void) {
    const char *dir = "/tmp/tsdb_test_layout_invariants";
    rm_rf(dir);

    printf("=== layout invariants gate (pre-Session-2) ===\n");
    printf("config: N_HOSTS=%d N_TICKS=%d total_rows=%d\n",
           N_HOSTS, N_TICKS, N_HOSTS * N_TICKS);

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

    /* Per-host random walk state; deterministic via rand_r-style LCG so
     * rerunning the test produces byte-identical input. */
    double walk[N_HOSTS];
    for (int h = 0; h < N_HOSTS; h++) walk[h] = 50.0 + (double)h * 0.125;
    uint64_t lcg = 0xC001D00DULL;

    tsdb_ts_t base_ts = tsdb_parse_ts("2026-01-01 00:00:00");
    tsdb_ts_t sub_step = TICK_NS / N_HOSTS;

    /* Independently-computed expected values. */
    int64_t  expected_count       = 0;
    uint64_t expected_v_multiset  = 0;
    int64_t  expected_host5_count = 0;

    /* For invariant D pick a 5-tick window starting at tick 100. */
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

            /* Cheap deterministic random walk step. */
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

    printf("expected: count=%lld  v_xor=%016llx  host5=%lld  range[%d,%d)=%lld\n",
           (long long)expected_count,
           (unsigned long long)expected_v_multiset,
           (long long)expected_host5_count,
           range_tick_lo, range_tick_hi,
           (long long)expected_range_count);

    /* ---- A. SELECT count(*) ---- */
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM cpu", &r));
        assert(tsdb_result_next(r) == 1);
        int64_t got = tsdb_result_i64(r, 0);
        assert(tsdb_result_next(r) == 0);
        tsdb_result_free(r);
        if (got != expected_count) FAIL("[A] count: got=%lld expected=%lld", (long long)got, (long long)expected_count);
        printf("  [A] count(*)                       = %lld  ✓\n", (long long)got);
    }

    /* ---- B. SELECT v multiset (XOR-of-bits) ---- */
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT v FROM cpu", &r));
        uint64_t got = 0;
        int64_t n = 0;
        while (tsdb_result_next(r) == 1) {
            got = xor_f64(got, tsdb_result_f64(r, 0));
            n++;
        }
        tsdb_result_free(r);
        if (n != expected_count) FAIL("[B] row count under projection: got=%lld expected=%lld", (long long)n, (long long)expected_count);
        if (got != expected_v_multiset) FAIL("[B] v multiset xor: got=%016llx expected=%016llx", (unsigned long long)got, (unsigned long long)expected_v_multiset);
        printf("  [B] v multiset xor                 = %016llx  ✓\n", (unsigned long long)got);
    }

    /* ---- C. SELECT count(*) WHERE host='host_5' ---- */
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM cpu WHERE host = 'host_5'", &r));
        assert(tsdb_result_next(r) == 1);
        int64_t got = tsdb_result_i64(r, 0);
        tsdb_result_free(r);
        if (got != expected_host5_count) FAIL("[C] host=host_5 count: got=%lld expected=%lld", (long long)got, (long long)expected_host5_count);
        printf("  [C] count(*) WHERE host='host_5'   = %lld  ✓\n", (long long)got);
    }

    /* ---- D. SELECT count(*) WHERE ts in [ts_lo, ts_hi) ---- */
    {
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

    tsdb_close(db);
    rm_rf(dir);

    printf("\nALL PASS — Session-2 sort_by_tag_col flag must replay these same expected values.\n");
    return 0;
}
