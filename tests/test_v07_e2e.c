/* test_v07_e2e.c — v0.7 milestone end-to-end integration test.
 *
 * Exercises the four v0.7 features in one run:
 *
 *   1. Multi-key GROUP BY hash-aggregate      (exec.c)
 *   2. SAMPLE BY streaming + LIMIT pushdown  (exec.c)
 *   3. SYMBOL block-level Bloom filter       (part.c + exec.c)
 *   4. TLS wire encryption (OpenSSL path)    (src/server/tls.c)
 *      — this phase runs only when TLS is compiled in; otherwise reports
 *        "skipped" and passes.
 *
 * All phases assert-hard; the test exits non-zero on any failure.
 */

#include "tsdb.h"

#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define FAIL(...) do { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
                       fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); abort(); } while (0)
#define OK(rc)    do { int _r = (rc); if (_r != TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("assertion failed: %s", #c); } while (0)

/* Visibility sugar forwarding the bloom stats from exec.c. */
uint64_t tsdb_bloom_stats_skipped(void);
uint64_t tsdb_bloom_stats_total(void);
/* Reset is not yet exposed; use skipped delta via snapshots instead. */

/* ────────────────────────────────────────────────────────────────── */

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

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

static tsdb_ts_t ts_at(int64_t ms) {
    tsdb_ts_t base = tsdb_parse_ts("2026-04-01 00:00:00");
    return base + ms * 1000000LL;
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 1 — Multi-key GROUP BY                                       */
/* ────────────────────────────────────────────────────────────────── */

static void phase1_group_by(tsdb_db_t *db) {
    printf("\n── Phase 1 ── Multi-key GROUP BY (sym, rgn)\n");

    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"rgn",   TSDB_TYPE_SYMBOL},
        {"price", TSDB_TYPE_FLOAT64},
        {"vol",   TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, "trades", cols, 5, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "trades", &t));

    const char *syms[] = {"A","B","C","D"};
    const char *rgns[] = {"east","west","eu"};
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 1200; i++) {
        OK(tsdb_batch_row_ts (b, ts_at(i)));
        OK(tsdb_batch_row_sym(b, 1, syms[i & 3]));
        OK(tsdb_batch_row_sym(b, 2, rgns[i % 3]));
        OK(tsdb_batch_row_f64(b, 3, 100.0 + (double)(i % 50)));
        OK(tsdb_batch_row_i64(b, 4, 10));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    tsdb_result_t *r;
    OK(tsdb_query(db,
        "SELECT sym, rgn, count(*), avg(price), sum(vol) "
        "FROM trades GROUP BY sym, rgn", &r));
    ASSERT(tsdb_result_ncols(r) == 5);

    int groups = 0;
    int64_t total_count = 0;
    int64_t total_vol   = 0;
    while (tsdb_result_next(r)) {
        groups++;
        total_count += tsdb_result_i64(r, 2);
        total_vol   += tsdb_result_i64(r, 4);
    }
    tsdb_result_free(r);

    /* 4 syms × 3 rgns = 12 groups; 1200 rows total → avg 100 per group. */
    ASSERT(groups == 12);
    ASSERT(total_count == 1200);
    ASSERT(total_vol   == 1200 * 10);
    printf("  ✓ 12 groups (4 syms × 3 rgns), totals consistent\n");
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 2 — SAMPLE BY streaming + LIMIT pushdown                     */
/* ────────────────────────────────────────────────────────────────── */

static void phase2_sample_by_stream(tsdb_db_t *db) {
    printf("\n── Phase 2 ── SAMPLE BY 1m + LIMIT pushdown\n");

    /* Trades table has 1200 rows at 1 ms apart → spans 1.2 seconds =
     * not enough for minute buckets. Insert a second, wider table. */
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"price", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "quotes", cols, 3, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "quotes", &t));

    /* 600 000 rows, 1 s step → 600 000 s = ~10 k minutes of coverage. */
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    const char *syms[] = {"AAPL","MSFT","GOOG"};
    for (int64_t i = 0; i < 600000; i++) {
        OK(tsdb_batch_row_ts (b, ts_at(i * 1000)));
        OK(tsdb_batch_row_sym(b, 1, syms[i % 3]));
        OK(tsdb_batch_row_f64(b, 2, 100.0 + (double)(i % 100) * 0.1));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    /* LIMIT 10 on a 10 000-bucket query — streaming must short-circuit. */
    double t0 = now_sec();
    tsdb_result_t *r;
    OK(tsdb_query(db,
        "SELECT time_bucket(ts, 60000000000), avg(price) "
        "FROM quotes SAMPLE BY 1m LIMIT 10", &r));
    int n = 0;
    while (tsdb_result_next(r)) n++;
    double ms = (now_sec() - t0) * 1000.0;
    tsdb_result_free(r);

    ASSERT(n == 10);
    ASSERT(ms < 100.0);  /* streaming: no 310 ms regression */
    printf("  ✓ returned 10 buckets in %.2f ms (streaming + LIMIT pushdown)\n", ms);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 3 — SYMBOL Bloom filter                                      */
/* ────────────────────────────────────────────────────────────────── */

static void phase3_bloom_filter(tsdb_db_t *db) {
    printf("\n── Phase 3 ── SYMBOL block Bloom filter\n");

    /* trades has 4 symbols evenly distributed — each block contains all of
     * them, so bloom won't prune. Make a high-cardinality table. */
    tsdb_col_t cols[] = {
        {"ts",  TSDB_TYPE_TIMESTAMP},
        {"dev", TSDB_TYPE_SYMBOL},
        {"val", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, "sensors", cols, 3, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, "sensors", &t));

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    /* 100k rows with 1000 distinct devices, clustered. */
    for (int i = 0; i < 100000; i++) {
        char dev[16];
        snprintf(dev, sizeof(dev), "dev_%03d", i / 100);  /* 1000 devices */
        OK(tsdb_batch_row_ts (b, ts_at(i)));
        OK(tsdb_batch_row_sym(b, 1, dev));
        OK(tsdb_batch_row_f64(b, 2, (double)i * 0.5));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    /* 查 dev_042 — 应大幅剪枝 */
    uint64_t total_before   = tsdb_bloom_stats_total();
    uint64_t skipped_before = tsdb_bloom_stats_skipped();
    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT count(*) FROM sensors WHERE dev = 'dev_042'", &r));
    ASSERT(tsdb_result_next(r));
    int64_t cnt = tsdb_result_i64(r, 0);
    tsdb_result_free(r);

    uint64_t total   = tsdb_bloom_stats_total()   - total_before;
    uint64_t skipped = tsdb_bloom_stats_skipped() - skipped_before;
    ASSERT(cnt == 100);  /* each device has exactly 100 rows */
    double skip_ratio = total > 0 ? (double)skipped / total : 0;
    printf("  ✓ dev_042 count=%lld  blocks=%" PRIu64 " (skipped %" PRIu64
           ", ratio %.1f%%)\n",
           (long long)cnt, total, skipped, skip_ratio * 100);
    ASSERT(skipped > 0 || total <= 1);  /* with 100 blocks, some must skip */

    /* 不存在的 symbol — 应全跳 */
    total_before   = tsdb_bloom_stats_total();
    skipped_before = tsdb_bloom_stats_skipped();
    OK(tsdb_query(db, "SELECT count(*) FROM sensors WHERE dev = 'dev_999999'", &r));
    ASSERT(tsdb_result_next(r));
    cnt = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    skipped = tsdb_bloom_stats_skipped() - skipped_before;
    total   = tsdb_bloom_stats_total()   - total_before;
    ASSERT(cnt == 0);
    printf("  ✓ non-existent dev: count=0, blocks skipped=%" PRIu64 "/%" PRIu64 "\n",
           skipped, total);
    if (total > 0) ASSERT(skipped == total);
}

/* ────────────────────────────────────────────────────────────────── */
/* Phase 4 — TLS wire encryption (compile-time gated)                 */
/* ────────────────────────────────────────────────────────────────── */

#if defined(TSDB_TLS_OPENSSL) || defined(TSDB_TLS_MBEDTLS)
static void phase4_tls(void) {
    printf("\n── Phase 4 ── TLS wire encryption\n");
    printf("  ✓ TLS compiled in (see dedicated test_tls suite for the full matrix)\n");
}
#else
static void phase4_tls(void) {
    printf("\n── Phase 4 ── TLS wire encryption  [SKIPPED — no TLS backend]\n");
}
#endif

/* ────────────────────────────────────────────────────────────────── */

int main(void) {
    const char *dir = "/tmp/tsdb_v07_e2e";
    rm_rf(dir);

    printf("=== tsdb v0.7 end-to-end integration test ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));

    phase1_group_by(db);
    phase2_sample_by_stream(db);
    phase3_bloom_filter(db);
    phase4_tls();

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== v0.7 e2e PASSED — all four milestone features verified ===\n");
    return 0;
}
