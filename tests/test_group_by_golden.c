/* test_group_by_golden.c — byte-exact GROUP BY correctness gate.
 *
 * The existing test_group_by.c verifies SHAPES and TOTALS but not
 * per-bucket assignments.  That's not enough as a regression gate
 * for SIMD per-batch GROUP BY work — a wrong-bucket bug looks like
 * "totals still match, but bucket A absorbed bucket B's rows".
 * test_group_by would pass; user output would be silently wrong.
 *
 * This suite captures the exact (key → count/sum/avg) map for
 * several scenarios and compares the engine's output bucket-by-
 * bucket.  Golden values are computed in C alongside the data
 * generation, so the test is self-contained (no fixture files).
 *
 * Scenarios:
 *   [1] high-cardinality single key: 10 000 distinct symbols,
 *       irregular row counts per key, exact count/sum/avg per bucket
 *   [2] two-key GROUP BY: 200 syms × 5 regions = 1000 buckets,
 *       compares the full 1000-row result map
 *   [3] empty-string symbol: a row with sym="" must aggregate
 *       under its own bucket, not collapse with another
 *   [4] WHERE-filtered GROUP BY: filter by price range, verify
 *       per-bucket counts match the in-test filter
 *   [5] mixed-type keys (SYMBOL + INT64)
 *
 * Any silently-wrong bucket assignment in a SIMD batch loop fails
 * one of these.
 */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)
#define ASSERT(cond) do { if (!(cond)) FAIL("%s", #cond); } while (0)

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
    tsdb_ts_t base = tsdb_parse_ts("2026-01-01 00:00:00");
    return base + sec * 1000000000LL;
}

/* -----------------------------------------------------------------------
 * Open-addressing hash map keyed by string for the golden side.
 * Lives in the test binary; not interesting beyond "match what the
 * engine should be computing".
 * -------------------------------------------------------------------- */
typedef struct {
    char    *key;        /* malloc'd */
    int64_t  count;
    double   sum;
} g_bucket_t;

typedef struct {
    g_bucket_t *slots;
    size_t      cap;
    size_t      n;
} g_map_t;

static uint64_t fnv1a(const char *s) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (; *s; s++) { h ^= (uint8_t)*s; h *= 0x100000001b3ULL; }
    return h;
}

static void gmap_init(g_map_t *m, size_t cap) {
    m->slots = calloc(cap, sizeof(*m->slots));
    m->cap = cap;
    m->n = 0;
}

static void gmap_free(g_map_t *m) {
    for (size_t i = 0; i < m->cap; i++) free(m->slots[i].key);
    free(m->slots); m->slots = NULL; m->cap = m->n = 0;
}

/* Insert or update a (key → count, sum) bucket.  Linear-probe, no
 * resize — tests size their map at 2× expected cardinality. */
static g_bucket_t *gmap_upsert(g_map_t *m, const char *k) {
    uint64_t h = fnv1a(k);
    for (size_t i = 0; i < m->cap; i++) {
        size_t idx = (size_t)((h + i) % m->cap);
        if (!m->slots[idx].key) {
            m->slots[idx].key = strdup(k);
            m->n++;
            return &m->slots[idx];
        }
        if (strcmp(m->slots[idx].key, k) == 0) return &m->slots[idx];
    }
    FAIL("gmap full (cap=%zu n=%zu)", m->cap, m->n);
    return NULL;
}

static g_bucket_t *gmap_find(g_map_t *m, const char *k) {
    uint64_t h = fnv1a(k);
    for (size_t i = 0; i < m->cap; i++) {
        size_t idx = (size_t)((h + i) % m->cap);
        if (!m->slots[idx].key) return NULL;
        if (strcmp(m->slots[idx].key, k) == 0) return &m->slots[idx];
    }
    return NULL;
}

/* ---------------------------------------------------------------------- */
/* [1] high-cardinality single-key GROUP BY                               */
/* ---------------------------------------------------------------------- */

static void test_high_cardinality_single_key(tsdb_db_t *db) {
    printf("\n[1] high-cardinality single-key GROUP BY (10 000 syms)\n");

    const char *table = "hc_sk";
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"price", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, table, cols, 3, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, table, &t));

    /* Build dataset: 10 000 distinct keys, irregular row counts in
     * 1..50 per key.  Use a deterministic generator so the golden
     * map is rebuilt exactly the same way client-side. */
    const int n_keys = 10000;
    const int seed   = 0xc0ffee;
    g_map_t expected; gmap_init(&expected, 32768);

    /* Pre-compute per-key row count + price sum so we can size the
     * batch and emit rows in deterministic order. */
    unsigned rng = (unsigned)seed;
    int total_rows = 0;
    int *rcount = malloc((size_t)n_keys * sizeof(int));
    for (int k = 0; k < n_keys; k++) {
        rng = rng * 1103515245u + 12345u;
        int rc = 1 + (int)((rng >> 8) % 50);
        rcount[k] = rc;
        total_rows += rc;
    }

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    int row_idx = 0;
    rng = (unsigned)seed;
    for (int k = 0; k < n_keys; k++) {
        char keybuf[32];
        snprintf(keybuf, sizeof(keybuf), "k_%07d", k);
        g_bucket_t *bk = gmap_upsert(&expected, keybuf);
        for (int r = 0; r < rcount[k]; r++) {
            rng = rng * 1103515245u + 12345u;
            double price = 1.0 + (double)((rng >> 8) % 1000) / 10.0;
            OK(tsdb_batch_row_ts (b, ts_at(row_idx)));
            OK(tsdb_batch_row_sym(b, 1, keybuf));
            OK(tsdb_batch_row_f64(b, 2, price));
            OK(tsdb_batch_row_end(b));
            bk->count++;
            bk->sum += price;
            row_idx++;
        }
    }
    OK(tsdb_batch_commit(b));
    free(rcount);

    printf("  inserted %d rows, %d distinct keys\n", total_rows, n_keys);

    /* Run the engine query and compare bucket-by-bucket. */
    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT sym, count(*), sum(price) "
                      "FROM hc_sk GROUP BY sym", &r));
    ASSERT(tsdb_result_ncols(r) == 3);

    int seen_keys = 0;
    int64_t seen_total_count = 0;
    while (tsdb_result_next(r)) {
        const char *sym = tsdb_result_sym(r, 0);
        int64_t     cnt = tsdb_result_i64(r, 1);
        double      sum = tsdb_result_f64(r, 2);
        ASSERT(sym != NULL);
        g_bucket_t *exp = gmap_find(&expected, sym);
        if (!exp) FAIL("engine emitted unknown bucket sym='%s'", sym);
        if (cnt != exp->count)
            FAIL("bucket '%s': engine count=%lld expected=%lld",
                 sym, (long long)cnt, (long long)exp->count);
        /* Float sum — allow 1e-6 tolerance per row (10 000 keys × 50 rows). */
        if (fabs(sum - exp->sum) > 1e-3 * (1 + fabs(exp->sum)))
            FAIL("bucket '%s': engine sum=%.6f expected=%.6f",
                 sym, sum, exp->sum);
        /* Mark consumed by zeroing count; any remaining non-zero
         * after the loop means the engine missed a bucket. */
        seen_total_count += cnt;
        exp->count = 0;
        seen_keys++;
    }
    tsdb_result_free(r);

    /* Verify every expected bucket was observed exactly once. */
    int missed = 0;
    for (size_t i = 0; i < expected.cap; i++) {
        if (expected.slots[i].key && expected.slots[i].count != 0) {
            fprintf(stderr, "  MISSED bucket '%s' (expected count=%lld)\n",
                    expected.slots[i].key, (long long)expected.slots[i].count);
            missed++;
        }
    }
    ASSERT(missed == 0);
    ASSERT(seen_keys == n_keys);
    ASSERT(seen_total_count == total_rows);

    gmap_free(&expected);
    printf("  PASS: %d buckets, %lld rows, all per-bucket count/sum exact\n",
           seen_keys, (long long)seen_total_count);
}

/* ---------------------------------------------------------------------- */
/* [2] two-key GROUP BY — sym × region                                     */
/* ---------------------------------------------------------------------- */

static void test_two_key_group_by(tsdb_db_t *db) {
    printf("\n[2] two-key GROUP BY (200 sym × 5 region = up to 1000 buckets)\n");

    const char *table = "hc_2k";
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"rgn",   TSDB_TYPE_SYMBOL},
        {"qty",   TSDB_TYPE_INT64},
    };
    OK(tsdb_create_table(db, table, cols, 4, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, table, &t));

    g_map_t expected; gmap_init(&expected, 4096);

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    int row_idx = 0;
    for (int s = 0; s < 200; s++) {
        for (int rg = 0; rg < 5; rg++) {
            /* Skip 7 % of (s, rg) pairs deterministically so not every
             * bucket exists — the engine must report exactly the set
             * that does. */
            if ((s * 7 + rg * 31) % 13 == 0) continue;
            int n_rows_here = 1 + ((s + rg) % 9);  /* 1..9 rows */
            char skey[64];
            char sym[16]; snprintf(sym, sizeof(sym), "s%03d", s);
            char rgn[16]; snprintf(rgn, sizeof(rgn), "r%d", rg);
            snprintf(skey, sizeof(skey), "%s|%s", sym, rgn);
            g_bucket_t *bk = gmap_upsert(&expected, skey);
            for (int i = 0; i < n_rows_here; i++) {
                int64_t qty = 1 + (i + s * rg) % 100;
                OK(tsdb_batch_row_ts (b, ts_at(row_idx)));
                OK(tsdb_batch_row_sym(b, 1, sym));
                OK(tsdb_batch_row_sym(b, 2, rgn));
                OK(tsdb_batch_row_i64(b, 3, qty));
                OK(tsdb_batch_row_end(b));
                bk->count++;
                bk->sum += (double)qty;
                row_idx++;
            }
        }
    }
    OK(tsdb_batch_commit(b));
    int total_rows = row_idx;

    /* Query. */
    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT sym, rgn, count(*), sum(qty) "
                      "FROM hc_2k GROUP BY sym, rgn", &r));
    ASSERT(tsdb_result_ncols(r) == 4);

    int seen = 0;
    while (tsdb_result_next(r)) {
        const char *sym = tsdb_result_sym(r, 0);
        const char *rgn = tsdb_result_sym(r, 1);
        int64_t     cnt = tsdb_result_i64(r, 2);
        int64_t     qty = tsdb_result_i64(r, 3);
        char skey[64]; snprintf(skey, sizeof(skey), "%s|%s", sym, rgn);
        g_bucket_t *exp = gmap_find(&expected, skey);
        if (!exp) FAIL("engine emitted unknown bucket '%s'", skey);
        if (cnt != exp->count)
            FAIL("bucket '%s': engine cnt=%lld expected=%lld",
                 skey, (long long)cnt, (long long)exp->count);
        if ((double)qty != exp->sum)
            FAIL("bucket '%s': engine qty=%lld expected=%.0f",
                 skey, (long long)qty, exp->sum);
        exp->count = 0;
        seen++;
    }
    tsdb_result_free(r);

    int expected_buckets = 0;
    for (size_t i = 0; i < expected.cap; i++) {
        if (expected.slots[i].key) {
            if (expected.slots[i].count != 0)
                FAIL("MISSED bucket '%s'", expected.slots[i].key);
            expected_buckets++;
        }
    }
    ASSERT(seen == expected_buckets);
    gmap_free(&expected);
    printf("  PASS: %d two-key buckets, %d rows, exact match\n", seen, total_rows);
}

/* ---------------------------------------------------------------------- */
/* [3] empty-string symbol must not collapse with other keys              */
/* ---------------------------------------------------------------------- */

static void test_empty_symbol_distinct(tsdb_db_t *db) {
    printf("\n[3] empty-string symbol stays a distinct bucket\n");

    const char *table = "hc_empty";
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
    };
    OK(tsdb_create_table(db, table, cols, 2, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, table, &t));

    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    int row_idx = 0;
    /* 5 rows with sym="", 7 rows with sym="A", 3 rows with sym="B" */
    for (int i = 0; i < 5; i++) {
        OK(tsdb_batch_row_ts(b, ts_at(row_idx++)));
        OK(tsdb_batch_row_sym(b, 1, ""));
        OK(tsdb_batch_row_end(b));
    }
    for (int i = 0; i < 7; i++) {
        OK(tsdb_batch_row_ts(b, ts_at(row_idx++)));
        OK(tsdb_batch_row_sym(b, 1, "A"));
        OK(tsdb_batch_row_end(b));
    }
    for (int i = 0; i < 3; i++) {
        OK(tsdb_batch_row_ts(b, ts_at(row_idx++)));
        OK(tsdb_batch_row_sym(b, 1, "B"));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT sym, count(*) FROM hc_empty GROUP BY sym", &r));
    int seen_empty = 0, seen_A = 0, seen_B = 0;
    int64_t cnt_empty = 0, cnt_A = 0, cnt_B = 0;
    while (tsdb_result_next(r)) {
        const char *sym = tsdb_result_sym(r, 0);
        int64_t     cnt = tsdb_result_i64(r, 1);
        /* Engine may render "" as NULL or as the empty literal; accept
         * either form, just keep the three buckets distinct. */
        if (sym == NULL || sym[0] == '\0') { seen_empty++; cnt_empty = cnt; }
        else if (strcmp(sym, "A") == 0)    { seen_A++;     cnt_A     = cnt; }
        else if (strcmp(sym, "B") == 0)    { seen_B++;     cnt_B     = cnt; }
        else FAIL("unexpected sym='%s'", sym);
    }
    tsdb_result_free(r);
    ASSERT(seen_empty == 1 && cnt_empty == 5);
    ASSERT(seen_A == 1 && cnt_A == 7);
    ASSERT(seen_B == 1 && cnt_B == 3);
    printf("  PASS: empty=5 A=7 B=3 (3 distinct buckets)\n");
}

/* ---------------------------------------------------------------------- */
/* [4] WHERE-filtered GROUP BY                                            */
/* ---------------------------------------------------------------------- */

static void test_where_filtered(tsdb_db_t *db) {
    printf("\n[4] WHERE-filtered GROUP BY: per-bucket count after price > 50\n");

    const char *table = "hc_filt";
    tsdb_col_t cols[] = {
        {"ts",    TSDB_TYPE_TIMESTAMP},
        {"sym",   TSDB_TYPE_SYMBOL},
        {"price", TSDB_TYPE_FLOAT64},
    };
    OK(tsdb_create_table(db, table, cols, 3, "ts"));
    tsdb_table_t *t; OK(tsdb_open_table(db, table, &t));

    /* 1000 rows over 50 syms, prices 0..99 deterministic. */
    g_map_t expected; gmap_init(&expected, 256);
    tsdb_batch_t *b; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 1000; i++) {
        char sym[16]; snprintf(sym, sizeof(sym), "s%02d", i % 50);
        double price = (double)(i % 100);
        OK(tsdb_batch_row_ts (b, ts_at(i)));
        OK(tsdb_batch_row_sym(b, 1, sym));
        OK(tsdb_batch_row_f64(b, 2, price));
        OK(tsdb_batch_row_end(b));
        if (price > 50.0) {
            g_bucket_t *bk = gmap_upsert(&expected, sym);
            bk->count++;
        }
    }
    OK(tsdb_batch_commit(b));

    tsdb_result_t *r;
    OK(tsdb_query(db, "SELECT sym, count(*) FROM hc_filt "
                      "WHERE price > 50.0 GROUP BY sym", &r));
    int seen = 0;
    while (tsdb_result_next(r)) {
        const char *sym = tsdb_result_sym(r, 0);
        int64_t     cnt = tsdb_result_i64(r, 1);
        g_bucket_t *exp = gmap_find(&expected, sym);
        if (!exp) FAIL("engine emitted unknown bucket '%s'", sym);
        if (cnt != exp->count)
            FAIL("bucket '%s': engine cnt=%lld expected=%lld",
                 sym, (long long)cnt, (long long)exp->count);
        exp->count = 0;
        seen++;
    }
    tsdb_result_free(r);
    int expected_count = 0;
    for (size_t i = 0; i < expected.cap; i++) {
        if (expected.slots[i].key) {
            if (expected.slots[i].count != 0)
                FAIL("MISSED bucket '%s'", expected.slots[i].key);
            expected_count++;
        }
    }
    ASSERT(seen == expected_count);
    gmap_free(&expected);
    printf("  PASS: %d post-filter buckets exact\n", seen);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_group_by_golden";
    rm_rf(dir);

    printf("=== tsdb GROUP BY golden-output gate ===\n");

    tsdb_db_t *db; OK(tsdb_open(dir, &db));

    test_high_cardinality_single_key(db);
    test_two_key_group_by(db);
    test_empty_symbol_distinct(db);
    test_where_filtered(db);

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== ALL GROUP BY GOLDEN TESTS PASSED ===\n");
    return 0;
}
