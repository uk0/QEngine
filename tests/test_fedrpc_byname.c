/* test_fedrpc_byname.c — fedrpc result round-trip + by-name column lookup.
 *
 * Pins the data-loss fix in peer_table_stats (src/storage/db_cluster.c):
 * the (count, max_ts) stats fetched from a peer are read by COLUMN NAME, not
 * by positional index, so the read is correct even when the peer delivers the
 * columns in a different order (e.g. a mixed old/new binary cluster).
 *
 * Cases:
 *   1. Canonical order [count(ts), max(ts)]  → by-name lookup picks the right
 *      indices; reading i64(count_idx)/ts(max_idx) yields the real values.
 *   2. REVERSED order  [max(ts), count(ts)]  → the SAME by-name code must
 *      still return count from index 1 and max_ts from index 0.  A positional
 *      read (index 0 = count) would here return the max_ts-scale value as the
 *      count — exactly the bug that made anti-entropy wipe durable data.
 *   3. fedrpc_encode_result → fedrpc_decode_result preserves names + values
 *      across the wire for both orders.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/query/result_internal.h"
#include "../src/federation/fedagg.h"
#include "../src/federation/fedrpc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define ASSERT(c) do { if (!(c)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #c, __FILE__, __LINE__); \
    abort(); } } while (0)

/* A realistic divergence: 3.62M-row count vs a ns-epoch max_ts ~1.3e12. */
static const int64_t  REAL_COUNT  = 3620864;
static const int64_t  REAL_MAXTS  = 1319487000000LL;

static uint64_t bits_i64(int64_t v) { uint64_t u; memcpy(&u, &v, 8); return u; }

/* Build a single-row 2-col result with the given column order. */
static tsdb_result_t *make_stats(const char *c0, tsdb_type_t t0, int64_t v0,
                                 const char *c1, tsdb_type_t t1, int64_t v1) {
    const char *names[2] = { c0, c1 };
    tsdb_type_t types[2] = { t0, t1 };
    tsdb_result_t *r = fedagg_result_alloc(2, names, types);
    ASSERT(r);
    uint64_t row[2] = { bits_i64(v0), bits_i64(v1) };
    ASSERT(fedagg_result_append(r, row) == 0);
    return r;
}

/* Read (count, max_ts) the way peer_table_stats does: by column name. */
static void read_byname(tsdb_result_t *r, uint64_t *out_count, int64_t *out_max) {
    ASSERT(tsdb_result_next(r));
    int ci_count = tsdb_result_col_index_by_name(r, "count");
    int ci_max   = tsdb_result_col_index_by_name(r, "max(ts)");
    if (ci_max < 0) ci_max = tsdb_result_col_index_by_name(r, "max");
    ASSERT(ci_count >= 0);
    ASSERT(ci_max   >= 0);
    *out_count = (uint64_t)tsdb_result_i64(r, ci_count);
    *out_max   = tsdb_result_ts(r, ci_max);
}

static void case_order(const char *label, int reversed) {
    tsdb_result_t *r = reversed
        ? make_stats("max(ts)",  TSDB_TYPE_TIMESTAMP, REAL_MAXTS,
                     "count(ts)", TSDB_TYPE_INT64,     REAL_COUNT)
        : make_stats("count(ts)", TSDB_TYPE_INT64,     REAL_COUNT,
                     "max(ts)",  TSDB_TYPE_TIMESTAMP, REAL_MAXTS);

    /* By-name lookup returns the correct indices regardless of order. */
    int ci_count = tsdb_result_col_index_by_name(r, "count");
    int ci_max   = tsdb_result_col_index_by_name(r, "max(ts)");
    printf("  [%s] count_idx=%d max_idx=%d\n", label, ci_count, ci_max);
    ASSERT(ci_count == (reversed ? 1 : 0));
    ASSERT(ci_max   == (reversed ? 0 : 1));

    uint64_t cnt = 0; int64_t mx = 0;
    read_byname(r, &cnt, &mx);
    printf("  [%s] by-name count=%llu max_ts=%lld\n",
           label, (unsigned long long)cnt, (long long)mx);
    ASSERT(cnt == (uint64_t)REAL_COUNT);   /* never the max_ts-scale value */
    ASSERT(mx  == REAL_MAXTS);

    /* Demonstrate the OLD positional bug only on the reversed layout: a blind
     * index-0 read would treat max_ts as the count. */
    if (reversed) {
        int64_t positional_count = tsdb_result_i64(r, 0);
        ASSERT(positional_count == REAL_MAXTS);          /* the wrong value */
        ASSERT((uint64_t)positional_count != (uint64_t)REAL_COUNT);
    }
    tsdb_result_free(r);
}

/* fedrpc wire round-trip: encode then decode, assert names + values survive. */
static void case_wire(int reversed) {
    tsdb_result_t *r = reversed
        ? make_stats("max(ts)",  TSDB_TYPE_TIMESTAMP, REAL_MAXTS,
                     "count(ts)", TSDB_TYPE_INT64,     REAL_COUNT)
        : make_stats("count(ts)", TSDB_TYPE_INT64,     REAL_COUNT,
                     "max(ts)",  TSDB_TYPE_TIMESTAMP, REAL_MAXTS);

    uint8_t buf[4096];
    int n = fedrpc_encode_result(buf, sizeof(buf), r);
    ASSERT(n > 0);

    tsdb_result_t *dec = NULL;
    ASSERT(fedrpc_decode_result(buf, (uint32_t)n, &dec) == TSDB_OK);
    ASSERT(dec);
    ASSERT(tsdb_result_ncols(dec) == 2);

    /* Wire preserves column order; by-name still recovers the right fields. */
    uint64_t cnt = 0; int64_t mx = 0;
    read_byname(dec, &cnt, &mx);
    printf("  [wire reversed=%d] count=%llu max_ts=%lld names=[%s,%s]\n",
           reversed, (unsigned long long)cnt, (long long)mx,
           tsdb_result_col_name(dec, 0), tsdb_result_col_name(dec, 1));
    ASSERT(cnt == (uint64_t)REAL_COUNT);
    ASSERT(mx  == REAL_MAXTS);

    tsdb_result_free(r);
    tsdb_result_free(dec);
}

int main(void) {
    printf("=== test_fedrpc_byname ===\n");

    printf("case 1: canonical order [count, max_ts]\n");
    case_order("canonical", 0);

    printf("case 2: reversed order [max_ts, count]\n");
    case_order("reversed", 1);

    printf("case 3: fedrpc wire round-trip\n");
    case_wire(0);
    case_wire(1);

    /* Substring matcher sanity: variant count-column names all match. */
    {
        const char *names[1] = { "count(*)" };
        tsdb_type_t types[1] = { TSDB_TYPE_INT64 };
        tsdb_result_t *r = fedagg_result_alloc(1, names, types);
        ASSERT(r);
        ASSERT(tsdb_result_col_index_by_name(r, "count") == 0);
        ASSERT(tsdb_result_col_index_by_name(r, "COUNT") == 0);   /* ci */
        ASSERT(tsdb_result_col_index_by_name(r, "max(ts)") == -1);
        tsdb_result_free(r);
    }

    printf("PASS test_fedrpc_byname\n");
    return 0;
}
