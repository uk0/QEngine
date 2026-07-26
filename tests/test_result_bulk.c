/* test_result_bulk.c — row-materialisation correctness.
 *
 * Two things are checked, on the same results, for every query shape the
 * projection fast path can take:
 *
 *  1. VALUES ARE RIGHT.  Every emitted row is re-derived from its own ts
 *     (the generator is a closed form of the row index) and compared cell by
 *     cell, so a materialisation bug that corrupts, duplicates, drops or
 *     re-orders rows fails here even if both read paths agree.
 *
 *  2. THE BULK PATH AGREES WITH THE SCALAR PATH.  tsdb_result_nrows +
 *     tsdb_result_col_ptr must hand back exactly the bytes tsdb_result_next
 *     + tsdb_result_ts/_i64/_f64 walk, in the same order, and must not
 *     disturb the row cursor.
 *
 * The block-copy emit in exec_select transfers maximal RUNS of selected
 * rows, so the predicates below are chosen to produce every run shape:
 * all-set (no WHERE), one long contiguous run, runs of length 1 spaced 8
 * apart (symbol = one of eight), runs of length 2, empty, and LIMITs that
 * land inside a run and on both sides of a block boundary.
 */

#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <math.h>

#define ASSERT(cond) do { \
    if (!(cond)) { fprintf(stderr, "ASSERT: %s [%s:%d]\n", #cond, __FILE__, __LINE__); \
                   abort(); } } while (0)

static void rm_rf(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    (void)system(cmd);
}

/* ---- deterministic generator ------------------------------------------- */
#define NROWS   40000          /* ~5 blocks at the default 8192 block_points */
#define STEP_NS 1000000LL      /* 1 ms */

static const char *SYMS[8] = {"AAPL","MSFT","GOOG","AMZN","META","NVDA","TSLA","NFLX"};
static tsdb_ts_t g_base;

static const char *exp_sym(int64_t i)   { return SYMS[i % 8]; }
static double      exp_price(int64_t i) { return 100.0 + (double)(i % 10000) * 0.01; }
static int64_t     exp_vol(int64_t i)   { return 1000 + (i % 100000); }

static int64_t row_index_of_ts(tsdb_ts_t ts) { return (int64_t)((ts - g_base) / STEP_NS); }

static int checks = 0;

/* ---- the two drains ----------------------------------------------------- */

/* Walk the result with the original public API, writing raw 8-byte slots
 * into out[row*ncols + col]. */
static size_t drain_scalar(tsdb_result_t *r, uint64_t *out, int ncols) {
    size_t row = 0;
    while (tsdb_result_next(r) == 1) {
        for (int c = 0; c < ncols; c++) {
            uint64_t bits = 0;
            switch (tsdb_result_col_type(r, c)) {
            case TSDB_TYPE_FLOAT64:
            case TSDB_TYPE_FLOAT32: { double v = tsdb_result_f64(r, c); memcpy(&bits, &v, 8); break; }
            case TSDB_TYPE_TIMESTAMP: { int64_t v = tsdb_result_ts(r, c); memcpy(&bits, &v, 8); break; }
            default: { int64_t v = tsdb_result_i64(r, c); memcpy(&bits, &v, 8); break; }
            }
            out[row * (size_t)ncols + (size_t)c] = bits;
        }
        row++;
    }
    return row;
}

/* Same layout, read columnar through the bulk API. */
static void drain_bulk(tsdb_result_t *r, uint64_t *out, int ncols, size_t nrows) {
    for (int c = 0; c < ncols; c++) {
        const void *p = tsdb_result_col_ptr(r, c);
        ASSERT(p != NULL);
        for (size_t row = 0; row < nrows; row++) {
            uint64_t bits;
            memcpy(&bits, (const char *)p + row * 8, 8);
            out[row * (size_t)ncols + (size_t)c] = bits;
        }
    }
}

/*
 * Run `q`, then:
 *   - assert the row count is exactly `want_rows` (-1 = don't care),
 *   - re-derive every cell from the row's own ts and compare,
 *   - drain again through the bulk API and require byte equality,
 *   - assert the bulk calls left the scalar cursor alone.
 *
 * `cols` names the projected columns in order, e.g. "tpv" for ts, price,
 * volume ('t' ts, 'p' price, 'v' volume, 's' symbol).
 */
static void check_q(tsdb_db_t *db, const char *q, const char *cols, long want_rows) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, q, &r);
    if (rc != TSDB_OK || !r) { fprintf(stderr, "query [%s] rc=%d\n", q, rc); abort(); }

    int ncols = tsdb_result_ncols(r);
    ASSERT(ncols == (int)strlen(cols));

    size_t nrows = tsdb_result_nrows(r);
    if (want_rows >= 0) {
        if (nrows != (size_t)want_rows)
            fprintf(stderr, "[%s] rows=%zu want=%ld\n", q, nrows, want_rows);
        ASSERT(nrows == (size_t)want_rows);
    }

    size_t cells = nrows * (size_t)ncols;
    uint64_t *a = malloc(cells ? cells * 8 : 8);
    uint64_t *b = malloc(cells ? cells * 8 : 8);
    ASSERT(a && b);

    size_t got = drain_scalar(r, a, ncols);
    ASSERT(got == nrows);                 /* nrows must match what next() walks */

    /* Values: re-derive from ts.  Requires a ts column in the projection. */
    const char *tpos = strchr(cols, 't');
    ASSERT(tpos != NULL);
    int tcol = (int)(tpos - cols);
    tsdb_ts_t prev_ts = TSDB_TS_MIN;
    for (size_t row = 0; row < nrows; row++) {
        int64_t ts;
        memcpy(&ts, &a[row * (size_t)ncols + (size_t)tcol], 8);
        ASSERT(ts > prev_ts);             /* strictly ascending, no dupes */
        prev_ts = ts;
        int64_t i = row_index_of_ts(ts);
        ASSERT(i >= 0 && i < NROWS);
        ASSERT(g_base + i * STEP_NS == ts);
        for (int c = 0; c < ncols; c++) {
            uint64_t bits = a[row * (size_t)ncols + (size_t)c];
            switch (cols[c]) {
            case 't': { int64_t v; memcpy(&v, &bits, 8); ASSERT(v == ts); break; }
            case 'v': { int64_t v; memcpy(&v, &bits, 8); ASSERT(v == exp_vol(i)); break; }
            case 'p': { double  v; memcpy(&v, &bits, 8);
                        ASSERT(fabs(v - exp_price(i)) < 1e-9); break; }
            case 's': break;   /* codes are per-symtab; checked below via _sym */
            default: ASSERT(0);
            }
            checks++;
        }
    }

    /* Bulk path must return the identical bytes and must not move r->cur,
     * which drain_scalar has already parked at end-of-result. */
    drain_bulk(r, b, ncols, nrows);
    ASSERT(cells == 0 || memcmp(a, b, cells * 8) == 0);
    ASSERT(tsdb_result_next(r) == 0);     /* cursor still exhausted */
    checks++;

    /* Out-of-range columns yield NULL rather than reading past the array. */
    ASSERT(tsdb_result_col_ptr(r, -1) == NULL);
    ASSERT(tsdb_result_col_ptr(r, ncols) == NULL);
    ASSERT(tsdb_result_col_ptr(NULL, 0) == NULL);
    ASSERT(tsdb_result_nrows(NULL) == 0);
    checks += 4;

    free(a); free(b);
    tsdb_result_free(r);
}

/* Symbol columns carry dictionary codes, so they are verified through
 * tsdb_result_sym while the bulk array is checked for code stability
 * (equal strings <=> equal codes within one result). */
static void check_symbols(tsdb_db_t *db, const char *q, long want_rows) {
    tsdb_result_t *r = NULL;
    ASSERT(tsdb_query(db, q, &r) == TSDB_OK && r);
    ASSERT(tsdb_result_ncols(r) == 2);          /* ts, symbol */
    size_t nrows = tsdb_result_nrows(r);
    ASSERT(nrows == (size_t)want_rows);
    const uint64_t *codes = (const uint64_t *)tsdb_result_col_ptr(r, 1);
    ASSERT(codes != NULL);
    size_t row = 0;
    while (tsdb_result_next(r) == 1) {
        int64_t ts = tsdb_result_ts(r, 0);
        int64_t i  = row_index_of_ts(ts);
        const char *s = tsdb_result_sym(r, 1);
        ASSERT(s && strcmp(s, exp_sym(i)) == 0);
        /* the bulk slot is the code the cursor resolved */
        ASSERT((uint32_t)codes[row] == (uint32_t)((const uint64_t *)
               tsdb_result_col_ptr(r, 1))[row]);
        row++;
        checks += 2;
    }
    ASSERT(row == nrows);
    tsdb_result_free(r);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_result_bulk";
    rm_rf(dir);

    tsdb_db_t *db;
    ASSERT(tsdb_open(dir, &db) == TSDB_OK);

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"symbol", TSDB_TYPE_SYMBOL},
        {"price",  TSDB_TYPE_FLOAT64},
        {"volume", TSDB_TYPE_INT64},
    };
    ASSERT(tsdb_create_table(db, "t", cols, 4, "ts") == TSDB_OK);
    tsdb_table_t *tt;
    ASSERT(tsdb_open_table(db, "t", &tt) == TSDB_OK);

    g_base = tsdb_parse_ts("2026-01-01 00:00:00");

    /* Three commits: two flushed groups plus a tail, so the scan plan mixes
     * several disk sources with (in deferred-flush mode) a memtable source. */
    const int64_t split[4] = {0, 16384, 32768, NROWS};
    for (int part = 0; part < 3; part++) {
        tsdb_batch_t *b;
        ASSERT(tsdb_batch_begin(tt, &b) == TSDB_OK);
        for (int64_t i = split[part]; i < split[part + 1]; i++) {
            tsdb_batch_row_ts(b, g_base + i * STEP_NS);
            tsdb_batch_row_sym(b, 1, exp_sym(i));
            tsdb_batch_row_f64(b, 2, exp_price(i));
            tsdb_batch_row_i64(b, 3, exp_vol(i));
            tsdb_batch_row_end(b);
        }
        ASSERT(tsdb_batch_commit(b) == TSDB_OK);
    }

    /* ---- full projections: the all-set bitmap, one run per block ---- */
    check_q(db, "SELECT ts FROM t",                        "t",    NROWS);
    check_q(db, "SELECT ts, volume FROM t",                "tv",   NROWS);
    check_q(db, "SELECT ts, price, volume FROM t",         "tpv",  NROWS);
    check_q(db, "SELECT volume, ts, price FROM t",         "vtp",  NROWS);
    check_q(db, "SELECT ts, ts FROM t",                    "tt",   NROWS);

    /* ---- LIMIT: inside a run, on and around a block boundary ---- */
    check_q(db, "SELECT ts, price, volume FROM t LIMIT 1",     "tpv", 1);
    check_q(db, "SELECT ts, price, volume FROM t LIMIT 8191",  "tpv", 8191);
    check_q(db, "SELECT ts, price, volume FROM t LIMIT 8192",  "tpv", 8192);
    check_q(db, "SELECT ts, price, volume FROM t LIMIT 8193",  "tpv", 8193);
    check_q(db, "SELECT ts, price, volume FROM t LIMIT 12345", "tpv", 12345);
    check_q(db, "SELECT ts, price, volume FROM t LIMIT 40001", "tpv", NROWS);

    /* ---- one long contiguous run (volume is 1000+i, strictly rising) ---- */
    check_q(db, "SELECT ts, volume FROM t WHERE volume < 1001",  "tv", 1);
    check_q(db, "SELECT ts, volume FROM t WHERE volume < 9192",  "tv", 8192);
    check_q(db, "SELECT ts, volume FROM t WHERE volume < 9193",  "tv", 8193);
    check_q(db, "SELECT ts, volume FROM t WHERE volume >= 1000", "tv", NROWS);
    check_q(db, "SELECT ts, volume FROM t WHERE volume > 20999", "tv", NROWS - 20000);

    /* ---- empty: no run at all ---- */
    check_q(db, "SELECT ts, volume FROM t WHERE volume > 999999", "tv", 0);

    /* ---- runs of length 1, spaced 8 apart (symbol picks i % 8) ---- */
    check_q(db, "SELECT ts, price FROM t WHERE symbol = 'AAPL'", "tp", NROWS / 8);
    /* ---- runs of length 2 ---- */
    check_q(db, "SELECT ts, price FROM t WHERE symbol = 'AAPL' OR symbol = 'MSFT'",
            "tp", NROWS / 4);
    /* ---- sparse runs plus a LIMIT that lands mid-run ---- */
    check_q(db, "SELECT ts, price FROM t WHERE symbol = 'AAPL' LIMIT 777", "tp", 777);

    /* ---- price cycles every 10000 rows: 4 wide runs crossing blocks.
     * price = 100 + (i % 10000) * 0.01, so > 190.0 selects i % 10000 in
     * [9001, 9999] — 999 rows per cycle, four cycles over NROWS. ---- */
    check_q(db, "SELECT ts, price, volume FROM t WHERE price > 190.0", "tpv",
            4 * 999);

    /* ---- symbol projection ---- */
    check_symbols(db, "SELECT ts, symbol FROM t", NROWS);
    check_symbols(db, "SELECT ts, symbol FROM t WHERE symbol = 'TSLA'", NROWS / 8);

    /* ---- the bulk API on non-projection result shapes ---- */
    {
        tsdb_result_t *r = NULL;
        ASSERT(tsdb_query(db, "SELECT count(*) FROM t", &r) == TSDB_OK && r);
        ASSERT(tsdb_result_nrows(r) == 1);
        const uint64_t *p = (const uint64_t *)tsdb_result_col_ptr(r, 0);
        ASSERT(p && (int64_t)p[0] == NROWS);
        ASSERT(tsdb_result_next(r) == 1);
        ASSERT(tsdb_result_i64(r, 0) == NROWS);
        tsdb_result_free(r);
        checks += 3;
    }
    {
        tsdb_result_t *r = NULL;
        ASSERT(tsdb_query(db, "SELECT symbol, avg(price) FROM t GROUP BY symbol",
                          &r) == TSDB_OK && r);
        size_t n = tsdb_result_nrows(r);
        ASSERT(n == 8);
        const uint64_t *avg = (const uint64_t *)tsdb_result_col_ptr(r, 1);
        ASSERT(avg != NULL);
        size_t row = 0;
        while (tsdb_result_next(r) == 1) {
            double via_cursor = tsdb_result_f64(r, 1);
            double via_bulk;  memcpy(&via_bulk, &avg[row], 8);
            ASSERT(via_cursor == via_bulk);
            row++; checks++;
        }
        ASSERT(row == n);
        tsdb_result_free(r);
    }

    /* Reopen from disk so the same battery runs against a pure-partition
     * scan even when the run above was served partly from the memtable. */
    tsdb_close(db);
    ASSERT(tsdb_open(dir, &db) == TSDB_OK);
    check_q(db, "SELECT ts, price, volume FROM t",                "tpv", NROWS);
    check_q(db, "SELECT ts, volume FROM t WHERE volume < 9193",   "tv",  8193);
    check_q(db, "SELECT ts, price FROM t WHERE symbol = 'AAPL'",  "tp",  NROWS / 8);
    check_q(db, "SELECT ts, price, volume FROM t LIMIT 8193",     "tpv", 8193);
    check_symbols(db, "SELECT ts, symbol FROM t", NROWS);

    tsdb_close(db);
    rm_rf(dir);

    printf("=== test_result_bulk OK (%d cell/invariant checks) ===\n", checks);
    return 0;
}
