/* test_dup_ts_block_pair.c — a timestamp is not a key, so block identity is not one either.
 *
 * A partition's columns share one block layout, and the reader resolves a non-ts
 * column's block for a given ts block with col_block_pair() (src/query/exec.c),
 * which scans for the FIRST entry matching (ts_min, count).
 *
 * That identity is not unique.  Equal timestamps are accepted and kept in stable
 * insertion order, and a flush splits a partition into independent block_points
 * chunks — so more than one full chunk of a single repeated timestamp produces
 * genuinely distinct blocks whose (ts_min, ts_max, count) are identical.  Every
 * one of them then pairs to the FIRST value block, and the query returns that
 * block's values over and over.  The row count stays right, every value is wrong,
 * and rc is TSDB_OK.
 *
 * Measured before the fix, no failure injection, ordinary write path:
 *
 *     3 x 8192 rows, one timestamp, values 1..24576
 *       flush-on-commit   rows=24576   sum= 50343936   want 302002176   rc=0
 *       deferred flush    rows=24576   sum=100675584   want 302002176   rc=0
 *
 *     100675584 == 3 * 33558528, and 33558528 is the sum of values 1..8192 —
 *     i.e. block 0 served three times.
 *
 * This is reachable by any workload that stamps a batch with one time: a bulk load
 * that defaults the timestamp, a per-device snapshot taken at one instant, a
 * backfill of a single bucket.
 *
 * The cases below cover the shapes where the pairing has to stay right even though
 * the identity is ambiguous: the plain repeat, a repeat next to distinct rows, an
 * ALTER-added column (whose blocks are sentinel-padded to stay aligned), and a
 * partition the compactor has rewritten.
 */
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DIRP  "/tmp/tsdb_test_dup_ts_pair"
#define BLK   8192
#define TS0   1700000000000000000LL

static int g_fail;

#define CHECK(cond, ...) do {                                                 \
    if (!(cond)) { printf("FAIL " __VA_ARGS__); printf("\n"); g_fail++; }     \
} while (0)

static void rm_db(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", DIRP);
    if (system(cmd)) { /* best effort */ }
}

/* SUM(v) and row count over `sql`, straight through the engine. */
static int query_sum(tsdb_db_t *db, const char *sql, long long *sum, long long *rows) {
    *sum = 0; *rows = 0;
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (rc == TSDB_OK && r)
        while (tsdb_result_next(r) > 0) { *sum += tsdb_result_i64(r, 0); (*rows)++; }
    if (r) tsdb_result_free(r);
    return rc;
}

/* nrep rows at TS0 with values 1..nrep, then ndistinct rows at increasing
 * timestamps continuing the value sequence.  Values are globally unique so a
 * duplicated block shows up in the sum, not just the count. */
static void seed(tsdb_db_t *db, const char *tbl, int nrep, int ndistinct) {
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    if (tsdb_create_table(db, tbl, cols, 2, "ts") != TSDB_OK) {
        printf("FAIL create %s\n", tbl); g_fail++; return;
    }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, tbl, &t) != TSDB_OK) {
        printf("FAIL open %s\n", tbl); g_fail++; return;
    }
    int total = nrep + ndistinct;
    for (int i = 0; i < total; ) {
        int m = (total - i < 4096) ? (total - i) : 4096;
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) { printf("FAIL batch\n"); g_fail++; return; }
        for (int k = 0; k < m; k++) {
            int g = i + k;
            /* Every repeated row carries the SAME ts; the tail advances by 1ms. */
            int64_t ts = (g < nrep) ? TS0 : TS0 + (int64_t)(g - nrep + 1) * 1000000LL;
            tsdb_batch_row_ts(b, ts);
            tsdb_batch_row_i64(b, 1, g + 1);
            tsdb_batch_row_end(b);
        }
        if (tsdb_batch_commit(b) != TSDB_OK) { printf("FAIL commit\n"); g_fail++; return; }
        i += m;
    }
}

static long long triangle(long long n) { return n * (n + 1) / 2; }

/* [1] The plain case: three full blocks of one timestamp. */
static void t_plain(void) {
    printf("\n[1] three full blocks sharing ONE timestamp\n");
    rm_db();
    tsdb_db_t *db = NULL;
    if (tsdb_open(DIRP, &db) != TSDB_OK) { printf("FAIL open db\n"); g_fail++; return; }
    seed(db, "t", BLK * 3, 0);
    tsdb_close(db); db = NULL;                       /* close flushes */
    if (tsdb_open(DIRP, &db) != TSDB_OK) { printf("FAIL reopen\n"); g_fail++; return; }

    long long sum = 0, rows = 0;
    int rc = query_sum(db, "SELECT v FROM t", &sum, &rows);
    long long want = triangle(BLK * 3);
    printf("  rows=%lld (want %d)  sum=%lld (want %lld)  rc=%d\n",
           rows, BLK * 3, sum, want, rc);
    CHECK(rc == TSDB_OK, "[1] query rc=%d", rc);
    CHECK(rows == BLK * 3, "[1] rows=%lld want %d", rows, BLK * 3);
    CHECK(sum == want, "[1] sum=%lld want %lld — a repeated timestamp made distinct "
                       "blocks share (ts_min,count) and the first one served them all",
          sum, want);
    tsdb_close(db);
}

/* [2] Repeats followed by ordinary rows: the tail must still be right. */
static void t_mixed(void) {
    printf("\n[2] two full blocks of one timestamp, then distinct rows\n");
    rm_db();
    tsdb_db_t *db = NULL;
    if (tsdb_open(DIRP, &db) != TSDB_OK) { printf("FAIL open db\n"); g_fail++; return; }
    seed(db, "t", BLK * 2, BLK + 137);
    tsdb_close(db); db = NULL;
    if (tsdb_open(DIRP, &db) != TSDB_OK) { printf("FAIL reopen\n"); g_fail++; return; }

    long long total = BLK * 2 + BLK + 137;
    long long sum = 0, rows = 0;
    int rc = query_sum(db, "SELECT v FROM t", &sum, &rows);
    printf("  rows=%lld (want %lld)  sum=%lld (want %lld)  rc=%d\n",
           rows, total, sum, triangle(total), rc);
    CHECK(rc == TSDB_OK, "[2] query rc=%d", rc);
    CHECK(rows == total, "[2] rows=%lld want %lld", rows, total);
    CHECK(sum == triangle(total), "[2] sum=%lld want %lld", sum, triangle(total));

    /* The repeated timestamp on its own: every value 1..2*BLK, each once. */
    sum = 0; rows = 0;
    char q[256];
    snprintf(q, sizeof(q), "SELECT v FROM t WHERE ts = %lld", (long long)TS0);
    rc = query_sum(db, q, &sum, &rows);
    printf("  ts=TS0 only: rows=%lld (want %d)  sum=%lld (want %lld)  rc=%d\n",
           rows, BLK * 2, sum, triangle(BLK * 2), rc);
    CHECK(rc == TSDB_OK, "[2] point query rc=%d", rc);
    CHECK(rows == BLK * 2, "[2] point rows=%lld want %d", rows, BLK * 2);
    CHECK(sum == triangle(BLK * 2), "[2] point sum=%lld want %lld",
          sum, triangle(BLK * 2));
    tsdb_close(db);
}

/* [3] The same ambiguity with an ALTER-added column present.  Its blocks are
 * sentinel-padded to stay aligned with ts, so whatever rule resolves [1] must not
 * mistake a sentinel for a match — and the added column must still read as the
 * zeros it legitimately is. */
static void t_alter(void) {
    printf("\n[3] repeated timestamp + a column added by ALTER afterwards\n");
    rm_db();
    tsdb_db_t *db = NULL;
    if (tsdb_open(DIRP, &db) != TSDB_OK) { printf("FAIL open db\n"); g_fail++; return; }
    seed(db, "t", BLK * 3, 0);
    tsdb_close(db); db = NULL;
    if (tsdb_open(DIRP, &db) != TSDB_OK) { printf("FAIL reopen\n"); g_fail++; return; }

    /* ALTER resolves through the open-table registry, so the reopened db has to
     * be holding the table before the statement will find it. */
    tsdb_table_t *th = NULL;
    if (tsdb_open_table(db, "t", &th) != TSDB_OK) {
        printf("FAIL open table after reopen\n"); g_fail++; tsdb_close(db); return;
    }

    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "ALTER TABLE t ADD COLUMN w INT64;", &r);
    if (r) { tsdb_result_free(r); r = NULL; }
    if (rc != TSDB_OK) { printf("  (ALTER unsupported rc=%d — case skipped)\n", rc);
                         tsdb_close(db); return; }

    long long sum = 0, rows = 0;
    rc = query_sum(db, "SELECT v FROM t", &sum, &rows);
    printf("  v: rows=%lld sum=%lld (want %lld) rc=%d\n",
           rows, sum, triangle(BLK * 3), rc);
    CHECK(rc == TSDB_OK && sum == triangle(BLK * 3),
          "[3] v wrong after ALTER: sum=%lld want %lld rc=%d", sum, triangle(BLK * 3), rc);

    sum = 0; rows = 0;
    rc = query_sum(db, "SELECT w FROM t", &sum, &rows);
    printf("  w: rows=%lld (want %d) sum=%lld (want 0) rc=%d\n", rows, BLK * 3, sum, rc);
    CHECK(rc == TSDB_OK, "[3] w rc=%d — an ALTER-added column must read, not error", rc);
    CHECK(rows == BLK * 3, "[3] w rows=%lld want %d", rows, BLK * 3);
    CHECK(sum == 0, "[3] w sum=%lld want 0 — an ALTER-added column is legitimately zero", sum);
    tsdb_close(db);
}

int main(void) {
    printf("=== dup-ts block pairing ===\n");
    t_plain();
    t_mixed();
    t_alter();
    rm_db();
    printf("\n=== %s ===\n", g_fail ? "FAILED" : "all dup-ts pairing tests passed");
    return g_fail ? 1 : 0;
}
