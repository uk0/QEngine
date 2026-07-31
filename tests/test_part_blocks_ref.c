/* test_part_blocks_ref.c — borrowed block metadata (tsdb_part_col_blocks_ref).
 *
 * tsdb_part_col_blocks() mallocs and memcpy's the WHOLE per-column block-meta
 * array on every call.  The scan path calls it once per (block, column), so a
 * partition of nb blocks pays O(nb^2) bytes of copying to read nb blocks; the
 * stats fast path pays it while reading no data at all.
 * tsdb_part_col_blocks_ref() hands back the partition's own array instead.
 *
 * Two things have to hold, and this test pins both:
 *
 *  A. The borrowed view is the SAME DATA as the copy — same count, byte-for-
 *     byte identical entries, same (NULL, 0) for an empty column, same
 *     TSDB_ERR_INVAL for the same bad arguments — and it is genuinely BORROWED
 *     (stable address across calls, not the copy's address).  A divergence
 *     here is silent: the converted call sites pair a block by ORDINAL, so a
 *     short-by-one count turns the partition's last block into
 *     TSDB_ERR_CORRUPT and a shifted array serves another block's values.
 *
 *  B. The three converted exec.c call sites still answer correctly.  The
 *     fixture is 20000 rows sharing ONE timestamp, which is the shape that
 *     makes pairing load-bearing: blocks 0 and 1 have byte-identical
 *     (ts_min, ts_max, count) keys, so only the ordinal distinguishes them and
 *     a mis-fetch returns the wrong block's values with rc=0 rather than an
 *     error.  Values are v[i] = i, so every expected answer is exact.
 *       - try_stats_fastpath   : a bare aggregate (takes the fast path) and the
 *                                same aggregate behind a WHERE (structurally
 *                                cannot), both against the closed form.
 *       - scan_load_col_block  : GROUP BY (exec_group_by always routes here).
 *       - exec_select row path : SELECT of raw columns behind a WHERE that
 *                                selects only inside the LAST block, and only
 *                                inside the FIRST.
 */

#include "../include/tsdb.h"
#include "../src/storage/schema.h"
#include "../src/storage/part.h"
#include "../src/core/types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/stat.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

#define CHECKF(c, ...) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
                fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); g_fail++; } \
    else      { printf("PASS: "); printf(__VA_ARGS__); printf("\n"); g_pass++; } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

#define NROWS   20000
#define DUP_TS  1700000000000000000LL   /* every row carries this one timestamp */
#define NGROUPS 4

/* Closed-form answers over v[i] = k[i] = i, g[i] = i % 4, i in [0, NROWS). */
#define SUM_ALL   ((double)NROWS * (double)(NROWS - 1) / 2.0)   /* 199990000 */
#define PER_GROUP (NROWS / NGROUPS)                             /* 5000 */

static void write_dataset(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); exit(1); }
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_FLOAT64   },
        { "k",  TSDB_TYPE_INT64     },
        { "g",  TSDB_TYPE_INT64     },
    };
    int rc = tsdb_create_table(db, "dup", cols, 4, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) {
        fprintf(stderr, "create rc=%d\n", rc); exit(1);
    }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "dup", &t) != TSDB_OK) {
        fprintf(stderr, "open_table failed\n"); exit(1);
    }
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    for (int64_t i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, DUP_TS);
        tsdb_batch_row_f64(b, 1, (double)i);
        tsdb_batch_row_i64(b, 2, i);
        tsdb_batch_row_i64(b, 3, i % NGROUPS);
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    tsdb_close(db);
}

static int find_part_dir(const char *table_dir, char *out, size_t cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s", table_dir, e->d_name);
        struct stat st;
        if (stat(p, &st) == 0 && S_ISDIR(st.st_mode)) {
            snprintf(out, cap, "%s", p);
            found = 1;
            break;
        }
    }
    closedir(d);
    return found;
}

/* ---- A: the borrowed view equals the copy ------------------------------- */
static void check_equivalence(tsdb_part_t *p, tsdb_schema_t *s) {
    size_t nb_total = 0;
    for (int c = 0; c < s->ncols; c++) {
        /* ABSOLUTE oracle first.  tsdb_part_col_blocks is implemented in terms
         * of the borrowing variant so the two cannot drift — which means a
         * comparison BETWEEN them cannot see a bug they now share.  The row
         * count is the fact neither of them gets to define: every row written
         * must be described by some block, or the partition silently shrinks
         * and count(*) answers short with rc=0. */
        {
            const tsdb_block_meta_t *m = NULL; size_t n = 0;
            int64_t rows = 0;
            if (tsdb_part_col_blocks_ref(p, c, &m, &n) == TSDB_OK)
                for (size_t b = 0; b < n; b++) rows += m[b].count;
            CHECKF(rows == NROWS, "col %d: blocks describe %lld rows (want %d)",
                   c, (long long)rows, NROWS);
        }

        tsdb_block_meta_t *copy = NULL; size_t ncopy = 0;
        const tsdb_block_meta_t *ref = NULL, *ref2 = NULL;
        size_t nref = 0, nref2 = 0;

        int rc_copy = tsdb_part_col_blocks(p, c, &copy, &ncopy);
        int rc_ref  = tsdb_part_col_blocks_ref(p, c, &ref, &nref);
        int rc_ref2 = tsdb_part_col_blocks_ref(p, c, &ref2, &nref2);

        CHECKF(rc_copy == rc_ref && rc_ref == rc_ref2,
               "col %d: both variants return the same rc (%d)", c, rc_copy);
        CHECKF(ncopy == nref && nref == nref2,
               "col %d: same block count (copy=%zu ref=%zu)", c, ncopy, nref);
        CHECKF(nref > 1, "col %d: fixture really is multi-block (%zu)", c, nref);
        CHECKF(ref == ref2,
               "col %d: ref is a stable borrow, not a fresh allocation", c);
        CHECKF(ref != copy,
               "col %d: ref is not the copy's buffer", c);
        CHECKF(ncopy > 0 && ref != NULL && copy != NULL &&
               memcmp(ref, copy, ncopy * sizeof(tsdb_block_meta_t)) == 0,
               "col %d: %zu block metas byte-identical to the copy", c, ncopy);
        if (c == 0) nb_total = nref;
        else CHECKF(nref == nb_total,
                    "col %d: aligned 1:1 with ts (%zu vs %zu)", c, nref, nb_total);
        free(copy);
    }

    /* Bad-argument parity.  These must NOT diverge: a caller that switches
     * variants would otherwise get a different rc for the same mistake. */
    {
        const tsdb_block_meta_t *ref = (const tsdb_block_meta_t *)0x1;
        tsdb_block_meta_t *copy = (tsdb_block_meta_t *)0x1;
        size_t n = 12345;
        CHECK(tsdb_part_col_blocks_ref(p, -1, &ref, &n) == TSDB_ERR_INVAL &&
              tsdb_part_col_blocks(p, -1, &copy, &n) == TSDB_ERR_INVAL,
              "col_idx < 0 is INVAL in both variants");
        CHECK(tsdb_part_col_blocks_ref(p, s->ncols, &ref, &n) == TSDB_ERR_INVAL &&
              tsdb_part_col_blocks(p, s->ncols, &copy, &n) == TSDB_ERR_INVAL,
              "col_idx >= ncols is INVAL in both variants");
        CHECK(tsdb_part_col_blocks_ref(NULL, 0, &ref, &n) == TSDB_ERR_INVAL &&
              tsdb_part_col_blocks(NULL, 0, &copy, &n) == TSDB_ERR_INVAL,
              "NULL part is INVAL in both variants");
        CHECK(tsdb_part_col_blocks_ref(p, 0, NULL, &n) == TSDB_ERR_INVAL &&
              tsdb_part_col_blocks(p, 0, NULL, &n) == TSDB_ERR_INVAL,
              "NULL out_arr is INVAL in both variants");
        CHECK(tsdb_part_col_blocks_ref(p, 0, &ref, NULL) == TSDB_ERR_INVAL &&
              tsdb_part_col_blocks(p, 0, &copy, NULL) == TSDB_ERR_INVAL,
              "NULL out_n is INVAL in both variants");
        CHECKF(n == 12345, "INVAL leaves the caller's out_n untouched (%zu)", n);
    }
}

/* ---- B: the three converted call sites ---------------------------------- */

/* try_stats_fastpath: a BARE aggregate takes the stats fast path; the same
 * aggregate behind a WHERE structurally cannot (both gates require no
 * predicate) and falls to the scan.  The two must agree, and both must equal
 * the closed form.
 *
 * The comparison is driven by the QUERY, not by TSDB_DISABLE_STATS_FASTPATH:
 * that env var is latched into a process-wide static the first time either
 * gate runs, so flipping it mid-process changes nothing and a test built on it
 * would compare the fast path against itself. */
static void check_stats_fastpath(const char *dir, int force_scan) {
    const char *lbl = force_scan ? "scan" : "stats";
    const char *sql = force_scan
        ? "SELECT count(*), min(v), max(v), sum(v) FROM dup WHERE v >= 0"
        : "SELECT count(*), min(v), max(v), sum(v) FROM dup";

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { CHECK(0, "reopen db"); return; }
    tsdb_result_t *r = NULL;
    if (tsdb_query(db, sql, &r) == TSDB_OK && r && tsdb_result_next(r)) {
        int64_t n   = tsdb_result_i64(r, 0);
        double  mn  = tsdb_result_f64(r, 1);
        double  mx  = tsdb_result_f64(r, 2);
        double  sum = tsdb_result_f64(r, 3);
        CHECKF(n == NROWS, "[%s] count(*) = %lld (want %d)", lbl, (long long)n, NROWS);
        CHECKF(mn == 0.0, "[%s] min(v) = %.1f (want 0)", lbl, mn);
        CHECKF(mx == (double)(NROWS - 1), "[%s] max(v) = %.1f (want %d)",
               lbl, mx, NROWS - 1);
        CHECKF(sum == SUM_ALL, "[%s] sum(v) = %.1f (want %.1f)", lbl, sum, SUM_ALL);
    } else {
        CHECKF(0, "[%s] bare aggregate query returned a row", lbl);
    }
    if (r) tsdb_result_free(r);
    tsdb_close(db);
}

/* scan_load_col_block: exec_group_by always goes through it. */
static void check_group_by(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { CHECK(0, "reopen db (group by)"); return; }
    tsdb_result_t *r = NULL;
    int seen[NGROUPS] = {0};
    int nrows = 0, all_ok = 1;
    if (tsdb_query(db,
            "SELECT g, count(*), min(v), max(v), sum(v) FROM dup GROUP BY g", &r)
            == TSDB_OK && r) {
        while (tsdb_result_next(r)) {
            int64_t g   = tsdb_result_i64(r, 0);
            int64_t n   = tsdb_result_i64(r, 1);
            double  mn  = tsdb_result_f64(r, 2);
            double  mx  = tsdb_result_f64(r, 3);
            double  sum = tsdb_result_f64(r, 4);
            nrows++;
            if (g < 0 || g >= NGROUPS || seen[g]) { all_ok = 0; continue; }
            seen[g] = 1;
            /* g, g+4, ..., g+NROWS-4 */
            double want_sum = (double)PER_GROUP * (double)g
                            + (double)NGROUPS * ((double)(PER_GROUP - 1) * PER_GROUP / 2.0);
            if (n != PER_GROUP || mn != (double)g ||
                mx != (double)(g + NROWS - NGROUPS) || sum != want_sum) {
                all_ok = 0;
                fprintf(stderr, "  group %lld: n=%lld min=%.1f max=%.1f sum=%.1f "
                                "(want n=%d min=%lld max=%lld sum=%.1f)\n",
                        (long long)g, (long long)n, mn, mx, sum,
                        PER_GROUP, (long long)g, (long long)(g + NROWS - NGROUPS),
                        want_sum);
            }
        }
        tsdb_result_free(r);
    } else {
        all_ok = 0;
    }
    CHECKF(nrows == NGROUPS, "GROUP BY g returned %d groups (want %d)", nrows, NGROUPS);
    CHECK(all_ok, "GROUP BY g: every group's count/min/max/sum exact");
    tsdb_close(db);
}

/* exec_select row path: raw columns behind a WHERE that lands in exactly one
 * block.  With every row on one timestamp, blocks 0 and 1 carry identical
 * pairing keys, so serving the wrong block here is silent — hence the exact
 * per-row check. */
static void check_row_path(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { CHECK(0, "reopen db (row path)"); return; }

    struct { const char *sql; int64_t lo, hi; const char *what; } cases[] = {
        { "SELECT ts, v, k FROM dup WHERE v > 19995", 19996, 19999, "last block" },
        { "SELECT ts, v, k FROM dup WHERE v < 3",         0,     2, "first block" },
    };

    for (unsigned ci = 0; ci < sizeof(cases) / sizeof(cases[0]); ci++) {
        tsdb_result_t *r = NULL;
        int64_t want_n = cases[ci].hi - cases[ci].lo + 1;
        int64_t got_n = 0;
        int ok = 1;
        uint64_t mask = 0;
        if (tsdb_query(db, cases[ci].sql, &r) == TSDB_OK && r) {
            while (tsdb_result_next(r)) {
                int64_t ts = tsdb_result_i64(r, 0);
                double  v  = tsdb_result_f64(r, 1);
                int64_t k  = tsdb_result_i64(r, 2);
                got_n++;
                if (ts != DUP_TS) ok = 0;
                if (v != (double)k) ok = 0;
                if (k < cases[ci].lo || k > cases[ci].hi) { ok = 0; continue; }
                mask |= (uint64_t)1 << (k - cases[ci].lo);
            }
            tsdb_result_free(r);
        } else {
            ok = 0;
        }
        CHECKF(got_n == want_n, "row path (%s): %lld rows (want %lld)",
               cases[ci].what, (long long)got_n, (long long)want_n);
        CHECKF(ok && mask == (((uint64_t)1 << want_n) - 1),
               "row path (%s): exactly v=k=%lld..%lld, ts intact",
               cases[ci].what, (long long)cases[ci].lo, (long long)cases[ci].hi);
    }
    tsdb_close(db);
}

int main(void) {
    printf("=== test_part_blocks_ref ===\n");
    const char *dir = "/tmp/tsdb_test_blocks_ref";
    rmrf(dir);

    printf("\n[0] write %d rows, ALL on one timestamp\n", NROWS);
    write_dataset(dir);

    char table_dir[4096], part_dir[4096];
    snprintf(table_dir, sizeof(table_dir), "%s/dup", dir);
    if (!find_part_dir(table_dir, part_dir, sizeof(part_dir))) {
        fprintf(stderr, "FAIL: no partition dir (not flushed?)\n"); return 1;
    }

    printf("\n[1] borrowed view == copied view\n");
    tsdb_schema_t *s = NULL;
    if (tsdb_schema_open(table_dir, &s) != TSDB_OK) {
        fprintf(stderr, "FAIL: schema_open\n"); return 1;
    }
    tsdb_part_t *p = NULL;
    if (tsdb_part_open(s, part_dir, &p) != TSDB_OK) {
        fprintf(stderr, "FAIL: part_open\n"); return 1;
    }
    check_equivalence(p, s);
    tsdb_part_close(p);
    tsdb_schema_free(s);

    printf("\n[2] try_stats_fastpath (bare aggregate) vs the same aggregate behind a WHERE\n");
    check_stats_fastpath(dir, 0);
    check_stats_fastpath(dir, 1);

    printf("\n[3] scan_load_col_block (GROUP BY)\n");
    check_group_by(dir);

    printf("\n[4] exec_select row path\n");
    check_row_path(dir);

    rmrf(dir);
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
