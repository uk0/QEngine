/* test_dup_ts_compaction.c — compaction and the short-column/ALTER
 * reconstruction, over data whose block identity is NOT unique.
 *
 * A partition's columns share one block layout, and the reader resolves a
 * non-ts column's block for a given ts block by FIRST-MATCH on (ts_min,count)
 * — col_block_pair(), src/query/exec.c.  That identity is not a key.  Equal
 * timestamps are accepted and kept in stable insertion order
 * (src/storage/memtable.c), and both writers that produce blocks cut fixed-size
 * chunks without looking at whether the chunk is distinguishable:
 *
 *   flush       block_points chunks           src/storage/part.c
 *   compaction  COMPACT_BLOCK_POINTS chunks   src/storage/compaction.c
 *
 * so a long enough run of ONE timestamp yields several genuinely different
 * blocks carrying identical (ts_min, ts_max, count).  Every one of them then
 * pairs to the FIRST of them, and the query returns that block's values over
 * and over: the row count stays right, every value is wrong, rc is TSDB_OK.
 *
 * test_dup_ts_block_pair.c owns the plain scan shapes.  THIS file owns the two
 * places where the ambiguity is resolved by REWRITING blocks rather than by
 * reading them, because a fix that leans on either of those to disambiguate is
 * a fix that does not hold:
 *
 *   [A] COMPACTION.  compact_column_file() re-cuts a column into fixed
 *       COMPACT_BLOCK_POINTS chunks and records only each chunk's extrema and
 *       count (compaction.c:569).  Two full chunks of one timestamp are just as
 *       indistinguishable after the rewrite as before it — compaction does not
 *       merge the ambiguity away, it re-creates it at a coarser grain.
 *
 *   [B] A PARTIAL ALTER HISTORY THAT IS THEN COMPACTED.  Compaction skips a
 *       column that is below the block threshold on its own (compaction.c:323)
 *       and derives a SHORT column's output range from ts row ZERO
 *       (compaction.c:586) — `blk_ts_min = ts_flat[base]` with `base` counted
 *       from the short column's own first row.  A column added by ALTER halfway
 *       through a partition starts at ts row K, so its rewritten blocks get
 *       stamped with the range of the partition's FIRST rows.  That is not the
 *       range tsdb_part_open's late-add check compares against, so the
 *       reconstruction stops recognising a legitimate late add.
 *
 *   [C] AN INTERIOR LOSS INSIDE A DUPLICATE-KEY RUN.  This is the case
 *       test_short_column_read.c does not have, and the one where a wrong fix
 *       is worst.  When every entry carries the same key, the late-add
 *       hypothesis test in tsdb_part_open (part.c:2700 — "cm[b] equals
 *       ts_m[nmiss+b] on (ts_min,count)") is satisfied by ANY alignment, so a
 *       column that lost one INTERIOR block is accepted as a late add and
 *       front-padded.  Measured on HEAD: the whole column then reads as
 *       fabricated zeros at rc=0.  Nothing in the remaining fields can say which
 *       ordinal went missing, so the only correct answer is to refuse the
 *       affected reads while count(*), the timestamps and every healthy column
 *       keep answering exactly.
 *
 *   [D] A REPEATED-TIMESTAMP RUN ON EITHER SIDE OF A PARTITION BOUNDARY.  One
 *       literal timestamp cannot span two partitions, so the two runs are
 *       distinct populations that must never be confused with each other.  Any
 *       ordinal a fix makes durable has to be PARTITION-LOCAL; a global or
 *       per-table one pairs day 1's first run block against day 0's.
 *
 * Every assertion goes through the engine (count(*), sum(), min/max, a point
 * query, the ts range) — none of it reads header bytes to decide the verdict.
 *
 * ---- STATUS ON HEAD (5d75238), measured, both durability modes identical ----
 *
 *   40 passed, 21 failed.  The failure set is byte-identical under
 *   TSDB_WAL_ONLY_COMMIT=0 and =1.
 *
 *   RED — reproduces a live defect on this commit:
 *     A4 A5 A7          a compacted duplicate-key run serves output block 0
 *                       twice: sum = 2 x triangle(32768) = 1073774592 instead
 *                       of triangle(65536) = 2147516416; max(v) = 32768.
 *     B11..B14          compaction stamps the ALTER-added column from ts row
 *                       zero; the late-add suffix is destroyed and `SELECT w`
 *                       goes from a correct answer to TSDB_ERR_CORRUPT.
 *     B31..B34          the same outcome by the other mechanism — w is below
 *                       the block threshold, so it is left behind while ts and
 *                       v are re-cut to 32768-row blocks around it.
 *     C1 C2 C3          an interior loss in an all-equal-key run is mistaken
 *                       for a late add: 8192 rows of fabricated zeros, rc=0.
 *     C4 C5             the untouched column beside it is wrong too —
 *                       8 x triangle(1024) = 4198400 instead of 33558528.
 *     D1..D5            each partition's own run mis-pairs before compaction.
 *
 *   GREEN — guards, not repros.  They pass on HEAD and exist to keep the fix
 *   from buying correctness with availability or blast radius:
 *     A0 A1 A2 A3 A6    compaction must still RUN on a duplicate-key partition,
 *                       and the read must still ANSWER — nothing is missing
 *                       here, so "ambiguous" must not become "unavailable".
 *     B0 B0c            scope checks (one partition per table; the compactor
 *                       really did rewrite it).
 *     B1..B6, B21..B26  the UNCOMPACTED late-add read, which is correct today.
 *                       Sensitivity is not assumed: the identical assertions
 *                       fire red at B11..B14 / B31..B34 on the broken layout.
 *     C0 C6 C7          the refusal stays one column wide.  Sensitivity checked
 *                       by moving the dropped entry to ts.idx, which takes
 *                       count(*) to 7168 and turns C6 and C7 red.
 *     D0, D6..D8        two partitions, two distinct answers, both rewritten.
 *     D1..D7/compacted  after compaction each day is a single block, so this
 *                       phase cannot mis-pair within a partition; it is here to
 *                       catch a fix whose durable ordinal is global or
 *                       per-table rather than partition-local.
 */
#include "tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

static int g_pass = 0;
static int g_fail = 0;

static void check(int cond, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void check(int cond, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    printf(cond ? "  PASS: " : "  FAIL: ");
    vprintf(fmt, ap);
    printf("\n");
    va_end(ap);
    if (cond) g_pass++; else g_fail++;
}

#define HARD(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
        fprintf(stderr, "harness broke at %s:%d rc=%d (%s)\n",              \
                __FILE__, __LINE__, _r, tsdb_errstr(_r));                   \
        printf("\n=== RESULTS: %d passed, %d failed (HARNESS ERROR) ===\n",  \
               g_pass, g_fail);                                             \
        exit(2); } } while (0)

/* ---- layout ------------------------------------------------------------- */

#define DAY_NS  86400000000000LL
#define D0      ((1700000000000000000LL / DAY_NS) * DAY_NS)   /* day-aligned */
#define D1      (D0 + DAY_NS)
#define STEP    1000000LL                                     /* 1 ms        */
#define BP      1024          /* per-table block_points (clamped low bound)  */

/* [A] two full COMPACT_BLOCK_POINTS chunks of ONE timestamp: after the rewrite
 * the partition still holds two blocks with identical (ts_min,ts_max,count). */
#define A_ROWS  (2 * COMPACT_BLOCK_POINTS)                    /* 65536       */
#define A_TS    (D0 + 3600000000000LL)                        /* one instant */

/* [B] one partition, one ALTER in the middle of its history.  Two tables, one
 * for each way compaction breaks the late-add shape:
 *   b   the added column is ABOVE the block threshold, so it is rewritten and
 *       stamped from ts row zero (compaction.c:586);
 *   b2  the added column is BELOW it, so compaction skips it (compaction.c:323)
 *       while ts and v are re-cut around it. */
#define B_OLD    40960                                        /* before ALTER */
#define B_NEW    24576                                        /* after  ALTER */
#define B_TOTAL  (B_OLD + B_NEW)                              /* 65536, 1 day */
#define B2_OLD   40960
#define B2_NEW   2048        /* 2 blocks of BP — under min_blocks_to_compact 4 */
#define B2_TOTAL (B2_OLD + B2_NEW)

/* [C] one repeated timestamp, one column that loses an INTERIOR block. */
#define C_ROWS  8192
#define C_TS    (D0 + 7200000000000LL)
#define C_DROP  3             /* interior idx entry of 'v' (of 8) to remove  */

/* [D] a repeated run at the end of day 0 and another at the start of day 1. */
#define D_DIST  2048
#define D_RUN   4096
#define D_TS0   (D0 + 3600000000000LL)     /* after day 0's distinct rows    */
#define D_TS1   (D1 + STEP)                /* before day 1's distinct rows   */
#define D_TOTAL (2 * (D_DIST + D_RUN))                        /* 12288       */

static long long triangle(long long n) { return n * (n + 1) / 2; }

/* ---- filesystem helpers ------------------------------------------------- */

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* The compactor skips any partition touched in the last 60 s. */
static void backdate(const char *tbl_dir) {
    DIR *td = opendir(tbl_dir);
    if (!td) return;
    struct dirent *pe;
    while ((pe = readdir(td))) {
        if (pe->d_name[0] == '.') continue;
        char pd[4096]; snprintf(pd, sizeof(pd), "%s/%s", tbl_dir, pe->d_name);
        struct stat st;
        if (stat(pd, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        DIR *dd = opendir(pd);
        if (dd) {
            struct dirent *fe;
            while ((fe = readdir(dd))) {
                if (fe->d_name[0] == '.') continue;
                char fp[8192]; snprintf(fp, sizeof(fp), "%s/%s", pd, fe->d_name);
                struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
                utime(fp, &tb);
            }
            closedir(dd);
        }
        struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
        utime(pd, &tb);
    }
    closedir(td);
}

/* Every table directory under a db root. */
static void backdate_all(const char *db_dir) {
    DIR *d = opendir(db_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char td[4096]; snprintf(td, sizeof(td), "%s/%s", db_dir, e->d_name);
        struct stat st;
        if (stat(td, &st) == 0 && S_ISDIR(st.st_mode)) backdate(td);
    }
    closedir(d);
}

/* nth partition directory of a table, ascending day order. */
#define PART_NAME_MAX 1024
static int nth_part(const char *table_dir, int n, char *out, size_t cap) {
    char names[64][PART_NAME_MAX]; int cnt = 0;
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    while ((e = readdir(d)) && cnt < 64) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(names[cnt++], PART_NAME_MAX, "%s", e->d_name);
    }
    closedir(d);
    for (int i = 0; i < cnt; i++)
        for (int j = i + 1; j < cnt; j++)
            if (strcmp(names[j], names[i]) < 0) {
                char t[PART_NAME_MAX];
                snprintf(t, PART_NAME_MAX, "%s", names[i]);
                snprintf(names[i], PART_NAME_MAX, "%s", names[j]);
                snprintf(names[j], PART_NAME_MAX, "%s", t);
            }
    if (n >= cnt) return 0;
    snprintf(out, cap, "%s/%s", table_dir, names[n]);
    return 1;
}

/* ---- little-endian .idx helpers (mirror part.c's on-disk layout) --------- */
static uint32_t g32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t g16(const uint8_t *p) { return (uint16_t)(p[0] | (p[1] << 8)); }
static void     p32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}

/* Blocks declared by <part>/<col>.idx. */
static long idx_count(const char *part, const char *col) {
    char path[4200];
    snprintf(path, sizeof path, "%s/%s.idx", part, col);
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    uint8_t h[12];
    long n = (fread(h, 1, sizeof h, f) == sizeof h) ? (long)g32(h + 4) : -1;
    fclose(f);
    return n;
}

/*
 * Delete entry `drop` from <part>/<col>.idx and decrement the header count.
 * The .col keeps every byte; only the manifest forgets a block — byte for byte
 * what a per-column raw-block apply that skipped one (column, block) leaves, or
 * a crash that lost the .idx rename and resurrected the previous generation.
 * Same construction as tests/test_short_column_read.c, which is where this
 * shape is documented; the difference here is that the surrounding entries all
 * carry the SAME key, so nothing in them says which ordinal is gone.
 */
static int idx_drop_entry(const char *part, const char *col, int drop) {
    char path[4200];
    snprintf(path, sizeof path, "%s/%s.idx", part, col);
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "no such idx: %s\n", path); return -1; }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 0) { fclose(f); return -1; }
    uint8_t *buf = malloc((size_t)sz);
    if (!buf) { fclose(f); return -1; }
    if (fread(buf, 1, (size_t)sz, f) != (size_t)sz) { fclose(f); free(buf); return -1; }
    fclose(f);

    uint32_t count = g32(buf + 4);
    uint16_t ver   = g16(buf + 8);
    size_t   hdr   = (ver == 1) ? 20 : (ver == 2) ? 36 : (ver == 3) ? 40 : 48;
    uint32_t esz   = (ver >= 3) ? g16(buf + 36) : 40;
    if (esz == 0) esz = 88;
    if (drop < 0 || (uint32_t)drop >= count || count < 2) { free(buf); return -1; }

    size_t nkeep = count - 1;
    uint8_t *nb = malloc(hdr + nkeep * esz);
    if (!nb) { free(buf); return -1; }
    memcpy(nb, buf, hdr);
    p32(nb + 4, (uint32_t)nkeep);
    size_t o = hdr;
    for (uint32_t i = 0; i < count; i++) {
        if ((int)i == drop) continue;
        memcpy(nb + o, buf + hdr + (size_t)i * esz, esz);
        o += esz;
    }
    FILE *w = fopen(path, "wb");
    if (!w) { free(nb); free(buf); return -1; }
    size_t wn = fwrite(nb, 1, o, w);
    fclose(w);
    free(nb); free(buf);
    if (wn != o) return -1;
    printf("  built interior loss: %s dropped entry %d (%u -> %zu blocks, "
           "ts still declares %u)\n", path, drop, count, nkeep, count);
    return 0;
}

/* ---- engine helpers ----------------------------------------------------- */

typedef struct {
    int       rc;
    long long rows;
    long long sum;      /* of column 0 — see the fold below */
    long long lo, hi;   /* min/max of column 0 over the returned rows */
    char      err[512];
} qres_t;

/* Run `sql`, fold column 0 as int64.  Never asserts — the caller decides. */
static qres_t q(tsdb_db_t *db, const char *sql) {
    qres_t o;
    memset(&o, 0, sizeof o);
    o.lo = INT64_MAX; o.hi = INT64_MIN;
    tsdb_result_t *r = NULL;
    o.rc = tsdb_query(db, sql, &r);
    if (o.rc == TSDB_OK && r) {
        /* Accumulate UNSIGNED.  Some of these projections are TIMESTAMP
         * columns whose nanosecond values are ~1.7e18, so a few thousand rows
         * overflow int64 — signed overflow is UB and UBSan (correctly) traps
         * it.  Unsigned wraparound is defined, and every assertion below either
         * compares the fold against an identically-computed expectation or
         * ignores it, so the wrap is harmless. */
        uint64_t acc = 0;
        while (tsdb_result_next(r) > 0) {
            long long v = (long long)tsdb_result_i64(r, 0);
            acc += (uint64_t)v;
            if (v < o.lo) o.lo = v;
            if (v > o.hi) o.hi = v;
            o.rows++;
        }
        o.sum = (long long)acc;
    }
    if (r) tsdb_result_free(r);
    const char *e = tsdb_last_error();
    snprintf(o.err, sizeof o.err, "%s", e ? e : "");
    if (o.rows == 0) { o.lo = 0; o.hi = 0; }
    return o;
}

static long long q_count(tsdb_db_t *db, const char *table) {
    char sql[128];
    snprintf(sql, sizeof sql, "SELECT count(*) FROM %s", table);
    qres_t o = q(db, sql);
    return (o.rc == TSDB_OK && o.rows == 1) ? o.sum : -1;
}

/* One manual compaction pass over every table of `db`.  Returns parts_merged,
 * or -1 if the compactor itself failed.  worker_threads < 0 means "no
 * background thread": the pass this call reports is the only one that ran, so
 * the printed counts are the counts. */
static long long compact_once(tsdb_db_t *db, const char *db_dir) {
    backdate_all(db_dir);
    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof opts);
    opts.min_blocks_to_compact = 4;
    opts.interval_ns           = 5000000000LL;
    opts.worker_threads        = -1;      /* manual — we drive run_once */
    tsdb_compactor_t *c = NULL;
    if (tsdb_compactor_start(db, &opts, &c) != TSDB_OK) return -1;
    int rc = tsdb_compactor_run_once(c);
    tsdb_compactor_stats_t st;
    memset(&st, 0, sizeof st);
    tsdb_compactor_stats(c, &st);
    tsdb_compactor_stop(c);
    if (rc != TSDB_OK) return -1;
    printf("  compactor: %llu column file(s) rewritten across %llu partition(s)\n",
           (unsigned long long)st.compactions_done,
           (unsigned long long)st.parts_merged);
    return (long long)st.parts_merged;
}

/* ==========================================================================
 * [A] compaction re-creates the ambiguity at a coarser grain
 *
 * 65536 rows, ONE timestamp, values 1..65536.  block_points is 1024, so before
 * compaction the partition holds 64 blocks that all read (A_TS, A_TS, 1024).
 * compact_column_file() re-cuts to COMPACT_BLOCK_POINTS = 32768, giving TWO
 * blocks that all read (A_TS, A_TS, 32768).  Both counts are ambiguous, so a
 * fix must carry the ordinal — it cannot wait for the compactor to make the
 * blocks distinguishable, because the compactor never does.
 *
 * RED ON HEAD both before and after compaction.
 * ======================================================================== */
static void case_a(void) {
    const char *dir = "/tmp/tsdb_test_dup_ts_compaction_a";
    printf("\n[A] a duplicate-key run survives compaction as a duplicate-key run\n");
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    HARD(tsdb_create_table_ex2(db, "a", cols, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "a", &t));

    for (int i = 0; i < A_ROWS; ) {
        int m = (A_ROWS - i < 4096) ? (A_ROWS - i) : 4096;
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int k = 0; k < m; k++) {
            HARD(tsdb_batch_row_ts(b, (tsdb_ts_t)A_TS));
            HARD(tsdb_batch_row_i64(b, 1, i + k + 1));
            HARD(tsdb_batch_row_end(b));
        }
        HARD(tsdb_batch_commit(b));
        i += m;
    }
    HARD(tsdb_db_flush_all(db));

    const long long want_sum = triangle(A_ROWS);

    /* The PRE-compaction value contract belongs to test_dup_ts_block_pair.c and
     * is not re-asserted here; it is printed so that A4's number can be read
     * against it — 64 x triangle(1024) before, 2 x triangle(32768) after, i.e.
     * the same defect at two different grains. */
    qres_t before = q(db, "SELECT v FROM a");
    printf("  before compaction: rc=%d rows=%lld sum=%lld (want %lld) "
           "min=%lld max=%lld (want 1 / %d)\n",
           before.rc, before.rows, before.sum, want_sum,
           before.lo, before.hi, A_ROWS);
    check(before.rc == TSDB_OK && before.rows == A_ROWS,
          "A1 the compactor's input is a readable %d-row partition "
          "(rc=%d rows=%lld)", A_ROWS, before.rc, before.rows);

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof tbl_dir, "%s/a", dir);
    char part[4096];
    long ts_blocks_before = 0, v_blocks_before = 0;
    if (nth_part(tbl_dir, 0, part, sizeof part)) {
        ts_blocks_before = idx_count(part, "ts");
        v_blocks_before  = idx_count(part, "v");
    }

    long long merged = compact_once(db, dir);
    long ts_blocks_after = 0, v_blocks_after = 0;
    if (nth_part(tbl_dir, 0, part, sizeof part)) {
        ts_blocks_after = idx_count(part, "ts");
        v_blocks_after  = idx_count(part, "v");
    }
    printf("  blocks: ts %ld -> %ld, v %ld -> %ld  (COMPACT_BLOCK_POINTS=%d)\n",
           ts_blocks_before, ts_blocks_after, v_blocks_before, v_blocks_after,
           COMPACT_BLOCK_POINTS);
    /* Scope, not the defect: if the compactor declined, A2..A7 would measure
     * the pre-compaction layout twice and prove nothing about compaction. */
    check(merged > 0 && ts_blocks_after > 0 && ts_blocks_after < ts_blocks_before,
          "A0 the compactor still compacts a duplicate-key partition — declining "
          "to compact it is not an acceptable fix (parts_merged=%lld, "
          "ts blocks %ld -> %ld)", merged, ts_blocks_before, ts_blocks_after);
    check(ts_blocks_after >= 2,
          "A0 the rewrite still leaves >= 2 indistinguishable blocks "
          "(%ld blocks of %d rows at one timestamp) — compaction does not "
          "disambiguate", ts_blocks_after, COMPACT_BLOCK_POINTS);

    qres_t after = q(db, "SELECT v FROM a");
    printf("  after  compaction: rc=%d rows=%lld sum=%lld (want %lld) "
           "min=%lld max=%lld (want 1 / %d)\n",
           after.rc, after.rows, after.sum, want_sum, after.lo, after.hi, A_ROWS);
    /* Nothing is lost here — every block is durable and every value present.
     * Refusing the read would be a fresh availability bug, so rc must be OK. */
    check(after.rc == TSDB_OK,
          "A2 post-compaction the scan still ANSWERS — no value is missing, so "
          "'ambiguous' must not become 'unavailable' (rc=%d%s%s)",
          after.rc, after.rc == TSDB_OK ? "" : " err=", after.err);
    check(after.rows == A_ROWS, "A3 post-compaction row count is %d (got %lld)",
          A_ROWS, after.rows);
    check(after.sum == want_sum,
          "A4 post-compaction every row keeps its own value "
          "(sum=%lld want %lld; %lld == %d x triangle(%d) means one output "
          "block served them all)",
          after.sum, want_sum, after.sum, A_ROWS / COMPACT_BLOCK_POINTS,
          COMPACT_BLOCK_POINTS);
    check(after.hi == A_ROWS,
          "A5 post-compaction the last value is still reachable (max=%lld want %d)",
          after.hi, A_ROWS);

    check(q_count(db, "a") == A_ROWS,
          "A6 count(*) is %d after compaction (served from ts — right even when "
          "the values are not, which is why this hides)", A_ROWS);

    /* min/max come off the block-stats fast path, which pairs the same way. */
    qres_t mx = q(db, "SELECT max(v) FROM a");
    check(mx.rc == TSDB_OK && mx.rows == 1 && mx.sum == A_ROWS,
          "A7 max(v) off the stats path agrees with the scan (rc=%d got %lld want %d)",
          mx.rc, mx.sum, A_ROWS);

    tsdb_close(db);
    rm_rf(dir);
}

/* ==========================================================================
 * [B] a partial ALTER history, then compacted
 *
 * One partition per table: n_old rows written with (ts,v), then ALTER TABLE ADD
 * COLUMN w, then n_new rows with (ts,v,w).  w is a genuine late add — its real
 * blocks are a contiguous SUFFIX of ts's, which is exactly the hypothesis
 * tsdb_part_open verifies before it front-pads with zero-fill sentinels.
 * Uncompacted that holds, and `SELECT w` reads 0 for the old rows and the real
 * values for the new ones (B1..B6 / B21..B26 — GREEN on HEAD, the baseline this
 * case exists to preserve).
 *
 * Then compaction rewrites the partition, and it has two independent ways to
 * destroy the correspondence:
 *
 *   b   w is ABOVE the block threshold, so it IS rewritten — from ITS OWN row
 *       zero, stamped with `ts_flat[base]` (compaction.c:586), i.e. with the ts
 *       range of the partition's FIRST rows rather than of the rows it holds.
 *
 *   b2  w is BELOW the threshold, so compact_column_file returns before marking
 *       the column eligible (compaction.c:323) and it keeps its old small blocks
 *       while ts and v are re-cut to COMPACT_BLOCK_POINTS around it.
 *
 * Either way the added column must still read zeros for the old rows and its
 * real values for the new ones.  MEASURED RED ON HEAD after compaction, on both
 * tables, by different mechanisms.
 * ======================================================================== */

/* n_old rows of (ts,v), ALTER ADD COLUMN w, then n_new rows of (ts,v,w).
 * Global row i carries ts = D0 + i*STEP and v = i+1; a post-ALTER row carries
 * w = i - n_old + 1, so sum(w) over the table is triangle(n_new). */
static void seed_alter(tsdb_db_t *db, const char *tbl, int n_old, int n_new) {
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    HARD(tsdb_create_table_ex2(db, tbl, cols, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, tbl, &t));
    for (int i = 0; i < n_old; ) {
        int m = (n_old - i < 4096) ? (n_old - i) : 4096;
        tsdb_batch_t *bt = NULL;
        HARD(tsdb_batch_begin(t, &bt));
        for (int k = 0; k < m; k++) {
            HARD(tsdb_batch_row_ts(bt, (tsdb_ts_t)(D0 + (int64_t)(i + k) * STEP)));
            HARD(tsdb_batch_row_i64(bt, 1, i + k + 1));
            HARD(tsdb_batch_row_end(bt));
        }
        HARD(tsdb_batch_commit(bt));
        i += m;
    }
    HARD(tsdb_db_flush_all(db));

    HARD(tsdb_alter_table_add_column(db, tbl, "w", TSDB_TYPE_INT64));

    tsdb_table_t *t2 = NULL;                    /* re-open: the schema grew */
    HARD(tsdb_open_table(db, tbl, &t2));
    int total = n_old + n_new;
    for (int i = n_old; i < total; ) {
        int m = (total - i < 4096) ? (total - i) : 4096;
        tsdb_batch_t *bt = NULL;
        HARD(tsdb_batch_begin(t2, &bt));
        for (int k = 0; k < m; k++) {
            HARD(tsdb_batch_row_ts(bt, (tsdb_ts_t)(D0 + (int64_t)(i + k) * STEP)));
            HARD(tsdb_batch_row_i64(bt, 1, i + k + 1));
            HARD(tsdb_batch_row_i64(bt, 2, i + k - n_old + 1));
            HARD(tsdb_batch_row_end(bt));
        }
        HARD(tsdb_batch_commit(bt));
        i += m;
    }
    HARD(tsdb_db_flush_all(db));
}

/* The same six reads before and after compaction.  `id0` keeps the assertion
 * ids stable per (table, phase): b -> B1..B6 / B11..B16, b2 -> B21.. / B31.. */
static void check_alter(tsdb_db_t *db, const char *dir, const char *tbl,
                        int n_old, int n_new, int id0, const char *phase) {
    const int       total  = n_old + n_new;
    const long long want_w = triangle(n_new);
    const long long want_v = triangle(total);
    const long long split  = D0 + (long long)n_old * STEP;

    char tbl_dir[4096], part[4096];
    snprintf(tbl_dir, sizeof tbl_dir, "%s/%s", dir, tbl);
    if (nth_part(tbl_dir, 0, part, sizeof part))
        printf("  [%s %s] blocks: ts=%ld v=%ld w=%ld\n", tbl, phase,
               idx_count(part, "ts"), idx_count(part, "v"), idx_count(part, "w"));

    char s_all[128], s_v[128], s_old[192], s_new[192];
    snprintf(s_all, sizeof s_all, "SELECT w FROM %s", tbl);
    snprintf(s_v,   sizeof s_v,   "SELECT v FROM %s", tbl);
    snprintf(s_old, sizeof s_old, "SELECT w FROM %s WHERE ts < %lld",  tbl, split);
    snprintf(s_new, sizeof s_new, "SELECT w FROM %s WHERE ts >= %lld", tbl, split);

    qres_t all = q(db, s_all);
    qres_t old = q(db, s_old);
    qres_t nw  = q(db, s_new);
    qres_t v   = q(db, s_v);
    printf("  [%s %s] w all rc=%d rows=%lld sum=%lld (want %lld) | pre-ALTER rc=%d "
           "rows=%lld sum=%lld (want 0) | post-ALTER rc=%d rows=%lld sum=%lld "
           "(want %lld)\n", tbl, phase, all.rc, all.rows, all.sum, want_w,
           old.rc, old.rows, old.sum, nw.rc, nw.rows, nw.sum, want_w);
    if (all.rc != TSDB_OK) printf("  [%s %s] err='%s'\n", tbl, phase, all.err);

    check(all.rc == TSDB_OK && all.rows == total,
          "B%d [%s %s] SELECT w answers every one of the %d rows (rc=%d rows=%lld)",
          id0 + 1, tbl, phase, total, all.rc, all.rows);
    check(all.sum == want_w,
          "B%d [%s %s] sum over w is %lld (got %lld)",
          id0 + 2, tbl, phase, want_w, all.sum);
    check(old.rc == TSDB_OK && old.rows == n_old && old.sum == 0,
          "B%d [%s %s] the %d pre-ALTER rows read w=0 — the legitimate zeros of a "
          "late add, not an error and not another block's values "
          "(rc=%d rows=%lld sum=%lld)",
          id0 + 3, tbl, phase, n_old, old.rc, old.rows, old.sum);
    check(nw.rc == TSDB_OK && nw.rows == n_new && nw.sum == want_w,
          "B%d [%s %s] the %d post-ALTER rows read their real w "
          "(rc=%d rows=%lld sum=%lld want %lld)",
          id0 + 4, tbl, phase, n_new, nw.rc, nw.rows, nw.sum, want_w);
    check(v.rc == TSDB_OK && v.rows == total && v.sum == want_v,
          "B%d [%s %s] the full-length column is unaffected "
          "(rc=%d rows=%lld sum=%lld want %lld)",
          id0 + 5, tbl, phase, v.rc, v.rows, v.sum, want_v);
    check(q_count(db, tbl) == total,
          "B%d [%s %s] count(*) is %d", id0 + 6, tbl, phase, total);
}

static void case_b(void) {
    const char *dir = "/tmp/tsdb_test_dup_ts_compaction_b";
    printf("\n[B] ALTER ADD COLUMN halfway through a partition, then compacted\n");
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    seed_alter(db, "b",  B_OLD,  B_NEW);    /* w above the block threshold */
    seed_alter(db, "b2", B2_OLD, B2_NEW);   /* w below it                  */

    char td[4096], p[4096];
    snprintf(td, sizeof td, "%s/b", dir);
    check(nth_part(td, 1, p, sizeof p) == 0,
          "B0 table b: all %d rows in ONE partition (the ALTER is interior to it)",
          B_TOTAL);
    snprintf(td, sizeof td, "%s/b2", dir);
    check(nth_part(td, 1, p, sizeof p) == 0,
          "B0 table b2: all %d rows in ONE partition", B2_TOTAL);

    printf("  -- uncompacted: w's blocks are still a suffix of ts's (baseline)\n");
    check_alter(db, dir, "b",  B_OLD,  B_NEW,   0, "uncompacted");
    check_alter(db, dir, "b2", B2_OLD, B2_NEW, 20, "uncompacted");

    long long merged = compact_once(db, dir);
    check(merged > 0,
          "B0c the compactor rewrote the partitions (parts_merged=%lld) — without "
          "this the two phases below measure the same layout twice", merged);

    printf("  -- compacted: ts and v were re-cut around w\n");
    check_alter(db, dir, "b",  B_OLD,  B_NEW,  10, "compacted");
    check_alter(db, dir, "b2", B2_OLD, B2_NEW, 30, "compacted");

    tsdb_close(db);
    rm_rf(dir);
}

/* ==========================================================================
 * [C] an INTERIOR loss inside a duplicate-key run
 *
 * C_ROWS rows, all at ONE timestamp, in one partition, block_points 1024 → 8
 * blocks per column, every entry reading (C_TS, C_TS, 1024).  Then one INTERIOR
 * entry is removed from v.idx while ts and the witness column g keep all eight.
 *
 * On HEAD tsdb_part_open sees v declaring 7 against ts's 8, tests the late-add
 * hypothesis "cm[b] equals ts_m[b+1] on (ts_min,count)" — which every pair of
 * identical keys satisfies — concludes ALTER, prepends a zero-fill sentinel and
 * slides the seven real blocks one slot later.  It logs nothing, because as far
 * as it can tell this is an ordinary ALTER-added column.
 *
 * MEASURED on HEAD: `SELECT v` returns rc=0, 8192 rows, sum=0.  Not "one block
 * of zeros" — ALL of them, because the prepended sentinel now sits at index 0
 * of v's array and col_block_pair's first-match on (ts_min,count) lands on it
 * for every one of the eight ts blocks.  A column lost one block and the engine
 * answered with 8192 fabricated values and no error.
 *
 * The remaining fields cannot say WHICH ordinal is missing, so there is no
 * recovery: the affected reads must ERROR.  What must NOT change is the blast
 * radius — count(*), the timestamps and the healthy column g stay exact.
 *
 * RED ON HEAD: C1..C3 (v must fail; HEAD answers rc=0 with fabricated zeros)
 * and C4/C5 (g is a full-length column whose eight blocks are indistinguishable,
 * so first-match pairing serves g's block 0 eight times: sum=8 x triangle(1024)
 * = 4198400 instead of triangle(8192) = 33558528).
 * ======================================================================== */
static void case_c(void) {
    const char *dir = "/tmp/tsdb_test_dup_ts_compaction_c";
    printf("\n[C] one interior block lost from a run whose keys are all equal\n");
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },   /* loses an interior idx entry */
        { "g",  TSDB_TYPE_INT64     },   /* witness: stays whole        */
    };
    HARD(tsdb_create_table_ex2(db, "c", cols, 3, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "c", &t));
    for (int i = 0; i < C_ROWS; ) {
        int m = (C_ROWS - i < 4096) ? (C_ROWS - i) : 4096;
        tsdb_batch_t *bt = NULL;
        HARD(tsdb_batch_begin(t, &bt));
        for (int k = 0; k < m; k++) {
            HARD(tsdb_batch_row_ts(bt, (tsdb_ts_t)C_TS));
            HARD(tsdb_batch_row_i64(bt, 1, i + k + 1));
            HARD(tsdb_batch_row_i64(bt, 2, i + k + 1));
            HARD(tsdb_batch_row_end(bt));
        }
        HARD(tsdb_batch_commit(bt));
        i += m;
    }
    tsdb_close(db); db = NULL;                      /* close flushes */

    char tbl_dir[4096], part[4096];
    snprintf(tbl_dir, sizeof tbl_dir, "%s/c", dir);
    if (!nth_part(tbl_dir, 0, part, sizeof part)) {
        check(0, "C0 the partition was flushed");
        rm_rf(dir);
        return;
    }
    printf("  %s: ts=%ld blocks, v=%ld, g=%ld — all reading (%lld,%lld,%d)\n",
           part, idx_count(part, "ts"), idx_count(part, "v"), idx_count(part, "g"),
           (long long)C_TS, (long long)C_TS, BP);
    check(idx_count(part, "ts") == C_ROWS / BP,
          "C0 the run really is %d indistinguishable blocks", C_ROWS / BP);
    if (idx_drop_entry(part, "v", C_DROP) != 0) {
        check(0, "C0 could not remove the interior entry");
        rm_rf(dir);
        return;
    }

    HARD(tsdb_open(dir, &db));

    const long long want = triangle(C_ROWS);

    qres_t v = q(db, "SELECT v FROM c");
    printf("  SELECT v: rc=%d rows=%lld sum=%lld (an intact run sums to %lld) "
           "err='%s'\n", v.rc, v.rows, v.sum, want, v.err);
    check(v.rc != TSDB_OK && v.rows == 0,
          "C1 a column that lost an interior block of an all-equal-key run is "
          "REFUSED, never re-served from a surviving block (rc=%d rows=%lld "
          "sum=%lld)", v.rc, v.rows, v.sum);
    check(strstr(v.err, "'v'") != NULL,
          "C2 the refusal names the column 'v' (err='%s')", v.err);

    qres_t agg = q(db, "SELECT sum(v) FROM c");
    check(agg.rc != TSDB_OK,
          "C3 the aggregate path refuses it too, rather than summing a "
          "fabricated block (rc=%d sum=%lld)", agg.rc, agg.sum);

    qres_t g = q(db, "SELECT g FROM c");
    printf("  SELECT g: rc=%d rows=%lld sum=%lld (want %lld) min=%lld max=%lld\n",
           g.rc, g.rows, g.sum, want, g.lo, g.hi);
    check(g.rc == TSDB_OK && g.rows == C_ROWS && g.sum == want,
          "C4 the whole column beside it reads every row with its own value "
          "(rc=%d rows=%lld sum=%lld want %lld)", g.rc, g.rows, g.sum, want);
    check(g.hi == C_ROWS,
          "C5 the witness column's last value is reachable (max=%lld want %d)",
          g.hi, C_ROWS);

    check(q_count(db, "c") == C_ROWS,
          "C6 count(*) is complete — the damage is one column, not the partition");

    qres_t ts = q(db, "SELECT ts FROM c");
    check(ts.rc == TSDB_OK && ts.rows == C_ROWS,
          "C7 SELECT ts still enumerates every row (rc=%d rows=%lld)",
          ts.rc, ts.rows);

    tsdb_close(db);
    rm_rf(dir);
}

/* ==========================================================================
 * [D] a repeated run immediately before a partition boundary and another
 *     immediately after it
 *
 * Day 0: D_DIST distinct rows, then D_RUN rows all at D_TS0.
 * Day 1: D_RUN rows all at D_TS1, then D_DIST distinct rows.
 * Values are globally unique 1..D_TOTAL, so any cross-run reuse shows up in a
 * per-run sum and not only in a total.
 *
 * Partitioning is deterministic on the timestamp, so one literal timestamp
 * cannot span both days: the two runs are separate populations.  Whatever
 * ordinal a fix makes durable therefore has to be PARTITION-LOCAL — a global or
 * per-table counter pairs day 1's run-block 0 against day 0's array.
 *
 * RED ON HEAD: within each day the run's four blocks are indistinguishable, so
 * first-match already mis-pairs.  The per-run and per-day sums are what make a
 * cross-partition confusion distinguishable from that.
 * ======================================================================== */
static void seed_d_segment(tsdb_table_t *t, int base, int n,
                           int64_t fixed_ts, int64_t start_ts) {
    for (int i = 0; i < n; ) {
        int m = (n - i < 4096) ? (n - i) : 4096;
        tsdb_batch_t *bt = NULL;
        HARD(tsdb_batch_begin(t, &bt));
        for (int k = 0; k < m; k++) {
            int64_t ts = fixed_ts ? fixed_ts : start_ts + (int64_t)(i + k) * STEP;
            HARD(tsdb_batch_row_ts(bt, (tsdb_ts_t)ts));
            HARD(tsdb_batch_row_i64(bt, 1, base + i + k + 1));
            HARD(tsdb_batch_row_end(bt));
        }
        HARD(tsdb_batch_commit(bt));
        i += m;
    }
}

static void case_d(void) {
    const char *dir = "/tmp/tsdb_test_dup_ts_compaction_d";
    printf("\n[D] a repeated run at the end of one partition and the start of the next\n");
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    HARD(tsdb_create_table_ex2(db, "d", cols, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "d", &t));

    seed_d_segment(t, 0,                   D_DIST, 0,     D0);        /* day 0 head */
    seed_d_segment(t, D_DIST,              D_RUN,  D_TS0, 0);         /* day 0 run  */
    seed_d_segment(t, D_DIST + D_RUN,      D_RUN,  D_TS1, 0);         /* day 1 run  */
    seed_d_segment(t, D_DIST + 2 * D_RUN,  D_DIST, 0,
                   D1 + 3600000000000LL);                             /* day 1 tail */
    HARD(tsdb_db_flush_all(db));

    char tbl_dir[4096], p0[4096], p1[4096];
    snprintf(tbl_dir, sizeof tbl_dir, "%s/d", dir);
    check(nth_part(tbl_dir, 0, p0, sizeof p0) && nth_part(tbl_dir, 1, p1, sizeof p1),
          "D0 the two runs really did land in two different partitions");
    check(nth_part(tbl_dir, 2, p0, sizeof p0) == 0, "D0 and in exactly two");
    if (nth_part(tbl_dir, 0, p0, sizeof p0) && nth_part(tbl_dir, 1, p1, sizeof p1))
        printf("  day 0 %s: ts=%ld blocks | day 1 %s: ts=%ld blocks\n",
               p0, idx_count(p0, "ts"), p1, idx_count(p1, "ts"));

    const long long day0_hi  = D_DIST + D_RUN;                 /* 6144  */
    const long long run0_lo  = D_DIST,     run0_hi = D_DIST + D_RUN;
    const long long run1_lo  = day0_hi,    run1_hi = day0_hi + D_RUN;
    const long long want_all  = triangle(D_TOTAL);
    const long long want_day0 = triangle(day0_hi);
    const long long want_day1 = want_all - want_day0;
    const long long want_run0 = triangle(run0_hi) - triangle(run0_lo);
    const long long want_run1 = triangle(run1_hi) - triangle(run1_lo);

    char q_day0[192], q_day1[192], q_run0[192], q_run1[192];
    snprintf(q_day0, sizeof q_day0, "SELECT v FROM d WHERE ts < %lld", (long long)D1);
    snprintf(q_day1, sizeof q_day1, "SELECT v FROM d WHERE ts >= %lld", (long long)D1);
    snprintf(q_run0, sizeof q_run0, "SELECT v FROM d WHERE ts = %lld", (long long)D_TS0);
    snprintf(q_run1, sizeof q_run1, "SELECT v FROM d WHERE ts = %lld", (long long)D_TS1);

    for (int phase = 0; phase < 2; phase++) {
        const char *tag = phase ? "compacted" : "flushed";
        if (phase) {
            long long merged = compact_once(db, dir);
            check(merged >= 2, "D8 the compactor rewrote both partitions "
                  "(parts_merged=%lld)", merged);
        }
        qres_t all  = q(db, "SELECT v FROM d");
        qres_t dd0  = q(db, q_day0);
        qres_t dd1  = q(db, q_day1);
        qres_t r0   = q(db, q_run0);
        qres_t r1   = q(db, q_run1);
        printf("  [%s] all rows=%lld sum=%lld (want %lld) | day0 rows=%lld sum=%lld "
               "(want %lld) | day1 rows=%lld sum=%lld (want %lld)\n",
               tag, all.rows, all.sum, want_all, dd0.rows, dd0.sum, want_day0,
               dd1.rows, dd1.sum, want_day1);
        printf("  [%s] run@day0 rows=%lld sum=%lld (want %lld) | "
               "run@day1 rows=%lld sum=%lld (want %lld)\n",
               tag, r0.rows, r0.sum, want_run0, r1.rows, r1.sum, want_run1);

        check(all.rc == TSDB_OK && all.rows == D_TOTAL && all.sum == want_all,
              "D1/%s every row of both partitions keeps its own value "
              "(rc=%d rows=%lld sum=%lld want %lld)",
              tag, all.rc, all.rows, all.sum, want_all);
        check(dd0.rc == TSDB_OK && dd0.rows == day0_hi && dd0.sum == want_day0,
              "D2/%s partition day 0 is exact (rows=%lld sum=%lld want %lld)",
              tag, dd0.rows, dd0.sum, want_day0);
        check(dd1.rc == TSDB_OK && dd1.rows == D_TOTAL - day0_hi &&
              dd1.sum == want_day1,
              "D3/%s partition day 1 is exact (rows=%lld sum=%lld want %lld)",
              tag, dd1.rows, dd1.sum, want_day1);
        check(r0.rc == TSDB_OK && r0.rows == D_RUN && r0.sum == want_run0,
              "D4/%s the run ending day 0 returns its own %d values "
              "(rows=%lld sum=%lld want %lld)",
              tag, D_RUN, r0.rows, r0.sum, want_run0);
        check(r1.rc == TSDB_OK && r1.rows == D_RUN && r1.sum == want_run1,
              "D5/%s the run starting day 1 returns its own %d values, not day 0's "
              "(rows=%lld sum=%lld want %lld)",
              tag, D_RUN, r1.rows, r1.sum, want_run1);
        check(r0.sum != r1.sum,
              "D6/%s the two runs are not the same answer twice "
              "(%lld vs %lld)", tag, r0.sum, r1.sum);
        check(q_count(db, "d") == D_TOTAL, "D7/%s count(*) is %d", tag, D_TOTAL);
    }

    tsdb_close(db);
    rm_rf(dir);
}

int main(void) {
    /* The engine logs to stderr; keep stdout line-buffered so a redirected log
     * interleaves in the order things actually happened. */
    setvbuf(stdout, NULL, _IOLBF, 0);
    printf("=== test_dup_ts_compaction ===\n");
    printf("block_points=%d  COMPACT_BLOCK_POINTS=%d  wal_only_commit=%s\n",
           BP, COMPACT_BLOCK_POINTS,
           getenv("TSDB_WAL_ONLY_COMMIT") ? getenv("TSDB_WAL_ONLY_COMMIT") : "unset");
    case_a();
    case_b();
    case_c();
    case_d();
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
