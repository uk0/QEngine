/* test_short_column_read.c — a SHORT non-ts column must not poison the read.
 *
 * SCOPE.  A "short" column here is one whose .idx declares FEWER blocks than
 * ts's, with the shortfall NOT shaped like a late-added suffix — the shape a
 * per-column raw-block replication apply leaves when it lands some columns and
 * not others, or a lost .idx rename resurrects a previous generation.  The
 * companion case where a column has NO .idx at all (zero-fill fabrication) is
 * owned elsewhere and is deliberately NOT exercised here.  test_torn_value_column
 * covers the third shape: idx count EQUAL to ts but the .col torn, which is a
 * tail tear and is clamped, not holed.
 *
 * WHAT tsdb_part_open ALREADY GUARANTEES (pinned by CONTRACT A below): it
 * aligns the short column's block array 1:1 with ts and flags the slots it has
 * no data for.  No ts block is ever hidden, so count(*), SELECT ts, every
 * healthy column and any ts range that misses the hole return exactly what an
 * intact partition returns.  Nothing in the suite tested that, so a regression
 * widening the blast radius back to the whole partition would ship green.
 *
 * WHAT THIS TEST DRIVES (CONTRACT B).  A cell inside the hole has no truthful
 * answer: zero is a value (it poisons aggregates and hides the loss from
 * anti-entropy, which compares count/max(ts)); dropping the row would make
 * `SELECT c7` disagree with `count(*)` at rc=0 on both; and "unavailable" is
 * not representable — result cells are raw 8-byte slots with no NULL bit, so
 * any marker would reach every existing client as ordinary data.  So the query
 * must fail.  What it must NOT do is fail BLIND, which is what shipped: a bare
 * TSDB_ERR_CORRUPT naming no column and no ts range, indistinguishable from a
 * CRC failure, withholding exactly the two facts needed to construct either
 * retry that DOES return complete data (drop that column, or restrict the ts
 * range).  Every B assertion below is RED before the exec.c fix.
 *
 * The failure must also stay BLOCK-scoped, not partition-scoped: a ts range
 * inside the damaged partition but past the hole still answers in full (B11).
 *
 * CONTRACT C — ONE ENGINE, ONE ANSWER.  The decision above is worth nothing if
 * only some execution paths implement it.  The engine pairs a non-ts column's
 * block to a ts block by first-match on (ts_min,count), and that loop was
 * open-coded EIGHT times in exec.c.  Two of the copies did not ask whether the
 * paired slot was a hole, and one of those decided a query: the stats fast
 * path served `SELECT count(c7) FROM t` from the ts block's row count, so the
 * damaged table reported c7 healthy with rc=0 — the same query that the scan
 * path (TSDB_DISABLE_STATS_FASTPATH=1) refused as TSDB_ERR_CORRUPT.  Two
 * answers to one query, selected by an environment variable.  C asserts they
 * agree, and that declining the fast path costs a fast path and not
 * availability: count(*), a healthy column's count, and min/max over a healthy
 * column all still answer exactly.
 *
 * CONTRACT D — a bloom skip must never DELETE rows.  bloom_can_skip_block was
 * the eighth copy.  It reads the SYMBOL column's paired block to test the
 * filter, and a skip removes the block from the answer silently, at rc=0.  D
 * puts a symbol value ONLY inside the holed block: if the skip ever fires on
 * an unreadable slot, `WHERE sym='hot'` answers "no such rows" instead of
 * failing.  Green before the fold too — the placeholder happens to have
 * TSDB_BF_HAS_BLOOM clear — which is why it is a guard rather than a repro.
 */

#include "tsdb.h"
#include "../src/server/proto.h"        /* tsdb_crc32c — CONTRACT E rewrites a block CRC */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *d) { char c[4096]; snprintf(c, sizeof c, "rm -rf %s", d); (void)system(c); }

#define NCOL     20                      /* c0..c19, one of which goes short */
#define BP       1024                    /* block_points → 4 blocks/partition */
#define NDAYS    3
#define PER_DAY  4096
#define NROWS    (NDAYS * PER_DAY)
#define BASE_TS  1000000000000LL
#define STEP_NS  1000000LL
#define DAY_NS   86400000000000LL

#define SHORT_COL   7                    /* c7 — one of twenty */
#define DAMAGED_DAY 1                    /* middle partition */
#define DAMAGED_BLK 1                    /* middle block: not a suffix shortfall */

/* Table 2 (CONTRACT D): one day, (ts, sym, v).  'hot' is written ONLY into the
 * block whose sym.idx entry is dropped, so a bloom skip of that block turns
 * "unreadable" into "absent" — a wrong answer at rc=0. */
#define D_HOT   "hot"
#define D_COLD  "cold"
static const char *d_sym_of(int i) { return (i / BP == DAMAGED_BLK) ? D_HOT : D_COLD; }

/* Value model: column j, row i within its day, holds j*1000000 + i. */
static int64_t cell_of(int j, int i) { return (int64_t)j * 1000000 + i; }
static int64_t ts_of(int d, int i)   { return BASE_TS + (int64_t)d * DAY_NS + (int64_t)i * STEP_NS; }

/* ts bounds of the block that c7 loses. */
static int64_t hole_ts_min(void) { return ts_of(DAMAGED_DAY, DAMAGED_BLK * BP); }
static int64_t hole_ts_max(void) { return ts_of(DAMAGED_DAY, (DAMAGED_BLK + 1) * BP - 1); }

/* ---- little-endian idx helpers (mirror part.c's on-disk layout) ---------- */
static uint32_t g32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t g16(const uint8_t *p) { return (uint16_t)(p[0] | (p[1] << 8)); }
static void     p32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}

/*
 * Remove one entry from <part>/<col>.idx and decrement the header count.
 *
 * This is the hand-built short-column state: the .col keeps every byte, only
 * the manifest forgets a block.  It is byte-for-byte what a partition looks
 * like when a per-column raw-block apply skipped one (column, block) — the
 * writer's own comment in part.c calls that path out — or when a crash lost the
 * .idx rename and resurrected the previous generation.
 */
static int idx_drop_entry(const char *part, const char *col, int drop) {
    char path[4200];                     /* > part[4096] + "/" + col + ".idx" */
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
    printf("  built short column: %s lost entry %d (%u -> %zu blocks; ts still %u)\n",
           path, drop, count, nkeep, count);
    return 0;
}

/*
 * CONTRACT E's damage: set bit 4 of the FIRST on-disk block header's flags in
 * <part>/<col>.col, and re-stamp the trailing CRC32C so the block is otherwise
 * pristine.
 *
 * BlockHeader is 32 bytes: magic u32 @0, codec u8 @4, pad @5, flags u16 @6,
 * count u32 @8, ts_min i64 @12, ts_max i64 @20, size u32 @28; the CRC32C
 * trailer covers header+data and follows the data.  Bit 4 is what
 * TSDB_BLOCK_FLAG_HOLE occupies IN MEMORY, and bits 0..3 of this same u16 are
 * already allocated on disk (OUTER_LZ/NOT_NULL/HAS_BLOOM/HAS_CRC) — so bit 4
 * is the next one anybody adding a block flag would take.  A reader that keys
 * on the flag ALONE turns every such block into a bogus "no stored value"
 * error; keying on the offset==UINT64_MAX sentinel too, exactly as
 * tsdb_part_read_block does, keeps old blocks readable.
 */
#define ONDISK_BIT4 (1u << 4)
static int block0_set_flag_bit4(const char *part, const char *col) {
    char path[4200];
    snprintf(path, sizeof path, "%s/%s.col", part, col);
    FILE *f = fopen(path, "r+b");
    if (!f) { fprintf(stderr, "no such col: %s\n", path); return -1; }
    uint8_t hdr[32];
    if (fread(hdr, 1, 32, f) != 32) { fclose(f); return -1; }
    if (g32(hdr) != 0x314B4C42u) {              /* TSDB_BLOCK_MAGIC "BLK1" */
        fprintf(stderr, "block 0 of %s is not at offset 0\n", path);
        fclose(f); return -1;
    }
    uint16_t flags = g16(hdr + 6);
    uint32_t dsz   = g32(hdr + 28);
    if (flags & ONDISK_BIT4) { fclose(f); return -1; }   /* already in use */
    int had_crc = (flags & (1u << 3)) != 0;              /* TSDB_BLOCK_FLAG_HAS_CRC */
    flags |= ONDISK_BIT4;
    hdr[6] = (uint8_t)flags; hdr[7] = (uint8_t)(flags >> 8);

    uint8_t *data = malloc(dsz ? dsz : 1);
    if (!data) { fclose(f); return -1; }
    if (dsz && fread(data, 1, dsz, f) != dsz) { free(data); fclose(f); return -1; }

    if (fseek(f, 0, SEEK_SET) != 0) { free(data); fclose(f); return -1; }
    if (fwrite(hdr, 1, 32, f) != 32) { free(data); fclose(f); return -1; }
    if (had_crc) {
        uint32_t crc = tsdb_crc32c(hdr, 32);
        if (dsz) crc = tsdb_crc32c_update(crc, data, dsz);
        uint8_t tr[4]; p32(tr, crc);
        if (fseek(f, (long)(32 + dsz), SEEK_SET) != 0) { free(data); fclose(f); return -1; }
        if (fwrite(tr, 1, 4, f) != 4) { free(data); fclose(f); return -1; }
    }
    free(data);
    fclose(f);
    printf("  set on-disk block-flag bit 4 on %s block 0 (crc re-stamped: %d)\n",
           path, had_crc);
    return 0;
}

/* nth partition directory of a table, in ascending day order. */
#define PART_NAME_MAX 1024               /* >= sizeof(struct dirent.d_name) anywhere */
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

/* ---- dataset ------------------------------------------------------------ */

static void write_all(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); exit(1); }
    tsdb_col_t cols[NCOL + 1];
    static char names[NCOL][8];
    cols[0].name = "ts"; cols[0].type = TSDB_TYPE_TIMESTAMP;
    for (int i = 0; i < NCOL; i++) {
        snprintf(names[i], sizeof names[i], "c%d", i);
        cols[i + 1].name = names[i];
        cols[i + 1].type = TSDB_TYPE_INT64;
    }
    int rc = tsdb_create_table_ex2(db, "t", cols, NCOL + 1, "ts", TSDB_CREATE_PART_DAY, BP);
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create rc=%d\n", rc); exit(1); }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "open_table failed\n"); exit(1); }
    for (int d = 0; d < NDAYS; d++) {
        tsdb_batch_t *b = NULL;
        tsdb_batch_begin(t, &b);
        for (int i = 0; i < PER_DAY; i++) {
            tsdb_batch_row_ts(b, ts_of(d, i));
            for (int c = 0; c < NCOL; c++) tsdb_batch_row_i64(b, c + 1, cell_of(c, i));
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);
    }

    /* CONTRACT D's table: one day, (ts, sym, v). */
    tsdb_col_t c2[3];
    c2[0].name = "ts";  c2[0].type = TSDB_TYPE_TIMESTAMP;
    c2[1].name = "sym"; c2[1].type = TSDB_TYPE_SYMBOL;
    c2[2].name = "v";   c2[2].type = TSDB_TYPE_INT64;
    rc = tsdb_create_table_ex2(db, "t2", c2, 3, "ts", TSDB_CREATE_PART_DAY, BP);
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create t2 rc=%d\n", rc); exit(1); }
    tsdb_table_t *t2 = NULL;
    if (tsdb_open_table(db, "t2", &t2) != TSDB_OK) { fprintf(stderr, "open t2 failed\n"); exit(1); }
    {
        tsdb_batch_t *b = NULL;
        tsdb_batch_begin(t2, &b);
        for (int i = 0; i < PER_DAY; i++) {
            tsdb_batch_row_ts(b, ts_of(0, i));
            tsdb_batch_row_sym(b, 1, d_sym_of(i));
            tsdb_batch_row_i64(b, 2, cell_of(0, i));
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);
    }
    tsdb_close(db);
}

/* Write from a CHILD that leaves via _exit(): every byte the assertions below
 * read came off disk after a real process death, never from live state. */
static void build_dataset(const char *dir) {
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) { write_all(dir); _exit(0); }
    int st = 0;
    (void)waitpid(pid, &st, 0);
    CHECK(pid > 0 && WIFEXITED(st) && WEXITSTATUS(st) == 0,
          "writer child flushed the dataset then exited");
}

/* ---- query helpers ------------------------------------------------------ */

typedef struct { int rc; long long rows; const char *err; } qres_t;
static char g_err_copy[512];

/* Run a query, discard rows, capture rc + the executor's diagnostic. */
static qres_t q_run(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    qres_t out = {0, 0, NULL};
    out.rc = tsdb_query(db, sql, &r);
    if (out.rc == TSDB_OK && r) while (tsdb_result_next(r) > 0) out.rows++;
    if (r) tsdb_result_free(r);
    const char *e = tsdb_last_error();
    snprintf(g_err_copy, sizeof g_err_copy, "%s", e ? e : "");
    out.err = g_err_copy;
    return out;
}

static int64_t q_scalar(tsdb_db_t *db, const char *sql, int *ok) {
    tsdb_result_t *r = NULL; int64_t v = 0; *ok = 0;
    if (tsdb_query(db, sql, &r) == TSDB_OK && r) {
        if (tsdb_result_next(r) > 0) { v = tsdb_result_i64(r, 0); *ok = 1; }
        tsdb_result_free(r);
    }
    return v;
}

/* SELECT ts, c<j> — verify EVERY returned cell against the value model.
 * Returns rows via *out_rows, mis-paired cells via *out_wrong. */
static int q_verify_col(tsdb_db_t *db, int j, const char *where,
                        long long *out_rows, long long *out_wrong) {
    char sql[256];
    snprintf(sql, sizeof sql, "SELECT ts, c%d FROM t%s%s", j,
             where ? " WHERE " : "", where ? where : "");
    tsdb_result_t *r = NULL;
    *out_rows = 0; *out_wrong = 0;
    int rc = tsdb_query(db, sql, &r);
    if (rc == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            int64_t v  = tsdb_result_i64(r, 1);
            int64_t off = ts - BASE_TS;
            int     i   = (int)((off % DAY_NS) / STEP_NS);
            if (v != cell_of(j, i)) (*out_wrong)++;
            (*out_rows)++;
        }
    }
    if (r) tsdb_result_free(r);
    return rc;
}

/* ---- differential: the same query down both execution paths -------------
 *
 * exec.c caches TSDB_DISABLE_STATS_FASTPATH in a function-local static on
 * first use, so the two paths cannot be A/B'd inside one process.  Run each
 * in its own child and pipe the verdict back.  `fastpath_off` selects the
 * path; the db must be CLOSED in the parent when this runs. */
typedef struct { int rc; long long rows; long long v0; int names_col; } diff_t;

static diff_t run_isolated(const char *dir, const char *sql, int fastpath_off) {
    diff_t d; memset(&d, 0, sizeof d);
    d.rc = -998;
    int fds[2];
    if (pipe(fds) != 0) return d;
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        close(fds[0]);
        if (fastpath_off) setenv("TSDB_DISABLE_STATS_FASTPATH", "1", 1);
        else              unsetenv("TSDB_DISABLE_STATS_FASTPATH");
        diff_t o; memset(&o, 0, sizeof o);
        o.rc = -997;
        tsdb_db_t *cdb = NULL;
        if (tsdb_open(dir, &cdb) == TSDB_OK) {
            tsdb_result_t *r = NULL;
            o.rc = tsdb_query(cdb, sql, &r);
            if (o.rc == TSDB_OK && r) {
                while (tsdb_result_next(r) > 0) {
                    if (o.rows == 0) o.v0 = (long long)tsdb_result_i64(r, 0);
                    o.rows++;
                }
            }
            if (r) tsdb_result_free(r);
            const char *e = tsdb_last_error();
            char needle[16]; snprintf(needle, sizeof needle, "'c%d'", SHORT_COL);
            o.names_col = (e && strstr(e, needle) != NULL);
            tsdb_close(cdb);
        }
        ssize_t wn = write(fds[1], &o, sizeof o);
        close(fds[1]);
        _exit(wn == (ssize_t)sizeof o ? 0 : 1);
    }
    close(fds[1]);
    if (pid > 0) {
        ssize_t rn = read(fds[0], &d, sizeof d);
        if (rn != (ssize_t)sizeof d) d.rc = -996;
    }
    close(fds[0]);
    int st = 0;
    if (pid > 0) (void)waitpid(pid, &st, 0);
    return d;
}

/* The whole point of folding the eight pair-matchers: the answer must not
 * depend on which path produced it. */
static void expect_paths_agree(const char *dir, const char *sql, const char *label) {
    char msg[320];
    diff_t fast = run_isolated(dir, sql, 0);
    diff_t scan = run_isolated(dir, sql, 1);

    snprintf(msg, sizeof msg, "%s: both paths ran (harness not silently broken)", label);
    CHECK(fast.rc > -900 && scan.rc > -900, msg);

    snprintf(msg, sizeof msg, "%s: stats fast path and scan path return the SAME rc", label);
    CHECK(fast.rc == scan.rc, msg);

    snprintf(msg, sizeof msg, "%s: stats fast path and scan path return the SAME rows/value", label);
    CHECK(fast.rows == scan.rows && fast.v0 == scan.v0, msg);

    if (fast.rc != scan.rc || fast.rows != scan.rows || fast.v0 != scan.v0)
        fprintf(stderr, "  [%s] fastpath rc=%d rows=%lld v0=%lld | scan rc=%d rows=%lld v0=%lld\n",
                label, fast.rc, fast.rows, fast.v0, scan.rc, scan.rows, scan.v0);
}

/* An unanswerable query must fail, and the failure must name the column and
 * the ts range that is gone — the two facts a caller needs to retry. */
static void expect_named_failure(tsdb_db_t *db, const char *sql, const char *label) {
    char msg[320];
    qres_t q = q_run(db, sql);

    snprintf(msg, sizeof msg, "%s: fails instead of answering from a hole", label);
    CHECK(q.rc != TSDB_OK, msg);

    snprintf(msg, sizeof msg, "%s: returns no rows on failure (never a partial set)", label);
    CHECK(q.rows == 0, msg);

    snprintf(msg, sizeof msg, "%s: error names the short column 'c%d'", label, SHORT_COL);
    char needle[16]; snprintf(needle, sizeof needle, "'c%d'", SHORT_COL);
    CHECK(strstr(q.err, needle) != NULL, msg);

    char tsbuf[32];
    snprintf(tsbuf, sizeof tsbuf, "%lld", (long long)hole_ts_min());
    snprintf(msg, sizeof msg, "%s: error names the missing block's ts range", label);
    CHECK(strstr(q.err, tsbuf) != NULL, msg);

    if (q.rc != TSDB_OK && strstr(q.err, needle) == NULL)
        fprintf(stderr, "  [%s] rc=%d err='%s'\n", label, q.rc, q.err);
}

int main(void) {
    const char *dir = "/tmp/tsdb_test_short_column_read";
    printf("=== test_short_column_read ===\n");
    rmrf(dir);
    build_dataset(dir);

    /* ---- build the short-column state by hand ---------------------------- */
    char table_dir[512], part[4096];
    snprintf(table_dir, sizeof table_dir, "%s/t", dir);
    if (!nth_part(table_dir, DAMAGED_DAY, part, sizeof part)) {
        fprintf(stderr, "FAIL: partition %d not flushed\n", DAMAGED_DAY);
        g_fail++;
        printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
        return 1;
    }
    char shortname[8]; snprintf(shortname, sizeof shortname, "c%d", SHORT_COL);
    if (idx_drop_entry(part, shortname, DAMAGED_BLK) != 0) {
        fprintf(stderr, "FAIL: could not build the short-column state\n");
        g_fail++;
        printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
        return 1;
    }
    printf("  hole covers ts [%lld,%lld] of partition %s\n",
           (long long)hole_ts_min(), (long long)hole_ts_max(), part);

    /* Same damage to t2's SYMBOL column, in its only partition (CONTRACT D). */
    char t2_dir[512], t2_part[4096];
    snprintf(t2_dir, sizeof t2_dir, "%s/t2", dir);
    if (!nth_part(t2_dir, 0, t2_part, sizeof t2_part) ||
        idx_drop_entry(t2_part, "sym", DAMAGED_BLK) != 0) {
        fprintf(stderr, "FAIL: could not build the short SYMBOL-column state\n");
        g_fail++;
        printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
        return 1;
    }

    /* CONTRACT E: an UNDAMAGED partition, one real block of which carries the
     * on-disk flag bit that TSDB_BLOCK_FLAG_HOLE occupies in memory. */
    char clean_part[4096];
    char e_col[8]; snprintf(e_col, sizeof e_col, "c%d", NCOL - 1);
    if (!nth_part(table_dir, 0, clean_part, sizeof clean_part) ||
        block0_set_flag_bit4(clean_part, e_col) != 0) {
        fprintf(stderr, "FAIL: could not set the on-disk flag bit\n");
        g_fail++;
        printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
        return 1;
    }

    /* ==== CONTRACT C, differential ====
     * Runs FIRST, and it has to: run_isolated() forks, and once this process
     * has executed one query it owns a parallel-scan thread pool whose mutexes
     * a forked child inherits locked (fork clones only the calling thread).
     * Here the process has never opened a db, so the children are clean. */
    printf("\n[C6..C8] the two execution paths agree, measured not argued\n");
    expect_paths_agree(dir, "SELECT count(c7) FROM t", "C6 count(short col)");
    expect_paths_agree(dir, "SELECT count(*) FROM t",  "C7 count(*)");
    expect_paths_agree(dir, "SELECT sum(c0) FROM t",   "C8 sum(healthy col)");

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen failed\n"); return 1; }

    /* ==== CONTRACT A — one short column must not poison anything else ==== */
    printf("\n[A] the damage stays inside the column that is short\n");
    {
        int ok = 0;
        int64_t n = q_scalar(db, "SELECT count(*) FROM t", &ok);
        CHECK(ok && n == NROWS, "A1 count(*) is complete (ts blocks all enumerable)");
    }
    {
        long long rows = 0, wrong = 0;
        int rc = q_verify_col(db, 0, NULL, &rows, &wrong);
        CHECK(rc == TSDB_OK && rows == NROWS && wrong == 0,
              "A2 a healthy column scans all rows with every value correct");
    }
    {
        long long rows = 0, wrong = 0;
        int rc = q_verify_col(db, NCOL - 1, NULL, &rows, &wrong);
        CHECK(rc == TSDB_OK && rows == NROWS && wrong == 0,
              "A3 the last healthy column is unaffected too");
    }
    {
        qres_t q = q_run(db, "SELECT ts, c0, c1, c19 FROM t");
        CHECK(q.rc == TSDB_OK && q.rows == NROWS,
              "A4 a multi-column projection that omits the short column is complete");
    }
    {
        int ok = 0;
        int64_t got = q_scalar(db, "SELECT sum(c0) FROM t", &ok);
        int64_t want = 0;
        for (int i = 0; i < PER_DAY; i++) want += cell_of(0, i);
        want *= NDAYS;
        CHECK(ok && got == want, "A5 an aggregate over a healthy column is exact");
    }
    {   /* undamaged partition before the hole */
        char w[128];
        snprintf(w, sizeof w, "ts < %lld", (long long)ts_of(DAMAGED_DAY, 0));
        long long rows = 0, wrong = 0;
        int rc = q_verify_col(db, SHORT_COL, w, &rows, &wrong);
        CHECK(rc == TSDB_OK && rows == PER_DAY && wrong == 0,
              "A6 the SHORT column reads complete + correct before the hole");
    }
    {   /* undamaged partition after the hole */
        char w[128];
        snprintf(w, sizeof w, "ts >= %lld", (long long)ts_of(DAMAGED_DAY + 1, 0));
        long long rows = 0, wrong = 0;
        int rc = q_verify_col(db, SHORT_COL, w, &rows, &wrong);
        CHECK(rc == TSDB_OK && rows == PER_DAY && wrong == 0,
              "A7 the SHORT column reads complete + correct after the hole");
    }

    /* ==== CONTRACT B — the unanswerable query fails, and says why ==== */
    printf("\n[B] a query that needs the hole fails LOUDLY, not blind\n");
    expect_named_failure(db, "SELECT c7 FROM t",                       "B1 scan");
    expect_named_failure(db, "SELECT * FROM t",                        "B2 SELECT *");
    expect_named_failure(db, "SELECT sum(c7) FROM t",                  "B3 parallel agg");
    expect_named_failure(db, "SELECT c7, count(*) FROM t GROUP BY c7", "B4 parallel GROUP BY");
    expect_named_failure(db, "SELECT count(c7) FROM t SAMPLE BY 1h",   "B5 SAMPLE BY");
    expect_named_failure(db, "SELECT DISTINCT c7 FROM t",              "B6 DISTINCT");
    expect_named_failure(db, "SELECT c7 FROM t LATEST ON ts PARTITION BY c0",
                                                                       "B7 LATEST ON");
    expect_named_failure(db, "SELECT c0 FROM t WHERE c7 > 0",          "B8 WHERE on short col");

    /* The hole is a BLOCK, not the partition: a range inside the damaged
     * partition but past the hole must still answer in full. */
    printf("\n[B11] the refusal is block-scoped, not partition-scoped\n");
    {
        char w[160];
        snprintf(w, sizeof w, "ts > %lld AND ts < %lld",
                 (long long)hole_ts_max(), (long long)ts_of(DAMAGED_DAY + 1, 0));
        long long rows = 0, wrong = 0;
        int rc = q_verify_col(db, SHORT_COL, w, &rows, &wrong);
        CHECK(rc == TSDB_OK && rows == PER_DAY - 2 * BP && wrong == 0,
              "B11 the damaged partition still serves the short column past the hole");
    }
    {   /* and the hole itself is refused, not zero-filled */
        char sql[192];
        snprintf(sql, sizeof sql,
                 "SELECT c7 FROM t WHERE ts >= %lld AND ts <= %lld",
                 (long long)hole_ts_min(), (long long)hole_ts_max());
        qres_t q = q_run(db, sql);
        CHECK(q.rc != TSDB_OK && q.rows == 0,
              "B12 a range that is exactly the hole is refused, never zero-filled");
    }

    /* ==== CONTRACT C — the stats fast path must not disagree with the scan ==== */
    printf("\n[C] one engine, one answer: the stats fast path obeys the same rule\n");

    /* The seventh copy.  agg_stats_requires() asks for no stats bits for
     * COUNT, so the fast path's `bits == 0` exit skipped the gate that
     * incidentally rejected the hole for every other aggregate, and
     * agg_apply_stats() then added the TS block's row count for a block c7
     * does not have.  rc=0, count identical to an intact table. */
    expect_named_failure(db, "SELECT count(c7) FROM t", "C1 count(short col)");

    /* Declining the fast path must cost a fast path, not availability. */
    {
        int ok = 0;
        int64_t n = q_scalar(db, "SELECT count(*) FROM t", &ok);
        CHECK(ok && n == NROWS, "C2 count(*) still answers off the fast path");
    }
    {
        int ok = 0;
        int64_t n = q_scalar(db, "SELECT count(c0) FROM t", &ok);
        CHECK(ok && n == NROWS, "C3 count() of a HEALTHY column is unaffected");
    }
    {
        int ok = 0;
        int64_t lo = q_scalar(db, "SELECT min(c0) FROM t", &ok);
        int ok2 = 0;
        int64_t hi = q_scalar(db, "SELECT max(c0) FROM t", &ok2);
        CHECK(ok && ok2 && lo == cell_of(0, 0) && hi == cell_of(0, PER_DAY - 1),
              "C4 min/max over a healthy column stay exact");
    }
    {   /* block-scoped, not table-scoped: a range that misses the hole counts. */
        char sql[192];
        snprintf(sql, sizeof sql, "SELECT count(c7) FROM t WHERE ts < %lld",
                 (long long)ts_of(DAMAGED_DAY, 0));
        int ok = 0;
        int64_t n = q_scalar(db, sql, &ok);
        CHECK(ok && n == PER_DAY, "C5 count(short col) still answers outside the hole");
    }

    /* ==== CONTRACT D — a bloom skip must never delete rows ==== */
    printf("\n[D] the bloom skip cannot turn 'unreadable' into 'absent'\n");
    /* The bloom pre-filter lives on the SERIAL row path (exec_select), so these
     * are row-returning selects, not aggregates — an aggregate never reaches
     * bloom_can_skip_block and would not exercise this at all. */
    {   /* 'hot' exists ONLY in the block sym lost.  Blocks 0/2/3 are legitimately
         * bloom-skipped; block 1 must NOT be, or "unreadable" silently becomes
         * "no such rows" at rc=0. */
        qres_t q = q_run(db, "SELECT ts, v FROM t2 WHERE sym = '" D_HOT "'");
        CHECK(q.rc != TSDB_OK && q.rows == 0,
              "D1 a symbol living only in the hole is not reported absent");
        CHECK(strstr(q.err, "'sym'") != NULL, "D1 the failure names the short symbol column");
    }
    {   /* 'cold' is in blocks 0/2/3, which answer; block 1 cannot be evaluated.
         * A skip there would return 3072 of 4096 matching rows at rc=0. */
        qres_t q = q_run(db, "SELECT ts, v FROM t2 WHERE sym = '" D_COLD "'");
        CHECK(q.rc != TSDB_OK && q.rows == 0,
              "D2 a predicate it cannot evaluate over the hole never returns a partial match set");
    }
    {   /* and the blast radius is still one column. */
        int ok = 0;
        int64_t n = q_scalar(db, "SELECT count(*) FROM t2", &ok);
        CHECK(ok && n == PER_DAY, "D3 count(*) on the symbol-damaged table is complete");
        qres_t q = q_run(db, "SELECT ts, v FROM t2");
        CHECK(q.rc == TSDB_OK && q.rows == PER_DAY,
              "D4 the healthy columns of the symbol-damaged table read in full");
    }

    /* ==== CONTRACT E — "old blocks stay readable" survives the new check ==== */
    printf("\n[E] a REAL block carrying on-disk flag bit 4 is not mistaken for a hole\n");
    {   /* c19 block 0 of the day-0 partition now has bit 4 set on disk and a
         * valid CRC.  The in-memory HOLE marker is bit 4 of the SAME u16, and
         * tsdb_part_open back-fills on-disk header flags into meta.flags — so a
         * reader testing the flag alone rejects a perfectly good block.  The
         * offset==UINT64_MAX co-condition is what keeps this readable, and it
         * is the gate tsdb_part_read_block itself uses. */
        long long rows = 0, wrong = 0;
        int rc = q_verify_col(db, NCOL - 1, NULL, &rows, &wrong);
        CHECK(rc == TSDB_OK && rows == NROWS && wrong == 0,
              "E1 a real block with on-disk bit 4 still reads, every value correct");
    }
    {
        int ok = 0;
        int64_t n = q_scalar(db, "SELECT count(*) FROM t", &ok);
        CHECK(ok && n == NROWS, "E2 count(*) unaffected by the on-disk flag bit");
    }
    {   /* and it must not become a hole on the stats fast path either. */
        int ok = 0;
        char sql[64]; snprintf(sql, sizeof sql, "SELECT min(c%d) FROM t", NCOL - 1);
        int64_t lo = q_scalar(db, sql, &ok);
        CHECK(ok && lo == cell_of(NCOL - 1, 0),
              "E3 the stats fast path serves it too, exactly");
    }

    tsdb_close(db);
    rmrf(dir);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
