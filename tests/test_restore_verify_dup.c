/* test_restore_verify_dup.c — the two ways tsdb_restore_verify still certified
 * a database that had lost a block.
 *
 * [D1] (ts_min, count) IS NOT A UNIQUE BLOCK IDENTITY.
 *
 *      Equal timestamps are accepted, kept in insertion order, and a flush
 *      splits on block_points, so one partition can hold two blocks with an
 *      identical (ts_min, ts_max, count) — the same root cause the reader's own
 *      pairing has been bitten by.  rst_deep_check_block paired a stream record
 *      to a target block by UNCONSUMED first-match on that pair, so when one of
 *      the two value blocks was lost the single survivor satisfied BOTH stream
 *      records: identical payload bytes, so both compared equal, and DEEP
 *      answered
 *
 *          blocks_checked=4 blocks_missing=0 blocks_mismatched=0
 *          blocks_unresolved=0   rc=TSDB_OK
 *
 *      — a positively clean, FULLY RESOLVED containment answer for a target
 *      whose value column reads back half fabricated zeros with rc=0.  It also
 *      disarms the guard that was supposed to catch this class: the
 *      "blocks_checked == 0 => TSDB_ERR_INDETERMINATE" invariant cannot fire
 *      against a count of 4.
 *
 *      The control inside the case: the TS column kept BOTH of its blocks, and
 *      they are byte-identical too.  A check that consumes matches wrongly
 *      would report those as missing as well, so the case pins missing == 1 —
 *      exactly the one block that is gone.
 *
 * [D2] THE HOLE AXIS IS THROWN AWAY UNDER AHEAD.  The block slots the reader
 *      itself refuses (TSDB_BLOCK_FLAG_HOLE, counted in tsdb_mig_stats_t.holes)
 *      were only consulted in the "fewer blocks than the set" arm.  A target
 *      that kept ingesting — the live cluster node this call exists for — has
 *      MORE blocks than the set, so no deficit can form, and the index level
 *      answered TSDB_OK for the SAME whole-column loss test_restore_verify_hole
 *      [H1] catches on a quiesced node.  [H1]'s "must NOT verify clean at
 *      EITHER level" held only because its target was not AHEAD.
 *
 * [D3] ...and the direction that must NOT be broken closing [D2].  A set taken
 *      from an already-torn SOURCE carries the tear; a copy reproducing it
 *      faithfully holds unreadable slots the set never carried a block for, and
 *      one more write puts it in the very same AHEAD-with-holes shape.  That is
 *      CONTAINMENT, not loss: it must not be called CORRUPT, and the DEEP level
 *      — which can ask the question per block — must certify it.
 *
 * [D4] A BYTE ROT ON A PARTITION THE COMPACTOR NEVER TOUCHED.  `reenc_shape` is
 *      a TABLE-wide predicate, and a MISMATCH used to be counted unresolved
 *      whenever it was set — so one merged partition anywhere in the table
 *      silenced byte-level corruption everywhere else in it, with rc=TSDB_OK.
 *      A block whose bytes the reader refuses is not what a re-encode produces:
 *      whatever rewrote it would have CRC-stamped what it encoded.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define DAY_A   1700000000000000000LL
#define NS_DAY  86400000000000LL
#define DAY_B   (DAY_A + NS_DAY)
#define STEP_NS 1000000LL

/* ---- fs helpers ---------------------------------------------------------- */

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

/* Partition directory names under <dir>/t, ascending.  Returns how many. */
static int part_names(const char *dir, char (*out)[32], int max) {
    char tbl[4096];
    snprintf(tbl, sizeof(tbl), "%s/t", dir);
    DIR *d = opendir(tbl);
    if (!d) return 0;
    struct dirent *e;
    int n = 0;
    while ((e = readdir(d)) && n < max) {
        size_t k = strlen(e->d_name);
        if (k != 8 && k != 10) continue;
        int all = 1;
        for (size_t i = 0; i < k; i++)
            if (e->d_name[i] < '0' || e->d_name[i] > '9') all = 0;
        if (all) snprintf(out[n++], 32, "%s", e->d_name);
    }
    closedir(d);
    for (int i = 1; i < n; i++) {
        char k[32]; snprintf(k, sizeof(k), "%s", out[i]);
        int j = i - 1;
        while (j >= 0 && strcmp(out[j], k) > 0) { snprintf(out[j + 1], 32, "%s", out[j]); j--; }
        snprintf(out[j + 1], 32, "%s", k);
    }
    return n;
}

/* The shape a lost replication group / interrupted import / half-deleted
 * partition leaves behind: the column's index AND its data, both gone. */
static void drop_column_files(const char *dir, const char *part, const char *col) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/t/%s/%s.idx", dir, part, col);
    if (unlink(p) != 0) FAIL("could not unlink %s: %s", p, strerror(errno));
    snprintf(p, sizeof(p), "%s/t/%s/%s.col", dir, part, col);
    if (unlink(p) != 0) FAIL("could not unlink %s: %s", p, strerror(errno));
}

/* ---- raw idx access ------------------------------------------------------ */

static uint32_t rd_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static int64_t rd_i64(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | p[i];
    return (int64_t)v;
}

/* Read every entry of <part>/<col>.idx into `out`.  Returns the entry count. */
static int idx_entries(const char *dir, const char *part, const char *col,
                       uint8_t **out, uint32_t *out_esz, int *out_hsz)
{
    char path[4300];
    snprintf(path, sizeof(path), "%s/t/%s/%s.idx", dir, part, col);
    uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(path, NULL, &cnt, &esz, NULL, NULL, NULL, NULL);
    if (hsz <= 0 || cnt == 0 || esz == 0) FAIL("cannot probe %s (hsz=%d)", path, hsz);

    uint8_t *buf = (uint8_t *)malloc((size_t)cnt * esz);
    if (!buf) FAIL("oom");
    FILE *f = fopen(path, "rb");
    if (!f) FAIL("cannot open %s", path);
    if (fseek(f, hsz, SEEK_SET) != 0 ||
        fread(buf, 1, (size_t)cnt * esz, f) != (size_t)cnt * esz)
        FAIL("short read of %s", path);
    fclose(f);

    *out = buf; *out_esz = esz; *out_hsz = hsz;
    return (int)cnt;
}

/* Drop all but ONE entry from <part>/<col>.idx, header included, so the file is
 * internally consistent: the loss a half-landed replication group leaves, on a
 * partition whose OTHER blocks are intact.  The .col bytes are left alone —
 * only the index entry that reached them is gone. */
static void idx_keep_one(const char *dir, const char *part, const char *col,
                         int keep)
{
    uint8_t *ent = NULL; uint32_t esz = 0; int hsz = 0;
    int n = idx_entries(dir, part, col, &ent, &esz, &hsz);
    if (keep < 0 || keep >= n) FAIL("keep=%d out of range (n=%d)", keep, n);

    char path[4300];
    snprintf(path, sizeof(path), "%s/t/%s/%s.idx", dir, part, col);
    uint16_t ncols = tsdb_part_idx_ncols(path);
    uint64_t mseq = 0;
    (void)tsdb_part_idx_probe(path, NULL, NULL, NULL, NULL, NULL, NULL, &mseq);

    const uint8_t *e = ent + (size_t)keep * esz;
    uint32_t count  = rd_u32(e + 12);
    int64_t  ts_min = rd_i64(e + 16);
    int64_t  ts_max = rd_i64(e + 24);

    uint8_t hdr[TSDB_IDX_HEADER_SIZE];
    memset(hdr, 0, sizeof(hdr));
    size_t nh = tsdb_part_write_idx_header(hdr, 1, count, ts_min, ts_max,
                                           mseq, ncols);

    FILE *f = fopen(path, "wb");
    if (!f) FAIL("cannot rewrite %s", path);
    if (fwrite(hdr, 1, nh, f) != nh) FAIL("short header write to %s", path);
    if (fwrite(e, 1, esz, f) != esz)  FAIL("short entry write to %s", path);
    fclose(f);
}

/* ---- db helpers ---------------------------------------------------------- */

static tsdb_db_t *fresh_db(const char *dir, tsdb_table_t **out_t) {
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },   /* trailing column: rule 1 is vacuous */
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    OK(tsdb_open_table(db, "t", out_t));
    return db;
}

/* `konst` != 0 writes the SAME value into every row, so two chunks carrying the
 * same timestamps encode to byte-identical blocks. */
static void write_chunk(tsdb_table_t *t, int64_t base, int from, int n,
                        int konst) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = from; i < from + n; i++) {
        OK(tsdb_batch_row_ts(b, base + (int64_t)i * STEP_NS));
        OK(tsdb_batch_row_f64(b, 1, konst ? 42.0 : (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

static void backup_to(tsdb_db_t *db, const char *bk) {
    rm_rf(bk);
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_backup_create(db, bk, &rep);
    if (rc != TSDB_OK || !rep.complete)
        FAIL("backup rc=%d (%s) complete=%d", rc, tsdb_errstr(rc), rep.complete);
    tsdb_restore_report_free(&rep);
}

/* The engine's own verdict on the column verify is being asked about. */
static int read_val(tsdb_db_t *db, int64_t *out_rows, double *out_sum) {
    tsdb_result_t *r = NULL;
    int64_t rows = 0; double sum = 0;
    int rc = tsdb_query(db, "SELECT ts, val FROM t", &r);
    if (rc == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) { rows++; sum += tsdb_result_f64(r, 1); }
    }
    if (r) tsdb_result_free(r);
    if (out_rows) *out_rows = rows;
    if (out_sum)  *out_sum  = sum;
    return rc;
}

static const tsdb_restore_verify_table_t *row_t(const tsdb_restore_verify_t *v,
                                                const char *name) {
    for (int i = 0; i < v->ntables; i++)
        if (!strcmp(v->tables[i].table, name)) return &v->tables[i];
    return NULL;
}

static void print_row(const char *tag, int rc, const tsdb_restore_verify_t *v,
                      const char *name) {
    const tsdb_restore_verify_table_t *r = row_t(v, name);
    printf("%s rc=%d (%s) nfailed=%d", tag, rc, tsdb_errstr(rc), v->nfailed);
    if (r)
        printf("  %s: rc=%d rel=%s rows=%llu/%llu checked=%llu missing=%llu "
               "mismatched=%llu unresolved=%llu",
               name, r->rc, tsdb_restore_target_rel_name(r->target_rel),
               (unsigned long long)r->rows_target,
               (unsigned long long)r->rows_backup,
               (unsigned long long)r->blocks_checked,
               (unsigned long long)r->blocks_missing,
               (unsigned long long)r->blocks_mismatched,
               (unsigned long long)r->blocks_unresolved);
    printf("\n");
}

/* ==========================================================================
 * [D1] two blocks with one key, one of them lost
 * ======================================================================== */
#define D1_DIR "/tmp/tsdb_vdup_d1"
#define D1_BK  "/tmp/tsdb_vdup_d1_bk"
#define D1_N   64

static void case_duplicate_key(void) {
    printf("\n[D1] one partition, two blocks with the same (ts_min, count)\n");

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(D1_DIR, &t);

    /* The same D1_N timestamps written twice, each flushed on its own: two
     * blocks per column, same ts_min, same ts_max, same count.  A re-sent
     * replication group and a WAL replayed over rows already on disk both
     * produce it; so does a single flush of >block_points rows that all carry
     * one timestamp. */
    write_chunk(t, DAY_A, 0, D1_N, 1);
    OK(tsdb_table_flush(db, "t"));
    write_chunk(t, DAY_A, 0, D1_N, 1);
    OK(tsdb_table_flush(db, "t"));

    backup_to(db, D1_BK);
    tsdb_close(db);

    char parts[8][32];
    int np = part_names(D1_DIR, parts, 8);
    if (np != 1) FAIL("[D1] expected 1 partition, found %d", np);

    /* The case is worthless unless the duplicate key really is there. */
    for (int c = 0; c < 2; c++) {
        const char *col = c ? "val" : "ts";
        uint8_t *ent = NULL; uint32_t esz = 0; int hsz = 0;
        int n = idx_entries(D1_DIR, parts[0], col, &ent, &esz, &hsz);
        printf("[D1] %s.idx: %d entr%s", col, n, n == 1 ? "y" : "ies");
        for (int i = 0; i < n; i++)
            printf("  [%d](ts_min=%lld count=%u)", i,
                   (long long)rd_i64(ent + (size_t)i * esz + 16),
                   rd_u32(ent + (size_t)i * esz + 12));
        printf("\n");
        if (n != 2 ||
            rd_i64(ent + 16) != rd_i64(ent + esz + 16) ||
            rd_u32(ent + 12) != rd_u32(ent + esz + 12))
            FAIL("[D1] column %s did not come out as two blocks with one key "
                 "(n=%d) — the case is not testing what it claims", col, n);
        free(ent);
    }

    /* Lose ONE of the two value blocks.  ts keeps both, so the partition's
     * layout is intact at that position and the loss is unambiguous. */
    idx_keep_one(D1_DIR, parts[0], "val", 0);

    OK(tsdb_open(D1_DIR, &db));
    int64_t got = 0; double sum = 0;
    int qrc = read_val(db, &got, &sum);
    printf("[D1] engine: SELECT ts, val -> rc=%d (%s) rows=%lld sum=%.1f "
           "(intact sum would be %.1f)\n",
           qrc, tsdb_errstr(qrc), (long long)got, sum, 42.0 * 2 * D1_N);

    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int rc = tsdb_restore_verify(db, D1_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    print_row("[D1] deep        ", rc, &v, "t");
    const tsdb_restore_verify_table_t *r = row_t(&v, "t");
    if (!r) FAIL("[D1] the deep verify did not report table 't'");

    if (r->blocks_checked != 4)
        FAIL("[D1] the deep verify looked at %llu block(s), want 4 — the set "
             "carries two per column and the case is not testing what it claims",
             (unsigned long long)r->blocks_checked);
    if (r->rc == TSDB_OK)
        FAIL("[D1] the deep verify CERTIFIED a target that holds ONE of the two "
             "value blocks the set carries: rc=%d checked=%llu missing=%llu "
             "mismatched=%llu unresolved=%llu.  Both stream records matched the "
             "same surviving block — (ts_min, count) is not a unique block "
             "identity, so a first-match that is never consumed answers twice "
             "for one block", r->rc,
             (unsigned long long)r->blocks_checked,
             (unsigned long long)r->blocks_missing,
             (unsigned long long)r->blocks_mismatched,
             (unsigned long long)r->blocks_unresolved);
    /* And it must name EXACTLY the block that is gone.  The ts column kept both
     * of ITS byte-identical blocks: a check that consumes matches wrongly would
     * report those as missing too. */
    if (r->blocks_missing != 1)
        FAIL("[D1] the deep verify reported missing=%llu, want 1 — one value "
             "block was lost and the ts column kept both of its own "
             "(mismatched=%llu unresolved=%llu)",
             (unsigned long long)r->blocks_missing,
             (unsigned long long)r->blocks_mismatched,
             (unsigned long long)r->blocks_unresolved);
    if (rc == TSDB_OK)
        FAIL("[D1] the call returned TSDB_OK while the table row failed");
    tsdb_restore_verify_free(&v);

    tsdb_close(db);
    rm_rf(D1_DIR); rm_rf(D1_BK);
    printf("[D1] the second record no longer claims the first record's block\n");
}

/* ==========================================================================
 * [D2] a target that kept ingesting AND lost a column's blocks
 * ======================================================================== */
#define D2_DIR "/tmp/tsdb_vdup_d2"
#define D2_BK  "/tmp/tsdb_vdup_d2_bk"
#define D2_ROWS 100

static void case_ahead_with_holes(void) {
    printf("\n[D2] AHEAD, and its own block slots are unreadable\n");

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(D2_DIR, &t);
    for (int k = 0; k < 3; k++) {
        write_chunk(t, DAY_A, k * D2_ROWS, D2_ROWS, 0);
        OK(tsdb_table_flush(db, "t"));
    }
    backup_to(db, D2_BK);
    tsdb_close(db);

    char parts[8][32];
    int np = part_names(D2_DIR, parts, 8);
    if (np != 1) FAIL("[D2] expected 1 partition, found %d", np);
    drop_column_files(D2_DIR, parts[0], "val");

    /* Now keep ingesting, into a partition the set never saw — the live node
     * a moment after the backup.  The target ends up holding MORE rows and
     * MORE blocks than the set, which is what buries the tear. */
    OK(tsdb_open(D2_DIR, &db));
    OK(tsdb_open_table(db, "t", &t));
    for (int k = 0; k < 5; k++) {
        write_chunk(t, DAY_B, k * D2_ROWS, D2_ROWS, 0);
        OK(tsdb_table_flush(db, "t"));
    }

    int64_t got = 0;
    int qrc = read_val(db, &got, NULL);
    printf("[D2] engine: SELECT ts, val -> rc=%d (%s) rows=%lld\n",
           qrc, tsdb_errstr(qrc), (long long)got);
    if (qrc == TSDB_OK)
        FAIL("[D2] the engine still answers SELECT ts, val after the column's "
             "blocks were removed — the tear did not take, so there is nothing "
             "for verify to be wrong about");

    for (int deep = 0; deep <= 1; deep++) {
        tsdb_restore_verify_t v;
        memset(&v, 0, sizeof(v));
        int rc = tsdb_restore_verify(db, D2_BK,
                                     deep ? TSDB_RESTORE_VERIFY_DEEP
                                          : TSDB_RESTORE_VERIFY_INDEX, &v);
        print_row(deep ? "[D2] deep        " : "[D2] index       ", rc, &v, "t");
        const tsdb_restore_verify_table_t *r = row_t(&v, "t");
        if (!r) FAIL("[D2] the %s verify did not report table 't'",
                     deep ? "deep" : "index");
        if (r->target_rel != TSDB_RESTORE_TARGET_AHEAD)
            FAIL("[D2] the %s verify reported rel=%s, want ahead — the target "
                 "must be holding MORE than the set or this case is not the "
                 "one it claims", deep ? "deep" : "index",
                 tsdb_restore_target_rel_name(r->target_rel));
        if (rc == TSDB_OK || r->rc == TSDB_OK)
            FAIL("[D2] the %s verify CERTIFIED a database whose SELECT ts, val "
                 "returns %s: verify rc=%d table rc=%d checked=%llu "
                 "missing=%llu.  Its unreadable block slots were measured "
                 "(tsdb_mig_stats_t.holes) and thrown away because a target "
                 "that kept ingesting can never show a block DEFICIT",
                 deep ? "deep" : "index", tsdb_errstr(qrc), rc, r->rc,
                 (unsigned long long)r->blocks_checked,
                 (unsigned long long)r->blocks_missing);
        if (deep && r->blocks_missing == 0)
            FAIL("[D2] the deep verify failed without naming a single missing "
                 "block — an operator cannot act on a bare rc");
        tsdb_restore_verify_free(&v);
    }

    tsdb_close(db);
    rm_rf(D2_DIR); rm_rf(D2_BK);
    printf("[D2] an AHEAD target with unreadable slots is not certified\n");
}

/* ==========================================================================
 * [D3] the false-alarm control for [D2]: the SET carries the same tear
 * ======================================================================== */
#define D3_DIR "/tmp/tsdb_vdup_d3"
#define D3_BK  "/tmp/tsdb_vdup_d3_bk"

static void case_ahead_with_the_sets_own_tear(void) {
    printf("\n[D3] AHEAD, with holes the SET itself carries\n");

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(D3_DIR, &t);
    for (int k = 0; k < 3; k++) {
        write_chunk(t, DAY_A, k * D2_ROWS, D2_ROWS, 0);
        OK(tsdb_table_flush(db, "t"));
    }
    tsdb_close(db);

    /* Tear FIRST, back up AFTER: the set is taken from an already-torn source,
     * so it carries no block for the slots that are unreadable. */
    char parts[8][32];
    int np = part_names(D3_DIR, parts, 8);
    if (np != 1) FAIL("[D3] expected 1 partition, found %d", np);
    drop_column_files(D3_DIR, parts[0], "val");

    OK(tsdb_open(D3_DIR, &db));
    int64_t got = 0;
    int qrc = read_val(db, &got, NULL);
    printf("[D3] engine: SELECT ts, val -> rc=%d (%s) (the SOURCE is torn)\n",
           qrc, tsdb_errstr(qrc));
    if (qrc == TSDB_OK)
        FAIL("[D3] the source is not actually torn — the control proves nothing");

    backup_to(db, D3_BK);

    /* ...and it keeps ingesting, exactly like [D2]. */
    OK(tsdb_open_table(db, "t", &t));
    for (int k = 0; k < 5; k++) {
        write_chunk(t, DAY_B, k * D2_ROWS, D2_ROWS, 0);
        OK(tsdb_table_flush(db, "t"));
    }

    for (int deep = 0; deep <= 1; deep++) {
        tsdb_restore_verify_t v;
        memset(&v, 0, sizeof(v));
        int rc = tsdb_restore_verify(db, D3_BK,
                                     deep ? TSDB_RESTORE_VERIFY_DEEP
                                          : TSDB_RESTORE_VERIFY_INDEX, &v);
        print_row(deep ? "[D3] deep        " : "[D3] index       ", rc, &v, "t");
        const tsdb_restore_verify_table_t *r = row_t(&v, "t");
        if (!r) FAIL("[D3] the %s verify did not report table 't'",
                     deep ? "deep" : "index");
        if (r->rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_CORRUPT)
            FAIL("[D3] the %s verify called a FAITHFUL copy of a torn source "
                 "corrupt (rc=%d table rc=%d missing=%llu mismatched=%llu). "
                 " The set carries no block for those slots; reproducing the "
                 "tear is containment, not loss",
                 deep ? "deep" : "index", rc, r->rc,
                 (unsigned long long)r->blocks_missing,
                 (unsigned long long)r->blocks_mismatched);
        if (deep && (rc != TSDB_OK || r->rc != TSDB_OK))
            FAIL("[D3] the DEEP verify — which looks every block of the set up "
                 "one at a time and found all %llu of them — still did not "
                 "certify the copy: rc=%d table rc=%d missing=%llu "
                 "unresolved=%llu", (unsigned long long)r->blocks_checked,
                 rc, r->rc, (unsigned long long)r->blocks_missing,
                 (unsigned long long)r->blocks_unresolved);
        tsdb_restore_verify_free(&v);
    }

    tsdb_close(db);
    rm_rf(D3_DIR); rm_rf(D3_BK);
    printf("[D3] the set's own tear, reproduced and out-grown, is not loss\n");
}

/* ==========================================================================
 * [D4] a byte rot beside a compacted partition
 * ======================================================================== */
#define D4_DIR "/tmp/tsdb_vdup_d4"
#define D4_BK  "/tmp/tsdb_vdup_d4_bk"

/* One commit is one block per column, and the compactor only rewrites a column
 * carrying at least COMPACT_THRESHOLD_DEFAULT (16) of them. */
#define D4_CHUNKS 20
#define D4_ROWS   100

/* The database's OWN compactor, stock options, driven synchronously.  Only
 * `part` is aged, so every other partition stays "hot" and is left alone. */
static void compact_one(tsdb_db_t *db, const char *dir, const char *part) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/t/%s", dir, part);
    struct timeval tv[2];
    tv[0].tv_sec = time(NULL) - 3600; tv[0].tv_usec = 0;
    tv[1] = tv[0];
    if (utimes(p, tv) != 0) FAIL("utimes(%s): %s", p, strerror(errno));

    tsdb_compactor_opts_t co;
    memset(&co, 0, sizeof(co));
    co.worker_threads = -1;                 /* manual: no background thread */
    tsdb_compactor_t *c = NULL;
    OK(tsdb_compactor_start(db, &co, &c));
    OK(tsdb_compactor_run_once(c));
    tsdb_compactor_stats_t cs;
    tsdb_compactor_stats(c, &cs);
    tsdb_compactor_stop(c);
    printf("  compactor: cols_rewritten=%llu parts_merged=%llu\n",
           (unsigned long long)cs.compactions_done,
           (unsigned long long)cs.parts_merged);
    if (cs.compactions_done == 0)
        FAIL("the compactor rewrote nothing — the case would be identical to an "
             "un-compacted one and prove nothing");
}

/* Flip one byte inside the first block's PAYLOAD.  Size and index are
 * untouched, so the block is still present under its key — it is the bytes
 * that are wrong, which is what disk rot and a half-written page look like. */
static void rot_one_byte(const char *dir, const char *part, const char *col) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/t/%s/%s.col", dir, part, col);
    FILE *f = fopen(p, "r+b");
    if (!f) FAIL("cannot open %s: %s", p, strerror(errno));
    struct stat st;
    if (stat(p, &st) != 0 || st.st_size <= (off_t)TSDB_BLOCK_HEADER_SIZE + 8)
        FAIL("%s is too small to rot (%lld bytes)", p, (long long)st.st_size);
    off_t at = (off_t)TSDB_BLOCK_HEADER_SIZE +
               (st.st_size - (off_t)TSDB_BLOCK_HEADER_SIZE) / 2;
    unsigned char b = 0;
    if (fseeko(f, at, SEEK_SET) != 0 || fread(&b, 1, 1, f) != 1)
        FAIL("cannot read %s at %lld", p, (long long)at);
    b = (unsigned char)(b ^ 0xFF);
    if (fseeko(f, at, SEEK_SET) != 0 || fwrite(&b, 1, 1, f) != 1)
        FAIL("cannot write %s at %lld", p, (long long)at);
    fclose(f);
    printf("     flipped byte %lld of %s\n", (long long)at, p);
}

static void case_rot_beside_a_merge(void) {
    printf("\n[D4] a byte rot on a partition the compactor never touched\n");

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(D4_DIR, &t);
    for (int k = 0; k < D4_CHUNKS; k++) {
        write_chunk(t, DAY_A, k * D4_ROWS, D4_ROWS, 0);
        OK(tsdb_table_flush(db, "t"));      /* one block per column, per chunk */
    }
    write_chunk(t, DAY_B, 0, D4_ROWS, 0);   /* a second, single-block partition */
    OK(tsdb_table_flush(db, "t"));
    backup_to(db, D4_BK);

    char parts[8][32];
    int np = part_names(D4_DIR, parts, 8);
    if (np != 2) FAIL("[D4] expected 2 partitions, found %d", np);
    compact_one(db, D4_DIR, parts[0]);
    tsdb_close(db);

    /* The OTHER partition — the one nothing re-encoded — takes the damage. */
    rot_one_byte(D4_DIR, parts[1], "val");

    OK(tsdb_open(D4_DIR, &db));
    int64_t got = 0;
    int qrc = read_val(db, &got, NULL);
    printf("[D4] engine: SELECT ts, val -> rc=%d (%s) rows=%lld\n",
           qrc, tsdb_errstr(qrc), (long long)got);
    if (qrc == TSDB_OK)
        FAIL("[D4] the engine still answers SELECT ts, val over the rotted "
             "block — the damage did not take");

    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int rc = tsdb_restore_verify(db, D4_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    print_row("[D4] deep        ", rc, &v, "t");
    const tsdb_restore_verify_table_t *r = row_t(&v, "t");
    if (!r) FAIL("[D4] the deep verify did not report table 't'");
    if (r->target_rel != TSDB_RESTORE_TARGET_REENCODED)
        FAIL("[D4] rel=%s, want reencoded — the compaction did not leave the "
             "table-wide shape whose swallow this case is about",
             tsdb_restore_target_rel_name(r->target_rel));
    if (rc == TSDB_OK || r->rc == TSDB_OK)
        FAIL("[D4] the deep verify CERTIFIED a database whose SELECT ts, val "
             "returns %s: rc=%d table rc=%d checked=%llu mismatched=%llu "
             "unresolved=%llu.  ONE merged partition made a table-wide "
             "`reenc_shape` true, and the rot on the partition beside it was "
             "counted 'unresolved' — a re-encode does not write bytes its own "
             "reader refuses", tsdb_errstr(qrc), rc, r->rc,
             (unsigned long long)r->blocks_checked,
             (unsigned long long)r->blocks_mismatched,
             (unsigned long long)r->blocks_unresolved);
    if (r->blocks_mismatched == 0)
        FAIL("[D4] the deep verify failed without naming the rotted block "
             "(mismatched=0 unresolved=%llu)",
             (unsigned long long)r->blocks_unresolved);
    /* And the merged partition's blocks must still be unresolved, not missing:
     * the [Y] false alarm this whole design exists to avoid. */
    if (r->blocks_missing != 0)
        FAIL("[D4] the deep verify reported missing=%llu against a merged "
             "partition that holds every row of the set — the compacted "
             "partition's blocks are re-encoded, not lost",
             (unsigned long long)r->blocks_missing);
    tsdb_restore_verify_free(&v);

    tsdb_close(db);
    rm_rf(D4_DIR); rm_rf(D4_BK);
    printf("[D4] the rot is named, and the merge is still not called a loss\n");
}

/* ==========================================================================
 * [D5] a rot whose ts key is gone too
 * ======================================================================== */
#define D5_DIR "/tmp/tsdb_vdup_d5"
#define D5_BK  "/tmp/tsdb_vdup_d5_bk"

static void copy_file(const char *from, const char *to) {
    FILE *a = fopen(from, "rb");
    if (!a) FAIL("cannot read %s: %s", from, strerror(errno));
    FILE *b = fopen(to, "wb");
    if (!b) FAIL("cannot write %s: %s", to, strerror(errno));
    char buf[65536]; size_t n;
    while ((n = fread(buf, 1, sizeof(buf), a)) > 0)
        if (fwrite(buf, 1, n, b) != n) FAIL("short write to %s", to);
    fclose(a); fclose(b);
}

static void col_pair_path(char *out, size_t cap, const char *dir,
                          const char *part, const char *col, const char *ext) {
    snprintf(out, cap, "%s/t/%s/%s.%s", dir, part, col, ext);
}

/* compact_partition rewrites ts first and derives every other column from it,
 * "swapping all columns or none".  A crash between the two swaps leaves the
 * shape below: ts carries the merged block, `val` still carries the old ones.
 * The stream's val blocks are then FOUND under their own keys while the ts key
 * that would attribute them is gone — so a rot in one of them is a MISMATCH
 * whose ts key does NOT resolve, the one shape the per-block question cannot
 * rule on.  It must still be reported: nothing that re-encodes blocks writes
 * bytes its own reader refuses. */
static void case_rot_with_no_ts_key(void) {
    printf("\n[D5] a rot in a block whose ts key is gone too\n");

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(D5_DIR, &t);
    for (int k = 0; k < D4_CHUNKS; k++) {
        write_chunk(t, DAY_A, k * D4_ROWS, D4_ROWS, 0);
        OK(tsdb_table_flush(db, "t"));
    }
    write_chunk(t, DAY_B, 0, D4_ROWS, 0);
    OK(tsdb_table_flush(db, "t"));
    backup_to(db, D5_BK);

    char parts[8][32];
    int np = part_names(D5_DIR, parts, 8);
    if (np != 2) FAIL("[D5] expected 2 partitions, found %d", np);

    char vidx[4300], vcol[4300];
    col_pair_path(vidx, sizeof(vidx), D5_DIR, parts[0], "val", "idx");
    col_pair_path(vcol, sizeof(vcol), D5_DIR, parts[0], "val", "col");
    copy_file(vidx, "/tmp/tsdb_vdup_d5_val.idx");
    copy_file(vcol, "/tmp/tsdb_vdup_d5_val.col");

    compact_one(db, D5_DIR, parts[0]);
    tsdb_close(db);

    /* The half of the swap that never landed. */
    copy_file("/tmp/tsdb_vdup_d5_val.idx", vidx);
    copy_file("/tmp/tsdb_vdup_d5_val.col", vcol);
    unlink("/tmp/tsdb_vdup_d5_val.idx");
    unlink("/tmp/tsdb_vdup_d5_val.col");

    rot_one_byte(D5_DIR, parts[0], "val");

    OK(tsdb_open(D5_DIR, &db));
    int64_t got = 0;
    int qrc = read_val(db, &got, NULL);
    printf("[D5] engine: SELECT ts, val -> rc=%d (%s) rows=%lld\n",
           qrc, tsdb_errstr(qrc), (long long)got);

    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int rc = tsdb_restore_verify(db, D5_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    print_row("[D5] deep        ", rc, &v, "t");
    const tsdb_restore_verify_table_t *r = row_t(&v, "t");
    if (!r) FAIL("[D5] the deep verify did not report table 't'");
    if (r->blocks_unresolved == 0)
        FAIL("[D5] nothing came back unresolved — the ts key is evidently still "
             "there, so this case is not the one it claims");
    if (rc == TSDB_OK || r->rc == TSDB_OK)
        FAIL("[D5] the deep verify CERTIFIED a target holding an UNREADABLE "
             "copy of one of the set's blocks: rc=%d table rc=%d checked=%llu "
             "mismatched=%llu unresolved=%llu.  Its ts key is gone, so the "
             "per-block question cannot attribute it — but a block the reader "
             "refuses is not something a re-encode produces, whatever the ts "
             "column says", rc, r->rc,
             (unsigned long long)r->blocks_checked,
             (unsigned long long)r->blocks_mismatched,
             (unsigned long long)r->blocks_unresolved);
    if (r->blocks_mismatched != 1)
        FAIL("[D5] mismatched=%llu, want 1 — exactly one block was rotted and "
             "the %d beside it are byte-identical",
             (unsigned long long)r->blocks_mismatched, D4_CHUNKS - 1);
    if (r->blocks_missing != 0)
        FAIL("[D5] missing=%llu against a target that still holds every block "
             "of the set", (unsigned long long)r->blocks_missing);
    tsdb_restore_verify_free(&v);

    tsdb_close(db);
    rm_rf(D5_DIR); rm_rf(D5_BK);
    printf("[D5] an unreadable block is named even with no ts key to ask\n");
}

int main(void) {
    printf("=== test_restore_verify_dup ===\n");
    case_duplicate_key();
    case_ahead_with_holes();
    case_ahead_with_the_sets_own_tear();
    case_rot_beside_a_merge();
    case_rot_with_no_ts_key();
    printf("\n=== test_restore_verify_dup PASSED ===\n");
    return 0;
}
