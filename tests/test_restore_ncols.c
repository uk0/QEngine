/* test_restore_ncols.c — a RESTORED database must keep the protection the
 * source had: a column whose blocks are gone must not read back as zeros.
 *
 * Shaped like tests/test_zerofill_absent_column.c, but every read is taken
 * against a data dir that tsdb_restore_run BUILT.
 *
 * THE BUG.  Every partition tsdb_restore_run creates is built by
 * tsdb_rawblock_apply, which passes tsdb_part_idx_ncols(idx_path) — the stamp
 * read back from the file it is republishing.  For a file that does not exist
 * yet that is TSDB_IDX_NCOLS_UNKNOWN, and correctly so: the raw-block applier
 * publishes ONE (column, block) per call and may not assert how many columns
 * the partition has (part.h).  Nothing downstream asserted it either —
 * rst_stamp_max_seq carried the UNKNOWN forward and only ran at all when the
 * .seq sidecar carried a non-zero checkpoint.  So every restored partition
 * came out unstamped, and rule 2 of part_col_absence_is_late_add was gone.
 * Rule 1 ("some later column has blocks") is VACUOUS for the last column, so
 * for a trailing column the restored database had no evidence left at all:
 *
 *     SOURCE    SELECT ts, val  ->  rc = TSDB_ERR_CORRUPT (-4)
 *     RESTORED  SELECT ts, val  ->  rc = TSDB_OK, sum(val) = 0
 *
 * The restore is being asked to reproduce a tear that is already on the
 * SOURCE, so no post-restore corruption is needed to reach it.
 *
 * Cases:
 *   [N1] a torn SOURCE (trailing column's .idx and .col gone) is reproduced
 *        faithfully: the restored copy refuses the same read the source
 *        refuses, instead of answering it with fabricated zeros.
 *   [N2] a HEALTHY restore keeps the protection for later: tear the restored
 *        copy afterwards and it errors, exactly as the source would have.
 *   [N3] availability guard — an ALTER-added column on the restored database
 *        still zero-fills its pre-existing rows.  The stamp must not turn a
 *        shipped feature into a permanent read error.
 *   [N4] the harder availability guard — a source partition that carries NO
 *        stamp (legacy / built by replication) plus a genuine late add.  The
 *        stream's header column count must NOT be stamped there: the stream
 *        carries no block for that column, so asserting it would make the
 *        restored copy STRICTER than the source and break the ALTER.
 *   [N5] ...but when the restore really did land every column of a partition,
 *        it may assert the count even though the source never did (part.h:
 *        "a writer that writes every column of the partition").  Tear the
 *        restored copy afterwards and it errors.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/part.h"

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

/* Explicit flush entry, declared the way test_zerofill_absent_column.c does. */
extern int tsdb_db_flush_all(tsdb_db_t *db);

#define NROWS    5000
#define BASE_TS  1000000000000LL
#define STEP_NS  1000000LL

/* floor(val_of(i)) == i, so a mis-paired cell is detectable as well as a
 * fabricated one, and the true sum is far from zero. */
static double val_of(int i) { return (double)i + (double)(i % 997) / 1000.0; }

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

/* <root>/t/<YYYYMMDD> */
static int find_part_dir(const char *root, char *out, size_t cap) {
    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/t", root);
    DIR *d = opendir(tdir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(out, cap, "%s/%s", tdir, e->d_name);
        found = 1;
        break;
    }
    closedir(d);
    return found;
}

/* The shape a partial restore / lost replication group / interrupted import
 * leaves behind: the column's index AND its data, both gone. */
static int drop_column_files(const char *part_dir, const char *col) {
    char p[4200];
    int ok = 1;
    snprintf(p, sizeof(p), "%s/%s.idx", part_dir, col);
    if (unlink(p) != 0) ok = 0;
    snprintf(p, sizeof(p), "%s/%s.col", part_dir, col);
    if (unlink(p) != 0) ok = 0;
    return ok;
}

/* Make every idx in this partition byte-identical to what a binary from before
 * the stamp existed produced. */
static int strip_ncols_stamp(const char *part_dir) {
    DIR *d = opendir(part_dir);
    if (!d) return 0;
    struct dirent *e;
    int n = 0;
    while ((e = readdir(d))) {
        size_t l = strlen(e->d_name);
        if (l < 5 || strcmp(e->d_name + l - 4, ".idx") != 0) continue;
        char p[4200];
        snprintf(p, sizeof(p), "%s/%s", part_dir, e->d_name);
        FILE *f = fopen(p, "r+b");
        if (!f) continue;
        uint8_t zero[2] = { 0, 0 };
        if (fseek(f, 10, SEEK_SET) == 0 && fwrite(zero, 1, 2, f) == 2) n++;
        fclose(f);
    }
    closedir(d);
    return n;
}

static uint16_t stamp_of(const char *part_dir, const char *col) {
    char p[4200];
    snprintf(p, sizeof(p), "%s/%s.idx", part_dir, col);
    return tsdb_part_idx_ncols(p);
}

/* ---- reads, all through the ENGINE --------------------------------------- */

/* SELECT ts, <vcol_name>.  *rows = rows returned, *sum = sum of the FLOAT64
 * result column 1, *wrong = cells whose value is not the one written. */
static void scan_f64(tsdb_db_t *db, const char *sql,
                     int64_t *rows, double *sum, int *wrong, int *out_rc)
{
    tsdb_result_t *r = NULL;
    *rows = 0; *sum = 0.0; *wrong = 0;
    int rc = tsdb_query(db, sql, &r);
    *out_rc = rc;
    if (rc != TSDB_OK || !r) { if (r) tsdb_result_free(r); return; }
    while (tsdb_result_next(r) > 0) {
        int64_t ts  = tsdb_result_ts(r, 0);
        int64_t idx = (ts - BASE_TS) / STEP_NS;
        double  v   = tsdb_result_f64(r, 1);
        *sum += v;
        if ((int64_t)v != idx) (*wrong)++;
        (*rows)++;
    }
    tsdb_result_free(r);
}

/* SELECT ts, w  (INT64) — the ALTER-added column cases. */
static void scan_i64(tsdb_db_t *db, const char *sql,
                     int64_t *rows, int64_t *sum, int *out_rc)
{
    tsdb_result_t *r = NULL;
    *rows = 0; *sum = 0;
    int rc = tsdb_query(db, sql, &r);
    *out_rc = rc;
    if (rc != TSDB_OK || !r) { if (r) tsdb_result_free(r); return; }
    while (tsdb_result_next(r) > 0) { *sum += tsdb_result_i64(r, 1); (*rows)++; }
    tsdb_result_free(r);
}

static int64_t q_count(tsdb_db_t *db) {
    tsdb_result_t *r = NULL; int64_t v = -1;
    if (tsdb_query(db, "SELECT count(*) FROM t", &r) == TSDB_OK && r) {
        if (tsdb_result_next(r) > 0) v = tsdb_result_i64(r, 0);
    }
    if (r) tsdb_result_free(r);
    return v;
}

/* ---- builders ------------------------------------------------------------ */

/* (ts, val) — val is the LAST column, which is the shape rule 1 cannot see. */
static void write_source(const char *dir) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < NROWS; i++) {
        OK(tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * STEP_NS));
        OK(tsdb_batch_row_f64(b, 1, val_of(i)));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));
    tsdb_close(db);
}

static void add_column_w(const char *dir) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_table_t *th = NULL;                 /* ALTER resolves through this */
    OK(tsdb_open_table(db, "t", &th));
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "ALTER TABLE t ADD COLUMN w INT64", &r);
    if (r) tsdb_result_free(r);
    OK(rc);
    tsdb_close(db);
}

static void backup_to(const char *src, const char *bk) {
    rm_rf(bk);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(src, &db));
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_backup_create(db, bk, &rep);
    if (rc != TSDB_OK || !rep.complete)
        FAIL("backup of %s rc=%d (%s) complete=%d", src, rc, tsdb_errstr(rc),
             rep.complete);
    tsdb_restore_report_free(&rep);
    tsdb_close(db);
}

static void restore_into(const char *bk, const char *dst) {
    rm_rf(dst);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dst, &db));
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_restore_run(db, bk, &rep);
    if (rc != TSDB_OK || !rep.complete)
        FAIL("restore into %s rc=%d (%s) complete=%d", dst, rc, tsdb_errstr(rc),
             rep.complete);
    tsdb_restore_report_free(&rep);
    tsdb_close(db);
}

/* ==========================================================================
 * [N1] a torn SOURCE is reproduced faithfully
 * ======================================================================== */
#define N1_SRC "/tmp/tsdb_rncols1_src"
#define N1_BK  "/tmp/tsdb_rncols1_bk"
#define N1_DST "/tmp/tsdb_rncols1_dst"

static void case_torn_source_reproduced(void) {
    printf("\n[N1] a torn trailing column on the SOURCE, backed up and restored\n");
    rm_rf(N1_SRC); rm_rf(N1_BK); rm_rf(N1_DST);
    write_source(N1_SRC);

    char part[4096];
    if (!find_part_dir(N1_SRC, part, sizeof(part))) FAIL("[N1] no source partition");
    if (stamp_of(part, "ts") != 2)
        FAIL("[N1] the source flush did not stamp ts.idx with 2 columns (got %u) — "
             "the case cannot test carrying a stamp that is not there",
             (unsigned)stamp_of(part, "ts"));
    if (!drop_column_files(part, "val"))
        FAIL("[N1] could not remove val.idx / val.col from the source");

    /* The SOURCE's answer — this is what the restored copy has to reproduce. */
    int src_rc = 0; int64_t src_rows = 0, src_cnt = 0; double src_sum = 0; int src_wrong = 0;
    {
        tsdb_db_t *db = NULL;
        OK(tsdb_open(N1_SRC, &db));
        scan_f64(db, "SELECT ts, val FROM t", &src_rows, &src_sum, &src_wrong, &src_rc);
        src_cnt = q_count(db);
        tsdb_close(db);
    }
    printf("[N1] SOURCE   rc=%d (%s) rows=%lld sum=%.1f count(*)=%lld\n",
           src_rc, tsdb_errstr(src_rc), (long long)src_rows, src_sum,
           (long long)src_cnt);
    if (src_rc == TSDB_OK)
        FAIL("[N1] the SOURCE answered a column whose blocks are gone — the "
             "fcacea4 protection is not working, so this case proves nothing");

    backup_to(N1_SRC, N1_BK);

    /* The backup opens and flushes every table on disk; make sure that did not
     * quietly rewrite the column we removed. */
    {
        tsdb_db_t *db = NULL; int rc = 0;
        int64_t rows = 0; double sum = 0; int wrong = 0;
        OK(tsdb_open(N1_SRC, &db));
        scan_f64(db, "SELECT ts, val FROM t", &rows, &sum, &wrong, &rc);
        tsdb_close(db);
        if (rc == TSDB_OK)
            FAIL("[N1] taking the backup repaired the source (rc=%d) — the set "
                 "no longer carries the tear this case is about", rc);
    }

    restore_into(N1_BK, N1_DST);

    char dpart[4096];
    if (!find_part_dir(N1_DST, dpart, sizeof(dpart))) FAIL("[N1] no restored partition");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(N1_DST, &db));
    int rc = 0; int64_t rows = 0, cnt = 0; double sum = 0; int wrong = 0;
    scan_f64(db, "SELECT ts, val FROM t", &rows, &sum, &wrong, &rc);
    cnt = q_count(db);
    printf("[N1] RESTORED rc=%d (%s) rows=%lld sum=%.1f count(*)=%lld "
           "ts.idx stamp=%u\n",
           rc, tsdb_errstr(rc), (long long)rows, sum, (long long)cnt,
           (unsigned)stamp_of(dpart, "ts"));

    if (cnt != src_cnt)
        FAIL("[N1] the restore lost rows: restored count(*)=%lld, source %lld",
             (long long)cnt, (long long)src_cnt);
    if (rc == TSDB_OK)
        FAIL("[N1] the RESTORED copy answered rc=0 with %lld row(s) and "
             "sum(val)=%.1f for a column whose blocks it never received, while "
             "the SOURCE it was copied from answered rc=%d (%s).  A restore "
             "that drops the ncols stamp turns a loud tear into fabricated "
             "zeros [core]",
             (long long)rows, sum, src_rc, tsdb_errstr(src_rc));
    if (wrong != 0)
        FAIL("[N1] the restored read handed back %d fabricated or mis-paired "
             "value cell(s)", wrong);

    /* ts alone still answers on both: the refusal is scoped to the lost column. */
    tsdb_result_t *r = NULL;
    int64_t trows = 0;
    if (tsdb_query(db, "SELECT ts FROM t", &r) == TSDB_OK && r)
        while (tsdb_result_next(r) > 0) trows++;
    if (r) tsdb_result_free(r);
    if (trows != NROWS)
        FAIL("[N1] SELECT ts on the restored copy returned %lld rows, want %d — "
             "the refusal must be scoped to the lost column",
             (long long)trows, NROWS);
    tsdb_close(db);
    printf("[N1] the restored copy refuses the same read the source refuses\n");
    rm_rf(N1_SRC); rm_rf(N1_BK); rm_rf(N1_DST);
}

/* ==========================================================================
 * [N2] a healthy restore keeps the protection for the NEXT loss
 * ======================================================================== */
#define N2_SRC "/tmp/tsdb_rncols2_src"
#define N2_BK  "/tmp/tsdb_rncols2_bk"
#define N2_DST "/tmp/tsdb_rncols2_dst"

static void case_restored_keeps_protection(void) {
    printf("\n[N2] a healthy restore, then the trailing column is lost on the copy\n");
    rm_rf(N2_SRC); rm_rf(N2_BK); rm_rf(N2_DST);
    write_source(N2_SRC);
    backup_to(N2_SRC, N2_BK);
    restore_into(N2_BK, N2_DST);

    char part[4096];
    if (!find_part_dir(N2_DST, part, sizeof(part))) FAIL("[N2] no restored partition");

    /* Baseline: the restored copy reads correctly before anything is removed. */
    {
        tsdb_db_t *db = NULL; int rc = 0;
        int64_t rows = 0; double sum = 0; int wrong = 0;
        OK(tsdb_open(N2_DST, &db));
        scan_f64(db, "SELECT ts, val FROM t", &rows, &sum, &wrong, &rc);
        tsdb_close(db);
        if (rc != TSDB_OK || rows != NROWS || wrong != 0)
            FAIL("[N2] the restore itself is wrong: rc=%d rows=%lld wrong=%d",
                 rc, (long long)rows, wrong);
    }

    if (!drop_column_files(part, "val"))
        FAIL("[N2] could not remove val.idx / val.col from the restored copy");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(N2_DST, &db));
    int rc = 0; int64_t rows = 0; double sum = 0; int wrong = 0;
    scan_f64(db, "SELECT ts, val FROM t", &rows, &sum, &wrong, &rc);
    printf("[N2] RESTORED-then-torn rc=%d (%s) rows=%lld sum=%.1f stamp=%u\n",
           rc, tsdb_errstr(rc), (long long)rows, sum,
           (unsigned)stamp_of(part, "ts"));
    if (rc == TSDB_OK)
        FAIL("[N2] a restored database answered rc=0 / sum=%.1f for a column "
             "whose blocks are gone.  The same loss on the SOURCE errors, so "
             "restoring a database permanently downgrades it [core]", sum);
    tsdb_close(db);
    printf("[N2] the restored database still refuses to fabricate\n");
    rm_rf(N2_SRC); rm_rf(N2_BK); rm_rf(N2_DST);
}

/* ==========================================================================
 * [N3] availability — ALTER ADD COLUMN on a RESTORED database still zero-fills
 * ======================================================================== */
#define N3_SRC "/tmp/tsdb_rncols3_src"
#define N3_BK  "/tmp/tsdb_rncols3_bk"
#define N3_DST "/tmp/tsdb_rncols3_dst"

static void case_alter_on_restored_still_fills(void) {
    printf("\n[N3] ALTER ADD COLUMN on the restored copy still reads zeros\n");
    rm_rf(N3_SRC); rm_rf(N3_BK); rm_rf(N3_DST);
    write_source(N3_SRC);
    backup_to(N3_SRC, N3_BK);
    restore_into(N3_BK, N3_DST);
    add_column_w(N3_DST);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(N3_DST, &db));
    int rc = 0; int64_t rows = 0, wsum = 0;
    scan_i64(db, "SELECT ts, w FROM t", &rows, &wsum, &rc);
    printf("[N3] rc=%d (%s) rows=%lld sum(w)=%lld\n",
           rc, tsdb_errstr(rc), (long long)rows, (long long)wsum);
    if (rc != TSDB_OK || rows != NROWS)
        FAIL("[N3] a genuinely late-added column turned a restored partition "
             "into an error: rc=%d rows=%lld — the stamp must not cost "
             "availability [core]", rc, (long long)rows);
    if (wsum != 0)
        FAIL("[N3] the pre-existing rows did not read the new column as zero "
             "(sum=%lld)", (long long)wsum);
    tsdb_close(db);
    rm_rf(N3_SRC); rm_rf(N3_BK); rm_rf(N3_DST);
}

/* ==========================================================================
 * [N4] an UNSTAMPED source partition + a genuine late add.  The stream's
 *      header column count must not be stamped over it: the stream carries no
 *      block for that column, so the claim would be one the restore did not
 *      earn — and it would break the ALTER on the restored copy.
 * ======================================================================== */
#define N4_SRC "/tmp/tsdb_rncols4_src"
#define N4_BK  "/tmp/tsdb_rncols4_bk"
#define N4_DST "/tmp/tsdb_rncols4_dst"

static void case_unstamped_source_late_add(void) {
    printf("\n[N4] unstamped source partition + a real ALTER-added column\n");
    rm_rf(N4_SRC); rm_rf(N4_BK); rm_rf(N4_DST);
    write_source(N4_SRC);

    char part[4096];
    if (!find_part_dir(N4_SRC, part, sizeof(part))) FAIL("[N4] no source partition");
    if (strip_ncols_stamp(part) < 2)
        FAIL("[N4] could not strip the stamp from the source partition");
    if (stamp_of(part, "ts") != TSDB_IDX_NCOLS_UNKNOWN)
        FAIL("[N4] the source partition is still stamped");

    add_column_w(N4_SRC);       /* w has no blocks in this partition */

    /* The SOURCE reads it as a late add — zeros, not an error. */
    {
        tsdb_db_t *db = NULL; int rc = 0; int64_t rows = 0, wsum = 0;
        OK(tsdb_open(N4_SRC, &db));
        scan_i64(db, "SELECT ts, w FROM t", &rows, &wsum, &rc);
        tsdb_close(db);
        printf("[N4] SOURCE   rc=%d rows=%lld sum(w)=%lld\n",
               rc, (long long)rows, (long long)wsum);
        if (rc != TSDB_OK || rows != NROWS || wsum != 0)
            FAIL("[N4] the source does not read the late add as zeros "
                 "(rc=%d rows=%lld sum=%lld) — the case proves nothing",
                 rc, (long long)rows, (long long)wsum);
    }

    backup_to(N4_SRC, N4_BK);
    restore_into(N4_BK, N4_DST);

    char dpart[4096];
    if (!find_part_dir(N4_DST, dpart, sizeof(dpart))) FAIL("[N4] no restored partition");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(N4_DST, &db));
    int rc = 0; int64_t rows = 0, wsum = 0;
    scan_i64(db, "SELECT ts, w FROM t", &rows, &wsum, &rc);
    printf("[N4] RESTORED rc=%d (%s) rows=%lld sum(w)=%lld stamp=%u\n",
           rc, tsdb_errstr(rc), (long long)rows, (long long)wsum,
           (unsigned)stamp_of(dpart, "ts"));
    if (rc != TSDB_OK || rows != NROWS)
        FAIL("[N4] the restore stamped a column count the stream did not carry "
             "blocks for: a legitimately ALTER-added column reads rc=%d on the "
             "restored copy where the source reads zeros [core]", rc);
    if (wsum != 0)
        FAIL("[N4] the restored copy did not zero-fill the late add (sum=%lld)",
             (long long)wsum);
    tsdb_close(db);
    rm_rf(N4_SRC); rm_rf(N4_BK); rm_rf(N4_DST);
}

/* ==========================================================================
 * [N5] ...but a restore that DID land every column of a partition may assert
 *      the count even when the source never stamped it.
 * ======================================================================== */
#define N5_SRC "/tmp/tsdb_rncols5_src"
#define N5_BK  "/tmp/tsdb_rncols5_bk"
#define N5_DST "/tmp/tsdb_rncols5_dst"

static void case_unstamped_source_full_partition(void) {
    printf("\n[N5] unstamped source, every column landed — the restore may assert\n");
    rm_rf(N5_SRC); rm_rf(N5_BK); rm_rf(N5_DST);
    write_source(N5_SRC);

    char part[4096];
    if (!find_part_dir(N5_SRC, part, sizeof(part))) FAIL("[N5] no source partition");
    if (strip_ncols_stamp(part) < 2)
        FAIL("[N5] could not strip the stamp from the source partition");

    backup_to(N5_SRC, N5_BK);
    restore_into(N5_BK, N5_DST);

    char dpart[4096];
    if (!find_part_dir(N5_DST, dpart, sizeof(dpart))) FAIL("[N5] no restored partition");
    if (!drop_column_files(dpart, "val"))
        FAIL("[N5] could not remove val.idx / val.col from the restored copy");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(N5_DST, &db));
    int rc = 0; int64_t rows = 0; double sum = 0; int wrong = 0;
    scan_f64(db, "SELECT ts, val FROM t", &rows, &sum, &wrong, &rc);
    printf("[N5] RESTORED-then-torn rc=%d (%s) rows=%lld sum=%.1f stamp=%u\n",
           rc, tsdb_errstr(rc), (long long)rows, sum,
           (unsigned)stamp_of(dpart, "ts"));
    if (rc == TSDB_OK)
        FAIL("[N5] the restore landed every column of this partition and still "
             "left the claim unasserted, so a later loss reads back as "
             "sum=%.1f with rc=0", sum);
    tsdb_close(db);
    rm_rf(N5_SRC); rm_rf(N5_BK); rm_rf(N5_DST);
}

/* ==========================================================================
 * [N6] a target that holds its OWN redo log still gets the stamp.
 *
 * The sidecar's WAL checkpoints are refused there — a foreign seq could mask
 * the target's un-replayed records — but the ncols stamp says nothing about
 * sequence numbers and cannot mask anything.  Skipping the whole sidecar left
 * exactly those partitions unstamped, i.e. fabricating zeros for a lost
 * trailing column, which is the defect the stamp exists to close.
 * ======================================================================== */
#define N6_SRC "/tmp/tsdb_rncols6_src"
#define N6_BK  "/tmp/tsdb_rncols6_bk"
#define N6_DST "/tmp/tsdb_rncols6_dst"

/* The target's own rows go to a different day, so its eventual flush builds its
 * own partition and never rewrites the restored one. */
#define N6_OTHER_DAY (BASE_TS + 86400LL * 1000000000LL)

static void case_target_with_own_wal(void) {
    printf("\n[N6] restoring into a target that holds its own redo log\n");
    const char *prev = getenv("TSDB_WAL_ONLY_COMMIT");
    char saved[16] = {0};
    if (prev) snprintf(saved, sizeof(saved), "%s", prev);
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);   /* a commit is WAL + memtable */

    rm_rf(N6_SRC); rm_rf(N6_BK); rm_rf(N6_DST);
    write_source(N6_SRC);
    backup_to(N6_SRC, N6_BK);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(N6_DST, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 100; i++) {
        OK(tsdb_batch_row_ts(b, N6_OTHER_DAY + (int64_t)i * STEP_NS));
        OK(tsdb_batch_row_f64(b, 1, val_of(i)));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));                 /* acked into the WAL, not flushed */

    {
        char wal[4200];
        snprintf(wal, sizeof(wal), "%s/wal/t.log", N6_DST);
        struct stat st;
        if (stat(wal, &st) != 0 || st.st_size == 0)
            FAIL("[N6] the target has no redo log of its own — the case does not "
                 "reach the path it is about");
        printf("[N6] target redo log: %lld bytes\n", (long long)st.st_size);
    }

    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_restore_run(db, N6_BK, &rep);
    if (rc != TSDB_OK || !rep.complete)
        FAIL("[N6] restore into a target with its own WAL rc=%d (%s) complete=%d",
             rc, tsdb_errstr(rc), rep.complete);
    tsdb_restore_report_free(&rep);
    tsdb_close(db);

    char part[4096];
    {   /* the RESTORED day, not the one the target's own rows flushed into */
        char p[4200];
        snprintf(p, sizeof(p), "%s/t/%s", N6_DST, "19700101");
        struct stat st;
        if (stat(p, &st) != 0)
            FAIL("[N6] the restored partition is not where the source's was");
        snprintf(part, sizeof(part), "%s", p);
    }
    if (!drop_column_files(part, "val"))
        FAIL("[N6] could not remove val.idx / val.col from the restored partition");

    OK(tsdb_open(N6_DST, &db));
    int qrc = 0; int64_t rows = 0; double sum = 0; int wrong = 0;
    scan_f64(db, "SELECT ts, val FROM t", &rows, &sum, &wrong, &qrc);
    printf("[N6] RESTORED-then-torn rc=%d (%s) rows=%lld stamp=%u\n",
           qrc, tsdb_errstr(qrc), (long long)rows, (unsigned)stamp_of(part, "ts"));
    if (qrc == TSDB_OK)
        FAIL("[N6] a partition restored into a target holding its own redo log "
             "came out unstamped, so a lost trailing column reads back as "
             "rc=0 — the WAL refusal is about CHECKPOINTS, not about the "
             "column-count stamp [core]");
    tsdb_close(db);

    rm_rf(N6_SRC); rm_rf(N6_BK); rm_rf(N6_DST);
    if (saved[0]) setenv("TSDB_WAL_ONLY_COMMIT", saved, 1);
    else          unsetenv("TSDB_WAL_ONLY_COMMIT");
}

int main(void) {
    printf("=== test_restore_ncols ===\n");
    case_torn_source_reproduced();
    case_restored_keeps_protection();
    case_alter_on_restored_still_fills();
    case_unstamped_source_late_add();
    case_unstamped_source_full_partition();
    case_target_with_own_wal();
    printf("\n=== test_restore_ncols PASSED ===\n");
    return 0;
}
