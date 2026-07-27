/* test_zerofill_absent_column.c — a column with NO index at all must not be
 * zero-filled unless zero really is the value.
 *
 * THE BUG (reproduced by hand at a7074b3, exactly as reported):
 *
 *     write 5000 rows -> flush -> delete val.idx AND val.col -> reopen
 *       SELECT ts, val   ->  rc = TSDB_OK
 *                            rows = 5000        (correct)
 *                            sum(val) = 0.0     (the real sum was 12497500)
 *
 * tsdb_part_open classified `val` as a column added later by ALTER TABLE (it
 * has no idx, so idx_decl_count == 0 and nb_col == 0), prepended sentinels with
 * offset == UINT64_MAX and WITHOUT TSDB_BLOCK_FLAG_HOLE, and tsdb_part_read_block
 * memset the output to zero and returned TSDB_OK.  count(*), max(ts), the backup
 * manifest check and anti-entropy (count/max(ts)) all still report healthy, so
 * nothing anywhere alerts.  It is worse than the torn-column case, which errors.
 *
 * WHY IT IS HARD: on disk these two are IDENTICAL — there is no block count to
 * compare, because there are no blocks.
 *   (a) a genuinely ALTER-added column: the rows predate it, zero-fill is RIGHT;
 *   (b) a write that never landed: zero-fill FABRICATES every value.
 * The idx_decl_count == 0 gate is what keeps (a) working and must stay; it is
 * necessary and not sufficient.  The fix ANDs it with evidence that is not a
 * count (src/storage/part.c, part_col_absence_is_late_add):
 *   1. SHAPE — tsdb_schema_add_column only ever appends, so a late-added column
 *      sits to the RIGHT of everything that predates it.  A column with blocks
 *      to its right cannot be the late add.  Free, and it works on partitions
 *      written by older binaries.
 *   2. THE STAMP — idx header bytes [10..11] (a field every writer has always
 *      zeroed and no reader ever read) now record how many columns the schema
 *      had when the FLUSH published that idx.  The flush writes every schema
 *      column into every partition it touches and publishes ts LAST, so a
 *      column with index < the ts stamp was written here and is missing.  Only
 *      the flush asserts it; replication, compaction and the ts retraction
 *      preserve it, so a one-column publish can never invent the claim.
 *
 * WHAT THIS ADDS over the two tests that already pass:
 *   test_torn_partition_read.c  tears the TS column.
 *   test_torn_value_column.c    tears a non-ts .col while its .idx SURVIVES, so
 *                               idx_decl_count > 0 and the read already errors.
 *   Neither can reach this bug: both leave an index behind.  This one removes
 *   the index entirely — the only shape for which counting proves nothing — and
 *   it is the shape a partial file-level restore, a lost replication group or an
 *   interrupted bulk import actually leaves on disk.
 *
 * Cases:
 *   [1] the reported repro: flush, delete val.idx + val.col, reopen.  No
 *       fabricated value may be returned with rc == TSDB_OK.
 *   [2] ALTER TABLE ADD COLUMN still zero-fills its old rows (the fix must not
 *       trade a correctness bug for an availability bug).
 *   [3] a partition with NO stamp (an older binary wrote it) losing a NON-
 *       trailing column is still caught, by shape alone.
 *   [4] the same unstamped partition with a genuine late add still reads zeros,
 *       so rule 1 does not over-fire on legacy data.
 *   [5] a REAL process death mid-flush (fork + TSDB_TEST_CRASH_BEFORE_TS, no
 *       tsdb_close) never advances the stamp, so a crash cannot convert a
 *       legitimate ALTER-added column into a permanent read error.
 */

#include "../include/tsdb.h"
#include "../src/storage/part.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

/* Internal explicit-flush entry, declared here exactly as
 * test_partial_flush_recovery.c does, so the crash case can force a flush at a
 * known point instead of hoping a commit triggers one. */
extern int tsdb_db_flush_all(tsdb_db_t *db);

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

#define NROWS    5000
#define BASE_TS  1000000000000LL
#define STEP_NS  1000000LL

/* floor(val_of(i)) == i, so a mis-paired cell is detectable as well as a
 * fabricated one.  sum over [0, NROWS) is well above zero. */
static double val_of(int i) { return (double)i + (double)(i % 997) / 1000.0; }

/* <db>/t/<YYYYMMDD> */
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

/* Delete a column's index AND data: the shape a partial restore / lost
 * replication group / interrupted import leaves behind. */
static int drop_column_files(const char *part_dir, const char *col) {
    char p[4200];
    int ok = 1;
    snprintf(p, sizeof(p), "%s/%s.idx", part_dir, col);
    if (unlink(p) != 0) ok = 0;
    snprintf(p, sizeof(p), "%s/%s.col", part_dir, col);
    if (unlink(p) != 0) ok = 0;
    return ok;
}

/* Rewrite every <col>.idx header in this partition with ncols == 0, i.e. make
 * it byte-identical to what a binary from before the stamp existed produced.
 * Everything else about the file is untouched. */
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

/* ---- read-back ----------------------------------------------------------- */

/* SELECT the given projection.  *rows = rows returned, *sum = sum of the FLOAT64
 * column at result index `vcol` (-1 to skip), *wrong = cells that are neither
 * the right value, *rc = the query rc. */
static void scan(tsdb_db_t *db, const char *sql, int vcol,
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
        if (vcol >= 0) {
            double v = tsdb_result_f64(r, vcol);
            *sum += v;
            if ((int64_t)v != idx) (*wrong)++;
        }
        (*rows)++;
    }
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

/* ---- dataset builders ---------------------------------------------------- */

/* (ts, val) or (ts, val, tag) depending on `with_tag`; NROWS rows, flushed. */
static void write_dataset(const char *dir, int with_tag) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); exit(1); }
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
        { "tag", TSDB_TYPE_INT64     },
    };
    int ncols = with_tag ? 3 : 2;
    int rc = tsdb_create_table(db, "t", cols, ncols, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) {
        fprintf(stderr, "create rc=%d\n", rc); exit(1);
    }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "open_table\n"); exit(1); }
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    for (int i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * STEP_NS);
        tsdb_batch_row_f64(b, 1, val_of(i));
        if (with_tag) tsdb_batch_row_i64(b, 2, (int64_t)i);
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    tsdb_close(db);
}

static double expected_sum(void) {
    double s = 0.0;
    for (int i = 0; i < NROWS; i++) s += val_of(i);
    return s;
}

/* ==========================================================================
 * [1] the reported repro
 * ======================================================================== */
static void case_reported_repro(void) {
    printf("\n[1] flush -> delete val.idx + val.col -> reopen\n");
    const char *dir = "/tmp/tsdb_zfac_repro";
    rmrf(dir);
    write_dataset(dir, /*with_tag=*/0);

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "partition was flushed");
    if (!part[0]) return;

    /* The marker the fix rests on: the flush recorded that this partition was
     * written with 2 columns. */
    CHECK(stamp_of(part, "ts") == 2,
          "flush stamped the ts idx with the schema's column count");

    /* Baseline: intact partition reads correctly. */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }
        int64_t rows = 0; double sum = 0; int wrong = 0, rc = 0;
        scan(db, "SELECT ts, val FROM t", 1, &rows, &sum, &wrong, &rc);
        printf("  intact: rc=%d rows=%lld sum=%.1f wrong=%d\n",
               rc, (long long)rows, sum, wrong);
        CHECK(rc == TSDB_OK && rows == NROWS && wrong == 0,
              "intact: every row and every value is correct");
        CHECK(sum > 0.0 && sum == expected_sum(),
              "intact: sum(val) is the real sum, not zero");
        tsdb_close(db);
    }

    CHECK(drop_column_files(part, "val"),
          "removed val.idx and val.col (partial restore / lost group shape)");

    /* Reopen and read.  The pre-fix answer was rc=0 / rows=5000 / sum=0. */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen damaged\n"); exit(1); }
        int64_t rows = 0; double sum = 0; int wrong = 0, rc = 0;
        scan(db, "SELECT ts, val FROM t", 1, &rows, &sum, &wrong, &rc);
        int64_t cnt = q_count(db);
        printf("  damaged: rc=%d rows=%lld sum=%.1f wrong=%d count(*)=%lld\n",
               rc, (long long)rows, sum, wrong, (long long)cnt);

        /* THE CORE REGRESSION, stated as the exact signature that shipped. */
        CHECK(!(rc == TSDB_OK && rows == NROWS && sum == 0.0),
              "the reported answer (rc=0, 5000 rows, sum=0) is gone [core]");
        CHECK(rc != TSDB_OK,
              "a column with no index errors instead of fabricating zeros [core]");
        CHECK(wrong == 0, "no fabricated or mis-paired value cell is returned [core]");
        CHECK(rows == 0, "no row is handed back carrying an unanswerable cell");

        /* Documented, unchanged: count(*) is served from ts alone, so it still
         * reports the rows.  That is why the tear is loud for readers and needs
         * tsdb_part_ts_retract_unpaired to become visible to anti-entropy. */
        CHECK(cnt == NROWS,
              "count(*) still reports every ts row (repair, not read, closes that)");

        /* ts alone is untouched: only the unanswerable column is refused. */
        int64_t trows = 0; double tsum = 0; int twrong = 0, trc = 0;
        scan(db, "SELECT ts FROM t", -1, &trows, &tsum, &twrong, &trc);
        CHECK(trc == TSDB_OK && trows == NROWS,
              "SELECT ts still succeeds — the refusal is scoped to the lost column");
        tsdb_close(db);
    }
    rmrf(dir);
}

/* ==========================================================================
 * [2] ALTER TABLE ADD COLUMN must still zero-fill (availability guard)
 * ======================================================================== */
static void case_alter_still_zero_fills(void) {
    printf("\n[2] ALTER ADD COLUMN still reads its pre-existing rows as zero\n");
    const char *dir = "/tmp/tsdb_zfac_alter";
    rmrf(dir);
    write_dataset(dir, /*with_tag=*/0);

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "partition was flushed");
    CHECK(stamp_of(part, "ts") == 2, "ts stamp records the pre-ALTER column count");

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }
    tsdb_table_t *th = NULL;   /* ALTER resolves through the open-table set */
    CHECK(tsdb_open_table(db, "t", &th) == TSDB_OK, "table reopened");
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "ALTER TABLE t ADD COLUMN w INT64", &r);
    if (r) { tsdb_result_free(r); r = NULL; }
    CHECK(rc == TSDB_OK, "ALTER ADD COLUMN w succeeded");
    tsdb_close(db);

    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen after alter\n"); exit(1); }
    int64_t rows = 0, wsum = 0; int wrong = 0;
    r = NULL;
    int qrc = tsdb_query(db, "SELECT ts, val, w FROM t", &r);
    if (qrc == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            double  v  = tsdb_result_f64(r, 1);
            int64_t w  = tsdb_result_i64(r, 2);
            int64_t idx = (ts - BASE_TS) / STEP_NS;
            if ((int64_t)v != idx) wrong++;
            wsum += w;
            rows++;
        }
    }
    if (r) tsdb_result_free(r);
    printf("  alter: rc=%d rows=%lld val_wrong=%d w_sum=%lld\n",
           qrc, (long long)rows, wrong, (long long)wsum);
    CHECK(qrc == TSDB_OK && rows == NROWS,
          "a genuinely late-added column does NOT turn the partition into an error");
    CHECK(wrong == 0, "val stays correctly paired across the ALTER");
    CHECK(wsum == 0, "pre-existing rows read the new column as zero-fill");
    tsdb_close(db);
    rmrf(dir);
}

/* ==========================================================================
 * [3] no stamp at all (a partition an older binary wrote): a NON-trailing
 *     column that vanished is still caught, from shape alone.
 * ======================================================================== */
static void case_unstamped_shape_rule(void) {
    printf("\n[3] unstamped (legacy) partition, middle column deleted\n");
    const char *dir = "/tmp/tsdb_zfac_legacy";
    rmrf(dir);
    write_dataset(dir, /*with_tag=*/1);          /* (ts, val, tag) */

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "partition was flushed");
    CHECK(strip_ncols_stamp(part) >= 3, "stripped the stamp from every idx here");
    CHECK(stamp_of(part, "ts") == TSDB_IDX_NCOLS_UNKNOWN,
          "partition now looks exactly like one written before the stamp existed");

    CHECK(drop_column_files(part, "val"), "removed val.idx and val.col");

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen legacy\n"); exit(1); }
    int64_t rows = 0; double sum = 0; int wrong = 0, rc = 0;
    scan(db, "SELECT ts, val, tag FROM t", 1, &rows, &sum, &wrong, &rc);
    printf("  legacy damaged: rc=%d rows=%lld sum=%.1f\n",
           rc, (long long)rows, sum);
    CHECK(!(rc == TSDB_OK && rows == NROWS && sum == 0.0),
          "no fabricated zeros even with no stamp to consult [core]");
    CHECK(rc != TSDB_OK,
          "shape alone (a column with blocks to its right) proves it is not a late add");

    /* The healthy columns are not punished for the lost one. */
    int64_t trows = 0; double tsum = 0; int twrong = 0, trc = 0;
    scan(db, "SELECT ts, tag FROM t", -1, &trows, &tsum, &twrong, &trc);
    CHECK(trc == TSDB_OK && trows == NROWS,
          "SELECT ts,tag still succeeds — only the lost column is refused");
    tsdb_close(db);
    rmrf(dir);
}

/* ==========================================================================
 * [4] the same unstamped partition, but with a REAL late add: rule 1 must not
 *     over-fire, or every legacy ALTER becomes an error.
 * ======================================================================== */
static void case_unstamped_alter_still_ok(void) {
    printf("\n[4] unstamped (legacy) partition + a genuine ALTER ADD COLUMN\n");
    const char *dir = "/tmp/tsdb_zfac_legacy_alter";
    rmrf(dir);
    write_dataset(dir, /*with_tag=*/0);

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "partition was flushed");
    CHECK(strip_ncols_stamp(part) >= 2, "stripped the stamp from every idx here");
    CHECK(stamp_of(part, "ts") == TSDB_IDX_NCOLS_UNKNOWN, "partition is unstamped");

    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }
    tsdb_table_t *th = NULL;   /* ALTER resolves through the open-table set */
    CHECK(tsdb_open_table(db, "t", &th) == TSDB_OK, "table reopened");
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "ALTER TABLE t ADD COLUMN w INT64", &r);
    if (r) { tsdb_result_free(r); r = NULL; }
    CHECK(rc == TSDB_OK, "ALTER ADD COLUMN w succeeded");
    tsdb_close(db);

    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen after alter\n"); exit(1); }
    int64_t rows = 0, wsum = 0;
    r = NULL;
    int qrc = tsdb_query(db, "SELECT ts, w FROM t", &r);
    if (qrc == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) { wsum += tsdb_result_i64(r, 1); rows++; }
    }
    if (r) tsdb_result_free(r);
    printf("  legacy alter: rc=%d rows=%lld w_sum=%lld\n",
           qrc, (long long)rows, (long long)wsum);
    CHECK(qrc == TSDB_OK && rows == NROWS,
          "a legacy partition + a real late add still reads (no availability regression)");
    CHECK(wsum == 0, "and its pre-existing rows still read the new column as zero");
    tsdb_close(db);
    rmrf(dir);
}

/* ==========================================================================
 * [5] A REAL process death mid-flush must not turn the stamp into a false
 *     accusation.
 *
 * The stamp is the one thing this fix adds that could cost availability: if a
 * crashed flush could leave a partition claiming K columns when only some of
 * them published, every ALTER-added column below K would read as a permanent
 * error.  It cannot, because the flush publishes ts LAST and the reader takes
 * the stamp from ts alone — but that is an argument, and arguments about crash
 * behaviour are worthless.  So kill a process for real (fork + the existing
 * TSDB_TEST_CRASH_BEFORE_TS hook, which returns from the flush after every
 * non-ts column has published and before ts) and look at the bytes.
 * ======================================================================== */
#define DAY2 (BASE_TS + 86400LL * 1000000000LL)

static uint32_t idx_blocks(const char *part_dir, const char *col) {
    char p[4200];
    snprintf(p, sizeof(p), "%s/%s.idx", part_dir, col);
    uint32_t cnt = 0;
    int hsz = tsdb_part_idx_probe(p, NULL, &cnt, NULL, NULL, NULL, NULL, NULL);
    return (hsz > 0) ? cnt : 0;
}

static int find_part_named(const char *root, const char *skip,
                           char *out, size_t cap) {
    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/t", root);
    DIR *d = opendir(tdir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4200];
        snprintf(p, sizeof(p), "%s/%s", tdir, e->d_name);
        if (skip && strcmp(p, skip) == 0) continue;
        snprintf(out, cap, "%s", p);
        found = 1;
        break;
    }
    closedir(d);
    return found;
}

static void case_crash_never_over_claims(void) {
    printf("\n[5] a real crash mid-flush must not advance the stamp\n");
    const char *dir = "/tmp/tsdb_zfac_crash";
    rmrf(dir);
    write_dataset(dir, /*with_tag=*/0);          /* partition P: ts,val */

    char p1[4096];
    CHECK(find_part_dir(dir, p1, sizeof(p1)), "partition P was flushed");
    CHECK(stamp_of(p1, "ts") == 2, "P: ts stamp is 2 (the pre-ALTER schema)");

    /* Add a third column.  P keeps its own stamp — no flush touches it. */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }
        tsdb_table_t *th = NULL;
        CHECK(tsdb_open_table(db, "t", &th) == TSDB_OK, "table reopened");
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(db, "ALTER TABLE t ADD COLUMN w INT64", &r);
        if (r) tsdb_result_free(r);
        CHECK(rc == TSDB_OK, "ALTER ADD COLUMN w succeeded");
        tsdb_close(db);
    }
    CHECK(stamp_of(p1, "ts") == 2, "P: the ALTER did not restamp an untouched partition");

    /* Child: write a SECOND partition's worth of rows, then die inside the
     * flush after val and w have published and before ts. */
    pid_t pid = fork();
    if (pid < 0) { printf("  fork unavailable, SKIP\n"); rmrf(dir); return; }
    if (pid == 0) {
        setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);   /* commit must not auto-flush */
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) _exit(2);
        tsdb_table_t *t = NULL;
        if (tsdb_open_table(db, "t", &t) != TSDB_OK) _exit(3);
        tsdb_batch_t *b = NULL;
        tsdb_batch_begin(t, &b);
        for (int i = 0; i < 2000; i++) {
            tsdb_batch_row_ts(b, DAY2 + (int64_t)i * STEP_NS);
            tsdb_batch_row_f64(b, 1, val_of(i));
            tsdb_batch_row_i64(b, 2, 7);
            tsdb_batch_row_end(b);
        }
        tsdb_batch_commit(b);
        setenv("TSDB_TEST_CRASH_BEFORE_TS", "1", 1);
        tsdb_db_flush_all(db);      /* publishes val + w, returns before ts */
        _exit(0);                   /* real death: no tsdb_close, no cleanup */
    }
    int status = 0;
    CHECK(waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
          WEXITSTATUS(status) == 0, "crash child ran and died without closing");
    unsetenv("TSDB_TEST_CRASH_BEFORE_TS");

    /* Look at the bytes the dead process left, before anything reopens. */
    char p2[4096];
    p2[0] = '\0';
    CHECK(find_part_named(dir, p1, p2, sizeof(p2)),
          "the crashed flush created a second partition Q");
    if (p2[0]) {
        CHECK(idx_blocks(p2, "val") > 0 && idx_blocks(p2, "w") > 0,
              "Q: the crash landed AFTER val and w published");
        CHECK(idx_blocks(p2, "ts") == 0,
              "Q: ts never published, so Q is invisible — not torn");
        /* The non-ts columns of Q DID take the new stamp.  Reading the stamp
         * from ts (published last) instead of the max over columns is exactly
         * what makes that harmless. */
        CHECK(stamp_of(p2, "val") == 3, "Q: a non-ts column carries the new stamp");
    }
    CHECK(stamp_of(p1, "ts") == 2,
          "P: a crashed flush never advanced P's claim from 2 to 3 [core]");

    /* And the behaviour that matters: w is still a legitimate late add on P. */
    {
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen post-crash\n"); exit(1); }
        int64_t old_rows = 0, old_w = 0, rows = 0; int wrong = 0;
        tsdb_result_t *r = NULL;
        int qrc = tsdb_query(db, "SELECT ts, val, w FROM t", &r);
        if (qrc == TSDB_OK && r) {
            while (tsdb_result_next(r) > 0) {
                int64_t ts = tsdb_result_ts(r, 0);
                double  v  = tsdb_result_f64(r, 1);
                int64_t w  = tsdb_result_i64(r, 2);
                if (ts < DAY2) {
                    int64_t idx = (ts - BASE_TS) / STEP_NS;
                    if ((int64_t)v != idx) wrong++;
                    old_w += w;
                    old_rows++;
                }
                rows++;
            }
        }
        if (r) tsdb_result_free(r);
        printf("  post-crash: rc=%d rows=%lld old_rows=%lld old_w_sum=%lld wrong=%d\n",
               qrc, (long long)rows, (long long)old_rows, (long long)old_w, wrong);
        CHECK(qrc == TSDB_OK,
              "the partition reads after a real crash (no availability regression) [core]");
        CHECK(old_rows == NROWS && wrong == 0,
              "every pre-ALTER row survived the crash with its value intact");
        CHECK(old_w == 0,
              "and still reads the late-added column as zero, not as an error [core]");
        tsdb_close(db);
    }
    rmrf(dir);
}

int main(void) {
    printf("=== test_zerofill_absent_column ===\n");
    case_reported_repro();
    case_alter_still_zero_fills();
    case_unstamped_shape_rule();
    case_unstamped_alter_still_ok();
    case_crash_never_over_claims();
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
