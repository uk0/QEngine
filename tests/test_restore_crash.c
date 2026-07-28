/* test_restore_crash.c — kill a real process in the middle of a restore and
 * assert what the disk is left holding.
 *
 * A crash test that unwinds cleanly is worse than no test: it reads as
 * coverage while proving nothing about a power cut.  Every phase here forks,
 * runs the restore in the CHILD, and the child dies inside library code via
 * _exit() — no atexit handlers, no tsdb_close, no buffered writes drained.
 * The parent waits, checks the child really died the way it was told to, and
 * then inspects the target from a fresh process.
 *
 * The kill points are env-gated hooks in src/storage/restore.c, matching
 * part.c's TSDB_TEST_CRASH_AFTER_PART / TSDB_TEST_CRASH_BEFORE_TS.  getenv
 * returns NULL in production, so neither is reachable from a shipped binary
 * and neither adds a production-visible switch.
 *
 * What each phase pins:
 *
 *   A. Crash after N blocks.  .tsdb_restore is STILL THERE — the marker's
 *      absence is the only claim a restore finished, so a crashed run must
 *      never look complete.  Whatever rows ARE visible must have CORRECT
 *      values: a partially restored partition may be short, it may not be
 *      wrong.
 *
 *   B. Crash between the value columns and ts.  This is the state that
 *      produces fabricated zeros if ts is published first: the value columns
 *      are on disk, ts is not.  Assert the partition publishes ZERO rows
 *      rather than rows whose value columns are missing.
 *
 *   C. Resume.  Re-running the restore after either crash completes it, drops
 *      the marker, and reproduces the source EXACTLY — by value, not by count
 *      — with no block duplicated: a resumed restore re-offers everything the
 *      crashed run already landed, which is precisely the path a tail-only
 *      dedup turns into double-counted rows.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"

#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define SRC   "/tmp/tsdb_test_rstcrash_src"
#define BK    "/tmp/tsdb_test_rstcrash_bk"
#define DSTA  "/tmp/tsdb_test_rstcrash_a"
#define DSTB  "/tmp/tsdb_test_rstcrash_b"
#define DSTC  "/tmp/tsdb_test_rstcrash_c"
#define SRCD  "/tmp/tsdb_test_rstcrash_srcd"
#define BKD   "/tmp/tsdb_test_rstcrash_bkd"

/* Phase D's own source: built under deferred flush so its partitions actually
 * carry a non-zero WAL checkpoint.  In flush-on-commit mode commit_seq stays 0
 * and there is nothing to stamp, which would make the phase vacuous in one of
 * the two modes scripts/test-both-modes.sh runs. */
#define DROWS  3000

/* Rows phase D appends AFTER the restore, interleaved into a partition the
 * restore already stamped with the SOURCE's WAL checkpoint. */
#define EXTRA  500
#define EXTRA_OFF 500000LL       /* half a millisecond off the seeded grid */

#define DAY0    1700000000000000000LL
#define DAY_NS  86400000000000LL
#define PERDAY  20000            /* 3 blocks per column per partition */

static const char *TAGS[4] = { "alpha", "bravo", "charlie", "delta" };

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

static double  val_f(int64_t i) { return (double)(i % 977) * 0.25 - 100.0; }
static int64_t val_n(int64_t i) { return (i * 7919) % 100003; }

/* Per-row expected values, keyed by the timestamp itself, so a partial
 * restore can be checked without knowing WHICH rows survived. */
static int expect_row(int64_t ts, double *out_v, int64_t *out_n, const char **out_tag) {
    for (int day = 0; day < 2; day++) {
        int64_t base = DAY0 + (int64_t)day * DAY_NS;
        if (ts < base || ts >= base + (int64_t)PERDAY * 1000000LL) continue;
        int64_t k = (ts - base) / 1000000LL;
        if (base + k * 1000000LL == ts) {           /* seeded grid */
            int64_t g = (int64_t)day * PERDAY + k;
            *out_v = val_f(g); *out_n = val_n(g); *out_tag = TAGS[g % 4];
            return 1;
        }
        /* phase D appends into day 0, offset half a millisecond. */
        if (day == 0 && k < EXTRA && base + k * 1000000LL + EXTRA_OFF == ts) {
            int64_t g = 1000000 + k;
            *out_v = val_f(g); *out_n = val_n(g); *out_tag = TAGS[g % 4];
            return 1;
        }
    }
    return 0;
}

/* Walk every visible row and check it against expect_row.  Returns the row
 * count; *out_bad counts rows whose values do not match what the source had
 * at that timestamp (a fabricated zero, a mis-paired block, a lost tag). */
static int64_t check_visible(tsdb_db_t *db, const char *table, int64_t *out_bad) {
    char q[160];
    snprintf(q, sizeof(q), "SELECT ts, v, n, tag FROM %s", table);
    tsdb_result_t *r = NULL;
    int64_t rows = 0, bad = 0;
    if (tsdb_query(db, q, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            double  v  = tsdb_result_f64(r, 1);
            int64_t n  = tsdb_result_i64(r, 2);
            const char *tag = tsdb_result_sym(r, 3);
            double ev; int64_t en; const char *et;
            if (!expect_row(ts, &ev, &en, &et)) { bad++; }
            else if (v != ev || n != en || !tag || strcmp(tag, et) != 0) { bad++; }
            rows++;
        }
        tsdb_result_free(r);
    }
    *out_bad = bad;
    return rows;
}

static int64_t row_count(tsdb_db_t *db, const char *table) {
    char q[128]; snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
    tsdb_result_t *r = NULL; int64_t n = -1;
    if (tsdb_query(db, q, &r) == TSDB_OK && r && tsdb_result_next(r) > 0)
        n = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);
    return n;
}

static uint32_t rd_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static int is_part_name(const char *s) {
    size_t k = strlen(s);
    if (k != 8 && k != 10) return 0;
    for (size_t i = 0; i < k; i++) if (s[i] < '0' || s[i] > '9') return 0;
    return 1;
}

static int idx_dup_keys(const char *idx_path) {
    uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(idx_path, NULL, &cnt, &esz, NULL, NULL, NULL, NULL);
    if (hsz <= 0 || cnt == 0 || esz < 24) return 0;
    FILE *f = fopen(idx_path, "rb");
    if (!f) return 0;
    int64_t *tmin = calloc(cnt, sizeof(*tmin));
    uint32_t *cn  = calloc(cnt, sizeof(*cn));
    uint8_t *e    = malloc(esz);
    int dup = 0;
    if (tmin && cn && e) {
        uint32_t got = 0;
        for (uint32_t i = 0; i < cnt; i++) {
            if (fseeko(f, (off_t)hsz + (off_t)i * esz, SEEK_SET) != 0) break;
            if (fread(e, 1, esz, f) != esz) break;
            uint64_t t = 0;
            for (int k = 7; k >= 0; k--) t = (t << 8) | e[16 + k];
            tmin[got] = (int64_t)t; cn[got] = rd_u32(e + 12); got++;
        }
        for (uint32_t i = 0; i < got; i++)
            for (uint32_t j = i + 1; j < got; j++)
                if (tmin[i] == tmin[j] && cn[i] == cn[j]) dup++;
    }
    free(tmin); free(cn); free(e); fclose(f);
    return dup;
}

/* Blocks in <part>/<col>.idx, straight from the header. */
static uint32_t idx_block_count(const char *data_dir, const char *table,
                                const char *part, const char *col)
{
    char ip[4400];
    snprintf(ip, sizeof(ip), "%s/%s/%s/%s.idx", data_dir, table, part, col);
    uint32_t cnt = 0;
    if (tsdb_part_idx_probe(ip, NULL, &cnt, NULL, NULL, NULL, NULL, NULL) <= 0) return 0;
    return cnt;
}

static int total_dup_keys(const char *data_dir, const char *table,
                          const char **cols, int ncols)
{
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl_dir);
    if (!d) return 0;
    int total = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!is_part_name(e->d_name)) continue;
        for (int c = 0; c < ncols; c++) {
            char ip[4400];
            snprintf(ip, sizeof(ip), "%s/%s/%s.idx", tbl_dir, e->d_name, cols[c]);
            total += idx_dup_keys(ip);
        }
    }
    closedir(d);
    return total;
}

/* Total blocks on disk for `table`, split into ts and non-ts.
 *
 * How many blocks a given row count produces depends on the durability mode
 * (flush-on-commit writes one block per commit; deferred flush writes 8192-row
 * chunks), so the crash point below is derived from what is actually on disk
 * rather than hardcoded — a hardcoded count either never fires or fires in the
 * wrong pass, and both turn this file back into a test that proves nothing. */
static void source_block_counts(const char *data_dir, const char *table,
                                const char **cols, int ncols, const char *ts_col,
                                uint64_t *out_nonts, uint64_t *out_ts)
{
    *out_nonts = 0; *out_ts = 0;
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!is_part_name(e->d_name)) continue;
        for (int c = 0; c < ncols; c++) {
            uint32_t n = idx_block_count(data_dir, table, e->d_name, cols[c]);
            if (!strcmp(cols[c], ts_col)) *out_ts += n;
            else                          *out_nonts += n;
        }
    }
    closedir(d);
}

/* Alphabetically first partition dir name under <data_dir>/<table>. */
static void first_part(const char *data_dir, const char *table, char *out, size_t cap) {
    out[0] = '\0';
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!is_part_name(e->d_name)) continue;
        if (!out[0] || strcmp(e->d_name, out) < 0) snprintf(out, cap, "%s", e->d_name);
    }
    closedir(d);
}

static void build_source(void) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(SRC, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "v",   TSDB_TYPE_FLOAT64   },
        { "n",   TSDB_TYPE_INT64     },
        { "tag", TSDB_TYPE_SYMBOL    },
    };
    OK(tsdb_create_table(db, "m", cols, 4, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));
    for (int day = 0; day < 2; day++) {
        int64_t base = DAY0 + (int64_t)day * DAY_NS;
        int64_t i = 0;
        while (i < PERDAY) {
            int m = (PERDAY - i < 1000) ? (int)(PERDAY - i) : 1000;
            tsdb_batch_t *b = NULL;
            OK(tsdb_batch_begin(t, &b));
            for (int k = 0; k < m; k++) {
                int64_t g = (int64_t)day * PERDAY + i + k;
                OK(tsdb_batch_row_ts(b, base + (i + k) * 1000000LL));
                OK(tsdb_batch_row_f64(b, 1, val_f(g)));
                OK(tsdb_batch_row_i64(b, 2, val_n(g)));
                OK(tsdb_batch_row_sym(b, 3, TAGS[g % 4]));
                OK(tsdb_batch_row_end(b));
            }
            OK(tsdb_batch_commit(b));
            i += m;
        }
    }
    OK(tsdb_db_flush_all(db));
    OK(tsdb_backup_create(db, BK, NULL));
    tsdb_close(db);
}

/* Fork, restore in the child with `env`=`val` set, and REQUIRE the child to
 * die by _exit(want_code).  A child that exits 0 means the hook never fired
 * and the phase proved nothing — that is a failure, not a pass. */
static void crash_restore(const char *target, const char *env, const char *val,
                          int want_code)
{
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) FAIL("fork: %s", strerror(errno));
    if (pid == 0) {
        setenv(env, val, 1);
        tsdb_db_t *d = NULL;
        if (tsdb_open(target, &d) != TSDB_OK) _exit(70);
        (void)tsdb_restore_run(d, BK, NULL);
        /* Reaching here means the hook did not fire.  Exit 0 so the parent
         * can tell "never crashed" apart from "crashed as instructed". */
        tsdb_close(d);
        _exit(0);
    }
    int st = 0;
    if (waitpid(pid, &st, 0) != pid) FAIL("waitpid failed");
    if (!WIFEXITED(st))
        FAIL("restore child died by signal %d, expected _exit(%d)",
             WTERMSIG(st), want_code);
    int code = WEXITSTATUS(st);
    printf("[crash]   child pid=%d exited %d (wanted %d, %s=%s)\n",
           (int)pid, code, want_code, env, val);
    if (code == 0)
        FAIL("the restore ran to completion — %s never fired, so nothing was "
             "actually crashed and this phase proves nothing", env);
    if (code != want_code)
        FAIL("child exited %d, expected %d", code, want_code);
}

int main(void) {
    printf("=== test_restore_crash ===\n");
    rm_rf(SRC); rm_rf(BK); rm_rf(DSTA); rm_rf(DSTB); rm_rf(DSTC);
    rm_rf(SRCD); rm_rf(BKD);
    build_source();

    int64_t src_rows = 0, src_bad = 0;
    {
        tsdb_db_t *s = NULL;
        OK(tsdb_open(SRC, &s));
        src_rows = check_visible(s, "m", &src_bad);
        tsdb_close(s);
    }
    if (src_rows != 2 * PERDAY || src_bad != 0)
        FAIL("source itself is wrong: rows=%lld bad=%lld", (long long)src_rows, (long long)src_bad);
    printf("[source]  rows=%lld all values match\n", (long long)src_rows);

    const char *COLS[4] = { "ts", "v", "n", "tag" };

    /* ---- A: kill the process partway through the ts pass ----
     * Restore lands every non-ts block first, so a cut at
     * (all non-ts) + (half the ts blocks) leaves the target with a real
     * PARTIAL visible set: some rows published, the rest not, every value
     * column already on disk. */
    uint64_t n_nonts = 0, n_ts = 0;
    source_block_counts(SRC, "m", COLS, 4, "ts", &n_nonts, &n_ts);
    if (n_ts < 2) FAIL("source has %llu ts blocks; need >= 2 to cut inside the pass",
                       (unsigned long long)n_ts);
    char cutbuf[32];
    snprintf(cutbuf, sizeof(cutbuf), "%llu", (unsigned long long)(n_nonts + n_ts / 2));
    printf("[plan]    source blocks: non-ts=%llu ts=%llu -> crash after %s landed\n",
           (unsigned long long)n_nonts, (unsigned long long)n_ts, cutbuf);

    crash_restore(DSTA, "TSDB_TEST_CRASH_RESTORE_AFTER_BLOCKS", cutbuf, 99);

    if (!tsdb_restore_in_progress(DSTA))
        FAIL(".tsdb_restore is gone after a crashed restore — its ABSENCE is "
             "the only signal that a restore finished, so a crash that clears "
             "it makes an incomplete database look healthy");
    printf("[A]       marker present after the crash\n");
    {
        tsdb_db_t *d = NULL;
        OK(tsdb_open(DSTA, &d));
        int64_t bad = 0;
        int64_t rows = check_visible(d, "m", &bad);
        printf("[A]       %lld of %lld rows visible, %lld with wrong values\n",
               (long long)rows, (long long)src_rows, (long long)bad);
        if (rows <= 0 || rows >= src_rows)
            FAIL("crash left %lld of %lld rows visible — the cut has to land "
                 "INSIDE the ts pass for this phase to say anything",
                 (long long)rows, (long long)src_rows);
        if (bad != 0)
            FAIL("%lld visible row(s) carry values the source never had — a "
                 "partial restore may be SHORT, it may not be WRONG",
                 (long long)bad);
        if (total_dup_keys(DSTA, "m", COLS, 4) != 0)
            FAIL("the crashed restore left a duplicated (ts_min,count) key");
        tsdb_close(d);
    }

    /* ---- C(a): resume the crashed restore to completion ---- */
    {
        tsdb_db_t *d = NULL;
        OK(tsdb_open(DSTA, &d));
        tsdb_restore_report_t rp;
        OK(tsdb_restore_run(d, BK, &rp));
        printf("[A-resume] landed=%llu skipped=%llu seen=%llu\n",
               (unsigned long long)rp.tables[0].blocks_landed,
               (unsigned long long)rp.tables[0].blocks_skipped,
               (unsigned long long)rp.tables[0].blocks_seen);
        if (!rp.complete) FAIL("resume did not complete");
        if (rp.tables[0].blocks_skipped == 0)
            FAIL("resume skipped nothing — it re-landed blocks the crashed run "
                 "had already written, which is the duplication path");
        tsdb_restore_report_free(&rp);

        if (tsdb_restore_in_progress(DSTA)) FAIL("marker survived a completed resume");

        int64_t bad = 0;
        int64_t rows = check_visible(d, "m", &bad);
        printf("[A-resume] rows=%lld bad=%lld dup_keys=%d\n",
               (long long)rows, (long long)bad, total_dup_keys(DSTA, "m", COLS, 4));
        if (rows != src_rows)
            FAIL("resumed restore has %lld rows, source has %lld",
                 (long long)rows, (long long)src_rows);
        if (bad != 0) FAIL("%lld resumed row(s) have wrong values", (long long)bad);
        if (total_dup_keys(DSTA, "m", COLS, 4) != 0)
            FAIL("resume duplicated a block key");
        tsdb_close(d);
    }

    /* ---- B: kill the process between the value columns and ts ---- */
    crash_restore(DSTB, "TSDB_TEST_CRASH_RESTORE_BEFORE_TS", "1", 98);

    if (!tsdb_restore_in_progress(DSTB)) FAIL("marker gone after the pre-ts crash");
    {
        char part[32];
        first_part(DSTB, "m", part, sizeof(part));
        if (!part[0]) FAIL("no partition on disk at all after the pre-ts crash");
        uint32_t nts = idx_block_count(DSTB, "m", part, "ts");
        uint32_t nv  = idx_block_count(DSTB, "m", part, "v");
        uint32_t nn  = idx_block_count(DSTB, "m", part, "n");
        uint32_t ntg = idx_block_count(DSTB, "m", part, "tag");
        printf("[B]       partition %s: ts=%u v=%u n=%u tag=%u blocks\n",
               part, nts, nv, nn, ntg);
        if (nv == 0 || nn == 0 || ntg == 0)
            FAIL("the value columns did not land before the crash — the phase "
                 "is not exercising the ts-last ordering it exists to test");
        if (nts != 0)
            FAIL("<ts>.idx published %u block(s) while the crash landed only "
                 "value columns: ts is the visibility marker, and publishing it "
                 "early is exactly what makes a partition report rows whose "
                 "value columns are missing", nts);

        tsdb_db_t *d = NULL;
        OK(tsdb_open(DSTB, &d));
        int64_t n = row_count(d, "m");
        int64_t bad = 0;
        int64_t rows = check_visible(d, "m", &bad);
        printf("[B]       count(*)=%lld visible=%lld bad=%lld\n",
               (long long)n, (long long)rows, (long long)bad);
        if (n != 0)
            FAIL("count(*) reports %lld rows from a partition whose ts column "
                 "was never published — those rows would read back as "
                 "fabricated zeros", (long long)n);
        if (bad != 0) FAIL("%lld wrong row(s) visible after the pre-ts crash", (long long)bad);
        tsdb_close(d);
    }

    /* ---- C(b): resume from the pre-ts crash ---- */
    {
        tsdb_db_t *d = NULL;
        OK(tsdb_open(DSTB, &d));
        tsdb_restore_report_t rp;
        OK(tsdb_restore_run(d, BK, &rp));
        printf("[B-resume] landed=%llu skipped=%llu\n",
               (unsigned long long)rp.tables[0].blocks_landed,
               (unsigned long long)rp.tables[0].blocks_skipped);
        if (!rp.complete) FAIL("resume from the pre-ts crash did not complete");
        if (rp.tables[0].blocks_skipped == 0)
            FAIL("resume re-landed every value block the crashed run had "
                 "already written");
        tsdb_restore_report_free(&rp);
        if (tsdb_restore_in_progress(DSTB)) FAIL("marker survived a completed resume");

        int64_t bad = 0;
        int64_t rows = check_visible(d, "m", &bad);
        int dups = total_dup_keys(DSTB, "m", COLS, 4);
        printf("[B-resume] rows=%lld bad=%lld dup_keys=%d\n",
               (long long)rows, (long long)bad, dups);
        if (rows != src_rows)
            FAIL("resumed restore has %lld rows, source has %lld",
                 (long long)rows, (long long)src_rows);
        if (bad != 0) FAIL("%lld resumed row(s) have wrong values", (long long)bad);
        if (dups != 0) FAIL("resume duplicated %d block key(s)", dups);

        /* And the deep level agrees the bytes are the source's. */
        tsdb_restore_verify_t vd;
        int vrc = tsdb_restore_verify(d, BK, TSDB_RESTORE_VERIFY_DEEP, &vd);
        printf("[B-resume] verify level=%s rc=%d failed=%d\n",
               vd.level_name ? vd.level_name : "?", vrc, vd.nfailed);
        if (vrc != TSDB_OK || vd.nfailed)
            FAIL("deep verify rejected a resumed restore");
        tsdb_restore_verify_free(&vd);
        tsdb_close(d);
    }

    /* ---- D: rows written AFTER a restore must survive a kill --------------
     *
     * The restore stamps each restored partition with the checkpoint the
     * SOURCE recorded.  Those seqs come from the source table's counter, and
     * the target's counter is 0 for a table the restore just created — so
     * without tsdb_db_raise_commit_seq the target's first commits get seqs at
     * or BELOW the stamp, and the next open's redo_recover_table skips exactly
     * those records for that partition.  Restore into an empty directory and
     * then write is the whole disaster-recovery workflow, so this is the main
     * path, not a corner.
     *
     * The child _exit()s without closing, so the appended rows exist only in
     * the WAL: whether the parent sees them is decided entirely by the
     * checkpoint the restore left behind. */
    {
        setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);   /* deferred flush: rows stay in the WAL */

        /* Build phase D's source here, under the forced mode, so its
         * partitions carry a real checkpoint. */
        {
            tsdb_db_t *a = NULL;
            OK(tsdb_open(SRCD, &a));
            tsdb_col_t c[] = {
                { "ts",  TSDB_TYPE_TIMESTAMP }, { "v",   TSDB_TYPE_FLOAT64 },
                { "n",   TSDB_TYPE_INT64     }, { "tag", TSDB_TYPE_SYMBOL  },
            };
            OK(tsdb_create_table(a, "m", c, 4, "ts"));
            tsdb_table_t *t = NULL;
            OK(tsdb_open_table(a, "m", &t));
            tsdb_batch_t *b = NULL;
            OK(tsdb_batch_begin(t, &b));
            for (int k = 0; k < DROWS; k++) {
                OK(tsdb_batch_row_ts(b, DAY0 + (int64_t)k * 1000000LL));
                OK(tsdb_batch_row_f64(b, 1, val_f(k)));
                OK(tsdb_batch_row_i64(b, 2, val_n(k)));
                OK(tsdb_batch_row_sym(b, 3, TAGS[k % 4]));
                OK(tsdb_batch_row_end(b));
            }
            OK(tsdb_batch_commit(b));
            OK(tsdb_db_flush_all(a));

            char part[32];
            first_part(SRCD, "m", part, sizeof(part));
            char ip[4400];
            snprintf(ip, sizeof(ip), "%s/m/%s/ts.idx", SRCD, part);
            uint64_t sseq = 0;
            tsdb_part_idx_probe(ip, NULL, NULL, NULL, NULL, NULL, NULL, &sseq);
            printf("[D]       source partition %s carries max_seq=%llu\n",
                   part, (unsigned long long)sseq);
            if (sseq == 0)
                FAIL("phase D's source has no WAL checkpoint — the phase would "
                     "not be exercising the stamp it exists to test");
            OK(tsdb_backup_create(a, BKD, NULL));
            tsdb_close(a);
        }

        fflush(NULL);
        pid_t pid = fork();
        if (pid < 0) FAIL("fork: %s", strerror(errno));
        if (pid == 0) {
            tsdb_db_t *d = NULL;
            if (tsdb_open(DSTC, &d) != TSDB_OK) _exit(70);
            if (tsdb_restore_run(d, BKD, NULL) != TSDB_OK) _exit(71);
            tsdb_table_t *t = NULL;
            if (tsdb_open_table(d, "m", &t) != TSDB_OK) _exit(72);
            tsdb_batch_t *b = NULL;
            if (tsdb_batch_begin(t, &b) != TSDB_OK) _exit(73);
            for (int k = 0; k < EXTRA; k++) {
                int64_t g = 1000000 + k;
                if (tsdb_batch_row_ts(b, DAY0 + (int64_t)k * 1000000LL + EXTRA_OFF) != TSDB_OK ||
                    tsdb_batch_row_f64(b, 1, val_f(g)) != TSDB_OK ||
                    tsdb_batch_row_i64(b, 2, val_n(g)) != TSDB_OK ||
                    tsdb_batch_row_sym(b, 3, TAGS[g % 4]) != TSDB_OK ||
                    tsdb_batch_row_end(b) != TSDB_OK) _exit(74);
            }
            if (tsdb_batch_commit(b) != TSDB_OK) _exit(75);
            /* Acked and durable in the WAL.  Die without closing. */
            _exit(0);
        }
        int st = 0;
        if (waitpid(pid, &st, 0) != pid) FAIL("waitpid failed");
        if (!WIFEXITED(st)) FAIL("phase D child died by signal %d", WTERMSIG(st));
        if (WEXITSTATUS(st) != 0)
            FAIL("phase D child failed to set up (exit %d)", WEXITSTATUS(st));
        printf("[D]       child restored + committed %d rows, then _exit(0) "
               "with no close\n", EXTRA);

        tsdb_db_t *d = NULL;
        OK(tsdb_open(DSTC, &d));
        int64_t bad = 0;
        int64_t rows = check_visible(d, "m", &bad);
        printf("[D]       after reopen: rows=%lld (want %d) bad=%lld\n",
               (long long)rows, DROWS + EXTRA, (long long)bad);
        if (rows != DROWS + EXTRA)
            FAIL("%lld of the %d rows committed after the restore did not "
                 "survive the reopen: the restored partition's checkpoint "
                 "covered seqs the target's own WAL was still holding",
                 (long long)(DROWS + EXTRA - rows), EXTRA);
        if (bad != 0) FAIL("%lld row(s) came back with wrong values", (long long)bad);
        tsdb_close(d);
        unsetenv("TSDB_WAL_ONLY_COMMIT");
    }

    rm_rf(SRC); rm_rf(BK); rm_rf(DSTA); rm_rf(DSTB); rm_rf(DSTC);
    rm_rf(SRCD); rm_rf(BKD);
    printf("\n=== test_restore_crash PASSED ===\n");
    return 0;
}
