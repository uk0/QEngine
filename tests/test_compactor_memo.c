/* test_compactor_memo.c — per-table mtime memo in the compactor.
 *
 * Verifies: on the SECOND run_once call after a no-op cycle, every table
 * whose dir mtime hasn't changed is fast-skipped via the memo (counted by
 * qengine_compaction_memo_skipped_total).  Disabling the memo via
 * TSDB_COMPACTION_MEMO=0 returns to the legacy per-cycle full enumeration.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/compaction.h"
#include "../src/server/metrics.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <glob.h>        /* enumerate partition dirs without <dirent.h> (its DIR
                            type name collides with this file's DIR constant) */
#include <sys/stat.h>
#include <time.h>

#define ASSERT(cond) do { if (!(cond)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #cond, __FILE__, __LINE__); abort(); } } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) { \
    fprintf(stderr, "rc=%d at %s:%d\n", _r, __FILE__, __LINE__); abort(); } } while (0)

static const char *DIR = "/tmp/tsdb_test_compactor_memo";

static void rm_tree(const char *p) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c); }

/* Parse a single Prometheus counter value out of the rendered text. */
static uint64_t metric_counter(const char *name) {
    size_t len = 0;
    char *txt = tsdb_metrics_render(&len);
    if (!txt) return 0;
    uint64_t val = 0;
    char needle[128]; snprintf(needle, sizeof(needle), "\n%s ", name);
    const char *p = strstr(txt, needle);
    if (p) { p += strlen(needle); val = (uint64_t)strtoull(p, NULL, 10); }
    free(txt);
    return val;
}

/* 100 rows at ts = (base+1..base+100) ms.  Every base used here keeps the rows
 * inside the same UTC day, i.e. the same partition dir. */
static void write_rows(tsdb_db_t *db, const char *name, int base) {
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, name, &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 100; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(base + i + 1) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

static void make_and_flush(tsdb_db_t *db, const char *name) {
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    OK(tsdb_create_table(db, name, cols, 2, "ts"));
    write_rows(db, name, 0);
}

static void run_memo_enabled(void) {
    printf("--- memo enabled (default) ---\n");
    rm_tree(DIR);
    unsetenv("TSDB_COMPACTION_MEMO");
    tsdb_metrics_init();

    tsdb_db_t *db = NULL; OK(tsdb_open(DIR, &db));
    make_and_flush(db, "tA");
    make_and_flush(db, "tB");
    OK(tsdb_db_flush_all(db));

    tsdb_compactor_opts_t opts = { .worker_threads = -1 };  /* manual: we drive
                                  run_once, no background worker to race the memo */
    tsdb_compactor_t *cpt = NULL;
    OK(tsdb_compactor_start(db, &opts, &cpt));

    uint64_t before = metric_counter("qengine_compaction_memo_skipped_total");

    /* Cycle 1: memo empty, so neither table is skipped via memo.  Both
     * have fresh on-disk partitions (< 60 s) so the existing hot-skip
     * prevents any real compaction work — but the memo entries DO get
     * recorded with the post-write mtime. */
    OK(tsdb_compactor_run_once(cpt));
    uint64_t after1 = metric_counter("qengine_compaction_memo_skipped_total");
    ASSERT(after1 == before);     /* no memo skips on the very first cycle */

    /* Cycle 2: nothing new written → both table dirs have unchanged mtime
     * → both skipped via the memo, counter advances by exactly 2. */
    OK(tsdb_compactor_run_once(cpt));
    uint64_t after2 = metric_counter("qengine_compaction_memo_skipped_total");
    ASSERT(after2 == before + 2);
    printf("  cycle1 skipped=%llu  cycle2 skipped=%llu (+2 expected)\n",
           (unsigned long long)(after1 - before), (unsigned long long)(after2 - before));

    tsdb_compactor_stop(cpt);
    tsdb_close(db);
    rm_tree(DIR);
    printf("  [PASS] memo skips tA + tB when their mtimes are unchanged\n");
}

static void run_memo_disabled(void) {
    printf("--- memo disabled (TSDB_COMPACTION_MEMO=0) ---\n");
    rm_tree(DIR);
    setenv("TSDB_COMPACTION_MEMO", "0", 1);
    tsdb_metrics_init();

    tsdb_db_t *db = NULL; OK(tsdb_open(DIR, &db));
    make_and_flush(db, "tA");
    OK(tsdb_db_flush_all(db));

    tsdb_compactor_opts_t opts = { .worker_threads = -1 };  /* manual: we drive
                                  run_once, no background worker to race the memo */
    tsdb_compactor_t *cpt = NULL;
    OK(tsdb_compactor_start(db, &opts, &cpt));

    uint64_t before = metric_counter("qengine_compaction_memo_skipped_total");
    OK(tsdb_compactor_run_once(cpt));
    OK(tsdb_compactor_run_once(cpt));
    uint64_t after  = metric_counter("qengine_compaction_memo_skipped_total");
    ASSERT(after == before);   /* memo path never fired */

    tsdb_compactor_stop(cpt);
    tsdb_close(db);
    rm_tree(DIR);
    unsetenv("TSDB_COMPACTION_MEMO");
    printf("  [PASS] no memo skips when TSDB_COMPACTION_MEMO=0\n");
}

/* First partition subdir under a table dir.  Uses glob to avoid <dirent.h>'s
 * DIR type clashing with this file's DIR constant. */
static int first_partition_dir(const char *tbl_dir, char *out, size_t outsz) {
    char pat[600]; snprintf(pat, sizeof(pat), "%s/*", tbl_dir);
    glob_t g; memset(&g, 0, sizeof(g));
    int found = 0;
    if (glob(pat, 0, NULL, &g) == 0) {
        for (size_t i = 0; i < g.gl_pathc && !found; i++) {
            struct stat st;
            if (stat(g.gl_pathv[i], &st) == 0 && S_ISDIR(st.st_mode)) {
                snprintf(out, outsz, "%s", g.gl_pathv[i]);
                found = 1;
            }
        }
    }
    globfree(&g);
    return found;
}

/* st_mtime is second-granular, so a second flush landing inside the same wall
 * second as the first is indistinguishable from no flush at all.  Wait past it
 * so "unchanged" can only mean "never bumped". */
static void wait_past_second(time_t sec) {
    struct timespec nap = { 0, 20 * 1000 * 1000 };   /* 20 ms */
    while (time(NULL) <= sec) nanosleep(&nap, NULL);
}

/* Regression for the table-dir-mtime bug: a steadily-appended table must NOT be
 * memo-skipped.  Keying on max(partition mtime) re-checks tA after its partition
 * grows, while the untouched idle tB is still skipped.
 *
 * The append here is a REAL flush into the partition tA already has — not a
 * synthetic utimensat.  That distinction is the whole point of the test: the
 * simulated version asserted the invariant while BYPASSING the code that has to
 * uphold it, so it stayed green through a publish rewrite that stopped bumping
 * the dir at all.  Driving the flush makes it fail unless
 * col_idx_append_publish() still moves the partition dir's mtime — the same
 * side effect db_cluster.c's anti-entropy COLD gate reads. */
static void run_memo_detects_partition_append(void) {
    printf("--- memo re-checks a table whose partition grew (REAL flush) ---\n");
    rm_tree(DIR);
    unsetenv("TSDB_COMPACTION_MEMO");
    tsdb_metrics_init();

    tsdb_db_t *db = NULL; OK(tsdb_open(DIR, &db));
    make_and_flush(db, "tA");
    make_and_flush(db, "tB");
    OK(tsdb_db_flush_all(db));

    tsdb_compactor_opts_t opts = { .worker_threads = -1 };  /* manual mode */
    tsdb_compactor_t *cpt = NULL;
    OK(tsdb_compactor_start(db, &opts, &cpt));

    uint64_t before = metric_counter("qengine_compaction_memo_skipped_total");
    OK(tsdb_compactor_run_once(cpt));   /* cycle 1: record memo, no skips */
    uint64_t after1 = metric_counter("qengine_compaction_memo_skipped_total");
    ASSERT(after1 == before);

    char ta_dir[512];  snprintf(ta_dir, sizeof(ta_dir), "%s/tA", DIR);
    char ta_part[640]; ASSERT(first_partition_dir(ta_dir, ta_part, sizeof(ta_part)));
    char ta_idx[700];  snprintf(ta_idx, sizeof(ta_idx), "%s/ts.idx", ta_part);

    struct stat tbl_b, part_b, idx_b;
    ASSERT(stat(ta_dir,  &tbl_b)  == 0);
    ASSERT(stat(ta_part, &part_b) == 0);
    ASSERT(stat(ta_idx,  &idx_b)  == 0);

    wait_past_second(part_b.st_mtime);
    time_t flush_at = time(NULL);

    /* REAL flush of 100 more rows into that SAME partition. */
    write_rows(db, "tA", 100);
    OK(tsdb_db_flush_all(db));

    struct stat tbl_a, part_a, idx_a;
    ASSERT(stat(ta_dir,  &tbl_a)  == 0);
    ASSERT(stat(ta_part, &part_a) == 0);
    ASSERT(stat(ta_idx,  &idx_a)  == 0);

    printf("  ts.idx %lld -> %lld bytes, inode %s;  partdir mtime %ld -> %ld"
           " (flush at %ld);  tabledir mtime %ld -> %ld\n",
           (long long)idx_b.st_size, (long long)idx_a.st_size,
           idx_b.st_ino == idx_a.st_ino ? "SAME (in-place publish)"
                                        : "CHANGED (temp+rename publish)",
           (long)part_b.st_mtime, (long)part_a.st_mtime, (long)flush_at,
           (long)tbl_b.st_mtime, (long)tbl_a.st_mtime);

    ASSERT(idx_a.st_size > idx_b.st_size);            /* the append really landed */

    /* The table dir does NOT move — which is why the memo key has to be
     * max(partition mtime) and not the table dir's own mtime. */
    ASSERT(tbl_a.st_mtime == tbl_b.st_mtime);

    /* The partition dir DOES move.  temp+rename got this for free by creating
     * and removing a dirent; an in-place publish has to do it explicitly. */
    ASSERT(part_a.st_mtime >= flush_at);
    ASSERT(part_a.st_mtime >  part_b.st_mtime);

    /* Cycle 2: tB idle → memo-skipped; tA's partition moved → re-checked, NOT
     * skipped.  Counter advances by exactly 1 (with a frozen partition mtime it
     * would be +2, and compaction would never look at tA again). */
    OK(tsdb_compactor_run_once(cpt));
    uint64_t after2 = metric_counter("qengine_compaction_memo_skipped_total");
    printf("  cycle2 skipped=%llu (expect +1: tB idle skipped, tA re-checked)\n",
           (unsigned long long)(after2 - before));
    ASSERT(after2 == before + 1);

    tsdb_compactor_stop(cpt);
    tsdb_close(db);
    rm_tree(DIR);
    printf("  [PASS] a real partition append busts the memo; idle table still skipped\n");
}

int main(void) {
    printf("=== test_compactor_memo ===\n");
    run_memo_enabled();
    run_memo_disabled();
    run_memo_detects_partition_append();
    printf("[PASS] all\n");
    return 0;
}
