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

static void make_and_flush(tsdb_db_t *db, const char *name) {
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    OK(tsdb_create_table(db, name, cols, 2, "ts"));
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, name, &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 100; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
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

int main(void) {
    printf("=== test_compactor_memo ===\n");
    run_memo_enabled();
    run_memo_disabled();
    printf("[PASS] all\n");
    return 0;
}
