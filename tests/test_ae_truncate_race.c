/* test_ae_truncate_race.c — a WRITE_BATCH landing in the FULL_PULL truncate
 * window must SURVIVE.
 *
 * THE BUG (4b70d4b made anti-entropy periodic).  ae_attempt_cb's FULL_PULL path
 * truncates the local table then re-pulls from a peer, and the decision to
 * FULL_PULL was gated on a local_stats measurement taken microseconds earlier
 * in tsdb_antientropy_run_candidates.  A WRITE_BATCH that commits in that
 * window is DESTROYED by the truncate and NOT replaced by the pull — the pull
 * is sized to the peer's pre-write rows, and the peer never had the raced
 * write.  The node then sits at count < peer with an equal max_ts, which
 * tsdb_antientropy_decide classifies SKIP_UNSAFE forever.  Before 4b70d4b this
 * raced once per process (5 s after boot); periodic AE races it every 30 s.
 *
 * THE FIX under test: ae_full_pull_truncate_guarded re-measures THIS node
 * immediately before the truncate and ABORTS if a row landed since the empty
 * measurement — a locally-committed row no peer holds is never wiped.
 *
 * DETERMINISTIC, not a race-hope: tsdb_ae_set_test_prewrite_hook lands a write
 * EXACTLY in the window (fired inside the guard, before its re-measure), and we
 * assert the row survives and the truncate did not run.  The pull half needs a
 * live peer and is not exercised — the guard aborts before it.
 */
#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/storage/antientropy.h"
#include "../src/storage/db.h"   /* tsdb_db_flush_all */

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int g_fail = 0;
#define CHECK(cond, ...) do {                                            \
    if (cond) { printf("  PASS: "); printf(__VA_ARGS__); printf("\n"); } \
    else { g_fail++; printf("  FAIL (%s:%d): ", __FILE__, __LINE__);     \
           printf(__VA_ARGS__); printf("\n"); }                          \
} while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) { \
    fprintf(stderr,"FAIL %s:%d rc=%d (%s)\n",__FILE__,__LINE__,_r,tsdb_errstr(_r)); \
    exit(1);} } while (0)

#define TDIR "/tmp/tsdb_test_ae_truncate_race"
#define BASE 1700000000000000000LL

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

static void insert_rows(tsdb_table_t *t, int64_t first, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, BASE + first + i));
        OK(tsdb_batch_row_i64(b, 1, first + i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

static uint64_t rowcount(tsdb_db_t *db, const char *table) {
    uint64_t c = 0; int64_t m = 0;
    OK(tsdb_cluster_local_table_stats(db, table, &c, &m));
    return c;
}

/* The racing WRITE_BATCH — fired by the guard inside the truncate window. */
typedef struct { tsdb_table_t *t; int fired; } racer_t;
static void race_write(void *arg) {
    racer_t *r = (racer_t *)arg;
    if (r->fired) return;         /* land exactly once, like one WRITE_BATCH */
    r->fired = 1;
    insert_rows(r->t, 0, 5);
}

int main(void) {
    printf("=== test_ae_truncate_race ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    OK(tsdb_create_table(db, "e", cols, 2, "ts"));
    tsdb_table_t *t = NULL, *e = NULL;
    OK(tsdb_open_table(db, "t", &t));
    OK(tsdb_open_table(db, "e", &e));

    /* [1] The race: FULL_PULL was decided while "t" measured EMPTY; a
     *     WRITE_BATCH lands in the window (the hook), THEN the guard runs. */
    printf("\n[1] a write in the truncate window is preserved, not wiped\n");
    CHECK(rowcount(db, "t") == 0, "t starts empty (FULL_PULL was gated on this)");
    racer_t racer = { t, 0 };
    tsdb_ae_set_test_prewrite_hook(race_write, &racer);
    int g = tsdb_ae_full_pull_truncate_guarded_for_test(db, "t");
    tsdb_ae_set_test_prewrite_hook(NULL, NULL);
    CHECK(racer.fired == 1, "the racing WRITE_BATCH committed inside the window");
    CHECK(g == 0, "the guard ABORTED the destructive truncate (returned 0)");
    CHECK(rowcount(db, "t") == 5,
          "all 5 locally-committed rows SURVIVED — no silent data loss");

    /* [2] Defense in depth: the guard refuses to truncate any non-empty table,
     *     even with no racing hook — it trusts the re-measurement, not the
     *     stale decision. */
    printf("\n[2] a non-empty table is never truncated by the guard\n");
    int g2 = tsdb_ae_full_pull_truncate_guarded_for_test(db, "t");
    CHECK(g2 == 0 && rowcount(db, "t") == 5,
          "5 rows still present; guard aborts on the fresh measurement");

    /* [3] The normal path still works: a GENUINELY empty table truncates (a
     *     no-op) and the caller is cleared to pull. */
    printf("\n[3] a genuinely-empty table still truncates (normal FULL_PULL)\n");
    CHECK(rowcount(db, "e") == 0, "e is empty");
    int g3 = tsdb_ae_full_pull_truncate_guarded_for_test(db, "e");
    CHECK(g3 == 1 && rowcount(db, "e") == 0,
          "guard truncated (returned 1) and the empty table is still empty");

    /* [4] And after a flush the surviving rows are still counted — the guard's
     *     re-measure sees durable rows, not just memtable ones. */
    printf("\n[4] the guard's measurement includes flushed rows\n");
    OK(tsdb_db_flush_all(db));
    racer_t racer2 = { e, 0 };
    /* prime "e" with a flushed row, then race another in the window */
    insert_rows(e, 100, 3);
    OK(tsdb_db_flush_all(db));
    tsdb_ae_set_test_prewrite_hook(race_write, &racer2);   /* would add 5 to e */
    int g4 = tsdb_ae_full_pull_truncate_guarded_for_test(db, "e");
    tsdb_ae_set_test_prewrite_hook(NULL, NULL);
    CHECK(g4 == 0 && rowcount(db, "e") == 8,
          "3 flushed + 5 raced rows all survive the guard");

    tsdb_close(db);
    rm_rf(TDIR);
    printf("\n=== test_ae_truncate_race: %s ===\n", g_fail ? "FAILED" : "ALL PASS");
    return g_fail ? 1 : 0;
}
