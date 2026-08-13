/* test_drop_race.c — DROP TABLE concurrent with DELETE / TRUNCATE.
 *
 * tsdb_delete_range and tsdb_truncate_table find the table under db->lock, drop
 * db->lock, then take t->compact_mtx (and batch_mu).  Without a lifetime guard,
 * a concurrent DROP could free the table — and destroy those mutexes — in the
 * gap, so the delete/truncate then locks a destroyed mutex: glibc aborts with
 *   pthread_mutex_lock.c: Assertion `e != ESRCH || !robust' failed.
 * (observed crashing a cluster node under drop+delete stress).  The fix makes
 * both take a scan_refs guard so DROP waits.  This test hammers the race; under
 * -fsanitize=address it must complete with no use-after-free and no abort.
 */
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK) { fprintf(stderr,"rc=%d %s\n",_r,tsdb_errstr(_r)); FAIL("rc"); } } while (0)

static const char *TMPDIR = "/tmp/tsdb_test_drop_race";
static const char *TBL = "raceT";
static tsdb_col_t COLS[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_INT64} };

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char s[4096]; snprintf(s, sizeof(s), "%s/%s", p, e->d_name); rm_rf(s);
    }
    closedir(d); rmdir(p);
}

static tsdb_db_t *g_db;
/* Atomic, not volatile: volatile orders nothing between threads, so the plain
 * read below racing main's write was itself a TSan report — harness noise in a
 * run whose entire purpose is reading TSan reports about the engine.  relaxed
 * is sufficient because the flag carries no payload; the pthread_join after the
 * store is what orders the mutators' last query before tsdb_close. */
static _Atomic int g_stop;
#define ITERS 4000

/* Loops DELETE (and occasionally TRUNCATE) on the table — both route through
 * the find→unlock→lock-compact_mtx path now guarded by scan_refs. */
static void *mutate_thread(void *ud) {
    (void)ud;
    int i = 0;
    while (!atomic_load_explicit(&g_stop, memory_order_relaxed)) {
        tsdb_result_t *r = NULL;
        const char *q = (i++ & 7) == 0
            ? "TRUNCATE TABLE raceT"
            : "DELETE FROM raceT WHERE ts < 9000000000000000000";
        (void)tsdb_query(g_db, q, &r);   /* errors (table gone) are fine */
        if (r) tsdb_result_free(r);
    }
    return NULL;
}

int main(void) {
    printf("[TEST] DROP TABLE concurrent with DELETE/TRUNCATE\n");
    rm_rf(TMPDIR);
    OK(tsdb_open(TMPDIR, &g_db));
    OK(tsdb_create_table(g_db, TBL, COLS, 2, "ts"));

    pthread_t m1, m2;
    pthread_create(&m1, NULL, mutate_thread, NULL);
    pthread_create(&m2, NULL, mutate_thread, NULL);

    /* Main thread churns DROP + CREATE so the mutators keep racing a table
     * whose struct (and mutexes) are being freed and rebuilt. */
    for (int i = 0; i < ITERS; i++) {
        (void)tsdb_drop_table(g_db, TBL);
        (void)tsdb_create_table(g_db, TBL, COLS, 2, "ts");
    }
    atomic_store_explicit(&g_stop, 1, memory_order_relaxed);
    pthread_join(m1, NULL);
    pthread_join(m2, NULL);

    /* Survived the race with no abort / use-after-free. */
    tsdb_close(g_db);
    rm_rf(TMPDIR);
    printf("[PASS] %d drop+create cycles vs concurrent delete/truncate, no crash\n", ITERS);
    return 0;
}
