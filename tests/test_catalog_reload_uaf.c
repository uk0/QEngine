/* test_catalog_reload_uaf.c — the catalog-reconcile tick frees a catalog that
 * readers are still holding.
 *
 * Mechanism.  Every read path starts with
 *
 *     tsdb_catalog_t *cat = tsdb_db_catalog(db);      // exec.c:6382, 5849, 8609
 *
 * and then uses `cat` for the rest of the statement — for a scatter over a
 * 122M-row super-table that is seconds.  `tsdb_db_catalog()` is a bare
 * `return db->catalog;` (db.c:3075): no refcount, no lock, no epoch.
 *
 * Meanwhile tsdb_delwm_reassert_thread (db_cluster.c:2073) calls
 * tsdb_catalog_reconcile_from_peers() every 30 s.  When that learns anything
 * it calls tsdb_db_reload_catalog() (catalog_sync.c:387), which opens a fresh
 * catalog, swaps the pointer, and then FREES the old object
 * (db.c:709-730) while asserting in a comment that "the overlap window is
 * microseconds".  It is not: the free happens after tsdb_catalog_close()
 * joins the catalog's compaction thread, which only re-checks its stop flag
 * every 50 ms, so the old object dies tens of milliseconds AFTER the swap and
 * any reader still inside a statement dereferences freed memory —
 * c->lock (pthread_mutex_lock on a freed mutex) and the hmap buckets.
 *
 * This is why every segfault in the 4-day incident log is immediately
 * preceded by `[catalog] v2 shadow (TSDB_CATALOG_V2): mirrored N, skipped 0`:
 * that line is printed from shadow_v2_resync() at the END of
 * tsdb_catalog_open() (catalog.c:1020), i.e. from inside the reload, a
 * handful of milliseconds before the old catalog is freed.  It is the reload
 * marker, not the culprit — but the flag makes the reload much slower and
 * therefore much wider.
 *
 * Two parts:
 *   [1] deterministic — hold the pointer across a reload, exactly as
 *       exec_select does, and use it afterwards.  Under ASan this is a
 *       heap-use-after-free; without ASan the identity assert still fails
 *       on the unfixed tree.
 *   [2] natural concurrency — SELECT count(*) loops against a reload loop,
 *       which is the production shape.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/catalog/stable.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <time.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d %s\n",_r,tsdb_errstr(_r));FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMPDIR = "/tmp/tsdb_test_catreload_uaf";

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name,".")||!strcmp(e->d_name,"..")) continue;
        char s[4096]; snprintf(s,sizeof(s),"%s/%s",p,e->d_name); rm_rf(s);
    }
    closedir(d); rmdir(p);
}

static double now_s(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

/* What tsdb_catalog_reconcile_from_peers -> filtered_append_log does: append a
 * `+stable` record straight into stables.log, then reload so the in-memory
 * catalog picks it up. */
static void peer_learns_stable(const char *name) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/catalog/stables.log", TMPDIR);
    FILE *f = fopen(path, "ab");
    ASSERT(f);
    fprintf(f, "+stable\t%s\t1\t0\t0\t100\tts:1\n", name);
    fclose(f);
}

static tsdb_db_t *g_db;
static volatile int g_stop;
static volatile long g_queries;
static volatile long g_reloads;

static void *reader_main(void *ud) {
    (void)ud;
    while (!g_stop) {
        tsdb_result_t *r = NULL;
        int rc = tsdb_query(g_db, "SELECT count(*) FROM metrics", &r);
        if (r) tsdb_result_free(r);
        if (rc == TSDB_OK) __sync_fetch_and_add(&g_queries, 1);
    }
    return NULL;
}

/* The exec.c shape, isolated: grab the pointer, then keep using it. */
static void *catreader_main(void *ud) {
    (void)ud;
    while (!g_stop) {
        tsdb_catalog_t *cat = tsdb_db_catalog(g_db);   /* exec.c:6382 */
        if (!cat) continue;
        for (int i = 0; i < 200 && !g_stop; i++) {
            if (tsdb_stable_exists(cat, "metrics")) {  /* exec.c:6384 */
                tsdb_stable_t st;
                if (tsdb_stable_get(cat, "metrics", &st) == TSDB_OK) {
                    tsdb_child_table_t *ch = NULL; size_t n = 0;
                    /* exec_stable_select:5884 — still on the SAME `cat` */
                    if (tsdb_child_table_list(cat, st.name, &ch, &n) == TSDB_OK)
                        free(ch);
                }
            }
        }
    }
    return NULL;
}

static void *reloader_main(void *ud) {
    (void)ud;
    while (!g_stop) {
        if (tsdb_db_reload_catalog(g_db) == TSDB_OK)
            __sync_fetch_and_add(&g_reloads, 1);
    }
    return NULL;
}

int main(void) {
    printf("=== test_catalog_reload_uaf ===\n");
    /* The production cluster runs with the v2 shadow armed. */
    setenv("TSDB_CATALOG_V2", "1", 1);
    rm_rf(TMPDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TMPDIR, &db));
    g_db = db;

    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"val", TSDB_TYPE_FLOAT64} };
    OK(tsdb_create_table(db, "metrics", cols, 2, "ts"));
    {
        tsdb_table_t *t = NULL; OK(tsdb_open_table(db, "metrics", &t));
        tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < 20000; i++) {
            OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000));
            OK(tsdb_batch_row_f64(b, 1, (double)(i % 997)));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
    }

    /* ---- [1] a reader holds the catalog across one reconcile tick -------- */
    {
        tsdb_catalog_t *cat = tsdb_db_catalog(db);      /* exec_select's line 1 */
        ASSERT(cat);
        ASSERT(tsdb_stable_exists(cat, "metrics"));

        peer_learns_stable("learned_from_peer");
        OK(tsdb_db_reload_catalog(db));                 /* the 30 s tick */

        /* The reload must not have destroyed the object this reader is still
         * holding.  On the unfixed tree db->catalog is a NEW object and `cat`
         * points at freed memory. */
        ASSERT(tsdb_db_catalog(db) == cat);

        /* ... and the reader keeps using it, exactly as exec_stable_select
         * does after its own tsdb_db_catalog().  ASan flags the unfixed tree
         * here even if the identity assert above is removed. */
        ASSERT(tsdb_stable_exists(cat, "metrics"));
        tsdb_stable_t st;
        OK(tsdb_stable_get(cat, "metrics", &st));
        tsdb_child_table_t *ch = NULL; size_t nch = 0;
        OK(tsdb_child_table_list(cat, st.name, &ch, &nch));
        free(ch);

        /* The reload still has to do its job: the learned record is visible
         * through that same pointer. */
        ASSERT(tsdb_stable_exists(cat, "learned_from_peer"));
        printf("[1] catalog survives a reload a reader is holding, and the "
               "learned record is visible through it\n");
    }

    /* ---- [2] the production shape: queries against a reload loop --------- */
    {
        pthread_t rd[3], cr[2], rl;
        g_stop = 0; g_queries = 0; g_reloads = 0;
        for (int i = 0; i < 3; i++) pthread_create(&rd[i], NULL, reader_main, NULL);
        for (int i = 0; i < 2; i++) pthread_create(&cr[i], NULL, catreader_main, NULL);
        pthread_create(&rl, NULL, reloader_main, NULL);

        double t0 = now_s();
        while (now_s() - t0 < 3.0) {
            struct timespec s = { 0, 20 * 1000 * 1000 };
            nanosleep(&s, NULL);
        }
        g_stop = 1;
        for (int i = 0; i < 3; i++) pthread_join(rd[i], NULL);
        for (int i = 0; i < 2; i++) pthread_join(cr[i], NULL);
        pthread_join(rl, NULL);
        printf("[2] %ld queries + %ld reloads concurrently, no use-after-free\n",
               g_queries, g_reloads);
        ASSERT(g_queries > 0);
        ASSERT(g_reloads > 0);
    }

    tsdb_close(db);
    rm_rf(TMPDIR);
    printf("=== test_catalog_reload_uaf PASSED ===\n");
    return 0;
}
