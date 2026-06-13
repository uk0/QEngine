/* test_drop_trash.c — DROP moves storage to .trash (O(1)) + bg GC reclaims it.
 *
 * A mass DROP DATABASE cascades thousands of tsdb_drop_table calls through the
 * single raft apply thread; an inline recursive rm_rf of each froze the apply
 * loop for tens of seconds and stalled all DDL.  DROP now renames the table dir
 * into <data_dir>/.trash (a fast same-fs rename) and a background thread
 * reclaims it.  This pins: after DROP the dir is gone from its table location,
 * the table is no longer openable, the .trash is reclaimed by the GC, and the
 * drop survives a reopen (no resurrection, no leftover storage).
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d\n",_r);FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_drop_trash";

static int dir_exists(const char *p) {
    struct stat st;
    return stat(p, &st) == 0 && S_ISDIR(st.st_mode);
}
static int dir_entry_count(const char *p) {
    DIR *d = opendir(p);
    if (!d) return 0;
    int n = 0; struct dirent *e;
    while ((e = readdir(d))) if (e->d_name[0] != '.') n++;
    closedir(d);
    return n;
}
static void rm_tree(const char *p) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c); }

static void make_table(tsdb_db_t *db, const char *name) {
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    OK(tsdb_create_table(db, name, cols, 2, "ts"));
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, name, &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < 200; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    OK(tsdb_db_flush_all(db));   /* force an on-disk partition so there's a real dir */
}

int main(void) {
    printf("=== test_drop_trash ===\n");
    rm_tree(TMP);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TMP, &db));

    char dir[512], trash[512];
    snprintf(dir,   sizeof(dir),   "%s/t1", TMP);
    snprintf(trash, sizeof(trash), "%s/.trash", TMP);

    make_table(db, "t1");
    ASSERT(dir_exists(dir));            /* real on-disk table dir */

    OK(tsdb_drop_table(db, "t1"));
    ASSERT(!dir_exists(dir));           /* moved out of the table location (fast rename) */
    {
        tsdb_table_t *t2 = NULL;
        ASSERT(tsdb_open_table(db, "t1", &t2) != TSDB_OK);  /* no longer openable */
    }
    /* Read path on a dropped table must surface a CLEAN NOTFOUND, never an
     * "internal error".  Across cluster nodes the dropped table's on-disk
     * dir can be in different teardown states (already trashed vs. a
     * residual half-torn schema), so exec_select must normalize ANY re-open
     * failure to NOTFOUND rather than leaking tsdb_schema_open's specific rc
     * (CORRUPT/IO) — otherwise the wire rc diverges per node. */
    {
        tsdb_result_t *qr = NULL;
        int qrc = tsdb_query(db, "SELECT count(*) FROM t1", &qr);
        if (qr) tsdb_result_free(qr);
        if (qrc != TSDB_ERR_NOTFOUND) {
            fprintf(stderr, "post-drop SELECT rc=%d (want NOTFOUND=%d)\n",
                    qrc, TSDB_ERR_NOTFOUND);
            FAIL("post-drop read did not return clean NOTFOUND");
        }
    }
    printf("  drop: dir left t1's location, not openable, read → NOTFOUND\n");

    /* The bg GC reclaims .trash within a couple of seconds. */
    int reclaimed = 0;
    for (int i = 0; i < 50; i++) {
        if (!dir_exists(trash) || dir_entry_count(trash) == 0) { reclaimed = 1; break; }
        usleep(200 * 1000);
    }
    ASSERT(reclaimed);
    printf("  bg GC reclaimed .trash\n");

    /* Drop several at once (mass-drop shape) — all gone, GC drains. */
    for (int i = 0; i < 8; i++) { char n[16]; snprintf(n, sizeof(n), "m%d", i); make_table(db, n); }
    for (int i = 0; i < 8; i++) { char n[16]; snprintf(n, sizeof(n), "m%d", i); OK(tsdb_drop_table(db, n)); }
    for (int i = 0; i < 8; i++) {
        char p[512]; snprintf(p, sizeof(p), "%s/m%d", TMP, i);
        ASSERT(!dir_exists(p));
    }
    for (int i = 0; i < 60; i++) {
        if (!dir_exists(trash) || dir_entry_count(trash) == 0) break;
        usleep(200 * 1000);
    }
    ASSERT(!dir_exists(trash) || dir_entry_count(trash) == 0);
    printf("  mass drop (8 tables): all gone, .trash drained\n");

    /* Reopen — drops are durable, no leftover storage resurrects a table. */
    tsdb_close(db);
    OK(tsdb_open(TMP, &db));
    {
        tsdb_table_t *t2 = NULL;
        ASSERT(tsdb_open_table(db, "t1", &t2) != TSDB_OK);
    }
    ASSERT(!dir_exists(dir));
    tsdb_close(db);
    printf("  reopen: t1 stays dropped, no leftover dir\n");

    rm_tree(TMP);
    printf("[PASS] DROP trashes storage off the apply path; bg GC reclaims; durable\n");
    return 0;
}
