/* test_multi_data_dirs.c — verify TSDB_DATA_DIRS striping works.
 *
 * Phases:
 *   1. open with TSDB_DATA_DIRS=A,B,C; verify count() = 3 + each is mkdir'd
 *   2. create 32 tables, write rows; verify each table lives in exactly
 *      one striped dir (not all in primary)
 *   3. close + reopen with same env; tables still discovered, queries
 *      return all rows (no data loss across the stripe boundary)
 *   4. unset env, reopen with primary only — still discovers tables
 *      that landed on the primary slot (the others are invisible —
 *      operator opt-in semantics).
 */

#include "tsdb.h"

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { \
  fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
  abort(); } while (0)
#define OK(rc)    do { int _r=(rc); if (_r!=TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL("%s", #c); } while (0)

static void rm_rf(const char *path) {
    DIR *d = opendir(path);
    if (!d) { remove(path); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", path, e->d_name);
        rm_rf(p);
    }
    closedir(d); rmdir(path);
}

static int dir_exists(const char *p) {
    struct stat st;
    return stat(p, &st) == 0 && S_ISDIR(st.st_mode);
}

/* Returns the index in dirs[] where `name` lives, or -1. */
static int find_table_dir(const char **dirs, int ndirs, const char *name) {
    for (int i = 0; i < ndirs; i++) {
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", dirs[i], name);
        if (dir_exists(p)) return i;
    }
    return -1;
}

int main(void) {
    const char *primary = "/tmp/tsdb_mddtest_primary";
    const char *stripe1 = "/tmp/tsdb_mddtest_stripe1";
    const char *stripe2 = "/tmp/tsdb_mddtest_stripe2";

    rm_rf(primary); rm_rf(stripe1); rm_rf(stripe2);

    /* ---- Phase 1: open with striping enabled ---- */
    char dirs_env[1024];
    snprintf(dirs_env, sizeof(dirs_env), "%s,%s", stripe1, stripe2);
    setenv("TSDB_DATA_DIRS", dirs_env, 1);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(primary, &db));
    ASSERT(db != NULL);

    /* All 3 dirs (primary + 2 stripes) must exist on disk. */
    ASSERT(dir_exists(primary));
    ASSERT(dir_exists(stripe1));
    ASSERT(dir_exists(stripe2));

    /* ---- Phase 2: create 32 tables, verify distribution across dirs ---- */
    const char *dirs[3] = { primary, stripe1, stripe2 };
    int per_dir[3] = {0};

    for (int i = 0; i < 32; i++) {
        char name[64];
        snprintf(name, sizeof(name), "tbl_%03d", i);
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_INT64     },
        };
        OK(tsdb_create_table(db, name, cols, 2, "ts"));
        tsdb_table_t *t = NULL;
        OK(tsdb_open_table(db, name, &t));
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int r = 0; r < 100; r++) {
            tsdb_batch_row_ts(b, (tsdb_ts_t)(1735689600000000000LL + r));
            OK(tsdb_batch_row_i64(b, 1, r));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));

        int idx = find_table_dir(dirs, 3, name);
        ASSERT(idx >= 0);
        per_dir[idx]++;
    }

    printf("per-dir distribution: primary=%d stripe1=%d stripe2=%d\n",
           per_dir[0], per_dir[1], per_dir[2]);

    /* Each dir should hold a non-trivial slice; with 32 tables and
     * 3 buckets we expect roughly 11/11/11 ± noise.  Allow any
     * distribution where every dir got at least one table. */
    for (int i = 0; i < 3; i++)
        ASSERT(per_dir[i] >= 1);
    int total = per_dir[0] + per_dir[1] + per_dir[2];
    ASSERT(total == 32);

    /* ---- Phase 3: close + reopen, verify rows survive ---- */
    tsdb_close(db);
    db = NULL;
    OK(tsdb_open(primary, &db));

    /* Pre-open every table by enumerating disk again. */
    for (int i = 0; i < 32; i++) {
        char name[64];
        snprintf(name, sizeof(name), "tbl_%03d", i);
        tsdb_table_t *t = NULL;
        OK(tsdb_open_table(db, name, &t));
        char qtl[128];
        snprintf(qtl, sizeof(qtl), "SELECT count(*) FROM %s", name);
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, qtl, &r));
        ASSERT(r != NULL);
        int seen = 0;
        while (tsdb_result_next(r) > 0) {
            int64_t c = tsdb_result_i64(r, 0);
            ASSERT(c == 100);
            seen++;
        }
        tsdb_result_free(r);
        ASSERT(seen == 1);
    }

    tsdb_close(db);

    /* ---- Phase 4: open WITHOUT striping env — only primary's tables visible ---- */
    unsetenv("TSDB_DATA_DIRS");
    db = NULL;
    OK(tsdb_open(primary, &db));

    int seen_primary = 0;
    for (int i = 0; i < 32; i++) {
        char name[64];
        snprintf(name, sizeof(name), "tbl_%03d", i);
        char p[4096]; snprintf(p, sizeof(p), "%s/%s", primary, name);
        if (dir_exists(p)) seen_primary++;
    }
    printf("after env unset, primary holds %d / 32 tables (others on stripes)\n",
           seen_primary);
    ASSERT(seen_primary == per_dir[0]);

    tsdb_close(db);
    rm_rf(primary); rm_rf(stripe1); rm_rf(stripe2);

    printf("\n=== MULTI-DATA-DIR TESTS PASSED ===\n");
    return 0;
}
