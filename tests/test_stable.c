/* test_stable.c — Super-table + child-table catalog round-trip tests. */

#include "tsdb.h"
#include "../src/catalog/stable.h"

/* Accessor implemented in db.c */
struct tsdb_catalog;
struct tsdb_catalog *tsdb_db_catalog(tsdb_db_t *db);

#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); abort(); } while (0)
#define OK(rc) do { int _rc = (rc); if (_rc != TSDB_OK) FAIL("rc=%d (%s)", _rc, tsdb_errstr(_rc)); } while (0)
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

int main(void) {
    const char *dir = "/tmp/tsdb_test_stable";
    rm_rf(dir);

    printf("=== tsdb STable tests ===\n");

    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    struct tsdb_catalog *cat = tsdb_db_catalog(db);
    ASSERT(cat != NULL);

    /* ─ Phase 1 ─ CREATE STABLE */
    printf("\n[1] create super table 'meters'\n");
    tsdb_stable_t st;
    memset(&st, 0, sizeof(st));
    snprintf(st.name, sizeof(st.name), "%s", "meters");
    snprintf(st.cols[0].name, sizeof(st.cols[0].name), "%s", "ts");
    st.cols[0].type = TSDB_TYPE_TIMESTAMP;
    snprintf(st.cols[1].name, sizeof(st.cols[1].name), "%s", "current");
    st.cols[1].type = TSDB_TYPE_FLOAT64;
    snprintf(st.cols[2].name, sizeof(st.cols[2].name), "%s", "voltage");
    st.cols[2].type = TSDB_TYPE_INT64;
    st.ncols = 3;
    st.ts_col_idx = 0;
    snprintf(st.tag_cols[0].name, sizeof(st.tag_cols[0].name), "%s", "location");
    st.tag_cols[0].type = TSDB_TYPE_SYMBOL;
    snprintf(st.tag_cols[1].name, sizeof(st.tag_cols[1].name), "%s", "groupId");
    st.tag_cols[1].type = TSDB_TYPE_INT64;
    st.ntag_cols = 2;

    OK(tsdb_stable_create(cat, &st));
    ASSERT(tsdb_stable_exists(cat, "meters"));

    tsdb_stable_t fetched;
    OK(tsdb_stable_get(cat, "meters", &fetched));
    ASSERT(fetched.ncols == 3);
    ASSERT(fetched.ntag_cols == 2);
    ASSERT(fetched.ts_col_idx == 0);
    ASSERT(strcmp(fetched.tag_cols[0].name, "location") == 0);
    ASSERT(fetched.tag_cols[1].type == TSDB_TYPE_INT64);
    printf("  PASS: created + fetched\n");

    /* ─ Phase 2 ─ CREATE TABLE d1001/d1002 USING meters */
    printf("\n[2] create 3 child tables\n");
    const char *names[] = {"d1001", "d1002", "d1003"};
    const char *locs[]  = {"us-east-1", "us-west-2", "us-east-1"};
    int         gids[]  = {42, 17, 99};
    for (int i = 0; i < 3; i++) {
        tsdb_child_table_t ct;
        memset(&ct, 0, sizeof(ct));
        snprintf(ct.name,        sizeof(ct.name),        "%s", names[i]);
        snprintf(ct.stable_name, sizeof(ct.stable_name), "%s", "meters");
        ct.ntags = 2;
        ct.tags[0].type = TSDB_TYPE_SYMBOL;
        snprintf(ct.tags[0].v.s, sizeof(ct.tags[0].v.s), "%s", locs[i]);
        ct.tags[1].type = TSDB_TYPE_INT64;
        ct.tags[1].v.i  = gids[i];
        OK(tsdb_child_table_create(cat, &ct));
    }
    printf("  PASS: 3 child tables registered\n");

    /* ─ Phase 3 ─ list and tag filter ─ */
    printf("\n[3] list children of 'meters'\n");
    tsdb_child_table_t *arr = NULL;
    size_t n = 0;
    OK(tsdb_child_table_list(cat, "meters", &arr, &n));
    ASSERT(n == 3);

    /* count children with location='us-east-1' */
    int east_count = 0;
    for (size_t i = 0; i < n; i++) {
        if (strcmp(arr[i].tags[0].v.s, "us-east-1") == 0) east_count++;
    }
    ASSERT(east_count == 2);
    printf("  PASS: 3 total, 2 in us-east-1\n");
    free(arr);

    /* ─ Phase 4 ─ persistence (reopen db, catalog must replay) */
    printf("\n[4] persistence via reopen\n");
    tsdb_close(db);
    OK(tsdb_open(dir, &db));
    cat = tsdb_db_catalog(db);
    ASSERT(cat != NULL);
    ASSERT(tsdb_stable_exists(cat, "meters"));
    OK(tsdb_stable_get(cat, "meters", &fetched));
    ASSERT(fetched.ncols == 3);
    ASSERT(fetched.ntag_cols == 2);

    tsdb_child_table_t ct_loaded;
    OK(tsdb_child_table_get(cat, "d1001", &ct_loaded));
    ASSERT(strcmp(ct_loaded.stable_name, "meters") == 0);
    ASSERT(ct_loaded.ntags == 2);
    ASSERT(strcmp(ct_loaded.tags[0].v.s, "us-east-1") == 0);
    ASSERT(ct_loaded.tags[1].v.i == 42);
    printf("  PASS: reopen replayed all entries correctly\n");

    /* ─ Phase 5 ─ cascade drop */
    printf("\n[5] DROP STABLE cascades to all children\n");
    OK(tsdb_stable_drop(cat, "meters"));
    ASSERT(!tsdb_stable_exists(cat, "meters"));

    tsdb_child_table_t probe;
    ASSERT(tsdb_child_table_get(cat, "d1001", &probe) != TSDB_OK);
    ASSERT(tsdb_child_table_get(cat, "d1002", &probe) != TSDB_OK);
    ASSERT(tsdb_child_table_get(cat, "d1003", &probe) != TSDB_OK);
    printf("  PASS: stable dropped + 3 children cascaded\n");

    tsdb_close(db);
    rm_rf(dir);

    printf("\n=== ALL STABLE TESTS PASSED ===\n");
    return 0;
}
