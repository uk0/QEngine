/* test_hier_mirror.c — SDK-created tables join the super-table hierarchy.
 *
 * Every tsdb_create_table now also registers a *data-bearing* super-table: a
 * VTable with NO child rows whose data is its own same-named plain storage.  A
 * SELECT dispatches to the super-table executor, which (finding zero children)
 * reads the same-named table back via the stable-child guard.  This test pins:
 *   (1) a plain create is now visible as a stable (in the synced catalog);
 *   (2) every operator returns identical results through the reroute;
 *   (3) DROP TABLE removes the mirrored stable;
 *   (4) a real `CREATE TABLE d USING st` child is NOT mirrored — so super-table
 *       operations (PARTITION BY tbname) are still correctly rejected on it.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/catalog/stable.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dirent.h>
#include <unistd.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d %s\n",_r,tsdb_errstr(_r));FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMPDIR = "/tmp/tsdb_test_hier";

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

static int64_t q_i64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL; OK(tsdb_query(db, sql, &r));
    ASSERT(tsdb_result_next(r) == 1);
    int64_t v = tsdb_result_i64(r, 0);
    tsdb_result_free(r);
    return v;
}
static double q_f64(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL; OK(tsdb_query(db, sql, &r));
    ASSERT(tsdb_result_next(r) == 1);
    double v = tsdb_result_f64(r, 0);
    tsdb_result_free(r);
    return v;
}
static int query_err(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    if (r) tsdb_result_free(r);
    return rc != TSDB_OK;
}
static void run_ddl(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL;
    OK(tsdb_query(db, sql, &r));
    if (r) tsdb_result_free(r);
}

int main(void) {
    printf("=== test_hier_mirror ===\n");
    rm_rf(TMPDIR);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(TMPDIR, &db));
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    ASSERT(cat);

    /* (1) A plain SDK-style create is now a super-table in the catalog. */
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"val", TSDB_TYPE_FLOAT64},
                          {"qty", TSDB_TYPE_INT64} };
    OK(tsdb_create_table(db, "metrics", cols, 3, "ts"));
    ASSERT(tsdb_stable_exists(cat, "metrics"));
    printf("[1] plain create 'metrics' registered as a super-table\n");

    /* Write a deterministic dataset. */
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, "metrics", &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    double sum = 0; int over = 0;
    for (int i = 0; i < 10000; i++) {
        double v = (double)(i % 1000);
        sum += v; if (v > 500.0) over++;
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, v));
        OK(tsdb_batch_row_i64(b, 2, (int64_t)(i % 7)));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));

    /* (2) Every operator returns correct results THROUGH the super-table reroute. */
    ASSERT(q_i64(db, "SELECT count(*) FROM metrics") == 10000);
    ASSERT(fabs(q_f64(db, "SELECT sum(val) FROM metrics") - sum) < 1.0);
    ASSERT(fabs(q_f64(db, "SELECT avg(val) FROM metrics") - sum/10000.0) < 1e-6);
    ASSERT(q_f64(db, "SELECT min(val) FROM metrics") == 0.0);
    ASSERT(q_f64(db, "SELECT max(val) FROM metrics") == 999.0);
    ASSERT(q_i64(db, "SELECT count(*) FROM metrics WHERE val > 500.0") == over);
    printf("[2] count/sum/avg/min/max/WHERE all correct via stable->self reroute\n");

    /* GROUP BY time bucket still works through the reroute. */
    {
        tsdb_result_t *r = NULL;
        OK(tsdb_query(db, "SELECT count(*) FROM metrics SAMPLE BY 1s", &r));
        int rows = 0; while (tsdb_result_next(r)) rows++;
        tsdb_result_free(r);
        ASSERT(rows > 1);   /* 10s span at 1ms step -> multiple 1s buckets */
        printf("[3] SAMPLE BY produced %d buckets through the reroute\n", rows);
    }

    /* Reopen — the stable must persist (it went to stables.log). */
    tsdb_close(db);
    OK(tsdb_open(TMPDIR, &db));
    cat = tsdb_db_catalog(db);
    ASSERT(tsdb_stable_exists(cat, "metrics"));
    ASSERT(q_i64(db, "SELECT count(*) FROM metrics") == 10000);
    printf("[4] super-table + data survive reopen\n");

    /* (5) DROP removes the mirrored stable. */
    run_ddl(db, "DROP TABLE metrics");
    ASSERT(!tsdb_stable_exists(cat, "metrics"));
    ASSERT(query_err(db, "SELECT count(*) FROM metrics"));
    printf("[5] DROP TABLE removed the mirrored super-table\n");

    /* (6) A real super-table + child: the child must NOT become its own stable,
     *     so PARTITION BY tbname stays rejected on it. */
    run_ddl(db, "CREATE STABLE meters (ts TIMESTAMP, v FLOAT64) TAGS (loc SYMBOL)");
    run_ddl(db, "CREATE TABLE d1 USING meters TAGS ('east')");
    ASSERT(tsdb_stable_exists(cat, "meters"));
    ASSERT(!tsdb_stable_exists(cat, "d1"));        /* child is NOT mirrored */
    ASSERT(query_err(db, "SELECT count(*) FROM d1 PARTITION BY tbname"));  /* rejected */
    printf("[6] real child 'd1' not mirrored; PARTITION BY tbname rejected on it\n");

    tsdb_close(db);
    rm_rf(TMPDIR);
    printf("[PASS] SDK tables join the hierarchy; reroute correct; children unaffected\n");
    return 0;
}
