/* test_stable_open_fail.c — regression for task #174 empty-result-set bug.
 *
 * Bug: on a cluster, `SELECT count(*) FROM <t>` returns an EMPTY result set
 * (0 rows, NOT count=0, NOT error -5) for a table whose partition data is on
 * the queried node's disk.
 *
 * Mechanism (root cause): every plain/replicated table is mirrored into the
 * catalog as a CHILDLESS super-table.  A SELECT therefore dispatches to
 * exec_stable_select, which (finding zero children) falls back to
 * `tsdb_open_table(st->name)` and reads the same-named storage as the sole
 * child.  If that fallback open FAILS for a table that nonetheless has
 * storage on disk (e.g. a replica that received partition .col/.idx files but
 * whose schema.bin / WAL is briefly unreadable, or any transient open error),
 * the stable path SWALLOWS the failure: nchildren stays 0 and the function
 * returns TSDB_OK with an EMPTY result (r->ncols == 0).  Because the table is
 * still resolvable as a stable, the caller sees 0 rows — never the -5 NOTFOUND
 * that the plain path would surface — exactly the server symptom.
 *
 * This test reproduces that state in-process WITHOUT a cluster:
 *   1. Create table `m`, write+flush rows so a real partition lands on disk.
 *   2. Confirm count(*) works (it is a stable mirror with on-disk storage).
 *   3. Force the fallback open to FAIL while the partition stays on disk and
 *      the stable stays in the catalog (delete schema.bin — the same state a
 *      replica is in when it has data files but not schema.bin yet).
 *   4. Re-open the db (drops the in-memory table handle so the read path must
 *      go through the stable fallback again).
 *   5. ASSERT the bug: a table whose storage exists on disk must NOT yield an
 *      empty result set.  Pre-fix this returns 0 rows; post-fix it must either
 *      return the rows or surface an error — never silently empty.
 */

#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/catalog/stable.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d %s\n",_r,tsdb_errstr(_r));FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TDIR = "/tmp/tsdb_test_stable_open_fail";

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

/* Does this table directory hold at least one on-disk partition (YYYYMMDD)? */
static int has_partition(const char *table_dir) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e; int found = 0;
    while ((e = readdir(d))) {
        size_t n = strlen(e->d_name);
        if (n != 8 && n != 10) continue;
        int all_digit = 1;
        for (size_t i = 0; i < n; i++) if (e->d_name[i] < '0' || e->d_name[i] > '9') { all_digit = 0; break; }
        if (all_digit) { found = 1; break; }
    }
    closedir(d);
    return found;
}

/* Run count(*) and report (rc, result_rows, value). */
static int run_count(tsdb_db_t *db, const char *tbl, int *out_rows, int64_t *out_val) {
    char q[128]; snprintf(q, sizeof q, "SELECT count(*) FROM %s", tbl);
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, q, &r);
    if (rc != TSDB_OK) { *out_rows = -1; *out_val = -1; return rc; }
    int n = 0; int64_t v = 0;
    while (tsdb_result_next(r) == 1) { if (n == 0) v = tsdb_result_i64(r, 0); n++; }
    *out_rows = n; *out_val = v;
    tsdb_result_free(r);
    return TSDB_OK;
}

int main(void) {
    printf("=== test_stable_open_fail (task #174 empty-result-set) ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_catalog_t *cat = tsdb_db_catalog(db);
    ASSERT(cat);

    /* 1. Create + write + flush so a real partition lands on disk. */
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    OK(tsdb_create_table(db, "m", cols, 2, "ts"));
    ASSERT(tsdb_stable_exists(cat, "m"));      /* mirrored as a childless stable */

    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, "m", &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    const int N = 5000;
    for (int i = 0; i < N; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
    /* Default mode is flush-on-commit, so a partition is on disk now.  Verify;
     * if the build defaults to deferred flush, close+reopen drains it. */
    {
        char td[4096]; snprintf(td, sizeof(td), "%s/m", TDIR);
        if (!has_partition(td)) {
            tsdb_close(db);
            OK(tsdb_open(TDIR, &db));
            cat = tsdb_db_catalog(db);
            ASSERT(has_partition(td));
        }
    }

    /* 2. Baseline: count(*) works while storage + handle are healthy. */
    {
        int rows = -9; int64_t val = -9;
        int rc = run_count(db, "m", &rows, &val);
        printf("[baseline] count(*) rc=%d result_rows=%d value=%lld\n", rc, rows, (long long)val);
        ASSERT(rc == TSDB_OK && rows == 1 && val == N);
    }

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/m", TDIR);
    ASSERT(has_partition(tbl_dir));            /* partition really is on disk */

    tsdb_close(db);

    /* 3. Replica-like damage: remove schema.bin but KEEP the partition files.
     *    This is the exact state of a node that received partition data but is
     *    momentarily unable to open the table (no/late schema, transient WAL
     *    error, etc).  The stable entry persists in catalog/stables.log. */
    char schema_path[4096];
    snprintf(schema_path, sizeof(schema_path), "%s/m/schema.bin", TDIR);
    ASSERT(remove(schema_path) == 0);
    ASSERT(has_partition(tbl_dir));            /* data still physically present */

    /* 4. Re-open: in-memory handle is gone; read path must use the stable
     *    fallback, whose tsdb_open_table("m") now fails (schema.bin missing). */
    OK(tsdb_open(TDIR, &db));
    cat = tsdb_db_catalog(db);
    ASSERT(tsdb_stable_exists(cat, "m"));       /* still resolvable as a stable */

    /* 5. The bug: a table that physically has a partition on disk must NOT
     *    silently return an empty result set.  Pre-fix exec_stable_select
     *    swallows the failed fallback open, leaving nchildren==0 and returning
     *    TSDB_OK with ZERO result rows. */
    int rows = -9; int64_t val = -9;
    int rc = run_count(db, "m", &rows, &val);
    printf("[after-damage] count(*) rc=%d result_rows=%d value=%lld\n", rc, rows, (long long)val);

    int empty_result_set = (rc == TSDB_OK && rows == 0);
    if (empty_result_set) {
        fprintf(stderr,
            "[BUG] count(*) returned an EMPTY RESULT SET (0 rows) for a table\n"
            "      whose partition is physically on disk at %s.\n"
            "      The stable fallback swallowed the failed tsdb_open_table.\n",
            tbl_dir);
        tsdb_close(db);
        rm_rf(TDIR);
        return 1;   /* bug reproduced */
    }

    /* Post-fix acceptance: NOT an empty result set.  Either the rows are
     * returned (best) or a real error is surfaced (acceptable — never the
     * silent 0-row lie). */
    ASSERT(!(rc == TSDB_OK && rows == 0));
    printf("[PASS] no empty-result-set: rc=%d rows=%d value=%lld\n", rc, rows, (long long)val);

    tsdb_close(db);
    rm_rf(TDIR);
    printf("=== test_stable_open_fail PASS ===\n");
    return 0;
}
