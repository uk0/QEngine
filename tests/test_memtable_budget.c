/* test_memtable_budget.c — aggregate-memtable backpressure.
 *
 * is_full() caps each table's memtable at TSDB_BLOCK_POINTS rows, but with many
 * tables the SUM is unbounded (a 2000-ptable stress held ~3.4 GiB).  With
 * TSDB_MEMTABLE_BUDGET_ROWS set, the maintenance thread raises an over-budget
 * flag when total live memtable rows exceed it, and the next write to any table
 * holding a worthwhile batch flushes it (on the writer's thread).  This pins:
 * two tables each well under the per-table flush threshold but together over
 * the budget → the next write flushes one to disk; under-budget it would not.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <ctype.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d\n",_r);FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_mtbudget";

static void rm_tree(const char *p){char c[512];snprintf(c,sizeof(c),"rm -rf %s",p);(void)system(c);}

/* A flushed table has a digit-named partition subdir; pre-flush it has only
 * schema.bin (a file). */
static int has_partition(const char *table_dir) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    int found = 0; struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (isdigit((unsigned char)e->d_name[0])) { found = 1; break; }
    }
    closedir(d);
    return found;
}

/* Append n rows to table `name` (ts strictly ascending from base). */
static void write_rows(tsdb_db_t *db, const char *name, int n, int64_t base) {
    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, name, &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(base + i) * 1000000));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

int main(void) {
    printf("=== test_memtable_budget ===\n");
    rm_tree(TMP);
    /* Deferred-flush mode: commit syncs WAL but leaves rows in the memtable
     * (this is the mode that bloats under many tables — flush-on-commit, the
     * default, never accumulates).  The budget then bounds the aggregate. */
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    setenv("TSDB_MEMTABLE_BUDGET_ROWS", "6000", 1);   /* small budget for the test */

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TMP, &db));
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    OK(tsdb_create_table(db, "t0", cols, 2, "ts"));
    OK(tsdb_create_table(db, "t1", cols, 2, "ts"));

    char d0[512]; snprintf(d0, sizeof(d0), "%s/t0", TMP);

    /* t0: 5000 rows — under the 8192 per-table flush AND under the 6000 budget,
     * so nothing flushes yet. */
    write_rows(db, "t0", 5000, 1);
    /* Give the maintenance thread a couple of passes; still under budget. */
    for (int i = 0; i < 15; i++) { if (has_partition(d0)) break; usleep(200 * 1000); }
    ASSERT(!has_partition(d0));     /* under budget → no flush */
    printf("  t0 @5000 rows, under budget: no flush (memtable only)\n");

    /* t1: 5000 more → total 10000 > 6000 budget.  Once the maintenance thread
     * raises the flag, the next write to t0 (which holds 5000 >= 2048 rows)
     * flushes it.  Poll, nudging t0 each round so a write actually happens. */
    write_rows(db, "t1", 5000, 1);
    int flushed = 0;
    for (int i = 0; i < 40; i++) {
        write_rows(db, "t0", 10, 100000 + i * 100);   /* small nudge writes */
        if (has_partition(d0)) { flushed = 1; break; }
        usleep(200 * 1000);
    }
    ASSERT(flushed);
    printf("  over budget (10000 > 6000): t0 flushed to disk under backpressure\n");

    tsdb_close(db);
    unsetenv("TSDB_MEMTABLE_BUDGET_ROWS");
    unsetenv("TSDB_WAL_ONLY_COMMIT");
    rm_tree(TMP);
    printf("[PASS] aggregate-memtable budget flushes under backpressure (bounds memory)\n");
    return 0;
}
