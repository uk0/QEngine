/* test_flush_symtab_rc.c — a flush that cannot persist a SYMBOL dictionary
 * must NOT report success and clear/truncate the rows.
 *
 * flush_and_clear_locked persists each SYMBOL column's dictionary BEFORE
 * publishing the coded blocks that reference it — the on-disk invariant is
 * "dict durable before the blocks".  The rc of tsdb_symtab_save was discarded
 * ((void)...), so a failed dict save let the flush go on to publish the coded
 * blocks and then clear the memtable + truncate the WAL.  Result: durable
 * SYMBOL-coded blocks with NO matching on-disk dict; a reopen rebuilds a
 * DIFFERENT dict and silently returns WRONG tag values, with the WAL — the last
 * recoverable copy — already gone.
 *
 * The dict save is failed deterministically by planting <table>/<col>.sym.tmp
 * as a DIRECTORY: tsdb_symtab_save fopen("...sym.tmp","wb")s that path and gets
 * EISDIR -> TSDB_ERR_IO, while tsdb_part_flush_ex2 (writing into a partition
 * SUBdir) still succeeds.  So on the unfixed engine the flush "succeeds".
 *
 * Mode-robust: in flush-on-commit mode the flush runs at tsdb_batch_commit; in
 * deferred mode it runs at tsdb_db_flush_all.  The test drives both and fails
 * if NEITHER surfaces the error.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define OK(rc)  do { int _r=(rc); if (_r!=TSDB_OK){fprintf(stderr,"rc=%d\n",_r);FAIL("rc");} } while (0)
#define ASSERT(c) do { if(!(c)) FAIL(#c); } while (0)

static const char *TMP = "/tmp/tsdb_test_flush_symtab_rc";
static void rm_tree(const char *p){ char c[512]; snprintf(c,sizeof(c),"rm -rf %s",p); (void)system(c); }

int main(void) {
    printf("=== test_flush_symtab_rc ===\n");
    rm_tree(TMP);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TMP, &db));

    tsdb_col_t cols[] = {
        { "ts",   TSDB_TYPE_TIMESTAMP },
        { "host", TSDB_TYPE_SYMBOL    },
        { "val",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table_ex2(db, "m", cols, 3, "ts", TSDB_CREATE_PART_DAY, 1024));

    /* Sabotage the dict-save target BEFORE any flush: a directory where the
     * atomic-publish temp file must go.  <data_dir>/m is the schema dir; the
     * SYMBOL column "host" saves to host.sym via host.sym.tmp. */
    char symtmp[512];
    snprintf(symtmp, sizeof(symtmp), "%s/m/host.sym.tmp", TMP);
    ASSERT(mkdir(symtmp, 0755) == 0);

    tsdb_table_t *t = NULL; OK(tsdb_open_table(db, "m", &t));
    tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
    /* Few rows (< block_points 1024) so deferred mode does not size-flush early;
     * the flush is driven explicitly below.  New symbol values grow the dict, so
     * the first flush genuinely attempts a save (not the unchanged no-op). */
    for (int i = 0; i < 50; i++) {
        OK(tsdb_batch_row_ts(b, (tsdb_ts_t)(i + 1) * 1000000000LL));
        OK(tsdb_batch_row_sym(b, 1, (i & 1) ? "web-1" : "web-2"));
        OK(tsdb_batch_row_i64(b, 2, i));
        OK(tsdb_batch_row_end(b));
    }
    int commit_rc = tsdb_batch_commit(b);   /* flush-on-commit mode flushes here */
    int flush_rc  = tsdb_db_flush_all(db);  /* deferred mode flushes here        */

    /* The invariant: with the dict unpersistable, the flush must surface the
     * failure in whichever mode ran it — never clear the rows behind a silent
     * "success".  Unfixed: both are TSDB_OK. */
    if (commit_rc == TSDB_OK && flush_rc == TSDB_OK) {
        fprintf(stderr, "flush reported success despite unpersistable SYMBOL "
                "dict (commit=%d flush_all=%d)\n", commit_rc, flush_rc);
        FAIL("flush did not surface tsdb_symtab_save failure");
    }
    printf("  flush surfaced dict-save failure (commit=%d flush_all=%d)\n",
           commit_rc, flush_rc);

    /* Clear the sabotage and confirm a normal flush now works — the guard only
     * blocks the failing case, it does not wedge the table. */
    ASSERT(rmdir(symtmp) == 0);
    OK(tsdb_db_flush_all(db));
    printf("  recovery: flush succeeds once the dict target is writable\n");

    tsdb_close(db);
    rm_tree(TMP);
    printf("[PASS] flush refuses to drop rows when the SYMBOL dict cannot persist\n");
    return 0;
}
