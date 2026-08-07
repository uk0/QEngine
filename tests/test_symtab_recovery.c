/* test_symtab_recovery.c — a SYMBOL dictionary must be durable after a flush,
 * so durable coded blocks decode to the RIGHT strings after a crash.
 *
 * THE BUG (pre-fix): tsdb_symtab_save ran only at clean close / schema change,
 * never at flush.  A flush publishes SYMBOL blocks carrying dictionary CODES
 * (0=web-1, 1=web-2) but the code->string map was never persisted; a process
 * crash then loses it, and the reopen rebuilds a DIFFERENT map from the WAL
 * tail (which only holds "web-2") — so the flushed prefix's code 0 decodes as
 * "web-2" and every "web-1" row silently reads wrong.  count(*) stays right.
 *
 * THE FIX: flush_and_clear_locked persists each SYMBOL column's dict (atomic
 * tmp+fsync+rename) BEFORE publishing the blocks that reference its codes.
 *
 * Repro: block_points=1024; write 1100 rows (600 "web-1", then 500 "web-2") so
 * row 1024 auto-flushes the first 1024 (codes durable); crash; reopen; verify
 * per-host counts.
 */
#include "tsdb.h"
#include "../src/core/symbol.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define TDIR "/tmp/tsdb_test_symtab"
#define DAY1 1700000000000000000LL
#define N_WEB1 600
#define N_TOTAL 1100

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        chmod(q, 0700); rm_rf(q);
    }
    closedir(d); rmdir(p);
}

/* --- part 2: interned-string pointers must survive heap growth -----------
 * symbol.h documents tsdb_symtab_str's return as "valid until table is
 * freed", and callers hold that pointer with NO symtab lock: ingest interns
 * under only the table's batch_mu, queries materialize SYMBOL columns under
 * scan_refs alone, and tsdb_result_sym hands the pointer to user code.
 * Pre-fix, heap_reserve realloc'd the string heap under the write lock, so
 * the first growth past the 4096-byte seed freed the block every previously
 * returned pointer points into — a use-after-free that ASAN flags
 * deterministically on the strcmp below (ASAN's realloc always moves).
 * 10000 interns force several growths; afterwards the old pointer must
 * still read "aaa", and a fresh lookup must agree (proves the copy path
 * carried the live bytes forward). */
static void heap_growth_pointer_stability(void) {
    tsdb_symtab_t *st = NULL;
    if (tsdb_symtab_new(&st) != TSDB_OK) FAIL("symtab_new");
    uint32_t code = tsdb_symtab_intern(st, "aaa");
    if (code == TSDB_SYMBOL_INVALID) FAIL("intern(aaa)");
    const char *p = tsdb_symtab_str(st, code);
    if (!p || strcmp(p, "aaa") != 0) FAIL("initial lookup");
    for (int i = 0; i < 10000; i++) {
        char s[32];
        snprintf(s, sizeof(s), "sym-%d", i);
        if (tsdb_symtab_intern(st, s) == TSDB_SYMBOL_INVALID) FAIL("intern #%d", i);
    }
    if (strcmp(p, "aaa") != 0)   /* pre-fix: heap-use-after-free right here */
        FAIL("pre-growth pointer now reads \"%s\"", p);
    const char *q = tsdb_symtab_str(st, code);
    if (!q || strcmp(q, "aaa") != 0)
        FAIL("post-growth lookup reads \"%s\"", q ? q : "(null)");
    tsdb_symtab_free(st);
    printf("[symtab] pointer stable across 10000 interns / heap growths\n");
}

static int count_rows(tsdb_db_t *db, const char *sql) {
    tsdb_result_t *r = NULL; int n = 0;
    if (tsdb_query(db, sql, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
        tsdb_result_free(r);
    }
    return n;
}

int main(int argc, char **argv) {
    if (argc > 1 && !strcmp(argv[1], "child")) {
        setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
        tsdb_db_t *db = NULL;
        if (tsdb_open(TDIR, &db) != TSDB_OK) _exit(2);
        tsdb_col_t cols[] = {
            { "ts",   TSDB_TYPE_TIMESTAMP },
            { "host", TSDB_TYPE_SYMBOL    },
            { "val",  TSDB_TYPE_INT64     },
        };
        if (tsdb_create_table_ex2(db, "m", cols, 3, "ts",
                                  TSDB_CREATE_PART_DAY, 1024) != TSDB_OK) _exit(2);
        tsdb_table_t *t = NULL;
        if (tsdb_open_table(db, "m", &t) != TSDB_OK) _exit(2);
        int i = 0;
        while (i < N_TOTAL) {
            int m = (N_TOTAL - i < 64) ? (N_TOTAL - i) : 64;
            tsdb_batch_t *b = NULL;
            if (tsdb_batch_begin(t, &b) != TSDB_OK) _exit(2);
            for (int k = 0; k < m; k++) {
                int idx = i + k;
                if (tsdb_batch_row_ts(b, DAY1 + idx) != TSDB_OK) _exit(2);
                if (tsdb_batch_row_sym(b, 1, idx < N_WEB1 ? "web-1" : "web-2") != TSDB_OK) _exit(2);
                if (tsdb_batch_row_i64(b, 2, idx) != TSDB_OK) _exit(2);
                if (tsdb_batch_row_end(b) != TSDB_OK) _exit(2);
            }
            if (tsdb_batch_commit(b) != TSDB_OK) _exit(2);
            i += m;
        }
        _exit(0);   /* crash: no tsdb_close, no clean symtab save */
    }

    printf("=== test_symtab_recovery ===\n");
    heap_growth_pointer_stability();
    rm_rf(TDIR);
    char cmd[4600];
    snprintf(cmd, sizeof(cmd), "%s child", argv[0]);
    int st = system(cmd);
    if (st == -1 || !WIFEXITED(st) || WEXITSTATUS(st) != 0)
        FAIL("child failed st=%d", st);

    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    int total = count_rows(db, "SELECT val FROM m");
    int w1    = count_rows(db, "SELECT val FROM m WHERE host = 'web-1'");
    int w2    = count_rows(db, "SELECT val FROM m WHERE host = 'web-2'");
    printf("[reopen] total=%d (expect %d)  web-1=%d (expect %d)  web-2=%d (expect %d)\n",
           total, N_TOTAL, w1, N_WEB1, w2, N_TOTAL - N_WEB1);
    tsdb_close(db);
    rm_rf(TDIR);

    if (total != N_TOTAL || w1 != N_WEB1 || w2 != N_TOTAL - N_WEB1)
        FAIL("symbol dict decoded wrong after crash: total=%d web-1=%d web-2=%d", total, w1, w2);
    printf("\n=== test_symtab_recovery PASSED ===\n");
    return 0;
}
