/* test_ae_local_stats.c — anti-entropy must measure ITS OWN rows, not the
 * cluster's.
 *
 * THE BUG (pre-fix): tsdb_cluster_resync_table measured "how much do I have?"
 * with `SELECT count(*), max(ts) FROM <table>`.  Hierarchy mirroring registers
 * every plain table as a childless data-bearing super-table, so exec routes
 * that through exec_stable_select; on a node holding NO local rows the
 * cluster-agg coordinator fires and the query answers with the CLUSTER-WIDE
 * total.  The node compared that number against its peers', found nothing
 * higher, and returned "already up-to-date" — so an empty replica never pulled
 * anything and stayed empty forever, silently diverged, with anti-entropy
 * reporting success.  (Measured on a live 2-node repro: the empty node
 * reported count=1000 as its own and pulled 0 rows.)
 *
 * THE FIX: tsdb_cluster_local_table_stats() counts what is actually on THIS
 * node — every partition's ts.idx header plus an atomic memtable snapshot.
 *
 * This is the single-node half: it pins the local measurement itself (no
 * sockets, no peers, no sleeps).  The two-node convergence half needs the
 * cluster harness and lives with the other cluster tests.
 */
#include "tsdb.h"
#include "tsdb_cluster.h"
#include "../src/storage/db.h"   /* tsdb_db_flush_all */

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define TDIR "/tmp/tsdb_test_ae_local_stats"
#define BASE 1700000000000000000LL

static void rm_rf(const char *p) {
    DIR *d = opendir(p);
    if (!d) { remove(p); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char q[4096]; snprintf(q, sizeof(q), "%s/%s", p, e->d_name);
        rm_rf(q);
    }
    closedir(d); rmdir(p);
}

static void insert_rows(tsdb_table_t *t, int64_t first, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, BASE + first + i));
        OK(tsdb_batch_row_i64(b, 1, first + i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

static void check(tsdb_db_t *db, const char *table,
                  uint64_t want_count, int64_t want_max, const char *what)
{
    uint64_t c = 12345; int64_t m = 999;
    int rc = tsdb_cluster_local_table_stats(db, table, &c, &m);
    printf("  %-34s rc=%d count=%llu max_ts=%lld\n",
           what, rc, (unsigned long long)c, (long long)m);
    if (rc != TSDB_OK) FAIL("%s: rc=%d (%s)", what, rc, tsdb_errstr(rc));
    if (c != want_count)
        FAIL("%s: count=%llu want %llu", what,
             (unsigned long long)c, (unsigned long long)want_count);
    if (m != want_max)
        FAIL("%s: max_ts=%lld want %lld", what, (long long)m, (long long)want_max);
}

int main(void) {
    printf("=== test_ae_local_stats ===\n");
    rm_rf(TDIR);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));

    /* 1. A table that exists but holds nothing reads as EMPTY — this is the
     *    case the cluster-scoped query got wrong (it answered with the peers'
     *    total), and the one that decides whether a fresh replica ever pulls. */
    check(db, "t", 0, INT64_MIN, "empty table");

    /* 2. Committed rows must count wherever they currently live — the commit
     *    path lands them in the memtable or straight in a partition depending
     *    on the durability mode, and anti-entropy must not care which. */
    insert_rows(t, 0, 100);
    check(db, "t", 100, BASE + 99, "100 rows committed");

    /* 3. A flush moves rows memtable -> partition; they must still be counted
     *    exactly once, never dropped and never double-counted. */
    OK(tsdb_db_flush_all(db));
    check(db, "t", 100, BASE + 99, "100 rows after flush");

    /* 4. More rows on top, and max_ts must follow the newest one. */
    insert_rows(t, 100, 50);
    check(db, "t", 150, BASE + 149, "150 rows total");

    /* 5. A table this node has never heard of is empty, not an error. */
    check(db, "nosuchtable", 0, INT64_MIN, "unknown table");

    /* 6. The measurement must drive the right action: an empty local table
     *    against a populated peer has to pull, never "up to date". */
    tsdb_ae_action_t act = tsdb_antientropy_decide(0, INT64_MIN, 5000, BASE);
    printf("  decide(empty local vs peer 5000)   -> %d\n", (int)act);
    if (act != TSDB_AE_TAIL_PULL && act != TSDB_AE_FULL_PULL)
        FAIL("empty local vs populated peer must pull, got action %d", (int)act);

    /* And a correctly-measured populated node must NOT be told it is behind
     *    when it is not — the guard against a needless destructive path. */
    act = tsdb_antientropy_decide(150, BASE + 149, 150, BASE + 149);
    printf("  decide(in sync)                    -> %d\n", (int)act);
    if (act == TSDB_AE_FULL_PULL)
        FAIL("an in-sync node must never full-pull");

    tsdb_close(db);
    rm_rf(TDIR);
    printf("\n=== test_ae_local_stats PASSED ===\n");
    return 0;
}
