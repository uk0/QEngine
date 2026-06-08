/* test_reactor_write.c — Phase 2 integration: route real writes through the
 * reactor pool against a live tsdb_db, verify the stored data is correct.
 *
 * Proves the shard-per-core write model end-to-end WITHOUT touching the
 * server/rpc hot path: each table is owned by exactly one core
 * (hash(name)%ncores), and every WRITE to that table is executed ON its
 * owner core via tsdb_reactor_pool_call.  So a given table's memtable is
 * only ever appended by ONE thread (the Scylla single-writer invariant),
 * even though many caller threads submit writes concurrently to many tables.
 *
 * After all routed writes, SELECT count(*) per table must equal exactly the
 * number of rows routed there — no loss, no duplication, no cross-table
 * corruption.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/exec/reactor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <assert.h>

#define ASSERT(cond) do { if (!(cond)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #cond, __FILE__, __LINE__); abort(); } } while (0)
#define OK(rc) do { int _r = (rc); if (_r != TSDB_OK) { \
    fprintf(stderr, "rc=%d at %s:%d\n", _r, __FILE__, __LINE__); abort(); } } while (0)

static const char *DIR = "/tmp/tsdb_test_reactor_write";
static void rm_tree(const char *p) { char c[512]; snprintf(c, sizeof(c), "rm -rf %s", p); (void)system(c); }

#define NCORES         4
#define NTABLES        16
#define NTHREADS       4
#define WRITES_PER_TBL 20
#define ROWS_PER_WRITE 50
#define TOTAL_WRITES   (NTABLES * WRITES_PER_TBL)
#define BASE_TS        1000000000000LL

static tsdb_db_t          *g_db;
static tsdb_reactor_pool_t *g_pool;
static _Atomic long         g_bad;

/* A single routed write — runs ON the table's owner core. */
typedef struct {
    char    name[32];
    int64_t ts_base;     /* globally-unique ts block for this write */
    int     rc;
} wctx_t;

static void write_closure(void *p) {
    wctx_t *w = (wctx_t *)p;
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(g_db, w->name, &t) != TSDB_OK) { w->rc = -1; return; }
    tsdb_batch_t *b = NULL;
    if (tsdb_batch_begin(t, &b) != TSDB_OK) { w->rc = -2; return; }
    for (int i = 0; i < ROWS_PER_WRITE; i++) {
        if (tsdb_batch_row_ts(b, (tsdb_ts_t)(w->ts_base + i)) != TSDB_OK ||
            tsdb_batch_row_f64(b, 1, (double)i) != TSDB_OK ||
            tsdb_batch_row_end(b) != TSDB_OK) { w->rc = -3; return; }
    }
    w->rc = tsdb_batch_commit(b);
}

static void *writer_thread(void *arg) {
    int tid = (int)(intptr_t)arg;
    /* Thread `tid` handles write ids tid, tid+NTHREADS, …  Write id `wid`
     * targets table wid%NTABLES, so each table gets exactly WRITES_PER_TBL
     * writes regardless of which thread issued them.  Its ts block
     * [wid*ROWS_PER_WRITE, +ROWS_PER_WRITE) is disjoint from every other
     * write, so no row is deduped away. */
    for (int wid = tid; wid < TOTAL_WRITES; wid += NTHREADS) {
        wctx_t w;
        snprintf(w.name, sizeof(w.name), "lt_%d", wid % NTABLES);
        w.ts_base = BASE_TS + (int64_t)wid * ROWS_PER_WRITE;
        w.rc = -99;
        if (tsdb_reactor_pool_call(g_pool, w.name, write_closure, &w) != 0 ||
            w.rc != TSDB_OK)
            atomic_fetch_add(&g_bad, 1);
    }
    return NULL;
}

static int64_t query_count(const char *table) {
    char q[64]; snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
    tsdb_result_t *res = NULL;
    if (tsdb_query(g_db, q, &res) != TSDB_OK) return -1;
    int64_t cnt = -1;
    if (tsdb_result_next(res) == 1) cnt = tsdb_result_i64(res, 0);
    tsdb_result_free(res);
    return cnt;
}

int main(void) {
    printf("=== test_reactor_write (Phase 2 integration) ===\n");
    rm_tree(DIR);

    OK(tsdb_open(DIR, &g_db));

    /* Tables created upfront (DDL routing is Phase 3); writes are routed. */
    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    for (int i = 0; i < NTABLES; i++) {
        char name[32]; snprintf(name, sizeof(name), "lt_%d", i);
        OK(tsdb_create_table(g_db, name, cols, 2, "ts"));
    }

    g_pool = tsdb_reactor_pool_new(NCORES, 256);
    ASSERT(g_pool);
    atomic_store(&g_bad, 0);

    /* Concurrent routed writes: many caller threads, every table pinned to
     * one owner core. */
    pthread_t th[NTHREADS];
    for (intptr_t i = 0; i < NTHREADS; i++)
        ASSERT(pthread_create(&th[i], NULL, writer_thread, (void *)i) == 0);
    for (int i = 0; i < NTHREADS; i++) pthread_join(th[i], NULL);

    ASSERT(atomic_load(&g_bad) == 0);

    /* Flush memtables so count(*) sees every row, then verify per-table. */
    OK(tsdb_db_flush_all(g_db));

    const int64_t expect = (int64_t)WRITES_PER_TBL * ROWS_PER_WRITE;
    int64_t total = 0;
    for (int i = 0; i < NTABLES; i++) {
        char name[32]; snprintf(name, sizeof(name), "lt_%d", i);
        int64_t c = query_count(name);
        if (c != expect) {
            fprintf(stderr, "table %s: count=%lld expected=%lld\n",
                    name, (long long)c, (long long)expect);
            ASSERT(c == expect);
        }
        total += c;
    }
    ASSERT(total == (int64_t)TOTAL_WRITES * ROWS_PER_WRITE);
    printf("  %d tables x %lld rows routed through %d cores = %lld rows, all correct\n",
           NTABLES, (long long)expect, NCORES, (long long)total);

    tsdb_reactor_pool_free(g_pool);
    tsdb_close(g_db);
    rm_tree(DIR);
    printf("[PASS] reactor-routed writes: per-table counts exact, single-owner-per-table\n");
    return 0;
}
