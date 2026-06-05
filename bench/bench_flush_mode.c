/* bench_flush_mode.c — in-process wide-cardinality write throughput, isolating
 * the flush mode.  No server, no raft: builds N tables and writes M rows
 * round-robin in batches, timing the write phase and counting on-disk
 * partitions (a proxy for flush count).
 *
 * Compare flush-on-commit vs deferred (TSDB_WAL_ONLY_COMMIT=1) +
 * aggregate-memtable budget (TSDB_MEMTABLE_BUDGET_ROWS):
 *
 *   ./build/bench/bench_flush_mode --tables 1000 --rows 8000000 --batch 2048
 *   TSDB_WAL_ONLY_COMMIT=1 TSDB_MEMTABLE_BUDGET_ROWS=8000000 \
 *     ./build/bench/bench_flush_mode --tables 1000 --rows 8000000 --batch 2048
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <ctype.h>

static int64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
static void rm_tree(const char *p){char c[600];snprintf(c,sizeof(c),"rm -rf %s",p);(void)system(c);}

/* Count digit-named partition dirs under every table dir (flush proxy). */
static long count_partitions(const char *root, int ntables) {
    long total = 0;
    for (int i = 0; i < ntables; i++) {
        char td[600]; snprintf(td, sizeof(td), "%s/t%d", root, i);
        DIR *d = opendir(td); if (!d) continue;
        struct dirent *e;
        while ((e = readdir(d))) if (isdigit((unsigned char)e->d_name[0])) total++;
        closedir(d);
    }
    return total;
}

int main(int argc, char **argv) {
    int ntables = 1000; long long nrows = 8000000; int batch = 2048;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--tables") && i+1<argc) ntables = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--rows") && i+1<argc) nrows = atoll(argv[++i]);
        else if (!strcmp(argv[i], "--batch") && i+1<argc) batch = atoi(argv[++i]);
    }
    const char *deferred = getenv("TSDB_WAL_ONLY_COMMIT");
    const char *budget   = getenv("TSDB_MEMTABLE_BUDGET_ROWS");
    printf("[bench_flush] mode=%s budget=%s tables=%d rows=%lld batch=%d\n",
           (deferred && deferred[0] && deferred[0] != '0') ? "deferred(WAL_ONLY)" : "flush-on-commit",
           budget ? budget : "(default 32M)", ntables, nrows, batch);

    const char *DIR = "/tmp/tsdb_bench_flush";
    rm_tree(DIR);

    tsdb_db_t *db = NULL;
    if (tsdb_open(DIR, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); return 1; }

    tsdb_col_t cols[] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_FLOAT64} };
    int64_t ddl0 = now_ns();
    tsdb_table_t **th = calloc(ntables, sizeof(*th));
    for (int i = 0; i < ntables; i++) {
        char n[16]; snprintf(n, sizeof(n), "t%d", i);
        if (tsdb_create_table(db, n, cols, 2, "ts") != TSDB_OK) { fprintf(stderr, "create %s failed\n", n); return 1; }
        if (tsdb_open_table(db, n, &th[i]) != TSDB_OK) { fprintf(stderr, "open %s failed\n", n); return 1; }
    }
    printf("[bench_flush] DDL: %d tables in %.3fs\n", ntables, (now_ns() - ddl0) / 1e9);

    /* Write nrows round-robin across tables, in batches.  ts ascending per table. */
    long long *ts_of = calloc(ntables, sizeof(long long));
    int64_t w0 = now_ns();
    long long written = 0;
    int ti = 0;
    while (written < nrows) {
        tsdb_table_t *t = th[ti];
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) { fprintf(stderr, "batch_begin failed\n"); return 1; }
        int this_batch = (int)((nrows - written) < batch ? (nrows - written) : batch);
        for (int r = 0; r < this_batch; r++) {
            long long ts = ++ts_of[ti];
            if (tsdb_batch_row_ts(b, (tsdb_ts_t)ts * 1000000) != TSDB_OK ||
                tsdb_batch_row_f64(b, 1, (double)r) != TSDB_OK ||
                tsdb_batch_row_end(b) != TSDB_OK) { fprintf(stderr, "row failed\n"); return 1; }
        }
        if (tsdb_batch_commit(b) != TSDB_OK) { fprintf(stderr, "commit failed\n"); return 1; }
        written += this_batch;
        ti = (ti + 1) % ntables;
    }
    /* Flush everything so the comparison includes all data on disk. */
    tsdb_db_flush_all(db);
    double wsec = (now_ns() - w0) / 1e9;

    long parts = count_partitions(DIR, ntables);
    printf("[bench_flush] wrote %lld rows in %.2fs → %.0f rows/s  | %ld partitions (flush proxy)\n",
           written, wsec, written / wsec, parts);

    tsdb_close(db);
    rm_tree(DIR);
    free(th); free(ts_of);
    return 0;
}
