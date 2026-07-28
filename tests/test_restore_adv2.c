/* test_restore_adv2.c — [X] does the restore stay idempotent after the
 * database's own background compactor has touched what it restored?
 *
 * tsdb_restore_run dedups on (ts_min, count) per (partition, column).  The
 * compactor rewrites a column's blocks into 32768-point blocks, so every key
 * the dedup primed from disappears.  A second restore run — the documented
 * "re-run a finished restore / resume an interrupted one" path — then matches
 * nothing.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define SRC "/tmp/tsdb_adv2_src"
#define BK  "/tmp/tsdb_adv2_bk"
#define DST "/tmp/tsdb_adv2_dst"

#define DAY0   1700000000000000000LL
#define NROWS  150000

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

static int64_t count_rows(tsdb_db_t *db, const char *table) {
    char q[128];
    snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
    tsdb_result_t *r = NULL;
    int64_t n = -1;
    if (tsdb_query(db, q, &r) == TSDB_OK && r && tsdb_result_next(r) > 0)
        n = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);
    return n;
}

/* SUM(n) — a duplicated block doubles this too, so it is not just count(*). */
static int64_t sum_n(tsdb_db_t *db, const char *table) {
    char q[128];
    snprintf(q, sizeof(q), "SELECT sum(n) FROM %s", table);
    tsdb_result_t *r = NULL;
    int64_t n = -1;
    if (tsdb_query(db, q, &r) == TSDB_OK && r && tsdb_result_next(r) > 0)
        n = tsdb_result_i64(r, 0);
    if (r) tsdb_result_free(r);
    return n;
}

static int is_part_name(const char *s) {
    size_t k = strlen(s);
    if (k != 8 && k != 10) return 0;
    for (size_t i = 0; i < k; i++) if (s[i] < '0' || s[i] > '9') return 0;
    return 1;
}

/* Block count of <first partition>/<ts>.idx. */
static uint32_t ts_blocks(const char *data_dir, const char *table,
                          const char *ts_col, char *out_part, size_t cap) {
    char tbl[4096];
    snprintf(tbl, sizeof(tbl), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl);
    if (!d) return 0;
    char best[64] = {0};
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!is_part_name(e->d_name)) continue;
        if (!best[0] || strcmp(e->d_name, best) < 0) snprintf(best, sizeof(best), "%s", e->d_name);
    }
    closedir(d);
    if (!best[0]) return 0;
    if (out_part) snprintf(out_part, cap, "%s", best);
    char ip[4400];
    snprintf(ip, sizeof(ip), "%s/%s/%s.idx", tbl, best, ts_col);
    uint32_t cnt = 0;
    if (tsdb_part_idx_probe(ip, NULL, &cnt, NULL, NULL, NULL, NULL, NULL) <= 0) return 0;
    return cnt;
}

static void build_src(void) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(SRC, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "n",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "m", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));
    int i = 0;
    while (i < NROWS) {
        int m = (NROWS - i < 1000) ? NROWS - i : 1000;
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int k = 0; k < m; k++) {
            OK(tsdb_batch_row_ts(b, DAY0 + (int64_t)(i + k) * 1000000LL));
            OK(tsdb_batch_row_i64(b, 1, 1));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        i += m;
    }
    OK(tsdb_db_flush_all(db));
    tsdb_close(db);
}

int main(void) {
    rm_rf(SRC); rm_rf(BK); rm_rf(DST);
    printf("=== test_restore_adv2 ===\n");

    build_src();

    tsdb_db_t *src = NULL;
    OK(tsdb_open(SRC, &src));
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    OK(tsdb_backup_create(src, BK, &rep));
    printf("[X] backup: ntables=%d complete=%d rows=%llu\n",
           rep.ntables, rep.complete, (unsigned long long)rep.tables[0].rows);
    tsdb_restore_report_free(&rep);
    tsdb_close(src);

    tsdb_db_t *dst = NULL;
    OK(tsdb_open(DST, &dst));
    memset(&rep, 0, sizeof(rep));
    OK(tsdb_restore_run(dst, BK, &rep));
    printf("[X] restore #1: rc=0 landed=%llu skipped=%llu -> count(*)=%lld sum(n)=%lld\n",
           (unsigned long long)rep.tables[0].blocks_landed,
           (unsigned long long)rep.tables[0].blocks_skipped,
           (long long)count_rows(dst, "m"), (long long)sum_n(dst, "m"));
    tsdb_restore_report_free(&rep);

    char part[64] = {0};
    uint32_t before = ts_blocks(DST, "m", "ts", part, sizeof(part));

    /* The compactor skips partitions touched in the last 60 s ("still hot").
     * Age this one so the pass behaves the way it does on any partition the
     * operator is not actively writing — i.e. all of them, a minute later. */
    {
        char pd[4200];
        snprintf(pd, sizeof(pd), "%s/m/%s", DST, part);
        struct timeval tv[2];
        tv[0].tv_sec = time(NULL) - 3600; tv[0].tv_usec = 0;
        tv[1] = tv[0];
        if (utimes(pd, tv) != 0) FAIL("[X] utimes(%s)", pd);
    }

    /* The database's own compactor, driven synchronously.  Threshold 2 so one
     * pass is enough; the background thread does the same thing on its 5 s
     * timer with threshold 16. */
    tsdb_compactor_opts_t co;
    memset(&co, 0, sizeof(co));
    /* DEFAULT threshold (16) — no test-only tuning. */
    co.worker_threads = -1;                 /* manual: no background thread */
    tsdb_compactor_t *c = NULL;
    OK(tsdb_compactor_start(dst, &co, &c));
    OK(tsdb_compactor_run_once(c));
    tsdb_compactor_stats_t cs;
    tsdb_compactor_stats(c, &cs);
    tsdb_compactor_stop(c);

    uint32_t after = ts_blocks(DST, "m", "ts", NULL, 0);
    printf("[X] compactor: cols_rewritten=%llu parts_merged=%llu | %s/ts.idx blocks %u -> %u\n",
           (unsigned long long)cs.compactions_done,
           (unsigned long long)cs.parts_merged, part, before, after);
    int64_t c_after_compact = count_rows(dst, "m");
    printf("[X] after compaction: count(*)=%lld sum(n)=%lld\n",
           (long long)c_after_compact, (long long)sum_n(dst, "m"));

    /* The documented idempotent re-run. */
    memset(&rep, 0, sizeof(rep));
    int rc2 = tsdb_restore_run(dst, BK, &rep);
    printf("[X] restore #2: rc=%d (%s) nfailed=%d complete=%d landed=%llu skipped=%llu\n",
           rc2, tsdb_errstr(rc2), rep.nfailed, rep.complete,
           (unsigned long long)rep.tables[0].blocks_landed,
           (unsigned long long)rep.tables[0].blocks_skipped);
    uint64_t landed2 = rep.tables[0].blocks_landed;
    tsdb_restore_report_free(&rep);

    int64_t c2 = count_rows(dst, "m");
    int64_t s2 = sum_n(dst, "m");
    printf("[X] after restore #2: count(*)=%lld sum(n)=%lld (want %d / %d)\n",
           (long long)c2, (long long)s2, NROWS, NROWS);
    tsdb_close(dst);

    if (c2 != NROWS || s2 != NROWS) {
        printf("[X] SILENT: restore #2 returned rc=%d and landed %llu block(s) over "
               "rows the target already held\n", rc2, (unsigned long long)landed2);
        FAIL("[X] a second tsdb_restore_run after the compactor touched the "
             "restored partition duplicated rows: count(*)=%lld sum(n)=%lld, "
             "want %d — rc=%d, nothing reported",
             (long long)c2, (long long)s2, NROWS, rc2);
    }
    printf("\n=== test_restore_adv2 PASSED ===\n");
    return 0;
}
