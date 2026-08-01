/* test_migrate_unflushed.c — an export must not silently omit un-flushed rows.
 *
 * tsdb_migrate_export reads the source's ON-DISK partitions. Rows still in the
 * memtable are not on disk, and under deferred flush (the cluster's mode) that
 * is most of the recent data. Before the fix, a plain export of a table with
 * un-flushed rows shipped a SHORT stream and reported success — an operator
 * tool holding less than the table while saying complete.
 *
 * The fix: the default export flushes the source first (complete). With
 * opts.no_flush set (for a source you must not mutate) it REFUSES with
 * TSDB_ERR_BUSY when the memtable holds rows rather than ship a short stream.
 *
 * This test writes rows and does NOT flush, then checks both paths.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../include/tsdb_migrate.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define SRC    "/tmp/tsdb_test_mig_unflushed_src"
#define DST    "/tmp/tsdb_test_mig_unflushed_dst"
#define STREAM "/tmp/tsdb_test_mig_unflushed.stream"
#define NROWS  1000
#define DAY1   1700000000000000000LL

static int g_fail;
#define CHECK(c, ...) do { if (!(c)) { printf("FAIL " __VA_ARGS__); \
    printf("\n"); g_fail++; } } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) { \
    printf("FAIL %s:%d rc=%d\n", __FILE__, __LINE__, _r); g_fail++; } } while (0)

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

/* Write NROWS but DO NOT flush — they stay in the memtable. Returns the db,
 * left open so the memtable is live. */
static tsdb_db_t *write_unflushed(void) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(SRC, &db) != TSDB_OK) return NULL;
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_INT64 } };
    if (tsdb_create_table(db, "m", cols, 2, "ts") != TSDB_OK) { tsdb_close(db); return NULL; }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "m", &t) != TSDB_OK) { tsdb_close(db); return NULL; }
    tsdb_batch_t *b = NULL;
    if (tsdb_batch_begin(t, &b) != TSDB_OK) { tsdb_close(db); return NULL; }
    for (int i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, DAY1 + (int64_t)i * 1000000LL);
        tsdb_batch_row_i64(b, 1, i + 1);
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    /* NO flush. */
    return db;
}

static long long stream_rows(const char *path) {
    /* Import into a fresh dst and count. */
    rm_rf(DST);
    tsdb_db_t *dst = NULL;
    if (tsdb_open(DST, &dst) != TSDB_OK) return -1;
    int fd = open(path, O_RDONLY);
    if (fd < 0) { tsdb_close(dst); return -1; }
    tsdb_mig_stats_t ist;
    int rc = tsdb_migrate_import(dst, fd, NULL, &ist);
    close(fd);
    if (rc != TSDB_OK) { tsdb_close(dst); return -1; }
    long long n = 0;
    tsdb_result_t *r = NULL;
    if (tsdb_query(dst, "SELECT v FROM m", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) n++;
    }
    if (r) tsdb_result_free(r);
    tsdb_close(dst);
    return n;
}

int main(void) {
    printf("=== migrate export completeness (un-flushed rows) ===\n");
    rm_rf(SRC); rm_rf(DST); remove(STREAM);

    /* --- [1] no_flush on a table with un-flushed rows -> BUSY, not a short
     *         stream. Do this FIRST, while the memtable still holds the rows. */
    tsdb_db_t *db = write_unflushed();
    CHECK(db != NULL, "opened source with 1000 un-flushed rows");
    if (db) {
        int fd = open(STREAM, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        CHECK(fd >= 0, "open stream");
        tsdb_mig_opts_t o; memset(&o, 0, sizeof(o)); o.no_flush = 1;
        tsdb_mig_stats_t st; memset(&st, 0, sizeof(st));
        int rc = tsdb_migrate_export(db, "m", fd, &o, &st);
        close(fd);
        /* In flush-on-commit mode the rows are already durable at commit, so the
         * memtable is empty and a no_flush export is legitimately COMPLETE (rc=OK).
         * The BUSY refusal is exactly for the deferred-flush case, where the rows
         * really are in the memtable and not on disk — which is the mode the
         * cluster runs and the one this whole fix is for. */
        const char *wc = getenv("TSDB_WAL_ONLY_COMMIT");
        int deferred = (wc && wc[0] == '1');
        printf("  no_flush export (deferred=%d) -> rc=%d\n", deferred, rc);
        if (deferred) {
            CHECK(rc == TSDB_ERR_BUSY,
                  "deferred-flush no_flush export REFUSES an un-flushed table "
                  "(rc=%d) rather than shipping a short stream", rc);
        } else {
            CHECK(rc == TSDB_OK,
                  "flush-on-commit no_flush export succeeds — the rows are "
                  "already durable so the stream is complete (rc=%d)", rc);
        }
        tsdb_close(db);
    }

    /* --- [2] default export flushes and ships ALL 1000 rows. Fresh source so
     *         the memtable is un-flushed again. */
    remove(STREAM);
    rm_rf(SRC);
    db = write_unflushed();
    CHECK(db != NULL, "reopened source with 1000 un-flushed rows");
    if (db) {
        int fd = open(STREAM, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        CHECK(fd >= 0, "open stream");
        tsdb_mig_stats_t st; memset(&st, 0, sizeof(st));
        int rc = tsdb_migrate_export(db, "m", fd, NULL, &st);   /* default: flush */
        close(fd);
        printf("  default export -> rc=%d rows=%llu (want %d)\n",
               rc, (unsigned long long)st.rows, NROWS);
        CHECK(rc == TSDB_OK, "default export succeeds (rc=%d)", rc);
        CHECK(st.rows == NROWS,
              "default export reports all %d rows (got %llu) — the flush made "
              "the un-flushed memtable rows part of the stream",
              NROWS, (unsigned long long)st.rows);
        tsdb_close(db);

        long long got = stream_rows(STREAM);
        printf("  imported stream holds %lld rows (want %d)\n", got, NROWS);
        CHECK(got == NROWS,
              "the exported stream imports to %d rows — no silent loss (got %lld)",
              NROWS, got);
    }

    rm_rf(SRC); rm_rf(DST); remove(STREAM);
    printf("\n=== %s ===\n", g_fail ? "FAILED" : "migrate export completeness: all pass");
    return g_fail ? 1 : 0;
}
