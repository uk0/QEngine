/* test_restore_adv.c — adversarial probe of tsdb_backup_create.
 *
 * [W] A table that exists on disk but has NOT been opened by the backing-up
 *     process still holds its acked rows in the per-table redo log under
 *     deferred flush (TSDB_WAL_ONLY_COMMIT=1, the cluster's configuration).
 *     tsdb_backup_create calls tsdb_db_flush_all(), but that only walks the
 *     tables THIS process has opened — so the export reads a partition
 *     directory that does not contain those rows, and the manifest is written
 *     with complete=1.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define SRC "/tmp/tsdb_adv_src"
#define BK  "/tmp/tsdb_adv_bk"
#define BK2 "/tmp/tsdb_adv_bk2"
#define DST "/tmp/tsdb_adv_dst"

#define DAY0   1700000000000000000LL
#define NROWS  1000

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

/* Number of YYYYMMDD partition dirs on disk for `table`. */
static int npart_dirs(const char *data_dir, const char *table) {
    char tbl[4096];
    snprintf(tbl, sizeof(tbl), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t k = strlen(e->d_name);
        if (k != 8 && k != 10) continue;
        int all = 1;
        for (size_t i = 0; i < k; i++) if (e->d_name[i] < '0' || e->d_name[i] > '9') all = 0;
        if (all) n++;
    }
    closedir(d);
    return n;
}

static long wal_size(const char *data_dir, const char *table) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/wal/%s.log", data_dir, table);
    struct stat st;
    if (stat(p, &st) != 0) return -1;
    return (long)st.st_size;
}

/* Child: create the table, commit NROWS, then die without closing the db —
 * exactly what a power cut or a `kill -9` on the server leaves behind. */
static void child_write_and_die(void) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(SRC, &db));
    tsdb_col_t cols[] = {
        { "ts", TSDB_TYPE_TIMESTAMP },
        { "v",  TSDB_TYPE_FLOAT64   },
        { "n",  TSDB_TYPE_INT64     },
    };
    OK(tsdb_create_table(db, "m", cols, 3, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < NROWS; i++) {
        OK(tsdb_batch_row_ts(b, DAY0 + (int64_t)i * 1000000LL));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_i64(b, 2, i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));           /* acked: WAL fsync + memtable */
    fflush(NULL);
    _exit(0);                           /* no tsdb_close -> no flush */
}

int main(void) {
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);   /* the cluster's configuration */
    rm_rf(SRC); rm_rf(BK); rm_rf(BK2); rm_rf(DST);

    printf("=== test_restore_adv ===\n");

    pid_t pid = fork();
    if (pid == 0) { child_write_and_die(); _exit(1); }
    int st = 0; waitpid(pid, &st, 0);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
        FAIL("[W] writer child did not exit cleanly (status %d)", st);

    printf("[W] after the crash: %d partition dir(s) on disk, wal/m.log = %ld bytes\n",
           npart_dirs(SRC, "m"), wal_size(SRC, "m"));

    /* The backup process opens the database and takes a backup.  It has not
     * touched table 'm' — nothing has, in this process. */
    tsdb_db_t *db = NULL;
    OK(tsdb_open(SRC, &db));

    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_backup_create(db, BK, &rep);
    printf("[W] tsdb_backup_create rc=%d (%s) ntables=%d nfailed=%d complete=%d\n",
           rc, tsdb_errstr(rc), rep.ntables, rep.nfailed, rep.complete);
    uint64_t backed_up = 0;
    for (int i = 0; i < rep.ntables; i++) {
        printf("[W]   table '%s' rc=%d rows=%llu blocks=%llu\n",
               rep.tables[i].table, rep.tables[i].rc,
               (unsigned long long)rep.tables[i].rows,
               (unsigned long long)rep.tables[i].blocks_seen);
        if (!strcmp(rep.tables[i].table, "m")) backed_up = rep.tables[i].rows;
    }
    tsdb_restore_report_free(&rep);

    /* Same handle, same instant: what the database says it holds. */
    int64_t live = count_rows(db, "m");
    printf("[W] the database itself answers count(*) FROM m = %lld\n", (long long)live);

    /* A verify of the set it just wrote. */
    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int vrc = tsdb_restore_verify(db, BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    printf("[W] tsdb_restore_verify(DEEP) rc=%d nfailed=%d\n", vrc, v.nfailed);
    tsdb_restore_verify_free(&v);

    /* Second backup on the SAME handle.  'm' is open now (the export opened
     * it), so this time flush_all reaches it. */
    memset(&rep, 0, sizeof(rep));
    int rc2 = tsdb_backup_create(db, BK2, &rep);
    uint64_t backed_up2 = 0;
    for (int i = 0; i < rep.ntables; i++)
        if (!strcmp(rep.tables[i].table, "m")) backed_up2 = rep.tables[i].rows;
    printf("[W] second tsdb_backup_create rc=%d complete=%d -> rows=%llu\n",
           rc2, rep.complete, (unsigned long long)backed_up2);
    tsdb_restore_report_free(&rep);
    tsdb_close(db);

    /* Restore the FIRST set into a fresh database. */
    tsdb_db_t *dst = NULL;
    OK(tsdb_open(DST, &dst));
    memset(&rep, 0, sizeof(rep));
    int rrc = tsdb_restore_run(dst, BK, &rep);
    printf("[W] tsdb_restore_run(set #1) rc=%d ntables=%d nfailed=%d complete=%d\n",
           rrc, rep.ntables, rep.nfailed, rep.complete);
    tsdb_restore_report_free(&rep);
    int64_t restored = count_rows(dst, "m");
    printf("[W] restored database answers count(*) FROM m = %lld\n", (long long)restored);
    tsdb_close(dst);

    if (backed_up != (uint64_t)NROWS || restored != NROWS) {
        printf("[W] SILENT: the set reported complete=1 with %llu of %d rows; "
               "restoring it yields %lld\n",
               (unsigned long long)backed_up, NROWS, (long long)restored);
        FAIL("[W] tsdb_backup_create reported a complete set that omits the "
             "unflushed redo log of a table this process never opened "
             "(backed_up=%llu live=%lld restored=%lld, want %d everywhere)",
             (unsigned long long)backed_up, (long long)live,
             (long long)restored, NROWS);
    }

    printf("\n=== test_restore_adv PASSED ===\n");
    return 0;
}
