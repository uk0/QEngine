/* test_wal_torn_tail.c — a power-loss torn WAL tail must not swallow the acked
 * records appended after it.
 *
 * THE BUG (pre-fix): tsdb_wal_replay stops at the first CRC-invalid record and
 * returns TSDB_ERR_CORRUPT, which db.c treats as an expected, benign torn tail
 * — but nothing ever REPAIRS the log.  The writer fd is O_WRONLY|O_APPEND
 * (wal.c:198), so every record committed after the reopen lands AFTER the
 * garbage bytes and is acked to the client, while the next replay still stops
 * AT the garbage.  A single power cut therefore silently swallows every
 * subsequent acked record, permanently, until a flush happens to truncate the
 * WAL.
 *
 * (Commit 27811d7 ftruncates back on a SHORT/failed writev *inside* one
 * process.  It cannot help with a torn tail that outlived the process.)
 *
 * THE FIX: tsdb_wal_open repairs the log to the end of its last CRC-valid
 * record before it hands out the appending fd, so no writer can ever run past
 * a torn tail.
 *
 * Shape: commit 100 rows and die; splice a torn record onto the log; commit
 * 100 more rows and die; reopen once and require all 200 back, each exactly
 * once.  Everything runs through the public API, so it is RED on the unfixed
 * tree as-is.
 */
#include "tsdb.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
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

#define TDIR   "/tmp/tsdb_test_wal_torn_tail"
#define WALLOG TDIR "/wal/t.log"
#define DAY1   1735689600000000000LL
#define N_EACH 100
#define N_TOTAL (N_EACH * 2)

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

static off_t file_size(const char *p) {
    struct stat st;
    if (stat(p, &st) != 0) return -1;
    return st.st_size;
}

/* Commit N_EACH rows starting at `base`, then die WITHOUT closing, so the
 * memtable is lost and only the WAL carries the rows. */
static int child_write(int base) {
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    tsdb_db_t *db = NULL;
    if (tsdb_open(TDIR, &db) != TSDB_OK) return 2;
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "t", &t) != TSDB_OK) return 2;
    int i = 0;
    while (i < N_EACH) {
        int m = (N_EACH - i < 4) ? (N_EACH - i) : 4;
        tsdb_batch_t *b = NULL;
        if (tsdb_batch_begin(t, &b) != TSDB_OK) return 2;
        for (int k = 0; k < m; k++) {
            int v = base + i + k;
            if (tsdb_batch_row_ts(b, DAY1 + (int64_t)v) != TSDB_OK) return 2;
            if (tsdb_batch_row_i64(b, 1, v) != TSDB_OK) return 2;
            if (tsdb_batch_row_end(b) != TSDB_OK) return 2;
        }
        if (tsdb_batch_commit(b) != TSDB_OK) return 2;
        i += m;
    }
    _exit(0);   /* crash: no close, no flush */
}

/* Append the residue of a writev cut by a power loss: an 8-byte record header
 * claiming a 64-byte payload, with only 8 payload bytes actually on disk. */
static void splice_torn_record(void) {
    int fd = open(WALLOG, O_WRONLY | O_APPEND);
    if (fd < 0) FAIL("open %s for torn append", WALLOG);
    uint8_t torn[16] = {
        0xDE, 0xAD, 0xBE, 0xEF,   /* crc  — will not match */
        0x40, 0x00, 0x00, 0x00,   /* len  = 64, but only 8 bytes follow */
        0, 0, 0, 0, 0, 0, 0, 0
    };
    if (write(fd, torn, sizeof(torn)) != (ssize_t)sizeof(torn))
        FAIL("short write of torn record");
    if (fsync(fd) != 0) FAIL("fsync torn record");
    close(fd);
}

int main(int argc, char **argv) {
    if (argc > 1 && !strcmp(argv[1], "a")) return child_write(0);
    if (argc > 1 && !strcmp(argv[1], "b")) return child_write(N_EACH);

    printf("=== test_wal_torn_tail ===\n");
    rm_rf(TDIR);

    /* Create the table (clean close, so the schema is on disk). */
    {
        setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
        tsdb_db_t *db = NULL;
        OK(tsdb_open(TDIR, &db));
        tsdb_col_t cols[] = {
            { "ts", TSDB_TYPE_TIMESTAMP },
            { "v",  TSDB_TYPE_INT64     },
        };
        OK(tsdb_create_table(db, "t", cols, 2, "ts"));
        tsdb_close(db);
    }

    char cmd[4600];

    /* Phase 1: 100 acked rows, then crash. */
    snprintf(cmd, sizeof(cmd), "%s a", argv[0]);
    int st = system(cmd);
    if (st == -1 || !WIFEXITED(st) || WEXITSTATUS(st) != 0)
        FAIL("child a failed st=%d", st);
    off_t s1 = file_size(WALLOG);
    if (s1 <= 0) FAIL("WAL missing/empty after phase 1 (size=%lld)", (long long)s1);
    printf("[phase1] 100 rows acked, wal=%lld bytes\n", (long long)s1);

    /* Power cut: a partial record is left at the end of the log. */
    splice_torn_record();
    if (file_size(WALLOG) != s1 + 16)
        FAIL("torn splice size mismatch: %lld != %lld",
             (long long)file_size(WALLOG), (long long)(s1 + 16));
    printf("[torn]   spliced a 16-byte partial record at offset %lld\n",
           (long long)s1);

    /* Phase 2: 100 more acked rows. Pre-fix these land AFTER the garbage. */
    snprintf(cmd, sizeof(cmd), "%s b", argv[0]);
    st = system(cmd);
    if (st == -1 || !WIFEXITED(st) || WEXITSTATUS(st) != 0)
        FAIL("child b failed st=%d", st);
    off_t s2 = file_size(WALLOG);
    printf("[phase2] 100 more rows acked, wal=%lld bytes\n", (long long)s2);
    if (s2 <= s1)
        FAIL("phase 2 appended nothing (%lld <= %lld)", (long long)s2, (long long)s1);

    /* Single reopen: every acked row must come back, exactly once. */
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_result_t *r = NULL;
    int seen[N_TOTAL];
    memset(seen, 0, sizeof(seen));
    int64_t total = 0, out_of_range = 0;
    if (tsdb_query(db, "SELECT ts, v FROM t", &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t v = tsdb_result_i64(r, 1);
            if (v >= 0 && v < N_TOTAL) seen[v]++; else out_of_range++;
            total++;
        }
        tsdb_result_free(r);
    }
    tsdb_close(db);

    int missing = 0, dup = 0;
    int first_missing = -1;
    for (int i = 0; i < N_TOTAL; i++) {
        if (seen[i] == 0) { missing++; if (first_missing < 0) first_missing = i; }
        else if (seen[i] > 1) dup++;
    }
    printf("[reopen] rows=%lld (expect %d)  missing=%d  duplicated=%d  out_of_range=%lld\n",
           (long long)total, N_TOTAL, missing, dup, (long long)out_of_range);
    if (first_missing >= 0)
        printf("         first missing value: %d\n", first_missing);

    rm_rf(TDIR);

    if (missing)
        FAIL("%d acked row(s) lost — records appended after a torn tail were "
             "swallowed (first missing v=%d)", missing, first_missing);
    if (dup)
        FAIL("%d value(s) duplicated after replay", dup);
    if (total != N_TOTAL)
        FAIL("row count %lld != %d", (long long)total, N_TOTAL);

    printf("\n=== test_wal_torn_tail PASSED ===\n");
    return 0;
}
