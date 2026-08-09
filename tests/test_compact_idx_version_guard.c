/* test_compact_idx_version_guard.c — compaction must REFUSE an idx version it
 * was not written for, never size it by guess.
 *
 * THE BUG (pre-fix): compact_column_file mapped any unknown idx version onto
 * the V4 header size:
 *
 *     hdr_sz = (ver==1) ? 20 : (ver==2) ? 36 : (ver==3) ? 40 : 48;  // v9 -> 48
 *
 * with no range check.  part.c's shared parser (read_idx_header_ex) returns -1
 * for unknown versions, so this hand-rolled copy was the one parser in the
 * tree that guesses — the exact silent-guess class as the V4-read-as-V3 bug
 * that destroyed 95% of a partition's rows.  Latent today (the manifest
 * pre-pass probes through the shared parser and vetoes first), it detonates
 * the day a V5 format teaches the shared parser but not this copy: V5 files
 * get sliced at V4 offsets and merged into garbage.
 *
 * THE FIX: compact_column_file refuses idx_ver outside [1,4] with
 * TSDB_ERR_CORRUPT and a stderr line, before reading any version-keyed field.
 * The error (not a skip) makes compact_partition veto the whole partition, so
 * siblings can never swap around the refused column.
 *
 * MECHANICS: the pre-pass reads the idx through the shared parser (which
 * would veto an unknown version before the buggy mapping runs), so the test
 * parks the pass between pre-pass and Phase 1 (TSDB_TEST_COMPACT_PHASE1_DELAY_MS)
 * and rewrites ts.idx's version field to 9 inside the park — exactly the
 * "shared parser accepts, private mapping guesses" state a V5 bump creates.
 * Pre-fix: no refusal is printed (default mode: the misparse fails quietly;
 * WAL-only mode: 48 happens to be right, the file is compacted and REPLACED —
 * caught by the version-must-still-be-9 assert).  Post-fix: the refusal line
 * appears, the file is untouched, and after restoring the version byte every
 * row reads back intact.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define TDIR        "/tmp/tsdb_test_compact_idx_ver_guard"
#define ERRLOG      TDIR "_stderr.log"
#define BLOCK_ROWS  8192
#define N_BLOCKS    20
#define N_ROWS      (BLOCK_ROWS * N_BLOCKS)
#define DAY1        1700000000000000000LL

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

static double val_at(int64_t i) { return (double)(i % 1000) * 0.01 - 5.0; }

/* Locate the (single) partition's ts.idx. */
static int find_ts_idx(const char *tbl_dir, char *out, size_t outsz,
                       char *out_part, size_t out_part_sz)
{
    DIR *td = opendir(tbl_dir);
    if (!td) return -1;
    struct dirent *pe;
    int found = -1;
    while ((pe = readdir(td)) != NULL) {
        if (pe->d_name[0] == '.') continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s/ts.idx", tbl_dir, pe->d_name);
        struct stat st;
        if (stat(p, &st) == 0) {
            snprintf(out, outsz, "%s", p);
            snprintf(out_part, out_part_sz, "%s/%s", tbl_dir, pe->d_name);
            found = 0;
            break;
        }
    }
    closedir(td);
    return found;
}

static uint16_t idx_version(const char *idx_path) {
    FILE *f = fopen(idx_path, "rb");
    if (!f) return 0;
    uint8_t hdr[10];
    size_t n = fread(hdr, 1, sizeof(hdr), f);
    fclose(f);
    if (n < 10) return 0;
    return (uint16_t)(hdr[8] | (hdr[9] << 8));
}

static void write_idx_version(const char *idx_path, uint16_t ver) {
    int fd = open(idx_path, O_WRONLY);
    if (fd < 0) FAIL("open %s for version poke", idx_path);
    uint8_t b[2] = { (uint8_t)(ver & 0xff), (uint8_t)(ver >> 8) };
    if (pwrite(fd, b, 2, 8) != 2) FAIL("pwrite version");
    close(fd);
}

static void backdate_partitions(const char *tbl_dir) {
    DIR *td = opendir(tbl_dir);
    if (!td) return;
    struct dirent *pe;
    while ((pe = readdir(td)) != NULL) {
        if (pe->d_name[0] == '.') continue;
        char part_dir[4096];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", tbl_dir, pe->d_name);
        struct stat st;
        if (stat(part_dir, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        DIR *pd = opendir(part_dir);
        if (pd) {
            struct dirent *fe;
            while ((fe = readdir(pd)) != NULL) {
                if (fe->d_name[0] == '.') continue;
                char fp[8192];
                snprintf(fp, sizeof(fp), "%s/%s", part_dir, fe->d_name);
                struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
                utime(fp, &tb);
            }
            closedir(pd);
        }
        struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
        utime(part_dir, &tb);
    }
}

static int64_t verify_rows(tsdb_db_t *db, int *out_bad)
{
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, "SELECT ts, val FROM m", &r);
    if (rc != TSDB_OK || !r) {
        fprintf(stderr, "  query failed: rc=%d (%s)\n", rc, tsdb_errstr(rc));
        if (r) tsdb_result_free(r);
        return -1;
    }
    int64_t n = 0; int bad = 0;
    while (tsdb_result_next(r)) {
        int64_t ts = tsdb_result_ts(r, 0);
        double  v  = tsdb_result_f64(r, 1);
        if (v != val_at(ts - DAY1)) bad++;
        n++;
    }
    tsdb_result_free(r);
    *out_bad = bad;
    return n;
}

static void *run_once_thread(void *arg)
{
    tsdb_compactor_t *cpt = arg;
    (void)tsdb_compactor_run_once(cpt);
    return NULL;
}

int main(void) {
    printf("=== test_compact_idx_version_guard ===\n");
    rm_rf(TDIR);
    remove(ERRLOG);

    tsdb_db_t *db = NULL;
    OK(tsdb_open(TDIR, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table_ex2(db, "m", cols, 2, "ts",
                             TSDB_CREATE_PART_DAY, BLOCK_ROWS));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));

    int64_t row = 0;
    for (int b = 0; b < N_BLOCKS; b++) {
        int64_t written = 0;
        while (written < BLOCK_ROWS) {
            int m = (int)((BLOCK_ROWS - written < 512) ? (BLOCK_ROWS - written) : 512);
            tsdb_batch_t *batch = NULL;
            OK(tsdb_batch_begin(t, &batch));
            for (int k = 0; k < m; k++) {
                OK(tsdb_batch_row_ts(batch, DAY1 + row));
                OK(tsdb_batch_row_f64(batch, 1, val_at(row)));
                OK(tsdb_batch_row_end(batch));
                row++; written++;
            }
            OK(tsdb_batch_commit(batch));
        }
        OK(tsdb_db_flush_all(db));
    }
    printf("[setup] wrote %lld rows\n", (long long)row);

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/m", TDIR);
    backdate_partitions(tbl_dir);

    char idx_path[4096], part_dir[4096];
    if (find_ts_idx(tbl_dir, idx_path, sizeof(idx_path),
                    part_dir, sizeof(part_dir)) != 0)
        FAIL("no partition with ts.idx found");
    uint16_t orig_ver = idx_version(idx_path);
    printf("[setup] %s version=%u\n", idx_path, orig_ver);
    if (orig_ver < 1 || orig_ver > 4) FAIL("unexpected on-disk version %u", orig_ver);

    /* Park the pass after its pre-pass (which reads the still-valid header),
     * then rewrite the version inside the park so ONLY the private Phase-1
     * mapping sees it — the state a future V5 writer would create. */
    setenv("TSDB_TEST_COMPACT_PHASE1_DELAY_MS", "1200", 1);

    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns = 5000000000LL;
    opts.worker_threads = -1;            /* manual: run_once only */
    tsdb_compactor_t *cpt = NULL;
    OK(tsdb_compactor_start(db, &opts, &cpt));

    /* Capture the compactor's stderr for the whole run. */
    fflush(stderr);
    int saved_err = dup(2);
    int lf = open(ERRLOG, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (saved_err < 0 || lf < 0) FAIL("stderr redirect setup");
    dup2(lf, 2);
    close(lf);

    pthread_t th;
    if (pthread_create(&th, NULL, run_once_thread, cpt) != 0) {
        dup2(saved_err, 2);
        FAIL("pthread_create");
    }
    usleep(300 * 1000);                  /* well inside the 1200 ms park */
    write_idx_version(idx_path, 9);
    pthread_join(th, NULL);

    fflush(stderr);
    dup2(saved_err, 2);
    close(saved_err);

    tsdb_compactor_stop(cpt);
    unsetenv("TSDB_TEST_COMPACT_PHASE1_DELAY_MS");

    /* 1. The guard must have fired — loudly. */
    char logbuf[8192] = { 0 };
    FILE *elf = fopen(ERRLOG, "rb");
    if (elf) {
        size_t got = fread(logbuf, 1, sizeof(logbuf) - 1, elf);
        (void)got;
        fclose(elf);
    }
    if (!strstr(logbuf, "unknown idx version"))
        FAIL("no refusal on stderr — compaction silently accepted idx version 9 "
             "(stderr was: %.300s)", logbuf);

    /* 2. The refused file must be untouched (pre-fix WAL-only mode compacts
     *    and REPLACES it, so the version byte comes back 3/4). */
    uint16_t ver_now = idx_version(idx_path);
    if (ver_now != 9)
        FAIL("ts.idx was rewritten (version now %u) — a version-9 index was "
             "compacted instead of refused", ver_now);

    /* 3. No half-produced .tmp files may remain in the partition. */
    DIR *pd = opendir(part_dir);
    if (!pd) FAIL("opendir %s", part_dir);
    struct dirent *fe;
    while ((fe = readdir(pd)) != NULL) {
        size_t len = strlen(fe->d_name);
        if (len > 4 && strcmp(fe->d_name + len - 4, ".tmp") == 0)
            FAIL("leftover %s in refused partition", fe->d_name);
    }
    closedir(pd);

    /* 4. Restore the real version: the data underneath must be intact. */
    write_idx_version(idx_path, orig_ver);
    int bad = 0;
    int64_t n = verify_rows(db, &bad);
    printf("[after]  rows=%lld bad_vals=%d\n", (long long)n, bad);

    tsdb_close(db);
    rm_rf(TDIR);
    remove(ERRLOG);

    if (n != N_ROWS) FAIL("row count %lld != %d after refused compaction",
                          (long long)n, N_ROWS);
    if (bad != 0)    FAIL("%d values corrupted", bad);

    printf("\n=== test_compact_idx_version_guard PASSED ===\n");
    return 0;
}
