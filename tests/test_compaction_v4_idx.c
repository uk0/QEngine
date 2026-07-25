/* test_compaction_v4_idx.c — compaction must read a V4 (48-byte header) index
 * correctly, or it silently corrupts every NON-TS column it merges.
 *
 * THE BUG (pre-fix): src/storage/part.c writes a V4 idx header (48 bytes,
 * carrying max_seq) ONLY when a WAL redo checkpoint is present — i.e. only
 * under TSDB_WAL_ONLY_COMMIT=1, which is exactly the cluster's configuration.
 * With max_seq == 0 (the default flush-on-commit path) it writes a
 * byte-identical V3 header (40 bytes).  src/storage/compaction.c knows only
 * V1/V2/V3:
 *
 *     hdr_sz   = (ver == 1) ? 20 : (ver == 2) ? 36 : 40;    // V4 -> 40, real is 48
 *     entry_sz = (ver == 3 && ...) ? <from header> : 40;    // V4 -> 40, real is 88
 *
 * so against a V4 index it reads every BlockIndexEntry at the wrong offset AND
 * the wrong stride, and `infos[b].ts_min` picks up bytes from the middle of a
 * neighbouring entry's 48-byte float stats payload.
 *
 * Why the TS column survives and every other column does not: the merge loop
 * computes the ts column's block ts_min/ts_max from the ACTUAL decoded
 * timestamps, but for a non-ts column it BORROWS ts_min/ts_max from
 * infos[b] (compaction.c ~:529-545).  So ts.idx comes out correct while
 * val.idx gets a float64 bit pattern in its ts_min/ts_max slots.  Reads pair a
 * non-ts column to its ts block by (ts_min,count) first-match, that match then
 * fails, and the column reads back as TSDB_ERR_CORRUPT / an internal error —
 * while count(*) and max(ts) stay perfectly correct.
 *
 * Observed in production: 52 of 256 child tables on a 4-node cluster could not
 * read `val` at all (per-replica, since compaction runs independently on each
 * node) while every ts-only query returned correct results.
 *
 * THE FIX: compaction must size the header and the entries from the on-disk
 * version (V4 -> 48-byte header, entry_size from the header field), and must
 * preserve max_seq into the rewritten index.
 *
 * This test writes in WAL-only-commit mode so the flushed index really is V4,
 * asserts that precondition, compacts, and then verifies every (ts,val) pair
 * survives byte-for-bit.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
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
#define ASSERT(c) do { if (!(c)) FAIL("assertion failed: %s", #c); } while (0)

#define TDIR        "/tmp/tsdb_test_compaction_v4"
#define BLOCK_ROWS  8192
#define N_BLOCKS    20                    /* > min_blocks_to_compact (4) */
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

/* The value stored at row i — deterministic, and spread over a range whose
 * float64 bit patterns are nothing like a nanosecond timestamp. */
static double val_at(int64_t i) { return (double)(i % 1000) * 0.01 - 5.0; }

/* Read <table>/<part>/<col>.idx and report the on-disk version + entry count. */
static int read_idx_version(const char *tbl_dir, const char *col,
                            uint16_t *out_ver, uint32_t *out_entries)
{
    DIR *td = opendir(tbl_dir);
    if (!td) return -1;
    struct dirent *pe;
    int found = -1;
    while ((pe = readdir(td)) != NULL) {
        if (pe->d_name[0] == '.') continue;
        char idx_path[4096];
        snprintf(idx_path, sizeof(idx_path), "%s/%s/%s.idx", tbl_dir, pe->d_name, col);
        FILE *f = fopen(idx_path, "rb");
        if (!f) continue;
        uint8_t hdr[48];
        size_t n = fread(hdr, 1, sizeof(hdr), f);
        fclose(f);
        if (n < 40) continue;
        *out_entries = (uint32_t)(hdr[4] | (hdr[5] << 8) | (hdr[6] << 16) | ((uint32_t)hdr[7] << 24));
        *out_ver     = (uint16_t)(hdr[8] | (hdr[9] << 8));
        found = 0;
        break;
    }
    closedir(td);
    return found;
}

/* Back-date partitions so the compactor (which skips partitions touched in the
 * last 60 s) treats them as cold. */
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

/* Scan every (ts,val) row; verify each val matches what was written.
 * Returns the row count, or -1 if the query itself failed. */
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
        double  want = val_at(ts - DAY1);
        if (v != want) {
            if (bad < 3)
                fprintf(stderr, "  row ts=%lld: val=%.6f want=%.6f\n",
                        (long long)ts, v, want);
            bad++;
        }
        n++;
    }
    tsdb_result_free(r);
    *out_bad = bad;
    return n;
}

int main(void) {
    printf("=== test_compaction_v4_idx ===\n");
    rm_rf(TDIR);

    /* WAL-only commit is what makes the flushed index V4 (it stamps max_seq). */
    setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);

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

    /* One flush per block, so we end with N_BLOCKS blocks per column. */
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
    printf("[setup] wrote %lld rows in %d flushed blocks\n",
           (long long)row, N_BLOCKS);

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/m", TDIR);

    /* PRECONDITION: the flush really produced a V4 index. If this is not V4 the
     * test is vacuous — it would exercise compaction's already-correct V3 path. */
    uint16_t ver = 0; uint32_t nent = 0;
    if (read_idx_version(tbl_dir, "ts", &ver, &nent) != 0)
        FAIL("could not read ts.idx header");
    printf("[setup] on-disk ts.idx version=%u entries=%u (expect version=4)\n", ver, nent);
    if (ver != 4)
        FAIL("precondition: expected a V4 index (48-byte header) but found v%u — "
             "the test would not exercise the V4 path", ver);

    /* Baseline: everything reads correctly BEFORE compaction. */
    int bad = 0;
    int64_t n_before = verify_rows(db, &bad);
    printf("[before] rows=%lld bad_vals=%d\n", (long long)n_before, bad);
    if (n_before != N_ROWS) FAIL("pre-compaction row count %lld != %d",
                                 (long long)n_before, N_ROWS);
    if (bad != 0) FAIL("pre-compaction values already wrong (%d)", bad);

    /* Compact. */
    backdate_partitions(tbl_dir);
    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns = 5000000000LL;
    opts.worker_threads = 1;
    tsdb_compactor_t *cpt = NULL;
    OK(tsdb_compactor_start(db, &opts, &cpt));
    OK(tsdb_compactor_run_once(cpt));
    tsdb_compactor_stop(cpt);

    if (read_idx_version(tbl_dir, "ts", &ver, &nent) == 0)
        printf("[after]  on-disk ts.idx version=%u entries=%u\n", ver, nent);

    /* THE ASSERTION: every value must survive the merge. */
    bad = 0;
    int64_t n_after = verify_rows(db, &bad);
    printf("[after]  rows=%lld bad_vals=%d\n", (long long)n_after, bad);

    tsdb_close(db);
    rm_rf(TDIR);

    if (n_after < 0)
        FAIL("post-compaction scan FAILED (the non-ts column is unreadable) — "
             "compaction corrupted the V4 index");
    if (n_after != N_ROWS)
        FAIL("post-compaction row count %lld != %d", (long long)n_after, N_ROWS);
    if (bad != 0)
        FAIL("%d values changed across compaction — the non-ts column was corrupted", bad);

    printf("\n=== test_compaction_v4_idx PASSED ===\n");
    return 0;
}
