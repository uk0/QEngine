/* test_compact_alter_race.c — ALTER TABLE ADD COLUMN landing inside a
 * compaction pass must not corrupt memory or the partition.
 *
 * THE BUG (pre-fix): compact_partition sized its per-column bookkeeping
 * arrays (produced / src_blocks / pad_rows) from ONE read of schema->ncols,
 * then re-read the LIVE schema->ncols / schema->cols in the pre-pass, the
 * Phase-1 loop and both Phase-2 loops.  ALTER TABLE ADD COLUMN reallocs
 * schema->cols and bumps ncols in place (db.c), and Phase 1 deliberately
 * holds no lock — so an ALTER landing mid-pass made the later loops index
 * past the calloc'd arrays (heap overflow: pad_rows[ci] / produced[ci] for
 * ci >= snapshot ncols) and chase the freed cols array (use-after-free).
 * TSDB_COMPACTION=1 is the shipped cluster default, so this was memory
 * corruption in the shipped configuration.
 *
 * THE FIX: compactor_scan_dir freezes the schema (ncols/ts_col_idx + a copy
 * of the cols array) under the table's compact_mtx — the same lock ALTER
 * holds for the realloc — and the whole pass runs against the frozen copy.
 *
 * DETERMINISM: TSDB_TEST_COMPACT_PHASE1_DELAY_MS parks the pass between its
 * pre-pass (arrays already sized) and Phase 1 (loops that re-read the live
 * schema pre-fix); the ALTERs land inside that window.  Pre-fix the failure
 * is undefined behaviour, so run under ASan (scripts/asan-build.sh) to see it
 * deterministically:
 *     heap-buffer-overflow READ of size 8 ... allocated by calloc
 *     in compact_partition (pad_rows sized to the pre-ALTER ncols)
 * Post-fix this test passes in a plain build too: every row survives, the
 * partition really was compacted, and every ALTER succeeded.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
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

#define TDIR        "/tmp/tsdb_test_compact_alter_race"
#define BLOCK_ROWS  8192
#define N_BLOCKS    20
#define N_ROWS      (BLOCK_ROWS * N_BLOCKS)
#define DAY1        1700000000000000000LL
#define N_ALTERS    8

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

/* Entry count of the first partition's <col>.idx (block count at bytes 4..7). */
static int read_idx_entries(const char *tbl_dir, const char *col, uint32_t *out)
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
        uint8_t hdr[8];
        size_t n = fread(hdr, 1, sizeof(hdr), f);
        fclose(f);
        if (n < 8) continue;
        *out = (uint32_t)(hdr[4] | (hdr[5] << 8) | (hdr[6] << 16) | ((uint32_t)hdr[7] << 24));
        found = 0;
        break;
    }
    closedir(td);
    return found;
}

/* Back-date partitions so the compactor's 60 s hot-partition skip passes. */
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
    printf("=== test_compact_alter_race ===\n");
    rm_rf(TDIR);

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

    uint32_t entries_before = 0;
    if (read_idx_entries(tbl_dir, "ts", &entries_before) != 0)
        FAIL("could not read ts.idx entry count");
    printf("[setup] ts.idx blocks before compaction: %u\n", entries_before);

    /* Park the pass between pre-pass and Phase 1 so the ALTERs below land in
     * the exact window where the arrays are sized but the columns unread. */
    setenv("TSDB_TEST_COMPACT_PHASE1_DELAY_MS", "800", 1);

    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.min_blocks_to_compact = 4;
    opts.interval_ns = 5000000000LL;
    opts.worker_threads = -1;            /* manual: run_once only */
    tsdb_compactor_t *cpt = NULL;
    OK(tsdb_compactor_start(db, &opts, &cpt));

    pthread_t th;
    if (pthread_create(&th, NULL, run_once_thread, cpt) != 0)
        FAIL("pthread_create");

    /* Grow the schema while the pass is parked: each ALTER reallocs
     * schema->cols and bumps ncols.  Pre-fix the resumed pass indexes its
     * ncols=2-sized arrays with the new ncols — heap overflow under ASan. */
    int alter_rc[N_ALTERS];
    for (int i = 0; i < N_ALTERS; i++) {
        usleep(60 * 1000);
        char cname[16];
        snprintf(cname, sizeof(cname), "c%d", i);
        alter_rc[i] = tsdb_alter_table_add_column(db, "m", cname, TSDB_TYPE_FLOAT64);
    }

    pthread_join(th, NULL);
    tsdb_compactor_stop(cpt);
    unsetenv("TSDB_TEST_COMPACT_PHASE1_DELAY_MS");

    for (int i = 0; i < N_ALTERS; i++)
        if (alter_rc[i] != TSDB_OK)
            FAIL("ALTER #%d failed: rc=%d (%s)", i, alter_rc[i], tsdb_errstr(alter_rc[i]));

    /* The pass must have finished its swap: fewer, larger blocks. */
    uint32_t entries_after = 0;
    if (read_idx_entries(tbl_dir, "ts", &entries_after) != 0)
        FAIL("could not re-read ts.idx entry count");
    printf("[after]  ts.idx blocks after compaction: %u\n", entries_after);
    if (entries_after >= entries_before)
        FAIL("partition was not compacted (%u -> %u blocks) — the pass "
             "aborted or corrupted its own bookkeeping", entries_before, entries_after);

    /* And every row must have survived it. */
    int bad = 0;
    int64_t n = verify_rows(db, &bad);
    printf("[after]  rows=%lld bad_vals=%d\n", (long long)n, bad);

    tsdb_close(db);
    rm_rf(TDIR);

    if (n != N_ROWS) FAIL("row count %lld != %d after ALTER-during-compaction",
                          (long long)n, N_ROWS);
    if (bad != 0)    FAIL("%d values corrupted by ALTER-during-compaction", bad);

    printf("\n=== test_compact_alter_race PASSED ===\n");
    return 0;
}
