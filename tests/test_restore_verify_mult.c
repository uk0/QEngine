/* test_restore_verify_mult.c — adversarial review probe.
 *
 * [R1] A HEALTHY COMPACTED TARGET WITH A DUPLICATED (ts_min, count) KEY.
 *
 *      The round-7 patch made rst_deep_check_block CONSUME the target block a
 *      stream record pairs with, so a second record under the same
 *      (ts_min, count) can no longer be satisfied by the same survivor.  For a
 *      MISS under `reenc_shape` the code then asks the TS column whether it
 *      still carries that key and, if it does, calls the miss a LOSS
 *      (blocks_missing) — the justification being "the partition's layout is
 *      provably intact at that position".
 *
 *      That justification fails exactly where the patch's own header says a
 *      merge can preserve a boundary: "32768 rows merged, then a trailing group
 *      of exactly one source block".  If every source block of the partition
 *      carries the SAME (ts_min, count) — many rows on one repeated timestamp,
 *      split by block_points — then the trailing output block is re-emitted
 *      under that very key.  The target then legitimately holds ONE block under
 *      the key while the set carries N, so N-1 records find no free candidate,
 *      the ts column answers "present" (from the preserved trailing block), and
 *      every one of them is counted as a lost block.
 *
 *      Nothing is lost: the partition holds every row, SELECT reads it back
 *      correctly, and the relation is REENCODED — the relation the header says
 *      must not fail.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define DAY_A   1700000000000000000LL

/* block_points for the table; COMPACT_BLOCK_POINTS is 32768, so 32 source
 * blocks fill one output block exactly and the 33rd is a trailing group of
 * exactly ONE source block — re-emitted under its own (ts_min, count). */
#define BP        1024
#define NCHUNKS   33

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

static int part_names(const char *dir, char (*out)[32], int max) {
    char tbl[4096];
    snprintf(tbl, sizeof(tbl), "%s/t", dir);
    DIR *d = opendir(tbl);
    if (!d) return 0;
    struct dirent *e;
    int n = 0;
    while ((e = readdir(d)) && n < max) {
        size_t k = strlen(e->d_name);
        if (k != 8 && k != 10) continue;
        int all = 1;
        for (size_t i = 0; i < k; i++)
            if (e->d_name[i] < '0' || e->d_name[i] > '9') all = 0;
        if (all) snprintf(out[n++], 32, "%s", e->d_name);
    }
    closedir(d);
    return n;
}

static uint32_t rd_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static int64_t rd_i64(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | p[i];
    return (int64_t)v;
}

static int idx_entries(const char *dir, const char *part, const char *col,
                       uint8_t **out, uint32_t *out_esz)
{
    char path[4300];
    snprintf(path, sizeof(path), "%s/t/%s/%s.idx", dir, part, col);
    uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(path, NULL, &cnt, &esz, NULL, NULL, NULL, NULL);
    if (hsz <= 0 || cnt == 0 || esz == 0) FAIL("cannot probe %s (hsz=%d)", path, hsz);
    uint8_t *buf = (uint8_t *)malloc((size_t)cnt * esz);
    if (!buf) FAIL("oom");
    FILE *f = fopen(path, "rb");
    if (!f) FAIL("cannot open %s", path);
    if (fseek(f, hsz, SEEK_SET) != 0 ||
        fread(buf, 1, (size_t)cnt * esz, f) != (size_t)cnt * esz)
        FAIL("short read of %s", path);
    fclose(f);
    *out = buf; *out_esz = esz;
    return (int)cnt;
}

static void dump_idx(const char *tag, const char *dir, const char *part,
                     const char *col)
{
    uint8_t *ent = NULL; uint32_t esz = 0;
    int n = idx_entries(dir, part, col, &ent, &esz);
    printf("%s %s.idx: %d entries", tag, col, n);
    for (int i = 0; i < n && i < 6; i++)
        printf("  [%d](ts_min=%lld count=%u)", i,
               (long long)rd_i64(ent + (size_t)i * esz + 16),
               rd_u32(ent + (size_t)i * esz + 12));
    if (n > 6) printf("  ...");
    printf("\n");
    free(ent);
}

static void write_chunk(tsdb_table_t *t, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < n; i++) {
        OK(tsdb_batch_row_ts(b, DAY_A));          /* ONE repeated timestamp */
        OK(tsdb_batch_row_f64(b, 1, 42.0));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

static void compact_one(tsdb_db_t *db, const char *dir, const char *part) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/t/%s", dir, part);
    struct timeval tv[2];
    tv[0].tv_sec = time(NULL) - 3600; tv[0].tv_usec = 0;
    tv[1] = tv[0];
    if (utimes(p, tv) != 0) FAIL("utimes(%s): %s", p, strerror(errno));
    tsdb_compactor_opts_t co;
    memset(&co, 0, sizeof(co));
    co.worker_threads = -1;
    tsdb_compactor_t *c = NULL;
    OK(tsdb_compactor_start(db, &co, &c));
    OK(tsdb_compactor_run_once(c));
    tsdb_compactor_stats_t cs;
    tsdb_compactor_stats(c, &cs);
    tsdb_compactor_stop(c);
    printf("  compactor: cols_rewritten=%llu parts_merged=%llu\n",
           (unsigned long long)cs.compactions_done,
           (unsigned long long)cs.parts_merged);
    if (cs.compactions_done == 0)
        FAIL("the compactor rewrote nothing — the case proves nothing");
}

static const tsdb_restore_verify_table_t *row_t(const tsdb_restore_verify_t *v,
                                                const char *name) {
    for (int i = 0; i < v->ntables; i++)
        if (!strcmp(v->tables[i].table, name)) return &v->tables[i];
    return NULL;
}

static void print_row(const char *tag, int rc, const tsdb_restore_verify_t *v) {
    const tsdb_restore_verify_table_t *r = row_t(v, "t");
    printf("%s rc=%d (%s) nfailed=%d", tag, rc, tsdb_errstr(rc), v->nfailed);
    if (r)
        printf("  t: rc=%d rel=%s rows=%llu/%llu checked=%llu missing=%llu "
               "mismatched=%llu unresolved=%llu",
               r->rc, tsdb_restore_target_rel_name(r->target_rel),
               (unsigned long long)r->rows_target,
               (unsigned long long)r->rows_backup,
               (unsigned long long)r->blocks_checked,
               (unsigned long long)r->blocks_missing,
               (unsigned long long)r->blocks_mismatched,
               (unsigned long long)r->blocks_unresolved);
    printf("\n");
}

#define R1_DIR "/tmp/tsdb_rev_dupmerge"
#define R1_BK  "/tmp/tsdb_rev_dupmerge_bk"

int main(void) {
    printf("\n[R1] a healthy compacted partition whose blocks all share one "
           "(ts_min, count)\n");

    rm_rf(R1_DIR);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(R1_DIR, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table_ex2(db, "t", cols, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));

    for (int k = 0; k < NCHUNKS; k++) {
        write_chunk(t, BP);
        OK(tsdb_table_flush(db, "t"));
    }

    char parts[8][32];
    int np = part_names(R1_DIR, parts, 8);
    if (np != 1) FAIL("[R1] expected 1 partition, found %d", np);
    dump_idx("[R1] pre ", R1_DIR, parts[0], "ts");
    dump_idx("[R1] pre ", R1_DIR, parts[0], "val");

    /* The case is worthless unless every source block really shares one key. */
    {
        uint8_t *ent = NULL; uint32_t esz = 0;
        int n = idx_entries(R1_DIR, parts[0], "val", &ent, &esz);
        if (n != NCHUNKS) FAIL("[R1] expected %d blocks, got %d", NCHUNKS, n);
        for (int i = 1; i < n; i++)
            if (rd_i64(ent + (size_t)i * esz + 16) != rd_i64(ent + 16) ||
                rd_u32(ent + (size_t)i * esz + 12) != rd_u32(ent + 12))
                FAIL("[R1] block %d does not share the key — case invalid", i);
        free(ent);
    }

    /* The set, taken BEFORE the compactor runs. */
    rm_rf(R1_BK);
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    OK(tsdb_backup_create(db, R1_BK, &rep));
    if (!rep.complete) FAIL("[R1] backup incomplete");
    tsdb_restore_report_free(&rep);

    /* The compactor, stock options — what tsdb_node_main runs on a 5 s timer. */
    compact_one(db, R1_DIR, parts[0]);

    dump_idx("[R1] post", R1_DIR, parts[0], "ts");
    dump_idx("[R1] post", R1_DIR, parts[0], "val");

    /* The engine's own verdict: is anything actually wrong? */
    {
        tsdb_result_t *r = NULL;
        int64_t rows = 0; double sum = 0;
        int rc = tsdb_query(db, "SELECT ts, val FROM t", &r);
        if (rc == TSDB_OK && r)
            while (tsdb_result_next(r) > 0) { rows++; sum += tsdb_result_f64(r, 1); }
        if (r) tsdb_result_free(r);
        printf("[R1] engine: SELECT ts, val -> rc=%d (%s) rows=%lld sum=%.1f "
               "(want rows=%d sum=%.1f)\n",
               rc, tsdb_errstr(rc), (long long)rows, sum,
               NCHUNKS * BP, 42.0 * NCHUNKS * BP);
        if (rc != TSDB_OK || rows != (int64_t)NCHUNKS * BP)
            FAIL("[R1] the target is NOT healthy — case invalid");
    }

    tsdb_restore_verify_t v;
    int irc, drc;
    memset(&v, 0, sizeof(v));
    irc = tsdb_restore_verify(db, R1_BK, TSDB_RESTORE_VERIFY_INDEX, &v);
    print_row("[R1] index", irc, &v);
    tsdb_restore_verify_free(&v);

    memset(&v, 0, sizeof(v));
    drc = tsdb_restore_verify(db, R1_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    print_row("[R1] deep ", drc, &v);
    const tsdb_restore_verify_table_t *r = row_t(&v, "t");
    uint64_t missing = r ? r->blocks_missing : 0;
    tsdb_restore_verify_free(&v);

    tsdb_close(db);

    if (drc != TSDB_OK || missing != 0) {
        printf("\n[R1] *** FALSE CORRUPT ***: a healthy compacted partition "
               "that holds every row of the set is reported with "
               "blocks_missing=%llu and rc=%d (%s)\n",
               (unsigned long long)missing, drc, tsdb_errstr(drc));
        FAIL("[R1] verify called a healthy REENCODED target corrupt");
    }
    printf("\n=== test_rev_dupmerge PASSED ===\n");
    return 0;
}
