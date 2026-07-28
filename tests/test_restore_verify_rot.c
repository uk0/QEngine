/* test_restore_verify_rot.c — adversarial review probe #2.
 *
 * [R2] THE "PREFER EQUAL" SCAN CAN HIDE AN UNREADABLE BLOCK.
 *
 *      Round 7 made rst_deep_check_block compare EVERY still-free candidate
 *      under a (ts_min, count) and keep the BEST verdict, preferring the one
 *      that compares equal (residual risk 5: "so the answer no longer depends
 *      on which order two interchangeable blocks sit in").
 *
 *      The base compared only the FIRST match.  So when a partition holds two
 *      blocks under one key — a re-sent replication group, a WAL replayed over
 *      rows already on disk — and the FIRST of them has rotted, the base
 *      landed on the rotted one and reported it; round 7 walks past it to the
 *      clean sibling and answers TSDB_OK.
 *
 *      The engine does not walk past it: SELECT on that partition returns
 *      TSDB_ERR_CORRUPT.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"

#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define DAY_A   1700000000000000000LL
#define STEP_NS 1000000LL
#define NROW    64

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

static void write_chunk(tsdb_table_t *t) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = 0; i < NROW; i++) {
        OK(tsdb_batch_row_ts(b, DAY_A + (int64_t)i * STEP_NS));
        OK(tsdb_batch_row_f64(b, 1, 42.0));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

/* Flip a byte inside the FIRST block's payload of <col>.col — the block the
 * base's first-match landed on. */
static void rot_first_block(const char *dir, const char *part, const char *col) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/t/%s/%s.col", dir, part, col);
    FILE *f = fopen(p, "r+b");
    if (!f) FAIL("cannot open %s: %s", p, strerror(errno));
    off_t at = (off_t)TSDB_BLOCK_HEADER_SIZE + 4;
    unsigned char b = 0;
    if (fseeko(f, at, SEEK_SET) != 0 || fread(&b, 1, 1, f) != 1)
        FAIL("cannot read %s at %lld", p, (long long)at);
    b = (unsigned char)(b ^ 0xFF);
    if (fseeko(f, at, SEEK_SET) != 0 || fwrite(&b, 1, 1, f) != 1)
        FAIL("cannot write %s", p);
    fclose(f);
    printf("     flipped byte %lld of %s (block 0 payload)\n", (long long)at, p);
}

static const tsdb_restore_verify_table_t *row_t(const tsdb_restore_verify_t *v) {
    for (int i = 0; i < v->ntables; i++)
        if (!strcmp(v->tables[i].table, "t")) return &v->tables[i];
    return NULL;
}

static void print_row(const char *tag, int rc, const tsdb_restore_verify_t *v) {
    const tsdb_restore_verify_table_t *r = row_t(v);
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

#define R2_DIR "/tmp/tsdb_rev_prefereq"
#define R2_BK  "/tmp/tsdb_rev_prefereq_bk"

int main(void) {
    printf("\n[R2] a rot in the FIRST of two blocks sharing one key\n");

    rm_rf(R2_DIR);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(R2_DIR, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "t", &t));

    /* The set is taken after ONE group has landed. */
    write_chunk(t);
    OK(tsdb_table_flush(db, "t"));
    rm_rf(R2_BK);
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    OK(tsdb_backup_create(db, R2_BK, &rep));
    if (!rep.complete) FAIL("[R2] backup incomplete");
    tsdb_restore_report_free(&rep);

    /* Then the SAME group lands a second time — a re-sent replication group.
     * The target now holds two blocks under one (ts_min, count); the set
     * carries one.  Both are byte-identical, so the pair is interchangeable. */
    write_chunk(t);
    OK(tsdb_table_flush(db, "t"));
    tsdb_close(db);

    char parts[8][32];
    int np = part_names(R2_DIR, parts, 8);
    if (np != 1) FAIL("[R2] expected 1 partition, found %d", np);

    /* Rot the FIRST of the two. */
    rot_first_block(R2_DIR, parts[0], "val");

    OK(tsdb_open(R2_DIR, &db));
    {
        tsdb_result_t *r = NULL;
        int64_t rows = 0;
        int rc = tsdb_query(db, "SELECT ts, val FROM t", &r);
        if (rc == TSDB_OK && r) while (tsdb_result_next(r) > 0) rows++;
        if (r) tsdb_result_free(r);
        printf("[R2] engine: SELECT ts, val -> rc=%d (%s) rows=%lld\n",
               rc, tsdb_errstr(rc), (long long)rows);
        if (rc == TSDB_OK)
            FAIL("[R2] the engine still reads the table — the rot did not take, "
                 "the case proves nothing");
    }

    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int irc = tsdb_restore_verify(db, R2_BK, TSDB_RESTORE_VERIFY_INDEX, &v);
    print_row("[R2] index", irc, &v);
    tsdb_restore_verify_free(&v);

    memset(&v, 0, sizeof(v));
    int drc = tsdb_restore_verify(db, R2_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    print_row("[R2] deep ", drc, &v);
    tsdb_restore_verify_free(&v);

    tsdb_close(db);

    if (drc == TSDB_OK) {
        printf("\n[R2] *** FALSE OK ***: DEEP certified rc=0 for a database "
               "whose SELECT returns TSDB_ERR_CORRUPT\n");
        FAIL("[R2] deep verify certified an unreadable database");
    }
    printf("\n=== test_rev_prefereq: deep did NOT certify (rc=%d) ===\n", drc);
    return 0;
}
