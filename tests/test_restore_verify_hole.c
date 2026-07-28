/* test_restore_verify_hole.c — tsdb_restore_verify must not certify a database
 * the ENGINE refuses to read.
 *
 * THE BUG.  tsdb_migrate_digest folds two counters over different axes:
 *
 *     for (ci = 0; ci < s->ncols; ci++)
 *         for (bi ...) { if (ci == ts_col_idx) st.rows += count;   <- ts ONLY
 *                        st.blocks++; }                            <- ALL cols
 *
 * so `rows` is a ts-only measure and `blocks` is an all-column measure.  A
 * target that loses one value column's blocks therefore keeps every row and
 * drops blocks — bit for bit the signature tsdb_restore_verify hands to
 * TSDB_RESTORE_TARGET_REENCODED ("at least the set's rows, in FEWER blocks —
 * the compactor; nothing is missing").  The index level passed it, and the DEEP
 * level DECLINED on that classification (blocks_checked = 0, "nothing is
 * reported missing because nothing was measured"), so the one call an operator
 * makes during an incident answered TSDB_OK at BOTH levels for a database whose
 * `SELECT ts, val` returns TSDB_ERR_CORRUPT.
 *
 * The premise the classification rests on is false: "fewer blocks holding at
 * least the set's rows" cannot mean "re-encoded" when the two counts do not
 * measure the same thing.
 *
 * Cases:
 *   [H1] a target that lost a whole non-ts column's blocks must NOT verify
 *        clean at EITHER level, and DEEP must name the blocks.
 *   [H2] "nothing was measured" is not "nothing is missing": a DEEP verify that
 *        looked at zero blocks while the manifest claims some must not answer
 *        TSDB_OK.
 *   [H3] the control that keeps [H1] from becoming a false alarm: a target the
 *        database's OWN compactor re-encoded, with nothing lost, still verifies
 *        clean at both levels and still reports `reencoded`.
 *   [H4] the mixed shape: ONE compacted partition must not switch off the
 *        per-block containment check for the others — a column lost in an
 *        un-compacted partition is still named while a compacted partition sits
 *        beside it.
 *   [H5] a .seq sidecar claiming more columns than the schema has must not be
 *        stamped into the restored partition: rule 2 of
 *        part_col_absence_is_late_add would then refuse zero-fill for EVERY
 *        column of it.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
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
#define NS_DAY  86400000000000LL
#define DAY_B   (DAY_A + NS_DAY)
#define STEP_NS 1000000LL

/* ---- fs helpers ---------------------------------------------------------- */

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

/* Partition directory names under <dir>/t, ascending.  Returns how many. */
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
    for (int i = 1; i < n; i++) {
        char k[32]; snprintf(k, sizeof(k), "%s", out[i]);
        int j = i - 1;
        while (j >= 0 && strcmp(out[j], k) > 0) { snprintf(out[j + 1], 32, "%s", out[j]); j--; }
        snprintf(out[j + 1], 32, "%s", k);
    }
    return n;
}

/* The shape a lost replication group / interrupted import / half-deleted
 * partition leaves behind: the column's index AND its data, both gone.  Exactly
 * what tests/test_restore_ncols.c [N1] does to a SOURCE. */
static void drop_column_files(const char *dir, const char *part, const char *col) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/t/%s/%s.idx", dir, part, col);
    if (unlink(p) != 0) FAIL("could not unlink %s: %s", p, strerror(errno));
    snprintf(p, sizeof(p), "%s/t/%s/%s.col", dir, part, col);
    if (unlink(p) != 0) FAIL("could not unlink %s: %s", p, strerror(errno));
}

/* ---- db helpers ---------------------------------------------------------- */

static tsdb_db_t *fresh_db(const char *dir, tsdb_table_t **out_t) {
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(dir, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },   /* trailing column: rule 1 is vacuous */
    };
    OK(tsdb_create_table(db, "t", cols, 2, "ts"));
    OK(tsdb_open_table(db, "t", out_t));
    return db;
}

static void write_chunk(tsdb_table_t *t, int64_t base, int from, int n) {
    tsdb_batch_t *b = NULL;
    OK(tsdb_batch_begin(t, &b));
    for (int i = from; i < from + n; i++) {
        OK(tsdb_batch_row_ts(b, base + (int64_t)i * STEP_NS));
        OK(tsdb_batch_row_f64(b, 1, (double)i));
        OK(tsdb_batch_row_end(b));
    }
    OK(tsdb_batch_commit(b));
}

static void backup_to(tsdb_db_t *db, const char *bk) {
    rm_rf(bk);
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_backup_create(db, bk, &rep);
    if (rc != TSDB_OK || !rep.complete)
        FAIL("backup rc=%d (%s) complete=%d", rc, tsdb_errstr(rc), rep.complete);
    tsdb_restore_report_free(&rep);
}

/* The engine's own verdict on the torn column — the thing verify is supposed to
 * agree with.  Returns the query rc. */
static int read_val(tsdb_db_t *db, int64_t *out_rows) {
    tsdb_result_t *r = NULL;
    int64_t rows = 0;
    int rc = tsdb_query(db, "SELECT ts, val FROM t", &r);
    if (rc == TSDB_OK && r) while (tsdb_result_next(r) > 0) rows++;
    if (r) tsdb_result_free(r);
    if (out_rows) *out_rows = rows;
    return rc;
}

static const tsdb_restore_verify_table_t *row_t(const tsdb_restore_verify_t *v,
                                                const char *name) {
    for (int i = 0; i < v->ntables; i++)
        if (!strcmp(v->tables[i].table, name)) return &v->tables[i];
    return NULL;
}

static void print_row(const char *tag, int rc, const tsdb_restore_verify_t *v,
                      const char *name) {
    const tsdb_restore_verify_table_t *r = row_t(v, name);
    printf("%s rc=%d (%s) nfailed=%d", tag, rc, tsdb_errstr(rc), v->nfailed);
    if (r)
        printf("  %s: rc=%d rel=%s rows=%llu/%llu checked=%llu missing=%llu "
               "mismatched=%llu",
               name, r->rc, tsdb_restore_target_rel_name(r->target_rel),
               (unsigned long long)r->rows_target,
               (unsigned long long)r->rows_backup,
               (unsigned long long)r->blocks_checked,
               (unsigned long long)r->blocks_missing,
               (unsigned long long)r->blocks_mismatched);
    printf("\n");
}

/* ==========================================================================
 * [H1] a lost non-ts column verifies clean at BOTH levels
 * ======================================================================== */
#define H1_DIR "/tmp/tsdb_vhole_h1"
#define H1_BK  "/tmp/tsdb_vhole_h1_bk"

#define H1_CHUNKS 3
#define H1_ROWS   100

static void case_lost_column(void) {
    printf("\n[H1] a target that lost a whole value column's blocks\n");

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(H1_DIR, &t);
    for (int k = 0; k < H1_CHUNKS; k++) {
        write_chunk(t, DAY_A, k * H1_ROWS, H1_ROWS);
        OK(tsdb_table_flush(db, "t"));
    }
    backup_to(db, H1_BK);

    /* The healthy control first: the set and the target agree, so both levels
     * must certify it.  Without this the case below could pass for any reason. */
    for (int deep = 0; deep <= 1; deep++) {
        tsdb_restore_verify_t v;
        memset(&v, 0, sizeof(v));
        int rc = tsdb_restore_verify(db, H1_BK,
                                     deep ? TSDB_RESTORE_VERIFY_DEEP
                                          : TSDB_RESTORE_VERIFY_INDEX, &v);
        print_row(deep ? "[H1] intact deep " : "[H1] intact index", rc, &v, "t");
        const tsdb_restore_verify_table_t *r = row_t(&v, "t");
        if (rc != TSDB_OK || !r || r->rc != TSDB_OK)
            FAIL("[H1] the INTACT set was not certified at the %s level "
                 "(rc=%d) — the case is not testing what it claims",
                 deep ? "deep" : "index", rc);
        tsdb_restore_verify_free(&v);
    }
    tsdb_close(db);

    /* Now lose the value column, the fcacea4 tear. */
    char parts[8][32];
    int np = part_names(H1_DIR, parts, 8);
    if (np != 1) FAIL("[H1] expected 1 partition, found %d", np);
    drop_column_files(H1_DIR, parts[0], "val");

    OK(tsdb_open(H1_DIR, &db));

    /* THE ENGINE'S OWN ANSWER.  Everything below is only meaningful because
     * this read fails: verify is being asked about a database that no longer
     * answers for one of its columns. */
    int64_t got = 0;
    int qrc = read_val(db, &got);
    printf("[H1] engine: SELECT ts, val -> rc=%d (%s) rows=%lld\n",
           qrc, tsdb_errstr(qrc), (long long)got);
    if (qrc == TSDB_OK)
        FAIL("[H1] the engine still answers SELECT ts, val after the column's "
             "blocks were removed (rows=%lld) — the tear did not take, so there "
             "is nothing for verify to be wrong about", (long long)got);

    for (int deep = 0; deep <= 1; deep++) {
        tsdb_restore_verify_t v;
        memset(&v, 0, sizeof(v));
        int rc = tsdb_restore_verify(db, H1_BK,
                                     deep ? TSDB_RESTORE_VERIFY_DEEP
                                          : TSDB_RESTORE_VERIFY_INDEX, &v);
        print_row(deep ? "[H1] torn   deep " : "[H1] torn   index", rc, &v, "t");
        const tsdb_restore_verify_table_t *r = row_t(&v, "t");
        if (!r) FAIL("[H1] the %s verify did not report table 't'",
                     deep ? "deep" : "index");
        if (rc == TSDB_OK || r->rc == TSDB_OK)
            FAIL("[H1] the %s verify CERTIFIED a database whose SELECT ts, val "
                 "returns %s: verify rc=%d table rc=%d rel=%s checked=%llu "
                 "missing=%llu.  A value column's blocks are gone; 'the target "
                 "holds the set's rows in fewer blocks' is the compactor's "
                 "signature and this is not the compactor",
                 deep ? "deep" : "index", tsdb_errstr(qrc), rc, r->rc,
                 tsdb_restore_target_rel_name(r->target_rel),
                 (unsigned long long)r->blocks_checked,
                 (unsigned long long)r->blocks_missing);
        if (r->target_rel == TSDB_RESTORE_TARGET_REENCODED)
            FAIL("[H1] the %s verify called it '%s' — nothing was re-encoded, a "
                 "column was lost", deep ? "deep" : "index",
                 tsdb_restore_target_rel_name(r->target_rel));
        if (r->blocks_missing == 0)
            FAIL("[H1] the %s verify failed without naming a single missing "
                 "block — an operator cannot act on a bare rc",
                 deep ? "deep" : "index");
        if (deep && r->blocks_checked == 0)
            FAIL("[H1] the deep verify measured NOTHING (blocks_checked=0) and "
                 "still reported %llu missing — the number is not a measurement",
                 (unsigned long long)r->blocks_missing);
        tsdb_restore_verify_free(&v);
    }

    tsdb_close(db);
    rm_rf(H1_DIR); rm_rf(H1_BK);
    printf("[H1] a lost value column fails BOTH levels, and deep names the "
           "blocks\n");
}

/* ==========================================================================
 * [H2] "nothing was measured" is not "nothing is missing"
 * ======================================================================== */
#define H2_DIR "/tmp/tsdb_vhole_h2"
#define H2_BK  "/tmp/tsdb_vhole_h2_bk"

/* Replace the FIRST occurrence of `from` with `to` inside BACKUP.manifest. */
static void manifest_rewrite(const char *bk, const char *from, const char *to) {
    char path[4200];
    snprintf(path, sizeof(path), "%s/BACKUP.manifest", bk);
    FILE *f = fopen(path, "rb");
    if (!f) FAIL("[H2] cannot open %s", path);
    char buf[65536];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';

    char *at = strstr(buf, from);
    if (!at) FAIL("[H2] '%s' not found in the manifest: %s", from, buf);
    char out[65536];
    size_t head = (size_t)(at - buf);
    memcpy(out, buf, head);
    size_t w = head;
    w += (size_t)snprintf(out + w, sizeof(out) - w, "%s%s", to, at + strlen(from));

    f = fopen(path, "wb");
    if (!f) FAIL("[H2] cannot rewrite %s", path);
    if (fwrite(out, 1, w, f) != w) FAIL("[H2] short write to %s", path);
    fclose(f);
}

static void case_nothing_measured(void) {
    printf("\n[H2] a deep verify that looked at nothing\n");

    rm_rf(H2_DIR);
    tsdb_db_t *db = NULL;
    OK(tsdb_open(H2_DIR, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    /* One table, and it is EMPTY — its stream carries no block record at all. */
    OK(tsdb_create_table(db, "e", cols, 2, "ts"));
    backup_to(db, H2_BK);

    /* The set now claims blocks its stream does not carry.  However that
     * happened — a set assembled from two runs, a stream truncated at its first
     * record, a manifest copied from the wrong directory — the check has to
     * answer for the claim, and the only honest answer is "I could not look".
     * It used to answer TSDB_OK. */
    manifest_rewrite(H2_BK, "\"blocks\":0", "\"blocks\":7");

    tsdb_restore_verify_t v;
    memset(&v, 0, sizeof(v));
    int rc = tsdb_restore_verify(db, H2_BK, TSDB_RESTORE_VERIFY_DEEP, &v);
    print_row("[H2] deep        ", rc, &v, "e");
    const tsdb_restore_verify_table_t *r = row_t(&v, "e");
    if (!r) FAIL("[H2] the deep verify did not report table 'e'");
    if (r->blocks_checked != 0)
        FAIL("[H2] the deep verify checked %llu block(s) against a stream that "
             "carries none — the case is not testing what it claims",
             (unsigned long long)r->blocks_checked);
    if (rc == TSDB_OK || r->rc == TSDB_OK)
        FAIL("[H2] the deep verify answered TSDB_OK with blocks_checked=0 "
             "against a manifest claiming 7 block(s): verify rc=%d table rc=%d. "
             " Nothing was measured, which is not the same as nothing being "
             "missing", rc, r->rc);
    tsdb_restore_verify_free(&v);

    tsdb_close(db);
    rm_rf(H2_DIR); rm_rf(H2_BK);
    printf("[H2] a deep level that measured nothing does not answer clean\n");
}

/* ==========================================================================
 * [H3] / [H4] a re-encoded partition beside an intact one
 * ======================================================================== */

/* One commit is one block per column, and the compactor only rewrites a column
 * carrying at least COMPACT_THRESHOLD_DEFAULT (16) of them. */
#define MIX_CHUNKS 20
#define MIX_ROWS   100

/* The database's OWN compactor, stock options, driven synchronously — the same
 * thing the background thread does on its 5 s timer.  Only `part` is aged, so
 * every other partition stays "hot" and is left alone. */
static void compact_one(tsdb_db_t *db, const char *dir, const char *part) {
    char p[4300];
    snprintf(p, sizeof(p), "%s/t/%s", dir, part);
    struct timeval tv[2];
    tv[0].tv_sec = time(NULL) - 3600; tv[0].tv_usec = 0;
    tv[1] = tv[0];
    if (utimes(p, tv) != 0) FAIL("utimes(%s): %s", p, strerror(errno));

    tsdb_compactor_opts_t co;
    memset(&co, 0, sizeof(co));
    co.worker_threads = -1;                 /* manual: no background thread */
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
        FAIL("the compactor rewrote nothing — this case would be identical to "
             "an un-compacted one and prove nothing");
}

/* tear == 0: [H3], the false-alarm control — a re-encoded target with nothing
 *            lost still verifies clean.
 * tear != 0: [H4], the mixed shape — one compacted partition must not switch
 *            off containment for the other. */
static void case_mixed(int tear) {
    char dir[64], bk[64];
    snprintf(dir, sizeof(dir), "/tmp/tsdb_vhole_h%d", tear ? 4 : 3);
    snprintf(bk,  sizeof(bk),  "/tmp/tsdb_vhole_h%d_bk", tear ? 4 : 3);
    printf("\n[H%d] a compacted partition %s\n", tear ? 4 : 3,
           tear ? "beside one that lost a column" : "and nothing lost");

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(dir, &t);
    for (int k = 0; k < MIX_CHUNKS; k++) {
        write_chunk(t, DAY_A, k * MIX_ROWS, MIX_ROWS);
        OK(tsdb_table_flush(db, "t"));      /* one block per column, per chunk */
    }
    write_chunk(t, DAY_B, 0, MIX_ROWS);     /* a second, single-block partition */
    OK(tsdb_table_flush(db, "t"));
    backup_to(db, bk);

    char parts[8][32];
    int np = part_names(dir, parts, 8);
    if (np != 2) FAIL("[H%d] expected 2 partitions, found %d", tear ? 4 : 3, np);
    compact_one(db, dir, parts[0]);
    tsdb_close(db);

    if (tear) drop_column_files(dir, parts[1], "val");

    OK(tsdb_open(dir, &db));

    int64_t got = 0;
    int qrc = read_val(db, &got);
    printf("[H%d] engine: SELECT ts, val -> rc=%d (%s) rows=%lld\n",
           tear ? 4 : 3, qrc, tsdb_errstr(qrc), (long long)got);
    if (tear && qrc == TSDB_OK)
        FAIL("[H4] the engine still answers SELECT ts, val — the tear did not "
             "take");
    if (!tear && qrc != TSDB_OK)
        FAIL("[H3] the engine refuses SELECT ts, val on a target that only got "
             "compacted (rc=%d) — the control is not a control", qrc);

    for (int deep = 0; deep <= 1; deep++) {
        tsdb_restore_verify_t v;
        memset(&v, 0, sizeof(v));
        int rc = tsdb_restore_verify(db, bk,
                                     deep ? TSDB_RESTORE_VERIFY_DEEP
                                          : TSDB_RESTORE_VERIFY_INDEX, &v);
        char tag[48];
        snprintf(tag, sizeof(tag), "[H%d] %-5s      ", tear ? 4 : 3,
                 deep ? "deep" : "index");
        print_row(tag, rc, &v, "t");
        const tsdb_restore_verify_table_t *r = row_t(&v, "t");
        if (!r) FAIL("[H%d] the %s verify did not report table 't'",
                     tear ? 4 : 3, deep ? "deep" : "index");

        if (!tear) {
            if (rc != TSDB_OK || r->rc != TSDB_OK)
                FAIL("[H3] a target whose compactor merged its blocks — and "
                     "which holds every row the set carries — was reported "
                     "%s at the %s level (table rc=%d rel=%s missing=%llu). "
                     " That is the false alarm the REENCODED relation exists "
                     "to prevent", tsdb_errstr(rc), deep ? "deep" : "index",
                     r->rc, tsdb_restore_target_rel_name(r->target_rel),
                     (unsigned long long)r->blocks_missing);
            if (r->target_rel != TSDB_RESTORE_TARGET_REENCODED)
                FAIL("[H3] the %s verify reported rel=%s, want reencoded — the "
                     "compaction did not change the layout, so this case is "
                     "vacuous", deep ? "deep" : "index",
                     tsdb_restore_target_rel_name(r->target_rel));
            if (r->blocks_missing || r->blocks_mismatched)
                FAIL("[H3] the %s verify claimed missing=%llu mismatched=%llu "
                     "against a target holding every row of the set",
                     deep ? "deep" : "index",
                     (unsigned long long)r->blocks_missing,
                     (unsigned long long)r->blocks_mismatched);
            /* Clean, but not silently: the merged partition's blocks were
             * looked at and could not be resolved, and the report has to say
             * so rather than let missing=0 read as "all present". */
            if (deep && r->blocks_unresolved != MIX_CHUNKS * 2)
                FAIL("[H3] the deep verify reported unresolved=%llu, want %d — "
                     "the compacted partition's blocks cannot be looked up, and "
                     "a check that does not disclose that is answering a "
                     "question it did not ask",
                     (unsigned long long)r->blocks_unresolved, MIX_CHUNKS * 2);
            tsdb_restore_verify_free(&v);
            continue;
        }

        if (rc == TSDB_OK || r->rc == TSDB_OK)
            FAIL("[H4] the %s verify CERTIFIED a database whose SELECT ts, val "
                 "returns %s: one partition was compacted, the OTHER lost its "
                 "value column.  verify rc=%d table rc=%d rel=%s checked=%llu "
                 "missing=%llu", deep ? "deep" : "index", tsdb_errstr(qrc),
                 rc, r->rc, tsdb_restore_target_rel_name(r->target_rel),
                 (unsigned long long)r->blocks_checked,
                 (unsigned long long)r->blocks_missing);
        if (deep && r->blocks_missing == 0)
            FAIL("[H4] the deep verify failed without naming the lost "
                 "partition's block — a compacted partition must not switch "
                 "off the per-block check for the partitions beside it "
                 "(checked=%llu)", (unsigned long long)r->blocks_checked);
        /* And it must name ONLY that block.  Exactly one value block was lost;
         * the compacted partition's are re-encoded, not gone, and folding them
         * into the same counter hands the operator a number they would act on
         * wrongly — the reason the whole-table decline was chosen over running
         * the check blind. */
        if (deep && (r->blocks_missing != 1 ||
                     r->blocks_unresolved != MIX_CHUNKS * 2))
            FAIL("[H4] the deep verify reported missing=%llu unresolved=%llu, "
                 "want 1 / %d — exactly one value block was lost and the "
                 "compacted partition's %d were merged, not lost",
                 (unsigned long long)r->blocks_missing,
                 (unsigned long long)r->blocks_unresolved,
                 MIX_CHUNKS * 2, MIX_CHUNKS * 2);
        tsdb_restore_verify_free(&v);
    }

    tsdb_close(db);
    rm_rf(dir); rm_rf(bk);
    printf("[H%d] %s\n", tear ? 4 : 3,
           tear ? "the un-compacted partition's lost column is still named"
                : "a purely re-encoded target still verifies clean");
}

/* ==========================================================================
 * [H5] a sidecar claiming more columns than the schema has
 * ======================================================================== */
#define H5_SRC "/tmp/tsdb_vhole_h5_src"
#define H5_BK  "/tmp/tsdb_vhole_h5_bk"
#define H5_DST "/tmp/tsdb_vhole_h5_dst"

/* Rewrite every "<part> <seq> <ncols>" line with an ncols the target's schema
 * cannot back.  Under TSDB_MAX_COLS, so the only bound that rejects it is the
 * target's own column count. */
#define H5_BOGUS_NCOLS 200

static void inflate_seq_sidecar(const char *bk) {
    char path[4200];
    snprintf(path, sizeof(path), "%s/t.seq", bk);
    FILE *f = fopen(path, "rb");
    if (!f) FAIL("[H5] cannot open %s", path);
    char buf[16384];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    if (n == 0) FAIL("[H5] %s is empty — nothing to inflate", path);

    char out[16384];
    size_t w = 0;
    int lines = 0;
    char *save = NULL;
    for (char *line = strtok_r(buf, "\n", &save); line;
         line = strtok_r(NULL, "\n", &save)) {
        char pname[32]; unsigned long long seq = 0; unsigned nc = 0;
        if (sscanf(line, "%31s %llu %u", pname, &seq, &nc) < 2) continue;
        w += (size_t)snprintf(out + w, sizeof(out) - w, "%s %llu %u\n",
                              pname, seq, (unsigned)H5_BOGUS_NCOLS);
        lines++;
    }
    if (lines == 0) FAIL("[H5] %s carried no partition line", path);

    f = fopen(path, "wb");
    if (!f) FAIL("[H5] cannot rewrite %s", path);
    if (fwrite(out, 1, w, f) != w) FAIL("[H5] short write to %s", path);
    fclose(f);
}

static void case_sidecar_ncols_clamp(void) {
    printf("\n[H5] a .seq sidecar claiming %d columns for a 2-column table\n",
           H5_BOGUS_NCOLS);

    tsdb_table_t *t = NULL;
    tsdb_db_t *db = fresh_db(H5_SRC, &t);
    write_chunk(t, DAY_A, 0, 500);
    OK(tsdb_table_flush(db, "t"));
    backup_to(db, H5_BK);
    tsdb_close(db);

    inflate_seq_sidecar(H5_BK);

    rm_rf(H5_DST);
    OK(tsdb_open(H5_DST, &db));
    tsdb_restore_report_t rep;
    memset(&rep, 0, sizeof(rep));
    int rc = tsdb_restore_run(db, H5_BK, &rep);
    if (rc != TSDB_OK || !rep.complete)
        FAIL("[H5] restore rc=%d (%s) complete=%d", rc, tsdb_errstr(rc),
             rep.complete);
    tsdb_restore_report_free(&rep);
    tsdb_close(db);

    char parts[8][32];
    int np = part_names(H5_DST, parts, 8);
    if (np != 1) FAIL("[H5] expected 1 restored partition, found %d", np);
    char idx[4300];
    snprintf(idx, sizeof(idx), "%s/t/%s/ts.idx", H5_DST, parts[0]);
    uint16_t stamp = tsdb_part_idx_ncols(idx);
    printf("[H5] restored ts.idx ncols stamp = %u\n", (unsigned)stamp);
    if (stamp > 2)
        FAIL("[H5] the restore stamped ncols=%u into a partition whose schema "
             "has 2 columns.  Rule 2 of part_col_absence_is_late_add reads that "
             "stamp as 'this partition was written with %u columns', so EVERY "
             "column of it is now a column whose blocks are gone — a legitimate "
             "ALTER-added column on this partition can never zero-fill again",
             (unsigned)stamp, (unsigned)stamp);

    /* And the copy still reads. */
    OK(tsdb_open(H5_DST, &db));
    int64_t got = 0;
    int qrc = read_val(db, &got);
    printf("[H5] restored: SELECT ts, val -> rc=%d (%s) rows=%lld\n",
           qrc, tsdb_errstr(qrc), (long long)got);
    if (qrc != TSDB_OK || got != 500)
        FAIL("[H5] the restored copy answers rc=%d (%s) rows=%lld, want 0 / 500",
             qrc, tsdb_errstr(qrc), (long long)got);
    tsdb_close(db);

    rm_rf(H5_SRC); rm_rf(H5_BK); rm_rf(H5_DST);
    printf("[H5] the sidecar's claim is clamped at the target's column count\n");
}

int main(void) {
    printf("=== test_restore_verify_hole ===\n");
    case_lost_column();
    case_nothing_measured();
    case_mixed(0);
    case_mixed(1);
    case_sidecar_ncols_clamp();
    printf("\n=== test_restore_verify_hole PASSED ===\n");
    return 0;
}
