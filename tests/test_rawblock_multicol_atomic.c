/* test_rawblock_multicol_atomic.c — the raw-block apply path must never
 * publish a partition's ts visibility marker past a group it did not fully
 * receive, and an already-torn partition must be repairable.
 *
 * WHAT THIS BUILDS THAT THE EXISTING TORN-COLUMN TESTS DO NOT
 * -----------------------------------------------------------
 * test_torn_partition_read.c truncates `ts.col`; test_torn_value_column.c
 * truncates `val.col`.  Both tear the DATA file while every column's `.idx`
 * still DECLARES the same block count, so both land in tsdb_part_open's
 * durable-prefix clamp (idx_decl_count[val] >= idx_decl_count[ts]).  Neither
 * touches the write path at all.
 *
 * This test builds the state neither covers: a partition where a non-ts
 * column's `.idx` ITSELF declares fewer blocks than ts's, with the gap in the
 * MIDDLE — the shape the per-(column, block) raw-block applier produces when
 * one push is lost, a receiver crashes between two applies, or two concurrent
 * appliers lose an idx update.  idx_decl_count[val] < idx_decl_count[ts], so
 * the clamp does not apply and the ALTER-vs-hole classifier is what runs.  It
 * is reached only through the WRITE path, and it is the state that made
 * `SELECT val` return TSDB_ERR_CORRUPT while `count(*)` — served from ts —
 * kept reporting every row as present.
 *
 * Case 2 builds a strictly worse variant no existing test reaches: a column
 * with ZERO idx entries in a partition ts has published into.  That is
 * indistinguishable from ALTER TABLE ADD COLUMN on the read side, so the
 * column reads back as fabricated ZEROS with rc == 0 — silent wrong data, not
 * an error.
 *
 * RED ON MAINLINE (9970f8e): every ts apply lands, so
 *   case 1 -> SELECT ts,val,tag returns TSDB_ERR_CORRUPT for the partition;
 *   case 2 -> SELECT returns NROWS rows whose val is silently 0.
 *
 * Single process, no cluster, no sockets: the blocks are captured from a real
 * flush through the raw-block hook and replayed into a second data dir, which
 * is byte-for-byte what a peer receives over TSDB_RPC_RAW_BLOCK_PUSH.
 */

#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/schema.h"
#include "../src/cluster/rawblock.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do {                                                      \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m);   \
                g_fail++; }                                                   \
    else      { printf("PASS: %s\n", m); g_pass++; }                          \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

/* ---- source dataset ------------------------------------------------------ */

#define NBLOCKS    4
#define BLOCK_PTS  1024
#define NROWS      (NBLOCKS * BLOCK_PTS)
#define BASE_TS    1700000000000000000LL
#define STEP_NS    1000000LL

#define COL_TS  0
#define COL_VAL 1
#define COL_TAG 2

/* floor(val) == the row's ts index, so a mis-paired cell is detectable. */
static double val_of(int i) { return (double)i + (double)(i % 997) / 1000.0; }

typedef struct {
    char              table[64];
    uint32_t          part_day;
    uint16_t          col_idx;
    tsdb_block_meta_t meta;
    uint8_t          *bytes;
    size_t            bytes_len;
} cap_blk_t;

#define CAP_MAX 64
typedef struct { cap_blk_t b[CAP_MAX]; int n; } cap_ctx_t;

static int cap_hook(void *ud, tsdb_db_t *db, const char *table, uint32_t day,
                    uint16_t col, const tsdb_block_meta_t *meta,
                    const uint8_t *bytes, size_t blen)
{
    (void)db;
    cap_ctx_t *c = (cap_ctx_t *)ud;
    if (c->n >= CAP_MAX) return TSDB_OK;
    cap_blk_t *e = &c->b[c->n++];
    snprintf(e->table, sizeof(e->table), "%s", table);
    e->part_day = day; e->col_idx = col; e->meta = *meta;
    e->bytes = malloc(blen ? blen : 1);
    memcpy(e->bytes, bytes, blen);
    e->bytes_len = blen;
    return TSDB_OK;
}

static void cap_free(cap_ctx_t *c) {
    for (int i = 0; i < c->n; i++) free(c->b[i].bytes);
    c->n = 0;
}

/* Capture NBLOCKS blocks per column from a real flush of `src_dir`. */
static void capture_source(const char *src_dir, cap_ctx_t *ctx) {
    tsdb_db_t *src = NULL;
    if (tsdb_open(src_dir, &src) != TSDB_OK) { fprintf(stderr, "open src\n"); exit(1); }
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
        { "tag", TSDB_TYPE_INT64     },
    };
    if (tsdb_create_table_ex2(src, "t", cols, 3, "ts",
                              TSDB_CREATE_PART_DAY, BLOCK_PTS) != TSDB_OK) {
        fprintf(stderr, "create src\n"); exit(1);
    }
    tsdb_db_set_raw_block_hook(src, cap_hook, ctx);

    tsdb_table_t *t = NULL;
    tsdb_open_table(src, "t", &t);
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    for (int i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, BASE_TS + (int64_t)i * STEP_NS);
        tsdb_batch_row_f64(b, COL_VAL, val_of(i));
        tsdb_batch_row_i64(b, COL_TAG, (int64_t)i);
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    /* Under TSDB_WAL_ONLY_COMMIT=1 a commit is WAL+memtable only, so drain
     * explicitly: the hook fires from the flush, not from the commit. */
    if (tsdb_db_flush_all(src) != TSDB_OK) { fprintf(stderr, "flush src\n"); exit(1); }
    tsdb_close(src);
}

/* Open a replica data dir that knows the table but holds no data. */
static tsdb_db_t *open_replica(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open replica\n"); exit(1); }
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
        { "tag", TSDB_TYPE_INT64     },
    };
    if (tsdb_create_table_local(db, "t", cols, 3, "ts") != TSDB_OK) {
        fprintf(stderr, "create replica table\n"); exit(1);
    }
    return db;
}

static int apply_blk(tsdb_db_t *dst, const cap_blk_t *e, uint32_t flags) {
    tsdb_rawblock_push_t r;
    memset(&r, 0, sizeof(r));
    snprintf(r.table, sizeof(r.table), "%s", e->table);
    r.part_day        = e->part_day;
    r.col_idx         = e->col_idx;
    r.codec           = e->meta.codec;
    r.flags           = e->meta.flags;
    r.count           = e->meta.count;
    r.ts_min          = e->meta.ts_min;
    r.ts_max          = e->meta.ts_max;
    r.stats_min       = e->meta.stats_min;
    r.stats_max       = e->meta.stats_max;
    r.stats_sum       = e->meta.stats_sum;
    r.stats_first     = e->meta.stats_first;
    r.stats_last      = e->meta.stats_last;
    r.stats_flags     = e->meta.stats_flags;
    r.block_bytes_len = (uint32_t)e->bytes_len;
    r.block_bytes     = e->bytes;
    return tsdb_rawblock_apply_ex(dst, &r, flags);
}

/* index into ctx of the `nth` captured block of column `col` (0-based). */
static int find_blk(const cap_ctx_t *c, uint16_t col, int nth) {
    int seen = 0;
    for (int i = 0; i < c->n; i++) {
        if (c->b[i].col_idx != col) continue;
        if (seen == nth) return i;
        seen++;
    }
    return -1;
}

/* ---- read-back ----------------------------------------------------------- */

/* Scan SELECT ts,val,tag.  *rows = rows returned, *wrong = mis-paired or
 * fabricated cells, *rc = the query rc. */
static void scan_verify(tsdb_db_t *db, int64_t *rows, int *wrong, int *out_rc) {
    tsdb_result_t *r = NULL;
    *rows = 0; *wrong = 0;
    int rc = tsdb_query(db, "SELECT ts, val, tag FROM t", &r);
    *out_rc = rc;
    if (rc != TSDB_OK || !r) { if (r) tsdb_result_free(r); return; }
    while (tsdb_result_next(r) > 0) {
        int64_t ts  = tsdb_result_ts(r, 0);
        double  v   = tsdb_result_f64(r, 1);
        int64_t tg  = tsdb_result_i64(r, 2);
        int64_t idx = (ts - BASE_TS) / STEP_NS;
        if ((int64_t)v != idx || tg != idx) (*wrong)++;
        (*rows)++;
    }
    tsdb_result_free(r);
}

static int64_t q_count(tsdb_db_t *db, int *out_rc) {
    tsdb_result_t *r = NULL;
    int64_t v = -1;
    int rc = tsdb_query(db, "SELECT count(*) FROM t", &r);
    if (out_rc) *out_rc = rc;
    if (rc == TSDB_OK && r) {
        if (tsdb_result_next(r)) v = tsdb_result_i64(r, 0);
    }
    if (r) tsdb_result_free(r);
    return v;
}

/* <replica>/t/<YYYYMMDD> */
static int find_part_dir(const char *root, char *out, size_t cap) {
    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/t", root);
    DIR *d = opendir(tdir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(out, cap, "%s/%s", tdir, e->d_name);
        found = 1;
        break;
    }
    closedir(d);
    return found;
}

static long file_size(const char *p) {
    struct stat st;
    return (stat(p, &st) == 0) ? (long)st.st_size : -1L;
}

static uint32_t idx_block_count(const char *part_dir, const char *col) {
    char p[4096];
    snprintf(p, sizeof(p), "%s/%s.idx", part_dir, col);
    uint32_t cnt = 0;
    (void)tsdb_part_idx_probe(p, NULL, &cnt, NULL, NULL, NULL, NULL, NULL);
    return cnt;
}

static tsdb_schema_t *replica_schema(tsdb_db_t *db) {
    tsdb_table_internal_t *ti = tsdb_db_find_table(db, "t");
    return ti ? tsdb_tbl_schema(ti) : NULL;
}

/* ==========================================================================
 * Case 1 — an interior gap in a non-ts column must NOT be followed by its ts
 * block.  This is the dropped-push shape.
 * ======================================================================== */
static void case_interior_gap_refused(const cap_ctx_t *ctx) {
    printf("\n[1] interior gap in val.idx: the matching ts block is refused\n");
    const char *dir = "/tmp/tsdb_rb_mca_gap";
    rmrf(dir);
    tsdb_db_t *db = open_replica(dir);

    const int DROP = 1;                    /* interior: not first, not last */
    int drop_ix = find_blk(ctx, COL_VAL, DROP);
    CHECK(drop_ix >= 0, "located val block 1 in the capture");

    int busy = 0, other_err = 0, ts_ok = 0;
    for (int i = 0; i < ctx->n; i++) {
        if (i == drop_ix) continue;        /* the lost push */
        int rc = apply_blk(db, &ctx->b[i], TSDB_RB_VERIFY_TS);
        if (ctx->b[i].col_idx == COL_TS) {
            if (rc == TSDB_ERR_BUSY) busy++;
            else if (rc == TSDB_OK)  ts_ok++;
            else                     other_err++;
        } else if (rc != TSDB_OK) {
            other_err++;
        }
    }
    CHECK(busy == 1, "exactly one ts block refused (TSDB_ERR_BUSY)");
    CHECK(ts_ok == NBLOCKS - 1, "every complete group's ts block still lands");
    CHECK(other_err == 0, "no other apply failed");

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "replica partition exists");
    CHECK(idx_block_count(part, "ts") == NBLOCKS - 1,
          "ts.idx does not advertise the incomplete group");
    CHECK(idx_block_count(part, "val") == NBLOCKS - 1, "val.idx is short by one");

    tsdb_close(db);
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }

    int64_t rows = 0; int wrong = 0, rc = 0, crc = 0;
    scan_verify(db, &rows, &wrong, &rc);
    int64_t cnt = q_count(db, &crc);
    printf("  rows=%lld wrong=%d scan_rc=%d count=%lld\n",
           (long long)rows, wrong, rc, (long long)cnt);

    /* THE CORE REGRESSION.  Pre-fix the refused ts block lands, val.idx ends
     * up one block short of ts.idx with the gap in the middle, tsdb_part_open
     * synthesises a HOLE, and the whole query fails. */
    CHECK(rc == TSDB_OK, "replica scan succeeds (not TSDB_ERR_CORRUPT) [core]");
    CHECK(wrong == 0, "no mis-paired or fabricated cell [core]");
    CHECK(rows == (int64_t)(NROWS - BLOCK_PTS),
          "replica is BEHIND by exactly the incomplete group, not torn");
    CHECK(cnt == rows, "count(*) agrees with the scan (no phantom rows)");

    tsdb_close(db);
    rmrf(dir);
}

/* ==========================================================================
 * Case 2 — a column whose group never arrived at all on a FRESH partition.
 * Pre-fix this reads back as fabricated zeros with rc == 0.
 * ======================================================================== */
static void case_whole_column_missing(const cap_ctx_t *ctx) {
    printf("\n[2] a whole column's pushes lost: ts must not publish at all\n");
    const char *dir = "/tmp/tsdb_rb_mca_zero";
    rmrf(dir);
    tsdb_db_t *db = open_replica(dir);

    int busy = 0, ts_ok = 0;
    for (int i = 0; i < ctx->n; i++) {
        if (ctx->b[i].col_idx == COL_VAL) continue;   /* every val push lost */
        int rc = apply_blk(db, &ctx->b[i], TSDB_RB_VERIFY_TS);
        if (ctx->b[i].col_idx == COL_TS) {
            if (rc == TSDB_ERR_BUSY) busy++;
            else if (rc == TSDB_OK)  ts_ok++;
        }
    }
    CHECK(busy == NBLOCKS && ts_ok == 0,
          "every ts block refused while val has no block here");

    tsdb_close(db);
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); exit(1); }
    int64_t rows = 0; int wrong = 0, rc = 0;
    scan_verify(db, &rows, &wrong, &rc);
    printf("  rows=%lld wrong=%d scan_rc=%d\n", (long long)rows, wrong, rc);
    /* Pre-fix: rows == NROWS and wrong == NROWS (val reads as 0 for every
     * row, rc == TSDB_OK).  Fabricating a value is worse than an error — it
     * poisons aggregates AND hides the loss from anti-entropy. */
    CHECK(rows == 0, "no rows are advertised for a group that never landed");
    CHECK(wrong == 0, "no fabricated zero is ever returned [core]");
    tsdb_close(db);
    rmrf(dir);
}

/* ==========================================================================
 * Case 3 — the ALTER TABLE ADD COLUMN exemption still passes.  A column with
 * ZERO blocks in a partition ts has ALREADY published into is a late add, not
 * a hole, and must not block the marker.
 * ======================================================================== */
static void case_alter_exemption(const cap_ctx_t *ctx) {
    printf("\n[3] a genuinely late-added column does not block the marker\n");
    const char *dir = "/tmp/tsdb_rb_mca_alter";
    rmrf(dir);
    tsdb_db_t *db = open_replica(dir);

    /* Land group 0 for ts+val only (flags 0 = the raw primitive), so the
     * partition exists with 1 ts block while tag.idx has no entries at all —
     * exactly the on-disk shape of a column added after this partition. */
    CHECK(apply_blk(db, &ctx->b[find_blk(ctx, COL_VAL, 0)], 0) == TSDB_OK,
          "seed: val block 0");
    CHECK(apply_blk(db, &ctx->b[find_blk(ctx, COL_TS, 0)], 0) == TSDB_OK,
          "seed: ts block 0");

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "partition exists");
    CHECK(idx_block_count(part, "tag") == 0, "tag column has no blocks here");

    tsdb_schema_t *s = replica_schema(db);
    CHECK(s != NULL, "replica schema resolved");

    /* Group 1 with val present and tag still absent: the exemption applies. */
    CHECK(apply_blk(db, &ctx->b[find_blk(ctx, COL_VAL, 1)], 0) == TSDB_OK,
          "seed: val block 1");
    int rc = apply_blk(db, &ctx->b[find_blk(ctx, COL_TS, 1)], TSDB_RB_VERIFY_TS);
    CHECK(rc == TSDB_OK, "ts publishes over a zero-block (ALTER-shaped) column");

    /* And the direct unit form: a column that HAS blocks but not this key is
     * still refused, so the exemption is not a blanket pass. */
    if (s) {
        const cap_blk_t *ts3 = &ctx->b[find_blk(ctx, COL_TS, 3)];
        char miss[64];
        int vrc = tsdb_part_ts_publish_ready(s, part, ts3->meta.ts_min,
                                             ts3->meta.count, miss, sizeof(miss));
        CHECK(vrc == TSDB_ERR_BUSY && strcmp(miss, "val") == 0,
              "a column WITH blocks but not this one is still refused");
    }
    tsdb_close(db);
    rmrf(dir);
}

/* ==========================================================================
 * Case 4 — repair a partition that is ALREADY torn (written before the fix).
 * ======================================================================== */
static void case_repair_existing_tear(const cap_ctx_t *ctx) {
    printf("\n[4] repair of a partition torn before the fix\n");
    const char *dir = "/tmp/tsdb_rb_mca_repair";
    rmrf(dir);
    tsdb_db_t *db = open_replica(dir);

    /* Build the pre-fix on-disk state by hand: flags == 0 is exactly the old
     * unverified applier, so this is byte-for-byte what mainline produces. */
    const int DROP = 1;
    int drop_ix = find_blk(ctx, COL_VAL, DROP);
    int seed_err = 0;
    for (int i = 0; i < ctx->n; i++) {
        if (i == drop_ix) continue;
        if (apply_blk(db, &ctx->b[i], 0) != TSDB_OK) seed_err++;
    }
    CHECK(seed_err == 0, "hand-built tear: every other block landed");

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "torn partition exists");
    CHECK(idx_block_count(part, "ts") == NBLOCKS &&
          idx_block_count(part, "val") == NBLOCKS - 1,
          "hand-built tear: ts.idx declares 4 blocks, val.idx 3");

    tsdb_close(db);
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen torn\n"); exit(1); }
    int64_t rows = 0; int wrong = 0, rc = 0, crc = 0;
    scan_verify(db, &rows, &wrong, &rc);
    int64_t cnt = q_count(db, &crc);
    printf("  torn: scan_rc=%d rows=%lld count=%lld\n",
           rc, (long long)rows, (long long)cnt);
    /* The read side already refuses to invent a value, so the query fails
     * while count(*) still reports every row: loud for readers, INVISIBLE to
     * anti-entropy, which compares (count, max_ts). */
    CHECK(rc != TSDB_OK, "torn partition fails the value scan (read side is safe)");
    CHECK(cnt == NROWS, "count(*) is blind to the tear — why repair is needed");

    /* The retraction republishes ts.idx, so it must not change the idx
     * VERSION or drop the max_seq WAL redo checkpoint (a silent V4->V3
     * downgrade would make recovery replay records it must skip). */
    char ts_idx[4200];
    snprintf(ts_idx, sizeof(ts_idx), "%s/ts.idx", part);
    uint16_t ver_before = 0; uint64_t seq_before = 0;
    (void)tsdb_part_idx_probe(ts_idx, &ver_before, NULL, NULL, NULL,
                              NULL, NULL, &seq_before);

    tsdb_schema_t *s = replica_schema(db);
    uint32_t retracted = 0;
    CHECK(s && tsdb_part_ts_retract_unpaired(s, part, &retracted) == TSDB_OK,
          "repair: tsdb_part_ts_retract_unpaired succeeded");

    uint16_t ver_after = 0; uint64_t seq_after = 0;
    (void)tsdb_part_idx_probe(ts_idx, &ver_after, NULL, NULL, NULL,
                              NULL, NULL, &seq_after);
    CHECK(ver_after == ver_before && seq_after == seq_before,
          "repair preserves the idx version and max_seq checkpoint");
    CHECK(retracted == (uint32_t)(NBLOCKS - DROP),
          "repair: ts lowered to the longest fully-paired prefix");

    uint32_t again = 99;
    CHECK(s && tsdb_part_ts_retract_unpaired(s, part, &again) == TSDB_OK &&
          again == 0, "repair is idempotent: a second call retracts 0");

    tsdb_close(db);
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen repaired\n"); exit(1); }
    scan_verify(db, &rows, &wrong, &rc);
    cnt = q_count(db, &crc);
    printf("  repaired: scan_rc=%d rows=%lld wrong=%d count=%lld\n",
           rc, (long long)rows, wrong, (long long)cnt);
    CHECK(rc == TSDB_OK && wrong == 0,
          "repaired partition reads cleanly with no wrong values");
    CHECK(rows == (int64_t)(DROP * BLOCK_PTS) && cnt == rows,
          "repaired partition exposes exactly the paired prefix");

    /* Non-destructive: re-landing the missing block heals the partition
     * upward, because nothing was deleted. */
    CHECK(apply_blk(db, &ctx->b[drop_ix], 0) == TSDB_OK, "heal: re-apply val block 1");
    for (int b = DROP; b < NBLOCKS; b++)
        (void)apply_blk(db, &ctx->b[find_blk(ctx, COL_TS, b)], TSDB_RB_VERIFY_TS);

    tsdb_close(db);
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen healed\n"); exit(1); }
    scan_verify(db, &rows, &wrong, &rc);
    cnt = q_count(db, &crc);
    printf("  healed: scan_rc=%d rows=%lld wrong=%d count=%lld\n",
           rc, (long long)rows, wrong, (long long)cnt);
    CHECK(rc == TSDB_OK && wrong == 0 && rows == NROWS && cnt == NROWS,
          "partition heals back to the full row count, values intact");
    tsdb_close(db);
    rmrf(dir);
}

/* ==========================================================================
 * Case 5 — a refused ts apply writes NOTHING, and the applier's temp file can
 * never be adopted by a crashed compaction swap.
 * ======================================================================== */
static void case_refusal_writes_nothing(const cap_ctx_t *ctx) {
    printf("\n[5] a refusal writes nothing; the applier temp is not swap-adoptable\n");
    const char *dir = "/tmp/tsdb_rb_mca_nowrite";
    rmrf(dir);
    tsdb_db_t *db = open_replica(dir);

    for (int b = 0; b < NBLOCKS; b++) {
        (void)apply_blk(db, &ctx->b[find_blk(ctx, COL_VAL, b)], 0);
        (void)apply_blk(db, &ctx->b[find_blk(ctx, COL_TAG, b)], 0);
    }
    /* Land ts groups 0..1 only, then hand it a group whose val we remove from
     * the picture by asking for a key val does not carry. */
    for (int b = 0; b < 2; b++)
        (void)apply_blk(db, &ctx->b[find_blk(ctx, COL_TS, b)], TSDB_RB_VERIFY_TS);

    char part[4096];
    CHECK(find_part_dir(dir, part, sizeof(part)), "partition exists");
    char ts_col[4200], ts_idx[4200];
    snprintf(ts_col, sizeof(ts_col), "%s/ts.col", part);
    snprintf(ts_idx, sizeof(ts_idx), "%s/ts.idx", part);
    long col_before = file_size(ts_col);
    long idx_before = file_size(ts_idx);

    /* A ts block whose (ts_min,count) no column carries: must be refused with
     * zero bytes written to either file. */
    cap_blk_t bogus = ctx->b[find_blk(ctx, COL_TS, 2)];
    bogus.meta.ts_min -= 7;
    int rc = apply_blk(db, &bogus, TSDB_RB_VERIFY_TS);
    CHECK(rc == TSDB_ERR_BUSY, "unpairable ts block refused");
    CHECK(file_size(ts_col) == col_before, "refusal appended nothing to ts.col");
    CHECK(file_size(ts_idx) == idx_before, "refusal did not touch ts.idx");

    /* Crash between the .col append and the idx publish: the appended bytes
     * are referenced by no idx entry, so they are invisible, and the NEXT
     * apply takes its offset from EOF — landing PAST the orphan with a
     * self-consistent entry.  Simulate the orphan directly and land the
     * remaining ts group on top of it. */
    {
        FILE *f = fopen(ts_col, "ab");
        if (f) { for (int k = 0; k < 137; k++) fputc(0xAB, f); fclose(f); }
        int arc = apply_blk(db, &ctx->b[find_blk(ctx, COL_TS, 2)], TSDB_RB_VERIFY_TS);
        CHECK(arc == TSDB_OK, "apply lands on top of an orphan .col tail");

        tsdb_close(db);
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen orphan\n"); exit(1); }
        int64_t rows = 0; int wrong = 0, srrc = 0;
        scan_verify(db, &rows, &wrong, &srrc);
        CHECK(srrc == TSDB_OK && wrong == 0 && rows == 3 * BLOCK_PTS,
              "an unreferenced .col tail is invisible and mis-pairs nothing");
    }

    /* A crashed applier leaves <col>.idx.rbtmp, NOT <col>.idx.tmp, so a
     * surviving .compact_swap marker cannot roll a partial manifest forward
     * into the live index. */
    {
        char rbtmp[4200], marker[4200], val_idx[4200];
        snprintf(rbtmp,   sizeof(rbtmp),   "%s/val.idx.rbtmp",  part);
        snprintf(marker,  sizeof(marker),  "%s/.compact_swap",  part);
        snprintf(val_idx, sizeof(val_idx), "%s/val.idx",        part);

        FILE *f = fopen(rbtmp, "wb");
        if (f) { fputs("GARBAGE-partial-manifest", f); fclose(f); }
        f = fopen(marker, "w");
        if (f) { fputs("ts\nval\ntag\n", f); fclose(f); }

        long val_idx_before = file_size(val_idx);
        tsdb_close(db);
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen swap\n"); exit(1); }
        int64_t rows = 0; int wrong = 0, srrc = 0;
        scan_verify(db, &rows, &wrong, &srrc);
        CHECK(file_size(val_idx) == val_idx_before,
              "compact-swap recovery did not adopt the applier's temp file");
        CHECK(srrc == TSDB_OK && wrong == 0,
              "partition still reads correctly after swap recovery ran");
    }
    tsdb_close(db);
    rmrf(dir);
}

int main(void) {
    printf("=== test_rawblock_multicol_atomic ===\n");

    const char *src_dir = "/tmp/tsdb_rb_mca_src";
    rmrf(src_dir);
    cap_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    capture_source(src_dir, &ctx);
    printf("captured %d blocks (expect %d cols x %d blocks)\n",
           ctx.n, 3, NBLOCKS);
    CHECK(ctx.n == 3 * NBLOCKS, "captured one block per (column, chunk)");
    /* The flush must emit ts LAST — the property the whole design rests on. */
    CHECK(ctx.b[ctx.n - 1].col_idx == COL_TS, "flush emits the ts column last");

    case_interior_gap_refused(&ctx);
    case_whole_column_missing(&ctx);
    case_alter_exemption(&ctx);
    case_repair_existing_tear(&ctx);
    case_refusal_writes_nothing(&ctx);

    cap_free(&ctx);
    rmrf(src_dir);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
