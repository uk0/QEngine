/* test_dup_ts_rawblock.c — duplicate-timestamp blocks on the REPLICATION and
 * MIGRATION paths.
 *
 * A partition's blocks are identified everywhere in this tree by their CONTENT
 * key — (ts_min, ts_max, count), plus the compressed `size` on the raw-block
 * wire.  That key is NOT unique.  Equal timestamps are accepted and kept in
 * stable insertion order (memtable.c), and a flush cuts a partition into
 * independent `block_points` chunks (part.c), so a run of rows that all carry
 * ONE timestamp produces several genuinely distinct blocks whose keys are
 * identical — and whose ts payloads are BYTE-IDENTICAL, because a block whose
 * ts_min == ts_max holds nothing but that one repeated value.
 *
 * That is an ordinary workload, not a corner case: a bulk load that defaults
 * the timestamp, a per-device snapshot taken at one instant, a backfill of a
 * single bucket.
 *
 * tests/test_dup_ts_block_pair.c covers the plain local scan path.  THIS file
 * covers the two paths where arrival order is not block order, which is why
 * the ordinal has to become DURABLE rather than inferred:
 *
 *   A  replication, blocks applied IN ORDER
 *   B  replication, blocks applied 2,0,1 WITHIN one column  (cross-column
 *      interleaving that preserves each column's internal order is already
 *      covered by tests/test_wire_count_disk.c; reversal within a column is
 *      not, and it is the case that separates a durable ordinal from an
 *      ordinal inferred from arrival position)
 *   C  a RETRIED push (rawblock.c's tail-idempotency check) versus a
 *      genuinely DISTINCT block carrying the identical (size, count, ts_min,
 *      ts_max).  Those two requirements pull in opposite directions on the
 *      current key; that tension is the test.  C0 is the tension itself; C1
 *      replays the whole run and is red for a BROADER reason — see its
 *      comment, and do not read it as evidence about duplicate keys.
 *   D  tsdb_part_ts_publish_ready must require every sibling's SAME ordinal,
 *      not merely SOME block with a matching key.
 *   E  a migration import of a duplicate-key run.
 *   F  the same import INTERRUPTED and RESUMED (migrate.c's resume dedup).
 *   G  absorbing a push is DISCARDING it, so the metadata key alone may never
 *      license it: two distinct value blocks of one duplicate-timestamp run
 *      share (size, count, ts_min, ts_max) and differ only in their bytes.
 *
 * Every case asserts through the engine — count(*) and the answer to
 * `SELECT <col> FROM t`, checked as an exact multiset, never by reading index
 * bytes by hand.  The one structural assertion (C's "the retry did not append
 * a second physical copy") goes through the public tsdb_migrate_digest().
 *
 * MEASURED ON HEAD, byte-identical under TSDB_WAL_ONLY_COMMIT=0 and =1:
 *
 *   A   count(*) 1024 of 3072; SELECT v returns block 0's values only
 *   B   count(*) 1024 of 3072; SELECT v returns block TWO's values — the same
 *       run, applied in a different order, answers differently
 *   C0  re-delivering ordinal 0 is correctly dropped; ordinal 1, which differs
 *       only by ordinal, is dropped TOO — count(*) stays 1024 instead of 2048
 *   C1  6 blocks land as 3, then the replay grows it to 5
 *   D1  the ts push is ACCEPTED; SELECT b then answers with b's ordinal-1
 *       values under ts block 0, rc=0
 *   D2  passes (the over-rejection guard)
 *   E   import lands 2 of 6 blocks, rc=0
 *   F   the resume lands 0 more; count(*) stays 1024
 *
 * Re-running the whole file with DISTINCT timestamps (one line: stamp
 * TS_ONE + i) turns A, B, C0, D1, E and F green, which is what pins each of
 * them on the key collision rather than on replication in general.  C1 stays
 * red there — see its comment.
 *
 * WIRE FIDELITY.  Blocks are pushed through tsdb_rawblock_serialize() ->
 * tsdb_rawblock_parse() -> tsdb_rawblock_apply_ex(), i.e. the actual receive
 * path, not a struct handed straight to the applier.  The ordinal has to
 * survive that round trip, so when it is added to tsdb_block_meta_t and to the
 * push struct's reserved bytes there is exactly ONE line to add here —
 * push_from_meta() below, the same line tsdb_rawblock_replicate() will need.
 */

#include "../src/cluster/rawblock.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../include/tsdb.h"
#include "../include/tsdb_migrate.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); g_fail++; } \
    else { printf("PASS: %s\n", msg); g_pass++; } \
} while(0)

/* schema_create clamps block_points into [1024, TSDB_BLOCK_POINTS], so 1024 is
 * the smallest chunk the flush will cut.  NROWS is an exact multiple of it so
 * every block is FULL and all three share one count — a short trailing block
 * would carry a different count and the keys would no longer collide. */
#define BLOCK_PTS   1024
#define NBLK        3
#define NROWS       (BLOCK_PTS * NBLK)          /* 3072 */

/* ONE timestamp for every row.  ts_min == ts_max == TS_ONE and count ==
 * BLOCK_PTS for all three blocks, so their keys — and their ts payloads — are
 * identical by construction. */
#define TS_ONE      1743465600000000000LL       /* 2025-04-01T00:00:00Z, ns */

/* Sum of 1..n, as int64. */
static int64_t sum_1_to(int64_t n) { return n * (n + 1) / 2; }

/* ---- block capture (mirrors tests/test_wire_count_disk.c) ---------------- */

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
                    const uint8_t *bytes, size_t blen) {
    (void)db;
    cap_ctx_t *c = (cap_ctx_t *)ud;
    if (c->n >= CAP_MAX) return TSDB_OK;
    cap_blk_t *e = &c->b[c->n++];
    snprintf(e->table, sizeof(e->table), "%s", table);
    e->part_day = day;
    e->col_idx  = col;
    e->meta     = *meta;
    e->bytes    = malloc(blen ? blen : 1);
    if (e->bytes && blen) memcpy(e->bytes, bytes, blen);
    e->bytes_len = blen;
    return TSDB_OK;
}

static void cap_free(cap_ctx_t *c) {
    for (int i = 0; i < c->n; i++) free(c->b[i].bytes);
    c->n = 0;
}

/* The nth captured block of column `col`, in flush (block-index) order. */
static const cap_blk_t *pick(const cap_ctx_t *c, int col, int nth) {
    int seen = 0;
    for (int i = 0; i < c->n; i++)
        if (c->b[i].col_idx == (uint16_t)col) {
            if (seen == nth) return &c->b[i];
            seen++;
        }
    return NULL;
}

/* meta -> wire push.  This is the ONE place the test copies block metadata
 * into the push struct; it mirrors tsdb_rawblock_replicate() field for field,
 * so the block's partition-local ordinal gets forwarded from here once both
 * structs carry it. */
static void push_from_meta(tsdb_rawblock_push_t *r, const cap_blk_t *e) {
    memset(r, 0, sizeof(*r));
    snprintf(r->table, sizeof(r->table), "%s", e->table);
    r->part_day        = e->part_day;
    r->col_idx         = e->col_idx;
    r->codec           = e->meta.codec;
    r->flags           = e->meta.flags;
    r->count           = e->meta.count;
    r->ts_min          = e->meta.ts_min;
    r->ts_max          = e->meta.ts_max;
    r->stats_min       = e->meta.stats_min;
    r->stats_max       = e->meta.stats_max;
    r->stats_sum       = e->meta.stats_sum;
    r->stats_first     = e->meta.stats_first;
    r->stats_last      = e->meta.stats_last;
    r->stats_flags     = e->meta.stats_flags;
    r->ord             = e->meta.ord;
    r->block_bytes_len = (uint32_t)e->bytes_len;
    r->block_bytes     = e->bytes;
}

/* Push one captured block through the real receive path: serialize, parse,
 * apply.  `flags` is what the RPC receiver passes (TSDB_RB_VERIFY_TS) or 0 for
 * the local bulk callers. */
static int push_block(tsdb_db_t *dst, const cap_blk_t *e, uint32_t flags) {
    if (!e) return TSDB_ERR_NOTFOUND;
    tsdb_rawblock_push_t r;
    push_from_meta(&r, e);

    uint8_t *buf = NULL; size_t len = 0;
    if (tsdb_rawblock_serialize(&r, &buf, &len) != TSDB_OK) return TSDB_ERR_NOMEM;

    tsdb_rawblock_push_t p;
    memset(&p, 0, sizeof(p));
    int rc = tsdb_rawblock_parse(buf, len, &p);   /* p.block_bytes -> into buf */
    if (rc == TSDB_OK) rc = tsdb_rawblock_apply_ex(dst, &p, flags);
    free(buf);
    return rc;
}

/* Push one captured block as a sender that predates the ordinal field would:
 * marker cleared, so the applier falls back to its historical TAIL compare. */
static int push_block_unmarked(tsdb_db_t *dst, const cap_blk_t *e) {
    if (!e) return TSDB_ERR_NOTFOUND;
    tsdb_rawblock_push_t r;
    push_from_meta(&r, e);
    r.ord.v = 0; r.ord.mark = 0;

    uint8_t *buf = NULL; size_t len = 0;
    if (tsdb_rawblock_serialize(&r, &buf, &len) != TSDB_OK) return TSDB_ERR_NOMEM;
    tsdb_rawblock_push_t p;
    memset(&p, 0, sizeof(p));
    int rc = tsdb_rawblock_parse(buf, len, &p);
    if (rc == TSDB_OK) rc = tsdb_rawblock_apply_ex(dst, &p, 0);
    free(buf);
    return rc;
}

/* ---- engine-level readback ---------------------------------------------- */

typedef struct {
    int      rc;          /* tsdb_query rc                                   */
    int64_t  rows;        /* result rows seen                                */
    int64_t  sum;         /* sum of the single projected i64 column          */
    int64_t  dup;         /* values seen more than once                      */
    int64_t  missing;     /* values in [lo,hi] never seen                    */
    int64_t  outside;     /* values outside [lo,hi]                          */
} scanres_t;

/* `SELECT col FROM table` read back as an exact multiset check over the
 * consecutive integers [lo, hi]. */
static scanres_t scan_ints(tsdb_db_t *db, const char *table, const char *col,
                           int64_t lo, int64_t hi) {
    scanres_t s; memset(&s, 0, sizeof(s));
    char q[160];
    snprintf(q, sizeof(q), "SELECT %s FROM %s", col, table);
    tsdb_result_t *r = NULL;
    s.rc = tsdb_query(db, q, &r);
    if (s.rc != TSDB_OK) return s;

    size_t span = (size_t)(hi - lo + 1);
    unsigned char *seen = calloc(span, 1);
    while (tsdb_result_next(r) == 1) {
        int64_t v = tsdb_result_i64(r, 0);
        s.rows++;
        s.sum += v;
        if (v >= lo && v <= hi) {
            if (seen && seen[v - lo]) s.dup++;
            else if (seen) seen[v - lo] = 1;
        } else {
            s.outside++;
        }
    }
    if (seen) for (size_t i = 0; i < span; i++) if (!seen[i]) s.missing++;
    free(seen);
    tsdb_result_free(r);
    return s;
}

static void scan_report(const char *tag, const scanres_t *s) {
    printf("    %s: rc=%d rows=%lld sum=%lld dup=%lld missing=%lld outside=%lld\n",
           tag, s->rc, (long long)s->rows, (long long)s->sum,
           (long long)s->dup, (long long)s->missing, (long long)s->outside);
}

/* count(*) through the engine.  Returns the value; *out_rows is the number of
 * RESULT rows (1 healthy), *out_rc the query rc. */
static int64_t count_star(tsdb_db_t *db, const char *table,
                          int *out_rows, int *out_rc) {
    char q[160];
    snprintf(q, sizeof(q), "SELECT count(*) FROM %s", table);
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, q, &r);
    *out_rc = rc; *out_rows = -1;
    if (rc != TSDB_OK) return -1;
    int nrows = 0; int64_t cv = -1;
    while (tsdb_result_next(r) == 1) { if (nrows == 0) cv = tsdb_result_i64(r, 0); nrows++; }
    *out_rows = nrows;
    tsdb_result_free(r);
    return cv;
}

/* Physical blocks a table holds, via the public migration digest (it walks
 * every partition's per-column block metadata).  Used only where the defect is
 * a DUPLICATE physical copy, which no query can distinguish from the correct
 * state once the reader pairs by ordinal. */
static uint64_t disk_blocks(tsdb_db_t *db, const char *table) {
    tsdb_mig_stats_t st; memset(&st, 0, sizeof(st));
    if (tsdb_migrate_digest(db, table, &st) != TSDB_OK) return UINT64_MAX;
    return st.blocks;
}

/* ---- fixtures ------------------------------------------------------------ */

static void rmrf(const char *p) {
    char c[600];
    snprintf(c, sizeof(c), "rm -rf %s", p);
    (void)system(c);
}

static tsdb_col_t COLS2[2] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_INT64} };
static tsdb_col_t COLS3[3] = { {"ts", TSDB_TYPE_TIMESTAMP},
                               {"a",  TSDB_TYPE_INT64},
                               {"b",  TSDB_TYPE_INT64} };

/* Build the source: NROWS rows, every one stamped TS_ONE, v = 1..NROWS.
 * Captures the flushed raw blocks into `ctx`. */
static tsdb_db_t *build_src2(const char *dir, cap_ctx_t *ctx, const char *tag) {
    rmrf(dir);
    tsdb_db_t *src = NULL;
    CHECK(tsdb_open(dir, &src) == TSDB_OK, "src: tsdb_open");
    CHECK(tsdb_create_table_ex2(src, "t", COLS2, 2, "ts",
                                TSDB_CREATE_PART_DAY, BLOCK_PTS) == TSDB_OK,
          "src: create t(ts,v) block_points=1024");
    tsdb_db_set_raw_block_hook(src, cap_hook, ctx);

    tsdb_table_t *tbl = NULL;
    CHECK(tsdb_open_table(src, "t", &tbl) == TSDB_OK, "src: open table");
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(tbl, &b);
    for (int i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, TS_ONE);              /* the SAME ts, every row */
        tsdb_batch_row_i64(b, 1, (int64_t)(i + 1));
        tsdb_batch_row_end(b);
    }
    CHECK(tsdb_batch_commit(b) == TSDB_OK, "src: commit");
    /* Under TSDB_WAL_ONLY_COMMIT=1 a commit is WAL + memtable only; drain so
     * both modes reach the identical on-disk state and capture ALL blocks. */
    CHECK(tsdb_db_flush_all(src) == TSDB_OK, "src: flush_all (drain tail)");

    printf("  [%s] captured %d raw blocks\n", tag, ctx->n);
    for (int i = 0; i < ctx->n; i++)
        printf("    cap[%d] col=%u count=%u ts=[%lld,%lld] size=%zu\n",
               i, ctx->b[i].col_idx, ctx->b[i].meta.count,
               (long long)ctx->b[i].meta.ts_min, (long long)ctx->b[i].meta.ts_max,
               ctx->b[i].bytes_len);
    CHECK(ctx->n == 2 * NBLK, "src: flush emitted 2 cols x 3 full blocks");

    /* The collision this file is about, MEASURED rather than assumed: the
     * three ts blocks agree on every field the wire's idempotency key looks
     * at — size, count, ts_min, ts_max — while being three different groups of
     * rows.  It is not luck: ts_min == ts_max means the block holds one
     * repeated value, so the compressed bytes cannot differ. */
    {
        const cap_blk_t *t0 = pick(ctx, 0, 0), *t1 = pick(ctx, 0, 1), *t2 = pick(ctx, 0, 2);
        int collide = (t0 && t1 && t2 &&
                       t0->bytes_len   == t1->bytes_len   && t1->bytes_len   == t2->bytes_len &&
                       t0->meta.count  == t1->meta.count  && t1->meta.count  == t2->meta.count &&
                       t0->meta.ts_min == t1->meta.ts_min && t1->meta.ts_min == t2->meta.ts_min &&
                       t0->meta.ts_max == t1->meta.ts_max && t1->meta.ts_max == t2->meta.ts_max);
        CHECK(collide, "src: all 3 ts blocks share (size,count,ts_min,ts_max)");
    }
    return src;
}

/* A replica that has the schema (SCHEMA_SYNC analogue) and no data. */
static tsdb_db_t *fresh_replica(const char *dir, const tsdb_col_t *cols, int ncols,
                                const char *table) {
    rmrf(dir);
    tsdb_db_t *dst = NULL;
    if (tsdb_open(dir, &dst) != TSDB_OK) return NULL;
    if (tsdb_create_table_local_ex(dst, table, cols, (size_t)ncols, "ts",
                                   0 /*DAY*/, BLOCK_PTS, -1) != TSDB_OK) {
        tsdb_close(dst);
        return NULL;
    }
    return dst;
}

/* ===== A / B: apply a duplicate-key run, in order and reversed ============ */
/*
 * `order` is the per-column apply sequence.  Both columns are applied whole
 * (col-major), so this varies ONLY the order WITHIN a column — cross-column
 * interleaving is test_wire_count_disk.c's case.
 */
static void test_apply_order(const int *order, const char *label) {
    printf("\n--- %s ---\n", label);
    char sd[256], dd[256];
    snprintf(sd, sizeof(sd), "/tmp/tsdb_dtrb_src_%d_%c", (int)getpid(), label[0]);
    snprintf(dd, sizeof(dd), "/tmp/tsdb_dtrb_dst_%d_%c", (int)getpid(), label[0]);

    cap_ctx_t ctx; memset(&ctx, 0, sizeof(ctx));
    tsdb_db_t *src = build_src2(sd, &ctx, label);

    tsdb_db_t *dst = fresh_replica(dd, COLS2, 2, "t");
    CHECK(dst != NULL, "dst: replica opened with schema");

    int all_ok = 1;
    for (int col = 0; col < 2 && dst; col++)
        for (int j = 0; j < NBLK; j++) {
            const cap_blk_t *e = pick(&ctx, col, order[j]);
            int rc = push_block(dst, e, 0);
            if (rc != TSDB_OK) {
                all_ok = 0;
                printf("    apply col=%d block=%d -> rc=%d\n", col, order[j], rc);
            }
        }
    CHECK(all_ok, "dst: every raw block accepted (no error reported)");

    if (dst) tsdb_close(dst);
    tsdb_db_t *r = NULL;
    CHECK(tsdb_open(dd, &r) == TSDB_OK, "dst: reopen (fresh read path)");

    int nr = -1, rcq = -1;
    int64_t cv = count_star(r, "t", &nr, &rcq);
    printf("    count(*): rc=%d rows=%d val=%lld (want %d)\n",
           rcq, nr, (long long)cv, NROWS);
    CHECK(rcq == TSDB_OK && nr == 1 && cv == NROWS,
          "dst: count(*) == NROWS (every replicated block is visible)");

    scanres_t s = scan_ints(r, "t", "v", 1, NROWS);
    scan_report("SELECT v", &s);
    printf("    want rows=%d sum=%lld dup=0 missing=0\n",
           NROWS, (long long)sum_1_to(NROWS));
    CHECK(s.rc == TSDB_OK && s.rows == NROWS && s.dup == 0 &&
          s.missing == 0 && s.outside == 0 && s.sum == sum_1_to(NROWS),
          "dst: SELECT v returns each of 1..NROWS exactly once");

    tsdb_close(r);
    tsdb_close(src);
    cap_free(&ctx);
    rmrf(sd); rmrf(dd);
}

/* ===== C: retried push vs distinct block with the identical key =========== */
/*
 * C0 is the tension itself, in three pushes, all of them decided by the SAME
 * tail comparison in rawblock.c — so this is squarely inside what that check
 * is documented to cover ("only the last one can be a repeat"):
 *
 *   push ts ordinal 0          -> lands
 *   push ts ordinal 0 AGAIN    -> a re-delivery (timeout retry, duplicate
 *                                 fan-out).  Must NOT append a second copy.
 *   push ts ordinal 1          -> a DIFFERENT group of rows whose
 *                                 (size, count, ts_min, ts_max) is identical
 *                                 and whose bytes are identical too, because a
 *                                 block with ts_min == ts_max holds one
 *                                 repeated value.  Must NOT be discarded.
 *
 * Nothing in the current key can separate push 2 from push 3.  Only a durable
 * ordinal can: push 2 repeats ordinal 0, push 3 is ordinal 1.
 *
 * C1 then replays the WHOLE run a second time, which is the coarser thing a
 * resync or a re-sent batch does.
 */
static void test_retry_vs_distinct(void) {
    printf("\n--- C: retried push vs distinct same-key block ---\n");
    char sd[256], dd[256], d0[256];
    snprintf(sd, sizeof(sd), "/tmp/tsdb_dtrb_src_%d_C", (int)getpid());
    snprintf(dd, sizeof(dd), "/tmp/tsdb_dtrb_dst_%d_C", (int)getpid());
    snprintf(d0, sizeof(d0), "/tmp/tsdb_dtrb_c0_%d_C", (int)getpid());

    cap_ctx_t ctx; memset(&ctx, 0, sizeof(ctx));
    tsdb_db_t *src = build_src2(sd, &ctx, "C");

    /* ---- C0: retry vs distinct, decided against the index TAIL ---------- */
    {
        tsdb_db_t *c0 = fresh_replica(d0, COLS2, 2, "t");
        CHECK(c0 != NULL, "C0: replica opened with schema");

        int nr = -1, rcq = -1;
        CHECK(push_block(c0, pick(&ctx, 0, 0), 0) == TSDB_OK, "C0: ts ordinal 0 applied");
        int64_t after_first = count_star(c0, "t", &nr, &rcq);
        printf("    after ts ordinal 0:            count(*)=%lld (want %d)\n",
               (long long)after_first, BLOCK_PTS);
        CHECK(rcq == TSDB_OK && after_first == BLOCK_PTS, "C0: one block visible");

        /* Re-delivery of the block that is currently the tail. */
        CHECK(push_block(c0, pick(&ctx, 0, 0), 0) == TSDB_OK, "C0: ts ordinal 0 RE-delivered");
        int64_t after_retry = count_star(c0, "t", &nr, &rcq);
        printf("    after re-delivering ordinal 0: count(*)=%lld (want %d — unchanged)\n",
               (long long)after_retry, BLOCK_PTS);
        CHECK(rcq == TSDB_OK && after_retry == BLOCK_PTS,
              "C0: the re-delivery was dropped (no double-apply)");

        /* A DIFFERENT group of rows with a byte-identical payload. */
        CHECK(push_block(c0, pick(&ctx, 0, 1), 0) == TSDB_OK, "C0: ts ordinal 1 applied");
        int64_t after_second = count_star(c0, "t", &nr, &rcq);
        printf("    after ts ordinal 1:            count(*)=%lld (want %d)\n",
               (long long)after_second, 2 * BLOCK_PTS);
        CHECK(rcq == TSDB_OK && after_second == 2 * BLOCK_PTS,
              "C0: the distinct same-key block was KEPT (not a false duplicate)");

        tsdb_close(c0);
    }

    tsdb_db_t *dst = fresh_replica(dd, COLS2, 2, "t");
    CHECK(dst != NULL, "C1: replica opened with schema");

    /* Pass 1: the whole run, in order. */
    int ok1 = 1;
    for (int col = 0; col < 2 && dst; col++)
        for (int k = 0; k < NBLK; k++)
            if (push_block(dst, pick(&ctx, col, k), 0) != TSDB_OK) ok1 = 0;
    CHECK(ok1, "C1: pass 1 applied without error");

    uint64_t after1 = dst ? disk_blocks(dst, "t") : UINT64_MAX;
    printf("    physical blocks after pass 1: %llu (want %d)\n",
           (unsigned long long)after1, 2 * NBLK);
    CHECK(after1 == (uint64_t)(2 * NBLK),
          "C1: all 6 distinct blocks landed (none discarded as a false dup)");

    /* Pass 2: EVERY block delivered a second time.
     *
     * NOTE, so nobody mis-attributes this one: unlike every other case in this
     * file, a whole-run replay is red with DISTINCT timestamps too — the tail
     * comparison simply cannot see a repeat of a block that is no longer the
     * last entry, and re-delivering all six doubles the table (12 blocks,
     * count(*) == 2*NROWS, every row twice).  It is asserted here because the
     * durable ordinal makes "do I already hold this block" decidable for ANY
     * position, which closes it; it is NOT evidence about duplicate keys. */
    int ok2 = 1;
    for (int col = 0; col < 2 && dst; col++)
        for (int k = 0; k < NBLK; k++)
            if (push_block(dst, pick(&ctx, col, k), 0) != TSDB_OK) ok2 = 0;
    CHECK(ok2, "C1: pass 2 (full replay) applied without error");

    uint64_t after2 = dst ? disk_blocks(dst, "t") : UINT64_MAX;
    printf("    physical blocks after pass 2: %llu (want %d — unchanged)\n",
           (unsigned long long)after2, 2 * NBLK);
    CHECK(after2 == (uint64_t)(2 * NBLK),
          "C1: the replay appended nothing (idempotent re-delivery)");

    if (dst) tsdb_close(dst);
    tsdb_db_t *r = NULL;
    CHECK(tsdb_open(dd, &r) == TSDB_OK, "C1: reopen");

    int nr = -1, rcq = -1;
    int64_t cv = count_star(r, "t", &nr, &rcq);
    printf("    count(*): rc=%d rows=%d val=%lld (want %d)\n",
           rcq, nr, (long long)cv, NROWS);
    CHECK(rcq == TSDB_OK && nr == 1 && cv == NROWS,
          "C1: count(*) == NROWS after replay (no loss, no double count)");

    scanres_t s = scan_ints(r, "t", "v", 1, NROWS);
    scan_report("SELECT v", &s);
    CHECK(s.rc == TSDB_OK && s.rows == NROWS && s.dup == 0 &&
          s.missing == 0 && s.outside == 0 && s.sum == sum_1_to(NROWS),
          "C1: SELECT v still returns each of 1..NROWS exactly once");

    tsdb_close(r);
    tsdb_close(src);
    cap_free(&ctx);
    rmrf(sd); rmrf(dd); rmrf(d0);
}

/* ===== D: publish_ready must require the sibling's SAME ordinal =========== */
/*
 * Source t3(ts, a, b), three duplicate-key blocks.  Column values are chosen so
 * each block's contents name its own ordinal:
 *
 *   a = global row + 1              block k -> [k*1024+1 .. k*1024+1024]
 *   b = 1000000*(k+1) + r + 1       block k -> [1000000*(k+1)+1 .. +1024]
 *
 * D1 (negative): the replica holds a's ordinal 0 and b's ordinal ONE.  A ts
 * block for ordinal 0 must NOT publish — b has no block for that group, and the
 * only reason it looks satisfied is that every block of the run carries the
 * same (ts_min, count).  Publishing pairs ts block 0 with b's block 1 and the
 * partition then answers `SELECT b` with the wrong 1024 values, rc=0.
 *
 * D2 (positive): a's ordinal 0 and b's ordinal 0 — the same call must succeed,
 * and the partition must read back as exactly block 0 of both columns.  This is
 * the over-rejection guard: refusing here would break legitimate replication.
 */
#define B_OF(k, r)  ((int64_t)1000000 * ((k) + 1) + (r) + 1)

static tsdb_db_t *build_src3(const char *dir, cap_ctx_t *ctx) {
    rmrf(dir);
    tsdb_db_t *s = NULL;
    CHECK(tsdb_open(dir, &s) == TSDB_OK, "D: open src3");
    CHECK(tsdb_create_table_ex2(s, "t3", COLS3, 3, "ts",
                                TSDB_CREATE_PART_DAY, BLOCK_PTS) == TSDB_OK,
          "D: create t3(ts,a,b)");
    tsdb_db_set_raw_block_hook(s, cap_hook, ctx);

    tsdb_table_t *tbl = NULL;
    tsdb_open_table(s, "t3", &tbl);
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(tbl, &b);
    for (int i = 0; i < NROWS; i++) {
        int k = i / BLOCK_PTS, r = i % BLOCK_PTS;
        tsdb_batch_row_ts(b, TS_ONE);
        tsdb_batch_row_i64(b, 1, (int64_t)(i + 1));
        tsdb_batch_row_i64(b, 2, B_OF(k, r));
        tsdb_batch_row_end(b);
    }
    CHECK(tsdb_batch_commit(b) == TSDB_OK, "D: src3 commit");
    CHECK(tsdb_db_flush_all(s) == TSDB_OK, "D: src3 flush_all");
    CHECK(ctx->n == 3 * NBLK, "D: captured 3 cols x 3 blocks");
    return s;
}

static void test_publish_ready_ordinal(void) {
    printf("\n--- D: ts publish must require the sibling's SAME ordinal ---\n");
    char sd[256], dn[256], dp[256];
    snprintf(sd, sizeof(sd), "/tmp/tsdb_dtrb_src_%d_D", (int)getpid());
    snprintf(dn, sizeof(dn), "/tmp/tsdb_dtrb_neg_%d_D", (int)getpid());
    snprintf(dp, sizeof(dp), "/tmp/tsdb_dtrb_pos_%d_D", (int)getpid());

    cap_ctx_t ctx; memset(&ctx, 0, sizeof(ctx));
    tsdb_db_t *src = build_src3(sd, &ctx);

    /* ---- D1: a[0] + b[1], ts[0] offered with the RPC receiver's flag ---- */
    {
        tsdb_db_t *dst = fresh_replica(dn, COLS3, 3, "t3");
        CHECK(dst != NULL, "D1: replica opened with schema");
        CHECK(push_block(dst, pick(&ctx, 1, 0), 0) == TSDB_OK, "D1: landed a ordinal 0");
        CHECK(push_block(dst, pick(&ctx, 2, 1), 0) == TSDB_OK, "D1: landed b ordinal 1");

        int rc = push_block(dst, pick(&ctx, 0, 0), TSDB_RB_VERIFY_TS);
        printf("    ts ordinal 0 with TSDB_RB_VERIFY_TS -> rc=%d (want TSDB_ERR_BUSY=%d)\n",
               rc, TSDB_ERR_BUSY);
        CHECK(rc == TSDB_ERR_BUSY,
              "D1: ts publish DEFERRED — b's ordinal 0 is missing, not merely 'some key match'");

        tsdb_close(dst);
        tsdb_db_t *r = NULL;
        CHECK(tsdb_open(dn, &r) == TSDB_OK, "D1: reopen");
        int nr = -1, rcq = -1;
        int64_t cv = count_star(r, "t3", &nr, &rcq);
        /* Diagnostic only: on a tree that published anyway, `SELECT b` answers
         * with b's ORDINAL 1 values under ts block 0 — the silent wrong answer.
         * b block 0 sums to 1024524800, b block 1 to 2048524800. */
        scanres_t sb = scan_ints(r, "t3", "b", B_OF(0, 0), B_OF(0, BLOCK_PTS - 1));
        printf("    count(*): rc=%d rows=%d val=%lld (want 0 — nothing published)\n",
               rcq, nr, (long long)cv);
        scan_report("SELECT b", &sb);
        printf("    (b ordinal 0 sums to %lld; b ordinal 1 sums to %lld)\n",
               (long long)((int64_t)BLOCK_PTS * 1000000 + sum_1_to(BLOCK_PTS)),
               (long long)((int64_t)BLOCK_PTS * 2000000 + sum_1_to(BLOCK_PTS)));
        CHECK(rcq == TSDB_OK && cv == 0,
              "D1: the partition publishes NO rows (a deferred push writes nothing)");
        tsdb_close(r);
    }

    /* ---- D2: a[0] + b[0], the same offer must be accepted --------------- */
    {
        tsdb_db_t *dst = fresh_replica(dp, COLS3, 3, "t3");
        CHECK(dst != NULL, "D2: replica opened with schema");
        CHECK(push_block(dst, pick(&ctx, 1, 0), 0) == TSDB_OK, "D2: landed a ordinal 0");
        CHECK(push_block(dst, pick(&ctx, 2, 0), 0) == TSDB_OK, "D2: landed b ordinal 0");

        int rc = push_block(dst, pick(&ctx, 0, 0), TSDB_RB_VERIFY_TS);
        printf("    ts ordinal 0 with TSDB_RB_VERIFY_TS -> rc=%d (want TSDB_OK)\n", rc);
        CHECK(rc == TSDB_OK, "D2: ts publish ACCEPTED when every sibling ordinal 0 is here");

        tsdb_close(dst);
        tsdb_db_t *r = NULL;
        CHECK(tsdb_open(dp, &r) == TSDB_OK, "D2: reopen");
        int nr = -1, rcq = -1;
        int64_t cv = count_star(r, "t3", &nr, &rcq);
        printf("    count(*): rc=%d rows=%d val=%lld (want %d)\n",
               rcq, nr, (long long)cv, BLOCK_PTS);
        CHECK(rcq == TSDB_OK && nr == 1 && cv == BLOCK_PTS,
              "D2: count(*) == one block's rows");

        scanres_t sa = scan_ints(r, "t3", "a", 1, BLOCK_PTS);
        scan_report("SELECT a", &sa);
        CHECK(sa.rc == TSDB_OK && sa.rows == BLOCK_PTS && sa.dup == 0 &&
              sa.missing == 0 && sa.outside == 0,
              "D2: SELECT a == exactly a's ordinal-0 values");

        scanres_t sb = scan_ints(r, "t3", "b", B_OF(0, 0), B_OF(0, BLOCK_PTS - 1));
        scan_report("SELECT b", &sb);
        CHECK(sb.rc == TSDB_OK && sb.rows == BLOCK_PTS && sb.dup == 0 &&
              sb.missing == 0 && sb.outside == 0,
              "D2: SELECT b == exactly b's ordinal-0 values (not a sibling's)");
        tsdb_close(r);
    }

    tsdb_close(src);
    cap_free(&ctx);
    rmrf(sd); rmrf(dn); rmrf(dp);
}

/* ===== G: a metadata match is not a checksum ============================== */
/*
 * The applier absorbs a push — ACKs it and writes nothing — when it decides the
 * block is already here.  That decision used to rest on (size, count, ts_min,
 * ts_max) alone, which is a DESCRIPTION and not a checksum: on a
 * duplicate-timestamp run every block of the run shares ts_min and ts_max and
 * count, and two different value blocks of one column routinely encode to the
 * same length.  v ordinals 1 and 2 here are exactly that pair — same length,
 * different rows, different bytes — and the second was ACKed and thrown away.
 *
 * The senders are UNMARKED (mark == 0, a peer that predates the ordinal field),
 * which is the shape a rolling upgrade produces and the one path where the
 * historical tail compare still decides.  Marked senders reach the same
 * comparison through the ordinal branch.
 *
 * The assertion is on the VALUE column only, and deliberately: an unmarked
 * sender has no identity for its ts blocks either, and on a one-timestamp run
 * those ARE byte-identical, so nothing on the wire can separate them.  That is
 * the gap the durable ordinal exists to close (cases A and B); the byte
 * comparison closes the different-bytes half of it, which is the half where an
 * absorbed push destroys rows nobody can get back.
 *
 * Measured with the byte comparison removed: 1 physical block instead of 2,
 * rc == TSDB_OK on the push that was discarded.
 */
static void test_same_key_different_bytes(void) {
    printf("\n--- G: same (size,count,ts_min,ts_max), different bytes ---\n");
    char sd[256], dd[256];
    snprintf(sd, sizeof(sd), "/tmp/tsdb_dtrb_src_%d_G", (int)getpid());
    snprintf(dd, sizeof(dd), "/tmp/tsdb_dtrb_dst_%d_G", (int)getpid());

    cap_ctx_t ctx; memset(&ctx, 0, sizeof(ctx));
    tsdb_db_t *src = build_src2(sd, &ctx, "G");

    const cap_blk_t *v1 = pick(&ctx, 1, 1), *v2 = pick(&ctx, 1, 2);
    int usable = (v1 && v2 &&
                  v1->bytes_len == v2->bytes_len &&
                  v1->meta.count == v2->meta.count &&
                  v1->meta.ts_min == v2->meta.ts_min &&
                  v1->meta.ts_max == v2->meta.ts_max &&
                  memcmp(v1->bytes, v2->bytes, v1->bytes_len) != 0);
    CHECK(usable, "G: v ordinals 1 and 2 share the whole metadata key and "
                  "differ in bytes");

    tsdb_db_t *dst = fresh_replica(dd, COLS2, 2, "t");
    CHECK(dst != NULL, "G: replica opened with schema");

    if (dst && usable) {
        CHECK(push_block_unmarked(dst, v1) == TSDB_OK, "G: v ordinal 1 applied");
        CHECK(push_block_unmarked(dst, v2) == TSDB_OK, "G: v ordinal 2 applied");
        printf("    physical blocks after both v pushes: %llu (want 2)\n",
               (unsigned long long)disk_blocks(dst, "t"));
        CHECK(disk_blocks(dst, "t") == 2,
              "G: BOTH value blocks are on disk — the second was not absorbed "
              "on a metadata match");

        /* The over-rejection guard.  Comparing the bytes must not turn dedup
         * off: a GENUINE re-delivery — same key, same bytes — is still absorbed
         * silently, which is what keeps a retry from doubling the column. */
        CHECK(push_block_unmarked(dst, v2) == TSDB_OK,
              "G: v ordinal 2 RE-delivered, rc=OK");
        CHECK(disk_blocks(dst, "t") == 2,
              "G: the re-delivery appended nothing (dedup still works)");
    }

    if (dst) tsdb_close(dst);
    tsdb_close(src);
    cap_free(&ctx);
    rmrf(sd); rmrf(dd);
}

/* ===== E / F: migration of a duplicate-key run ============================ */

/* Byte offset just past the `want`-th block record of a migration stream, or 0
 * if the stream holds fewer.  Layout (include/tsdb_migrate.h):
 *   magic u32 | version u32 | table[64] | ncols u32 | ts_idx u32
 *   { name[64], type u32 } * ncols
 *   nsym u32 | { col u32, len u32, bytes } * nsym
 *   { len u32, payload } * ...  | len u32 == 0
 * The fixture has no SYMBOL column, so nsym is 0 and there is nothing to skip.
 */
static size_t mig_record_end(const uint8_t *s, size_t len, int want, int *out_recs) {
    if (out_recs) *out_recs = 0;
    size_t off = 4 + 4 + 64 + 4 + 4;
    if (len < off) return 0;
    uint32_t ncols = (uint32_t)s[8 + 64] | ((uint32_t)s[9 + 64] << 8)
                   | ((uint32_t)s[10 + 64] << 16) | ((uint32_t)s[11 + 64] << 24);
    off += (size_t)ncols * (64 + 4);
    if (len < off + 4) return 0;
    uint32_t nsym = (uint32_t)s[off] | ((uint32_t)s[off + 1] << 8)
                  | ((uint32_t)s[off + 2] << 16) | ((uint32_t)s[off + 3] << 24);
    off += 4;
    if (nsym != 0) return 0;                       /* fixture has none */

    int rec = 0;
    while (off + 4 <= len) {
        uint32_t l = (uint32_t)s[off] | ((uint32_t)s[off + 1] << 8)
                   | ((uint32_t)s[off + 2] << 16) | ((uint32_t)s[off + 3] << 24);
        if (l == 0) break;                          /* terminator */
        if (off + 4 + l > len) break;
        off += 4 + (size_t)l;
        rec++;
        if (out_recs) *out_recs = rec;
        if (rec == want) return off;
    }
    return 0;
}

static uint8_t *slurp(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *p = (sz > 0) ? malloc((size_t)sz) : NULL;
    if (p && fread(p, 1, (size_t)sz, f) != (size_t)sz) { free(p); p = NULL; }
    fclose(f);
    if (p) *out_len = (size_t)sz;
    return p;
}

static void assert_full_table(const char *dir, const char *tag) {
    tsdb_db_t *r = NULL;
    char msg[160];
    snprintf(msg, sizeof(msg), "%s: reopen target", tag);
    CHECK(tsdb_open(dir, &r) == TSDB_OK, msg);
    if (!r) return;

    int nr = -1, rcq = -1;
    int64_t cv = count_star(r, "t", &nr, &rcq);
    printf("    count(*): rc=%d rows=%d val=%lld (want %d)\n",
           rcq, nr, (long long)cv, NROWS);
    snprintf(msg, sizeof(msg), "%s: count(*) == NROWS", tag);
    CHECK(rcq == TSDB_OK && nr == 1 && cv == NROWS, msg);

    scanres_t s = scan_ints(r, "t", "v", 1, NROWS);
    scan_report("SELECT v", &s);
    snprintf(msg, sizeof(msg),
             "%s: target holds the source's multiset (1..NROWS, once each)", tag);
    CHECK(s.rc == TSDB_OK && s.rows == NROWS && s.dup == 0 &&
          s.missing == 0 && s.outside == 0 && s.sum == sum_1_to(NROWS), msg);
    tsdb_close(r);
}

static void test_migrate_dupkey(void) {
    printf("\n--- E/F: migration import of a duplicate-key run ---\n");
    char sd[256], stream[256], part[256], de[256], df[256];
    snprintf(sd,     sizeof(sd),     "/tmp/tsdb_dtrb_src_%d_M",    (int)getpid());
    snprintf(stream, sizeof(stream), "/tmp/tsdb_dtrb_strm_%d",     (int)getpid());
    snprintf(part,   sizeof(part),   "/tmp/tsdb_dtrb_strm_%d.cut", (int)getpid());
    snprintf(de,     sizeof(de),     "/tmp/tsdb_dtrb_mig_%d_E",    (int)getpid());
    snprintf(df,     sizeof(df),     "/tmp/tsdb_dtrb_mig_%d_F",    (int)getpid());

    cap_ctx_t ctx; memset(&ctx, 0, sizeof(ctx));
    tsdb_db_t *src = build_src2(sd, &ctx, "M");

    tsdb_mig_stats_t est; memset(&est, 0, sizeof(est));
    {
        int fd = open(stream, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        CHECK(fd >= 0, "E: created stream file");
        int rc = tsdb_migrate_export(src, "t", fd, NULL, &est);
        close(fd);
        printf("    export: rc=%d blocks=%llu rows=%llu\n", rc,
               (unsigned long long)est.blocks, (unsigned long long)est.rows);
        CHECK(rc == TSDB_OK && est.blocks == (uint64_t)(2 * NBLK) &&
              est.rows == (uint64_t)NROWS,
              "E: export streamed all 6 blocks / NROWS rows");
    }
    tsdb_close(src);

    /* ---- E: a single, uninterrupted import into a fresh target ---------- */
    {
        rmrf(de);
        tsdb_db_t *dst = NULL;
        CHECK(tsdb_open(de, &dst) == TSDB_OK, "E: open import target");
        int fd = open(stream, O_RDONLY);
        tsdb_mig_stats_t ist; memset(&ist, 0, sizeof(ist));
        int rc = tsdb_migrate_import(dst, fd, NULL, &ist);
        close(fd);
        printf("    import: rc=%d blocks=%llu landed=%llu rows=%llu\n", rc,
               (unsigned long long)ist.blocks,
               (unsigned long long)ist.blocks_landed,
               (unsigned long long)ist.rows);
        CHECK(rc == TSDB_OK, "E: import rc == OK");
        CHECK(ist.blocks_landed == (uint64_t)(2 * NBLK),
              "E: every streamed block landed (none collapsed by the resume key)");
        tsdb_close(dst);
        assert_full_table(de, "E");
    }

    /* ---- F: interrupt after 4 of the 6 records, then RESUME ------------- */
    {
        size_t slen = 0;
        uint8_t *sbuf = slurp(stream, &slen);
        CHECK(sbuf != NULL, "F: read the exported stream");
        int nrecs = 0;
        size_t cut = sbuf ? mig_record_end(sbuf, slen, 4, &nrecs) : 0;
        printf("    stream %zu bytes, cutting after 4 of %d records at %zu\n",
               slen, nrecs, cut);
        CHECK(cut > 0 && cut < slen, "F: found a record boundary to cut at");
        if (sbuf && cut > 0 && cut < slen) {
            int pf = open(part, O_CREAT | O_TRUNC | O_WRONLY, 0644);
            CHECK(pf >= 0 && write(pf, sbuf, cut) == (ssize_t)cut,
                  "F: wrote the truncated (interrupted) stream");
            if (pf >= 0) close(pf);

            rmrf(df);
            tsdb_db_t *dst = NULL;
            CHECK(tsdb_open(df, &dst) == TSDB_OK, "F: open import target");
            int fd = open(part, O_RDONLY);
            int rc1 = tsdb_migrate_import(dst, fd, NULL, NULL);
            close(fd);
            printf("    interrupted import: rc=%d (a truncated stream is an error)\n", rc1);
            CHECK(rc1 != TSDB_OK, "F: the interrupted import reported failure");
            tsdb_close(dst);

            /* Resume: the SAME target, the FULL stream. */
            tsdb_db_t *dst2 = NULL;
            CHECK(tsdb_open(df, &dst2) == TSDB_OK, "F: reopen target to resume");
            int ffd = open(stream, O_RDONLY);
            tsdb_mig_stats_t rst; memset(&rst, 0, sizeof(rst));
            int rc2 = tsdb_migrate_import(dst2, ffd, NULL, &rst);
            close(ffd);
            printf("    resumed import: rc=%d blocks=%llu landed=%llu\n", rc2,
                   (unsigned long long)rst.blocks,
                   (unsigned long long)rst.blocks_landed);
            CHECK(rc2 == TSDB_OK, "F: resumed import rc == OK");
            tsdb_close(dst2);

            assert_full_table(df, "F");
        }
        free(sbuf);
    }

    cap_free(&ctx);
    rmrf(sd); rmrf(de); rmrf(df);
    unlink(stream); unlink(part);
}

int main(void) {
    static const int IN_ORDER[NBLK]  = { 0, 1, 2 };
    static const int REVERSED[NBLK]  = { 2, 0, 1 };

    printf("=== test_dup_ts_rawblock: duplicate-timestamp blocks over "
           "replication + migration ===\n");
    test_apply_order(IN_ORDER, "A: raw blocks applied IN ORDER");
    test_apply_order(REVERSED, "B: raw blocks applied 2,0,1 WITHIN each column");
    test_retry_vs_distinct();
    test_publish_ready_ordinal();
    test_same_key_different_bytes();
    test_migrate_dupkey();
    printf("\n=== test_dup_ts_rawblock: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
