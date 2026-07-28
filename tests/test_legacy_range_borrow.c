/* test_legacy_range_borrow.c — ONE file, compiles and behaves NATIVELY on both
 * 9dab5a2 and this tree, so the two can be compared directly.
 *
 * A LEGACY partition — no durable block ordinal on any entry, i.e. every
 * partition an existing fleet already holds — in which ONE value entry's ts_max
 * is a tick wider than its ts partner's.  The producer is the range borrow in
 * the compaction this same change fixes, on output chunk 0; the fix stops new
 * ones appearing and does nothing for the ones already on disk.
 *
 * The pristine reader identified a block by (ts_min, count) and never looked at
 * ts_max, so it answers this partition correctly.  A reader that demands ts_max
 * unconditionally pairs nothing for that block and the column reads
 * TSDB_ERR_CORRUPT forever, with no self-heal path — an availability regression
 * on data nobody can re-stamp.
 *
 * [B1] the entry is where the writer left it (positional alignment holds)
 * [B2] the same borrow after the column's entries have been REORDERED, which is
 *      what replication and one-sided compaction do — the positional fast path
 *      cannot fire and the content rule has to place it
 * [B3] the guard: an all-equal-timestamp run, where nothing on disk says which
 *      block is which, must still come back unavailable and NEVER as another
 *      block's values
 * [B5] the same borrow on a value entry that has been RE-SYNCED by an upgraded
 *      sender, against a ts column that is still legacy — one marker is not
 *      evidence, because the entry it is compared against was written by a
 *      binary that could produce the borrow
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/cluster/rawblock.h"

#include <dirent.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_pass = 0, g_fail = 0;

static void check(int cond, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void check(int cond, const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    printf(cond ? "  PASS: " : "  FAIL: ");
    vprintf(fmt, ap);
    printf("\n");
    va_end(ap);
    if (cond) g_pass++; else g_fail++;
}

#define HARD(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
        fprintf(stderr, "harness broke at %s:%d rc=%d (%s)\n",              \
                __FILE__, __LINE__, _r, tsdb_errstr(_r));                   \
        printf("\n=== RESULTS: %d passed, %d failed (HARNESS ERROR) ===\n", \
               g_pass, g_fail);                                             \
        exit(2); } } while (0)

#define DAY_NS  86400000000000LL
#define D0      ((1700000000000000000LL / DAY_NS) * DAY_NS)
#define STEP    1000000LL
#define BP      1024
#define NBLK    3
#define NROW    (NBLK * BP)

static long long triangle(long long n) { return n * (n + 1) / 2; }

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

static int only_part(const char *db_dir, const char *table, char *out, size_t cap) {
    char td[4096]; snprintf(td, sizeof(td), "%s/%s", db_dir, table);
    DIR *d = opendir(td);
    if (!d) return 0;
    struct dirent *e; int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(out, cap, "%s/%s", td, e->d_name);
        found = 1; break;
    }
    closedir(d);
    return found;
}

/* ---- .idx image ---------------------------------------------------------- */

static uint32_t g32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t g16(const uint8_t *p) { return (uint16_t)(p[0] | (p[1] << 8)); }

typedef struct { uint8_t *buf; size_t len, hdr; uint32_t esz, count; } idx_img_t;

static int idx_load(const char *part, const char *col, idx_img_t *o) {
    memset(o, 0, sizeof(*o));
    char path[4200]; snprintf(path, sizeof path, "%s/%s.idx", part, col);
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 12) { fclose(f); return 0; }
    o->buf = malloc((size_t)sz);
    if (!o->buf || fread(o->buf, 1, (size_t)sz, f) != (size_t)sz) {
        fclose(f); free(o->buf); memset(o, 0, sizeof(*o)); return 0;
    }
    fclose(f);
    o->len = (size_t)sz; o->count = g32(o->buf + 4);
    uint16_t ver = g16(o->buf + 8);
    o->hdr = (ver == 1) ? 20 : (ver == 2) ? 36 : (ver == 3) ? 40 : 48;
    o->esz = (ver >= 3) ? g16(o->buf + 36) : 40;
    if (o->esz == 0) o->esz = 88;
    return 1;
}

static int idx_store(const char *part, const char *col, const idx_img_t *o) {
    char path[4200]; snprintf(path, sizeof path, "%s/%s.idx", part, col);
    FILE *w = fopen(path, "wb");
    if (!w) return 0;
    size_t n = fwrite(o->buf, 1, o->len, w);
    fclose(w);
    return n == o->len;
}

static void idx_free(idx_img_t *o) { free(o->buf); memset(o, 0, sizeof(*o)); }

static uint8_t *ent(idx_img_t *o, uint32_t i) {
    return o->buf + o->hdr + (size_t)i * o->esz;
}

/* Entry layout (part.h): [0..7] offset, [8..11] size, [12..15] count,
 * [16..23] ts_min, [24..31] ts_max. */
static int64_t ent_ts_max(idx_img_t *o, uint32_t i) {
    const uint8_t *p = ent(o, i) + 24;
    uint64_t v = 0;
    for (int k = 7; k >= 0; k--) v = (v << 8) | p[k];
    return (int64_t)v;
}
static void ent_set_ts_max(idx_img_t *o, uint32_t i, int64_t val) {
    uint8_t *p = ent(o, i) + 24;
    uint64_t v = (uint64_t)val;
    for (int k = 0; k < 8; k++) p[k] = (uint8_t)(v >> (8 * k));
}

/* Erase the reserved 6 bytes of every entry and drop the sidecar: byte for byte
 * what a binary predating the durable ordinal leaves behind.  A no-op on a tree
 * that has no such field. */
static void part_make_legacy(const char *part) {
    DIR *d = opendir(part);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            size_t l = strlen(e->d_name);
            if (l < 5 || strcmp(e->d_name + l - 4, ".idx") != 0) continue;
            char col[256]; snprintf(col, sizeof col, "%.*s", (int)(l - 4), e->d_name);
            idx_img_t im;
            if (!idx_load(part, col, &im)) continue;
            if (im.esz >= 88)
                for (uint32_t i = 0; i < im.count; i++)
                    memset(im.buf + im.hdr + (size_t)i * im.esz + 82, 0, 6);
            idx_store(part, col, &im);
            idx_free(&im);
        }
        closedir(d);
    }
    char om[4200]; snprintf(om, sizeof om, "%s/.ordmap", part);
    unlink(om);
}

/* ---- capture / push (the repair granularity a replica actually gets) ------ */

typedef struct {
    char table[64]; uint32_t day; uint16_t col;
    tsdb_block_meta_t meta; uint8_t *bytes; size_t len;
} cap_blk_t;
static cap_blk_t CAP[32];
static int NCAP;
#define SENDER_A 0xA1A1A1A1ULL

static int cap_hook(void *ud, tsdb_db_t *db, const char *table, uint32_t day,
                    uint16_t col, const tsdb_block_meta_t *meta,
                    const uint8_t *bytes, size_t blen) {
    (void)ud; (void)db;
    if (NCAP >= 32) return TSDB_OK;
    cap_blk_t *e = &CAP[NCAP++];
    snprintf(e->table, sizeof e->table, "%s", table);
    e->day = day; e->col = col; e->meta = *meta;
    e->bytes = malloc(blen ? blen : 1);
    if (e->bytes && blen) memcpy(e->bytes, bytes, blen);
    e->len = blen;
    return TSDB_OK;
}
static void cap_free(void) {
    for (int i = 0; i < NCAP; i++) free(CAP[i].bytes);
    NCAP = 0;
}
static cap_blk_t *cap_pick(int col, int nth) {
    int seen = 0;
    for (int i = 0; i < NCAP; i++)
        if (CAP[i].col == (uint16_t)col) { if (seen == nth) return &CAP[i]; seen++; }
    return NULL;
}
static int push_block(tsdb_db_t *dst, cap_blk_t *e) {
    if (!e) return TSDB_ERR_NOTFOUND;
    tsdb_rawblock_push_t r; memset(&r, 0, sizeof r);
    snprintf(r.table, sizeof r.table, "%s", e->table);
    r.part_day = e->day;         r.col_idx = e->col;
    r.codec    = e->meta.codec;  r.flags   = e->meta.flags;
    r.count    = e->meta.count;  r.ts_min  = e->meta.ts_min;
    r.ts_max   = e->meta.ts_max;
    r.stats_min  = e->meta.stats_min;   r.stats_max   = e->meta.stats_max;
    r.stats_sum  = e->meta.stats_sum;   r.stats_first = e->meta.stats_first;
    r.stats_last = e->meta.stats_last;  r.stats_flags = e->meta.stats_flags;
#ifdef TSDB_IDX_ORD_MARK
    r.ord = e->meta.ord;  r.issuer = SENDER_A;    /* an UPGRADED sender */
#endif
    r.block_bytes_len = (uint32_t)e->len;
    r.block_bytes     = e->bytes;

    uint8_t *buf = NULL; size_t len = 0;
    if (tsdb_rawblock_serialize(&r, &buf, &len) != TSDB_OK) return TSDB_ERR_NOMEM;
    tsdb_rawblock_push_t p; memset(&p, 0, sizeof p);
    int rc = tsdb_rawblock_parse(buf, len, &p);
    if (rc == TSDB_OK) rc = tsdb_rawblock_apply_ex(dst, &p, 0);
    free(buf);
    return rc;
}

/* ---- query --------------------------------------------------------------- */

typedef struct { int rc; long long rows, sum; char err[512]; } qres_t;

static qres_t q(tsdb_db_t *db, const char *sql) {
    qres_t o; memset(&o, 0, sizeof o);
    tsdb_result_t *r = NULL;
    o.rc = tsdb_query(db, sql, &r);
    if (o.rc == TSDB_OK && r) {
        uint64_t acc = 0;
        while (tsdb_result_next(r) > 0) { acc += (uint64_t)tsdb_result_i64(r, 0); o.rows++; }
        o.sum = (long long)acc;
    }
    if (r) tsdb_result_free(r);
    const char *e = tsdb_last_error();
    snprintf(o.err, sizeof o.err, "%s", e ? e : "");
    return o;
}

/* SELECT ts, v and answer the two questions a sum cannot.
 *
 * `paired`   every row's v is the value written at that row's ts
 * `permuted` the v column is 1..NROW each exactly once — i.e. no block was
 *            served twice, which IS the bug (a first-match answered `SELECT v`
 *            over three same-key blocks with block zero's values three times).
 *
 * A sum alone proves neither: it is invariant under any permutation of the
 * blocks, so it cannot see a reorder at all. */
typedef struct { int rc; long long rows; int paired, permuted; } pairres_t;

static pairres_t q_pairs(tsdb_db_t *db, int dup) {
    pairres_t o; memset(&o, 0, sizeof o);
    o.paired = 1; o.permuted = 1;
    uint8_t *seen = calloc(NROW + 1, 1);
    tsdb_result_t *r = NULL;
    o.rc = tsdb_query(db, "SELECT ts, v FROM t", &r);
    if (o.rc == TSDB_OK && r && seen) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_i64(r, 0);
            int64_t v  = tsdb_result_i64(r, 1);
            o.rows++;
            if (v < 1 || v > NROW || seen[v]) o.permuted = 0;
            else seen[v] = 1;
            /* With every row on ONE timestamp there is no durable statement
             * about which value belongs to which row, so only the permutation
             * claim is meaningful there. */
            if (!dup && v != (ts - D0) / STEP + 1) o.paired = 0;
        }
        if (o.rows != NROW) o.permuted = 0;
    } else {
        o.paired = 0; o.permuted = 0;
    }
    if (r) tsdb_result_free(r);
    free(seen);
    return o;
}

static tsdb_col_t COLS3[3] = { {"ts", TSDB_TYPE_TIMESTAMP},
                               {"v",  TSDB_TYPE_INT64},
                               {"w",  TSDB_TYPE_INT64} };

/* `dup` == 1 gives every row the SAME timestamp. */
static void build(const char *dir, int dup) {
    rm_rf(dir);
    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "t", COLS3, 3, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    HARD(tsdb_batch_begin(t, &b));
    for (long long i = 0; i < NROW; i++) {
        tsdb_batch_row_ts(b, dup ? D0 : D0 + i * STEP);
        tsdb_batch_row_i64(b, 1, i + 1);
        tsdb_batch_row_i64(b, 2, 7);
        tsdb_batch_row_end(b);
    }
    HARD(tsdb_batch_commit(b));
    HARD(tsdb_db_flush_all(db));
    tsdb_close(db);
}

/* The same content as build(dir, 0), written with the replication hook armed so
 * the blocks an upgraded sender would push are captured verbatim — ordinal and
 * all. */
static void build_captured(const char *dir) {
    rm_rf(dir);
    cap_free();
    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "t", COLS3, 3, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(db, cap_hook, NULL);
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    HARD(tsdb_batch_begin(t, &b));
    for (long long i = 0; i < NROW; i++) {
        tsdb_batch_row_ts(b, D0 + i * STEP);
        tsdb_batch_row_i64(b, 1, i + 1);
        tsdb_batch_row_i64(b, 2, 7);
        tsdb_batch_row_end(b);
    }
    HARD(tsdb_batch_commit(b));
    HARD(tsdb_db_flush_all(db));
    tsdb_close(db);
}

static tsdb_db_t *reopen(const char *dir) {
    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "t", &t));
    return db;
}

/* Widen v.idx entry 0's ts_max by one tick, optionally swapping entries 1 and 2
 * first so positional alignment cannot fire.  Returns 0 if the fixture could
 * not be built.
 *
 * `reorder == 0` is not a weaker version of `reorder == 1`: with the entries
 * where the writer left them the positional rule is the ONLY thing that can
 * place them once the ts keys repeat, because a repeated ts key makes the
 * content rule ambiguous by construction.  [B4] is that case. */
static int borrow_range(const char *part, int reorder) {
    idx_img_t im;
    if (!idx_load(part, "v", &im)) return 0;
    if (im.count != NBLK) { idx_free(&im); return 0; }
    if (reorder) {
        uint8_t *tmp = malloc(im.esz);
        if (!tmp) { idx_free(&im); return 0; }
        memcpy(tmp,         ent(&im, 1), im.esz);
        memcpy(ent(&im, 1), ent(&im, 2), im.esz);
        memcpy(ent(&im, 2), tmp,         im.esz);
        free(tmp);
    }
    ent_set_ts_max(&im, 0, ent_ts_max(&im, 0) + 1);
    int ok = idx_store(part, "v", &im);
    idx_free(&im);
    return ok;
}

/* Widen v.idx entry `i`'s ts_max by one tick, whatever the entry count is.
 * borrow_range() insists on all NBLK entries because its fixtures are whole
 * columns; this one also serves a column holding part of the partition. */
static int widen_entry(const char *part, uint32_t i) {
    idx_img_t im;
    if (!idx_load(part, "v", &im)) return 0;
    if (i >= im.count) { idx_free(&im); return 0; }
    ent_set_ts_max(&im, i, ent_ts_max(&im, i) + 1);
    int ok = idx_store(part, "v", &im);
    idx_free(&im);
    return ok;
}

static void case_borrow(const char *tag, const char *dir, int reorder) {
    printf("\n[%s] a legacy partition whose value entry 0 borrowed a wider "
           "range%s\n", tag, reorder ? ", entries reordered" : "");
    build(dir, 0);
    char part[4096];
    if (!only_part(dir, "t", part, sizeof part)) {
        check(0, "%s harness: no partition", tag); return;
    }
    part_make_legacy(part);
    if (!borrow_range(part, reorder)) {
        check(0, "%s harness: could not build the borrow", tag); return;
    }
    tsdb_db_t *db = reopen(dir);
    qres_t vq = q(db, "SELECT v FROM t");
    qres_t wq = q(db, "SELECT w FROM t");
    pairres_t pr = q_pairs(db, 0);
    check(vq.rc == TSDB_OK && vq.rows == NROW && vq.sum == triangle(NROW),
          "%s SELECT v rc=%d rows=%lld sum=%lld (want 0 / %d / %lld) err=%s",
          tag, vq.rc, vq.rows, vq.sum, NROW, triangle(NROW), vq.err);
    check(pr.rc == TSDB_OK && pr.paired && pr.permuted,
          "%s every value sits on the row it was written to (paired=%d "
          "permuted=%d rows=%lld rc=%d)",
          tag, pr.paired, pr.permuted, pr.rows, pr.rc);
    check(wq.rc == TSDB_OK && wq.rows == NROW,
          "%s the untouched column w still reads: rc=%d rows=%lld",
          tag, wq.rc, wq.rows);
    tsdb_close(db);
    rm_rf(dir);
}

int main(void) {
    printf("=== test_legacy_range_borrow ===\n");

    char d1[256], d2[256], d3[256];
    snprintf(d1, sizeof d1, "/tmp/tsdb_lrb_b1_%d", (int)getpid());
    snprintf(d2, sizeof d2, "/tmp/tsdb_lrb_b2_%d", (int)getpid());
    snprintf(d3, sizeof d3, "/tmp/tsdb_lrb_b3_%d", (int)getpid());

    case_borrow("B1", d1, 0);
    case_borrow("B2", d2, 1);

    /* [B3] The guard.  Every row carries ONE timestamp, so all three ts blocks
     * are byte-identical keys and nothing on disk says which value block goes
     * with which.  Relaxing ts_max must not turn that into a first-match: the
     * only two acceptable answers are the right values or a named error. */
    printf("\n[B3] all-equal timestamps: relaxing ts_max must not resurrect "
           "first-match\n");
    build(d3, 1);
    char part[4096];
    if (!only_part(d3, "t", part, sizeof part)) {
        check(0, "B3 harness: no partition");
    } else {
        part_make_legacy(part);
        if (!borrow_range(part, 1)) {
            check(0, "B3 harness: could not build the borrow");
        } else {
            tsdb_db_t *db = reopen(d3);
            pairres_t pr = q_pairs(db, 1);
            check(pr.rc != TSDB_OK || pr.permuted,
                  "B3 rc=%d rows=%lld permuted=%d — an error, or every value "
                  "exactly once; NEVER one block served twice (which is what "
                  "the first-match this series removes did here)",
                  pr.rc, pr.rows, pr.permuted);
            tsdb_close(db);
        }
    }
    rm_rf(d3);

    /* [B4] The same all-equal-timestamp run with the borrow, but the entries
     * left where the writer put them.
     *
     * A repeated ts key makes the content rule ambiguous BY CONSTRUCTION — no
     * field on disk says which value block belongs to which ts block — so the
     * positional rule is the only thing that can place these at all, and it can
     * only fire if the borrowed ts_max does not veto it.  Demanding ts_max here
     * costs the column outright: not a wrong answer, a permanently unreadable
     * one, on a shape a duplicate-timestamp table produces normally. */
    printf("\n[B4] all-equal timestamps, borrow, entries where the writer left "
           "them: the positional rule is the only one that can place these\n");
    char d4[256];
    snprintf(d4, sizeof d4, "/tmp/tsdb_lrb_b4_%d", (int)getpid());
    build(d4, 1);
    if (!only_part(d4, "t", part, sizeof part)) {
        check(0, "B4 harness: no partition");
    } else {
        part_make_legacy(part);
        if (!borrow_range(part, 0)) {
            check(0, "B4 harness: could not build the borrow");
        } else {
            tsdb_db_t *db = reopen(d4);
            qres_t vq = q(db, "SELECT v FROM t");
            pairres_t pr = q_pairs(db, 1);
            check(vq.rc == TSDB_OK && vq.rows == NROW &&
                  vq.sum == triangle(NROW) && pr.permuted,
                  "B4 SELECT v rc=%d rows=%lld sum=%lld permuted=%d "
                  "(want 0 / %d / %lld / 1) err=%s",
                  vq.rc, vq.rows, vq.sum, pr.permuted, NROW, triangle(NROW),
                  vq.err);
            tsdb_close(db);
        }
    }
    rm_rf(d4);

    /* [B5] The borrow on a RE-SYNCED value entry, against a legacy ts column.
     *
     * This is the end state test_adv_repair_portable certifies (`rc=0
     * rows=3072 sum=4720128`) with ONE extra fact: the sender's entry for
     * output chunk 0 carries the compaction range borrow — the same artefact
     * [B1]..[B4] are built on, now arriving over the wire instead of sitting
     * on local bytes.
     *
     * The column entry IS marked, so a rule scoped to "the marker means this
     * binary wrote it, therefore ts_max is exact" fires.  But the entry it is
     * being compared against is the LEGACY ts entry, written by a binary that
     * could produce the borrow, so the comparison is still against a ts_max
     * nobody can vouch for.  One marker is not evidence; the rule needs BOTH
     * sides marked.  Without that the repair the error message asks for lands
     * rc=0, every block on disk, and the column reads TSDB_ERR_CORRUPT forever
     * — a re-sync that makes the partition permanently worse. */
    printf("\n[B5] a legacy partition whose value column was RE-SYNCED whole, "
           "the sender's entry 0 carrying the borrow\n");
    char sd5[256], d5[256];
    snprintf(sd5, sizeof sd5, "/tmp/tsdb_lrb_b5s_%d", (int)getpid());
    snprintf(d5,  sizeof d5,  "/tmp/tsdb_lrb_b5d_%d", (int)getpid());
    build_captured(sd5);
    build(d5, 0);
    if (!only_part(d5, "t", part, sizeof part)) {
        check(0, "B5 harness: no partition");
    } else {
        /* Everything on disk as an unpatched binary left it. */
        part_make_legacy(part);
        {
            char p1[4300], p2[4300];
            snprintf(p1, sizeof p1, "%s/v.idx", part);
            snprintf(p2, sizeof p2, "%s/v.col", part);
            unlink(p1); unlink(p2);
        }
        tsdb_db_t *db = reopen(d5);
        int ok = 1;
        for (int i = 0; i < NBLK; i++)
            if (push_block(db, cap_pick(1, i)) != TSDB_OK) ok = 0;
        check(ok, "B5 the three repair pushes land");
        tsdb_close(db);

        if (!borrow_range(part, 0)) {
            check(0, "B5 harness: could not build the borrow");
        } else {
            db = reopen(d5);
            qres_t vq = q(db, "SELECT v FROM t");
            qres_t wq = q(db, "SELECT w FROM t");
            pairres_t pr = q_pairs(db, 0);
            check(vq.rc == TSDB_OK && vq.rows == NROW &&
                  vq.sum == triangle(NROW),
                  "B5 SELECT v rc=%d rows=%lld sum=%lld (want 0 / %d / %lld) "
                  "err=%s", vq.rc, vq.rows, vq.sum, NROW, triangle(NROW),
                  vq.err);
            check(pr.rc == TSDB_OK && pr.paired && pr.permuted,
                  "B5 every value sits on the row it was written to (paired=%d "
                  "permuted=%d rows=%lld rc=%d)",
                  pr.paired, pr.permuted, pr.rows, pr.rc);
            check(wq.rc == TSDB_OK && wq.rows == NROW,
                  "B5 the untouched column w still reads: rc=%d rows=%lld",
                  wq.rc, wq.rows);
            tsdb_close(db);
        }
    }
    cap_free();
    rm_rf(sd5); rm_rf(d5);

    /* [B6] The same borrow, on a PARTIAL re-sync — where the classification,
     * not just the pairing, turns on it.
     *
     * One push short is all a dropped connection costs, and the column's
     * survivors are then a contiguous SUFFIX of ts's blocks: the shape
     * tsdb_part_open reads as a late add and answers with the zeros those rows
     * legitimately hold (rc=0, rows=3072, sum=4195328 — the answer this same
     * partition gives with no borrow anywhere near it).
     *
     * The positional fit that reaches that classification is decided by
     * part_meta_agrees, so demanding ts_max of a marked column entry against a
     * legacy ts entry does not merely fail to pair one block — it costs the
     * SHAPE.  The run stops looking like a suffix, the fallback places the two
     * blocks without the late-add finding, and the leading slot becomes a HOLE:
     * a column that read rc=0 before the peer helped reads TSDB_ERR_CORRUPT
     * after.  An artefact in a ts_max nobody can vouch for must not change how
     * a column is classified. */
    printf("\n[B6] a PARTIAL re-sync carrying the borrow: the late-add shape "
           "must survive it\n");
    char sd6[256], d6[256];
    snprintf(sd6, sizeof sd6, "/tmp/tsdb_lrb_b6s_%d", (int)getpid());
    snprintf(d6,  sizeof d6,  "/tmp/tsdb_lrb_b6d_%d", (int)getpid());
    build_captured(sd6);
    build(d6, 0);
    if (!only_part(d6, "t", part, sizeof part)) {
        check(0, "B6 harness: no partition");
    } else {
        part_make_legacy(part);
        {
            char p1[4300], p2[4300];
            snprintf(p1, sizeof p1, "%s/v.idx", part);
            snprintf(p2, sizeof p2, "%s/v.col", part);
            unlink(p1); unlink(p2);
        }
        tsdb_db_t *db = reopen(d6);
        int ok = 1;
        for (int i = 1; i < NBLK; i++)          /* block 0 never arrives */
            if (push_block(db, cap_pick(1, i)) != TSDB_OK) ok = 0;
        check(ok, "B6 the two tail pushes land, the first never arrives");
        tsdb_close(db);

        if (!widen_entry(part, 0)) {
            check(0, "B6 harness: could not build the borrow");
        } else {
            long long want = triangle(NROW) - triangle(BP);
            db = reopen(d6);
            qres_t vq = q(db, "SELECT v FROM t");
            qres_t wq = q(db, "SELECT w FROM t");
            check(vq.rc == TSDB_OK && vq.rows == NROW && vq.sum == want,
                  "B6 SELECT v rc=%d rows=%lld sum=%lld (want 0 / %d / %lld) "
                  "err=%s", vq.rc, vq.rows, vq.sum, NROW, want, vq.err);
            check(wq.rc == TSDB_OK && wq.rows == NROW,
                  "B6 the untouched column w still reads: rc=%d rows=%lld",
                  wq.rc, wq.rows);
            tsdb_close(db);
        }
    }
    cap_free();
    rm_rf(sd6); rm_rf(d6);

    /* [B7] The OTHER side of the same rule, which nothing in this suite pinned.
     *
     * Every case above relaxes ts_max because one of the two entries was
     * written by a binary that could borrow.  Where BOTH carry the marker there
     * is no such excuse: both were written by this binary, ts_max is exact, and
     * a disagreement means the two indexes contradict each other about what the
     * partition holds.  That must be REFUSED — it is the pristine reader's
     * first-match served under a different name, and the relaxations above must
     * not reach it.
     *
     * The partition here is written by an ordinary local flush and left alone,
     * so every entry of ts and of v is marked; one value entry's ts_max is then
     * moved a tick. */
    printf("\n[B7] both sides marked: a ts_max contradiction is REFUSED, never "
           "served\n");
    char d7[256];
    snprintf(d7, sizeof d7, "/tmp/tsdb_lrb_b7_%d", (int)getpid());
    build(d7, 0);
    if (!only_part(d7, "t", part, sizeof part)) {
        check(0, "B7 harness: no partition");
    } else if (!widen_entry(part, 1)) {
        check(0, "B7 harness: could not move the ts_max");
    } else {
        tsdb_db_t *db = reopen(d7);
        qres_t cq = q(db, "SELECT count(*) FROM t");
        qres_t vq = q(db, "SELECT v FROM t");
        qres_t wq = q(db, "SELECT w FROM t");
        check(cq.rc == TSDB_OK && cq.sum == NROW,
              "B7 count(*) = %lld (want %d) rc=%d — ts is intact",
              cq.sum, NROW, cq.rc);
        check(vq.rc != TSDB_OK,
              "B7 SELECT v is REFUSED (rc=%d rows=%lld sum=%lld) — two marked "
              "entries that disagree are corruption, not an answer",
              vq.rc, vq.rows, vq.sum);
        check(wq.rc == TSDB_OK && wq.rows == NROW,
              "B7 the untouched column w still reads: rc=%d rows=%lld",
              wq.rc, wq.rows);
        tsdb_close(db);
    }
    rm_rf(d7);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
