/* test_adv_repair_portable.c — ONE file, compiles and behaves NATIVELY on both
 * 9dab5a2 and the round-4 tree, so the two can be compared directly.
 *
 * The sequence is the ordinary cluster repair:
 *   1. a replica receives a whole partition by raw-block replication
 *   2. one value column's files are lost (bad sector, partial file-level
 *      restore, interrupted truncate+re-pull) — ts and the other column intact
 *   3. the sender re-syncs THAT COLUMN, block by block: the applier's actual
 *      repair granularity
 *   4. SELECT the repaired column
 *
 * On the round-4 tree step 1 is done by an OLD sender (no ordinal on the wire)
 * and the partition is then rewritten exactly as an unpatched binary leaves it
 * (bytes [82..87] zero, no <part>/.ordmap), which is what a rolling upgrade
 * actually has on disk.  Step 3 is done by the UPGRADED sender.
 *
 * Everything the round-4 tree adds is behind #ifdef TSDB_IDX_ORD_MARK, so on
 * 9dab5a2 this file is the same operational sequence with the fields that tree
 * has.
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
#include <sys/stat.h>
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
#define SENDER_A 0xA1A1A1A1ULL

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

static long idx_count_of(const char *part, const char *col) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return -1;
    long n = (long)im.count;
    idx_free(&im);
    return n;
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

/* ---- capture / push ------------------------------------------------------ */

typedef struct {
    char table[64]; uint32_t part_day; uint16_t col_idx;
    tsdb_block_meta_t meta; uint8_t *bytes; size_t bytes_len;
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
    e->part_day = day; e->col_idx = col; e->meta = *meta;
    e->bytes = malloc(blen ? blen : 1);
    if (e->bytes && blen) memcpy(e->bytes, bytes, blen);
    e->bytes_len = blen;
    return TSDB_OK;
}

static void cap_free(cap_ctx_t *c) {
    for (int i = 0; i < c->n; i++) free(c->b[i].bytes);
    c->n = 0;
}

static const cap_blk_t *pick(const cap_ctx_t *c, int col, int nth) {
    int seen = 0;
    for (int i = 0; i < c->n; i++)
        if (c->b[i].col_idx == (uint16_t)col) {
            if (seen == nth) return &c->b[i];
            seen++;
        }
    return NULL;
}

/* `upgraded` == 1 emits everything tsdb_rawblock_replicate emits on THIS tree.
 * On 9dab5a2 the two are the same push; there is no ordinal to carry. */
static int push_from(tsdb_db_t *dst, const cap_blk_t *e, int upgraded) {
    if (!e) return TSDB_ERR_NOTFOUND;
    tsdb_rawblock_push_t r; memset(&r, 0, sizeof r);
    snprintf(r.table, sizeof(r.table), "%s", e->table);
    r.part_day = e->part_day; r.col_idx = e->col_idx;
    r.codec = e->meta.codec;  r.flags   = e->meta.flags;
    r.count = e->meta.count;  r.ts_min  = e->meta.ts_min; r.ts_max = e->meta.ts_max;
    r.stats_min = e->meta.stats_min;   r.stats_max   = e->meta.stats_max;
    r.stats_sum = e->meta.stats_sum;   r.stats_first = e->meta.stats_first;
    r.stats_last = e->meta.stats_last; r.stats_flags = e->meta.stats_flags;
#ifdef TSDB_IDX_ORD_MARK
    if (upgraded) { r.ord = e->meta.ord; r.issuer = SENDER_A; }
#else
    (void)upgraded;
#endif
    r.block_bytes_len = (uint32_t)e->bytes_len;
    r.block_bytes     = e->bytes;

    uint8_t *buf = NULL; size_t len = 0;
    if (tsdb_rawblock_serialize(&r, &buf, &len) != TSDB_OK) return TSDB_ERR_NOMEM;
    tsdb_rawblock_push_t p; memset(&p, 0, sizeof p);
    int rc = tsdb_rawblock_parse(buf, len, &p);
    if (rc == TSDB_OK) rc = tsdb_rawblock_apply_ex(dst, &p, 0);
    free(buf);
    return rc;
}

static tsdb_col_t COLS3[3] = { {"ts", TSDB_TYPE_TIMESTAMP},
                               {"v",  TSDB_TYPE_INT64},
                               {"w",  TSDB_TYPE_INT64} };

static void seed(tsdb_db_t *db, long long n) {
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    HARD(tsdb_batch_begin(t, &b));
    for (long long i = 0; i < n; i++) {
        tsdb_batch_row_ts(b, D0 + i * STEP);
        tsdb_batch_row_i64(b, 1, i + 1);
        tsdb_batch_row_i64(b, 2, 7);
        tsdb_batch_row_end(b);
    }
    HARD(tsdb_batch_commit(b));
    HARD(tsdb_db_flush_all(db));
}

static tsdb_db_t *reopen(const char *dir) {
    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "t", &t));
    return db;
}

/* How the replica's partition came to exist BEFORE the loss.  The repair is the
 * same operation in both, and both are shapes a real fleet holds:
 *
 *   ORIGIN_PUSHED_LEGACY  received from a sender that predates the ordinal, so
 *                         every entry here is unmarked — what a rolling upgrade
 *                         finds on disk on day one.
 *
 *   ORIGIN_LOCAL_FLUSH    written by THIS binary's own flush, so ts and every
 *                         surviving column carry marked ordinals 0..NBLK-1 and
 *                         there is no <part>/.ordmap (nothing was ever received
 *                         here).  This is what EVERY partition becomes once the
 *                         fleet has upgraded and written for a while, so it is
 *                         the shape the repair has to work on FOREVER, not just
 *                         during the rollout.
 *
 * The second one is what the ordinal-space escape misses when it is gated on
 * "ts is not fully marked": the re-synced column is translated into free local
 * ordinals (NBLK, NBLK+1, ...) precisely because ts already owns 0..NBLK-1, so
 * the two sides are marked in spaces that provably do not meet — and there is
 * no other rule left to place the blocks. */
enum { ORIGIN_PUSHED_LEGACY = 0, ORIGIN_LOCAL_FLUSH = 1 };

static void repair_case(const char *tag, int origin, cap_ctx_t *ctx) {
    printf("\n[%s] repair into a partition %s\n", tag,
           origin == ORIGIN_LOCAL_FLUSH ? "THIS BINARY FLUSHED ITSELF"
                                        : "received from a legacy sender");

    char dd[256];
    snprintf(dd, sizeof dd, "/tmp/tsdb_advrep_d%s_%d", tag, (int)getpid());
    rm_rf(dd);

    tsdb_db_t *dst = NULL;
    HARD(tsdb_open(dd, &dst));

    char part[4096];

    if (origin == ORIGIN_LOCAL_FLUSH) {
        /* 1. the ordinary write path builds the partition, locally. */
        HARD(tsdb_create_table_ex2(dst, "t", COLS3, 3, "ts",
                                   TSDB_CREATE_PART_DAY, BP));
        seed(dst, NROW);
        check(1, "%s replica wrote all %d rows itself", tag, NROW);
        if (!only_part(dd, "t", part, sizeof part)) {
            check(0, "%s harness: no replica partition", tag);
            tsdb_close(dst); rm_rf(dd); return;
        }
    } else {
        HARD(tsdb_create_table_local_ex(dst, "t", COLS3, 3, "ts", 0, BP, -1));

        /* 1. the replica receives the whole partition from a NOT-yet-upgraded
         *    sender (on 9dab5a2 that is the only kind there is). */
        int ok = 1;
        for (int c = 1; c <= 2; c++)
            for (int i = 0; i < NBLK; i++)
                if (push_from(dst, pick(ctx, c, i), 0) != TSDB_OK) ok = 0;
        for (int i = 0; i < NBLK; i++)
            if (push_from(dst, pick(ctx, 0, i), 0) != TSDB_OK) ok = 0;
        check(ok, "%s replica received all 3x%d blocks", tag, NBLK);

        if (!only_part(dd, "t", part, sizeof part)) {
            check(0, "%s harness: no replica partition", tag);
            tsdb_close(dst); rm_rf(dd); return;
        }
        part_make_legacy(part);
    }

    tsdb_close(dst);
    dst = reopen(dd);
    qres_t pre = q(dst, "SELECT v FROM t");
    check(pre.rc == TSDB_OK && pre.rows == NROW && pre.sum == triangle(NROW),
          "%s before the loss: SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d",
          tag, pre.rows, pre.sum, NROW, triangle(NROW), pre.rc);
    tsdb_close(dst); dst = NULL;

    /* 2. the value column's files are lost. */
    char p1[4300], p2[4300];
    snprintf(p1, sizeof p1, "%s/v.idx", part);
    snprintf(p2, sizeof p2, "%s/v.col", part);
    unlink(p1); unlink(p2);

    dst = reopen(dd);
    qres_t gone = q(dst, "SELECT v FROM t");
    check(gone.rc != TSDB_OK,
          "%s with v gone the column is unavailable: rc=%d rows=%lld",
          tag, gone.rc, gone.rows);
    tsdb_close(dst); dst = NULL;

    /* 3. the sender — now upgraded — re-syncs the column, block by block. */
    dst = reopen(dd);
    int r0 = push_from(dst, pick(ctx, 1, 0), 1);
    int r1 = push_from(dst, pick(ctx, 1, 1), 1);
    int r2 = push_from(dst, pick(ctx, 1, 2), 1);
    check(r0 == TSDB_OK && r1 == TSDB_OK && r2 == TSDB_OK,
          "%s the three repair pushes are ACCEPTED (rc=%d/%d/%d) — the sender "
          "counts a durable replica", tag, r0, r1, r2);
    check(idx_count_of(part, "v") == NBLK,
          "%s v.idx holds %ld block(s) — every one landed (want %d)",
          tag, idx_count_of(part, "v"), NBLK);
    tsdb_close(dst); dst = NULL;

    /* 4. read it back. */
    dst = reopen(dd);
    qres_t cnt = q(dst, "SELECT count(*) FROM t");
    qres_t vq  = q(dst, "SELECT v FROM t");
    qres_t wq  = q(dst, "SELECT w FROM t");
    check(cnt.rc == TSDB_OK && cnt.sum == NROW,
          "%s count(*) = %lld (want %d) rc=%d", tag, cnt.sum, NROW, cnt.rc);
    check(wq.rc == TSDB_OK && wq.rows == NROW,
          "%s the untouched column w reads: rows=%lld rc=%d", tag, wq.rows, wq.rc);
    check(vq.rc == TSDB_OK && vq.rows == NROW && vq.sum == triangle(NROW),
          "%s AFTER THE REPAIR: SELECT v rows=%lld sum=%lld (want %d / %lld) "
          "rc=%d err=%s", tag, vq.rows, vq.sum, NROW, triangle(NROW),
          vq.rc, vq.err);

    /* 5. and it must still be true after a reopen — nothing self-heals.
     *
     * Round 1 of the repair is what creates <part>/.ordmap, so rounds 2 and 3
     * translate to the SAME local ordinals and land idempotently: if the first
     * round left the column unreadable, no later one can rescue it. */
    tsdb_close(dst); dst = NULL;
    dst = reopen(dd);
    qres_t vq2 = q(dst, "SELECT v FROM t");
    check(vq2.rc == TSDB_OK && vq2.rows == NROW,
          "%s still readable after a second reopen: rows=%lld rc=%d",
          tag, vq2.rows, vq2.rc);

    tsdb_close(dst);
    rm_rf(dd);
}

int main(void) {
    printf("=== test_adv_repair_portable ===\n");
#ifdef TSDB_IDX_ORD_MARK
    printf("(tree HAS the durable block ordinal)\n");
#else
    printf("(tree has NO durable block ordinal — 9dab5a2 baseline)\n");
#endif

    char sd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_advrep_s_%d", (int)getpid());
    rm_rf(sd);

    cap_ctx_t ctx; memset(&ctx, 0, sizeof ctx);
    tsdb_db_t *src = NULL;
    HARD(tsdb_open(sd, &src));
    HARD(tsdb_create_table_ex2(src, "t", COLS3, 3, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, &ctx);
    seed(src, NROW);

    repair_case("R1", ORIGIN_PUSHED_LEGACY, &ctx);
    repair_case("R2", ORIGIN_LOCAL_FLUSH,   &ctx);

    tsdb_close(src);
    cap_free(&ctx);
    rm_rf(sd);
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
