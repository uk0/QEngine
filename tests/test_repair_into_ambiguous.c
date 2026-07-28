/* test_repair_into_ambiguous.c — the documented repair, run against a partition
 * where the placement is not recoverable.  ONE file, compiles and behaves
 * NATIVELY on both 9dab5a2 and this tree.
 *
 * The shape: a LEGACY partition (no marker on ts, none on the column), the ts
 * keys REPEAT (every row on one timestamp), and the column is SHORT.  Nothing
 * on disk says which ts block the survivors belong to, so the column is
 * correctly unavailable — and there is no automatic healing path, because the
 * information needed to place the blocks does not exist anywhere.
 *
 * What must NOT happen is the repair making it WORSE.  The engine's own error
 * message tells the operator to "re-sync it", and the applier can only do that
 * one (column, block) push at a time, which APPENDS marked entries beside the
 * unmarked survivors.  The column then has more entries than ts, a mixture of
 * marked and unmarked, and every content key identical — and a purely
 * positional pairing accepted a re-delivered copy of block 0 as the answer for
 * ts block 2:
 *
 *     before the repair   SELECT v rc=-4 (refused, named)
 *     after  the repair   SELECT v rc=0  rows=3072 sum=2622976, intact 4720128
 *
 * A refusal turning into a silent wrong answer under the repair the error
 * message asks for is the worst trade in the file.  It must stay a refusal.
 *
 * 9dab5a2 is worse still and in a different way: its applier drops blocks 1 and
 * 2 of the duplicate-timestamp run as re-deliveries, so the replica holds
 * ts.idx=1 v.idx=1, and SELECT v answers rc=0 rows=1024 sum=524800 both before
 * and after — one third of the rows, silently.
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

#define BP    1024
#define NBLK  3
#define NROW  (NBLK * BP)
#define D0    1743465600000000000LL
#define SENDER_A 0xA1A1A1A1ULL

#define OK(e) do { int _r=(e); if(_r!=TSDB_OK){ \
    printf("FATAL %s -> %d (%s)\n", #e, _r, tsdb_errstr(_r)); exit(2);} } while(0)

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

static uint32_t g32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t g16(const uint8_t *p) { return (uint16_t)(p[0] | (p[1] << 8)); }
static void p32(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}

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
    idx_img_t im; if (!idx_load(part, col, &im)) return -1;
    long n = (long)im.count; idx_free(&im); return n;
}

static void part_make_legacy(const char *part) {
    DIR *d = opendir(part);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            size_t l = strlen(e->d_name);
            if (l < 5 || strcmp(e->d_name + l - 4, ".idx") != 0) continue;
            char col[256]; snprintf(col, sizeof col, "%.*s", (int)(l-4), e->d_name);
            idx_img_t im;
            if (!idx_load(part, col, &im)) continue;
            if (im.esz >= 88)
                for (uint32_t i = 0; i < im.count; i++)
                    memset(im.buf + im.hdr + (size_t)i*im.esz + 82, 0, 6);
            idx_store(part, col, &im);
            idx_free(&im);
        }
        closedir(d);
    }
    char om[4200]; snprintf(om, sizeof om, "%s/.ordmap", part);
    unlink(om);
}

/* Drop the LAST entry of a column's idx: an interior/tail block lost. */
static int idx_drop_last(const char *part, const char *col) {
    idx_img_t im; if (!idx_load(part, col, &im)) return 0;
    if (im.count == 0) { idx_free(&im); return 0; }
    im.count--; p32(im.buf + 4, im.count);
    im.len -= im.esz;
    int ok = idx_store(part, col, &im);
    idx_free(&im);
    return ok;
}

typedef struct { int rc; long long rows, sum; char err[400]; } qres_t;
static qres_t q(tsdb_db_t *db, const char *sql) {
    qres_t o; memset(&o, 0, sizeof o);
    tsdb_result_t *r = NULL;
    o.rc = tsdb_query(db, sql, &r);
    if (o.rc == TSDB_OK && r) {
        uint64_t acc = 0;
        while (tsdb_result_next(r) > 0) { acc += (uint64_t)tsdb_result_i64(r,0); o.rows++; }
        o.sum = (long long)acc;
    }
    if (r) tsdb_result_free(r);
    const char *e = tsdb_last_error();
    snprintf(o.err, sizeof o.err, "%s", e ? e : "");
    return o;
}

typedef struct {
    char table[64]; uint32_t part_day; uint16_t col_idx;
    tsdb_block_meta_t meta; uint8_t *bytes; size_t bytes_len;
} cap_blk_t;
#define CAP_MAX 32
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
static const cap_blk_t *pick(const cap_ctx_t *c, int col, int nth) {
    int seen = 0;
    for (int i = 0; i < c->n; i++)
        if (c->b[i].col_idx == (uint16_t)col) { if (seen == nth) return &c->b[i]; seen++; }
    return NULL;
}
static int push_from(tsdb_db_t *dst, const cap_blk_t *e) {
    if (!e) return TSDB_ERR_NOTFOUND;
    tsdb_rawblock_push_t r; memset(&r, 0, sizeof r);
    snprintf(r.table, sizeof(r.table), "%s", e->table);
    r.part_day = e->part_day; r.col_idx = e->col_idx;
    r.codec = e->meta.codec;  r.flags = e->meta.flags;
    r.count = e->meta.count;  r.ts_min = e->meta.ts_min; r.ts_max = e->meta.ts_max;
    r.stats_min = e->meta.stats_min;   r.stats_max = e->meta.stats_max;
    r.stats_sum = e->meta.stats_sum;   r.stats_first = e->meta.stats_first;
    r.stats_last = e->meta.stats_last; r.stats_flags = e->meta.stats_flags;
#ifdef TSDB_IDX_ORD_MARK
    r.ord = e->meta.ord; r.issuer = SENDER_A;
#endif
    r.block_bytes_len = (uint32_t)e->bytes_len;
    r.block_bytes = e->bytes;
    uint8_t *buf = NULL; size_t len = 0;
    if (tsdb_rawblock_serialize(&r, &buf, &len) != TSDB_OK) return TSDB_ERR_NOMEM;
    tsdb_rawblock_push_t p; memset(&p, 0, sizeof p);
    int rc = tsdb_rawblock_parse(buf, len, &p);
    if (rc == TSDB_OK) rc = tsdb_rawblock_apply_ex(dst, &p, 0);
    free(buf);
    return rc;
}

static tsdb_col_t COLS2[2] = { {"ts", TSDB_TYPE_TIMESTAMP},
                               {"v",  TSDB_TYPE_INT64} };

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

/* The only two acceptable answers: a named error, or the database's own values.
 * Never rc=0 with somebody else's. */
static int answer_is_honest(const qres_t *r) {
    return r->rc != TSDB_OK || (r->rows == NROW && r->sum == triangle(NROW));
}

int main(void) {
    printf("=== test_repair_into_ambiguous ===\n");
#ifdef TSDB_IDX_ORD_MARK
    printf("(tree HAS the durable block ordinal)\n");
#else
    printf("(tree has NO durable block ordinal — 9dab5a2 baseline)\n");
#endif
    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_rpamb_s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_rpamb_d_%d", (int)getpid());
    rm_rf(sd); rm_rf(dd);

    /* Source: every row on ONE timestamp, so all three ts keys are identical. */
    cap_ctx_t ctx; memset(&ctx, 0, sizeof ctx);
    tsdb_db_t *src = NULL;
    OK(tsdb_open(sd, &src));
    OK(tsdb_create_table_ex2(src, "t", COLS2, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, &ctx);
    {
        tsdb_table_t *t = NULL; OK(tsdb_open_table(src, "t", &t));
        tsdb_batch_t *b = NULL; OK(tsdb_batch_begin(t, &b));
        for (long long i = 0; i < NROW; i++) {
            tsdb_batch_row_ts(b, D0);
            tsdb_batch_row_i64(b, 1, i + 1);
            tsdb_batch_row_end(b);
        }
        OK(tsdb_batch_commit(b));
        OK(tsdb_db_flush_all(src));
    }

    /* Replica gets the whole partition from a NOT-yet-upgraded sender, then the
     * partition is rewritten as an unpatched binary leaves it. */
    tsdb_db_t *dst = NULL;
    OK(tsdb_open(dd, &dst));
    OK(tsdb_create_table_local_ex(dst, "t", COLS2, 2, "ts", 0, BP, -1));
    for (int c = 1; c >= 0; c--)
        for (int i = 0; i < NBLK; i++)
            if (push_from(dst, pick(&ctx, c, i)) != TSDB_OK)
                { printf("FATAL seed push\n"); return 2; }
    tsdb_close(dst);

    char part[4096];
    if (!only_part(dd, "t", part, sizeof part)) { printf("FATAL no part\n"); return 2; }
    part_make_legacy(part);
    if (!idx_drop_last(part, "v")) { printf("FATAL drop\n"); return 2; }
    printf("  legacy partition: ts.idx=%ld v.idx=%ld, every ts key identical\n",
           idx_count_of(part, "ts"), idx_count_of(part, "v"));

    OK(tsdb_open(dd, &dst));
    { tsdb_table_t *tt = NULL; OK(tsdb_open_table(dst, "t", &tt)); }
    qres_t a = q(dst, "SELECT v FROM t");
    check(answer_is_honest(&a),
          "BEFORE the repair: SELECT v rc=%d rows=%lld sum=%lld — an error, or "
          "the database's own values (intact = %lld); never another block's",
          a.rc, a.rows, a.sum, triangle(NROW));
    tsdb_close(dst);

    /* The documented repair: an UPGRADED sender re-syncs the column. */
    OK(tsdb_open(dd, &dst));
    { tsdb_table_t *tt = NULL; OK(tsdb_open_table(dst, "t", &tt)); }
    int r0 = push_from(dst, pick(&ctx, 1, 0));
    int r1 = push_from(dst, pick(&ctx, 1, 1));
    int r2 = push_from(dst, pick(&ctx, 1, 2));
    printf("  (repair pushes rc=%d/%d/%d, v.idx now %ld entries)\n",
           r0, r1, r2, idx_count_of(part, "v"));
    tsdb_close(dst);

    OK(tsdb_open(dd, &dst));
    { tsdb_table_t *tt = NULL; OK(tsdb_open_table(dst, "t", &tt)); }
    qres_t b = q(dst, "SELECT v FROM t");
    check(answer_is_honest(&b),
          "AFTER the repair: SELECT v rc=%d rows=%lld sum=%lld — still an "
          "error, or the database's own values (intact = %lld); the re-sync "
          "the error message asks for must not turn a refusal into a wrong "
          "answer", b.rc, b.rows, b.sum, triangle(NROW));
    tsdb_close(dst);

    tsdb_close(src);
    rm_rf(sd); rm_rf(dd);
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
