/* test_retract_short_column.c — tsdb_part_ts_retract_unpaired must repair a
 * TORN partition and must not touch a merely SHORT one.
 *
 * The repair lowers <ts>.idx to the longest block prefix every column can pair
 * with, so a partition that lost a value-column push stops counting rows nobody
 * can read and the gap becomes visible to anti-entropy (which compares
 * (count, max_ts) and is otherwise blind to a missing value column).  It deletes
 * nothing, so a later push heals the partition back upward.
 *
 * A column that is SHORT is not torn.  An ALTER TABLE ADD COLUMN owns a
 * contiguous SUFFIX of the partition's ts blocks and the pre-add rows read as
 * the zeros they legitimately are — tsdb_part_open classifies exactly that shape
 * as a late add.  Retracting there deletes rows that read perfectly, and no push
 * is ever coming for a locally added column, so the deletion is FOREVER.
 *
 * Two things make that easy to get wrong, and both are pinned here:
 *
 *   [A1] Pairing "on the ordinal" is meaningless on a LEGACY unmarked index,
 *        where the effective ordinal IS the physical position.  The scan over
 *        every column entry then degenerates to `k == b` and a column sitting at
 *        a different position from its ts block — which an ALTER-added column
 *        ALWAYS does — pairs with nothing at all.  ts.idx is republished
 *        truncated, permanently.
 *
 *   [A2] The duplicate-timestamp case hides it: every block of a one-timestamp
 *        run carries the same content key, so a key scan finds *a* match for
 *        every ts block by accident.  The unique-timestamp case does not, and
 *        loses the whole partition instead of part of it.
 *
 *   [A3] The control.  Whatever rule keeps [A1]/[A2] whole must not disarm the
 *        repair: a real interior gap must still lower ts to the paired prefix.
 *
 *   [A4] The mixed index.  A repair push lands at an ordinal in THIS node's
 *        space, next to legacy entries that carry none, under a legacy ts column
 *        whose effective ordinal is its position.  Comparing the two numbers is
 *        a category error — they come from different spaces — and it rejects a
 *        block that is demonstrably on disk.  The reader gates the ordinal on
 *        "EVERY entry of this column records one" (part_align_column's
 *        ord_mode); the repair has to gate it the same way or it truncates a
 *        partition the reader answers perfectly.
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

extern tsdb_table_internal_t *tsdb_db_find_table(tsdb_db_t *db, const char *name);
extern tsdb_schema_t *tsdb_tbl_schema(tsdb_table_internal_t *t);

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
        fprintf(stderr, "harness broke at %s:%d rc=%d\n", __FILE__, __LINE__, _r); \
        printf("\n=== RESULTS: %d passed, %d failed (HARNESS ERROR) ===\n", \
               g_pass, g_fail);                                             \
        exit(2); } } while (0)

#define DAY_NS  86400000000000LL
#define D0      ((1700000000000000000LL / DAY_NS) * DAY_NS)
#define STEP    1000000LL
#define BP      1024
#define TS_ONE  (D0 + 3600000000000LL)

#define OLD_BLK 3
#define NEW_BLK 2
#define OLD_ROWS (OLD_BLK * BP)
#define NEW_ROWS (NEW_BLK * BP)
#define ALL_ROWS (OLD_ROWS + NEW_ROWS)

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

static int only_part(const char *db_dir, const char *table,
                     char *out, size_t cap) {
    char td[4096]; snprintf(td, sizeof(td), "%s/%s", db_dir, table);
    DIR *d = opendir(td);
    if (!d) return 0;
    struct dirent *e; int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(out, cap, "%s/%s", td, e->d_name);
        found = 1;
        break;
    }
    closedir(d);
    return found;
}

/* ---- little-endian .idx surgery (mirrors part.c's on-disk layout) --------- */

static uint32_t g32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t g16(const uint8_t *p) { return (uint16_t)(p[0] | (p[1] << 8)); }
static void p32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}
static void p16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
}

typedef struct {
    uint8_t *buf; size_t len, hdr; uint32_t esz, count;
} idx_img_t;

static int idx_load(const char *part, const char *col, idx_img_t *o) {
    memset(o, 0, sizeof(*o));
    char path[4200];
    snprintf(path, sizeof path, "%s/%s.idx", part, col);
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 12) { fclose(f); return 0; }
    o->buf = malloc((size_t)sz);
    if (!o->buf) { fclose(f); return 0; }
    if (fread(o->buf, 1, (size_t)sz, f) != (size_t)sz) {
        fclose(f); free(o->buf); memset(o, 0, sizeof(*o)); return 0;
    }
    fclose(f);
    o->len   = (size_t)sz;
    o->count = g32(o->buf + 4);
    uint16_t ver = g16(o->buf + 8);
    o->hdr = (ver == 1) ? 20 : (ver == 2) ? 36 : (ver == 3) ? 40 : 48;
    o->esz = (ver >= 3) ? g16(o->buf + 36) : 40;
    if (o->esz == 0) o->esz = 88;
    return 1;
}

static int idx_store(const char *part, const char *col, const idx_img_t *o) {
    char path[4200];
    snprintf(path, sizeof path, "%s/%s.idx", part, col);
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

/* Clear every entry's ordinal marker: the file an UNPATCHED binary leaves. */
static int idx_strip_ords(const char *part, const char *col) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return 0;
    if (im.esz >= 88)
        for (uint32_t i = 0; i < im.count; i++) {
            uint8_t *e = im.buf + im.hdr + (size_t)i * im.esz;
            p32(e + 82, 0);
            p16(e + 86, 0);
        }
    int ok = idx_store(part, col, &im);
    idx_free(&im);
    return ok;
}

static int part_strip_ords(const char *part) {
    DIR *d = opendir(part);
    if (!d) return 0;
    struct dirent *e;
    int ok = 1;
    while ((e = readdir(d))) {
        size_t l = strlen(e->d_name);
        if (l < 5 || strcmp(e->d_name + l - 4, ".idx") != 0) continue;
        char col[256];
        snprintf(col, sizeof col, "%.*s", (int)(l - 4), e->d_name);
        if (!idx_strip_ords(part, col)) ok = 0;
    }
    closedir(d);
    return ok;
}

/* ...and the sidecar that records which local ordinal each received block group
 * was translated onto.  A partition an unpatched binary wrote has none, and a
 * partition THIS binary flushed itself has none either (nothing was ever
 * received there), so dropping it is part of "make this look legacy". */
static void part_drop_ordmap(const char *part) {
    char p[4200];
    snprintf(p, sizeof p, "%s/.ordmap", part);
    unlink(p);
}

/* Drop entry `k` from a column index: the on-disk shape a lost push leaves,
 * with the survivors closed up over the gap. */
static int idx_drop_entry(const char *part, const char *col, uint32_t k) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return 0;
    if (k >= im.count) { idx_free(&im); return 0; }
    uint8_t *base = im.buf + im.hdr;
    memmove(base + (size_t)k * im.esz, base + (size_t)(k + 1) * im.esz,
            (size_t)(im.count - k - 1) * im.esz);
    im.count--;
    p32(im.buf + 4, im.count);
    im.len -= im.esz;
    int ok = idx_store(part, col, &im);
    idx_free(&im);
    return ok;
}

/* ---- queries ------------------------------------------------------------- */

typedef struct { int rc; long long rows, sum; } qres_t;

static qres_t q(tsdb_db_t *db, const char *sql) {
    qres_t r; memset(&r, 0, sizeof r);
    tsdb_result_t *res = NULL;
    r.rc = tsdb_query(db, sql, &res);
    if (r.rc != TSDB_OK) return r;
    while (tsdb_result_next(res) == 1) { r.rows++; r.sum += tsdb_result_i64(res, 0); }
    tsdb_result_free(res);
    return r;
}

static tsdb_col_t COLS2[2] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_INT64} };

static tsdb_schema_t *sch(tsdb_db_t *db, const char *tbl) {
    tsdb_table_internal_t *ti = tsdb_db_find_table(db, tbl);
    return ti ? tsdb_tbl_schema(ti) : NULL;
}

/* ==========================================================================
 * [A1]/[A2] a LEGACY unmarked partition with an ALTER-added (short) column
 * ========================================================================== */

/* `one_ts` picks the shape: every row at one timestamp (content keys repeat) or
 * a distinct timestamp per row (they do not). */
static void test_legacy_short_column(int one_ts) {
    const char *tag = one_ts ? "A1 duplicate-ts" : "A2 unique-ts";
    printf("\n[%s] legacy unmarked index + ALTER-added column: retract is a "
           "no-op\n", one_ts ? "A1" : "A2");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_retract_%s_%d",
             one_ts ? "dup" : "uniq", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "r", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int i = 0; i < OLD_ROWS; i++) {
            tsdb_batch_row_ts(b, one_ts ? TS_ONE : D0 + (int64_t)i * STEP);
            tsdb_batch_row_i64(b, 1, i + 1);
            tsdb_batch_row_end(b);
        }
        HARD(tsdb_batch_commit(b));
        HARD(tsdb_db_flush_all(db));
    }
    tsdb_close(db); db = NULL;

    char part[4096];
    if (!only_part(dir, "r", part, sizeof part)) {
        check(0, "%s harness: no partition", tag); rm_rf(dir); return;
    }
    /* Everything on disk as an UNPATCHED binary would have left it. */
    check(part_strip_ords(part), "%s partition rewritten as legacy (no marks)", tag);

    /* Now this binary performs an ordinary ALTER + write. */
    HARD(tsdb_open(dir, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
        HARD(tsdb_alter_table_add_column(db, "r", "w", TSDB_TYPE_INT64));
        HARD(tsdb_open_table(db, "r", &t));
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int i = 0; i < NEW_ROWS; i++) {
            tsdb_batch_row_ts(b, one_ts ? TS_ONE
                                        : D0 + (int64_t)(OLD_ROWS + i) * STEP);
            tsdb_batch_row_i64(b, 1, OLD_ROWS + i + 1);
            tsdb_batch_row_i64(b, 2, i + 1);
            tsdb_batch_row_end(b);
        }
        HARD(tsdb_batch_commit(b));
        HARD(tsdb_db_flush_all(db));
    }
    tsdb_close(db); db = NULL;

    /* ... and the whole partition is legacy again: the shape a node that was
     * upgraded, wrote, and then rolled back leaves, and the one where "the
     * effective ordinal is the position" is true of every entry. */
    check(part_strip_ords(part), "%s partition legacy again after the ALTER", tag);
    check(idx_count_of(part, "ts") == OLD_BLK + NEW_BLK &&
          idx_count_of(part, "w") == NEW_BLK,
          "%s ts.idx=%ld w.idx=%ld (want %d / %d)", tag,
          idx_count_of(part, "ts"), idx_count_of(part, "w"),
          OLD_BLK + NEW_BLK, NEW_BLK);

    HARD(tsdb_open(dir, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));      /* tables load lazily */
    }
    qres_t c0 = q(db, "SELECT count(*) FROM r");
    qres_t v0 = q(db, "SELECT v FROM r");
    qres_t w0 = q(db, "SELECT w FROM r");
    check(c0.rc == TSDB_OK && c0.sum == ALL_ROWS,
          "%s count(*) before the repair = %lld (want %d)",
          tag, c0.sum, ALL_ROWS);

    uint32_t retracted = 99;
    tsdb_schema_t *s = sch(db, "r");
    int rrc = s ? tsdb_part_ts_retract_unpaired(s, part, &retracted)
                : TSDB_ERR_INTERNAL;
    check(rrc == TSDB_OK, "%s retract rc=%d (want 0)", tag, rrc);

    /* THE assertion: a short column is a late add, not a tear.  Nothing is
     * unpaired, so nothing may be retracted. */
    check(retracted == 0, "%s retracted=%u (want 0)", tag, retracted);
    check(idx_count_of(part, "ts") == OLD_BLK + NEW_BLK,
          "%s ts.idx still declares %ld blocks (want %d)",
          tag, idx_count_of(part, "ts"), OLD_BLK + NEW_BLK);

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dir, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
    }
    qres_t c1 = q(db, "SELECT count(*) FROM r");
    qres_t v1 = q(db, "SELECT v FROM r");
    qres_t w1 = q(db, "SELECT w FROM r");
    check(c1.rc == TSDB_OK && c1.sum == ALL_ROWS,
          "%s count(*) after the repair = %lld (want %d) rc=%d",
          tag, c1.sum, ALL_ROWS, c1.rc);
    check(v1.rc == TSDB_OK && v1.rows == ALL_ROWS &&
          v1.sum == triangle(ALL_ROWS),
          "%s SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d",
          tag, v1.rows, v1.sum, ALL_ROWS, triangle(ALL_ROWS), v1.rc);

    /* The repair changed NOTHING it could observe — that is the whole claim.
     * Held against the before-picture rather than an absolute, because the two
     * shapes read the added column differently and only one of those is the
     * repair's business (see below). */
    check(c1.rc == c0.rc && c1.sum == c0.sum &&
          v1.rc == v0.rc && v1.rows == v0.rows && v1.sum == v0.sum &&
          w1.rc == w0.rc && w1.rows == w0.rows && w1.sum == w0.sum,
          "%s every answer is byte-identical before and after the repair "
          "(w: rc %d->%d rows %lld->%lld sum %lld->%lld)",
          tag, w0.rc, w1.rc, w0.rows, w1.rows, w0.sum, w1.sum);

    if (one_ts) {
        /* COMPAT, re-measured: when the ts index AND the column are BOTH
         * legacy-unmarked and the content keys repeat, nothing on disk says
         * which ts block the column's blocks belong to, so the reader refuses
         * by name instead of guessing.  A flush only appends and compaction
         * vetoes the shape, so there is no automatic healing path — the
         * partition has to be rebuilt.  It is not a regression from a correct
         * answer: pristine HEAD answers this shape rc=0 with WRONG values.
         * What matters here is that the REPAIR does not make it worse. */
        check(w1.rc == TSDB_ERR_CORRUPT,
              "%s SELECT w is refused by name, not answered wrongly (rc=%d)",
              tag, w1.rc);
    } else {
        /* Unique keys: the legacy content rule still places the suffix, so the
         * added column reads its own values with the pre-ALTER rows as the
         * zeros they legitimately are. */
        check(w1.rc == TSDB_OK && w1.rows == ALL_ROWS &&
              w1.sum == triangle(NEW_ROWS),
              "%s SELECT w rows=%lld sum=%lld (want %d / %lld) rc=%d",
              tag, w1.rows, w1.sum, ALL_ROWS, triangle(NEW_ROWS), w1.rc);
    }

    /* Idempotent, as the contract says. */
    uint32_t again = 99;
    s = sch(db, "r");
    check(s && tsdb_part_ts_retract_unpaired(s, part, &again) == TSDB_OK &&
          again == 0, "%s a second call retracts 0 too", tag);

    tsdb_close(db);
    rm_rf(dir);
}

/* ==========================================================================
 * [A4] a legacy index with ONE repaired (marked) entry — the mixed case
 * ========================================================================== */

#define A4_BLK  3
#define A4_ROWS (A4_BLK * BP)

typedef struct {
    char table[64]; uint32_t day; uint16_t col;
    tsdb_block_meta_t meta; uint8_t *bytes; size_t len;
} cap_blk_t;
static cap_blk_t CAP[32];
static int NCAP;

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
    r.part_day = e->day;         r.col_idx  = e->col;
    r.codec    = e->meta.codec;  r.flags    = e->meta.flags;
    r.count    = e->meta.count;  r.ts_min   = e->meta.ts_min;
    r.ts_max   = e->meta.ts_max;
    r.stats_min   = e->meta.stats_min;   r.stats_max   = e->meta.stats_max;
    r.stats_sum   = e->meta.stats_sum;   r.stats_first = e->meta.stats_first;
    r.stats_last  = e->meta.stats_last;  r.stats_flags = e->meta.stats_flags;
    r.ord = e->meta.ord;
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

static void test_repaired_legacy_index(void) {
    printf("\n[A4] a repair push next to legacy entries: retract is a no-op\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_retract_a4s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_retract_a4d_%d", (int)getpid());
    rm_rf(sd); rm_rf(dd);
    cap_free();

    tsdb_db_t *src = NULL, *db = NULL;
    HARD(tsdb_open(sd, &src));
    HARD(tsdb_create_table_ex2(src, "r", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, NULL);
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(src, "r", &t));
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int i = 0; i < A4_ROWS; i++) {
            tsdb_batch_row_ts(b, D0 + (int64_t)i * STEP);
            tsdb_batch_row_i64(b, 1, i + 1);
            tsdb_batch_row_end(b);
        }
        HARD(tsdb_batch_commit(b));
        HARD(tsdb_db_flush_all(src));
    }

    /* The replica got the whole ts spine and all but the MIDDLE v block — the
     * ordinary dropped-push state. */
    HARD(tsdb_open(dd, &db));
    HARD(tsdb_create_table_local_ex(db, "r", COLS2, 2, "ts", 0, BP, -1));
    int ok = 1;
    for (int i = 0; i < A4_BLK; i++)
        if (push_block(db, cap_pick(0, i)) != TSDB_OK) ok = 0;
    if (push_block(db, cap_pick(1, 0)) != TSDB_OK) ok = 0;
    if (push_block(db, cap_pick(1, 2)) != TSDB_OK) ok = 0;
    check(ok, "A4 replica received ts x%d and v blocks 0 and 2", A4_BLK);

    char part[4096];
    if (!only_part(dd, "r", part, sizeof part)) {
        check(0, "A4 harness: no partition"); goto done;
    }
    /* This replica was never upgraded, so nothing it holds carries an ordinal
     * and the survivors have closed up over the gap: entry 1 is block 2. */
    check(part_strip_ords(part), "A4 replica index rewritten as legacy");

    /* The repair lands.  Its ordinal is allocated in THIS node's space and has
     * no relation to the legacy entries' positions. */
    check(push_block(db, cap_pick(1, 1)) == TSDB_OK, "A4 the repair push lands");
    check(idx_count_of(part, "v") == A4_BLK,
          "A4 v.idx holds %ld blocks (want %d)", idx_count_of(part, "v"), A4_BLK);

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dd, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
    }
    qres_t v0 = q(db, "SELECT v FROM r");
    check(v0.rc == TSDB_OK && v0.rows == A4_ROWS && v0.sum == triangle(A4_ROWS),
          "A4 the repaired partition reads whole: rows=%lld sum=%lld "
          "(want %d / %lld) rc=%d", v0.rows, v0.sum, A4_ROWS,
          triangle(A4_ROWS), v0.rc);

    uint32_t retracted = 99;
    tsdb_schema_t *s = sch(db, "r");
    int rrc = s ? tsdb_part_ts_retract_unpaired(s, part, &retracted)
                : TSDB_ERR_INTERNAL;
    /* THE assertion: nothing is unpaired, so nothing may be retracted. */
    check(rrc == TSDB_OK && retracted == 0,
          "A4 retract rc=%d retracted=%u (want 0 / 0)", rrc, retracted);
    check(idx_count_of(part, "ts") == A4_BLK,
          "A4 ts.idx still declares %ld blocks (want %d)",
          idx_count_of(part, "ts"), A4_BLK);

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dd, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
    }
    qres_t c1 = q(db, "SELECT count(*) FROM r");
    qres_t v1 = q(db, "SELECT v FROM r");
    check(c1.rc == TSDB_OK && c1.sum == A4_ROWS,
          "A4 count(*) = %lld (want %d) rc=%d", c1.sum, A4_ROWS, c1.rc);
    check(v1.rc == TSDB_OK && v1.rows == A4_ROWS && v1.sum == triangle(A4_ROWS),
          "A4 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d",
          v1.rows, v1.sum, A4_ROWS, triangle(A4_ROWS), v1.rc);

done:
    if (db) tsdb_close(db);
    if (src) tsdb_close(src);
    cap_free();
    rm_rf(sd); rm_rf(dd);
}

/* ==========================================================================
 * [A5] a WHOLE column re-synced into a legacy partition — every entry of the
 *      column is marked, every entry of ts is not
 * ==========================================================================
 *
 * [A4] is the MIXED index: some of the column's entries carry an ordinal, so
 * `col_ord_mode` is off and the repair falls back on the content key.  Losing
 * the column's files outright and re-pulling it — the coarser repair the engine's
 * own error message asks for, and the one an operator reaches for first — leaves
 * the opposite shape: EVERY entry of the column is marked, so `col_ord_mode` is
 * ON, and it is decided from the column's entries ALONE.
 *
 * The ts side is still legacy, so its "ordinals" are the physical positions
 * idx_eff_ord() invents (0, 1, 2), while the re-synced column was translated
 * into free local ordinals — which start at ts.idx's entry count BY
 * CONSTRUCTION (tsdb_part_next_ordinal's legacy floor), so they are 3, 4, 5 and
 * cannot collide with a position.  Driving the ordinal rule against that pairs
 * NOTHING, and the repair concludes the partition is torn from block 0 and
 * republishes ts.idx at ZERO entries — deleting every row of a partition that
 * reads back perfectly, which is the state tsdb_part_open reaches on exactly
 * these bytes (test_adv_repair_portable certifies rc=0 rows=3072 sum=4720128 on
 * them).  A repair primitive must never delete rows the reader can serve.
 */

#define A5_BLK  3
#define A5_ROWS (A5_BLK * BP)

static void test_whole_column_resync(void) {
    printf("\n[A5] a whole-column re-sync into a legacy partition: retract is "
           "a no-op\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_retract_a5s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_retract_a5d_%d", (int)getpid());
    rm_rf(sd); rm_rf(dd);
    cap_free();

    tsdb_db_t *src = NULL, *db = NULL;
    HARD(tsdb_open(sd, &src));
    HARD(tsdb_create_table_ex2(src, "r", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, NULL);
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(src, "r", &t));
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int i = 0; i < A5_ROWS; i++) {
            tsdb_batch_row_ts(b, D0 + (int64_t)i * STEP);
            tsdb_batch_row_i64(b, 1, i + 1);
            tsdb_batch_row_end(b);
        }
        HARD(tsdb_batch_commit(b));
        HARD(tsdb_db_flush_all(src));
    }

    HARD(tsdb_open(dd, &db));
    HARD(tsdb_create_table_local_ex(db, "r", COLS2, 2, "ts", 0, BP, -1));
    int ok = 1;
    for (int i = 0; i < A5_BLK; i++) {
        if (push_block(db, cap_pick(0, i)) != TSDB_OK) ok = 0;
        if (push_block(db, cap_pick(1, i)) != TSDB_OK) ok = 0;
    }
    check(ok, "A5 replica received ts x%d and v x%d", A5_BLK, A5_BLK);

    char part[4096];
    if (!only_part(dd, "r", part, sizeof part)) {
        check(0, "A5 harness: no partition"); goto done;
    }
    /* Byte for byte what a node that was never upgraded holds. */
    check(part_strip_ords(part), "A5 replica index rewritten as legacy");
    part_drop_ordmap(part);

    /* The value column's files are lost, and the peer re-syncs the WHOLE
     * column — the repair the read-side error message asks for. */
    {
        char p1[4300], p2[4300];
        snprintf(p1, sizeof p1, "%s/v.idx", part);
        snprintf(p2, sizeof p2, "%s/v.col", part);
        unlink(p1); unlink(p2);
    }
    ok = 1;
    for (int i = 0; i < A5_BLK; i++)
        if (push_block(db, cap_pick(1, i)) != TSDB_OK) ok = 0;
    check(ok, "A5 the three repair pushes land");
    check(idx_count_of(part, "v") == A5_BLK,
          "A5 v.idx holds %ld blocks (want %d)", idx_count_of(part, "v"), A5_BLK);

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dd, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
    }
    qres_t v0 = q(db, "SELECT v FROM r");
    check(v0.rc == TSDB_OK && v0.rows == A5_ROWS && v0.sum == triangle(A5_ROWS),
          "A5 the repaired partition reads whole: rows=%lld sum=%lld "
          "(want %d / %lld) rc=%d", v0.rows, v0.sum, A5_ROWS,
          triangle(A5_ROWS), v0.rc);

    uint32_t retracted = 99;
    tsdb_schema_t *s = sch(db, "r");
    int rrc = s ? tsdb_part_ts_retract_unpaired(s, part, &retracted)
                : TSDB_ERR_INTERNAL;
    /* THE assertion: the reader pairs every block, so nothing is unpaired and
     * nothing may be deleted. */
    check(rrc == TSDB_OK && retracted == 0,
          "A5 retract rc=%d retracted=%u (want 0 / 0)", rrc, retracted);
    check(idx_count_of(part, "ts") == A5_BLK,
          "A5 ts.idx still declares %ld blocks (want %d)",
          idx_count_of(part, "ts"), A5_BLK);

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dd, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
    }
    qres_t c1 = q(db, "SELECT count(*) FROM r");
    qres_t v1 = q(db, "SELECT v FROM r");
    check(c1.rc == TSDB_OK && c1.sum == A5_ROWS,
          "A5 count(*) = %lld (want %d) rc=%d", c1.sum, A5_ROWS, c1.rc);
    check(v1.rc == TSDB_OK && v1.rows == A5_ROWS && v1.sum == triangle(A5_ROWS),
          "A5 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d",
          v1.rows, v1.sum, A5_ROWS, triangle(A5_ROWS), v1.rc);

done:
    if (db) tsdb_close(db);
    if (src) tsdb_close(src);
    cap_free();
    rm_rf(sd); rm_rf(dd);
}

/* ==========================================================================
 * [A6] the same re-sync, PARTIAL: only the tail blocks came back
 * ==========================================================================
 *
 * [A5] with one push short — which is all a dropped connection costs.  The
 * column's surviving entries are a contiguous SUFFIX of ts's blocks, every one
 * of them marked, against a ts that is not; that is the ALTER-shaped case the
 * suffix guard above exists for, and tsdb_part_open answers it exactly that way:
 * the leading ts block reads as the zeros a late add legitimately has, the rest
 * read their real values, rc=0.
 *
 * The retract has to agree with the reader or it deletes what the reader serves.
 * Comparing a translated ordinal against ts's invented position makes the suffix
 * guard reject a suffix that IS one, and the fall-through then finds ts block 0
 * unpairable and republishes ts.idx at zero entries — taking blocks 1 and 2,
 * which pair perfectly, down with it. */

#define A6_BLK  3
#define A6_ROWS (A6_BLK * BP)
#define A6_KEPT (A6_ROWS - BP)

static void test_partial_column_resync(void) {
    printf("\n[A6] a PARTIAL re-sync (the tail blocks only): retract is a "
           "no-op\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_retract_a6s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_retract_a6d_%d", (int)getpid());
    rm_rf(sd); rm_rf(dd);
    cap_free();

    tsdb_db_t *src = NULL, *db = NULL;
    HARD(tsdb_open(sd, &src));
    HARD(tsdb_create_table_ex2(src, "r", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, NULL);
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(src, "r", &t));
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int i = 0; i < A6_ROWS; i++) {
            tsdb_batch_row_ts(b, D0 + (int64_t)i * STEP);
            tsdb_batch_row_i64(b, 1, i + 1);
            tsdb_batch_row_end(b);
        }
        HARD(tsdb_batch_commit(b));
        HARD(tsdb_db_flush_all(src));
    }

    HARD(tsdb_open(dd, &db));
    HARD(tsdb_create_table_local_ex(db, "r", COLS2, 2, "ts", 0, BP, -1));
    int ok = 1;
    for (int i = 0; i < A6_BLK; i++) {
        if (push_block(db, cap_pick(0, i)) != TSDB_OK) ok = 0;
        if (push_block(db, cap_pick(1, i)) != TSDB_OK) ok = 0;
    }
    check(ok, "A6 replica received ts x%d and v x%d", A6_BLK, A6_BLK);

    char part[4096];
    if (!only_part(dd, "r", part, sizeof part)) {
        check(0, "A6 harness: no partition"); goto done;
    }
    check(part_strip_ords(part), "A6 replica index rewritten as legacy");
    part_drop_ordmap(part);

    /* The column is lost and the peer gets only the last two blocks back. */
    {
        char p1[4300], p2[4300];
        snprintf(p1, sizeof p1, "%s/v.idx", part);
        snprintf(p2, sizeof p2, "%s/v.col", part);
        unlink(p1); unlink(p2);
    }
    ok = 1;
    for (int i = 1; i < A6_BLK; i++)
        if (push_block(db, cap_pick(1, i)) != TSDB_OK) ok = 0;
    check(ok, "A6 the two tail pushes land, the first never arrives");
    check(idx_count_of(part, "v") == A6_BLK - 1,
          "A6 v.idx holds %ld blocks (want %d)",
          idx_count_of(part, "v"), A6_BLK - 1);

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dd, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
    }
    qres_t v0 = q(db, "SELECT v FROM r");
    check(v0.rc == TSDB_OK && v0.rows == A6_ROWS &&
          v0.sum == triangle(A6_ROWS) - triangle(BP),
          "A6 the reader serves it as a late add: rows=%lld sum=%lld "
          "(want %d / %lld) rc=%d", v0.rows, v0.sum, A6_ROWS,
          triangle(A6_ROWS) - triangle(BP), v0.rc);

    uint32_t retracted = 99;
    tsdb_schema_t *s = sch(db, "r");
    int rrc = s ? tsdb_part_ts_retract_unpaired(s, part, &retracted)
                : TSDB_ERR_INTERNAL;
    check(rrc == TSDB_OK && retracted == 0,
          "A6 retract rc=%d retracted=%u (want 0 / 0)", rrc, retracted);
    check(idx_count_of(part, "ts") == A6_BLK,
          "A6 ts.idx still declares %ld blocks (want %d)",
          idx_count_of(part, "ts"), A6_BLK);

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dd, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
    }
    qres_t c1 = q(db, "SELECT count(*) FROM r");
    qres_t v1 = q(db, "SELECT v FROM r");
    check(c1.rc == TSDB_OK && c1.sum == A6_ROWS,
          "A6 count(*) = %lld (want %d) rc=%d", c1.sum, A6_ROWS, c1.rc);
    check(v1.rc == TSDB_OK && v1.rows == A6_ROWS &&
          v1.sum == triangle(A6_ROWS) - triangle(BP),
          "A6 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d — the %d rows "
          "that pair are STILL served", v1.rows, v1.sum, A6_ROWS,
          triangle(A6_ROWS) - triangle(BP), v1.rc, A6_KEPT);

done:
    if (db) tsdb_close(db);
    if (src) tsdb_close(src);
    cap_free();
    rm_rf(sd); rm_rf(dd);
}

/* ==========================================================================
 * [A3] the control — a real interior gap must STILL be retracted
 * ========================================================================== */

#define A3_BLK  4
#define A3_ROWS (A3_BLK * BP)

static void test_interior_gap_still_retracts(void) {
    printf("\n[A3] an interior gap in a value column is still repaired\n");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_retract_gap_%d", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "r", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int i = 0; i < A3_ROWS; i++) {
            tsdb_batch_row_ts(b, D0 + (int64_t)i * STEP);
            tsdb_batch_row_i64(b, 1, i + 1);
            tsdb_batch_row_end(b);
        }
        HARD(tsdb_batch_commit(b));
        HARD(tsdb_db_flush_all(db));
    }
    tsdb_close(db); db = NULL;

    char part[4096];
    if (!only_part(dir, "r", part, sizeof part)) {
        check(0, "A3 harness: no partition"); rm_rf(dir); return;
    }
    /* Lose the MIDDLE v block — the dropped-push shape, and the one a suffix
     * test must not mistake for a late add. */
    check(idx_drop_entry(part, "v", 1), "A3 v.idx entry 1 dropped");
    check(idx_count_of(part, "ts") == A3_BLK && idx_count_of(part, "v") == A3_BLK - 1,
          "A3 ts.idx=%ld v.idx=%ld (want %d / %d)",
          idx_count_of(part, "ts"), idx_count_of(part, "v"), A3_BLK, A3_BLK - 1);

    HARD(tsdb_open(dir, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "r", &t));      /* tables load lazily */
    }
    uint32_t retracted = 0;
    tsdb_schema_t *s = sch(db, "r");
    int rrc = s ? tsdb_part_ts_retract_unpaired(s, part, &retracted)
                : TSDB_ERR_INTERNAL;
    check(rrc == TSDB_OK && retracted == A3_BLK - 1,
          "A3 retract rc=%d retracted=%u (want 0 / %d)",
          rrc, retracted, A3_BLK - 1);
    check(idx_count_of(part, "ts") == 1,
          "A3 ts.idx lowered to the paired prefix: %ld block (want 1)",
          idx_count_of(part, "ts"));

    tsdb_close(db); db = NULL;
    HARD(tsdb_open(dir, &db));
    qres_t c = q(db, "SELECT count(*) FROM r");
    qres_t sv = q(db, "SELECT v FROM r");
    check(c.rc == TSDB_OK && c.sum == BP,
          "A3 count(*) now reports only the readable prefix: %lld (want %d) rc=%d",
          c.sum, BP, c.rc);
    check(sv.rc == TSDB_OK && sv.rows == BP && sv.sum == triangle(BP),
          "A3 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d",
          sv.rows, sv.sum, BP, triangle(BP), sv.rc);

    tsdb_close(db);
    rm_rf(dir);
}

/* ========================================================================== */

int main(void) {
    printf("=== test_retract_short_column: a SHORT column is a late add, "
           "a TORN one is not ===\n");

    test_legacy_short_column(1);
    test_legacy_short_column(0);
    test_repaired_legacy_index();
    test_whole_column_resync();
    test_partial_column_resync();
    test_interior_gap_still_retracts();

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
