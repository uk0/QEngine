/* test_block_ordinal_space.c — the durable block ordinal is an IDENTITY, and an
 * identity has to survive everything that touches the partition.
 *
 * A sibling file (test_dup_ts_block_pair.c and friends) covers the read rule:
 * a column's block is addressed by the ts block's ordinal, never by a content
 * scan, because (ts_min, ts_max, count) is not a key.  That rule is only worth
 * anything if the NUMBER it reads is trustworthy.  This file covers where the
 * number comes from and who is allowed to believe it — four failures that have
 * nothing to do with duplicate timestamps at all:
 *
 *   [O1] ALLOCATION.  Compaction re-cuts a partition into fewer, larger blocks
 *        and is per-node — it is never replicated.  Stamping the output INDEX
 *        renumbered the space downward, and seeding the next flush from the
 *        local ts BLOCK COUNT then re-issued ordinals a replica still held for
 *        entirely different rows.  The applier read those as re-deliveries and
 *        dropped them with rc == TSDB_OK, and the matching ts push was then
 *        refused for a group the replica "already had": the partition stopped
 *        replicating, permanently, and every retry hit the same collision.
 *        The space must be monotone for the partition's whole lifetime.
 *
 *   [O2] APPLIER TRUST.  A receiver cannot verify a remote ordinal space at all:
 *        the number is partition-local to the node that ISSUED it, every node
 *        compacts on its own schedule and never replicates the result, and the
 *        receiver's own flush allocates out of the same range.  Believing one
 *        failed in both directions — dropping a genuinely different block as a
 *        re-delivery (rc == TSDB_OK, rows gone), and refusing every later push
 *        with TSDB_ERR_CORRUPT once a replica-side compaction had burned the
 *        range.  The receiver must TRANSLATE into its own space instead, so a
 *        re-issued remote ordinal is neither eaten nor refused, while a genuine
 *        re-delivery — same group, same bytes — is still absorbed silently.
 *
 *   [O3] LEGACY ENTRIES.  An entry with no marker has no ordinal.  Handing it
 *        its physical position instead is exactly false in the state a repair
 *        push exists to fix: after a lost push the surviving entries have
 *        closed up over the gap, so the n-th entry is no longer ordinal n.  The
 *        invented ordinal collided with the wrong entry, the repair was dropped
 *        as a re-delivery, and the column read TSDB_ERR_CORRUPT forever — on
 *        the rolling-upgrade path, where the fix was supposed to help most.
 *
 *   [O4] PARTIAL UPGRADE.  A flush only APPENDS to ts.idx, so a partition an
 *        older binary started keeps its unmarked legacy prefix forever.  Gating
 *        ordinal resolution on "every ts entry carries one" therefore turned it
 *        off for that partition permanently — and on a one-timestamp table the
 *        legacy content rule then calls the placement ambiguous and marks the
 *        whole column unreadable.  Measured: ALTER TABLE ADD COLUMN performed by
 *        the PATCHED binary produced a column that never read.  That is this
 *        binary's own ordinary write path, not old data.
 *
 *   [O8] THE ISSUER.  An ordinal names a group inside the partition that
 *        ISSUED it, so it is only half an identity on the wire.  Two shards of
 *        one synchronised ingest write the SAME timestamp grid, both number
 *        from 0, and their ts blocks are byte-identical — every field a
 *        receiver can key on agrees.  Without the issuer the two groups collapse
 *        onto one local ordinal and the second sender's ts block is ACKed and
 *        discarded while its value block overwrites the first's.
 *
 *   [O9] COLUMN-SIDE REPETITION.  Uniqueness of the content key is load-bearing
 *        on the TS side only.  Two column entries under one key describe the
 *        SAME rows (the orphan-entry + WAL-re-flush shape part_ordref_find
 *        already handles by taking the last); calling that ambiguity turned
 *        every ts block into a HOLE and the column into a permanent read error.
 *
 *   [O5] REPORTING.  "The ordinal paired and the metadata disagreed" is a
 *        different fact from "the column has no block here" — the indexes
 *        contradict each other rather than one of them being short — and the
 *        read path advertises a separate error and a separate counter for it.
 *        Both have to actually be reachable.
 *
 * Every case is asserted on the FILES or on query results, never on a log line.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"
#include "../src/cluster/rawblock.h"
#include "../src/server/metrics.h"

#include <dirent.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

static int g_pass = 0;
static int g_fail = 0;

static void check(int cond, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void check(int cond, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
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

/* ---- layout -------------------------------------------------------------- */

#define DAY_NS  86400000000000LL
#define D0      ((1700000000000000000LL / DAY_NS) * DAY_NS)   /* day-aligned  */
#define STEP    1000000LL                                     /* 1 ms         */
#define BP      1024        /* schema_create's low clamp — the smallest cut   */

/* One repeated timestamp, for the shapes where the content key collides. */
#define TS_ONE  (D0 + 3600000000000LL)

static long long triangle(long long n) { return n * (n + 1) / 2; }

/* ---- filesystem ---------------------------------------------------------- */

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

/* The compactor skips any partition touched in the last 60 s. */
static void backdate_all(const char *db_dir) {
    DIR *d = opendir(db_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char td[4096]; snprintf(td, sizeof(td), "%s/%s", db_dir, e->d_name);
        struct stat st;
        if (stat(td, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        DIR *tdd = opendir(td);
        if (!tdd) continue;
        struct dirent *pe;
        while ((pe = readdir(tdd))) {
            if (pe->d_name[0] == '.') continue;
            char pd[8192]; snprintf(pd, sizeof(pd), "%s/%s", td, pe->d_name);
            struct stat ps;
            if (stat(pd, &ps) != 0 || !S_ISDIR(ps.st_mode)) continue;
            DIR *dd = opendir(pd);
            if (dd) {
                struct dirent *fe;
                while ((fe = readdir(dd))) {
                    if (fe->d_name[0] == '.') continue;
                    char fp[12288]; snprintf(fp, sizeof(fp), "%s/%s", pd, fe->d_name);
                    struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
                    utime(fp, &tb);
                }
                closedir(dd);
            }
            struct utimbuf tb; tb.actime = tb.modtime = time(NULL) - 3600;
            utime(pd, &tb);
        }
        closedir(tdd);
    }
    closedir(d);
}

/* The single partition directory of a table (these fixtures write one day). */
static int only_part(const char *db_dir, const char *table,
                     char *out, size_t cap) {
    char td[4096]; snprintf(td, sizeof(td), "%s/%s", db_dir, table);
    DIR *d = opendir(td);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
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

typedef struct {
    uint8_t *buf;
    size_t   len;
    size_t   hdr;
    uint32_t esz;
    uint32_t count;
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

/* Effective ordinals of every entry, plus how many actually carry a marker. */
static int idx_ords(const char *part, const char *col,
                    uint32_t *out, size_t cap, size_t *n_out, size_t *marked) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return 0;
    size_t n = 0, mk = 0;
    for (uint32_t i = 0; i < im.count && n < cap; i++) {
        const uint8_t *e = im.buf + im.hdr + (size_t)i * im.esz;
        uint32_t v = i;                                     /* legacy: position */
        if (im.esz >= 88 && g16(e + 86) == TSDB_IDX_ORD_MARK) {
            v = g32(e + 82);
            mk++;
        }
        out[n++] = v;
    }
    idx_free(&im);
    if (n_out)  *n_out  = n;
    if (marked) *marked = mk;
    return 1;
}

/* Erase bytes [82..87] of every entry: byte for byte what an UNPATCHED writer
 * left there, since the field was reserved and written as zero. */
static int idx_strip_ords(const char *part, const char *col) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return 0;
    if (im.esz < 88) { idx_free(&im); return 1; }        /* nothing to strip */
    for (uint32_t i = 0; i < im.count; i++)
        memset(im.buf + im.hdr + (size_t)i * im.esz + 82, 0, 6);
    int ok = idx_store(part, col, &im);
    idx_free(&im);
    return ok;
}

/* Strip every column of a partition — an entire partition written pre-upgrade. */
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

/* Make entry `i`'s row count disagree with the ts block's while leaving its
 * ordinal alone: the pairing still resolves, the content check then does not.
 * `size` is untouched, so tsdb_part_open's per-block size filter keeps it. */
static int idx_poison_count(const char *part, const char *col, uint32_t i) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return 0;
    if (i >= im.count) { idx_free(&im); return 0; }
    uint8_t *e = im.buf + im.hdr + (size_t)i * im.esz;
    p32(e + 12, g32(e + 12) - 1);
    int ok = idx_store(part, col, &im);
    idx_free(&im);
    return ok;
}

/* ---- metrics ------------------------------------------------------------- */

static uint64_t metric_of(const char *name) {
    size_t len = 0;
    char *txt = tsdb_metrics_render(&len);
    if (!txt) return UINT64_MAX;
    uint64_t v = UINT64_MAX;
    char needle[128];
    snprintf(needle, sizeof needle, "\n%s ", name);
    const char *p = strstr(txt, needle);
    if (p) v = strtoull(p + strlen(needle), NULL, 10);
    free(txt);
    return v;
}

/* ---- query helpers ------------------------------------------------------- */

typedef struct {
    int       rc;
    long long rows;
    long long sum;      /* of column 0, folded UNSIGNED (values may be ts) */
    char      err[512];
} qres_t;

static qres_t q(tsdb_db_t *db, const char *sql) {
    qres_t o; memset(&o, 0, sizeof o);
    tsdb_result_t *r = NULL;
    o.rc = tsdb_query(db, sql, &r);
    if (o.rc == TSDB_OK && r) {
        uint64_t acc = 0;
        while (tsdb_result_next(r) > 0) {
            acc += (uint64_t)tsdb_result_i64(r, 0);
            o.rows++;
        }
        o.sum = (long long)acc;
    }
    if (r) tsdb_result_free(r);
    const char *e = tsdb_last_error();
    snprintf(o.err, sizeof o.err, "%s", e ? e : "");
    return o;
}

/* SELECT ts, v over a table seeded with `first_row=0`, answering the question a
 * sum cannot: is every value on the row it was written to?  A sum is invariant
 * under any permutation of the blocks, so it cannot see a block served for the
 * wrong ts block at all. */
typedef struct { int rc; long long rows; int paired; } pairres_t;

static pairres_t q_pairs(tsdb_db_t *db, const char *tbl, long long n) {
    pairres_t o; memset(&o, 0, sizeof o);
    o.paired = 1;
    char sql[128];
    snprintf(sql, sizeof sql, "SELECT ts, v FROM %s", tbl);
    tsdb_result_t *r = NULL;
    o.rc = tsdb_query(db, sql, &r);
    if (o.rc == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_i64(r, 0);
            int64_t v  = tsdb_result_i64(r, 1);
            o.rows++;
            if (v != (ts - D0) / STEP + 1) o.paired = 0;
        }
        if (o.rows != n) o.paired = 0;
    } else {
        o.paired = 0;
    }
    if (r) tsdb_result_free(r);
    return o;
}

/* ---- fixtures ------------------------------------------------------------ */

static tsdb_col_t COLS2[2] = { {"ts", TSDB_TYPE_TIMESTAMP},
                               {"v",  TSDB_TYPE_INT64} };

/* `n` rows into `tbl`, v = first_v .. first_v+n-1.  `ts_of` picks the stamp:
 * a repeated one when `dup`, else D0 + row*STEP. */
static void seed(tsdb_db_t *db, const char *tbl, long long first_row,
                 long long n, int dup) {
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, tbl, &t));
    tsdb_batch_t *b = NULL;
    HARD(tsdb_batch_begin(t, &b));
    for (long long i = 0; i < n; i++) {
        long long row = first_row + i;
        tsdb_batch_row_ts(b, dup ? TS_ONE : (D0 + row * STEP));
        tsdb_batch_row_i64(b, 1, row + 1);
        tsdb_batch_row_end(b);
    }
    HARD(tsdb_batch_commit(b));
    HARD(tsdb_db_flush_all(db));
}

static long long compact_once(tsdb_db_t *db, const char *db_dir) {
    backdate_all(db_dir);
    tsdb_compactor_opts_t opts;
    memset(&opts, 0, sizeof opts);
    opts.min_blocks_to_compact = 4;
    opts.interval_ns           = 5000000000LL;
    opts.worker_threads        = -1;
    tsdb_compactor_t *c = NULL;
    if (tsdb_compactor_start(db, &opts, &c) != TSDB_OK) return -1;
    int rc = tsdb_compactor_run_once(c);
    tsdb_compactor_stats_t st; memset(&st, 0, sizeof st);
    tsdb_compactor_stats(c, &st);
    tsdb_compactor_stop(c);
    if (rc != TSDB_OK) return -1;
    return (long long)st.parts_merged;
}

/* ==========================================================================
 * [O1] the ordinal space must never renumber downward
 * ========================================================================== */

#define O1_ROWS   8192              /* 8 blocks of BP — above min_blocks 4    */
#define O1_MORE   2048

static int ord_set_has(const uint32_t *v, size_t n, uint32_t want) {
    for (size_t i = 0; i < n; i++) if (v[i] == want) return 1;
    return 0;
}

static void test_ordinal_space_monotone(void) {
    printf("\n[O1] compaction + re-flush keep the partition's ordinal space monotone\n");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_ordspace_o1_%d", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "o1", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    seed(db, "o1", 0, O1_ROWS, 0);

    char part[4096];
    if (!only_part(dir, "o1", part, sizeof part)) {
        check(0, "O1 harness: no partition directory");
        tsdb_close(db); rm_rf(dir); return;
    }

    uint32_t before[64], after[64], final[64];
    size_t nb = 0, na = 0, nf = 0, mk = 0;
    check(idx_ords(part, "ts", before, 64, &nb, &mk) && nb == O1_ROWS / BP,
          "O1 pre-compaction: ts.idx declares %zu blocks (want %d)",
          nb, O1_ROWS / BP);
    check(mk == nb, "O1 pre-compaction: all %zu ts entries carry an ordinal", nb);

    long long merged = compact_once(db, dir);
    check(merged >= 1, "O1 compactor merged %lld partition(s)", merged);

    check(idx_ords(part, "ts", after, 64, &na, &mk) && na > 0 && na < nb,
          "O1 post-compaction: ts.idx re-cut to %zu block(s) from %zu", na, nb);

    /* THE assertion.  Compaction may renumber, but never DOWN into ordinals the
     * partition has already handed out — a replica is still holding those. */
    int reused = 0;
    for (size_t i = 0; i < na; i++)
        if (ord_set_has(before, nb, after[i])) reused++;
    check(reused == 0,
          "O1 compaction re-issued %d ordinal(s) the partition had already used "
          "(want 0)", reused);

    /* And the next flush must continue ABOVE everything, not restart from the
     * post-compaction block count. */
    seed(db, "o1", O1_ROWS, O1_MORE, 0);
    check(idx_ords(part, "ts", final, 64, &nf, &mk) && nf == na + O1_MORE / BP,
          "O1 post-flush: ts.idx declares %zu blocks (want %zu)",
          nf, na + O1_MORE / BP);

    int collided = 0;
    for (size_t i = na; i < nf; i++)
        if (ord_set_has(before, nb, final[i]) || ord_set_has(after, na, final[i]))
            collided++;
    check(collided == 0,
          "O1 the %zu newly flushed block(s) collide with %d earlier ordinal(s) "
          "(want 0)", nf - na, collided);

    /* Data is still whole and correct through all of it. */
    qres_t cnt = q(db, "SELECT count(*) FROM o1");
    qres_t sv  = q(db, "SELECT v FROM o1");
    check(cnt.rc == TSDB_OK && cnt.sum == O1_ROWS + O1_MORE,
          "O1 count(*) = %lld (want %d) rc=%d",
          cnt.sum, O1_ROWS + O1_MORE, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == O1_ROWS + O1_MORE &&
          sv.sum == triangle(O1_ROWS + O1_MORE),
          "O1 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d",
          sv.rows, sv.sum, O1_ROWS + O1_MORE,
          triangle(O1_ROWS + O1_MORE), sv.rc);

    tsdb_close(db);
    rm_rf(dir);
}

/* ==========================================================================
 * [O2] / [O3] the replication applier
 * ========================================================================== */

typedef struct {
    char              table[64];
    uint32_t          part_day;
    uint16_t          col_idx;
    tsdb_block_meta_t meta;
    uint8_t          *bytes;
    size_t            bytes_len;
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

/* Push one captured block through the real receive path.  `force_ord >= 0`
 * overrides the ordinal on the wire — a primary whose space renumbered.
 * `issuer` is the sending node's id, which is what scopes that ordinal. */
static int push_block_from(tsdb_db_t *dst, const cap_blk_t *e, long force_ord,
                           uint64_t issuer) {
    if (!e) return TSDB_ERR_NOTFOUND;
    tsdb_rawblock_push_t r;
    memset(&r, 0, sizeof r);
    snprintf(r.table, sizeof(r.table), "%s", e->table);
    r.part_day = e->part_day;   r.col_idx     = e->col_idx;
    r.codec    = e->meta.codec; r.flags       = e->meta.flags;
    r.count    = e->meta.count; r.ts_min      = e->meta.ts_min;
    r.ts_max   = e->meta.ts_max;
    r.stats_min   = e->meta.stats_min;   r.stats_max   = e->meta.stats_max;
    r.stats_sum   = e->meta.stats_sum;   r.stats_first = e->meta.stats_first;
    r.stats_last  = e->meta.stats_last;  r.stats_flags = e->meta.stats_flags;
    r.ord = e->meta.ord;
    if (force_ord >= 0) {
        r.ord.v    = (uint32_t)force_ord;
        r.ord.mark = TSDB_IDX_ORD_MARK;
    }
    r.issuer          = issuer;
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

static int push_block(tsdb_db_t *dst, const cap_blk_t *e, long force_ord) {
    return push_block_from(dst, e, force_ord, 0);
}

#define REP_BLK  3
#define REP_ROWS (REP_BLK * BP)

/* A primary with REP_BLK full blocks per column, all with DISTINCT timestamps —
 * the content key is unique here, so nothing below is about duplicate keys. */
static tsdb_db_t *build_primary(const char *dir, cap_ctx_t *ctx) {
    rm_rf(dir);
    tsdb_db_t *src = NULL;
    HARD(tsdb_open(dir, &src));
    HARD(tsdb_create_table_ex2(src, "t", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, ctx);
    seed(src, "t", 0, REP_ROWS, 0);
    return src;
}

static tsdb_db_t *build_replica(const char *dir) {
    rm_rf(dir);
    tsdb_db_t *dst = NULL;
    if (tsdb_open(dir, &dst) != TSDB_OK) return NULL;
    if (tsdb_create_table_local_ex(dst, "t", COLS2, 2, "ts", 0, BP, -1, 0)
        != TSDB_OK) { tsdb_close(dst); return NULL; }
    return dst;
}

static long idx_count_of(const char *part, const char *col) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return -1;
    long n = (long)im.count;
    idx_free(&im);
    return n;
}

static void test_applier_ordinal_collision(void) {
    printf("\n[O2] a RE-ISSUED remote ordinal is translated, not eaten and not "
           "refused\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_ordspace_o2s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_ordspace_o2d_%d", (int)getpid());

    cap_ctx_t ctx; memset(&ctx, 0, sizeof ctx);
    tsdb_db_t *src = build_primary(sd, &ctx);
    tsdb_db_t *dst = build_replica(dd);
    if (!dst) { check(0, "O2 harness: replica open failed"); goto done; }

    const cap_blk_t *v0 = pick(&ctx, 1, 0), *v1 = pick(&ctx, 1, 1);
    const cap_blk_t *t0 = pick(&ctx, 0, 0), *t1 = pick(&ctx, 0, 1);
    if (!v0 || !v1 || !t0 || !t1) {
        check(0, "O2 harness: capture missing blocks"); goto done;
    }

    check(push_block(dst, v0, -1) == TSDB_OK && push_block(dst, t0, -1) == TSDB_OK,
          "O2 replica accepts block group 0 (v then ts)");

    char part[4096];
    if (!only_part(dd, "t", part, sizeof part)) {
        check(0, "O2 harness: no replica partition"); goto done;
    }
    check(idx_count_of(part, "v") == 1 && idx_count_of(part, "ts") == 1,
          "O2 replica holds 1 block per column");

    /* The primary renumbered — a compaction it performed and never replicated —
     * and now calls block group 1 "ordinal 0", for every column of the group.
     * Round 1 dropped that as a re-delivery (rc == TSDB_OK, rows gone).  Round 2
     * refused it with TSDB_ERR_CORRUPT and the partition stopped replicating.
     * The number is the SENDER's; it is translated, so neither happens. */
    uint64_t m0 = metric_of("qengine_rawblock_ordinal_collision_total");
    int rcv = push_block(dst, v1, 0);
    int rct = push_block(dst, t1, 0);
    uint64_t m1 = metric_of("qengine_rawblock_ordinal_collision_total");

    check(rcv == TSDB_OK && rct == TSDB_OK,
          "O2 the re-issued ordinal is accepted (v rc=%d, ts rc=%d)", rcv, rct);
    check(idx_count_of(part, "v") == 2 && idx_count_of(part, "ts") == 2,
          "O2 it LANDED: v.idx=%ld ts.idx=%ld (want 2 / 2)",
          idx_count_of(part, "v"), idx_count_of(part, "ts"));
    check(m0 != UINT64_MAX && m1 == m0,
          "O2 nothing collided — the sender's number never entered our space "
          "(%llu -> %llu)", (unsigned long long)m0, (unsigned long long)m1);

    /* Genuine idempotency is untouched: the same group re-delivered under the
     * same sender ordinal is still absorbed silently, at any position. */
    check(push_block(dst, v0, -1) == TSDB_OK && idx_count_of(part, "v") == 2,
          "O2 re-delivering group 0 is still a no-op, rc=OK");
    check(push_block(dst, v1, 0) == TSDB_OK && idx_count_of(part, "v") == 2,
          "O2 re-delivering the RENUMBERED group is a no-op too, rc=OK");

    /* And the two groups pair: the translation gave every column of one group
     * the same local number, which is the only thing the read side asks of it. */
    tsdb_close(dst); dst = NULL;
    HARD(tsdb_open(dd, &dst));
    qres_t cnt = q(dst, "SELECT count(*) FROM t");
    qres_t sv  = q(dst, "SELECT v FROM t");
    check(cnt.rc == TSDB_OK && cnt.sum == 2 * BP,
          "O2 count(*) = %lld (want %d) rc=%d", cnt.sum, 2 * BP, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == 2 * BP && sv.sum == triangle(2 * BP),
          "O2 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, 2 * BP, triangle(2 * BP), sv.rc, sv.err);

done:
    if (src) tsdb_close(src);
    if (dst) tsdb_close(dst);
    cap_free(&ctx);
    rm_rf(sd); rm_rf(dd);
}

static void test_repair_push_into_legacy_index(void) {
    printf("\n[O3] a repair push into a LEGACY index lands (position != ordinal)\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_ordspace_o3s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_ordspace_o3d_%d", (int)getpid());

    cap_ctx_t ctx; memset(&ctx, 0, sizeof ctx);
    tsdb_db_t *src = build_primary(sd, &ctx);
    tsdb_db_t *dst = build_replica(dd);
    if (!dst) { check(0, "O3 harness: replica open failed"); goto done; }

    /* The replica received the whole ts spine and all but ONE v block: the
     * middle one.  That is the ordinary "a push was dropped" state. */
    int ok = 1;
    for (int i = 0; i < REP_BLK; i++)
        if (push_block(dst, pick(&ctx, 0, i), -1) != TSDB_OK) ok = 0;
    if (push_block(dst, pick(&ctx, 1, 0), -1) != TSDB_OK) ok = 0;
    if (push_block(dst, pick(&ctx, 1, 2), -1) != TSDB_OK) ok = 0;
    check(ok, "O3 replica received ts x3 and v blocks 0 and 2");

    char part[4096];
    if (!only_part(dd, "t", part, sizeof part)) {
        check(0, "O3 harness: no replica partition"); goto done;
    }

    /* This replica has not been upgraded, so NOTHING in its index carries an
     * ordinal.  The surviving v entries have closed up over the gap: entry 1 is
     * ordinal 2.  Handing it its position (1) is what made the repair collide. */
    check(part_strip_ords(part), "O3 replica index rewritten as legacy (no marks)");
    size_t n = 0, mk = 0; uint32_t tmp[16];
    check(idx_ords(part, "v", tmp, 16, &n, &mk) && n == 2 && mk == 0,
          "O3 replica v.idx: %zu entries, %zu marked (want 2 / 0)", n, mk);

    int rc = push_block(dst, pick(&ctx, 1, 1), -1);   /* the repair, ord == 1 */
    check(rc == TSDB_OK, "O3 the repair push is accepted (rc=%d)", rc);
    check(idx_count_of(part, "v") == 3,
          "O3 replica v.idx holds 3 blocks (the repair was not dropped)");

    /* And the column reads whole again, which is the point: pre-fix the repair
     * was discarded as a re-delivery and every read stayed TSDB_ERR_CORRUPT. */
    tsdb_close(dst); dst = NULL;
    HARD(tsdb_open(dd, &dst));
    qres_t sv = q(dst, "SELECT v FROM t");
    check(sv.rc == TSDB_OK && sv.rows == REP_ROWS && sv.sum == triangle(REP_ROWS),
          "O3 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, REP_ROWS, triangle(REP_ROWS), sv.rc, sv.err);

done:
    if (src) tsdb_close(src);
    if (dst) tsdb_close(dst);
    cap_free(&ctx);
    rm_rf(sd); rm_rf(dd);
}

/* ==========================================================================
 * [O6] TWO senders, one receiver: two ordinal spaces that mean nothing to
 *      each other and nothing to the receiver
 * ========================================================================== */

/* The flush hook fans a block out to every ALIVE peer regardless of shard
 * ownership, so one node routinely receives pushes from several senders for the
 * same table and day.  Each sender numbers from its OWN partition, so both call
 * their first group "ordinal 0" — and they are not the same rows.  Only a
 * receiver that translates can keep them apart. */
static void test_two_senders_one_receiver(void) {
    printf("\n[O6] two senders both number from 0; the receiver keeps them "
           "apart\n");

    char sa[256], sb[256], dd[256];
    snprintf(sa, sizeof sa, "/tmp/tsdb_ordspace_o6a_%d", (int)getpid());
    snprintf(sb, sizeof sb, "/tmp/tsdb_ordspace_o6b_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_ordspace_o6d_%d", (int)getpid());

    cap_ctx_t ca; memset(&ca, 0, sizeof ca);
    cap_ctx_t cb; memset(&cb, 0, sizeof cb);
    tsdb_db_t *dst = NULL;

    /* Two primaries, disjoint row ranges, same table and same day. */
    rm_rf(sa);
    tsdb_db_t *A = NULL;
    HARD(tsdb_open(sa, &A));
    HARD(tsdb_create_table_ex2(A, "t", COLS2, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(A, cap_hook, &ca);
    seed(A, "t", 0, REP_ROWS, 0);

    rm_rf(sb);
    tsdb_db_t *B = NULL;
    HARD(tsdb_open(sb, &B));
    HARD(tsdb_create_table_ex2(B, "t", COLS2, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(B, cap_hook, &cb);
    seed(B, "t", REP_ROWS, REP_ROWS, 0);

    dst = build_replica(dd);
    if (!dst) { check(0, "O6 harness: replica open failed"); goto done; }

    /* Both senders' first groups carry ordinal 0.  The columns are shipped in
     * DIFFERENT sender orders, which is what a real fan-out looks like and what
     * stops a positional accident from covering the mistake up. */
    int bad = 0;
    for (int j = 0; j < REP_BLK; j++) {
        if (push_block(dst, pick(&ca, 1, j), -1) != TSDB_OK) bad++;   /* A.v */
        if (push_block(dst, pick(&cb, 1, j), -1) != TSDB_OK) bad++;   /* B.v */
    }
    for (int j = 0; j < REP_BLK; j++) {
        if (push_block(dst, pick(&cb, 0, j), -1) != TSDB_OK) bad++;   /* B.ts */
        if (push_block(dst, pick(&ca, 0, j), -1) != TSDB_OK) bad++;   /* A.ts */
    }
    check(bad == 0, "O6 every block of both senders was accepted (%d refused)",
          bad);

    char part[4096];
    if (!only_part(dd, "t", part, sizeof part)) {
        check(0, "O6 harness: no replica partition"); goto done;
    }
    check(idx_count_of(part, "ts") == 2 * REP_BLK &&
          idx_count_of(part, "v")  == 2 * REP_BLK,
          "O6 the receiver holds both senders' blocks: ts=%ld v=%ld (want %d)",
          idx_count_of(part, "ts"), idx_count_of(part, "v"), 2 * REP_BLK);

    /* Believing the wire ordinal makes A's group j and B's group j ONE slot:
     * the ts entries and the value entries then resolve to different senders'
     * rows and the column reads as a mismatch. */
    tsdb_close(dst); dst = NULL;
    HARD(tsdb_open(dd, &dst));
    qres_t cnt = q(dst, "SELECT count(*) FROM t");
    qres_t sv  = q(dst, "SELECT v FROM t");
    check(cnt.rc == TSDB_OK && cnt.sum == 2 * REP_ROWS,
          "O6 count(*) = %lld (want %d) rc=%d", cnt.sum, 2 * REP_ROWS, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == 2 * REP_ROWS &&
          sv.sum == triangle(2 * REP_ROWS),
          "O6 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, 2 * REP_ROWS, triangle(2 * REP_ROWS), sv.rc, sv.err);

done:
    if (A) tsdb_close(A);
    if (B) tsdb_close(B);
    if (dst) tsdb_close(dst);
    cap_free(&ca); cap_free(&cb);
    rm_rf(sa); rm_rf(sb); rm_rf(dd);
}

/* ==========================================================================
 * [O7] one partition that BOTH receives pushes and is flushed into locally
 * ========================================================================== */

/* A node is a replica for some rows and the writer of others, in the same table
 * and the same day.  Both the applier and the flush hand out ordinals here, so
 * they must hand out from the same allocator — a flush that reads only ts.idx
 * cannot see the ordinal a received group has already consumed, because the
 * group's ts block is exactly what is still outstanding. */
static void test_local_flush_beside_a_push(void) {
    printf("\n[O7] a partition written locally AND received into: one "
           "allocator\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_ordspace_o7s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_ordspace_o7d_%d", (int)getpid());

    cap_ctx_t ctx; memset(&ctx, 0, sizeof ctx);
    tsdb_db_t *src = build_primary(sd, &ctx);   /* rows 0..REP_ROWS-1 */
    tsdb_db_t *dst = NULL;

    rm_rf(dd);
    HARD(tsdb_open(dd, &dst));
    HARD(tsdb_create_table_ex2(dst, "t", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));

    /* 1. the VALUE half of a remote group lands; its ts block is still in
     *    flight, so nothing in ts.idx records that the ordinal is taken. */
    check(push_block(dst, pick(&ctx, 1, 0), -1) == TSDB_OK,
          "O7 the received group's value block lands");

    /* 2. this node flushes rows of its OWN into the same partition. */
    seed(dst, "t", REP_ROWS, BP, 0);

    /* 3. the received group's ts block finally arrives. */
    check(push_block(dst, pick(&ctx, 0, 0), -1) == TSDB_OK,
          "O7 the received group's ts block lands");

    char part[4096];
    if (!only_part(dd, "t", part, sizeof part)) {
        check(0, "O7 harness: no partition"); goto done;
    }
    check(idx_count_of(part, "ts") == 2 && idx_count_of(part, "v") == 2,
          "O7 the partition holds both groups: ts=%ld v=%ld (want 2 / 2)",
          idx_count_of(part, "ts"), idx_count_of(part, "v"));

    tsdb_close(dst); dst = NULL;
    HARD(tsdb_open(dd, &dst));
    qres_t cnt = q(dst, "SELECT count(*) FROM t");
    qres_t sv  = q(dst, "SELECT v FROM t");
    check(cnt.rc == TSDB_OK && cnt.sum == 2 * BP,
          "O7 count(*) = %lld (want %d) rc=%d", cnt.sum, 2 * BP, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == 2 * BP &&
          sv.sum == triangle(BP) + (triangle(REP_ROWS + BP) - triangle(REP_ROWS)),
          "O7 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, 2 * BP,
          triangle(BP) + (triangle(REP_ROWS + BP) - triangle(REP_ROWS)),
          sv.rc, sv.err);

done:
    if (src) tsdb_close(src);
    if (dst) tsdb_close(dst);
    cap_free(&ctx);
    rm_rf(sd); rm_rf(dd);
}

/* ==========================================================================
 * [O4] a legacy ts prefix must not make THIS binary's own ALTER unreadable
 * ========================================================================== */

#define O4_OLD  (3 * BP)     /* written "before the upgrade" */
#define O4_NEW  (2 * BP)     /* written after, with the added column */

static void test_legacy_ts_prefix_marked_short_column(void) {
    printf("\n[O4] legacy ts prefix + fully-marked short column stays readable\n");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_ordspace_o4_%d", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "o4", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    /* ONE timestamp for every row: the content key repeats, so the legacy
     * placement rule has nothing to go on and must not be what decides. */
    seed(db, "o4", 0, O4_OLD, 1);
    tsdb_close(db); db = NULL;

    char part[4096];
    if (!only_part(dir, "o4", part, sizeof part)) {
        check(0, "O4 harness: no partition directory"); rm_rf(dir); return;
    }
    check(part_strip_ords(part),
          "O4 partition rewritten as an unpatched binary would have left it");

    size_t n = 0, mk = 0; uint32_t tmp[32];
    check(idx_ords(part, "ts", tmp, 32, &n, &mk) && n == O4_OLD / BP && mk == 0,
          "O4 ts.idx: %zu legacy entries, %zu marked (want %d / 0)",
          n, mk, O4_OLD / BP);

    /* Now the PATCHED binary does an ordinary ALTER + write into that
     * partition.  ts.idx keeps its unmarked prefix forever — a flush only
     * appends — so gating on "every ts entry is marked" would keep this column
     * unreadable no matter how often it is rewritten. */
    HARD(tsdb_open(dir, &db));
    {
        tsdb_table_t *t = NULL;
        HARD(tsdb_open_table(db, "o4", &t));   /* db->tables[] is lazily loaded */
        HARD(tsdb_alter_table_add_column(db, "o4", "w", TSDB_TYPE_INT64));
        HARD(tsdb_open_table(db, "o4", &t));   /* re-resolve after the ALTER   */
        tsdb_batch_t *b = NULL;
        HARD(tsdb_batch_begin(t, &b));
        for (int i = 0; i < O4_NEW; i++) {
            tsdb_batch_row_ts(b, TS_ONE);
            tsdb_batch_row_i64(b, 1, O4_OLD + i + 1);
            tsdb_batch_row_i64(b, 2, i + 1);
            tsdb_batch_row_end(b);
        }
        HARD(tsdb_batch_commit(b));
        HARD(tsdb_db_flush_all(db));
    }
    tsdb_close(db); db = NULL;

    check(idx_ords(part, "w", tmp, 32, &n, &mk) && n == O4_NEW / BP && mk == n,
          "O4 w.idx: %zu entries, %zu marked (want %d / all)",
          n, mk, O4_NEW / BP);

    HARD(tsdb_open(dir, &db));
    qres_t cnt = q(db, "SELECT count(*) FROM o4");
    qres_t sv  = q(db, "SELECT v FROM o4");
    qres_t sw  = q(db, "SELECT w FROM o4");
    tsdb_close(db); db = NULL;

    check(cnt.rc == TSDB_OK && cnt.sum == O4_OLD + O4_NEW,
          "O4 count(*) = %lld (want %d) rc=%d",
          cnt.sum, O4_OLD + O4_NEW, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == O4_OLD + O4_NEW &&
          sv.sum == triangle(O4_OLD + O4_NEW),
          "O4 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d",
          sv.rows, sv.sum, O4_OLD + O4_NEW,
          triangle(O4_OLD + O4_NEW), sv.rc);
    /* THE assertion: the column this binary just wrote reads its own values,
     * with the pre-ALTER rows as the zeros they legitimately are. */
    check(sw.rc == TSDB_OK && sw.rows == O4_OLD + O4_NEW &&
          sw.sum == triangle(O4_NEW),
          "O4 SELECT w rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sw.rows, sw.sum, O4_OLD + O4_NEW, triangle(O4_NEW), sw.rc, sw.err);

    rm_rf(dir);
}

/* ==========================================================================
 * [O5] "the ordinal paired and the rows disagree" is its own reported fact
 * ========================================================================== */

#define O5_ROWS (4 * BP)
#define O5_BAD  2

static void test_mismatch_is_reported_as_mismatch(void) {
    printf("\n[O5] an ordinal that pairs against disagreeing rows is a MISMATCH\n");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_ordspace_o5_%d", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "o5", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    seed(db, "o5", 0, O5_ROWS, 0);
    tsdb_close(db); db = NULL;

    char part[4096];
    if (!only_part(dir, "o5", part, sizeof part)) {
        check(0, "O5 harness: no partition directory"); rm_rf(dir); return;
    }
    check(idx_poison_count(part, "v", O5_BAD),
          "O5 v.idx entry %d keeps its ordinal but claims a different row count",
          O5_BAD);

    uint64_t mm0 = metric_of("qengine_block_ordinal_mismatch_total");
    uint64_t sc0 = metric_of("qengine_short_column_read_total");

    HARD(tsdb_open(dir, &db));
    qres_t cnt = q(db, "SELECT count(*) FROM o5");
    qres_t sv  = q(db, "SELECT v FROM o5");
    tsdb_close(db); db = NULL;

    uint64_t mm1 = metric_of("qengine_block_ordinal_mismatch_total");
    uint64_t sc1 = metric_of("qengine_short_column_read_total");

    check(cnt.rc == TSDB_OK && cnt.sum == O5_ROWS,
          "O5 the damage is confined: count(*) = %lld (want %d) rc=%d",
          cnt.sum, O5_ROWS, cnt.rc);
    check(sv.rc == TSDB_ERR_CORRUPT,
          "O5 SELECT v fails rather than serving another block (rc=%d)", sv.rc);
    check(strstr(sv.err, "ordinal") != NULL,
          "O5 the error names the ordinal disagreement: \"%s\"", sv.err);
    check(mm0 != UINT64_MAX && mm1 > mm0,
          "O5 qengine_block_ordinal_mismatch_total %llu -> %llu",
          (unsigned long long)mm0, (unsigned long long)mm1);
    check(sc0 != UINT64_MAX && sc1 == sc0,
          "O5 it is NOT counted as a short column (%llu -> %llu)",
          (unsigned long long)sc0, (unsigned long long)sc1);

    rm_rf(dir);
}

/* ==========================================================================
 * [O8] two senders over ONE timestamp grid — the ordinal needs an ISSUER
 * ========================================================================== */

/* [O6] above keeps two senders apart on distinct timestamps, where the group
 * key (ordinal, ts_min, ts_max, count) differs on its own.  The default
 * topology does not look like that.  Two shards of one synchronised ingest — a
 * metric scrape, a TSBS-style loader, anything with a shared clock — write the
 * SAME timestamp grid with different values, so their blocks agree on ts_min,
 * ts_max and count, both senders number from 0, and their ts payloads are
 * BYTE-IDENTICAL because the timestamps are the same values.
 *
 * Everything a receiver can see about the two groups then agrees except who
 * sent them.  Keyed without an issuer, both collapse onto ONE local ordinal:
 * the second sender's ts block matches at that ordinal, its bytes agree, and it
 * is absorbed as a re-delivery — ACKed and destroyed — while its value block,
 * whose bytes do NOT agree, lands as a second claimant of the same ordinal and
 * the surviving ts block is answered with the wrong sender's values.
 *
 * Measured with the issuer removed from the key: count(*) = 3072 of 6144, and
 * SELECT v returns sender B's 3072 values twice over. */
static void test_two_senders_one_timestamp_grid(void) {
    printf("\n[O8] two senders over ONE timestamp grid: the ordinal is scoped "
           "to its issuer\n");

    char sa[256], sb[256], dd[256];
    snprintf(sa, sizeof sa, "/tmp/tsdb_ordspace_o8a_%d", (int)getpid());
    snprintf(sb, sizeof sb, "/tmp/tsdb_ordspace_o8b_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_ordspace_o8d_%d", (int)getpid());

    cap_ctx_t ca; memset(&ca, 0, sizeof ca);
    cap_ctx_t cb; memset(&cb, 0, sizeof cb);
    tsdb_db_t *A = NULL, *B = NULL, *dst = NULL;

    /* Same timestamps, disjoint values.  A holds v = 1..REP_ROWS, B holds
     * v = 10000001..10000000+REP_ROWS, both at ts D0 + row*STEP. */
    for (int side = 0; side < 2; side++) {
        const char *dir = side ? sb : sa;
        cap_ctx_t  *cc  = side ? &cb : &ca;
        tsdb_db_t **out = side ? &B  : &A;
        rm_rf(dir);
        HARD(tsdb_open(dir, out));
        HARD(tsdb_create_table_ex2(*out, "t", COLS2, 2, "ts",
                                   TSDB_CREATE_PART_DAY, BP));
        tsdb_db_set_raw_block_hook(*out, cap_hook, cc);
        tsdb_table_t *tb = NULL;
        HARD(tsdb_open_table(*out, "t", &tb));
        tsdb_batch_t *bt = NULL;
        HARD(tsdb_batch_begin(tb, &bt));
        for (long long i = 0; i < REP_ROWS; i++) {
            tsdb_batch_row_ts(bt, D0 + i * STEP);
            tsdb_batch_row_i64(bt, 1, (side ? 10000000LL : 0LL) + i + 1);
            tsdb_batch_row_end(bt);
        }
        HARD(tsdb_batch_commit(bt));
        HARD(tsdb_db_flush_all(*out));
    }

    /* The collision, measured rather than assumed. */
    {
        const cap_blk_t *ta = pick(&ca, 0, 0), *tb2 = pick(&cb, 0, 0);
        int same = (ta && tb2 && ta->bytes_len == tb2->bytes_len &&
                    ta->meta.count  == tb2->meta.count &&
                    ta->meta.ts_min == tb2->meta.ts_min &&
                    ta->meta.ts_max == tb2->meta.ts_max &&
                    memcmp(ta->bytes, tb2->bytes, ta->bytes_len) == 0);
        check(same, "O8 both senders' ts block 0 is byte-identical and shares "
                    "the whole group key");
    }

    dst = build_replica(dd);
    if (!dst) { check(0, "O8 harness: replica open failed"); goto done; }

    int bad = 0;
    for (int j = 0; j < REP_BLK; j++) {
        if (push_block_from(dst, pick(&ca, 1, j), -1, 101) != TSDB_OK) bad++;
        if (push_block_from(dst, pick(&cb, 1, j), -1, 202) != TSDB_OK) bad++;
    }
    for (int j = 0; j < REP_BLK; j++) {
        if (push_block_from(dst, pick(&cb, 0, j), -1, 202) != TSDB_OK) bad++;
        if (push_block_from(dst, pick(&ca, 0, j), -1, 101) != TSDB_OK) bad++;
    }
    check(bad == 0, "O8 every block of both senders was accepted (%d refused)",
          bad);

    char part[4096];
    if (!only_part(dd, "t", part, sizeof part)) {
        check(0, "O8 harness: no replica partition"); goto done;
    }
    check(idx_count_of(part, "ts") == 2 * REP_BLK &&
          idx_count_of(part, "v")  == 2 * REP_BLK,
          "O8 nothing was eaten: ts.idx=%ld v.idx=%ld (want %d each)",
          idx_count_of(part, "ts"), idx_count_of(part, "v"), 2 * REP_BLK);

    tsdb_close(dst); dst = NULL;
    HARD(tsdb_open(dd, &dst));
    qres_t cnt = q(dst, "SELECT count(*) FROM t");
    qres_t sv  = q(dst, "SELECT v FROM t");
    /* sum of 1..REP_ROWS plus sum of 10000001..10000000+REP_ROWS. */
    long long want = triangle(REP_ROWS) +
                     triangle(REP_ROWS) + 10000000LL * REP_ROWS;
    check(cnt.rc == TSDB_OK && cnt.sum == 2 * REP_ROWS,
          "O8 count(*) = %lld (want %d) rc=%d", cnt.sum, 2 * REP_ROWS, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == 2 * REP_ROWS && sv.sum == want,
          "O8 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, 2 * REP_ROWS, want, sv.rc, sv.err);

done:
    if (A) tsdb_close(A);
    if (B) tsdb_close(B);
    if (dst) tsdb_close(dst);
    cap_free(&ca); cap_free(&cb);
    rm_rf(sa); rm_rf(sb); rm_rf(dd);
}

/* ==========================================================================
 * [O9] a repeated key on the COLUMN side is not ambiguity
 * ========================================================================== */

/* Duplicate entry `k` of a column index in place.  That is not a corrupt shape:
 * part_ordref_find names its producer — a flush that published a non-ts column
 * and died before ts leaves the column an orphan entry, and the WAL replay's
 * re-flush of the same rows publishes it again.  Both entries then describe the
 * SAME rows. */
static int idx_dup_entry(const char *part, const char *col, uint32_t k) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return 0;
    if (k >= im.count) { idx_free(&im); return 0; }
    uint8_t *nb = realloc(im.buf, im.len + im.esz);
    if (!nb) { idx_free(&im); return 0; }
    im.buf = nb;
    uint8_t *base = im.buf + im.hdr;
    memmove(base + (size_t)(k + 1) * im.esz, base + (size_t)k * im.esz,
            (size_t)(im.count - k) * im.esz);
    im.count++;
    p32(im.buf + 4, im.count);
    im.len += im.esz;
    int ok = idx_store(part, col, &im);
    idx_free(&im);
    return ok;
}

/* Shift every entry's ordinal of ONE column by `delta`, keeping the mark: a
 * column that is fully stamped and numbered in a space that does not meet ts's.
 * Nothing else about the entries changes. */
static int idx_shift_ords(const char *part, const char *col, uint32_t delta) {
    idx_img_t im;
    if (!idx_load(part, col, &im)) return 0;
    if (im.esz < 88) { idx_free(&im); return 0; }
    for (uint32_t i = 0; i < im.count; i++) {
        uint8_t *e = im.buf + im.hdr + (size_t)i * im.esz;
        if (g16(e + 86) != TSDB_IDX_ORD_MARK) { idx_free(&im); return 0; }
        p32(e + 82, g32(e + 82) + delta);
    }
    int ok = idx_store(part, col, &im);
    idx_free(&im);
    return ok;
}

/* A column that pairs with NO ts ordinal, on a partition whose TS SIDE IS
 * FULLY MARKED.
 *
 * `matched == 0` says the two sides are numbering in spaces that do not meet.
 * It does NOT say the column lost its blocks, and a marked ts does not make it
 * say that either — tsdb_part_next_ordinal never re-issues a number the
 * partition has already bound, so a column re-delivered into a partition THIS
 * BINARY flushed is stamped in the free range ABOVE everything ts owns, by
 * construction, exactly as it is on a legacy one.  That is the ordinary repair
 * of a lost column on an upgraded fleet, and it is what this fixture builds:
 * every block is on disk and only the numbers were moved.
 *
 * So the answer must be decided by what is left on disk that CAN decide, which
 * is the content key — forced where the ts keys are distinct (here), refused
 * where they repeat (O11).  The two things it must never be are a fabricated
 * zero and another block's values:
 *
 *   - Without the `matched > 0` conjunct in the alter-shaped test, nmiss ==
 *     nb_ts makes "every slot before nmiss is unpaired" vacuously true, every
 *     slot becomes a zero-fill sentinel and the column answers rc=0 with a
 *     fabricated zero for every row.  O11 is where that is measurable, because
 *     there the content rule declines to place anything.
 *   - Serving another block's values is what the whole series removes, and
 *     q_pairs is what sees it; a sum cannot. */
static void test_marked_column_matching_no_ts_ordinal(void) {
    printf("\n[O10] a fully marked column numbered clear of ts: the content "
           "key decides, and it is FORCED here\n");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_ordspace_o10_%d", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "o10", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    seed(db, "o10", 0, REP_ROWS, 0);
    tsdb_close(db); db = NULL;

    char part[4096];
    if (!only_part(dir, "o10", part, sizeof part)) {
        check(0, "O10 harness: no partition directory"); rm_rf(dir); return;
    }

    uint32_t ords[64]; size_t n = 0, marked = 0;
    check(idx_ords(part, "ts", ords, 64, &n, &marked) && n > 0 && marked == n,
          "O10 the ts side is FULLY marked (%zu of %zu entries)", marked, n);
    check(idx_shift_ords(part, "v", 1000),
          "O10 v's ordinals shifted clear of ts's");

    HARD(tsdb_open(dir, &db));
    qres_t cnt = q(db, "SELECT count(*) FROM o10");
    qres_t sv  = q(db, "SELECT v FROM o10");
    pairres_t pr = q_pairs(db, "o10", REP_ROWS);
    tsdb_close(db);

    check(cnt.rc == TSDB_OK && cnt.sum == REP_ROWS,
          "O10 count(*) = %lld (want %d) rc=%d — ts is intact",
          cnt.sum, REP_ROWS, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == REP_ROWS &&
          sv.sum == triangle(REP_ROWS),
          "O10 SELECT v rc=%d rows=%lld sum=%lld (want 0 / %d / %lld) — never "
          "%d fabricated zeros at rc=0, and never a refusal on a partition "
          "whose every block is on disk",
          sv.rc, sv.rows, sv.sum, REP_ROWS, triangle(REP_ROWS), REP_ROWS);
    check(pr.rc == TSDB_OK && pr.paired,
          "O10 every value sits on the row it was written to (paired=%d "
          "rows=%lld rc=%d)", pr.paired, pr.rows, pr.rc);

    rm_rf(dir);
}

/* [O11] The same shape with every row on ONE timestamp — the guard.
 *
 * Now the ts keys repeat, so the content rule cannot tell which value block
 * belongs to which ts block and there is nothing else left: the ordinals are in
 * spaces that do not meet and the keys are byte-identical.  The answer must be
 * a named error.  It must NOT be a fabricated zero for every row (the
 * `matched > 0` conjunct: measured without it, SELECT v rc=0 rows=3072 sum=0),
 * and it must NOT be block zero's values three times, which is the first-match
 * this whole series removes. */
static void test_marked_column_no_ts_ordinal_dup(void) {
    printf("\n[O11] the same, with the ts keys REPEATING: nothing on disk can "
           "decide, so it is refused\n");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_ordspace_o11_%d", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "o11", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    seed(db, "o11", 0, REP_ROWS, 1);
    tsdb_close(db); db = NULL;

    char part[4096];
    if (!only_part(dir, "o11", part, sizeof part)) {
        check(0, "O11 harness: no partition directory"); rm_rf(dir); return;
    }

    uint32_t ords[64]; size_t n = 0, marked = 0;
    check(idx_ords(part, "ts", ords, 64, &n, &marked) && n > 0 && marked == n,
          "O11 the ts side is FULLY marked (%zu of %zu entries)", marked, n);
    check(idx_shift_ords(part, "v", 1000),
          "O11 v's ordinals shifted clear of ts's");

    HARD(tsdb_open(dir, &db));
    qres_t cnt = q(db, "SELECT count(*) FROM o11");
    qres_t sv  = q(db, "SELECT v FROM o11");
    tsdb_close(db);

    check(cnt.rc == TSDB_OK && cnt.sum == REP_ROWS,
          "O11 count(*) = %lld (want %d) rc=%d — ts is intact",
          cnt.sum, REP_ROWS, cnt.rc);
    check(sv.rc != TSDB_OK,
          "O11 SELECT v is REFUSED (rc=%d rows=%lld sum=%lld) — never %d rows "
          "of fabricated zeros at rc=0, never another block's values",
          sv.rc, sv.rows, sv.sum, REP_ROWS);

    rm_rf(dir);
}

/* Every ts key here is DISTINCT — the timestamps increase by STEP — so nothing
 * about this partition is ambiguous: each ts block has exactly one key and the
 * column carries a block under it.  The only repetition is on the COLUMN side,
 * where it means "two entries describe the same rows", and either answers.
 *
 * Requiring the column's keys to be unique as well turned every ts block into a
 * HOLE and made the whole column read TSDB_ERR_CORRUPT permanently — a flush
 * only appends and compaction vetoes the shape, so nothing heals it.  Measured
 * with the column-side uniqueness conjunct restored: SELECT v rc=-4, rows=0. */
static void test_legacy_repeated_column_key(void) {
    printf("\n[O9] a repeated key on the COLUMN side is not ambiguity\n");

    char dir[256];
    snprintf(dir, sizeof dir, "/tmp/tsdb_ordspace_o9_%d", (int)getpid());
    rm_rf(dir);

    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    HARD(tsdb_create_table_ex2(db, "o9", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    seed(db, "o9", 0, REP_ROWS, 0);
    tsdb_close(db); db = NULL;

    char part[4096];
    if (!only_part(dir, "o9", part, sizeof part)) {
        check(0, "O9 harness: no partition directory"); rm_rf(dir); return;
    }

    /* An index an UNPATCHED binary wrote — no entry carries an ordinal, so the
     * legacy content rule is the only one available. */
    check(part_strip_ords(part), "O9 partition rewritten as legacy (no marks)");
    check(idx_dup_entry(part, "v", 1),
          "O9 v.idx entry 1 published twice (the orphan + WAL re-flush shape)");
    check(idx_count_of(part, "v") == REP_BLK + 1 &&
          idx_count_of(part, "ts") == REP_BLK,
          "O9 v.idx=%ld ts.idx=%ld (want %d / %d)",
          idx_count_of(part, "v"), idx_count_of(part, "ts"),
          REP_BLK + 1, REP_BLK);

    HARD(tsdb_open(dir, &db));
    qres_t cnt = q(db, "SELECT count(*) FROM o9");
    qres_t sv  = q(db, "SELECT v FROM o9");
    tsdb_close(db);

    check(cnt.rc == TSDB_OK && cnt.sum == REP_ROWS,
          "O9 count(*) = %lld (want %d) rc=%d", cnt.sum, REP_ROWS, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == REP_ROWS &&
          sv.sum == triangle(REP_ROWS),
          "O9 SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, REP_ROWS, triangle(REP_ROWS), sv.rc, sv.err);

    rm_rf(dir);
}

/* ========================================================================== */

int main(void) {
    tsdb_metrics_init();

    printf("=== test_block_ordinal_space: allocating and trusting the "
           "durable block ordinal ===\n");

    test_ordinal_space_monotone();
    test_applier_ordinal_collision();
    test_repair_push_into_legacy_index();
    test_two_senders_one_receiver();
    test_two_senders_one_timestamp_grid();
    test_local_flush_beside_a_push();
    test_legacy_ts_prefix_marked_short_column();
    test_mismatch_is_reported_as_mismatch();
    test_legacy_repeated_column_key();
    test_marked_column_matching_no_ts_ordinal();
    test_marked_column_no_ts_ordinal_dup();

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
