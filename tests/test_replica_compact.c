/* test_replica_compact.c — compaction on ONE side of a replication pair.
 *
 * The block ordinal is partition-local to the node that ISSUED it.  Every node
 * runs a compactor unconditionally (tsdb_node_main.c), compaction is node-local
 * and is never replicated, and nothing marks a partition as "arrived by
 * replication" — so a REPLICA compacts a replicated partition exactly as a
 * primary compacts its own, on its own schedule, while the primary keeps
 * flushing into the same partition and keeps pushing.
 *
 * That is the direction two earlier attempts at a cross-node ordinal both broke,
 * in opposite ways:
 *
 *   renumber DOWN  the primary's compaction re-issued low ordinals the replica
 *                  still held for other rows; the replica dropped every one at
 *                  rc == TSDB_OK and the partition stopped replicating.
 *   renumber UP    a REPLICA-side compaction consumed exactly the ordinals the
 *                  primary's flush was about to hand out, and the applier's
 *                  collision check then refused every push with
 *                  TSDB_ERR_CORRUPT — permanently, on every retry.
 *
 * The fix is not a third numbering rule.  Ingest TRANSLATES: a block arriving
 * over the wire is mapped into the receiver's own ordinal space, one mapping per
 * block group shared by every column of it, so the two spaces never meet and
 * each compactor only has to keep its own partition consistent.
 *
 * Asserted on query results, not on log lines: after the replica compacts and
 * the primary keeps writing, count(*) and SELECT v must both be EXACT.
 */
#include "../include/tsdb.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"
#include "../src/storage/compaction.h"
#include "../src/cluster/rawblock.h"

#include <dirent.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>

static int g_pass = 0, g_fail = 0;

static void check(int cond, const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    printf("  %s: ", cond ? "PASS" : "FAIL");
    vprintf(fmt, ap);
    printf("\n");
    va_end(ap);
    if (cond) g_pass++; else g_fail++;
}

#define HARD(expr) do { int _r = (expr); if (_r != TSDB_OK) { \
    printf("  FATAL: %s -> %d\n", #expr, _r); exit(1); } } while (0)

#define BP    1024
#define D0    1743465600000000000LL      /* 2025-04-01T00:00:00Z */
#define STEP  1000000LL

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
                    char fp[12288];
                    snprintf(fp, sizeof(fp), "%s/%s", pd, fe->d_name);
                    struct utimbuf ub; ub.actime = 1000000; ub.modtime = 1000000;
                    utime(fp, &ub);
                }
                closedir(dd);
            }
            struct utimbuf ub2; ub2.actime = 1000000; ub2.modtime = 1000000;
            utime(pd, &ub2);
        }
        closedir(tdd);
    }
    closedir(d);
}

static int only_part(const char *db_dir, const char *table,
                     char *out, size_t cap) {
    char td[4096]; snprintf(td, sizeof(td), "%s/%s", db_dir, table);
    DIR *d = opendir(td);
    if (!d) return 0;
    struct dirent *e; int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char pd[8192]; snprintf(pd, sizeof(pd), "%s/%s", td, e->d_name);
        struct stat st;
        if (stat(pd, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        snprintf(out, cap, "%s", pd);
        found = 1;
    }
    closedir(d);
    return found;
}

static long idx_count_of(const char *part, const char *col) {
    char p[8192]; snprintf(p, sizeof(p), "%s/%s.idx", part, col);
    uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(p, NULL, &cnt, &esz, NULL, NULL, NULL, NULL);
    if (hsz <= 0) return -1;
    return (long)cnt;
}

/* ---- block capture ------------------------------------------------------ */

typedef struct {
    char              table[64];
    uint32_t          part_day;
    uint16_t          col_idx;
    tsdb_block_meta_t meta;
    uint8_t          *bytes;
    size_t            bytes_len;
} cap_blk_t;

#define CAP_MAX 256
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

/* Through the real receive path: serialize -> parse -> apply, exactly as the
 * RPC handler does, so the wire encoding is exercised too. */
static int push_block(tsdb_db_t *dst, const cap_blk_t *e, uint32_t flags) {
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
    r.block_bytes_len = (uint32_t)e->bytes_len;
    r.block_bytes     = e->bytes;

    uint8_t *buf = NULL; size_t len = 0;
    if (tsdb_rawblock_serialize(&r, &buf, &len) != TSDB_OK) return TSDB_ERR_NOMEM;
    tsdb_rawblock_push_t p; memset(&p, 0, sizeof p);
    int rc = tsdb_rawblock_parse(buf, len, &p);
    if (rc == TSDB_OK) rc = tsdb_rawblock_apply_ex(dst, &p, flags);
    free(buf);
    return rc;
}

/* Ship every captured block: value columns first, ts last — the order the flush
 * hook itself uses, and the one the ts commit test requires. */
static int ship_all(tsdb_db_t *dst, const cap_ctx_t *ctx) {
    int refused = 0;
    for (int col = 1; col >= 0; col--)
        for (int j = 0; ; j++) {
            const cap_blk_t *b = pick(ctx, col, j);
            if (!b) break;
            if (push_block(dst, b, col == 0 ? TSDB_RB_VERIFY_TS : 0) != TSDB_OK)
                refused++;
        }
    return refused;
}

/* ---- queries ------------------------------------------------------------ */

typedef struct { int rc; long long rows, sum; char err[256]; } qres_t;

static qres_t q(tsdb_db_t *db, const char *sql) {
    qres_t r; memset(&r, 0, sizeof r);
    tsdb_result_t *res = NULL;
    r.rc = tsdb_query(db, sql, &res);
    if (r.rc != TSDB_OK) {
        const char *e = tsdb_last_error();
        snprintf(r.err, sizeof(r.err), "%s", e ? e : "");
        return r;
    }
    while (tsdb_result_next(res) == 1) { r.rows++; r.sum += tsdb_result_i64(res, 0); }
    tsdb_result_free(res);
    return r;
}

static tsdb_col_t COLS2[2] = { {"ts", TSDB_TYPE_TIMESTAMP}, {"v", TSDB_TYPE_INT64} };

static void seed(tsdb_db_t *db, const char *tbl, long long from, long long n) {
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, tbl, &t));
    tsdb_batch_t *b = NULL;
    HARD(tsdb_batch_begin(t, &b));
    for (long long i = 0; i < n; i++) {
        long long row = from + i;
        tsdb_batch_row_ts(b, D0 + row * STEP);
        tsdb_batch_row_i64(b, 1, row + 1);
        tsdb_batch_row_end(b);
    }
    HARD(tsdb_batch_commit(b));
    HARD(tsdb_db_flush_all(db));
}

/* Production threshold: min_blocks_to_compact == 0 means COMPACT_THRESHOLD_DEFAULT
 * (16).  No test tuning — the whole point is that this is what the cluster runs. */
static long long compact_once(tsdb_db_t *db, const char *dir) {
    backdate_all(dir);
    tsdb_compactor_opts_t opts; memset(&opts, 0, sizeof opts);
    opts.min_blocks_to_compact = 0;
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
 * [R1] the REPLICA compacts; the primary keeps flushing and keeps pushing
 * ========================================================================== */

#define R1_BLK    96
#define R1_ROWS   (R1_BLK * BP)          /* 98304 — well over the 16 threshold */
#define R1_MORE   (5 * BP)               /*  5120                              */
#define R1_TOTAL  (R1_ROWS + R1_MORE)

static void test_replica_side_compaction(void) {
    printf("\n[R1] the REPLICA compacts a replicated partition; replication "
           "continues\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_repcomp_r1s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_repcomp_r1d_%d", (int)getpid());
    rm_rf(sd); rm_rf(dd);

    cap_ctx_t ctx; memset(&ctx, 0, sizeof ctx);

    tsdb_db_t *src = NULL, *dst = NULL;
    HARD(tsdb_open(sd, &src));
    HARD(tsdb_create_table_ex2(src, "t", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, &ctx);
    seed(src, "t", 0, R1_ROWS);

    HARD(tsdb_open(dd, &dst));
    HARD(tsdb_create_table_local_ex(dst, "t", COLS2, 2, "ts", 0, BP, -1));
    check(ship_all(dst, &ctx) == 0, "R1 the replica accepted all %d seed blocks",
          ctx.n);

    char rpart[4096];
    if (!only_part(dd, "t", rpart, sizeof rpart)) {
        check(0, "R1 harness: no replica partition"); goto done;
    }
    long ts_pre = idx_count_of(rpart, "ts");
    check(ts_pre == R1_BLK, "R1 replica ts.idx holds %ld blocks (want %d)",
          ts_pre, R1_BLK);

    /* The replica's OWN background compactor.  Nothing here knows or cares that
     * the partition arrived by replication — nothing can. */
    long long merged = compact_once(dst, dd);
    check(merged >= 1, "R1 the replica compacted %lld partition(s)", merged);
    long ts_post = idx_count_of(rpart, "ts");
    check(ts_post > 0 && ts_post < ts_pre,
          "R1 replica ts.idx re-cut to %ld block(s) from %ld", ts_post, ts_pre);

    /* The primary — which has NOT compacted — keeps writing and keeps pushing.
     * Its next flush issues ordinals from ITS own space, which the replica's
     * compaction has just moved.  Round 2 measured every one of these refused
     * with TSDB_ERR_CORRUPT, forever. */
    cap_free(&ctx);
    seed(src, "t", R1_ROWS, R1_MORE);
    int refused = ship_all(dst, &ctx);
    check(refused == 0,
          "R1 the %d post-compaction pushes were all accepted (%d refused)",
          ctx.n, refused);

    /* Retries must not be needed, and must not change the answer either. */
    check(ship_all(dst, &ctx) == 0, "R1 a full re-push is idempotent");

    tsdb_close(dst); dst = NULL;
    HARD(tsdb_open(dd, &dst));
    qres_t cnt = q(dst, "SELECT count(*) FROM t");
    qres_t sv  = q(dst, "SELECT v FROM t");
    check(cnt.rc == TSDB_OK && cnt.sum == R1_TOTAL,
          "R1 replica count(*) = %lld (want %d) rc=%d",
          cnt.sum, R1_TOTAL, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == R1_TOTAL &&
          sv.sum == triangle(R1_TOTAL),
          "R1 replica SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, R1_TOTAL, triangle(R1_TOTAL), sv.rc, sv.err);

done:
    if (dst) tsdb_close(dst);
    if (src) tsdb_close(src);
    cap_free(&ctx);
    rm_rf(sd); rm_rf(dd);
}

/* ==========================================================================
 * [R2] BOTH sides compact, on their own schedules — the cluster steady state
 * ========================================================================== */

static void test_both_sides_compact(void) {
    printf("\n[R2] primary and replica each compact independently\n");

    char sd[256], dd[256];
    snprintf(sd, sizeof sd, "/tmp/tsdb_repcomp_r2s_%d", (int)getpid());
    snprintf(dd, sizeof dd, "/tmp/tsdb_repcomp_r2d_%d", (int)getpid());
    rm_rf(sd); rm_rf(dd);

    cap_ctx_t ctx; memset(&ctx, 0, sizeof ctx);

    tsdb_db_t *src = NULL, *dst = NULL;
    HARD(tsdb_open(sd, &src));
    HARD(tsdb_create_table_ex2(src, "t", COLS2, 2, "ts",
                               TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(src, cap_hook, &ctx);
    seed(src, "t", 0, R1_ROWS);
    HARD(tsdb_open(dd, &dst));
    HARD(tsdb_create_table_local_ex(dst, "t", COLS2, 2, "ts", 0, BP, -1));
    check(ship_all(dst, &ctx) == 0, "R2 replica accepted the seed");

    /* Replica first ... */
    check(compact_once(dst, dd) >= 1, "R2 replica compacted");
    /* ... then the primary, which renumbers ITS space downward-then-upward
     * relative to what the replica now holds. */
    check(compact_once(src, sd) >= 1, "R2 primary compacted");

    cap_free(&ctx);
    seed(src, "t", R1_ROWS, R1_MORE);
    check(ship_all(dst, &ctx) == 0, "R2 post-compaction pushes accepted");

    tsdb_close(dst); dst = NULL;
    HARD(tsdb_open(dd, &dst));
    qres_t cnt = q(dst, "SELECT count(*) FROM t");
    qres_t sv  = q(dst, "SELECT v FROM t");
    check(cnt.rc == TSDB_OK && cnt.sum == R1_TOTAL,
          "R2 replica count(*) = %lld (want %d) rc=%d",
          cnt.sum, R1_TOTAL, cnt.rc);
    check(sv.rc == TSDB_OK && sv.rows == R1_TOTAL &&
          sv.sum == triangle(R1_TOTAL),
          "R2 replica SELECT v rows=%lld sum=%lld (want %d / %lld) rc=%d err=%s",
          sv.rows, sv.sum, R1_TOTAL, triangle(R1_TOTAL), sv.rc, sv.err);

    /* The PRIMARY is untouched by any of it. */
    qres_t pc = q(src, "SELECT count(*) FROM t");
    qres_t pv = q(src, "SELECT v FROM t");
    check(pc.rc == TSDB_OK && pc.sum == R1_TOTAL &&
          pv.rc == TSDB_OK && pv.rows == R1_TOTAL &&
          pv.sum == triangle(R1_TOTAL),
          "R2 primary count(*)=%lld SELECT v rows=%lld sum=%lld (want %d / %d / %lld)",
          pc.sum, pv.rows, pv.sum, R1_TOTAL, R1_TOTAL, triangle(R1_TOTAL));

    if (dst) tsdb_close(dst);
    if (src) tsdb_close(src);
    cap_free(&ctx);
    rm_rf(sd); rm_rf(dd);
}

/* ========================================================================== */

int main(void) {
    printf("=== test_replica_compact: compaction on one side of a "
           "replication pair ===\n");

    test_replica_side_compaction();
    test_both_sides_compact();

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
