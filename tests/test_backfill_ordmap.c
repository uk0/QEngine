/* test_backfill_ordmap.c — the anti-entropy backfill is the ONE writer that
 * renumbers a partition's ordinal space DOWNWARD, and the remote-ordinal map
 * must not survive it.
 *
 * part.c states the invariant on part_idx_next_ord: "a partition's ordinal space
 * has to stay monotone for the partition's whole LIFETIME".  Every allocator
 * obeys it by going through tsdb_part_next_ordinal — the flush, compaction (which
 * floors its ord_base there) and the replication applier.
 *
 * tsdb_cluster_backfill_partition_from_result does not extend the space, it
 * REPLACES the rows.  Phase 1 flushes the peer's copy into an EMPTY scratch dir,
 * so part_ord_base starts at 0 and the rebuilt blocks are stamped 0..k-1 —
 * numbers this partition has already bound to other rows.  Phase 2 renames only
 * the .col/.idx pairs into place.
 *
 * <part>/.ordmap is neither rebuilt nor removed by that swap.  It maps each
 * sender's block GROUP onto a local ordinal, and after the rebuild those local
 * ordinals name a DIFFERENT peer's rows: local ordinal N is now the N-th block
 * of the pulled row set, not the group the sender was given it for.  The next
 * delivery of that group — a retry, a resync, an ordinary fan-out repeat — is
 * then translated onto a rebuilt block's ordinal, lands beside it as a second
 * claimant, and the read side (which takes the LAST claimant of an ordinal)
 * answers the rebuilt ts block with the SENDER's values.  Rows that a
 * successful, verified backfill had just put in place read back as another
 * node's data, rc == TSDB_OK.
 *
 * Deleting the map is the only coherent answer: no mapping onto the old
 * numbering survives a rebuild in any meaningful form, so the next delivery of
 * each group must allocate a fresh local ordinal and land as a new group — which
 * is what this path did before ordinals existed.  The cost is bounded and
 * visible: a re-delivered group can land twice (an over-count anti-entropy
 * measures and repairs), never on top of somebody else's rows.
 *
 * MEASURED with the removal reverted, both durability modes:
 *
 *     .ordmap survives the swap
 *     SELECT v after the sender re-delivers ONE value block:
 *         rc = -4 (data corrupt), 0 rows — the partition the backfill had just
 *         restored stops answering, because the stale mapping put the sender's
 *         block on top of a rebuilt block's ordinal
 */

#include "../include/tsdb.h"
#include "../include/tsdb_cluster.h"
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
#include <time.h>
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

#define HARD(rc) do { int _r = (rc); if (_r != TSDB_OK) {                     \
        fprintf(stderr, "harness broke at %s:%d rc=%d (%s)\n",                \
                __FILE__, __LINE__, _r, tsdb_errstr(_r));                     \
        printf("\n=== RESULTS: %d passed, %d failed (HARNESS ERROR) ===\n",   \
               g_pass, g_fail);                                               \
        exit(2); } } while (0)

#define DAY_NS  86400000000000LL
#define D0      ((1700000000000000000LL / DAY_NS) * DAY_NS)
#define STEP    1000000LL       /* 1 ms — 5120 rows stay inside one day       */
#define BP      1024            /* schema_create's low clamp                  */

#define B_ROWS   (2 * BP)       /* what the divergent replica already holds    */
#define S_FIRST  6000           /* the remote sender's row range (disjoint)    */
#define S_ROWS   BP
#define A_ROWS   (5 * BP)       /* the fuller peer                             */
#define SENDER_ID 0x5EDDEDULL   /* the sender's node id — what scopes its ords */

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

static tsdb_col_t COLS2[2] = { {"ts", TSDB_TYPE_TIMESTAMP},
                               {"v",  TSDB_TYPE_INT64} };

/* Rows [first, first+n) — ts = D0 + row*STEP, v = row+1. */
static void seed(tsdb_db_t *db, long long first, long long n) {
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    HARD(tsdb_batch_begin(t, &b));
    for (long long i = 0; i < n; i++) {
        long long row = first + i;
        tsdb_batch_row_ts(b, D0 + row * STEP);
        tsdb_batch_row_i64(b, 1, row + 1);
        tsdb_batch_row_end(b);
    }
    HARD(tsdb_batch_commit(b));
    HARD(tsdb_db_flush_all(db));
}

static tsdb_db_t *open_with_table(const char *dir) {
    tsdb_db_t *db = NULL;
    HARD(tsdb_open(dir, &db));
    int rc = tsdb_create_table_ex2(db, "t", COLS2, 2, "ts",
                                   TSDB_CREATE_PART_DAY, BP);
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) HARD(rc);
    return db;
}

/* ---- raw-block capture + replay (the real receive path) ------------------ */

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

static int push_block(tsdb_db_t *dst, const cap_blk_t *e, uint64_t issuer) {
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
    r.ord             = e->meta.ord;
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

/* ---- query helpers ------------------------------------------------------- */

typedef struct { int rc; long long rows, missing, outside; } scanres_t;

/* Every value of [lo, hi] must come back at least once. */
static scanres_t scan_ints(tsdb_db_t *db, long long lo, long long hi) {
    scanres_t s; memset(&s, 0, sizeof s);
    tsdb_result_t *r = NULL;
    s.rc = tsdb_query(db, "SELECT v FROM t", &r);
    if (s.rc != TSDB_OK || !r) return s;
    size_t span = (size_t)(hi - lo + 1);
    unsigned char *seen = calloc(span, 1);
    while (tsdb_result_next(r) > 0) {
        long long v = tsdb_result_i64(r, 0);
        s.rows++;
        if (v >= lo && v <= hi) { if (seen) seen[v - lo] = 1; }
        else s.outside++;
    }
    if (seen) for (size_t i = 0; i < span; i++) if (!seen[i]) s.missing++;
    free(seen);
    tsdb_result_free(r);
    return s;
}

static int file_exists(const char *p) {
    struct stat st;
    return stat(p, &st) == 0;
}

int main(void) {
    printf("=== test_backfill_ordmap: the backfill replaces the ordinal space, "
           "so the remote-ordinal map must not outlive it ===\n");

    char da[256], db_dir[256], ds[256];
    snprintf(da,     sizeof da,     "/tmp/tsdb_bfom_a_%d", (int)getpid());
    snprintf(db_dir, sizeof db_dir, "/tmp/tsdb_bfom_b_%d", (int)getpid());
    snprintf(ds,     sizeof ds,     "/tmp/tsdb_bfom_s_%d", (int)getpid());
    rm_rf(da); rm_rf(db_dir); rm_rf(ds);

    /* The partition every node is talking about. */
    char pname[16];
    {
        time_t secs = (time_t)(D0 / 1000000000LL);
        struct tm tm; gmtime_r(&secs, &tm);
        snprintf(pname, sizeof pname, "%04d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    }
    char live_part[4096], ordmap[4200];
    snprintf(live_part, sizeof live_part, "%s/t/%s", db_dir, pname);
    snprintf(ordmap,    sizeof ordmap,    "%s/.ordmap", live_part);

    /* A: the fuller peer, rows 0..A_ROWS-1.
     * B: the divergent replica, rows 0..B_ROWS-1.
     * S: a remote sender holding rows S_FIRST.. in the same day. */
    tsdb_db_t *A = open_with_table(da);
    tsdb_db_t *B = open_with_table(db_dir);
    seed(A, 0, A_ROWS);
    seed(B, 0, B_ROWS);

    cap_ctx_t cs; memset(&cs, 0, sizeof cs);
    tsdb_db_t *S = open_with_table(ds);
    tsdb_db_set_raw_block_hook(S, cap_hook, &cs);
    seed(S, S_FIRST, S_ROWS);
    check(cs.n == 2, "sender flushed one group (2 columns), captured %d", cs.n);

    /* The sender's group lands on B: a .ordmap is created and B's local
     * ordinal for the group is recorded against (SENDER_ID, its ordinal). */
    check(push_block(B, pick(&cs, 1, 0), SENDER_ID) == TSDB_OK &&
          push_block(B, pick(&cs, 0, 0), SENDER_ID) == TSDB_OK,
          "the sender's group landed on the replica");
    check(file_exists(ordmap), "the push created %s", ordmap);

    /* Anti-entropy pulls A's copy of the partition and rebuilds B's. */
    uint64_t written = 0;
    int brc;
    {
        char qtl[256];
        snprintf(qtl, sizeof qtl,
                 "SELECT * FROM t WHERE ts >= %lld AND ts <= %lld",
                 (long long)D0, (long long)(D0 + DAY_NS - 1));
        tsdb_result_t *res = NULL;
        HARD(tsdb_query(A, qtl, &res));
        brc = tsdb_cluster_backfill_partition_from_result(
                  B, "t", pname, res, (uint64_t)(B_ROWS + S_ROWS), &written);
        tsdb_result_free(res);
    }
    printf("  backfill rc=%d rows_written=%llu (want %d)\n",
           brc, (unsigned long long)written, A_ROWS);
    check(brc == TSDB_OK && written == (uint64_t)A_ROWS,
          "the backfill swapped A's %d rows in", A_ROWS);

    /* THE assertion.  The rebuilt blocks are stamped 0..k-1 out of an empty
     * scratch dir, so every ordinal the map hands out now names different rows. */
    check(!file_exists(ordmap),
          "the swap dropped %s — the map cannot outlive the numbering it maps to",
          ordmap);

    /* And behaviourally: the sender re-delivers the VALUE block of that group.
     * A push is one (column, block) — that granularity is the whole reason the
     * applier exists, and a repair push, a partial fan-out and a per-column
     * retry all produce exactly this.
     *
     * Translated through a surviving map it lands on a REBUILT block's ordinal,
     * where it becomes the LAST claimant and displaces the block the backfill
     * just restored.  The rows behind it are a different peer's, so the reader
     * refuses the whole column — the partition anti-entropy had just repaired
     * stops answering at all. */
    check(push_block(B, pick(&cs, 1, 0), SENDER_ID) == TSDB_OK,
          "the sender re-delivered its value block after the rebuild");

    tsdb_close(B); B = NULL;
    HARD(tsdb_open(db_dir, &B));
    scanres_t s = scan_ints(B, 1, A_ROWS);
    printf("  SELECT v: rc=%d rows=%lld missing=%lld outside=%lld\n",
           s.rc, s.rows, s.missing, s.outside);
    check(s.rc == TSDB_OK && s.rows == (long long)A_ROWS &&
          s.missing == 0 && s.outside == 0,
          "the restored partition still answers A's %d values and nothing else "
          "(rc=%d rows=%lld missing=%lld outside=%lld)",
          A_ROWS, s.rc, s.rows, s.missing, s.outside);

    tsdb_close(A); tsdb_close(B); tsdb_close(S);
    cap_free(&cs);
    rm_rf(da); rm_rf(db_dir); rm_rf(ds);

    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
