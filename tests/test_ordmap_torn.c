/* PROBE (reviewer, round 6): a torn/zero-length <part>/.ordmap makes every
 * raw-block push into that partition fail with TSDB_ERR_CORRUPT, forever.
 *
 * tsdb_part_ord_translate creates <part>/.ordmap with fopen("w+b") — which
 * truncates to zero — then writes header+entry, fflush, fsync.  A crash inside
 * that window leaves a 0-length (or short) file.  On the next push the reader
 * sees fopen succeed, the 16-byte header read fail, have_map stay 0, and the
 * function returns TSDB_ERR_CORRUPT.  tsdb_rawblock_apply_ex propagates it, so
 * the partition stops accepting replication.
 *
 * 9dab5a2 has no such file and no such state.
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
#include <sys/wait.h>
#include <unistd.h>

static int g_pass = 0, g_fail = 0;
static void check(int cond, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void check(int cond, const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    printf(cond ? "  PASS: " : "  FAIL: ");
    vprintf(fmt, ap); printf("\n"); va_end(ap);
    if (cond) g_pass++; else g_fail++;
}
#define HARD(rc) do { int _r = (rc); if (_r != TSDB_OK) {                   \
    fprintf(stderr, "harness broke %s:%d rc=%d\n", __FILE__, __LINE__, _r); \
    exit(2); } } while (0)

#define DAY_NS  86400000000000LL
#define D0      ((1700000000000000000LL / DAY_NS) * DAY_NS)
#define STEP    1000000LL
#define BP      1024
#define NROWS   (3 * BP)
#define SENDER  0x5EDDEDULL

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
        if (c->b[i].col_idx == (uint16_t)col) {
            if (seen == nth) return &c->b[i];
            seen++;
        }
    return NULL;
}
static int push_block(tsdb_db_t *dst, const cap_blk_t *e, uint64_t issuer) {
    if (!e) return TSDB_ERR_NOTFOUND;
    tsdb_rawblock_push_t r; memset(&r, 0, sizeof r);
    snprintf(r.table, sizeof(r.table), "%s", e->table);
    r.part_day = e->part_day; r.col_idx = e->col_idx;
    r.codec = e->meta.codec;  r.flags   = e->meta.flags;
    r.count = e->meta.count;  r.ts_min  = e->meta.ts_min;
    r.ts_max = e->meta.ts_max;
    r.stats_min = e->meta.stats_min;   r.stats_max = e->meta.stats_max;
    r.stats_sum = e->meta.stats_sum;   r.stats_first = e->meta.stats_first;
    r.stats_last = e->meta.stats_last; r.stats_flags = e->meta.stats_flags;
#ifdef TSDB_IDX_ORD_MARK
    r.ord = e->meta.ord;
    r.issuer = issuer;
#else
    (void)issuer;
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

static void seed(tsdb_db_t *db, long long n) {
    tsdb_table_t *t = NULL;
    HARD(tsdb_open_table(db, "t", &t));
    tsdb_batch_t *b = NULL;
    HARD(tsdb_batch_begin(t, &b));
    for (long long i = 0; i < n; i++) {
        tsdb_batch_row_ts(b, D0 + i * STEP);
        tsdb_batch_row_i64(b, 1, i + 1);
        tsdb_batch_row_end(b);
    }
    HARD(tsdb_batch_commit(b));
    HARD(tsdb_db_flush_all(db));
}

static int only_part(const char *root, char *out, size_t cap) {
    char tdir[4096]; snprintf(tdir, sizeof tdir, "%s/t", root);
    DIR *d = opendir(tdir); if (!d) return 0;
    struct dirent *e; int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (strlen(e->d_name) != 8) continue;
        snprintf(out, cap, "%s/%s", tdir, e->d_name); found = 1; break;
    }
    closedir(d); return found;
}

static long long q_rows(tsdb_db_t *db, const char *sql, int *out_rc) {
    tsdb_result_t *r = NULL;
    int rc = tsdb_query(db, sql, &r);
    *out_rc = rc;
    long long n = 0;
    if (rc == TSDB_OK && r) { while (tsdb_result_next(r) > 0) n++; }
    if (r) tsdb_result_free(r);
    return n;
}

int main(void) {
    printf("=== PROBE: torn/zero-length <part>/.ordmap ===\n");
#ifdef TSDB_IDX_ORD_MARK
    printf("(tree HAS the durable block ordinal)\n");
#else
    printf("(tree has NO durable block ordinal — 9dab5a2 baseline)\n");
#endif

    char src[256], dst[256];
    snprintf(src, sizeof src, "/tmp/tsdb_probe_om_s_%d", (int)getpid());
    snprintf(dst, sizeof dst, "/tmp/tsdb_probe_om_d_%d", (int)getpid());
    rm_rf(src); rm_rf(dst);

    /* --- source: 3 blocks, capture them ---------------------------------- */
    cap_ctx_t cap; memset(&cap, 0, sizeof cap);
    tsdb_db_t *s = NULL;
    HARD(tsdb_open(src, &s));
    HARD(tsdb_create_table_ex2(s, "t", COLS2, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    tsdb_db_set_raw_block_hook(s, cap_hook, &cap);
    seed(s, NROWS);
    tsdb_close(s);

    printf("  captured %d raw blocks\n", cap.n);
    for (int i = 0; i < cap.n; i++)
        printf("    [%d] col=%u count=%u ts_min=%lld\n", i,
               (unsigned)cap.b[i].col_idx, cap.b[i].meta.count,
               (long long)cap.b[i].meta.ts_min);

    /* --- destination: land group 0 (v then ts) --------------------------- */
    tsdb_db_t *d = NULL;
    HARD(tsdb_open(dst, &d));
    HARD(tsdb_create_table_ex2(d, "t", COLS2, 2, "ts", TSDB_CREATE_PART_DAY, BP));
    int rc0v = push_block(d, pick(&cap, 1, 0), SENDER);
    int rc0t = push_block(d, pick(&cap, 0, 0), SENDER);
    check(rc0v == TSDB_OK && rc0t == TSDB_OK,
          "group 0 lands (v rc=%d ts rc=%d)", rc0v, rc0t);
    tsdb_close(d);

    char part[4096];
    if (!only_part(dst, part, sizeof part)) {
        check(0, "no partition dir"); goto done;
    }
    char om[4200]; snprintf(om, sizeof om, "%s/.ordmap", part);
    struct stat st;
    int have_map = (stat(om, &st) == 0);
    printf("  .ordmap present=%d size=%lld\n", have_map,
           have_map ? (long long)st.st_size : -1LL);

    /* --- the crash window: the file exists but its bytes never landed ----- */
    if (have_map) {
        FILE *f = fopen(om, "wb");   /* truncate to 0 — exactly what a crash
                                        between fopen("w+b") and fsync leaves */
        if (f) fclose(f);
        check(stat(om, &st) == 0 && st.st_size == 0,
              "'.ordmap' truncated to 0 bytes (the post-crash state)");
    }

    /* --- every later push into this partition ---------------------------- */
    HARD(tsdb_open(dst, &d));
    { tsdb_table_t *tt = NULL; HARD(tsdb_open_table(d, "t", &tt)); }
    int rc1v = push_block(d, pick(&cap, 1, 1), SENDER);
    int rc1t = push_block(d, pick(&cap, 0, 1), SENDER);
    int rc1v2 = push_block(d, pick(&cap, 1, 1), SENDER);   /* the sender retries */
    int qrc = 0;
    long long rows = q_rows(d, "SELECT v FROM t", &qrc);
    tsdb_close(d);

    printf("  after the tear: push v rc=%d, push ts rc=%d, RETRY v rc=%d\n",
           rc1v, rc1t, rc1v2);
    printf("  SELECT v -> rc=%d rows=%lld\n", qrc, rows);

    check(rc1v == TSDB_OK && rc1t == TSDB_OK && rc1v2 == TSDB_OK,
          "replication into this partition still works (rc %d/%d/%d)",
          rc1v, rc1t, rc1v2);

    /* ---- [2] a crash BETWEEN the map's two writes ------------------------
     *
     * The append path writes the header (bumping the high-water) and then the
     * entry, and the order is the whole point: a crash between them can only
     * leave a high-water AHEAD of the entries, never behind.  Ahead costs one
     * skipped ordinal.  Behind means an entry the high-water does not cover, so
     * a LATER group is handed the SAME local ordinal — and on a
     * duplicate-timestamp run those two groups' ts blocks are byte-identical,
     * the applier calls the second a re-delivery, and it is dropped at
     * rc == TSDB_OK.  Silent loss of acked rows.
     *
     * So the invariant is: every entry's local ordinal is strictly below the
     * header's next_local.  A real child process is killed at the fault point
     * to produce the state, rather than the state being written by hand — the
     * question is what the WRITER can leave behind, and only the writer can
     * answer it. */
    {
        char dst2[256];
        snprintf(dst2, sizeof dst2, "/tmp/tsdb_probe_om_c_%d", (int)getpid());
        rm_rf(dst2);

        tsdb_db_t *c = NULL;
        HARD(tsdb_open(dst2, &c));
        HARD(tsdb_create_table_ex2(c, "t", COLS2, 2, "ts",
                                   TSDB_CREATE_PART_DAY, BP));
        (void)push_block(c, pick(&cap, 1, 0), SENDER);   /* creates the map */
        (void)push_block(c, pick(&cap, 0, 0), SENDER);
        tsdb_close(c);

        fflush(NULL);
        pid_t pid = fork();
        if (pid == 0) {
            setenv("TSDB_TEST_CRASH_ORDMAP_MID", "1", 1);
            tsdb_db_t *k = NULL;
            if (tsdb_open(dst2, &k) != TSDB_OK) _exit(90);
            tsdb_table_t *kt = NULL;
            if (tsdb_open_table(k, "t", &kt) != TSDB_OK) _exit(91);
            (void)push_block(k, pick(&cap, 1, 1), SENDER);  /* APPEND path */
            _exit(92);                                      /* not reached */
        }
        int wst = 0; (void)waitpid(pid, &wst, 0);
        int died = WIFEXITED(wst) && WEXITSTATUS(wst) == 71;
        check(died, "the child died between the map's two writes (exit %d)",
              WIFEXITED(wst) ? WEXITSTATUS(wst) : -WTERMSIG(wst));

        char part2[4096], om2[4200];
        if (died && only_part(dst2, part2, sizeof part2)) {
            snprintf(om2, sizeof om2, "%s/.ordmap", part2);
            FILE *m = fopen(om2, "rb");
            unsigned hw2 = 0, nent = 0, worst = 0; int cover = 1;
            if (m) {
                unsigned char h2[16];
                if (fread(h2, 1, 16, m) == 16) {
                    hw2 = (unsigned)h2[8] | ((unsigned)h2[9] << 8) |
                          ((unsigned)h2[10] << 16) | ((unsigned)h2[11] << 24);
                    unsigned char e2[40];
                    while (fread(e2, 1, 40, m) == 40) {
                        unsigned lo = (unsigned)e2[12] | ((unsigned)e2[13] << 8) |
                                      ((unsigned)e2[14] << 16) |
                                      ((unsigned)e2[15] << 24);
                        nent++;
                        if (lo >= hw2) { cover = 0; if (lo > worst) worst = lo; }
                    }
                }
                fclose(m);
            }
            printf("  after the kill: .ordmap next_local=%u entries=%u\n",
                   hw2, nent);
            check(cover,
                  "the high-water covers every entry (next_local=%u but an "
                  "entry holds local=%u — a later group would be handed that "
                  "same ordinal and silently dropped)", hw2, worst);

            /* And the partition must still take the retry and read correctly. */
            tsdb_db_t *r = NULL;
            HARD(tsdb_open(dst2, &r));
            { tsdb_table_t *rt = NULL; HARD(tsdb_open_table(r, "t", &rt)); }
            int rv = push_block(r, pick(&cap, 1, 1), SENDER);
            int rt2 = push_block(r, pick(&cap, 0, 1), SENDER);
            int qrc2 = 0;
            long long n2 = q_rows(r, "SELECT v FROM t", &qrc2);
            tsdb_close(r);
            printf("  retry after the kill: v rc=%d ts rc=%d; SELECT v rc=%d "
                   "rows=%lld\n", rv, rt2, qrc2, n2);
            check(rv == TSDB_OK && rt2 == TSDB_OK,
                  "the retry lands after the kill (v rc=%d ts rc=%d)", rv, rt2);
            check(qrc2 == TSDB_OK && n2 == 2 * (long long)BP,
                  "both groups are readable after the kill (rc=%d rows=%lld, "
                  "want %lld)", qrc2, n2, 2 * (long long)BP);
        }
        rm_rf(dst2);
    }

done:
    rm_rf(src); rm_rf(dst);
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
