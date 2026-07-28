/* test_restore.c — backup set -> restore round trip, by VALUE.
 *
 * count(*) staying right while the values are wrong (or unreadable) is this
 * project's recurring failure, so every check here folds the actual cell
 * contents: the timestamps, the FLOAT64s, the INT64s and the SYMBOL strings.
 *
 * What it pins:
 *   1. Round trip preserves every value, including tag strings — a restore
 *      that shipped blocks without the dictionary reads every tag back as
 *      zero rows and count(*) never notices.
 *   2. Re-running a restore lands NOTHING and changes nothing.  This is the
 *      silent-duplication regression: the raw-block applier only compares a
 *      block against the LAST entry of the target index, so replaying a
 *      stream re-offers block 0 against a tail of block N-1, matches nothing,
 *      and appends every block a second time.  In <ts>.idx that is
 *      double-counted rows with no error anywhere.
 *   3. No column of any restored partition ever holds two blocks with the
 *      same (ts_min, count).  That pair is what exec.c pairs a value column
 *      to its ts block on, FIRST match — a second block carrying a key the
 *      column already has is unreachable by construction, so its presence
 *      means rows were duplicated or a value column silently shadowed.
 *   4. idx v4 max_seq survives; the header is never downgraded to v3.
 *   5. verify has two levels, they behave differently, and the report says
 *      which one ran: a flipped byte inside a block passes the index level
 *      and fails the deep level.
 *   6. One unreadable table does not abandon the others, and the
 *      .tsdb_restore marker stays behind when anything failed.
 */
#define _POSIX_C_SOURCE 200809L

#include "tsdb.h"
#include "tsdb_restore.h"
#include "tsdb_migrate.h"
#include "../src/storage/db.h"
#include "../src/storage/part.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FAIL(fmt, ...) do { fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
    __FILE__, __LINE__, ##__VA_ARGS__); exit(1); } while (0)
#define OK(rc) do { int _r=(rc); if (_r!=TSDB_OK) \
    FAIL("rc=%d (%s)", _r, tsdb_errstr(_r)); } while (0)

#define SRC   "/tmp/tsdb_test_restore_src"
#define BK    "/tmp/tsdb_test_restore_bk"
#define BK2   "/tmp/tsdb_test_restore_bk2"
#define DST   "/tmp/tsdb_test_restore_dst"
#define DST2  "/tmp/tsdb_test_restore_dst2"
#define SEQSRC "/tmp/tsdb_test_restore_seqsrc"
#define SEQBK  "/tmp/tsdb_test_restore_seqbk"
#define SEQDST "/tmp/tsdb_test_restore_seqdst"

/* Two partitions so the ts-last ordering has to hold across partitions, and
 * 20000 rows each so every column carries three blocks — one block per column
 * would hide the tail-only dedup defect entirely. */
#define DAY0    1700000000000000000LL
#define DAY_NS  86400000000000LL
#define PERDAY  20000

static const char *TAGS[4] = { "alpha", "bravo", "charlie", "delta" };

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

static double  val_f(int64_t i) { return (double)(i % 977) * 0.25 - 100.0; }
static int64_t val_n(int64_t i) { return (i * 7919) % 100003; }

/* Fold ts + every value column, including the tag STRING, into one number.
 * A dictionary that did not survive changes the string and therefore this. */
static void scan_values(tsdb_db_t *db, const char *table,
                        uint64_t *out_h, int64_t *out_n)
{
    char q[160];
    snprintf(q, sizeof(q), "SELECT ts, v, n, tag FROM %s", table);
    tsdb_result_t *r = NULL;
    uint64_t h = 1469598103934665603ULL;
    int64_t rows = 0;
    if (tsdb_query(db, q, &r) == TSDB_OK && r) {
        while (tsdb_result_next(r) > 0) {
            int64_t ts = tsdb_result_ts(r, 0);
            double  v  = tsdb_result_f64(r, 1);
            int64_t n  = tsdb_result_i64(r, 2);
            const char *tag = tsdb_result_sym(r, 3);
            uint64_t vb; memcpy(&vb, &v, sizeof(vb));
            h = (h ^ (uint64_t)ts) * 1099511628211ULL;
            h = (h ^ vb)           * 1099511628211ULL;
            h = (h ^ (uint64_t)n)  * 1099511628211ULL;
            for (const char *c = tag ? tag : "\x01"; *c; c++)
                h = (h ^ (uint64_t)(unsigned char)*c) * 1099511628211ULL;
            h = (h ^ 0x5aULL) * 1099511628211ULL;   /* field separator */
            rows++;
        }
        tsdb_result_free(r);
    }
    *out_h = h; *out_n = rows;
}

/* Independent of the projection path: per-tag row count AND value sum.  If the
 * dictionary is lost the WHERE matches nothing; if the rows pair to the wrong
 * blocks the sum moves even when the count does not. */
static void scan_per_tag(tsdb_db_t *db, const char *table,
                         int64_t *out_cnt, int64_t *out_sum)
{
    for (int i = 0; i < 4; i++) {
        char q[192];
        snprintf(q, sizeof(q),
                 "SELECT count(*), sum(n) FROM %s WHERE tag='%s'", table, TAGS[i]);
        tsdb_result_t *r = NULL;
        out_cnt[i] = -1; out_sum[i] = -1;
        if (tsdb_query(db, q, &r) == TSDB_OK && r && tsdb_result_next(r) > 0) {
            out_cnt[i] = tsdb_result_i64(r, 0);
            out_sum[i] = tsdb_result_i64(r, 1);
        }
        if (r) tsdb_result_free(r);
    }
}

/* ---- direct idx inspection ---------------------------------------------- */

static uint32_t rd_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* Count blocks in <part>/<col>.idx whose (ts_min, count) another block in the
 * SAME file already carries — the key exec.c pairs on, so a repeat is either
 * duplicated rows (ts) or an unreachable shadow (value column). */
static int idx_dup_keys(const char *idx_path, uint32_t *out_n) {
    if (out_n) *out_n = 0;
    uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(idx_path, NULL, &cnt, &esz, NULL, NULL, NULL, NULL);
    if (hsz <= 0 || cnt == 0 || esz < 24) return 0;
    FILE *f = fopen(idx_path, "rb");
    if (!f) return 0;

    int64_t  *tmin = calloc(cnt, sizeof(*tmin));
    uint32_t *cn   = calloc(cnt, sizeof(*cn));
    uint8_t  *e    = malloc(esz);
    int dup = 0;
    if (tmin && cn && e) {
        uint32_t got = 0;
        for (uint32_t i = 0; i < cnt; i++) {
            if (fseeko(f, (off_t)hsz + (off_t)i * esz, SEEK_SET) != 0) break;
            if (fread(e, 1, esz, f) != esz) break;
            uint64_t t = 0;
            for (int k = 7; k >= 0; k--) t = (t << 8) | e[16 + k];
            tmin[got] = (int64_t)t;
            cn[got]   = rd_u32(e + 12);
            got++;
        }
        for (uint32_t i = 0; i < got; i++)
            for (uint32_t j = i + 1; j < got; j++)
                if (tmin[i] == tmin[j] && cn[i] == cn[j]) dup++;
        if (out_n) *out_n = got;
    }
    free(tmin); free(cn); free(e);
    fclose(f);
    return dup;
}

static int is_part_name(const char *s) {
    size_t k = strlen(s);
    if (k != 8 && k != 10) return 0;
    for (size_t i = 0; i < k; i++) if (s[i] < '0' || s[i] > '9') return 0;
    return 1;
}

/* Walk every partition x column of `table` and total the duplicate keys. */
static int table_dup_keys(tsdb_db_t *db, const char *table, int *out_parts) {
    if (out_parts) *out_parts = 0;
    tsdb_table_t *th = NULL;
    if (tsdb_open_table(db, table, &th) != TSDB_OK) return -1;
    tsdb_table_internal_t *ti = tsdb_db_find_table(db, table);
    tsdb_schema_t *s = ti ? tsdb_tbl_schema(ti) : NULL;
    if (!s) return -1;

    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", tsdb_db_data_dir(db), table);
    DIR *d = opendir(tbl_dir);
    if (!d) return 0;
    int total = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!is_part_name(e->d_name)) continue;
        if (out_parts) (*out_parts)++;
        for (int ci = 0; ci < s->ncols; ci++) {
            char ip[4400];
            snprintf(ip, sizeof(ip), "%s/%s/%s.idx", tbl_dir, e->d_name, s->cols[ci].name);
            total += idx_dup_keys(ip, NULL);
        }
    }
    closedir(d);
    return total;
}

/* ts.idx (version, max_seq) of one partition of `table`. */
static int ts_idx_probe(tsdb_db_t *db, const char *table, const char *part,
                        uint16_t *ver, uint64_t *mseq)
{
    tsdb_table_t *th = NULL;
    if (tsdb_open_table(db, table, &th) != TSDB_OK) return -1;
    tsdb_table_internal_t *ti = tsdb_db_find_table(db, table);
    tsdb_schema_t *s = ti ? tsdb_tbl_schema(ti) : NULL;
    if (!s || s->ts_col_idx < 0) return -1;
    char ip[4400];
    snprintf(ip, sizeof(ip), "%s/%s/%s/%s.idx", tsdb_db_data_dir(db), table,
             part, s->cols[s->ts_col_idx].name);
    return tsdb_part_idx_probe(ip, ver, NULL, NULL, NULL, NULL, NULL, mseq);
}

/* First partition dir name of `table`, or "" if none. */
static void first_part(const char *data_dir, const char *table, char *out, size_t cap) {
    out[0] = '\0';
    char tbl_dir[4096];
    snprintf(tbl_dir, sizeof(tbl_dir), "%s/%s", data_dir, table);
    DIR *d = opendir(tbl_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!is_part_name(e->d_name)) continue;
        if (!out[0] || strcmp(e->d_name, out) < 0) snprintf(out, cap, "%s", e->d_name);
    }
    closedir(d);
}

/* ---- source ------------------------------------------------------------- */

static void build_source(void) {
    tsdb_db_t *db = NULL;
    OK(tsdb_open(SRC, &db));
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "v",   TSDB_TYPE_FLOAT64   },
        { "n",   TSDB_TYPE_INT64     },
        { "tag", TSDB_TYPE_SYMBOL    },
    };
    OK(tsdb_create_table(db, "m", cols, 4, "ts"));
    tsdb_table_t *t = NULL;
    OK(tsdb_open_table(db, "m", &t));

    for (int day = 0; day < 2; day++) {
        int64_t base = DAY0 + (int64_t)day * DAY_NS;
        int64_t i = 0;
        while (i < PERDAY) {
            int m = (PERDAY - i < 1000) ? (int)(PERDAY - i) : 1000;
            tsdb_batch_t *b = NULL;
            OK(tsdb_batch_begin(t, &b));
            for (int k = 0; k < m; k++) {
                int64_t g = (int64_t)day * PERDAY + i + k;
                OK(tsdb_batch_row_ts(b, base + (i + k) * 1000000LL));
                OK(tsdb_batch_row_f64(b, 1, val_f(g)));
                OK(tsdb_batch_row_i64(b, 2, val_n(g)));
                OK(tsdb_batch_row_sym(b, 3, TAGS[g % 4]));
                OK(tsdb_batch_row_end(b));
            }
            OK(tsdb_batch_commit(b));
            i += m;
        }
    }

    /* A second, tiny table: multi-table backup, and the one we corrupt later
     * to prove a bad stream does not abandon the good ones. */
    tsdb_col_t c2[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "v",   TSDB_TYPE_FLOAT64   },
        { "n",   TSDB_TYPE_INT64     },
        { "tag", TSDB_TYPE_SYMBOL    },
    };
    OK(tsdb_create_table(db, "m2", c2, 4, "ts"));
    tsdb_table_t *t2 = NULL;
    OK(tsdb_open_table(db, "m2", &t2));
    tsdb_batch_t *b2 = NULL;
    OK(tsdb_batch_begin(t2, &b2));
    for (int i = 0; i < 500; i++) {
        OK(tsdb_batch_row_ts(b2, DAY0 + i * 1000000LL));
        OK(tsdb_batch_row_f64(b2, 1, val_f(i)));
        OK(tsdb_batch_row_i64(b2, 2, val_n(i)));
        OK(tsdb_batch_row_sym(b2, 3, TAGS[i % 4]));
        OK(tsdb_batch_row_end(b2));
    }
    OK(tsdb_batch_commit(b2));

    OK(tsdb_db_flush_all(db));
    tsdb_close(db);
}

/* Flip one bit inside the first block payload of a value column so the block
 * still has the right size, offset and index entry — invisible to anything
 * that only reads the index. */
static int corrupt_one_block(const char *data_dir, const char *table,
                             const char *col)
{
    char part[32];
    first_part(data_dir, table, part, sizeof(part));
    if (!part[0]) return -1;
    char cp[4400];
    snprintf(cp, sizeof(cp), "%s/%s/%s/%s.col", data_dir, table, part, col);
    int fd = open(cp, O_RDWR);
    if (fd < 0) return -1;
    /* Byte 40: inside the first block's compressed payload (the 32-byte
     * BlockHeader precedes it). */
    uint8_t b = 0;
    if (pread(fd, &b, 1, 40) != 1) { close(fd); return -1; }
    b ^= 0xFF;
    if (pwrite(fd, &b, 1, 40) != 1) { close(fd); return -1; }
    fsync(fd); close(fd);
    return 0;
}

int main(void) {
    printf("=== test_restore ===\n");
    rm_rf(SRC); rm_rf(BK); rm_rf(BK2); rm_rf(DST); rm_rf(DST2);
    rm_rf(SEQSRC); rm_rf(SEQBK); rm_rf(SEQDST);

    build_source();

    uint64_t src_h = 0; int64_t src_rows = 0;
    int64_t src_tc[4], src_ts_[4];
    {
        tsdb_db_t *s = NULL;
        OK(tsdb_open(SRC, &s));
        scan_values(s, "m", &src_h, &src_rows);
        scan_per_tag(s, "m", src_tc, src_ts_);
        if (src_rows != 2 * PERDAY)
            FAIL("source has %lld rows, expected %d", (long long)src_rows, 2 * PERDAY);
        for (int i = 0; i < 4; i++)
            if (src_tc[i] <= 0) FAIL("source tag '%s' count=%lld", TAGS[i], (long long)src_tc[i]);

        /* ---- phase 1: backup ---- */
        tsdb_restore_report_t brp;
        OK(tsdb_backup_create(s, BK, &brp));
        printf("[backup] tables=%d failed=%d complete=%d\n",
               brp.ntables, brp.nfailed, brp.complete);
        if (brp.ntables < 2) FAIL("backup covered %d tables, expected >= 2", brp.ntables);
        if (brp.nfailed || !brp.complete) FAIL("backup reported incomplete");
        tsdb_restore_report_free(&brp);
        tsdb_close(s);
    }
    {
        char mp[4200];
        snprintf(mp, sizeof(mp), "%s/%s", BK, TSDB_BACKUP_MANIFEST);
        struct stat st;
        if (stat(mp, &st) != 0 || st.st_size <= 0) FAIL("manifest missing/empty");
        printf("[backup] manifest %lld bytes\n", (long long)st.st_size);
    }

    /* ---- phase 2: restore into an empty dir, compare VALUES ---- */
    {
        tsdb_db_t *d = NULL;
        OK(tsdb_open(DST, &d));
        if (tsdb_restore_in_progress(DST)) FAIL("marker present before restore");

        tsdb_restore_report_t rp;
        OK(tsdb_restore_run(d, BK, &rp));
        printf("[restore] tables=%d failed=%d complete=%d  m: seen=%llu landed=%llu skipped=%llu rows=%llu\n",
               rp.ntables, rp.nfailed, rp.complete,
               (unsigned long long)rp.tables[0].blocks_seen,
               (unsigned long long)rp.tables[0].blocks_landed,
               (unsigned long long)rp.tables[0].blocks_skipped,
               (unsigned long long)rp.tables[0].rows);
        if (!rp.complete) FAIL("first restore reported incomplete");
        uint64_t first_landed = 0, first_seen = 0;
        for (int i = 0; i < rp.ntables; i++) {
            first_landed += rp.tables[i].blocks_landed;
            first_seen   += rp.tables[i].blocks_seen;
        }
        if (first_landed == 0) FAIL("restore landed no blocks at all");
        tsdb_restore_report_free(&rp);

        if (tsdb_restore_in_progress(DST))
            FAIL("marker still present after a complete restore — absence is "
                 "the ONLY signal that a restore finished");

        uint64_t h = 0; int64_t n = 0;
        scan_values(d, "m", &h, &n);
        printf("[values]  src rows=%lld digest=%llu | dst rows=%lld digest=%llu\n",
               (long long)src_rows, (unsigned long long)src_h,
               (long long)n, (unsigned long long)h);
        if (n != src_rows) FAIL("restored row count %lld != %lld", (long long)n, (long long)src_rows);
        if (h != src_h)    FAIL("restored VALUES differ from the source");

        int64_t tc[4], tsum[4];
        scan_per_tag(d, "m", tc, tsum);
        for (int i = 0; i < 4; i++) {
            printf("[tag] %-8s src=(%lld,%lld) dst=(%lld,%lld)\n", TAGS[i],
                   (long long)src_tc[i], (long long)src_ts_[i],
                   (long long)tc[i], (long long)tsum[i]);
            if (tc[i] != src_tc[i] || tsum[i] != src_ts_[i])
                FAIL("tag '%s' did not survive: the SYMBOL dictionary is the "
                     "only thing that makes a tag block mean anything", TAGS[i]);
        }

        /* m2 landed too. */
        uint64_t h2 = 0; int64_t n2 = 0;
        scan_values(d, "m2", &h2, &n2);
        if (n2 != 500) FAIL("m2 restored %lld rows, expected 500", (long long)n2);

        /* ---- phase 3: re-run.  THE silent-duplication regression. ---- */
        tsdb_restore_report_t rp2;
        int rc2 = tsdb_restore_run(d, BK, &rp2);
        OK(rc2);
        uint64_t landed = 0, skipped = 0, seen = 0;
        for (int i = 0; i < rp2.ntables; i++) {
            landed  += rp2.tables[i].blocks_landed;
            skipped += rp2.tables[i].blocks_skipped;
            seen    += rp2.tables[i].blocks_seen;
        }
        printf("[replay]  seen=%llu landed=%llu skipped=%llu\n",
               (unsigned long long)seen, (unsigned long long)landed,
               (unsigned long long)skipped);
        tsdb_restore_report_free(&rp2);
        if (seen != first_seen) FAIL("replay saw %llu records, first run saw %llu",
                                     (unsigned long long)seen,
                                     (unsigned long long)first_seen);
        if (landed != 0)
            FAIL("re-running a finished restore landed %llu blocks — every one "
                 "of them is a duplicate the reader cannot pair",
                 (unsigned long long)landed);
        if (skipped != seen)
            FAIL("replay skipped %llu of %llu records",
                 (unsigned long long)skipped, (unsigned long long)seen);

        uint64_t h3 = 0; int64_t n3 = 0;
        scan_values(d, "m", &h3, &n3);
        if (n3 != src_rows)
            FAIL("re-restore changed the row count: %lld != %lld — duplicated blocks "
                 "in ts.idx are COUNTED, so this is silent data corruption",
                 (long long)n3, (long long)src_rows);
        if (h3 != src_h) FAIL("re-restore changed VALUES");

        /* ---- phase 3b: no column holds a repeated (ts_min,count) ---- */
        int parts = 0;
        int dups = table_dup_keys(d, "m", &parts);
        printf("[layout]  m: %d partitions, duplicate (ts_min,count) keys = %d\n",
               parts, dups);
        if (parts < 2) FAIL("expected >= 2 restored partitions, got %d", parts);
        if (dups != 0)
            FAIL("%d block(s) repeat a (ts_min,count) another block in the same "
                 "column already carries — that pair is exactly what exec.c "
                 "pairs on, FIRST match", dups);
        if (table_dup_keys(d, "m2", NULL) != 0) FAIL("m2 carries duplicate keys");

        /* ---- phase 5: verify, both levels, on a clean restore ---- */
        tsdb_restore_verify_t vi;
        int vrc = tsdb_restore_verify(d, BK, TSDB_RESTORE_VERIFY_INDEX, &vi);
        printf("[verify]  level=%s tables=%d failed=%d rc=%d\n",
               vi.level_name ? vi.level_name : "?", vi.ntables, vi.nfailed, vrc);
        if (strcmp(vi.level_name ? vi.level_name : "", "index") != 0)
            FAIL("index verify did not report which level ran");
        if (vrc != TSDB_OK || vi.nfailed) FAIL("index verify failed on a clean restore");
        tsdb_restore_verify_free(&vi);

        tsdb_restore_verify_t vd;
        vrc = tsdb_restore_verify(d, BK, TSDB_RESTORE_VERIFY_DEEP, &vd);
        printf("[verify]  level=%s tables=%d failed=%d rc=%d blocks_checked=%llu\n",
               vd.level_name ? vd.level_name : "?", vd.ntables, vd.nfailed, vrc,
               (unsigned long long)vd.tables[0].blocks_checked);
        if (strcmp(vd.level_name ? vd.level_name : "", "deep") != 0)
            FAIL("deep verify did not report which level ran");
        if (vrc != TSDB_OK || vd.nfailed) FAIL("deep verify failed on a clean restore");
        if (vd.tables[0].blocks_checked == 0) FAIL("deep verify checked no blocks");
        tsdb_restore_verify_free(&vd);

        tsdb_close(d);
    }

    /* ---- phase 4: idx v4 max_seq survives the round trip ----------------
     * The checkpoint is only stamped under deferred flush (commit_seq is 0
     * otherwise), so force that mode for this sub-case: the assertion has to
     * be real in BOTH modes of scripts/test-both-modes.sh, not vacuous in one. */
    {
        const char *prev = getenv("TSDB_WAL_ONLY_COMMIT");
        char saved[16] = {0};
        if (prev) snprintf(saved, sizeof(saved), "%s", prev);
        setenv("TSDB_WAL_ONLY_COMMIT", "1", 1);

        tsdb_db_t *a = NULL;
        OK(tsdb_open(SEQSRC, &a));
        tsdb_col_t c[] = {
            { "ts", TSDB_TYPE_TIMESTAMP }, { "v", TSDB_TYPE_FLOAT64 },
        };
        OK(tsdb_create_table(a, "s", c, 2, "ts"));
        tsdb_table_t *t = NULL;
        OK(tsdb_open_table(a, "s", &t));
        tsdb_batch_t *b = NULL;
        OK(tsdb_batch_begin(t, &b));
        for (int i = 0; i < 3000; i++) {
            OK(tsdb_batch_row_ts(b, DAY0 + i * 1000000LL));
            OK(tsdb_batch_row_f64(b, 1, val_f(i)));
            OK(tsdb_batch_row_end(b));
        }
        OK(tsdb_batch_commit(b));
        OK(tsdb_db_flush_all(a));

        char part[32];
        first_part(SEQSRC, "s", part, sizeof(part));
        uint16_t sver = 0; uint64_t sseq = 0;
        ts_idx_probe(a, "s", part, &sver, &sseq);
        OK(tsdb_backup_create(a, SEQBK, NULL));
        tsdb_close(a);

        tsdb_db_t *d = NULL;
        OK(tsdb_open(SEQDST, &d));
        OK(tsdb_restore_run(d, SEQBK, NULL));
        uint16_t dver = 0; uint64_t dseq = 0;
        ts_idx_probe(d, "s", part, &dver, &dseq);
        printf("[max_seq] source v%u seq=%llu -> restored v%u seq=%llu\n",
               sver, (unsigned long long)sseq, dver, (unsigned long long)dseq);
        if (sseq == 0)
            FAIL("source partition carries no WAL checkpoint — the sub-case "
                 "that proves max_seq survives is not actually exercising it");
        if (dseq != sseq)
            FAIL("restored max_seq %llu != source %llu: WAL replay would re-apply "
                 "records the partition already contains",
                 (unsigned long long)dseq, (unsigned long long)sseq);
        if (sver == 4 && dver != 4)
            FAIL("idx downgraded v4 -> v%u by the restore", dver);
        tsdb_close(d);

        if (saved[0]) setenv("TSDB_WAL_ONLY_COMMIT", saved, 1);
        else          unsetenv("TSDB_WAL_ONLY_COMMIT");
    }

    /* ---- phase 6: one bad stream does not abandon the good tables ---- */
    {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "cp -R '%s' '%s'", BK, BK2);
        if (system(cmd) != 0) FAIL("could not copy the backup set");
        char bad[4300];
        snprintf(bad, sizeof(bad), "%s/m2.tsm", BK2);
        if (truncate(bad, 10) != 0) FAIL("could not truncate m2.tsm");

        tsdb_db_t *d = NULL;
        OK(tsdb_open(DST2, &d));
        tsdb_restore_report_t rp;
        int rc = tsdb_restore_run(d, BK2, &rp);
        printf("[report]  rc=%d tables=%d failed=%d complete=%d\n",
               rc, rp.ntables, rp.nfailed, rp.complete);
        if (rc == TSDB_OK) FAIL("a restore whose stream is truncated must not return OK");
        if (rp.nfailed != 1) FAIL("expected exactly 1 failed table, got %d", rp.nfailed);
        if (rp.complete) FAIL("a partial restore must not report complete");

        int saw_m_ok = 0, saw_m2_bad = 0;
        for (int i = 0; i < rp.ntables; i++) {
            printf("[report]  %-4s rc=%d landed=%llu\n", rp.tables[i].table,
                   rp.tables[i].rc, (unsigned long long)rp.tables[i].blocks_landed);
            if (!strcmp(rp.tables[i].table, "m")  && rp.tables[i].rc == TSDB_OK) saw_m_ok = 1;
            if (!strcmp(rp.tables[i].table, "m2") && rp.tables[i].rc != TSDB_OK) saw_m2_bad = 1;
        }
        if (!saw_m_ok)   FAIL("the good table did not land — restore aborted on the first failure");
        if (!saw_m2_bad) FAIL("the truncated table was not reported");
        tsdb_restore_report_free(&rp);

        if (!tsdb_restore_in_progress(DST2))
            FAIL("marker was cleared despite a failed table — its absence must "
                 "mean the restore completed");

        uint64_t h = 0; int64_t n = 0;
        scan_values(d, "m", &h, &n);
        if (n != src_rows || h != src_h)
            FAIL("the surviving table's values are wrong: rows=%lld digest=%llu",
                 (long long)n, (unsigned long long)h);
        printf("[report]  surviving table 'm' still matches the source by value\n");
        tsdb_close(d);
    }

    /* ---- phase 7: a flipped byte passes INDEX and fails DEEP ---- */
    {
        if (corrupt_one_block(DST, "m", "v") != 0) FAIL("could not corrupt a block");
        tsdb_db_t *d = NULL;
        OK(tsdb_open(DST, &d));

        tsdb_restore_verify_t vi;
        int irc = tsdb_restore_verify(d, BK, TSDB_RESTORE_VERIFY_INDEX, &vi);
        printf("[rot]     index level: rc=%d failed=%d (level=%s)\n",
               irc, vi.nfailed, vi.level_name ? vi.level_name : "?");
        if (irc != TSDB_OK)
            FAIL("the index level is supposed to be blind to payload rot — if it "
                 "catches this, the two levels are not actually different");
        tsdb_restore_verify_free(&vi);

        tsdb_restore_verify_t vd;
        int drc = tsdb_restore_verify(d, BK, TSDB_RESTORE_VERIFY_DEEP, &vd);
        uint64_t bad = 0;
        for (int i = 0; i < vd.ntables; i++)
            bad += vd.tables[i].blocks_mismatched + vd.tables[i].blocks_missing;
        printf("[rot]     deep level:  rc=%d failed=%d bad_blocks=%llu (level=%s)\n",
               drc, vd.nfailed, (unsigned long long)bad,
               vd.level_name ? vd.level_name : "?");
        if (drc == TSDB_OK) FAIL("deep verify missed a flipped byte inside a block");
        if (bad == 0)       FAIL("deep verify failed without naming a bad block");
        if (strcmp(vd.level_name ? vd.level_name : "", "deep") != 0)
            FAIL("deep verify did not report which level ran");
        tsdb_restore_verify_free(&vd);
        tsdb_close(d);
    }

    rm_rf(SRC); rm_rf(BK); rm_rf(BK2); rm_rf(DST); rm_rf(DST2);
    rm_rf(SEQSRC); rm_rf(SEQBK); rm_rf(SEQDST);
    printf("\n=== test_restore PASSED ===\n");
    return 0;
}
