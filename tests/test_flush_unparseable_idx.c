/* test_flush_unparseable_idx.c — the LOCAL FLUSH sibling of the raw-block
 * applier's refuse-on-unparseable-idx rule (tests/test_idx_version_mongrel.c
 * case 3).
 *
 * col_writer_close publishes a column's manifest as a FULL REWRITE: header +
 * every OLD entry it read back + this flush's new ones, renamed over <col>.idx.
 * The old entries therefore have to be READ before they can be republished, and
 * every way of failing to read them collapsed into `old_count = 0`:
 *
 *   - the idx opens but read_idx_header_ex returns -1 (corrupt magic, or a
 *     version this binary does not know — a rolled-back binary beside a newer
 *     writer, or a future V5);
 *   - the header parses and declares N entries but the entry array is SHORT
 *     (a torn tail), so the fread comes up short;
 *   - fopen fails for a reason other than ENOENT (fd exhaustion, EACCES, EIO).
 *
 * In all three the flush renamed a manifest holding ONLY the new flush's
 * entries over the N-entry index, returned TSDB_OK, and the caller then cleared
 * the memtable and checkpointed the WAL.  The old .col bytes are still on disk
 * but nothing names them, and the next compaction rewrites the column from the
 * 1-entry manifest — at which point the loss is permanent.  Adversary-free,
 * silent, on the most common write path.
 *
 * INVARIANT: a writer may only rewrite a manifest it has read IN FULL.  When it
 * cannot, it must fail the flush and leave the partition byte-intact, so the
 * rows stay in the memtable + WAL and an operator can repair the index.
 *
 * Each case flushes a 3-block V4 partition, damages ts.idx, then flushes again:
 *   pre-fix : flush returns TSDB_OK and ts.idx is a 1-entry manifest
 *   post-fix: flush returns an error, ts.idx and ts.col are byte-unchanged, and
 *             repairing the damaged bytes brings all 3 entries back.
 */

#include "../include/tsdb.h"
#include "../src/storage/part.h"
#include "../src/storage/schema.h"
#include "../src/storage/memtable.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

static void put_u16le(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static uint32_t get_u32le(const uint8_t *p) {
    return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
}

#define IDX_ENTRY_SIZE 88u

#define NBLOCKS        3
#define ROWS_PER_BLOCK 100
#define NROWS          (NBLOCKS * ROWS_PER_BLOCK)

#define BASE_NS 1700000000000000000LL
#define STEP_NS 1000000LL

static tsdb_schema_t *make_schema(const char *dir, const char *table) {
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    tsdb_schema_t *s = NULL;
    int rc = tsdb_schema_create(dir, table, cols, 2, "ts", &s);
    if (rc != TSDB_OK) { fprintf(stderr, "schema_create failed rc=%d\n", rc); exit(1); }
    return s;
}

/* One flush == one on-disk block per column (rows << block_points).  max_seq>0
 * keeps the idx V4 so the damaged-header cases start from the same shape the
 * cluster runs. */
static int flush_one_block(tsdb_schema_t *s, const char *table,
                           uint64_t max_seq, int64_t base_ns) {
    tsdb_memtable_t *m = NULL;
    if (tsdb_memtable_new(s, &m) != TSDB_OK) { fprintf(stderr, "memtable_new failed\n"); exit(1); }
    for (int i = 0; i < ROWS_PER_BLOCK; i++) {
        if (tsdb_memtable_row_begin(m) != TSDB_OK) break;
        tsdb_memtable_row_ts(m, base_ns + (int64_t)i * STEP_NS);
        tsdb_memtable_row_f64(m, 1, (double)i);
        tsdb_memtable_row_end(m);
    }
    int rc = tsdb_part_flush_ex2(s, m, NULL, table, max_seq);
    tsdb_memtable_free(m);
    return rc;
}

static int find_part_dir(const char *table_dir, char *out, size_t cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(out, cap, "%s/%s", table_dir, e->d_name);
        found = 1;
        break;
    }
    closedir(d);
    return found;
}

static int read_file(const char *path, uint8_t **buf, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    *buf = malloc((size_t)sz ? (size_t)sz : 1);
    if (!*buf) { fclose(f); return -1; }
    *len = fread(*buf, 1, (size_t)sz, f);
    fclose(f);
    return 0;
}

static int write_file(const char *path, const uint8_t *buf, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    size_t w = fwrite(buf, 1, len, f);
    fclose(f);
    return (w == len) ? 0 : -1;
}

static off_t file_size(const char *p) {
    struct stat st;
    return (stat(p, &st) == 0) ? st.st_size : (off_t)-1;
}

/* kind 0: flip byte 0 of the idx magic       (unparseable header)
 * kind 1: stamp version 0x0005 at offset 8   (unparseable header)
 * kind 2: drop the last entry from the body  (header declares 3, body holds 2) */
static const char *kind_name(int kind) {
    return (kind == 0) ? "corrupt-magic"
         : (kind == 1) ? "future-version-V5"
                       : "truncated-entry-tail";
}

static void test_flush_refuses_unreadable_idx(int kind) {
    printf("\n[case %d] %s ts.idx: the next flush must not rewrite the manifest\n",
           kind, kind_name(kind));

    char root[128];
    snprintf(root, sizeof(root), "/tmp/tsdb_flush_unparse_%d", kind);
    const char *table = "lt_0";
    rmrf(root);
    mkdir(root, 0755);

    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/%s", root, table);
    mkdir(tdir, 0755);

    tsdb_schema_t *s = make_schema(tdir, table);

    for (int b = 0; b < NBLOCKS; b++) {
        int rc = flush_one_block(s, table, /*max_seq=*/7,
                                 BASE_NS + (int64_t)b * ROWS_PER_BLOCK * STEP_NS);
        if (rc != TSDB_OK) { fprintf(stderr, "setup flush %d rc=%d\n", b, rc); exit(1); }
    }
    CHECK(1, "flushed a 3-block V4 partition");

    char pdir[4096];
    if (!find_part_dir(tdir, pdir, sizeof(pdir))) {
        fprintf(stderr, "FAIL: no partition dir created\n"); g_fail++;
        tsdb_schema_free(s); return;
    }
    char ts_idx[4200], ts_col[4200];
    snprintf(ts_idx, sizeof(ts_idx), "%s/ts.idx", pdir);
    snprintf(ts_col, sizeof(ts_col), "%s/ts.col", pdir);

    uint8_t *orig = NULL; size_t olen = 0;
    CHECK(read_file(ts_idx, &orig, &olen) == 0, "read ts.idx pre-damage");
    CHECK(get_u32le(orig + 4) == NBLOCKS, "ts.idx declares 3 entries pre-damage");
    off_t col_before = file_size(ts_col);
    CHECK(col_before > 0, "ts.col present pre-damage");

    /* ── damage ─────────────────────────────────────────────────────────── */
    {
        uint8_t *dmg = malloc(olen);
        CHECK(dmg != NULL, "alloc damaged image");
        memcpy(dmg, orig, olen);
        size_t dlen = olen;
        if (kind == 0)       dmg[0] = (uint8_t)(orig[0] ^ 0xFF);
        else if (kind == 1)  put_u16le(dmg + 8, 5);
        else                 dlen = olen - IDX_ENTRY_SIZE;   /* lose one entry */
        CHECK(write_file(ts_idx, dmg, dlen) == 0, "wrote damaged ts.idx");
        free(dmg);
    }

    uint8_t *cor = NULL; size_t clen = 0;
    CHECK(read_file(ts_idx, &cor, &clen) == 0, "read damaged ts.idx back");

    /* ── the flush that used to eat the manifest ────────────────────────── */
    int rc = flush_one_block(s, table, /*max_seq=*/9,
                             BASE_NS + (int64_t)NBLOCKS * ROWS_PER_BLOCK * STEP_NS);
    printf("  flush over damaged ts.idx rc=%d (want != TSDB_OK=%d)\n", rc, TSDB_OK);
    CHECK(rc != TSDB_OK,
          "flush over an unreadable manifest FAILS [core regression]");

    /* ── nothing may have been written ──────────────────────────────────── */
    {
        uint8_t *now = NULL; size_t nlen = 0;
        CHECK(read_file(ts_idx, &now, &nlen) == 0, "read ts.idx post-flush");
        printf("  ts.idx: damaged=%zu bytes, post-flush=%zu bytes\n", clen, nlen);
        CHECK(nlen == clen && memcmp(now, cor, clen) == 0,
              "ts.idx byte-identical after the failed flush [core regression]");
        free(now);
        printf("  ts.col: before=%lld after=%lld\n",
               (long long)col_before, (long long)file_size(ts_col));
        CHECK(file_size(ts_col) == col_before,
              "ts.col byte-length unchanged (blocks rolled back)");
    }

    /* ── repair: the whole original manifest must still be recoverable ──── */
    {
        CHECK(write_file(ts_idx, orig, olen) == 0, "restored ts.idx bytes");
        uint16_t ver = 0; uint32_t cnt = 0, esz = 0;
        uint64_t tot = 0, mseq = 0;
        int64_t  fmn = 0, fmx = 0;
        int hsz = tsdb_part_idx_probe(ts_idx, &ver, &cnt, &esz,
                                      &tot, &fmn, &fmx, &mseq);
        printf("  repaired probe: hsz=%d ver=%u cnt=%u (want cnt=%d)\n",
               hsz, ver, cnt, NBLOCKS);
        CHECK(hsz > 0, "repaired ts.idx parses");
        CHECK(cnt == NBLOCKS,
              "repaired ts.idx still holds ALL original entries [core regression]");
    }

    /* ── and it still reads back every row ──────────────────────────────── */
    {
        tsdb_part_t *p = NULL;
        int orc = tsdb_part_open(s, pdir, &p);
        CHECK(orc == TSDB_OK, "reopen repaired partition");
        if (orc == TSDB_OK) {
            const tsdb_block_meta_t *blks = NULL; size_t nb = 0;
            CHECK(tsdb_part_col_blocks_ref(p, s->ts_col_idx, &blks, &nb) == TSDB_OK,
                  "enumerate ts blocks");
            uint64_t rows = 0;
            for (size_t i = 0; i < nb; i++) rows += blks[i].count;
            printf("  repaired scan: ts blocks=%zu rows=%llu (want %d)\n",
                   nb, (unsigned long long)rows, NROWS);
            CHECK(rows == NROWS, "repaired partition reads the full row count");
            tsdb_part_close(p);
        }
    }

    free(orig);
    free(cor);
    tsdb_schema_free(s);
    rmrf(root);
}

int main(void) {
    printf("=== test_flush_unparseable_idx ===\n");
    test_flush_refuses_unreadable_idx(0);   /* corrupt magic       */
    test_flush_refuses_unreadable_idx(1);   /* future version V5   */
    test_flush_refuses_unreadable_idx(2);   /* truncated entry tail */
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
