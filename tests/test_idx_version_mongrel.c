/* test_idx_version_mongrel.c — regression for the partition-index VERSION
 * INCONSISTENCY that makes a cluster SELECT return 0 rows for on-disk data.
 *
 * SERVER-FORENSIC ROOT (measured on a replica partition that read 0):
 *   - Two writers disagree on the idx header SIZE.
 *       * flush path (part.c write_idx_header): 48-byte V4 header when
 *         max_seq>0, byte-identical 40-byte V3 header when max_seq==0;
 *         entries always follow at the returned header size (consistent).
 *       * raw-block replication (rawblock.c rawblk_write_idx_header):
 *         HARDCODED 40-byte V3 header, in-place (fopen "wb") non-atomic.
 *   - When a partition is touched by BOTH paths (a V4 flush, then a
 *     raw-block append/rewrite), the resulting <ts>.idx can end up with a
 *     header SIZE, version byte, and entry offsets that are mutually
 *     inconsistent: the file body holds entries at the V4 offset (48) while
 *     the version byte reads V3 → the reader (read_idx_header_ex) picks
 *     header size 40 from the version byte, reads entry0 at offset 40 (which
 *     lands inside the V4 max_seq field + the head of the real entry0) →
 *     a GARBAGE block offset → the per-block filter discards every block
 *     (end > mapped size) → 0 blocks → 0 rows for the WHOLE partition.
 *
 * This test reproduces that exact on-disk byte state deterministically:
 *   1. Flush a partition with max_seq>0  → ts.idx is V4 (48-byte header,
 *      version byte 4, entries at offset 48).
 *   2. Overwrite bytes [0..40) with a V3 header (version byte 3) that keeps
 *      the same block count — leaving the entries stranded at offset 48
 *      while the version byte now says V3.  This is byte-for-byte the
 *      "header-size != entry offset" mongrel the forensic measured.
 *   3. Open + enumerate blocks.
 *
 * Pre-fix : tsdb_part_col_blocks returns 0 blocks (the 0-row bug).
 * Post-fix: the reader detects entry0 is out-of-bounds for the version's
 *           header size, retries the other known header size (40 vs 48),
 *           finds valid in-bounds entries, and returns the true block count.
 *
 * A second case drives the real two-writer path (V4 flush + raw-block apply
 * on the same partition) to assert the consistency fix keeps a V4 partition
 * readable after a replication append.
 *
 * A third case covers the OTHER probe answer: an idx that EXISTS but cannot
 * be parsed (corrupt magic, or a version this binary does not know).  The
 * probe reports that as -1, distinct from 0 == absent; the applier used to
 * collapse the two, treat the partition as empty, and rename a fresh ONE-entry
 * manifest over the N-entry index it merely failed to read.  The apply must
 * instead fail with TSDB_ERR_CORRUPT and write NOTHING, so the manifest is
 * still intact once the corruption is repaired.
 */

#include "../include/tsdb.h"
#include "../src/storage/part.h"
#include "../src/storage/schema.h"
#include "../src/storage/memtable.h"
#include "../src/storage/db.h"
#include "../src/cluster/rawblock.h"

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

/* LE helpers (match part.c on-disk layout). */
static void put_u32le(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static void put_u16le(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static uint16_t get_u16le(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }
static uint32_t get_u32le(const uint8_t *p) {
    return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
}

#define IDX_MAGIC        0x31584449u
#define IDX_HDR_V3       40u
#define IDX_HDR_V4       48u
#define IDX_ENTRY_SIZE   88u

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

/* The memtable caps at block_points rows (one on-disk block per flush).  To
 * get a MULTI-block partition — the cluster's mongrel had 66 blocks — fill
 * one block, flush, clear, and repeat NBLOCKS times into the same partition.
 * Each flush appends a block; passing max_seq>0 keeps the idx V4 throughout. */
#define BLOCK_ROWS 8192      /* == TSDB_BLOCK_POINTS (memtable cap) */
#define NBLOCKS    3
#define NROWS      (BLOCK_ROWS * NBLOCKS)

/* Fill `m` with one block of rows starting at `base_ns`; returns rows added. */
static int fill_one_block(tsdb_memtable_t *m, int64_t base_ns) {
    int added = 0;
    for (int i = 0; i < BLOCK_ROWS; i++) {
        if (tsdb_memtable_row_begin(m) != TSDB_OK) break;   /* TSDB_ERR_FULL */
        tsdb_memtable_row_ts(m, base_ns + (int64_t)i * 1000000LL);
        tsdb_memtable_row_f64(m, 1, (double)i);
        tsdb_memtable_row_end(m);
        added++;
    }
    return added;
}

/* Flush NBLOCKS blocks into one partition via repeated flush+clear of a single
 * memtable, every flush stamping max_seq>0 so the idx is V4. */
static void flush_multiblock_v4(tsdb_schema_t *s, const char *table,
                                uint64_t max_seq) {
    tsdb_memtable_t *m = NULL;
    if (tsdb_memtable_new(s, &m) != TSDB_OK) { fprintf(stderr, "memtable_new failed\n"); exit(1); }
    int64_t base = 1700000000000000000LL;   /* same day for all blocks */
    for (int b = 0; b < NBLOCKS; b++) {
        int added = fill_one_block(m, base + (int64_t)b * BLOCK_ROWS * 1000000LL);
        if (added != BLOCK_ROWS) { fprintf(stderr, "fill short: %d\n", added); exit(1); }
        int rc = tsdb_part_flush_ex2(s, m, NULL, table, max_seq);
        if (rc != TSDB_OK) { fprintf(stderr, "flush block %d rc=%d\n", b, rc); exit(1); }
        tsdb_memtable_clear(m);
    }
    tsdb_memtable_free(m);
}

/* Find "<table_dir>/<YYYYMMDD>" partition dir. Returns 1 on success. */
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

/* Read full file into malloc'd buffer. */
static int read_file(const char *path, uint8_t **buf, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    *buf = malloc((size_t)sz);
    if (!*buf) { fclose(f); return -1; }
    *len = fread(*buf, 1, (size_t)sz, f);
    fclose(f);
    return 0;
}

/* ─── Case 1: precise mongrel (V4-sized body, V3 version byte) ─────────────── */
static void test_mongrel_idx(void) {
    printf("\n[case 1] V4-sized idx body with a V3 version byte → 0-row scan\n");

    const char *root  = "/tmp/tsdb_idx_mongrel";
    const char *table = "lt_0";
    rmrf(root);
    mkdir(root, 0755);

    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/%s", root, table);
    mkdir(tdir, 0755);

    tsdb_schema_t *s = make_schema(tdir, table);

    /* Flush NBLOCKS blocks with a non-zero WAL checkpoint → V4 idx. */
    flush_multiblock_v4(s, table, /*max_seq=*/42);
    CHECK(1, "flushed multi-block V4 partition (max_seq>0)");

    char pdir[4096];
    if (!find_part_dir(tdir, pdir, sizeof(pdir))) {
        fprintf(stderr, "FAIL: no partition dir created\n"); g_fail++;
        tsdb_schema_free(s); return;
    }

    char ts_idx[4096];
    snprintf(ts_idx, sizeof(ts_idx), "%s/ts.idx", pdir);

    /* Sanity: the flush wrote a V4 header (48 bytes, version byte 4). */
    uint8_t *idx = NULL; size_t ilen = 0;
    CHECK(read_file(ts_idx, &idx, &ilen) == 0, "ts.idx readable");
    uint16_t ver_before = get_u16le(idx + 8);
    uint32_t cnt        = get_u32le(idx + 4);
    CHECK(ver_before == 4, "flush produced V4 ts.idx (version byte == 4)");
    CHECK(cnt >= 2, "ts.idx has multiple blocks");
    /* V4 layout: file size == 48 + cnt*88. */
    CHECK(ilen == IDX_HDR_V4 + (size_t)cnt * IDX_ENTRY_SIZE,
          "ts.idx is V4-sized (48 + cnt*88)");
    printf("  ts.idx: version=4 count=%u size=%zu (V4 body, entries @48)\n",
           cnt, ilen);

    /* Confirm the intact V4 partition reads the full count first. */
    {
        tsdb_part_t *p = NULL;
        CHECK(tsdb_part_open(s, pdir, &p) == TSDB_OK, "open intact V4 partition");
        tsdb_block_meta_t *blks = NULL; size_t nb = 0;
        CHECK(tsdb_part_col_blocks(p, 0, &blks, &nb) == TSDB_OK, "enumerate ts blocks (intact)");
        uint64_t rows = 0;
        for (size_t i = 0; i < nb; i++) rows += blks[i].count;
        printf("  intact V4: ts blocks=%zu rows=%llu\n", nb, (unsigned long long)rows);
        CHECK(rows == NROWS, "intact V4 partition reads full row count");
        free(blks);
        tsdb_part_close(p);
    }

    /* ── Corrupt into the measured mongrel ──────────────────────────────────
     * Overwrite bytes [0..40) with a V3 header (version byte 3) that keeps the
     * SAME block count, but DO NOT move the entry body — it stays at the V4
     * offset (48).  The file is now V4-sized with a V3 version byte: exactly
     * the "header-size != entry offset" state the server forensic measured
     * (version byte flips 3/4 across reads from torn two-writer rewrites;
     * here we freeze it at 3 deterministically). */
    {
        FILE *f = fopen(ts_idx, "r+b");
        CHECK(f != NULL, "reopen ts.idx for in-place corruption");
        uint8_t v3[IDX_HDR_V3];
        memset(v3, 0, sizeof(v3));
        put_u32le(v3 + 0,  IDX_MAGIC);
        put_u32le(v3 + 4,  cnt);            /* same block count */
        put_u16le(v3 + 8,  3);              /* version = 3  (the flip) */
        put_u16le(v3 + 10, 0);
        /* total_rows @12..19, file_ts_min @20..27, file_ts_max @28..35:
         * copy from the existing header so only the header SIZE/version is
         * inconsistent, not the zone map. */
        memcpy(v3 + 12, idx + 12, 8);       /* total_rows */
        memcpy(v3 + 20, idx + 20, 8);       /* file_ts_min */
        memcpy(v3 + 28, idx + 28, 8);       /* file_ts_max */
        put_u16le(v3 + 36, (uint16_t)IDX_ENTRY_SIZE);  /* entry_size */
        put_u16le(v3 + 38, 0);
        fseek(f, 0, SEEK_SET);
        size_t w = fwrite(v3, 1, sizeof(v3), f);
        fclose(f);
        CHECK(w == sizeof(v3), "wrote V3 header over V4 header (body untouched)");
    }
    free(idx);

    /* Re-confirm the on-disk state is the mongrel: V3 version byte, V4 size. */
    {
        uint8_t *chk = NULL; size_t clen = 0;
        CHECK(read_file(ts_idx, &chk, &clen) == 0, "re-read corrupted ts.idx");
        CHECK(get_u16le(chk + 8) == 3, "corrupted ts.idx version byte == 3");
        CHECK(clen == IDX_HDR_V4 + (size_t)cnt * IDX_ENTRY_SIZE,
              "corrupted ts.idx still V4-sized (entries stranded @48)");
        free(chk);
    }

    /* ── The bug: scan the mongrel partition ────────────────────────────────
     * Pre-fix the reader trusts the V3 version byte (header size 40), reads
     * entry0 at offset 40 → a garbage offset → every block filtered → 0
     * blocks → 0 rows.  Post-fix the reader recovers by trying the V4 header
     * size, finds in-bounds entries, and returns the full count. */
    {
        tsdb_part_t *p = NULL;
        int orc = tsdb_part_open(s, pdir, &p);
        CHECK(orc == TSDB_OK, "open mongrel partition (no error)");
        tsdb_block_meta_t *blks = NULL; size_t nb = 0;
        int brc = tsdb_part_col_blocks(p, 0, &blks, &nb);
        CHECK(brc == TSDB_OK, "enumerate ts blocks (mongrel)");
        uint64_t rows = 0;
        for (size_t i = 0; i < nb; i++) rows += blks[i].count;
        printf("  mongrel scan: ts blocks=%zu rows=%llu (want %d)\n",
               nb, (unsigned long long)rows, NROWS);

        /* THE REGRESSION ASSERT: must NOT collapse to 0 blocks / 0 rows. */
        CHECK(nb > 0, "mongrel partition recovers blocks (NOT 0) [core regression]");
        CHECK(rows == NROWS, "mongrel partition recovers FULL row count [core regression]");

        free(blks);
        tsdb_part_close(p);
    }

    tsdb_schema_free(s);
    rmrf(root);
}

/* ─── Case 2: real two-writer path — V4 flush then raw-block apply ─────────── */
static void test_v4_then_rawblock_apply(void) {
    printf("\n[case 2] V4 flush + raw-block apply on same partition stays readable\n");

    const char *root  = "/tmp/tsdb_idx_v4rawblk";
    const char *table = "lt_1";
    rmrf(root);
    mkdir(root, 0755);

    /* A real DB so we can drive tsdb_rawblock_apply against a flushed
     * partition's column. */
    tsdb_db_t *db = NULL;
    CHECK(tsdb_open(root, &db) == TSDB_OK, "open db");
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    CHECK(tsdb_create_table_local(db, table, cols, 2, "ts") == TSDB_OK, "create table");

    /* Flush a V4 partition directly via the schema/memtable so max_seq>0. */
    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/%s", root, table);
    tsdb_schema_t *s = make_schema(tdir, table);
    flush_multiblock_v4(s, table, /*max_seq=*/99);
    CHECK(1, "flush V4 partition (max_seq=99)");

    char pdir[4096];
    CHECK(find_part_dir(tdir, pdir, sizeof(pdir)), "locate partition dir");

    /* Snapshot ts.idx version before the raw-block append. */
    char ts_idx[4096];
    snprintf(ts_idx, sizeof(ts_idx), "%s/ts.idx", pdir);
    uint8_t *idx = NULL; size_t ilen = 0;
    CHECK(read_file(ts_idx, &idx, &ilen) == 0, "read ts.idx pre-append");
    uint16_t ver_before = get_u16le(idx + 8);
    uint32_t cnt_before = get_u32le(idx + 4);
    int64_t  fmax = 0;
    for (int i = 7; i >= 0; i--) fmax = (fmax << 8) | idx[28 + i];
    CHECK(ver_before == 4, "partition is V4 before raw-block append");
    free(idx);

    /* Drive a raw-block append onto the ts column (col_idx 0) — the
     * replication path that, pre-fix, rewrites the header as hardcoded V3. */
    {
        /* Synthesise one extra ts block appended after the existing data. */
        tsdb_rawblock_push_t r;
        memset(&r, 0, sizeof(r));
        strncpy(r.table, table, 63);
        /* Reuse the same part_day as the flushed partition. */
        const char *day = strrchr(pdir, '/');
        r.part_day = (uint32_t)strtoul(day ? day + 1 : "0", NULL, 10);
        r.col_idx  = 0;                 /* ts column */
        r.codec    = 0;                 /* CODEC_NONE — a tiny opaque block */
        r.flags    = 0;
        r.count    = 4;
        r.ts_min   = fmax + 1000000LL;
        r.ts_max   = fmax + 4000000LL;
        static uint8_t blk[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
        r.block_bytes     = blk;
        r.block_bytes_len = sizeof(blk);
        int arc = tsdb_rawblock_apply(db, &r);
        CHECK(arc == TSDB_OK, "raw-block apply onto V4 ts column");
    }

    /* The consistency fix: after a raw-block append the idx MUST remain
     * self-consistent (header size matches the version byte and the entry
     * offset), and a reader MUST still read every block.  Whether the writer
     * preserves V4 or rewrites as a consistent V3, the file must not be a
     * mongrel and the scan must not drop to 0. */
    {
        uint8_t *idx2 = NULL; size_t ilen2 = 0;
        CHECK(read_file(ts_idx, &idx2, &ilen2) == 0, "read ts.idx post-append");
        uint16_t ver = get_u16le(idx2 + 8);
        uint32_t c2  = get_u32le(idx2 + 4);
        size_t   hsz = (ver == 4) ? IDX_HDR_V4 : IDX_HDR_V3;
        size_t   esz = get_u16le(idx2 + 36);
        if (esz == 0) esz = IDX_ENTRY_SIZE;
        printf("  post-append ts.idx: version=%u count=%u size=%zu\n",
               ver, c2, ilen2);
        CHECK(c2 == cnt_before + 1, "raw-block append added exactly one block");
        /* Self-consistency: size == header + count*entry_size. */
        CHECK(ilen2 == hsz + (size_t)c2 * esz,
              "post-append ts.idx is self-consistent (size == hdr + cnt*esz)");
        free(idx2);
    }

    /* Reopen the partition and confirm the scan reads all blocks. */
    {
        tsdb_part_t *p = NULL;
        CHECK(tsdb_part_open(s, pdir, &p) == TSDB_OK, "reopen partition post-append");
        tsdb_block_meta_t *blks = NULL; size_t nb = 0;
        CHECK(tsdb_part_col_blocks(p, 0, &blks, &nb) == TSDB_OK, "enumerate ts blocks post-append");
        uint64_t rows = 0;
        for (size_t i = 0; i < nb; i++) rows += blks[i].count;
        printf("  post-append scan: ts blocks=%zu rows=%llu\n",
               nb, (unsigned long long)rows);
        CHECK(nb == (size_t)(cnt_before + 1), "post-append scan sees all blocks (NOT 0)");
        CHECK(rows == NROWS + 4, "post-append scan reads original + appended rows");
        free(blks);
        tsdb_part_close(p);
    }

    tsdb_schema_free(s);
    tsdb_close(db);
    rmrf(root);
}

/* ─── Case 3: unparseable idx must FAIL the apply, not be overwritten ────────
 *
 * kind == 0: flip byte 0 of the idx magic (corrupt magic).
 * kind == 1: stamp version 0x0005 at offset 8 (a future version — the state a
 *            rolled-back binary sees beside a newer writer).
 *
 * Both make tsdb_part_idx_probe return -1 ("exists but unparseable"), NOT 0
 * ("absent").  The apply must refuse with TSDB_ERR_CORRUPT and leave ts.idx
 * and ts.col byte-untouched, so restoring the two corrupted bytes brings the
 * whole 3-entry manifest back.  Pre-fix the apply returned TSDB_OK and had
 * already renamed a 1-entry manifest over it — unrepairable. */
static void test_unparseable_idx_refuses_apply(int kind) {
    printf("\n[case 3%c] %s idx must fail the apply, untouched on disk\n",
           kind ? 'b' : 'a', kind ? "future-version (V5)" : "corrupt-magic");

    const char *root  = kind ? "/tmp/tsdb_idx_unparse_v5"
                             : "/tmp/tsdb_idx_unparse_magic";
    const char *table = "lt_2";
    rmrf(root);
    mkdir(root, 0755);

    tsdb_db_t *db = NULL;
    CHECK(tsdb_open(root, &db) == TSDB_OK, "open db");
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    CHECK(tsdb_create_table_local(db, table, cols, 2, "ts") == TSDB_OK, "create table");

    char tdir[4096];
    snprintf(tdir, sizeof(tdir), "%s/%s", root, table);
    tsdb_schema_t *s = make_schema(tdir, table);
    flush_multiblock_v4(s, table, /*max_seq=*/7);
    CHECK(1, "flushed 3-block V4 partition");

    char pdir[4096];
    CHECK(find_part_dir(tdir, pdir, sizeof(pdir)), "locate partition dir");
    char ts_idx[4096], ts_col[4096];
    snprintf(ts_idx, sizeof(ts_idx), "%s/ts.idx", pdir);
    snprintf(ts_col, sizeof(ts_col), "%s/ts.col", pdir);

    struct stat idx_before, col_before;
    CHECK(stat(ts_idx, &idx_before) == 0, "stat ts.idx pre-corruption");
    CHECK(stat(ts_col, &col_before) == 0, "stat ts.col pre-corruption");

    /* Snapshot the bytes we are about to corrupt (for the restore) and the
     * zone-map max so the pushed block sorts after the existing data. */
    uint8_t *idx = NULL; size_t ilen = 0;
    CHECK(read_file(ts_idx, &idx, &ilen) == 0, "read ts.idx pre-corruption");
    CHECK(get_u32le(idx + 4) == NBLOCKS, "ts.idx holds 3 entries pre-corruption");
    int64_t fmax = 0;
    for (int i = 7; i >= 0; i--) fmax = (fmax << 8) | idx[28 + i];
    uint8_t orig0 = idx[0], orig8 = idx[8], orig9 = idx[9];
    free(idx);

    {
        FILE *f = fopen(ts_idx, "r+b");
        CHECK(f != NULL, "reopen ts.idx for corruption");
        if (kind) {
            uint8_t v[2]; put_u16le(v, 5);
            fseek(f, 8, SEEK_SET);
            CHECK(fwrite(v, 1, 2, f) == 2, "stamped version 0x0005 at offset 8");
        } else {
            uint8_t b = (uint8_t)(orig0 ^ 0xFF);
            fseek(f, 0, SEEK_SET);
            CHECK(fwrite(&b, 1, 1, f) == 1, "flipped byte 0 of idx magic");
        }
        fclose(f);
    }

    /* One new block through the applier — the path that used to overwrite. */
    {
        tsdb_rawblock_push_t r;
        memset(&r, 0, sizeof(r));
        strncpy(r.table, table, 63);
        const char *day = strrchr(pdir, '/');
        r.part_day = (uint32_t)strtoul(day ? day + 1 : "0", NULL, 10);
        r.col_idx  = 0;                 /* ts column */
        r.codec    = 0;
        r.flags    = 0;
        r.count    = 4;
        r.ts_min   = fmax + 1000000LL;
        r.ts_max   = fmax + 4000000LL;
        static uint8_t blk[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
        r.block_bytes     = blk;
        r.block_bytes_len = sizeof(blk);
        int arc = tsdb_rawblock_apply_ex(db, &r, 0);
        printf("  apply rc=%d (want TSDB_ERR_CORRUPT=%d)\n", arc, TSDB_ERR_CORRUPT);
        CHECK(arc == TSDB_ERR_CORRUPT,
              "apply against unparseable idx returns TSDB_ERR_CORRUPT [core regression]");
    }

    /* Nothing may have been written: neither the manifest nor the .col. */
    {
        struct stat idx_after, col_after;
        CHECK(stat(ts_idx, &idx_after) == 0, "stat ts.idx post-apply");
        printf("  ts.idx size: before=%lld after=%lld\n",
               (long long)idx_before.st_size, (long long)idx_after.st_size);
        CHECK(idx_after.st_size == idx_before.st_size,
              "ts.idx size unchanged (manifest NOT overwritten) [core regression]");
        CHECK(stat(ts_col, &col_after) == 0, "stat ts.col post-apply");
        CHECK(col_after.st_size == col_before.st_size,
              "ts.col size unchanged (no orphan block appended)");
    }

    /* Repair the two corrupted bytes: the FULL original manifest must still
     * be there — that is what "nothing was written" buys the operator. */
    {
        FILE *f = fopen(ts_idx, "r+b");
        CHECK(f != NULL, "reopen ts.idx to restore");
        if (kind) {
            uint8_t v[2] = { orig8, orig9 };
            fseek(f, 8, SEEK_SET);
            CHECK(fwrite(v, 1, 2, f) == 2, "restored original version bytes");
        } else {
            fseek(f, 0, SEEK_SET);
            CHECK(fwrite(&orig0, 1, 1, f) == 1, "restored original magic byte");
        }
        fclose(f);
    }
    {
        uint16_t ver = 0; uint32_t cnt = 0, esz = 0;
        uint64_t tot = 0, mseq = 0;
        int64_t  fmn = 0, fmx = 0;
        int hsz = tsdb_part_idx_probe(ts_idx, &ver, &cnt, &esz,
                                      &tot, &fmn, &fmx, &mseq);
        printf("  restored probe: hsz=%d ver=%u cnt=%u (want cnt=%d)\n",
               hsz, ver, cnt, NBLOCKS);
        CHECK(hsz > 0, "restored ts.idx parses again");
        CHECK(cnt == NBLOCKS,
              "restored ts.idx still holds ALL original entries [core regression]");
    }

    tsdb_schema_free(s);
    tsdb_close(db);
    rmrf(root);
}

int main(void) {
    printf("=== test_idx_version_mongrel ===\n");
    test_mongrel_idx();
    test_v4_then_rawblock_apply();
    test_unparseable_idx_refuses_apply(0);   /* corrupt magic */
    test_unparseable_idx_refuses_apply(1);   /* future version 0x0005 */
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
