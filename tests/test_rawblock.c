/* test_rawblock.c — Unit tests for raw-block replication and Merkle diff.
 *
 * Tests:
 *   1. Single-machine flush → serialize → apply to "replica" DB directory →
 *      compare .col / .idx byte-for-byte.
 *   2. Merkle build: identical data → identical root hash.
 *   3. Merkle diff: modify one block in remote → diff contains exactly
 *      that block index.
 *   4. Throughput benchmark: 10000-row flush via raw-block apply vs
 *      in-process encode+flush (simulates row-level encode overhead).
 */

#include "../include/tsdb.h"
#include "../src/storage/part.h"
#include "../src/storage/schema.h"
#include "../src/storage/memtable.h"
#include "../src/storage/db.h"
#include "../src/cluster/rawblock.h"
#include "../src/cluster/merkle.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <dirent.h>
#include <errno.h>

/* ---- Helpers -------------------------------------------------------------- */

#define ASSERT(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", \
                    #cond, __FILE__, __LINE__); \
            exit(1); \
        } \
    } while(0)

#define ASSERT_EQ(a, b) \
    do { \
        if ((a) != (b)) { \
            fprintf(stderr, "ASSERT_EQ FAILED: %s (%ld) != %s (%ld)  [%s:%d]\n", \
                    #a, (long)(a), #b, (long)(b), __FILE__, __LINE__); \
            exit(1); \
        } \
    } while(0)

/* Simple nanosecond clock. */
static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Recursively remove a directory tree (simple, single-level + one extra). */
static void rmtree(const char *path) {
    DIR *d = opendir(path);
    if (!d) return;
    struct dirent *e;
    char buf[4096];
    while ((e = readdir(d))) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
        snprintf(buf, sizeof(buf), "%s/%s", path, e->d_name);
        struct stat st;
        if (stat(buf, &st) == 0 && S_ISDIR(st.st_mode)) rmtree(buf);
        else remove(buf);
    }
    closedir(d);
    rmdir(path);
}

static int file_bytes(const char *path, uint8_t **buf, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    *buf = malloc((size_t)sz + 1);
    if (!*buf) { fclose(f); return -1; }
    *len = fread(*buf, 1, (size_t)sz, f);
    fclose(f);
    return 0;
}

/* ---- Schema helpers ------------------------------------------------------- */

static tsdb_schema_t *make_schema(const char *dir, const char *table_name) {
    tsdb_col_t cols[] = {
        { "ts",    TSDB_TYPE_TIMESTAMP },
        { "value", TSDB_TYPE_FLOAT64   },
        { "tag",   TSDB_TYPE_INT64     },
    };
    tsdb_schema_t *s = NULL;
    int rc = tsdb_schema_create(dir, table_name, cols, 3, "ts", &s);
    ASSERT(rc == TSDB_OK);
    return s;
}

static tsdb_memtable_t *make_memtable(tsdb_schema_t *s, int nrows) {
    tsdb_memtable_t *m = NULL;
    int rc = tsdb_memtable_new(s, &m);
    ASSERT(rc == TSDB_OK);
    /* Use a fixed timestamp base. */
    int64_t base_ns = 1700000000000000000LL; /* ~2023-11-14 */
    for (int i = 0; i < nrows; i++) {
        tsdb_memtable_row_begin(m);
        int64_t ts = base_ns + (int64_t)i * 1000000LL; /* 1ms steps */
        tsdb_memtable_row_ts(m, ts);
        /* value col (idx 1) */
        double v = (double)i * 1.1;
        int rc2 = TSDB_OK;
        (void)rc2;
        tsdb_memtable_row_f64(m, 1, v);
        tsdb_memtable_row_i64(m, 2, (int64_t)i);
        tsdb_memtable_row_end(m);
    }
    return m;
}

/* ---- Mock raw-block hook + collected pushes ------------------------------- */

typedef struct {
    char          table[64];
    uint32_t      part_day;
    uint16_t      col_idx;
    tsdb_block_meta_t meta;
    uint8_t      *bytes;
    size_t        bytes_len;
} captured_block_t;

#define MAX_CAPTURED 128

typedef struct {
    captured_block_t blocks[MAX_CAPTURED];
    int              n;
} capture_ctx_t;

static int mock_raw_block_hook(void *ud, tsdb_db_t *db,
                                const char *table_name,
                                uint32_t part_day,
                                uint16_t col_idx,
                                const tsdb_block_meta_t *meta,
                                const uint8_t *block_bytes,
                                size_t block_bytes_len)
{
    (void)db;
    capture_ctx_t *ctx = (capture_ctx_t *)ud;
    if (ctx->n >= MAX_CAPTURED) return TSDB_OK;

    captured_block_t *c = &ctx->blocks[ctx->n++];
    strncpy(c->table, table_name, 63);
    c->part_day  = part_day;
    c->col_idx   = col_idx;
    c->meta      = *meta;
    c->bytes     = malloc(block_bytes_len);
    if (!c->bytes) return TSDB_ERR_NOMEM;
    memcpy(c->bytes, block_bytes, block_bytes_len);
    c->bytes_len = block_bytes_len;
    return TSDB_OK;
}

static void capture_ctx_free(capture_ctx_t *ctx) {
    for (int i = 0; i < ctx->n; i++) free(ctx->blocks[i].bytes);
}

/* ---- Test 1: flush → apply, compare .col/.idx ----------------------------- */

static void test_flush_apply(void) {
    printf("[test_rawblock] test 1: flush → apply → file compare\n");

    const char *src_dir = "/tmp/tsdb_rawblk_src";
    const char *dst_dir = "/tmp/tsdb_rawblk_dst";
    const char *table   = "sensor";

    rmtree(src_dir); rmtree(dst_dir);
    mkdir(src_dir, 0755); mkdir(dst_dir, 0755);

    /* Source DB. */
    tsdb_db_t *src_db = NULL;
    ASSERT(tsdb_open(src_dir, &src_db) == TSDB_OK);

    tsdb_col_t cols[] = {
        { "ts",    TSDB_TYPE_TIMESTAMP },
        { "value", TSDB_TYPE_FLOAT64   },
        { "tag",   TSDB_TYPE_INT64     },
    };
    ASSERT(tsdb_create_table(src_db, table, cols, 3, "ts") == TSDB_OK);

    /* Destination DB (mimic replica — same schema). */
    tsdb_db_t *dst_db = NULL;
    ASSERT(tsdb_open(dst_dir, &dst_db) == TSDB_OK);
    ASSERT(tsdb_create_table_local(dst_db, table, cols, 3, "ts") == TSDB_OK);

    /* Register raw-block capture hook on src_db. */
    capture_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    tsdb_db_set_raw_block_hook(src_db, mock_raw_block_hook, &ctx);

    /* Write 100 rows (fills one block per column). */
    tsdb_table_t *tbl = NULL;
    ASSERT(tsdb_open_table(src_db, table, &tbl) == TSDB_OK);
    tsdb_batch_t *b = NULL;
    ASSERT(tsdb_batch_begin(tbl, &b) == TSDB_OK);

    int64_t base_ns = 1700000000000000000LL;
    for (int i = 0; i < 100; i++) {
        tsdb_batch_row_ts(b, base_ns + (int64_t)i * 1000000LL);
        tsdb_batch_row_f64(b, 1, (double)i);
        tsdb_batch_row_i64(b, 2, (int64_t)i);
        tsdb_batch_row_end(b);
    }
    ASSERT(tsdb_batch_commit(b) == TSDB_OK);

    /* Hook should have captured 3 blocks (ts, value, tag). */
    ASSERT(ctx.n >= 3);
    printf("    captured %d blocks from flush\n", ctx.n);

    /* Apply each captured block to dst_db. */
    for (int i = 0; i < ctx.n; i++) {
        tsdb_rawblock_push_t r;
        memset(&r, 0, sizeof(r));
        strncpy(r.table, ctx.blocks[i].table, 63);
        r.part_day        = ctx.blocks[i].part_day;
        r.col_idx         = ctx.blocks[i].col_idx;
        r.codec           = ctx.blocks[i].meta.codec;
        r.flags           = ctx.blocks[i].meta.flags;
        r.count           = ctx.blocks[i].meta.count;
        r.ts_min          = ctx.blocks[i].meta.ts_min;
        r.ts_max          = ctx.blocks[i].meta.ts_max;
        r.block_bytes_len = (uint32_t)ctx.blocks[i].bytes_len;
        r.block_bytes     = ctx.blocks[i].bytes;

        int rc = tsdb_rawblock_apply(dst_db, &r);
        ASSERT_EQ(rc, TSDB_OK);
    }

    /* Compare .col and .idx files between src and dst for the first partition. */
    /* Find the partition day directory (e.g. 20231114). */
    {
        /* Get part_day from first captured block. */
        uint32_t pday = ctx.blocks[0].part_day;
        char src_part[256], dst_part[256];
        snprintf(src_part, sizeof(src_part), "%s/%s/%08u", src_dir, table, pday);
        snprintf(dst_part, sizeof(dst_part), "%s/%s/%08u", dst_dir, table, pday);

        const char *col_names[] = {"ts", "value", "tag"};
        for (int c = 0; c < 3; c++) {
            char src_col[256], dst_col[256];
            snprintf(src_col, sizeof(src_col), "%s/%s.col", src_part, col_names[c]);
            snprintf(dst_col, sizeof(dst_col), "%s/%s.col", dst_part, col_names[c]);

            uint8_t *sb = NULL, *db_b = NULL;
            size_t   sl  = 0,    dl  = 0;
            int r1 = file_bytes(src_col, &sb, &sl);
            int r2 = file_bytes(dst_col, &db_b, &dl);
            ASSERT(r1 == 0);
            ASSERT(r2 == 0);
            ASSERT(sl > 0);
            ASSERT_EQ(sl, dl);
            ASSERT(memcmp(sb, db_b, sl) == 0);
            printf("    col=%s src=%zu dst=%zu bytes — match\n",
                   col_names[c], sl, dl);
            free(sb); free(db_b);

            /* .idx comparison. */
            snprintf(src_col, sizeof(src_col), "%s/%s.idx", src_part, col_names[c]);
            snprintf(dst_col, sizeof(dst_col), "%s/%s.idx", dst_part, col_names[c]);
            r1 = file_bytes(src_col, &sb, &sl);
            r2 = file_bytes(dst_col, &db_b, &dl);
            ASSERT(r1 == 0);
            ASSERT(r2 == 0);
            ASSERT_EQ(sl, dl);
            ASSERT(memcmp(sb, db_b, sl) == 0);
            printf("    idx=%s src=%zu dst=%zu bytes — match\n",
                   col_names[c], sl, dl);
            free(sb); free(db_b);
        }
    }

    /* Idempotency: apply again — must not change file sizes. */
    {
        uint32_t pday = ctx.blocks[0].part_day;
        char dst_col_path[256];
        snprintf(dst_col_path, sizeof(dst_col_path), "%s/%s/%08u/ts.col",
                 dst_dir, table, pday);
        struct stat st1, st2;
        ASSERT(stat(dst_col_path, &st1) == 0);

        /* Re-apply first block. */
        tsdb_rawblock_push_t r;
        memset(&r, 0, sizeof(r));
        strncpy(r.table, ctx.blocks[0].table, 63);
        r.part_day        = ctx.blocks[0].part_day;
        r.col_idx         = ctx.blocks[0].col_idx;
        r.codec           = ctx.blocks[0].meta.codec;
        r.flags           = ctx.blocks[0].meta.flags;
        r.count           = ctx.blocks[0].meta.count;
        r.ts_min          = ctx.blocks[0].meta.ts_min;
        r.ts_max          = ctx.blocks[0].meta.ts_max;
        r.block_bytes_len = (uint32_t)ctx.blocks[0].bytes_len;
        r.block_bytes     = ctx.blocks[0].bytes;
        ASSERT(tsdb_rawblock_apply(dst_db, &r) == TSDB_OK);

        ASSERT(stat(dst_col_path, &st2) == 0);
        ASSERT_EQ(st1.st_size, st2.st_size);
        printf("    idempotency check passed (no double-write)\n");
    }

    /* Serialize / parse round-trip. */
    {
        captured_block_t *cb = &ctx.blocks[0];
        tsdb_rawblock_push_t orig;
        memset(&orig, 0, sizeof(orig));
        strncpy(orig.table, cb->table, 63);
        orig.part_day        = cb->part_day;
        orig.col_idx         = cb->col_idx;
        orig.codec           = cb->meta.codec;
        orig.flags           = cb->meta.flags;
        orig.count           = cb->meta.count;
        orig.ts_min          = cb->meta.ts_min;
        orig.ts_max          = cb->meta.ts_max;
        orig.block_bytes_len = (uint32_t)cb->bytes_len;
        orig.block_bytes     = cb->bytes;

        uint8_t *buf = NULL; size_t blen = 0;
        ASSERT(tsdb_rawblock_serialize(&orig, &buf, &blen) == TSDB_OK);
        ASSERT(buf != NULL); ASSERT(blen > 0);

        tsdb_rawblock_push_t parsed;
        ASSERT(tsdb_rawblock_parse(buf, blen, &parsed) == TSDB_OK);
        ASSERT(strcmp(orig.table, parsed.table) == 0);
        ASSERT_EQ(orig.part_day,        parsed.part_day);
        ASSERT_EQ(orig.col_idx,         parsed.col_idx);
        ASSERT_EQ((int)orig.codec,      (int)parsed.codec);
        ASSERT_EQ(orig.count,           parsed.count);
        ASSERT(orig.ts_min == parsed.ts_min);
        ASSERT(orig.ts_max == parsed.ts_max);
        ASSERT_EQ(orig.block_bytes_len, parsed.block_bytes_len);
        ASSERT(memcmp(orig.block_bytes, parsed.block_bytes, orig.block_bytes_len) == 0);
        free(buf);
        printf("    serialize/parse round-trip passed\n");
    }

    capture_ctx_free(&ctx);
    tsdb_close(src_db);
    tsdb_close(dst_db);
    rmtree(src_dir);
    rmtree(dst_dir);
    printf("[test_rawblock] test 1 PASSED\n\n");
}

/* ---- Test 2: Merkle build — same data → same root ------------------------- */

static void test_merkle_same_root(void) {
    printf("[test_rawblock] test 2: Merkle build — identical data → same root\n");

    const char *dir1 = "/tmp/tsdb_merkle_a";
    const char *dir2 = "/tmp/tsdb_merkle_b";
    const char *table = "mtest";

    rmtree(dir1); rmtree(dir2);
    mkdir(dir1, 0755); mkdir(dir2, 0755);

    /* Build two identical schemas and flush identical data. */
    char sdir1[256], sdir2[256];
    snprintf(sdir1, sizeof(sdir1), "%s/%s", dir1, table);
    snprintf(sdir2, sizeof(sdir2), "%s/%s", dir2, table);
    mkdir(sdir1, 0755); mkdir(sdir2, 0755);

    tsdb_schema_t *s1 = make_schema(sdir1, table);
    tsdb_schema_t *s2 = make_schema(sdir2, table);

    int nrows = 50;
    tsdb_memtable_t *m1 = make_memtable(s1, nrows);
    tsdb_memtable_t *m2 = make_memtable(s2, nrows);

    ASSERT(tsdb_part_flush(s1, m1) == TSDB_OK);
    ASSERT(tsdb_part_flush(s2, m2) == TSDB_OK);

    /* Find the partition dir. */
    DIR *d = opendir(sdir1);
    ASSERT(d != NULL);
    struct dirent *e;
    char part_name[16] = {0};
    while ((e = readdir(d))) {
        if (e->d_name[0] >= '0' && e->d_name[0] <= '9') {
            strncpy(part_name, e->d_name, 15);
            break;
        }
    }
    closedir(d);
    ASSERT(part_name[0] != 0);

    char pdir1[256], pdir2[256];
    snprintf(pdir1, sizeof(pdir1), "%s/%s", sdir1, part_name);
    snprintf(pdir2, sizeof(pdir2), "%s/%s", sdir2, part_name);

    tsdb_part_t *p1 = NULL, *p2 = NULL;
    ASSERT(tsdb_part_open(s1, pdir1, &p1) == TSDB_OK);
    ASSERT(tsdb_part_open(s2, pdir2, &p2) == TSDB_OK);

    /* Build Merkle trees for column 0 (timestamp). */
    tsdb_merkle_t mt1, mt2;
    ASSERT(tsdb_merkle_build(p1, 0, &mt1) == TSDB_OK);
    ASSERT(tsdb_merkle_build(p2, 0, &mt2) == TSDB_OK);

    printf("    root1 = 0x%016llx  root2 = 0x%016llx  nleaves=%d\n",
           (unsigned long long)(uint64_t)mt1.root,
           (unsigned long long)(uint64_t)mt2.root,
           mt1.nleaves);

    ASSERT(mt1.root == mt2.root);
    ASSERT_EQ(mt1.nleaves, mt2.nleaves);
    printf("    root match confirmed\n");

    tsdb_part_close(p1); tsdb_part_close(p2);
    tsdb_memtable_free(m1); tsdb_memtable_free(m2);
    tsdb_schema_free(s1); tsdb_schema_free(s2);
    rmtree(dir1); rmtree(dir2);
    printf("[test_rawblock] test 2 PASSED\n\n");
}

/* ---- Test 3: Merkle diff — modify one leaf → diff has that index ---------- */

static void test_merkle_diff(void) {
    printf("[test_rawblock] test 3: Merkle diff — one modified block\n");

    /* Build two merkle trees from scratch using tsdb_merkle_build_raw. */

    /* Create synthetic block metadata for 3 blocks. */
    int nblocks = 3;
    tsdb_block_meta_t metas[3] = {
        { .offset = 0,   .size = 64,  .count = 10, .ts_min = 100, .ts_max = 200 },
        { .offset = 96,  .size = 128, .count = 20, .ts_min = 201, .ts_max = 400 },
        { .offset = 256, .size = 96,  .count = 15, .ts_min = 401, .ts_max = 500 },
    };

    /* Fake col map: just the compressed data bytes. */
    size_t total_size = 256 + 96 + 32; /* rough bound */
    uint8_t *col_map = calloc(1, total_size + 1024);
    ASSERT(col_map != NULL);

    /* Fill fake compressed bytes with distinct patterns. */
    /* Block 0: at offset 0+32 = 32, size 64. */
    /* Block 1: at offset 96+32 = 128, size 128. */
    /* Block 2: at offset 256+32 = 288, size 96. */
    for (int i = 0; i < 64;  i++) col_map[32  + i] = (uint8_t)(i + 1);
    for (int i = 0; i < 128; i++) col_map[128 + i] = (uint8_t)(i + 2);
    for (int i = 0; i < 96;  i++) col_map[288 + i] = (uint8_t)(i + 3);
    size_t col_map_size = 288 + 96 + 32;

    tsdb_merkle_t mt_local, mt_remote;
    ASSERT(tsdb_merkle_build_raw(col_map, col_map_size, metas, nblocks, &mt_local) == TSDB_OK);

    /* Remote: identical. */
    ASSERT(tsdb_merkle_build_raw(col_map, col_map_size, metas, nblocks, &mt_remote) == TSDB_OK);

    /* Verify no diff when identical. */
    uint16_t *diff = NULL;
    size_t ndiff = 0;
    ASSERT(tsdb_merkle_diff(&mt_local, &mt_remote, &diff, &ndiff) == TSDB_OK);
    ASSERT_EQ((int)ndiff, 0);
    printf("    identical trees: ndiff=%zu (expected 0)\n", ndiff);

    /* Modify block 1 in remote. */
    uint8_t *remote_map = malloc(col_map_size + 1024);
    ASSERT(remote_map != NULL);
    memcpy(remote_map, col_map, col_map_size);
    /* Flip one byte in block 1's compressed data. */
    remote_map[128] ^= 0xFF;

    ASSERT(tsdb_merkle_build_raw(remote_map, col_map_size, metas, nblocks, &mt_remote) == TSDB_OK);

    diff = NULL; ndiff = 0;
    ASSERT(tsdb_merkle_diff(&mt_local, &mt_remote, &diff, &ndiff) == TSDB_OK);
    printf("    after modifying block 1: ndiff=%zu diff[0]=%d\n",
           ndiff, ndiff > 0 ? diff[0] : -1);
    ASSERT(ndiff >= 1);
    int found = 0;
    for (size_t i = 0; i < ndiff; i++) {
        if (diff[i] == 1) { found = 1; break; }
    }
    ASSERT(found);
    free(diff);
    printf("    diff correctly contains leaf index 1\n");

    free(col_map);
    free(remote_map);
    printf("[test_rawblock] test 3 PASSED\n\n");
}

/* ---- Test 4: Throughput benchmark ----------------------------------------- */

static void test_benchmark(void) {
    printf("[test_rawblock] test 4: throughput benchmark\n");

    const char *bdir = "/tmp/tsdb_bench_rawblk";
    const char *table = "bench";
    const int NROWS = 10000;

    rmtree(bdir);
    mkdir(bdir, 0755);

    tsdb_db_t *db = NULL;
    ASSERT(tsdb_open(bdir, &db) == TSDB_OK);

    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "v",   TSDB_TYPE_FLOAT64   },
    };
    ASSERT(tsdb_create_table(db, table, cols, 2, "ts") == TSDB_OK);

    /* ---- Row-level path: encode+flush (baseline) ---- */
    int64_t t0 = now_ns();

    tsdb_table_t *tbl = NULL;
    ASSERT(tsdb_open_table(db, table, &tbl) == TSDB_OK);
    tsdb_batch_t *b = NULL;
    ASSERT(tsdb_batch_begin(tbl, &b) == TSDB_OK);

    int64_t base_ns = 1700000000000000000LL;
    for (int i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, base_ns + (int64_t)i * 1000000LL);
        tsdb_batch_row_f64(b, 1, (double)i);
        tsdb_batch_row_end(b);
    }
    ASSERT(tsdb_batch_commit(b) == TSDB_OK);

    int64_t t1 = now_ns();
    int64_t row_level_ns = t1 - t0;

    /* ---- Raw-block path: capture blocks, apply to replica dir ---- */
    /* Register capture hook. */
    capture_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    tsdb_db_set_raw_block_hook(db, mock_raw_block_hook, &ctx);

    /* Write again (triggers raw-block hook on flush). */
    ASSERT(tsdb_batch_begin(tbl, &b) == TSDB_OK);
    for (int i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, base_ns + 1000000000000LL + (int64_t)i * 1000000LL);
        tsdb_batch_row_f64(b, 1, (double)i * 2.0);
        tsdb_batch_row_end(b);
    }
    ASSERT(tsdb_batch_commit(b) == TSDB_OK);

    /* Now apply captured blocks to a replica dir (measures apply cost). */
    const char *rdir = "/tmp/tsdb_bench_rawblk_replica";
    rmtree(rdir); mkdir(rdir, 0755);
    tsdb_db_t *rdb = NULL;
    ASSERT(tsdb_open(rdir, &rdb) == TSDB_OK);
    ASSERT(tsdb_create_table_local(rdb, table, cols, 2, "ts") == TSDB_OK);

    int64_t t2 = now_ns();
    for (int i = 0; i < ctx.n; i++) {
        tsdb_rawblock_push_t r;
        memset(&r, 0, sizeof(r));
        strncpy(r.table, ctx.blocks[i].table, 63);
        r.part_day        = ctx.blocks[i].part_day;
        r.col_idx         = ctx.blocks[i].col_idx;
        r.codec           = ctx.blocks[i].meta.codec;
        r.flags           = ctx.blocks[i].meta.flags;
        r.count           = ctx.blocks[i].meta.count;
        r.ts_min          = ctx.blocks[i].meta.ts_min;
        r.ts_max          = ctx.blocks[i].meta.ts_max;
        r.block_bytes_len = (uint32_t)ctx.blocks[i].bytes_len;
        r.block_bytes     = ctx.blocks[i].bytes;
        ASSERT(tsdb_rawblock_apply(rdb, &r) == TSDB_OK);
    }
    int64_t t3 = now_ns();
    int64_t rawblk_apply_ns = t3 - t2;

    printf("\n");
    printf("  ┌─────────────────────────────────────────────┐\n");
    printf("  │ Benchmark: %d rows, 2 columns               │\n", NROWS);
    printf("  ├─────────────────────────────────────────────┤\n");
    printf("  │ Row-level encode+flush  : %8.3f ms        │\n",
           (double)row_level_ns / 1e6);
    printf("  │ Raw-block apply (replica): %7.3f ms        │\n",
           (double)rawblk_apply_ns / 1e6);
    printf("  │ Speedup (apply vs encode): %6.2f ×          │\n",
           (double)row_level_ns / (double)(rawblk_apply_ns > 0 ? rawblk_apply_ns : 1));
    printf("  │ Blocks captured: %-5d                       │\n", ctx.n);
    printf("  └─────────────────────────────────────────────┘\n");
    printf("\n");
    printf("  Note: raw-block apply skips encode/decode entirely — only\n");
    printf("  file I/O (header write + idx rewrite). On a network path\n");
    printf("  the savings are in avoided re-encode at the replica.\n\n");

    capture_ctx_free(&ctx);
    tsdb_close(db);
    tsdb_close(rdb);
    rmtree(bdir);
    rmtree(rdir);
    printf("[test_rawblock] test 4 PASSED\n\n");
}

/* ---- main ----------------------------------------------------------------- */

int main(void) {
    printf("=== test_rawblock ===\n\n");
    test_flush_apply();
    test_merkle_same_root();
    test_merkle_diff();
    test_benchmark();
    printf("=== ALL TESTS PASSED ===\n");
    return 0;
}
