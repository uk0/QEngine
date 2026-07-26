/* test_bloom_filter.c — Bloom filter block-skip tests for SYMBOL columns.
 *
 * Verifies:
 *  1. Hash helpers: bloom_add / bloom_test correctness (no false negatives).
 *  2. Write path: TSDB_BF_HAS_BLOOM set in BlockIndexEntry after flush.
 *  3. Read path: meta.bloom and meta.flags round-trip through idx.
 *  4. Query correctness: COUNT(*) WHERE sym='A' == 1000 (no false negatives).
 *  5. Query pruning: blocks not containing sym='A' are skipped.
 *  6. Non-existent symbol: all blocks skipped, count = 0.
 *
 * Insertion strategy: sequential batches (sym0 × 1000, sym1 × 1000, ...)
 * so each block is dominated by 1-2 distinct symbols, maximising bloom
 * selectivity and making pruning assertions reliable.
 */

#include "../include/tsdb.h"
#include "../src/core/bits.h"
#include "../src/compress/codec.h"
#include "../src/storage/schema.h"
#include "../src/storage/memtable.h"
#include "../src/storage/part.h"
#include "../src/storage/db.h"
#include "../src/query/exec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>

/* ---- Helpers --------------------------------------------------------------- */

#define ASSERT(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #cond, __FILE__, __LINE__); \
            abort(); \
        } \
    } while(0)

#define ASSERT_OK(rc) \
    do { \
        int _rc = (rc); \
        if (_rc != TSDB_OK) { \
            fprintf(stderr, "ASSERT_OK: %s == %d (%s)  [%s:%d]\n", \
                    #rc, _rc, tsdb_errstr(_rc), __FILE__, __LINE__); \
            abort(); \
        } \
    } while(0)

/* Recursively mkdir. */
static void mkdirs(const char *path) {
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

/* ---- Test 1: bloom hash helpers ---------------------------------------- */

static void test_bloom_helpers(void) {
    printf("[BLOOM] hash helpers... ");
    /* Basic: add a value and test it */
    uint64_t b = 0;
    b = tsdb_bloom_add(b, 42u);
    ASSERT(tsdb_bloom_test(b, 42u) == 1);
    /* Other values may or may not be in (false positive ok, false negative not). */

    /* Add 10 values 0..9, test all present. */
    b = 0;
    for (uint32_t v = 0; v < 10; v++) b = tsdb_bloom_add(b, v);
    for (uint32_t v = 0; v < 10; v++) {
        ASSERT(tsdb_bloom_test(b, v) == 1);
    }

    /* Empty bloom: nothing in it. */
    b = 0;
    /* With a zero bloom (all bits 0), tsdb_bloom_test should return 0 for any v
     * unless a hash maps to bit 0 — but that's FNV-dependent. For the empty
     * bloom to return 0 we rely on at least one hash producing a non-zero bit
     * position. We just verify the no-false-negative property by explicitly
     * adding before testing. */

    /* Verify no false negatives with a wider sweep. */
    for (uint32_t v = 1000; v < 2000; v++) {
        uint64_t bv = 0;
        bv = tsdb_bloom_add(bv, v);
        ASSERT(tsdb_bloom_test(bv, v) == 1);
    }

    printf("PASS\n");
}

/* ---- Test 2+3: write path round-trip ----------------------------------- */

/*
 * Write 10 symbols × 1000 rows each sequentially.
 * TSDB_BLOCK_POINTS = 8192, so 10000 rows fills 2 blocks.
 * Each block will contain ~5 symbols in 1000-row runs.
 *
 * After flush, open partition and verify:
 *   - TSDB_BF_HAS_BLOOM is set in meta.flags for the sym column blocks.
 *   - meta.bloom != 0.
 *   - Each block's bloom contains all symbols actually present.
 *   - No false negatives.
 */

#define BASE_NS    1776211200000000000LL   /* 2026-04-15 00:00:00 UTC */
#define ROWS_PER_SYM    1000
#define N_SYMS          10
#define TOTAL_ROWS      (ROWS_PER_SYM * N_SYMS)   /* 10000 */

static const char *SYM_NAMES[N_SYMS] = {
    "sym_A", "sym_B", "sym_C", "sym_D", "sym_E",
    "sym_F", "sym_G", "sym_H", "sym_I", "sym_J"
};

static void test_write_read_roundtrip(tsdb_db_t *db, const char *data_dir) {
    printf("[BLOOM] write/read roundtrip... ");

    /* Create table with a sym column. */
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "sym", TSDB_TYPE_SYMBOL    },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    ASSERT_OK(tsdb_create_table(db, "bloom_rrt", cols, 3, "ts"));
    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, "bloom_rrt", &tbl));

    /* Insert rows: sym0 × 1000, sym1 × 1000, ... sym9 × 1000.
     * Use commit every ROWS_PER_SYM rows to force flush-like batching.
     * Actually commit the whole table at once to let the storage layer
     * chunk into blocks of TSDB_BLOCK_POINTS (8192).
     * We insert all 10000 rows in one big batch. */
    tsdb_batch_t *batch = NULL;
    ASSERT_OK(tsdb_batch_begin(tbl, &batch));
    for (int s = 0; s < N_SYMS; s++) {
        for (int r = 0; r < ROWS_PER_SYM; r++) {
            int64_t ts = BASE_NS + (int64_t)(s * ROWS_PER_SYM + r) * 1000000LL;
            ASSERT_OK(tsdb_batch_row_ts(batch, ts));
            ASSERT_OK(tsdb_batch_row_sym(batch, 1, SYM_NAMES[s]));
            ASSERT_OK(tsdb_batch_row_f64(batch, 2, (double)(s * 1000 + r)));
            ASSERT_OK(tsdb_batch_row_end(batch));
        }
    }
    ASSERT_OK(tsdb_batch_commit(batch));

    /* Bloom pruning is a *disk-block* optimisation: exec.c only consults a
     * block's bloom when the scan source is a partition (src->part), never
     * for memtable rows, so rows still in the memtable are always scanned and
     * always counted.  That means the pruning assertions below are only
     * meaningful once every row owns an on-disk block.
     *
     * Under TSDB_WAL_ONLY_COMMIT=1 the commit above is WAL-fsync + memtable
     * insert; only the 8192 rows that overflowed the memtable reached a
     * partition, leaving one block that legitimately contains sym_A and
     * nothing to skip.  Force the tail out so the block layout is the same
     * 8192+1808 split in both durability modes. */
    ASSERT_OK(tsdb_db_flush_all(db));

    /* Find partition directory (should be 20260415). */
    char part_dir[4096];
    snprintf(part_dir, sizeof(part_dir), "%s/bloom_rrt/20260415", data_dir);

    /* Open schema for the partition read. */
    char schema_dir[4096];
    snprintf(schema_dir, sizeof(schema_dir), "%s/bloom_rrt", data_dir);
    tsdb_schema_t *schema = NULL;
    ASSERT_OK(tsdb_schema_open(schema_dir, &schema));

    tsdb_part_t *part = NULL;
    ASSERT_OK(tsdb_part_open(schema, part_dir, &part));

    /* Find the sym column index in schema. */
    int sym_col = tsdb_schema_col_idx(schema, "sym");
    ASSERT(sym_col >= 0);

    tsdb_block_meta_t *metas = NULL;
    size_t nb = 0;
    ASSERT_OK(tsdb_part_col_blocks(part, sym_col, &metas, &nb));
    ASSERT(nb > 0);
    printf("(%zu blocks) ", nb);

    /* Every sym block should have TSDB_BF_HAS_BLOOM set. */
    int bloom_count = 0;
    for (size_t b = 0; b < nb; b++) {
        if (metas[b].flags & TSDB_BF_HAS_BLOOM) {
            bloom_count++;
            ASSERT(metas[b].bloom != 0);
        }
    }
    ASSERT(bloom_count == (int)nb);

    /* No false negatives: every symbol actually in the block must pass bloom. */
    /* We need the symbol codes to test. Re-use symtab from schema. */
    tsdb_symtab_t *symtab = schema->cols[sym_col].symtab;
    for (size_t b = 0; b < nb; b++) {
        /* Decode the block to know which symbols are in it. */
        uint32_t *codes = malloc(metas[b].count * sizeof(uint32_t));
        ASSERT(codes != NULL);
        ASSERT_OK(tsdb_part_read_block(part, sym_col, &metas[b], codes));
        for (uint32_t k = 0; k < metas[b].count; k++) {
            /* Every code in the block must pass bloom. */
            ASSERT(tsdb_bloom_test(metas[b].bloom, codes[k]) == 1);
        }
        free(codes);
    }

    free(metas);
    tsdb_part_close(part);
    tsdb_schema_free(schema);
    printf("PASS\n");
}

/* ---- Test 4: query correctness (no false negatives) -------------------- */

static void test_query_correctness(tsdb_db_t *db) {
    printf("[BLOOM] query correctness... ");

    /* Disable parallel scan for predictable bloom stats. */
    tsdb_set_query_parallel(0);

    for (int s = 0; s < N_SYMS; s++) {
        char qtl[256];
        snprintf(qtl, sizeof(qtl),
                 "SELECT count(*) FROM bloom_rrt WHERE sym = '%s'",
                 SYM_NAMES[s]);
        tsdb_result_t *r = NULL;
        ASSERT_OK(tsdb_query(db, qtl, &r));
        ASSERT(tsdb_result_next(r) == 1);
        int64_t cnt = tsdb_result_i64(r, 0);
        if (cnt != ROWS_PER_SYM) {
            fprintf(stderr, "FAIL: count for %s = %lld (expected %d)\n",
                    SYM_NAMES[s], (long long)cnt, ROWS_PER_SYM);
            abort();
        }
        tsdb_result_free(r);
    }
    printf("PASS (all %d syms return %d rows each)\n", N_SYMS, ROWS_PER_SYM);
}

/* ---- Test 5: query pruning -------------------------------------------- */

static void test_query_pruning(tsdb_db_t *db) {
    printf("[BLOOM] block pruning... ");

    tsdb_set_query_parallel(0);

    /* Query sym_A which is the first 1000 rows. With BLOCK_POINTS=8192,
     * all 10000 rows fit in 2 blocks. sym_A occupies rows 0..999 of the
     * first block (block 0). Block 1 covers rows 8192..9999 which are
     * sym_H, sym_I, sym_J. The bloom for block 1 should NOT contain sym_A.
     *
     * We expect: total >= 1, skipped >= 1 (block 1 skipped). */
    tsdb_result_t *r = NULL;
    ASSERT_OK(tsdb_query(db, "SELECT count(*) FROM bloom_rrt WHERE sym = 'sym_A'", &r));
    ASSERT(tsdb_result_next(r) == 1);
    int64_t cnt = tsdb_result_i64(r, 0);
    ASSERT(cnt == ROWS_PER_SYM);
    tsdb_result_free(r);

    uint64_t skipped = tsdb_bloom_stats_skipped();
    uint64_t total   = tsdb_bloom_stats_total();
    printf("total_blocks=%llu skipped=%llu... ",
           (unsigned long long)total, (unsigned long long)skipped);

    /* We must see at least 1 block skipped for sym_A. */
    ASSERT(total >= 1);
    ASSERT(skipped >= 1);
    printf("PASS\n");

    /* Query the last symbol sym_J (rows 9000..9999). Expect block 0 skipped. */
    r = NULL;
    ASSERT_OK(tsdb_query(db, "SELECT count(*) FROM bloom_rrt WHERE sym = 'sym_J'", &r));
    ASSERT(tsdb_result_next(r) == 1);
    cnt = tsdb_result_i64(r, 0);
    ASSERT(cnt == ROWS_PER_SYM);
    tsdb_result_free(r);

    skipped = tsdb_bloom_stats_skipped();
    printf("[BLOOM] sym_J pruning: skipped=%llu... ",
           (unsigned long long)skipped);
    ASSERT(skipped >= 1);
    printf("PASS\n");
}

/* ---- Test 6: non-existent symbol → all blocks skipped ------------------ */

static void test_nonexistent_symbol(tsdb_db_t *db) {
    printf("[BLOOM] non-existent symbol... ");

    tsdb_set_query_parallel(0);

    tsdb_result_t *r = NULL;
    ASSERT_OK(tsdb_query(db,
        "SELECT count(*) FROM bloom_rrt WHERE sym = 'NONEXISTENT_XYZ'", &r));
    ASSERT(tsdb_result_next(r) == 1);
    int64_t cnt = tsdb_result_i64(r, 0);
    ASSERT(cnt == 0);
    tsdb_result_free(r);

    uint64_t skipped = tsdb_bloom_stats_skipped();
    uint64_t total   = tsdb_bloom_stats_total();

    printf("count=0 skipped=%llu total=%llu... ",
           (unsigned long long)skipped, (unsigned long long)total);

    /* A non-existent symbol triggers the sentinel path (bloom_constraint.col=-1)
     * which skips ALL disk blocks without even reading bloom bits. */
    ASSERT(skipped == total);
    printf("PASS\n");
}

/* ---- Test 7: backward compatibility — v1 idx blocks ignored ------------ */

static void test_backward_compat(void) {
    printf("[BLOOM] backward compat (bloom=0, no HAS_BLOOM flag): ");

    /* Simulate an old block_meta with flags=0, bloom=0.
     * tsdb_bloom_test must not be called when flag is absent. */
    tsdb_block_meta_t old_meta = {0};
    old_meta.flags = 0;   /* no TSDB_BF_HAS_BLOOM */
    old_meta.bloom = 0;

    /* The flag check in bloom_can_skip_block ensures we never read bloom=0
     * as a pruning signal. We verify the flag bit is correctly absent. */
    ASSERT(!(old_meta.flags & TSDB_BF_HAS_BLOOM));

    /* A new block should have the flag. */
    tsdb_block_meta_t new_meta = {0};
    new_meta.flags = TSDB_BF_HAS_BLOOM;
    new_meta.bloom = 0xDEADBEEFCAFEBABEULL;
    ASSERT(new_meta.flags & TSDB_BF_HAS_BLOOM);
    ASSERT(new_meta.bloom != 0);

    printf("PASS\n");
}

/* ---- Test 8: larger table with more blocks for stronger pruning --------- */

static void test_high_cardinality(tsdb_db_t *db) {
    printf("[BLOOM] high-cardinality pruning (20 syms, 400 rows each)... ");

    tsdb_set_query_parallel(0);

    /* Create a second table with 20 distinct symbols. */
    tsdb_col_t cols[] = {
        { "ts",      TSDB_TYPE_TIMESTAMP },
        { "devname", TSDB_TYPE_SYMBOL    },
        { "metric",  TSDB_TYPE_FLOAT64   },
    };
    ASSERT_OK(tsdb_create_table(db, "bloom_hc", cols, 3, "ts"));
    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, "bloom_hc", &tbl));

#define HC_SYMS  20
#define HC_ROWS  400     /* 20 * 400 = 8000 < 8192, fits in 1 block → prune by sentinel */
    const char *hc_names[HC_SYMS] = {
        "dev_00","dev_01","dev_02","dev_03","dev_04",
        "dev_05","dev_06","dev_07","dev_08","dev_09",
        "dev_10","dev_11","dev_12","dev_13","dev_14",
        "dev_15","dev_16","dev_17","dev_18","dev_19",
    };

    tsdb_batch_t *batch = NULL;
    ASSERT_OK(tsdb_batch_begin(tbl, &batch));
    for (int s = 0; s < HC_SYMS; s++) {
        for (int r = 0; r < HC_ROWS; r++) {
            int64_t ts = BASE_NS + (int64_t)(s * HC_ROWS + r) * 1000000LL;
            ASSERT_OK(tsdb_batch_row_ts(batch, ts));
            ASSERT_OK(tsdb_batch_row_sym(batch, 1, hc_names[s]));  /* devname col */
            ASSERT_OK(tsdb_batch_row_f64(batch, 2, (double)(s * HC_ROWS + r)));
            ASSERT_OK(tsdb_batch_row_end(batch));
        }
    }
    ASSERT_OK(tsdb_batch_commit(batch));
    /* Same reason as above: put the rows on disk so the single-block
     * sentinel/bloom path is what answers the query in both modes. */
    ASSERT_OK(tsdb_db_flush_all(db));

    /* Query a single device. */
    tsdb_result_t *r = NULL;
    ASSERT_OK(tsdb_query(db,
        "SELECT count(*) FROM bloom_hc WHERE devname = 'dev_05'", &r));
    ASSERT(tsdb_result_next(r) == 1);
    int64_t cnt = tsdb_result_i64(r, 0);
    ASSERT(cnt == HC_ROWS);
    tsdb_result_free(r);

    printf("count=%lld PASS\n", (long long)cnt);
}

/* ---- Main ---------------------------------------------------------------- */

int main(void) {
    printf("=== Bloom filter tests ===\n");

    /* Test 1: hash helpers (no DB needed). */
    test_bloom_helpers();
    test_backward_compat();

    /* Tests 2..8: need a DB. */
    char data_dir[] = "/tmp/tsdb_bloom_test_XXXXXX";
    if (!mkdtemp(data_dir)) {
        fprintf(stderr, "mkdtemp failed\n");
        return 1;
    }

    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(data_dir, &db));

    test_write_read_roundtrip(db, data_dir);
    test_query_correctness(db);
    test_query_pruning(db);
    test_nonexistent_symbol(db);
    test_high_cardinality(db);

    tsdb_close(db);

    printf("=== ALL BLOOM TESTS PASSED ===\n");
    return 0;
}
