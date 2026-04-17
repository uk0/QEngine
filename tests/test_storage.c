/* test_storage.c — storage engine integration tests. */

#include "../include/tsdb.h"
#include "../src/storage/schema.h"
#include "../src/storage/memtable.h"
#include "../src/storage/part.h"
#include "../src/storage/wal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <sys/stat.h>
#include <math.h>

/* ---- Test utilities ---------------------------------------------------- */

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
            fprintf(stderr, "ASSERT_OK FAILED: %s == %d (%s)  [%s:%d]\n", \
                    #rc, _rc, tsdb_errstr(_rc), __FILE__, __LINE__); \
            abort(); \
        } \
    } while(0)

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

/* ---- Test data ---------------------------------------------------------- */

/*
 * Two-day dataset:
 *   Day 1: 2026-04-15 UTC  (rows 0..9999)
 *   Day 2: 2026-04-16 UTC  (rows 10000..19999)
 * ts = day_start_ns + row_within_day * 1000000 (1ms apart)
 */
#define N_ROWS      20000
#define N_DAY1      10000
#define N_DAY2      10000
#define DAY1_NS     ((int64_t)1776211200LL * 1000000000LL)  /* 2026-04-15 00:00:00 UTC */
#define DAY2_NS     (DAY1_NS + (int64_t)86400LL * 1000000000LL)
#define TS_STEP_NS  1000000LL  /* 1 ms */

static const char *SYMBOLS[] = { "AAPL", "GOOG", "MSFT", "AMZN", "TSLA" };
#define N_SYMBOLS   5

static int64_t test_ts(int row)    { return (row < N_DAY1 ? DAY1_NS : DAY2_NS) + (int64_t)(row % N_DAY1) * TS_STEP_NS; }
static double  test_price(int row) { return 100.0 + (row % 1000) * 0.01; }
static int64_t test_vol(int row)   { return 1000L + row; }
static const char *test_sym(int row) { return SYMBOLS[row % N_SYMBOLS]; }

/* ---- Test 1: schema create / open --------------------------------------- */

static void test_schema(const char *base_dir) {
    printf("[TEST] schema create and open...\n");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/test_schema_tbl", base_dir);

    tsdb_col_t cols[] = {
        { "ts",     TSDB_TYPE_TIMESTAMP },
        { "symbol", TSDB_TYPE_SYMBOL    },
        { "price",  TSDB_TYPE_FLOAT64   },
        { "volume", TSDB_TYPE_INT64     },
    };
    int ncols = 4;

    tsdb_schema_t *s = NULL;
    ASSERT_OK(tsdb_schema_create(dir, "test_schema_tbl", cols, ncols, "ts", &s));
    ASSERT(s != NULL);
    ASSERT(s->ncols == 4);
    ASSERT(s->ts_col_idx == 0);
    ASSERT(strcmp(s->name, "test_schema_tbl") == 0);
    ASSERT(s->cols[1].type == TSDB_TYPE_SYMBOL);
    ASSERT(s->cols[1].symtab != NULL);

    /* Column index lookup. */
    ASSERT(tsdb_schema_col_idx(s, "ts")     == 0);
    ASSERT(tsdb_schema_col_idx(s, "symbol") == 1);
    ASSERT(tsdb_schema_col_idx(s, "price")  == 2);
    ASSERT(tsdb_schema_col_idx(s, "volume") == 3);
    ASSERT(tsdb_schema_col_idx(s, "nope")   == -1);

    /* Check schema.bin exists. */
    char schema_path[512];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.bin", dir);
    ASSERT(file_exists(schema_path));

    tsdb_schema_free(s);

    /* Re-open. */
    tsdb_schema_t *s2 = NULL;
    ASSERT_OK(tsdb_schema_open(dir, &s2));
    ASSERT(s2->ncols == 4);
    ASSERT(s2->ts_col_idx == 0);
    ASSERT(strcmp(s2->name, "test_schema_tbl") == 0);
    ASSERT(s2->cols[0].type == TSDB_TYPE_TIMESTAMP);
    ASSERT(s2->cols[1].type == TSDB_TYPE_SYMBOL);
    ASSERT(s2->cols[1].symtab != NULL);
    tsdb_schema_free(s2);

    printf("[TEST] schema: PASS\n");
}

/* ---- Test 2: memtable write / read -------------------------------------- */

static void test_memtable(const char *base_dir) {
    printf("[TEST] memtable write and read...\n");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/test_mt_tbl", base_dir);

    tsdb_col_t cols[] = {
        { "ts",     TSDB_TYPE_TIMESTAMP },
        { "symbol", TSDB_TYPE_SYMBOL    },
        { "price",  TSDB_TYPE_FLOAT64   },
        { "volume", TSDB_TYPE_INT64     },
    };
    tsdb_schema_t *s = NULL;
    ASSERT_OK(tsdb_schema_create(dir, "test_mt_tbl", cols, 4, "ts", &s));

    tsdb_memtable_t *m = NULL;
    ASSERT_OK(tsdb_memtable_new(s, &m));
    ASSERT(tsdb_memtable_rows(m) == 0);
    ASSERT(!tsdb_memtable_is_full(m));

    /* Write 3 rows. */
    for (int i = 0; i < 3; i++) {
        ASSERT_OK(tsdb_memtable_row_begin(m));
        ASSERT_OK(tsdb_memtable_row_ts(m, test_ts(i)));
        ASSERT_OK(tsdb_memtable_row_sym(m, 1, test_sym(i)));
        ASSERT_OK(tsdb_memtable_row_f64(m, 2, test_price(i)));
        ASSERT_OK(tsdb_memtable_row_i64(m, 3, test_vol(i)));
        ASSERT_OK(tsdb_memtable_row_end(m));
    }

    ASSERT(tsdb_memtable_rows(m) == 3);

    /* Verify ts column. */
    const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, 0);
    ASSERT(ts_buf[0] == test_ts(0));
    ASSERT(ts_buf[1] == test_ts(1));
    ASSERT(ts_buf[2] == test_ts(2));

    /* Verify price column. */
    const double *price_buf = (const double *)tsdb_memtable_col(m, 2);
    ASSERT(price_buf[0] == test_price(0));
    ASSERT(price_buf[1] == test_price(1));

    /* Missing column should fail. */
    int rc = tsdb_memtable_row_begin(m);
    ASSERT(rc == TSDB_OK);
    rc = tsdb_memtable_row_ts(m, test_ts(3));
    ASSERT(rc == TSDB_OK);
    /* Don't set other columns. */
    rc = tsdb_memtable_row_end(m);
    ASSERT(rc == TSDB_ERR_SCHEMA);  /* must fail */

    /* Row count unchanged (row was discarded). */
    ASSERT(tsdb_memtable_rows(m) == 3);

    tsdb_memtable_free(m);
    tsdb_schema_free(s);
    printf("[TEST] memtable: PASS\n");
}

/* ---- Test 3: WAL append and replay ------------------------------------- */

static int wal_test_basic_cb(const void *rec, size_t n, void *ctx) {
    (void)rec; (void)n; (void)ctx;
    /* Nothing to verify in this simple test; just count. */
    return 0;
}

static void test_wal(const char *base_dir) {
    printf("[TEST] WAL append and replay...\n");

    tsdb_wal_t *w = NULL;
    ASSERT_OK(tsdb_wal_open(base_dir, "test_wal_table", &w));

    const char *msg1 = "hello world";
    const char *msg2 = "tsdb wal test";
    ASSERT_OK(tsdb_wal_append(w, msg1, strlen(msg1)));
    ASSERT_OK(tsdb_wal_append(w, msg2, strlen(msg2)));
    ASSERT_OK(tsdb_wal_sync(w));
    tsdb_wal_close(w);

    /* Replay. */
    ASSERT_OK(tsdb_wal_replay(base_dir, "test_wal_table", wal_test_basic_cb, NULL));

    /* Reopen and truncate. */
    tsdb_wal_t *w2 = NULL;
    ASSERT_OK(tsdb_wal_open(base_dir, "test_wal_table", &w2));
    ASSERT_OK(tsdb_wal_truncate(w2));
    tsdb_wal_close(w2);

    printf("[TEST] WAL: PASS\n");
}

/* Callback for WAL replay. */
static int wal_replay_count_g = 0;
static int wal_replay_cb(const void *rec, size_t n, void *ctx) {
    (void)rec; (void)n; (void)ctx;
    wal_replay_count_g++;
    return 0;
}

static void test_wal_v2(const char *base_dir) {
    printf("[TEST] WAL v2 replay...\n");

    tsdb_wal_t *w = NULL;
    ASSERT_OK(tsdb_wal_open(base_dir, "wal_v2", &w));

    const char *msgs[] = { "record1", "record2", "record3" };
    for (int i = 0; i < 3; i++) {
        ASSERT_OK(tsdb_wal_append(w, msgs[i], strlen(msgs[i])));
    }
    ASSERT_OK(tsdb_wal_sync(w));
    tsdb_wal_close(w);

    wal_replay_count_g = 0;
    ASSERT_OK(tsdb_wal_replay(base_dir, "wal_v2", wal_replay_cb, NULL));
    ASSERT(wal_replay_count_g == 3);

    printf("[TEST] WAL v2: PASS\n");
}

/* ---- Test 4: full DB create_table / insert 20k rows / flush ------------ */

static void test_db_insert_flush(const char *db_dir) {
    printf("[TEST] DB: create table, insert 20000 rows, flush...\n");

    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(db_dir, &db));

    tsdb_col_t cols[] = {
        { "ts",     TSDB_TYPE_TIMESTAMP },
        { "symbol", TSDB_TYPE_SYMBOL    },
        { "price",  TSDB_TYPE_FLOAT64   },
        { "volume", TSDB_TYPE_INT64     },
    };
    ASSERT_OK(tsdb_create_table(db, "trades", cols, 4, "ts"));

    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, "trades", &tbl));

    /* Insert 20000 rows in a single batch. */
    tsdb_batch_t *b = NULL;
    ASSERT_OK(tsdb_batch_begin(tbl, &b));

    for (int i = 0; i < N_ROWS; i++) {
        ASSERT_OK(tsdb_batch_row_ts(b, test_ts(i)));
        ASSERT_OK(tsdb_batch_row_sym(b, 1, test_sym(i)));
        ASSERT_OK(tsdb_batch_row_f64(b, 2, test_price(i)));
        ASSERT_OK(tsdb_batch_row_i64(b, 3, test_vol(i)));
        ASSERT_OK(tsdb_batch_row_end(b));
    }

    ASSERT_OK(tsdb_batch_commit(b));

    /* Close DB (triggers final flush). */
    tsdb_close(db);

    /* Verify .col/.idx files exist for both days. */
    char path[512];
    snprintf(path, sizeof(path), "%s/trades/20260415/ts.col", db_dir);
    ASSERT(file_exists(path));
    snprintf(path, sizeof(path), "%s/trades/20260415/ts.idx", db_dir);
    ASSERT(file_exists(path));
    snprintf(path, sizeof(path), "%s/trades/20260416/ts.col", db_dir);
    ASSERT(file_exists(path));
    snprintf(path, sizeof(path), "%s/trades/20260416/ts.idx", db_dir);
    ASSERT(file_exists(path));

    printf("[TEST] DB insert + flush: PASS\n");
}

/* ---- Test 5: reopen DB, open table, verify schema ---------------------- */

static void test_db_reopen_schema(const char *db_dir) {
    printf("[TEST] DB reopen, open_table, verify schema...\n");

    tsdb_db_t *db = NULL;
    ASSERT_OK(tsdb_open(db_dir, &db));

    tsdb_table_t *tbl = NULL;
    ASSERT_OK(tsdb_open_table(db, "trades", &tbl));
    ASSERT(tbl != NULL);

    /* Verify schema by opening it directly (schema is the source of truth). */
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/trades", db_dir);
    tsdb_schema_t *s = NULL;
    ASSERT_OK(tsdb_schema_open(dir, &s));

    ASSERT(s->ncols == 4);
    ASSERT(s->ts_col_idx == 0);
    ASSERT(strcmp(s->cols[0].name, "ts")     == 0);
    ASSERT(strcmp(s->cols[1].name, "symbol") == 0);
    ASSERT(strcmp(s->cols[2].name, "price")  == 0);
    ASSERT(strcmp(s->cols[3].name, "volume") == 0);
    ASSERT(s->cols[0].type == TSDB_TYPE_TIMESTAMP);
    ASSERT(s->cols[1].type == TSDB_TYPE_SYMBOL);
    ASSERT(s->cols[2].type == TSDB_TYPE_FLOAT64);
    ASSERT(s->cols[3].type == TSDB_TYPE_INT64);

    tsdb_schema_free(s);
    tsdb_close(db);

    printf("[TEST] reopen schema: PASS\n");
}

/* ---- Test 6: block metadata + decompress consistency ------------------- */

/*
 * Verify all blocks of a single column against expected values.
 * row_base: the global row index of the first row in this partition's block 0.
 * col_idx:  column index in schema.
 * expected_total: total rows expected across all blocks in this partition.
 */
static void verify_col_all_blocks(tsdb_part_t *p, int col_idx,
                                   int row_base, size_t expected_total,
                                   tsdb_schema_t *s,
                                   const char *col_name)
{
    tsdb_block_meta_t *metas = NULL;
    size_t n_blocks = 0;
    ASSERT_OK(tsdb_part_col_blocks(p, col_idx, &metas, &n_blocks));
    ASSERT(n_blocks > 0);

    size_t total_count = 0;
    for (size_t bi = 0; bi < n_blocks; bi++) {
        ASSERT(metas[bi].count > 0);
        ASSERT(metas[bi].ts_min <= metas[bi].ts_max);
        total_count += metas[bi].count;
    }
    ASSERT(total_count == expected_total);

    /* Decode and verify every block. */
    tsdb_type_t col_type = s->cols[col_idx].type;
    size_t block_base = 0;

    for (size_t bi = 0; bi < n_blocks; bi++) {
        uint32_t cnt = metas[bi].count;
        size_t elem_sz = (col_type == TSDB_TYPE_FLOAT64) ? sizeof(double)
                       : (col_type == TSDB_TYPE_SYMBOL)  ? sizeof(uint32_t)
                                                         : sizeof(int64_t);
        void *decoded = malloc(cnt * elem_sz);
        ASSERT(decoded != NULL);
        ASSERT_OK(tsdb_part_read_block(p, col_idx, &metas[bi], decoded));

        for (uint32_t i = 0; i < cnt; i++) {
            int row = row_base + (int)block_base + (int)i;

            if (col_type == TSDB_TYPE_TIMESTAMP) {
                int64_t got = ((int64_t *)decoded)[i];
                int64_t exp = test_ts(row);
                if (got != exp) {
                    fprintf(stderr, "  %s block[%zu][%u] mismatch: "
                            "got %lld exp %lld (row %d)\n",
                            col_name, bi, i, (long long)got, (long long)exp, row);
                    ASSERT(0);
                }
            } else if (col_type == TSDB_TYPE_INT64) {
                int64_t got = ((int64_t *)decoded)[i];
                int64_t exp = test_vol(row);
                if (got != exp) {
                    fprintf(stderr, "  %s block[%zu][%u] mismatch: "
                            "got %lld exp %lld (row %d)\n",
                            col_name, bi, i, (long long)got, (long long)exp, row);
                    ASSERT(0);
                }
            } else if (col_type == TSDB_TYPE_FLOAT64) {
                double got = ((double *)decoded)[i];
                double exp = test_price(row);
                double diff = got - exp;
                if (diff < -1e-9 || diff > 1e-9) {
                    fprintf(stderr, "  %s block[%zu][%u] mismatch: "
                            "got %f exp %f (row %d)\n",
                            col_name, bi, i, got, exp, row);
                    ASSERT(0);
                }
            } else if (col_type == TSDB_TYPE_SYMBOL) {
                uint32_t code = ((uint32_t *)decoded)[i];
                const char *got = tsdb_symtab_str(s->cols[col_idx].symtab, code);
                const char *exp = test_sym(row);
                if (!got || strcmp(got, exp) != 0) {
                    fprintf(stderr, "  %s block[%zu][%u] mismatch: "
                            "got '%s' exp '%s' (row %d)\n",
                            col_name, bi, i, got ? got : "(null)", exp, row);
                    ASSERT(0);
                }
            }
        }
        free(decoded);
        block_base += cnt;
    }

    printf("  %s: %zu blocks, %zu values all verified OK\n",
           col_name, n_blocks, total_count);
    free(metas);
}

static void test_part_read(const char *db_dir) {
    printf("[TEST] partition read: block metadata and decompression...\n");

    char schema_dir[512];
    snprintf(schema_dir, sizeof(schema_dir), "%s/trades", db_dir);

    /* ---- Day 1 (20260415): rows 0..9999 --------------------------------- */
    {
        char part_dir[512];
        snprintf(part_dir, sizeof(part_dir), "%s/trades/20260415", db_dir);

        tsdb_schema_t *s = NULL;
        ASSERT_OK(tsdb_schema_open(schema_dir, &s));

        tsdb_part_t *p = NULL;
        ASSERT_OK(tsdb_part_open(s, part_dir, &p));

        printf("  --- Day 1 (20260415): rows 0..%d ---\n", N_DAY1 - 1);
        verify_col_all_blocks(p, 0, 0,       N_DAY1, s, "ts");
        verify_col_all_blocks(p, 1, 0,       N_DAY1, s, "symbol");
        verify_col_all_blocks(p, 2, 0,       N_DAY1, s, "price");
        verify_col_all_blocks(p, 3, 0,       N_DAY1, s, "volume");

        tsdb_part_close(p);
        tsdb_schema_free(s);
    }

    /* ---- Day 2 (20260416): rows 10000..19999 ---------------------------- */
    {
        char part_dir[512];
        snprintf(part_dir, sizeof(part_dir), "%s/trades/20260416", db_dir);

        tsdb_schema_t *s = NULL;
        ASSERT_OK(tsdb_schema_open(schema_dir, &s));

        tsdb_part_t *p = NULL;
        ASSERT_OK(tsdb_part_open(s, part_dir, &p));

        printf("  --- Day 2 (20260416): rows %d..%d ---\n", N_DAY1, N_ROWS - 1);
        /* row_base = N_DAY1 so that row index maps correctly into test_* helpers */
        verify_col_all_blocks(p, 0, N_DAY1,  N_DAY2, s, "ts");
        verify_col_all_blocks(p, 1, N_DAY1,  N_DAY2, s, "symbol");
        verify_col_all_blocks(p, 2, N_DAY1,  N_DAY2, s, "price");
        verify_col_all_blocks(p, 3, N_DAY1,  N_DAY2, s, "volume");

        tsdb_part_close(p);
        tsdb_schema_free(s);
    }

    printf("[TEST] partition read: PASS\n");
}

/* ---- Test 7: symbol column round-trip ---------------------------------- */

static void test_symbol_roundtrip(const char *db_dir) {
    printf("[TEST] symbol column round-trip...\n");

    char schema_dir[512];
    snprintf(schema_dir, sizeof(schema_dir), "%s/trades", db_dir);

    /* Verify symbol col (col 1) for both partition days. */
    const struct { const char *subdir; int row_base; size_t expected; } days[] = {
        { "20260415", 0,       N_DAY1 },
        { "20260416", N_DAY1,  N_DAY2 },
    };
    int ndays = 2;

    for (int d = 0; d < ndays; d++) {
        char part_dir[512];
        snprintf(part_dir, sizeof(part_dir), "%s/trades/%s", db_dir, days[d].subdir);

        tsdb_schema_t *s = NULL;
        ASSERT_OK(tsdb_schema_open(schema_dir, &s));

        tsdb_part_t *p = NULL;
        ASSERT_OK(tsdb_part_open(s, part_dir, &p));

        tsdb_block_meta_t *sym_metas = NULL;
        size_t n_sym_blocks = 0;
        ASSERT_OK(tsdb_part_col_blocks(p, 1, &sym_metas, &n_sym_blocks));
        ASSERT(n_sym_blocks > 0);

        size_t block_base = 0;
        size_t total_verified = 0;

        for (size_t bi = 0; bi < n_sym_blocks; bi++) {
            uint32_t cnt = sym_metas[bi].count;
            uint32_t *sym_decoded = malloc(cnt * sizeof(uint32_t));
            ASSERT(sym_decoded != NULL);
            ASSERT_OK(tsdb_part_read_block(p, 1, &sym_metas[bi], sym_decoded));

            for (uint32_t i = 0; i < cnt; i++) {
                int row = days[d].row_base + (int)block_base + (int)i;
                const char *expected = test_sym(row);
                const char *got = tsdb_symtab_str(s->cols[1].symtab, sym_decoded[i]);
                if (!got || strcmp(got, expected) != 0) {
                    fprintf(stderr, "  symbol %s block[%zu][%u] mismatch: "
                            "got '%s' expected '%s' (row %d)\n",
                            days[d].subdir, bi, i,
                            got ? got : "(null)", expected, row);
                    ASSERT(0);
                }
            }
            total_verified += cnt;
            free(sym_decoded);
            block_base += cnt;
        }
        free(sym_metas);

        printf("  %s: %zu blocks, %zu symbol values verified OK\n",
               days[d].subdir, n_sym_blocks, total_verified);

        tsdb_part_close(p);
        tsdb_schema_free(s);
    }

    printf("[TEST] symbol roundtrip: PASS\n");
}

/* ---- main -------------------------------------------------------------- */

int main(void) {
    /* Use /tmp for test data to avoid cluttering the project. */
    const char *base = "/tmp/tsdb_test_storage_data";

    /* Clean up any previous run. */
    system("rm -rf /tmp/tsdb_test_storage_data");

    printf("=== tsdb storage engine tests ===\n");
    printf("data dir: %s\n\n", base);

    test_schema(base);
    test_memtable(base);
    test_wal(base);
    test_wal_v2(base);
    test_db_insert_flush(base);
    test_db_reopen_schema(base);
    test_part_read(base);
    test_symbol_roundtrip(base);

    printf("\n=== ALL TESTS PASSED ===\n");
    return 0;
}
