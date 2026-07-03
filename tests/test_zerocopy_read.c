/* test_zerocopy_read.c — zero-copy RAW block reads via mmap pointer handoff.
 *
 * The partition reader mmaps every .col PROT_READ (tsdb_part_open).  For a
 * block whose codec is RAW/NONE, carries no outer-LZ wrap, and whose payload
 * is naturally aligned for the column type, tsdb_part_read_block_ref() hands
 * out a pointer INTO the mapping instead of memcpy'ing into a scratch buffer.
 * Producer side: the adaptive float encoder stores a FLOAT64 block RAW when
 * both XOR codecs would expand past 8 B/value (high-entropy data), and the
 * column writer pads block starts to 8 bytes so those payloads land aligned.
 *
 * Asserts:
 *   1. a high-entropy FLOAT64 column flushes as RAW blocks (fast-path
 *      producer) and the ts column stays compressed (DoD/PFOR);
 *   2. fast path ON: ref-reads of the val column return in-map pointers
 *      (owned == NULL, ptr inside tsdb_part_col_map range), values match
 *      the legacy tsdb_part_read_block decode bit-for-bit, hit counter
 *      moves; ts column ref-reads FALL BACK to owned buffers (compressed);
 *   3. TSDB_ZEROCOPY_READ=0 (latched at part open): ref-reads return owned
 *      buffers, same values, hit counter does NOT move;
 *   4. SQL layer: sum(val) with a WHERE (defeats the stats fastpath, forces
 *      block loads) is bit-identical ON vs OFF; ON moves the hit counter
 *      (converted agg/GROUP-BY scan paths), OFF does not; row scan spot-check
 *      matches under both settings;
 *   5. micro-bench: ns/row scanning ~5M rows through the ref API with the
 *      fast path ON vs OFF — printed only, correctness asserted via sums.
 */

#include "../include/tsdb.h"
#include "../src/storage/schema.h"
#include "../src/storage/part.h"
#include "../src/core/types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof(cmd), "rm -rf %s", dir); (void)system(cmd);
}

#define NROWS    1000000            /* ~123 blocks of 8192 */
#define BASE_TS  1000000000000LL
#define STEP_NS  1000000LL          /* 1 ms — whole dataset in one partition */
#define BENCH_REPS 5                /* 5 * NROWS = 5M rows scanned per mode */

/* splitmix64-driven high-entropy doubles: random sign + random mantissa +
 * exponent uniform in 2^[-32,32] (always finite, moderate magnitude).  The
 * XOR of consecutive values then has ~no leading/trailing zeros, so BOTH
 * float XOR codecs expand past 8 B/value — which is what routes the blocks
 * to RAW in the adaptive encoder.  (Plain uniform [0,1) is NOT enough: its
 * shared exponent bits let Chimp squeak in just under 8 B/value.) */
static uint64_t g_rng;
static uint64_t rng_next(void) {
    uint64_t z = (g_rng += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}
static double val_of(int64_t i) {
    (void)i;
    uint64_t sign_mant = rng_next() & 0x800FFFFFFFFFFFFFULL;
    uint64_t exp = 1023 - 32 + (rng_next() % 65);
    uint64_t bits = sign_mant | (exp << 52);
    double d;
    memcpy(&d, &bits, 8);
    return d;
}

static int64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* 4-accumulator sum: breaks the serial FP-add dependency so consumption is
 * closer to the SIMD-ized executor's, letting the read-path cost show.
 * Used for BOTH the reference and the measured paths — identical order →
 * bit-identical sums whenever the underlying values are identical. */
static double sum4(const double *v, size_t n) {
    double a0 = 0, a1 = 0, a2 = 0, a3 = 0;
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        a0 += v[i]; a1 += v[i + 1]; a2 += v[i + 2]; a3 += v[i + 3];
    }
    for (; i < n; i++) a0 += v[i];
    return (a0 + a1) + (a2 + a3);
}

static void write_dataset(const char *dir) {
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "open failed\n"); exit(1); }
    tsdb_col_t cols[] = {
        { "ts",  TSDB_TYPE_TIMESTAMP },
        { "val", TSDB_TYPE_FLOAT64   },
    };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "create rc=%d\n", rc); exit(1); }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "open_table failed\n"); exit(1); }
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    g_rng = 0x5EEDF00DULL;
    for (int64_t i = 0; i < NROWS; i++) {
        tsdb_batch_row_ts(b, BASE_TS + i * STEP_NS);
        tsdb_batch_row_f64(b, 1, val_of(i));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    tsdb_close(db);
}

/* Find the (single) "<table_dir>/<YYYYMMDD...>" partition subdirectory. */
static int find_part_dir(const char *table_dir, char *out, size_t cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4096];
        snprintf(p, sizeof(p), "%s/%s", table_dir, e->d_name);
        struct stat st;
        if (stat(p, &st) == 0 && S_ISDIR(st.st_mode)) {
            snprintf(out, cap, "%s", p);
            found = 1;
            break;
        }
    }
    closedir(d);
    return found;
}

/* Sum one column via the ref API over all blocks; returns rows summed.
 * When expect_zerocopy >= 0, checks every block's ownership mode. */
static size_t ref_sum_col(tsdb_part_t *p, int col,
                          const tsdb_block_meta_t *metas, size_t nb,
                          int expect_zerocopy, double *out_sum,
                          int *ownership_ok) {
    const uint8_t *map = NULL; size_t map_len = 0;
    tsdb_part_col_map(p, col, &map, &map_len);
    double sum = 0.0;
    size_t rows = 0;
    int ok = 1;
    for (size_t b = 0; b < nb; b++) {
        const void *data = NULL; void *owned = NULL;
        if (tsdb_part_read_block_ref(p, col, &metas[b], &data, &owned) != TSDB_OK) {
            ok = 0; break;
        }
        if (expect_zerocopy == 1) {
            /* Must be a pointer into the column mapping, not a copy. */
            if (owned != NULL) ok = 0;
            const uint8_t *d = (const uint8_t *)data;
            if (!(d >= map && d + (size_t)metas[b].count * 8 <= map + map_len))
                ok = 0;
        } else if (expect_zerocopy == 0) {
            if (owned == NULL || owned != data) ok = 0;
        }
        sum += sum4((const double *)data, metas[b].count);
        rows += metas[b].count;
        free(owned);
    }
    if (out_sum) *out_sum = sum;
    if (ownership_ok) *ownership_ok = ok;
    return rows;
}

int main(void) {
    printf("=== test_zerocopy_read ===\n");
    const char *dir = "/tmp/tsdb_test_zerocopy";
    rmrf(dir);

    printf("\n[0] write %d rows (high-entropy FLOAT64 val)\n", NROWS);
    write_dataset(dir);

    char table_dir[4096], part_dir[4096];
    snprintf(table_dir, sizeof(table_dir), "%s/t", dir);
    if (!find_part_dir(table_dir, part_dir, sizeof(part_dir))) {
        fprintf(stderr, "FAIL: no partition dir (not flushed?)\n"); return 1;
    }

    tsdb_schema_t *s = NULL;
    if (tsdb_schema_open(table_dir, &s) != TSDB_OK) {
        fprintf(stderr, "FAIL: schema_open\n"); return 1;
    }
    const int ts_col = 0, val_col = 1;

    /* ---- [1] producer: val blocks are RAW, ts blocks are compressed ---- */
    printf("\n[1] RAW producer + block codecs\n");
    setenv("TSDB_ZEROCOPY_READ", "1", 1);
    tsdb_part_t *p = NULL;
    if (tsdb_part_open(s, part_dir, &p) != TSDB_OK) {
        fprintf(stderr, "FAIL: part_open\n"); return 1;
    }
    tsdb_block_meta_t *vmetas = NULL; size_t vnb = 0;
    tsdb_block_meta_t *tmetas = NULL; size_t tnb = 0;
    CHECK(tsdb_part_col_blocks(p, val_col, &vmetas, &vnb) == TSDB_OK && vnb > 1,
          "val column has multiple blocks");
    CHECK(tsdb_part_col_blocks(p, ts_col, &tmetas, &tnb) == TSDB_OK && tnb == vnb,
          "ts column has matching block count");
    int all_raw = 1, all_aligned = 1, ts_compressed = 1;
    size_t total = 0;
    for (size_t b = 0; b < vnb; b++) {
        if (vmetas[b].codec != TSDB_CODEC_RAW) all_raw = 0;
        if (((vmetas[b].offset + TSDB_BLOCK_HEADER_SIZE) & 7) != 0) all_aligned = 0;
        total += vmetas[b].count;
    }
    for (size_t b = 0; b < tnb; b++)
        if (tmetas[b].codec == TSDB_CODEC_RAW || tmetas[b].codec == TSDB_CODEC_NONE)
            ts_compressed = 0;
    CHECK(all_raw, "every high-entropy val block stored RAW (fast-path producer)");
    CHECK(all_aligned, "every block payload 8-byte aligned (writer pad)");
    CHECK(ts_compressed, "ts column still compressed (DoD/PFOR)");
    CHECK(total == NROWS, "block row counts sum to NROWS");

    /* Reference decode via the legacy copying API. */
    double ref_sum = 0.0;
    {
        double *buf = malloc((size_t)8192 * 8 * 2);
        for (size_t b = 0; b < vnb; b++) {
            if (tsdb_part_read_block(p, val_col, &vmetas[b], buf) != TSDB_OK) {
                fprintf(stderr, "FAIL: read_block\n"); return 1;
            }
            ref_sum += sum4(buf, vmetas[b].count);
        }
        free(buf);
    }

    /* ---- [2] fast path ON: in-map pointers, same values, counter moves - */
    printf("\n[2] ref-read with fast path ON\n");
    uint64_t h0 = 0, f0 = 0, h1 = 0, f1 = 0;
    tsdb_part_zerocopy_stats(&h0, &f0);
    double on_sum = 0.0; int own_ok = 0;
    size_t rows = ref_sum_col(p, val_col, vmetas, vnb, 1, &on_sum, &own_ok);
    tsdb_part_zerocopy_stats(&h1, &f1);
    CHECK(rows == NROWS, "ON: scanned all rows");
    CHECK(own_ok, "ON: every val block served zero-copy from inside the mmap");
    CHECK(memcmp(&on_sum, &ref_sum, 8) == 0, "ON: sum bit-identical to legacy decode");
    CHECK(h1 - h0 == vnb, "ON: hit counter advanced once per val block");

    /* ts column (DoD/PFOR) must FALL BACK to an owned decode even when ON. */
    {
        tsdb_part_zerocopy_stats(&h0, &f0);
        const void *data = NULL; void *owned = NULL;
        int rc = tsdb_part_read_block_ref(p, ts_col, &tmetas[0], &data, &owned);
        tsdb_part_zerocopy_stats(&h1, &f1);
        CHECK(rc == TSDB_OK && owned != NULL && owned == data,
              "ON: compressed ts block falls back to an owned decode");
        CHECK(h1 == h0 && f1 - f0 == 1, "ON: ts fallback counted as fallback, not hit");
        CHECK(((const int64_t *)data)[0] == BASE_TS, "ON: ts fallback decodes correctly");
        free(owned);
    }
    tsdb_part_close(p);

    /* ---- [3] kill switch: TSDB_ZEROCOPY_READ=0 --------------------------- */
    printf("\n[3] ref-read with TSDB_ZEROCOPY_READ=0\n");
    setenv("TSDB_ZEROCOPY_READ", "0", 1);
    tsdb_part_t *poff = NULL;
    if (tsdb_part_open(s, part_dir, &poff) != TSDB_OK) {
        fprintf(stderr, "FAIL: part_open (off)\n"); return 1;
    }
    tsdb_part_zerocopy_stats(&h0, &f0);
    double off_sum = 0.0; own_ok = 0;
    rows = ref_sum_col(poff, val_col, vmetas, vnb, 0, &off_sum, &own_ok);
    tsdb_part_zerocopy_stats(&h1, &f1);
    CHECK(rows == NROWS, "OFF: scanned all rows");
    CHECK(own_ok, "OFF: every block returned as an owned copy");
    CHECK(memcmp(&off_sum, &ref_sum, 8) == 0, "OFF: sum bit-identical to legacy decode");
    CHECK(h1 == h0, "OFF: hit counter did not move");
    tsdb_part_close(poff);

    /* ---- [4] micro-bench: ns/row, ON vs OFF ------------------------------ */
    printf("\n[4] micro-bench: %d x %d rows via ref API\n", BENCH_REPS, NROWS);
    setenv("TSDB_ZEROCOPY_READ", "1", 1);
    tsdb_part_t *pon = NULL;
    tsdb_part_open(s, part_dir, &pon);
    setenv("TSDB_ZEROCOPY_READ", "0", 1);
    tsdb_part_open(s, part_dir, &poff);

    volatile double sink = 0.0;
    double warm = 0.0;
    ref_sum_col(pon, val_col, vmetas, vnb, -1, &warm, NULL);  /* fault pages in */
    sink += warm;

    int64_t t_on = 0, t_off = 0;
    for (int r = 0; r < BENCH_REPS; r++) {
        double s1 = 0.0, s2 = 0.0;
        int64_t a = now_ns();
        ref_sum_col(pon, val_col, vmetas, vnb, -1, &s1, NULL);
        int64_t b = now_ns();
        ref_sum_col(poff, val_col, vmetas, vnb, -1, &s2, NULL);
        int64_t c = now_ns();
        t_on  += b - a;
        t_off += c - b;
        sink += s1 + s2;
        if (memcmp(&s1, &ref_sum, 8) != 0 || memcmp(&s2, &ref_sum, 8) != 0) {
            CHECK(0, "bench sums stay bit-identical");
            break;
        }
    }
    double rows_total = (double)BENCH_REPS * (double)NROWS;
    printf("  zero-copy ON : %8.3f ms total  %6.3f ns/row\n",
           t_on / 1e6,  (double)t_on / rows_total);
    printf("  zero-copy OFF: %8.3f ms total  %6.3f ns/row\n",
           t_off / 1e6, (double)t_off / rows_total);
    printf("  speedup      : %.2fx (copy path / pointer path)\n",
           t_on > 0 ? (double)t_off / (double)t_on : 0.0);
    CHECK(sink == sink, "bench sums finite");
    tsdb_part_close(pon);
    tsdb_part_close(poff);
    free(vmetas); free(tmetas);
    tsdb_schema_free(s);

    /* ---- [5] SQL layer: ON vs OFF bit-identical, counter proves path ---- */
    printf("\n[5] SQL scan ON vs OFF\n");
    double sql_sum_on = 0.0, sql_sum_off = 0.0;
    int64_t cnt_on = -1, cnt_off = -1;
    double spot_on = -1.0, spot_off = -2.0;
    uint64_t hits_on_delta = 0, hits_off_delta = 0;

    for (int mode = 1; mode >= 0; mode--) {
        setenv("TSDB_ZEROCOPY_READ", mode ? "1" : "0", 1);
        tsdb_db_t *db = NULL;
        if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "reopen\n"); return 1; }
        tsdb_result_t *r = NULL;

        if (tsdb_query(db, "SELECT count(*) FROM t", &r) == TSDB_OK && r) {
            if (tsdb_result_next(r)) {
                if (mode) cnt_on = tsdb_result_i64(r, 0);
                else      cnt_off = tsdb_result_i64(r, 0);
            }
            tsdb_result_free(r); r = NULL;
        }

        /* WHERE defeats the stats fastpath → forces real block loads. */
        tsdb_part_zerocopy_stats(&h0, &f0);
        if (tsdb_query(db, "SELECT sum(val) FROM t WHERE ts >= 0", &r) == TSDB_OK && r) {
            if (tsdb_result_next(r)) {
                if (mode) sql_sum_on = tsdb_result_f64(r, 0);
                else      sql_sum_off = tsdb_result_f64(r, 0);
            }
            tsdb_result_free(r); r = NULL;
        }
        tsdb_part_zerocopy_stats(&h1, &f1);
        if (mode) hits_on_delta = h1 - h0; else hits_off_delta = h1 - h0;

        /* Row-scan spot check (unconverted row-emit path keeps the copy). */
        if (tsdb_query(db, "SELECT ts, val FROM t LIMIT 3", &r) == TSDB_OK && r) {
            if (tsdb_result_next(r)) {
                if (mode) spot_on = tsdb_result_f64(r, 1);
                else      spot_off = tsdb_result_f64(r, 1);
            }
            tsdb_result_free(r); r = NULL;
        }
        tsdb_close(db);
    }

    CHECK(cnt_on == NROWS && cnt_off == NROWS, "SQL: count(*) right under both settings");
    /* ON vs OFF must be BIT-identical: same engine, same summation order,
     * only the buffer source differs.  (No compare against ref_sum — the
     * parallel workers sum in a different order than the sequential
     * block loop, and float addition is not associative.) */
    CHECK(memcmp(&sql_sum_on, &sql_sum_off, 8) == 0, "SQL: sum(val) bit-identical ON vs OFF");
    CHECK(hits_on_delta > 0, "SQL ON: aggregate scan actually took the zero-copy path");
    CHECK(hits_off_delta == 0, "SQL OFF: kill switch kept the scan on the copy path");
    CHECK(memcmp(&spot_on, &spot_off, 8) == 0, "SQL: row-scan spot value identical ON vs OFF");

    unsetenv("TSDB_ZEROCOPY_READ");
    rmrf(dir);
    printf("\n=== RESULTS: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
