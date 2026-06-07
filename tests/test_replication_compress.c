/* test_replication_compress.c — WRITE_BATCH lzlite roundtrip for replication.
 *
 * Proves the iter-11 Kafka-style replication compression is lossless:
 *   build columnar batch → encode_write_batch → lzlite_encode →
 *   lzlite_decode → assert byte-identical → decode_write_batch → assert
 *   table_name / ncols / nrows / column bytes all match the original.
 *
 * Also asserts the payload actually compresses (ratio < 0.9) on the kind of
 * data the loader produces (ts = +1 ms arithmetic run, val = random walk),
 * since the sender only ships WRITE_BATCH_LZ when it saved >10%.
 */
#define _POSIX_C_SOURCE 200809L

#include "../include/tsdb.h"
#include "../src/core/types.h"
#include "../src/cluster/rpc.h"
#include "../src/compress/lzlite.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#define ASSERT(c) do { if (!(c)) { \
    fprintf(stderr, "ASSERT FAILED: %s  [%s:%d]\n", #c, __FILE__, __LINE__); abort(); } } while (0)

#define N_ROWS 4096

int main(void) {
    printf("=== test_replication_compress ===\n");

    /* Build the same column shape the loader replicates: ts (TIMESTAMP) +
     * val (FLOAT64), N rows. */
    int64_t *ts  = malloc(N_ROWS * sizeof(int64_t));
    double  *val = malloc(N_ROWS * sizeof(double));
    ASSERT(ts && val);
    uint64_t seed = 0x1234567;
    double w = 0;
    for (int i = 0; i < N_ROWS; i++) {
        ts[i] = 1000000000000LL + (int64_t)i * 1000000LL;   /* +1 ms */
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        w += ((double)((seed >> 33) % 201) - 100.0) * 0.01;  /* random walk */
        val[i] = w;
    }

    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_FLOAT64 };
    const void *col_data[2] = { ts, val };

    /* 1. encode raw WRITE_BATCH payload */
    size_t cap = 8 + 2 * 2 + (size_t)N_ROWS * 16 + 256;
    uint8_t *raw = malloc(cap);
    ASSERT(raw);
    int plen = tsdb_rpc_encode_write_batch(raw, (uint32_t)cap, "lt_0",
                                           2, col_types, N_ROWS, col_data);
    ASSERT(plen > 0);
    printf("  raw WRITE_BATCH payload: %d bytes\n", plen);

    /* 2. lzlite compress */
    size_t lzcap = tsdb_lzlite_max_output((size_t)plen);
    uint8_t *comp = malloc(lzcap);
    ASSERT(comp);
    size_t complen = 0;
    /* encode returns bytes written (>=0) on success, negative on error. */
    ASSERT(tsdb_lzlite_encode(raw, (size_t)plen, comp, lzcap, &complen) >= 0);
    double ratio = (double)complen / (double)plen;
    printf("  compressed: %zu bytes (ratio %.3f, %.1f%% saved)\n",
           complen, ratio, (1.0 - ratio) * 100.0);
    ASSERT(complen < (size_t)plen);          /* it shrank */
    ASSERT(ratio < 0.9);                      /* enough to clear the sender gate */

    /* 3. lzlite decompress → must be byte-identical to raw */
    uint8_t *raw2 = malloc((size_t)plen);
    ASSERT(raw2);
    size_t got = 0;
    ASSERT(tsdb_lzlite_decode(comp, complen, raw2, (size_t)plen, &got) == TSDB_OK);
    ASSERT(got == (size_t)plen);
    ASSERT(memcmp(raw, raw2, (size_t)plen) == 0);
    printf("  decompress: byte-identical to original (%zu bytes)\n", got);

    /* 4. decode the decompressed payload → fields + column bytes intact */
    char tbl[64] = {0};
    int dncols = 0, dnrows = 0;
    int dtypes[TSDB_MAX_COLS];
    uint8_t *dcol[TSDB_MAX_COLS] = {0};
    ASSERT(tsdb_rpc_decode_write_batch(raw2, (uint32_t)plen, tbl, sizeof(tbl),
                                       &dncols, dtypes, &dnrows,
                                       (uint8_t **)dcol) == 0);
    ASSERT(strcmp(tbl, "lt_0") == 0);
    ASSERT(dncols == 2);
    ASSERT(dnrows == N_ROWS);
    ASSERT(dtypes[0] == TSDB_TYPE_TIMESTAMP && dtypes[1] == TSDB_TYPE_FLOAT64);

    /* Column layout: ts at offset 0 (N×8), val at offset N×8 (N×8). */
    const int64_t *ts_out  = (const int64_t *)(dcol[0]);
    const double  *val_out = (const double  *)(dcol[0] + (size_t)N_ROWS * 8);
    ASSERT(memcmp(ts_out, ts, (size_t)N_ROWS * 8) == 0);
    ASSERT(memcmp(val_out, val, (size_t)N_ROWS * 8) == 0);
    printf("  decode: table=%s ncols=%d nrows=%d, ts+val bytes match\n",
           tbl, dncols, dnrows);

    free(ts); free(val); free(raw); free(comp); free(raw2);
    printf("[PASS] replication compression roundtrip is lossless\n");
    return 0;
}
