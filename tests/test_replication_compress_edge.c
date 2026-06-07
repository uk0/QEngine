/* test_replication_compress_edge.c — edge cases for WRITE_BATCH lzlite compression.
 *
 * Companion to test_replication_compress.c (the happy-path roundtrip).  This
 * file hardens the iter-11 replication compression against the cases the sender
 * gate and the receiver decode path actually have to survive:
 *
 *   (a) RATIO GATE   — incompressible payload does NOT shrink past the
 *                      sender's ">10% saved" gate, so the leader correctly
 *                      ships RAW (type 1) instead of WRITE_BATCH_LZ.
 *   (b) CORRUPT      — garbled compressed bytes decode without overflowing the
 *                      out buffer (negative code or bounded success, no crash).
 *   (c) TRUNCATED    — a valid stream with in_n cut short → no crash/overrun.
 *   (d) OUT-TOO-SMALL— out_cap below the true decompressed size → OVERFLOW.
 *   (e) SYMBOL COL   — WRITE_BATCH with a SYMBOL col (wire: [u32 total][u16
 *                      len][bytes]…) + a TIMESTAMP col survives
 *                      encode_write_batch → lz encode → lz decode (byte-
 *                      identical) → decode_write_batch.
 *   (f) TINY         — a sub-1KB payload (below the sender size threshold) still
 *                      roundtrips losslessly.
 *
 * The decoder is fully bounds-checked (see lzlite.c), so for (b)/(c) the only
 * guarantee that always holds is "never writes past out_cap and never reads
 * past in_n" — we assert that with a guard canary plus, where the corruption
 * is strong enough to be unambiguous, a NEGATIVE return.
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

/* splitmix64 — a strong PRNG.  Its output is high-entropy, so a buffer filled
 * with it has essentially no LZ-exploitable redundancy. */
static uint64_t splitmix64(uint64_t *s) {
    uint64_t z = (*s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* ---- (a) ratio gate: incompressible data must NOT clear the sender gate. */
static void test_ratio_gate(void) {
    const size_t plen = 4096;                 /* > 1KB sender threshold */
    uint8_t *raw = malloc(plen);
    ASSERT(raw);
    uint64_t s = 0xCAFEF00DD15EA5E5ULL;
    for (size_t i = 0; i < plen; i++)
        raw[i] = (uint8_t)(splitmix64(&s) >> 24);

    size_t cap = tsdb_lzlite_max_output(plen);
    uint8_t *comp = malloc(cap);
    ASSERT(comp);
    size_t complen = 0;
    ASSERT(tsdb_lzlite_encode(raw, plen, comp, cap, &complen) >= 0);

    /* The sender (replica.c) ships WRITE_BATCH_LZ only when
     *   (4 + complen) < plen * 9 / 10.
     * For incompressible input that gate must be FALSE, i.e. the wire size
     * (4-byte orig_len prefix + compressed body) is NOT below 90% of raw. */
    size_t wire = 4 + complen;
    printf("  (a) ratio gate: raw=%zu wire=%zu (gate ships LZ iff %zu < %zu)\n",
           plen, wire, wire, plen * 9 / 10);
    ASSERT(wire >= plen * 9 / 10);   /* gate is FALSE → leader ships RAW */

    free(raw);
    free(comp);
}

/* ---- (b) corrupt stream: garble bytes, decode must stay in-bounds. */
static void test_corrupt_stream(void) {
    /* Build a real compressible payload so the stream has tokens/offsets
     * worth corrupting (a +Δ ts run + a correlated walk, like the loader). */
    const int N = 512;
    int64_t *ts  = malloc((size_t)N * 8);
    double  *val = malloc((size_t)N * 8);
    ASSERT(ts && val);
    double w = 0;
    for (int i = 0; i < N; i++) {
        ts[i] = 1000000000000LL + (int64_t)i * 1000000LL;
        w += (double)(i % 7) * 0.5;
        val[i] = w;
    }
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_FLOAT64 };
    const void *col_data[2] = { ts, val };

    size_t bcap = 8 + 2 * 2 + (size_t)N * 16 + 256;
    uint8_t *raw = malloc(bcap);
    ASSERT(raw);
    int plen = tsdb_rpc_encode_write_batch(raw, (uint32_t)bcap, "t",
                                           2, col_types, N, col_data);
    ASSERT(plen > 0);

    size_t cap = tsdb_lzlite_max_output((size_t)plen);
    uint8_t *comp = malloc(cap);
    ASSERT(comp);
    size_t complen = 0;
    ASSERT(tsdb_lzlite_encode(raw, (size_t)plen, comp, cap, &complen) >= 0);
    ASSERT(complen >= 4);

    /* Out buffer sized to the true decompressed length + a guard canary just
     * past the end; a correct decoder must never touch the canary. */
    const uint8_t CANARY = 0xA5;
    uint8_t *out = malloc((size_t)plen + 1);
    ASSERT(out);

    /* Many independent garbles of the compressed stream. */
    uint64_t s = 0x123456789ABCDEF0ULL;
    int saw_negative = 0;
    for (int trial = 0; trial < 200; trial++) {
        uint8_t *bad = malloc(complen);
        ASSERT(bad);
        memcpy(bad, comp, complen);
        /* Flip 1-3 random bytes. */
        int flips = 1 + (int)(splitmix64(&s) % 3);
        for (int f = 0; f < flips; f++) {
            size_t idx = (size_t)(splitmix64(&s) % complen);
            bad[idx] ^= (uint8_t)(splitmix64(&s) | 1);
        }
        memset(out, 0, (size_t)plen);
        out[plen] = CANARY;
        size_t got = 0;
        int rc = tsdb_lzlite_decode(bad, complen, out, (size_t)plen, &got);
        /* Contract that ALWAYS holds: no out-of-bounds write (canary intact)
         * and on success the reported size never exceeds out_cap. */
        ASSERT(out[plen] == CANARY);
        if (rc < 0) saw_negative = 1;
        else        ASSERT(got <= (size_t)plen);
        free(bad);
    }
    /* Garbling a compressed stream is overwhelmingly likely to break framing;
     * require at least one trial to have been rejected with a negative code. */
    ASSERT(saw_negative);
    printf("  (b) corrupt: 200 garbles bounded, canary intact, "
           "negatives observed\n");

    /* A deliberately unambiguous corruption: force a back-ref offset that
     * points before the output start.  Craft the first sequence to claim a
     * zero-literal run + a 15-nibble match (overflow varint = +0 → match_len
     * = MIN_MATCH+15 = 19) with offset 0xFFFF.  At op==0 any nonzero offset
     * exceeds op, so the decoder MUST return CORRUPT (offset > op). */
    if (complen >= 4) {
        uint8_t *bad = malloc(complen);
        ASSERT(bad);
        memcpy(bad, comp, complen);
        bad[0] = 0x0F;       /* token: lit_nib=0, match_nib=15 (overflow)   */
        bad[1] = 0x00;       /* match overflow varint = +0 → match_len = 19 */
        bad[2] = 0xFF;       /* offset LO                                   */
        bad[3] = 0xFF;       /* offset HI → offset 65535 > op(0)            */
        out[plen] = CANARY;
        size_t got = 0;
        int rc = tsdb_lzlite_decode(bad, complen, out, (size_t)plen, &got);
        ASSERT(out[plen] == CANARY);
        ASSERT(rc < 0);          /* offset 65535 > current op → CORRUPT */
        printf("  (b') forced bad back-ref offset → rc=%d (negative)\n", rc);
        free(bad);
    }

    free(ts); free(val); free(raw); free(comp); free(out);
}

/* ---- (c) truncated stream: valid bytes but in_n cut short. */
static void test_truncated_stream(void) {
    /* A run-length payload: 4096 identical bytes compress to a tiny stream
     * dominated by an overflow match-length varint — truncating it mid-varint
     * or mid-offset must be caught, never looped or overrun. */
    const size_t plen = 4096;
    uint8_t *raw = malloc(plen);
    ASSERT(raw);
    memset(raw, 0x7C, plen);

    size_t cap = tsdb_lzlite_max_output(plen);
    uint8_t *comp = malloc(cap);
    ASSERT(comp);
    size_t complen = 0;
    ASSERT(tsdb_lzlite_encode(raw, plen, comp, cap, &complen) >= 0);
    ASSERT(complen > 1);

    const uint8_t CANARY = 0x5A;
    uint8_t *out = malloc(plen + 1);
    ASSERT(out);

    int saw_negative = 0;
    /* Try every truncation length from 1 .. complen-1. */
    for (size_t cut = 1; cut < complen; cut++) {
        memset(out, 0, plen);
        out[plen] = CANARY;
        size_t got = 0;
        int rc = tsdb_lzlite_decode(comp, cut, out, plen, &got);
        ASSERT(out[plen] == CANARY);          /* never overran */
        if (rc < 0) saw_negative = 1;
        else        ASSERT(got <= plen);
    }
    /* This stream's tail is a multi-byte match overflow + 2-byte offset, so at
     * least one short cut must land mid-token and be rejected. */
    ASSERT(saw_negative);
    printf("  (c) truncated: all %zu short cuts bounded, negatives observed\n",
           complen - 1);

    free(raw); free(comp); free(out);
}

/* ---- (d) out buffer too small: decode must report OVERFLOW. */
static void test_out_too_small(void) {
    const size_t plen = 2048;
    uint8_t *raw = malloc(plen);
    ASSERT(raw);
    memset(raw, 0x33, plen);               /* highly compressible */

    size_t cap = tsdb_lzlite_max_output(plen);
    uint8_t *comp = malloc(cap);
    ASSERT(comp);
    size_t complen = 0;
    ASSERT(tsdb_lzlite_encode(raw, plen, comp, cap, &complen) >= 0);

    /* Decode into a buffer half the true size — the overflow guard fires. */
    size_t small = plen / 2;
    uint8_t *out = malloc(small);
    ASSERT(out);
    size_t got = 0;
    int rc = tsdb_lzlite_decode(comp, complen, out, small, &got);
    ASSERT(rc == TSDB_ERR_OVERFLOW);
    printf("  (d) out too small: out_cap=%zu true=%zu → rc=TSDB_ERR_OVERFLOW\n",
           small, plen);

    free(raw); free(comp); free(out);
}

/* Build a SYMBOL column buffer in the wire format the cluster uses:
 *   [u32 total][u16 len][bytes][u16 len][bytes]…
 * where `total` counts every byte after the u32.  Returns malloc'd buffer,
 * sets *out_len to its full length (4 + total). */
static uint8_t *build_symbol_col(const char *const *vals, int n, size_t *out_len) {
    /* First pass: size. */
    uint32_t total = 0;
    for (int i = 0; i < n; i++)
        total += 2 + (uint32_t)strlen(vals[i]);
    size_t full = 4 + total;
    uint8_t *buf = malloc(full);
    ASSERT(buf);
    memcpy(buf, &total, 4);
    uint8_t *p = buf + 4;
    for (int i = 0; i < n; i++) {
        uint16_t len = (uint16_t)strlen(vals[i]);
        memcpy(p, &len, 2); p += 2;
        memcpy(p, vals[i], len); p += len;
    }
    ASSERT((size_t)(p - buf) == full);
    *out_len = full;
    return buf;
}

/* ---- (e) SYMBOL column roundtrip through the full compress pipeline. */
static void test_symbol_col_roundtrip(void) {
    const int N = 6;
    int64_t ts[6];
    for (int i = 0; i < N; i++)
        ts[i] = 1700000000000000000LL + (int64_t)i * 1000000LL;

    const char *syms[6] = { "host-a", "host-bb", "host-a", "x", "", "host-ccc" };
    size_t sym_len = 0;
    uint8_t *sym_buf = build_symbol_col(syms, N, &sym_len);

    /* Column order: ts (TIMESTAMP), tag (SYMBOL). */
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_SYMBOL };
    const void *col_data[2] = { ts, sym_buf };

    size_t bcap = 8 + 2 * 2 + (size_t)N * 8 + sym_len + 256;
    uint8_t *raw = malloc(bcap);
    ASSERT(raw);
    int plen = tsdb_rpc_encode_write_batch(raw, (uint32_t)bcap, "sym_tbl",
                                           2, col_types, N, col_data);
    ASSERT(plen > 0);

    /* lzlite roundtrip → byte-identical. */
    size_t cap = tsdb_lzlite_max_output((size_t)plen);
    uint8_t *comp = malloc(cap);
    ASSERT(comp);
    size_t complen = 0;
    ASSERT(tsdb_lzlite_encode(raw, (size_t)plen, comp, cap, &complen) >= 0);
    uint8_t *raw2 = malloc((size_t)plen);
    ASSERT(raw2);
    size_t got = 0;
    ASSERT(tsdb_lzlite_decode(comp, complen, raw2, (size_t)plen, &got) == TSDB_OK);
    ASSERT(got == (size_t)plen);
    ASSERT(memcmp(raw, raw2, (size_t)plen) == 0);

    /* decode_write_batch on the decompressed payload → header fields match. */
    char tbl[64] = {0};
    int dncols = 0, dnrows = 0;
    int dtypes[TSDB_MAX_COLS];
    uint8_t *dcol[TSDB_MAX_COLS] = {0};
    ASSERT(tsdb_rpc_decode_write_batch(raw2, (uint32_t)plen, tbl, sizeof(tbl),
                                       &dncols, dtypes, &dnrows,
                                       (uint8_t **)dcol) == 0);
    ASSERT(strcmp(tbl, "sym_tbl") == 0);
    ASSERT(dncols == 2);
    ASSERT(dnrows == N);
    ASSERT(dtypes[0] == TSDB_TYPE_TIMESTAMP && dtypes[1] == TSDB_TYPE_SYMBOL);

    /* Columns are contiguous from dcol[0]: ts (N×8) then the symbol blob. */
    const int64_t *ts_out = (const int64_t *)dcol[0];
    ASSERT(memcmp(ts_out, ts, (size_t)N * 8) == 0);
    const uint8_t *sym_out = dcol[0] + (size_t)N * 8;
    ASSERT(memcmp(sym_out, sym_buf, sym_len) == 0);   /* [u32 total][u16 len]… */
    printf("  (e) symbol col: table=%s ncols=%d nrows=%d, ts+symbol bytes match "
           "(sym blob %zu B)\n", tbl, dncols, dnrows, sym_len);

    free(sym_buf); free(raw); free(comp); free(raw2);
}

/* ---- (f) tiny payload (< 1KB sender threshold) still roundtrips. */
static void test_tiny_roundtrip(void) {
    /* 3 rows × (ts + val) = ~ tens of bytes, well under the 1024-byte gate. */
    const int N = 3;
    int64_t ts[3]  = { 10, 20, 30 };
    double  val[3] = { 1.5, 2.5, 3.5 };
    int col_types[2] = { TSDB_TYPE_TIMESTAMP, TSDB_TYPE_FLOAT64 };
    const void *col_data[2] = { ts, val };

    uint8_t raw[256];
    int plen = tsdb_rpc_encode_write_batch(raw, sizeof(raw), "tiny",
                                           2, col_types, N, col_data);
    ASSERT(plen > 0);
    ASSERT(plen < 1024);                  /* genuinely below the sender gate */

    size_t cap = tsdb_lzlite_max_output((size_t)plen);
    uint8_t *comp = malloc(cap);
    ASSERT(comp);
    size_t complen = 0;
    ASSERT(tsdb_lzlite_encode(raw, (size_t)plen, comp, cap, &complen) >= 0);

    uint8_t raw2[256];
    size_t got = 0;
    ASSERT(tsdb_lzlite_decode(comp, complen, raw2, (size_t)plen, &got) == TSDB_OK);
    ASSERT(got == (size_t)plen);
    ASSERT(memcmp(raw, raw2, (size_t)plen) == 0);

    char tbl[64] = {0};
    int dncols = 0, dnrows = 0;
    int dtypes[TSDB_MAX_COLS];
    uint8_t *dcol[TSDB_MAX_COLS] = {0};
    ASSERT(tsdb_rpc_decode_write_batch(raw2, (uint32_t)plen, tbl, sizeof(tbl),
                                       &dncols, dtypes, &dnrows,
                                       (uint8_t **)dcol) == 0);
    ASSERT(strcmp(tbl, "tiny") == 0 && dncols == 2 && dnrows == N);
    const int64_t *ts_out  = (const int64_t *)dcol[0];
    const double  *val_out = (const double  *)(dcol[0] + (size_t)N * 8);
    ASSERT(memcmp(ts_out, ts, (size_t)N * 8) == 0);
    ASSERT(memcmp(val_out, val, (size_t)N * 8) == 0);
    printf("  (f) tiny payload: %d bytes roundtrips losslessly\n", plen);

    free(comp);
}

int main(void) {
    printf("=== test_replication_compress_edge ===\n");
    test_ratio_gate();
    test_corrupt_stream();
    test_truncated_stream();
    test_out_too_small();
    test_symbol_col_roundtrip();
    test_tiny_roundtrip();
    printf("[PASS] replication compression edge cases hold\n");
    return 0;
}
