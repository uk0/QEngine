/* chimp.c — Chimp XOR encoding for float64 columns.
 *
 * Reference: Panagiotis Liakos et al. "Chimp: Efficient Lossless Floating
 * Point Compression for Time Series Databases," VLDB 2022.
 *
 * Wire format:
 *   bytes 0..7  : first value, raw little-endian uint64.
 *   per value (i >= 1):
 *     '00' (2 bits): XOR = 0 (exact repeat of previous).
 *     '01' (2 bits): trailing-zero strip case.
 *         3 bits : leading-zero LUT index (0..7)
 *         6 bits : significant length (stored_len; 0 means 64)
 *         N bits : xor >> trailing  (N = significant length)
 *         After emitting '01', prev_leading is reset to SENTINEL (no reuse).
 *     '10' (2 bits): reuse previous leading zeros.
 *         (64 - prev_leading) bits : xor with leading zeros elided
 *     '11' (2 bits): full — new leading, no trailing strip.
 *         3 bits : leading-zero LUT index (0..7)
 *         (64 - rounded_leading) bits : xor >> 0 (trailing zeros kept)
 *         After emitting '11', prev_leading is set to rounded_leading.
 *
 * Dispatch rule (from the paper):
 *   if xor == 0              → '00'
 *   else if trailing >= 7    → '01' (strip trailing zeros)
 *   else if leading_round == prev_leading → '10' (reuse leading)
 *   else                     → '11' (new window, no strip)
 *
 * Leading-zero LUT (8 buckets, 3-bit index):
 *   clz  0..7  → bucket 0 (leading=0)
 *   clz  8..11 → bucket 1 (leading=8)
 *   clz 12..15 → bucket 2 (leading=12)
 *   clz 16..17 → bucket 3 (leading=16)
 *   clz 18..19 → bucket 4 (leading=18)
 *   clz 20..21 → bucket 5 (leading=20)
 *   clz 22..23 → bucket 6 (leading=22)
 *   clz 24+    → bucket 7 (leading=24)
 *
 * NOTE: tsdb_bw_put / tsdb_br_get are limited to safe 56-bit single calls.
 * We replicate gorilla.c's bw_put_safe / br_get_safe helpers to split
 * writes/reads > 32 bits into two chunks.
 */
#include "chimp.h"
#include "../core/bits.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>

/* ------------------------------------------------------------------ helpers */

static inline uint64_t f64_to_u64(double v) {
    uint64_t u; memcpy(&u, &v, 8); return u;
}

static inline double u64_to_f64(uint64_t u) {
    double v; memcpy(&v, &u, 8); return v;
}

/* Safe write of up to 64 bits (split at 32). */
static void bw_put_safe(tsdb_bw_t *bw, uint64_t val, unsigned n) {
    if (n == 0) return;
    if (n <= 32) { tsdb_bw_put(bw, val, n); return; }
    tsdb_bw_put(bw, val & 0xFFFFFFFFULL, 32);
    tsdb_bw_put(bw, val >> 32, n - 32);
}

/* Safe read of up to 64 bits (split at 32). */
static uint64_t br_get_safe(tsdb_br_t *br, unsigned n) {
    if (n == 0) return 0;
    if (n <= 32) return tsdb_br_get(br, n);
    uint64_t lo = tsdb_br_get(br, 32);
    uint64_t hi = tsdb_br_get(br, n - 32);
    return lo | (hi << 32);
}

/* ------------------------------------------------------------------ LUT */

/* Map clz(xor) → rounded leading count (one of 8 buckets). */
static const uint8_t CHIMP_LEADING_ROUND[65] = {
    /* 0..7  */ 0, 0, 0, 0, 0, 0, 0, 0,
    /* 8..11 */ 8, 8, 8, 8,
    /* 12..15 */ 12, 12, 12, 12,
    /* 16..17 */ 16, 16,
    /* 18..19 */ 18, 18,
    /* 20..21 */ 20, 20,
    /* 22..23 */ 22, 22,
    /* 24..63 */ 24, 24, 24, 24, 24, 24, 24, 24,
                 24, 24, 24, 24, 24, 24, 24, 24,
                 24, 24, 24, 24, 24, 24, 24, 24,
                 24, 24, 24, 24, 24, 24, 24, 24,
                 24, 24, 24, 24, 24, 24, 24, 24,
    /* 64 (xor==0 degenerate, never used on this path) */ 24
};

/* Map rounded leading count → 3-bit LUT index. */
static const uint8_t CHIMP_LEADING_IDX[25] = {
    /* 0  */ 0,
    /* 1..7 */ 0, 0, 0, 0, 0, 0, 0,
    /* 8  */ 1,
    /* 9..11 */ 1, 1, 1,
    /* 12 */ 2,
    /* 13..15 */ 2, 2, 2,
    /* 16 */ 3,
    /* 17 */ 3,
    /* 18 */ 4,
    /* 19 */ 4,
    /* 20 */ 5,
    /* 21 */ 5,
    /* 22 */ 6,
    /* 23 */ 6,
    /* 24 */ 7
};

/* Inverse: 3-bit index → rounded leading count. */
static const uint8_t CHIMP_IDX_TO_LEADING[8] = {
    0, 8, 12, 16, 18, 20, 22, 24
};

/* Sentinel value meaning "no valid prev_leading, force '11' on next value". */
#define CHIMP_NO_PREV_LEADING 65u

/* Trailing-zero strip threshold (strip when trailing >= this). */
#define CHIMP_TRAIL_THRESH 7u

/* ------------------------------------------------------------------ encode */

int tsdb_chimp_encode(const double *in, size_t n, uint8_t *out, size_t cap,
                      size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;
    if (cap < 8) return TSDB_ERR_OVERFLOW;

    /* Write first value as raw LE uint64. */
    size_t pos = 0;
    uint64_t u0 = f64_to_u64(in[0]);
    for (int i = 0; i < 8; i++) out[pos++] = (uint8_t)(u0 >> (8 * i));

    if (n == 1) { *out_bytes = pos; return TSDB_OK; }

    tsdb_bw_t bw;
    tsdb_bw_init(&bw, out + pos, cap - pos);

    uint64_t prev_u = u0;
    unsigned prev_leading = CHIMP_NO_PREV_LEADING; /* sentinel: no valid window yet */

    for (size_t i = 1; i < n; i++) {
        uint64_t cur_u = f64_to_u64(in[i]);
        uint64_t xor_val = cur_u ^ prev_u;
        prev_u = cur_u;

        if (xor_val == 0) {
            /* Case '00': identical value. */
            tsdb_bw_put(&bw, 0, 2);
            continue;
        }

        unsigned clz = tsdb_clz64(xor_val);
        unsigned ctz = tsdb_ctz64(xor_val);
        unsigned lead_round = CHIMP_LEADING_ROUND[clz < 65 ? clz : 64];
        unsigned lead_idx   = CHIMP_LEADING_IDX[lead_round];

        if (ctz >= CHIMP_TRAIL_THRESH) {
            /* Case '01': strip trailing zeros. */
            unsigned sig_len = 64u - lead_round - ctz;
            /* stored_len: 0 encodes 64; sig_len is in [1..64] */
            unsigned stored_len = (sig_len == 64u) ? 0u : sig_len;

            tsdb_bw_put(&bw, 1, 2);                         /* '01' */
            tsdb_bw_put(&bw, lead_idx, 3);                  /* leading LUT index */
            tsdb_bw_put(&bw, stored_len, 6);                /* significant length */
            bw_put_safe(&bw, xor_val >> ctz, sig_len);      /* meaningful bits */

            /* After '01', invalidate prev_leading so next uses '11'. */
            prev_leading = CHIMP_NO_PREV_LEADING;

        } else if (prev_leading != CHIMP_NO_PREV_LEADING &&
                   lead_round == prev_leading) {
            /* Case '10': reuse previous leading zeros count. */
            unsigned bits = 64u - prev_leading;

            tsdb_bw_put(&bw, 2, 2);                         /* '10' */
            bw_put_safe(&bw, xor_val, bits);                /* trailing kept */

        } else {
            /* Case '11': new leading, no trailing strip. */
            unsigned bits = 64u - lead_round;

            tsdb_bw_put(&bw, 3, 2);                         /* '11' */
            tsdb_bw_put(&bw, lead_idx, 3);                  /* leading LUT index */
            bw_put_safe(&bw, xor_val, bits);                /* full (trailing kept) */

            prev_leading = lead_round;
        }
    }

    ssize_t bits_bytes = tsdb_bw_finish(&bw);
    if (bits_bytes < 0) return TSDB_ERR_OVERFLOW;

    *out_bytes = pos + (size_t)bits_bytes;
    return TSDB_OK;
}

/* ------------------------------------------------------------------ decode */

int tsdb_chimp_decode(const uint8_t *in, size_t n_bytes, double *out, size_t n)
{
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;
    if (n_bytes < 8) return TSDB_ERR_CORRUPT;

    /* Read first value. */
    size_t pos = 0;
    uint64_t u0 = 0;
    for (int i = 0; i < 8; i++) u0 |= (uint64_t)in[pos++] << (8 * i);
    out[0] = u64_to_f64(u0);

    if (n == 1) return TSDB_OK;

    tsdb_br_t br;
    tsdb_br_init(&br, in + pos, n_bytes - pos);

    uint64_t prev_u = u0;
    unsigned prev_leading = CHIMP_NO_PREV_LEADING;

    for (size_t i = 1; i < n; i++) {
        uint64_t flag = tsdb_br_get(&br, 2);
        uint64_t xor_val;

        switch (flag) {
        case 0: /* '00': exact repeat */
            xor_val = 0;
            break;

        case 1: { /* '01': trailing-zero strip case */
            unsigned lead_idx  = (unsigned)tsdb_br_get(&br, 3);
            unsigned stored_len = (unsigned)tsdb_br_get(&br, 6);
            unsigned lead_round = CHIMP_IDX_TO_LEADING[lead_idx & 7];
            unsigned sig_len    = (stored_len == 0) ? 64u : stored_len;
            unsigned ctz        = 64u - lead_round - sig_len;

            if (lead_round + sig_len > 64u) return TSDB_ERR_CORRUPT;

            uint64_t sig = br_get_safe(&br, sig_len);
            xor_val = sig << ctz;

            /* Invalidate prev_leading (matching encoder). */
            prev_leading = CHIMP_NO_PREV_LEADING;
            break;
        }

        case 2: { /* '10': reuse previous leading zeros */
            if (prev_leading == CHIMP_NO_PREV_LEADING) return TSDB_ERR_CORRUPT;
            unsigned bits = 64u - prev_leading;
            xor_val = br_get_safe(&br, bits);
            break;
        }

        case 3: { /* '11': new leading, full XOR (trailing kept) */
            unsigned lead_idx  = (unsigned)tsdb_br_get(&br, 3);
            unsigned lead_round = CHIMP_IDX_TO_LEADING[lead_idx & 7];
            unsigned bits       = 64u - lead_round;

            xor_val = br_get_safe(&br, bits);

            prev_leading = lead_round;
            break;
        }

        default:
            return TSDB_ERR_CORRUPT;
        }

        if (br.underflow) return TSDB_ERR_CORRUPT;

        uint64_t cur_u = prev_u ^ xor_val;
        out[i] = u64_to_f64(cur_u);
        prev_u = cur_u;
    }

    return TSDB_OK;
}
