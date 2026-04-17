/* gorilla.c — Gorilla XOR encoding for float64 columns.
 *
 * Format:
 *   bytes 0..7  : first value, raw little-endian uint64.
 *   per value   :
 *     '0'               (1 bit): xor == 0 (repeat previous).
 *     '10' + meaningful : reuse previous (leading, trailing) window.
 *     '11' + 5b leading + 6b len + meaningful bits : new window.
 *
 * Convention: stored meaningful_len == 0 means 64 (to distinguish from no
 * meaningful bits, which is the xor==0 case handled separately).
 * leading is clamped to [0, 63]; trailing = 64 - leading - meaningful.
 */
#include "gorilla.h"
#include "../core/bits.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>

/* Reinterpret double <-> uint64 without UB. */
static inline uint64_t f64_to_u64(double v)
{
    uint64_t u;
    memcpy(&u, &v, 8);
    return u;
}

static inline double u64_to_f64(uint64_t u)
{
    double v;
    memcpy(&v, &u, 8);
    return v;
}

int tsdb_gorilla_encode(const double *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;
    if (cap < 8) return TSDB_ERR_OVERFLOW;

    /* Write first value as raw little-endian. */
    size_t pos = 0;
    uint64_t u0 = f64_to_u64(in[0]);
    for (int i = 0; i < 8; i++) out[pos++] = (uint8_t)(u0 >> (8 * i));

    if (n == 1) {
        *out_bytes = pos;
        return TSDB_OK;
    }

    tsdb_bw_t bw;
    tsdb_bw_init(&bw, out + pos, cap - pos);

    uint64_t prev_u = u0;
    unsigned prev_leading = 0;
    unsigned prev_trailing = 0;
    int has_prev_window = 0;

    for (size_t i = 1; i < n; i++) {
        uint64_t cur_u = f64_to_u64(in[i]);
        uint64_t xor_val = cur_u ^ prev_u;
        prev_u = cur_u;

        if (xor_val == 0) {
            tsdb_bw_put(&bw, 0, 1);
            continue;
        }

        unsigned leading = tsdb_clz64(xor_val);
        unsigned trailing = tsdb_ctz64(xor_val);

        /* Clamp leading to 5-bit representable range (0..31). */
        if (leading > 31) leading = 31;

        unsigned meaningful = 64 - leading - trailing;

        /* If we have a previous window and the meaningful bits fit inside it,
         * reuse the window (must have meaningful <= prev_meaningful). */
        if (has_prev_window) {
            unsigned prev_meaningful = 64 - prev_leading - prev_trailing;
            if (leading >= prev_leading && trailing >= prev_trailing) {
                /* Reuse: '10' + meaningful bits from prev_leading position. */
                tsdb_bw_put(&bw, 0x2, 2); /* bits: 01 (LSB first -> '10') */
                tsdb_bw_put(&bw, xor_val >> prev_trailing, prev_meaningful);
                continue;
            }
        }

        /* New window: '11' + 5b leading + 6b meaningful_len + bits. */
        tsdb_bw_put(&bw, 0x3, 2); /* bits: 11 */
        tsdb_bw_put(&bw, (uint64_t)leading, 5);
        /* Stored meaningful_len: 0 means 64. */
        uint64_t stored_len = (meaningful == 64) ? 0 : meaningful;
        tsdb_bw_put(&bw, stored_len, 6);
        tsdb_bw_put(&bw, xor_val >> trailing, meaningful);

        prev_leading = leading;
        prev_trailing = trailing;
        has_prev_window = 1;
    }

    ssize_t bits_bytes = tsdb_bw_finish(&bw);
    if (bits_bytes < 0) return TSDB_ERR_OVERFLOW;

    *out_bytes = pos + (size_t)bits_bytes;
    return TSDB_OK;
}

int tsdb_gorilla_decode(const uint8_t *in, size_t n_bytes, double *out, size_t n)
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
    unsigned prev_trailing = 0;
    unsigned prev_meaningful = 0;
    int has_prev_window = 0;

    for (size_t i = 1; i < n; i++) {
        uint64_t b0 = tsdb_br_get(&br, 1);
        uint64_t xor_val;

        if (b0 == 0) {
            xor_val = 0;
        } else {
            uint64_t b1 = tsdb_br_get(&br, 1);
            if (b1 == 0) {
                /* '10': reuse previous window. */
                if (!has_prev_window) return TSDB_ERR_CORRUPT;
                xor_val = tsdb_br_get(&br, prev_meaningful) << prev_trailing;
            } else {
                /* '11': new window. */
                unsigned leading = (unsigned)tsdb_br_get(&br, 5);
                unsigned stored_len = (unsigned)tsdb_br_get(&br, 6);
                unsigned meaningful = (stored_len == 0) ? 64 : stored_len;
                unsigned trailing = 64 - leading - meaningful;
                if (leading + meaningful > 64) return TSDB_ERR_CORRUPT;
                xor_val = tsdb_br_get(&br, meaningful) << trailing;
                (void)leading; /* leading is implicitly baked into trailing */
                prev_trailing = trailing;
                prev_meaningful = meaningful;
                has_prev_window = 1;
            }
        }

        if (br.underflow) return TSDB_ERR_CORRUPT;
        uint64_t cur_u = prev_u ^ xor_val;
        out[i] = u64_to_f64(cur_u);
        prev_u = cur_u;
    }

    return TSDB_OK;
}
