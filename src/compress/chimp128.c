/* chimp128.c — Chimp128 XOR encoding for float64 columns.
 *
 * Reference: Panagiotis Liakos et al. "Chimp: Efficient Lossless Floating
 * Point Compression for Time Series Databases," VLDB 2022.
 *
 * Chimp128 extends Chimp with a 128-element sliding ring buffer. For each
 * value, we consider the hash-indexed reference from the ring in addition to
 * the immediately previous value. We select the reference that minimizes the
 * encoded bit count.
 *
 * Wire format (LSB-first bitstream, after the 8-byte raw first value):
 *
 * The decoder reads a 2-bit prefix; if prefix == 0 it's the 2-bit exact-repeat
 * case. Otherwise it reads 1 more bit and reconstructs flag = prefix | (bit2<<2).
 *
 * Flag assignments (encoder writes these 3-bit values via bw_put(flag, 3) or
 * 2-bit value via bw_put(0, 2); decoder reconstructs the same flag value):
 *
 *   flag 0  (2 bits, value 0):  exact repeat (XOR with prev == 0)
 *   flag 1  (3 bits, value 1):  ring ref, new leading window
 *                               + 7b ring_idx + 3b lead_lut + 6b sig_len + sig_len bits
 *   flag 2  (3 bits, value 2):  ring ref, reuse leading
 *                               + 7b ring_idx + (64-prev_leading) bits
 *   flag 3  (3 bits, value 3):  prev ref, new leading, no trailing strip
 *                               + 3b lead_lut + (64-lead_round) bits
 *   flag 5  (3 bits, value 5):  prev ref, trailing-strip
 *                               + 3b lead_lut + 6b sig_len + sig_len bits
 *   flag 6  (3 bits, value 6):  prev ref, reuse leading
 *                               + (64-prev_leading) bits
 *
 * NOTE: Flag value 4 (binary 100, LSB-first prefix=00) conflicts with the
 * 2-bit exact-repeat case and is NOT used.
 *
 * Bit-cost summary (ring path saves when XOR leading zeros >> prev XOR):
 *   flag 0: 2
 *   flag 1: 3 + 7 + 3 + 6 + sig_len
 *   flag 2: 3 + 7 + (64 - prev_leading)
 *   flag 3: 3 + 3 + (64 - lead_round)
 *   flag 5: 3 + 3 + 6 + sig_len   (sig_len = 64 - lead_round - ctz)
 *   flag 6: 3 + (64 - prev_leading)
 *
 * NOTE: tsdb_bw_put / tsdb_br_get are limited to 32 bits per call safely.
 * bw_put_safe / br_get_safe split larger writes at 32 bits.
 */
#include "chimp128.h"
#include "../core/bits.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

/* ------------------------------------------------------------------ helpers */

static inline uint64_t f64_to_u64(double v) {
    uint64_t u; memcpy(&u, &v, 8); return u;
}

static inline double u64_to_f64(uint64_t u) {
    double v; memcpy(&v, &u, 8); return v;
}

static void bw_put_safe(tsdb_bw_t *bw, uint64_t val, unsigned n) {
    if (n == 0) return;
    if (n <= 32) { tsdb_bw_put(bw, val, n); return; }
    tsdb_bw_put(bw, val & 0xFFFFFFFFULL, 32);
    tsdb_bw_put(bw, val >> 32, n - 32);
}

static uint64_t br_get_safe(tsdb_br_t *br, unsigned n) {
    if (n == 0) return 0;
    if (n <= 32) return tsdb_br_get(br, n);
    uint64_t lo = tsdb_br_get(br, 32);
    uint64_t hi = tsdb_br_get(br, n - 32);
    return lo | (hi << 32);
}

/* ------------------------------------------------------------------ LUT */

static const uint8_t C128_LEADING_ROUND[65] = {
    0, 0, 0, 0, 0, 0, 0, 0,          /* 0..7  */
    8, 8, 8, 8,                        /* 8..11 */
    12, 12, 12, 12,                    /* 12..15 */
    16, 16,                            /* 16..17 */
    18, 18,                            /* 18..19 */
    20, 20,                            /* 20..21 */
    22, 22,                            /* 22..23 */
    24, 24, 24, 24, 24, 24, 24, 24,   /* 24..63 */
    24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24,
    24                                 /* 64     */
};

static const uint8_t C128_LEADING_IDX[25] = {
    0, 0, 0, 0, 0, 0, 0, 0,   /* 0..7  */
    1, 1, 1, 1,                /* 8..11 */
    2, 2, 2, 2,                /* 12..15 */
    3, 3,                      /* 16..17 */
    4, 4,                      /* 18..19 */
    5, 5,                      /* 20..21 */
    6, 6,                      /* 22..23 */
    7                          /* 24     */
};

static const uint8_t C128_IDX_TO_LEADING[8] = {
    0, 8, 12, 16, 18, 20, 22, 24
};

#define C128_NO_PREV_LEADING  65u
#define C128_TRAIL_THRESH     7u
#define C128_RING_SIZE        128u
#define C128_HASH_SIZE        16384u
#define C128_HASH_MASK        (C128_HASH_SIZE - 1u)

static inline uint32_t c128_hash(uint64_t bits) {
    return (uint32_t)((bits >> 50) & C128_HASH_MASK);
}

/* ------------------------------------------------------------------ ring state */

typedef struct {
    uint64_t ring[C128_RING_SIZE];
    int16_t  index_map[C128_HASH_SIZE];
    uint32_t write_pos;
} c128_ring_t;

static void c128_ring_init(c128_ring_t *s) {
    memset(s->ring, 0, sizeof(s->ring));
    memset(s->index_map, 0xFF, sizeof(s->index_map));
    s->write_pos = 0;
}

static inline void c128_ring_update(c128_ring_t *s, uint64_t bits) {
    s->ring[s->write_pos] = bits;
    s->index_map[c128_hash(bits)] = (int16_t)s->write_pos;
    s->write_pos = (s->write_pos + 1) & (C128_RING_SIZE - 1);
}

/* ------------------------------------------------------------------ bit-cost helpers */

/* flag 6: 3 + (64 - prev_leading) */
static inline unsigned cost_prev_reuse(unsigned prev_leading) {
    return 3u + (64u - prev_leading);
}

/* flag 3: 3 + 3 + (64 - lead_round) */
static inline unsigned cost_prev_new(unsigned lead_round) {
    return 3u + 3u + (64u - lead_round);
}

/* flag 5: 3 + 3 + 6 + sig_len */
static inline unsigned cost_prev_strip(unsigned lead_round, unsigned ctz) {
    return 3u + 3u + 6u + (64u - lead_round - ctz);
}

/* Best cost using prev-ref cases (flags 3/5/6). xor_val != 0 required. */
static unsigned cost_best_prev(uint64_t xor_val, unsigned prev_leading) {
    unsigned clz = tsdb_clz64(xor_val);
    unsigned ctz = tsdb_ctz64(xor_val);
    unsigned lead_round = C128_LEADING_ROUND[clz < 65 ? clz : 64];

    unsigned best = cost_prev_new(lead_round);

    if (ctz >= C128_TRAIL_THRESH) {
        unsigned c = cost_prev_strip(lead_round, ctz);
        if (c < best) best = c;
    }

    if (prev_leading != C128_NO_PREV_LEADING && lead_round == prev_leading) {
        unsigned c = cost_prev_reuse(prev_leading);
        if (c < best) best = c;
    }

    return best;
}

/* flag 1: 3 + 7 + 3 + 6 + sig_len */
static inline unsigned cost_ring_new(unsigned lead_round, unsigned ctz) {
    unsigned sig_len = (ctz >= C128_TRAIL_THRESH) ? 64u - lead_round - ctz
                                                   : 64u - lead_round;
    return 3u + 7u + 3u + 6u + sig_len;
}

/* flag 2: 3 + 7 + (64 - prev_leading) */
static inline unsigned cost_ring_reuse(unsigned prev_leading) {
    return 3u + 7u + (64u - prev_leading);
}

/* Best cost using ring-ref cases (flags 1/2). xor_val != 0 required. */
static unsigned cost_best_ring(uint64_t xor_val, unsigned prev_leading) {
    unsigned clz = tsdb_clz64(xor_val);
    unsigned ctz = tsdb_ctz64(xor_val);
    unsigned lead_round = C128_LEADING_ROUND[clz < 65 ? clz : 64];

    unsigned best = cost_ring_new(lead_round, ctz);

    if (prev_leading != C128_NO_PREV_LEADING && lead_round == prev_leading) {
        unsigned c = cost_ring_reuse(prev_leading);
        if (c < best) best = c;
    }

    return best;
}

/* ------------------------------------------------------------------ encode */

int tsdb_chimp128_encode(const double *in, size_t n, uint8_t *out, size_t cap,
                         size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;
    if (cap < 8) return TSDB_ERR_OVERFLOW;

    /* First value: raw LE uint64. */
    size_t pos = 0;
    uint64_t u0 = f64_to_u64(in[0]);
    for (int i = 0; i < 8; i++) out[pos++] = (uint8_t)(u0 >> (8 * i));

    if (n == 1) { *out_bytes = pos; return TSDB_OK; }

    tsdb_bw_t bw;
    tsdb_bw_init(&bw, out + pos, cap - pos);

    c128_ring_t ring;
    c128_ring_init(&ring);
    c128_ring_update(&ring, u0);

    uint64_t prev_u = u0;
    unsigned prev_leading = C128_NO_PREV_LEADING;

    for (size_t i = 1; i < n; i++) {
        uint64_t cur_u = f64_to_u64(in[i]);
        uint64_t xor_prev = cur_u ^ prev_u;

        if (xor_prev == 0) {
            /* flag 0 (2 bits): exact repeat of previous. */
            tsdb_bw_put(&bw, 0, 2);
            prev_leading = C128_NO_PREV_LEADING;
            prev_u = cur_u;
            c128_ring_update(&ring, cur_u);
            continue;
        }

        /* Prepare Chimp-prev encoding params. */
        unsigned clz_p = tsdb_clz64(xor_prev);
        unsigned ctz_p = tsdb_ctz64(xor_prev);
        unsigned lead_p     = C128_LEADING_ROUND[clz_p < 65 ? clz_p : 64];
        unsigned lead_idx_p = C128_LEADING_IDX[lead_p];

        /* Look up ring reference. */
        int16_t ri_raw = ring.index_map[c128_hash(cur_u)];
        bool use_ring   = false;
        bool ring_reuse = false;
        uint32_t ring_idx = 0;
        uint64_t xor_ring = 0;
        unsigned lead_r   = 0;
        unsigned ctz_r    = 0;
        unsigned lead_idx_r = 0;

        if (ri_raw >= 0) {
            ring_idx = (uint32_t)ri_raw & (C128_RING_SIZE - 1u);
            uint64_t ring_ref = ring.ring[ring_idx];
            xor_ring = cur_u ^ ring_ref;

            if (xor_ring != 0) {
                unsigned cost_prev = cost_best_prev(xor_prev, prev_leading);
                unsigned cost_ring = cost_best_ring(xor_ring, prev_leading);

                if (cost_ring < cost_prev) {
                    use_ring = true;
                    unsigned clz_r2 = tsdb_clz64(xor_ring);
                    ctz_r     = tsdb_ctz64(xor_ring);
                    lead_r    = C128_LEADING_ROUND[clz_r2 < 65 ? clz_r2 : 64];
                    lead_idx_r = C128_LEADING_IDX[lead_r];

                    if (prev_leading != C128_NO_PREV_LEADING &&
                        lead_r == prev_leading &&
                        cost_ring_reuse(prev_leading) <= cost_ring_new(lead_r, ctz_r)) {
                        ring_reuse = true;
                    }
                }
            }
        }

        if (use_ring) {
            if (ring_reuse) {
                /* flag 2 (3 bits): ring ref, reuse leading. */
                unsigned bits = 64u - prev_leading;
                tsdb_bw_put(&bw, 2, 3);
                tsdb_bw_put(&bw, ring_idx, 7);
                bw_put_safe(&bw, xor_ring, bits);
                /* prev_leading unchanged */
            } else {
                /* flag 1 (3 bits): ring ref, new leading. */
                unsigned sig_len, stored_ctz2;
                if (ctz_r >= C128_TRAIL_THRESH) {
                    sig_len     = 64u - lead_r - ctz_r;
                    stored_ctz2 = ctz_r;
                } else {
                    sig_len     = 64u - lead_r;
                    stored_ctz2 = 0u;
                }
                unsigned stored_len = (sig_len == 64u) ? 0u : sig_len;

                tsdb_bw_put(&bw, 1, 3);
                tsdb_bw_put(&bw, ring_idx, 7);
                tsdb_bw_put(&bw, lead_idx_r, 3);
                tsdb_bw_put(&bw, stored_len, 6);
                if (sig_len > 0)
                    bw_put_safe(&bw, xor_ring >> stored_ctz2, sig_len);

                prev_leading = lead_r;
            }

        } else {
            /* Prev-ref encoding. */
            if (ctz_p >= C128_TRAIL_THRESH) {
                unsigned cost_strip = cost_prev_strip(lead_p, ctz_p);
                unsigned cost_new   = cost_prev_new(lead_p);
                bool can_reuse = (prev_leading != C128_NO_PREV_LEADING &&
                                  lead_p == prev_leading);
                unsigned cost_reuse = can_reuse ? cost_prev_reuse(prev_leading) : ~0u;

                if (cost_strip <= cost_new && cost_strip <= cost_reuse) {
                    /* flag 5 (3 bits): prev ref, trailing-strip. */
                    unsigned sig_len    = 64u - lead_p - ctz_p;
                    unsigned stored_len = (sig_len == 64u) ? 0u : sig_len;
                    tsdb_bw_put(&bw, 5, 3);
                    tsdb_bw_put(&bw, lead_idx_p, 3);
                    tsdb_bw_put(&bw, stored_len, 6);
                    if (sig_len > 0)
                        bw_put_safe(&bw, xor_prev >> ctz_p, sig_len);
                    prev_leading = lead_p;
                } else if (can_reuse && cost_reuse < cost_new) {
                    /* flag 6 (3 bits): prev ref, reuse leading. */
                    unsigned bits = 64u - prev_leading;
                    tsdb_bw_put(&bw, 6, 3);
                    bw_put_safe(&bw, xor_prev, bits);
                    /* prev_leading unchanged */
                } else {
                    /* flag 3 (3 bits): prev ref, new leading, no strip. */
                    unsigned bits = 64u - lead_p;
                    tsdb_bw_put(&bw, 3, 3);
                    tsdb_bw_put(&bw, lead_idx_p, 3);
                    bw_put_safe(&bw, xor_prev, bits);
                    prev_leading = lead_p;
                }

            } else if (prev_leading != C128_NO_PREV_LEADING &&
                       lead_p == prev_leading) {
                /* flag 6 (3 bits): prev ref, reuse leading. */
                unsigned bits = 64u - prev_leading;
                tsdb_bw_put(&bw, 6, 3);
                bw_put_safe(&bw, xor_prev, bits);
                /* prev_leading unchanged */

            } else {
                /* flag 3 (3 bits): prev ref, new leading, no strip. */
                unsigned bits = 64u - lead_p;
                tsdb_bw_put(&bw, 3, 3);
                tsdb_bw_put(&bw, lead_idx_p, 3);
                bw_put_safe(&bw, xor_prev, bits);
                prev_leading = lead_p;
            }
        }

        prev_u = cur_u;
        c128_ring_update(&ring, cur_u);
    }

    ssize_t bits_bytes = tsdb_bw_finish(&bw);
    if (bits_bytes < 0) return TSDB_ERR_OVERFLOW;

    *out_bytes = pos + (size_t)bits_bytes;
    return TSDB_OK;
}

/* ------------------------------------------------------------------ decode */

int tsdb_chimp128_decode(const uint8_t *in, size_t n_bytes, double *out, size_t n)
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

    c128_ring_t ring;
    c128_ring_init(&ring);
    c128_ring_update(&ring, u0);

    uint64_t prev_u = u0;
    unsigned prev_leading = C128_NO_PREV_LEADING;

    for (size_t i = 1; i < n; i++) {
        /* Read 2-bit prefix. */
        uint64_t prefix = tsdb_br_get(&br, 2);
        uint64_t cur_u;

        if (prefix == 0) {
            /* flag 0: exact repeat of previous. */
            cur_u = prev_u;
            prev_leading = C128_NO_PREV_LEADING;

        } else {
            /* Read 3rd bit; reconstruct flag = prefix | (bit2 << 2). */
            uint64_t bit2 = tsdb_br_get(&br, 1);
            unsigned flag = (unsigned)(prefix | (bit2 << 2));

            switch (flag) {

            case 1: { /* ring ref, new leading */
                uint32_t ring_idx  = (uint32_t)tsdb_br_get(&br, 7);
                unsigned lead_idx  = (unsigned)tsdb_br_get(&br, 3);
                unsigned stored_len = (unsigned)tsdb_br_get(&br, 6);
                unsigned lead_round = C128_IDX_TO_LEADING[lead_idx & 7u];
                unsigned sig_len    = (stored_len == 0) ? 64u : stored_len;
                unsigned ctz2       = 64u - lead_round - sig_len;

                if (lead_round + sig_len > 64u) return TSDB_ERR_CORRUPT;

                uint64_t sig     = (sig_len > 0) ? br_get_safe(&br, sig_len) : 0;
                uint64_t xor_val = sig << ctz2;
                uint64_t rref    = ring.ring[ring_idx & (C128_RING_SIZE - 1u)];
                cur_u = rref ^ xor_val;
                prev_leading = lead_round;
                break;
            }

            case 2: { /* ring ref, reuse leading */
                if (prev_leading == C128_NO_PREV_LEADING) return TSDB_ERR_CORRUPT;
                uint32_t ring_idx = (uint32_t)tsdb_br_get(&br, 7);
                unsigned bits     = 64u - prev_leading;
                uint64_t xor_val  = br_get_safe(&br, bits);
                uint64_t rref     = ring.ring[ring_idx & (C128_RING_SIZE - 1u)];
                cur_u = rref ^ xor_val;
                /* prev_leading unchanged */
                break;
            }

            case 3: { /* prev ref, new leading, no strip */
                unsigned lead_idx  = (unsigned)tsdb_br_get(&br, 3);
                unsigned lead_round = C128_IDX_TO_LEADING[lead_idx & 7u];
                unsigned bits       = 64u - lead_round;
                uint64_t xor_val    = br_get_safe(&br, bits);
                cur_u = prev_u ^ xor_val;
                prev_leading = lead_round;
                break;
            }

            case 5: { /* prev ref, trailing-strip */
                unsigned lead_idx   = (unsigned)tsdb_br_get(&br, 3);
                unsigned stored_len = (unsigned)tsdb_br_get(&br, 6);
                unsigned lead_round = C128_IDX_TO_LEADING[lead_idx & 7u];
                unsigned sig_len    = (stored_len == 0) ? 64u : stored_len;
                unsigned ctz2       = 64u - lead_round - sig_len;

                if (lead_round + sig_len > 64u) return TSDB_ERR_CORRUPT;

                uint64_t sig     = (sig_len > 0) ? br_get_safe(&br, sig_len) : 0;
                uint64_t xor_val = sig << ctz2;
                cur_u = prev_u ^ xor_val;
                prev_leading = lead_round;
                break;
            }

            case 6: { /* prev ref, reuse leading */
                if (prev_leading == C128_NO_PREV_LEADING) return TSDB_ERR_CORRUPT;
                unsigned bits    = 64u - prev_leading;
                uint64_t xor_val = br_get_safe(&br, bits);
                cur_u = prev_u ^ xor_val;
                /* prev_leading unchanged */
                break;
            }

            default:
                /* Flag values 4 and 7 are unused/reserved. */
                return TSDB_ERR_CORRUPT;
            }
        }

        if (br.underflow) return TSDB_ERR_CORRUPT;

        out[i] = u64_to_f64(cur_u);
        prev_u = cur_u;
        c128_ring_update(&ring, cur_u);
    }

    return TSDB_OK;
}
