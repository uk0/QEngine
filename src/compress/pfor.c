/* pfor.c — PFOR-Delta implementation.
 *
 * Block structure per 128 values:
 *   2 bytes: header  { b_minus1[4:0], n_exc[12:5], raw[13], rsvd[15:14] }
 *   4 bytes: base (min FOR value, uint32 little-endian)
 *   b*16 bytes: bit-packed residuals (absent for raw block)
 *   n_exc * 5 bytes: exceptions { pos:1byte, value:4bytes }
 *   -- OR for raw block: 128*4 bytes raw uint32 values
 */
#include "pfor.h"
#include "bp128.h"
#include "../core/bits.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

/* Block size in elements. */
#define PFOR_BLOCK  128
/* Outlier threshold: 5% of block = 6.4 → 6 exceptions max before we bump b. */
#define PFOR_MAX_EXC_RATIO_NUM  5
#define PFOR_MAX_EXC_RATIO_DEN  100
/* Maximum exceptions per block (5% of 128 = 6). */
#define PFOR_MAX_EXC  6

/* Stream prologue size: 4 bytes for uint32 count. */
#define PFOR_PROLOGUE_BYTES 4

/* Block header size. */
#define PFOR_HDR_BYTES  6   /* 2 (flags) + 4 (base) */

/* Exception entry size: 1 byte position + 4 bytes value. */
#define PFOR_EXC_BYTES  5

/* Raw block: 128 * 4 bytes = 512 bytes per block. */
#define PFOR_RAW_BLOCK_BYTES  (PFOR_BLOCK * 4)

/* -------------------------------------------------------------------------
 * Little-endian helpers
 * -------------------------------------------------------------------------*/
static inline void put_u16_le(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v      );
    p[1] = (uint8_t)(v >>  8);
}

static inline uint16_t get_u16_le(const uint8_t *p)
{
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static inline void put_u32_le(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v      );
    p[1] = (uint8_t)(v >>  8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static inline uint32_t get_u32_le(const uint8_t *p)
{
    return (uint32_t)p[0]
         | ((uint32_t)p[1] <<  8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

static inline void put_u64_le(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}

static inline uint64_t get_u64_le(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}

/* -------------------------------------------------------------------------
 * Determine bit-width b such that ≥95% of values fit in b bits.
 * Returns the minimum b (1..32) satisfying that condition.
 * -------------------------------------------------------------------------*/
static unsigned choose_b(const uint32_t *residuals, unsigned n)
{
    /* Compute histogram of msb positions. */
    unsigned hist[33] = {0};
    for (unsigned i = 0; i < n; i++) {
        uint32_t v = residuals[i];
        if (v == 0) {
            hist[0]++;
        } else {
            /* Number of bits needed = 32 - clz(v). */
            unsigned bits;
#if defined(__GNUC__) || defined(__clang__)
            bits = 32 - (unsigned)__builtin_clz(v);
#else
            bits = 1;
            while ((v >> bits) != 0) bits++;
#endif
            hist[bits]++;
        }
    }

    /* Find smallest b such that sum(hist[0..b]) >= ceil(95% of n). */
    /* 95% threshold: (n * 95 + 99) / 100 */
    unsigned threshold = (n * 95u + 99u) / 100u;
    unsigned cumulative = 0;
    for (unsigned b = 0; b <= 32; b++) {
        cumulative += hist[b];
        if (cumulative >= threshold) {
            /* b=0 means value=0 → need 1 bit minimum for bp128. */
            return (b == 0) ? 1 : b;
        }
    }
    return 32;
}

/* -------------------------------------------------------------------------
 * Encode one block of up to PFOR_BLOCK uint32 values.
 * src[0..count-1] are the raw (already delta-encoded) values.
 * Returns bytes written to out, or -1 on overflow.
 * -------------------------------------------------------------------------*/
static int encode_block(const uint32_t *src, unsigned count,
                         uint8_t *out, size_t out_cap, int raw_flag)
{
    /* Minimum space: header(6) + packed(b*16) + exceptions(n_exc*5). */
    /* For raw block: header(6) + raw(count*4). */

    if (raw_flag) {
        /* Raw block. */
        size_t need = PFOR_HDR_BYTES + (size_t)count * 4;
        if (need > out_cap) return -1;
        /* Header: raw=1, b=0 stored as 31 (max), n_exc=0. */
        uint16_t hdr = (uint16_t)(31u | (0u << 5) | (1u << 13));
        put_u16_le(out, hdr);
        put_u32_le(out + 2, 0); /* base unused */
        for (unsigned i = 0; i < count; i++) {
            put_u32_le(out + PFOR_HDR_BYTES + i * 4, src[i]);
        }
        return (int)(PFOR_HDR_BYTES + (size_t)count * 4);
    }

    /* Find block minimum (FOR base). */
    uint32_t base = src[0];
    for (unsigned i = 1; i < count; i++) {
        if (src[i] < base) base = src[i];
    }

    /* Compute residuals (FOR-subtracted). */
    uint32_t residuals[PFOR_BLOCK];
    for (unsigned i = 0; i < count; i++) {
        residuals[i] = src[i] - base;
    }

    /* Choose bit-width b. */
    unsigned b = choose_b(residuals, count);
    const uint32_t mask = (b == 32) ? 0xFFFFFFFFu : ((1u << b) - 1u);

    /* Identify exceptions (residuals >= 2^b). */
    uint8_t exc_pos[PFOR_BLOCK];
    uint32_t exc_val[PFOR_BLOCK];
    unsigned n_exc = 0;

    uint32_t packed_residuals[PFOR_BLOCK];
    for (unsigned i = 0; i < count; i++) {
        if (residuals[i] > mask) {
            exc_pos[n_exc] = (uint8_t)i;
            exc_val[n_exc] = residuals[i];
            n_exc++;
            packed_residuals[i] = 0; /* placeholder in packed stream */
        } else {
            packed_residuals[i] = residuals[i];
        }
    }

    /* Pad to 128 if block is partial (pad with value 0, no exceptions). */
    for (unsigned i = count; i < PFOR_BLOCK; i++) {
        packed_residuals[i] = 0;
    }

    /* Compute required output space. */
    size_t need = PFOR_HDR_BYTES + (size_t)b * 16 + (size_t)n_exc * PFOR_EXC_BYTES;
    if (need > out_cap) return -1;

    /* Write header. */
    uint16_t hdr = (uint16_t)((b - 1u)
                              | ((uint16_t)n_exc << 5)
                              | (0u << 13)); /* not raw */
    put_u16_le(out, hdr);
    put_u32_le(out + 2, base);
    size_t pos = PFOR_HDR_BYTES;

    /* Write bit-packed residuals. */
    int rc = tsdb_bp128_pack(packed_residuals, (uint32_t *)(out + pos), b);
    if (rc != TSDB_OK) return -1;
    pos += (size_t)b * 16;

    /* Write exceptions. */
    for (unsigned e = 0; e < n_exc; e++) {
        out[pos++] = exc_pos[e];
        put_u32_le(out + pos, exc_val[e]);
        pos += 4;
    }

    return (int)pos;
}

/* -------------------------------------------------------------------------
 * Decode one block.
 * Returns bytes consumed from in, or -1 on error.
 * Writes count values to out[0..count-1].
 * -------------------------------------------------------------------------*/
static int decode_block(const uint8_t *in, size_t in_avail, unsigned count,
                         uint32_t *out, size_t out_avail)
{
    if (in_avail < PFOR_HDR_BYTES) return -1;
    if (out_avail < count) return -1;

    uint16_t hdr  = get_u16_le(in);
    uint32_t base = get_u32_le(in + 2);
    unsigned b    = (unsigned)(hdr & 0x1Fu) + 1u;
    unsigned n_exc = (unsigned)((hdr >> 5) & 0xFFu);
    int raw_flag   = (hdr >> 13) & 1;

    size_t pos = PFOR_HDR_BYTES;

    if (raw_flag) {
        /* Raw block. */
        size_t need = (size_t)count * 4;
        if (pos + need > in_avail) return -1;
        for (unsigned i = 0; i < count; i++) {
            out[i] = get_u32_le(in + pos + i * 4);
        }
        return (int)(pos + need);
    }

    /* Normal PFOR block. */
    size_t packed_bytes = (size_t)b * 16;
    size_t exc_bytes    = (size_t)n_exc * PFOR_EXC_BYTES;
    if (pos + packed_bytes + exc_bytes > in_avail) return -1;
    if (n_exc > PFOR_BLOCK) return -1; /* sanity */

    /* Unpack bit-packed residuals. */
    uint32_t residuals[PFOR_BLOCK];
    int rc = tsdb_bp128_unpack((const uint32_t *)(in + pos), residuals, b);
    if (rc != TSDB_OK) return -1;
    pos += packed_bytes;

    /* Apply exceptions. */
    for (unsigned e = 0; e < n_exc; e++) {
        if (pos + PFOR_EXC_BYTES > in_avail) return -1;
        unsigned idx = (unsigned)in[pos++];
        uint32_t val = get_u32_le(in + pos); pos += 4;
        if (idx >= PFOR_BLOCK) return -1; /* corrupt */
        residuals[idx] = val;
    }

    /* Add base (FOR reconstruction) and write output. */
    for (unsigned i = 0; i < count; i++) {
        out[i] = residuals[i] + base;
    }

    return (int)pos;
}

/* =========================================================================
 * Public API — uint32
 * =========================================================================*/

int tsdb_pfor_encode(const uint32_t *in, size_t n,
                     uint8_t *out, size_t cap, size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;
    if (n > 0xFFFFFFFFu) return TSDB_ERR_INVAL; /* 4-byte count limit */

    /* Write prologue: n as uint32 LE. */
    if (cap < PFOR_PROLOGUE_BYTES) return TSDB_ERR_OVERFLOW;
    put_u32_le(out, (uint32_t)n);
    size_t pos = PFOR_PROLOGUE_BYTES;

    /* Encode blocks. */
    size_t i = 0;
    while (i < n) {
        unsigned count = (unsigned)((n - i) < PFOR_BLOCK ? (n - i) : PFOR_BLOCK);
        /* Delta-encode this block's values relative to previous. */
        uint32_t delta_block[PFOR_BLOCK];
        /* For PFOR of uint32, just pass values directly (already uint32). */
        /* The caller is responsible for delta/zigzag if needed. */
        /* For raw SYMBOL codes, pass as-is. */
        for (unsigned k = 0; k < count; k++) {
            delta_block[k] = in[i + k];
        }

        size_t avail = cap - pos;
        int written = encode_block(delta_block, count, out + pos, avail, 0);
        if (written < 0) return TSDB_ERR_OVERFLOW;
        pos += (size_t)written;
        i += count;
    }

    *out_bytes = pos;
    return TSDB_OK;
}

int tsdb_pfor_decode(const uint8_t *in, size_t n_bytes,
                     uint32_t *out, size_t n)
{
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;

    if (n_bytes < PFOR_PROLOGUE_BYTES) return TSDB_ERR_CORRUPT;
    uint32_t stored_n = get_u32_le(in);
    if ((size_t)stored_n != n) return TSDB_ERR_CORRUPT;

    size_t pos = PFOR_PROLOGUE_BYTES;
    size_t i = 0;

    while (i < n) {
        unsigned count = (unsigned)((n - i) < PFOR_BLOCK ? (n - i) : PFOR_BLOCK);
        size_t avail = (pos <= n_bytes) ? (n_bytes - pos) : 0;
        int consumed = decode_block(in + pos, avail, count, out + i, n - i);
        if (consumed < 0) return TSDB_ERR_CORRUPT;
        pos += (size_t)consumed;
        i += count;
    }

    return TSDB_OK;
}

/* =========================================================================
 * Public API — int64
 *
 * Wire format prologue:
 *   4 bytes: count n (uint32 LE)
 *
 * Then per 128-element block:
 *   1 byte: block_type
 *     0 = PFOR block (uint32 zigzag-delta residuals)
 *     1 = RAW block  (int64 LE values, 8 bytes each)
 *   For PFOR block:
 *     encode_block() output on zigzag-delta uint32 values
 *   For RAW block:
 *     count * 8 bytes of raw int64 LE values (all values in block, absolute)
 * =========================================================================*/

#define I64_BLOCK_TYPE_PFOR  0
#define I64_BLOCK_TYPE_RAW   1

int tsdb_pfor_encode_i64(const int64_t *in, size_t n,
                          uint8_t *out, size_t cap, size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;
    if (n > 0xFFFFFFFFu) return TSDB_ERR_INVAL;

    /* Write prologue. */
    if (cap < PFOR_PROLOGUE_BYTES) return TSDB_ERR_OVERFLOW;
    put_u32_le(out, (uint32_t)n);
    size_t pos = PFOR_PROLOGUE_BYTES;

    int64_t prev = 0;
    size_t i = 0;

    while (i < n) {
        unsigned count = (unsigned)((n - i) < PFOR_BLOCK ? (n - i) : PFOR_BLOCK);

        /* Compute zigzag-deltas and check if they fit in uint32. */
        uint32_t u32_block[PFOR_BLOCK];
        int64_t  i64_abs[PFOR_BLOCK]; /* absolute values for raw fallback */
        int fits_u32 = 1;

        for (unsigned k = 0; k < count; k++) {
            int64_t delta = in[i + k] - prev;
            uint64_t zz = tsdb_zigzag_enc(delta);
            i64_abs[k] = in[i + k];
            if (zz > 0xFFFFFFFFu) {
                fits_u32 = 0;
            }
            u32_block[k] = (uint32_t)(zz & 0xFFFFFFFFu);
            prev = in[i + k];
        }

        if (pos >= cap) return TSDB_ERR_OVERFLOW;

        if (fits_u32) {
            /* PFOR block. */
            out[pos++] = I64_BLOCK_TYPE_PFOR;
            size_t avail = cap - pos;
            int written = encode_block(u32_block, count, out + pos, avail, 0);
            if (written < 0) return TSDB_ERR_OVERFLOW;
            pos += (size_t)written;
        } else {
            /* RAW block: 1 byte type + count*8 bytes. */
            size_t raw_need = 1 + (size_t)count * 8;
            if (pos + raw_need > cap) return TSDB_ERR_OVERFLOW;
            out[pos++] = I64_BLOCK_TYPE_RAW;
            for (unsigned k = 0; k < count; k++) {
                put_u64_le(out + pos, (uint64_t)i64_abs[k]);
                pos += 8;
            }
        }

        i += count;
    }

    *out_bytes = pos;
    return TSDB_OK;
}

int tsdb_pfor_decode_i64(const uint8_t *in, size_t n_bytes,
                          int64_t *out, size_t n)
{
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;

    if (n_bytes < PFOR_PROLOGUE_BYTES) return TSDB_ERR_CORRUPT;
    uint32_t stored_n = get_u32_le(in);
    if ((size_t)stored_n != n) return TSDB_ERR_CORRUPT;

    size_t pos = PFOR_PROLOGUE_BYTES;
    size_t i = 0;
    int64_t prev = 0;

    while (i < n) {
        unsigned count = (unsigned)((n - i) < PFOR_BLOCK ? (n - i) : PFOR_BLOCK);

        if (pos >= n_bytes) return TSDB_ERR_CORRUPT;
        uint8_t block_type = in[pos++];

        if (block_type == I64_BLOCK_TYPE_PFOR) {
            /* Decode uint32 PFOR block. */
            uint32_t u32_out[PFOR_BLOCK];
            size_t avail = (pos <= n_bytes) ? (n_bytes - pos) : 0;
            int consumed = decode_block(in + pos, avail, count, u32_out, PFOR_BLOCK);
            if (consumed < 0) return TSDB_ERR_CORRUPT;
            pos += (size_t)consumed;

            /* Reverse zigzag-delta. */
            for (unsigned k = 0; k < count; k++) {
                int64_t delta = tsdb_zigzag_dec((uint64_t)u32_out[k]);
                int64_t val   = prev + delta;
                out[i + k]    = val;
                prev          = val;
            }
        } else if (block_type == I64_BLOCK_TYPE_RAW) {
            /* Raw int64 block. */
            size_t raw_need = (size_t)count * 8;
            if (pos + raw_need > n_bytes) return TSDB_ERR_CORRUPT;
            for (unsigned k = 0; k < count; k++) {
                int64_t val = (int64_t)get_u64_le(in + pos);
                pos += 8;
                out[i + k] = val;
                prev = val;
            }
        } else {
            return TSDB_ERR_CORRUPT;
        }

        i += count;
    }

    return TSDB_OK;
}
