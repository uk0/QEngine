/* dod.c — delta-of-delta (Gorilla-style) encoding for int64 sequences.
 *
 * Format:
 *   byte 0..7: little-endian int64 first value
 *   varint-zigzag: second delta (delta1 = in[1] - in[0])
 *   then per-value prefix code for D = delta_n - delta_{n-1}:
 *     '0'        (1 bit)  : D == 0
 *     '10' + 7b  (9 bits) : D in [-63, 64], stored = D + 63 (biased)
 *     '110' + 9b (12 bits): D in [-255, 256], stored = D + 255 (biased)
 *     '1110'+12b (16 bits): D in [-2047, 2048], stored = D + 2047 (biased)
 *     '1111'+32b (36 bits): fallback zigzag32 (|D| <= ~2^31)
 */
#include "dod.h"
#include "../core/bits.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>

/* Write unsigned 7-bit-per-byte varint into bit stream (byte-aligned for first
 * two values only; we just write it byte-aligned into the raw buffer). */
static int write_uvarint_buf(uint8_t *buf, size_t cap, size_t *pos, uint64_t v)
{
    do {
        if (*pos >= cap) return TSDB_ERR_OVERFLOW;
        uint8_t b = v & 0x7F;
        v >>= 7;
        if (v) b |= 0x80;
        buf[(*pos)++] = b;
    } while (v);
    return TSDB_OK;
}

static int read_uvarint_buf(const uint8_t *buf, size_t n_bytes, size_t *pos, uint64_t *out)
{
    uint64_t result = 0;
    unsigned shift = 0;
    while (1) {
        if (*pos >= n_bytes) return TSDB_ERR_CORRUPT;
        uint8_t b = buf[(*pos)++];
        result |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
        if (shift >= 64) return TSDB_ERR_CORRUPT;
    }
    *out = result;
    return TSDB_OK;
}

int tsdb_dod_encode(const int64_t *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;
    if (cap < 1) return TSDB_ERR_OVERFLOW;

    size_t pos = 0;

    /* First value: raw 8 bytes little-endian. */
    if (pos + 8 > cap) return TSDB_ERR_OVERFLOW;
    uint64_t v0 = (uint64_t)in[0];
    for (int i = 0; i < 8; i++) out[pos++] = (uint8_t)(v0 >> (8 * i));

    if (n == 1) {
        *out_bytes = pos;
        return TSDB_OK;
    }

    /* Second value: zigzag-varint of delta1. */
    int64_t delta1 = in[1] - in[0];
    uint64_t zz1 = tsdb_zigzag_enc(delta1);
    int rc = write_uvarint_buf(out, cap, &pos, zz1);
    if (rc) return rc;

    if (n == 2) {
        *out_bytes = pos;
        return TSDB_OK;
    }

    /* Remaining values via bit stream for prefix codes. */
    tsdb_bw_t bw;
    tsdb_bw_init(&bw, out + pos, cap - pos);

    int64_t prev_delta = delta1;

    for (size_t i = 2; i < n; i++) {
        int64_t delta = in[i] - in[i - 1];
        int64_t D = delta - prev_delta;
        prev_delta = delta;

        if (D == 0) {
            tsdb_bw_put(&bw, 0, 1);
        } else if (D >= -63 && D <= 64) {
            /* '10' + 7-bit biased (stored = D + 63) */
            tsdb_bw_put(&bw, 0x2, 2);   /* bits: 01 (LSB-first = 10 prefix) */
            uint64_t stored = (uint64_t)(D + 63);
            tsdb_bw_put(&bw, stored, 7);
        } else if (D >= -255 && D <= 256) {
            /* '110' + 9-bit biased (stored = D + 255) */
            tsdb_bw_put(&bw, 0x6, 3);   /* bits: 011 */
            uint64_t stored = (uint64_t)(D + 255);
            tsdb_bw_put(&bw, stored, 9);
        } else if (D >= -2047 && D <= 2048) {
            /* '1110' + 12-bit biased (stored = D + 2047) */
            tsdb_bw_put(&bw, 0xE, 4);   /* bits: 0111 */
            uint64_t stored = (uint64_t)(D + 2047);
            tsdb_bw_put(&bw, stored, 12);
        } else {
            /* '1111' + 32-bit zigzag fallback */
            /* Check |D| fits in zigzag32 */
            if (D < -(int64_t)0x7FFFFFFF || D > (int64_t)0x80000000)
                return TSDB_ERR_OVERFLOW;
            tsdb_bw_put(&bw, 0xF, 4);   /* bits: 1111 */
            uint64_t zz = tsdb_zigzag_enc(D);
            tsdb_bw_put(&bw, zz & 0xFFFFFFFF, 32);
        }
    }

    ssize_t bits_bytes = tsdb_bw_finish(&bw);
    if (bits_bytes < 0) return TSDB_ERR_OVERFLOW;

    *out_bytes = pos + (size_t)bits_bytes;
    return TSDB_OK;
}

int tsdb_dod_decode(const uint8_t *in, size_t n_bytes, int64_t *out, size_t n)
{
    if (n == 0) return TSDB_OK;
    if (!in || !out) return TSDB_ERR_INVAL;

    size_t pos = 0;

    /* First value. */
    if (pos + 8 > n_bytes) return TSDB_ERR_CORRUPT;
    uint64_t v0 = 0;
    for (int i = 0; i < 8; i++) v0 |= (uint64_t)in[pos++] << (8 * i);
    out[0] = (int64_t)v0;

    if (n == 1) return TSDB_OK;

    /* Second value: zigzag-varint delta. */
    uint64_t zz1;
    int rc = read_uvarint_buf(in, n_bytes, &pos, &zz1);
    if (rc) return rc;
    int64_t delta1 = tsdb_zigzag_dec(zz1);
    out[1] = out[0] + delta1;

    if (n == 2) return TSDB_OK;

    /* Remaining: bit stream. */
    tsdb_br_t br;
    tsdb_br_init(&br, in + pos, n_bytes - pos);

    int64_t prev_delta = delta1;

    for (size_t i = 2; i < n; i++) {
        uint64_t b0 = tsdb_br_get(&br, 1);
        int64_t D;
        if (b0 == 0) {
            D = 0;
        } else {
            uint64_t b1 = tsdb_br_get(&br, 1);
            if (b1 == 0) {
                /* '10': 7-bit biased, bias=63 */
                uint64_t stored = tsdb_br_get(&br, 7);
                D = (int64_t)stored - 63;
            } else {
                uint64_t b2 = tsdb_br_get(&br, 1);
                if (b2 == 0) {
                    /* '110': 9-bit biased, bias=255 */
                    uint64_t stored = tsdb_br_get(&br, 9);
                    D = (int64_t)stored - 255;
                } else {
                    uint64_t b3 = tsdb_br_get(&br, 1);
                    if (b3 == 0) {
                        /* '1110': 12-bit biased, bias=2047 */
                        uint64_t stored = tsdb_br_get(&br, 12);
                        D = (int64_t)stored - 2047;
                    } else {
                        /* '1111': 32-bit zigzag */
                        uint64_t raw = tsdb_br_get(&br, 32);
                        D = tsdb_zigzag_dec(raw);
                    }
                }
            }
        }
        if (br.underflow) return TSDB_ERR_CORRUPT;
        int64_t delta = prev_delta + D;
        out[i] = out[i - 1] + delta;
        prev_delta = delta;
    }

    return TSDB_OK;
}
