/* dict.c — delta + zigzag + 7-bit varint encoding for uint32 symbol columns. */
#include "dict.h"
#include "../../include/tsdb.h"
#include <string.h>

/* Write unsigned varint (7 bits per byte, LSB first, msb=continuation). */
static int write_uvarint(uint8_t *out, size_t cap, size_t *pos, uint64_t v)
{
    do {
        if (*pos >= cap)
            return TSDB_ERR_OVERFLOW;
        uint8_t b = v & 0x7F;
        v >>= 7;
        if (v) b |= 0x80;
        out[(*pos)++] = b;
    } while (v);
    return TSDB_OK;
}

/* Read unsigned varint. Returns TSDB_ERR_CORRUPT on truncation. */
static int read_uvarint(const uint8_t *in, size_t n_bytes, size_t *pos, uint64_t *out)
{
    uint64_t result = 0;
    unsigned shift = 0;
    while (1) {
        if (*pos >= n_bytes)
            return TSDB_ERR_CORRUPT;
        uint8_t b = in[(*pos)++];
        result |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80))
            break;
        shift += 7;
        if (shift >= 64)
            return TSDB_ERR_CORRUPT;
    }
    *out = result;
    return TSDB_OK;
}

int tsdb_dict_encode(const uint32_t *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes)
{
    if (!out_bytes)
        return TSDB_ERR_INVAL;
    *out_bytes = 0;
    if (n == 0)
        return TSDB_OK;
    if (!in || !out)
        return TSDB_ERR_INVAL;

    size_t pos = 0;
    int rc;

    /* Write count as varint so decoder can validate. */
    rc = write_uvarint(out, cap, &pos, (uint64_t)n);
    if (rc) return rc;

    /* First value: zigzag-encode the raw value. */
    uint64_t zz = ((uint64_t)in[0] << 1) ^ (uint64_t)((int64_t)(int32_t)in[0] >> 63);
    rc = write_uvarint(out, cap, &pos, zz);
    if (rc) return rc;

    /* Subsequent values: delta then zigzag. */
    for (size_t i = 1; i < n; i++) {
        int64_t delta = (int64_t)(int32_t)in[i] - (int64_t)(int32_t)in[i - 1];
        uint64_t encoded = (delta >= 0)
            ? (uint64_t)delta << 1
            : (((uint64_t)(-delta) << 1) - 1);
        rc = write_uvarint(out, cap, &pos, encoded);
        if (rc) return rc;
    }

    *out_bytes = pos;
    return TSDB_OK;
}

int tsdb_dict_decode(const uint8_t *in, size_t n_bytes, uint32_t *out, size_t n)
{
    if (n == 0)
        return TSDB_OK;
    if (!in || !out)
        return TSDB_ERR_INVAL;

    size_t pos = 0;
    int rc;

    /* Read and validate count. */
    uint64_t stored_n;
    rc = read_uvarint(in, n_bytes, &pos, &stored_n);
    if (rc) return rc;
    if (stored_n != n)
        return TSDB_ERR_CORRUPT;

    /* First value. */
    uint64_t zz;
    rc = read_uvarint(in, n_bytes, &pos, &zz);
    if (rc) return rc;
    int64_t v0 = (int64_t)((zz >> 1) ^ -(int64_t)(zz & 1));
    out[0] = (uint32_t)v0;

    int64_t prev = v0;
    for (size_t i = 1; i < n; i++) {
        uint64_t encoded;
        rc = read_uvarint(in, n_bytes, &pos, &encoded);
        if (rc) return rc;
        int64_t delta;
        if (encoded & 1)
            delta = -((int64_t)((encoded + 1) >> 1));
        else
            delta = (int64_t)(encoded >> 1);
        int64_t cur = prev + delta;
        out[i] = (uint32_t)cur;
        prev = cur;
    }
    return TSDB_OK;
}
