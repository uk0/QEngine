/* dec.c — scaled-decimal FLOAT64 codec.  See dec.h for the wire format. */
#include "dec.h"
#include "dod.h"
#include "pfor.h"
#include "../core/types.h"
#include "../../include/tsdb.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

static const double DEC_P10[TSDB_DEC_MAX_SCALE + 1] = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8
};

/*
 * The decoder's reconstruction, as ONE expression shared with the encoder's
 * acceptance test.  The encoder accepts a value only when this function
 * reproduces its exact bits, so "encoder said yes" and "decoder gives the
 * input back" cannot drift apart.
 */
static inline double dec_expand(int64_t q, int s)
{
    return (double)q / DEC_P10[s];
}

/*
 * Try to represent one value at scale s.  Returns 1 and writes the quantised
 * integer on success, 0 when this value forces a fallback.
 */
static inline int dec_try(double v, int s, int64_t *q_out)
{
    double x = v * DEC_P10[s];

    /* NaN and ±inf fail BOTH comparisons, so this one test rejects them along
     * with the magnitudes that would make the int64 conversion undefined.
     * 9e15 < 2^53, which keeps both (int64_t)r and the (double)q widening in
     * dec_expand exact. */
    if (!(x >= -9.0e15 && x <= 9.0e15)) return 0;

    double  r = round(x);          /* half away from zero */
    int64_t q = (int64_t)r;
    double  back = dec_expand(q, s);

    /* Compare BIT PATTERNS, not values.  `back == v` is true for the
     * (+0.0, -0.0) pair, so a value-compare accepts -0.0 and silently decodes
     * it as +0.0 — a real, undetectable-by-== corruption.  memcmp sees it. */
    uint64_t a, b;
    memcpy(&a, &back, sizeof a);
    memcpy(&b, &v,    sizeof b);
    if (a != b) return 0;

    *q_out = q;
    return 1;
}

int tsdb_dec_find_scale(const double *v, size_t n, int64_t *out)
{
    if (!v || !out || n == 0) return -1;

    /* Smallest scale first: it yields the smallest integers, hence the
     * tightest DoD/PFOR stream.  A scale that works for value k is not
     * guaranteed to work at k's next-larger scale (the multiply is not
     * exact), so each candidate is verified over the whole block rather than
     * escalated in place.  Non-decimal data bails on element 0 for every s,
     * which is what makes the search free on the columns it cannot help. */
    for (int s = 0; s <= TSDB_DEC_MAX_SCALE; s++) {
        size_t k = 0;
        while (k < n && dec_try(v[k], s, &out[k])) k++;
        if (k == n) return s;
    }
    return -1;
}

int tsdb_dec_pack(const int64_t *q, size_t n, int scale, tsdb_codec_t inner,
                  uint8_t *out, size_t cap, size_t *out_bytes)
{
    if (!q || !out || !out_bytes || n == 0) return TSDB_ERR_INVAL;
    if (scale < 0 || scale > TSDB_DEC_MAX_SCALE) return TSDB_ERR_INVAL;
    if (inner != TSDB_CODEC_DOD && inner != TSDB_CODEC_PFOR) return TSDB_ERR_INVAL;
    if (cap < 2) return TSDB_ERR_OVERFLOW;

    /* `q` is the caller's own array and never aliases `out`: tsdb_pfor_encode_i64
     * writes a 4-byte count prologue, which over an aliased array would clobber
     * element 0 before it is read. */
    size_t body = 0;
    int rc = (inner == TSDB_CODEC_DOD)
           ? tsdb_dod_encode      (q, n, out + 2, cap - 2, &body)
           : tsdb_pfor_encode_i64 (q, n, out + 2, cap - 2, &body);
    if (rc != TSDB_OK) return rc;

    out[0] = (uint8_t)scale;
    out[1] = (uint8_t)inner;
    *out_bytes = 2 + body;
    return TSDB_OK;
}

int tsdb_dec_decode(const uint8_t *in, size_t in_bytes,
                    void *out, size_t out_count)
{
    if (!in || !out) return TSDB_ERR_INVAL;
    if (out_count == 0) return TSDB_OK;
    if (in_bytes < 2) return TSDB_ERR_CORRUPT;

    int     s     = in[0];
    uint8_t inner = in[1];
    if (s > TSDB_DEC_MAX_SCALE) return TSDB_ERR_CORRUPT;
    if (inner != (uint8_t)TSDB_CODEC_DOD && inner != (uint8_t)TSDB_CODEC_PFOR)
        return TSDB_ERR_CORRUPT;

    /* Staged through its own int64 buffer rather than widened in place inside
     * the caller's array: the caller's storage may have a declared type, and
     * writing an int64 stream over a double[] would be an aliasing violation.
     * One allocation per block, against n divisions — it does not show up. */
    int64_t *q = malloc(out_count * sizeof(int64_t));
    if (!q) return TSDB_ERR_NOMEM;

    int rc = (inner == (uint8_t)TSDB_CODEC_DOD)
           ? tsdb_dod_decode      (in + 2, in_bytes - 2, q, out_count)
           : tsdb_pfor_decode_i64 (in + 2, in_bytes - 2, q, out_count);
    if (rc != TSDB_OK) { free(q); return rc; }

    double *d = (double *)out;
    for (size_t k = 0; k < out_count; k++) d[k] = dec_expand(q[k], s);

    free(q);
    return TSDB_OK;
}
