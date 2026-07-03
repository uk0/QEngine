/* codec.c — unified codec dispatch. */
#include "codec.h"
#include "dod.h"
#include "gorilla.h"
#include "chimp.h"
#include "chimp128.h"
#include "dict.h"
#include "pfor.h"
#include "lzlite.h"
#include "../core/types.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

/* Minimum byte saving required to accept lzlite outer-wrapper. */
#define OUTER_LZ_MIN_GAIN 16

/* Skip the outer lzlite pass for a float XOR codec (Gorilla/Chimp) whose output
 * already reaches this percent of the raw double size.  Measured across varying
 * float distributions (price-band, wide-range, full-entropy, random-walk, sine)
 * the domain output is always >= 50% of raw and lzlite loses by ~200 B (rejected
 * for gain < min_gain) — running it only burns CPU.  Only constant/near-constant
 * float compresses far below this (~1.6% of raw), and there lzlite wins ~60x;
 * such blocks stay below the threshold and still take the LZ path. */
#define FLOAT_LZ_SKIP_PCT 50

/*
 * Routing:
 *   TIMESTAMP -> DOD (int64)
 *   INT64     -> best of DOD vs PFOR-Delta (winner chosen per block)
 *   FLOAT64   -> best of Gorilla vs Chimp (winner chosen per block)
 *   SYMBOL    -> best of DICT vs PFOR (winner chosen per block)
 */

int tsdb_codec_encode(tsdb_type_t type,
                      const void *in, size_t in_count,
                      uint8_t *out, size_t out_cap,
                      tsdb_codec_t *out_codec)
{
    if (!out_codec) return TSDB_ERR_INVAL;
    if (in_count > 0 && !in) return TSDB_ERR_INVAL;
    if (!out && out_cap > 0) return TSDB_ERR_INVAL;

    int rc;
    size_t out_bytes = 0;

    switch (type) {
    case TSDB_TYPE_TIMESTAMP: {
        /* Timestamps: try DoD first (ideal for monotone nanosecond series).
         * DoD can fail with TSDB_ERR_OVERFLOW when delta-of-deltas exceed ±2^31
         * (e.g. timestamps with gaps > ~2s when consecutive deltas differ by > 2^31 ns).
         * On overflow, fall back to PFOR-i64, then to raw (CODEC_NONE). */
        rc = tsdb_dod_encode((const int64_t *)in, in_count, out, out_cap, &out_bytes);
        if (rc == TSDB_OK) {
            *out_codec = TSDB_CODEC_DOD;
            return (int)out_bytes;
        }
        /* DoD failed — try PFOR-i64. */
        {
            size_t tmp_cap = out_cap + 64;
            uint8_t *tmp = malloc(tmp_cap);
            if (tmp) {
                size_t pfor_bytes = 0;
                int pfor_rc = tsdb_pfor_encode_i64((const int64_t *)in, in_count,
                                                    tmp, tmp_cap, &pfor_bytes);
                if (pfor_rc == TSDB_OK && pfor_bytes <= out_cap) {
                    memcpy(out, tmp, pfor_bytes);
                    free(tmp);
                    *out_codec = TSDB_CODEC_PFOR;
                    return (int)pfor_bytes;
                }
                free(tmp);
            }
        }
        /* Final fallback: raw copy (CODEC_NONE). */
        {
            size_t raw_bytes = in_count * sizeof(int64_t);
            if (raw_bytes <= out_cap) {
                memcpy(out, in, raw_bytes);
                *out_codec = TSDB_CODEC_NONE;
                return (int)raw_bytes;
            }
        }
        /* Buffer too small even for raw. */
        return TSDB_ERR_OVERFLOW;
    }
    case TSDB_TYPE_INT64: {
        /*
         * INT64: try DoD first (into out), then PFOR-Delta (into tmp); pick smaller.
         * DoD can fail with TSDB_ERR_OVERFLOW on data with extreme delta-of-deltas;
         * treat that as non-fatal and use PFOR only.
         */
        rc = tsdb_dod_encode((const int64_t *)in, in_count, out, out_cap, &out_bytes);
        size_t dod_bytes = (rc == TSDB_OK) ? out_bytes : (size_t)-1;

        /* Try PFOR into a temp buffer. */
        size_t tmp_cap = out_cap + 64;
        uint8_t *tmp = malloc(tmp_cap);
        if (!tmp) {
            if (dod_bytes != (size_t)-1) {
                *out_codec = TSDB_CODEC_DOD;
                return (int)dod_bytes;
            }
            return TSDB_ERR_NOMEM;
        }
        size_t pfor_bytes = 0;
        int pfor_rc = tsdb_pfor_encode_i64((const int64_t *)in, in_count,
                                            tmp, tmp_cap, &pfor_bytes);
        if (pfor_rc == TSDB_OK &&
            (dod_bytes == (size_t)-1 || pfor_bytes < dod_bytes)) {
            /* PFOR is smaller (or DoD failed): use PFOR. */
            if (pfor_bytes <= out_cap) {
                memcpy(out, tmp, pfor_bytes);
                free(tmp);
                *out_codec = TSDB_CODEC_PFOR;
                return (int)pfor_bytes;
            }
        }
        free(tmp);
        if (dod_bytes == (size_t)-1) return rc; /* both failed */
        *out_codec = TSDB_CODEC_DOD;
        return (int)dod_bytes;
    }
    case TSDB_TYPE_FLOAT64: {
        /*
         * Float path: best of Gorilla vs Chimp, picked per block.
         *
         * Chimp128 was dropped from the encode path after measuring six
         * representative distributions (constant, price-band [100,150],
         * wide-range [1e6,2e6], full-entropy random doubles, random-walk, sine):
         * Chimp128 won 0 of 6 while costing 1.2-1.6x Gorilla's encode time, and
         * best-of-2(Gorilla,Chimp) matched the old best-of-3 byte-for-byte on
         * every one.  Gorilla wins on constant / monotone / lag-1 data; Chimp
         * wins on smooth real-valued series (e.g. sine: -23% vs Gorilla).  The
         * Chimp128 *decoder* is retained for any pre-existing CHIMP128 blocks.
         */
        size_t tmp_cap = in_count * 11 + 64;
        if (tmp_cap < out_cap + 64) tmp_cap = out_cap + 64;

        uint8_t *tg = malloc(tmp_cap); /* Gorilla */
        uint8_t *tc = malloc(tmp_cap); /* Chimp   */
        if (!tg || !tc) {
            free(tg); free(tc);
            /* malloc failed; encode Chimp directly into out as a fallback. */
            size_t cb = 0;
            rc = tsdb_chimp_encode((const double *)in, in_count, out, out_cap, &cb);
            if (rc) return rc;
            *out_codec = TSDB_CODEC_CHIMP;
            return (int)cb;
        }

        size_t gbytes = 0, cbytes = 0;
        int g_rc = tsdb_gorilla_encode((const double *)in, in_count, tg, tmp_cap, &gbytes);
        int c_rc = tsdb_chimp_encode  ((const double *)in, in_count, tc, tmp_cap, &cbytes);

        size_t best_bytes = (size_t)-1;
        tsdb_codec_t best_codec = TSDB_CODEC_CHIMP;
        uint8_t *best_tmp = NULL;
        if (c_rc == TSDB_OK) {
            best_bytes = cbytes; best_codec = TSDB_CODEC_CHIMP;   best_tmp = tc;
        }
        if (g_rc == TSDB_OK && gbytes < best_bytes) {
            best_bytes = gbytes; best_codec = TSDB_CODEC_GORILLA; best_tmp = tg;
        }

        if (best_tmp == NULL) {
            free(tg); free(tc);
            return TSDB_ERR_INVAL;
        }
        if (best_bytes > out_cap) {
            free(tg); free(tc);
            return TSDB_ERR_OVERFLOW;
        }

        memcpy(out, best_tmp, best_bytes);
        free(tg); free(tc);
        *out_codec = best_codec;
        return (int)best_bytes;
    }
    case TSDB_TYPE_SYMBOL: {
        /* SYMBOL: try DICT first (into out), then PFOR (into tmp); pick smaller. */
        rc = tsdb_dict_encode((const uint32_t *)in, in_count, out, out_cap, &out_bytes);
        if (rc) return rc;
        size_t dict_bytes = out_bytes;

        size_t tmp_cap = out_cap + 64;
        uint8_t *tmp = malloc(tmp_cap);
        if (!tmp) {
            *out_codec = TSDB_CODEC_DICT;
            return (int)dict_bytes;
        }
        size_t pfor_bytes = 0;
        int pfor_rc = tsdb_pfor_encode((const uint32_t *)in, in_count,
                                        tmp, tmp_cap, &pfor_bytes);
        if (pfor_rc == TSDB_OK && pfor_bytes < dict_bytes) {
            if (pfor_bytes <= out_cap) {
                memcpy(out, tmp, pfor_bytes);
                free(tmp);
                *out_codec = TSDB_CODEC_PFOR;
                return (int)pfor_bytes;
            }
        }
        free(tmp);
        *out_codec = TSDB_CODEC_DICT;
        return (int)dict_bytes;
    }
    case TSDB_TYPE_FLOAT32: {
        /* The user chose 32-bit precision for this column.  Narrow every value
         * to float, then keep whichever is smaller: a Gorilla pass over the
         * narrowed values (wins on slowly-varying data — matches or beats
         * full-precision FLOAT64), or a flat 4-byte store (wins on high-entropy
         * data — half of FLOAT64's ~7 bytes/value).  So a FLOAT32 column is
         * never larger than the same data as FLOAT64; it's the user's
         * lossy-by-choice space win. */
        const double *d = (const double *)in;
        size_t raw_bytes = in_count * 4;
        /* Gorilla candidate over f32-precision doubles. */
        double *nd = malloc(in_count * sizeof(double));
        if (nd) {
            for (size_t i = 0; i < in_count; i++) nd[i] = (double)(float)d[i];
            size_t gcap = in_count * 8 + 64;
            uint8_t *gtmp = malloc(gcap);
            size_t gbytes = 0;
            int grc = gtmp ? tsdb_gorilla_encode(nd, in_count, gtmp, gcap, &gbytes) : -1;
            free(nd);
            if (grc == TSDB_OK && gbytes < raw_bytes && gbytes <= out_cap) {
                memcpy(out, gtmp, gbytes);
                free(gtmp);
                *out_codec = TSDB_CODEC_GORILLA;  /* decodes straight to doubles */
                return (int)gbytes;
            }
            free(gtmp);
        }
        /* Flat 4-byte narrow. */
        if (raw_bytes > out_cap) return TSDB_ERR_FULL;
        float *f = (float *)out;
        for (size_t i = 0; i < in_count; i++) f[i] = (float)d[i];
        *out_codec = TSDB_CODEC_F32;
        return (int)raw_bytes;
    }
    default:
        return TSDB_ERR_UNSUPPORTED;
    }
}

int tsdb_codec_decode(tsdb_codec_t codec,
                      tsdb_type_t type,
                      const uint8_t *in, size_t in_bytes,
                      void *out, size_t out_count)
{
    if (out_count > 0 && (!in || !out)) return TSDB_ERR_INVAL;

    (void)type; /* type is advisory; codec determines format */

    switch (codec) {
    case TSDB_CODEC_DOD:
        return tsdb_dod_decode(in, in_bytes, (int64_t *)out, out_count);
    case TSDB_CODEC_GORILLA:
        return tsdb_gorilla_decode(in, in_bytes, (double *)out, out_count);
    case TSDB_CODEC_CHIMP:
        return tsdb_chimp_decode(in, in_bytes, (double *)out, out_count);
    case TSDB_CODEC_CHIMP128:
        return tsdb_chimp128_decode(in, in_bytes, (double *)out, out_count);
    case TSDB_CODEC_DICT:
        return tsdb_dict_decode(in, in_bytes, (uint32_t *)out, out_count);
    case TSDB_CODEC_PFOR:
        /* Dispatch based on type. */
        if (type == TSDB_TYPE_INT64 || type == TSDB_TYPE_TIMESTAMP) {
            return tsdb_pfor_decode_i64(in, in_bytes, (int64_t *)out, out_count);
        } else {
            /* SYMBOL or any uint32 type. */
            return tsdb_pfor_decode(in, in_bytes, (uint32_t *)out, out_count);
        }
    case TSDB_CODEC_F32: {
        /* 4-byte float on disk -> double in the output buffer (FLOAT32 column's
         * in-memory width is 8). */
        if (in_bytes < out_count * 4) return TSDB_ERR_CORRUPT;
        const float *f = (const float *)in;
        double *d = (double *)out;
        for (size_t i = 0; i < out_count; i++) d[i] = (double)f[i];
        return TSDB_OK;
    }
    case TSDB_CODEC_RAW:
    case TSDB_CODEC_NONE: {
        size_t width = tsdb_type_width(type);
        if (width == 0) return TSDB_ERR_UNSUPPORTED;
        size_t need = out_count * width;
        if (in_bytes < need) return TSDB_ERR_CORRUPT;
        memcpy(out, in, need);
        return TSDB_OK;
    }
    default:
        return TSDB_ERR_UNSUPPORTED;
    }
}

/* ---- Adaptive encode ---------------------------------------------------- */

int tsdb_codec_encode_adaptive(tsdb_type_t type,
                               const void *in, size_t in_count,
                               uint8_t *out, size_t out_cap,
                               tsdb_codec_t *out_codec,
                               uint16_t *out_flags)
{
    return tsdb_codec_encode_adaptive_ex(type, in, in_count, out, out_cap,
                                         out_codec, out_flags,
                                         OUTER_LZ_MIN_GAIN);
}

int tsdb_codec_encode_adaptive_ex(tsdb_type_t type,
                                  const void *in, size_t in_count,
                                  uint8_t *out, size_t out_cap,
                                  tsdb_codec_t *out_codec,
                                  uint16_t *out_flags,
                                  int min_gain)
{
    if (!out_codec || !out_flags) return TSDB_ERR_INVAL;
    if (min_gain <= 0) min_gain = 1;  /* a 0/negative-gain wrap is never worth it */

    /* Step 1: domain-codec selection (existing logic). */
    tsdb_codec_t codec = TSDB_CODEC_NONE;
    int domain_bytes = tsdb_codec_encode(type, in, in_count, out, out_cap, &codec);
    if (domain_bytes < 0) return domain_bytes;

    *out_codec  = codec;
    *out_flags  = 0;

    /* Step 1.5: FLOAT64 raw fallback.  On high-entropy blocks BOTH float
     * XOR codecs EXPAND (up to ~+12.5% over the plain 8 B/value), so
     * storing the doubles verbatim is strictly smaller — and a RAW block
     * is eligible for the reader's zero-copy mmap fast path (pointer
     * handoff, no decode, no memcpy; see tsdb_part_read_block_ref).
     * '>=': ties go to RAW for the cheaper read.  Data this incompressible
     * gains nothing from the outer LZ wrap either, so return without the
     * LZ pass.  Kept out of tsdb_codec_encode: legacy callers assert the
     * XOR-codec choice there. */
    if (type == TSDB_TYPE_FLOAT64 && in_count > 0) {
        size_t raw_bytes = in_count * 8;
        if ((size_t)domain_bytes >= raw_bytes && raw_bytes <= out_cap) {
            memcpy(out, in, raw_bytes);
            *out_codec = TSDB_CODEC_RAW;
            return (int)raw_bytes;
        }
    }

    /* Early-exit: skip the outer LZ pass for a dense float block.  When a float
     * XOR codec's output already reaches FLOAT_LZ_SKIP_PCT% of the raw double
     * size, byte-level lzlite cannot find matches and the wrapper is rejected for
     * gain < min_gain anyway (measured: ~-200 B on every varying distribution).
     * Skipping produces byte-identical output (flags = 0) at no CPU cost.  Only
     * constant/near-constant float falls below the threshold, where LZ wins and
     * the normal path below runs. */
    if ((codec == TSDB_CODEC_GORILLA || codec == TSDB_CODEC_CHIMP ||
         codec == TSDB_CODEC_CHIMP128) && in_count > 0) {
        size_t raw_bytes = in_count * 8;
        if ((size_t)domain_bytes * 100 >= raw_bytes * FLOAT_LZ_SKIP_PCT) {
            return domain_bytes;  /* dense float — LZ cannot help */
        }
    }

    /* Step 2: try lzlite over the domain-codec output. */
    size_t lz_cap = tsdb_lzlite_max_output((size_t)domain_bytes);
    /* Wire format: [u32 LE orig_size][lz bytes] — need 4 extra bytes for header. */
    size_t wrapped_cap = lz_cap + 4;
    uint8_t *lz_buf = malloc(wrapped_cap);
    if (!lz_buf) {
        /* malloc failed; use plain domain-codec result. */
        return domain_bytes;
    }

    size_t lz_bytes = 0;
    int lz_rc = tsdb_lzlite_encode(out, (size_t)domain_bytes,
                                   lz_buf + 4, lz_cap, &lz_bytes);
    if (lz_rc >= 0) {
        size_t wrapped_bytes = 4 + lz_bytes;
        int gain = (int)domain_bytes - (int)wrapped_bytes;
        if (gain >= min_gain && wrapped_bytes <= out_cap) {
            /* Write the 4-byte orig_size header then lz bytes into out. */
            uint32_t orig_u32 = (uint32_t)domain_bytes;
            out[0] = (uint8_t)(orig_u32 & 0xFF);
            out[1] = (uint8_t)((orig_u32 >> 8) & 0xFF);
            out[2] = (uint8_t)((orig_u32 >> 16) & 0xFF);
            out[3] = (uint8_t)((orig_u32 >> 24) & 0xFF);
            memcpy(out + 4, lz_buf + 4, lz_bytes);
            free(lz_buf);
            *out_flags = TSDB_BF_OUTER_LZ;
            return (int)wrapped_bytes;
        }
    }

    free(lz_buf);
    /* No lz gain: return plain domain-codec result already in out. */
    return domain_bytes;
}

/* ---- Adaptive decode ---------------------------------------------------- */

int tsdb_codec_decode_adaptive(tsdb_codec_t codec,
                               tsdb_type_t type,
                               uint16_t flags,
                               const uint8_t *in, size_t in_bytes,
                               void *out, size_t out_count)
{
    if (!(flags & TSDB_BF_OUTER_LZ)) {
        /* Legacy path: no outer LZ wrapper. */
        return tsdb_codec_decode(codec, type, in, in_bytes, out, out_count);
    }

    /* Outer-LZ path: decode 4-byte orig_size, lzlite-decode into temp, then domain-decode. */
    if (in_bytes < 4) return TSDB_ERR_CORRUPT;

    uint32_t orig_size = (uint32_t)in[0]
                       | ((uint32_t)in[1] <<  8)
                       | ((uint32_t)in[2] << 16)
                       | ((uint32_t)in[3] << 24);
    if (orig_size == 0 || orig_size > 64u * 1024u * 1024u) return TSDB_ERR_CORRUPT;

    uint8_t *tmp = malloc(orig_size);
    if (!tmp) return TSDB_ERR_NOMEM;

    size_t decoded_bytes = 0;
    int lz_rc = tsdb_lzlite_decode(in + 4, in_bytes - 4,
                                   tmp, orig_size, &decoded_bytes);
    if (lz_rc != TSDB_OK) {
        free(tmp);
        return TSDB_ERR_CORRUPT;
    }
    if (decoded_bytes != orig_size) {
        free(tmp);
        return TSDB_ERR_CORRUPT;
    }

    int rc = tsdb_codec_decode(codec, type, tmp, decoded_bytes, out, out_count);
    free(tmp);
    return rc;
}
