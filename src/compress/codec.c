/* codec.c — unified codec dispatch. */
#include "codec.h"
#include "dod.h"
#include "gorilla.h"
#include "chimp.h"
#include "chimp128.h"
#include "dec.h"
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

/* First half of the float outer-LZ gate: the domain codec's output reaches this
 * percent of the raw double size.  On its own this is NOT evidence that lzlite
 * cannot help.  Measured on quantized telemetry, f64_sensor_2dp lands at 81% of
 * raw and lzlite still takes 36% off it, f64_sensor_1dp 68% / -50%, f64_pct_2dp
 * 89% / -31% — while f64_walk_full at 80% genuinely gains nothing.  What
 * separates them is whether the values are quantized, not how big the domain
 * output is, so this threshold only *arms* the skip; lz_probe_futile() decides
 * it.  (The original comment here claimed the domain output is "always >= 50% of
 * raw and lzlite loses by ~200 B" on varying distributions; the shapes above
 * falsify that.) */
#define FLOAT_LZ_SKIP_PCT 50

/* Second half of the gate: one lzlite pass over a sample of the RAW input.
 * Quantized doubles repeat byte patterns that survive into the XOR stream;
 * full-precision ones do not.  The probe sees that difference directly instead
 * of inferring it from an output size.
 *
 * The sample is drawn as LZ_PROBE_CHUNKS evenly spaced windows rather than one
 * head window: a block whose head is high-entropy and whose tail is compressible
 * (a regime change inside one flush) is exactly what a head-only window
 * mispredicts.  Cost is one pass over 4 KB — 0.6-6 ns/value, against 100-150
 * ns/value for the full pass it is deciding about. */
#define LZ_PROBE_BYTES    4096
#define LZ_PROBE_CHUNKS   4
/* Probe out/in at or above this percent means "no byte-level redundancy". */
#define LZ_PROBE_SKIP_PCT 99

/*
 * Return 1 when a sample of the raw column bytes shows no byte-level redundancy,
 * i.e. a full lzlite pass is predicted to be futile.  Returns 0 whenever the
 * probe cannot tell, so the caller falls back to running the real pass — the
 * probe may only ever add confidence to a skip, never force one.
 */
static int lz_probe_futile(const void *in, size_t raw_bytes)
{
    uint8_t sample[LZ_PROBE_BYTES];
    uint8_t probe_out[LZ_PROBE_BYTES + LZ_PROBE_BYTES / 255 + 16];
    size_t n;

    if (raw_bytes == 0) return 1;
    if (raw_bytes <= LZ_PROBE_BYTES) {
        n = raw_bytes;
        memcpy(sample, in, n);
    } else {
        size_t chunk  = LZ_PROBE_BYTES / LZ_PROBE_CHUNKS;
        size_t stride = (raw_bytes - chunk) / (LZ_PROBE_CHUNKS - 1);
        for (size_t c = 0; c < LZ_PROBE_CHUNKS; c++)
            memcpy(sample + c * chunk, (const uint8_t *)in + c * stride, chunk);
        n = chunk * LZ_PROBE_CHUNKS;
    }

    size_t out_n = 0;
    if (tsdb_lzlite_encode(sample, n, probe_out, sizeof(probe_out), &out_n) < 0)
        return 0;
    return out_n * 100 >= n * LZ_PROBE_SKIP_PCT;
}

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
    case TSDB_CODEC_DEC:
        /* Scaled decimal — always produces doubles, so it serves both FLOAT64
         * and the (width-8) FLOAT32 column layout. */
        return tsdb_dec_decode(in, in_bytes, out, out_count);
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

/*
 * Outer-LZ stage as a reusable step: wrap `dom`/`dom_bytes` when lzlite saves
 * at least min_gain bytes, else copy the domain bytes through unchanged.
 * Writes into `dst`; returns the final byte count, or -1 if it does not fit.
 * *flags gets TSDB_BF_OUTER_LZ exactly when the wrap was taken.  Byte-for-byte
 * the same wire format the inline path below produces.
 */
static int lz_finalize(const uint8_t *dom, size_t dom_bytes,
                       uint8_t *dst, size_t dst_cap,
                       int min_gain, uint16_t *flags)
{
    *flags = 0;
    size_t lz_cap = tsdb_lzlite_max_output(dom_bytes);
    uint8_t *lz_buf = malloc(lz_cap);
    if (lz_buf) {
        size_t lz_bytes = 0;
        if (tsdb_lzlite_encode(dom, dom_bytes, lz_buf, lz_cap, &lz_bytes) >= 0) {
            size_t wrapped = 4 + lz_bytes;
            if ((int)dom_bytes - (int)wrapped >= min_gain && wrapped <= dst_cap) {
                uint32_t o = (uint32_t)dom_bytes;
                dst[0] = (uint8_t)(o & 0xFF);
                dst[1] = (uint8_t)((o >> 8) & 0xFF);
                dst[2] = (uint8_t)((o >> 16) & 0xFF);
                dst[3] = (uint8_t)((o >> 24) & 0xFF);
                memcpy(dst + 4, lz_buf, lz_bytes);
                free(lz_buf);
                *flags = TSDB_BF_OUTER_LZ;
                return (int)wrapped;
            }
        }
        free(lz_buf);
    }
    if (dom_bytes > dst_cap) return -1;
    memmove(dst, dom, dom_bytes);
    return (int)dom_bytes;
}

/*
 * TSDB_CODEC_DECIMAL=1 turns on the scaled-decimal FLOAT64 codec.  DEFAULT OFF,
 * and it has to stay that way for at least one release, because unlike every
 * other selection change in this file it is a FORMAT CHANGE: a block written
 * with codec id 12 is unreadable by any binary that does not know id 12.
 *
 * ROLLOUT ORDER — READERS BEFORE WRITERS:
 *   1. deploy the new binary everywhere with the flag unset.  It reads codec
 *      12 and writes exactly what the old one wrote (byte-identical output;
 *      dec_enabled() is the only thing gating the new path).
 *   2. only once NO node in the cluster runs an older binary, set
 *      TSDB_CODEC_DECIMAL=1 on the writers.
 *
 * Why the order is not merely advisable: src/cluster/rawblock.c ships the
 * COMPRESSED block plus its codec byte to peers verbatim (serialize at :79,
 * parse at :124) with no version negotiation, so a codec-12 block reaches an
 * old peer, is stored, and fails at read time there.
 *
 * Compaction is a writer too (compaction.c:615 re-encodes aged blocks through
 * this same function), so the flag does not only affect NEW rows.  Measured
 * both ways on a 60,000-row 2-decimal column: one compaction pass with the
 * flag on turned 8 pre-existing GORILLA/CHIMP blocks into 2 codec-12 blocks,
 * and one pass with it off turned 8 codec-12 blocks back into 2 non-DEC
 * blocks — bit-exact in both directions.  So there IS a downgrade path, but
 * it only covers partitions compaction chooses to rewrite; turning the flag
 * off does not by itself convert anything already on disk.
 *
 * Read on every call rather than cached once: it is one environ scan against
 * a 10-150 us block encode, and keeping it live is what lets a single test
 * process encode the same block both ways and diff the results.
 */
static int dec_enabled(void)
{
    const char *e = getenv("TSDB_CODEC_DECIMAL");
    return (e && e[0] && e[0] != '0') ? 1 : 0;
}

/*
 * Scaled-decimal FLOAT64 candidate, taken through the same outer-LZ stage as
 * every other candidate.  Writes into `dst`; returns the final byte count and
 * sets *dflags, or -1 when the block is not exactly representable at any
 * scale (NaN, ±inf, -0.0, subnormals, >8 decimals, huge magnitudes) or the
 * result does not fit.
 *
 * Both inner integer codecs are evaluated on FINAL size, for the same reason
 * SYMBOL is: PFOR is smaller pre-LZ on jittery data but bit-packed, so lzlite
 * finds nothing in it, while DoD's byte-aligned varints keep a repeating
 * column's period visible.  Measured on a 100-value 2-decimal cycle at
 * N=8192: choosing the inner codec pre-LZ gives 3,382 B, choosing it on final
 * size gives 1,744 B.
 */
static int encode_decimal_final(const void *in, size_t in_count,
                                uint8_t *dst, size_t dst_cap,
                                uint16_t *dflags, int min_gain)
{
    size_t cap = in_count * 11 + 64;   /* DoD worst case + the 2-byte prefix */
    int64_t *q    = malloc(in_count * sizeof(int64_t));
    uint8_t *raw  = malloc(cap);
    uint8_t *dfin = malloc(dst_cap ? dst_cap : 1);
    int ret = -1;
    if (!q || !raw || !dfin) goto done;

    int scale = tsdb_dec_find_scale((const double *)in, in_count, q);
    if (scale < 0) goto done;   /* not exactly representable — float path */

    static const tsdb_codec_t inners[2] = { TSDB_CODEC_DOD, TSDB_CODEC_PFOR };
    for (int i = 0; i < 2; i++) {
        size_t nb = 0;
        if (tsdb_dec_pack(q, in_count, scale, inners[i], raw, cap, &nb) != TSDB_OK)
            continue;
        uint16_t f = 0;
        int fin = lz_finalize(raw, nb, dfin, dst_cap, min_gain, &f);
        if (fin < 0 || (ret >= 0 && fin >= ret)) continue;
        memcpy(dst, dfin, (size_t)fin);
        *dflags = f;
        ret = fin;
    }

done:
    free(q); free(raw); free(dfin);
    return ret;
}

/*
 * SYMBOL: choose DICT vs PFOR on FINAL size, i.e. after the outer LZ stage.
 *
 * tsdb_codec_encode picks between them on domain size alone, but the two
 * compress very differently afterwards: DICT is byte-aligned varint, so lzlite
 * finds its repeating period, while PFOR is bit-packed and leaves lzlite
 * nothing to match.  On a clustered tag column that inverts the ranking —
 * measured DICT 8,275 B vs PFOR 7,556 B before LZ (so PFOR is taken), but
 * 58 B vs 2,925 B after it, a 50x miss.  End to end, a TSBS hostname column
 * went 157,291 -> 5,076 bytes (-96.8%).
 *
 * Neither codec dominates — PFOR+LZ still wins on single-value, small-cycle,
 * boolean and high-cardinality-random columns — so both are evaluated rather
 * than swapping the default.  Returns the final byte count, or -1 to fall back
 * to the caller's ordinary path.
 */
static int encode_symbol_final(const void *in, size_t in_count,
                               uint8_t *out, size_t out_cap,
                               tsdb_codec_t *out_codec, uint16_t *out_flags,
                               int min_gain)
{
    size_t cap = out_cap + 64;
    uint8_t *dbuf = malloc(cap), *pbuf = malloc(cap);
    uint8_t *dfin = malloc(cap), *pfin = malloc(cap);
    int ret = -1;
    if (!dbuf || !pbuf || !dfin || !pfin) goto done;

    size_t dbytes = 0, pbytes = 0;
    int have_d = (tsdb_dict_encode((const uint32_t *)in, in_count,
                                   dbuf, cap, &dbytes) == TSDB_OK);
    int have_p = (tsdb_pfor_encode((const uint32_t *)in, in_count,
                                   pbuf, cap, &pbytes) == TSDB_OK);
    if (!have_d && !have_p) goto done;

    uint16_t dflags = 0, pflags = 0;
    int dfinal = have_d ? lz_finalize(dbuf, dbytes, dfin, cap, min_gain, &dflags) : -1;
    int pfinal = have_p ? lz_finalize(pbuf, pbytes, pfin, cap, min_gain, &pflags) : -1;

    /* Ties go to DICT: equal size but cheaper to decode. */
    int use_dict = (dfinal >= 0) && (pfinal < 0 || dfinal <= pfinal);
    const uint8_t *src = use_dict ? dfin : pfin;
    int          final = use_dict ? dfinal : pfinal;
    if (final < 0 || (size_t)final > out_cap) goto done;

    memcpy(out, src, (size_t)final);
    *out_codec = use_dict ? TSDB_CODEC_DICT : TSDB_CODEC_PFOR;
    *out_flags = use_dict ? dflags : pflags;
    ret = final;

done:
    free(dbuf); free(pbuf); free(dfin); free(pfin);
    return ret;
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

    if (type == TSDB_TYPE_SYMBOL && in_count > 0) {
        int n = encode_symbol_final(in, in_count, out, out_cap,
                                    out_codec, out_flags, min_gain);
        if (n >= 0) return n;
        /* allocation failure — fall through to the ordinary path */
    }

    /* Step 0: scaled-decimal FLOAT64 (opt-in, format change — see
     * dec_enabled()).  Quantized telemetry re-expressed as round(v*10^s)
     * int64 lands at a fraction of what the XOR codecs produce, AND costs
     * less to encode, because it replaces two XOR passes with one integer
     * pass.  Taken only when it is at least 2x better than storing the
     * doubles raw; below that bar the block is not really decimal data and
     * the ordinary path (which is left byte-identical) decides.  Not folded
     * into tsdb_codec_encode: legacy callers assert the XOR-codec choice
     * there. */
    if (type == TSDB_TYPE_FLOAT64 && in_count > 0 && dec_enabled()) {
        uint8_t *dbuf = malloc(out_cap ? out_cap : 1);
        if (dbuf) {
            uint16_t dflags = 0;
            int dfinal = encode_decimal_final(in, in_count, dbuf, out_cap,
                                              &dflags, min_gain);
            if (dfinal >= 0 && (size_t)dfinal * 2 <= in_count * 8) {
                memcpy(out, dbuf, (size_t)dfinal);
                free(dbuf);
                *out_codec = TSDB_CODEC_DEC;
                *out_flags = dflags;
                return dfinal;
            }
            free(dbuf);
        }
    }

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

    /* Early-exit: skip the outer LZ pass for a float block only when BOTH halves
     * of the gate agree — the domain output is dense (>= FLOAT_LZ_SKIP_PCT% of
     * raw) AND a probe over the raw input finds no byte-level redundancy.
     *
     * Density alone mispredicts quantized telemetry, where lzlite still takes
     * 31-50% off a domain output sitting at 68-89% of raw.  The probe alone
     * mispredicts a block whose sampled windows miss a compressible region, and
     * density catches those.  Requiring both means this can only ever skip a
     * subset of what density alone skipped, so for every input the encoded size
     * is <= what the old rule produced.  Skipping produces byte-identical output
     * (flags = 0). */
    if ((codec == TSDB_CODEC_GORILLA || codec == TSDB_CODEC_CHIMP ||
         codec == TSDB_CODEC_CHIMP128) && in_count > 0) {
        size_t raw_bytes = in_count * 8;
        if ((size_t)domain_bytes * 100 >= raw_bytes * FLOAT_LZ_SKIP_PCT &&
            lz_probe_futile(in, raw_bytes)) {
            return domain_bytes;  /* dense float with no raw redundancy */
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
