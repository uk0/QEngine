/* codec.c — unified codec dispatch. */
#include "codec.h"
#include "dod.h"
#include "gorilla.h"
#include "chimp.h"
#include "dict.h"
#include "pfor.h"
#include "../core/types.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

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
        /* Timestamps: always use DoD — ideal for monotone nanosecond series. */
        rc = tsdb_dod_encode((const int64_t *)in, in_count, out, out_cap, &out_bytes);
        if (rc) return rc;
        *out_codec = TSDB_CODEC_DOD;
        return (int)out_bytes;
    }
    case TSDB_TYPE_INT64: {
        /* INT64: try DoD first (into out), then PFOR-Delta (into tmp); pick smaller. */
        rc = tsdb_dod_encode((const int64_t *)in, in_count, out, out_cap, &out_bytes);
        if (rc) return rc;
        size_t dod_bytes = out_bytes;

        /* Try PFOR into a temp buffer. */
        size_t tmp_cap = out_cap + 64;
        uint8_t *tmp = malloc(tmp_cap);
        if (!tmp) {
            /* malloc failed; use DoD result already in out. */
            *out_codec = TSDB_CODEC_DOD;
            return (int)dod_bytes;
        }
        size_t pfor_bytes = 0;
        int pfor_rc = tsdb_pfor_encode_i64((const int64_t *)in, in_count,
                                            tmp, tmp_cap, &pfor_bytes);
        if (pfor_rc == TSDB_OK && pfor_bytes < dod_bytes) {
            /* PFOR is smaller: copy into out. */
            if (pfor_bytes <= out_cap) {
                memcpy(out, tmp, pfor_bytes);
                free(tmp);
                *out_codec = TSDB_CODEC_PFOR;
                return (int)pfor_bytes;
            }
        }
        free(tmp);
        *out_codec = TSDB_CODEC_DOD;
        return (int)dod_bytes;
    }
    case TSDB_TYPE_FLOAT64: {
        /*
         * Try both Gorilla and Chimp; pick the smaller result.
         * Gorilla wins on constant/monotone integer doubles;
         * Chimp wins on irregular real-valued time series.
         * We allocate a temporary buffer for the losing candidate.
         */
        size_t chimp_bytes = 0, gorilla_bytes = 0;

        /* First: try Chimp directly into out. */
        rc = tsdb_chimp_encode((const double *)in, in_count, out, out_cap, &chimp_bytes);
        if (rc) return rc;

        /* Second: try Gorilla into a heap buffer. */
        size_t tmp_cap = out_cap + 16;
        uint8_t *tmp = malloc(tmp_cap);
        if (!tmp) {
            /* malloc failed; fall back to Chimp-only result already in out. */
            *out_codec = TSDB_CODEC_CHIMP;
            return (int)chimp_bytes;
        }

        rc = tsdb_gorilla_encode((const double *)in, in_count, tmp, tmp_cap, &gorilla_bytes);
        if (rc != TSDB_OK) {
            free(tmp);
            /* Gorilla failed; use Chimp result already in out. */
            *out_codec = TSDB_CODEC_CHIMP;
            return (int)chimp_bytes;
        }

        if (gorilla_bytes < chimp_bytes) {
            /* Gorilla is smaller: copy its output over out. */
            if (gorilla_bytes > out_cap) {
                free(tmp);
                return TSDB_ERR_OVERFLOW;
            }
            memcpy(out, tmp, gorilla_bytes);
            free(tmp);
            *out_codec = TSDB_CODEC_GORILLA;
            return (int)gorilla_bytes;
        }

        /* Chimp is smaller (or equal): result already in out. */
        free(tmp);
        *out_codec = TSDB_CODEC_CHIMP;
        return (int)chimp_bytes;
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
