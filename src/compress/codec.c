/* codec.c — unified codec dispatch. */
#include "codec.h"
#include "dod.h"
#include "gorilla.h"
#include "dict.h"
#include "../core/types.h"
#include "../../include/tsdb.h"
#include <string.h>
#include <stdint.h>
#include <stddef.h>

/*
 * Routing:
 *   TIMESTAMP -> DOD (int64)
 *   INT64     -> DOD (int64)
 *   FLOAT64   -> GORILLA (double)
 *   SYMBOL    -> DICT (uint32)
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
    case TSDB_TYPE_TIMESTAMP:
    case TSDB_TYPE_INT64: {
        rc = tsdb_dod_encode((const int64_t *)in, in_count, out, out_cap, &out_bytes);
        if (rc) return rc;
        *out_codec = TSDB_CODEC_DOD;
        return (int)out_bytes;
    }
    case TSDB_TYPE_FLOAT64: {
        rc = tsdb_gorilla_encode((const double *)in, in_count, out, out_cap, &out_bytes);
        if (rc) return rc;
        *out_codec = TSDB_CODEC_GORILLA;
        return (int)out_bytes;
    }
    case TSDB_TYPE_SYMBOL: {
        rc = tsdb_dict_encode((const uint32_t *)in, in_count, out, out_cap, &out_bytes);
        if (rc) return rc;
        *out_codec = TSDB_CODEC_DICT;
        return (int)out_bytes;
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
    case TSDB_CODEC_DICT:
        return tsdb_dict_decode(in, in_bytes, (uint32_t *)out, out_count);
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
