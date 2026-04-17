/* codec.h — compression codec interface.
 *
 * This header defines the contract between the storage layer and the
 * compression module. The storage module assumes this interface exists
 * but does not implement it.
 */
#ifndef TSDB_COMPRESS_CODEC_H
#define TSDB_COMPRESS_CODEC_H

#include "../core/types.h"
#include "../../include/tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Encode an array of `in_count` values of the given type into `out`.
 *
 * in       - pointer to input values (typed: int64_t[], double[], uint32_t[])
 * in_count - number of values
 * out      - caller-supplied output buffer
 * out_cap  - capacity of out buffer in bytes
 * out_codec- codec actually used is written here
 *
 * Returns: number of bytes written, or negative on error.
 */
int tsdb_codec_encode(tsdb_type_t type,
                      const void *in, size_t in_count,
                      uint8_t *out, size_t out_cap,
                      tsdb_codec_t *out_codec);

/*
 * Decode `out_count` values from `in` (in_bytes bytes of compressed data).
 *
 * codec     - codec used during encoding (from block header)
 * type      - target value type
 * in        - compressed bytes
 * in_bytes  - number of input bytes
 * out       - caller-supplied output buffer (capacity >= out_count * type_width)
 * out_count - number of values to decode
 *
 * Returns: TSDB_OK (0) on success, negative on error.
 */
int tsdb_codec_decode(tsdb_codec_t codec,
                      tsdb_type_t type,
                      const uint8_t *in, size_t in_bytes,
                      void *out, size_t out_count);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_COMPRESS_CODEC_H */
