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

/* Block-header flag bits (stored in BlockHeader.flags u16). */
#define TSDB_BF_OUTER_LZ  (1u<<0)   /* block data is wrapped with lzlite  */
#define TSDB_BF_NOT_NULL  (1u<<1)   /* all values present (no null bitmap) */
#define TSDB_BF_HAS_BLOOM (1u<<2)   /* BlockIndexEntry._reserved holds a 64-bit bloom */

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

/*
 * Adaptive encode: selects best domain codec then optionally wraps with lzlite.
 *
 * Same signature as tsdb_codec_encode plus out_flags:
 *   out_codec - domain codec chosen (DOD/PFOR/GORILLA/CHIMP/CHIMP128/DICT)
 *   out_flags - TSDB_BF_OUTER_LZ set if lzlite wrapper was applied
 *
 * Wire format when TSDB_BF_OUTER_LZ is set:
 *   [u32 LE: original domain-codec byte count][lzlite-compressed bytes]
 *
 * Returns: number of bytes written (possibly wrapped), or negative on error.
 */
int tsdb_codec_encode_adaptive(tsdb_type_t type,
                               const void *in, size_t in_count,
                               uint8_t *out, size_t out_cap,
                               tsdb_codec_t *out_codec,
                               uint16_t *out_flags);

/*
 * L2 / cold-tier variant of tsdb_codec_encode_adaptive.
 *
 * Identical domain-codec selection, but the outer-lzlite wrapper is
 * applied whenever it saves at least `min_gain` bytes (instead of the
 * default 16-byte floor).  Compaction recompresses aged/cold blocks
 * where read frequency is low, so even a few bytes of size reduction is
 * worth the marginally slower decode — pass min_gain=1 there to capture
 * the sub-16-byte LZ wins the hot path intentionally skips.
 *
 * min_gain <= 0 is clamped to 1 (a 0/negative gain is never worth the
 * wrap).  Output wire format and the TSDB_BF_OUTER_LZ flag are identical
 * to the default path, so tsdb_codec_decode_adaptive handles both with
 * no changes.
 */
int tsdb_codec_encode_adaptive_ex(tsdb_type_t type,
                                  const void *in, size_t in_count,
                                  uint8_t *out, size_t out_cap,
                                  tsdb_codec_t *out_codec,
                                  uint16_t *out_flags,
                                  int min_gain);

/*
 * Adaptive decode: handles both legacy (flags=0) and lzlite-wrapped blocks.
 *
 * flags     - TSDB_BF_* bitmask from block header
 * If TSDB_BF_OUTER_LZ is NOT set, delegates directly to tsdb_codec_decode.
 *
 * Returns: TSDB_OK on success, negative on error.
 */
int tsdb_codec_decode_adaptive(tsdb_codec_t codec,
                               tsdb_type_t type,
                               uint16_t flags,
                               const uint8_t *in, size_t in_bytes,
                               void *out, size_t out_count);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_COMPRESS_CODEC_H */
