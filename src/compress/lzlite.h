/* lzlite.h — lightweight LZ77-style block compressor.
 *
 * A self-contained, pure C11 compressor with no third-party dependencies.
 * Designed as an optional block-level wrapper over domain codecs (DoD, Gorilla,
 * Dict) to exploit residual locality in already-encoded data.
 *
 * Format (LZ4-inspired):
 *   Sequence = [ token:u8 ] [ lit_extra varint ] [ match_extra varint ]
 *              [ offset:u16le ] [ literal bytes... ]
 *   Final sequence has literals only (no offset/match fields).
 *
 *   token = (lit_nibble << 4) | match_nibble
 *     lit_nibble   : # of literal bytes (0-14), or 15 = overflow via varint
 *     match_nibble : (match_len - MIN_MATCH) clamped to 0-14, or 15 = overflow
 *
 *   Overlapping copies (offset < match_len) are handled byte-by-byte for
 *   correct run-length expansion behaviour.
 */
#ifndef TSDB_COMPRESS_LZLITE_H
#define TSDB_COMPRESS_LZLITE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Compress `in_n` bytes from `in` into `out`.
 *
 * Returns: number of bytes written to `out`, or a negative tsdb_err_t code.
 *   TSDB_ERR_INVAL   — null pointer with non-zero size
 *   TSDB_ERR_OVERFLOW — out_cap too small (use tsdb_lzlite_max_output)
 *
 * Sets *out_bytes to the number of bytes written (same as return value on
 * success).
 */
int tsdb_lzlite_encode(const uint8_t *in, size_t in_n,
                       uint8_t *out, size_t out_cap, size_t *out_bytes);

/*
 * Decompress `in_n` bytes from `in` into `out`.
 *
 * Returns: TSDB_OK (0) on success, or a negative tsdb_err_t code.
 *   TSDB_ERR_INVAL   — null pointer with non-zero size
 *   TSDB_ERR_CORRUPT — malformed stream (bad offset, truncated, etc.)
 *   TSDB_ERR_OVERFLOW — output buffer too small
 *
 * Sets *out_bytes to actual decompressed size on success.
 */
int tsdb_lzlite_decode(const uint8_t *in, size_t in_n,
                       uint8_t *out, size_t out_cap, size_t *out_bytes);

/*
 * Return a conservative upper bound on encoded output size for `in_n` input
 * bytes. Caller should allocate at least this many bytes for the `out` buffer
 * passed to tsdb_lzlite_encode.
 */
size_t tsdb_lzlite_max_output(size_t in_n);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_COMPRESS_LZLITE_H */
