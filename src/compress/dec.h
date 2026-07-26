/* dec.h — scaled-decimal FLOAT64 codec (TSDB_CODEC_DEC, id 12).
 *
 * Telemetry doubles are almost never full-precision: a CPU-usage or price
 * column carries 0-3 decimal digits.  The XOR float codecs (Gorilla/Chimp)
 * cannot see that — they work on the mantissa bits, where a 2-decimal value
 * looks like noise.  Re-expressing the column as round(v * 10^s) int64 and
 * handing it to the integer codecs recovers the structure.
 *
 * Wire format (what lands in the block payload):
 *
 *     byte 0      scale exponent s, 0..TSDB_DEC_MAX_SCALE
 *     byte 1      inner codec id: TSDB_CODEC_DOD or TSDB_CODEC_PFOR
 *     byte 2..    the inner codec's int64 payload
 *
 * LOSSLESS OR NOTHING.  A scale is accepted only when EVERY value in the
 * block reproduces its exact 8-byte pattern through the decoder's own
 * expression, `(double)q / 10^s`.  Anything that does not — NaN, ±inf, -0.0,
 * a subnormal, more decimals than the largest scale, a magnitude past the
 * exact-integer range — rejects the whole block, and the caller keeps the
 * existing float path.  There is no lossy mode and no per-value exception
 * list.
 *
 * FORMAT CHANGE.  Codec id 12 was unassigned before this codec, so a binary
 * that predates it hits the `default:` arm of tsdb_codec_decode and returns
 * TSDB_ERR_UNSUPPORTED — a loud failure, not a misread.  Blocks written with
 * this codec are therefore NOT readable by older binaries; see the rollout
 * note above dec_enabled() in codec.c.
 */
#ifndef TSDB_COMPRESS_DEC_H
#define TSDB_COMPRESS_DEC_H

#include <stddef.h>
#include <stdint.h>
#include "../core/types.h"
#include "../../include/tsdb.h"

/* Largest scale exponent tried.  10^8 keeps round(v*10^s) inside the range
 * where int64 <-> double is exact in both directions for the magnitudes real
 * telemetry uses; beyond it the search costs more than it recovers. */
#define TSDB_DEC_MAX_SCALE 8

/*
 * Find the smallest s in [0, TSDB_DEC_MAX_SCALE] for which every value of
 * `v` round-trips bit-exactly, and write the quantised int64s to `out`
 * (capacity n).  Returns s, or -1 when no scale represents the block.
 *
 * On -1 the contents of `out` are unspecified.
 */
int tsdb_dec_find_scale(const double *v, size_t n, int64_t *out);

/*
 * Serialise a quantised block (`q`, `scale` — as produced by
 * tsdb_dec_find_scale) with `inner` as the integer codec: TSDB_CODEC_DOD or
 * TSDB_CODEC_PFOR.
 *
 * Both are offered because neither dominates AFTER the outer lzlite stage:
 * DoD is byte-aligned varint, so lzlite finds a repeating column's period in
 * it, while PFOR is bit-packed and leaves lzlite nothing to match — yet PFOR
 * is much smaller pre-LZ on jittery data.  The caller picks on final size.
 *
 * Returns TSDB_OK and sets *out_bytes; TSDB_ERR_OVERFLOW when the result does
 * not fit in `cap`; TSDB_ERR_INVAL for a scale or inner codec out of range.
 */
int tsdb_dec_pack(const int64_t *q, size_t n, int scale, tsdb_codec_t inner,
                  uint8_t *out, size_t cap, size_t *out_bytes);

/*
 * Decode n values written by tsdb_dec_pack.  `out` receives n doubles.
 *
 * Returns TSDB_OK, or TSDB_ERR_CORRUPT for a truncated payload, an
 * out-of-range scale byte, or an inner codec id this codec never emits.
 */
int tsdb_dec_decode(const uint8_t *in, size_t in_bytes,
                    void *out, size_t out_count);

#endif /* TSDB_COMPRESS_DEC_H */
