/* pfor.h — PFOR-Delta (Patched Frame Of Reference) integer compression.
 *
 * Compresses arrays of uint32 or int64 values using:
 *   1. Delta encoding (to decorrelate adjacent values)
 *   2. FOR (Frame-Of-Reference): subtract per-block minimum
 *   3. Bit-packing via bp128 (128-value blocks at uniform bit-width b)
 *   4. Exception list for outlier values that exceed b bits
 *
 * Block layout (per 128-value group):
 *   [2 bytes header]
 *     bits 0..4  : b-1 (5 bits, encodes b in range 1..32; 0 means b=1)
 *     bits 5..12 : n_exceptions (8 bits, 0..128)
 *     bit 13     : raw_block (1 bit, set if delta overflow → raw 4B/value)
 *     bits 14..15: reserved
 *   [4 bytes base]: minimum value (FOR base), always present
 *   [b*16 bytes]: bit-packed residuals (omitted for raw blocks)
 *   [n_exc * 5 bytes]: exception list (1 byte pos + 4 bytes full value each)
 *     -- for raw blocks: 128*4 bytes raw values instead
 *
 * Stream prologue:
 *   [4 bytes, little-endian]: total element count n
 *
 * int64 path:
 *   - Apply delta + zigzag encoding to produce uint64 values.
 *   - If all deltas fit in uint32, proceed as uint32 PFOR.
 *   - If any delta exceeds uint32, mark that block as RAW (8 bytes/value).
 *   - On decode, reverse zigzag then prefix-sum to recover int64.
 */
#ifndef TSDB_COMPRESS_PFOR_H
#define TSDB_COMPRESS_PFOR_H

#include <stddef.h>
#include <stdint.h>
#include "../../include/tsdb.h"

/* Encode n uint32 values into out buffer.
 * Returns TSDB_OK on success; *out_bytes set to bytes written.
 * Returns TSDB_ERR_OVERFLOW if out buffer too small.
 */
int tsdb_pfor_encode(const uint32_t *in, size_t n,
                     uint8_t *out, size_t cap, size_t *out_bytes);

/* Decode n uint32 values from in buffer.
 * Returns TSDB_OK on success.
 * Returns TSDB_ERR_CORRUPT on malformed data.
 */
int tsdb_pfor_decode(const uint8_t *in, size_t n_bytes,
                     uint32_t *out, size_t n);

/* Encode n int64 values. Uses zigzag-delta internally.
 * Blocks where delta fits in uint32 use PFOR; others use RAW (8B/value).
 */
int tsdb_pfor_encode_i64(const int64_t *in, size_t n,
                          uint8_t *out, size_t cap, size_t *out_bytes);

/* Decode n int64 values. */
int tsdb_pfor_decode_i64(const uint8_t *in, size_t n_bytes,
                          int64_t *out, size_t n);

#endif /* TSDB_COMPRESS_PFOR_H */
