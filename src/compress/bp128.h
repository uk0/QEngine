/* bp128.h — SIMD-accelerated 128-integer bitpacking (Lemire & Boytsov style).
 *
 * Packs/unpacks groups of exactly 128 uint32 values at a fixed bit-width b
 * (1 ≤ b ≤ 32).  Each packed group occupies exactly b * 16 bytes (b * 128 bits).
 *
 * Memory layout: 4-way interleaved for SIMD throughput.
 *   Position k → lane (k % 4), slot (k / 4) within that lane.
 *   A 128-bit register holds 4 uint32 lanes simultaneously.
 *
 * APIs:
 *   tsdb_bp128_pack(src, dst, b)   — pack 128 uint32s from src into dst
 *   tsdb_bp128_unpack(src, dst, b) — unpack 128 uint32s from src into dst
 *
 * Both functions return TSDB_OK (0) on success, TSDB_ERR_INVAL on bad args.
 * dst must have room for b*16 bytes (pack) or 128*4 bytes (unpack).
 */
#ifndef TSDB_COMPRESS_BP128_H
#define TSDB_COMPRESS_BP128_H

#include <stdint.h>
#include "../../include/tsdb.h"

/* Pack 128 uint32s at bit-width b (1..32) into dst.
 * dst must point to at least b*16 bytes of writable memory.
 * src must point to exactly 128 uint32 values.
 */
int tsdb_bp128_pack(const uint32_t *src, uint32_t *dst, unsigned b);

/* Unpack 128 uint32s at bit-width b (1..32) from src into dst.
 * src must point to at least b*16 bytes.
 * dst must point to at least 128*4 = 512 bytes.
 */
int tsdb_bp128_unpack(const uint32_t *src, uint32_t *dst, unsigned b);

#endif /* TSDB_COMPRESS_BP128_H */
