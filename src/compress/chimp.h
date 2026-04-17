/* chimp.h — Chimp XOR float64 compression (VLDB 2022).
 *
 * Panagiotis Liakos et al. "Chimp: Efficient Lossless Floating Point
 * Compression for Time Series Databases," VLDB 2022.
 *
 * This implements Chimp (not Chimp128). Key improvements over Gorilla:
 *  - Leading-zero LUT: quantise clz to 8 buckets, encode in 3 bits (vs 5)
 *  - Trailing-zero stripping: when XOR trailing >= threshold, strip trailing
 *    zeros and encode only significant bits
 *  - 2-element state: prev_value and prev_leading
 */
#ifndef TSDB_COMPRESS_CHIMP_H
#define TSDB_COMPRESS_CHIMP_H

#include <stddef.h>
#include <stdint.h>

int tsdb_chimp_encode(const double *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes);
int tsdb_chimp_decode(const uint8_t *in, size_t n_bytes, double *out, size_t n);

#endif /* TSDB_COMPRESS_CHIMP_H */
