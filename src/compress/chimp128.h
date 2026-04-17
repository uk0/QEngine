/* chimp128.h — Chimp128 XOR float64 compression (VLDB 2022).
 *
 * Chimp128 extends Chimp with a 128-element sliding window ring buffer
 * and a hash table for selecting the best reference value for XOR encoding.
 *
 * This allows encoding against any of the 128 most recent values rather
 * than just the immediately preceding value, yielding ~10-15% better
 * compression on irregular time series compared to base Chimp.
 */
#ifndef TSDB_COMPRESS_CHIMP128_H
#define TSDB_COMPRESS_CHIMP128_H

#include <stddef.h>
#include <stdint.h>

int tsdb_chimp128_encode(const double *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes);
int tsdb_chimp128_decode(const uint8_t *in, size_t n_bytes, double *out, size_t n);

#endif /* TSDB_COMPRESS_CHIMP128_H */
