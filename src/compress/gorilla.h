/* gorilla.h — Gorilla XOR float64 compression. */
#ifndef TSDB_COMPRESS_GORILLA_H
#define TSDB_COMPRESS_GORILLA_H

#include <stddef.h>
#include <stdint.h>

int tsdb_gorilla_encode(const double *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes);
int tsdb_gorilla_decode(const uint8_t *in, size_t n_bytes, double *out, size_t n);

#endif /* TSDB_COMPRESS_GORILLA_H */
