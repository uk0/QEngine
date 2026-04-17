/* dod.h — delta-of-delta encoding for int64 sequences (timestamps, INT64). */
#ifndef TSDB_COMPRESS_DOD_H
#define TSDB_COMPRESS_DOD_H

#include <stddef.h>
#include <stdint.h>

int tsdb_dod_encode(const int64_t *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes);
int tsdb_dod_decode(const uint8_t *in, size_t n_bytes, int64_t *out, size_t n);

#endif /* TSDB_COMPRESS_DOD_H */
