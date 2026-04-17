/* dict.h — dictionary-encoded uint32 column compression.
 * Strategy: delta + zigzag + bit-varint.
 */
#ifndef TSDB_COMPRESS_DICT_H
#define TSDB_COMPRESS_DICT_H

#include <stddef.h>
#include <stdint.h>

int tsdb_dict_encode(const uint32_t *in, size_t n, uint8_t *out, size_t cap, size_t *out_bytes);
int tsdb_dict_decode(const uint8_t *in, size_t n_bytes, uint32_t *out, size_t n);

#endif /* TSDB_COMPRESS_DICT_H */
