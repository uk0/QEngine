/* types.h — internal types shared across modules */
#ifndef TSDB_CORE_TYPES_H
#define TSDB_CORE_TYPES_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "../../include/tsdb.h"

/* Cache line assumed. Padding helper. */
#define TSDB_CACHELINE 64
#define TSDB_ALIGN(n, a) (((n) + (a) - 1) & ~((a) - 1))

/* Block size — 8192 points/block, well-known cache-friendly value. */
#define TSDB_BLOCK_POINTS 8192

/* Maximum columns per table (hard cap). */
#define TSDB_MAX_COLS 256

/* Maximum table name / column name length (bytes, not chars). */
#define TSDB_MAX_NAME 63

/* Number of write shards for memtable. Power of two. */
#define TSDB_SHARDS 16

/* Compression codec identifiers (fits in 1 byte, stored in block header). */
typedef enum {
    TSDB_CODEC_NONE    = 0,
    TSDB_CODEC_DOD     = 1, /* delta-of-delta + zigzag + varint (timestamps) */
    TSDB_CODEC_GORILLA = 2, /* XOR float64 */
    TSDB_CODEC_FOR     = 3, /* frame of reference integer */
    TSDB_CODEC_DICT    = 4, /* dictionary-encoded uint32 payload */
    TSDB_CODEC_RAW     = 5, /* raw copy, used for debugging/fallback */
    TSDB_CODEC_CHIMP   = 6, /* Chimp XOR float64 (VLDB 2022) */
    TSDB_CODEC_LZLITE  = 7, /* LZ77-style block wrapper (optional outer layer) */
    TSDB_CODEC_BP128   = 9, /* SIMD-BP128 bit-packing (Lemire & Boytsov) */
    TSDB_CODEC_PFOR    = 10 /* PFOR-Delta (patched frame of reference) */
} tsdb_codec_t;

/* Size in bytes of a physical value for a given type (fixed-width columns). */
static inline size_t tsdb_type_width(tsdb_type_t t) {
    switch (t) {
    case TSDB_TYPE_TIMESTAMP: return 8;
    case TSDB_TYPE_INT64:     return 8;
    case TSDB_TYPE_FLOAT64:   return 8;
    case TSDB_TYPE_SYMBOL:    return 4; /* uint32 dict code */
    default:                  return 0;
    }
}

static inline bool tsdb_type_is_fixed(tsdb_type_t t) {
    return tsdb_type_width(t) > 0;
}

static inline const char *tsdb_type_name(tsdb_type_t t) {
    switch (t) {
    case TSDB_TYPE_TIMESTAMP: return "TIMESTAMP";
    case TSDB_TYPE_INT64:     return "INT64";
    case TSDB_TYPE_FLOAT64:   return "FLOAT64";
    case TSDB_TYPE_SYMBOL:    return "SYMBOL";
    default:                  return "?";
    }
}

#endif
