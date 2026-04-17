/* bits.h — bitstream reader/writer for variable-length codecs.
 *
 * Writer buffers up to 64 bits in a scratch register and spills to a caller-
 * owned byte buffer. Reader mirrors. Both are single-pass, no seek.
 */
#ifndef TSDB_CORE_BITS_H
#define TSDB_CORE_BITS_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

typedef struct {
    uint8_t *buf;       /* caller-owned output buffer, writable */
    size_t   cap;       /* capacity in bytes */
    size_t   bytes;     /* whole bytes written */
    uint64_t scratch;   /* pending bits (LSB-first) */
    unsigned nbits;     /* number of bits in scratch (0..63) */
    int      overflow;  /* non-zero if ran out of space */
} tsdb_bw_t;

typedef struct {
    const uint8_t *buf;
    size_t         cap;        /* total bytes available */
    size_t         bytes;      /* bytes consumed into scratch */
    uint64_t       scratch;
    unsigned       nbits;
    int            underflow;
} tsdb_br_t;

static inline void tsdb_bw_init(tsdb_bw_t *w, uint8_t *buf, size_t cap) {
    w->buf = buf; w->cap = cap; w->bytes = 0; w->scratch = 0; w->nbits = 0; w->overflow = 0;
}

/* Write `n` bits (n ≤ 64). LSB-first write order. */
static inline void tsdb_bw_put(tsdb_bw_t *w, uint64_t val, unsigned n) {
    if (n == 0) return;
    if (n < 64) val &= ((uint64_t)1 << n) - 1;
    w->scratch |= val << w->nbits;
    /* If overflow: we need to split */
    if (w->nbits + n < 64) {
        w->nbits += n;
        return;
    }
    /* Flush 8 bytes */
    if (w->bytes + 8 > w->cap) { w->overflow = 1; return; }
    uint64_t out = w->scratch;
    for (int i = 0; i < 8; i++) w->buf[w->bytes + i] = (uint8_t)(out >> (8 * i));
    w->bytes += 8;
    unsigned shift = 64 - w->nbits;
    if (shift == 64 || n == shift) {
        w->scratch = 0;
        w->nbits = (w->nbits + n) - 64;
    } else {
        w->scratch = val >> shift;
        w->nbits = n - shift;
    }
}

/* Finalize — pad final byte with zeros. Returns bytes written or -1 on overflow. */
static inline ssize_t tsdb_bw_finish(tsdb_bw_t *w) {
    if (w->overflow) return -1;
    unsigned need = (w->nbits + 7) / 8;
    if (w->bytes + need > w->cap) { w->overflow = 1; return -1; }
    for (unsigned i = 0; i < need; i++) {
        w->buf[w->bytes + i] = (uint8_t)(w->scratch >> (8 * i));
    }
    w->bytes += need;
    w->scratch = 0;
    w->nbits = 0;
    return (ssize_t)w->bytes;
}

static inline void tsdb_br_init(tsdb_br_t *r, const uint8_t *buf, size_t cap) {
    r->buf = buf; r->cap = cap; r->bytes = 0; r->scratch = 0; r->nbits = 0; r->underflow = 0;
}

/* Refill scratch to have at least `n` bits available. */
static inline void tsdb_br_refill(tsdb_br_t *r, unsigned n) {
    while (r->nbits < n) {
        if (r->bytes >= r->cap) {
            /* Pad with zeros but flag underflow if too short */
            if (r->nbits + 8 < n && r->bytes >= r->cap) r->underflow = 1;
            r->nbits += 8; /* effectively zero */
            if (r->nbits >= 64) { r->nbits = 64; return; }
            continue;
        }
        r->scratch |= (uint64_t)r->buf[r->bytes] << r->nbits;
        r->bytes++;
        r->nbits += 8;
    }
}

static inline uint64_t tsdb_br_get(tsdb_br_t *r, unsigned n) {
    if (n == 0) return 0;
    if (n > 64) n = 64;
    tsdb_br_refill(r, n);
    uint64_t v = r->scratch & (n == 64 ? ~(uint64_t)0 : (((uint64_t)1 << n) - 1));
    if (n == 64) r->scratch = 0;
    else r->scratch >>= n;
    r->nbits -= n;
    return v;
}

/* Zigzag helpers. Map signed int64 to unsigned so that small magnitudes map
 * to small unsigneds regardless of sign. */
static inline uint64_t tsdb_zigzag_enc(int64_t v) {
    return ((uint64_t)v << 1) ^ (uint64_t)(v >> 63);
}

static inline int64_t tsdb_zigzag_dec(uint64_t v) {
    return (int64_t)((v >> 1) ^ -(int64_t)(v & 1));
}

/* Number of leading zeros for 64-bit. Portable fallback. */
static inline unsigned tsdb_clz64(uint64_t x) {
    if (x == 0) return 64;
#if defined(__GNUC__) || defined(__clang__)
    return (unsigned)__builtin_clzll(x);
#else
    unsigned n = 0;
    if (!(x >> 32)) { n += 32; x <<= 32; }
    if (!(x >> 48)) { n += 16; x <<= 16; }
    if (!(x >> 56)) { n += 8;  x <<= 8;  }
    if (!(x >> 60)) { n += 4;  x <<= 4;  }
    if (!(x >> 62)) { n += 2;  x <<= 2;  }
    if (!(x >> 63)) { n += 1; }
    return n;
#endif
}

static inline unsigned tsdb_ctz64(uint64_t x) {
    if (x == 0) return 64;
#if defined(__GNUC__) || defined(__clang__)
    return (unsigned)__builtin_ctzll(x);
#else
    unsigned n = 0;
    while (!(x & 1)) { n++; x >>= 1; }
    return n;
#endif
}

#endif
