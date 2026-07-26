/* bits.h — bitstream reader/writer for variable-length codecs.
 *
 * Writer buffers up to 64 bits in a scratch register and spills to a caller-
 * owned byte buffer. Reader mirrors. Both are single-pass, no seek.
 */
#ifndef TSDB_CORE_BITS_H
#define TSDB_CORE_BITS_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>
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

/* Refill scratch to have at least `n` bits available.
 *
 * Bulk path: when a whole 8-byte load fits inside the buffer and scratch still
 * has room for at least one byte, absorb up to 8 bytes with one unaligned
 * 64-bit load instead of stepping the byte loop 1..8 times.  `take` is how many
 * WHOLE bytes fit above `nbits`; the load always brings in 8, so the bits past
 * `nbits + 8*take` must be masked back off — the reader's contract is that
 * everything above `nbits` in scratch reads as zero, and leaving those bits
 * behind corrupts every later read.
 *
 * The bulk path deliberately falls THROUGH to the byte loop instead of
 * returning: `take` is 0 once nbits >= 57 (so the branch is skipped entirely
 * and the loop must still run), and even when it does run it only tops scratch
 * up to 57..64 bits, which is short of a 58..64-bit request.  The byte loop
 * below stays the single authority on "enough bits", on end-of-buffer zero
 * padding, and on the underflow flag.
 *
 * Bit-for-bit equivalent to the pure byte loop.  Both keep
 *     scratch == bits[off, off + nbits)   with zeros above nbits
 *     off     == 8*bytes - nbits          (off = next unread bit)
 * and nbits is congruent to -off mod 8 in both, so the bulk path only changes
 * HOW MANY bytes are banked ahead — never the value of the next `n` bits, never
 * which byte a >56-bit request truncates against, and never the point at which
 * the buffer runs out (padding starts at bytes == cap with nbits == 8*cap - off
 * either way).  The 8-byte load is fully in bounds, so the tail near the end of
 * the buffer is handled by the byte loop exactly as before.
 */
static inline void tsdb_br_refill(tsdb_br_t *r, unsigned n) {
    if (r->nbits < n && r->nbits <= 56 && r->bytes + 8 <= r->cap) {
        uint64_t w;
        memcpy(&w, r->buf + r->bytes, 8);           /* unaligned-safe */
#if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        w = __builtin_bswap64(w);                   /* stream is LSB-first */
#endif
        unsigned take = (64u - r->nbits) >> 3;      /* whole bytes, 1..8 */
        unsigned have = r->nbits + (take << 3);     /* 57..64 */
        r->scratch |= w << r->nbits;
        /* have is 57..64, so the shift count is 0..7 — never the UB that
         * ((uint64_t)1 << 64) would be. */
        r->scratch &= ~(uint64_t)0 >> (64u - have);
        r->bytes += take;
        r->nbits  = have;
    }
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

/* ---- 64-bit Bloom filter (3 FNV-1a variant hashes) ----------------------
 *
 * A single uint64_t holds the 64-bit bitvector.
 * Three independent hash functions are computed from the input value:
 *   h0 = FNV-1a(v, seed=0x811c9dc5)
 *   h1 = FNV-1a(v, seed=0x01000193)   (h0 XOR rotated)
 *   h2 = FNV-1a(v, seed=0x6b432795)
 * Each hash selects one bit (hash % 64).
 *
 * False-positive probability at k=3 hashes, m=64 bits:
 *   ~p = (1 - e^(-k*n/m))^k
 *   For n=10: ~2%   For n=3: ~0.1%
 */
static inline uint32_t tsdb_bloom_fnv1a(uint32_t v, uint32_t seed) {
    /* FNV-1a 32-bit: process 4 bytes of v */
    uint32_t h = seed;
    h ^= (uint8_t)(v);         h *= 0x01000193u;
    h ^= (uint8_t)(v >> 8);    h *= 0x01000193u;
    h ^= (uint8_t)(v >> 16);   h *= 0x01000193u;
    h ^= (uint8_t)(v >> 24);   h *= 0x01000193u;
    return h;
}

/* Set 3 bits in the 64-bit bloom for symbol code v. */
static inline uint64_t tsdb_bloom_add(uint64_t bloom, uint32_t v) {
    uint32_t h0 = tsdb_bloom_fnv1a(v, 0x811c9dc5u);
    uint32_t h1 = tsdb_bloom_fnv1a(v, 0x01000193u) ^ (h0 >> 5);
    uint32_t h2 = tsdb_bloom_fnv1a(v, 0x6b432795u) ^ (h0 >> 11);
    bloom |= (uint64_t)1 << (h0 & 63u);
    bloom |= (uint64_t)1 << (h1 & 63u);
    bloom |= (uint64_t)1 << (h2 & 63u);
    return bloom;
}

/* Test 3 bits for symbol code v.
 * Returns 0 if v is DEFINITELY NOT in the set (safe to skip block).
 * Returns 1 if v MIGHT be in the set (must scan the block). */
static inline int tsdb_bloom_test(uint64_t bloom, uint32_t v) {
    uint32_t h0 = tsdb_bloom_fnv1a(v, 0x811c9dc5u);
    uint32_t h1 = tsdb_bloom_fnv1a(v, 0x01000193u) ^ (h0 >> 5);
    uint32_t h2 = tsdb_bloom_fnv1a(v, 0x6b432795u) ^ (h0 >> 11);
    if (!(bloom & ((uint64_t)1 << (h0 & 63u)))) return 0;
    if (!(bloom & ((uint64_t)1 << (h1 & 63u)))) return 0;
    if (!(bloom & ((uint64_t)1 << (h2 & 63u)))) return 0;
    return 1;
}

#endif
