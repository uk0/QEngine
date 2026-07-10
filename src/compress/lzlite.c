/* lzlite.c — lightweight LZ77-style block compressor.
 *
 * Algorithm: sliding window (64 KB) + hash chain (4-byte hash → position table).
 * No hard match-length cap — match_len can extend as far as data allows,
 * encoded with the 255-chain overflow varint (same as LZ4).
 *
 * Token byte layout: (lit_nibble << 4) | match_nibble
 *   lit_nibble   : 0-14 = exact literal count; 15 = count-15 follows as varint
 *   match_nibble : 0-14 = (match_len - MIN_MATCH); 15 = overflow follows as varint
 *
 * Encoder allocates ~384 KB on the heap for the hash table + chain; stack
 * usage is O(1).
 *
 * All decode paths are bounds-checked; malicious input returns TSDB_ERR_CORRUPT.
 */
#include "lzlite.h"
#include "../../include/tsdb.h"

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ constants */

#define WINDOW_BITS  16
#define WINDOW_SIZE  (1u << WINDOW_BITS)   /* 64 KB                        */
#define WINDOW_MASK  (WINDOW_SIZE - 1)

#define HASH_BITS    16
#define HASH_SIZE    (1u << HASH_BITS)     /* 65536 hash buckets           */
#define HASH_MASK    (HASH_SIZE - 1)

#define MIN_MATCH    4                      /* minimum match length (bytes) */
#define NIL          0xFFFFu               /* sentinel: no entry           */

/* LZ4-style overflow sentinel for nibble fields. */
#define NIBBLE_MAX   15u

/* ------------------------------------------------------------------ helpers */

/* 4-byte hash. Spread bits so adjacent positions map to different buckets. */
static inline uint32_t lz_hash4(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    /* Knuth multiplicative hash, 32-bit. */
    return (v * 2654435761u) >> (32 - HASH_BITS);
}

/*
 * Write an LZ4-style overflow varint (255-chain).
 * Emits a sequence of 0xFF bytes for each full 255, then the remainder.
 * Returns TSDB_ERR_OVERFLOW if out of buffer space.
 */
static int write_overflow(uint8_t *out, size_t out_cap, size_t *op,
                          size_t extra)
{
    while (extra >= 255) {
        if (*op >= out_cap) return TSDB_ERR_OVERFLOW;
        out[(*op)++] = 0xFF;
        extra -= 255;
    }
    if (*op >= out_cap) return TSDB_ERR_OVERFLOW;
    out[(*op)++] = (uint8_t)extra;
    return TSDB_OK;
}

/*
 * Read an LZ4-style overflow varint from compressed input.
 * Adds into *acc.  Returns TSDB_ERR_CORRUPT if input is truncated.
 */
static int read_overflow(const uint8_t *in, size_t in_n, size_t *ip,
                         size_t *acc)
{
    uint8_t b;
    do {
        if (*ip >= in_n) return TSDB_ERR_CORRUPT;
        b = in[(*ip)++];
        *acc += (size_t)b;
    } while (b == 0xFF);
    return TSDB_OK;
}

/* ---------------------------------------------------------------- encoder */

/*
 * Hash table structures allocated on the heap by the encoder.
 * head[h]    : most-recent input position (uint16_t, wraps mod 64K) with
 *              that hash, or NIL.
 * prev[pos&MASK] : previous input position with the same hash (chain).
 *
 * Position 0 is reserved as the "not-yet-seen" sentinel.  Because we use
 * 16-bit positions that wrap at 64K, chains naturally stay within the window.
 */
typedef struct {
    uint16_t head[HASH_SIZE];
    uint16_t prev[WINDOW_SIZE];
} lz_ht_t;

size_t tsdb_lzlite_max_output(size_t in_n)
{
    /* LZ4 bound: worst case is literal-only, which adds ~1 byte overhead per
     * 255 literals, plus a fixed 16-byte header allowance. */
    return in_n + (in_n / 255) + 16;
}

int tsdb_lzlite_encode(const uint8_t *in, size_t in_n,
                       uint8_t *out, size_t out_cap, size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;

    if (in_n == 0) return TSDB_OK;
    if (!in) return TSDB_ERR_INVAL;
    if (!out && out_cap > 0) return TSDB_ERR_INVAL;

    /* Check worst-case early so inner loop doesn't need per-byte checks on the
     * literal copy path — we still check on token/overflow writes. */
    if (out_cap < tsdb_lzlite_max_output(in_n)) return TSDB_ERR_OVERFLOW;

    /* Hash table: one lazily-allocated 256 KB scratch per thread, reused
     * across calls.  This encoder runs once per column block on the flush
     * path, and the former per-call malloc+free of 256 KB dominated the cost
     * of hashing a ~16-32 KB domain buffer.  The head[] reset below is what
     * guarantees byte-identical output vs the fresh-alloc version; prev[] is
     * only ever read at indices linked in the current call. */
    static __thread lz_ht_t *ht = NULL;
    if (!ht) {
        ht = (lz_ht_t *)malloc(sizeof(lz_ht_t));
        if (!ht) return TSDB_ERR_NOMEM;
    }
    memset(ht->head, 0xFF, sizeof(ht->head)); /* NIL = 0xFFFF */

    size_t ip   = 0;  /* input position (read cursor)      */
    size_t op   = 0;  /* output position (write cursor)    */
    size_t lit_start = 0; /* start of current unmatched literal run */

    /*
     * Main loop: advance ip, find best match, emit (literal-run, match) pair.
     * We keep `in_n - MIN_MATCH` as the "can-still-hash" bound.
     */
    while (ip < in_n) {
        size_t best_len = 0;
        size_t best_off = 0;

        /* Only attempt matching when ≥ MIN_MATCH bytes remain and we have
         * enough look-ahead for a 4-byte hash. */
        if (ip + MIN_MATCH <= in_n) {
            uint32_t h  = lz_hash4(in + ip);
            uint16_t cur16 = (uint16_t)(ip & WINDOW_MASK);
            uint16_t ref16 = ht->head[h];

            /* Walk the chain (limit depth to avoid O(n²) on pathological
             * inputs).  64 iterations balances speed vs ratio well. */
            int chain_limit = 64;
            while (ref16 != NIL && chain_limit-- > 0) {
                /* Compute the actual input position for this chain entry.
                 * Because positions wrap at 64K, we reconstruct the full
                 * position by anchoring to ip's upper bits. */
                size_t ref_pos;
                {
                    /* ref16 is a 16-bit position; recover the full offset. */
                    uint16_t delta16 = (uint16_t)(cur16 - ref16);
                    if (delta16 == 0) break; /* same position, stop */
                    size_t offset = (size_t)delta16;
                    if (offset > ip) break;  /* would go before start */
                    ref_pos = ip - offset;
                }

                /* Verify 4-byte match (cheap before extending). */
                if (ref_pos + MIN_MATCH > in_n) {
                    ref16 = ht->prev[ref16];
                    continue;
                }
                if (in[ref_pos]     != in[ip]     ||
                    in[ref_pos + 1] != in[ip + 1] ||
                    in[ref_pos + 2] != in[ip + 2] ||
                    in[ref_pos + 3] != in[ip + 3]) {
                    ref16 = ht->prev[ref16];
                    continue;
                }

                /* Extend match as far as possible. */
                size_t ml = MIN_MATCH;
                size_t max_ml = in_n - ip;
                while (ml < max_ml && in[ref_pos + ml] == in[ip + ml])
                    ml++;

                if (ml > best_len) {
                    best_len = ml;
                    best_off = ip - ref_pos;
                    /* Could break early if best_len is large enough; for
                     * correctness we let the chain continue for better ratio. */
                    if (best_len >= 256) break; /* good enough */
                }

                ref16 = ht->prev[ref16];
            }

            /* Insert current position into hash table. */
            ht->prev[cur16] = ht->head[h];
            ht->head[h]     = cur16;
        }

        if (best_len < MIN_MATCH) {
            /* No useful match: advance one byte and keep accumulating
             * literals. */
            ip++;
            continue;
        }

        /* ---- Emit (literal-run, match) sequence ---- */
        size_t lit_len   = ip - lit_start;
        size_t match_len = best_len;

        /* Token byte. */
        uint8_t lit_nib   = (lit_len   >= NIBBLE_MAX) ? (uint8_t)NIBBLE_MAX
                                                       : (uint8_t)lit_len;
        uint8_t match_nib = (match_len - MIN_MATCH >= NIBBLE_MAX)
                              ? (uint8_t)NIBBLE_MAX
                              : (uint8_t)(match_len - MIN_MATCH);
        uint8_t token = (uint8_t)((lit_nib << 4) | match_nib);

        /* We already checked out_cap >= max_output, so simple writes are safe,
         * but we double-check op stays in bounds for the overflow varint paths
         * (which can be lengthy on huge data). */
        if (op >= out_cap) { return TSDB_ERR_OVERFLOW; }
        out[op++] = token;

        /* Literal overflow varint. */
        if (lit_nib == NIBBLE_MAX) {
            int rc = write_overflow(out, out_cap, &op, lit_len - NIBBLE_MAX);
            if (rc) { return rc; }
        }

        /* Match length overflow varint. */
        if (match_nib == NIBBLE_MAX) {
            size_t match_extra = match_len - MIN_MATCH - NIBBLE_MAX;
            int rc = write_overflow(out, out_cap, &op, match_extra);
            if (rc) { return rc; }
        }

        /* Offset: 2 bytes little-endian. */
        if (op + 2 > out_cap) { return TSDB_ERR_OVERFLOW; }
        uint16_t off16 = (uint16_t)best_off;
        out[op++] = (uint8_t)(off16 & 0xFF);
        out[op++] = (uint8_t)(off16 >> 8);

        /* Literal bytes. */
        if (op + lit_len > out_cap) { return TSDB_ERR_OVERFLOW; }
        memcpy(out + op, in + lit_start, lit_len);
        op += lit_len;

        /* Advance past match. */
        /* Insert hashes for positions inside the match (lazy: just advance). */
        size_t match_end = ip + match_len;
        ip++;
        while (ip < match_end && ip + MIN_MATCH <= in_n) {
            uint32_t h2  = lz_hash4(in + ip);
            uint16_t p16 = (uint16_t)(ip & WINDOW_MASK);
            ht->prev[p16] = ht->head[h2];
            ht->head[h2]  = p16;
            ip++;
        }
        ip = match_end;
        lit_start = ip;
    }

    /* ---- Final literal-only sequence (no match fields). ---- */
    {
        size_t lit_len = in_n - lit_start;
        uint8_t lit_nib = (lit_len >= NIBBLE_MAX) ? (uint8_t)NIBBLE_MAX
                                                  : (uint8_t)lit_len;
        /* match_nibble == 0 for the last sequence (no match). */
        uint8_t token = (uint8_t)(lit_nib << 4);

        if (op >= out_cap) { return TSDB_ERR_OVERFLOW; }
        out[op++] = token;

        if (lit_nib == NIBBLE_MAX) {
            int rc = write_overflow(out, out_cap, &op, lit_len - NIBBLE_MAX);
            if (rc) { return rc; }
        }

        if (op + lit_len > out_cap) { return TSDB_ERR_OVERFLOW; }
        memcpy(out + op, in + lit_start, lit_len);
        op += lit_len;
    }
    *out_bytes = op;
    return (int)op;
}

/* ---------------------------------------------------------------- decoder */

int tsdb_lzlite_decode(const uint8_t *in, size_t in_n,
                       uint8_t *out, size_t out_cap, size_t *out_bytes)
{
    if (!out_bytes) return TSDB_ERR_INVAL;
    *out_bytes = 0;

    if (in_n == 0) return TSDB_OK;
    if (!in) return TSDB_ERR_INVAL;
    if (!out && out_cap > 0) return TSDB_ERR_INVAL;

    size_t ip = 0;   /* input position  */
    size_t op = 0;   /* output position */

    while (ip < in_n) {
        /* ---- Read token ---- */
        uint8_t token = in[ip++];
        size_t lit_nib   = (size_t)(token >> 4);
        size_t match_nib = (size_t)(token & 0x0F);

        /* ---- Literal length ---- */
        size_t lit_len = lit_nib;
        if (lit_nib == NIBBLE_MAX) {
            int rc = read_overflow(in, in_n, &ip, &lit_len);
            if (rc) return rc;
        }

        /* Determine whether this sequence includes a back-reference match.
         *
         * When match_nib != 0, this is always a match sequence (encoder never
         * writes match_nib != 0 without a following offset).
         *
         * When match_nib == 0, it is ambiguous: it could be:
         *   (a) The terminal literal-only sequence (no offset follows), OR
         *   (b) A real match of exactly MIN_MATCH bytes (match_nib = 0 encodes
         *       match_len - MIN_MATCH == 0, i.e., match_len = 4).
         *
         * We disambiguate by checking whether there is room for an offset (2
         * bytes) after the current literal run.  If there is, treat as (b).
         */
        int has_match = (match_nib != 0) ||
                        (ip + lit_len + 2 <= in_n);

        /* ---- Match length ---- */
        size_t match_len = 0;
        if (has_match) {
            match_len = MIN_MATCH + match_nib;
            if (match_nib == NIBBLE_MAX) {
                int rc = read_overflow(in, in_n, &ip, &match_len);
                if (rc) return rc;
            }
        }

        /* ---- Offset (2 bytes LE) — only present when has_match ---- */
        size_t offset = 0;
        if (has_match) {
            if (ip + 2 > in_n) return TSDB_ERR_CORRUPT;
            offset  = (size_t)in[ip];
            offset |= (size_t)in[ip + 1] << 8;
            ip += 2;

            if (offset == 0) return TSDB_ERR_CORRUPT;   /* invalid back-ref */
        }

        /* ---- Copy literals ---- */
        if (lit_len > 0) {
            if (ip + lit_len > in_n)  return TSDB_ERR_CORRUPT;
            if (op + lit_len > out_cap) return TSDB_ERR_OVERFLOW;
            memcpy(out + op, in + ip, lit_len);
            op += lit_len;
            ip += lit_len;
        }

        /* ---- Copy match ---- */
        if (match_len >= MIN_MATCH) {
            if (offset > op) return TSDB_ERR_CORRUPT; /* ref before start */
            if (op + match_len > out_cap) return TSDB_ERR_OVERFLOW;

            size_t src = op - offset;
            /* Must copy byte-by-byte when offset < match_len to correctly
             * handle overlapping (run-length) expansions, e.g., offset=1
             * repeats a single byte `match_len` times. */
            if (offset < match_len) {
                for (size_t i = 0; i < match_len; i++)
                    out[op + i] = out[src + i];
            } else {
                memcpy(out + op, out + src, match_len);
            }
            op += match_len;
        }
    }

    *out_bytes = op;
    return TSDB_OK;
}
