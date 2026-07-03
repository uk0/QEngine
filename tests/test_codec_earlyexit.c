/* test_codec_earlyexit.c — float compression CPU reduction: correctness + honest cost.
 *
 * Two data-driven changes to the float codec path, both verified here with NUMBERS:
 *
 *   A. Chimp128 dropped from the encode best-of-N (now Gorilla vs Chimp best-of-2).
 *      Measured across 6 distributions, Chimp128 won 0 and best-of-2 matched
 *      best-of-3 byte-for-byte.  This test re-confirms: adaptive never emits
 *      CHIMP128 and its size equals the true min(gorilla,chimp,chimp128).
 *
 *   B. Outer LZ skipped for a dense float block (domain >= 50% of raw), where it
 *      always loses.  Constant float (domain ~1.6% of raw) stays below the
 *      threshold and keeps its ~60x LZ win.
 *
 * Plus the safety net the original (rejected) ratio-probe design would have
 * broken: sine is poorly compressed by Gorilla (~95% of raw) yet Chimp halves it.
 * The best-of-2 path MUST still pick Chimp here — proven below.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "../src/compress/codec.h"
#include "../src/compress/gorilla.h"
#include "../src/compress/chimp.h"
#include "../src/compress/chimp128.h"
#include "../src/core/types.h"
#include "../include/tsdb.h"

#define FAIL(m) do { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); abort(); } while (0)
#define ASSERT(c) do { if (!(c)) FAIL(#c); } while (0)

static const char *codec_name(tsdb_codec_t c) {
    switch (c) {
    case TSDB_CODEC_GORILLA:  return "GORILLA";
    case TSDB_CODEC_CHIMP:    return "CHIMP";
    case TSDB_CODEC_CHIMP128: return "CHIMP128";
    default:                  return "other";
    }
}

/* Size of the old unconditional best-of-3 (gorilla/chimp/chimp128). */
static size_t best_of_3(const double *v, size_t n, tsdb_codec_t *which) {
    size_t cap = n * 11 + 64;
    uint8_t *a = malloc(cap), *b = malloc(cap), *c = malloc(cap);
    ASSERT(a && b && c);
    size_t ga = 0, gb = 0, gc = 0;
    int ra = tsdb_gorilla_encode(v, n, a, cap, &ga);
    int rb = tsdb_chimp_encode(v, n, b, cap, &gb);
    int rc = tsdb_chimp128_encode(v, n, c, cap, &gc);
    size_t best = (size_t)-1; tsdb_codec_t w = TSDB_CODEC_GORILLA;
    if (ra == TSDB_OK && ga < best) { best = ga; w = TSDB_CODEC_GORILLA; }
    if (rb == TSDB_OK && gb < best) { best = gb; w = TSDB_CODEC_CHIMP; }
    if (rc == TSDB_OK && gc < best) { best = gc; w = TSDB_CODEC_CHIMP128; }
    free(a); free(b); free(c);
    if (which) *which = w;
    return best;
}

/* Adaptive encode + decode with a bit-exact round-trip check. */
static int enc_dec(const double *v, size_t n, tsdb_codec_t *codec, uint16_t *flags) {
    static uint8_t buf[1 << 22];
    int nb = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, v, n, buf, sizeof(buf), codec, flags);
    ASSERT(nb >= 0);
    double *out = malloc(n * sizeof(double));
    ASSERT(out);
    ASSERT(tsdb_codec_decode_adaptive(*codec, TSDB_TYPE_FLOAT64, *flags,
                                      buf, (size_t)nb, out, n) == TSDB_OK);
    for (size_t i = 0; i < n; i++) {
        uint64_t x, y; memcpy(&x, &v[i], 8); memcpy(&y, &out[i], 8);
        if (x != y) { fprintf(stderr, "FAIL: float mismatch at %zu\n", i); abort(); }
    }
    free(out);
    return nb;
}

/* Encode one distribution; verify losslessness, no-CHIMP128, best-of-2==best-of-3,
 * and the expected LZ decision.  want_lz: 1 = expect OUTER_LZ set, 0 = expect skipped. */
static void check(const char *label, const double *v, size_t n, int want_lz) {
    tsdb_codec_t codec = TSDB_CODEC_NONE, b3w = TSDB_CODEC_NONE; uint16_t flags = 0;
    int nb = enc_dec(v, n, &codec, &flags);
    size_t b3 = best_of_3(v, n, &b3w);
    int has_lz = (flags & TSDB_BF_OUTER_LZ) ? 1 : 0;
    printf("%-16s adaptive=%-8s %6d B (%.3f B/pt) lz=%-3s | best-of-3=%-8s %6zu B (won by %s)\n",
           label, codec_name(codec), nb, (double)nb / n, has_lz ? "YES" : "no",
           codec_name(b3w), b3, codec_name(b3w));
    /* A. encode must never choose CHIMP128. */
    ASSERT(codec != TSDB_CODEC_CHIMP128);
    /* A. dropping CHIMP128 cost zero bytes: best-of-2 result equals best-of-3.
     *    (When LZ is applied the wrapped size can only be <= domain, so compare
     *    the plain domain size by re-deriving it: with LZ off, nb is the domain
     *    size directly; with LZ on, the win only makes nb smaller — still <= b3.)
     *    RAW: when every XOR codec EXPANDS past 8 B/value the adaptive path now
     *    stores the block raw (zero-copy-readable) — strictly smaller than b3,
     *    so dropping CHIMP128 still cost nothing. */
    if (codec == TSDB_CODEC_RAW) ASSERT((size_t)nb == n * 8 && (size_t)nb <= b3);
    else if (!has_lz)            ASSERT((size_t)nb == b3);
    else                         ASSERT((size_t)nb <= b3);
    /* B. LZ decision matches expectation. */
    ASSERT(has_lz == want_lz);
}

int main(void) {
    printf("=== test_codec_earlyexit ===\n");
    const size_t N = 8192;
    double *v = malloc(N * sizeof(double));
    ASSERT(v);
    uint64_t st = 0x9e3779b97f4a7c15ULL;
    #define NEXT (st = st * 6364136223846793005ULL + 1442695040888963407ULL)

    /* Varying floats — domain >= 50% of raw, LZ skipped. */
    for (size_t i = 0; i < N; i++) { NEXT; v[i] = 100.0 + (double)(st >> 11) / (double)(1ULL << 53) * 50.0; }
    check("price[100,150]", v, N, /*want_lz=*/0);

    for (size_t i = 0; i < N; i++) { NEXT; uint64_t b = st; b &= ~(0x7FFULL << 52); b |= ((uint64_t)(512 + (st % 512)) << 52); memcpy(&v[i], &b, 8); }
    check("full-entropy", v, N, 0);

    { uint32_t r = 0xfeedcafe; double a = 50.0;
      for (size_t i = 0; i < N; i++) { r = r * 1664525u + 1013904223u; a += ((double)(r >> 8) / (double)(1u << 24) - 0.5) * 2.0; if (a < 0) a = 0; if (a > 100) a = 100; v[i] = a; } }
    check("random-walk", v, N, 0);

    /* SINE — the case the rejected ratio-probe design would have wrecked.
     * Gorilla compresses it poorly (~95% raw, would look "incompressible") but
     * Chimp roughly halves it.  best-of-2 must still pick CHIMP. */
    for (size_t i = 0; i < N; i++) {
        double x = 6.28318530717958647692 * (double)i / (double)N, s = 0, term = x, xx = x * x;
        for (int k = 0; k < 9; k++) { s += term; term *= -xx / (double)((2*k+2) * (2*k+3)); }
        v[i] = s;
    }
    tsdb_codec_t sc = TSDB_CODEC_NONE; uint16_t sf = 0;
    int snb = enc_dec(v, N, &sc, &sf);
    size_t sg = 0; { size_t cap = N*11+64; uint8_t *g = malloc(cap); tsdb_gorilla_encode(v, N, g, cap, &sg); free(g); }
    printf("%-16s adaptive=%-8s %6d B (%.3f B/pt) | gorilla-only=%zu B  => chimp saves %.1f%%\n",
           "sine", codec_name(sc), snb, (double)snb / N, sg, 100.0 * ((double)sg - (double)snb) / (double)sg);
    ASSERT(sc == TSDB_CODEC_CHIMP);          /* smooth data still routed to Chimp */
    ASSERT((size_t)snb < sg * 90 / 100);     /* and it's a real win (>10%) over gorilla */

    /* CONSTANT — domain ~1.6% of raw, LZ wins ~60x and MUST be kept. */
    for (size_t i = 0; i < N; i++) v[i] = 3.14159265358979;
    tsdb_codec_t cc = TSDB_CODEC_NONE; uint16_t cf = 0;
    int cnb = enc_dec(v, N, &cc, &cf);
    printf("%-16s adaptive=%-8s %6d B (%.4f B/pt) lz=%s\n",
           "constant", codec_name(cc), cnb, (double)cnb / N, (cf & TSDB_BF_OUTER_LZ) ? "YES" : "no");
    ASSERT(cf & TSDB_BF_OUTER_LZ);           /* LZ preserved on constant float */
    ASSERT((size_t)cnb < N);                 /* < 1 byte/pt — the 60x win survives */

    free(v);
    printf("[PASS] best-of-2 == best-of-3 (no CHIMP128); LZ kept for constant, skipped for varying; sine->Chimp\n");
    return 0;
}
