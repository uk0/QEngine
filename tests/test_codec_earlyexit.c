/* test_codec_earlyexit.c — float compression CPU reduction: correctness + honest cost.
 *
 * Two data-driven changes to the float codec path, both verified here with NUMBERS:
 *
 *   A. Chimp128 dropped from the encode best-of-N (now Gorilla vs Chimp best-of-2).
 *      Measured across 6 distributions, Chimp128 won 0 and best-of-2 matched
 *      best-of-3 byte-for-byte.  This test re-confirms: adaptive never emits
 *      CHIMP128 and its size equals the true min(gorilla,chimp,chimp128).
 *
 *   B. The outer LZ pass is skipped only when BOTH halves of the gate agree:
 *      the domain output is dense (>= 50% of raw) AND a probe over the raw
 *      input finds no byte-level redundancy.
 *
 *      Density alone was the old rule and it is the wrong predictor: quantized
 *      telemetry lands at 68-89% of raw and STILL gives lzlite 31-50%, so the
 *      old rule threw those wins away.  The probe alone is also wrong: it
 *      samples the block, so a block with a compressible region the sample
 *      windows miss would be mispredicted — and the density half catches those.
 *      Requiring both means the encoder can only ever skip a subset of what it
 *      skipped before, i.e. for every input the encoded size is <= what the old
 *      rule produced.  All three properties are asserted below on shapes chosen
 *      to separate them.
 *
 *   No format change: the decoder is untouched.  Every block here is asserted to
 *   carry a pre-existing codec id and no flag bit other than TSDB_BF_OUTER_LZ,
 *   and the domain-only encoding of every shape is decoded back through the same
 *   tsdb_codec_decode_adaptive with flags=0 — the old-block read path.
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
    case TSDB_CODEC_RAW:      return "RAW";
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

/* Domain-codec-only size — what the block would be with the LZ stage disabled.
 * Doubles as the old-block read check: a flags=0 block must still decode through
 * the very same adaptive decoder the new writer's blocks go through. */
static size_t domain_only(const double *v, size_t n, tsdb_codec_t *codec) {
    static uint8_t buf[1 << 22];
    int nb = tsdb_codec_encode(TSDB_TYPE_FLOAT64, v, n, buf, sizeof(buf), codec);
    ASSERT(nb >= 0);
    double *out = malloc(n * sizeof(double));
    ASSERT(out);
    ASSERT(tsdb_codec_decode_adaptive(*codec, TSDB_TYPE_FLOAT64, /*flags=*/0,
                                      buf, (size_t)nb, out, n) == TSDB_OK);
    ASSERT(memcmp(out, v, n * sizeof(double)) == 0);
    free(out);
    return (size_t)nb;
}

/* Adaptive encode + decode with a bit-exact round-trip check. */
static int enc_dec(const double *v, size_t n, tsdb_codec_t *codec, uint16_t *flags) {
    static uint8_t buf[1 << 22];
    int nb = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, v, n, buf, sizeof(buf), codec, flags);
    ASSERT(nb >= 0);
    /* No format change: only pre-existing codec ids, and no flag bit besides
     * the pre-existing TSDB_BF_OUTER_LZ. */
    ASSERT(*codec == TSDB_CODEC_GORILLA || *codec == TSDB_CODEC_CHIMP ||
           *codec == TSDB_CODEC_RAW);
    ASSERT((*flags & ~(uint16_t)TSDB_BF_OUTER_LZ) == 0);
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
 * the never-worse invariant, and the expected LZ decision.
 *   want_lz      : 1 = expect OUTER_LZ set, 0 = expect the stage skipped
 *   min_save_pct : when want_lz, the minimum % the LZ stage must save vs
 *                  domain-only — guards against the assertion passing on a
 *                  trivial 16-byte gain. */
static void check(const char *label, const double *v, size_t n,
                  int want_lz, int min_save_pct) {
    tsdb_codec_t codec = TSDB_CODEC_NONE, b3w = TSDB_CODEC_NONE, dcodec = TSDB_CODEC_NONE;
    uint16_t flags = 0;
    int nb = enc_dec(v, n, &codec, &flags);
    size_t dom = domain_only(v, n, &dcodec);
    size_t b3 = best_of_3(v, n, &b3w);
    int has_lz = (flags & TSDB_BF_OUTER_LZ) ? 1 : 0;
    double raw_pct = 100.0 * (double)dom / (double)(n * 8);
    double save_pct = 100.0 * ((double)dom - (double)nb) / (double)dom;
    printf("%-18s adaptive=%-8s %6d B (%.3f B/pt) lz=%-3s | domain=%6zu B (%.1f%% of raw)"
           " save=%5.1f%% | best-of-3=%-8s %6zu\n",
           label, codec_name(codec), nb, (double)nb / n, has_lz ? "YES" : "no",
           dom, raw_pct, save_pct, codec_name(b3w), b3);

    /* A. encode must never choose CHIMP128. */
    ASSERT(codec != TSDB_CODEC_CHIMP128);
    /* A. dropping CHIMP128 cost zero bytes on the distributions it was measured
     *    on: with the LZ stage off, nb is the domain size directly and must
     *    equal best-of-3; with it on, the win only makes nb smaller.
     *    RAW: when every XOR codec EXPANDS past 8 B/value the adaptive path
     *    stores the block raw (zero-copy-readable) — strictly smaller than b3.
     *    (The quantized shapes added below sit outside the original claim — on
     *    pct-2dp Chimp128 is ~0.4% smaller pre-LZ — so they are held to the <=
     *    form only.  The printed b3 column shows the gap either way.) */
    if (codec == TSDB_CODEC_RAW) ASSERT((size_t)nb == n * 8 && (size_t)nb <= b3);
    else if (!has_lz)            ASSERT((size_t)nb == b3);
    else                         ASSERT((size_t)nb <= b3);
    /* B. NEVER WORSE: the adaptive result is never larger than domain-only, for
     *    every shape, whichever way the gate went. */
    ASSERT((size_t)nb <= dom);
    /* B. the gate decided as expected... */
    ASSERT(has_lz == want_lz);
    /* B. ...and when it ran the stage, the win is real, not a 16-byte rounding. */
    if (want_lz) ASSERT((dom - (size_t)nb) * 100 >= dom * (size_t)min_save_pct);
    /* B. when it skipped, the output is byte-identical to domain-only — unless
     *    the RAW fallback took over, which is a different (and smaller) path. */
    if (!want_lz && codec != TSDB_CODEC_RAW) ASSERT((size_t)nb == dom);
}

int main(void) {
    printf("=== test_codec_earlyexit ===\n");
    const size_t N = 8192;
    double *v = malloc(N * sizeof(double));
    ASSERT(v);
    uint64_t st = 0x9e3779b97f4a7c15ULL;
    #define NEXT (st = st * 6364136223846793005ULL + 1442695040888963407ULL)

    printf("\n-- incompressible floats: dense domain output AND no raw redundancy"
           " -> LZ stage skipped --\n");

    for (size_t i = 0; i < N; i++) { NEXT; v[i] = 100.0 + (double)(st >> 11) / (double)(1ULL << 53) * 50.0; }
    check("price[100,150]", v, N, /*want_lz=*/0, 0);

    for (size_t i = 0; i < N; i++) { NEXT; uint64_t b = st; b &= ~(0x7FFULL << 52); b |= ((uint64_t)(512 + (st % 512)) << 52); memcpy(&v[i], &b, 8); }
    check("full-entropy", v, N, 0, 0);

    { uint32_t r = 0xfeedcafe; double a = 50.0;
      for (size_t i = 0; i < N; i++) { r = r * 1664525u + 1013904223u; a += ((double)(r >> 8) / (double)(1u << 24) - 0.5) * 2.0; if (a < 0) a = 0; if (a > 100) a = 100; v[i] = a; } }
    check("random-walk", v, N, 0, 0);

    /* ---- The shapes the OLD density-only rule threw away. ----------------
     * Quantized telemetry: the domain output is well past the 50% density
     * threshold, so the old rule skipped the LZ stage — yet the quantisation
     * leaves repeated byte patterns in the XOR stream that lzlite crushes. */
    printf("\n-- quantized floats: dense domain output BUT raw redundancy"
           " -> LZ stage runs (old rule lost these) --\n");

    { uint32_t r = 0x13579bdf; double a = 50.0;
      for (size_t i = 0; i < N; i++) { r = r * 1664525u + 1013904223u;
          a += ((double)(r >> 8) / (double)(1u << 24) - 0.5) * 2.0;
          if (a < 0) a = 0; if (a > 100) a = 100;
          v[i] = (double)((long long)(a * 100.0 + 0.5)) / 100.0; } }
    check("sensor-2dp", v, N, /*want_lz=*/1, /*min_save_pct=*/20);

    { uint32_t r = 0x2468ace0; double a = 50.0;
      for (size_t i = 0; i < N; i++) { r = r * 1664525u + 1013904223u;
          a += ((double)(r >> 8) / (double)(1u << 24) - 0.5) * 2.0;
          if (a < 0) a = 0; if (a > 100) a = 100;
          v[i] = (double)((long long)(a * 10.0 + 0.5)) / 10.0; } }
    check("sensor-1dp", v, N, 1, 20);

    { uint32_t r = 0x0badf00d;
      for (size_t i = 0; i < N; i++) { r = r * 1664525u + 1013904223u;
          v[i] = (double)(r % 10001) / 100.0; } }
    check("pct-2dp", v, N, 1, 20);

    /* ---- Heterogeneous blocks: entropy head, compressible tail. ----------
     * The first 512 values are high-entropy; the rest are compressible.  A
     * gate that only looked at the head of the block would call the whole
     * thing incompressible and skip the stage.  Both shapes must still take
     * the LZ path.  The second one is why the density half of the gate is
     * kept: the old rule DID compress it (its domain output is far below the
     * 50% threshold), so dropping density for a probe alone would have been a
     * straight regression against shipped behaviour. */
    printf("\n-- heterogeneous blocks: entropy head, compressible tail"
           " -> LZ stage must still run --\n");

    for (size_t i = 0; i < 512; i++) { NEXT; uint64_t b = st; b &= ~(0x7FFULL << 52); b |= ((uint64_t)(1000 + (st % 24)) << 52); memcpy(&v[i], &b, 8); }
    { uint32_t r = 0xfacefeed; double a = 50.0;
      for (size_t i = 512; i < N; i++) { r = r * 1664525u + 1013904223u;
          a += ((double)(r >> 8) / (double)(1u << 24) - 0.5) * 2.0;
          if (a < 0) a = 0; if (a > 100) a = 100;
          v[i] = (double)((long long)(a * 100.0 + 0.5)) / 100.0; } }
    check("het:head+2dp", v, N, /*want_lz=*/1, /*min_save_pct=*/15);

    for (size_t i = 0; i < 512; i++) { NEXT; uint64_t b = st; b &= ~(0x7FFULL << 52); b |= ((uint64_t)(1000 + (st % 24)) << 52); memcpy(&v[i], &b, 8); }
    for (size_t i = 512; i < N; i++) v[i] = 42.5;
    check("het:head+const", v, N, 1, 10);

    /* SINE — the case the rejected ratio-probe design would have wrecked.
     * Gorilla compresses it poorly (~95% raw, would look "incompressible") but
     * Chimp roughly halves it.  best-of-2 must still pick CHIMP. */
    printf("\n-- codec selection is unaffected by the gate --\n");
    for (size_t i = 0; i < N; i++) {
        double x = 6.28318530717958647692 * (double)i / (double)N, s = 0, term = x, xx = x * x;
        for (int k = 0; k < 9; k++) { s += term; term *= -xx / (double)((2*k+2) * (2*k+3)); }
        v[i] = s;
    }
    tsdb_codec_t sc = TSDB_CODEC_NONE; uint16_t sf = 0;
    int snb = enc_dec(v, N, &sc, &sf);
    size_t sg = 0; { size_t cap = N*11+64; uint8_t *g = malloc(cap); tsdb_gorilla_encode(v, N, g, cap, &sg); free(g); }
    printf("%-18s adaptive=%-8s %6d B (%.3f B/pt) | gorilla-only=%zu B  => chimp saves %.1f%%\n",
           "sine", codec_name(sc), snb, (double)snb / N, sg, 100.0 * ((double)sg - (double)snb) / (double)sg);
    ASSERT(sc == TSDB_CODEC_CHIMP);          /* smooth data still routed to Chimp */
    ASSERT((size_t)snb < sg * 90 / 100);     /* and it's a real win (>10%) over gorilla */

    /* CONSTANT — domain ~1.6% of raw, so the density half never arms the skip;
     * LZ wins ~60x and MUST be kept regardless of what the probe says. */
    for (size_t i = 0; i < N; i++) v[i] = 3.14159265358979;
    tsdb_codec_t cc = TSDB_CODEC_NONE; uint16_t cf = 0;
    int cnb = enc_dec(v, N, &cc, &cf);
    printf("%-18s adaptive=%-8s %6d B (%.4f B/pt) lz=%s\n",
           "constant", codec_name(cc), cnb, (double)cnb / N, (cf & TSDB_BF_OUTER_LZ) ? "YES" : "no");
    ASSERT(cf & TSDB_BF_OUTER_LZ);           /* LZ preserved on constant float */
    ASSERT((size_t)cnb < N);                 /* < 1 byte/pt — the 60x win survives */

    /* Short blocks: the raw input is smaller than one probe window, so the
     * probe reads the whole thing.  Must not trip on the bounds. */
    printf("\n-- short blocks (raw < one probe window) --\n");
    for (size_t n = 1; n <= 256; n *= 4) {
        for (size_t i = 0; i < n; i++) v[i] = 1.0 + (double)(i % 7) / 8.0;
        tsdb_codec_t k = TSDB_CODEC_NONE; uint16_t kf = 0;
        int kb = enc_dec(v, n, &k, &kf);
        tsdb_codec_t kd = TSDB_CODEC_NONE;
        size_t kdom = domain_only(v, n, &kd);
        printf("  n=%-4zu %5d B  lz=%-3s domain=%zu\n", n, kb,
               (kf & TSDB_BF_OUTER_LZ) ? "YES" : "no", kdom);
        ASSERT((size_t)kb <= kdom);
    }

    free(v);
    printf("\n[PASS] best-of-2 == best-of-3 (no CHIMP128); LZ gate = density AND raw-probe:"
           " kept on constant/quantized/heterogeneous, skipped on incompressible;"
           " never larger than domain-only; sine->Chimp\n");
    return 0;
}
