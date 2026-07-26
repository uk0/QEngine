/* test_bits_refill.c — differential test for the 64-bit bulk refill in
 * src/core/bits.h.
 *
 * The bulk path in tsdb_br_refill banks up to 8 bytes ahead of where the old
 * byte-at-a-time loop would be, so the two readers do NOT hold the same
 * (bytes, nbits, scratch).  What must be identical is everything a consumer can
 * observe: the value returned by every tsdb_br_get, and the underflow flag.
 * This file keeps a verbatim copy of the pre-bulk reader (ref_*) and drives
 * both in lockstep.
 *
 *  1. Differential, exhaustive small caps: every cap 0..40, every base-pointer
 *     alignment 0..7, every fixed read width 1..64 — the widths above 56 are
 *     where the reader truncates a byte against a full scratch, and the small
 *     caps are the tail where a 64-bit load would run off the end.
 *  2. Differential, random width schedules (incl. n == 0 and n > 64) over caps
 *     0..80 plus a few large buffers.
 *  3. Differential, deliberate over-read past end-of-buffer: same values, same
 *     underflow, for every cap 0..24.
 *  4. Writer→reader round-trip: random (value, width) streams come back exact.
 *  5. End-to-end codec round-trip through the real decoders (CHIMP / GORILLA /
 *     DOD / CHIMP128 all sit on this reader).
 */

#include "../src/core/bits.h"
#include "../src/compress/codec.h"
#include "../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

/* ── reference reader: src/core/bits.h @ f993722, byte loop only ───────────*/

typedef tsdb_br_t ref_br_t;

static void ref_br_init(ref_br_t *r, const uint8_t *buf, size_t cap) {
    r->buf = buf; r->cap = cap; r->bytes = 0; r->scratch = 0; r->nbits = 0; r->underflow = 0;
}

static void ref_br_refill(ref_br_t *r, unsigned n) {
    while (r->nbits < n) {
        if (r->bytes >= r->cap) {
            if (r->nbits + 8 < n && r->bytes >= r->cap) r->underflow = 1;
            r->nbits += 8;
            if (r->nbits >= 64) { r->nbits = 64; return; }
            continue;
        }
        r->scratch |= (uint64_t)r->buf[r->bytes] << r->nbits;
        r->bytes++;
        r->nbits += 8;
    }
}

static uint64_t ref_br_get(ref_br_t *r, unsigned n) {
    if (n == 0) return 0;
    if (n > 64) n = 64;
    ref_br_refill(r, n);
    uint64_t v = r->scratch & (n == 64 ? ~(uint64_t)0 : (((uint64_t)1 << n) - 1));
    if (n == 64) r->scratch = 0;
    else r->scratch >>= n;
    r->nbits -= n;
    return v;
}

/* ── helpers ──────────────────────────────────────────────────────────────*/

static uint64_t g_st = 0x243f6a8885a308d3ULL;
static uint64_t rnd(void) {
    g_st ^= g_st << 13; g_st ^= g_st >> 7; g_st ^= g_st << 17;
    return g_st;
}

/* Run both readers over the same bytes with the same width schedule.
 * Returns 0 on full agreement, else the 1-based step that first diverged. */
static int diff_run(const uint8_t *buf, size_t cap,
                    const unsigned *widths, int nsteps,
                    uint64_t *bad_new, uint64_t *bad_ref, unsigned *bad_w)
{
    tsdb_br_t nw; ref_br_t rf;
    tsdb_br_init(&nw, buf, cap);
    ref_br_init(&rf, buf, cap);
    for (int i = 0; i < nsteps; i++) {
        uint64_t a = tsdb_br_get(&nw, widths[i]);
        uint64_t b = ref_br_get(&rf, widths[i]);
        if (a != b || (nw.underflow != 0) != (rf.underflow != 0)) {
            if (bad_new) *bad_new = a;
            if (bad_ref) *bad_ref = b;
            if (bad_w)   *bad_w   = widths[i];
            return i + 1;
        }
    }
    return 0;
}

int main(void) {
    printf("=== test_bits_refill ===\n");

    /* Backing store: 8 slack bytes in front so buf+align stays in bounds, and
     * plenty behind so an out-of-bounds 8-byte load would still be readable —
     * i.e. a stray load is caught by the value comparison, not by a crash. */
    enum { RAW = 4096 };
    static uint8_t raw[RAW];
    for (int i = 0; i < RAW; i++) raw[i] = (uint8_t)(rnd() >> 19);

    /* 1. Exhaustive: cap 0..40 × alignment 0..7 × fixed width 1..64. */
    {
        int bad = 0; long cases = 0;
        for (size_t cap = 0; cap <= 40 && !bad; cap++) {
            for (unsigned align = 0; align < 8 && !bad; align++) {
                const uint8_t *buf = raw + 8 + align;
                for (unsigned w = 1; w <= 64 && !bad; w++) {
                    unsigned widths[512];
                    /* Drain the whole buffer, then over-read by 4 reads. */
                    int steps = (int)((cap * 8) / w) + 4;
                    if (steps > 512) steps = 512;
                    for (int i = 0; i < steps; i++) widths[i] = w;
                    uint64_t an = 0, ar = 0; unsigned bw = 0;
                    int step = diff_run(buf, cap, widths, steps, &an, &ar, &bw);
                    cases++;
                    if (step) {
                        fprintf(stderr,
                                "  divergence: cap=%zu align=%u width=%u step=%d "
                                "new=0x%016llx ref=0x%016llx\n",
                                cap, align, bw, step,
                                (unsigned long long)an, (unsigned long long)ar);
                        bad = 1;
                    }
                }
            }
        }
        printf("  exhaustive fixed-width cases: %ld\n", cases);
        CHECK(!bad, "bulk refill == byte refill for cap 0..40 x align 0..7 x width 1..64");
    }

    /* 2. Random width schedules, including n == 0 and n > 64. */
    {
        int bad = 0; long cases = 0;
        static const size_t caps[] = { 0,1,2,3,4,5,6,7,8,9,15,16,17,23,24,25,
                                       31,32,33,47,48,49,63,64,65,79,80,
                                       127,128,129,255,256,257,1023,1024,2048 };
        for (size_t ci = 0; ci < sizeof(caps)/sizeof(caps[0]) && !bad; ci++) {
            size_t cap = caps[ci];
            for (unsigned align = 0; align < 8 && !bad; align++) {
                for (int trial = 0; trial < 24 && !bad; trial++) {
                    unsigned widths[512];
                    int steps = 0;
                    size_t bits = 0;
                    while (steps < 512 && bits < cap * 8 + 96) {
                        unsigned w;
                        switch (rnd() % 8) {
                        case 0:  w = 0;                              break; /* no-op */
                        case 1:  w = 65 + (unsigned)(rnd() % 6);     break; /* clamped */
                        case 2:  w = 57 + (unsigned)(rnd() % 8);     break; /* 57..64 */
                        case 3:  w = 32;                             break;
                        default: w = 1 + (unsigned)(rnd() % 40);     break;
                        }
                        widths[steps++] = w;
                        bits += (w > 64 ? 64 : w);
                    }
                    uint64_t an = 0, ar = 0; unsigned bw = 0;
                    int step = diff_run(raw + 8 + align, cap, widths, steps, &an, &ar, &bw);
                    cases++;
                    if (step) {
                        fprintf(stderr,
                                "  divergence: cap=%zu align=%u width=%u step=%d "
                                "new=0x%016llx ref=0x%016llx\n",
                                cap, align, bw, step,
                                (unsigned long long)an, (unsigned long long)ar);
                        bad = 1;
                    }
                }
            }
        }
        printf("  random-schedule cases: %ld\n", cases);
        CHECK(!bad, "bulk refill == byte refill for random width schedules");
    }

    /* 3. Deliberate over-read past end-of-buffer: values AND underflow agree. */
    {
        int bad = 0, saw_underflow = 0;
        for (size_t cap = 0; cap <= 24 && !bad; cap++) {
            for (unsigned align = 0; align < 8 && !bad; align++) {
                for (unsigned w = 1; w <= 64 && !bad; w++) {
                    const uint8_t *buf = raw + 8 + align;
                    tsdb_br_t nw; ref_br_t rf;
                    tsdb_br_init(&nw, buf, cap);
                    ref_br_init(&rf, buf, cap);
                    for (int i = 0; i < 40; i++) {
                        uint64_t a = tsdb_br_get(&nw, w);
                        uint64_t b = ref_br_get(&rf, w);
                        if (a != b || nw.underflow != rf.underflow) { bad = 1; break; }
                    }
                    if (rf.underflow) saw_underflow = 1;
                }
            }
        }
        CHECK(!bad, "over-read past end: identical values and identical underflow flag");
        CHECK(saw_underflow, "the over-read cases really do trip underflow");
    }

    /* 4. Writer -> reader round-trip. */
    {
        int bad = 0;
        for (int trial = 0; trial < 200 && !bad; trial++) {
            enum { NV = 400 };
            uint64_t val[NV]; unsigned wid[NV];
            uint8_t  out[NV * 9];
            tsdb_bw_t w;
            tsdb_bw_init(&w, out, sizeof out);
            for (int i = 0; i < NV; i++) {
                unsigned n = 1 + (unsigned)(rnd() % 56);   /* writer is safe <= 56 */
                uint64_t v = rnd() & (((uint64_t)1 << n) - 1);
                val[i] = v; wid[i] = n;
                tsdb_bw_put(&w, v, n);
            }
            ssize_t nb = tsdb_bw_finish(&w);
            if (nb <= 0) { bad = 1; break; }
            tsdb_br_t r;
            tsdb_br_init(&r, out, (size_t)nb);
            for (int i = 0; i < NV; i++) {
                if (tsdb_br_get(&r, wid[i]) != val[i]) { bad = 1; break; }
            }
            if (r.underflow) bad = 1;
        }
        CHECK(!bad, "bw_put -> br_get round-trip exact over 200 random streams");
    }

    /* 5. End-to-end: the real codecs decode bit-identically. */
    {
        enum { N = 8192 };
        double  *fin  = malloc(sizeof(double) * N);
        double  *fout = malloc(sizeof(double) * N);
        int64_t *tin  = malloc(sizeof(int64_t) * N);
        int64_t *tout = malloc(sizeof(int64_t) * N);
        uint8_t *enc  = malloc((size_t)N * 12 + 128);
        int ok = fin && fout && tin && tout && enc;
        int bad = 0;
        for (int shape = 0; shape < 4 && ok && !bad; shape++) {
            double acc = 50.0;
            for (int i = 0; i < N; i++) {
                switch (shape) {
                case 0: fin[i] = 100.0 + (double)(rnd() % 5000) / 100.0; break;
                case 1: acc += (double)((int64_t)(rnd() % 200) - 100) / 100.0;
                        fin[i] = acc; break;
                case 2: fin[i] = (double)i * 0.25; break;
                default: { uint64_t bv = rnd();
                           bv &= ~(0x7FFULL << 52);
                           bv |= ((uint64_t)(512 + (rnd() % 512)) << 52);
                           memcpy(&fin[i], &bv, 8); } break;
                }
                tin[i] = (int64_t)(1700000000000000000LL
                                   + (int64_t)i * 1000000LL
                                   + (int64_t)(rnd() % (shape == 3 ? 999983 : 3)));
            }
            tsdb_codec_t c; uint16_t fl;
            int nb = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, fin, N, enc,
                                                (size_t)N * 12 + 128, &c, &fl);
            if (nb < 0 || tsdb_codec_decode_adaptive(c, TSDB_TYPE_FLOAT64, fl, enc,
                                                     (size_t)nb, fout, N) < 0) { bad = 1; break; }
            if (memcmp(fin, fout, sizeof(double) * N) != 0) { bad = 1; break; }

            nb = tsdb_codec_encode_adaptive(TSDB_TYPE_TIMESTAMP, tin, N, enc,
                                            (size_t)N * 12 + 128, &c, &fl);
            if (nb < 0 || tsdb_codec_decode_adaptive(c, TSDB_TYPE_TIMESTAMP, fl, enc,
                                                     (size_t)nb, tout, N) < 0) { bad = 1; break; }
            if (memcmp(tin, tout, sizeof(int64_t) * N) != 0) { bad = 1; break; }
        }
        CHECK(ok && !bad, "adaptive codec round-trip bit-exact over 4 float + ts shapes");
        free(fin); free(fout); free(tin); free(tout); free(enc);
    }

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
