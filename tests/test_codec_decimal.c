/* test_codec_decimal.c — scaled-decimal FLOAT64 codec (TSDB_CODEC_DEC, id 12).
 *
 * The codec re-expresses a float column as round(v * 10^s) int64 and hands it
 * to the integer codecs.  That is only legitimate if it is EXACTLY lossless,
 * and it is a FORMAT CHANGE, so this test pins both halves:
 *
 *  [1] SCALE SELECTION IS BIT-EXACT OR IT REFUSES.  Over an adversarial
 *      matrix — NaN, ±inf, ±0.0, subnormals, huge magnitudes, 0..9 decimal
 *      digits, mixed signs, the exact-representation boundary — the encoder
 *      accepts a scale only when every value reproduces its 8-byte pattern,
 *      and the pack/decode pair reproduces it for BOTH inner integer codecs.
 *
 *  [2] -0.0 IS THE TRAP.  round(-0.0 * 100) is -0.0, (int64)(-0.0) is 0, and
 *      0/100 decodes to +0.0.  `back == v` is TRUE for that pair, so a
 *      value-compare acceptance test silently drops the sign.  Asserted here
 *      both ways: the naive compare accepts, the shipped memcmp rejects.
 *
 *  [3] DIFFERENTIAL vs THE SHIPPED PATH.  Every shape is encoded twice in one
 *      process, with TSDB_CODEC_DECIMAL off and on, and both are decoded:
 *      the values must be bit-identical to the input and to each other.  On
 *      every shape the codec refuses, the two ENCODINGS must be byte-for-byte
 *      identical — proof the fallback is the old path, not a re-derivation.
 *
 *  [4] AN UNKNOWN CODEC ID FAILS LOUDLY.  Id 12 was unassigned, so a binary
 *      that predates this codec hits the default: arm of tsdb_codec_decode.
 *      This asserts what that arm does for every id this build does not know
 *      (including the reserved-but-unemitted 3/7/9): TSDB_ERR_UNSUPPORTED,
 *      through both the plain and the outer-LZ decode entry points, with the
 *      caller's buffer left untouched.  Malformed DEC payloads (scale byte
 *      out of range, inner codec this codec never emits, truncated) are
 *      rejected as TSDB_ERR_CORRUPT rather than decoded into garbage.
 *
 *  [5] END TO END THROUGH STORAGE.  A 2-decimal column written with the flag
 *      on lands codec-12 blocks on disk and reads back bit-exact through the
 *      real partition reader; the same data written with the flag off lands
 *      no codec-12 block and reads back to the identical values.
 */

#include "../include/tsdb.h"
#include "../src/compress/codec.h"
#include "../src/compress/dec.h"
#include "../src/storage/schema.h"
#include "../src/storage/part.h"
#include "../src/core/types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <float.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

static int g_pass = 0, g_fail = 0;

#define CHECK(c, ...) do {                                          \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
                fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); g_fail++; } \
    else      { g_pass++; }                                          \
} while (0)

#define CHECKP(c, ...) do {                                          \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);  \
                fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); g_fail++; } \
    else      { printf("PASS: "); printf(__VA_ARGS__); printf("\n"); g_pass++; } \
} while (0)

static int same_bits(double a, double b) {
    uint64_t x, y;
    memcpy(&x, &a, 8); memcpy(&y, &b, 8);
    return x == y;
}

static int bits_equal_n(const double *a, const double *b, size_t n) {
    return memcmp(a, b, n * sizeof(double)) == 0;
}

/* ------------------------------------------------------------------ shapes */

/* One full block: the selection decisions below (and the outer-LZ match
 * lengths they turn on) are block-size dependent, so the shapes are measured
 * at the size the flush path actually uses. */
#define NV TSDB_BLOCK_POINTS
static double g_v[NV];

static uint32_t g_r;
static double urand(void) { g_r = g_r * 1664525u + 1013904223u; return (double)(g_r >> 8) / (double)(1u << 24); }

/* v = round(uniform(0,span) * 10^dec) / 10^dec, i.e. exactly `dec` decimals. */
static void fill_decimals(int dec, double span, uint32_t seed) {
    g_r = seed;
    double p = pow(10.0, dec);
    for (size_t i = 0; i < NV; i++) {
        double q = floor(urand() * span * p + 0.5);
        g_v[i] = q / p;
    }
}

static void fill_walk_fullprec(uint32_t seed) {
    g_r = seed;
    double a = 50.0;
    for (size_t i = 0; i < NV; i++) { a += (urand() - 0.5) * 2.0; g_v[i] = a; }
}

/* ------------------------------------------- [1] adversarial scale matrix */

/* Encode `g_v` with an explicit inner codec, decode, compare bit patterns. */
static int pack_roundtrip(int scale, const int64_t *q, tsdb_codec_t inner,
                          const char *label)
{
    size_t cap = NV * 11 + 64;
    uint8_t *buf = malloc(cap);
    double  *out = malloc(NV * sizeof(double));
    int ok = 0;
    size_t nb = 0;
    if (buf && out &&
        tsdb_dec_pack(q, NV, scale, inner, buf, cap, &nb) == TSDB_OK) {
        memset(out, 0xA5, NV * sizeof(double));
        if (tsdb_dec_decode(buf, nb, out, NV) == TSDB_OK)
            ok = bits_equal_n(g_v, out, NV);
    }
    if (!ok) fprintf(stderr, "  (%s: inner=%d pack/decode not bit-exact)\n", label, (int)inner);
    free(buf); free(out);
    return ok;
}

/* expect_scale >= 0: must be accepted at exactly that scale.
 * expect_scale == -2: must be accepted, scale unpinned.
 * expect_scale == -1: must be refused. */
static void case_scale(const char *label, int expect_scale)
{
    int64_t *q = malloc(NV * sizeof(int64_t));
    int s = tsdb_dec_find_scale(g_v, NV, q);

    if (expect_scale == -1) {
        CHECKP(s == -1, "%-26s refused (no exact scale)", label);
    } else {
        CHECKP(s >= 0, "%-26s accepted at scale %d", label, s);
        if (expect_scale >= 0)
            CHECK(s == expect_scale, "%s: scale %d, wanted %d", label, s, expect_scale);
        if (s >= 0) {
            CHECK(pack_roundtrip(s, q, TSDB_CODEC_DOD,  label),
                  "%s: DoD inner round-trip", label);
            CHECK(pack_roundtrip(s, q, TSDB_CODEC_PFOR, label),
                  "%s: PFOR inner round-trip", label);
        }
    }
    free(q);
}

/* ------------------------------------------------ [3] differential harness */

typedef struct { int bytes; tsdb_codec_t codec; uint16_t flags; } enc_t;

static uint8_t g_buf_off[NV * 16 + 512];
static uint8_t g_buf_on [NV * 16 + 512];

static enc_t encode_mode(int dec_on, uint8_t *dst, size_t cap)
{
    enc_t e = { 0, TSDB_CODEC_NONE, 0 };
    if (dec_on) setenv("TSDB_CODEC_DECIMAL", "1", 1);
    else        unsetenv("TSDB_CODEC_DECIMAL");
    e.bytes = tsdb_codec_encode_adaptive(TSDB_TYPE_FLOAT64, g_v, NV,
                                         dst, cap, &e.codec, &e.flags);
    unsetenv("TSDB_CODEC_DECIMAL");
    return e;
}

static void decode_check(const enc_t *e, const uint8_t *src, const char *label,
                         const char *mode)
{
    double *out = malloc(NV * sizeof(double));
    memset(out, 0xA5, NV * sizeof(double));
    int rc = tsdb_codec_decode_adaptive(e->codec, TSDB_TYPE_FLOAT64, e->flags,
                                        src, (size_t)e->bytes, out, NV);
    CHECK(rc == TSDB_OK, "%s/%s: decode rc=%d", label, mode, rc);
    CHECK(rc == TSDB_OK && bits_equal_n(g_v, out, NV),
          "%s/%s: decoded values are NOT bit-identical to the input", label, mode);
    free(out);
}

/* want_dec: 1 = the codec must be chosen, 0 = the block must fall back.
 * max_growth_pct: when DEC is chosen, the ceiling on bytes_on vs bytes_off. */
static void case_diff(const char *label, int want_dec, int max_growth_pct)
{
    enc_t off = encode_mode(0, g_buf_off, sizeof g_buf_off);
    enc_t on  = encode_mode(1, g_buf_on,  sizeof g_buf_on);
    CHECK(off.bytes >= 0 && on.bytes >= 0, "%s: encode failed", label);
    if (off.bytes < 0 || on.bytes < 0) return;

    decode_check(&off, g_buf_off, label, "off");
    decode_check(&on,  g_buf_on,  label, "on");

    double ratio = off.bytes ? 100.0 * (double)on.bytes / (double)off.bytes : 100.0;
    printf("  %-26s off=%-8s %7d B   on=%-8s %7d B  (%.1f%% of shipped)\n",
           label, off.codec == TSDB_CODEC_DEC ? "DEC" : "float", off.bytes,
           on.codec == TSDB_CODEC_DEC ? "DEC" : "float", on.bytes, ratio);

    CHECK(off.codec != TSDB_CODEC_DEC,
          "%s: DEC leaked into the default (flag-off) path", label);

    if (want_dec) {
        CHECK(on.codec == TSDB_CODEC_DEC, "%s: expected DEC, got codec %d",
              label, (int)on.codec);
        CHECK((long)on.bytes * 100 <= (long)off.bytes * max_growth_pct,
              "%s: DEC %d B exceeds the %d%% ceiling over shipped %d B",
              label, on.bytes, max_growth_pct, off.bytes);
    } else {
        CHECK(on.codec != TSDB_CODEC_DEC,
              "%s: DEC accepted a block it cannot represent exactly", label);
        /* The fallback must be the shipped path itself, byte for byte. */
        CHECK(on.bytes == off.bytes && on.codec == off.codec &&
              on.flags == off.flags &&
              memcmp(g_buf_on, g_buf_off, (size_t)off.bytes) == 0,
              "%s: fallback encoding differs from the shipped encoding", label);
    }
}

/* --------------------------------------------------- [5] storage end to end */

static void rmrf(const char *dir) {
    char cmd[4096]; snprintf(cmd, sizeof cmd, "rm -rf %s", dir); (void)system(cmd);
}

#define EROWS 60000
static double erow(int64_t i) { return (double)((i * 7919) % 1000001) / 100.0; }

static void write_e2e(const char *dir, int dec_on) {
    if (dec_on) setenv("TSDB_CODEC_DECIMAL", "1", 1);
    else        unsetenv("TSDB_CODEC_DECIMAL");
    tsdb_db_t *db = NULL;
    if (tsdb_open(dir, &db) != TSDB_OK) { fprintf(stderr, "e2e: open failed\n"); exit(1); }
    tsdb_col_t cols[] = { { "ts", TSDB_TYPE_TIMESTAMP }, { "val", TSDB_TYPE_FLOAT64 } };
    int rc = tsdb_create_table(db, "t", cols, 2, "ts");
    if (rc != TSDB_OK && rc != TSDB_ERR_EXISTS) { fprintf(stderr, "e2e: create rc=%d\n", rc); exit(1); }
    tsdb_table_t *t = NULL;
    if (tsdb_open_table(db, "t", &t) != TSDB_OK) { fprintf(stderr, "e2e: open_table\n"); exit(1); }
    tsdb_batch_t *b = NULL;
    tsdb_batch_begin(t, &b);
    for (int64_t i = 0; i < EROWS; i++) {
        tsdb_batch_row_ts(b, 1000000000000LL + i * 1000000LL);
        tsdb_batch_row_f64(b, 1, erow(i));
        tsdb_batch_row_end(b);
    }
    tsdb_batch_commit(b);
    tsdb_close(db);
    unsetenv("TSDB_CODEC_DECIMAL");
}

static int find_part_dir(const char *table_dir, char *out, size_t cap) {
    DIR *d = opendir(table_dir);
    if (!d) return 0;
    struct dirent *e;
    int found = 0;
    while ((e = readdir(d))) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        char p[4096];
        snprintf(p, sizeof p, "%s/%s", table_dir, e->d_name);
        struct stat st;
        if (stat(p, &st) == 0 && S_ISDIR(st.st_mode)) {
            snprintf(out, cap, "%s", p); found = 1; break;
        }
    }
    closedir(d);
    return found;
}

/* Read every val block back; returns rows read, sets *dec_blocks and *exact. */
static size_t read_e2e(const char *dir, size_t *dec_blocks, int *exact, long *col_bytes)
{
    char table_dir[4096], part_dir[4096];
    snprintf(table_dir, sizeof table_dir, "%s/t", dir);
    if (!find_part_dir(table_dir, part_dir, sizeof part_dir)) return 0;

    tsdb_schema_t *s = NULL;
    if (tsdb_schema_open(table_dir, &s) != TSDB_OK) return 0;
    tsdb_part_t *p = NULL;
    if (tsdb_part_open(s, part_dir, &p) != TSDB_OK) return 0;

    tsdb_block_meta_t *m = NULL; size_t nb = 0;
    if (tsdb_part_col_blocks(p, 1, &m, &nb) != TSDB_OK) return 0;

    double *buf = malloc((size_t)TSDB_BLOCK_POINTS * 4 * sizeof(double));
    size_t rows = 0;
    *dec_blocks = 0; *exact = 1;
    for (size_t b = 0; b < nb; b++) {
        if (m[b].codec == TSDB_CODEC_DEC) (*dec_blocks)++;
        if (tsdb_part_read_block(p, 1, &m[b], buf) != TSDB_OK) { *exact = 0; break; }
        for (uint32_t k = 0; k < m[b].count; k++) {
            if (!same_bits(buf[k], erow((int64_t)(rows + k)))) { *exact = 0; break; }
        }
        rows += m[b].count;
    }
    free(buf);
    tsdb_part_close(p);
    tsdb_schema_free(s);

    char colf[4096];
    snprintf(colf, sizeof colf, "%s/val.col", part_dir);
    struct stat st;
    *col_bytes = (stat(colf, &st) == 0) ? (long)st.st_size : -1;
    return rows;
}

/* ------------------------------------------------------------------- main */

int main(void)
{
    printf("=== test_codec_decimal ===\n");
    unsetenv("TSDB_CODEC_DECIMAL");

    /* ---------------------------------------------------------- [1] scales */
    printf("\n[1] scale selection: accepted only when every value round-trips\n");

    for (size_t i = 0; i < NV; i++) g_v[i] = (double)(int64_t)(i % 1000);
    case_scale("integers", 0);

    fill_decimals(1, 100.0, 0x1111u);  case_scale("1 decimal", 1);
    fill_decimals(2, 100.0, 0x2222u);  case_scale("2 decimals", 2);
    fill_decimals(3, 100.0, 0x3333u);  case_scale("3 decimals", 3);
    fill_decimals(4, 100.0, 0x4444u);  case_scale("4 decimals", 4);
    fill_decimals(6, 10.0,  0x6666u);  case_scale("6 decimals", 6);
    fill_decimals(8, 1.0,   0x8888u);  case_scale("8 decimals (max scale)", -2);

    /* 9 decimals is one past TSDB_DEC_MAX_SCALE. */
    g_r = 0x9999u;
    for (size_t i = 0; i < NV; i++) {
        double q = floor(urand() * 1e9 + 0.5);
        if (fmod(q, 10.0) == 0.0) q += 1.0;   /* force the 9th digit non-zero */
        g_v[i] = q / 1e9;
    }
    case_scale("9 decimals", -1);

    /* Mixed signs and both zeros-that-are-legal. */
    fill_decimals(2, 100.0, 0xabcdu);
    for (size_t i = 0; i < NV; i += 3) g_v[i] = -g_v[i];
    for (size_t i = 0; i < NV; i += 97) g_v[i] = 0.0;
    case_scale("mixed sign + (+0.0)", -2);

    /* Exact-representation boundary: |v * 10^s| == 9e15 is the last accepted
     * magnitude; 10x past it must refuse at every scale. */
    for (size_t i = 0; i < NV; i++) g_v[i] = 9.0e15;
    case_scale("9e15 (boundary)", 0);
    for (size_t i = 0; i < NV; i++) g_v[i] = 9.1e16;
    case_scale("9.1e16 (past boundary)", -1);

    /* One poisoned value must sink the whole block — there is no lossy mode
     * and no per-value exception list. */
    struct { const char *name; double poison; } poisons[] = {
        { "NaN",              NAN },
        { "-NaN",             -NAN },
        { "+inf",             INFINITY },
        { "-inf",             -INFINITY },
        { "-0.0",             -0.0 },
        { "min subnormal",    5e-324 },
        { "subnormal",        DBL_MIN / 2.0 },
        { "DBL_MIN",          DBL_MIN },
        { "1e300",            1e300 },
        { "DBL_MAX",          DBL_MAX },
        { "1/3",              1.0 / 3.0 },
        { "pi",               3.14159265358979311600 },
    };
    for (size_t k = 0; k < sizeof poisons / sizeof poisons[0]; k++) {
        char label[64];
        fill_decimals(2, 100.0, 0x5150u);
        g_v[NV / 2] = poisons[k].poison;
        snprintf(label, sizeof label, "2dp + one %s", poisons[k].name);
        case_scale(label, -1);
    }

    fill_walk_fullprec(0x7777u);
    case_scale("full-precision walk", -1);

    /* ------------------------------------------------------- [2] -0.0 trap */
    printf("\n[2] the -0.0 trap: `back == v` cannot see it, memcmp can\n");
    {
        double v = -0.0;
        double x = v * 100.0;
        double r = round(x);
        int64_t q = (int64_t)r;
        double back = (double)q / 100.0;
        CHECKP(back == v,
               "a value-compare acceptance test ACCEPTS -0.0 (back == v is true)");
        CHECKP(!same_bits(back, v),
               "the bit-pattern test REJECTS it (decoded +0.0 != stored -0.0)");
        double one[1] = { -0.0 };
        int64_t qq[1];
        CHECKP(tsdb_dec_find_scale(one, 1, qq) == -1,
               "tsdb_dec_find_scale refuses a block whose only value is -0.0");
    }

    /* -------------------------------------------------- [3] differential */
    printf("\n[3] differential: same block encoded with the flag off and on\n");

    fill_decimals(2, 100.0, 0x2222u);      case_diff("sensor 2dp",        1, 100);
    fill_decimals(1, 100.0, 0x1111u);      case_diff("sensor 1dp",        1, 100);
    fill_decimals(3, 1000.0, 0x3333u);     case_diff("wide 3dp",          1, 100);
    fill_decimals(0, 100000.0, 0x0000u);   case_diff("integer valued",    1, 100);
    for (size_t i = 0; i < NV; i++) g_v[i] = 42.5;
    case_diff("constant 1dp", 1, 100);

    /* A 100-value cycle is the one measured shape where the shipped path wins:
     * CHIMP's output keeps the period in a form lzlite matches better than the
     * DoD varint stream does.  DEC is still 0.2 B/value here; the ceiling below
     * pins the gap so it cannot silently widen. */
    {
        double cyc[100];
        g_r = 0xc0ffee11u;
        for (int i = 0; i < 100; i++) {
            g_r = g_r * 1664525u + 1013904223u;
            cyc[i] = (double)(g_r % 10001u) / 100.0;
        }
        for (size_t i = 0; i < NV; i++) g_v[i] = cyc[i % 100];
    }
    case_diff("100-value 2dp cycle", 1, 130);

    fill_walk_fullprec(0x7777u);           case_diff("full-precision walk", 0, 0);
    for (size_t i = 0; i < NV; i++) g_v[i] = 3.14159265358979311600;
    case_diff("constant full-prec", 0, 0);
    fill_decimals(2, 100.0, 0x2222u); g_v[7] = NAN;
    case_diff("2dp + NaN",             0, 0);
    fill_decimals(2, 100.0, 0x2222u); g_v[NV - 1] = -0.0;
    case_diff("2dp + -0.0",            0, 0);
    fill_decimals(2, 100.0, 0x2222u); g_v[0] = INFINITY;
    case_diff("2dp + inf",             0, 0);
    fill_decimals(2, 100.0, 0x2222u); g_v[3] = 1e300;
    case_diff("2dp + 1e300",           0, 0);

    /* ------------------------------------------- [4] unknown-id rejection */
    printf("\n[4] unknown codec ids fail loudly (this is what an OLD binary\n"
           "    does when handed a codec-12 block)\n");
    {
        /* Build one real LZ-wrapped block so the adaptive entry point gets a
         * decodable wrapper and only the codec id is wrong. */
        for (size_t i = 0; i < NV; i++) g_v[i] = 42.5;
        enc_t lz = encode_mode(0, g_buf_off, sizeof g_buf_off);
        CHECK(lz.bytes > 0 && (lz.flags & TSDB_BF_OUTER_LZ),
              "setup: constant block is outer-LZ wrapped");

        double sentinel[64];
        int unknown_ok = 1, adaptive_ok = 1, untouched = 1;
        int known[256];
        memset(known, 0, sizeof known);
        known[TSDB_CODEC_NONE] = known[TSDB_CODEC_DOD] = known[TSDB_CODEC_GORILLA] =
        known[TSDB_CODEC_DICT] = known[TSDB_CODEC_RAW] = known[TSDB_CODEC_CHIMP] =
        known[TSDB_CODEC_CHIMP128] = known[TSDB_CODEC_PFOR] = known[TSDB_CODEC_F32] =
        known[TSDB_CODEC_DEC] = 1;

        for (int id = 0; id < 256; id++) {
            if (known[id]) continue;
            memset(sentinel, 0xA5, sizeof sentinel);
            if (tsdb_codec_decode((tsdb_codec_t)id, TSDB_TYPE_FLOAT64,
                                  g_buf_off, (size_t)lz.bytes,
                                  sentinel, 8) != TSDB_ERR_UNSUPPORTED)
                unknown_ok = 0;
            for (size_t k = 0; k < sizeof sentinel; k++)
                if (((const uint8_t *)sentinel)[k] != 0xA5) untouched = 0;
            if (tsdb_codec_decode_adaptive((tsdb_codec_t)id, TSDB_TYPE_FLOAT64,
                                           lz.flags, g_buf_off, (size_t)lz.bytes,
                                           sentinel, 8) != TSDB_ERR_UNSUPPORTED)
                adaptive_ok = 0;
        }
        CHECKP(unknown_ok,
               "every unknown id (incl. reserved 3/7/9) -> TSDB_ERR_UNSUPPORTED");
        CHECKP(adaptive_ok, "same through the outer-LZ decode entry point");
        CHECKP(untouched, "the caller's output buffer is left untouched");

        /* id 12 must NOT be in that set — that is the whole point of picking a
         * fresh id: an old binary refuses it, this one decodes it. */
        for (size_t i = 0; i < NV; i++) g_v[i] = 1.25;
        int64_t *q = malloc(NV * sizeof(int64_t));
        int sc = tsdb_dec_find_scale(g_v, NV, q);
        size_t cap = NV * 11 + 64, nb = 0;
        uint8_t *raw = malloc(cap);
        double *out = malloc(NV * sizeof(double));
        CHECK(sc >= 0 && tsdb_dec_pack(q, NV, sc, TSDB_CODEC_DOD, raw, cap, &nb) == TSDB_OK,
              "setup: packed a DEC block");
        CHECKP(tsdb_codec_decode(TSDB_CODEC_DEC, TSDB_TYPE_FLOAT64,
                                 raw, nb, out, NV) == TSDB_OK &&
               bits_equal_n(g_v, out, NV),
               "id 12 IS handled by this build (dispatch reaches tsdb_dec_decode)");

        /* Malformed DEC payloads must be refused, not decoded into garbage. */
        uint8_t bad[64];
        memcpy(bad, raw, nb < sizeof bad ? nb : sizeof bad);
        bad[0] = TSDB_DEC_MAX_SCALE + 1;
        CHECKP(tsdb_codec_decode(TSDB_CODEC_DEC, TSDB_TYPE_FLOAT64, bad,
                                 nb < sizeof bad ? nb : sizeof bad,
                                 out, 4) == TSDB_ERR_CORRUPT,
               "DEC rejects a scale byte past TSDB_DEC_MAX_SCALE");
        memcpy(bad, raw, nb < sizeof bad ? nb : sizeof bad);
        bad[1] = TSDB_CODEC_GORILLA;
        CHECKP(tsdb_codec_decode(TSDB_CODEC_DEC, TSDB_TYPE_FLOAT64, bad,
                                 nb < sizeof bad ? nb : sizeof bad,
                                 out, 4) == TSDB_ERR_CORRUPT,
               "DEC rejects an inner codec id it never emits");
        CHECKP(tsdb_codec_decode(TSDB_CODEC_DEC, TSDB_TYPE_FLOAT64, raw, 1,
                                 out, 4) == TSDB_ERR_CORRUPT,
               "DEC rejects a payload too short to hold its own header");
        free(q); free(raw); free(out);
    }

    /* ------------------------------------------------------ [5] end to end */
    printf("\n[5] end to end: 2-decimal column through the real writer/reader\n");
    {
        const char *don  = "/tmp/tsdb_test_dec_on";
        const char *doff = "/tmp/tsdb_test_dec_off";
        rmrf(don); rmrf(doff);
        write_e2e(don, 1);
        write_e2e(doff, 0);

        size_t on_dec = 0, off_dec = 0;
        int on_exact = 0, off_exact = 0;
        long on_bytes = 0, off_bytes = 0;
        size_t on_rows  = read_e2e(don,  &on_dec,  &on_exact,  &on_bytes);
        size_t off_rows = read_e2e(doff, &off_dec, &off_exact, &off_bytes);

        printf("  flag on : %zu rows, %zu codec-12 blocks, val.col = %ld B\n",
               on_rows, on_dec, on_bytes);
        printf("  flag off: %zu rows, %zu codec-12 blocks, val.col = %ld B\n",
               off_rows, off_dec, off_bytes);

        CHECKP(on_rows == EROWS && off_rows == EROWS, "both writers stored %d rows", EROWS);
        CHECKP(on_dec > 0, "the flag-on partition really carries codec-12 blocks");
        CHECKP(off_dec == 0, "the flag-off partition carries none");
        CHECKP(on_exact, "codec-12 blocks read back bit-exact through tsdb_part_read_block");
        CHECKP(off_exact, "the shipped path reads back bit-exact too");
        CHECKP(on_bytes > 0 && off_bytes > 0 && on_bytes < off_bytes,
               "val.col shrank %ld -> %ld B (%.1f%%)", off_bytes, on_bytes,
               100.0 * (double)on_bytes / (double)off_bytes);
        rmrf(don); rmrf(doff);
    }

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
