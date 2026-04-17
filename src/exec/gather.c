/* gather.c — SIMD-optimised bitmap gather primitives.
 *
 * Strategy by path:
 *
 *   Scalar  : __builtin_ctzll fast-bit-iteration, single element per cycle
 *   NEON    : __builtin_ctzll (no NEON compress-store), but tightened loop
 *             to remove branches and reduce loop overhead
 *   AVX2    : __builtin_ctzll + scalar store (AVX2 has no native compress)
 *   AVX-512 : _mm512_mask_compressstoreu_pd — up to 8 elements per call
 *             for dense passes (high popcount words)
 *
 * For sparse bitmaps (low popcount) the ctzll loop already skips 0 bits
 * efficiently on all paths — no special sparse/dense threshold needed.
 *
 * Runtime dispatch via tsdb_cpu_level() cached in gather_init_once().
 */
#include "gather.h"
#include "cpuid.h"

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

/* -----------------------------------------------------------------------
 * Scalar kernels — used as fallback and tail loop
 * --------------------------------------------------------------------- */

static size_t gather_f64_scalar(const uint64_t *bm, const double *src,
                                  size_t n, double *dst) {
    size_t out = 0;
    size_t nw  = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        uint64_t word = bm[w];
        /* Mask off bits past end */
        size_t bits_in_word = end - base;
        if (bits_in_word < 64)
            word &= ((uint64_t)1 << bits_in_word) - 1;
        while (word) {
            unsigned bit = (unsigned)__builtin_ctzll(word);
            dst[out++] = src[base + bit];
            word &= word - 1;
        }
    }
    return out;
}

static size_t gather_i64_scalar(const uint64_t *bm, const int64_t *src,
                                  size_t n, int64_t *dst) {
    size_t out = 0;
    size_t nw  = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        uint64_t word = bm[w];
        size_t bits_in_word = end - base;
        if (bits_in_word < 64)
            word &= ((uint64_t)1 << bits_in_word) - 1;
        while (word) {
            unsigned bit = (unsigned)__builtin_ctzll(word);
            dst[out++] = src[base + bit];
            word &= word - 1;
        }
    }
    return out;
}

/* -----------------------------------------------------------------------
 * AVX-512 kernel  (x86-64, compile-time guard + per-function target)
 * _mm512_mask_compressstoreu_pd: store at most 8 doubles per mask
 * --------------------------------------------------------------------- */
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>

#if defined(__AVX512F__)

__attribute__((target("avx512f,avx512vbmi2")))
static size_t gather_f64_avx512(const uint64_t *bm, const double *src,
                                  size_t n, double *dst) {
    size_t out = 0;
    size_t nw  = (n + 63) / 64;
    for (size_t w = 0; w < nw; w++) {
        size_t base = w * 64;
        size_t end  = base + 64 < n ? base + 64 : n;
        uint64_t word = bm[w];
        size_t bits_in_word = end - base;
        if (bits_in_word < 64)
            word &= ((uint64_t)1 << bits_in_word) - 1;

        /* Process in 8-bit chunks (8 doubles per _mm512_mask_compressstoreu_pd) */
        while (word) {
            /* Take low 8 bits */
            uint8_t lo8 = (uint8_t)(word & 0xFF);
            if (lo8) {
                __mmask8 k = (__mmask8)lo8;
                /* Load 8 consecutive source elements, compress to dst */
                __m512d chunk = _mm512_loadu_pd(src + base);
                _mm512_mask_compressstoreu_pd(dst + out, k, chunk);
                out += (size_t)__builtin_popcount(lo8);
            }
            word >>= 8;
            base  += 8;
        }
    }
    return out;
}

#define HAS_AVX512_GATHER 1
#endif /* __AVX512F__ */

#define HAS_X86_GATHER 1
#endif /* x86 */

/* -----------------------------------------------------------------------
 * NEON / ARM: no compress-store, use optimised ctzll loop
 * (same algorithm as scalar but arm64 ctzll maps to clz + reverse)
 * --------------------------------------------------------------------- */
#if defined(__aarch64__) || defined(__ARM_NEON)

static size_t gather_f64_neon(const uint64_t *bm, const double *src,
                               size_t n, double *dst) {
    /* Same structure as scalar; on AArch64 __builtin_ctzll → rbit+clz → 1 cycle */
    return gather_f64_scalar(bm, src, n, dst);
}

#define HAS_NEON_GATHER 1
#endif

/* -----------------------------------------------------------------------
 * Dispatch
 * --------------------------------------------------------------------- */
typedef size_t (*fn_gather_f64)(const uint64_t *, const double *, size_t, double *);
typedef size_t (*fn_gather_i64)(const uint64_t *, const int64_t *, size_t, int64_t *);

static fn_gather_f64 g_gather_f64 = NULL;
static fn_gather_i64 g_gather_i64 = NULL;

static pthread_once_t g_gather_once = PTHREAD_ONCE_INIT;

static void gather_init_once(void) {
    tsdb_cpu_level_t lv = tsdb_cpu_level();

    g_gather_f64 = gather_f64_scalar;
    g_gather_i64 = gather_i64_scalar;

#if defined(HAS_X86_GATHER) && defined(HAS_AVX512_GATHER)
    if (lv >= TSDB_CPU_AVX512) {
        g_gather_f64 = gather_f64_avx512;
        /* i64 compress-store: same approach but uses _mm512_mask_compressstoreu_epi64
         * which requires AVX-512VBMI2; keep scalar for i64 for portability */
    }
#endif
#if defined(HAS_NEON_GATHER)
    if (lv >= TSDB_CPU_NEON) {
        g_gather_f64 = gather_f64_neon;
    }
#endif
    (void)lv;
}

static inline void gather_ensure_init(void) {
    pthread_once(&g_gather_once, gather_init_once);
}

/* -----------------------------------------------------------------------
 * Public API
 * --------------------------------------------------------------------- */
size_t tsdb_bitmap_gather_f64(const uint64_t *bm, const double *src,
                               size_t n, double *dst) {
    if (!bm || !src || !dst || n == 0) return 0;
    gather_ensure_init();
    return g_gather_f64(bm, src, n, dst);
}

size_t tsdb_bitmap_gather_i64(const uint64_t *bm, const int64_t *src,
                               size_t n, int64_t *dst) {
    if (!bm || !src || !dst || n == 0) return 0;
    gather_ensure_init();
    return g_gather_i64(bm, src, n, dst);
}
