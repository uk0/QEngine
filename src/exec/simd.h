/* simd.h — SIMD feature detection and portability macros.
 *
 * Detects the available SIMD ISA at compile time and exposes:
 *   TSDB_SIMD_AVX2   — x86-64 AVX2 (256-bit), 4 double lanes / register
 *   TSDB_SIMD_NEON   — ARM NEON / AArch64 SIMD, 2 double lanes / register
 *   TSDB_SIMD_SCALAR — no SIMD, scalar fallback
 *   TSDB_SIMD_WIDTH  — number of double lanes per SIMD register
 *
 * Only one of the three is defined.  Consumers:
 *   #if TSDB_SIMD_AVX2
 *     ... _mm256_* intrinsics ...
 *   #elif TSDB_SIMD_NEON
 *     ... vld1q_f64 intrinsics ...
 *   #else
 *     ... scalar loop ...
 *   #endif
 */
#ifndef TSDB_EXEC_SIMD_H
#define TSDB_EXEC_SIMD_H

#if defined(__AVX2__)
#  define TSDB_SIMD_AVX2  1
#  define TSDB_SIMD_WIDTH 4   /* double lanes per 256-bit register */
#  include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__aarch64__)
#  define TSDB_SIMD_NEON  1
#  define TSDB_SIMD_WIDTH 2   /* double lanes per 128-bit register */
#  include <arm_neon.h>
#else
#  define TSDB_SIMD_SCALAR 1
#  define TSDB_SIMD_WIDTH  1
#endif

/* Runtime path name for diagnostics. */
static inline const char *tsdb_simd_name(void) {
#if defined(TSDB_SIMD_AVX2)
    return "AVX2";
#elif defined(TSDB_SIMD_NEON)
    return "NEON";
#else
    return "scalar";
#endif
}

/* Null-bitmap helpers (MSB-first byte array, bit 1 = valid, bit 0 = null).
 * Element i: byte index = i/8, bit index within byte = 7 - (i%8).
 */
#define TSDB_NULL_VALID(bitmap, i) \
    (((bitmap)[(i) >> 3] >> (7 - ((i) & 7))) & 1)

/* Filter-bitmap helpers (LSB-first uint64 array, bit 1 = row passes).
 * Element i: word index = i/64, bit position = i%64.
 */
#define TSDB_BM_SET(bm, i)   ((bm)[(i) >> 6] |=  ((uint64_t)1 << ((i) & 63)))
#define TSDB_BM_CLR(bm, i)   ((bm)[(i) >> 6] &= ~((uint64_t)1 << ((i) & 63)))
#define TSDB_BM_GET(bm, i)   (((bm)[(i) >> 6] >> ((i) & 63)) & 1)

#endif /* TSDB_EXEC_SIMD_H */
