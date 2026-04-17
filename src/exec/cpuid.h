/* cpuid.h — Runtime CPU feature detection with function-pointer dispatch.
 *
 * Provides a once-initialized tsdb_cpu_level_t that selects the best
 * available SIMD ISA at runtime, separate from compile-time macros.
 *
 * Priority: AVX-512F > AVX2 > NEON > Scalar
 *
 * Environment variable override (for benchmarking / testing):
 *   TSDB_CPU_LEVEL=SCALAR|NEON|AVX2|AVX512
 *   Clamped to what is actually compiled in; cannot force a level that
 *   was not compiled.
 */
#ifndef TSDB_EXEC_CPUID_H
#define TSDB_EXEC_CPUID_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    TSDB_CPU_SCALAR = 0,
    TSDB_CPU_NEON   = 1,
    TSDB_CPU_AVX2   = 2,
    TSDB_CPU_AVX512 = 3,
} tsdb_cpu_level_t;

/* Returns the best available CPU level for this process.
 * Thread-safe: detection is done once via pthread_once / call_once. */
tsdb_cpu_level_t tsdb_cpu_level(void);

/* Human-readable name for diagnostics. */
const char *tsdb_cpu_level_name(tsdb_cpu_level_t level);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_EXEC_CPUID_H */
