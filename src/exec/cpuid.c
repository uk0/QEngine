/* cpuid.c — Runtime CPU feature detection implementation.
 *
 * x86-64: uses __builtin_cpu_supports() for AVX2 / AVX-512F.
 * ARM64 : NEON is architecturally guaranteed on AArch64; no runtime check.
 * Scalar: always available as fallback.
 *
 * The result is cached via pthread_once so detection pays at most once.
 * TSDB_CPU_LEVEL env-var allows forcing a lower level for comparison runs.
 */
#include "cpuid.h"

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* -----------------------------------------------------------------------
 * State
 * --------------------------------------------------------------------- */
static pthread_once_t  g_once  = PTHREAD_ONCE_INIT;
static tsdb_cpu_level_t g_level = TSDB_CPU_SCALAR;

/* -----------------------------------------------------------------------
 * Internal: hardware detection
 * --------------------------------------------------------------------- */
static tsdb_cpu_level_t detect_hw(void) {
#if defined(__x86_64__) || defined(__i386__)
    /* __builtin_cpu_supports requires __builtin_cpu_init on some toolchains;
     * GCC/Clang initialise it automatically in main, but call it explicitly
     * to be safe when called from library init paths. */
    __builtin_cpu_init();

#  if defined(__AVX512F__)
    /* If we were compiled with AVX-512F we can check at runtime too. */
    if (__builtin_cpu_supports("avx512f")) return TSDB_CPU_AVX512;
#  endif

    if (__builtin_cpu_supports("avx2"))    return TSDB_CPU_AVX2;
    return TSDB_CPU_SCALAR;

#elif defined(__aarch64__) || defined(__ARM_NEON)
    /* AArch64 mandates NEON; no OS query needed. Apple Silicon always has it. */
    return TSDB_CPU_NEON;

#else
    return TSDB_CPU_SCALAR;
#endif
}

/* -----------------------------------------------------------------------
 * Internal: parse env-var override
 * --------------------------------------------------------------------- */
static tsdb_cpu_level_t apply_env_override(tsdb_cpu_level_t hw) {
    const char *env = getenv("TSDB_CPU_LEVEL");
    if (!env) return hw;

    tsdb_cpu_level_t req;
    if      (strcmp(env, "AVX512") == 0) req = TSDB_CPU_AVX512;
    else if (strcmp(env, "AVX2")   == 0) req = TSDB_CPU_AVX2;
    else if (strcmp(env, "NEON")   == 0) req = TSDB_CPU_NEON;
    else if (strcmp(env, "SCALAR") == 0) req = TSDB_CPU_SCALAR;
    else return hw;   /* unknown string: ignore */

    /* Clamp: cannot request a level the hardware (or compile) doesn't support */
    if (req > hw) req = hw;
    return req;
}

/* -----------------------------------------------------------------------
 * once-initialiser
 * --------------------------------------------------------------------- */
static void init_once(void) {
    tsdb_cpu_level_t hw = detect_hw();
    g_level = apply_env_override(hw);
}

/* -----------------------------------------------------------------------
 * Public API
 * --------------------------------------------------------------------- */
tsdb_cpu_level_t tsdb_cpu_level(void) {
    pthread_once(&g_once, init_once);
    return g_level;
}

const char *tsdb_cpu_level_name(tsdb_cpu_level_t level) {
    switch (level) {
    case TSDB_CPU_AVX512: return "AVX512";
    case TSDB_CPU_AVX2:   return "AVX2";
    case TSDB_CPU_NEON:   return "NEON";
    default:              return "SCALAR";
    }
}
