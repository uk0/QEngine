/* udf_sample.c — reference UDF shared library used by test_udf.c.
 *
 * Registers four functions:
 *
 *   udf_double(x: FLOAT64) -> FLOAT64       — returns 2*x
 *   udf_add  (a: INT64, b: INT64) -> INT64  — returns a + b
 *   udf_clamp(x: FLOAT64, lo: FLOAT64, hi: FLOAT64) -> FLOAT64
 *                                            — returns max(lo, min(hi, x))
 *   udf_fail42(a: INT64) -> INT64            — identity, but errors on 42
 *
 * Build:
 *   cc -fPIC -shared -I<repo>/include -o udf_sample.so tests/udf_sample.c
 */

#include "tsdb_udf.h"
#include <stddef.h>
#include <stdint.h>

TSDB_UDF_ABI_EXPORT

__attribute__((visibility("default")))
int udf_double(const tsdb_udf_ctx_t *ctx,
               const void *const   *in,
               size_t               n,
               void                *out)
{
    (void)ctx;
    const double *x = (const double *)in[0];
    double       *y = (double       *)out;
    for (size_t i = 0; i < n; i++) y[i] = x[i] * 2.0;
    return TSDB_UDF_OK;
}

__attribute__((visibility("default")))
int udf_add(const tsdb_udf_ctx_t *ctx,
            const void *const   *in,
            size_t               n,
            void                *out)
{
    (void)ctx;
    const int64_t *a = (const int64_t *)in[0];
    const int64_t *b = (const int64_t *)in[1];
    int64_t       *r = (int64_t       *)out;
    for (size_t i = 0; i < n; i++) r[i] = a[i] + b[i];
    return TSDB_UDF_OK;
}

/* Identity on INT64, but returns TSDB_UDF_ERR when it sees the magic
 * value 42 — used by test_udf.c to assert that a failing UDF aborts the
 * query with an error instead of silently emitting zeros. */
__attribute__((visibility("default")))
int udf_fail42(const tsdb_udf_ctx_t *ctx,
               const void *const   *in,
               size_t               n,
               void                *out)
{
    (void)ctx;
    const int64_t *a = (const int64_t *)in[0];
    int64_t       *r = (int64_t       *)out;
    for (size_t i = 0; i < n; i++) {
        if (a[i] == 42) return TSDB_UDF_ERR;
        r[i] = a[i];
    }
    return TSDB_UDF_OK;
}

__attribute__((visibility("default")))
int udf_clamp(const tsdb_udf_ctx_t *ctx,
              const void *const   *in,
              size_t               n,
              void                *out)
{
    (void)ctx;
    const double *x  = (const double *)in[0];
    const double *lo = (const double *)in[1];
    const double *hi = (const double *)in[2];
    double       *r  = (double       *)out;
    for (size_t i = 0; i < n; i++) {
        double v = x[i];
        if (v < lo[i]) v = lo[i];
        if (v > hi[i]) v = hi[i];
        r[i] = v;
    }
    return TSDB_UDF_OK;
}
