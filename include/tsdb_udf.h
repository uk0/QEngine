/* tsdb_udf.h — scalar user-defined function ABI v1.
 *
 * ─── CONTRACT ─────────────────────────────────────────────────────────────
 *
 * A UDF is a C-ABI function loaded from a shared object (.so / .dylib) at
 * runtime. The server calls it in vectorised batches. A single UDF looks
 * like this on disk:
 *
 *   #include <tsdb_udf.h>
 *
 *   TSDB_UDF_ABI_EXPORT
 *
 *   int my_double(const tsdb_udf_ctx_t *ctx,
 *                 const void *const *in,
 *                 size_t n,
 *                 void *out)
 *   {
 *       (void)ctx;
 *       const double *x = (const double *)in[0];
 *       double       *y = (double *)out;
 *       for (size_t i = 0; i < n; i++) y[i] = x[i] * 2.0;
 *       return TSDB_UDF_OK;
 *   }
 *
 * Build it:
 *   cc -fPIC -shared -I<tsdb-include> -o my_udf.so my_udf.c
 *
 * Register it:
 *   CREATE FUNCTION my_double(FLOAT64) RETURNS FLOAT64
 *        FROM '/opt/udf/my_udf.so' SYMBOL 'my_double';
 *
 * Use it:
 *   SELECT my_double(val) FROM trades;
 *
 * ─── VERSIONING ───────────────────────────────────────────────────────────
 *
 * Every .so MUST export the symbol `tsdb_udf_abi_version` (provided by the
 * TSDB_UDF_ABI_EXPORT macro below). The server refuses to load a library
 * whose reported version does not match `TSDB_UDF_ABI_V1` — this prevents
 * silent ABI drift across QEngine upgrades.
 *
 * ─── THREAD SAFETY ────────────────────────────────────────────────────────
 *
 * UDFs MUST be stateless and re-entrant. The parallel executor may call the
 * same symbol concurrently from multiple threads with different inputs.
 * Mutable file-scope state is not allowed unless the UDF itself guards it
 * with synchronisation primitives (strongly discouraged). Read-only globals
 * (e.g. precomputed tables) are fine.
 *
 * ─── TYPE SUPPORT ─────────────────────────────────────────────────────────
 *
 * MVP v1 covers: INT64, FLOAT64, TIMESTAMP (stored as int64).
 * Unsupported in v1: SYMBOL (dictionary-encoded strings). If SYMBOL comes
 * out in a future version, a `null_mask` argument will be appended —
 * existing v1 UDFs will refuse to load against that newer ABI, by design.
 *
 * NULL handling: v1 has none. Every input element is assumed non-null.
 *
 * ─── SECURITY ─────────────────────────────────────────────────────────────
 *
 * CREATE FUNCTION loads user-supplied native code into the server process.
 * The server enforces an ADMIN-role check at CREATE FUNCTION time — see
 * src/catalog/user.c. DO NOT disable this check. Document for operators:
 * "registering a UDF is equivalent to granting shell access".
 */
#ifndef TSDB_UDF_H
#define TSDB_UDF_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ─── ABI version ────────────────────────────────────────────────────────── */

#define TSDB_UDF_ABI_V1   1u

/* Return codes returned by UDF implementations. */
#define TSDB_UDF_OK       0
#define TSDB_UDF_ERR     (-1)

/* ─── Value types ────────────────────────────────────────────────────────── */

/* Must match values in include/tsdb.h — duplicated here so UDF authors do
 * not need to depend on the full public header. Keep these in sync. */
#ifndef TSDB_UDF_TYPE_DEFS
#  define TSDB_UDF_TYPE_DEFS
typedef enum {
    TSDB_UDF_TYPE_TIMESTAMP = 1,
    TSDB_UDF_TYPE_INT64     = 2,
    TSDB_UDF_TYPE_FLOAT64   = 3,
} tsdb_udf_type_t;
#endif

/* Maximum number of positional arguments a v1 UDF can declare. */
#define TSDB_UDF_MAX_ARGS   8

/* ─── Call context ───────────────────────────────────────────────────────── */

/* Passed by reference to every invocation. Reserved for future per-call
 * parameters (query-level hints, cancellation token, etc.). */
typedef struct {
    uint32_t abi_version;     /* always TSDB_UDF_ABI_V1 */
    uint32_t reserved;        /* must be zero in v1 */
} tsdb_udf_ctx_t;

/*
 * UDF callable signature.
 *
 *   ctx    — call metadata (caller-owned, do not retain past return)
 *   in     — array of nargs pointers; in[i] points to n values of the
 *             declared argument type (8 bytes per element for v1 types)
 *   n      — element count in each input column
 *   out    — pre-allocated output buffer of n * sizeof(ret_type) bytes
 *
 * Return TSDB_UDF_OK on success, TSDB_UDF_ERR on failure. On failure the
 * server discards the result for this batch and surfaces
 * TSDB_ERR_INTERNAL to the caller.
 */
typedef int (*tsdb_udf_fn_t)(const tsdb_udf_ctx_t *ctx,
                              const void *const   *in,
                              size_t               n,
                              void                *out);

/* ─── Export macro ───────────────────────────────────────────────────────── */

/*
 * Put TSDB_UDF_ABI_EXPORT at the top of any UDF translation unit. It
 * defines the version symbol the loader checks. Without this the .so
 * will be rejected with TSDB_ERR_UNSUPPORTED.
 */
#define TSDB_UDF_ABI_EXPORT                                     \
    __attribute__((visibility("default")))                      \
    uint32_t tsdb_udf_abi_version(void) { return TSDB_UDF_ABI_V1; }

#ifdef __cplusplus
}
#endif

#endif /* TSDB_UDF_H */
