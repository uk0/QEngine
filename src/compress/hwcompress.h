/* hwcompress.h — hardware-compression offload dispatch with software
 * fallback.
 *
 * The general-purpose outer-compression stage (today: software lzlite)
 * can optionally be offloaded to a hardware accelerator when one is
 * present.  Backends are bound at runtime via dlopen so the engine has
 * NO link-time dependency on any accelerator library — on a node without
 * the hardware (or its library) the probe simply finds nothing and every
 * call reports TSDB_HW_UNAVAIL, which the caller treats as "use software".
 *
 * Supported backend probes (best-effort, optional):
 *   - Intel QAT  via libqatzip  (qzCompress / qzDecompress)
 *   - Intel ISA-L via libisal   (isal igzip)
 *
 * Selection / override:
 *   TSDB_HWCOMPRESS = off | auto | qat | isal   (default: auto)
 *     off  → never probe; always software.
 *     auto → probe qat then isal; first that binds wins.
 *     qat/isal → require that backend; fall back to software if absent.
 *
 * Thread-safety: the backend is resolved once (pthread_once) and then
 * read-only.  Encode/decode entry points are reentrant.
 *
 * IMPORTANT: this is the offload *dispatch layer*.  Wiring it into the
 * block codec's outer stage (with a TSDB_BF_OUTER_HW wire flag so the
 * decoder knows hardware vs lzlite produced the bytes) is a separate,
 * format-versioned change; this module deliberately ships first so the
 * fallback contract is in place and testable before any format change.
 */
#ifndef TSDB_COMPRESS_HWCOMPRESS_H
#define TSDB_COMPRESS_HWCOMPRESS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Return codes for the encode/decode entry points. */
enum {
    TSDB_HW_OK       =  0,   /* hardware produced output                     */
    TSDB_HW_UNAVAIL  = -1,   /* no hardware backend — caller uses software   */
    TSDB_HW_ERROR    = -2,   /* a bound backend failed — caller uses software*/
    TSDB_HW_NOSPACE  = -3,   /* out_cap too small                            */
};

typedef enum {
    TSDB_HW_BACKEND_NONE = 0,
    TSDB_HW_BACKEND_QAT  = 1,
    TSDB_HW_BACKEND_ISAL = 2,
} tsdb_hw_backend_t;

/* Resolve the active backend (idempotent).  Safe to call from anywhere;
 * the first call performs the dlopen probe under pthread_once. */
tsdb_hw_backend_t tsdb_hwcompress_backend(void);

/* 1 if a hardware backend is bound, 0 if everything will use software. */
int tsdb_hwcompress_available(void);

/* Human-readable backend name: "none" / "qat" / "isal" — for logging
 * and the test suite. */
const char *tsdb_hwcompress_name(void);

/* Compress `in_len` bytes into `out` (capacity `out_cap`).  On success
 * writes the compressed length to *out_len and returns TSDB_HW_OK.
 * Returns TSDB_HW_UNAVAIL when no backend is bound (caller must use the
 * software path), TSDB_HW_NOSPACE / TSDB_HW_ERROR otherwise. */
int tsdb_hwcompress_encode(const uint8_t *in, size_t in_len,
                           uint8_t *out, size_t out_cap, size_t *out_len);

/* Inverse of encode.  `orig_len` is the known decompressed size. */
int tsdb_hwcompress_decode(const uint8_t *in, size_t in_len,
                           uint8_t *out, size_t orig_len, size_t *out_len);

/* Test-only: force a backend state, bypassing the dlopen probe, so the
 * fallback contract can be exercised deterministically without hardware.
 * Passing TSDB_HW_BACKEND_NONE restores "no hardware".  NOT for production
 * use — there is no real codec behind a forced QAT/ISAL on a host that
 * lacks the library, so encode will report TSDB_HW_ERROR. */
void tsdb_hwcompress_force_backend_for_test(tsdb_hw_backend_t b);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_COMPRESS_HWCOMPRESS_H */
