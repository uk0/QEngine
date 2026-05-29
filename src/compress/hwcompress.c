/* hwcompress.c — runtime-bound hardware-compression offload + software
 * fallback.  See header for the contract. */

#include "hwcompress.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>

#if defined(__unix__) || defined(__APPLE__)
#include <dlfcn.h>
#define HW_HAVE_DLOPEN 1
#endif

/* ---- backend vtable ----------------------------------------------------- */

typedef struct {
    tsdb_hw_backend_t kind;
    const char       *name;
    /* Returns 0 on success and sets *out_len; negative on failure. */
    int (*encode)(const uint8_t *in, size_t in_len,
                  uint8_t *out, size_t out_cap, size_t *out_len);
    int (*decode)(const uint8_t *in, size_t in_len,
                  uint8_t *out, size_t orig_len, size_t *out_len);
    void *dl;   /* dlopen handle (kept open for process lifetime) */
} hw_backend_t;

static hw_backend_t g_backend = { TSDB_HW_BACKEND_NONE, "none", NULL, NULL, NULL };
static pthread_once_t g_once = PTHREAD_ONCE_INIT;
static int g_forced = 0;   /* set by the test hook to skip the probe */

/* ---- Intel QAT (libqatzip) binding -------------------------------------- */
/* qatzip exposes a stateful session API.  We bind the simple one-shot
 * helpers qzCompress/qzDecompress when available; full session management
 * is left to a follow-up since lvm1 has no QAT to validate against. */
#if HW_HAVE_DLOPEN
typedef int (*qz_compress_fn)(void *sess, const unsigned char *src,
                              unsigned int *src_len, unsigned char *dst,
                              unsigned int *dst_len, unsigned int last);

static qz_compress_fn g_qz_compress;
static qz_compress_fn g_qz_decompress;

static void *try_dlopen(const char *const *names) {
    for (int i = 0; names[i]; i++) {
        void *h = dlopen(names[i], RTLD_NOW | RTLD_LOCAL);
        if (h) return h;
    }
    return NULL;
}

/* QAT one-shot wrappers.  Session is NULL (qatzip permits an implicit
 * default session for the simple API on supported builds).  If the bound
 * library rejects a NULL session it returns non-zero → we surface
 * TSDB_HW_ERROR and the caller falls back to software, so correctness is
 * never at risk. */
static int qat_encode(const uint8_t *in, size_t in_len,
                      uint8_t *out, size_t out_cap, size_t *out_len) {
    if (!g_qz_compress) return TSDB_HW_ERROR;
    unsigned int sl = (unsigned int)in_len, dl = (unsigned int)out_cap;
    int rc = g_qz_compress(NULL, in, &sl, out, &dl, 1);
    if (rc != 0) return TSDB_HW_ERROR;
    *out_len = dl;
    return TSDB_HW_OK;
}
static int qat_decode(const uint8_t *in, size_t in_len,
                      uint8_t *out, size_t orig_len, size_t *out_len) {
    if (!g_qz_decompress) return TSDB_HW_ERROR;
    unsigned int sl = (unsigned int)in_len, dl = (unsigned int)orig_len;
    int rc = g_qz_decompress(NULL, in, &sl, out, &dl, 1);
    if (rc != 0) return TSDB_HW_ERROR;
    *out_len = dl;
    return TSDB_HW_OK;
}

static int bind_qat(void) {
    static const char *libs[] = { "libqatzip.so.3", "libqatzip.so", NULL };
    void *h = try_dlopen(libs);
    if (!h) return 0;
    g_qz_compress   = (qz_compress_fn)dlsym(h, "qzCompress");
    g_qz_decompress = (qz_compress_fn)dlsym(h, "qzDecompress");
    if (!g_qz_compress || !g_qz_decompress) { dlclose(h); return 0; }
    g_backend.kind = TSDB_HW_BACKEND_QAT;
    g_backend.name = "qat";
    g_backend.encode = qat_encode;
    g_backend.decode = qat_decode;
    g_backend.dl = h;
    return 1;
}

/* ---- Intel ISA-L (libisal) binding -------------------------------------- */
/* ISA-L's igzip uses a streaming state struct whose layout we can't safely
 * declare without the header.  We probe for the library + entry symbols so
 * the backend reports "present", but the one-shot wrappers require the real
 * struct; until that's added (with the header on a host that has ISA-L),
 * the encode/decode return TSDB_HW_ERROR → software fallback.  The probe is
 * still useful: it proves the dispatch + naming + fallback path end to end. */
static int g_isal_present;

static int isal_encode(const uint8_t *in, size_t in_len,
                       uint8_t *out, size_t out_cap, size_t *out_len) {
    (void)in; (void)in_len; (void)out; (void)out_cap; (void)out_len;
    return TSDB_HW_ERROR;   /* not yet implemented — caller uses software */
}
static int isal_decode(const uint8_t *in, size_t in_len,
                       uint8_t *out, size_t orig_len, size_t *out_len) {
    (void)in; (void)in_len; (void)out; (void)orig_len; (void)out_len;
    return TSDB_HW_ERROR;
}

static int bind_isal(void) {
    static const char *libs[] = { "libisal.so.2", "libisal.so", NULL };
    void *h = try_dlopen(libs);
    if (!h) return 0;
    void *def = dlsym(h, "isal_deflate");
    void *inf = dlsym(h, "isal_inflate");
    if (!def || !inf) { dlclose(h); return 0; }
    g_isal_present = 1;
    g_backend.kind = TSDB_HW_BACKEND_ISAL;
    g_backend.name = "isal";
    g_backend.encode = isal_encode;
    g_backend.decode = isal_decode;
    g_backend.dl = h;
    return 1;
}
#endif /* HW_HAVE_DLOPEN */

/* ---- probe (once) ------------------------------------------------------- */

static void probe_once(void) {
    if (g_forced) return;  /* test hook already set the backend */

    const char *sel = getenv("TSDB_HWCOMPRESS");
    if (sel && strcasecmp(sel, "off") == 0) return;   /* stays NONE */

#if HW_HAVE_DLOPEN
    int want_qat  = !sel || !strcasecmp(sel, "auto") || !strcasecmp(sel, "qat");
    int want_isal = !sel || !strcasecmp(sel, "auto") || !strcasecmp(sel, "isal");

    if (want_qat  && bind_qat())  goto done;
    if (want_isal && bind_isal()) goto done;
done:
#endif
    fprintf(stderr, "[hwcompress] backend=%s (TSDB_HWCOMPRESS=%s)\n",
            g_backend.name, sel ? sel : "auto");
}

/* ---- public API --------------------------------------------------------- */

tsdb_hw_backend_t tsdb_hwcompress_backend(void) {
    pthread_once(&g_once, probe_once);
    return g_backend.kind;
}

int tsdb_hwcompress_available(void) {
    return tsdb_hwcompress_backend() != TSDB_HW_BACKEND_NONE;
}

const char *tsdb_hwcompress_name(void) {
    pthread_once(&g_once, probe_once);
    return g_backend.name;
}

int tsdb_hwcompress_encode(const uint8_t *in, size_t in_len,
                           uint8_t *out, size_t out_cap, size_t *out_len) {
    pthread_once(&g_once, probe_once);
    if (g_backend.kind == TSDB_HW_BACKEND_NONE || !g_backend.encode)
        return TSDB_HW_UNAVAIL;
    if (out_cap == 0) return TSDB_HW_NOSPACE;
    return g_backend.encode(in, in_len, out, out_cap, out_len);
}

int tsdb_hwcompress_decode(const uint8_t *in, size_t in_len,
                           uint8_t *out, size_t orig_len, size_t *out_len) {
    pthread_once(&g_once, probe_once);
    if (g_backend.kind == TSDB_HW_BACKEND_NONE || !g_backend.decode)
        return TSDB_HW_UNAVAIL;
    return g_backend.decode(in, in_len, out, orig_len, out_len);
}

void tsdb_hwcompress_force_backend_for_test(tsdb_hw_backend_t b) {
    /* Make the (already-run-or-not) probe a no-op and set state directly. */
    g_forced = 1;
    (void)pthread_once(&g_once, probe_once);  /* ensure once-flag consumed */
    g_backend.kind = b;
    switch (b) {
    case TSDB_HW_BACKEND_QAT:  g_backend.name = "qat";  break;
    case TSDB_HW_BACKEND_ISAL: g_backend.name = "isal"; break;
    default:                   g_backend.name = "none"; break;
    }
    /* No real codec is bound in the test path, so leave encode/decode NULL
     * → encode reports UNAVAIL (NONE) or the caller-visible "no fn" path. */
    if (b == TSDB_HW_BACKEND_NONE) { g_backend.encode = NULL; g_backend.decode = NULL; }
}
