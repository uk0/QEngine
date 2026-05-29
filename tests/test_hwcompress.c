/* test_hwcompress.c — hardware-compression dispatch + software-fallback
 * contract.  Runs on any host (no accelerator required): the point is to
 * prove that absence of hardware yields a clean "use software" signal,
 * never a crash or a wrong codec. */

#include "../src/compress/hwcompress.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

static int g_pass = 0, g_fail = 0;
#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

int main(void) {
    printf("=== test_hwcompress ===\n");

    /* 1. Backend resolves to a valid enum and a matching name. */
    tsdb_hw_backend_t b = tsdb_hwcompress_backend();
    const char *name = tsdb_hwcompress_name();
    printf("probed backend: %s\n", name);
    CHECK(b == TSDB_HW_BACKEND_NONE || b == TSDB_HW_BACKEND_QAT ||
          b == TSDB_HW_BACKEND_ISAL, "backend is a valid enum");
    CHECK(tsdb_hwcompress_available() == (b != TSDB_HW_BACKEND_NONE),
          "available() agrees with backend()");
    if (b == TSDB_HW_BACKEND_NONE) CHECK(strcmp(name, "none") == 0, "none name");

    /* 2. Fallback contract: with no bound backend, encode/decode must
     *    report UNAVAIL (the signal callers use to run software) — never
     *    crash, never claim success. */
    uint8_t in[256], out[512];
    for (int i = 0; i < 256; i++) in[i] = (uint8_t)(i * 7 + 3);
    size_t out_len = 0;
    int rc = tsdb_hwcompress_encode(in, sizeof(in), out, sizeof(out), &out_len);
    if (!tsdb_hwcompress_available()) {
        CHECK(rc == TSDB_HW_UNAVAIL, "encode UNAVAIL when no hardware (software fallback)");
    } else {
        CHECK(rc == TSDB_HW_OK || rc == TSDB_HW_ERROR || rc == TSDB_HW_NOSPACE,
              "encode returns a defined code when hardware present");
    }

    /* 3. out_cap == 0 guard. */
    rc = tsdb_hwcompress_encode(in, sizeof(in), out, 0, &out_len);
    CHECK(rc == TSDB_HW_UNAVAIL || rc == TSDB_HW_NOSPACE,
          "zero out_cap is rejected (UNAVAIL or NOSPACE), never OK");

    /* 4. Test hook: force NONE → encode must report UNAVAIL deterministically. */
    tsdb_hwcompress_force_backend_for_test(TSDB_HW_BACKEND_NONE);
    CHECK(tsdb_hwcompress_available() == 0, "forced NONE → unavailable");
    rc = tsdb_hwcompress_encode(in, sizeof(in), out, sizeof(out), &out_len);
    CHECK(rc == TSDB_HW_UNAVAIL, "forced NONE → encode UNAVAIL (clean fallback)");
    CHECK(strcmp(tsdb_hwcompress_name(), "none") == 0, "forced NONE name");

    /* 5. Forcing a backend name flips the reported name (probe-bypass). */
    tsdb_hwcompress_force_backend_for_test(TSDB_HW_BACKEND_QAT);
    CHECK(strcmp(tsdb_hwcompress_name(), "qat") == 0, "forced QAT name");
    /* No real lib bound in test → encode must NOT crash; it returns a
     * defined non-OK code so the caller still falls back to software. */
    rc = tsdb_hwcompress_encode(in, sizeof(in), out, sizeof(out), &out_len);
    CHECK(rc == TSDB_HW_UNAVAIL || rc == TSDB_HW_ERROR,
          "forced QAT w/o lib → defined non-OK (software fallback), no crash");

    tsdb_hwcompress_force_backend_for_test(TSDB_HW_BACKEND_NONE);  /* restore */

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
