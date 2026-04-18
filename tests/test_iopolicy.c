/* test_iopolicy.c — HDD/SSD detection + env override + advise no-crash.
 *
 * This test exercises the policy API without requiring a specific device:
 *
 *   1. Default detect on an arbitrary path returns a valid policy.
 *   2. TSDB_IOPOLICY=ssd forces SSD regardless of hardware.
 *   3. TSDB_IOPOLICY=hdd forces HDD.
 *   4. TSDB_IOPOLICY=auto (or unset) falls through to detection.
 *   5. The recommended write-buffer size is 0 for SSD, >0 for HDD.
 *   6. advise_read() on a page-aligned mmap region does not crash.
 */

#include "../src/storage/iopolicy.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, m); g_fail++; } \
    else      { printf("PASS: %s\n", m); g_pass++; } \
} while (0)

int main(void) {
    printf("=== test_iopolicy ===\n");

    /* 1. Default detect. */
    unsetenv("TSDB_IOPOLICY");
    tsdb_iopolicy_t def = tsdb_iopolicy_detect("/tmp");
    printf("auto-detected /tmp policy: %s\n", tsdb_iopolicy_name(def));
    CHECK(def == TSDB_IOPOLICY_SSD || def == TSDB_IOPOLICY_HDD,
          "auto detection returns a valid policy");

    /* 2. ssd override. */
    setenv("TSDB_IOPOLICY", "ssd", 1);
    CHECK(tsdb_iopolicy_detect("/tmp") == TSDB_IOPOLICY_SSD,
          "TSDB_IOPOLICY=ssd forces SSD");

    /* 3. hdd override. */
    setenv("TSDB_IOPOLICY", "hdd", 1);
    CHECK(tsdb_iopolicy_detect("/tmp") == TSDB_IOPOLICY_HDD,
          "TSDB_IOPOLICY=hdd forces HDD");

    /* Case insensitive. */
    setenv("TSDB_IOPOLICY", "HDD", 1);
    CHECK(tsdb_iopolicy_detect("/tmp") == TSDB_IOPOLICY_HDD,
          "case insensitive");

    /* 4. auto falls through. */
    setenv("TSDB_IOPOLICY", "auto", 1);
    tsdb_iopolicy_t a = tsdb_iopolicy_detect("/tmp");
    CHECK(a == def, "auto matches no-env detection");

    /* Unknown string → fall through to detection. */
    setenv("TSDB_IOPOLICY", "bogus", 1);
    tsdb_iopolicy_t b = tsdb_iopolicy_detect("/tmp");
    CHECK(b == def, "bogus value falls back to detection");

    /* Reset for subsequent tests. */
    unsetenv("TSDB_IOPOLICY");

    /* 5. write buffer sizing. */
    CHECK(tsdb_iopolicy_write_buf_bytes(TSDB_IOPOLICY_SSD) == 0,
          "SSD write buffer recommendation is 0 (stdio default)");
    CHECK(tsdb_iopolicy_write_buf_bytes(TSDB_IOPOLICY_HDD) >= 64u * 1024u,
          "HDD write buffer recommendation is >= 64 KiB");

    /* 7. advise_seq_fd — no crash on any combination of (policy, fd). */
    {
        /* Create a temp file with some bytes. */
        char path[] = "/tmp/tsdb_iopolicy_seqfdXXXXXX";
        int fd = mkstemp(path);
        CHECK(fd >= 0, "mkstemp");
        if (fd >= 0) {
            char data[4096] = {0};
            (void)write(fd, data, sizeof(data));
            lseek(fd, 0, SEEK_SET);
            tsdb_iopolicy_advise_seq_fd(TSDB_IOPOLICY_SSD, fd);
            tsdb_iopolicy_advise_seq_fd(TSDB_IOPOLICY_HDD, fd);
            /* Invalid fd → silent no-op. */
            tsdb_iopolicy_advise_seq_fd(TSDB_IOPOLICY_HDD, -1);
            close(fd);
            unlink(path);
            CHECK(1, "advise_seq_fd handled both policies + bad fd");
        }
    }

    /* 6. advise_read on an anonymous mmap region — no crash. */
    size_t pg = (size_t)sysconf(_SC_PAGESIZE);
    void *region = mmap(NULL, pg * 4, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANON, -1, 0);
    CHECK(region != MAP_FAILED, "mmap scratch region");
    if (region != MAP_FAILED) {
        tsdb_iopolicy_advise_read(TSDB_IOPOLICY_SSD, region, pg * 4);
        tsdb_iopolicy_advise_read(TSDB_IOPOLICY_HDD, region, pg * 4);
        tsdb_iopolicy_advise_read(TSDB_IOPOLICY_SSD, NULL, 0);  /* no-op guard */
        tsdb_iopolicy_advise_read(TSDB_IOPOLICY_HDD, region, 0); /* no-op guard */
        munmap(region, pg * 4);
        CHECK(1, "advise_read + guards did not crash");
    }

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
