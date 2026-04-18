/* iopolicy.c — see header for contract.
 *
 * Detection uses TSDB_IOPOLICY first, then Linux sysfs
 * (/sys/block/<name>/queue/rotational), and defaults to SSD elsewhere.
 * The per-device probe traces st_dev back to a major:minor pair and
 * looks up the base block name via /proc/partitions — similar to how
 * `lsblk` does it, but without the dependency.
 */

#include "iopolicy.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <strings.h>
#include <fcntl.h>

/* ---- Linux-only sysfs probe --------------------------------------------- */

#if defined(__linux__)

/* Read the contents of /sys/block/<name>/queue/rotational.
 * Returns 0 (SSD), 1 (HDD), or -1 if the file cannot be read. */
static int read_rotational(const char *dev_name) {
    char path[256];
    snprintf(path, sizeof(path), "/sys/block/%s/queue/rotational", dev_name);
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char buf[8] = {0};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    if (n == 0) return -1;
    /* Trim whitespace. */
    for (size_t i = 0; i < n; i++)
        if (buf[i] == '\n' || buf[i] == ' ') { buf[i] = '\0'; break; }
    if (buf[0] == '0') return 0;
    if (buf[0] == '1') return 1;
    return -1;
}

/* Strip a trailing partition number from a device name ("sda3" → "sda",
 * "nvme0n1p1" → "nvme0n1", "mmcblk0p1" → "mmcblk0").  Best-effort; returns
 * a newly-allocated string. */
static char *base_device_name(const char *dev) {
    size_t n = strlen(dev);
    if (n == 0) return strdup(dev);
    /* nvme / mmcblk use 'p' + digits. */
    if ((strncmp(dev, "nvme", 4) == 0 || strncmp(dev, "mmcblk", 6) == 0)) {
        const char *pp = strrchr(dev, 'p');
        if (pp && isdigit((unsigned char)pp[1])) {
            size_t keep = (size_t)(pp - dev);
            char *out = (char *)malloc(keep + 1);
            if (!out) return strdup(dev);
            memcpy(out, dev, keep);
            out[keep] = '\0';
            return out;
        }
        return strdup(dev);
    }
    /* Legacy SCSI/ATA: strip trailing digits. */
    size_t end = n;
    while (end > 0 && isdigit((unsigned char)dev[end - 1])) end--;
    if (end == 0) return strdup(dev);
    char *out = (char *)malloc(end + 1);
    if (!out) return strdup(dev);
    memcpy(out, dev, end);
    out[end] = '\0';
    return out;
}

/* Probe the block device that backs <path> and return rotational state.
 * Returns 0 (SSD), 1 (HDD), or -1 on failure. */
static int probe_linux(const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) return -1;
    unsigned major = (unsigned)((st.st_dev >> 8) & 0xFFFu);
    unsigned minor = (unsigned)(st.st_dev & 0xFFu);

    /* Walk /proc/partitions to find the device name for major:minor. */
    FILE *f = fopen("/proc/partitions", "r");
    if (!f) return -1;
    char line[256];
    int rc = -1;
    /* Skip header. */
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
    while (fgets(line, sizeof(line), f)) {
        unsigned mj, mn;
        unsigned long long blocks;
        char name[64];
        if (sscanf(line, "%u %u %llu %63s", &mj, &mn, &blocks, name) != 4)
            continue;
        if (mj == major && mn == minor) {
            char *base = base_device_name(name);
            rc = read_rotational(base);
            free(base);
            if (rc < 0) {
                /* The partition itself may expose rotational directly. */
                rc = read_rotational(name);
            }
            break;
        }
    }
    fclose(f);
    return rc;
}

#endif  /* __linux__ */

/* ---- Public API --------------------------------------------------------- */

static int parse_env_override(tsdb_iopolicy_t *out) {
    const char *env = getenv("TSDB_IOPOLICY");
    if (!env || !*env) return 0;
    if (!strcasecmp(env, "ssd")) { *out = TSDB_IOPOLICY_SSD; return 1; }
    if (!strcasecmp(env, "hdd")) { *out = TSDB_IOPOLICY_HDD; return 1; }
    /* "auto" or anything else → fall through to detection */
    return 0;
}

tsdb_iopolicy_t tsdb_iopolicy_detect(const char *path) {
    tsdb_iopolicy_t forced;
    if (parse_env_override(&forced)) return forced;

#if defined(__linux__)
    if (path) {
        int rot = probe_linux(path);
        if (rot == 0) return TSDB_IOPOLICY_SSD;
        if (rot == 1) return TSDB_IOPOLICY_HDD;
    }
#else
    (void)path;
#endif
    /* Default on non-Linux or unreadable sysfs. */
    return TSDB_IOPOLICY_SSD;
}

void tsdb_iopolicy_advise_read(tsdb_iopolicy_t p, void *addr, size_t len) {
    if (!addr || len == 0) return;
#if defined(MADV_SEQUENTIAL) && defined(MADV_WILLNEED)
    int advice = (p == TSDB_IOPOLICY_HDD) ? MADV_SEQUENTIAL : MADV_WILLNEED;
    (void)madvise(addr, len, advice);
#else
    (void)p;
#endif
}

void tsdb_iopolicy_advise_seq_fd(tsdb_iopolicy_t p, int fd) {
    if (fd < 0) return;
    /* SSD: kernel default readahead is already tuned well.  Avoid hints
     * that could displace hot pages in the page cache. */
    if (p != TSDB_IOPOLICY_HDD) return;
#if defined(__linux__) && defined(POSIX_FADV_SEQUENTIAL)
    /* Tell the kernel to read ahead aggressively + evict pages behind us
     * once read, and to prefetch the whole range into the page cache
     * asynchronously. .idx files are small (≪ MB per column-partition), so
     * the WILLNEED pass rarely blows the cache. */
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#  if defined(POSIX_FADV_WILLNEED)
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
#  endif
#elif defined(F_RDADVISE) && defined(F_RDAHEAD)
    /* macOS: F_RDAHEAD is a boolean; F_RDADVISE needs a range. Enable the
     * former only — macOS laptops default to SSD so this branch seldom
     * fires in practice. */
    (void)fcntl(fd, F_RDAHEAD, 1);
#else
    (void)fd;
#endif
}

size_t tsdb_iopolicy_write_buf_bytes(tsdb_iopolicy_t p) {
    /* HDD: 256 KiB — amortises seek latency over many block headers+data.
     * SSD: 0 — let stdio pick (typically 4–8 KiB), the syscall cost is
     *          negligible compared to the media. */
    return (p == TSDB_IOPOLICY_HDD) ? (size_t)(256u * 1024u) : 0u;
}

const char *tsdb_iopolicy_name(tsdb_iopolicy_t p) {
    return (p == TSDB_IOPOLICY_HDD) ? "hdd" : "ssd";
}
