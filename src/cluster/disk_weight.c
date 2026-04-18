/* disk_weight.c — see header. */

#include "disk_weight.h"

#include <sys/statvfs.h>
#include <stddef.h>
#include <stdint.h>

static int clamp_vn(int vn) {
    if (vn < TSDB_DISK_WEIGHT_MIN_VN) return TSDB_DISK_WEIGHT_MIN_VN;
    if (vn > TSDB_DISK_WEIGHT_MAX_VN) return TSDB_DISK_WEIGHT_MAX_VN;
    return vn;
}

int tsdb_disk_weight_detail(const char *data_dir, int base_vn_per_tb,
                             uint64_t *out_total_bytes,
                             uint64_t *out_free_bytes)
{
    if (base_vn_per_tb <= 0) base_vn_per_tb = TSDB_DISK_WEIGHT_DEFAULT_PER_TB;
    if (!data_dir) return -1;

    struct statvfs vfs;
    if (statvfs(data_dir, &vfs) != 0) return -1;

    /* f_frsize is the fundamental block size, used by f_blocks/f_bfree.
     * f_bsize is fs preferred I/O block (same on most systems).  Prefer
     * f_frsize per POSIX. */
    uint64_t bsize = (vfs.f_frsize > 0) ? vfs.f_frsize : vfs.f_bsize;
    uint64_t total_bytes = (uint64_t)vfs.f_blocks * bsize;
    uint64_t free_bytes  = (uint64_t)vfs.f_bavail * bsize;

    if (out_total_bytes) *out_total_bytes = total_bytes;
    if (out_free_bytes)  *out_free_bytes  = free_bytes;

    /* vn = (total_bytes / 1 TiB) * base_vn_per_tb, integer with 1 decimal
     * of precision. Using TiB (2^40) rather than TB (10^12) for tidy
     * math on binary disk sizes. */
    const uint64_t TIB = (uint64_t)1 << 40;
    /* scale*base / TIB — do the multiplication with 1 decimal factor to
     * avoid truncation for sub-TiB disks (a 500 GiB disk should yield
     * ~50 vn rather than 0). */
    uint64_t scaled = (total_bytes * (uint64_t)(base_vn_per_tb * 10)) / TIB;
    int vn = (int)((scaled + 5) / 10);
    return clamp_vn(vn);
}

int tsdb_disk_weight_from_statvfs(const char *data_dir, int base_vn_per_tb) {
    return tsdb_disk_weight_detail(data_dir, base_vn_per_tb, NULL, NULL);
}
