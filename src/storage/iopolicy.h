/* iopolicy.h — runtime-detected I/O policy for the storage engine.
 *
 * The storage layer reads sequentially through mmap'd .col files; the
 * kernel's readahead behaviour and the page-cache locality characteristics
 * differ markedly between spinning media (HDD) and solid-state media (SSD).
 *
 *   SSD  — bandwidth-bound, no seek penalty.  Readahead beyond one page is
 *          usually neutral or counterproductive (wastes DRAM on data we
 *          already decode quickly).  Stick with the kernel default.
 *   SATA — rotational consumer/nearline HDD.  Seek-bound: favour large,
 *          contiguous I/O + aggressive readahead so the platter rotates
 *          past a whole block while we decompress.  madvise(MADV_SEQUENTIAL)
 *          + large (256 KiB) stdio write buffer.
 *   SAS  — rotational enterprise HDD (10k/15k RPM).  Seek-bound too, but
 *          lower latency than SATA — intermediate readahead / write buffer
 *          (128 KiB) so we don't over-prefetch on faster spindles.
 *
 * Detection order:
 *   1. TSDB_DISK_ENGINE env var (ssd|sata|sas|hdd|auto) — preferred; or
 *      legacy TSDB_IOPOLICY (ssd|hdd|auto).  Explicit class wins.
 *   2. /sys/block/<dev>/queue/rotational on Linux (0 → SSD).  Rotational
 *      devices are probed for a SCSI SAS transport (sas_address present →
 *      SAS, else SATA); the rotational flag alone cannot tell SATA from
 *      SAS, so the transport probe is best-effort and falls back to SATA.
 *   3. Default SSD on other platforms (macOS desktops, cloud VMs).
 *
 * This is an I/O *tuning profile*, not a separate storage engine — the
 * on-disk format is identical across classes; only access hints differ.
 *
 * Thread-safe; values are cached per data directory after first query.
 */
#ifndef TSDB_STORAGE_IOPOLICY_H
#define TSDB_STORAGE_IOPOLICY_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    TSDB_IOPOLICY_SSD = 0,
    TSDB_IOPOLICY_HDD = 1,   /* rotational SATA — canonical name SATA below */
    TSDB_IOPOLICY_SAS = 2,   /* rotational SAS enterprise HDD              */
} tsdb_iopolicy_t;

/* SATA is the canonical name for the rotational consumer-HDD class; HDD
 * stays as a back-compat alias so existing callers keep compiling. */
#define TSDB_IOPOLICY_SATA TSDB_IOPOLICY_HDD

/* Return the best-guess policy for <path>.  Reads TSDB_IOPOLICY env var
 * first; otherwise probes the block device on Linux; otherwise defaults
 * SSD.  Safe to call before any DB state exists. */
tsdb_iopolicy_t tsdb_iopolicy_detect(const char *path);

/* Apply madvise() hints appropriate for the policy to a read-only mmap'd
 * region. No-op on failure (madvise is advisory). */
void tsdb_iopolicy_advise_read(tsdb_iopolicy_t p, void *addr, size_t len);

/* Apply posix_fadvise() SEQUENTIAL + WILLNEED on an open fd opened for
 * reading.  Used on .idx files — sequentially scanned, typically small
 * enough that a full prefetch fits in page cache.  No-op on platforms
 * without posix_fadvise (macOS) and on SSD policy. */
void tsdb_iopolicy_advise_seq_fd(tsdb_iopolicy_t p, int fd);

/* Recommended stdio write-buffer size (bytes) for append-heavy writes on
 * this medium.  HDD favours larger buffers to coalesce syscall load; SSD
 * is fine with the stdio default (0 = caller should not call setvbuf). */
size_t tsdb_iopolicy_write_buf_bytes(tsdb_iopolicy_t p);

/* Returns "ssd" / "hdd" — intended for logging + the test suite. */
const char *tsdb_iopolicy_name(tsdb_iopolicy_t p);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_IOPOLICY_H */
