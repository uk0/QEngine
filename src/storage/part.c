/* part.c — disk partition flush and read. */

/* utimensat()/AT_FDCWD are POSIX.1-2008 and are what col_idx_append_publish
 * uses to bump the partition dir's mtime.  glibc hides BOTH behind
 * __USE_ATFILE, which the Makefile's default -std=c11 does NOT set (strict
 * ANSI suppresses _DEFAULT_SOURCE), so on Linux AT_FDCWD is an undeclared
 * identifier — a hard error — without this.  Measured: gcc 13 / glibc 2.36,
 * `gcc -std=c11`, "error: 'AT_FDCWD' undeclared".  _DEFAULT_SOURCE and
 * _DARWIN_C_SOURCE give back the BSD/Darwin namespace that naming a strict
 * POSIX level would otherwise take away.  Same preamble as db_cluster.c. */
#ifndef _POSIX_C_SOURCE
#  define _POSIX_C_SOURCE 200809L
#endif
#ifndef _DEFAULT_SOURCE
#  define _DEFAULT_SOURCE 1
#endif
#ifndef _DARWIN_C_SOURCE
#  define _DARWIN_C_SOURCE 1
#endif

#include "part.h"
#include "iopolicy.h"
#include "io_async.h"
#include "../compress/codec.h"
#include "../core/bits.h"
#include "../server/proto.h"  /* tsdb_crc32c — block-level integrity check */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>

/* Thread-local io_async ring used by tsdb_part_fsync_fd — lazily created
 * on first use, destroyed at thread exit.  Each flush thread gets its own
 * ring so submissions don't need cross-thread serialisation.
 *
 * Default ON since iter 4 (cluster validated: 1.40M → 1.55M @480s, errs=0,
 * reverse-drift); opt OUT via env TSDB_PART_IO_URING=0 if a deployment
 * needs to fall back to the legacy fsync path (e.g. kernel < 5.1, or
 * seccomp blocks io_uring_setup).  When the ring can't be created
 * (TSDB_USE_IOURING not compiled, kernel rejects setup, etc) we fall
 * back to plain fsync() per-call — the writer just pays the syscall
 * cost without the ring overhead. */
static __thread tsdb_io_async_t *tl_io_async   = NULL;
static __thread int              tl_io_async_inited = 0;
static __thread int              tl_io_async_enabled = 0;

/* fsync one fd (idx OR .col) to the device.  Returns 0 on success, -1 on
 * error — callers MUST check it before treating a publish as durable.  Routes
 * through the thread-local io_uring ring when available (fsync_sync is one
 * submit+wait, identical semantics to plain fsync) so future iters can batch
 * many fsyncs in one device dispatch; falls back to plain fsync. */
static int tsdb_part_fsync_fd(int fd) {
    if (!tl_io_async_inited) {
        tl_io_async_inited = 1;
        const char *e = getenv("TSDB_PART_IO_URING");
        tl_io_async_enabled = !(e && e[0] == '0');   /* default ON */
        if (tl_io_async_enabled && tsdb_io_async_available()) {
            if (tsdb_io_async_create(32, &tl_io_async) != 0) tl_io_async = NULL;
        }
    }
    if (tl_io_async && tsdb_io_async_fsync_sync(tl_io_async, fd) == 0) return 0;
    return fsync(fd);
}

/* ---- per-partition idx publish lock ---------------------------------------
 *
 * See the contract on tsdb_part_idx_lock in part.h.  Fixed stripe table, so
 * there is nothing to allocate, nothing to free, and no per-partition state to
 * garbage-collect.  pthread_once (not a repeated PTHREAD_MUTEX_INITIALIZER
 * array initialiser, which gcc rejects as non-constant on some libcs). */
#define PART_IDX_LOCK_STRIPES 64u
static pthread_mutex_t g_part_idx_locks[PART_IDX_LOCK_STRIPES];
static pthread_once_t  g_part_idx_locks_once = PTHREAD_ONCE_INIT;

static void part_idx_locks_init(void) {
    for (unsigned i = 0; i < PART_IDX_LOCK_STRIPES; i++)
        pthread_mutex_init(&g_part_idx_locks[i], NULL);
}

static unsigned part_idx_stripe(const char *part_dir) {
    uint32_t h = 2166136261u;                       /* FNV-1a */
    for (const unsigned char *p = (const unsigned char *)part_dir; *p; p++) {
        h ^= (uint32_t)*p;
        h *= 16777619u;
    }
    return (unsigned)(h % PART_IDX_LOCK_STRIPES);
}

void tsdb_part_idx_lock(const char *part_dir) {
    if (!part_dir) return;
    pthread_once(&g_part_idx_locks_once, part_idx_locks_init);
    pthread_mutex_lock(&g_part_idx_locks[part_idx_stripe(part_dir)]);
}

void tsdb_part_idx_unlock(const char *part_dir) {
    if (!part_dir) return;
    pthread_once(&g_part_idx_locks_once, part_idx_locks_init);
    pthread_mutex_unlock(&g_part_idx_locks[part_idx_stripe(part_dir)]);
}

/* fsync a directory so a rename inside it is durable.  POSIX makes rename
 * ATOMIC but says nothing about its durability: without this a crash can lose
 * the rename and resurrect the previous idx — which manufactures exactly the
 * short-column state this file's alignment pass has to clean up, on a node
 * that never lost a single network push.  Copies the compactor's pattern; a
 * filesystem that rejects fsync on a directory fd just returns EINVAL and we
 * are no worse off than before. */
static void part_fsync_dir(const char *dir) {
    int dfd = open(dir, O_RDONLY);
    if (dfd >= 0) { (void)fsync(dfd); close(dfd); }
}

/*
 * Raw-block hook type local to part.c (mirrors db.h's tsdb_on_raw_block_fn,
 * but avoids a circular include since db.h includes part.h).
 * tsdb_db_get_raw_block_hook is defined in db.c.
 */
typedef int (*part_raw_block_fn_t)(void *ud, struct tsdb_db *db,
                                    const char *table_name,
                                    uint32_t part_day,
                                    uint16_t col_idx,
                                    const tsdb_block_meta_t *meta,
                                    const uint8_t *block_bytes,
                                    size_t block_bytes_len);
/* out_fn is void** (function pointer via void*) to avoid typedef mismatch. */
extern void tsdb_db_get_raw_block_hook(struct tsdb_db *db,
                                        void **out_fn,
                                        void **out_ud);

/* Forward declaration of mkdir_p from schema.c. */
extern int tsdb_mkdir_p(const char *path);

/* ---- Byte-level serialization helpers ----------------------------------- */

static inline void put_u8(uint8_t *p, uint8_t v)    { p[0] = v; }
static inline void put_u16le(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static inline void put_u32le(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static inline void put_u64le(uint8_t *p, uint64_t v) {
    put_u32le(p,   (uint32_t)v);
    put_u32le(p+4, (uint32_t)(v>>32));
}
static inline void put_i64le(uint8_t *p, int64_t v)  { put_u64le(p, (uint64_t)v); }

static inline uint32_t get_u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static inline uint64_t get_u64le(const uint8_t *p) {
    return (uint64_t)get_u32le(p) | ((uint64_t)get_u32le(p+4)<<32);
}
static inline int64_t  get_i64le(const uint8_t *p)  { return (int64_t)get_u64le(p); }
static inline uint16_t get_u16le(const uint8_t *p)  { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }

/* ---- Partition directory name from timestamp ----------------------------- */

/*
 * Stringify a timestamp into its partition directory name, honouring the
 * schema's partition_unit:
 *   DAY  -> "YYYYMMDD"        (9 bytes incl. NUL)
 *   HOUR -> "YYYYMMDDHH"     (11 bytes incl. NUL)
 * buf must be at least 11 bytes.
 */
static void ts_to_part_str(int64_t ts_ns, tsdb_partition_unit_t unit, char *buf) {
    time_t secs = (time_t)(ts_ns / 1000000000LL);
    struct tm tm;
    gmtime_r(&secs, &tm);
    if (unit == TSDB_PARTITION_HOUR) {
        snprintf(buf, 11, "%04d%02d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour);
    } else {
        snprintf(buf, 11, "%04d%02d%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    }
}

/*
 * Partition index = unique bucket number covering one row's timestamp.
 * DAY  -> days since 1970-01-01   (negative OK)
 * HOUR -> hours since 1970-01-01 00:00 UTC
 */
static int64_t ts_part_index(int64_t ts_ns, tsdb_partition_unit_t unit) {
    int64_t ns_per_unit = (unit == TSDB_PARTITION_HOUR)
                          ? 3600000000000LL
                          : 86400000000000LL;
    if (ts_ns >= 0) return ts_ns / ns_per_unit;
    return (ts_ns - ns_per_unit + 1) / ns_per_unit;
}

/*
 * Pack a partition string into a uint32 integer identifier used by the
 * cluster's raw-block hook (opaque to consumers). YYYYMMDD fits into
 * u32. YYYYMMDDHH overflows u32 — for HOUR partitions we compute
 * day*100+hour*1 packed differently; we encode as (year*1000000 +
 * month*10000 + day*100 + hour) which fits within u32 through year 4294.
 */
static uint32_t part_str_to_int(const char *s, tsdb_partition_unit_t unit) {
    unsigned y = 0, mo = 0, dy = 0, hh = 0;
    if (unit == TSDB_PARTITION_HOUR) {
        sscanf(s, "%4u%2u%2u%2u", &y, &mo, &dy, &hh);
        return (uint32_t)(y * 1000000u + mo * 10000u + dy * 100u + hh);
    }
    sscanf(s, "%4u%2u%2u", &y, &mo, &dy);
    return (uint32_t)(y * 10000u + mo * 100u + dy);
}

/* ---- BlockHeader helpers ------------------------------------------------ */

/*
 * BlockHeader layout (32 bytes, little-endian):
 *   [0..3]   magic   u32 = TSDB_BLOCK_MAGIC
 *   [4]      codec   u8
 *   [5]      _pad    u8
 *   [6..7]   flags   u16
 *   [8..11]  count   u32
 *   [12..19] ts_min  i64
 *   [20..27] ts_max  i64
 *   [28..31] size    u32  (compressed data bytes following this header)
 */
static size_t write_block_header(uint8_t *buf,
                                 uint8_t codec, uint16_t flags,
                                 uint32_t count,
                                 int64_t ts_min, int64_t ts_max,
                                 uint32_t data_size)
{
    put_u32le(buf + 0,  TSDB_BLOCK_MAGIC);
    put_u8   (buf + 4,  codec);
    put_u8   (buf + 5,  0);
    put_u16le(buf + 6,  flags);
    put_u32le(buf + 8,  count);
    put_i64le(buf + 12, ts_min);
    put_i64le(buf + 20, ts_max);
    put_u32le(buf + 28, data_size);
    return TSDB_BLOCK_HEADER_SIZE;
}

static int read_block_header(const uint8_t *buf,
                             uint8_t *out_codec, uint16_t *out_flags,
                             uint32_t *out_count,
                             int64_t *out_ts_min, int64_t *out_ts_max,
                             uint32_t *out_size)
{
    uint32_t magic = get_u32le(buf);
    if (magic != TSDB_BLOCK_MAGIC) return TSDB_ERR_CORRUPT;
    *out_codec  = buf[4];
    *out_flags  = get_u16le(buf + 6);
    *out_count  = get_u32le(buf + 8);
    *out_ts_min = get_i64le(buf + 12);
    *out_ts_max = get_i64le(buf + 20);
    *out_size   = get_u32le(buf + 28);
    return TSDB_OK;
}

/*
 * IdxHeader v4 layout (48 bytes, little-endian):
 *   [0..3]    magic          u32 = TSDB_IDX_MAGIC
 *   [4..7]    count          u32
 *   [8..9]    version        u16 = 4
 *   [10..11]  _pad           u16
 *   [12..19]  total_rows     u64
 *   [20..27]  file_ts_min    i64   (file-level zone map — v2+)
 *   [28..35]  file_ts_max    i64
 *   [36..37]  entry_size     u16   = TSDB_IDX_ENTRY_SIZE (88)
 *   [38..39]  stats_variant  u16   = 0  (reserved; future stats layouts)
 *   [40..47]  max_seq        u64   (durable WAL redo checkpoint — v4)
 *
 * V3 header is 40 bytes (no max_seq). V2 is 36 bytes (no entry_size/variant).
 * V1 is 20 bytes (no zone map). Readers negotiate by version; pre-v4 readers
 * stop at byte 40 and never see max_seq, so the bump is forward-compatible.
 */
static size_t write_idx_header(uint8_t *buf, uint32_t count, uint64_t total_rows,
                               int64_t file_ts_min, int64_t file_ts_max,
                               uint64_t max_seq, uint16_t ncols) {
    /* Emit v4 (48 bytes, carries max_seq) ONLY when a WAL redo checkpoint is
     * present.  With max_seq == 0 (the default flush-on-commit path) emit a
     * byte-identical v3 header, so default-mode partitions stay unchanged. */
    /* Column-count stamp at [10..11] (see part.h).  Anything outside
     * [1, TSDB_MAX_COLS] is written as "unknown" rather than propagated, so a
     * garbage value can never make the reader STRICTER than it should be. */
    if (ncols > (uint16_t)TSDB_MAX_COLS) ncols = TSDB_IDX_NCOLS_UNKNOWN;
    put_u32le(buf + 0,  TSDB_IDX_MAGIC);
    put_u32le(buf + 4,  count);
    put_u64le(buf + 12, total_rows);
    put_i64le(buf + 20, file_ts_min);
    put_i64le(buf + 28, file_ts_max);
    put_u16le(buf + 36, (uint16_t)TSDB_IDX_ENTRY_SIZE);  /* entry_size */
    put_u16le(buf + 38, 0u);                              /* stats_variant */
    if (max_seq == 0) {
        put_u16le(buf + 8,  3);                           /* version = 3 */
        put_u16le(buf + 10, ncols);
        return TSDB_IDX_HEADER_SIZE_V3;
    }
    put_u16le(buf + 8,  4);                               /* version = 4 */
    put_u16le(buf + 10, ncols);
    put_u64le(buf + 40, max_seq);                         /* WAL redo checkpoint */
    return TSDB_IDX_HEADER_SIZE;
}

/*
 * Public idx-header writer — the ONE canonical encoder, shared by the flush
 * path (col_writer_close) and the raw-block replication path (rawblock.c) so
 * both stamp byte-identical headers and select V3/V4 the same way.  `buf` must
 * hold at least TSDB_IDX_HEADER_SIZE bytes.  Returns the header size written
 * (40 for V3 when max_seq==0, 48 for V4 when max_seq>0).
 */
size_t tsdb_part_write_idx_header(uint8_t *buf, uint32_t count,
                                  uint64_t total_rows,
                                  int64_t file_ts_min, int64_t file_ts_max,
                                  uint64_t max_seq, uint16_t ncols)
{
    return write_idx_header(buf, count, total_rows,
                            file_ts_min, file_ts_max, max_seq, ncols);
}

/*
 * Decode an IdxHeader supporting v1 / v2 / v3 / v4.
 *
 * Returns the number of header bytes consumed (20 for v1, 36 for v2,
 * 40 for v3, 48 for v4), or -1 on corruption.  Also reports the per-entry
 * size: v1/v2 → 40; v3/v4 → value read from header (normally 88).
 *
 * out_max_seq (may be NULL) receives the durable WAL redo checkpoint: the
 * v4 max_seq field, or 0 for any pre-v4 header (no checkpoint recorded).
 */
static int read_idx_header_ex(const uint8_t *buf, size_t avail,
                              uint32_t *out_count, uint16_t *out_version,
                              uint64_t *out_total_rows,
                              int64_t  *out_file_ts_min, int64_t *out_file_ts_max,
                              uint32_t *out_entry_size, uint64_t *out_max_seq)
{
    if (out_max_seq) *out_max_seq = 0;
    if (avail < TSDB_IDX_HEADER_SIZE_V1) return -1;
    if (get_u32le(buf) != TSDB_IDX_MAGIC) return -1;
    *out_count      = get_u32le(buf + 4);
    *out_version    = get_u16le(buf + 8);
    *out_total_rows = get_u64le(buf + 12);

    if (*out_version == 1) {
        *out_file_ts_min = INT64_MAX;
        *out_file_ts_max = INT64_MIN;
        *out_entry_size  = TSDB_IDX_ENTRY_SIZE_V2;
        return (int)TSDB_IDX_HEADER_SIZE_V1;
    }
    if (*out_version == 2) {
        if (avail < TSDB_IDX_HEADER_SIZE_V2) return -1;
        *out_file_ts_min = get_i64le(buf + 20);
        *out_file_ts_max = get_i64le(buf + 28);
        *out_entry_size  = TSDB_IDX_ENTRY_SIZE_V2;
        return (int)TSDB_IDX_HEADER_SIZE_V2;
    }
    if (*out_version == 3) {
        if (avail < TSDB_IDX_HEADER_SIZE_V3) return -1;
        *out_file_ts_min = get_i64le(buf + 20);
        *out_file_ts_max = get_i64le(buf + 28);
        uint16_t esz = get_u16le(buf + 36);
        /* stats_variant @38..39 — unused today */
        *out_entry_size = (esz == 0) ? TSDB_IDX_ENTRY_SIZE : esz;
        return (int)TSDB_IDX_HEADER_SIZE_V3;
    }
    if (*out_version == 4) {
        if (avail < TSDB_IDX_HEADER_SIZE) return -1;
        *out_file_ts_min = get_i64le(buf + 20);
        *out_file_ts_max = get_i64le(buf + 28);
        uint16_t esz = get_u16le(buf + 36);
        *out_entry_size = (esz == 0) ? TSDB_IDX_ENTRY_SIZE : esz;
        if (out_max_seq) *out_max_seq = get_u64le(buf + 40);
        return (int)TSDB_IDX_HEADER_SIZE;
    }
    return -1;   /* unknown version */
}

/*
 * The column-count stamp at bytes [10..11] of an already-read idx header.
 *
 * Honoured only for V3+ (write_idx_header is the only producer that has ever
 * put a non-zero value there, and it has emitted V3/V4 since it existed) and
 * only when the value is a plausible column count.  Anything else reads back as
 * "unknown", which keeps the reader at its pre-stamp behaviour — a garbage
 * value must never be able to make it STRICTER, only the absence of one makes
 * it more permissive.
 */
static uint16_t idx_hdr_ncols(const uint8_t *buf, size_t avail, uint16_t version)
{
    if (version < 3 || avail < TSDB_IDX_HEADER_SIZE_V3)
        return TSDB_IDX_NCOLS_UNKNOWN;
    uint16_t n = get_u16le(buf + 10);
    return (n <= (uint16_t)TSDB_MAX_COLS) ? n : TSDB_IDX_NCOLS_UNKNOWN;
}

uint16_t tsdb_part_idx_ncols(const char *idx_path)
{
    if (!idx_path) return TSDB_IDX_NCOLS_UNKNOWN;
    FILE *f = fopen(idx_path, "rb");
    if (!f) return TSDB_IDX_NCOLS_UNKNOWN;
    uint8_t hdr[TSDB_IDX_HEADER_SIZE];
    size_t  n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, f);
    fclose(f);

    uint32_t cnt = 0, esz = 0;
    uint16_t ver = 0;
    uint64_t tot = 0, mseq = 0;
    int64_t  fmn = 0, fmx = 0;
    if (read_idx_header_ex(hdr, n, &cnt, &ver, &tot, &fmn, &fmx, &esz, &mseq) <= 0)
        return TSDB_IDX_NCOLS_UNKNOWN;
    return idx_hdr_ncols(hdr, n, ver);
}

/*
 * Recover the true idx header size for a mixed-writer "mongrel" idx file.
 *
 * Two writers stamp idx headers: the flush path (write_idx_header — V3 40
 * bytes when max_seq==0, V4 48 bytes when max_seq>0) and the raw-block
 * replication path (rawblk_write_idx_header).  If a single partition is
 * touched by BOTH and they disagree on the header size, the file can end up
 * with a version byte whose IMPLIED header size does not match where the
 * fixed-size entries actually begin — e.g. a V4-sized body (entries at 48)
 * carrying a V3 version byte, so a reader keying off the version reads
 * entry0 at offset 40 (garbage) and filters every block → 0 rows for a
 * partition full of durable data.
 *
 * The on-disk entry array is fixed-stride, so the layout is self-checking:
 * for the correct header size H, (idx_file_size - H) is an exact multiple of
 * entry_size AND equals block_count*entry_size.  Given the version-derived
 * header size `hdr_size`, verify that invariant; if it fails, try the only
 * other header size a stats-bearing idx uses (V3 40 <-> V4 48) and return
 * whichever fits.  Returns the chosen header size, or `hdr_size` unchanged
 * when neither candidate is a clean fit (leave the existing behaviour).
 *
 * `idx_file_size` is the total byte length of the .idx file.  Logs once to
 * stderr when a recovery actually changes the header size.
 */
static int idx_recover_header_size(int hdr_size, uint32_t entry_size,
                                   uint32_t block_count, uint64_t idx_file_size,
                                   const char *where)
{
    if (hdr_size <= 0 || entry_size == 0 || block_count == 0) return hdr_size;

    uint64_t want = (uint64_t)block_count * entry_size;

    /* Candidate 1: the version-derived header size. */
    if (idx_file_size >= (uint64_t)hdr_size &&
        idx_file_size - (uint64_t)hdr_size == want) {
        return hdr_size;   /* self-consistent — the common case */
    }

    /* Candidate 2: the alternate stats-era header size (40 <-> 48).  Only
     * V3/V4 carry 88-byte stats entries, which is the only case that can
     * mongrel (V1/V2 use 40-byte entries and a distinct size). */
    if (entry_size == TSDB_IDX_ENTRY_SIZE) {
        int alt = (hdr_size == (int)TSDB_IDX_HEADER_SIZE)      /* 48 -> 40 */
                      ? (int)TSDB_IDX_HEADER_SIZE_V3
                  : (hdr_size == (int)TSDB_IDX_HEADER_SIZE_V3) /* 40 -> 48 */
                      ? (int)TSDB_IDX_HEADER_SIZE
                      : 0;
        if (alt > 0 && idx_file_size >= (uint64_t)alt &&
            idx_file_size - (uint64_t)alt == want) {
            fprintf(stderr,
                    "[part] %s: idx header size %d inconsistent with file "
                    "(size=%llu count=%u esz=%u); recovering as %d\n",
                    where ? where : "?", hdr_size,
                    (unsigned long long)idx_file_size, block_count, entry_size,
                    alt);
            return alt;
        }
    }

    return hdr_size;   /* no clean alternate — caller keeps prior behaviour */
}

/*
 * Probe an existing idx file's header: report its version, per-entry size,
 * file-level zone map, total rows, and durable max_seq checkpoint — applying
 * the mixed-writer header-size recovery so a V3/V4-mongrel reports the values
 * that match where its entries actually live.  Returns the (recovered) header
 * size, 0 if the file is absent/too short, or -1 on a corrupt magic/version.
 *
 * The raw-block writer uses this to PRESERVE an existing partition's idx
 * version and carry its max_seq forward, instead of silently downgrading a
 * V4 partition to V3 (which would drop the WAL redo checkpoint).
 *
 * ENOENT IS THE ONLY "THERE IS NO INDEX HERE" ANSWER.  Every other fopen
 * failure — EMFILE/ENFILE at the fd limit, EACCES, EIO — means an index may
 * well exist and simply could not be looked at, and the callers that REWRITE a
 * manifest from what this reports (rawblock.c's applier, migrate.c's prime)
 * turn "absent" into a fresh index over an N-entry one.  Report those as -1,
 * the same answer an unparseable header gets, so a writer refuses rather than
 * guessing; read-only probes already treat -1 as conservatively as 0.
 */
int tsdb_part_idx_probe(const char *idx_path,
                        uint16_t *out_version, uint32_t *out_count,
                        uint32_t *out_entry_size, uint64_t *out_total_rows,
                        int64_t *out_file_ts_min, int64_t *out_file_ts_max,
                        uint64_t *out_max_seq)
{
    if (out_version)     *out_version     = 0;
    if (out_count)       *out_count       = 0;
    if (out_entry_size)  *out_entry_size  = 0;
    if (out_total_rows)  *out_total_rows  = 0;
    if (out_file_ts_min) *out_file_ts_min = INT64_MAX;
    if (out_file_ts_max) *out_file_ts_max = INT64_MIN;
    if (out_max_seq)     *out_max_seq     = 0;

    FILE *f = fopen(idx_path, "rb");
    if (!f) return (errno == ENOENT) ? 0 : -1;   /* see the ENOENT note above */

    uint8_t hdr[TSDB_IDX_HEADER_SIZE];
    size_t n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, f);

    uint32_t cnt = 0, esz = 0;
    uint16_t ver = 0;
    uint64_t tot = 0, mseq = 0;
    int64_t  fmn = INT64_MAX, fmx = INT64_MIN;
    int hsz = read_idx_header_ex(hdr, n, &cnt, &ver, &tot,
                                  &fmn, &fmx, &esz, &mseq);
    if (hsz > 0 && esz > 0 && cnt > 0) {
        struct stat ist;
        if (fstat(fileno(f), &ist) == 0 && ist.st_size > 0) {
            int rhsz = idx_recover_header_size(hsz, esz, cnt,
                                               (uint64_t)ist.st_size, idx_path);
            /* If recovery moved us to a V4 header, re-read max_seq from the
             * (now correct) header offset so a mongrel's checkpoint survives. */
            if (rhsz != hsz && rhsz == (int)TSDB_IDX_HEADER_SIZE &&
                (size_t)ist.st_size >= TSDB_IDX_HEADER_SIZE) {
                ver  = 4;
                mseq = get_u64le(hdr + 40);
            } else if (rhsz != hsz && rhsz == (int)TSDB_IDX_HEADER_SIZE_V3) {
                ver  = 3;
                mseq = 0;
            }
            hsz = rhsz;
        }
    }
    fclose(f);

    if (hsz <= 0) return hsz;
    if (out_version)     *out_version     = ver;
    if (out_count)       *out_count       = cnt;
    if (out_entry_size)  *out_entry_size  = esz;
    if (out_total_rows)  *out_total_rows  = tot;
    if (out_file_ts_min) *out_file_ts_min = fmn;
    if (out_file_ts_max) *out_file_ts_max = fmx;
    if (out_max_seq)     *out_max_seq     = mseq;
    return hsz;
}


/*
 * BlockIndexEntry layout.
 *
 * Bytes 0..39 are stable across v1/v2/v3:
 *   [0..7]    offset    u64
 *   [8..11]   size      u32
 *   [12..15]  count     u32
 *   [16..23]  ts_min    i64
 *   [24..31]  ts_max    i64
 *   [32..39]  _reserved u64   (used as 64-bit bloom for SYMBOL columns)
 *
 * V3 entries append a 48-byte column-stats payload at bytes 40..87:
 *   [40..47]  stats_min    i64 bits  (reinterpret as double for FLOAT64)
 *   [48..55]  stats_max    i64 bits
 *   [56..63]  stats_sum    i64 bits
 *   [64..71]  stats_first  i64 bits  (block is ts-ordered on flush)
 *   [72..79]  stats_last   i64 bits
 *   [80..81]  stats_flags  u16       (see TSDB_STATS_HAS_* in part.h)
 *   [82..85]  ordinal      u32       partition-local block ordinal
 *   [86..87]  ord_mark     u16       TSDB_IDX_ORD_MARK, or 0 == "unknown"
 *
 * The stats payload is written unconditionally at V3 so the layout is
 * uniform; for SYMBOL columns stats_flags == 0 and the fields are 0 —
 * those columns are served from the existing bloom filter instead.
 *
 * [82..87] were reserved and written as zero by every previous writer, which is
 * exactly the "ordinal unknown" encoding — so recording the ordinal there needs
 * no entry-size change and no version bump.  See part.h for why the ordinal has
 * to be durable rather than inferred from position.
 */
static size_t write_idx_entry(uint8_t *buf,
                              uint64_t offset, uint32_t size, uint32_t count,
                              int64_t ts_min, int64_t ts_max,
                              uint64_t bloom,
                              const tsdb_block_meta_t *stats,
                              tsdb_block_ord_t ord)
{
    put_u64le(buf + 0,  offset);
    put_u32le(buf + 8,  size);
    put_u32le(buf + 12, count);
    put_i64le(buf + 16, ts_min);
    put_i64le(buf + 24, ts_max);
    put_u64le(buf + 32, bloom);

    /* Stats payload.  A NULL `stats` means "no stats" — e.g. a legacy
     * V1/V2 entry being widened on merge.  We still zero the payload
     * so readers see a consistent 88-byte record. */
    memset(buf + 40, 0, TSDB_IDX_STATS_BYTES);
    if (stats) {
        put_i64le(buf + 40, stats->stats_min);
        put_i64le(buf + 48, stats->stats_max);
        put_i64le(buf + 56, stats->stats_sum);
        put_i64le(buf + 64, stats->stats_first);
        put_i64le(buf + 72, stats->stats_last);
        put_u16le(buf + 80, stats->stats_flags);
    }
    if (TSDB_ORD_KNOWN(ord)) {
        put_u32le(buf + 82, ord.v);
        put_u16le(buf + 86, ord.mark);
    }
    return TSDB_IDX_ENTRY_SIZE;
}

/* Read the ordinal back out of an on-disk entry.  `esz` is the file's entry
 * size: a V1/V2 40-byte entry has no room for one, so it is UNKNOWN. */
static tsdb_block_ord_t read_idx_entry_ord(const uint8_t *entry, uint32_t esz)
{
    tsdb_block_ord_t o = { 0, 0 };
    if (esz < TSDB_IDX_ENTRY_SIZE) return o;
    uint16_t mark = get_u16le(entry + 86);
    if (mark != TSDB_IDX_ORD_MARK) return o;
    o.v    = get_u32le(entry + 82);
    o.mark = mark;
    return o;
}

/* The ordinal to use for an entry at physical position `pos`.
 *
 * LEGACY RULE (part.h): an entry with no marker gets its PHYSICAL position.
 * That is sound only where the arrays are equal length and agree
 * position-by-position, which every caller of this helper checks separately —
 * it never stands in for that check. */
static uint32_t idx_eff_ord(tsdb_block_ord_t o, uint32_t pos)
{
    return TSDB_ORD_KNOWN(o) ? o.v : pos;
}

/* The NEXT FREE ordinal of ONE column index, i.e. max(effective ordinal) + 1,
 * floored at the entry count.  0 when the index is absent.
 *
 * It is deliberately NOT the entry count.  A partition's ordinal space has to
 * stay monotone for the partition's whole LIFETIME: compaction re-cuts the
 * local blocks into fewer, larger ones, so seeding the next flush from the
 * local block count would re-issue numbers this partition has already bound to
 * other rows.  Monotonicity is a LOCAL requirement — see the note above
 * tsdb_part_ord_translate for why it is only ever a local one.
 *
 * The entry-count floor is the legacy rule idx_eff_ord() encodes: an unmarked
 * entry's ordinal IS its position, so the first free ordinal after N unmarked
 * entries is N. */
static uint32_t part_idx_next_ord(const char *idx_path)
{
    uint32_t cnt = 0, esz = 0;
    int hsz = tsdb_part_idx_probe(idx_path, NULL, &cnt, &esz,
                                  NULL, NULL, NULL, NULL);
    if (hsz <= 0 || cnt == 0 || esz == 0) return 0;

    uint32_t next = cnt;                  /* legacy floor: position == ordinal */
    if (esz >= TSDB_IDX_ENTRY_SIZE) {
        FILE *f = fopen(idx_path, "rb");
        if (f) {
            uint8_t e[TSDB_IDX_ENTRY_SIZE];
            for (uint32_t i = 0; i < cnt; i++) {
                if (fseek(f, (long)((uint64_t)hsz + (uint64_t)i * esz),
                          SEEK_SET) != 0 ||
                    fread(e, 1, sizeof(e), f) != sizeof(e)) break;
                tsdb_block_ord_t o = read_idx_entry_ord(e, TSDB_IDX_ENTRY_SIZE);
                if (TSDB_ORD_KNOWN(o) && o.v + 1u > next) next = o.v + 1u;
            }
            fclose(f);
        }
    }
    return next;
}

/* ---- Cross-node ordinal translation (<part>/.ordmap) ----------------------
 *
 * THE ORDINAL IS A PARTITION-LOCAL IDENTITY, ISSUED BY ONE NODE.  It answers
 * exactly one question — "which ts block group does this column block belong
 * to, HERE" — and it answers it only inside the partition that issued it.  For
 * that, all it must satisfy is: every column of one block group carries the
 * same value, and no two distinct groups in one partition collide.  Absolute
 * values need not agree across nodes at all.
 *
 * A block arriving over the wire carries the SENDER's number.  Believing it
 * turns a node-local counter into a cluster-wide one, and nothing keeps the two
 * spaces apart: every node runs its own compactor unconditionally, compaction
 * is node-local and is never replicated, and the receiver's own flush allocates
 * out of the same range.  Both directions have been measured to lose data —
 * renumbering DOWN re-issues numbers a replica still holds, renumbering UP
 * consumes the ones the primary is about to hand out — and any further variation
 * on "make compaction number differently" fails the same way, because the
 * number is still being asked a question it cannot answer.
 *
 * So the receiver TRANSLATES.  Each incoming block-group ordinal is mapped,
 * ONCE, to a free ordinal in the RECEIVER's own space, and that mapping is
 * durable and shared by every column of the group — the same invariant
 * `part_ord_base` maintains for a local flush.  The sender's numbers never
 * enter the receiver's space, so a replica-side compaction cannot alias
 * anything the primary is about to issue, and vice versa.
 *
 * The group key is (ISSUER, sender ordinal, ts_min, ts_max, count).  Every
 * column of one flush block carries the last four identically (the flush derives
 * one block's ts_min/ts_max from the same rows for every column), and the
 * sender's ordinal is what separates the two blocks of a duplicate-timestamp run
 * that agree on the other three.
 *
 * THE ISSUER IS NOT OPTIONAL, and leaving it out was silent destruction of acked
 * rows.  An ordinal names a group inside the partition that ISSUED it, so
 * "ordinal 0" is a different group on every node — and two nodes flushing the
 * same timestamp grid into the same table+day both start at 0 with byte-equal ts
 * blocks, which is what any synchronised ingest (a metric scrape, a sharded
 * loader) produces by construction.  Without an issuer the two groups collapse
 * onto ONE local ordinal: the second sender's ts block matches at that ordinal,
 * is declared a re-delivery and is dropped with rc == TSDB_OK — ACKed and
 * thrown away — while its value block, whose bytes differ, lands as a second
 * claimant of the same ordinal and the surviving ts block is then answered with
 * the WRONG sender's values.  Scoping the key to the issuer is what keeps two
 * senders' spaces apart, exactly as translation keeps sender and receiver apart.
 *
 *   header 16 B: magic u32 | version u16 | entry_size u16 | next_local u32 | pad
 *   entry  40 B: issuer u64 | remote u32 | local u32 | count u32 | pad u32
 *                | ts_min i64 | ts_max i64
 *
 * `next_local` is a HIGH-WATER MARK and is never lowered: an allocation that is
 * recorded and then lost to a crash before its block lands must not be handed
 * out a second time, or the retry lands on top of another group's rows.  The
 * record is therefore written BEFORE the block; a mapping with no block behind
 * it costs one skipped ordinal and nothing else.
 *
 * An absent file means "this partition has never received a remote block",
 * which is every partition of every single-node database, so nothing is
 * created until the first push arrives.
 */
#define ORDMAP_NAME     ".ordmap"
#define ORDMAP_MAGIC    0x4D44524Fu     /* 'O','R','D','M', little-endian */
#define ORDMAP_VERSION  2u              /* v1 had no issuer — see above */
#define ORDMAP_HDR_SZ   16u
#define ORDMAP_ENT_SZ   40u

static void ordmap_path(char *dst, size_t cap, const char *part_dir) {
    snprintf(dst, cap, "%s/%s", part_dir, ORDMAP_NAME);
}

static int ordmap_hdr_ok(const uint8_t *h) {
    return get_u32le(h) == ORDMAP_MAGIC &&
           get_u16le(h + 4) == (uint16_t)ORDMAP_VERSION &&
           get_u16le(h + 6) == (uint16_t)ORDMAP_ENT_SZ;
}

/* The map's high-water, or 0 when there is no readable map here. */
static uint32_t ordmap_next_local(const char *part_dir)
{
    char p[4200];
    ordmap_path(p, sizeof(p), part_dir);
    FILE *f = fopen(p, "rb");
    if (!f) return 0;
    uint8_t h[ORDMAP_HDR_SZ];
    uint32_t nx = 0;
    if (fread(h, 1, sizeof(h), f) == sizeof(h) && ordmap_hdr_ok(h))
        nx = get_u32le(h + 8);
    fclose(f);
    return nx;
}

uint32_t tsdb_part_next_ordinal(const tsdb_schema_t *s, const char *part_dir)
{
    if (!s || !part_dir) return 0;

    /* Over EVERY column, not just ts.  A non-ts column can legitimately sit
     * above ts's highest ordinal: a flush that published its value columns and
     * died before ts leaves exactly that, and so does a received group whose ts
     * block the commit test is still holding back. */
    uint32_t next = ordmap_next_local(part_dir);
    for (int ci = 0; ci < s->ncols; ci++) {
        char p[4200];
        snprintf(p, sizeof(p), "%s/%s.idx", part_dir, s->cols[ci].name);
        uint32_t n = part_idx_next_ord(p);
        if (n > next) next = n;
    }
    return next;
}

void tsdb_part_ord_reset(const char *part_dir)
{
    if (!part_dir) return;
    char p[4200];
    ordmap_path(p, sizeof(p), part_dir);
    (void)unlink(p);
}

int tsdb_part_ord_translate(const tsdb_schema_t *s, const char *part_dir,
                            uint64_t issuer, tsdb_block_ord_t remote,
                            int64_t ts_min, int64_t ts_max, uint32_t count,
                            tsdb_block_ord_t *out_local)
{
    if (!s || !part_dir || !out_local) return TSDB_ERR_INVAL;

    tsdb_block_ord_t none = { 0, 0 };
    *out_local = none;
    /* A sender that predates the field has no ordinal to translate.  Nothing is
     * recorded and the caller keeps its historical content-only behaviour. */
    if (!TSDB_ORD_KNOWN(remote)) return TSDB_OK;

    char p[4200];
    ordmap_path(p, sizeof(p), part_dir);

    uint32_t hw = 0;
    int      have_map = 0;
    off_t    aligned_end = (off_t)ORDMAP_HDR_SZ;

    FILE *f = fopen(p, "rb");
    if (f) {
        uint8_t h[ORDMAP_HDR_SZ];
        if (fread(h, 1, sizeof(h), f) == sizeof(h) && ordmap_hdr_ok(h)) {
            have_map = 1;
            hw = get_u32le(h + 8);
            uint8_t e[ORDMAP_ENT_SZ];
            while (fread(e, 1, sizeof(e), f) == sizeof(e)) {
                aligned_end += (off_t)ORDMAP_ENT_SZ;
                if (get_u64le(e + 0)  != issuer)   continue;
                if (get_u32le(e + 8)  != remote.v) continue;
                if (get_u32le(e + 16) != count)    continue;
                if (get_i64le(e + 24) != ts_min)   continue;
                if (get_i64le(e + 32) != ts_max)   continue;
                out_local->v    = get_u32le(e + 12);
                out_local->mark = TSDB_IDX_ORD_MARK;
                fclose(f);
                return TSDB_OK;
            }
        }
        long flen = (fseek(f, 0, SEEK_END) == 0) ? ftell(f) : -1;
        fclose(f);
        if (!have_map) {
            /* SHORTER THAN A HEADER IS ABSENT, NOT CORRUPT.  A file with fewer
             * bytes than the header carries no mapping at all, so it is
             * indistinguishable from no file, and refusing buys nothing: there
             * is nothing to translate against either way.  It matters because
             * the create path used to publish a 0-byte file the instant it
             * opened, so an ENOSPC or a process death on the FIRST remote push
             * into a partition left one behind — and the refusal below is NOT
             * retryable, it is sticky for the life of the partition.  The
             * create path is atomic now (see below) so this state should no
             * longer arise, but a map written by a build that predates that fix
             * still has to be recoverable.
             *
             * A FULL HEADER THAT IS NOT OURS is different and still refuses:
             * that is a foreign file or a version this build cannot read, and
             * translating against a map we cannot interpret would cost values.
             * `tsdb_part_ord_reset` is the operator escape, and the anti-entropy
             * backfill already calls it. */
            if (flen >= 0 && flen < (long)ORDMAP_HDR_SZ) {
                (void)unlink(p);
            } else {
                fprintf(stderr,
                        "[part] %s: %s carries a header this build cannot read; "
                        "refusing to translate a remote block ordinal against "
                        "it.  This does NOT clear itself — reset the map "
                        "(tsdb_part_ord_reset) or let anti-entropy rebuild the "
                        "partition\n", part_dir, ORDMAP_NAME);
                return TSDB_ERR_CORRUPT;
            }
        }
    }

    /* A group this node has not seen.  Allocate out of OUR space. */
    uint32_t local = tsdb_part_next_ordinal(s, part_dir);
    if (local < hw) local = hw;

    uint8_t h[ORDMAP_HDR_SZ];
    memset(h, 0, sizeof(h));
    put_u32le(h + 0, ORDMAP_MAGIC);
    put_u16le(h + 4, (uint16_t)ORDMAP_VERSION);
    put_u16le(h + 6, (uint16_t)ORDMAP_ENT_SZ);
    put_u32le(h + 8, local + 1u);

    uint8_t e[ORDMAP_ENT_SZ];
    memset(e, 0, sizeof(e));
    put_u64le(e + 0,  issuer);
    put_u32le(e + 8,  remote.v);
    put_u32le(e + 12, local);
    put_u32le(e + 16, count);
    put_i64le(e + 24, ts_min);
    put_i64le(e + 32, ts_max);

    /* THE MAP IS DURABLE STATE, SO IT IS PUBLISHED LIKE DURABLE STATE.  Two
     * different disciplines, because the two paths fail differently and a
     * whole-file rewrite is not affordable on the hot one.
     *
     * CREATE: temp + rename, the pattern 5d75238 established for the idx
     * header.  The old code's `fopen(p, "w+b")` published a 0-byte file the
     * instant it opened, and every failure path left it there — so an ENOSPC or
     * a process death on the first remote push into a partition permanently
     * refused all later replication into it.  Now a crash leaves either no map
     * or a complete one.  The image is one header plus one entry, 56 bytes.
     *
     * APPEND: header FIRST, then the entry.  Rewriting the whole file would be
     * O(entries^2) over a partition's life — one entry per received block
     * group, so thousands on a busy day — and it is not needed, because the two
     * orders fail in opposite directions.  Entry-then-header (what this used to
     * do) can leave an entry the high-water does not cover, and a LATER group
     * then gets the SAME local ordinal; on a duplicate-timestamp run the two
     * groups' ts blocks are byte-identical, the applier calls the second a
     * re-delivery, and it is dropped at rc == TSDB_OK.  Silent loss of acked
     * rows.  Header-then-entry can only leave a high-water ahead of the entries
     * — the group is simply re-allocated the next ordinal on retry, costing one
     * skipped number and nothing else.  Too-high never aliases; too-low does.
     *
     * The header is 16 bytes at offset 0, inside sector 0, so it lands whole or
     * not at all — the same assumption the idx header publish already documents
     * and relies on. */
    int ok = 1;
    if (have_map) {
        FILE *w = fopen(p, "r+b");
        if (!w) return TSDB_ERR_IO;
        /* Drop any short tail first, so the entry lands on the record grid. */
        if (ftruncate(fileno(w), aligned_end) != 0) ok = 0;
        if (ok && (fseek(w, 0, SEEK_SET) != 0 ||
                   fwrite(h, 1, sizeof(h), w) != sizeof(h))) ok = 0;
        if (ok && fflush(w) != 0) ok = 0;
        if (ok && tsdb_part_fsync_fd(fileno(w)) != 0) ok = 0;   /* [1] high-water */
        /* TEST-ONLY: die exactly between the two writes, which is the whole
         * point of their order.  Production always falls straight through. */
        if (ok && getenv("TSDB_TEST_CRASH_ORDMAP_MID")) { fflush(NULL); _exit(71); }
        if (ok && fseek(w, (long)aligned_end, SEEK_SET) != 0) ok = 0;
        if (ok && fwrite(e, 1, sizeof(e), w) != sizeof(e)) ok = 0;
        if (ok && fflush(w) != 0) ok = 0;
        if (ok && ferror(w))      ok = 0;
        if (ok && tsdb_part_fsync_fd(fileno(w)) != 0) ok = 0;   /* [2] the entry */
        if (fclose(w) != 0) ok = 0;
        if (!ok) return TSDB_ERR_IO;
    } else {
        char tmp[4300];
        snprintf(tmp, sizeof(tmp), "%s.tmp", p);
        FILE *w = fopen(tmp, "wb");
        if (!w) return TSDB_ERR_IO;
        if (fwrite(h, 1, sizeof(h), w) != sizeof(h)) ok = 0;
        if (ok && fwrite(e, 1, sizeof(e), w) != sizeof(e)) ok = 0;
        if (ok && fflush(w) != 0) ok = 0;
        if (ok && ferror(w))      ok = 0;
        if (ok && tsdb_part_fsync_fd(fileno(w)) != 0) ok = 0;
        if (fclose(w) != 0) ok = 0;
        if (ok && rename(tmp, p) != 0) ok = 0;
        if (!ok) { (void)unlink(tmp); return TSDB_ERR_IO; }
        part_fsync_dir(part_dir);
    }

    out_local->v    = local;
    out_local->mark = TSDB_IDX_ORD_MARK;
    return TSDB_OK;
}

/* ---- Per-partition, per-column writer ----------------------------------- */

/*
 * Maximum compressed output per block (2× raw + overhead).
 */
#define MAX_COMPRESSED_BLOCK (TSDB_BLOCK_POINTS * 8 * 2 + 256)

typedef struct {
    FILE    *col_fp;
    uint64_t col_offset;
    uint64_t col_start_offset;  /* .col EOF at open — rollback point on a
                                   failed flush (e.g. ENOSPC) so a partial
                                   append leaves no orphan block behind */
    uint32_t block_count;
    uint64_t total_rows;

    /* File-level zone map accumulated during this flush session.
     * Seeded from existing v2 idx header if present, else from
     * per-block metadata in the existing v1 idx. */
    int64_t  file_ts_min;
    int64_t  file_ts_max;
    int      has_zone;      /* 1 if file_ts_min/max are initialized */

    uint8_t *idx_entries;  /* accumulated new entries for this flush session */
    size_t   idx_cap;
    size_t   idx_n;

    /* Durable WAL redo checkpoint to stamp into this column's idx header on
     * close.  Seeded from the existing idx (so a re-flush never lowers it),
     * then raised to the flush's hwm by tsdb_part_flush_ex2.  0 = leave as-is
     * (default/non-redo flush path). */
    uint64_t max_seq;

    /* Column-count stamp for this column's idx header (part.h).  The flush is
     * the ONLY writer allowed to assert it, because it is the only one that
     * writes every schema column into every partition it touches; it sets this
     * to s->ncols right after col_writer_open.  Left 0 (unknown) by the memset
     * in col_writer_open, so any other user of col_writer_t stays unasserting. */
    uint16_t ncols;

    /* Partition-local ordinal of the FIRST block this session writes.  It is
     * the ts column's pre-flush block count for this partition — the SHARED
     * number, not this column's own physical entry count.  The two differ
     * exactly when the column is short or ALTER-added, which is the case the
     * ordinal exists for: an ALTER-added column's first block belongs to ts
     * block N, not to ts block 0.  Set by tsdb_part_flush_ex2. */
    uint32_t part_ord_base;

    char     idx_path[4096];
    char     col_path[4096];   /* for rollback-by-path on a failed close */
    char     part_dir[4096];   /* stripe key for the idx publish lock */

    /* Set when the raw-block hook reported a failure for THIS column.  The
     * local write still proceeds (a peer outage must never fail a local
     * commit), but the caller uses it to suppress this partition's ts pushes
     * so no peer is handed a visibility marker for a group it did not
     * fully receive. */
    int      raw_failed;

    /* Raw-block hook (optional, set by tsdb_part_flush_ex). */
    part_raw_block_fn_t  raw_block_fn;
    void                *raw_block_ud;
    struct tsdb_db      *raw_block_db;
    const char          *raw_block_table;
    uint32_t             raw_block_day;   /* YYYYMMDD or YYYYMMDDHH packed */
    int                  col_idx;
} col_writer_t;

static int col_writer_open(col_writer_t *w, const char *part_dir,
                           const char *col_name)
{
    memset(w, 0, sizeof(*w));

    snprintf(w->col_path,  sizeof(w->col_path),  "%s/%s.col", part_dir, col_name);
    snprintf(w->idx_path,  sizeof(w->idx_path),  "%s/%s.idx", part_dir, col_name);
    snprintf(w->part_dir,  sizeof(w->part_dir),  "%s", part_dir);

    /* Open .col for appending. */
    w->col_fp = fopen(w->col_path, "ab");
    if (!w->col_fp) return TSDB_ERR_IO;

    /* On HDD, coalesce write syscalls with a larger stdio buffer.  Each
     * block-header+payload is typically 10–100 KiB; a 256 KiB buffer
     * flushes 2–25 blocks per write(), amortising seek + metadata cost.
     * On SSD, the default 4–8 KiB buffer is fine. */
    size_t wbuf = tsdb_iopolicy_write_buf_bytes(
        tsdb_iopolicy_detect(part_dir));
    if (wbuf > 0) {
        /* setvbuf may fail silently if called after I/O; we've only just
         * opened, so this is the legal moment.  Failure is non-fatal —
         * the stream stays on its default buffering. */
        (void)setvbuf(w->col_fp, NULL, _IOFBF, wbuf);
    }

    fseek(w->col_fp, 0, SEEK_END);
    w->col_offset       = (uint64_t)ftell(w->col_fp);
    w->col_start_offset = w->col_offset;   /* rollback point on failure */

    /* Read existing idx header to prime block_count, total_rows, and the
     * file-level zone map (v2 only).
     *
     * THESE ARE PRIMED ONCE, HERE, AND THE PUBLISH IN col_writer_close REWRITES
     * THE WHOLE HEADER FROM THEM — the full-rewrite path re-reads only max_seq,
     * so a failure to read here is not corrected later.  Priming from zeros
     * therefore publishes a header that under-declares total_rows AND narrows
     * the file zone map to this flush's own blocks; a zone that does not cover
     * a block prunes it out of every range query (the same invariant
     * col_idx_append_publish states where it merges the two).  So distinguish
     * "no index yet" from "could not read the index": only ENOENT is the
     * former.  Anything else fails the flush, which keeps the rows in the
     * memtable + WAL instead of publishing a header that hides them. */
    w->has_zone    = 0;
    w->file_ts_min = INT64_MAX;
    w->file_ts_max = INT64_MIN;
    {
        FILE *idx_r = fopen(w->idx_path, "rb");
        if (!idx_r && errno != ENOENT) {
            fprintf(stderr,
                    "[part] %s: existing index cannot be opened (errno=%d %s); "
                    "failing the flush rather than republishing it from zeros\n",
                    w->idx_path, errno, strerror(errno));
            fclose(w->col_fp);
            w->col_fp = NULL;
            return TSDB_ERR_IO;
        }
        if (idx_r) {
            /* HDD: prefetch whole idx file async via fadvise. */
            tsdb_iopolicy_advise_seq_fd(tsdb_iopolicy_detect(part_dir),
                                         fileno(idx_r));
            uint8_t hdr[TSDB_IDX_HEADER_SIZE];
            size_t n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_r);
            uint32_t cnt = 0;
            uint16_t ver = 0;
            uint64_t tot = 0;
            int64_t  fmn = INT64_MAX, fmx = INT64_MIN;
            uint32_t esz = 0;
            uint64_t mseq = 0;
            int hsz = read_idx_header_ex(hdr, n, &cnt, &ver, &tot,
                                          &fmn, &fmx, &esz, &mseq);
            if (hsz <= 0) {
                /* The index EXISTS but this binary cannot parse it (corrupt
                 * magic, or a version it does not know — a rolled-back binary
                 * beside a newer writer, or a future V5).  Same refusal the
                 * raw-block applier makes for the same state. */
                fprintf(stderr,
                        "[part] %s: existing index is unparseable (corrupt "
                        "magic or unknown version); failing the flush rather "
                        "than republishing it from zeros\n", w->idx_path);
                fclose(idx_r);
                fclose(w->col_fp);
                w->col_fp = NULL;
                return TSDB_ERR_CORRUPT;
            }
            if (esz > 0) {
                w->block_count = cnt;
                w->total_rows  = tot;
                w->max_seq     = mseq;  /* preserve prior checkpoint */
                if (ver >= 2) {
                    w->file_ts_min = fmn;
                    w->file_ts_max = fmx;
                    w->has_zone    = (cnt > 0);
                } else if (cnt > 0) {
                    /* v1 idx: derive zone from per-block entries.
                     * Entries are 40 bytes in v1. */
                    if (fseek(idx_r, (long)hsz, SEEK_SET) == 0) {
                        for (uint32_t b = 0; b < cnt; b++) {
                            uint8_t e[TSDB_IDX_ENTRY_SIZE];  /* oversized buf */
                            if (fread(e, 1, esz, idx_r) != esz) break;
                            int64_t mn = get_i64le(e + 16);
                            int64_t mx = get_i64le(e + 24);
                            if (mn < w->file_ts_min) w->file_ts_min = mn;
                            if (mx > w->file_ts_max) w->file_ts_max = mx;
                        }
                        w->has_zone = 1;
                    }
                }
            }
            fclose(idx_r);
        }
    }

    w->idx_cap = 16;
    w->idx_entries = malloc(w->idx_cap * TSDB_IDX_ENTRY_SIZE);
    if (!w->idx_entries) { fclose(w->col_fp); return TSDB_ERR_NOMEM; }
    return TSDB_OK;
}

/*
 * Compute per-column block statistics (min/max/sum/first/last).
 *
 * Block rows are ts-sorted at flush time (see tsdb_memtable_sorted_indices),
 * so `first` = raw_vals[0] and `last` = raw_vals[count-1] in ts order.
 *
 * Sum semantics:
 *   INT64     — saturating i64 add; if we detect overflow, clear the
 *               SUM bit so the reader falls back to scanning.
 *   FLOAT64   — double accumulator; if it goes non-finite we clear SUM.
 *   TIMESTAMP — MIN/MAX/FIRST/LAST only (sum of timestamps is nonsense).
 *   SYMBOL    — all stats cleared; bloom is the right tool there.
 */
static void compute_block_stats(tsdb_type_t type,
                                 const void *raw_vals, size_t count,
                                 tsdb_block_meta_t *out)
{
    memset(out, 0, sizeof(*out));
    if (count == 0 || type == TSDB_TYPE_SYMBOL) return;

    switch (type) {
        case TSDB_TYPE_TIMESTAMP:
        case TSDB_TYPE_INT64: {
            const int64_t *v = (const int64_t *)raw_vals;
            int64_t mn = v[0], mx = v[0];
            __int128 sum = v[0];
            int sum_ok = 1;
            for (size_t i = 1; i < count; i++) {
                int64_t x = v[i];
                if (x < mn) mn = x;
                if (x > mx) mx = x;
                if (sum_ok) {
                    sum += x;
                    if (sum >  ((__int128)INT64_MAX) ||
                        sum < -((__int128)INT64_MAX) - 1) {
                        sum_ok = 0;
                    }
                }
            }
            out->stats_min   = mn;
            out->stats_max   = mx;
            out->stats_sum   = sum_ok ? (int64_t)sum : 0;
            out->stats_first = v[0];
            out->stats_last  = v[count - 1];
            out->stats_flags = TSDB_STATS_HAS_MIN_MAX
                             | TSDB_STATS_HAS_FIRST_LAST
                             | (sum_ok && type == TSDB_TYPE_INT64
                                    ? TSDB_STATS_HAS_SUM : 0u);
            /* TIMESTAMP: SUM deliberately left off even when lossless. */
            break;
        }
        case TSDB_TYPE_FLOAT32:  /* doubles in memory (width 8) — same stats path */
        case TSDB_TYPE_FLOAT64: {
            const double *v = (const double *)raw_vals;
            double mn = v[0], mx = v[0], s = v[0];
            int sum_ok = (s == s && s < 1e308 && s > -1e308);
            int mnmx_ok = (mn == mn && mx == mx);
            for (size_t i = 1; i < count; i++) {
                double x = v[i];
                if (!(x == x)) { /* NaN poisons min/max */
                    mnmx_ok = 0;
                    continue;
                }
                if (x < mn) mn = x;
                if (x > mx) mx = x;
                if (sum_ok) {
                    s += x;
                    if (!(s == s) || s > 1e308 || s < -1e308) sum_ok = 0;
                }
            }
            /* Reinterpret doubles into the i64 slots bit-for-bit. */
            memcpy(&out->stats_min,   &mn, 8);
            memcpy(&out->stats_max,   &mx, 8);
            memcpy(&out->stats_sum,   &s,  8);
            memcpy(&out->stats_first, &v[0], 8);
            memcpy(&out->stats_last,  &v[count - 1], 8);
            out->stats_flags = (mnmx_ok ? TSDB_STATS_HAS_MIN_MAX : 0u)
                             | (sum_ok  ? TSDB_STATS_HAS_SUM     : 0u)
                             | TSDB_STATS_HAS_FIRST_LAST;
            break;
        }
        default:
            break;
    }
}

/*
 * Public wrapper over compute_block_stats — the ONE implementation of the V3
 * stats payload, shared by the flush path and by compaction.  Compaction used
 * to zero the payload, which silently disabled zone-map skipping on every
 * compacted (i.e. every aged) partition; it must stamp byte-identical stats to
 * a flush, so it calls this rather than growing a second copy that can drift.
 */
void tsdb_part_compute_block_stats(tsdb_type_t type, const void *raw_vals,
                                   size_t count, tsdb_block_meta_t *out)
{
    compute_block_stats(type, raw_vals, count, out);
}


static int col_writer_write_block(col_writer_t *w,
                                  tsdb_type_t type,
                                  const void *raw_vals, size_t count,
                                  int64_t ts_min, int64_t ts_max)
{
    uint8_t *comp_buf = malloc(MAX_COMPRESSED_BLOCK);
    if (!comp_buf) return TSDB_ERR_NOMEM;

    tsdb_codec_t codec_used = TSDB_CODEC_NONE;
    uint16_t     blk_flags  = 0;
    int comp_bytes = tsdb_codec_encode_adaptive(type, raw_vals, count,
                                                comp_buf, MAX_COMPRESSED_BLOCK,
                                                &codec_used, &blk_flags);
    if (comp_bytes < 0) { free(comp_buf); return TSDB_ERR_INTERNAL; }

    /* For SYMBOL columns: build a 64-bit Bloom filter over all codes in this
     * block. The filter lives in BlockIndexEntry._reserved (bytes 32-39). */
    uint64_t bloom = 0;
    if (type == TSDB_TYPE_SYMBOL) {
        const uint32_t *codes = (const uint32_t *)raw_vals;
        for (size_t k = 0; k < count; k++) {
            bloom = tsdb_bloom_add(bloom, codes[k]);
        }
        blk_flags |= TSDB_BF_HAS_BLOOM;
    }

    /* Mark block as carrying a trailing CRC32C — verified at read time
     * to detect bit-rot, partial writes, or out-of-band corruption.
     * Old blocks without the flag stay readable (legacy compat). */
    blk_flags |= TSDB_BLOCK_FLAG_HAS_CRC;

    uint8_t hdr[TSDB_BLOCK_HEADER_SIZE];
    write_block_header(hdr, (uint8_t)codec_used, blk_flags,
                       (uint32_t)count, ts_min, ts_max,
                       (uint32_t)comp_bytes);

    /* Pad the block start to an 8-byte boundary.  BlockHeader is 32 B, so
     * the payload then starts 8-aligned too — the precondition for the
     * zero-copy read path handing out direct int64/double pointers into
     * the mmap (tsdb_part_read_block_ref).  The pad lives in the gap
     * BETWEEN blocks: readers address blocks only through idx-entry
     * offsets, so mixed padded/unpadded files (older writers, appended
     * legacy tails) stay fully readable — an unaligned legacy block just
     * takes the copy path. */
    {
        uint32_t pad = (uint32_t)(-(int64_t)w->col_offset & 7);
        if (pad) {
            static const uint8_t zpad[8] = {0};
            if (fwrite(zpad, 1, pad, w->col_fp) != pad) {
                free(comp_buf); return TSDB_ERR_IO;
            }
            w->col_offset += pad;
        }
    }

    if (fwrite(hdr, 1, TSDB_BLOCK_HEADER_SIZE, w->col_fp) != TSDB_BLOCK_HEADER_SIZE) {
        free(comp_buf); return TSDB_ERR_IO;
    }
    if ((size_t)comp_bytes > 0 &&
        fwrite(comp_buf, 1, (size_t)comp_bytes, w->col_fp) != (size_t)comp_bytes) {
        free(comp_buf); return TSDB_ERR_IO;
    }

    /* Trailing CRC32C: header + compressed data, written little-endian.
     * Reader reconstructs the same range and rejects on mismatch. */
    uint32_t crc = tsdb_crc32c(hdr, TSDB_BLOCK_HEADER_SIZE);
    if ((size_t)comp_bytes > 0)
        crc = tsdb_crc32c_update(crc, comp_buf, (size_t)comp_bytes);
    uint8_t crc_le[TSDB_BLOCK_CRC_TRAILER_SIZE];
    put_u32le(crc_le, crc);
    if (fwrite(crc_le, 1, TSDB_BLOCK_CRC_TRAILER_SIZE, w->col_fp)
        != TSDB_BLOCK_CRC_TRAILER_SIZE) {
        free(comp_buf); return TSDB_ERR_IO;
    }

    /* Precompute the stats payload up-front so the raw-block hook + the
     * idx entry both see the same numbers. */
    tsdb_block_meta_t stats;
    compute_block_stats(type, raw_vals, count, &stats);

    /* This block's partition-local ordinal.  idx_n counts the blocks THIS
     * session has written, so base + idx_n is the shared ordinal every column
     * of the same rows stamps. */
    tsdb_block_ord_t ord;
    ord.v    = w->part_ord_base + (uint32_t)w->idx_n;
    ord.mark = TSDB_IDX_ORD_MARK;

    /* Raw-block replication hook: fire BEFORE freeing comp_buf. */
    if (w->raw_block_fn) {
        tsdb_block_meta_t meta = stats;        /* carry stats to peers */
        meta.ord    = ord;                     /* ... and the ordinal */
        meta.offset = w->col_offset;
        meta.size   = (uint32_t)comp_bytes;
        meta.count  = (uint32_t)count;
        meta.ts_min = ts_min;
        meta.ts_max = ts_max;
        meta.codec  = (uint8_t)codec_used;
        meta.flags  = blk_flags;
        int hrc = w->raw_block_fn(w->raw_block_ud, w->raw_block_db,
                                  w->raw_block_table, w->raw_block_day,
                                  (uint16_t)w->col_idx, &meta,
                                  comp_buf, (size_t)comp_bytes);
        /* The LOCAL write proceeds regardless — a peer outage must never fail
         * a local commit, which is what the old "errors are intentionally
         * ignored" comment was protecting.  What is NOT safe is throwing the
         * result away entirely: publishing ts to a peer that NAK'd or timed
         * out on THIS column's block hands it a visibility marker for a group
         * it never received, and ts is what count(*)/max(ts) — and therefore
         * anti-entropy — are answered from, so the loss is invisible.  Record
         * it; tsdb_part_flush_ex2 suppresses this partition's ts pushes. */
        if (hrc != TSDB_OK) w->raw_failed = 1;
    }

    free(comp_buf);

    /* Grow idx_entries buffer if needed. */
    if (w->idx_n >= w->idx_cap) {
        size_t nc = w->idx_cap * 2;
        uint8_t *nb = realloc(w->idx_entries, nc * TSDB_IDX_ENTRY_SIZE);
        if (!nb) return TSDB_ERR_NOMEM;
        w->idx_entries = nb;
        w->idx_cap = nc;
    }
    uint8_t *entry = w->idx_entries + w->idx_n * TSDB_IDX_ENTRY_SIZE;
    write_idx_entry(entry, w->col_offset, (uint32_t)comp_bytes,
                    (uint32_t)count, ts_min, ts_max, bloom, &stats, ord);
    w->idx_n++;
    w->col_offset  += TSDB_BLOCK_HEADER_SIZE
                      + (uint64_t)comp_bytes
                      + TSDB_BLOCK_CRC_TRAILER_SIZE;  /* trailer always present
                                                         for blocks written by
                                                         this version */
    w->total_rows  += count;
    w->block_count++;

    /* Extend file-level zone map. */
    if (!w->has_zone) {
        w->file_ts_min = ts_min;
        w->file_ts_max = ts_max;
        w->has_zone = 1;
    } else {
        if (ts_min < w->file_ts_min) w->file_ts_min = ts_min;
        if (ts_max > w->file_ts_max) w->file_ts_max = ts_max;
    }
    return TSDB_OK;
}

/*
 * Abort a column writer after a failed flush (e.g. ENOSPC mid-write).
 * Rolls the .col file back to the EOF it had at open and closes WITHOUT
 * publishing the idx, so the on-disk state is byte-identical to before this
 * flush started — no orphan/partial block is left behind.  Without this, a
 * partial append leaks dead bytes into the .col on every failed flush; under
 * sustained disk-full retries those orphans accumulate and consume the very
 * space that is already scarce.
 *
 * The .col was opened append-only ("ab"), so the appended bytes are this
 * flush's alone; truncating to col_start_offset removes exactly them.  The
 * idx is never touched, so a reader (which enumerates blocks via the ts
 * column's idx) sees the unchanged pre-flush picture.
 */
static void col_writer_abort(col_writer_t *w) {
    if (w->col_fp) {
        int fd = fileno(w->col_fp);
        /* Drop buffered (un-written) bytes, then truncate back to pre-flush
         * length.  Either step failing is non-fatal: the worst case is the
         * pre-existing orphan-block behaviour, never worse. */
        (void)fflush(w->col_fp);
        if (fd >= 0) (void)ftruncate(fd, (off_t)w->col_start_offset);
        fclose(w->col_fp);
        w->col_fp = NULL;
    }
    free(w->idx_entries);
    w->idx_entries = NULL;
}

/* Write the whole buffer at `off`.  Returns 1 on success, 0 if any write
 * failed or short-wrote — the case a full disk (ENOSPC) and an RLIMIT_FSIZE
 * cap (EFBIG) both take, and the one the caller has to roll back.
 * lseek+write rather than pwrite: pwrite sits behind __USE_UNIX98 in glibc's
 * unistd.h and this tree compiles -std=c11, where the deployment gcc would
 * reject it as an implicit declaration. */
static int part_write_all_at(int fd, const void *buf, size_t n, off_t off) {
    const uint8_t *p = (const uint8_t *)buf;
    if (lseek(fd, off, SEEK_SET) != off) return 0;
    while (n > 0) {
        ssize_t k = write(fd, p, n);
        if (k <= 0) {
            if (k < 0 && errno == EINTR) continue;
            return 0;
        }
        p += (size_t)k;
        n -= (size_t)k;
    }
    return 1;
}

/* TEST-ONLY crash points inside the append publish below, in the order the
 * publish reaches them.  IDX_FAIL_HEADER is not a kill — it tears the header
 * write and returns failure, so the ROLLBACK runs (test_enospc [6d]). */
#define IDX_CRASH_OFF        0
#define IDX_CRASH_PRE_EXTEND 1
#define IDX_CRASH_ENTRIES    2
#define IDX_CRASH_HEADER_MID 3
#define IDX_CRASH_HEADER     4
#define IDX_FAIL_HEADER      5

static int g_idx_crash_hits;   /* TEST-ONLY: only touched when the env is set */

/*
 * TEST-ONLY fault injection: TSDB_TEST_CRASH_IDX_APPEND=<step>[:<n>] kills
 * this process with _exit() the n-th time (default 1st) the append publish
 * reaches <step> — pre_extend | entries | header_mid | header — or, for
 * fail_header, tears the header write and takes the rollback instead.  The
 * point is that the states the publish can leave behind are produced by a REAL
 * process death and then read back by a fresh process, instead of being argued
 * from the code.  Deliberately NOT latched in a static: tests fork children
 * from a parent that has already run publishes of its own, and a latched "off"
 * would be inherited and silently disarm the injection.  Production cost is one
 * getenv per column per flush, the same as TSDB_TEST_CRASH_BEFORE_TS below.
 */
static int idx_append_crash_cfg(int *out_nth) {
    *out_nth = 1;
    const char *e = getenv("TSDB_TEST_CRASH_IDX_APPEND");
    if (!e) return IDX_CRASH_OFF;
    const char *c = strchr(e, ':');
    size_t len = c ? (size_t)(c - e) : strlen(e);
    if (c && c[1]) {
        int n = atoi(c + 1);
        *out_nth = (n < 1) ? 1 : n;
    }
    if (len == 10 && !strncmp(e, "pre_extend", 10)) return IDX_CRASH_PRE_EXTEND;
    if (len == 7  && !strncmp(e, "entries",     7)) return IDX_CRASH_ENTRIES;
    if (len == 10 && !strncmp(e, "header_mid", 10)) return IDX_CRASH_HEADER_MID;
    if (len == 6  && !strncmp(e, "header",      6)) return IDX_CRASH_HEADER;
    if (len == 11 && !strncmp(e, "fail_header",11)) return IDX_FAIL_HEADER;
    return IDX_CRASH_OFF;
}

static void idx_append_crash_point(int cfg, int nth, int step) {
    if (cfg != step) return;                 /* production: always returns here */
    if (++g_idx_crash_hits < nth) return;
    _exit(70 + step);
}

/* TEST-ONLY: 1 when this publish should tear its header write and roll back. */
static int idx_append_fail_header(int cfg, int nth) {
    if (cfg != IDX_FAIL_HEADER) return 0;    /* production: always returns here */
    return ++g_idx_crash_hits >= nth;
}

/*
 * Append-only idx publish — fast path for col_writer_close.
 * =========================================================
 *
 * The full-rewrite publish below re-reads every existing entry, writes them
 * ALL back into <idx>.tmp, fsyncs and renames.  Per flush that is O(blocks) of
 * index traffic to persist O(1) new blocks, so building a partition costs
 * O(blocks^2): at 8M rows (977 blocks x 4 columns) it moves 168 MB in and
 * 168 MB out, plus a rename and a directory fsync per column per flush.
 *
 * This path appends the new entries at the END of the entry array and then
 * republishes the FIXED-SIZE header in place.  That is the same publish-last
 * discipline the flush already uses for the ts column: the appended entries do
 * not exist until the header's `count` names them.  Two fsyncs, same as the
 * temp+rename path (idx + directory), but no rename, no directory fsync and no
 * O(blocks) read/write.
 *
 * WHAT A CRASH LEAVES BEHIND
 * --------------------------
 * Steps, in order, on the live file:
 *   [1] ftruncate to the FINAL length  hdr + (count+k)*88
 *   [2] write the k new entries into that region, fsync
 *   [3] republish the whole fixed-size header in ONE write; fsync
 *   [4] bump the partition DIR's mtime — the side effect temp+rename had for
 *       free, which two consumers key decisions on (see [4] in the body)
 *
 * Crash after [1], before or during [2]:
 *   the header still says `count`, so a reader reads exactly the `count`
 *   entries that were already durable and never looks past them — the new
 *   blocks are invisible, as if the flush had never run.  Their .col bytes are
 *   orphans, which is precisely what the temp+rename path leaves when it dies
 *   before the rename.  No acked row is lost: flush-on-commit has not returned
 *   yet, and under deferred flush the redo records are still live because
 *   max_seq is published in [3], never before the entries.
 *
 * Crash after [2], before [3]:
 *   indistinguishable from the above to a reader.  The entries are durable but
 *   unnamed.  They are not overwritten by the next flush — the file is still
 *   the length [1] set, hdr + (count+k)*88, so the exact-fit ELIGIBILITY gate
 *   below fails against the surviving header's `count` and this fast path
 *   DECLINES; the full rewrite then re-reads the `count` named entries, writes
 *   a fresh temp and renames, dropping the unnamed ones.  Healed either way,
 *   but by the rewrite, not by an overwrite in place.
 *
 * Crash after [3], before [4]:
 *   fully published and correct; only the partition dir's mtime is missing.
 *   See [4] in the body for who reads it and what a stale one costs.
 *
 * Crash during [3]:
 *   the header is one write of <= 48 bytes at offset 0, so the only durable
 *   states are the old header and the new one — see the next note, which is
 *   the whole reason it is one write.
 *
 * COUNT AND MAX_SEQ ARE ONE WRITE
 * ------------------------------
 * The header carries two fields that MUST move together:
 *   count    names the new entries, i.e. makes the new blocks visible;
 *   max_seq  is the durable WAL redo checkpoint tsdb_part_max_seq reads off
 *            ts.idx, i.e. retires the redo records for those same rows.
 * Neither half-state is safe, and both are SILENT:
 *   new count + old max_seq -> reopen replays records the partition already
 *            holds, the re-flush appends a SECOND copy of the same blocks, and
 *            every query counts those rows twice with TSDB_OK.  (Measured, with
 *            a real kill between a separate count write and a separate max_seq
 *            write: SELECT v = 600 for 500 durable rows, permanent across
 *            reopens.  test_idx_append_crash's "header_mid" kill is that kill.)
 *   old count + new max_seq -> replay is skipped for rows no entry names:
 *            silent loss of acked rows.
 * Writing them field-by-field makes both reachable — not only by a crash, but
 * by any writeback in the window (jbd2 commit, dirty_expire, memory pressure,
 * an unrelated sync) followed by a power loss.  Publishing the whole header in
 * a single write at offset 0 leaves exactly two durable states, which is the
 * atomicity temp+rename gave for free.  A process death cannot split it at all
 * (write() has returned or it has not); a power loss cannot either unless a
 * <= 48-byte overwrite inside sector 0 can tear, which is the same
 * single-sector assumption a filesystem makes about its own journal — and it is
 * strictly WEAKER than what the field-by-field version needed, since that
 * needed per-field atomicity AND an ordering guarantee across three writes.
 * This is the only place in the tree that overwrites a live header in place;
 * everything else appends (wal.c) or publishes by rename, so the assumption is
 * introduced here and stated here.  (Starting the write at offset 0 rather than
 * at the first mutable field is deliberate: bytes 0..3 are the magic, byte-
 * identical to what is already there — eligibility validated it — so the first
 * four bytes of the write cannot change the file even if a short write stops
 * inside them.)
 *
 * Fields other than those two are safe in either state on their own — a zone
 * map wider than the blocks it covers only fails to prune, and an over-stated
 * total_rows only over-states compaction sizing and anti-entropy — but they
 * ride along in the same write, so no partial-header state exists to reason
 * about.
 *
 * WHY [1] IS NOT OPTIONAL — the sub-entry torn tail
 * -------------------------------------------------
 * Without the pre-extension the entries would be a plain append and a crash
 * could leave a PARTIAL entry: length hdr + count*88 + t for 0 < t < 88.
 * idx_recover_header_size() reads exactly that shape as the V3/V4 header
 * "mongrel" it exists to repair, and for t == 8 the alternate candidate
 * (40 <-> 48) fits the length EXACTLY — so the reader relocates the whole
 * entry array by 8 bytes and every block decodes from a garbage offset.  That
 * is the failure mode that shredded 95% of rows when compaction read V4
 * indexes as V3.  A sub-entry torn tail and a real mongrel are
 * INDISTINGUISHABLE by file length, so the writer must never produce one.
 * Pre-extending makes the length hdr + (count+j)*88 in every reachable state,
 * and the alternate candidate can never fit that (it would need j*88 == +-8).
 * ftruncate sets the final i_size up front, so no later write can publish an
 * intermediate length either.
 *
 * ELIGIBILITY.  The fast path runs only when the append cannot change the
 * SHAPE of the file:
 *   - the idx exists and opens for update in place (no new file, no rename);
 *   - its entries are 88 bytes wide (V3/V4) and it declares at least one;
 *   - its length is an EXACT fit for the header size its VERSION implies —
 *     which excludes both a mongrel and a torn tail left by an older writer,
 *     handing them to the full rewrite that heals them permanently, and which
 *     is also what makes the rollback below byte-exact;
 *   - the header we are about to stamp is the same size as the one on disk
 *     (a V3 -> V4 upgrade moves every entry by 8 bytes: a rewrite).
 * Anything else falls through to the temp+rename path, unchanged.
 *
 * CONCURRENCY.  Callers hold tsdb_part_idx_lock, which is what serialises this
 * against the raw-block writer of the same file.  No other process writes a
 * data dir this engine owns.
 *
 * In-process readers are NOT uniformly serialised against the flush.  Three
 * call sites open a partition lock-free, without t->compact_mtx:
 * exec.c:9185 (the stats fast path), migrate.c:243 and migrate.c:729 (the
 * export/measure walks).  They read the header, then the entries it names.
 * Against this path that is the same race the temp+rename publish already had
 * — a reader can observe the pre-publish header or the post-publish one — with
 * one difference: temp+rename swapped an inode, so a reader holding the old fd
 * kept a consistent old file, whereas here the bytes move under it.  Reading
 * `count` from the old header and entries from the new file is still safe, as
 * [1]/[2] only ever write BEYOND hdr + count*88 and the ftruncate only grows;
 * the reverse (new `count`, old entries) cannot happen because [3] is ordered
 * after [2]'s fsync.  A concurrent full REWRITE is a different matter, but
 * that has always swapped the inode.
 *
 * Returns 1 when it owns the publish (*out_rc carries the result), 0 when the
 * caller must fall back to the full rewrite.
 */
static int col_idx_append_publish(col_writer_t *w, int *out_rc)
{
    *out_rc = TSDB_OK;

    int crash_nth = 1;
    int crash_at  = idx_append_crash_cfg(&crash_nth);

    int fd = open(w->idx_path, O_RDWR);
    if (fd < 0) return 0;                    /* absent / not writable in place */

    uint8_t     hdr[TSDB_IDX_HEADER_SIZE];
    struct stat st;
    ssize_t     hn = read(fd, hdr, TSDB_IDX_HEADER_SIZE);
    if (hn != (ssize_t)TSDB_IDX_HEADER_SIZE || fstat(fd, &st) != 0) {
        close(fd); return 0;
    }

    uint32_t cnt = 0, esz = 0;
    uint16_t ver = 0;
    uint64_t tot = 0, mseq = 0;
    int64_t  fmn = 0, fmx = 0;
    int hsz = read_idx_header_ex(hdr, (size_t)hn, &cnt, &ver, &tot,
                                 &fmn, &fmx, &esz, &mseq);
    if (hsz <= 0 || cnt == 0 || esz != TSDB_IDX_ENTRY_SIZE) { close(fd); return 0; }

    /* Exact fit at the VERSION-derived header size.  Deliberately NOT
     * idx_recover_header_size(): a file that needs recovering is a mongrel (or
     * carries a torn tail), and appending onto one would open a window where a
     * crash leaves a length that no longer matches EITHER candidate, taking the
     * recovery away.  Those go to the full rewrite, which rewrites them into a
     * single self-consistent format. */
    uint64_t body    = (uint64_t)cnt * TSDB_IDX_ENTRY_SIZE;
    uint64_t old_len = (uint64_t)st.st_size;
    if (old_len != (uint64_t)hsz + body) { close(fd); return 0; }

    /* Both were seeded at col_writer_open, before this lock was taken: never
     * lower the durable checkpoint, and never NARROW the file zone map (a zone
     * that does not cover a block prunes it out of every range query). */
    if (mseq > w->max_seq)     w->max_seq     = mseq;
    if (fmn  < w->file_ts_min) w->file_ts_min = fmn;
    if (fmx  > w->file_ts_max) w->file_ts_max = fmx;

    uint32_t new_count = cnt + (uint32_t)w->idx_n;
    uint8_t  nhdr[TSDB_IDX_HEADER_SIZE];
    /* Same encoder, same arguments as the full rewrite below — including
     * w->ncols, the column-count stamp at header bytes [10..11] (part.h).  This
     * publish REWRITES THE WHOLE HEADER, so the stamp has to be re-asserted
     * here or an in-place publish would erase it, and erasing it makes the
     * reader MORE permissive: part_col_absence_is_late_add() would fall back to
     * the shape rule alone and zero-fill a column whose write was actually
     * lost.  w->ncols is set by tsdb_part_flush_ex2 to s->ncols right after
     * col_writer_open, and the flush is the ONLY writer part.h allows to assert
     * the claim (it writes every schema column into every partition it
     * touches); col_writer_close has no other caller.  The header this path
     * stamps is therefore byte-identical to the one the full rewrite would
     * stamp for the same state — that equivalence is the whole correctness
     * argument, and it must include [10..11]. */
    size_t   nhsz = write_idx_header(nhdr, new_count, w->total_rows,
                                     w->has_zone ? w->file_ts_min : 0,
                                     w->has_zone ? w->file_ts_max : 0,
                                     w->max_seq, w->ncols);
    if ((int)nhsz != hsz) { close(fd); return 0; }   /* V3 <-> V4 resize */

    uint64_t new_len = (uint64_t)hsz + (uint64_t)new_count * TSDB_IDX_ENTRY_SIZE;
    size_t   nbytes  = w->idx_n * TSDB_IDX_ENTRY_SIZE;

    /* [1] Pre-extend to the final length.  Nothing is mutated yet if this
     *     fails (ENOSPC / EFBIG / an fs without ftruncate), so hand the publish
     *     back to the full rewrite rather than failing the flush here. */
    if (ftruncate(fd, (off_t)new_len) != 0) { close(fd); return 0; }
    idx_append_crash_point(crash_at, crash_nth, IDX_CRASH_PRE_EXTEND);

    /* [2] The entries, durable before anything names them. */
    if (!part_write_all_at(fd, w->idx_entries, nbytes, (off_t)(hsz + body)))
        goto rollback;
    if (tsdb_part_fsync_fd(fd) != 0) goto rollback;
    idx_append_crash_point(crash_at, crash_nth, IDX_CRASH_ENTRIES);

    /* [3] Publish the header — ONE write, the whole 40/48 bytes at offset 0.
     *     `count` (which names the new entries) and `max_seq` (which retires
     *     their redo records) are a PAIR: either durable state that carries one
     *     without the other is a wrong answer, and both are silent.  See the
     *     COUNT AND MAX_SEQ ARE ONE WRITE note above. */
    if (idx_append_fail_header(crash_at, crash_nth)) {    /* TEST-ONLY */
        (void)part_write_all_at(fd, nhdr, 8, 0);          /* tear it: new count */
        goto rollback;
    }
    if (!part_write_all_at(fd, nhdr, nhsz, 0)) goto rollback;
    idx_append_crash_point(crash_at, crash_nth, IDX_CRASH_HEADER_MID);
    if (tsdb_part_fsync_fd(fd) != 0) goto rollback;
    idx_append_crash_point(crash_at, crash_nth, IDX_CRASH_HEADER);

    /* [4] temp+rename bumped <part_dir>'s mtime as a side effect of creating
     *     and removing a dirent.  compaction.c table_max_part_mtime() (the
     *     compaction memo key) and db_cluster.c's anti-entropy COLD gate both
     *     depend on that.  An in-place publish changes no dirent, so bump it
     *     explicitly.  The rollback path bumps it too — see the note there.
     *     Best effort: the rows are already durable and named, so a failure
     *     here must not turn a completed publish into a reported failure. */
    (void)utimensat(AT_FDCWD, w->part_dir, NULL, 0);

    close(fd);
    return 1;

rollback:
    /* Eligibility required an exact fit, so every byte written before [3] lies
     * beyond the original EOF: restoring the old header and truncating back
     * leaves the file byte-identical — the guarantee test_enospc [6b] and [6d]
     * pin, [6d] by tearing this very write.
     *
     * Header first, then shorten — and ONLY shorten if the header really went
     * back.  The step that fails here can be the FSYNC, i.e. after the new
     * header (count AND max_seq) is fully written; truncating under a header
     * whose restore also failed then publishes a manifest naming entries the
     * file no longer holds, with the checkpoint already past them.  Measured on
     * that state built by hand: SELECT v = 400 for 500 acked rows, rc=TSDB_OK,
     * permanent.  Leaving the file long instead is harmless: whichever header
     * survives names entries that are all present, and the trailing bytes fail
     * the exact-fit gate above so the next publish takes the full rewrite,
     * which heals the length.
     *
     * The mtime IS bumped here too, matching [4].  A rolled-back publish leaves
     * the partition byte-identical, so "unmodified" is the tempting reading —
     * but temp+rename bumped the dir on its FAILURE path as well (it had already
     * created the <col>.idx.tmp dirent before it could fail, and unlinked it
     * after), and the anti-entropy COLD gate at db_cluster.c:1393 was tuned
     * against that.  Skipping the bump here means a partition whose publishes
     * keep failing — ENOSPC, EIO — ages past the 60 s gate and becomes eligible
     * for tsdb_cluster_backfill_partition_from_result(), which by its own log
     * line replaces local-unique rows with a fuller peer's copy.
     *
     * So the two readings differ only in what they cost when wrong: bumping
     * costs the compaction memo one redundant re-scan, not bumping costs rows.
     * And "hot" is the truthful signal anyway — a partition whose flush is
     * failing is under active write, which is exactly when a peer's copy must
     * not be allowed to overwrite it. */
    if (part_write_all_at(fd, hdr, (size_t)hsz, 0))
        (void)ftruncate(fd, (off_t)old_len);
    (void)tsdb_part_fsync_fd(fd);
    (void)utimensat(AT_FDCWD, w->part_dir, NULL, 0);
    close(fd);
    *out_rc = TSDB_ERR_IO;
    return 1;
}

static int col_writer_close(col_writer_t *w) {
    int rc = TSDB_OK;

    if (w->col_fp) {
        int col_fd = fileno(w->col_fp);
        if (fflush(w->col_fp) != 0) rc = TSDB_ERR_IO;
        /* Durability ordering (residual C): the idx published below carries the
         * max_seq checkpoint that gates WAL replay on reopen.  Its .col bytes
         * MUST reach the device BEFORE that checkpoint is durable, or a power
         * loss can leave the checkpoint claiming rows whose data never hit disk
         * -> recovery skips their WAL records -> silent loss of ACKED rows.  On
         * fsync failure, fall through to the rc!=OK rollback below (truncate the
         * .col back, leave the old idx). */
        else if (col_fd >= 0 && tsdb_part_fsync_fd(col_fd) != 0) rc = TSDB_ERR_IO;
        fclose(w->col_fp);
        w->col_fp = NULL;
    }

    if (w->idx_n > 0 && rc == TSDB_OK) {
        /* Serialise the whole read-modify-write against the OTHER writer of
         * this same file, tsdb_rawblock_apply_ex.  Both read N entries and
         * write N+k; unsynchronised, the later rename silently drops the
         * other's entries, and a dropped NON-ts entry under a surviving ts is
         * the multi-column hole.  Innermost lock — see part.h. */
        tsdb_part_idx_lock(w->part_dir);

        /* Append-only publish when the file's shape allows it (see the
         * contract on col_idx_append_publish); otherwise the full rewrite
         * below, which is also what creates the idx in the first place and
         * what heals a legacy / mongrel / torn-tail file. */
        int append_rc = TSDB_OK;
        if (col_idx_append_publish(w, &append_rc)) {
            rc = append_rc;
            goto idx_published;
        }

        /* Read all existing entries from old idx (if any).  Handles v1 /
         * v2 (40-byte entries) and v3 (88-byte entries) transparently —
         * we widen legacy entries to V3 on write-out so the resulting
         * file is single-format.
         *
         * THIS PUBLISH REWRITES THE WHOLE MANIFEST, so every entry it does not
         * read is an entry it DELETES.  Collapsing a failed read into
         * old_count == 0 renames a manifest holding only this flush's entries
         * over the N-entry index: the old .col bytes survive but nothing names
         * them, the caller then clears the memtable and checkpoints the WAL,
         * and the next compaction rewrites the column from the short manifest —
         * at which point the rows are gone from disk, RAM and WAL at once.  So
         * "there is no index" (ENOENT) and "the index could not be read in
         * full" are kept apart, and only the first may publish; the second
         * fails the flush and leaves the partition byte-intact, exactly as the
         * raw-block applier already refuses the same state. */
        uint8_t *old_entries_v3 = NULL;   /* widened to V3 layout */
        uint32_t old_count      = 0;
        int      old_idx_rc     = TSDB_OK;   /* != OK: unread, must not publish */

        {
            FILE *idx_r = fopen(w->idx_path, "rb");
            if (!idx_r && errno != ENOENT) old_idx_rc = TSDB_ERR_IO;
            if (idx_r) {
                /* HDD: prefetch the idx file into page cache async. */
                tsdb_iopolicy_advise_seq_fd(tsdb_iopolicy_detect(w->idx_path),
                                             fileno(idx_r));
                uint8_t hdr[TSDB_IDX_HEADER_SIZE];
                size_t n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, idx_r);
                uint32_t cnt = 0;
                uint16_t ver = 0;
                uint64_t tot = 0;
                int64_t  fmn = 0, fmx = 0;
                uint32_t esz = 0;
                uint64_t mseq = 0;
                int hsz = read_idx_header_ex(hdr, n, &cnt, &ver, &tot,
                                              &fmn, &fmx, &esz, &mseq);
                /* Never let a re-flush lower the durable checkpoint. */
                if (mseq > w->max_seq) w->max_seq = mseq;
                /* Mixed-writer recovery: re-derive the header size from the
                 * idx length so a V3/V4-mongrel's old entries are read from
                 * their real offset, not widened from garbage. */
                if (hsz > 0 && esz > 0 && cnt > 0) {
                    struct stat ist;
                    if (fstat(fileno(idx_r), &ist) == 0 && ist.st_size > 0)
                        hsz = idx_recover_header_size(hsz, esz, cnt,
                                                      (uint64_t)ist.st_size,
                                                      w->idx_path);
                }
                if (hsz <= 0) old_idx_rc = TSDB_ERR_CORRUPT;
                if (hsz > 0 && esz > 0) {
                    old_count = cnt;
                    if (old_count > 0) {
                        size_t old_raw_sz = (size_t)old_count * esz;
                        uint8_t *old_raw = malloc(old_raw_sz);
                        if (old_raw && fseek(idx_r, (long)hsz, SEEK_SET) == 0 &&
                            fread(old_raw, 1, old_raw_sz, idx_r) == old_raw_sz)
                        {
                            /* Always widen into fresh V3 buffer — even if
                             * esz is already 88, zeroing the stats tail
                             * for legacy entries never hurts. */
                            old_entries_v3 = calloc((size_t)old_count,
                                                    TSDB_IDX_ENTRY_SIZE);
                            if (old_entries_v3) {
                                size_t copy_prefix = (esz < TSDB_IDX_ENTRY_SIZE)
                                                     ? esz : TSDB_IDX_ENTRY_SIZE;
                                for (uint32_t i = 0; i < old_count; i++) {
                                    memcpy(old_entries_v3 + (size_t)i * TSDB_IDX_ENTRY_SIZE,
                                           old_raw + (size_t)i * esz,
                                           copy_prefix);
                                }
                                /* Zero-init stats tail already via calloc.
                                 * stats_flags == 0 signals "absent" — the
                                 * reader will skip the fast-path on these
                                 * legacy blocks, which is the safe answer. */
                            } else {
                                old_idx_rc = TSDB_ERR_NOMEM;
                            }
                        } else {
                            /* Declared N entries, the file does not hold them:
                             * a torn tail, or the read itself failed. */
                            old_idx_rc = old_raw ? TSDB_ERR_CORRUPT
                                                 : TSDB_ERR_NOMEM;
                        }
                        free(old_raw);
                    }
                }
                fclose(idx_r);
            }
        }

        if (old_idx_rc != TSDB_OK) {
            fprintf(stderr,
                    "[part] %s: existing index declares entries this flush "
                    "could not read (rc=%d); refusing to rewrite the manifest "
                    "without them\n", w->idx_path, old_idx_rc);
            free(old_entries_v3);
            rc = old_idx_rc;
            goto idx_published;   /* old idx untouched; .col rolls back below */
        }

        /* Atomic manifest publish via temp + rename.  Pre-fix this used
         * fopen(idx_path, "wb") which truncates the existing idx and
         * leaves a window where a concurrent reader sees a half-written
         * (or zero-length) idx — and even when the rewrite finishes,
         * since the executor scans columns one at a time, a reader can
         * observe N blocks in column ci=0's idx but only N-1 in ci=1's
         * idx if it caught the flush mid-loop.  Test gate
         * test_continuous_rw.c reproduces it as torn rows where the
         * last-written column reads as calloc-zero.
         *
         * Fix: write to <idx>.tmp, fflush, rename onto <idx>.  Per-
         * column idx is now atomic.  Cross-column atomicity is provided
         * by the caller (tsdb_part_flush_ex) ordering the ts column
         * last, since exec.c's scan_plan_push uses ts.idx as the
         * canonical block enumerator. */
        char tmp_path[4200];
        snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", w->idx_path);
        FILE *idx_w = fopen(tmp_path, "wb");
        if (!idx_w) {
            free(old_entries_v3);
            rc = TSDB_ERR_IO;
        } else {
            uint32_t total_count = old_count + (uint32_t)w->idx_n;
            int64_t  fmn = w->has_zone ? w->file_ts_min : 0;
            int64_t  fmx = w->has_zone ? w->file_ts_max : 0;
            uint8_t  hdr[TSDB_IDX_HEADER_SIZE];
            size_t   hdr_sz = write_idx_header(hdr, total_count, w->total_rows,
                                               fmn, fmx, w->max_seq, w->ncols);
            /* Every write MUST be checked.  This publish is a FULL REWRITE
             * (header + all OLD entries + this flush's new ones), so a short
             * write drops manifest entries for blocks that were ALREADY
             * durable — and the header still declares the full count, so the
             * partition reads back short (a non-ts column even routes through
             * the ALTER-add-column zero-fill sentinel and reads WRONG values).
             * The failure is observable ONLY here: after a short fwrite the
             * subsequent fflush, fsync, fclose and rename all report success,
             * so an unchecked chain publishes the truncated manifest and
             * returns TSDB_OK — which lets flush_and_clear_locked clear the
             * memtable and truncate the WAL, losing acked rows permanently.
             * ferror() is checked as well as the fwrite returns because a
             * short write inside fwrite can leave fflush returning 0. */
            int io_ok = (fwrite(hdr, 1, hdr_sz, idx_w) == hdr_sz);
            if (io_ok && old_count > 0 && old_entries_v3) {
                size_t on = (size_t)old_count * TSDB_IDX_ENTRY_SIZE;
                io_ok = (fwrite(old_entries_v3, 1, on, idx_w) == on);
            }
            if (io_ok) {
                size_t nn = w->idx_n * TSDB_IDX_ENTRY_SIZE;
                io_ok = (fwrite(w->idx_entries, 1, nn, idx_w) == nn);
            }
            if (io_ok && fflush(idx_w) != 0) io_ok = 0;
            if (io_ok && ferror(idx_w))      io_ok = 0;
            /* fsync the idx file before rename: POSIX guarantees rename
             * is atomic only after the bytes have hit the device.
             *
             * Phase 1B (kdb+/Kafka-style modernisation): route the fsync
             * through io_uring when available so future iters can batch
             * many idx fsyncs in one device-queue dispatch.  fsync_sync
             * is single submit+wait — identical observable semantics to
             * plain fsync(), but the wiring runs on the new io_async
             * substrate. */
            int idx_fsync_ok = io_ok &&
                               (tsdb_part_fsync_fd(fileno(idx_w)) == 0);
            if (fclose(idx_w) != 0) idx_fsync_ok = 0;
            /* A short idx write or a failed idx fsync must NOT publish a
             * truncated or never-durable checkpoint idx: keep the old idx
             * (the rc!=OK path below rolls the .col back), so recovery never
             * trusts a manifest whose entries or max_seq aren't on the
             * device. */
            if (!idx_fsync_ok) {
                unlink(tmp_path);
                rc = TSDB_ERR_IO;
            } else if (rename(tmp_path, w->idx_path) != 0) {
                unlink(tmp_path);
                rc = TSDB_ERR_IO;
            } else {
                /* POSIX gives the rename atomicity, not durability: without
                 * this a crash can lose it and resurrect the previous idx —
                 * silently rolling ts back, or (for a non-ts column) leaving
                 * it short against a ts that did survive. */
                part_fsync_dir(w->part_dir);
            }
            free(old_entries_v3);
        }
idx_published:
        tsdb_part_idx_unlock(w->part_dir);
    }

    free(w->idx_entries);
    w->idx_entries = NULL;

    /* Failed close (final col fflush or idx publish failed, e.g. ENOSPC):
     * roll the .col back to its pre-flush length.  col_fp is already closed,
     * so truncate by path.  This removes the blocks this flush appended whose
     * idx was never published, keeping the partition byte-identical to before
     * the flush (no orphan-block accumulation under disk-full retries). */
    if (rc != TSDB_OK)
        (void)truncate(w->col_path, (off_t)w->col_start_offset);

    return rc;
}

/* ---- tsdb_part_flush / tsdb_part_flush_ex --------------------------------- */

int tsdb_part_flush(tsdb_schema_t *s, tsdb_memtable_t *m) {
    return tsdb_part_flush_ex2(s, m, NULL, NULL, 0);
}

int tsdb_part_flush_ex(tsdb_schema_t *s, tsdb_memtable_t *m,
                       struct tsdb_db *db, const char *table_name)
{
    return tsdb_part_flush_ex2(s, m, db, table_name, 0);
}

int tsdb_part_flush_ex2(tsdb_schema_t *s, tsdb_memtable_t *m,
                        struct tsdb_db *db, const char *table_name,
                        uint64_t max_seq)
{
    if (!s || !m) return TSDB_ERR_INVAL;

    /* Retrieve raw-block hook (may be NULL for standalone mode). */
    part_raw_block_fn_t raw_fn = NULL;
    void *raw_ud = NULL;
    if (db) {
        void *fn_ptr = NULL;
        tsdb_db_get_raw_block_hook(db, &fn_ptr, &raw_ud);
        __builtin_memcpy(&raw_fn, &fn_ptr, sizeof(raw_fn));
    }

    size_t nrows = tsdb_memtable_rows(m);
    if (nrows == 0) return TSDB_OK;

    const int64_t *ts_buf = (const int64_t *)tsdb_memtable_col(m, s->ts_col_idx);
    if (!ts_buf) return TSDB_ERR_INTERNAL;

    /*
     * Collect unique day indices from the memtable.
     * With TSDB_BLOCK_POINTS=8192 max rows we only expect 1-2 days.
     * Dynamic array handles edge cases gracefully.
     */
    int64_t *days = NULL;
    int ndays = 0, days_cap = 4;
    days = malloc((size_t)days_cap * sizeof(int64_t));
    if (!days) return TSDB_ERR_NOMEM;

    const tsdb_partition_unit_t part_unit = s->partition_unit;

    for (size_t r = 0; r < nrows; r++) {
        int64_t bucket = ts_part_index(ts_buf[r], part_unit);
        int found = 0;
        for (int d = 0; d < ndays; d++) {
            if (days[d] == bucket) { found = 1; break; }
        }
        if (!found) {
            if (ndays >= days_cap) {
                days_cap *= 2;
                int64_t *nb = realloc(days, (size_t)days_cap * sizeof(int64_t));
                if (!nb) { free(days); return TSDB_ERR_NOMEM; }
                days = nb;
            }
            days[ndays++] = bucket;
        }
    }

    /* Pull the ts-sorted permutation from the memtable's skip-list once.
     * When inserts arrived in-order this is the identity permutation and
     * the call is O(n); for out-of-order inserts it's a level-0 traversal
     * that yields each row in ascending ts order.  Blocks emitted below
     * inherit that order even when writes were out of sequence, which
     * tightens per-block ts_min/ts_max zone maps.
     *
     * Buffer is sized to the memtable's row CAPACITY, not the `nrows`
     * snapshot above: tsdb_memtable_sorted_indices fills indices for the
     * row count it observes under its own lock, so a writer that is not
     * serialised against this flush can grow m->nrows in between and the
     * walk would run past an nrows-sized buffer (glibc heap corruption
     * under anti-entropy truncate + re-pull concurrent with live wire
     * writes).  block_points is the hard cap on m->nrows. */
    size_t sort_cap = (s->block_points > 0 &&
                       s->block_points <= TSDB_BLOCK_POINTS)
                        ? (size_t)s->block_points : (size_t)TSDB_BLOCK_POINTS;
    if (sort_cap < nrows) sort_cap = nrows;
    size_t *sorted_all = malloc(sort_cap * sizeof(size_t));
    if (!sorted_all) { free(days); return TSDB_ERR_NOMEM; }
    if (tsdb_memtable_sorted_indices(m, sorted_all) != TSDB_OK) {
        for (size_t i = 0; i < nrows; i++) sorted_all[i] = i;
    }

    /* (d) Block-layout reorder: when sort_by_tag_col is set, re-permute
     * sorted_all so rows are tag-major, ts-minor.  Counting-sort by
     * SYMBOL dict code gives O(n + S) and is inherently stable — within
     * each tag group, rows preserve their incoming ts-ascending order
     * (because we iterate sorted_all in that order and append to each
     * bucket in arrival order).  The downstream day-bucket extraction
     * loop carries this ordering forward into every emitted block.
     *
     * Why tag-major helps Gorilla / Chimp: TSBS row order interleaves
     * different hosts' uncorrelated random walks at every timestamp, so
     * adjacent floats in a column buffer XOR to high-entropy values and
     * leading-zero matchers underperform.  Tag-sort makes each host's
     * run contiguous, so adjacent floats are correlated → 1.5-2 B/value
     * instead of 7.3.
     *
     * Block-level ts_min/ts_max remains tight (TSBS hosts ingest at the
     * same ts ranges); only within-block ts ordering changes.  Scanners
     * predicate per-row after zone-map cull, no monotonicity assumption.
     */
    if (s->sort_by_tag_col >= 0
        && s->sort_by_tag_col < s->ncols
        && s->cols[s->sort_by_tag_col].type == TSDB_TYPE_SYMBOL
        && nrows > 1) {
        const uint32_t *sym_buf =
            (const uint32_t *)tsdb_memtable_col(m, s->sort_by_tag_col);
        if (sym_buf) {
            uint32_t max_sym = 0;
            for (size_t k = 0; k < nrows; k++) {
                uint32_t sid = sym_buf[sorted_all[k]];
                if (sid > max_sym) max_sym = sid;
            }
            size_t nbuckets = (size_t)max_sym + 1;
            size_t *bucket_pos = calloc(nbuckets, sizeof(size_t));
            size_t *sorted_new = malloc(nrows * sizeof(size_t));
            if (bucket_pos && sorted_new) {
                /* counting pass */
                for (size_t k = 0; k < nrows; k++) {
                    bucket_pos[sym_buf[sorted_all[k]]]++;
                }
                /* exclusive prefix sum → bucket start offsets */
                size_t cum = 0;
                for (size_t i = 0; i < nbuckets; i++) {
                    size_t cnt = bucket_pos[i];
                    bucket_pos[i] = cum;
                    cum += cnt;
                }
                /* stable scatter */
                for (size_t k = 0; k < nrows; k++) {
                    size_t r = sorted_all[k];
                    uint32_t sid = sym_buf[r];
                    sorted_new[bucket_pos[sid]++] = r;
                }
                free(sorted_all);
                sorted_all = sorted_new;
                sorted_new = NULL;
            }
            free(bucket_pos);
            free(sorted_new);  /* NULL on success path; non-NULL only on alloc-fail half-state */
            /* Allocation failure: silently fall back to ts-sort.  No row
             * is lost; only the encoder benefit is forgone for this
             * flush.  An OOM here would already have nuked larger
             * allocations downstream. */
        }
    }

    int published_parts = 0;   /* TEST-ONLY: TSDB_TEST_CRASH_AFTER_PART counter */
    for (int d = 0; d < ndays; d++) {
        int64_t bucket = days[d];

        /* Collect row indices for this partition bucket, in ts order. */
        size_t *row_idx = malloc(nrows * sizeof(size_t));
        if (!row_idx) { free(sorted_all); free(days); return TSDB_ERR_NOMEM; }
        size_t day_nrows = 0;
        for (size_t k = 0; k < nrows; k++) {
            size_t r = sorted_all[k];
            if (ts_part_index(ts_buf[r], part_unit) == bucket) row_idx[day_nrows++] = r;
        }
        if (day_nrows == 0) { free(row_idx); continue; }

        /* Build partition directory name (DAY: 8 chars, HOUR: 10 chars). */
        char part_name[11];
        ts_to_part_str(ts_buf[row_idx[0]], part_unit, part_name);

        /* Pack partition identifier for the cluster raw-block hook. */
        uint32_t part_day_int = part_str_to_int(part_name, part_unit);

        char part_dir[4096];
        snprintf(part_dir, sizeof(part_dir), "%s/%s", s->dir, part_name);
        if (tsdb_mkdir_p(part_dir) < 0) { free(row_idx); free(days); free(sorted_all); return TSDB_ERR_IO; }

        /* Write each column.  ORDERING: process the timestamp column
         * LAST so it acts as the partition's atomic visibility marker.
         * Rationale: exec.c's scan_plan_push enumerates blocks via the
         * ts column's idx (see exec.c:377).  Once we publish a block to
         * ts.idx (via temp+rename in col_writer_close), the same block
         * has ALREADY been published to every non-ts column's idx.
         * A reader that observes block N in ts.idx is therefore
         * guaranteed to find a matching block in every other column,
         * with the right data behind it.  Without this reorder, the
         * reader could see ts.idx with N+1 blocks but col_v.idx still
         * at N, fall back to reading uninitialised file bytes for the
         * v column at the new block — surfaces as torn rows in
         * test_continuous_rw under default-mode commits. */
        int ts_ci = s->ts_col_idx;
        int ci_iter[TSDB_MAX_COLS];
        int ci_count = 0;
        for (int ci = 0; ci < s->ncols; ci++) {
            if (ci == ts_ci) continue;
            ci_iter[ci_count++] = ci;
        }
        ci_iter[ci_count++] = ts_ci;  /* ts last */

        /* The partition's SHARED ordinal base: its NEXT FREE ordinal.  Every
         * column of this flush stamps base+j on its j-th block, so an
         * ALTER-added column's first block records the ts block it actually
         * belongs to instead of 0.
         *
         * Using the block count here was a data bug, not a cosmetic one:
         * compaction re-cuts this partition into FEWER blocks, so the count
         * collapses while the ordinals already handed out do not, and the next
         * flush re-issued numbers this partition had bound to other rows.
         *
         * It is the max over EVERY column and over the remote-ordinal map's
         * high-water, not over ts.idx alone: a received group whose ts block
         * the commit test is still holding back has already consumed an
         * ordinal, and so has a flush that published its value columns and died
         * before ts.  Computed ONCE, before any column is touched. */
        uint32_t part_ord_base = tsdb_part_next_ordinal(s, part_dir);

        /* Set when the raw-block hook failed for ANY non-ts column of THIS
         * partition.  ts-last only makes the incompleteness of a group a
         * SUFFIX when the writer is fail-stop on the first error; the flush is
         * (a failed col_writer_close returns immediately), the raw-block
         * fan-out was not.  Restoring fail-stop for the remote half turns an
         * arbitrary-subset failure back into a suffix failure, which leaves
         * the peer BEHIND (repairable) instead of TORN (not). */
        int raw_poisoned = 0;

        for (int ix = 0; ix < ci_count; ix++) {
            int ci = ci_iter[ix];
            /* TEST-ONLY fault injection: simulate a crash AFTER every non-ts
             * column has been published but BEFORE the ts column, to exercise
             * partial-flush recovery.  Gated by an env var that is never set in
             * production (getenv returns NULL → no-op), so prod behaviour is
             * unchanged. */
            if (ci == ts_ci && getenv("TSDB_TEST_CRASH_BEFORE_TS")) {
                free(row_idx); free(days); free(sorted_all);
                return TSDB_ERR_IO;
            }
            tsdb_type_t   type  = s->cols[ci].type;
            size_t        width = tsdb_type_width(type);
            const uint8_t *col_buf = (const uint8_t *)tsdb_memtable_col(m, ci);
            if (!col_buf) { free(row_idx); free(days); free(sorted_all); return TSDB_ERR_INTERNAL; }

            col_writer_t w;
            int rc = col_writer_open(&w, part_dir, s->cols[ci].name);
            if (rc != TSDB_OK) { free(row_idx); free(days); free(sorted_all); return rc; }

            /* Raise this column's durable checkpoint to the flush hwm (never
             * lower it — col_writer_open seeded it from the existing idx). */
            if (max_seq > w.max_seq) w.max_seq = max_seq;

            /* Column-count stamp (part.h).  This loop runs over EVERY schema
             * column of this partition and is fail-stop, so by the time the ts
             * column (published last) carries the stamp, all s->ncols columns
             * have published their idx here.  That is what lets the reader tell
             * "column added by a later ALTER, zero-fill is correct" from
             * "column index < stamp, its write is gone, zero-fill would be a
             * fabrication".  Set AFTER col_writer_open, which memsets w. */
            w.ncols = (uint16_t)s->ncols;
            w.part_ord_base = part_ord_base;

            /* Wire up raw-block hook if available.  Suppressed for the ts
             * column once a sibling column's push failed: ts is the peers'
             * visibility marker, so shipping it after a lost non-ts block is
             * precisely what hands a peer a partition it cannot read while
             * count(*) still reports the rows present. */
            if (ci == ts_ci && raw_poisoned && raw_fn) {
                fprintf(stderr,
                        "[part] %s/%s: a non-ts raw-block push failed; "
                        "withholding this partition's ts blocks from peers "
                        "(they stay behind, not torn)\n",
                        table_name ? table_name : "?", part_name);
                w.raw_block_fn = NULL;
            } else {
                w.raw_block_fn = raw_fn;
            }
            w.raw_block_ud    = raw_ud;
            w.raw_block_db    = db;
            w.raw_block_table = table_name;
            w.raw_block_day   = part_day_int;
            w.col_idx         = ci;

            /* Chunk into blocks using the per-table block size.  Clamped
             * into [1024, TSDB_BLOCK_POINTS] by schema_create. */
            size_t sbp = (s->block_points > 0 &&
                          s->block_points <= TSDB_BLOCK_POINTS)
                            ? (size_t)s->block_points : (size_t)TSDB_BLOCK_POINTS;
            size_t base = 0;
            while (base < day_nrows) {
                size_t chunk = day_nrows - base;
                if (chunk > sbp) chunk = sbp;

                uint8_t *chunk_buf = malloc(chunk * width);
                if (!chunk_buf) {
                    col_writer_abort(&w);
                    free(row_idx); free(days); free(sorted_all);
                    return TSDB_ERR_NOMEM;
                }

                /* Extract rows and compute ts_min/ts_max. */
                int64_t ts_min = INT64_MAX, ts_max = INT64_MIN;
                const int64_t *ts_col_data = (const int64_t *)tsdb_memtable_col(m, s->ts_col_idx);
                for (size_t k = 0; k < chunk; k++) {
                    size_t ri = row_idx[base + k];
                    memcpy(chunk_buf + k * width, col_buf + ri * width, width);
                    int64_t t = ts_col_data[ri];
                    if (t < ts_min) ts_min = t;
                    if (t > ts_max) ts_max = t;
                }
                /* For the timestamp column itself, use the actual values. */
                if (type == TSDB_TYPE_TIMESTAMP) {
                    ts_min = ((const int64_t *)chunk_buf)[0];
                    ts_max = ts_min;
                    for (size_t k = 0; k < chunk; k++) {
                        int64_t t = ((const int64_t *)chunk_buf)[k];
                        if (t < ts_min) ts_min = t;
                        if (t > ts_max) ts_max = t;
                    }
                }

                rc = col_writer_write_block(&w, type, chunk_buf, chunk,
                                            ts_min, ts_max);
                free(chunk_buf);
                if (rc != TSDB_OK) {
                    col_writer_abort(&w);
                    free(row_idx); free(days); free(sorted_all);
                    return rc;
                }
                base += chunk;
            }

            rc = col_writer_close(&w);
            if (rc != TSDB_OK) { free(row_idx); free(days); free(sorted_all); return rc; }
            if (w.raw_failed && ci != ts_ci) raw_poisoned = 1;
        }
        free(row_idx);
        /* TEST-ONLY fault injection: after fully publishing this partition,
         * fail as if the process crashed before the next partition — exercises
         * multi-partition recovery (a sibling partition published, the next
         * not).  Env unset -> getenv NULL -> no-op in production. */
        {
            const char *cs = getenv("TSDB_TEST_CRASH_AFTER_PART");
            if (cs && ++published_parts >= atoi(cs)) {
                free(days); free(sorted_all);
                return TSDB_ERR_IO;
            }
        }
    }
    free(days);
    free(sorted_all);
    return TSDB_OK;
}

uint64_t tsdb_part_max_seq(tsdb_schema_t *s, const char *partition_dir) {
    if (!s || !partition_dir) return 0;
    /* Recovery checkpoint = the TS column's stamped hwm ONLY, not the max over
     * all columns.  The flush publishes non-ts columns FIRST and ts LAST (ts is
     * the reader-visibility marker), stamping each column's idx with the flush
     * hwm as it is written.  A crash after a non-ts column publishes but before
     * ts leaves that column's idx at the new hwm while ts is still behind;
     * taking the MAX would advance the WAL-replay cutoff past rows ts never
     * received, so redo_recover_table would SKIP and permanently drop them
     * (observed live as vanished rows + a torn val column).  Because ts is
     * published last, ts.idx.max_seq is exactly "every column of this partition
     * is durable up to this seq" — the only safe cutoff.  A partition with no
     * ts.idx (ts never published) is not durable at all → 0, replay everything. */
    int ts_ci = s->ts_col_idx;
    if (ts_ci < 0 || ts_ci >= s->ncols) return 0;
    char idx_path[4096];
    snprintf(idx_path, sizeof(idx_path), "%s/%s.idx",
             partition_dir, s->cols[ts_ci].name);
    FILE *f = fopen(idx_path, "rb");
    if (!f) return 0;
    uint8_t hdr[TSDB_IDX_HEADER_SIZE];
    size_t n = fread(hdr, 1, TSDB_IDX_HEADER_SIZE, f);
    fclose(f);
    uint32_t cnt = 0; uint16_t ver = 0; uint64_t tot = 0;
    int64_t fmn = 0, fmx = 0; uint32_t esz = 0; uint64_t mseq = 0;
    if (read_idx_header_ex(hdr, n, &cnt, &ver, &tot,
                           &fmn, &fmx, &esz, &mseq) > 0)
        return mseq;
    return 0;
}

/* ---- multi-column publish invariant ---------------------------------------
 *
 * The reader pairs a non-ts column's block to a ts block by (ts_min, count),
 * and tsdb_part_open turns any ts block a column cannot answer for into a
 * TSDB_BLOCK_FLAG_HOLE.  That keeps READS safe.  It does not stop the WRITE
 * side manufacturing holes, and a partition full of holes fails every query
 * touching that column while count(*) — served from ts alone — keeps saying
 * the rows are there.
 *
 * ts-last ordering is what the flush relies on, and it is a PREFIX-commit
 * protocol: sound only when the incompleteness of a group is always a suffix
 * of one totally ordered sequence.  That holds for the flush (one writer,
 * fixed order, fail-stop on the first error) and does NOT hold for the raw
 * block path, where the unit of failure is one independent RPC — so the
 * failure set is an arbitrary SUBSET, and no ordering of independent publishes
 * makes an arbitrary-subset failure safe.
 *
 * The fix is not more ordering, it is testing the property the marker asserts
 * before advancing it.  The commit condition is locally checkable because the
 * join key is IN THE DATA: every column's block for the same rows carries the
 * identical (ts_min, count).  That is why no manifest is needed here.
 */

/* ---- "no idx at all": late add, or a write that never landed? -------------
 *
 * A non-ts column with ZERO block-index entries in a partition ts HAS published
 * into is the one shape block counts cannot classify, because there is nothing
 * to count.  Two histories produce byte-identical directories:
 *
 *   (a) ALTER TABLE ADD COLUMN after those rows were flushed.  The rows really
 *       have no value for it; zero-fill is the CORRECT answer and erroring
 *       instead would break a shipped feature.
 *   (b) the column's write never landed — an interrupted migrate import, a
 *       partial file-level restore, a lost replication group, deleted files.
 *       Zero-fill FABRICATES a value for every row: rc==0, the row count is
 *       right, every cell is 0, and count(*)/max(ts) — hence anti-entropy —
 *       report the partition healthy.
 *
 * So decide on evidence that is not a count.  Two independent facts exist:
 *
 *  1. SHAPE.  tsdb_schema_add_column only ever APPENDS (at index s->ncols), so
 *     a column added later always sits to the RIGHT of every column that
 *     predates it, and the columns present in a partition written by a flush
 *     are always a PREFIX of the schema.  If any column to the right of `ci`
 *     published blocks here, `ci` existed before that column did and cannot be
 *     the late add.  Costs nothing, needs no new on-disk state, and therefore
 *     also protects partitions written by older binaries.  (Note this makes
 *     every column left of the ts column non-late by construction: ts exists
 *     from CREATE TABLE.)
 *
 *  2. THE STAMP.  The ts column's idx records how many columns the schema had
 *     when the flush that published it ran, and the flush writes EVERY schema
 *     column into every partition it touches.  ci < that count therefore means
 *     the column's data WAS written here.  Only the flush stamps it; everyone
 *     else preserves, so the claim is never invented by a one-column publish.
 *     0 == never stamped (a legacy partition, or one built purely by
 *     replication / bulk import) and rule 1 stands alone.
 *
 * Returns 1 when nothing contradicts the late-add hypothesis (caller zero-fills,
 * exactly as before), 0 when it is contradicted (caller must not fabricate).
 * Both the reader (tsdb_part_open) and the writer's commit test
 * (tsdb_part_ts_publish_ready) call THIS function, so the property part.h
 * promises — a writer never publishes something the reader would refuse —
 * holds by construction rather than by two copies agreeing.
 */
static int part_col_absence_is_late_add(const tsdb_schema_t *s, int ci,
                                        const uint32_t *idx_block_count,
                                        uint16_t ts_ncols)
{
    for (int cj = ci + 1; cj < s->ncols; cj++)
        if (idx_block_count[cj] > 0) return 0;          /* rule 1: shape */

    if (ts_ncols != TSDB_IDX_NCOLS_UNKNOWN &&
        ci < (int)ts_ncols) return 0;                   /* rule 2: the stamp */

    return 1;
}

/* Read entry `idx`'s pairing key out of an already-probed idx file, together
 * with its durable ordinal (UNKNOWN on a V1/V2 entry or a pre-ordinal writer). */
static int part_idx_entry_key(FILE *f, int hsz, uint32_t esz, uint32_t idx,
                              int64_t *out_ts_min, uint32_t *out_count,
                              tsdb_block_ord_t *out_ord)
{
    uint8_t e[TSDB_IDX_ENTRY_SIZE];
    size_t want = (esz < sizeof(e)) ? (size_t)esz : sizeof(e);
    if (want < 24) return 0;                       /* no key in this entry */
    if (fseek(f, (long)((uint64_t)hsz + (uint64_t)idx * esz), SEEK_SET) != 0)
        return 0;
    if (fread(e, 1, want, f) != want) return 0;
    *out_count  = get_u32le(e + 12);
    *out_ts_min = get_i64le(e + 16);
    if (out_ord) *out_ord = read_idx_entry_ord(e, (uint32_t)want);
    return 1;
}

int tsdb_part_ts_publish_ready(tsdb_schema_t *s, const char *part_dir,
                               int64_t ts_min, uint32_t count,
                               char *out_missing_col, size_t cap)
{
    tsdb_block_ord_t none = { 0, 0 };
    return tsdb_part_ts_publish_ready_ord(s, part_dir, none, ts_min, count,
                                          out_missing_col, cap);
}

int tsdb_part_ts_publish_ready_ord(tsdb_schema_t *s, const char *part_dir,
                                   tsdb_block_ord_t ord,
                                   int64_t ts_min, uint32_t count,
                                   char *out_missing_col, size_t cap)
{
    if (out_missing_col && cap) out_missing_col[0] = '\0';
    if (!s || !part_dir) return TSDB_ERR_INVAL;
    int ts_ci = s->ts_col_idx;
    if (ts_ci < 0 || ts_ci >= s->ncols) return TSDB_ERR_INVAL;
    if (s->ncols <= 1) return TSDB_OK;

    char idx_path[4096];

    /* How many ts blocks this partition already publishes.  Both producers
     * (the flush and the migration exporter) emit block i of every column in
     * the same order, so the sibling entry is normally at exactly this index —
     * probe it first, so the common case is one 88-byte read per column and
     * not a scan. */
    uint32_t ts_cnt = 0;
    snprintf(idx_path, sizeof(idx_path), "%s/%s.idx",
             part_dir, s->cols[ts_ci].name);
    if (tsdb_part_idx_probe(idx_path, NULL, &ts_cnt, NULL, NULL,
                            NULL, NULL, NULL) < 0)
        return TSDB_ERR_IO;
    uint16_t ts_ncols = tsdb_part_idx_ncols(idx_path);

    /* One probe per column, up front, because the zero-block case below is
     * classified from the shape of the WHOLE column set, not from this column
     * alone (part_col_absence_is_late_add).  Same number of probes as before —
     * the results are reused rather than re-read. */
    uint32_t col_cnt[TSDB_MAX_COLS];
    uint32_t col_esz[TSDB_MAX_COLS];
    int      col_hsz[TSDB_MAX_COLS];
    for (int ci = 0; ci < s->ncols; ci++) {
        col_cnt[ci] = 0; col_esz[ci] = 0; col_hsz[ci] = 0;
        if (ci == ts_ci) { col_cnt[ci] = ts_cnt; continue; }
        snprintf(idx_path, sizeof(idx_path), "%s/%s.idx",
                 part_dir, s->cols[ci].name);
        col_hsz[ci] = tsdb_part_idx_probe(idx_path, NULL, &col_cnt[ci],
                                          &col_esz[ci], NULL, NULL, NULL, NULL);
        if (col_hsz[ci] < 0) return TSDB_ERR_IO;   /* corrupt — not ready */
        if (col_hsz[ci] == 0 || col_esz[ci] == 0) col_cnt[ci] = 0;
    }

    for (int ci = 0; ci < s->ncols; ci++) {
        if (ci == ts_ci) continue;
        snprintf(idx_path, sizeof(idx_path), "%s/%s.idx",
                 part_dir, s->cols[ci].name);

        uint32_t cnt = col_cnt[ci], esz = col_esz[ci];
        int      hsz = col_hsz[ci];

        if (hsz == 0 || cnt == 0 || esz == 0) {
            /* No blocks at all for this column here.
             *
             * EXEMPT only while nothing contradicts ALTER TABLE ADD COLUMN,
             * which is exactly what tsdb_part_open zero-fills by design — the
             * SAME test the reader applies, so this can never hand the reader a
             * ts block it will then refuse to answer for.
             *
             * REFUSED otherwise — including when ts has published nothing here
             * either (a FRESH partition whose first group is landing: the flush
             * writes every schema column for every partition it touches, so a
             * column with no block here means the whole column's push was
             * lost).  Publishing ts anyway is the case that reads back as
             * fabricated ZEROS with rc=0, which is worse than an error: it
             * poisons aggregates and hides the loss from anti-entropy. */
            if (ts_cnt == 0 ||
                !part_col_absence_is_late_add(s, ci, col_cnt, ts_ncols)) {
                if (out_missing_col && cap)
                    snprintf(out_missing_col, cap, "%s", s->cols[ci].name);
                return TSDB_ERR_BUSY;
            }
            continue;
        }

        FILE *f = fopen(idx_path, "rb");
        if (!f) return TSDB_ERR_IO;

        int      found = 0;
        int64_t  emin  = 0;
        uint32_t ecnt  = 0;
        tsdb_block_ord_t eord = { 0, 0 };

        if (TSDB_ORD_KNOWN(ord)) {
            /* The ordinal is the identity.  "Some block with a matching key"
             * is NOT good enough: on a duplicate-timestamp run every block of
             * the run carries the same (ts_min, count), so the test passes on
             * the WRONG sibling and publishes a ts block over a group this node
             * never received — the partition then answers that column with
             * another group's values, rc=0.  A legacy entry with no marker
             * keeps its physical position as its ordinal, which is the writers'
             * ordering contract and what the reader assumes for it too. */
            for (uint32_t b = 0; !found && b < cnt; b++) {
                if (!part_idx_entry_key(f, hsz, esz, b, &emin, &ecnt, &eord))
                    continue;
                if (idx_eff_ord(eord, b) == ord.v &&
                    emin == ts_min && ecnt == count)
                    found = 1;
            }
        } else {
            if (ts_cnt < cnt &&
                part_idx_entry_key(f, hsz, esz, ts_cnt, &emin, &ecnt, NULL) &&
                emin == ts_min && ecnt == count)
                found = 1;
            for (uint32_t b = 0; !found && b < cnt; b++) {
                if (part_idx_entry_key(f, hsz, esz, b, &emin, &ecnt, NULL) &&
                    emin == ts_min && ecnt == count)
                    found = 1;
            }
        }
        fclose(f);

        if (!found) {
            if (out_missing_col && cap)
                snprintf(out_missing_col, cap, "%s", s->cols[ci].name);
            return TSDB_ERR_BUSY;
        }
    }
    return TSDB_OK;
}

int tsdb_part_ts_retract_unpaired(tsdb_schema_t *s, const char *part_dir,
                                  uint32_t *out_retracted)
{
    if (out_retracted) *out_retracted = 0;
    if (!s || !part_dir) return TSDB_ERR_INVAL;
    int ts_ci = s->ts_col_idx;
    if (ts_ci < 0 || ts_ci >= s->ncols) return TSDB_ERR_INVAL;

    char ts_idx_path[4096];
    snprintf(ts_idx_path, sizeof(ts_idx_path), "%s/%s.idx",
             part_dir, s->cols[ts_ci].name);

    int rc = TSDB_OK;
    uint8_t *ts_entries = NULL;

    tsdb_part_idx_lock(part_dir);

    uint32_t ts_cnt = 0, ts_esz = 0;
    uint64_t ts_mseq = 0;
    int ts_hsz = tsdb_part_idx_probe(ts_idx_path, NULL, &ts_cnt, &ts_esz,
                                     NULL, NULL, NULL, &ts_mseq);
    /* write_idx_header always stamps entry_size = TSDB_IDX_ENTRY_SIZE, so the
     * entries republished below must be that wide.  A legacy V1/V2 idx has
     * 40-byte entries: widen them (zero stats tail == "absent", exactly what
     * col_writer_close does).  Anything wider than we understand is left
     * alone rather than rewritten into a shape we would be guessing at. */
    if (ts_hsz <= 0 || ts_cnt == 0 || ts_esz == 0 ||
        ts_esz > TSDB_IDX_ENTRY_SIZE) goto done;   /* nothing safe to do */

    ts_entries = calloc((size_t)ts_cnt, TSDB_IDX_ENTRY_SIZE);
    if (!ts_entries) { rc = TSDB_ERR_NOMEM; goto done; }
    {
        FILE *f = fopen(ts_idx_path, "rb");
        if (!f) { rc = TSDB_ERR_IO; goto done; }
        int ok = (fseek(f, (long)ts_hsz, SEEK_SET) == 0);
        for (uint32_t b = 0; ok && b < ts_cnt; b++)
            ok = (fread(ts_entries + (size_t)b * TSDB_IDX_ENTRY_SIZE,
                        1, ts_esz, f) == ts_esz);
        fclose(f);
        if (!ok) { rc = TSDB_ERR_IO; goto done; }
    }
    ts_esz = TSDB_IDX_ENTRY_SIZE;                  /* in-memory stride from here */

    /* Longest ts block PREFIX every column can pair with.  Blocks append in
     * ts order, so the readable part of a torn partition is a prefix; keeping
     * the prefix is what the torn-.col clamp already does on the read side. */
    uint32_t keep = ts_cnt;
    for (int ci = 0; ci < s->ncols && keep > 0; ci++) {
        if (ci == ts_ci) continue;
        char cpath[4096];
        snprintf(cpath, sizeof(cpath), "%s/%s.idx", part_dir, s->cols[ci].name);
        uint32_t ccnt = 0, cesz = 0;
        int chsz = tsdb_part_idx_probe(cpath, NULL, &ccnt, &cesz,
                                       NULL, NULL, NULL, NULL);
        /* A column with NO entries here is an ALTER-added column as far as
         * this partition can tell — the same call tsdb_part_open makes.  Never
         * retract a whole partition on that evidence. */
        if (chsz <= 0 || ccnt == 0 || cesz == 0) continue;

        FILE *cf = fopen(cpath, "rb");
        if (!cf) { rc = TSDB_ERR_IO; goto done; }

        /* May this column's ordinals be believed?  Only if EVERY entry records
         * one — exactly part_align_column's `ord_mode`, and for the same reason:
         * a column holding even one unmarked entry is on the legacy content
         * rule, and its OTHER entries can carry ordinals from a space the
         * unmarked ts prefix knows nothing about (a repair push translated into
         * this node's own numbering, for one).  Comparing those against a ts
         * entry's positional effective ordinal is a category error, and it
         * rejects a block that is demonstrably there.
         *
         * It is a property of the COLUMN's entries alone, so it is only half the
         * question: each comparison below ALSO needs the ts entry it is made
         * against to carry a marker, or the ordinal rule runs with nothing real
         * on the other side.  A whole column re-synced into a legacy partition
         * is exactly that — every entry of the column stamped, every entry of ts
         * not — and tsdb_part_next_ordinal's legacy floor guarantees the
         * re-synced numbers start at ts.idx's ENTRY COUNT, so they can never
         * equal one of ts's invented positions.  The ordinal rule then pairs
         * NOTHING, and this function concludes the partition is torn from block
         * 0 and republishes ts.idx at zero entries — deleting every row of a
         * partition tsdb_part_open reads back whole (test_adv_repair_portable
         * certifies rc=0 rows=3072 sum=4720128 on those bytes).  A repair
         * primitive must not delete rows the reader can serve. */
        int col_ord_mode = 1;
        for (uint32_t k = 0; k < ccnt; k++) {
            int64_t emin = 0; uint32_t ecnt = 0;
            tsdb_block_ord_t eord = { 0, 0 };
            if (!part_idx_entry_key(cf, chsz, cesz, k, &emin, &ecnt, &eord) ||
                !TSDB_ORD_KNOWN(eord)) { col_ord_mode = 0; break; }
        }

        /* ...and neither is a column whose entries are a contiguous SUFFIX of
         * ts's blocks.  That is the ALTER shape at every non-zero length, and
         * it is precisely what tsdb_part_open's alignment pass classifies as a
         * late add and answers with the zeros those rows legitimately hold —
         * `idx_decl_count[ci] >= idx_decl_count[ts_ci]` is the same gate on the
         * read side.  Retracting it deletes rows that read perfectly, and since
         * no push is ever coming for a locally ALTER-added column, the deletion
         * is permanent.  Measured on this shape with the ordinal-only rule
         * below: count(*) 5120 -> 2048 with repeating timestamps and 5120 -> 0
         * with unique ones, where HEAD retracted nothing. */
        if (ccnt < ts_cnt) {
            uint32_t nmiss = ts_cnt - ccnt;
            int      suffix = 1;
            for (uint32_t k = 0; suffix && k < ccnt; k++) {
                const uint8_t *te = ts_entries + (size_t)(nmiss + k) * ts_esz;
                int64_t emin = 0; uint32_t ecnt = 0;
                tsdb_block_ord_t eord = { 0, 0 };
                if (!part_idx_entry_key(cf, chsz, cesz, k, &emin, &ecnt, &eord) ||
                    emin != get_i64le(te + 16) || ecnt != get_u32le(te + 12) ||
                    (col_ord_mode &&
                     TSDB_ORD_KNOWN(read_idx_entry_ord(te, ts_esz)) &&
                     eord.v != idx_eff_ord(read_idx_entry_ord(te, ts_esz),
                                           nmiss + k)))
                    suffix = 0;
            }
            if (suffix) { fclose(cf); continue; }
        }

        for (uint32_t b = 0; b < keep; b++) {
            const uint8_t *te = ts_entries + (size_t)b * ts_esz;
            uint32_t want_cnt = get_u32le(te + 12);
            int64_t  want_min = get_i64le(te + 16);
            uint32_t want_ord = idx_eff_ord(read_idx_entry_ord(te, ts_esz), b);
            int      found = 0;
            int64_t  emin = 0; uint32_t ecnt = 0;
            tsdb_block_ord_t eord = { 0, 0 };

            /* The positional probe first: both writers emit block b of every
             * column in the same order, so this is the overwhelmingly common
             * case and it stays a pure check. */
            if (b < ccnt && part_idx_entry_key(cf, chsz, cesz, b, &emin, &ecnt, &eord) &&
                emin == want_min && ecnt == want_cnt &&
                (!(col_ord_mode &&
                   TSDB_ORD_KNOWN(read_idx_entry_ord(te, ts_esz))) ||
                 eord.v == want_ord))
                found = 1;

            /* Otherwise scan.  Where this column's ordinals may be believed
             * the ordinal decides, because on a duplicate-timestamp run every
             * block of the run carries the same content key and a key match
             * cannot tell the column that holds THIS group from one holding
             * another.  Where they may not, the key is all there is: an unmarked
             * entry's position is NOT its ordinal (a lost push closes the
             * survivors up over the gap), so demanding
             * `idx_eff_ord(eord, k) == want_ord` would collapse the scan to
             * `k == b` and make it dead code — which is how this repair came to
             * truncate whole readable partitions. */
            for (uint32_t k = 0; !found && k < ccnt; k++) {
                if (!part_idx_entry_key(cf, chsz, cesz, k, &emin, &ecnt, &eord))
                    continue;
                if (emin != want_min || ecnt != want_cnt) continue;
                if (col_ord_mode &&
                    TSDB_ORD_KNOWN(read_idx_entry_ord(te, ts_esz)) &&
                    eord.v != want_ord) continue;
                found = 1;
            }
            if (!found) { keep = b; break; }
        }
        fclose(cf);
    }

    if (keep == ts_cnt) goto done;                 /* already paired — no-op */

    /* Republish ts.idx at `keep` entries.  Version and max_seq are preserved
     * (write_idx_header emits V4 exactly when max_seq > 0), the .col bytes and
     * every non-ts entry are left alone, so a later push re-lands the missing
     * block and the partition heals upward. */
    {
        uint64_t new_total = 0;
        int64_t  fmn = 0, fmx = 0;
        for (uint32_t b = 0; b < keep; b++) {
            const uint8_t *e = ts_entries + (size_t)b * ts_esz;
            new_total += get_u32le(e + 12);
            int64_t emin = get_i64le(e + 16), emax = get_i64le(e + 24);
            if (b == 0 || emin < fmn) fmn = emin;
            if (b == 0 || emax > fmx) fmx = emax;
        }

        char tmp_path[4200];
        snprintf(tmp_path, sizeof(tmp_path), "%s.retract.tmp", ts_idx_path);
        FILE *w = fopen(tmp_path, "wb");
        if (!w) { rc = TSDB_ERR_IO; goto done; }

        uint8_t hdr[TSDB_IDX_HEADER_SIZE];
        /* Republishing one column's idx: PRESERVE its column-count stamp
         * (part.h) exactly as the version and max_seq are preserved.  A
         * retraction knows nothing new about which columns this partition was
         * written with, so it must neither assert nor erase the claim. */
        size_t  hsz = write_idx_header(hdr, keep, new_total, fmn, fmx, ts_mseq,
                                       tsdb_part_idx_ncols(ts_idx_path));
        int io_ok = (fwrite(hdr, 1, hsz, w) == hsz);
        if (io_ok && keep > 0) {
            size_t n = (size_t)keep * ts_esz;
            io_ok = (fwrite(ts_entries, 1, n, w) == n);
        }
        if (io_ok && fflush(w) != 0) io_ok = 0;
        if (io_ok && ferror(w))      io_ok = 0;
        if (io_ok && tsdb_part_fsync_fd(fileno(w)) != 0) io_ok = 0;
        if (fclose(w) != 0) io_ok = 0;
        if (!io_ok || rename(tmp_path, ts_idx_path) != 0) {
            unlink(tmp_path);
            rc = TSDB_ERR_IO;
            goto done;
        }
        part_fsync_dir(part_dir);

        fprintf(stderr,
                "[part] %s: retracted %u unpaired ts block(s) (%u -> %u); the "
                "rows are no longer counted, so the gap is visible to "
                "anti-entropy instead of reading as an error forever\n",
                part_dir, ts_cnt - keep, ts_cnt, keep);
        if (out_retracted) *out_retracted = ts_cnt - keep;
    }

done:
    tsdb_part_idx_unlock(part_dir);
    free(ts_entries);
    return rc;
}

/* ---- tsdb_part_t (read side) ------------------------------------------- */

struct tsdb_part {
    tsdb_schema_t *schema;
    char           dir[4096];

    struct {
        int      fd;
        uint8_t *map;
        size_t   map_size;
    } col_maps[TSDB_MAX_COLS];

    tsdb_block_meta_t *col_metas[TSDB_MAX_COLS];
    size_t             col_meta_n[TSDB_MAX_COLS];

    /* File-level zone map aggregated across all columns of this partition.
     * Populated on open from v2 idx headers; falls back to per-block
     * ts column metadata for v1 idx files. */
    int64_t            zone_ts_min;
    int64_t            zone_ts_max;
    int                zone_valid;

    /* Zero-copy read gate, latched from env TSDB_ZEROCOPY_READ at open
     * (default ON; "0" disables).  Per-handle so an in-process setenv
     * takes effect on the next open, never mid-scan. */
    int                zerocopy;
};

/* Process-wide zero-copy read counters (stats/tests only; relaxed). */
static uint64_t g_zerocopy_hits;
static uint64_t g_zerocopy_fallbacks;

void tsdb_part_zerocopy_stats(uint64_t *out_hits, uint64_t *out_fallbacks) {
    if (out_hits)
        *out_hits = __atomic_load_n(&g_zerocopy_hits, __ATOMIC_RELAXED);
    if (out_fallbacks)
        *out_fallbacks = __atomic_load_n(&g_zerocopy_fallbacks, __ATOMIC_RELAXED);
}

/* Complete a compaction column-swap that a crash interrupted.  The compactor
 * persists <part>/.compact_swap (one column name per line, fsynced before the
 * first rename) and unlinks it only after every produced column's .tmp pair
 * has been renamed in and the directory fsynced.  So when the marker exists
 * here, the staged .tmp files that remain are the not-yet-renamed remainder
 * of a fully-staged swap: renaming them in rolls the partition FORWARD to
 * the complete post-compaction state.  Without this, a restart mid-swap
 * (this engine's shutdown is deliberately a fast-exit "controlled crash")
 * left e.g. ts/h compacted but v on the old block layout — a column-desynced
 * partition surfacing as "data corrupt" with a collapsed row count (observed
 * live on the test cluster).  Idempotent and safe under concurrent callers:
 * each rename is atomic, a second rename of the same pair is a harmless
 * ENOENT, and every opener runs this to completion before reading. */
static void part_compact_swap_recover(const char *part_dir) {
    char marker[4096];
    snprintf(marker, sizeof(marker), "%s/.compact_swap", part_dir);
    FILE *f = fopen(marker, "r");
    if (!f) return;                          /* common case: no crashed swap */
    char col[256];
    while (fgets(col, sizeof(col), f)) {
        size_t L = strlen(col);
        while (L > 0 && (col[L-1] == '\n' || col[L-1] == '\r')) col[--L] = '\0';
        if (!L) continue;
        char tmp[4096], live[4096];
        snprintf(tmp,  sizeof(tmp),  "%s/%s.col.tmp", part_dir, col);
        snprintf(live, sizeof(live), "%s/%s.col",     part_dir, col);
        rename(tmp, live);                   /* ENOENT = already renamed */
        snprintf(tmp,  sizeof(tmp),  "%s/%s.idx.tmp", part_dir, col);
        snprintf(live, sizeof(live), "%s/%s.idx",     part_dir, col);
        rename(tmp, live);
    }
    fclose(f);
    unlink(marker);
    int dfd = open(part_dir, O_RDONLY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }
}

/* ---- Aligning a non-ts column's block array to the ts column's -------------
 *
 * exec.c addresses a sibling block by the ts block's POSITION.  This pass is
 * where that position is established, and the durable ordinal is what
 * establishes it — never a content scan.  (ts_min, ts_max, count) is not a key:
 * a run of rows carrying one timestamp produces several genuinely different
 * blocks whose keys are byte-identical, so a first-match scan returns whichever
 * one happens to come first and the query answers with another group's values,
 * rc=0.  The content is still CHECKED, it is just no longer what selects.
 */

/* Does a column block describe the same rows as a ts block?
 *
 * ts_max is only EVIDENCE when the column entry carries a durable ordinal.
 *
 * The pristine reader identified a block by (ts_min, count) and never looked at
 * ts_max.  Requiring it unconditionally reaches backwards onto bytes this change
 * cannot re-stamp, and there is a producer: the range borrow in the compaction
 * this same change fixes could leave output chunk 0's value entry a tick wider
 * than ts's.  On such a partition the strict rule pairs nothing for that block,
 * so a column pristine answers correctly —
 *
 *     pristine 9dab5a2   SELECT v rc=0 rows=3072 sum=4720128
 *
 * — becomes a permanent TSDB_ERR_CORRUPT with no self-heal path.  That is the
 * same defect class as the repair-push regression ord_retry_legacy exists to
 * close, and it gets the same answer: a rule introduced with the marker applies
 * to entries that carry the marker.
 *
 * BOTH sides have to carry it, and one marker was not enough.  A comparison has
 * two operands: a marked COLUMN entry says the sender ran the fixed compaction,
 * but the ts entry it is measured against can still be a legacy one whose
 * ts_max a binary that COULD borrow wrote — and the legacy partition is exactly
 * where a re-synced column lands, because that is what the repair path is for.
 * Measured on that shape (a legacy partition whose value column an upgraded
 * sender re-synced whole, one entry carrying the borrow): the column pairs by
 * content, the strict rule then rejects the pairing, and
 *
 *     SELECT v rc=0 rows=3072 sum=4720128   ->   TSDB_ERR_CORRUPT, forever
 *
 * with rc=0 on every push — the repair the error message asks for making the
 * partition permanently worse.
 *
 * Where BOTH entries are marked, both were written by a binary with the fixed
 * compaction, ts_max is exact, and a disagreement is real corruption that used
 * to be served as an answer.  The ordinal branch below is therefore strict
 * exactly there — ord_mode requires every column entry to be marked, and this
 * adds the other operand. */
static int part_meta_agrees(const tsdb_block_meta_t *c, const tsdb_block_meta_t *t) {
    if (c->count != t->count || c->ts_min != t->ts_min) return 0;
    return !TSDB_ORD_KNOWN(c->ord) || !TSDB_ORD_KNOWN(t->ord) ||
           c->ts_max == t->ts_max;
}

static int part_all_ord_known(const tsdb_block_meta_t *m, size_t n) {
    for (size_t i = 0; i < n; i++)
        if (!TSDB_ORD_KNOWN(m[i].ord)) return 0;
    return 1;
}

/* (ordinal, position) pairs sorted by ordinal, for an O(log n) lookup. */
typedef struct { uint32_t ord; uint32_t pos; } part_ordref_t;

static int part_ordref_cmp(const void *a, const void *b) {
    uint32_t x = ((const part_ordref_t *)a)->ord;
    uint32_t y = ((const part_ordref_t *)b)->ord;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

/* Position of the entry with ordinal `want`, or -1 if there is none.
 *
 * Two entries CAN claim one ordinal, and it is not corruption: a flush that
 * crashed after publishing a non-ts column but before ts leaves that column an
 * orphan entry, and the re-flush of the same rows (replayed from the WAL) then
 * stamps the same ordinal again.  The idx is append-ordered, so the LAST such
 * entry is the one the surviving ts block was published with; the earlier one
 * is the dead prefix.  Take the last. */
static long part_ordref_find(const part_ordref_t *v, size_t n, uint32_t want) {
    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (v[mid].ord <= want) lo = mid + 1; else hi = mid;
    }
    if (lo == 0 || v[lo - 1].ord != want) return -1;
    /* qsort is not stable, so scan the equal run for the highest position. */
    uint32_t best = v[lo - 1].pos;
    for (size_t i = lo - 1; i > 0 && v[i - 1].ord == want; i--)
        if (v[i - 1].pos > best) best = v[i - 1].pos;
    return (long)best;
}

/* Content key, used ONLY on the legacy path below and ONLY when unique.
 *
 * Two keys, deliberately.  FULL is (ts_min, ts_max, count); REL drops ts_max and
 * is the identity the pristine reader used.  A legacy entry's ts_max can be a
 * tick wider than its ts partner's (see part_meta_agrees), so the full key can
 * fail to place a block that is not missing at all — while the full key is what
 * separates two blocks of one duplicate-timestamp run that differ only there.
 * Neither alone is right; the pairing below runs FULL first and lets REL fill
 * only what FULL left empty, each gated on TS-side uniqueness under its own key
 * so that both remain forced placements rather than guesses. */
typedef struct { int64_t mn, mx; uint32_t cnt, pos; } part_keyref_t;

static int part_keyref_cmp(const void *a, const void *b) {
    const part_keyref_t *x = (const part_keyref_t *)a;
    const part_keyref_t *y = (const part_keyref_t *)b;
    if (x->mn  != y->mn)  return x->mn  < y->mn  ? -1 : 1;
    if (x->mx  != y->mx)  return x->mx  < y->mx  ? -1 : 1;
    if (x->cnt != y->cnt) return x->cnt < y->cnt ? -1 : 1;
    return 0;
}

static int part_keyref_cmp_rel(const void *a, const void *b) {
    const part_keyref_t *x = (const part_keyref_t *)a;
    const part_keyref_t *y = (const part_keyref_t *)b;
    if (x->mn  != y->mn)  return x->mn  < y->mn  ? -1 : 1;
    if (x->cnt != y->cnt) return x->cnt < y->cnt ? -1 : 1;
    return 0;
}

/* Fill `out` sorted by key; return 1 iff every key is pairwise DISTINCT.
 *
 * UNIQUENESS IS LOAD-BEARING ON THE TS SIDE ONLY.  A repeated key among the ts
 * blocks means the correspondence is a guess — nothing left on disk says which
 * ts block a column block belongs to.  A repeated key among the COLUMN's blocks
 * says something else entirely: two entries describe the SAME rows, so either
 * answers the ts block that asks for them.  Treating that as ambiguity turned
 * every ts block into a HOLE and the whole column into a permanent
 * TSDB_ERR_CORRUPT — for a shape part_ordref_find explicitly calls *not*
 * corruption (a flush that published a non-ts column and died before ts, then
 * re-flushed the same rows from the WAL). */
static int part_keyref_build(const tsdb_block_meta_t *m, size_t n,
                             part_keyref_t *out, int rel) {
    int (*cmp)(const void *, const void *) =
        rel ? part_keyref_cmp_rel : part_keyref_cmp;
    for (size_t i = 0; i < n; i++) {
        out[i].mn = m[i].ts_min; out[i].mx = m[i].ts_max;
        out[i].cnt = m[i].count; out[i].pos = (uint32_t)i;
    }
    if (n > 1) qsort(out, n, sizeof(*out), cmp);
    for (size_t i = 1; i < n; i++)
        if (cmp(&out[i - 1], &out[i]) == 0) return 0;
    return 1;
}

/* Position of the column entry answering ts block `t`, or -1.
 *
 * Resolves a repeated key the way part_ordref_find resolves a repeated ordinal,
 * and for the same reason: the idx is append-ordered, so the LAST claimant is
 * the one the surviving ts block was published with and the earlier one is the
 * dead prefix.  (Upper bound, then walk the equal run back for its highest
 * position — qsort is not stable, so the run's order is not the file's.) */
static long part_keyref_find(const part_keyref_t *v, size_t n,
                             const tsdb_block_meta_t *t, int rel) {
    int (*cmp)(const void *, const void *) =
        rel ? part_keyref_cmp_rel : part_keyref_cmp;
    part_keyref_t probe;
    probe.mn = t->ts_min; probe.mx = t->ts_max; probe.cnt = t->count; probe.pos = 0;
    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (cmp(&v[mid], &probe) <= 0) lo = mid + 1; else hi = mid;
    }
    if (lo == 0 || cmp(&v[lo - 1], &probe) != 0) return -1;
    uint32_t best = v[lo - 1].pos;
    for (size_t i = lo - 1; i > 0 && cmp(&v[i - 1], &probe) == 0; i--)
        if (v[i - 1].pos > best) best = v[i - 1].pos;
    return (long)best;
}

/* The legacy content rule, in one place: fill `pick` by key, twice.
 *
 * Returns 1 when a placement was made (or provably none exists), 0 when the TS
 * keys repeat and any alignment would be a guess — the caller reports that as
 * ambiguity and the slots read as an error rather than as another block's
 * values.  -1 is out of memory.
 *
 * The REL pass touches ONLY slots the FULL pass left empty and cannot steal a
 * block the FULL pass placed: REL-uniqueness on the TS side means no two ts
 * slots share a REL key, and a column entry has exactly one, so at most one slot
 * can ever claim it. */
static int part_legacy_pair(const tsdb_block_meta_t *ts_m, size_t nb_ts,
                            const tsdb_block_meta_t *cm, size_t nb_col,
                            long *pick)
{
    part_keyref_t *kt = malloc(nb_ts  * sizeof(*kt));
    part_keyref_t *kc = malloc(nb_col * sizeof(*kc));
    if (!kt || !kc) { free(kt); free(kc); return -1; }

    int ok = part_keyref_build(ts_m, nb_ts, kt, 0);
    (void)part_keyref_build(cm, nb_col, kc, 0);    /* sorts; uniqueness moot */
    if (ok) {
        size_t unplaced = 0;
        for (size_t b = 0; b < nb_ts; b++) {
            pick[b] = part_keyref_find(kc, nb_col, &ts_m[b], 0);
            if (pick[b] < 0) unplaced++;
        }
        if (unplaced && part_keyref_build(ts_m, nb_ts, kt, 1)) {
            (void)part_keyref_build(cm, nb_col, kc, 1);
            for (size_t b = 0; b < nb_ts; b++)
                if (pick[b] < 0)
                    pick[b] = part_keyref_find(kc, nb_col, &ts_m[b], 1);
        }
    }
    free(kt); free(kc);
    return ok;
}

/*
 * Build the nb_ts-long array that answers for column `ci`.
 *
 * *out_merged == NULL means "already exactly aligned, keep the column's own
 * array".  Otherwise the caller takes ownership.  Every slot is one of:
 *   - a real block                (paired by ordinal, metadata verified)
 *   - an ALTER zero-fill sentinel (offset UINT64_MAX, no NO_VALUE flag)
 *   - a HOLE                      (offset UINT64_MAX | TSDB_BLOCK_FLAG_HOLE)
 *   - a MISMATCH                  (offset UINT64_MAX | TSDB_BLOCK_FLAG_MISMATCH)
 * and keeping those four apart is the whole job: a sentinel reads as the zeros
 * it legitimately is, a HOLE and a MISMATCH each read as their own named error,
 * and none of them is ever another's values.
 */
static int part_align_column(const tsdb_schema_t *s, int ci,
                             const char *partition_dir,
                             const tsdb_block_meta_t *ts_m, size_t nb_ts,
                             const tsdb_block_meta_t *cm, size_t nb_col,
                             const uint32_t *idx_decl_count,
                             uint16_t ts_ncols_stamp,
                             tsdb_block_meta_t **out_merged)
{
    *out_merged = NULL;

    /* Ordinal pairing is gated on the COLUMN's ordinals, not on both arrays.
     *
     * Gating on ts as well made the fix unreachable on any partition an
     * unpatched binary ever wrote: a flush only APPENDS to ts.idx, so its
     * unmarked legacy prefix survives forever and no amount of subsequent
     * patched writing ever removes it.  The partition would stay on the legacy
     * content rule permanently — and on a duplicate-timestamp table that rule
     * declares the placement ambiguous, so a column this binary wrote itself
     * (an ALTER TABLE ADD COLUMN, say) became unreadable for good.
     *
     * The ts column does not need its own marker to be usable here, because ts
     * is the partition's spine: entries are only ever appended to it and
     * part_idx_next_ord() seeds the first stamped ordinal at the pre-existing
     * entry count, so on an unmarked ts prefix position IS ordinal by
     * construction — exactly what idx_eff_ord() encodes.  A short COLUMN gets
     * no such guarantee, which is why its ordinals must all be recorded before
     * they are believed; without that the legacy content reasoning below (which
     * can still place a legacy ALTER-added suffix correctly) has to stand. */
    const int ord_mode = nb_col > 0 && part_all_ord_known(cm, nb_col);

    /* Already aligned?  The overwhelmingly common case — both writers emit
     * block i of every column in the same order — and it stays a pure check.
     * The metadata is verified either way: an ordinal that pairs over rows the
     * two indexes describe differently is corruption, and taking the fast exit
     * on it would serve it instead of naming it. */
    if (nb_col >= nb_ts) {
        int exact = 1;
        for (size_t b = 0; b < nb_ts && exact; b++) {
            if (ord_mode && cm[b].ord.v != idx_eff_ord(ts_m[b].ord, (uint32_t)b))
                exact = 0;
            /* A MARKED column entry sitting against an UNMARKED ts entry is
             * numbering in a space that does not meet ts's invented positions —
             * tsdb_part_next_ordinal's legacy floor is ts.idx's entry count, so
             * the two are disjoint by construction — and its position therefore
             * says nothing about which ts block it answers.
             *
             * Reached when only SOME of the column's entries are marked, so
             * ord_mode is off and the loop would otherwise fall through to a
             * pure content check.  That is the state the documented repair
             * leaves behind: a legacy partition whose column is short is
             * re-synced block by block, the pushes APPEND marked entries beside
             * the unmarked survivors, and on a duplicate-timestamp table every
             * key is identical — so position alone accepted a re-delivered copy
             * of block 0 as the answer for ts block 2.  Measured without this:
             * SELECT v rc=0 rows=3072 sum=2622976 against an intact 4720128 —
             * a refusal turning into a silent wrong answer under the repair the
             * error message asks for.
             *
             * A partition being flushed concurrently is untouched: a writer
             * appends to ts and to the column in lockstep, so their marked
             * prefixes begin at the same position and a marked column entry
             * never lines up with an unmarked ts entry. */
            else if (TSDB_ORD_KNOWN(cm[b].ord) && !TSDB_ORD_KNOWN(ts_m[b].ord))
                exact = 0;
            else if (!part_meta_agrees(&cm[b], &ts_m[b]))
                exact = 0;
        }
        if (exact) return TSDB_OK;
    }

    /* pick[b]: >= 0 the column entry that answers ts block b
     *          PICK_NONE      nothing pairs (sentinel or HOLE, per alter_shaped)
     *          PICK_MISMATCH  an ordinal paired but its metadata disagreed */
    enum { PICK_NONE = -1, PICK_MISMATCH = -2 };

    long *pick = malloc(nb_ts * sizeof(*pick));
    if (!pick) return TSDB_ERR_NOMEM;
    for (size_t b = 0; b < nb_ts; b++) pick[b] = PICK_NONE;

    int alter_shaped = 0;
    int mismatched   = 0;      /* an ordinal paired but the content disagreed */
    int ambiguous    = 0;      /* legacy + duplicate keys: nothing is provable */

    /* Set when the ordinal branch paired NOTHING: the two sides are numbering
     * in disjoint spaces, not describing different rows, so the legacy content
     * rule below has to decide instead.  See the long comment at the end of the
     * ordinal branch. */
    int ord_retry_legacy = 0;

    if (nb_col == 0) {
        /* Nothing to pair against.  Unchanged rule: only a shape/stamp argument
         * (part_col_absence_is_late_add) may license the zero-fill. */
        alter_shaped = (idx_decl_count[ci] == 0) &&
                       part_col_absence_is_late_add(s, ci, idx_decl_count,
                                                    ts_ncols_stamp);
    } else if (ord_mode) {
        part_ordref_t *ov = malloc(nb_col * sizeof(*ov));
        if (!ov) { free(pick); return TSDB_ERR_NOMEM; }
        for (size_t k = 0; k < nb_col; k++) {
            ov[k].ord = cm[k].ord.v; ov[k].pos = (uint32_t)k;
        }
        if (nb_col > 1) qsort(ov, nb_col, sizeof(*ov), part_ordref_cmp);

        size_t matched = 0;
        for (size_t b = 0; b < nb_ts; b++) {
            long k = part_ordref_find(ov, nb_col, idx_eff_ord(ts_m[b].ord,
                                                              (uint32_t)b));
            if (k < 0) continue;
            if (!part_meta_agrees(&cm[k], &ts_m[b])) {
                /* The ordinal paired and the rows disagree.  That is its own
                 * fact — the two indexes contradict each other about what this
                 * partition holds — and it is kept apart from a missing block
                 * all the way to the read site, which reports it under its own
                 * name and metric instead of calling it a short column. */
                mismatched = 1;
                pick[b] = PICK_MISMATCH;
                continue;
            }
            pick[b] = k; matched++;
        }
        free(ov);

        /* A genuine late add owns a contiguous SUFFIX of the ts ordinals, and
         * nothing else.  Verified on the ordinal, so a duplicate-key run can no
         * longer satisfy it by accident — which is how an interior loss used to
         * be misread as an ALTER and answered with fabricated zeros.
         *
         * matched > 0 is required: a column that has blocks but matches NO ts
         * ordinal is corruption, and zero-filling it whole would fabricate
         * every one of its values.  (The genuinely blockless case is the
         * nb_col == 0 branch above, which is decided on the stamp.) */
        if (!mismatched && matched > 0) {
            size_t nmiss = nb_ts - matched;
            alter_shaped = 1;
            for (size_t b = 0; b < nb_ts; b++) {
                int want = (b >= nmiss);
                if ((pick[b] >= 0) != want) { alter_shaped = 0; break; }
            }
        }

        /* Paired NOTHING.
         *
         * A column whose blocks match NONE of ts's ordinals has not been shown
         * to be short — the two sides may simply be numbering in DISJOINT
         * spaces, which tsdb_part_next_ordinal() guarantees by construction.
         * It never re-issues a number this partition has already bound, so a
         * column re-delivered block by block is stamped out of the FREE range
         * above everything ts already owns.  Nothing overlaps, ever, and that
         * holds whether or not ts carries markers of its own:
         *
         *   - legacy ts: its "ordinals" are the positions idx_eff_ord() invents,
         *     0..nb_ts-1, and the re-sync is stamped nb_ts, nb_ts+1, ...
         *   - ts THIS BINARY WROTE: it owns marked 0..nb_ts-1 for real, and the
         *     re-sync is stamped nb_ts, nb_ts+1, ... for exactly the same
         *     reason — those are the ordinals that are free.
         *
         * Gating this escape on "ts is not fully marked" therefore closed the
         * repair for the FIRST shape and left it open for the second, which is
         * backwards: every partition becomes the second shape once the fleet has
         * upgraded and written, so the gate turned a rollout-window bug into a
         * permanent one.
         *
         * That is not a hypothetical: it is the documented repair path.  The
         * engine's own error message tells the operator to re-sync a lost
         * column — which the applier can only do one (column, block) push at a
         * time.  Falling through here with matched == 0 HOLEs every slot, so the
         * repair is accepted at rc=TSDB_OK, anti-entropy sees a healthy
         * partition, and the column reads TSDB_ERR_CORRUPT forever with nothing
         * left to self-heal it — round 1 of the repair writes <part>/.ordmap, so
         * rounds 2 and 3 translate onto the same local ordinals and land
         * idempotently on the same wrong answer.  The state is self-locking.
         *
         * So hand the decision to the legacy content rule the function already
         * has.  It is not weaker: it still refuses (ambiguous) when the ts keys
         * repeat, which is the only case where the placement would be a guess.
         *
         * `matched == 0` is the whole trigger.  A PARTIAL match is real evidence
         * — the spaces provably overlap — and keeps its meaning: the unmatched
         * slots are genuinely missing blocks. */
        if (!mismatched && matched == 0) {
            ord_retry_legacy = 1;
            for (size_t b = 0; b < nb_ts; b++) pick[b] = PICK_NONE;
        }
    }

    /* The legacy content rule: reached either because this column carries no
     * durable ordinal on every entry, or because the ordinal branch just handed
     * the decision back. */
    const int use_legacy = ord_retry_legacy || (!ord_mode && nb_col > 0);

    if (use_legacy && nb_col < nb_ts) {
        /* LEGACY, short.  A late add is a contiguous positional run of ts, and
         * that is what used to be tested — at the ONE offset a late add would
         * sit at.  On an all-equal-key run every offset satisfies it, so an
         * interior loss was accepted as an ALTER and answered with a zero block
         * plus every surviving block shifted one slot.  Count the offsets that
         * fit instead: exactly one is a forced placement, two or more means the
         * keys repeat and the alignment would be a guess. */
        size_t nmiss = nb_ts - nb_col;
        size_t fits = 0, fit_d = 0;
        for (size_t d = 0; d + nb_col <= nb_ts && fits < 2; d++) {
            int ok = 1;
            for (size_t k = 0; k < nb_col && ok; k++)
                if (!part_meta_agrees(&cm[k], &ts_m[d + k])) ok = 0;
            if (ok) { fits++; fit_d = d; }
        }
        if (fits == 1) {
            for (size_t k = 0; k < nb_col; k++) pick[fit_d + k] = (long)k;
            /* Only a run sitting at the END is the late add whose missing
             * blocks are legitimately zero; anywhere else the gap is loss. */
            alter_shaped = (fit_d == nmiss);
        } else if (fits >= 2) {
            ambiguous = 1;
        } else {
            /* Not a suffix.  Pair by key ONLY where the TS keys are pairwise
             * distinct, which makes the correspondence forced rather than
             * guessed; a duplicate ts key is information-theoretically ambiguous
             * (no remaining field says which ordinal is missing) and is marked
             * unavailable instead.  A duplicate COLUMN key is not ambiguity —
             * both entries describe the same rows — and part_keyref_find takes
             * the last of the run, as the ordinal path does. */
            int lr = part_legacy_pair(ts_m, nb_ts, cm, nb_col, pick);
            if (lr < 0) { free(pick); return TSDB_ERR_NOMEM; }
            if (!lr) ambiguous = 1;
        }
    } else if (use_legacy) {
        /* LEGACY, equal or longer, but the positional check above disagreed.
         * Same rule: forced by TS uniqueness or unavailable, never first-match. */
        int lr = part_legacy_pair(ts_m, nb_ts, cm, nb_col, pick);
        if (lr < 0) { free(pick); return TSDB_ERR_NOMEM; }
        if (!lr) ambiguous = 1;
    }

    tsdb_block_meta_t *merged = malloc(nb_ts * sizeof(*merged));
    if (!merged) { free(pick); return TSDB_ERR_NOMEM; }

    size_t holes = 0, mismatches = 0;
    for (size_t b = 0; b < nb_ts; b++) {
        if (pick[b] >= 0) { merged[b] = cm[pick[b]]; continue; }
        memset(&merged[b], 0, sizeof(merged[b]));
        merged[b].offset = UINT64_MAX;                  /* sentinel */
        merged[b].count  = ts_m[b].count;
        merged[b].ts_min = ts_m[b].ts_min;
        merged[b].ts_max = ts_m[b].ts_max;
        merged[b].codec  = TSDB_CODEC_NONE;
        merged[b].ord    = ts_m[b].ord;
        if (pick[b] == PICK_MISMATCH) {
            merged[b].flags = TSDB_BLOCK_FLAG_MISMATCH; mismatches++;
        } else if (!alter_shaped) {
            merged[b].flags = TSDB_BLOCK_FLAG_HOLE; holes++;
        }
    }
    free(pick);

    if (holes && idx_decl_count[ci] == 0)
        fprintf(stderr,
                "[part] %s: column %s has NO index here at all, but this "
                "partition was written with it (ts stamp=%u, column index=%d); "
                "its %zu block(s) of values are gone and now read as an error, "
                "not as zero\n",
                partition_dir, s->cols[ci].name,
                (unsigned)ts_ncols_stamp, ci, holes);
    else if (holes && ambiguous)
        /* `ambiguous` is only ever reached on the legacy content rule, which is
         * either "at least one entry of THIS column has no marker" or "both
         * sides are marked in spaces that do not meet" (ord_retry_legacy).  Do
         * not name a cause that may not be this partition's: say what actually
         * decided, which is that the keys repeat. */
        fprintf(stderr,
                "[part] %s: column %s declares %u blocks against ts's %u and "
                "the two cannot be paired by durable block ordinal (%s); the "
                "keys repeat, so which block is missing is not recoverable — "
                "%zu ts block(s) now read as an error, not as another block's "
                "values\n",
                partition_dir, s->cols[ci].name,
                idx_decl_count[ci], idx_decl_count[s->ts_col_idx],
                ord_retry_legacy ? "this column is stamped in a space that does "
                                   "not meet ts's"
                                 : "not every entry of this column is stamped",
                holes);
    else if (holes)
        fprintf(stderr,
                "[part] %s: column %s declares %u blocks against ts's %u and "
                "they are not a late-added suffix; %zu ts block(s) have no "
                "value for it and now read as an error, not as zero\n",
                partition_dir, s->cols[ci].name,
                idx_decl_count[ci], idx_decl_count[s->ts_col_idx], holes);

    if (mismatches)
        fprintf(stderr,
                "[part] %s: column %s pairs %zu ts block(s) by ordinal but its "
                "entries describe different rows; the two indexes disagree "
                "about what this partition holds, so those block(s) now read "
                "as an error, not as another block's values\n",
                partition_dir, s->cols[ci].name, mismatches);

    *out_merged = merged;
    return TSDB_OK;
}

/* Classify an open()/fopen()/mmap() failure inside tsdb_part_open.  ENOENT
 * and kin mean the COLUMN has no data here (never flushed, ALTER-added later)
 * — the open loop keeps reading the partition without it, which is the
 * long-standing contract every bare `continue` below implements.  EMFILE /
 * ENFILE / ENOMEM / EAGAIN are different in kind: the data IS on disk but
 * THIS PROCESS ran out of fds / memory / maps while reaching for it.
 * Treating that as "column absent" feeds col_meta_n == 0 into the recovery
 * passes below — for ts the whole partition silently reads as 0 rows (with
 * no log line at all, since both the clamp and the align pass key off a
 * non-empty ts), for a value column the torn-column clamp drives the durable
 * prefix to 0 — and either way tsdb_part_open still returned TSDB_OK, so a
 * scan under fd pressure answered SHORT with rc=0.  Only these transient
 * process-level resource errnos are promoted to a hard TSDB_ERR_IO; they
 * describe the process, not the partition, so failing loudly and letting the
 * caller retry is the only answer that never mis-states the data.
 *
 * EBADF is in the set for one reason: Darwin.  Measured on macOS (Darwin 27),
 * an open()/fopen() blocked by RLIMIT_NOFILE reports EBADF, not EMFILE, once
 * any descriptor slot has been consumed since the limit was set — the very
 * situation a partition-open loop that keeps one col fd per column creates.
 * Promoting it can never mis-classify a legitimately absent column: open(2)
 * and fopen(3) take no fd argument, so POSIX defines no EBADF failure for
 * them, and the mmap fd below was returned by open() two lines earlier — an
 * EBADF from any of these calls is only ever the descriptor-limit anomaly. */
static int part_open_rsrc_exhausted(int e) {
    return e == EMFILE || e == ENFILE || e == ENOMEM || e == EAGAIN
        || e == EBADF;
}

int tsdb_part_open(tsdb_schema_t *s, const char *partition_dir, tsdb_part_t **out) {
    if (!s || !partition_dir || !out) return TSDB_ERR_INVAL;

    part_compact_swap_recover(partition_dir);

    tsdb_part_t *p = calloc(1, sizeof(*p));
    if (!p) return TSDB_ERR_NOMEM;

    p->schema = s;
    snprintf(p->dir, sizeof(p->dir), "%s", partition_dir);
    p->zone_ts_min = INT64_MAX;
    p->zone_ts_max = INT64_MIN;
    p->zone_valid  = 0;
    {
        const char *zc = getenv("TSDB_ZEROCOPY_READ");
        p->zerocopy = !(zc && zc[0] == '0' && zc[1] == '\0');
    }

    for (int i = 0; i < s->ncols; i++)
        p->col_maps[i].fd = -1;

    /* Per-column block count DECLARED by the idx header (before the per-block
     * size filter below drops any block whose .col bytes are torn off the
     * tail).  The alignment pass uses this to tell two different "fewer blocks
     * than ts" situations apart: a genuinely later-added column (its idx has
     * fewer entries → ALTER ADD COLUMN, prepend zero-fill sentinels) vs a
     * column whose idx declares the SAME blocks as ts but whose .col was torn
     * (idx count equal, filtered meta fewer → the missing blocks are the TAIL,
     * NOT the front; prepending would mis-pair surviving rows with the wrong
     * ts and silently return wrong values). */
    uint32_t idx_decl_count[TSDB_MAX_COLS];
    for (int i = 0; i < s->ncols; i++) idx_decl_count[i] = 0;

    /* Column-count stamp carried by the TS column's idx header (part.h): how
     * many columns the schema had when the flush that published ts here ran.
     * 0 for a partition no flush of this vintage ever wrote (legacy on-disk
     * data, or one built purely by replication / bulk import). */
    uint16_t ts_ncols_stamp = TSDB_IDX_NCOLS_UNKNOWN;

    /* Column read order must be the REVERSE of the write order (writers
     * publish ts LAST), or a reader racing a concurrent publish samples
     * val.idx at N and then ts.idx at N+1 and synthesises a HOLE for a block
     * that is not missing at all — a transient TSDB_ERR_CORRUPT on a healthy
     * partition.  Reading ts FIRST can only ever see ts <= the other columns,
     * which the alignment pass treats as "already aligned".  Today this holds
     * only by the accident of ts_col_idx == 0; make it explicit. */
    int open_iter[TSDB_MAX_COLS];
    int open_n = 0;
    {
        int tsc = s->ts_col_idx;
        if (tsc >= 0 && tsc < s->ncols) open_iter[open_n++] = tsc;
        for (int i = 0; i < s->ncols; i++)
            if (i != tsc) open_iter[open_n++] = i;
    }

    for (int ix = 0; ix < open_n; ix++) {
        int ci = open_iter[ix];
        /* ── Parquet-footer open order ──────────────────────────────────
         * 1. Read idx fully into a scratch buffer (idx is the atomic
         *    manifest — published via rename by col_writer_close).
         * 2. Compute the max byte offset any entry references.
         * 3. Open col + fstat.  Writer orders data-fflush BEFORE idx
         *    rename, so if we observed an idx entry referencing byte N,
         *    the col file is durable up to N on disk.
         * 4. mmap col at current size — always covers max_end.
         * Zero locks, no reader-writer visibility gap. */
        char idx_path[4096];
        snprintf(idx_path, sizeof(idx_path), "%s/%s.idx",
                 partition_dir, s->cols[ci].name);

        FILE *idx_f = fopen(idx_path, "rb");
        if (!idx_f) {
            int e = errno;
            if (part_open_rsrc_exhausted(e)) {
                fprintf(stderr,
                        "[part] %s/%s.idx: open failed (errno=%d %s); "
                        "resource exhaustion — failing the partition open "
                        "instead of silently dropping the column\n",
                        partition_dir, s->cols[ci].name, e, strerror(e));
                tsdb_part_close(p);
                return TSDB_ERR_IO;
            }
            continue;
        }
        tsdb_iopolicy_advise_seq_fd(tsdb_iopolicy_detect(partition_dir),
                                     fileno(idx_f));

        uint8_t idx_hdr[TSDB_IDX_HEADER_SIZE];
        size_t hdr_n = fread(idx_hdr, 1, TSDB_IDX_HEADER_SIZE, idx_f);
        uint32_t block_count  = 0;
        uint16_t idx_version  = 0;
        uint64_t total_rows_u = 0;
        int64_t  fmn = 0, fmx = 0;
        uint32_t entry_size   = 0;
        int      hdr_size = read_idx_header_ex(idx_hdr, hdr_n,
                                                &block_count, &idx_version,
                                                &total_rows_u, &fmn, &fmx,
                                                &entry_size, NULL);
        if (hdr_size < 0) {
            /* The idx EXISTS but this binary cannot parse it.  Skipping the
             * column reads 0 blocks for it, and 0 blocks is not distinguishable
             * downstream from "this column was added by a later ALTER": for the
             * ts column the whole partition answers 0 rows, and for a non-ts
             * column the alignment pass below zero-fills, FABRICATING a value
             * for every row at rc=0 — which also hides the loss from
             * anti-entropy, since it compares count/max(ts).  Fail loudly, like
             * the torn-.col and resource-exhaustion legs, so the partition can
             * be repaired instead of silently reading empty. */
            fprintf(stderr,
                    "[part] %s/%s.idx: unparseable index header (corrupt magic "
                    "or unknown version); failing the partition open instead "
                    "of reading the column as empty\n",
                    partition_dir, s->cols[ci].name);
            fclose(idx_f);
            tsdb_part_close(p);
            return TSDB_ERR_CORRUPT;
        }
        if (block_count == 0 || entry_size == 0) {
            fclose(idx_f); continue;
        }
        if (ci == s->ts_col_idx)
            ts_ncols_stamp = idx_hdr_ncols(idx_hdr, hdr_n, idx_version);

        /* Mixed-writer recovery: if the version-derived header size does not
         * match where the fixed-stride entries actually start (a V3/V4 header
         * mongrel produced by the flush + raw-block writers disagreeing), the
         * entries would be read at the wrong offset → garbage offsets → every
         * block filtered → 0 rows.  Re-derive the header size from the idx
         * file length so entry0 is read where it really lives. */
        {
            struct stat ist;
            if (fstat(fileno(idx_f), &ist) == 0 && ist.st_size > 0) {
                hdr_size = idx_recover_header_size(hdr_size, entry_size,
                                                   block_count,
                                                   (uint64_t)ist.st_size,
                                                   idx_path);
            }
        }

        if (fseek(idx_f, (long)hdr_size, SEEK_SET) != 0) {
            fclose(idx_f); continue;
        }

        /* Read all entries up-front so we can finalise col_metas later
         * against the mmap snapshot in one consistent pass.  Buffer uses
         * the file's `entry_size` (40 for V1/V2, 88 for V3). */
        uint8_t *entries = malloc((size_t)block_count * entry_size);
        if (!entries) { fclose(idx_f); tsdb_part_close(p); return TSDB_ERR_NOMEM; }
        size_t nread = fread(entries, entry_size,
                             (size_t)block_count, idx_f);
        fclose(idx_f);
        if (nread == 0) { free(entries); continue; }

        /* Record how many blocks this column's idx declares (what the writer
         * published), independent of how many survive the .col size filter. */
        idx_decl_count[ci] = (uint32_t)nread;

        uint64_t max_end = 0;
        for (size_t i = 0; i < nread; i++) {
            uint64_t off = get_u64le(entries + i * entry_size);
            uint32_t sz  = get_u32le(entries + i * entry_size + 8);
            uint64_t end = off + TSDB_BLOCK_HEADER_SIZE + (uint64_t)sz;
            if (end > max_end) max_end = end;
        }

        /* Open col file AFTER reading idx — this is the ordering that
         * makes read-side consistency a local property of this loop. */
        char col_path[4096];
        snprintf(col_path, sizeof(col_path), "%s/%s.col",
                 partition_dir, s->cols[ci].name);

        int fd = open(col_path, O_RDONLY);
        if (fd < 0) {
            int e = errno;
            free(entries);
            if (part_open_rsrc_exhausted(e)) {
                fprintf(stderr,
                        "[part] %s/%s.col: open failed (errno=%d %s); "
                        "resource exhaustion — failing the partition open "
                        "instead of silently dropping the column\n",
                        partition_dir, s->cols[ci].name, e, strerror(e));
                tsdb_part_close(p);
                return TSDB_ERR_IO;
            }
            continue;
        }

        struct stat st;
        if (fstat(fd, &st) < 0 || st.st_size == 0) {
            free(entries); close(fd); continue;
        }

        /* Writer ordering guarantee: data durable before idx publish, so in the
         * normal flush path st.st_size >= max_end always holds.  When it does
         * NOT (a .col torn shorter than the idx references — e.g. a crash mid
         * flush, or an interrupted/racing anti-entropy truncate+re-pull), do
         * NOT drop the whole column and read 0 rows for an entire partition
         * that still has durable data: the per-block filter below already skips
         * any block whose end exceeds the mapped size, so falling through reads
         * the intact durable prefix and discards only the torn tail.  Returning
         * 0 for a partition whose .idx header counts millions of rows is the
         * worse failure — a SELECT silently loses on-disk data. */
        if ((uint64_t)st.st_size < max_end) {
            fprintf(stderr,
                    "[part] %s/%s.col torn (size=%llu < idx max_end=%llu); "
                    "reading durable prefix only\n",
                    partition_dir, s->cols[ci].name,
                    (unsigned long long)st.st_size,
                    (unsigned long long)max_end);
        }

        void *map = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (map == MAP_FAILED) {
            int e = errno;
            free(entries);
            close(fd);
            if (part_open_rsrc_exhausted(e)) {
                fprintf(stderr,
                        "[part] %s/%s.col: mmap failed (errno=%d %s); "
                        "resource exhaustion — failing the partition open "
                        "instead of silently dropping the column\n",
                        partition_dir, s->cols[ci].name, e, strerror(e));
                tsdb_part_close(p);
                return TSDB_ERR_IO;
            }
            continue;
        }

        tsdb_iopolicy_advise_read(tsdb_iopolicy_detect(partition_dir),
                                  map, (size_t)st.st_size);

        p->col_maps[ci].fd       = fd;
        p->col_maps[ci].map      = (uint8_t *)map;
        p->col_maps[ci].map_size = (size_t)st.st_size;

        if (idx_version >= 2) {
            if (fmn < p->zone_ts_min) p->zone_ts_min = fmn;
            if (fmx > p->zone_ts_max) p->zone_ts_max = fmx;
            p->zone_valid = 1;
        }

        p->col_metas[ci] = malloc((size_t)nread * sizeof(tsdb_block_meta_t));
        if (!p->col_metas[ci]) { free(entries); tsdb_part_close(p); return TSDB_ERR_NOMEM; }

        /* Filter retained as defence-in-depth for crash-recovery edge
         * cases where idx is newer than col (incomplete flush, legacy
         * non-atomic idx).  With the new write path it should be a
         * no-op on every entry. */
        size_t map_sz = p->col_maps[ci].map_size;
        int    stats_present = (entry_size >= TSDB_IDX_ENTRY_SIZE);
        for (size_t i = 0; i < nread; i++) {
            uint8_t *entry  = entries + i * entry_size;
            uint64_t offset = get_u64le(entry + 0);
            uint32_t bsize  = get_u32le(entry + 8);
            uint64_t end = offset + TSDB_BLOCK_HEADER_SIZE + (uint64_t)bsize;
            if (end > (uint64_t)map_sz) continue;
            size_t slot = p->col_meta_n[ci];
            tsdb_block_meta_t *m = &p->col_metas[ci][slot];
            memset(m, 0, sizeof(*m));
            m->offset = offset;
            m->size   = bsize;
            m->count  = get_u32le(entry + 12);
            m->ts_min = get_i64le(entry + 16);
            m->ts_max = get_i64le(entry + 24);
            m->bloom  = get_u64le(entry + 32);
            m->codec  = TSDB_CODEC_NONE;
            m->flags  = 0;
            if (stats_present) {
                m->stats_min   = get_i64le(entry + 40);
                m->stats_max   = get_i64le(entry + 48);
                m->stats_sum   = get_i64le(entry + 56);
                m->stats_first = get_i64le(entry + 64);
                m->stats_last  = get_i64le(entry + 72);
                m->stats_flags = get_u16le(entry + 80);
            }
            /* The durable ordinal, when this writer recorded one.  NOTE `i`,
             * not `slot`: the ordinal describes the ENTRY, and the size filter
             * above can drop entries, so the surviving array's positions are
             * not the file's positions. */
            m->ord = read_idx_entry_ord(entry, entry_size);
            p->col_meta_n[ci]++;
        }
        free(entries);

        /* Back-fill codec from block headers. */
        for (size_t b = 0; b < p->col_meta_n[ci]; b++) {
            uint64_t off = p->col_metas[ci][b].offset;
            if (off + TSDB_BLOCK_HEADER_SIZE <= p->col_maps[ci].map_size) {
                uint8_t  codec = 0;
                uint16_t flags = 0;
                uint32_t count = 0, dsz = 0;
                int64_t  tmin = 0, tmax = 0;
                if (read_block_header(p->col_maps[ci].map + off,
                                      &codec, &flags, &count,
                                      &tmin, &tmax, &dsz) == TSDB_OK) {
                    p->col_metas[ci][b].codec = codec;
                    p->col_metas[ci][b].flags = flags;
                }
            }
        }
    }

    /* ─── Torn non-ts column: clamp the partition to its durable block prefix ─
     * A non-ts column whose idx declares the SAME block count as ts but whose
     * .col was truncated (crash mid-flush, or an interrupted/racing
     * anti-entropy truncate+re-pull on one file) loses its TAIL blocks to the
     * per-block size filter above.  Those rows have no recoverable value for
     * that column.  Prepending sentinels (the ALTER path below) would be WRONG
     * here: it assumes the missing blocks are the column's leading/oldest ones,
     * so it would shift the surviving real blocks to align with the LAST ts
     * blocks and zero-fill the front — silently returning wrong values for
     * every row (count stays right; the cells are mis-paired).
     *
     * Correct behaviour, matching the torn-ts case (tsdb_part_open already
     * reads only the durable .col prefix for ts): expose only the block prefix
     * for which EVERY column is durable.  Blocks are appended in ts order, so
     * a tail truncation drops the highest-offset (newest) blocks; the durable
     * prefix is the first K blocks, K = min readable block count over columns
     * whose idx is fully present.  Clamp ts (the block enumerator) and every
     * such column to K. */
    int ts_ci = s->ts_col_idx;
    if (ts_ci >= 0 && ts_ci < s->ncols && p->col_meta_n[ts_ci] > 0) {
        size_t durable_prefix = p->col_meta_n[ts_ci];
        for (int ci = 0; ci < s->ncols; ci++) {
            if (ci == ts_ci) continue;
            /* Only columns whose idx is at least as long as ts's idx can be
             * "torn": a shorter idx means the column was genuinely added later
             * (ALTER ADD COLUMN) and is handled by the sentinel pass below. */
            if (idx_decl_count[ci] >= idx_decl_count[ts_ci] &&
                p->col_meta_n[ci] < durable_prefix) {
                durable_prefix = p->col_meta_n[ci];
            }
        }
        if (durable_prefix < p->col_meta_n[ts_ci]) {
            fprintf(stderr,
                    "[part] %s: a non-ts column is torn shorter than ts; "
                    "clamping partition to durable block prefix (%zu of %zu "
                    "blocks) to avoid mis-paired rows\n",
                    partition_dir, durable_prefix, p->col_meta_n[ts_ci]);
            /* Clamp ts and every column whose idx matched ts (the fully-present
             * columns) down to the durable prefix.  A genuinely-shorter ALTER
             * column keeps its own (smaller) count and is padded below. */
            p->col_meta_n[ts_ci] = durable_prefix;
            for (int ci = 0; ci < s->ncols; ci++) {
                if (ci == ts_ci) continue;
                if (idx_decl_count[ci] >= idx_decl_count[ts_ci] &&
                    p->col_meta_n[ci] > durable_prefix) {
                    p->col_meta_n[ci] = durable_prefix;
                }
            }
        }
    }

    /* ─── ALTER TABLE ADD COLUMN support ───────────────────────────────────
     * Columns added after earlier flushes have fewer (or zero) blocks than
     * the TS column for this partition. Pad the front with synthetic
     * block-meta records so readers that walk TS-aligned blocks always
     * find a matching entry. Sentinel: offset=UINT64_MAX, codec=TSDB_CODEC_NONE,
     * no col file mapped for the synthesised block range.
     * tsdb_part_read_block zero-fills when it sees the sentinel.
     *
     * Only a column whose IDX genuinely declares fewer blocks than ts is an
     * ALTER-added column; a same-idx-count column that merely lost tail blocks
     * to a tear was already clamped (handled above), so it can't reach here
     * with a short count and be mistaken for a late add.
     *
     * The idx count is necessary but NOT sufficient, because it says nothing
     * about WHERE the shortfall is.  A second producer reaches this gate: the
     * per-column raw-block replication hook is applied one (column, block) at
     * a time and its errors are discarded (see the w->raw_block_fn call in the
     * flush path above), so a peer — or a half-finished migration — can end up
     * with a val.idx that simply never learned about a block ts.idx has.  That
     * gap can be anywhere, and prepending assumes it is at the FRONT: every
     * surviving real block then sits one slot too far and the reader, which
     * pairs by (ts_min,count), matches the partition's leading ts blocks
     * against fabricated sentinels — a pruned query returns a whole block of
     * silent zeros with rc=0, and any ts block left unpaired kills the query.
     *
     * So verify the ALTER hypothesis against CONTENT before acting on it: a
     * genuinely later-added column's real blocks are a contiguous SUFFIX of
     * ts's block list, block for block, on the same key the reader pairs with.
     * When that holds, prepend as before.  When it does not, pair by content
     * and mark every ts block the column has no block for as a HOLE.
     *
     * Either way the column array comes out 1:1 with ts, so no ts block is
     * ever hidden: `SELECT ts`, count(*), max(ts) and every healthy column
     * keep returning exactly what they return on an intact partition.  The
     * difference is what an unanswerable cell reads as — a HOLE is
     * TSDB_ERR_CORRUPT, not a zero.  Zero is a value; fabricating it silently
     * poisons every aggregate and hides the loss from anti-entropy, which
     * compares count/max(ts). */
    if (ts_ci >= 0 && ts_ci < s->ncols && p->col_meta_n[ts_ci] > 0) {
        size_t nb_ts = p->col_meta_n[ts_ci];
        const tsdb_block_meta_t *ts_m = p->col_metas[ts_ci];
        for (int ci = 0; ci < s->ncols; ci++) {
            if (ci == ts_ci) continue;
            tsdb_block_meta_t *merged = NULL;
            int arc = part_align_column(s, ci, partition_dir,
                                        ts_m, nb_ts,
                                        p->col_metas[ci], p->col_meta_n[ci],
                                        idx_decl_count, ts_ncols_stamp,
                                        &merged);
            if (arc != TSDB_OK) { tsdb_part_close(p); return arc; }
            if (!merged) continue;                 /* already exactly aligned */
            free(p->col_metas[ci]);
            p->col_metas[ci]  = merged;
            p->col_meta_n[ci] = nb_ts;
        }
    }

    *out = p;
    return TSDB_OK;
}

void tsdb_part_close(tsdb_part_t *p) {
    if (!p) return;
    for (int i = 0; i < p->schema->ncols; i++) {
        if (p->col_maps[i].map) munmap(p->col_maps[i].map, p->col_maps[i].map_size);
        if (p->col_maps[i].fd >= 0) close(p->col_maps[i].fd);
        free(p->col_metas[i]);
    }
    free(p);
}

/* Borrowed view of the array tsdb_part_open built.  See the lifetime and
 * immutability contract on the declaration in part.h — the whole reason this
 * is safe is that nothing below tsdb_part_open ever writes col_metas[] again,
 * and only tsdb_part_close frees it. */
int tsdb_part_col_blocks_ref(tsdb_part_t *p, int col_idx,
                             const tsdb_block_meta_t **out_arr, size_t *out_n)
{
    if (!p || col_idx < 0 || col_idx >= p->schema->ncols || !out_arr || !out_n)
        return TSDB_ERR_INVAL;

    size_t n = p->col_meta_n[col_idx];
    *out_n   = n;
    /* Report an empty column as (NULL, 0) rather than a possibly non-NULL
     * malloc(0), so the two variants are indistinguishable to a caller. */
    *out_arr = n ? p->col_metas[col_idx] : NULL;
    return TSDB_OK;
}

/* Copying variant, kept for callers whose metadata outlives the part handle
 * or who mutate it.  Expressed in terms of the borrowing one so the two
 * cannot drift on bounds checking or on the empty-column result. */
int tsdb_part_col_blocks(tsdb_part_t *p, int col_idx,
                         tsdb_block_meta_t **out_arr, size_t *out_n)
{
    const tsdb_block_meta_t *src = NULL;
    size_t n = 0;
    /* This variant's own out-params are not the ones ref validated. */
    if (!out_arr || !out_n) return TSDB_ERR_INVAL;
    int rc = tsdb_part_col_blocks_ref(p, col_idx, &src, &n);
    if (rc != TSDB_OK) return rc;

    *out_n = n;
    if (n == 0) { *out_arr = NULL; return TSDB_OK; }

    *out_arr = malloc(n * sizeof(tsdb_block_meta_t));
    if (!*out_arr) return TSDB_ERR_NOMEM;
    memcpy(*out_arr, src, n * sizeof(tsdb_block_meta_t));
    return TSDB_OK;
}

/*
 * Locate + validate one block inside the column mmap: bounds-check the
 * idx-derived meta against the mapping, parse the 32-byte block header,
 * cross-check it, and verify the CRC32C trailer when the writer marked
 * one.  On success, *out_data and *out_dsize describe the (still encoded)
 * payload bytes INSIDE the mapping.  Shared by the copying reader
 * (tsdb_part_read_block) and the zero-copy reader
 * (tsdb_part_read_block_ref) so both enforce identical integrity checks.
 */
static int part_block_locate(tsdb_part_t *p, int col_idx,
                             const tsdb_block_meta_t *meta,
                             uint8_t *out_codec, uint16_t *out_flags,
                             const uint8_t **out_data, uint32_t *out_dsize)
{
    if (!p->col_maps[col_idx].map) return TSDB_ERR_NOTFOUND;

    uint64_t off    = meta->offset;
    size_t   map_sz = p->col_maps[col_idx].map_size;

    if (off + TSDB_BLOCK_HEADER_SIZE > map_sz) return TSDB_ERR_CORRUPT;

    const uint8_t *hdr_ptr = p->col_maps[col_idx].map + off;
    uint8_t  codec = 0;
    uint16_t flags = 0;
    uint32_t count = 0, data_size = 0;
    int64_t  ts_min = 0, ts_max = 0;
    int rc = read_block_header(hdr_ptr, &codec, &flags, &count,
                               &ts_min, &ts_max, &data_size);
    (void)ts_min; (void)ts_max;
    if (rc != TSDB_OK) return rc;

    /* Cross-check the header against the idx-derived meta.  The writer
     * emits both from the same values, so a mismatch means the caller's
     * idx snapshot is paired with a different generation of the .col
     * file (truncate + re-pull or a compactor swap replaced the pair
     * between the idx read and the col mmap).  The output buffer is sized
     * from meta->count; decoding header `count` values into it would
     * overrun the heap — same class the compactor read loop already
     * guards. */
    if (count != meta->count || data_size != meta->size)
        return TSDB_ERR_CORRUPT;

    if (off + TSDB_BLOCK_HEADER_SIZE + data_size > map_sz) return TSDB_ERR_CORRUPT;

    const uint8_t *data_ptr = hdr_ptr + TSDB_BLOCK_HEADER_SIZE;

    /* Verify trailing CRC32C when the writer marked it.  Old blocks
     * without the flag pass through unverified for backward compat.
     * Verification reads the mapping in place — no copy either way. */
    if (flags & TSDB_BLOCK_FLAG_HAS_CRC) {
        if (off + TSDB_BLOCK_HEADER_SIZE + data_size + TSDB_BLOCK_CRC_TRAILER_SIZE
            > map_sz)
            return TSDB_ERR_CORRUPT;
        uint32_t expected = tsdb_crc32c(hdr_ptr, TSDB_BLOCK_HEADER_SIZE);
        if (data_size > 0)
            expected = tsdb_crc32c_update(expected, data_ptr, data_size);
        uint32_t stored = get_u32le(data_ptr + data_size);
        if (expected != stored) return TSDB_ERR_CORRUPT;
    }

    *out_codec = codec;
    *out_flags = flags;
    *out_data  = data_ptr;
    *out_dsize = data_size;
    return TSDB_OK;
}

int tsdb_part_read_block(tsdb_part_t *p, int col_idx,
                         const tsdb_block_meta_t *meta, void *out_buf)
{
    if (!p || col_idx < 0 || col_idx >= p->schema->ncols || !meta || !out_buf)
        return TSDB_ERR_INVAL;

    /* ALTER TABLE ADD COLUMN sentinel: offset == UINT64_MAX marks a block
     * that pre-dates the column's creation — zero-fill with the type's
     * default value. Works whether the column file is mapped or not.
     *
     * The HOLE and MISMATCH flags mark the other two producers of a missing
     * block: the column DID exist for these rows, it either lost the block or
     * its index disagrees about which rows the block holds (see
     * tsdb_part_open).  The value is unknown, so refuse rather than invent
     * one. */
    if (meta->offset == UINT64_MAX) {
        if (meta->flags & TSDB_BLOCK_FLAG_NO_VALUE) return TSDB_ERR_CORRUPT;
        tsdb_type_t type = p->schema->cols[col_idx].type;
        size_t w = tsdb_type_width(type);
        memset(out_buf, 0, (size_t)meta->count * w);
        return TSDB_OK;
    }

    uint8_t  codec = 0;
    uint16_t flags = 0;
    const uint8_t *data_ptr = NULL;
    uint32_t data_size = 0;
    int rc = part_block_locate(p, col_idx, meta,
                               &codec, &flags, &data_ptr, &data_size);
    if (rc != TSDB_OK) return rc;

    return tsdb_codec_decode_adaptive((tsdb_codec_t)codec,
                                      p->schema->cols[col_idx].type, flags,
                                      data_ptr, data_size,
                                      out_buf, meta->count);
}

int tsdb_part_read_block_ref(tsdb_part_t *p, int col_idx,
                             const tsdb_block_meta_t *meta,
                             const void **out_data, void **out_owned)
{
    if (!p || col_idx < 0 || col_idx >= p->schema->ncols || !meta ||
        !out_data || !out_owned)
        return TSDB_ERR_INVAL;

    *out_data  = NULL;
    *out_owned = NULL;

    tsdb_type_t type = p->schema->cols[col_idx].type;
    size_t w = tsdb_type_width(type);
    if (w == 0) return TSDB_ERR_UNSUPPORTED;
    size_t need = (size_t)meta->count * w;

    /* ALTER ADD COLUMN sentinel: there are no on-disk bytes to point at —
     * materialise the zero-fill into an owned buffer.  A HOLE is a lost
     * block and a MISMATCH is an index that disagrees about the rows, not a
     * pre-dating one: neither has a value to serve. */
    if (meta->offset == UINT64_MAX) {
        if (meta->flags & TSDB_BLOCK_FLAG_NO_VALUE) return TSDB_ERR_CORRUPT;
        void *buf = calloc(1, need ? need : 1);
        if (!buf) return TSDB_ERR_NOMEM;
        *out_owned = buf;
        *out_data  = buf;
        return TSDB_OK;
    }

    uint8_t  codec = 0;
    uint16_t flags = 0;
    const uint8_t *data_ptr = NULL;
    uint32_t data_size = 0;
    int rc = part_block_locate(p, col_idx, meta,
                               &codec, &flags, &data_ptr, &data_size);
    if (rc != TSDB_OK) return rc;

    /* Zero-copy fast path: a RAW block's payload IS the decoded column
     * slice, so hand out a pointer into the mmap instead of memcpy'ing
     * into a scratch buffer.  Gates:
     *   - codec RAW/NONE and no outer-LZ wrap (bytes are verbatim values);
     *   - payload covers all meta->count values;
     *   - payload naturally aligned for the type (w is 4 or 8): blocks
     *     from the current writer start 8-aligned (see the pad in
     *     col_writer_write_block); unaligned legacy blocks fall back to
     *     the copy below rather than risking misaligned typed loads.
     * LIFETIME: the pointer aliases p's PROT_READ MAP_PRIVATE mapping and
     * is valid until tsdb_part_close(p).  The query executor consumes
     * block buffers strictly before scan_plan_free() closes the plan's
     * partitions (workers are pool_wait()ed first — see exec.c), which is
     * what makes the handoff safe there. */
    if (p->zerocopy &&
        (codec == TSDB_CODEC_RAW || codec == TSDB_CODEC_NONE) &&
        !(flags & TSDB_BF_OUTER_LZ) &&
        (size_t)data_size >= need &&
        ((uintptr_t)data_ptr & (w - 1)) == 0) {
        __atomic_fetch_add(&g_zerocopy_hits, 1, __ATOMIC_RELAXED);
        *out_data = data_ptr;
        return TSDB_OK;
    }

    void *buf = malloc(need ? need : 1);
    if (!buf) return TSDB_ERR_NOMEM;
    rc = tsdb_codec_decode_adaptive((tsdb_codec_t)codec, type, flags,
                                    data_ptr, data_size, buf, meta->count);
    if (rc != TSDB_OK) { free(buf); return rc; }
    __atomic_fetch_add(&g_zerocopy_fallbacks, 1, __ATOMIC_RELAXED);
    *out_owned = buf;
    *out_data  = buf;
    return TSDB_OK;
}

void tsdb_part_col_map(const tsdb_part_t *p, int col_idx,
                        const uint8_t **out_map, size_t *out_len)
{
    if (!p || !out_map || !out_len) return;
    *out_map = NULL;
    *out_len = 0;
    if (col_idx < 0 || col_idx >= p->schema->ncols) return;
    *out_map = p->col_maps[col_idx].map;
    *out_len = p->col_maps[col_idx].map_size;
}

/* Schema accessor for the Parquet exporter (see src/storage/parquet.c). */
tsdb_schema_t *tsdb_part_schema(const tsdb_part_t *p) {
    return p ? p->schema : NULL;
}

int tsdb_part_zone_map(tsdb_part_t *p, int64_t *out_ts_min, int64_t *out_ts_max) {
    if (!p || !out_ts_min || !out_ts_max) return TSDB_ERR_INVAL;

    if (p->zone_valid) {
        *out_ts_min = p->zone_ts_min;
        *out_ts_max = p->zone_ts_max;
        return TSDB_OK;
    }

    /* v1-idx fallback: compute from the ts column's per-block metadata. */
    int ts_col = p->schema->ts_col_idx;
    if (ts_col < 0 || ts_col >= p->schema->ncols) return TSDB_ERR_NOTFOUND;
    if (p->col_meta_n[ts_col] == 0) return TSDB_ERR_NOTFOUND;

    int64_t mn = INT64_MAX, mx = INT64_MIN;
    for (size_t b = 0; b < p->col_meta_n[ts_col]; b++) {
        if (p->col_metas[ts_col][b].ts_min < mn) mn = p->col_metas[ts_col][b].ts_min;
        if (p->col_metas[ts_col][b].ts_max > mx) mx = p->col_metas[ts_col][b].ts_max;
    }
    /* Memoize for subsequent calls. */
    p->zone_ts_min = mn;
    p->zone_ts_max = mx;
    p->zone_valid  = 1;

    *out_ts_min = mn;
    *out_ts_max = mx;
    return TSDB_OK;
}
