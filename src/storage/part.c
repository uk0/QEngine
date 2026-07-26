/* part.c — disk partition flush and read. */

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
                               uint64_t max_seq) {
    /* Emit v4 (48 bytes, carries max_seq) ONLY when a WAL redo checkpoint is
     * present.  With max_seq == 0 (the default flush-on-commit path) emit a
     * byte-identical v3 header, so default-mode partitions stay unchanged. */
    put_u32le(buf + 0,  TSDB_IDX_MAGIC);
    put_u32le(buf + 4,  count);
    put_u64le(buf + 12, total_rows);
    put_i64le(buf + 20, file_ts_min);
    put_i64le(buf + 28, file_ts_max);
    put_u16le(buf + 36, (uint16_t)TSDB_IDX_ENTRY_SIZE);  /* entry_size */
    put_u16le(buf + 38, 0u);                              /* stats_variant */
    if (max_seq == 0) {
        put_u16le(buf + 8,  3);                           /* version = 3 */
        put_u16le(buf + 10, 0);
        return TSDB_IDX_HEADER_SIZE_V3;
    }
    put_u16le(buf + 8,  4);                               /* version = 4 */
    put_u16le(buf + 10, 0);
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
                                  uint64_t max_seq)
{
    return write_idx_header(buf, count, total_rows,
                            file_ts_min, file_ts_max, max_seq);
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
    if (!f) return 0;

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
 *   [82..87]  _pad         6 bytes   (reserved)
 *
 * The stats payload is written unconditionally at V3 so the layout is
 * uniform; for SYMBOL columns stats_flags == 0 and the fields are 0 —
 * those columns are served from the existing bloom filter instead.
 */
static size_t write_idx_entry(uint8_t *buf,
                              uint64_t offset, uint32_t size, uint32_t count,
                              int64_t ts_min, int64_t ts_max,
                              uint64_t bloom,
                              const tsdb_block_meta_t *stats)
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
    return TSDB_IDX_ENTRY_SIZE;
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

    char     idx_path[4096];
    char     col_path[4096];   /* for rollback-by-path on a failed close */

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

    snprintf(w->col_path, sizeof(w->col_path), "%s/%s.col", part_dir, col_name);
    snprintf(w->idx_path, sizeof(w->idx_path), "%s/%s.idx", part_dir, col_name);

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
     * file-level zone map (v2 only). */
    w->has_zone    = 0;
    w->file_ts_min = INT64_MAX;
    w->file_ts_max = INT64_MIN;
    {
        FILE *idx_r = fopen(w->idx_path, "rb");
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
            if (hsz > 0 && esz > 0) {
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

    /* Raw-block replication hook: fire BEFORE freeing comp_buf. */
    if (w->raw_block_fn) {
        tsdb_block_meta_t meta = stats;        /* carry stats to peers */
        meta.offset = w->col_offset;
        meta.size   = (uint32_t)comp_bytes;
        meta.count  = (uint32_t)count;
        meta.ts_min = ts_min;
        meta.ts_max = ts_max;
        meta.codec  = (uint8_t)codec_used;
        meta.flags  = blk_flags;
        w->raw_block_fn(w->raw_block_ud, w->raw_block_db,
                        w->raw_block_table, w->raw_block_day,
                        (uint16_t)w->col_idx, &meta,
                        comp_buf, (size_t)comp_bytes);
        /* Errors are intentionally ignored here — local write must proceed. */
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
                    (uint32_t)count, ts_min, ts_max, bloom, &stats);
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
        /* Read all existing entries from old idx (if any).  Handles v1 /
         * v2 (40-byte entries) and v3 (88-byte entries) transparently —
         * we widen legacy entries to V3 on write-out so the resulting
         * file is single-format. */
        uint8_t *old_entries_v3 = NULL;   /* widened to V3 layout */
        uint32_t old_count      = 0;

        {
            FILE *idx_r = fopen(w->idx_path, "rb");
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
                                old_count = 0;
                            }
                        } else {
                            old_count = 0;
                        }
                        free(old_raw);
                    }
                }
                fclose(idx_r);
            }
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
                                               fmn, fmx, w->max_seq);
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
            }
            free(old_entries_v3);
        }
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

            /* Wire up raw-block hook if available. */
            w.raw_block_fn    = raw_fn;
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

    for (int ci = 0; ci < s->ncols; ci++) {
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
        if (!idx_f) continue;
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
        if (hdr_size < 0 || block_count == 0 || entry_size == 0) {
            fclose(idx_f); continue;
        }

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
        if (fd < 0) { free(entries); continue; }

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
        if (map == MAP_FAILED) { free(entries); close(fd); continue; }

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
            if (ci == ts_ci)                  continue;
            size_t nb_col = p->col_meta_n[ci];
            if (nb_col >= nb_ts)              continue;   /* already aligned */
            /* Skip a torn (not late-added) column: its idx is as long as ts's,
             * so the shortfall is a tail tear, not missing leading blocks.
             * The clamp above already bounded the partition to the prefix
             * these blocks live in. */
            if (idx_decl_count[ci] >= idx_decl_count[ts_ci]) continue;

            size_t nmiss = nb_ts - nb_col;
            const tsdb_block_meta_t *cm = p->col_metas[ci];

            /* Is the shortfall shaped like a late add?  Compare against ts on
             * the reader's own key. */
            int alter_shaped;
            if (nb_col > 0) {
                alter_shaped = 1;
                for (size_t b = 0; b < nb_col; b++) {
                    if (cm[b].ts_min != ts_m[nmiss + b].ts_min ||
                        cm[b].count  != ts_m[nmiss + b].count) {
                        alter_shaped = 0; break;
                    }
                }
            } else {
                /* No readable blocks at all, so there is no content to check.
                 * That is only credible as a late add when the idx declares
                 * none either — a column whose idx declares blocks the .col
                 * filter then dropped LOST them, and zero-filling the whole
                 * column would fabricate every one of its values. */
                alter_shaped = (idx_decl_count[ci] == 0);
            }

            tsdb_block_meta_t *merged = malloc(nb_ts * sizeof(*merged));
            if (!merged) { tsdb_part_close(p); return TSDB_ERR_NOMEM; }

            size_t holes = 0;
            for (size_t b = 0; b < nb_ts; b++) {
                const tsdb_block_meta_t *use = NULL;
                if (alter_shaped) {
                    if (b >= nmiss) use = &cm[b - nmiss];
                } else {
                    /* Exactly the first-match the readers do, so this pass can
                     * never place a block the reader would fail to find. */
                    for (size_t k = 0; k < nb_col; k++)
                        if (cm[k].ts_min == ts_m[b].ts_min &&
                            cm[k].count  == ts_m[b].count) { use = &cm[k]; break; }
                }
                if (use) { merged[b] = *use; continue; }
                memset(&merged[b], 0, sizeof(merged[b]));
                merged[b].offset = UINT64_MAX;                /* sentinel */
                merged[b].count  = ts_m[b].count;
                merged[b].ts_min = ts_m[b].ts_min;
                merged[b].ts_max = ts_m[b].ts_max;
                merged[b].codec  = TSDB_CODEC_NONE;
                if (!alter_shaped) { merged[b].flags = TSDB_BLOCK_FLAG_HOLE; holes++; }
            }

            if (holes)
                fprintf(stderr,
                        "[part] %s: column %s declares %u blocks against ts's %u "
                        "and they are not a late-added suffix; %zu ts block(s) "
                        "have no value for it and now read as an error, not as "
                        "zero\n",
                        partition_dir, s->cols[ci].name,
                        idx_decl_count[ci], idx_decl_count[ts_ci], holes);

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

int tsdb_part_col_blocks(tsdb_part_t *p, int col_idx,
                         tsdb_block_meta_t **out_arr, size_t *out_n)
{
    if (!p || col_idx < 0 || col_idx >= p->schema->ncols || !out_arr || !out_n)
        return TSDB_ERR_INVAL;

    size_t n = p->col_meta_n[col_idx];
    *out_n = n;
    if (n == 0) { *out_arr = NULL; return TSDB_OK; }

    *out_arr = malloc(n * sizeof(tsdb_block_meta_t));
    if (!*out_arr) return TSDB_ERR_NOMEM;
    memcpy(*out_arr, p->col_metas[col_idx], n * sizeof(tsdb_block_meta_t));
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
     * The HOLE flag marks the other producer of a missing block: the column
     * DID exist for these rows, it just lost the block (see tsdb_part_open).
     * The value is unknown, so refuse rather than invent one. */
    if (meta->offset == UINT64_MAX) {
        if (meta->flags & TSDB_BLOCK_FLAG_HOLE) return TSDB_ERR_CORRUPT;
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
     * block, not a pre-dating one: it has no value to serve. */
    if (meta->offset == UINT64_MAX) {
        if (meta->flags & TSDB_BLOCK_FLAG_HOLE) return TSDB_ERR_CORRUPT;
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
