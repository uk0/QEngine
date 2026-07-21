/* rawblock.c — Raw block replication (primary encode-once, replica write-verbatim). */

#include "rawblock.h"
#include "replica.h"
#include "node.h"
#include "rpc.h"
#include "../storage/db.h"
#include "../storage/schema.h"
#include "../storage/part.h"
#include "../server/proto.h"  /* tsdb_crc32c — match flush-side trailer */
#include "../../include/tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>

/* ---- Extern helpers -------------------------------------------------------- */

extern int tsdb_mkdir_p(const char *path);
extern const char *tsdb_db_data_dir(tsdb_db_t *db);
extern tsdb_table_internal_t *tsdb_db_find_table(tsdb_db_t *db, const char *name);
extern tsdb_schema_t *tsdb_tbl_schema(tsdb_table_internal_t *t);

/* ---- LE helpers ------------------------------------------------------------ */

static inline void rb_put_u8(uint8_t *p, uint8_t v)    { p[0] = v; }
static inline void rb_put_u16(uint8_t *p, uint16_t v)  { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static inline void rb_put_u32(uint8_t *p, uint32_t v)  {
    p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8);
    p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24);
}
static inline void rb_put_i64(uint8_t *p, int64_t v) {
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; i++) { p[i] = (uint8_t)(u & 0xFF); u >>= 8; }
}
static inline uint8_t  rb_get_u8(const uint8_t *p)  { return p[0]; }
static inline uint16_t rb_get_u16(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }
static inline uint32_t rb_get_u32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static inline int64_t  rb_get_i64(const uint8_t *p) {
    uint64_t u = 0;
    for (int i = 7; i >= 0; i--) u = (u << 8) | p[i];
    return (int64_t)u;
}

/* ---- Serialize / parse ----------------------------------------------------- */

/* Stats block on the wire: 5 × i64 + u16 + 6 bytes pad = 48 bytes,
 * matching the V3 index entry tail byte-for-byte so the replica can
 * memcpy the payload straight into its idx file. */
#define RAWBLK_WIRE_STATS_SIZE 48u

int tsdb_rawblock_serialize(const tsdb_rawblock_push_t *r,
                             uint8_t **buf, size_t *len)
{
    if (!r || !buf || !len) return TSDB_ERR_INVAL;

    uint8_t tlen = (uint8_t)strnlen(r->table, 63);
    /* 1 + tlen + 4 + 2 + 1 + 2 + 4 + 8 + 8 + 48 stats + 4 + block_bytes_len */
    size_t total = 1 + tlen + 4 + 2 + 1 + 2 + 4 + 8 + 8
                   + RAWBLK_WIRE_STATS_SIZE + 4 + r->block_bytes_len;

    uint8_t *p = malloc(total);
    if (!p) return TSDB_ERR_NOMEM;

    uint8_t *w = p;
    rb_put_u8(w, tlen); w++;
    memcpy(w, r->table, tlen); w += tlen;
    rb_put_u32(w, r->part_day); w += 4;
    rb_put_u16(w, r->col_idx);  w += 2;
    rb_put_u8(w, r->codec);     w++;
    rb_put_u16(w, r->flags);    w += 2;
    rb_put_u32(w, r->count);    w += 4;
    rb_put_i64(w, r->ts_min);   w += 8;
    rb_put_i64(w, r->ts_max);   w += 8;

    /* stats block */
    memset(w, 0, RAWBLK_WIRE_STATS_SIZE);
    rb_put_i64(w +  0, r->stats_min);
    rb_put_i64(w +  8, r->stats_max);
    rb_put_i64(w + 16, r->stats_sum);
    rb_put_i64(w + 24, r->stats_first);
    rb_put_i64(w + 32, r->stats_last);
    rb_put_u16(w + 40, r->stats_flags);
    w += RAWBLK_WIRE_STATS_SIZE;

    rb_put_u32(w, r->block_bytes_len); w += 4;
    if (r->block_bytes_len > 0 && r->block_bytes)
        memcpy(w, r->block_bytes, r->block_bytes_len);

    *buf = p;
    *len = total;
    return TSDB_OK;
}

int tsdb_rawblock_parse(const uint8_t *buf, size_t len,
                         tsdb_rawblock_push_t *out)
{
    if (!buf || !out) return TSDB_ERR_INVAL;

    const uint8_t *p = buf;
    const uint8_t *end = buf + len;

#define RB_NEED(n) if (p + (n) > end) return TSDB_ERR_CORRUPT

    RB_NEED(1);
    uint8_t tlen = rb_get_u8(p); p++;
    if (tlen > 63) return TSDB_ERR_CORRUPT;
    RB_NEED(tlen);
    memcpy(out->table, p, tlen);
    out->table[tlen] = '\0';
    p += tlen;

    RB_NEED(4); out->part_day = rb_get_u32(p); p += 4;
    RB_NEED(2); out->col_idx  = rb_get_u16(p); p += 2;
    RB_NEED(1); out->codec    = rb_get_u8(p);  p++;
    RB_NEED(2); out->flags    = rb_get_u16(p); p += 2;
    RB_NEED(4); out->count    = rb_get_u32(p); p += 4;
    RB_NEED(8); out->ts_min   = rb_get_i64(p); p += 8;
    RB_NEED(8); out->ts_max   = rb_get_i64(p); p += 8;

    RB_NEED(RAWBLK_WIRE_STATS_SIZE);
    out->stats_min   = rb_get_i64(p +  0);
    out->stats_max   = rb_get_i64(p +  8);
    out->stats_sum   = rb_get_i64(p + 16);
    out->stats_first = rb_get_i64(p + 24);
    out->stats_last  = rb_get_i64(p + 32);
    out->stats_flags = (uint16_t)p[40] | ((uint16_t)p[41] << 8);
    p += RAWBLK_WIRE_STATS_SIZE;

    RB_NEED(4); out->block_bytes_len = rb_get_u32(p); p += 4;
    RB_NEED(out->block_bytes_len);
    out->block_bytes = (uint8_t *)p; /* points into caller's buf */

#undef RB_NEED
    return TSDB_OK;
}

/* ---- BlockHeader helpers (mirrors part.c) ---------------------------------- */

#define RAWBLK_HDR_SIZE 32u
#define RAWBLK_MAGIC    0x314B4C42u /* "BLK1" */

static void rawblk_write_header(uint8_t *hdr,
                                 uint8_t codec, uint16_t flags,
                                 uint32_t count,
                                 int64_t ts_min, int64_t ts_max,
                                 uint32_t data_size)
{
    rb_put_u32(hdr + 0,  RAWBLK_MAGIC);
    rb_put_u8 (hdr + 4,  codec);
    rb_put_u8 (hdr + 5,  0);
    rb_put_u16(hdr + 6,  flags);
    rb_put_u32(hdr + 8,  count);
    rb_put_i64(hdr + 12, ts_min);
    rb_put_i64(hdr + 20, ts_max);
    rb_put_u32(hdr + 28, data_size);
}

/* ---- IDX helpers -----------------------------------------------------------
 *
 * The idx HEADER is now stamped by the ONE shared encoder in the storage
 * layer (tsdb_part_write_idx_header), so the replication path and the flush
 * path produce byte-identical headers and select V3/V4 identically (carrying
 * max_seq forward via tsdb_part_idx_probe).  Only the per-block ENTRY tail is
 * still written here — its 88-byte V3 layout mirrors part.c's BlockIndexEntry
 * so the replica idx is byte-equivalent to the primary's. */

#define RAWBLK_IDX_ENT_SIZE    88u   /* V3/V4 entries */

/* Forward the primary's stats block verbatim.  When the caller doesn't
 * have a meta handy (e.g. v2 replication) `stats_src` is NULL and the
 * stats tail is zeroed — readers treat stats_flags==0 as absent. */
static void rawblk_write_idx_entry(uint8_t *buf,
                                   uint64_t offset, uint32_t size, uint32_t count,
                                   int64_t ts_min, int64_t ts_max,
                                   const tsdb_block_meta_t *stats_src)
{
    memset(buf, 0, RAWBLK_IDX_ENT_SIZE);
    for (int i = 0; i < 8; i++) buf[0+i] = (uint8_t)(((uint64_t)offset >> (i*8)) & 0xFF);
    rb_put_u32(buf + 8,  size);
    rb_put_u32(buf + 12, count);
    rb_put_i64(buf + 16, ts_min);
    rb_put_i64(buf + 24, ts_max);
    /* [32..39] bloom/reserved — callers currently write zero; the block
     * header carries the real bloom for SYMBOL columns. */
    if (stats_src) {
        rb_put_i64(buf + 40, stats_src->stats_min);
        rb_put_i64(buf + 48, stats_src->stats_max);
        rb_put_i64(buf + 56, stats_src->stats_sum);
        rb_put_i64(buf + 64, stats_src->stats_first);
        rb_put_i64(buf + 72, stats_src->stats_last);
        rb_put_u16(buf + 80, stats_src->stats_flags);
    }
}

/* ---- Apply on replica ------------------------------------------------------ */

int tsdb_rawblock_apply(tsdb_db_t *db, const tsdb_rawblock_push_t *r)
{
    if (!db || !r) return TSDB_ERR_INVAL;

    const char *data_dir = tsdb_db_data_dir(db);
    if (!data_dir) return TSDB_ERR_INTERNAL;

    /* Resolve column name from schema. */
    tsdb_table_internal_t *tbl = tsdb_db_find_table(db, r->table);
    if (!tbl) return TSDB_ERR_NOTFOUND;

    tsdb_schema_t *schema = tsdb_tbl_schema(tbl);
    if (!schema) return TSDB_ERR_INTERNAL;
    if (r->col_idx >= (uint16_t)schema->ncols) return TSDB_ERR_INVAL;

    const char *col_name = schema->cols[r->col_idx].name;

    /* Build partition directory path from YYYYMMDD. */
    char day_str[9];
    snprintf(day_str, sizeof(day_str), "%08u", r->part_day);

    char part_dir[4096];
    snprintf(part_dir, sizeof(part_dir), "%s/%s/%s",
             data_dir, r->table, day_str);

    if (tsdb_mkdir_p(part_dir) < 0) return TSDB_ERR_IO;

    char col_path[4096];
    char idx_path[4096];
    snprintf(col_path, sizeof(col_path), "%s/%s.col", part_dir, col_name);
    snprintf(idx_path, sizeof(idx_path), "%s/%s.idx", part_dir, col_name);

    /* --- Idempotency: skip if last idx entry matches.  Use the storage
     * probe so the last-entry offset is correct even on a V3/V4-mongrel
     * header (a wrong header size here would mis-locate the last entry, miss
     * the match, and double-append the block). */
    {
        uint16_t pver = 0; uint32_t pcnt = 0, pesz = 0;
        uint64_t ptot = 0, pmseq = 0;
        int64_t  pfmn = 0, pfmx = 0;
        int phsz = tsdb_part_idx_probe(idx_path, &pver, &pcnt, &pesz,
                                       &ptot, &pfmn, &pfmx, &pmseq);
        if (phsz > 0 && pesz > 0 && pcnt > 0) {
            FILE *idx_r = fopen(idx_path, "rb");
            if (idx_r) {
                long off = (long)((uint64_t)phsz + (uint64_t)(pcnt - 1) * pesz);
                if (fseek(idx_r, off, SEEK_SET) == 0) {
                    uint8_t ent[RAWBLK_IDX_ENT_SIZE];
                    if (fread(ent, 1, pesz, idx_r) == pesz) {
                        uint32_t e_size  = rb_get_u32(ent + 8);
                        uint32_t e_count = rb_get_u32(ent + 12);
                        int64_t  e_tmin  = rb_get_i64(ent + 16);
                        int64_t  e_tmax  = rb_get_i64(ent + 24);
                        if (e_size == r->block_bytes_len &&
                            e_count == r->count &&
                            e_tmin  == r->ts_min &&
                            e_tmax  == r->ts_max) {
                            fclose(idx_r);
                            return TSDB_OK; /* already applied */
                        }
                    }
                }
                fclose(idx_r);
            }
        }
    }

    /* --- Append to .col file --- */
    FILE *col_fp = fopen(col_path, "ab");
    if (!col_fp) return TSDB_ERR_IO;

    fseek(col_fp, 0, SEEK_END);
    long col_offset = ftell(col_fp);
    if (col_offset < 0) { fclose(col_fp); return TSDB_ERR_IO; }

    /* Write 32-byte BlockHeader. */
    uint8_t hdr[RAWBLK_HDR_SIZE];
    rawblk_write_header(hdr, r->codec, r->flags, r->count,
                        r->ts_min, r->ts_max, r->block_bytes_len);

    if (fwrite(hdr, 1, RAWBLK_HDR_SIZE, col_fp) != RAWBLK_HDR_SIZE) {
        fclose(col_fp); return TSDB_ERR_IO;
    }
    if (r->block_bytes_len > 0 && r->block_bytes) {
        if (fwrite(r->block_bytes, 1, r->block_bytes_len, col_fp) != r->block_bytes_len) {
            fclose(col_fp); return TSDB_ERR_IO;
        }
    }

    /* Match the flush-side CRC trailer when the sender's flags advertise
     * one — keeps a byte-for-byte copy of the source partition.  Without
     * this the receiver's reader sees TSDB_BLOCK_FLAG_HAS_CRC set in the
     * header but no trailer behind the data, and every read fails CORRUPT.
     * Senders predating the CRC patch leave the flag clear and we skip
     * the trailer (back-compat). */
    if (r->flags & TSDB_BLOCK_FLAG_HAS_CRC) {
        uint32_t crc = tsdb_crc32c(hdr, RAWBLK_HDR_SIZE);
        if (r->block_bytes_len > 0 && r->block_bytes)
            crc = tsdb_crc32c_update(crc, r->block_bytes, r->block_bytes_len);
        uint8_t trailer[TSDB_BLOCK_CRC_TRAILER_SIZE];
        trailer[0] = (uint8_t)(crc      );
        trailer[1] = (uint8_t)(crc >>  8);
        trailer[2] = (uint8_t)(crc >> 16);
        trailer[3] = (uint8_t)(crc >> 24);
        if (fwrite(trailer, 1, TSDB_BLOCK_CRC_TRAILER_SIZE, col_fp)
            != TSDB_BLOCK_CRC_TRAILER_SIZE) {
            fclose(col_fp); return TSDB_ERR_IO;
        }
    }

    fclose(col_fp);

    /* --- Rewrite .idx --------------------------------------------------------
     * Read the existing idx (if any) via the storage layer's probe so we (a)
     * locate old entries at the right offset even on a V3/V4-mongrel header,
     * and (b) PRESERVE the partition's idx version + carry its max_seq forward.
     * Pre-fix this path hardcoded a V3 40-byte header, so applying a raw block
     * to a V4 partition silently downgraded it to V3 (dropping the WAL redo
     * checkpoint) and — racing the flush writer — could leave a header size /
     * version / entry-offset mongrel that makes a SELECT read 0 rows. */
    uint8_t *old_entries = NULL;
    uint32_t old_count   = 0;
    uint64_t old_total   = 0;
    int64_t  old_fmn     = INT64_MAX;
    int64_t  old_fmx     = INT64_MIN;
    int      have_old_zone = 0;
    uint64_t old_max_seq = 0;     /* carried forward so a V4 partition stays V4 */

    {
        uint16_t pver = 0; uint32_t pcnt = 0, pesz = 0;
        uint64_t ptot = 0, pmseq = 0;
        int64_t  pfmn = INT64_MAX, pfmx = INT64_MIN;
        /* probe returns >0 only when the magic + version are valid, so a
         * missing/short/corrupt-magic idx falls through to a fresh write. */
        int phsz = tsdb_part_idx_probe(idx_path, &pver, &pcnt, &pesz,
                                       &ptot, &pfmn, &pfmx, &pmseq);
        if (phsz > 0 && pesz > 0) {
            old_count   = pcnt;
            old_total   = ptot;
            old_max_seq = pmseq;          /* 0 for V3, the checkpoint for V4 */
            if (pver >= 2 && pcnt > 0) {
                old_fmn = pfmn;
                old_fmx = pfmx;
                have_old_zone = 1;
            }
            if (old_count > 0) {
                FILE *idx_r = fopen(idx_path, "rb");
                if (idx_r && fseek(idx_r, (long)phsz, SEEK_SET) == 0) {
                    /* Read old entries at their native size then widen to V3
                     * so the resulting file is uniform 88-byte entries. */
                    size_t raw_sz = (size_t)old_count * pesz;
                    uint8_t *raw = malloc(raw_sz);
                    if (raw && fread(raw, 1, raw_sz, idx_r) == raw_sz) {
                        old_entries = calloc((size_t)old_count, RAWBLK_IDX_ENT_SIZE);
                        if (old_entries) {
                            size_t copy_prefix = (pesz < RAWBLK_IDX_ENT_SIZE)
                                                 ? pesz : RAWBLK_IDX_ENT_SIZE;
                            for (uint32_t b = 0; b < old_count; b++) {
                                memcpy(old_entries + (size_t)b * RAWBLK_IDX_ENT_SIZE,
                                       raw + (size_t)b * pesz,
                                       copy_prefix);
                            }
                            if (!have_old_zone) {
                                for (uint32_t b = 0; b < old_count; b++) {
                                    uint8_t *e = old_entries + (size_t)b * RAWBLK_IDX_ENT_SIZE;
                                    int64_t mn = rb_get_i64(e + 16);
                                    int64_t mx = rb_get_i64(e + 24);
                                    if (mn < old_fmn) old_fmn = mn;
                                    if (mx > old_fmx) old_fmx = mx;
                                }
                                have_old_zone = 1;
                            }
                        } else {
                            old_count = 0;
                        }
                    } else {
                        old_count = 0;
                    }
                    free(raw);
                }
                if (idx_r) fclose(idx_r);
            }
        }
    }

    uint32_t new_count = old_count + 1;
    uint64_t new_total = old_total + r->count;

    /* Extend the file-level zone map with this block's [ts_min, ts_max]. */
    int64_t  new_fmn = have_old_zone ? old_fmn : r->ts_min;
    int64_t  new_fmx = have_old_zone ? old_fmx : r->ts_max;
    if (r->ts_min < new_fmn) new_fmn = r->ts_min;
    if (r->ts_max > new_fmx) new_fmx = r->ts_max;

    /* New entry — forward the stats carried by the wire push so the
     * replica's idx is byte-equivalent to the primary's. */
    tsdb_block_meta_t stats;
    memset(&stats, 0, sizeof(stats));
    stats.stats_min   = r->stats_min;
    stats.stats_max   = r->stats_max;
    stats.stats_sum   = r->stats_sum;
    stats.stats_first = r->stats_first;
    stats.stats_last  = r->stats_last;
    stats.stats_flags = r->stats_flags;

    uint8_t ent[RAWBLK_IDX_ENT_SIZE];
    rawblk_write_idx_entry(ent, (uint64_t)col_offset,
                           r->block_bytes_len, r->count,
                           r->ts_min, r->ts_max, &stats);

    /* Header via the SHARED storage writer so flush + replication stamp
     * byte-identical headers; preserving old_max_seq keeps a V4 partition V4
     * (and a fresh replica-only partition V3, since old_max_seq stays 0). */
    uint8_t ih[TSDB_IDX_HEADER_SIZE];
    size_t  hdr_sz = tsdb_part_write_idx_header(ih, new_count, new_total,
                                                new_fmn, new_fmx, old_max_seq);

    /* Atomic publish: write <idx>.tmp, fflush + fsync, then rename onto the
     * real path so a concurrent reader never observes a torn header (matches
     * the flush path's temp+fsync+rename in part.c col_writer_close). */
    char tmp_path[4200];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", idx_path);
    FILE *idx_w = fopen(tmp_path, "wb");
    if (!idx_w) { free(old_entries); return TSDB_ERR_IO; }

    int io_ok = 1;
    if (fwrite(ih, 1, hdr_sz, idx_w) != hdr_sz) io_ok = 0;
    if (io_ok && old_count > 0 && old_entries &&
        fwrite(old_entries, 1, (size_t)old_count * RAWBLK_IDX_ENT_SIZE, idx_w)
            != (size_t)old_count * RAWBLK_IDX_ENT_SIZE) io_ok = 0;
    if (io_ok && fwrite(ent, 1, RAWBLK_IDX_ENT_SIZE, idx_w)
            != RAWBLK_IDX_ENT_SIZE) io_ok = 0;
    if (io_ok && fflush(idx_w) != 0) io_ok = 0;
    if (io_ok) {
        int fd = fileno(idx_w);
        if (fd >= 0) (void)fsync(fd);   /* durable before rename */
    }
    fclose(idx_w);
    free(old_entries);

    if (!io_ok) { unlink(tmp_path); return TSDB_ERR_IO; }
    if (rename(tmp_path, idx_path) != 0) { unlink(tmp_path); return TSDB_ERR_IO; }
    return TSDB_OK;
}

/* ---- Replicate (primary fan-out) ------------------------------------------ */

/* Per-replica send state. */
typedef struct {
    tsdb_replica_mgr_t *rmgr;
    tsdb_node_id_t      node_id;
    uint8_t            *payload;
    uint32_t            payload_len;
    volatile int       *ack_count;
    pthread_mutex_t    *ack_lock;
} rawblk_send_arg_t;

static void evict_rawblk_conn(tsdb_replica_mgr_t *rmgr, tsdb_node_id_t nid) {
    /* We reuse replica_mgr infrastructure; connection eviction is handled by
     * the existing pool logic — just mark it dirty by closing. */
    (void)rmgr; (void)nid;
    /* Best-effort: reconnect on next call. */
}

static void *rawblk_send_thread(void *arg) {
    rawblk_send_arg_t *sa = (rawblk_send_arg_t *)arg;
    tsdb_rpc_conn_t *conn = tsdb_replica_mgr_get_conn(sa->rmgr, sa->node_id);
    if (conn) {
        /* Bounded send: the pool is CONNS_PER_PEER=1, so this call holds the
         * single per-peer conn->lock for its whole round-trip.  A no-timeout
         * send against a peer wedged on its own batch_mu pinned that lock
         * forever — a concurrent scatter query (or the next flush) then parked
         * unbounded on it, and THIS worker thread never exited (the 140-277
         * idle-thread leak).  Cap the round-trip so a stuck peer frees the
         * conn and this thread dies; anti-entropy closes any dropped block. */
        uint8_t ackbuf[1]; uint32_t acklen = 0;
        int rc = tsdb_rpc_call_recv_to(conn, TSDB_RPC_RAW_BLOCK_PUSH,
                                       sa->payload, sa->payload_len,
                                       ackbuf, sizeof(ackbuf), &acklen,
                                       TSDB_REPL_SEND_TIMEOUT_MS);
        if (rc == TSDB_OK) {
            pthread_mutex_lock(sa->ack_lock);
            (*sa->ack_count)++;
            pthread_mutex_unlock(sa->ack_lock);
        } else {
            evict_rawblk_conn(sa->rmgr, sa->node_id);
        }
    }
    free(sa);
    return NULL;
}

int tsdb_rawblock_replicate(tsdb_cluster_t *c,
                             const char *table, uint32_t day, uint16_t col_idx,
                             const tsdb_block_meta_t *meta,
                             const uint8_t *block_bytes, size_t block_len,
                             int quorum_w)
{
    if (!c || !table || !meta) return TSDB_ERR_INVAL;

    /* Build push struct. */
    tsdb_rawblock_push_t r;
    memset(&r, 0, sizeof(r));
    strncpy(r.table, table, 63);
    r.part_day        = day;
    r.col_idx         = col_idx;
    r.codec           = meta->codec;
    r.flags           = meta->flags;
    r.count           = meta->count;
    r.ts_min          = meta->ts_min;
    r.ts_max          = meta->ts_max;
    r.stats_min       = meta->stats_min;
    r.stats_max       = meta->stats_max;
    r.stats_sum       = meta->stats_sum;
    r.stats_first     = meta->stats_first;
    r.stats_last      = meta->stats_last;
    r.stats_flags     = meta->stats_flags;
    r.block_bytes_len = (uint32_t)block_len;
    r.block_bytes     = (uint8_t *)block_bytes;

    /* Serialize once. */
    uint8_t *payload = NULL;
    size_t   plen    = 0;
    if (tsdb_rawblock_serialize(&r, &payload, &plen) != TSDB_OK)
        return TSDB_ERR_INTERNAL;

    /* Gather ALIVE peers (excluding self). */
    tsdb_node_manager_t *mgr = tsdb_cluster_node_mgr(c);
    tsdb_node_info_t nodes[TSDB_CLUSTER_MAX_NODES];
    int n = tsdb_node_manager_snapshot(mgr, nodes, TSDB_CLUSTER_MAX_NODES);

    tsdb_node_id_t local_id = tsdb_cluster_local_id(c);
    tsdb_replica_mgr_t *rmgr = tsdb_cluster_replica_mgr(c);

    volatile int ack_count = 1; /* local write = 1 */
    pthread_mutex_t ack_lock;
    pthread_mutex_init(&ack_lock, NULL);

    pthread_t tids[TSDB_CLUSTER_MAX_NODES];
    int nthreads = 0;

    for (int i = 0; i < n && nthreads < TSDB_CLUSTER_MAX_NODES; i++) {
        if (nodes[i].id == local_id || nodes[i].state != TSDB_NODE_ALIVE)
            continue;

        rawblk_send_arg_t *sa = malloc(sizeof(*sa));
        if (!sa) continue;
        sa->rmgr        = rmgr;
        sa->node_id     = nodes[i].id;
        sa->payload     = payload;
        sa->payload_len = (uint32_t)plen;
        sa->ack_count   = &ack_count;
        sa->ack_lock    = &ack_lock;

        if (pthread_create(&tids[nthreads], NULL, rawblk_send_thread, sa) == 0)
            nthreads++;
        else
            free(sa);
    }

    for (int i = 0; i < nthreads; i++)
        pthread_join(tids[i], NULL);

    pthread_mutex_destroy(&ack_lock);
    free(payload);

    return (ack_count >= quorum_w) ? TSDB_OK : TSDB_ERR_IO;
}
