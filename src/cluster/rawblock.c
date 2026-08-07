/* rawblock.c — Raw block replication (primary encode-once, replica write-verbatim). */

#include "rawblock.h"
#include "replica.h"
#include "node.h"
#include "rpc.h"
#include "../storage/db.h"
#include "../storage/schema.h"
#include "../storage/part.h"
#include "../server/proto.h"  /* tsdb_crc32c — match flush-side trailer */
#include "../server/metrics.h"
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
 * memcpy the payload straight into its idx file.
 *
 * The 6 pad bytes at [42..47] are the durable block ordinal + marker — the same
 * fields, at the same relative offsets, as idx entry bytes [82..87].  Every
 * previous sender wrote them as zero, which is exactly "ordinal unknown", so no
 * wire length or version changes. */
#define RAWBLK_WIRE_STATS_SIZE 48u
#define RAWBLK_WIRE_ORD_OFF    42u

int tsdb_rawblock_serialize(const tsdb_rawblock_push_t *r,
                             uint8_t **buf, size_t *len)
{
    if (!r || !buf || !len) return TSDB_ERR_INVAL;

    uint8_t tlen = (uint8_t)strnlen(r->table, 63);
    /* 1 + tlen + 4 + 2 + 1 + 2 + 4 + 8 + 8 + 48 stats + 4 + block_bytes_len
     * + 8 issuer.  The issuer sits AFTER the block bytes on purpose: every
     * field before it keeps its offset, and tsdb_rawblock_parse has never
     * required the payload to end where it stops reading, so an older receiver
     * ignores the tail and a payload written without one parses as issuer 0. */
    size_t total = 1 + tlen + 4 + 2 + 1 + 2 + 4 + 8 + 8
                   + RAWBLK_WIRE_STATS_SIZE + 4 + r->block_bytes_len + 8;

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
    if (TSDB_ORD_KNOWN(r->ord)) {
        rb_put_u32(w + RAWBLK_WIRE_ORD_OFF,     r->ord.v);
        rb_put_u16(w + RAWBLK_WIRE_ORD_OFF + 4, r->ord.mark);
    }
    w += RAWBLK_WIRE_STATS_SIZE;

    rb_put_u32(w, r->block_bytes_len); w += 4;
    if (r->block_bytes_len > 0 && r->block_bytes)
        memcpy(w, r->block_bytes, r->block_bytes_len);
    w += r->block_bytes_len;
    rb_put_i64(w, (int64_t)r->issuer);

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
    {
        uint16_t mk = rb_get_u16(p + RAWBLK_WIRE_ORD_OFF + 4);
        out->ord.mark = (mk == TSDB_IDX_ORD_MARK) ? mk : 0;
        out->ord.v    = out->ord.mark ? rb_get_u32(p + RAWBLK_WIRE_ORD_OFF) : 0;
    }
    p += RAWBLK_WIRE_STATS_SIZE;

    RB_NEED(4); out->block_bytes_len = rb_get_u32(p); p += 4;
    RB_NEED(out->block_bytes_len);
    out->block_bytes = (uint8_t *)p; /* points into caller's buf */
    p += out->block_bytes_len;

    /* Optional issuer tail.  A sender that predates it stops here, and 0 is
     * exactly the "unknown issuer" state the applier falls back on. */
    out->issuer = (p + 8 <= end) ? (uint64_t)rb_get_i64(p) : 0;

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
                                   const tsdb_block_meta_t *stats_src,
                                   tsdb_block_ord_t ord)
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
    /* [82..87] the durable ordinal, in THIS node's space (the applier has
     * already translated the sender's).  A sender that predates the field
     * leaves the marker 0, which is the entry's "unknown" state. */
    if (TSDB_ORD_KNOWN(ord)) {
        rb_put_u32(buf + 82, ord.v);
        rb_put_u16(buf + 86, ord.mark);
    }
}

/* The RECORDED ordinal of an entry in an idx image of 88-byte entries.
 * Returns 1 and fills `*out` when the entry carries one, 0 when it does not.
 *
 * It deliberately does NOT fall back to the physical position.  Position ==
 * ordinal is precisely false in the state a repair push exists to fix: once a
 * push has been lost, the surviving entries have closed up over the gap, so the
 * n-th entry is no longer ordinal n.  Handing an unmarked entry an invented
 * ordinal made the repair collide with the WRONG entry and be discarded as a
 * re-delivery — turning a repairable gap into a column that reads
 * TSDB_ERR_CORRUPT forever, on exactly the rolling-upgrade path (upgraded
 * sender, not-yet-upgraded index) this field was supposed to help. */
static int rawblk_entry_ord(const uint8_t *ent, uint32_t *out) {
    if (rb_get_u16(ent + 86) != TSDB_IDX_ORD_MARK) return 0;
    *out = rb_get_u32(ent + 82);
    return 1;
}

/* Does an existing entry describe the same block as the incoming push? */
static int rawblk_entry_agrees(const uint8_t *ent,
                               const tsdb_rawblock_push_t *r) {
    return rb_get_u32(ent + 8)  == r->block_bytes_len &&
           rb_get_u32(ent + 12) == r->count &&
           rb_get_i64(ent + 16) == r->ts_min &&
           rb_get_i64(ent + 24) == r->ts_max;
}

/* Is an existing entry THE SAME BLOCK as the incoming push — bytes included?
 *
 * ABSORBING A PUSH IS DISCARDING IT, so the metadata compare above is not
 * enough to license it.  (size, count, ts_min, ts_max) is a description, not a
 * checksum: two different value blocks of one column routinely encode to the
 * same length under the same codec, and the whole point of this file's rework is
 * that content keys repeat.  A push absorbed on that alone is ACKed and thrown
 * away — the sender counts a durable replica for rows this node does not hold.
 *
 * So read the bytes the entry points at and compare them.  Anything that stops
 * the comparison from PROVING equality — a .col shorter than its own index, a
 * failed open, OOM — answers "not the same block", which routes the push through
 * the keep-both path: a duplicate is an over-count anti-entropy can see and
 * repair, where the drop is silent and permanent. */
static int rawblk_entry_is_same_block(const char *col_path, const uint8_t *ent,
                                      const tsdb_rawblock_push_t *r) {
    if (!rawblk_entry_agrees(ent, r)) return 0;
    uint32_t n = r->block_bytes_len;
    if (n == 0) return 1;                 /* nothing to compare */
    if (!r->block_bytes) return 0;

    uint64_t off = (uint64_t)rb_get_i64(ent + 0);
    FILE *f = fopen(col_path, "rb");
    if (!f) return 0;
    uint8_t *have = malloc(n);
    int same = 0;
    if (have) {
        if (fseeko(f, (off_t)(off + RAWBLK_HDR_SIZE), SEEK_SET) == 0 &&
            fread(have, 1, n, f) == n)
            same = (memcmp(have, r->block_bytes, n) == 0);
        free(have);
    }
    fclose(f);
    return same;
}

/* ---- Apply on replica ------------------------------------------------------ */

/* fsync the partition directory so the idx rename below is durable, not just
 * atomic.  Without it a crash can lose the rename and resurrect the previous
 * idx — for a NON-ts column that manufactures the exact short-column state
 * this file is being fixed for, on a node that never lost a push. */
static void rawblk_fsync_dir(const char *dir) {
    int dfd = open(dir, O_RDONLY);
    if (dfd >= 0) { (void)fsync(dfd); close(dfd); }
}

int tsdb_rawblock_apply(tsdb_db_t *db, const tsdb_rawblock_push_t *r)
{
    return tsdb_rawblock_apply_ex(db, r, 0);
}

int tsdb_rawblock_apply_ex(tsdb_db_t *db, const tsdb_rawblock_push_t *r,
                           uint32_t flags)
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

    /* Declared before the first `goto out` so no jump skips an initialiser. */
    int       rc            = TSDB_OK;
    FILE     *col_fp        = NULL;
    long      col_offset    = -1;   /* .col EOF before this append (rollback) */
    int       appended      = 0;
    uint8_t  *old_entries   = NULL;
    uint32_t  old_count     = 0;
    uint64_t  old_total     = 0;
    int64_t   old_fmn       = INT64_MAX;
    int64_t   old_fmx       = INT64_MIN;
    int       have_old_zone = 0;
    uint64_t  old_max_seq   = 0;   /* carried forward so a V4 partition stays V4 */
    uint32_t  insert_at     = 0;   /* entry position this block belongs at */

    /* Serialise probe -> append -> idx read-modify-write -> rename against the
     * other writers of this partition: a concurrent applier for the same
     * column (rpc.c runs a thread per connection, and the flush hook fans out
     * to every alive peer regardless of shard ownership, so one node receives
     * pushes from several senders at once) and the local flush's
     * col_writer_close.  Both do a FULL-FILE idx rewrite; unsynchronised they
     * both read N and write N+1 and one entry is silently lost.  A lost NON-ts
     * entry under a surviving ts is the multi-column hole. */
    tsdb_part_idx_lock(part_dir);

    /* --- Translate the sender's ordinal into OUR space ----------------------
     *
     * A block on the wire carries the SENDER's partition-local number.  This
     * node's space is its own: it compacts on its own schedule (every node runs
     * a compactor, and compaction is never replicated), and its own flush
     * allocates out of the same range.  Two rounds of trusting a remote ordinal
     * lost data in both directions — a primary that renumbered re-issued
     * numbers the replica held for other rows, and a replica-side compaction
     * consumed exactly the numbers the primary was about to issue.
     *
     * tsdb_part_ord_translate maps the incoming block GROUP — keyed on the
     * ISSUER plus the four fields every column of one flush block shares — to
     * one free ordinal here, durably, so every column of the group lands on the
     * same number and the group stays pairable.  Nothing the sender numbered
     * ever enters this node's space, which is what makes the two compactors
     * independent again; and the issuer is what keeps TWO senders' spaces apart
     * from each other, which matters because they both start at ordinal 0 over
     * the same timestamps and their ts blocks are then byte-identical.
     *
     * It runs before the commit test because that test is answered against THIS
     * index, so it has to be asked about THIS node's ordinal. */
    tsdb_block_ord_t lord = { 0, 0 };
    {
        int trc = tsdb_part_ord_translate(schema, part_dir, r->issuer, r->ord,
                                          r->ts_min, r->ts_max, r->count, &lord);
        if (trc != TSDB_OK) { rc = trc; goto out; }
    }

    /* --- Commit test -------------------------------------------------------
     * Never advance the partition's visibility marker past a group this node
     * has not fully received.  ts.idx is what exec.c enumerates blocks from
     * and what count(*)/max(ts) — and therefore anti-entropy — are answered
     * from, so a ts block published for a group some column is missing yields
     * a partition that fails every query touching that column while still
     * reporting the rows as present.  Nothing alerts.
     *
     * Ordering cannot fix this on the receiver: it sees one message at a time
     * and cannot enforce a group property it has no way to name.  But the
     * commit condition is locally checkable, because the join key is IN THE
     * DATA — every column's block for the same rows carries the identical
     * (ts_min, count), which is exactly the key the reader pairs on.  That is
     * why this needs no manifest, no wire change and no format change, and why
     * it holds against ANY sender, in any order, at any version.
     *
     * Nothing is written when it fails, so the caller may simply retry. */
    if ((flags & TSDB_RB_VERIFY_TS) && (int)r->col_idx == schema->ts_col_idx) {
        char missing[64];
        /* By ORDINAL when the sender carries one.  "Some sibling block with a
         * matching key" is satisfied by the WRONG sibling on a duplicate-
         * timestamp run — the replica then publishes ts for a group it has not
         * received and answers that column with another group's values. */
        int vrc = tsdb_part_ts_publish_ready_ord(schema, part_dir, lord,
                                                 r->ts_min, r->count,
                                                 missing, sizeof(missing));
        if (vrc != TSDB_OK) {
            tsdb_metric_inc("qengine_rawblock_ts_deferred_total");
            fprintf(stderr,
                    "[rawblock] %s/%s: refusing ts block (ts_min=%lld count=%u): "
                    "column '%s' has no block for it here — the node stays "
                    "BEHIND (repairable) instead of TORN (not)\n",
                    r->table, day_str, (long long)r->ts_min, r->count,
                    missing[0] ? missing : "?");
            rc = vrc;
            goto out;
        }
    }

    /* --- Read the existing idx -----------------------------------------------
     * Read via the storage layer's probe so we (a) locate old entries at the
     * right offset even on a V3/V4-mongrel header, and (b) PRESERVE the
     * partition's idx version + carry its max_seq forward.  Pre-fix this path
     * hardcoded a V3 40-byte header, so applying a raw block to a V4 partition
     * silently downgraded it to V3 (dropping the WAL redo checkpoint) and —
     * racing the flush writer — could leave a header size / version /
     * entry-offset mongrel that makes a SELECT read 0 rows.
     *
     * This runs BEFORE the .col append because the placement decision below
     * needs the whole entry array, not just its tail. */
    {
        uint16_t pver = 0; uint32_t pcnt = 0, pesz = 0;
        uint64_t ptot = 0, pmseq = 0;
        int64_t  pfmn = INT64_MAX, pfmx = INT64_MIN;
        /* The probe's two non-positive answers are NOT the same thing
         * (part.c): 0 means the idx is ABSENT — there is nothing to preserve,
         * so falling through to a fresh one-entry write is correct.  -1 means
         * the file EXISTS but cannot be parsed: corrupt magic, torn short
         * header, or a version this binary does not know (a rolled-back
         * binary beside a newer writer).  Treating -1 as absence let the
         * publish below rename a fresh ONE-entry manifest over an N-entry
         * index it merely failed to read: rc came back OK, the sender was
         * acked, anti-entropy saw nothing (max(ts) unchanged), and the next
         * threshold compaction rewrote the .col from the 1-entry idx — the
         * loss of the other N-1 blocks became permanent.  An unparseable
         * manifest must fail the apply LOUDLY instead: nothing is written
         * (this runs before the .col append), the sender keeps retrying, and
         * the partition stays byte-intact for an operator or anti-entropy to
         * repair.  Read-only probe sites (db_cluster.c, restore.c,
         * compaction.c, db.c) may keep reading -1 as "0 blocks" — for a
         * reader that is conservative; this is the one site that durably
         * REWRITES the manifest from what it probed. */
        int phsz = tsdb_part_idx_probe(idx_path, &pver, &pcnt, &pesz,
                                       &ptot, &pfmn, &pfmx, &pmseq);
        /* The refusal turns a previously-"succeeding" push into a persistent
         * failure (the sender retries forever until the idx is repaired), so
         * it must be loud and attributable: the line below names the table,
         * partition day and column.  No counter here on purpose — the metric
         * registry is a fixed table (metrics.c) and tsdb_metric_inc on an
         * unregistered name is a silent no-op, which is worse than no counter
         * because it looks instrumented. */
        if (phsz < 0) {
            fprintf(stderr,
                    "[rawblock] %s/%s: column '%s': existing idx is "
                    "unparseable (corrupt magic or unknown version); refusing "
                    "to overwrite a manifest this binary cannot read\n",
                    r->table, day_str, col_name);
            rc = TSDB_ERR_CORRUPT;
            goto out;
        }
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
                     * so the resulting file is uniform 88-byte entries.  The
                     * widen zero-fills [82..87] for a legacy 40-byte entry,
                     * which is exactly "ordinal unknown". */
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
                        }
                    }
                    free(raw);
                }
                if (idx_r) fclose(idx_r);
            }
        }
    }

    /* Everything below reads `old_entries` and then copies it back out, so a
     * declared-but-unreadable entry array is fatal, not something to route
     * around.  A failed fopen/fseek/malloc/fread above leaves old_count > 0
     * with old_entries == NULL; the placement scan would dereference NULL, and
     * pretending old_count == 0 instead would publish a header claiming one
     * entry over an index whose other entries were silently dropped.  Fail the
     * apply and let the sender retry — nothing has been written yet. */
    if (old_count > 0 && !old_entries) { rc = TSDB_ERR_IO; goto out; }

    /* --- Placement + idempotency -------------------------------------------
     *
     * The old rule compared the incoming block against the index TAIL on
     * (size, count, ts_min, ts_max).  Neither half of that works:
     *
     *   - it DISCARDED real data.  A run of rows carrying one timestamp
     *     produces several genuinely different blocks whose keys — and whose ts
     *     payloads, since ts_min == ts_max means one repeated value — are
     *     identical.  Block 1 of the run therefore looked like a re-delivery of
     *     block 0 and was dropped, silently: count(*) reported one block's rows
     *     for the whole run.
     *   - it could not absorb a repeat that was no longer the last entry, so a
     *     resync or a re-sent batch appended the whole stream a second time.
     *
     * The TRANSLATED ordinal decides both: it is a number in this node's own
     * space, so an entry already carrying it describes a group we have already
     * been given, and a new one is INSERTED at its place rather than appended,
     * so blocks that arrive out of order still land in order.  The scan below
     * reads every entry, not just the tail, so the absorb no longer depends on
     * the repeat still being the last thing written.
     *
     * WHAT IT DOES NOT SPAN IS A RENUMBERING OF THIS PARTITION.  A local
     * compaction re-cuts the partition and stamps its output above the ordinal
     * high-water, so the mapping a sender's group was absorbed under names
     * nothing that is still on disk, the scan finds no match, and a resync
     * appends the whole group a second time.  Measured on a 24-block partition:
     * replicate, compact locally, resync the same 48 blocks -> every push
     * ACCEPTED, count(*) 24576 -> 49152, rc=0.
     *
     * That is NOT something the ordinal introduced.  9dab5a2 compares the
     * incoming block against the index TAIL and fails for the same reason — the
     * blocks the sender is re-delivering no longer exist here in any form a
     * comparison can recognise — and measures identically: count(*) 24576 ->
     * 49152, rc=0.  Closing it needs the applier to reason about ROW RANGES
     * already covered rather than about block identity, which neither rule
     * does; it is recorded as a known gap, not papered over here.
     *
     * TWO things this must NOT do:
     *
     *   - drop on the ordinal ALONE.  The mapping is a local bookkeeping fact,
     *     not evidence about the bytes; the CONTENT still has to agree before a
     *     block is discarded as a re-delivery.  Nor may a disagreement be
     *     refused: round 2 answered it with TSDB_ERR_CORRUPT and a replica-side
     *     compaction then made every subsequent push permanently unacceptable.
     *     Keep both entries — the read side takes the last claimant of an
     *     ordinal — and count it, because losing acked rows is the worse of the
     *     two failures by a wide margin.
     *   - invent an ordinal for an unmarked entry.  Position == ordinal is
     *     precisely false in the state a repair push exists to fix (see
     *     rawblk_entry_ord), so an unmarked entry can never satisfy the drop
     *     test.  A push against an index that still has unmarked entries falls
     *     back to the historical tail compare — and only when the TAIL ITSELF is
     *     unmarked, so a duplicate-timestamp run of marked blocks is never
     *     collapsed by it. */
    if (TSDB_ORD_KNOWN(lord) && old_count > 0) {
        int saw_unmarked = 0;
        for (uint32_t b = 0; b < old_count; b++) {
            uint8_t *e = old_entries + (size_t)b * RAWBLK_IDX_ENT_SIZE;
            uint32_t eo = 0;
            if (!rawblk_entry_ord(e, &eo)) {
                /* Ordinal unknown: predates the field, so it sorts before
                 * anything a marked writer issues, and it proves nothing. */
                saw_unmarked = 1;
                insert_at = b + 1;
                continue;
            }
            if (eo == lord.v) {
                /* Re-delivery only if the BYTES match too — see
                 * rawblk_entry_is_same_block. */
                if (rawblk_entry_is_same_block(col_path, e, r)) goto out;
                tsdb_metric_inc("qengine_rawblock_ordinal_collision_total");
                fprintf(stderr,
                        "[rawblock] %s/%s: column '%s' already holds DIFFERENT "
                        "bytes at local ordinal %u (have size=%u count=%u "
                        "ts=[%lld,%lld], got size=%u count=%u ts=[%lld,%lld]); "
                        "keeping both rather than dropping acked rows\n",
                        r->table, day_str, col_name, lord.v,
                        rb_get_u32(e + 8), rb_get_u32(e + 12),
                        (long long)rb_get_i64(e + 16),
                        (long long)rb_get_i64(e + 24),
                        r->block_bytes_len, r->count,
                        (long long)r->ts_min, (long long)r->ts_max);
                insert_at = b + 1;
                continue;
            }
            if (eo < lord.v) insert_at = b + 1;
        }
        if (saw_unmarked) {
            uint8_t *tail = old_entries +
                            (size_t)(old_count - 1) * RAWBLK_IDX_ENT_SIZE;
            uint32_t dummy = 0;
            if (!rawblk_entry_ord(tail, &dummy) &&
                rawblk_entry_is_same_block(col_path, tail, r))
                goto out;                          /* already applied, rc==OK */
        }
    } else if (!TSDB_ORD_KNOWN(lord) && old_count > 0) {
        uint8_t *e = old_entries + (size_t)(old_count - 1) * RAWBLK_IDX_ENT_SIZE;
        if (rawblk_entry_is_same_block(col_path, e, r))
            goto out;                              /* already applied, rc==OK */
        insert_at = old_count;
    }

    /* --- Append to .col file --- */
    col_fp = fopen(col_path, "ab");
    if (!col_fp) { rc = TSDB_ERR_IO; goto out; }

    fseek(col_fp, 0, SEEK_END);
    col_offset = ftell(col_fp);
    if (col_offset < 0) { rc = TSDB_ERR_IO; goto out; }
    appended = 1;

    /* Write 32-byte BlockHeader. */
    uint8_t hdr[RAWBLK_HDR_SIZE];
    rawblk_write_header(hdr, r->codec, r->flags, r->count,
                        r->ts_min, r->ts_max, r->block_bytes_len);

    if (fwrite(hdr, 1, RAWBLK_HDR_SIZE, col_fp) != RAWBLK_HDR_SIZE) {
        rc = TSDB_ERR_IO; goto out;
    }
    if (r->block_bytes_len > 0 && r->block_bytes) {
        if (fwrite(r->block_bytes, 1, r->block_bytes_len, col_fp) != r->block_bytes_len) {
            rc = TSDB_ERR_IO; goto out;
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
            rc = TSDB_ERR_IO; goto out;
        }
    }

    /* Durability ordering: the .col bytes MUST reach the device BEFORE the idx
     * entry that points at them.  The flush path has done this since the
     * max_seq checkpoint landed (col_writer_close fsyncs the .col before it
     * fsyncs and renames the idx); this path did neither, so a plain power
     * loss — with no dropped message at all — could leave a durable idx entry
     * over never-written .col bytes.  On restart the per-block size filter in
     * tsdb_part_open drops that block, leaving THIS column short while ts,
     * whose own bytes happened to be flushed by the page cache, stays long:
     * the multi-column hole, manufactured locally. */
    if (fflush(col_fp) != 0) { rc = TSDB_ERR_IO; goto out; }
    {
        int cfd = fileno(col_fp);
        if (cfd < 0 || fsync(cfd) != 0) { rc = TSDB_ERR_IO; goto out; }
    }
    fclose(col_fp);
    col_fp = NULL;

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

    /* Stamp the LOCAL ordinal.  The sender's number is never written here: it
     * would only be meaningful on the sender. */
    uint8_t ent[RAWBLK_IDX_ENT_SIZE];
    rawblk_write_idx_entry(ent, (uint64_t)col_offset,
                           r->block_bytes_len, r->count,
                           r->ts_min, r->ts_max, &stats, lord);

    /* Header via the SHARED storage writer so flush + replication stamp
     * byte-identical headers; preserving old_max_seq keeps a V4 partition V4
     * (and a fresh replica-only partition V3, since old_max_seq stays 0). */
    uint8_t ih[TSDB_IDX_HEADER_SIZE];
    /* The column-count stamp (part.h) is PRESERVED, never asserted here: this
     * applier lands ONE (column, block) per call and cannot know whether the
     * rest of the schema's columns are coming, which is the whole reason the
     * commit test above exists.  Restating it from schema->ncols would let a
     * single-column repair push claim a partition holds columns it never held —
     * turning a legitimate ALTER-added column into a permanent read error. */
    size_t  hdr_sz = tsdb_part_write_idx_header(ih, new_count, new_total,
                                                new_fmn, new_fmx, old_max_seq,
                                                tsdb_part_idx_ncols(idx_path));

    /* Atomic publish: write <idx>.rbtmp, fflush + fsync, then rename onto the
     * real path so a concurrent reader never observes a torn header (matches
     * the flush path's temp+fsync+rename in part.c col_writer_close).
     *
     * The suffix is deliberately NOT ".idx.tmp": part_compact_swap_recover
     * renames <col>.idx.tmp -> <col>.idx for every column named in a surviving
     * <part>/.compact_swap marker.  A crashed applier's PARTIAL temp sitting
     * next to a crashed compaction's marker was therefore rolled FORWARD into
     * the live index — a truncated manifest published as authoritative. */
    char tmp_path[4200];
    snprintf(tmp_path, sizeof(tmp_path), "%s.rbtmp", idx_path);
    FILE *idx_w = fopen(tmp_path, "wb");
    if (!idx_w) { rc = TSDB_ERR_IO; goto out; }

    /* Entries stay in ORDINAL order: [0, insert_at) then the new block then the
     * rest.  insert_at == old_count is the historical append. */
    if (insert_at > old_count) insert_at = old_count;
    size_t head_n = (size_t)insert_at * RAWBLK_IDX_ENT_SIZE;
    size_t tail_n = (size_t)(old_count - insert_at) * RAWBLK_IDX_ENT_SIZE;

    int io_ok = 1;
    if (fwrite(ih, 1, hdr_sz, idx_w) != hdr_sz) io_ok = 0;
    if (io_ok && head_n > 0 && old_entries &&
        fwrite(old_entries, 1, head_n, idx_w) != head_n) io_ok = 0;
    if (io_ok && fwrite(ent, 1, RAWBLK_IDX_ENT_SIZE, idx_w)
            != RAWBLK_IDX_ENT_SIZE) io_ok = 0;
    if (io_ok && tail_n > 0 && old_entries &&
        fwrite(old_entries + head_n, 1, tail_n, idx_w) != tail_n) io_ok = 0;
    if (io_ok && fflush(idx_w) != 0) io_ok = 0;
    if (io_ok) {
        /* Durable before rename, and CHECKED: publishing an idx whose bytes
         * never reached the device is how a manifest ends up referencing
         * blocks that are not there. */
        int fd = fileno(idx_w);
        if (fd < 0 || fsync(fd) != 0) io_ok = 0;
    }
    if (fclose(idx_w) != 0) io_ok = 0;

    if (!io_ok) { unlink(tmp_path); rc = TSDB_ERR_IO; goto out; }
    if (rename(tmp_path, idx_path) != 0) { unlink(tmp_path); rc = TSDB_ERR_IO; goto out; }
    rawblk_fsync_dir(part_dir);

out:
    if (col_fp) fclose(col_fp);
    /* Roll the .col back to its pre-append length on ANY failure after the
     * append, mirroring col_writer_abort on the flush path.  Without it every
     * failed apply leaks a dead block into the .col; under a retry loop those
     * orphans accumulate, and a retry that DOES succeed would otherwise index
     * a second copy of the same bytes. */
    if (rc != TSDB_OK && appended && col_offset >= 0)
        (void)truncate(col_path, (off_t)col_offset);
    tsdb_part_idx_unlock(part_dir);
    free(old_entries);
    return rc;
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

/* Actually evict.  This used to be a comment and two (void) casts: the failing
 * conn stayed in the pool, so once a peer restarted (or one push hit the 10 s
 * deadline) every later RAW_BLOCK_PUSH to it drew the same dead socket back out
 * and failed forever — raw-mode replication to that peer never resumed for the
 * life of the sender process.  replica.h has exported the evictor since the
 * scatter path needed it for exactly this failure. */
static void evict_rawblk_conn(tsdb_replica_mgr_t *rmgr, tsdb_node_id_t nid,
                              tsdb_rpc_conn_t *bad) {
    tsdb_replica_mgr_evict_conn(rmgr, nid, bad);
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
            evict_rawblk_conn(sa->rmgr, sa->node_id, conn);
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
    r.ord             = meta->ord;
    r.block_bytes_len = (uint32_t)block_len;
    r.block_bytes     = (uint8_t *)block_bytes;
    /* `ord` is OUR partition-local number, so it only means anything paired with
     * who we are: two nodes flushing the same timestamps into one table+day both
     * issue ordinal 0 for byte-identical ts blocks, and a receiver that cannot
     * tell the two apart collapses them onto one group. */
    r.issuer          = (uint64_t)tsdb_cluster_local_id(c);

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
