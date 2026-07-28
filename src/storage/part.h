/* part.h — on-disk partition management.
 *
 * A partition corresponds to one calendar day (UTC) by default, or one
 * calendar hour (UTC) when the table schema declares HOUR granularity.
 * Layout: <schema->dir>/<PARTSTR>/<colname>.col  — compressed data blocks
 *         <schema->dir>/<PARTSTR>/<colname>.idx  — block index
 *
 *   PARTSTR = "YYYYMMDD"   (DAY,  length 8)
 *           | "YYYYMMDDHH" (HOUR, length 10)
 *
 * BlockHeader (32 bytes, little-endian, no padding):
 *   magic   u32  = 'BLK1' (0x314B4C42)
 *   codec   u8
 *   _pad    u8
 *   flags   u16
 *   count   u32
 *   ts_min  i64
 *   ts_max  i64
 *   size    u32   (compressed bytes following this header)
 *   [CompressedData: size bytes]
 *
 * IdxHeader v1 (20 bytes, legacy):
 *   magic       u32  = 'IDX1' (0x31584449)
 *   count       u32  (number of BlockIndexEntry records)
 *   version     u16  = 1
 *   ncols       u16  (was _pad; 0 == "not recorded" — see below)
 *   total_rows  u64
 *
 * bytes [10..11] — `ncols`, the column-count stamp.
 *   Every writer before 2026-07 put a literal 0 here and no reader ever looked
 *   at it, so the field is free in EVERY version (v1..v4) at a fixed offset:
 *   recording it needs no version bump, no size change and no migration, and
 *   an older binary keeps ignoring it.
 *
 *   Meaning: "when this idx was published, the schema had `ncols` columns AND
 *   the writer wrote every one of them into this partition."  Only the FLUSH
 *   may assert that (it loops over all s->ncols for each partition it touches
 *   and is fail-stop).  Every other writer — raw-block replication, compaction,
 *   the ts retraction — publishes ONE column at a time and therefore PRESERVES
 *   whatever is already there instead of restating it.  0 means "no writer has
 *   ever asserted it here" and the reader falls back to the shape rules alone.
 *
 *   Why it exists: a non-ts column with NO .idx in a partition is on-disk
 *   IDENTICAL whether it was added by ALTER TABLE after those rows were written
 *   (zero-fill is the CORRECT answer) or its write never landed (zero-fill is
 *   FABRICATION).  Block counts cannot separate them — there is nothing to
 *   count.  The stamp on the TS column's idx separates them: ts is published
 *   LAST, so its stamp is the partition's "a complete K-column flush landed
 *   here" marker, and a column with index < K was written here and is missing.
 *
 * IdxHeader v2 (36 bytes, current writer — 2026-04):
 *   ... v1 fields ...
 *   file_ts_min i64  (zone map — rows < this definitely absent)
 *   file_ts_max i64  (zone map — rows > this definitely absent)
 *   Enables whole-partition skipping in the query planner without
 *   walking every block's BlockIndexEntry. Reader accepts both v1 and
 *   v2; v1 callers fall back to per-block ts_min/ts_max computation.
 *
 * BlockIndexEntry (40 bytes, little-endian):
 *   offset    u64
 *   size      u32
 *   count     u32
 *   ts_min    i64
 *   ts_max    i64
 *   _reserved u64
 */
#ifndef TSDB_STORAGE_PART_H
#define TSDB_STORAGE_PART_H

#include "schema.h"
#include "memtable.h"
#include "../../include/tsdb.h"
#include "../core/types.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Magic constants. */
#define TSDB_BLOCK_MAGIC  0x314B4C42u  /* "BLK1" LE */
#define TSDB_IDX_MAGIC    0x31584449u  /* "IDX1" LE */
#define TSDB_IDX_VERSION  4            /* writer version: v4 adds durable max commit-seq
                                        * (WAL redo checkpoint); v3 added per-block stats */

/* Block header on-disk size. */
#define TSDB_BLOCK_HEADER_SIZE  32u

/* Block flag bits (bytes 6..7 of the BlockHeader, little-endian u16).
 *
 * Bits 0..2 are owned by the codec layer (TSDB_BF_OUTER_LZ / NOT_NULL /
 * HAS_BLOOM in compress/codec.h).  Block-layer flags start at bit 3. */
#define TSDB_BLOCK_FLAG_HAS_CRC   (1u << 3)  /* compressed data is followed by
                                                4 bytes of CRC32C; reader must
                                                verify before decoding.  Old
                                                blocks have flag=0 and no
                                                trailer (forward-compat). */
#define TSDB_BLOCK_CRC_TRAILER_SIZE 4u

/* IN-MEMORY ONLY (never written to a block header, never read from one).
 * Set by tsdb_part_open on a synthesised meta whose offset is UINT64_MAX to
 * distinguish the two kinds of "this column has no block here":
 *   flag clear → ALTER TABLE ADD COLUMN: the rows pre-date the column, so
 *                zero IS the value and the readers zero-fill;
 *   flag set   → HOLE: the column lost a block ts still has (a dropped
 *                raw-block replication push, a half-finished migration), so
 *                the value is UNKNOWN and the readers return
 *                TSDB_ERR_CORRUPT rather than fabricating one. */
#define TSDB_BLOCK_FLAG_HOLE      (1u << 4)

/* IN-MEMORY ONLY, same rules as HOLE above.  The third kind of "this column has
 * no usable block here": the slot's ORDINAL paired with the ts block's, but the
 * entry it named describes different rows (count/ts_min/ts_max disagree).  That
 * is structural corruption of the block index, not a missing block, and a
 * caller acts on it differently — a HOLE is repaired by re-syncing the column,
 * a mismatch says the two indexes disagree about what the partition contains.
 * Kept apart so the read path can name it (COLPAIR_MISMATCH in query/exec.c)
 * instead of describing it as a short column. */
#define TSDB_BLOCK_FLAG_MISMATCH  (1u << 5)

/* Both of the above mean "offset == UINT64_MAX carries no value to serve", as
 * opposed to the ALTER zero-fill sentinel which does. */
#define TSDB_BLOCK_FLAG_NO_VALUE  (TSDB_BLOCK_FLAG_HOLE | TSDB_BLOCK_FLAG_MISMATCH)

/* Index header sizes.
 *   V1 = legacy (no zone map)
 *   V2 = adds file-level ts zone map
 *   V3 = adds explicit entry_size + stats_variant so stats layout can
 *        evolve without another header bump. */
#define TSDB_IDX_HEADER_SIZE_V1  20u
#define TSDB_IDX_HEADER_SIZE_V2  36u
#define TSDB_IDX_HEADER_SIZE_V3  40u
#define TSDB_IDX_HEADER_SIZE     48u   /* V4: V3 + max_seq u64 at [40..47] */

/* Block index entry on-disk sizes.
 *   V1/V2 entries = 40 bytes (offset,size,count,ts_min,ts_max,bloom).
 *   V3 entries    = 88 bytes (V1/V2 prefix + 48 bytes of column stats). */
#define TSDB_IDX_ENTRY_SIZE_V2  40u
#define TSDB_IDX_ENTRY_SIZE     88u   /* V3 */

/* Stats payload (bytes 40..87 of a V3 BlockIndexEntry) — see comment at
 * the top of part.c for full layout. */
#define TSDB_IDX_STATS_BYTES  48u

/* ---- Durable block ordinal (entry bytes [82..87]) --------------------------
 *
 * A block's CONTENT — (ts_min, ts_max, count) — is not a key.  Equal timestamps
 * are kept in insertion order and a flush cuts a partition into independent
 * block_points chunks, so two full chunks of one repeated timestamp are
 * genuinely different blocks with a byte-identical key.  Every path that paired
 * or deduplicated on that key therefore mispaired or silently dropped data.
 *
 * The identity is now DURABLE: each entry records the block's PARTITION-LOCAL
 * ordinal — its position in the partition's ts block sequence, shared by every
 * column of the same rows — in the six bytes the V3 entry already reserved:
 *
 *   [82..85]  ordinal u32
 *   [86..87]  marker  u16 == TSDB_IDX_ORD_MARK when [82..85] is meaningful
 *
 * No entry-size change and no idx version bump: the bytes were reserved and
 * written as zero by every previous writer, and a marker of 0 is exactly the
 * "this entry predates the ordinal" state.  Note the marker lives in the ENTRY;
 * the column-count stamp (below) lives in the HEADER at [10..11] — different
 * fields, different writers, neither disturbs the other.  An OLD binary never
 * reads [82..87], so it ignores the ordinal entirely and keeps behaving exactly
 * as it does today. */
#define TSDB_IDX_ORD_MARK  0x4F52u   /* 'R','O' little-endian — "ordinal recorded" */

/* One block's durable ordinal.  `mark == TSDB_IDX_ORD_MARK` means `v` is
 * meaningful; mark == 0 (the all-zero / memset default) means UNKNOWN — a
 * legacy entry, or a sender that predates the field.  Kept as a struct so a
 * copy between a meta and a wire push is a single assignment that cannot
 * forget half of the pair. */
typedef struct {
    uint32_t v;
    uint16_t mark;
} tsdb_block_ord_t;

#define TSDB_ORD_KNOWN(o)  ((o).mark == TSDB_IDX_ORD_MARK)

/* Column-stats flag bits (bytes 80..81 of a V3 entry, little-endian u16). */
#define TSDB_STATS_HAS_MIN_MAX    0x0001u
#define TSDB_STATS_HAS_SUM        0x0002u
#define TSDB_STATS_HAS_FIRST_LAST 0x0004u

/* Block metadata returned to query layer. */
typedef struct {
    uint64_t offset;   /* byte offset of BlockHeader in .col file */
    uint32_t size;     /* compressed data bytes */
    uint32_t count;    /* number of values */
    int64_t  ts_min;
    int64_t  ts_max;
    uint8_t  codec;
    uint16_t flags;    /* TSDB_BF_* bitmask from block header */
    uint64_t bloom;    /* 64-bit bloom filter (TSDB_BF_HAS_BLOOM must be set); 0 otherwise */

    /* Per-column precomputed statistics (V3 stats payload).
     *
     * Interpretation depends on the column type:
     *   TIMESTAMP/INT64 → min/max/sum/first/last are int64 values
     *                     stored in the 8-byte fields below.
     *   FLOAT64         → same fields hold IEEE 754 doubles (reinterpret).
     *   SYMBOL          → stats_flags == 0 (absent); use bloom instead.
     *
     * stats_flags indicates which fields are valid.  0 means "absent"
     * (V1/V2 entry, a SYMBOL column, or a widened legacy entry). */
    int64_t  stats_min;
    int64_t  stats_max;
    int64_t  stats_sum;
    int64_t  stats_first;
    int64_t  stats_last;
    uint16_t stats_flags;

    /* Durable partition-local ordinal (entry bytes [82..87], see above).
     * tsdb_part_open leaves this UNKNOWN for a legacy entry.  After the
     * alignment pass every column's array is 1:1 with the ts column's, so the
     * reader addresses a sibling by ARRAY POSITION and only validates the
     * content — the ordinal itself is what tsdb_part_open aligned with, and
     * what the replication / migration paths carry over the wire. */
    tsdb_block_ord_t ord;
} tsdb_block_meta_t;

/*
 * Flush the memtable to disk.
 * Creates partition subdirectories as needed; groups rows by day.
 * Each group is chunked into blocks of at most TSDB_BLOCK_POINTS points.
 *
 * db / table_name are only used for the on_raw_block hook.
 * Pass (NULL, NULL) for standalone (non-cluster) usage.
 */
int tsdb_part_flush(tsdb_schema_t *s, tsdb_memtable_t *m);
int tsdb_part_flush_ex(tsdb_schema_t *s, tsdb_memtable_t *m,
                       struct tsdb_db *db, const char *table_name);
/*
 * Like tsdb_part_flush_ex but stamps each written partition's idx with a
 * durable max commit-sequence (the WAL redo checkpoint).  Every column idx
 * published by this flush records max(existing_max_seq, max_seq), so on
 * reopen the recovery path can skip WAL records whose seq <= that value
 * (already durable in the partition).  Pass max_seq == 0 to leave the
 * checkpoint untouched (used by the non-redo / default flush path).
 */
int tsdb_part_flush_ex2(tsdb_schema_t *s, tsdb_memtable_t *m,
                        struct tsdb_db *db, const char *table_name,
                        uint64_t max_seq);

/*
 * Read the maximum durable commit-sequence checkpoint recorded across all
 * columns of a single partition directory (the max over every <col>.idx
 * v4 header).  Returns 0 if the partition predates v4 (no checkpoint) or
 * has no idx files.  Used by recovery to compute C for the WAL seq>C skip.
 */
uint64_t tsdb_part_max_seq(tsdb_schema_t *s, const char *partition_dir);

/* "no writer has recorded a column count in this idx header." */
#define TSDB_IDX_NCOLS_UNKNOWN 0u

/*
 * The ONE canonical idx-header encoder, shared by the flush path and the
 * raw-block replication path so both stamp byte-identical headers and select
 * V3 (max_seq==0, 40 bytes) vs V4 (max_seq>0, 48 bytes) the same way.  `buf`
 * must hold at least TSDB_IDX_HEADER_SIZE bytes.  Returns the header size.
 *
 * `ncols` is the column-count stamp documented at the top of this file.  Pass
 * the schema's column count ONLY from a writer that writes every column of the
 * partition (the flush); every other writer must pass the value it read back
 * from the file it is republishing, so a one-column publish never invents the
 * claim.  TSDB_IDX_NCOLS_UNKNOWN leaves the field unasserted.
 */
size_t tsdb_part_write_idx_header(uint8_t *buf, uint32_t count,
                                  uint64_t total_rows,
                                  int64_t file_ts_min, int64_t file_ts_max,
                                  uint64_t max_seq, uint16_t ncols);

/*
 * Read just the column-count stamp out of an idx header.  Returns
 * TSDB_IDX_NCOLS_UNKNOWN when the file is absent, short, corrupt, pre-V3 (only
 * V3+ writers can have stamped it) or carries an out-of-range value.  Writers
 * republishing someone else's idx use this to carry the stamp forward.
 */
uint16_t tsdb_part_idx_ncols(const char *idx_path);

/*
 * Probe an existing idx file's header (version, entry size, zone map, total
 * rows, durable max_seq), applying the mixed-writer header-size recovery so a
 * V3/V4-mongrel reports values consistent with where its entries live.
 * Returns the (recovered) header size, 0 if absent/short, -1 if corrupt.
 * Out-params may be NULL.  The raw-block writer uses this to PRESERVE a
 * partition's idx version and carry its max_seq forward (no silent downgrade).
 */
int tsdb_part_idx_probe(const char *idx_path,
                        uint16_t *out_version, uint32_t *out_count,
                        uint32_t *out_entry_size, uint64_t *out_total_rows,
                        int64_t *out_file_ts_min, int64_t *out_file_ts_max,
                        uint64_t *out_max_seq);

/*
 * Serialise the read-modify-write publish of any <col>.idx inside ONE
 * partition directory.  Striped by a hash of part_dir over a fixed mutex
 * table, so different partitions never contend.
 *
 * Every <col>.idx publish is a full read-modify-write (read all old entries,
 * append, rewrite via tmp+fsync+rename).  Two of them running concurrently on
 * the same file both read N and both write N+1: one entry is silently lost.
 * When the loser is a NON-ts column and ts survives, that is exactly the
 * multi-column hole this lock exists to prevent.  Both writers of an idx take
 * it: col_writer_close (flush/compaction) and tsdb_rawblock_apply_ex
 * (replication/migration).
 *
 * LOCK ORDER: innermost.  Never acquire compact_mtx, db->lock or batch_mu
 * while holding it.  It is deliberately NOT compact_mtx: the raw-block hook
 * fires from inside tsdb_part_flush_ex2 while the SENDER holds its own
 * compact_mtx across a blocking RPC, so making the RECEIVER's applier take
 * compact_mtx would let two nodes that flush-and-push to each other stall for
 * the full replication timeout and then drop the block — a self-inflicted
 * version of the very failure being fixed.  The sender never holds this lock
 * during the hook (it is taken only around the idx RMW), so no cycle exists.
 */
void tsdb_part_idx_lock(const char *part_dir);
void tsdb_part_idx_unlock(const char *part_dir);

/*
 * Commit test for the partition's visibility marker.
 *
 * Answers: may a ts block with key (ts_min, count) be published into this
 * partition without creating a block the reader cannot pair?  The key is
 * exactly the one exec.c's block matcher uses, and the accept/reject rule is
 * exactly the classification tsdb_part_open's alignment pass applies, so a
 * writer can never publish something the reader would refuse.
 *
 *   TSDB_OK        every non-ts column already carries this key, or has no
 *                  blocks here at all in a partition that ts has already
 *                  published into (ALTER TABLE ADD COLUMN — tsdb_part_open
 *                  zero-fills those by design).
 *   TSDB_ERR_BUSY  at least one column HAS blocks here but not this one, or a
 *                  column has NO blocks in a partition ts has not published
 *                  into either (a whole column's group never arrived, which
 *                  reads back as fabricated zeros).  The caller must not
 *                  publish.  out_missing_col (may be NULL) names the column.
 *   TSDB_ERR_IO    a probe failed; treat as not-ready.
 *
 * Caller MUST hold tsdb_part_idx_lock(part_dir).  Returns TSDB_OK immediately
 * for a single-column schema.
 */
int tsdb_part_ts_publish_ready(tsdb_schema_t *s, const char *part_dir,
                               int64_t ts_min, uint32_t count,
                               char *out_missing_col, size_t cap);

/*
 * The partition's NEXT FREE ordinal in THIS NODE's space: max(effective
 * ordinal) + 1 over every column's idx, and over the remote-ordinal map's
 * durable high-water.  0 for a partition that holds nothing.
 *
 * Every allocator of an ordinal — the flush, compaction, the replication
 * applier — goes through this one function, so the space stays monotone and
 * collision-free for the partition's LIFETIME.  Monotone locally is all it has
 * to be; see tsdb_part_ord_translate.
 *
 * Reads files without locking.  Callers that need it exclusive hold
 * tsdb_part_idx_lock(part_dir); the flush does not, exactly as it did not
 * before, so a flush racing an applier for the same partition can still land on
 * one number twice (last entry wins on the read side, no rows lost).
 */
uint32_t tsdb_part_next_ordinal(const tsdb_schema_t *s, const char *part_dir);

/*
 * Map a SENDER's partition-local block ordinal into THIS node's ordinal space.
 *
 * The ordinal names a block group inside ONE partition on ONE node.  It is not,
 * and cannot be made into, a cross-node identity: every node compacts
 * independently and never replicates the result, and the receiver's own flush
 * allocates out of the same range, so a number issued elsewhere means nothing
 * here.  Believing one is how a replica-side compaction came to consume exactly
 * the ordinals the primary was about to issue.
 *
 * The mapping is keyed on (ISSUER, remote ordinal, ts_min, ts_max, count).  The
 * last four are the fields every column of one flush block carries identically,
 * so every column of a group translates to the SAME local ordinal and the group
 * stays pairable.  `issuer` scopes them to the node that issued the ordinal:
 * without it, two senders flushing the same timestamp grid into one table+day
 * both start at ordinal 0, their groups collapse onto one local ordinal, and the
 * second sender's ts block — byte-identical to the first — is ACKed and
 * discarded as a re-delivery while its value block silently overwrites the
 * first's.  Pass the sending node's id; 0 means "this node's own stream" (the
 * migration importer and the restore replay, which have exactly one source).
 *
 * It is durable (<part>/.ordmap) and is recorded BEFORE the block is written,
 * so a crash costs a skipped ordinal, never a second identity for one group.
 *
 * `remote.mark == 0` (a sender predating the field) yields an unknown local
 * ordinal and records nothing: there is no identity to translate.
 *
 * Caller MUST hold tsdb_part_idx_lock(part_dir).
 * Returns TSDB_ERR_CORRUPT if a map exists here that this build cannot read,
 * and TSDB_ERR_IO if the record could not be made durable — in both cases the
 * caller must write nothing and let the sender retry.
 */
int tsdb_part_ord_translate(const tsdb_schema_t *s, const char *part_dir,
                            uint64_t issuer, tsdb_block_ord_t remote,
                            int64_t ts_min, int64_t ts_max, uint32_t count,
                            tsdb_block_ord_t *out_local);

/*
 * Drop this partition's remote-ordinal map.
 *
 * For the ONE writer that does not extend the partition's ordinal space but
 * REPLACES it: tsdb_cluster_backfill_partition_from_result rebuilds a partition
 * from a peer's row set in an empty scratch dir, so the rebuilt blocks are
 * stamped 0..k-1 and every previously-issued local ordinal now names rows that
 * are no longer there.  A mapping onto the old numbering does not survive that
 * in any meaningful form — the rows behind local ordinal N are simply not the
 * rows the sender's group N holds any more — so the map is removed rather than
 * rewritten, and the next delivery of each group allocates a fresh ordinal and
 * lands as a new group.
 *
 * Removing it can only cost a re-delivered group a second local ordinal (an
 * over-count anti-entropy can see); keeping it costs the sender's rows.
 */
void tsdb_part_ord_reset(const char *part_dir);

/*
 * As tsdb_part_ts_publish_ready, but tested on the block's DURABLE ORDINAL.
 *
 * (ts_min, count) is not a key: on a duplicate-timestamp run every block of the
 * run carries the same one, so "some sibling block matches" is satisfied by the
 * WRONG sibling and the ts block publishes over a group that column has not
 * received.  When `ord` is known this requires the sibling with that exact
 * ordinal, and still requires its (ts_min, count) to agree.
 *
 * `ord.mark == 0` (unknown — a sender predating the field) falls back to the
 * key test, which is what tsdb_part_ts_publish_ready passes.
 */
int tsdb_part_ts_publish_ready_ord(tsdb_schema_t *s, const char *part_dir,
                                   tsdb_block_ord_t ord,
                                   int64_t ts_min, uint32_t count,
                                   char *out_missing_col, size_t cap);

/*
 * Repair an ALREADY-torn partition: lower <ts>.idx to the longest block
 * PREFIX every column can pair with, republished via tmp+fsync+rename with the
 * idx version and max_seq preserved (never downgrades V4).
 *
 * Forward-only and idempotent: a second call retracts 0.  Deletes NOTHING —
 * the .col bytes and the non-ts idx entries beyond the new count stay on disk,
 * so a later push re-lands the missing block through the existing dedup and
 * the partition heals upward.  Lowering the visibility marker is what makes
 * the gap visible to anti-entropy, which compares (count, max_ts) and is
 * otherwise structurally blind to a missing value column.
 *
 * A column with ZERO entries in this partition is treated as an ALTER-added
 * column (the same call the reader makes) and never forces a retraction.
 *
 * NOT called from tsdb_part_open: reads must not mutate.  Call it from a
 * restart-time repair sweep or an operator tool.  *out_retracted (may be NULL)
 * receives the number of ts blocks dropped.
 */
int tsdb_part_ts_retract_unpaired(tsdb_schema_t *s, const char *part_dir,
                                  uint32_t *out_retracted);

/* Opaque partition handle (for reading). */
typedef struct tsdb_part tsdb_part_t;

/* Open a partition directory for reading. */
/* Compute the V3 per-block stats payload (min/max/sum/first/last/flags) for a
 * decoded column chunk.  Exposed so compaction stamps the SAME stats a flush
 * would; SYMBOL and count==0 yield an all-zero payload (stats_flags == 0,
 * meaning "absent"). */
void tsdb_part_compute_block_stats(tsdb_type_t type, const void *raw_vals,
                                   size_t count, tsdb_block_meta_t *out);

int  tsdb_part_open(tsdb_schema_t *s, const char *partition_dir, tsdb_part_t **out);

/* Close a partition handle. */
void tsdb_part_close(tsdb_part_t *p);

/*
 * Return all block metadata for a column.
 * *out_arr is malloc'd by callee; caller must free(*out_arr).
 */
int tsdb_part_col_blocks(tsdb_part_t *p, int col_idx,
                         tsdb_block_meta_t **out_arr, size_t *out_n);

/*
 * Decompress one block into out_buf.
 * out_buf must hold at least (meta->count * tsdb_type_width(col_type)) bytes.
 */
int tsdb_part_read_block(tsdb_part_t *p, int col_idx,
                         const tsdb_block_meta_t *meta, void *out_buf);

/*
 * Zero-copy variant of tsdb_part_read_block.
 *
 * On success *out_data points at meta->count decoded values:
 *   - *out_owned == NULL: the block was served ZERO-COPY — *out_data points
 *     straight into the partition's PROT_READ mmap of the .col file (RAW
 *     codec, no outer-LZ wrap, payload naturally aligned for the column
 *     type).  The pointer is read-only and valid only until
 *     tsdb_part_close(p); the caller must keep the partition open across
 *     consumption and must NOT write through or free it.
 *   - *out_owned != NULL: the block needed decoding (or the zero-copy gate
 *     rejected it); *out_owned == *out_data is a malloc'd buffer the caller
 *     frees after use.
 *
 * Integrity checks (header cross-check, CRC trailer) are identical to
 * tsdb_part_read_block.  The fast path is disabled process-wide by setting
 * env TSDB_ZEROCOPY_READ=0 (latched at tsdb_part_open time; default ON).
 */
int tsdb_part_read_block_ref(tsdb_part_t *p, int col_idx,
                             const tsdb_block_meta_t *meta,
                             const void **out_data, void **out_owned);

/*
 * Process-wide zero-copy read counters (monotonic, relaxed): blocks served
 * as direct mmap pointers vs. blocks that fell back to a decode/copy.
 * Test/observability aid; either out-param may be NULL.
 */
void tsdb_part_zerocopy_stats(uint64_t *out_hits, uint64_t *out_fallbacks);

/*
 * Return a read-only pointer to the mmap'd .col file for a column.
 * *out_map / *out_len are set to NULL/0 if the column file is not mapped.
 * Caller must NOT free the returned pointer — it is owned by the partition.
 */
void tsdb_part_col_map(const tsdb_part_t *p, int col_idx,
                        const uint8_t **out_map, size_t *out_len);

/*
 * File-level zone map: the min/max timestamp covering every row in this
 * partition, across every column. Derived from the IdxHeader v2 `file_ts_*`
 * fields when available, otherwise computed from per-block metadata of
 * the ts column as a one-time fallback.
 *
 * Returns TSDB_OK and fills out_ts_min, out_ts_max on success.
 * Returns TSDB_ERR_NOTFOUND if no data has been written to this partition.
 */
int tsdb_part_zone_map(tsdb_part_t *p, int64_t *out_ts_min, int64_t *out_ts_max);

/* Accessor: the schema this partition was opened against. */
tsdb_schema_t *tsdb_part_schema(const tsdb_part_t *p);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_STORAGE_PART_H */
