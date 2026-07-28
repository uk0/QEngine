/* tsdb_restore.h — backup sets and restore-into-a-live-database.
 *
 * /backup already ships a tar.gz of data_dir: a PHYSICAL snapshot whose only
 * restore procedure is "stop the process, extract over data_dir, start again".
 * That works for whole-node disaster recovery and for nothing else — it cannot
 * put one table back, it cannot restore into a running instance, it cannot be
 * resumed after an interruption, and a truncated tar is indistinguishable from
 * a complete one until a query happens to touch the missing bytes.
 *
 * This is the logical half.  A BACKUP SET is a directory:
 *
 *     <backup_dir>/BACKUP.manifest      written LAST, atomically
 *     <backup_dir>/<table>.tsm          one migration stream per table
 *     <backup_dir>/<table>.seq          per-partition WAL redo checkpoints
 *
 * and restore lands those streams into a live tsdb_db_t.
 *
 * DISCIPLINES, and why each is here rather than in the caller:
 *
 *   Absence means complete.  BACKUP.manifest is written last, so a backup
 *   without one is incomplete by construction and tsdb_restore_run refuses it.
 *   Symmetrically, tsdb_restore_run drops <data_dir>/.tsdb_restore before it
 *   writes anything and removes it only when every table landed — so the
 *   ABSENCE of that marker is the only claim that a restore finished.  This is
 *   RocksDB BackupEngine's rule and it is the one that survives a power cut:
 *   nothing has to be written correctly for the failure case to be detected.
 *
 *   ts lands LAST.  <ts>.idx is the partition's visibility marker: count(*)
 *   and max(ts) are answered from it, and exec.c pairs every other column to
 *   it by (ts_min, count).  A restore that publishes ts before the value
 *   columns have landed yields a partition that reports its rows as present
 *   while every query touching those columns reads fabricated zeros — a silent
 *   wrong answer.  Restore therefore lands every non-ts block of the whole
 *   stream first and every ts block afterwards, so an interruption always
 *   leaves the target BEHIND (repairable) and never TORN (not).
 *
 *   Dedup on what the reader pairs on.  Landing skips a block only when the
 *   target already holds one with the same (ts_min, count) in the same
 *   (partition, column) — occurrence for occurrence.  That is exactly the key
 *   exec.c matches on, so "already there" means "the reader already sees it".
 *   Keying on anything else duplicates: see the comment above rst_seen_take in
 *   restore.c for the reachable path this closes.
 *
 *   ...and REFUSE when that key no longer means anything.  (ts_min, count) is
 *   a block BOUNDARY, and this database rewrites boundaries on purpose: the
 *   compactor re-encodes a cold partition into 32768-row blocks and the flush
 *   picks its own off block_points.  Once that has happened the dedup
 *   recognises nothing and a re-run lands the whole stream a second time —
 *   measured 150000 rows -> 300000, rc=0.  So before anything lands,
 *   tsdb_restore_run checks every partition the stream names: the target's ts
 *   keys must be a SUB-MULTISET of the stream's.  Empty target -> land.
 *   Prefix or exact match (a resume, or a re-run nobody rewrote) -> the skip
 *   path, unchanged.  Anything else -> TSDB_ERR_UNSUPPORTED for that table
 *   with the partition and both row counts on stderr.  Matching on ts COVERAGE
 *   instead would survive the rewrite and is not safe: a raw block is opaque
 *   and indivisible so partial coverage has no correct answer, and coverage is
 *   not identity — two ts blocks with the same range and count are an ordinary
 *   two-flush partition, and skipping the second one turns a silent
 *   over-count into a silent LOSS.  Full argument above rst_precheck_rerun.
 *
 *   Restore and report.  One unreadable stream does not abandon the other
 *   tables.  Every table gets an rc in the report; the marker stays behind
 *   unless all of them are TSDB_OK.
 *
 *   VERIFY ANSWERS CONTAINMENT, NOT EQUALITY.  "Is this set intact?" and "is
 *   the database currently byte-for-byte this set?" are different questions
 *   and only the first one has a stable answer.  A source that is still
 *   ingesting stops equalling any set it took the instant the next row lands,
 *   so folding equality into the return code made tsdb_restore_verify answer
 *   TSDB_ERR_CORRUPT for an INTACT set on any live database — measured
 *   rows_backup=2000 vs rows_target=2700 -> rc=-4.  An operator asking "did my
 *   data survive?" during an incident must not be told CORRUPT because the node
 *   kept ingesting.  So the rc means CONTAINMENT — nothing the set carries is
 *   missing or mangled — and the equality answer is reported beside it, per
 *   table, as tsdb_restore_target_rel_t: AHEAD is informational, BEHIND and
 *   DIVERGED are the ones an operator has to act on.
 *
 *   SYMBOL dictionaries and idx v4 max_seq survive.  The blocks of a SYMBOL
 *   column hold dictionary CODES, so a restore that ships them without the
 *   dictionary reads every tag back as zero rows; and dropping a partition's
 *   max_seq checkpoint re-arms WAL replay for records already durable in it.
 *   Both are carried, and max_seq is only ever raised, never lowered.
 *
 *   ...AND SO DOES THE ncols STAMP, which is the same header write.  Bytes
 *   [10..11] of <ts>.idx record how many columns the writer of that partition
 *   knew about: rule 2 of part_col_absence_is_late_add, the only evidence that
 *   separates a column whose blocks are GONE (an error) from one ALTER-added
 *   later (zero-filled by design).  Rule 1 — "some later column has blocks" —
 *   is vacuous for the LAST column, so for a trailing column the stamp is the
 *   only thing between a hole and fabricated zeros.
 *
 *   Every partition a restore CREATES is built by tsdb_rawblock_apply, which
 *   passes the stamp it read back off the file it is republishing — correct for
 *   an applier that publishes one (column, block) per call, and UNKNOWN for a
 *   file that does not exist yet.  So the stamp has to come from somewhere
 *   else: the SOURCE's, carried per partition beside max_seq in the .seq
 *   sidecar.  Without it a restored copy answered rc=0 / sum 0 for a trailing
 *   column the source answered TSDB_ERR_CORRUPT for.  Where the source never
 *   stamped, the restore may assert the count itself — part.h allows "a writer
 *   that writes every column of the partition", which the restore is and the
 *   applier is not — but only for a partition it can SEE it landed every column
 *   of; asserting it over a column the stream carried no block for is how a
 *   legitimate ALTER-added column becomes a permanent read error.
 *
 *   The re-head runs for every partition of the set, not only the ones carrying
 *   a checkpoint, and it writes nothing when neither field changes (the
 *   partition dir's mtime is load-bearing for the compaction memo and the
 *   anti-entropy COLD gate).  A LEGACY idx (V1/V2, 40-byte entries) is not
 *   re-headed at all — the header this writes always declares 88-byte entries,
 *   so re-heading one would re-interpret its whole entry array at the wrong
 *   stride — and it therefore gets neither field.
 *
 * NOT IN THIS VERSION: no incremental/differential backups (every set is
 * full), no compression or encryption of the set, no cross-schema restore
 * (the target's schema must match the stream's), no point-in-time selection
 * (a set is whatever was on disk when it was taken), and un-flushed rows are
 * flushed into the snapshot rather than captured from the WAL.
 *
 * Also not here: HOUR-granularity tables are REFUSED (TSDB_ERR_UNSUPPORTED)
 * because the raw-block applier rebuilds a partition directory from the DAY
 * form of the key and all 24 hourly partitions would land in one directory —
 * refused at BACKUP time, not only at restore, so the operator learns before
 * the incident rather than during it; STRIPED deployments (TSDB_DATA_DIRS set,
 * i.e. tsdb_db_data_dir_count() > 1) are REFUSED with TSDB_ERR_UNSUPPORTED by
 * ALL THREE of tsdb_backup_create, tsdb_restore_run and tsdb_restore_verify —
 * not "covered through data dir 0", which is what a table living on another
 * stripe would silently be omitted from; landing into a partition whose block
 * boundaries no longer match the stream's is REFUSED per table rather than
 * merged (see the dedup discipline above), so re-running a restore over a
 * compacted target, or merging one table back into a database that has kept
 * writing, is an operator procedure — empty data dir, then merge; a torn
 * source partition is REPRODUCED rather than repaired, with its verdict intact
 * (the source's ncols stamp travels beside the stream and is written back, so
 * the copy refuses the same read the source refuses instead of answering it
 * with zeros — restore is not a repair tool, see tsdb_part_ts_retract_unpaired
 * for that), except that a partition the SOURCE never stamped may end up
 * stamped on the copy and therefore STRICTER than the source: an error where
 * the source fabricates zeros, never the other way round;
 * and nothing here coordinates with a cluster's Raft state or catalog, so
 * restoring a node that is a member of a live group is an operator procedure,
 * not an API call.
 */
#ifndef TSDB_RESTORE_H
#define TSDB_RESTORE_H

#include "tsdb.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Written into the TARGET data_dir while a restore is in flight. */
#define TSDB_RESTORE_MARKER    ".tsdb_restore"
/* Written into the BACKUP dir, last, when the set is complete. */
#define TSDB_BACKUP_MANIFEST   "BACKUP.manifest"

/* How hard tsdb_restore_verify looks.  The report always names which one
 * actually ran, because "verified" with the level left implicit is how an
 * index-only check gets mistaken for proof that the bytes decode. */
typedef enum {
    /* Index only: idx headers and block metadata on the target, folded and
     * compared against the manifest.  No .col byte is read and nothing is
     * decompressed.  Catches a missing table, a missing partition, a lost
     * block, a wrong row count.  Blind to a corrupt payload — and blind, for
     * the same reason, to a block the landing dedup SKIPPED because the target
     * already held one with its (ts_min, count) but different bytes.  Only
     * DEEP compares payloads, so only DEEP can tell those two apart. */
    TSDB_RESTORE_VERIFY_INDEX = 0,
    /* Everything the index level does, plus: every block of the backup set is
     * byte-compared against the target's on-disk payload AND decoded through
     * the normal read path (header cross-check + CRC + codec).  Reads both
     * copies in full. */
    TSDB_RESTORE_VERIFY_DEEP  = 1
} tsdb_restore_level_t;

typedef struct {
    char     table[64];
    int      rc;                /* TSDB_OK, or why this table failed */
    uint64_t blocks_seen;       /* records in the stream                      */
    uint64_t blocks_landed;     /* records that actually wrote                */
    uint64_t blocks_skipped;    /* already present — a resume, not a loss     */
    uint64_t rows;              /* ts-column rows in the stream               */
    uint64_t max_seq;           /* highest partition checkpoint carried over  */
} tsdb_restore_table_t;

typedef struct {
    int  ntables;
    int  nfailed;
    int  complete;              /* 1 only when the marker was removed */
    tsdb_restore_table_t *tables;
} tsdb_restore_report_t;

/* How the TARGET currently compares to the SET.  Reported beside `rc`, not
 * folded into it: only BEHIND and DIVERGED mean something the set carries is
 * gone, and only those two make `rc` fail.  See "VERIFY ANSWERS CONTAINMENT"
 * above for why AHEAD cannot be an error.
 *
 * A BARE BLOCK COUNT MEANS NOTHING ON ITS OWN, IN EITHER DIRECTION.  This
 * database re-encodes block boundaries on purpose — the compactor merges a cold
 * partition's blocks into 32768-row ones and tsdb_node_main runs it on a 5 s
 * timer — so "fewer blocks" is the steady state of every partition nobody is
 * writing, not a symptom.  Classifying it as BEHIND made an intact set report
 * TSDB_ERR_CORRUPT on any node whose compactor had run, with a blocks_missing
 * that was a difference of totals rather than a measurement, and made the drain
 * below fire on healthy nodes.
 *
 * BUT "FEWER BLOCKS HOLDING AT LEAST THE SET'S ROWS" IS NOT PROOF OF A
 * RE-ENCODE, AND IT WAS ASSERTED HERE AS IF IT WERE.  The two counts do not
 * measure the same thing: tsdb_migrate_digest sums `rows` over the TS COLUMN
 * ONLY and `blocks` over EVERY column.  A target that loses one value column's
 * blocks therefore keeps every row and drops blocks — bit for bit the signature
 * of a merge — so a database whose SELECT of that column answers
 * TSDB_ERR_CORRUPT was classified REENCODED and certified TSDB_OK at both
 * levels.  Fewer blocks is REENCODED only when nothing on the target is
 * UNREADABLE: tsdb_part_open marks a ts block some column has no value for, and
 * whose absence is not an ALTER-added column, with TSDB_BLOCK_FLAG_HOLE, and
 * tsdb_migrate_digest counts those separately (tsdb_mig_stats_t.holes).  A
 * block deficit explained by holes is BEHIND; one with no holes behind it is
 * REENCODED.  Holes alone are NOT a failure — a set taken from an
 * already-torn source carries the tear, and the copy reproducing it faithfully
 * is containment, not loss — which is why only a deficit they explain fails.
 *
 * It compares the set against the target's PARTITIONS.  A target holding extra
 * rows only in a memtable therefore reads EQUAL rather than AHEAD — the drain
 * that would surface them runs only when the target holds FEWER ROWS than the
 * set (see tsdb_restore_verify), because a check must not write to a database
 * it has no question about.  The conservative direction is the safe one: the
 * value that FAILS is never under-reported, since falling short on disk is
 * exactly what makes the check drain and look again. */
typedef enum {
    /* Same rows, same blocks, same fold.  A quiesced target of this set. */
    TSDB_RESTORE_TARGET_EQUAL    = 0,
    /* The target holds MORE than the set: it kept ingesting, or it carries
     * tables/rows this set was never asked to cover.  NOT set corruption, and
     * the normal state of any live source a moment after the set was taken.
     * The INDEX level cannot prove the set's own blocks are still among the
     * larger population — its fold is an XOR over everything on disk, so extra
     * blocks make it differ without telling you which ones.  DEEP can, and
     * does: it looks up every block of the set individually. */
    TSDB_RESTORE_TARGET_AHEAD    = 1,
    /* Something the set carries is not on disk — act on this one.  Two shapes
     * reach it: FEWER ROWS than the set, and a block deficit the target's own
     * HOLES account for (a column whose blocks are gone, which the reader
     * answers TSDB_ERR_CORRUPT for).  The second used to read as REENCODED. */
    TSDB_RESTORE_TARGET_BEHIND   = 2,
    /* Same row and block counts, different fold: same shape, different blocks.
     * Act on this one too. */
    TSDB_RESTORE_TARGET_DIVERGED = 3,
    /* At least the set's rows, in FEWER blocks, AND nothing on the target is
     * unreadable: the target's block boundaries were rewritten under it — the
     * compactor, normally.  Nothing is known to be missing, so `rc` does not
     * fail.
     *
     * THE "AND NOTHING UNREADABLE" HALF IS LOAD-BEARING and it used not to be
     * checked.  `rows` is a ts-only sum and `blocks` is an all-column count
     * (tsdb_migrate_digest), so a target that lost one value column's blocks
     * produces exactly "at least the set's rows, in fewer blocks" — and was
     * certified TSDB_OK at both levels while the engine answered
     * TSDB_ERR_CORRUPT for that column.  A deficit the target's HOLES account
     * for is BEHIND, not this.
     *
     * It is still the relation for which the INDEX level cannot answer
     * containment: the set's blocks no longer exist AS BLOCKS, so a fold of
     * totals cannot say which of them survived.  DEEP is not blind here and no
     * longer declines — it looks every block of the set up and, for each MISS,
     * asks whether the target's TS column still carries that block's
     * (ts_min, count).  If it does, the partition's layout is intact at that
     * position and a column with no block there has LOST it (blocks_missing);
     * if it does not, that partition really was re-encoded and the block is
     * counted in blocks_unresolved instead — "could not tell", never "all
     * present".  A table can therefore report both at once: one merged
     * partition and one torn partition beside it are separate numbers.
     *
     * A BLOCK THAT IS PRESENT UNDER ITS KEY BUT HOLDS DIFFERENT BYTES IS NOT
     * ASKED THAT QUESTION, because the answer is fixed: the block was found in
     * this very column under that key, so the ts column carries it too.  It is
     * counted straight into blocks_unresolved, on this relation only.  A merge
     * can preserve a boundary (32768 rows merged, then a trailing group of
     * exactly one source block) and the outer-LZ gate is measured per block and
     * per build, so different bytes under an identical key are what a healthy
     * compacted node produces.  DIFFERENT AND UNREADABLE IS NOT SWALLOWED: a
     * block whose .col cannot be read at its own offset, or that the codec, the
     * CRC trailer or the header cross-check rejects, is counted in
     * blocks_mismatched whatever the relation — nothing that re-encodes blocks
     * writes bytes its own reader refuses. */
    TSDB_RESTORE_TARGET_REENCODED = 4
} tsdb_restore_target_rel_t;

/* "equal" | "ahead" | "behind" | "diverged" | "reencoded" — static storage. */
const char *tsdb_restore_target_rel_name(tsdb_restore_target_rel_t rel);

typedef struct {
    char     table[64];
    int      rc;
    uint64_t blocks_checked;
    uint64_t blocks_missing;    /* in the backup, absent from the target      */
    uint64_t blocks_mismatched; /* present but different bytes / undecodable  */
    /* DEEP only, and only on a table holding at least the set's rows in FEWER
     * blocks — the shape a re-encode leaves.  Blocks the check LOOKED AT and
     * could not resolve either way, for one of two reasons:
     *
     *   - the target's ts column no longer carries their (ts_min, count), so
     *     the partition holding them was re-encoded and there is nothing left
     *     to look up and nothing to byte-compare;
     *   - or the block IS there under its key and DECODES, but its bytes differ
     *     — which is equally what a merge that preserved a boundary produces,
     *     since the outer-LZ gate is measured per block and per build.
     *
     * A block that is present and UNREADABLE is not in here; that is
     * blocks_mismatched, on every relation.
     *
     * Not a failure and not a clean bill — read it as the part of
     * `blocks_checked` that produced no answer. */
    uint64_t blocks_unresolved;
    uint64_t rows_backup;
    uint64_t rows_target;
    tsdb_restore_target_rel_t target_rel;  /* informational — see above       */
    int      drained;           /* 1 when the check flushed this table's
                                 * memtable to answer (see tsdb_restore_verify) */
} tsdb_restore_verify_table_t;

typedef struct {
    tsdb_restore_level_t level_ran;
    const char          *level_name;   /* "index" | "deep" — static storage */
    int  ntables;
    int  nfailed;
    tsdb_restore_verify_table_t *tables;
} tsdb_restore_verify_t;

/*
 * Write a full backup set for every table on disk into `backup_dir`
 * (created if absent).  Flushes first — a snapshot that silently omits the
 * unflushed tail is the failure this exists to prevent — and fails if that
 * flush fails.  The set to flush is the set to back up: every table ON DISK is
 * opened and drained, not only the ones this process happens to have opened.
 * Under TSDB_WAL_ONLY_COMMIT a commit is a WAL fsync plus a memtable insert,
 * so a table written before an unclean shutdown and untouched since reopen
 * holds every acked row in its redo log and none on disk — and "back the node
 * up before touching anything" is the first move of every DR runbook.
 *
 * One table's failure does not stop the others; `out` (may be NULL) carries a
 * per-table rc.  BACKUP.manifest is written last and only when every table
 * succeeded, so an incomplete set has no manifest and cannot be restored by
 * accident.
 *
 * Returns TSDB_OK when every table was written, otherwise the first failure.
 */
int tsdb_backup_create(tsdb_db_t *db, const char *backup_dir,
                       tsdb_restore_report_t *out);

/*
 * Land every table of `backup_dir` into `db`.
 *
 * Idempotent and resumable AS LONG AS NOTHING HAS REWRITTEN THE TARGET'S BLOCK
 * BOUNDARIES.  Re-running lands only what is missing: a block whose
 * (ts_min, count) the target already holds is skipped, occurrence for
 * occurrence, and the result is reported so tsdb_restore_verify can be run
 * against it.  That recognition is only possible while the keys survive, so
 * the guarantee is CONDITIONAL and the condition is enforced rather than
 * assumed — before landing anything, each partition the stream names must find
 * the target's ts keys empty or a sub-multiset of the stream's.  A partition
 * the compactor has re-encoded, or one already holding rows this set does not
 * carry, fails that test and the table is refused with TSDB_ERR_UNSUPPORTED;
 * the alternative was landing the whole stream on top of itself and doubling
 * count(*) with rc=0.  Restore that table into an empty data dir instead.
 *
 * INTERACTION WITH tsdb_restore_verify.  That check drains a table's memtable
 * when — and only when — the target's partitions hold FEWER rows or blocks than
 * the manifest (see there).  A drain publishes blocks whose (ts_min,count)
 * boundaries come from the target's own memtable, not from this stream, so a
 * table that verify DRAINED can fail the sub-multiset test above and be refused
 * here with TSDB_ERR_UNSUPPORTED.  The refusal is correct when it fires — those
 * rows really are rows this set does not carry — but the operator has to know
 * the diagnostic can be what put them on disk.  verify reports it per table
 * (`drained`) and says so on stderr at the moment it happens, rather than
 * letting the restore be the first news of it.
 *
 * Returns TSDB_OK only when every table landed; otherwise the first failing
 * table's rc, with `out` (may be NULL) naming each.  On any failure the
 * .tsdb_restore marker is left in place.
 */
int tsdb_restore_run(tsdb_db_t *db, const char *backup_dir,
                     tsdb_restore_report_t *out);

/*
 * Check a backup set against a database, at the requested level.
 *
 * THE QUESTION THIS ANSWERS is "is everything this SET carries still present
 * and readable in the target?" — containment, not equality.  A target that has
 * kept ingesting holds MORE than the set and is reported TSDB_OK with
 * target_rel == TSDB_RESTORE_TARGET_AHEAD (unless its own block slots are
 * unreadable — see WHAT EACH LEVEL CAN BACK); a target missing rows the set
 * carries is TSDB_ERR_CORRUPT with BEHIND.  Equality is a question about an
 * instant, not about a set, and folding it into the rc made this call answer
 * CORRUPT for an intact set on any live database.  If you want equality, read
 * target_rel and require EQUAL.
 *
 * WHAT EACH LEVEL CAN BACK.  INDEX folds the target's block metadata into one
 * XOR digest and compares counts and fold.  That proves EQUAL and detects
 * BEHIND and DIVERGED, but under AHEAD it cannot say which blocks the extra
 * ones are, so it cannot prove containment: rc is TSDB_OK because nothing is
 * known to be missing, and the honest answer to "are the set's own blocks
 * still there" at that level is "run DEEP".  DEEP looks up every block of the
 * set individually — blocks_missing / blocks_mismatched are the containment
 * answer and they are unaffected by how much extra the target holds.
 *
 * WITH ONE EXCEPTION, and it is the live cluster node this call exists for: an
 * AHEAD target whose own block slots are UNREADABLE.  Extra blocks make a
 * deficit impossible, so the hole axis that catches a lost value column on a
 * quiesced target (see TSDB_RESTORE_TARGET_BEHIND) can never fire there, and
 * INDEX used to hand back TSDB_OK for a database whose SELECT of that column
 * returns TSDB_ERR_CORRUPT — measured, not hypothetical.  It is not proof of
 * loss either: a set taken from an already-torn SOURCE carries the tear, and a
 * copy reproducing it faithfully and then ingesting one more row lands in
 * exactly this shape.  So INDEX now returns TSDB_ERR_INDETERMINATE for that
 * table — "unreadable slots, and this level cannot tell whose" — and DEEP,
 * which asks the question per block, clears it as soon as it resolves anything.
 *
 * A COUNT NOBODY TOOK IS NOT A ZERO.  Two rules keep that from being asserted
 * again:
 *
 *   DEEP does not decline on a re-encoded target.  It used to, on the ground
 *   that a merged partition's blocks "no longer exist AS BLOCKS" so there was
 *   nothing to compare — true of a merged partition and NOT true of the target
 *   that reaches the same classification by losing a whole value column's
 *   blocks (`rows` is ts-only, `blocks` is all-column, so a lost column looks
 *   exactly like a merge).  DEEP now runs and separates the two per block: a
 *   miss whose (ts_min, count) the target's TS column still carries is a LOSS
 *   (that partition's layout is intact and this column has no block there); a
 *   miss whose ts key is gone too is a re-encode and lands in
 *   blocks_unresolved, as does a block that is present under its key and
 *   decodes but holds different bytes.  A block that is present and UNREADABLE
 *   is blocks_mismatched on every relation.  That is per BLOCK, so one merged
 *   partition no longer switches the check off for the intact partitions
 *   beside it.
 *
 *   And a target block is CLAIMED by the record that matched it.  (ts_min,
 *   count) is not a unique block identity — equal timestamps are kept in
 *   insertion order and a flush splits on block_points — so a partition can
 *   hold two blocks with the same key, and an unconsumed first-match let one
 *   survivor answer for both when the other was lost: checked=4, missing=0,
 *   unresolved=0, TSDB_OK, for a column reading back half fabricated zeros.
 *   Each record now takes a block no earlier record took, so the second falls
 *   through to blocks_missing.
 *
 *   And DEEP never answers TSDB_OK having measured nothing.  A DEEP pass that
 *   looked at zero blocks while the manifest claims some has not cleared the
 *   set — it has failed to look at it — and it returns TSDB_ERR_INDETERMINATE
 *   rather than the TSDB_OK an operator reads as "your data survived".
 *
 * IT DRAINS ONLY A TARGET HOLDING FEWER ROWS THAN THE SET.  The comparison
 * reads partition files, so a table whose acked rows are still in a memtable —
 * or, before anything opens it, only in its redo log — digests short and would
 * be called CORRUPT on exactly the DR path this feature exists for: partitions
 * empty, 1000 rows in the WAL, digest 0 against a manifest of 1000.  So when
 * the ROW count comes back short of the manifest the table is opened and
 * flushed and the fold is retaken.  When it does not come back short, nothing
 * is written: a live source is measured as it lies.
 *
 * ROWS, and not the block count.  A flush moves rows out of a memtable; how
 * many blocks the partition ends up holding is the target's own business, and
 * the compactor takes it DOWN — it merges a cold partition into 32768-row
 * blocks, on a 5 s timer.  Triggering on `blocks` therefore drained every
 * healthy node whose compactor had run, and reported the intact set BEHIND with
 * a blocks_missing that was a difference of totals.  There was never anything
 * for the flush to add: the rows were already on disk.
 *
 * That bound matters twice over —
 *
 *   a drain is a real flush, and a memtable flushes exactly ONCE.  It fires the
 *   cluster replication hook (db.c: suppressing it "silently dropped them from
 *   replication for good"), so on a cluster a check that drains also ships
 *   rows.  Draining only a target short of ROWS means a healthy or
 *   still-ingesting node is never touched, whatever its compactor has done to
 *   its block layout; draining one that IS short with the hook suppressed would
 *   be worse than shipping, not better — it would delete those rows from the
 *   replication stream permanently.
 *
 *   a drain publishes blocks with the target's own (ts_min,count) boundaries,
 *   which a later tsdb_restore_run can refuse on.  See the note there; the
 *   per-table `drained` flag and a line on stderr say when it happened.
 *
 * It never creates a table, never adopts a dictionary from the stream and never
 * writes a byte of the SET into the target — the only thing it can move is the
 * target's own already-acked rows, from its memtable to its partitions.
 *
 * `out` (may be NULL) reports per-table findings and, in level_ran /
 * level_name, WHICH level actually ran.  Returns TSDB_OK when nothing the set
 * carries was found missing or mangled, TSDB_ERR_CORRUPT when any table's was,
 * TSDB_ERR_INDETERMINATE when no table failed but at least one could not be
 * measured — nothing looked at (see "A COUNT NOBODY TOOK IS NOT A ZERO"), or
 * unreadable block slots this level cannot attribute (see "WITH ONE
 * EXCEPTION") — or an IO error.  Only TSDB_OK means the set was checked and
 * found intact.
 */
int tsdb_restore_verify(tsdb_db_t *db, const char *backup_dir,
                        tsdb_restore_level_t level,
                        tsdb_restore_verify_t *out);

/* 1 when <data_dir>/.tsdb_restore exists — i.e. a restore started here and
 * has not been reported complete.  0 otherwise. */
int tsdb_restore_in_progress(const char *data_dir);

void tsdb_restore_report_free(tsdb_restore_report_t *r);
void tsdb_restore_verify_free(tsdb_restore_verify_t *v);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_RESTORE_H */
