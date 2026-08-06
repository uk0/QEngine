/* dedup.h — exact receiver-side dedup ledger for replicated batches.
 *
 * THE PROBLEM.  WRITE_BATCH carries no identity, so a fanout retry whose ACK
 * was lost is applied twice and the peer holds the rows twice — and no count
 * comparison can repair that afterwards ("local has more" is indistinguishable
 * from "local has unique legitimate writes").  The fix has to stop the SECOND
 * apply, which means the receiver must be able to answer "have I already
 * applied (stream, seq)?" exactly.
 *
 * WHY A SCALAR HIGH-WATER IS WRONG.  The obvious ledger is one number per
 * stream and the test `seq <= high_water`.  It is unsafe here: batches are
 * dispatched by independent per-peer workers with no FIFO guarantee, so seq 7
 * can arrive before seq 5.  Recording 7 as the watermark then makes the later,
 * never-applied seq 5 look "already applied" and its rows are DROPPED — the
 * watermark converts a duplicate-rows bug into a silent MISSING-rows bug, which
 * is strictly worse.  A Bloom filter fails the same way: a false positive is
 * silent row loss.
 *
 * THE SHAPE THAT WORKS.  Per stream keep
 *
 *     frontier : every seq <= frontier is applied — and it only ever advances
 *                over a CONTIGUOUS prefix, so that statement is a proof, not an
 *                assumption.  This is also the GC frontier: once a seq is under
 *                it, its individual record is redundant and is dropped.
 *     gaps[]   : the exact set of applied seqs ABOVE the frontier — the
 *                out-of-order tail, which is what the scalar form loses.
 *
 * Recording seq == frontier+1 advances the frontier and then absorbs whatever
 * run of gaps has become contiguous, which is what keeps `gaps` small: it holds
 * only the genuinely out-of-order window, not history.  Memory is therefore
 * bounded by concurrency, not by uptime — the unbounded-growth objection that
 * blocked this design.
 *
 * FAIL CLOSED.  `gaps` is capped.  When a stream's out-of-order window is full
 * the ledger REFUSES the record (TSDB_ERR_FULL) and does NOT mark the seq seen,
 * so the sender's retry can still deliver it.  Refusing a batch costs a retry;
 * silently accepting one we cannot remember costs rows.
 *
 * Pure and in-memory: no I/O, no locking, no globals.  Persisting the frontier
 * (and replaying the tail) is the caller's job — see the ordering the dedup
 * design requires: append rows -> fsync ONE record carrying id and rows ->
 * publish -> ACK.  Nothing here can be correct-by-itself across a crash; it is
 * the exactness primitive that layer needs, and it is unit-testable alone.
 */
#ifndef TSDB_DEDUP_H
#define TSDB_DEDUP_H

#include <stddef.h>
#include <stdint.h>

/* The stream a batch belongs to: one SENDER's view of one TABLE INCARNATION.
 *
 * Both halves had to become durable and cluster-agreed before this was possible
 * — they are dedup prerequisites 2 and 3.  The issuer alone is not enough: a
 * table's seq numbering restarts at 1 after a DROP+recreate, so without the
 * incarnation the new table's seq 1.. would collide with the old table's
 * history and its rows would be dropped as "already applied".  The incarnation
 * alone is not enough either: two senders replicating the same table each
 * number their own batches from 1.
 *
 * Returns 0 iff either half is 0 (identity unknown) — and 0 means "no stream",
 * so the caller must fall back to applying without dedup rather than inventing
 * an id.  Never returns 0 for two valid halves. */
uint64_t tsdb_stream_id(uint64_t issuer_node_id, uint64_t table_incarnation);

typedef struct tsdb_dedup_ledger tsdb_dedup_ledger_t;

/* max_streams — capacity for distinct stream ids (a stream is one sender's view
 *               of one table incarnation; DROP+recreate mints a new one).
 * max_gap     — the most out-of-order seqs ONE stream may hold above its
 *               frontier before records are refused.  Bounds memory and makes
 *               a stuck sender loud instead of unbounded. */
int  tsdb_dedup_open(size_t max_streams, size_t max_gap,
                     tsdb_dedup_ledger_t **out);
void tsdb_dedup_close(tsdb_dedup_ledger_t *l);

/* 1 = this (stream, seq) is KNOWN applied; 0 = not known.  Never a false 1. */
int  tsdb_dedup_seen(const tsdb_dedup_ledger_t *l, uint64_t stream, uint64_t seq);

/* Record (stream, seq) as applied.
 *   TSDB_OK        — recorded (the caller may apply the batch)
 *   TSDB_ERR_EXISTS— already applied; the caller must DROP the batch
 *   TSDB_ERR_FULL  — out-of-order window exhausted; NOT recorded, caller must
 *                    refuse the batch so the sender retries
 *   TSDB_ERR_INVAL — seq 0 (sequences start at 1) or bad args
 *   TSDB_ERR_NOMEM — out of memory / no stream slot left */
int  tsdb_dedup_record(tsdb_dedup_ledger_t *l, uint64_t stream, uint64_t seq);

/* Highest seq S such that EVERY seq in [1, S] is applied (0 = none).  Safe GC
 * point and the only value that needs to survive a restart. */
uint64_t tsdb_dedup_frontier(const tsdb_dedup_ledger_t *l, uint64_t stream);

/* How many out-of-order seqs this stream currently holds above its frontier —
 * the pressure against max_gap.  Exposed for tests and telemetry. */
size_t   tsdb_dedup_gap_count(const tsdb_dedup_ledger_t *l, uint64_t stream);

/* Restore a stream's frontier after a restart (from the durable checkpoint).
 * Only ever moves it FORWARD: a lower value would re-admit seqs already applied
 * and duplicate rows, which is the bug this file exists to prevent. */
int  tsdb_dedup_set_frontier(tsdb_dedup_ledger_t *l, uint64_t stream,
                             uint64_t frontier);

/* Advance past seqs the SENDER has promised it will never send.
 *
 * A durable sender allocator reserves seqs in chunks, so a crash leaves a hole:
 * it reserved up to 100, used 3, and resumes at 101.  Seqs 4..100 will never
 * arrive.  Left alone that hole is fatal — the frontier can never advance over
 * it, the out-of-order window fills with 101, 102, ... and the stream jams at
 * TSDB_ERR_FULL forever.  It cannot be fixed by a timeout either: "nothing
 * arrived for a while" is exactly what a slow batch looks like, and advancing on
 * that guess is the silent row loss this whole design refuses.
 *
 * `base` is the sender's PROMISE — the lowest seq it may still send.  That is
 * positive evidence, carried by the sender that alone knows the answer, so
 * advancing the frontier to base-1 is safe.  Forward-only, like every other
 * frontier move; a stale base is ignored. */
int  tsdb_dedup_advance_base(tsdb_dedup_ledger_t *l, uint64_t stream,
                             uint64_t base);

/* ---- Durable checkpoint -------------------------------------------------
 *
 * ONLY THE FRONTIER IS PERSISTED, never the out-of-order tail.  That is
 * deliberate, not a shortcut: the frontier is the one value with a standalone
 * meaning ("every seq up to here is applied"), so a stale copy of it is merely
 * conservative — it re-admits batches that were already applied, which the
 * receiver's own WAL replay then reconciles.  Persisting the tail would need it
 * to be fsynced in lockstep with the rows, and the design already has a better
 * mechanism for that: the WAL record carries the batch id alongside its rows, so
 * replay rebuilds the tail exactly.  Checkpoint = fast path; WAL = truth.
 *
 * Written atomically and durably (tmp -> fsync -> rename -> fsync dir), the
 * pattern the node-id and schema writers use.
 *
 * Load FAILS CLOSED: a missing file restores nothing and returns TSDB_OK (a
 * fresh node); a corrupt or truncated one restores NOTHING and returns
 * TSDB_ERR_CORRUPT.  Partially trusting a damaged checkpoint could skip a batch
 * that was never applied — silent row loss — whereas restoring nothing only
 * degrades to today's no-dedup behaviour.  Frontiers are applied through
 * tsdb_dedup_set_frontier, so a stale file can never rewind a live ledger. */
int  tsdb_dedup_checkpoint_save(const tsdb_dedup_ledger_t *l, const char *path);
int  tsdb_dedup_checkpoint_load(tsdb_dedup_ledger_t *l, const char *path);

#endif /* TSDB_DEDUP_H */
