/* outbox.h — durable per-stream sequence allocator for the sender.
 *
 * WHAT IT IS FOR.  Dedup only works if a RETRY carries the SAME seq as the
 * original send.  In memory that is trivial; across a crash it is not — a
 * restarted sender that starts counting from 1 again makes every retry look
 * like a brand-new batch, and the receiver's ledger dutifully applies all of
 * them.  So the sender must persist its intent BEFORE the first send, and after
 * a restart must never hand out a seq it may already have used.
 *
 * WHY IT DOES NOT FSYNC PER BATCH.  Reserving one seq at a time would mean a
 * durable write on the hot path.  Instead the allocator RESERVES A CHUNK, makes
 * that reservation durable once, and then hands out seqs from memory until the
 * chunk is exhausted.  After a crash it resumes ABOVE the whole reservation, so
 * a seq is never reused — the price is that unused seqs in the interrupted chunk
 * are SKIPPED.
 *
 * WHY THE SKIP IS SAFE (and why `base` exists).  A hole would normally be fatal
 * to the receiver: its frontier can never advance over seqs that will never
 * arrive, the out-of-order window fills, and the stream jams forever.  It cannot
 * be papered over with a timeout — "nothing arrived for a while" is what a slow
 * batch looks like too, and advancing on that guess loses rows.  So the
 * allocator also publishes `base`: the lowest seq it may still send.  That is a
 * PROMISE from the only party that knows, and the receiver advances its frontier
 * to base-1 on it (tsdb_dedup_advance_base).  The gap-tolerant receiver is
 * exactly what makes this cheap allocator possible; `base` is what keeps the
 * gaps from accumulating.
 *
 * Single writer per file.  No locking here — the caller serialises, the same way
 * it already serialises a table's batch commits.
 */
#ifndef TSDB_OUTBOX_H
#define TSDB_OUTBOX_H

#include <stddef.h>
#include <stdint.h>

typedef struct tsdb_outbox tsdb_outbox_t;

/* `chunk` = how many seqs one durable reservation covers.  Bigger means fewer
 * fsyncs and a bigger skip after a crash; both are bounded by it. */
int  tsdb_outbox_open(const char *path, uint32_t chunk, tsdb_outbox_t **out);
void tsdb_outbox_close(tsdb_outbox_t *ob);

/* Next seq for `stream`, plus the sender's low-water PROMISE.
 *
 * *out_seq  — strictly increasing per stream, never reused across a restart.
 * *out_base — the lowest seq this sender may still send for the stream; the
 *             receiver may advance its frontier to base-1.  Equals 1 until a
 *             restart skips a reservation, then rises to the resume point.
 * Either out-param may be NULL. */
int  tsdb_outbox_next(tsdb_outbox_t *ob, uint64_t stream,
                      uint64_t *out_seq, uint64_t *out_base);

/* The durable reservation high-water for a stream (0 = unknown).  Exposed for
 * tests and telemetry: after a restart the next seq is always above it. */
uint64_t tsdb_outbox_reserved(const tsdb_outbox_t *ob, uint64_t stream);

#endif /* TSDB_OUTBOX_H */
