/* dr_forwarder.h — cross-DC async replication sender.
 *
 * Wire format: the sender ships WRITE_BATCH-shaped payloads over the
 * new TSDB_RPC_FED_INGEST opcode.  Receive-side applies them with the
 * local_only flag so the batch does not re-enter the intra-cluster
 * replication fanout on the remote DC — i.e. the replication channel
 * is strictly one-way by design.
 *
 * Durability model: **best-effort async**.  The forwarder keeps a
 * bounded in-memory ring buffer; on process exit the backlog is
 * dropped.  If this is the wrong trade-off for an operator (e.g. a
 * financial system needing RPO=0), the next phase should add an
 * on-disk WAL tail.  The drop counter is exposed via /metrics so the
 * gap is always observable.
 *
 * Conflict resolution: last-write-wins by timestamp.  tsdb's memtable
 * insertion order determines visibility of identical (ts, tags) rows;
 * the receiver does not deduplicate because it can't distinguish "same
 * row twice" from "different row with same ts" without a primary key.
 */
#ifndef TSDB_FEDERATION_DR_FORWARDER_H
#define TSDB_FEDERATION_DR_FORWARDER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb_dr_forwarder tsdb_dr_forwarder_t;

typedef struct {
    uint64_t enqueued;   /* accepted into the ring */
    uint64_t sent;       /* RPC sent (may or may not have ACKed) */
    uint64_t ack;        /* ACKed by remote */
    uint64_t fail;       /* RPC call returned error */
    uint64_t dropped;    /* enqueue refused because ring was full */
    int      backlog;    /* current queue depth */
    int      remote_alive; /* 1 if the remote connected since last send */
} tsdb_dr_stats_t;

/* Start the forwarder.  remote_addr is "host:port", e.g.
 * "dc2-coord:28081".  ring_cap bounds the queue depth; 4096 is a
 * reasonable default for low-write workloads, bump for bursty. */
int  tsdb_dr_forwarder_new(const char *remote_addr, int ring_cap,
                            tsdb_dr_forwarder_t **out);

/* Stop the background thread and free the handle.  Safe on NULL. */
void tsdb_dr_forwarder_free(tsdb_dr_forwarder_t *fw);

/* Enqueue one WRITE_BATCH payload for async delivery.  Returns
 * TSDB_OK on accept, negative if the ring was full (caller must
 * already have bumped the drop metric or will do so on their own). */
int  tsdb_dr_forwarder_enqueue(tsdb_dr_forwarder_t *fw,
                                const uint8_t *payload, size_t len);

/* Snapshot current counters.  Lock-free atomic reads on the hot path. */
void tsdb_dr_forwarder_stats(tsdb_dr_forwarder_t *fw, tsdb_dr_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TSDB_FEDERATION_DR_FORWARDER_H */
