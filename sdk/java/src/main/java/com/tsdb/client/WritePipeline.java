package com.tsdb.client;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;

/**
 * Pipelined WRITE_BATCH writer: keeps up to {@code depth} frames in flight on
 * the parent {@link TsdbClient}'s connection, amortising the network round
 * trip across the window.  Mirrors the Go SDK's {@code WritePipeline}
 * (sdk/go/pipeline.go): the synchronous {@link TsdbClient#writeBatch} pays one
 * RTT per batch, whereas a depth-4 window overlaps sends with outstanding
 * acks.
 *
 * <p><b>In-order acks (protocol assumption).</b>  Ack matching is FIFO: the
 * server (src/server/server.c) runs one loop per connection that reads a
 * frame, fully handles it, sends the WRITE_ACK (or MSG_ERROR) echoing the
 * request's req_id, and only then reads the next frame — so replies arrive
 * strictly in send order.  The echoed req_id is still verified against the
 * oldest in-flight req_id, and any mismatch kills the pipeline as a stream
 * desync.
 *
 * <p><b>Error semantics.</b>
 * <ul>
 *   <li>A server rejection (MSG_ERROR frame) is an application-level failure
 *       of the OLDEST in-flight batch only.  It surfaces as an
 *       {@link IOException} naming that batch's 0-based index; the slot is
 *       freed and the pipeline stays usable.  Because acks lag sends, the
 *       error surfaces on a LATER {@link #write} (or {@link #flush}) than the
 *       call that submitted the rejected rows.</li>
 *   <li>Any transport error — send failure, ack recv failure, or a stream
 *       desync — BREAKS the pipeline: the error is sticky (every subsequent
 *       write/flush rethrows it immediately, with no I/O), and the underlying
 *       socket is closed, because with frames in flight the stream position
 *       is unknowable.  There is deliberately NO auto-reconnect/retry here,
 *       unlike writeBatch's reconnect layer: a broken pipeline has up to
 *       {@code depth} non-idempotent batches in post-send ambiguity (each may
 *       or may not have been applied), so only the caller can decide what is
 *       safe to resend.  The parent client's next call sees the dead socket
 *       and follows its normal reconnect path.</li>
 * </ul>
 *
 * <p>NOT safe for concurrent use: drive it from a single thread.  While a
 * pipeline is open the parent client must not be used either — the connection
 * is shared — and query/writeBatch/login enforce this by throwing
 * {@link IllegalStateException} until {@link #close()} is called.  Always
 * close the pipeline, even after an error, to release the client.
 */
public final class WritePipeline implements AutoCloseable {

    /**
     * Caps the in-flight window.  Beyond a few dozen frames the pipeline is
     * bandwidth-bound, not RTT-bound, and a deeper window only grows the
     * post-failure ambiguity (every un-acked batch is in an unknown state),
     * so {@link TsdbClient#newWritePipeline} silently clamps here.
     */
    public static final int MAX_DEPTH = 64;

    /** One sent-but-unacked frame: its req_id and the 0-based ordinal of the
     *  {@link #write} call that sent it (used to name rejected batches). */
    private static final class Pending {
        final long reqId;
        final int  index;
        Pending(long reqId, int index) { this.reqId = reqId; this.index = index; }
    }

    private final TsdbClient c;
    private final int depth;
    private final ArrayDeque<Pending> inflight = new ArrayDeque<>();
    private IOException dead;   // sticky transport/desync error; null while healthy
    private boolean closed;
    private int nextIndex;      // ordinal assigned to the next batch sent

    WritePipeline(TsdbClient c, int depth) {
        this.c = c;
        this.depth = depth;
    }

    /**
     * Encodes rows (same arguments as {@link TsdbClient#writeBatch}) and sends
     * one WRITE_BATCH frame without waiting for its ack.  When the in-flight
     * window is full it first blocks to consume exactly ONE ack — the oldest
     * in-flight batch's — so a thrown server error refers to that earlier
     * batch, not to the rows passed here (which, in that case, were NOT sent;
     * the caller may retry the same write once the error is handled).  A
     * transport error breaks the pipeline permanently (see class doc).
     */
    public void write(String table, List<TsdbClient.Column> cols, List<TsdbClient.Row> rows)
            throws IOException {
        ensureUsable();
        if (rows.isEmpty()) return;
        // Encode failure (e.g. unsupported column type): nothing sent, the
        // pipeline is unaffected.
        byte[] payload = TsdbClient.encodeBatch(table, cols, rows);
        if (inflight.size() >= depth) readOneAck();
        long reqId;
        try {
            reqId = c.send(TsdbClient.MSG_WRITE_BATCH, (short) 0, payload);
        } catch (IOException e) {
            throw markDead(new IOException("write pipeline: send failed: " + e.getMessage(), e));
        }
        inflight.addLast(new Pending(reqId, nextIndex++));
    }

    /**
     * Blocks until every in-flight batch is acked.  Server rejections do not
     * stop the drain: all remaining acks are consumed and the FIRST error
     * seen is thrown.  A transport error breaks the pipeline and ends the
     * drain early (the remaining acks can never arrive).
     */
    public void flush() throws IOException {
        ensureUsable();
        IOException first = null;
        while (!inflight.isEmpty()) {
            try {
                readOneAck();
            } catch (IOException e) {
                if (first == null) first = e;
                if (dead != null) throw first; // transport: drain is over
            }
        }
        if (first != null) throw first;
    }

    /**
     * Flushes in-flight batches, marks the pipeline closed, and releases the
     * parent client for normal use again — the guard is released even when
     * the flush throws (sticky transport error or a server rejection).
     * Idempotent: subsequent calls are no-ops.
     */
    @Override public void close() throws IOException {
        if (closed) return;
        try {
            flush();
        } finally {
            closed = true;
            if (c.activePipeline == this) c.activePipeline = null;
        }
    }

    private void ensureUsable() throws IOException {
        if (closed) throw new IllegalStateException("write pipeline is closed");
        if (dead != null) throw dead; // sticky: rethrown as-is, no I/O
    }

    /**
     * Consumes the reply for the oldest in-flight batch, enforcing FIFO
     * req_id order and the u32 WRITE_ACK shape.  A MSG_ERROR frame frees the
     * slot and throws a plain (non-fatal) IOException naming the batch; a
     * transport error or stream desync kills the pipeline via markDead.
     */
    private void readOneAck() throws IOException {
        TsdbClient.Frame f;
        try {
            f = c.recv();
        } catch (IOException e) {
            throw markDead(new IOException("write pipeline: ack recv failed with "
                    + inflight.size() + " batch(es) in flight (delivery unknown): "
                    + e.getMessage(), e));
        }
        Pending want = inflight.pollFirst();
        if (f.reqId != want.reqId) {
            throw markDead(new IOException("write pipeline: ack req_id=" + f.reqId
                    + ", want " + want.reqId + " - stream desynchronised"));
        }
        if (f.type == TsdbClient.MSG_ERROR) {
            // Application-level rejection of that batch only; stream intact.
            IOException srv = TsdbClient.decodeError(f.payload);
            throw new IOException("batch #" + want.index + ": " + srv.getMessage(), srv);
        }
        if (f.type != TsdbClient.MSG_WRITE_ACK) {
            throw markDead(new IOException("write pipeline: unexpected frame type="
                    + f.type + " for req_id=" + want.reqId));
        }
        if (f.payload.length < 4) {
            throw markDead(new IOException("write pipeline: short WRITE_ACK for req_id=" + want.reqId));
        }
    }

    /**
     * Records the fatal error, drops the in-flight bookkeeping, and closes
     * the shared socket so it cannot be reused in a desynchronised state (a
     * later request could misread a stale pipelined ack as its own reply).
     * The parent client's next call then takes its normal reconnect path.
     */
    private IOException markDead(IOException e) {
        dead = e;
        inflight.clear();
        try { c.close(); } catch (IOException ignore) { /* already dead */ }
        return e;
    }
}
