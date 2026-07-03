package tsdb

import (
	"errors"
	"fmt"
)

// maxWritePipelineDepth caps the in-flight window.  Beyond a few dozen frames
// the pipeline is bandwidth-bound, not RTT-bound, and a deeper window only
// grows the post-failure ambiguity (every un-acked batch is in an unknown
// state), so depth is silently clamped here.
const maxWritePipelineDepth = 64

// errPipelineActive is returned by Client.Query/Client.WriteBatch while a
// WritePipeline is open on the client: the pipeline owns the connection and
// interleaving another request would corrupt the FIFO ack accounting.
var errPipelineActive = errors.New("tsdb: write pipeline in progress on this connection")

// errPipelineClosed is returned by Write/Flush after Close.
var errPipelineClosed = errors.New("tsdb: write pipeline is closed")

// WritePipeline keeps up to depth WRITE_BATCH frames in flight on one
// connection, amortising the network round-trip across the window.  The
// synchronous WriteBatch pays one RTT per batch; measured on the live cluster,
// raising the per-connection window from 1 to 4 lifted ingest from 8.2M to
// 19.8M rows/s (~2.4x) — the same lever bench/load_cluster.c exposes as
// TSDB_LOAD_PIPELINE.
//
// Ack matching is FIFO: the server (src/server/server.c) runs one loop per
// connection that reads a frame, fully handles it, sends the WRITE_ACK (or
// ERROR) echoing the request's req_id, and only then reads the next frame —
// so replies arrive strictly in send order.  load_cluster.c relies on the
// same in-order property.  The echoed req_id is still verified against the
// oldest in-flight req_id and any mismatch kills the pipeline.
//
// Error semantics:
//
//   - A server rejection (ERROR frame) is an application-level failure of the
//     OLDEST in-flight batch only.  The frame stream stays intact, the slot is
//     freed, and the pipeline remains usable.  Because acks lag sends, the
//     error surfaces on a LATER Write (or Flush) than the one that submitted
//     the rejected rows.
//   - Any transport error — send or recv — BREAKS the pipeline: the error is
//     sticky, every subsequent Write/Flush returns it immediately, and the
//     underlying connection is closed (with frames in flight the stream
//     position is unknowable, and a later request on the socket could misread
//     a stale pipelined ack as its own reply).  There is deliberately NO
//     auto-retry/reconnect here, unlike WriteBatch's withReconnect layer: a
//     broken pipeline has up to depth non-idempotent batches in post-send
//     ambiguity (each may or may not have been applied), so only the caller
//     can decide what is safe to resend.  The parent client's next call will
//     see a dead socket and follow its normal reconnect policy.
//
// A WritePipeline is NOT safe for concurrent use: drive it from a single
// goroutine.  While a pipeline is open the parent Client must not be used
// either — the connection is shared — and Query/WriteBatch enforce this by
// returning errPipelineActive until Close is called.  Always Close the
// pipeline, even after an error, to release the client.
type WritePipeline struct {
	c        *Client
	depth    int      // max frames in flight
	inflight []uint64 // req_ids of sent-but-unacked frames, oldest first
	dead     error    // sticky transport/protocol error; nil while healthy
	closed   bool
}

// NewWritePipeline opens a pipelined writer on the client's connection with
// the given in-flight window.  depth must be >= 1 and is clamped to
// maxWritePipelineDepth.  Only one pipeline may be open per client at a time;
// see the WritePipeline doc for the (single-goroutine) usage contract.
func (c *Client) NewWritePipeline(depth int) (*WritePipeline, error) {
	if depth < 1 {
		return nil, fmt.Errorf("tsdb: write pipeline depth must be >= 1, got %d", depth)
	}
	if depth > maxWritePipelineDepth {
		depth = maxWritePipelineDepth
	}
	if c.Conn == nil {
		return nil, errors.New("tsdb: write pipeline: client has no live connection")
	}
	if c.pipeline != nil {
		return nil, errPipelineActive
	}
	p := &WritePipeline{c: c, depth: depth, inflight: make([]uint64, 0, depth)}
	c.pipeline = p
	return p, nil
}

// Write encodes rows (same arguments as Client.WriteBatch) and sends one
// WRITE_BATCH frame without waiting for its ack.  When the in-flight window
// is full it first blocks to consume exactly ONE ack — the oldest in-flight
// batch's — so a returned server error refers to that earlier batch, not to
// the rows passed here (which, in that case, were NOT sent; the caller may
// retry the same Write once the error is handled).  A transport error breaks
// the pipeline permanently (see type doc).
func (p *WritePipeline) Write(table string, cols []Column, rows []Row) error {
	if p.closed {
		return errPipelineClosed
	}
	if p.dead != nil {
		return p.dead
	}
	if len(rows) == 0 {
		return nil
	}
	payload, err := encodeBatch(table, cols, rows)
	if err != nil {
		return err // encode error: nothing sent, pipeline unaffected
	}
	if len(payload) > maxPayload {
		// Pre-check so an oversized batch is rejected before any bytes hit the
		// socket; the pipeline stays usable.
		return fmt.Errorf("tsdb: batch payload %d bytes exceeds frame limit %d", len(payload), maxPayload)
	}
	if len(p.inflight) >= p.depth {
		if err := p.readOneAck(); err != nil {
			return err
		}
	}
	reqID, err := p.c.Conn.Send(MsgWriteBatch, 0, 0, payload)
	if err != nil {
		return p.markDead(fmt.Errorf("tsdb: write pipeline: send failed: %w", err))
	}
	p.inflight = append(p.inflight, reqID)
	return nil
}

// Flush blocks until every in-flight batch is acked.  Server rejections do
// not stop the drain: all remaining acks are consumed and the FIRST error
// seen is returned.  A transport error breaks the pipeline and ends the drain
// early (the remaining acks can never arrive).
func (p *WritePipeline) Flush() error {
	if p.closed {
		return errPipelineClosed
	}
	if p.dead != nil {
		return p.dead
	}
	var first error
	for len(p.inflight) > 0 {
		if err := p.readOneAck(); err != nil {
			if first == nil {
				first = err
			}
			if p.dead != nil {
				return first
			}
		}
	}
	return first
}

// Close flushes in-flight batches, marks the pipeline closed, and releases
// the parent client for normal use again.  It returns Flush's error.  Close
// is idempotent; the pipeline must not be used afterwards.
func (p *WritePipeline) Close() error {
	if p.closed {
		return nil
	}
	err := p.Flush()
	p.closed = true
	if p.c.pipeline == p {
		p.c.pipeline = nil
	}
	return err
}

// readOneAck consumes the reply for the oldest in-flight batch, enforcing
// FIFO req_id order.  An ERROR frame frees the slot and is returned as a
// plain (non-fatal) error; a transport error or stream desync kills the
// pipeline via markDead.
func (p *WritePipeline) readOneAck() error {
	f, err := p.c.Conn.Recv()
	if err != nil {
		return p.markDead(fmt.Errorf(
			"tsdb: write pipeline: ack recv failed with %d batch(es) in flight (delivery unknown): %w",
			len(p.inflight), err))
	}
	want := p.inflight[0]
	p.inflight = p.inflight[1:]
	if f.ReqID != want {
		return p.markDead(fmt.Errorf(
			"tsdb: write pipeline: ack req_id=%d, want %d — stream desynchronised", f.ReqID, want))
	}
	switch {
	case f.Type == MsgError:
		// Application-level rejection of that batch only; stream intact.
		return decodeError(f.Payload)
	case f.Type != MsgWriteAck:
		return p.markDead(fmt.Errorf(
			"tsdb: write pipeline: unexpected frame type=%d for req_id=%d", f.Type, want))
	case len(f.Payload) < 4:
		return p.markDead(fmt.Errorf("tsdb: write pipeline: short WRITE_ACK for req_id=%d", want))
	}
	return nil
}

// markDead records the fatal error, drops the in-flight bookkeeping, and
// closes the shared connection so it cannot be reused in a desynchronised
// state (see the transport-error notes on WritePipeline).
func (p *WritePipeline) markDead(err error) error {
	p.dead = err
	p.inflight = nil
	if p.c.Conn != nil {
		_ = p.c.Conn.Close()
	}
	return err
}
