package tsdb

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// pipeServe starts a mock listener that accepts ONE connection, completes the
// HELLO handshake (reusing the server-side codec from reconnect_test.go), and
// hands the connection to handler.  done closes when handler returns and the
// connection is torn down.
func pipeServe(t *testing.T, handler func(c net.Conn)) (addr string, done chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done = make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if typ, _, _, _, err := srvReadFrame(c); err != nil || typ != MsgHello {
			t.Errorf("mock server: want HELLO, got typ=%d err=%v", typ, err)
			return
		}
		if err := srvWriteFrame(c, MsgHelloOK, 0, 0, nil); err != nil {
			t.Errorf("mock server: HELLO_OK: %v", err)
			return
		}
		handler(c)
	}()
	return ln.Addr().String(), done
}

// latencyAckLoop reads WRITE_BATCH frames and acks each IN ORDER, delayed
// until the frame's arrival time plus d.  This models link latency (delays on
// outstanding frames overlap, as on a real network) rather than serial server
// processing time, which is exactly the cost pipelining is meant to hide.
// Returns when the client closes the connection.
func latencyAckLoop(c net.Conn, d time.Duration, frames *int32) {
	type pending struct {
		reqID uint64
		due   time.Time
	}
	q := make(chan pending, maxWritePipelineDepth)
	ackerDone := make(chan struct{})
	go func() {
		defer close(ackerDone)
		for p := range q {
			time.Sleep(time.Until(p.due))
			if err := srvWriteFrame(c, MsgWriteAck, 0, p.reqID, writeAck(1)); err != nil {
				return
			}
		}
	}()
	for {
		typ, _, reqID, _, err := srvReadFrame(c)
		if err != nil || typ != MsgWriteBatch {
			break
		}
		atomic.AddInt32(frames, 1)
		q <- pending{reqID: reqID, due: time.Now().Add(d)}
	}
	close(q)
	<-ackerDone
}

// TestWritePipelineFIFOAcks is the correctness test: depth 4, 12 batches +
// Flush against a mock server that acks each frame in order after a small
// delay.  All 12 must be acked with no error and the server must have seen
// all 12 frames.  Also asserts the client guard: Query/WriteBatch are
// rejected while the pipeline is open.
func TestWritePipelineFIFOAcks(t *testing.T) {
	var frames int32
	addr, done := pipeServe(t, func(c net.Conn) {
		latencyAckLoop(c, 3*time.Millisecond, &frames)
	})

	cl, err := Open(addr, time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cl.Close()

	p, err := cl.NewWritePipeline(4)
	if err != nil {
		t.Fatalf("NewWritePipeline: %v", err)
	}

	// Guard: the parent client must refuse to interleave on the shared conn.
	if _, err := cl.Query("SELECT 1"); err != errPipelineActive {
		t.Fatalf("Query during pipeline: err=%v, want errPipelineActive", err)
	}
	if _, err := cl.WriteBatch("t", wbCols, wbRows); err != errPipelineActive {
		t.Fatalf("WriteBatch during pipeline: err=%v, want errPipelineActive", err)
	}
	if _, err := cl.NewWritePipeline(4); err != errPipelineActive {
		t.Fatalf("second NewWritePipeline: err=%v, want errPipelineActive", err)
	}

	for i := 0; i < 12; i++ {
		if err := p.Write("t", wbCols, wbRows); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if cl.pipeline != nil {
		t.Fatal("Close must release the client guard")
	}

	cl.Close() // unblocks the mock server's read loop
	<-done
	if got := atomic.LoadInt32(&frames); got != 12 {
		t.Fatalf("server saw %d WRITE_BATCH frames, want 12", got)
	}
}

// TestWritePipelineOverlapsAcks proves the pipeline actually overlaps
// round-trips: against a server with per-frame ack latency D, 12 synchronous
// WriteBatch calls cost >= 12*D of wall time, while a depth-4 pipeline must
// come in materially under (< 60% of the sync time; the ideal is ~25%).
func TestWritePipelineOverlapsAcks(t *testing.T) {
	const (
		d = 5 * time.Millisecond
		n = 12
	)

	run := func(name string, driver func(cl *Client) error) time.Duration {
		var frames int32
		addr, done := pipeServe(t, func(c net.Conn) {
			latencyAckLoop(c, d, &frames)
		})
		cl, err := Open(addr, time.Second)
		if err != nil {
			t.Fatalf("%s: Open: %v", name, err)
		}
		start := time.Now()
		if err := driver(cl); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		elapsed := time.Since(start)
		cl.Close()
		<-done
		if got := atomic.LoadInt32(&frames); got != n {
			t.Fatalf("%s: server saw %d frames, want %d", name, got, n)
		}
		return elapsed
	}

	syncT := run("sync", func(cl *Client) error {
		for i := 0; i < n; i++ {
			if _, err := cl.WriteBatch("t", wbCols, wbRows); err != nil {
				return err
			}
		}
		return nil
	})
	pipeT := run("pipelined", func(cl *Client) error {
		p, err := cl.NewWritePipeline(4)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := p.Write("t", wbCols, wbRows); err != nil {
				return err
			}
		}
		return p.Close() // Flush included in the timed window
	})

	t.Logf("sync=%v pipelined=%v ratio=%.2f", syncT, pipeT, float64(pipeT)/float64(syncT))
	if syncT < n*d {
		t.Fatalf("sync lower bound violated: %v < %v — latency model broken", syncT, n*d)
	}
	if pipeT >= syncT*6/10 {
		t.Fatalf("pipelining did not overlap acks: pipelined=%v, sync=%v (want < 60%%)", pipeT, syncT)
	}
}

// TestWritePipelineBreaksOnTransportError: the server acks k frames then
// hard-closes the connection mid-pipeline.  A Write must surface a transport
// error, and the pipeline must stay dead: subsequent Write returns the same
// sticky error immediately (no I/O), and Flush/Close fail too.
func TestWritePipelineBreaksOnTransportError(t *testing.T) {
	const acksBeforeClose = 3
	addr, done := pipeServe(t, func(c net.Conn) {
		for i := 0; i < acksBeforeClose; i++ {
			typ, _, reqID, _, err := srvReadFrame(c)
			if err != nil || typ != MsgWriteBatch {
				t.Errorf("mock server frame %d: typ=%d err=%v", i, typ, err)
				return
			}
			if err := srvWriteFrame(c, MsgWriteAck, 0, reqID, writeAck(1)); err != nil {
				return
			}
		}
		// Return: the deferred Close drops the conn with un-acked frames in
		// flight — the client's next ack read (or send) hits a transport error.
	})

	cl, err := Open(addr, time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cl.Close()

	p, err := cl.NewWritePipeline(2)
	if err != nil {
		t.Fatalf("NewWritePipeline: %v", err)
	}
	defer p.Close()

	var firstErr error
	for i := 0; i < 12; i++ {
		if err := p.Write("t", wbCols, wbRows); err != nil {
			firstErr = err
			break
		}
	}
	if firstErr == nil {
		t.Fatal("expected a transport error mid-pipeline, all 12 writes succeeded")
	}

	// The pipeline is dead: the same sticky error, returned without touching
	// the (closed) socket.
	start := time.Now()
	if err := p.Write("t", wbCols, wbRows); err != firstErr {
		t.Fatalf("Write on broken pipeline: err=%v, want the sticky error %v", err, firstErr)
	}
	if since := time.Since(start); since > 50*time.Millisecond {
		t.Fatalf("Write on broken pipeline took %v, must fail immediately", since)
	}
	if err := p.Flush(); err != firstErr {
		t.Fatalf("Flush on broken pipeline: err=%v, want the sticky error %v", err, firstErr)
	}
	if err := p.Close(); err != firstErr {
		t.Fatalf("Close on broken pipeline: err=%v, want the sticky error %v", err, firstErr)
	}
	if err := p.Write("t", wbCols, wbRows); err != errPipelineClosed {
		t.Fatalf("Write after Close: err=%v, want errPipelineClosed", err)
	}
	<-done
}
