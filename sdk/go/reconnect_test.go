package tsdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- minimal server-side wire codec for the mock listener -------------------
//
// These mirror Conn.Send/Conn.Recv from the *server* side so the test can speak
// just enough of the protocol (HELLO handshake + one query/error reply) to
// drive the client's reconnect path without the real C server.

func srvReadFrame(conn net.Conn) (typ byte, flags uint16, reqID uint64, payload []byte, err error) {
	var hdr [hdrSize]byte
	if _, err = io.ReadFull(conn, hdr[:]); err != nil {
		return
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != protoMagic {
		err = errors.New("srv: bad magic")
		return
	}
	typ = hdr[5]
	flags = binary.LittleEndian.Uint16(hdr[6:8])
	reqID = binary.LittleEndian.Uint64(hdr[12:20])
	plen := binary.LittleEndian.Uint32(hdr[20:24])
	if plen > 0 {
		payload = make([]byte, plen)
		if _, err = io.ReadFull(conn, payload); err != nil {
			return
		}
	}
	var trailer [4]byte
	_, err = io.ReadFull(conn, trailer[:]) // CRC — not validated server-side here
	return
}

func srvWriteFrame(conn net.Conn, typ byte, flags uint16, reqID uint64, payload []byte) error {
	var hdr [hdrSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], protoMagic)
	hdr[4] = protoVer
	hdr[5] = typ
	binary.LittleEndian.PutUint16(hdr[6:8], flags)
	binary.LittleEndian.PutUint64(hdr[12:20], reqID)
	binary.LittleEndian.PutUint32(hdr[20:24], uint32(len(payload)))
	sum := crc32.Update(0, crcTable, hdr[4:24])
	if len(payload) > 0 {
		sum = crc32.Update(sum, crcTable, payload)
	}
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], sum)
	if _, err := conn.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}
	_, err := conn.Write(trailer[:])
	return err
}

// emptyQueryHdr returns a QUERY_RESULT_HDR payload for a zero-column result
// (ncols=0), with FlagFin set so the client treats it as a complete empty
// result and returns without draining row chunks.
func emptyQueryHdr() []byte {
	p := make([]byte, 2)
	binary.LittleEndian.PutUint16(p[0:2], 0) // ncols = 0
	return p
}

// TestReconnectOnDrop is the listener test: a local TCP server completes the
// HELLO handshake, drops the connection, and the client must transparently
// re-dial (a SECOND Accept) and complete a follow-up Query.
func TestReconnectOnDrop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var accepts int32
	secondQueryOK := make(chan struct{})
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		// --- connection #1: handshake, then DROP to simulate a peer restart.
		c1, err := ln.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&accepts, 1)
		// Expect HELLO, reply HELLO_OK.
		if typ, _, _, _, err := srvReadFrame(c1); err != nil || typ != MsgHello {
			t.Errorf("conn1: want HELLO got typ=%d err=%v", typ, err)
			c1.Close()
			return
		}
		if err := srvWriteFrame(c1, MsgHelloOK, 0, 0, nil); err != nil {
			t.Errorf("conn1 HELLO_OK: %v", err)
		}
		// Drop the connection mid-session: the client's next Recv sees EOF.
		c1.Close()

		// --- connection #2: the reconnect. Handshake again, then serve the
		//     retried Query so the client call returns success.
		c2, err := ln.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&accepts, 2)
		if typ, _, _, _, err := srvReadFrame(c2); err != nil || typ != MsgHello {
			t.Errorf("conn2: want HELLO got typ=%d err=%v", typ, err)
			c2.Close()
			return
		}
		if err := srvWriteFrame(c2, MsgHelloOK, 0, 0, nil); err != nil {
			t.Errorf("conn2 HELLO_OK: %v", err)
		}
		// The retried Query lands here.
		typ, _, reqID, _, err := srvReadFrame(c2)
		if err != nil || typ != MsgQuery {
			t.Errorf("conn2: want QUERY got typ=%d err=%v", typ, err)
			c2.Close()
			return
		}
		if err := srvWriteFrame(c2, MsgQueryRsltHdr, FlagFin, reqID, emptyQueryHdr()); err != nil {
			t.Errorf("conn2 QUERY hdr: %v", err)
		}
		close(secondQueryOK)
		c2.Close()
	}()

	cl, err := Open(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cl.Close()

	// This Query is issued on the now-dropped conn #1: the first Recv hits EOF,
	// the client reconnects (Accept #2) and re-runs the query, which succeeds.
	if _, err := cl.Query("SELECT 1"); err != nil {
		t.Fatalf("Query across reconnect: %v", err)
	}

	select {
	case <-secondQueryOK:
	case <-time.After(2 * time.Second):
		t.Fatal("server never served the retried query on the second connection")
	}
	<-serverDone

	if got := atomic.LoadInt32(&accepts); got != 3 { // 1 + 2
		t.Fatalf("expected the client to re-dial (accept marker=3), got %d", got)
	}
}

// TestReconnectDisabled verifies the opt-out: with MaxReconnect<0 a dropped
// connection surfaces the transport error instead of re-dialing.
func TestReconnectDisabled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var accepts int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c1, err := ln.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&accepts, 1)
		_, _, _, _, _ = srvReadFrame(c1) // HELLO
		_ = srvWriteFrame(c1, MsgHelloOK, 0, 0, nil)
		c1.Close() // drop
		// A second Accept would only happen if the client (wrongly) reconnected;
		// give it a short window then stop accepting.
		_ = ln.(*net.TCPListener).SetDeadline(time.Now().Add(300 * time.Millisecond))
		if c2, err := ln.Accept(); err == nil {
			atomic.AddInt32(&accepts, 1)
			c2.Close()
		}
	}()

	cl, err := Open(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cl.MaxReconnect = -1 // opt out
	defer cl.Close()

	if _, err := cl.Query("SELECT 1"); err == nil {
		t.Fatal("expected Query to fail (reconnect disabled), got nil")
	}
	wg.Wait()
	if got := atomic.LoadInt32(&accepts); got != 1 {
		t.Fatalf("reconnect was disabled but client re-dialed: accepts=%d", got)
	}
}

// writeAck returns a WRITE_ACK payload reporting n accepted rows.
func writeAck(n uint32) []byte {
	p := make([]byte, 4)
	binary.LittleEndian.PutUint32(p, n)
	return p
}

// oneRow is a trivial single-row batch for write tests.
var (
	wbCols = []Column{{"ts", TypeTimestamp}, {"v", TypeInt64}}
	wbRows = []Row{{TS: 1, I64: map[int]int64{1: 42}}}
)

// TestWriteBatchRetriesOnSendPhaseDrop proves the idempotency-safe case: when
// the connection is already dead at send time, the client reconnects and
// re-sends the batch exactly once (the frame never reached the old server), and
// the call succeeds against the new connection.
func TestWriteBatchRetriesOnSendPhaseDrop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	writesServed := make(chan struct{}, 2)
	go func() {
		// conn #1: handshake, then RST the socket (SetLinger(0)) so the client's
		// next Send WRITE fails with "broken pipe" — a send-phase failure where
		// the server provably never processed the batch (it discarded the socket
		// on RST), so an auto-resend cannot duplicate rows.
		c1, err := ln.Accept()
		if err != nil {
			return
		}
		_, _, _, _, _ = srvReadFrame(c1) // HELLO
		_ = srvWriteFrame(c1, MsgHelloOK, 0, 0, nil)
		if tc, ok := c1.(*net.TCPConn); ok {
			_ = tc.SetLinger(0) // force RST instead of graceful FIN
		}
		c1.Close()

		// conn #2: the reconnect. Handshake, then serve the re-sent batch.
		c2, err := ln.Accept()
		if err != nil {
			return
		}
		_, _, _, _, _ = srvReadFrame(c2) // HELLO
		_ = srvWriteFrame(c2, MsgHelloOK, 0, 0, nil)
		typ, _, reqID, _, err := srvReadFrame(c2)
		if err == nil && typ == MsgWriteBatch {
			_ = srvWriteFrame(c2, MsgWriteAck, 0, reqID, writeAck(1))
			writesServed <- struct{}{}
		}
		c2.Close()
	}()

	cl, err := Open(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cl.Close()

	// Give the kernel time to process the RST so the next Send write fails.
	time.Sleep(150 * time.Millisecond)
	// A larger batch makes the failing write deterministic (the payload alone
	// exceeds a single buffered write on a reset socket).
	bigRows := make([]Row, 4096)
	for i := range bigRows {
		bigRows[i] = Row{TS: int64(i), I64: map[int]int64{1: int64(i)}}
	}
	acked, err := cl.WriteBatch("t", wbCols, bigRows)
	if err != nil {
		t.Fatalf("WriteBatch should auto-retry a send-phase drop: %v", err)
	}
	if acked != 1 {
		t.Fatalf("acked=%d want 1", acked)
	}
	select {
	case <-writesServed:
	case <-time.After(2 * time.Second):
		t.Fatal("re-sent batch never reached the second connection")
	}
}

// TestWriteBatchNoRetryAfterSend proves the idempotency-UNSAFE case: when the
// drop happens AFTER the batch was sent (failure on the ack read), the client
// reconnects but must NOT auto-resend (resending could duplicate rows). The
// error is surfaced and the second connection sees no WRITE_BATCH.
func TestWriteBatchNoRetryAfterSend(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var sawWriteOnConn2 int32
	go func() {
		// conn #1: handshake, READ the write batch (frame fully sent), then drop
		// WITHOUT acking — the client's Recv for the ack sees EOF (post-send).
		c1, err := ln.Accept()
		if err != nil {
			return
		}
		_, _, _, _, _ = srvReadFrame(c1) // HELLO
		_ = srvWriteFrame(c1, MsgHelloOK, 0, 0, nil)
		if typ, _, _, _, err := srvReadFrame(c1); err != nil || typ != MsgWriteBatch {
			t.Errorf("conn1: want WRITE_BATCH got typ=%d err=%v", typ, err)
		}
		c1.Close() // drop after consuming the batch, before acking

		// conn #2: the reconnect happens (live socket for future calls), but no
		// WRITE_BATCH must be auto-resent. Flag it if one arrives.
		c2, err := ln.Accept()
		if err != nil {
			return
		}
		_, _, _, _, _ = srvReadFrame(c2) // HELLO
		_ = srvWriteFrame(c2, MsgHelloOK, 0, 0, nil)
		_ = c2.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
		if typ, _, _, _, err := srvReadFrame(c2); err == nil && typ == MsgWriteBatch {
			atomic.AddInt32(&sawWriteOnConn2, 1)
		}
		c2.Close()
	}()

	cl, err := Open(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cl.Close()

	_, err = cl.WriteBatch("t", wbCols, wbRows)
	if err == nil {
		t.Fatal("WriteBatch must return an error on a post-send drop (no silent auto-retry)")
	}
	// The client should have reconnected: a follow-up Query on the fresh socket
	// would work, but we only assert the batch was NOT re-sent.
	time.Sleep(500 * time.Millisecond)
	if atomic.LoadInt32(&sawWriteOnConn2) != 0 {
		t.Fatal("WriteBatch was auto-resent after a post-send drop — risks duplicate rows")
	}
}

// srvErrPayload builds a MSG_ERROR payload in the spec shape
// [i32 rc][u16 msg_len][msg].
func srvErrPayload(rc int32, msg string) []byte {
	p := make([]byte, 6+len(msg))
	binary.LittleEndian.PutUint32(p[0:4], uint32(rc))
	binary.LittleEndian.PutUint16(p[4:6], uint16(len(msg)))
	copy(p[6:], msg)
	return p
}

// TestDropTable drives the DROP_TABLE round-trip against the mock listener:
// the server must see MSG_DROP_TABLE carrying the raw table name as payload,
// a generic-OK reply means success, and an MSG_ERROR reply surfaces as a
// decoded error.
func TestDropTable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		c1, err := ln.Accept()
		if err != nil {
			return
		}
		defer c1.Close()
		_, _, _, _, _ = srvReadFrame(c1) // HELLO
		_ = srvWriteFrame(c1, MsgHelloOK, 0, 0, nil)

		// Drop #1: succeed.  The payload must be the bare table name.
		typ, _, reqID, payload, err := srvReadFrame(c1)
		if err != nil || typ != MsgDropTable {
			t.Errorf("want DROP_TABLE got typ=%d err=%v", typ, err)
			return
		}
		if string(payload) != "trades" {
			t.Errorf("DROP_TABLE payload = %q, want raw name %q", payload, "trades")
		}
		_ = srvWriteFrame(c1, MsgHelloOK, FlagFin, reqID, nil) // generic OK

		// Drop #2: fail with a server error frame.
		typ, _, reqID, _, err = srvReadFrame(c1)
		if err != nil || typ != MsgDropTable {
			t.Errorf("want 2nd DROP_TABLE got typ=%d err=%v", typ, err)
			return
		}
		_ = srvWriteFrame(c1, MsgError, FlagFin, reqID, srvErrPayload(-3, "no such table"))
	}()

	cl, err := Open(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cl.Close()

	if err := cl.DropTable("trades"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	err = cl.DropTable("missing")
	if err == nil {
		t.Fatal("DropTable on MSG_ERROR reply must return an error")
	}
	if !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("error should carry the server message, got: %v", err)
	}
}

// TestIsConnError table-tests the classifier that decides reconnect-worthy
// (transport failure) vs. not (server application error / nil).
func TestIsConnError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"eof", io.EOF, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"wrapped eof", fmt.Errorf("recv: %w", io.EOF), true},
		{"broken pipe text", errors.New("write tcp: broken pipe"), true},
		{"conn reset text", errors.New("read tcp 1.2.3.4:5: connection reset by peer"), true},
		{"conn refused text", errors.New("dial tcp: connection refused"), true},
		{"closed network conn", errors.New("use of closed network connection"), true},
		{"net.OpError", &net.OpError{Op: "read", Err: errors.New("reset")}, true},
		// Server application error (decoded MsgError frame) — must NOT reconnect.
		{"server app error", fmt.Errorf("server error rc=%d: %s", 7, "no such table"), false},
		{"crc mismatch (frame-level, not transport)", errors.New("crc mismatch"), false},
		{"generic", errors.New("some other failure"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isConnError(tc.err); got != tc.want {
				t.Fatalf("isConnError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
