package tsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Client is a high-level wrapper on Conn with session + auth state.
//
// The client retains everything needed to transparently re-establish the
// session after a mid-session connection drop (peer restart, network blip):
// the server address, the dial timeout, and — once Login is called — the
// credentials.  On a transport error during Query/WriteBatch the client
// re-dials and re-handshakes automatically; see MaxReconnect.
type Client struct {
	Conn  *Conn
	token string // set after Login

	// Retained reconnect state.
	addr     string        // server address passed to Open
	timeout  time.Duration // dial timeout passed to Open
	user     string        // login username (set by Login)
	pass     string        // login password (set by Login)
	loggedIn bool          // true once Login succeeded; re-Login on reconnect

	// MaxReconnect bounds automatic reconnect attempts on a dropped
	// connection.  Zero (the default) means auto-reconnect is ON with 2
	// attempts.  A negative value disables auto-reconnect entirely.
	MaxReconnect int

	// pipeline is non-nil while a WritePipeline is open on this client.  The
	// pipeline owns the connection until Close: Query/WriteBatch check this
	// and refuse to interleave frames with pipelined writes.
	pipeline *WritePipeline
}

// Open dials + sends HELLO.
func Open(addr string, timeout time.Duration) (*Client, error) {
	conn, err := Dial(addr, timeout)
	if err != nil {
		return nil, err
	}
	cl := &Client{Conn: conn, addr: addr, timeout: timeout}
	if _, err := conn.Send(MsgHello, 0, 0, nil); err != nil {
		conn.Close()
		return nil, err
	}
	f, err := conn.Recv()
	if err != nil {
		conn.Close()
		return nil, err
	}
	if f.Type != MsgHelloOK {
		conn.Close()
		return nil, fmt.Errorf("unexpected HELLO response type=%d", f.Type)
	}
	return cl, nil
}

// Close tears down the connection.
func (c *Client) Close() error { return c.Conn.Close() }

// Login performs AUTH_LOGIN.  On success the token is attached to the
// connection server-side; no further work is needed from the client.
func (c *Client) Login(user, pass string) error {
	// Retain credentials so a reconnect can re-authenticate the fresh socket.
	c.user, c.pass = user, pass
	var buf bytes.Buffer
	buf.WriteByte(byte(len(user)))
	buf.WriteString(user)
	buf.WriteByte(byte(len(pass)))
	buf.WriteString(pass)
	if _, err := c.Conn.Send(MsgAuthLogin, 0, 0, buf.Bytes()); err != nil {
		return err
	}
	f, err := c.Conn.Recv()
	if err != nil {
		return err
	}
	if f.Type == MsgError {
		return decodeError(f.Payload)
	}
	if f.Type != MsgAuthOK {
		return fmt.Errorf("unexpected AUTH response type=%d", f.Type)
	}
	if len(f.Payload) != 32 {
		return fmt.Errorf("bad token length: %d", len(f.Payload))
	}
	c.token = string(f.Payload)
	c.loggedIn = true
	return nil
}

// Token returns the session token (empty if Login not called).
func (c *Client) Token() string { return c.token }

// Column describes a column for CreateTable.
type Column struct {
	Name string
	Type byte // Type{Timestamp,Int64,Float64,Symbol}
}

// CreateTable runs TSDB_MT_CREATE_TABLE.  Idempotent on the server.
func (c *Client) CreateTable(table, tsCol string, cols []Column) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(len(table)))
	buf.WriteString(table)
	buf.WriteByte(byte(len(tsCol)))
	buf.WriteString(tsCol)
	buf.WriteByte(byte(len(cols)))
	for _, c := range cols {
		buf.WriteByte(byte(len(c.Name)))
		buf.WriteString(c.Name)
		buf.WriteByte(c.Type)
	}
	if _, err := c.Conn.Send(MsgCreateTable, 0, 0, buf.Bytes()); err != nil {
		return err
	}
	f, err := c.Conn.Recv()
	if err != nil {
		return err
	}
	if f.Type == MsgError {
		return decodeError(f.Payload)
	}
	return nil
}

// Row is a single row to insert.  Col indexes correspond to the order in
// CreateTable (ts is always col 0 if you used the default shape).
type Row struct {
	TS  int64
	I64 map[int]int64
	F64 map[int]float64
	Sym map[int]string
}

// WriteBatch sends a columnar WRITE_BATCH of rows.  All rows must share
// the same set of columns.  cols describes ALL columns, in schema order
// (matching the server's CreateTable).
// encodeBatch builds the WRITE_BATCH payload (column-major wire format) for a
// set of rows.  Shared by the synchronous WriteBatch and the pipelined writer.
func encodeBatch(table string, cols []Column, rows []Row) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(byte(len(table)))
	buf.WriteString(table)

	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[0:2], uint16(len(cols)))
	buf.Write(tmp[0:2])
	binary.LittleEndian.PutUint32(tmp[0:4], uint32(len(rows)))
	buf.Write(tmp[0:4])

	for ci, col := range cols {
		buf.WriteByte(byte(len(col.Name)))
		buf.WriteString(col.Name)
		buf.WriteByte(col.Type)
		buf.WriteByte(0) // codec = RAW

		switch col.Type {
		case TypeTimestamp, TypeInt64, TypeFloat64, TypeFloat32:
			// Fixed-width: 8 bytes/row, contiguous.  FLOAT32 travels as a
			// double too — the server stores it 8-byte in memory and the codec
			// narrows to 4 bytes only on disk — so write it via Row.F64.
			binary.LittleEndian.PutUint32(tmp[0:4], uint32(len(rows)*8))
			buf.Write(tmp[0:4])
			for _, r := range rows {
				switch col.Type {
				case TypeTimestamp:
					binary.LittleEndian.PutUint64(tmp[0:8], uint64(r.TS))
				case TypeInt64:
					var v int64
					if r.I64 != nil {
						v = r.I64[ci]
					}
					binary.LittleEndian.PutUint64(tmp[0:8], uint64(v))
				case TypeFloat64, TypeFloat32:
					var v float64
					if r.F64 != nil {
						v = r.F64[ci]
					}
					binary.LittleEndian.PutUint64(tmp[0:8], math.Float64bits(v))
				}
				buf.Write(tmp[0:8])
			}

		case TypeSymbol:
			// SYMBOL wire format mirrors the cluster RPC WRITE_BATCH:
			//   [u32 total_bytes] [u16 len_0] [bytes_0] … [u16 len_n-1] [bytes_n-1]
			// We pre-encode each row's value into a scratch buffer, then
			// prepend [u32 total] + [u32 col_size_for_NEED_check] in one go.
			var sym bytes.Buffer
			for _, r := range rows {
				var v string
				if r.Sym != nil {
					v = r.Sym[ci]
				}
				if len(v) > 65535 {
					v = v[:65535]
				}
				binary.LittleEndian.PutUint16(tmp[0:2], uint16(len(v)))
				sym.Write(tmp[0:2])
				sym.WriteString(v)
			}
			total := uint32(sym.Len())
			binary.LittleEndian.PutUint32(tmp[0:4], 4+total) // col_size header
			buf.Write(tmp[0:4])
			binary.LittleEndian.PutUint32(tmp[0:4], total) // [u32 total]
			buf.Write(tmp[0:4])
			buf.Write(sym.Bytes())

		default:
			return nil, fmt.Errorf("unknown column type %d", col.Type)
		}
	}
	return buf.Bytes(), nil
}

// WriteBatch synchronously writes rows and waits for the server's ack,
// returning the accepted row count.  One round-trip per call: throughput over
// a high-latency link is bounded by 1/RTT.  For bulk loads over a network use
// NewWritePipeline (bounded FIFO window, per-batch error reporting) or
// NewPipelineWriter (background ack reader), which keep many batches in flight.
func (c *Client) WriteBatch(table string, cols []Column, rows []Row) (uint32, error) {
	if c.pipeline != nil {
		return 0, errPipelineActive
	}
	if len(rows) == 0 {
		return 0, nil
	}
	payload, err := encodeBatch(table, cols, rows)
	if err != nil {
		return 0, err
	}

	// WriteBatch idempotency: a WRITE_BATCH is NOT idempotent — the server
	// appends rows, so a blind resend could double-write.  We split the
	// round-trip into a send phase and a receive phase and auto-retry ONLY the
	// send phase:
	//   - A Send/dial failure means the request frame never landed (or landed
	//     incomplete and is rejected on CRC), so re-sending after reconnect is
	//     safe — we reconnect and resend once.
	//   - A failure on Recv() happens AFTER the frame was fully written; the
	//     server may have already applied the batch, so resending could
	//     duplicate rows.  We reconnect (so the next call uses a live socket)
	//     but do NOT auto-retry; the underlying error is returned for the
	//     caller to decide whether to resend.
	acked, err := c.writeBatchOnce(payload)
	if err == nil || !isConnError(err) || c.maxReconnect() <= 0 {
		return acked, err
	}
	sentBytes := errors.Is(err, errAfterSend)
	if rcErr := c.reconnect(); rcErr != nil {
		return 0, err // surface the original transport error
	}
	if sentBytes {
		// Post-send drop: reconnected, but resending may duplicate. Return.
		return 0, errors.Unwrap(err)
	}
	return c.writeBatchOnce(payload)
}

// errAfterSend tags a transport error that occurred while awaiting the ack
// (after the request frame was fully written), distinguishing it from a
// send/dial failure where nothing reached the server.
var errAfterSend = errors.New("connection dropped after request was sent")

// writeBatchOnce performs a single WRITE_BATCH round-trip.  A Recv-phase
// transport error is wrapped to satisfy errors.Is(err, errAfterSend) while
// still unwrapping (via the wrapped chain) to the original conn error so
// isConnError matches.
func (c *Client) writeBatchOnce(payload []byte) (uint32, error) {
	if _, err := c.Conn.Send(MsgWriteBatch, 0, 0, payload); err != nil {
		return 0, err // send/dial failure: nothing landed, safe to retry
	}
	f, err := c.Conn.Recv()
	if err != nil {
		return 0, afterSendError{err}
	}
	if f.Type == MsgError {
		return 0, decodeError(f.Payload)
	}
	if f.Type != MsgWriteAck {
		return 0, fmt.Errorf("unexpected WRITE response type=%d", f.Type)
	}
	if len(f.Payload) < 4 {
		return 0, errors.New("short WRITE_ACK")
	}
	return binary.LittleEndian.Uint32(f.Payload[:4]), nil
}

// afterSendError wraps a post-send transport error so that it matches both
// errors.Is(err, errAfterSend) (retry-suppression) and isConnError (reconnect).
type afterSendError struct{ err error }

func (e afterSendError) Error() string        { return e.err.Error() }
func (e afterSendError) Unwrap() error        { return e.err }
func (e afterSendError) Is(target error) bool { return target == errAfterSend }

// PipelineWriter streams WRITE_BATCH frames without blocking on each ack, up to
// a bounded in-flight window, so bulk-load throughput is governed by bandwidth
// and server ingest rate rather than round-trip latency.  A background
// goroutine reads acks; the first error seen is surfaced by Write or Close.
//
// Use from a SINGLE goroutine: Write serialises frame sends on the connection,
// and concurrent callers would interleave (and corrupt) frames.  The same Conn
// must not be used for other requests while the writer is open.
type PipelineWriter struct {
	c    *Client
	sem  chan struct{} // cap = window; acquired before each send, released per ack
	work chan struct{} // one token per successfully-sent batch awaiting an ack
	done chan struct{} // closed when the ack reader exits
	mu   sync.Mutex
	err  error
}

// NewPipelineWriter starts a pipelined writer on the client's connection.
// window is the maximum number of un-acked batches in flight (<=0 → 64).
func (c *Client) NewPipelineWriter(window int) *PipelineWriter {
	if window <= 0 {
		window = 64
	}
	pw := &PipelineWriter{
		c:    c,
		sem:  make(chan struct{}, window),
		work: make(chan struct{}, window),
		done: make(chan struct{}),
	}
	go pw.ackReader()
	return pw
}

func (pw *PipelineWriter) ackReader() {
	defer close(pw.done)
	for range pw.work {
		f, err := pw.c.Conn.Recv()
		<-pw.sem // free an in-flight slot regardless of outcome
		if err != nil {
			pw.setErr(err)
			continue
		}
		if f.Type == MsgError {
			pw.setErr(decodeError(f.Payload))
			continue
		}
		if f.Type != MsgWriteAck {
			pw.setErr(fmt.Errorf("unexpected WRITE response type=%d", f.Type))
		}
	}
}

func (pw *PipelineWriter) setErr(err error) {
	pw.mu.Lock()
	if pw.err == nil {
		pw.err = err
	}
	pw.mu.Unlock()
}

// Err returns the first error seen by the writer (send failure or ack error),
// or nil. Safe to call concurrently with the background reader.
func (pw *PipelineWriter) Err() error {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	return pw.err
}

// Write encodes and sends one batch.  It blocks only when the in-flight window
// is full (backpressure), not for the batch's own ack.  A prior error (from any
// earlier batch's ack) is returned without sending.
func (pw *PipelineWriter) Write(table string, cols []Column, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	if err := pw.Err(); err != nil {
		return err
	}
	payload, err := encodeBatch(table, cols, rows)
	if err != nil {
		return err
	}
	pw.sem <- struct{}{} // acquire an in-flight slot (blocks if window full)
	if err := pw.Err(); err != nil {
		<-pw.sem
		return err
	}
	if _, err := pw.c.Conn.Send(MsgWriteBatch, 0, 0, payload); err != nil {
		<-pw.sem
		pw.setErr(err)
		return err
	}
	pw.work <- struct{}{} // tell the reader one ack is expected (never blocks: gated by sem)
	return nil
}

// Close flushes all in-flight batches, waits for their acks, and returns the
// first error seen.  The PipelineWriter must not be used after Close.
func (pw *PipelineWriter) Close() error {
	close(pw.work)
	<-pw.done
	return pw.Err()
}

// QueryResult holds rows returned from a SELECT.
type QueryResult struct {
	ColNames []string
	ColTypes []byte
	Rows     [][]interface{} // each row has one entry per column
}

// Query runs a QTL string and returns the full result set.
//
// Query is read-only and therefore safe to auto-retry: on a mid-flight
// transport drop the client reconnects and re-runs the whole query once (the
// result set is rebuilt from scratch, so a partially-drained prior attempt
// cannot corrupt it).  Auto-retry is governed by MaxReconnect.
func (c *Client) Query(qtl string) (*QueryResult, error) {
	if c.pipeline != nil {
		return nil, errPipelineActive
	}
	var qr *QueryResult
	err := c.withReconnect(true, func() error {
		var qerr error
		qr, qerr = c.queryOnce(qtl)
		return qerr
	})
	return qr, err
}

// queryOnce performs a single QUERY round-trip and full result drain.
func (c *Client) queryOnce(qtl string) (*QueryResult, error) {
	if _, err := c.Conn.Send(MsgQuery, 0, 0, []byte(qtl)); err != nil {
		return nil, err
	}
	hdrF, err := c.Conn.Recv()
	if err != nil {
		return nil, err
	}
	if hdrF.Type == MsgError {
		return nil, decodeError(hdrF.Payload)
	}
	if hdrF.Type != MsgQueryRsltHdr {
		return nil, fmt.Errorf("unexpected HDR type=%d", hdrF.Type)
	}

	// Parse header: [ncols u16] then for each [name_len u8][name][type u8]
	p := hdrF.Payload
	if len(p) < 2 {
		return nil, errors.New("short HDR")
	}
	ncols := int(binary.LittleEndian.Uint16(p[0:2]))
	p = p[2:]
	qr := &QueryResult{
		ColNames: make([]string, ncols),
		ColTypes: make([]byte, ncols),
	}
	for i := 0; i < ncols; i++ {
		if len(p) < 1 {
			return nil, errors.New("HDR name truncated")
		}
		nl := int(p[0])
		p = p[1:]
		if len(p) < nl+1 {
			return nil, errors.New("HDR name truncated")
		}
		qr.ColNames[i] = string(p[:nl])
		p = p[nl:]
		qr.ColTypes[i] = p[0]
		p = p[1:]
	}

	// If FIN already set on HDR, no rows to read.
	if hdrF.Flags&FlagFin != 0 {
		return qr, nil
	}

	// Drain rows chunks until FIN.
	for {
		rf, err := c.Conn.Recv()
		if err != nil {
			return nil, err
		}
		if rf.Type == MsgError {
			return nil, decodeError(rf.Payload)
		}
		if rf.Type != MsgQueryRsltRows {
			return nil, fmt.Errorf("unexpected ROWS type=%d", rf.Type)
		}
		if err := parseRowsChunk(qr, rf.Payload); err != nil {
			return nil, err
		}
		if rf.Flags&FlagFin != 0 {
			return qr, nil
		}
	}
}

// parseRowsChunk decodes a QUERY_RESULT_ROWS payload.
// Wire format (server.c handle_query build-chunk):
//
//	[nrows u32][ncols u16] + for each col: [nrows * 8 bytes of raw data]
func parseRowsChunk(qr *QueryResult, p []byte) error {
	if len(p) < 6 {
		return errors.New("short rows chunk")
	}
	nrows := int(binary.LittleEndian.Uint32(p[0:4]))
	ncols := int(binary.LittleEndian.Uint16(p[4:6]))
	p = p[6:]
	if ncols != len(qr.ColNames) {
		return fmt.Errorf("column count mismatch hdr=%d chunk=%d",
			len(qr.ColNames), ncols)
	}
	// Columns are laid out sequentially.  Fixed columns occupy nrows*8
	// bytes (8-byte stride); SYMBOL columns carry a [u32 blocklen] header
	// followed by nrows × [u16 len][bytes] records.  Decode per column,
	// then transpose into rows.
	colVals := make([][]interface{}, ncols)
	for ci := 0; ci < ncols; ci++ {
		colVals[ci] = make([]interface{}, nrows)
		if qr.ColTypes[ci] == TypeSymbol {
			if len(p) < 4 {
				return errors.New("short symbol block header")
			}
			blockLen := int(binary.LittleEndian.Uint32(p[0:4]))
			p = p[4:]
			if len(p) < blockLen {
				return errors.New("short symbol block data")
			}
			sp := p[:blockLen]
			for ri := 0; ri < nrows; ri++ {
				if len(sp) < 2 {
					return errors.New("truncated symbol len")
				}
				l := int(binary.LittleEndian.Uint16(sp[0:2]))
				sp = sp[2:]
				if len(sp) < l {
					return errors.New("truncated symbol bytes")
				}
				colVals[ci][ri] = string(sp[:l])
				sp = sp[l:]
			}
			p = p[blockLen:]
		} else {
			need := nrows * 8
			if len(p) < need {
				return errors.New("short column data")
			}
			for ri := 0; ri < nrows; ri++ {
				raw := binary.LittleEndian.Uint64(p[ri*8 : ri*8+8])
				switch qr.ColTypes[ci] {
				case TypeTimestamp, TypeInt64:
					colVals[ci][ri] = int64(raw)
				case TypeFloat64, TypeFloat32:
					// FLOAT32 travels as an 8-byte double on the wire (the
					// server widens it); decode it identically so aggregates
					// like min/max over a FLOAT32 column don't come back nil.
					colVals[ci][ri] = math.Float64frombits(raw)
				}
			}
			p = p[need:]
		}
	}
	for ri := 0; ri < nrows; ri++ {
		row := make([]interface{}, ncols)
		for ci := 0; ci < ncols; ci++ {
			row[ci] = colVals[ci][ri]
		}
		qr.Rows = append(qr.Rows, row)
	}
	return nil
}

func decodeError(p []byte) error {
	if len(p) < 4 {
		return errors.New("server error")
	}
	rc := int32(binary.LittleEndian.Uint32(p[0:4]))
	msg := ""
	if len(p) > 4 {
		msg = string(p[4:])
	}
	return fmt.Errorf("server error rc=%d: %s", rc, msg)
}
