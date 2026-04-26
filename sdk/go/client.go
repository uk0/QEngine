package tsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Client is a high-level wrapper on Conn with session + auth state.
type Client struct {
	Conn  *Conn
	token string // set after Login
}

// Open dials + sends HELLO.
func Open(addr string, timeout time.Duration) (*Client, error) {
	conn, err := Dial(addr, timeout)
	if err != nil {
		return nil, err
	}
	cl := &Client{Conn: conn}
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
func (c *Client) WriteBatch(table string, cols []Column, rows []Row) (uint32, error) {
	if len(rows) == 0 {
		return 0, nil
	}

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
		case TypeTimestamp, TypeInt64, TypeFloat64:
			// Fixed-width: 8 bytes/row, contiguous.
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
				case TypeFloat64:
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
			return 0, fmt.Errorf("unknown column type %d", col.Type)
		}
	}

	if _, err := c.Conn.Send(MsgWriteBatch, 0, 0, buf.Bytes()); err != nil {
		return 0, err
	}
	f, err := c.Conn.Recv()
	if err != nil {
		return 0, err
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

// QueryResult holds rows returned from a SELECT.
type QueryResult struct {
	ColNames []string
	ColTypes []byte
	Rows     [][]interface{} // each row has one entry per column
}

// Query runs a QTL string and returns the full result set.
func (c *Client) Query(qtl string) (*QueryResult, error) {
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
	colLen := nrows * 8
	if len(p) < ncols*colLen {
		return errors.New("short column data")
	}
	// Emit rows interleaving column pulls.
	for ri := 0; ri < nrows; ri++ {
		row := make([]interface{}, ncols)
		for ci := 0; ci < ncols; ci++ {
			off := ci*colLen + ri*8
			raw := binary.LittleEndian.Uint64(p[off : off+8])
			switch qr.ColTypes[ci] {
			case TypeTimestamp:
				row[ci] = int64(raw)
			case TypeInt64:
				row[ci] = int64(raw)
			case TypeFloat64:
				row[ci] = math.Float64frombits(raw)
			case TypeSymbol:
				row[ci] = uint32(raw)
			}
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
