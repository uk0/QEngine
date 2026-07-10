// Package tsdb is a pure-Go client for the QEngine (tsdb) TCP wire protocol v1.
//
// Frame layout (all little-endian):
//
//	MAGIC       u32 = 0x42445354 ('TSDB' LE)
//	VER         u8  = 1
//	TYPE        u8
//	FLAGS       u16
//	RESERVED    u32 (zero on send)
//	REQ_ID      u64
//	PAYLOAD_LEN u32
//	PAYLOAD     [PAYLOAD_LEN]
//	CRC32C      u32  (Castagnoli, over [ver..end-of-payload])
//
// Authentication:  when the server is configured require_auth=true,
// clients must send AUTH_LOGIN before QUERY/WRITE_BATCH/etc.  This
// package exposes Login(user, pass) which handles the handshake.
package tsdb

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"
)

const (
	protoMagic    uint32 = 0x42445354
	protoVer      byte   = 1
	hdrSize              = 24
	maxPayload           = 16 * 1024 * 1024
)

// Message types mirror src/server/proto.h.
const (
	MsgHello         byte = 1
	MsgHelloOK       byte = 2
	MsgError         byte = 3
	MsgPing          byte = 4
	MsgAuthLogin     byte = 5
	MsgAuthOK        byte = 6
	MsgCreateTable   byte = 20
	MsgDropTable     byte = 21
	MsgWriteBatch    byte = 30
	MsgWriteAck      byte = 34
	MsgQuery         byte = 40
	MsgQueryRsltHdr  byte = 41
	MsgQueryRsltRows byte = 42
	MsgSubscribe     byte = 50
	MsgSubEvent      byte = 51
	MsgUnsubscribe   byte = 52
	MsgClusterStats  byte = 60
)

const (
	FlagFin     uint16 = 1 << 0
	FlagPong    uint16 = 1 << 3
)

// Column types — must match include/tsdb.h.
const (
	TypeTimestamp byte = 1
	TypeInt64     byte = 2
	TypeFloat64   byte = 3
	TypeSymbol    byte = 4
	// TypeFloat32 stores 4 bytes on disk (half of Float64) but is sent and
	// returned as a 64-bit double — write its values via Row.F64.
	TypeFloat32 byte = 5
)

// crc32c table (Castagnoli polynomial) — Go's stdlib hash/crc32 uses hardware
// CRC on x86/ARM64 when available, matching the server's SSE4.2/ARMv8 path.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Frame is a decoded protocol frame returned by Recv.
type Frame struct {
	Type    byte
	Flags   uint16
	ReqID   uint64
	Payload []byte
}

// Conn is a single TCP connection to a tsdb server.
type Conn struct {
	c       net.Conn
	nextReq uint64
}

// Dial opens a plaintext connection.  For TLS see DialTLS.
// timeout may be zero to use the default of 10 s.
func Dial(addr string, timeout time.Duration) (*Conn, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Conn{c: c, nextReq: 1}, nil
}

// DialTLS opens a TLS connection.  A server started with --tls-cert/--tls-key
// speaks standard TLS on its regular port (every accepted connection is
// wrapped before the first frame), so this is Dial plus a TLS handshake — no
// STARTTLS step.  cfg may be nil for defaults; when cfg.ServerName is empty
// it is derived from addr.  timeout bounds the TCP connect and the TLS
// handshake together and may be zero to use the default of 10 s.
func DialTLS(addr string, timeout time.Duration, cfg *tls.Config) (*Conn, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{c: c, nextReq: 1}, nil
}

// Close closes the underlying socket.
func (c *Conn) Close() error {
	if c == nil || c.c == nil {
		return nil
	}
	return c.c.Close()
}

// SetDeadline applies a read/write deadline to the connection.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.c.SetDeadline(t)
}

// Send constructs and sends a frame, returning the req_id used.  If reqID is 0
// an auto-incrementing ID is picked.
func (c *Conn) Send(typ byte, flags uint16, reqID uint64, payload []byte) (uint64, error) {
	if len(payload) > maxPayload {
		return 0, errors.New("payload too large")
	}
	if reqID == 0 {
		reqID = c.nextReq
		c.nextReq++
	}
	var hdr [hdrSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], protoMagic)
	hdr[4] = protoVer
	hdr[5] = typ
	binary.LittleEndian.PutUint16(hdr[6:8], flags)
	// reserved [8..12) = 0
	binary.LittleEndian.PutUint64(hdr[12:20], reqID)
	binary.LittleEndian.PutUint32(hdr[20:24], uint32(len(payload)))

	// CRC32C over hdr[4..24) + payload, written LE after payload.
	sum := crc32.Update(0, crcTable, hdr[4:24])
	if len(payload) > 0 {
		sum = crc32.Update(sum, crcTable, payload)
	}
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], sum)

	if _, err := c.c.Write(hdr[:]); err != nil {
		return 0, err
	}
	if len(payload) > 0 {
		if _, err := c.c.Write(payload); err != nil {
			return 0, err
		}
	}
	if _, err := c.c.Write(trailer[:]); err != nil {
		return 0, err
	}
	return reqID, nil
}

// Recv reads one complete frame from the socket.  Validates magic, version,
// and CRC32C; returns the decoded Frame with its malloc'd Payload.
func (c *Conn) Recv() (*Frame, error) {
	var hdr [hdrSize]byte
	if _, err := io.ReadFull(c.c, hdr[:]); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != protoMagic {
		return nil, errors.New("bad magic")
	}
	if hdr[4] != protoVer {
		return nil, fmt.Errorf("unsupported ver %d", hdr[4])
	}
	f := &Frame{
		Type:  hdr[5],
		Flags: binary.LittleEndian.Uint16(hdr[6:8]),
		ReqID: binary.LittleEndian.Uint64(hdr[12:20]),
	}
	plen := binary.LittleEndian.Uint32(hdr[20:24])
	if plen > maxPayload {
		return nil, fmt.Errorf("payload too large: %d", plen)
	}
	if plen > 0 {
		f.Payload = make([]byte, plen)
		if _, err := io.ReadFull(c.c, f.Payload); err != nil {
			return nil, err
		}
	}
	var trailer [4]byte
	if _, err := io.ReadFull(c.c, trailer[:]); err != nil {
		return nil, err
	}
	want := binary.LittleEndian.Uint32(trailer[:])
	sum := crc32.Update(0, crcTable, hdr[4:24])
	if len(f.Payload) > 0 {
		sum = crc32.Update(sum, crcTable, f.Payload)
	}
	if sum != want {
		return nil, errors.New("crc mismatch")
	}
	return f, nil
}
