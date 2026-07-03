package tsdb

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// selfSignedCert generates an in-memory ECDSA certificate for 127.0.0.1 —
// just enough for a loopback tls.Listener; the client dials with
// InsecureSkipVerify so chain validation is not exercised here.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "tsdb-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestOpenTLS stands up an in-test tls.Listener speaking the mock wire
// protocol (same srvReadFrame/srvWriteFrame codec as reconnect_test.go) and
// asserts that (1) OpenTLS completes the TLS + HELLO handshake, and (2) after
// the server drops the session, the automatic reconnect re-dials over TLS as
// well — the second accept only succeeds on the TLS listener — and the
// retried query completes.
func TestOpenTLS(t *testing.T) {
	cert := selfSignedCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	defer ln.Close()

	var accepts int32
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		// --- connection #1: TLS handshake happens implicitly on the first
		//     read.  Complete HELLO, then drop to force a reconnect.
		c1, err := ln.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&accepts, 1)
		if typ, _, _, _, err := srvReadFrame(c1); err != nil || typ != MsgHello {
			t.Errorf("conn1: want HELLO got typ=%d err=%v", typ, err)
			c1.Close()
			return
		}
		if err := srvWriteFrame(c1, MsgHelloOK, 0, 0, nil); err != nil {
			t.Errorf("conn1 HELLO_OK: %v", err)
		}
		c1.Close()

		// --- connection #2: the re-dial.  Reaching here over the TLS
		//     listener proves the reconnect used DialTLS, not plaintext.
		c2, err := ln.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&accepts, 1)
		defer c2.Close()
		if typ, _, _, _, err := srvReadFrame(c2); err != nil || typ != MsgHello {
			t.Errorf("conn2: want HELLO got typ=%d err=%v", typ, err)
			return
		}
		if err := srvWriteFrame(c2, MsgHelloOK, 0, 0, nil); err != nil {
			t.Errorf("conn2 HELLO_OK: %v", err)
		}
		typ, _, reqID, _, err := srvReadFrame(c2)
		if err != nil || typ != MsgQuery {
			t.Errorf("conn2: want QUERY got typ=%d err=%v", typ, err)
			return
		}
		if err := srvWriteFrame(c2, MsgQueryRsltHdr, FlagFin, reqID, emptyQueryHdr()); err != nil {
			t.Errorf("conn2 QUERY hdr: %v", err)
		}
	}()

	cl, err := OpenTLS(ln.Addr().String(), time.Second,
		&tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("OpenTLS: %v", err)
	}
	defer cl.Close()

	// Issued on the now-dropped conn #1: the client must reconnect over TLS
	// and re-run the query on conn #2.
	if _, err := cl.Query("SELECT 1"); err != nil {
		t.Fatalf("Query across TLS reconnect: %v", err)
	}
	<-serverDone
	if got := atomic.LoadInt32(&accepts); got != 2 {
		t.Fatalf("expected 2 TLS accepts (dial + re-dial), got %d", got)
	}
}

// TestDialTLSRefusesPlaintext pins the failure mode against a plaintext
// listener: the TLS handshake must error out instead of silently speaking
// plaintext frames.
func TestDialTLSRefusesPlaintext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		// Reply with garbage (a plaintext frame header) to the ClientHello.
		_ = srvWriteFrame(c, MsgHelloOK, 0, 0, nil)
		c.Close()
	}()
	c, err := DialTLS(ln.Addr().String(), time.Second,
		&tls.Config{InsecureSkipVerify: true})
	if err == nil {
		c.Close()
		t.Fatal("DialTLS against a plaintext server must fail the handshake")
	}
}
