package tsdb

import (
	"net"
	"testing"
	"time"
)

// TestUDFSQLStrings drives RegisterUDF/DropUDF against a mock listener
// (reconnect_test.go wire helpers) and asserts the exact SQL sent on the
// wire matches the server's CREATE FUNCTION grammar.
func TestUDFSQLStrings(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	const nQueries = 3
	got := make(chan string, nQueries)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if typ, _, _, _, err := srvReadFrame(conn); err != nil || typ != MsgHello {
			t.Errorf("want HELLO got typ=%d err=%v", typ, err)
			return
		}
		if err := srvWriteFrame(conn, MsgHelloOK, 0, 0, nil); err != nil {
			t.Errorf("HELLO_OK: %v", err)
			return
		}
		for i := 0; i < nQueries; i++ {
			typ, _, reqID, payload, err := srvReadFrame(conn)
			if err != nil {
				return
			}
			if typ != MsgQuery {
				t.Errorf("frame %d: want QUERY got typ=%d", i, typ)
				return
			}
			got <- string(payload)
			if err := srvWriteFrame(conn, MsgQueryRsltHdr, FlagFin, reqID, emptyQueryHdr()); err != nil {
				return
			}
		}
	}()

	cl, err := Open(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cl.Close()

	expect := func(want string) {
		t.Helper()
		select {
		case sql := <-got:
			if sql != want {
				t.Fatalf("SQL mismatch:\n got: %q\nwant: %q", sql, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no query captured (want %q)", want)
		}
	}

	if err := cl.RegisterUDF("my_double",
		[]byte{TypeFloat64, TypeInt64, TypeTimestamp}, TypeFloat64,
		"/opt/udf/x.so", "udf_double"); err != nil {
		t.Fatalf("RegisterUDF: %v", err)
	}
	expect("CREATE FUNCTION my_double(FLOAT64, INT64, TIMESTAMP) " +
		"RETURNS FLOAT64 FROM '/opt/udf/x.so' SYMBOL 'udf_double'")

	// Empty symbol → SYMBOL clause omitted (server defaults it to name);
	// zero args → empty parens.
	if err := cl.RegisterUDF("nofn", nil, TypeInt64, "/opt/udf/y.so", ""); err != nil {
		t.Fatalf("RegisterUDF(no symbol): %v", err)
	}
	expect("CREATE FUNCTION nofn() RETURNS INT64 FROM '/opt/udf/y.so'")

	if err := cl.DropUDF("my_double"); err != nil {
		t.Fatalf("DropUDF: %v", err)
	}
	expect("DROP FUNCTION my_double")

	// Unsupported type byte fails client-side without touching the wire.
	if err := cl.RegisterUDF("bad", []byte{TypeSymbol}, TypeInt64, "/p.so", ""); err == nil {
		t.Fatal("RegisterUDF with SYMBOL arg type: want error, got nil")
	}
	if err := cl.RegisterUDF("bad2", []byte{TypeInt64}, TypeSymbol, "/p.so", ""); err == nil {
		t.Fatal("RegisterUDF with SYMBOL ret type: want error, got nil")
	}
}
