package tsdb

import (
	"net"
	"testing"
	"time"
)

// TestAdminSQLStrings drives the admin helpers against a mock listener
// (reconnect_test.go wire helpers) and asserts the exact SQL sent on the
// wire matches the server grammar (src/query/parse.c):
//
//	CREATE USER n IDENTIFIED BY '...' [ROLE ADMIN|NORMAL]
//	DROP USER n
//	GRANT|REVOKE <priv> ON <table>|* TO|FROM <user>
//	CREATE DATABASE n
func TestAdminSQLStrings(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	const nQueries = 6
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

	// Password with an embedded single quote: must be doubled on the wire.
	if err := cl.CreateUser("alice", "s3cr'et", true); err != nil {
		t.Fatalf("CreateUser(admin): %v", err)
	}
	expect("CREATE USER alice IDENTIFIED BY 's3cr''et' ROLE ADMIN")

	if err := cl.CreateUser("bob", "pw", false); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	expect("CREATE USER bob IDENTIFIED BY 'pw'")

	if err := cl.Grant("SELECT", "trades", "bob"); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	expect("GRANT SELECT ON trades TO bob")

	if err := cl.Revoke("ALL", "*", "bob"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	expect("REVOKE ALL ON * FROM bob")

	if err := cl.DropUser("bob"); err != nil {
		t.Fatalf("DropUser: %v", err)
	}
	expect("DROP USER bob")

	if err := cl.CreateDatabase("metrics"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	expect("CREATE DATABASE metrics")
}

// statsPayload builds a CLUSTER_STATS kv payload in the server's shape:
// [kv_count u16 LE] then per pair [klen u8][key][vlen u8][val].
func statsPayload(pairs [][2]string) []byte {
	p := []byte{byte(len(pairs)), byte(len(pairs) >> 8)}
	for _, kv := range pairs {
		p = append(p, byte(len(kv[0])))
		p = append(p, kv[0]...)
		p = append(p, byte(len(kv[1])))
		p = append(p, kv[1]...)
	}
	return p
}

// TestPingAndClusterStats drives Ping and ClusterStats against the mock
// listener: PING must be echoed back (same payload, PONG flag) and the
// CLUSTER_STATS kv payload must decode to key=value lines in server order.
func TestPingAndClusterStats(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

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
			return
		}
		// PING: echo the payload back, PONG|FIN, mirroring server.c.
		typ, _, reqID, payload, err := srvReadFrame(conn)
		if err != nil || typ != MsgPing {
			t.Errorf("want PING got typ=%d err=%v", typ, err)
			return
		}
		if err := srvWriteFrame(conn, MsgPing, FlagPong|FlagFin, reqID, payload); err != nil {
			return
		}
		// CLUSTER_STATS: HELLO_OK + kv payload, mirroring handle_cluster_stats.
		typ, _, reqID, _, err = srvReadFrame(conn)
		if err != nil || typ != MsgClusterStats {
			t.Errorf("want CLUSTER_STATS got typ=%d err=%v", typ, err)
			return
		}
		kv := statsPayload([][2]string{
			{"connections_active", "1"},
			{"rows_written_total", "12345"},
		})
		_ = srvWriteFrame(conn, MsgHelloOK, FlagFin, reqID, kv)
	}()

	cl, err := Open(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cl.Close()

	if err := cl.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	stats, err := cl.ClusterStats()
	if err != nil {
		t.Fatalf("ClusterStats: %v", err)
	}
	want := "connections_active=1\nrows_written_total=12345"
	if stats != want {
		t.Fatalf("ClusterStats = %q, want %q", stats, want)
	}
}
