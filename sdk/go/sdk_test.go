package tsdb

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Requires a running tsdb-server on TSDB_TEST_ADDR (default 127.0.0.1:28090)
// with require_auth=false.  Spin one up outside this test and set the env.
func addrOrSkip(t *testing.T) string {
	a := os.Getenv("TSDB_TEST_ADDR")
	if a == "" {
		a = "127.0.0.1:28090"
	}
	c, err := Open(a, 1*time.Second)
	if err != nil {
		t.Skipf("no server at %s (%v) — run: build/tsdb-server --bind %s", a, err, a)
	}
	_ = c.Close()
	return a
}

func TestHelloAndQuery(t *testing.T) {
	addr := addrOrSkip(t)
	c, err := Open(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	// CREATE a small table (idempotent server-side).
	if err := c.CreateTable("sdktest",
		"ts",
		[]Column{
			{"ts", TypeTimestamp},
			{"v", TypeInt64},
		}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Insert 10 rows.
	rows := make([]Row, 10)
	for i := 0; i < 10; i++ {
		rows[i] = Row{
			TS:  int64(i) * 1_000_000_000,
			I64: map[int]int64{1: int64(i * 2)},
		}
	}
	acked, err := c.WriteBatch("sdktest",
		[]Column{{"ts", TypeTimestamp}, {"v", TypeInt64}},
		rows)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if acked != 10 {
		t.Fatalf("expected 10 acked, got %d", acked)
	}

	// Query back.
	qr, err := c.Query("SELECT count(*) FROM sdktest")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(qr.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(qr.Rows))
	}
	// count(*) comes back as int64.
	gotRaw := qr.Rows[0][0]
	got, ok := gotRaw.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", gotRaw)
	}
	if got < 10 {
		t.Fatalf("count(*) = %d, expected >=10", got)
	}
}

// Helper for child-process orchestration in the bench harness.
var _ = exec.Command
var _ = strings.TrimSpace
