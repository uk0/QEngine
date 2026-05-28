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

// TestSymbolRoundTrip guards the binary-wire query-result encoding for
// SYMBOL columns.  Pre-2026-05 the server emitted strlen() into an 8-byte
// cell (a never-finished stub), so SELECTing a symbol/tag column returned
// the string's *length* instead of the string.  This writes distinct
// symbol values and asserts they round-trip exactly — directly and through
// GROUP BY (where the group key is the symbol).
func TestSymbolRoundTrip(t *testing.T) {
	addr := addrOrSkip(t)
	c, err := Open(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	cols := []Column{
		{"ts", TypeTimestamp},
		{"v", TypeFloat64},
		{"sym", TypeSymbol},
	}
	if err := c.CreateTable("sdk_symrt", "ts", cols); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// 3 distinct symbols of DIFFERENT lengths so a length-encoding bug
	// (the old behavior) produces detectably wrong, non-string values.
	want := []string{"alpha", "b", "gamma12345"}
	var rows []Row
	for i := 0; i < 9; i++ {
		rows = append(rows, Row{
			TS:  int64(i) * 1_000_000_000,
			F64: map[int]float64{1: float64(i)},
			Sym: map[int]string{2: want[i%3]},
		})
	}
	if _, err := c.WriteBatch("sdk_symrt", cols, rows); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Direct projection: every sym cell must be one of the written strings.
	qr, err := c.Query("SELECT sym FROM sdk_symrt LIMIT 9")
	if err != nil {
		t.Fatalf("Query sym: %v", err)
	}
	seen := map[string]int{}
	for _, row := range qr.Rows {
		s, ok := row[0].(string)
		if !ok {
			t.Fatalf("sym cell is %T (%v), want string — symbol wire decode regressed", row[0], row[0])
		}
		seen[s]++
	}
	for _, w := range want {
		if seen[w] == 0 {
			t.Fatalf("symbol %q missing from result (got %v)", w, seen)
		}
	}

	// GROUP BY symbol: labels must be the real strings, counts 3 each.
	qr, err = c.Query("SELECT sym, count(*) FROM sdk_symrt GROUP BY sym")
	if err != nil {
		t.Fatalf("Query group: %v", err)
	}
	groups := map[string]int64{}
	for _, row := range qr.Rows {
		s, ok := row[0].(string)
		if !ok {
			t.Fatalf("group key is %T, want string", row[0])
		}
		switch cnt := row[1].(type) {
		case int64:
			groups[s] = cnt
		case float64:
			groups[s] = int64(cnt)
		}
	}
	for _, w := range want {
		if groups[w] != 3 {
			t.Fatalf("group %q count=%d want=3 (groups=%v)", w, groups[w], groups)
		}
	}

	_, _ = c.Query("DROP TABLE sdk_symrt")
}

// Helper for child-process orchestration in the bench harness.
var _ = exec.Command
var _ = strings.TrimSpace
