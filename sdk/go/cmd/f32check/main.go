// f32check — FLOAT32 aggregate + projection verification over the binary wire.
//
// Regression guard for the FLOAT32 wire bug: min/max/spread/first/last over a
// FLOAT32 column used to label the result cell as raw type-5, which the binary
// emit path left uninitialised and the SDK decoded as nil.  This writes a
// deterministic FLOAT32 dataset (values 0..N-1, all exactly representable in
// float32 for N < 2^24) and asserts every operator returns the exact expected
// double — non-nil — end to end through the Go SDK.
//
//	go run ./cmd/f32check -addr HOST:29301 -rows 100000
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

var fails int

func check(name string, ok bool, detail string) {
	mark := "OK"
	if !ok {
		mark = "FAIL"
		fails++
	}
	fmt.Printf("  [%-4s] %-22s %s\n", mark, name, detail)
}

// q1 returns (value, isNil, ok).  isNil distinguishes a real null cell (the
// pre-fix symptom) from a decoded double.
func q1(c *tsdb.Client, sql string) (float64, bool, bool) {
	r, err := c.Query(sql)
	if err != nil || len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
		return 0, false, false
	}
	switch v := r.Rows[0][0].(type) {
	case float64:
		return v, false, true
	case int64:
		return float64(v), false, true
	case nil:
		return 0, true, true // the bug: FLOAT32 op decoded as nil
	}
	return 0, false, false
}

func main() {
	addr := flag.String("addr", "127.0.0.1:29301", "tsdb wire address")
	rows := flag.Int("rows", 100000, "row count N (< 2^24 for exact f32)")
	table := flag.String("table", "f32check_rig", "table name")
	flag.Parse()

	N := int64(*rows)

	c, err := tsdb.Open(*addr, 8*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	if err := c.Login("root", "123456"); err != nil {
		fmt.Fprintf(os.Stderr, "login: %v\n", err)
		os.Exit(1)
	}

	cols := []tsdb.Column{
		{Name: "ts", Type: tsdb.TypeTimestamp},
		{Name: "p", Type: tsdb.TypeFloat32}, // the column under test
	}
	_, _ = c.Query("DROP TABLE " + *table)
	if err := c.CreateTable(*table, "ts", cols); err != nil {
		fmt.Fprintf(os.Stderr, "create: %v\n", err)
		os.Exit(1)
	}

	const batch = 4096
	var buf []tsdb.Row
	flush := func() {
		if len(buf) == 0 {
			return
		}
		if _, err := c.WriteBatch(*table, cols, buf); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			os.Exit(1)
		}
		buf = buf[:0]
	}
	for i := int64(0); i < N; i++ {
		buf = append(buf, tsdb.Row{
			TS:  1_700_000_000_000_000_000 + i,
			F64: map[int]float64{1: float64(i)}, // FLOAT32 written via F64
		})
		if len(buf) >= batch {
			flush()
		}
	}
	flush()
	fmt.Printf("ingested N=%d FLOAT32 rows\n\n=== FLOAT32 operators over binary wire ===\n", N)

	// Ground truth (values 0..N-1, exact in float32).
	want := map[string]float64{
		"min(p)":    0,
		"max(p)":    float64(N - 1),
		"spread(p)": float64(N - 1),
		"first(p)":  0,
		"last(p)":   float64(N - 1),
		"sum(p)":    float64(N*(N-1)) / 2.0,
		"avg(p)":    float64(N-1) / 2.0,
	}
	// Order matters for readability; min/max/spread/first/last were the bug.
	for _, op := range []string{"min(p)", "max(p)", "spread(p)", "first(p)", "last(p)", "sum(p)", "avg(p)"} {
		got, isNil, ok := q1(c, "SELECT "+op+" FROM "+*table)
		if !ok {
			check(op, false, "query failed")
		} else if isNil {
			check(op, false, "decoded NIL (the FLOAT32 wire bug)")
		} else {
			check(op, got == want[op], fmt.Sprintf("got=%.1f want=%.1f", got, want[op]))
		}
	}

	// Raw projection: a bare FLOAT32 column cell must decode as a double too.
	{
		r, err := c.Query("SELECT p FROM " + *table + " WHERE ts = 1700000000000000042")
		if err != nil || len(r.Rows) == 0 {
			check("raw proj p (ts#42)", false, "query failed")
		} else if r.Rows[0][0] == nil {
			check("raw proj p (ts#42)", false, "decoded NIL")
		} else {
			v, _ := r.Rows[0][0].(float64)
			check("raw proj p (ts#42)", v == 42.0, fmt.Sprintf("got=%v want=42", r.Rows[0][0]))
		}
	}

	_, _ = c.Query("DROP TABLE " + *table)
	fmt.Printf("\n==== f32check: %d FAIL ====\n", fails)
	if fails > 0 {
		os.Exit(1)
	}
}
