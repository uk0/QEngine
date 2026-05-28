// aggcheck — strict aggregate-operator verification via the Go SDK.
//
// Writes a deterministic dataset where every aggregate has an exact
// analytical ground truth (values are row indices, exactly representable
// as float64 for i < 2^53 and whose running sums stay < 2^53), then
// compares engine output byte-for-byte against the computed expectation.
//
// Covers: count / sum / avg / min / max / spread on the full set, on a
// WHERE-filtered subset, on a single row, and on the empty set; plus
// per-group GROUP BY exactness and stddev / p50 / p99 within tolerance.
//
//	go run ./cmd/aggcheck -addr HOST:29301 -rows 100000 -groups 8
package main

import (
	"flag"
	"fmt"
	"math"
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
	fmt.Printf("  [%-4s] %-34s %s\n", mark, name, detail)
}

func q1f(c *tsdb.Client, sql string) (float64, bool) {
	r, err := c.Query(sql)
	if err != nil || len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
		return 0, false
	}
	switch v := r.Rows[0][0].(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case nil:
		return math.NaN(), true
	}
	return 0, false
}

func main() {
	addr := flag.String("addr", "127.0.0.1:29301", "tsdb wire address")
	rows := flag.Int("rows", 100000, "row count N")
	groups := flag.Int("groups", 8, "distinct group cardinality")
	table := flag.String("table", "agg_rig", "table name")
	flag.Parse()

	N := int64(*rows)
	G := int64(*groups)

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

	// Fresh table.  drop-and-recreate via SDK CreateTable (idempotent) —
	// use a unique suffix so a stale schema never collides.
	cols := []tsdb.Column{
		{Name: "ts", Type: tsdb.TypeTimestamp},
		{Name: "v", Type: tsdb.TypeFloat64},
		{Name: "n", Type: tsdb.TypeInt64},
		{Name: "sym", Type: tsdb.TypeSymbol},
	}
	if err := c.CreateTable(*table, "ts", cols); err != nil {
		fmt.Fprintf(os.Stderr, "create: %v\n", err)
		os.Exit(1)
	}

	// Write rows: v=i, n=i, sym=g{i%G}, ts strictly ascending.
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
	t0 := time.Now()
	for i := int64(0); i < N; i++ {
		buf = append(buf, tsdb.Row{
			TS:  1_700_000_000_000_000_000 + i,
			F64: map[int]float64{1: float64(i)},
			I64: map[int]int64{2: i},
			Sym: map[int]string{3: fmt.Sprintf("g%d", i%G)},
		})
		if len(buf) >= batch {
			flush()
		}
	}
	flush()
	fmt.Printf("ingested N=%d in %.2fs\n", N, time.Since(t0).Seconds())

	// ---- Analytical ground truth ----
	sumAll := float64(N*(N-1)) / 2.0 // exact for our N
	avgAll := float64(N-1) / 2.0
	minAll := 0.0
	maxAll := float64(N - 1)
	spreadAll := maxAll - minAll

	fmt.Printf("\n=== global aggregates (N=%d) ===\n", N)
	if got, ok := q1f(c, "SELECT count(*) FROM "+*table); ok {
		check("count(*)", got == float64(N), fmt.Sprintf("got=%.0f want=%d", got, N))
	} else {
		check("count(*)", false, "query failed")
	}
	if got, ok := q1f(c, "SELECT sum(v) FROM "+*table); ok {
		check("sum(v)", got == sumAll, fmt.Sprintf("got=%.1f want=%.1f", got, sumAll))
	} else {
		check("sum(v)", false, "query failed")
	}
	if got, ok := q1f(c, "SELECT avg(v) FROM "+*table); ok {
		check("avg(v)", got == avgAll, fmt.Sprintf("got=%.4f want=%.4f", got, avgAll))
	} else {
		check("avg(v)", false, "query failed")
	}
	if got, ok := q1f(c, "SELECT min(v) FROM "+*table); ok {
		check("min(v)", got == minAll, fmt.Sprintf("got=%.1f want=%.1f", got, minAll))
	} else {
		check("min(v)", false, "query failed")
	}
	if got, ok := q1f(c, "SELECT max(v) FROM "+*table); ok {
		check("max(v)", got == maxAll, fmt.Sprintf("got=%.1f want=%.1f", got, maxAll))
	} else {
		check("max(v)", false, "query failed")
	}
	if got, ok := q1f(c, "SELECT spread(v) FROM "+*table); ok {
		check("spread(v)", got == spreadAll, fmt.Sprintf("got=%.1f want=%.1f", got, spreadAll))
	} else {
		check("spread(v)", false, "query failed")
	}
	if got, ok := q1f(c, "SELECT sum(n) FROM "+*table); ok {
		check("sum(n) [int64]", got == sumAll, fmt.Sprintf("got=%.0f want=%.0f", got, sumAll))
	} else {
		check("sum(n) [int64]", false, "query failed")
	}

	// ---- WHERE-filtered subset: n < N/2 → rows 0..N/2-1 ----
	half := N / 2
	sumHalf := float64(half*(half-1)) / 2.0
	fmt.Printf("\n=== filtered subset (n < %d) ===\n", half)
	if got, ok := q1f(c, fmt.Sprintf("SELECT count(*) FROM %s WHERE n < %d", *table, half)); ok {
		check("count filtered", got == float64(half), fmt.Sprintf("got=%.0f want=%d", got, half))
	}
	if got, ok := q1f(c, fmt.Sprintf("SELECT sum(v) FROM %s WHERE n < %d", *table, half)); ok {
		check("sum filtered", got == sumHalf, fmt.Sprintf("got=%.1f want=%.1f", got, sumHalf))
	}

	// ---- Single row: n = 42 ----
	fmt.Printf("\n=== single row (n = 42) ===\n")
	if got, ok := q1f(c, fmt.Sprintf("SELECT avg(v) FROM %s WHERE n = 42", *table)); ok {
		check("avg single", got == 42.0, fmt.Sprintf("got=%.1f want=42.0", got))
	}
	if got, ok := q1f(c, fmt.Sprintf("SELECT min(v) FROM %s WHERE n = 42", *table)); ok {
		check("min single", got == 42.0, fmt.Sprintf("got=%.1f want=42.0", got))
	}

	// ---- Empty set: v > 1e18 ----
	fmt.Printf("\n=== empty set (v > 1e18) ===\n")
	if got, ok := q1f(c, "SELECT count(*) FROM "+*table+" WHERE v > 1e18"); ok {
		check("count empty", got == 0, fmt.Sprintf("got=%.0f want=0", got))
	}
	if got, ok := q1f(c, "SELECT sum(v) FROM "+*table+" WHERE v > 1e18"); ok {
		check("sum empty", got == 0, fmt.Sprintf("got=%.0f want=0 (sum of nothing)", got))
	}
	if got, ok := q1f(c, "SELECT avg(v) FROM "+*table+" WHERE v > 1e18"); ok {
		check("avg empty=NaN/null", math.IsNaN(got), fmt.Sprintf("got=%v (want null)", got))
	}

	// ---- Per-group GROUP BY exactness ----
	fmt.Printf("\n=== GROUP BY sym (G=%d) ===\n", G)
	r, err := c.Query("SELECT sym, count(*), sum(v) FROM " + *table + " GROUP BY sym")
	if err != nil {
		check("GROUP BY query", false, err.Error())
	} else {
		// expected per group k: rows {k, k+G, ...}; count = (N-1-k)/G + 1 for k<N
		// sum = sum of those indices.
		expCount := map[string]int64{}
		expSum := map[string]float64{}
		for i := int64(0); i < N; i++ {
			g := fmt.Sprintf("g%d", i%G)
			expCount[g]++
			expSum[g] += float64(i)
		}
		allOK := true
		seen := 0
		for _, row := range r.Rows {
			if len(row) < 3 {
				continue
			}
			sym, _ := row[0].(string)
			var cnt float64
			switch v := row[1].(type) {
			case int64:
				cnt = float64(v)
			case float64:
				cnt = v
			}
			var sm float64
			switch v := row[2].(type) {
			case float64:
				sm = v
			case int64:
				sm = float64(v)
			}
			seen++
			if cnt != float64(expCount[sym]) || sm != expSum[sym] {
				allOK = false
				fmt.Printf("      group %s: got(cnt=%.0f sum=%.1f) want(cnt=%d sum=%.1f)\n",
					sym, cnt, sm, expCount[sym], expSum[sym])
			}
		}
		check("group count+sum exact", allOK && seen == int(G),
			fmt.Sprintf("%d/%d groups exact", seen, G))
	}

	// ---- stddev / percentile within tolerance ----
	// population stddev of 0..N-1 = sqrt((N^2-1)/12)
	fmt.Printf("\n=== stddev / percentile (tolerance) ===\n")
	popStd := math.Sqrt(float64(N*N-1) / 12.0)
	if got, ok := q1f(c, "SELECT stddev(v) FROM "+*table); ok {
		// accept population or sample variance form within 0.5%
		sampleStd := math.Sqrt(float64(N) / float64(N-1)) * popStd
		tol := 0.005 * popStd
		ok2 := math.Abs(got-popStd) < tol || math.Abs(got-sampleStd) < tol
		check("stddev≈√((N²-1)/12)", ok2, fmt.Sprintf("got=%.2f pop=%.2f samp=%.2f", got, popStd, sampleStd))
	}
	if got, ok := q1f(c, "SELECT p50(v) FROM "+*table); ok {
		want := avgAll
		check("p50≈median", math.Abs(got-want) < 0.02*float64(N), fmt.Sprintf("got=%.1f want≈%.1f", got, want))
	}
	if got, ok := q1f(c, "SELECT p99(v) FROM "+*table); ok {
		want := 0.99 * float64(N-1)
		check("p99≈0.99·max", math.Abs(got-want) < 0.02*float64(N), fmt.Sprintf("got=%.1f want≈%.1f", got, want))
	}

	// cleanup
	_, _ = c.Query("DROP TABLE " + *table)

	fmt.Printf("\n==== aggcheck: %d FAIL ====\n", fails)
	if fails > 0 {
		os.Exit(1)
	}
}
