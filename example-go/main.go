// example-go/main.go — end-to-end demo of the Router against two QEngine
// clusters.
//
//   go run . [flags]
//
// Flags:
//   -a addr         cluster-a client addr (default lvm1:29090)
//   -b addr         cluster-b client addr (default lvm1:29190)
//   -am addr        cluster-a metrics addr (default lvm1:29094)
//   -bm addr        cluster-b metrics addr (default lvm1:29194)
//   -rows N         total rows to insert across the two clusters
//   -batch N        rows per WRITE_BATCH (larger = fewer round-trips)
//   -policy P       write policy — least_loaded | hash | round_robin
//   -reset          DROP TABLE before ingest on both sides
//
// What this does:
//   1. Connects to both clusters (Router + per-backend health probe).
//   2. Broadcasts CREATE TABLE to both.
//   3. Writes N rows, picking the "less loaded" cluster per batch under
//      LeastLoaded policy.  Snapshot stats after the ingest to show the
//      row-count split.
//   4. Fanouts several read queries and merges:
//        · count(*) across both (QueryMergeSum)
//        · SELECT * ... LIMIT   (QueryConcat)
//        · avg(price)           (QueryMergeAvg)
//   5. Shows how to pin traffic to one backend via router.Route("cluster-a").
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

func main() {
	var (
		addrA   = flag.String("a",  "lvm1:29090", "cluster-a client address")
		addrB   = flag.String("b",  "lvm1:29190", "cluster-b client address")
		metrA   = flag.String("am", "lvm1:29094", "cluster-a /metrics address")
		metrB   = flag.String("bm", "lvm1:29194", "cluster-b /metrics address")
		rows    = flag.Int("rows", 200000, "total rows to insert (split across clusters)")
		batch   = flag.Int("batch", 4096,   "rows per WRITE_BATCH")
		policy  = flag.String("policy", "least_loaded", "least_loaded | hash | round_robin")
		reset   = flag.Bool("reset", true, "DROP TABLE on both clusters first")
		table   = flag.String("table", "demo_trades", "target table name")
	)
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	pol := LeastLoaded
	switch *policy {
	case "least_loaded": pol = LeastLoaded
	case "hash":         pol = HashTable
	case "round_robin":  pol = RoundRobin
	default:
		log.Fatalf("unknown policy %q", *policy)
	}

	backends := []*Backend{
		{Name: "cluster-a", ClientAddr: *addrA, MetricsAddr: *metrA},
		{Name: "cluster-b", ClientAddr: *addrB, MetricsAddr: *metrB},
	}

	log.Printf("dialing backends…")
	r, err := NewRouter(backends, pol, 5*time.Second)
	if err != nil {
		log.Fatalf("NewRouter: %v", err)
	}
	defer r.Close()

	printStats := func(label string) {
		fmt.Printf("\n---- %s ----\n", label)
		for _, s := range r.Stats() {
			health := "DOWN"
			if s.Healthy {
				health = "UP"
			}
			fmt.Printf("  %-12s %-20s %s   rows_written=%d\n",
				s.Name, s.ClientAddr, health, s.RowsWritten)
		}
	}

	printStats("initial state")

	// ---- 1. DDL --------------------------------------------------------
	cols := []tsdb.Column{
		{Name: "ts",    Type: tsdb.TypeTimestamp},
		{Name: "price", Type: tsdb.TypeFloat64},
		{Name: "qty",   Type: tsdb.TypeInt64},
	}
	if *reset {
		for _, b := range r.Backends {
			_, _ = b.client.Query("DROP TABLE " + *table)
		}
	}
	if err := r.CreateTableEverywhere(*table, "ts", cols); err != nil {
		log.Fatalf("CREATE TABLE: %v", err)
	}
	log.Printf("created %s on both clusters", *table)

	// ---- 2. Ingest -----------------------------------------------------
	log.Printf("ingesting %d rows (batch=%d, policy=%s)…", *rows, *batch, *policy)
	rng := rand.New(rand.NewSource(42))
	perBackend := map[string]int{}
	t0 := time.Now()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	buf := make([]tsdb.Row, 0, *batch)
	for i := 0; i < *rows; i++ {
		buf = append(buf, tsdb.Row{
			TS:  base + int64(i)*1_000_000,
			F64: map[int]float64{1: 100.0 + rng.Float64()*20.0},
			I64: map[int]int64{2: int64(rng.Intn(1000) + 1)},
		})
		if len(buf) >= *batch {
			name, acked, err := r.WriteBatch(*table, cols, buf)
			if err != nil {
				log.Fatalf("WriteBatch [%s]: %v", name, err)
			}
			perBackend[name] += int(acked)
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		name, acked, err := r.WriteBatch(*table, cols, buf)
		if err != nil {
			log.Fatalf("WriteBatch tail: %v", err)
		}
		perBackend[name] += int(acked)
	}
	elapsed := time.Since(t0).Seconds()
	log.Printf("done: %d rows in %.2fs (%.0f rows/s)",
		*rows, elapsed, float64(*rows)/elapsed)
	for name, n := range perBackend {
		fmt.Printf("  %-12s → %d rows\n", name, n)
	}

	// Let the background prober catch up with the new counts.
	time.Sleep(2500 * time.Millisecond)
	printStats("post-ingest")

	// ---- 3. Merged reads ----------------------------------------------
	fmt.Println()
	fmt.Println("---- merged reads (fanout → reduce) ----")

	{
		n, err := r.QueryMergeSum("SELECT count(*) FROM " + *table)
		if err != nil {
			log.Fatalf("count(*): %v", err)
		}
		fmt.Printf("  count(*) across clusters         = %d\n", n)
	}

	{
		avg, err := r.QueryMergeAvg(*table, "price", "")
		if err != nil {
			log.Fatalf("avg(price): %v", err)
		}
		fmt.Printf("  avg(price) across clusters       = %.4f\n", avg)
	}

	{
		totalQty, err := r.QueryMergeSum("SELECT sum(qty) FROM " + *table)
		if err != nil {
			log.Fatalf("sum(qty): %v", err)
		}
		fmt.Printf("  sum(qty) across clusters         = %d\n", totalQty)
	}

	// Concat (non-aggregate) query limited to 3 rows per cluster.
	{
		merged, err := r.QueryConcat(fmt.Sprintf(
			"SELECT ts, price, qty FROM %s LIMIT 3", *table))
		if err != nil {
			log.Fatalf("SELECT ts,price,qty: %v", err)
		}
		fmt.Printf("  SELECT ts,price,qty LIMIT 3 per cluster → %d merged rows\n",
			len(merged.Rows))
		for name, n := range merged.BySource {
			fmt.Printf("    %-12s contributed %d rows\n", name, n)
		}
		for i, row := range merged.Rows {
			fmt.Printf("    row[%d] ts=%v price=%.2f qty=%v\n",
				i, row[0], row[1].(float64), row[2])
		}
	}

	// ---- 4. Pin a query to one backend --------------------------------
	fmt.Println()
	fmt.Println("---- pinned (target one backend) ----")
	for _, name := range []string{"cluster-a", "cluster-b"} {
		b := r.Route(name)
		if b == nil {
			log.Fatalf("no such backend %q", name)
		}
		res, err := b.client.Query("SELECT count(*), avg(price) FROM " + *table)
		if err != nil {
			log.Fatalf("pinned query %s: %v", name, err)
		}
		if len(res.Rows) > 0 && len(res.Rows[0]) >= 2 {
			fmt.Printf("  %-12s %s → count=%v avg(price)=%.4f\n",
				name, b.ClientAddr, res.Rows[0][0], res.Rows[0][1])
		}
	}

	fmt.Println()
	fmt.Println("done.")
	os.Exit(0)
}
