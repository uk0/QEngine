// pipebench — compare synchronous WriteBatch vs pipelined writes.
//
// Over a high-RTT link, synchronous WriteBatch is bounded by 1/RTT because it
// waits for each batch's ack.  PipelineWriter keeps `window` batches in flight,
// so throughput becomes bandwidth/ingest-bound.  This tool writes the same
// deterministic dataset both ways and prints the speedup.
//
//	go run ./cmd/pipebench -addr 10.88.51.102:29301 -rows 500000 -batch 8192 -window 64
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

var cols = []tsdb.Column{
	{Name: "ts", Type: tsdb.TypeTimestamp},
	{Name: "price", Type: tsdb.TypeFloat64},
	{Name: "qty", Type: tsdb.TypeInt64},
}

func mkRows(base int64, start, n int) []tsdb.Row {
	rng := rand.New(rand.NewSource(int64(start) + 42))
	rows := make([]tsdb.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = tsdb.Row{
			TS:  base + int64(start+i)*1_000_000,
			F64: map[int]float64{1: 100.0 + rng.Float64()*50.0},
			I64: map[int]int64{2: int64(rng.Intn(1000) + 1)},
		}
	}
	return rows
}

func dial(addr, table string) *tsdb.Client {
	c, err := tsdb.Open(addr, 5*time.Second)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	_, _ = c.Query("DROP TABLE " + table)
	if err := c.CreateTable(table, "ts", cols); err != nil {
		log.Fatalf("create %s: %v", table, err)
	}
	return c
}

func main() {
	addr := flag.String("addr", "127.0.0.1:28090", "tsdb wire address")
	rows := flag.Int("rows", 500000, "rows to ingest per mode")
	batch := flag.Int("batch", 8192, "rows per batch")
	window := flag.Int("window", 64, "pipeline in-flight window")
	flag.Parse()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	nbatch := (*rows + *batch - 1) / *batch

	// ---- synchronous ----
	cs := dial(*addr, "pipebench_sync")
	t0 := time.Now()
	for b := 0; b < nbatch; b++ {
		n := *batch
		if b*(*batch)+n > *rows {
			n = *rows - b*(*batch)
		}
		if _, err := cs.WriteBatch("pipebench_sync", cols, mkRows(base, b*(*batch), n)); err != nil {
			log.Fatalf("sync write: %v", err)
		}
	}
	syncS := time.Since(t0).Seconds()
	cs.Close()

	// ---- pipelined ----
	cp := dial(*addr, "pipebench_pipe")
	pw := cp.NewPipelineWriter(*window)
	t0 = time.Now()
	for b := 0; b < nbatch; b++ {
		n := *batch
		if b*(*batch)+n > *rows {
			n = *rows - b*(*batch)
		}
		if err := pw.Write("pipebench_pipe", cols, mkRows(base, b*(*batch), n)); err != nil {
			log.Fatalf("pipe write: %v", err)
		}
	}
	if err := pw.Close(); err != nil {
		log.Fatalf("pipe close: %v", err)
	}
	pipeS := time.Since(t0).Seconds()
	cp.Close()

	syncRps := float64(*rows) / syncS
	pipeRps := float64(*rows) / pipeS
	fmt.Printf("rows=%d batch=%d window=%d  (%d batches each)\n", *rows, *batch, *window, nbatch)
	fmt.Printf("  sync     : %7.3fs  %10.0f rows/s\n", syncS, syncRps)
	fmt.Printf("  pipelined: %7.3fs  %10.0f rows/s\n", pipeS, pipeRps)
	fmt.Printf("  speedup  : %.2fx\n", pipeRps/syncRps)
}
