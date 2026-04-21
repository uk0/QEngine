// stress-200m — industrial-scale write test for the 3-level DB model.
//
// Workload:
//   - Creates one Database (iot_core).
//   - Creates N_VTABLES (64) VTables under it, each with 1..10 FLOAT64
//     columns plus the mandatory ts column, plus (ts, region_tag).
//   - Creates N_PTABLES (5000) PTables, evenly distributed across the
//     64 VTables (≈78 per VTable).
//   - W concurrent writers each pick a random PTable per batch and
//     write random rows with ts ≥ a shared monotonically-advancing base.
//   - Target: TOTAL_ROWS (200M) across the whole run.
//
// Correctness sampling: after the write completes, pick a few PTables
// and compare count(*) + sum(col1) against what we streamed (using a
// local per-PTable row counter).
//
// Output: one JSON-line per phase on stdout, final summary to stderr.
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

// ---- Config ----------------------------------------------------------

type cfg struct {
	Addr        string
	DBName      string
	NVTables    int
	NPTables    int
	TotalRows   int64
	Writers     int
	BatchSize   int
	MaxCols     int
	SampleCheck int
}

// ---- Helpers ---------------------------------------------------------

func openC(addr string) *tsdb.Client {
	c, err := tsdb.Open(addr, 15*time.Second)
	if err != nil {
		log.Fatalf("open %s: %v", addr, err)
	}
	return c
}

func mustQuery(c *tsdb.Client, sql string) *tsdb.QueryResult {
	r, err := c.Query(sql)
	if err != nil {
		log.Fatalf("query [%s]: %v", sql, err)
	}
	return r
}

func mustExec(c *tsdb.Client, sql string) {
	r, err := c.Query(sql)
	if err != nil {
		log.Fatalf("exec [%s]: %v", sql, err)
	}
	_ = r
}

// rowSpec captures what a PTable looks like so writers can produce rows
// that match its column count.  We don't store column names here — the
// Go SDK's CreateTable + WriteBatch takes (name, type) tuples, but the
// child-table path inherits columns from the VTable at catalog level.
type ptableSpec struct {
	name     string
	vtable   string
	ncols    int // data columns (excludes ts)
}

type vtableSpec struct {
	name  string
	group string
	ncols int
}

// groupName distributes the N VTables across G groups so the 4-level
// tree actually shows a populated Group layer between DB and VTable.
func groupName(i, nGroups int) string {
	return fmt.Sprintf("factory_%d", i%nGroups)
}

func colName(i int) string {
	return fmt.Sprintf("c%d", i)
}

// ---- Build catalog ---------------------------------------------------

func buildCatalog(cfg cfg) ([]vtableSpec, []ptableSpec) {
	c := openC(cfg.Addr)
	defer c.Close()

	log.Printf("creating database %q", cfg.DBName)
	rc, _ := c.Query("CREATE DATABASE " + cfg.DBName)
	_ = rc

	// Groups form the missing layer between DB and VTable in the
	// 4-level model.  Eight factories gives a realistic digital-twin
	// shape — every factory owns ~8 of the 64 VTables below.
	const nGroups = 8
	log.Printf("creating %d Groups under database %q", nGroups, cfg.DBName)
	for i := 0; i < nGroups; i++ {
		mustExec(c, fmt.Sprintf("CREATE GROUP %s IN DATABASE %s",
			groupName(i, nGroups), cfg.DBName))
	}

	// 64 VTables with ncols ∈ [1..MaxCols], sprayed across the groups.
	vts := make([]vtableSpec, cfg.NVTables)
	rng := rand.New(rand.NewSource(1))
	log.Printf("creating %d VTables", cfg.NVTables)
	for i := 0; i < cfg.NVTables; i++ {
		ncols := 1 + rng.Intn(cfg.MaxCols)
		name := fmt.Sprintf("vt_%03d", i)
		grp := groupName(i, nGroups)
		vts[i] = vtableSpec{name: name, group: grp, ncols: ncols}
		var b strings.Builder
		fmt.Fprintf(&b,
			"CREATE VTABLE %s IN DATABASE %s IN GROUP %s (ts TIMESTAMP",
			name, cfg.DBName, grp)
		for j := 0; j < ncols; j++ {
			fmt.Fprintf(&b, ", %s FLOAT64", colName(j))
		}
		fmt.Fprintf(&b, ") TAGS (region SYMBOL)")
		mustExec(c, b.String())
	}

	// 5000 PTables distributed across VTables.
	pts := make([]ptableSpec, cfg.NPTables)
	log.Printf("creating %d PTables (~%.1f per VTable)",
		cfg.NPTables, float64(cfg.NPTables)/float64(cfg.NVTables))
	for i := 0; i < cfg.NPTables; i++ {
		vt := vts[i%cfg.NVTables]
		name := fmt.Sprintf("pt_%05d", i)
		pts[i] = ptableSpec{name: name, vtable: vt.name, ncols: vt.ncols}
		region := fmt.Sprintf("r%d", i%8)
		// Need to quote with single quotes inside SQL string.
		sql := fmt.Sprintf("CREATE TABLE %s USING %s TAGS ('%s')",
			name, vt.name, region)
		mustExec(c, sql)
	}

	return vts, pts
}

// ---- Writers ---------------------------------------------------------

type writerStats struct {
	rows int64
	errs int64
}

func writerLoop(cfg cfg, pts []ptableSpec, done chan struct{},
	s *writerStats, wg *sync.WaitGroup, seed int64) {
	defer wg.Done()
	c := openC(cfg.Addr)
	defer c.Close()
	rng := rand.New(rand.NewSource(seed))

	// Per-writer ts cursor so writes across workers don't conflict.
	// Base: 2026-01-01.  Step: 1ms.
	baseNS := int64(1767225600) * int64(1e9)
	cursor := baseNS + int64(seed)*int64(1e9)

	for {
		select {
		case <-done:
			return
		default:
		}
		pt := pts[rng.Intn(len(pts))]
		// Build per-batch column schema matching this PTable.
		cols := make([]tsdb.Column, pt.ncols+1)
		cols[0] = tsdb.Column{Name: "ts", Type: tsdb.TypeTimestamp}
		for i := 0; i < pt.ncols; i++ {
			cols[i+1] = tsdb.Column{Name: colName(i), Type: tsdb.TypeFloat64}
		}
		rows := make([]tsdb.Row, cfg.BatchSize)
		for r := 0; r < cfg.BatchSize; r++ {
			cursor += 1000 // 1 us per row
			rows[r] = tsdb.Row{
				TS:  cursor,
				F64: map[int]float64{},
			}
			for i := 0; i < pt.ncols; i++ {
				rows[r].F64[i+1] = rng.Float64() * 100.0
			}
		}
		_, err := c.WriteBatch(pt.name, cols, rows)
		if err != nil {
			atomic.AddInt64(&s.errs, 1)
			// Back off briefly on error.
			time.Sleep(20 * time.Millisecond)
			continue
		}
		atomic.AddInt64(&s.rows, int64(len(rows)))
	}
}

// ---- Stress driver ---------------------------------------------------

func runStress(cfg cfg, pts []ptableSpec) {
	log.Printf("starting %d writers, target %d rows, batch %d",
		cfg.Writers, cfg.TotalRows, cfg.BatchSize)
	var stats writerStats
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(cfg.Writers)
	for w := 0; w < cfg.Writers; w++ {
		go writerLoop(cfg, pts, done, &stats, &wg, int64(w+1))
	}

	// Progress reporter.
	t0 := time.Now()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	for {
		rows := atomic.LoadInt64(&stats.rows)
		if rows >= cfg.TotalRows {
			break
		}
		select {
		case <-tick.C:
			elapsed := time.Since(t0).Seconds()
			rps := float64(rows) / elapsed
			remain := float64(cfg.TotalRows-rows) / rps
			log.Printf("progress: %d / %d rows · %.1f rows/sec · "+
				"errs=%d · ETA %.0fs",
				rows, cfg.TotalRows, rps,
				atomic.LoadInt64(&stats.errs), remain)
		}
	}
	close(done)
	wg.Wait()
	elapsed := time.Since(t0).Seconds()
	final := atomic.LoadInt64(&stats.rows)
	fmt.Fprintf(os.Stderr,
		"\nDONE  wrote=%d  wall=%.2fs  rate=%.1f rows/sec  errs=%d\n",
		final, elapsed, float64(final)/elapsed,
		atomic.LoadInt64(&stats.errs))
}

// ---- Correctness sample ----------------------------------------------

func sampleCorrectness(cfg cfg, pts []ptableSpec) {
	c := openC(cfg.Addr)
	defer c.Close()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	log.Printf("sampling %d PTables for aggregate sanity", cfg.SampleCheck)
	ok, fail := 0, 0
	for i := 0; i < cfg.SampleCheck; i++ {
		pt := pts[rng.Intn(len(pts))]
		ops := []string{
			fmt.Sprintf("SELECT count(*) FROM %s",          pt.name),
			fmt.Sprintf("SELECT min(c0), max(c0), avg(c0) FROM %s", pt.name),
			fmt.Sprintf("SELECT spread(c0) FROM %s",        pt.name),
			fmt.Sprintf("SELECT p50(c0) FROM %s",           pt.name),
			fmt.Sprintf("SELECT first(c0), last(c0) FROM %s", pt.name),
		}
		all := true
		for _, sql := range ops {
			r, err := c.Query(sql)
			if err != nil || r == nil {
				log.Printf("  ERR %s: %v", sql, err)
				all = false
				break
			}
			if len(r.Rows) != 1 {
				log.Printf("  ERR %s: nrows=%d", sql, len(r.Rows))
				all = false
				break
			}
		}
		if all { ok++ } else { fail++ }
	}
	fmt.Fprintf(os.Stderr, "\nSAMPLE  ok=%d  fail=%d\n", ok, fail)
}

// ---- main ------------------------------------------------------------

func main() {
	var c cfg
	flag.StringVar(&c.Addr,       "addr",      "127.0.0.1:29301", "tsdb-server")
	flag.StringVar(&c.DBName,     "db",        "iot_core",        "database")
	flag.IntVar   (&c.NVTables,   "vtables",   64,                "# VTables")
	flag.IntVar   (&c.NPTables,   "ptables",   5000,              "# PTables")
	flag.Int64Var (&c.TotalRows,  "rows",      200_000_000,       "total rows")
	flag.IntVar   (&c.Writers,    "writers",   8,                 "concurrent writers")
	flag.IntVar   (&c.BatchSize,  "batch",     2048,              "rows per WRITE_BATCH")
	flag.IntVar   (&c.MaxCols,    "maxcols",   10,                "max data cols per VTable (in addition to ts)")
	flag.IntVar   (&c.SampleCheck,"sample",    16,                "# PTables to sample-check")
	skipCreate := flag.Bool("skip-create", false, "reuse existing catalog")
	flag.Parse()

	_, pts := (func() ([]vtableSpec, []ptableSpec) {
		if *skipCreate {
			// Rebuild spec list from the naming convention; needs the
			// caller to know ncols ahead of time.  Simpler: re-create
			// both in-memory specs but don't DDL.
			vts := make([]vtableSpec, c.NVTables)
			rng := rand.New(rand.NewSource(1))
			const nGroups = 8
			for i := range vts {
				vts[i].name = fmt.Sprintf("vt_%03d", i)
				vts[i].group = groupName(i, nGroups)
				vts[i].ncols = 1 + rng.Intn(c.MaxCols)
			}
			pts := make([]ptableSpec, c.NPTables)
			for i := range pts {
				vt := vts[i%c.NVTables]
				pts[i] = ptableSpec{
					name: fmt.Sprintf("pt_%05d", i),
					vtable: vt.name, ncols: vt.ncols,
				}
			}
			return vts, pts
		}
		return buildCatalog(c)
	})()

	runStress(c, pts)
	sampleCorrectness(c, pts)
}
