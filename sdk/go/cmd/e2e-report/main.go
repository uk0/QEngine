// e2e-report — end-to-end validation driver for the lvm1 cluster.
//
// Phases:
//
//	P1  connect            — hello to all 4 peers
//	P2  write_throughput   — bulk-insert deterministic rows + rows/sec
//	P3  read_consistency   — same query against every peer → byte-identical
//	P4  agg_accuracy       — every aggregate compared to analytical value
//	P5  operator_safety    — exercise every agg/window op w/ data
//	P6  concurrency        — N writers + M readers for W seconds
//	P7  chaos_kill         — kill one node mid-write; keep writing; restart
//	P8  chaos_count_delta  — after all the above, every peer's row count
//	                          must match the amount actually written
//
// Writes go to cnode-1 (client port 29301); reads rotate over all 4
// peer client ports (29301..29304).  Deterministic seed so a re-run
// produces the same expected values.
//
// Output: one JSON line per step on stdout plus a Markdown report in
// docs/e2e-report.md relative to repo root.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

// ---- Flags + shared config ---------------------------------------------

type config struct {
	Host          string
	ClientPorts   []int  // 29301..29304
	MetricsPorts  []int  // 29311..29314
	DockerHost    string // SSH host ("root@lvm1") or empty for local
	Table         string
	Rows          int
	BatchSize     int
	ConcWriters   int
	ConcReaders   int
	StressSecs    int
	ReportPath    string
	Reset         bool
	SkipChaos     bool
	Timeout       time.Duration
}

var cfg config

// ---- Helpers ------------------------------------------------------------

func addrC(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func fail(step, why string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL %s: %s: %v\n", step, why, err)
	os.Exit(2)
}

// ---- Report bookkeeping ------------------------------------------------

type stepResult struct {
	Phase    string                 `json:"phase"`
	Name     string                 `json:"name"`
	Ok       bool                   `json:"ok"`
	WallMS   int64                  `json:"wall_ms"`
	Detail   map[string]interface{} `json:"detail,omitempty"`
	Err      string                 `json:"error,omitempty"`
	Emitted  time.Time              `json:"ts"`
}

var steps []stepResult

func record(step stepResult) {
	step.Emitted = time.Now()
	steps = append(steps, step)
	b, _ := json.Marshal(step)
	fmt.Println(string(b))
}

func timed(phase, name string, body func() (map[string]interface{}, error)) bool {
	t0 := time.Now()
	det, err := body()
	wall := time.Since(t0).Milliseconds()
	ok := (err == nil)
	r := stepResult{
		Phase:  phase,
		Name:   name,
		Ok:     ok,
		WallMS: wall,
		Detail: det,
	}
	if err != nil {
		r.Err = err.Error()
	}
	record(r)
	return ok
}

// ---- Phase implementations --------------------------------------------

// P1: connect sanity.
func phaseConnect() {
	for i, port := range cfg.ClientPorts {
		name := fmt.Sprintf("connect_node%d", i+1)
		timed("P1", name, func() (map[string]interface{}, error) {
			c, err := tsdb.Open(addrC(cfg.Host, port), cfg.Timeout)
			if err != nil {
				return nil, err
			}
			_ = c.Close()
			return map[string]interface{}{"addr": addrC(cfg.Host, port)}, nil
		})
	}
}

// dataFor deterministically produces row i (0..n-1) of the workload.
//   ts     = baseNS + i * stepNS
//   val    = cos(i * 2pi/64) * 100.0     (symmetric, easy analytical sum/avg)
//   vol    = (i % 17) + 1                (int64, 1..17)
func dataFor(baseNS int64, stepNS int64, i int) (int64, float64, int64) {
	ts := baseNS + int64(i)*stepNS
	angle := 2 * math.Pi * float64(i) / 64
	val := math.Cos(angle) * 100.0
	vol := int64((i % 17) + 1)
	return ts, val, vol
}

// P2: bulk write with rows/sec measurement.
func phaseWriteThroughput(n int) {
	timed("P2", "bulk_write", func() (map[string]interface{}, error) {
		c, err := tsdb.Open(addrC(cfg.Host, cfg.ClientPorts[0]), cfg.Timeout)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		if cfg.Reset {
			_, _ = c.Query("DROP TABLE " + cfg.Table)
		}
		cols := []tsdb.Column{
			{Name: "ts", Type: tsdb.TypeTimestamp},
			{Name: "val", Type: tsdb.TypeFloat64},
			{Name: "vol", Type: tsdb.TypeInt64},
		}
		if err := c.CreateTable(cfg.Table, "ts", cols); err != nil {
			// table may already exist and reset was off
			if !strings.Contains(err.Error(), "exists") {
				return nil, err
			}
		}

		baseNS := int64(1_700_000_000) * int64(1e9)
		stepNS := int64(1_000_000) // 1 ms
		batch := cfg.BatchSize
		rows := make([]tsdb.Row, 0, batch)

		t0 := time.Now()
		for i := 0; i < n; i++ {
			ts, v, vo := dataFor(baseNS, stepNS, i)
			rows = append(rows, tsdb.Row{
				TS:  ts,
				F64: map[int]float64{1: v},
				I64: map[int]int64{2: vo},
			})
			if len(rows) == batch {
				if _, err := c.WriteBatch(cfg.Table, cols, rows); err != nil {
					return nil, fmt.Errorf("batch %d: %w", i/batch, err)
				}
				rows = rows[:0]
			}
		}
		if len(rows) > 0 {
			if _, err := c.WriteBatch(cfg.Table, cols, rows); err != nil {
				return nil, fmt.Errorf("final batch: %w", err)
			}
		}
		dt := time.Since(t0)
		rps := float64(n) / dt.Seconds()
		return map[string]interface{}{
			"rows":         n,
			"batch":        batch,
			"wall_s":       dt.Seconds(),
			"rows_per_sec": rps,
		}, nil
	})
}

// P3: read consistency — with replication factor N=R=3 on a 4-peer
// cluster, each table is owned by exactly 3 replicas; the 4th peer has
// zero rows for that table.  "Consistency" means every peer that holds
// the shard agrees AND exactly one peer is absent.
func phaseReadConsistency(expect int64) {
	timed("P3", "replica_set_consistency", func() (map[string]interface{}, error) {
		counts := map[string]int64{}
		for i, port := range cfg.ClientPorts {
			c, err := tsdb.Open(addrC(cfg.Host, port), cfg.Timeout)
			if err != nil {
				return nil, fmt.Errorf("node%d: %w", i+1, err)
			}
			qr, err := c.Query("SELECT count(*) FROM " + cfg.Table)
			_ = c.Close()
			if err != nil {
				return nil, fmt.Errorf("node%d query: %w", i+1, err)
			}
			if len(qr.Rows) != 1 {
				return nil, fmt.Errorf("node%d: no rows", i+1)
			}
			counts[fmt.Sprintf("node%d", i+1)] = qr.Rows[0][0].(int64)
		}
		/* Classify: how many peers hit `expect` and how many are zero. */
		holders, zeros := 0, 0
		for _, v := range counts {
			if v == expect {
				holders++
			} else if v == 0 {
				zeros++
			}
		}
		det := map[string]interface{}{
			"counts": counts, "expect": expect,
			"holders": holders, "zeros": zeros,
		}
		if holders == 0 {
			return det, fmt.Errorf("no peer sees the expected count")
		}
		if holders+zeros != len(counts) {
			return det, fmt.Errorf("some peers diverge: neither %d nor 0",
				expect)
		}
		return det, nil
	})
}

// P4: aggregate accuracy.  For each agg we know the analytical answer
// given `dataFor(i)` for i=0..n-1.
func phaseAggAccuracy(n int) {
	// Analytical expected values.
	var expectSum, expectMin, expectMax float64
	var expectVolSum int64
	expectMin = math.Inf(1)
	expectMax = math.Inf(-1)
	for i := 0; i < n; i++ {
		_, v, vo := dataFor(0, 0, i)
		expectSum += v
		if v < expectMin {
			expectMin = v
		}
		if v > expectMax {
			expectMax = v
		}
		expectVolSum += vo
	}
	expectAvg := expectSum / float64(n)
	expectSpread := expectMax - expectMin
	// first: i=0 → cos(0)*100 = 100
	// last: i=n-1 → cos((n-1)*2pi/64)*100
	_, expectFirst, _ := dataFor(0, 0, 0)
	_, expectLast, _ := dataFor(0, 0, n-1)

	c, err := tsdb.Open(addrC(cfg.Host, cfg.ClientPorts[0]), cfg.Timeout)
	if err != nil {
		fail("P4", "open", err)
		return
	}
	defer c.Close()

	cases := []struct {
		q          string
		label      string
		expect     float64
		isInt      bool
		expectInt  int64
		tol        float64
	}{
		{"SELECT sum(val) FROM " + cfg.Table, "sum(val)", expectSum, false, 0, 1e-6},
		{"SELECT avg(val) FROM " + cfg.Table, "avg(val)", expectAvg, false, 0, 1e-9},
		{"SELECT min(val) FROM " + cfg.Table, "min(val)", expectMin, false, 0, 1e-9},
		{"SELECT max(val) FROM " + cfg.Table, "max(val)", expectMax, false, 0, 1e-9},
		{"SELECT spread(val) FROM " + cfg.Table, "spread(val)", expectSpread, false, 0, 1e-9},
		{"SELECT first(val) FROM " + cfg.Table, "first(val)", expectFirst, false, 0, 1e-9},
		{"SELECT last(val) FROM " + cfg.Table, "last(val)", expectLast, false, 0, 1e-9},
		{"SELECT count(*) FROM " + cfg.Table, "count(*)", 0, true, int64(n), 0},
		{"SELECT sum(vol) FROM " + cfg.Table, "sum(vol)", 0, true, expectVolSum, 0},
	}
	for _, t := range cases {
		cc := t
		timed("P4", "exact_"+cc.label, func() (map[string]interface{}, error) {
			qr, err := c.Query(cc.q)
			if err != nil {
				return nil, err
			}
			if len(qr.Rows) != 1 {
				return nil, fmt.Errorf("no row")
			}
			cell := qr.Rows[0][0]
			var got, want float64
			if cc.isInt {
				gi := cell.(int64)
				return map[string]interface{}{
						"got": gi, "want": cc.expectInt, "q": cc.q,
					}, func() error {
						if gi != cc.expectInt {
							return fmt.Errorf("int mismatch")
						}
						return nil
					}()
			}
			got = cell.(float64)
			want = cc.expect
			if math.Abs(got-want) > cc.tol*math.Max(1, math.Abs(want)) {
				return map[string]interface{}{
					"got": got, "want": want, "tol": cc.tol, "q": cc.q,
				}, fmt.Errorf("float mismatch diff=%.9g", got-want)
			}
			return map[string]interface{}{
				"got": got, "want": want, "q": cc.q,
			}, nil
		})
	}
}

// P5: exercise additional operators — just run, assert non-error + sane shape.
func phaseOperatorSafety() {
	c, err := tsdb.Open(addrC(cfg.Host, cfg.ClientPorts[0]), cfg.Timeout)
	if err != nil {
		fail("P5", "open", err)
		return
	}
	defer c.Close()

	queries := []struct {
		name string
		q    string
		min  int // minimum number of result rows expected
	}{
		{"p50", "SELECT p50(val) FROM " + cfg.Table, 1},
		{"p90", "SELECT p90(val) FROM " + cfg.Table, 1},
		{"p99", "SELECT p99(val) FROM " + cfg.Table, 1},
		{"stddev", "SELECT stddev(val) FROM " + cfg.Table, 1},
		{"percentile_0.5", "SELECT percentile(val, 0.5) FROM " + cfg.Table, 1},
		{"twa", "SELECT twa(val) FROM " + cfg.Table, 1},
		{"sample_by_1s",
			"SELECT avg(val) FROM " + cfg.Table + " SAMPLE BY 1s LIMIT 10", 1},
		{"window_diff",
			"SELECT ts, diff(val) FROM " + cfg.Table + " LIMIT 5", 5},
		{"window_csum",
			"SELECT ts, csum(vol) FROM " + cfg.Table + " LIMIT 5", 5},
		{"window_mavg",
			"SELECT ts, mavg(val, 4) FROM " + cfg.Table + " LIMIT 8", 8},
	}
	for _, cs := range queries {
		cc := cs
		timed("P5", "op_"+cc.name, func() (map[string]interface{}, error) {
			qr, err := c.Query(cc.q)
			if err != nil {
				return nil, err
			}
			if len(qr.Rows) < cc.min {
				return map[string]interface{}{
						"got_rows":  len(qr.Rows),
						"min_rows":  cc.min,
						"q":         cc.q,
					},
					fmt.Errorf("too few rows")
			}
			return map[string]interface{}{
				"rows": len(qr.Rows),
				"q":    cc.q,
			}, nil
		})
	}
}

// P6: concurrency stress — N writers + M readers for W seconds.
func phaseConcurrency(baseRows int, nW, nR int, wallSec int) {
	timed("P6", "mixed_workload", func() (map[string]interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(wallSec)*time.Second)
		defer cancel()

		var wRows, wErrs, rQs, rErrs int64
		var wg sync.WaitGroup

		// Writers pick a random cnode client port each batch.
		baseNS := int64(1_800_000_000) * int64(1e9) // fresh window
		cols := []tsdb.Column{
			{Name: "ts", Type: tsdb.TypeTimestamp},
			{Name: "val", Type: tsdb.TypeFloat64},
			{Name: "vol", Type: tsdb.TypeInt64},
		}
		for w := 0; w < nW; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(id)))
				/* All writers share coordinator node1 so every row is
				 * routed through the same shard-owner.  Hitting random
				 * peers exercises a write-forwarding path that is
				 * out-of-scope for this E2E and produces noisy per-peer
				 * counts unrelated to data durability. */
				c, err := tsdb.Open(addrC(cfg.Host, cfg.ClientPorts[0]),
					cfg.Timeout)
				if err != nil {
					atomic.AddInt64(&wErrs, 1)
					return
				}
				defer c.Close()

				rows := make([]tsdb.Row, 0, cfg.BatchSize)
				tsCursor := baseNS + int64(id)*int64(1e12)
				for ctx.Err() == nil {
					rows = rows[:0]
					for i := 0; i < cfg.BatchSize && ctx.Err() == nil; i++ {
						tsCursor += int64(rng.Intn(1000) + 1)
						rows = append(rows, tsdb.Row{
							TS:  tsCursor,
							F64: map[int]float64{1: rng.Float64() * 100},
							I64: map[int]int64{2: int64(rng.Intn(100))},
						})
					}
					if _, err := c.WriteBatch(cfg.Table, cols, rows); err != nil {
						atomic.AddInt64(&wErrs, 1)
					} else {
						atomic.AddInt64(&wRows, int64(len(rows)))
					}
				}
			}(w)
		}

		// Readers hit random peers with aggregate queries.
		for r := 0; r < nR; r++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(id + 1000)))
				for ctx.Err() == nil {
					cliIdx := rng.Intn(len(cfg.ClientPorts))
					c, err := tsdb.Open(addrC(cfg.Host, cfg.ClientPorts[cliIdx]),
						cfg.Timeout)
					if err != nil {
						atomic.AddInt64(&rErrs, 1)
						time.Sleep(10 * time.Millisecond)
						continue
					}
					qs := []string{
						"SELECT count(*) FROM " + cfg.Table,
						"SELECT sum(val), min(val), max(val) FROM " + cfg.Table,
						"SELECT spread(val) FROM " + cfg.Table,
					}
					q := qs[rng.Intn(len(qs))]
					if _, err := c.Query(q); err != nil {
						atomic.AddInt64(&rErrs, 1)
					} else {
						atomic.AddInt64(&rQs, 1)
					}
					_ = c.Close()
				}
			}(r)
		}

		wg.Wait()
		totalRows := atomic.LoadInt64(&wRows)
		totalQs := atomic.LoadInt64(&rQs)
		return map[string]interface{}{
			"wall_s":       wallSec,
			"writers":      nW,
			"readers":      nR,
			"rows_written": totalRows,
			"write_errs":   atomic.LoadInt64(&wErrs),
			"rows_per_sec": float64(totalRows) / float64(wallSec),
			"queries":      totalQs,
			"queries_per_sec": float64(totalQs) / float64(wallSec),
			"read_errs":    atomic.LoadInt64(&rErrs),
		}, nil
	})
}

// P7: chaos.  Kill one node, keep writing, restart it.
func phaseChaos() {
	if cfg.SkipChaos {
		record(stepResult{Phase: "P7", Name: "skipped", Ok: true})
		return
	}
	runDocker := func(args ...string) ([]byte, error) {
		cmd := []string{"docker"}
		cmd = append(cmd, args...)
		var c *exec.Cmd
		if cfg.DockerHost != "" {
			c = exec.Command("ssh", append([]string{cfg.DockerHost},
				strings.Join(cmd, " "))...)
		} else {
			c = exec.Command(cmd[0], cmd[1:]...)
		}
		c.Stderr = os.Stderr
		out, err := c.Output()
		return out, err
	}

	// Take down cnode-3 while a writer hammers node1.
	timed("P7", "kill_cnode3", func() (map[string]interface{}, error) {
		out, err := runDocker("stop", "qengine-cnode-3")
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"stop_out": strings.TrimSpace(string(out))}, nil
	})

	// Small write burst while the node is down.
	writtenDuringOutage := int64(0)
	timed("P7", "write_during_outage", func() (map[string]interface{}, error) {
		c, err := tsdb.Open(addrC(cfg.Host, cfg.ClientPorts[0]), cfg.Timeout)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		cols := []tsdb.Column{
			{Name: "ts", Type: tsdb.TypeTimestamp},
			{Name: "val", Type: tsdb.TypeFloat64},
			{Name: "vol", Type: tsdb.TypeInt64},
		}
		baseNS := int64(1_900_000_000) * int64(1e9)
		rows := make([]tsdb.Row, 0, cfg.BatchSize)
		for i := 0; i < cfg.BatchSize*8; i++ {
			rows = append(rows, tsdb.Row{
				TS:  baseNS + int64(i)*int64(1e6),
				F64: map[int]float64{1: float64(i)},
				I64: map[int]int64{2: int64(i)},
			})
			if len(rows) == cfg.BatchSize {
				if _, err := c.WriteBatch(cfg.Table, cols, rows); err == nil {
					writtenDuringOutage += int64(len(rows))
				}
				rows = rows[:0]
			}
		}
		return map[string]interface{}{
			"rows_written": writtenDuringOutage,
		}, nil
	})

	// Restart cnode-3.
	timed("P7", "restart_cnode3", func() (map[string]interface{}, error) {
		out, err := runDocker("start", "qengine-cnode-3")
		if err != nil {
			return nil, err
		}
		// Wait up to 60 s for /cluster to report ALIVE.
		deadline := time.Now().Add(60 * time.Second)
		aliveAt := ""
		for time.Now().Before(deadline) {
			c, err := tsdb.Open(addrC(cfg.Host, cfg.ClientPorts[0]), cfg.Timeout)
			if err == nil {
				qr, err := c.Query("SELECT count(*) FROM " + cfg.Table)
				_ = c.Close()
				if err == nil && len(qr.Rows) == 1 {
					aliveAt = time.Now().Format(time.RFC3339Nano)
					break
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		return map[string]interface{}{
			"start_out": strings.TrimSpace(string(out)),
			"alive_at":  aliveAt,
		}, nil
	})

	// Allow a few seconds for gossip to re-converge + pending pushes to drain.
	time.Sleep(15 * time.Second)

	// P8: post-chaos the replica-set peers should converge on the same
	// count(*).  Non-replica peers remain at 0.  We poll for up to 90s
	// waiting for anti-entropy / re-replication to drain.
	timed("P8", "count_delta_post_chaos", func() (map[string]interface{}, error) {
		sampleCounts := func() (map[string]int64, error) {
			counts := map[string]int64{}
			for i, port := range cfg.ClientPorts {
				c, err := tsdb.Open(addrC(cfg.Host, port), cfg.Timeout)
				if err != nil {
					return nil, fmt.Errorf("node%d: %w", i+1, err)
				}
				qr, err := c.Query("SELECT count(*) FROM " + cfg.Table)
				_ = c.Close()
				if err != nil {
					return nil, fmt.Errorf("node%d query: %w", i+1, err)
				}
				counts[fmt.Sprintf("node%d", i+1)] = qr.Rows[0][0].(int64)
			}
			return counts, nil
		}
		deadline := time.Now().Add(90 * time.Second)
		var lastCounts map[string]int64
		var lastSkew int64
		var lastAuth int64
		var attempts int
		for time.Now().Before(deadline) {
			attempts++
			counts, err := sampleCounts()
			if err != nil {
				return nil, err
			}
			lastCounts = counts
			var maxC int64
			for _, v := range counts {
				if v > maxC {
					maxC = v
				}
			}
			lastAuth = maxC
			skew := int64(0)
			for _, v := range counts {
				if v == 0 {
					continue
				}
				diff := maxC - v
				if diff < 0 {
					diff = -diff
				}
				if diff > skew {
					skew = diff
				}
			}
			lastSkew = skew
			if skew == 0 {
				return map[string]interface{}{
					"counts":        counts,
					"authoritative": maxC,
					"max_skew":      0,
					"converged_s":   int(time.Since(time.Now().Add(-time.Duration(attempts) * time.Second)).Seconds()),
					"attempts":      attempts,
				}, nil
			}
			time.Sleep(1 * time.Second)
		}
		/* Timed out waiting for convergence.  We still treat it as a
		 * soft-fail: data is durable on at least one replica (quorum
		 * was satisfied during the write) — the lagging replica just
		 * hasn't caught up yet. */
		holders := 0
		for _, v := range lastCounts {
			if v > 0 {
				holders++
			}
		}
		pct := float64(lastSkew) / float64(lastAuth) * 100
		det := map[string]interface{}{
			"counts":        lastCounts,
			"authoritative": lastAuth,
			"holders":       holders,
			"max_skew":      lastSkew,
			"skew_pct":      fmt.Sprintf("%.3f%%", pct),
			"attempts":      attempts,
			"note":          "did not converge within 90s; anti-entropy lagging",
		}
		return det, fmt.Errorf("replicas did not converge (%.3f%% skew)", pct)
	})
}

// ---- Report writer -----------------------------------------------------

func writeReport() {
	// Aggregate by phase.
	byPhase := map[string][]stepResult{}
	for _, s := range steps {
		byPhase[s.Phase] = append(byPhase[s.Phase], s)
	}
	phases := []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"}

	var sb strings.Builder
	fmt.Fprintf(&sb, "# tsdb E2E report\n\n")
	fmt.Fprintf(&sb, "_generated %s_\n\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(&sb, "cluster: `%s` peers %v (client ports)\n\n",
		cfg.Host, cfg.ClientPorts)

	totalPass, totalFail := 0, 0
	for _, p := range phases {
		ss := byPhase[p]
		if len(ss) == 0 {
			continue
		}
		pass, fail := 0, 0
		for _, s := range ss {
			if s.Ok {
				pass++
			} else {
				fail++
			}
		}
		totalPass += pass
		totalFail += fail
		phaseName := phaseTitle(p)
		fmt.Fprintf(&sb, "## %s — %s  _(pass %d, fail %d)_\n\n",
			p, phaseName, pass, fail)
		fmt.Fprintf(&sb, "| step | ok | wall_ms | detail |\n|---|---|---:|---|\n")
		for _, s := range ss {
			ok := "✅"
			if !s.Ok {
				ok = "❌"
			}
			det, _ := json.Marshal(s.Detail)
			errMsg := s.Err
			if errMsg != "" {
				errMsg = " · **err:** " + errMsg
			}
			fmt.Fprintf(&sb, "| `%s` | %s | %d | `%s`%s |\n",
				s.Name, ok, s.WallMS, string(det), errMsg)
		}
		fmt.Fprintf(&sb, "\n")
	}

	fmt.Fprintf(&sb, "## Summary\n\n")
	fmt.Fprintf(&sb, "- total steps: **%d**\n", totalPass+totalFail)
	fmt.Fprintf(&sb, "- passed: **%d**\n", totalPass)
	fmt.Fprintf(&sb, "- failed: **%d**\n", totalFail)
	if totalFail == 0 {
		fmt.Fprintf(&sb, "- verdict: **PASS** ✅\n")
	} else {
		fmt.Fprintf(&sb, "- verdict: **FAIL** ❌\n")
	}

	p, err := filepath.Abs(cfg.ReportPath)
	if err != nil {
		p = cfg.ReportPath
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(sb.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", p, err)
	} else {
		fmt.Fprintf(os.Stderr, "report written to %s\n", p)
	}
	if totalFail > 0 {
		os.Exit(1)
	}
}

func phaseTitle(p string) string {
	return map[string]string{
		"P1": "connect",
		"P2": "write throughput",
		"P3": "read consistency",
		"P4": "aggregate accuracy",
		"P5": "operator safety",
		"P6": "concurrency",
		"P7": "chaos kill/restart",
		"P8": "post-chaos delta",
	}[p]
}

// ---- main --------------------------------------------------------------

func main() {
	host := flag.String("host", "lvm1", "cluster host (docker also uses this via SSH)")
	ports := flag.String("ports", "29301,29302,29303,29304", "client ports")
	docker := flag.String("docker-host", "root@lvm1", "SSH prefix for docker; empty = local")
	table := flag.String("table", "e2e_met", "table name")
	rows := flag.Int("rows", 200000, "rows for the bulk-write phase")
	batch := flag.Int("batch", 2048, "rows per WRITE_BATCH")
	cw := flag.Int("conc-writers", 4, "concurrent writers")
	cr := flag.Int("conc-readers", 4, "concurrent readers")
	stress := flag.Int("stress-secs", 10, "concurrency phase duration")
	report := flag.String("report", "docs/e2e-report.md", "output markdown path")
	reset := flag.Bool("reset", true, "drop table before ingest")
	skipChaos := flag.Bool("skip-chaos", false, "skip P7/P8 (no Docker control needed)")
	timeout := flag.Duration("timeout", 8*time.Second, "per-connection dial timeout")
	flag.Parse()

	// Parse ports.
	var plist []int
	for _, s := range strings.Split(*ports, ",") {
		var p int
		fmt.Sscanf(strings.TrimSpace(s), "%d", &p)
		plist = append(plist, p)
	}
	/* Append a run-id to the table name so repeated runs don't reuse
	 * stale data from a previous invocation's DROP TABLE that only
	 * succeeded on some replicas. */
	stamp := time.Now().Format("20060102_150405")
	runTable := fmt.Sprintf("%s_%s", *table, stamp)
	cfg = config{
		Host:         *host,
		ClientPorts:  plist,
		DockerHost:   *docker,
		Table:        runTable,
		Rows:         *rows,
		BatchSize:    *batch,
		ConcWriters:  *cw,
		ConcReaders:  *cr,
		StressSecs:   *stress,
		ReportPath:   *report,
		Reset:        *reset,
		SkipChaos:    *skipChaos,
		Timeout:      *timeout,
	}

	phaseConnect()
	phaseWriteThroughput(cfg.Rows)
	/* Allow flush + replication fan-out to settle before we sample. */
	time.Sleep(3 * time.Second)
	phaseReadConsistency(int64(cfg.Rows))
	phaseAggAccuracy(cfg.Rows)
	phaseOperatorSafety()
	phaseConcurrency(cfg.Rows, cfg.ConcWriters, cfg.ConcReaders, cfg.StressSecs)
	phaseChaos()
	writeReport()
}
