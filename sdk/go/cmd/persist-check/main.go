// Persistence + operator-safety across container restart.
package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

func conn(addr string) *tsdb.Client {
	c, err := tsdb.Open(addr, 8*time.Second)
	if err != nil { log.Fatalf("open %s: %v", addr, err) }
	return c
}

func q(c *tsdb.Client, sql string) *tsdb.QueryResult {
	r, err := c.Query(sql)
	if err != nil { log.Fatalf("query [%s]: %v", sql, err) }
	return r
}

// 9 aggregates on the same deterministic dataset.
type probe struct { name, sql string; kind string; want float64; wantI int64 }

func probes(table string) []probe {
	// dataFor(i) = cos(2*pi*i/64)*100, vol=(i%17)+1
	// Closed-form over N rows:
	// sum is a partial cosine sum; with N=20000, N%64==0 so sum=0.
	// min=-100, max=100, spread=200
	// first = 100, last depends on N-1 mod 64
	// count=N, sum(vol) = sum_{i=0}^{N-1} ((i%17)+1)
	return []probe{
		{"最新值 last",  "SELECT last(val) FROM " + table,  "flt", 0, 0}, // we compute at runtime
		{"最老值 first", "SELECT first(val) FROM " + table, "flt", 100, 0},
		{"最大值 max",   "SELECT max(val) FROM " + table,   "flt", 100, 0},
		{"最小值 min",   "SELECT min(val) FROM " + table,   "flt", -100, 0},
		{"累加值 sum",   "SELECT sum(val) FROM " + table,   "flt", 0, 0}, // near-zero (N%64==0)
		{"累计值 count", "SELECT count(*) FROM " + table,   "int", 0, 0}, // checked dynamically
		{"平均值 mean",  "SELECT avg(val) FROM " + table,   "flt", 0, 0}, // near-zero
		{"极差值 spread","SELECT spread(val) FROM " + table,"flt", 200, 0},
		{"中间值 median","SELECT p50(val) FROM " + table,   "flt", 0, 0}, // near-zero
	}
}

func main() {
	host := "localhost"
	ports := []int{29301, 29302, 29303, 29304}
	stamp := time.Now().Format("150405")
	table := "persist_met_" + stamp

	log.Printf("== Phase 1: create table + insert 20000 rows ==")
	c := conn(fmt.Sprintf("%s:%d", host, ports[0]))
	_, _ = c.Query("DROP TABLE " + table)
	if err := c.CreateTable(table, "ts",
		[]tsdb.Column{
			{"ts",  tsdb.TypeTimestamp},
			{"val", tsdb.TypeFloat64},
			{"vol", tsdb.TypeInt64},
		}); err != nil {
		log.Fatalf("create table: %v", err)
	}
	const N = 20000
	base := int64(1_700_000_000) * int64(1e9)
	rows := make([]tsdb.Row, 0, 2048)
	cols := []tsdb.Column{
		{"ts",  tsdb.TypeTimestamp},
		{"val", tsdb.TypeFloat64},
		{"vol", tsdb.TypeInt64},
	}
	for i := 0; i < N; i++ {
		rows = append(rows, tsdb.Row{
			TS:  base + int64(i)*int64(1e6),
			F64: map[int]float64{1: math.Cos(2 * math.Pi * float64(i) / 64) * 100},
			I64: map[int]int64{2: int64((i % 17) + 1)},
		})
		if len(rows) == 2048 {
			if _, err := c.WriteBatch(table, cols, rows); err != nil {
				log.Fatalf("batch: %v", err)
			}
			rows = rows[:0]
		}
	}
	if len(rows) > 0 { c.WriteBatch(table, cols, rows) }
	c.Close()

	check := func(label string) {
		log.Printf("== %s ==", label)
		cc := conn(fmt.Sprintf("%s:%d", host, ports[0]))
		defer cc.Close()

		// Compute expected values from the same deterministic formula so
		// the checks don't depend on N being a multiple of 64.
		var expSum float64
		var expLast float64
		var expVolSum int64
		for i := 0; i < N; i++ {
			v := math.Cos(2*math.Pi*float64(i)/64) * 100
			expSum += v
			expLast = v
			expVolSum += int64((i%17)+1)
		}
		expAvg := expSum / float64(N)

		runs := []struct{ name, sql string; isInt bool; expF float64; expI int64; tol float64 }{
			{"最新值 last(val)",   "SELECT last(val) FROM " + table,   false, expLast, 0, 1e-9},
			{"最老值 first(val)",  "SELECT first(val) FROM " + table,  false, 100, 0, 1e-9},
			{"最大值 max(val)",    "SELECT max(val) FROM " + table,    false, 100, 0, 1e-9},
			{"最小值 min(val)",    "SELECT min(val) FROM " + table,    false, -100, 0, 1e-9},
			{"累加值 sum(val)",    "SELECT sum(val) FROM " + table,    false, expSum, 0, 1e-6},
			{"累计值 count(*)",    "SELECT count(*) FROM " + table,    true,  0, N, 0},
			{"平均值 avg(val)",    "SELECT avg(val) FROM " + table,    false, expAvg, 0, 1e-12},
			{"极差值 spread(val)", "SELECT spread(val) FROM " + table, false, 200, 0, 1e-9},
			{"中间值 p50(val)",    "SELECT p50(val) FROM " + table,    false, 0, 0, 5.0},
			{"sum(vol) int",       "SELECT sum(vol) FROM " + table,    true,  0, expVolSum, 0},
		}
		pass, fail := 0, 0
		for _, r := range runs {
			qr := q(cc, r.sql)
			if len(qr.Rows) != 1 { log.Fatalf("%s: no row", r.name) }
			ok := false
			var got interface{}
			if r.isInt {
				gi := qr.Rows[0][0].(int64)
				got = gi
				ok = gi == r.expI
			} else {
				gf := qr.Rows[0][0].(float64)
				got = gf
				ok = math.Abs(gf-r.expF) <= r.tol*math.Max(1, math.Abs(r.expF)) || math.Abs(gf-r.expF) <= r.tol
			}
			if ok { pass++; fmt.Printf("  ✅ %-25s got=%v  want≈%v\n", r.name, got, pickExp(r)) } else { fail++; fmt.Printf("  ❌ %-25s got=%v  want≈%v\n", r.name, got, pickExp(r)) }
		}
		log.Printf("-> %s: %d passed, %d failed", label, pass, fail)
		if fail > 0 { os.Exit(2) }
	}
	check("Phase 2: queries BEFORE restart")

	log.Printf("== Phase 3: restart ALL 4 containers ==")
	cmd := exec.Command("bash", "-lc",
		"for c in qengine-cnode-1 qengine-cnode-2 qengine-cnode-3 qengine-cnode-4; do "+
			"docker restart $c > /dev/null; done")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("docker restart: %v", err)
	}
	log.Printf("   waiting 15s for cluster to form...")
	time.Sleep(15 * time.Second)

	check("Phase 4: queries AFTER restart — data still present + operators correct")
	fmt.Println("\n=== PERSISTENCE + OPERATORS: PASS ===")
}

func pickExp(r struct{ name, sql string; isInt bool; expF float64; expI int64; tol float64 }) interface{} {
	if r.isInt { return r.expI }
	return r.expF
}

var _ = strings.TrimSpace
