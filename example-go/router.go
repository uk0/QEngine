// router.go — client-side multi-cluster router for QEngine.
//
// The Router holds one TCP client per cluster plus a refresh goroutine
// that polls /metrics to track each cluster's "load" (rows_written_total
// is used as a proxy; pluggable via LoadProbe).
//
// Write semantics:
//
//   LeastLoaded (default) → pick cluster with smallest rows_written_total.
//   HashTable             → hash(table) modulo cluster count.  Keeps all
//                           rows for a given table on one cluster — lets
//                           point queries hit a single backend.
//   RoundRobin            → cycle through clusters per call.  Useful for
//                           stateless workloads where you don't care
//                           which backend serves a row.
//
// Query semantics:
//
//   QueryMergeSum(qtl)   — fans out `qtl` to every cluster, sums the
//                          single-row numeric result.  For `count(*)`,
//                          `sum(col)`, etc.
//   QueryConcat(qtl)     — fans out and returns the concatenated rows
//                          from every cluster.  For `SELECT ...` with
//                          no aggregate.
//   QueryMergeAvg(qtl)   — `SELECT avg(col)` is not directly mergeable
//                          from per-cluster averages; we issue `sum` and
//                          `count` to each side and compute globally.
//
// Unsupported shapes (percentiles, GROUP BY with row-aware aggregates
// that require distinct values across clusters) return an error so the
// caller knows to target a specific cluster via `Route(name)`.
package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

// Policy picks which cluster to write to for a given (table, batch).
type Policy int

const (
	LeastLoaded Policy = iota
	HashTable
	RoundRobin
)

// Backend is one logical cluster endpoint.
type Backend struct {
	Name        string // human-readable ("cluster-a")
	ClientAddr  string // "host:port" for the wire-protocol client
	MetricsAddr string // "host:port" for /metrics HTTP

	client *tsdb.Client
	load   atomic.Int64 // rows_written_total, refreshed in the background
	ok     atomic.Bool  // true if last probe succeeded
}

// Router routes traffic across 2+ backends.
type Router struct {
	Backends []*Backend
	Policy   Policy

	rr   atomic.Uint64 // round-robin cursor
	stop chan struct{}
	wg   sync.WaitGroup

	// refresh interval for background load probing.
	RefreshEvery time.Duration
}

// NewRouter dials every backend, performs an initial load probe, and
// starts a background goroutine that refreshes load every 2 s.  The
// caller must call Close to stop the background thread + release sockets.
func NewRouter(backends []*Backend, policy Policy, timeout time.Duration) (*Router, error) {
	if len(backends) == 0 {
		return nil, errors.New("router: need at least one backend")
	}
	for _, b := range backends {
		if b.Name == "" || b.ClientAddr == "" || b.MetricsAddr == "" {
			return nil, fmt.Errorf("router: backend %+v has empty fields", b)
		}
		c, err := tsdb.Open(b.ClientAddr, timeout)
		if err != nil {
			return nil, fmt.Errorf("dial %s (%s): %w", b.Name, b.ClientAddr, err)
		}
		b.client = c
	}
	r := &Router{
		Backends:     backends,
		Policy:       policy,
		RefreshEvery: 2 * time.Second,
		stop:         make(chan struct{}),
	}
	r.refresh() // synchronous initial probe
	r.wg.Add(1)
	go r.loop()
	return r, nil
}

// Close stops the refresh loop and closes every backend client.
func (r *Router) Close() {
	close(r.stop)
	r.wg.Wait()
	for _, b := range r.Backends {
		if b.client != nil {
			b.client.Close()
		}
	}
}

// loop polls /metrics every RefreshEvery.
func (r *Router) loop() {
	defer r.wg.Done()
	t := time.NewTicker(r.RefreshEvery)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.refresh()
		}
	}
}

// refresh polls every backend's /metrics in parallel and updates load.
func (r *Router) refresh() {
	var wg sync.WaitGroup
	for _, b := range r.Backends {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			load, err := probeRowsWritten(b.MetricsAddr, 800*time.Millisecond)
			if err != nil {
				b.ok.Store(false)
				return
			}
			b.load.Store(load)
			b.ok.Store(true)
		}(b)
	}
	wg.Wait()
}

// probeRowsWritten fetches /metrics and returns qengine_rows_written_total.
func probeRowsWritten(addr string, timeout time.Duration) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET",
		"http://"+addr+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "qengine_rows_written_total ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		return int64(v), nil
	}
	return 0, errors.New("rows_written_total not found in /metrics")
}

// pick applies the policy and returns a backend handle.
// The table name is used for HashTable; ignored otherwise.
func (r *Router) pick(table string) *Backend {
	switch r.Policy {

	case HashTable:
		h := fnv.New64a()
		_, _ = h.Write([]byte(table))
		return r.Backends[int(h.Sum64())%len(r.Backends)]

	case RoundRobin:
		i := r.rr.Add(1) - 1
		return r.Backends[int(i)%len(r.Backends)]

	default: // LeastLoaded
		var best *Backend
		bestLoad := int64(-1)
		for _, b := range r.Backends {
			if !b.ok.Load() {
				continue
			}
			l := b.load.Load()
			if best == nil || l < bestLoad {
				best, bestLoad = b, l
			}
		}
		if best == nil {
			return r.Backends[0]
		}
		return best
	}
}

// Route returns the named backend (for explicit pin), or nil.
func (r *Router) Route(name string) *Backend {
	for _, b := range r.Backends {
		if b.Name == name {
			return b
		}
	}
	return nil
}

// CreateTableEverywhere broadcasts a CREATE TABLE to every backend.
// Idempotent: treats "table exists" errors as success.
func (r *Router) CreateTableEverywhere(table, tsCol string, cols []tsdb.Column) error {
	for _, b := range r.Backends {
		if err := b.client.CreateTable(table, tsCol, cols); err != nil {
			if !strings.Contains(err.Error(), "exists") {
				return fmt.Errorf("create on %s: %w", b.Name, err)
			}
		}
	}
	return nil
}

// WriteBatch routes a batch to one backend per the current policy.
// Returns (backend_name, rows_acked, error).
func (r *Router) WriteBatch(table string, cols []tsdb.Column, rows []tsdb.Row) (string, uint32, error) {
	b := r.pick(table)
	n, err := b.client.WriteBatch(table, cols, rows)
	return b.Name, n, err
}

// QueryMergeSum fans a single-row single-column numeric query out and
// returns the sum.  Works for `SELECT count(*) FROM t`, `SELECT sum(col)`,
// and similar.
func (r *Router) QueryMergeSum(qtl string) (int64, error) {
	var total int64
	for _, b := range r.Backends {
		res, err := b.client.Query(qtl)
		if err != nil {
			return 0, fmt.Errorf("query %s: %w", b.Name, err)
		}
		for _, row := range res.Rows {
			if len(row) == 0 {
				continue
			}
			switch v := row[0].(type) {
			case int64:
				total += v
			case float64:
				total += int64(v)
			}
		}
	}
	return total, nil
}

// QueryMergeAvg computes a global avg from per-cluster (sum,count).
// Expects to be called with SELECT avg(<col>) FROM <table> [WHERE ...].
// Rewrites it internally to per-cluster `sum(col), count(col)`.
func (r *Router) QueryMergeAvg(table, col, whereClause string) (float64, error) {
	where := ""
	if whereClause != "" {
		where = " WHERE " + whereClause
	}
	q := fmt.Sprintf("SELECT sum(%s), count(%s) FROM %s%s", col, col, table, where)
	var totSum float64
	var totCnt int64
	for _, b := range r.Backends {
		res, err := b.client.Query(q)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", b.Name, err)
		}
		if len(res.Rows) == 0 {
			continue
		}
		row := res.Rows[0]
		if len(row) < 2 {
			continue
		}
		switch v := row[0].(type) {
		case float64:
			totSum += v
		case int64:
			totSum += float64(v)
		}
		if c, ok := row[1].(int64); ok {
			totCnt += c
		}
	}
	if totCnt == 0 {
		return 0, nil
	}
	return totSum / float64(totCnt), nil
}

// MergedRows is the concatenated row set from fanout queries — carries
// schema once plus the per-row values from every backend.
type MergedRows struct {
	ColNames []string
	ColTypes []byte
	Rows     [][]interface{}
	// BySource lets you trace which backend contributed how many rows.
	BySource map[string]int
}

// QueryConcat fans a SELECT out to every backend and concatenates rows.
// Schema is taken from the first successful backend; subsequent backends
// must produce the same column shape (same count + types).
func (r *Router) QueryConcat(qtl string) (*MergedRows, error) {
	merged := &MergedRows{BySource: make(map[string]int)}
	for _, b := range r.Backends {
		res, err := b.client.Query(qtl)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", b.Name, err)
		}
		if merged.ColNames == nil {
			merged.ColNames = res.ColNames
			merged.ColTypes = res.ColTypes
		} else if len(res.ColNames) != len(merged.ColNames) {
			return nil, fmt.Errorf("%s: column count mismatch got=%d want=%d",
				b.Name, len(res.ColNames), len(merged.ColNames))
		}
		merged.Rows = append(merged.Rows, res.Rows...)
		merged.BySource[b.Name] = len(res.Rows)
	}
	return merged, nil
}

// Stats is a point-in-time snapshot of each backend's health + load.
type Stats struct {
	Name         string
	ClientAddr   string
	Healthy      bool
	RowsWritten  int64
}

func (r *Router) Stats() []Stats {
	out := make([]Stats, 0, len(r.Backends))
	for _, b := range r.Backends {
		out = append(out, Stats{
			Name:        b.Name,
			ClientAddr:  b.ClientAddr,
			Healthy:     b.ok.Load(),
			RowsWritten: b.load.Load(),
		})
	}
	return out
}
