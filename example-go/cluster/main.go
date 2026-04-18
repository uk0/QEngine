// Cluster write + resilience demo.
//
// Workflow:
//
//   1. Connect to every cluster node listed in -nodes.
//   2. CREATE TABLE on the primary (-primary).  DDL propagates via the
//      cluster's schema hook so every node has the same table.
//   3. Ingest -rows rows in -batch-sized batches through the primary.
//      Every batch measures wall time; the primary flushes via rawblock
//      replication so the data lands on replica_factor peers.
//   4. Every 2 s (configurable) we poll each node's count(*) for the
//      table and print a per-node snapshot.  This is the column that
//      answers "did the writes replicate?" live as the ingest runs.
//   5. Simulate node failure: if -kill-at <N> is set, the program
//      invokes `docker stop <container>` on -kill-node after the Nth
//      batch.  The remaining batches observe the impact (latency spike
//      or partial failure).
//   6. Simulate node add: if -add-at <N> is set, invokes
//      `docker compose -f -add-compose up -d` after the Nth batch.
//      Useful with deployment/docker-compose.addnode.yml.
//   7. Final check: count from each node and report the spread.
//
// The docker commands run locally (or via SSH if you set -ssh); replace
// them with your orchestration wiring as needed.
//
// Example:
//   go run . \
//     -nodes lvm1:29301,lvm1:29302,lvm1:29303,lvm1:29304 \
//     -primary lvm1:29301 \
//     -rows 500000 -batch 4096 \
//     -kill-at 20 -kill-node qengine-cnode-2 -ssh root@lvm1
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tsdb "github.com/qengine/tsdb-go"
)

func main() {
	nodesFlag := flag.String("nodes", "lvm1:29301,lvm1:29302,lvm1:29303,lvm1:29304",
		"comma-separated cluster node client addresses")
	primary := flag.String("primary", "lvm1:29301",
		"which node to write to (reads still fan across all)")
	rows := flag.Int("rows", 500_000, "total rows to write")
	batch := flag.Int("batch", 4096, "rows per WRITE_BATCH")
	pollEvery := flag.Duration("poll", 2*time.Second, "per-node count(*) poll interval")
	table := flag.String("table", "cluster_demo", "target table name")
	killAt := flag.Int("kill-at", 0,
		"if > 0, run docker stop on -kill-node after the Nth batch")
	killNode := flag.String("kill-node", "", "container name to docker stop")
	addAt := flag.Int("add-at", 0,
		"if > 0, run docker compose -f <-add-compose> up -d after the Nth batch")
	addCompose := flag.String("add-compose",
		"/home/nas/tsdb/src/deployment/docker-compose.addnode.yml",
		"compose file to bring up when -add-at fires")
	sshHost := flag.String("ssh", "",
		"run docker commands over ssh <target>; empty = local")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	addrs := strings.Split(*nodesFlag, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	fmt.Println("=== qengine cluster demo ===")
	fmt.Printf("nodes:   %v\n", addrs)
	fmt.Printf("primary: %s\n", *primary)
	fmt.Printf("rows:    %d  batch=%d\n\n", *rows, *batch)

	// ---- Connect to every node -----------------------------------------
	clients := make(map[string]*tsdb.Client, len(addrs))
	for _, addr := range addrs {
		c, err := tsdb.Open(addr, 5*time.Second)
		if err != nil {
			log.Fatalf("connect %s: %v", addr, err)
		}
		clients[addr] = c
		defer c.Close()
	}

	// ---- DDL on the primary --------------------------------------------
	primC := clients[*primary]
	if primC == nil {
		log.Fatalf("-primary %s is not in -nodes list", *primary)
	}
	_, _ = primC.Query("DROP TABLE " + *table)
	cols := []tsdb.Column{
		{Name: "ts", Type: tsdb.TypeTimestamp},
		{Name: "price", Type: tsdb.TypeFloat64},
		{Name: "qty", Type: tsdb.TypeInt64},
	}
	if err := primC.CreateTable(*table, "ts", cols); err != nil {
		log.Fatalf("CREATE TABLE: %v", err)
	}
	fmt.Printf("created %s on primary; waiting 2s for schema replication…\n", *table)
	time.Sleep(2 * time.Second)

	// ---- Start the poller (reads count(*) from each node continuously) --
	pollerCtx, stopPoller := context.WithCancel(context.Background())
	defer stopPoller()
	var pollSeen atomic.Int64
	go func() {
		t := time.NewTicker(*pollEvery)
		defer t.Stop()
		for {
			select {
			case <-pollerCtx.Done():
				return
			case <-t.C:
				fmt.Printf("  [poll] ")
				for _, addr := range addrs {
					c := clients[addr]
					if c == nil {
						continue
					}
					count := queryCount(c, *table)
					fmt.Printf("%s=%d  ", shortAddr(addr), count)
				}
				fmt.Println()
				pollSeen.Add(1)
			}
		}
	}()

	// ---- Ingest loop ---------------------------------------------------
	rng := rand.New(rand.NewSource(42))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

	buf := make([]tsdb.Row, 0, *batch)
	batchNum := 0
	batchFailed := 0
	t0 := time.Now()
	var lastLatency time.Duration

	for i := 0; i < *rows; i++ {
		buf = append(buf, tsdb.Row{
			TS:  base + int64(i)*1_000_000,
			F64: map[int]float64{1: 100.0 + rng.Float64()*20.0},
			I64: map[int]int64{2: int64(rng.Intn(1000) + 1)},
		})
		if len(buf) >= *batch {
			batchNum++
			bt := time.Now()
			_, err := primC.WriteBatch(*table, cols, buf)
			lastLatency = time.Since(bt)
			if err != nil {
				batchFailed++
				log.Printf("  BATCH #%d FAILED (%v): %v — retrying once after 1s",
					batchNum, lastLatency, err)
				time.Sleep(1 * time.Second)
				if _, err := primC.WriteBatch(*table, cols, buf); err != nil {
					log.Printf("  BATCH #%d retry FAILED: %v", batchNum, err)
				} else {
					log.Printf("  BATCH #%d retry OK", batchNum)
				}
			}
			if lastLatency > 100*time.Millisecond {
				log.Printf("  BATCH #%d slow: %v", batchNum, lastLatency)
			}
			buf = buf[:0]

			if *killAt > 0 && batchNum == *killAt && *killNode != "" {
				fmt.Printf("\n>>> killing %s (at batch #%d) <<<\n\n",
					*killNode, batchNum)
				go runDocker(*sshHost, "stop", *killNode)
			}
			if *addAt > 0 && batchNum == *addAt {
				fmt.Printf("\n>>> bringing up addnode compose (at batch #%d) <<<\n\n",
					batchNum)
				go runComposeUp(*sshHost, *addCompose)
			}
		}
	}
	if len(buf) > 0 {
		if _, err := primC.WriteBatch(*table, cols, buf); err != nil {
			log.Printf("tail batch failed: %v", err)
			batchFailed++
		}
	}

	elapsed := time.Since(t0).Seconds()
	fmt.Printf("\n=== ingest done: %d rows in %.2fs (%.0f rows/s)  failed_batches=%d ===\n",
		*rows, elapsed, float64(*rows)/elapsed, batchFailed)

	// Give replication a moment to settle before the final count sweep.
	time.Sleep(3 * time.Second)
	stopPoller()
	// Final consistency check.
	fmt.Println("\n=== final counts ===")
	results := make(map[string]int64)
	for _, addr := range addrs {
		c := clients[addr]
		if c == nil {
			continue
		}
		cnt := queryCount(c, *table)
		results[addr] = cnt
		fmt.Printf("  %-20s  count=%d\n", addr, cnt)
	}
	var seen int64 = -1
	consistent := true
	for _, v := range results {
		if seen == -1 {
			seen = v
		} else if v != seen {
			consistent = false
		}
	}
	if consistent {
		fmt.Printf("\n✓ all surviving nodes agree on count = %d\n", seen)
	} else {
		fmt.Printf("\n⚠ nodes disagree on count — data is split by replica set; " +
			"use QueryMergeSum across all replicas for a cluster-wide total\n")
	}
}

func queryCount(c *tsdb.Client, table string) int64 {
	r, err := c.Query("SELECT count(*) FROM " + table)
	if err != nil || len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
		return -1
	}
	if v, ok := r.Rows[0][0].(int64); ok {
		return v
	}
	return -1
}

func shortAddr(a string) string {
	if i := strings.LastIndex(a, ":"); i >= 0 {
		return a[i+1:]
	}
	return a
}

func runDocker(sshHost, action, container string) {
	args := []string{"docker", action, container}
	if sshHost != "" {
		cmd := exec.Command("ssh", sshHost, strings.Join(args, " "))
		out, err := cmd.CombinedOutput()
		log.Printf("  docker %s %s → %v %s", action, container, err, strings.TrimSpace(string(out)))
		return
	}
	cmd := exec.Command(args[0], args[1:]...)
	out, err := cmd.CombinedOutput()
	log.Printf("  docker %s %s → %v %s", action, container, err, strings.TrimSpace(string(out)))
}

func runComposeUp(sshHost, composeFile string) {
	dockerArgs := []string{"docker", "compose", "-f", composeFile, "up", "-d"}
	var cmd *exec.Cmd
	if sshHost != "" {
		cmd = exec.Command("ssh", sshHost, strings.Join(dockerArgs, " "))
	} else {
		cmd = exec.Command(dockerArgs[0], dockerArgs[1:]...)
	}
	out, err := cmd.CombinedOutput()
	log.Printf("  compose up %s → %v %s", composeFile, err, strings.TrimSpace(string(out)))
}

var _ = sync.Mutex{} // reserved for future per-node write routing
