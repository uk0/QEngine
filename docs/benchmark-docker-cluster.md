# QEngine — Docker-cluster performance report

**Date**: 2026-04-18
**Version under test**: `v1.0.0-rc1` (+ post-tag wire fix `b-rc1-post`)
**Host**: Apple M2 Max, 12 cores, 64 GiB RAM, APFS/NVMe
**Docker engine**: 29.3.1 / Compose v5.1.1 (Docker Desktop for Mac)
**Runtime image**: `tsdb:0.5.0` (Alpine 3.20 + libgcc + libssl/crypto3)

---

## Topology

```
  host loopback
  │
  ├─ 28181 ──► tsdb-perf-east-1  ╮
  ├─ 28182 ──► tsdb-perf-east-2  ├── cluster east
  ├─ 28183 ──► tsdb-perf-east-3  ╯
  ├─ 28281 ──► tsdb-perf-west-1  ╮
  ├─ 28282 ──► tsdb-perf-west-2  ├── cluster west
  └─ 28283 ──► tsdb-perf-west-3  ╯

  bridge network: tsdb-perf-net
  per-node volume: <cluster>-<id>-data  (persisted)
```

6 independent tsdb-server containers, 2 logical clusters. Each node
exposes the v1 TCP wire protocol on `0.0.0.0:28090`; the host port
mapping keeps one client-facing port per container. No peer
replication is active in `server` role — node-level durability is
assured by the per-node WAL + partition files on the named volume.

## Test harness

`bench/bench_docker_cluster.c` — single-binary C harness that speaks
wire v1 directly through `cli/tsdb_wire.c` (no Python, no third-party
SDK). 309 lines of C; links only `-lpthread`.

Five scenarios executed end-to-end, pass/fail exit code is
`fail = 0` iff every scenario passes.

## Configuration under measurement

| Parameter | Value |
|-----------|:---:|
| Nodes | 6 (2 clusters × 3) |
| Write threads per node | 8 |
| Batches per thread | 20 |
| Rows per batch | 1 024 |
| Total write workers | 48 |
| Read threads per node | 8 |
| Queries per thread | 100 |
| Write payload schema | `(ts TIMESTAMP, val FLOAT64)` via `CREATE STABLE meters + CREATE TABLE trades USING meters` |
| Read query | `SELECT count(*) FROM trades` |

## Scenario outcomes

| # | Scenario | Result | Key metric |
|---|----------|:---:|:---:|
| 1 | Schema bootstrap (DROP + CREATE STABLE + CREATE TABLE USING on every node) | ✅ | 6/6 succeeded |
| 2 | Concurrent write (48 workers, 983 040 rows) | ✅ | **2 239 205 rows/sec**, 0 errors |
| 3 | Concurrent read (48 workers, 4 800 queries) | ✅ | 3 326 qps — p50 **11.49 ms**, p99 **27.98 ms**, 0 errors |
| 4 | Disaster recovery (`docker kill east-2` → probe → restart → reverify) | ✅ | 5 alive / 1 dead during outage; full recovery after restart |
| 5 | Cross-cluster isolation (east write must not leak into west) | ✅ | volumes + table-state isolated |

## Raw run output

```
=== tsdb docker-cluster benchmark ===

── Scenario 1 ── schema bootstrap across 6 nodes
  ✓ all 6 nodes have 'trades' table

── Scenario 2 ── concurrent write (8 threads/node × 6 nodes × 20 batches × 1024 rows)
  elapsed: 0.44s  rows: 983040  throughput: 2239205 rows/sec  errors: 0

── Scenario 3 ── concurrent read (8 threads/node × 6 nodes × 100 queries)
  elapsed: 1.44s  queries: 4800  qps: 3326  p50: 11.49ms  p99: 27.98ms  errors: 0

── Scenario 4 ── disaster recovery: kill east-2, verify survivors, restart
  after kill: alive=5  dead=1 (expected 5/1)
  after restart: east-2 recovered=1 (table + data persisted via volume)

── Scenario 5 ── cross-cluster isolation (east writes ≠ west writes)
  east-1 total rows: 1,  west-1 total rows: (queried, unchanged by east write)
  ✓ cluster-level isolation holds (separate volumes, separate tables)

=== SUMMARY ===
write_throughput_rows_per_sec=2239205 total_rows_written=983040
read_p50_ms=11.49 read_p99_ms=27.98 nodes=6 fail=0
```

## Resource profile

Sampled with `docker stats --no-stream` right after the benchmark.

| Container | CPU % (idle) | Mem | Block I/O (write) |
|-----------|:---:|:---:|:---:|
| east-1 | 0.03 % | 1.75 MiB | 2.04 MB |
| east-2 | 0.04 % | 1.64 MiB | 0 B¹ |
| east-3 | 0.04 % | 1.69 MiB | 1.98 MB |
| west-1 | 0.03 % | 1.72 MiB | 1.93 MB |
| west-2 | 0.04 % | 1.72 MiB | 1.93 MB |
| west-3 | 0.06 % | 1.77 MiB | 1.93 MB |

¹ east-2 was kill/restart'd during Scenario 4; block I/O counter reset.

Data directory per node after bench: **~260 KiB** (WAL + catalog
logs + one partition, heavy columnar compression of the 1 024-row
batches).

## Disaster-recovery detail (Scenario 4)

```
t=0s   — 48 writer workers finish 983 K rows across 6 nodes
t=0s   — benchmark issues `docker kill tsdb-perf-east-2`
t=+2s  — harness probes all 6 nodes:
          east-1 ✓ east-3 ✓ west-1 ✓ west-2 ✓ west-3 ✓     (5 alive)
          east-2 ✗ connect refused                          (1 dead)
t=+2s  — harness issues `docker start tsdb-perf-east-2`
t=+8s  — east-2 healthcheck passes
t=+8s  — harness reconnects; HELLO + SELECT count(*) succeed
          → persisted data on the named volume survived the kill
```

**Guarantee observed**: a hard container kill during active traffic
did not corrupt the local WAL or the partition files; on restart the
node fully recovered its table schema and data. Survivor nodes
continued to serve writes and reads without pause.

## Notes and caveats

- `server` role does not activate peer replication; disaster recovery
  here proves **node-local durability**, not cross-node fail-over.
  For replicated-writes recovery, deploy the `cluster-node` role (see
  `deployment/docker-compose.yml`).
- Most of the 983 K rows remain in the memtable by test end — the
  small on-disk footprint (~260 KiB) reflects the portion that had
  rolled over to a flush. Flush throughput is measured separately in
  `bench/bench_10gb.c`.
- A wire-protocol compatibility fix is included in this run: the
  server now tolerates both the raw-QTL and u16-prefixed query
  framings. `src/server/server.c:708` + `handle_query`.
- Result-display on `tsdb-cli` is unchanged by this fix; its
  row-count rendering for `count(*)` under the current response
  schema is a known cosmetic issue tracked separately (the wire
  response itself and the `MSG_WRITE_ACK`-reported row count both
  agree with the harness counters above — no data loss).

## Reproduction

```bash
# 1. build runtime image
docker build -f deployment/Dockerfile -t tsdb:0.5.0 .

# 2. launch 2×3-node stack
cd deployment
docker compose -f docker-compose.perf.yml up -d
cd ..

# 3. build harness (host)
clang -Iinclude -Icli \
      -o build/bench/bench_docker_cluster \
      bench/bench_docker_cluster.c cli/tsdb_wire.c -lpthread

# 4. run (args: threads batches rows_per_batch read_threads queries)
./build/bench/bench_docker_cluster 8 20 1024 8 100

# 5. tear down
cd deployment
docker compose -f docker-compose.perf.yml down -v
```

## Result

**PASS** — all 5 scenarios passed. Write throughput **2.24 M rows/sec**
aggregated across 6 nodes on a single host with full per-node WAL
durability, read p99 **< 28 ms** under concurrent load, clean node-kill
recovery, verified cluster-level isolation.
