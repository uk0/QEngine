# example-go — QEngine multi-cluster router example

A self-contained Go program that demonstrates **client-side** cluster
routing for QEngine: two (or more) independent clusters, one logical
read/write API.

```
┌──────────────┐        writes ──▶  least-loaded / hash / round-robin
│  example-go  │
│   Router     │────────┬─────────────────┬────────────
│              │        │                 │
└──────────────┘        ▼                 ▼
                   cluster-a         cluster-b
                   (A-metrics)      (B-metrics)

  reads:  fanout →  cluster-a.client   cluster-b.client
                         │                   │
                         └─── merge ─────────┘
                              (sum / avg / concat)
```

## What the demo does

1. Connects to **both** clusters' wire ports (default `lvm1:29090` +
   `lvm1:29190`) and their metrics endpoints (`:29094` / `:29194`).
2. Spawns a background prober that polls `/metrics` every 2 s and
   records each cluster's `rows_written_total` as a load score.
3. Broadcasts `CREATE TABLE` to both so schema is consistent on both
   sides.
4. Writes N rows (default 200 k) using the selected policy:
   - `least_loaded` (default) — send batch to whichever backend has the
     smallest `rows_written_total`.  As one side fills up, subsequent
     batches skew to the other.
   - `hash` — `fnv(table) mod 2` — deterministic, useful when you want
     all rows for a table on one backend so point queries avoid fanout.
   - `round_robin` — alternate per batch.  Simplest fairness.
5. Issues merged reads:
   - `count(*)` → `QueryMergeSum` (fanout + sum).
   - `avg(price)` → `QueryMergeAvg` (rewrites to per-cluster `sum +
     count`, then computes global average).
   - `SELECT ts, price, qty LIMIT 3` → `QueryConcat` (fanout + row
     concatenation, carries per-source row counts for traceability).
6. Demonstrates pinning a query to one backend via `r.Route("cluster-a")`.

## Quick start

```bash
# Assuming two QEngine servers on lvm1 per the main repo's
# deployment/docker-compose.twin.yml (ports 29090 client / 29094 metrics
# for cluster-a, 29190/29194 for cluster-b):

go build -o example-router .
./example-router -rows 200000 -batch 4096 -policy least_loaded
```

Output (abridged):

```
dialing backends…

---- initial state ----
  cluster-a    lvm1:29090      UP   rows_written=0
  cluster-b    lvm1:29190      UP   rows_written=0

ingesting 200000 rows (batch=4096, policy=least_loaded)…
done: 200000 rows in 2.62s (76237 rows/s)
  cluster-a    → 110592 rows
  cluster-b    →  89408 rows

---- post-ingest ----
  cluster-a    lvm1:29090      UP   rows_written=110592
  cluster-b    lvm1:29190      UP   rows_written=89408

---- merged reads ----
  count(*) across clusters         = 200000
  avg(price) across clusters       = 110.0031
  sum(qty) across clusters         = 99942...
```

The split depends on timing — `LeastLoaded` reacts on 2 s refresh
ticks, so on a fast ingest the cursor only gets a chance to swing
between backends a handful of times.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `-a`  | `lvm1:29090` | cluster-a client address |
| `-b`  | `lvm1:29190` | cluster-b client address |
| `-am` | `lvm1:29094` | cluster-a `/metrics` endpoint |
| `-bm` | `lvm1:29194` | cluster-b `/metrics` endpoint |
| `-rows` | `200000` | total rows to insert across both clusters |
| `-batch` | `4096` | rows per `WRITE_BATCH` frame |
| `-policy` | `least_loaded` | `least_loaded` / `hash` / `round_robin` |
| `-reset` | `true` | `DROP TABLE` on both sides before ingest |
| `-table` | `demo_trades` | target table name |

## How merged queries work

| Query shape | Merge function | Correctness |
|---|---|---|
| `SELECT count(*)` | `QueryMergeSum` | exact |
| `SELECT sum(col)` | `QueryMergeSum` | exact |
| `SELECT avg(col)` | `QueryMergeAvg` (sum+count) | exact |
| `SELECT min(col)` | use `QueryConcat` + min client-side | exact but one extra pass |
| `SELECT p99(col)` | **not supported** — pin to one backend | would need HyperLogLog-style sketches |
| `SELECT *` | `QueryConcat` | rows interleaved by backend; sort client-side if needed |

## Using the Router as a library

The router is intentionally a single file (`router.go`) with no external
deps beyond the QEngine Go SDK (`github.com/qengine/tsdb-go`).  Copy it
into your own project, adjust the `Policy` enum to add custom
placement (per-tenant, per-region, hot-key pinning), and you're set.

## Files

```
example-go/
├── go.mod         # depends on github.com/qengine/tsdb-go (replace to
│                  # the local checkout; swap for a tag once published)
├── router.go      # Backend + Router + policies + merge helpers
├── main.go        # demo driver (flag parsing, ingest + reads)
└── README.md      # you are here
```

## Requirements

- Go 1.21+
- Two running QEngine servers (any mix of single-node and clustered)
- Each server has the `/metrics` HTTP endpoint enabled
  (`TSDB_METRICS_BIND=0.0.0.0:28094` or similar)

## Caveats / next steps

- **Transactionality**: this is best-effort routing, no 2-phase commit.
  A `WRITE_BATCH` that picks one backend and fails will leave that batch
  un-ingested; retry logic lives in caller code.
- **Consistency**: `LeastLoaded` writes the same `ts` to different
  backends on different batches — if you need "all rows for key K on one
  backend", use `HashTable`.
- **Cross-cluster aggregates**: percentiles and cardinality queries
  don't merge correctly without sketches.  For those, pin to one
  backend via `r.Route(name)`.
- **Failure mode**: if a backend's `/metrics` probe fails, it's marked
  `ok=false` and skipped by `LeastLoaded`.  `HashTable` and `RoundRobin`
  still attempt it — extend the policy if you want to skip unhealthy
  peers universally.
