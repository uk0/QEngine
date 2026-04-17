# tsdb

Single-machine, column-oriented time-series database in C11, with a
purpose-built vectorized query language (**QTL**). Borrowing architectural
ideas from kdb+, ClickHouse MergeTree, QuestDB, and VictoriaMetrics.

## Design goals

- **Time-series first**. Every table has a designated timestamp column.
  Nanosecond precision, partition-by-day on disk.
- **Column-oriented**. One file per column (`.col` / `.idx`). Only touched
  columns are read from disk.
- **Compressed**. Delta-of-delta for timestamps, Gorilla XOR for floats,
  dictionary encoding for SYMBOLs; each pipeline is per-column.
- **Vectorized execution**. SIMD (AVX2 + NEON) scan / filter / aggregate
  primitives; scalar fallback. ~26–71 GB/s f64 sum throughput on NEON.
- **Purpose-built query language**. QTL: SQL subset + time-series operators
  (`SAMPLE BY`, `LATEST ON`, `ASOF JOIN` — syntax).
- **No external dependencies**. libc + pthread + POSIX, C11.

## Build & test

```bash
make            # build lib, CLI, and run tests
make test       # run unit tests only
make bench      # run ingestion + query benchmarks
make debug      # -O0 -g -fsanitize=address,undefined
make clean
```

## Quickstart — CLI

```bash
./build/tsdb /tmp/mydb
tsdb 0.1.0 — type QTL queries, empty line to quit.
qtl> SELECT count(*) FROM trades;
```

## Quickstart — C API

```c
#include "tsdb.h"

int main(void) {
    tsdb_db_t *db;
    tsdb_open("/tmp/mydb", &db);

    tsdb_col_t cols[] = {
        {"ts",     TSDB_TYPE_TIMESTAMP},
        {"symbol", TSDB_TYPE_SYMBOL},
        {"price",  TSDB_TYPE_FLOAT64},
        {"volume", TSDB_TYPE_INT64},
    };
    tsdb_create_table(db, "trades", cols, 4, "ts");

    tsdb_table_t *t;
    tsdb_open_table(db, "trades", &t);

    tsdb_batch_t *b;
    tsdb_batch_begin(t, &b);
    tsdb_batch_row_ts (b,   tsdb_parse_ts("2026-01-01 09:30:00"));
    tsdb_batch_row_sym(b, 1, "AAPL");
    tsdb_batch_row_f64(b, 2, 187.42);
    tsdb_batch_row_i64(b, 3, 1200);
    tsdb_batch_row_end(b);
    tsdb_batch_commit(b);

    tsdb_result_t *r;
    tsdb_query(db, "SELECT avg(price) FROM trades WHERE symbol='AAPL'", &r);
    while (tsdb_result_next(r)) {
        printf("avg = %.4f\n", tsdb_result_f64(r, 0));
    }
    tsdb_result_free(r);
    tsdb_close(db);
}
```

## QTL — Query Time-series Language

Grammar subset supported:

```
SELECT sel_list
FROM   ident
[ WHERE expr ]
[ SAMPLE BY interval [ FILL (PREV | NULL | number) ] ]
[ LATEST ON ident [ PARTITION BY ident_list ] ]
[ GROUP BY ident_list ]
[ ORDER BY ident [ ASC | DESC ] ]
[ LIMIT N ]

interval := NUMBER unit            -- ns us ms s m h d (summable: 1h30m)
```

Functions: `sum`, `avg`, `min`, `max`, `count`, `time_bucket`.

Example queries exercised by `make test`:

```sql
SELECT * FROM trades LIMIT 10;

SELECT ts, price FROM trades WHERE symbol = 'AAPL';

SELECT sum(volume), avg(price), min(price), max(price)
FROM trades WHERE symbol = 'AAPL';

SELECT count(*) FROM trades
WHERE ts >= '2026-01-02' AND symbol = 'MSFT';

SELECT time_bucket(ts, 1m), avg(price)
FROM trades WHERE symbol = 'AAPL'
SAMPLE BY 1m FILL(PREV) LIMIT 100;
```

## Architecture

```
           ┌─────────────┐
 writer →  │  MemTable   │  (128-way sharded, columnar)
           └──────┬──────┘
                  │  flush (full / commit)
                  ▼
           ┌─────────────┐         ┌──────────────┐
           │  Codec      │ encode  │ .col blocks  │
           │  (DoD/XOR/  │────────▶│ + .idx index │
           │   Dict)     │         │ per YYYYMMDD │
           └─────────────┘         └──────┬───────┘
                                          │ mmap
                                          ▼
           ┌─────────────┐         ┌──────────────┐
  query →  │ QTL parser  │──plan──▶│ Block decode │
           │ (Lex/Parse) │         │ → SIMD exec  │
           └─────────────┘         │ → Aggregate  │
                                   └──────────────┘
```

### File layout

```
<db_dir>/
  <table>/
    schema.bin
    <col>.sym                 # dictionary, per SYMBOL column
    <YYYYMMDD>/
      <col>.col               # compressed blocks
      <col>.idx               # block index
  wal/
    <table>.log               # write-ahead log, truncated on flush
```

## Directory layout

```
include/tsdb.h          public API
src/core/               arena, bits, symbol, util
src/compress/           dod, gorilla, dict, codec
src/storage/            schema, memtable, wal, part, db
src/exec/               agg, filter, bucket, simd dispatch
src/query/              ast, lex, parse, exec
cli/                    REPL
tests/                  unit tests (4 binaries)
bench/                  ingest + query benchmarks
docs/                   design, research, changelog (gitignored)
```

## Limits and roadmap

The MVP established a working write → compress → store → query path.
Known gaps:

- **LATEST ON** and **ASOF JOIN** — syntax parsed, executor is next.
- **Chimp / Chimp128** — Gorilla is the MVP float codec; Chimp roughly halves
  compressed size on monitoring data.
- **Parallel scan** — current executor is single-threaded.
- **Block skipping via idx metadata** — time-range predicates should prune
  blocks before decode; currently only decoded ranges are filtered.
- **Advanced aggregates** — `stddev`, `p50`, `p99`, `rate`, `ewma` planned.

## License

MIT (drop-in if needed; no header in source yet).
