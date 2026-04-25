#!/usr/bin/env bash
# bench_influx_ingest.sh — measure Influx-line throughput on the
# lvm1 cluster.  Generates N lines per batch × M batches across K
# concurrent writers, posts to /write on a node, prints MB/s + lines/s
# observed at the server.
#
# Used as the before/after needle for parser + lock-scope optimisations.
#
# Usage:
#   bash bench/bench_influx_ingest.sh [LINES] [BATCHES] [CONCURRENCY]
#   defaults: 5000 lines × 200 batches × 4 writers = 4M lines

set -u
LINES=${1:-5000}
BATCHES=${2:-200}
CONC=${3:-4}
SSH=${SSH:-ssh root@10.88.51.102}

TOTAL=$((LINES * BATCHES * CONC))
say() { printf "\e[36m[bench]\e[0m %s\n" "$*"; }

say "config: lines/batch=$LINES  batches/writer=$BATCHES  writers=$CONC  total=$TOTAL"

# Build a lines payload once on the remote.
$SSH "
  ts=\$(date +%s)000000000
  awk -v n=$LINES -v ts=\$ts 'BEGIN {
    for (i=1; i<=n; i++) {
      printf \"bench_t,host=h%d cpu=%.2f,mem=%di %d\n\",
             (i % 100), (i*0.5), (i*7), ts + i*1000
    }
  }' > /tmp/bench_lines.txt
  wc -l /tmp/bench_lines.txt
  ls -lh /tmp/bench_lines.txt
"

# Create the destination table.
$SSH "curl -s -c /tmp/bck -X POST 'http://127.0.0.1:29311/login' -d 'user=root&pass=123456' -o /dev/null
      curl -sb /tmp/bck -X POST 'http://127.0.0.1:29311/sql' -H 'Content-Type: application/json' \
        --data-binary '{\"q\":\"DROP TABLE bench_t\"}' > /dev/null 2>&1"

# Snapshot baseline counters.
metric() { $SSH "curl -s 'http://127.0.0.1:29311/metrics' | awk -v k=\"\$1\" '\$1==k{print \$2}'"; }
say "baseline counters:"
$SSH "curl -s 'http://127.0.0.1:29311/metrics' | grep -E '^qengine_(rows_written|connections|queries|replicate)_total'"

t0=$(date +%s%N)
# Fire C concurrent writers, each looping B batches.
$SSH "
  pids=
  for w in \$(seq 1 $CONC); do
    (
      for b in \$(seq 1 $BATCHES); do
        curl -s -X POST 'http://127.0.0.1:29321/write' --data-binary @/tmp/bench_lines.txt > /dev/null
      done
    ) &
    pids=\"\$pids \$!\"
  done
  wait \$pids
"
t1=$(date +%s%N)
elapsed_ns=$((t1 - t0))
elapsed_s=$(awk -v ns=$elapsed_ns 'BEGIN {printf "%.2f", ns/1e9}')
rate=$(awk -v t=$TOTAL -v ns=$elapsed_ns 'BEGIN {printf "%.0f", t*1e9/ns}')

say "elapsed: ${elapsed_s}s    rate: ${rate} rows/s    over $TOTAL rows"

say "post counters:"
$SSH "curl -s 'http://127.0.0.1:29311/metrics' | grep -E '^qengine_(rows_written|connections|queries|replicate|dr_)_total'"

# Verify the rows actually landed.
cnt=$($SSH "curl -sb /tmp/bck -X POST 'http://127.0.0.1:29311/sql' -H 'Content-Type: application/json' \
  --data-binary '{\"q\":\"SELECT count(*) FROM bench_t\"}' | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[\"rows\"][0][0])'")
say "table count(*) = $cnt"

# Cleanup
$SSH "curl -sb /tmp/bck -X POST 'http://127.0.0.1:29311/sql' -H 'Content-Type: application/json' \
  --data-binary '{\"q\":\"DROP TABLE bench_t\"}' > /dev/null 2>&1
  rm -f /tmp/bench_lines.txt /tmp/bck"
