#!/usr/bin/env bash
# bench_influx_multi.sh — same as bench_influx_ingest but writes to
# T different measurements per writer, so per-table locks don't all
# converge on a single mutex.  Tests whether g_create_mu shrink + the
# arena reduces contention multi-table workloads.

set -u
LINES=${1:-2000}
BATCHES=${2:-20}
CONC=${3:-4}
TABLES=${4:-8}
SSH=${SSH:-ssh root@10.88.51.102}

TOTAL=$((LINES * BATCHES * CONC))
say() { printf "\e[36m[bench]\e[0m %s\n" "$*"; }
say "config: lines/batch=$LINES batches=$BATCHES writers=$CONC tables=$TABLES total=$TOTAL"

# Generate one payload per writer that round-robins across $TABLES.
$SSH "ts=\$(date +%s)000000000
  for w in \$(seq 0 $(($CONC - 1))); do
    awk -v n=$LINES -v ts=\$ts -v w=\$w -v T=$TABLES 'BEGIN {
      for (i=1; i<=n; i++) {
        t = (i + w) % T
        printf \"bm_%d,host=h%d cpu=%.2f,mem=%di %d\n\", t, (i % 100), (i*0.5), (i*7), ts + i*1000
      }
    }' > /tmp/multi_w\$w.txt
  done
  ls -lh /tmp/multi_w*.txt | head -3"

# Cleanup any prior tables.
$SSH "curl -s -c /tmp/bck -X POST 'http://127.0.0.1:29311/login' -d 'user=root&pass=123456' -o /dev/null
      for t in \$(seq 0 $(($TABLES - 1))); do
        curl -sb /tmp/bck -X POST 'http://127.0.0.1:29311/sql' -H 'Content-Type: application/json' \
          --data-binary \"{\\\"q\\\":\\\"DROP TABLE bm_\$t\\\"}\" > /dev/null 2>&1
      done"

t0=$(date +%s%N)
$SSH "
  pids=
  for w in \$(seq 0 $(($CONC - 1))); do
    (
      for b in \$(seq 1 $BATCHES); do
        curl -s -X POST 'http://127.0.0.1:29321/write' --data-binary @/tmp/multi_w\$w.txt > /dev/null
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

# Verify
sum=0
for t in $(seq 0 $((TABLES - 1))); do
  c=$($SSH "curl -sb /tmp/bck -X POST 'http://127.0.0.1:29311/sql' -H 'Content-Type: application/json' \
    --data-binary '{\"q\":\"SELECT count(*) FROM bm_$t\"}' | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(\"rows\",[[0]])[0][0])' 2>/dev/null")
  sum=$((sum + c))
done
say "table sum count(*) = $sum (target $TOTAL)"

# Cleanup
$SSH "for t in \$(seq 0 $(($TABLES - 1))); do
  curl -sb /tmp/bck -X POST 'http://127.0.0.1:29311/sql' -H 'Content-Type: application/json' \
    --data-binary \"{\\\"q\\\":\\\"DROP TABLE bm_\$t\\\"}\" > /dev/null 2>&1
done
rm -f /tmp/multi_w*.txt /tmp/bck"
