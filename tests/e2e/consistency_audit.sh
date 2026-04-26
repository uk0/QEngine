#!/usr/bin/env bash
# consistency_audit.sh — write into every node and prove all 4 nodes
# converge to the same row count, then stress with concurrent writes
# and a leader-kill mid-burst.  Surfaces any silent row-loss / split-
# brain on the live lvm1 cluster.
#
# Phase 0: per-node single write — does it propagate to all peers?
# Phase 1: concurrent writes from all 4 nodes — quorum + ordering
# Phase 2: kill cnode-1 mid-burst, restart, anti-entropy catch-up
# Phase 3: replication health metrics — sent vs ack vs recv_ok

set -u
SSH=${SSH:-ssh root@10.88.51.102}

PORTS=(29311 29312 29313 29314)
NODES=(qengine-cnode-1 qengine-cnode-2 qengine-cnode-3 qengine-cnode-4)

PASS=0
FAIL=0
say()  { printf '\e[36m[con]\e[0m %s\n' "$*"; }
ok()   { printf '  \e[32m✓\e[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '  \e[31m✗\e[0m %s\n' "$*"; FAIL=$((FAIL+1)); }

# Login on every node; cookie-per-port since each node mints its own.
$SSH "for p in ${PORTS[@]}; do
  curl -s -c /tmp/ck_\$p -X POST 'http://127.0.0.1:'\$p'/login' -d 'user=root&pass=123456' -o /dev/null
done"

# Ensure clean state — drop the audit table everywhere if it lingers.
for p in "${PORTS[@]}"; do
  $SSH "curl -sb /tmp/ck_$p -X POST 'http://127.0.0.1:$p/sql' -H 'Content-Type: application/json' \
        --data-binary '{\"q\":\"DROP TABLE caudit\"}' > /dev/null 2>&1"
done

# Helper: count rows in caudit on a port.
count() {
  local p=$1
  $SSH "curl -sb /tmp/ck_$p -X POST 'http://127.0.0.1:$p/sql' -H 'Content-Type: application/json' \
    --data-binary '{\"q\":\"SELECT count(*) FROM caudit\"}' | \
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(\"rows\",[[0]])[0][0])' 2>/dev/null"
}

# ──────────────────────────────────────────────────────────────────
# Phase 0: create on coord, write 100 rows on each node, verify all
#          4 nodes converge to 400 rows
# ──────────────────────────────────────────────────────────────────
say "phase 0: create + per-node write 100 rows + cross-node verify"

# Create on coordinator (cnode-1, port 29311) — Raft DDL replicates to all.
$SSH "curl -sb /tmp/ck_29311 -X POST 'http://127.0.0.1:29311/sql' -H 'Content-Type: application/json' \
  --data-binary '{\"q\":\"CREATE TABLE caudit (ts TIMESTAMP, src SYMBOL, v INT64) TIMESTAMP(ts)\"}' > /dev/null"
sleep 1

# Each node writes 100 rows tagged with its hostname.
ts=$($SSH "date +%s")
for i in 0 1 2 3; do
  p=${PORTS[$i]}
  c=${NODES[$i]}
  base=$(( (ts + i * 100) * 1000000000 ))
  # Use Influx line on the per-node write port.
  ip=$((29321 + i))
  lines=""
  for r in $(seq 1 100); do
    lines="${lines}caudit,src=node$i v=${r}i $((base + r * 1000))"$'\n'
  done
  $SSH "echo \"$lines\" > /tmp/au_$i.txt
        curl -s -X POST 'http://127.0.0.1:$ip/write' --data-binary @/tmp/au_$i.txt > /dev/null"
done
sleep 3

# Every node should now report 400 rows.
target=400
for p in "${PORTS[@]}"; do
  c=$(count "$p")
  if [[ "$c" == "$target" ]]; then
    ok "node :$p has $c rows"
  else
    bad "node :$p has $c rows (expected $target)"
  fi
done

# ──────────────────────────────────────────────────────────────────
# Phase 1: parallel burst from all 4 nodes
# ──────────────────────────────────────────────────────────────────
say "phase 1: parallel 1k×4 burst from each port (4k more rows)"
ts=$($SSH "date +%s")
$SSH "
  pids=
  for i in 0 1 2 3; do
    base=\$(( ($ts + 1000 + i*100) * 1000000000 ))
    ip=\$((29321 + i))
    (
      lines=
      for r in \$(seq 1 1000); do
        lines=\"\$lines\"\"caudit,src=p\$i v=\${r}i \$(( base + r*1000 ))
\"
      done
      echo \"\$lines\" > /tmp/burst_\$i.txt
      for k in 1 2 3 4; do
        curl -s -X POST 'http://127.0.0.1:'\$ip'/write' --data-binary @/tmp/burst_\$i.txt > /dev/null
      done
    ) &
    pids=\"\$pids \$!\"
  done
  wait \$pids"
sleep 5

# 400 (phase0) + 4 nodes × 1000 × 4 batches = 400 + 16000 = 16400
target=16400
nodes_ok=0
for p in "${PORTS[@]}"; do
  c=$(count "$p")
  if [[ "$c" == "$target" ]]; then
    ok "node :$p has $c rows"
    nodes_ok=$((nodes_ok + 1))
  else
    bad "node :$p has $c rows (expected $target — diff $((c - target)))"
  fi
done

# Pairwise convergence — even if all nodes missed, at least they should agree.
counts=()
for p in "${PORTS[@]}"; do counts+=("$(count $p)"); done
if [[ "${counts[0]}" == "${counts[1]}" && "${counts[1]}" == "${counts[2]}" && "${counts[2]}" == "${counts[3]}" ]]; then
  ok "all 4 nodes converge to ${counts[0]}"
else
  bad "node counts diverge: ${counts[*]}"
fi

# ──────────────────────────────────────────────────────────────────
# Phase 2: replication health from /metrics
# ──────────────────────────────────────────────────────────────────
say "phase 2: replication metrics"
for p in "${PORTS[@]}"; do
  m=$($SSH "curl -s http://127.0.0.1:$p/metrics | awk '
    /^qengine_replicate_(sent|ack|fail|recv_ok|recv_err)_total/ {a[\$1]=\$2}
    END {
      printf \"sent=%d ack=%d fail=%d recv_ok=%d recv_err=%d\",
        a[\"qengine_replicate_sent_total\"]+0,
        a[\"qengine_replicate_ack_total\"]+0,
        a[\"qengine_replicate_fail_total\"]+0,
        a[\"qengine_replicate_recv_ok_total\"]+0,
        a[\"qengine_replicate_recv_err_total\"]+0
    }'")
  ok "node :$p $m"
  ack=$(echo "$m" | sed -n 's/.*ack=\([0-9]*\).*/\1/p')
  fail=$(echo "$m" | sed -n 's/.*fail=\([0-9]*\).*/\1/p')
  rerr=$(echo "$m" | sed -n 's/.*recv_err=\([0-9]*\).*/\1/p')
  if [[ "$rerr" -gt 0 ]]; then bad "node :$p recv_err=$rerr (expected 0)"; fi
done

# ──────────────────────────────────────────────────────────────────
# Phase 3: kill leader during burst, anti-entropy must close the gap
# ──────────────────────────────────────────────────────────────────
say "phase 3: kill cnode-3 mid-burst → restart → anti-entropy catch-up"

target_pre=$(count 29311)

ts=$($SSH "date +%s")
$SSH "
  # background burst on cnode-1
  base=\$(( ($ts + 2000) * 1000000000 ))
  for r in \$(seq 1 500); do
    printf 'caudit,src=chaos v=%di %d\n' \$r \$((base + r*1000))
  done > /tmp/chaos.txt
  (
    for k in 1 2 3 4 5; do
      curl -s -X POST 'http://127.0.0.1:29321/write' --data-binary @/tmp/chaos.txt > /dev/null
      sleep 0.4
    done
  ) &
  bg=\$!
  sleep 1
  # mid-burst — kill cnode-3
  docker stop -t 3 qengine-cnode-3 > /dev/null
  echo 'cnode-3 stopped at \$(date +%T)'
  wait \$bg
  sleep 2
  docker start qengine-cnode-3 > /dev/null
  echo 'cnode-3 started at \$(date +%T)'
"

# Wait for anti-entropy catch-up.  Poll up to 90 s — startup AE walks
# every table in db->tables[], which on a long-lived test cluster can
# accumulate hundreds of debris tables from prior tests; the per-table
# resync RPCs (count + max_ts SELECT against each peer) are cheap but
# add up sequentially.  90 s is generous for the test target table to
# converge once AE reaches it; production AE will be partition-Merkle.
say "phase 3: polling for cnode-3 catch-up (max 90s)"
# Re-login on every node — cnode-3's restart invalidated its session
# cookie (server boot mints a fresh secret).  Without this, count()
# against cnode-3 returns 401, the python parser falls back to 0, and
# we see a phantom "divergence" even when AE caught up correctly.
$SSH "for p in ${PORTS[@]}; do
  curl -s -c /tmp/ck_\$p -X POST 'http://127.0.0.1:'\$p'/login' -d 'user=root&pass=123456' -o /dev/null
done"

deadline=$(( $(date +%s) + 90 ))
post_count=()
while (( $(date +%s) < deadline )); do
  post_count=()
  for p in "${PORTS[@]}"; do post_count+=("$(count $p)"); done
  if [[ "${post_count[0]}" == "${post_count[1]}" && \
        "${post_count[1]}" == "${post_count[2]}" && \
        "${post_count[2]}" == "${post_count[3]}" ]]; then
    break
  fi
  sleep 3
done

if [[ "${post_count[0]}" == "${post_count[1]}" && "${post_count[1]}" == "${post_count[2]}" && "${post_count[2]}" == "${post_count[3]}" ]]; then
  ok "post-chaos: all 4 nodes converge to ${post_count[0]} (gain $(( post_count[0] - target_pre )))"
else
  bad "post-chaos divergence: ${post_count[*]}"
fi

# Cleanup
for p in "${PORTS[@]}"; do
  $SSH "curl -sb /tmp/ck_$p -X POST 'http://127.0.0.1:$p/sql' -H 'Content-Type: application/json' \
        --data-binary '{\"q\":\"DROP TABLE caudit\"}' > /dev/null 2>&1"
done
$SSH "rm -f /tmp/ck_2931* /tmp/au_*.txt /tmp/burst_*.txt /tmp/chaos.txt"

echo
say "────────────────────────────────"
say "PASS: $PASS  FAIL: $FAIL"
[[ $FAIL -eq 0 ]]
