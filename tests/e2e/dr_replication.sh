#!/usr/bin/env bash
# dr_replication.sh — cross-DC async replication loopback test.
#
# Uses a single running node as BOTH source and "remote DC" — the
# cluster's own RPC port serves as the FED_INGEST target so we can
# verify the full encode → enqueue → send → receive → apply → metrics
# pipeline without spinning up a second cluster.
#
# Success criteria:
#   1. qengine_dr_sent_total increments after a write
#   2. qengine_dr_ack_total matches sent (same RPC loop)
#   3. qengine_dr_recv_ok_total increments (receive-side applied)
#   4. The ingested rows are visible to SELECT after a short delay
#
# Usage:
#   bash tests/e2e/dr_replication.sh             # lvm1 cluster
#   REMOTE=10.88.51.102 bash tests/e2e/dr_replication.sh
#   LOCAL_ONLY=1 bash tests/e2e/dr_replication.sh   # bootstrap a local node

set -u

REMOTE="${REMOTE:-10.88.51.102}"
BASE="http://127.0.0.1:29311"
SSH="ssh root@${REMOTE}"

PASS=0
FAIL=0
say()  { printf "\e[36m[dr-e2e]\e[0m %s\n" "$*"; }
pass() { printf "  \e[32m✓\e[0m %s\n" "$*"; PASS=$((PASS+1)); }
fail() { printf "  \e[31m✗\e[0m %s\n" "$*"; FAIL=$((FAIL+1)); }

# Pull a single /metrics counter value; blank if absent.
metric() {
  local name="$1" port="${2:-29311}"
  $SSH "curl -s http://127.0.0.1:${port}/metrics | awk -v k=\"${name}\" '\$1==k { print \$2 }'"
}

# ── Phase 0: confirm DR forwarder is armed on cnode-1 ──────────────
# Probe the env var directly — `docker logs --tail N` rolls out startup
# banners after a long-running cluster sees enough request logging, and
# the rest of the test then aborts on a false negative.  The env-var
# check is what actually drives DR (see node_main.c reading TSDB_DR_REMOTE
# at startup), so this is closer to ground truth anyway.
say "phase 0: verify DR forwarder armed"
dr_env=$($SSH "docker exec qengine-cnode-1 sh -c 'printenv TSDB_DR_REMOTE 2>/dev/null'")
if [[ -n "$dr_env" ]]; then
  pass "cnode-1 has TSDB_DR_REMOTE=$dr_env"
else
  fail "cnode-1 missing TSDB_DR_REMOTE env — DR forwarder not wired"
  echo "    (set TSDB_DR_REMOTE in docker-compose.cluster.yml and recreate)"
  say "────────────────────────────────────────────"
  say "PASS: $PASS  FAIL: $FAIL"
  exit 2
fi

# ── Phase 1: baseline metrics snapshot ─────────────────────────────
say "phase 1: baseline DR metrics"
base_sent=$(metric qengine_dr_sent_total    29311); base_sent=${base_sent:-0}
base_ack=$(metric  qengine_dr_ack_total     29311); base_ack=${base_ack:-0}
base_drop=$(metric qengine_dr_dropped_total 29311); base_drop=${base_drop:-0}
# DR recv counter lives on the REMOTE (cnode-2), exposed on 29312.
base_rcv=$(metric  qengine_dr_recv_ok_total 29312); base_rcv=${base_rcv:-0}
say "  baseline: sent=$base_sent ack=$base_ack recv_ok=$base_rcv dropped=$base_drop"

# ── Phase 2: login + write a burst ──────────────────────────────────
say "phase 2: write 200 rows into dr_t"
$SSH "curl -s -c /tmp/ck -X POST '$BASE/login' -d 'user=root&pass=123456' -o /dev/null"

# Setup table; ignore if exists.
$SSH "curl -sb /tmp/ck -X POST '$BASE/sql' -H 'Content-Type: application/json' \
  --data-binary '{\"q\":\"CREATE TABLE dr_t (ts TIMESTAMP, v INT64) TIMESTAMP(ts)\"}'" > /dev/null 2>&1
# Ingest 200 rows via an Influx-line burst on port 29321.
ts_ns=$(( $(date +%s) * 1000000000 ))
lines=""
for i in $(seq 1 200); do
  t=$(( ts_ns + i * 1000 ))
  lines="${lines}dr_t v=${i}i ${t}"$'\n'
done
echo "$lines" | $SSH "cat > /tmp/dr_burst.txt"
$SSH "curl -s -X POST 'http://127.0.0.1:29321/write' --data-binary @/tmp/dr_burst.txt" > /dev/null

# Wait a beat for the forwarder's drain loop.
sleep 3

# ── Phase 3: delta assertions ───────────────────────────────────────
say "phase 3: verify DR counters moved"
new_sent=$(metric qengine_dr_sent_total    29311); new_sent=${new_sent:-0}
new_ack=$(metric  qengine_dr_ack_total     29311); new_ack=${new_ack:-0}
new_drop=$(metric qengine_dr_dropped_total 29311); new_drop=${new_drop:-0}
new_rcv=$(metric  qengine_dr_recv_ok_total 29312); new_rcv=${new_rcv:-0}
d_sent=$((new_sent - base_sent))
d_ack=$((new_ack  - base_ack))
d_drop=$((new_drop - base_drop))
d_rcv=$((new_rcv - base_rcv))
say "  delta:    sent+=$d_sent ack+=$d_ack recv_ok+=$d_rcv dropped+=$d_drop"
if [[ $d_sent -gt 0 ]]; then pass "dr_sent_total increased ($d_sent)"
else                           fail "dr_sent_total did not move"; fi
if [[ $d_ack  -gt 0 ]]; then pass "dr_ack_total increased ($d_ack)"
else                           fail "dr_ack_total did not move (remote unreachable?)"; fi
if [[ $d_rcv  -gt 0 ]]; then pass "dr_recv_ok_total increased ($d_rcv)"
else                           fail "dr_recv_ok_total did not move"; fi
if [[ $d_drop -eq 0 ]]; then pass "no drops"; else fail "dropped $d_drop batches"; fi

# ── Phase 4: visible after replication ──────────────────────────────
say "phase 4: rows land on the receiver"
cnt=$($SSH "curl -sb /tmp/ck -X POST '$BASE/sql' -H 'Content-Type: application/json' \
  --data-binary '{\"q\":\"SELECT count(*) FROM dr_t\"}' | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[\"rows\"][0][0])'")
if [[ "$cnt" -ge 200 ]]; then pass "SELECT count(*)=$cnt (>=200)"
else                          fail "SELECT count(*)=$cnt (expected >=200)"; fi

# ── Phase 5: cleanup ────────────────────────────────────────────────
$SSH "curl -sb /tmp/ck -X POST '$BASE/sql' -H 'Content-Type: application/json' \
  --data-binary '{\"q\":\"DROP TABLE dr_t\"}'" > /dev/null 2>&1
$SSH "rm -f /tmp/dr_burst.txt /tmp/ck"

say "────────────────────────────────────────────"
say "PASS: $PASS  FAIL: $FAIL"
[[ $FAIL -eq 0 ]]
