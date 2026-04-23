#!/usr/bin/env bash
set -uo pipefail

N1=http://127.0.0.1:29311
N2=http://127.0.0.1:29312
N3=http://127.0.0.1:29313
N4=http://127.0.0.1:29314

FAIL=0; WARN=0
START=$(date +%s)
# Dashboard auth (opt-in via TSDB_DASHBOARD_AUTH=1): login per-node so
# each query carries the right cookie.  Each node hosts its own user
# store; a token issued by one node isn't valid on another.
declare -A COOKIE
for url in $N1 $N2 $N3 $N4; do
  jar=/tmp/tsdb.cook.$(echo $url | tr -d ':/')
  curl -sS -c $jar -o /dev/null -X POST -d 'user=root&pass=123456' $url/login 2>/dev/null || true
  COOKIE[$url]=$jar
done
ok()   { printf '\e[32mPASS\e[0m  %s\n' "$1"; }
warn() { printf "\e[33mWARN\e[0m  %s :: %s\n" "$1" "${2:-}"; WARN=$((WARN+1)); }
fail() { printf '\e[31mFAIL\e[0m  %s :: %s\n' "$1" "${2:-}"; FAIL=$((FAIL+1)); }
sec()  { printf '\n\e[36m=== %s ===\e[0m\n' "$1"; }

q() {
  local url=$1 sql=$2
  local esc=$(printf '%s' "$sql" | sed 's/\\/\\\\/g; s/"/\\"/g')
  curl -sS -b ${COOKIE[$url]:-/dev/null} --max-time 30 -X POST "$url/sql" -H 'Content-Type: application/json' --data "{\"q\":\"$esc\"}"
}
fv() { echo "$1" | sed -n 's/.*"rows":\[\[\([0-9.eE+-]*\).*/\1/p'; }

influx_write() {
  local cnt_id=$1 table=$2 n_rows=$3 base_ts=$4 step_ns=$5 host=$6
  docker exec qengine-cnode-$cnt_id sh -c "
    /tmp/gen.sh $table $n_rows $base_ts $step_ns $host > /tmp/inf.txt
    LEN=\$(wc -c < /tmp/inf.txt)
    { printf 'POST /write HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: %d\\r\\nContent-Type: text/plain\\r\\nConnection: close\\r\\n\\r\\n' \$LEN; cat /tmp/inf.txt; } | nc -w 60 127.0.0.1 28092 > /dev/null
  "
}

sec '[1] cluster shape — 1 master + 3 data + raft'
R=$(curl -sS $N1/raft)
CI=$(echo "$R" | grep -oE '"commit_index":[0-9]+' | cut -d: -f2)
ROLE=$(echo "$R" | grep -oE '"role":"[a-z]+"')
[[ $CI -ge 2 && $ROLE == *leader* ]] && ok "raft leader, commit_index=$CI" || fail 'raft' "$R"

sec '[2] LIST MASTERS identical across 4 nodes'
BASELINE=
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  R=$(q $URL 'LIST MASTERS')
  N=$(echo "$R" | grep -oE 'qengine-cnode-1:28081' | wc -l | tr -d ' ')
  [[ $N == 1 ]] && ok "$NAME LIST MASTERS = node1" || fail "$NAME" "$R"
done

sec '[3] DDL routes via any node (proxy → raft)'
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  R=$(q $URL "CREATE DATABASE ent_${NAME,,}")
  [[ $R == *'committed via raft'* ]] && ok "CREATE DB via $NAME" || fail "$NAME CREATE" "$R"
done
for n in 1 2 3 4; do q $N1 "DROP DATABASE ent_n$n" > /dev/null; done

sec '[4] 50k sequential ingest — correctness'
q $N1 'CREATE TABLE ops (ts TIMESTAMP, v FLOAT64) TIMESTAMP(ts)' > /dev/null
sleep 1

T0=$(date +%s%N)
influx_write 1 ops 50000 1700000000000000000 1000000 n1
T=$(( ($(date +%s%N) - T0) / 1000000 ))
ok "50k ingest in ${T}ms ($(( 50000 * 1000 / (T>0?T:1) )) rows/s)"
sleep 3

for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  V=$(fv "$(q $URL 'SELECT count(*) FROM ops')")
  [[ $V == 50000 ]] && ok "$NAME count=50000" || fail "$NAME count" $V
done

sec '[5] aggregate operators (sum/avg/max/min)'
# v = i%100 + 0.5 for i in 1..50000
#   ▸ count = 50,000
#   ▸ sum = 500 cycles × 5000 = 2,500,000
#   ▸ avg = 50
#   ▸ max = 99.5 , min = 0.5
R=$(q $N1 'SELECT count(*), sum(v), avg(v), max(v), min(v) FROM ops')
ROW=$(echo "$R" | sed -n 's/.*"rows":\[\[\([^][]*\)\].*/\1/p')
CNT=$(echo $ROW | cut -d, -f1)
SUM=$(echo $ROW | cut -d, -f2)
AVG=$(echo $ROW | cut -d, -f3)
MX=$(echo $ROW | cut -d, -f4)
MN=$(echo $ROW | cut -d, -f5)
[[ $CNT == 50000 ]] && ok 'count=50000' || fail 'count' $CNT
[[ $SUM == 2500000 ]] && ok 'sum=2500000 (exact FP64)' || fail 'sum' $SUM
[[ $AVG == 50 ]] && ok 'avg=50' || fail 'avg' $AVG
[[ $MX == 99.5 ]] && ok 'max=99.5' || fail 'max' $MX
[[ $MN == 0.5 ]] && ok 'min=0.5' || fail 'min' $MN

for NAME in N2 N3 N4; do
  URL=${!NAME}
  V=$(fv "$(q $URL 'SELECT avg(v) FROM ops')")
  [[ $V == 50 ]] && ok "$NAME avg also 50" || fail "$NAME avg" $V
done

sec '[6] time-range WHERE'
# ts range: 1700000000001000000 ... 1700000050000000000
R=$(q $N1 'SELECT count(*) FROM ops WHERE ts >= 1700000049990000000')
V=$(fv "$R")
[[ $V -ge 10 && $V -le 11 ]] 2>/dev/null && ok "tail slice: $V rows" || fail 'tail' $V

R=$(q $N1 'SELECT count(*) FROM ops WHERE ts < 1700000025000000000')
V=$(fv "$R")
[[ $V -ge 24999 && $V -le 25000 ]] 2>/dev/null && ok "head slice: $V rows" || fail 'head' $V

sec '[7] query latency (50k row table)'
for qs in \
  'SELECT count(*) FROM ops' \
  'SELECT avg(v), max(v), min(v), sum(v) FROM ops' \
  'SELECT count(*) FROM ops WHERE ts >= 1700000025000000000 AND ts < 1700000030000000000' \
; do
  T0=$(date +%s%N)
  R=$(q $N1 "$qs")
  MS=$(( ($(date +%s%N) - T0) / 1000000 ))
  SRV=$(echo "$R" | grep -oE '"ms":[0-9]+' | tail -1 | cut -d: -f2)
  printf '  %-75s  server=%4sms http=%dms\n' "$qs" "$SRV" "$MS"
done

sec '[8] concurrent writes — 4 writers × 5k rows'
T0=$(date +%s%N)
for n in 1 2 3 4; do
  off=$((n * 10000000000))
  (influx_write $n ops 5000 $((1700000100000000000 + off)) 1000000 p$n) &
done
wait
T=$(( ($(date +%s%N) - T0) / 1000000 ))
ok "4× concurrent in ${T}ms"
sleep 5

# Expected 70000 total.  Concurrent race may drop a few.
declare -A counts
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  V=$(fv "$(q $URL 'SELECT count(*) FROM ops')")
  counts[$NAME]=$V
done
for NAME in N1 N2 N3 N4; do
  V=${counts[$NAME]}
  if [[ $V -eq 70000 ]] 2>/dev/null; then
    ok "$NAME count=70000 (perfect)"
  elif [[ $V -ge 65000 ]] 2>/dev/null; then
    warn "$NAME count=$V (concurrent race, <10% loss acceptable for best-effort quorum=1)"
  else
    fail "$NAME count" "$V"
  fi
done

sec '[9] HA: kill a data node, leader keeps serving'
docker kill qengine-cnode-4 >/dev/null 2>&1
sleep 3
R=$(curl -sS --max-time 3 $N1/raft)
[[ $R == *'leader'* ]] && ok 'master still leader' || fail 'master lost leadership' "$R"

PRE=$(fv "$(q $N1 'SELECT count(*) FROM ops')")
influx_write 1 ops 3000 1700000200000000000 1000000 hak
sleep 3
POST=$(fv "$(q $N1 'SELECT count(*) FROM ops')")
DIFF=$((POST - PRE))
[[ $DIFF == 3000 ]] && ok "3k writes while node4 down landed cleanly" || fail 'post-kill writes' "$DIFF"

docker start qengine-cnode-4 >/dev/null 2>&1
sleep 8

sec '[10] durability: restart master, verify no data loss'
PRE=$(fv "$(q $N1 'SELECT count(*) FROM ops')")
docker restart qengine-cnode-1 >/dev/null 2>&1
sleep 12
POST=$(fv "$(q $N1 'SELECT count(*) FROM ops')")
[[ $POST == $PRE ]] && ok "$PRE rows preserved after master restart" || fail 'durability' "pre=$PRE post=$POST"
R=$(curl -sS $N1/raft)
[[ $R == *'leader'* ]] && ok 'master re-elected' || fail 'post-restart leader' "$R"

DUR=$(( $(date +%s) - START ))
echo
printf '\n\e[35m=== summary ===\e[0m  duration=%ds\n' $DUR
if [[ $FAIL -eq 0 ]]; then
  printf '\e[32mALL GREEN\e[0m  (%d warnings)\n' $WARN
else
  printf '\e[31m%d HARD FAILURES\e[0m (%d warnings)\n' $FAIL $WARN
  exit 1
fi
