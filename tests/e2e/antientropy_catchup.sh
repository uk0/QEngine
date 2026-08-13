#!/usr/bin/env bash
set -uo pipefail

N1=http://127.0.0.1:29311
N4=http://127.0.0.1:29314

# fail() must reach the exit status.  It previously only printed, and the script
# ended on a `curl | grep` pipeline, so a node that never converged still exited
# 0 and the run was recorded as a pass.
FAILURES=0

ok()   { printf '\e[32mPASS\e[0m  %s\n' "$1"; }
fail() { printf '\e[31mFAIL\e[0m  %s :: %s\n' "$1" "${2:-}"; FAILURES=$((FAILURES + 1)); }

finish() {
  local rc=$?
  if [[ $FAILURES -gt 0 ]]; then
    printf '\n\e[31m%d check(s) failed\e[0m\n' "$FAILURES"
    exit 1
  fi
  # Preserve an abort (set -e / set -u killed us mid-run): reporting "passed"
  # for a script that died before reaching its checks is the same lie this fix
  # exists to remove.
  if [[ $rc -ne 0 ]]; then
    printf '\n\e[31mscript aborted before finishing (status %d)\e[0m\n' "$rc"
    exit "$rc"
  fi
  printf '\n\e[32mall checks passed\e[0m\n'
  exit 0
}
trap finish EXIT

q() {
  local url=$1 sql=$2
  curl -sS --max-time 30 -X POST "$url/sql" -H 'Content-Type: application/json' \
       --data "{\"q\":\"$sql\"}"
}
fv() { echo "$1" | sed -n 's/.*rows..\[\[\([0-9]*\).*/\1/p'; }

influx_write() {
  local cnt_id=$1 table=$2 n=$3 base=$4 step=$5 host=$6
  docker exec qengine-cnode-$cnt_id sh -c "
    /tmp/gen.sh $table $n $base $step $host > /tmp/inf.txt
    LEN=\$(wc -c < /tmp/inf.txt)
    { printf 'POST /write HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: %d\\r\\nContent-Type: text/plain\\r\\nConnection: close\\r\\n\\r\\n' \$LEN; cat /tmp/inf.txt; } | nc -w 30 127.0.0.1 28092 > /dev/null
  "
}

echo '=== [1] bootstrap cluster + table ==='
q $N1 'CREATE TABLE ae (ts TIMESTAMP, v FLOAT64) TIMESTAMP(ts)' > /dev/null
sleep 1

echo '=== [2] write 5000 rows with all 4 nodes alive ==='
influx_write 1 ae 5000 1700000000000000000 1000000 phase1
sleep 3
for n in 1 2 3 4; do
  V=$(fv "$(q http://127.0.0.1:2931$n/sql 'SELECT count(*) FROM ae')")
  [[ $V == 5000 ]] && ok "node$n baseline=5000" || fail "node$n baseline" $V
done

echo '=== [3] kill node4, write 3000 more ==='
docker kill qengine-cnode-4 >/dev/null 2>&1
sleep 2
influx_write 1 ae 3000 1700100000000000000 1000000 phase2
sleep 3
for n in 1 2 3; do
  V=$(fv "$(q http://127.0.0.1:2931$n/sql 'SELECT count(*) FROM ae')")
  [[ $V == 8000 ]] && ok "node$n phase2=8000" || fail "node$n phase2" $V
done

echo '=== [4] restart node4 — triggers anti-entropy ==='
# Anti-entropy delay is 5s post-startup; set lower via env to speed test
docker exec qengine-cnode-4 sh -c 'echo ready' 2>/dev/null || true
docker start qengine-cnode-4 >/dev/null 2>&1
# Wait for auto-sync: 5s delay + pull + fan
echo '  waiting 15s for anti-entropy to run...'
sleep 15

V4=$(fv "$(q $N4 'SELECT count(*) FROM ae')")
if [[ $V4 == 8000 ]]; then
  ok "node4 auto-caught-up: 8000 rows (was 5000 before kill)"
else
  fail 'node4 anti-entropy' $V4
fi
echo '  node4 stderr:'
docker logs qengine-cnode-4 2>&1 | grep -iE 'anti-entropy' | tail -5

echo '=== [5] metrics ==='
curl -sS $N4/metrics | grep antientropy | grep -v '#'
