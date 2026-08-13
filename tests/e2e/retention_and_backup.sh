#!/usr/bin/env bash
set -uo pipefail

N1=http://127.0.0.1:29311
# fail() must reach the exit status.  It previously only printed, and the script
# ends on `[[ $TC == 1 ]] && ok ... || fail ...` whose status is the printf's, so
# even the last assertion failing still exited 0.
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
q() { curl -sS --max-time 30 -X POST "$1/sql" -H 'Content-Type: application/json' --data "{\"q\":\"$2\"}"; }
fv() { echo "$1" | sed -n 's/.*rows..\[\[\([0-9]*\).*/\1/p'; }

echo '=== [1] cluster up ==='
curl -sS $N1/raft | grep -qE 'leader' && ok 'raft leader' || fail 'raft' ''

echo '=== [2] retention seeded ==='
docker logs qengine-cnode-1 2>&1 | grep -iE 'retention' | tail -3

echo '=== [3] create table with old + new partitions ==='
q $N1 'CREATE TABLE tmetric (ts TIMESTAMP, v FLOAT64) TIMESTAMP(ts) WITH (PARTITION='\\''day'\\'')' > /dev/null
sleep 1

docker exec qengine-cnode-1 sh -c '
i=1
while [ $i -le 100 ]; do
  ts=$(( 1704067200000000000 + i * 1000000 ))  # 2024-01-01
  echo "tmetric v=${i}.5 $ts"
  i=$((i+1))
done > /tmp/old.txt
LEN=$(wc -c < /tmp/old.txt)
{ printf "POST /write HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n" $LEN; cat /tmp/old.txt; } | nc -w 5 127.0.0.1 28092 > /dev/null
'

NOW_NS=$(date +%s%N)
docker exec qengine-cnode-1 sh -c "
i=1
NOW=$NOW_NS
while [ \$i -le 50 ]; do
  ts=\$((\$NOW + i * 1000000))
  echo \"tmetric v=\${i}.7 \$ts\"
  i=\$((i+1))
done > /tmp/new.txt
LEN=\$(wc -c < /tmp/new.txt)
{ printf 'POST /write HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: %d\\r\\nContent-Type: text/plain\\r\\nConnection: close\\r\\n\\r\\n' \$LEN; cat /tmp/new.txt; } | nc -w 5 127.0.0.1 28092 > /dev/null
"

sleep 3
R=$(q $N1 'SELECT count(*) FROM tmetric')
V=$(fv "$R")
[[ $V == 150 ]] && ok "150 total rows" || fail 'pre-sweep count' $V

echo '=== [4] partitions before sweep ==='
docker exec qengine-cnode-1 sh -c 'ls -d /var/lib/tsdb/tmetric/*/ 2>/dev/null'

echo '=== [5] trigger sweep via /retention/sweep ==='
R=$(curl -sS -X POST $N1/retention/sweep)
echo "  $R"
[[ $R == *'partitions_deleted'* ]] && ok 'retention endpoint responded' || fail 'retention endpoint' "$R"

sleep 2
echo '=== [6] partitions after sweep ==='
docker exec qengine-cnode-1 sh -c 'ls -d /var/lib/tsdb/tmetric/*/ 2>/dev/null'
R=$(q $N1 'SELECT count(*) FROM tmetric')
V=$(fv "$R")
echo "  rows now: $V"
[[ $V -lt 150 ]] 2>/dev/null && ok "sweep removed old partitions: $V rows remaining" || fail 'no rows swept' $V

echo '=== [7] retention metrics ==='
curl -sS $N1/metrics | grep -E 'qengine_retention_' | grep -v '#'

echo '=== [8] backup endpoint ==='
SIZE=$(curl -sS -o /tmp/backup.tgz $N1/backup && wc -c < /tmp/backup.tgz)
echo "  tarball size: $SIZE bytes"
[[ $SIZE -gt 1000 ]] 2>/dev/null && ok 'backup > 1KB' || fail 'backup size' $SIZE
NF=$(tar -tzf /tmp/backup.tgz 2>/dev/null | wc -l)
ok "tarball has $NF entries"
curl -sS $N1/metrics | grep -E 'qengine_backup_' | grep -v '#'

echo '=== [9] backup restore simulation (extract tarball) ==='
mkdir -p /tmp/restore-test
rm -rf /tmp/restore-test/*
tar -xzf /tmp/backup.tgz -C /tmp/restore-test
LC=$(find /tmp/restore-test -name 'catalog*' | wc -l)
[[ $LC -ge 1 ]] 2>/dev/null && ok 'catalog file extracted' || fail 'no catalog in backup' $LC
TC=$(find /tmp/restore-test -type d -name tmetric | wc -l)
[[ $TC == 1 ]] && ok 'tmetric dir extracted' || fail 'no tmetric dir in backup' $TC
