#!/usr/bin/env bash
set -uo pipefail

N1=http://127.0.0.1:29311
N2=http://127.0.0.1:29312
N3=http://127.0.0.1:29313
N4=http://127.0.0.1:29314

# login per node for auth
declare -A COOKIE
for url in $N1 $N2 $N3 $N4; do
  jar=/tmp/tsdb.vp.$(echo $url | tr -d ':/')
  curl -sS -c $jar -o /dev/null -X POST -d 'user=root&pass=123456' $url/login 2>/dev/null || true
  COOKIE[$url]=$jar
done

FAIL=0
ok()   { printf '\e[32mPASS\e[0m  %s\n' "$1"; }
fail() { printf '\e[31mFAIL\e[0m  %s :: %s\n' "$1" "${2:-}"; FAIL=$((FAIL+1)); }
sec()  { printf '\n\e[36m=== %s ===\e[0m\n' "$1"; }

q() {
  local url=$1 sql=$2
  local esc=$(printf '%s' "$sql" | sed 's/\\/\\\\/g; s/"/\\"/g')
  curl -sS -b ${COOKIE[$url]:-/dev/null} --max-time 15 -X POST "$url/sql" \
       -H 'Content-Type: application/json' --data "{\"q\":\"$esc\"}"
}
fv() { echo "$1" | sed -n 's/.*rows..\[\[\([0-9.eE+-]*\).*/\1/p'; }

sec '[1] 4-layer: DATABASE'
R=$(q $N1 'CREATE DATABASE iot')
[[ $R == *'committed via raft'* ]] && ok 'CREATE DATABASE iot' || fail 'create db' "$R"
sleep 2
# all 4 nodes see database
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  R=$(q $URL 'LIST DATABASES')
  [[ $R == *'"iot"'* ]] && ok "$NAME sees iot DB" || fail "$NAME iot DB" "$R"
done

sec '[2] 4-layer: GROUP'
R=$(q $N1 'CREATE GROUP factory_a IN DATABASE iot')
[[ $R == *'committed via raft'* ]] && ok 'CREATE GROUP' || fail 'create group' "$R"
sleep 2
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  R=$(q $URL 'LIST GROUPS')
  [[ $R == *'factory_a'* ]] && ok "$NAME sees factory_a" || fail "$NAME factory_a" "$R"
done

sec '[3] 4-layer: VTABLE (super-table aka STable)'
R=$(q $N1 'CREATE STABLE meters (ts TIMESTAMP, volt FLOAT64) TAGS (region SYMBOL, unit INT64)')
echo "  resp: $R" | head -c 160; echo
[[ $R == *'OK'* || $R == *'committed'* ]] && ok 'CREATE STABLE meters' || fail 'create stable' "$R"
sleep 2
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  R=$(q $URL 'LIST VTABLES')
  [[ $R == *'meters'* ]] && ok "$NAME sees meters STable" || fail "$NAME meters stable" "$R"
done

sec '[4] 4-layer: PTABLE (child table)'
R=$(q $N1 "CREATE TABLE d1 USING meters TAGS ('east', 42)")
[[ $R == *'OK'* || $R == *'committed'* ]] && ok 'CREATE child d1' || fail 'create child d1' "$R"
R=$(q $N1 "CREATE TABLE d2 USING meters TAGS ('west', 43)")
[[ $R == *'OK'* || $R == *'committed'* ]] && ok 'CREATE child d2' || fail 'create child d2' "$R"
sleep 2
# LIST PTABLES USING meters on every node
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  R=$(q $URL 'LIST PTABLES USING meters')
  C=$(echo "$R" | grep -oE '"nrows":[0-9]+' | cut -d: -f2)
  [[ $C == 2 ]] && ok "$NAME PTables under meters = 2 (d1+d2)" || fail "$NAME child count" "$R"
done

sec '[5] INSERT + SELECT against PTable'
# insert via node1 influx using table name d1
docker exec qengine-cnode-1 sh -c '
/tmp/gen_col.sh d1 volt 100 1700000000000000000 1000000 > /tmp/inf.txt
LEN=$(wc -c < /tmp/inf.txt)
{ printf "POST /write HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n" $LEN; cat /tmp/inf.txt; } | nc -w 5 127.0.0.1 28092 > /dev/null
'
sleep 3
V=$(fv "$(q $N1 'SELECT count(*) FROM d1')")
[[ $V == 100 ]] && ok "100 rows in d1 (via direct ptable SELECT)" || fail 'd1 count' $V

# Query from data node (N3)
V=$(fv "$(q $N3 'SELECT count(*) FROM d1')")
[[ $V == 100 ]] && ok "data-node N3 SELECT d1 = 100" || fail 'N3 d1' $V

sec '[6] /tree renders 4-layer hierarchy'
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  T=$(curl -sS -b ${COOKIE[$URL]:-/dev/null} $URL/tree 2>/dev/null)
  # Expect: databases contains 'iot'; groups contains 'factory_a'; vtables contains 'meters'; ptables contains 'd1'+'d2'
  HAS_DB=$(echo "$T" | grep -oE '"name":"iot"' | head -1)
  HAS_GRP=$(echo "$T" | grep -oE '"name":"factory_a"' | head -1)
  HAS_VT=$(echo "$T" | grep -oE '"name":"meters"' | head -1)
  HAS_PT=$(echo "$T" | grep -oE '"name":"d[12]"' | sort -u | wc -l | tr -d ' ')
  if [[ -n $HAS_DB && -n $HAS_GRP && -n $HAS_VT && $HAS_PT -ge 2 ]]; then
    ok "$NAME tree: iot > factory_a > meters > d1+d2"
  else
    fail "$NAME tree incomplete" "db=$HAS_DB grp=$HAS_GRP vt=$HAS_VT pt=$HAS_PT"
  fi
done

sec '[7] DROP propagates'
q $N1 'DROP TABLE d1' > /dev/null
q $N1 'DROP TABLE d2' > /dev/null
q $N1 'DROP STABLE meters' > /dev/null
q $N1 'DROP GROUP factory_a' > /dev/null
q $N1 'DROP DATABASE iot' > /dev/null
sleep 2
for NAME in N1 N2 N3 N4; do
  URL=${!NAME}
  R=$(q $URL 'LIST DATABASES')
  [[ $R != *'"iot"'* ]] && ok "$NAME iot gone" || fail "$NAME iot still present"
done

echo
[[ $FAIL -eq 0 ]] && printf '\e[32mALL GREEN\e[0m\n' || printf '\e[31m%d FAILURES\e[0m\n' $FAIL
exit $((FAIL > 0 ? 1 : 0))
