#!/bin/bash
curl -s -c /tmp/bck -X POST "http://127.0.0.1:29311/login" -d "user=root&pass=123456" -o /dev/null
SQL() { curl -sb /tmp/bck -X POST "http://127.0.0.1:29311/sql" -H "Content-Type: application/json" --data-binary "$1"; echo; }
PVAL() { python3 -c "import json,sys; d=json.load(sys.stdin); rows=d.get('rows',[]); print(rows[0][0] if rows else '')" 2>/dev/null; }
# A failed EXPECT must reach the exit status, or this script "verifies" nothing:
# it previously only printed a red cross and the script's status came from
# whatever ran last (a docker exec ... ls pipeline), so every run reported
# success no matter how many assertions failed.
FAILURES=0

EXPECT() {
  local label="$1"; local got="$2"; local want="$3"
  if [[ "$got" == "$want" ]]; then printf "  \033[32m✓\033[0m %s\n" "$label"
  else
    printf "  \033[31m✗\033[0m %s   got:[%s] want:[%s]\n" "$label" "$got" "$want"
    FAILURES=$((FAILURES + 1))
  fi
}

# Report the tally and exit non-zero on any failure, however the script ends.
finish() {
  local rc=$?
  if [[ $FAILURES -gt 0 ]]; then
    printf "\n\033[31m%d assertion(s) failed\033[0m\n" "$FAILURES"
    exit 1
  fi
  # Preserve an abort: a script that died before its checks ran has not passed.
  if [[ $rc -ne 0 ]]; then
    printf "\n\033[31mscript aborted before finishing (status %d)\033[0m\n" "$rc"
    exit "$rc"
  fi
  printf "\n\033[32mall assertions passed\033[0m\n"
  exit 0
}
trap finish EXIT

# Cleanup
SQL "{\"q\":\"DROP DATABASE iotfleet\"}" >/dev/null 2>&1
sleep 1

echo "=== A. Industrial fleet — 1 DB / 2 Groups / 3 STables / 30 PTables / data ==="
SQL "{\"q\":\"CREATE DATABASE iotfleet\"}" >/dev/null
SQL "{\"q\":\"CREATE GROUP gw_east IN DATABASE iotfleet\"}" >/dev/null
SQL "{\"q\":\"CREATE GROUP gw_west IN DATABASE iotfleet\"}" >/dev/null
SQL "{\"q\":\"CREATE STABLE meters_e (ts TIMESTAMP, kwh FLOAT64, status SYMBOL) TAGS (rack SYMBOL, slot INT64) IN DATABASE iotfleet GROUP gw_east\"}" >/dev/null
SQL "{\"q\":\"CREATE STABLE meters_w (ts TIMESTAMP, kwh FLOAT64, status SYMBOL) TAGS (rack SYMBOL, slot INT64) IN DATABASE iotfleet GROUP gw_west\"}" >/dev/null
SQL "{\"q\":\"CREATE STABLE temps   (ts TIMESTAMP, c FLOAT64)                    TAGS (zone SYMBOL)            IN DATABASE iotfleet GROUP gw_east\"}" >/dev/null
sleep 1

# 10 PTables per stable, varied tags
for i in $(seq 1 10); do
  SQL "{\"q\":\"CREATE TABLE me_$i USING meters_e TAGS ('rack$((i%3))', $i)\"}" >/dev/null
  SQL "{\"q\":\"CREATE TABLE mw_$i USING meters_w TAGS ('rack$((i%3))', $i)\"}" >/dev/null
  SQL "{\"q\":\"CREATE TABLE tp_$i USING temps   TAGS ('zone$((i%4))')\"}"      >/dev/null
done
sleep 2

# Write 5 rows per PT
ts=$(date +%s)000000000
for stable in me mw tp; do
  for i in $(seq 1 10); do
    body=""
    for r in $(seq 1 5); do
      if [[ $stable == "tp" ]]; then
        body+="${stable}_${i} c=$(echo "$i*0.1" | bc) $((ts + r * 1000000))"$'\n'
      else
        body+="${stable}_${i} kwh=$((i * 10)).5,status=\"on\" $((ts + r * 1000000))"$'\n'
      fi
    done
    echo "$body" | curl -s -X POST "http://127.0.0.1:29321/write" --data-binary @- > /dev/null
  done
done
sleep 3

EXPECT "iotfleet group count"  "$(SQL '{"q":"LIST GROUPS IN DATABASE iotfleet"}' | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("rows",[])))')" "2"
EXPECT "iotfleet vtable count" "$(SQL '{"q":"LIST VTABLES IN DATABASE iotfleet"}' | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("rows",[])))')" "3"
EXPECT "iotfleet ptable count (30)" "$(SQL '{"q":"LIST PTABLES IN DATABASE iotfleet"}' | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("rows",[])))')" "30"

EXPECT "STable meters_e count (10×5)" "$(SQL '{"q":"SELECT count(*) FROM meters_e"}' | PVAL)" "50"
EXPECT "STable meters_w count (10×5)" "$(SQL '{"q":"SELECT count(*) FROM meters_w"}' | PVAL)" "50"
EXPECT "STable temps count (10×5)"    "$(SQL '{"q":"SELECT count(*) FROM temps"}'    | PVAL)" "50"

echo
echo "=== B. Tag pushdown over fleet ==="
EXPECT "rack0 in meters_e"     "$(SQL "{\"q\":\"SELECT count(*) FROM meters_e WHERE rack='rack0'\"}" | PVAL)" "15"
EXPECT "slot=5 in meters_e"    "$(SQL "{\"q\":\"SELECT count(*) FROM meters_e WHERE slot=5\"}" | PVAL)" "5"
EXPECT "rack0 OR rack1"        "$(SQL "{\"q\":\"SELECT count(*) FROM meters_e WHERE rack='rack0' OR rack='rack1'\"}" | PVAL)" "35"
EXPECT "rack0 AND slot=3"      "$(SQL "{\"q\":\"SELECT count(*) FROM meters_e WHERE rack='rack0' AND slot=3\"}" | PVAL)" "5"

echo
echo "=== C. Cross-node consistency (data nodes see all 30 PTs) ==="
sleep 5
for p in 29312 29313 29314; do
  curl -s -c /tmp/c$p -X POST "http://127.0.0.1:$p/login" -d "user=root&pass=123456" -o /dev/null
  pcount=$(curl -sb /tmp/c$p -X POST "http://127.0.0.1:$p/sql" -H "Content-Type: application/json" --data-binary '{"q":"LIST PTABLES IN DATABASE iotfleet"}' | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('rows',[])))" 2>/dev/null)
  echo "  cnode :$p PT count = $pcount"
done

echo
echo "=== D. /tree JSON shape after fleet ==="
curl -sb /tmp/bck "http://127.0.0.1:29311/tree" | python3 -c "
import json, sys
t = json.load(sys.stdin)
iot = [d for d in t.get('databases',[]) if d.get('name')=='iotfleet']
gs = [g for g in t.get('groups',[]) if g.get('database')=='iotfleet']
vs = [v for v in t.get('vtables',[]) if v.get('database')=='iotfleet']
ps = [p for p in t.get('ptables',[]) if p.get('database')=='iotfleet']
print(f'  databases: {[d[\"name\"] for d in iot]}')
print(f'  groups under iotfleet: {sorted(g[\"name\"] for g in gs)}')
print(f'  vtables under iotfleet: {sorted(v[\"name\"] for v in vs)}')
print(f'  ptables under iotfleet: total={len(ps)}; first 3 with parents: {[(p[\"name\"], p[\"vtable\"], p[\"group\"]) for p in ps[:3]]}')
"

echo
echo "=== E. DESCRIBE / SHOW for introspection ==="
SQL "{\"q\":\"DESCRIBE STABLE meters_e\"}" | head -c 200; echo
SQL "{\"q\":\"DESCRIBE TABLE me_1\"}"      | head -c 200; echo
SQL "{\"q\":\"DESCRIBE me_1\"}"             | head -c 200; echo
SQL "{\"q\":\"SHOW CREATE TABLE me_1\"}"   | head -c 200; echo

echo
echo "=== F. Cleanup & final cascade ==="
SQL "{\"q\":\"DROP DATABASE iotfleet\"}" >/dev/null
sleep 2
EXPECT "iotfleet gone"      "$(SQL '{"q":"LIST DATABASES"}' | python3 -c 'import json,sys; print("iotfleet" in [r[0] for r in json.load(sys.stdin).get("rows",[])])')" "False"
EXPECT "all groups gone"    "$(SQL '{"q":"LIST GROUPS IN DATABASE iotfleet"}' | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("rows",[])))')" "0"
docker exec qengine-cnode-1 ls -d /var/lib/tsdb/me_1 /var/lib/tsdb/mw_1 /var/lib/tsdb/tp_1 2>&1 | head -3
