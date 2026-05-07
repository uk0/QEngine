#!/usr/bin/env bash
# backup_restore.sh — round-trip verifier for the /backup endpoint.
#
# Pre-fix /backup shipped a tar.gz of data_dir but had no way to prove
# the restored copy contained the same rows as the source.  Now /backup
# emits _backup_manifest.json with per-table (count, max_ts) just before
# the tar runs, so a verifier can:
#
#   1. Pull the tarball.
#   2. Extract _backup_manifest.json.
#   3. Re-query each listed table on the live (or restored) cluster.
#   4. Diff manifest count vs live count.
#
# Step 4 fails the test when any table's row count diverges — the
# canonical "did we lose data" signal an enterprise pilot will want.
#
# This script does steps 1–4 against the live lvm1 cluster + a fresh
# read against the same node.  Full restore-into-empty-dir is a follow-
# up: needs an ephemeral docker node to swap data_dirs without touching
# the prod cluster.

set -u
SSH=${SSH:-ssh root@10.88.51.102}
BASE=${BASE:-http://10.88.51.102:29311}

PASS=0; FAIL=0
say()  { printf '\e[36m[backup]\e[0m %s\n' "$*"; }
ok()   { printf '  \e[32m✓\e[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '  \e[31m✗\e[0m %s\n' "$*"; FAIL=$((FAIL+1)); }

WORK=$(mktemp -d -t tsdb-backup-rt-XXXXX)
trap "rm -rf $WORK" EXIT

# ── Phase 0: auth ────────────────────────────────────────────────
say "phase 0: login"
CK=$WORK/ck
curl -s -c $CK -X POST "$BASE/login" -d "user=root&pass=123456" -o /dev/null
curl -s -b $CK -X POST "$BASE/sql" \
     -H 'Content-Type: application/json' \
     --data-binary '{"q":"DROP TABLE bk_rt_demo"}' >/dev/null 2>&1

# ── Phase 1: seed a known table ─────────────────────────────────
say "phase 1: seed bk_rt_demo with 100 rows"
curl -s -b $CK -X POST "$BASE/sql" \
     -H 'Content-Type: application/json' \
     --data-binary '{"q":"CREATE TABLE bk_rt_demo (ts TIMESTAMP, v INT64) TIMESTAMP(ts)"}' \
     >/dev/null
sleep 1
ts=$($SSH "date +%s")
ts_ns=$((ts * 1000000000))
> $WORK/seed.txt
for r in $(seq 1 100); do
  echo "bk_rt_demo v=${r}i $((ts_ns + r * 1000))" >> $WORK/seed.txt
done
$SSH "cat > /tmp/bk_rt_demo.txt" < $WORK/seed.txt
$SSH "curl -s -X POST 'http://127.0.0.1:29321/write' --data-binary @/tmp/bk_rt_demo.txt > /dev/null"
sleep 2

# Note the live count for cross-check after backup.
live_count=$(curl -s -b $CK -X POST "$BASE/sql" \
             -H 'Content-Type: application/json' \
             --data-binary '{"q":"SELECT count(*) FROM bk_rt_demo"}' \
             | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["rows"][0][0])' 2>/dev/null)
if [[ "$live_count" == "100" ]]; then
  ok "seed live count = 100"
else
  bad "seed live count got=$live_count want=100"
  echo "PASS: $PASS  FAIL: $FAIL"; exit 1
fi

# ── Phase 2: pull /backup ───────────────────────────────────────
say "phase 2: GET /backup"
curl -s -b $CK -o $WORK/backup.tgz "$BASE/backup"
if [[ ! -s $WORK/backup.tgz ]]; then
  bad "/backup returned empty body"
  echo "PASS: $PASS  FAIL: $FAIL"; exit 1
fi
sz=$(wc -c < $WORK/backup.tgz)
ok "/backup streamed $sz bytes"

# ── Phase 3: extract + locate manifest ──────────────────────────
say "phase 3: extract tar + parse _backup_manifest.json"
mkdir -p $WORK/restore
tar -xzf $WORK/backup.tgz -C $WORK/restore
manifest=$(find $WORK/restore -name "_backup_manifest.json" | head -1)
if [[ -z "$manifest" || ! -s "$manifest" ]]; then
  bad "manifest not found inside tarball"
  echo "PASS: $PASS  FAIL: $FAIL"; exit 1
fi
ok "manifest at ${manifest#$WORK/restore/} ($(wc -c < $manifest) bytes)"

# ── Phase 4: verify manifest fields ─────────────────────────────
say "phase 4: parse + sanity-check manifest fields"
fmt_ver=$(python3 -c "import json; print(json.load(open('$manifest'))['format_version'])")
[[ "$fmt_ver" == "1" ]] && ok "format_version = 1" || bad "format_version got=$fmt_ver"

tcount=$(python3 -c "import json; print(json.load(open('$manifest'))['table_count'])")
[[ "$tcount" -ge 1 ]] && ok "table_count = $tcount" || bad "table_count = $tcount (want ≥1)"

trunc=$(python3 -c "import json; print(json.load(open('$manifest'))['truncated'])")
[[ "$trunc" == "False" ]] && ok "truncated = false" || bad "truncated = $trunc"

# ── Phase 5: round-trip diff — manifest vs live ─────────────────
say "phase 5: compare manifest counts vs live counts (per table)"
mismatches=0
total=0
while IFS=$'\t' read -r tbl mcount; do
  total=$((total+1))
  live=$(curl -s -b $CK -X POST "$BASE/sql" \
         -H 'Content-Type: application/json' \
         --data-binary "{\"q\":\"SELECT count(*) FROM $tbl\"}" \
         | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("rows",[[0]])[0][0])' 2>/dev/null)
  if [[ "$live" != "$mcount" ]]; then
    mismatches=$((mismatches+1))
    echo "    ✗ $tbl: manifest=$mcount  live=$live"
  fi
done < <(python3 -c "
import json
m = json.load(open('$manifest'))
for t in m['tables']:
    if t.get('ok'):
        print(f\"{t['name']}\t{t['count']}\")")

if (( mismatches == 0 )); then
  ok "all $total tables: manifest count == live count"
else
  bad "$mismatches / $total tables diverged manifest vs live"
fi

# Specifically check our seeded table's bookkeeping survived round-trip.
demo_mcount=$(python3 -c "
import json
m = json.load(open('$manifest'))
for t in m['tables']:
    if t['name'] == 'bk_rt_demo':
        print(t['count']); break
" 2>/dev/null)
if [[ "$demo_mcount" == "100" ]]; then
  ok "bk_rt_demo manifest count = 100"
else
  bad "bk_rt_demo manifest count got=$demo_mcount want=100"
fi

# ── Cleanup ────────────────────────────────────────────────────
curl -s -b $CK -X POST "$BASE/sql" \
     -H 'Content-Type: application/json' \
     --data-binary '{"q":"DROP TABLE bk_rt_demo"}' >/dev/null 2>&1
$SSH "rm -f /tmp/bk_rt_demo.txt"

echo
say "────────────────────────────────────────────"
say "PASS: $PASS  FAIL: $FAIL"
[[ $FAIL -eq 0 ]]
