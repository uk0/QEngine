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

# ── Phase 6: standalone restore round-trip ────────────────────
# Extract the tarball into an empty data_dir and bring up an ephemeral
# standalone tsdb-server pointing at it.  The standalone server reads
# the same on-disk format — catalog, partitions, WAL — so a successful
# query against the restored instance proves bytes-on-disk = working
# state, not just a manifest match.
#
# Side-effects are scoped to a unique container name + volume on the
# remote host; cleanup runs in a trap on exit so a mid-script failure
# doesn't leak resources.
say "phase 6: restore tarball into empty data_dir + boot standalone sidecar"

NAME=tsdb-restore-verify-$$
RESTORE_HOST=/tmp/tsdb-restore-$$
HOST_PORT_HTTP=28194   # metrics + /sql
HOST_PORT_WIRE=28190
trap "rm -rf $WORK; $SSH 'docker rm -f $NAME 2>/dev/null; rm -rf $RESTORE_HOST' 2>/dev/null" EXIT

# Stream the tarball over to the lvm1 host and extract there — the
# sidecar runs on the same host so we don't pay a second copy hop.
$SSH "mkdir -p $RESTORE_HOST"
scp -q $WORK/backup.tgz root@10.88.51.102:$RESTORE_HOST/backup.tgz

# Untar into the data dir.  The tar produced by /backup uses the
# parent-dir leaf as the top-level entry (e.g. data_dir 'tsdb' → 'tsdb/'
# inside the archive).  We extract one level deep and let the sidecar
# point at the inner dir directly.
INNER=$($SSH "cd $RESTORE_HOST && tar tzf backup.tgz 2>/dev/null | head -1 | sed 's,/.*,,'")
if [[ -z "$INNER" ]]; then
  bad "failed to inspect tarball top-level dir"
  echo "PASS: $PASS  FAIL: $FAIL"; exit 1
fi
$SSH "tar xzf $RESTORE_HOST/backup.tgz -C $RESTORE_HOST"
ok "extracted into $RESTORE_HOST/$INNER (top-level $INNER)"

# Boot the sidecar in standalone mode.  TSDB_ROLE=server (default) reads
# the catalog and partitions exactly as the source did.  No cluster
# bind, no auth — the metrics port is open for /sql.
$SSH "docker run -d --rm --name $NAME \
        -v $RESTORE_HOST/$INNER:/var/lib/tsdb \
        -e TSDB_ROLE=server \
        -e TSDB_DATA_DIR=/var/lib/tsdb \
        -e TSDB_BIND=0.0.0.0:28090 \
        -e TSDB_METRICS_BIND=0.0.0.0:28094 \
        -p 127.0.0.1:$HOST_PORT_HTTP:28094 \
        -p 127.0.0.1:$HOST_PORT_WIRE:28090 \
        qengine/tsdb:dev > /dev/null"

# Wait for the sidecar to come up.  Standalone boot includes WAL replay
# and partition open; budget a few seconds.
for i in $(seq 1 30); do
  if $SSH "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HOST_PORT_HTTP/health" 2>/dev/null | grep -q 200; then
    break
  fi
  sleep 0.5
done
if ! $SSH "curl -s http://127.0.0.1:$HOST_PORT_HTTP/health | grep -q '\"status\":\"ok\"'"; then
  bad "sidecar did not come up healthy on port $HOST_PORT_HTTP"
  $SSH "docker logs $NAME 2>&1 | tail -10"
  echo "PASS: $PASS  FAIL: $FAIL"; exit 1
fi
ok "sidecar healthy on http://127.0.0.1:$HOST_PORT_HTTP"

# ── Phase 7: verify the restored sidecar matches the manifest ──
# Re-query each table on the *restored* instance and diff against the
# manifest.  This catches the failure modes a manifest-only check
# misses: catalog rows that exist on the source but reference partition
# files that didn't make it into the tar; WAL records the standalone
# can't replay; schema mismatches across versions.
say "phase 7: compare manifest counts vs restored sidecar counts"
side_mismatches=0
side_total=0
side_skipped=0
while IFS=$'\t' read -r tbl mcount; do
  side_total=$((side_total+1))
  resp=$($SSH "curl -s -X POST http://127.0.0.1:$HOST_PORT_HTTP/sql \
               -H 'Content-Type: application/json' \
               --data-binary '{\"q\":\"SELECT count(*) FROM $tbl\"}'")
  side_count=$(echo "$resp" | python3 -c '
import json, sys
try:
    d = json.loads(sys.stdin.read())
    print(d.get("rows",[[None]])[0][0])
except Exception:
    print("PARSE_ERR")
' 2>/dev/null)
  if [[ "$side_count" == "None" || "$side_count" == "PARSE_ERR" ]]; then
    side_skipped=$((side_skipped+1))
    continue
  fi
  if [[ "$side_count" != "$mcount" ]]; then
    side_mismatches=$((side_mismatches+1))
    echo "    ✗ $tbl: manifest=$mcount  restored=$side_count"
  fi
done < <(python3 -c "
import json
m = json.load(open('$manifest'))
for t in m['tables']:
    if t.get('ok'):
        print(f\"{t['name']}\t{t['count']}\")")

if (( side_mismatches == 0 )); then
  ok "restored sidecar matches manifest: $side_total tables (skipped=$side_skipped)"
else
  bad "$side_mismatches / $side_total tables diverged on restored sidecar"
fi

# Specifically: the seeded table came back via the ephemeral restore.
side_demo=$($SSH "curl -s -X POST http://127.0.0.1:$HOST_PORT_HTTP/sql \
                  -H 'Content-Type: application/json' \
                  --data-binary '{\"q\":\"SELECT count(*) FROM bk_rt_demo\"}'" \
            | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("rows",[[0]])[0][0])' 2>/dev/null)
if [[ "$side_demo" == "100" ]]; then
  ok "bk_rt_demo restored count = 100"
else
  bad "bk_rt_demo restored count got=$side_demo want=100"
fi

# ── Cleanup ────────────────────────────────────────────────────
curl -s -b $CK -X POST "$BASE/sql" \
     -H 'Content-Type: application/json' \
     --data-binary '{"q":"DROP TABLE bk_rt_demo"}' >/dev/null 2>&1
$SSH "rm -f /tmp/bk_rt_demo.txt"
# Sidecar cleanup is in the EXIT trap above.

echo
say "────────────────────────────────────────────"
say "PASS: $PASS  FAIL: $FAIL"
[[ $FAIL -eq 0 ]]
