#!/bin/bash
# selfheal_50.sh — exercise the cnode-3 catalog self-heal path.
#
# Phase 1: with all 4 nodes ALIVE, create a fresh DB + group + stable +
#          50 ptables through master cnode-1.  Verify all 4 nodes see
#          50/50 children for the new DB (broadcast path).
# Phase 2: stop cnode-3, recreate the same shape under a different DB
#          name, restart cnode-3, wait for the polling self-heal to
#          fire, then verify cnode-3 catches up to the new ptables
#          without any further manual intervention.
#
# Run from anywhere — script ssh's into root@10.88.51.102.

set -euo pipefail

REMOTE="${REMOTE_HOST:-root@10.88.51.102}"
TS=$(date +%s)
DB1="selfheal_${TS}_a"
DB2="selfheal_${TS}_b"
PTABLES=50

# Only cnode-1 (port 29311) accepts writes through the master path; the
# read-side /catalog/check works on every node.
NODES=(29311:cnode-1 29312:cnode-2 29313:cnode-3 29314:cnode-4)

say() { printf "\e[36m[selfheal]\e[0m %s\n" "$*"; }
ok()  { printf "  \e[32m✓\e[0m %s\n" "$*"; }
bad() { printf "  \e[31m✗\e[0m %s\n" "$*"; }

ssh_master() {
  ssh -n "$REMOTE" "$@"
}

login_all() {
  ssh_master 'for p in 29311 29312 29313 29314; do
    rm -f /tmp/sh$p
    curl -s -c /tmp/sh$p -X POST http://127.0.0.1:$p/login -d "user=root&pass=123456" -o /dev/null
  done'
}

sql_master() {
  local q="$1"
  ssh_master "curl -sb /tmp/sh29311 -X POST -H 'Content-Type: application/json' \
    --data '{\"q\":\"$q\"}' http://127.0.0.1:29311/sql"
}

children_count() {
  local port="$1"
  ssh_master "curl -sb /tmp/sh$port http://127.0.0.1:$port/catalog/check" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["summary"]["children"])'
}

create_50_ptables() {
  local db="$1"
  # Child table names are globally unique across the catalog, so we
  # need a per-run prefix or successive runs collide with each other's
  # leftover tables.  Use the timestamp-derived db name as the prefix.
  local prefix="${db//[^a-z0-9_]/}"  # sanitise for SQL identifiers
  say "DDL: CREATE DATABASE $db + GROUP + STABLE + $PTABLES ptables (prefix=${prefix}_pt_)"
  # Build the entire DDL burst inside one ssh session so we don't pay
  # the ssh handshake cost 50 times and don't trip MaxStartups.  Per-
  # table failures are surfaced via the counter — match both ERR and
  # error spellings since tsdb's status rows use uppercase ERR:.
  local stable="st_${prefix}"
  ssh -n "$REMOTE" "
    set +e
    curl -sb /tmp/sh29311 -X POST -H 'Content-Type: application/json' \
      --data '{\"q\":\"CREATE DATABASE $db\"}' http://127.0.0.1:29311/sql >/dev/null
    curl -sb /tmp/sh29311 -X POST -H 'Content-Type: application/json' \
      --data '{\"q\":\"CREATE GROUP g1 IN DATABASE $db\"}' http://127.0.0.1:29311/sql >/dev/null
    curl -sb /tmp/sh29311 -X POST -H 'Content-Type: application/json' \
      --data '{\"q\":\"CREATE STABLE $stable (ts TIMESTAMP, v FLOAT64) TAGS (slot INT64) IN DATABASE $db GROUP g1\"}' http://127.0.0.1:29311/sql >/dev/null
    fail=0
    for i in \$(seq 1 $PTABLES); do
      n=\$(printf %03d \$i)
      r=\$(curl -sb /tmp/sh29311 -X POST -H 'Content-Type: application/json' \
            --data \"{\\\"q\\\":\\\"CREATE TABLE ${prefix}_pt_\${n} USING $stable TAGS (\$i)\\\"}\" \
            http://127.0.0.1:29311/sql)
      case \"\$r\" in
        *ERR:*|*error*) fail=\$((fail+1)) ;;
      esac
    done
    echo \"ddl-failures=\$fail\"
  "
  say "DDL submitted; sleeping 5s for broadcast to settle"
  sleep 5
}

snapshot_counts() {
  local label="$1"
  printf "  %-22s" "$label:"
  for entry in "${NODES[@]}"; do
    p="${entry%%:*}"; n="${entry##*:}"
    c=$(children_count "$p" || echo "?")
    printf "  %s=%s" "$n" "$c"
  done
  echo
}

# ─────────────────────────────────────────────────────────────────────
# Phase 1 — happy path: all nodes alive, verify broadcast reaches cnode-3
# ─────────────────────────────────────────────────────────────────────
say "── Phase 1 — all nodes alive ──"
login_all
before1=$(children_count 29311)
say "baseline children on cnode-1 (master): $before1"
create_50_ptables "$DB1"
snapshot_counts "after $DB1 creation"

ok_phase1=1
for entry in "${NODES[@]}"; do
  p="${entry%%:*}"; n="${entry##*:}"
  c=$(children_count "$p")
  expected=$((before1 + PTABLES))
  if [[ "$c" -ge "$expected" ]]; then
    ok "$n reached $c (≥ $expected)"
  else
    bad "$n stuck at $c (expected ≥ $expected)"
    ok_phase1=0
  fi
done

if [[ "$ok_phase1" -ne 1 ]]; then
  bad "Phase 1 broadcast did not reach all nodes — aborting before phase 2"
  exit 1
fi

# ─────────────────────────────────────────────────────────────────────
# Phase 2 — cnode-3 goes down during DDL burst, must self-heal on boot
# ─────────────────────────────────────────────────────────────────────
say "── Phase 2 — stop cnode-3, run DDL, restart cnode-3 ──"
say "stopping cnode-3 …"
ssh_master 'docker stop -t 5 qengine-cnode-3 >/dev/null'
sleep 3

before2=$(children_count 29311)
say "baseline children on cnode-1: $before2"
create_50_ptables "$DB2"

# Sanity check — cnode-1/2/4 should already have the new entries.
say "checking cnode-1/2/4 picked up the burst …"
for p in 29311 29312 29314; do
  c=$(children_count "$p")
  expected=$((before2 + PTABLES))
  if [[ "$c" -ge "$expected" ]]; then ok "node :$p reached $c (≥ $expected)"
  else bad "node :$p stuck at $c"; fi
done

say "starting cnode-3 + waiting 75s for catalog self-heal polling to fire …"
ssh_master 'docker start qengine-cnode-3 >/dev/null'
sleep 75

# After self-heal, cnode-3 must have at least the same children count
# as the master (it may have more from older drift, that's fine).
login_all
c3_after=$(children_count 29313)
master_after=$(children_count 29311)
if [[ "$c3_after" -ge "$master_after" ]]; then
  ok "cnode-3 reached $c3_after children (master has $master_after) — self-heal works"
else
  bad "cnode-3 stuck at $c3_after, master at $master_after — self-heal failed"
  say "cnode-3 catalog-sync log lines:"
  ssh_master 'docker logs --tail 200 qengine-cnode-3 2>&1 | grep -E "catalog-sync|catalog dump|self-heal" | tail -20'
  exit 1
fi

snapshot_counts "final state"
say "DONE — all 4 nodes converged on $DB1 and $DB2 catalog DDL"
