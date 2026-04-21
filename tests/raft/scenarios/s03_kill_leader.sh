#!/usr/bin/env bash
# s03 — kill the current leader; within 5 s a survivor is the new leader
# and accepts a fresh DDL that replicates to the other survivor.

set -euo pipefail
source "$(dirname "$0")/../lib.sh"
trap raft_down EXIT

raft_up
leader=$(raft_wait_leader 5)

leader_idx=""
for i in 1 2 3; do
  id=$(raft_info_on "$i" | jq -r '.self_id // ""')
  [[ "$id" == "$leader" ]] && { leader_idx=$i; break; }
done
[[ -n "$leader_idx" ]] || fail "could not map leader id ${leader}"

say "killing leader container (node${leader_idx})"
docker kill "raft-node-${RAFT_PORT_BASE}-${leader_idx}" >/dev/null

# Wait up to 5 s for a NEW leader whose id != old one.
deadline=$(( $(date +%s) + 5 ))
new_leader=""
while (( $(date +%s) < deadline )); do
  for i in 1 2 3; do
    [[ "$i" == "$leader_idx" ]] && continue
    seen=$(raft_info_on "$i" | jq -r '.leader_id // ""' 2>/dev/null || echo "")
    if [[ -n "$seen" && "$seen" != "$leader" && "$seen" != "0" ]]; then
      new_leader="$seen"; break 2
    fi
  done
  sleep 0.2
done
[[ -n "$new_leader" ]] || fail "no new leader within 5 s"

new_idx=""
for i in 1 2 3; do
  [[ "$i" == "$leader_idx" ]] && continue
  id=$(raft_info_on "$i" | jq -r '.self_id // ""')
  [[ "$id" == "$new_leader" ]] && { new_idx=$i; break; }
done
[[ -n "$new_idx" ]] || fail "new_leader ${new_leader} does not map to a surviving node"

say "new leader=node${new_idx} (id=${new_leader})"

# Write via new leader.  Same tolerance as s02 — accept any OK status.
resp=$(sql_on "$new_idx" 'CREATE DATABASE s03_db')
echo "$resp" | jq -e '.rows[0][0] | startswith("OK")' >/dev/null \
  || fail "new leader refused CREATE: $resp"

# Both surviving nodes must see it.  Raft commit on the leader
# completes before the follower has necessarily applied, so give
# the apply thread a couple of heartbeats to catch up.
for i in 1 2 3; do
  [[ "$i" == "$leader_idx" ]] && continue
  seen=""
  for _ in 1 2 3 4 5; do
    dbs=$(sql_on "$i" 'LIST DATABASES' | jq -r '[.rows[][0]] | join(",")')
    if [[ "$dbs" == *"s03_db"* ]]; then seen=1; break; fi
    sleep 0.3
  done
  [[ -n "$seen" ]] || fail "survivor node${i} missing s03_db (dbs=${dbs})"
done

pass "s03 kill_leader"
