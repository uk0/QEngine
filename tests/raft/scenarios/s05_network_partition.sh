#!/usr/bin/env bash
# s05 — 1 minority node, 2 majority nodes.
#
# Sequence:
#   1. disconnect node X from the raft bridge network
#   2. majority side keeps electing and accepts a CREATE
#   3. isolated node can't reach anyone: its propose times out or it
#      returns "not leader" with an empty leader_hint (leader unknown).
#   4. reconnect
#   5. isolated node catches up; its LIST matches the majority

set -euo pipefail
source "$(dirname "$0")/../lib.sh"
trap cleanup EXIT

cleanup() {
  # Belt-and-suspenders: reconnect before tear-down so the orphan
  # container can shut down cleanly.
  if [[ -n "${ISO_CONT:-}" ]]; then
    docker network connect "$NET_NAME" "$ISO_CONT" >/dev/null 2>&1 || true
  fi
  raft_down
}

raft_up
leader=$(raft_wait_leader 5)

# Pick a non-leader node to isolate.
ISO_IDX=""
for i in 1 2 3; do
  id=$(raft_info_on "$i" | jq -r '.self_id // ""')
  if [[ "$id" != "$leader" ]]; then ISO_IDX=$i; break; fi
done
ISO_CONT="raft-node-${RAFT_PORT_BASE}-${ISO_IDX}"
say "isolating node${ISO_IDX} (${ISO_CONT})"
docker network disconnect "$NET_NAME" "$ISO_CONT"

# Give SWIM a moment to notice.
sleep 2

# Find a majority-side node that's still happy (leader might have
# stepped down if it was the one that got partitioned, but we picked
# a non-leader so leader should remain).
MAJ_IDX=""
for i in 1 2 3; do
  [[ "$i" == "$ISO_IDX" ]] && continue
  role=$(raft_info_on "$i" | jq -r '.role // ""' 2>/dev/null || echo "?")
  if [[ "$role" == "leader" ]]; then MAJ_IDX=$i; break; fi
done
[[ -n "$MAJ_IDX" ]] || fail "majority side lost its leader too"
say "majority leader after partition = node${MAJ_IDX}"

# Majority write.
resp=$(sql_on "$MAJ_IDX" 'CREATE DATABASE majority_db')
echo "$resp" | jq -e '.rows[0][0] | startswith("OK")' >/dev/null \
  || fail "majority side refused CREATE: $resp"

# Isolated side: the container's ports are still published on the
# host because docker maps ports via iptables not via the bridge,
# so the /sql endpoint is reachable.  The propose will time out (5 s
# default) because the state machine can't reach quorum.  We expect
# either an error row or a "not leader" row — never "OK".
iso_port=$(_dport "$ISO_IDX")
iso_resp=$(curl -s --max-time 8 -X POST "http://127.0.0.1:${iso_port}/sql" \
             -H 'Content-Type: application/json' \
             -d '{"q":"CREATE DATABASE isolated_db"}' || echo '{}')
say "  isolated /sql response: ${iso_resp}"
row0=$(echo "$iso_resp" | jq -r '.rows[0][0] // "none"' 2>/dev/null || echo "none")
if [[ "$row0" == "OK: committed via raft" ]]; then
  fail "isolated node incorrectly committed: ${iso_resp}"
fi

# Reconnect.
say "reconnecting node${ISO_IDX}"
docker network connect "$NET_NAME" "$ISO_CONT" >/dev/null

# Wait for the isolated side to see majority_db.
seen=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  dbs=$(curl -sf --max-time 2 -X POST "http://127.0.0.1:${iso_port}/sql" \
         -H 'Content-Type: application/json' \
         -d '{"q":"LIST DATABASES"}' \
       | jq -r '[.rows[][0]] | sort | join(",")' 2>/dev/null || echo "")
  if [[ "$dbs" == *"majority_db"* ]]; then seen=1; break; fi
  sleep 0.5
done
[[ -n "$seen" ]] || fail "isolated node didn't catch up: dbs=${dbs}"

pass "s05 network_partition"
