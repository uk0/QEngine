#!/usr/bin/env bash
# s02 — CREATE DATABASE on the leader is visible on all followers after ACK.

set -euo pipefail
source "$(dirname "$0")/../lib.sh"
trap raft_down EXIT

raft_up
leader=$(raft_wait_leader 2)

# Find which node index holds the leader id.
leader_idx=""
for i in 1 2 3; do
  id=$(raft_info_on "$i" | jq -r '.self_id // ""')
  [[ "$id" == "$leader" ]] && { leader_idx=$i; break; }
done
[[ -n "$leader_idx" ]] || fail "could not map leader_id ${leader} to a node"

say "leader=node${leader_idx}"

# Write via the leader.
resp=$(sql_on "$leader_idx" 'CREATE DATABASE s02_db')
echo "$resp" | jq -e '.rows[0][0] == "OK: database created"' >/dev/null \
  || fail "leader refused CREATE: $resp"

# Every node — including followers — must now LIST it back.
for i in 1 2 3; do
  dbs=$(sql_on "$i" 'LIST DATABASES' | jq -r '[.rows[][0]] | join(",")')
  [[ "$dbs" == *"s02_db"* ]] \
    || fail "node${i} does not have s02_db: dbs=[${dbs}]"
  say "  node${i} dbs: ${dbs}"
done

pass "s02 ddl_replicates"
