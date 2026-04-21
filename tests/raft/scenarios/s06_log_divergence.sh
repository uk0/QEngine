#!/usr/bin/env bash
# s06 — force a log divergence scenario and verify the new leader's
# AppendEntries truncates the stale follower's tail.
#
# Recipe:
#   1. elect a leader in term N
#   2. partition the leader away from both followers
#   3. followers can't elect (odd behaviour in a 3-node cluster where
#      majority = 2 and we have 2 live nodes, so they CAN elect —
#      that's what makes the bug visible).  They elect a new leader
#      in term N+1 and commit a CREATE there.
#   4. heal the partition; the old leader's term-N entry (if any) is
#      uncommitted — the new leader's AE with prev_log_term = N+1
#      conflicts and the old leader truncates its tail to match.
#
# Pass criterion: after heal, every node's LIST matches, and nobody
# has the uncommitted ghost entry that the old leader might have
# appended during the partition.

set -euo pipefail
source "$(dirname "$0")/../lib.sh"
trap cleanup EXIT

cleanup() {
  if [[ -n "${OLD_CONT:-}" ]]; then
    docker network connect "$NET_NAME" "$OLD_CONT" >/dev/null 2>&1 || true
  fi
  raft_down
}

raft_up
old_leader=$(raft_wait_leader 5)

old_idx=""
for i in 1 2 3; do
  id=$(raft_info_on "$i" | jq -r '.self_id // ""')
  [[ "$id" == "$old_leader" ]] && { old_idx=$i; break; }
done
OLD_CONT="raft-node-${RAFT_PORT_BASE}-${old_idx}"
say "partitioning old leader = node${old_idx}"
docker network disconnect "$NET_NAME" "$OLD_CONT"

# Old leader, now isolated, will try to propose.  It'll time out
# because it can't replicate — but the attempt itself appends to
# its local log, creating the "ghost tail" we're testing for.
iso_port=$(_dport "$old_idx")
curl -s --max-time 6 -X POST "http://127.0.0.1:${iso_port}/sql" \
  -H 'Content-Type: application/json' \
  -d '{"q":"CREATE DATABASE ghost_db"}' >/dev/null 2>&1 || true
say "  old leader's ghost propose complete (expected timeout)"

# Meanwhile, the 2-node majority side should elect a new leader.
new_idx=""
deadline=$(( $(date +%s) + 8 ))
while (( $(date +%s) < deadline )); do
  for i in 1 2 3; do
    [[ "$i" == "$old_idx" ]] && continue
    role=$(raft_info_on "$i" | jq -r '.role // ""' 2>/dev/null || echo "?")
    if [[ "$role" == "leader" ]]; then new_idx=$i; break 2; fi
  done
  sleep 0.2
done
[[ -n "$new_idx" ]] || fail "new leader didn't emerge within 8 s"
say "new leader = node${new_idx}"

resp=$(sql_on "$new_idx" 'CREATE DATABASE post_part_db')
echo "$resp" | jq -e '.rows[0][0] | startswith("OK")' >/dev/null \
  || fail "new leader refused CREATE: $resp"

# Heal.
say "reconnecting node${old_idx}"
docker network connect "$NET_NAME" "$OLD_CONT" >/dev/null

# Everyone converges on the same DB list.  Crucially, the old
# leader's ghost_db must NOT show up on any node (it was never
# committed by a majority).
for _ in 1 2 3 4 5 6 7 8 9 10; do
  all_match=1
  baseline=""
  for i in 1 2 3; do
    dbs=$(sql_on "$i" 'LIST DATABASES' \
          | jq -r '[.rows[][0]] | sort | join(",")' 2>/dev/null || echo "")
    if [[ -z "$baseline" ]]; then baseline="$dbs"
    elif [[ "$dbs" != "$baseline" ]]; then all_match=0; fi
  done
  [[ "$all_match" == "1" ]] && break
  sleep 0.5
done
[[ "$all_match" == "1" ]] || fail "cluster didn't converge"

# ghost_db must be absent.
if [[ "$baseline" == *"ghost_db"* ]]; then
  fail "ghost_db leaked into committed state: $baseline"
fi
[[ "$baseline" == *"post_part_db"* ]] \
  || fail "post_part_db missing on all nodes: $baseline"

say "  converged dbs: ${baseline}"
pass "s06 log_divergence"
