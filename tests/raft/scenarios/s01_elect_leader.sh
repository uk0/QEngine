#!/usr/bin/env bash
# s01 — 3 masters elect a single leader within 1 s.
#
# Pass criteria:
#   - Each node's /raft reports the same leader_id
#   - leader_id is non-zero and corresponds to one of the 3 node ids
#   - Election finishes within 1 second after the last node became healthy

set -euo pipefail
source "$(dirname "$0")/../lib.sh"
trap raft_down EXIT

raft_up

t0=$(date +%s%3N)
leader=$(raft_wait_leader 1)
t1=$(date +%s%3N)

say "leader=${leader} elected in $(( t1 - t0 ))ms"

# All 3 nodes should agree.
for i in 1 2 3; do
  info=$(raft_info_on "$i")
  seen=$(echo "$info" | jq -r '.leader_id // ""')
  term=$(echo "$info" | jq -r '.current_term // ""')
  [[ "$seen" == "$leader" ]] \
    || fail "node$i sees leader=${seen}, expected ${leader}"
  say "  node${i}: term=${term} leader=${seen}"
done

pass "s01 elect_leader"
