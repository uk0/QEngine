#!/usr/bin/env bash
# s00 — fresh 3-master boot → LIST MASTERS returns the same rows on every
# node within the election window.
#
# Before this task the config was seeded independently by every node right
# after the startup-grace window; early starters saw 2 masters, late
# starters saw 4.  The fix is leader-seeded config bootstrap
# (TSDB_RAFT_CFG_OP_SEED log entry replicated via normal AppendEntries),
# so every node applies the same snapshot and LIST MASTERS converges.
#
# Pass criteria:
#   - cluster comes up (raft_up)
#   - leader elected
#   - LIST MASTERS on every node returns the same set of (id, addr) rows

set -euo pipefail
source "$(dirname "$0")/../lib.sh"
trap raft_down EXIT

raft_up
leader=$(raft_wait_leader 5)
say "leader=${leader}"

# Give the leader-seeded propose a moment to commit + apply everywhere.
# One heartbeat (~50 ms) + a safety margin should be plenty; we poll
# up to 3 s to avoid flakes on slow CI hosts.
converged=""
expected=""
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do
  ok=1
  tmp_signatures=()
  for i in 1 2 3; do
    # Sort rows so order differences don't cause spurious mismatches.
    sig=$(sql_on "$i" 'LIST MASTERS' \
          | jq -c '[.rows[] | [.[0], .[1]]] | sort' 2>/dev/null || echo "null")
    tmp_signatures+=("$sig")
    if [[ -z "$expected" ]]; then expected="$sig"
    elif [[ "$sig" != "$expected" ]]; then ok=0
    fi
    # Also ensure the list is non-empty (3 masters configured).
    len=$(echo "$sig" | jq 'length' 2>/dev/null || echo 0)
    if (( len != 3 )); then ok=0; fi
  done
  if (( ok == 1 )); then
    converged=1
    break
  fi
  expected=""
  sleep 0.25
done

for i in 1 2 3; do
  say "  node${i} LIST MASTERS: $(sql_on "$i" 'LIST MASTERS' | jq -c '.rows' 2>/dev/null)"
done

[[ -n "$converged" ]] || fail "LIST MASTERS did not converge on 3 identical rows across all nodes"

pass "s00 masters_consistent"
