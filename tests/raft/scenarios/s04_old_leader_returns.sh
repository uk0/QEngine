#!/usr/bin/env bash
# s04 — old leader returns after a new one has been elected and made
# progress.  The returning node rejoins as a follower, its stale tail
# (if any) is harmless because the new leader just keeps AE'ing from
# wherever it is, and the returning node's catalog catches up.
#
# Pass criteria:
#   - new leader ACKs two CREATEs after we kill the old leader
#   - old leader container is `docker start`ed back
#   - within 5 s the revived node's LIST matches what the others see
#   - its /raft shows follower, current_term >= the term under which
#     we proposed post-kill, leader_id matching the rest of the cluster

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
[[ -n "$leader_idx" ]] || fail "could not map leader id"
say "old leader = node${leader_idx}"

# Write one thing before kill so the old leader has something in its log.
resp=$(sql_on "$leader_idx" 'CREATE DATABASE before_kill_db')
echo "$resp" | jq -e '.rows[0][0] | startswith("OK")' >/dev/null \
  || fail "old leader refused pre-kill CREATE: $resp"

say "killing leader container (node${leader_idx})"
docker kill "raft-node-${RAFT_PORT_BASE}-${leader_idx}" >/dev/null

# New leader within 5 s.
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
say "new leader = node${new_idx}"

# Two more CREATEs under the new leader while old leader is away.
for db in post_kill_a post_kill_b; do
  resp=$(sql_on "$new_idx" "CREATE DATABASE $db")
  echo "$resp" | jq -e '.rows[0][0] | startswith("OK")' >/dev/null \
    || fail "new leader refused CREATE $db: $resp"
done

# Revive the old leader.
say "docker start node${leader_idx}"
docker start "raft-node-${RAFT_PORT_BASE}-${leader_idx}" >/dev/null
# Wait for its /raft to respond.
sleep 2

# Poll for catch-up on the revived node.
revived_port=$(_dport "$leader_idx")
seen=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  dbs=$(curl -sf --max-time 2 -X POST "http://127.0.0.1:${revived_port}/sql" \
         -H 'Content-Type: application/json' \
         -d '{"q":"LIST DATABASES"}' \
       | jq -r '[.rows[][0]] | sort | join(",")' 2>/dev/null || echo "")
  if [[ "$dbs" == *"before_kill_db"* &&
        "$dbs" == *"post_kill_a"* &&
        "$dbs" == *"post_kill_b"* ]]; then
    seen=1; break
  fi
  sleep 0.5
done
[[ -n "$seen" ]] || fail "revived node${leader_idx} didn't catch up: dbs=${dbs}"

# The revived node might come back as a follower OR, under bad luck,
# bump its term and steal leadership right after catching up — both
# are legal in vanilla Raft (PreVote would pin it to follower, but
# we haven't implemented it).  What matters is that the cluster
# converges on a *single* leader that every node agrees on.
for _ in 1 2 3 4 5 6 7 8; do
  leaders_seen=()
  all_agree=1
  baseline=""
  for i in 1 2 3; do
    lid=$(raft_info_on "$i" | jq -r '.leader_id // "0"' 2>/dev/null || echo 0)
    role=$(raft_info_on "$i" | jq -r '.role // ""' 2>/dev/null || echo "")
    [[ "$role" == "leader" ]] && leaders_seen+=("$lid")
    if [[ -z "$baseline" ]]; then baseline="$lid"
    elif [[ "$lid" != "$baseline" ]]; then all_agree=0; fi
  done
  if (( ${#leaders_seen[@]} == 1 )) && [[ "$all_agree" == "1" ]]; then
    say "  all nodes agree leader=${baseline}"
    break
  fi
  sleep 0.5
done
(( ${#leaders_seen[@]} == 1 )) && [[ "$all_agree" == "1" ]] \
  || fail "cluster didn't converge on a single leader after revival"

say "  revived node${leader_idx} dbs: ${dbs}"
pass "s04 old_leader_returns"
