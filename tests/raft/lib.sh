#!/usr/bin/env bash
# Shared helpers for raft acceptance scenarios.
#
# Each scenario sources this file, starts a 3-master stack with
# `raft_up`, runs curl-driven assertions, and lets `trap raft_down EXIT`
# clean up.
#
# All state lives under $TMP — scenarios can run in parallel as long as
# they pick distinct host ports via RAFT_PORT_BASE.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Host-side ports.  Each scenario can override RAFT_PORT_BASE to avoid
# collisions — node N's client port is BASE+N, dashboard is BASE+100+N.
RAFT_PORT_BASE="${RAFT_PORT_BASE:-39300}"

TMP="${TMP:-/tmp/tsdb-raft-$$}"
COMPOSE="${TMP}/docker-compose.yml"

NET_NAME="raft-net-${RAFT_PORT_BASE}"
IMG="qengine/tsdb:dev"

_cluster_size=3

# Colorised log so scenario output is easy to eyeball in CI.
say()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
pass() { printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*"; exit 1; }

# Build the compose file that stands up $_cluster_size master nodes.
# TSDB_CONSENSUS=raft is set so scenarios exercise the consensus path,
# not the fanout fallback.
raft_write_compose() {
  mkdir -p "$TMP"
  cat > "$COMPOSE" <<YAML
services:
YAML

  local seeds=""
  for i in $(seq 1 "$_cluster_size"); do
    local name="raft-node-${RAFT_PORT_BASE}-${i}"
    local client_port=$(( RAFT_PORT_BASE + i ))
    local dash_port=$(( RAFT_PORT_BASE + 100 + i ))
    local rpc_alias="rn${i}"
    mkdir -p "$TMP/node${i}/data"
    if [[ -n "$seeds" ]]; then
      seeds="${seeds},rn1:28080"
    else
      seeds="rn1:28080"
    fi
  done

  for i in $(seq 1 "$_cluster_size"); do
    local name="raft-node-${RAFT_PORT_BASE}-${i}"
    local client_port=$(( RAFT_PORT_BASE + i ))
    local dash_port=$(( RAFT_PORT_BASE + 100 + i ))
    local rpc_alias="rn${i}"
    local seed_arg=""
    [[ "$i" != "1" ]] && seed_arg="rn1:28080"
    cat >> "$COMPOSE" <<YAML
  node${i}:
    image: ${IMG}
    container_name: ${name}
    hostname: ${rpc_alias}
    environment:
      TSDB_ROLE: cluster-node
      TSDB_NODE_ROLE: master
      TSDB_CONSENSUS: raft
      TSDB_DATA_DIR: /var/lib/tsdb
      TSDB_BIND: 0.0.0.0:28090
      TSDB_RPC_BIND: ${rpc_alias}:28081
      TSDB_METRICS_BIND: 0.0.0.0:28094
      TSDB_SEEDS: "${seed_arg}"
    ports:
      - "${client_port}:28090"
      - "${dash_port}:28094"
    volumes:
      - ${TMP}/node${i}/data:/var/lib/tsdb
    networks:
      ${NET_NAME}:
        aliases: [${rpc_alias}]
    ulimits:
      nofile:
        soft: 1048576
        hard: 1048576
YAML
  done

  cat >> "$COMPOSE" <<YAML
networks:
  ${NET_NAME}:
    driver: bridge
    name: ${NET_NAME}
YAML
}

raft_up() {
  say "bringing up ${_cluster_size}-master stack under ${TMP}"
  raft_write_compose
  docker compose -f "$COMPOSE" up -d >/dev/null
  # Wait for all containers to be healthy enough to accept /cluster.
  local tries=30
  while (( tries > 0 )); do
    local ok=0
    for i in $(seq 1 "$_cluster_size"); do
      local dash=$(( RAFT_PORT_BASE + 100 + i ))
      if curl -sf --max-time 1 "http://127.0.0.1:${dash}/cluster" >/dev/null 2>&1; then
        ok=$(( ok + 1 ))
      fi
    done
    if (( ok == _cluster_size )); then
      say "all ${ok} nodes responding"
      return
    fi
    sleep 1
    tries=$(( tries - 1 ))
  done
  fail "cluster did not come up within 30 s"
}

raft_down() {
  [[ -f "$COMPOSE" ]] || return 0
  say "tearing down stack"
  docker compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true
  rm -rf "$TMP"
}

# ---- Query helpers ----

# Node client port.
_nport() { echo $(( RAFT_PORT_BASE + $1 )); }
# Node dashboard port.
_dport() { echo $(( RAFT_PORT_BASE + 100 + $1 )); }

sql_on() {
  local node="$1"; shift
  local q="$1"; shift
  curl -sf --max-time 5 -X POST "http://127.0.0.1:$(_dport "$node")/sql" \
       -H 'Content-Type: application/json' \
       -d "{\"q\":$(printf '%s' "$q" | jq -Rs .)}" \
       || { echo '{"error":"curl failed"}'; return 1; }
}

# Raft self-description: returns current term + role + leader id (or "")
# as newline-separated tokens.  Implementation queries the local
# /raft endpoint once it exists; until then, falls back to cluster.
raft_info_on() {
  local node="$1"
  local resp
  resp=$(curl -sf --max-time 3 "http://127.0.0.1:$(_dport "$node")/raft" 2>/dev/null || echo '{}')
  echo "$resp"
}

raft_leader_id() {
  local any=1
  raft_info_on "$any" | jq -r '.leader_id // ""'
}

raft_wait_leader() {
  local deadline_s="${1:-5}"
  local end=$(( $(date +%s) + deadline_s ))
  while (( $(date +%s) < end )); do
    local leader
    leader=$(raft_leader_id)
    if [[ -n "$leader" && "$leader" != "0" ]]; then
      echo "$leader"
      return
    fi
    sleep 0.2
  done
  fail "no leader elected within ${deadline_s}s"
}
