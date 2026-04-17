#!/bin/sh
# entrypoint.sh — launch tsdb in one of three roles.
#
# TSDB_ROLE:
#   server       - standalone TCP server (default)
#   cluster-node - cluster member (gossip + rpc + client)
#   cli          - one-shot tsdb-cli (no server)
#
# Environment variables:
#   TSDB_DATA_DIR       Data directory (default /var/lib/tsdb)
#   TSDB_BIND           Client bind addr (default 0.0.0.0:28090)
#   TSDB_RPC_BIND       Cluster RPC bind (cluster-node only, default 0.0.0.0:28081)
#   TSDB_SEEDS          Comma-separated seed addresses (cluster-node only)
#
# --------------------------------------------------------------------------

set -e

: "${TSDB_DATA_DIR:=/var/lib/tsdb}"
: "${TSDB_BIND:=0.0.0.0:28090}"
: "${TSDB_RPC_BIND:=0.0.0.0:28081}"
: "${TSDB_ROLE:=server}"
: "${TSDB_SEEDS:=}"

mkdir -p "$TSDB_DATA_DIR"
chown -R tsdb:tsdb "$TSDB_DATA_DIR"

case "$TSDB_ROLE" in
  server)
    echo "[entrypoint] mode=server  data=$TSDB_DATA_DIR  bind=$TSDB_BIND"
    exec su-exec tsdb tsdb-server \
         --data-dir "$TSDB_DATA_DIR" \
         --bind     "$TSDB_BIND"
    ;;

  cluster-node)
    echo "[entrypoint] mode=cluster-node  data=$TSDB_DATA_DIR  rpc=$TSDB_RPC_BIND  seeds=${TSDB_SEEDS:-<none>}"
    ARGS="--data-dir $TSDB_DATA_DIR --rpc $TSDB_RPC_BIND"
    if [ -n "$TSDB_SEEDS" ]; then
      ARGS="$ARGS --seeds $TSDB_SEEDS"
    fi
    exec su-exec tsdb tsdb-node $ARGS
    ;;

  cli)
    shift 2>/dev/null || true
    exec tsdb-cli "$@"
    ;;

  *)
    echo "[entrypoint] unknown TSDB_ROLE=$TSDB_ROLE (valid: server | cluster-node | cli)" >&2
    exit 1
    ;;
esac
