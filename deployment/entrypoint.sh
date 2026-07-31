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

# runuser + PAM reset soft nofile to 1024 even when the container
# advertises a higher limit.  Raise it here AND inside the drop-user
# shell so tsdb-{node,server} sees the container's real cap — needed
# for thousands of PTables in industrial workloads.
ulimit -n 1048576 2>/dev/null || true

# Core dumps.  The child's real status already reaches docker (runuser maps a
# signal death to 128+signo, tini passes it through: measured ExitCode=139 on a
# deliberate SIGSEGV).  What was missing is the core itself: the host's
# core_pattern is the relative `core.%e.%p`, so the kernel writes into the
# crashing process's CWD — and the CWD was `/`, root-owned while the server runs
# as uid 999.  The dump failed silently; the 4-day incident log shows eleven
# `Segmentation fault` lines and not one `(core dumped)`.
#
# A CORE MUST NOT BE ABLE TO KILL THE NODE IT IS DIAGNOSING.  Three bounds,
# because `core.%e.%p` is unique per pid — nothing is ever overwritten — and the
# restart policy is `unless-stopped`, so a crash loop writes one full core per
# restart forever:
#
#   1. Its own directory, NOT the data dir.  A core beside the partitions
#      competes with them for the same volume; keeping it in a subdirectory at
#      least makes it prunable and countable without touching data files.
#   2. A SOFT limit, not unlimited.  RLIMIT_CORE is measured in 512-byte blocks,
#      so 4194304 caps each core at 2 GiB.  A truncated core still loads in gdb
#      and still gives the backtrace, which is what this is for; an uncapped one
#      on a node with memtable_budget_rows=32000000 is far larger (file-backed
#      mmap is excluded by the default coredump_filter, the memtable and arenas
#      are not).  Raise it deliberately if a truncated core ever proves useless.
#   3. Keep the newest few and delete the rest, at every start.  The start after
#      a crash is exactly when the previous core exists and the next one is
#      about to be written.
TSDB_CORE_DIR="${TSDB_CORE_DIR:-$TSDB_DATA_DIR/cores}"
TSDB_CORE_KEEP="${TSDB_CORE_KEEP:-3}"
TSDB_CORE_MAX_BLOCKS="${TSDB_CORE_MAX_BLOCKS:-4194304}"   # 512-B blocks = 2 GiB
mkdir -p "$TSDB_CORE_DIR" 2>/dev/null || true
chown tsdb "$TSDB_CORE_DIR" 2>/dev/null || true
# Prune to the newest $TSDB_CORE_KEEP before the process that might add one starts.
ls -1t "$TSDB_CORE_DIR"/core.* 2>/dev/null | tail -n +$((TSDB_CORE_KEEP + 1)) \
    | while IFS= read -r f; do rm -f "$f"; done
echo "[entrypoint] cores -> $TSDB_CORE_DIR (keep $TSDB_CORE_KEEP, max $((TSDB_CORE_MAX_BLOCKS / 2048)) MiB each)"

case "$TSDB_ROLE" in
  server)
    : "${TSDB_METRICS_BIND:=0.0.0.0:28094}"
    : "${TSDB_INFLUX_BIND:=0.0.0.0:28092}"
    echo "[entrypoint] mode=server  data=$TSDB_DATA_DIR  bind=$TSDB_BIND  metrics=$TSDB_METRICS_BIND  influx=$TSDB_INFLUX_BIND"
    exec runuser -u tsdb -- sh -c 'ulimit -n 1048576; ulimit -c "$5" 2>/dev/null; cd "$6" 2>/dev/null || cd /tmp; exec tsdb-server --data-dir "$1" --bind "$2" --metrics-bind "$3" --influx-bind "$4"' _ \
         "$TSDB_DATA_DIR" "$TSDB_BIND" "$TSDB_METRICS_BIND" "$TSDB_INFLUX_BIND" "$TSDB_CORE_MAX_BLOCKS" "$TSDB_CORE_DIR"
    ;;

  cluster-node)
    : "${TSDB_METRICS_BIND:=0.0.0.0:28094}"
    echo "[entrypoint] mode=cluster-node  data=$TSDB_DATA_DIR  rpc=$TSDB_RPC_BIND  bind=$TSDB_BIND  metrics=$TSDB_METRICS_BIND  seeds=${TSDB_SEEDS:-<none>}"
    ARGS="--data-dir $TSDB_DATA_DIR --rpc $TSDB_RPC_BIND --bind $TSDB_BIND --metrics-bind $TSDB_METRICS_BIND"
    if [ -n "$TSDB_SEEDS" ]; then
      ARGS="$ARGS --seeds $TSDB_SEEDS"
    fi
    exec runuser -u tsdb -- sh -c "ulimit -n 1048576; ulimit -c $TSDB_CORE_MAX_BLOCKS 2>/dev/null; cd '$TSDB_CORE_DIR' 2>/dev/null || cd /tmp; exec tsdb-node $ARGS"
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
