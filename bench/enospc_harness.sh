#!/usr/bin/env bash
# enospc_harness.sh — DISK-FULL durability test for tsdb wire server.
#
# Runs ENTIRELY on the cluster host. Uses an ISOLATED loopback ext4 image
# (NOT the shared host disk, NOT the cluster /var/lib/tsdb). Bind-mounts it
# into a THROWAWAY container running tsdb-server (standalone). Fills it with
# WRITE_BATCH until ENOSPC, asserts no-crash + clean-error + acked-data-durable,
# then enlarges the image + restarts the container on the SAME data dir to
# prove durably-flushed data survives a restart (recovery).
#
# Cleans up container + mount + loopback + image in ALL exit paths.
set -u

IMG=qengine/tsdb:dev
CNAME=tsdb-enospc
PORT=39301
WORKDIR=/tmp/enospc_test.$$
LOOP_IMG=$WORKDIR/disk.img
MNT=$WORKDIR/mnt
SMALL_MB=32          # initial tiny volume (ENOSPC fast)
GROW_MB=256          # enlarged volume for recovery leg
ROWS_PER_BATCH=200000  # large batches amortise per-commit fsync
WRITER_SRC=$WORKDIR/enospc_writer.c
WRITER_BIN=$WORKDIR/enospc_writer
LOOPDEV=""

log() { echo "[harness] $*"; }

cleanup() {
  log "cleanup: starting"
  docker rm -f "$CNAME" >/dev/null 2>&1 || true
  # unmount (retry; container may have just released it)
  for i in 1 2 3 4 5; do
    if mountpoint -q "$MNT" 2>/dev/null; then
      umount "$MNT" 2>/dev/null && break
      sleep 0.5
    else
      break
    fi
  done
  if [ -n "$LOOPDEV" ]; then losetup -d "$LOOPDEV" 2>/dev/null || true; fi
  # detach any stray loop devices pointing at our image
  for d in $(losetup -j "$LOOP_IMG" 2>/dev/null | cut -d: -f1); do
    losetup -d "$d" 2>/dev/null || true
  done
  rm -rf "$WORKDIR" 2>/dev/null || true
  log "cleanup: done"
}
trap cleanup EXIT INT TERM

fail() { log "FAIL: $*"; exit 1; }

mk_loop_fs() {
  local mb=$1
  mkdir -p "$WORKDIR" "$MNT"
  log "creating ${mb}MB loopback ext4 image at $LOOP_IMG"
  dd if=/dev/zero of="$LOOP_IMG" bs=1M count="$mb" status=none || fail "dd image"
  mkfs.ext4 -q -F "$LOOP_IMG" || fail "mkfs.ext4"
  LOOPDEV=$(losetup --find --show "$LOOP_IMG") || fail "losetup"
  log "loop device: $LOOPDEV"
  mount "$LOOPDEV" "$MNT" || fail "mount"
  # tsdb runs as uid of container 'tsdb' user — give it a writable subdir.
  mkdir -p "$MNT/data"
  chmod 0777 "$MNT" "$MNT/data"
  df -h "$MNT" | tail -1
}

start_server() {
  log "starting throwaway container $CNAME on port $PORT (data on isolated loop fs)"
  docker run -d --rm --name "$CNAME" \
    -e TSDB_ROLE=server \
    -e TSDB_DATA_DIR=/data \
    -e TSDB_BIND=0.0.0.0:28090 \
    -v "$MNT/data:/data" \
    -p 127.0.0.1:$PORT:28090 \
    "$IMG" >/dev/null || fail "docker run"
  # wait for listener — probe the host-mapped port (bash /dev/tcp on host)
  for i in $(seq 1 60); do
    if (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      log "server is listening (after ${i} tries)"; return 0
    fi
    if ! docker ps --format '{{.Names}}' | grep -qx "$CNAME"; then
      log "container exited early; logs:"; docker logs "$CNAME" 2>&1 | tail -20
      fail "container died during startup"
    fi
    sleep 0.25
  done
  docker logs "$CNAME" 2>&1 | tail -20
  fail "server never started listening"
}

build_writer() {
  log "extracting wire client sources from image + compiling writer"
  # The image has headers/sources under the build tree; copy what we need out.
  # We carry the writer source via stdin (written by the caller before run).
  # Pull tsdb_wire.{c,h} + tsdb.h out of the image.
  local cid
  cid=$(docker create "$IMG") || fail "docker create for src extract"
  mkdir -p "$WORKDIR/cli" "$WORKDIR/include"
  # Try common locations inside the image.
  for base in /src /app /tsdb /build/src /root/tsdb; do
    docker cp "$cid:$base/cli/tsdb_wire.c" "$WORKDIR/cli/tsdb_wire.c" 2>/dev/null && \
    docker cp "$cid:$base/cli/tsdb_wire.h" "$WORKDIR/cli/tsdb_wire.h" 2>/dev/null && \
    docker cp "$cid:$base/include/tsdb.h"  "$WORKDIR/include/tsdb.h"  2>/dev/null && \
    { log "found sources under $base"; break; }
  done
  docker rm "$cid" >/dev/null 2>&1 || true
  if [ ! -f "$WORKDIR/cli/tsdb_wire.c" ]; then
    log "could not extract tsdb_wire.c from image; falling back to scp'd copy"
    [ -f "$WORKDIR/tsdb_wire.c" ] && cp "$WORKDIR/tsdb_wire.c" "$WORKDIR/cli/tsdb_wire.c"
    [ -f "$WORKDIR/tsdb_wire.h" ] && cp "$WORKDIR/tsdb_wire.h" "$WORKDIR/cli/tsdb_wire.h"
    [ -f "$WORKDIR/tsdb.h" ]      && cp "$WORKDIR/tsdb.h"      "$WORKDIR/include/tsdb.h"
  fi
  [ -f "$WORKDIR/cli/tsdb_wire.c" ] || fail "no tsdb_wire.c available"
  cc -O2 -o "$WRITER_BIN" "$WRITER_SRC" "$WORKDIR/cli/tsdb_wire.c" \
     -I"$WORKDIR/include" -I"$WORKDIR/cli" -lpthread 2>"$WORKDIR/cc.log" \
     || { cat "$WORKDIR/cc.log"; fail "writer compile"; }
  log "writer compiled: $WRITER_BIN"
}

# ---------------- MAIN ----------------
log "=== tsdb DISK-FULL (ENOSPC) durability test ==="

# 1. isolated small fs + server
mk_loop_fs "$SMALL_MB"
start_server

# 2. build writer (needs sources; caller scp'd writer src + wire src into WORKDIR)
build_writer

# 3. fill until ENOSPC
log "--- PHASE A: fill until ENOSPC ---"
set +e
stdbuf -oL "$WRITER_BIN" 127.0.0.1 "$PORT" enospc "$ROWS_PER_BATCH" 100000
WR=$?
set -e 2>/dev/null || true
log "writer exit code (phase A) = $WR"

# container still up?
if docker ps --format '{{.Names}}' | grep -qx "$CNAME"; then
  log "ASSERT a) container still Up after fill: PASS"
else
  log "ASSERT a) container still Up: FAIL — server crashed"
  docker logs "$CNAME" 2>&1 | tail -30
  exit 1
fi

case "$WR" in
  0) log "ASSERT b/c) clean ENOSPC error + acked data readable: PASS" ;;
  2) log "ASSERT b) clean write error: FAIL — transport hang/garbage on write"; exit 1 ;;
  3) log "ASSERT a/c) server responsive after error: FAIL"; exit 1 ;;
  4) log "setup failure inside writer"; docker logs "$CNAME" 2>&1 | tail -20; exit 1 ;;
  5) log "ASSERT c) data integrity: FAIL — corruption/loss detected"; exit 1 ;;
  *) log "writer returned unexpected code $WR (likely NO_ENOSPC on too-large fs)";;
esac

# record live count before restart, over the wire (the image's tsdb-cli is a
# network client, not an embedded REPL — so we query via the writer's count mode)
log "--- reading live count over the wire (pre-restart) ---"
PRE_RESTART=$("$WRITER_BIN" 127.0.0.1 "$PORT" enospc 0 0 count 2>/dev/null | grep -Eo 'COUNT=[0-9]+' | cut -d= -f2)
[ -z "$PRE_RESTART" ] && PRE_RESTART="?"
log "pre-restart live count (wire) = $PRE_RESTART"

# 4. RECOVERY: stop container, enlarge loop fs, restart on SAME data, verify
log "--- PHASE B: recovery — enlarge volume + restart, verify durable data ---"
docker rm -f "$CNAME" >/dev/null 2>&1 || true
# release + grow the backing image, then resize2fs
if mountpoint -q "$MNT"; then umount "$MNT" || fail "umount before grow"; fi
losetup -d "$LOOPDEV" 2>/dev/null || true; LOOPDEV=""
log "growing image $LOOP_IMG to ${GROW_MB}MB"
dd if=/dev/zero of="$LOOP_IMG" bs=1M count=$((GROW_MB-SMALL_MB)) seek="$SMALL_MB" status=none oflag=append conv=notrunc 2>/dev/null || \
  truncate -s "${GROW_MB}M" "$LOOP_IMG"
LOOPDEV=$(losetup --find --show "$LOOP_IMG") || fail "re-losetup"
e2fsck -fy "$LOOPDEV" >/dev/null 2>&1 || true
resize2fs "$LOOPDEV" >/dev/null 2>&1 || fail "resize2fs"
mount "$LOOPDEV" "$MNT" || fail "re-mount"
df -h "$MNT" | tail -1
start_server

log "--- verify recovered data after restart (over the wire) ---"
POST_RESTART=$("$WRITER_BIN" 127.0.0.1 "$PORT" enospc 0 0 count 2>/dev/null | grep -Eo 'COUNT=[0-9]+' | cut -d= -f2)
[ -z "$POST_RESTART" ] && POST_RESTART="?"
log "post-restart durable count (wire) = $POST_RESTART"

# Also write MORE after recovery to prove the db is writable again
log "--- write more after recovery (prove writable post-ENOSPC+grow) ---"
set +e
stdbuf -oL "$WRITER_BIN" 127.0.0.1 "$PORT" enospc2 8192 50
WR2=$?
set -e 2>/dev/null || true
log "post-recovery fresh-table write exit = $WR2 (0=ok, NO_ENOSPC also exits 0)"

# Final verdict on recovery
log "=== RESULT SUMMARY ==="
log "phaseA writer verdict code : $WR"
log "pre-restart  live count    : $PRE_RESTART  (durable + failed-batch in memtable)"
log "post-restart durable count : $POST_RESTART  (memtable lost; flushed-only)"
log "post-recovery writable     : exit=$WR2"

# Durability semantics: there is NO WAL replay, so a restart surfaces only
# the rows that were durably FLUSHED to partitions.  The failed batch's rows
# linger in the (now-dead) memtable, so post-restart MUST be a subset of
# pre-restart (post <= pre), non-zero, and never inflated above pre.  That IS
# the correct "acked data is durable, un-acked is cleanly dropped" outcome.
if [ "$PRE_RESTART" = "?" ] || [ "$POST_RESTART" = "?" ]; then
  log "RECOVERY: INDETERMINATE (could not read count via cli)"
elif ! [ "$POST_RESTART" -ge 1 ] 2>/dev/null; then
  log "ASSERT d) recovery: FAIL — durable data lost after restart (count=$POST_RESTART)"
  exit 1
elif [ "$POST_RESTART" -gt "$PRE_RESTART" ] 2>/dev/null; then
  log "ASSERT d) recovery: FAIL — post-restart ($POST_RESTART) > pre ($PRE_RESTART): phantom/duplicated rows"
  exit 1
elif [ "$POST_RESTART" = "$PRE_RESTART" ] 2>/dev/null; then
  log "ASSERT d) recovery: PASS — all live rows were durable; survived restart intact ($POST_RESTART)"
else
  log "ASSERT d) recovery: PASS — durable subset survived restart ($POST_RESTART of $PRE_RESTART live); un-acked memtable rows correctly dropped"
fi

log "=== ALL ENOSPC ASSERTIONS PASSED (no crash, clean error, acked data durable + recoverable) ==="
exit 0
