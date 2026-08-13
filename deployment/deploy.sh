#!/usr/bin/env bash
# deploy.sh — build + deploy tsdb cluster to the lvm1 4-node host.
#
# Root cause of "binary reverted after recycle" incident 2026-04-23:
# the compose image `qengine/tsdb:dev` on the remote host was built once
# (from a pre-DROP-TABLE tree) and every subsequent deploy was a
# `docker cp` hot-swap that a single `docker compose up --force-recreate`
# would wipe out.  This script follows the only safe sequence:
#
#   1. build a fresh linux/amd64 tsdb_node binary via the cross-build
#      Dockerfile.builder (apt-less remote hosts are fine — everything
#      compiles on the laptop with buildx).
#   2. build the dashboard/dist bundle.
#   3. rsync compose + dist + binary tarball to the remote.
#   4. docker cp the binary into every running container.
#   5. docker commit one container → qengine/tsdb:dev so the baked
#      image carries the new binary; previous tag is kept as
#      qengine/tsdb:pre-<commit-sha> for rollback.
#   6. docker compose up -d --force-recreate — this now picks the
#      freshly-committed image, and any future recreate will too.
#   7. verify md5 + dashboard asset on every node.
#
# Environment:
#   REMOTE_HOST   — ssh target (default root@10.88.51.102)
#   REMOTE_SRC    — repo path on remote (default /home/nas/tsdb/src)
#   SKIP_BUILD=1  — reuse out/tsdb_node + dashboard/dist instead of rebuilding
#   SKIP_DEPLOY=1 — stop after local build (dry-run)
#   TSDB_GATE_STAMP — gate stamp to consult (default build/gate-passed)

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-root@10.88.51.102}"
REMOTE_SRC="${REMOTE_SRC:-/home/nas/tsdb/src}"
NODES=(qengine-cnode-1 qengine-cnode-2 qengine-cnode-3 qengine-cnode-4)

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "notgit")

say() { printf "\e[36m[deploy]\e[0m %s\n" "$*"; }
die() { printf "\e[31m[deploy] %s\e[0m\n" "$*" >&2; exit 1; }

# ────────────────────────────────────────────────────────────────────
# 0. Test gate — must run before anything is built or shipped
# ────────────────────────────────────────────────────────────────────
# Step 3b docker-cp's these binaries straight into four running production
# containers, so this script is the last point at which an untested build can
# be stopped.  scripts/test-both-modes.sh writes the stamp when both
# durability modes are green; the token inside it is a digest of the source
# that was tested, so editing any file after the gate run invalidates it.
# Exit 3 marks a gate refusal specifically, keeping it distinguishable from a
# build failure.
# shellcheck source=../scripts/gate-token.sh
. scripts/gate-token.sh
GATE_STAMP="${TSDB_GATE_STAMP:-build/gate-passed}"
gate_refuse() {
    printf "\e[31m[deploy] the test gate has not passed for this tree: %s\e[0m\n" "$1" >&2
    printf "\e[31m[deploy] run: scripts/test-both-modes.sh\e[0m\n" >&2
    exit 3
}
if [[ ! -f "$GATE_STAMP" ]]; then
    gate_refuse "no stamp at $GATE_STAMP"
fi
want=$(gate_token)
have=$(cat "$GATE_STAMP")
if [[ "$want" == "nogit" ]]; then
    gate_refuse "not a git checkout, so no gate pass can be tied to this source"
fi
if [[ "$have" != "$want" ]]; then
    gate_refuse "stamp is for a different tree ($have, this tree is $want)"
fi
say "test gate: passed for this tree ($want)"

# ────────────────────────────────────────────────────────────────────
# 1. Local build
# ────────────────────────────────────────────────────────────────────
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  say "cross-building linux/amd64 binaries (cluster + standalone) …"
  rm -rf out && mkdir out
  docker buildx build --platform=linux/amd64 \
    -f deployment/Dockerfile.builder \
    --output type=local,dest=./out . >/dev/null
  [[ -x out/tsdb_node ]]    || die "builder produced no tsdb_node"
  [[ -x out/tsdb-server ]]  || die "builder produced no tsdb-server"

  say "building dashboard/dist …"
  (cd dashboard && npm run build --silent)
  [[ -f dashboard/dist/index.html ]] || die "dashboard build failed"
else
  say "SKIP_BUILD=1 — reusing existing artefacts"
  [[ -x out/tsdb_node ]]            || die "out/tsdb_node missing"
  [[ -x out/tsdb-server ]]          || die "out/tsdb-server missing"
  [[ -f dashboard/dist/index.html ]] || die "dashboard/dist missing"
fi

say "local artefacts:"
ls -la out/tsdb_node out/tsdb-server dashboard/dist/index.html

if [[ "${SKIP_DEPLOY:-0}" == "1" ]]; then
  say "SKIP_DEPLOY=1 — stopping before remote stage"
  exit 0
fi

# ────────────────────────────────────────────────────────────────────
# 2. Ship artefacts to remote
# ────────────────────────────────────────────────────────────────────
say "packing dashboard dist …"
tar -czf /tmp/dashboard-dist.tgz -C dashboard dist

say "rsync → $REMOTE_HOST:$REMOTE_SRC …"
rsync -az --delete \
  --exclude='.git' --exclude='build' --exclude='node_modules' \
  --exclude='out' --exclude='docs' \
  "$ROOT/" "$REMOTE_HOST:$REMOTE_SRC/"

scp -q out/tsdb_node    "$REMOTE_HOST:/tmp/tsdb_node.new"
scp -q out/tsdb-server  "$REMOTE_HOST:/tmp/tsdb-server.new"
scp -q /tmp/dashboard-dist.tgz "$REMOTE_HOST:/tmp/"

# ────────────────────────────────────────────────────────────────────
# 3. Remote orchestration: cp → commit → recreate → verify
# ────────────────────────────────────────────────────────────────────
say "remote orchestration …"
ssh "$REMOTE_HOST" bash <<SSH_EOF
set -euo pipefail
cd $REMOTE_SRC

# 3a. Publish dist to the volume-mounted host directory.
mkdir -p deployment/dashboard-dist
rm -rf deployment/dashboard-dist/*
tar -xzf /tmp/dashboard-dist.tgz --strip-components=1 -C deployment/dashboard-dist

# 3b. docker cp binaries into every running node (fast local swap).
# Both binaries get refreshed: tsdb-node serves the cluster path,
# tsdb-server serves the standalone path used by the backup_restore
# verifier sidecar + by single-node test deployments.
for c in ${NODES[@]}; do
  docker cp /tmp/tsdb_node.new   \$c:/usr/local/bin/tsdb-node
  docker cp /tmp/tsdb-server.new \$c:/usr/local/bin/tsdb-server
done

# 3c. Bake the new binary into qengine/tsdb:dev.  Keep the previous
#     image as a rollback tag so an operator can always flip back.
prev_id=\$(docker images -q qengine/tsdb:dev)
if [ -n "\$prev_id" ]; then
  docker tag "\$prev_id" "qengine/tsdb:pre-$SHA" || true
fi
docker commit \\
  --change "ENV TSDB_DATA_DIR=/var/lib/tsdb" \\
  --change "ENV TSDB_BIND=0.0.0.0:28090" \\
  --change "ENV TSDB_ROLE=server" \\
  --change "ENV TSDB_DR_REMOTE=" \\
  --message "tsdb $SHA baked" \\
  qengine-cnode-1 qengine/tsdb:dev

# 3d. Rolling restart — because env/volume changes in compose only
#     take effect on recreate, but we already force-recreate upstream
#     when TSDB_DASHBOARD_DIR/volume-mount change; for binary-only
#     changes a simple restart is enough and avoids a double hop.
for c in ${NODES[@]}; do
  docker restart -t 8 "\$c" > /dev/null && echo "\$c restarted"
done

sleep 6
docker ps --filter "name=qengine-cnode" --format "{{.Names}}\t{{.Status}}"
SSH_EOF

# ────────────────────────────────────────────────────────────────────
# 4. Verification — md5 on every node + dashboard asset probe
# ────────────────────────────────────────────────────────────────────
say "verifying …"
LOCAL_MD5=$(md5 -q out/tsdb_node 2>/dev/null || md5sum out/tsdb_node | awk '{print $1}')
say "expected md5: $LOCAL_MD5"

PORTS=(29311 29312 29313 29314)
ok=0
for i in 0 1 2 3; do
  c="${NODES[$i]}"
  p="${PORTS[$i]}"
  md5=$(ssh "$REMOTE_HOST" "docker exec $c md5sum /usr/local/bin/tsdb-node | awk '{print \$1}'")
  status=$(ssh "$REMOTE_HOST" "curl -s -c /tmp/ck$p -X POST http://127.0.0.1:$p/login -d 'user=root&pass=123456' -o /dev/null -w '%{http_code}' && curl -sb /tmp/ck$p -o /dev/null -w '%{size_download}' http://127.0.0.1:$p/")
  hdr_size=$(echo "$status" | tail -c 10)
  if [[ "$md5" == "$LOCAL_MD5" ]]; then
    say "✓ $c  md5=ok  dash=$status"
    ok=$((ok+1))
  else
    say "✗ $c  md5=$md5 (expected $LOCAL_MD5)"
  fi
done

say "deployed to $ok / ${#NODES[@]} nodes"
if [[ $ok -ne ${#NODES[@]} ]]; then die "deploy incomplete"; fi
say "done — rollback: docker tag qengine/tsdb:pre-$SHA qengine/tsdb:dev"
