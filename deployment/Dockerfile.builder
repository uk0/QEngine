# Dockerfile.builder — extract the cluster + standalone server
# binaries for linux/amd64 so we can hot-swap into running containers
# without needing the remote host to reach Ubuntu apt mirrors.
#
# Pre-fix this only built `cluster_node`; the docker image's
# `tsdb-server` (standalone) stayed at whatever version the original
# `docker build` baked.  Recent fixes to the standalone /sql provider
# (commit b0d4744) never reached production because of that gap —
# the backup_restore.sh sidecar verifier had to run cluster-node mode
# instead.  Now the builder ships both binaries and deploy.sh mirrors
# them into every container.
#
# Usage (from repo root):
#   docker buildx build --platform=linux/amd64 \
#     -f deployment/Dockerfile.builder \
#     --output type=local,dest=./out .
#   # → ./out/tsdb_node is the cluster ELF
#   # → ./out/tsdb-server is the standalone ELF

FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc make pkg-config libssl-dev libreadline-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN find . -name '*.o' -delete && rm -rf build

ARG ARCH="-O2 -g -fno-omit-frame-pointer"
# See deployment/Dockerfile: TSDB_NO_AVX2_FILTER is no longer needed now that
# the AVX2 predicate kernels compile under gcc, and dropping it is a 3-14x win
# on f64/i64 filtering.
RUN make CC=gcc \
         CFLAGS="${ARCH} -std=gnu11 -Wall -Wextra -Wpedantic -Wshadow -Wno-unused-parameter -Wno-error -Iinclude -D_GNU_SOURCE" \
         cluster_node server_cli

FROM scratch AS out
COPY --from=builder /src/build/cluster/tsdb_node /tsdb_node
COPY --from=builder /src/build/tsdb-server      /tsdb-server
