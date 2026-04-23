# Dockerfile.builder — extract only the cluster_node binary for linux/amd64
# so we can hot-swap into running containers without needing the remote
# host to reach Ubuntu apt mirrors.
#
# Usage (from repo root):
#   docker buildx build --platform=linux/amd64 \
#     -f deployment/Dockerfile.builder \
#     --output type=local,dest=./out .
#   # → ./out/tsdb_node is a linux/amd64 ELF ready to scp + docker cp

FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc make pkg-config libssl-dev libreadline-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN find . -name '*.o' -delete && rm -rf build

ARG ARCH="-O2 -g -fno-omit-frame-pointer"
RUN make CC=gcc \
         CFLAGS="${ARCH} -std=gnu11 -Wall -Wextra -Wpedantic -Wshadow -Wno-unused-parameter -Wno-error -Iinclude -D_GNU_SOURCE -DTSDB_NO_AVX2_FILTER" \
         cluster_node

FROM scratch AS out
COPY --from=builder /src/build/cluster/tsdb_node /tsdb_node
