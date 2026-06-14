# -----------------------------------------------------------------------------
# tsdb Dockerfile — two-stage build
#
# Note: no `# syntax=` directive — we stay on classic build syntax so this
# image can be built on hosts that can't reach docker.io/docker/dockerfile.
# All features used here (ARG, multi-stage, COPY --from) are pre-BuildKit.
#
#   stage 1: compile tsdb-server, tsdb-cli, tsdb_node using clang under Alpine
#   stage 2: minimal runtime image (Alpine + libgcc + libstdc++) containing
#            only the three binaries under /usr/local/bin
#
# Build locally:
#   docker build -f deployment/Dockerfile -t tsdb:dev .
# Push to registry:
#   docker tag tsdb:dev ghcr.io/your-org/tsdb:0.5.0
# -----------------------------------------------------------------------------

ARG DEBIAN_VERSION=22.04

# -----------------------------------------------------------------------------
# Stage 1 — builder (glibc-based Ubuntu; earlier Alpine+musl image hit a
# runtime crash in the float64 write path that reproduced only under musl.
# Native gcc + glibc build worked fine on the same host.)
# -----------------------------------------------------------------------------
FROM ubuntu:${DEBIAN_VERSION} AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        make \
        clang \
        pkg-config \
        libssl-dev \
        libreadline-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Clean any host build artifacts that may have been copied in.
RUN find . -name '*.o' -delete && rm -rf build

# Release build. -march=native is intentionally NOT used here — containers
# run on heterogeneous hardware. Set ARCH via build-arg to enable avx2 on
# a known Intel fleet, e.g. --build-arg ARCH="-march=x86-64-v3".
ARG ARCH="-O2 -g -fno-omit-frame-pointer"
# Compiler is overridable: default gcc (canonical linux/amd64 fleet build), but
# a native linux/arm64 build needs clang — gcc-aarch64 rejects the NEON
# intrinsic signedness in src/exec/simd.h.  Build native arm64 with:
#   docker build --build-arg CC_BIN=clang -t qengine/tsdb:dev .
ARG CC_BIN=gcc

RUN make CC=${CC_BIN} \
         CFLAGS="${ARCH} -std=gnu11 -Wall -Wextra -Wpedantic -Wshadow -Wno-unused-parameter -Wno-error -Iinclude -D_GNU_SOURCE -DTSDB_NO_AVX2_FILTER" \
         tcp_cli server_cli cluster_node

# -----------------------------------------------------------------------------
# Stage 2 — runtime (slim Ubuntu with only OpenSSL + readline runtime libs)
# -----------------------------------------------------------------------------
FROM ubuntu:${DEBIAN_VERSION}

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 libreadline8 tini netcat-openbsd wget ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r tsdb && useradd -r -g tsdb -d /var/lib/tsdb -s /bin/sh tsdb

COPY --from=builder /src/build/tsdb-server          /usr/local/bin/tsdb-server
COPY --from=builder /src/build/tsdb-cli             /usr/local/bin/tsdb-cli
COPY --from=builder /src/build/cluster/tsdb_node    /usr/local/bin/tsdb-node
COPY deployment/entrypoint.sh                        /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh

VOLUME ["/var/lib/tsdb"]

# Default ports (override via env or compose):
#   28090/tcp  client traffic (wire protocol v1)
#   28081/tcp  cluster RPC
#   28080/udp  gossip
EXPOSE 28090 28081 28080/udp

ENV TSDB_DATA_DIR=/var/lib/tsdb \
    TSDB_BIND=0.0.0.0:28090 \
    TSDB_ROLE=server

ENTRYPOINT ["/usr/bin/tini","-g","--","/usr/local/bin/entrypoint.sh"]
CMD []
