# syntax=docker/dockerfile:1.6
# -----------------------------------------------------------------------------
# tsdb Dockerfile — two-stage build
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

ARG ALPINE_VERSION=3.20

# -----------------------------------------------------------------------------
# Stage 1 — builder
# -----------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION} AS builder

RUN apk add --no-cache \
        clang \
        make \
        musl-dev \
        linux-headers \
        pkgconfig \
        readline-dev

WORKDIR /src
COPY . .

# Clean any host build artifacts that may have been copied in.
RUN find . -name '*.o' -delete && rm -rf build

# Release build. -march=native is intentionally NOT used here — containers
# run on heterogeneous hardware. Set ARCH via build-arg to enable avx2 on
# a known Intel fleet, e.g. --build-arg ARCH="-march=x86-64-v3".
ARG ARCH="-O3"

RUN make CC=clang OPT="${ARCH}" all cluster_node \
      && strip build/tsdb-server build/tsdb-cli build/cluster/tsdb_node

# -----------------------------------------------------------------------------
# Stage 2 — runtime
# -----------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

# libgcc / readline for the binaries; su-exec for dropping root in entrypoint.
RUN apk add --no-cache libgcc readline su-exec tini \
    && addgroup -S tsdb && adduser -S -G tsdb -h /var/lib/tsdb tsdb

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

ENTRYPOINT ["/sbin/tini","-g","--","/usr/local/bin/entrypoint.sh"]
CMD []
