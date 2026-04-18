# tsdb Makefile
# Single-machine column-oriented time-series database in C11.

CC        := clang
STD       ?= -std=c11
OPT       ?= -O3
WARN      := -Wall -Wextra -Wpedantic -Wshadow -Wno-unused-parameter
INC       := -Iinclude
LIBS      := -lpthread -lm

# Per-architecture SIMD flags.
# -march=native covers AVX2/NEON at the global level.
# AVX-512 is guarded by __attribute__((target("avx512f,..."))) per-function
# so the binary remains loadable on non-AVX512 CPUs; the global flag is
# harmless on CPUs that do support it (native auto-detects).
UNAME_M   := $(shell uname -m)
ifeq ($(UNAME_M), x86_64)
  ARCH    := -march=native -mavx -mavx2 -mbmi2
else ifeq ($(UNAME_M), aarch64)
  ARCH    := -march=armv8-a+simd
else
  ARCH    := -march=native
endif

# ── TLS backend detection ─────────────────────────────────────────────────
# Preference: OpenSSL (stable, well-tested) → mbedtls → no TLS.
# Note: mbedtls 4.0 renamed libmbedcrypto to libtfpsacrypto; pkg-config
#       for 'mbedtls' alone pulls in the crypto and x509 transitively.
OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl 2>/dev/null)
OPENSSL_LIBS   := $(shell pkg-config --libs   openssl 2>/dev/null)
ifneq ($(OPENSSL_LIBS),)
  TLS_DEF    := -DTSDB_TLS_OPENSSL
  TLS_FLAGS  := $(OPENSSL_CFLAGS)
  TLS_LIBS   := $(OPENSSL_LIBS)
else
  MBEDTLS_CFLAGS := $(shell pkg-config --cflags mbedtls 2>/dev/null)
  MBEDTLS_LIBS   := $(shell pkg-config --libs   mbedtls 2>/dev/null)
  ifneq ($(MBEDTLS_LIBS),)
    TLS_DEF    := -DTSDB_TLS_MBEDTLS
    TLS_FLAGS  := $(MBEDTLS_CFLAGS)
    TLS_LIBS   := $(MBEDTLS_LIBS)
  else
    TLS_DEF    :=
    TLS_FLAGS  :=
    TLS_LIBS   :=
  endif
endif

COMMON    := $(STD) $(OPT) $(WARN) $(ARCH) $(INC) $(TLS_DEF) $(TLS_FLAGS)
CFLAGS    ?= $(COMMON)
LDFLAGS   ?= $(LIBS) $(TLS_LIBS)

SRC_DIRS  := src/core src/compress src/storage src/exec src/query src/cluster src/federation src/server src/catalog
SRCS      := $(filter-out src/cluster/tsdb_node_main.c,$(foreach d,$(SRC_DIRS),$(wildcard $(d)/*.c)))
OBJS      := $(SRCS:.c=.o)

TEST_SRCS := tests/test_compress.c tests/test_storage.c tests/test_exec.c tests/test_query.c tests/test_lzlite.c tests/test_pfor.c tests/test_simd_dispatch.c tests/test_adaptive.c tests/test_parallel.c tests/test_server.c tests/test_catalog.c tests/test_rawblock.c tests/test_pubsub.c tests/test_autobalance.c tests/test_tdigest.c tests/test_compaction.c tests/test_group_commit.c tests/test_asof_join.c tests/test_retention.c tests/test_v06_e2e.c tests/test_sample_by_stream.c tests/test_bloom_filter.c tests/test_tls.c
TEST_BINS := $(patsubst tests/%.c,build/test/%,$(TEST_SRCS))

# Cluster integration test: built by default but run separately.
CLUSTER_TEST_SRCS := tests/test_cluster.c
CLUSTER_TEST_BINS := $(patsubst tests/%.c,build/test/%,$(CLUSTER_TEST_SRCS))

# Federation integration test.
FED_TEST_SRCS := tests/test_federation.c
FED_TEST_BINS := $(patsubst tests/%.c,build/test/%,$(FED_TEST_SRCS))

BENCH_SRCS := $(wildcard bench/*.c)
BENCH_BINS := $(patsubst bench/%.c,build/bench/%,$(BENCH_SRCS))

CLI_SRC         := cli/tsdb_cli.c
CLI_BIN         := build/tsdb

# TCP client CLI — standalone, links only against cli/tsdb_wire.c (no libtsdb)
TCP_CLI_SRCS    := cli/tsdb_client.c cli/tsdb_wire.c
TCP_CLI_BIN     := build/tsdb-cli

# Detect readline; fallback to fgets if absent
READLINE_FLAGS  := $(shell pkg-config --cflags readline 2>/dev/null)
READLINE_LIBS   := $(shell pkg-config --libs   readline 2>/dev/null)
ifneq ($(READLINE_LIBS),)
  TCP_CLI_CFLAGS  := -DHAVE_READLINE $(READLINE_FLAGS)
  TCP_CLI_LDFLAGS := $(READLINE_LIBS)
else
  TCP_CLI_CFLAGS  :=
  TCP_CLI_LDFLAGS :=
endif

# Wire object (shared by tsdb-cli and test_client, no libtsdb needed)
WIRE_OBJ        := cli/tsdb_wire.o

SERVER_CLI_SRC  := cli/tsdb_server_main.c
SERVER_CLI_BIN  := build/tsdb-server

CLUSTER_NODE_SRC := src/cluster/tsdb_node_main.c
CLUSTER_NODE_BIN := build/cluster/tsdb_node

.PHONY: all clean test test-client test-cluster test-federation bench cli tcp_cli server_cli cluster_node debug
.DEFAULT_GOAL := all

all: lib cli tcp_cli server_cli test $(CLUSTER_TEST_BINS) $(FED_TEST_BINS)

lib: build/libtsdb.a

build/libtsdb.a: $(OBJS)
	@mkdir -p build
	@ar rcs $@ $(OBJS)
	@echo "AR  $@"

%.o: %.c
	@$(CC) $(CFLAGS) -c -o $@ $<
	@echo "CC  $<"

debug: CFLAGS = $(STD) -O0 -g -Wall -Wextra -Wno-unused-parameter -fsanitize=address,undefined $(ARCH) $(INC)
debug: LDFLAGS += -fsanitize=address,undefined
debug: all

test: $(TEST_BINS)
	@for t in $(TEST_BINS); do \
	  echo "--- $$t ---"; $$t || exit 1; \
	done

test-cluster: $(CLUSTER_TEST_BINS)
	@for t in $(CLUSTER_TEST_BINS); do \
	  echo "--- $$t ---"; $$t || exit 1; \
	done

test-federation: $(FED_TEST_BINS)
	@for t in $(FED_TEST_BINS); do \
	  echo "--- $$t ---"; $$t || exit 1; \
	done

# test_tls has a special rule (links wire obj + TLS libs) — defined elsewhere
build/test/test_tls: tests/test_tls.c $(OBJS) $(WIRE_OBJ)

build/test/%: tests/%.c $(OBJS)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -o $@ $< $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

bench: $(BENCH_BINS)
	@for b in $(BENCH_BINS); do \
	  echo "--- $$b ---"; $$b; \
	done

build/bench/%: bench/%.c $(OBJS)
	@mkdir -p build/bench
	@$(CC) $(CFLAGS) -o $@ $< $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

cli: $(CLI_BIN)

$(CLI_BIN): $(CLI_SRC) $(OBJS)
	@mkdir -p build
	@$(CC) $(CFLAGS) -o $@ $(CLI_SRC) $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

# TCP client REPL (tsdb-cli): standalone, no libtsdb dependency
tcp_cli: $(TCP_CLI_BIN)

$(WIRE_OBJ): cli/tsdb_wire.c cli/tsdb_wire.h
	@$(CC) $(CFLAGS) -Icli -c -o $@ cli/tsdb_wire.c
	@echo "CC  cli/tsdb_wire.c"

$(TCP_CLI_BIN): $(TCP_CLI_SRCS) cli/tsdb_wire.h
	@mkdir -p build
	@$(CC) $(CFLAGS) $(TCP_CLI_CFLAGS) -Icli -o $@ $(TCP_CLI_SRCS) \
	    -lpthread -lm $(TCP_CLI_LDFLAGS) $(TLS_LIBS)
	@echo "LD  $@"

# test_client: links against tsdb_wire.o only (no full libtsdb)
build/test/test_client: tests/test_client.c $(WIRE_OBJ)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Icli -o $@ tests/test_client.c $(WIRE_OBJ) -lpthread $(TLS_LIBS)
	@echo "LD  build/test/test_client"

test-client: build/test/test_client
	@echo "--- build/test/test_client ---"
	@build/test/test_client

# TLS test: links libtsdb + tsdb_wire.o (needs both server-side and client-side)
build/test/test_tls: tests/test_tls.c $(OBJS) $(WIRE_OBJ)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Icli -o $@ tests/test_tls.c $(OBJS) $(WIRE_OBJ) \
	    $(LDFLAGS) $(TLS_LIBS)
	@echo "LD  build/test/test_tls"

test-tls: build/test/test_tls
	@echo "--- build/test/test_tls ---"
	@build/test/test_tls

server_cli: $(SERVER_CLI_BIN)

$(SERVER_CLI_BIN): $(SERVER_CLI_SRC) $(OBJS)
	@mkdir -p build
	@$(CC) $(CFLAGS) -o $@ $(SERVER_CLI_SRC) $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

cluster_node: $(CLUSTER_NODE_BIN)

$(CLUSTER_NODE_BIN): $(CLUSTER_NODE_SRC) $(OBJS)
	@mkdir -p build/cluster
	@$(CC) $(CFLAGS) -o $@ $(CLUSTER_NODE_SRC) $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

clean:
	@rm -rf build $(OBJS)
	@echo "CLEAN"

# Run test_simd_dispatch under each CPU level supported on this machine.
# On ARM64: SCALAR + NEON.  On x86-64: SCALAR + AVX2 (+ AVX512 if present).
check-levels: build/test/test_simd_dispatch
	@echo "--- SCALAR ---"
	@TSDB_CPU_LEVEL=SCALAR build/test/test_simd_dispatch
	@echo "--- NEON ---"
	@TSDB_CPU_LEVEL=NEON   build/test/test_simd_dispatch 2>/dev/null || true
	@echo "--- AVX2 ---"
	@TSDB_CPU_LEVEL=AVX2   build/test/test_simd_dispatch 2>/dev/null || true
	@echo "--- AVX512 ---"
	@TSDB_CPU_LEVEL=AVX512 build/test/test_simd_dispatch 2>/dev/null || true

print-%:
	@echo $* = $($*)
