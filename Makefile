# tsdb Makefile
# Single-machine column-oriented time-series database in C11.

CC        := clang
STD       ?= -std=c11
OPT       ?= -O3
WARN      := -Wall -Wextra -Wpedantic -Wshadow -Wno-unused-parameter
INC       := -Iinclude
# -ldl only needed on Linux; macOS libc provides dlopen unconditionally.
ifeq ($(shell uname -s), Linux)
  LIBS    := -lpthread -lm -ldl
else
  LIBS    := -lpthread -lm
endif

# liburing — Phase 1 of the async I/O upgrade.  Detected via pkg-config on
# Linux only; absence is non-fatal (io_async.c falls back to sync syscalls).
ifeq ($(shell uname -s), Linux)
  ifneq ($(shell pkg-config --exists liburing 2>/dev/null && echo y),)
    LIBS    += $(shell pkg-config --libs liburing)
    INC     += $(shell pkg-config --cflags liburing) -DTSDB_USE_IOURING
  endif
endif

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

SRC_DIRS  := src/core src/compress src/storage src/exec src/query src/cluster src/federation src/server src/catalog src/raft
SRCS      := $(filter-out src/cluster/tsdb_node_main.c,$(foreach d,$(SRC_DIRS),$(wildcard $(d)/*.c)))
OBJS      := $(SRCS:.c=.o)

TEST_SRCS := tests/test_compress.c tests/test_storage.c tests/test_exec.c tests/test_query.c tests/test_lzlite.c tests/test_pfor.c tests/test_simd_dispatch.c tests/test_adaptive.c tests/test_parallel.c tests/test_server.c tests/test_catalog.c tests/test_rawblock.c tests/test_pubsub.c tests/test_autobalance.c tests/test_tdigest.c tests/test_compaction.c tests/test_group_commit.c tests/test_asof_join.c tests/test_retention.c tests/test_v06_e2e.c tests/test_sample_by_stream.c tests/test_bloom_filter.c tests/test_tls.c tests/test_group_by.c tests/test_group_by_golden.c tests/test_v07_e2e.c tests/test_window_fns.c tests/test_ts_aggregates.c tests/test_advanced_windows.c tests/test_stable.c tests/test_v08_e2e.c tests/test_promql.c tests/test_influx_line.c tests/test_metrics.c tests/test_stable_sql.c tests/test_parallel_groupby.c tests/test_v09_e2e.c tests/test_alter_table.c tests/test_tmq.c tests/test_udf.c tests/test_parquet.c tests/test_rbac.c tests/test_auth_enforce.c tests/test_auth_wire.c tests/test_crc32c.c tests/test_iopolicy.c tests/test_hwcompress.c tests/test_bp128.c tests/test_block_points.c tests/test_skiplist_memtable.c tests/test_concurrent_delete.c tests/test_concurrent_writers.c tests/test_continuous_rw.c tests/test_block_stats.c tests/test_layout_invariants.c tests/test_raft_log.c tests/test_raft_rpc.c tests/test_raft_cluster.c tests/test_mpmc_ring.c tests/test_pitr.c tests/test_multi_data_dirs.c tests/test_cluster_route.c tests/test_drop_race.c tests/test_float32.c tests/test_codec_earlyexit.c tests/test_hier_mirror.c tests/test_catalog_reconcile.c tests/test_catalog_tombstone.c tests/test_catalog_id.c tests/test_catalog_v2.c tests/test_catalog_mirror.c tests/test_drop_trash.c tests/test_memtable_budget.c tests/test_compactor_memo.c tests/test_io_async.c tests/test_replication_compress.c tests/test_skiplist_monotonic.c tests/test_replication_compress_edge.c tests/test_io_async_robust.c tests/test_reactor.c tests/test_reactor_write.c tests/test_wal_interval.c tests/test_flush_append_race.c tests/test_rpc_tls.c tests/test_enospc.c tests/test_wal_recovery.c tests/test_torn_partition_read.c tests/test_fedrpc_byname.c tests/test_antientropy_safety.c tests/test_stable_open_fail.c tests/test_idx_version_mongrel.c tests/test_wire_count_disk.c tests/test_metrics_http.c tests/test_torn_value_column.c tests/test_zerocopy_read.c tests/test_net_hardening.c tests/test_standalone_parity.c tests/test_tls_connect_timeout.c tests/test_replica_backoff.c tests/test_sum_overflow.c tests/test_sql_insert.c tests/test_sql_having.c tests/test_sql_distinct.c tests/test_stable_scatter.c tests/test_partition_backfill.c tests/test_compact_swap_recover.c tests/test_partial_flush_recovery.c tests/test_multipart_recovery.c tests/test_symtab_recovery.c tests/test_compaction_v4_idx.c tests/test_wal_torn_tail.c tests/test_ae_local_stats.c tests/test_migrate.c tests/test_migrate_symbol.c tests/test_compaction_misaligned_blocks.c tests/test_migrate_resolve.c tests/test_bits_refill.c tests/test_wal_replay_abort.c tests/test_result_bulk.c
TEST_SRCS += tests/test_rawblock_multicol_atomic.c
TEST_SRCS += tests/test_zerofill_absent_column.c
TEST_BINS := $(patsubst tests/%.c,build/test/%,$(TEST_SRCS))

# Cluster integration test: built by default but run separately.
CLUSTER_TEST_SRCS := tests/test_cluster.c tests/test_cluster_count_repro.c tests/test_ae_phantom_peer.c
CLUSTER_TEST_BINS := $(patsubst tests/%.c,build/test/%,$(CLUSTER_TEST_SRCS))

# Anti-entropy peer probe (TSDB_RPC_LOCAL_TABLE_STATS) unit test: single
# process — an in-tree RPC server plus a stub modelling a peer too old for the
# opcode.  Appended here rather than folded into the TEST_SRCS line above so
# the diff stays readable.
TEST_SRCS += tests/test_ae_peer_stats.c

# Anti-entropy pull-candidate ranking + bounded retry (single process, no
# sockets): the peer probe can no longer be trusted to identify ONE usable
# source, so the resync keeps every peer's answer and walks them best-first.
TEST_SRCS += tests/test_ae_candidates.c

# Scaled-decimal FLOAT64 codec (TSDB_CODEC_DEC, id 12): adversarial scale
# selection, the -0.0 trap, a flag-off/flag-on differential, unknown-codec-id
# rejection, and a partition-level round trip.
TEST_SRCS += tests/test_codec_decimal.c

# Graceful-degrade read path for a column that is SHORT for a partition (fewer
# durable blocks than ts).  Builds the on-disk state by hand, then pins BOTH
# halves of the contract: everything that does not need the missing cells still
# answers exactly, and everything that does fails naming the column and the ts
# range — from every read site, including the stats fast path.
TEST_SRCS += tests/test_short_column_read.c
TEST_BINS := $(patsubst tests/%.c,build/test/%,$(TEST_SRCS))

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

.PHONY: all clean test test-client test-cluster test-federation bench cli tcp_cli server_cli cluster_node debug sdk-go sdk-java
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

debug: CFLAGS = $(STD) -O0 -g -Wall -Wextra -Wno-unused-parameter -fsanitize=address,undefined $(ARCH) $(INC) $(TLS_DEF) $(TLS_FLAGS)
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

# UDF sample shared library — built alongside tests, not linked into libtsdb.
build/test/udf_sample.so: tests/udf_sample.c include/tsdb_udf.h
	@mkdir -p build/test
	@$(CC) -fPIC -shared -Iinclude -o $@ tests/udf_sample.c
	@echo "SO  $@"

# test_udf depends on the sample .so being built first.
build/test/test_udf: tests/test_udf.c $(OBJS) build/test/udf_sample.so

# test_auth_enforce exercises CREATE FUNCTION via auth path; needs the .so.
build/test/test_auth_enforce: tests/test_auth_enforce.c $(OBJS) build/test/udf_sample.so

# test_standalone_parity drives the REAL tsdb_server_wire_metrics_providers
# from cli/tsdb_server_main.c.  That TU has its own main(), so compile it to a
# dedicated object with main renamed out of the way, then link it in.
build/test/tsdb_server_main_nomain.o: cli/tsdb_server_main.c
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Dmain=tsdb_server_cli_main_unused -c -o $@ cli/tsdb_server_main.c
	@echo "CC  cli/tsdb_server_main.c (nomain)"

build/test/test_standalone_parity: tests/test_standalone_parity.c $(OBJS) build/test/tsdb_server_main_nomain.o
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -o $@ tests/test_standalone_parity.c \
	    build/test/tsdb_server_main_nomain.o $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

# test_replica_backoff calls static helpers in src/cluster/replica.c.  That TU
# exposes them as non-static *_test shims under -DTSDB_TEST, so recompile just
# replica.c with that flag into a side object and link it in place of the
# normal replica.o (filtered out of $(OBJS) to avoid a duplicate symbol).
build/test/replica_test.o: src/cluster/replica.c
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -DTSDB_TEST -c -o $@ src/cluster/replica.c
	@echo "CC  src/cluster/replica.c (TSDB_TEST)"

build/test/test_replica_backoff: tests/test_replica_backoff.c \
        $(filter-out src/cluster/replica.o,$(OBJS)) build/test/replica_test.o
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -o $@ tests/test_replica_backoff.c \
	    $(filter-out src/cluster/replica.o,$(OBJS)) build/test/replica_test.o $(LDFLAGS)
	@echo "LD  $@"

build/test/%: tests/%.c $(OBJS)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -o $@ $< $(OBJS) $(LDFLAGS)
	@echo "LD  $@"

bench: $(BENCH_BINS)
	@for b in $(BENCH_BINS); do \
	  echo "--- $$b ---"; $$b; \
	done

# Five benches drive a server over the wire protocol (bench_docker_cluster,
# lat_probe, load_cluster, verify_count, enospc_writer) and need frame_send /
# frame_recv from cli/tsdb_wire.c.  Linking it into every bench is harmless —
# unused objects contribute no symbols — and keeps one rule instead of five.
# Without it `make bench` died on the first wire bench and never built the rest.
build/bench/%: bench/%.c $(OBJS) $(WIRE_OBJ)
	@mkdir -p build/bench
	@$(CC) $(CFLAGS) -Icli -o $@ $< $(OBJS) $(WIRE_OBJ) $(LDFLAGS)
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

# test_tls_connect_timeout: exercises the wire non-blocking-connect helper
# directly; links tsdb_wire.o only (no full libtsdb), like test_client.
build/test/test_tls_connect_timeout: tests/test_tls_connect_timeout.c $(WIRE_OBJ)
	@mkdir -p build/test
	@$(CC) $(CFLAGS) -Icli -o $@ tests/test_tls_connect_timeout.c $(WIRE_OBJ) -lpthread $(TLS_LIBS)
	@echo "LD  build/test/test_tls_connect_timeout"

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

# ── SDK builds (not part of the default target) ────────────────────────────
# sdk-go: build + test the pure-Go client (mock-listener tests need no server).
sdk-go:
	cd sdk/go && go build ./... && go test ./...

# sdk-java: plain-javac compile of the Java client + JDBC driver (main + test
# sources; JDK-only, no Maven needed), --release 11 matching the pom.
sdk-java:
	@mkdir -p build/java
	@find sdk/java/src -name '*.java' | xargs javac --release 11 -d build/java
	@echo "JAVAC sdk/java -> build/java"

print-%:
	@echo $* = $($*)
