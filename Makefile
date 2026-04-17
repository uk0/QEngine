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

COMMON    := $(STD) $(OPT) $(WARN) $(ARCH) $(INC)
CFLAGS    ?= $(COMMON)
LDFLAGS   ?= $(LIBS)

SRC_DIRS  := src/core src/compress src/storage src/exec src/query
SRCS      := $(foreach d,$(SRC_DIRS),$(wildcard $(d)/*.c))
OBJS      := $(SRCS:.c=.o)

TEST_SRCS := tests/test_compress.c tests/test_storage.c tests/test_exec.c tests/test_query.c tests/test_lzlite.c tests/test_pfor.c tests/test_simd_dispatch.c
TEST_BINS := $(patsubst tests/%.c,build/test/%,$(TEST_SRCS))

BENCH_SRCS := $(wildcard bench/*.c)
BENCH_BINS := $(patsubst bench/%.c,build/bench/%,$(BENCH_SRCS))

CLI_SRC   := cli/tsdb_cli.c
CLI_BIN   := build/tsdb

.PHONY: all clean test bench cli debug
.DEFAULT_GOAL := all

all: lib cli test

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
