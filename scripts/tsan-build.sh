#!/usr/bin/env bash
# Local ThreadSanitizer build helper, the race-detector counterpart of
# scripts/asan-build.sh.  The suite carries a row of tests whose names are
# claims about concurrency — test_flush_append_race, test_drop_race,
# test_concurrent_writers, test_continuous_rw, test_mpmc_ring — and until now
# nothing ever checked those claims with a race detector.
#
#   scripts/tsan-build.sh build/test/test_drop_race ...
#
# TSan and ASan cannot share object files, and this tree compiles its .o next
# to the source rather than into a build directory, so switch sanitizers with
# a `make clean` in between or the link will mix instrumentations.
#
# CC: the default is /usr/bin/clang on macOS rather than whatever `clang` is
# first on PATH.  Verified on this dev host (macOS 27, arm64): a TSan binary
# built by Homebrew clang 22.1.0 dies with SIGSEGV before reaching main,
# while the same program built by Apple clang 21.0.0 runs and reports races
# normally.  On Linux the plain `clang` is used, matching the Makefile.
set -uo pipefail
cd "$(dirname "$0")/.."

if [ "$(uname -s)" = "Darwin" ]; then
    CC="${CC:-/usr/bin/clang}"
else
    CC="${CC:-clang}"
fi

CF="-std=c11 -O1 -g -fno-omit-frame-pointer -Wall -Wextra -Wno-unused-parameter -fsanitize=thread -march=native -Iinclude -DTSDB_TLS_OPENSSL $(pkg-config --cflags openssl)"
LF="-lpthread -lm $(pkg-config --libs openssl) -fsanitize=thread"
exec make -j8 CC="$CC" CFLAGS="$CF" LDFLAGS="$LF" "$@"
