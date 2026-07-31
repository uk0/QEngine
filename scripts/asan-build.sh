#!/usr/bin/env bash
# Local ASan build helper for the catalog-reload investigation.
set -uo pipefail
cd "$(dirname "$0")/.."
CF="-std=c11 -O1 -g -fno-omit-frame-pointer -Wall -Wextra -Wno-unused-parameter -fsanitize=address -march=native -Iinclude -DTSDB_TLS_OPENSSL $(pkg-config --cflags openssl)"
LF="-lpthread -lm $(pkg-config --libs openssl) -fsanitize=address"
exec make -j8 CFLAGS="$CF" LDFLAGS="$LF" "$@"
