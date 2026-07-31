#!/usr/bin/env bash
# Run the named build/test binaries under ASan in the production durability mode.
set -uo pipefail
cd "$(dirname "$0")/.."
rc=0
for t in "$@"; do
    printf '%-30s ' "$t"
    if ASAN_OPTIONS=detect_leaks=0 TSDB_WAL_ONLY_COMMIT=1 timeout 300 "build/test/$t" > "/tmp/asan_$t.log" 2>&1; then
        echo PASS
    else
        echo "FAIL (see /tmp/asan_$t.log)"
        rc=1
    fi
done
exit $rc
