#!/usr/bin/env bash
# Run the test suite in BOTH durability modes.
#
# The engine commits two ways:
#   default                  flush-on-commit — a commit writes through to a partition
#   TSDB_WAL_ONLY_COMMIT=1   deferred flush  — a commit is WAL fsync + memtable; the
#                            partition is written later, on a size/idle flush
#
# The cluster runs the deferred mode, and it is auto-enabled on any host whose disk
# probes as rotational (tsdb_iopolicy_detect), so it is the production configuration —
# not an exotic option. For a long time the suite only ever ran the default mode, and
# when the deferred mode was first exercised 17 tests failed, several of them real
# defects rather than stale test assumptions. Running only one mode proves very little.
#
#   scripts/test-both-modes.sh            # build if needed, run both modes
#   scripts/test-both-modes.sh --no-build # reuse existing binaries
#
# Exit status is non-zero if EITHER mode has a failure.
set -uo pipefail
cd "$(dirname "$0")/.."

BUILD=1
[ "${1:-}" = "--no-build" ] && BUILD=0

if [ "$BUILD" = "1" ]; then
    echo "== building test binaries =="
    # Build the binaries only. The default `make` / `make test` target also RUNS
    # every test, which is minutes of work and stops at the first failure.
    targets=$(ls tests/test_*.c | sed 's|tests/|build/test/|; s|\.c$||' | tr '\n' ' ')
    if ! make -j4 $targets > /tmp/tbm_build.log 2>&1; then
        echo "BUILD FAILED — see /tmp/tbm_build.log"
        grep -E "error:" /tmp/tbm_build.log | head -20
        exit 1
    fi
fi

run_mode() {
    local label="$1" env_assign="$2"
    local pass=0 fail=0 failed=""
    for b in build/test/test_*; do
        [ -f "$b" ] && [ -x "$b" ] || continue
        local n; n=$(basename "$b")
        # Skip the integration tests: they need live nodes and their own ports,
        # and are driven by `make test-cluster` / `make test-federation`.
        #
        # Named EXPLICITLY, not matched as test_cluster* — that prefix also
        # swallowed test_cluster_route, a self-contained hashring unit test that
        # spins up no gossip or RPC at all, so it silently never ran in either
        # mode while looking like part of the suite.  Keep this list in step
        # with CLUSTER_TEST_SRCS in the Makefile; a new self-contained test
        # whose name happens to start with test_cluster now runs by default.
        case "$n" in
            test_federation|test_cluster|test_cluster_count_repro|test_cluster_show)
                continue;;
        esac
        if env $env_assign timeout 300 "$b" > "/tmp/tbm_${label}_$n.log" 2>&1; then
            pass=$((pass + 1))
        else
            fail=$((fail + 1)); failed="$failed $n"
        fi
    done
    printf '%-22s PASS=%-4d FAIL=%d\n' "$label" "$pass" "$fail"
    if [ -n "$failed" ]; then
        for f in $failed; do echo "    FAIL $f   (log: /tmp/tbm_${label}_$f.log)"; done
    fi
    return $fail
}

echo "== running both modes =="
run_mode "flush-on-commit" "TSDB_WAL_ONLY_COMMIT=0"; rc_default=$?
run_mode "deferred-flush"  "TSDB_WAL_ONLY_COMMIT=1"; rc_deferred=$?

echo
if [ "$rc_default" -eq 0 ] && [ "$rc_deferred" -eq 0 ]; then
    echo "BOTH MODES GREEN"
    exit 0
fi
echo "FAILURES: flush-on-commit=$rc_default  deferred-flush=$rc_deferred"
exit 1
