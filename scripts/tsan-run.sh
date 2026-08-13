#!/usr/bin/env bash
# Run the named test binaries under ThreadSanitizer, in the production
# durability mode, the way scripts/asan-run.sh does for ASan.
#
#   scripts/tsan-run.sh test_drop_race test_mpmc_ring
#
# halt_on_error=0 keeps the run going after the first report so one pass
# collects every race a test can reach, and exitcode=66 makes "the test logic
# passed but TSan complained" distinguishable from "the test itself failed":
# 66 is reported as RACE, any other non-zero as FAIL.
#
# abort_on_error=0 is required for that to work at all.  On Darwin the
# sanitizers default it to 1, which raises SIGABRT on the way out and the
# shell sees 134 regardless of exitcode — measured on this tree before the
# option was added: every racy test came back "FAIL exit=134".
#
# No suppression file is installed on purpose.  A suppression is a decision
# that a report is benign, and none of these reports has been triaged yet.
set -uo pipefail
cd "$(dirname "$0")/.."

rc=0
for t in "$@"; do
    printf '%-30s ' "$t"
    TSAN_OPTIONS="halt_on_error=0 abort_on_error=0 exitcode=66 second_deadlock_stack=1" \
    TSDB_WAL_ONLY_COMMIT=1 timeout 600 "build/test/$t" > "/tmp/tsan_$t.log" 2>&1
    st=$?
    if [ "$st" -eq 0 ]; then
        echo PASS
    elif [ "$st" -eq 66 ]; then
        echo "RACE ($(grep -c 'WARNING: ThreadSanitizer' "/tmp/tsan_$t.log") report(s), see /tmp/tsan_$t.log)"
        rc=1
    else
        echo "FAIL exit=$st (see /tmp/tsan_$t.log)"
        rc=1
    fi
done
exit $rc
