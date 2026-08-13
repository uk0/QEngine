#!/usr/bin/env bash
# gate-token.sh — identity of the exact source state a gate run covers.
#
# Sourced by scripts/test-both-modes.sh (which writes the token when both
# modes are green) and by deployment/deploy.sh (which refuses to ship unless
# the stored token still matches).  It is not meant to be executed.
#
# It is the CONTENT of the working tree that is hashed — every tracked file
# plus every untracked file git would not ignore — and deliberately not the
# commit sha:
#
#   * The sha alone is not enough.  An uncommitted edit leaves HEAD unchanged,
#     so a sha-keyed stamp would authorise shipping code the gate never
#     compiled.
#   * The sha is also wrong in the other direction.  Committing a tested tree
#     changes HEAD without changing a single byte of source, and a stamp that
#     went stale on `git commit` would send an operator to deploy the exact
#     code the gate just passed — and straight into working around the gate.
#
# Ignored paths are excluded, so build output and the stamp itself do not
# feed back into the token.  563 tracked files hash in ~0.15 s on this tree.
# Outside a git checkout there is nothing to enumerate, so the token is the
# constant "nogit", which deploy.sh treats as no evidence at all.

gate_token() {
    if ! git rev-parse --git-dir >/dev/null 2>&1; then
        echo "nogit"
        return 0
    fi
    local sum
    if command -v shasum >/dev/null 2>&1; then
        sum="shasum -a 256"
    else
        sum="sha256sum"
    fi
    # `true` closes the subshell so a deleted-but-still-tracked file, which
    # makes the hasher exit non-zero, cannot fail the pipeline under the
    # callers' `set -o pipefail`; the file simply drops out of the digest and
    # the token changes, which is the intended signal.
    (
        git ls-files -z | xargs -0 $sum
        git ls-files -z --others --exclude-standard | xargs -0 $sum
        true
    ) 2>/dev/null | sort | $sum | cut -d' ' -f1
}
