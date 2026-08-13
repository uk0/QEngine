#!/usr/bin/env bash
# ci-selfcheck.sh — assert that the automation this repo relies on is wired up.
#
# Every property asserted here was FALSE before the CI unit landed: there was
# no workflow of any kind (no .github, no .gitlab-ci.yml, no Jenkinsfile), no
# ThreadSanitizer harness next to the ASan one, and deployment/deploy.sh
# shipped binaries into a live 4-node cluster with no evidence that the test
# gate had ever run.  This script is what fails when any of that regresses.
#
# It is deliberately cheap — no compile, no docker, no network — so CI can run
# it as the first job and so a developer can run it in a second.
#
#   scripts/ci-selfcheck.sh
#
# Exit status 0 = every invariant holds.
set -uo pipefail
cd "$(dirname "$0")/.."

fails=0
ok()  { printf '  ok    %s\n' "$*"; }
bad() { printf '  FAIL  %s\n' "$*"; fails=$((fails + 1)); }

# ── 1. A workflow exists at all ───────────────────────────────────────────
echo "== workflows =="
shopt -s nullglob
WORKFLOWS=(.github/workflows/*.yml .github/workflows/*.yaml)
shopt -u nullglob

if [ "${#WORKFLOWS[@]}" -eq 0 ]; then
    bad "no workflow under .github/workflows — nothing runs the suite but a human"
else
    ok "${#WORKFLOWS[@]} workflow file(s) present"
fi

# ── 2. They parse, and their triggers obey the two owner constraints ──────
# Parsing needs a YAML reader.  python3+PyYAML is the first choice; ruby's
# psych is the fallback.  Both are present on the GitHub-hosted ubuntu and
# macos images and on this project's dev machines.
yaml2json() {
    if python3 -c 'import yaml' 2>/dev/null; then
        python3 -c 'import sys, yaml, json; json.dump(yaml.safe_load(open(sys.argv[1])), sys.stdout, default=str)' "$1"
    elif command -v ruby >/dev/null 2>&1; then
        ruby -ryaml -rjson -e 'print YAML.safe_load(File.read(ARGV[0])).to_json' "$1"
    else
        echo "PARSER_MISSING" >&2
        return 2
    fi
}

if [ "${#WORKFLOWS[@]}" -gt 0 ]; then
    for wf in "${WORKFLOWS[@]}"; do
        if ! yaml2json "$wf" > "/tmp/ciselfcheck_$(basename "$wf").json" 2>/tmp/ciselfcheck_err; then
            bad "$wf does not parse as YAML: $(head -3 /tmp/ciselfcheck_err | tr '\n' ' ')"
        else
            ok "$wf parses"
        fi
    done

    # The trigger rules are evaluated over the parsed document rather than
    # grepped, because the interesting cases are structural.
    #
    # NOTE on the key name: in YAML 1.1 the bare word `on` is the boolean
    # true, and BOTH PyYAML and psych resolve GitHub's `on:` block to the
    # boolean key.  Verified on this tree: both emit {"true": {...}} after a
    # JSON round-trip.  A checker that only looked up the string "on" would
    # find nothing and pass vacuously, so the evaluator accepts either.
    if python3 - "${WORKFLOWS[@]}" <<'PY'
import json, sys, os
from fnmatch import fnmatch

# Runner labels the project owner fixed: macOS x86 is the literal
# macos-15-intel, macOS arm is matrix-driven, Linux is ubuntu-latest.
ALLOWED_RUNNERS = {"ubuntu-latest", "macos-15-intel", "${{ matrix.os }}"}
GATE_SCRIPT = "scripts/test-both-modes.sh"

def load(path):
    with open("/tmp/ciselfcheck_%s.json" % os.path.basename(path)) as f:
        return json.load(f)

def triggers(doc):
    t = doc.get("on", doc.get("true"))
    if t is None:
        return {}
    if isinstance(t, str):
        return {t: None}
    if isinstance(t, list):
        return {k: None for k in t}
    return t

def push_spec(doc):
    """The `push:` value, or None if the workflow has no push trigger."""
    t = triggers(doc)
    if "push" not in t:
        return None
    return t["push"] or {}

def fires_on_branch(doc, branch):
    p = push_spec(doc)
    if p is None:
        return False
    if not isinstance(p, dict):
        return True
    has_branch = "branches" in p or "branches-ignore" in p
    has_tag = "tags" in p or "tags-ignore" in p
    # No filter at all: every push fires, branches included.
    if not has_branch and not has_tag:
        return True
    # A tag filter alone means branch pushes never reach the workflow.
    if not has_branch:
        return False
    if "branches" in p:
        return any(fnmatch(branch, pat) for pat in p["branches"])
    return not any(fnmatch(branch, pat) for pat in p["branches-ignore"])

def fires_on_tag(doc, tag):
    p = push_spec(doc)
    if p is None:
        return False
    if not isinstance(p, dict):
        return True
    has_branch = "branches" in p or "branches-ignore" in p
    has_tag = "tags" in p or "tags-ignore" in p
    if not has_branch and not has_tag:
        return True
    if not has_tag:
        return False
    if "tags" in p:
        return any(fnmatch(tag, pat) for pat in p["tags"])
    return not any(fnmatch(tag, pat) for pat in p["tags-ignore"])

def runs_gate(doc):
    for job in (doc.get("jobs") or {}).values():
        for step in (job.get("steps") or []):
            if GATE_SCRIPT in (step.get("run") or ""):
                return True
    return False

def runners(doc):
    out = []
    for job in (doc.get("jobs") or {}).values():
        if "runs-on" in job:
            out.append(job["runs-on"])
    return out

bad = []
docs = {}
for path in sys.argv[1:]:
    try:
        docs[path] = load(path)
    except Exception as e:
        bad.append("%s: unreadable (%s)" % (path, e))

# Owner constraint 1: a push to main must not start a build, anywhere.
for path, doc in docs.items():
    if fires_on_branch(doc, "main"):
        bad.append("%s fires on a push to main — releases are tag-driven only" % path)

# The gate must run on pull requests and on ordinary branch pushes.
pr_gate = [p for p, d in docs.items() if "pull_request" in triggers(d) and runs_gate(d)]
if not pr_gate:
    bad.append("no workflow runs %s on pull_request" % GATE_SCRIPT)
for path in pr_gate:
    if not fires_on_branch(docs[path], "fix/some-branch"):
        bad.append("%s skips pushes to non-main branches" % path)

# Owner constraint 2: the release/artefact workflow is tag-only.  Stated as
# "no branch filter of any kind", not as "does not fire on branch X": a
# `branches: [release/*]` slipped past the by-name form when it was tried.
rel = [p for p in docs if os.path.basename(p).startswith("release")]
if not rel:
    bad.append("no release workflow — artefact builds are not tag-gated")
for path in rel:
    d = docs[path]
    if not fires_on_tag(d, "v1.2.3"):
        bad.append("%s does not fire on a version tag" % path)
    p = push_spec(d) or {}
    if isinstance(p, dict) and ("branches" in p or "branches-ignore" in p):
        bad.append("%s carries a branch filter; it must be tag-only" % path)
    other = [e for e in triggers(d) if e != "push"]
    if other:
        bad.append("%s also fires on %s; it must be tag-only" % (path, ", ".join(sorted(other))))

# Runner convention.
for path, doc in docs.items():
    for r in runners(doc):
        if r not in ALLOWED_RUNNERS:
            bad.append("%s: runs-on %r is outside the agreed labels %s"
                       % (path, r, sorted(ALLOWED_RUNNERS)))

for b in bad:
    print("  FAIL  " + b)
# Exit with the number of problems so the caller's tally stays honest.
sys.exit(min(len(bad), 100))
PY
    then
        ok "triggers and runner labels obey both owner constraints"
    else
        fails=$((fails + $?))
    fi
fi

# ── 3. The ThreadSanitizer harness exists and runs ────────────────────────
echo "== tsan harness =="
for s in scripts/tsan-build.sh scripts/tsan-run.sh; do
    if [ ! -f "$s" ]; then
        bad "$s missing — the suite has named race regressions and no TSan"
    elif [ ! -x "$s" ]; then
        bad "$s is not executable"
    elif ! bash -n "$s" 2>/tmp/ciselfcheck_err; then
        bad "$s is not valid bash: $(head -2 /tmp/ciselfcheck_err | tr '\n' ' ')"
    else
        ok "$s present and parses"
    fi
done

# tsan-run.sh with no binaries named must be a clean no-op, the same way
# asan-run.sh is; that proves the file is runnable, not merely present.
if [ -x scripts/tsan-run.sh ]; then
    if scripts/tsan-run.sh >/dev/null 2>&1; then
        ok "scripts/tsan-run.sh runs"
    else
        bad "scripts/tsan-run.sh fails with no arguments"
    fi

    # ...and it must actually EXECUTE what it is given.  "Runs with no
    # arguments" is satisfied by `#!/usr/bin/env bash; exit 0`, which is
    # exactly how a TSan harness stops working without anyone noticing --
    # doubly so because the workflow marks the tsan job continue-on-error, so
    # a silently-empty run reports the same green as a real one.  Hand it a
    # stub that leaves a marker and require the marker.
    _probe_bin="build/test/test_zz_selfcheck_probe"
    _probe_marker="/tmp/tsdb_tsan_probe_$$"
    mkdir -p build/test
    rm -f "$_probe_marker"
    printf '#!/usr/bin/env bash\ntouch %s\nexit 0\n' "$_probe_marker" > "$_probe_bin"
    chmod +x "$_probe_bin"
    scripts/tsan-run.sh test_zz_selfcheck_probe >/dev/null 2>&1
    if [ -f "$_probe_marker" ]; then
        ok "scripts/tsan-run.sh executes the binaries it is named"
    else
        bad "scripts/tsan-run.sh did not run the binary it was given — the TSan harness is a no-op"
    fi
    rm -f "$_probe_bin" "$_probe_marker"
fi

# ── 4. deploy.sh refuses to ship without a gate pass ──────────────────────
# Both SKIP_BUILD and SKIP_DEPLOY are set so this probe can never cross-build,
# rsync, ssh or docker-cp anything: the refusal has to happen before all of
# that, and with SKIP_DEPLOY=1 the script stops before the remote stage even
# if the refusal is missing.
echo "== deploy gate =="
STAMP="/tmp/ciselfcheck_stamp.$$"
rm -f "$STAMP"

out=$(TSDB_GATE_STAMP="$STAMP" SKIP_BUILD=1 SKIP_DEPLOY=1 \
      deployment/deploy.sh 2>&1); rc=$?
if [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "test gate has not passed"; then
    ok "deploy.sh refuses to ship without a gate pass (exit 3)"
else
    bad "deploy.sh shipped without a gate pass (exit $rc): $(printf '%s' "$out" | tail -1)"
fi

# And the gate must be satisfiable: a stamp matching this exact tree lets the
# script past the refusal.  Without this half, a hardcoded `exit 3` would pass
# the check above.
# shellcheck source=scripts/gate-token.sh
if [ -f scripts/gate-token.sh ]; then
    . scripts/gate-token.sh
    gate_token > "$STAMP"
    out=$(TSDB_GATE_STAMP="$STAMP" SKIP_BUILD=1 SKIP_DEPLOY=1 \
          deployment/deploy.sh 2>&1); rc=$?
    if [ "$rc" -eq 3 ] || printf '%s' "$out" | grep -q "test gate has not passed"; then
        bad "deploy.sh rejects a stamp that matches the working tree"
    else
        ok "deploy.sh accepts a stamp that matches the working tree"
    fi

    # And a stamp from a DIFFERENT tree must be refused.  Without this probe the
    # two above are both satisfiable by a deploy.sh that merely requires the
    # file to exist -- deleting the `have != want` comparison passed the whole
    # selfcheck, which makes the stamp a formality rather than a gate.  The
    # token binds a pass to the source that was tested; if a stale one is
    # accepted, editing code after a green run ships untested changes.
    printf 'not-the-token-for-this-tree\n' > "$STAMP"
    out=$(TSDB_GATE_STAMP="$STAMP" SKIP_BUILD=1 SKIP_DEPLOY=1 \
          deployment/deploy.sh 2>&1); rc=$?
    if [ "$rc" -eq 3 ]; then
        ok "deploy.sh refuses a stamp belonging to a different tree (exit 3)"
    else
        bad "deploy.sh accepted a foreign stamp (exit $rc) — the gate is a formality"
    fi
    rm -f "$STAMP"
else
    bad "scripts/gate-token.sh missing — no way to bind a gate pass to a tree"
fi

echo
if [ "$fails" -eq 0 ]; then
    echo "CI SELFCHECK OK"
    exit 0
fi
echo "CI SELFCHECK FAILED ($fails)"
exit 1
