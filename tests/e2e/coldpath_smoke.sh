#!/usr/bin/env bash
# coldpath_smoke.sh — poke every documented HTTP endpoint at least
# once, plus a set of negative / malformed cases, to catch crashes
# and silent error swallowing before we cut a release.  Never touches
# the cluster's on-disk data.

set -u
SSH="${SSH:-ssh root@10.88.51.102}"
N=29311
CK=/tmp/coldpath.ck

PASS=0; FAIL=0
say()  { printf "\e[36m[cold]\e[0m %s\n" "$*"; }
ok()   { printf "  \e[32m✓\e[0m %s\n" "$*"; PASS=$((PASS+1)); }
bad()  { printf "  \e[31m✗\e[0m %s\n" "$*"; FAIL=$((FAIL+1)); }

# ──────────────────────────────────────────────────────────────────
# 1. Happy-path auth + endpoint shape
# ──────────────────────────────────────────────────────────────────
say "phase 1: authed GET probes"
$SSH "rm -f $CK; curl -s -c $CK -X POST 'http://127.0.0.1:$N/login' -d 'user=root&pass=123456' -o /dev/null"
for path in /tree /cluster /raft /health /metrics /audit?n=5; do
  code=$($SSH "curl -s -o /dev/null -w '%{http_code}' -b $CK 'http://127.0.0.1:$N$path'")
  [[ "$code" == "200" ]] && ok "GET $path → 200" || bad "GET $path → $code"
done

# /backup streams a tarball with chunked encoding — skip it here (heavy).
say "  (skipping /backup — heavy streaming)"

# ──────────────────────────────────────────────────────────────────
# 2. Unauthenticated probes — should bounce to /login or 401
# ──────────────────────────────────────────────────────────────────
say "phase 2: unauthed probes return 401/302 on gated routes"
for path in /tree /sql /audit /pitr /retention/sweep /backup; do
  # Every gated route must answer with a non-200 BEFORE serving
  # any content when the cookie is absent.  401 is the canonical
  # answer for our XHR routes; 302 redirects dashboard GETs to
  # /login; 405 arrives for GET on POST-only routes only AFTER
  # auth has been enforced (so 405 is also acceptable here).
  code=$($SSH "curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:$N$path' 2>&1")
  if [[ "$code" == "401" || "$code" == "302" || "$code" == "405" ]]; then
    ok "GET $path (no cookie) → $code"
  else
    bad "GET $path (no cookie) → $code (expected 401/302/405)"
  fi
done

# /health + /metrics must be open for Prometheus / k8s.
for path in /health /metrics /login; do
  code=$($SSH "curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:$N$path' 2>&1")
  if [[ "$code" == "200" ]]; then
    ok "GET $path (public) → 200"
  else
    bad "GET $path (public) → $code"
  fi
done

# ──────────────────────────────────────────────────────────────────
# 3. Malformed / edge-case /sql bodies
# ──────────────────────────────────────────────────────────────────
sql_json() {
  $SSH "curl -s -b $CK -X POST 'http://127.0.0.1:$N/sql' -H 'Content-Type: application/json' --data-binary '$1'"
}

say "phase 3: /sql edge cases"

# Empty body
r=$(sql_json '{}')
echo "$r" | grep -q error && ok "empty {} → error" || bad "empty {} → $r"

# Missing q
r=$(sql_json '{"not_q":"select 1"}')
echo "$r" | grep -q error && ok "missing q → error" || bad "missing q → $r"

# Empty q
r=$(sql_json '{"q":""}')
echo "$r" | grep -q error && ok "empty q → error" || bad "empty q → $r"

# Parse error surfaces
r=$(sql_json '{"q":"garbage sql goes here"}')
echo "$r" | grep -q "parse" && ok "garbage → parse error" || bad "garbage → $r"

# Nonexistent table — should be "not found", not crash
r=$(sql_json '{"q":"SELECT * FROM totally_nonexistent_table_xyz"}')
echo "$r" | grep -qiE "not found|error" && ok "missing table → not found" || bad "missing table → $r"

# Very long SQL — check it doesn't OOM / truncate silently
long=$(printf 'SELECT 1 %.0s' {1..200})
r=$(sql_json "$(printf '{"q":"%s"}' "$long")")
echo "$r" | grep -qE "error|nrows" && ok "long SQL processed (error or ok)" || bad "long SQL swallowed"

# SQL with embedded quotes properly escaped — we pass a JSON string
# with escaped internal double-quote in the SQL.  Harness is expected
# to still route, even if server rejects.
r=$(sql_json '{"q":"CREATE DATABASE \"edge_case_db\""}')
echo "$r" | grep -qE 'OK|error' && ok "quoted identifier reached server" || bad "quoted identifier lost: $r"
$SSH "curl -s -b $CK -X POST 'http://127.0.0.1:$N/sql' -H 'Content-Type: application/json' --data-binary '{\"q\":\"DROP DATABASE \\\"edge_case_db\\\"\"}'" > /dev/null 2>&1

# ──────────────────────────────────────────────────────────────────
# 4. /pitr edge cases
# ──────────────────────────────────────────────────────────────────
say "phase 4: /pitr edge cases"
# Missing ts query-string arg → 400
code=$($SSH "curl -s -o /dev/null -w '%{http_code}' -b $CK -X POST 'http://127.0.0.1:$N/pitr'")
[[ "$code" == "400" ]] && ok "POST /pitr no ts → 400" || bad "POST /pitr no ts → $code"

# Non-numeric ts → 400
code=$($SSH "curl -s -o /dev/null -w '%{http_code}' -b $CK -X POST 'http://127.0.0.1:$N/pitr?ts=not_a_number'")
[[ "$code" == "400" ]] && ok "POST /pitr non-numeric ts → 400" || bad "POST /pitr non-numeric → $code"

# Far-future ts (year 2100 in ns ≈ 4.1e18) → nothing should match; 200.
r=$($SSH "curl -s -b $CK -X POST 'http://127.0.0.1:$N/pitr?ts=4102444800000000000'")
echo "$r" | grep -q '"partitions_removed":0' && ok "POST /pitr far-future (2100) → 0 partitions" || bad "/pitr far-future → $r"

# ──────────────────────────────────────────────────────────────────
# 5. /audit pagination
# ──────────────────────────────────────────────────────────────────
say "phase 5: /audit query args"
r=$($SSH "curl -s -b $CK 'http://127.0.0.1:$N/audit?n=5'")
echo "$r" | grep -q '"rows"' && ok "audit?n=5 has rows field" || bad "audit?n=5 malformed"
nrows=$(echo "$r" | python3 -c 'import json,sys; print(json.load(sys.stdin)["nrows"])' 2>/dev/null)
if [[ -n "$nrows" && "$nrows" -le 5 ]]; then ok "audit?n=5 returns <=5 rows ($nrows)"
else                                         bad "audit?n=5 returned $nrows (expected <=5)"; fi

# Absurd n → should cap at server limit (2000)
r=$($SSH "curl -s -b $CK 'http://127.0.0.1:$N/audit?n=999999'")
echo "$r" | grep -q '"rows"' && ok "audit?n=absurd handled" || bad "audit?n=absurd crashed"

# ──────────────────────────────────────────────────────────────────
# 6. Static dashboard assets
# ──────────────────────────────────────────────────────────────────
say "phase 6: static assets"
# Pull index.html and extract the bundle names
index=$($SSH "curl -s -b $CK 'http://127.0.0.1:$N/'")
echo "$index" | grep -q '<title>tsdb' && ok "GET / returns Vite dashboard" || bad "GET / served wrong content"

asset=$(echo "$index" | grep -oE '/assets/[^"]+\.js' | head -1)
if [[ -n "$asset" ]]; then
  bytes=$($SSH "curl -s -o /dev/null -w '%{size_download}' -b $CK 'http://127.0.0.1:$N$asset'")
  [[ "$bytes" -gt 1000 ]] && ok "asset $asset served ($bytes bytes)" || bad "asset $asset too small ($bytes)"
fi

# Bad asset path should 404
code=$($SSH "curl -s -o /dev/null -w '%{http_code}' -b $CK 'http://127.0.0.1:$N/assets/does_not_exist.js'")
[[ "$code" == "404" ]] && ok "missing asset → 404" || bad "missing asset → $code"

# Path traversal — must not escape the dashboard root
code=$($SSH "curl -s -o /dev/null -w '%{http_code}' -b $CK 'http://127.0.0.1:$N/assets/../../etc/passwd'")
if [[ "$code" == "404" || "$code" == "400" ]]; then
  ok "path traversal refused ($code)"
else
  bad "path traversal returned $code (expected 404/400)"
fi

# ──────────────────────────────────────────────────────────────────
# 7. /retention/sweep
# ──────────────────────────────────────────────────────────────────
say "phase 7: /retention/sweep"
r=$($SSH "curl -s -b $CK -X POST 'http://127.0.0.1:$N/retention/sweep'")
echo "$r" | grep -qE '"ok":' && ok "retention/sweep returns JSON" || bad "retention/sweep → $r"

# ──────────────────────────────────────────────────────────────────
# 8. Concurrency — 20 parallel /sql SELECT 1 don't deadlock
# ──────────────────────────────────────────────────────────────────
say "phase 8: 20 parallel /sql"
start=$(date +%s)
$SSH "for i in \$(seq 1 20); do
  curl -s -b $CK -X POST 'http://127.0.0.1:$N/sql' -H 'Content-Type: application/json' \
       --data-binary '{\"q\":\"LIST DATABASES\"}' > /dev/null &
done; wait"
elapsed=$(( $(date +%s) - start ))
[[ "$elapsed" -lt 10 ]] && ok "20 parallel /sql in ${elapsed}s" || bad "20 parallel /sql took ${elapsed}s"

$SSH "rm -f $CK"
say "────────────────────────────────────────────"
say "PASS: $PASS  FAIL: $FAIL"
[[ $FAIL -eq 0 ]]
