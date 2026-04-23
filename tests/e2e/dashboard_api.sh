#!/usr/bin/env bash
# dashboard_api.sh — black-box E2E against a running tsdb cluster.
#
# Exercises every API the Vite dashboard depends on:
#   /login /logout     — session cookie issuance
#   /tree              — catalog tree (DB → Group → VTable → PTable)
#   /cluster           — gossip view + per-node state
#   /raft              — state machine snapshot
#   /metrics           — Prometheus-text counters (replication health)
#   /audit             — append-only audit tail
#   /sql               — CREATE / DROP DATABASE, GROUP, TABLE, VTABLE, PTABLE
#
# Default target: the lvm1 4-node cluster's node-1 dashboard on 29311.
# Override via BASE=http://host:port.
#
# Every assertion prints PASS/FAIL and contributes to a final summary.

set -u  # don't fail fast — we want to see every failure

BASE="${BASE:-http://127.0.0.1:29311}"
SSH="${SSH:-ssh root@10.88.51.102}"
USER="${USER_:-root}"
PASS="${PASS:-123456}"

TAG="e2e_$$_$(date +%s)"   # unique names so the test is replay-safe
DB="${TAG}_db"
GRP="${TAG}_grp"
TBL="${TAG}_tbl"

PASS_COUNT=0
FAIL_COUNT=0
COOKIE="/tmp/e2e-cookie-$$"

say()  { printf '\e[36m[e2e]\e[0m %s\n' "$*"; }
pass() { printf '  \e[32m✓\e[0m %s\n' "$*"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { printf '  \e[31m✗\e[0m %s\n' "$*"; FAIL_COUNT=$((FAIL_COUNT+1)); }

# Run a curl from inside the remote host so we can reach internal URLs
# without exposing them on the laptop.  $1 = curl args; prints body.
rcurl() {
  $SSH "curl -s -b $COOKIE -c $COOKIE $*"
}
# Same but quiet / headers-only.
rcurl_stat() {
  $SSH "curl -s -o /dev/null -b $COOKIE -c $COOKIE -w '%{http_code}' $*"
}

# ──────────────────────────────────────────────────────────────────────
# Phase 1: login
# ──────────────────────────────────────────────────────────────────────
say "phase 1: login"
$SSH "rm -f $COOKIE"
code=$(rcurl_stat "-X POST $BASE/login -d 'user=$USER&pass=$PASS'")
[[ "$code" == "302" ]] && pass "POST /login → $code" || fail "POST /login → $code (expected 302)"

# Probe an authed endpoint to confirm cookie works.
code=$(rcurl_stat "$BASE/tree")
[[ "$code" == "200" ]] && pass "GET /tree with cookie → 200" || fail "GET /tree → $code"

# ──────────────────────────────────────────────────────────────────────
# Phase 2: tree + cluster + raft + metrics shapes
# ──────────────────────────────────────────────────────────────────────
say "phase 2: catalog + cluster shapes"

body=$(rcurl "$BASE/tree")
echo "$body" | grep -q '"databases"'      && pass "/tree has .databases" || fail "/tree missing .databases"
echo "$body" | grep -q '"vtables"'        && pass "/tree has .vtables"   || fail "/tree missing .vtables"
echo "$body" | grep -q '"ptables"'        && pass "/tree has .ptables"   || fail "/tree missing .ptables"
echo "$body" | grep -q '"groups"'         && pass "/tree has .groups"    || fail "/tree missing .groups"

body=$(rcurl "$BASE/cluster")
echo "$body" | grep -q '"local_id"'       && pass "/cluster has .local_id" || fail "/cluster missing .local_id"
echo "$body" | grep -q '"hb_age_ms"'      && pass "/cluster has .hb_age_ms" || fail "/cluster missing hb_age_ms"
echo "$body" | grep -q '"disk"'           && pass "/cluster has .local.disk" || fail "/cluster missing local.disk"

body=$(rcurl "$BASE/raft")
echo "$body" | grep -qE '"role":"(leader|follower|candidate)"' && pass "/raft has role" || fail "/raft missing role"

body=$(rcurl "$BASE/metrics")
echo "$body" | grep -q '^qengine_replicate_sent_total'  && pass "/metrics has replicate_sent"   || fail "/metrics missing replicate_sent"
echo "$body" | grep -q '^qengine_antientropy_rows_pulled_total' && pass "/metrics has antientropy" || fail "/metrics missing antientropy"

# ──────────────────────────────────────────────────────────────────────
# Phase 3: full CREATE / DROP round-trip via /sql
# ──────────────────────────────────────────────────────────────────────
sql() {
  # $1 = SQL statement.  JSON requires the SQL to be wrapped in double
  # quotes, but our SQL may contain single quotes and literal double
  # quotes.  Escape every " and \ for safe JSON inclusion, then ship
  # the body to the remote via an argument (avoiding nested shell
  # quoting hell).
  local q="$1"
  local esc=${q//\\/\\\\}
  esc=${esc//\"/\\\"}
  $SSH "curl -s -b $COOKIE -c $COOKIE -X POST '$BASE/sql' -H 'Content-Type: application/json' --data-binary @-" <<JSON
{"q":"$esc"}
JSON
}

say "phase 3: CREATE DATABASE $DB"
r=$(sql "CREATE DATABASE $DB")
echo "$r" | grep -q 'OK' && pass "CREATE DATABASE → OK" || fail "CREATE DATABASE: $r"

r=$(sql "LIST DATABASES")
echo "$r" | grep -q "$DB" && pass "DB appears in LIST DATABASES" || fail "DB missing from LIST"

say "phase 3: CREATE GROUP $GRP IN DATABASE $DB"
r=$(sql "CREATE GROUP $GRP IN DATABASE $DB")
echo "$r" | grep -q 'OK' && pass "CREATE GROUP → OK" || fail "CREATE GROUP: $r"

r=$(sql "LIST GROUPS IN DATABASE $DB")
echo "$r" | grep -q "$GRP" && pass "GROUP appears in LIST" || fail "GROUP missing"

say "phase 3: CREATE TABLE $TBL"
r=$(sql "CREATE TABLE $TBL (ts TIMESTAMP, v INT64) TIMESTAMP(ts) WITH (PARTITION='day')")
echo "$r" | grep -qE '"nrows"|OK' && pass "CREATE TABLE → OK" || fail "CREATE TABLE: $r"

r=$(sql "SELECT * FROM $TBL LIMIT 10")
echo "$r" | grep -qE '"nrows":0|"cols"' && pass "SELECT from new table works" || fail "SELECT: $r"

# DROP in reverse order of creation.
say "phase 3: DROP TABLE $TBL"
r=$(sql "DROP TABLE $TBL")
echo "$r" | grep -q 'OK' && pass "DROP TABLE → OK" || fail "DROP TABLE: $r"

say "phase 3: DROP GROUP $GRP"
r=$(sql "DROP GROUP $GRP")
echo "$r" | grep -q 'OK' && pass "DROP GROUP → OK" || fail "DROP GROUP: $r"

say "phase 3: DROP DATABASE $DB"
r=$(sql "DROP DATABASE $DB")
echo "$r" | grep -q 'OK' && pass "DROP DATABASE → OK" || fail "DROP DATABASE: $r"

r=$(sql "LIST DATABASES")
! echo "$r" | grep -q "$DB" && pass "DB gone from LIST" || fail "DB lingered"

# ──────────────────────────────────────────────────────────────────────
# Phase 4: VTable / PTable lifecycle
# ──────────────────────────────────────────────────────────────────────
VTDB="${TAG}_vtdb"
VT="${TAG}_vt"
PT="${TAG}_pt"

say "phase 4: STable + child table lifecycle"
sql "CREATE DATABASE $VTDB" >/dev/null
r=$(sql "CREATE STABLE $VT (ts TIMESTAMP, usage FLOAT64) TAGS (host SYMBOL) IN DATABASE $VTDB")
echo "$r" | grep -q 'OK' && pass "CREATE STABLE → OK" || fail "CREATE STABLE: $r"

r=$(sql "LIST VTABLES IN DATABASE $VTDB")
echo "$r" | grep -q "$VT" && pass "VTABLE visible" || fail "VTABLE missing"

r=$(sql "CREATE TABLE $PT USING $VT TAGS ('host01')")
echo "$r" | grep -qE 'OK|catalog|child table' && pass "CREATE child ptable → OK" || fail "CREATE ptable: $r"

r=$(sql "LIST PTABLES USING $VT")
echo "$r" | grep -q "$PT" && pass "PTABLE listed under VT" || fail "PTABLE missing"

say "phase 4: DROP PTABLE (via DROP TABLE)"
r=$(sql "DROP TABLE $PT")
echo "$r" | grep -q 'OK' && pass "DROP PTABLE → OK" || fail "DROP PTABLE: $r"

r=$(sql "DROP VTABLE $VT")
echo "$r" | grep -q 'OK' && pass "DROP VTABLE → OK" || fail "DROP VTABLE: $r"

sql "DROP DATABASE $VTDB" >/dev/null

# ──────────────────────────────────────────────────────────────────────
# Phase 5: audit log — every DDL above should show up
# ──────────────────────────────────────────────────────────────────────
say "phase 5: audit log"
body=$(rcurl "'$BASE/audit?n=200'")
echo "$body" | grep -q "CREATE DATABASE"  && pass "audit has CREATE DATABASE" || fail "audit missing CREATE DATABASE"
echo "$body" | grep -q "DROP DATABASE"    && pass "audit has DROP DATABASE"   || fail "audit missing DROP DATABASE"
echo "$body" | grep -q "CREATE STABLE"    && pass "audit has CREATE STABLE"   || fail "audit missing CREATE STABLE"
echo "$body" | grep -q "DROP TABLE"       && pass "audit has DROP TABLE"      || fail "audit missing DROP TABLE"

# ──────────────────────────────────────────────────────────────────────
# Phase 6: 4-node parity — each dashboard port responds identically
# ──────────────────────────────────────────────────────────────────────
say "phase 6: parity across 4 nodes"
for port in 29311 29312 29313 29314; do
  code=$($SSH "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$port/health")
  [[ "$code" == "200" ]] && pass "node :$port /health → 200" || fail "node :$port /health → $code"
done

# Each dashboard serves the same index.html bundle version.
bundle=$($SSH "curl -s -c /tmp/ck-e2e1 -X POST http://127.0.0.1:29311/login -d 'user=$USER&pass=$PASS' -o /dev/null
               curl -sb /tmp/ck-e2e1 http://127.0.0.1:29311/ | grep -oE '/assets/[^\"]+\.js' | head -1")
for port in 29311 29312 29313 29314; do
  ref=$($SSH "curl -s -c /tmp/ck-e2e-$port -X POST http://127.0.0.1:$port/login -d 'user=$USER&pass=$PASS' -o /dev/null
              curl -sb /tmp/ck-e2e-$port http://127.0.0.1:$port/ | grep -oE '/assets/[^\"]+\.js' | head -1")
  [[ "$bundle" == "$ref" ]] && pass "node :$port ships same bundle" || fail "node :$port ships $ref (expected $bundle)"
done

# ──────────────────────────────────────────────────────────────────────
# Phase 7: logout clears the cookie
# ──────────────────────────────────────────────────────────────────────
say "phase 7: logout"
code=$(rcurl_stat "$BASE/logout")
[[ "$code" == "200" || "$code" == "302" ]] && pass "GET /logout → $code" || fail "GET /logout → $code"
# Session cookie the server returned on /logout should no longer authenticate.
code=$(rcurl_stat "$BASE/tree")
[[ "$code" == "401" ]] && pass "/tree after logout → 401" || fail "/tree after logout → $code"

# ──────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────
$SSH "rm -f $COOKIE /tmp/ck-e2e*"
echo
say "────────────────────────────────────────────"
say "PASS: $PASS_COUNT"
say "FAIL: $FAIL_COUNT"
say "────────────────────────────────────────────"
[[ $FAIL_COUNT -eq 0 ]]
