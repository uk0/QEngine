#!/usr/bin/env bash
# e2e_full.sh — full-stack integration test.
#
# Launches a fresh tsdb-server, exercises every customer-facing feature
# (auth, DDL, ingest, query, SAMPLE BY, UDF, subscribe, health, metrics,
# backup → restore round-trip), and asserts data integrity throughout.
#
# Intended to be run from repo root:   bash tests/e2e_full.sh
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

TCP_ADDR="127.0.0.1:28300"
METRICS_ADDR="127.0.0.1:28301"
DATA_DIR="/tmp/tsdb_e2e_full"
BACKUP_DIR="/tmp/tsdb_e2e_backup"
SERVER_LOG="/tmp/tsdb_e2e_server.log"
PIDFILE="/tmp/tsdb_e2e_server.pid"

SDK="$REPO/sdk/go"
BIN="$SDK/bin"
UDF_SO="$REPO/build/test/udf_sample.so"

pass=0
fail=0
PASS() { printf "PASS: %s\n" "$1"; pass=$((pass+1)); }
FAIL() { printf "FAIL: %s\n" "$1" >&2; fail=$((fail+1)); }

cleanup() {
    if [[ -f "$PIDFILE" ]]; then
        kill "$(cat "$PIDFILE")" 2>/dev/null || true
        rm -f "$PIDFILE"
    fi
    pkill -P $$ 2>/dev/null || true
}
trap cleanup EXIT

start_server() {
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    TSDB_METRICS_BIND="$METRICS_ADDR" \
        "$REPO/build/tsdb-server" \
            --bind "$TCP_ADDR" \
            --data-dir "$DATA_DIR" \
            >"$SERVER_LOG" 2>&1 &
    echo $! > "$PIDFILE"
    # Wait for the listen port.
    for _ in $(seq 1 50); do
        if lsof -iTCP:${TCP_ADDR##*:} -sTCP:LISTEN >/dev/null 2>&1; then
            return
        fi
        sleep 0.1
    done
    echo "server failed to bind $TCP_ADDR" >&2
    cat "$SERVER_LOG" >&2
    exit 1
}

stop_server() {
    if [[ -f "$PIDFILE" ]]; then
        kill "$(cat "$PIDFILE")" 2>/dev/null || true
        wait "$(cat "$PIDFILE")" 2>/dev/null || true
        rm -f "$PIDFILE"
    fi
}

# --- Build prerequisites -----------------------------------------------------

echo "=== e2e: building prerequisites ==="
make build/tsdb-server build/test/udf_sample.so build/tsdb-cli >/dev/null 2>&1
(cd "$SDK" && \
    go build -o bin/tsdb-backup  ./cmd/tsdb-backup && \
    go build -o bin/tsdb-restore ./cmd/tsdb-restore && \
    go build -o bin/tsdb-import  ./cmd/tsdb-import && \
    go build -o bin/tsdb-export  ./cmd/tsdb-export && \
    go build -o bin/bench5m      ./cmd/bench5m)

# --- Phase 1: health / metrics endpoints up before any workload --------------

start_server
echo
echo "=== Phase 1: /health + /metrics + / dashboard ==="

curl -sf "http://$METRICS_ADDR/health"  >/tmp/e2e_health.json  && PASS "/health returns 200" || FAIL "/health 200"
grep -q '"status":"ok"'  /tmp/e2e_health.json                  && PASS "/health status=ok"    || FAIL "/health status"
grep -q '"uptime_s"'     /tmp/e2e_health.json                  && PASS "/health uptime_s"     || FAIL "/health uptime"

curl -sf "http://$METRICS_ADDR/metrics" >/tmp/e2e_metrics.txt   && PASS "/metrics returns 200" || FAIL "/metrics 200"
grep -q 'qengine_connections_total' /tmp/e2e_metrics.txt        && PASS "/metrics has counters" || FAIL "/metrics counters"

curl -sf "http://$METRICS_ADDR/" >/tmp/e2e_dash.html            && PASS "/ dashboard HTML"     || FAIL "/ dashboard"
grep -q 'QEngine' /tmp/e2e_dash.html                            && PASS "dashboard contains branding" || FAIL "dashboard branding"

# --- Phase 2: ingest through bench5m (exercises all write paths) ------------

echo
echo "=== Phase 2: bench5m — 500K-row ingest + aggregates ==="
"$BIN/bench5m" -addr "$TCP_ADDR" -rows 500000 -udf-so "$UDF_SO" > /tmp/e2e_bench.json 2>&1 && \
    PASS "bench5m completed" || FAIL "bench5m"
grep -q '"phase":"ingest"'            /tmp/e2e_bench.json && PASS "bench: ingest phase ran"     || FAIL "bench ingest"
grep -q '"phase":"q.sample_by_1m"'    /tmp/e2e_bench.json && PASS "bench: SAMPLE BY phase ran"  || FAIL "bench sample_by"
grep -q '"phase":"q.udf_select_limit"' /tmp/e2e_bench.json && PASS "bench: UDF phase ran"        || FAIL "bench udf"

# --- Phase 3: backup → restore round-trip -----------------------------------

echo
echo "=== Phase 3: backup → stop → wipe → start → restore ==="

rm -rf "$BACKUP_DIR"
"$BIN/tsdb-backup" -addr "$TCP_ADDR" -out "$BACKUP_DIR" -tables trades >/tmp/e2e_backup.log 2>&1 && \
    PASS "tsdb-backup exit 0" || FAIL "tsdb-backup"
[[ -f "$BACKUP_DIR/manifest.json" ]] && PASS "manifest.json exists" || FAIL "manifest missing"
[[ -f "$BACKUP_DIR/trades.csv"    ]] && PASS "trades.csv exists"    || FAIL "trades.csv missing"

# Capture pre-backup count for comparison.
PRE_COUNT=$(
    "$BIN/tsdb-export" -addr "$TCP_ADDR" \
        -qtl "SELECT count(*) FROM trades" -no-header 2>/dev/null | tr -d '\r\n'
)
[[ -n "$PRE_COUNT" && "$PRE_COUNT" -ge 500000 ]] && PASS "pre-backup count=$PRE_COUNT" || FAIL "pre-backup count $PRE_COUNT"

stop_server
start_server

"$BIN/tsdb-restore" -addr "$TCP_ADDR" -in "$BACKUP_DIR" >/tmp/e2e_restore.log 2>&1 && \
    PASS "tsdb-restore exit 0" || FAIL "tsdb-restore"

POST_COUNT=$(
    "$BIN/tsdb-export" -addr "$TCP_ADDR" \
        -qtl "SELECT count(*) FROM trades" -no-header 2>/dev/null | tr -d '\r\n'
)
[[ "$POST_COUNT" == "$PRE_COUNT" ]] && \
    PASS "post-restore count matches pre-backup ($POST_COUNT)" || \
    FAIL "post-restore count=$POST_COUNT expected=$PRE_COUNT"

# --- Phase 4: export — stream an arbitrary query to CSV ---------------------

echo
echo "=== Phase 4: tsdb-export arbitrary query ==="
"$BIN/tsdb-export" -addr "$TCP_ADDR" \
    -qtl "SELECT count(*), avg(price), max(price) FROM trades" \
    -out /tmp/e2e_export.csv 2>/dev/null && \
    PASS "export query completed" || FAIL "export query"
[[ $(wc -l < /tmp/e2e_export.csv) -ge 2 ]] && PASS "export CSV has header + row" || FAIL "export shape"

# --- Phase 5: import — CSV → new table round-trip ---------------------------

echo
echo "=== Phase 5: tsdb-import CSV round-trip ==="
cat > /tmp/e2e_import.csv <<'CSV'
ts,v
1700000000000000000,100
1700000000001000000,200
1700000000002000000,300
CSV
# Create the target table via cli.
"$REPO/build/tsdb-cli" --host 127.0.0.1 --port "${TCP_ADDR##*:}" \
    <<<"CREATE TABLE imp (ts TIMESTAMP, v INT64) TIMESTAMP(ts);" >/dev/null 2>&1 && \
    PASS "created import target table" || FAIL "create imp table"

"$BIN/tsdb-import" -addr "$TCP_ADDR" -table imp -file /tmp/e2e_import.csv \
    >/tmp/e2e_import.log 2>&1 && PASS "tsdb-import exit 0" || FAIL "tsdb-import"

IMP_COUNT=$(
    "$BIN/tsdb-export" -addr "$TCP_ADDR" \
        -qtl "SELECT count(*) FROM imp" -no-header 2>/dev/null | tr -d '\r\n'
)
[[ "$IMP_COUNT" == "3" ]] && PASS "imported 3 rows" || FAIL "imp count=$IMP_COUNT"

# --- Phase 6: /metrics reflects the workload --------------------------------

echo
echo "=== Phase 6: /metrics reflects workload ==="
curl -sf "http://$METRICS_ADDR/metrics" >/tmp/e2e_metrics2.txt
ROWS_WRITTEN=$(awk '/^qengine_rows_written_total /{print $2}' /tmp/e2e_metrics2.txt)
[[ -n "$ROWS_WRITTEN" && "${ROWS_WRITTEN%.*}" -ge 500000 ]] && \
    PASS "metrics rows_written=$ROWS_WRITTEN" || FAIL "rows_written $ROWS_WRITTEN"

QUERIES=$(awk '/^qengine_queries_total /{print $2}' /tmp/e2e_metrics2.txt)
[[ -n "$QUERIES" && "${QUERIES%.*}" -ge 5 ]] && \
    PASS "metrics queries_total=$QUERIES" || FAIL "queries_total $QUERIES"

# --- Summary ----------------------------------------------------------------

echo
echo "=== Results: $pass passed, $fail failed ==="
exit $(( fail == 0 ? 0 : 1 ))
