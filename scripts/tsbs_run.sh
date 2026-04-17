#!/usr/bin/env bash
# tsbs_run.sh — one-shot TSBS cpu-only benchmark for tsdb
#
# Usage: ./scripts/tsbs_run.sh [scale=100] [hours=4]
#
# Builds bench binaries (if needed), wipes DB, generates data, then queries.

set -e

SCALE=${1:-100}
HOURS=${2:-4}
DB=/tmp/tsbs_tsdb
PROJ="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== TSBS cpu-only: scale=${SCALE} hours=${HOURS} ==="
echo "Project: ${PROJ}"
echo "DB:      ${DB}"
echo ""

# Build bench targets.
cd "${PROJ}"
make bench 2>&1 | grep -E "^(CC|LD|AR|---)" || true

echo ""
echo "--- Cleanup & generate ---"
rm -rf "${DB}"
time "${PROJ}/build/bench/tsbs_cpu_gen" "${DB}" "${SCALE}" "${HOURS}"

echo ""
echo "--- Query benchmark ---"
time "${PROJ}/build/bench/tsbs_cpu_query" "${DB}"

echo ""
echo "--- Disk usage ---"
du -sh "${DB}" 2>/dev/null || echo "(du not available)"
