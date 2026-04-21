#!/usr/bin/env bash
# Run every scenario in order; stop on first failure unless KEEP_GOING=1.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

keep_going="${KEEP_GOING:-0}"
failed=0
passed=0

skipped=()
if [[ -f "$here/skip.txt" ]]; then
  while IFS= read -r line; do
    [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
    skipped+=("$line")
  done < "$here/skip.txt"
fi

for s in "$here/scenarios/"s*.sh; do
  name="$(basename "$s" .sh)"
  if [[ " ${skipped[*]} " == *" $name "* ]]; then
    printf '\033[1;33mSKIP\033[0m %s\n' "$name"
    continue
  fi
  printf '\033[1;36m---\033[0m running %s\n' "$name"
  if bash "$s"; then
    passed=$(( passed + 1 ))
  else
    failed=$(( failed + 1 ))
    [[ "$keep_going" == "1" ]] || break
  fi
done

printf '\n=== %d passed, %d failed, %d skipped ===\n' \
       "$passed" "$failed" "${#skipped[@]}"
exit $(( failed > 0 ? 1 : 0 ))
