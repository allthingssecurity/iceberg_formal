#!/usr/bin/env bash
# Runs the faithful workload against each released Iceberg the harness compiles against.
set -uo pipefail
cd "$(dirname "$0")"
SEEDS="${SEEDS:-1000}"
OUT=build/versions.txt
mkdir -p build
: > "$OUT"
for v in 1.12.0 1.11.0 1.10.2 1.9.2 1.8.1; do
  for mode in default aggressive dupdelete; do
    printf '%s %s ' "$v" "$mode" | tee -a "$OUT"
    ICEBERG_VERSION="$v" ./run.sh sweep "$SEEDS" "$mode" 2>/dev/null \
      | grep -E '^mode=' | tee -a "$OUT"
  done
done
echo "done -> $OUT"
