#!/usr/bin/env bash
# usage: run-lockcommit.sh config runs threads commits
cd "$(dirname "$0")"
for i in $(seq 1 "$2"); do
  out=$(./gw -q run -Pmain=races.HadoopLockCommit -Pargs="$1 $3 $4" 2>&1)
  echo "$out" | grep -E "^RESULT" || { echo "NO RESULT:"; echo "$out" | head -20; exit 1; }
done
