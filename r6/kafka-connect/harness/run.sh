#!/bin/bash
# usage: ./run.sh <icebergVersion> <scenario> <runs> [key=value ...]   (output capped at 120 lines)
set -euo pipefail
cd "$(dirname "$0")"
V=$1; shift
./gw -q -PicebergVersion=$V cp >/dev/null
J=/Users/I074560/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home/bin/java
mkdir -p out
LOG=out/$V-$(echo "$@" | tr " =" "_-").log
$J -Xmx1g -Dorg.slf4j.simpleLogger.defaultLogLevel=${LOGLEVEL:-off} -cp "$(cat build/$V/cp.txt)" harness.Main "$@" 2>&1 | head -c 400000 > "$LOG" || true
grep -E "run [0-9]+:|SUMMARY|Exception|Error" "$LOG" | head -${HEADN:-40}
