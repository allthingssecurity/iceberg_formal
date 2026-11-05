#!/usr/bin/env bash
# CR-9 reproduction driver (bug-confirmation Phase 2).
#
# Builds and runs the PRISTINE sink (apache/iceberg@9fffeb6, kafka-connect tree restored to HEAD and
# verified below) and executes the CR-9 unit reproduction against it. The reproduction is a JUnit
# test placed in the module's own test source set (src/test), so it links the real
# Coordinator / CoordinatorThread / CommitterImpl / Tasks.throwOne / isFenced production code.
#
# Level 2 (state injection) via the module's own test seams only: a spied InMemoryCatalog (the
# developers' own idiom in TestCoordinator.testCoordinatorCommittedOffsetValidation) to represent
# "a successor coordinator already committed the table", and the real exception classes. No
# production source is modified (asserted below: src/main == HEAD).
#
# Usage:  bash test_bugCR-9_fenced_zombie_task_failure.sh
# Exit:   0 = test ran and PASSED (bug reproduced); 1 = test FAILED/not reproduced; 2 = build/setup error.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_ROOT="$(cd "$HERE/.." && pwd)"                                   # .specula-output
WT="${CR9_WORKTREE:-$OUT_ROOT/confirmation/CR-9/worktree}"
LOG="$HERE/CR-9.log"
PROJECT=":iceberg-kafka-connect:iceberg-kafka-connect"
PKG_DIR="kafka-connect/kafka-connect/src/test/java/org/apache/iceberg/connect/channel"
TEST_CLASS="org.apache.iceberg.connect.channel.TestBugCR9FencedZombieTaskFailure"

echo "==> CR-9 reproduction, worktree $WT" | tee "$LOG"
cd "$WT" || exit 2

# 1. restore the pristine sink (drop the uncommitted TLA trace instrumentation in kafka-connect/)
git checkout HEAD -- kafka-connect/kafka-connect/src/main kafka-connect/build.gradle 2>>"$LOG" || exit 2
git clean -fdq -- kafka-connect/kafka-connect/src/main kafka-connect/kafka-connect/src/tlaHarness 2>>"$LOG" || exit 2
if ! git diff --quiet HEAD -- kafka-connect/kafka-connect/src/main kafka-connect/build.gradle; then
  echo "    kafka-connect production tree differs from HEAD after restore" | tee -a "$LOG"; exit 2
fi
echo "    kafka-connect production sources == HEAD ($(git rev-parse --short HEAD)); pristine" | tee -a "$LOG"

# 2. install the reproduction test into the module's test source set
mkdir -p "$PKG_DIR"
cp "$HERE/test_bugCR-9_fenced_zombie_task_failure.java" "$PKG_DIR/TestBugCR9FencedZombieTaskFailure.java" || exit 2
if ! git diff --quiet HEAD -- kafka-connect/kafka-connect/src/main; then
  echo "    production (src/main) changed?!" | tee -a "$LOG"; exit 2
fi
echo "    test installed; src/main still pristine" | tee -a "$LOG"

# 3. build + run just the CR-9 test (limit engines to kafka to avoid building spark/flink)
GRADLE_FLAGS="-DsparkVersions= -DflinkVersions= -DkafkaVersions=3 --console=plain"
echo "==> Running $TEST_CLASS" | tee -a "$LOG"
timeout 1800 ./gradlew $GRADLE_FLAGS "$PROJECT:test" --tests "$TEST_CLASS" >> "$LOG" 2>&1
rc=$?

echo "" | tee -a "$LOG"
echo "==> gradle exit=$rc" | tee -a "$LOG"
grep -E "^CR9\| |PASSED|FAILED|BUILD SUCCESSFUL|BUILD FAILED|tests completed|ValidationException|NotRunningException" "$LOG" \
  | grep -v "^\s*at " | sed 's/^/    /' | tail -60

# clean the installed test so the worktree is left as we found it (pristine)
rm -f "$PKG_DIR/TestBugCR9FencedZombieTaskFailure.java"

if [ "$rc" -eq 124 ]; then echo "    TIMEOUT"; exit 2; fi
if [ "$rc" -eq 0 ]; then echo "    => CR-9 REPRODUCED (test passed)"; exit 0; fi
if grep -q "BUILD FAILED" "$LOG" && ! grep -q "tests completed" "$LOG"; then echo "    => build/setup error"; exit 2; fi
echo "    => test did not pass"; exit 1
