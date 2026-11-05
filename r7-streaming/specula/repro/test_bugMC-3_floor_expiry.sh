#!/bin/bash
# MC-3 reproduction: the Coordinator's replay floor (kafka.connect.offsets summary) is reachable
# only through the ancestry of the connector's own snapshots; after a foreign snapshot + expiry of
# the connector's snapshot, a replayed (already committed) DataWritten is committed again.
#
# Runs against the PRISTINE sink (apache/iceberg@9fffeb6). Two harnesses, no sink code modified:
#   unit/  TestMc3CrashReplayFloor      real Coordinator + Kafka MockProducer/MockConsumer,
#          exact CE replay (crash at Coordinator.java:216) + control + in-process partial retry
#   e2e/   Mc3E2E*Test                  embedded Kafka 3.9.2 KRaft broker + Kafka Connect worker
#          running the real IcebergSinkConnector, REST restart, real transactions/consumer groups
#          (Level0NoFault: no fault at all; the others: catalog applies the commit and reports
#          CommitStateUnknownException once -- a fault of the declared failure model)
#
# Usage: bash test_bugMC-3_floor_expiry.sh [ICEBERG_WORKTREE]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SRCDIR="$HERE/test_bugMC-3_floor_expiry"
W="${1:-$HERE/../confirmation/MC-3/worktree}"
W="$(cd "$W" && pwd)"
OUT="$HERE/test_bugMC-3_floor_expiry/out"
PROJ=":iceberg-kafka-connect:iceberg-kafka-connect"
KC="$W/kafka-connect/kafka-connect"
rm -rf "$OUT"; mkdir -p "$OUT"

echo "== worktree: $W ($(git -C "$W" rev-parse --short HEAD))"
# 1. the system under test must be pristine: no change and no extra file under src/main
if [ -n "$(git -C "$W" status --porcelain -- kafka-connect/kafka-connect/src/main kafka-connect/kafka-connect-events/src/main core/src/main api/src/main)" ]; then
  echo "ABORT: sink/core main sources are not pristine:"; git -C "$W" status --porcelain -- kafka-connect/kafka-connect/src/main core/src/main api/src/main; exit 2
fi
echo "== sink + core main sources pristine at HEAD (git status clean under src/main)"
# 2. test-only Gradle source set 'tlaHarness' (embedded Kafka/Connect test jars); no main change
if ! grep -q "tlaHarnessTest" "$W/kafka-connect/build.gradle"; then
  git -C "$W" apply "$SRCDIR/build.gradle.patch" || { echo "ABORT: cannot apply build.gradle.patch"; exit 2; }
fi
# 3. install the reproduction sources (test source set only)
mkdir -p "$KC/src/tlaHarness/java/org/apache/iceberg/connect/mc3" \
         "$KC/src/tlaHarness/java/org/apache/iceberg/connect/channel" "$KC/src/tlaHarness/resources"
cp "$SRCDIR"/e2e/*.java "$KC/src/tlaHarness/java/org/apache/iceberg/connect/mc3/"
cp "$SRCDIR"/unit/*.java "$KC/src/tlaHarness/java/org/apache/iceberg/connect/channel/"
cp "$SRCDIR/simplelogger.properties" "$KC/src/tlaHarness/resources/simplelogger.properties"

cd "$W" || exit 2
echo "== build (compileTlaHarnessJava)"
if ! timeout 1500 ./gradlew "$PROJ:compileTlaHarnessJava" --console=plain -q ${GRADLE_FLAGS:---offline} > "$OUT/build.log" 2>&1; then
  echo "BUILD FAILED"; tail -40 "$OUT/build.log"; exit 3
fi

run() { # $1 = test filter, $2 = log name
  echo "== run $1"
  timeout 900 ./gradlew "$PROJ:tlaHarnessTest" --tests "$1" -Dtla.trace.dir="$OUT" \
      --console=plain ${GRADLE_FLAGS:---offline} > "$OUT/$2.gradle.log" 2>&1
  local rc=$?
  cp "$KC"/build/test-results/tlaHarnessTest/TEST-*.xml "$OUT/" 2>/dev/null
  [ $rc -eq 124 ] && echo "   TIMEOUT (possible hang) for $1"
  grep -E "PASSED|FAILED|SKIPPED" "$OUT/$2.gradle.log" | sed 's/^/   /'
  return $rc
}
status=0
# Level 0 (no fault anywhere): expected NOT to trigger (no replay without a failure)
run "org.apache.iceberg.connect.mc3.Mc3E2ELevel0NoFaultTest" Mc3E2ELevel0NoFaultTest || status=1
# Level 1 (one failure-model fault): unit CE replay + e2e variants and control
run "org.apache.iceberg.connect.channel.TestMc3CrashReplayFloor" unit || status=1
for t in Mc3E2EControlNoExpiryTest Mc3E2EForeignAppendExpiryTest Mc3E2ERewriteManifestsExpiryTest; do
  run "org.apache.iceberg.connect.mc3.$t" "$t" || status=1
done

echo; echo "== reports"
for f in "$OUT"/MC-3_*.txt; do echo "---------- $(basename "$f")"; cat "$f"; done
echo; echo "== sink log lines (JUnit XML system-err): replay re-read, coordinator failure, table commits"
for x in "$OUT"/TEST-*.xml; do
  [ -e "$x" ] || continue
  echo "---------- $(basename "$x")"
  grep -E "Received commit (response|ready) when no commit in progress|Coordinator error during process|completed commit to table|found nothing to commit|elected leader" "$x" \
    | sed -E 's/^.*(INFO|WARN|ERROR) /\1 /' | cut -c1-230
done
echo
if [ $status -eq 0 ]; then
  echo "MC-3 REPRODUCED (Level 1): duplicates in every floor-unreachable variant, none in the control; Level 0 (no fault) not triggered"
else
  echo "MC-3: some runs did not pass (see logs in $OUT)"
fi
exit $status
