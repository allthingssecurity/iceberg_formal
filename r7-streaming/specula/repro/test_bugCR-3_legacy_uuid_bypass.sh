#!/usr/bin/env bash
# CR-3 reproduction: a DataWritten with a null table uuid bypasses the replaced-table guard in
# Coordinator.commitToTable (Coordinator.java:256), so files written for a DROPPED table are
# committed into a RE-CREATED table with the same identifier.
#
# Escalation level: 0 (black box). Only normal operations are used: a mixed-version sink (the
# released Apache Iceberg 1.10.2 sink from Maven Central next to this checkout's pristine sink),
# DROP TABLE / CREATE TABLE through a separate catalog client, and the coordinator's normal
# start()/process() loop. The Kafka brokers are replaced by Kafka's MockProducer/MockConsumer,
# which carry the exact bytes each peer produces. Nothing in the system under test is
# stubbed, patched or delayed.
#
# Scenarios:
#   A1  rolling upgrade, JdbcCatalog: a 1.10.2 task answers the current coordinator's
#       StartCommit; the table is dropped and re-created (id bigint -> string) in between
#   A1c CONTROL: same as A1, but the task runs this checkout's Worker (uuid-bearing DataWritten)
#   B1  full-stop upgrade, JdbcCatalog: 1.10.2 events left uncommitted in the control topic are
#       replayed by the upgraded coordinator (the #15011 "upgrade case")
#   A2  rolling upgrade, HadoopCatalog: DROP removes the table directory, so the re-created
#       table ends up referencing a deleted file
#   A2c CONTROL for A2
#
# Exit code 0 = CR-3 reproduced as described (A1, B1, A2 commit U1 files into U2; controls reject).
#
# Usage: bash test_bugCR-3_legacy_uuid_bypass.sh [scenario-name|all]
#        CR3_SRC=/path/to/pristine/iceberg bash test_bugCR-3_legacy_uuid_bypass.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
D="$HERE/test_bugCR-3_legacy_uuid_bypass"
OUT="$D/out"
SRC="${CR3_SRC:-$HERE/../confirmation/CR-3/worktree}"
SRC="$(cd "$SRC" && pwd)"
ONLY="${1:-all}"
mkdir -p "$OUT"

echo "== CR-3 repro  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "== system under test: $SRC @ $(git -C "$SRC" rev-parse --short HEAD)"

# ---- 0. preflight: the system under test must be the pristine upstream source -------------
dirty="$(git -C "$SRC" status --porcelain --untracked-files=all -- kafka-connect core api common data parquet | grep -v '/build/' || true)"
if [ -n "$dirty" ]; then
  echo "PREFLIGHT FAILED: source tree is not pristine (instrumentation present?):"
  echo "$dirty"
  echo "Point CR3_SRC at a pristine checkout, or revert the Specula trace instrumentation with:"
  echo "  ICEBERG_SRC=$SRC bash $HERE/../harness/clean.sh"
  exit 2
fi
echo "== preflight: kafka-connect/core/api/common/data/parquet sources identical to HEAD"
if [ ! -f "$SRC/gradle/wrapper/gradle-wrapper.jar" ]; then
  for cand in "$SRC/../../../../source/gradle/wrapper/gradle-wrapper.jar"; do
    [ -f "$cand" ] && cp "$cand" "$SRC/gradle/wrapper/" && echo "== copied gradle-wrapper.jar from $cand"
  done
fi

# ---- 1. build this checkout's sink + export its test runtime classpath ---------------------
echo "== [1/4] building pristine sink (gradle, offline)"
( cd "$SRC" && timeout 1500 ./gradlew --offline --console=plain -q \
    -DsparkVersions= -DflinkVersions= -DkafkaVersions=3 \
    -Dcr3.cpfile="$OUT/current.classpath" -I "$D/gradle/cr3-classpath.init.gradle" \
    :iceberg-kafka-connect:iceberg-kafka-connect:cr3PrintTestClasspath ) > "$OUT/gradle-build.log" 2>&1 \
  || { echo "BUILD FAILED"; tail -30 "$OUT/gradle-build.log"; exit 3; }
MAIN_CLASSES="$SRC/kafka-connect/kafka-connect/build/classes/java/main"
if javap -c -p -cp "$MAIN_CLASSES" org.apache.iceberg.connect.channel.Coordinator | grep -q -E "TlaHooks|TlaTrace|traceCtx"; then
  echo "PREFLIGHT FAILED: compiled Coordinator contains instrumentation"; exit 2
fi
echo "   compiled Coordinator.class is un-instrumented"

# ---- 2. the released 1.10.2 sink (legacy peer) from Maven Central ---------------------------
echo "== [2/4] resolving released Apache Iceberg 1.10.2 Kafka Connect sink (maven)"
if [ ! -f "$OUT/legacy-lib/iceberg-kafka-connect-1.10.2.jar" ]; then
  mkdir -p "$OUT/legacy-lib"
  ( cd /tmp && timeout 900 mvn -q -B -f "$D/legacy/pom.xml" dependency:copy-dependencies \
      -DoutputDirectory="$OUT/legacy-lib" -DincludeScope=runtime ) > "$OUT/mvn-legacy.log" 2>&1 \
    || { echo "MAVEN FAILED"; tail -30 "$OUT/mvn-legacy.log"; exit 3; }
fi
for a in iceberg-kafka-connect iceberg-kafka-connect-events; do
  want="$(curl -s --max-time 30 "https://repo1.maven.org/maven2/org/apache/iceberg/$a/1.10.2/$a-1.10.2.jar.sha1" | cut -c1-40 || true)"
  have="$(shasum -a 1 "$OUT/legacy-lib/$a-1.10.2.jar" | cut -d' ' -f1)"
  if [ -n "$want" ] && [ "$want" != "$have" ]; then echo "CHECKSUM MISMATCH for $a"; exit 3; fi
  echo "   $a-1.10.2.jar sha1=$have ${want:+(matches Maven Central)}"
done

# ---- 3. compile the two drivers ---------------------------------------------------------------
echo "== [3/4] compiling drivers"
rm -rf "$OUT/legacy-classes" "$OUT/current-classes"
mkdir -p "$OUT/legacy-classes" "$OUT/current-classes"
javac -nowarn -d "$OUT/legacy-classes" -cp "$OUT/legacy-lib/*" \
  $(find "$D/legacy/src" -name '*.java')
CURRENT_CP="$(cat "$OUT/current.classpath")"
javac -nowarn -d "$OUT/current-classes" -cp "$CURRENT_CP" \
  $(find "$D/current/src" -name '*.java')

# ---- 4. run -------------------------------------------------------------------------------------
echo "== [4/4] running scenarios"
set +e
timeout 600 java \
  -Dorg.slf4j.simpleLogger.logFile=System.out \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
  -Dorg.slf4j.simpleLogger.log.org.apache.iceberg.connect=info \
  -cp "$OUT/current-classes:$CURRENT_CP" \
  org.apache.iceberg.connect.channel.Cr3Repro "$OUT/run" "$OUT/legacy-classes:$OUT/legacy-lib/*" "$ONLY"
rc=$?
set -e
echo "== exit code $rc"
exit $rc
