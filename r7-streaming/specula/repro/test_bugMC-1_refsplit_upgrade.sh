#!/usr/bin/env bash
# MC-1 reproduction: one table under two TableReference keys during a rolling upgrade
# (<=1.10 worker -> uuid-less DataWritten, >=1.11 worker -> DataWritten with the table uuid);
# the first group's commit stores the coordinator's whole control-topic position as the
# kafka.connect.offsets floor and the second group's DataWritten is dropped.
#
# System under test : pristine apache/iceberg @ 9fffeb6 (git archive of the run's worktree HEAD,
#                     no TLA instrumentation), module :iceberg-kafka-connect:iceberg-kafka-connect.
# Legacy peer       : released Apache Iceberg 1.10.0 jars only, separate JVM (LegacyPeer110).
# Driver            : JUnit test TestBugMC1RefSplit (real Coordinator, Worker, SinkWriter,
#                     HadoopCatalog table; Kafka clients = MockProducer/MockConsumer as in the
#                     module's own tests). Escalation level: 0 (variant A) / 1 (variant B timing aid).
#
# Usage: bash test_bugMC-1_refsplit_upgrade.sh      (exit 0 = bug triggered in every variant)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SUP="$HERE/test_bugMC-1_refsplit_upgrade"
OUT_ROOT="$(cd "$HERE/.." && pwd)"                       # .specula-output
WORK="$OUT_ROOT/confirmation/MC-1"
SRC="$WORK/build-pristine/src"
LAB="$WORK/lab"
LEGACY_DIR="$LAB/plugins/legacy/iceberg-1.10.0"
PEER_CLASSES="$LAB/peer-classes"
RUN_DIR="$LAB/run-$(date +%Y%m%d-%H%M%S)"
GRADLE_DIST="$(ls -d "$HOME"/.gradle/wrapper/dists/gradle-8.14.6-bin/*/gradle-8.14.6 2>/dev/null | head -1)"
mkdir -p "$RUN_DIR"

echo "==> [1/5] pristine source tree (apache/iceberg @ 9fffeb6, no instrumentation)"
if [ ! -f "$SRC/settings.gradle" ]; then
  mkdir -p "$SRC"
  git -C "$WORK/worktree" archive --format=tar HEAD | tar -x -C "$SRC" || { echo "git archive failed"; exit 2; }
  echo "1.13.0-SNAPSHOT" > "$SRC/version.txt"
fi
git -C "$WORK/worktree" rev-parse HEAD
diff -q <(git -C "$WORK/worktree" show HEAD:kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitState.java) \
        "$SRC/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitState.java" \
  && diff -q <(git -C "$WORK/worktree" show HEAD:kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java) \
        "$SRC/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java" \
  && echo "CommitState.java / Coordinator.java identical to HEAD (pristine)" \
  || { echo "pristine tree differs from HEAD"; exit 2; }
if grep -rq "TlaHooks" "$SRC/kafka-connect/kafka-connect/src/main"; then echo "instrumentation found in pristine tree"; exit 2; fi

echo "==> [2/5] released Iceberg Kafka Connect 1.10.0 jars (legacy peer classpath)"
if [ ! -f "$LEGACY_DIR/iceberg-kafka-connect-events-1.10.0.jar" ]; then
  mkdir -p "$LAB/assemble"
  cat > "$LAB/assemble/settings.gradle" <<'EOF'
rootProject.name = 'mc1-repro-assemble'
EOF
  cat > "$LAB/assemble/build.gradle" <<'EOF'
plugins { id 'java-base' }
repositories { mavenCentral() }
configurations { legacy { transitive = true }; legacyExtra { transitive = false } }
configurations.all {
  attributes {
    attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage, Usage.JAVA_RUNTIME))
    attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category, Category.LIBRARY))
    attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements, LibraryElements.JAR))
  }
}
dependencies {
  legacy 'org.apache.iceberg:iceberg-kafka-connect:1.10.0'
  legacy 'org.apache.iceberg:iceberg-parquet:1.10.0'
  legacyExtra 'org.apache.hadoop:hadoop-client-api:3.4.2'
  legacyExtra 'org.apache.hadoop:hadoop-client-runtime:3.4.2'
}
tasks.register('legacyPluginDir', Copy) {
  from configurations.legacy
  from configurations.legacyExtra
  into "${rootDir}/../plugins/legacy/iceberg-1.10.0"
}
EOF
  (cd "$LAB/assemble" && timeout 600 "$GRADLE_DIST/bin/gradle" --console=plain -q legacyPluginDir) \
    || { echo "legacy jar assembly failed"; exit 2; }
fi
ls "$LEGACY_DIR" | grep -E '^iceberg-' | tr '\n' ' '; echo
LEGACY_CP="$(ls "$LEGACY_DIR"/*.jar | tr '\n' ':' | sed 's/:$//')"

echo "==> [3/5] compile the legacy peer against the 1.10.0 jars only"
rm -rf "$PEER_CLASSES" && mkdir -p "$PEER_CLASSES"
timeout 120 javac -nowarn -d "$PEER_CLASSES" -cp "$LEGACY_CP" "$SUP/LegacyPeer110.java" || { echo "javac failed"; exit 2; }

echo "==> [4/5] install the JUnit reproduction into the pristine tree (test sources only)"
TEST_DIR="$SRC/kafka-connect/kafka-connect/src/test/java/org/apache/iceberg/connect/channel"
cp "$SUP/TestBugMC1RefSplit.java" "$TEST_DIR/TestBugMC1RefSplit.java"

echo "==> [5/5] run (work dir $RUN_DIR)"
LOG="$RUN_DIR/gradle-test.log"
(cd "$SRC" && timeout 1500 ./gradlew --offline -I "$SUP/mc1-test-init.gradle" \
    -DsparkVersions= -DflinkVersions= -DkafkaVersions=3 \
    -Dmc1.legacyCp="$LEGACY_CP" -Dmc1.peerClasses="$PEER_CLASSES" -Dmc1.workDir="$RUN_DIR" \
    :iceberg-kafka-connect:iceberg-kafka-connect:test \
    --tests 'org.apache.iceberg.connect.channel.TestBugMC1RefSplit' --console=plain) > "$LOG" 2>&1
RC=$?
[ "$RC" -eq 124 ] && echo "TIMEOUT (possible hang in the system under test)"
grep -E '\[MC-1\]|Coordinator .* (found nothing|completed commit)|Snapshot ancestry validation|BUILD|TestBugMC1RefSplit > |tests completed' "$LOG"

A=$(grep -c '\[MC-1\]\[A-threads1\] RESULT BUG-TRIGGERED' "$LOG")
B=$(grep -c '\[MC-1\]\[B-threads4\] RESULT BUG-TRIGGERED' "$LOG")
echo "gradle exit=$RC  variantA_triggered=$A  variantB_triggered=$B  log=$LOG"
if [ "$A" -ge 1 ] && [ "$B" -ge 1 ]; then
  echo "PASS: MC-1 triggered (records with committed source offsets are missing from db.tbl)"
  exit 0
fi
echo "FAIL: MC-1 not triggered in every variant"
exit 1
