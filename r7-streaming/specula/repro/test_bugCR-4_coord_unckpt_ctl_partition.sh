#!/usr/bin/env bash
# CR-4 reproduction driver (Specula bug confirmation, Level 0 = black-box, public operations only).
#
# Builds the UNMODIFIED Iceberg Kafka Connect sink from the CR-4 worktree's HEAD (9fffeb6) in a
# pristine copy (the worktree itself carries TLA trace instrumentation, which is reverted in the
# copy via read-only `git show HEAD:<path>`), adds one extra Gradle source set (cr4Repro) holding
# test_bugCR-4_coord_unckpt_ctl_partition.java, and runs it: embedded Kafka 3.9.2 (KRaft) broker +
# Kafka Connect cluster + IcebergSinkConnector (2 tasks) + multi-partition control topic.
#
# Usage:  bash repro/test_bugCR-4_coord_unckpt_ctl_partition.sh [pristine|pristine-1ctl|pr18006|both|all]
#   pristine (default): unmodified HEAD -> expect loss exactly when the answering worker's control
#                       partition has no -coord checkpoint (UNCKPT), no loss otherwise (CKPT).
#   pristine-1ctl:      control: unmodified HEAD, same scenario, 1-partition control topic ->
#                       the answering partition is always checkpointed -> expect no loss.
#   pr18006:            fix-validation control: HEAD + the coordinator-only `earliest` default of
#                       open PR apache/iceberg#18006 (ported to HEAD's Channel signature).
#   env: CR4_CTL_PARTITIONS (2), CR4_ATTEMPTS (8), CR4_MIN_UNCKPT (2), CR4_MIN_CKPT (1)
set -uo pipefail

VARIANTS="${1:-pristine}"
[ "$VARIANTS" = "both" ] && VARIANTS="pristine pr18006"
[ "$VARIANTS" = "all" ] && VARIANTS="pristine pristine-1ctl pr18006"

REPRO_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="$(cd "$REPRO_DIR/.." && pwd)"                      # .specula-output
CR4_DIR="$OUT_DIR/confirmation/CR-4"
WT="$CR4_DIR/worktree"
TEST_SRC="$REPRO_DIR/test_bugCR-4_coord_unckpt_ctl_partition.java"
REPORT_DIR="$REPRO_DIR/CR-4.out"
GRADLE_PROJECT=":iceberg-kafka-connect:iceberg-kafka-connect"
mkdir -p "$REPORT_DIR"

die() { echo "ERROR: $*" >&2; exit 2; }
[ -d "$WT" ] || die "worktree not found: $WT"
[ -f "$TEST_SRC" ] || die "test source not found: $TEST_SRC"

# --- 1. pristine copy of HEAD (no git writes anywhere: the worktree is a linked git worktree) ----
make_pristine() {
  local copy="$1"
  if [ -f "$copy/.cr4-pristine-ok" ]; then
    return 0
  fi
  echo "==> creating pristine copy of $WT HEAD at $copy"
  mkdir -p "$copy"
  rsync -a --exclude '.git' --exclude 'build/' --exclude '.gradle/' "$WT/" "$copy/" || die "rsync"
  local f
  while IFS= read -r f; do
    git -C "$WT" show "HEAD:$f" > "${copy:?}/${f:?}" || die "restore $f"
  done < <(git -C "$WT" diff --name-only HEAD)
  while IFS= read -r f; do
    rm -f "${copy:?}/${f:?}"
  done < <(git -C "$WT" ls-files --others --exclude-standard)
  rm -rf "${copy:?}/kafka-connect/kafka-connect/src/tlaHarness" \
         "${copy:?}/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/tla"
  echo "9fffeb6" > "$copy/version.txt"
  # gradlew downloads gradle-wrapper.jar (v8.14.6) when missing; reuse the copy the run's source
  # checkout already fetched for the trace-harness build (same version, known-good)
  if [ ! -e "$copy/gradle/wrapper/gradle-wrapper.jar" ] && [ -e "$OUT_DIR/../source/gradle/wrapper/gradle-wrapper.jar" ]; then
    cp "$OUT_DIR/../source/gradle/wrapper/gradle-wrapper.jar" "$copy/gradle/wrapper/gradle-wrapper.jar"
  fi
  local sha; sha="$(git -C "$WT" rev-parse HEAD)"
  printf 'git.branch=main\ngit.build.version=9fffeb6\ngit.closest.tag.name=\ngit.commit.id=%s\ngit.commit.id.abbrev=%s\ngit.commit.message.short=%s\ngit.commit.time=%s\ngit.tags=\n' \
    "$sha" "${sha:0:7}" "$(git -C "$WT" log -1 --format=%s)" "$(git -C "$WT" log -1 --format=%cI)" \
    > "$copy/iceberg-build.properties"
  mkdir -p "$copy/build"
  # the kafka-connect main sources must equal HEAD byte for byte
  local p
  while IFS= read -r p; do
    git -C "$WT" show "HEAD:$p" | cmp -s - "$copy/$p" || die "copy differs from HEAD: $p"
  done < <(git -C "$WT" ls-files kafka-connect/kafka-connect/src/main kafka-connect/kafka-connect-events/src/main)
  touch "$copy/.cr4-pristine-ok"
}

# --- 2. the extra source set + test task (appended as a separate project block if missing) ----
add_source_set() {
  local copy="$1" gradle="$1/kafka-connect/build.gradle"
  if grep -q "cr4ReproTest" "$gradle"; then
    return 0
  fi
  cat >> "$gradle" <<'GRADLE'

// CR-4 reproduction (Specula bug confirmation): extra source set + test task only.
project(':iceberg-kafka-connect:iceberg-kafka-connect') {
  sourceSets {
    cr4Repro {
      compileClasspath += sourceSets.main.output
      runtimeClasspath += sourceSets.main.output
    }
  }
  configurations {
    cr4ReproImplementation.extendsFrom testImplementation
    cr4ReproRuntimeOnly.extendsFrom testRuntimeOnly
  }
  dependencies {
    def kafkaVersion = libs.versions.kafka.get()
    cr4ReproImplementation libs.kafka.clients
    cr4ReproImplementation libs.kafka.connect.api
    cr4ReproImplementation libs.kafka.connect.json
    cr4ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}"
    cr4ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}:test"
    cr4ReproImplementation "org.apache.kafka:kafka-clients:${kafkaVersion}:test"
    cr4ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}"
    cr4ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}:test"
    cr4ReproImplementation "org.apache.kafka:kafka-server-common:${kafkaVersion}:test"
  }
  pluginManager.withPlugin('net.ltgt.errorprone') {
    tasks.matching { it.name == 'compileCr4ReproJava' }.configureEach {
      options.errorprone.enabled = false
    }
  }
  task cr4ReproTest(type: Test) {
    useJUnitPlatform()
    testClassesDirs = sourceSets.cr4Repro.output.classesDirs
    classpath = sourceSets.cr4Repro.runtimeClasspath
    forkEvery = 1
    maxParallelForks = 1
    maxHeapSize = "3g"
    jvmArgs += project.property('extraJvmArgs')
    ['cr4.out', 'cr4.attempts', 'cr4.ctlPartitions', 'cr4.minUnckpt', 'cr4.minCkpt', 'cr4.variant'].each { p ->
      if (System.getProperty(p) != null) {
        systemProperty p, System.getProperty(p)
      }
    }
    systemProperty 'org.slf4j.simpleLogger.defaultLogLevel', 'warn'
    outputs.upToDateWhen { false }
    testLogging {
      events "started", "passed", "skipped", "failed"
      exceptionFormat "full"
      showStandardStreams = true
    }
  }
}
GRADLE
}

install_test() {
  local copy="$1"
  local dst="$copy/kafka-connect/kafka-connect/src/cr4Repro"
  mkdir -p "$dst/java/org/apache/iceberg/connect/cr4" "$dst/resources"
  cp "$TEST_SRC" "$dst/java/org/apache/iceberg/connect/cr4/Cr4CoordUncheckpointedPartitionTest.java"
  cat > "$dst/resources/simplelogger.properties" <<'EOF'
org.slf4j.simpleLogger.defaultLogLevel=warn
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS
org.slf4j.simpleLogger.log.org.apache.iceberg.connect.channel=info
EOF
  cat > "$dst/resources/log4j.properties" <<'EOF'
log4j.rootLogger=WARN, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{HH:mm:ss.SSS} %-5p [%t] %c{1}: %m%n
log4j.logger.org.apache.iceberg.connect.channel=INFO
EOF
}

# --- 3. fix-validation variant: port of PR #18006 (coordinator consumer defaults to earliest) ---
apply_pr18006() {
  local copy="$1"
  [ -f "$copy/.cr4-pr18006-applied" ] && return 0
  python3 -I - "$copy/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel" <<'PY' || die "pr18006 port failed"
import pathlib, sys
d = pathlib.Path(sys.argv[1])
def sub(name, old, new):
    p = d / name
    s = p.read_text()
    assert s.count(old) == 1, (name, old)
    p.write_text(s.replace(old, new))
sub("KafkaClientFactory.java",
    "  Consumer<String, byte[]> createConsumer(String consumerGroupId) {\n"
    "    Map<String, Object> consumerProps = Maps.newHashMap(kafkaProps);\n"
    "    consumerProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, \"latest\");\n",
    "  Consumer<String, byte[]> createConsumer(String consumerGroupId) {\n"
    "    return createConsumer(consumerGroupId, \"latest\");\n"
    "  }\n\n"
    "  Consumer<String, byte[]> createConsumer(String consumerGroupId, String autoOffsetReset) {\n"
    "    Map<String, Object> consumerProps = Maps.newHashMap(kafkaProps);\n"
    "    consumerProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);\n")
sub("Channel.java",
    "      KafkaClientFactory clientFactory,\n      SinkTaskContext context) {\n    this.controlTopic",
    "      KafkaClientFactory clientFactory,\n      SinkTaskContext context) {\n"
    "    this(consumerGroupId, transactionalId, config, clientFactory, context, \"latest\");\n  }\n\n"
    "  Channel(\n      String consumerGroupId,\n      String transactionalId,\n      IcebergSinkConfig config,\n"
    "      KafkaClientFactory clientFactory,\n      SinkTaskContext context,\n      String autoOffsetReset) {\n"
    "    this.controlTopic")
sub("Channel.java",
    "this.consumer = clientFactory.createConsumer(consumerGroupId);",
    "this.consumer = clientFactory.createConsumer(consumerGroupId, autoOffsetReset);")
sub("Coordinator.java",
    "        clientFactory,\n        context);\n\n    this.catalog = catalog;",
    "        clientFactory,\n        context,\n        \"earliest\");\n\n    this.catalog = catalog;")
print("applied PR #18006-equivalent change (coordinator consumer default: earliest)")
PY
  touch "$copy/.cr4-pr18006-applied"
}

run_variant() {
  local variant="$1" copy
  local ctl="${CR4_CTL_PARTITIONS:-2}" attempts="${CR4_ATTEMPTS:-8}"
  local min_unckpt="${CR4_MIN_UNCKPT:-2}" min_ckpt="${CR4_MIN_CKPT:-1}"
  case "$variant" in
    pristine) copy="$CR4_DIR/pristine-src"; make_pristine "$copy" ;;
    pristine-1ctl)
      # control: unmodified HEAD, same scenario, 1-partition control topic (always checkpointed)
      copy="$CR4_DIR/pristine-src"; make_pristine "$copy"
      ctl=1; attempts="${CR4_ATTEMPTS:-2}"; min_unckpt=0; min_ckpt="${CR4_MIN_CKPT:-2}" ;;
    pr18006)
      copy="$CR4_DIR/pr18006-src"
      if [ ! -f "$copy/.cr4-pristine-ok" ]; then
        make_pristine "$CR4_DIR/pristine-src"
        mkdir -p "$copy"
        rsync -a --exclude 'build/' --exclude '.gradle/' --exclude 'kafka-connect/kafka-connect/src/cr4Repro/' \
          "$CR4_DIR/pristine-src/" "$copy/" || die "rsync pr18006"
        mkdir -p "$copy/build"
      fi
      apply_pr18006 "$copy" ;;
    *) die "unknown variant $variant" ;;
  esac
  add_source_set "$copy"
  install_test "$copy"

  local log="$REPRO_DIR/CR-4.$variant.gradle.log"
  local task="cr4ReproTest"
  [ "${CR4_COMPILE_ONLY:-0}" = "1" ] && task="compileCr4ReproJava"
  echo "==> [$variant] building + running $task in $copy (log: $log)"
  (
    cd "$copy" || exit 2
    timeout 3600 ./gradlew "$GRADLE_PROJECT:$task" --offline --console=plain \
      -Dcr4.variant="$variant" -Dcr4.out="$REPORT_DIR" \
      -Dcr4.ctlPartitions="$ctl" -Dcr4.attempts="$attempts" \
      -Dcr4.minUnckpt="$min_unckpt" -Dcr4.minCkpt="$min_ckpt"
  ) > "$log" 2>&1
  local rc=$?
  [ $rc -eq 124 ] && echo "TIMEOUT (outer 60 min) in variant $variant -- a hang is itself a finding"
  echo "==> [$variant] gradle exit code: $rc"
  echo "---- [$variant] CR-4 report lines ----"
  grep -h "\[CR-4\]" "$log" | sed 's/^/  /'
  grep -E "BUILD (SUCCESSFUL|FAILED)|tests completed|FAILED$|PASSED$" "$log" | sed 's/^/  /'
  return $rc
}

status=0
for v in $VARIANTS; do
  run_variant "$v" || status=1
done
exit $status
