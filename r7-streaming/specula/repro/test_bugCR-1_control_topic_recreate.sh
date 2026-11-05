#!/usr/bin/env bash
# =================================================================================================
# CR-1 reproduction: control-topic offsets carry no topic identity.
#
# Mechanism under test (pristine upstream code, HEAD 9fffeb6 == apache/iceberg main for kafka-connect):
#   * Channel.consumeAvailable (Channel.java:126-139) skips every control record whose offset is below
#     the in-memory high-water mark controlTopicOffsets, keyed by partition number only.
#   * Coordinator.commitToTable (Coordinator.java:108-110, 270-285) filters DataWritten envelopes
#     below the floor stored in the snapshot summary under kafka.connect.offsets.<topic>.<group>.
#   When the control topic is deleted and recreated, its offsets restart at 0.
#
# Level 0 (black box). The test uses a real in-JVM Kafka broker (KRaft) and a 2-worker Kafka Connect
# cluster (EmbeddedConnectCluster), and runs the UNMODIFIED IcebergSinkConnector with 2 tasks against
# an in-memory Iceberg catalog. It acts only through public interfaces:
#   - Kafka Admin API: delete and recreate the control topic, the operator action
#   - Kafka producer: source records
#   - Connect REST API: restart a task, the operator action
#   - Iceberg catalog API: read the table
# There are no failpoints, no source patches, and no injected state. The sink sources are exported
# with `git archive HEAD` and verified byte-for-byte against git before the run. Only a test-only
# Gradle source set (src/crRepro) is added.
#
# Phases of test 1 (recreatedControlTopic):
#   A  normal operation; every record reaches the table (harness control)
#   B  operator deletes and recreates the control topic while the connector is RUNNING
#        -> STALL: no table commit; running channels skip the new topic (HWM)
#   C  routine restart of the NON-leader task (cooperative rebalance keeps the coordinator)
#        -> LOSS: fresh workers commit source offsets with DataWritten; the coordinator's stale HWM skips them
#   D  later: the loss is permanent
#   E  restart of the leader task (fresh coordinator) -> the KNOWN stored-floor filter (#15293/#16360)
# Test 2 (controlNoRecreateTaskRestartLosesNothing): the same task restart without recreating the
# control topic loses nothing (negative control).
# Test 3 (recreatedControlTopicDefaultEagerAssignor, informational): phases A-C with the Kafka
# consumer's DEFAULT assignors (eager). It reports which mechanism dropped the records.
#
# Usage: bash repro/test_bugCR-1_control_topic_recreate.sh
#   CR1_REPO  git checkout of the target commit (default: confirmation/CR-1/worktree; HEAD is used)
#   CR1_WORK  pristine export directory (default: confirmation/CR-1/pristine-src)
#   CR1_TESTS optional Gradle --tests filter (development only)
# Exit 0 = CR-1 HWM consequence reproduced AND the negative control passed.
# =================================================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO="${CR1_REPO:-$OUT_DIR/confirmation/CR-1/worktree}"
WORK="${CR1_WORK:-$OUT_DIR/confirmation/CR-1/pristine-src}"
SINK_LOG="${CR1_SINK_LOG:-$SCRIPT_DIR/CR-1.sink.log}"
GRADLE_LOG="${CR1_GRADLE_LOG:-$SCRIPT_DIR/CR-1.gradle-output.log}"
MOD="kafka-connect/kafka-connect"
TEST_DIR="$WORK/$MOD/src/crRepro"

# ---- 1. pristine export of the target commit ---------------------------------------------------
HEAD_SHA="$(git -C "$REPO" rev-parse HEAD)" || { echo "not a git checkout: $REPO"; exit 2; }
if [ ! -f "$WORK/settings.gradle" ]; then
  mkdir -p "$WORK"
  git -C "$REPO" archive --format=tar HEAD | tar -x -C "$WORK" || { echo "export failed"; exit 2; }
fi

# ---- 2. verify the system under test is byte-identical to git HEAD -----------------------------
bad=0
while IFS= read -r f; do
  if ! git -C "$REPO" show "HEAD:$f" | cmp -s - "$WORK/$f"; then echo "MODIFIED vs HEAD: $f"; bad=1; fi
done < <(git -C "$REPO" ls-tree -r --name-only HEAD -- "$MOD/src/main" "kafka-connect/kafka-connect-events/src/main")
while IFS= read -r f; do
  rel="${f#"$WORK"/}"
  git -C "$REPO" cat-file -e "HEAD:$rel" 2>/dev/null || { echo "NOT IN HEAD: $rel"; bad=1; }
done < <(find "$WORK/$MOD/src/main" "$WORK/kafka-connect/kafka-connect-events/src/main" -type f)
[ "$bad" -eq 0 ] || { echo "export is not pristine - refusing to run"; exit 2; }
echo "==> sink sources verified identical to $HEAD_SHA (no instrumentation)"

# ---- 3. test-only Gradle source set ---------------------------------------------------------------
if ! grep -q "CR1-REPRO-SOURCESET" "$WORK/kafka-connect/build.gradle"; then
  cat >> "$WORK/kafka-connect/build.gradle" <<'GRADLE'

// CR1-REPRO-SOURCESET: Specula CR-1 reproduction (test sources only; src/main untouched).
// Embedded Kafka (KRaft) + Kafka Connect cluster running the unmodified sink.
project(':iceberg-kafka-connect:iceberg-kafka-connect') {
  sourceSets {
    crRepro {
      compileClasspath += sourceSets.main.output
      runtimeClasspath += sourceSets.main.output
    }
  }

  configurations {
    crReproImplementation.extendsFrom testImplementation
    crReproRuntimeOnly.extendsFrom testRuntimeOnly
  }

  dependencies {
    def kafkaVersion = libs.versions.kafka.get()
    crReproImplementation libs.kafka.clients
    crReproImplementation libs.kafka.connect.api
    crReproImplementation libs.kafka.connect.json
    crReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}"
    crReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}:test"
    crReproImplementation "org.apache.kafka:kafka-clients:${kafkaVersion}:test"
    crReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}"
    crReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}:test"
    crReproImplementation "org.apache.kafka:kafka-server-common:${kafkaVersion}:test"
  }

  pluginManager.withPlugin('net.ltgt.errorprone') {
    tasks.matching { it.name == 'compileCrReproJava' }.configureEach {
      options.errorprone.enabled = false
    }
  }

  task crReproTest(type: Test) {
    description = 'Specula CR-1 reproduction'
    useJUnitPlatform()
    testClassesDirs = sourceSets.crRepro.output.classesDirs
    classpath = sourceSets.crRepro.runtimeClasspath
    forkEvery = 1
    maxParallelForks = 1
    maxHeapSize = "3g"
    jvmArgs += project.property('extraJvmArgs')
    def sinkLog = System.getProperty('cr1.log.file', "${buildDir}/cr1/cr1-sink.log")
    systemProperty 'cr1.log.file', sinkLog
    systemProperty 'org.slf4j.simpleLogger.logFile', sinkLog
    systemProperty 'org.slf4j.simpleLogger.defaultLogLevel', 'warn'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.iceberg.connect.channel', 'debug'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.iceberg.connect.cr1', 'info'
    systemProperty 'org.slf4j.simpleLogger.showDateTime', 'true'
    systemProperty 'org.slf4j.simpleLogger.dateTimeFormat', 'HH:mm:ss.SSS'
    outputs.upToDateWhen { false }
    testLogging {
      events "started", "passed", "skipped", "failed"
      exceptionFormat "full"
      showStandardStreams = true
    }
  }
}
GRADLE
fi

# ---- 4. test sources ------------------------------------------------------------------------------
mkdir -p "$TEST_DIR/java/org/apache/iceberg/connect/cr1" "$TEST_DIR/resources"

cat > "$TEST_DIR/resources/log4j.properties" <<'EOF'
# reload4j is on the classpath without an SLF4J binding (slf4j-simple is the provider); keep it quiet
log4j.rootLogger=WARN, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{HH:mm:ss.SSS} %-5p [%t] %c{1}: %m%n
EOF

cat > "$TEST_DIR/java/org/apache/iceberg/connect/cr1/SharedInMemoryCatalog.java" <<'JAVA'
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.cr1;

import java.util.Map;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

/**
 * Plain Iceberg InMemoryCatalog shared by every sink task of the JVM: each task instantiates its
 * own catalog through iceberg.catalog.catalog-impl, and all table operations go to one root. No
 * fault injection; this is only the environment (a correct CAS catalog).
 */
public class SharedInMemoryCatalog extends InMemoryCatalog {
  private static volatile SharedInMemoryCatalog root = null;

  public static synchronized SharedInMemoryCatalog newRoot() {
    SharedInMemoryCatalog catalog = new SharedInMemoryCatalog();
    catalog.initialize("iceberg", Map.of());
    root = catalog;
    return catalog;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    SharedInMemoryCatalog shared = root;
    if (shared == null || this == shared) {
      return super.newTableOps(tableIdentifier);
    }

    return shared.newTableOps(tableIdentifier);
  }
}
JAVA

cat > "$TEST_DIR/java/org/apache/iceberg/connect/cr1/CR1ControlTopicRecreateTest.java" <<'JAVA'
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.cr1;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConnector;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CR1ControlTopicRecreateTest {
  private static final Logger LOG = LoggerFactory.getLogger(CR1ControlTopicRecreateTest.class);

  private static final String CONNECTOR = "iceberg-sink";
  private static final String CONNECT_GROUP = "connect-" + CONNECTOR;
  private static final String COORD_GROUP = CONNECT_GROUP + "-coord";
  private static final String SRC = "src";
  private static final String CTL = "ctl";
  private static final int SRC_PARTITIONS = 2;
  private static final TopicPartition CTL0 = new TopicPartition(CTL, 0);
  private static final String FLOOR_KEY = "kafka.connect.offsets." + CTL + "." + CONNECT_GROUP;
  private static final int COMMIT_INTERVAL_MS = 2000;
  private static final int COMMIT_TIMEOUT_MS = 5000;
  private static final long ROUND_MS = COMMIT_INTERVAL_MS + COMMIT_TIMEOUT_MS;
  // control-topic history accumulated by normal operation before the recreation
  private static final long MIN_OLD_CTL_END = 400;
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  // log lines of the unmodified sink (slf4j-simple, thread name in brackets)
  private static final String SKIP = "Skipping already-consumed control topic offset";
  private static final String COORD_THREAD = "[iceberg-coord]";
  private static final String HANDLED_START = "Handled event of type: START_COMMIT";
  private static final String HANDLED_DW = "Handled event of type: DATA_WRITTEN";
  private static final String HANDLED_DC = "Handled event of type: DATA_COMPLETE";
  private static final String COORD_STARTED = "elected leader, starting commit coordinator";
  private static final String WORKER_STARTED = "Starting commit worker";
  private static final String NOTHING = "found nothing to commit to table";
  private static final String ZERO_TABLES = "committed to 0 table(s)";
  private static final String COMPLETED = "completed commit";
  private static final String TIMEOUT = "Commit timeout reached";

  private final Path logFile =
      Paths.get(System.getProperty("cr1.log.file", "build/cr1/cr1-sink.log"));

  private EmbeddedConnectCluster connect;
  private Admin admin;
  private KafkaProducer<String, String> producer;
  private SharedInMemoryCatalog catalog;
  private TableIdentifier tableId;
  private String testName;
  private long nextId = 0;
  private boolean cooperative = true; // false: Kafka consumer default assignors (eager)
  private final Map<Long, long[]> where = new HashMap<>(); // id -> {partition, offset}

  @FunctionalInterface
  interface Check {
    boolean ok() throws Exception;
  }

  // -----------------------------------------------------------------------------------------------
  // Test 1: the finding

  @Test
  @Order(1)
  void recreatedControlTopic() throws Exception {
    startConnector();

    // ---- A: normal operation; also builds control-topic history (offsets) as time passes
    mark("A");
    for (int round = 0; round < 6; round += 1) {
      produceRound(5);
      awaitAllInTable(Duration.ofSeconds(120));
    }
    await(
        "control topic history >= " + MIN_OLD_CTL_END,
        Duration.ofSeconds(240),
        () -> ctlEndOffset() >= MIN_OLD_CTL_END);
    produceRound(5); // last data commit right before the operator action
    awaitAllInTable(Duration.ofSeconds(120));
    State a = state("A-baseline");
    check(
        a.lost.isEmpty() && a.ids.size() == a.produced && a.rows == a.produced,
        "baseline: every produced record is in the table exactly once");

    // ---- B: the operator deletes and recreates the control topic while the connector RUNS.
    // Act right after a commit round completed (no control-topic transaction in flight).
    mark("B0");
    awaitLog(COMPLETED, "B0", Duration.ofSeconds(60));
    long oldCtlEnd = ctlEndOffset();
    mark("B");
    long t0 = System.currentTimeMillis();
    recreateControlTopic();
    out(
        "operator action: control topic '"
            + CTL
            + "' deleted and recreated in "
            + (System.currentTimeMillis() - t0)
            + " ms; old log-end offset was "
            + oldCtlEnd
            + ", new log-end offset is "
            + ctlEndOffset());
    Thread.sleep(2000);
    produceRound(5); // batch S: arrives after the recreation
    observe("B", Duration.ofSeconds(35));
    State b = state("B-end");
    List<String> lb = logSince("B");
    long bCoordSkip = count(lb, SKIP, COORD_THREAD, true);
    long bWorkerSkip = count(lb, SKIP, COORD_THREAD, false);
    long bWorkerHandledStart = count(lb, HANDLED_START, COORD_THREAD, false);
    long bCoordHandledDw = count(lb, HANDLED_DW, COORD_THREAD, true);
    long bZero = count(lb, ZERO_TABLES, null, true);
    long bTimeouts = count(lb, TIMEOUT, null, true);
    out("phase B log: coordinator skipped " + bCoordSkip + " new-topic records, workers skipped "
        + bWorkerSkip + ", workers handled StartCommit " + bWorkerHandledStart
        + " times, coordinator handled DataWritten " + bCoordHandledDw + " times, commit timeouts "
        + bTimeouts + ", rounds that committed 0 tables " + bZero);
    sample(lb, SKIP, COORD_THREAD, true, 3);
    sample(lb, SKIP, COORD_THREAD, false, 3);
    boolean stall =
        b.snapshots == a.snapshots
            && b.ids.size() == a.ids.size()
            && b.produced > a.produced
            && b.committedTotal == a.committedTotal
            && bCoordSkip > 0
            && bCoordHandledDw == 0
            && bWorkerHandledStart == 0
            && b.tasks.equals("0=RUNNING,1=RUNNING");

    // ---- C: routine restart of the NON-leader task (cooperative rebalance keeps the coordinator)
    int leader = leaderTaskId();
    int other = 1 - leader;
    mark("C");
    out("leader (coordinator) task is " + leader + "; restarting task " + other + " via REST");
    connect.restartTask(CONNECTOR, other);
    Thread.sleep(3000);
    awaitTasksRunning(Duration.ofSeconds(60));
    produceRound(5); // batch L
    long target = nextId;
    try {
      await(
          "source offsets committed for every produced record",
          Duration.ofSeconds(90),
          () -> committedTotal() >= target);
    } catch (AssertionError e) {
      out("source offsets did not reach " + target + ": " + e.getMessage());
    }
    Thread.sleep(2 * ROUND_MS); // further commit rounds of the (stale) coordinator
    State c = state("C-end");
    List<String> lc = logSince("C");
    long cCoordStarts = count(lc, COORD_STARTED, null, true);
    long cWorkerStarts = count(lc, WORKER_STARTED, null, true);
    long cWorkerHandledStart = count(lc, HANDLED_START, COORD_THREAD, false);
    long cCoordSkip = count(lc, SKIP, COORD_THREAD, true);
    long cCoordHandledDw = count(lc, HANDLED_DW, COORD_THREAD, true);
    long cCoordHandledDc = count(lc, HANDLED_DC, COORD_THREAD, true);
    long cNothing = count(lc, NOTHING, null, true);
    long cZero = count(lc, ZERO_TABLES, null, true);
    out("phase C log: new coordinators started " + cCoordStarts + ", commit workers started "
        + cWorkerStarts + ", workers handled StartCommit " + cWorkerHandledStart
        + " times, coordinator skipped " + cCoordSkip + " records, coordinator handled DataWritten "
        + cCoordHandledDw + " / DataComplete " + cCoordHandledDc + " times, 'found nothing to "
        + "commit' (stored-floor filter) " + cNothing + ", rounds that committed 0 tables " + cZero);
    sample(lc, HANDLED_START, COORD_THREAD, false, 3);
    sample(lc, SKIP, COORD_THREAD, true, 6);
    boolean lossHwm =
        !c.lost.isEmpty()
            && c.snapshots == a.snapshots
            && cCoordStarts == 0
            && cWorkerHandledStart > 0
            && cCoordSkip > 0
            && cCoordHandledDw == 0
            && cNothing == 0;

    // ---- D: later, the records whose source offsets are committed are still not in the table
    mark("D");
    Thread.sleep(3 * ROUND_MS);
    State d = state("D-end");
    boolean permanent =
        !c.lost.isEmpty()
            && d.lost.containsAll(c.lost)
            && d.committedTotal >= c.committedTotal
            && d.snapshots == a.snapshots;

    // ---- E: restart the leader task (fresh coordinator, empty HWM): the stored floor filter
    // (the KNOWN half, #15293 / PR #16360) now drops the new DataWritten envelopes
    mark("E");
    int leaderNow = leaderTaskId();
    out("restarting the leader task " + leaderNow + " via REST (fresh coordinator)");
    connect.restartTask(CONNECTOR, leaderNow);
    Thread.sleep(3000);
    awaitTasksRunning(Duration.ofSeconds(60));
    produceRound(5); // batch K
    long targetE = nextId;
    try {
      await(
          "source offsets committed for every produced record",
          Duration.ofSeconds(90),
          () -> committedTotal() >= targetE);
    } catch (AssertionError e) {
      out("source offsets did not reach " + targetE + ": " + e.getMessage());
    }
    Thread.sleep(2 * ROUND_MS);
    State e = state("E-end");
    List<String> le = logSince("E");
    long eCoordStarts = count(le, COORD_STARTED, null, true);
    long eCoordHandledDw = count(le, HANDLED_DW, COORD_THREAD, true);
    long eNothing = count(le, NOTHING, null, true);
    out("phase E log: new coordinators started " + eCoordStarts + ", coordinator handled "
        + "DataWritten " + eCoordHandledDw + " times, 'found nothing to commit' " + eNothing);
    sample(le, NOTHING, null, true, 2);
    boolean lossFloor =
        eCoordStarts > 0 && eNothing > 0 && e.lost.size() > d.lost.size()
            && e.snapshots == a.snapshots;

    out("CR1-RESULT BASELINE_OK=true rows=" + a.rows + " produced=" + a.produced
        + " storedFloor=" + a.floor + " oldCtlEnd=" + oldCtlEnd);
    out("CR1-RESULT STALL_HWM=" + stall + " snapshots " + a.snapshots + "->" + b.snapshots
        + ", rows " + a.ids.size() + "->" + b.ids.size() + " while produced " + a.produced + "->"
        + b.produced + ", coordinatorSkips=" + bCoordSkip + ", workerSkips=" + bWorkerSkip
        + ", tasks=" + b.tasks);
    out("CR1-RESULT LOSS_HWM=" + lossHwm + " lost=" + c.lost.size() + " ids=" + c.lost
        + " committed=" + c.committed + " rows=" + c.ids.size() + "/" + c.produced
        + " coordinatorRestarts=" + cCoordStarts + " coordinatorSkips=" + cCoordSkip
        + " floorFilterHits=" + cNothing);
    out("CR1-RESULT PERMANENT=" + permanent + " lostLater=" + d.lost.size() + " snapshots="
        + d.snapshots + " committed=" + d.committed + " coordGroupCtlOffsets=" + d.coordCtl);
    out("CR1-RESULT LOSS_FLOOR_KNOWN_15293=" + lossFloor + " lost=" + e.lost.size()
        + " floorFilterHits=" + eNothing + " coordinatorRestarts=" + eCoordStarts
        + " snapshots=" + e.snapshots);

    check(stall, "STALL: running channels skip the recreated control topic");
    check(lossHwm, "LOSS: committed source offsets whose records never reach the table");
    check(permanent, "the loss is permanent");
  }

  // -----------------------------------------------------------------------------------------------
  // Test 2: negative control, the same task restart without recreating the control topic

  @Test
  @Order(2)
  void controlNoRecreateTaskRestartLosesNothing() throws Exception {
    startConnector();
    mark("A");
    for (int round = 0; round < 3; round += 1) {
      produceRound(5);
      awaitAllInTable(Duration.ofSeconds(120));
    }
    State a = state("control-A");
    produceRound(5); // buffered, not yet committed, when the task restarts
    int leader = leaderTaskId();
    int other = 1 - leader;
    mark("C");
    out("control: leader task is " + leader + "; restarting task " + other + " via REST");
    connect.restartTask(CONNECTOR, other);
    Thread.sleep(3000);
    awaitTasksRunning(Duration.ofSeconds(60));
    produceRound(5);
    boolean allIn = true;
    try {
      awaitAllInTable(Duration.ofSeconds(120));
    } catch (AssertionError e) {
      allIn = false;
    }
    Thread.sleep(ROUND_MS);
    State c = state("control-C");
    List<String> lc = logSince("C");
    out("CR1-RESULT CONTROL_NO_RECREATE allInTable=" + allIn + " lost=" + c.lost.size()
        + " rows=" + c.rows + " distinct=" + c.ids.size() + " produced=" + c.produced
        + " snapshots " + a.snapshots + "->" + c.snapshots + " coordinatorRestarts="
        + count(lc, COORD_STARTED, null, true) + " coordinatorSkips="
        + count(lc, SKIP, COORD_THREAD, true));
    check(
        allIn && c.lost.isEmpty() && c.ids.size() == c.produced && c.rows == c.produced,
        "control: a task restart without control-topic recreation loses nothing");
  }

  // -----------------------------------------------------------------------------------------------
  // Test 3 (informational): the same scenario with the Kafka consumer's DEFAULT assignor list
  // [RangeAssignor, CooperativeStickyAssignor], i.e. eager rebalancing; reports which mechanism
  // drops the records

  @Test
  @Order(3)
  void recreatedControlTopicDefaultEagerAssignor() throws Exception {
    cooperative = false;
    startConnector();
    mark("A");
    for (int round = 0; round < 3; round += 1) {
      produceRound(5);
      awaitAllInTable(Duration.ofSeconds(120));
    }
    await(
        "control topic history >= " + MIN_OLD_CTL_END,
        Duration.ofSeconds(240),
        () -> ctlEndOffset() >= MIN_OLD_CTL_END);
    produceRound(5);
    awaitAllInTable(Duration.ofSeconds(120));
    State a = state("eager-A-baseline");
    check(a.lost.isEmpty() && a.ids.size() == a.produced, "eager baseline: all records in table");

    mark("B0");
    awaitLog(COMPLETED, "B0", Duration.ofSeconds(60));
    mark("B");
    recreateControlTopic();
    Thread.sleep(2000);
    produceRound(5);
    observe("eager-B", Duration.ofSeconds(15));
    State b = state("eager-B-end");
    List<String> lb = logSince("B");
    long bCoordSkip = count(lb, SKIP, COORD_THREAD, true);
    boolean stall =
        b.snapshots == a.snapshots
            && b.ids.size() == a.ids.size()
            && b.committedTotal == a.committedTotal
            && bCoordSkip > 0
            && count(lb, HANDLED_DW, COORD_THREAD, true) == 0;

    int leader = leaderTaskId();
    int other = 1 - leader;
    mark("C");
    out("eager: leader task is " + leader + "; restarting task " + other + " via REST");
    connect.restartTask(CONNECTOR, other);
    Thread.sleep(3000);
    awaitTasksRunning(Duration.ofSeconds(60));
    produceRound(5);
    long target = nextId;
    try {
      await(
          "source offsets committed for every produced record",
          Duration.ofSeconds(60),
          () -> committedTotal() >= target);
    } catch (AssertionError e) {
      out("eager: source offsets did not reach " + target + ": " + e.getMessage());
    }
    Thread.sleep(2 * ROUND_MS);
    State c = state("eager-C-end");
    List<String> lc = logSince("C");
    long coordStarts = count(lc, COORD_STARTED, null, true);
    long coordStops = count(lc, "lost leader partition", null, true);
    long noLeader = count(lc, "cannot determine leader", null, true);
    long workerStarts = count(lc, WORKER_STARTED, null, true);
    long workerHandledStart = count(lc, HANDLED_START, COORD_THREAD, false);
    long coordSkips = count(lc, SKIP, COORD_THREAD, true);
    long coordDw = count(lc, HANDLED_DW, COORD_THREAD, true);
    long nothing = count(lc, NOTHING, null, true);
    String mechanism;
    if (c.lost.isEmpty()) {
      mechanism = "none";
    } else if (coordStarts == 0 && coordDw == 0 && nothing == 0 && coordSkips > 0) {
      mechanism = "in-memory-HWM(Channel.java:129)";
    } else if (nothing > 0) {
      mechanism = "stored-floor(Coordinator.java:277-285,KNOWN#15293)";
    } else {
      mechanism = "other";
    }
    sample(lc, "cannot determine leader", null, true, 2);
    out("CR1-RESULT EAGER_DEFAULT stall=" + stall + " lost=" + c.lost.size() + " mechanism="
        + mechanism + " coordinatorRestarts=" + coordStarts + " coordinatorStops=" + coordStops
        + " cannotDetermineLeader=" + noLeader + " workersStarted=" + workerStarts
        + " workersHandledStartCommit=" + workerHandledStart + " coordinatorSkips=" + coordSkips
        + " coordinatorHandledDataWritten=" + coordDw + " floorFilterHits=" + nothing
        + " committed=" + c.committed + " rows=" + c.ids.size() + "/" + c.produced);
  }

  // -----------------------------------------------------------------------------------------------
  // Environment

  @BeforeEach
  void setUp(TestInfo info) throws Exception {
    testName = info.getTestMethod().map(m -> m.getName()).orElse("test");
    catalog = SharedInMemoryCatalog.newRoot();
    catalog.createNamespace(Namespace.of("db"));
    String tableName =
        testName.startsWith("control")
            ? "tbl_control"
            : testName.contains("Eager") ? "tbl_eager" : "tbl";
    tableId = TableIdentifier.of("db", tableName);
    catalog.createTable(
        tableId,
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of("format-version", "2", "write.format.default", "avro"));

    Properties brokerProps = new Properties();
    brokerProps.put("auto.create.topics.enable", "false"); // the operator recreates explicitly
    brokerProps.put("group.initial.rebalance.delay.ms", "0");
    brokerProps.put("transaction.state.log.replication.factor", "1");
    brokerProps.put("transaction.state.log.min.isr", "1");
    brokerProps.put("transaction.state.log.num.partitions", "4");
    brokerProps.put("offsets.topic.num.partitions", "4");

    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("connector.client.config.override.policy", "All");
    // the sink processes control events inside put(); a short flush interval makes idle tasks
    // call put([]) promptly (IcebergSinkTask.preCommit returns no offsets)
    workerProps.put("offset.flush.interval.ms", "500");
    workerProps.put("scheduled.rebalance.max.delay.ms", "1000");
    workerProps.put("task.shutdown.graceful.timeout.ms", "10000");

    connect =
        new EmbeddedConnectCluster.Builder()
            .name("cr1-" + testName)
            .numWorkers(2)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(2, "Connect workers did not start");
    connect.kafka().createTopic(SRC, SRC_PARTITIONS);
    connect.kafka().createTopic(CTL, 1);

    Map<String, Object> adminProps = new HashMap<>();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    admin = Admin.create(adminProps);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    warmUpTransactions();
    out("==== " + testName + ": cluster up, bootstrap " + connect.kafka().bootstrapServers());
  }

  @AfterEach
  void tearDown() {
    try {
      producer.close(Duration.ofSeconds(5));
      admin.close(Duration.ofSeconds(5));
      connect.stop();
    } catch (Exception e) {
      LOG.warn("Error stopping the embedded cluster", e);
    }
  }

  /** The first InitProducerId of a fresh broker creates __transaction_state; do it up front. */
  private void warmUpTransactions() {
    for (int i = 0; i < 4; i += 1) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "cr1-warmup-" + i);
      try (KafkaProducer<String, String> warm =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  private Map<String, String> connectorConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("connector.class", IcebergSinkConnector.class.getName());
    config.put("tasks.max", "2");
    config.put("topics", SRC);
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("value.converter.schemas.enable", "false");
    if (cooperative) {
      config.put(
          "consumer.override.partition.assignment.strategy",
          "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    }
    config.put("iceberg.tables", tableId.toString());
    config.put("iceberg.catalog.catalog-impl", SharedInMemoryCatalog.class.getName());
    config.put("iceberg.control.topic", CTL);
    config.put("iceberg.control.commit.interval-ms", String.valueOf(COMMIT_INTERVAL_MS));
    config.put("iceberg.control.commit.timeout-ms", String.valueOf(COMMIT_TIMEOUT_MS));
    config.put("iceberg.kafka.bootstrap.servers", connect.kafka().bootstrapServers());
    return config;
  }

  private void startConnector() throws InterruptedException {
    connect.configureConnector(CONNECTOR, connectorConfig());
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 2, "Connector did not start");
  }

  // -----------------------------------------------------------------------------------------------
  // Workload and observations

  private void produceRound(int perPartition) throws Exception {
    for (int i = 0; i < perPartition; i += 1) {
      for (int partition = 0; partition < SRC_PARTITIONS; partition += 1) {
        long id = nextId++;
        String value = "{\"id\": " + id + ", \"data\": \"p" + partition + "\"}";
        RecordMetadata md =
            producer.send(new ProducerRecord<>(SRC, partition, "k" + id, value)).get();
        where.put(id, new long[] {partition, md.offset()});
      }
    }
  }

  private void recreateControlTopic() throws Exception {
    admin.deleteTopics(List.of(CTL)).all().get(30, TimeUnit.SECONDS);
    await("control topic deleted", Duration.ofSeconds(30), () -> !topics().contains(CTL));
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (true) {
      try {
        admin.createTopics(List.of(new NewTopic(CTL, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        break;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TopicExistsException && System.nanoTime() < deadline) {
          Thread.sleep(100); // deletion still completing
          continue;
        }
        throw e;
      }
    }
    await("control topic recreated", Duration.ofSeconds(30), () -> topics().contains(CTL));
  }

  private Set<String> topics() throws Exception {
    return admin.listTopics().names().get(10, TimeUnit.SECONDS);
  }

  private long ctlEndOffset() throws Exception {
    return admin
        .listOffsets(Map.of(CTL0, OffsetSpec.latest()))
        .all()
        .get(10, TimeUnit.SECONDS)
        .get(CTL0)
        .offset();
  }

  private Map<Integer, Long> groupOffsets(String group, String topic) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin
            .listConsumerGroupOffsets(group)
            .partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS);
    Map<Integer, Long> result = new TreeMap<>();
    offsets.forEach(
        (tp, om) -> {
          if (tp.topic().equals(topic) && om != null) {
            result.put(tp.partition(), om.offset());
          }
        });
    return result;
  }

  private long committedTotal() throws Exception {
    return groupOffsets(CONNECT_GROUP, SRC).values().stream().mapToLong(Long::longValue).sum();
  }

  private void awaitAllInTable(Duration timeout) throws Exception {
    long expected = nextId;
    await(
        "all " + expected + " records in the table with committed source offsets",
        timeout,
        () -> {
          Rows rows = rows();
          return rows.ids.size() == expected && committedTotal() == expected;
        });
  }

  private int leaderTaskId() throws Exception {
    TopicPartition first = new TopicPartition(SRC, 0);
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < deadline) {
      ConsumerGroupDescription desc =
          admin
              .describeConsumerGroups(List.of(CONNECT_GROUP))
              .describedGroups()
              .get(CONNECT_GROUP)
              .get(10, TimeUnit.SECONDS);
      for (MemberDescription member : desc.members()) {
        if (member.assignment().topicPartitions().contains(first)) {
          String clientId = member.clientId(); // connector-consumer-<connector>-<task>
          return Integer.parseInt(clientId.substring(clientId.lastIndexOf('-') + 1));
        }
      }
      Thread.sleep(200);
    }
    throw new AssertionError("no member owns " + first);
  }

  private String taskStates() {
    ConnectorStateInfo info = connect.connectorStatus(CONNECTOR);
    return info.tasks().stream()
        .sorted((x, y) -> Integer.compare(x.id(), y.id()))
        .map(t -> t.id() + "=" + t.state())
        .collect(Collectors.joining(","));
  }

  private void awaitTasksRunning(Duration timeout) throws Exception {
    await("tasks RUNNING", timeout, () -> taskStates().equals("0=RUNNING,1=RUNNING"));
  }

  private static final class Rows {
    private final Set<Long> ids = new TreeSet<>();
    private long rows = 0;
  }

  private Rows rows() throws Exception {
    Table table = catalog.loadTable(tableId);
    Rows result = new Rows();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        result.ids.add((Long) record.getField("id"));
        result.rows += 1;
      }
    }
    return result;
  }

  private static final class State {
    private long produced;
    private Map<Integer, Long> committed;
    private long committedTotal;
    private int snapshots;
    private String floor;
    private Set<Long> ids;
    private long rows;
    private List<Long> lost;
    private long ctlEnd;
    private Map<Integer, Long> coordCtl;
    private String tasks;
  }

  private State state(String label) throws Exception {
    State s = new State();
    Table table = catalog.loadTable(tableId);
    int snapshots = 0;
    for (Snapshot ignored : table.snapshots()) {
      snapshots += 1;
    }
    s.snapshots = snapshots;
    Snapshot current = table.currentSnapshot();
    s.floor = current == null ? null : current.summary().get(FLOOR_KEY);
    Rows rows = rows();
    s.ids = rows.ids;
    s.rows = rows.rows;
    s.produced = nextId;
    s.committed = groupOffsets(CONNECT_GROUP, SRC);
    s.committedTotal = s.committed.values().stream().mapToLong(Long::longValue).sum();
    s.ctlEnd = ctlEndOffset();
    s.coordCtl = groupOffsets(COORD_GROUP, CTL);
    s.tasks = taskStates();
    s.lost = new ArrayList<>();
    for (long id = 0; id < nextId; id += 1) {
      long[] pos = where.get(id);
      Long committedOffset = s.committed.get((int) pos[0]);
      if (committedOffset != null && pos[1] < committedOffset && !s.ids.contains(id)) {
        s.lost.add(id);
      }
    }
    out(String.format(
        "state[%s] produced=%d committedSourceOffsets=%s (total %d) tableRows=%d distinct=%d "
            + "snapshots=%d storedFloor[%s]=%s ctlLogEnd=%d coordGroupCtlOffsets=%s tasks=%s "
            + "LOST(committed but not in table)=%d",
        label, s.produced, s.committed, s.committedTotal, s.rows, s.ids.size(), s.snapshots,
        FLOOR_KEY, s.floor, s.ctlEnd, s.coordCtl, s.tasks, s.lost.size()));
    return s;
  }

  private void observe(String phase, Duration duration) throws Exception {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < duration.toMillis()) {
      Thread.sleep(5000);
      state(phase + "@+" + (System.currentTimeMillis() - start) / 1000 + "s");
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Log helpers (the sink's own log lines; slf4j-simple writes to cr1.log.file)

  private void mark(String phase) {
    LOG.info("CR1-MARK {} {}", testName, phase);
  }

  private List<String> logSince(String phase) throws Exception {
    List<String> all = Files.readAllLines(logFile, StandardCharsets.UTF_8);
    String needle = "CR1-MARK " + testName + " " + phase;
    int start = -1;
    for (int i = 0; i < all.size(); i += 1) {
      if (all.get(i).endsWith(needle)) {
        start = i;
      }
    }
    return start < 0 ? List.of() : new ArrayList<>(all.subList(start + 1, all.size()));
  }

  private void awaitLog(String needle, String phase, Duration timeout) throws Exception {
    await("log line '" + needle + "'", timeout, () -> count(logSince(phase), needle, null, true) > 0);
  }

  private static long count(List<String> lines, String needle, String thread, boolean onThread) {
    return lines.stream()
        .filter(l -> l.contains(needle))
        .filter(l -> thread == null || l.contains(thread) == onThread)
        .count();
  }

  private void sample(List<String> lines, String needle, String thread, boolean onThread, int n) {
    lines.stream()
        .filter(l -> l.contains(needle))
        .filter(l -> thread == null || l.contains(thread) == onThread)
        .limit(n)
        .forEach(l -> out("  log> " + l));
  }

  // -----------------------------------------------------------------------------------------------

  private static void await(String what, Duration timeout, Check check) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!check.ok()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      Thread.sleep(200);
    }
  }

  private static void check(boolean condition, String what) {
    out((condition ? "CHECK OK   : " : "CHECK FAIL : ") + what);
    if (!condition) {
      throw new AssertionError(what);
    }
  }

  private static void out(String message) {
    System.out.println("CR1| " + message);
    LOG.info("CR1| {}", message);
  }
}
JAVA

# ---- 5. run --------------------------------------------------------------------------------------
cd "$WORK" || exit 2
if [ "${CR1_COMPILE_ONLY:-0}" = "1" ]; then
  timeout 1800 ./gradlew ":iceberg-kafka-connect:iceberg-kafka-connect:compileCrReproJava" --console=plain
  exit $?
fi
rm -f "$SINK_LOG"
mkdir -p "$(dirname "$SINK_LOG")"
echo "==> running (sink log: $SINK_LOG, console: $GRADLE_LOG)"
FILTER=()
[ -n "${CR1_TESTS:-}" ] && FILTER=(--tests "$CR1_TESTS")
timeout 2400 ./gradlew ":iceberg-kafka-connect:iceberg-kafka-connect:crReproTest" ${FILTER[@]+"${FILTER[@]}"} \
  -Dcr1.log.file="$SINK_LOG" --console=plain > "$GRADLE_LOG" 2>&1
rc=$?
if [ "$rc" -eq 124 ]; then
  echo "TIMEOUT (possible hang in the system under test)"
  ./gradlew --stop > /dev/null 2>&1
fi

# ---- 6. summary -----------------------------------------------------------------------------------
echo ""
echo "==> Test results"
grep -E "CR1ControlTopicRecreateTest > .* (PASSED|FAILED|SKIPPED)" "$GRADLE_LOG" || tail -40 "$GRADLE_LOG"
echo ""
echo "==> Result lines"
grep -E "CR1\| CR1-RESULT|CR1\| CHECK" "$GRADLE_LOG"
echo ""
[ -n "${CR1_TESTS:-}" ] && { echo "(filtered run: $CR1_TESTS; gradle exit $rc)"; exit "$rc"; }
if grep -q "CR1-RESULT STALL_HWM=true" "$GRADLE_LOG" \
  && grep -q "CR1-RESULT LOSS_HWM=true" "$GRADLE_LOG" \
  && grep -q "CR1-RESULT PERMANENT=true" "$GRADLE_LOG" \
  && grep -q "CR1-RESULT CONTROL_NO_RECREATE allInTable=true lost=0" "$GRADLE_LOG"; then
  echo "BUG REPRODUCED (CR-1): a recreated control topic is skipped by the in-memory high-water mark;"
  echo "records whose source offsets were committed never reach the Iceberg table (negative control passed)."
  exit 0
fi
echo "CR-1 NOT reproduced by this run (gradle exit $rc)"
exit 1
