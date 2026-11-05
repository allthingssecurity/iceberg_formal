#!/bin/bash
# =================================================================================================
# CR-12 reproduction — "The partial-commit path swallows every exception with no retry budget"
#   Coordinator.java:138-140 (process -> commit(true) on timeout)
#   Coordinator.java:165-174 (partial branch: count + WARN "will retry" + return, for ANY RuntimeException)
#   Coordinator.java:176-191 (full branch: non-CommitFailedException -> rethrow; CFE -> budget)
#   Coordinator.java:198-200 (finally endCurrentCommit; commitBuffer is NOT cleared -> retried forever)
#
# Level 0 (pure black box): the PRISTINE sink (apache/iceberg @ 9fffeb6, extracted with `git archive
# HEAD`, no Specula instrumentation) runs in a real Kafka Connect runtime (EmbeddedConnectCluster,
# real WorkerSinkTask) against a real embedded Kafka 3.9.2 broker (KRaft, real transactions) and a
# real HadoopCatalog on the local filesystem. No source change, no mock, no failpoint. The only
# "fault" is an ordinary environment event: the table's metadata directory stops being writable
# for the connector (chmod 555 — the local analogue of a revoked catalog/storage privilege, i.e.
# a permanent, non-retryable commit failure such as ForbiddenException / 403). Data-file writes of
# the workers are unaffected.
#
# Scenarios (one JUnit class each, fresh cluster per class):
#   A  PartialPathPermanentFailure  tasks.max=2, records only on src-1 -> the task owning src-0 is
#      idle and never answers StartCommit (upstream #17811, unfixed here) -> EVERY round is a
#      partial commit. Then revoke metadata writes. Expect (bug): coordinator retries forever
#      (>> max-consecutive-failures=3), task stays RUNNING, no snapshot, source offsets advance.
#      Finally the permission is restored to show what the retained buffer then does.
#   B  FullPathControl  same, but records on both partitions -> full commits. Expect (developer-
#      intended behaviour, #16237): the same failure terminates the coordinator -> task FAILED.
#   C  FencedCoordinator  partial steady state; a new coordinator's initTransactions() on the
#      coordinator transactional.id (exactly what KafkaClientFactory.createProducer does for a
#      newly elected coordinator) fences the running one mid-commit. Measures whether the fenced
#      coordinator "keeps looping" or exits.
#
# Usage:  bash test_bugCR-12_partial_commit_no_retry_budget.sh            (all three scenarios)
#         SCENARIOS="PartialPathPermanentFailure" bash test_bugCR-12_...sh
# Output: repro/CR-12.log (this driver) and repro/CR-12.<Scenario>.log (Gradle + test output)
# =================================================================================================
set -uo pipefail

OUT_DIR="${CR12_OUT_DIR:-/Users/I074560/Downloads/experiments/specula/runs/20261104-163223-c58c/iceberg-kafka-connect/.specula-output}"
WT="${CR12_WORKTREE:-$OUT_DIR/confirmation/CR-12/worktree}"
BUILD="${CR12_BUILD_DIR:-$OUT_DIR/confirmation/CR-12/pristine-build}"
LOG_DIR="$OUT_DIR/repro"
DRIVER_LOG="$LOG_DIR/CR-12.log"
SCENARIOS="${SCENARIOS:-PartialPathPermanentFailure FullPathControl FencedCoordinator}"
GRADLE_PROJECT=":iceberg-kafka-connect:iceberg-kafka-connect"
GRADLE_FLAGS=(-DsparkVersions= -DflinkVersions= -DkafkaVersions=3 --console=plain)
KC_REL="kafka-connect/kafka-connect"

mkdir -p "$LOG_DIR"
exec > >(tee "$DRIVER_LOG") 2>&1

echo "==> CR-12 repro driver  $(date '+%Y-%m-%d %H:%M:%S')"
echo "    worktree (git source of HEAD): $WT"
echo "    pristine build dir:            $BUILD"
echo "    java: $(java -version 2>&1 | head -1)"

# -------------------------------------------------------------------------------------------------
# 1. Pristine tree = `git archive HEAD` of the worktree (drops the Specula instrumentation, which is
#    only present as uncommitted changes / untracked files in the worktree).
HEAD_SHA="$(git -C "$WT" rev-parse HEAD)" || { echo "cannot read worktree HEAD"; exit 2; }
echo "    HEAD: $HEAD_SHA"
if [ ! -f "$BUILD/.cr12-pristine-$HEAD_SHA" ]; then
  echo "==> Extracting pristine tree (git archive HEAD)"
  rm -rf "$BUILD"
  mkdir -p "$BUILD"
  git -C "$WT" archive --format=tar HEAD | tar -x -C "$BUILD" || { echo "git archive failed"; exit 2; }
  echo "1.12.0-SNAPSHOT" > "$BUILD/version.txt"   # version.txt replaces the git-describe version
  touch "$BUILD/.cr12-pristine-$HEAD_SHA"
fi

# Verify the sink's main sources are byte-identical to HEAD (no instrumentation, no edits).
CMP_DIR="$(mktemp -d)"
git -C "$WT" archive --format=tar HEAD "$KC_REL/src/main" | tar -x -C "$CMP_DIR"
if diff -r "$CMP_DIR/$KC_REL/src/main" "$BUILD/$KC_REL/src/main" > /dev/null; then
  echo "PRISTINE CHECK: OK — $KC_REL/src/main identical to $HEAD_SHA (no TlaHooks/TlaTrace, no edits)"
else
  echo "PRISTINE CHECK: FAILED — build tree differs from HEAD"; diff -r "$CMP_DIR/$KC_REL/src/main" "$BUILD/$KC_REL/src/main" | head; exit 2
fi
rm -rf "$CMP_DIR"
grep -n "Partial commit {} failed for task {}, will retry" "$BUILD/$KC_REL/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java"

# -------------------------------------------------------------------------------------------------
# 2. Test-only Gradle source set with the embedded Kafka / Connect test jars (main is untouched).
if ! grep -q "CR-12 repro source set" "$BUILD/kafka-connect/build.gradle"; then
  cat >> "$BUILD/kafka-connect/build.gradle" <<'GRADLE'

// CR-12 repro source set (test-only; the sink's main source set is pristine)
project(':iceberg-kafka-connect:iceberg-kafka-connect') {
  sourceSets {
    cr12Repro {
      compileClasspath += sourceSets.main.output
      runtimeClasspath += sourceSets.main.output
    }
  }
  configurations {
    cr12ReproImplementation.extendsFrom testImplementation
    cr12ReproRuntimeOnly.extendsFrom testRuntimeOnly
  }
  dependencies {
    def kafkaVersion = libs.versions.kafka.get()
    cr12ReproImplementation libs.kafka.clients
    cr12ReproImplementation libs.kafka.connect.api
    cr12ReproImplementation libs.kafka.connect.json
    cr12ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}"
    cr12ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}:test"
    cr12ReproImplementation "org.apache.kafka:kafka-clients:${kafkaVersion}:test"
    cr12ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}"
    cr12ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}:test"
    cr12ReproImplementation "org.apache.kafka:kafka-server-common:${kafkaVersion}:test"
  }
  pluginManager.withPlugin('net.ltgt.errorprone') {
    tasks.matching { it.name == 'compileCr12ReproJava' }.configureEach {
      options.errorprone.enabled = false
    }
  }
  task cr12ReproTest(type: Test) {
    useJUnitPlatform()
    testClassesDirs = sourceSets.cr12Repro.output.classesDirs
    classpath = sourceSets.cr12Repro.runtimeClasspath
    forkEvery = 1
    maxParallelForks = 1
    maxHeapSize = "3g"
    jvmArgs += project.property('extraJvmArgs')
    systemProperty 'org.slf4j.simpleLogger.defaultLogLevel', 'warn'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.iceberg.connect.channel', 'info'
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

SRC_DIR="$BUILD/$KC_REL/src/cr12Repro/java/org/apache/iceberg/connect/cr12"
RES_DIR="$BUILD/$KC_REL/src/cr12Repro/resources"
mkdir -p "$SRC_DIR" "$RES_DIR"

cat > "$RES_DIR/simplelogger.properties" <<'EOF'
org.slf4j.simpleLogger.defaultLogLevel=warn
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS
org.slf4j.simpleLogger.log.org.apache.iceberg.connect.channel=info
EOF
cat > "$RES_DIR/log4j.properties" <<'EOF'
log4j.rootLogger=WARN, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{HH:mm:ss.SSS} %-5p [%t] %c{1}: %m%n
log4j.logger.org.apache.iceberg.connect.channel=INFO
EOF

# -------------------------------------------------------------------------------------------------
# 3. The tests (black box: Connect REST status, catalog, control topic, consumer-group offsets).
cat > "$SRC_DIR/Cr12Base.java" <<'JAVA'
package org.apache.iceberg.connect.cr12;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConnector;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;

abstract class Cr12Base {
  static final String CONNECTOR = "iceberg-sink";
  static final String SRC = "src";
  static final String CTL = "ctl";
  static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");
  static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  static final int MAX_CONSECUTIVE_FAILURES = 3; // the budget the operator configured

  protected EmbeddedConnectCluster connect;
  protected KafkaProducer<String, String> producer;
  protected Path warehouse;
  protected HadoopCatalog catalog;
  protected final Map<Integer, Integer> produced = new TreeMap<>();
  protected final List<CtlEvent> ctlEvents = new CopyOnWriteArrayList<>();
  private volatile boolean tailing = false;
  private Thread tailer;
  private long nextId = 0;
  private final long t0 = System.currentTimeMillis();

  /** One control-topic record as a read_committed consumer sees it (wall-clock arrival time). */
  static final class CtlEvent {
    final long seenAt;
    final long offset;
    final String type;
    final String commitId;
    final String detail;

    CtlEvent(long seenAt, long offset, String type, String commitId, String detail) {
      this.seenAt = seenAt;
      this.offset = offset;
      this.type = type;
      this.commitId = commitId;
      this.detail = detail;
    }
  }

  static void log(String fmt, Object... args) {
    System.out.println("[CR12] " + String.format(fmt, args));
    System.out.flush();
  }

  String rel(long ts) {
    return String.format("t+%5.1fs", (ts - t0) / 1000.0);
  }

  void startCluster(String name) throws Exception {
    warehouse = Files.createTempDirectory("cr12-warehouse-");
    catalog = new HadoopCatalog(new Configuration(), warehouse.toUri().toString());
    catalog.createNamespace(Namespace.of("db"));
    catalog.createTable(
        TABLE,
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of("format-version", "2", "write.format.default", "avro"));
    log("warehouse=%s (HadoopCatalog, local filesystem)", warehouse);

    Properties brokerProps = new Properties();
    brokerProps.put("transaction.state.log.replication.factor", "1");
    brokerProps.put("transaction.state.log.min.isr", "1");
    brokerProps.put("transaction.state.log.num.partitions", "4");
    brokerProps.put("offsets.topic.num.partitions", "4");
    Map<String, String> workerProps = new HashMap<>();
    // put() follows each consumer poll whose timeout derives from the flush interval: keep it short
    // so active tasks poll the control topic promptly (IcebergSinkTask.preCommit commits nothing)
    workerProps.put("offset.flush.interval.ms", "500");
    workerProps.put("scheduled.rebalance.max.delay.ms", "1000");
    workerProps.put("task.shutdown.graceful.timeout.ms", "10000");
    connect =
        new EmbeddedConnectCluster.Builder()
            .name(name)
            .numWorkers(1)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(1, "Connect worker did not start");
    connect.kafka().createTopic(SRC, 2);
    connect.kafka().createTopic(CTL, 1);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    warmUpTransactions();
    startControlTopicTailer();
  }

  /** The first InitProducerId of a fresh broker is slow; warm __transaction_state up. */
  private void warmUpTransactions() {
    for (int i = 0; i < 8; i += 1) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "cr12-warmup-" + i);
      try (KafkaProducer<String, String> warm =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  Map<String, String> connectorConfig(int intervalMs, int timeoutMs) {
    Map<String, String> c = new HashMap<>();
    c.put("connector.class", IcebergSinkConnector.class.getName());
    c.put("tasks.max", "2");
    c.put("topics", SRC);
    c.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    c.put("value.converter.schemas.enable", "false");
    c.put("iceberg.tables", "db.tbl");
    c.put("iceberg.catalog.catalog-impl", HadoopCatalog.class.getName());
    c.put("iceberg.catalog.warehouse", warehouse.toUri().toString());
    c.put("iceberg.control.topic", CTL);
    c.put("iceberg.control.commit.interval-ms", String.valueOf(intervalMs));
    c.put("iceberg.control.commit.timeout-ms", String.valueOf(timeoutMs));
    c.put(
        "iceberg.control.commit.max-consecutive-failures",
        String.valueOf(MAX_CONSECUTIVE_FAILURES));
    c.put("iceberg.kafka.bootstrap.servers", connect.kafka().bootstrapServers());
    return c;
  }

  void startConnector(Map<String, String> config) throws InterruptedException {
    log("connector config: %s", new TreeMap<>(config));
    connect.configureConnector(CONNECTOR, config);
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 2, "Connector did not start");
    log("connector %s RUNNING with 2 tasks", CONNECTOR);
  }

  void produce(int partition, int n, long pauseMs) throws Exception {
    for (int i = 0; i < n; i += 1) {
      int offset = produced.getOrDefault(partition, 0);
      long id = nextId++;
      String value = "{\"id\": " + id + ", \"data\": \"p" + partition + "-" + offset + "\"}";
      producer.send(new ProducerRecord<>(SRC, partition, "k" + id, value)).get();
      produced.put(partition, offset + 1);
      if (pauseMs > 0) {
        Thread.sleep(pauseMs);
      }
    }
  }

  // --------------------------------------------------------------------------------------------
  // Observations through public interfaces

  Map<Integer, String> taskStates() {
    Map<Integer, String> states = new TreeMap<>();
    ConnectorStateInfo info = connect.connectorStatus(CONNECTOR);
    for (ConnectorStateInfo.TaskState state : info.tasks()) {
      states.put(state.id(), state.state());
    }
    return states;
  }

  String taskTraces() {
    StringBuilder sb = new StringBuilder();
    for (ConnectorStateInfo.TaskState state : connect.connectorStatus(CONNECTOR).tasks()) {
      if (state.trace() != null) {
        String[] lines = state.trace().split("\n");
        sb.append("task ").append(state.id()).append(" ").append(state.state()).append(" trace: ");
        for (int i = 0; i < Math.min(lines.length, 3); i += 1) {
          sb.append(lines[i].trim()).append(" | ");
        }
        for (String line : lines) {
          if (line.startsWith("Caused by")) {
            sb.append(line.trim()).append(" | ");
          }
        }
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  Table table() {
    return catalog.loadTable(TABLE);
  }

  List<Snapshot> snapshots() {
    List<Snapshot> result = new ArrayList<>();
    table().snapshots().forEach(result::add);
    return result;
  }

  Map<String, Integer> rows() throws Exception {
    Map<String, Integer> rows = new TreeMap<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table()).build()) {
      for (Record record : records) {
        rows.merge(String.valueOf(record.getField("data")), 1, Integer::sum);
      }
    }
    return rows;
  }

  Map<Integer, Long> sourceCommitted() throws Exception {
    Map<Integer, Long> committed = new TreeMap<>();
    try (Admin admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers()))) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin
              .listConsumerGroupOffsets("connect-" + CONNECTOR)
              .partitionsToOffsetAndMetadata()
              .get(20, TimeUnit.SECONDS);
      offsets.forEach(
          (tp, o) -> {
            if (o != null && SRC.equals(tp.topic())) {
              committed.put(tp.partition(), o.offset());
            }
          });
    }
    return committed;
  }

  /** Source records whose offset the sink committed in Kafka but that are not in the table. */
  List<String> committedButNotInTable() throws Exception {
    Map<String, Integer> rows = rows();
    List<String> missing = new ArrayList<>();
    sourceCommitted()
        .forEach(
            (partition, offset) -> {
              for (long o = 0; o < offset; o += 1) {
                if (!rows.containsKey("p" + partition + "-" + o)) {
                  missing.add("p" + partition + "-" + o);
                }
              }
            });
    return missing;
  }

  static boolean coordinatorThreadAlive() {
    return Thread.getAllStackTraces().keySet().stream()
        .anyMatch(t -> "iceberg-coord".equals(t.getName()) && t.isAlive());
  }

  long countCtl(String type, long sinceMillis) {
    return ctlEvents.stream().filter(e -> e.type.equals(type) && e.seenAt >= sinceMillis).count();
  }

  /** Partitions reported in DataComplete events per commit id (to classify partial vs full). */
  Map<String, Set<String>> reportedPartitionsByCommit() {
    Map<String, Set<String>> result = new TreeMap<>();
    for (CtlEvent e : ctlEvents) {
      if ("START_COMMIT".equals(e.type)) {
        result.computeIfAbsent(e.commitId, k -> new TreeSet<>());
      } else if ("DATA_COMPLETE".equals(e.type)) {
        Set<String> parts = result.computeIfAbsent(e.commitId, k -> new TreeSet<>());
        for (String p : e.detail.split(",")) {
          if (!p.isEmpty()) {
            parts.add(p);
          }
        }
      }
    }
    return result;
  }

  private void startControlTopicTailer() {
    tailing = true;
    tailer =
        new Thread(
            () -> {
              Map<String, Object> props = new HashMap<>();
              props.put(
                  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
              props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
              props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
              try (KafkaConsumer<String, byte[]> consumer =
                  new KafkaConsumer<>(
                      props, new StringDeserializer(), new ByteArrayDeserializer())) {
                TopicPartition tp = new TopicPartition(CTL, 0);
                consumer.assign(List.of(tp));
                consumer.seekToBeginning(List.of(tp));
                while (tailing) {
                  for (ConsumerRecord<String, byte[]> record :
                      consumer.poll(Duration.ofMillis(100))) {
                    Event event = AvroUtil.decode(record.value());
                    String type = event.payload().type().name();
                    String commitId = "";
                    String detail = "";
                    switch (event.payload().type()) {
                      case START_COMMIT:
                        commitId =
                            ((org.apache.iceberg.connect.events.StartCommit) event.payload())
                                .commitId()
                                .toString();
                        break;
                      case DATA_WRITTEN:
                        org.apache.iceberg.connect.events.DataWritten dw =
                            (org.apache.iceberg.connect.events.DataWritten) event.payload();
                        commitId = dw.commitId().toString();
                        detail = "files=" + (dw.dataFiles() == null ? 0 : dw.dataFiles().size());
                        break;
                      case DATA_COMPLETE:
                        org.apache.iceberg.connect.events.DataComplete dc =
                            (org.apache.iceberg.connect.events.DataComplete) event.payload();
                        commitId = dc.commitId().toString();
                        StringBuilder parts = new StringBuilder();
                        dc.assignments()
                            .forEach(
                                a -> {
                                  if (parts.length() > 0) {
                                    parts.append(",");
                                  }
                                  parts.append(a.topic()).append("-").append(a.partition());
                                });
                        detail = parts.toString();
                        break;
                      case COMMIT_TO_TABLE:
                        commitId =
                            ((org.apache.iceberg.connect.events.CommitToTable) event.payload())
                                .commitId()
                                .toString();
                        break;
                      case COMMIT_COMPLETE:
                        commitId =
                            ((org.apache.iceberg.connect.events.CommitComplete) event.payload())
                                .commitId()
                                .toString();
                        break;
                      default:
                        break;
                    }
                    ctlEvents.add(
                        new CtlEvent(
                            System.currentTimeMillis(), record.offset(), type, commitId, detail));
                  }
                }
              } catch (Exception e) {
                if (tailing) {
                  log("control-topic tailer stopped: %s", e);
                }
              }
            },
            "cr12-ctl-tailer");
    tailer.setDaemon(true);
    tailer.start();
  }

  // --------------------------------------------------------------------------------------------
  // Environment event: the connector loses write access to the table metadata location

  Path metadataDir() {
    return warehouse.resolve("db").resolve("tbl").resolve("metadata");
  }

  void revokeMetadataWrite() throws Exception {
    Files.setPosixFilePermissions(metadataDir(), PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      Files.createFile(metadataDir().resolve("cr12-probe"));
      Files.delete(metadataDir().resolve("cr12-probe"));
      throw new IllegalStateException(
          "ENV: chmod 555 did not revoke write access (running as root?)");
    } catch (AccessDeniedException expected) {
      log("ENV EVENT: write access to %s revoked (probe write -> AccessDeniedException)",
          metadataDir());
    }
  }

  void restoreMetadataWrite() throws Exception {
    if (warehouse != null && Files.exists(metadataDir())) {
      Files.setPosixFilePermissions(metadataDir(), PosixFilePermissions.fromString("rwxr-xr-x"));
    }
  }

  static void await(String what, Duration timeout, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      Thread.sleep(100);
    }
  }

  static boolean quietly(java.util.concurrent.Callable<Boolean> check) {
    try {
      return check.call();
    } catch (Exception e) {
      return false;
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    tailing = false;
    try {
      restoreMetadataWrite();
    } catch (Exception e) {
      log("could not restore permissions: %s", e);
    }
    try {
      if (producer != null) {
        producer.close(Duration.ofSeconds(5));
      }
      if (connect != null) {
        connect.stop();
      }
    } catch (Exception e) {
      log("error stopping the embedded cluster: %s", e);
    }
  }
}
JAVA

cat > "$SRC_DIR/Cr12PartialPathPermanentFailureTest.java" <<'JAVA'
package org.apache.iceberg.connect.cr12;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.Test;

/**
 * Scenario A. Every commit round is partial because the task owning src-0 is idle (#17811); a
 * permanent, non-retryable commit failure then makes the coordinator retry forever while the task
 * stays RUNNING, no snapshot lands, and the workers keep committing source offsets.
 */
class Cr12PartialPathPermanentFailureTest extends Cr12Base {

  @Test
  void permanentFailureOnPartialPathIsRetriedForeverWhileTaskRunning() throws Exception {
    startCluster("cr12-partial");
    startConnector(connectorConfig(1000, 3000));

    // ---- healthy phase: records only on src-1 (src-0 idle)
    produce(1, 5, 200);
    await("healthy partial commit lands 5 rows", Duration.ofSeconds(120),
        () -> quietly(() -> rows().size() >= 5));
    Thread.sleep(1500);
    List<Snapshot> snapsBefore = snapshots();
    Map<String, Set<String>> reported = reportedPartitionsByCommit();
    log("HEALTHY: snapshots=%d rows=%d sourceCommitted=%s states=%s",
        snapsBefore.size(), rows().size(), sourceCommitted(), taskStates());
    log("HEALTHY: partitions reported per commit id (DataComplete) = %s", reported);
    for (Snapshot s : snapsBefore) {
      log("HEALTHY: snapshot %d summary valid-through-ts=%s added-data-files=%s commit-id=%s",
          s.snapshotId(), s.summary().get("kafka.connect.valid-through-ts"),
          s.summary().get("added-data-files"), s.summary().get("kafka.connect.commit-id"));
    }
    boolean everyCommitPartial =
        reported.values().stream().noneMatch(p -> p.contains("src-0") && p.contains("src-1"));
    log("HEALTHY: every commit so far had only src-1 reported (=> timeout => commit(true)): %s",
        everyCommitPartial);

    // ---- the environment event: permanent loss of write access to the table metadata
    long revokedAt = System.currentTimeMillis();
    revokeMetadataWrite();
    int rowsAtRevoke = rows().size();

    boolean everFailed = false;
    boolean coordinatorAlways = true;
    for (int i = 1; i <= 45; i += 1) {
      produce(1, 1, 0);
      Thread.sleep(1000);
      Map<Integer, String> states = taskStates();
      everFailed |= states.containsValue("FAILED");
      boolean alive = coordinatorThreadAlive();
      coordinatorAlways &= alive;
      if (i % 5 == 0 || states.containsValue("FAILED")) {
        long now = System.currentTimeMillis();
        log("%s (%4.1fs after revoke): states=%s coordThreadAlive=%s snapshots=%d rows=%d "
                + "sourceCommitted=%s StartCommit=%d CommitToTable=%d CommitComplete=%d",
            rel(now), (now - revokedAt) / 1000.0, states, alive, snapshots().size(), rows().size(),
            sourceCommitted(), countCtl("START_COMMIT", revokedAt),
            countCtl("COMMIT_TO_TABLE", revokedAt), countCtl("COMMIT_COMPLETE", revokedAt));
      }
    }

    long startCommits = countCtl("START_COMMIT", revokedAt);
    long commitCompletes = countCtl("COMMIT_COMPLETE", revokedAt);
    long commitToTables = countCtl("COMMIT_TO_TABLE", revokedAt);
    long dataWritten = countCtl("DATA_WRITTEN", revokedAt);
    List<String> missing = committedButNotInTable();
    int snapsAfter = snapshots().size();
    // per-round outcome read back from the control topic: a round "failed" if its commit id never
    // got a CommitComplete; each later StartCommit proves the coordinator survived that failure.
    // (A round whose worker answered with DataComplete only has nothing to commit, touches no
    // table metadata and completes: "committed to 0 table(s)".) The newest round may be open.
    Set<String> completed = new java.util.HashSet<>();
    List<String> roundsAfter = new java.util.ArrayList<>();
    for (CtlEvent e : ctlEvents) {
      if ("COMMIT_COMPLETE".equals(e.type)) {
        completed.add(e.commitId);
      } else if ("START_COMMIT".equals(e.type) && e.seenAt >= revokedAt) {
        roundsAfter.add(e.commitId);
      }
    }
    long failedRoundsSurvived =
        roundsAfter.subList(0, Math.max(0, roundsAfter.size() - 1)).stream()
            .filter(id -> !completed.contains(id))
            .count();
    long emptyRoundsCompleted = roundsAfter.stream().filter(completed::contains).count();

    log("RESULT A: window=45s after revoke: StartCommit=%d DataWritten=%d CommitToTable=%d "
            + "CommitComplete=%d (empty rounds that completed without touching the table=%d)",
        startCommits, dataWritten, commitToTables, commitCompletes, emptyRoundsCompleted);
    log("RESULT A: failed partial rounds survived=%d (configured max-consecutive-failures=%d)",
        failedRoundsSurvived, MAX_CONSECUTIVE_FAILURES);
    log("RESULT A: task ever FAILED=%s, coordinator thread alive throughout=%s, final states=%s",
        everFailed, coordinatorAlways, taskStates());
    log("RESULT A: snapshots before=%d after=%d; rows at revoke=%d now=%d",
        snapsBefore.size(), snapsAfter, rowsAtRevoke, rows().size());
    log("RESULT A: sourceCommitted=%s produced=%s; %d records committed in Kafka but NOT in the "
            + "table: %s", sourceCommitted(), produced, missing.size(), missing);

    boolean bug =
        everyCommitPartial
            && !everFailed
            && coordinatorAlways
            && commitToTables == 0
            && snapsAfter == snapsBefore.size()
            && failedRoundsSurvived > MAX_CONSECUTIVE_FAILURES
            && !missing.isEmpty();
    log("BUG A %s: permanent non-retryable commit failure on the partial path retried %d times "
            + "(> budget %d) with the task RUNNING and %d committed source records absent from "
            + "the table", bug ? "REPRODUCED" : "NOT REPRODUCED", failedRoundsSurvived,
        MAX_CONSECUTIVE_FAILURES, missing.size());

    // ---- epilogue: what the retained buffer does if the environment is repaired
    restoreMetadataWrite();
    long restoredAt = System.currentTimeMillis();
    log("ENV EVENT: write access restored at %s", rel(restoredAt));
    int total = produced.values().stream().mapToInt(Integer::intValue).sum();
    await("all produced rows in the table after repair", Duration.ofSeconds(90),
        () -> quietly(() -> rows().size() >= total));
    Thread.sleep(1500);
    Map<String, Integer> rows = rows();
    long dups = rows.values().stream().filter(n -> n > 1).count();
    List<Snapshot> snaps = snapshots();
    Snapshot healing = snaps.get(snaps.size() - 1);
    log("EPILOGUE A: after repair rows=%d (produced %d) duplicates=%d, missing-below-committed=%d;"
            + " healing snapshot added-data-files=%s added-records=%s",
        rows.size(), total, dups, committedButNotInTable().size(),
        healing.summary().get("added-data-files"), healing.summary().get("added-records"));

    assertThat(bug).as("CR-12 scenario A manifested").isTrue();
  }
}
JAVA

cat > "$SRC_DIR/Cr12FullPathControlTest.java" <<'JAVA'
package org.apache.iceberg.connect.cr12;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Scenario B (control). Both partitions receive records, both workers answer, commits are full:
 * the same permanent failure goes through commit(false) and the coordinator terminates
 * (Coordinator.java:176-179), failing the task -- the behaviour #16237 designed for operators.
 */
class Cr12FullPathControlTest extends Cr12Base {

  @Test
  void samePermanentFailureOnFullPathFailsTheTask() throws Exception {
    startCluster("cr12-full");
    startConnector(connectorConfig(1000, 3000));

    for (int i = 0; i < 5; i += 1) {
      produce(0, 1, 0);
      produce(1, 1, 200);
    }
    await("healthy full commit lands 10 rows", Duration.ofSeconds(120),
        () -> quietly(() -> rows().size() >= 10));
    Thread.sleep(1500);
    log("HEALTHY: snapshots=%d rows=%d states=%s", snapshots().size(), rows().size(), taskStates());
    log("HEALTHY: partitions reported per commit id (DataComplete) = %s",
        reportedPartitionsByCommit());

    long revokedAt = System.currentTimeMillis();
    revokeMetadataWrite();
    boolean failed = false;
    long failedAt = -1;
    for (int i = 1; i <= 60 && !failed; i += 1) {
      produce(0, 1, 0);
      produce(1, 1, 0);
      Thread.sleep(1000);
      Map<Integer, String> states = taskStates();
      if (states.containsValue("FAILED")) {
        failed = true;
        failedAt = System.currentTimeMillis();
      }
      if (i % 5 == 0 || failed) {
        long now = System.currentTimeMillis();
        log("%s (%4.1fs after revoke): states=%s coordThreadAlive=%s StartCommit=%d",
            rel(now), (now - revokedAt) / 1000.0, states, coordinatorThreadAlive(),
            countCtl("START_COMMIT", revokedAt));
      }
    }
    log("RESULT B: task FAILED=%s after %.1fs; StartCommit after revoke=%d; %s",
        failed, failed ? (failedAt - revokedAt) / 1000.0 : -1.0,
        countCtl("START_COMMIT", revokedAt), taskTraces().replace('\n', ' '));
    log("CONTROL B %s: on the full path the same failure terminates the coordinator and fails "
        + "the task", failed ? "CONFIRMED" : "NOT CONFIRMED");
    assertThat(failed).as("full-path control fails the task").isTrue();
  }
}
JAVA

cat > "$SRC_DIR/Cr12FencedCoordinatorTest.java" <<'JAVA'
package org.apache.iceberg.connect.cr12;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * Scenario C. Partial steady state (src-0 idle). While a commit is in progress, a newly elected
 * coordinator's initTransactions() on the stable coordinator transactional.id fences the running
 * coordinator. Does the fenced coordinator keep looping, or exit?
 */
class Cr12FencedCoordinatorTest extends Cr12Base {

  @Test
  void fencedCoordinatorOnPartialPath() throws Exception {
    startCluster("cr12-fenced");
    // interval 8 s > timeout 3 s: a gap after each partial commit before the next StartCommit
    startConnector(connectorConfig(8000, 3000));
    produce(1, 3, 200);
    await("healthy partial commit lands", Duration.ofSeconds(120),
        () -> quietly(() -> rows().size() >= 3));

    long marker = System.currentTimeMillis();
    await("next StartCommit", Duration.ofSeconds(30), () -> countCtl("START_COMMIT", marker) > 0);
    long fencedAt = System.currentTimeMillis();
    int snapsAtFence = snapshots().size();
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "connect-" + CONNECTOR + "-coordinator");
    try (KafkaProducer<String, String> newCoordinator =
        new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
      newCoordinator.initTransactions(); // what KafkaClientFactory.createProducer does
    }
    log("FENCE: coordinator transactional.id epoch bumped at %s (commit in progress)", rel(fencedAt));
    produce(1, 2, 200);

    long diedAt = -1;
    int snapsWhileAlive = snapsAtFence;
    for (int i = 0; i < 160; i += 1) { // 40 s
      Thread.sleep(250);
      boolean alive = coordinatorThreadAlive();
      if (alive) {
        snapsWhileAlive = snapshots().size();
      } else if (diedAt < 0) {
        diedAt = System.currentTimeMillis();
        log("FENCE: coordinator thread gone at %s (%.1fs after the fence)", rel(diedAt),
            (diedAt - fencedAt) / 1000.0);
      }
      if (i % 8 == 0) {
        long now = System.currentTimeMillis();
        log("%s (%4.1fs after fence): coordThreadAlive=%s states=%s snapshots=%d StartCommit=%d "
                + "CommitToTable=%d CommitComplete=%d",
            rel(now), (now - fencedAt) / 1000.0, alive, taskStates(), snapshots().size(),
            countCtl("START_COMMIT", fencedAt), countCtl("COMMIT_TO_TABLE", fencedAt),
            countCtl("COMMIT_COMPLETE", fencedAt));
      }
    }
    log("RESULT C: fenced at %s, coordinator thread exited=%s after %.1fs; snapshots at fence=%d,"
            + " while fenced coordinator alive=%d; task states=%s",
        rel(fencedAt), diedAt > 0, diedAt > 0 ? (diedAt - fencedAt) / 1000.0 : -1.0,
        snapsAtFence, snapsWhileAlive, taskStates());
    assertThat(taskStates().values()).doesNotContain("FAILED");
  }
}
JAVA

# -------------------------------------------------------------------------------------------------
# 4. Build (offline first: every dependency is in the local Gradle cache) and run.
cd "$BUILD" || exit 2
echo "==> Building pristine sink + CR-12 repro source set"
if ! timeout 2400 ./gradlew --offline "${GRADLE_FLAGS[@]}" "$GRADLE_PROJECT:compileCr12ReproJava" \
    > "$LOG_DIR/CR-12.build.log" 2>&1; then
  echo "offline build failed; retrying online"
  if ! timeout 2400 ./gradlew "${GRADLE_FLAGS[@]}" "$GRADLE_PROJECT:compileCr12ReproJava" \
      >> "$LOG_DIR/CR-12.build.log" 2>&1; then
    echo "BUILD FAILED (see $LOG_DIR/CR-12.build.log)"; tail -40 "$LOG_DIR/CR-12.build.log"; exit 3
  fi
fi
echo "    build OK"

status=0
for s in $SCENARIOS; do
  log="$LOG_DIR/CR-12.$s.log"
  echo "==> Scenario $s  ($(date '+%H:%M:%S'))  log: $log"
  if timeout 900 ./gradlew --offline "${GRADLE_FLAGS[@]}" "$GRADLE_PROJECT:cr12ReproTest" \
      --tests "org.apache.iceberg.connect.cr12.Cr12${s}Test" > "$log" 2>&1; then
    echo "    TEST PASSED: $s"
  else
    rc=$?
    [ "$rc" -eq 124 ] && echo "    TIMEOUT: $s" || echo "    TEST FAILED ($rc): $s"
    status=1
  fi
  echo "    --- key lines ($s)"
  grep -E "\[CR12\]" "$log" | sed 's/^.*\[CR12\]/    [CR12]/' | grep -E "HEALTHY: every|HEALTHY: snapshots|ENV EVENT|RESULT|BUG|CONTROL|EPILOGUE|FENCE|after revoke|after fence" | head -60
  echo "    --- sink log lines ($s)"
  echo "    'Partial commit ... failed ... will retry' WARN lines: $(grep -c 'Partial commit .* failed for task .*, will retry' "$log")"
  echo "    'Commit timeout reached' INFO lines:                  $(grep -c 'Commit timeout reached' "$log")"
  echo "    'ready, received responses for all' INFO lines:       $(grep -c 'ready, received responses for all' "$log")"
  echo "    'Coordinator error during process, exiting thread':   $(grep -c 'Coordinator error during process, exiting thread' "$log")"
  echo "    'was fenced by a newer coordinator; clearing':         $(grep -c 'was fenced by a newer coordinator; clearing' "$log")"
  echo "    'Coordinator unexpectedly terminated':                 $(grep -c 'Coordinator unexpectedly terminated' "$log")"
  grep -m3 -E "Partial commit .* failed for task .*, will retry" -A2 "$log" | sed 's/^/    | /' | cut -c1-260
  grep -m2 -E "Coordinator error during process, exiting thread" -A2 "$log" | sed 's/^/    | /' | cut -c1-260
done

echo "==> done ($(date '+%H:%M:%S')), status=$status"
exit $status
