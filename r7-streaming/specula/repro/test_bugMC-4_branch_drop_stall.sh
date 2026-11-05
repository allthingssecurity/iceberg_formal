#!/usr/bin/env bash
# =================================================================================================
# MC-4 reproduction — Iceberg Kafka Connect sink, commit branch dropped after a WAP publish:
#   read floor   : Coordinator.lastCommittedOffsetsForTable -> table.snapshot(branch) == null -> {}
#                  (Coordinator.java:388-400)
#   validator    : SnapshotProducer.apply -> SnapshotUtil.latestSnapshot(base, branch) falls back
#                  to main (SnapshotProducer.java:299, SnapshotUtil.java:478-486) -> main's floor
#   => {} != {0=n}: ValidationException on EVERY commit; full path fails the task on every restart
#      (Coordinator.java:176-179 -> CoordinatorThread -> CommitterImpl NotRunningException), partial
#      path swallows and retries forever (Coordinator.java:165-174). Only a successful commit would
#      recreate the branch, and that is exactly the commit that fails.
#
# Escalation level: 0 (pure black box). Real pristine sink code (apache/iceberg @ 9fffeb6), driven
# through public interfaces only:
#   * Mc4BranchDropStallE2ETest — embedded Kafka 3.9.2 KRaft broker + Kafka Connect worker running the
#     real IcebergSinkConnector against a real HadoopCatalog warehouse; the "operator" performs the
#     documented Iceberg Audit-Branch (WAP) workflow with the public ManageSnapshots API
#     (docs/docs/branching.md:77-104, docs/docs/spark-ddl.md:602-607). No failpoints, no sleeps in
#     the system under test, no source patches, no state injection.
#   * Mc4BranchDropComponentTest — upstream's own Coordinator harness (ChannelTestBase: MockProducer/
#     MockConsumer + InMemoryCatalog) fed the legitimate control-topic events real workers send;
#     covers the partial-commit path deterministically and the automatic ref-expiry trigger
#     (RETAIN + expireSnapshots, branching.md:91,104).
#
# Usage: test_bugMC-4_branch_drop_stall.sh [ICEBERG_SOURCE_REPO] [WORK_DIR]
#   ICEBERG_SOURCE_REPO : any git checkout containing commit 9fffeb6 (default: the run's source/)
#   WORK_DIR            : where the pristine clone is built (default: confirmation/MC-4)
# Exit: 0 = bug reproduced (all assertions of the buggy behaviour held), 1 = not reproduced,
#       2 = build/environment problem.
# =================================================================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$(cd "$HERE/.." && pwd)"                                   # .specula-output
SRC="${1:-$(cd "$OUT/../source" 2>/dev/null && pwd)}"
WORK="${2:-$OUT/confirmation/MC-4}"
PRISTINE="$WORK/pristine-src"
SHA=9fffeb63b1605a7c3acf218e58b7eeb43bdb1b01
LOG="$HERE/MC-4.log"
RAW="$HERE/MC-4.gradle-output.log"
PROJ=":iceberg-kafka-connect:iceberg-kafka-connect"
MOD="$PRISTINE/kafka-connect/kafka-connect"
PKG_DIR="$MOD/src/mc4Repro/java/org/apache/iceberg/connect/channel"

say() { echo "$*" | tee -a "$LOG"; }
: > "$LOG"
say "== MC-4 repro started $(date -u +%FT%TZ)"
say "source repo : $SRC"
say "pristine    : $PRISTINE"

# ---------------------------------------------------------------- 1. pristine checkout of 9fffeb6
if [ ! -d "$PRISTINE/.git" ]; then
  git clone -q "$SRC" "$PRISTINE" || { say "clone failed"; exit 2; }
fi
git -C "$PRISTINE" checkout -q --detach "$SHA" 2>/dev/null || true
HEAD_SHA="$(git -C "$PRISTINE" rev-parse HEAD)"
say "HEAD        : $HEAD_SHA"
[ "$HEAD_SHA" = "$SHA" ] || { say "unexpected HEAD"; exit 2; }
# the system under test must be unmodified (only the build file gets the extra test source set)
if ! git -C "$PRISTINE" diff --quiet HEAD -- kafka-connect/kafka-connect/src/main \
     kafka-connect/kafka-connect-events/src/main core/src/main api/src/main data/src/main; then
  say "system-under-test sources are modified; refusing to run"; exit 2
fi
say "pristine    : OK (no changes under kafka-connect/*/src/main, core, api, data)"
if [ ! -f "$PRISTINE/gradle/wrapper/gradle-wrapper.jar" ] && [ -f "$SRC/gradle/wrapper/gradle-wrapper.jar" ]; then
  cp "$SRC/gradle/wrapper/gradle-wrapper.jar" "$PRISTINE/gradle/wrapper/gradle-wrapper.jar"
fi

# ---------------------------------------------------------------- 2. test source set (build config only)
if ! grep -q "MC-4 repro source set" "$PRISTINE/kafka-connect/build.gradle"; then
cat >> "$PRISTINE/kafka-connect/build.gradle" <<'GRADLE'

// >>> MC-4 repro source set (bug-confirmation; build configuration only) >>>
project(':iceberg-kafka-connect:iceberg-kafka-connect') {
  sourceSets {
    mc4Repro {
      compileClasspath += sourceSets.main.output + sourceSets.test.output
      runtimeClasspath += sourceSets.main.output + sourceSets.test.output
    }
  }
  configurations {
    mc4ReproImplementation.extendsFrom testImplementation
    mc4ReproRuntimeOnly.extendsFrom testRuntimeOnly
  }
  dependencies {
    def kafkaVersion = libs.versions.kafka.get()
    mc4ReproImplementation libs.kafka.clients
    mc4ReproImplementation libs.kafka.connect.api
    mc4ReproImplementation libs.kafka.connect.json
    mc4ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}"
    mc4ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}:test"
    mc4ReproImplementation "org.apache.kafka:kafka-clients:${kafkaVersion}:test"
    mc4ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}"
    mc4ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}:test"
    mc4ReproImplementation "org.apache.kafka:kafka-server-common:${kafkaVersion}:test"
  }
  pluginManager.withPlugin('net.ltgt.errorprone') {
    tasks.matching { it.name == 'compileMc4ReproJava' }.configureEach {
      options.errorprone.enabled = false
    }
  }
  task mc4ReproTest(type: Test) {
    description = 'MC-4 bug reproduction'
    useJUnitPlatform()
    testClassesDirs = sourceSets.mc4Repro.output.classesDirs
    classpath = sourceSets.mc4Repro.runtimeClasspath
    forkEvery = 1
    maxParallelForks = 1
    maxHeapSize = "3g"
    jvmArgs += project.property('extraJvmArgs')
    systemProperty 'org.slf4j.simpleLogger.defaultLogLevel', 'warn'
    systemProperty 'org.slf4j.simpleLogger.showDateTime', 'true'
    systemProperty 'org.slf4j.simpleLogger.dateTimeFormat', 'HH:mm:ss.SSS'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka', 'error'
    systemProperty 'org.slf4j.simpleLogger.log.kafka', 'error'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.hadoop', 'error'
    systemProperty 'org.slf4j.simpleLogger.log.org.eclipse.jetty', 'error'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.iceberg.connect.channel.Coordinator', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.connect.runtime.WorkerTask', 'error'
    outputs.upToDateWhen { false }
    testLogging {
      events "started", "passed", "skipped", "failed", "standard_out", "standard_error"
      exceptionFormat "full"
      showStandardStreams = true
    }
  }
}
// <<< MC-4 repro source set <<<
GRADLE
fi

mkdir -p "$PKG_DIR" "$MOD/src/mc4Repro/resources"
cat > "$MOD/src/mc4Repro/resources/log4j.properties" <<'LOG4J'
log4j.rootLogger=WARN, stderr
log4j.appender.stderr=org.apache.log4j.ConsoleAppender
log4j.appender.stderr.Target=System.err
log4j.appender.stderr.layout=org.apache.log4j.PatternLayout
log4j.appender.stderr.layout.ConversionPattern=%d{HH:mm:ss.SSS} %-5p %c{1} - %m%n
log4j.logger.org.apache.kafka=ERROR
log4j.logger.kafka=ERROR
log4j.logger.org.apache.iceberg.connect.channel.Coordinator=INFO
LOG4J

# ---------------------------------------------------------------- 3a. end-to-end test (Level 0)
cat > "$PKG_DIR/Mc4BranchDropStallE2ETest.java" <<'JAVA'
/*
 * MC-4 end-to-end reproduction (Level 0): real Kafka broker + Kafka Connect worker + pristine
 * IcebergSinkConnector + HadoopCatalog. The test plays the operator of the documented Iceberg
 * Audit-Branch (WAP) workflow; it never touches connector internals.
 */
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class Mc4BranchDropStallE2ETest {
  private static final String SRC = "mc4-src";
  private static final String CTL = "mc4-ctl";
  private static final String CONNECTOR = "mc4-sink";
  private static final String BRANCH = "audit";
  private static final String GROUP = "connect-" + CONNECTOR; // IcebergSinkConfig default group id
  private static final String OFFSETS_PROP = "kafka.connect.offsets." + CTL + "." + GROUP;
  private static final String STALE = "stale offsets: Expected: {} Committed: {0=";
  private static final TableIdentifier TBL = TableIdentifier.of("db", "tbl");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir Path tmp;

  private EmbeddedConnectCluster connect;
  private KafkaProducer<String, String> producer;
  private HadoopCatalog catalog; // the operator's own catalog client (same warehouse as the sink)
  private String warehouse;
  private long nextId = 1;
  private final long t0 = System.currentTimeMillis();

  private void observed(String fmt, Object... args) {
    System.out.printf(
        "MC4-OBSERVED [e2e +%5.1fs] %s%n",
        (System.currentTimeMillis() - t0) / 1000.0,
        String.format(fmt, args));
  }

  @BeforeEach
  void setUp() throws Exception {
    warehouse = tmp.resolve("warehouse").toUri().toString();
    catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("iceberg", Map.of("warehouse", warehouse));
    catalog.createNamespace(Namespace.of("db"));
    catalog.createTable(
        TBL,
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of("format-version", "2", "write.format.default", "avro"));

    Properties brokerProps = new Properties();
    brokerProps.put("transaction.state.log.replication.factor", "1");
    brokerProps.put("transaction.state.log.min.isr", "1");
    brokerProps.put("transaction.state.log.num.partitions", "4");
    brokerProps.put("offsets.topic.num.partitions", "4");

    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("connector.client.config.override.policy", "All");
    // idle tasks call put([]) at least every 500 ms, so workers answer StartCommit promptly
    workerProps.put("offset.flush.interval.ms", "500");
    workerProps.put("task.shutdown.graceful.timeout.ms", "10000");

    connect =
        new EmbeddedConnectCluster.Builder()
            .name("mc4-connect")
            .numWorkers(1)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(1, "Connect worker did not start");
    connect.kafka().createTopic(SRC, 1);
    connect.kafka().createTopic(CTL, 1);

    Map<String, Object> pp = new HashMap<>();
    pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    pp.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(pp, new StringSerializer(), new StringSerializer());
    warmUpTransactions();
  }

  @AfterEach
  void tearDown() {
    try {
      if (producer != null) {
        producer.close(Duration.ofSeconds(5));
      }
    } catch (Exception e) {
      // ignore
    }
    try {
      if (connect != null) {
        connect.stop();
      }
    } catch (Exception e) {
      // ignore
    }
    try {
      if (catalog != null) {
        catalog.close();
      }
    } catch (Exception e) {
      // ignore
    }
  }

  /** The first InitProducerId on a fresh broker is slow; do it before the connector starts. */
  private void warmUpTransactions() {
    for (int i = 0; i < 4; i += 1) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "mc4-warmup-" + i);
      try (KafkaProducer<String, String> warm =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  private Map<String, String> connectorConfig() {
    Map<String, String> c = new HashMap<>();
    c.put("connector.class", IcebergSinkConnector.class.getName());
    c.put("tasks.max", "1");
    c.put("topics", SRC);
    c.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    c.put("value.converter.schemas.enable", "false");
    c.put("iceberg.tables", TBL.toString());
    c.put("iceberg.tables.default-commit-branch", BRANCH); // docs/docs/kafka-connect.md:71
    c.put("iceberg.catalog.type", "hadoop");
    c.put("iceberg.catalog.warehouse", warehouse);
    c.put("iceberg.control.topic", CTL);
    c.put("iceberg.control.commit.interval-ms", "3000");
    c.put("iceberg.control.commit.timeout-ms", "5000");
    c.put("iceberg.kafka.bootstrap.servers", connect.kafka().bootstrapServers());
    return c;
  }

  private void produce(int n) {
    for (int i = 0; i < n; i += 1) {
      long id = nextId++;
      producer.send(
          new ProducerRecord<>(SRC, 0, "k" + id, "{\"id\":" + id + ",\"data\":\"row-" + id + "\"}"));
    }
    producer.flush();
  }

  private static void await(String what, long timeoutMs, BooleanSupplier cond)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (cond.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for: " + what);
  }

  private Table table() {
    return catalog.loadTable(TBL);
  }

  /** total-records of a ref's head, or -1 if the ref does not exist. */
  private long refRecords(String ref) {
    Snapshot s = table().snapshot(ref);
    if (s == null) {
      return -1;
    }
    String total = s.summary().get("total-records");
    return total == null ? 0 : Long.parseLong(total);
  }

  private Map<Long, Integer> idCounts(String ref) throws Exception {
    Table t = table();
    Snapshot s = t.snapshot(ref);
    Map<Long, Integer> counts = new TreeMap<>();
    if (s == null) {
      return counts;
    }
    try (CloseableIterable<Record> rows =
        IcebergGenerics.read(t).useSnapshot(s.snapshotId()).build()) {
      for (Record r : rows) {
        counts.merge((Long) r.getField("id"), 1, Integer::sum);
      }
    }
    return counts;
  }

  private ConnectorStateInfo.TaskState task0() {
    ConnectorStateInfo info = connect.connectorStatus(CONNECTOR);
    return info.tasks().isEmpty() ? null : info.tasks().get(0);
  }

  private String awaitTaskState(String state, long timeoutMs) throws InterruptedException {
    String[] trace = new String[1];
    await(
        "task 0 state " + state,
        timeoutMs,
        () -> {
          try {
            ConnectorStateInfo.TaskState ts = task0();
            if (ts != null && state.equals(ts.state())) {
              trace[0] = ts.trace();
              return true;
            }
          } catch (Exception e) {
            // status not available yet
          }
          return false;
        });
    return trace[0];
  }

  /** Read-committed census of the control topic by event type. */
  private Map<PayloadType, Integer> controlTopicCounts() {
    Map<String, Object> cp = new HashMap<>();
    cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    Map<PayloadType, Integer> counts = new EnumMap<>(PayloadType.class);
    for (PayloadType p : PayloadType.values()) {
      counts.put(p, 0);
    }
    try (KafkaConsumer<String, byte[]> c =
        new KafkaConsumer<>(cp, new StringDeserializer(), new ByteArrayDeserializer())) {
      TopicPartition tp = new TopicPartition(CTL, 0);
      c.assign(List.of(tp));
      c.seekToBeginning(List.of(tp));
      long end = c.endOffsets(List.of(tp)).get(tp);
      long deadline = System.currentTimeMillis() + 15_000;
      while (c.position(tp) < end && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> rec : c.poll(Duration.ofMillis(500))) {
          Event e = AvroUtil.decode(rec.value());
          counts.merge(e.payload().type(), 1, Integer::sum);
        }
      }
    }
    return counts;
  }

  private static String delta(Map<PayloadType, Integer> now, Map<PayloadType, Integer> before) {
    return now.keySet().stream()
        .map(k -> k + "+" + (now.get(k) - before.get(k)))
        .collect(Collectors.joining(", "));
  }

  private static String excerpt(String trace) {
    if (trace == null) {
      return "<no trace>";
    }
    return trace
        .lines()
        .filter(l -> l.contains("Exception") || l.contains("Caused by"))
        .limit(6)
        .map(String::trim)
        .collect(Collectors.joining(" | "));
  }

  private void assertStalled(long publishedHead, long rowsOnMain) {
    Table t = table();
    assertThat(t.currentSnapshot().snapshotId()).isEqualTo(publishedHead);
    assertThat(refRecords("main")).isEqualTo(rowsOnMain);
    assertThat(t.refs()).doesNotContainKey(BRANCH); // the connector never recreates the branch
  }

  @Test
  void droppedCommitBranchStallsEveryLaterCommit() throws Exception {
    // ---- WAP setup (docs/docs/branching.md "Audit Branch" steps 1-2): main has history and the
    // audit branch is created from it; the connector writes to the branch.
    Table t = table();
    t.newAppend().set("mc4.seed", "main-history").commit();
    long seed = t.currentSnapshot().snapshotId();
    t.manageSnapshots().createBranch(BRANCH, seed).commit();
    observed("setup: main head=%d (no rows); branch '%s' created at %d", seed, BRANCH, seed);

    connect.configureConnector(CONNECTOR, connectorConfig());
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 1, "connector did not start");
    observed(
        "connector RUNNING: tasks.max=1, iceberg.tables.default-commit-branch=%s, catalog=hadoop",
        BRANCH);

    // ---- Phase A: the connector commits to the audit branch
    produce(5); // ids 1..5
    await("5 rows on " + BRANCH, 120_000, () -> refRecords(BRANCH) == 5);
    t = table();
    observed(
        "phase A: audit head=%d total-records=5 %s=%s; main head=%d unchanged",
        t.snapshot(BRANCH).snapshotId(),
        OFFSETS_PROP,
        t.snapshot(BRANCH).summary().get(OFFSETS_PROP),
        t.currentSnapshot().snapshotId());

    // ---- Phase B (control): publish by fast-forwarding main, branch kept -> commits keep working
    t.manageSnapshots().fastForwardBranch("main", BRANCH).commit();
    produce(5); // ids 6..10
    await("10 rows on " + BRANCH, 120_000, () -> refRecords(BRANCH) == 10);
    observed("phase B (control): fast_forward(main, audit) with the branch kept -> next commit OK"
        + " (audit total-records=10)");
    t = table();
    t.manageSnapshots().fastForwardBranch("main", BRANCH).commit(); // publish again
    t = table();
    long published = t.currentSnapshot().snapshotId();
    observed(
        "phase B: published: main head=%d total-records=%d %s=%s",
        published,
        refRecords("main"),
        OFFSETS_PROP,
        t.currentSnapshot().summary().get(OFFSETS_PROP));

    // quiescence before the baseline census: the last pre-drop commit sends CommitComplete only
    // after its CommitToTable and its consumer-offset transaction, so wait until every started
    // commit has completed (otherwise its late CommitComplete would be counted after the drop)
    await(
        "all pre-drop commits complete",
        30_000,
        () -> {
          Map<PayloadType, Integer> c = controlTopicCounts();
          return c.get(PayloadType.START_COMMIT).equals(c.get(PayloadType.COMMIT_COMPLETE));
        });

    // ---- Phase C (trigger): DROP BRANCH audit (docs/docs/spark-ddl.md:602-607)
    t.manageSnapshots().removeBranch(BRANCH).commit();
    t = table();
    assertThat(t.refs()).doesNotContainKey(BRANCH);
    Map<PayloadType, Integer> ctlAtDrop = controlTopicCounts();
    observed(
        "phase C: branch '%s' dropped; refs=%s; control topic so far %s",
        BRANCH, t.refs().keySet(), ctlAtDrop);
    long dropAt = System.currentTimeMillis();
    produce(5); // ids 11..15
    String trace = awaitTaskState("FAILED", 120_000);
    observed(
        "phase C: %.1fs after the drop task 0 is FAILED: %s",
        (System.currentTimeMillis() - dropAt) / 1000.0,
        excerpt(trace));
    assertThat(trace)
        .contains("NotRunningException: Coordinator unexpectedly terminated")
        .contains("ValidationException")
        .contains(STALE);
    assertStalled(published, 10);

    // ---- Phase D1: restart, no new data -> worker not started -> commit times out -> partial path:
    // the ValidationException is swallowed ("will retry") and the task stays RUNNING, stalled
    connect.restartTask(CONNECTOR, 0);
    awaitTaskState("RUNNING", 60_000);
    Map<PayloadType, Integer> ctlD1 = controlTopicCounts();
    // observation window only (the system under test is untouched): the commit interval counts
    // from the previous StartCommit, so each timed-out round restarts at once -> ~5 rounds in 25 s
    Thread.sleep(25_000);
    assertThat(task0().state()).isEqualTo("RUNNING");
    Map<PayloadType, Integer> ctlD1end = controlTopicCounts();
    observed(
        "phase D1: restart #1 (no new data): task stays RUNNING for 25 s; control topic during the"
            + " window: %s",
        delta(ctlD1end, ctlD1));
    assertThat(ctlD1end.get(PayloadType.START_COMMIT) - ctlD1.get(PayloadType.START_COMMIT))
        .isGreaterThanOrEqualTo(2);
    assertThat(ctlD1end.get(PayloadType.COMMIT_TO_TABLE))
        .isEqualTo(ctlD1.get(PayloadType.COMMIT_TO_TABLE));
    assertStalled(published, 10);

    // ---- Phase D2: new data arrives -> worker answers -> full commit -> FAILED again
    produce(5); // ids 16..20
    String trace2 = awaitTaskState("FAILED", 120_000);
    observed("phase D2: new data (ids 16..20) -> task 0 FAILED again: %s", excerpt(trace2));
    assertThat(trace2).contains("Coordinator unexpectedly terminated").contains(STALE);
    assertStalled(published, 10);

    // ---- Phase D3: one more restart with data -> FAILED again (permanent)
    connect.restartTask(CONNECTOR, 0);
    awaitTaskState("RUNNING", 60_000);
    produce(1); // id 21
    String trace3 = awaitTaskState("FAILED", 120_000);
    observed("phase D3: restart #2 + id 21 -> task 0 FAILED again: %s", excerpt(trace3));
    assertThat(trace3).contains(STALE);
    assertStalled(published, 10);
    Map<PayloadType, Integer> ctlStall = controlTopicCounts();
    observed(
        "stall summary: %.1fs since the drop; control topic since the drop: %s; main still %d rows,"
            + " branch '%s' absent, ids 11..21 not in the table",
        (System.currentTimeMillis() - dropAt) / 1000.0,
        delta(ctlStall, ctlAtDrop),
        refRecords("main"),
        BRANCH);
    // no table commit at all since the drop (an empty round could still emit CommitComplete without
    // touching the table, so CommitComplete is reported above but is not the progress criterion)
    assertThat(ctlStall.get(PayloadType.COMMIT_TO_TABLE))
        .isEqualTo(ctlAtDrop.get(PayloadType.COMMIT_TO_TABLE));

    // ---- Phase E (root-cause check / operator workaround): recreate the branch at main's head ->
    // both floors resolve the same snapshot again -> the very next commit succeeds; no loss, no dup
    t = table();
    t.manageSnapshots().createBranch(BRANCH, t.currentSnapshot().snapshotId()).commit();
    connect.restartTask(CONNECTOR, 0);
    awaitTaskState("RUNNING", 60_000);
    produce(2); // ids 22..23
    await("23 rows on " + BRANCH, 120_000, () -> refRecords(BRANCH) == 23);
    Map<Long, Integer> ids = idCounts(BRANCH);
    observed(
        "phase E: after re-creating '%s' at main's head the commit succeeds: audit ids=%s"
            + " (each exactly once: %s); task 0 %s",
        BRANCH,
        ids.keySet(),
        ids.values().stream().allMatch(v -> v == 1),
        task0().state());
    assertThat(ids.keySet())
        .containsExactlyElementsOf(LongStream.rangeClosed(1, 23).boxed().collect(Collectors.toList()));
    assertThat(ids.values()).allMatch(v -> v == 1);
    assertThat(task0().state()).isEqualTo("RUNNING");
  }
}
JAVA

# ---------------------------------------------------------------- 3b. component test (Level 0)
cat > "$PKG_DIR/Mc4BranchDropComponentTest.java" <<'JAVA'
/*
 * MC-4 component reproduction on upstream's Coordinator harness (ChannelTestBase): the Coordinator
 * is fed exactly the control-topic events real workers send; the table is changed only through the
 * public Iceberg API (the coordinator's own commits + operator WAP/maintenance operations).
 */
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Mc4BranchDropComponentTest extends ChannelTestBase {
  private static final String BRANCH = "audit";
  private Coordinator coordinator;
  private long ctlOffset = 1;

  private static void observed(String fmt, Object... args) {
    System.out.println("MC4-OBSERVED [component] " + String.format(fmt, args));
  }

  @BeforeEach
  void commitBranchConfigured() {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.commitBranch()).thenReturn(BRANCH); // iceberg.tables.default-commit-branch
    when(config.tableConfig(any())).thenReturn(tableConfig);
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, mock(SinkTaskContext.class));
    coordinator.start();
    initConsumer();
  }

  private UUID startCommit() {
    int before = producer.history().size();
    coordinator.process();
    for (int i = producer.history().size() - 1; i >= before; i -= 1) {
      Event e = AvroUtil.decode(producer.history().get(i).value());
      if (e.type() == PayloadType.START_COMMIT) {
        return ((StartCommit) e.payload()).commitId();
      }
    }
    throw new AssertionError("coordinator sent no StartCommit");
  }

  private static DataFile newDataFile() {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("mc4/" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(5L)
        .build();
  }

  /** What a worker sends for its files of commit {@code commitId}. */
  private void dataWritten(UUID commitId) {
    Event e =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TABLE_IDENTIFIER, null),
                ImmutableList.of(newDataFile()),
                ImmutableList.of()));
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "key", AvroUtil.encode(e)));
  }

  /** What a worker sends once it has answered commit {@code commitId} for its partitions. */
  private void dataComplete(UUID commitId) {
    Event e =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(
                    new TopicPartitionOffset(SRC_TOPIC_NAME, 0, ctlOffset * 10, EventTestUtil.now()))));
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "key", AvroUtil.encode(e)));
  }

  private long sent(PayloadType type) {
    return producer.history().stream()
        .filter(r -> AvroUtil.decode(r.value()).type() == type)
        .count();
  }

  /** Full commit #1 lands on the (not yet existing) audit branch, creating it. */
  private Snapshot firstCommitCreatesBranch() {
    UUID c1 = startCommit();
    dataWritten(c1);
    dataComplete(c1);
    coordinator.process();
    table.refresh();
    Snapshot head = table.snapshot(BRANCH);
    assertThat(head).isNotNull();
    assertThat(head.summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}");
    assertThat(table.currentSnapshot()).isNull(); // main untouched
    observed(
        "commit #1 landed on branch '%s' (snapshot %d, %s={\"0\":3}); main has no snapshot",
        BRANCH, head.snapshotId(), OFFSETS_SNAPSHOT_PROP);
    return head;
  }

  /** Partial path: a worker's files arrive, its DataComplete does not; the commit times out. */
  private void partialCommitCycle() throws InterruptedException {
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    UUID c = startCommit();
    dataWritten(c);
    when(config.commitTimeoutMs()).thenReturn(0);
    Thread.sleep(5);
    coordinator.process(); // consumes the DataWritten, times out -> commit(true)
  }

  @Test
  void partialCommitPathRetriesForeverAfterBranchDrop() throws Exception {
    firstCommitCreatesBranch();

    // WAP publish + DROP BRANCH (operator, public API)
    table.manageSnapshots().fastForwardBranch("main", BRANCH).commit();
    table.manageSnapshots().removeBranch(BRANCH).commit();
    table.refresh();
    long mainHead = table.currentSnapshot().snapshotId();
    observed(
        "published: fast_forward(main, %s) then DROP BRANCH %s -> refs=%s, main head=%d carries %s=%s",
        BRANCH,
        BRANCH,
        table.refs().keySet(),
        mainHead,
        OFFSETS_SNAPSHOT_PROP,
        table.currentSnapshot().summary().get(OFFSETS_SNAPSHOT_PROP));
    long tablesBefore = sent(PayloadType.COMMIT_TO_TABLE);
    long completesBefore = sent(PayloadType.COMMIT_COMPLETE);

    for (int cycle = 1; cycle <= 5; cycle += 1) {
      partialCommitCycle(); // never throws: the ValidationException is swallowed ("will retry")
      table.refresh();
      assertThat(coordinator.partialCommitFailureCount()).isEqualTo(cycle);
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(mainHead);
      assertThat(table.refs()).doesNotContainKey(BRANCH);
      observed(
          "partial commit cycle %d: failed and swallowed (partialCommitFailureCount=%d); main head"
              + " still %d; branch '%s' still absent; buffered DataWritten files=%d",
          cycle, coordinator.partialCommitFailureCount(), mainHead, BRANCH, cycle);
    }
    assertThat(sent(PayloadType.COMMIT_TO_TABLE)).isEqualTo(tablesBefore);
    assertThat(sent(PayloadType.COMMIT_COMPLETE)).isEqualTo(completesBefore);
    observed(
        "after 5 cycles: CommitToTable sent +%d, CommitComplete sent +%d (no progress at all)",
        sent(PayloadType.COMMIT_TO_TABLE) - tablesBefore,
        sent(PayloadType.COMMIT_COMPLETE) - completesBefore);

    // control / root cause: re-create the branch at main's head -> both floors resolve the same
    // snapshot -> the very next partial commit succeeds and lands all 6 buffered files once
    table.manageSnapshots().createBranch(BRANCH, mainHead).commit();
    partialCommitCycle();
    table.refresh();
    Snapshot after = table.snapshot(BRANCH);
    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(5);
    assertThat(after.snapshotId()).isNotEqualTo(mainHead);
    assertThat(after.summary()).containsEntry("added-data-files", "6");
    observed(
        "control: branch re-created at main head -> next partial commit succeeded: snapshot %d"
            + " added-data-files=%s %s=%s",
        after.snapshotId(),
        after.summary().get("added-data-files"),
        OFFSETS_SNAPSHOT_PROP,
        after.summary().get(OFFSETS_SNAPSHOT_PROP));
  }

  @Test
  void fullCommitPathFailsAfterBranchRefExpires() throws Exception {
    firstCommitCreatesBranch();

    // publish, then the documented retention removes the idle branch ref automatically
    // (docs/docs/branching.md:91,104: "RETAIN n DAYS" + expireSnapshots; RemoveSnapshots.java:289-293)
    table.manageSnapshots().fastForwardBranch("main", BRANCH).commit();
    table.manageSnapshots().setMaxRefAgeMs(BRANCH, 1L).commit();
    Thread.sleep(20);
    table.expireSnapshots().commit();
    table.refresh();
    assertThat(table.refs()).doesNotContainKey(BRANCH);
    observed(
        "published + branch ref expired by expireSnapshots (max-ref-age): refs=%s",
        table.refs().keySet());

    UUID c2 = startCommit();
    dataWritten(c2);
    dataComplete(c2);
    assertThatThrownBy(() -> coordinator.process()) // full commit -> rethrown (Coordinator:176-179)
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot commit to db.tbl, stale offsets: Expected: {} Committed: {0=3}");
    table.refresh();
    assertThat(table.refs()).doesNotContainKey(BRANCH);
    observed(
        "full commit after expiry threw ValidationException 'stale offsets: Expected: {} Committed:"
            + " {0=3}' (in production this kills CoordinatorThread -> task FAILED)");
  }

  @Test
  void rowDeltaCommitPathFailsTheSameWay() throws Exception {
    firstCommitCreatesBranch();
    table.manageSnapshots().fastForwardBranch("main", BRANCH).commit();
    table.manageSnapshots().removeBranch(BRANCH).commit();
    table.refresh();
    long mainHead = table.currentSnapshot().snapshotId();

    // a worker of an upsert/CDC table answers with data + delete files -> RowDelta commit path
    // (Coordinator.java:327-340: newRowDelta().validateWith(...).toBranch(branch))
    UUID c2 = startCommit();
    Event e =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                c2,
                TableReference.of("catalog", TABLE_IDENTIFIER, null),
                ImmutableList.of(newDataFile()),
                ImmutableList.of(EventTestUtil.createDeleteFile())));
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "key", AvroUtil.encode(e)));
    dataComplete(c2);
    assertThatThrownBy(() -> coordinator.process())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot commit to db.tbl, stale offsets: Expected: {} Committed: {0=3}");
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(mainHead);
    assertThat(table.refs()).doesNotContainKey(BRANCH);
    observed(
        "RowDelta path (data + delete files) after DROP BRANCH threw the same ValidationException"
            + " 'Expected: {} Committed: {0=3}'; main head unchanged, branch '%s' absent",
        BRANCH);
  }
}
JAVA

# ---------------------------------------------------------------- 4. build + run (bounded)
cd "$PRISTINE" || exit 2
say "== gradle $PROJ:mc4ReproTest (offline, timeout 25 min)"
timeout 1500 ./gradlew --offline "$PROJ:mc4ReproTest" --console=plain > "$RAW" 2>&1
rc=$?
if [ $rc -ne 0 ] && grep -q "Could not resolve\|No cached version" "$RAW"; then
  say "offline resolution failed; retrying online"
  timeout 1500 ./gradlew "$PROJ:mc4ReproTest" --console=plain > "$RAW" 2>&1
  rc=$?
fi
say "gradle exit code: $rc (124 = timeout)"

# ---------------------------------------------------------------- 5. evidence
RES="$MOD/build/test-results/mc4ReproTest"
mkdir -p "$HERE/MC-4.junit"
cp "$RES"/*.xml "$HERE/MC-4.junit/" 2>/dev/null || true
say ""
say "== observations"
grep -h "MC4-OBSERVED" "$RAW" | sed 's/^ *//' | tee -a "$LOG"
say ""
say "== sink log lines (Coordinator / CoordinatorThread / WorkerTask), first occurrences"
grep -h -E "Partial commit .* failed|will retry|Coordinator error during process|unrecoverable exception|Snapshot ancestry validation failed|completed commit to table" "$RAW" \
  | sed 's/^ *//' | cut -c1-330 | awk '!seen[substr($0, 14, 120)]++' | head -40 | tee -a "$LOG"
PARTIAL_WARN=$(grep -c -E "Partial commit .* failed for task .* will retry" "$RAW")
VALIDATION=$(grep -c "Snapshot ancestry validation failed: Cannot commit to db.tbl, stale offsets: Expected: {} Committed: {0=" "$RAW")
say "count: 'Partial commit ... failed ..., will retry' WARN lines = $PARTIAL_WARN"
say "count: 'Snapshot ancestry validation failed ... Expected: {} Committed: {0=...}' lines = $VALIDATION"
say ""
say "== JUnit results"
grep -h -E "^(org\.apache|Mc4|.*Mc4).*(PASSED|FAILED|SKIPPED)|tests completed|BUILD (SUCCESSFUL|FAILED)" "$RAW" | sed 's/^ *//' | tee -a "$LOG"
for f in "$HERE"/MC-4.junit/*.xml; do
  [ -f "$f" ] && say "$(basename "$f"): $(grep -o 'tests="[0-9]*" skipped="[0-9]*" failures="[0-9]*" errors="[0-9]*"' "$f" | head -1)"
done

if [ $rc -eq 0 ] && grep -q "Mc4BranchDropStallE2ETest > droppedCommitBranchStallsEveryLaterCommit() PASSED" "$RAW" \
   && grep -q "Mc4BranchDropComponentTest > partialCommitPathRetriesForeverAfterBranchDrop() PASSED" "$RAW" \
   && grep -q "Mc4BranchDropComponentTest > fullCommitPathFailsAfterBranchRefExpires() PASSED" "$RAW" \
   && grep -q "Mc4BranchDropComponentTest > rowDeltaCommitPathFailsTheSameWay() PASSED" "$RAW"; then
  say ""
  say "RESULT: MC-4 REPRODUCED (all assertions of the buggy behaviour held; full log: $RAW)"
  exit 0
fi
if [ $rc -ne 0 ] && ! grep -q "Mc4BranchDrop" "$RAW"; then
  say "RESULT: BUILD/ENV PROBLEM (see $RAW)"; tail -40 "$RAW" | tee -a "$LOG"; exit 2
fi
say "RESULT: NOT REPRODUCED (see $RAW)"
grep -h -A 30 "FAILED" "$RAW" | head -80 | tee -a "$LOG"
exit 1
