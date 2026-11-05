#!/bin/bash
# =================================================================================================
# CR-6 reproduction: the coordinator thread hangs forever after terminate() when table commits are
# queued (Coordinator.terminate() -> exec.shutdownNow() drains queued commitToTable tasks whose
# futures Tasks.waitFor keeps blocking on, so CoordinatorThread never reaches coordinator.stop()).
#
# System under test: PRISTINE apache/iceberg @ HEAD of the per-finding worktree (9fffeb6), copied
# with `git archive` and byte-compared with git for every cited file (the worktree itself is
# TLA-instrumented and is NOT used for the run).
#
# Topology (one JVM): embedded Kafka 3.9.2 broker (KRaft) + one Kafka Connect worker running the
# real IcebergSinkConnector (tasks.max=1), a 1-partition source topic, a 1-partition control topic,
# and an in-memory Iceberg catalog shared by all sink-task instances (environment, as in the
# Specula trace harness). The connector writes every record to N tables (static iceberg.tables).
#
# Escalation level: 0 (black-box). Only public interfaces and normal operations are used:
# connector config through the Connect REST API, records produced to the source topic, and a
# task restart through the Connect REST API (POST /connectors/<c>/tasks/0/restart), which drives
# IcebergSinkTask.stop() -> CommitterImpl.close([]) -> stopCoordinator() -> terminate().
# No failpoints, no delays, no source changes. The restart is timed by observing the coordinator
# thread's stack the way jstack/ThreadMXBean would (fire when it is inside Coordinator.doCommit ->
# Tasks.waitFor). Observations: thread states, Kafka consumer-group membership (Admin API), JMX
# app-info MBeans, table contents.
#
# Scenarios (separate JVMs):
#   queued   : 60 tables, iceberg.control.commit.threads=2   -> tables queued in the commit pool
#   default  : 60 tables, commit.threads = connector default (2 x availableProcessors)
#   control  : 60 tables, iceberg.control.commit.threads=60  -> no table is ever queued
#
# Usage:  bash test_bugCR-6_coordinator_hang_after_terminate.sh            # build + all scenarios
#         SCENARIOS="build" bash ...;  SCENARIOS="queued" bash ...;  SCENARIOS="summary" bash ...
# Each scenario run is wrapped in an outer timeout (a hang of the test itself is reported, not
# retried). Logs: .specula-output/repro/CR-6.<scenario>.log
# =================================================================================================
set -uo pipefail

REPRO_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$(cd "$REPRO_DIR/.." && pwd)"
WT="$OUT/confirmation/CR-6/worktree"
SRC_TREE="$(cd "$OUT/../source" 2>/dev/null && pwd || true)"
PRISTINE="${CR6_PRISTINE:-$OUT/confirmation/CR-6/pristine}"
SCENARIOS="${SCENARIOS:-build queued control default summary}"
GP=":iceberg-kafka-connect:iceberg-kafka-connect"
KC="$PRISTINE/kafka-connect/kafka-connect"
PKG_DIR="$KC/src/cr6Repro/java/org/apache/iceberg/connect/cr6"

CITED_FILES=(
  kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java
  kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CoordinatorThread.java
  kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitterImpl.java
  kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Channel.java
  kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/IcebergSinkTask.java
  core/src/main/java/org/apache/iceberg/util/Tasks.java
)

# -------------------------------------------------------------------------------------------------
# 1. Pristine source tree
# -------------------------------------------------------------------------------------------------
prepare_tree() {
  if [ ! -f "$PRISTINE/settings.gradle" ]; then
    echo "==> Materializing pristine HEAD $(git -C "$WT" rev-parse HEAD) into $PRISTINE"
    mkdir -p "$PRISTINE"
    git -C "$WT" archive HEAD | tar -x -C "$PRISTINE" || { echo "git archive failed"; exit 2; }
  fi
  if [ ! -f "$PRISTINE/gradle/wrapper/gradle-wrapper.jar" ]; then
    # gradle-wrapper.jar is not tracked by git (gradlew downloads it); reuse the local copy
    cp "$SRC_TREE/gradle/wrapper/gradle-wrapper.jar" "$PRISTINE/gradle/wrapper/" \
      || { echo "no gradle-wrapper.jar available"; exit 2; }
  fi
  echo "==> Verifying the system under test is pristine (byte-compare with git HEAD)"
  for f in "${CITED_FILES[@]}"; do
    if git -C "$WT" show "HEAD:$f" | cmp -s - "$PRISTINE/$f"; then
      echo "    pristine: $f"
    else
      echo "    MODIFIED: $f  (refusing to run on a non-pristine tree)"; exit 2
    fi
  done
  if [ -d "$KC/src/main/java/org/apache/iceberg/connect/tla" ] \
     || [ -f "$KC/src/main/java/org/apache/iceberg/connect/channel/TlaHooks.java" ]; then
    echo "    instrumentation found in the pristine tree; refusing to run"; exit 2
  fi

  # --- Gradle wiring: a separate source set + test task (build files only; main sources untouched)
  if ! grep -q "CR6-REPRO-WIRING" "$PRISTINE/kafka-connect/build.gradle"; then
    cat >> "$PRISTINE/kafka-connect/build.gradle" <<'GRADLE'

// CR6-REPRO-WIRING (Specula bug confirmation CR-6): in-JVM Kafka + Connect cluster running the
// pristine sink. Adds a source set and a test task only; no production source is modified.
project(':iceberg-kafka-connect:iceberg-kafka-connect') {
  sourceSets {
    cr6Repro {
      compileClasspath += sourceSets.main.output
      runtimeClasspath += sourceSets.main.output
    }
  }

  configurations {
    cr6ReproImplementation.extendsFrom testImplementation
    cr6ReproRuntimeOnly.extendsFrom testRuntimeOnly
  }

  dependencies {
    def kafkaVersion = libs.versions.kafka.get()
    cr6ReproImplementation libs.kafka.clients
    cr6ReproImplementation libs.kafka.connect.api
    cr6ReproImplementation libs.kafka.connect.json
    cr6ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}"
    cr6ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}:test"
    cr6ReproImplementation "org.apache.kafka:kafka-clients:${kafkaVersion}:test"
    cr6ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}"
    cr6ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}:test"
    cr6ReproImplementation "org.apache.kafka:kafka-server-common:${kafkaVersion}:test"
  }

  pluginManager.withPlugin('net.ltgt.errorprone') {
    tasks.matching { it.name == 'compileCr6ReproJava' }.configureEach {
      options.errorprone.enabled = false
    }
  }

  task cr6ReproTest(type: Test) {
    description = 'Specula CR-6 reproduction scenarios'
    useJUnitPlatform()
    testClassesDirs = sourceSets.cr6Repro.output.classesDirs
    classpath = sourceSets.cr6Repro.runtimeClasspath
    forkEvery = 1
    maxParallelForks = 1
    maxHeapSize = "3g"
    jvmArgs += project.property('extraJvmArgs')
    systemProperty 'org.slf4j.simpleLogger.defaultLogLevel', 'warn'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.iceberg.connect.channel', 'info'
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

  mkdir -p "$PKG_DIR" "$KC/src/cr6Repro/resources"
  write_sources
}

# -------------------------------------------------------------------------------------------------
# 2. Reproduction sources (written into the cr6Repro source set)
# -------------------------------------------------------------------------------------------------
write_sources() {
cat > "$KC/src/cr6Repro/resources/simplelogger.properties" <<'EOF'
org.slf4j.simpleLogger.defaultLogLevel=warn
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS
org.slf4j.simpleLogger.log.org.apache.iceberg.connect.channel=info
EOF
cat > "$KC/src/cr6Repro/resources/log4j.properties" <<'EOF'
log4j.rootLogger=WARN, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{HH:mm:ss.SSS} %-5p [%t] %c{1}: %m%n
log4j.logger.org.apache.iceberg.connect.channel=INFO
EOF

cat > "$PKG_DIR/SharedInMemoryCatalog.java" <<'EOF'
package org.apache.iceberg.connect.cr6;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * Environment of the reproduction: one in-memory catalog shared by every sink task, worker and
 * coordinator of the JVM (each task instantiates its own instance through
 * iceberg.catalog.catalog-impl; all table operations go to the shared root). It only COUNTS
 * landed connector commits (like a catalog server's request log); it never delays, fails or
 * alters a commit.
 */
public class SharedInMemoryCatalog extends InMemoryCatalog {
  static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  static final AtomicInteger CONNECTOR_COMMITS_LANDED = new AtomicInteger();
  private static volatile SharedInMemoryCatalog root = null;

  static synchronized SharedInMemoryCatalog initRoot() {
    if (root == null) {
      SharedInMemoryCatalog catalog = new SharedInMemoryCatalog();
      catalog.initialize("iceberg", Map.of());
      root = catalog;
    }
    return root;
  }

  static SharedInMemoryCatalog root() {
    return root;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    if (this == root) {
      return new CountingOps(super.newTableOps(tableIdentifier));
    }
    return root.newTableOps(tableIdentifier);
  }

  private static boolean isConnectorCommit(TableMetadata base, TableMetadata metadata) {
    Snapshot next = metadata.currentSnapshot();
    if (next == null || !next.summary().containsKey(COMMIT_ID_PROP)) {
      return false;
    }
    Snapshot previous = base != null ? base.currentSnapshot() : null;
    return previous == null || previous.snapshotId() != next.snapshotId();
  }

  private static final class CountingOps implements TableOperations {
    private final TableOperations delegate;

    private CountingOps(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      boolean connectorCommit = isConnectorCommit(base, metadata);
      delegate.commit(base, metadata);
      if (connectorCommit) {
        CONNECTOR_COMMITS_LANDED.incrementAndGet();
      }
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      LocationProvider provider = delegate.locationProvider();
      return provider != null
          ? provider
          : LocationProviders.locationsFor(current().location(), current().properties());
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return delegate.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }
}
EOF

cat > "$PKG_DIR/Cr6Base.java" <<'EOF'
package org.apache.iceberg.connect.cr6;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Shared cluster setup and the scenario driver; subclasses choose tables and commit threads. */
abstract class Cr6Base {
  static final String CONNECTOR = "iceberg-sink";
  static final String SRC_TOPIC = "src";
  static final String CTL_TOPIC = "ctl";
  static final String COORD_GROUP = "connect-" + CONNECTOR + "-coord";
  static final String COORD_THREAD = "iceberg-coord";
  static final int RECORDS_PER_CYCLE = 5;
  static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  interface Check {
    boolean ok() throws Exception;
  }

  private EmbeddedConnectCluster connect;
  private KafkaProducer<String, String> producer;
  private Admin admin;
  private long nextId = 0;
  private boolean duplicatesSeen = false;
  private final long startNanos = System.nanoTime();

  abstract String scenario();

  abstract int numTables();

  /** Value for iceberg.control.commit.threads, or null for the connector default. */
  abstract String commitThreads();

  abstract long maxObserveSeconds();

  static String table(int index) {
    return String.format("db.t%03d", index);
  }

  void log(String format, Object... args) {
    double elapsed = (System.nanoTime() - startNanos) / 1e9;
    System.out.printf("CR6 [%s +%7.2fs] %s%n", scenario(), elapsed, String.format(format, args));
    System.out.flush();
  }

  @BeforeEach
  void setUp() throws Exception {
    SharedInMemoryCatalog root = SharedInMemoryCatalog.initRoot();
    root.createNamespace(Namespace.of("db"));
    for (int i = 0; i < numTables(); i += 1) {
      root.createTable(
          TableIdentifier.parse(table(i)),
          SCHEMA,
          PartitionSpec.unpartitioned(),
          Map.of("format-version", "2", "write.format.default", "avro"));
    }

    Properties brokerProps = new Properties();
    brokerProps.put("transaction.state.log.replication.factor", "1");
    brokerProps.put("transaction.state.log.min.isr", "1");
    brokerProps.put("transaction.state.log.num.partitions", "4");
    brokerProps.put("offsets.topic.num.partitions", "4");

    Map<String, String> workerProps = new HashMap<>();
    // put([]) cadence of an idle sink task: Worker.process() handles StartCommit promptly
    workerProps.put("offset.flush.interval.ms", "500");
    workerProps.put("task.shutdown.graceful.timeout.ms", "10000");

    connect =
        new EmbeddedConnectCluster.Builder()
            .name("cr6-" + scenario())
            .numWorkers(1)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(1, "Connect worker did not start");
    connect.kafka().createTopic(SRC_TOPIC, 1);
    connect.kafka().createTopic(CTL_TOPIC, 1);

    String bootstrap = connect.kafka().bootstrapServers();
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    Map<String, Object> adminProps = new HashMap<>();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    admin = Admin.create(adminProps);
    warmUpTransactions(bootstrap);
  }

  /** The first InitProducerId of a fresh broker creates __transaction_state (takes seconds). */
  private void warmUpTransactions(String bootstrap) {
    for (int i = 0; i < 4; i += 1) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "cr6-warmup-" + i);
      try (KafkaProducer<String, String> warm =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  @AfterEach
  void tearDown() {
    try {
      producer.close(Duration.ofSeconds(5));
      admin.close(Duration.ofSeconds(5));
    } catch (Exception e) {
      log("client close: %s", e);
    }
    try {
      connect.stop();
    } catch (Exception e) {
      log("cluster stop: %s", e);
    }
  }

  private Map<String, String> connectorConfig() {
    List<String> tables = new ArrayList<>();
    for (int i = 0; i < numTables(); i += 1) {
      tables.add(table(i));
    }
    Map<String, String> config = new HashMap<>();
    config.put("connector.class", IcebergSinkConnector.class.getName());
    config.put("tasks.max", "1");
    config.put("topics", SRC_TOPIC);
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("value.converter.schemas.enable", "false");
    config.put("iceberg.tables", String.join(",", tables));
    config.put("iceberg.catalog.catalog-impl", SharedInMemoryCatalog.class.getName());
    config.put("iceberg.control.topic", CTL_TOPIC);
    config.put("iceberg.control.commit.interval-ms", "3000");
    if (commitThreads() != null) {
      config.put("iceberg.control.commit.threads", commitThreads());
    }
    config.put("iceberg.kafka.bootstrap.servers", connect.kafka().bootstrapServers());
    return config;
  }

  private void produce(int count) throws Exception {
    for (int i = 0; i < count; i += 1) {
      long id = nextId++;
      String value = "{\"id\": " + id + ", \"data\": \"r" + id + "\"}";
      producer.send(new ProducerRecord<>(SRC_TOPIC, 0, "k" + id, value)).get();
    }
  }

  private static void await(String what, long timeoutMs, long pollMs, Check check)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!check.ok()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      Thread.sleep(pollMs);
    }
  }

  static List<Thread> coordinatorThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> COORD_THREAD.equals(t.getName()) && t.isAlive())
        .collect(Collectors.toList());
  }

  static long countThreads(String prefix) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.isAlive() && t.getName().startsWith(prefix))
        .count();
  }

  static boolean inTasksWaitFor(Thread thread) {
    for (StackTraceElement frame : thread.getStackTrace()) {
      if ("org.apache.iceberg.util.Tasks".equals(frame.getClassName())
          && "waitFor".equals(frame.getMethodName())) {
        return true;
      }
    }
    return false;
  }

  static String frames(Thread thread, int max) {
    StringBuilder result = new StringBuilder();
    StackTraceElement[] stack = thread.getStackTrace();
    for (int i = 0; i < Math.min(max, stack.length); i += 1) {
      result.append("        at ").append(stack[i]).append('\n');
    }
    return result.toString();
  }

  private List<MemberDescription> coordGroupMembers() throws Exception {
    ConsumerGroupDescription description =
        admin
            .describeConsumerGroups(List.of(COORD_GROUP))
            .describedGroups()
            .get(COORD_GROUP)
            .get(10, TimeUnit.SECONDS);
    return new ArrayList<>(description.members());
  }

  private static String members(List<MemberDescription> members, String oldMemberId) {
    return members.stream()
        .map(
            m ->
                (m.consumerId().equals(oldMemberId) ? "OLD" : "new")
                    + m.assignment().topicPartitions())
        .collect(Collectors.joining(",", "[", "]"));
  }

  /** Number of tables that contain exactly the ids [0, expected), each once. */
  private int tablesComplete(long expected) throws Exception {
    int complete = 0;
    for (int i = 0; i < numTables(); i += 1) {
      Table table = SharedInMemoryCatalog.root().loadTable(TableIdentifier.parse(table(i)));
      List<Long> ids = new ArrayList<>();
      try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
        for (Record row : rows) {
          ids.add(((Number) row.getField("id")).longValue());
        }
      }
      Set<Long> distinct = new HashSet<>(ids);
      if (ids.size() > distinct.size()) {
        duplicatesSeen = true;
      }
      if (ids.size() == expected && distinct.size() == expected) {
        complete += 1;
      }
    }
    return complete;
  }

  private static String fmt(Double seconds) {
    return seconds == null ? "never" : String.format("%.1f", seconds);
  }

  void runScenario() throws Exception {
    int procs = Runtime.getRuntime().availableProcessors();
    log(
        "setup: %d tables, iceberg.control.commit.threads=%s (connector default here = 2 x %d = %d),"
            + " Kafka consumer max.poll.interval.ms = default (300000)",
        numTables(),
        commitThreads() == null ? "<default>" : commitThreads(),
        procs,
        2 * procs);

    connect.configureConnector(CONNECTOR, connectorConfig());
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 1, "connector did not start");
    await("a coordinator thread", 60_000, 50, () -> coordinatorThreads().size() == 1);
    Thread oldCoord = coordinatorThreads().get(0);
    log(
        "coordinator thread started: name=%s id=%d daemon=%s",
        oldCoord.getName(), oldCoord.getId(), oldCoord.isDaemon());

    // ---- cycle 1: an undisturbed commit to every table (also checkpoints the -coord offsets)
    produce(RECORDS_PER_CYCLE);
    await(
        "cycle 1 committed to all tables",
        180_000,
        500,
        () -> tablesComplete(RECORDS_PER_CYCLE) == numTables());
    log(
        "cycle 1 committed normally: all %d tables contain %d rows (connector commits landed so far: %d)",
        numTables(),
        RECORDS_PER_CYCLE,
        SharedInMemoryCatalog.CONNECTOR_COMMITS_LANDED.get());
    List<MemberDescription> before = coordGroupMembers();
    log("-coord consumer group '%s' members before restart: %s", COORD_GROUP, members(before, ""));
    if (before.size() != 1) {
      throw new AssertionError("expected exactly one -coord member, got " + before);
    }
    String oldMemberId = before.get(0).consumerId();
    String oldClientId = before.get(0).clientId();

    // ---- cycle 2: restart the task (REST API) while the coordinator is inside cycle 2's doCommit.
    // (run 1 lesson: with many commit threads, cycle 1's doCommit can still be sending its
    // CommitToTable events after every cycle-1 table commit has landed; triggering on that tail
    // finds every task already dequeued. Wait for cycle 1's doCommit to end, then require that at
    // least one CYCLE-2 table commit has landed while the coordinator is in Tasks.waitFor.)
    await("cycle 1 doCommit finished", 60_000, 1, () -> !inTasksWaitFor(oldCoord));
    int landedBefore = SharedInMemoryCatalog.CONNECTOR_COMMITS_LANDED.get();
    produce(RECORDS_PER_CYCLE);
    await(
        "coordinator inside cycle-2 doCommit (Tasks.waitFor, >= 1 cycle-2 table landed)",
        120_000,
        1,
        () ->
            SharedInMemoryCatalog.CONNECTOR_COMMITS_LANDED.get() > landedBefore
                && inTasksWaitFor(oldCoord));
    int landedAtTrigger = SharedInMemoryCatalog.CONNECTOR_COMMITS_LANDED.get() - landedBefore;
    log(
        "coordinator observed inside Coordinator.doCommit -> Tasks.waitFor (cycle-2 table commits"
            + " landed so far: %d of %d); requesting REST restart of task 0",
        landedAtTrigger,
        numTables());
    long restartAt = System.nanoTime();
    connect.restartTask(CONNECTOR, 0);
    int landedAtRestartReturn =
        SharedInMemoryCatalog.CONNECTOR_COMMITS_LANDED.get() - landedBefore;
    log(
        "restart returned after %.2fs; cycle-2 table commits landed by the old coordinator: %d of %d",
        (System.nanoTime() - restartAt) / 1e9,
        landedAtRestartReturn,
        numTables());
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 1, "task did not come back");

    // ---- observe
    Double oldThreadExitedAt = null;
    Double oldMemberGoneAt = null;
    Double successorAssignedAt = null;
    Double allCompleteAt = null;
    boolean newCoordSeen = false;
    long deadline = restartAt + TimeUnit.SECONDS.toNanos(maxObserveSeconds());
    while (System.nanoTime() < deadline) {
      double since = (System.nanoTime() - restartAt) / 1e9;
      List<Thread> coords = coordinatorThreads();
      newCoordSeen = newCoordSeen || coords.stream().anyMatch(t -> t != oldCoord);
      boolean oldAlive = oldCoord.isAlive();
      if (!oldAlive && oldThreadExitedAt == null) {
        oldThreadExitedAt = since;
      }
      List<MemberDescription> group = coordGroupMembers();
      boolean oldMemberPresent = group.stream().anyMatch(m -> m.consumerId().equals(oldMemberId));
      if (!oldMemberPresent && oldMemberGoneAt == null) {
        oldMemberGoneAt = since;
      }
      boolean successorAssigned =
          group.stream()
              .anyMatch(
                  m ->
                      !m.consumerId().equals(oldMemberId)
                          && !m.assignment().topicPartitions().isEmpty());
      if (successorAssigned && successorAssignedAt == null) {
        successorAssignedAt = since;
      }
      int complete = tablesComplete(2L * RECORDS_PER_CYCLE);
      if (complete == numTables() && allCompleteAt == null) {
        allCompleteAt = since;
      }
      log(
          "t+%6.1fs | old iceberg-coord alive=%s state=%s inTasks.waitFor=%s | iceberg-coord"
              + " threads=%d, iceberg-committer threads=%d | -coord members=%s | tables with all"
              + " %d rows: %d/%d",
          since,
          oldAlive,
          oldCoord.getState(),
          oldAlive && inTasksWaitFor(oldCoord),
          coords.size(),
          countThreads("iceberg-committer"),
          members(group, oldMemberId),
          2 * RECORDS_PER_CYCLE,
          complete,
          numTables());
      if (allCompleteAt != null
          && (oldThreadExitedAt != null || oldMemberGoneAt != null)
          && since >= 15) {
        break;
      }
      Thread.sleep(since < 20 ? 2_000 : 10_000);
    }

    // ---- final evidence
    boolean oldConsumerOpen =
        ManagementFactory.getPlatformMBeanServer()
            .isRegistered(new ObjectName("kafka.consumer:type=app-info,id=" + oldClientId));
    boolean hung = oldCoord.isAlive() && inTasksWaitFor(oldCoord);
    log(
        "old coordinator thread at end: alive=%s state=%s daemon=%s, top frames:%n%s",
        oldCoord.isAlive(),
        oldCoord.getState(),
        oldCoord.isDaemon(),
        frames(oldCoord, 16));
    log(
        "old -coord consumer client.id=%s still open (JMX kafka.consumer app-info registered) = %s",
        oldClientId,
        oldConsumerOpen);
    log(
        "client threads alive: kafka-coordinator-heartbeat-thread|%s=%d,"
            + " kafka-producer-network-thread=%d, kafka-admin-client-thread=%d",
        COORD_GROUP,
        countThreads("kafka-coordinator-heartbeat-thread | " + COORD_GROUP),
        countThreads("kafka-producer-network-thread"),
        countThreads("kafka-admin-client-thread"));
    System.out.printf(
        "CR6-SUMMARY scenario=%s tables=%d commitThreads=%s restartInsideDoCommit=true"
            + " cycle2LandedByOldCoordinator=%d/%d oldThreadExitedAfterSec=%s"
            + " oldThreadHungInTasksWaitFor=%s oldMemberLeftGroupAfterSec=%s"
            + " successorAssignedAfterSec=%s allDataCommittedAfterSec=%s oldConsumerStillOpen=%s"
            + " newCoordinatorStarted=%s duplicates=%s verdict=%s%n",
        scenario(),
        numTables(),
        commitThreads() == null ? "default(" + (2 * procs) + ")" : commitThreads(),
        landedAtRestartReturn,
        numTables(),
        fmt(oldThreadExitedAt),
        hung,
        fmt(oldMemberGoneAt),
        fmt(successorAssignedAt),
        fmt(allCompleteAt),
        oldConsumerOpen,
        newCoordSeen,
        duplicatesSeen,
        hung ? "COORDINATOR_THREAD_HUNG" : "COORDINATOR_THREAD_EXITED");
    System.out.flush();
  }
}
EOF

cat > "$PKG_DIR/Cr6QueuedCommitsTest.java" <<'EOF'
package org.apache.iceberg.connect.cr6;

import org.junit.jupiter.api.Test;

/** 60 tables, 2 commit threads: 58 table commits wait in the pool queue during doCommit. */
public class Cr6QueuedCommitsTest extends Cr6Base {
  @Override
  String scenario() {
    return "queued";
  }

  @Override
  int numTables() {
    return 60;
  }

  @Override
  String commitThreads() {
    return "2";
  }

  @Override
  long maxObserveSeconds() {
    return 420;
  }

  @Test
  void restartDuringCommitWithQueuedTables() throws Exception {
    runScenario();
  }
}
EOF

cat > "$PKG_DIR/Cr6DefaultThreadsTest.java" <<'EOF'
package org.apache.iceberg.connect.cr6;

import org.junit.jupiter.api.Test;

/** 60 tables, connector-default commit threads (2 x availableProcessors). */
public class Cr6DefaultThreadsTest extends Cr6Base {
  @Override
  String scenario() {
    return "default";
  }

  @Override
  int numTables() {
    return 60;
  }

  @Override
  String commitThreads() {
    return null;
  }

  @Override
  long maxObserveSeconds() {
    return 420;
  }

  @Test
  void restartDuringCommitWithDefaultThreads() throws Exception {
    runScenario();
  }
}
EOF

cat > "$PKG_DIR/Cr6ControlTest.java" <<'EOF'
package org.apache.iceberg.connect.cr6;

import org.junit.jupiter.api.Test;

/** Control: 60 tables, 60 commit threads, so no table commit is ever queued. */
public class Cr6ControlTest extends Cr6Base {
  @Override
  String scenario() {
    return "control";
  }

  @Override
  int numTables() {
    return 60;
  }

  @Override
  String commitThreads() {
    return "60";
  }

  @Override
  long maxObserveSeconds() {
    return 150;
  }

  @Test
  void restartDuringCommitWithoutQueuedTables() throws Exception {
    runScenario();
  }
}
EOF
}

# -------------------------------------------------------------------------------------------------
# 3. Build / run / summarize
# -------------------------------------------------------------------------------------------------
gradle_run() { # $1 = log name, rest = gradle args
  local name="$1"; shift
  local log="$OUT/repro/CR-6.$name.log"
  (cd "$PRISTINE" && timeout "${CR6_STEP_TIMEOUT:-560}" ./gradlew "$@" --console=plain \
      > "$log" 2>&1)
  local rc=$?
  [ "$rc" -eq 124 ] && echo "    TIMEOUT after ${CR6_STEP_TIMEOUT:-560}s (see $log)"
  return $rc
}

run_scenario() { # $1 = short name, $2 = test class
  local name="$1" cls="$2"
  echo "==> Scenario $name ($cls)"
  gradle_run "$name" "$GP:cr6ReproTest" --offline --tests "org.apache.iceberg.connect.cr6.$cls"
  echo "    gradle exit code: $?"
  grep -E "^[[:space:]]*CR6 |CR6-SUMMARY|PASSED|FAILED|Exception|AssertionError" "$OUT/repro/CR-6.$name.log" \
    | grep -v "^\s*at " | head -120
}

summary() {
  echo "==> Summary"
  local q d c
  q=$(grep -h "CR6-SUMMARY scenario=queued" "$OUT/repro/CR-6.queued.log" 2>/dev/null | tail -1)
  d=$(grep -h "CR6-SUMMARY scenario=default" "$OUT/repro/CR-6.default.log" 2>/dev/null | tail -1)
  c=$(grep -h "CR6-SUMMARY scenario=control" "$OUT/repro/CR-6.control.log" 2>/dev/null | tail -1)
  for s in "$q" "$d" "$c"; do [ -n "$s" ] && echo "$s"; done
  if echo "$q" | grep -q "verdict=COORDINATOR_THREAD_HUNG" \
     && echo "$c" | grep -q "verdict=COORDINATOR_THREAD_EXITED"; then
    echo "RESULT: BUG REPRODUCED - with queued table commits the terminated coordinator thread stays"
    echo "        blocked in Tasks.waitFor (never reaches coordinator.stop()); the control without"
    echo "        queued commits exits normally."
    return 0
  fi
  echo "RESULT: bug NOT reproduced by this run (see logs)"
  return 1
}

prepare_tree
status=0
for s in $SCENARIOS; do
  case "$s" in
    build)
      echo "==> Building pristine sink + reproduction sources"
      if gradle_run build "$GP:compileCr6ReproJava" --offline; then
        echo "    build OK"
      else
        echo "    build FAILED"; tail -40 "$OUT/repro/CR-6.build.log"; exit 1
      fi ;;
    queued)  run_scenario queued Cr6QueuedCommitsTest ;;
    default) run_scenario default Cr6DefaultThreadsTest ;;
    control) run_scenario control Cr6ControlTest ;;
    summary) summary || status=1 ;;
    *) echo "unknown scenario $s"; status=1 ;;
  esac
done
exit $status
