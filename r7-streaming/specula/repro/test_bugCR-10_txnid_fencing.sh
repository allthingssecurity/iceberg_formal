#!/usr/bin/env bash
# =================================================================================================
# CR-10 reproduction: "Transactional ids are not stable across task reconfiguration or versions"
#
# System under test: PRISTINE apache/iceberg HEAD (git archive of the CR-10 worktree's HEAD; the
# Specula TLA+ instrumentation in the worktree is NOT used). Real in-JVM Kafka 3.9.2 KRaft broker +
# a real Kafka Connect 3.9.2 cluster (EmbeddedConnectCluster, the pipeline harness recipe) running
# the real IcebergSinkConnector / IcebergSinkTask / CommitterImpl / Worker / Coordinator.
#
# Scenarios (one JVM each):
#   ZombieWorkerEvicted     - the finding's worker claim, end to end. Task k's thread stalls inside
#                             Channel.send() (after beginTransaction, on its DataWritten send) like a
#                             GC pause / a send() blocked on metadata. While it is stalled the
#                             connector is reconfigured (config update => herder re-runs
#                             IcebergSinkConnector.taskConfigs() => new random suffix => new worker
#                             transactional.ids). The stalled old-generation task misses its
#                             graceful-stop deadline and is abandoned (a zombie). Its consumer is
#                             evicted (max.poll.interval.ms) before it resumes.
#                             => does the zombie's DataWritten land (duplicates), or is it fenced, and by what?
#   ZombieWorkerNotEvicted  - same, but the zombie resumes BEFORE its consumer is evicted (it is still
#                             a member of the current group generation when it commits).
#   ZombieWorkerNoReconfig  - control: same stall, NO reconfiguration; the zombie's partition moves to
#                             the OTHER task index of the same generation (so even a per-task
#                             stable transactional.id could not fence this zombie).
#   CoordinatorAcrossVersions - the finding's coordinator claim. The live current-version coordinator
#                             (transactional.id <connectGroupId>-coordinator, #18039) vs the
#                             transactional.id a 1.12.0 coordinator of the same task generation uses
#                             (prefix + "coordinator" + suffix; 1.12.0 Channel.java:69): does either
#                             fence the other? Includes same-id positive controls.
#
# Escalation level: Level 0 for the id mechanism (real connector API, real Connect REST, broker
# state); Level 1 for the zombie: a Kafka ProducerInterceptor (a client plugin configured through
# iceberg.kafka.interceptor.classes) that only DELAYS one send() of one worker. No sink logic is
# changed, no state is injected. The CoordinatorAcrossVersions scenario additionally uses a plain
# KafkaProducer carrying the 1.12.0 coordinator's transactional.id: that is the real request
# sequence a 1.12.0 coordinator issues (KafkaClientFactory.createProducer(id) + initTransactions()).
#
# Exit codes: 0 = all scenarios ran and the harm was NOT triggered (each scenario prints which
#             safeguard fired); 1 = harm triggered (duplicates/loss or an unfenced zombie DataWritten
#             visible); 2 = infrastructure/build failure or an inconclusive run.
# =================================================================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"                     # .specula-output/repro
OUTPUT_DIR="$(cd "$HERE/.." && pwd)"                       # .specula-output
WORK="$OUTPUT_DIR/confirmation/CR-10"
WORKTREE="${CR10_WORKTREE:-$WORK/worktree}"
PRISTINE="${CR10_PRISTINE:-$WORK/pristine-src}"
SOURCE_DIR="${CR10_SOURCE:-$OUTPUT_DIR/../source}"
REPORT_DIR="$HERE/CR-10.out"
SCENARIOS="${SCENARIOS-ZombieWorkerEvicted ZombieWorkerNotEvicted ZombieWorkerNoReconfig CoordinatorAcrossVersions}"
GRADLE_PROJECT=":iceberg-kafka-connect:iceberg-kafka-connect"
mkdir -p "$REPORT_DIR"

say() { echo "[test_bugCR-10] $*"; }

# ---------------------------------------------------------------------------------------------
# 1. Pristine export of HEAD (no instrumentation)
# ---------------------------------------------------------------------------------------------
HEAD_SHA="$(git -C "$WORKTREE" rev-parse HEAD)" || { say "cannot read worktree HEAD"; exit 2; }
if [ ! -f "$PRISTINE/.cr10-pristine-$HEAD_SHA" ]; then
  say "exporting pristine HEAD $HEAD_SHA -> $PRISTINE"
  rm -rf "$PRISTINE" && mkdir -p "$PRISTINE"
  git -C "$WORKTREE" archive HEAD | tar -x -C "$PRISTINE" || { say "git archive failed"; exit 2; }
  touch "$PRISTINE/.cr10-pristine-$HEAD_SHA"
fi
if [ ! -f "$PRISTINE/gradle/wrapper/gradle-wrapper.jar" ] && [ -f "$SOURCE_DIR/gradle/wrapper/gradle-wrapper.jar" ]; then
  cp "$SOURCE_DIR/gradle/wrapper/gradle-wrapper.jar" "$PRISTINE/gradle/wrapper/gradle-wrapper.jar"
fi
# prove the system under test is unmodified: every main source of the sink equals git HEAD
MAIN_REL="kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect"
modified=0
while IFS= read -r f; do
  if ! git -C "$WORKTREE" show "HEAD:$f" | cmp -s - "$PRISTINE/$f"; then
    say "NOT PRISTINE: $f"; modified=1
  fi
done < <(git -C "$WORKTREE" ls-tree -r --name-only HEAD "$MAIN_REL")
extra="$(cd "$PRISTINE" && find "$MAIN_REL" -type f | sort | comm -23 - <(git -C "$WORKTREE" ls-tree -r --name-only HEAD "$MAIN_REL" | sort))"
[ -n "$extra" ] && { say "NOT PRISTINE (untracked main files): $extra"; modified=1; }
[ "$modified" = 0 ] || exit 2
say "sink main sources are byte-identical to git HEAD $HEAD_SHA ($(git -C "$WORKTREE" ls-tree -r --name-only HEAD "$MAIN_REL" | wc -l | tr -d ' ') files)"

# ---------------------------------------------------------------------------------------------
# 2. Test-only source set (same dependency recipe as .specula-output/harness: Kafka 3.9.2 test jars)
# ---------------------------------------------------------------------------------------------
BUILD_GRADLE="$PRISTINE/kafka-connect/build.gradle"
git -C "$WORKTREE" show HEAD:kafka-connect/build.gradle > "$BUILD_GRADLE"
cat >> "$BUILD_GRADLE" <<'GRADLE'

// ---- CR-10 reproduction source set (test-only; main sources untouched) ----
project(':iceberg-kafka-connect:iceberg-kafka-connect') {
  sourceSets {
    cr10Repro {
      compileClasspath += sourceSets.main.output
      runtimeClasspath += sourceSets.main.output
    }
  }
  configurations {
    cr10ReproImplementation.extendsFrom testImplementation
    cr10ReproRuntimeOnly.extendsFrom testRuntimeOnly
  }
  dependencies {
    def kafkaVersion = libs.versions.kafka.get()
    cr10ReproImplementation libs.kafka.clients
    cr10ReproImplementation libs.kafka.connect.api
    cr10ReproImplementation libs.kafka.connect.json
    cr10ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}"
    cr10ReproImplementation "org.apache.kafka:connect-runtime:${kafkaVersion}:test"
    cr10ReproImplementation "org.apache.kafka:kafka-clients:${kafkaVersion}:test"
    cr10ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}"
    cr10ReproImplementation "org.apache.kafka:kafka_2.13:${kafkaVersion}:test"
    cr10ReproImplementation "org.apache.kafka:kafka-server-common:${kafkaVersion}:test"
  }
  pluginManager.withPlugin('net.ltgt.errorprone') {
    tasks.matching { it.name == 'compileCr10ReproJava' }.configureEach {
      options.errorprone.enabled = false
    }
  }
  task cr10ReproTest(type: Test) {
    useJUnitPlatform()
    testClassesDirs = sourceSets.cr10Repro.output.classesDirs
    classpath = sourceSets.cr10Repro.runtimeClasspath
    forkEvery = 1
    maxParallelForks = 1
    maxHeapSize = "3g"
    jvmArgs += project.property('extraJvmArgs')
    def logFile = System.getProperty('cr10.log.file', "${buildDir}/cr10/sink.log")
    systemProperty 'cr10.report.dir', System.getProperty('cr10.report.dir', "${buildDir}/cr10")
    systemProperty 'cr10.log.file', logFile
    systemProperty 'org.slf4j.simpleLogger.logFile', logFile
    systemProperty 'org.slf4j.simpleLogger.defaultLogLevel', 'warn'
    systemProperty 'org.slf4j.simpleLogger.showDateTime', 'true'
    systemProperty 'org.slf4j.simpleLogger.dateTimeFormat', 'HH:mm:ss.SSS'
    systemProperty 'org.slf4j.simpleLogger.showThreadName', 'true'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.iceberg.connect', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.clients.producer.internals.TransactionManager', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.connect.runtime.WorkerSinkTask', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.connect.runtime.WorkerTask', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.connect.runtime.Worker', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.connect.runtime.distributed.DistributedHerder', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.clients.consumer.internals.ConsumerCoordinator', 'info'
    systemProperty 'org.slf4j.simpleLogger.log.org.apache.kafka.clients.consumer.internals.AbstractCoordinator', 'info'
    outputs.upToDateWhen { false }
    testLogging {
      events "started", "passed", "skipped", "failed"
      exceptionFormat "full"
      showStandardStreams = true
    }
  }
}
GRADLE

# ---------------------------------------------------------------------------------------------
# 3. Test sources
# ---------------------------------------------------------------------------------------------
SRC_DIR="$PRISTINE/kafka-connect/kafka-connect/src/cr10Repro/java/org/apache/iceberg/connect/cr10"
RES_DIR="$PRISTINE/kafka-connect/kafka-connect/src/cr10Repro/resources"
rm -rf "$PRISTINE/kafka-connect/kafka-connect/src/cr10Repro"
mkdir -p "$SRC_DIR" "$RES_DIR"

cat > "$RES_DIR/log4j.properties" <<'EOF'
log4j.rootLogger=WARN, stderr
log4j.appender.stderr=org.apache.log4j.ConsoleAppender
log4j.appender.stderr.Target=System.err
log4j.appender.stderr.layout=org.apache.log4j.PatternLayout
log4j.appender.stderr.layout.ConversionPattern=%d{HH:mm:ss.SSS} %-5p [%t] %c{1}: %m%n
EOF

cat > "$SRC_DIR/SharedCatalog.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import java.util.Map;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

/**
 * One in-memory Iceberg catalog shared by every sink task of the JVM: each task instantiates its own
 * catalog through iceberg.catalog.catalog-impl, and all of them operate on the same tables (the
 * table operations of the shared root). Plain InMemoryCatalog semantics (CAS commit).
 */
public class SharedCatalog extends InMemoryCatalog {
  private static SharedCatalog root;

  public static synchronized SharedCatalog root() {
    if (root == null) {
      SharedCatalog catalog = new SharedCatalog();
      catalog.initialize("iceberg", Map.of());
      root = catalog;
    }
    return root;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    SharedCatalog shared = root();
    return shared == this ? super.newTableOps(tableIdentifier) : shared.newTableOps(tableIdentifier);
  }
}
EOF

cat > "$SRC_DIR/StallInterceptor.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Escalation Level 1 (timing assistance only). A Kafka client interceptor, configured through
 * iceberg.kafka.interceptor.classes, that DELAYS exactly one producer.send(): the first DataWritten
 * sent by the producer whose transactional.id equals the armed id. That send runs inside
 * Channel.send() after producer.beginTransaction() and before sendOffsetsToTransaction(), so the
 * task thread stalls there, as during a long GC pause or a send() blocked on metadata
 * (max.block.ms). Records are not modified, nothing is injected, no sink logic is changed. The
 * iceberg.kafka.* properties are shared by the sink's producers, consumers and admin clients, so the
 * class also implements the (pass-through) consumer interceptor interface.
 */
public class StallInterceptor
    implements ProducerInterceptor<String, byte[]>, ConsumerInterceptor<String, byte[]> {
  private static final AtomicReference<String> TARGET = new AtomicReference<>();
  private static volatile long stallMs = 0L;
  private static volatile CountDownLatch stalled = new CountDownLatch(1);
  private static volatile CountDownLatch resumed = new CountDownLatch(1);
  private static volatile String stalledThread;
  private static volatile long stalledAtMs;
  private static volatile long resumedAtMs;

  private volatile String transactionalId;

  public static void arm(String targetTransactionalId, long millis) {
    stallMs = millis;
    stalled = new CountDownLatch(1);
    resumed = new CountDownLatch(1);
    stalledThread = null;
    TARGET.set(targetTransactionalId);
  }

  public static CountDownLatch stalled() {
    return stalled;
  }

  public static CountDownLatch resumed() {
    return resumed;
  }

  public static String stalledThread() {
    return stalledThread;
  }

  public static long stalledAtMs() {
    return stalledAtMs;
  }

  public static long resumedAtMs() {
    return resumedAtMs;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Object id = configs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    this.transactionalId = id == null ? null : String.valueOf(id);
  }

  @Override
  public ProducerRecord<String, byte[]> onSend(ProducerRecord<String, byte[]> record) {
    String target = TARGET.get();
    if (target != null
        && target.equals(transactionalId)
        && isDataWritten(record)
        && TARGET.compareAndSet(target, null)) {
      CountDownLatch done = resumed;
      stalledThread = Thread.currentThread().getName();
      stalledAtMs = System.currentTimeMillis();
      stalled.countDown();
      long deadline = stalledAtMs + stallMs;
      boolean interrupted = false;
      for (long left = stallMs; left > 0; left = deadline - System.currentTimeMillis()) {
        try {
          Thread.sleep(left);
        } catch (InterruptedException e) {
          interrupted = true; // a paused thread does not observe interrupts; keep stalling
        }
      }
      resumedAtMs = System.currentTimeMillis();
      done.countDown();
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return record;
  }

  private static boolean isDataWritten(ProducerRecord<String, byte[]> record) {
    try {
      return record.value() != null
          && AvroUtil.decode(record.value()).type() == PayloadType.DATA_WRITTEN;
    } catch (RuntimeException e) {
      return false;
    }
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

  @Override
  public ConsumerRecords<String, byte[]> onConsume(ConsumerRecords<String, byte[]> records) {
    return records;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {}

  @Override
  public void close() {}
}
EOF

cat > "$SRC_DIR/Cr10Base.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.IcebergSinkConnector;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
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
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;

abstract class Cr10Base {
  static final String SRC = "src";
  static final String CTL = "control-iceberg";
  static final String SCRATCH = "cr10-scratch";
  static final String CONNECTOR = "cr10";
  static final String SINK_GROUP = "connect-" + CONNECTOR;
  static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "payload", Types.StringType.get()));

  protected EmbeddedConnectCluster connect;
  protected KafkaProducer<String, String> producer;
  protected Admin admin;
  protected String table;
  protected final Map<Long, Integer> producedIds = new TreeMap<>();
  private long nextId = 0L;
  private final List<String> report = new ArrayList<>();

  protected abstract String scenario();

  // ---------------------------------------------------------------------------------------------
  // reporting

  protected void note(String format, Object... args) {
    String line = String.format(Locale.ROOT, format, args);
    report.add(line);
    System.out.println("[CR10][" + scenario() + "] " + line);
  }

  protected void writeReport() throws IOException {
    Path dir = Paths.get(System.getProperty("cr10.report.dir", "build/cr10"));
    Files.createDirectories(dir);
    Files.write(dir.resolve(scenario() + ".report.txt"), report, StandardCharsets.UTF_8);
  }

  protected List<String> sinkLogLines(java.util.function.Predicate<String> filter) {
    String file = System.getProperty("cr10.log.file");
    if (file == null || !Files.exists(Paths.get(file))) {
      return List.of();
    }
    try {
      return Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8).stream()
          .filter(filter)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // cluster

  protected void startCluster(String tableName) throws Exception {
    this.table = tableName;
    SharedCatalog root = SharedCatalog.root();
    if (!root.namespaceExists(Namespace.of("db"))) {
      root.createNamespace(Namespace.of("db"));
    }
    root.createTable(
        TableIdentifier.parse(tableName),
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of("format-version", "2"));

    Properties brokerProps = new Properties();
    brokerProps.put("transaction.state.log.replication.factor", "1");
    brokerProps.put("transaction.state.log.min.isr", "1");
    brokerProps.put("transaction.state.log.num.partitions", "4");
    brokerProps.put("offsets.topic.num.partitions", "4");
    brokerProps.put("group.initial.rebalance.delay.ms", "0");

    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("connector.client.config.override.policy", "All");
    // idle tasks call put([]) -> Worker.process() every ~500 ms
    workerProps.put("offset.flush.interval.ms", "500");
    workerProps.put("scheduled.rebalance.max.delay.ms", "1000");
    // a task that does not stop within 2 s is cancelled and abandoned by the herder
    workerProps.put("task.shutdown.graceful.timeout.ms", "2000");

    connect =
        new EmbeddedConnectCluster.Builder()
            .name("cr10-" + scenario())
            .numWorkers(1)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(1, "Connect worker did not start");
    connect.kafka().createTopic(SRC, 2);
    connect.kafka().createTopic(CTL, 1);
    connect.kafka().createTopic(SCRATCH, 1);

    admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers()));
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    warmUpTransactions();
  }

  /** The first InitProducerId of a fresh broker creates __transaction_state; do it up front. */
  private void warmUpTransactions() {
    for (int i = 0; i < 4; i += 1) {
      try (KafkaProducer<String, String> warm = transactionalProducer("cr10-warmup-" + i)) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  protected KafkaProducer<String, String> transactionalProducer(String transactionalId) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
  }

  @AfterEach
  void tearDown() {
    try {
      writeReport();
    } catch (Exception e) {
      System.out.println("[CR10] could not write report: " + e);
    }
    try {
      if (producer != null) {
        producer.close(Duration.ofSeconds(5));
      }
      if (admin != null) {
        admin.close(Duration.ofSeconds(5));
      }
      if (connect != null) {
        connect.stop();
      }
    } catch (Exception e) {
      System.out.println("[CR10] error stopping the embedded cluster: " + e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // connector

  protected Map<String, String> connectorConfig(Map<String, String> overrides, String intervalMs) {
    Map<String, String> config = new HashMap<>();
    config.put("connector.class", IcebergSinkConnector.class.getName());
    config.put("tasks.max", "2");
    config.put("topics", SRC);
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("value.converter.schemas.enable", "false");
    config.put("iceberg.tables", table);
    config.put("iceberg.catalog.catalog-impl", SharedCatalog.class.getName());
    config.put("iceberg.control.topic", CTL);
    config.put("iceberg.control.commit.interval-ms", intervalMs);
    config.put("iceberg.control.commit.timeout-ms", "15000");
    config.put("iceberg.kafka.bootstrap.servers", connect.kafka().bootstrapServers());
    config.putAll(overrides);
    return config;
  }

  protected void configure(Map<String, String> overrides, String intervalMs) {
    connect.configureConnector(CONNECTOR, connectorConfig(overrides, intervalMs));
  }

  /** The task configs the herder stored = the output of IcebergSinkConnector.taskConfigs(). */
  protected Map<Integer, IcebergSinkConfig> taskConfigs() {
    Map<Integer, IcebergSinkConfig> result = new TreeMap<>();
    for (TaskInfo info : connect.taskConfigs(CONNECTOR)) {
      result.put(info.id().task(), new IcebergSinkConfig(info.config()));
    }
    return result;
  }

  /** Worker.java:53 */
  static String workerTxnId(IcebergSinkConfig config) {
    return config.transactionalPrefix() + "worker" + config.transactionalSuffix();
  }

  /** apache-iceberg-1.12.0 Channel.java:69 with name = "coordinator" (Coordinator.java:97). */
  static String coordinatorTxnIdV112(IcebergSinkConfig config) {
    return config.transactionalPrefix() + "coordinator" + config.transactionalSuffix();
  }

  protected String describeIds(Map<Integer, IcebergSinkConfig> configs) {
    return configs.entrySet().stream()
        .map(
            e ->
                "task "
                    + e.getKey()
                    + ": suffix="
                    + e.getValue().transactionalSuffix()
                    + " worker.txn.id="
                    + workerTxnId(e.getValue())
                    + " coordinator.txn.id="
                    + e.getValue().coordinatorTransactionalId())
        .collect(Collectors.joining("; "));
  }

  protected Map<Integer, IcebergSinkConfig> awaitNewGeneration(
      Map<Integer, IcebergSinkConfig> previous, long timeoutMs) throws InterruptedException {
    Set<String> oldSuffixes =
        previous.values().stream()
            .map(IcebergSinkConfig::transactionalSuffix)
            .collect(Collectors.toSet());
    Map<Integer, IcebergSinkConfig>[] holder = new Map[1];
    boolean ok =
        waitFor(
            () -> {
              try {
                Map<Integer, IcebergSinkConfig> current = taskConfigs();
                if (current.size() == previous.size()
                    && current.values().stream()
                        .noneMatch(c -> oldSuffixes.contains(c.transactionalSuffix()))) {
                  holder[0] = current;
                  return true;
                }
              } catch (RuntimeException e) {
                // REST 409 during rebalance: retry
              }
              return false;
            },
            timeoutMs,
            500);
    if (!ok) {
      throw new AssertionError("task configs were not regenerated within " + timeoutMs + " ms");
    }
    return holder[0];
  }

  // ---------------------------------------------------------------------------------------------
  // workload

  protected List<Long> produce(int partition, int count, String payload) throws Exception {
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < count; i += 1) {
      long id = nextId++;
      String value = String.format(Locale.ROOT, "{\"id\":%d,\"payload\":\"%s\"}", id, payload);
      producer.send(new ProducerRecord<>(SRC, partition, "k" + id, value)).get(30, TimeUnit.SECONDS);
      producedIds.put(id, partition);
      ids.add(id);
    }
    return ids;
  }

  protected void produceToAll(int perPartition, String payload) throws Exception {
    for (int i = 0; i < perPartition; i += 1) {
      produce(0, 1, payload);
      produce(1, 1, payload);
    }
  }

  protected static boolean waitFor(BooleanSupplier condition, long timeoutMs, long pollMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      Thread.sleep(pollMs);
    }
    return condition.getAsBoolean();
  }

  protected boolean awaitAllInTable(long timeoutMs) throws InterruptedException {
    return waitFor(() -> tableIdCounts().keySet().containsAll(producedIds.keySet()), timeoutMs, 1000);
  }

  // ---------------------------------------------------------------------------------------------
  // observations (all read-only, through public client / catalog APIs)

  protected Map<Long, Integer> tableIdCounts() {
    Table t = SharedCatalog.root().loadTable(TableIdentifier.parse(table));
    Map<Long, Integer> counts = new TreeMap<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(t).build()) {
      for (Record record : records) {
        counts.merge((Long) record.getField("id"), 1, Integer::sum);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return counts;
  }

  protected int snapshotCount() {
    Table t = SharedCatalog.root().loadTable(TableIdentifier.parse(table));
    int count = 0;
    for (Snapshot ignored : t.snapshots()) {
      count += 1;
    }
    return count;
  }

  protected Map<String, TransactionDescription> sinkTransactions() throws Exception {
    Collection<TransactionListing> listings =
        admin.listTransactions().all().get(30, TimeUnit.SECONDS);
    List<String> ids =
        listings.stream()
            .map(TransactionListing::transactionalId)
            .filter(id -> !id.startsWith("cr10-warmup"))
            .sorted()
            .collect(Collectors.toList());
    Map<String, TransactionDescription> result = new TreeMap<>();
    if (!ids.isEmpty()) {
      result.putAll(admin.describeTransactions(ids).all().get(30, TimeUnit.SECONDS));
    }
    return result;
  }

  static String txn(TransactionDescription d) {
    return d == null
        ? "<absent>"
        : "producerId=" + d.producerId() + ",epoch=" + d.producerEpoch() + ",state=" + d.state();
  }

  protected String txns(Map<String, TransactionDescription> txns) {
    return txns.entrySet().stream()
        .map(e -> e.getKey() + "{" + txn(e.getValue()) + "}")
        .collect(Collectors.joining(" "));
  }

  protected Map<String, Set<String>> sinkAssignment() throws Exception {
    ConsumerGroupDescription desc =
        admin
            .describeConsumerGroups(List.of(SINK_GROUP))
            .describedGroups()
            .get(SINK_GROUP)
            .get(30, TimeUnit.SECONDS);
    Map<String, Set<String>> result = new TreeMap<>();
    for (MemberDescription member : desc.members()) {
      result.put(
          member.clientId() + "/" + member.consumerId(),
          member.assignment().topicPartitions().stream()
              .map(TopicPartition::toString)
              .collect(Collectors.toCollection(TreeSet::new)));
    }
    return result;
  }

  /** "clientId/memberId" of the sink-group member owning a source partition (null if none). */
  protected String memberOwning(String partition) throws Exception {
    for (Map.Entry<String, Set<String>> e : sinkAssignment().entrySet()) {
      if (e.getValue().contains(partition)) {
        return e.getKey();
      }
    }
    return null;
  }

  /** Task index owning a source partition, from the sink consumer group (client.id ...-<task>). */
  protected int taskOwning(String partition) throws Exception {
    long deadline = System.currentTimeMillis() + 60_000L;
    while (System.currentTimeMillis() < deadline) {
      for (Map.Entry<String, Set<String>> e : sinkAssignment().entrySet()) {
        if (e.getValue().contains(partition)) {
          String clientId = e.getKey().substring(0, e.getKey().indexOf('/'));
          return Integer.parseInt(clientId.substring(clientId.lastIndexOf('-') + 1));
        }
      }
      Thread.sleep(500);
    }
    throw new AssertionError("no member owns " + partition);
  }

  protected Map<String, Long> committedSourceOffsets() throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin
            .listConsumerGroupOffsets(
                SINK_GROUP, new ListConsumerGroupOffsetsOptions().requireStable(true))
            .partitionsToOffsetAndMetadata()
            .get(30, TimeUnit.SECONDS);
    Map<String, Long> result = new TreeMap<>();
    offsets.forEach(
        (tp, om) -> {
          if (om != null && SRC.equals(tp.topic())) {
            result.put(tp.toString(), om.offset());
          }
        });
    return result;
  }

  protected Map<String, Long> endOffsets() throws Exception {
    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
    request.put(new TopicPartition(SRC, 0), OffsetSpec.latest());
    request.put(new TopicPartition(SRC, 1), OffsetSpec.latest());
    Map<String, Long> result = new TreeMap<>();
    admin
        .listOffsets(request)
        .all()
        .get(30, TimeUnit.SECONDS)
        .forEach((tp, info) -> result.put(tp.toString(), info.offset()));
    return result;
  }

  /** Control topic as a consumer with the given isolation level sees it: offset -> event. */
  protected Map<Long, String> controlTopic(String isolation) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    Map<Long, String> result = new TreeMap<>();
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
      TopicPartition tp = new TopicPartition(CTL, 0);
      consumer.assign(List.of(tp));
      consumer.seekToBeginning(List.of(tp));
      long end = consumer.endOffsets(List.of(tp)).get(tp);
      long deadline = System.currentTimeMillis() + 30_000L;
      while (consumer.position(tp) < end && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(500))) {
          result.put(record.offset(), describe(record));
        }
      }
    }
    return result;
  }

  private static String describe(ConsumerRecord<String, byte[]> record) {
    Event event = AvroUtil.decode(record.value());
    StringBuilder sb = new StringBuilder(event.type().name());
    switch (event.type()) {
      case START_COMMIT:
        sb.append(" commit=").append(((StartCommit) event.payload()).commitId());
        break;
      case DATA_WRITTEN:
        DataWritten written = (DataWritten) event.payload();
        sb.append(" commit=")
            .append(written.commitId())
            .append(" dataFiles=")
            .append(written.dataFiles() == null ? 0 : written.dataFiles().size())
            .append(" rows=")
            .append(
                written.dataFiles() == null
                    ? 0
                    : written.dataFiles().stream().mapToLong(f -> f.recordCount()).sum());
        break;
      case DATA_COMPLETE:
        DataComplete complete = (DataComplete) event.payload();
        sb.append(" commit=")
            .append(complete.commitId())
            .append(" assignments=")
            .append(
                complete.assignments().stream()
                    .map(a -> a.topic() + "-" + a.partition() + "@" + a.offset())
                    .collect(Collectors.joining(",")));
        break;
      default:
        break;
    }
    sb.append(" key=").append(record.key());
    return sb.toString();
  }

  // ---------------------------------------------------------------------------------------------
  // exactly-once oracle

  /** Returns true when the table holds every produced id exactly once. */
  protected boolean exactlyOnceOracle() throws Exception {
    Map<Long, Integer> counts = tableIdCounts();
    List<Long> duplicates =
        counts.entrySet().stream()
            .filter(e -> e.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    List<Long> missing =
        producedIds.keySet().stream().filter(id -> !counts.containsKey(id)).collect(Collectors.toList());
    long rows = counts.values().stream().mapToLong(Integer::longValue).sum();
    note(
        "TABLE %s: produced=%d distinct-in-table=%d rows=%d duplicates=%s missing=%s snapshots=%d",
        table,
        producedIds.size(),
        counts.size(),
        rows,
        duplicates,
        missing,
        snapshotCount());
    note(
        "SOURCE OFFSETS: committed(%s)=%s end=%s",
        SINK_GROUP,
        committedSourceOffsets(),
        endOffsets());
    return duplicates.isEmpty() && missing.isEmpty();
  }
}
EOF

cat > "$SRC_DIR/ZombieScenario.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;

/** Zombie old-generation worker (finding CR-10, worker half). */
abstract class ZombieScenario extends Cr10Base {

  protected void runZombieScenario(long maxPollIntervalMs, long stallMs, boolean reconfigure)
      throws Exception {
    startCluster("db.t_" + scenario().toLowerCase(Locale.ROOT));
    Map<String, String> overrides = new HashMap<>();
    overrides.put("consumer.override.max.poll.interval.ms", String.valueOf(maxPollIntervalMs));
    overrides.put("consumer.override.session.timeout.ms", "10000");
    overrides.put("consumer.override.heartbeat.interval.ms", "1000");
    overrides.put("iceberg.kafka.interceptor.classes", StallInterceptor.class.getName());
    configure(overrides, "1000");
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 2, "connector did not start");
    note(
        "setup: 1 Connect worker, tasks.max=2, src has 2 partitions, max.poll.interval.ms=%d, "
            + "stall=%d ms, task.shutdown.graceful.timeout.ms=2000, reconfigure-during-stall=%s",
        maxPollIntervalMs,
        stallMs,
        reconfigure);

    // ---- generation 1: baseline exactly-once -------------------------------------------------
    Map<Integer, IcebergSinkConfig> gen1 = taskConfigs();
    note("GEN-1 (IcebergSinkConnector.taskConfigs() #1): %s", describeIds(gen1));
    produceToAll(20, "gen1");
    assertThat(awaitAllInTable(180_000)).as("baseline records committed").isTrue();
    int leader = taskOwning("src-0");
    int zombieTask = 1 - leader;
    String zombiePartition = "src-1";
    assertThat(taskOwning(zombiePartition)).isEqualTo(zombieTask);
    String zombieMember = memberOwning(zombiePartition);
    String zombieWorkerId = workerTxnId(gen1.get(zombieTask));
    String coordinatorId = gen1.get(leader).coordinatorTransactionalId();
    Map<String, TransactionDescription> txGen1 = sinkTransactions();
    note(
        "leader (coordinator) task=%d; zombie-to-be task=%d owns %s as sink-group member %s",
        leader,
        zombieTask,
        zombiePartition,
        zombieMember);
    note("BROKER transactional ids after GEN-1: %s", txns(txGen1));
    assertThat(txGen1).containsKey(zombieWorkerId).containsKey(coordinatorId);

    // the zombie-to-be Worker's control-topic record key (Channel producerId), from its GEN-1
    // DataComplete events (they report exactly its assignment {src-1})
    Map<Long, String> preStall = controlTopic("read_committed");
    long preStallEnd = preStall.isEmpty() ? -1L : ((TreeMap<Long, String>) preStall).lastKey();
    Set<String> zombieKeys =
        preStall.values().stream()
            .filter(
                e ->
                    e.startsWith("DATA_COMPLETE")
                        && e.contains(zombiePartition + "@")
                        && !e.contains("src-0@"))
            .map(e -> e.substring(e.indexOf(" key=") + 5))
            .collect(Collectors.toCollection(TreeSet::new));
    note(
        "zombie-to-be Worker control-topic record key(s) (Channel producerId): %s; control topic "
            + "end before the stall: %d",
        zombieKeys,
        preStallEnd);

    // ---- stall the zombie-to-be worker inside Channel.send() -------------------------------------
    StallInterceptor.arm(zombieWorkerId, stallMs);
    List<Long> zombieBatch = produce(1, 5, "zombie-batch");
    assertThat(StallInterceptor.stalled().await(60, TimeUnit.SECONDS))
        .as("worker reached its DataWritten send")
        .isTrue();
    note(
        "STALL: worker %s (thread %s) is inside Channel.send() after beginTransaction(), on the "
            + "DataWritten that carries ids %s; its sendOffsetsToTransaction() comes next",
        zombieWorkerId,
        StallInterceptor.stalledThread(),
        zombieBatch);

    Map<Integer, IcebergSinkConfig> gen2 = gen1;
    if (reconfigure) {
      // ---- task reconfiguration: config update => the herder calls taskConfigs() again ----------
      configure(overrides, "1001");
      note("RECONFIGURE: connector config updated (commit interval 1000 -> 1001 ms)");
      gen2 = awaitNewGeneration(gen1, 90_000);
      note("GEN-2 (IcebergSinkConnector.taskConfigs() #2): %s", describeIds(gen2));
      note(
          "same task index %d: GEN-1 worker id %s vs GEN-2 worker id %s -> equal=%s",
          zombieTask,
          zombieWorkerId,
          workerTxnId(gen2.get(zombieTask)),
          zombieWorkerId.equals(workerTxnId(gen2.get(zombieTask))));
      assertThat(gen2.get(zombieTask).coordinatorTransactionalId()).isEqualTo(coordinatorId);
    } else {
      note("NO RECONFIGURATION: same task generation; only the zombie's consumer is evicted");
    }

    // who owns the zombie's partition while the zombie is still stalled?
    final String[] newOwner = new String[1];
    boolean moved =
        waitFor(
            () -> {
              try {
                String owner = memberOwning(zombiePartition);
                if (owner != null && !owner.equals(zombieMember)) {
                  newOwner[0] = owner;
                  return true;
                }
              } catch (Exception e) {
                // group rebalancing
              }
              return false;
            },
            Math.max(1_000L, stallMs - 3_000L),
            250);
    long ownerSeenAt = System.currentTimeMillis();
    String newOwnerClient = newOwner[0] == null ? null : newOwner[0].substring(0, newOwner[0].indexOf('/'));
    Integer newOwnerTask =
        newOwnerClient == null
            ? null
            : Integer.parseInt(newOwnerClient.substring(newOwnerClient.lastIndexOf('-') + 1));
    String newOwnerWorkerId = newOwnerTask == null ? null : workerTxnId(gen2.get(newOwnerTask));
    note(
        "while the zombie is stalled (%d ms after stall start): %s reassigned=%s to member %s -> "
            + "its worker transactional.id %s (equal to the zombie's %s: %s)",
        ownerSeenAt - StallInterceptor.stalledAtMs(),
        zombiePartition,
        moved,
        newOwner[0],
        newOwnerWorkerId,
        zombieWorkerId,
        zombieWorkerId.equals(newOwnerWorkerId));

    // keep traffic flowing so the new owner creates its worker producer and commits
    produceToAll(10, "during-stall");

    // ---- the zombie resumes ----------------------------------------------------------------------
    assertThat(StallInterceptor.resumed().await(stallMs + 120_000, TimeUnit.MILLISECONDS))
        .as("zombie resumed")
        .isTrue();
    note(
        "RESUME: old worker %s resumed after %d ms and continues Channel.send(): send(DataWritten), "
            + "send(DataComplete), sendOffsetsToTransaction(offsets, its consumer's group metadata), "
            + "commitTransaction()",
        zombieWorkerId,
        StallInterceptor.resumedAtMs() - StallInterceptor.stalledAtMs());
    Thread.sleep(5_000);
    note("BROKER transactional ids after the zombie's attempt: %s", txns(sinkTransactions()));

    produceToAll(10, "final");
    boolean complete = awaitAllInTable(180_000);
    Thread.sleep(8_000); // more commit cycles: a visible zombie DataWritten would be committed now
    note("all produced ids reached the table: %s", complete);

    // ---- oracle ------------------------------------------------------------------------------------
    boolean exactlyOnce = exactlyOnceOracle();
    Map<String, TransactionDescription> txEnd = sinkTransactions();
    TransactionDescription zombieG1 = txGen1.get(zombieWorkerId);
    TransactionDescription zombieEnd = txEnd.get(zombieWorkerId);
    TransactionDescription coordG1 = txGen1.get(coordinatorId);
    TransactionDescription coordEnd = txEnd.get(coordinatorId);
    boolean zombieIdUntouched =
        zombieG1 != null
            && zombieEnd != null
            && zombieG1.producerId() == zombieEnd.producerId()
            && zombieG1.producerEpoch() == zombieEnd.producerEpoch();
    note(
        "FENCING-BY-ID worker: zombie id %s GEN-1{%s} END{%s} -> producerId/epoch untouched by "
            + "every later initTransactions(): %s",
        zombieWorkerId,
        txn(zombieG1),
        txn(zombieEnd),
        zombieIdUntouched);
    note(
        "FENCING-BY-ID coordinator (positive control, stable id %s): GEN-1{%s} END{%s} -> same "
            + "producerId with bumped epoch: %s",
        coordinatorId,
        txn(coordG1),
        txn(coordEnd),
        coordG1 != null
            && coordEnd != null
            && coordG1.producerId() == coordEnd.producerId()
            && coordEnd.producerEpoch() > coordG1.producerEpoch());

    Map<Long, String> committedView = controlTopic("read_committed");
    Map<Long, String> uncommittedView = controlTopic("read_uncommitted");
    long committedMax =
        committedView.isEmpty() ? -1L : ((TreeMap<Long, String>) committedView).lastKey();
    Map<Long, String> aborted = new TreeMap<>(uncommittedView);
    aborted.keySet().removeAll(committedView.keySet());
    aborted.keySet().removeIf(offset -> offset > committedMax);
    List<Long> zombieWrittenVisible =
        committedView.entrySet().stream()
            .filter(e -> e.getKey() > preStallEnd && isZombieDataWritten(e.getValue(), zombieKeys))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    List<Long> zombieWrittenAborted =
        aborted.entrySet().stream()
            .filter(e -> isZombieDataWritten(e.getValue(), zombieKeys))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    note(
        "CONTROL TOPIC: read_committed=%d records, read_uncommitted=%d; aborted (written but never "
            + "visible)=%s",
        committedView.size(),
        uncommittedView.size(),
        aborted);
    note(
        "zombie Worker DataWritten after the stall: visible to read_committed consumers at offsets "
            + "%s; written-and-aborted at offsets %s",
        zombieWrittenVisible,
        zombieWrittenAborted);

    List<String> zombieErrors =
        sinkLogLines(
            line ->
                (line.contains(zombieWorkerId) || line.contains("task-thread-" + CONNECTOR))
                    && (line.contains("CommitFailedException")
                        || line.contains("consumer group metadata mismatch")
                        || line.contains("ProducerFenced")
                        || line.contains("InvalidProducerEpoch")
                        || line.contains("unrecoverable exception")
                        || line.contains("orphan task")
                        || line.contains("abortable error")));
    List<String> herder =
        sinkLogLines(l -> l.contains("Graceful stop of task") || l.contains("consumer poll timeout has expired"));
    note("SINK LOG (Connect runtime + zombie producer/task thread):");
    herder.stream().limit(4).forEach(line -> note("  | %s", shorten(line)));
    zombieErrors.stream().limit(8).forEach(line -> note("  | %s", shorten(line)));
    boolean groupFenced =
        zombieErrors.stream()
            .anyMatch(l -> l.contains(zombieWorkerId) && l.contains("consumer group metadata mismatch"));
    boolean idFenced =
        zombieErrors.stream()
            .anyMatch(
                l ->
                    l.contains(zombieWorkerId)
                        && (l.contains("ProducerFenced") || l.contains("InvalidProducerEpoch")));
    String zombieState = zombieEnd == null ? "?" : String.valueOf(zombieEnd.state());
    ConnectorStateInfo status = connect.connectorStatus(CONNECTOR);
    note(
        "connector status at the end: %s",
        status.tasks().stream()
            .map(t -> "task " + t.id() + "=" + t.state())
            .collect(Collectors.joining(", ")));

    String result;
    if (!exactlyOnce) {
      result = "BUG TRIGGERED: duplicates or loss in the table";
    } else if (groupFenced
        && !idFenced
        && "CompleteAbort".equals(zombieState)
        && zombieWrittenVisible.isEmpty()) {
      result =
          "HARM NOT TRIGGERED. The zombie was NOT fenced by transactional.id (producerId/epoch "
              + "untouched, no ProducerFencedException); the KIP-447 consumer-group check rejected "
              + "its TxnOffsetCommit (CommitFailedException: consumer group metadata mismatch), its "
              + "transaction aborted and its DataWritten never became visible; table exactly-once";
    } else if (!groupFenced
        && !idFenced
        && "CompleteCommit".equals(zombieState)
        && !zombieWrittenVisible.isEmpty()) {
      result =
          "HARM NOT TRIGGERED. The zombie resumed while still a member of the current group "
              + "generation; its DataWritten and source offsets committed atomically and the next "
              + "owner resumed after that committed offset; table exactly-once";
    } else {
      result = "INCONCLUSIVE (table exactly-once, zombie attempt not observed as expected)";
    }
    note("RESULT: %s", result);
    System.out.println(
        "[CR10][" + scenario() + "] RESULT-CODE " + (exactlyOnce ? "NO_HARM" : "HARM"));
    assertThat(exactlyOnce).as("table exactly-once").isTrue();
  }

  private static boolean isZombieDataWritten(String event, Set<String> zombieKeys) {
    return event.startsWith("DATA_WRITTEN")
        && zombieKeys.stream().anyMatch(k -> event.endsWith(" key=" + k));
  }

  private static String shorten(String line) {
    return line.length() > 420 ? line.substring(0, 420) + " ..." : line;
  }
}
EOF

cat > "$SRC_DIR/TestCr10ZombieWorkerEvicted.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import org.junit.jupiter.api.Test;

public class TestCr10ZombieWorkerEvicted extends ZombieScenario {
  @Override
  protected String scenario() {
    return "ZombieWorkerEvicted";
  }

  @Test
  public void zombieWorkerAcrossReconfigurationEvictedBeforeResume() throws Exception {
    // the zombie's consumer misses max.poll.interval.ms (8 s) long before it resumes (25 s)
    runZombieScenario(8_000L, 25_000L, true);
  }
}
EOF

cat > "$SRC_DIR/TestCr10ZombieWorkerNotEvicted.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import org.junit.jupiter.api.Test;

public class TestCr10ZombieWorkerNotEvicted extends ZombieScenario {
  @Override
  protected String scenario() {
    return "ZombieWorkerNotEvicted";
  }

  @Test
  public void zombieWorkerAcrossReconfigurationResumesBeforeEviction() throws Exception {
    // the zombie resumes (6 s) while its consumer is still a member of the current generation
    runZombieScenario(60_000L, 6_000L, true);
  }
}
EOF

cat > "$SRC_DIR/TestCr10ZombieWorkerNoReconfig.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import org.junit.jupiter.api.Test;

public class TestCr10ZombieWorkerNoReconfig extends ZombieScenario {
  @Override
  protected String scenario() {
    return "ZombieWorkerNoReconfig";
  }

  @Test
  public void zombieWorkerWithinOneGenerationEvictedBeforeResume() throws Exception {
    // control: no taskConfigs() regeneration; the zombie's partition moves to the other task index
    runZombieScenario(8_000L, 25_000L, false);
  }
}
EOF

cat > "$SRC_DIR/TestCr10CoordinatorAcrossVersions.java" <<'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iceberg.connect.cr10;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

/** Finding CR-10, coordinator half: current-version vs 1.12.0 coordinator transactional ids. */
public class TestCr10CoordinatorAcrossVersions extends Cr10Base {
  @Override
  protected String scenario() {
    return "CoordinatorAcrossVersions";
  }

  @Test
  public void coordinatorTransactionalIdNamespacesAcrossVersions() throws Exception {
    startCluster("db.t_coord");
    configure(Map.of(), "1000");
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 2, "connector did not start");
    produceToAll(10, "base");
    assertThat(awaitAllInTable(180_000)).isTrue();

    Map<Integer, IcebergSinkConfig> configs = taskConfigs();
    int leader = taskOwning("src-0");
    IcebergSinkConfig leaderConfig = configs.get(leader);
    String currentId = leaderConfig.coordinatorTransactionalId();
    String v112Id = coordinatorTxnIdV112(leaderConfig);
    note("task configs: %s", describeIds(configs));
    note(
        "leader task %d: current-version coordinator transactional.id=%s ; the id a 1.12.0 "
            + "coordinator elected on the same task of the same generation uses=%s ; equal=%s",
        leader,
        currentId,
        v112Id,
        currentId.equals(v112Id));
    Map<String, TransactionDescription> tx0 = sinkTransactions();
    note("BROKER: %s", txns(tx0));
    assertThat(tx0).containsKey(currentId);

    // (1) a 1.12.0 coordinator initializes its producer (KafkaClientFactory.createProducer(id))
    KafkaProducer<String, String> v112Coordinator = transactionalProducer(v112Id);
    v112Coordinator.initTransactions();
    note("1.12.0-format coordinator producer initialized: %s", txn(sinkTransactions().get(v112Id)));
    int snapshotsBefore = snapshotCount();
    produceToAll(5, "after-v112-init");
    assertThat(awaitAllInTable(120_000)).isTrue();
    Thread.sleep(3_000);
    TransactionDescription live1 = sinkTransactions().get(currentId);
    List<String> fencedLines =
        sinkLogLines(l -> l.contains("was fenced by a newer coordinator"));
    note(
        "(1) after the 1.12.0-format init, the live current-version coordinator %s kept committing "
            + "(snapshots %d -> %d) and is untouched: before{%s} after{%s}; 'fenced' log lines=%d",
        currentId,
        snapshotsBefore,
        snapshotCount(),
        txn(tx0.get(currentId)),
        txn(live1),
        fencedLines.size());

    // (2) a current-version coordinator (re-)election re-initializes the stable id ...
    connect.restartTask(CONNECTOR, leader);
    final long epochBefore = live1.producerEpoch();
    boolean bumped =
        waitFor(
            () -> {
              try {
                TransactionDescription d = sinkTransactions().get(currentId);
                return d != null && d.producerEpoch() > epochBefore;
              } catch (Exception e) {
                return false;
              }
            },
            90_000,
            500);
    TransactionDescription live2 = sinkTransactions().get(currentId);
    note(
        "(2) restarted leader task %d -> a new current-version Coordinator ran initTransactions() on "
            + "%s: {%s} (epoch bumped: %s; a stale current-version coordinator would now be fenced)",
        leader,
        currentId,
        txn(live2),
        bumped);
    // ... but the 1.12.0-format producer is in another namespace and can still commit
    String v112Outcome;
    try {
      v112Coordinator.beginTransaction();
      v112Coordinator
          .send(new ProducerRecord<>(SCRATCH, "v112-coordinator", "write after current init"))
          .get(30, TimeUnit.SECONDS);
      v112Coordinator.commitTransaction();
      v112Outcome = "COMMITTED (not fenced)";
    } catch (Exception e) {
      v112Outcome = "FAILED: " + e;
    }
    note(
        "    1.12.0-format coordinator producer %s transaction after the current-version init: %s",
        v112Id,
        v112Outcome);

    // (3) positive control: a second producer on the SAME 1.12.0-format id fences the first
    String control;
    try (KafkaProducer<String, String> v112Restarted = transactionalProducer(v112Id)) {
      v112Restarted.initTransactions();
      try {
        v112Coordinator.beginTransaction();
        v112Coordinator
            .send(new ProducerRecord<>(SCRATCH, "v112-coordinator", "zombie write"))
            .get(30, TimeUnit.SECONDS);
        v112Coordinator.commitTransaction();
        control = "NOT fenced";
      } catch (Exception e) {
        Throwable root = e;
        while (root.getCause() != null) {
          root = root.getCause();
        }
        control = "fenced: " + root.getClass().getSimpleName();
      }
    }
    note("(3) control: re-init of the SAME id %s -> the first producer is %s", v112Id, control);
    try {
      v112Coordinator.close();
    } catch (Exception ignored) {
      // fenced producer
    }

    produceToAll(5, "final");
    assertThat(awaitAllInTable(180_000)).isTrue();
    Thread.sleep(3_000);
    boolean exactlyOnce = exactlyOnceOracle();
    note(
        "RESULT: current-version id %s and 1.12.0 id %s are different transactional.ids -> no "
            + "fencing in either direction (1: live coordinator untouched by the 1.12.0 init; 2: the "
            + "1.12.0 producer still commits after the current-version init) while same-id re-init "
            + "fences (2: epoch bump %s; 3: %s). Table exactly-once in this run: %s",
        currentId,
        v112Id,
        bumped,
        control,
        exactlyOnce);
    assertThat(currentId).isNotEqualTo(v112Id);
    assertThat(exactlyOnce).isTrue();
  }
}
EOF

# ---------------------------------------------------------------------------------------------
# 4. Build and run each scenario in its own JVM / Gradle invocation
# ---------------------------------------------------------------------------------------------
cd "$PRISTINE" || exit 2
overall=0
for scenario in $SCENARIOS; do
  GLOG="$HERE/CR-10.$scenario.gradle.log"
  SLOG="$HERE/CR-10.$scenario.sink.log"
  rm -f "$SLOG"
  say "=== scenario $scenario (gradle log: $GLOG, sink log: $SLOG)"
  timeout 1500 ./gradlew --offline -DsparkVersions= -DflinkVersions= -DkafkaVersions=3 \
      "$GRADLE_PROJECT:cr10ReproTest" --tests "org.apache.iceberg.connect.cr10.TestCr10$scenario" \
      -Dcr10.log.file="$SLOG" -Dcr10.report.dir="$REPORT_DIR" --console=plain > "$GLOG" 2>&1
  rc=$?
  if [ "$rc" -eq 124 ]; then
    say "TIMEOUT in $scenario (possible deadlock: a finding, not retried)"; overall=2
  fi
  grep -E "^\s*\[CR10\]" "$GLOG" | sed 's/^\s*//'
  if grep -q "RESULT-CODE HARM" "$GLOG"; then
    say "$scenario: HARM TRIGGERED"; overall=1
  elif grep -q "RESULT-CODE NO_HARM" "$GLOG" || { [ "$rc" -eq 0 ] && grep -q "\[CR10\]\[$scenario\] RESULT:" "$GLOG"; }; then
    say "$scenario: harm not triggered (gradle rc=$rc)"
  else
    say "$scenario: no result (gradle rc=$rc); tail of gradle log:"; tail -40 "$GLOG"
    [ "$overall" = 1 ] || overall=2
  fi
done
[ -z "$SCENARIOS" ] && say "SCENARIOS empty: tree prepared only"
say "overall exit code: $overall (0 = harm not triggered in any scenario, 1 = harm, 2 = infra/inconclusive)"
exit "$overall"
