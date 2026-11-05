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
package org.apache.iceberg.connect.mc3;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConnector;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
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
import org.junit.jupiter.api.BeforeEach;

/**
 * MC-3 end-to-end reproduction on the PRISTINE sink (apache/iceberg@9fffeb6): one JVM with an
 * embedded Kafka 3.9.2 broker (KRaft, real transactions / read_committed / consumer groups), a
 * Kafka Connect worker running the real IcebergSinkConnector (1 task: worker + coordinator), a
 * 1-partition source topic, a 1-partition control topic and a shared in-memory Iceberg catalog.
 *
 * <p>Scenario (MC_hunt_s2_multitable variant of MC-3; same durable state as the CE crash):
 *
 * <ol>
 *   <li>batch b1 is written; the connector's table commit S1 lands but the catalog reports
 *       CommitStateUnknownException -> full commit rethrows -> coordinator terminates before
 *       commitConsumerOffsets() -> task FAILED (the -coord checkpoint stays behind S1).
 *   <li>maintenance while the task is down: a non-connector snapshot becomes the head of main and
 *       expire_snapshots removes S1 (variant-specific; the control variant does not expire).
 *   <li>operator restarts the FAILED task through the Connect REST API; batch b2 is written.
 *   <li>the restarted coordinator resumes the control topic at the stale -coord checkpoint,
 *       re-reads S1's DataWritten and commits again.
 * </ol>
 */
abstract class Mc3E2EBase {
  static final String CONNECTOR = "iceberg-sink";
  static final String CONNECT_GROUP = "connect-" + CONNECTOR;
  static final String COORD_GROUP = CONNECT_GROUP + "-coord";
  static final String SRC_TOPIC = "src";
  static final String CTL_TOPIC = "ctl";
  static final TableIdentifier TABLE = TableIdentifier.parse("db.tbl");
  static final String OFFSETS_PROP = "kafka.connect.offsets." + CTL_TOPIC + "." + CONNECT_GROUP;
  static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  static final int BATCH = 5;
  static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  protected EmbeddedConnectCluster connect;
  protected KafkaProducer<String, String> producer;
  protected PrintStream report;
  private long nextId = 0;
  private int produced = 0;

  /** Name of the variant (report file name). */
  abstract String variant();

  /** Table maintenance run while the failed task is down; returns a description. */
  abstract String maintenance(Table table) throws Exception;

  @BeforeEach
  void setUp() throws Exception {
    Path dir = Paths.get(System.getProperty("tla.trace.dir", "build/mc3"));
    Files.createDirectories(dir);
    report =
        new PrintStream(
            new TeeOutputStream(
                new FileOutputStream(dir.resolve("MC-3_" + variant() + ".txt").toFile())),
            true,
            StandardCharsets.UTF_8);

    Mc3Catalog catalog = Mc3Catalog.initRoot(Map.of());
    catalog.createNamespace(Namespace.of("db"));
    catalog.createTable(
        TABLE,
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
    workerProps.put("offset.flush.interval.ms", "500");
    workerProps.put("scheduled.rebalance.max.delay.ms", "1000");
    workerProps.put("task.shutdown.graceful.timeout.ms", "10000");

    connect =
        new EmbeddedConnectCluster.Builder()
            .name("mc3-" + variant())
            .numWorkers(1)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(1, "Connect worker did not start");
    connect.kafka().createTopic(SRC_TOPIC, 1);
    connect.kafka().createTopic(CTL_TOPIC, 1);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    warmUpTransactions();
  }

  @AfterEach
  void tearDown() {
    try {
      producer.close(Duration.ofSeconds(5));
      connect.stop();
    } catch (Exception e) {
      report.println("WARN error stopping the embedded cluster: " + e);
    } finally {
      report.close();
    }
  }

  /** The first InitProducerId of a fresh broker creates __transaction_state and is slow. */
  private void warmUpTransactions() {
    for (int i = 0; i < 4; i += 1) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "mc3-warmup-" + i);
      try (KafkaProducer<String, String> warm =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  Map<String, String> connectorConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("connector.class", IcebergSinkConnector.class.getName());
    config.put("tasks.max", "1");
    config.put("topics", SRC_TOPIC);
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("value.converter.schemas.enable", "false");
    config.put("iceberg.tables", TABLE.toString());
    config.put("iceberg.catalog.catalog-impl", Mc3Catalog.class.getName());
    config.put("iceberg.control.topic", CTL_TOPIC);
    config.put("iceberg.control.commit.interval-ms", "3000");
    config.put("iceberg.control.commit.timeout-ms", "20000");
    config.put("iceberg.kafka.bootstrap.servers", connect.kafka().bootstrapServers());
    // NOTE: auto.offset.reset of the coordinator consumer is left at the sink default ("latest")
    return config;
  }

  // ---------------------------------------------------------------------------------------------
  // The scenario

  static final class Result {
    final Map<String, Integer> rowsByKey;
    final Map<String, Integer> liveFileMultiplicity;
    final List<String> s1Files;
    final Map<Integer, Long> sourceCommitted;
    final boolean replayPending;

    Result(
        Map<String, Integer> rowsByKey,
        Map<String, Integer> liveFileMultiplicity,
        List<String> s1Files,
        Map<Integer, Long> sourceCommitted,
        boolean replayPending) {
      this.rowsByKey = rowsByKey;
      this.liveFileMultiplicity = liveFileMultiplicity;
      this.s1Files = s1Files;
      this.sourceCommitted = sourceCommitted;
      this.replayPending = replayPending;
    }

    List<String> duplicateRows() {
      List<String> result = new ArrayList<>();
      rowsByKey.forEach(
          (key, n) -> {
            if (n > 1) {
              result.add(key + " x" + n);
            }
          });
      return result;
    }

    List<String> duplicateFiles() {
      List<String> result = new ArrayList<>();
      liveFileMultiplicity.forEach(
          (file, n) -> {
            if (n > 1) {
              result.add(file + " x" + n);
            }
          });
      return result;
    }
  }

  Result runScenario() throws Exception {
    step("0. start connector (tasks.max=1, commit interval 3s, coordinator reset=latest default)");
    connect.configureConnector(CONNECTOR, connectorConfig());
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 1, "Connector did not start");

    // CE states 6-11: a running coordinator has checkpointed its control-topic position at least
    // once (here: an empty commit cycle). Without any checkpoint, a restarted coordinator starts at
    // the sink default auto.offset.reset=latest and does not re-read anything (PR #18006 setting).
    await(
        "first -coord checkpoint (empty commit cycle)",
        Duration.ofSeconds(90),
        () -> {
          try {
            return !groupOffsets(COORD_GROUP).isEmpty();
          } catch (Exception e) {
            return false;
          }
        });
    report.println("   -coord checkpoint after empty cycle(s) = " + groupOffsets(COORD_GROUP));

    step("1. arm catalog fault: next connector commit LANDS, client sees CommitStateUnknown");
    Mc3Catalog.armUnknownLanded();
    produceBatch("b1");
    await(
        "connector snapshot S1 + fault fired",
        Duration.ofSeconds(120),
        () -> Mc3Catalog.firedCount() == 1 && !connectorSnapshots(table()).isEmpty());
    Snapshot s1 = connectorSnapshots(table()).get(0);
    List<String> s1Files = addedFiles(table(), s1);
    report.println("   S1 = " + describe(s1));
    report.println("   S1 added files = " + s1Files);
    awaitTaskState("FAILED", Duration.ofSeconds(90));
    report.println("   task 0 state = FAILED, trace head: " + traceHead());

    step("2. durable state after the failure");
    Map<Integer, Long> coordCkpt = groupOffsets(COORD_GROUP);
    Map<Integer, Long> srcCommitted = groupOffsets(CONNECT_GROUP);
    List<String> ctl = new ArrayList<>();
    long s1DataWrittenOffset = dumpControlTopic(s1.summary().get(COMMIT_ID_PROP), ctl);
    ctl.forEach(line -> report.println("   ctl " + line));
    report.println("   -coord group committed offsets (" + COORD_GROUP + ") = " + coordCkpt);
    report.println("   source group committed offsets (" + CONNECT_GROUP + ") = " + srcCommitted);
    report.println("   S1 kafka.connect.offsets = " + s1.summary().get(OFFSETS_PROP));
    long ckpt = coordCkpt.getOrDefault(0, -1L);
    boolean replayPending = s1DataWrittenOffset >= 0 && ckpt >= 0 && ckpt <= s1DataWrittenOffset;
    report.println(
        "   S1's DataWritten is at ctl offset "
            + s1DataWrittenOffset
            + "; -coord checkpoint "
            + ckpt
            + " <= it -> a restarted coordinator re-reads it: "
            + replayPending);

    step("3. table maintenance while the task is down: " + variant());
    String what = maintenance(table());
    report.println("   " + what);
    Table afterMaintenance = table();
    for (Snapshot snapshot : afterMaintenance.snapshots()) {
      report.println("   live snapshot " + describe(snapshot));
    }
    report.println(
        "   floor as Coordinator.lastCommittedOffsetsForTable reads it now = "
            + floor(afterMaintenance));

    step("4. operator restarts the FAILED task (REST), then batch b2 is produced");
    connect.restartTask(CONNECTOR, 0);
    awaitTaskState("RUNNING", Duration.ofSeconds(90));
    produceBatch("b2");
    await(
        "all b2 rows committed to the table",
        Duration.ofSeconds(120),
        () -> {
          Map<String, Integer> rows = rows(table());
          for (int i = 0; i < BATCH; i += 1) {
            if (!rows.containsKey("b2-" + i)) {
              return false;
            }
          }
          return true;
        });
    Thread.sleep(8000L); // let one more commit cycle run (nothing else may change)

    step("5. result");
    Table end = table();
    for (Snapshot snapshot : end.snapshots()) {
      report.println("   snapshot " + describe(snapshot));
    }
    Map<String, Integer> rows = rows(end);
    Map<String, Integer> files = liveFiles(end);
    Map<Integer, Long> srcEnd = groupOffsets(CONNECT_GROUP);
    report.println("   source group committed offsets = " + srcEnd + " (produced " + produced + ")");
    report.println("   rows by data key = " + rows);
    report.println("   live data-file multiplicity = " + files);
    report.println("   connector task state = " + taskState());
    return new Result(rows, files, s1Files, srcEnd, replayPending);
  }

  // ---------------------------------------------------------------------------------------------
  // Maintenance helpers (public Iceberg API, what Spark procedures call)

  /** Another writer appends one row (its snapshot has no connector offsets property). */
  static String foreignAppend(Table table) throws IOException {
    GenericFileWriterFactory writerFactory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.AVRO).build();
    OutputFileFactory files =
        OutputFileFactory.builderFor(table, 999, System.nanoTime()).format(FileFormat.AVRO).build();
    DataWriter<Record> writer =
        writerFactory.newDataWriter(files.newOutputFile(), PartitionSpec.unpartitioned(), null);
    GenericRecord row = GenericRecord.create(SCHEMA);
    row.setField("id", -1L);
    row.setField("data", "other-writer");
    try {
      writer.write(row);
    } finally {
      writer.close();
    }

    DataFile file = writer.toDataFile();
    table.newAppend().appendFile(file).commit();
    return "other writer appended 1 row: snapshot " + table.currentSnapshot().snapshotId();
  }

  /** rewrite_manifests: a pure maintenance 'replace' snapshot, data files unchanged. */
  static String rewriteManifests(Table table) {
    table.rewriteManifests().clusterBy(file -> "all").rewriteIf(manifest -> true).commit();
    return "rewrite_manifests: snapshot "
        + table.currentSnapshot().snapshotId()
        + " op="
        + table.currentSnapshot().operation();
  }

  /** expire_snapshots(older_than => now, retain_last => 1), default file cleanup. */
  static String expireAllButHead(Table table) {
    table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
    return "expire_snapshots(older_than=now, retain_last=1)";
  }

  // ---------------------------------------------------------------------------------------------
  // Observation helpers

  static Table table() {
    return Mc3Catalog.root().loadTable(TABLE);
  }

  static List<Snapshot> connectorSnapshots(Table table) {
    List<Snapshot> result = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.summary().containsKey(COMMIT_ID_PROP)) {
        result.add(snapshot);
      }
    }
    return result;
  }

  static List<String> addedFiles(Table table, Snapshot snapshot) {
    List<String> result = new ArrayList<>();
    for (DataFile file : snapshot.addedDataFiles(table.io())) {
      result.add(file.location());
    }
    return result;
  }

  /** The floor exactly as Coordinator.lastCommittedOffsetsForTable computes it (main branch). */
  static String floor(Table table) {
    Snapshot head = table.currentSnapshot();
    if (head == null) {
      return "{} (empty table)";
    }
    List<Long> walked = new ArrayList<>();
    for (Snapshot snapshot : SnapshotUtil.ancestorsOf(head.snapshotId(), table::snapshot)) {
      walked.add(snapshot.snapshotId());
      if (snapshot.summary().containsKey(OFFSETS_PROP)) {
        return snapshot.summary().get(OFFSETS_PROP) + " (found at " + snapshot.snapshotId() + ")";
      }
    }
    return "{} (ancestry walked " + walked + ", parent of last = " + lastParent(table, walked) + ")";
  }

  private static String lastParent(Table table, List<Long> walked) {
    Long parent = table.snapshot(walked.get(walked.size() - 1)).parentId();
    return parent == null ? "none" : parent + (table.snapshot(parent) == null ? " EXPIRED" : "");
  }

  static String describe(Snapshot snapshot) {
    Map<String, String> s = snapshot.summary();
    return snapshot.snapshotId()
        + " parent="
        + snapshot.parentId()
        + " op="
        + snapshot.operation()
        + " added-files="
        + s.get("added-data-files")
        + " commit-id="
        + s.get(COMMIT_ID_PROP)
        + " offsets="
        + s.get(OFFSETS_PROP);
  }

  static Map<String, Integer> rows(Table table) {
    Map<String, Integer> rows = new TreeMap<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.merge(String.valueOf(record.getField("data")), 1, Integer::sum);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return rows;
  }

  static Map<String, Integer> liveFiles(Table table) {
    Map<String, Integer> files = new TreeMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String location = task.file().location();
        files.merge(location.substring(location.lastIndexOf('/') + 1), 1, Integer::sum);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return files;
  }

  private Admin admin() {
    return Admin.create(
        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers()));
  }

  Map<Integer, Long> groupOffsets(String group) throws Exception {
    Map<Integer, Long> result = new TreeMap<>();
    try (Admin admin = admin()) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin
              .listConsumerGroupOffsets(group)
              .partitionsToOffsetAndMetadata()
              .get(20, TimeUnit.SECONDS);
      offsets.forEach(
          (tp, offset) -> {
            if (offset != null) {
              result.put(tp.partition(), offset.offset());
            }
          });
    }
    return result;
  }

  /**
   * read_committed dump of the control topic; returns the offset of the first DataWritten of the
   * given commit id (or -1).
   */
  long dumpControlTopic(String commitId, List<String> lines) {
    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    long result = -1;
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(consumerProps, new StringDeserializer(), new ByteArrayDeserializer())) {
      TopicPartition tp = new TopicPartition(CTL_TOPIC, 0);
      consumer.assign(List.of(tp));
      consumer.seekToBeginning(List.of(tp));
      long end = consumer.endOffsets(List.of(tp)).get(tp);
      long deadline = System.currentTimeMillis() + 20_000L;
      while (consumer.position(tp) < end && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(500))) {
          Event event = AvroUtil.decode(record.value());
          String detail;
          switch (event.payload().type()) {
            case START_COMMIT:
              detail = "commit=" + ((StartCommit) event.payload()).commitId();
              break;
            case DATA_WRITTEN:
              DataWritten dw = (DataWritten) event.payload();
              detail = "commit=" + dw.commitId() + " files=" + dw.dataFiles().size();
              if (result < 0 && String.valueOf(dw.commitId()).equals(commitId)) {
                result = record.offset();
              }
              break;
            case DATA_COMPLETE:
              detail = "commit=" + ((DataComplete) event.payload()).commitId();
              break;
            case COMMIT_TO_TABLE:
              detail =
                  "commit="
                      + ((CommitToTable) event.payload()).commitId()
                      + " snapshot="
                      + ((CommitToTable) event.payload()).snapshotId();
              break;
            case COMMIT_COMPLETE:
              detail = "commit=" + ((CommitComplete) event.payload()).commitId();
              break;
            default:
              detail = "";
          }
          lines.add("@" + record.offset() + " " + event.payload().type() + " " + detail);
        }
      }
    }
    return result;
  }

  String taskState() {
    ConnectorStateInfo info = connect.connectorStatus(CONNECTOR);
    for (ConnectorStateInfo.TaskState state : info.tasks()) {
      if (state.id() == 0) {
        return state.state();
      }
    }
    return "UNKNOWN";
  }

  String traceHead() {
    ConnectorStateInfo info = connect.connectorStatus(CONNECTOR);
    for (ConnectorStateInfo.TaskState state : info.tasks()) {
      if (state.id() == 0 && state.trace() != null) {
        String[] lines = state.trace().split("\n");
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
          if (line.contains("Exception") || line.contains("Caused by")) {
            sb.append(" | ").append(line.trim());
          }
        }
        return sb.toString();
      }
    }
    return "";
  }

  void awaitTaskState(String state, Duration timeout) throws InterruptedException {
    await("task 0 " + state, timeout, () -> state.equals(taskState()));
  }

  void produceBatch(String batch) throws Exception {
    for (int i = 0; i < BATCH; i += 1) {
      long id = nextId++;
      String value = "{\"id\": " + id + ", \"data\": \"" + batch + "-" + i + "\"}";
      producer.send(new ProducerRecord<>(SRC_TOPIC, 0, "k" + id, value)).get();
      produced += 1;
    }
    report.println("   produced " + BATCH + " records " + batch + "-0.." + batch + "-" + (BATCH - 1));
  }

  void step(String text) {
    report.println("[MC-3 " + variant() + "] " + text);
  }

  static void await(String what, Duration timeout, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      Thread.sleep(200);
    }
  }

  static Map<String, Object> linked() {
    return new LinkedHashMap<>();
  }

  /** Writes to the report file and to stdout. */
  private static final class TeeOutputStream extends java.io.OutputStream {
    private final java.io.OutputStream file;

    private TeeOutputStream(java.io.OutputStream file) {
      this.file = file;
    }

    @Override
    public void write(int b) throws IOException {
      file.write(b);
      System.out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      file.write(b, off, len);
      System.out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      file.flush();
      System.out.flush();
    }

    @Override
    public void close() throws IOException {
      file.close();
    }
  }
}
