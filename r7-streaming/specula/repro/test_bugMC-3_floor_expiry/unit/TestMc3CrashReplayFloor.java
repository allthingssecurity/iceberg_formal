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
package org.apache.iceberg.connect.channel;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * MC-3, deterministic replay of the counterexample MC_hunt_s2_floor_reachability_latest_bfs.out on
 * the real (pristine) Coordinator. Kafka is Kafka's own MockProducer / MockConsumer (as in the
 * project's TestControlTopicReplay); the control topic is one append-only log: every record a
 * coordinator commits through its transactional producer and every DataWritten / DataComplete a
 * worker would send is appended in order and delivered to the current coordinator's consumer from
 * its group position. Nothing in the sink is modified or bypassed: only Coordinator.start() and
 * Coordinator.process() are called, exactly as CoordinatorThread.run() does.
 */
class TestMc3CrashReplayFloor {
  private static final String CTL = "ctl-topic";
  private static final String SRC = "src-topic";
  private static final String GROUP = "cg-connect";
  private static final String OFFSETS_PROP = "kafka.connect.offsets." + CTL + "." + GROUP;
  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "tbl");
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  private static final TopicPartition CTL_TP = new TopicPartition(CTL, 0);

  /** Models the coordinator host dying: nothing after this point runs in that process. */
  static final class SimulatedCrash extends RuntimeException {
    SimulatedCrash() {
      super("MC-3: coordinator process crashed (no further action of this process)");
    }
  }

  private FaultyCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;
  private KafkaClientFactory clientFactory;
  private final AtomicReference<MockProducer<String, byte[]>> nextProducer = new AtomicReference<>();
  private final AtomicReference<MockConsumer<String, byte[]>> nextConsumer = new AtomicReference<>();
  private final List<byte[]> ctlLog = new ArrayList<>(); // the control topic, offset = index
  private PrintStream out;
  private long rowId = 0;
  private final Map<String, String> logicalNames = new java.util.HashMap<>();

  @BeforeEach
  void before(TestInfo info) throws IOException {
    Path dir = Paths.get(System.getProperty("tla.trace.dir", "build/mc3"));
    Files.createDirectories(dir);
    out =
        new PrintStream(
            new FileOutputStream(
                dir.resolve("MC-3_unit_" + info.getTestMethod().get().getName() + ".txt")
                    .toFile()),
            true,
            StandardCharsets.UTF_8);

    catalog = new FaultyCatalog();
    catalog.initialize("iceberg", Map.of());
    catalog.createNamespace(Namespace.of("db"));
    table =
        catalog.createTable(
            TABLE,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", "2", "write.format.default", "avro"));

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL);
    when(config.connectGroupId()).thenReturn(GROUP);
    when(config.coordinatorTransactionalId()).thenReturn(GROUP + "-coordinator");
    when(config.commitThreads()).thenReturn(1);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class)); // main branch
    when(config.commitMaxConsecutiveFailures()).thenReturn(1);
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    clientFactory = mock(KafkaClientFactory.class);
    when(clientFactory.createProducer(any())).thenAnswer(inv -> nextProducer.get());
    when(clientFactory.createConsumer(any())).thenAnswer(inv -> nextConsumer.get());
    when(clientFactory.createAdmin()).thenReturn(mock(Admin.class));
  }

  @AfterEach
  void after() throws IOException {
    out.close();
    catalog.close();
  }

  // -----------------------------------------------------------------------------------------------
  // Tests

  /** The counterexample: crash at Coordinator.java:216, other writer + expire, replay commits. */
  @Test
  void crashBeforeCheckpointThenForeignCommitAndExpiryDuplicates() throws Exception {
    Map<String, Integer> files = crashScenario(true);
    assertThat(files.get("F1-b1")).as("F1 (S1's file) live entries").isEqualTo(2);
    assertThat(rows()).containsEntry("b1-0", 2).containsEntry("b1-4", 2).containsEntry("b2-0", 1);
    log("RESULT crash+expire: F1 committed twice, rows b1-* x2 -> NoDuplicateRows violated");
  }

  /** Control: same crash and replay, S1 not expired -> the floor filters the replay. */
  @Test
  void crashBeforeCheckpointFloorReachableNoDuplicates() throws Exception {
    Map<String, Integer> files = crashScenario(false);
    assertThat(files.get("F1-b1")).as("F1 (S1's file) live entries").isEqualTo(1);
    assertThat(rows()).containsEntry("b1-0", 1).containsEntry("b1-4", 1).containsEntry("b2-0", 1);
    log("RESULT control (no expire): replay filtered by the floor, no duplicates");
  }

  /**
   * In-process variant (no crash, no restart): a PARTIAL commit whose table commit lands but
   * reports CommitStateUnknownException is "will retry" (Coordinator.java:166-173), the buffer is
   * kept, and the next cycle re-commits it guarded only by the floor.
   */
  @Test
  void partialCommitUnknownLandedRetryAfterExpiryDuplicates() throws Exception {
    // two source partitions; only the worker owning p0 answers in time -> timeout -> partial
    MemberDescription member =
        member(ImmutableSet.of(new TopicPartition(SRC, 0), new TopicPartition(SRC, 1)));
    MockProducer<String, byte[]> producer = newProducer();
    MockConsumer<String, byte[]> consumer = newConsumer(null);
    Coordinator coordinator = startCoordinator(member, producer, consumer);

    coordinator.process(); // StartCommit c1
    UUID c1 = lastStartCommit(producer);
    syncProducer(producer, consumer);
    DataFile f1 = writeFile("F1-b1", "b1");
    publish(consumer, dataWritten(c1, f1));
    publish(consumer, dataComplete(c1, 0, 5L));
    coordinator.process(); // reads DW/DC of p0; p1 missing -> not ready
    log("c1: DW(F1) + DC(p0) received; p1 did not answer");

    catalog.armUnknownLanded();
    when(config.commitTimeoutMs()).thenReturn(-1); // commit timeout -> partial commit
    coordinator.process();
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    assertThat(catalog.fired()).isEqualTo(1);
    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(1);
    table.refresh();
    Snapshot s1 = table.currentSnapshot();
    log("partial commit c1: table commit LANDED as S1=" + describe(s1));
    log("  but the client saw CommitStateUnknownException -> 'will retry', buffer kept");
    log("  -coord checkpoint = " + checkpoint(producer));
    syncProducer(producer, consumer);

    maintenance(true);

    coordinator.process(); // StartCommit c2
    UUID c2 = lastStartCommit(producer);
    syncProducer(producer, consumer);
    DataFile f2 = writeFile("F2-b2", "b2");
    publish(consumer, dataWritten(c2, f2));
    publish(consumer, dataComplete(c2, 0, 10L));
    publish(consumer, dataComplete(c2, 1, 0L));
    coordinator.process(); // full commit c2 with the retained DW(F1)
    table.refresh();
    log("c2 commit: S=" + describe(table.currentSnapshot()));
    Map<String, Integer> files = liveFiles();
    log("live data files = " + files + "; rows = " + rows());
    assertThat(files.get("F1-b1")).as("F1 live entries").isEqualTo(2);
    assertThat(rows()).containsEntry("b1-0", 2).containsEntry("b2-0", 1);
    log("RESULT partial-commit retry + expire: F1 committed twice (no crash, no restart)");
  }

  // -----------------------------------------------------------------------------------------------
  // The counterexample, step by step (CE state numbers in brackets)

  private Map<String, Integer> crashScenario(boolean expire) throws Exception {
    MemberDescription member = member(ImmutableSet.of(new TopicPartition(SRC, 0)));

    // [2-5] t1 owns src p0 and the leader partition -> coordinator #1 starts; the control topic is
    // empty and the -coord group has no checkpoint: latest == 0
    MockProducer<String, byte[]> producer1 = newProducer();
    MockConsumer<String, byte[]> consumer1 = newConsumer(null);
    Coordinator coordinator1 = startCoordinator(member, producer1, consumer1);

    // [6-11] StartCommit c1, no data, commit timeout -> empty partial commit -> checkpoint
    coordinator1.process();
    syncProducer(producer1, consumer1);
    when(config.commitTimeoutMs()).thenReturn(-1);
    coordinator1.process();
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    syncProducer(producer1, consumer1);
    log("[6-11] c1 empty partial commit; -coord checkpoint = " + checkpoint(producer1));

    // [12-13] StartCommit c2; the worker answers DW(c2, F1 = rows b1-0..4) + DC(c2) in one txn
    coordinator1.process();
    UUID c2 = lastStartCommit(producer1);
    syncProducer(producer1, consumer1);
    DataFile f1 = writeFile("F1-b1", "b1");
    long dwOffset = publish(consumer1, dataWritten(c2, f1));
    publish(consumer1, dataComplete(c2, 0, 5L));
    log("[12-13] worker answered c2: DW(F1) @" + dwOffset + ", DC @" + (dwOffset + 1));

    // [14-19] coordinator #1 commits S1 = F1; the process dies right after appendOp.commit()
    // (Coordinator.java:325) returned and before commitConsumerOffsets() (Coordinator.java:216)
    // finished: the checkpoint transaction never commits.
    producer1.sendOffsetsToTransactionException = new SimulatedCrash();
    assertThatThrownBy(coordinator1::process).isInstanceOf(SimulatedCrash.class);
    table.refresh();
    Snapshot s1 = table.currentSnapshot();
    Map<String, Long> ckpt = checkpoint(producer1);
    log("[14-18] S1 = " + describe(s1));
    log("[19] CRASH before commitConsumerOffsets(); -coord checkpoint still " + ckpt);
    syncProducer(producer1, null); // committed records of the dead process stay in the topic
    // coordinator1 is never used again (crashed process)

    // [20-24] restart: coordinator #2 resumes at the -coord group's committed offset
    MockProducer<String, byte[]> producer2 = newProducer();
    MockConsumer<String, byte[]> consumer2 = newConsumer(ckpt.get(CTL_TP.toString()));
    when(config.commitIntervalMs()).thenReturn(Integer.MAX_VALUE); // [24-28]: consume first
    Coordinator coordinator2 = startCoordinator(member, producer2, consumer2);
    coordinator2.process(); // [25-28] re-reads SC(c2), DW(c2) (buffered), DC(c2)
    log("[24] coordinator #2 started from the -coord checkpoint " + ckpt);
    log("[25-28] coordinator #2 position after replay = " + consumer2.position(CTL_TP));

    // [29-33] StartCommit c3; the worker answers DW(c3, F2 = rows b2-0..4) + DC(c3)
    when(config.commitIntervalMs()).thenReturn(0);
    coordinator2.process();
    UUID c3 = lastStartCommit(producer2);
    syncProducer(producer2, consumer2);
    DataFile f2 = writeFile("F2-b2", "b2");
    publish(consumer2, dataWritten(c3, f2));
    publish(consumer2, dataComplete(c3, 0, 10L));

    // [34-35] another writer commits on main, expire_snapshots removes S1
    maintenance(expire);

    // [36-37] coordinator #2 reads the floor and commits
    coordinator2.process();
    table.refresh();
    log("[36-37] coordinator #2 commit: " + describe(table.currentSnapshot()));
    Map<String, Integer> files = liveFiles();
    log("live data files = " + files + "; rows = " + rows());
    return files;
  }

  private void maintenance(boolean expire) throws IOException {
    DataFile other = writeFile("OTHER", "other-writer");
    table.newAppend().appendFile(other).commit(); // a non-connector writer: no offsets key
    table.refresh();
    log("[34] other writer committed " + describe(table.currentSnapshot()));
    if (expire) {
      table
          .expireSnapshots()
          .expireOlderThan(System.currentTimeMillis() + 1)
          .retainLast(1)
          .commit();
      table.refresh();
      log("[35] expire_snapshots(older_than=now, retain_last=1)");
    }
    log("     floor seen through the ancestry of main = " + floor());
  }

  // -----------------------------------------------------------------------------------------------
  // Harness

  private MemberDescription member(java.util.Set<TopicPartition> partitions) {
    return new MemberDescription(
        null, Optional.empty(), null, null, new MemberAssignment(partitions));
  }

  private MockProducer<String, byte[]> newProducer() {
    MockProducer<String, byte[]> producer =
        new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
    return producer;
  }

  /**
   * A consumer of the control topic (sink default auto.offset.reset=latest). {@code committed} is
   * the -coord group's committed offset held by the broker; it is installed after subscribe()
   * because MockConsumer.subscribe() clears its local committed map (a mock artifact: a real
   * KafkaConsumer fetches the group's committed offset from the broker on assignment).
   */
  private Long pendingCommitted = null;

  private MockConsumer<String, byte[]> newConsumer(Long committed) {
    pendingCommitted = committed;
    return new MockConsumer<>(OffsetResetStrategy.LATEST);
  }

  private Coordinator startCoordinator(
      MemberDescription member,
      MockProducer<String, byte[]> producer,
      MockConsumer<String, byte[]> consumer) {
    nextProducer.set(producer);
    nextConsumer.set(consumer);
    producer.initTransactions(); // KafkaClientFactory.createProducer() does this
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(member), clientFactory, mock(SinkTaskContext.class));
    coordinator.start(); // subscribe + first poll
    if (pendingCommitted != null) {
      consumer.commitSync(Map.of(CTL_TP, new OffsetAndMetadata(pendingCommitted)));
      pendingCommitted = null;
    }
    consumer.rebalance(ImmutableList.of(CTL_TP)); // the -coord group assigns the partition
    consumer.updateBeginningOffsets(Map.of(CTL_TP, 0L));
    consumer.updateEndOffsets(Map.of(CTL_TP, (long) ctlLog.size()));
    for (int offset = 0; offset < ctlLog.size(); offset += 1) {
      consumer.addRecord(new ConsumerRecord<>(CTL, 0, offset, "key", ctlLog.get(offset)));
    }
    return coordinator;
  }

  private int synced = 0;
  private MockProducer<String, byte[]> syncedProducer = null;

  /** Append the producer's committed control records to the topic and deliver them. */
  private void syncProducer(MockProducer<String, byte[]> producer, MockConsumer<String, byte[]> c) {
    if (syncedProducer != producer) {
      syncedProducer = producer;
      synced = 0;
    }
    List<ProducerRecord<String, byte[]>> history = producer.history();
    for (; synced < history.size(); synced += 1) {
      byte[] value = history.get(synced).value();
      long offset = ctlLog.size();
      ctlLog.add(value);
      if (c != null) {
        c.addRecord(new ConsumerRecord<>(CTL, 0, offset, "key", value));
      }
    }
  }

  private long publish(MockConsumer<String, byte[]> consumer, Event event) {
    byte[] value = AvroUtil.encode(event);
    long offset = ctlLog.size();
    ctlLog.add(value);
    consumer.addRecord(new ConsumerRecord<>(CTL, 0, offset, "key", value));
    return offset;
  }

  private UUID lastStartCommit(MockProducer<String, byte[]> producer) {
    UUID result = null;
    for (ProducerRecord<String, byte[]> record : producer.history()) {
      Event event = AvroUtil.decode(record.value());
      if (event.payload() instanceof StartCommit) {
        result = ((StartCommit) event.payload()).commitId();
      }
    }
    return result;
  }

  private Map<String, Long> checkpoint(MockProducer<String, byte[]> producer) {
    Map<String, Long> result = new TreeMap<>();
    for (Map<String, Map<TopicPartition, OffsetAndMetadata>> commit :
        producer.consumerGroupOffsetsHistory()) {
      // only the coordinator commits group offsets through this producer; MockConsumer reports
      // the group as "dummy.group.id" (production: COORD_GROUP = "<connect group>-coord")
      commit.values().forEach(offsets -> offsets.forEach((tp, om) -> result.put(tp.toString(), om.offset())));
    }
    return result;
  }

  /** The events a real Worker sends (Worker.java:94-111). */
  private Event dataWritten(UUID commitId, DataFile file) {
    return new Event(
        GROUP,
        new DataWritten(
            table.spec().partitionType(),
            commitId,
            TableReference.of("iceberg", TABLE, table.uuid()),
            ImmutableList.of(file),
            ImmutableList.of()));
  }

  private Event dataComplete(UUID commitId, int partition, long offset) {
    return new Event(
        GROUP,
        new DataComplete(
            commitId,
            ImmutableList.of(new TopicPartitionOffset(SRC, partition, offset, null))));
  }

  /** A real data file with five rows "<prefix>-0..4" (what the sink's writer produces). */
  private DataFile writeFile(String name, String prefix) throws IOException {
    GenericFileWriterFactory writerFactory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.AVRO).build();
    String location = table.locationProvider().newDataLocation(name + "-" + UUID.randomUUID() + ".avro");
    logicalNames.put(location, name);
    DataWriter<Record> writer =
        writerFactory.newDataWriter(
            org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(
                table.io().newOutputFile(location)),
            PartitionSpec.unpartitioned(),
            null);
    int count = "other-writer".equals(prefix) ? 1 : 5;
    try {
      for (int i = 0; i < count; i += 1) {
        GenericRecord row = GenericRecord.create(SCHEMA);
        row.setField("id", rowId++);
        row.setField("data", count == 1 ? prefix : prefix + "-" + i);
        writer.write(row);
      }
    } finally {
      writer.close();
    }
    return writer.toDataFile();
  }

  private Map<String, Integer> liveFiles() throws IOException {
    table.refresh();
    Map<String, Integer> files = new TreeMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String location = task.file().location();
        files.merge(logicalNames.getOrDefault(location, location), 1, Integer::sum);
      }
    }
    return files;
  }

  private Map<String, Integer> rows() throws IOException {
    table.refresh();
    Map<String, Integer> rows = new TreeMap<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.merge(String.valueOf(record.getField("data")), 1, Integer::sum);
      }
    }
    return rows;
  }

  private String floor() {
    table.refresh();
    Snapshot head = table.currentSnapshot();
    List<Long> walked = new ArrayList<>();
    for (Snapshot snapshot : SnapshotUtil.ancestorsOf(head.snapshotId(), table::snapshot)) {
      walked.add(snapshot.snapshotId());
      if (snapshot.summary().containsKey(OFFSETS_PROP)) {
        return snapshot.summary().get(OFFSETS_PROP) + " (at " + snapshot.snapshotId() + ")";
      }
    }
    Long parent = table.snapshot(walked.get(walked.size() - 1)).parentId();
    return "{} (walked "
        + walked
        + ", next parent "
        + parent
        + (parent != null && table.snapshot(parent) == null ? " is EXPIRED" : "")
        + ")";
  }

  private static String describe(Snapshot snapshot) {
    return snapshot.snapshotId()
        + " parent="
        + snapshot.parentId()
        + " op="
        + snapshot.operation()
        + " added="
        + snapshot.summary().get("added-data-files")
        + " offsets="
        + snapshot.summary().get(OFFSETS_PROP);
  }

  private void log(String line) {
    out.println(line);
    System.out.println("[MC-3 unit] " + line);
  }

  /** InMemoryCatalog whose next connector commit can land and then report an unknown state. */
  static final class FaultyCatalog extends InMemoryCatalog {
    private final AtomicBoolean armed = new AtomicBoolean(false);
    private int fired = 0;

    void armUnknownLanded() {
      armed.set(true);
    }

    int fired() {
      return fired;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      TableOperations delegate = super.newTableOps(tableIdentifier);
      return new TableOperations() {
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
          Snapshot next = metadata.currentSnapshot();
          boolean connector =
              next != null && next.summary().containsKey("kafka.connect.commit-id");
          delegate.commit(base, metadata);
          if (connector && armed.compareAndSet(true, false)) {
            fired += 1;
            throw new CommitStateUnknownException(
                new RuntimeException("MC-3 injected: commit applied, response lost"));
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
      };
    }
  }
}
