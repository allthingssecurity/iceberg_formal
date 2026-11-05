/*
 * MC-1 reproduction (run inside a pristine apache/iceberg @ 9fffeb6 tree, module
 * :iceberg-kafka-connect:iceberg-kafka-connect, same package as the module's own TestCoordinator /
 * TestControlTopicReplay so the package-private Coordinator/Worker can be driven the way
 * CoordinatorThread and CommitterImpl drive them).
 *
 * Rolling upgrade 1.10.0 -> HEAD, mid-way:
 *   task-0  UPGRADED (this tree's real Worker + SinkWriter + IcebergWriterFactory), owns src-0 and
 *           therefore hosts the commit Coordinator (this tree's real Coordinator / CommitState).
 *   task-1  LEGACY 1.10.0, owns src-1. Its StartCommit answer is produced by a separate JVM that
 *           runs only the released 1.10.0 jars (LegacyPeer110), i.e. the uuid-less DataWritten a
 *           1.10.0 worker really puts on the control topic.
 * Both tasks write table db.tbl. Nothing in the system under test is stubbed except the Kafka
 * clients (MockProducer / MockConsumer, as in the module's own tests): the control topic is an
 * append-only log kept by this test (offset = append index, read_committed view = committed
 * MockProducer transactions + the legacy peer's answer). KafkaUtils.consumerGroupMetadata is
 * stubbed exactly as TestWorker does (it needs the Connect runtime's WorkerSinkTaskContext).
 *
 * Variant A: iceberg.control.commit.threads=1      -> second group silently "nothing to commit".
 * Variant B: iceberg.control.commit.threads=4 + Level-1 timing aid (the TEST's catalog wrapper
 *            sleeps 400 ms after loadTable on iceberg-committer threads so both groups read the
 *            same floor) -> second group's CAS retry fails validation, coordinator dies; restart.
 */
package org.apache.iceberg.connect.channel;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TestLeadMC4BranchDrop {
  private static final String CONNECTOR = "mc1-sink";
  private static final String GROUP = "connect-" + CONNECTOR; // IcebergSinkConfig.connectGroupId()
  private static final String COORD_GROUP = GROUP + "-coord"; // Coordinator's control consumer group
  private static final String CTL = "mc1-control";
  private static final String SRC = "src";
  private static final String CATALOG_NAME = "iceberg"; // iceberg.catalog (both versions)
  private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "tbl");
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  private static final int RECORDS_PER_TASK = 3;
  private static final String BRANCH = "audit";
  private static final TopicPartition CTL_TP = new TopicPartition(CTL, 0);
  private static final TopicPartition SRC0 = new TopicPartition(SRC, 0);
  private static final TopicPartition SRC1 = new TopicPartition(SRC, 1);

  @Test
  void trigger_branchDroppedAfterPublish() throws Exception {
    run("drop", true);
  }

  @Test
  void control_branchKeptAfterPublish() throws Exception {
    run("kept", false);
  }

  /** Control topic partition 0 + Kafka client mocks shared by the actors of one scenario. */
  private static final class Lab {
    private final String name;
    private final File dir;
    private final Table table;
    private final Catalog catalog;
    private final IcebergSinkConfig config;
    private final MockProducer<String, byte[]> producer =
        new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
    private final List<byte[]> ctlLog = new ArrayList<>();
    private final Map<CtlConsumer, Integer> delivered = new IdentityHashMap<>();
    private final List<CtlConsumer> created = new ArrayList<>();
    private int historySeen = 0;
    private final KafkaClientFactory clientFactory = mock(KafkaClientFactory.class);

    Lab(String name, int commitThreads, boolean delayCommitterLoads) throws IOException {
      this.name = name;
      this.dir = new File(System.getProperty("mc1.workDir", "build/mc1"), name);
      if (dir.exists()) {
        deleteRecursively(dir);
      }
      dir.mkdirs();
      File warehouse = new File(dir, "warehouse");

      HadoopCatalog hadoopCatalog = new HadoopCatalog();
      hadoopCatalog.setConf(new Configuration());
      hadoopCatalog.initialize(
          CATALOG_NAME, Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toURI().toString()));
      this.table =
          hadoopCatalog.createTable(
              TABLE_ID,
              SCHEMA,
              PartitionSpec.unpartitioned(),
              Map.of(TableProperties.FORMAT_VERSION, "2"));
      if (delayCommitterLoads) {
        // Level-1 timing aid in the TEST's catalog (environment), not in the system under test:
        // after loading the table on a coordinator commit thread, wait so that the two parallel
        // commit groups both hold the pre-commit metadata before either CAS runs.
        HadoopCatalog spied = spy(hadoopCatalog);
        doAnswer(
                inv -> {
                  Object loaded = inv.callRealMethod();
                  if (Thread.currentThread().getName().startsWith("iceberg-committer")) {
                    Thread.sleep(400);
                  }
                  return loaded;
                })
            .when(spied)
            .loadTable(any());
        this.catalog = spied;
      } else {
        this.catalog = hadoopCatalog;
      }

      Map<String, String> props = new HashMap<>();
      props.put("name", CONNECTOR);
      props.put("task.id", "0");
      props.put("iceberg.tables", TABLE_ID.toString());
      props.put("iceberg.catalog", CATALOG_NAME);
      props.put("iceberg.catalog.warehouse", warehouse.toURI().toString());
      props.put("iceberg.control.topic", CTL);
      props.put("iceberg.control.commit.interval-ms", "0");
      props.put("iceberg.control.commit.timeout-ms", String.valueOf(Integer.MAX_VALUE));
      props.put("iceberg.control.commit.threads", String.valueOf(commitThreads));
      props.put("iceberg.tables.default-commit-branch", BRANCH);
      this.config = new IcebergSinkConfig(props);

      producer.initTransactions();
      when(clientFactory.createProducer(any())).thenReturn(producer);
      when(clientFactory.createAdmin()).thenReturn(mock(Admin.class));
      when(clientFactory.createConsumer(any()))
          .thenAnswer(
              inv -> {
                CtlConsumer consumer = new CtlConsumer(inv.getArgument(0));
                created.add(consumer);
                return consumer;
              });
    }

    CtlConsumer lastConsumer() {
      return created.get(created.size() - 1);
    }

    /** Assign the control partition to a consumer that starts reading at {@code fromOffset}. */
    void attach(CtlConsumer consumer, int fromOffset) {
      consumer.rebalance(List.of(CTL_TP));
      consumer.updateBeginningOffsets(Map.of(CTL_TP, 0L));
      consumer.seek(CTL_TP, fromOffset);
      delivered.put(consumer, fromOffset);
      deliver();
    }

    /** Move newly committed producer records to the log, then feed every attached consumer. */
    void deliver() {
      List<org.apache.kafka.clients.producer.ProducerRecord<String, byte[]>> history =
          producer.history();
      for (int i = historySeen; i < history.size(); i++) {
        ctlLog.add(history.get(i).value());
      }
      historySeen = history.size();
      for (Map.Entry<CtlConsumer, Integer> entry : delivered.entrySet()) {
        for (int off = entry.getValue(); off < ctlLog.size(); off++) {
          entry.getKey().addRecord(new ConsumerRecord<>(CTL, 0, off, "legacy-or-head", ctlLog.get(off)));
        }
        entry.setValue(ctlLog.size());
      }
    }

    Map<TopicPartition, Long> committedOffsets(String groupId) {
      Map<TopicPartition, Long> latest = new TreeMap<>((a, b) -> a.toString().compareTo(b.toString()));
      producer.consumerGroupOffsetsHistory().stream()
          .filter(commit -> commit.containsKey(groupId))
          .forEach(
              commit -> {
                for (Map.Entry<TopicPartition, OffsetAndMetadata> e : commit.get(groupId).entrySet()) {
                  latest.put(e.getKey(), e.getValue().offset());
                }
              });
      return latest;
    }

    UUID lastStartCommitId() {
      for (int i = ctlLog.size() - 1; i >= 0; i--) {
        Event event = AvroUtil.decode(ctlLog.get(i));
        if (event.payload() instanceof StartCommit) {
          return ((StartCommit) event.payload()).commitId();
        }
      }
      throw new IllegalStateException("no StartCommit on the control topic");
    }

    /** task-1 (legacy 1.10.0) answers the latest StartCommit, from its own JVM. */
    void legacyAnswer(long firstOffset, int count) throws Exception {
      UUID commitId = lastStartCommitId();
      File out = new File(dir, "legacy-answer-" + commitId);
      File schemaFile = new File(dir, "schema.json");
      Files.write(schemaFile.toPath(), SchemaParser.toJson(table.schema()).getBytes(StandardCharsets.UTF_8));
      String dataPath = "-";
      if (count > 0) {
        String location = table.location();
        File tableDir = location.startsWith("file:") ? new File(URI.create(location)) : new File(location);
        dataPath = new File(tableDir, "data/legacy-task1-" + commitId + ".parquet").getAbsolutePath();
      }
      List<String> cmd = new ArrayList<>();
      cmd.add(System.getProperty("java.home") + "/bin/java");
      cmd.add("-cp");
      cmd.add(System.getProperty("mc1.legacyCp") + File.pathSeparator + System.getProperty("mc1.peerClasses"));
      cmd.add("LegacyPeer110");
      cmd.add(out.getAbsolutePath());
      cmd.add(GROUP);
      cmd.add(CATALOG_NAME);
      cmd.add(TABLE_ID.namespace().toString());
      cmd.add(TABLE_ID.name());
      cmd.add(schemaFile.getAbsolutePath());
      cmd.add(dataPath);
      cmd.add(commitId.toString());
      cmd.add(SRC);
      cmd.add("1");
      cmd.add(String.valueOf(firstOffset));
      cmd.add(String.valueOf(count));
      Process process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      boolean finished = process.waitFor(180, TimeUnit.SECONDS);
      for (String line : output.split("\n")) {
        if (line.startsWith("[legacy-1.10.0 peer]") || line.contains("Exception")) {
          log(line);
        }
      }
      assertThat(finished && process.exitValue() == 0).as("legacy 1.10.0 peer JVM: " + output).isTrue();
      File dw = new File(out, "dw.bin");
      if (dw.exists()) {
        byte[] bytes = Files.readAllBytes(dw.toPath());
        if (Boolean.getBoolean("mc1.control")) {
          // LEAD CONTROL: identical answer (same data file, commit id, group) but carrying the
          // table uuid, exactly what a >=1.11 worker sends. Only TableReference.uuid differs.
          Event legacy = AvroUtil.decode(bytes);
          DataWritten d = (DataWritten) legacy.payload();
          DataWritten withUuid =
              new DataWritten(
                  table.spec().partitionType(),
                  d.commitId(),
                  TableReference.of(CATALOG_NAME, TABLE_ID, table.uuid()),
                  d.dataFiles(),
                  d.deleteFiles());
          bytes = AvroUtil.encode(new Event(legacy.groupId(), withUuid));
          log("CONTROL: legacy DataWritten re-encoded with table uuid " + table.uuid());
        }
        ctlLog.add(bytes);
      }
      ctlLog.add(Files.readAllBytes(new File(out, "dc.bin").toPath()));
      deliver();
    }

    void log(String msg) {
      System.out.println("[MC-4L][" + name + "] " + msg);
    }

    void dumpControlTopic() {
      log("control topic " + CTL + "-0 (read_committed), decoded with this tree's AvroUtil:");
      for (int off = 0; off < ctlLog.size(); off++) {
        Event event = AvroUtil.decode(ctlLog.get(off));
        String detail;
        if (event.payload() instanceof DataWritten) {
          DataWritten dw = (DataWritten) event.payload();
          TableReference ref = dw.tableReference();
          long records = 0;
          for (DataFile f : dw.dataFiles()) {
            records += f.recordCount();
          }
          detail =
              "DATA_WRITTEN commit=" + dw.commitId() + " table=" + ref.catalog() + "." + ref.identifier()
                  + " uuid=" + ref.uuid() + " files=" + dw.dataFiles().size() + " records=" + records;
        } else if (event.payload() instanceof DataComplete) {
          DataComplete dc = (DataComplete) event.payload();
          StringBuilder sb = new StringBuilder();
          for (TopicPartitionOffset tpo : dc.assignments()) {
            sb.append(tpo.topic()).append('-').append(tpo.partition()).append('=').append(tpo.offset()).append(' ');
          }
          detail = "DATA_COMPLETE commit=" + dc.commitId() + " " + sb.toString().trim();
        } else if (event.payload() instanceof StartCommit) {
          detail = "START_COMMIT commit=" + ((StartCommit) event.payload()).commitId();
        } else if (event.payload() instanceof CommitToTable) {
          CommitToTable ct = (CommitToTable) event.payload();
          detail =
              "COMMIT_TO_TABLE commit=" + ct.commitId() + " table=" + ct.tableReference().identifier()
                  + " uuid=" + ct.tableReference().uuid() + " snapshot=" + ct.snapshotId();
        } else {
          detail = String.valueOf(event.type());
        }
        log("  @" + off + " " + detail);
      }
    }

    /** Rows per source partition, snapshots, offsets; returns the missing record ids. */
    Set<Long> report(String phase, long src0Records, long src1Records) throws IOException {
      table.refresh();
      log("=== " + phase + " ===");
      List<Snapshot> snapshots = new ArrayList<>();
      table.snapshots().forEach(snapshots::add);
      String offsetsProp = "kafka.connect.offsets." + CTL + "." + GROUP;
      log("snapshots=" + snapshots.size());
      for (Snapshot s : snapshots) {
        log("  snapshot " + s.snapshotId() + " op=" + s.operation()
            + " added-records=" + s.summary().get("added-records")
            + " total-records=" + s.summary().get("total-records")
            + " " + offsetsProp + "=" + s.summary().get(offsetsProp)
            + " commit-id=" + s.summary().get("kafka.connect.commit-id"));
      }
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          log("  live data file " + task.file().location() + " records=" + task.file().recordCount());
        }
      }
      Set<Long> present = new TreeSet<>();
      long rowCount = 0;
      try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
        for (Record row : rows) {
          present.add((Long) row.getField("id"));
          rowCount++;
        }
      }
      Set<Long> expected = new TreeSet<>();
      for (long off = 0; off < src0Records; off++) {
        expected.add(off);
      }
      for (long off = 0; off < src1Records; off++) {
        expected.add(1_000_000L + off);
      }
      Set<Long> missing = new TreeSet<>(expected);
      missing.removeAll(present);
      log("source offsets committed by task-0 (upgraded) in its answer txn, group " + GROUP + ": "
          + committedOffsets(GROUP));
      log("source offsets committed by task-1 (legacy) in its answer txn (its DataComplete): src-1="
          + src1Records);
      log("coordinator checkpoint, group " + COORD_GROUP + ": " + committedOffsets(COORD_GROUP));
      log("table rows=" + rowCount + " expected=" + expected.size() + " missing ids=" + missing);
      return missing;
    }

    private static void deleteRecursively(File file) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursively(child);
        }
      }
      file.delete();
    }
  }

  /** A control-topic consumer whose group metadata carries its real group id. */
  private static final class CtlConsumer extends MockConsumer<String, byte[]> {
    private final String groupId;

    CtlConsumer(String groupId) {
      super(OffsetResetStrategy.EARLIEST);
      this.groupId = groupId;
    }

    @Override
    public synchronized ConsumerGroupMetadata groupMetadata() {
      return new ConsumerGroupMetadata(groupId);
    }
  }

  private static Collection<MemberDescription> members() {
    MemberDescription upgraded =
        new MemberDescription(
            "task-0-upgraded", Optional.empty(), "connector-consumer-" + CONNECTOR + "-0", "/w-new",
            new MemberAssignment(Set.of(SRC0)));
    MemberDescription legacy =
        new MemberDescription(
            "task-1-legacy", Optional.empty(), "connector-consumer-" + CONNECTOR + "-1", "/w-old",
            new MemberAssignment(Set.of(SRC1)));
    return List.of(upgraded); // MC-4: one task, no legacy peer
  }

  private static Coordinator startCoordinator(Lab lab, int fromOffset) {
    Coordinator coordinator =
        new Coordinator(lab.catalog, lab.config, members(), lab.clientFactory, mock(SinkTaskContext.class));
    coordinator.start();
    lab.attach(lab.lastConsumer(), fromOffset);
    return coordinator;
  }

  /** One commit cycle: StartCommit, legacy answer, upgraded answer, coordinator commits. */
  private static RuntimeException cycle(
      Lab lab, Coordinator coordinator, Worker upgradedWorker, long legacyFirst, int legacyCount)
      throws Exception {
    coordinator.process(); // interval 0 -> StartCommit
    lab.deliver();
    if (legacyCount >= 0) {
      lab.legacyAnswer(legacyFirst, legacyCount);
    }
    upgradedWorker.process(); // task-0 (HEAD) answers in one txn with its source offsets
    lab.deliver();
    RuntimeException failure = null;
    try {
      coordinator.process(); // consumes both answers -> commit(false)
    } catch (RuntimeException e) {
      failure = e; // in production CoordinatorThread terminates and the task fails
      lab.log("coordinator.process() threw (CoordinatorThread would exit -> NotRunningException, task FAILED): "
          + e);
      Throwable t = e;
      while (t != null) {
        StackTraceElement[] frames = t.getStackTrace();
        for (int i = 0; i < Math.min(frames.length, 14); i++) {
          lab.log("    at " + frames[i]);
        }
        t = t.getCause();
        if (t != null) {
          lab.log("  caused by " + t);
        }
      }
    }
    lab.deliver();
    return failure;
  }

  private static List<SinkRecord> recs(long from, long to) {
    List<SinkRecord> records = new ArrayList<>();
    for (long off = from; off < to; off++) {
      records.add(new SinkRecord(SRC, 0, null, null, null, Map.of("id", off, "data", "r" + off), off));
    }
    return records;
  }

  private static long rows(Table table, String ref) throws IOException {
    table.refresh();
    if (table.refs().get(ref) == null) {
      return -1;
    }
    long n = 0;
    try (CloseableIterable<Record> it = IcebergGenerics.read(table).useSnapshot(table.refs().get(ref).snapshotId()).build()) {
      for (Record r : it) {
        n++;
      }
    }
    return n;
  }

  private void run(String name, boolean drop) throws Exception {
    Lab lab = new Lab(name, 1, false);
    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class, Mockito.CALLS_REAL_METHODS)) {
      kafkaUtils.when(() -> KafkaUtils.consumerGroupMetadata(any())).thenReturn(new ConsumerGroupMetadata(GROUP));
      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(Set.of(SRC0));
      Worker worker = new Worker(lab.config, lab.clientFactory, new SinkWriter(lab.catalog, lab.config), context);
      worker.start();
      lab.attach(lab.lastConsumer(), lab.ctlLog.size());
      Coordinator coordinator = startCoordinator(lab, 0);

      // phase A: 3 records committed to branch 'audit' (created by the commit)
      worker.save(recs(0, 3));
      RuntimeException a = cycle(lab, coordinator, worker, 0, -1);
      lab.log("phase A failure=" + a + " refs=" + rows(lab.table, BRANCH) + " rows on audit, main rows=" + rows(lab.table, "main")
          + " refs=" + lab.table.refs().keySet());

      // operator: WAP publish (fast-forward main to audit), then drop the branch (trigger) or keep it (control)
      lab.table.refresh();
      lab.table.manageSnapshots().fastForwardBranch("main", BRANCH).commit();
      if (drop) {
        lab.table.manageSnapshots().removeBranch(BRANCH).commit();
      }
      lab.table.refresh();
      lab.log("operator: published audit->main" + (drop ? " and DROPPED branch audit" : " (branch kept)") + "; refs=" + lab.table.refs().keySet());

      // phase B: three more commit cycles with new data
      List<String> outcomes = new ArrayList<>();
      for (int c = 0; c < 3; c++) {
        worker.save(recs(3 + 3L * c, 6 + 3L * c));
        RuntimeException f = cycle(lab, coordinator, worker, 0, -1);
        outcomes.add(f == null ? "ok" : f.getClass().getSimpleName());
        if (f != null) {
          // as in production: CoordinatorThread exits, task FAILED, operator/Connect restarts the task
          coordinator.terminate();
          int from = lab.committedOffsets(COORD_GROUP).getOrDefault(CTL_TP, 0L).intValue();
          coordinator = startCoordinator(lab, from);
        }
      }
      coordinator.terminate();
      lab.table.refresh();
      long mainRows = rows(lab.table, "main");
      long branchRows = rows(lab.table, BRANCH);
      lab.log("phase B commit outcomes=" + outcomes + " main rows=" + mainRows + " audit rows=" + branchRows
          + " refs=" + lab.table.refs().keySet() + " source offsets committed=" + lab.committedOffsets(GROUP));
      boolean stalled = outcomes.stream().allMatch(o -> !o.equals("ok"));
      lab.log("RESULT " + (stalled ? "STALL" : "NO-STALL") + ": " + outcomes);
      if (drop) {
        assertThat(stalled).as("MC-4: every commit after the branch drop fails").isTrue();
      } else {
        assertThat(outcomes).as("control: commits keep succeeding").containsOnly("ok");
      }
    }
  }
}
