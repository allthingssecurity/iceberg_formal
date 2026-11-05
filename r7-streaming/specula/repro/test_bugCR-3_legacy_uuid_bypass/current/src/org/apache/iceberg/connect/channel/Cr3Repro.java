package org.apache.iceberg.connect.channel;

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.Cr3CurrentCatalogs;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

/**
 * CR-3 reproduction driver (current, pristine sink code at upstream main 9fffeb6).
 *
 * <p>The real {@link Coordinator} of this checkout is driven through its normal entry points
 * (start(), process()), and control-topic records are delivered to its consumer. A separate
 * catalog client plays the operator (another engine) that drops and recreates the table under the
 * same identifier. The worker that answers the commit is either
 *
 * <ul>
 *   <li>the RELEASED 1.10.2 sink (official Maven Central jars, separate JVM: Cr3LegacyPeer), as
 *       during a rolling upgrade (path A) or as retained control-topic records replayed after a
 *       full-stop upgrade (path B), or
 *   <li>this checkout's own Worker (control run), whose DataWritten carry the table uuid.
 * </ul>
 *
 * Kafka brokers are replaced by Kafka's MockProducer/MockConsumer, which carry the exact bytes the
 * peers produce. Nothing in the system under test is stubbed or modified.
 */
public class Cr3Repro {
  static final TableIdentifier TID = TableIdentifier.of(Namespace.of("db"), "tbl");
  static final String SRC_TOPIC = "src-topic";

  static final Schema SCHEMA_V1 =
      new Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));
  // the re-created table: id changed from bigint to string (not a legal promotion -> drop/create)
  static final Schema SCHEMA_V2_ID_AS_STRING =
      new Schema(
          Types.NestedField.optional(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  static Path root;
  static String legacyClasspath;
  static String javaBin;

  public static void main(String[] args) throws Exception {
    root = Paths.get(args[0]).toAbsolutePath();
    legacyClasspath = args[1];
    javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    String only = args.length > 2 ? args[2] : "all";

    log("system under test: " + Coordinator.class.getProtectionDomain().getCodeSource().getLocation());
    log("iceberg version of system under test: " + org.apache.iceberg.IcebergBuild.fullVersion());

    List<Scenario> scenarios =
        List.of(
            new Scenario("A1-rolling-legacy-jdbc", "jdbc", "legacy", "rolling", true, true),
            new Scenario("A1c-rolling-current-jdbc-CONTROL", "jdbc", "current", "rolling", true, true),
            new Scenario("B1-replay-legacy-jdbc", "jdbc", "legacy", "replay", true, true),
            new Scenario("A2-rolling-legacy-hadoop", "hadoop", "legacy", "rolling", false, true),
            new Scenario("A2c-rolling-current-hadoop-CONTROL", "hadoop", "current", "rolling", false, true),
            // isolates the SEPARATE defect seen on this path: no table replacement at all
            new Scenario("A0-rolling-legacy-jdbc-NO-REPLACEMENT", "jdbc", "legacy", "rolling", false, false));

    Map<String, String> results = new LinkedHashMap<>();
    boolean allAsExpected = true;
    for (Scenario s : scenarios) {
      if (!only.equals("all") && !only.equals(s.name)) {
        continue;
      }
      banner("SCENARIO " + s.name);
      String outcome;
      try {
        outcome = s.run();
      } catch (Throwable t) {
        t.printStackTrace(System.out);
        outcome = "ERROR " + t;
      }
      boolean asExpected;
      if (!s.replace) {
        asExpected = true; // informational (separate defect), not part of the CR-3 oracle
      } else if (s.worker.equals("legacy")) {
        asExpected = outcome.startsWith("BUG");
      } else {
        asExpected = outcome.startsWith("REJECTED");
      }
      allAsExpected &= asExpected;
      results.put(s.name, outcome + (asExpected ? "" : "   <-- UNEXPECTED"));
    }

    banner("SUMMARY");
    results.forEach((k, v) -> log(String.format("%-36s %s", k, v)));
    log(allAsExpected
        ? "RESULT: CR-3 REPRODUCED (legacy null-uuid DataWritten committed into the recreated table;"
            + " uuid-bearing control rejected)"
        : "RESULT: NOT AS EXPECTED");
    System.out.flush();
    System.exit(allAsExpected ? 0 : 1);
  }

  static final class Scenario {
    final String name;
    final String catalogType;
    final String worker;
    final String path;
    final boolean incompatibleSchema;
    final boolean replace;

    Scenario(
        String name, String catalogType, String worker, String path, boolean incompatible,
        boolean replace) {
      this.name = name;
      this.catalogType = catalogType;
      this.worker = worker;
      this.path = path;
      this.incompatibleSchema = incompatible;
      this.replace = replace;
    }

    String run() throws Exception {
      Path dir = root.resolve(name);
      deleteRecursively(dir);
      Files.createDirectories(dir.resolve("job"));
      Map<String, String> props = connectorProps(catalogType, dir);
      try (OutputStream out = Files.newOutputStream(dir.resolve("job/connector.properties"))) {
        Properties p = new Properties();
        p.putAll(props);
        p.store(out, "CR-3 connector config shared by both sink versions");
      }
      IcebergSinkConfig config = new IcebergSinkConfig(props);
      log("connector config: catalog=" + config.catalogProps() + " control-topic="
          + config.controlTopic() + " group=" + config.connectGroupId());

      // the operator / other engine (e.g. Spark or Trino) with its own catalog client
      Catalog operator =
          CatalogUtil.buildIcebergCatalog(config.catalogName(), config.catalogProps(), new Configuration());
      ((SupportsNamespaces) operator).createNamespace(TID.namespace());
      Table oldTable = operator.createTable(TID, SCHEMA_V1);
      UUID u1 = oldTable.uuid();
      log("operator: created " + TID + " uuid U1=" + u1 + " location=" + oldTable.location()
          + " schema=" + oneLine(oldTable.schema()));

      // the connector's own catalog (as loaded by the sink task)
      Catalog sinkCatalog = Cr3CurrentCatalogs.load(config);
      TopicPartition ctlTp = new TopicPartition(config.controlTopic(), 0);
      TopicPartition srcTp = new TopicPartition(SRC_TOPIC, 0);
      ControlTopic ctl = new ControlTopic(config.controlTopic());
      MemberDescription member =
          new MemberDescription(
              "member-0", Optional.empty(), "client-0", "host", new MemberAssignment(Set.of(srcTp)));

      List<byte[]> workerResponses;
      boolean alive;
      Long initialCheckpoint = null; // coordinator-group control offset before this run
      MockClientFactory coordClients;
      Coordinator coordinator;
      MockProducer<String, byte[]> coordProducer;
      MockConsumer<String, byte[]> coordConsumer;

      if (path.equals("replay")) {
        // B: the pre-upgrade (1.10.2) connector left an unanswered commit in the control topic
        workerResponses = runLegacyPeer(dir, "pending", List.of());
        workerResponses.forEach(ctl::append);
        log("control topic retained from the 1.10.2 connector: offsets 0.." + (ctl.size() - 1)
            + " (coordinator group checkpoint = 0)");

        dropAndRecreate(operator, u1);

        // the upgraded (current) connector starts; its coordinator resumes from the checkpoint
        coordClients = new MockClientFactory(OffsetResetStrategy.LATEST);
        coordinator = new Coordinator(sinkCatalog, config, List.of(member), coordClients, context(srcTp));
        coordProducer = coordClients.lastProducer();
        coordConsumer = coordClients.lastConsumer();
        coordinator.start();
        coordConsumer.commitSync(Map.of(ctlTp, new OffsetAndMetadata(0L)));
        coordConsumer.rebalance(List.of(ctlTp));
        coordConsumer.updateBeginningOffsets(Map.of(ctlTp, 0L));
        coordConsumer.updateEndOffsets(Map.of(ctlTp, (long) ctl.size()));
        ctl.deliverAll(coordConsumer);
        for (byte[] r : workerResponses) {
          log("coordinator will replay " + describeCurrent(r));
        }

        alive = processLikeThread(coordinator, "upgraded coordinator"); // StartCommit C1 + replay
        byte[] startCommit = coordProducer.history().get(0).value();
        ctl.append(startCommit);
        log("current coordinator sent " + describeCurrent(startCommit));
        // post-upgrade worker (current version, no new records yet) answers C1
        List<byte[]> current = runCurrentWorker(config, List.of(startCommit), 0, srcTp);
        current.forEach(ctl::append);
        ctl.deliverNew(coordConsumer);
        alive = alive && processLikeThread(coordinator, "upgraded coordinator"); // -> commit
        initialCheckpoint = 0L;
      } else {
        // A: rolling upgrade, the coordinator already runs the current version
        coordClients = new MockClientFactory(OffsetResetStrategy.EARLIEST);
        coordinator = new Coordinator(sinkCatalog, config, List.of(member), coordClients, context(srcTp));
        coordProducer = coordClients.lastProducer();
        coordConsumer = coordClients.lastConsumer();
        coordinator.start();
        coordConsumer.rebalance(List.of(ctlTp));
        coordConsumer.updateBeginningOffsets(Map.of(ctlTp, 0L));

        alive = processLikeThread(coordinator, "coordinator"); // interval reached -> StartCommit
        byte[] startCommit = coordProducer.history().get(0).value();
        ctl.append(startCommit);
        ctl.deliverNew(coordConsumer);
        log("current coordinator sent " + describeCurrent(startCommit));

        workerResponses =
            worker.equals("legacy")
                ? runLegacyPeer(dir, "respond", List.of(startCommit))
                : runCurrentWorker(config, List.of(startCommit), 3, srcTp);

        // the table is dropped and re-created while the responses are in flight
        if (replace) {
          dropAndRecreate(operator, u1);
        } else {
          log("operator: (no DROP/CREATE in this scenario)");
        }

        workerResponses.forEach(ctl::append);
        for (byte[] r : workerResponses) {
          log("coordinator consumes " + describeCurrent(r));
        }
        ctl.deliverNew(coordConsumer);
        alive = alive && processLikeThread(coordinator, "coordinator"); // DataComplete -> commit
      }

      // ---- what the coordinator did
      for (ProducerRecord<String, byte[]> rec : coordProducer.history()) {
        log("coordinator produced " + describeCurrent(rec.value()));
      }
      log("coordinator committed control-topic offsets: " + coordProducer.consumerGroupOffsetsHistory());
      Set<String> workerFiles = workerFileLocations(workerResponses);
      boolean leaked = inspect(operator, config, u1, workerFiles, "after the commit cycle");

      if (!alive) {
        // CommitterImpl.processControlEvents now throws NotRunningException -> the task FAILS.
        // Model the operator restarting the failed task: a NEW coordinator resumes from the
        // coordinator group's last committed control-topic offset.
        Long checkpoint = lastCommitted(coordProducer, ctlTp);
        if (checkpoint == null) {
          checkpoint = initialCheckpoint;
        }
        banner(name + ": task restart -> new coordinator resumes from checkpoint " + checkpoint
            + (checkpoint == null ? " (none -> auto.offset.reset=latest)" : ""));
        boolean recovered = restartAndRecover(config, member, ctl, srcTp, ctlTp, checkpoint);
        log("restarted coordinator completed a commit cycle without terminating: " + recovered);
        leaked = inspect(operator, config, u1, workerFiles, "after restart/recovery (final state)");
        if (!replace) {
          return "SEPARATE-DEFECT: coordinator terminated after committing a 1.10.2 DataWritten"
              + " (no table replacement involved); files landed in U1=" + leaked;
        }
      }

      if (!replace) {
        return "no crash";
      }
      if (leaked) {
        return "BUG: file(s) written for dropped table U1 committed into re-created table U2"
            + (alive ? "" : "; coordinator terminated after the append (task failed), restart kept them");
      }
      Table finalTable = operator.loadTable(TID);
      return finalTable.currentSnapshot() == null
          ? "REJECTED: U2 untouched (no snapshot)"
          : "REJECTED-BUT-OTHER-SNAPSHOT";
    }

    /** Prints what a reader of TID now sees; returns true if it references any worker file. */
    boolean inspect(Catalog operator, IcebergSinkConfig config, UUID u1, Set<String> workerFiles,
        String when) throws IOException {
      Table newTable = operator.loadTable(TID);
      log("[" + when + "] table " + TID + " uuid=" + newTable.uuid() + " (U1=" + u1 + ") snapshots="
          + count(newTable.snapshots()));
      List<String> committedFiles = new ArrayList<>();
      Snapshot snap = newTable.currentSnapshot();
      if (snap != null) {
        log("  current snapshot " + snap.snapshotId() + " summary: commit-id="
            + snap.summary().get("kafka.connect.commit-id") + " offsets="
            + snap.summary().get("kafka.connect.offsets." + config.controlTopic() + "." + config.connectGroupId())
            + " added-records=" + snap.summary().get("added-records"));
        try (CloseableIterable<FileScanTask> tasks = newTable.newScan().planFiles()) {
          for (FileScanTask t : tasks) {
            DataFile f = t.file();
            committedFiles.add(f.location());
            boolean exists = newTable.io().newInputFile(f.location()).exists();
            log("  references data file " + f.location() + " records=" + f.recordCount()
                + " specId=" + f.specId() + " exists=" + exists
                + (workerFiles.contains(f.location()) ? "  <== written by the worker for table U1" : ""));
            if (exists) {
              log("    that file's own (writer) schema: " + parquetFileSchema(f.location()));
            }
          }
        }
        log("  table schema:                       " + oneLine(newTable.schema()));
        readTable(newTable);
      }
      return committedFiles.stream().anyMatch(workerFiles::contains);
    }

    /** A restarted task: new Coordinator, resumes from the checkpoint, runs one commit cycle. */
    boolean restartAndRecover(IcebergSinkConfig config, MemberDescription member, ControlTopic ctl,
        TopicPartition srcTp, TopicPartition ctlTp, Long checkpoint) {
      MockClientFactory clients = new MockClientFactory(OffsetResetStrategy.LATEST);
      Catalog catalog = Cr3CurrentCatalogs.load(config);
      Coordinator c2 = new Coordinator(catalog, config, List.of(member), clients, context(srcTp));
      MockProducer<String, byte[]> p2 = clients.lastProducer();
      MockConsumer<String, byte[]> cons2 = clients.lastConsumer();
      c2.start();
      if (checkpoint != null) {
        cons2.commitSync(Map.of(ctlTp, new OffsetAndMetadata(checkpoint)));
      }
      cons2.rebalance(List.of(ctlTp));
      cons2.updateBeginningOffsets(Map.of(ctlTp, 0L));
      cons2.updateEndOffsets(Map.of(ctlTp, (long) ctl.size()));
      ctl.deliverAll(cons2);
      boolean ok = processLikeThread(c2, "restarted coordinator"); // StartCommit C2 (+ replay)
      byte[] startCommit = p2.history().get(0).value();
      ctl.append(startCommit);
      log("restarted coordinator sent " + describeCurrent(startCommit));
      List<byte[]> answers = runCurrentWorker(config, List.of(startCommit), 0, srcTp);
      answers.forEach(ctl::append);
      ctl.deliverNew(cons2);
      ok = ok && processLikeThread(c2, "restarted coordinator");
      for (ProducerRecord<String, byte[]> rec : p2.history()) {
        log("restarted coordinator produced " + describeCurrent(rec.value()));
      }
      log("restarted coordinator committed control-topic offsets: " + p2.consumerGroupOffsetsHistory());
      return ok;
    }

    void dropAndRecreate(Catalog operator, UUID u1) {
      boolean dropped = operator.dropTable(TID, false /* DROP TABLE, no PURGE */);
      Schema s = incompatibleSchema ? SCHEMA_V2_ID_AS_STRING : SCHEMA_V1;
      Table t = operator.createTable(TID, s);
      log("operator: DROP TABLE " + TID + " (purge=false) -> " + dropped + "; CREATE TABLE " + TID
          + " uuid U2=" + t.uuid() + " location=" + t.location() + " schema=" + oneLine(t.schema()));
    }
  }

  // ------------------------------------------------------------------------------------------
  // peers

  static List<byte[]> runLegacyPeer(Path dir, String mode, List<byte[]> toWorker) throws Exception {
    Path job = dir.resolve("job");
    Path in = job.resolve("ctl-in");
    Files.createDirectories(in);
    for (int i = 0; i < toWorker.size(); i++) {
      Files.write(in.resolve(String.format("%03d.bin", i)), toWorker.get(i));
    }
    List<String> cmd =
        List.of(
            javaBin,
            "-cp",
            legacyClasspath,
            "-Dorg.slf4j.simpleLogger.logFile=System.out",
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
            "-Dorg.slf4j.simpleLogger.log.org.apache.iceberg.connect=info",
            "org.apache.iceberg.connect.channel.Cr3LegacyPeer",
            mode,
            job.toString(),
            "3");
    log("spawning legacy 1.10.2 sink JVM: mode=" + mode);
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    try (var reader = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println("    " + line);
      }
    }
    if (!p.waitFor(180, TimeUnit.SECONDS) || p.exitValue() != 0) {
      throw new IllegalStateException("legacy peer failed");
    }
    List<byte[]> out = new ArrayList<>();
    try (Stream<Path> files = Files.list(job.resolve("ctl-out"))) {
      for (Path f : files.sorted().collect(Collectors.toList())) {
        out.add(Files.readAllBytes(f));
      }
    }
    return out;
  }

  /** This checkout's own Worker, answering the given control records (control run). */
  static List<byte[]> runCurrentWorker(
      IcebergSinkConfig config, List<byte[]> toWorker, int records, TopicPartition srcTp) {
    MockClientFactory clients = new MockClientFactory(OffsetResetStrategy.EARLIEST);
    Catalog workerCatalog = Cr3CurrentCatalogs.load(config);
    SinkWriter sinkWriter = new SinkWriter(workerCatalog, config);
    Worker worker = new Worker(config, clients, sinkWriter, context(srcTp));
    MockProducer<String, byte[]> producer = clients.lastProducer();
    MockConsumer<String, byte[]> consumer = clients.lastConsumer();
    worker.start();
    TopicPartition ctlTp = new TopicPartition(config.controlTopic(), 0);
    consumer.rebalance(List.of(ctlTp));
    consumer.updateBeginningOffsets(Map.of(ctlTp, 0L));

    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .name("cr3.Row")
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
    List<SinkRecord> batch = new ArrayList<>();
    for (int i = 0; i < records; i++) {
      Struct value = new Struct(valueSchema).put("id", (long) (i + 1)).put("data", "row-" + (i + 1));
      batch.add(new SinkRecord(SRC_TOPIC, 0, null, null, valueSchema, value, i,
          1_700_000_000_000L + i, TimestampType.CREATE_TIME));
    }
    worker.save(batch);
    log("current worker buffered " + batch.size() + " source records");
    long off = 0;
    for (byte[] bytes : toWorker) {
      consumer.addRecord(new ConsumerRecord<>(config.controlTopic(), 0, off++, "k", bytes));
    }
    worker.process();
    List<byte[]> out = new ArrayList<>();
    for (ProducerRecord<String, byte[]> rec : producer.history()) {
      out.add(rec.value());
      log("current worker sent " + describeCurrent(rec.value()));
    }
    return out;
  }

  /**
   * One iteration of CoordinatorThread.run(): process(); an exception is logged as "Coordinator
   * error during process, exiting thread" and ends the thread (CommitterImpl then fails the task).
   */
  static boolean processLikeThread(Coordinator coordinator, String who) {
    try {
      coordinator.process();
      return true;
    } catch (Exception e) {
      log(who + " THREW -> CoordinatorThread: \"Coordinator error during process, exiting thread\": " + e);
      StackTraceElement[] st = e.getStackTrace();
      for (int i = 0; i < Math.min(st.length, 14); i++) {
        if (st[i].getClassName().startsWith("org.apache.iceberg")) {
          log("      at " + st[i]);
        }
      }
      return false;
    }
  }

  static Long lastCommitted(MockProducer<String, byte[]> producer, TopicPartition tp) {
    Long last = null;
    for (Map<String, Map<TopicPartition, OffsetAndMetadata>> commit : producer.consumerGroupOffsetsHistory()) {
      for (Map<TopicPartition, OffsetAndMetadata> offsets : commit.values()) {
        if (offsets.containsKey(tp)) {
          last = offsets.get(tp).offset();
        }
      }
    }
    return last;
  }

  static WorkerSinkTaskContext context(TopicPartition srcTp) {
    MockConsumer<byte[], byte[]> source = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    source.assign(List.of(srcTp));
    return new WorkerSinkTaskContext(source, null, null);
  }

  // ------------------------------------------------------------------------------------------
  // inspection helpers

  static void readTable(Table table) {
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      int n = 0;
      for (Record r : rows) {
        n++;
        StringBuilder sb = new StringBuilder("  row: ");
        for (Types.NestedField f : table.schema().columns()) {
          Object v = r.getField(f.name());
          sb.append(f.name()).append('[').append(f.type()).append("]=").append(v)
              .append(" (java ").append(v == null ? "null" : v.getClass().getSimpleName()).append(") ");
        }
        log(sb.toString());
        Types.NestedField first = table.schema().columns().get(0);
        if (first.type().typeId() == org.apache.iceberg.types.Type.TypeID.STRING) {
          try {
            CharSequence typed = r.get(0, CharSequence.class);
            log("  typed read of string column '" + first.name() + "': " + typed);
          } catch (RuntimeException e) {
            log("  typed read of string column '" + first.name() + "' FAILED: " + e);
          }
        }
      }
      log("IcebergGenerics.read(U2) returned " + n + " row(s)");
    } catch (Exception e) {
      log("IcebergGenerics.read(U2) FAILED: " + e);
      Throwable c = e.getCause();
      while (c != null) {
        log("  caused by: " + c);
        c = c.getCause();
      }
    }
  }

  static String parquetFileSchema(String location) {
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(location), new Configuration()))) {
      return oneLine(ParquetSchemaUtil.convert(reader.getFooter().getFileMetaData().getSchema()));
    } catch (IOException e) {
      return "<unreadable: " + e + ">";
    }
  }

  static Set<String> workerFileLocations(List<byte[]> responses) {
    return responses.stream()
        .map(AvroUtil::decode)
        .filter(e -> e.payload() instanceof DataWritten)
        .flatMap(e -> ((DataWritten) e.payload()).dataFiles().stream())
        .map(DataFile::location)
        .collect(Collectors.toSet());
  }

  /** How THIS checkout (the coordinator) decodes a control record. */
  static String describeCurrent(byte[] bytes) {
    Event event = AvroUtil.decode(bytes);
    StringBuilder sb = new StringBuilder(event.type().name());
    Object p = event.payload();
    if (p instanceof StartCommit) {
      sb.append(" commitId=").append(((StartCommit) p).commitId());
    } else if (p instanceof DataWritten) {
      DataWritten dw = (DataWritten) p;
      sb.append(" commitId=").append(dw.commitId()).append(" tableReference={identifier=")
          .append(dw.tableReference().identifier()).append(", uuid=").append(dw.tableReference().uuid())
          .append("} files=").append(dw.dataFiles().stream().map(DataFile::location).collect(Collectors.toList()));
    } else if (p instanceof DataComplete) {
      sb.append(" commitId=").append(((DataComplete) p).commitId());
    } else if (p instanceof CommitToTable) {
      CommitToTable c = (CommitToTable) p;
      sb.append(" commitId=").append(c.commitId()).append(" table=").append(c.tableReference().identifier())
          .append(" tableReference.uuid=").append(c.tableReference().uuid()).append(" snapshotId=").append(c.snapshotId());
    } else if (p instanceof CommitComplete) {
      sb.append(" commitId=").append(((CommitComplete) p).commitId());
    }
    return sb.toString();
  }

  static String oneLine(Schema schema) {
    return schema.columns().stream()
        .map(f -> f.fieldId() + ":" + f.name() + " " + f.type())
        .collect(Collectors.joining(", ", "(", ")"));
  }

  static long count(Iterable<?> it) {
    long n = 0;
    for (Object ignored : it) {
      n++;
    }
    return n;
  }

  static Map<String, String> connectorProps(String catalogType, Path dir) {
    Map<String, String> m = new HashMap<>();
    m.put("name", "cr3-sink");
    m.put("task.id", "0");
    m.put("topics", SRC_TOPIC);
    m.put("iceberg.tables", TID.toString());
    m.put("iceberg.control.topic", "control-iceberg");
    m.put("iceberg.control.commit.interval-ms", "0");
    m.put("iceberg.control.commit.timeout-ms", "600000");
    m.put("iceberg.control.commit.threads", "1");
    String warehouse = "file:" + dir.resolve("warehouse").toAbsolutePath();
    if (catalogType.equals("jdbc")) {
      m.put("iceberg.catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog");
      m.put("iceberg.catalog.uri", "jdbc:sqlite:file:" + dir.resolve("catalog.db").toAbsolutePath());
      m.put("iceberg.catalog.jdbc.schema-version", "V1");
    } else {
      m.put("iceberg.catalog.type", "hadoop");
    }
    m.put("iceberg.catalog.warehouse", warehouse);
    return m;
  }

  static void deleteRecursively(Path p) throws IOException {
    if (!Files.exists(p)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(p)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  static void banner(String s) {
    System.out.println();
    System.out.println("==================== " + s + " ====================");
    System.out.flush();
  }

  static void log(String msg) {
    System.out.println("[current] " + msg);
    System.out.flush();
  }

  /** In-memory model of the single-partition control topic (offset-ordered log). */
  static final class ControlTopic {
    private final String topic;
    private final List<byte[]> records = new ArrayList<>();
    private final Map<MockConsumer<String, byte[]>, Integer> delivered = new HashMap<>();

    ControlTopic(String topic) {
      this.topic = topic;
    }

    void append(byte[] value) {
      records.add(value);
    }

    int size() {
      return records.size();
    }

    void deliverAll(MockConsumer<String, byte[]> consumer) {
      delivered.put(consumer, 0);
      deliverNew(consumer);
    }

    void deliverNew(MockConsumer<String, byte[]> consumer) {
      int from = delivered.getOrDefault(consumer, 0);
      for (int i = from; i < records.size(); i++) {
        consumer.addRecord(new ConsumerRecord<>(topic, 0, i, "k", records.get(i)));
      }
      delivered.put(consumer, records.size());
    }
  }

  /** Kafka clients replaced by Kafka's MockProducer / MockConsumer (environment only). */
  static class MockClientFactory extends KafkaClientFactory {
    private final OffsetResetStrategy reset;
    private final List<MockProducer<String, byte[]>> producers = new ArrayList<>();
    private final List<MockConsumer<String, byte[]>> consumers = new ArrayList<>();

    MockClientFactory(OffsetResetStrategy reset) {
      super(new HashMap<>());
      this.reset = reset;
    }

    @Override
    Producer<String, byte[]> createProducer(String transactionalId) {
      MockProducer<String, byte[]> p =
          new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
      p.initTransactions();
      producers.add(p);
      return p;
    }

    @Override
    Consumer<String, byte[]> createConsumer(String consumerGroupId) {
      MockConsumer<String, byte[]> c = new MockConsumer<>(reset);
      consumers.add(c);
      return c;
    }

    @Override
    Admin createAdmin() {
      return (Admin)
          Proxy.newProxyInstance(
              Admin.class.getClassLoader(),
              new Class<?>[] {Admin.class},
              (proxy, method, a) -> {
                if (method.getName().equals("close")) {
                  return null;
                }
                throw new UnsupportedOperationException(method.getName());
              });
    }

    MockProducer<String, byte[]> lastProducer() {
      return producers.get(producers.size() - 1);
    }

    MockConsumer<String, byte[]> lastConsumer() {
      return consumers.get(consumers.size() - 1);
    }
  }
}
