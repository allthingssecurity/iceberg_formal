package org.apache.iceberg.connect.channel;

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Cr3LegacyCatalogs;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
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

/**
 * CR-3 legacy peer. Runs the RELEASED Apache Iceberg 1.10.2 Kafka Connect sink classes,
 * unmodified, from the official Maven Central jars: Worker, SinkWriter, IcebergWriterFactory,
 * IcebergWriter, AvroUtil, and in mode "pending" also the 1.10.2 Coordinator. The Kafka brokers
 * are replaced by Kafka's own MockProducer/MockConsumer test clients. The task context is the
 * real Connect runtime class WorkerSinkTaskContext, wrapping the task's source-topic consumer.
 *
 * <p>Modes:
 *
 * <ul>
 *   <li>respond: the 1.10.2 worker receives the control records in job/ctl-in (the StartCommit
 *       of a NEWER coordinator), buffers its source records and answers. This is the rolling
 *       upgrade case.
 *   <li>pending: the 1.10.2 coordinator starts a commit and the 1.10.2 worker answers it. The
 *       connector is then stopped for the upgrade before the coordinator commits. This is the
 *       "existing messages in the control topic" upgrade case of apache/iceberg#15011.
 * </ul>
 *
 * Every control record this JVM produces is written, in order, to job/ctl-out/NNN.bin.
 */
public class Cr3LegacyPeer {
  private static final String SRC_TOPIC = "src-topic";

  public static void main(String[] args) throws Exception {
    String mode = args[0];
    Path job = Paths.get(args[1]);
    int records = Integer.parseInt(args[2]);

    Map<String, String> props = loadProps(job.resolve("connector.properties"));
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    log(
        "running Iceberg Kafka Connect "
            + org.apache.iceberg.IcebergBuild.version()
            + " from "
            + Worker.class.getProtectionDomain().getCodeSource().getLocation());
    log("mode=" + mode + " connectGroupId=" + config.connectGroupId());

    Catalog catalog = Cr3LegacyCatalogs.load(config);
    log("catalog=" + catalog.getClass().getName() + " name=" + catalog.name());

    // the Connect runtime's task context, wrapping the task's source-topic consumer
    MockConsumer<byte[], byte[]> sourceConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    TopicPartition srcTp = new TopicPartition(SRC_TOPIC, 0);
    sourceConsumer.assign(List.of(srcTp));
    WorkerSinkTaskContext context = new WorkerSinkTaskContext(sourceConsumer, null, null);

    MockClientFactory clients = new MockClientFactory();
    TopicPartition ctlTp = new TopicPartition(config.controlTopic(), 0);
    List<byte[]> produced = new ArrayList<>();

    Coordinator legacyCoordinator = null;
    MockProducer<String, byte[]> coordProducer = null;
    if ("pending".equals(mode)) {
      MemberDescription member =
          new MemberDescription(
              "member-0", Optional.empty(), "client-0", "host", new MemberAssignment(Set.of(srcTp)));
      legacyCoordinator =
          new Coordinator(catalog, config, List.of(member), clients, context);
      coordProducer = clients.lastProducer();
      MockConsumer<String, byte[]> coordConsumer = clients.lastConsumer();
      legacyCoordinator.start();
      coordConsumer.rebalance(List.of(ctlTp));
      coordConsumer.updateBeginningOffsets(Map.of(ctlTp, 0L));
      legacyCoordinator.process(); // commit interval reached -> 1.10.2 StartCommit
      produced.add(coordProducer.history().get(0).value());
      log("1.10.2 coordinator sent " + describe(produced.get(0)));
    }

    SinkWriter sinkWriter = new SinkWriter(catalog, config);
    Worker worker = new Worker(config, clients, sinkWriter, context);
    MockProducer<String, byte[]> workerProducer = clients.lastProducer();
    MockConsumer<String, byte[]> workerConsumer = clients.lastConsumer();
    worker.start();
    workerConsumer.rebalance(List.of(ctlTp));

    // the task's put(): records consumed from the source topic while the commit interval elapses
    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .name("cr3.Row")
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
    List<SinkRecord> batch = new ArrayList<>();
    for (int i = 0; i < records; i++) {
      Struct value = new Struct(valueSchema).put("id", (long) (i + 1)).put("data", "row-" + (i + 1));
      batch.add(
          new SinkRecord(
              SRC_TOPIC,
              0,
              null,
              null,
              valueSchema,
              value,
              i,
              1_700_000_000_000L + i,
              TimestampType.CREATE_TIME));
    }
    worker.save(batch);
    log("1.10.2 worker buffered " + batch.size() + " source records (src-topic:0 offsets 0.." + (records - 1) + ")");

    // deliver the control-topic records the worker would consume
    List<byte[]> toWorker = new ArrayList<>();
    if ("pending".equals(mode)) {
      toWorker.add(produced.get(0));
    } else {
      try (Stream<Path> files = Files.list(job.resolve("ctl-in"))) {
        for (Path f : files.sorted().collect(Collectors.toList())) {
          toWorker.add(Files.readAllBytes(f));
        }
      }
    }
    workerConsumer.updateBeginningOffsets(Map.of(ctlTp, 0L));
    long off = 0;
    for (byte[] bytes : toWorker) {
      workerConsumer.addRecord(new ConsumerRecord<>(config.controlTopic(), 0, off++, "k", bytes));
      log("1.10.2 worker consumed " + describe(bytes));
    }
    worker.process(); // receive(StartCommit) -> completeWrite -> DataWritten + DataComplete

    for (ProducerRecord<String, byte[]> rec : workerProducer.history()) {
      produced.add(rec.value());
      log("1.10.2 worker sent " + describe(rec.value()));
    }
    log("1.10.2 worker committed source offsets in its transaction: "
        + workerProducer.consumerGroupOffsetsHistory());
    if (legacyCoordinator != null) {
      log("connector stopped for the upgrade before the 1.10.2 coordinator committed;"
          + " its responses remain in the control topic past the coordinator group offset");
    }

    Path out = job.resolve("ctl-out");
    Files.createDirectories(out);
    for (int i = 0; i < produced.size(); i++) {
      Files.write(out.resolve(String.format("%03d.bin", i)), produced.get(i));
    }
    log("wrote " + produced.size() + " control record(s) to " + out);
    System.exit(0);
  }

  /** Human-readable summary of an encoded control event, decoded with the 1.10.2 classes. */
  static String describe(byte[] bytes) throws IOException {
    Event event = AvroUtil.decode(bytes);
    StringBuilder sb = new StringBuilder(event.type().name());
    switch (event.type()) {
      case START_COMMIT:
        sb.append(" commitId=").append(((StartCommit) event.payload()).commitId());
        break;
      case DATA_WRITTEN:
        DataWritten dw = (DataWritten) event.payload();
        sb.append(" commitId=").append(dw.commitId())
            .append(" tableReference={catalog=").append(dw.tableReference().catalog())
            .append(", identifier=").append(dw.tableReference().identifier())
            .append("} writerSchemaHasTableUuidField=").append(writerSchema(bytes).contains("table_uuid"));
        for (DataFile f : dw.dataFiles()) {
          sb.append(" dataFile=").append(f.location()).append(" (").append(f.recordCount()).append(" rows)");
        }
        break;
      case DATA_COMPLETE:
        DataComplete dc = (DataComplete) event.payload();
        sb.append(" commitId=").append(dc.commitId()).append(" assignments=")
            .append(dc.assignments().stream().map(a -> a.topic() + ":" + a.partition() + "@" + a.offset())
                .collect(Collectors.toList()));
        break;
      default:
        break;
    }
    return sb.toString();
  }

  /** The Avro writer schema embedded by AvroEncoderUtil (2 magic bytes, then writeUTF(schema)). */
  static String writerSchema(byte[] bytes) throws IOException {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      in.readByte();
      in.readByte();
      return in.readUTF();
    }
  }

  static Map<String, String> loadProps(Path file) throws IOException {
    Properties p = new Properties();
    try (InputStream in = Files.newInputStream(file)) {
      p.load(in);
    }
    Map<String, String> m = new HashMap<>();
    p.stringPropertyNames().forEach(k -> m.put(k, p.getProperty(k)));
    return m;
  }

  static void log(String msg) {
    System.out.println("[legacy-1.10.2] " + msg);
    System.out.flush();
  }

  /** Kafka clients replaced by Kafka's MockProducer / MockConsumer (environment only). */
  static class MockClientFactory extends KafkaClientFactory {
    private final List<MockProducer<String, byte[]>> producers = new ArrayList<>();
    private final List<MockConsumer<String, byte[]>> consumers = new ArrayList<>();

    MockClientFactory() {
      super(new HashMap<>());
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
      MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
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
