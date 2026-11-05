/*
 * MC-1 reproduction helper: the LEGACY (<= 1.10) worker's answer to one StartCommit.
 *
 * Compiled and run ONLY against the released Apache Iceberg 1.10.0 jars
 * (org.apache.iceberg:iceberg-kafka-connect-events:1.10.0, iceberg-core/api/data/parquet:1.10.0),
 * in its own JVM, so every class used here is the released 1.10.0 class.
 *
 * It does what a 1.10.0 task does between put() and its StartCommit answer:
 *  - writes the task's buffered source records to a Parquet data file in the table's data dir
 *    (1.10.0 generic Parquet appender), and
 *  - builds the answer events exactly like 1.10.0 Worker.receive (Worker.java:96-109):
 *      new Event(connectGroupId, new DataWritten(partitionStruct, commitId,
 *          TableReference.of(config.catalogName(), tableIdentifier), dataFiles, deleteFiles))
 *      new Event(connectGroupId, new DataComplete(commitId, assignments))
 *    and encodes them with 1.10.0 AvroUtil.encode, the bytes 1.10.0 Channel.send puts on the
 *    control topic (Channel.java:89). (1.10.0 Channel.send commits the source offsets in the same
 *    Kafka transaction, Channel.java:96-105.)
 *
 * Usage: LegacyPeer110 <outDir> <connectGroupId> <catalogName> <namespace> <table>
 *            <schemaJsonFile> <dataFilePath|-> <commitId> <srcTopic> <srcPartition>
 *            <firstOffset> <count>
 * Writes <outDir>/dw.bin (only if count > 0) and <outDir>/dc.bin.
 */
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

public class LegacyPeer110 {
  public static void main(String[] args) throws Exception {
    if (args.length != 12) {
      throw new IllegalArgumentException("expected 12 args, got " + args.length);
    }
    File outDir = new File(args[0]);
    String connectGroupId = args[1];
    String catalogName = args[2];
    String namespace = args[3];
    String tableName = args[4];
    Schema schema =
        SchemaParser.fromJson(new String(Files.readAllBytes(Paths.get(args[5])), StandardCharsets.UTF_8));
    String dataFilePath = args[6];
    UUID commitId = UUID.fromString(args[7]);
    String srcTopic = args[8];
    int srcPartition = Integer.parseInt(args[9]);
    long firstOffset = Long.parseLong(args[10]);
    int count = Integer.parseInt(args[11]);

    outDir.mkdirs();
    PartitionSpec spec = PartitionSpec.unpartitioned();
    OffsetDateTime ts = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

    // DataComplete offset = next offset to consume (SinkWriter stores record offset + 1), or null
    Long nextOffset = null;
    if (count > 0) {
      OutputFile out = org.apache.iceberg.Files.localOutput(new File(dataFilePath));
      FileAppender<Record> appender =
          new GenericAppenderFactory(schema, spec).newAppender(out, FileFormat.PARQUET);
      try {
        for (long off = firstOffset; off < firstOffset + count; off++) {
          GenericRecord rec = GenericRecord.create(schema);
          rec.setField("id", 1_000_000L * srcPartition + off);
          rec.setField("data", "legacy-1.10.0 " + srcTopic + "-" + srcPartition + "@" + off);
          appender.add(rec);
        }
      } finally {
        appender.close();
      }
      DataFile dataFile =
          DataFiles.builder(spec)
              .withPath(out.location())
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(appender.length())
              .withMetrics(appender.metrics())
              .withSplitOffsets(appender.splitOffsets())
              .build();
      List<DataFile> dataFiles = new ArrayList<>();
      dataFiles.add(dataFile);
      nextOffset = firstOffset + count;

      // exactly as 1.10.0 Worker.java:96-106 (partitionStruct of an unpartitioned table = empty struct)
      Event dataWritten =
          new Event(
              connectGroupId,
              new DataWritten(
                  spec.partitionType(),
                  commitId,
                  TableReference.of(catalogName, TableIdentifier.of(Namespace.of(namespace), tableName)),
                  dataFiles,
                  Collections.emptyList()));
      Files.write(new File(outDir, "dw.bin").toPath(), AvroUtil.encode(dataWritten));
      System.out.println(
          "[legacy-1.10.0 peer] DataWritten table="
              + namespace + "." + tableName
              + " file=" + dataFile.path()
              + " records=" + dataFile.recordCount());
    }

    List<TopicPartitionOffset> assignments = new ArrayList<>();
    assignments.add(new TopicPartitionOffset(srcTopic, srcPartition, nextOffset, nextOffset == null ? null : ts));
    Event dataComplete = new Event(connectGroupId, new DataComplete(commitId, assignments));
    Files.write(new File(outDir, "dc.bin").toPath(), AvroUtil.encode(dataComplete));

    // provenance: which Iceberg build produced these bytes, and the 1.10.0 TableReference schema
    System.out.println(
        "[legacy-1.10.0 peer] IcebergBuild.version()=" + IcebergBuild.version()
            + " TableReference fields=" + TableReference.ICEBERG_SCHEMA.fields()
            + " DataComplete " + srcTopic + "-" + srcPartition + " offset=" + nextOffset
            + " commitId=" + commitId);
  }
}
