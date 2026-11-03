package org.apache.iceberg.r5;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/** Test fixtures: in-memory catalog, a FaultOps-wrapped table, Avro data files, puffin DVs. */
public final class R5Tables {
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final AtomicLong SEQ = new AtomicLong();
  static final Map<String, Table> REGISTRY = new ConcurrentHashMap<>();

  private R5Tables() {}

  public static final class Handle {
    public final InMemoryCatalog catalog;
    public final TableIdentifier id;
    public final FaultOps ops;
    public final Table table;

    Handle(InMemoryCatalog catalog, TableIdentifier id, FaultOps ops, Table table) {
      this.catalog = catalog;
      this.id = id;
      this.ops = ops;
      this.table = table;
    }

    /** A fresh Table over the same (fault-injecting) ops, like a reloaded table. */
    public Table reload() {
      Table t = new BaseTable(ops, id.toString());
      t.refresh();
      return t;
    }
  }

  public static Handle create(int formatVersion) {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("mem" + SEQ.incrementAndGet(), ImmutableMap.of());
    catalog.createNamespace(Namespace.of("db"));
    TableIdentifier id = TableIdentifier.of("db", "t" + SEQ.incrementAndGet());
    Table real =
        catalog.createTable(
            id,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
                TableProperties.DEFAULT_FILE_FORMAT, "avro"));
    FaultOps ops = new FaultOps(((HasTableOperations) real).operations());
    Table table = new BaseTable(ops, id.toString());
    return new Handle(catalog, id, ops, table);
  }

  public static DataFile writeData(Table table, int firstId, int count) throws IOException {
    OutputFileFactory files =
        OutputFileFactory.builderFor(table, 1, SEQ.incrementAndGet())
            .format(FileFormat.AVRO)
            .build();
    DataWriter<Record> writer =
        Avro.writeData(files.newOutputFile())
            .schema(SCHEMA)
            .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
            .withSpec(table.spec())
            .overwrite()
            .build();
    try (writer) {
      for (int i = 0; i < count; i++) {
        GenericRecord r = GenericRecord.create(SCHEMA);
        r.setField("id", firstId + i);
        r.setField("data", "v" + (firstId + i));
        writer.write(r);
      }
    }
    return writer.toDataFile();
  }

  public static List<DeleteFile> writeDV(Table table, DataFile dataFile, long... positions)
      throws IOException {
    OutputFileFactory files =
        OutputFileFactory.builderFor(table, 1, SEQ.incrementAndGet())
            .format(FileFormat.PUFFIN)
            .build();
    BaseDVFileWriter writer = new BaseDVFileWriter(files, path -> null);
    try (writer) {
      for (long pos : positions) {
        writer.delete(dataFile.location(), pos, table.spec(), null);
      }
    }
    return writer.result().deleteFiles();
  }

  /** InMemoryFileIO.newInputFile throws NotFoundException for a missing file. */
  public static boolean exists(org.apache.iceberg.io.FileIO io, String location) {
    try {
      return io.newInputFile(location).exists();
    } catch (org.apache.iceberg.exceptions.NotFoundException e) {
      return false;
    }
  }

  /** Reads all rows. Returns the sorted ids, or throws whatever the read throws. */
  public static List<Integer> readIds(Table table) throws IOException {
    List<Integer> ids = Lists.newArrayList();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record r : rows) {
        ids.add((Integer) r.getField("id"));
      }
    }
    ids.sort(Integer::compare);
    return ids;
  }

  /** A TableLoader that hands out tables registered in this JVM (the harness is in-process). */
  public static TableLoader loader(String key, Table table) {
    REGISTRY.put(key, table);
    return new RegistryLoader(key);
  }

  static final class RegistryLoader implements TableLoader {
    private final String key;
    private boolean open;

    RegistryLoader(String key) {
      this.key = key;
    }

    @Override
    public void open() {
      open = true;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public Table loadTable() {
      Table t = REGISTRY.get(key);
      if (t == null) {
        throw new IllegalStateException("unknown table key " + key);
      }
      return t;
    }

    @Override
    public TableLoader clone() {
      return new RegistryLoader(key);
    }

    @Override
    public void close() {
      open = false;
    }
  }
}
