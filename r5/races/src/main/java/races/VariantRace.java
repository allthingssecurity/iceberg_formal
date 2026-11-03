package races;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

/**
 * B-122. W writer threads each write one Avro data file into the same format-v3 table through the
 * real Iceberg Avro data writer (Avro.writeData + org.apache.iceberg.data.avro.DataWriter, the path
 * GenericAppenderFactory / GenericFileWriterFactory use). Every row carries a variant whose value
 * is a deterministic function of (writer, row). All files are appended and the table is read back
 * with IcebergGenerics; each read variant is re-serialized into a fresh buffer and compared
 * byte-for-byte with the expected value serialized into a fresh buffer.
 *
 * <p>args: mode(concurrent|sequential) trials writers rows
 */
public class VariantRace {
  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "writer", Types.IntegerType.get()),
          Types.NestedField.required(2, "row", Types.IntegerType.get()),
          Types.NestedField.optional(3, "v", Types.VariantType.get()));

  static final VariantMetadata META = Variants.emptyMetadata();

  static String text(int writer, int row) {
    // length varies 1..3000 so some values exceed the initial 2048-byte buffer (ensureCapacity
    // reallocation path) and lengths differ between writers.
    int len = 1 + ((row * 7919 + writer * 104729) % 3000);
    char c = (char) ('A' + writer);
    StringBuilder sb = new StringBuilder(len);
    sb.append(writer).append(':').append(row).append(':');
    while (sb.length() < len) {
      sb.append(c);
    }
    return sb.toString();
  }

  static String shape = "varied";

  static VariantValue expected(int writer, int row) {
    if (shape.equals("fixed")) {
      // every value is a 64-char string: a race swaps content but keeps lengths consistent
      String s = String.format("%d:%08d:", writer, row);
      StringBuilder sb = new StringBuilder(s);
      char c = (char) ('A' + writer);
      while (sb.length() < 64) {
        sb.append(c);
      }
      return Variants.of(sb.toString());
    }
    if (row % 5 == 0) {
      return Variants.of((long) writer * 1_000_000_000L + row);
    }
    return Variants.of(text(writer, row));
  }

  static byte[] bytes(VariantValue v) {
    ByteBuffer b = ByteBuffer.allocate(v.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    int n = v.writeTo(b, 0);
    return Arrays.copyOf(b.array(), n);
  }

  public static void main(String[] args) throws Exception {
    String mode = args[0];
    if (!mode.equals("concurrent") && !mode.equals("sequential")) {
      throw new IllegalArgumentException("unknown mode " + mode);
    }
    int trials = Integer.parseInt(args[1]);
    int writers = Integer.parseInt(args[2]);
    int rows = Integer.parseInt(args[3]);
    shape = args.length > 4 ? args[4] : "varied";
    if (!shape.equals("varied") && !shape.equals("fixed")) {
      throw new IllegalArgumentException("unknown shape " + shape);
    }

    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("mem", Map.of());
    catalog.createNamespace(Namespace.of("db"));

    int badTrials = 0;
    long badRows = 0;
    long missingRows = 0;
    int writeFailures = 0;
    int readFailures = 0;
    int silentFiles = 0;
    int cleanFiles = 0;
    Map<String, Integer> exceptions = new TreeMap<>();
    String firstExample = null;
    Throwable firstReadError = null;
    Throwable firstWriteError = null;

    for (int trial = 0; trial < trials; trial++) {
      TableIdentifier id = TableIdentifier.of("db", "v" + trial);
      Table table =
          catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), Map.of("format-version", "3"));
      DataFile[] files = new DataFile[writers];
      Throwable[] errors = new Throwable[writers];
      CyclicBarrier barrier = new CyclicBarrier(mode.equals("concurrent") ? writers : 1);
      final int t = trial;
      List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int writer = w;
        Runnable body =
            () -> {
              try {
                org.apache.iceberg.io.DataWriter<Record> dw =
                    Avro.writeData(
                            table.io().newOutputFile(table.location() + "/data/w" + writer + "-t" + t + ".avro"))
                        .schema(table.schema())
                        .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
                        .withSpec(table.spec())
                        .overwrite()
                        .build();
                barrier.await();
                GenericRecord rec = GenericRecord.create(table.schema());
                try {
                  for (int r = 0; r < rows; r++) {
                    rec.set(0, writer);
                    rec.set(1, r);
                    rec.set(2, Variant.of(META, expected(writer, r)));
                    dw.write(rec);
                  }
                } finally {
                  dw.close();
                }
                files[writer] = dw.toDataFile();
              } catch (Throwable e) {
                errors[writer] = e;
              }
            };
        if (mode.equals("concurrent")) {
          Thread th = new Thread(body, "writer-" + w);
          threads.add(th);
          th.start();
        } else {
          body.run();
        }
      }
      for (Thread th : threads) {
        th.join();
      }
      boolean trialBad = false;
      for (int w = 0; w < writers; w++) {
        Throwable e = errors[w];
        if (e != null) {
          writeFailures++;
          trialBad = true;
          exceptions.merge("write:" + e.getClass().getName(), 1, Integer::sum);
          if (firstWriteError == null) {
            firstWriteError = e;
          }
          continue;
        }
        // each file is committed to its own table so a broken file cannot hide another file's result
        TableIdentifier fid = TableIdentifier.of("db", "v" + trial + "_w" + w);
        Table ft =
            catalog.createTable(fid, SCHEMA, PartitionSpec.unpartitioned(), Map.of("format-version", "3"));
        ft.newAppend().appendFile(files[w]).commit();
        long bad = 0;
        long seen = 0;
        boolean unreadable = false;
        try (CloseableIterable<Record> read = IcebergGenerics.read(ft).build()) {
          for (Record rec : read) {
            seen++;
            int writer = (Integer) rec.getField("writer");
            int row = (Integer) rec.getField("row");
            Variant v = (Variant) rec.getField("v");
            byte[] want = bytes(expected(writer, row));
            byte[] got = bytes(v.value());
            if (writer != w || !Arrays.equals(want, got)) {
              bad++;
              if (firstExample == null) {
                firstExample =
                    "trial=" + trial + " file-of-writer=" + w + " writer=" + writer + " row=" + row
                        + " wantLen=" + want.length + " gotLen=" + got.length + " got="
                        + abbreviate(v.value().toString()) + " want="
                        + abbreviate(expected(writer, row).toString());
              }
            }
          }
        } catch (RuntimeException ex) {
          unreadable = true;
          exceptions.merge("read:" + ex.getClass().getName(), 1, Integer::sum);
          if (firstReadError == null) {
            firstReadError = ex;
          }
        } catch (java.io.IOException ex) {
          throw new java.io.UncheckedIOException(ex);
        }
        if (unreadable) {
          readFailures++;
          trialBad = true;
        } else if (bad > 0 || seen != rows) {
          silentFiles++;
          trialBad = true;
          badRows += bad;
          missingRows += rows - seen;
        } else {
          cleanFiles++;
        }
        catalog.dropTable(fid, true);
      }
      if (trialBad) {
        badTrials++;
      }
      catalog.dropTable(id, true);
    }
    System.out.printf(
        "RESULT version=%s shape=" + shape + " mode=%s trials=%d writers=%d rows=%d files=%d badTrials=%d "
            + "cleanFiles=%d silentlyWrongFiles=%d wrongRowsInSilentFiles=%d missingRowsInSilentFiles=%d "
            + "unreadableFiles=%d writeFailedFiles=%d exceptions=%s%n",
        System.getProperty("icebergVersion"), mode, trials, writers, rows, trials * writers,
        badTrials, cleanFiles, silentFiles, badRows, missingRows, readFailures, writeFailures,
        exceptions);
    if (firstExample != null) {
      System.out.println("EXAMPLE " + firstExample);
    }
    printShort("FIRST_WRITE_ERROR", firstWriteError);
    printShort("FIRST_READ_ERROR", firstReadError);
  }

  static void printShort(String label, Throwable e) {
    int depth = 0;
    while (e != null && depth < 4) {
      System.out.println(label + " " + e.getClass().getName() + ": " + abbreviate(String.valueOf(e.getMessage())));
      StackTraceElement[] st = e.getStackTrace();
      for (int i = 0; i < Math.min(6, st.length); i++) {
        System.out.println("    at " + st[i]);
      }
      e = e.getCause();
      depth++;
    }
  }

  static String abbreviate(String s) {
    return s.length() <= 80 ? s : s.substring(0, 80) + "...(" + s.length() + ")";
  }
}
