package races;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

/**
 * Calibration gate for apache/iceberg#16686. Drives the real ManifestFilterManager.filterManifests
 * through a real MergingSnapshotProducer (StreamingDelete via table.newDelete()) on an in-memory
 * table with M manifests, each holding K distinct data files that appear twice (so K duplicate
 * deletes per manifest). A multi-thread scanManifestsWith pool filters the manifests in parallel.
 *
 * <p>Oracle (independent of the code under test): deleted-data-files must be M*K and
 * deleted-duplicate-files must be M*K. Only one commit attempt is made per trial (no concurrent
 * writers, so no retry), which rules out the separate retry double-count part of #16686.
 *
 * <p>args: trials manifests filesPerManifest threads
 */
public class Calibration {
  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  /** Pool that measures how many of its tasks actually overlapped. */
  static class MeasuringPool extends ThreadPoolExecutor {
    final AtomicInteger active = new AtomicInteger();
    final AtomicInteger maxActive = new AtomicInteger();

    MeasuringPool(int n) {
      super(n, n, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      int now = active.incrementAndGet();
      maxActive.accumulateAndGet(now, Math::max);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      active.decrementAndGet();
    }
  }

  public static void main(String[] args) throws Exception {
    int trials = args.length > 0 ? Integer.parseInt(args[0]) : 50;
    int manifests = args.length > 1 ? Integer.parseInt(args[1]) : 32;
    int k = args.length > 2 ? Integer.parseInt(args[2]) : 2000;
    int threads = args.length > 3 ? Integer.parseInt(args[3]) : 8;
    String version = System.getProperty("icebergVersion");

    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("mem", Map.of());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));

    long expected = (long) manifests * k;
    int wrongDup = 0;
    int wrongDeleted = 0;
    int threw = 0;
    int hung = 0;
    long minDup = Long.MAX_VALUE;
    long maxDup = Long.MIN_VALUE;
    int minOverlap = Integer.MAX_VALUE;
    Map<String, Integer> exceptions = new TreeMap<>();

    ExecutorService driver = java.util.concurrent.Executors.newSingleThreadExecutor();
    for (int trial = 0; trial < trials; trial++) {
      Table table =
          catalog.createTable(
              TableIdentifier.of("db", "t" + trial),
              SCHEMA,
              PartitionSpec.unpartitioned(),
              Map.of("format-version", "2"));
      // one fast append per manifest; each manifest has K files, each file written twice
      for (int m = 0; m < manifests; m++) {
        ManifestFile mf = writeManifest(table, trial, m, k);
        table.newFastAppend().appendManifest(mf).commit();
      }
      int snapshotsBefore = size(table.snapshots());

      MeasuringPool pool = new MeasuringPool(threads);
      Future<?> f =
          driver.submit(
              () ->
                  table
                      .newDelete()
                      .deleteFromRowFilter(Expressions.alwaysTrue())
                      .scanManifestsWith(pool)
                      .commit());
      try {
        f.get(120, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        hung++;
        f.cancel(true);
        pool.shutdownNow();
        continue;
      } catch (java.util.concurrent.ExecutionException e) {
        threw++;
        exceptions.merge(e.getCause().getClass().getName(), 1, Integer::sum);
        pool.shutdownNow();
        continue;
      }
      pool.shutdown();
      minOverlap = Math.min(minOverlap, pool.maxActive.get());

      table.refresh();
      int snapshotsAfter = size(table.snapshots());
      if (snapshotsAfter != snapshotsBefore + 1) {
        throw new IllegalStateException("expected exactly one new snapshot");
      }
      Snapshot s = table.currentSnapshot();
      Map<String, String> summary = s.summary();
      long dup = Long.parseLong(summary.getOrDefault("deleted-duplicate-files", "0"));
      long del = Long.parseLong(summary.getOrDefault("deleted-data-files", "0"));
      minDup = Math.min(minDup, dup);
      maxDup = Math.max(maxDup, dup);
      if (dup != expected) {
        wrongDup++;
      }
      if (del != expected) {
        wrongDeleted++;
      }
      catalog.dropTable(TableIdentifier.of("db", "t" + trial), true);
    }
    driver.shutdownNow();

    System.out.printf(
        "RESULT version=%s trials=%d manifests=%d k=%d threads=%d expected=%d "
            + "wrongDuplicateCount=%d wrongDeletedCount=%d threw=%d hung=%d "
            + "dupRange=[%d,%d] minMaxOverlap=%d exceptions=%s%n",
        version, trials, manifests, k, threads, expected, wrongDup, wrongDeleted, threw, hung,
        minDup, maxDup, minOverlap, exceptions);
  }

  static int size(Iterable<?> it) {
    int n = 0;
    for (Object o : it) {
      n++;
    }
    return n;
  }

  static ManifestFile writeManifest(Table table, int trial, int m, int k) throws Exception {
    String path = table.location() + "/metadata/calib-" + trial + "-" + m + ".avro";
    ManifestWriter<DataFile> w =
        ManifestFiles.write(2, table.spec(), table.io().newOutputFile(path), null);
    try {
      for (int i = 0; i < k; i++) {
        DataFile f =
            DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/t" + trial + "-m" + m + "-f" + i + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withFormat(FileFormat.PARQUET)
                .build();
        w.add(f);
        w.add(f);
      }
    } finally {
      w.close();
    }
    return w.toManifestFile();
  }
}
