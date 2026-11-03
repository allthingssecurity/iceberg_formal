package races;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * B-24. One shared HadoopTableOperations (the ops behind a HadoopTables-loaded table) is used by
 * R refresher threads calling ops.refresh() and by one committer thread doing fast appends through
 * the same table object. An independent writer advances the table through its own HadoopTables
 * instance. Measured:
 *
 * <ul>
 *   <li>regressions: a thread's refresh() returns metadata with a lower version number than that
 *       same thread got from an earlier refresh()
 *   <li>read-your-writes misses: right after the shared-table committer's commit() returns, the
 *       same table's current metadata does not contain the snapshot it just committed
 *   <li>lost commits: reported-successful appends whose file is not live at the end
 * </ul>
 *
 * <p>args: refreshers seconds
 */
public class RefreshRace {
  static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  static final Pattern V = Pattern.compile("v(\\d+)\\.metadata\\.json$");

  static int version(TableMetadata m) {
    Matcher x = V.matcher(m.metadataFileLocation());
    if (!x.find()) {
      throw new IllegalStateException("unexpected metadata file name " + m.metadataFileLocation());
    }
    return Integer.parseInt(x.group(1));
  }

  static DataFile file(String path) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  /**
   * Local filesystem with injected stat latency (0..maxMicros, uniform), modelling a remote
   * filesystem (HDFS NameNode RPC, object store HEAD). Does not change any result, only timing.
   */
  public static class LatencyLocalFileSystem extends org.apache.hadoop.fs.LocalFileSystem {
    static volatile int maxMicros = 0;
    static final AtomicLong delayed = new AtomicLong();

    @Override
    public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path f)
        throws java.io.IOException {
      int m = maxMicros;
      if (m > 0) {
        delayed.incrementAndGet();
        java.util.concurrent.locks.LockSupport.parkNanos(
            java.util.concurrent.ThreadLocalRandom.current().nextLong(m) * 1000L);
      }
      return super.getFileStatus(f);
    }
  }

  static class CountingOps extends org.apache.iceberg.hadoop.HadoopTableOperations {
    final AtomicLong attempts = new AtomicLong();
    final Map<String, Integer> outcomes = new ConcurrentHashMap<>();

    CountingOps(org.apache.hadoop.fs.Path location, Configuration conf) {
      super(location, new org.apache.iceberg.hadoop.HadoopFileIO(conf), conf,
          org.apache.iceberg.util.LockManagers.defaultLockManager());
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      attempts.incrementAndGet();
      try {
        super.commit(base, metadata);
        outcomes.merge("ok", 1, Integer::sum);
      } catch (RuntimeException e) {
        outcomes.merge(e.getClass().getSimpleName(), 1, Integer::sum);
        throw e;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int refreshers = Integer.parseInt(args[0]);
    int seconds = Integer.parseInt(args[1]);
    LatencyLocalFileSystem.maxMicros = Integer.parseInt(args[2]);
    String sc = args[3];
    if (!sc.equals("on") && !sc.equals("off")) {
      throw new IllegalArgumentException("unknown sharedCommitter " + sc);
    }
    boolean sharedCommitter = sc.equals("on");
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", LatencyLocalFileSystem.class.getName());
    File dir = Files.createTempDirectory("r5-refresh").toFile();
    String location = new File(dir, "t").getAbsolutePath();
    new HadoopTables(conf)
        .create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            Map.of(
                "commit.retry.num-retries", "100000",
                "commit.retry.min-wait-ms", "1",
                "commit.retry.max-wait-ms", "5",
                "commit.retry.total-timeout-ms", "600000"),
            location);

    // same class HadoopTables.load builds; the subclass only counts commit() calls and their outcome
    CountingOps ops = new CountingOps(new org.apache.hadoop.fs.Path(location), conf);
    Table shared = new org.apache.iceberg.BaseTable(ops, location);
    Table writerTable = new HadoopTables(conf).load(location);

    AtomicBoolean stop = new AtomicBoolean();
    AtomicLong refreshes = new AtomicLong();
    AtomicLong regressions = new AtomicLong();
    AtomicLong readerReads = new AtomicLong();
    AtomicLong readerRegressions = new AtomicLong();
    AtomicLong readerEpisodes = new AtomicLong();
    AtomicLong maxRegress = new AtomicLong();
    AtomicLong rywMiss = new AtomicLong();
    AtomicLong sharedCommits = new AtomicLong();
    AtomicLong writerCommits = new AtomicLong();
    Set<String> reported = ConcurrentHashMap.newKeySet();
    Map<String, Integer> failures = new ConcurrentHashMap<>();
    List<Thread> ts = new ArrayList<>();

    for (int r = 0; r < refreshers; r++) {
      ts.add(
          new Thread(
              () -> {
                int max = -1;
                while (!stop.get()) {
                  TableMetadata m = ops.refresh();
                  refreshes.incrementAndGet();
                  int v = version(m);
                  if (v < max) {
                    regressions.incrementAndGet();
                    final int d = max - v;
                    maxRegress.accumulateAndGet(d, Math::max);
                  }
                  max = Math.max(max, v);
                }
              }));
    }
    // readers: use the cached metadata exactly as BaseTable.currentSnapshot() does (ops.current())
    for (int r = 0; r < 2; r++) {
      ts.add(
          new Thread(
              () -> {
                int max = -1;
                boolean inRegress = false;
                while (!stop.get()) {
                  TableMetadata m = ops.current();
                  readerReads.incrementAndGet();
                  int v = version(m);
                  if (v >= max) {
                    inRegress = false;
                  }
                  if (v < max) {
                    if (!inRegress) {
                      readerEpisodes.incrementAndGet();
                    }
                    inRegress = true;
                    readerRegressions.incrementAndGet();
                    final int d = max - v;
                    maxRegress.accumulateAndGet(d, Math::max);
                  }
                  max = Math.max(max, v);
                }
              }));
    }
    // independent writer, own ops
    ts.add(
        new Thread(
            () -> {
              int c = 0;
              while (!stop.get()) {
                String p = location + "/data/w-" + (c++) + ".parquet";
                try {
                  writerTable.newFastAppend().appendFile(file(p)).commit();
                  reported.add(p);
                  writerCommits.incrementAndGet();
                } catch (RuntimeException e) {
                  failures.merge("writer:" + e.getClass().getSimpleName(), 1, Integer::sum);
                }
              }
            }));
    // committer through the shared table/ops
    if (sharedCommitter) ts.add(
        new Thread(
            () -> {
              int c = 0;
              while (!stop.get()) {
                String p = location + "/data/s-" + (c++) + ".parquet";
                String id = UUID.randomUUID().toString();
                try {
                  shared.newFastAppend().appendFile(file(p)).set("r5-commit-id", id).commit();
                  reported.add(p);
                  sharedCommits.incrementAndGet();
                  boolean seen = false;
                  for (Snapshot s : ops.current().snapshots()) {
                    if (id.equals(s.summary().get("r5-commit-id"))) {
                      seen = true;
                      break;
                    }
                  }
                  if (!seen) {
                    rywMiss.incrementAndGet();
                  }
                } catch (RuntimeException e) {
                  failures.merge("shared:" + e.getClass().getSimpleName(), 1, Integer::sum);
                }
              }
            }));
    for (Thread t : ts) {
      t.start();
    }
    Thread.sleep(seconds * 1000L);
    stop.set(true);
    for (Thread t : ts) {
      t.join();
    }
    Table fin = new HadoopTables(conf).load(location);
    Set<String> live = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = fin.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        live.add(task.file().location());
      }
    }
    long lost = reported.stream().filter(p -> !live.contains(p)).count();
    System.out.printf(
        "RESULT version=%s statLatencyMaxMicros=" + LatencyLocalFileSystem.maxMicros
            + " delayedStats=" + LatencyLocalFileSystem.delayed.get() + " refreshers=%d seconds=%d refreshes=%d refreshReturnRegressions=%d readerReads=" + readerReads.get() + " readerRegressedReads=" + readerRegressions.get() + " readerRegressEpisodes=" + readerEpisodes.get() + " maxRegressVersions=%d "
            + "sharedCommits=%d writerCommits=%d readYourWritesMisses=%d reported=%d live=%d LOST=%d failures=%s%n",
        System.getProperty("icebergVersion"), refreshers, seconds, refreshes.get(), regressions.get(),
        maxRegress.get(), sharedCommits.get(), writerCommits.get(), rywMiss.get(), reported.size(),
        live.size(), lost, new TreeMap<>(failures));
    System.out.printf("SHARED_OPS_COMMIT attempts=%d outcomes=%s%n", ops.attempts.get(), new TreeMap<>(ops.outcomes));
    System.exit(0);
  }
}
