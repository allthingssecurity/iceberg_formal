package races;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;

/**
 * B-19/B-20 user-visible effect. T threads each own a separate HadoopTableOperations (the real
 * class, via its protected constructor) on the same local-filesystem table and the same lock
 * manager, and each does C fast appends of a uniquely named data file. A commit that returns
 * normally has been reported successful; its file must be live in the final table (fast appends
 * never remove files). Lost commit = reported success but file absent.
 *
 * <p>args: config(prod|fast|stall) threads commitsPerThread
 */
public class HadoopLockCommit {
  static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  static class Ops extends HadoopTableOperations {
    Ops(Path location, Configuration conf, LockManager lm) {
      super(location, new HadoopFileIO(conf), conf, lm);
    }
  }

  static final AtomicLong overlaps = new AtomicLong();
  static final AtomicLong acquired = new AtomicLong();

  /** Delegates to the real manager; only measures how many holders are inside per entity. */
  static class Measuring implements LockManager {
    final LockManager d;
    final Map<String, java.util.concurrent.atomic.AtomicInteger> occ = new ConcurrentHashMap<>();
    final Set<String> held = ConcurrentHashMap.newKeySet();

    Measuring(LockManager d) {
      this.d = d;
    }

    @Override
    public boolean acquire(String entityId, String ownerId) {
      boolean ok = d.acquire(entityId, ownerId);
      if (ok) {
        held.add(entityId + "|" + ownerId);
        acquired.incrementAndGet();
        if (occ.computeIfAbsent(entityId, k -> new java.util.concurrent.atomic.AtomicInteger())
                .incrementAndGet()
            > 1) {
          overlaps.incrementAndGet();
        }
      }
      return ok;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      if (held.remove(entityId + "|" + ownerId)) {
        occ.get(entityId).decrementAndGet();
      }
      return d.release(entityId, ownerId);
    }

    @Override
    public void initialize(Map<String, String> properties) {}

    @Override
    public void close() {}
  }

  public static void main(String[] args) throws Exception {
    String config = args[0];
    int threads = Integer.parseInt(args[1]);
    int commits = Integer.parseInt(args[2]);
    LockManager lm;
    switch (config) {
      case "prod":
        lm = LockManagers.defaultLockManager(); // what HadoopCatalog/HadoopTables get: 3s / 15s
        break;
      case "fast":
        lm = inMemory("2", "50");
        break;
      case "stall":
        lm = inMemory("200", "1");
        break;
      default:
        throw new IllegalArgumentException("unknown config " + config);
    }

    final LockManager measured = new Measuring(lm);
    Configuration conf = new Configuration();
    File dir = Files.createTempDirectory("r5-hadoop-lock").toFile();
    String location = new File(dir, "t").getAbsolutePath();
    Table created =
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

    Set<String> reported = ConcurrentHashMap.newKeySet();
    AtomicLong failed = new AtomicLong();
    Map<String, Integer> failures = new ConcurrentHashMap<>();
    CyclicBarrier barrier = new CyclicBarrier(threads);
    List<Thread> ts = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int tid = t;
      Thread th =
          new Thread(
              () -> {
                Table table = new BaseTable(new Ops(new Path(location), conf, measured), "t" + tid);
                try {
                  barrier.await();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
                for (int c = 0; c < commits; c++) {
                  String path = location + "/data/t" + tid + "-c" + c + ".parquet";
                  DataFile f =
                      DataFiles.builder(PartitionSpec.unpartitioned())
                          .withPath(path)
                          .withFileSizeInBytes(10)
                          .withRecordCount(1)
                          .withFormat(FileFormat.PARQUET)
                          .build();
                  try {
                    table.newFastAppend().appendFile(f).commit();
                    reported.add(path);
                  } catch (RuntimeException e) {
                    failed.incrementAndGet();
                    failures.merge(e.getClass().getSimpleName(), 1, Integer::sum);
                  }
                }
              });
      ts.add(th);
      th.start();
    }
    for (Thread th : ts) {
      th.join();
    }
    Table fin = new HadoopTables(conf).load(location);
    Set<String> live = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = fin.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        live.add(task.file().location());
      }
    }
    long lost = reported.stream().filter(p -> !live.contains(p)).count();
    int snapshots = 0;
    for (Object s : fin.snapshots()) {
      snapshots++;
    }
    System.out.printf(
        "RESULT version=%s config=%s threads=%d commitsPerThread=%d reportedSuccess=%d failed=%d "
            + "liveFiles=%d snapshotsInMetadata=%d LOST=%d lockAcquisitions=%d lockOverlaps=%d failures=%s%n",
        System.getProperty("icebergVersion"), config, threads, commits, reported.size(),
        failed.get(), live.size(), snapshots, lost, acquired.get(), overlaps.get(),
        new TreeMap<>(failures));
    System.exit(0);
  }

  static LockManager inMemory(String interval, String timeout) throws Exception {
    Class<?> cls = Class.forName("org.apache.iceberg.util.LockManagers$InMemoryLockManager");
    Constructor<?> ctor = cls.getDeclaredConstructor(Map.class);
    ctor.setAccessible(true);
    return (LockManager)
        ctor.newInstance(
            Map.of(
                "lock.acquire-interval-ms", "1",
                "lock.acquire-timeout-ms", "20000",
                "lock.heartbeat-interval-ms", interval,
                "lock.heartbeat-timeout-ms", timeout));
  }
}
