package org.apache.iceberg.actions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

/**
 * B-133 / D-10. The real BaseCommitService (offer / commitReadyCommitGroups / succeededCommits from
 * the 1.12.0 jar) with a minimal concrete subclass (it is abstract; this class sits in its package
 * only to reach the package-private constructor). T threads offer groups concurrently, as
 * RewriteDataFilesSparkAction's rewrite pool does under partial progress, with rewritesPerCommit=1.
 * Every commitOrClean that returns counts as one successful commit, so succeededCommits() must
 * equal the number of groups in results().
 *
 * <p>args: work(noop|commit) threads groupsPerThread trials
 */
public class CounterRace {
  static class Service extends BaseCommitService<Integer> {
    final String work;
    final Table table;
    final AtomicInteger realSuccesses = new AtomicInteger();

    Service(Table table, String work) {
      super(table, 1);
      this.table = table;
      this.work = work;
    }

    @Override
    protected void commitOrClean(Set<Integer> batch) {
      if (work.equals("commit")) {
        // a real Iceberg commit per batch (in-memory catalog, concurrent appends retry on conflict)
        int g = batch.iterator().next();
        table
            .newFastAppend()
            .appendFile(
                DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(table.location() + "/data/g" + g + ".parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .withFormat(FileFormat.PARQUET)
                    .build())
            .commit();
      }
      realSuccesses.incrementAndGet();
    }

    @Override
    protected void abortFileGroup(Integer group) {}
  }

  public static void main(String[] args) throws Exception {
    String work = args[0];
    if (!work.equals("noop") && !work.equals("commit")) {
      throw new IllegalArgumentException("unknown work " + work);
    }
    int threads = Integer.parseInt(args[1]);
    int groups = Integer.parseInt(args[2]);
    int trials = Integer.parseInt(args[3]);
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("mem", Map.of());
    catalog.createNamespace(Namespace.of("db"));
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    int badTrials = 0;
    long lostIncrements = 0;
    long totalCommits = 0;
    for (int trial = 0; trial < trials; trial++) {
      Table table =
          catalog.createTable(
              TableIdentifier.of("db", "c" + trial),
              schema,
              PartitionSpec.unpartitioned(),
              Map.of("commit.retry.num-retries", "100000", "commit.retry.min-wait-ms", "0",
                  "commit.retry.max-wait-ms", "1"));
      Service svc = new Service(table, work);
      svc.start();
      CyclicBarrier barrier = new CyclicBarrier(threads);
      List<Thread> ts = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int tid = t;
        Thread th =
            new Thread(
                () -> {
                  try {
                    barrier.await();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                  for (int g = 0; g < groups; g++) {
                    svc.offer(tid * groups + g);
                  }
                });
        ts.add(th);
        th.start();
      }
      for (Thread th : ts) {
        th.join();
      }
      svc.close();
      int results = svc.results().size();
      int counted = svc.succeededCommits();
      int real = svc.realSuccesses.get();
      if (real != results) {
        throw new IllegalStateException("harness: " + real + " successes vs " + results + " results");
      }
      totalCommits += real;
      if (counted != real) {
        badTrials++;
        lostIncrements += real - counted;
      }
      catalog.dropTable(TableIdentifier.of("db", "c" + trial), true);
    }
    System.out.printf(
        "RESULT version=%s work=%s threads=%d groupsPerThread=%d trials=%d badTrials=%d "
            + "successfulCommits=%d lostIncrements=%d%n",
        System.getProperty("icebergVersion"), work, threads, groups, trials, badTrials,
        totalCommits, lostIncrements);
    System.exit(0);
  }
}
