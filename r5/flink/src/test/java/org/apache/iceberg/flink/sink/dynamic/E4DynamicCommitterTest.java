package org.apache.iceberg.flink.sink.dynamic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.ManifestOutputFileFactory;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.r5.R5Tables;
import org.junit.jupiter.api.Test;

/** E-4 for DynamicCommitter: its own ancestor walk (DynamicCommitter.java:190-209 in 1.12.0). */
class E4DynamicCommitterTest {

  static final int CKPTS = 3;
  static final int ROWS = 2;

  enum Mode {
    CONTROL_NO_EXPIRE(false, false),
    EXPIRE(true, false),
    CONTROL_NO_EXPIRE_DELETEFAIL(false, true),
    EXPIRE_DELETEFAIL(true, true);

    final boolean expire;
    final boolean failDeletes;

    Mode(boolean expire, boolean failDeletes) {
      this.expire = expire;
      this.failDeletes = failDeletes;
    }
  }

  /** Catalog whose loadTable hands out tables over the handle's fault-injecting ops. */
  static final class HandleCatalog implements Catalog {
    final R5Tables.Handle h;

    HandleCatalog(R5Tables.Handle h) {
      this.h = h;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return List.of(h.id);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      if (!identifier.equals(h.id)) {
        throw new IllegalArgumentException("unknown table " + identifier);
      }
      return h.reload();
    }
  }

  static final class Req implements Committer.CommitRequest<DynamicCommittable> {
    final DynamicCommittable c;
    String signal = "committed-or-none";

    Req(DynamicCommittable c) {
      this.c = c;
    }

    @Override
    public DynamicCommittable getCommittable() {
      return c;
    }

    @Override
    public int getNumberOfRetries() {
      return 0;
    }

    @Override
    public void signalFailedWithKnownReason(Throwable t) {
      signal = "failedKnown";
    }

    @Override
    public void signalFailedWithUnknownReason(Throwable t) {
      signal = "failedUnknown";
    }

    @Override
    public void retryLater() {
      signal = "retryLater";
    }

    @Override
    public void updateAndRetryLater(DynamicCommittable committable) {
      signal = "updateAndRetryLater";
    }

    @Override
    public void signalAlreadyCommitted() {
      signal = "alreadyCommitted";
    }
  }

  static DynamicCommitter committer(Catalog catalog) {
    return new DynamicCommitter(
        catalog,
        Map.of(),
        false,
        1,
        "r5dyn",
        new DynamicCommitterMetrics(new UnregisteredMetricsGroup()));
  }

  static String run(Mode mode) throws Exception {
    R5Tables.Handle h = R5Tables.create(2);
    HandleCatalog catalog = new HandleCatalog(h);
    String jobId = new JobID().toString();
    String operatorId = new OperatorID().toString();
    TableKey key = new TableKey(h.id.toString(), "main");
    DynamicCommitter c1 = committer(catalog);
    ManifestOutputFileFactory mf =
        FlinkManifestUtil.createOutputFileFactory(
            () -> h.table, h.table.properties(), jobId, operatorId, 0, 0);
    DynamicCommittable last = null;
    List<String> lastManifests = new ArrayList<>();
    for (long ckpt = 1; ckpt <= CKPTS; ckpt++) {
      final long cid = ckpt;
      DataFile f = R5Tables.writeData(h.table, (int) ckpt * 10, ROWS);
      DeltaManifests dm =
          FlinkManifestUtil.writeCompletedFiles(
              WriteResult.builder().addDataFiles(f).build(),
              () -> mf.create(cid),
              h.table.spec(),
              2);
      byte[] bytes =
          SimpleVersionedSerialization.writeVersionAndSerialize(
              DeltaManifestsSerializer.INSTANCE, dm);
      DynamicCommittable dc =
          new DynamicCommittable(key, new byte[][] {bytes}, jobId, operatorId, ckpt);
      if (ckpt == CKPTS) {
        last = dc;
        dm.manifests().forEach(m -> lastManifests.add(m.path()));
        h.ops.failDeletes.set(mode.failDeletes);
      }
      Req r = new Req(dc);
      c1.commit(List.of(r));
      h.ops.failDeletes.set(false);
      assertEquals("committed-or-none", r.signal);
    }
    c1.close();
    Table t = h.reload();
    long jobLast = t.currentSnapshot().snapshotId();
    assertEquals(
        String.valueOf(CKPTS), t.currentSnapshot().summary().get("flink.max-committed-checkpoint-id"));

    // outage: other writers, then expiry
    for (int i = 0; i < 2; i++) {
      t.newAppend().appendFile(R5Tables.writeData(t, 1000 * (i + 1), ROWS)).commit();
    }
    if (mode.expire) {
      Thread.sleep(2);
      t.expireSnapshots().expireOlderThan(System.currentTimeMillis()).retainLast(2).commit();
    }
    t.refresh();
    boolean expired = t.snapshot(jobLast) == null;
    boolean survived = true;
    for (String p : lastManifests) {
      survived &= R5Tables.exists(t.io(), p);
    }

    DynamicCommitter c2 = committer(catalog);
    Req again = new Req(last);
    String restore = "ok";
    try {
      c2.commit(List.of(again));
    } catch (Exception e) {
      Throwable r = e;
      while (r.getCause() != null) {
        r = r.getCause();
      }
      restore = "FAILED:" + r.getClass().getSimpleName();
    }
    c2.close();

    Table fin = h.reload();
    List<Integer> ids = R5Tables.readIds(fin);
    Map<String, Integer> byPath = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = fin.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        byPath.merge(task.file().location(), 1, Integer::sum);
      }
    }
    int dup = byPath.values().stream().mapToInt(x -> x - 1).sum();
    return String.format(
        "jobSnapExpired=%s deleteFaults=%d tempManifestSurvived=%s restore=%s signal=%s rows=%d"
            + " distinct=%d dupFileEntries=%d",
        expired,
        h.ops.injectedDeleteFailures.get(),
        survived,
        restore,
        again.signal,
        ids.size(),
        ids.stream().distinct().count(),
        dup);
  }

  @Test
  void e4dynamic() throws Exception {
    String v = System.getProperty("r5.runs");
    int n = v == null ? 10 : Integer.parseInt(v);
    Path out = Paths.get(System.getProperty("r5.out", "build/r5"));
    Files.createDirectories(out);
    Map<String, Map<String, Integer>> results = new TreeMap<>();
    for (Mode m : Mode.values()) {
      Map<String, Integer> counts = new TreeMap<>();
      for (int i = 0; i < n; i++) {
        counts.merge(run(m), 1, Integer::sum);
      }
      results.put(String.format("%-29s DynamicCommitter", m), counts);
    }
    try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("e4-dynamic.txt")))) {
      w.println("E-4 DynamicCommitter iceberg " + System.getProperty("icebergVersion") + " runs=" + n);
      results.forEach(
          (k, c) -> c.forEach((o, cnt) -> w.printf("%s | %d/%d | %s%n", k, cnt, n, o)));
    }
    for (Map.Entry<String, Map<String, Integer>> e : results.entrySet()) {
      if (e.getKey().startsWith("CONTROL")) {
        for (String o : e.getValue().keySet()) {
          assertTrue(o.contains("dupFileEntries=0") && o.contains("restore=ok"), e.toString());
        }
      }
    }
  }
}
