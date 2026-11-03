package org.apache.iceberg.flink.sink;

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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.r5.R5Tables;
import org.junit.jupiter.api.Test;

/**
 * E-4: SinkUtil.getMaxCommittedCheckpointId walks parents from the branch head and stops at the
 * first missing snapshot. If snapshot expiry removed the job's last snapshot while the job was
 * down, the restored committables for an already-committed checkpoint are committed again.
 *
 * <p>Part A drives the v1 IcebergFilesCommitter operator through Flink's
 * OneInputStreamOperatorTestHarness (snapshot, notifyOfCompletedCheckpoint, initializeState). Part
 * B drives the v2 IcebergCommitter the way CommitterOperator does after restore: a fresh committer
 * receives the checkpointed committable again.
 */
class E4MaxCommittedCheckpointTest {

  static final int CKPTS = 3;
  static final int ROWS_PER_FILE = 2;

  /** Unknown names throw (valueOf). */
  enum Mode {
    CONTROL_NO_EXPIRE(false, false),
    EXPIRE(true, false),
    CONTROL_NO_EXPIRE_DELETEFAIL(false, true),
    EXPIRE_DELETEFAIL(true, true);

    final boolean expire;
    final boolean failTempManifestDeletes;

    Mode(boolean expire, boolean failTempManifestDeletes) {
      this.expire = expire;
      this.failTempManifestDeletes = failTempManifestDeletes;
    }
  }

  static final class Outcome {
    boolean jobSnapshotExpired;
    long lookup; // SinkUtil.getMaxCommittedCheckpointId just before restore
    String restore = "ok"; // or exception class of the restore/commit failure
    int injectedDeleteFailures;
    boolean restoredManifestSurvived;
    int rows;
    int distinctRows;
    int dupDataFileEntries;
    String requestSignal = "-";

    String key() {
      return String.format(
          "jobSnapExpired=%s lookup=%d deleteFaults=%d tempManifestSurvived=%s restore=%s"
              + " signal=%s rows=%d distinct=%d dupFileEntries=%d",
          jobSnapshotExpired,
          lookup,
          injectedDeleteFailures,
          restoredManifestSurvived,
          restore,
          requestSignal,
          rows,
          distinctRows,
          dupDataFileEntries);
    }
  }

  private static int runs() {
    String v = System.getProperty("r5.runs");
    return v == null ? 10 : Integer.parseInt(v);
  }

  // ---------- shared phases ----------

  /** Other writers commit while the job is down, then (optionally) snapshots are expired. */
  static void outage(R5Tables.Handle h, Mode mode, long jobLastSnapshotId, Outcome o)
      throws Exception {
    Table t = h.reload();
    for (int i = 0; i < 2; i++) {
      DataFile other = R5Tables.writeData(t, 1000 * (i + 1), ROWS_PER_FILE);
      t.newAppend().appendFile(other).commit();
    }
    if (mode.expire) {
      Thread.sleep(2);
      t.expireSnapshots().expireOlderThan(System.currentTimeMillis()).retainLast(2).commit();
    }
    t.refresh();
    o.jobSnapshotExpired = t.snapshot(jobLastSnapshotId) == null;
  }

  static void measure(R5Tables.Handle h, Outcome o) throws Exception {
    Table t = h.reload();
    List<Integer> ids = R5Tables.readIds(t);
    o.rows = ids.size();
    o.distinctRows = (int) ids.stream().distinct().count();
    Map<String, Integer> byPath = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = t.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        byPath.merge(task.file().location(), 1, Integer::sum);
      }
    }
    o.dupDataFileEntries = byPath.values().stream().mapToInt(c -> c - 1).sum();
    o.injectedDeleteFailures = h.ops.injectedDeleteFailures.get();
  }

  static String rootClass(Throwable e) {
    Throwable r = e;
    while (r.getCause() != null && r.getCause() != r) {
      r = r.getCause();
    }
    return r.getClass().getSimpleName();
  }

  // ---------- Part A: v1 IcebergFilesCommitter through the operator harness ----------

  static final class Factory extends AbstractStreamOperatorFactory<Void>
      implements OneInputStreamOperatorFactory<FlinkWriteResult, Void> {
    private final TableLoader loader;
    private final org.apache.iceberg.PartitionSpec spec;

    Factory(TableLoader loader, org.apache.iceberg.PartitionSpec spec) {
      this.loader = loader;
      this.spec = spec;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Void>> T createStreamOperator(
        StreamOperatorParameters<Void> params) {
      return (T)
          new IcebergFilesCommitter(params, loader, false, Map.of(), 1, "main", spec);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergFilesCommitter.class;
    }
  }

  static MockEnvironment env(JobID jobId) {
    return new MockEnvironmentBuilder()
        .setTaskName("r5 committer")
        .setManagedMemorySize(32 * 1024)
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(256)
        .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
        .setExecutionConfig(new ExecutionConfig())
        .setMaxParallelism(16)
        .setJobID(jobId)
        .build();
  }

  static Outcome runV1(Mode mode, int run) throws Exception {
    R5Tables.Handle h = R5Tables.create(2);
    TableLoader loader = R5Tables.loader("e4v1-" + mode + run, h.table);
    h.table.refresh();
    // The operator reads table.spec() for manifests; give it a fresh table object.
    JobID jobId = new JobID();
    OperatorID operatorId;
    OperatorSubtaskState stateAtLastCkpt = null;
    long jobLastSnapshotId;
    Outcome o = new Outcome();

    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        new OneInputStreamOperatorTestHarness<>(new Factory(loader, h.table.spec()), env(jobId))) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();
      long ts = 0;
      for (long ckpt = 1; ckpt <= CKPTS; ckpt++) {
        DataFile f = R5Tables.writeData(h.table, (int) ckpt * 10, ROWS_PER_FILE);
        harness.processElement(
            new FlinkWriteResult(ckpt, WriteResult.builder().addDataFiles(f).build()), ++ts);
        OperatorSubtaskState s = harness.snapshot(ckpt, ++ts);
        if (ckpt == CKPTS) {
          stateAtLastCkpt = s;
          h.ops.failDeletes.set(mode.failTempManifestDeletes);
        }
        harness.notifyOfCompletedCheckpoint(ckpt);
        h.ops.failDeletes.set(false);
      }
      h.table.refresh();
      jobLastSnapshotId = h.table.currentSnapshot().snapshotId();
      assertEquals(
          String.valueOf(CKPTS),
          h.table.currentSnapshot().summary().get("flink.max-committed-checkpoint-id"),
          "harness: last checkpoint must have been committed before the crash");
      // Crash here: checkpoint CKPTS+1 never completes. The latest completed checkpoint's state
      // (taken before its notifyCheckpointComplete) still holds checkpoint CKPTS's manifest.
    }

    outage(h, mode, jobLastSnapshotId, o);
    Table t = h.reload();
    o.lookup =
        SinkUtil.getMaxCommittedCheckpointId(t, jobId.toString(), operatorId.toString(), "main");
    o.restoredManifestSurvived = !h.ops.failedDeletePaths.isEmpty();
    for (String p : h.ops.failedDeletePaths) {
      o.restoredManifestSurvived &= R5Tables.exists(t.io(), p);
    }

    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        new OneInputStreamOperatorTestHarness<>(new Factory(loader, h.table.spec()), env(jobId))) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      try {
        harness.initializeState(stateAtLastCkpt);
        harness.open();
      } catch (Exception e) {
        o.restore = "FAILED:" + rootClass(e);
      }
    } catch (Exception e) {
      if ("ok".equals(o.restore)) {
        o.restore = "CLOSE_FAILED:" + rootClass(e);
      }
    }
    measure(h, o);
    return o;
  }

  // ---------- Part B: v2 IcebergCommitter, re-fed the checkpointed committable ----------

  static final class Req implements Committer.CommitRequest<IcebergCommittable> {
    final IcebergCommittable c;
    String signal = "committed-or-none";

    Req(IcebergCommittable c) {
      this.c = c;
    }

    @Override
    public IcebergCommittable getCommittable() {
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
    public void updateAndRetryLater(IcebergCommittable committable) {
      signal = "updateAndRetryLater";
    }

    @Override
    public void signalAlreadyCommitted() {
      signal = "alreadyCommitted";
    }
  }

  static IcebergCommitter v2Committer(TableLoader loader, boolean maintenance, Table t) {
    return new IcebergCommitter(
        loader,
        "main",
        Map.of(),
        false,
        1,
        "r5sink",
        new IcebergFilesCommitterMetrics(new UnregisteredMetricsGroup(), t.name()),
        maintenance,
        0);
  }

  static Outcome runV2(Mode mode, boolean maintenance, int run) throws Exception {
    R5Tables.Handle h = R5Tables.create(2);
    TableLoader loader = R5Tables.loader("e4v2-" + mode + maintenance + run, h.table);
    String jobId = new JobID().toString();
    String operatorId = new OperatorID().toHexString();
    Outcome o = new Outcome();
    IcebergCommittable last = null;
    List<String> lastManifests = new ArrayList<>();
    IcebergCommitter committer1 = v2Committer(loader, maintenance, h.table);
    ManifestOutputFileFactory mf =
        FlinkManifestUtil.createOutputFileFactory(
            () -> h.table, h.table.properties(), jobId, operatorId, 0, 0);
    for (long ckpt = 1; ckpt <= CKPTS; ckpt++) {
      final long c = ckpt;
      DataFile f = R5Tables.writeData(h.table, (int) ckpt * 10, ROWS_PER_FILE);
      DeltaManifests dm =
          FlinkManifestUtil.writeCompletedFiles(
              WriteResult.builder().addDataFiles(f).build(), () -> mf.create(c), h.table.spec(), 2);
      byte[] bytes =
          SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, dm);
      IcebergCommittable committable = new IcebergCommittable(bytes, jobId, operatorId, ckpt);
      if (ckpt == CKPTS) {
        last = committable;
        dm.manifests().forEach(m -> lastManifests.add(m.path()));
        h.ops.failDeletes.set(mode.failTempManifestDeletes);
      }
      Req r = new Req(committable);
      committer1.commit(List.of(r));
      h.ops.failDeletes.set(false);
      assertEquals("committed-or-none", r.signal, "harness: first commit must succeed");
    }
    committer1.close();
    h.table.refresh();
    long jobLastSnapshotId = h.table.currentSnapshot().snapshotId();
    assertEquals(
        String.valueOf(CKPTS),
        h.table.currentSnapshot().summary().get("flink.max-committed-checkpoint-id"));

    outage(h, mode, jobLastSnapshotId, o);
    Table t = h.reload();
    o.lookup = SinkUtil.getMaxCommittedCheckpointId(t, jobId, operatorId, "main");
    o.restoredManifestSurvived = true;
    for (String p : lastManifests) {
      o.restoredManifestSurvived &= R5Tables.exists(t.io(), p);
    }

    // Restore: CommitterOperator re-delivers the committable of the restored checkpoint.
    IcebergCommitter committer2 = v2Committer(loader, maintenance, t);
    Req again = new Req(last);
    try {
      committer2.commit(List.of(again));
    } catch (Exception e) {
      o.restore = "FAILED:" + rootClass(e);
    }
    o.requestSignal = again.signal;
    committer2.close();
    measure(h, o);
    return o;
  }

  @Test
  void e4() throws Exception {
    int n = runs();
    Path out = Paths.get(System.getProperty("r5.out", "build/r5"));
    Files.createDirectories(out);
    Map<String, Map<String, Integer>> results = new TreeMap<>();
    for (Mode mode : Mode.values()) {
      Map<String, Integer> v1 = new TreeMap<>();
      Map<String, Integer> v2 = new TreeMap<>();
      Map<String, Integer> v2m = new TreeMap<>();
      for (int i = 0; i < n; i++) {
        v1.merge(runV1(mode, i).key(), 1, Integer::sum);
        v2.merge(runV2(mode, false, i).key(), 1, Integer::sum);
        v2m.merge(runV2(mode, true, i).key(), 1, Integer::sum);
      }
      results.put(String.format("%-29s v1-IcebergFilesCommitter       ", mode), v1);
      results.put(String.format("%-29s v2-IcebergCommitter maint=false", mode), v2);
      results.put(String.format("%-29s v2-IcebergCommitter maint=true ", mode), v2m);
    }
    try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("e4.txt")))) {
      w.println(
          "E-4 iceberg "
              + System.getProperty("icebergVersion")
              + " runs="
              + n
              + " ckpts="
              + CKPTS
              + " rowsPerFile="
              + ROWS_PER_FILE
              + " expectedDistinctRows="
              + (CKPTS + 2) * ROWS_PER_FILE);
      results.forEach(
          (k, v) -> v.forEach((outcome, c) -> w.printf("%s | %d/%d | %s%n", k, c, n, outcome)));
    }
    for (Map.Entry<String, Map<String, Integer>> e : results.entrySet()) {
      if (e.getKey().startsWith("CONTROL")) {
        for (String outcome : e.getValue().keySet()) {
          assertTrue(outcome.contains("dupFileEntries=0") && outcome.contains("restore=ok"), e.toString());
        }
      }
    }
  }
}
