package org.apache.iceberg.r5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.maintenance.operator.DVWriteResult;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertCommitter;
import org.apache.iceberg.flink.maintenance.operator.EqualityConvertPlan;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * E-45: EqualityConvertCommitter (iceberg-flink-2.3 1.12.0) deletes the DVs it just wrote on any
 * non-CSUE exception from RowDelta.commit(), even when the exception is not a CleanableFailure and
 * the commit actually landed. Core's SnapshotProducer keeps its files in the same situation.
 */
class E45EqualityConvertCommitterTest {

  /** Fault injected by FaultOps after the delegate commit has landed. Unknown names throw. */
  enum Fault {
    NONE(null),
    PLAIN_RUNTIME(() -> new RuntimeException("injected lost ack")),
    // What core's HTTPClient.execute throws when the socket read times out after the POST was sent
    // (HTTPClient.java:327-328 in 1.12.0): non-CSUE and not a CleanableFailure.
    REST_READ_TIMEOUT(
        () ->
            new RESTException(
                new SocketTimeoutException("Read timed out"),
                "Error occurred while processing %s request",
                "POST")),
    RUNTIME_IO(() -> new RuntimeIOException(new IOException("injected response IO error"))),
    CSUE(() -> new CommitStateUnknownException(new RuntimeException("injected unknown")));

    final Supplier<RuntimeException> supplier;

    Fault(Supplier<RuntimeException> supplier) {
      this.supplier = supplier;
    }
  }

  static final class Outcome {
    int injected;
    boolean attributed; // the exception that reached the caller is the injected instance
    boolean landed; // head snapshot is the one this commit produced
    boolean dvReferenced; // head snapshot's added delete files include our DV
    boolean dvExists;
    String read; // "ids=[...]" or exception class
    int sideErrors;
    String sideErrorClass = "-";

    String key() {
      return String.format(
          "injected=%d attributed=%s sideErrors=%d(%s) landed=%s dvReferenced=%s dvExists=%s read=%s",
          injected, attributed, sideErrors, sideErrorClass, landed, dvReferenced, dvExists, read);
    }
  }

  private static int runs() {
    String v = System.getProperty("r5.runs");
    return v == null ? 10 : Integer.parseInt(v);
  }

  private static String read(Table table) {
    try {
      return "ids=" + R5Tables.readIds(table);
    } catch (Exception e) {
      return "READ_FAILED:" + e.getClass().getSimpleName();
    }
  }

  private static boolean referenced(Table t, Snapshot head, DeleteFile dv) {
    for (DeleteFile f : head.addedDeleteFiles(t.io())) {
      if (f.location().equals(dv.location())) {
        return true;
      }
    }
    return false;
  }

  /** One run through the Flink operator, driven by Flink's own two-input test harness. */
  private static Outcome runFlink(Fault fault, int run) throws Exception {
    R5Tables.Handle h = R5Tables.create(3);
    Table table = h.table;
    DataFile data = R5Tables.writeData(table, 1, 3); // ids 1,2,3
    table.newAppend().appendFile(data).commit();
    long mainBefore = table.currentSnapshot().snapshotId();
    List<DeleteFile> dvs = R5Tables.writeDV(table, data, 0L); // delete id 1
    DeleteFile dv = dvs.get(0);
    assertTrue(table.io().newInputFile(dv.location()).exists(), "harness: DV must exist");

    long stagingSnapshotId = 4242L + run;
    EqualityConvertCommitter committer =
        new EqualityConvertCommitter(
            table.name(), "eqconv", R5Tables.loader("e45-" + run + fault, table), "main", "main");
    Outcome o = new Outcome();
    int injectedBefore = h.ops.injected.get();
    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        new TwoInputStreamOperatorTestHarness<>(committer)) {
      harness.open();
      long doneTs = 1_000_000L + run;
      EqualityConvertPlan plan =
          new EqualityConvertPlan(
              Lists.newArrayList(data),
              Lists.newArrayList(),
              Lists.newArrayList(),
              stagingSnapshotId,
              mainBefore,
              doneTs - 1,
              doneTs);
      harness.processElement1(new StreamRecord<>(new DVWriteResult(dvs, Lists.newArrayList())));
      harness.processElement2(new StreamRecord<>(plan, doneTs - 1));
      if (fault.supplier != null) {
        h.ops.armLostAck(fault.supplier);
      }
      try {
        harness.processBothWatermarks(new Watermark(doneTs));
      } catch (Exception e) {
        // CSUE is rethrown out of processWatermark? (it is caught at 161 in 1.12.0) - record it.
        o.attributed = rootIs(e, h.ops.lastThrown);
      }
      List<StreamRecord<Exception>> errors = new ArrayList<>();
      if (harness.getSideOutput(TaskResultAggregator.ERROR_STREAM) != null) {
        errors.addAll(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM));
      }
      o.sideErrors = errors.size();
      for (StreamRecord<Exception> err : errors) {
        // The harness copies side-output records through Flink's serializer, so identity is lost;
        // attribute by exact exception class of the single injected fault instead.
        o.sideErrorClass = err.getValue().getClass().getSimpleName();
        if (h.ops.lastThrown != null && err.getValue().getClass() == h.ops.lastThrown.getClass()) {
          o.attributed = true;
        }
      }
    }
    o.injected = h.ops.injected.get() - injectedBefore;
    fill(o, h, dv, stagingSnapshotId);
    return o;
  }

  /** The same lost-ack fault on core's own RowDelta, for calibration. */
  private static Outcome runCore(Fault fault, int run) throws Exception {
    R5Tables.Handle h = R5Tables.create(3);
    Table table = h.table;
    DataFile data = R5Tables.writeData(table, 1, 3);
    table.newAppend().appendFile(data).commit();
    DeleteFile dv = R5Tables.writeDV(table, data, 0L).get(0);
    long stagingSnapshotId = 9000L + run;
    Outcome o = new Outcome();
    int injectedBefore = h.ops.injected.get();
    if (fault.supplier != null) {
      h.ops.armLostAck(fault.supplier);
    }
    try {
      table
          .newRowDelta()
          .addDeletes(dv)
          .validateDataFilesExist(List.of(data.location()))
          .set(EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY, "" + stagingSnapshotId)
          .commit();
    } catch (RuntimeException e) {
      o.attributed = rootIs(e, h.ops.lastThrown);
    }
    o.injected = h.ops.injected.get() - injectedBefore;
    fill(o, h, dv, stagingSnapshotId);
    return o;
  }

  private static void fill(Outcome o, R5Tables.Handle h, DeleteFile dv, long stagingSnapshotId) {
    Table fresh = h.reload();
    Snapshot head = fresh.currentSnapshot();
    o.landed =
        String.valueOf(stagingSnapshotId)
            .equals(head.summary().get(EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY));
    o.dvReferenced = referenced(fresh, head, dv);
    o.dvExists = R5Tables.exists(fresh.io(), dv.location());
    o.read = read(fresh);
  }

  private static boolean rootIs(Throwable e, Throwable injected) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t == injected) {
        return true;
      }
    }
    return false;
  }

  @Test
  void e45() throws Exception {
    int n = runs();
    Path out = Paths.get(System.getProperty("r5.out", "build/r5"));
    Files.createDirectories(out);
    Map<String, Map<String, Integer>> table = new TreeMap<>();
    for (Fault f : Fault.values()) {
      for (String engine : List.of("flink-EqualityConvertCommitter", "core-RowDelta")) {
        Map<String, Integer> counts = new TreeMap<>();
        for (int i = 0; i < n; i++) {
          Outcome o = engine.startsWith("flink") ? runFlink(f, i) : runCore(f, i);
          counts.merge(o.key(), 1, Integer::sum);
        }
        table.put(
            String.format(
                "%-18s %-30s cleanable=%s",
                f, engine, f.supplier != null && f.supplier.get() instanceof CleanableFailure),
            counts);
      }
    }
    try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("e45.txt")))) {
      w.println("E-45 iceberg " + System.getProperty("icebergVersion") + " runs=" + n);
      table.forEach(
          (k, v) -> v.forEach((outcome, c) -> w.printf("%s | %d/%d | %s%n", k, c, n, outcome)));
    }
    // Sanity: the control must be clean.
    for (Map.Entry<String, Map<String, Integer>> e : table.entrySet()) {
      if (e.getKey().startsWith("NONE")) {
        assertEquals(1, e.getValue().size(), "control must be deterministic: " + e);
        String only = e.getValue().keySet().iterator().next();
        assertTrue(only.contains("dvExists=true") && only.contains("ids=[2, 3]"), only);
      }
    }
  }
}
