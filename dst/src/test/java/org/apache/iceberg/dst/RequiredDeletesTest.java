package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * r5, candidate A1-18: {@code ManifestFilterManager.deleteFiles} (ManifestFilterManager.java:75)
 * only grows: every {@code filterManifests} adds what that pass deleted (:232), including delete
 * files aged off by {@code dropDeleteFilesOlderThan} (MergingSnapshotProducer.java:1034,
 * ManifestFilterManager.java:523-526). {@code BaseRewriteFiles} always sets
 * {@code failMissingDeletePaths} (BaseRewriteFiles.java:34), so on a re-apply
 * {@code validateRequiredDeletes} (:285-291) demands that every file deleted in ANY earlier pass is
 * deleted again.
 *
 * <p>Scenario: seed0 data files in partitions a and b (seq 1); a position delete D in partition a
 * (seq 2); an unrelated append in b (seq 3). The compaction rewrites both seed0 files, so the
 * minimum live data sequence number becomes 3 and D (seq 2) is aged off by Iceberg itself: the
 * caller never asks to delete D. A concurrent commit removes D. The re-apply must not fail because
 * D is "missing".
 *
 * <p>Attribution is structural: the exception class, whether D's path (and no path the caller asked
 * to delete) is what is reported, and, by reflection after the failure, whether D is in the
 * producer's {@code deleteFilterManager.deleteFiles}.
 */
class RequiredDeletesTest {

  private static final int SEEDS = Integer.getInteger("dst.r5.requireddeletes.seeds", 100);

  /** Per-run state shared between the setup and the actions. */
  static final class State {
    DeleteFile d;
    DataFile seedA;
    DataFile seedB;
    Object rewriteProducer;
    Throwable rewriteFailure;
  }

  static final class Row {
    final String label;
    int seeds;
    int rewriteOk;
    int rewriteFailed;
    int retried;
    final Map<String, Integer> failureClasses = new LinkedHashMap<>();
    int validationReportingOnlyD;
    int dStickyInFilterManager;
    int dAgedOffInFinal;
    final Map<String, Integer> unexplained = new LinkedHashMap<>();
    String first;

    Row(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return String.format(
          "%-50s seeds=%d rewrite{ok=%d failed=%d %s} rewriteRetried=%d"
              + " ValidationException-reporting-only-D=%d D-in-deleteFilterManager.deleteFiles=%d"
              + " harnessInvariants=%s",
          label, seeds, rewriteOk, rewriteFailed, failureClasses, retried,
          validationReportingOnlyD, dStickyInFilterManager, unexplained);
    }
  }

  static FaultConfig config() {
    FaultConfig c = FaultConfig.quiet();
    c.seedSnapshots = 1;
    c.writers = 2;
    return c;
  }

  static void rewrite(Table table, State st, boolean inTransaction, Workload.Outcome outcome) {
    Snapshot current = table.currentSnapshot();
    org.apache.iceberg.Transaction txn = inTransaction ? table.newTransaction() : null;
    org.apache.iceberg.RewriteFiles rewrite = inTransaction ? txn.newRewrite() : table.newRewrite();
    st.rewriteProducer = rewrite;
    rewrite.validateFromSnapshot(current.snapshotId());
    rewrite.deleteFile(st.seedA);
    rewrite.deleteFile(st.seedB);
    org.apache.iceberg.io.FileIO payload = table.io();
    rewrite.addFile(
        Payload.dataFile(payload, Simulation.SPEC, Simulation.LOCATION, "rw", 0, "a", 4));
    rewrite.addFile(
        Payload.dataFile(payload, Simulation.SPEC, Simulation.LOCATION, "rw", 1, "b", 4));
    rewrite.dataSequenceNumber(current.sequenceNumber());
    CommitUuid.makeDeterministic(rewrite);
    try {
      SameThreadExecutor.inject(rewrite).commit();
      if (inTransaction) {
        txn.commitTransaction();
      }
    } catch (RuntimeException e) {
      st.rewriteFailure = e;
      throw e;
    }
  }

  static Row sweep(String label, String[]... scripts) {
    Row row = new Row(label);
    for (long seed = 1; seed <= SEEDS; seed++) {
      State st = new State();
      ScriptRun.Setup setup =
          (table, io, seedFiles) -> {
            st.seedA = seedFiles.get(0);
            st.seedB = seedFiles.get(1);
            st.d =
                Payload.positionDeletes(io, Simulation.SPEC, Simulation.LOCATION, "setup", 0, "a");
            org.apache.iceberg.RowDelta delta = table.newRowDelta().addDeletes(st.d);
            CommitUuid.makeDeterministic(delta);
            SameThreadExecutor.inject(delta).commit();
            org.apache.iceberg.AppendFiles append =
                table.newAppend()
                    .appendFile(
                        Payload.dataFile(
                            io, Simulation.SPEC, Simulation.LOCATION, "setupx", 0, "b"));
            CommitUuid.makeDeterministic(append);
            SameThreadExecutor.inject(append).commit();
          };
      ScriptRun.Actions actions =
          (workload, table, seedFiles) -> {
            workload.registerAction("rewrite-aging", (t, o) -> rewrite(t, st, false, o));
            workload.registerAction("txn-rewrite-aging", (t, o) -> rewrite(t, st, true, o));
            workload.registerAction(
                "remove-D",
                (t, o) -> {
                  org.apache.iceberg.RowDelta delta = t.newRowDelta().removeDeletes(st.d);
                  CommitUuid.makeDeterministic(delta);
                  SameThreadExecutor.inject(delta).commit();
                });
          };
      ScriptRun.Ctx ctx = ScriptRun.run(seed, config(), setup, actions, scripts);
      RunResult result = ctx.result;
      row.seeds++;
      for (FakeTableOperations.CommitRecord r : result.ops.commits()) {
        if (r.fiber.equals("w1") && "stale".equals(r.answer)) {
          row.retried++;
          break;
        }
      }
      for (Workload.Outcome o : result.outcomes) {
        if (!o.action.contains("rewrite-aging")) {
          continue;
        }
        if (o.reportedSuccess) {
          row.rewriteOk++;
          continue;
        }
        row.rewriteFailed++;
        Throwable t = st.rewriteFailure;
        String cls = t == null ? "?" : t.getClass().getSimpleName();
        row.failureClasses.merge(cls, 1, Integer::sum);
        if (t instanceof ValidationException) {
          String m = String.valueOf(t.getMessage());
          boolean reportsD = m.contains(st.d.location());
          boolean reportsCallerFile =
              m.contains(st.seedA.location()) || m.contains(st.seedB.location());
          if (reportsD && !reportsCallerFile) {
            row.validationReportingOnlyD++;
          }
        }
        if (deleteFilesContains(st.rewriteProducer, st.d)) {
          row.dStickyInFilterManager++;
        }
        if (row.first == null) {
          row.first =
              "seed " + seed + ": " + result.outcomes + "\n  " + t + "\n  D = " + st.d.location();
        }
      }
      for (Violation v : result.unexplained()) {
        row.unexplained.merge(v.invariant, 1, Integer::sum);
      }
    }
    return row;
  }

  /** Reads {@code deleteFilterManager.deleteFiles} of the producer and looks for D by path. */
  static boolean deleteFilesContains(Object producer, DeleteFile d) {
    if (producer == null) {
      return false;
    }
    try {
      Object manager = field(producer, "deleteFilterManager");
      Object files = field(manager, "deleteFiles");
      for (Object f : (Iterable<?>) files) {
        if (((org.apache.iceberg.ContentFile<?>) f).location().equals(d.location())) {
          return true;
        }
      }
      return false;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Object field(Object target, String name) throws ReflectiveOperationException {
    for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
      try {
        java.lang.reflect.Field f = c.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
      } catch (NoSuchFieldException e) {
        // walk up
      }
    }
    throw new NoSuchFieldException(name);
  }

  @Test
  @DisplayName("A1-18: a re-applied RewriteFiles demands the delete file it aged off in pass 1")
  void stickyDeleteFilesFailTheReapply() {
    Row[] rows = {
      sweep("control: rewrite alone", new String[] {"rewrite-aging"}),
      sweep("control: txn rewrite alone", new String[] {"txn-rewrite-aging"}),
      sweep("control serial: remove-D then rewrite", new String[] {"remove-D", "rewrite-aging"}),
      sweep("control: rewrite || unrelated append", new String[] {"rewrite-aging"},
          new String[] {"append"}),
      sweep("control: txn rewrite || unrelated append", new String[] {"txn-rewrite-aging"},
          new String[] {"append"}),
      sweep("trigger: rewrite || remove-D", new String[] {"rewrite-aging"},
          new String[] {"remove-D"}),
      sweep("trigger: txn rewrite || remove-D", new String[] {"txn-rewrite-aging"},
          new String[] {"remove-D"}),
    };
    StringBuilder out = new StringBuilder();
    out.append("=== A1-18 ManifestFilterManager.deleteFiles across re-apply, iceberg ")
        .append(System.getProperty("icebergVersion", "?")).append(", ").append(SEEDS)
        .append(" seeds per row ===\n");
    for (Row r : rows) {
      out.append(r).append('\n');
    }
    for (Row r : rows) {
      if (r.first != null) {
        out.append("\nfirst failure in [").append(r.label).append("]\n  ").append(r.first).append('\n');
      }
    }
    Reports.write("r5-a1-18-required-deletes.txt", out.toString());
    // Controls: the compaction must succeed whenever nothing removed D under it.
    for (int i = 0; i < 5; i++) {
      assertEquals(0, rows[i].rewriteFailed, out.toString());
    }
  }
}
