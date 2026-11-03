package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * r5, candidate A2-9: IncrementalFileCleanup reads the manifest lists of the snapshots an expiry
 * RETAINS with {@code retry(3).suppressFailureWhenFinished()} (IncrementalFileCleanup.java:120-123).
 * A read that fails four times in a row is swallowed, {@code validManifests} is then missing that
 * snapshot's manifests, and a manifest the retained snapshot shares with an expired one is deleted
 * (IncrementalFileCleanup.java:224-226, deleted at :282).
 *
 * <p>Three rows, same harness, same seeds, the real {@code RemoveSnapshots} concurrent with
 * main-only writers:
 *
 * <ul>
 *   <li>control: no fault. Nothing reachable may be lost.
 *   <li>trigger: a manifest-list read fault confined to the cleanup's reads of retained snapshots,
 *       Iceberg left to choose the cleanup strategy (incremental, on a main-only table).
 *   <li>calibration: the same fault with ReachableFileCleanup forced. Its equivalent loop uses
 *       {@code throwFailureWhenFinished()} (ReachableFileCleanup.java:115-118), so the expiry must
 *       throw and delete no manifest. If it did not, the fault would be the cause, not Iceberg.
 * </ul>
 *
 * <p>The cleanup's retries back off 1 s, 2 s, 4 s (Tasks defaults), so a faulted seed costs about 7
 * seconds per faulted snapshot of real time. Seed count: {@code -Ddst.r5.readfault.seeds} (default
 * 3); the numbers in findings/r5-dst-core.md were taken with more.
 */
class ExpiryReadFaultTest {

  private static final int SEEDS = Integer.getInteger("dst.r5.readfault.seeds", 3);

  static final class Row {
    final String label;
    int seeds;
    int seedsWithLoss;
    int i1Present;
    int i1Readable;
    int i5;
    int otherUnexplained;
    int expirySucceeded;
    int expiryFailed;
    final Map<String, Integer> expiryFailureClasses = new LinkedHashMap<>();
    int manifestsDeletedByExpiry;
    int reachableManifestsDeletedByExpiry;
    final Map<String, Integer> faultsBySite = new LinkedHashMap<>();
    final Map<String, Integer> cleanupReadsBySite = new LinkedHashMap<>();
    String firstLoss;
    final Map<String, Integer> unexplainedByInvariant = new LinkedHashMap<>();

    Row(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return String.format(
          "%-44s seeds=%d lossSeeds=%d I1-present=%d I1-readable=%d I5=%d other=%d"
              + " expiry{ok=%d failed=%d %s} expiryDeletedManifests=%d"
              + " ofWhichReachableAtEnd=%d%n      unexplained=%s%n      injectedFaults=%s%n"
              + "      cleanupReads=%s",
          label,
          seeds,
          seedsWithLoss,
          i1Present,
          i1Readable,
          i5,
          otherUnexplained,
          expirySucceeded,
          expiryFailed,
          expiryFailureClasses,
          manifestsDeletedByExpiry,
          reachableManifestsDeletedByExpiry,
          unexplainedByInvariant,
          faultsBySite,
          cleanupReadsBySite);
    }
  }

  static Row sweep(String label, FaultConfig template, long firstSeed, int seeds) {
    Row row = new Row(label);
    for (long seed = firstSeed; seed < firstSeed + seeds; seed++) {
      FaultConfig config = copy(template);
      RunResult result = Simulation.run(seed, config);
      row.seeds++;
      for (FakeStore.ReadFaultRecord fault : result.store.readFaults()) {
        row.faultsBySite.merge(fault.toString(), 1, Integer::sum);
      }
      result.store.cleanupReadsBySite().forEach((k, v) -> row.cleanupReadsBySite.merge(k, v, Integer::sum));
      for (Workload.Outcome outcome : result.outcomes) {
        if (!outcome.action.startsWith("expire")) {
          continue;
        }
        if (outcome.reportedSuccess) {
          row.expirySucceeded++;
        } else {
          row.expiryFailed++;
          String cls = outcome.failure == null ? "?" : outcome.failure.split(":")[0];
          row.expiryFailureClasses.merge(cls, 1, Integer::sum);
        }
      }
      // Which manifests the expiry fiber (w1) removed, and which of those a ref can still reach.
      Set<String> reachableAtEnd = new LinkedHashSet<>();
      for (long id : TableView.reachable(result.finalMetadata)) {
        TableView.SnapshotFiles files = TableView.read(result.finalMetadata, id, observer(result));
        reachableAtEnd.addAll(files.manifests.keySet());
      }
      for (FakeStore.DeleteRecord delete : result.store.deleteRecords()) {
        if (!"w1".equals(delete.fiber) || !"man".equals(FakeStore.kindOf(delete.path))) {
          continue;
        }
        row.manifestsDeletedByExpiry++;
        if (reachableAtEnd.contains(delete.path)) {
          row.reachableManifestsDeletedByExpiry++;
        }
      }
      boolean loss = false;
      for (Violation v : result.unexplained()) {
        row.unexplainedByInvariant.merge(v.invariant, 1, Integer::sum);
        switch (v.invariant) {
          case "I1-present":
            row.i1Present++;
            loss = true;
            break;
          case "I1-readable":
            row.i1Readable++;
            loss = true;
            break;
          case "I5":
            row.i5++;
            break;
          default:
            row.otherUnexplained++;
            break;
        }
      }
      if (loss) {
        row.seedsWithLoss++;
        if (row.firstLoss == null) {
          StringBuilder sb = new StringBuilder();
          sb.append("seed ").append(seed).append('\n');
          for (Workload.Outcome o : result.outcomes) {
            sb.append("  ").append(o).append('\n');
          }
          int n = 0;
          for (Violation v : result.unexplained()) {
            if (n++ < 6) {
              sb.append("  ").append(v).append('\n');
            }
          }
          sb.append("  injected read faults: ").append(result.store.readFaults()).append('\n');
          row.firstLoss = sb.toString();
        }
      }
    }
    return row;
  }

  private static org.apache.iceberg.io.FileIO observer(RunResult result) {
    return ((FakeFileIO) result.ops.io()).observerView();
  }

  private static FaultConfig copy(FaultConfig t) {
    FaultConfig c = FaultConfig.quiet();
    c.expireWorkload = t.expireWorkload;
    c.seedSnapshots = t.seedSnapshots;
    c.writers = t.writers;
    c.expireRetainLast = t.expireRetainLast;
    c.expireIncrementalCleanup = t.expireIncrementalCleanup;
    c.readFailManifestListPct = t.readFailManifestListPct;
    c.readFailScope = t.readFailScope;
    return c;
  }

  @Test
  @DisplayName("A2-9: a swallowed read of a retained manifest list deletes a live manifest")
  void swallowedReadDeletesLiveManifest() {
    Row control = sweep("control: default strategy, no fault", FaultConfig.expiryReadFault("default", 0), 1, SEEDS);
    Row trigger =
        sweep("trigger: default strategy, 100% retained-list reads", FaultConfig.expiryReadFault("default", 100), 1, SEEDS);
    Row calibration =
        sweep("calibration: reachable forced, same fault", FaultConfig.expiryReadFault("reachable", 100), 1, SEEDS);
    Row controlReachable =
        sweep("control: reachable forced, no fault", FaultConfig.expiryReadFault("reachable", 0), 1, SEEDS);

    int transientSeeds = Integer.getInteger("dst.r5.readfault.transientSeeds", 0);
    Row transientRow =
        transientSeeds <= 0
            ? null
            : sweep(
                "transient: default strategy, 50% per read",
                FaultConfig.expiryReadFault("default", 50),
                1,
                transientSeeds);
    StringBuilder out = new StringBuilder();
    out.append("=== A2-9 IncrementalFileCleanup read-failure, iceberg ")
        .append(System.getProperty("icebergVersion", "?"))
        .append(", seeds 1..")
        .append(SEEDS)
        .append(" ===\n");
    for (Row row : new Row[] {control, trigger, calibration, controlReachable}) {
      out.append(row).append('\n');
    }
    if (transientRow != null) {
      out.append(transientRow).append('\n');
    }
    out.append("\nfirst trigger loss:\n").append(trigger.firstLoss);
    Reports.write("r5-a2-9-readfault.txt", out.toString());

    // The control: nothing reachable lost, and the expiry did real work.
    assertEquals(0, control.seedsWithLoss, out.toString());
    assertEquals(0, control.reachableManifestsDeletedByExpiry, out.toString());
    assertEquals(0, controlReachable.seedsWithLoss, out.toString());
    // Calibration of the fault: under ReachableFileCleanup the same fault must make the expiry throw
    // and delete no manifest at all.
    assertEquals(0, calibration.seedsWithLoss, out.toString());
    assertEquals(0, calibration.manifestsDeletedByExpiry, out.toString());
    assertEquals(calibration.seeds, calibration.expiryFailed, out.toString());
    // Every injected fault in every row hit a retained snapshot's list, never an expired one.
    for (Row row : new Row[] {trigger, calibration}) {
      for (String site : row.faultsBySite.keySet()) {
        assertTrue(site.endsWith("/retained"), "fault outside its scope: " + row.faultsBySite);
      }
    }
    // The trigger itself is recorded, not asserted either way: whether Iceberg loses the manifest
    // is the finding, and the test must not go red if a later release fixes it.
  }
}
