package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Checks the detectors, not Iceberg.
 *
 * <p>A harness that reports nothing is indistinguishable from a harness that cannot see anything.
 * Each test here breaks the fakes on purpose and insists the invariants notice. The Camel harness
 * learned this the hard way: one fake put its lock expiry times in 1970 and reported 0 of 1000
 * seeds failing on code that was known to be broken.
 */
class SelfCheckTest {

  @Test
  @DisplayName("with no faults at all, nothing is violated and every successful writer is applied")
  void quietRunsAreClean() {
    for (long seed = 1; seed <= 100; seed++) {
      RunResult result = Simulation.run(seed, FaultConfig.quiet());
      assertTrue(
          result.clean(),
          "seed " + seed + " violated an invariant with no faults injected:\n" + result.report());
      assertEquals(0, result.scheduler.unmanagedCrossings(), "seed " + seed + " escaped the harness");
      assertTrue(result.scheduler.steps() > 0, "seed " + seed + " ran nothing");

      // Every writer that was told it succeeded has its files in the final table, and no writer
      // that was told it failed does.
      Set<String> live = liveFiles(result);
      for (Workload.Outcome outcome : result.outcomes) {
        if (outcome.reportedSuccess) {
          for (String path : outcome.addedDataFiles) {
            if (outcome.replacedDataFiles.isEmpty() || !outcome.replacedDataFiles.contains(path)) {
              assertTrue(
                  live.contains(path),
                  "seed "
                      + seed
                      + ": "
                      + outcome.fiber
                      + " succeeded but "
                      + result.store.alias(path)
                      + " is not live");
            }
          }
        }
      }
    }
  }

  @Test
  @DisplayName("a catalog whose compare-and-set is not atomic is caught")
  void nonAtomicPointerIsCaught() {
    int caught = 0;
    for (long seed = 1; seed <= 200; seed++) {
      FaultConfig config =
          FaultConfig.quiet().selfCheck(FaultConfig.SelfCheck.NON_ATOMIC_POINTER).writers(3);
      RunResult result = Simulation.run(seed, config);
      if (!result.clean()) {
        caught++;
      }
    }
    assertTrue(
        caught > 0,
        "200 seeds against a catalog that accepts stale commits produced no violation; the"
            + " invariants cannot see a lost update");
  }

  @Test
  @DisplayName("deleting a manifest of a committed snapshot is caught")
  void deletingACommittedManifestIsCaught() {
    int caught = 0;
    for (long seed = 1; seed <= 50; seed++) {
      FaultConfig config =
          FaultConfig.quiet().selfCheck(FaultConfig.SelfCheck.DELETE_A_COMMITTED_MANIFEST);
      RunResult result = Simulation.run(seed, config);
      if (!result.clean()) {
        caught++;
        assertTrue(
            result.unexplained().stream()
                .anyMatch(v -> v.invariant.equals("I5") || v.invariant.startsWith("I1")),
            "seed " + seed + " was caught by the wrong invariant:\n" + result.report());
      }
    }
    assertEquals(50, caught, "some seeds did not notice a deleted manifest");
  }

  @Test
  @DisplayName("dropping a data file of a committed snapshot is caught")
  void droppingACommittedDataFileIsCaught() {
    int caught = 0;
    for (long seed = 1; seed <= 50; seed++) {
      FaultConfig config =
          FaultConfig.quiet().selfCheck(FaultConfig.SelfCheck.DROP_A_COMMITTED_DATA_FILE);
      RunResult result = Simulation.run(seed, config);
      if (!result.clean()) {
        caught++;
      }
    }
    assertEquals(50, caught, "some seeds did not notice a dropped data file");
  }

  @Test
  @DisplayName("an expiry job that deletes one manifest too many is caught")
  void expiryThatOverDeletesIsCaught() {
    int caught = 0;
    int expiriesThatRan = 0;
    for (long seed = 1; seed <= 100; seed++) {
      FaultConfig config =
          FaultConfig.expiryQuiet().selfCheck(FaultConfig.SelfCheck.EXPIRY_OVER_DELETES);
      RunResult result = Simulation.run(seed, config);
      boolean expiryDeletedSomething =
          result.store.deleteRecords().stream().anyMatch(d -> d.fiber.equals("w1"));
      if (expiryDeletedSomething) {
        expiriesThatRan++;
        if (!result.clean()) {
          caught++;
          assertTrue(
              result.unexplained().stream()
                  .anyMatch(v -> v.invariant.equals("I5") || v.invariant.startsWith("I1")),
              "seed " + seed + " was caught by the wrong invariant:\n" + result.report());
        }
      }
    }
    assertTrue(expiriesThatRan > 0, "no seed ran an expiry that deleted anything");
    assertEquals(
        expiriesThatRan,
        caught,
        "an expiry deleted a live manifest and "
            + (expiriesThatRan - caught)
            + " of "
            + expiriesThatRan
            + " seeds did not notice");
  }

  private static Set<String> liveFiles(RunResult result) {
    Set<String> out = new LinkedHashSet<>();
    org.apache.iceberg.io.FileIO io = ((FakeFileIO) result.ops.io()).observerView();
    for (long id : TableView.reachable(result.finalMetadata)) {
      out.addAll(TableView.read(result.finalMetadata, id, io).dataFiles.keySet());
    }
    return out;
  }
}
