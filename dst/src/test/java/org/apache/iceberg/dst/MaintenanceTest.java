package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The two operations whose job is to delete files that something referenced recently.
 *
 * <p>These are the only places where deleting a file is legal, which is why I5 had to learn the
 * retention rule: before this it said "nothing a committed snapshot ever referenced may be
 * deleted", and that calls every correct expiry a defect.
 */
class MaintenanceTest {

  private static final int SEEDS = Integer.getInteger("dst.maintenance.seeds", 500);

  @Test
  @DisplayName("snapshot expiry concurrent with writers deletes files and breaks nothing")
  void expiryIsSafe() {
    Sweep.Summary withFaults = Sweep.run(1L, SEEDS, FaultConfig::expiry);
    Sweep.Summary quiet = Sweep.run(1L, SEEDS, FaultConfig::expiryQuiet);

    // The run has to have done the dangerous thing, or a clean sweep says nothing.
    assertTrue(quiet.expiriesCommitted > 0, "no expiry committed: " + quiet);
    assertTrue(quiet.filesDeleted > 0, "the expiry deleted nothing: " + quiet);
    assertTrue(withFaults.expiriesCommitted > 0, "no expiry committed: " + withFaults);

    assertEquals(0, quiet.failing, "expiry broke an invariant:\n" + quiet.firstFailureReport);
    assertEquals(
        0, withFaults.failing, "expiry broke an invariant:\n" + withFaults.firstFailureReport);
  }

  @Test
  @DisplayName("an orphan cleaner with no age cutoff destroys the table, and I5 sees it")
  void orphanCleaningWithoutACutoffIsCaught() {
    Sweep.Summary summary = Sweep.run(1L, 200, () -> FaultConfig.orphanClean(0));
    assertTrue(
        summary.failing > summary.seeds / 4,
        "deleting every unreachable file the instant it appears should break most seeds, but only "
            + summary.failing
            + " of "
            + summary.seeds
            + " broke: "
            + summary);
    assertTrue(
        summary.byInvariant.containsKey("I5") || summary.byInvariant.containsKey("I1-present"),
        "caught by the wrong invariants: " + summary.byInvariant);
  }

  @Test
  @DisplayName("an age cutoff past the write-to-commit window makes orphan cleaning safe")
  void orphanCleaningWithACutoffIsSafe() {
    // The window in this workload closes somewhere between 20 and 30 scheduler steps; a real
    // write-to-commit gap is minutes to hours, which is why the Spark action defaults to three
    // days. The point of the ladder is that the cutoff is the *only* thing protecting an in-flight
    // write: nothing in the commit protocol tells a cleaner to leave a file alone.
    Sweep.Summary generous = Sweep.run(1L, SEEDS, () -> FaultConfig.orphanClean(200));
    assertTrue(generous.filesDeleted > 0, "the cleaner never deleted anything: " + generous);
    assertEquals(
        0,
        generous.failing,
        "a generous cutoff should be safe:\n" + generous.firstFailureReport);

    Sweep.Summary tight = Sweep.run(1L, SEEDS, () -> FaultConfig.orphanClean(10));
    assertTrue(
        tight.failing > 0,
        "a cutoff of 10 steps should still catch some writers mid-flight: " + tight);
  }
}
