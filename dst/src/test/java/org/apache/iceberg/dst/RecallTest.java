package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the recall scorecard.
 *
 * <p>A harness that no longer catches a bug it used to catch has quietly stopped working. Each test
 * here reinstates a behaviour from Iceberg's history, or exercises one that is still unguarded, and
 * insists the harness notices. The control for every row is the same sweep with the switch off,
 * which must stay clean.
 *
 * <p>The numbers these tests assert are lower bounds, not the measured rates; the measured rates
 * are in findings/r2-dst-harness.md and are reproduced by {@code ./scorecard.sh}.
 */
class RecallTest {

  private static final int SEEDS = Integer.getInteger("dst.recall.seeds", 500);

  @Test
  @DisplayName("A: a landed commit reported as a plain failure is caught")
  void landedCommitReportedAsFailure() {
    Sweep.Summary preFix =
        Sweep.run(
            1L,
            SEEDS,
            () -> catalogOnly().preFix(FaultConfig.PreFix.RETRY_ON_UNKNOWN_STATE));
    Sweep.Summary control = Sweep.run(1L, SEEDS, RecallTest::catalogOnly);
    assertEquals(0, control.failing, "the control sweep is not clean: " + control);
    assertTrue(
        preFix.failing > 0,
        "the pre-CommitStateUnknownException behaviour was not caught in "
            + SEEDS
            + " seeds: "
            + preFix);
    // The damage is a committed snapshot losing files, or the same payload landing twice.
    assertTrue(
        preFix.byInvariant.containsKey("I5")
            || preFix.byInvariant.containsKey("I1-present")
            || preFix.byInvariant.containsKey("I3"),
        "caught by the wrong invariants: " + preFix.byInvariant);
  }

  @Test
  @DisplayName("E: a rewrite that supplies a sequence number ahead of the table is caught")
  void sequenceNumberAheadOfTheTable() {
    Sweep.Summary ahead =
        Sweep.run(
            1L,
            SEEDS,
            () -> {
              FaultConfig config = FaultConfig.quiet();
              config.sequenceAheadRewrite = true;
              return config;
            });
    Sweep.Summary control = Sweep.run(1L, SEEDS, FaultConfig::quiet);
    assertEquals(0, control.failing, "the control sweep is not clean: " + control);
    assertTrue(ahead.failing > 0, "no seed noticed the sequence number: " + ahead);
    assertTrue(
        ahead.byInvariant.containsKey("I8"),
        "I8 should see a file ordered after deletes that do not exist: " + ahead.byInvariant);
  }

  @Test
  @DisplayName("F: a catalog whose pointer is not atomic is caught on every seed")
  void nonAtomicCatalogPointer() {
    Sweep.Summary preFix =
        Sweep.run(
            1L,
            200,
            () -> FaultConfig.quiet().selfCheck(FaultConfig.SelfCheck.NON_ATOMIC_POINTER));
    assertEquals(200, preFix.failing, "a lost update went unnoticed: " + preFix);
  }

  @Test
  @DisplayName("C: the duplicate snapshot id guard rejects every duplicate the harness hands out")
  void duplicateSnapshotIdIsRejected() {
    Sweep.Summary summary =
        Sweep.run(1L, SEEDS, () -> new FaultConfig().preFix(FaultConfig.PreFix.DUPLICATE_SNAPSHOT_ID));
    assertTrue(
        summary.duplicateIdsHandedOut > 0,
        "the harness never handed out a duplicate snapshot id: " + summary);
    assertEquals(
        0,
        summary.failing,
        "the guard from apache/iceberg#4747 did not hold: " + summary.firstFailureReport);
  }

  @Test
  @DisplayName("D: a rewrite with no starting sequence number is refused, not silently applied")
  void rewriteWithoutSequenceNumberIsRefused() {
    Sweep.Summary summary =
        Sweep.run(
            1L,
            SEEDS,
            () -> new FaultConfig().preFix(FaultConfig.PreFix.REWRITE_WITHOUT_SEQUENCE_NUMBER));
    assertEquals(
        0,
        summary.failing,
        "equality-delete conflict detection did not hold: " + summary.firstFailureReport);
  }

  @Test
  @DisplayName("G: the duplicate-delete workload reaches the state #16686 miscounted")
  void duplicateDeleteWorkloadReachesTheState() {
    Sweep.Summary summary = Sweep.run(1L, 200, FaultConfig::duplicateDeletes);
    // The workload has to actually get there: a duplicate entry deleted, and writers that lost the
    // race and retried. Without both, a clean sweep on the current release would mean nothing.
    assertTrue(
        summary.duplicateDeleteSummaries > 0,
        "no snapshot reported a duplicate delete, so the workload never built the state: "
            + summary);
    assertTrue(summary.commitsStale > 0, "no writer ever retried: " + summary);
    assertEquals(
        0,
        summary.failing,
        "I9 fired on the current release: " + summary.firstFailureReport);
    // The pre-fix measurement needs the pre-fix jar and so cannot live in this test:
    //   ICEBERG_VERSION=1.11.0 ./run.sh sweep 1000 dupdelete   -> 1000/1000 seeds, I9 x2000
    // ./versions.sh runs it for every release the harness supports.
  }

  private static FaultConfig catalogOnly() {
    FaultConfig config = FaultConfig.quiet();
    config.commitRejectPct = 6;
    config.commitUnknownAfterLandingPct = 6;
    config.commitUnknownWithoutLandingPct = 4;
    return config;
  }
}
