package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/** Runs a range of seeds and counts what happened. */
public final class Sweep {

  /** How many seeds did what. */
  public static final class Summary {
    public final int seeds;
    public int failing;
    public int hung;
    public int harnessFailures;
    public final List<Long> failingSeeds = new ArrayList<>();
    public final Map<String, Integer> byInvariant = new LinkedHashMap<>();
    public final Map<String, Integer> excusedByInvariant = new LinkedHashMap<>();
    public int seedsWithExcusedOnly;
    public final Map<String, Integer> writerOutcomes = new LinkedHashMap<>();
    public int commitsLanded;
    public int commitsStale;
    public int commitsRejected;
    public int commitsUnknown;
    public int truncatedWrites;
    public int orphanWrites;
    public long steps;
    public long duplicateIdsHandedOut;
    /** Committed rewrites, and how many supplied a number above the replaced file's own. */
    public long rewritesCommitted;
    public long rewritesSupplyingAboveReplaced;
    /** Snapshots whose summary reports a duplicate delete, however it got there. */
    public long duplicateDeleteSummaries;
    /** Files removed from the store, and expiries that committed. */
    public long filesDeleted;
    public long expiriesCommitted;
    public long snapshotsExpired;
    public long manifestRewritesCommitted;
    /** The full report of the first failing seed, ready to paste into a finding. */
    public String firstFailureReport;

    Summary(int seeds) {
      this.seeds = seeds;
    }

    public double failRate() {
      return seeds == 0 ? 0 : (double) failing / seeds;
    }

    @Override
    public String toString() {
      return failing
          + "/"
          + seeds
          + " seeds with an unexplained violation"
          + (failingSeeds.isEmpty() ? "" : ", firstFailingSeed=" + failingSeeds.get(0))
          + (hung > 0 ? ", " + hung + " hung" : "")
          + (harnessFailures > 0 ? ", " + harnessFailures + " harness failures" : "")
          + ", unexplained="
          + byInvariant
          + ", excused="
          + excusedByInvariant
          + " (on "
          + seedsWithExcusedOnly
          + " seeds)"
          + ", commits{landed="
          + commitsLanded
          + " stale="
          + commitsStale
          + " rejected="
          + commitsRejected
          + " unknown="
          + commitsUnknown
          + "}, writes{truncated="
          + truncatedWrites
          + " orphan="
          + orphanWrites
          + "}, steps="
          + steps
          + (duplicateIdsHandedOut > 0 ? ", duplicateIdsHandedOut=" + duplicateIdsHandedOut : "")
          + (filesDeleted > 0 ? ", filesDeleted=" + filesDeleted : "")
          + (expiriesCommitted > 0 ? ", expiriesCommitted=" + expiriesCommitted : "")
          + (manifestRewritesCommitted > 0
              ? ", manifestRewritesCommitted=" + manifestRewritesCommitted
              : "")
          + (duplicateDeleteSummaries > 0
              ? ", duplicateDeleteSummaries=" + duplicateDeleteSummaries
              : "")
          + (rewritesCommitted > 0
              ? ", rewritesCommitted="
                  + rewritesCommitted
                  + " ofWhichSuppliedAboveReplaced="
                  + rewritesSupplyingAboveReplaced
              : "");
    }
  }

  private Sweep() {}

  public static Summary run(long firstSeed, int seeds, Supplier<FaultConfig> config) {
    return run(firstSeed, seeds, seed -> config.get());
  }

  /** For swarm testing, where the configuration is itself a function of the seed. */
  public static Summary run(long firstSeed, int seeds, LongFunction<FaultConfig> config) {
    Summary summary = new Summary(seeds);
    for (int i = 0; i < seeds; i++) {
      long seed = firstSeed + i;
      RunResult result = Simulation.run(seed, config.apply(seed));
      summary.steps += result.scheduler.steps();
      for (FakeTableOperations.CommitRecord record : result.ops.commits()) {
        switch (record.answer) {
          case "ok":
            summary.commitsLanded++;
            break;
          case "stale":
            summary.commitsStale++;
            break;
          case "rejected":
            summary.commitsRejected++;
            break;
          default:
            summary.commitsUnknown++;
            break;
        }
      }
      summary.duplicateIdsHandedOut += result.ops.duplicateIdsHandedOut();
      for (org.apache.iceberg.Snapshot snapshot : result.finalMetadata.snapshots()) {
        java.util.Map<String, String> snapshotSummary = snapshot.summary();
        if (snapshotSummary != null && snapshotSummary.containsKey("deleted-duplicate-files")) {
          summary.duplicateDeleteSummaries++;
        }
      }
      summary.filesDeleted += result.store.deleteRecords().size();
      for (Workload.Outcome outcome : result.outcomes) {
        if (outcome.manifestRewriteSnapshotId != null && outcome.reportedSuccess) {
          summary.manifestRewritesCommitted++;
        }
        if (outcome.expiredSnapshotId != null && outcome.reportedSuccess) {
          summary.expiriesCommitted++;
          summary.snapshotsExpired++;
        }
      }
      summary.truncatedWrites += result.store.truncatedPaths().size();
      summary.orphanWrites += result.store.orphans().size();
      for (Workload.Outcome outcome : result.outcomes) {
        if (outcome.reportedSuccess
            && !outcome.replacedDataFiles.isEmpty()
            && outcome.suppliedSequenceNumber != null) {
          summary.rewritesCommitted++;
          // The seed files every run starts from are appended in the bootstrap snapshot, so their
          // data sequence number is 1. This counts how often a faithful caller — one that mirrors
          // RewriteDataFilesCommitManager and supplies the starting snapshot's sequence number —
          // ends up above the files it is replacing.
          if (outcome.suppliedSequenceNumber > 1L) {
            summary.rewritesSupplyingAboveReplaced++;
          }
        }
        String key =
            outcome.action
                + (outcome.reportedSuccess
                    ? " ok"
                    : " " + firstWord(outcome.failure));
        summary.writerOutcomes.merge(key, 1, Integer::sum);
      }
      if (result.scheduler.hung()) {
        summary.hung++;
      }
      if (result.harnessFailure != null) {
        summary.harnessFailures++;
      }
      for (Violation violation : result.violations) {
        if (violation.excused()) {
          summary.excusedByInvariant.merge(violation.invariant, 1, Integer::sum);
        } else {
          summary.byInvariant.merge(violation.invariant, 1, Integer::sum);
        }
      }
      if (!result.violations.isEmpty() && result.clean()) {
        summary.seedsWithExcusedOnly++;
      }
      if (!result.clean()) {
        summary.failing++;
        summary.failingSeeds.add(seed);
        if (summary.firstFailureReport == null) {
          summary.firstFailureReport = result.report();
        }
      }
    }
    return summary;
  }

  private static String firstWord(String failure) {
    if (failure == null) {
      return "failed";
    }
    int colon = failure.indexOf(':');
    return colon < 0 ? failure : failure.substring(0, colon);
  }
}
