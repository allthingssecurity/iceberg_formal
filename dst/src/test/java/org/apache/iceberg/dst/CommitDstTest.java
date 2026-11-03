package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The harness proper: thousands of seeds of concurrent commits through the real
 * {@code SnapshotProducer} against a fake catalog and a fake store, with faults drawn from the
 * seed.
 *
 * <p>Seed count comes from {@code -Ddst.seeds=N} (default 500, so a plain {@code ./gradlew test}
 * stays quick). {@code -Ddst.seed=N} replays one seed and writes its full decision log to
 * {@code build/dst/}.
 */
class CommitDstTest {

  private static final int SEEDS = Integer.getInteger("dst.seeds", 500);
  private static final Long ONE_SEED = Long.getLong("dst.seed");

  @Test
  @DisplayName("concurrent commits hold every invariant, default faults")
  void defaultFaults() {
    sweep("default", FaultConfig::new);
  }

  @Test
  @DisplayName("concurrent commits hold every invariant, aggressive faults")
  void aggressiveFaults() {
    sweep("aggressive", FaultConfig::aggressive);
  }

  @Test
  @DisplayName("concurrent commits hold every invariant with a catalog that loses answers")
  void catalogFaultsOnly() {
    sweep(
        "catalog-only",
        () -> {
          FaultConfig config = FaultConfig.quiet();
          config.commitRejectPct = 6;
          config.commitUnknownAfterLandingPct = 6;
          config.commitUnknownWithoutLandingPct = 4;
          return config;
        });
  }

  @Test
  @DisplayName("more writers, more interleavings")
  void fiveWriters() {
    sweep("five-writers", () -> new FaultConfig().writers(5));
  }

  private void sweep(String label, Supplier<FaultConfig> config) {
    if (ONE_SEED != null) {
      replay(label, ONE_SEED, config.get());
      return;
    }
    Sweep.Summary summary = Sweep.run(1L, SEEDS, config);
    // The run has to have gone somewhere: a sweep that commits nothing proves nothing.
    assertTrue(summary.commitsLanded > SEEDS, label + " landed too few commits: " + summary);
    assertEquals(0, summary.hung, label + " had runs that did not finish: " + summary);
    assertEquals(0, summary.harnessFailures, label + " had harness failures: " + summary);
    if (summary.failing > 0) {
      fail(
          label
              + ": "
              + summary
              + "\nfirst failing seed "
              + summary.failingSeeds.get(0)
              + ":\n"
              + summary.firstFailureReport);
    }
  }

  /** Replays one seed and leaves the whole decision log on disk. */
  private void replay(String label, long seed, FaultConfig config) {
    RunResult result = Simulation.run(seed, config);
    try {
      Path dir = Paths.get("build", "dst");
      Files.createDirectories(dir);
      Path file = dir.resolve(label + "-seed-" + seed + ".txt");
      Files.write(file, result.report().getBytes(StandardCharsets.UTF_8));
      System.out.println("wrote " + file.toAbsolutePath());
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    if (!result.clean()) {
      fail(result.report());
    }
  }
}
