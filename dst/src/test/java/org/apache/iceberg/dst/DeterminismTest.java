package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The property the whole method rests on: a seed fixes the run. If this test is red, every number
 * in the scorecard is noise.
 */
class DeterminismTest {

  @Test
  @DisplayName("the same seed produces a byte-identical decision log twice")
  void sameSeedSameTrace() {
    for (long seed = 1; seed <= 40; seed++) {
      RunResult first = Simulation.run(seed, FaultConfig.aggressive());
      RunResult second = Simulation.run(seed, FaultConfig.aggressive());
      assertEquals(
          first.trace.sha256(),
          second.trace.sha256(),
          "seed " + seed + " produced two different decision logs");
      assertEquals(first.trace.lineCount(), second.trace.lineCount(), "seed " + seed);
      assertEquals(first.trace.tail(), second.trace.tail(), "seed " + seed);
      assertEquals(
          describe(first), describe(second), "seed " + seed + " produced two different outcomes");
    }
  }

  @Test
  @DisplayName("a seed replays identically after hundreds of other seeds have run")
  void noStateLeaksBetweenRuns() {
    // Clearing state between runs is the other half of determinism: a cached manifest, a static
    // pool or a leftover pointer would show up here and nowhere else.
    String before = Simulation.run(7L, FaultConfig.aggressive()).trace.sha256();
    Sweep.run(1_000L, 200, FaultConfig::aggressive);
    String after = Simulation.run(7L, FaultConfig.aggressive()).trace.sha256();
    assertEquals(before, after, "seed 7 replayed differently after 200 other seeds");
  }

  @Test
  @DisplayName("different seeds explore different interleavings")
  void seedsDiffer() {
    List<String> hashes = new ArrayList<>();
    for (long seed = 1; seed <= 40; seed++) {
      hashes.add(Simulation.run(seed, FaultConfig.aggressive()).trace.sha256());
    }
    long distinct = hashes.stream().distinct().count();
    assertTrue(
        distinct > 30,
        "only " + distinct + " of 40 seeds produced a distinct schedule; the search is too narrow");
  }

  private static String describe(RunResult result) {
    StringBuilder sb = new StringBuilder();
    for (Workload.Outcome outcome : result.outcomes) {
      sb.append(outcome).append('\n');
    }
    for (FakeTableOperations.CommitRecord record : result.ops.commits()) {
      sb.append(record).append('\n');
    }
    for (Violation violation : result.violations) {
      sb.append(violation).append('\n');
    }
    return sb.toString();
  }
}
