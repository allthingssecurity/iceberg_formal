package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * r5, candidate A2-1: {@code RemoveSnapshots.idsToRemove} (RemoveSnapshots.java:69) is a field that
 * {@code internalApply()} adds to on every pass (:233-236) and nothing clears, while
 * {@code commit()} re-runs {@code internalApply()} on every CommitFailedException (:361-373).
 *
 * <p>The invariant this needs, R ("retention"): let E be an expiry whose commit landed, B the
 * metadata it committed against (the catalog pointer it compare-and-set from) and A the metadata it
 * produced. Let O be what a fresh {@code RemoveSnapshots}, configured identically, removes when
 * applied once to B. Then {@code ids(B) - ids(A)} must be a subset of O: an expiry may not remove a
 * snapshot that its own policy, evaluated on the metadata it actually replaced, keeps. The oracle is
 * Iceberg's own retention code run on B, so R does not depend on a re-implementation of the rules.
 * R is time-independent here because the expiry uses {@code expireOlderThan(Long.MAX_VALUE)} and
 * no ref sets a max age.
 */
class ExpiryRetentionTest {

  private static final int SEEDS = Integer.getInteger("dst.r5.retention.seeds", 200);

  /** A read-only TableOperations pinned to one metadata, for the oracle. */
  static final class PinnedOps implements TableOperations {
    private final TableMetadata metadata;
    private final FileIO io;

    PinnedOps(TableMetadata metadata, FileIO io) {
      this.metadata = metadata;
      this.io = io;
    }

    @Override
    public TableMetadata current() {
      return metadata;
    }

    @Override
    public TableMetadata refresh() {
      return metadata;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata updated) {
      throw new UnsupportedOperationException("the oracle never commits");
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return Simulation.LOCATION + "/metadata/" + fileName;
    }

    @Override
    public LocationProvider locationProvider() {
      return LocationProviders.locationsFor(Simulation.LOCATION, metadata.properties());
    }
  }

  static Set<Long> ids(TableMetadata m) {
    Set<Long> out = new LinkedHashSet<>();
    for (Snapshot s : m.snapshots()) {
      out.add(s.snapshotId());
    }
    return out;
  }

  /** What one fresh, single-pass expiry with the workload's policy removes from {@code base}. */
  static Set<Long> oracleRemoves(TableMetadata base, FileIO io, int retainLast) {
    BaseTable pinned = new BaseTable(new PinnedOps(base, io), "oracle");
    Set<Long> out = new LinkedHashSet<>();
    for (Snapshot s :
        pinned.expireSnapshots().expireOlderThan(Long.MAX_VALUE).retainLast(retainLast).apply()) {
      out.add(s.snapshotId());
    }
    return out;
  }

  static final class Row {
    final String label;
    int seeds;
    int expiryLanded;
    int expiryRetried;
    int expiryFailed;
    final Map<String, Integer> expiryFailureClasses = new LinkedHashMap<>();
    int seedsViolatingR;
    int snapshotsViolatingR;
    int retriedSeedsViolatingR;
    int iaeAfterRetry;
    int otherUnexplained;
    final Map<String, Integer> unexplainedByInvariant = new LinkedHashMap<>();
    String firstViolation;
    String firstIae;

    Row(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return String.format(
          "%-58s seeds=%d expiry{landed=%d retried=%d failed=%d %s} R-violations{seeds=%d"
              + " snapshots=%d onRetriedSeeds=%d} IAE-after-retry=%d otherUnexplained=%s",
          label, seeds, expiryLanded, expiryRetried, expiryFailed, expiryFailureClasses,
          seedsViolatingR, snapshotsViolatingR, retriedSeedsViolatingR, iaeAfterRetry,
          unexplainedByInvariant);
    }
  }

  static FaultConfig config() {
    FaultConfig c = FaultConfig.quiet();
    c.seedSnapshots = 4;
    c.expireRetainLast = 1;
    c.writers = 2;
    return c;
  }

  static Row sweep(String label, ScriptRun.Setup setup, String[]... scripts) {
    Row row = new Row(label);
    for (long seed = 1; seed <= SEEDS; seed++) {
      ScriptRun.Ctx ctx = ScriptRun.run(seed, config(), setup, scripts);
      RunResult result = ctx.result;
      row.seeds++;
      String expiryFiber = null;
      for (int f = 0; f < scripts.length; f++) {
        for (String a : scripts[f]) {
          if (a.equals("expire-policy")) {
            expiryFiber = "w" + (f + 1);
          }
        }
      }
      boolean retried = false;
      for (FakeTableOperations.CommitRecord record : result.ops.commits()) {
        if (record.fiber.equals(expiryFiber) && "stale".equals(record.answer)) {
          retried = true;
        }
      }
      if (retried) {
        row.expiryRetried++;
      }
      for (Workload.Outcome o : result.outcomes) {
        if (o.action.startsWith("expire") && !o.reportedSuccess) {
          row.expiryFailed++;
          String cls = o.failure == null ? "?" : o.failure.split(":")[0];
          row.expiryFailureClasses.merge(cls, 1, Integer::sum);
          if (retried && cls.endsWith("IllegalArgumentException")) {
            row.iaeAfterRetry++;
            if (row.firstIae == null) {
              row.firstIae = "seed " + seed + ": " + result.outcomes;
            }
          }
        }
      }
      List<TableMetadata> history = result.ops.history();
      List<String> fibers = result.ops.historyFibers();
      FileIO view = ctx.io.observerView();
      boolean violated = false;
      for (int i = 1; i < history.size(); i++) {
        if (!fibers.get(i).equals(expiryFiber)) {
          continue;
        }
        TableMetadata base = history.get(i - 1);
        TableMetadata after = history.get(i);
        if (!base.refs().equals(after.refs())) {
          // A ManageSnapshots commit by the same fiber (the serial controls run both operations on
          // one fiber). This expiry policy never removes a ref: main is always kept and no other
          // ref has a max age, so an entry whose refs changed is not the expiry's.
          continue;
        }
        Set<Long> removed = ids(base);
        removed.removeAll(ids(after));
        row.expiryLanded++;
        Set<Long> allowed = oracleRemoves(base, view, config().expireRetainLast);
        Set<Long> excess = new LinkedHashSet<>(removed);
        excess.removeAll(allowed);
        if (!excess.isEmpty()) {
          violated = true;
          row.snapshotsViolatingR += excess.size();
          if (row.firstViolation == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("seed ").append(seed).append(retried ? " (expiry retried)" : "").append('\n');
            sb.append("  base refs: ").append(base.refs()).append('\n');
            sb.append("  base snapshots: ").append(ids(base)).append('\n');
            sb.append("  expiry removed: ").append(removed).append('\n');
            sb.append("  a fresh expiry on that base removes: ").append(allowed).append('\n');
            sb.append("  removed although the policy on its own base retains: ").append(excess).append('\n');
            for (long id : excess) {
              sb.append("  time travel to ").append(id).append(": ");
              try {
                ctx.table.refresh();
                Snapshot s = ctx.table.snapshot(id);
                sb.append(s == null ? "snapshot gone from metadata (clean 'not found')" : "present");
              } catch (RuntimeException e) {
                sb.append(e.getClass().getSimpleName());
              }
              sb.append('\n');
            }
            sb.append("  outcomes: ").append(result.outcomes).append('\n');
            row.firstViolation = sb.toString();
          }
        }
      }
      if (violated) {
        row.seedsViolatingR++;
        if (retried) {
          row.retriedSeedsViolatingR++;
        }
      }
      for (Violation v : result.unexplained()) {
        row.unexplainedByInvariant.merge(v.invariant, 1, Integer::sum);
        row.otherUnexplained++;
      }
    }
    return row;
  }

  @Test
  @DisplayName("A2-1: an expiry retried after a concurrent retention change removes what it keeps")
  void expiryRetryViolatesRetention() {
    ScriptRun.Setup keepAtOldest =
        (table, io, seeds) -> {
          long oldest = table.snapshots().iterator().next().snapshotId();
          table.manageSnapshots().createBranch("keep", oldest).commit();
        };
    String[] expire = {"expire-policy"};
    Row[] rows = {
      // Controls: serial, so the expiry never retries; and concurrent with a writer that changes
      // no retention rule.
      sweep("control serial: create-keep-branch then expire", null,
          new String[] {"ms:create-keep-branch", "expire-policy"}),
      sweep("control serial: expire then create-keep-branch", null,
          new String[] {"expire-policy", "ms:create-keep-branch"}),
      sweep("control serial: branch-at-oldest then expire", null,
          new String[] {"ms:branch-at-oldest", "expire-policy"}),
      sweep("control concurrent: expire || append", null, expire, new String[] {"append"}),
      // Triggers.
      sweep("expire || create-keep-branch(min=100)", null, expire,
          new String[] {"ms:create-keep-branch"}),
      sweep("expire || setMinSnapshotsToKeep(main,100)", null, expire,
          new String[] {"ms:set-min-main"}),
      sweep("expire || replace-keep-to-head(min=100) [keep@oldest]", keepAtOldest, expire,
          new String[] {"ms:replace-keep-to-head"}),
      sweep("expire || createBranch(old, oldest)", null, expire,
          new String[] {"ms:branch-at-oldest"}),
      sweep("expire || createTag(t-old, oldest)", null, expire, new String[] {"ms:tag-oldest"}),
    };
    StringBuilder out = new StringBuilder();
    out.append("=== A2-1 RemoveSnapshots.idsToRemove across retries, iceberg ")
        .append(System.getProperty("icebergVersion", "?"))
        .append(", ")
        .append(SEEDS)
        .append(" seeds per row, retainLast(1), expireOlderThan(MAX) ===\n");
    for (Row row : rows) {
      out.append(row).append('\n');
    }
    for (Row row : rows) {
      if (row.firstViolation != null) {
        out.append("\nfirst R violation in [").append(row.label).append("]\n").append(row.firstViolation);
      }
      if (row.firstIae != null) {
        out.append("\nfirst IAE in [").append(row.label).append("]\n  ").append(row.firstIae).append('\n');
      }
    }
    Reports.write("r5-a2-1-retention.txt", out.toString());
    // R must hold whenever the expiry did not retry: that is what makes it a sound invariant.
    for (int i = 0; i < 4; i++) {
      assertEquals(0, rows[i].seedsViolatingR, out.toString());
      assertTrue(rows[i].expiryLanded > 0, out.toString());
    }
    for (Row row : rows) {
      assertEquals(
          row.retriedSeedsViolatingR, row.seedsViolatingR, "R fired without a retry:\n" + out);
    }
  }
}
