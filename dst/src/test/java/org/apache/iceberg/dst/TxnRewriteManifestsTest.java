package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DstEntries;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * r5, candidate A1-29: {@code BaseRewriteManifests.newManifests} (BaseRewriteManifests.java:60) is
 * reused across a {@code BaseTransaction} re-apply whenever every manifest it rewrote is still in the
 * current list by path (requiresRewrite, :206-219). Inside one transaction the append's manifest P
 * keeps its path across the re-apply (MergingSnapshotProducer cachedNewDataManifests), so the
 * rewrite's pass-1 output, whose EXISTING entries were written with explicit pass-1 data sequence
 * numbers ({@code writer.existing(entry)}), would be committed under a different sequence number.
 *
 * <p>The check, "origin": in every reachable snapshot, every live entry of a data file that the
 * workload only ever APPENDED (no rewrite supplied a number) has {@code dataSequenceNumber} equal to
 * the sequence number of the snapshot that added it (the entry's own {@code snapshot_id}), and the
 * same for {@code fileSequenceNumber}. A single-writer run of the same transaction is the fresh
 * reference. Plus the cross-snapshot check of round 2 (one data file, two data sequence numbers).
 */
class TxnRewriteManifestsTest {

  private static final int SEEDS = Integer.getInteger("dst.r5.txnrewrite.seeds", 60);

  static final class Row {
    final String label;
    int seeds;
    int txnOk;
    int txnRetried;
    int seedsOriginBad;
    int entriesOriginBad;
    int seedsCrossSnapshot;
    final Map<String, Integer> unexplained = new LinkedHashMap<>();
    String first;
    int wrongRowFiles;
    int seedsWrongRows;
    int probeCleared;
    /** Seeds where Iceberg's own scan planning attaches the equality delete differently. */
    int seedsScanDisagrees;
    String firstScan;
    final Map<String, Integer> txnFailures = new LinkedHashMap<>();
    String firstWrongRows;

    Row(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return String.format(
          "%-52s seeds=%d txnOk=%d w1ReApplied=%d origin{seeds=%d entries=%d}"
              + " oneFileTwoDataSeqs{seeds=%d} eqDeleteMisapplied{seeds=%d fileDeletePairs=%d} scanPlanDisagrees{seeds=%d}"
              + " harnessInvariants=%s txnFailures=%s",
          label, seeds, txnOk, txnRetried, seedsOriginBad, entriesOriginBad, seedsCrossSnapshot,
          seedsWrongRows, wrongRowFiles, seedsScanDisagrees, unexplained, txnFailures);
    }
  }

  static FaultConfig config(int formatVersion) {
    FaultConfig c = FaultConfig.quiet();
    c.seedSnapshots = 2;
    c.writers = 2;
    c.formatVersion = formatVersion;
    c.rewriteManifestsWorkload = true;
    return c;
  }

  /** Data file path -> names of the equality delete files Iceberg's scan planning attaches. */
  static Map<String, Set<String>> equalityDeletesPerFile(org.apache.iceberg.Table table, long snapshotId) {
    Map<String, Set<String>> out = new LinkedHashMap<>();
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        Set<String> eq = new java.util.TreeSet<>();
        for (org.apache.iceberg.DeleteFile d : task.deletes()) {
          if (d.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES) {
            eq.add(d.location().substring(d.location().lastIndexOf('/') + 1));
          }
        }
        out.put(task.file().location(), eq);
      }
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return out;
  }

  static String snapshotLine(TableMetadata m) {
    StringBuilder sb = new StringBuilder();
    for (Snapshot s : m.snapshots()) {
      sb.append(s.snapshotId()).append("(seq").append(s.sequenceNumber()).append(',')
          .append(s.operation()).append(") ");
    }
    return sb.toString();
  }

  /** An equality delete in partition a, the partition of w1's first appended file. */
  static final ScriptRun.Actions EQ_DELETE =
      (workload, table, seedFiles) ->
          workload.registerAction(
              "eq-delete-a",
              (t, o) -> {
                org.apache.iceberg.RowDelta delta =
                    t.newRowDelta()
                        .addDeletes(
                            Payload.equalityDeletes(
                                t.io(), Simulation.SPEC, Simulation.LOCATION, o.fiber + "eq", 0,
                                "a", 1));
                CommitUuid.makeDeterministic(delta);
                SameThreadExecutor.inject(delta).commit();
              });

  static Row sweep(String label, int formatVersion, String[]... scripts) {
    Row row = new Row(label);
    for (long seed = 1; seed <= SEEDS; seed++) {
      ScriptRun.Ctx ctx = ScriptRun.run(seed, config(formatVersion), null, EQ_DELETE, scripts);
      row.probeCleared += FilterCacheProbe.cleared();
      int wrongBefore = row.wrongRowFiles;
      RunResult result = ctx.result;
      row.seeds++;
      for (Workload.Outcome o : result.outcomes) {
        if (o.action.startsWith("transaction") && o.reportedSuccess) {
          row.txnOk++;
        } else if (o.action.startsWith("transaction")) {
          String f = String.valueOf(o.failure);
          row.txnFailures.merge(f.length() > 160 ? f.substring(0, 160) : f, 1, Integer::sum);
        }
      }
      TableMetadata metadata = result.finalMetadata;
      // BaseTransaction re-applies when it finds the base moved on refresh, before it ever calls
      // the catalog (BaseTransaction.commitSimpleTransaction), so CAS rejections do not count
      // re-applies. The manifest-list name does: snap-<id>-<attempt>-<uuid>, where attempt is the
      // producer's own apply() counter (SnapshotProducer.java:624-635).
      Map<Long, String> landedBy = result.ops.landedBy();
      for (Snapshot s : metadata.snapshots()) {
        if ("w1".equals(landedBy.get(s.snapshotId())) && s.manifestListLocation() != null) {
          String name = s.manifestListLocation().substring(s.manifestListLocation().lastIndexOf('/') + 1);
          String[] parts = name.split("-");
          if (parts.length > 2 && Integer.parseInt(parts[2]) >= 2) {
            row.txnRetried++;
            break;
          }
        }
      }
      FileIO view = ctx.io.observerView();
      int bad = 0;
      StringBuilder detail = new StringBuilder();
      for (long id : TableView.reachable(metadata)) {
        for (DstEntries.Entry e : DstEntries.dataEntries(metadata, id, view)) {
          if (e.status.equals("DELETED") || e.snapshotId == null) {
            continue;
          }
          Snapshot adder = metadata.snapshot(e.snapshotId);
          if (adder == null) {
            continue;
          }
          long expected = adder.sequenceNumber();
          boolean ok =
              e.dataSequenceNumber != null
                  && e.dataSequenceNumber == expected
                  && e.fileSequenceNumber != null
                  && e.fileSequenceNumber == expected;
          if (!ok) {
            bad++;
            if (detail.length() < 2000) {
              detail.append("    in snapshot ").append(id).append(" (seq ")
                  .append(metadata.snapshot(id).sequenceNumber()).append("): ")
                  .append(e.path.substring(e.path.lastIndexOf('/') + 1)).append(' ').append(e)
                  .append(" but its adding snapshot ").append(e.snapshotId).append(" has seq ")
                  .append(expected).append("; manifest ")
                  .append(e.manifest.substring(e.manifest.lastIndexOf('/') + 1)).append('\n');
            }
          }
        }
      }
      if (bad > 0) {
        row.seedsOriginBad++;
        row.entriesOriginBad += bad;
      }
      // Wrong rows: an equality delete applies to a data file in its partition iff the delete's data
      // sequence number is strictly greater than the file's. Compare what a reader of the main head
      // resolves with what the file's own commit says.
      Snapshot head = metadata.currentSnapshot();
      if (head != null) {
        TableView.SnapshotFiles hv = TableView.read(metadata, head.snapshotId(), view);
        for (DstEntries.Entry e : DstEntries.dataEntries(metadata, head.snapshotId(), view)) {
          if (e.status.equals("DELETED") || e.snapshotId == null) {
            continue;
          }
          Snapshot adder = metadata.snapshot(e.snapshotId);
          if (adder == null || e.dataSequenceNumber == null) {
            continue;
          }
          String partition = e.path.contains("/data=a/") ? "data=a" : "data=b";
          for (TableView.FileFact del : hv.facts.values()) {
            if (del.content != org.apache.iceberg.FileContent.EQUALITY_DELETES
                || !del.partition.equals(partition)) {
              continue;
            }
            boolean appliesAsRead = del.dataSequenceNumber > e.dataSequenceNumber;
            boolean appliesTruly = del.dataSequenceNumber > adder.sequenceNumber();
            if (appliesAsRead != appliesTruly) {
              row.wrongRowFiles++;
              if (row.firstWrongRows == null) {
                row.firstWrongRows =
                    "seed " + seed + ": head " + head.snapshotId() + " reads "
                        + e.path.substring(e.path.lastIndexOf('/') + 1) + " at dataSeq="
                        + e.dataSequenceNumber + "; it was committed by " + e.snapshotId
                        + " at seq " + adder.sequenceNumber() + "; equality delete "
                        + del.path.substring(del.path.lastIndexOf('/') + 1) + " has seq "
                        + del.dataSequenceNumber + ", so it " + (appliesAsRead ? "DELETES" : "spares")
                        + " those rows in the head but "
                        + (appliesTruly ? "deletes" : "spares") + " them by the commit order\n"
                        + "  snapshots: " + snapshotLine(metadata) + "\n  outcomes: "
                        + result.outcomes;
              }
            }
          }
        }
      }
      if (row.wrongRowFiles > wrongBefore) {
        row.seedsWrongRows++;
      }
      // The same question answered by Iceberg's real scan planning (DeleteFileIndex): a manifest
      // rewrite ("replace" snapshot) must not change which delete files a FileScanTask carries for
      // any data file, so plan its snapshot and its parent and compare.
      for (Snapshot s : metadata.snapshots()) {
        if (!"replace".equals(s.operation()) || s.parentId() == null
            || metadata.snapshot(s.parentId()) == null) {
          continue;
        }
        Map<String, Set<String>> after = equalityDeletesPerFile(ctx.table, s.snapshotId());
        Map<String, Set<String>> before = equalityDeletesPerFile(ctx.table, s.parentId());
        if (!after.equals(before)) {
          row.seedsScanDisagrees++;
          if (row.firstScan == null) {
            StringBuilder sb = new StringBuilder("seed " + seed + ": manifest rewrite " + s.snapshotId()
                + " vs its parent " + s.parentId() + ":");
            for (String path : after.keySet()) {
              if (!java.util.Objects.equals(after.get(path), before.get(path))) {
                sb.append("\n    ").append(path.substring(path.lastIndexOf('/') + 1))
                    .append(" equality deletes before=").append(before.get(path))
                    .append(" after=").append(after.get(path));
              }
            }
            row.firstScan = sb.toString();
          }
          break;
        }
      }
      Map<String, Set<Long>> cross = SequenceNumberFlip.findFileDisagreement(metadata, view);
      if (!cross.isEmpty()) {
        row.seedsCrossSnapshot++;
      }
      if ((bad > 0 || !cross.isEmpty()) && row.first == null) {
        StringBuilder sb = new StringBuilder("seed " + seed + "\n");
        sb.append("  outcomes: ").append(result.outcomes).append('\n');
        for (Snapshot s : metadata.snapshots()) {
          sb.append("  snapshot ").append(s.snapshotId()).append(" parent=").append(s.parentId())
              .append(" seq=").append(s.sequenceNumber()).append(' ').append(s.operation())
              .append('\n');
        }
        sb.append("  origin mismatches:\n").append(detail);
        sb.append("  one data file, several data seqs: ").append(cross).append('\n');
        row.first = sb.toString();
      }
      for (Violation v : result.unexplained()) {
        row.unexplained.merge(v.invariant, 1, Integer::sum);
      }
    }
    return row;
  }

  static Row probeRow(String label, String mode) {
    String previous = FilterCacheProbe.setMode(mode);
    try {
      Row row = sweep(label, 2, new String[] {"transaction-rewrite-manifests"},
          new String[] {"append", "eq-delete-a"});
      return row;
    } finally {
      FilterCacheProbe.setMode(previous);
    }
  }

  @Test
  @DisplayName("A1-29: rewriteManifests inside a re-applied transaction keeps pass-1 entry seqs")
  void rewriteManifestsInsideTransaction() {
    Row[] rows = {
      sweep("control: ONE writer, txn(append+rewriteManifests)", 2,
          new String[] {"transaction-rewrite-manifests"}),
      sweep("control: append,rewrite-manifests (no txn) || append", 2,
          new String[] {"append", "rewrite-manifests"}, new String[] {"append"}),
      sweep("trigger: txn(append+rewriteManifests) || append", 2,
          new String[] {"transaction-rewrite-manifests"}, new String[] {"append"}),
      sweep("trigger v3: txn(append+rewriteManifests) || append", 3,
          new String[] {"transaction-rewrite-manifests"}, new String[] {"append"}),
      sweep("control: ONE writer, txn then eq-delete(a)", 2,
          new String[] {"transaction-rewrite-manifests", "eq-delete-a"}),
      sweep("control: append,rewrite-manifests || append, eq-delete(a)", 2,
          new String[] {"append", "rewrite-manifests"}, new String[] {"append", "eq-delete-a"}),
      sweep("trigger: txn(append+rewriteManifests) || append, eq-delete(a)", 2,
          new String[] {"transaction-rewrite-manifests"}, new String[] {"append", "eq-delete-a"}),
    };
    // The lever: force requiresRewrite() on every refresh by emptying rewrittenManifests. If the
    // stale numbers come from reused pass-1 newManifests, this removes them; the round-2 joint
    // eviction of the A1-1 caches (filter,withmeta) is the other candidate and must not.
    Row lever = probeRow("lever rewrite: txn || append, eq-delete(a)", "rewrite");
    Row a11 = probeRow("lever filter,withmeta: txn || append, eq-delete(a)", "filter,withmeta");
    StringBuilder out = new StringBuilder();
    out.append("=== A1-29 BaseRewriteManifests.newManifests across a txn re-apply, iceberg ")
        .append(System.getProperty("icebergVersion", "?")).append(", ").append(SEEDS)
        .append(" seeds per row ===\n");
    for (Row r : rows) {
      out.append(r).append('\n');
    }
    out.append(lever).append("  probeCleared=").append(lever.probeCleared).append('\n');
    out.append(a11).append("  probeCleared=").append(a11.probeCleared).append('\n');
    for (Row r : rows) {
      if (r.first != null) {
        out.append("\nfirst in [").append(r.label).append("]\n").append(r.first);
      }
      if (r.firstScan != null) {
        out.append("\nfirst scan-planning disagreement in [").append(r.label).append("]\n  ")
            .append(r.firstScan).append('\n');
      }
      if (r.firstWrongRows != null) {
        out.append("\nfirst misapplied equality delete in [").append(r.label).append("]\n  ")
            .append(r.firstWrongRows).append('\n');
      }
    }
    Reports.write("r5-a1-29-txn-rewrite.txt", out.toString());
    // The controls must be clean, or the origin check is wrong rather than Iceberg.
    assertEquals(0, rows[0].seedsOriginBad, out.toString());
    assertEquals(0, rows[0].seedsCrossSnapshot, out.toString());
    assertEquals(0, rows[1].seedsOriginBad, out.toString());
    assertEquals(0, rows[4].wrongRowFiles, out.toString());
    assertEquals(0, rows[4].seedsScanDisagrees, out.toString());
    assertEquals(0, rows[5].seedsScanDisagrees, out.toString());
    assertEquals(0, rows[5].wrongRowFiles, out.toString());
    assertEquals(0, rows[4].seedsOriginBad, out.toString());
    assertEquals(0, rows[5].seedsOriginBad, out.toString());
    assertTrue(rows[0].txnOk > 0, out.toString());
  }
}
