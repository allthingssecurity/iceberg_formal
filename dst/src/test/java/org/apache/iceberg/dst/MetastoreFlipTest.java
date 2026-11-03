package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The experiment that decides the candidate finding in findings/r2-dst-harness.md §6.
 *
 * <p>The same two-writer workload, the same seeds, the same store and the same scheduler, run
 * against two catalogs that differ in exactly the property the finding could not exclude:
 *
 * <ul>
 *   <li>{@link FakeTableOperations}: metadata kept as objects in memory, compare-and-set on object
 *       identity. Iceberg's own {@code TestTables} has both of these properties too, which is why
 *       porting the sequence there would decide nothing.
 *   <li>{@link FakeMetastoreOperations}: Iceberg's real {@code BaseMetastoreTableOperations}, so
 *       every {@code refresh()} re-reads and re-parses the metadata JSON and the compare-and-set is
 *       on the metadata location. This is what REST, Hive and Glue do.
 * </ul>
 *
 * <p>If the flip appears under both, the in-memory metadata is not the cause. If it appears only
 * under the first, it is an artefact of the fake and the candidate is dead.
 */
class MetastoreFlipTest {

  private static final int SEEDS = Integer.getInteger("dst.flip.seeds", 2000);

  /** w1's script, then w2's: the operation sequence of the seed that first showed the flip. */
  private static final String[] W1 = {
    "replace-partitions", "rewrite-manifests", "row-delta", "branch-append"
  };
  private static final String[] W2 = {"transaction"};

  /** The last run's final metadata and a read-only view of its store, for the combined assertion. */
  private static TableMetadata lastMetadata;
  private static FileIO lastView;
  private static int lastManifestWrites;
  private static int lastManifestDeletes;

  @Test
  @DisplayName("which field carries the stale value: sequence_number or min_sequence_number")
  void whichFieldCarriesTheStaleValue() {
    String[] w1 = {"transaction"};
    String[] w2 = {"transaction"};
    StringBuilder out = new StringBuilder();
    out.append("=== manifest-list entries of every reachable snapshot, both sequence fields ===\n");
    // ManifestWriter.UNASSIGNED_SEQ is package-private, so it cannot be referenced from here.
    out.append("(UNASSIGNED_SEQ is -1)\n\n");

    int seqDisagree = 0;
    int minSeqDisagree = 0;
    int entryDisagree = 0;
    String firstDetail = null;

    for (long seed = 1; seed <= Math.min(SEEDS, 200); seed++) {
      Map<String, Set<Long>>[] result = run(seed, true, w1, w2);
      TableMetadata metadata = lastMetadata;
      FileIO view = lastView;
      Map<String, Set<Long>> fileDisagreement =
          SequenceNumberFlip.findFileDisagreement(metadata, view);
      if (fileDisagreement.isEmpty()) {
        continue;
      }

      // Per manifest path, the distinct values of each field across reachable snapshots.
      Map<String, Set<Long>> seqByManifest = new LinkedHashMap<>();
      Map<String, Set<Long>> minSeqByManifest = new LinkedHashMap<>();
      StringBuilder detail = new StringBuilder();
      long head =
          metadata.currentSnapshot() == null ? -1L : metadata.currentSnapshot().snapshotId();
      detail.append("  main head = ").append(head).append('\n');
      for (long id : TableView.reachable(metadata)) {
        org.apache.iceberg.Snapshot snapshot = metadata.snapshot(id);
        String list = snapshot.manifestListLocation();
        detail
            .append("  snapshot ")
            .append(id)
            .append(" seq=")
            .append(snapshot.sequenceNumber())
            .append(id == head ? "  <== MAIN HEAD" : "")
            .append("  manifestList=")
            .append(list == null ? "-" : list.substring(list.lastIndexOf('/') + 1, Math.min(list.length(), list.lastIndexOf('/') + 14)))
            .append("  (snap-<id>-<attempt>-<uuid>, so the middle number is which apply() wrote it)")
            .append('\n');
        for (org.apache.iceberg.ManifestFile m : snapshot.allManifests(view)) {
          if (m.content() != org.apache.iceberg.ManifestContent.DATA) {
            continue;
          }
          String tail = m.path().substring(m.path().lastIndexOf('-') - 12);
          seqByManifest.computeIfAbsent(tail, k -> new LinkedHashSet<>()).add(m.sequenceNumber());
          minSeqByManifest
              .computeIfAbsent(tail, k -> new LinkedHashSet<>())
              .add(m.minSequenceNumber());
          detail
              .append("      ")
              .append(tail)
              .append("  sequence_number=")
              .append(m.sequenceNumber())
              .append("  min_sequence_number=")
              .append(m.minSequenceNumber())
              .append("  addedBy=")
              .append(m.snapshotId())
              .append('\n');
        }
      }

      boolean seqBad = seqByManifest.values().stream().anyMatch(v -> v.size() > 1);
      boolean minSeqBad = minSeqByManifest.values().stream().anyMatch(v -> v.size() > 1);
      if (seqBad) {
        seqDisagree++;
      }
      if (minSeqBad) {
        minSeqDisagree++;
      }
      if (!seqBad && !minSeqBad) {
        entryDisagree++;
      }
      if (firstDetail == null) {
        firstDetail =
            "seed "
                + seed
                + "\n  data files disagreeing: "
                + fileDisagreement
                + "\n  sequence_number per manifest: "
                + seqByManifest
                + "\n  min_sequence_number per manifest: "
                + minSeqByManifest
                + "\n"
                + detail;
      }
    }

    out.append("of the failing seeds:\n");
    out.append("  sequence_number disagrees between snapshots:     ").append(seqDisagree).append('\n');
    out.append("  min_sequence_number disagrees between snapshots: ").append(minSeqDisagree).append('\n');
    out.append("  neither disagrees (so it is at the entry level): ").append(entryDisagree).append('\n');
    out.append('\n').append(firstDetail == null ? "(no failing seed)" : firstDetail);
    try {
      java.nio.file.Path dir = java.nio.file.Paths.get("build", "dst");
      java.nio.file.Files.createDirectories(dir);
      java.nio.file.Files.write(
          dir.resolve("which-field-report.txt"),
          out.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  @Test
  @DisplayName("Iceberg accepts the metadata as valid while two readers of it disagree")
  void metadataIsValidWhileReadersDisagree() {
    String[] w1 = {"transaction"};
    String[] w2 = {"transaction"};
    StringBuilder out = new StringBuilder();
    int both = 0;
    int rejected = 0;
    String firstReport = null;

    for (long seed = 1; seed <= Math.min(SEEDS, 300); seed++) {
      Map<String, Set<Long>>[] result = run(seed, true, w1, w2);
      TableMetadata finalMetadata = lastMetadata;
      Map<String, Set<Long>> disagreement =
          SequenceNumberFlip.findFileDisagreement(finalMetadata, lastView);
      if (disagreement.isEmpty()) {
        continue;
      }
      String whyRejected = SequenceNumberFlip.whyIcebergWouldReject(finalMetadata);
      if (whyRejected != null) {
        rejected++;
        continue;
      }
      both++;
      if (firstReport == null) {
        StringBuilder r = new StringBuilder();
        r.append("seed ").append(seed).append('\n');
        r.append("  Iceberg's own metadata-level validation: ACCEPTED\n");
        r.append("    every snapshot sequenceNumber <= lastSequenceNumber=")
            .append(finalMetadata.lastSequenceNumber())
            .append('\n');
        r.append("    TableMetadata.buildFrom(...).build() -> ok (runs TableMetadata.java:835)\n");
        r.append("    TableMetadataParser round-trip      -> ok\n");
        r.append("  and yet, across REACHABLE snapshots only:\n");
        disagreement.forEach(
            (path, seqs) ->
                r.append("    data file ")
                    .append(path.substring(path.lastIndexOf('/') + 1))
                    .append(" resolves to data_sequence_number ")
                    .append(seqs)
                    .append('\n'));
        r.append("  manifests listed with more than one sequence_number: ")
            .append(result[1])
            .append('\n');
        r.append("  snapshots:\n");
        for (org.apache.iceberg.Snapshot snapshot : finalMetadata.snapshots()) {
          r.append("    ")
              .append(snapshot.snapshotId())
              .append(" parent=")
              .append(snapshot.parentId())
              .append(" seq=")
              .append(snapshot.sequenceNumber())
              .append(' ')
              .append(snapshot.operation())
              .append('\n');
        }
        r.append("  refs: ").append(finalMetadata.refs().keySet()).append('\n');
        firstReport = r.toString();
      }
    }

    out.append("=== both halves in one run, two concurrent transactions ===\n");
    out.append("seeds where Iceberg accepts the metadata AND reachable snapshots disagree: ")
        .append(both)
        .append('/')
        .append(Math.min(SEEDS, 300))
        .append('\n');
    out.append("seeds where the metadata would have been rejected instead: ")
        .append(rejected)
        .append('\n');
    if (firstReport != null) {
      out.append(firstReport);
    }
    try {
      java.nio.file.Path dir = java.nio.file.Paths.get("build", "dst");
      java.nio.file.Files.createDirectories(dir);
      java.nio.file.Files.write(
          dir.resolve("both-halves-report.txt"),
          out.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    // Guards the experiment, not Iceberg. If this goes red, either Iceberg has changed and the
    // disagreement is gone — which is the outcome to hope for — or the harness has drifted and the
    // numbers in findings/r2-dst-harness.md no longer mean what they say.
    org.junit.jupiter.api.Assertions.assertTrue(
        both > 0,
        "the experiment no longer shows Iceberg accepting metadata that reachable snapshots"
            + " disagree about; check whether Iceberg changed or the harness drifted:\n"
            + out);
  }

  @Test
  @DisplayName("narrowing: which operations are needed for the flip at all")
  void narrowTheScript() {
    StringBuilder out = new StringBuilder();
    out.append("=== narrowing the script, ").append(SEEDS).append(" seeds, real metastore ops ===\n");
    record(out, "append + transaction", new String[] {"append", "transaction"}, new String[] {"transaction"});
    record(out, "append only", new String[] {"append", "append"}, new String[] {"append"});
    record(out, "transaction only", new String[] {"transaction"}, new String[] {"transaction"});
    record(
        out,
        "append + rewrite-manifests",
        new String[] {"append", "rewrite-manifests"},
        new String[] {"append"});
    record(
        out,
        "append + branch-append",
        new String[] {"append", "branch-append"},
        new String[] {"append"});
    // The question that decides whether this is a concurrency bug at all: one writer, one
    // transaction, nothing else running. If this reproduces, concurrency is not involved.
    out.append("  (probe dst.invalidateFilterCache=")
        .append(FilterCacheProbe.mode())
        .append(")\n");
    record(out, "ONE writer, one transaction", new String[] {"transaction"}, new String[] {});
    record(out, "ONE writer, two transactions", new String[] {"transaction", "transaction"}, new String[] {});
    try {
      java.nio.file.Path dir = java.nio.file.Paths.get("build", "dst");
      java.nio.file.Files.createDirectories(dir);
      java.nio.file.Files.write(
          dir.resolve("narrow-report.txt"),
          out.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  private static void record(StringBuilder out, String label, String[] w1, String[] w2) {
    int flips = 0;
    int probeCleared = 0;
    int manifestWrites = 0;
    int manifestDeletes = 0;
    long evicted = 0;
    Map<String, Set<Long>> first = null;
    long firstSeed = -1;
    for (long seed = 1; seed <= SEEDS; seed++) {
      Map<String, Set<Long>>[] both = run(seed, true, w1, w2);
      probeCleared += FilterCacheProbe.cleared();
      manifestWrites += lastManifestWrites;
      manifestDeletes += lastManifestDeletes;
      evicted += FilterCacheProbe.withMetadataEvicted();
      if (!both[1].isEmpty()) {
        flips++;
        if (first == null) {
          first = both[1];
          firstSeed = seed;
        }
      }
    }
    out.append(
        String.format(
            "  %-28s %4d/%d  cleared=%-5d manifestWrites=%-5d withMetaEvicted=%d",
            label, flips, SEEDS, probeCleared, manifestWrites, evicted));
    if (first != null) {
      out.append("  first seed ").append(firstSeed).append(' ').append(first);
    }
    out.append('\n');
  }

  /** One row of the r5 cause table. */
  private static final class CauseRow {
    final String mode;
    int seeds;
    int flips;
    int manifestWrites;
    long withMetaEvicted;
    int cleared;

    CauseRow(String mode) {
      this.mode = mode;
    }

    @Override
    public String toString() {
      return String.format(
          "  %-18s flip %4d/%d  manifestWrites=%-6d withMetaEvicted=%-6d cleared=%d",
          mode, flips, seeds, manifestWrites, withMetaEvicted, cleared);
    }
  }

  private static CauseRow causeRow(String mode, int seeds, int formatVersion) {
    String previous = FilterCacheProbe.setMode(mode);
    try {
      CauseRow row = new CauseRow(mode);
      String[] txn = {"transaction"};
      for (long seed = 1; seed <= seeds; seed++) {
        Map<String, Set<Long>>[] both = run(seed, true, formatVersion, txn, txn);
        row.seeds++;
        row.manifestWrites += lastManifestWrites;
        row.withMetaEvicted += FilterCacheProbe.withMetadataEvicted();
        row.cleared += FilterCacheProbe.cleared();
        if (!both[1].isEmpty()) {
          row.flips++;
        }
      }
      return row;
    } finally {
      FilterCacheProbe.setMode(previous);
    }
  }

  /**
   * r5 (findings/r5-open-defect-cause.md, findings/r5-dst-core.md): the four-row table that
   * established the cause. Each of the two path-keyed caches masks the other, so evicting either one
   * alone changes nothing, and evicting both together removes the flip with no path churn (the
   * manifest-write count is unchanged, unlike the {@code new} lever).
   */
  @Test
  @DisplayName("r5 cause table: filter alone and withmeta alone keep the flip, both together remove it")
  void causeTable() {
    int seeds = Integer.getInteger("dst.r5.cause.seeds", 200);
    CauseRow off = causeRow("off", seeds, 2);
    CauseRow filter = causeRow("filter", seeds, 2);
    CauseRow withmeta = causeRow("withmeta", seeds, 2);
    CauseRow both = causeRow("filter,withmeta", seeds, 2);
    String out =
        "=== r5 cause table, transaction / transaction, real metastore ops, v2, "
            + seeds + " seeds ===\n" + off + "\n" + filter + "\n" + withmeta + "\n" + both + "\n";
    Reports.write("r5-cause-table.txt", out);
    org.junit.jupiter.api.Assertions.assertEquals(seeds, off.flips, out);
    org.junit.jupiter.api.Assertions.assertEquals(seeds, filter.flips, out);
    org.junit.jupiter.api.Assertions.assertEquals(seeds, withmeta.flips, out);
    org.junit.jupiter.api.Assertions.assertEquals(0, both.flips, out);
    // No path churn: the joint eviction writes exactly as many manifests as the control.
    org.junit.jupiter.api.Assertions.assertEquals(off.manifestWrites, both.manifestWrites, out);
    org.junit.jupiter.api.Assertions.assertTrue(both.withMetaEvicted > 0, out);
  }

  /** Row-id facts of one v3 run, over reachable snapshots only. */
  private static final class RowIdFacts {
    /** Manifest path -> distinct first_row_id values it is listed with. */
    final Map<String, Set<Long>> manifestFirstRowIds = new LinkedHashMap<>();
    /** Data file path -> distinct first_row_id values a reader resolves for it. */
    final Map<String, Set<Long>> fileFirstRowIds = new LinkedHashMap<>();
    /** Pairs of distinct live data files whose [first_row_id, +record_count) ranges overlap. */
    final Set<String> overlappingPairs = new LinkedHashSet<>();
    /** Same, but within one snapshot: what one reader would see as duplicate _row_id values. */
    final Set<String> overlapsWithinOneSnapshot = new LinkedHashSet<>();
    int filesWithoutRowId;
  }

  private static RowIdFacts rowIdFacts(TableMetadata metadata, FileIO io) {
    RowIdFacts facts = new RowIdFacts();
    Map<String, long[]> rangeByFileAndId = new LinkedHashMap<>();
    for (long id : TableView.reachable(metadata)) {
      org.apache.iceberg.Snapshot snapshot = metadata.snapshot(id);
      for (org.apache.iceberg.ManifestFile m : snapshot.dataManifests(io)) {
        facts.manifestFirstRowIds
            .computeIfAbsent(tail(m.path()), k -> new LinkedHashSet<>())
            .add(m.firstRowId());
      }
      List<long[]> ranges = new ArrayList<>();
      List<String> names = new ArrayList<>();
      for (org.apache.iceberg.DstEntries.Entry e :
          org.apache.iceberg.DstEntries.dataEntries(metadata, id, io)) {
        if (e.status.equals("DELETED")) {
          continue;
        }
        if (e.firstRowId == null) {
          facts.filesWithoutRowId++;
          continue;
        }
        facts.fileFirstRowIds
            .computeIfAbsent(tail(e.path), k -> new LinkedHashSet<>())
            .add(e.firstRowId);
        long[] range = {e.firstRowId, e.firstRowId + e.recordCount};
        rangeByFileAndId.put(tail(e.path) + "@" + e.firstRowId, range);
        ranges.add(range);
        names.add(tail(e.path));
      }
      for (int i = 0; i < ranges.size(); i++) {
        for (int j = i + 1; j < ranges.size(); j++) {
          if (!names.get(i).equals(names.get(j))
              && ranges.get(i)[0] < ranges.get(j)[1]
              && ranges.get(j)[0] < ranges.get(i)[1]) {
            facts.overlapsWithinOneSnapshot.add(
                "snapshot " + id + ": " + names.get(i) + " & " + names.get(j));
          }
        }
      }
    }
    List<String> keys = new ArrayList<>(rangeByFileAndId.keySet());
    for (int i = 0; i < keys.size(); i++) {
      for (int j = i + 1; j < keys.size(); j++) {
        String a = keys.get(i).substring(0, keys.get(i).indexOf('@'));
        String b = keys.get(j).substring(0, keys.get(j).indexOf('@'));
        long[] ra = rangeByFileAndId.get(keys.get(i));
        long[] rb = rangeByFileAndId.get(keys.get(j));
        if (!a.equals(b) && ra[0] < rb[1] && rb[0] < ra[1]) {
          facts.overlappingPairs.add(keys.get(i) + " & " + keys.get(j));
        }
      }
    }
    facts.manifestFirstRowIds.values().removeIf(v -> v.size() < 2);
    facts.fileFirstRowIds.values().removeIf(v -> v.size() < 2);
    return facts;
  }

  private static String tail(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  private static final class V3Row {
    final String label;
    int seeds;
    int seqFlip;
    int manifestFirstRowIdFlip;
    int fileFirstRowIdFlip;
    int overlapAcrossSnapshots;
    int overlapWithinSnapshot;
    int filesWithoutRowId;
    String first;

    V3Row(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return String.format(
          "  %-36s seeds=%d manifestSeqFlip=%d manifestFirstRowIdFlip=%d fileFirstRowIdFlip=%d"
              + " overlap{acrossReachable=%d withinOneSnapshot=%d} entriesWithoutRowId=%d",
          label, seeds, seqFlip, manifestFirstRowIdFlip, fileFirstRowIdFlip,
          overlapAcrossSnapshots, overlapWithinSnapshot, filesWithoutRowId);
    }
  }

  private static V3Row v3Row(String label, String mode, int seeds, String[] w1, String[] w2) {
    String previous = FilterCacheProbe.setMode(mode);
    try {
      V3Row row = new V3Row(label);
      for (long seed = 1; seed <= seeds; seed++) {
        Map<String, Set<Long>>[] both = run(seed, true, 3, w1, w2);
        row.seeds++;
        if (!both[1].isEmpty()) {
          row.seqFlip++;
        }
        RowIdFacts facts = rowIdFacts(lastMetadata, lastView);
        row.filesWithoutRowId += facts.filesWithoutRowId;
        if (!facts.manifestFirstRowIds.isEmpty()) {
          row.manifestFirstRowIdFlip++;
        }
        if (!facts.fileFirstRowIds.isEmpty()) {
          row.fileFirstRowIdFlip++;
        }
        if (!facts.overlappingPairs.isEmpty()) {
          row.overlapAcrossSnapshots++;
        }
        if (!facts.overlapsWithinOneSnapshot.isEmpty()) {
          row.overlapWithinSnapshot++;
        }
        if (row.first == null
            && (!facts.manifestFirstRowIds.isEmpty() || !facts.overlappingPairs.isEmpty())) {
          StringBuilder sb = new StringBuilder("seed " + seed + "\n");
          for (org.apache.iceberg.Snapshot s : lastMetadata.snapshots()) {
            sb.append("    snapshot ").append(s.snapshotId()).append(" parent=")
                .append(s.parentId()).append(" seq=").append(s.sequenceNumber())
                .append(" firstRowId=").append(s.firstRowId()).append(" addedRows=")
                .append(s.addedRows()).append('\n');
          }
          sb.append("    nextRowId=").append(lastMetadata.nextRowId()).append('\n');
          sb.append("    manifest listed with several first_row_id: ")
              .append(facts.manifestFirstRowIds).append('\n');
          sb.append("    data file resolving to several first_row_id: ")
              .append(facts.fileFirstRowIds).append('\n');
          int n = 0;
          for (String p : facts.overlappingPairs) {
            if (n++ < 6) {
              sb.append("    overlapping ranges: ").append(p).append('\n');
            }
          }
          row.first = sb.toString();
        }
      }
      return row;
    } finally {
      FilterCacheProbe.setMode(previous);
    }
  }

  /**
   * r5: does the stale pass-1 manifest instance also carry a stale first_row_id in v3
   * (ManifestListWriter.java:198-199 keeps a non-null firstRowId), so that row-id ranges of
   * reachable snapshots overlap? Same workload as the cause table, format v3.
   */
  @Test
  @DisplayName("r5: the same defect on format v3, first_row_id")
  void v3FirstRowId() {
    int seeds = Integer.getInteger("dst.r5.v3.seeds", 100);
    String[] txn = {"transaction-dv"};
    V3Row control = v3Row("ONE writer, transaction-dv (control)", "off", seeds, txn, new String[] {});
    V3Row trigger = v3Row("transaction-dv / transaction-dv", "off", seeds, txn, txn);
    V3Row joint = v3Row("txn-dv / txn-dv, filter,withmeta", "filter,withmeta", seeds, txn, txn);
    StringBuilder out = new StringBuilder();
    out.append("=== r5 v3 first_row_id, real metastore ops, ").append(seeds).append(" seeds ===\n");
    out.append(control).append('\n').append(trigger).append('\n').append(joint).append('\n');
    if (trigger.first != null) {
      out.append("\nfirst trigger seed:\n").append(trigger.first);
    }
    Reports.write("r5-v3-first-row-id.txt", out.toString());
    org.junit.jupiter.api.Assertions.assertEquals(0, control.manifestFirstRowIdFlip, out.toString());
    org.junit.jupiter.api.Assertions.assertEquals(0, control.overlapAcrossSnapshots, out.toString());
    // The joint eviction that removes the v2 flip must remove the v3 observable too, or the two are
    // not the same defect.
    org.junit.jupiter.api.Assertions.assertEquals(0, joint.manifestFirstRowIdFlip, out.toString());
    org.junit.jupiter.api.Assertions.assertEquals(0, joint.overlapWithinSnapshot, out.toString());
  }

  @Test
  @DisplayName("the same workload against an identity catalog and against a real metastore one")
  void compareCatalogs() {
    int identityFlips = 0;
    int metastoreFlips = 0;
    int identityReachable = 0;
    int metastoreReachable = 0;
    List<Long> identitySeeds = new ArrayList<>();
    List<Long> metastoreSeeds = new ArrayList<>();
    Map<String, Set<Long>> firstIdentity = null;
    Map<String, Set<Long>> firstMetastore = null;

    for (long seed = 1; seed <= SEEDS; seed++) {
      Map<String, Set<Long>>[] identityBoth = run(seed, false, W1, W2);
      Map<String, Set<Long>> identity = identityBoth[0];
      if (!identityBoth[1].isEmpty()) {
        identityReachable++;
      }
      if (!identity.isEmpty()) {
        identityFlips++;
        identitySeeds.add(seed);
        if (firstIdentity == null) {
          firstIdentity = identity;
        }
      }
      Map<String, Set<Long>>[] metastoreBoth = run(seed, true, W1, W2);
      Map<String, Set<Long>> metastore = metastoreBoth[0];
      if (!metastoreBoth[1].isEmpty()) {
        metastoreReachable++;
      }
      if (!metastore.isEmpty()) {
        metastoreFlips++;
        metastoreSeeds.add(seed);
        if (firstMetastore == null) {
          firstMetastore = metastore;
        }
      }
    }

    StringBuilder report = new StringBuilder();
    report.append("=== sequence-number flip, ").append(SEEDS).append(" seeds each ===\n");
    report
        .append("identity CAS + in-memory metadata (FakeTableOperations): ")
        .append(identityFlips)
        .append('/')
        .append(SEEDS)
        .append(" seeds, first=")
        .append(identitySeeds.isEmpty() ? "-" : identitySeeds.get(0))
        .append(' ')
        .append(firstIdentity == null ? "" : firstIdentity)
        .append("\n  of which reachable-only: ")
        .append(identityReachable)
        .append('/')
        .append(SEEDS)
        .append('\n');
    report
        .append("real BaseMetastoreTableOperations (JSON round-trip + location CAS): ")
        .append(metastoreFlips)
        .append('/')
        .append(SEEDS)
        .append(" seeds, first=")
        .append(metastoreSeeds.isEmpty() ? "-" : metastoreSeeds.get(0))
        .append(' ')
        .append(firstMetastore == null ? "" : firstMetastore)
        .append("\n  of which reachable-only: ")
        .append(metastoreReachable)
        .append('/')
        .append(SEEDS)
        .append('\n');
    report
        .append("first 20 identity flip seeds: ")
        .append(identitySeeds.subList(0, Math.min(20, identitySeeds.size())))
        .append('\n');
    report
        .append("first 20 metastore flip seeds: ")
        .append(metastoreSeeds.subList(0, Math.min(20, metastoreSeeds.size())))
        .append('\n');
    try {
      java.nio.file.Path dir = java.nio.file.Paths.get("build", "dst");
      java.nio.file.Files.createDirectories(dir);
      java.nio.file.Files.write(
          dir.resolve("flip-report.txt"),
          report.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    System.out.println(report);
  }

  /** Runs the fixed two-writer script for one seed and returns any flip found. */
  @SuppressWarnings("unchecked")
  private static Map<String, Set<Long>>[] run(
      long seed, boolean metastoreOps, String[] w1Script, String[] w2Script) {
    return run(seed, metastoreOps, 2, w1Script, w2Script);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Set<Long>>[] run(
      long seed, boolean metastoreOps, int formatVersion, String[] w1Script, String[] w2Script) {
    CommitUuid.reset();
    FilterCacheProbe.reset();
    Trace trace = new Trace();
    FaultConfig config = FaultConfig.quiet();
    config.writers = 2;
    config.seedSnapshots = 2;
    config.rewriteManifestsWorkload = true;
    config.refActions = true;
    config.formatVersion = formatVersion;
    Scheduler sched = new Scheduler(seed, trace, 500_000);
    FakeStore store = new FakeStore();
    FakeFileIO io = new FakeFileIO(store, sched, config);
    store.measureAgeWith(sched::steps);

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion));
    properties.put(TableProperties.COMMIT_NUM_RETRIES, "6");
    properties.put(TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "0");
    properties.put(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "0");
    properties.put("io.manifest.cache-enabled", "false");
    TableMetadata initial =
        TableMetadata.newTableMetadata(
            Simulation.SCHEMA, Simulation.SPEC, Simulation.LOCATION, properties);

    Table table;
    Workload.LastLanded lastLanded;
    java.util.function.Supplier<TableMetadata> currentMetadata;
    if (metastoreOps) {
      FakeMetastoreOperations ops =
          new FakeMetastoreOperations(store, sched, config, io, Simulation.LOCATION, "flip");
      store.measureDeletesAgainst(() -> 0L);
      ops.bootstrap(initial);
      table = new BaseTable(ops, "flip");
      lastLanded = ops::lastLandedFor;
      currentMetadata = ops::current;
    } else {
      FakeTableOperations ops =
          new FakeTableOperations(store, sched, config, io, Simulation.LOCATION);
      store.measureDeletesAgainst(ops::pointerVersion);
      ops.bootstrap(initial);
      table = new BaseTable(ops, "flip");
      lastLanded = ops::lastLandedFor;
      currentMetadata = ops::current;
    }

    // Seed the table, with the scheduler disarmed so setup cannot fail.
    List<DataFile> seedFiles = new ArrayList<>();
    for (int round = 0; round < config.seedSnapshots; round++) {
      org.apache.iceberg.AppendFiles append = table.newAppend();
      for (int i = 0; i < Payload.PARTITIONS.length; i++) {
        DataFile df =
            Payload.dataFile(
                io, Simulation.SPEC, Simulation.LOCATION, "seed" + round, i,
                Payload.PARTITIONS[i]);
        if (round == 0) {
          seedFiles.add(df);
        }
        append.appendFile(df);
      }
      CommitUuid.makeDeterministic(append);
      SameThreadExecutor.inject(append).commit();
    }

    Workload workload =
        new Workload(
            table, lastLanded, store, io.observerView(), sched, config, Simulation.SPEC,
            Simulation.LOCATION, seedFiles);
    List<Workload.Outcome> outcomes = Collections.synchronizedList(new ArrayList<>());

    sched.arm();
    sched.spawn("w1", () -> runScript(workload, "w1", w1Script, outcomes));
    if (w2Script.length > 0) {
      sched.spawn("w2", () -> runScript(workload, "w2", w2Script, outcomes));
    }
    sched.runToCompletion();
    sched.disarm();

    FileIO view = io.observerView();
    try {
      TableMetadata finalMetadata = currentMetadata.get();
      lastMetadata = finalMetadata;
      lastView = view;
      lastManifestWrites = 0;
      for (FakeStore.WriteRecord w : store.writes()) {
        if (FakeStore.kindOf(w.path).equals("man")) {
          lastManifestWrites++;
        }
      }
      lastManifestDeletes = 0;
      for (String deleted : store.deletes()) {
        if (FakeStore.kindOf(deleted).equals("man")) {
          lastManifestDeletes++;
        }
      }
      return new Map[] {
        SequenceNumberFlip.find(finalMetadata, view),
        SequenceNumberFlip.findReachable(finalMetadata, view)
      };
    } finally {
      org.apache.iceberg.ManifestFiles.dropCache(io);
    }
  }

  private static void runScript(
      Workload workload, String fiber, String[] script, List<Workload.Outcome> outcomes) {
    for (int i = 0; i < script.length; i++) {
      Workload.Outcome outcome = new Workload.Outcome(fiber);
      outcome.action = script[i];
      try {
        workload.runAction(script[i], fiber, fiber + "o" + (i + 1), i, outcome);
        outcome.reportedSuccess = true;
      } catch (Throwable t) {
        outcome.reportedSuccess = false;
        outcome.failure = Canon.normalize(t.getClass().getSimpleName() + ": " + t.getMessage());
      }
      outcomes.add(outcome);
    }
  }
}
