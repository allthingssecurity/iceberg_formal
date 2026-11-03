package org.apache.iceberg.dst;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Narrows the swarm's I10 report down to the smallest thing that shows it: one thread, no faults,
 * no concurrency, no scheduler. Everything the harness normally brings is switched off, so whatever
 * is left is either Iceberg or the fake catalog's compare-and-set.
 */
class CherrypickSequenceNumberTest {

  @Test
  @DisplayName("a cherrypicked manifest keeps one sequence number across every snapshot")
  void cherrypickedManifestSequenceNumberIsStable() {
    Trace trace = new Trace();
    Scheduler sched = new Scheduler(1L, trace, 1_000_000);
    FakeStore store = new FakeStore();
    FaultConfig config = FaultConfig.quiet();
    FakeFileIO io = new FakeFileIO(store, sched, config);
    FakeTableOperations ops =
        new FakeTableOperations(store, sched, config, io, Simulation.LOCATION);
    store.measureDeletesAgainst(ops::pointerVersion);
    store.measureAgeWith(sched::steps);

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(TableProperties.FORMAT_VERSION, "2");
    properties.put("io.manifest.cache-enabled", "false");
    ops.bootstrap(
        TableMetadata.newTableMetadata(
            Simulation.SCHEMA, Simulation.SPEC, Simulation.LOCATION, properties));
    Table table = new BaseTable(ops, "cherrypick");

    // The scheduler is never armed, so nothing parks and no fault is ever drawn.
    DataFile base = Payload.dataFile(io, Simulation.SPEC, Simulation.LOCATION, "base", 0, "a");
    SameThreadExecutor.inject(table.newAppend().appendFile(base)).commit();

    // A staged snapshot: it moves no ref, and its manifest is written while the table is young.
    DataFile staged = Payload.dataFile(io, Simulation.SPEC, Simulation.LOCATION, "staged", 0, "a");
    SameThreadExecutor.inject(table.newAppend().appendFile(staged).stageOnly()).commit();
    long stagedSnapshotId = findStagedSnapshot(ops.current());

    // Several ordinary commits, so the table's sequence number moves well past the staged one.
    for (int i = 0; i < 4; i++) {
      DataFile more =
          Payload.dataFile(io, Simulation.SPEC, Simulation.LOCATION, "more" + i, 0, "a");
      SameThreadExecutor.inject(table.newAppend().appendFile(more)).commit();
    }

    // Now cherrypick the staged snapshot onto main, then commit once more on top.
    table.manageSnapshots().cherrypick(stagedSnapshotId).commit();
    DataFile after = Payload.dataFile(io, Simulation.SPEC, Simulation.LOCATION, "after", 0, "a");
    SameThreadExecutor.inject(table.newAppend().appendFile(after)).commit();

    // Every snapshot that lists the staged file's manifest must agree on its sequence number, and
    // therefore on the data sequence number its rows inherit. Which deletes apply to a row is
    // decided by that number, so two snapshots disagreeing about it is two different tables.
    TableMetadata metadata = ops.current();
    FileIO view = io.observerView();
    Map<String, Set<Long>> sequenceNumbersPerManifest = new LinkedHashMap<>();
    Map<String, Set<Long>> inheritedPerFile = new LinkedHashMap<>();
    for (long id : TableView.reachable(metadata)) {
      Snapshot snapshot = metadata.snapshot(id);
      for (ManifestFile manifest : snapshot.allManifests(view)) {
        if (manifest.content() != ManifestContent.DATA) {
          continue;
        }
        sequenceNumbersPerManifest
            .computeIfAbsent(manifest.path(), k -> new LinkedHashSet<>())
            .add(manifest.sequenceNumber());
      }
      TableView.SnapshotFiles files = TableView.read(metadata, id, view);
      for (TableView.FileFact fact : files.facts.values()) {
        inheritedPerFile
            .computeIfAbsent(fact.path, k -> new LinkedHashSet<>())
            .add(fact.dataSequenceNumber);
      }
    }

    StringBuilder report = new StringBuilder();
    report.append("staged snapshot = ").append(stagedSnapshotId).append('\n');
    for (Snapshot snapshot : metadata.snapshots()) {
      report
          .append("  snap ")
          .append(snapshot.snapshotId())
          .append(" parent=")
          .append(snapshot.parentId())
          .append(" seq=")
          .append(snapshot.sequenceNumber())
          .append(' ')
          .append(snapshot.operation())
          .append('\n');
    }
    sequenceNumbersPerManifest.forEach(
        (path, seqs) ->
            report.append("  manifest ").append(store.alias(path)).append(" seqs=").append(seqs).append('\n'));
    inheritedPerFile.forEach(
        (path, seqs) ->
            report.append("  file ").append(store.alias(path)).append(" dataSeqs=").append(seqs).append('\n'));

    sequenceNumbersPerManifest.forEach(
        (path, seqs) ->
            assertEquals(
                1,
                seqs.size(),
                "manifest "
                    + store.alias(path)
                    + " is listed with more than one sequence number: "
                    + seqs
                    + "\n"
                    + report));
    inheritedPerFile.forEach(
        (path, seqs) ->
            assertEquals(
                1,
                seqs.size(),
                "file "
                    + store.alias(path)
                    + " resolves to more than one data sequence number: "
                    + seqs
                    + "\n"
                    + report));
  }

  private static long findStagedSnapshot(TableMetadata metadata) {
    Set<Long> reachable = TableView.reachable(metadata);
    for (Snapshot snapshot : metadata.snapshots()) {
      if (!reachable.contains(snapshot.snapshotId())) {
        return snapshot.snapshotId();
      }
    }
    throw new IllegalStateException("no staged snapshot found");
  }
}
