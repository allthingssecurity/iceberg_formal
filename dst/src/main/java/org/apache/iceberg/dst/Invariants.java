package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

/**
 * The properties that must hold however the seed interleaved the writers.
 *
 * <p>Each one is stated as a thing a user of the table would notice, not as a thing the code does,
 * so a violation is a defect report rather than a diff against the implementation.
 */
public final class Invariants {

  private Invariants() {}

  public static void check(RunResult result) {
    FileIO io = ((FakeFileIO) result.ops.io()).observerView();
    TableMetadata finalMetadata = result.finalMetadata;
    Set<Long> reachable = TableView.reachable(finalMetadata);

    Map<Long, TableView.SnapshotFiles> finalView = new LinkedHashMap<>();
    for (long id : reachable) {
      finalView.put(id, TableView.read(finalMetadata, id, io));
    }

    i0TheFakesAgreeWithThemselves(result, finalView);
    i1EveryReachableFileIsThereAndWhole(result, finalView);
    i2SuccessMeansVisibleWhenItLanded(result, reachable);
    i3PayloadAppliedAtMostOncePerRef(result, finalMetadata, io);
    i4SnapshotsNeverChange(result, finalView);
    i5FilesAreDeletedOnlyWhenUnreachable(result);
    i7EqualityDeletesKeepApplying(result, finalView);
    i8SequenceNumbersStayWithinTheirSnapshot(result, finalMetadata, reachable, io);
    i9DuplicateDeletesBelongToASnapshotThatDeleted(result, finalMetadata);
    i10ManifestRewritesChangeNothing(result, finalMetadata, io);
    i6HarnessIsInControl(result);
  }

  /**
   * I0: the fakes agree with their own ledger. A violation here is a bug in the harness, not in
   * Iceberg, and it has to fail loudly: a fake that stops recording what it did turns every clean
   * sweep into a false negative.
   */
  private static void i0TheFakesAgreeWithThemselves(
      RunResult result, Map<Long, TableView.SnapshotFiles> view) {
    FakeStore store = result.store;
    Set<String> deleted = new LinkedHashSet<>(store.deletes());
    for (FakeStore.WriteRecord write : store.writes()) {
      if (deleted.contains(write.path)) {
        continue;
      }
      long actual = store.length(write.path);
      if (write.storedBytes == 0 && actual < 0) {
        continue;
      }
      if (actual != write.storedBytes) {
        result.violations.add(
            new Violation(
                "I0-ledger",
                "the store holds "
                    + actual
                    + " bytes for "
                    + store.alias(write.path)
                    + " but the ledger recorded "
                    + write.storedBytes
                    + "; the harness is lying to itself"));
      }
    }
    for (TableView.SnapshotFiles files : view.values()) {
      for (String path : files.everyFile()) {
        if (store.exists(path) && store.writeRecordFor(path) == null) {
          result.violations.add(
              new Violation(
                  "I0-ledger",
                  "reachable file "
                      + store.alias(path)
                      + " is in the store but the ledger has no write for it"));
        }
      }
    }
  }

  /**
   * I1: nothing a reachable snapshot points at is missing, and nothing is shorter than the length
   * recorded for it. This is the one that says the table can still be read.
   */
  private static void i1EveryReachableFileIsThereAndWhole(
      RunResult result, Map<Long, TableView.SnapshotFiles> view) {
    FakeStore store = result.store;
    for (TableView.SnapshotFiles files : view.values()) {
      if (files.unreadable != null) {
        result.violations.add(
            new Violation(
                "I1-readable",
                "snapshot " + files.snapshotId + " cannot be read: " + files.unreadable,
                files.unreadablePath == null
                    ? null
                    : result.store.silentlyTruncated(files.unreadablePath)));
      }
      if (files.manifestList != null && !store.exists(files.manifestList)) {
        result.violations.add(
            new Violation(
                "I1-present",
                "manifest list "
                    + store.alias(files.manifestList)
                    + " of reachable snapshot "
                    + files.snapshotId
                    + " is not in the store"));
      }
      checkAll(result, files, files.manifests, "manifest");
      checkAll(result, files, files.dataFiles, "data file");
      checkAll(result, files, files.deleteFiles, "delete file");
    }
  }

  private static void checkAll(
      RunResult result,
      TableView.SnapshotFiles files,
      Map<String, Long> declared,
      String what) {
    FakeStore store = result.store;
    for (Map.Entry<String, Long> entry : declared.entrySet()) {
      String path = entry.getKey();
      long expected = entry.getValue();
      long actual = store.length(path);
      if (actual < 0) {
        result.violations.add(
            new Violation(
                "I1-present",
                what
                    + " "
                    + store.alias(path)
                    + " of reachable snapshot "
                    + files.snapshotId
                    + " is not in the store",
                result.store.silentlyTruncated(path)));
      } else if (expected >= 0 && actual != expected) {
        result.violations.add(
            new Violation(
                "I1-whole",
                what
                    + " "
                    + store.alias(path)
                    + " of reachable snapshot "
                    + files.snapshotId
                    + " is "
                    + actual
                    + " bytes but the metadata records "
                    + expected,
                result.store.silentlyTruncated(path)));
      }
    }
  }

  /**
   * I2: a writer that was told its commit succeeded saw its snapshot become visible.
   *
   * <p>Stated at the moment the commit landed, not at the end of the run: snapshot expiry, a
   * rollback and a branch replace all legally remove a snapshot that a writer committed, so "it is
   * in the final ancestry" is only true of runs where nothing was ever removed. Where that holds,
   * the stronger form is checked too. This is the same trap I5 fell into — an invariant written
   * before expiry existed encodes the absence of expiry.
   */
  private static void i2SuccessMeansVisibleWhenItLanded(RunResult result, Set<Long> finalReachable) {
    Map<String, List<FakeTableOperations.CommitRecord>> okByFiber = new LinkedHashMap<>();
    for (FakeTableOperations.CommitRecord record : result.ops.commits()) {
      if ("ok".equals(record.answer)) {
        okByFiber.computeIfAbsent(record.fiber, k -> new ArrayList<>()).add(record);
      }
    }
    Map<Long, Set<Long>> reachableAt = result.ops.reachableSnapshotsAtVersion();
    Set<Long> staged = new LinkedHashSet<>();
    for (Workload.Outcome outcome : result.outcomes) {
      staged.addAll(outcome.stagedSnapshotIds);
    }
    boolean nothingEverRemoved = !result.ops.anySnapshotEverRemoved();

    for (Workload.Outcome outcome : result.outcomes) {
      if (!outcome.reportedSuccess || !outcome.expectsCommit) {
        continue;
      }
      List<FakeTableOperations.CommitRecord> landed = okByFiber.get(outcome.fiber);
      if (landed == null || landed.isEmpty()) {
        result.violations.add(
            new Violation(
                "I2",
                outcome.fiber
                    + " was told its "
                    + outcome.action
                    + " succeeded but the catalog never accepted a commit from it"));
        continue;
      }
      for (FakeTableOperations.CommitRecord record : landed) {
        if (record.snapshotId < 0 || staged.contains(record.snapshotId)) {
          // A stageOnly() commit deliberately moves no ref, so it is not reachable and is not
          // supposed to be.
          continue;
        }
        Set<Long> reachableThen = reachableAt.get(record.pointerVersion);
        if (reachableThen != null && !reachableThen.contains(record.snapshotId)) {
          result.violations.add(
              new Violation(
                  "I2",
                  outcome.fiber
                      + " had snapshot "
                      + record.snapshotId
                      + " accepted at pointer version "
                      + record.pointerVersion
                      + ", but no ref could reach it even then"));
        } else if (nothingEverRemoved && !finalReachable.contains(record.snapshotId)) {
          result.violations.add(
              new Violation(
                  "I2",
                  outcome.fiber
                      + " was told its "
                      + outcome.action
                      + " succeeded, snapshot "
                      + record.snapshotId
                      + " was accepted, and nothing was ever expired or rolled back, yet it is not"
                      + " in the ancestry of the pointer"));
        }
      }
    }
  }

  /**
   * I3: a writer's payload is applied at most once <em>on any one ref</em>.
   *
   * <p>Per ref, not globally: branches are independent lineages, so the same data file legitimately
   * appears on {@code main} and on a branch, and a cherrypick legitimately re-applies a staged
   * snapshot's files onto main. What may never happen is the same payload landing twice in the
   * ancestry of a single ref, which is what a retried commit of an already-committed payload, or a
   * cherrypick applied twice, would produce.
   */
  private static void i3PayloadAppliedAtMostOncePerRef(
      RunResult result, TableMetadata metadata, FileIO io) {
    Map<String, Set<Long>> snapshotsAddingPath = new LinkedHashMap<>();
    for (long id : TableView.reachable(metadata)) {
      Snapshot snapshot = metadata.snapshot(id);
      if (snapshot == null) {
        continue;
      }
      try {
        for (DataFile file : snapshot.addedDataFiles(io)) {
          snapshotsAddingPath
              .computeIfAbsent(file.location(), k -> new LinkedHashSet<>())
              .add(id);
        }
      } catch (RuntimeException e) {
        // I1 already reports the unreadable snapshot.
      }
    }
    for (Map.Entry<String, org.apache.iceberg.SnapshotRef> ref : metadata.refs().entrySet()) {
      Set<Long> ancestry = TableView.ancestry(metadata, ref.getValue().snapshotId());
      for (Workload.Outcome outcome : result.outcomes) {
        for (String path : outcome.addedDataFiles) {
          Set<Long> adding = snapshotsAddingPath.get(path);
          if (adding == null) {
            continue;
          }
          Set<Long> onThisRef = new LinkedHashSet<>(adding);
          onThisRef.retainAll(ancestry);
          if (onThisRef.size() > 1) {
            result.violations.add(
                new Violation(
                    "I3",
                    "payload "
                        + result.store.alias(path)
                        + " of "
                        + outcome.fiber
                        + " ("
                        + outcome.action
                        + ", told "
                        + (outcome.reportedSuccess ? "success" : "failure: " + outcome.failure)
                        + ") is added by "
                        + onThisRef.size()
                        + " snapshots in the ancestry of ref '"
                        + ref.getKey()
                        + "': "
                        + onThisRef));
          }
        }
      }
    }
  }

  /** I4: what a snapshot showed when it was created is what it still shows at the end. */
  private static void i4SnapshotsNeverChange(
      RunResult result, Map<Long, TableView.SnapshotFiles> finalView) {
    for (Map.Entry<Long, TableView.SnapshotFiles> entry :
        result.ops.capturedAtCommit().entrySet()) {
      long id = entry.getKey();
      TableView.SnapshotFiles atCommit = entry.getValue();
      TableView.SnapshotFiles atEnd = finalView.get(id);
      if (atEnd == null) {
        // Not reachable at the end. Only legal if it never became an ancestor, which for these
        // operations cannot happen once the pointer accepted it; I2 covers that case.
        continue;
      }
      if (atCommit.unreadable != null) {
        // The snapshot was already broken when it landed; I1 reports it.
        continue;
      }
      if (!atCommit.dataFiles.keySet().equals(atEnd.dataFiles.keySet())) {
        Set<String> gone = new LinkedHashSet<>(atCommit.dataFiles.keySet());
        gone.removeAll(atEnd.dataFiles.keySet());
        Set<String> appeared = new LinkedHashSet<>(atEnd.dataFiles.keySet());
        appeared.removeAll(atCommit.dataFiles.keySet());
        result.violations.add(
            new Violation(
                "I4",
                "snapshot "
                    + id
                    + " showed a different set of rows at the end: "
                    + gone.size()
                    + " data files gone "
                    + alias(result, gone)
                    + ", "
                    + appeared.size()
                    + " appeared "
                    + alias(result, appeared)));
      }
    }
  }

  /**
   * I5: a file is deleted only once nothing can reach it any more.
   *
   * <p>Each delete is judged against the catalog pointer version in force when it happened: the
   * file must be unreachable from the refs at that version, and must stay unreachable at every
   * later version. That is the retention rule. Before snapshot expiry existed in this harness the
   * invariant could be blunter — nothing a committed snapshot ever referenced may be deleted — but
   * expiry's whole job is to make files legally deletable, so the blunt form would have called
   * every correct expiry a defect.
   *
   * <p>Versions whose reachable set could not be read in full are skipped; I1 reports those, and
   * judging a delete against a partial set would invent violations.
   */
  private static void i5FilesAreDeletedOnlyWhenUnreachable(RunResult result) {
    Map<Long, Set<String>> reachableByVersion = result.ops.reachableFilesAtVersion();
    Set<Long> incomplete = result.ops.incompleteVersions();
    long lastVersion = result.ops.pointerVersion();
    for (FakeStore.DeleteRecord deleted : result.store.deleteRecords()) {
      for (long version = deleted.pointerVersion; version <= lastVersion; version++) {
        if (incomplete.contains(version)) {
          continue;
        }
        Set<String> reachable = reachableByVersion.get(version);
        if (reachable != null && reachable.contains(deleted.path)) {
          result.violations.add(
              new Violation(
                  "I5",
                  "file "
                      + result.store.alias(deleted.path)
                      + " was deleted by "
                      + deleted.fiber
                      + " at pointer version "
                      + deleted.pointerVersion
                      + ", but it is still reachable from a ref at pointer version "
                      + version
                      + (version == deleted.pointerVersion
                          ? " (the version in force when it was deleted)"
                          : " (a later version)")));
          break;
        }
      }
    }
  }

  /**
   * I7: a row an equality delete removed never comes back.
   *
   * <p>A reader applies an equality delete to every data file in its partition whose data sequence
   * number is <em>lower</em> than the delete's. A rewrite that replaces a seed file carries the
   * same rows, so if its output ends up at or above the sequence number of an equality delete in
   * the same partition, the rows that delete removed are visible again.
   *
   * <p>The premise holds only for a delete that became visible <em>after</em> the rewrite planned.
   * A delete already in the rewrite's starting snapshot is one the rewrite was meant to apply while
   * rewriting, so those rows are not in the output and giving the output the starting sequence
   * number is correct. An earlier version of this invariant left that condition out and reported
   * one seed in 5,000 of the faithful workload as a defect; it was the invariant that was wrong.
   */
  private static void i7EqualityDeletesKeepApplying(
      RunResult result, Map<Long, TableView.SnapshotFiles> view) {
    Set<String> rewriteOutputsOverSeedFiles = new LinkedHashSet<>();
    Map<String, Workload.Outcome> owner = new LinkedHashMap<>();
    for (Workload.Outcome outcome : result.outcomes) {
      if (outcome.replacedDataFiles.isEmpty()) {
        continue;
      }
      for (String path : outcome.addedDataFiles) {
        rewriteOutputsOverSeedFiles.add(path);
        owner.put(path, outcome);
      }
    }
    if (rewriteOutputsOverSeedFiles.isEmpty()) {
      return;
    }
    for (TableView.SnapshotFiles files : view.values()) {
      for (TableView.FileFact delete : files.facts.values()) {
        if (delete.content != org.apache.iceberg.FileContent.EQUALITY_DELETES) {
          continue;
        }
        for (TableView.FileFact data : files.facts.values()) {
          if (data.content != org.apache.iceberg.FileContent.DATA
              || !data.partition.equals(delete.partition)
              || !rewriteOutputsOverSeedFiles.contains(data.path)) {
            continue;
          }
          Workload.Outcome rewriter = owner.get(data.path);
          long startingSequenceNumber =
              rewriter == null || rewriter.startingSequenceNumber == null
                  ? 0L
                  : rewriter.startingSequenceNumber;
          boolean deleteIsNewerThanThePlan = delete.dataSequenceNumber > startingSequenceNumber;
          if (deleteIsNewerThanThePlan && data.dataSequenceNumber >= delete.dataSequenceNumber) {
            result.violations.add(
                new Violation(
                    "I7",
                    "at snapshot "
                        + files.snapshotId
                        + ", equality delete "
                        + result.store.alias(delete.path)
                        + " (partition "
                        + delete.partition
                        + ", seq "
                        + delete.dataSequenceNumber
                        + ") no longer applies to "
                        + result.store.alias(data.path)
                        + " (seq "
                        + data.dataSequenceNumber
                        + "), a rewrite output whose plan only saw sequence number "
                        + startingSequenceNumber
                        + (rewriter == null
                            ? ""
                            : "; the rewrite was committed by "
                                + rewriter.fiber
                                + " with suppliedSequenceNumber="
                                + rewriter.suppliedSequenceNumber)
                        + ": the rows that delete removed are visible again"));
          }
        }
      }
    }
  }

  /**
   * I8: no file carries a data sequence number greater than the snapshot that added it.
   *
   * <p>In the v2 and v3 format a file's data sequence number is the sequence number of the snapshot
   * that added it, or an older one when a rewrite preserves the age of the rows it moves. A number
   * <em>ahead</em> of the adding snapshot has no meaning: it orders the file after deletes that
   * have not been written yet, so a delete committed later and legitimately will not apply to it
   * either. This holds whatever the harness does and needs no model of rows, which makes it the
   * cheapest statement of the same defect I7 describes.
   */
  private static void i8SequenceNumbersStayWithinTheirSnapshot(
      RunResult result, TableMetadata metadata, Set<Long> reachable, FileIO io) {
    for (long id : reachable) {
      Snapshot snapshot = metadata.snapshot(id);
      if (snapshot == null) {
        continue;
      }
      long snapshotSequenceNumber = snapshot.sequenceNumber();
      TableView.SnapshotFiles files = TableView.read(metadata, id, io);
      if (files.unreadable != null) {
        continue;
      }
      for (TableView.FileFact fact : files.facts.values()) {
        if (fact.dataSequenceNumber > snapshotSequenceNumber) {
          result.violations.add(
              new Violation(
                  "I8",
                  result.store.alias(fact.path)
                      + " carries data sequence number "
                      + fact.dataSequenceNumber
                      + " but snapshot "
                      + id
                      + ", which is where a reader finds it, has sequence number "
                      + snapshotSequenceNumber
                      + "; the file is ordered after deletes that do not exist yet"));
        }
      }
    }
  }

  /**
   * I9: a snapshot that deleted no data file cannot have deleted a duplicate of one.
   *
   * <p>{@code deleted-duplicate-files} counts entries that named a path already removed in the same
   * manifest, so it is a subset of the work {@code deleted-data-files} counts. A snapshot that
   * reports the first without the second is describing work it did not do, which is what a reader
   * of the summary — a maintenance job deciding whether compaction is worthwhile, a dashboard — is
   * then wrong about.
   *
   * <p>This is the reachable half of apache/iceberg#16686. Before that fix
   * {@code ManifestFilterManager.duplicateDeleteCount} was a field on the manager, so a commit
   * attempt that found a duplicate and was then rejected for a stale pointer carried its count
   * into the attempt that eventually committed.
   */
  private static void i9DuplicateDeletesBelongToASnapshotThatDeleted(
      RunResult result, TableMetadata metadata) {
    for (Snapshot snapshot : metadata.snapshots()) {
      Map<String, String> summary = snapshot.summary();
      if (summary == null) {
        continue;
      }
      long duplicates = asLong(summary.get("deleted-duplicate-files"));
      if (duplicates <= 0) {
        continue;
      }
      long deleted = asLong(summary.get("deleted-data-files"));
      if (deleted <= 0) {
        result.violations.add(
            new Violation(
                "I9",
                "snapshot "
                    + snapshot.snapshotId()
                    + " ("
                    + snapshot.operation()
                    + ") reports deleted-duplicate-files="
                    + duplicates
                    + " but deleted-data-files="
                    + (summary.containsKey("deleted-data-files")
                        ? summary.get("deleted-data-files")
                        : "absent")
                    + "; it is claiming duplicate deletes for files it did not delete"));
      }
    }
  }

  private static long asLong(String value) {
    if (value == null) {
      return 0;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * I10: regrouping the manifests changes nothing a reader can see.
   *
   * <p>A manifest rewrite moves entries between manifests. The snapshot it produces must contain
   * exactly the same content files as its parent, each with the same data sequence number, because
   * the sequence number is what decides which deletes apply to a file. Iceberg checks only that the
   * active file <em>count</em> matches ({@code BaseRewriteManifests.validateFilesCounts}), so a
   * rewrite that swapped a file for another, or lifted a file's sequence number, would pass its own
   * validation. apache/iceberg#3842 was exactly that: merged manifests lost the sequence number of
   * added files.
   */
  private static void i10ManifestRewritesChangeNothing(
      RunResult result, TableMetadata metadata, FileIO io) {
    Map<Long, String> rewrites = new LinkedHashMap<>();
    for (Workload.Outcome outcome : result.outcomes) {
      if (outcome.manifestRewriteSnapshotId != null) {
        rewrites.put(outcome.manifestRewriteSnapshotId, outcome.fiber);
      }
    }
    if (rewrites.isEmpty()) {
      return;
    }
    for (Map.Entry<Long, String> landed : rewrites.entrySet()) {
      Snapshot snapshot = metadata.snapshot(landed.getKey());
      if (snapshot == null || snapshot.parentId() == null) {
        continue;
      }
      Snapshot parent = metadata.snapshot(snapshot.parentId());
      if (parent == null) {
        continue;
      }
      TableView.SnapshotFiles after = TableView.read(metadata, snapshot.snapshotId(), io);
      TableView.SnapshotFiles before = TableView.read(metadata, parent.snapshotId(), io);
      if (after.unreadable != null || before.unreadable != null) {
        // I1 reports the unreadable side; comparing against a partial read invents violations.
        continue;
      }
      Set<String> gone = new LinkedHashSet<>(before.facts.keySet());
      gone.removeAll(after.facts.keySet());
      Set<String> appeared = new LinkedHashSet<>(after.facts.keySet());
      appeared.removeAll(before.facts.keySet());
      if (!gone.isEmpty() || !appeared.isEmpty()) {
        result.violations.add(
            new Violation(
                "I10",
                "the manifest rewrite in snapshot "
                    + snapshot.snapshotId()
                    + " by "
                    + landed.getValue()
                    + " changed the table's contents: "
                    + gone.size()
                    + " files gone "
                    + alias(result, gone)
                    + ", "
                    + appeared.size()
                    + " appeared "
                    + alias(result, appeared)));
        continue;
      }
      for (Map.Entry<String, TableView.FileFact> entry : after.facts.entrySet()) {
        TableView.FileFact wasFact = before.facts.get(entry.getKey());
        TableView.FileFact isFact = entry.getValue();
        if (wasFact == null) {
          continue;
        }
        if (before.ambiguousSequenceNumbers.containsKey(entry.getKey())
            || after.ambiguousSequenceNumbers.containsKey(entry.getKey())) {
          // The path appears more than once in one of the two snapshots with different sequence
          // numbers, so "its" sequence number is not a single value and comparing one is
          // meaningless. I11 reports the ambiguity itself.
          continue;
        }
        if (wasFact.dataSequenceNumber != isFact.dataSequenceNumber) {
          result.violations.add(
              new Violation(
                  "I10",
                  "the manifest rewrite in snapshot "
                      + snapshot.snapshotId()
                      + " by "
                      + landed.getValue()
                      + " moved "
                      + result.store.alias(entry.getKey())
                      + " from data sequence number "
                      + wasFact.dataSequenceNumber
                      + " to "
                      + isFact.dataSequenceNumber
                      + "; which deletes apply to it has changed"));
        }
      }
    }
  }

  /** I6: the harness was in control. A failure here means the result above cannot be trusted. */
  private static void i6HarnessIsInControl(RunResult result) {
    Scheduler sched = result.scheduler;
    String escapeExcuse = null;
    if (!SameThreadExecutor.writePoolInjectable()) {
      escapeExcuse =
          "this Iceberg release has no SnapshotUpdate.writeManifestsWith (added in 1.12.0), so"
              + " manifest writes run on Iceberg's own worker pool; with"
              + " -Diceberg.worker.num-threads=1 they are serialised inside the turn of the"
              + " writer that submitted them, but the harness cannot interleave another writer"
              + " inside one manifest-write batch";
    } else if (usedManageSnapshots(result)) {
      // SnapshotManager.cherrypick and friends run an internal SnapshotProducer
      // (CherryPickOperation, SetSnapshotOperation) inside a transaction the caller never sees.
      // ManageSnapshots declares no ExecutorService and no deleteWith, so those producers always
      // take ThreadPools.getWorkerPool(). With -Diceberg.worker.num-threads=1 the work is
      // serialised inside the committing fiber's turn.
      escapeExcuse =
          "a ManageSnapshots operation ran (cherrypick / fast-forward / rollback); that API"
              + " exposes no executor and no delete callback, so its internal snapshot producers"
              + " cannot be injected. Labels: "
              + sched.unmanagedLabels();
    } else if (onlyDeletesEscaped(sched)) {
      // BaseTransaction overrides each operation's deleteWith with its own collector and then
      // batches the paths through CatalogUtil.deleteFiles, which takes no ExecutorService and
      // uses ThreadPools.getWorkerPool(). Unlike SnapshotProducer (scanManifestsWith,
      // writeManifestsWith, deleteWith) and RemoveSnapshots (planWith, executeDeleteWith), a
      // transaction's cleanup offers the caller no way to supply either. With
      // -Diceberg.worker.num-threads=1 the deletes are serialised inside the committing fiber's
      // turn, so the schedule still holds; the harness just cannot interleave inside that batch.
      escapeExcuse =
          "every escaped crossing was a delete on Iceberg's global worker pool:"
              + " BaseTransaction cleans up through CatalogUtil.deleteFiles, which takes no"
              + " executor and no delete callback, so there is no injection point. Labels: "
              + sched.unmanagedLabels();
    }
    if (sched.unmanagedCrossings() > 0) {
      result.violations.add(
          new Violation(
              "I6-escape",
              sched.unmanagedCrossings()
                  + " fake-boundary calls came from a thread the harness did not spawn;"
                  + " the schedule is not the whole story. First one:\n      "
                  + sched.firstUnmanagedStack(),
              escapeExcuse));
    }
    if (sched.unmanagedDraws() > 0) {
      result.violations.add(
          new Violation(
              "I6-escape",
              sched.unmanagedDraws() + " fault draws came from an unmanaged thread",
              escapeExcuse));
    }
    if (sched.hung()) {
      result.violations.add(
          new Violation(
              "I6-hang",
              "the run did not finish after "
                  + sched.steps()
                  + " steps; still alive: "
                  + sched.stuckFibers()));
    }
  }

  private static boolean usedManageSnapshots(RunResult result) {
    for (Workload.Outcome outcome : result.outcomes) {
      if (outcome.usedManageSnapshots) {
        return true;
      }
    }
    return false;
  }

  private static boolean onlyDeletesEscaped(Scheduler sched) {
    Set<String> labels = sched.unmanagedLabels();
    if (labels.isEmpty()) {
      return false;
    }
    for (String label : labels) {
      if (!label.startsWith("delete:")) {
        return false;
      }
    }
    return true;
  }

  private static String alias(RunResult result, Set<String> paths) {
    List<String> out = new ArrayList<>();
    for (String path : paths) {
      out.add(result.store.alias(path));
    }
    return out.toString();
  }
}
