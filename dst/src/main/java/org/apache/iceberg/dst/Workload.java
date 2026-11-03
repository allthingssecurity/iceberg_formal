package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;

/** One writer's job: pick an operation from the seed, build its payload, and commit it. */
public final class Workload {

  /** What a writer did and what it was told. */
  public static final class Outcome {
    public final String fiber;
    public String action = "none";
    public boolean reportedSuccess;
    public String failure;
    public final Set<String> addedDataFiles = new LinkedHashSet<>();
    public final Set<String> addedDeleteFiles = new LinkedHashSet<>();
    public final Set<String> replacedDataFiles = new LinkedHashSet<>();
    /** The data sequence number this writer handed to {@code dataSequenceNumber}, if any. */
    public Long suppliedSequenceNumber;

    /**
     * The sequence number of the snapshot this writer actually planned against. A rewrite is only
     * wrong about a delete that became visible <em>after</em> this point: a delete already in the
     * starting snapshot is one the rewrite was supposed to apply while rewriting, so its rows are
     * not in the output at all.
     */
    public Long startingSequenceNumber;

    /** The snapshot this writer expired, when it was the expiry job. */
    public Long expiredSnapshotId;

    /** How many files this writer removed, when it was the orphan cleaner. */
    public int orphansDeleted;

    /** The snapshot a manifest rewrite produced, when this operation was one. */
    public Long manifestRewriteSnapshotId;

    /**
     * Whether this writer is expected to land a commit. An orphan cleaner only deletes files, and
     * an expiry with nothing to expire does nothing at all, so neither has a snapshot for I2 to
     * find in the ancestry.
     */
    public boolean expectsCommit = true;

    /** Snapshots this operation staged: they move no ref, so I2 must not expect them visible. */
    public final Set<Long> stagedSnapshotIds = new LinkedHashSet<>();

    /**
     * Set when the operation went through {@code ManageSnapshots}, which exposes no executor and no
     * delete callback, so its internal snapshot producers always use Iceberg's global pools.
     */
    public boolean usedManageSnapshots;
    public boolean validated;

    public Outcome(String fiber) {
      this.fiber = fiber;
    }

    @Override
    public String toString() {
      return fiber
          + " "
          + action
          + (validated ? " (validated)" : "")
          + " -> "
          + (reportedSuccess ? "success" : "failed: " + failure);
    }
  }

  /** Where a writer asks which snapshot it just landed; both catalogs can answer it. */
  public interface LastLanded {
    Long forFiber(String fiber);
  }

  private final Table table;
  private final LastLanded ops;
  private final FakeStore store;
  private final org.apache.iceberg.io.FileIO payloadIo;
  private final Scheduler sched;
  private final FaultConfig config;
  private final PartitionSpec spec;
  private final String location;
  private final List<DataFile> seedFiles;

  public Workload(
      Table table,
      LastLanded ops,
      FakeStore store,
      org.apache.iceberg.io.FileIO payloadIo,
      Scheduler sched,
      FaultConfig config,
      PartitionSpec spec,
      String location,
      List<DataFile> seedFiles) {
    this.table = table;
    this.ops = ops;
    this.store = store;
    this.payloadIo = payloadIo;
    this.sched = sched;
    this.config = config;
    this.spec = spec;
    this.location = location;
    this.seedFiles = seedFiles;
  }

  /**
   * Scenario-specific actions a test registers by name (r5). They run on the fiber like any other
   * action and must inject their own executors ({@link SameThreadExecutor}) and commit UUIDs.
   */
  private final java.util.Map<String, java.util.function.BiConsumer<Table, Outcome>> customActions =
      new java.util.LinkedHashMap<>();

  public void registerAction(String name, java.util.function.BiConsumer<Table, Outcome> action) {
    customActions.put(name, action);
  }

  private static final String[] ACTIONS = {
    "append", "rewrite", "overwrite", "replace-partitions", "row-delta"
  };

  private static final String[] ACTIONS_WITH_MANIFEST_REWRITE = {
    "append", "rewrite", "overwrite", "replace-partitions", "row-delta", "rewrite-manifests"
  };

  /**
   * The operations that compose several commits or move refs around. These are much less exercised
   * than a plain append: transactions have their own retry and cleanup logic in
   * {@code BaseTransaction}, and cherrypick, fast-forward and rollback all rewrite which snapshots
   * a ref can reach while other writers are committing.
   */
  private static final String[] REF_ACTIONS = {
    "transaction", "branch-append", "stage-and-cherrypick", "fast-forward", "rollback"
  };

  /**
   * Narrowing set: appends, transactions and manifest rewrites. I10 only has anything to say once a
   * manifest rewrite has happened, so leaving it out makes the invariant vacuous rather than clean.
   */
  private static final String[] TRANSACTION_ONLY = {"transaction", "append", "rewrite-manifests"};

  private static final String[] BRANCHES = {"b1", "b2"};

  /** Runs this writer's whole program: {@code operationsPerWriter} operations, in order. */
  public java.util.List<Outcome> runAll(String fiber, int index) {
    java.util.List<Outcome> out = new java.util.ArrayList<>();
    int operations = Math.max(1, config.operationsPerWriter);
    for (int i = 0; i < operations; i++) {
      // The fiber name stays the scheduler's, because that is what the fake catalog records
      // against each commit; the operation number goes into the action label instead.
      // The payload owner has to be unique per operation or the second append of a writer reuses
      // the first one's file paths, and then every invariant about "applied at most once" or
      // "never deleted while reachable" fires on the harness's own doing.
      Outcome outcome = run(fiber, operations > 1 ? fiber + "o" + (i + 1) : fiber, index + i);
      if (operations > 1) {
        outcome.action = "#" + (i + 1) + " " + outcome.action;
      }
      out.add(outcome);
    }
    return out;
  }

  public Outcome run(String fiber, int index) {
    return run(fiber, fiber, index);
  }

  /**
   * @param fiber the scheduler fiber, which is what the fake catalog records against a commit
   * @param owner a name unique to this operation, used for the payload file paths
   */
  public Outcome run(String fiber, String owner, int index) {
    Outcome outcome = new Outcome(fiber);
    if (config.duplicateDeleteWorkload) {
      // Every writer deletes everything, so one wins and the others retry with nothing left to
      // delete. That retry is what apache/iceberg#16686 miscounted.
      outcome.action = "delete-all";
      try {
        deleteAll();
        outcome.reportedSuccess = true;
      } catch (Throwable t) {
        outcome.reportedSuccess = false;
        outcome.failure = Canon.normalize(t.getClass().getSimpleName() + ": " + t.getMessage());
      }
      return outcome;
    }
    if (config.orphanCleanWorkload && index == 1) {
      outcome.action = "orphan-clean";
      outcome.expectsCommit = false;
      try {
        orphanClean(outcome);
        outcome.reportedSuccess = true;
      } catch (Throwable t) {
        outcome.reportedSuccess = false;
        outcome.failure = Canon.normalize(t.getClass().getSimpleName() + ": " + t.getMessage());
      }
      return outcome;
    }
    if (config.expireWorkload && index == 0) {
      // One writer is the maintenance job. Keeping it at a fixed index means every seed has an
      // expiry racing the others, rather than only the seeds where the draw happened to pick one.
      outcome.action = "expire";
      try {
        expireOldestSnapshot(outcome);
        outcome.reportedSuccess = true;
      } catch (Throwable t) {
        outcome.reportedSuccess = false;
        outcome.failure = Canon.normalize(t.getClass().getSimpleName() + ": " + t.getMessage());
      }
      return outcome;
    }
    String[] actions =
        config.rewriteManifestsWorkload ? ACTIONS_WITH_MANIFEST_REWRITE : ACTIONS;
    String action;
    if (config.transactionsOnly) {
      action = TRANSACTION_ONLY[sched.draw("txn.action", TRANSACTION_ONLY.length)];
      outcome.action = action;
      runActionGuarded(action, fiber, owner, index, outcome);
      return outcome;
    }
    if (config.refActions && sched.draw("action.kind", 3) == 0) {
      action = REF_ACTIONS[sched.draw("ref.action", REF_ACTIONS.length)];
    } else {
      action = actions[sched.draw("action", actions.length)];
    }
    outcome.action = action;
    try {
      runAction(action, fiber, owner, index, outcome);
      outcome.reportedSuccess = true;
    } catch (Throwable t) {
      outcome.reportedSuccess = false;
      outcome.failure = Canon.normalize(t.getClass().getSimpleName() + ": " + t.getMessage());
    }
    return outcome;
  }

  private void runActionGuarded(
      String action, String fiber, String owner, int index, Outcome outcome) {
    try {
      runAction(action, fiber, owner, index, outcome);
      outcome.reportedSuccess = true;
    } catch (Throwable t) {
      outcome.reportedSuccess = false;
      outcome.failure = Canon.normalize(t.getClass().getSimpleName() + ": " + t.getMessage());
    }
  }

  void runAction(String action, String fiber, String owner, int index, Outcome outcome) {
    {
      switch (action) {
        case "append":
          append(owner, index, outcome);
          break;
        case "rewrite":
          rewrite(owner, index, outcome);
          break;
        case "overwrite":
          overwrite(owner, index, outcome);
          break;
        case "replace-partitions":
          replacePartitions(owner, index, outcome);
          break;
        case "row-delta":
          rowDelta(owner, index, outcome);
          break;
        case "rewrite-manifests":
          rewriteManifests(outcome);
          break;
        case "transaction":
          transaction(owner, index, outcome);
          break;
        case "branch-append":
          branchAppend(owner, index, outcome);
          break;
        case "stage-and-cherrypick":
          stageAndCherrypick(owner, index, outcome);
          break;
        case "fast-forward":
          fastForward(outcome);
          break;
        case "rollback":
          rollback(outcome);
          break;
        case "expire-policy":
          expireByPolicy(outcome);
          break;
        case "transaction-dv":
          transactionWithDv(owner, index, outcome);
          break;
        case "transaction-rewrite-manifests":
          transactionRewriteManifests(owner, index, outcome);
          break;
        default:
          if (customActions.containsKey(action)) {
            customActions.get(action).accept(table, outcome);
            break;
          }
          if (action.startsWith("ms:")) {
            manageSnapshots(action, outcome);
            break;
          }
          throw new IllegalStateException(action);
      }
    }
  }

  private String partition(int index) {
    return Payload.PARTITIONS[index % Payload.PARTITIONS.length];
  }

  private <T extends SnapshotUpdate<T>> T controlled(T update) {
    CommitUuid.makeDeterministic(update);
    // Every pool the producer would otherwise take from ThreadPools runs inline instead.
    return SameThreadExecutor.inject(update);
  }

  private void append(String fiber, int index, Outcome outcome) {
    org.apache.iceberg.AppendFiles append = table.newAppend();
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df =
          Payload.dataFile(
              payloadIo, spec, location, fiber, i, partition(index + i));
      outcome.addedDataFiles.add(df.location());
      append.appendFile(df);
    }
    controlled(append).commit();
  }

  private void rewrite(String fiber, int index, Outcome outcome) {
    Set<DataFile> replaced = new LinkedHashSet<>();
    // Replace one of the files the table was seeded with. Two writers that pick the same one must
    // not both succeed.
    replaced.add(seedFiles.get(index % seedFiles.size()));
    // A rewrite may not add more records than it replaces, so split the replaced file's four
    // records across the replacement files.
    int perFile = Math.max(1, 4 / config.filesPerWriter);
    Set<DataFile> added = new LinkedHashSet<>();
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df =
          Payload.dataFile(payloadIo, spec, location, fiber, i, partition(index), perFile);
      added.add(df);
      outcome.addedDataFiles.add(df.location());
    }
    for (DataFile df : replaced) {
      outcome.replacedDataFiles.add(df.location());
    }
    org.apache.iceberg.RewriteFiles rewrite = table.newRewrite();
    Snapshot current = table.currentSnapshot();
    if (current != null) {
      rewrite.validateFromSnapshot(current.snapshotId());
    }
    outcome.startingSequenceNumber = current == null ? 0L : current.sequenceNumber();
    // The per-file API plus dataSequenceNumber(long) is the supported shape: every
    // rewriteFiles(Set, ...) overload has been deprecated since 1.3.0.
    for (DataFile df : replaced) {
      rewrite.deleteFile(df);
    }
    for (DataFile df : added) {
      rewrite.addFile(df);
    }
    if (config.preFix == FaultConfig.PreFix.REWRITE_WITHOUT_SEQUENCE_NUMBER) {
      // No sequence number at all: the replacement files inherit a fresh, higher one. Iceberg then
      // keeps equality-delete conflict detection on, so this should fail the commit rather than
      // resurrect rows. The scorecard records which of the two happens.
      outcome.suppliedSequenceNumber = null;
    } else if (config.sequenceAheadRewrite) {
      // The unguarded path: dataSequenceNumber(long) takes any value, and supplying one switches
      // off equality-delete conflict detection. A value ahead of the table's therefore both skips
      // the check and out-ranks every delete committed so far.
      long ahead = (current == null ? 0L : current.sequenceNumber()) + 5L;
      outcome.suppliedSequenceNumber = ahead;
      rewrite.dataSequenceNumber(ahead);
    } else {
      long starting = current == null ? 0L : current.sequenceNumber();
      outcome.suppliedSequenceNumber = starting;
      rewrite.dataSequenceNumber(starting);
    }
    controlled(rewrite).commit();
  }

  private void overwrite(String fiber, int index, Outcome outcome) {
    org.apache.iceberg.OverwriteFiles overwrite = table.newOverwrite();
    String part = partition(index);
    overwrite.overwriteByRowFilter(Expressions.equal("data", part));
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df = Payload.dataFile(payloadIo, spec, location, fiber, i, part);
      outcome.addedDataFiles.add(df.location());
      overwrite.addFile(df);
    }
    // Round 1 found that the Spark overwrite paths validate nothing when isolation-level is unset
    // (findings/r1-code-hunt.md, M2). Draw whether to validate, so both paths are explored.
    if (sched.draw("overwrite.validate", 2) == 1) {
      Snapshot current = table.currentSnapshot();
      if (current != null) {
        overwrite.validateFromSnapshot(current.snapshotId());
      }
      overwrite.validateNoConflictingData();
      overwrite.validateNoConflictingDeletes();
      outcome.validated = true;
    }
    controlled(overwrite).commit();
  }

  private void replacePartitions(String fiber, int index, Outcome outcome) {
    org.apache.iceberg.ReplacePartitions replace = table.newReplacePartitions();
    String part = partition(index);
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df = Payload.dataFile(payloadIo, spec, location, fiber, i, part);
      outcome.addedDataFiles.add(df.location());
      replace.addFile(df);
    }
    if (sched.draw("replace.validate", 2) == 1) {
      Snapshot current = table.currentSnapshot();
      if (current != null) {
        replace.validateFromSnapshot(current.snapshotId());
      }
      replace.validateNoConflictingData();
      replace.validateNoConflictingDeletes();
      outcome.validated = true;
    }
    controlled(replace).commit();
  }

  /**
   * Expires the oldest snapshot that is not the head of a ref.
   *
   * <p>By snapshot id rather than by age: {@code RemoveSnapshots} reads
   * {@code System.currentTimeMillis()} in its constructor, and every snapshot in a run is written
   * within the same millisecond or two, so {@code expireOlderThan} would be a coin toss rather
   * than a decision the seed controls.
   */
  private void expireOldestSnapshot(Outcome outcome) {
    if (config.expireRetainLast > 0) {
      expireByPolicy(outcome);
      return;
    }
    Long target = null;
    java.util.Set<Long> refHeads = new LinkedHashSet<>();
    for (org.apache.iceberg.SnapshotRef ref : table.refs().values()) {
      refHeads.add(ref.snapshotId());
    }
    for (Snapshot snapshot : table.snapshots()) {
      if (refHeads.contains(snapshot.snapshotId())) {
        continue;
      }
      if (target == null || snapshot.snapshotId() < target) {
        target = snapshot.snapshotId();
      }
    }
    if (target == null) {
      outcome.action = "expire(nothing to expire)";
      outcome.expectsCommit = false;
      return;
    }
    outcome.expiredSnapshotId = target;
    outcome.action = "expire(" + target + ")";
    org.apache.iceberg.ExpireSnapshots expire =
        table
            .expireSnapshots()
            .expireSnapshotId(target)
            .planWith(new SameThreadExecutor())
            .executeDeleteWith(new SameThreadExecutor())
            .cleanExpiredFiles(true);
    if (config.selfCheck == FaultConfig.SelfCheck.EXPIRY_OVER_DELETES) {
      java.util.concurrent.atomic.AtomicBoolean overDeleted =
          new java.util.concurrent.atomic.AtomicBoolean();
      expire.deleteWith(
          path -> {
            table.io().deleteFile(path);
            if (overDeleted.compareAndSet(false, true)) {
              // One manifest of the snapshot that is still the head of main: a correct expiry
              // would have pruned it out of the deletion candidates.
              Snapshot head = table.currentSnapshot();
              if (head != null) {
                for (org.apache.iceberg.ManifestFile manifest : head.allManifests(table.io())) {
                  table.io().deleteFile(manifest.path());
                  break;
                }
              }
            }
          });
    }
    expire.commit();
  }

  /**
   * Expires by policy: {@code expireOlderThan(Long.MAX_VALUE).retainLast(n)}. Every snapshot is
   * older than Long.MAX_VALUE, so the outcome depends only on {@code retainLast} and the refs, not
   * on the wall clock. No snapshot id is named, so on a main-only table Iceberg chooses
   * IncrementalFileCleanup (RemoveSnapshots.java:390-397) unless the config forces a strategy.
   */
  private void expireByPolicy(Outcome outcome) {
    outcome.action = "expire(retainLast=" + config.expireRetainLast + ")";
    // Nothing for I2 to find: an expiry adds no snapshot.
    outcome.expectsCommit = false;
    org.apache.iceberg.ExpireSnapshots expire =
        table
            .expireSnapshots()
            .expireOlderThan(Long.MAX_VALUE)
            .retainLast(config.expireRetainLast)
            .planWith(new SameThreadExecutor())
            .executeDeleteWith(new SameThreadExecutor())
            .cleanExpiredFiles(true);
    if (config.expireIncrementalCleanup != null) {
      forceCleanupStrategy(expire, config.expireIncrementalCleanup);
    }
    outcome.action += config.expireIncrementalCleanup == null
        ? "[cleanup=iceberg-default]"
        : config.expireIncrementalCleanup ? "[cleanup=incremental]" : "[cleanup=reachable]";
    expire.commit();
  }

  /** RemoveSnapshots.withIncrementalCleanup(boolean) is package-private; reached by reflection. */
  static void forceCleanupStrategy(org.apache.iceberg.ExpireSnapshots expire, boolean incremental) {
    try {
      java.lang.reflect.Method method =
          expire.getClass().getDeclaredMethod("withIncrementalCleanup", boolean.class);
      method.setAccessible(true);
      method.invoke(expire, incremental);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "cannot force the cleanup strategy on " + expire.getClass().getName(), e);
    }
  }

  /**
   * Deletes files the table cannot reach and that are older than the cutoff.
   *
   * <p>What counts as reachable comes from real Iceberg code: {@link
   * org.apache.iceberg.ReachableFileUtil} for the manifest lists and statistics files, and the real
   * manifest readers for the content files. The listing and the age filter stand in for the Spark
   * action. Reads go through the table's own {@code FileIO}, so they park and other writers
   * interleave with the cleaner exactly as they would with a real one.
   *
   * <p>Metadata JSON objects are left alone: the fake catalog keeps its pointer in memory rather
   * than in a version-hint file, so metadata-file reachability is not modelled here.
   */
  private void orphanClean(Outcome outcome) {
    long now = sched.steps();
    Set<String> reachable = new LinkedHashSet<>();
    reachable.addAll(org.apache.iceberg.ReachableFileUtil.manifestListLocations(table));
    reachable.addAll(org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations(table));
    org.apache.iceberg.TableMetadata metadata =
        ((org.apache.iceberg.HasTableOperations) table).operations().current();
    for (Snapshot snapshot : metadata.snapshots()) {
      reachable.addAll(
          TableView.read(metadata, snapshot.snapshotId(), table.io()).everyFile());
    }
    int deleted = 0;
    int tooYoung = 0;
    for (String path : store.paths()) {
      if (FakeStore.kindOf(path).equals("meta") || reachable.contains(path)) {
        continue;
      }
      Long written = store.writeStep(path);
      if (written == null) {
        continue;
      }
      if (now - written < config.orphanCleanCutoffSteps) {
        tooYoung++;
        continue;
      }
      table.io().deleteFile(path);
      deleted++;
    }
    outcome.action = "orphan-clean(deleted=" + deleted + ", tooYoung=" + tooYoung + ")";
    outcome.orphansDeleted = deleted;
  }

  /**
   * Regroups the current snapshot's manifests without changing what the table contains.
   *
   * <p>Clustered by partition value, so the regrouping is real rather than a copy: entries move
   * between manifests and every one of them has to come out the other side with the same path and
   * the same data sequence number.
   */
  private void rewriteManifests(Outcome outcome) {
    org.apache.iceberg.RewriteManifests rewrite = table.rewriteManifests();
    rewrite.clusterBy(file -> file.partition().get(0, Object.class));
    controlled(rewrite).commit();
    // Which snapshot this produced, so I10 judges that one rather than every snapshot the same
    // writer landed. A writer doing several operations may rewrite manifests once and append next,
    // and an append is supposed to change the table's contents.
    outcome.manifestRewriteSnapshotId = ops.forFiber(sched.currentName());
  }

  /** Several operations committed as one transaction, through {@code BaseTransaction}. */
  private void transaction(String owner, int index, Outcome outcome) {
    org.apache.iceberg.Transaction txn = table.newTransaction();
    org.apache.iceberg.AppendFiles append = txn.newAppend();
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df = Payload.dataFile(payloadIo, spec, location, owner, i, partition(index + i));
      outcome.addedDataFiles.add(df.location());
      append.appendFile(df);
    }
    CommitUuid.makeDeterministic(append);
    FilterCacheProbe.register(append);
    SameThreadExecutor.inject(append).commit();

    org.apache.iceberg.RowDelta rowDelta = txn.newRowDelta();
    org.apache.iceberg.DeleteFile deletes =
        Payload.positionDeletes(payloadIo, spec, location, owner + "t", 0, partition(index));
    outcome.addedDeleteFiles.add(deletes.location());
    rowDelta.addDeletes(deletes);
    CommitUuid.makeDeterministic(rowDelta);
    FilterCacheProbe.register(rowDelta);
    SameThreadExecutor.inject(rowDelta).commit();

    txn.commitTransaction();
  }

  /**
   * Ref and retention changes through {@code ManageSnapshots}, for the A2-1 workload: each one
   * makes an old ancestor of main retained (or a ref head) when it lands. Unknown names throw.
   */
  private void manageSnapshots(String action, Outcome outcome) {
    outcome.usedManageSnapshots = true;
    outcome.expectsCommit = false; // a ref change adds no snapshot for I2 to find
    Snapshot head = table.currentSnapshot();
    long oldest = head.snapshotId();
    for (long id : TableView.ancestry(
        ((org.apache.iceberg.HasTableOperations) table).operations().current(),
        head.snapshotId())) {
      oldest = id; // ancestry is head-first, so the last one is the oldest still present
    }
    switch (action) {
      case "ms:create-keep-branch":
        table.manageSnapshots()
            .createBranch("keep", head.snapshotId())
            .setMinSnapshotsToKeep("keep", 100)
            .commit();
        break;
      case "ms:set-min-main":
        table.manageSnapshots().setMinSnapshotsToKeep("main", 100).commit();
        break;
      case "ms:replace-keep-to-head":
        table.manageSnapshots()
            .replaceBranch("keep", head.snapshotId())
            .setMinSnapshotsToKeep("keep", 100)
            .commit();
        break;
      case "ms:branch-at-oldest":
        table.manageSnapshots().createBranch("old", oldest).commit();
        break;
      case "ms:tag-oldest":
        table.manageSnapshots().createTag("t-old", oldest).commit();
        break;
      default:
        throw new IllegalArgumentException("unknown ManageSnapshots action " + action);
    }
    outcome.action = action + "(head=" + head.snapshotId() + ", oldest=" + oldest + ")";
  }

  /**
   * The round-2 {@code transaction} (append, then a row delta) in the shape format v3 accepts: the
   * position delete is a deletion vector, and it references the transaction's own first data file
   * so two concurrent transactions never put two DVs on one data file. Separate from
   * {@code transaction} so the existing sweeps keep their exact behaviour on v3.
   */
  private void transactionWithDv(String owner, int index, Outcome outcome) {
    org.apache.iceberg.Transaction txn = table.newTransaction();
    org.apache.iceberg.AppendFiles append = txn.newAppend();
    String first = null;
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df = Payload.dataFile(payloadIo, spec, location, owner, i, partition(index + i));
      outcome.addedDataFiles.add(df.location());
      append.appendFile(df);
      if (first == null) {
        first = df.location();
      }
    }
    CommitUuid.makeDeterministic(append);
    FilterCacheProbe.register(append);
    SameThreadExecutor.inject(append).commit();

    org.apache.iceberg.RowDelta rowDelta = txn.newRowDelta();
    DeleteFile dv =
        Payload.deletionVector(payloadIo, spec, location, owner + "t", 0, partition(index), first);
    outcome.addedDeleteFiles.add(dv.location());
    rowDelta.addDeletes(dv);
    CommitUuid.makeDeterministic(rowDelta);
    FilterCacheProbe.register(rowDelta);
    SameThreadExecutor.inject(rowDelta).commit();

    txn.commitTransaction();
  }

  /**
   * A1-29: one transaction holding an append and a clustered manifest rewrite. On a
   * {@code BaseTransaction} re-apply the rewrite may reuse the manifests it wrote in pass 1
   * (BaseRewriteManifests.java:206-219, :186), whose EXISTING entries carry explicit pass-1 data
   * sequence numbers.
   */
  private void transactionRewriteManifests(String owner, int index, Outcome outcome) {
    org.apache.iceberg.Transaction txn = table.newTransaction();
    org.apache.iceberg.AppendFiles append = txn.newAppend();
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df = Payload.dataFile(payloadIo, spec, location, owner, i, partition(index + i));
      outcome.addedDataFiles.add(df.location());
      append.appendFile(df);
    }
    CommitUuid.makeDeterministic(append);
    SameThreadExecutor.inject(append).commit();

    org.apache.iceberg.RewriteManifests rewrite = txn.rewriteManifests();
    rewrite.clusterBy(file -> file.partition().get(0, Object.class));
    CommitUuid.makeDeterministic(rewrite);
    FilterCacheProbe.register(append);
    FilterCacheProbe.register(rewrite);
    SameThreadExecutor.inject(rewrite).commit();

    txn.commitTransaction();
  }

  /** An append onto a branch rather than main. */
  private void branchAppend(String owner, int index, Outcome outcome) {
    String branch = BRANCHES[sched.draw("branch", BRANCHES.length)];
    org.apache.iceberg.AppendFiles append = table.newAppend();
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df = Payload.dataFile(payloadIo, spec, location, owner, i, partition(index + i));
      outcome.addedDataFiles.add(df.location());
      append.appendFile(df);
    }
    append.toBranch(branch);
    outcome.action = "branch-append(" + branch + ")";
    controlled(append).commit();
  }

  /** Stages a snapshot that moves no ref, then cherrypicks it onto main. */
  private void stageAndCherrypick(String owner, int index, Outcome outcome) {
    org.apache.iceberg.AppendFiles staged = table.newAppend();
    for (int i = 0; i < config.filesPerWriter; i++) {
      DataFile df = Payload.dataFile(payloadIo, spec, location, owner, i, partition(index + i));
      outcome.addedDataFiles.add(df.location());
      staged.appendFile(df);
    }
    staged.stageOnly();
    controlled(staged).commit();
    Long stagedId = ops.forFiber(sched.currentName());
    if (stagedId == null) {
      outcome.action = "stage-and-cherrypick(nothing staged)";
      return;
    }
    outcome.stagedSnapshotIds.add(stagedId);
    outcome.action = "stage-and-cherrypick(" + stagedId + ")";
    outcome.usedManageSnapshots = true;
    table.manageSnapshots().cherrypick(stagedId).commit();
  }

  /** Fast-forwards main to a branch head, if the branch exists and is ahead. */
  private void fastForward(Outcome outcome) {
    String branch = BRANCHES[sched.draw("branch", BRANCHES.length)];
    if (table.refs().get(branch) == null) {
      outcome.action = "fast-forward(no branch " + branch + ")";
      outcome.expectsCommit = false;
      return;
    }
    outcome.action = "fast-forward(main <- " + branch + ")";
    outcome.usedManageSnapshots = true;
    // Likewise a no-op when main is already at the branch head.
    outcome.expectsCommit = false;
    table.manageSnapshots().fastForwardBranch("main", branch).commit();
  }

  /** Rolls main back to its parent. */
  private void rollback(Outcome outcome) {
    Snapshot current = table.currentSnapshot();
    if (current == null || current.parentId() == null) {
      outcome.action = "rollback(nothing to roll back to)";
      outcome.expectsCommit = false;
      return;
    }
    long target = current.parentId();
    outcome.action = "rollback(" + target + ")";
    outcome.usedManageSnapshots = true;
    // Rolling back to the snapshot that is already current produces no metadata change, and
    // SnapshotProducer.commit() returns without calling the catalog when changes() is empty. Two
    // writers rolling back to the same target means the second is a no-op, which is correct.
    outcome.expectsCommit = false;
    table.manageSnapshots().rollbackTo(target).commit();
  }

  private void deleteAll() {
    org.apache.iceberg.DeleteFiles delete = table.newDelete();
    delete.deleteFromRowFilter(Expressions.alwaysTrue());
    CommitUuid.makeDeterministic(delete);
    SameThreadExecutor.inject(delete).commit();
  }

  private void rowDelta(String fiber, int index, Outcome outcome) {
    org.apache.iceberg.RowDelta rowDelta = table.newRowDelta();
    String part = partition(index);
    List<DeleteFile> deletes = new ArrayList<>();
    boolean equality = sched.draw("rowdelta.equality", 2) == 1;
    DeleteFile df;
    if (equality) {
      df = Payload.equalityDeletes(payloadIo, spec, location, fiber, 0, part, 1);
    } else if (config.formatVersion >= 3) {
      // v3 accepts only deletion vectors for position deletes, and a DV references exactly one
      // data file. Point it at the seed file in this partition, which is always present.
      String referenced = null;
      for (DataFile seed : seedFiles) {
        if (seed.location().contains("data=" + part)) {
          referenced = seed.location();
          break;
        }
      }
      if (referenced == null) {
        referenced = seedFiles.get(0).location();
      }
      df = Payload.deletionVector(payloadIo, spec, location, fiber, 0, part, referenced);
      outcome.action = "row-delta(dv)";
    } else {
      df = Payload.positionDeletes(payloadIo, spec, location, fiber, 0, part);
    }
    deletes.add(df);
    outcome.addedDeleteFiles.add(df.location());
    if (outcome.action == null || !outcome.action.equals("row-delta(dv)")) {
      outcome.action = equality ? "row-delta(eq)" : "row-delta(pos)";
    }
    for (DeleteFile d : deletes) {
      rowDelta.addDeletes(d);
    }
    if (sched.draw("rowdelta.validate", 2) == 1) {
      Snapshot current = table.currentSnapshot();
      if (current != null) {
        rowDelta.validateFromSnapshot(current.snapshotId());
      }
      rowDelta.validateDeletedFiles();
      rowDelta.validateNoConflictingDataFiles();
      rowDelta.validateNoConflictingDeleteFiles();
      outcome.validated = true;
    }
    controlled(rowDelta).commit();
  }
}
