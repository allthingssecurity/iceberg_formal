package org.apache.iceberg.dst;

/**
 * What the fakes are allowed to do, and which historical behaviour to put back.
 *
 * <p>Percentages are per boundary crossing and are drawn from the seed. The defaults describe a
 * store with atomic single-shot puts (S3-like) behind a catalog with an atomic compare-and-set
 * (REST, Hive, Glue, Nessie). The stronger faults are off by default and each one is named after
 * the real store property it needs, so a reader can tell which findings depend on which assumption.
 */
public final class FaultConfig {

  // ---- object store faults -------------------------------------------------

  /** The create() call throws before any byte is stored. */
  public int writeFailBeforeBytesPct = 4;

  /**
   * A prefix of the bytes is stored and close() throws. Needs a store that is not single-shot
   * atomic: HDFS, NFS, a local filesystem, or an S3 multipart upload that is completed in parts.
   */
  public int writeTruncateAndThrowPct = 4;

  /** Every byte is stored and close() throws: the write is an orphan the writer does not know of. */
  public int writeLostAckPct = 4;

  /**
   * A prefix of the bytes is stored and close() returns normally. This is the only fault that can
   * break the "no reachable file is truncated" invariant on its own, and it is the strongest
   * assumption in the harness: it needs a store that can lose the tail of a closed write without
   * reporting it. Off by default; turn it on to exercise that invariant.
   */
  public int writeSilentTruncatePct = 0;

  /**
   * A read of a manifest list ({@code newInputFile(path).newStream()}) throws this often. Off by
   * default, and drawn only for reads inside {@link #readFailScope}, so with the fault off the
   * schedule and the RNG stream are exactly what they were before the fault existed.
   *
   * <p>Added in r5 for A2-9 (IncrementalFileCleanup swallows a failed read of a retained snapshot's
   * manifest list). A real object store answers a read with a 503, a throttle or a reset
   * connection; Iceberg's cleanup retries three times with 1 s, 2 s and 4 s back-off, so a fault
   * that is to survive the retries must persist for about seven seconds, and at 100% it does.
   */
  public int readFailManifestListPct = 0;

  /** Which manifest-list reads {@link #readFailManifestListPct} may hit. */
  public ReadFaultScope readFailScope = ReadFaultScope.CLEANUP_RETAINED;

  public enum ReadFaultScope {
    /** Every manifest-list read by a managed fiber, wherever it comes from. */
    ANY,
    /**
     * Only reads made from inside a {@code FileCleanupStrategy} (IncrementalFileCleanup or
     * ReachableFileCleanup, decided by the call stack), and only of the manifest list of a snapshot
     * that is still in the catalog's current metadata: the snapshots the expiry retains. That is
     * the first loop of IncrementalFileCleanup (IncrementalFileCleanup.java:120-160) and
     * ReachableFileCleanup.pruneReferencedManifests.
     */
    CLEANUP_RETAINED,
    /** The other half: cleanup reads of the manifest list of a snapshot the expiry removed. */
    CLEANUP_EXPIRED;

    /** Unknown names throw: a silent fall-through to a default would make a control look like data. */
    public static ReadFaultScope parse(String name) {
      for (ReadFaultScope value : values()) {
        if (value.name().equalsIgnoreCase(name.replace('-', '_'))) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "unknown read-fault scope '" + name + "', expected one of "
              + java.util.Arrays.toString(values()));
    }
  }

  /** Deleting a file that is not there is a no-op; deleting throws this often. */
  public int deleteFailPct = 2;

  // ---- catalog faults ------------------------------------------------------

  /** The catalog rejects the commit although the pointer was not stale. */
  public int commitRejectPct = 6;

  /** The commit lands and the caller is told the outcome is unknown. */
  public int commitUnknownAfterLandingPct = 6;

  /** The commit does not land and the caller is told the outcome is unknown. */
  public int commitUnknownWithoutLandingPct = 4;

  /**
   * refresh() returns the previous metadata instead of the current one. Needs a catalog whose reads
   * are eventually consistent; a REST or Hive catalog is not one, so this is off by default.
   */
  public int refreshStalePct = 0;

  /** What {@code TableOperations.requireStrictCleanup()} answers. */
  public boolean strictCleanup = true;

  // ---- workload ------------------------------------------------------------

  public int writers = 3;
  public int formatVersion = 2;
  public int filesPerWriter = 2;

  /**
   * How many operations each writer performs. More than one makes the schedules deeper, so a
   * writer's retry and cleanup paths compose with another writer's instead of each seed being one
   * operation per writer and done.
   */
  public int operationsPerWriter = 1;

  /**
   * How many appends to seed the table with before the writers start. More than one gives the table
   * a history, so a rewrite that mirrors {@code RewriteDataFilesCommitManager} — supplying the
   * starting snapshot's sequence number — ends up above the data sequence number of the files it
   * replaces, which is the normal case on any table that has been written to for a while.
   */
  public int seedSnapshots = 1;

  /**
   * Seeds the table with a manifest that holds the same data file twice, and makes every writer
   * delete everything, so the writers race and the loser retries with nothing left to delete.
   *
   * <p>This is the reachable half of apache/iceberg#16686 ("Core: Fix thread conflict when deleting
   * duplicate files in manifest", merged 2026-07-03, so 1.11.0 is pre-fix and 1.12.0 is post-fix):
   * before the fix, {@code ManifestFilterManager.duplicateDeleteCount} was a field on the manager
   * rather than a per-manifest value, so a commit retry counted the same duplicate again and the
   * snapshot summary reported duplicate deletes for files that attempt had not deleted. The other
   * half of that PR is the thread-safety of the same two members under a parallel worker pool,
   * which this harness cannot reach because it replaces that pool with a same-thread executor.
   */
  public boolean duplicateDeleteWorkload = false;

  /**
   * Runs one writer as a snapshot-expiry job while the others keep committing.
   *
   * <p>This is the operation whose whole purpose is to delete files that something referenced
   * recently, so it is the one place where a delete is legal and the guard that makes it legal is
   * subtle. {@code RemoveSnapshots.commit()} moves the pointer first and only then runs
   * {@code cleanExpiredSnapshots()}, which takes a fresh {@code ops.refresh()} and diffs it against
   * the metadata the expiry started from — so a writer that commits in between is supposed to be
   * accounted for. The sweep is what says whether it is.
   */
  public boolean expireWorkload = false;

  /**
   * When positive, the expiry job expires by policy instead of by id:
   * {@code expireOlderThan(Long.MAX_VALUE).retainLast(n)}. That is time-independent (every snapshot
   * is older than Long.MAX_VALUE, so only {@code retainLast} and the refs decide), and it is the
   * shape that lets Iceberg pick IncrementalFileCleanup: an expiry by id always uses
   * ReachableFileCleanup (RemoveSnapshots.java:394-397). 0 keeps the by-id expiry of round 2.
   */
  public int expireRetainLast = 0;

  /**
   * Which cleanup strategy the expiry is told to use. {@code null} lets Iceberg decide (the
   * default path); {@code false} forces ReachableFileCleanup and {@code true} forces
   * IncrementalFileCleanup, through the package-private {@code RemoveSnapshots
   * .withIncrementalCleanup(boolean)} (RemoveSnapshots.java:380), reached by reflection.
   */
  public Boolean expireIncrementalCleanup = null;

  /**
   * Snapshot expiry by policy, concurrent with main-only writers, with a manifest-list read fault
   * confined to the cleanup's reads of retained snapshots. {@code strategy} is {@code default},
   * {@code reachable} or {@code incremental}; anything else throws.
   */
  public static FaultConfig expiryReadFault(String strategy, int pct) {
    FaultConfig c = FaultConfig.quiet();
    c.expireWorkload = true;
    c.seedSnapshots = 4;
    c.writers = 3;
    c.expireRetainLast = 1;
    c.readFailManifestListPct = pct;
    c.readFailScope = ReadFaultScope.CLEANUP_RETAINED;
    switch (strategy) {
      case "default":
        c.expireIncrementalCleanup = null;
        break;
      case "reachable":
        c.expireIncrementalCleanup = Boolean.FALSE;
        break;
      case "incremental":
        c.expireIncrementalCleanup = Boolean.TRUE;
        break;
      default:
        throw new IllegalArgumentException(
            "unknown cleanup strategy '" + strategy + "', expected default|reachable|incremental");
    }
    return c;
  }

  /**
   * Runs one writer as an orphan-file cleaner while the others keep committing.
   *
   * <p>The reachability half is real Iceberg code ({@code ReachableFileUtil} plus the real manifest
   * readers); the directory listing and the age cutoff stand in for what the Spark action supplies,
   * because {@code DeleteOrphanFilesSparkAction} is not in {@code iceberg-core}. The question the
   * sweep answers is about the writers, not the cleaner: a writer writes a manifest well before it
   * commits the snapshot that references it, and in that window the file is unreachable. Nothing in
   * the commit protocol tells a cleaner to leave it alone — the only protection is the age cutoff,
   * which defaults to three days in Spark. This measures how large it has to be.
   */
  public boolean orphanCleanWorkload = false;

  /**
   * Adds manifest rewriting to the action mix.
   *
   * <p>{@code BaseRewriteManifests} regroups the entries of the current snapshot into new manifests
   * without changing what the table contains. Iceberg checks that the active file <em>count</em>
   * matches ({@code validateFilesCounts}); it does not check that the files are the same ones or
   * that they kept their data sequence numbers. That is the gap apache/iceberg#3842 fell into, and
   * it is what I10 states.
   */
  public boolean rewriteManifestsWorkload = false;

  /**
   * Puts transactions, branch commits, cherrypick, fast-forward and rollback into the mix.
   *
   * <p>These are the parts of the commit surface the first rounds never touched, and they are much
   * less exercised than a plain append: {@code BaseTransaction} has its own retry and cleanup, and
   * the ref operations rewrite what a ref can reach while other writers are committing.
   */
  public boolean refActions = false;

  /** Narrowing switch: only appends and transactions, nothing else. */
  public boolean transactionsOnly = false;

  /** Manifest rewriting concurrent with ordinary writers. */
  public static FaultConfig manifests() {
    FaultConfig c = new FaultConfig();
    c.rewriteManifestsWorkload = true;
    c.seedSnapshots = 3;
    return c;
  }

  /** The same, with no injected faults. */
  public static FaultConfig manifestsQuiet() {
    FaultConfig c = FaultConfig.quiet();
    c.rewriteManifestsWorkload = true;
    c.seedSnapshots = 3;
    return c;
  }

  /** How old, in scheduler steps, a file must be before the cleaner will remove it. */
  public long orphanCleanCutoffSteps = 0;

  /** Orphan cleaning concurrent with writers, at a given cutoff. */
  public static FaultConfig orphanClean(long cutoffSteps) {
    FaultConfig c = FaultConfig.quiet();
    c.orphanCleanWorkload = true;
    c.orphanCleanCutoffSteps = cutoffSteps;
    c.seedSnapshots = 2;
    c.writers = 3;
    return c;
  }

  /** Snapshot expiry concurrent with ordinary writers. */
  public static FaultConfig expiry() {
    FaultConfig c = new FaultConfig();
    c.expireWorkload = true;
    c.seedSnapshots = 4;
    c.writers = 3;
    return c;
  }

  /** The same, with no injected faults: concurrency alone. */
  public static FaultConfig expiryQuiet() {
    FaultConfig c = FaultConfig.quiet();
    c.expireWorkload = true;
    c.seedSnapshots = 4;
    c.writers = 3;
    return c;
  }

  /** A duplicate entry plus concurrent deletes: the #16686 recall row. */
  public static FaultConfig duplicateDeletes() {
    FaultConfig c = FaultConfig.quiet();
    c.duplicateDeleteWorkload = true;
    c.writers = 3;
    return c;
  }

  /**
   * Makes the rewrite writer pass a data sequence number <em>ahead</em> of the table's to
   * {@code RewriteFiles.rewriteFiles(replaced, added, sequenceNumber)}.
   *
   * <p>This is the unguarded path round 1 found (findings/r1-code-hunt.md, M1):
   * {@code MergingSnapshotProducer.setNewDataFilesDataSequenceNumber} checks nothing about the
   * number it is given, and supplying one switches off equality-delete conflict detection. A
   * compaction job that computes the number from stale metadata therefore both skips the check and
   * out-ranks the deletes. Off by default, because the default workload is meant to be a faithful
   * caller.
   */
  public boolean sequenceAheadRewrite = false;

  /** Upper bound on scheduler steps; a run past it is reported as a runaway, not left to hang. */
  public long stepBudget = 200_000;

  // ---- pre-fix behaviour, for the recall scorecard -------------------------

  /**
   * Put back the pre-fix behaviour of a known bug. Each value is wired into the fakes or into a
   * subclass of the real producer, and the scorecard records how many seeds fail with it on.
   */
  public PreFix preFix = PreFix.NONE;

  public enum PreFix {
    NONE,

    /**
     * The catalog reports a commit that landed as an ordinary {@code CommitFailedException} instead
     * of {@code CommitStateUnknownException}. That is what a client saw before the unknown-state
     * answer was introduced and propagated through the writers (apache/iceberg#4687, #4719, #4836,
     * #4893, and the cleanup rules in #8397 and #10373). The producer then retries a payload that
     * is already committed, and on giving up cleans up files the committed snapshot references.
     */
    RETRY_ON_UNKNOWN_STATE,

    /**
     * Pre-apache/iceberg#4747 (merged 2022-06-29, released in 0.14.0): snapshot ids were drawn by
     * folding a random UUID into 64 bits and nothing checked for a collision, so two uncommitted
     * operations that drew the same id lost data. The fake hands out a duplicate on purpose; the
     * guard that must reject it lives in {@code TableMetadata}.
     */
    DUPLICATE_SNAPSHOT_ID,

    /**
     * A rewrite that supplies no starting sequence number at all, which is what every engine did
     * before apache/iceberg#3480 added the parameter (and what Flink still did until #7218 made it
     * the default). The replacement files then inherit a fresh, higher number, so an equality
     * delete committed concurrently would stop applying to the rewritten rows. Current Iceberg
     * answers this by keeping equality-delete conflict detection on when no number is supplied, so
     * the expected outcome is a rejected commit rather than resurrected rows.
     */
    REWRITE_WITHOUT_SEQUENCE_NUMBER,
  }

  /**
   * Breaks the harness on purpose, so the invariants can be shown to notice. If a mode here does
   * not produce the violation it is supposed to, the detector is broken and every clean sweep in
   * the findings is worth nothing. This is the answer to the Camel harness's worst failure, where
   * a fake put its lock times in 1970 and quietly reported 0 of 1000 seeds failing.
   */
  public SelfCheck selfCheck = SelfCheck.OFF;

  public enum SelfCheck {
    OFF,
    /** The catalog pointer accepts any commit, stale or not: a lost update. */
    NON_ATOMIC_POINTER,
    /** One manifest of a committed snapshot is deleted after the run. */
    DELETE_A_COMMITTED_MANIFEST,
    /** The store drops a committed data file. */
    DROP_A_COMMITTED_DATA_FILE,
    /**
     * The expiry job deletes one manifest too many: alongside the files {@code RemoveSnapshots}
     * asked for, it also removes a manifest of the snapshot that is still the head of main. That is
     * the failure mode an expiry actually has — getting the reachability computation wrong — so if
     * the retention-aware I5 cannot see it, a clean expiry sweep means nothing.
     */
    EXPIRY_OVER_DELETES,
  }

  public FaultConfig selfCheck(SelfCheck value) {
    this.selfCheck = value;
    return this;
  }

  public FaultConfig writers(int n) {
    this.writers = n;
    return this;
  }

  public FaultConfig preFix(PreFix value) {
    this.preFix = value;
    return this;
  }

  /** No faults at all: used by the self-check, where every invariant must hold trivially. */
  public static FaultConfig quiet() {
    FaultConfig c = new FaultConfig();
    c.writeFailBeforeBytesPct = 0;
    c.writeTruncateAndThrowPct = 0;
    c.writeLostAckPct = 0;
    c.writeSilentTruncatePct = 0;
    c.deleteFailPct = 0;
    c.commitRejectPct = 0;
    c.commitUnknownAfterLandingPct = 0;
    c.commitUnknownWithoutLandingPct = 0;
    c.refreshStalePct = 0;
    return c;
  }

  /**
   * A configuration drawn from the seed: swarm testing.
   *
   * <p>Every sweep so far held the configuration fixed and varied only the schedule, which explores
   * one shape of run very thoroughly and every other shape not at all. Swarm testing randomises the
   * configuration itself per seed — how many writers, how deep, which operations are in the mix,
   * which faults are switched on and how hard — and is the single biggest lever in the
   * deterministic-simulation literature for finding something new. The configuration is a function
   * of the seed, so a failing seed still replays exactly.
   */
  public static FaultConfig swarm(long seed) {
    // Mixed so the configuration draw is not correlated with the scheduler's own stream.
    java.util.Random rng = new java.util.Random(Long.rotateLeft(seed, 32) ^ 0x5DEECE66DL);
    FaultConfig c = new FaultConfig();
    c.writers = 2 + rng.nextInt(4);
    c.operationsPerWriter = 1 + rng.nextInt(4);
    c.filesPerWriter = 1 + rng.nextInt(3);
    c.seedSnapshots = 1 + rng.nextInt(5);
    c.formatVersion = rng.nextInt(4) == 0 ? 3 : 2;

    // Faults: sometimes none at all, sometimes harsh. A profile that is always "a bit faulty"
    // never explores either end.
    int severity = rng.nextInt(4);
    int cap = severity == 0 ? 0 : 3 * severity;
    c.writeFailBeforeBytesPct = cap == 0 ? 0 : rng.nextInt(cap + 1);
    c.writeTruncateAndThrowPct = cap == 0 ? 0 : rng.nextInt(cap + 1);
    c.writeLostAckPct = cap == 0 ? 0 : rng.nextInt(cap + 1);
    c.writeSilentTruncatePct = 0;
    c.deleteFailPct = cap == 0 ? 0 : rng.nextInt(3);
    c.commitRejectPct = cap == 0 ? 0 : rng.nextInt(cap + 1);
    c.commitUnknownAfterLandingPct = cap == 0 ? 0 : rng.nextInt(cap + 1);
    c.commitUnknownWithoutLandingPct = cap == 0 ? 0 : rng.nextInt(cap + 1);
    c.refreshStalePct = 0;
    c.strictCleanup = rng.nextBoolean();

    // Which of the bigger operations join the mix.
    c.rewriteManifestsWorkload = rng.nextInt(3) == 0;
    c.refActions = rng.nextInt(3) != 0;
    c.expireWorkload = rng.nextInt(4) == 0;
    // The orphan cleaner is deliberately NOT in the swarm. Its unsafe window is already measured
    // (section 5 of the findings) and it is a property of the cleaner's cutoff, not of Iceberg, so
    // including it here only manufactures violations that are already understood and drowns
    // anything new. The dedicated orphanN modes keep it.
    if (c.expireWorkload) {
      c.seedSnapshots = Math.max(c.seedSnapshots, 3);
      c.writers = Math.max(c.writers, 3);
    }
    return c;
  }

  /** A table with a history before the writers start. */
  public static FaultConfig deepHistory() {
    FaultConfig c = new FaultConfig();
    c.seedSnapshots = 4;
    return c;
  }

  /** Everything the harness can do, including the assumptions a plain S3 store does not allow. */
  public static FaultConfig aggressive() {
    FaultConfig c = new FaultConfig();
    c.writeSilentTruncatePct = 3;
    c.refreshStalePct = 5;
    return c;
  }

  @Override
  public String toString() {
    return "FaultConfig{writers="
        + writers
        + ", v"
        + formatVersion
        + ", failBefore="
        + writeFailBeforeBytesPct
        + ", truncThrow="
        + writeTruncateAndThrowPct
        + ", lostAck="
        + writeLostAckPct
        + ", silentTrunc="
        + writeSilentTruncatePct
        + ", reject="
        + commitRejectPct
        + ", unknownAfter="
        + commitUnknownAfterLandingPct
        + ", unknownWithout="
        + commitUnknownWithoutLandingPct
        + ", staleRefresh="
        + refreshStalePct
        + ", strictCleanup="
        + strictCleanup
        + (sequenceAheadRewrite ? ", sequenceAheadRewrite" : "")
        + (duplicateDeleteWorkload ? ", duplicateDeleteWorkload" : "")
        + (expireWorkload ? ", expireWorkload(seedSnapshots=" + seedSnapshots + ")" : "")
        + (orphanCleanWorkload ? ", orphanClean(cutoffSteps=" + orphanCleanCutoffSteps + ")" : "")
        + (rewriteManifestsWorkload ? ", rewriteManifestsWorkload" : "")
        + (refActions ? ", refActions" : "")
        + (expireRetainLast > 0 ? ", expireRetainLast=" + expireRetainLast : "")
        + (expireIncrementalCleanup != null ? ", incrementalCleanup=" + expireIncrementalCleanup : "")
        + (readFailManifestListPct > 0
            ? ", readFailMlist=" + readFailManifestListPct + "%@" + readFailScope
            : "")
        + ", preFix="
        + preFix
        + (selfCheck == SelfCheck.OFF ? "" : ", selfCheck=" + selfCheck)
        + "}";
  }
}
