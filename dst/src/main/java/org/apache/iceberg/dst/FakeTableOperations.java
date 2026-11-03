package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * A catalog whose {@code commit} is an atomic compare-and-set on one in-memory pointer, which is
 * what a REST, Hive, Glue or Nessie catalog gives you. The whole metadata history is kept so the
 * invariants can walk snapshot ancestry after the run.
 *
 * <p>The sequence is the one a real client performs against such a catalog: write the new metadata
 * object, then ask the catalog to move the pointer only if it still points where the writer thinks.
 * Another writer may land in between, and then the compare-and-set rejects: that is the real race,
 * and the scheduler can place it anywhere.
 *
 * <p>No lock is ever held across a scheduler park. The pointer swap happens inside a short block
 * that cannot park, which is what makes it atomic; everything that can park is outside it. Holding
 * a monitor across a park deadlocks the whole run, and that mistake is worth stating because the
 * first version of this class made it.
 *
 * <p>The three faults are the answers a writer cannot tell apart by itself: rejected,
 * landed-but-unknown, and not-landed-but-unknown.
 */
public final class FakeTableOperations implements TableOperations {

  /** One attempt at the pointer, in scheduler order. */
  public static final class CommitRecord {
    public final String fiber;
    public final long snapshotId;
    public final boolean landed;
    public final String answer;
    /** The pointer version this commit produced, or -1 if it did not land. */
    public long pointerVersion = -1;

    CommitRecord(String fiber, long snapshotId, boolean landed, String answer) {
      this.fiber = fiber;
      this.snapshotId = snapshotId;
      this.landed = landed;
      this.answer = answer;
    }

    @Override
    public String toString() {
      return fiber + " snap=" + snapshotId + " landed=" + landed + " answer=" + answer;
    }
  }

  private final FakeStore store;
  private final Scheduler sched;
  private final FaultConfig config;
  private final FakeFileIO io;
  private final FakeFileIO observerIo;
  private final String location;

  /** Guards the pointer and the ledgers. Never held while a fiber can park. */
  private final Object state = new Object();

  private volatile TableMetadata current;
  private volatile TableMetadata previous;
  private final List<TableMetadata> history = new ArrayList<>();
  /** Which fiber landed each entry of {@link #history}, index for index. */
  private final List<String> historyFibers = new ArrayList<>();
  private final List<CommitRecord> commits = new ArrayList<>();
  private final Map<Long, String> landedBy = new LinkedHashMap<>();
  private final Map<Long, TableView.SnapshotFiles> capturedAtCommit = new LinkedHashMap<>();
  private final AtomicLong nextSnapshotId = new AtomicLong(1_000L);
  private final AtomicLong duplicateIdsHandedOut = new AtomicLong();
  private final AtomicLong pointerVersion = new AtomicLong();
  private final Map<Long, Set<String>> reachableFilesAtVersion = new LinkedHashMap<>();
  private final Map<Long, Set<Long>> reachableSnapshotsAtVersion = new LinkedHashMap<>();
  private final Set<Long> incompleteVersions = new LinkedHashSet<>();
  private final AtomicLong metadataVersion = new AtomicLong();
  private volatile boolean bootstrapping;

  public FakeTableOperations(
      FakeStore store, Scheduler sched, FaultConfig config, FakeFileIO io, String location) {
    this.store = store;
    this.sched = sched;
    this.config = config;
    this.io = io;
    this.observerIo = io.observerView();
    this.location = location;
  }

  /** Lands the first metadata with no park and no fault; used only to create the table. */
  public void bootstrap(TableMetadata metadata) {
    this.bootstrapping = true;
    try {
      writeMetadata(metadata);
      this.previous = null;
      this.current = metadata;
      synchronized (state) {
        history.add(metadata);
        historyFibers.add("bootstrap");
      }
      captureReachableFiles(metadata);
    } finally {
      this.bootstrapping = false;
    }
  }

  @Override
  public TableMetadata current() {
    // A local cached read in every real implementation, so not a boundary.
    return current;
  }

  /**
   * Serialising the metadata and re-parsing it on every {@code refresh()}, the way a real catalog
   * does, is <em>not</em> possible while the compare-and-set below is an identity check: a re-parsed
   * copy is never {@code ==} the pointer, so every commit would be rejected as stale and nothing
   * would ever land. Doing it needs a content- or version-stamped compare-and-set first — which is
   * also closer to the etag a REST catalog uses — and that is the blocker on confirming the
   * candidate finding in section 6, because the in-memory metadata object is the one thing that
   * finding does not yet exclude.
   */
  @Override
  public TableMetadata refresh() {
    sched.point("refresh");
    FilterCacheProbe.invalidateAll();
    TableMetadata stale = previous;
    if (sched.armed()
        && config.refreshStalePct > 0
        && stale != null
        && sched.draw("refresh.stale", 100) < config.refreshStalePct) {
      return stale;
    }
    return current;
  }

  /**
   * The snapshots this metadata adds relative to its base.
   *
   * <p>Not {@code metadata.currentSnapshot()}: a {@code stageOnly()} commit adds a snapshot without
   * moving any ref, and a commit {@code toBranch} moves that branch rather than main, so in both
   * cases the current snapshot is somebody else's. Attributing the commit to the wrong snapshot id
   * quietly breaks I2, I3 and I10, which is why this is computed by difference.
   */
  private static List<Long> addedSnapshots(TableMetadata base, TableMetadata metadata) {
    Set<Long> before = new LinkedHashSet<>();
    if (base != null) {
      for (org.apache.iceberg.Snapshot snapshot : base.snapshots()) {
        before.add(snapshot.snapshotId());
      }
    }
    List<Long> added = new ArrayList<>();
    for (org.apache.iceberg.Snapshot snapshot : metadata.snapshots()) {
      if (!before.contains(snapshot.snapshotId())) {
        added.add(snapshot.snapshotId());
      }
    }
    return added;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    String fiber = sched.currentName();
    List<Long> added = addedSnapshots(base, metadata);
    long snapshotId = added.isEmpty() ? -1L : added.get(added.size() - 1);

    sched.point("commit");

    int roll = sched.armed() ? sched.draw("commit.fault", 100) : 100;
    int at = config.commitRejectPct;
    if (roll < at) {
      record(new CommitRecord(fiber, snapshotId, false, "rejected"));
      throw new CommitFailedException("fake catalog: rejected the commit of %s", fiber);
    }
    at += config.commitUnknownWithoutLandingPct;
    if (roll < at) {
      record(new CommitRecord(fiber, snapshotId, false, "unknown-not-landed"));
      throw new CommitStateUnknownException(
          new RuntimeException("fake catalog: answer lost before the pointer moved"));
    }
    boolean unknownAfterLanding = roll < at + config.commitUnknownAfterLandingPct;

    // Write the new metadata object first. This can park and can fail; the pointer has not moved,
    // so a failure here leaves the table exactly as it was.
    writeMetadata(metadata);

    // The pointer swap. Atomic by construction: no park inside.
    synchronized (state) {
      if (base != current && config.selfCheck != FaultConfig.SelfCheck.NON_ATOMIC_POINTER) {
        commits.add(new CommitRecord(fiber, snapshotId, false, "stale"));
        throw new CommitFailedException(
            "fake catalog: pointer moved while %s was committing", fiber);
      }
      this.previous = this.current;
      this.current = metadata;
      history.add(metadata);
      historyFibers.add(fiber);
      for (long id : added) {
        landedBy.put(id, fiber);
      }
      pointerVersion.incrementAndGet();
    }

    for (long id : added) {
      // What this snapshot pointed at the instant it became visible, read through an observer view
      // that neither parks nor faults. Nothing may remove a file from this set afterwards: that is
      // the "a snapshot never changes" invariant.
      TableView.SnapshotFiles captured = TableView.read(metadata, id, observerIo);
      synchronized (state) {
        capturedAtCommit.put(id, captured);
      }
    }
    captureReachableFiles(metadata);

    if (unknownAfterLanding) {
      if (config.preFix == FaultConfig.PreFix.RETRY_ON_UNKNOWN_STATE) {
        // Pre-fix: a lost answer was reported as an ordinary commit failure, so the producer
        // retried and applied the same payload a second time.
        record(new CommitRecord(fiber, snapshotId, true, "landed-reported-failed"));
        throw new CommitFailedException(
            "fake catalog (pre-fix): answer lost after the pointer moved, reported as a failure");
      }
      record(new CommitRecord(fiber, snapshotId, true, "unknown-landed"));
      throw new CommitStateUnknownException(
          new RuntimeException("fake catalog: answer lost after the pointer moved"));
    }

    CommitRecord ok = new CommitRecord(fiber, snapshotId, true, "ok");
    ok.pointerVersion = pointerVersion.get();
    record(ok);
  }

  /**
   * Everything reachable from any ref once this metadata is the pointer.
   *
   * <p>This is the timeline a delete is judged against: a file may be deleted only if it is
   * unreachable from the pointer at the moment of the delete and stays unreachable. Snapshot expiry
   * is what moves a file out of this set, so without a per-version record there is no way to tell a
   * legal expiry delete from data loss.
   */
  private void captureReachableFiles(TableMetadata metadata) {
    long version = pointerVersion.get();
    Set<String> reachable = new LinkedHashSet<>();
    Set<Long> reachableSnapshots = TableView.reachable(metadata);
    boolean complete = true;
    for (long id : reachableSnapshots) {
      TableView.SnapshotFiles files = TableView.read(metadata, id, observerIo);
      if (files.unreadable != null) {
        complete = false;
      }
      reachable.addAll(files.everyFile());
    }
    synchronized (state) {
      reachableFilesAtVersion.put(version, reachable);
      reachableSnapshotsAtVersion.put(version, reachableSnapshots);
      if (!complete) {
        incompleteVersions.add(version);
      }
    }
  }

  private void record(CommitRecord commitRecord) {
    synchronized (state) {
      commits.add(commitRecord);
    }
  }

  private void writeMetadata(TableMetadata metadata) {
    String path =
        metadataFileLocation("v" + metadataVersion.incrementAndGet() + ".metadata.json");
    if (bootstrapping || !sched.armed()) {
      store.put(
          path,
          TableMetadataParser.toJson(metadata)
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      return;
    }
    TableMetadataParser.overwrite(metadata, io.newOutputFile(path));
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return location + "/metadata/" + fileName;
  }

  @Override
  public LocationProvider locationProvider() {
    TableMetadata metadata = current;
    return LocationProviders.locationsFor(
        location, metadata == null ? java.util.Collections.emptyMap() : metadata.properties());
  }

  @Override
  public long newSnapshotId() {
    if (config.preFix == FaultConfig.PreFix.DUPLICATE_SNAPSHOT_ID && sched.armed()) {
      TableMetadata metadata = current;
      if (metadata != null
          && metadata.currentSnapshot() != null
          && sched.draw("snapshotid.duplicate", 100) < 50) {
        // The collision apache/iceberg#4747 fixed. Current Iceberg must reject the metadata this
        // produces; if it does not, I2 and I4 have something to say.
        duplicateIdsHandedOut.incrementAndGet();
        return metadata.currentSnapshot().snapshotId();
      }
    }
    // Iceberg's default is random, which would make paths and ids differ between two runs of the
    // same seed. A counter keeps the whole run reproducible.
    return nextSnapshotId.getAndIncrement();
  }

  @Override
  public boolean requireStrictCleanup() {
    return config.strictCleanup;
  }

  public long pointerVersion() {
    return pointerVersion.get();
  }

  public Map<Long, Set<String>> reachableFilesAtVersion() {
    synchronized (state) {
      return new LinkedHashMap<>(reachableFilesAtVersion);
    }
  }

  public Map<Long, Set<Long>> reachableSnapshotsAtVersion() {
    synchronized (state) {
      return new LinkedHashMap<>(reachableSnapshotsAtVersion);
    }
  }

  /** Whether any snapshot ever stopped being reachable: an expiry, a rollback, a branch replace. */
  public boolean anySnapshotEverRemoved() {
    synchronized (state) {
      Set<Long> seen = new LinkedHashSet<>();
      for (Set<Long> reachable : reachableSnapshotsAtVersion.values()) {
        for (long id : seen) {
          if (!reachable.contains(id)) {
            return true;
          }
        }
        seen.addAll(reachable);
      }
      return false;
    }
  }

  /** Versions whose reachable set could not be read in full; I5 must not judge against those. */
  public Set<Long> incompleteVersions() {
    synchronized (state) {
      return new LinkedHashSet<>(incompleteVersions);
    }
  }

  public long duplicateIdsHandedOut() {
    return duplicateIdsHandedOut.get();
  }

  public List<TableMetadata> history() {
    synchronized (state) {
      return new ArrayList<>(history);
    }
  }

  /** The fiber that landed each entry of {@link #history()}, index for index. */
  public List<String> historyFibers() {
    synchronized (state) {
      return new ArrayList<>(historyFibers);
    }
  }

  public List<CommitRecord> commits() {
    synchronized (state) {
      return new ArrayList<>(commits);
    }
  }

  public Map<Long, TableView.SnapshotFiles> capturedAtCommit() {
    synchronized (state) {
      return new LinkedHashMap<>(capturedAtCommit);
    }
  }

  /** The last snapshot this fiber got accepted, or null. Safe to read right after a commit. */
  public Long lastLandedFor(String fiber) {
    synchronized (state) {
      Long last = null;
      for (Map.Entry<Long, String> entry : landedBy.entrySet()) {
        if (entry.getValue().equals(fiber)) {
          last = entry.getKey();
        }
      }
      return last;
    }
  }

  public Map<Long, String> landedBy() {
    synchronized (state) {
      return new LinkedHashMap<>(landedBy);
    }
  }
}
