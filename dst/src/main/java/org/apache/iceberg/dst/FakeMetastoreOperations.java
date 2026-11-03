package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;

/**
 * A catalog built on Iceberg's real {@link BaseMetastoreTableOperations}, which is what the REST,
 * Hive and Glue catalogs extend.
 *
 * <p>This exists to settle one question. {@link FakeTableOperations} keeps {@code TableMetadata}
 * objects in memory and compares them by object identity, and that is the one property the
 * candidate finding in findings/r2-dst-harness.md §6 could not exclude. Iceberg's own
 * {@code TestTables} is no help: its {@code commit} also starts {@code if (base != current)} and
 * its {@code refresh} reads a static in-memory map, so it has both of the same properties.
 *
 * <p>Subclassing the real base class instead gives the properties a production catalog has, with
 * the harness's store and scheduler still underneath:
 *
 * <ul>
 *   <li>{@code refresh()} goes through the real {@code refreshFromMetadataLocation}, which calls
 *       {@code TableMetadataParser.read} — a genuine JSON round-trip on every refresh.
 *   <li>{@code commit()} is the real base-class orchestration, and {@code doCommit} writes a real
 *       metadata object through {@code writeNewMetadataIfRequired} before moving the pointer.
 *   <li>the compare-and-set is on the metadata <em>location</em>, which is content, not identity.
 * </ul>
 *
 * <p>Every byte still goes through {@link FakeFileIO}, so the scheduler keeps control and the faults
 * still apply.
 */
public class FakeMetastoreOperations extends BaseMetastoreTableOperations {

  private final FakeStore store;
  private final Scheduler sched;
  private final FaultConfig config;
  private final FakeFileIO io;
  private final String location;
  private final String name;

  private final Object state = new Object();
  private volatile String pointer;
  private final List<String> pointerHistory = new ArrayList<>();
  private final Map<Long, String> landedBy = new LinkedHashMap<>();
  private final AtomicLong nextSnapshotId = new AtomicLong(1_000L);

  public FakeMetastoreOperations(
      FakeStore store, Scheduler sched, FaultConfig config, FakeFileIO io, String location,
      String name) {
    this.store = store;
    this.sched = sched;
    this.config = config;
    this.io = io;
    this.location = location;
    this.name = name;
  }

  @Override
  protected String tableName() {
    return name;
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  protected void doRefresh() {
    sched.point("refresh");
    FilterCacheProbe.invalidateAll();
    String current = pointer;
    if (current == null) {
      disableRefresh();
    } else {
      // The real path: read the metadata object and parse it.
      refreshFromMetadataLocation(current);
    }
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String fiber = sched.currentName();
    // Writes a real metadata object through FileIO before the pointer moves.
    String newLocation = writeNewMetadataIfRequired(base == null, metadata);
    String expected = base == null ? null : base.metadataFileLocation();

    sched.point("commit");
    int roll = sched.armed() ? sched.draw("commit.fault", 100) : 100;
    int at = config.commitRejectPct;
    if (roll < at) {
      throw new CommitFailedException("fake metastore: rejected the commit of %s", fiber);
    }
    at += config.commitUnknownWithoutLandingPct;
    if (roll < at) {
      throw new CommitStateUnknownException(
          new RuntimeException("fake metastore: answer lost before the pointer moved"));
    }
    boolean unknownAfterLanding = roll < at + config.commitUnknownAfterLandingPct;

    synchronized (state) {
      if (!Objects.equals(expected, pointer)) {
        throw new CommitFailedException(
            "fake metastore: pointer moved while %s was committing", fiber);
      }
      this.pointer = newLocation;
      pointerHistory.add(newLocation);
      for (Snapshot snapshot : metadata.snapshots()) {
        landedBy.putIfAbsent(snapshot.snapshotId(), fiber);
      }
    }

    if (unknownAfterLanding) {
      throw new CommitStateUnknownException(
          new RuntimeException("fake metastore: answer lost after the pointer moved"));
    }
  }

  /** Lands the first metadata with no park and no fault. */
  public void bootstrap(TableMetadata metadata) {
    String path = metadataFileLocation("v0.metadata.json");
    store.put(
        path,
        org.apache.iceberg.TableMetadataParser.toJson(metadata)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    synchronized (state) {
      this.pointer = path;
      pointerHistory.add(path);
    }
    requestRefresh();
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return location + "/metadata/" + fileName;
  }

  @Override
  public long newSnapshotId() {
    return nextSnapshotId.getAndIncrement();
  }

  @Override
  public boolean requireStrictCleanup() {
    return config.strictCleanup;
  }

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

  public List<String> pointerHistory() {
    synchronized (state) {
      return new ArrayList<>(pointerHistory);
    }
  }
}
