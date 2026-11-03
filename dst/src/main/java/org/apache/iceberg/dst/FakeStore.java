package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The in-memory object store plus the ledger of everything that happened to it.
 *
 * <p>Bytes are real: the real {@code ManifestWriter} and {@code TableMetadataParser} write Avro and
 * JSON into this map, and the invariants read them back with the real readers. So a truncated write
 * produces a real Avro decode failure, not a simulated one.
 *
 * <p>Iceberg names manifests after a random commit UUID and snapshots after a random long, so raw
 * paths differ between two runs of the same seed. {@link #alias} hands out a stable short name on
 * first sight; because first sight happens in scheduler order, aliases are a function of the seed.
 * Only aliases go into the trace and into violation reports.
 */
public final class FakeStore {

  /** One completed or attempted write. */
  public static final class WriteRecord {
    public final String path;
    public final int declaredBytes;
    public final int storedBytes;
    public final boolean truncated;
    public final boolean reportedFailure;
    public final String fiber;

    WriteRecord(
        String path,
        int declaredBytes,
        int storedBytes,
        boolean truncated,
        boolean reportedFailure,
        String fiber) {
      this.path = path;
      this.declaredBytes = declaredBytes;
      this.storedBytes = storedBytes;
      this.truncated = truncated;
      this.reportedFailure = reportedFailure;
      this.fiber = fiber;
    }
  }

  /** One delete, with the catalog pointer version in force when it happened. */
  public static final class DeleteRecord {
    public final String path;
    public final long pointerVersion;
    public final String fiber;

    DeleteRecord(String path, long pointerVersion, String fiber) {
      this.path = path;
      this.pointerVersion = pointerVersion;
      this.fiber = fiber;
    }

    @Override
    public String toString() {
      return path + " by=" + fiber + " atPointerVersion=" + pointerVersion;
    }
  }

  /**
   * One injected read failure. {@code site} is the Iceberg frame that issued the read, taken from
   * the call stack (class and method, e.g. {@code IncrementalFileCleanup.lambda$cleanFiles$1}), so
   * which loop a fault hit is decided structurally rather than from a message.
   */
  public static final class ReadFaultRecord {
    public final String path;
    public final String site;
    /** Whether the manifest list belonged to a snapshot still in the current metadata. */
    public final boolean retainedSnapshot;
    public final String fiber;

    ReadFaultRecord(String path, String site, boolean retainedSnapshot, String fiber) {
      this.path = path;
      this.site = site;
      this.retainedSnapshot = retainedSnapshot;
      this.fiber = fiber;
    }

    @Override
    public String toString() {
      return site + (retainedSnapshot ? "/retained" : "/expired");
    }
  }

  private final List<ReadFaultRecord> readFaults = new ArrayList<>();
  private final Map<String, Integer> readsInScopeBySite = new LinkedHashMap<>();

  private final Map<String, Integer> cleanupReadsBySite = new LinkedHashMap<>();

  public synchronized void recordCleanupRead(String site, boolean retained) {
    cleanupReadsBySite.merge(site + (retained ? "/retained" : "/expired"), 1, Integer::sum);
  }

  /** Every manifest-list read issued from a FileCleanupStrategy frame, by site and liveness. */
  public synchronized Map<String, Integer> cleanupReadsBySite() {
    return new LinkedHashMap<>(cleanupReadsBySite);
  }

  public synchronized void recordReadInScope(String site, boolean retained) {
    readsInScopeBySite.merge(site + (retained ? "/retained" : "/expired"), 1, Integer::sum);
  }

  public synchronized void recordReadFault(
      String path, String site, boolean retained, String fiber) {
    readFaults.add(new ReadFaultRecord(path, site, retained, fiber));
  }

  public synchronized List<ReadFaultRecord> readFaults() {
    return new ArrayList<>(readFaults);
  }

  /** Every cleanup manifest-list read the fault was eligible for, by site; injected or not. */
  public synchronized Map<String, Integer> readsInScopeBySite() {
    return new LinkedHashMap<>(readsInScopeBySite);
  }

  private final Map<String, byte[]> files = new LinkedHashMap<>();
  private final Map<String, String> aliases = new LinkedHashMap<>();
  private final Map<String, Integer> aliasCounters = new LinkedHashMap<>();
  private final List<WriteRecord> writes = new ArrayList<>();
  private final List<DeleteRecord> deleteRecords = new ArrayList<>();
  private java.util.function.LongSupplier pointerVersion = () -> 0L;
  private final Set<String> truncatedPaths = new LinkedHashSet<>();
  private final Set<String> orphans = new LinkedHashSet<>();
  private final Map<String, WriteRecord> lastWrite = new LinkedHashMap<>();
  private final Map<String, Long> writeStep = new LinkedHashMap<>();
  private java.util.function.LongSupplier step = () -> 0L;

  public synchronized void put(String path, byte[] bytes) {
    files.put(path, bytes);
  }

  public synchronized byte[] get(String path) {
    return files.get(path);
  }

  public synchronized boolean exists(String path) {
    return files.containsKey(path);
  }

  public synchronized long length(String path) {
    byte[] b = files.get(path);
    return b == null ? -1 : b.length;
  }

  /**
   * Which catalog pointer version the deletes are measured against. Set once by the simulation; a
   * delete is legal exactly when the file is unreachable from the pointer at that version and
   * stays unreachable, which is what makes snapshot expiry expressible at all.
   */
  public synchronized void measureDeletesAgainst(java.util.function.LongSupplier supplier) {
    this.pointerVersion = supplier;
  }

  /**
   * The clock the file ages are measured on: scheduler steps rather than milliseconds. An orphan
   * cleaner only deletes files older than a cutoff, and that cutoff is the only thing protecting a
   * file a writer has written but not yet committed, so the harness needs an age it controls.
   */
  public synchronized void measureAgeWith(java.util.function.LongSupplier supplier) {
    this.step = supplier;
  }

  /** The scheduler step at which this path became visible, or null if it never did. */
  public synchronized Long writeStep(String path) {
    return writeStep.get(path);
  }

  public synchronized void delete(String path, String fiber) {
    deleteRecords.add(new DeleteRecord(path, pointerVersion.getAsLong(), fiber));
    files.remove(path);
  }

  public synchronized void recordWrite(WriteRecord record) {
    writes.add(record);
    lastWrite.put(record.path, record);
    if (record.storedBytes > 0) {
      writeStep.put(record.path, step.getAsLong());
    }
    if (record.truncated) {
      truncatedPaths.add(record.path);
    }
    if (record.reportedFailure && record.storedBytes > 0) {
      // bytes landed but the caller was told the write failed: an orphan, not a correctness
      // violation, but worth counting so the findings can say how often it happens.
      orphans.add(record.path);
    }
  }

  /**
   * Whether the store silently lost the tail of this path: a prefix was stored and close()
   * returned normally. Nothing Iceberg does can keep a table readable after that, so a violation
   * about such a file is excused rather than reported as a defect.
   */
  public synchronized String silentlyTruncated(String path) {
    WriteRecord record = lastWrite.get(path);
    if (record != null && record.truncated && !record.reportedFailure) {
      return "the store silently lost "
          + (record.declaredBytes - record.storedBytes)
          + " of "
          + record.declaredBytes
          + " bytes of "
          + alias(path)
          + " (writeSilentTruncatePct fault); close() reported success";
    }
    return null;
  }

  public synchronized WriteRecord writeRecordFor(String path) {
    return lastWrite.get(path);
  }

  public synchronized List<WriteRecord> writes() {
    return new ArrayList<>(writes);
  }

  public synchronized List<String> deletes() {
    List<String> out = new ArrayList<>(deleteRecords.size());
    for (DeleteRecord record : deleteRecords) {
      out.add(record.path);
    }
    return out;
  }

  public synchronized List<DeleteRecord> deleteRecords() {
    return new ArrayList<>(deleteRecords);
  }

  public synchronized Set<String> truncatedPaths() {
    return new LinkedHashSet<>(truncatedPaths);
  }

  public synchronized Set<String> orphans() {
    return new LinkedHashSet<>(orphans);
  }

  public synchronized Set<String> paths() {
    return new LinkedHashSet<>(files.keySet());
  }

  /** A stable, seed-deterministic short name for a path. */
  public synchronized String alias(String path) {
    String existing = aliases.get(path);
    if (existing != null) {
      return existing;
    }
    String kind = kindOf(path);
    int n = aliasCounters.merge(kind, 1, Integer::sum);
    String alias = kind + n;
    aliases.put(path, alias);
    return alias;
  }

  /** The kind of a path, used both for aliases and for trace labels. */
  public static String kindOf(String path) {
    String name = path.substring(path.lastIndexOf('/') + 1);
    if (name.endsWith(".json")) {
      return "meta";
    }
    if (name.startsWith("snap-")) {
      return "mlist";
    }
    if (name.endsWith(".avro")) {
      return "man";
    }
    if (name.contains("-deletes-")) {
      return "del";
    }
    return "data";
  }
}
