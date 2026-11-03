package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/**
 * Reads a snapshot back the way a reader would: manifest list, then manifests, then the live
 * content files. Everything goes through the real Iceberg readers over the fake store, so a
 * truncated Avro object fails here exactly as it would in production.
 */
public final class TableView {

  /** One content file as a reader sees it. */
  public static final class FileFact {
    public final String path;
    public final long sizeInBytes;
    public final long dataSequenceNumber;
    public final String partition;
    public final org.apache.iceberg.FileContent content;

    FileFact(
        String path,
        long sizeInBytes,
        long dataSequenceNumber,
        String partition,
        org.apache.iceberg.FileContent content) {
      this.path = path;
      this.sizeInBytes = sizeInBytes;
      this.dataSequenceNumber = dataSequenceNumber;
      this.partition = partition;
      this.content = content;
    }

    @Override
    public String toString() {
      return content + " " + partition + " seq=" + dataSequenceNumber;
    }
  }

  /** What one snapshot points at. */
  public static final class SnapshotFiles {
    public final long snapshotId;
    public final String manifestList;
    public final Map<String, Long> manifests = new LinkedHashMap<>();
    public final Map<String, Long> dataFiles = new LinkedHashMap<>();
    public final Map<String, Long> deleteFiles = new LinkedHashMap<>();
    /** The same files with the sequence number and content a reader would resolve. */
    public final Map<String, FileFact> facts = new LinkedHashMap<>();

    /**
     * Paths that appear more than once in this snapshot with <em>different</em> data sequence
     * numbers. {@code facts} is keyed by path and would silently keep the last one, so anything
     * comparing sequence numbers has to know these exist rather than report the map's iteration
     * order as a change.
     */
    public final Map<String, Set<Long>> ambiguousSequenceNumbers = new LinkedHashMap<>();
    /** Set when the snapshot could not be read; the message says why. */
    public String unreadable;

    /**
     * The file whose read failed. Recorded structurally rather than scraped out of the exception
     * message: Iceberg's wording for a bad object changed between releases, and matching on text
     * made the harness report a store fault as an Iceberg defect.
     */
    public String unreadablePath;

    SnapshotFiles(long snapshotId, String manifestList) {
      this.snapshotId = snapshotId;
      this.manifestList = manifestList;
    }

    public Set<String> everyFile() {
      Set<String> out = new LinkedHashSet<>();
      if (manifestList != null) {
        out.add(manifestList);
      }
      out.addAll(manifests.keySet());
      out.addAll(dataFiles.keySet());
      out.addAll(deleteFiles.keySet());
      return out;
    }
  }

  private TableView() {}

  /** Snapshot ids reachable from one ref, following parents. */
  public static Set<Long> ancestry(TableMetadata metadata, long head) {
    Set<Long> out = new LinkedHashSet<>();
    long id = head;
    while (true) {
      Snapshot snapshot = metadata.snapshot(id);
      if (snapshot == null || !out.add(id) || snapshot.parentId() == null) {
        return out;
      }
      id = snapshot.parentId();
    }
  }

  /** Snapshot ids reachable from any branch or tag in {@code metadata}, following parents. */
  public static Set<Long> reachable(TableMetadata metadata) {
    Set<Long> out = new LinkedHashSet<>();
    for (SnapshotRef ref : metadata.refs().values()) {
      long id = ref.snapshotId();
      while (true) {
        Snapshot snapshot = metadata.snapshot(id);
        if (snapshot == null) {
          // Snapshot expiry removes a snapshot from the metadata but leaves the parent id of its
          // child pointing at it, so an ancestry walk reaches ids that no longer exist. They are
          // not reachable: a reader cannot load them, and their files are legally deletable.
          break;
        }
        if (!out.add(id)) {
          break;
        }
        if (snapshot.parentId() == null) {
          break;
        }
        id = snapshot.parentId();
      }
    }
    return out;
  }

  /** Reads one snapshot. Never throws: a read failure lands in {@code unreadable}. */
  public static SnapshotFiles read(TableMetadata metadata, long snapshotId, FileIO io) {
    Snapshot snapshot = metadata.snapshot(snapshotId);
    if (snapshot == null) {
      SnapshotFiles files = new SnapshotFiles(snapshotId, null);
      files.unreadable = "snapshot " + snapshotId + " is not in the metadata";
      return files;
    }
    SnapshotFiles files = new SnapshotFiles(snapshotId, snapshot.manifestListLocation());
    List<ManifestFile> manifests;
    try {
      manifests = new ArrayList<>(snapshot.allManifests(io));
    } catch (RuntimeException e) {
      files.unreadable = Canon.normalize("manifest list unreadable: " + e);
      files.unreadablePath = snapshot.manifestListLocation();
      return files;
    }
    for (ManifestFile manifest : manifests) {
      files.manifests.put(manifest.path(), manifest.length());
      try {
        if (manifest.content() == ManifestContent.DELETES) {
          try (ManifestReader<?> reader =
              ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
            collect(reader, files.deleteFiles, files, metadata, manifest);
          }
        } else {
          try (ManifestReader<?> reader =
              ManifestFiles.read(manifest, io, metadata.specsById())) {
            collect(reader, files.dataFiles, files, metadata, manifest);
          }
        }
      } catch (Exception e) {
        files.unreadable = Canon.normalize("manifest " + manifest.path() + " unreadable: " + e);
        files.unreadablePath = manifest.path();
      }
    }
    return files;
  }

  private static void collect(
      CloseableIterable<?> entries,
      Map<String, Long> into,
      SnapshotFiles files,
      TableMetadata metadata,
      ManifestFile manifest) {
    org.apache.iceberg.PartitionSpec spec = metadata.specsById().get(manifest.partitionSpecId());
    for (Object entry : entries) {
      ContentFile<?> file = (ContentFile<?>) entry;
      into.put(file.location(), file.fileSizeInBytes());
      Long seq = file.dataSequenceNumber();
      long seqValue = seq == null ? -1L : seq;
      FileFact previous = files.facts.get(file.location());
      if (previous != null && previous.dataSequenceNumber != seqValue) {
        files
            .ambiguousSequenceNumbers
            .computeIfAbsent(file.location(), k -> new LinkedHashSet<>())
            .add(previous.dataSequenceNumber);
        files.ambiguousSequenceNumbers.get(file.location()).add(seqValue);
      }
      files.facts.put(
          file.location(),
          new FileFact(
              file.location(),
              file.fileSizeInBytes(),
              seqValue,
              spec == null ? "?" : spec.partitionToPath(file.partition()),
              file.content()));
    }
  }
}
