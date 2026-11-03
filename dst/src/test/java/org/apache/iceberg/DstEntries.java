package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/**
 * Test-only bridge to the package-private {@code ManifestReader.entries()} and {@code
 * ManifestEntry}, so the r5 checks can read an entry's own snapshot id, status and sequence numbers
 * through Iceberg's real reader (inheritance from the manifest-list entry included).
 */
public final class DstEntries {

  /** One manifest entry, as Iceberg's reader resolves it. */
  public static final class Entry {
    public final String manifest;
    public final String path;
    public final String status;
    public final Long snapshotId;
    public final Long dataSequenceNumber;
    public final Long fileSequenceNumber;
    public final Long firstRowId;
    public final long recordCount;

    Entry(String manifest, ManifestEntry<DataFile> e) {
      this.manifest = manifest;
      this.path = e.file().location();
      this.status = e.status().name();
      this.snapshotId = e.snapshotId();
      this.dataSequenceNumber = e.dataSequenceNumber();
      this.fileSequenceNumber = e.fileSequenceNumber();
      this.firstRowId = e.file().firstRowId();
      this.recordCount = e.file().recordCount();
    }

    @Override
    public String toString() {
      return status + " snap=" + snapshotId + " dataSeq=" + dataSequenceNumber + " fileSeq="
          + fileSequenceNumber + (firstRowId == null ? "" : " firstRowId=" + firstRowId);
    }
  }

  private DstEntries() {}

  /** Every entry (ADDED, EXISTING and DELETED) of every data manifest of one snapshot. */
  public static List<Entry> dataEntries(TableMetadata metadata, long snapshotId, FileIO io) {
    List<Entry> out = new ArrayList<>();
    Snapshot snapshot = metadata.snapshot(snapshotId);
    for (ManifestFile manifest : snapshot.dataManifests(io)) {
      try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, metadata.specsById());
          CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries()) {
        for (ManifestEntry<DataFile> e : entries) {
          out.add(new Entry(manifest.path(), e));
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return out;
  }
}
