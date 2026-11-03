package org.apache.iceberg.dst;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

/**
 * Looks for the observable behind the candidate finding: one manifest file listed in different
 * snapshots' manifest lists with different {@code sequence_number} values.
 *
 * <p>Stated on the manifest rather than on an invariant, so the same check can be run against two
 * different catalog implementations and compared. In v2 and v3 an entry whose own
 * {@code data_sequence_number} is null inherits the number recorded for its manifest, and that
 * number decides which equality deletes apply to those rows, so two snapshots disagreeing about it
 * are two different tables.
 */
public final class SequenceNumberFlip {

  private SequenceNumberFlip() {}

  /**
   * The reader-visible form: a data file path that resolves to more than one
   * {@code data_sequence_number} across the snapshots a ref can still reach.
   *
   * <p>This is the claim that matters. A file's data sequence number is fixed when it is added, and
   * it is what decides which equality deletes apply to its rows, so two reachable snapshots
   * resolving it differently means two readers of the same table get different rows. Stated on the
   * file rather than on the manifest because that is the level a reader sees.
   */
  public static Map<String, Set<Long>> findFileDisagreement(TableMetadata metadata, FileIO io) {
    Map<String, Set<Long>> seen = new LinkedHashMap<>();
    for (long id : TableView.reachable(metadata)) {
      TableView.SnapshotFiles files = TableView.read(metadata, id, io);
      if (files.unreadable != null) {
        continue;
      }
      for (TableView.FileFact fact : files.facts.values()) {
        if (fact.content == org.apache.iceberg.FileContent.DATA) {
          seen.computeIfAbsent(fact.path, k -> new LinkedHashSet<>()).add(fact.dataSequenceNumber);
        }
      }
    }
    Map<String, Set<Long>> disagreeing = new LinkedHashMap<>();
    seen.forEach(
        (path, numbers) -> {
          if (numbers.size() > 1) {
            disagreeing.put(path, numbers);
          }
        });
    return disagreeing;
  }

  /**
   * Whether Iceberg itself accepts this metadata. Both checks the peer session identified run here:
   * {@code TableMetadata.indexAndValidateSnapshots} (TableMetadata.java:835) runs in the
   * constructor, so it executes on a JSON parse and on every {@code buildFrom(...).build()}.
   *
   * @return null when the metadata is accepted, otherwise why it was rejected
   */
  public static String whyIcebergWouldReject(TableMetadata metadata) {
    for (org.apache.iceberg.Snapshot snapshot : metadata.snapshots()) {
      if (snapshot.sequenceNumber() > metadata.lastSequenceNumber()) {
        return "snapshot "
            + snapshot.snapshotId()
            + " has sequence number "
            + snapshot.sequenceNumber()
            + " above lastSequenceNumber "
            + metadata.lastSequenceNumber();
      }
    }
    try {
      // Runs indexAndValidateSnapshots via the constructor.
      org.apache.iceberg.TableMetadata.buildFrom(metadata).build();
    } catch (RuntimeException e) {
      return "TableMetadata.buildFrom(...).build() rejected it: " + e;
    }
    try {
      // The same validation again, through the real serialiser a catalog would use.
      org.apache.iceberg.TableMetadataParser.fromJson(
          org.apache.iceberg.TableMetadataParser.toJson(metadata));
    } catch (RuntimeException e) {
      return "TableMetadataParser round-trip rejected it: " + e;
    }
    return null;
  }

  /** Over every snapshot the metadata still holds, including ones no ref can reach. */
  public static Map<String, Set<Long>> find(TableMetadata metadata, FileIO io) {
    return find(metadata, io, null);
  }

  /** Over the snapshots a ref can still reach: the stronger, less arguable claim. */
  public static Map<String, Set<Long>> findReachable(TableMetadata metadata, FileIO io) {
    return find(metadata, io, TableView.reachable(metadata));
  }

  /**
   * Manifest path to the distinct sequence numbers it is listed with; only entries with more than
   * one. {@code only} limits which snapshots count, or null for all of them.
   */
  public static Map<String, Set<Long>> find(
      TableMetadata metadata, FileIO io, Set<Long> only) {
    Map<String, Set<Long>> seen = new LinkedHashMap<>();
    for (Snapshot snapshot : metadata.snapshots()) {
      if (only != null && !only.contains(snapshot.snapshotId())) {
        continue;
      }
      Iterable<ManifestFile> manifests;
      try {
        manifests = snapshot.allManifests(io);
      } catch (RuntimeException e) {
        continue;
      }
      for (ManifestFile manifest : manifests) {
        if (manifest.content() != ManifestContent.DATA) {
          continue;
        }
        seen.computeIfAbsent(manifest.path(), k -> new LinkedHashSet<>())
            .add(manifest.sequenceNumber());
      }
    }
    Map<String, Set<Long>> flipped = new LinkedHashMap<>();
    seen.forEach(
        (path, numbers) -> {
          if (numbers.size() > 1) {
            flipped.put(path, numbers);
          }
        });
    return flipped;
  }
}
