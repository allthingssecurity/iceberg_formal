package org.apache.iceberg.dst;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;

/**
 * Builds the data and delete files a writer commits, and puts their bytes in the store.
 *
 * <p>The bytes are not real Parquet: the harness never reads a data file, only checks that it is
 * there and has the length the manifest claims. What matters is that the path and the declared
 * length are the writer's payload identity, so "applied at most once" and "never deleted while
 * reachable" can be decided from the manifests alone.
 */
public final class Payload {

  public static final String[] PARTITIONS = {"a", "b"};

  private Payload() {}

  /** A data file whose bytes are written through {@code io}, so a write fault can hit it. */
  public static DataFile dataFile(
      FileIO io, PartitionSpec spec, String location, String owner, int ordinal, String partition) {
    return dataFile(io, spec, location, owner, ordinal, partition, 4);
  }

  /** A data file with an explicit record count, so a rewrite can shrink what it replaces. */
  public static DataFile dataFile(
      FileIO io,
      PartitionSpec spec,
      String location,
      String owner,
      int ordinal,
      String partition,
      int recordCount) {
    String path =
        location + "/data/data=" + partition + "/" + owner + "-d" + ordinal + ".parquet";
    byte[] bytes = body("data " + owner + " " + ordinal + " " + partition);
    writeThrough(io, path, bytes);
    return DataFiles.builder(spec)
        .withPath(path)
        .withPartitionPath("data=" + partition)
        .withFileSizeInBytes(bytes.length)
        .withRecordCount(recordCount)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  /** A position delete file against one partition. */
  public static DeleteFile positionDeletes(
      FileIO io, PartitionSpec spec, String location, String owner, int ordinal, String partition) {
    String path =
        location + "/data/data=" + partition + "/" + owner + "-deletes-" + ordinal + ".parquet";
    byte[] bytes = body("pos-deletes " + owner + " " + ordinal + " " + partition);
    writeThrough(io, path, bytes);
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath(path)
        .withPartitionPath("data=" + partition)
        .withFileSizeInBytes(bytes.length)
        .withRecordCount(2)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  /**
   * A deletion vector: the only shape of position delete the v3 format accepts.
   *
   * <p>A DV is a Puffin-format position-delete file ({@code ContentFileUtil.isDV} is exactly
   * {@code format() == PUFFIN}) that references one data file. Without this the harness could not
   * exercise position deletes on v3 at all, which matters because the DV code is the newest in the
   * commit path.
   */
  public static DeleteFile deletionVector(
      FileIO io,
      PartitionSpec spec,
      String location,
      String owner,
      int ordinal,
      String partition,
      String referencedDataFile) {
    String path =
        location + "/data/data=" + partition + "/" + owner + "-deletes-dv" + ordinal + ".puffin";
    byte[] bytes = body("dv " + owner + " " + ordinal + " " + referencedDataFile);
    writeThrough(io, path, bytes);
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath(path)
        .withFormat(FileFormat.PUFFIN)
        .withPartitionPath("data=" + partition)
        .withFileSizeInBytes(bytes.length)
        .withRecordCount(1)
        .withReferencedDataFile(referencedDataFile)
        .withContentOffset(0L)
        .withContentSizeInBytes(bytes.length)
        .build();
  }

  /** An equality delete file on the id column. */
  public static DeleteFile equalityDeletes(
      FileIO io,
      PartitionSpec spec,
      String location,
      String owner,
      int ordinal,
      String partition,
      int... equalityFieldIds) {
    String path =
        location + "/data/data=" + partition + "/" + owner + "-deletes-eq" + ordinal + ".parquet";
    byte[] bytes = body("eq-deletes " + owner + " " + ordinal + " " + partition);
    writeThrough(io, path, bytes);
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldIds)
        .withPath(path)
        .withPartitionPath("data=" + partition)
        .withFileSizeInBytes(bytes.length)
        .withRecordCount(2)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  public static List<String> locations(Iterable<DataFile> files) {
    List<String> out = new ArrayList<>();
    for (DataFile f : files) {
      out.add(f.location());
    }
    return out;
  }

  private static byte[] body(String text) {
    StringBuilder sb = new StringBuilder();
    // A fixed, path-derived body: same seed, same bytes, same lengths.
    for (int i = 0; i < 8; i++) {
      sb.append(text).append('\n');
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static void writeThrough(FileIO io, String path, byte[] bytes) {
    try (java.io.OutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      out.write(bytes);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
