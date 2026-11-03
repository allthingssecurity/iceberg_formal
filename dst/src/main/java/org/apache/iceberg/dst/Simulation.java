package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;

/** One seed, one run of the real commit path against the fakes. */
public final class Simulation {

  public static final String LOCATION = "dst://table";

  /** The table format version the harness writes. */
  public static final int SPEC_FORMAT_VERSION = 2;

  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  public static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").build();

  private Simulation() {}

  /**
   * Removes a file a committed snapshot references, so the invariants have something they must
   * catch. Only ever called for a {@link FaultConfig.SelfCheck} mode.
   */
  private static void breakOnPurpose(RunResult result, FakeFileIO io) {
    FaultConfig.SelfCheck mode = result.config.selfCheck;
    if (mode == FaultConfig.SelfCheck.OFF) {
      return;
    }
    for (TableView.SnapshotFiles files : result.ops.capturedAtCommit().values()) {
      java.util.Collection<String> candidates =
          mode == FaultConfig.SelfCheck.DELETE_A_COMMITTED_MANIFEST
              ? files.manifests.keySet()
              : files.dataFiles.keySet();
      for (String path : candidates) {
        if (result.store.exists(path)) {
          result.store.delete(path, "self-check");
          return;
        }
      }
    }
  }


  /**
   * Puts a manifest holding the same data file twice into the table.
   *
   * <p>A duplicate is only a duplicate inside one manifest: {@code ManifestFilterManager} keeps its
   * "already deleted" set per manifest, so the same path in two different manifests is two ordinary
   * deletes. The only way to get two entries into one manifest is to write it directly and append
   * it, which is exactly what the test apache/iceberg#16686 added does.
   */
  private static void seedDuplicateManifest(
      Table table, FakeFileIO io, FakeTableOperations ops) throws java.io.IOException {
    DataFile duplicated =
        Payload.dataFile(io.observerView(), SPEC, LOCATION, "dup", 0, Payload.PARTITIONS[0]);
    String path = ops.metadataFileLocation("duplicate-entries.avro");
    org.apache.iceberg.ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            SPEC_FORMAT_VERSION, SPEC, io.observerView().newOutputFile(path), null);
    try {
      writer.add(duplicated);
      writer.add(duplicated);
    } finally {
      writer.close();
    }
    org.apache.iceberg.AppendFiles append = table.newFastAppend();
    append.appendManifest(writer.toManifestFile());
    CommitUuid.makeDeterministic(append);
    SameThreadExecutor.inject(append).commit();
  }

  public static RunResult run(long seed, FaultConfig config) {
    CommitUuid.reset();
    Trace trace = new Trace();
    Scheduler sched = new Scheduler(seed, trace, config.stepBudget);
    FakeStore store = new FakeStore();
    FakeFileIO io = new FakeFileIO(store, sched, config);
    FakeTableOperations ops = new FakeTableOperations(store, sched, config, io, LOCATION);
    // Every delete is recorded against the catalog pointer version in force at the time, which is
    // what lets I5 tell a legal expiry delete from data loss.
    store.measureDeletesAgainst(ops::pointerVersion);
    store.measureAgeWith(sched::steps);
    // The read fault (off by default) needs to know which manifest lists belong to snapshots the
    // catalog still holds. Read through current(), which neither parks nor faults.
    io.classifyManifestListsWith(
        path -> {
          TableMetadata metadata = ops.current();
          if (metadata == null) {
            return false;
          }
          for (org.apache.iceberg.Snapshot snapshot : metadata.snapshots()) {
            if (path.equals(snapshot.manifestListLocation())) {
              return true;
            }
          }
          return false;
        });

    List<Workload.Outcome> outcomes = Collections.synchronizedList(new ArrayList<>());
    Throwable harnessFailure = null;
    try {
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put(TableProperties.FORMAT_VERSION, Integer.toString(config.formatVersion));
      // Retries must not sleep: the harness has no clock to advance and a real sleep would only
      // slow the seed sweep down.
      properties.put(TableProperties.COMMIT_NUM_RETRIES, "4");
      properties.put(TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "0");
      properties.put(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "0");
      properties.put(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS, "60000");
      // The manifest cache is global and keyed by FileIO instance; leaving it on would carry state
      // between seeds.
      properties.put("io.manifest.cache-enabled", "false");

      ops.bootstrap(TableMetadata.newTableMetadata(SCHEMA, SPEC, LOCATION, properties));
      Table table = new BaseTable(ops, "dst");

      // Seed the table with files the rewrite and overwrite writers can act on. No faults yet.
      // The files the rewrite writers replace all come from the first append, so they keep data
      // sequence number 1 however many later appends the table gets.
      List<DataFile> seedFiles = new ArrayList<>();
      for (int round = 0; round < Math.max(1, config.seedSnapshots); round++) {
        org.apache.iceberg.AppendFiles seedAppend = table.newAppend();
        for (int i = 0; i < Payload.PARTITIONS.length; i++) {
          DataFile df =
              Payload.dataFile(
                  io, SPEC, LOCATION, "seed" + round, i, Payload.PARTITIONS[i]);
          if (round == 0) {
            seedFiles.add(df);
          }
          seedAppend.appendFile(df);
        }
        CommitUuid.makeDeterministic(seedAppend);
        SameThreadExecutor.inject(seedAppend).commit();
      }

      // Data and delete files are written by the engine, not by iceberg-core, so they are written
      // through a fault-free view. Faults stay on the files Iceberg itself writes: manifests,
      // manifest lists and the metadata object. Faulting the engine's writes would only abort
      // writers before they reach the commit path under test.
      if (config.duplicateDeleteWorkload) {
        seedDuplicateManifest(table, io, ops);
      }

      Workload workload =
          new Workload(
              table, ops::lastLandedFor, store, io.observerView(), sched, config, SPEC, LOCATION, seedFiles);

      sched.arm();
      for (int i = 0; i < config.writers; i++) {
        final String name = "w" + (i + 1);
        final int index = i;
        sched.spawn(name, () -> outcomes.addAll(workload.runAll(name, index)));
      }
      sched.runToCompletion();
    } catch (Throwable t) {
      harnessFailure = t;
    } finally {
      sched.disarm();
      ManifestFiles.dropCache(io);
    }

    List<Workload.Outcome> ordered = new ArrayList<>(outcomes);
    ordered.sort(java.util.Comparator.comparing(o -> o.fiber));

    RunResult result =
        new RunResult(seed, config, trace, sched, store, ops, ordered, ops.current());
    result.harnessFailure = harnessFailure;
    for (Throwable t : sched.fiberFailures()) {
      result.violations.add(
          new Violation("I6-harness", "a fiber died outside its own try/catch: " + t));
    }
    if (harnessFailure == null) {
      breakOnPurpose(result, io);
      try {
        Invariants.check(result);
      } catch (Throwable t) {
        result.harnessFailure = t;
      }
    }
    return result;
  }
}
