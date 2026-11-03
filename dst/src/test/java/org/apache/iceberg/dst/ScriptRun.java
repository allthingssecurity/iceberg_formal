package org.apache.iceberg.dst;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;

/**
 * {@link Simulation#run} with a fixed script per fiber instead of a drawn action, plus an optional
 * setup step that runs disarmed (no park, no fault) after the seed appends. The same fakes, the
 * same scheduler and the same invariants; only the choice of operations is pinned.
 */
final class ScriptRun {

  /** Setup work on the seeded table before the scheduler is armed. */
  interface Setup {
    void apply(Table table, FakeFileIO io, List<DataFile> seedFiles) throws Exception;
  }

  static final class Ctx {
    RunResult result;
    Table table;
    FakeFileIO io;
    List<DataFile> seedFiles;
  }

  private ScriptRun() {}

  static Ctx run(long seed, FaultConfig config, Setup setup, String[]... scripts) {
    return run(seed, config, setup, null, scripts);
  }

  /** Registers scenario actions on the workload once the table is seeded and set up. */
  interface Actions {
    void register(Workload workload, Table table, List<DataFile> seedFiles);
  }

  static Ctx run(long seed, FaultConfig config, Setup setup, Actions actions, String[]... scripts) {
    CommitUuid.reset();
    FilterCacheProbe.reset();
    Trace trace = new Trace();
    Scheduler sched = new Scheduler(seed, trace, config.stepBudget);
    FakeStore store = new FakeStore();
    FakeFileIO io = new FakeFileIO(store, sched, config);
    FakeTableOperations ops = new FakeTableOperations(store, sched, config, io, Simulation.LOCATION);
    store.measureDeletesAgainst(ops::pointerVersion);
    store.measureAgeWith(sched::steps);
    io.classifyManifestListsWith(
        path -> {
          TableMetadata m = ops.current();
          if (m == null) {
            return false;
          }
          for (org.apache.iceberg.Snapshot s : m.snapshots()) {
            if (path.equals(s.manifestListLocation())) {
              return true;
            }
          }
          return false;
        });

    Ctx ctx = new Ctx();
    ctx.io = io;
    List<Workload.Outcome> outcomes = Collections.synchronizedList(new ArrayList<>());
    Throwable harnessFailure = null;
    try {
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put(TableProperties.FORMAT_VERSION, Integer.toString(config.formatVersion));
      properties.put(TableProperties.COMMIT_NUM_RETRIES, "6");
      properties.put(TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "0");
      properties.put(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "0");
      properties.put(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS, "60000");
      properties.put("io.manifest.cache-enabled", "false");
      ops.bootstrap(
          TableMetadata.newTableMetadata(
              Simulation.SCHEMA, Simulation.SPEC, Simulation.LOCATION, properties));
      Table table = new BaseTable(ops, "dst");
      ctx.table = table;

      List<DataFile> seedFiles = new ArrayList<>();
      for (int round = 0; round < Math.max(1, config.seedSnapshots); round++) {
        org.apache.iceberg.AppendFiles seedAppend = table.newAppend();
        for (int i = 0; i < Payload.PARTITIONS.length; i++) {
          DataFile df =
              Payload.dataFile(
                  io, Simulation.SPEC, Simulation.LOCATION, "seed" + round, i, Payload.PARTITIONS[i]);
          if (round == 0) {
            seedFiles.add(df);
          }
          seedAppend.appendFile(df);
        }
        CommitUuid.makeDeterministic(seedAppend);
        SameThreadExecutor.inject(seedAppend).commit();
      }
      ctx.seedFiles = seedFiles;
      if (setup != null) {
        setup.apply(table, io, seedFiles);
      }

      Workload workload =
          new Workload(
              table, ops::lastLandedFor, store, io.observerView(), sched, config, Simulation.SPEC,
              Simulation.LOCATION, seedFiles);
      if (actions != null) {
        actions.register(workload, table, seedFiles);
      }
      sched.arm();
      for (int f = 0; f < scripts.length; f++) {
        String fiber = "w" + (f + 1);
        String[] script = scripts[f];
        if (script.length == 0) {
          continue;
        }
        final int fiberIndex = f;
        sched.spawn(
            fiber,
            () -> {
              for (int i = 0; i < script.length; i++) {
                Workload.Outcome outcome = new Workload.Outcome(fiber);
                outcome.action = script[i];
                try {
                  workload.runAction(script[i], fiber, fiber + "o" + (i + 1), fiberIndex + i, outcome);
                  outcome.reportedSuccess = true;
                } catch (Throwable t) {
                  outcome.reportedSuccess = false;
                  outcome.failure = Canon.normalize(t.getClass().getName() + ": " + t.getMessage());
                }
                outcomes.add(outcome);
              }
            });
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
    RunResult result = new RunResult(seed, config, trace, sched, store, ops, ordered, ops.current());
    result.harnessFailure = harnessFailure;
    for (Throwable t : sched.fiberFailures()) {
      result.violations.add(new Violation("I6-harness", "a fiber died outside its own try/catch: " + t));
    }
    if (harnessFailure == null) {
      try {
        Invariants.check(result);
      } catch (Throwable t) {
        result.harnessFailure = t;
      }
    }
    ctx.result = result;
    return ctx;
  }
}
