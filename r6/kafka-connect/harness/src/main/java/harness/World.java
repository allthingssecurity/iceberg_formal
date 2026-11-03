package harness;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.SharedCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

/** One run: fresh fake Kafka, fresh metastore, an emulated Connect cluster, and the oracle. */
public final class World {
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "p", Types.LongType.get()),
          Types.NestedField.optional(3, "o", Types.LongType.get()));

  public final Emu emu;
  public final FakeKafka kafka;
  public final List<String> tables;
  public final int commitTimeoutMs;

  public World(int srcPartitions, int ctlPartitions, List<String> tables, Map<String, String> extra) {
    this.tables = tables;
    SharedCatalog.reset();
    InMemoryCatalog d = new InMemoryCatalog();
    d.initialize("mem", ImmutableMap.of());
    SharedCatalog.DELEGATE = d;
    d.createNamespace(Namespace.of("db"));
    for (String t : tables) {
      d.createTable(
          TableIdentifier.parse(t),
          SCHEMA,
          org.apache.iceberg.PartitionSpec.unpartitioned(),
          ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "avro"));
    }
    Map<String, String> props = new HashMap<>();
    props.put("name", "sink");
    props.put("iceberg.tables", String.join(",", tables));
    props.put("iceberg.catalog.catalog-impl", SharedCatalog.class.getName());
    props.put("iceberg.control.topic", "ctl");
    props.put("iceberg.control.commit.interval-ms", "0");
    props.put("iceberg.control.commit.timeout-ms", "2000");
    props.put("iceberg.control.commit.threads", "1");
    props.put("iceberg.tables.auto-create-enabled", "false");
    props.putAll(extra);
    this.commitTimeoutMs = Integer.parseInt(props.get("iceberg.control.commit.timeout-ms"));
    this.emu = new Emu(srcPartitions, props);
    this.kafka = new FakeKafka("ctl", ctlPartitions, emu);
    FakeKafka.K = kafka;
    SharedCatalog.isCrashed = emu::isCrashed;
    SharedCatalog.currentTask = Emu::currentTask;
    SharedCatalog.crashHook =
        () -> {
          int t = kafka.steppingTask;
          if (t < 0) {
            throw new IllegalStateException("crash fault outside a coordinator step");
          }
          emu.markCrashed(t);
          return new Emu.CrashedProcess("injected crash of task " + t + " at table commit");
        };
  }

  // ------------------------------------------------------------------ scheduler helpers

  /** Runs a task callback; a failed task is closed and stopped the way WorkerSinkTask does. */
  public void safe(Runnable r) {
    try {
      r.run();
    } catch (Emu.TaskFailed f) {
      try {
        emu.failTask(f.task);
      } catch (RuntimeException ignored) {
        // already dead
      }
    }
    quiesce();
  }

  public void quiesce() {
    try {
      kafka.awaitQuiescent();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean step(int task) {
    try {
      boolean b = kafka.step(task);
      emu.finishPendingCrash();
      return b;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void fault(String kind, int from, int count) {
    SharedCatalog.faultKind = SharedCatalog.Kind.of(kind);
    SharedCatalog.faultFrom = from;
    SharedCatalog.faultCount = count;
  }

  public void stepAll() {
    for (FakeKafka.Gate g : kafka.liveGates()) {
      step(g.ownerTask);
    }
  }

  public void tick() {
    try {
      Thread.sleep(commitTimeoutMs + 150L);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /** One normal commit cycle: start commit, workers answer, coordinator commits. */
  public void cycle() {
    stepAll();
    safe(() -> emu.pollAll(1_000_000));
    stepAll();
    safe(() -> emu.pollAll(1_000_000));
    stepAll();
  }

  /**
   * Fault-free tail: operator restarts failed tasks, orphan partitions go to the lowest live
   * task, commit cycles run (with commit timeouts) until everything produced is committed or the
   * round budget is spent.
   */
  public int heal(int rounds) {
    String last = null;
    int stable = 0;
    for (int r = 1; r <= rounds; r++) {
      // stop early once nothing changes any more (table rows + committed offsets) for 2 rounds
      String now = rows().toString() + committedSource();
      if (now.equals(last)) {
        if (++stable >= 2) {
          return -r;
        }
      } else {
        stable = 0;
      }
      last = now;
      for (Emu.Slot s : emu.slots()) {
        if (!s.running) {
          safe(() -> emu.startTask(s.id));
        }
      }
      for (int p = 0; p < emu.srcPartitions; p++) {
        final int fp = p;
        boolean owned = emu.slots().stream().anyMatch(s -> s.running && !s.crashed && s.assignment.contains(emu.tp(fp)));
        if (!owned) {
          int target = emu.slots().stream().filter(s -> s.running && !s.crashed).mapToInt(s -> s.id).min().orElseThrow();
          safe(() -> emu.assign(target, fp));
        }
      }
      cycle();
      if (done()) {
        return r;
      }
      tick();
      cycle();
      if (done()) {
        return r;
      }
    }
    return -1;
  }

  boolean done() {
    Map<Long, Integer> rows = rows();
    for (int p = 0; p < emu.srcPartitions; p++) {
      Long c = kafka.committed(emu.connectGroup, emu.tp(p));
      long end = emu.produced.get(p);
      if (end > 0 && (c == null || c < end)) {
        return false;
      }
    }
    long total = emu.produced.values().stream().mapToLong(Integer::longValue).sum();
    return rows.size() >= total;
  }

  // ------------------------------------------------------------------ oracle

  /** (p*1e6+o) -> number of rows in the table (per table: summed over tables / tables.size()). */
  public Map<Long, Integer> rows(String tableName) {
    Table t = SharedCatalog.DELEGATE.loadTable(TableIdentifier.parse(tableName));
    Map<Long, Integer> m = new TreeMap<>();
    try (CloseableIterable<Record> it = IcebergGenerics.read(t).build()) {
      for (Record rec : it) {
        m.merge((Long) rec.getField("id"), 1, Integer::sum);
      }
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
    return m;
  }

  Map<Long, Integer> rows() {
    return rows(tables.get(0));
  }

  public static final class Verdict {
    public long produced;
    public long lost; // offset < committed source offset, but not in the table: permanent loss
    public long uncommittedMissing; // not in table, offset >= committed: will be re-read
    public long dupRows;
    public long dupFiles; // data file paths added by more than one snapshot
    public int snapshots;
    public String detail = "";

    public boolean bad() {
      return lost > 0 || dupRows > 0 || dupFiles > 0;
    }

    @Override
    public String toString() {
      return String.format(
          "produced=%d lost=%d dupRows=%d dupFiles=%d uncommittedMissing=%d snapshots=%d%s",
          produced, lost, dupRows, dupFiles, uncommittedMissing, snapshots, detail);
    }
  }

  public Map<String, Verdict> verdicts() {
    Map<String, Verdict> out = new LinkedHashMap<>();
    for (String tn : tables) {
      out.put(tn, verdict(tn));
    }
    return out;
  }

  public Verdict verdict(String tableName) {
    Verdict v = new Verdict();
    Map<Long, Integer> rows = rows(tableName);
    StringBuilder lostSample = new StringBuilder();
    for (int p = 0; p < emu.srcPartitions; p++) {
      Long c = kafka.committed(emu.connectGroup, emu.tp(p));
      long committed = c == null ? 0 : c;
      int end = emu.produced.get(p);
      v.produced += end;
      for (long o = 0; o < end; o++) {
        long id = p * 1_000_000L + o;
        int n = rows.getOrDefault(id, 0);
        if (n == 0 && o < committed) {
          v.lost++;
          if (lostSample.length() < 120) {
            lostSample.append(" p").append(p).append("@").append(o);
          }
        } else if (n == 0) {
          v.uncommittedMissing++;
        } else if (n > 1) {
          v.dupRows += n - 1;
        }
      }
    }
    Table t = SharedCatalog.DELEGATE.loadTable(TableIdentifier.parse(tableName));
    Map<String, Integer> fileSnaps = new HashMap<>();
    for (Snapshot s : t.snapshots()) {
      v.snapshots++;
      for (DataFile f : s.addedDataFiles(t.io())) {
        fileSnaps.merge(f.location(), 1, Integer::sum);
      }
    }
    v.dupFiles = fileSnaps.values().stream().filter(n -> n > 1).count();
    if (lostSample.length() > 0) {
      v.detail = " lostSample:" + lostSample;
    }
    return v;
  }

  public Map<Integer, Long> committedSource() {
    Map<Integer, Long> m = new TreeMap<>();
    for (int p = 0; p < emu.srcPartitions; p++) {
      m.put(p, kafka.committed(emu.connectGroup, emu.tp(p)));
    }
    return m;
  }
}
