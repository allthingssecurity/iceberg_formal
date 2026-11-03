package r5;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * A2-28: JDBC CAS UPDATE lands, client sees SQLState 08007, ClientPoolImpl re-runs it (0 rows).
 *
 * <p>-Dr5.scenario=<name>|all -Dr5.mode=fault|control|both -Dr5.runs=N
 */
public class JdbcLostAckTest {
  static final List<String> SCENARIOS =
      List.of("ops-direct", "fast-append", "merge-append", "txn-fast-append", "overwrite", "delete",
          "fast-append-cw", "merge-append-cw", "txn-fast-append-cw", "overwrite-cw", "delete-cw", "add-column");
  static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  static final TableIdentifier ID = TableIdentifier.of(Namespace.of("db"), "t");
  static final File OUT = new File("build/r5");

  static {
    try {
      Class.forName(FaultDriver.class.getName());
    } catch (ClassNotFoundException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  static List<String> scenarios() {
    String s = System.getProperty("r5.scenario", "all");
    if (s.equals("all")) {
      return SCENARIOS;
    }
    List<String> out = new ArrayList<>();
    for (String name : s.split(",")) {
      if (!SCENARIOS.contains(name)) {
        throw new IllegalArgumentException("unknown r5.scenario: " + name + " (known " + SCENARIOS + ")");
      }
      out.add(name);
    }
    return out;
  }

  static List<Boolean> modes() {
    String m = System.getProperty("r5.mode", "both");
    switch (m) {
      case "fault":
        return List.of(true);
      case "control":
        return List.of(false);
      case "both":
        return List.of(true, false);
      default:
        throw new IllegalArgumentException("unknown r5.mode: " + m);
    }
  }

  @Test
  public void run() throws Exception {
    int runs = Integer.parseInt(System.getProperty("r5.runs", "10"));
    OUT.mkdirs();
    try (PrintWriter sum = new PrintWriter(new File(OUT, "jdbc-summary-" + System.getProperty("r5.scenario", "all").replace(',', '_') + ".txt"));
        PrintWriter detail = new PrintWriter(new File(OUT, "jdbc-detail-" + System.getProperty("r5.scenario", "all").replace(',', '_') + ".txt"))) {
      for (String scenario : scenarios()) {
        for (boolean fault : modes()) {
          Map<String, Integer> outcomes = new TreeMap<>();
          int injectedTotal = 0;
          for (int i = 0; i < runs; i++) {
            Map<String, Object> r = runOnce(scenario, fault, i);
            injectedTotal += (Integer) r.get("injected");
            if (fault && (Integer) r.get("injected") != 1) {
              throw new IllegalStateException("harness: expected exactly 1 injected fault, got " + r);
            }
            if (!fault && (Integer) r.get("injected") != 0) {
              throw new IllegalStateException("harness: control injected a fault " + r);
            }
            outcomes.merge(String.valueOf(r.get("outcome")), 1, Integer::sum);
            if (i == 0) {
              detail.println(scenario + " " + (fault ? "fault" : "control") + " run0: " + r);
            }
          }
          sum.printf("%-16s %-7s runs=%d injected=%d outcomes=%s%n",
              scenario, fault ? "fault" : "control", runs, injectedTotal, outcomes);
        }
      }
    }
  }

  static JdbcCatalog catalog(File dir) {
    JdbcCatalog c = new JdbcCatalog();
    Map<String, String> p = new LinkedHashMap<>();
    p.put(CatalogProperties.URI, FaultDriver.PREFIX + new File(dir, "cat.db").getAbsolutePath());
    p.put(CatalogProperties.WAREHOUSE_LOCATION, new File(dir, "wh").getAbsolutePath());
    p.put(CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName());
    p.put("jdbc.schema-version", "V1");
    c.initialize("r5", p);
    return c;
  }

  static DataFile dataFile(Table t, String name) throws IOException {
    File f = new File(LocalFileIO.strip(t.location()), "data/" + name + ".parquet");
    f.getParentFile().mkdirs();
    Files.write(f.toPath(), new byte[] {1, 2, 3, 4});
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(f.getAbsolutePath())
        .withFileSizeInBytes(4)
        .withRecordCount(10)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  static Map<String, Object> runOnce(String scenario, boolean fault, int i) throws Exception {
    File dir = new File(OUT, "work/" + scenario + "-" + (fault ? "fault" : "control") + "-" + i);
    deleteTree(dir);
    dir.mkdirs();
    FaultDriver.reset();
    LocalFileIO.DELETED.clear();
    Map<String, Object> r = new LinkedHashMap<>();

    JdbcCatalog cat = catalog(dir);
    cat.createNamespace(ID.namespace());
    Table t = cat.createTable(ID, SCHEMA, PartitionSpec.unpartitioned(),
        Map.of("format-version", "2", "commit.retry.min-wait-ms", "10"));
    DataFile a = dataFile(t, "A");
    t.newFastAppend().appendFile(a).commit(); // S0, never faulted
    DataFile b = dataFile(t, "B");
    String locBefore = ((HasTableOperations) t).operations().current().metadataFileLocation();
    FaultDriver.CAS_ROWS.clear();

    boolean writer = scenario.endsWith("-cw");
    String op = writer ? scenario.substring(0, scenario.length() - 3) : scenario;
    Map<String, Object> cRes = new LinkedHashMap<>();
    DataFile c = writer ? dataFile(t, "C") : null;
    if (writer) {
      // concurrent writer C commits from its own catalog, right after A's CAS UPDATE has executed
      // on the database and before A's client sees the result (fault or not).
      FaultDriver.HOOK = () -> {
        Thread th = new Thread(() -> {
          try (JdbcCatalog cc = catalog(dir)) {
            Table ct = cc.loadTable(ID);
            ct.newFastAppend().appendFile(c).commit();
            cRes.put("C", "success");
            cRes.put("Cid", ct.currentSnapshot().snapshotId());
          } catch (Exception e) {
            cRes.put("C", e.getClass().getSimpleName());
          }
        });
        th.start();
        try {
          th.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      };
    }
    long s0 = t.currentSnapshot().snapshotId();
    if (fault) {
      FaultDriver.ARMED.set(1);
    }
    Throwable seen = null;
    try {
      switch (op) {
        case "ops-direct": {
          TableOperations ops = ((HasTableOperations) t).operations();
          TableMetadata base = ops.current();
          TableMetadata upd = TableMetadata.buildFrom(base).setProperties(Map.of("r5", "v")).build();
          ops.commit(base, upd);
          break;
        }
        case "fast-append":
          t.newFastAppend().appendFile(b).commit();
          break;
        case "merge-append":
          t.newAppend().appendFile(b).commit();
          break;
        case "txn-fast-append": {
          Transaction txn = t.newTransaction();
          txn.newFastAppend().appendFile(b).commit();
          txn.commitTransaction();
          break;
        }
        case "overwrite":
          t.newOverwrite().deleteFile(a).addFile(b).commit();
          break;
        case "delete":
          t.newDelete().deleteFile(a).commit();
          break;
        case "add-column":
          t.updateSchema().addColumn("x", Types.IntegerType.get()).commit();
          break;
        default:
          throw new IllegalArgumentException("unknown scenario " + scenario);
      }
    } catch (RuntimeException e) {
      seen = e;
    }
    FaultDriver.ARMED.set(0);
    r.put("injected", FaultDriver.INJECTED.get());
    r.put("casRows", new ArrayList<>(FaultDriver.CAS_ROWS));
    r.put("hookRuns", FaultDriver.HOOK_RUNS.get());
    r.putAll(cRes);
    r.put("callerSaw", seen == null ? "success" : seen.getClass().getSimpleName());
    if (seen != null) {
      List<String> sup = new ArrayList<>();
      for (Throwable s : seen.getSuppressed()) {
        sup.add(s.getClass().getSimpleName());
      }
      r.put("suppressed", sup);
    }
    cat.close();

    // ---- inspect the final table through a fresh catalog, no faults armed
    JdbcCatalog fresh = catalog(dir);
    Table ft = fresh.loadTable(ID);
    TableMetadata md = ((HasTableOperations) ft).operations().current();
    FileIO io = ft.io();
    r.put("metadataChanged", !md.metadataFileLocation().equals(locBefore));
    List<String> hist = new ArrayList<>();
    List<String> deletedButReachable = new ArrayList<>();
    int snapshotsAddingB = 0;
    for (Snapshot s : md.snapshots()) {
      boolean mlExists = LocalFileIO.exists(s.manifestListLocation());
      if (!mlExists && LocalFileIO.DELETED.contains(LocalFileIO.strip(s.manifestListLocation()))) {
        deletedButReachable.add("manifestList(" + s.snapshotId() + ")");
      }
      String added = "?";
      if (mlExists) {
        for (ManifestFile m : s.allManifests(io)) {
          if (!LocalFileIO.exists(m.path()) && LocalFileIO.DELETED.contains(LocalFileIO.strip(m.path()))) {
            deletedButReachable.add("manifest(" + new File(m.path()).getName() + " in " + s.snapshotId() + ")");
          }
        }
        try {
          List<String> names = new ArrayList<>();
          for (DataFile df : s.addedDataFiles(io)) {
            names.add(new File(df.location()).getName());
            if (df.location().equals(b.location())) {
              snapshotsAddingB++;
            }
          }
          added = names.toString();
        } catch (RuntimeException e) {
          added = "unreadable:" + e.getClass().getSimpleName();
        }
      }
      hist.add(s.sequenceNumber() + ":" + s.operation() + (mlExists ? "" : "[ML-MISSING]") + "+" + added);
    }
    r.put("history", hist);
    if (op.equals("add-column")) {
      r.put("schemaHasX", md.schema().findField("x") != null);
    }
    if (writer && cRes.get("Cid") != null) {
      long cid = (Long) cRes.get("Cid");
      boolean onMain = false;
      for (Snapshot s = md.currentSnapshot(); s != null;
          s = s.parentId() == null ? null : md.snapshot(s.parentId())) {
        if (s.snapshotId() == cid) {
          onMain = true;
        }
      }
      r.put("CinSnapshots", md.snapshot(cid) != null);
      r.put("ConMain", onMain);
    }
    r.put("snapshotsAddingB", snapshotsAddingB);
    r.put("deletedButReachable", deletedButReachable);

    // live data-file multiplicity in the current snapshot, from manifest entries and from a scan
    Map<String, Integer> entryCount = new TreeMap<>();
    Map<String, Integer> scanCount = new TreeMap<>();
    long scanRecords = 0;
    String scanError = null;
    Snapshot cur = md.currentSnapshot();
    if (cur != null) {
      try {
        for (ManifestFile m : cur.allManifests(io)) {
          if (m.content() != ManifestContent.DATA) {
            continue;
          }
          try (CloseableIterable<DataFile> files = ManifestFiles.read(m, io, md.specsById())) {
            for (DataFile df : files) {
              entryCount.merge(new File(df.location()).getName(), 1, Integer::sum);
            }
          }
        }
        try (CloseableIterable<FileScanTask> tasks = ft.newScan().planFiles()) {
          for (FileScanTask task : tasks) {
            scanCount.merge(new File(task.file().location()).getName(), 1, Integer::sum);
            scanRecords += task.file().recordCount();
          }
        }
      } catch (RuntimeException | IOException e) {
        scanError = e.getClass().getSimpleName();
      }
    }
    r.put("liveEntries", entryCount);
    r.put("scanTasks", scanCount);
    r.put("scanRecords", scanRecords);
    if (scanError != null) {
      r.put("scanError", scanError);
    }
    // permanence: after a routine expiry that keeps only what main references, does C's file survive?
    if (writer && cRes.get("Cid") != null) {
      ft.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
      r.put("CfileAfterExpire", LocalFileIO.exists(c.location()) ? "exists" : "DELETED");
    }
    fresh.close();

    // ---- classify
    Map<String, Integer> exp = new TreeMap<>();
    switch (op) {
      case "ops-direct":
        exp.put("A.parquet", 1);
        break;
      case "overwrite":
        exp.put("B.parquet", 1);
        break;
      case "delete":
        break;
      case "add-column":
        exp.put("A.parquet", 1);
        break;
      default:
        exp.put("A.parquet", 1);
        exp.put("B.parquet", 1);
    }
    if (writer && "success".equals(cRes.get("C"))) {
      exp.put("C.parquet", 1);
    }
    String expectedLive = exp.toString();
    r.put("expectedLive", expectedLive);
    List<String> bad = new ArrayList<>();
    if (writer && "success".equals(cRes.get("C")) && !Boolean.TRUE.equals(r.get("ConMain"))) {
      bad.add("C_ACKED_BUT_NOT_ON_MAIN");
    }
    if (writer && FaultDriver.HOOK_RUNS.get() != 1) {
      throw new IllegalStateException("harness: writer hook did not run exactly once: " + r);
    }
    if (!deletedButReachable.isEmpty()) {
      bad.add("DELETED_REACHABLE");
    }
    if (scanError != null) {
      bad.add("SCAN_FAILS");
    }
    if (!scanCount.toString().equals(expectedLive) && scanError == null) {
      bad.add("SCAN_LIVE=" + scanCount);
    }
    if (!entryCount.toString().equals(expectedLive) && scanError == null) {
      bad.add("ENTRIES=" + entryCount);
    }
    if ("DELETED".equals(r.get("CfileAfterExpire"))) {
      bad.add("C_FILE_DELETED_BY_EXPIRY");
    }
    if (snapshotsAddingB > 1) {
      bad.add("B_ADDED_BY_" + snapshotsAddingB + "_SNAPSHOTS");
    }
    boolean landed = (Boolean) r.get("metadataChanged");
    String report = (seen == null ? "success" : seen.getClass().getSimpleName()) + (landed ? "/landed" : "/not-landed");
    r.put("outcome", report + (bad.isEmpty() ? "/ok" : "/" + String.join("+", bad)));
    return r;
  }

  static void deleteTree(File f) {
    File[] kids = f.listFiles();
    if (kids != null) {
      for (File k : kids) {
        deleteTree(k);
      }
    }
    f.delete();
  }
}
