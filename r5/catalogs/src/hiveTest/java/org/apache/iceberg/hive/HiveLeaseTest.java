package org.apache.iceberg.hive;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.ql.txn.AcidHouseKeeperService;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

/**
 * C-3 / C-8: Hive lock lease without fencing, on a real embedded HMS 2.3.10 (Iceberg's own
 * TestHiveMetastore from the 1.12.0 tests jar) with Hive's own AcidHouseKeeperService expiring
 * locks.
 *
 * <p>Writer A (catalog name procA, its own JVM lock) stalls inside persistTable's alter_table call.
 * Writer B (catalog name procB) commits an append meanwhile. Then A resumes.
 *
 * <p>-Dr5.mode=<comma list of MODES>|all -Dr5.runs=N
 */
public class HiveLeaseTest {
  static final List<String> MODES =
      List.of("stall-hbpaused", "stall-hbpaused-joint", "stall-hblive", "nostall", "nolock-stall-hbpaused");
  static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  static final File OUT = new File("build/r5");
  static final long TXN_TIMEOUT_MS = 3000;
  static final long MAX_STALL_MS = 9000;

  static List<String> modes() {
    String m = System.getProperty("r5.mode", "all");
    if (m.equals("all")) {
      return MODES;
    }
    List<String> out = new ArrayList<>();
    for (String name : m.split(",")) {
      if (!MODES.contains(name)) {
        throw new IllegalArgumentException("unknown r5.mode: " + name + " (known " + MODES + ")");
      }
      out.add(name);
    }
    return out;
  }

  /** Per-run control of writer A's metastore client. */
  static final class Gate {
    volatile boolean stallAlter;
    volatile boolean pauseHeartbeats;
    final CountDownLatch reachedAlter = new CountDownLatch(1);
    final CountDownLatch releaseAlter = new CountDownLatch(1);
    final CountDownLatch releaseHeartbeats = new CountDownLatch(1);
    final AtomicInteger alterCalls = new AtomicInteger();
    final AtomicInteger heartbeatsOk = new AtomicInteger();
    final AtomicInteger heartbeatsFailed = new AtomicInteger();
    final AtomicInteger heartbeatsHeld = new AtomicInteger();
    final Set<Long> lockIds = ConcurrentHashMap.newKeySet();
    final List<String> events = java.util.Collections.synchronizedList(new ArrayList<>());
    static final long T0 = System.currentTimeMillis();
    static final Set<String> LOGGED = Set.of("lock", "checkLock", "unlock", "showLocks", "heartbeat");

    void log(String n, Object res) {
      if ((LOGGED.contains(n) || n.startsWith("alter_table")) && events.size() < 60) {
        events.add((System.currentTimeMillis() - T0) + ":" + n + "=" + res);
      }
    }
  }

  /** ClientPool that hands A's actions a proxy client controlled by a Gate. */
  static final class GatedPool implements ClientPool<IMetaStoreClient, TException> {
    final ClientPool<IMetaStoreClient, TException> delegate;
    final Gate gate;

    GatedPool(ClientPool<IMetaStoreClient, TException> delegate, Gate gate) {
      this.delegate = delegate;
      this.gate = gate;
    }

    IMetaStoreClient wrap(IMetaStoreClient real) {
      return (IMetaStoreClient)
          Proxy.newProxyInstance(
              HiveLeaseTest.class.getClassLoader(),
              new Class<?>[] {IMetaStoreClient.class},
              (proxy, m, args) -> {
                String n = m.getName();
                if (n.startsWith("alter_table")) {
                  gate.alterCalls.incrementAndGet();
                  if (gate.stallAlter) {
                    gate.stallAlter = false;
                    gate.reachedAlter.countDown();
                    gate.releaseAlter.await();
                  }
                } else if (n.equals("heartbeat")) {
                  if (gate.pauseHeartbeats) {
                    gate.heartbeatsHeld.incrementAndGet();
                    gate.releaseHeartbeats.await();
                  }
                }
                try {
                  Object r = m.invoke(real, args);
                  if (n.equals("heartbeat")) {
                    gate.heartbeatsOk.incrementAndGet();
                  }
                  if (r instanceof LockResponse) {
                    gate.lockIds.add(((LockResponse) r).getLockid());
                    gate.log(n, ((LockResponse) r).getLockid() + "/" + ((LockResponse) r).getState());
                  } else {
                    gate.log(n, "ok");
                  }
                  return r;
                } catch (InvocationTargetException e) {
                  gate.log(n, e.getCause().getClass().getSimpleName());
                  if (n.equals("heartbeat")) {
                    gate.heartbeatsFailed.incrementAndGet();
                  }
                  throw e.getCause();
                }
              });
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
      return delegate.run(c -> action.run(wrap(c)));
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
        throws TException, InterruptedException {
      return delegate.run(c -> action.run(wrap(c)), retry);
    }
  }

  static HiveConf conf(HiveConf base, boolean lockEnabled) {
    HiveConf c = new HiveConf(base);
    c.set("iceberg.engine.hive.lock-enabled", String.valueOf(lockEnabled));
    c.setLong("iceberg.hive.lock-heartbeat-interval-ms", 1000); // first beat 500 ms, then every 1 s
    c.setLong("iceberg.hive.lock-timeout-ms", 30_000);
    c.setLong("iceberg.hive.lock-check-min-wait-ms", 50);
    c.setLong("iceberg.hive.lock-check-max-wait-ms", 300);
    return c;
  }

  static DataFile file(String dir, String name) throws Exception {
    File f = new File(dir, name + ".parquet");
    f.getParentFile().mkdirs();
    Files.write(f.toPath(), new byte[] {1, 2, 3, 4});
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(f.getAbsolutePath()).withFileSizeInBytes(4).withRecordCount(10)
        .withFormat(FileFormat.PARQUET).build();
  }

  @Test
  public void run() throws Exception {
    int runs = Integer.parseInt(System.getProperty("r5.runs", "5"));
    OUT.mkdirs();
    HiveConf base = new HiveConf(TestHiveMetastore.class);
    base.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TXN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    base.setTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_START, 0, TimeUnit.MILLISECONDS);
    base.setTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_INTERVAL, 250, TimeUnit.MILLISECONDS);
    TestHiveMetastore hms = new TestHiveMetastore();
    hms.start(base, 8, true);
    HiveConf clientConf = hms.hiveConf();

    // Hive's own lock reaper, as an HMS with hive.compactor.initiator.on runs it, on the same Derby DB.
    Field dp = TestHiveMetastore.class.getDeclaredField("DERBY_PATH");
    dp.setAccessible(true);
    HiveConf hk = new HiveConf(base);
    hk.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:derby:" + dp.get(null) + ";create=true");
    hk.set("datanucleus.connectionPoolingType", "DBCP");
    // the housekeeper only starts when the configured txn manager supports ACID (as on an ACID HMS)
    hk.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    hk.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    AcidHouseKeeperService reaper = new AcidHouseKeeperService();
    reaper.start(hk);
    Thread.sleep(1500);
    if (reaper.getIsAliveCounter() == Integer.MIN_VALUE) {
      throw new IllegalStateException("harness: Hive lock reaper is not running: " + reaper.getIsAliveCounter());
    }

    HiveMetaStoreClient admin = new HiveMetaStoreClient(clientConf);
    String dataDir = new File(OUT, "hive-data").getAbsolutePath();
    try (PrintWriter sum = new PrintWriter(new File(OUT, "hive-summary.txt"));
        PrintWriter detail = new PrintWriter(new File(OUT, "hive-detail.txt"))) {
      sum.printf("hive.txn.timeout=%dms reaper=250ms heartbeat-interval=1000ms maxStall=%dms%n",
          TXN_TIMEOUT_MS, MAX_STALL_MS);
      boolean nsCreated = false;
      for (String mode : modes()) {
        Map<String, Integer> outcomes = new TreeMap<>();
        for (int i = 0; i < runs; i++) {
          boolean lockEnabled = !mode.startsWith("nolock");
          HiveConf cA = conf(clientConf, lockEnabled);
          HiveConf cB = conf(clientConf, lockEnabled);
          HiveCatalog catB = (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "procB",
              Map.of(CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS, "600000"), cB);
          HiveCatalog catA = (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "procA",
              Map.of(CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS, "600000"), cA);
          if (!nsCreated) {
            catB.createNamespace(Namespace.of("db"));
            nsCreated = true;
          }
          String tname = mode.replace('-', '_') + "_" + i;
          TableIdentifier id = TableIdentifier.of("db", tname);
          Table setup = catB.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
              Map.of("format-version", "2", "commit.retry.min-wait-ms", "10"));
          setup.newFastAppend().appendFile(file(dataDir + "/" + tname, "S0")).commit();

          // A and B model two processes: each gets its own HMS client pool (the catalogs' CachedClientPool
          // would share one static pool in this JVM) and its own JVM lock (different catalog names).
          HiveClientPool poolA = new HiveClientPool(4, cA);
          HiveClientPool poolB = new HiveClientPool(4, cB);
          Gate gate = new Gate();
          HiveTableOperations opsA = new HiveTableOperations(cA, new GatedPool(poolA, gate),
              new HadoopFileIO(cA), null, "procA", "db", tname);
          Table tA = new BaseTable(opsA, "procA.db." + tname);
          Gate gateB = new Gate();
          HiveTableOperations opsB = new HiveTableOperations(cB, new GatedPool(poolB, gateB),
              new HadoopFileIO(cB), null, "procB", "db", tname);
          Table tB = new BaseTable(opsB, "procB.db." + tname);
          DataFile fa = file(dataDir + "/" + tname, "A");
          DataFile fb = file(dataDir + "/" + tname, "B");
          boolean stall = !mode.equals("nostall");
          gate.stallAlter = stall;
          gate.pauseHeartbeats = mode.contains("hbpaused");
          boolean jointResume = mode.endsWith("-joint");

          Map<String, Object> r = new LinkedHashMap<>();
          long[] aSnap = {-1};
          long[] bSnap = {-1};
          Thread a = new Thread(() -> {
            try {
              tA.newFastAppend().appendFile(fa).commit();
              r.put("A", "success");
            } catch (RuntimeException e) {
              r.put("A", e.getClass().getSimpleName());
            }
            try {
              for (Snapshot s : opsA.refresh().snapshots()) {
                if (s.addedDataFiles(opsA.io()).iterator().next().location().equals(fa.location())) {
                  aSnap[0] = s.snapshotId();
                }
              }
            } catch (RuntimeException e) {
              // A's snapshot might be absent; leave -1
            }
          });
          Thread b = new Thread(() -> {
            try {
              tB.newFastAppend().appendFile(fb).commit();
              r.put("B", "success");
              bSnap[0] = tB.currentSnapshot().snapshotId();
            } catch (RuntimeException e) {
              r.put("B", e.getClass().getSimpleName());
            }
          });
          long t0 = System.currentTimeMillis();
          a.start();
          if (stall) {
            if (!gate.reachedAlter.await(60, TimeUnit.SECONDS)) {
              throw new IllegalStateException("harness: A never reached alter_table");
            }
            r.put("AlocksHeldAtStall", new ArrayList<>(gate.lockIds));
            b.start();
            b.join(MAX_STALL_MS);
            r.put("BdoneWhileAStalled", !b.isAlive());
            Set<Long> live = ConcurrentHashMap.newKeySet();
            for (ShowLocksResponseElement e : admin.showLocks(new ShowLocksRequest()).getLocks()) {
              live.add(e.getLockid());
            }
            boolean aLockLive = false;
            for (Long l : gate.lockIds) {
              aLockLive |= live.contains(l);
            }
            r.put("AlockLiveAtResume", aLockLive);
            r.put("reaperAlive", reaper.getIsAliveCounter());
            r.put("locksSeen", live.size());
            for (ShowLocksResponseElement e : admin.showLocks(new ShowLocksRequest()).getLocks()) {
              r.put("lock" + e.getLockid(), e.getState() + "/hb=" + (System.currentTimeMillis() - e.getLastheartbeat()) + "ms-ago/txn=" + e.getTxnid());
            }
            r.put("stallMs", System.currentTimeMillis() - t0);
            if (jointResume) {
              gate.releaseHeartbeats.countDown(); // the pause ends for both of A's threads at once
            }
            gate.releaseAlter.countDown();
            a.join();
            gate.releaseHeartbeats.countDown();
            b.join();
          } else {
            a.join();
            b.start();
            b.join();
          }
          gate.releaseHeartbeats.countDown();
          r.put("eventsA", gate.events);
          r.put("eventsB", gateB.events);
          r.put("alterCalls", gate.alterCalls.get());
          r.put("hbHeld", gate.heartbeatsHeld.get());
          r.put("hbOk", gate.heartbeatsOk.get());
          r.put("hbFailed", gate.heartbeatsFailed.get());

          // final state, through a fresh catalog
          HiveCatalog check = (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "check",
              Map.of(), conf(clientConf, true));
          Table ft = check.loadTable(id);
          TableMetadata md = ((BaseTable) ft).operations().current();
          List<Long> main = new ArrayList<>();
          for (Snapshot s = md.currentSnapshot(); s != null;
              s = s.parentId() == null ? null : md.snapshot(s.parentId())) {
            main.add(s.snapshotId());
          }
          Map<String, Integer> live = new TreeMap<>();
          try (CloseableIterable<FileScanTask> tasks = ft.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
              live.merge(new File(task.file().location()).getName(), 1, Integer::sum);
            }
          }
          r.put("live", live);
          boolean aOnMain = aSnap[0] != -1 && main.contains(aSnap[0]);
          boolean bOnMain = bSnap[0] != -1 && main.contains(bSnap[0]);
          r.put("AonMain", aOnMain);
          r.put("BonMain", bOnMain);
          List<String> bad = new ArrayList<>();
          if ("success".equals(r.get("B")) && !bOnMain) {
            bad.add("B_ACKED_BUT_LOST");
          }
          if ("success".equals(r.get("A")) && !aOnMain) {
            bad.add("A_ACKED_BUT_LOST");
          }
          String outcome = "A=" + r.get("A") + ",B=" + r.get("B")
              + (stall ? ",BdoneDuringStall=" + r.get("BdoneWhileAStalled") + ",AlockLive=" + r.get("AlockLiveAtResume") : "")
              + "," + (bad.isEmpty() ? "ok" : String.join("+", bad));
          r.put("outcome", outcome);
          outcomes.merge(outcome, 1, Integer::sum);
          if (i == 0) {
            detail.println(mode + " run0: " + r);
          }
          detail.flush();
          check.close();
          poolA.close();
          poolB.close();
          catA.close();
          catB.close();
        }
        sum.printf("%-24s runs=%d outcomes=%s%n", mode, runs, outcomes);
        sum.flush();
      }
    } finally {
      reaper.stop();
      admin.close();
      hms.stop();
    }
  }
}
