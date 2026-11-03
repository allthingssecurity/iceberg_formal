package races;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.iceberg.LockManager;

/**
 * B-19 / B-20 / B-18. Drives the real org.apache.iceberg.util.LockManagers$InMemoryLockManager
 * (built through its package-private Map constructor, since lock-impl cannot load it) from T
 * threads over K entity ids and checks mutual exclusion directly with a per-key occupancy counter.
 *
 * <p>While inside the critical section each thread samples LOCKS.get(key) (reflection on the real
 * static map) and classifies what it sees structurally, by owner identity:
 *
 * <ul>
 *   <li>self: expected
 *   <li>null: my entry was removed by someone else's release() while I hold it (B-20 signature;
 *       only release() and close() remove entries, and close() is never called here)
 *   <li>exited owner: an owner whose release() had already returned is the recorded owner, i.e.
 *       its heartbeat rewrote ownership after it released (B-19 signature)
 *   <li>live other: another owner currently inside took the lock (lease expiry steal)
 * </ul>
 *
 * <p>args: config threads keys seconds holdMaxMicros
 */
public class LockRace {
  static Field locksField;
  static Method ownerIdM;
  static Method expireMsM;
  static Field heartbeatsField;

  public static void main(String[] args) throws Exception {
    String config = args[0];
    int threads = Integer.parseInt(args[1]);
    int keys = Integer.parseInt(args[2]);
    int seconds = Integer.parseInt(args[3]);
    int holdMaxMicros = Integer.parseInt(args[4]);

    long hbInterval;
    long hbTimeout;
    switch (config) {
      case "prod": // the defaults HadoopCatalog/HadoopTables always get (3s / 15s)
        hbInterval = 3000;
        hbTimeout = 15000;
        break;
      case "fast": // short but consistent: interval well below timeout
        hbInterval = 2;
        hbTimeout = 20;
        break;
      case "mixed": // odd threads use a stalled-heartbeat manager, even threads a healthy one
      case "stall": // heartbeat later than the timeout: models a heartbeat stalled past timeout
        hbInterval = 200;
        hbTimeout = 5;
        break;
      default:
        throw new IllegalArgumentException("unknown config " + config);
    }
    Map<String, String> props =
        Map.of(
            "lock.acquire-interval-ms", "1",
            "lock.acquire-timeout-ms", "20000",
            "lock.heartbeat-interval-ms", String.valueOf(hbInterval),
            "lock.heartbeat-timeout-ms", String.valueOf(hbTimeout),
            "lock.heartbeat-threads", "4");

    Class<?> cls = Class.forName("org.apache.iceberg.util.LockManagers$InMemoryLockManager");
    Constructor<?> ctor = cls.getDeclaredConstructor(Map.class);
    ctor.setAccessible(true);
    LockManager mgr = (LockManager) ctor.newInstance(props);
    // healthy manager for even threads in "mixed"; same static LOCKS/HEARTBEATS as every instance
    Map<String, String> healthyProps = new java.util.HashMap<>(props);
    healthyProps.put("lock.heartbeat-interval-ms", "2");
    healthyProps.put("lock.heartbeat-timeout-ms", "50");
    LockManager healthy = (LockManager) ctor.newInstance(healthyProps);
    boolean mixed = config.equals("mixed");
    locksField = cls.getDeclaredField("LOCKS");
    locksField.setAccessible(true);
    heartbeatsField = cls.getDeclaredField("HEARTBEATS");
    heartbeatsField.setAccessible(true);
    Class<?> content = Class.forName("org.apache.iceberg.util.LockManagers$InMemoryLockContent");
    ownerIdM = content.getDeclaredMethod("ownerId");
    ownerIdM.setAccessible(true);
    expireMsM = content.getDeclaredMethod("expireMs");
    expireMsM.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> locks = (Map<String, Object>) locksField.get(null);

    AtomicInteger[] occupancy = new AtomicInteger[keys];
    for (int i = 0; i < keys; i++) {
      occupancy[i] = new AtomicInteger();
    }
    Set<String> inside = ConcurrentHashMap.newKeySet();
    List<Set<String>> occupants = new ArrayList<>();
    for (int i = 0; i < keys; i++) {
      occupants.add(ConcurrentHashMap.newKeySet());
    }
    AtomicLong violationsAmongHealthy = new AtomicLong();
    AtomicLong healthyHoldsWithNull = new AtomicLong();
    Set<String> exited = ConcurrentHashMap.newKeySet(); // release() returned
    Set<String> releasing = ConcurrentHashMap.newKeySet(); // left the section, release() running
    AtomicLong acquisitions = new AtomicLong();
    AtomicLong acquireFalse = new AtomicLong();
    AtomicLong violations = new AtomicLong();
    AtomicInteger maxOccupancy = new AtomicInteger();
    AtomicLong releaseFalse = new AtomicLong();
    AtomicLong sampleSelf = new AtomicLong();
    AtomicLong sampleNull = new AtomicLong();
    AtomicLong sampleExitedOwner = new AtomicLong();
    AtomicLong sampleLiveOther = new AtomicLong();
    AtomicLong sampleUnknown = new AtomicLong();
    AtomicLong holdsWithNull = new AtomicLong();
    AtomicLong holdsWithExited = new AtomicLong();
    AtomicLong holdsWithLiveOther = new AtomicLong();
    // violations where the earlier occupant's entry was gone/foreign at the time the newcomer
    // entered: attributes each violation to the state the newcomer observed
    AtomicLong violationsNewcomerSawSelf = new AtomicLong();
    AtomicLong errors = new AtomicLong();

    long deadline = System.nanoTime() + seconds * 1_000_000_000L;
    CyclicBarrier barrier = new CyclicBarrier(threads);
    List<Thread> ts = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int tid = t;
      Thread th =
          new Thread(
              () -> {
                try {
                  barrier.await();
                  long seq = 0;
                  while (System.nanoTime() < deadline) {
                    int k = ThreadLocalRandom.current().nextInt(keys);
                    String key = "entity-" + k;
                    boolean isHealthy = mixed && tid % 2 == 0;
                    LockManager m = isHealthy ? healthy : mgr;
                    String owner = (isHealthy ? "h" : "s") + tid + "-" + (seq++);
                    if (!m.acquire(key, owner)) {
                      acquireFalse.incrementAndGet();
                      continue;
                    }
                    acquisitions.incrementAndGet();
                    inside.add(owner);
                    occupants.get(k).add(owner);
                    int occ = occupancy[k].incrementAndGet();
                    if (occ > 1 && mixed) {
                      boolean allHealthy = true;
                      for (String o : occupants.get(k)) {
                        allHealthy &= o.startsWith("h");
                      }
                      if (allHealthy && occupants.get(k).size() > 1) {
                        violationsAmongHealthy.incrementAndGet();
                      }
                    }
                    maxOccupancy.accumulateAndGet(occ, Math::max);
                    boolean sawNull = false;
                    boolean sawExited = false;
                    boolean sawLive = false;
                    if (occ > 1) {
                      violations.incrementAndGet();
                      if (owner.equals(ownerOf(locks.get(key)))) {
                        violationsNewcomerSawSelf.incrementAndGet();
                      }
                    }
                    long holdNanos =
                        holdMaxMicros == 0
                            ? 0
                            : ThreadLocalRandom.current().nextLong(holdMaxMicros) * 1000L;
                    long until = System.nanoTime() + holdNanos;
                    do {
                      String o = ownerOf(locks.get(key));
                      if (o == null) {
                        sampleNull.incrementAndGet();
                        sawNull = true;
                      } else if (o.equals(owner)) {
                        sampleSelf.incrementAndGet();
                      } else if (exited.contains(o)) {
                        sampleExitedOwner.incrementAndGet();
                        sawExited = true;
                      } else if (inside.contains(o) || releasing.contains(o)) {
                        sampleLiveOther.incrementAndGet();
                        sawLive = true;
                      } else {
                        sampleUnknown.incrementAndGet();
                      }
                      if (holdNanos > 0) {
                        LockSupport.parkNanos(Math.min(200_000L, holdNanos));
                      }
                    } while (System.nanoTime() < until);
                    if (sawNull) {
                      holdsWithNull.incrementAndGet();
                    }
                    if (sawExited) {
                      holdsWithExited.incrementAndGet();
                    }
                    if (sawLive) {
                      holdsWithLiveOther.incrementAndGet();
                    }
                    if (sawNull && owner.startsWith("h")) {
                      healthyHoldsWithNull.incrementAndGet();
                    }
                    occupants.get(k).remove(owner);
                    occupancy[k].decrementAndGet();
                    releasing.add(owner);
                    inside.remove(owner);
                    if (!m.release(key, owner)) {
                      releaseFalse.incrementAndGet();
                    }
                    exited.add(owner);
                    releasing.remove(owner);
                  }
                } catch (Throwable e) {
                  errors.incrementAndGet();
                }
              });
      ts.add(th);
      th.start();
    }
    for (Thread th : ts) {
      th.join();
    }
    // after all owners released: wait past the timeout and look for entries that are still live
    Thread.sleep(Math.max(3 * hbTimeout, 3 * hbInterval) + 100);
    long now = System.currentTimeMillis();
    int liveLeft = 0;
    for (Object c : locks.values()) {
      if ((Long) expireMsM.invoke(c) > now) {
        liveLeft++;
      }
    }
    @SuppressWarnings("unchecked")
    Map<String, ?> hbs = (Map<String, ?>) heartbeatsField.get(null);
    System.out.printf(
        "RESULT version=%s config=%s hbInterval=%d hbTimeout=%d threads=%d keys=%d seconds=%d holdMaxMicros=%d "
            + "acquisitions=%d acquireFalse=%d violations=%d maxOccupancy=%d violationsWhereNewcomerOwnedEntry=%d "
            + "releaseFalse=%d samples[self=%d null=%d exitedOwner=%d liveOther=%d unknown=%d] "
            + "holdsWith[null=%d exitedOwner=%d liveOther=%d] afterQuiesce[locksEntries=%d liveEntries=%d heartbeatsMap=%d] errors=%d"
            + (mixed ? " mixed[violationsAmongHealthyOnly=" + violationsAmongHealthy.get()
                + " healthyHoldsThatSawTheirEntryRemoved=" + healthyHoldsWithNull.get() + "]" : "")
            + "%n",
        System.getProperty("icebergVersion"), config, hbInterval, hbTimeout, threads, keys, seconds,
        holdMaxMicros, acquisitions.get(), acquireFalse.get(), violations.get(), maxOccupancy.get(),
        violationsNewcomerSawSelf.get(), releaseFalse.get(), sampleSelf.get(), sampleNull.get(),
        sampleExitedOwner.get(), sampleLiveOther.get(), sampleUnknown.get(), holdsWithNull.get(),
        holdsWithExited.get(), holdsWithLiveOther.get(), locks.size(), liveLeft, hbs.size(),
        errors.get());
    System.exit(0);
  }

  static String ownerOf(Object content) {
    if (content == null) {
      return null;
    }
    try {
      return (String) ownerIdM.invoke(content);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
