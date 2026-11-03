package harness;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.utils.Utils;

/**
 * A single-process stand-in for the Kafka broker, as seen by the Iceberg sink's control channel.
 *
 * <p>Modelled: the control topic as an append-only log per partition, read_committed transactions
 * (records and offsets become visible atomically on commitTransaction, are discarded on abort),
 * transactional.id fencing by epoch, consumer groups with committed offsets and EAGER rebalancing
 * (the default RangeAssignor: every membership change revokes all partitions and members restart
 * from the group's committed offsets), auto.offset.reset, KIP-447 generation checks on
 * TxnOffsetCommit for the connect (source) group.
 *
 * <p>Every Producer/Consumer/Admin method the connector calls is implemented; anything else
 * throws UnsupportedOperationException (unknown operations never fall back silently).
 */
public final class FakeKafka {
  public static volatile FakeKafka K;

  final String controlTopic;
  final int ctlPartitions;
  final Map<Integer, List<ConsumerRecord<String, byte[]>>> log = new TreeMap<>();
  final Map<String, Map<TopicPartition, Long>> groupOffsets = new HashMap<>();
  final Map<String, Integer> txnEpoch = new HashMap<>();
  final Map<String, CtlGroup> ctlGroups = new HashMap<>();
  final List<Gate> gates = new ArrayList<>();
  final Emu emu;
  int memberSeq = 0;
  // counters for the report
  int txnCommits = 0;
  int txnAborts = 0;
  final List<String> events = new ArrayList<>();

  /** producer-side fault: fail the n-th commitTransaction of producers whose txId contains match */
  public String producerFaultMatch = null;
  public int producerFaultAtCommit = -1;
  int producerCommitsSeen = 0;

  FakeKafka(String controlTopic, int ctlPartitions, Emu emu) {
    this.controlTopic = controlTopic;
    this.ctlPartitions = ctlPartitions;
    this.emu = emu;
    for (int p = 0; p < ctlPartitions; p++) {
      log.put(p, new ArrayList<>());
    }
  }

  synchronized void note(String s) {
    if (events.size() < 400) {
      events.add(s);
    }
  }

  synchronized long endOffset(int p) {
    return log.get(p).size();
  }

  synchronized Long committed(String group, TopicPartition tp) {
    Map<TopicPartition, Long> m = groupOffsets.get(group);
    return m == null ? null : m.get(tp);
  }

  synchronized void commitGroupOffsets(String group, Map<TopicPartition, OffsetAndMetadata> offs) {
    Map<TopicPartition, Long> m = groupOffsets.computeIfAbsent(group, g -> new HashMap<>());
    offs.forEach((tp, om) -> m.put(tp, om.offset()));
  }

  // ------------------------------------------------------------------ producer

  @SuppressWarnings("unchecked")
  public Producer<String, byte[]> newProducer(String txId) {
    int epoch;
    synchronized (this) {
      epoch = txnEpoch.merge(txId, 1, Integer::sum);
      note("initTransactions " + txId + " epoch=" + epoch);
    }
    FakeProducer fp = new FakeProducer(txId, epoch, Emu.currentTask());
    return (Producer<String, byte[]>)
        Proxy.newProxyInstance(
            FakeKafka.class.getClassLoader(), new Class<?>[] {Producer.class}, fp);
  }

  public static final class InjectedProducerFault extends org.apache.kafka.common.KafkaException {
    public InjectedProducerFault(String m) {
      super(m);
    }
  }

  final class FakeProducer implements InvocationHandler {
    final String txId;
    final int epoch;
    final int ownerTask;
    boolean inTxn = false;
    boolean closed = false;
    final List<ProducerRecord<String, byte[]>> pending = new ArrayList<>();
    final Map<String, Map<TopicPartition, OffsetAndMetadata>> pendingOffsets = new LinkedHashMap<>();
    final Map<String, ConsumerGroupMetadata> pendingMeta = new HashMap<>();

    FakeProducer(String txId, int epoch, int ownerTask) {
      this.txId = txId;
      this.epoch = epoch;
      this.ownerTask = ownerTask;
    }

    void checkEpoch() {
      synchronized (FakeKafka.this) {
        if (txnEpoch.get(txId) != epoch) {
          throw new ProducerFencedException("fake: " + txId + " epoch " + epoch + " fenced");
        }
      }
      if (closed) {
        throw new IllegalStateException("fake: producer closed");
      }
      if (emu.isCrashed(ownerTask)) {
        throw new Emu.CrashedProcess("producer of crashed task " + ownerTask);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, java.lang.reflect.Method m, Object[] a) throws Throwable {
      switch (m.getName()) {
        case "initTransactions":
          return null; // already done at creation
        case "beginTransaction":
          checkEpoch();
          if (inTxn) {
            throw new IllegalStateException("fake: txn already open");
          }
          inTxn = true;
          return null;
        case "send":
          {
            checkEpoch();
            if (!inTxn) {
              throw new IllegalStateException("fake: send outside txn");
            }
            ProducerRecord<String, byte[]> r = (ProducerRecord<String, byte[]>) a[0];
            if (!r.topic().equals(controlTopic)) {
              throw new UnsupportedOperationException("fake: unknown topic " + r.topic());
            }
            pending.add(r);
            int p = partitionFor(r.key());
            RecordMetadata md = new RecordMetadata(new TopicPartition(r.topic(), p), -1, 0, 0L, 0, 0);
            return CompletableFuture.completedFuture(md);
          }
        case "sendOffsetsToTransaction":
          {
            checkEpoch();
            if (!inTxn) {
              throw new IllegalStateException("fake: offsets outside txn");
            }
            if (!(a[1] instanceof ConsumerGroupMetadata)) {
              throw new UnsupportedOperationException("fake: sendOffsetsToTransaction(String)");
            }
            ConsumerGroupMetadata gm = (ConsumerGroupMetadata) a[1];
            pendingOffsets
                .computeIfAbsent(gm.groupId(), g -> new LinkedHashMap<>())
                .putAll((Map<TopicPartition, OffsetAndMetadata>) a[0]);
            pendingMeta.put(gm.groupId(), gm);
            return null;
          }
        case "commitTransaction":
          {
            checkEpoch();
            if (!inTxn) {
              throw new IllegalStateException("fake: commit outside txn");
            }
            try {
              synchronized (FakeKafka.this) {
                if (producerFaultMatch != null && txId.contains(producerFaultMatch)) {
                  producerCommitsSeen++;
                  if (producerCommitsSeen == producerFaultAtCommit) {
                    note("INJECT producer commit fault txId=" + txId);
                    throw new InjectedProducerFault("injected commitTransaction failure " + txId);
                  }
                }
                // KIP-447: group generation check for the source group
                for (Map.Entry<String, ConsumerGroupMetadata> e : pendingMeta.entrySet()) {
                  emu.checkGeneration(e.getValue());
                }
                for (ProducerRecord<String, byte[]> r : pending) {
                  int p = partitionFor(r.key());
                  List<ConsumerRecord<String, byte[]>> l = log.get(p);
                  l.add(new ConsumerRecord<>(controlTopic, p, l.size(), r.key(), r.value()));
                }
                pendingOffsets.forEach(FakeKafka.this::commitGroupOffsets);
                txnCommits++;
                FakeKafka.this.notifyAll();
              }
            } finally {
              pending.clear();
              pendingOffsets.clear();
              pendingMeta.clear();
              inTxn = false;
            }
            return null;
          }
        case "abortTransaction":
          checkEpoch();
          synchronized (FakeKafka.this) {
            txnAborts++;
          }
          pending.clear();
          pendingOffsets.clear();
          pendingMeta.clear();
          inTxn = false;
          return null;
        case "flush":
          return null;
        case "close":
          closed = true;
          return null;
        case "toString":
          return "FakeProducer[" + txId + "#" + epoch + "]";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == a[0];
        default:
          throw new UnsupportedOperationException("fake producer: " + m);
      }
    }
  }

  int partitionFor(String key) {
    byte[] kb = key.getBytes(StandardCharsets.UTF_8);
    return Utils.toPositive(Utils.murmur2(kb)) % ctlPartitions;
  }

  // ------------------------------------------------------------------ control-topic consumer

  /** Consumer group over the control topic (coordinator group or a worker's private group). */
  final class CtlGroup {
    final String id;
    final List<FakeConsumer> members = new ArrayList<>();
    int generation = 0;

    CtlGroup(String id) {
      this.id = id;
    }

    /** eager rebalance: everyone loses everything; ranges are re-dealt; positions refetched. */
    void rebalance() {
      generation++;
      int n = members.size();
      for (int i = 0; i < n; i++) {
        FakeConsumer c = members.get(i);
        List<Integer> mine = new ArrayList<>();
        for (int p = 0; p < ctlPartitions; p++) {
          if (p % n == i) {
            mine.add(p);
          }
        }
        c.pendingAssignment = mine;
        c.pendingGeneration = generation;
      }
      note("ctl-group " + id + " rebalance gen=" + generation + " members=" + n);
    }
  }

  public enum Reset {
    LATEST,
    EARLIEST;

    public static Reset of(String s) {
      switch (s) {
        case "latest":
          return LATEST;
        case "earliest":
          return EARLIEST;
        default:
          throw new IllegalArgumentException("unknown auto.offset.reset " + s);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public Consumer<String, byte[]> newConsumer(String groupId, Reset reset) {
    boolean gated = groupId.endsWith("-coord");
    FakeConsumer fc = new FakeConsumer(groupId, reset, gated, Emu.currentTask());
    return (Consumer<String, byte[]>)
        Proxy.newProxyInstance(
            FakeKafka.class.getClassLoader(), new Class<?>[] {Consumer.class}, fc);
  }

  /** Scheduler gate on a coordinator consumer: an empty poll parks until the scheduler steps it. */
  public final class Gate {
    final int ownerTask;
    final FakeConsumer consumer;
    Thread thread;
    boolean idle = false;
    int permits = 0;
    boolean dead = false;

    Gate(int ownerTask, FakeConsumer consumer) {
      this.ownerTask = ownerTask;
      this.consumer = consumer;
    }

    boolean ownerTerminated() {
      Thread t = thread;
      if (t == null) {
        return false;
      }
      if (!t.isAlive()) {
        return true;
      }
      try {
        java.lang.reflect.Field f = t.getClass().getDeclaredField("terminated");
        f.setAccessible(true);
        return (Boolean) f.get(t);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new IllegalStateException("cannot read CoordinatorThread.terminated", e);
      }
    }

    public boolean alive() {
      return thread != null && thread.isAlive() && !dead;
    }

    public String toString() {
      return "gate(task" + ownerTask + "," + consumer.memberId + ")";
    }
  }

  final class FakeConsumer implements InvocationHandler {
    final String groupId;
    final Reset reset;
    final boolean gated;
    final int ownerTask;
    final String memberId = "m" + (++memberSeq);
    final Map<Integer, Long> positions = new HashMap<>();
    List<Integer> assignment = new ArrayList<>();
    int generation = -1;
    List<Integer> pendingAssignment = null;
    int pendingGeneration = -1;
    boolean subscribed = false;
    boolean closed = false;
    Gate gate;

    FakeConsumer(String groupId, Reset reset, boolean gated, int ownerTask) {
      this.groupId = groupId;
      this.reset = reset;
      this.gated = gated;
      this.ownerTask = ownerTask;
      if (gated) {
        // registered at construction (task thread) so the scheduler waits for the
        // CoordinatorThread to reach its first poll before it does anything else
        synchronized (FakeKafka.this) {
          gate = new Gate(ownerTask, this);
          gates.add(gate);
        }
      }
    }

    /** applies a pending eager rebalance at poll time (as the real client does). */
    void maybeRejoin() {
      synchronized (FakeKafka.this) {
        if (pendingAssignment != null) {
          assignment = pendingAssignment;
          generation = pendingGeneration;
          pendingAssignment = null;
          positions.clear(); // eager: fetch positions are discarded with the revocation
          for (int p : assignment) {
            Long c = committed(groupId, new TopicPartition(controlTopic, p));
            positions.put(p, c != null ? c : (reset == Reset.EARLIEST ? 0L : endOffset(p)));
          }
          note(
              "join "
                  + groupId
                  + " "
                  + memberId
                  + " task"
                  + ownerTask
                  + " gen="
                  + generation
                  + " pos="
                  + positions);
        }
      }
    }

    ConsumerRecords<String, byte[]> fetch() {
      synchronized (FakeKafka.this) {
        maybeRejoin();
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> out = new HashMap<>();
        for (int p : assignment) {
          long pos = positions.get(p);
          List<ConsumerRecord<String, byte[]>> l = log.get(p);
          if (pos < l.size()) {
            out.put(new TopicPartition(controlTopic, p), new ArrayList<>(l.subList((int) pos, l.size())));
            positions.put(p, (long) l.size());
          }
        }
        return new ConsumerRecords<>(out);
      }
    }

    ConsumerRecords<String, byte[]> poll() throws InterruptedException {
      if (closed) {
        throw new IllegalStateException("fake: consumer closed");
      }
      if (emu.isCrashed(ownerTask)) {
        throw new Emu.CrashedProcess("consumer of crashed task " + ownerTask);
      }
      ConsumerRecords<String, byte[]> r = fetch();
      if (!r.isEmpty() || !gated) {
        return r;
      }
      // coordinator: park until stepped (or until the CoordinatorThread is terminated)
      Gate g = gate;
      synchronized (FakeKafka.this) {
        if (g.thread == null) {
          g.thread = Thread.currentThread();
        }
        g.idle = true;
        FakeKafka.this.notifyAll();
        while (g.permits == 0) {
          if (g.ownerTerminated() || closed) {
            g.idle = false;
            return ConsumerRecords.empty();
          }
          if (emu.isCrashed(ownerTask)) {
            // a crashed process never runs again: park forever (frozen zombie), but stay
            // interruptible so the JVM can exit
            g.dead = true;
            FakeKafka.this.wait(50);
            continue;
          }
          FakeKafka.this.wait(50);
        }
        g.permits--;
        g.idle = false;
      }
      return fetch();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, java.lang.reflect.Method m, Object[] a) throws Throwable {
      switch (m.getName()) {
        case "subscribe":
          {
            if (!(a[0] instanceof Collection) || a.length != 1) {
              throw new UnsupportedOperationException("fake consumer: subscribe variant " + m);
            }
            Collection<String> topics = (Collection<String>) a[0];
            if (topics.size() != 1 || !topics.contains(controlTopic)) {
              throw new UnsupportedOperationException("fake consumer: topics " + topics);
            }
            synchronized (FakeKafka.this) {
              subscribed = true;
              CtlGroup g = ctlGroups.computeIfAbsent(groupId, CtlGroup::new);
              g.members.add(this);
              g.rebalance();
            }
            return null;
          }
        case "poll":
          if (!(a[0] instanceof Duration)) {
            throw new UnsupportedOperationException("fake consumer: poll(long)");
          }
          try {
            return poll();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new org.apache.kafka.common.errors.InterruptException(e);
          }
        case "commitSync":
          {
            if (a == null || a.length != 1 || !(a[0] instanceof Map)) {
              throw new UnsupportedOperationException("fake consumer: commitSync variant " + m);
            }
            if (emu.isCrashed(ownerTask)) {
              throw new Emu.CrashedProcess("commitSync of crashed task " + ownerTask);
            }
            synchronized (FakeKafka.this) {
              CtlGroup g = ctlGroups.get(groupId);
              if (pendingAssignment != null || g == null || g.generation != generation) {
                note("commitSync REJECTED (stale generation) " + groupId + " " + memberId);
                throw new CommitFailedException();
              }
              commitGroupOffsets(groupId, (Map<TopicPartition, OffsetAndMetadata>) a[0]);
              note("commitSync " + groupId + " task" + ownerTask + " " + a[0]);
            }
            return null;
          }
        case "groupMetadata":
          synchronized (FakeKafka.this) {
            return new ConsumerGroupMetadata(groupId, generation, memberId, Optional.empty());
          }
        case "close":
          synchronized (FakeKafka.this) {
            if (!closed) {
              closed = true;
              leave();
            }
          }
          return null;
        case "toString":
          return "FakeConsumer[" + groupId + "/" + memberId + "]";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == a[0];
        default:
          throw new UnsupportedOperationException("fake consumer: " + m);
      }
    }

    void leave() {
      CtlGroup g = ctlGroups.get(groupId);
      if (g != null && g.members.remove(this)) {
        note("leave " + groupId + " " + memberId + " task" + ownerTask);
        if (!g.members.isEmpty()) {
          g.rebalance();
        }
      }
      if (gate != null) {
        gate.dead = true;
      }
    }
  }

  /** session timeout of a crashed process: its control-topic memberships expire. */
  synchronized void expireMembersOf(int task) {
    for (CtlGroup g : ctlGroups.values()) {
      for (FakeConsumer c : new ArrayList<>(g.members)) {
        if (c.ownerTask == task) {
          c.leave();
        }
      }
    }
  }

  // ------------------------------------------------------------------ scheduler interface

  /** Waits until every live coordinator gate is parked (or its thread is gone). */
  public synchronized void awaitQuiescent() throws InterruptedException {
    long deadline = System.currentTimeMillis() + 20_000;
    while (true) {
      boolean ok = true;
      for (Gate g : gates) {
        if (g.dead) {
          continue;
        }
        if (g.thread == null) {
          // constructed, thread not yet at its first poll
          if (!g.consumer.closed) {
            ok = false;
          }
          continue;
        }
        if (g.thread.isAlive() && !g.idle) {
          ok = false;
        }
      }
      if (ok) {
        return;
      }
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("coordinator did not park within 20s: " + gates);
      }
      wait(20);
    }
  }

  /** Lets the coordinator of a task run one poll-to-poll slice. Returns false if there is none. */
  public boolean step(int task) throws InterruptedException {
    awaitQuiescent();
    Gate target = null;
    synchronized (this) {
      for (Gate g : gates) {
        if (g.ownerTask == task && g.alive() && !emu.isCrashed(task) && g.idle) {
          target = g;
        }
      }
      if (target == null) {
        return false;
      }
      target.permits++;
      steppingTask = task;
      notifyAll();
      // wait until it has taken the permit
      long deadline = System.currentTimeMillis() + 20_000;
      while (target.permits > 0 && target.thread.isAlive()) {
        if (System.currentTimeMillis() > deadline) {
          throw new IllegalStateException("permit not taken " + target);
        }
        wait(20);
      }
    }
    awaitQuiescent();
    steppingTask = -1;
    return true;
  }

  public volatile int steppingTask = -1;

  public synchronized List<Gate> liveGates() {
    List<Gate> l = new ArrayList<>();
    for (Gate g : gates) {
      if (g.alive()) {
        l.add(g);
      }
    }
    return l;
  }

  // ------------------------------------------------------------------ admin

  public Admin newAdmin() {
    InvocationHandler h =
        (proxy, m, a) -> {
          switch (m.getName()) {
            case "describeConsumerGroups":
              {
                Collection<String> ids = (Collection<String>) a[0];
                Map<String, KafkaFuture<ConsumerGroupDescription>> res = new HashMap<>();
                for (String id : ids) {
                  res.put(id, KafkaFuture.completedFuture(emu.describeConnectGroup(id)));
                }
                return new DescribeConsumerGroupsResult(res);
              }
            case "close":
              return null;
            case "toString":
              return "FakeAdmin";
            case "hashCode":
              return System.identityHashCode(proxy);
            case "equals":
              return proxy == a[0];
            default:
              throw new UnsupportedOperationException("fake admin: " + m);
          }
        };
    return (Admin)
        Proxy.newProxyInstance(FakeKafka.class.getClassLoader(), new Class<?>[] {Admin.class}, h);
  }

  static ConsumerGroupDescription description(
      String id, Map<String, Set<TopicPartition>> members, ConsumerGroupState state) {
    List<MemberDescription> ms = new ArrayList<>();
    members.forEach(
        (mid, tps) -> ms.add(new MemberDescription(mid, mid, "host", new MemberAssignment(tps))));
    return new ConsumerGroupDescription(id, false, ms, "cooperative-sticky", state, null);
  }

  static Throwable unwrap(Throwable t) {
    while (t instanceof InvocationTargetException) {
      t = ((InvocationTargetException) t).getTargetException();
    }
    return t;
  }
}
