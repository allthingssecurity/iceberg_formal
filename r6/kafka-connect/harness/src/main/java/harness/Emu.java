package harness;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.IcebergSinkTask;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Emulates the parts of the Kafka Connect runtime (WorkerSinkTask) the Iceberg sink depends on:
 * the source topic, the connect consumer group (cooperative / ICR), per-task fetch positions,
 * open/close/put/flush/stop callbacks in the order WorkerSinkTask issues them, task failure
 * (close(all) then stop()), and process crash (no callbacks at all, clients dead).
 */
public final class Emu {
  static final ThreadLocal<Integer> CUR = ThreadLocal.withInitial(() -> -1);

  static int currentTask() {
    return CUR.get();
  }

  public static final class CrashedProcess extends RuntimeException {
    CrashedProcess(String m) {
      super(m);
    }
  }

  /** What Admin.describeConsumerGroups returns while a close() callback runs. */
  public enum DescribeDuringClose {
    TARGET, // the final assignment of the rebalance (most favourable to the connector)
    PARTIAL, // broker generation in which the revoked partition is owned by nobody (cooperative 1st round)
    UNSTABLE; // group not Stable: classic coordinator returns members without assignments

    static DescribeDuringClose of(String s) {
      return valueOf(s.toUpperCase());
    }
  }

  public final String srcTopic = "src";
  public final String connectGroup;
  final int srcPartitions;
  final Map<Integer, Integer> produced = new TreeMap<>();
  final Map<Integer, Slot> slots = new TreeMap<>();
  final Map<String, String> baseProps;
  DescribeDuringClose describeMode = DescribeDuringClose.TARGET;
  int groupGeneration = 0;
  // view exposed through Admin while a callback runs (null -> live view)
  Map<Integer, Set<TopicPartition>> describeView = null;
  ConsumerGroupState describeState = ConsumerGroupState.STABLE;
  final List<String> trace = new ArrayList<>();
  final Map<String, Integer> failures = new LinkedHashMap<>();
  String firstFailure = null;

  final class Slot {
    final int id;
    IcebergSinkTask task;
    final Set<TopicPartition> assignment = new LinkedHashSet<>();
    final Map<TopicPartition, Long> pos = new HashMap<>();
    boolean crashed = false;
    boolean running = false;
    int incarnation = 0;
    WorkerSinkTaskContext ctx;

    Slot(int id) {
      this.id = id;
    }
  }

  Emu(int srcPartitions, Map<String, String> baseProps) {
    this.srcPartitions = srcPartitions;
    this.baseProps = baseProps;
    this.connectGroup = "connect-" + baseProps.get("name");
    for (int p = 0; p < srcPartitions; p++) {
      produced.put(p, 0);
    }
  }

  void log(String s) {
    if (trace.size() < 300) {
      trace.add(s);
    }
  }

  public TopicPartition tp(int p) {
    return new TopicPartition(srcTopic, p);
  }

  synchronized boolean isCrashed(int task) {
    Slot s = slots.get(task);
    return s != null && s.crashed;
  }

  // ------------------------------------------------------------------ source topic

  public void produce(int p, int n) {
    produced.merge(p, n, Integer::sum);
  }

  // ------------------------------------------------------------------ task lifecycle

  public void startTask(int id) {
    Slot s = slots.computeIfAbsent(id, Slot::new);
    if (s.running && !s.crashed) {
      throw new IllegalStateException("task " + id + " already running");
    }
    s.crashed = false;
    s.running = true;
    s.incarnation++;
    s.assignment.clear();
    s.pos.clear();
    s.task = new IcebergSinkTask();
    s.ctx = new Ctx(sourceConsumer(s));
    Map<String, String> props = new HashMap<>(baseProps);
    props.put("task.id", String.valueOf(id));
    props.put(IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP, "-txn-fixed-" + id);
    final Slot fs = s;
    call(s, "start", () -> {
      fs.task.initialize(fs.ctx);
      fs.task.start(props);
    });
    log("start task" + id + " #" + s.incarnation);
  }

  /** WorkerSinkTaskContext subclass: KafkaUtils reads its private 'consumer' field via DynFields. */
  static final class Ctx extends WorkerSinkTaskContext {
    Ctx(Consumer<byte[], byte[]> c) {
      super(c, null, null);
    }

    @Override
    public String toString() {
      return "EmuCtx";
    }
  }

  /** Incremental cooperative assignment of new partitions; WorkerSinkTask then calls open(). */
  public void assign(int id, int... ps) {
    Slot s = live(id);
    List<TopicPartition> added = new ArrayList<>();
    for (int p : ps) {
      TopicPartition tp = tp(p);
      for (Slot o : slots.values()) {
        if (o.running && !o.crashed && o.assignment.contains(tp)) {
          throw new IllegalStateException(tp + " still owned by task" + o.id);
        }
      }
      Long c = FakeKafka.K.committed(connectGroup, tp);
      s.pos.put(tp, c == null ? 0L : c); // sink connectors default to auto.offset.reset=earliest
      s.assignment.add(tp);
      added.add(tp);
    }
    groupGeneration++;
    log("assign task" + id + " " + added + " gen=" + groupGeneration);
    if (s.task == null) {
      return;
    }
    describeView = null;
    describeState = ConsumerGroupState.STABLE;
    call(s, "open", () -> s.task.open(added));
  }

  /** Initial assignment of a whole group in one rebalance: assignments first, then open()s. */
  public void assignAll(int[][] taskParts) {
    for (int[] tp : taskParts) {
      for (int i = 1; i < tp.length; i++) {
        Slot s = live(tp[0]);
        TopicPartition t = tp(tp[i]);
        Long c = FakeKafka.K.committed(connectGroup, t);
        s.pos.put(t, c == null ? 0L : c);
        s.assignment.add(t);
      }
    }
    groupGeneration++;
    for (int[] tp : taskParts) {
      Slot s = live(tp[0]);
      List<TopicPartition> added = new ArrayList<>();
      for (int i = 1; i < tp.length; i++) {
        added.add(tp(tp[i]));
      }
      log("assignAll task" + tp[0] + " " + added + " gen=" + groupGeneration);
      call(s, "open", () -> s.task.open(added));
    }
  }

  /** Cooperative revocation: close(revoked) runs while the partitions are still assigned. */
  public void revoke(int id, int... ps) {
    Slot s = live(id);
    List<TopicPartition> removed = new ArrayList<>();
    for (int p : ps) {
      if (!s.assignment.contains(tp(p))) {
        throw new IllegalStateException("task" + id + " does not own " + p);
      }
      removed.add(tp(p));
    }
    groupGeneration++;
    log("revoke task" + id + " " + removed + " gen=" + groupGeneration + " describe=" + describeMode);
    // view during the close() callback
    Map<Integer, Set<TopicPartition>> view = liveView();
    view.get(id).removeAll(removed);
    switch (describeMode) {
      case TARGET:
        describeView = view; // caller fixes up the receiver via pendingTarget
        if (pendingTargetTask >= 0) {
          view.get(pendingTargetTask).addAll(removed);
        }
        describeState = ConsumerGroupState.STABLE;
        break;
      case PARTIAL:
        describeView = view;
        describeState = ConsumerGroupState.STABLE;
        break;
      case UNSTABLE:
        Map<Integer, Set<TopicPartition>> empty = new TreeMap<>();
        view.keySet().forEach(k -> empty.put(k, new LinkedHashSet<>()));
        describeView = empty;
        describeState = ConsumerGroupState.PREPARING_REBALANCE;
        break;
      default:
        throw new IllegalStateException("unknown describe mode");
    }
    try {
      if (s.task != null) {
        call(s, "close", () -> s.task.close(removed));
      }
    } finally {
      describeView = null;
      describeState = ConsumerGroupState.STABLE;
      s.assignment.removeAll(removed);
      removed.forEach(s.pos::remove);
    }
  }

  int pendingTargetTask = -1;

  /** Moves partitions between tasks the way ICR does: revoke round, then assign round. */
  public void move(int from, int to, int... ps) {
    pendingTargetTask = to;
    try {
      revoke(from, ps);
    } finally {
      pendingTargetTask = -1;
    }
    assign(to, ps);
  }

  /** One WorkerSinkTask.poll iteration: up to max records per assigned partition, then put(). */
  public void poll(int id, int max) {
    Slot s = slots.get(id);
    if (s == null || !s.running || s.crashed) {
      return;
    }
    List<SinkRecord> batch = new ArrayList<>();
    for (TopicPartition tp : s.assignment) {
      long pos = s.pos.get(tp);
      long end = produced.get(tp.partition());
      long upto = Math.min(end, pos + max);
      for (long o = pos; o < upto; o++) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", (long) tp.partition() * 1_000_000L + o);
        v.put("p", (long) tp.partition());
        v.put("o", o);
        batch.add(
            new SinkRecord(
                srcTopic, tp.partition(), null, null, null, v, o, 1_000L + o, TimestampType.CREATE_TIME));
      }
      s.pos.put(tp, upto);
    }
    call(s, "put", () -> s.task.put(batch));
  }

  public void pollAll(int max) {
    for (int id : new ArrayList<>(slots.keySet())) {
      poll(id, max);
    }
  }

  /** Task failure as WorkerSinkTask handles it: close(all assigned), stop(), leave the group. */
  public void failTask(int id) {
    Slot s = live(id);
    List<TopicPartition> all = new ArrayList<>(s.assignment);
    groupGeneration++;
    try {
      try {
        call(s, "close", () -> s.task.close(all));
      } catch (TaskFailed ignored) {
        // already failing: WorkerSinkTask logs and goes on to stop()
      }
      call(s, "stop", () -> s.task.stop());
    } finally {
      s.running = false;
      s.assignment.clear();
      s.pos.clear();
      log("failTask task" + id + " released " + all);
    }
  }

  /** Graceful stop (rebalance away or connector stop): identical callbacks to failTask. */
  public void stopTask(int id) {
    failTask(id);
  }

  /** Called from a fault hook on another thread: from now on the task's clients are dead. */
  public synchronized void markCrashed(int id) {
    Slot s = slots.get(id);
    s.crashed = true;
    pendingCrash = id;
  }

  int pendingCrash = -1;

  /** Completes a crash flagged by a fault hook (runs on the scheduler thread). */
  public void finishPendingCrash() {
    int id;
    synchronized (this) {
      id = pendingCrash;
      pendingCrash = -1;
    }
    if (id >= 0) {
      Slot s = slots.get(id);
      s.crashed = false; // let crash() do the bookkeeping
      crash(id);
    }
  }

  /** Process crash: no callbacks, the task's Kafka clients and coordinator thread are dead. */
  public void crash(int id) {
    Slot s = live(id);
    synchronized (this) {
      s.crashed = true;
    }
    s.running = false;
    log("CRASH task" + id + " (held " + s.assignment + ")");
    s.assignment.clear();
    s.pos.clear();
    FakeKafka.K.expireMembersOf(id);
    groupGeneration++;
  }

  Slot live(int id) {
    Slot s = slots.get(id);
    if (s == null || !s.running || s.crashed) {
      throw new IllegalStateException("task " + id + " not running");
    }
    return s;
  }

  public boolean isRunning(int id) {
    Slot s = slots.get(id);
    return s != null && s.running && !s.crashed;
  }

  interface Body {
    void run() throws Exception;
  }

  /** Calls into the task on behalf of task id; exceptions mark the task failed (as Connect does). */
  void call(Slot s, String what, Body b) {
    Integer prev = CUR.get();
    CUR.set(s.id);
    try {
      b.run();
    } catch (Exception e) {
      Throwable t = FakeKafka.unwrap(e);
      String k = what + ":" + t.getClass().getSimpleName();
      failures.merge(k, 1, Integer::sum);
      log("task" + s.id + " " + what + " threw " + t.getClass().getName() + ": " + t.getMessage());
      if (firstFailure == null) {
        java.io.StringWriter sw = new java.io.StringWriter();
        t.printStackTrace(new java.io.PrintWriter(sw));
        firstFailure = sw.toString().lines().limit(25).reduce("", (x, y) -> x + "\n      " + y);
      }
      if (what.equals("put") || what.equals("open") || what.equals("close")) {
        // WorkerSinkTask: put() failures, and open()/close() failures (stored as
        // rebalanceException and rethrown from poll), fail the task -> close(all) + stop()
        throw new TaskFailed(s.id, t);
      }
    } finally {
      CUR.set(prev);
    }
  }

  public static final class TaskFailed extends RuntimeException {
    public final int task;

    TaskFailed(int task, Throwable cause) {
      super("task " + task + " failed: " + cause, cause);
      this.task = task;
    }
  }

  Map<Integer, Set<TopicPartition>> liveView() {
    Map<Integer, Set<TopicPartition>> m = new TreeMap<>();
    for (Slot s : slots.values()) {
      if (s.running && !s.crashed) {
        m.put(s.id, new LinkedHashSet<>(s.assignment));
      }
    }
    return m;
  }

  ConsumerGroupDescription describeConnectGroup(String id) {
    if (!id.equals(connectGroup)) {
      throw new UnsupportedOperationException("describe of unknown group " + id);
    }
    Map<Integer, Set<TopicPartition>> view = describeView != null ? describeView : liveView();
    Map<String, Set<TopicPartition>> members = new LinkedHashMap<>();
    view.forEach((k, v) -> members.put("member-task" + k, v));
    return FakeKafka.description(id, members, describeState);
  }

  void checkGeneration(ConsumerGroupMetadata gm) {
    if (!gm.groupId().equals(connectGroup)) {
      throw new UnsupportedOperationException("txn offsets for unknown group " + gm.groupId());
    }
    String mid = gm.memberId();
    int task = Integer.parseInt(mid.substring("member-task".length(), mid.indexOf('#')));
    Slot s = slots.get(task);
    if (s == null || !s.running || s.crashed || !mid.endsWith("#" + s.incarnation)) {
      throw new CommitFailedException();
    }
  }

  /** The framework's KafkaConsumer for the source topic, as far as the sink touches it. */
  @SuppressWarnings("unchecked")
  Consumer<byte[], byte[]> sourceConsumer(Slot s) {
    InvocationHandler h =
        (proxy, m, a) -> {
          switch (m.getName()) {
            case "assignment":
              return new LinkedHashSet<>(s.assignment);
            case "committed":
              {
                if (a.length != 1 || !(a[0] instanceof Set)) {
                  throw new UnsupportedOperationException("committed variant " + m);
                }
                Map<TopicPartition, OffsetAndMetadata> r = new HashMap<>();
                for (TopicPartition tp : (Set<TopicPartition>) a[0]) {
                  Long c = FakeKafka.K.committed(connectGroup, tp);
                  r.put(tp, c == null ? null : new OffsetAndMetadata(c));
                }
                return r;
              }
            case "seek":
              {
                if (!(a[1] instanceof Long)) {
                  throw new UnsupportedOperationException("seek variant " + m);
                }
                TopicPartition tp = (TopicPartition) a[0];
                if (!s.assignment.contains(tp)) {
                  throw new IllegalStateException("No current assignment for partition " + tp);
                }
                log("seek task" + s.id + " " + tp + " -> " + a[1] + " (was " + s.pos.get(tp) + ")");
                s.pos.put(tp, (Long) a[1]);
                return null;
              }
            case "groupMetadata":
              return new ConsumerGroupMetadata(
                  connectGroup,
                  groupGeneration,
                  "member-task" + s.id + "#" + s.incarnation,
                  Optional.empty());
            case "toString":
              return "EmuSourceConsumer[task" + s.id + "]";
            case "hashCode":
              return System.identityHashCode(proxy);
            case "equals":
              return proxy == a[0];
            default:
              throw new UnsupportedOperationException("source consumer: " + m);
          }
        };
    return (Consumer<byte[], byte[]>)
        Proxy.newProxyInstance(Emu.class.getClassLoader(), new Class<?>[] {Consumer.class}, h);
  }

  Collection<Slot> slots() {
    return slots.values();
  }
}
