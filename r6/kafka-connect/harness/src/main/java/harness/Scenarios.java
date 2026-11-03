package harness;

import java.util.List;
import java.util.Map;
import java.util.Set;

final class Scenarios {
  static final Set<String> KNOWN_OPTS = Set.of(
      "verbose", "n", "describe", "precommit", "reset", "kind", "count", "tables", "replay", "eager");

  static void check(Map<String, String> o) {
    for (String k : o.keySet()) {
      if (!KNOWN_OPTS.contains(k)) {
        throw new IllegalArgumentException("unknown option " + k);
      }
    }
  }

  static World world(Map<String, String> o, int src, int ctl, List<String> tables, Map<String, String> extra) {
    check(o);
    World w = new World(src, ctl, tables, extra);
    w.emu.describeMode = Emu.DescribeDuringClose.of(o.getOrDefault("describe", "target"));
    return w;
  }

  /** Control: 2 tasks, 2 partitions, no faults, no rebalances. Must be exactly-once. */
  static World smoke(Map<String, String> o) {
    int n = Main.intOpt(o, "n", 10);
    World w = world(o, 2, 1, List.of("db.tbl"), Map.of());
    Emu e = w.emu;
    w.safe(() -> e.startTask(0));
    w.safe(() -> e.startTask(1));
    w.safe(() -> e.assignAll(new int[][] {{0, 0}, {1, 1}}));
    for (int i = 0; i < 3; i++) {
      e.produce(0, n);
      e.produce(1, n);
      w.safe(() -> e.pollAll(1_000_000));
      w.cycle();
    }
    w.heal(6);
    return w;
  }

  /**
   * #12372 calibration (ICR, close without open). T0 owns P0,P1 and is the coordinator. P1 moves
   * to T1 (close(T0,[P1]) then open(T1,[P1])); T0 keeps consuming P0; then P1 moves back
   * (close(T1,[P1]), open(T0,[P1])) and more data arrives. Pre-fix (1.8.1) close() tears down
   * T0's committer and put() drops P0 records silently until the next open().
   */
  static World icrClose(Map<String, String> o) {
    int n = Main.intOpt(o, "n", 10);
    World w = world(o, 2, 1, List.of("db.tbl"), Map.of());
    Emu e = w.emu;
    w.safe(() -> e.startTask(0));
    w.safe(() -> e.startTask(1));
    w.safe(() -> e.assign(0, 0, 1));
    e.produce(0, n);
    e.produce(1, n);
    w.safe(() -> e.pollAll(1_000_000));
    w.cycle();
    w.safe(() -> e.move(0, 1, 1));
    e.produce(0, n);
    e.produce(1, n);
    w.safe(() -> e.pollAll(1_000_000));
    w.cycle();
    w.safe(() -> e.move(1, 0, 1));
    e.produce(0, n);
    e.produce(1, n);
    w.safe(() -> e.pollAll(1_000_000));
    w.heal(6);
    return w;
  }

  /**
   * ICR rebalance before the first commit that covers a partition. T0 owns P0,P1 and has buffered
   * records for both; P1 moves to T1. close(T0,[P1]) stops T0's worker (buffer discarded) and seeks
   * the retained P0 back to its committed offset -- but P0 has no committed offset yet.
   * precommit=1 (control): one full commit cycle first, so P0 has a committed offset.
   */
  static World icrNoCommit(Map<String, String> o) {
    int n = Main.intOpt(o, "n", 10);
    boolean precommit = Main.intOpt(o, "precommit", 0) == 1;
    World w = world(o, 2, 1, List.of("db.tbl"), Map.of());
    Emu e = w.emu;
    w.safe(() -> e.startTask(0));
    w.safe(() -> e.startTask(1));
    w.safe(() -> e.assign(0, 0, 1));
    if (precommit) {
      e.produce(0, n);
      e.produce(1, n);
      w.safe(() -> e.pollAll(1_000_000));
      w.cycle();
    }
    e.produce(0, n);
    e.produce(1, n);
    w.safe(() -> e.pollAll(1_000_000)); // T0 buffers P0 and P1, no StartCommit seen yet
    if (Main.intOpt(o, "eager", 0) == 1) {
      // control: eager protocol -- everything is revoked, then the new assignment is opened and
      // fetch positions come from committed offsets (auto.offset.reset=earliest when none)
      // eager: the group is not Stable while partitions are revoked
      e.describeMode = Emu.DescribeDuringClose.UNSTABLE;
      w.safe(() -> e.revoke(0, 0, 1));
      e.describeMode = Emu.DescribeDuringClose.TARGET;
      if (e.isRunning(0)) {
        w.safe(() -> e.assignAll(new int[][] {{0, 0}, {1, 1}}));
      }
    } else {
      w.safe(() -> e.move(0, 1, 1));
    }
    e.produce(0, n);
    e.produce(1, n);
    w.safe(() -> e.pollAll(1_000_000));
    w.heal(6);
    return w;
  }

  /**
   * Coordinator failover before the coordinator group ever committed a control-topic offset.
   * Workers answer StartCommit (source offsets committed in the same Kafka txn as DataWritten);
   * the coordinator task crashes before consuming the answers; P0 moves to T1 whose new
   * coordinator subscribes with auto.offset.reset=latest (KafkaClientFactory putIfAbsent).
   * precommit=1 (control): one full commit cycle first. reset=earliest: user override.
   */
  static World coordLatest(Map<String, String> o) {
    int n = Main.intOpt(o, "n", 10);
    boolean precommit = Main.intOpt(o, "precommit", 0) == 1;
    Map<String, String> extra = new java.util.HashMap<>();
    if (o.containsKey("reset")) {
      extra.put("iceberg.kafka.auto.offset.reset", o.get("reset"));
    }
    World w = world(o, 2, 1, List.of("db.tbl"), extra);
    Emu e = w.emu;
    w.safe(() -> e.startTask(0));
    w.safe(() -> e.startTask(1));
    w.safe(() -> e.assignAll(new int[][] {{0, 0}, {1, 1}}));
    if (precommit) {
      e.produce(0, n);
      e.produce(1, n);
      w.safe(() -> e.pollAll(1_000_000));
      w.cycle();
    }
    e.produce(0, n);
    e.produce(1, n);
    w.safe(() -> e.pollAll(1_000_000)); // workers exist and buffer data
    w.stepAll(); // coordinator: StartCommit
    w.safe(() -> e.pollAll(1_000_000)); // workers answer: DataWritten + DataComplete + src offsets
    w.safe(() -> e.crash(0)); // coordinator task dies before consuming the answers
    w.safe(() -> e.assign(1, 0)); // P0 -> T1: T1 becomes leader, new coordinator
    w.heal(6);
    return w;
  }

  /**
   * Catalog fault on the next table commit after `precommit` healthy cycles. kind = a
   * SharedCatalog.Kind, count = consecutive failing commit attempts (SnapshotProducer retries
   * CommitFailedException up to 4 times, so CFE needs count>=5 to surface). tables=2 routes every
   * record to db.a and db.b (multi-table commit, stopOnFailure). Failed tasks are restarted by the
   * operator in heal(), orphaned partitions reassigned.
   */
  static World fault(Map<String, String> o) {
    int n = Main.intOpt(o, "n", 10);
    int pre = Main.intOpt(o, "precommit", 1);
    int count = Main.intOpt(o, "count", 1);
    int nt = Main.intOpt(o, "tables", 1);
    String kind = o.getOrDefault("kind", "RTE_BEFORE");
    List<String> tables = nt == 1 ? List.of("db.tbl") : List.of("db.a", "db.b");
    World w = world(o, 2, 1, tables, Map.of());
    Emu e = w.emu;
    w.safe(() -> e.startTask(0));
    w.safe(() -> e.startTask(1));
    w.safe(() -> e.assignAll(new int[][] {{0, 0}, {1, 1}}));
    for (int i = 0; i < pre; i++) {
      e.produce(0, n);
      e.produce(1, n);
      w.safe(() -> e.pollAll(1_000_000));
      w.cycle();
    }
    w.fault(kind, org.apache.iceberg.inmemory.SharedCatalog.commitAttempts + 1, count);
    e.produce(0, n);
    e.produce(1, n);
    w.safe(() -> e.pollAll(1_000_000));
    w.cycle();
    w.safe(() -> e.pollAll(1_000_000));
    w.cycle();
    e.produce(0, n);
    e.produce(1, n);
    w.heal(6);
    return w;
  }

  /**
   * #16282 path: split-brain coordinator + eager rewind of the zombie's control-topic consumer.
   * T0{P0}=C, T1{P1}, T2{P2}. One healthy commit. Commit c2 starts; T0 and T1 answer, C consumes
   * both answers (2/3 DataComplete). P0 flaps to T1 while Admin.describe returns the partial
   * (cooperative first-round) assignment, so close(T0,[P0]) does not stop C; open(T1,[P0]) starts
   * C' which joins <group>-coord: eager rebalance, C's fetch position goes back to the committed
   * offset. C re-reads its uncommitted answers. replay=0 is the control (no second coordinator:
   * describe=target, the leader moves cleanly).
   */
  static World zombieRewind(Map<String, String> o) {
    int n = Main.intOpt(o, "n", 10);
    boolean replay = Main.intOpt(o, "replay", 1) == 1;
    World w = world(o, 3, 1, List.of("db.tbl"), Map.of());
    Emu e = w.emu;
    for (int t = 0; t < 3; t++) {
      final int ft = t;
      w.safe(() -> e.startTask(ft));
    }
    w.safe(() -> e.assignAll(new int[][] {{0, 0}, {1, 1}, {2, 2}}));
    for (int p = 0; p < 3; p++) {
      e.produce(p, n);
    }
    w.safe(() -> e.pollAll(1_000_000));
    w.cycle();
    for (int p = 0; p < 3; p++) {
      e.produce(p, n);
    }
    // cycle() ends with StartCommit(c2) already on the control topic
    w.safe(() -> e.poll(0, 1_000_000)); // T0 buffers and answers c2
    w.safe(() -> e.poll(1, 1_000_000)); // T1 buffers and answers c2
    w.step(0); // C consumes DW0 DC0 DW1 DC1: 2/3
    e.describeMode = replay ? Emu.DescribeDuringClose.PARTIAL : Emu.DescribeDuringClose.TARGET;
    w.safe(() -> e.move(0, 1, 0)); // zombie C stays (replay=1); C' starts on T1
    e.describeMode = Emu.DescribeDuringClose.TARGET;
    w.step(0); // C (if alive) rejoins after the eager rebalance and re-reads
    w.safe(() -> e.pollAll(1_000_000)); // T2 answers c2 (and whatever StartCommit came after)
    w.stepAll();
    w.safe(() -> e.move(1, 0, 0)); // flap back: C' stopped, C rewinds again
    w.stepAll();
    for (int p = 0; p < 3; p++) {
      e.produce(p, n);
    }
    w.heal(6);
    return w;
  }
}
