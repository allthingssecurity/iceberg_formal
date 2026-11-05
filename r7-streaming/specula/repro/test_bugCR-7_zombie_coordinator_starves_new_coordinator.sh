#!/usr/bin/env bash
# =================================================================================================
# CR-7 reproduction: a terminated Iceberg sink coordinator never leaves Channel.consumeAvailable while
# control-topic traffic keeps arriving (< 1 s gaps). The zombie keeps the "-coord" partition, so the
# newly elected coordinator receives no control events and the table commits stall.
#
# Escalation level 0/1 (black box). Real in-JVM Kafka KRaft broker + a real Kafka Connect worker
# (connect-runtime EmbeddedConnectCluster). The UNMODIFIED sink, compiled from `git show HEAD:` (the
# TLA instrumentation in the worktree is NOT used). Only public operations are used: the Connect REST
# API (create / restart?includeTasks=true / stop / resume / delete connector), Kafka producers feeding
# the source topics, and read-only observation (Kafka Admin, Iceberg catalog, JVM thread liveness and
# stacks). Nothing is added to or changed in the system under test.
#
# Environment (not sink logic):
#  * control topic "control-iceberg" = the sink's DEFAULT (IcebergSinkConfig.java:101), not configured
#    on any connector, created with 1 partition as in docs/docs/kafka-connect.md.
#  * N unrelated Iceberg sink connectors ("neighbour-i") share that default control topic and commit
#    every NB_INTERVAL ms. They supply the "control records keep arriving" premise of the finding.
#  * a shared in-memory Iceberg catalog (InMemoryCatalog routed to one root so all tasks see the
#    same tables).
#  * victim: tasks.max=2; its coordinator is handed over with a REST restart of connector + tasks.
#
# Usage:  [ATTEMPTS=N] test_bugCR-7_zombie_coordinator_starves_new_coordinator.sh \
#           [range|dense|lowlatency|sticky|worstcase|injected]
#   range       1 neighbour @500 ms, victim interval 5 s / timeout 2 s, Kafka default assignor (range)
#   dense       3 neighbours @500 ms, victim 5 s / 2 s
#   lowlatency  3 neighbours @500 ms, victim interval 1 s / timeout 500 ms (sub-second timeout)
#   sticky      as range, plus iceberg.kafka.partition.assignment.strategy=CooperativeStickyAssignor
#   worstcase   3 neighbours @500 ms, victim 1 s / 500 ms, CooperativeStickyAssignor (most favourable
#               legal configuration for the finding)
#   injected    LEVEL 2, mask removed: no neighbour connectors; a plain producer injects valid Iceberg
#               control events (StartCommit of an unrelated group) every 200 ms with no 1 s gap. No
#               real Iceberg peer emits such ungated traffic. This shows what the defect does if the
#               gaps are absent, and how a healthy (non-terminated) coordinator behaves under the
#               same traffic.
# Exit code 0 = stall triggered (all checks hold); 1 = not triggered; >1 = harness error.
# =================================================================================================
set -euo pipefail

MODE="${1:-range}"
case "$MODE" in
  range)      NB=1; NBI=500; VI=5000; VT=2000; ST=false ;;
  dense)      NB=3; NBI=500; VI=5000; VT=2000; ST=false ;;
  lowlatency) NB=3; NBI=500; VI=1000; VT=500;  ST=false ;;
  sticky)     NB=1; NBI=500; VI=5000; VT=2000; ST=true ;;
  worstcase)  NB=3; NBI=500; VI=1000; VT=500;  ST=true ;;
  injected)   NB=0; NBI=500; VI=5000; VT=2000; ST=false; INJ=true ;;
  *) echo "unknown mode $MODE"; exit 2 ;;
esac
INJ="${INJ:-false}"
ATTEMPTS="${ATTEMPTS:-10}"
OUT="/Users/I074560/Downloads/experiments/specula/runs/20261104-163223-c58c/iceberg-kafka-connect/.specula-output"
WT="$OUT/confirmation/CR-7/worktree"
WORK="$OUT/confirmation/CR-7/repro-build"
REPRO_DIR="$OUT/repro"
CP_FILE="$WORK/classpath.txt"
SRC_DIR="$WORK/src"
PRISTINE="$WORK/pristine"
CLASSES="$WORK/classes"
SINK_LOG="$REPRO_DIR/CR-7.$MODE.sink.log"
mkdir -p "$WORK" "$SRC_DIR/cr7"

# ---- 1. dependency classpath (Gradle, offline). The module's own classes are excluded on purpose ----
if [ ! -s "$CP_FILE" ]; then
  cat > "$WORK/cr7-init.gradle" <<'EOF'
allprojects { p ->
  if (p.path == ':iceberg-kafka-connect:iceberg-kafka-connect') {
    p.afterEvaluate {
      p.configurations.create('cr7Repro') { extendsFrom p.configurations.testRuntimeClasspath }
      def kv = '3.9.2'
      p.dependencies {
        cr7Repro "org.apache.kafka:kafka-clients:${kv}"
        cr7Repro "org.apache.kafka:connect-api:${kv}"
        cr7Repro "org.apache.kafka:connect-json:${kv}"
        cr7Repro "org.apache.kafka:connect-runtime:${kv}"
        cr7Repro "org.apache.kafka:connect-runtime:${kv}:test"
        cr7Repro "org.apache.kafka:kafka-clients:${kv}:test"
        cr7Repro "org.apache.kafka:kafka_2.13:${kv}"
        cr7Repro "org.apache.kafka:kafka_2.13:${kv}:test"
        cr7Repro "org.apache.kafka:kafka-server-common:${kv}:test"
      }
      p.tasks.register('cr7PrintClasspath') {
        def cfg = p.configurations.cr7Repro
        dependsOn cfg
        doLast {
          def out = new File(System.getProperty('cr7.cp.out'))
          out.text = cfg.files.collect { it.absolutePath }.join(File.pathSeparator)
        }
      }
    }
  }
}
EOF
  (cd "$WT" && timeout 1500 ./gradlew --offline --console=plain -q -I "$WORK/cr7-init.gradle" \
      -Dcr7.cp.out="$CP_FILE" :iceberg-kafka-connect:iceberg-kafka-connect:cr7PrintClasspath)
fi
CP="$(cat "$CP_FILE")"

# ---- 2. pristine sink sources from git HEAD (no instrumentation) ----
rm -rf "$PRISTINE" "$CLASSES"; mkdir -p "$PRISTINE" "$CLASSES"
git -C "$WT" archive HEAD kafka-connect/kafka-connect/src/main | tar -x -C "$PRISTINE"
PMAIN="$PRISTINE/kafka-connect/kafka-connect/src/main"
echo "pristine source: $(git -C "$WT" rev-parse HEAD) ($(git -C "$WT" log -1 --format=%cd --date=short HEAD))"
for f in channel/Channel.java channel/Coordinator.java channel/CoordinatorThread.java channel/CommitterImpl.java; do
  p="kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/$f"
  echo "  $f blob=$(git -C "$WT" rev-parse "HEAD:$p" | cut -c1-12) extracted=$(git hash-object "$PMAIN/java/org/apache/iceberg/connect/$f" | cut -c1-12)"
done
if find "$PMAIN" -name 'Tla*' | grep -q .; then echo "instrumentation found in pristine tree"; exit 4; fi

# ---- 3. test sources (environment + driver only) ----
cat > "$SRC_DIR/cr7/SharedInMemoryCatalog.java" <<'EOF'
package cr7;

import java.util.Map;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

/**
 * Environment only: every sink task instantiates its own catalog through
 * iceberg.catalog.catalog-impl; all table operations are routed to one shared root so that the
 * coordinator, the workers and the test observe the same tables. No sink logic involved.
 */
public class SharedInMemoryCatalog extends InMemoryCatalog {
  private static SharedInMemoryCatalog root;

  public static synchronized SharedInMemoryCatalog root() {
    if (root == null) {
      SharedInMemoryCatalog catalog = new SharedInMemoryCatalog();
      catalog.initialize("iceberg", Map.of());
      root = catalog;
    }
    return root;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    SharedInMemoryCatalog shared = root();
    return this == shared ? super.newTableOps(identifier) : shared.newTableOps(identifier);
  }
}
EOF

cat > "$SRC_DIR/cr7/Cr7ZombieCoordinatorRepro.java" <<'EOF'
package cr7;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConnector;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

public class Cr7ZombieCoordinatorRepro {
  static final String CONTROL_TOPIC = "control-iceberg"; // sink default; NOT set on any connector
  static final TopicPartition CTL0 = new TopicPartition(CONTROL_TOPIC, 0);
  static final String VICTIM = "victim";
  static final String VICTIM_SRC = "victim-src";
  static final TableIdentifier VICTIM_TABLE = TableIdentifier.of("db", "victim");
  static final String VICTIM_COORD_GROUP = "connect-" + VICTIM + "-coord"; // connectGroupId + "-coord"
  static final String VICTIM_SINK_GROUP = "connect-" + VICTIM;
  static final int NEIGHBOURS = Integer.getInteger("cr7.neighbours", 1);
  static final int NB_INTERVAL_MS = Integer.getInteger("cr7.nbIntervalMs", 500);
  static final int VICTIM_INTERVAL_MS = Integer.getInteger("cr7.victimIntervalMs", 5000);
  static final int VICTIM_TIMEOUT_MS = Integer.getInteger("cr7.victimTimeoutMs", 2000);
  static final int ATTEMPTS = Integer.getInteger("cr7.attempts", 10);
  static final boolean STICKY = Boolean.getBoolean("cr7.sticky");
  static final boolean INJECT = Boolean.getBoolean("cr7.inject");
  static final AtomicBoolean injecting = new AtomicBoolean(false);
  static final int STALL_WINDOW_S = 60;
  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));
  static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

  static EmbeddedConnectCluster connect;
  static Admin admin;
  static KafkaProducer<String, String> producer;
  static String bootstrap;
  static final AtomicBoolean nbTraffic = new AtomicBoolean(false);
  static final AtomicLong nbNext = new AtomicLong();
  static long victimNext = 0;

  static String nbName(int i) { return "neighbour-" + i; }
  static String nbSrc(int i) { return "neighbour-" + i + "-src"; }
  static TableIdentifier nbTable(int i) { return TableIdentifier.of("db", "neighbour_" + i); }

  public static void main(String[] args) {
    int exit = 2;
    try {
      exit = run() ? 0 : 1;
    } catch (Throwable t) {
      log("HARNESS ERROR: " + t);
      t.printStackTrace(System.out);
      exit = 3;
    } finally {
      try {
        nbTraffic.set(false);
        if (admin != null) admin.close();
        if (producer != null) producer.close();
        if (connect != null) connect.stop();
      } catch (Throwable t) {
        log("cleanup: " + t);
      }
      System.out.flush();
      Runtime.getRuntime().halt(exit); // a zombie iceberg-coord thread is non-daemon
    }
  }

  static boolean run() throws Exception {
    log(String.format("=== CR-7: pristine sink, real KRaft broker + Connect worker | neighbours=%d @%d ms,"
            + " victim interval=%d ms timeout=%d ms, -coord assignor=%s, attempts=%d",
        NEIGHBOURS, NB_INTERVAL_MS, VICTIM_INTERVAL_MS, VICTIM_TIMEOUT_MS,
        STICKY ? "CooperativeStickyAssignor (iceberg.kafka.partition.assignment.strategy)"
               : "Kafka default [RangeAssignor, CooperativeStickyAssignor] (not configured)", ATTEMPTS));
    SharedInMemoryCatalog catalog = SharedInMemoryCatalog.root();
    catalog.createNamespace(Namespace.of("db"));
    Map<String, String> tableProps = Map.of("format-version", "2", "write.format.default", "avro");
    catalog.createTable(VICTIM_TABLE, SCHEMA, PartitionSpec.unpartitioned(), tableProps);
    for (int i = 0; i < NEIGHBOURS; i++) {
      catalog.createTable(nbTable(i), SCHEMA, PartitionSpec.unpartitioned(), tableProps);
    }

    Properties brokerProps = new Properties();
    brokerProps.put("transaction.state.log.replication.factor", "1");
    brokerProps.put("transaction.state.log.min.isr", "1");
    brokerProps.put("transaction.state.log.num.partitions", "4");
    brokerProps.put("offsets.topic.num.partitions", "4");
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("offset.flush.interval.ms", "500"); // put() cadence of idle sink tasks
    workerProps.put("task.shutdown.graceful.timeout.ms", "10000");
    connect =
        new EmbeddedConnectCluster.Builder()
            .name("cr7")
            .numWorkers(1)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(1, "Connect worker did not start");
    bootstrap = connect.kafka().bootstrapServers();
    connect.kafka().createTopic(CONTROL_TOPIC, 1); // docs: kafka-topics --create --topic control-iceberg --partitions 1
    connect.kafka().createTopic(VICTIM_SRC, 2);
    for (int i = 0; i < NEIGHBOURS; i++) connect.kafka().createTopic(nbSrc(i), 1);
    admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    Map<String, Object> pp = new HashMap<>();
    pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    pp.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(pp, new StringSerializer(), new StringSerializer());
    warmUpTransactions();
    if (INJECT) startInjector();

    // ---------------------------------------------------------------------------------------
    log("--- Phase 1: " + NEIGHBOURS + " unrelated Iceberg sink(s) share the default control topic");
    Thread nbProducer =
        new Thread(
            () -> {
              while (true) {
                if (nbTraffic.get()) {
                  for (int i = 0; i < NEIGHBOURS; i++) {
                    long n = nbNext.getAndIncrement();
                    producer.send(new ProducerRecord<>(nbSrc(i), 0, null, json(n, "nb-" + n)));
                  }
                }
                sleepQuietly(50);
              }
            },
            "neighbour-source-producer");
    nbProducer.setDaemon(true);
    nbTraffic.set(true);
    nbProducer.start();
    for (int i = 0; i < NEIGHBOURS; i++) {
      connect.configureConnector(nbName(i), config(nbSrc(i), nbTable(i), 1, NB_INTERVAL_MS, 5000, false));
    }
    for (int i = 0; i < NEIGHBOURS; i++) {
      final int k = i;
      connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(nbName(i), 1, nbName(i));
      waitUntil(60_000, () -> snapshots(nbTable(k)) > 0, nbName(i) + " first table commit");
    }
    long[] gaps = measureControlTraffic(20_000);
    log(String.format(
        "control-topic traffic over 20 s: records=%d, max gap=%d ms, gaps >= 1000 ms: %d"
            + " (Coordinator POLL_DURATION = 1000 ms)", gaps[0], gaps[1], gaps[2]));
    Set<Thread> nbCoordThreads = coordThreads();
    log("iceberg-coord threads before the victim starts (neighbours'): " + nbCoordThreads.size());

    // ---------------------------------------------------------------------------------------
    log("--- Phase 2: victim connector, healthy baseline");
    connect.configureConnector(
        VICTIM, config(VICTIM_SRC, VICTIM_TABLE, 2, VICTIM_INTERVAL_MS, VICTIM_TIMEOUT_MS, STICKY));
    connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(VICTIM, 2, "victim");
    produceVictim(50); // 50 per source partition
    waitUntil(90_000, () -> rows(VICTIM_TABLE) >= 100, "victim baseline commit of 100 rows");
    waitUntil(30_000, () -> coordMembers().size() == 1, "victim -coord group has one member");
    log(String.format(
        "baseline OK: victim table rows=%d snapshots=%d, committed source offsets=%s, -coord members=%s",
        rows(VICTIM_TABLE), snapshots(VICTIM_TABLE), sourceOffsets(), fmt(coordMembers())));
    if (INJECT) {
      healthyCoordinatorUnderInjection(nbCoordThreads);
      log("injection ON for the hand-over (no gap >= 1 s on " + CTL0 + ")");
      injecting.set(true);
    }

    // ---------------------------------------------------------------------------------------
    log("--- Phase 3: coordinator hand-over via the Connect REST API"
        + " (POST /connectors/victim/restart?includeTasks=true), " + ATTEMPTS + " attempts");
    String zombieClient = null;
    String newClient = null;
    Thread zombieThread = null;
    List<String> outcomes = new ArrayList<>();
    int badStateSeen = 0;
    long badStateMaxMs = 0;
    long zombieMaxLifeMs = 0;
    for (int attempt = 1; attempt <= ATTEMPTS && zombieClient == null; attempt++) {
      waitUntil(60_000, () -> coordMembers().size() == 1
          && minus(coordThreads(), nbCoordThreads).size() == 1, "single victim coordinator");
      Map<String, Set<TopicPartition>> before = coordMembers();
      String oldClient = before.keySet().iterator().next();
      Thread oldThread = minus(coordThreads(), nbCoordThreads).iterator().next();
      long tR = System.currentTimeMillis();
      connect.restartConnectorAndTasks(VICTIM, false, true, false);
      long tDead = -1, tJoin = -1, tNewHolds = -1;
      String nc = null;
      boolean held = false;
      long lastDescribe = 0;
      long badFirst = -1;
      long badLast = -1;
      while (System.currentTimeMillis() - tR < 40_000) {
        long now = System.currentTimeMillis();
        if (tDead < 0 && !oldThread.isAlive()) tDead = now - tR;
        if (now - lastDescribe >= 100) {
          lastDescribe = now;
          Map<String, Set<TopicPartition>> m;
          try {
            m = coordMembers();
          } catch (RuntimeException e) {
            m = Map.of();
          }
          if (nc == null) {
            nc = m.keySet().stream().filter(c -> !before.containsKey(c)).findFirst().orElse(null);
            if (nc != null) tJoin = now - tR;
          }
          if (nc != null && tNewHolds < 0 && m.getOrDefault(nc, Set.of()).contains(CTL0)) {
            tNewHolds = now - tR;
          }
          // bad state of the finding: terminated coordinator still holds CTL0, new one has none
          if (nc != null && oldThread.isAlive() && m.getOrDefault(oldClient, Set.of()).contains(CTL0)
              && m.containsKey(nc) && !m.get(nc).contains(CTL0)) {
            if (badFirst < 0) badFirst = now - tR;
            badLast = now - tR;
          }
          if (nc != null && oldThread.isAlive() && m.getOrDefault(oldClient, Set.of()).contains(CTL0)
              && now - tR - tJoin > 10_000) {
            held = true;
            break;
          }
        }
        if (tDead >= 0 && tNewHolds >= 0) break;
        sleepQuietly(10);
      }
      String outcome =
          held ? "ZOMBIE KEPT " + CTL0 + " (>10 s after the new coordinator joined)"
              : (tDead >= 0 && (tJoin < 0 || tDead <= tJoin)) ? "zombie exited before the new coordinator joined"
              : (tDead >= 0) ? "zombie exited after the rebalance (lost the partition or saw an empty poll)"
              : "undetermined";
      outcomes.add(outcome);
      if (tDead > zombieMaxLifeMs) zombieMaxLifeMs = tDead;
      String bad = "";
      if (badFirst >= 0) {
        badStateSeen++;
        long dur = (tNewHolds > 0 ? tNewHolds : badLast) - badFirst;
        badStateMaxMs = Math.max(badStateMaxMs, dur);
        bad = String.format(" | BAD STATE seen: terminated coordinator held %s while new coordinator had"
            + " none, +%d..+%d ms (~%d ms), cleared when the zombie's 1 s poll came back empty", CTL0,
            badFirst, tNewHolds > 0 ? tNewHolds : badLast, dur);
      }
      log(String.format("attempt %2d: old coordinator %s thread exit at +%d ms | new coordinator %s joined"
              + " at +%d ms, owns %s at +%d ms | %s%s",
          attempt, shortId(oldClient), tDead, shortId(nc), tJoin, CTL0, tNewHolds, outcome, bad));
      if (held) {
        zombieClient = oldClient;
        newClient = nc;
        zombieThread = oldThread;
      } else {
        waitUntil(60_000, () -> coordMembers().size() == 1, "victim -coord group settles");
      }
    }
    if (zombieClient == null) {
      log("--- Phase 3 summary: the terminated coordinator kept " + CTL0 + " in 0 of "
          + outcomes.size() + " hand-overs: " + outcomes.stream()
              .collect(Collectors.groupingBy(o -> o, TreeMap::new, Collectors.counting())));
      log(String.format("MASK: control topic had %d gaps >= 1000 ms in 20 s (max gap %d ms) although %d records"
              + " flowed; terminated coordinator thread exited in %d/%d hand-overs, latest +%d ms after the"
              + " restart; transient bad state (zombie holds %s, new coordinator none) seen in %d hand-overs,"
              + " longest ~%d ms", gaps[2], gaps[1], gaps[0], outcomes.size(), outcomes.size(),
          zombieMaxLifeMs, CTL0, badStateSeen, badStateMaxMs));
      // delete path with the same traffic (finding's 'keeps its -coord partition' on stop/delete)
      deletePath();
      log("RESULT: NOT TRIGGERED - the terminated coordinator always left consumeAvailable");
      return false;
    }

    // ---------------------------------------------------------------------------------------
    log("--- Phase 4: stall window (" + STALL_WINDOW_S + " s = "
        + STALL_WINDOW_S * 1000 / VICTIM_INTERVAL_MS + " victim commit intervals), neighbour traffic on");
    long rows0 = rows(VICTIM_TABLE);
    int snaps0 = snapshots(VICTIM_TABLE);
    produceVictim(50); // 100 more records
    long expectedRows = victimNext * 2;
    log(String.format("t=0: produced 100 more victim records (expected total rows %d); table rows=%d"
        + " snapshots=%d", expectedRows, rows0, snaps0));
    boolean c1 = true;
    boolean c2 = true;
    long t0 = System.currentTimeMillis();
    long lastRows = rows0;
    int lastSnaps = snaps0;
    long committedSum = 0;
    while (System.currentTimeMillis() - t0 < STALL_WINDOW_S * 1000L) {
      sleepQuietly(5000);
      Map<String, Set<TopicPartition>> m = coordMembers();
      lastRows = rows(VICTIM_TABLE);
      lastSnaps = snapshots(VICTIM_TABLE);
      Map<Integer, Long> so = sourceOffsets();
      committedSum = so.values().stream().mapToLong(Long::longValue).sum();
      boolean zHolds = m.containsKey(zombieClient) && m.get(zombieClient).contains(CTL0);
      boolean nEmpty = m.containsKey(newClient) && m.get(newClient).isEmpty();
      String zStack = channelFrames(zombieThread);
      c1 &= zHolds && zombieThread.isAlive() && zStack.contains("Channel.consumeAvailable");
      c2 &= nEmpty;
      log(String.format(
          "t=%2ds table rows=%d snapshots=%d | committed source offsets=%s (sum %d) | -coord: zombie %s %s,"
              + " new %s %s | zombie thread alive=%s at %s",
          (System.currentTimeMillis() - t0) / 1000, lastRows, lastSnaps, so, committedSum,
          shortId(zombieClient), m.get(zombieClient), shortId(newClient), m.get(newClient),
          zombieThread.isAlive(), zStack));
    }
    boolean c3 = lastRows == rows0 && lastSnaps == snaps0 && committedSum >= expectedRows;
    log(String.format("stall window result: rows %d -> %d, snapshots %d -> %d; committed source"
        + " offsets sum %d", rows0, lastRows, snaps0, lastSnaps, committedSum));

    // ---------------------------------------------------------------------------------------
    log("--- Phase 5: control - remove the neighbour traffic (stop neighbour connectors + sources)");
    nbTraffic.set(false);
    injecting.set(false);
    for (int i = 0; i < NEIGHBOURS; i++) connect.stopConnector(nbName(i));
    long t1 = System.currentTimeMillis();
    long zombieGoneAt = -1;
    long caughtUpAt = -1;
    while (System.currentTimeMillis() - t1 < 90_000 && (zombieGoneAt < 0 || caughtUpAt < 0)) {
      sleepQuietly(1000);
      Map<String, Set<TopicPartition>> m = coordMembers();
      long r = rows(VICTIM_TABLE);
      long dt = (System.currentTimeMillis() - t1) / 1000;
      if (zombieGoneAt < 0 && !m.containsKey(zombieClient) && !zombieThread.isAlive()) {
        zombieGoneAt = dt;
        log(String.format("t=%2ds zombie left -coord group, thread exited; members=%s", dt, fmt(m)));
      }
      if (caughtUpAt < 0 && r >= expectedRows) {
        caughtUpAt = dt;
        log(String.format("t=%2ds victim table caught up: rows=%d snapshots=%d", dt, r, snapshots(VICTIM_TABLE)));
      }
    }
    long[] dup = distinctIds(VICTIM_TABLE);
    boolean c4 = zombieGoneAt >= 0 && caughtUpAt >= 0 && dup[0] == expectedRows && dup[1] == expectedRows;
    log(String.format("control result: zombie gone after %ds, caught up after %ds, rows=%d distinct=%d"
        + " (expected %d)", zombieGoneAt, caughtUpAt, dup[0], dup[1], expectedRows));
    for (int i = 0; i < NEIGHBOURS; i++) connect.resumeConnector(nbName(i));
    nbTraffic.set(true);
    deletePath();

    log("=== CHECKS");
    log("C1 terminated coordinator alive in Channel.consumeAvailable holding " + CTL0 + " all window: " + c1);
    log("C2 newly elected coordinator a -coord member with NO partition all window: " + c2);
    log("C3 no victim table commit in the window although source offsets were committed: " + c3);
    log("C4 control: removing the traffic ends the episode, table catches up, no duplicates: " + c4);
    boolean triggered = c1 && c2 && c3 && c4;
    log(triggered ? "RESULT: CR-7 TRIGGERED" : "RESULT: NOT TRIGGERED");
    return triggered;
  }

  /** Delete the victim connector while the neighbour traffic flows; watch its -coord group. */
  static void deletePath() throws Exception {
    log("--- Delete path: DELETE /connectors/victim while neighbour traffic flows");
    for (int i = 0; i < NEIGHBOURS; i++) {
      connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(nbName(i), 1, nbName(i));
    }
    if (INJECT) injecting.set(true);
    sleepQuietly(2000);
    Map<String, Set<TopicPartition>> beforeDelete = coordMembers();
    long tD = System.currentTimeMillis();
    connect.deleteConnector(VICTIM);
    long gone = -1;
    while (System.currentTimeMillis() - tD < 30_000) {
      Map<String, Set<TopicPartition>> m;
      try {
        m = coordMembers();
      } catch (RuntimeException e) {
        m = Map.of("?", Set.of());
      }
      if (m.isEmpty()) {
        gone = System.currentTimeMillis() - tD;
        break;
      }
      sleepQuietly(50);
    }
    log(String.format("-coord members before delete=%s; group empty %s after DELETE; "
            + "Admin.deleteConsumerGroups(%s) -> %s", fmt(beforeDelete),
        gone >= 0 ? "+" + gone + " ms" : "NOT within 30 s (phantom member: " + fmt(coordMembers()) + ")",
        VICTIM_COORD_GROUP, tryDeleteCoordGroup()));
    if (gone < 0) {
      injecting.set(false);
      nbTraffic.set(false);
      for (int i = 0; i < NEIGHBOURS; i++) connect.stopConnector(nbName(i));
      long tS = System.currentTimeMillis();
      boolean empty = waitQuietly(60_000, () -> coordMembers().isEmpty());
      log(String.format("after the control traffic stops: group empty=%s after +%d ms;"
              + " Admin.deleteConsumerGroups -> %s", empty, System.currentTimeMillis() - tS,
          tryDeleteCoordGroup()));
    }
  }

  /** LEVEL 2 helper: steady, ungated control traffic (valid events of an unrelated group). */
  static void startInjector() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    KafkaProducer<String, byte[]> inj =
        new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
    Thread t = new Thread(() -> {
      while (true) {
        if (injecting.get()) {
          Event e = new Event("connect-ungated-peer", new StartCommit(java.util.UUID.randomUUID()));
          inj.send(new ProducerRecord<>(CONTROL_TOPIC, "ungated-peer", AvroUtil.encode(e)));
        }
        sleepQuietly(200);
      }
    }, "cr7-injector");
    t.setDaemon(true);
    t.start();
  }

  /**
   * LEVEL 2 comparison: the same ungated traffic with NO terminated coordinator. The healthy
   * coordinator cannot leave consumeAvailable either, so it never sends the next StartCommit.
   */
  static void healthyCoordinatorUnderInjection(Set<Thread> nbCoordThreads) throws Exception {
    log("--- Phase 2b (LEVEL 2, mask removed, NO termination): inject ungated traffic at the healthy coordinator");
    Thread healthy = minus(coordThreads(), nbCoordThreads).iterator().next();
    long rowsA = rows(VICTIM_TABLE);
    long soA = sourceOffsets().values().stream().mapToLong(Long::longValue).sum();
    injecting.set(true);
    sleepQuietly(2000);
    produceVictim(50);
    long expected = victimNext * 2;
    long rowsB = rowsA;
    long soB = soA;
    for (int s = 5; s <= 30; s += 5) {
      sleepQuietly(5000);
      rowsB = rows(VICTIM_TABLE);
      soB = sourceOffsets().values().stream().mapToLong(Long::longValue).sum();
      log(String.format("  t=%2ds healthy coordinator (not terminated): table rows=%d, committed source"
          + " offsets sum=%d, -coord=%s, thread at %s", s, rowsB, soB, fmt(coordMembers()), channelFrames(healthy)));
    }
    injecting.set(false);
    long tS = System.currentTimeMillis();
    waitUntil(60_000, () -> rows(VICTIM_TABLE) >= expected, "healthy coordinator catches up");
    log(String.format("  healthy coordinator under ungated traffic: rows %d -> %d and source offsets %d -> %d"
            + " in 30 s (stalled=%s); after the traffic stops it commits (rows=%d) within %d ms",
        rowsA, rowsB, soA, soB, rowsB == rowsA, rows(VICTIM_TABLE), System.currentTimeMillis() - tS));
  }

  // ------------------------------------------------------------------------------------------
  static Map<String, String> config(
      String topic, TableIdentifier table, int tasks, int intervalMs, int timeoutMs, boolean st) {
    Map<String, String> c = new HashMap<>();
    c.put("connector.class", IcebergSinkConnector.class.getName());
    c.put("tasks.max", String.valueOf(tasks));
    c.put("topics", topic);
    c.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    c.put("value.converter.schemas.enable", "false");
    c.put("iceberg.tables", table.toString());
    c.put("iceberg.catalog.catalog-impl", SharedInMemoryCatalog.class.getName());
    c.put("iceberg.control.commit.interval-ms", String.valueOf(intervalMs));
    c.put("iceberg.control.commit.timeout-ms", String.valueOf(timeoutMs));
    c.put("iceberg.kafka.bootstrap.servers", bootstrap);
    if (st) {
      c.put("iceberg.kafka.partition.assignment.strategy",
          "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    }
    return c; // NOTE: iceberg.control.topic deliberately not set -> default "control-iceberg"
  }

  static void produceVictim(int perPartition) throws Exception {
    for (int i = 0; i < perPartition; i++) {
      for (int p = 0; p < 2; p++) {
        long id = victimNext * 2 + p;
        producer.send(new ProducerRecord<>(VICTIM_SRC, p, null, json(id, "v-" + id))).get();
      }
      victimNext++;
    }
  }

  static String json(long id, String data) {
    return "{\"id\":" + id + ",\"data\":\"" + data + "\"}";
  }

  static Table table(TableIdentifier id) {
    return SharedInMemoryCatalog.root().loadTable(id);
  }

  static long rows(TableIdentifier id) {
    Snapshot s = table(id).currentSnapshot();
    return s == null ? 0 : Long.parseLong(s.summary().getOrDefault("total-records", "0"));
  }

  static int snapshots(TableIdentifier id) {
    int n = 0;
    for (Snapshot ignored : table(id).snapshots()) n++;
    return n;
  }

  static long[] distinctIds(TableIdentifier id) throws Exception {
    long total = 0;
    Set<Long> ids = new HashSet<>();
    try (CloseableIterable<Record> it = IcebergGenerics.read(table(id)).build()) {
      for (Record r : it) {
        total++;
        ids.add((Long) r.getField("id"));
      }
    }
    return new long[] {total, ids.size()};
  }

  static Map<String, Set<TopicPartition>> coordMembers() {
    try {
      ConsumerGroupDescription d =
          admin.describeConsumerGroups(List.of(VICTIM_COORD_GROUP)).all().get().get(VICTIM_COORD_GROUP);
      Map<String, Set<TopicPartition>> m = new TreeMap<>();
      for (MemberDescription md : d.members()) {
        m.put(md.clientId(), new HashSet<>(md.assignment().topicPartitions()));
      }
      return m;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static Map<Integer, Long> sourceOffsets() throws Exception {
    Map<TopicPartition, OffsetAndMetadata> o =
        admin.listConsumerGroupOffsets(VICTIM_SINK_GROUP).partitionsToOffsetAndMetadata().get();
    Map<Integer, Long> r = new TreeMap<>();
    o.forEach((tp, om) -> { if (om != null && tp.topic().equals(VICTIM_SRC)) r.put(tp.partition(), om.offset()); });
    return r;
  }

  static String tryDeleteCoordGroup() {
    try {
      admin.deleteConsumerGroups(List.of(VICTIM_COORD_GROUP)).all().get();
      return "deleted";
    } catch (ExecutionException e) {
      return e.getCause() instanceof GroupNotEmptyException
          ? "GroupNotEmptyException(" + e.getCause().getMessage() + ")"
          : String.valueOf(e.getCause());
    } catch (Exception e) {
      return String.valueOf(e);
    }
  }

  /** Arrival gaps of committed control-topic records (all connectors), group-less reader. */
  static long[] measureControlTraffic(long millis) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    Map<String, Integer> byGroup = new TreeMap<>();
    Map<String, Integer> byType = new TreeMap<>();
    long n = 0;
    long maxGap = 0;
    long bigGaps = 0;
    try (KafkaConsumer<String, byte[]> c =
        new KafkaConsumer<>(p, new StringDeserializer(), new ByteArrayDeserializer())) {
      c.assign(List.of(CTL0));
      c.seekToEnd(List.of(CTL0));
      c.position(CTL0);
      long start = System.currentTimeMillis();
      long last = start;
      while (System.currentTimeMillis() - start < millis) {
        for (ConsumerRecord<String, byte[]> r : c.poll(java.time.Duration.ofMillis(20))) {
          long now = System.currentTimeMillis();
          long gap = now - last;
          maxGap = Math.max(maxGap, gap);
          if (gap >= 1000) bigGaps++;
          last = now;
          n++;
          org.apache.iceberg.connect.events.Event e = AvroUtil.decode(r.value());
          byGroup.merge(e.groupId(), 1, Integer::sum);
          byType.merge(e.payload().type().name(), 1, Integer::sum);
        }
      }
      maxGap = Math.max(maxGap, System.currentTimeMillis() - last);
    }
    log("  control-topic records by connect group: " + byGroup + " by type: " + byType);
    return new long[] {n, maxGap, bigGaps};
  }

  static Set<Thread> coordThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().equals("iceberg-coord") && t.isAlive())
        .collect(Collectors.toSet());
  }

  static Set<Thread> minus(Set<Thread> a, Set<Thread> b) {
    Set<Thread> r = new HashSet<>(a);
    r.removeAll(b);
    return r;
  }

  static String channelFrames(Thread t) {
    List<String> frames = new ArrayList<>();
    for (StackTraceElement e : t.getStackTrace()) {
      if (e.getClassName().startsWith("org.apache.iceberg.connect.channel.")) {
        String cls = e.getClassName().substring(e.getClassName().lastIndexOf('.') + 1);
        frames.add(cls + "." + e.getMethodName() + ":" + e.getLineNumber());
      }
    }
    return frames.isEmpty() ? "-" : String.join(" < ", frames);
  }

  static String fmt(Map<String, Set<TopicPartition>> m) {
    Map<String, Set<TopicPartition>> r = new TreeMap<>();
    m.forEach((k, v) -> r.put(shortId(k), v));
    return r.toString();
  }

  static String shortId(String clientId) {
    return clientId == null ? "null" : clientId.substring(0, Math.min(8, clientId.length()));
  }

  static void warmUpTransactions() {
    for (int i = 0; i < 4; i++) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "cr7-warmup-" + i);
      try (KafkaProducer<String, String> w =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        w.initTransactions();
        w.beginTransaction();
        w.commitTransaction();
      }
    }
  }

  static void waitUntil(long millis, BooleanSupplier cond, String what) {
    if (!waitQuietly(millis, cond)) {
      throw new IllegalStateException("timed out waiting for: " + what);
    }
  }

  static boolean waitQuietly(long millis, BooleanSupplier cond) {
    long end = System.currentTimeMillis() + millis;
    while (System.currentTimeMillis() < end) {
      try {
        if (cond.getAsBoolean()) return true;
      } catch (RuntimeException e) {
        // group not yet described / transient admin error
      }
      sleepQuietly(250);
    }
    return false;
  }

  static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  static void log(String msg) {
    System.out.println(LocalTime.now().format(TS) + " " + msg);
    System.out.flush();
  }
}
EOF

# ---- 4. compile pristine sink + test driver ----
find "$PMAIN/java" "$SRC_DIR" -name '*.java' > "$WORK/sources.txt"
javac --release 17 -nowarn -proc:none -encoding UTF-8 -d "$CLASSES" -cp "$CP" @"$WORK/sources.txt" 2>&1 | grep -v -E "^Note:" || true
cp -R "$PMAIN/resources/." "$CLASSES/"
test -f "$CLASSES/cr7/Cr7ZombieCoordinatorRepro.class" || { echo "compile failed"; exit 5; }
echo "compiled $(wc -l < "$WORK/sources.txt" | tr -d ' ') sources"

# ---- 5. run (outer timeout: the scenario is about a thread that does not stop) ----
set +e
timeout 1500 java -Xmx3g -cp "$CLASSES:$CP" \
  -Dcr7.neighbours="$NB" -Dcr7.nbIntervalMs="$NBI" -Dcr7.victimIntervalMs="$VI" \
  -Dcr7.victimTimeoutMs="$VT" -Dcr7.sticky="$ST" -Dcr7.attempts="$ATTEMPTS" -Dcr7.inject="$INJ" \
  -Dorg.slf4j.simpleLogger.logFile="$SINK_LOG" \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
  -Dorg.slf4j.simpleLogger.log.org.apache.iceberg.connect=info \
  -Dorg.slf4j.simpleLogger.showDateTime=true \
  -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS \
  cr7.Cr7ZombieCoordinatorRepro
rc=$?
set -e
echo "--- sink log excerpts (pristine CommitterImpl INFO lines; coordinator-thread exits) ---"
grep -E "Committer victim-[0-9] (stopped|lost leader|received leader)|\[iceberg-coord\] INFO .*Channel stopping" "$SINK_LOG" | sed -E 's/^([0-9:.]+) \[([^]]+)\] INFO [^ ]+ - /\1 [\2] /' | head -24 || true
echo "exit code: $rc"
exit $rc
