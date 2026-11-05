#!/bin/bash
# MC-2 reproduction: WakeupException from consumer.committed() in CommitterImpl.close()
# (KafkaUtils.seekToLastCommittedOffsets) -> no rewind + the next polled batch is dropped -> LOSS.
#
# Runs the PRISTINE Iceberg Kafka Connect sink (apache/iceberg @ 9fffeb6, sources taken with
# `git archive HEAD`, so the Specula trace instrumentation in the working tree is NOT used) inside
# one local JVM: embedded Kafka 3.9.2 broker (KRaft) + a real Kafka Connect worker (real
# WorkerSinkTask) + an in-memory Iceberg catalog shared by the tasks.
#
# A "run" is scenario:mode[:L1]. scenario = coop (CooperativeStickyAssignor) | eager (RangeAssignor).
# mode = control (ONE focused rebalance, no wakeup injection -> baseline, expect no loss)
#      | inject  (the same ONE focused rebalance, with the wakeup injected across it -> the bug)
#      | wakeup  (secondary: churny restarts + real REST pause/resume cycling; shows committed()
#                 throwing repeatedly but loss masked by continual partition reassignment).
#
# Level 0/1 (black box): the sink is PRISTINE (apache/iceberg @ 9fffeb6, sources via `git archive
# HEAD`, so the working-tree trace instrumentation is NOT used) and nothing in the sink or Connect
# is modified; rebalances and wakeups come only from real REST calls.
# Level 2 (:L1 suffix): the ONLY change vs pristine is one injected `consumer.wakeup()` in
# KafkaUtils.seekToLastCommittedOffsets immediately before consumer.committed(), gated by
# -Dmc2.injectWakeup. This is the exact call WorkerSinkTask.transitionTo issues on a REST
# pause/resume; it deterministically places the finding's precondition (a wakeup pending when
# committed() runs during a revoke close) at the site. No sink logic is changed; committed() is the
# real call and throws the real WakeupException. mode=wakeup also fires real pause/resume cycles.
#
# Usage: bash test_bugMC-2_wakeup_close_loss.sh [runs...]
#   default runs: coop:control coop:inject:L1 eager:inject:L1
# Env: MC2_ROUNDS (wakeup-mode rounds, default 8), MC2_WAKEUP_MS (pause/resume interval ms, default 40).
# Exit: 0 = no loss observed; 3 = loss reproduced (records below the committed source offset are
#       missing from the table); 1 = harness/build error.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="$(cd "$HERE/.." && pwd)"                       # .specula-output
WORK="$OUT_DIR/confirmation/MC-2"
B="$WORK/repro-build"
W="$WORK/worktree"                                      # git checkout of apache/iceberg @ 9fffeb6
S="$OUT_DIR/../source"                                  # built (unmodified) iceberg modules
RUN_OUT="$WORK/repro-out"
RUNS=("$@"); [ ${#RUNS[@]} -eq 0 ] && RUNS=(coop:control coop:inject:L1 eager:inject:L1)
mkdir -p "$B" "$RUN_OUT"

# ---------------------------------------------------------------------------------------------
# 1. Classpath: external jars resolved by Gradle (offline) + unmodified iceberg module jars
if [ ! -s "$B/deps.cp" ]; then
  cat > "$B/print-cp.init.gradle" <<'EOF'
allprojects {
  if (it.path == ':iceberg-kafka-connect:iceberg-kafka-connect') {
    it.afterEvaluate { p ->
      p.tasks.register('printReproCp') {
        doLast {
          def cfg = p.configurations.getByName('tlaHarnessRuntimeClasspath')
          def out = new File(System.getProperty('repro.cp.out'))
          out.text = ''
          cfg.incoming.artifactView { componentFilter { id -> id instanceof org.gradle.api.artifacts.component.ModuleComponentIdentifier } }.artifacts.each { a ->
            out << a.id.componentIdentifier.displayName + '\t' + a.file.absolutePath + '\n'
          }
        }
      }
    }
  }
}
EOF
  G=$(ls -d ~/.gradle/wrapper/dists/gradle-8.14.6-bin/*/gradle-8.14.6 | head -1)/bin/gradle
  (cd "$W" && timeout 900 "$G" --offline -q --console=plain -I "$B/print-cp.init.gradle" \
      -Drepro.cp.out="$B/external-cp.tsv" :iceberg-kafka-connect:iceberg-kafka-connect:printReproCp) \
      > "$B/print-cp.log" 2>&1 || { echo "classpath resolution failed (see $B/print-cp.log)"; exit 1; }
  if git -C "$S" status --porcelain | grep -v " kafka-connect/" | grep -q .; then
    echo "source tree has modifications outside kafka-connect/: module jars not provably pristine"; exit 1
  fi
  ICE=""
  for j in api/build/libs/iceberg-api-9fffeb6.dirty.jar bundled-guava/build/libs/iceberg-bundled-guava-9fffeb6.jar \
           common/build/libs/iceberg-common-9fffeb6.dirty.jar core/build/libs/iceberg-core-9fffeb6.dirty.jar \
           data/build/libs/iceberg-data-9fffeb6.dirty.jar orc/build/libs/iceberg-orc-9fffeb6.dirty.jar \
           parquet/build/libs/iceberg-parquet-9fffeb6.dirty.jar \
           kafka-connect/kafka-connect-events/build/libs/iceberg-kafka-connect-events-9fffeb6.dirty.jar; do
    [ -f "$S/$j" ] || { echo "missing module jar $S/$j"; exit 1; }
    ICE="$ICE:$S/$j"
  done
  EXT=$(awk -F'\t' '$1 !~ /^org\.slf4j:slf4j-(simple|reload4j):/ && !seen[$2]++ {print $2}' "$B/external-cp.tsv" | paste -sd: -)
  LB=$(find ~/.gradle/caches/modules-2/files-2.1/ch.qos.logback -name "logback-*-1.5.38.jar" | grep -v sources | paste -sd: -)
  echo "${ICE#:}:$EXT:$LB" > "$B/deps.cp"
fi
DEPS="$(cat "$B/deps.cp")"

# ---------------------------------------------------------------------------------------------
# 2. Pristine sink classes (git HEAD, not the instrumented working tree)
rm -rf "$B/pristine-src" "$B/classes/sink" && mkdir -p "$B/pristine-src" "$B/classes/sink"
git -C "$W" archive HEAD kafka-connect/kafka-connect/src/main | tar -x -C "$B/pristine-src"
if grep -rq "TlaHooks\|TlaTrace" "$B/pristine-src"; then echo "instrumented sources leaked"; exit 1; fi
find "$B/pristine-src/kafka-connect/kafka-connect/src/main/java" -name "*.java" > "$B/sink-sources.txt"
timeout 300 javac --release 17 -nowarn -encoding UTF-8 -cp "$DEPS" -d "$B/classes/sink" @"$B/sink-sources.txt" \
  > "$B/javac-sink.log" 2>&1 || { cat "$B/javac-sink.log"; exit 1; }
cp -R "$B/pristine-src/kafka-connect/kafka-connect/src/main/resources/." "$B/classes/sink/"
echo "pristine sink: $(find "$B/classes/sink" -name '*.class' | wc -l | tr -d ' ') classes from $(git -C "$W" rev-parse --short HEAD)"

# Level-2 injection variant (built only if a *:L1 run is requested): the ONLY change vs pristine is
# one `consumer.wakeup()` inserted in KafkaUtils.seekToLastCommittedOffsets immediately BEFORE
# `consumer.committed(consumer.assignment())`, gated by -Dmc2.injectWakeup. No sink logic is altered;
# it is the exact call WorkerSinkTask.transitionTo issues on a real REST pause/resume, placed
# deterministically at the finding's site so the real committed() throws the real WakeupException.
case " ${RUNS[*]} " in
  *:L1\ *|*:L1)
    rm -rf "$B/l1-src" "$B/classes/sink-l1" && mkdir -p "$B/classes/sink-l1"
    cp -R "$B/pristine-src" "$B/l1-src"
    KU="$B/l1-src/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/KafkaUtils.java"
    python3 -I - "$KU" <<'PY'
import sys
p = sys.argv[1]; s = open(p).read()
needle = "    Map<TopicPartition, OffsetAndMetadata> committedOffsets =\n        consumer.committed(consumer.assignment());"
# Deterministic injection of the finding's precondition: a concurrent consumer.wakeup() (exactly
# what WorkerSinkTask.transitionTo issues on a REST pause/resume) has arrived on THIS task consumer
# during the revoke callback, so the real committed() below throws the real WakeupException at the
# finding's site. No sink logic is changed. The event is reachable in the real system via a
# pause/resume concurrent with a rebalance (mode=wakeup fires real ones); here its timing is made
# deterministic because committed()'s in-JVM network window is sub-millisecond. It instantiates CE
# steps State 9 PauseResume -> State 10 CloseSeekWakeup of MC_hunt_s3_close_discard_bfs.out.
inject = ("    if (Boolean.getBoolean(\"mc2.injectWakeup\")) {\n"
          "      consumer.wakeup(); // [MC-2] concurrent pause/resume wakeup lands during close callback\n"
          "    }\n")
assert needle in s, "anchor not found in KafkaUtils.java"
s = s.replace(needle, inject + needle, 1)
open(p, "w").write(s)
PY
    find "$B/l1-src/kafka-connect/kafka-connect/src/main/java" -name "*.java" > "$B/l1-sources.txt"
    timeout 300 javac --release 17 -nowarn -encoding UTF-8 -cp "$DEPS" -d "$B/classes/sink-l1" @"$B/l1-sources.txt" \
      > "$B/javac-sink-l1.log" 2>&1 || { cat "$B/javac-sink-l1.log"; exit 1; }
    cp -R "$B/l1-src/kafka-connect/kafka-connect/src/main/resources/." "$B/classes/sink-l1/"
    echo "Level-2 sink (pristine + one injected consumer.wakeup() before committed()): $(find "$B/classes/sink-l1" -name '*.class' | wc -l | tr -d ' ') classes"
    echo "  --- exact diff vs pristine KafkaUtils.seekToLastCommittedOffsets:"
    diff "$B/pristine-src/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/KafkaUtils.java" "$KU" | sed 's/^/      /' || true
    ;;
esac

# ---------------------------------------------------------------------------------------------
# 3. Harness (test-only code: cluster setup, workload, REST calls, verification)
mkdir -p "$B/harness-src/mc2repro" "$B/classes/harness"
cat > "$B/harness-src/mc2repro/ReproMC2.java" <<'JAVA'
package mc2repro;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConnector;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

public class ReproMC2 {
  static final String CONNECTOR = "iceberg-sink";
  static final String GROUP = "connect-" + CONNECTOR;
  static final String SRC = "src";
  static final String CTL = "ctl";
  static final String TABLE = "db.tbl";
  static final int PARTS = 2;
  static final int ROUNDS = Integer.getInteger("mc2.rounds", 8);
  static final Schema SCHEMA =
      new Schema(optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  /** One in-memory catalog for the whole JVM: every task's instance delegates table ops to root. */
  public static class SharedCatalog extends InMemoryCatalog {
    private static volatile SharedCatalog root;

    public SharedCatalog() {}

    static synchronized SharedCatalog initRoot() {
      if (root == null) {
        SharedCatalog catalog = new SharedCatalog();
        catalog.initialize("iceberg", new HashMap<>());
        root = catalog;
      }
      return root;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier identifier) {
      SharedCatalog r = root;
      return (r == null || r == this) ? super.newTableOps(identifier) : r.newTableOps(identifier);
    }
  }

  static void log(String fmt, Object... args) {
    System.out.println("[repro " + LocalTime.now() + "] " + String.format(fmt, args));
    System.out.flush();
  }

  private final String scenario; // coop | eager
  private final String mode; // control | resume
  private EmbeddedConnectCluster connect;
  private KafkaProducer<String, String> producer;
  private final long[] produced = new long[PARTS];
  private long nextId = 0;
  private final AtomicInteger resumeCalls = new AtomicInteger();

  ReproMC2(String scenario, String mode) {
    this.scenario = scenario;
    this.mode = mode;
  }

  public static void main(String[] args) throws Exception {
    ReproMC2 repro = new ReproMC2(args[0], args[1]);
    int rc;
    try {
      rc = repro.run();
    } catch (Throwable t) {
      t.printStackTrace(System.out);
      rc = 1;
    } finally {
      repro.shutdown();
    }
    System.out.flush();
    Runtime.getRuntime().halt(rc);
  }

  private Map<String, String> connectorConfig() {
    Map<String, String> c = new HashMap<>();
    c.put("connector.class", IcebergSinkConnector.class.getName());
    c.put("tasks.max", "2");
    c.put("topics", SRC);
    c.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    c.put("value.converter.schemas.enable", "false");
    // one record per poll, as in the MC trace harness: the poll that completes a rebalance then
    // fetches exactly one record, which the stored-exception rethrow drops (the finding's dropped
    // batch). With the default 500 the rebalance-completing poll usually returns nothing.
    c.put("consumer.override.max.poll.records", "1");
    c.put(
        "consumer.override.partition.assignment.strategy",
        "coop".equals(scenario)
            ? "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
            : "org.apache.kafka.clients.consumer.RangeAssignor");
    c.put("iceberg.tables", TABLE);
    c.put("iceberg.catalog.catalog-impl", SharedCatalog.class.getName());
    c.put("iceberg.control.topic", CTL);
    c.put("iceberg.control.commit.interval-ms", "2000");
    c.put("iceberg.control.commit.timeout-ms", "20000");
    c.put("iceberg.kafka.bootstrap.servers", connect.kafka().bootstrapServers());
    return c;
  }

  int run() throws Exception {
    log("scenario=%s mode=%s rounds=%d", scenario, mode, ROUNDS);
    SharedCatalog root = SharedCatalog.initRoot();
    root.createNamespace(Namespace.of("db"));
    root.createTable(
        TableIdentifier.parse(TABLE),
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of("format-version", "2", "write.format.default", "avro"));

    Properties broker = new Properties();
    broker.put("transaction.state.log.replication.factor", "1");
    broker.put("transaction.state.log.min.isr", "1");
    broker.put("transaction.state.log.num.partitions", "4");
    broker.put("offsets.topic.num.partitions", "4");
    Map<String, String> worker = new HashMap<>();
    worker.put("connector.client.config.override.policy", "All");
    worker.put("offset.flush.interval.ms", "500");
    worker.put("task.shutdown.graceful.timeout.ms", "10000");
    connect =
        new EmbeddedConnectCluster.Builder()
            .name("mc2-" + scenario + "-" + mode)
            .numWorkers(1) // both tasks share one worker; a second worker is added at the trigger
            .numBrokers(1)
            .brokerProps(broker)
            .workerProps(worker)
            .build();
    connect.start();
    connect.kafka().createTopic(SRC, PARTS);
    connect.kafka().createTopic(CTL, 1);
    Map<String, Object> pp = new HashMap<>();
    pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    pp.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(pp, new StringSerializer(), new StringSerializer());
    warmUpTransactions();

    connect.configureConnector(CONNECTOR, connectorConfig());
    connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 2, "connector not running");
    log("connector running: %s", taskStates());

    // Phase 1: every source partition gets a committed offset first (keeps the known
    // null-committed-offset rewind gap, apache/iceberg#18767, out of this run).
    for (int i = 0; i < 20; i += 1) {
      for (int p = 0; p < PARTS; p += 1) {
        produceOne(p);
      }
    }
    awaitCommitted(Duration.ofSeconds(120));
    log("baseline committed=%s (all partitions have a committed offset)", committed());

    // Phase 2: continuous traffic + ROUNDS sink-group rebalances (REST task restarts)
    AtomicBoolean stopTraffic = new AtomicBoolean(false);
    Thread traffic =
        new Thread(
            () -> {
              try {
                while (!stopTraffic.get()) {
                  for (int p = 0; p < PARTS; p += 1) {
                    produceOne(p);
                  }
                  Thread.sleep(25);
                }
              } catch (Exception e) {
                log("traffic thread error: %s", e);
              }
            },
            "mc2-traffic");
    traffic.start();

    AtomicBoolean stopResume = new AtomicBoolean(false);
    Thread resumer = null;
    if ("wakeup".equals(mode)) {
      // Secondary, churny evidence: operator-style REST pause/resume cycling + repeated restarts.
      // Every pause/resume edge is a target-state write; the herder calls
      // WorkerSinkTask.transitionTo(state) = `super.transitionTo(state); consumer.wakeup();`
      // (verified in connect-runtime 3.9.2 bytecode) -> a real consumer.wakeup() on each task's own
      // consumer. committed()-threw closes are counted from the log, but the continual reassignment
      // this causes also resets the consumer to the committed offset, so loss is usually re-read
      // (masked). The focused modes below isolate the permanent loss.
      resumer =
          new Thread(
              () -> {
                boolean paused = false;
                while (!stopResume.get()) {
                  try {
                    if (paused) {
                      connect.resumeConnector(CONNECTOR);
                    } else {
                      connect.pauseConnector(CONNECTOR);
                    }
                    // real wakeup() lands rarely inside committed()'s sub-ms window; with the L1
                    // sink the injection is armed throughout so committed() reliably throws here
                    paused = !paused;
                    resumeCalls.incrementAndGet();
                    Thread.sleep(Long.getLong("mc2.wakeupIntervalMs", 25L));
                  } catch (Exception e) {
                    // transient REST races during a rebalance are expected; keep cycling
                  }
                }
                if (paused) {
                  try {
                    connect.resumeConnector(CONNECTOR);
                  } catch (Exception ignored) {
                    // best effort
                  }
                }
              },
              "mc2-wakeup");
      System.setProperty("mc2.injectWakeup", "true"); // L1 sink: arm the injection for the churny run
      resumer.start();
      for (int r = 1; r <= ROUNDS; r += 1) {
        int victim = nonLeaderTask();
        log("round %d: REST restart of task %d (owners before: %s)", r, victim, owners());
        connect.restartTask(CONNECTOR, victim);
        Thread.sleep(Long.getLong("mc2.roundMs", 5000L));
        log("round %d done: owners=%s wakeupCycles=%d tasks=%s", r, owners(), resumeCalls.get(), taskStates());
      }
      stopResume.set(true);
      resumer.join(10000);
    } else {
      // Focused trigger (modes control / inject): reproduce the MC counterexample shape exactly -
      // one task owns BOTH partitions, a second joins, the owner revokes one partition and RETAINS
      // the other. Remove a worker so the surviving task's consumer gets both p0 and p1 (the removed
      // task stays unassigned under the default rebalance delay); build an uncommitted buffer on it;
      // then add a worker so the removed task rejoins and the owner does close([revoked]) while
      // retaining the other partition. close() discards the whole Worker (incl. the retained
      // partition's buffered-but-uncommitted records) and then runs the fallible seek. mode=inject
      // arms the KafkaUtils wakeup injection across this one rebalance, so committed() throws (the
      // real pause/resume wakeup landing in the callback) and the retained partition is NOT rewound.
      // No further rebalance touches it afterwards, so its committed offset advances past the hole
      // and the loss is permanent.
      boolean inject = "inject".equals(mode);
      // Add a second worker so Connect MOVES one task (task1) to it. task1's consumer leaves the
      // sink group and rejoins on the new worker, while task0 KEEPS RUNNING on worker0 (not
      // restarted). The surviving task0 therefore does a pure cooperative open([p1]) then
      // close([p1]) RETAINING p0 - the MC counterexample shape (t1 revokes p1, retains p0). It
      // enters close([p1]) with p0 records buffered-but-uncommitted; stopWorker() discards them and
      // committed() (injected wakeup) throws, so p0 is NOT rewound. task0 then consumes p0 forward
      // from its un-rewound position and the Worker commits past the discarded records. Because
      // task0 never leaves the group and p0 is not reassigned again, the loss is permanent.
      if (inject) {
        System.setProperty("mc2.injectWakeup", "true");
      }
      log("adding a second worker so task1 moves to it and task0 does a pure partial revoke; injectWakeup=%s (owners before: %s)",
          inject, owners());
      connect.addWorker();
      Thread.sleep(Long.getLong("mc2.triggerMs", 12000L));
      if (inject) {
        System.clearProperty("mc2.injectWakeup");
      }
      log("after adding worker: owners=%s tasks=%s", owners(), taskStates());
      // Let the retained partition's committed offset advance PAST the hole (no more rebalances).
      Thread.sleep(Long.getLong("mc2.settleMs", 8000L));
    }

    // Quiet settle with traffic STILL running and NO more rebalances: the retained partition's
    // Worker now commits its source offset forward past any un-rewound hole, making a loss
    // permanent. Observe committed-vs-table each second so a transient hole (one the next rebalance
    // would have re-read, i.e. masked) is distinguished from a persistent one.
    int maxTransientMissing = 0;
    for (int i = 0; i < 12; i += 1) {
      Thread.sleep(1000);
      int m = missingNow();
      if (m > maxTransientMissing) {
        maxTransientMissing = m;
      }
    }
    log("post-trigger observation: maxTransientMissing=%d (traffic still flowing, no rebalances)", maxTransientMissing);

    Thread.sleep(500);
    stopTraffic.set(true);
    traffic.join(10000);
    log("traffic stopped: produced=%s wakeupCycles=%d maxTransientMissing=%d",
        producedMap(), resumeCalls.get(), maxTransientMissing);

    // Phase 3: let the sink converge, then compare committed source offsets with the table
    boolean converged = awaitCommittedQuiet(Duration.ofSeconds(120));
    awaitTableStable();
    int missingAfterRounds = report("after-rounds", converged);

    // Phase 4: permanence: restart every task (they resume from the group-committed offsets)
    connect.restartTask(CONNECTOR, 0);
    connect.restartTask(CONNECTOR, 1);
    Thread.sleep(15000);
    awaitTableStable();
    int missingAfterRestart = report("after-restart-all-tasks", awaitCommittedQuiet(Duration.ofSeconds(30)));

    return (missingAfterRounds > 0 || missingAfterRestart > 0) ? 3 : 0;
  }

  // -------------------------------------------------------------------------------------------

  private void warmUpTransactions() {
    for (int i = 0; i < 6; i += 1) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "mc2-warmup-" + i);
      try (KafkaProducer<String, String> warm =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  private synchronized void produceOne(int partition) throws Exception {
    long offset = produced[partition];
    long id = nextId++;
    String value = "{\"id\": " + id + ", \"data\": \"p" + partition + "-" + offset + "\"}";
    RecordMetadata md = producer.send(new ProducerRecord<>(SRC, partition, "k" + id, value)).get();
    if (md.offset() != offset) {
      throw new IllegalStateException("unexpected offset " + md.offset() + " != " + offset);
    }
    produced[partition] = offset + 1;
  }

  private synchronized Map<Integer, Long> producedMap() {
    Map<Integer, Long> m = new TreeMap<>();
    for (int p = 0; p < PARTS; p += 1) {
      m.put(p, produced[p]);
    }
    return m;
  }

  private static void await(String what, Duration timeout, java.util.function.BooleanSupplier cond)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(100);
    }
  }

  private Admin admin() {
    return Admin.create(
        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers()));
  }

  private Map<Integer, Long> committed() throws Exception {
    Map<Integer, Long> m = new TreeMap<>();
    try (Admin admin = admin()) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(20, TimeUnit.SECONDS);
      offsets.forEach(
          (tp, o) -> {
            if (o != null && SRC.equals(tp.topic())) {
              m.put(tp.partition(), o.offset());
            }
          });
    }
    return m;
  }

  private Map<String, List<Integer>> owners() {
    Map<String, List<Integer>> m = new TreeMap<>();
    try (Admin admin = admin()) {
      ConsumerGroupDescription d =
          admin.describeConsumerGroups(List.of(GROUP)).describedGroups().get(GROUP).get(10, TimeUnit.SECONDS);
      for (MemberDescription member : d.members()) {
        List<Integer> parts = new ArrayList<>();
        member.assignment().topicPartitions().forEach(tp -> parts.add(tp.partition()));
        parts.sort(null);
        m.put(member.clientId().replace("connector-consumer-", ""), parts);
      }
    } catch (Exception e) {
      m.put("error", List.of());
    }
    return m;
  }

  /** The connector task (clientId suffix) whose consumer currently owns BOTH partitions, or null. */
  private String taskOwningBoth() {
    for (Map.Entry<String, List<Integer>> e : owners().entrySet()) {
      if (e.getKey().startsWith(CONNECTOR + "-") && e.getValue().contains(0) && e.getValue().contains(1)) {
        return e.getKey();
      }
    }
    return null;
  }

  /** The task that does not own src-0 (the coordinator host owns src-0); 1 if unknown. */
  private int nonLeaderTask() {
    for (Map.Entry<String, List<Integer>> e : owners().entrySet()) {
      if (e.getValue().contains(0) && e.getKey().startsWith(CONNECTOR + "-")) {
        int leader = Integer.parseInt(e.getKey().substring(e.getKey().lastIndexOf('-') + 1));
        return 1 - leader;
      }
    }
    return 1;
  }

  private String taskStates() {
    StringBuilder sb = new StringBuilder();
    try {
      connect.connectorStatus(CONNECTOR).tasks().forEach(t -> sb.append(t.id()).append('=').append(t.state()).append(' '));
    } catch (Exception e) {
      sb.append("status-error");
    }
    return sb.toString().trim();
  }

  private void awaitCommitted(Duration timeout) throws Exception {
    if (!awaitCommittedQuiet(timeout)) {
      throw new AssertionError("source offsets not committed: committed=" + committed() + " produced=" + producedMap());
    }
  }

  private boolean awaitCommittedQuiet(Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Map<Integer, Long> target = producedMap();
    while (System.nanoTime() < deadline) {
      Map<Integer, Long> c = committed();
      boolean all = true;
      for (int p = 0; p < PARTS; p += 1) {
        all &= c.getOrDefault(p, -1L) >= target.get(p);
      }
      if (all) {
        return true;
      }
      Thread.sleep(500);
    }
    log("WARN committed offsets did not reach produced: committed=%s produced=%s", committed(), target);
    return false;
  }

  private Map<String, Integer> tableRows() throws Exception {
    Map<String, Integer> rows = new TreeMap<>();
    Table table = SharedCatalog.initRoot().loadTable(TableIdentifier.parse(TABLE));
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.merge(String.valueOf(record.getField("data")), 1, Integer::sum);
      }
    }
    return rows;
  }

  private void awaitTableStable() throws Exception {
    int last = -1;
    int stable = 0;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (stable < 3 && System.nanoTime() < deadline) {
      int n = tableRows().values().stream().mapToInt(Integer::intValue).sum();
      stable = n == last ? stable + 1 : 0;
      last = n;
      Thread.sleep(2000);
    }
  }

  private static String ranges(List<Long> offsets) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < offsets.size(); ) {
      int j = i;
      while (j + 1 < offsets.size() && offsets.get(j + 1) == offsets.get(j) + 1) {
        j += 1;
      }
      sb.append(sb.length() == 0 ? "" : ",").append(offsets.get(i));
      if (j > i) {
        sb.append("-").append(offsets.get(j));
      }
      i = j + 1;
    }
    return sb.toString();
  }

  /** Records below the committed source offset that are not in the table, right now. */
  private int missingNow() throws Exception {
    Map<Integer, Long> c = committed();
    Map<String, Integer> rows = tableRows();
    int missing = 0;
    for (int p = 0; p < PARTS; p += 1) {
      for (long o = 0; o < c.getOrDefault(p, 0L); o += 1) {
        if (!rows.containsKey("p" + p + "-" + o)) {
          missing += 1;
        }
      }
    }
    return missing;
  }

  private int report(String stage, boolean converged) throws Exception {
    Map<Integer, Long> c = committed();
    Map<String, Integer> rows = tableRows();
    int total = rows.values().stream().mapToInt(Integer::intValue).sum();
    int missing = 0;
    StringBuilder detail = new StringBuilder();
    for (int p = 0; p < PARTS; p += 1) {
      List<Long> lost = new ArrayList<>();
      for (long o = 0; o < c.getOrDefault(p, 0L); o += 1) {
        if (!rows.containsKey("p" + p + "-" + o)) {
          lost.add(o);
        }
      }
      missing += lost.size();
      if (!lost.isEmpty()) {
        detail.append(String.format("%n    p%d below committed offset %d, not in table: %d record(s) at offsets [%s]",
            p, c.get(p), lost.size(), ranges(lost)));
      }
    }
    long dups = rows.values().stream().filter(n -> n > 1).count();
    int snapshots = 0;
    for (Object ignored : SharedCatalog.initRoot().loadTable(TableIdentifier.parse(TABLE)).snapshots()) {
      snapshots += 1;
    }
    log("RESULT stage=%s scenario=%s mode=%s produced=%s committed=%s converged=%s rows=%d distinct=%d "
            + "snapshots=%d duplicates=%d missingBelowCommitted=%d resumeCalls=%d%s",
        stage, scenario, mode, producedMap(), c, converged, total, rows.size(), snapshots, dups, missing,
        resumeCalls.get(), detail);
    return missing;
  }

  void shutdown() {
    try {
      if (producer != null) {
        producer.close(Duration.ofSeconds(5));
      }
      if (connect != null) {
        connect.stop();
      }
    } catch (Exception e) {
      log("shutdown: %s", e);
    }
  }
}
JAVA
timeout 300 javac --release 17 -nowarn -encoding UTF-8 -cp "$DEPS:$B/classes/sink" -d "$B/classes/harness" \
  "$B/harness-src/mc2repro/ReproMC2.java" > "$B/javac-harness.log" 2>&1 || { cat "$B/javac-harness.log"; exit 1; }

# logging: thread names + the sink's close() steps + consumer revoke/assign/seek/wakeup lines
mkdir -p "$B/res"
cat > "$B/res/logback.xml" <<'EOF'
<configuration>
  <appender name="OUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder><pattern>%d{HH:mm:ss.SSS} %-5level [%thread] %logger{0}: %msg%n</pattern></encoder>
  </appender>
  <root level="WARN"><appender-ref ref="OUT"/></root>
  <logger name="org.apache.iceberg.connect.channel.CommitterImpl" level="INFO"/>
  <logger name="org.apache.iceberg.connect.channel.KafkaUtils" level="INFO"/>
  <logger name="org.apache.kafka.connect.runtime.Worker" level="INFO"/>
  <logger name="org.apache.kafka.connect.runtime.WorkerSinkTask" level="${MC2_WST_LEVEL:-INFO}"/>
  <logger name="org.apache.kafka.clients.consumer.internals.ConsumerCoordinator" level="INFO"/>
  <logger name="org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer" level="INFO"/>
  <logger name="org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient" level="DEBUG"/>
</configuration>
EOF
# Log analyzer: per task thread, classify every close()-time seek as committed()-threw vs
# committed()-ok. "Seeking to last committed offsets for worker N" (CommitterImpl:185) is logged
# right before KafkaUtils calls consumer.committed(); a following "Seeking to offset .. for
# partition" (ClassicKafkaConsumer) means committed() returned and the rewind ran; no such line
# before the next seek-enter on that thread means committed() threw (the rewind was skipped). A
# "Raising WakeupException" on the same thread in that window attributes the throw to a wakeup.
cat > "$B/analyze_log.py" <<'PY'
import re, sys
enter = re.compile(r'\[(task-thread-[^\]]+)\] CommitterImpl: Seeking to last committed offsets for worker (\S+?)\.')
seek  = re.compile(r'\[(task-thread-[^\]]+)\].*Seeking to offset \d+ for partition')
wake  = re.compile(r'\[(task-thread-[^\]]+)\].*Raising WakeupException in response to user wakeup')
per = {}
for line in open(sys.argv[1], errors='replace'):
    m = enter.search(line)
    if m: per.setdefault(m.group(1), []).append(('enter', line.strip())); continue
    m = seek.search(line)
    if m: per.setdefault(m.group(1), []).append(('seek', line.strip())); continue
    m = wake.search(line)
    if m: per.setdefault(m.group(1), []).append(('wake', line.strip())); continue
threw = ok = threw_with_wake = 0
examples = []
for thread, toks in per.items():
    for i, (kind, text) in enumerate(toks):
        if kind != 'enter': continue
        got_seek = False; saw_wake = False
        for kind2, text2 in toks[i+1:]:
            if kind2 == 'enter': break
            if kind2 == 'seek': got_seek = True; break
            if kind2 == 'wake': saw_wake = True
        if got_seek:
            ok += 1
        else:
            threw += 1
            if saw_wake:
                threw_with_wake += 1
                if len(examples) < 3: examples.append((thread, text))
print(f"close()-time seeks: committed_ok={ok} committed_THREW(no rewind)={threw} of which wakeup-attributed={threw_with_wake}")
for thread, text in examples:
    print(f"  THREW@ {text}")
PY

# ---------------------------------------------------------------------------------------------
# 4. Runs
status=0
for run in "${RUNS[@]}"; do
  # run spec: scenario:mode[:L1]   scenario=coop|eager  mode=control|wakeup
  scen="$(echo "$run" | cut -d: -f1)"; mode="$(echo "$run" | cut -d: -f2)"; variant="$(echo "$run" | cut -d: -f3)"
  name="${scen}_${mode}"; sinkdir="$B/classes/sink"; props="-Dmc2.rounds=${MC2_ROUNDS:-8}"
  if [ "$variant" = "L1" ]; then
    # select the injection-capable sink; whether injection is armed is decided by the harness
    # (focused inject mode arms it only across the single rebalance; wakeup mode arms it throughout)
    name="${name}_L1"; sinkdir="$B/classes/sink-l1"
    props="$props -Dmc2.wakeupIntervalMs=${MC2_WAKEUP_MS:-40}"
  fi
  log="$RUN_OUT/$name.log"
  echo "=== run $name (sink=$(basename "$sinkdir"), log: $log)"
  timeout 900 java -Xmx2g $props \
      --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
      --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED \
      -cp "$B/res:$B/classes/harness:$sinkdir:$DEPS" mc2repro.ReproMC2 "$scen" "$mode" > "$log" 2>&1
  rc=$?
  echo "    exit=$rc"
  grep -E "^\[repro .*\] (RESULT|baseline|traffic stopped|WARN)" -A4 "$log" | grep -v "^--$" | sed 's/^/    /'
  python3 -I "$B/analyze_log.py" "$log" | sed 's/^/    /'
  missing=$(grep -E "RESULT stage=after-rounds" "$log" | sed -E 's/.*missingBelowCommitted=([0-9]+).*/\1/' | head -1)
  missing2=$(grep -E "RESULT stage=after-restart-all-tasks" "$log" | sed -E 's/.*missingBelowCommitted=([0-9]+).*/\1/' | head -1)
  transient=$(grep -E "traffic stopped:" "$log" | sed -E 's/.*maxTransientMissing=([0-9]+).*/\1/' | head -1)
  echo "    maxTransientMissing=${transient:-?} (a hole seen only here, with final=0, means the mask re-read it)"
  echo "${missing:-ERR}" > "$RUN_OUT/$name.missing"
  echo "${missing2:-ERR}" > "$RUN_OUT/$name.missing2"
  echo "${transient:-ERR}" > "$RUN_OUT/$name.transient"
  [ "$rc" -ne 0 ] && [ "$rc" -ne 3 ] && status=1
done

echo ""
echo "=== summary (missing = records below the committed source offset that are not in the table)"
bug=0
for run in "${RUNS[@]}"; do
  scen="$(echo "$run" | cut -d: -f1)"; mode="$(echo "$run" | cut -d: -f2)"; variant="$(echo "$run" | cut -d: -f3)"
  n="${scen}_${mode}"; [ "$variant" = "L1" ] && n="${n}_L1"
  m="$(cat "$RUN_OUT/$n.missing" 2>/dev/null)"; m2="$(cat "$RUN_OUT/$n.missing2" 2>/dev/null)"
  echo "    $n: missing(after-rounds)=$m missing(after-restart)=$m2"
  if [ "$m" != "ERR" ] && [ "$m" != "" ] && [ "$m" -gt 0 ] 2>/dev/null; then bug=1; fi
  if [ "$m2" != "ERR" ] && [ "$m2" != "" ] && [ "$m2" -gt 0 ] 2>/dev/null; then bug=1; fi
done
[ "$status" -ne 0 ] && { echo "harness error (a run exited abnormally)"; exit "$status"; }
[ "$bug" -eq 1 ] && { echo "BUG TRIGGERED: records committed as source offsets are missing from the Iceberg table (data loss)"; exit 3; }
echo "no loss observed"; exit 0
