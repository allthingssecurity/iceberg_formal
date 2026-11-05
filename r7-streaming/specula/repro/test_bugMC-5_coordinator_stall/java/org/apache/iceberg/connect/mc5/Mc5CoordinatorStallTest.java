/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.mc5;

import static org.apache.iceberg.types.Types.NestedField.optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
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
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * MC-5 reproduction against the PRISTINE sink (apache/iceberg@9fffeb6, no instrumentation).
 *
 * <p>Topology: one JVM, embedded Kafka 3.9.2 broker (KRaft), 2-worker Kafka Connect cluster running
 * the real IcebergSinkConnector with 2 sink tasks (classic group protocol, CooperativeStickyAssignor),
 * a 2-partition source topic, a 1-partition control topic and one shared in-memory Iceberg table.
 *
 * <p>Trigger (real operations only): the coordinator host -- the owner of the first partition --
 * leaves the consumer group (mode {@code restart}: Connect REST task restart; mode {@code kickout}:
 * the task's put() is slow past max.poll.interval.ms because of catalog latency, as in the model
 * counterexample) while the other task receives the first partition and the leaving task's new
 * consumer re-joins. Level 0 uses only real configuration; Level 1 adds environment latency
 * ({@link DelayProxy} on the sink's Kafka bootstrap address, {@link Mc5Catalog#armStall}).
 *
 * <p>Oracle (black box): no live {@code iceberg-coord} thread in the JVM and no StartCommit on the
 * control topic while both tasks are RUNNING, the group is Stable and records keep flowing; the
 * table gets no new snapshot and the source offsets are not committed.
 */
public class Mc5CoordinatorStallTest {
  private static final String CONNECTOR = "iceberg-sink";
  private static final String GROUP = "connect-" + CONNECTOR;
  private static final String SRC_TOPIC = System.getProperty("mc5.topic", "src");
  private static final String CTL_TOPIC = "ctl";
  private static final String TABLE = "db.tbl";
  private static final int SRC_PARTITIONS = 2;
  private static final String COORD_THREAD = "iceberg-coord";
  private static final TopicPartition P0 = new TopicPartition(SRC_TOPIC, 0);
  private static final DateTimeFormatter TS =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final String mode = System.getProperty("mc5.mode", "restart");
  private final int level = Integer.getInteger("mc5.level", 0);
  private final int iterations = Integer.getInteger("mc5.iterations", 20);
  private final long commitIntervalMs = Long.getLong("mc5.commit.interval.ms", 2000L);
  private final long proxyDelayMs = Long.getLong("mc5.proxy.delay.ms", 0L);
  private final long observeSec = Long.getLong("mc5.stall.observe.sec", 90L);
  private final boolean heal = Boolean.parseBoolean(System.getProperty("mc5.heal", "true"));
  private final String heartbeatMs = System.getProperty("mc5.heartbeat.ms", "");
  private final long maxPollIntervalMs = Long.getLong("mc5.max.poll.interval.ms", 6000L);
  private final long kickoutExtraMs = Long.getLong("mc5.kickout.extra.ms", 1500L);
  private final String initialRebalanceDelayMs =
      System.getProperty("mc5.initial.rebalance.delay.ms", "3000");
  private final Path outDir = Paths.get(System.getProperty("mc5.out.dir", "build/mc5"));
  private final String runName = System.getProperty("mc5.run.name", mode + "-L" + level);
  private final String sutLog = System.getProperty("org.slf4j.simpleLogger.logFile", "");

  private EmbeddedConnectCluster connect;
  private KafkaProducer<String, String> producer;
  private Admin admin;
  private DelayProxy proxy;
  private Path timelineFile;

  private final Map<Integer, AtomicLong> produced = new ConcurrentHashMap<>();
  private volatile boolean running = true;
  private volatile boolean producing = true;
  private final List<Long> startCommitTimes = new CopyOnWriteArrayList<>();
  private final Map<String, Integer> ctlCounts = new ConcurrentHashMap<>();
  private final List<String> groupTransitions = new CopyOnWriteArrayList<>();
  private final List<Thread> background = new ArrayList<>();
  private final Map<String, Object> result = new LinkedHashMap<>();

  @BeforeEach
  void setUp() throws Exception {
    Files.createDirectories(outDir);
    timelineFile = outDir.resolve(runName + ".timeline.log");
    Files.deleteIfExists(timelineFile);

    Mc5Catalog catalog = Mc5Catalog.initRoot();
    catalog.createNamespace(Namespace.of("db"));
    catalog.createTable(
        TableIdentifier.parse(TABLE),
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of("format-version", "2", "write.format.default", "avro"));

    Properties brokerProps = new Properties();
    brokerProps.put("transaction.state.log.replication.factor", "1");
    brokerProps.put("transaction.state.log.min.isr", "1");
    brokerProps.put("transaction.state.log.num.partitions", "4");
    brokerProps.put("offsets.topic.num.partitions", "4");
    // broker default is 3000 ms; EmbeddedKafkaCluster would silently set 0
    brokerProps.put("group.initial.rebalance.delay.ms", initialRebalanceDelayMs);

    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("connector.client.config.override.policy", "All");
    workerProps.put("offset.flush.interval.ms", "500");
    workerProps.put("scheduled.rebalance.max.delay.ms", "1000");
    workerProps.put("task.shutdown.graceful.timeout.ms", "15000");

    connect =
        new EmbeddedConnectCluster.Builder()
            .name("mc5-" + runName)
            .numWorkers(2)
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();
    connect.start();
    connect.assertions().assertAtLeastNumWorkersAreUp(2, "Connect workers did not start");
    connect.kafka().createTopic(SRC_TOPIC, SRC_PARTITIONS);
    connect.kafka().createTopic(CTL_TOPIC, 1);

    String bootstrap = connect.kafka().bootstrapServers();
    admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    warmUpTransactions(bootstrap);

    if (proxyDelayMs > 0) {
      String hostPort = bootstrap.split(",")[0];
      int colon = hostPort.lastIndexOf(':');
      proxy =
          new DelayProxy(
              hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
    }

    note(
        "setup: run="
            + runName
            + " mode="
            + mode
            + " level="
            + level
            + " kafkaBootstrap="
            + bootstrap
            + " sinkBootstrap="
            + sinkBootstrap()
            + " heartbeat.interval.ms="
            + (heartbeatMs.isEmpty() ? "default(3000)" : heartbeatMs)
            + " group.initial.rebalance.delay.ms="
            + initialRebalanceDelayMs
            + " commitIntervalMs="
            + commitIntervalMs
            + " proxyDelayMs="
            + proxyDelayMs);
  }

  @AfterEach
  void tearDown() throws Exception {
    running = false;
    producing = false;
    for (Thread thread : background) {
      thread.join(5000);
    }

    try {
      producer.close(Duration.ofSeconds(5));
      admin.close(Duration.ofSeconds(5));
      if (proxy != null) {
        proxy.close();
      }

      connect.stop();
    } catch (Exception e) {
      note("teardown error: " + e);
    }
  }

  // ---------------------------------------------------------------------------------------------

  @Test
  void coordinatorElectionIsNeverRetried() throws Exception {
    result.put("run", runName);
    result.put("mode", mode);
    result.put("level", level);
    result.put("heartbeatIntervalMs", heartbeatMs.isEmpty() ? "default" : heartbeatMs);
    result.put("proxyDelayMs", proxyDelayMs);
    result.put("commitIntervalMs", commitIntervalMs);
    result.put(
        "assignor",
        "range".equals(System.getProperty("mc5.assignor", "cooperative"))
            ? "RangeAssignor (eager)"
            : "CooperativeStickyAssignor");
    result.put("sinkSource", "pristine apache/iceberg@9fffeb6");

    connect.configureConnector(CONNECTOR, connectorConfig());
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 2, "Connector did not start");
    startObservers();

    boolean steady = awaitSteadyCoordinator(90_000L);
    note(
        "initial steady state reached="
            + steady
            + " coordinatorThreads="
            + coordinatorThreads()
            + " group="
            + render(describe())
            + " startCommits="
            + startCommitTimes.size()
            + " table="
            + tableStats());
    result.put("initialSteady", steady);
    List<Long> initDelays = new ArrayList<>();
    for (String value : System.getProperty("mc5.catalog.init.delays", "").split(",")) {
      if (!value.isBlank()) {
        initDelays.add(Long.parseLong(value.trim()));
      }
    }

    result.put("catalogInitDelaysMs", initDelays);

    Integer stallIteration = null;
    long stallIterationStart = -1L;
    long stallSettled = -1L;
    List<Map<String, Object>> iterationLog = new ArrayList<>();

    if (!steady) {
      // the connector start itself may already have missed the election
      long settled = awaitSettled(30_000L);
      Thread.sleep(3 * commitIntervalMs + 1000L);
      if (settled > 0 && coordinatorThreads() == 0 && startCommitsSince(settled) == 0) {
        stallIteration = 0;
        stallIterationStart = settled - 30_000L;
        stallSettled = settled;
        note("STALL already after connector start (no coordinator was ever elected)");
      } else {
        result.put("aborted", "no initial steady state and no stall");
        writeResult();
        throw new AssertionError("no initial steady state; see " + timelineFile);
      }
    }

    for (int i = 1; stallIteration == null && i <= iterations; i += 1) {
      ConsumerGroupDescription before = describe();
      Integer host = ownerTask(before, P0);
      if (host == null) {
        note("iteration " + i + ": no owner of " + P0 + " (" + render(before) + "), waiting");
        awaitSettled(30_000L);
        continue;
      }

      long start = System.currentTimeMillis();
      int snapshotsBefore = snapshotCount();

      if ("kickout".equals(mode)) {
        Mc5Catalog.armStall(
            "task-thread-" + CONNECTOR + "-" + host, maxPollIntervalMs + kickoutExtraMs);
        note(
            "iteration "
                + i
                + ": kick-out of task "
                + host
                + " (owner of "
                + P0
                + " = coordinator host): its next catalog loadTable on put() stalls "
                + (maxPollIntervalMs + kickoutExtraMs)
                + " ms > max.poll.interval.ms="
                + maxPollIntervalMs);
        boolean stallStarted =
            await("kick-out stall to start", 30_000L, () -> Mc5Catalog.stallStartedAt() > start);
        if (proxy != null && stallStarted) {
          // latency only for connections opened while the host is stalled (the survivor's open()),
          // ending 300 ms before the host wakes up so its own lost-close is not delayed
          proxy.armUntil(
              proxyDelayMs,
              Mc5Catalog.stallStartedAt() + maxPollIntervalMs + kickoutExtraMs - 300L);
        }

        boolean stallDone =
            await("kick-out stall to end", 60_000L, () -> Mc5Catalog.stallEndedAt() > start);
        if (proxy != null) {
          proxy.disarm();
        }

        note(
            "iteration "
                + i
                + ": catalog stall "
                + (stallDone
                    ? "ran " + TS.format(Instant.ofEpochMilli(Mc5Catalog.stallStartedAt()))
                    + " -> " + TS.format(Instant.ofEpochMilli(Mc5Catalog.stallEndedAt()))
                    : "did not complete")
                + (proxy != null ? ", proxy delayed " + proxy.delayedConnections() + " connections" : ""));
      } else {
        if (proxy != null) {
          proxy.arm(proxyDelayMs);
        }

        long initDelay = initDelays.isEmpty() ? 0L : initDelays.get((i - 1) % initDelays.size());
        Mc5Catalog.setInitDelay(initDelay);
        note(
            "iteration "
                + i
                + ": Connect REST restart of task "
                + host
                + " (owner of "
                + P0
                + " = coordinator host), catalog init latency "
                + initDelay
                + " ms, group before: "
                + render(before));
        connect.restartTask(CONNECTOR, host);
        Thread.sleep(1000L);
      }

      long settled = awaitSettled(90_000L);
      if (proxy != null) {
        proxy.disarm();
      }

      if (settled < 0) {
        List<Integer> failed = new ArrayList<>();
        String reason = "";
        for (ConnectorStateInfo.TaskState state : connect.connectorStatus(CONNECTOR).tasks()) {
          if ("FAILED".equals(state.state())) {
            failed.add(state.id());
            String trace = state.trace() != null ? state.trace() : "";
            int cause = trace.lastIndexOf("Caused by: ");
            String line = cause >= 0 ? trace.substring(cause) : trace;
            reason = line.split("\n")[0];
          }
        }

        if (!failed.isEmpty()) {
          note(
              "iteration "
                  + i
                  + ": task(s) "
                  + failed
                  + " FAILED -- not the MC-5 path, recovering with a REST restart (no timing"
                  + " assistance): "
                  + reason);
          Map<String, Object> failedIt = new LinkedHashMap<>();
          failedIt.put("iteration", i);
          failedIt.put("start", TS.format(Instant.ofEpochMilli(start)));
          failedIt.put("coordinatorHostBefore", host);
          failedIt.put("catalogInitDelayMs", Mc5Catalog.initDelay());
          failedIt.put("settled", "no");
          failedIt.put("ownerOfP0After", ownerTask(describe(), P0));
          failedIt.put("groupAfter", render(describe()));
          failedIt.put("coordinatorThreads", coordinatorThreads());
          failedIt.put("startCommitsSinceSettled", -1);
          failedIt.put("taskFailed", failed);
          failedIt.put("taskFailedReason", reason);
          iterationLog.add(failedIt);
          long savedDelay = Mc5Catalog.initDelay();
          Mc5Catalog.setInitDelay(0L);
          Mc5Catalog.disarmStall();
          for (int task : failed) {
            connect.restartTask(CONNECTOR, task);
          }

          awaitSettled(90_000L);
          boolean recovered = awaitSteadyCoordinator(90_000L);
          Mc5Catalog.setInitDelay(savedDelay);
          note("iteration " + i + ": recovered=" + recovered + " group=" + render(describe()));
          if (!recovered) {
            break;
          }

          continue;
        }
      }

      Thread.sleep(3 * commitIntervalMs + 1000L);
      long coords = coordinatorThreads();
      int startCommits = settled > 0 ? startCommitsSince(settled) : -1;
      ConsumerGroupDescription after = describe();
      Integer ownerAfter = ownerTask(after, P0);

      Map<String, Object> it = new LinkedHashMap<>();
      it.put("iteration", i);
      it.put("start", TS.format(Instant.ofEpochMilli(start)));
      it.put("coordinatorHostBefore", host);
      it.put("catalogInitDelayMs", Mc5Catalog.initDelay());
      it.put("settled", settled > 0 ? TS.format(Instant.ofEpochMilli(settled)) : "no");
      it.put("ownerOfP0After", ownerAfter);
      it.put("groupAfter", render(after));
      it.put("coordinatorThreads", coords);
      it.put("startCommitsSinceSettled", startCommits);
      it.put("snapshotsBefore", snapshotsBefore);
      it.put("snapshotsAfter", snapshotCount());
      if (proxy != null) {
        it.put("proxyDelayedConnections", proxy.delayedConnections());
      }

      iterationLog.add(it);
      note("iteration " + i + " result: " + it);

      if (settled > 0 && coords == 0 && startCommits == 0) {
        stallIteration = i;
        stallIterationStart = start;
        stallSettled = settled;
        break;
      }

      if (coords > 1) {
        note("iteration " + i + ": NOTE " + coords + " coordinator threads (duplicate, not MC-5)");
      }

      boolean committedAgain =
          await("a table commit after iteration " + i, 60_000L, () -> snapshotCount() > snapshotsBefore);
      it.put("committedAfterIteration", committedAgain);
      if (!committedAgain || !awaitSteadyCoordinator(60_000L)) {
        note("iteration " + i + ": no steady coordinator afterwards, re-checking for a stall");
        long reSettled = awaitSettled(30_000L);
        Thread.sleep(3 * commitIntervalMs + 1000L);
        if (reSettled > 0 && coordinatorThreads() == 0 && startCommitsSince(reSettled) == 0) {
          stallIteration = i;
          stallIterationStart = start;
          stallSettled = reSettled;
        }

        break;
      }
    }

    result.put("iterations", iterationLog);
    result.put("stallDetected", stallIteration != null);
    if (stallIteration != null) {
      result.put("stallIteration", stallIteration);
      observeStall(stallIteration, stallIterationStart, stallSettled);
      if (heal) {
        healAndCheck();
      }
    } else {
      note("NO STALL in " + iterationLog.size() + " iterations");
    }

    result.put("groupTransitions", groupTransitions);
    writeResult();
  }

  // ---------------------------------------------------------------------------------------------
  // Stall evidence

  private void observeStall(int iteration, long iterationStart, long settled) throws Exception {
    note(
        "=== STALL after iteration "
            + iteration
            + ": both tasks RUNNING, group Stable, no live coordinator; observing "
            + observeSec
            + " s ===");
    Map<String, Object> before = stateSample();
    note("STALL begin: " + before);
    int startCommitsBefore = startCommitTimes.size();
    List<Map<String, Object>> samples = new ArrayList<>();
    long begin = System.currentTimeMillis();
    while (System.currentTimeMillis() - begin < observeSec * 1000L) {
      Thread.sleep(15_000L);
      Map<String, Object> sample = stateSample();
      sample.put("elapsedSec", (System.currentTimeMillis() - begin) / 1000L);
      samples.add(sample);
      note("STALL t+" + sample.get("elapsedSec") + "s: " + sample);
    }

    Map<String, Object> after = stateSample();
    Map<String, Object> stall = new LinkedHashMap<>();
    stall.put("iteration", iteration);
    stall.put("settledAt", TS.format(Instant.ofEpochMilli(settled)));
    stall.put("observedSec", (System.currentTimeMillis() - begin) / 1000L);
    stall.put("sinceSettledSec", (System.currentTimeMillis() - settled) / 1000L);
    stall.put("commitIntervalsMissed", (System.currentTimeMillis() - settled) / commitIntervalMs);
    stall.put("startCommitsDuringStall", startCommitTimes.size() - startCommitsBefore);
    stall.put("startCommitsSinceSettled", startCommitsSince(settled));
    stall.put("before", before);
    stall.put("after", after);
    stall.put("samples", samples);
    stall.put("rightBugCheck", rightBugCheck(iterationStart, settled, after));
    result.put("stall", stall);
  }

  /** SUT-log evidence that the stall is the MC-5 mechanism (open() saw no assignments). */
  private Map<String, Object> rightBugCheck(long iterationStart, long settled, Map<String, Object> after)
      throws IOException {
    Map<String, Object> check = new LinkedHashMap<>();
    Integer holder = ownerTaskFromRendered(String.valueOf(after.get("group")));
    check.put("holderOfP0", holder);
    List<String> window =
        sutLogWindow(iterationStart - 2000L, settled + 3 * commitIntervalMs + 2000L);
    check.put("sutLogExcerpt", window);
    if (holder != null) {
      String thread = "[task-thread-" + CONNECTOR + "-" + holder + "]";
      String committer = "Committer " + CONNECTOR + "-" + holder + " ";
      boolean addedP0 = false;
      boolean warnAfterAdd = false;
      boolean electedAfterAdd = false;
      Pattern added = Pattern.compile("Added partitions \\(assigned - owned\\):\\s*\\[([^\\]]*)\\]");
      for (String line : window) {
        java.util.regex.Matcher addedMatcher = added.matcher(line);
        boolean addsP0 =
            addedMatcher.find()
                && List.of(addedMatcher.group(1).split(",\\s*")).contains(P0.toString());
        if (line.contains(thread)
            && line.contains("groupId=" + GROUP + "]")
            && line.contains("Updating assignment with")
            && addsP0) {
          addedP0 = true;
          warnAfterAdd = false;
          electedAfterAdd = false;
        } else if (addedP0
            && line.contains(committer + "found no partitions assigned across all members")) {
          warnAfterAdd = true;
        } else if (addedP0
            && (line.contains(committer + "received leader partition")
                || line.contains("Task " + CONNECTOR + "-" + holder + " elected leader"))) {
          electedAfterAdd = true;
        }
      }

      check.put("holderOpenedP0InWindow", addedP0);
      check.put("holderLoggedNoAssignmentsAfterOpen", warnAfterAdd);
      check.put("holderElectedAfterOpen", electedAfterAdd);
    }

    List<String> nonStable =
        groupTransitions.stream()
            .filter(line -> !line.contains(" GROUP Stable "))
            .filter(line -> inWindow(line, iterationStart - 2000L, settled + 1000L))
            .collect(Collectors.toList());
    check.put("nonStableDescribesInWindow", nonStable);
    return check;
  }

  private void healAndCheck() throws Exception {
    ConsumerGroupDescription desc = describe();
    Integer holder = ownerTask(desc, P0);
    Map<String, Object> healing = new LinkedHashMap<>();
    healing.put("holderOfP0", holder);
    if (holder == null) {
      result.put("healing", healing);
      return;
    }

    // remove all timing assistance first, otherwise the heal restart can hit the same race again
    Mc5Catalog.setInitDelay(0L);
    Mc5Catalog.disarmStall();
    if (proxy != null) {
      proxy.disarm();
    }

    long start = System.currentTimeMillis();
    int snapshots = snapshotCount();
    note(
        "HEAL: Connect REST restart of task "
            + holder
            + " (holds "
            + P0
            + ", never elected), all timing assistance removed");
    connect.restartTask(CONNECTOR, holder);
    boolean coordinatorBack =
        await(
            "coordinator after heal",
            120_000L,
            () -> coordinatorThreads() >= 1 && startCommitsSince(start) > 0);
    boolean newSnapshot = await("snapshot after heal", 120_000L, () -> snapshotCount() > snapshots);
    healing.put("coordinatorBack", coordinatorBack);
    healing.put("newSnapshotAfterHeal", newSnapshot);
    healing.put("secondsToFirstStartCommit", firstStartCommitAfter(start));
    note("HEAL: coordinatorBack=" + coordinatorBack + " newSnapshot=" + newSnapshot);

    // drain: stop producing and let the sink commit everything, then check exactly-once
    producing = false;
    Thread.sleep(1000L);
    Map<Integer, Long> producedNow = producedSnapshot();
    await(
        "source offsets committed up to produced",
        120_000L,
        () -> {
          try {
            Map<Integer, Long> committed = committedSourceOffsets();
            for (Map.Entry<Integer, Long> entry : producedNow.entrySet()) {
              Long offset = committed.get(entry.getKey());
              if (offset == null || offset < entry.getValue()) {
                return false;
              }
            }

            return true;
          } catch (Exception e) {
            return false;
          }
        });
    Thread.sleep(2 * commitIntervalMs);
    healing.put("final", stateSample());
    healing.put("exactlyOnce", exactlyOnceSummary());
    note("FINAL: " + healing.get("exactlyOnce"));
    result.put("healing", healing);
  }

  private Map<String, Object> exactlyOnceSummary() throws Exception {
    Map<String, Integer> rows = new TreeMap<>();
    Table table = Mc5Catalog.root().loadTable(TableIdentifier.parse(TABLE));
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.merge(String.valueOf(record.getField("data")), 1, Integer::sum);
      }
    }

    Map<Integer, Long> committed = committedSourceOffsets();
    List<String> duplicates = new ArrayList<>();
    rows.forEach(
        (key, n) -> {
          if (n > 1) {
            duplicates.add(key + " x" + n);
          }
        });
    List<String> missingBelowCommitted = new ArrayList<>();
    committed.forEach(
        (partition, offset) -> {
          for (long o = 0; o < offset; o += 1) {
            if (!rows.containsKey("p" + partition + "-" + o)) {
              missingBelowCommitted.add("p" + partition + "-" + o);
            }
          }
        });
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("produced", producedSnapshot());
    summary.put("sourceCommitted", committed);
    summary.put("rowsInTable", rows.values().stream().mapToInt(Integer::intValue).sum());
    summary.put("distinctRows", rows.size());
    summary.put("duplicates", duplicates.size());
    summary.put("missingBelowCommitted", missingBelowCommitted.size());
    summary.put(
        "missingBelowCommittedSample",
        missingBelowCommitted.subList(0, Math.min(10, missingBelowCommitted.size())));
    return summary;
  }

  // ---------------------------------------------------------------------------------------------
  // Connector and observers

  private String sinkBootstrap() {
    return proxy != null ? "127.0.0.1:" + proxy.port() : connect.kafka().bootstrapServers();
  }

  private Map<String, String> connectorConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("connector.class", IcebergSinkConnector.class.getName());
    config.put("tasks.max", "2");
    config.put("topics", SRC_TOPIC);
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("value.converter.schemas.enable", "false");
    config.put(
        "consumer.override.partition.assignment.strategy",
        "range".equals(System.getProperty("mc5.assignor", "cooperative"))
            ? "org.apache.kafka.clients.consumer.RangeAssignor"
            : "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    if (!heartbeatMs.isEmpty()) {
      config.put("consumer.override.heartbeat.interval.ms", heartbeatMs);
    }

    if ("kickout".equals(mode)) {
      config.put("consumer.override.max.poll.interval.ms", String.valueOf(maxPollIntervalMs));
    }

    config.put("iceberg.tables", TABLE);
    config.put("iceberg.catalog.catalog-impl", Mc5Catalog.class.getName());
    config.put("iceberg.control.topic", CTL_TOPIC);
    config.put("iceberg.control.commit.interval-ms", String.valueOf(commitIntervalMs));
    config.put(
        "iceberg.control.commit.timeout-ms", System.getProperty("mc5.commit.timeout.ms", "20000"));
    config.put("iceberg.kafka.bootstrap.servers", sinkBootstrap());
    return config;
  }

  private void startObservers() {
    startDaemon("mc5-producer", this::produceLoop);
    startDaemon("mc5-ctl-watch", this::watchControlTopic);
    startDaemon("mc5-group-sampler", this::sampleGroup);
  }

  private void startDaemon(String name, Runnable body) {
    Thread thread = new Thread(body, name);
    thread.setDaemon(true);
    thread.start();
    background.add(thread);
  }

  private void produceLoop() {
    long id = 0;
    long period = Long.getLong("mc5.produce.period.ms", 200L);
    while (running) {
      if (producing) {
        for (int partition = 0; partition < SRC_PARTITIONS; partition += 1) {
          AtomicLong counter = produced.computeIfAbsent(partition, k -> new AtomicLong());
          long offset = counter.get();
          String value = "{\"id\": " + id + ", \"data\": \"p" + partition + "-" + offset + "\"}";
          try {
            producer
                .send(new ProducerRecord<>(SRC_TOPIC, partition, "k" + id, value))
                .get(10, TimeUnit.SECONDS);
            counter.incrementAndGet();
            id += 1;
          } catch (Exception e) {
            if (running) {
              note("produce failed: " + e);
            }
          }
        }
      }

      sleepQuietly(period);
    }
  }

  private void watchControlTopic() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
      TopicPartition tp = new TopicPartition(CTL_TOPIC, 0);
      consumer.assign(List.of(tp));
      consumer.seekToBeginning(List.of(tp));
      while (running) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
          Event event = AvroUtil.decode(record.value());
          PayloadType type = event.payload().type();
          ctlCounts.merge(type.name(), 1, Integer::sum);
          if (type == PayloadType.START_COMMIT) {
            startCommitTimes.add(System.currentTimeMillis());
          }
        }
      }
    } catch (Exception e) {
      if (running) {
        note("control-topic watcher error: " + e);
      }
    }
  }

  /** Records every change of the described Connect group (read-only Admin describe). */
  private void sampleGroup() {
    long period = Long.getLong("mc5.sample.period.ms", 25L);
    try (Admin sampler =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers()))) {
      String last = null;
      while (running) {
        try {
          ConsumerGroupDescription desc =
              sampler
                  .describeConsumerGroups(List.of(GROUP))
                  .describedGroups()
                  .get(GROUP)
                  .get(5, TimeUnit.SECONDS);
          String rendered = render(desc);
          if (!rendered.equals(last)) {
            String line = TS.format(Instant.now()) + " GROUP " + rendered;
            groupTransitions.add(line);
            appendTimeline(line);
            last = rendered;
          }
        } catch (Exception e) {
          // transient describe errors are not interesting here
        }

        sleepQuietly(period);
      }
    }
  }

  private void warmUpTransactions(String bootstrap) {
    for (int i = 0; i < 8; i += 1) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "mc5-warmup-" + i);
      try (KafkaProducer<String, String> warm =
          new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
        warm.initTransactions();
        warm.beginTransaction();
        warm.commitTransaction();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Observations

  private static long coordinatorThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> COORD_THREAD.equals(thread.getName()) && thread.isAlive())
        .count();
  }

  private ConsumerGroupDescription describe() throws Exception {
    return admin
        .describeConsumerGroups(List.of(GROUP))
        .describedGroups()
        .get(GROUP)
        .get(10, TimeUnit.SECONDS);
  }

  private static String render(ConsumerGroupDescription desc) {
    TreeMap<String, String> members = new TreeMap<>();
    for (MemberDescription member : desc.members()) {
      members.put(
          taskOf(member.clientId()),
          member.assignment().topicPartitions().stream()
              .map(TopicPartition::toString)
              .sorted()
              .collect(Collectors.joining(",", "[", "]")));
    }

    return desc.state() + " " + members;
  }

  private static String taskOf(String clientId) {
    return "task" + clientId.substring(clientId.lastIndexOf('-') + 1);
  }

  private static Integer ownerTask(ConsumerGroupDescription desc, TopicPartition tp) {
    for (MemberDescription member : desc.members()) {
      if (member.assignment().topicPartitions().contains(tp)) {
        String clientId = member.clientId();
        return Integer.parseInt(clientId.substring(clientId.lastIndexOf('-') + 1));
      }
    }

    return null;
  }

  private static Integer ownerTaskFromRendered(String rendered) {
    java.util.regex.Matcher matcher =
        Pattern.compile("task(\\d+)=\\[[^\\]]*\\b" + Pattern.quote(P0.toString()) + "\\b")
            .matcher(rendered);
    return matcher.find() ? Integer.parseInt(matcher.group(1)) : null;
  }

  private static boolean balanced(ConsumerGroupDescription desc) {
    if (desc.state() != ConsumerGroupState.STABLE || desc.members().size() != 2) {
      return false;
    }

    for (MemberDescription member : desc.members()) {
      if (member.assignment().topicPartitions().size() != 1) {
        return false;
      }
    }

    return true;
  }

  private Map<Integer, String> taskStates() {
    Map<Integer, String> states = new TreeMap<>();
    ConnectorStateInfo info = connect.connectorStatus(CONNECTOR);
    for (ConnectorStateInfo.TaskState state : info.tasks()) {
      states.put(state.id(), state.state());
    }

    return states;
  }

  private int startCommitsSince(long ts) {
    int count = 0;
    for (long time : startCommitTimes) {
      if (time > ts) {
        count += 1;
      }
    }

    return count;
  }

  private Object firstStartCommitAfter(long ts) {
    for (long time : startCommitTimes) {
      if (time > ts) {
        return (time - ts) / 1000.0;
      }
    }

    return null;
  }

  private int snapshotCount() {
    Table table = Mc5Catalog.root().loadTable(TableIdentifier.parse(TABLE));
    int count = 0;
    for (Snapshot ignored : table.snapshots()) {
      count += 1;
    }

    return count;
  }

  private Map<String, Object> tableStats() {
    Map<String, Object> stats = new LinkedHashMap<>();
    Table table = Mc5Catalog.root().loadTable(TableIdentifier.parse(TABLE));
    int snapshots = 0;
    for (Snapshot ignored : table.snapshots()) {
      snapshots += 1;
    }

    long rows = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record ignored : records) {
        rows += 1;
      }
    } catch (IOException e) {
      rows = -1;
    }

    Snapshot current = table.currentSnapshot();
    stats.put("snapshots", snapshots);
    stats.put("rows", rows);
    stats.put(
        "lastCommitTask", current != null ? current.summary().get("kafka.connect.task-id") : null);
    return stats;
  }

  private Map<Integer, Long> producedSnapshot() {
    Map<Integer, Long> snapshot = new TreeMap<>();
    produced.forEach((partition, counter) -> snapshot.put(partition, counter.get()));
    return snapshot;
  }

  private Map<Integer, Long> committedSourceOffsets() throws Exception {
    Map<Integer, Long> committed = new TreeMap<>();
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin
            .listConsumerGroupOffsets(GROUP)
            .partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS);
    offsets.forEach(
        (tp, offset) -> {
          if (offset != null && SRC_TOPIC.equals(tp.topic())) {
            committed.put(tp.partition(), offset.offset());
          }
        });
    return committed;
  }

  private Map<Integer, Long> endOffsets() throws Exception {
    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
    for (int partition = 0; partition < SRC_PARTITIONS; partition += 1) {
      request.put(new TopicPartition(SRC_TOPIC, partition), OffsetSpec.latest());
    }

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
        admin.listOffsets(request).all().get(10, TimeUnit.SECONDS);
    Map<Integer, Long> end = new TreeMap<>();
    offsets.forEach((tp, info) -> end.put(tp.partition(), info.offset()));
    return end;
  }

  private Map<String, Object> stateSample() throws Exception {
    Map<String, Object> sample = new LinkedHashMap<>();
    sample.put("ts", TS.format(Instant.now()));
    sample.put("coordinatorThreads", coordinatorThreads());
    sample.put("startCommitsTotal", startCommitTimes.size());
    sample.put("controlTopic", new TreeMap<>(ctlCounts));
    sample.put("table", tableStats());
    sample.put("produced", producedSnapshot());
    sample.put("sourceEndOffsets", endOffsets());
    sample.put("sourceCommitted", committedSourceOffsets());
    sample.put("group", render(describe()));
    sample.put("tasks", taskStates());
    return sample;
  }

  // ---------------------------------------------------------------------------------------------
  // Waiting

  private boolean awaitSteadyCoordinator(long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (balanced(describe())
          && coordinatorThreads() >= 1
          && startCommitsSince(System.currentTimeMillis() - 3 * commitIntervalMs) > 0
          && snapshotCount() > 0) {
        return true;
      }

      Thread.sleep(250L);
    }

    return false;
  }

  /** Group Stable, one partition per member and both tasks RUNNING for 4 samples in a row. */
  private long awaitSettled(long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    int ok = 0;
    while (System.currentTimeMillis() < deadline) {
      ConsumerGroupDescription desc = describe();
      Map<Integer, String> states = taskStates();
      boolean tasksRunning =
          states.size() == 2 && states.values().stream().allMatch("RUNNING"::equals);
      if (balanced(desc) && tasksRunning) {
        ok += 1;
        if (ok >= 4) {
          return System.currentTimeMillis();
        }
      } else {
        ok = 0;
      }

      Thread.sleep(400L);
    }

    return -1L;
  }

  private static boolean await(String what, long timeoutMs, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }

      Thread.sleep(100L);
    }

    System.out.println("[MC5] timed out waiting for " + what);
    return false;
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Output

  private void note(String message) {
    String line = TS.format(Instant.now()) + " [MC5] " + message;
    System.out.println(line);
    appendTimeline(line);
  }

  private synchronized void appendTimeline(String line) {
    if (timelineFile == null) {
      return;
    }

    try {
      Files.write(
          timelineFile,
          (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      // ignore
    }
  }

  private static boolean inWindow(String line, long fromEpochMs, long toEpochMs) {
    try {
      LocalTime time = LocalTime.parse(line.substring(0, 12));
      LocalTime from = LocalTime.ofInstant(Instant.ofEpochMilli(fromEpochMs), ZoneId.systemDefault());
      LocalTime to = LocalTime.ofInstant(Instant.ofEpochMilli(toEpochMs), ZoneId.systemDefault());
      return !time.isBefore(from) && !time.isAfter(to);
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static final Pattern SUT_LINES =
      Pattern.compile(
          "CommitterImpl|elected leader|Coordinator iceberg-sink|CoordinatorThread"
              + "|groupId="
              + Pattern.quote(GROUP)
              + "\\].*(Updating assignment with|Finished assignment for group"
              + "|Revoke previously assigned partitions"
              + "|Successfully synced group|Successfully joined group|Request joining group due to"
              + "|Giving away all assigned partitions as lost|Lost previously assigned partitions"
              + "|consumer poll timeout has expired|Member .* sending LeaveGroup|Resetting generation)");

  private List<String> sutLogWindow(long fromEpochMs, long toEpochMs) throws IOException {
    List<String> lines = new ArrayList<>();
    if (sutLog.isEmpty() || !Files.exists(Paths.get(sutLog))) {
      return lines;
    }

    // fold continuation lines (Kafka logs the assignment as a tab-indented multi-line message)
    List<String> entries = new ArrayList<>();
    for (String line : Files.readAllLines(Paths.get(sutLog), StandardCharsets.UTF_8)) {
      if (!entries.isEmpty() && (line.startsWith("\t") || line.startsWith(" "))) {
        int last = entries.size() - 1;
        entries.set(last, entries.get(last) + " | " + line.trim());
      } else if (!line.isEmpty()) {
        entries.add(line);
      }
    }

    for (String entry : entries) {
      if (entry.length() > 12
          && inWindow(entry, fromEpochMs, toEpochMs)
          && SUT_LINES.matcher(entry).find()) {
        String compact =
            entry
                .replace("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator", "CC")
                .replace("org.apache.iceberg.connect.channel.", "");
        lines.add(compact.length() > 520 ? compact.substring(0, 520) + "..." : compact);
      }
    }

    return lines;
  }

  private void writeResult() throws IOException {
    MAPPER.writeValue(outDir.resolve(runName + ".result.json").toFile(), result);
    note("result written to " + outDir.resolve(runName + ".result.json"));
  }
}
