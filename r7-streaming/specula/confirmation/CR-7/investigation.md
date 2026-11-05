# CR-7 — Phase 1 investigation (evidence only)

Finding: a terminated coordinator never leaves `Channel.consumeAvailable` while control-topic
traffic continues, so the zombie keeps the `-coord` partition and the newly elected coordinator
is starved.

Source: code review (Scenario 4 residual, modeling-brief §6.2 T-5 / analysis-report F6). The
model contains the behaviour (no `terminated` check in `CoordConsume`), but no invariant targets
it and there is no counterexample, so **Source = Code Review**.

Code base: upstream apache/iceberg `9fffeb6` (2026-11-02). All line numbers below refer to the
**pristine** files (`git show HEAD:<path>`). The worktree carries the TLA trace instrumentation
as uncommitted changes, so the line numbers in the working copy differ.

Persistent-findings registry: `specula findings lookup --query consumeAvailable --query
CoordinatorThread --query terminated --query "zombie coordinator" --query CoordConsume` → `[]`
(no prior record).

## Step 1 — Code audit

### Cited code (pristine)

`channel/Channel.java:122-152`, `consumeAvailable`:
```java
ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
while (!records.isEmpty()) {
  for (ConsumerRecord<String, byte[]> record : records) {
    ... controlTopicOffsets.merge(...);           // every record, any connector
    Event event = AvroUtil.decode(record.value());
    if (event.groupId().equals(connectGroupId)) { // filter by connector AFTER counting
      receive(new Envelope(...));
    }
  }
  records = consumer.poll(pollDuration);          // loop while ANY record arrived
}
```
- The loop condition is "the last poll returned ≥1 record". It does not filter by connector
  group: records of *other* connectors on the same control topic keep the loop running.
- The method does not check any `terminated` flag. `Channel` has none; `Coordinator.terminated`
  is private to `Coordinator`.

`channel/Coordinator.java:77` `POLL_DURATION = Duration.ofSeconds(1)`;
`Coordinator.java:126-141` `process()` → `consumeAvailable(POLL_DURATION)` (`:136`), then
`isCommitTimedOut()` (`:138`).
`Coordinator.java:434-447` `terminate()`: sets `terminated = true`, `exec.shutdownNow()`, waits
≤ 1 min for the *commit executor*. It does not interrupt the coordinator thread, call
`consumer.wakeup()`, or close the consumer.
`Coordinator.java:303-306`: the only read of `terminated` is inside `commitToTable` (abort before
the table commit).

`channel/CoordinatorThread.java:49-57`:
```java
while (!terminated) {
  try { coordinator.process(); } catch (Exception e) { error = e; terminated = true; }
}
try { coordinator.stop(); } ...   // :59-63 → Channel.stop(): producer/consumer/admin close
```
`terminated` is re-checked only between `process()` calls. `coordinator.stop()` (the only place
the `-coord` consumer is closed, `Channel.java:235-240`) runs only after the loop is left.
`CoordinatorThread.java:83-86` `terminate()`: sets the flag and calls `coordinator.terminate()`.
It does not join.

`IcebergSinkConfig.java:101`: `DEFAULT_CONTROL_TOPIC = "control-iceberg"`. Every connector that
does not override `iceberg.control.topic` shares it. The docs (`docs/docs/kafka-connect.md:261-273`)
create it with `--partitions 1`, and auto-creation uses the broker default `num.partitions=1`.

### Call chain (public entry points → cited code)

1. Coordinator start: Connect runtime → `IcebergSinkTask.open` (`IcebergSinkTask.java:58-60`) →
   `CommitterImpl.open` (`CommitterImpl.java:141-151`) → `hasLeaderPartition` → `startCoordinator`
   (`:223-231`) → `new Coordinator(...)`. The `Channel` constructor creates a consumer in group
   `<connectGroupId>-coord` (`Coordinator.java:97-102`, `KafkaClientFactory.createConsumer`:
   `auto.offset.reset=latest`, random `client.id`, `read_committed`) and a producer with the
   stable `coordinatorTransactionalId()` (`initTransactions()` fences the previous coordinator's
   producer). Then `CoordinatorThread.start()` → `Channel.start()` (`:228-233`) subscribes →
   `process()` loop.
2. Coordinator termination (normal operations): Connect runtime → `IcebergSinkTask.stop()` →
   `close()` → `CommitterImpl.close(List.of())` (`IcebergSinkTask.java:67-71`,
   `CommitterImpl.java:172-176`) → `stopCoordinator()` (`:240-245`) →
   `CoordinatorThread.terminate()` → `coordinatorThread = null`. A task stop happens on connector
   restart, task restart, reconfiguration, pause or delete, and when a rebalance revokes the
   leader partition (`CommitterImpl.java:179-182`). In each case the worker JVM keeps running.
3. After step 2 the old `iceberg-coord` thread is untracked: `coordinatorThread` is null and
   nothing joins it. It keeps running `consumeAvailable` until a 1 s poll returns empty.

### Reachability / trigger scenario

1. Connector V (the victim) and one or more other connectors N share the control topic
   `control-iceberg`. The default name is shared, and the topic has 1 partition as documented.
2. The traffic on the control topic has gaps under 1 s. All connectors' StartCommit,
   DataWritten, DataComplete, CommitToTable and CommitComplete events count, whatever their group.
   Sources of such traffic are many connectors, a short `iceberg.control.commit.interval-ms`, or
   both.
3. V's coordinator task is stopped (for example a connector restart via the REST API, or a
   rebalance that moves the leader partition) → `terminate()`. The old thread is inside
   `consumeAvailable` (it spends nearly all its time there) and keeps polling. Every poll returns
   records, so it never returns to the `while (!terminated)` check.
4. A new V task elects a new coordinator, whose consumer joins `<V>-coord`. Because the zombie
   keeps calling `poll()`, it stays a live member and takes part in the rebalance. With the
   default assignor (`RangeAssignor`, eager; members sorted by member id, which starts with a
   random UUID `client.id`), the single control partition stays with the zombie in about half of
   the elections. With `CooperativeStickyAssignor` set via `iceberg.kafka.partition.assignment.strategy`,
   the zombie keeps it in every election.
5. While the zombie holds the partition, the new coordinator consumes nothing. Its commits time
   out and commit an empty `commitMap`, so no table snapshot is written. V's workers keep answering
   StartCommit with DataWritten in transactions that also commit the source offsets
   (`Worker.java:111` → `Channel.send` `:104-106` `sendOffsetsToTransaction`). The source offsets
   committed to Kafka therefore advance while the table does not. The zombie buffers those
   DataWritten events (`CommitState.addResponse`) but never commits them: its commit id does not
   match, and it never re-enters `process()`.
6. When a ≥1 s gap occurs, the zombie's poll returns empty, the thread runs `stop()`, and the
   partition moves to the new coordinator. That coordinator re-reads from the last committed
   `-coord` offset and should catch up (to be verified in Phase 2).

### Safeguards / masks recorded (not dismissed; Phase 2 to prove whether they fire)

- S1: Producer fencing (`coordinatorTransactionalId`, #18039). It fences only *transactional
  writes* by the zombie. It does not stop the zombie's consumer polling or its group membership.
- S2: `Coordinator.commitToTable` `terminated` check (`:303`). It applies only if the zombie
  starts a commit. It cannot, because no DataComplete matches its commit id and `process()` is
  never re-entered.
- S3: Assignor randomness. With `RangeAssignor`, the zombie loses the partition in about 50% of
  elections, and its poll then returns empty. This is a probabilistic partial mask, not a guard.
- S4: The traffic gap. Any ≥1 s gap on the zombie's partition ends the episode. The stall lasts
  exactly as long as the traffic lasts.
- S5: Late-response recovery (`CommitState.addResponse` buffers DataWritten without a commit in
  progress, and the `commitToTable` filter `offset >= table offset`). After the zombie exits, the
  new coordinator should commit the backlog, so data is delayed rather than lost unless
  control-topic retention removes the backlog first.

## Step 2 — Developer-knowledge search (verbatim evidence, not classified)

- Tests: `TestCoordinatorThread.testRun` (pristine) expresses the intent that
  `coordinatorThread.terminate()` leads promptly to `coordinator.stop()`:
  `coordinatorThread.terminate(); verify(coordinator, timeout(1000)).stop();` This uses a mocked
  `Coordinator` whose `process()` returns immediately, so the poll loop is never exercised.
- Comments: `Coordinator.terminate()`: "wait for coordinator termination, else cause the sink task
  to fail" (it waits only for the commit executor). `CommitterImpl.close`: "Normal close: if leader
  partition is lost, stop coordinator." There is no comment about the poll loop ignoring
  termination.
- Git: the local clone is shallow (1 commit). Upstream history of `Channel.java` (via `gh api`):
  d8e7642d4 (2024-07 initial, #10351), 03fc1ae33, 4d725363c (#17933 high-water mark), 1019497e3
  (#17552), ef99ad3f8 (#17713), dad714b8f (#18039, 2026-10-02 stable transactional id). The
  `while (!records.isEmpty())` loop dates from the initial commit. No commit since 2026-10-15
  touches `channel/`.
- Upstream tracker (gh search over issues and PRs, open/closed/merged, including PRs merged or
  closed since 2026-10-15). Related symptom reports, each with a *different* diagnosed mechanism:
  - #17349 (open, 2026-07-24): "Coord consumer group on iceberg control topic with phantom consumer
    after stopping a connector". The `-coord` group keeps a consumer holding partition 0 after the
    connector is stopped, on a control topic at offset ~2.39M. Diagnosis: `stopCoordinator()` does
    not `join()`. Proposed fix: PR #17350 (open), `join(30 s)`. Comment: "Even after deleting the
    connector, the -coord consumer group remains active, which prevents us from adjusting its
    offsets".
  - #16016 (open): zombie coordinator thread when S3 writes fail. The comments name two leak
    paths: (1) `terminate()` does not join (PR #16020, closed unmerged); (2) `close()` skips
    `stopCoordinator()` when `hasLeaderPartition` is false mid-rebalance (#17637 / PR #18687, open).
    Symptom quoted: "consumer offsets keep advancing ... but no new snapshots land".
  - #16843 (closed unmerged): "make sink cleanup robust". It joins the thread in `terminate()` with
    a timeout and closes resources independently.
  - #17910 (open): "Prevent hanging coordinator in consumer group". It closes the consumer first in
    `Channel.stop()` and adds close timeouts, for the case where the producer close hangs. Refs
    #15452 (closed as stale): "thread/consumer hanging ... data consumer group progresses, but
    nothing gets committed".
  - #17551 / PR #17552 (merged): a zombie re-committing old `-coord` offsets. Different mechanism.
  None of these identifies the `consumeAvailable` loop ignoring termination while control records
  keep arriving. The proposed fixes (join with timeout, close order) would not end that loop:
  `join(30 s)` returns, or throws, while the thread keeps polling.

## Step 3 — Known-status / precedent

- No issue, PR, CVE or Specula dataset entry reports THIS mechanism at THIS site
  (`Channel.consumeAvailable` / `CoordinatorThread.run` loop not observing `terminated` under
  continuous control traffic). #17349 reports the closest *symptom* (a phantom `-coord`
  consumer), but attributes it to the missing join in `CommitterImpl.stopCoordinator`, a
  different site and mechanism. Its fix would not stop this loop. Not a duplicate, so the finding
  is **not dropped**; proceed to Phase 2.
- Novelty (to record): NEW (mechanism unreported). Related symptom reports: #17349/#17350,
  #16016, #15452/#17910, #16843.
