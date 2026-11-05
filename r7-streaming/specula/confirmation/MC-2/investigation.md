# MC-2 — Phase 1 investigation (evidence only, no verdict)

Finding: `WakeupException` from `consumer.committed()` in `CommitterImpl.close()` — no rewind, and the
next polled batch is dropped (LOSS). Source: model checking, `MC_hunt_s3_close_discard.cfg`
(cooperative) and `MC_hunt_s3_eager.cfg` (eager); invariant `SourceCommittedCovered`.

Code under test: apache/iceberg @ `9fffeb6` (pristine files read with `git show HEAD:<path>` in the
worktree; the worktree/source checkouts carry the Specula trace instrumentation, which is NOT used for
the reproduction). Kafka runtime the sink is built/tested against: 3.9.2 (`gradle/libs.versions.toml:75`).

Persistent-findings lookup (`specula findings ... lookup --query seekToLastCommittedOffsets --query
WakeupException --query SourceCommittedCovered --query CommitterImpl.close`): `[]` (no prior record).

## Step 1 — Code audit

### Cited sites (pristine)

`kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitterImpl.java`
```java
160  @Override
161  public void close(Collection<TopicPartition> closedPartitions) {
162    // Always try to stop the worker to avoid duplicates.
163    stopWorker();                       // discards the Worker + SinkWriter buffer of EVERY partition
...
178    // Normal close: if leader partition is lost, stop coordinator.
179    if (hasLeaderPartition(closedPartitions)) {   // Admin describeConsumerGroups round trip
...
184    // Reset offsets to last committed to avoid data loss.
185    LOG.info("Seeking to last committed offsets for worker {}.", taskId);
186    KafkaUtils.seekToLastCommittedOffsets(context);
187  }
```
`stopWorker()` (`:233-238`) → `Worker.stop()` (`Worker.java:117-120`) → `Channel.stop()` +
`SinkWriter.close()`; the buffered records and their `sourceOffsets` are dropped with the Worker.

`kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/KafkaUtils.java`
```java
60  static void seekToLastCommittedOffsets(SinkTaskContext context) {
61    Consumer<byte[], byte[]> consumer = kafkaConsumer(context);   // the Connect task's own consumer
...
66    Map<TopicPartition, OffsetAndMetadata> committedOffsets =
67        consumer.committed(consumer.assignment());               // blocking; NOT inside any try
...
75            try {
76              consumer.seek(topicPartition, offsetAndMetadata.offset());
77            } catch (IllegalStateException e) {                 // only seek() is guarded
```

### Call chain (all real Connect runtime code, kafka 3.9.2 bytecode checked with `javap -c -p`)

1. `WorkerSinkTask.iteration()` → `poll(timeout)` → `pollConsumer()` → `consumer.poll()`; the classic
   consumer runs the rebalance inside `poll()`.
2. Revocation → `WorkerSinkTask$HandleRebalance.onPartitionsRevoked/Lost` → `onPartitionsRemoved` →
   `closePartitions(partitions, lost)` → (revoke) `commitOffsets(now, closing=true, partitions)` whose
   `finally` calls `task.close(partitions)` → `IcebergSinkTask.close` (`IcebergSinkTask.java:63-65`) →
   `CommitterImpl.close(closedPartitions)`.
3. `onPartitionsRemoved` wraps step 2 in `catch (RuntimeException e) { rebalanceException = e; }`
   (bytecode: exception table `70 90 93 Class java/lang/RuntimeException`, then `access$1202` = store).
   `WakeupException extends KafkaException extends RuntimeException`, so it is stored, not propagated to
   the consumer (the consumer's own `invokePartitionsRevoked` would rethrow a WakeupException out of
   `poll()`, but it never sees it).
4. `pollConsumer()`: `msgs = consumer.poll(...)`; `if (rebalanceException != null) { e = it; clear; throw e; }`
   — the batch `msgs` the same `poll()` returned is discarded (bytecode `pollConsumer` offsets 0-34).
   `poll()` therefore never reaches `convertMessages(msgs)` / `deliverMessages()`.
5. `iteration()` catches `WakeupException` (exception table `13 146 149 WakeupException`): if stopping
   → return; else if `shouldPause()` → `pauseAll(); onPause(); requestCommit()`; else if
   `!pausedForRedelivery` → `resumeAll(); onResume()`. No rewind, no failure: the task keeps running.
6. Who calls `consumer.wakeup()`: `WorkerSinkTask.transitionTo(TargetState)` = `super.transitionTo(state);
   consumer.wakeup();` (bytecode offsets 0-14) — invoked by the herder for every target-state change of
   the connector (REST `PUT /connectors/{name}/pause` and `/resume`). `WorkerTask.transitionTo` only
   skips when the task is stopping, so a `resume` of an already-running connector also wakes the
   consumer.
7. Wakeups are honoured inside rebalance callbacks: kafka-clients 3.9.2 `ConsumerCoordinator` calls
   `ConsumerNetworkClient.disableWakeups()` only in `close(Timer)`; `committed()` →
   `fetchCommittedOffsets` → `client.poll(future, timer)` → `maybeTriggerWakeup()` ("Raising
   WakeupException in response to user wakeup") throws when the flag is set.
8. Connect `onPartitionsAssigned` (runs after the revoke in the same rebalance): if `shouldPause()` →
   `pauseAll()` before anything else, so after a PAUSE transition the post-rebalance poll fetches
   nothing; after a RESUME (target STARTED) it fetches normally.
9. Source offsets the table commit relies on: `IcebergSinkTask.preCommit` returns `{}` (Connect never
   commits); the Worker commits the connect group's offsets inside its control-topic transaction
   (`Worker.receive` → `Channel.send(events, results.sourceOffsets())` →
   `producer.sendOffsetsToTransaction`, `Channel.java:104-106`) with `sourceOffsets` = last saved
   record offset + 1 per partition (`SinkWriter.save`, `SinkWriter.java:85-87`).

### Reachability

Reachable from public operations only: a sink consumer-group rebalance (task restart, worker
join/leave, scaling, an eager rebalance of any member) concurrent with a connector `pause`/`resume`
REST call. The wakeup must land after the consumer's last wakeup check before the callback and before
`committed()` — i.e. while the task thread executes `stopWorker()` (closes the Worker's producer,
consumer and writers) and `hasLeaderPartition()` (creates an Admin client and describes the group).
Wakeups that land earlier abort the join phase harmlessly (`poll()` throws before any callback).

### Trigger scenarios

- (V1, the MC traces) Cooperative: t1 owns {p0,p1}; t2 joins; t1 revokes p1 → `close([p1])`; a
  `resume` arrives during the callback → `committed()` throws WakeupException → stored → the same
  `poll()` returns p0 records → rethrown → batch dropped, positions advanced → t1 puts later p0 records
  → answers StartCommit → group offset for p0 committed past the dropped records, which never reach
  the table. Eager (one task, revoke-all/re-add): the same, after the eager reassignment re-reads the
  committed offset.
- (V2, the "discard before a fallible rewind" half) Cooperative with records buffered on the retained
  partition: `stopWorker()` already discarded them; the rewind is skipped, so the retained partition's
  position stays past them; they are skipped once later offsets are committed. With a PAUSE
  transition this happens after the operator resumes.

### Safeguards encountered (recorded, not dismissed)

- `KafkaUtils.java:77` catches only `IllegalStateException` around `seek()`; nothing around `committed()`.
- Connect stores-and-rethrows (step 3-4) instead of failing the task for WakeupException (step 5).
- Eager protocol: the consumer re-initialises positions of re-assigned partitions from the committed
  offsets, which re-delivers *discarded* records (V2 does not apply to eager), but not the batch the
  rethrow drops (V1 applies).
- PAUSE transition: `onPartitionsAssigned` pauses all partitions, so V1 needs a RESUME/no-op transition;
  V2 still applies (cooperative).
- Loss becomes permanent only once the Worker commits a later offset of the same partition
  (continuous traffic) — a restart/eager rebalance before that would re-read from the committed offset.
- Known, separate: null committed offset is not rewound (PR #18767); excluded by `FixNullSeek = TRUE`
  in the model; the reproduction must make every partition have a committed offset first.

## Step 2 — Developer knowledge

- Comments at the site: `// Always try to stop the worker to avoid duplicates.` and
  `// Reset offsets to last committed to avoid data loss.` (`CommitterImpl.java:162,184`) — the
  developers rely on the rewind for no-loss; no comment about wakeups.
- `KafkaUtils.java:78-81` logs "Rebalance may have occurred, partition {} lost before seeking" — only
  the seek's IllegalStateException was anticipated.
- Git history: the checkout is a single grafted commit (`9fffeb6`), no local blame history.
- Kafka documentation (ConsumerRebalanceListener): callbacks may see WakeupException from nested
  consumer calls and it is "propagated to the current invocation of KafkaConsumer.poll" — but Kafka
  Connect's listener catches and stores it (step 3), which the sink does not account for.
- Tests: no test in `kafka-connect/kafka-connect/src/test` exercises a WakeupException in `close()`
  (searched for `WakeupException`/`wakeup`).
- Kafka side, related but different: KAFKA-10240 (WakeupException on sink-task *shutdown* commit,
  fixed 2.7.0) — https://issues.apache.org/jira/browse/KAFKA-10240.

## Step 3 — Known-status / precedent

Searched apache/iceberg issues and PRs (open, closed, merged; incl. PRs updated since 2026-07-01) via
`gh search issues/prs` for: `seekToLastCommittedOffsets`, `WakeupException`, `wakeup`, `KafkaUtils`,
`CommitterImpl close`, `stopWorker`, `pause`, `onPartitionsRevoked`, `rebalance callback exception`,
`kafka connect data loss/missing records`. Related but different mechanisms:

- #18767 (open): rewind partitions *without a committed offset* on close — the known null-offset
  mechanism, explicitly excluded here.
- #16843 (closed, unmerged): continue cleanup and the offset seek after `hasLeaderPartition` fails
  during *shutdown* (GroupAuthorizationException) — a different exception site, task fails.
- #16366 (closed): tolerate CommitFailedException in `Channel.send` during rebalance.
- #12372 (merged 1.9.0): close-without-open in cooperative rebalance.
- #18687 (open): stop the coordinator when its leader partition is revoked.

No issue/PR reports a WakeupException from `consumer.committed()` in `CommitterImpl.close()` /
`KafkaUtils.seekToLastCommittedOffsets`, or the dropped post-rebalance batch caused by a
pause/resume during the revoke callback. Recorded novelty: NEW (looked; nothing for this mechanism).

Pre-filter: MC-sourced with an actual counterexample → proceed to Phase 2.

## Counterexample (spec/output/MC_hunt_s3_close_discard_bfs.out, state diffs)

1-3 Join gen 1, t1 Open {p0,p1}, coordinator 1 elected on t1, pos = 0/0.
6 Rebalance gen 2: t1 closing {p1}. 7 CloseDiscard (empty buffer). 8 CloseElect (keeps coordinator).
9 PauseResume: `wakeup[t1] = TRUE`. 10 CloseSeekWakeup: `stored[t1] = "wakeup"`, assign {p0}.
11 Open (empty add). 12 PollRethrowWakeup: `stored -> none`, `pos[p0]: 0 -> 1` (record p0@0 dropped).
13 Put p0@1. 14 StartCommit cid 1. 15 WorkerAnswer: `srcCommitted[p0] = 2`, DW rows {p0@1} → record
p0@0 is below the committed offset and in no DataWritten / snapshot → `SourceCommittedCovered` fails.
Eager CE (`MC_hunt_s3_eager_bfs.out`): identical with one task revoking and re-adding {p0}.
