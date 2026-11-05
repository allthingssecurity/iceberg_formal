# Instrumentation Spec: Apache Iceberg Kafka Connect sink (exactly-once commit protocol)

Maps every action of `base.tla` (as consumed by `Trace.tla`) to a code location in `apache/iceberg @ 9fffeb6`.
Paths: `CI = kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/`,
`KC = kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/`.
Category A: one totally ordered NDJSON trace (`../traces/<name>.ndjson`, override with `IOEnv.JSON`).

## Section 1: Trace Event Schema

### 1.1 Envelope

```json
{"tag": "trace", "seq": 42,
 "event": {"name": "<SpecAction>", "actor": "task" | "coord" | "env",
           "task": "t1",            // task events only
           "coord": 2,              // coordinator events only (instance id, see §3.3)
           "args":  { ... },        // action arguments / global effects (per event, §2)
           "state": { ... }}}       // actor post-state snapshot (§1.2 / §1.3)
```

* `event.name` is exactly the spec action name (1:1, §2). `seq` is the global order (single synchronized logger, §3.1).
* Every snapshot is taken **after** the action's effects (post-state), at the trigger point listed in §2.
* JSON maps keyed by partition/offset are encoded as arrays of `[key, value]` pairs (`Trace.tla` `PairMap`);
  sets as arrays. Offsets are control-topic **ordinals** (§3.2).

### 1.2 Task state fields (`actor = "task"`)

| JSON field | Implementation source | TLA+ variable |
|---|---|---|
| `up` | task running (false only in the harness-emitted `Crash` / failure events) | `tUp[t]` |
| `variant` | `"legacy"` if the worker writes `TableReference.uuid == null`, else `"v"` (`KC/data/IcebergWriterFactory.java:72-78`) | `tVariant[t]` |
| `assign` | `consumer.assignment()` of the Connect consumer (via `KafkaUtils.kafkaConsumer(context)`, `CI/KafkaUtils.java:87-97`) | `assign[t]` |
| `pos` | `[[p, consumer.position(tp)]]` for assigned `tp` (position is already valid: never blocks here) | `pos[t]` |
| `stored` | WorkerSinkTask `rebalanceException` class: `"none"`, `"wakeup"`, `"fatal"` (runtime hook, §3.4; `"none"` if not hooked) | `stored[t]` |
| `worker` | `CommitterImpl.worker != null` (`CI/CommitterImpl.java:43`) | `worker[t]` |
| `buf` | shadow set `[[originalKafkaPartition, originalKafkaOffset]]` of records saved since the last `completeWrite()`/`stop()` (`KC/data/SinkWriter.java:58-94`) | `wBuf[t]` |
| `coord` | instance id of `CommitterImpl.coordinatorThread` (0 = null) | `coordRef[t]` |
| `wpart` | control partition of the worker producer key `producerId` (`CI/Channel.java:75,94`; default partitioner) | `wcPart[t]` |
| `wpos` | `[[c, position]]` of the Worker control consumer (ordinals) | `wcPos[t]` |

### 1.3 Coordinator state fields (`actor = "coord"`)

| JSON field | Implementation source | TLA+ variable |
|---|---|---|
| `pc` | `"consume"` inside `consumeAvailable`, `"commit"` inside `commit()`, `"loop"` otherwise, `"exited"` after the thread loop ends, `"dead"` (harness, process killed) | `cPc[k]` |
| `terminated` | `Coordinator.terminated` (`CI/Coordinator.java:86`) | `cTerm[k]` |
| `err` | class of `CoordinatorThread.error`: `"none"`, `"fenced"` (`isFenced()`, `CI/CoordinatorThread.java:78-81`), `"other"` | `cErr[k]` (exit events only) |
| `pos` | `[[c, consumer.position(c)]]` for owned control partitions | `cPos[k]` |
| `hwm` | `Channel.controlTopicOffsets` (`CI/Channel.java:57,139`) | `cHwm[k]` |
| `ckpt` | `Channel.committedOffsets` (`CI/Channel.java:58,209-211`) | `cCkpt[k]` |
| `buf` | `CommitState.commitBuffer` as `[[partition, ordinal]]` (`CI/CommitState.java:40`) | `cBuf[k]` |
| `cid` | mapped `currentCommitId` (0 = null, §3.3) | `cCid[k]` |
| `count` | `receivedPartitionCount` (`CI/CommitState.java:42`) | `cCount[k]` |
| `total` | `totalPartitionCount` (`CI/Coordinator.java:81,106-107`) | `cTotal[k]` |
| `fails` | `consecutiveCommitFailures` (`CI/Coordinator.java:88`) | `cFails[k]` |
| `mode` | `"none"`, `"full"`, `"partial"` (argument of the running `commit(partialCommit)`) | `cMode[k]` |

### 1.4 Argument fields (event-specific)

| Field | Meaning | Validated against |
|---|---|---|
| `p` | source partition of the delivered record | `Put(t, p)` |
| `c` | control partition of the consumed record / of the StartCommit answered | action parameter |
| `offsets` | `[[p, offset]]` source offsets in the worker transaction (`results.sourceOffsets()`) | `srcCommitted'` |
| `part`, `end` | control partition written and its end ordinal after the transaction | `ctl'` length |
| `tbl`, `var` | table identifier, `"legacy"` iff `TableReference.uuid() == null` | group `<<tbl, var>>` |
| `st` | per-table task outcome `"read"`, `"done"`, `"failed"` | `cGrp'[k][g].st` |
| `exp`, `merged`, `files` | `committedOffsets`, `mergedOffsets`, kept envelopes `[[c, ordinal]]` (`CI/Coordinator.java:270-293`) | `cGrp'[k][g]` |
| `snapCount`, `floor` | number of table snapshots after the commit; stored `kafka.connect.offsets` map | `snaps'` |
| `landed`, `cls` | table commit landed (harness checks the catalog for the commit id); exception class `"commitFailed"` (Iceberg `CommitFailedException`) or `"other"` | `CoordCasFault` |
| `committed` | `[[c, offset]]` offsets in the `-coord` transaction (`offsetsToCommit`, `CI/Channel.java:172-184`) | `cgCommitted'` |
| `cgOwner` | final `-coord` assignment `[[c, instance]]` after a join/leave (§3.5) | `cgOwner'` |
| `cpart`, `total` | newly started coordinator: control partition of its producer key, `totalPartitionCount` | `cPart'`, `cTotal'` |
| `owner`, `lost` | Rebalance: final assignment `[[p, task]]` of the generation, kicked-out member list | `owner'`, `lostParts'` |
| `idx` | ExpireSnapshot: per-table snapshot index (§3.3) | `ExpireSnapshot(T, i)` |

## Section 2: Action-to-Code Mapping

Trigger points are "after X" unless stated. "→ FAILED" means the event also ends the task (its post-state has `up: false`).

### 2.1 Connect task / CommitterImpl / Worker (`actor = "task"`)

| # | Spec action = event | Code location | Trigger point | Args | Notes |
|---|---|---|---|---|---|
| 1 | `Put` | `KC/IcebergSinkTask.java:85-90` → `CI/CommitterImpl.java:189-196` (`startWorker` :214-221) → `KC/data/SinkWriter.java:75-94` | after `worker.save(sinkRecords)` (`CI/CommitterImpl.java:193`), one event per record | `p` | run the connector with `consumer.override.max.poll.records=1` so a batch is one record |
| 2 | `WorkerAnswer` | `CI/Worker.java:67-114`, `CI/Channel.java:82-118` | after `send(events, sourceOffsets)` returns (`CI/Worker.java:111`) | `c`, `offsets`, `part`, `end` | StartCommit records are the only ones a Worker handles (:70-72); offsets of the sent records come from the send futures after `commitTransaction` |
| 3 | `WorkerAnswerRejected` | `CI/Channel.java:109-116` | in the catch, before rethrow, when the failure is the KIP-447 offset commit (`CommitFailedException`, `IllegalGenerationException`, `FencedInstanceIdException`) | `c` | → FAILED |
| 4 | `WorkerOutOfRange` | Worker control consumer, `CI/Channel.java:123` | position reset after `OffsetOutOfRange` (consumer interceptor) | `c` | only with control-topic recreation |
| 5 | `PollLost` | runtime `WorkerSinkTask.HandleRebalance.onPartitionsLost` → `KC/IcebergSinkTask.java:62-65` | at entry of `CommitterImpl.close` (`CI/CommitterImpl.java:161`) when invoked for lost partitions | — | runtime hook §3.4 |
| 6 | `CloseDiscard` | `CI/CommitterImpl.java:162-163,233-238` | after `stopWorker()` returns | — | buffer of every partition dropped |
| 7 | `CloseDiscardFail` | same | `stopWorker()` threw (catch-rethrow wrapper) | — | `stored: "fatal"` |
| 8 | `CloseElect` | `CI/CommitterImpl.java:178-182` | after the `if (hasLeaderPartition(..)) stopCoordinator()` block | — | `coord` shows whether the coordinator was stopped (`terminate()` sets `terminated`) |
| 9 | `CloseElectFail` | `CI/CommitterImpl.java:179` → `CI/KafkaUtils.java:44-54` | `hasLeaderPartition` threw `ConnectException` | — | `stored: "fatal"` |
| 10 | `CloseSeek` | `CI/CommitterImpl.java:186` → `CI/KafkaUtils.java:60-85` | after `seekToLastCommittedOffsets` returns | — | post-state `pos` shows which partitions were rewound |
| 11 | `CloseSeekWakeup` | `CI/KafkaUtils.java:66-67` | `consumer.committed()` threw `WakeupException` | — | `stored: "wakeup"`; F2 |
| 12 | `CloseSeekFail` | `CI/KafkaUtils.java:61-67` | any other exception from the seek step | — | `stored: "fatal"` |
| 13 | `Open` | `KC/IcebergSinkTask.java:57-60` → `CI/CommitterImpl.java:140-151` (`startCoordinator` :223-231) | after `open(...)` returns | `cpart`, `total` (if a coordinator was started: producer-key partition, `totalPartitionCount`) | new coordinator id assigned at `new Coordinator(...)` (§3.3) |
| 14 | `OpenFail` | `CI/CommitterImpl.java:147` | `hasLeaderPartition` threw inside `open` | — | `stored: "fatal"` |
| 15 | `PollRethrowWakeup` | runtime `WorkerSinkTask.pollConsumer` / `iteration` | stored `WakeupException` rethrown after `poll()` returned; batch dropped | — | runtime hook §3.4; `pos` shows the dropped records |
| 16 | `PollRethrowFatal` | runtime `WorkerSinkTask.pollConsumer` | stored non-wakeup exception rethrown | — | → FAILED |
| 17 | `PollWakeup` | runtime `WorkerSinkTask.iteration` catch `WakeupException` | wakeup raised by a plain poll | — | runtime hook §3.4 |
| 18 | `PauseResume` | runtime `WorkerSinkTask.transitionTo` → `consumer.wakeup()` | when the harness pauses/resumes the connector | — | emitted by the harness |
| 19 | `ClearFencedCoordinator` | `CI/CommitterImpl.java:199-202` | after `coordinatorThread = null` | — | |
| 20 | `CoordinatorNotRunning` | `CI/CommitterImpl.java:203-207` | before `throw new NotRunningException` | — | → FAILED |
| 21 | `Crash` | harness | when the harness kills the task's process (no further events from it) | `cgOwner` | hosted coordinator instances become `"dead"` |
| 22 | `Restart` | harness / Connect REST restart → `KC/IcebergSinkTask.java:48-55` | after `start(props)` | — | `variant` gives the worker version after a rolling upgrade |

### 2.2 Coordinator (`actor = "coord"`)

| # | Spec action = event | Code location | Trigger point | Args | Notes |
|---|---|---|---|---|---|
| 23 | `CoordStart` | `CI/CoordinatorThread.java:41-47` → `CI/Channel.java:228-233` | first `onPartitionsAssigned` of the `-coord` consumer (test-only listener on `subscribe`, :229), before records are returned | `cgOwner` | the initial `consumeAvailable` records follow as `CoordConsume` |
| 24 | `CoordStartCommit` | `CI/Coordinator.java:127-134` | after `send(StartCommit)` returns (:132) | `part`, `end` | `cid` = mapped new commit id |
| 25 | `CoordStartCommitFenced` | `CI/Coordinator.java:132` | `send` threw `ProducerFencedException`/`InvalidProducerEpochException` | `cgOwner` | `pc: "exited"`, `err: "fenced"` |
| 26 | `CoordExit` | `CI/CoordinatorThread.java:49,59` | loop left because `terminated`, before `coordinator.stop()` | `cgOwner` | `err: "none"` |
| 27 | `CoordConsume` | `CI/Channel.java:125-149` + `CI/Coordinator.java:143-157` | per control record of a Coordinator channel: after the HWM skip (`continue`, :129-135) or after `addResponse`/`addReady` (`CI/CommitState.java:51-71`); if `isCommitReady` is true, **before** `commit(false)` runs (`pc: "commit"`, `mode: "full"`) | `c` | emit nothing for Worker channels |
| 28 | `CoordOutOfRange` | `-coord` consumer, `CI/Channel.java:123` | position reset after `OffsetOutOfRange` | `c` | only with control-topic recreation |
| 29 | `CoordTimeout` | `CI/Coordinator.java:138-139` | `isCommitTimedOut()` returned true, before `commit(true)` runs | — | `pc: "commit"`, `mode: "partial"` |
| 30 | `CoordReadFloor` | `CI/Coordinator.java:242-310` | per table task: after the filter (:277-301) — `st: "read"` before the commit (:312); `st: "done"` at "nothing to commit" (:308-310); `st: "failed"` when the terminated check throws (:303-306) | `tbl`, `var`, `st`, `exp`, `merged`, `files` | runs on `iceberg-committer` threads (§3.1) |
| 31 | `CoordCasCommit` | `CI/Coordinator.java:312-349` | after `appendOp.commit()` and `send(CommitToTable)` (`st: "done"`), or `send` threw fenced (`st: "failed"`) | `tbl`, `var`, `st`, `snapCount`, `floor`, `files` | |
| 32 | `CoordCasValidationFail` | `CI/Coordinator.java:325` | `appendOp.commit()` threw `ValidationException` | `tbl`, `var`, `st` | |
| 33 | `CoordCasFault` | `CI/Coordinator.java:325` | `commit()` threw `CommitFailedException` / `CommitStateUnknownException` / other | `tbl`, `var`, `st`, `landed`, `cls` (+ `snapCount`, `floor`, `files` if landed) | |
| 34 | `CoordCommitSwallow` | `CI/Coordinator.java:165-174,198-200` | partial commit threw; after the `finally` | — | `pc: "loop"` |
| 35 | `CoordCommitRetry` | `CI/Coordinator.java:182-197` | Iceberg `CommitFailedException` below budget; after the `finally` | — | `pc: "consume"` |
| 36 | `CoordCommitThrow` | `CI/Coordinator.java:176-190` → `CI/CoordinatorThread.java:52-56` | full commit rethrows; in the thread catch | `cgOwner` | `pc: "exited"` |
| 37 | `CoordCheckpoint` | `CI/Coordinator.java:216-217` → `CI/Channel.java:171-213` | after `commit()` returned normally | `committed` | `pc: "consume"` (full) / `"loop"` (partial) |
| 38 | `CoordCheckpointFencedPartial` | `CI/Channel.java:199-207` (or the CommitComplete send, `CI/Coordinator.java:223`) | fenced exception in a partial commit; after the `finally` | — | buffer cleared iff nothing was to commit |
| 39 | `CoordCheckpointFencedFull` | same | fenced exception in a full commit; thread catch | `cgOwner` | `err: "fenced"` |
| 40 | `CoordCheckpointFailPartial` | `CI/Channel.java:196-207` | non-fenced offset-transaction failure, partial | — | |
| 41 | `CoordCheckpointFailFull` | same | non-fenced failure, full; thread catch | `cgOwner` | `err: "other"` |

### 2.3 Environment (`actor = "env"`, emitted by the harness / post-processor)

| # | Spec action = event | Source | Trigger point | Args |
|---|---|---|---|---|
| 42 | `Rebalance` (= `RebalanceWith(lost, FALSE)`) | post-processor | one per Connect-group generation (each cooperative round is its own generation), inserted before the first callback event (`CloseDiscard*` / `Open*` / `PollLost`) of the generation, or earlier, before a `WorkerAnswerRejected` of a member still on an older generation (§3.5) | `owner`, `lost` |
| 43 | `CgRebalance` | harness | `-coord` reassignment not caused by a coordinator join/leave | `cgOwner` |
| 44 | `OtherWriterCommit` | harness (separate writer appends to main) | after its commit | `tbl`, `snapCount` |
| 45 | `ExpireSnapshot` | harness `expireSnapshots().expireSnapshotId(id)` | after commit | `tbl`, `idx` |
| 46 | `FastForwardDropBranch` | harness `manageSnapshots().fastForwardBranch("main", b).removeBranch(b)` | after commit | `tbl` |
| 47 | `LoseCoordOffsets` | harness `Admin.deleteConsumerGroupOffsets("connect-<name>-coord", …)` | after it returns | `c` |
| 48 | `RecreateControlTopic` | harness delete + create topic | after creation | — |

### 2.4 Silent actions (no event)

| Spec action | Why silent | Constraint in `Trace.tla` |
|---|---|---|
| `CoordPollEmpty`, `CoordSkipStartCommit` | coordinator loop iterations are too frequent to log | `SilentCoordLoop`: only when the next event of the same coordinator needs the other pc |
| `Open` with no added partitions | Connect does not call `task.open` for an empty set (`WorkerSinkTask.java:745-747`) | `SilentRejoin`: deterministic, one step per task |
| `Rejoin` (member's generation catches up) | an unaffected member processes the join response in a later `poll()` without any callback (`ConsumerCoordinator.java:351`) | `SilentGenSync`: only right before an accepted `WorkerAnswer` of that task whose `taskGen` lags (the KIP-447 check is its only reader) |

## Section 3: Special Considerations

### 3.1 Harness topology and ordering
* One JVM: embedded Kafka (KRaft), Connect `EmbeddedConnectCluster` with 2 workers (one sink task each: `t1`, `t2`),
  in-memory/JDBC Iceberg catalog, control topic with 1 partition (`Trace.cfg`: `CtlParts = {0}`), source topic with
  2 partitions. Several threads emit events (Connect task threads, `iceberg-coord`, `iceberg-committer-%d`): a
  single synchronized logger assigns `seq` at emission, so the file order is a linearisation of the real execution.
* Table tasks of one cycle run on `iceberg-committer` threads; their `CoordReadFloor` / `CoordCas*` events carry the
  coordinator id of the instance that submitted them.

### 3.2 Offset normalisation (post-processor)
The spec has no transaction markers. After a run, read the control topic with `read_committed` and, per partition,
the sorted offsets of the committed data records; replace every control offset in the trace (envelope offsets,
`pos`, `hwm`, `ckpt`, `committed`, stored floors, `end`, `wpos`) by `ordinal(o) = #{data records with offset < o}`.
Source-partition offsets are kept as is (Connect consumers see contiguous offsets for a non-transactional source topic).

### 3.3 Identifier mapping
Tasks → `"t1"`, `"t2"` (Connect task id); coordinator instances → 1, 2, … in `new Coordinator(...)` order
(`CI/CommitterImpl.java:226-227`); commit ids (UUID) → 1, 2, … in `StartCommit` order; tables → `"T1"`; snapshots →
per-table 1-based commit order (`snapCount` = number of snapshots after the event).

### 3.4 Runtime-internal events
`PollLost`, `PollRethrowWakeup`, `PollRethrowFatal`, `PollWakeup` and the `stored` field live in Connect's
`WorkerSinkTask` (not Iceberg code). Use a test-only patched runtime (hooks at `WorkerSinkTask` `onPartitionsLost`,
`pollConsumer` rethrow, `iteration` wakeup catch, `onPartitionsRemoved` store) or keep first validation campaigns free
of pause/resume and session timeouts (then these events never occur and `stored` is always `"none"`).

### 3.5 Group reassignments
* `Rebalance`: one spec step per Connect-group generation. Cooperative round 1 only revokes a moving partition (it stays
  unowned, `owner = NoTask`), round 2 assigns it; the post-processor emits one `Rebalance` per generation with that
  generation's assignment and the kicked-out member. The broker bumps the generation when the join completes, which can
  precede the first callback: a `WorkerAnswerRejected` (ILLEGAL_GENERATION) of a member whose `meta.gen` is older than
  the next generation pulls that generation's `Rebalance` in front of it.
* `-coord`: joins/leaves reassign the group (eager range). The event that causes it (`CoordStart`, thread exits,
  `Crash`) must carry the final `cgOwner` (the harness reads every live coordinator's `consumer.assignment()` after the
  reassignment settles). Without it the trace spec branches on the assignment.

### 3.6 Shadow fields
* `buf` (task): SinkWriter keeps writers, not records — keep a shadow set of `(originalPartition, originalOffset)`
  updated in `SinkWriter.save` (`KC/data/SinkWriter.java:85-87`) and cleared in `completeWrite`/`close`.
* `landed` (`CoordCasFault`): look up the table history for a snapshot whose `kafka.connect.commit-id` equals the
  current commit id and whose files are the event's `files`.
* `variant`: derived from the worker's `TableReference.uuid()`; mixed versions need two worker builds (rolling-upgrade
  harness).

### 3.7 Bootstrap
Fresh cluster: empty control topic, no consumer-group offsets, empty tables, tasks started but unassigned — identical
to `base.tla` `Init`; `Trace.tla` `TraceInit == Init /\ l = 1`.
