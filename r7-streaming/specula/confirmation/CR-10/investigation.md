# CR-10 — Phase 1 investigation (evidence only)

Finding: "Transactional ids are not stable across task reconfiguration or versions: worker ids get a
new random suffix on every taskConfigs(), and the coordinator id changed in #18039" (code-review,
Scenario 4 residual / brief §6.3 C-8, Medium).

Source tree: apache/iceberg `9fffeb63b1` (main, 2026-11-02). All code below is read from
`git show HEAD:<path>` — the pristine tree. The CR-10 worktree carries the Specula TLA+
instrumentation (10 main files modified, `Coordinator.java` +483/-165), so it was **not** used as the
system under test.

Persistent-findings lookup (`specula findings lookup --query transactional.id --query fencing ...`)
returned `[]`: no earlier record.

## Step 1 — Code audit

### Cited sites (pristine)

* `IcebergSinkConnector.java:51-61` — `taskConfigs(maxTasks)`:
  `String txnSuffix = "-txn-" + UUID.randomUUID() + "-";` then each task `i` gets
  `iceberg.coordinator.transactional.suffix = txnSuffix + i`. A new UUID is drawn on **every**
  `taskConfigs()` call.
* `IcebergSinkConfig.java:320-323` — `transactionalSuffix()` returns that internal property
  (default `""`).
* `Worker.java:51-56` — worker producer `transactional.id =
  config.transactionalPrefix() + "worker" + config.transactionalSuffix()`.
* `IcebergSinkConfig.java:454-465` — `coordinatorTransactionalId() = transactionalPrefix() +
  connectGroupId() + "-coordinator"`. Javadoc: "stable across coordinator instances and task restarts
  within a connector ... lets an incoming coordinator's initTransactions() bump the producer epoch and
  fence a stale coordinator. Unlike the worker producer id, this intentionally omits
  transactionalSuffix(): a per-task/per-instance suffix would make each coordinator's id unique and
  prevent fencing."
* `KafkaClientFactory.java:43-51` — `createProducer(id)` sets `transactional.id` and calls
  `initTransactions()` at construction time.

### What happened to the coordinator id in #18039 (merged 2026-10-02, after the 1.12.0 release on 2026-09-30)

PR diff (`gh api repos/apache/iceberg/pulls/18039/files`):

* Before: `Channel(String name, ...)` built `transactionalPrefix() + name + transactionalSuffix()`,
  and `Coordinator` passed `name = "coordinator"`. So the coordinator id was
  `<prefix>coordinator-txn-<uuid>-<taskIndex>`, which changes with the task index and with every
  `taskConfigs()` call.
* Before: `Channel.commitConsumerOffsets()` used `consumer.commitSync(offsetsToCommit)`, which is
  **not transactional**.
* After: the coordinator id is `<prefix><connectGroupId>-coordinator`, and the `-coord` offset commit
  runs in a producer transaction (`sendOffsetsToTransaction(..., consumer.groupMetadata())`).
* The released 1.12.0 tag still has the old code (`gh api .../contents/...?ref=apache-iceberg-1.12.0`):
  Channel.java:69 `transactionalPrefix() + name + transactionalSuffix()`, Channel.java:188
  `consumer.commitSync(offsetsToCommit)`, plus the in-memory monotonic guard (#17552), the high-water
  mark (#17933) and `SnapshotAncestryValidator`. The "old version" in a rolling upgrade to the next
  release is therefore 1.12.0. It has no producer-epoch fencing of its `-coord` offset commits under
  **any** id.

### Call chains (reachable in normal operation)

* Worker id: Connect herder → `IcebergSinkConnector.taskConfigs()` (connector start, config update,
  connector restart, or the connector moving to another Connect worker) → task config →
  `IcebergSinkTask.start` → `IcebergSinkConfig` → first non-empty `put()` →
  `CommitterImpl.save` → `startWorker()` → `new Worker(...)` → `Channel(...)` →
  `KafkaClientFactory.createProducer(prefix+"worker"+suffix)` → `initTransactions()`.
  A plain task restart reuses the stored task config, so it keeps the same suffix. Only a
  `taskConfigs()` regeneration changes the id.
* Worker write path: `Worker.receive(StartCommit)` → `Channel.send(events, sourceOffsets)`:
  `beginTransaction`, send DataWritten/DataComplete to the control topic, then
  `if (!sourceOffsets.isEmpty()) producer.sendOffsetsToTransaction(offsets,
  KafkaUtils.consumerGroupMetadata(context))`, then `commitTransaction`, and abort plus rethrow on any
  exception (Channel.java:83-118). `consumerGroupMetadata(context)` reads the real Connect sink
  consumer from `WorkerSinkTaskContext` by reflection (KafkaUtils.java:56-58, 87-97).
  `SinkWriter.save` records a source offset for every saved record (`SinkWriter.java:85-87`). So any
  DataWritten that carries files also carries source offsets, and the broker validates that
  transaction against the consumer-group generation (KIP-447).
* Coordinator: `CommitterImpl.open` → `hasLeaderPartition` → `startCoordinator` →
  `new Coordinator(...)` → `Channel(connectGroupId+"-coord", coordinatorTransactionalId(), ...)`.
  `CoordinatorThread.isFenced()` treats `ProducerFencedException` / `InvalidProducerEpochException` as
  "superseded by a newer coordinator" (CoordinatorThread.java:74-81, CommitterImpl.java:198-208).

### Trigger scenarios

1. **Worker, across a task reconfiguration.** Task 1's thread is stalled (GC pause, slow catalog or
   S3, a blocking SMT, and so on) while its connector is reconfigured. The herder's graceful stop
   times out. Connect 3.9.2 `WorkerSinkTask` does not override `WorkerTask.cancel()` (javap: it only
   sets `cancelled` and calls `retryWithToleranceOperator.triggerStop()`), so the old task thread
   keeps running with its consumer and producers open: a zombie. New tasks start with a new suffix,
   so new task 1's `initTransactions()` is on `worker-txn-<NEW>-1` and does not touch the zombie's
   `worker-txn-<OLD>-1`. When the zombie wakes, its `put()` drives `Worker.process()`, which can
   answer a StartCommit with DataWritten + `sendOffsetsToTransaction(stale group metadata)`.
2. **Coordinator, mixed versions.** During a rolling upgrade from 1.12.0, an orphaned or zombie
   1.12.0 coordinator (for example #16016, or the #17637/#18687 close() path) coexists with a
   new-version coordinator. The ids are `coordinator-txn-<uuid>-<i>` and
   `<connectGroupId>-coordinator`, so the new `initTransactions()` cannot fence the old one. The old
   one's `-coord` offset commit is a plain `commitSync`, which no producer epoch could fence anyway.

### Safeguards recorded (not dismissed; Phase 2 must show whether they fire)

* KIP-447 group-generation / member validation of `TxnOffsetCommit`: the zombie worker's transaction
  carries the stale `ConsumerGroupMetadata`.
* read_committed consumers on the control topic (KafkaClientFactory.java:58), so an aborted
  transaction's DataWritten is never seen by the coordinator.
* Coordinator side: the table-stored control-topic offset floor filter (Coordinator.java:277-285),
  `SnapshotAncestryValidator` offset CAS (Coordinator.java:313-314, 327-328, 361-381),
  `distinctByKey(location)`, the high-water mark (Channel.java:126-139) and the in-memory monotonic
  offset guard (Channel.java:171-213).
* Kafka consumer offset fetch uses `require_stable`, so a new partition owner waits for a pending
  transactional offset commit before it reads its start offset.

## Step 2 — Developer knowledge

* **Docs**: `docs/docs/kafka-connect.md:61-62`: "The sink relies on
  [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447...) for exactly-once semantics.
  This requires Kafka 2.5 or later." Zombie-worker fencing is designed around consumer-group
  generation, not transactional-id stability.
* **Javadoc** at `IcebergSinkConfig.java:460-461`: "Unlike the worker producer id, this intentionally
  omits transactionalSuffix(): a per-task/per-instance suffix would make each coordinator's id unique
  and prevent fencing." This states explicitly that the worker id is per task and per instance and does
  not fence across instances.
* **PR #18039 review**, reviewer laskoviymishka at IcebergSinkConfig.java:456
  (https://github.com/apache/iceberg/pull/18039#discussion_r3981219594), verbatim: "The other one is
  the upgrade window: the old coordinator holds the epoch on the old `...coordinator` id while the new
  one initializes on `...<connectGroupId>-coordinator`, so for the duration of a rolling restart
  they're in different transactional-id namespaces and can't fence each other. Snapshot validation
  still protects the data, but the fencing guarantee has a gap exactly during the transition. An
  upgrade note covering both would do it — wdyt?" Author twthorn: "For upgrading, agree on that. As
  long as the upgrade is completed in a timely manner it should be a no-op whether via down-then-up or
  rolling deploy. For the upgrade notes I added a section to this PR description."
* **PR #18039 description, "Release Notes"** (verbatim excerpt): "During the rollout of the upgraded
  version this guard is not yet in effect: a coordinator still on the previous version uses a
  different transactional.id, sits in a separate fencing namespace, and can keep committing until it
  is restarted onto the new version. Confirm the rollout is complete and the offset-commit guard fully
  active by verifying each connector is back to a single coordinator ..."
* **PR #18039 review at Worker.java:53**
  (https://github.com/apache/iceberg/pull/18039#discussion_r3981219612). The reviewer asked about a
  null suffix. Author: "suffix will always be set" by `IcebergSinkConnector`. The per-taskConfigs
  random worker suffix was kept on purpose.
* **#17376 / #17450** (closed, unmerged coordinator-hardening PRs): "Worker ids are unchanged
  (`<prefix>worker<suffix>`), so sibling workers never fence each other."
* **#16366** (closed, unmerged) records the KIP-447 check firing in production on the worker
  `sendOffsetsToTransaction` path: "Transaction offset Commit failed due to consumer group metadata
  mismatch: Specified group generation id is not valid".
* **#17551** (closed by #17552): data loss caused by a zombie coordinator committing a stale `-coord`
  offset (last writer wins), followed by retention and an `auto.offset.reset=latest` skip. #18039 adds
  producer-epoch fencing for exactly this. The mixed-version window re-opens it only for a 1.12.0
  (old-version) zombie, which the release note above describes.
* Tests: `TestCoordinatorFencing` (integration, added in #18039) asserts that two coordinators built
  from the same connector config share one transactional id and that the second fences the first. No
  test asserts anything about worker-id stability.
* Upstream main since #18039 (`gh api repos/apache/iceberg/commits?path=...`): no later change to
  Worker.java, IcebergSinkConnector.java, KafkaClientFactory.java or the coordinator id.

## Step 3 — Known status / precedent

Searches run on 2026-11-05 with `gh api search/issues` over apache/iceberg (open and closed issues
and PRs, including merged and closed PRs): "transactional.id" kafka connect, transactional suffix,
zombie worker, worker fencing, "transactional id" worker, kafka connect fencing, rolling upgrade
coordinator, taskConfigs UUID, KIP-447, ProducerFencedException, worker producer epoch, "txn" suffix,
sendOffsetsToTransaction, consumer group generation. Also searched databricks/iceberg-kafka-connect
(the tabular-io repo is not searchable).

* **Coordinator mixed-version half: already reported.** The same mechanism at the same site
  (`IcebergSinkConfig.coordinatorTransactionalId`, different transactional-id namespaces during a
  rolling upgrade, so no fencing) was raised in the PR #18039 review (discussion_r3981219594) and
  documented in the PR's release-notes section. There is no code fix; it is an accepted, documented
  rollout caveat.
* **Worker half: no report found.** No issue or PR reports the per-`taskConfigs()` worker suffix (or
  missing worker transactional-id fencing) as a defect. The evidence found is design intent: the docs'
  KIP-447 requirement, the Javadoc contrast, and #17376/#17450 keeping worker ids unchanged.
* Pre-filter: the finding is code-review-sourced but only one of its two mechanisms is already
  reported. The worker site (Worker.java:53 / IcebergSinkConnector.java:52) is a different site with
  no report, so the finding is **not** a pure duplicate. **Not dropped; proceed to Phase 2.**
