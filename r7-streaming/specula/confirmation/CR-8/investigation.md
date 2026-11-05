# CR-8 — Phase 1 investigation record

Finding: "A long table commit makes the -coord checkpoint fail with Kafka's CommitFailedException,
which the retry logic mistakes for a non-retryable error (task FAILED)". Source: code review
(Scenario 4 residual; no model-checking counterexample exists for this mechanism).

Status: **DROPPED (code-review × known, cite: https://github.com/apache/iceberg/pull/17376; also
https://github.com/apache/iceberg/pull/17450)**. Fix status: **unfixed**. No `repro/` test was written
(guide.md pre-filter rule).

## 0. Persistent-findings lookup

`specula findings --work <.specula-output> lookup --query CommitFailedException --query
sendOffsetsToTransaction --query max.poll.interval.ms --query Coordinator.commit` returned `[]`
(no prior records; `persistent-findings-context.json` has `previous: null`).

## 1. Code audit (pristine `git show HEAD:` — the worktree carries TLA+ trace instrumentation)

Checkout: `9fffeb6` (2026-11-02). Upstream `main` has no later commits to `Coordinator.java`,
`Channel.java`, `CoordinatorThread.java` or `CommitterImpl.java` (last change: #18039, 2026-10-02).

Cited sites (line numbers verified against HEAD):
- `channel/Coordinator.java:57`: `import org.apache.iceberg.exceptions.CommitFailedException;`
- `channel/Coordinator.java:176`: `if (!(e instanceof CommitFailedException)) { ... throw e; }`. Only
  Iceberg's class is treated as retryable; every other RuntimeException is fatal on a full commit.
- `channel/Coordinator.java:216`: `commitConsumerOffsets();` runs after all `commitToTable` calls succeeded.
- `channel/Channel.java:199`: `producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());`
  `Channel.java:201-206` aborts if the producer is not fenced, then rethrows.

Call chain (coordinator thread):
`CoordinatorThread.run` (CoordinatorThread.java:49-56) → `Coordinator.process` (Coordinator.java:126)
→ `consumeAvailable` (Channel.java:122) → `receive(DATA_COMPLETE)` (Coordinator.java:149-152) →
`commit(false)` → `doCommit` → `Tasks.foreach(...).run(commitToTable)` (Coordinator.java:207-213, can be slow:
`catalog.loadTable`, ancestry walk, `appendOp.commit()` with Iceberg's own retries) →
`commitConsumerOffsets()` (Coordinator.java:216) → `sendOffsetsToTransaction(..., consumer.groupMetadata())`
(Channel.java:199).
The control consumer is not polled during this sequence. The commit runs inside the record loop of
`consumeAvailable`, before the next `consumer.poll()` (Channel.java:150).

Failure propagation (full commit): exception caught at Coordinator.java:165 → `partialCommit == false` →
line 176 `instanceof` (Iceberg class) false for `org.apache.kafka.clients.consumer.CommitFailedException`
→ `throw e` (line 179) → `CoordinatorThread.run` stores `error`, `terminated = true` (CoordinatorThread.java:52-55)
→ `isFenced()` is false (only ProducerFenced/InvalidProducerEpoch, CoordinatorThread.java:78-81) →
`CommitterImpl.processControlEvents` throws `NotRunningException` (CommitterImpl.java:199-207; a plain
RuntimeException) from `IcebergSinkTask.put` (IcebergSinkTask.java:86-90). Connect's WorkerSinkTask treats a
non-RetriableException from put() as unrecoverable, so the task is FAILED until someone restarts it.
Connect calls put() even with empty batches, so this happens without new data.

Partial-commit path (`commit(true)` on timeout, Coordinator.java:138-139, 166-173): the same exception
is logged as "Partial commit ... will retry" and swallowed. `clearResponses()` was skipped, so the next
cycle re-offers the same envelopes and the per-table offset floors filter them
(Coordinator.java:270-285). This path recovers on its own; the defect is on the full-commit path only.

Kafka-side semantics (verified from apache/kafka tag 3.9.2 sources; Iceberg pins `kafka = "3.9.2"`):
- `AbstractCoordinator.java:1147-1155` `handlePollTimeoutExpiry()` → `maybeLeaveGroup("consumer poll timeout has expired.")`
  (heartbeat thread, AbstractCoordinator.java:1519-1522). LeaveGroup is sent and the generation is reset.
- `ConsumerCoordinator.java:129/202/351/969-970`: `groupMetadata` is assigned only in the constructor and in
  `onJoinComplete`, so after a poll-timeout leave `consumer.groupMetadata()` still returns the old
  generation/memberId.
- Broker `GroupCoordinator.scala:1019-1045` / `ClassicGroup.java:825-848`: a txn offset commit with
  generation ≥ 0 or a non-empty memberId is checked against current members. An unknown member gets
  UNKNOWN_MEMBER_ID, and a wrong generation gets ILLEGAL_GENERATION.
- `TransactionManager.java:1697-1701`: UNKNOWN_MEMBER_ID / ILLEGAL_GENERATION →
  `abortableError(new CommitFailedException("Transaction offset Commit failed due to consumer group metadata mismatch: ..."))`
  (`org.apache.kafka.clients.consumer.CommitFailedException extends KafkaException`).
  `TransactionalRequestResult.await` rethrows it unwrapped.

Reachability: real but needs a slow environment. The time between control-consumer polls must exceed
the coordinator consumer's `max.poll.interval.ms` (default 300000; settable via `iceberg.kafka.max.poll.interval.ms`).
Contributors include a slow REST, Glue or Hive catalog (Hive lock waits), Iceberg commit retries
(`commit.retry.*`, total timeout up to 30 min by default), slow object storage while writing manifests,
or many tables with fewer `commitThreads` than tables.

Safeguards / masks encountered (recorded, not dismissed):
- Default `iceberg.control.commit.max-consecutive-failures = 1` (IcebergSinkConfig.java:111-112, 250-256).
  With the default, an Iceberg `CommitFailedException` also terminates the coordinator. The name clash
  only changes behavior when operators opt into retries (≥ 2). Under the default config the slow-commit
  checkpoint failure still fails the task, because every full-commit failure is fatal.
- Data safety: the table commit already landed. A restarted coordinator replays from the last committed
  `-coord` offset, and the per-table offset floors (Coordinator.java:270-285) drop the already-committed
  DataWritten envelopes. The consequence is availability (task FAILED, manual restart), not loss or
  duplication. This assumes the floors are intact (MC-3 is out of scope here).
- Mitigation knob: raising `iceberg.kafka.max.poll.interval.ms`.

Trigger scenario: (1) the connector runs with one coordinator task. (2) A full commit starts after all
DataComplete events arrive. (3) Table commit(s) take longer than `max.poll.interval.ms`. (4) The
coordinator consumer's heartbeat thread leaves `connect-<name>-coord`. (5) The table commits succeed.
(6) `commitConsumerOffsets` → `sendOffsetsToTransaction` with stale group metadata → broker UNKNOWN_MEMBER_ID
→ Kafka `CommitFailedException`. (7) `Coordinator.commit` rethrows it (line 176). (8) The coordinator
thread dies. (9) The next `put()` throws `NotRunningException`. (10) The task is FAILED until a manual restart.

## 2. Developer-knowledge search (verbatim evidence)

- PR #16434 (merged 2026-07-16, "Add bounded retry for transient commit exceptions", fixes #16393):
  "Only `CommitFailedException` is retried -- all other exceptions (CommitStateUnknownException,
  ValidationException, ForbiddenException) are fatal immediately" and "Default is 1 -- preserves existing
  behavior out-of-the-box; users opt-in to retries". The motivation is "transient `CommitFailedException`
  errors (e.g., catalog contention from concurrent commits)". The Kafka-side checkpoint failure was not discussed.
- PR #16237 (merged 2026-05-18): full-commit failures now terminate the coordinator so they "surface to
  operators ... which transitions the Connect task to FAILED".
- **PR #17376** (kumarpritam863, opened 2026-07-26, closed unmerged by the stale bot 2026-09-15, "not a
  judgement on the merit"): "**Transient Kafka commit errors are retryable.** A consumer-offset commit
  hiccup from `commitConsumerOffsets()` (Iceberg `CommitFailedException`, Kafka `CommitFailedException`,
  `RebalanceInProgressException`, `RetriableException` — e.g. the `-coord` consumer briefly evicted when a
  catalog commit exceeds `max.poll.interval.ms`) no longer fails the task: the table commit already
  succeeded and the watermark re-advances on the next cycle."
  Its diff replaces exactly `if (!(e instanceof CommitFailedException))` in `Coordinator.commit` with
  `isRetryable(e)`, which adds `org.apache.kafka.clients.consumer.CommitFailedException`. Its test comment:
  "Kafka consumer-commit failures from commitConsumerOffsets() -> retry (must NOT be confused with the
  Iceberg CommitFailedException, which was the original bug)".
  Reviewer laskoviymishka (2026-08-07, Coordinator.java:200): "these are thrown from
  `commitConsumerOffsets()` *after* every `commitToTable()` has already succeeded ... With
  `max-consecutive-failures = 1`, a single `RebalanceInProgressException` terminates the coordinator even
  though every table committed fine".
- **PR #17450** (same author, opened 2026-07-31, closed unmerged by the stale bot 2026-09-21) repeats the
  item. Reviewer laskoviymishka: "Iceberg's `CommitFailedException`, the Kafka consumer
  `CommitFailedException`, and `RebalanceInProgressException` are all safe to retry". The reviewer
  APPROVED it on 2026-08-13 and asked AnatolyPopov to review before merge. It was never merged.
- PR #16366 (closed by the stale bot 2026-06-24): Kafka `CommitFailedException` from `Channel.send()`
  (worker path, source-group rebalance) is "transient, expected ... should be recoverable, not fatal".
  This is the same shape at a different site, so it does not count toward known-status.
- Tests: `TestCoordinator` covers only Iceberg `CommitFailedException` from `appendOp.commit()`
  (testCommitFailedExceptionPropagates / testCommitBoundedRetry / ...). No test covers a Kafka-side
  failure from `commitConsumerOffsets()`.
- Docs (`docs/docs/kafka-connect.md:90`): "`iceberg.control.commit.max-consecutive-failures` | Maximum
  number of consecutive commit failures before the coordinator terminates, default is `1`".

Searches run (`gh search issues --include-prs` / `gh search prs`, repo apache/iceberg): "CommitFailedException",
"max.poll.interval", "max-consecutive-failures", "consecutive commit failures", "coordinator terminates",
"commitConsumerOffsets", "sendOffsetsToTransaction", "isRetryable", "Kafka CommitFailedException",
"coordinator consumer evicted", "coord consumer poll", plus the PR author's later PRs (Aug–Nov 2026: Flink
only). No open or merged fix was found. `main` still has the Iceberg-only check.

## 3. Known-status / pre-filter

PR #17376 (and #17450) already reported this exact defect, at the same site and through the same mechanism:
- Trigger: the `-coord` consumer is evicted when a catalog commit exceeds `max.poll.interval.ms`.
- Error: Kafka `CommitFailedException` from `commitConsumerOffsets()`.
- Misclassification: Kafka's `CommitFailedException` is confused with Iceberg's in `Coordinator.commit`'s retry check.
- Consequence: the task fails although the table commit succeeded.
- Fix location: the same check at Coordinator.java:176.

The PRs were closed by the stale bot and never merged, so the defect is **unfixed**. Open-vs-fixed does
not matter for the pre-filter.

Code-review-sourced × already reported → **DROPPED (code-review × known, cite:
https://github.com/apache/iceberg/pull/17376; also https://github.com/apache/iceberg/pull/17450)**.
No Phase 2 reproduction and no `repro/` test, per guide.md.
No `issue.json` was written: the persistent registry stores only REPRODUCED / ENV_LIMITED / MASKED findings.
