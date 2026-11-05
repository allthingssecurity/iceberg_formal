# CR-9 — Phase 1 investigation (evidence only, no verdict)

Finding: a fenced zombie coordinator can fail its task — `Tasks.throwOne` may surface a
`ValidationException` instead of `ProducerFencedException`, and `CoordinatorThread.isFenced()`
inspects only the top-level class of the stored error.

Source: code review (modeling-brief §6.2 T-7 / §6.3 C-3 half, analysis-report F10). No model-checking
counterexample exists for this finding (the model does not distinguish which exception surfaces).

All line numbers below are for the **pristine** tree (`git show HEAD:<path>`, commit 9fffeb6). The
CR-9 worktree carries the uncommitted TLA trace-harness instrumentation (TlaHooks etc.); the pristine
line numbers match the finding's citations exactly.

## Step 1 — code audit

### Cited code (pristine)

* `CoordinatorThread.java:39-64` — `run()`: any `Exception` from `coordinator.start()` /
  `coordinator.process()` is stored in `error`, `terminated = true`, then `coordinator.stop()`.
* `CoordinatorThread.java:78-81` — `isFenced()`:
  `error instanceof ProducerFencedException || error instanceof InvalidProducerEpochException`.
  Only the top-level exception object is inspected (no `getCause()` / `getSuppressed()` walk).
* `CommitterImpl.java:189-212` — `save()` (called from `IcebergSinkTask.put()` for every batch,
  including empty batches) → `processControlEvents()`:
  * `:199-202` terminated + `isFenced()` → `LOG.warn("... was fenced by a newer coordinator; clearing")`,
    `coordinatorThread = null` (task keeps running);
  * `:203-206` otherwise → `throw new NotRunningException("Coordinator unexpectedly terminated on
    committer ...", coordinatorThread.error())`.
* `Coordinator.java:159-201` — `commit(partialCommit)`: for a **full** commit (`partialCommit=false`),
  any `RuntimeException` that is not `org.apache.iceberg.exceptions.CommitFailedException` is
  rethrown (`:176-180`, comment "CommitStateUnknownException, ValidationException, ... not retryable,
  terminate immediately"). For a **partial** (timed-out) commit every exception is swallowed
  (`:166-174`).
* `Coordinator.java:203-213` — `doCommit`: `Tasks.foreach(commitMap.entrySet()).executeWith(exec)
  .stopOnFailure().run(entry -> commitToTable(...))` — one task per table on the
  `iceberg-committer-%d` pool (`commitThreads`, default `2 × availableProcessors`,
  `IcebergSinkConfig.java:227-229`).
* `Coordinator.java:242-359` — `commitToTable`: `catalog.loadTable` → `lastCommittedOffsetsForTable`
  (floor offsets read at load time) → append/rowDelta with
  `validateWith(offsetValidator(tableIdentifier, committedOffsets))` → `commit()` → then
  `send(CommitToTable)` (`:343-349`) on the coordinator's transactional producer.
* `Coordinator.java:361-381` — the `SnapshotAncestryValidator` requires the table's latest stored
  offsets to equal those read at load time; `SnapshotProducer.runValidations`
  (`core/.../SnapshotProducer.java:377-389`) runs it inside `apply()` after `refresh()` on every
  commit attempt and throws `ValidationException("Snapshot ancestry validation failed: Cannot commit
  to <t>, stale offsets: ...")`; `SnapshotProducer.commit` retries only `CommitFailedException`
  (`:491-498`), so the validation failure is final.
* `Channel.java:78-118` — `send()`: `beginTransaction / send / commitTransaction`; on any exception
  tries `abortTransaction()` (logs a WARN if that fails) and rethrows the original.
* `core/src/main/java/org/apache/iceberg/util/Tasks.java:297-403` (`runParallel`) — each item's
  exception is appended to a `ConcurrentLinkedQueue` in the order the items fail; `stopOnFailure`
  only skips items that have not *started* yet. `:396-397` → `throwOne(exceptions, ...)`.
* `Tasks.java:603-617` — `throwOne`: throws the **first** collected exception; every later exception
  of a different class is attached with `addSuppressed` (`ExceptionUtil.castAndThrow` rethrows a
  `RuntimeException` as-is, `api/.../ExceptionUtil.java:30-40`).

### Call chain (entry points → cited code)

1. Kafka Connect `WorkerSinkTask` → `IcebergSinkTask.put(records)` (`IcebergSinkTask.java:84-88`)
   → `CommitterImpl.save` → `processControlEvents` → `isFenced()` / `NotRunningException`.
   An exception escaping `put()` makes `WorkerSinkTask` kill the task ("will not recover until
   manually restarted"); Apache Kafka Connect has no automatic restart of FAILED tasks.
2. `CommitterImpl.open` → `hasLeaderPartition` (Admin `describeConsumerGroups` on the connect group)
   → `startCoordinator` → `new Coordinator(...)` → `Channel` ctor → `KafkaClientFactory.createProducer
   (config.coordinatorTransactionalId())` → `initTransactions()` (`KafkaClientFactory.java:39-47`).
   The id is connector-stable (`IcebergSinkConfig.coordinatorTransactionalId()`), so a newly elected
   coordinator's `initTransactions()` bumps the epoch and fences the previous coordinator's producer.
3. `CoordinatorThread.run` → `Coordinator.process` → `consumeAvailable` → `receive(DATA_COMPLETE)`
   → `isCommitReady(totalPartitionCount)` → `commit(false)` → `doCommit` → parallel `commitToTable`.

### Reachability assessment

Pre-conditions for the claimed outcome (task failure of a superseded coordinator's host):

* (P1) **Zombie coordinator**: a coordinator C1 keeps running after a successor C2 was elected on
  another task. Acknowledged reality in the code base: `Channel.commitConsumerOffsets` javadoc
  (`Channel.java:158-170`), `CoordinatorThread.isFenced` javadoc, the fenced-coordinator tests, and
  upstream #16282/#17340/#17933/#18039. Natural causes: the leader task's consumer is evicted
  (put() slower than `max.poll.interval.ms`, e.g. slow catalog/storage, long GC/VM pause, network
  partition to the group coordinator) while its coordinator thread runs on; the eviction is only
  noticed at the task's next `poll()`.
* (P2) C1 performs a **full** commit (partial commits swallow all errors) of a round whose
  StartCommit was committed before C2's `initTransactions()` (a later StartCommit would be fenced
  and terminate C1 with `ProducerFencedException`, which is cleared correctly).
* (P3) C2 commits a table T1 between C1's `loadTable(T1)` and C1's apply/CAS of T1 → C1's T1 commit
  ends in `ValidationException` (stale offsets).
  Constraint found in the audit: both coordinators share the `-coord` consumer group
  (`Coordinator.java:97-98`, `connectGroupId + "-coord"`), and C1's full commit runs synchronously on
  its poll thread, so with a 1-partition control topic C2 can only receive control events while C1's
  commit is in flight after C1's `-coord` consumer exceeded `max.poll.interval.ms` and left the group.
  Hence P3 needs a long C1 commit (slow catalog / pause) — timing-dependent, but a production
  condition, not a code-level impossibility.
* (P4) Multi-table variant (the finding's scenario): another table T2 of the same C1 round commits
  successfully and its `send(CommitToTable)` throws `ProducerFencedException`; whichever of the two
  failures is queued first is what `throwOne` throws. Single-table variant: the
  `ValidationException` is the only exception — same consequence without the ordering question.
* (P5) The host task calls `put()` and reaches `processControlEvents()` **before** a rebalance
  callback (`close()` → `stopCoordinator()`, which nulls `coordinatorThread`) runs — e.g. the
  stalled `put()` itself continues to `processControlEvents()` at its end, or the host task is
  still a member (cooperative revoke where `close(revoked)` did not stop the coordinator).

Safeguards encountered (recorded, not dismissed):

* `CommitterImpl.close()` → `hasLeaderPartition(closedPartitions)` → `stopCoordinator()` nulls the
  thread before `processControlEvents` can see it — masks the outcome if the rebalance callback runs
  first (P5).
* Partial commits swallow every exception (`Coordinator.java:166-174`) — the outcome needs a full
  commit (P2).
* A fenced StartCommit / CommitComplete / offset commit throws `ProducerFencedException` directly
  from `process()` (top-level → cleared) — the outcome needs the failure to surface through
  `Tasks.throwOne` or as a non-fencing error.

### Trigger scenario (concrete)

Connector with 2 tasks (A = leader hosting C1, B), `iceberg.tables=db.t1,db.t2` (every record goes
to both tables), 1-partition control topic, 2-partition source topic.

1. C1 sends StartCommit(X); both workers answer (DataWritten(X,t1), DataWritten(X,t2),
   DataComplete(X)); C1 receives them and starts a full commit of t1 and t2 in parallel.
2. The catalog is slow for the leader's process: C1's t1/t2 commit requests and task A's next
   `loadTable` on the put path hang.
3. Task A's `put()` exceeds `max.poll.interval.ms` → evicted; task B gets p0 → elected → C2
   (`initTransactions()` fences C1). C1's `-coord` consumer also exceeds its poll interval → C2 gets
   the control partition, replays round X from the `-coord` checkpoint, runs round Y, full commit:
   t1 lands.
4. C1's t1 CAS (now conflicting) is retried → refresh → stale-offset `ValidationException`; then C1's
   t2 commit lands and its `CommitToTable` send fails with `ProducerFencedException`.
   `throwOne` → `ValidationException` (PFE only suppressed) → C1 terminates with a non-fenced error.
5. Task A's stalled `put()` resumes → `processControlEvents()` → `isFenced()==false` →
   `NotRunningException` → Connect kills task A (FAILED, manual restart needed). With the opposite
   failure order (t2's PFE first) the same situation is cleared and task A survives.

## Step 2 — developer-knowledge search (verbatim evidence)

* **PR #18039** (merged 2026-10-02, introduced the stable coordinator transactional id and
  `isFenced()` / the clearing branch): "A stale coordinator will identify that it has been fenced,
  and exit the coordinator thread. We leave the worker task running and defer to Kafka Connect on
  determining its lifecycle." https://github.com/apache/iceberg/pull/18039
* #18039 review (laskoviymishka, on `Channel.java`): "A stale coordinator can still land a stale
  snapshot in the window between the new coordinator's `initTransactions()` and its own fenced
  offset commit; that's caught at the Iceberg level, not by epoch fencing." — the validator is the
  acknowledged zombie guard; the consequence of that guard firing (task failure) is not discussed.
* #18039 review (on `TestCommitterImpl`): asked for an end-to-end test that a real fenced
  Coordinator "asserts `isFenced()` then that the committer clears without throwing" → added
  `TestCoordinator.testFencedCoordinatorThreadIsClearedByCommitterWithoutFailingTask`
  (`TestCoordinator.java:360-391`, assertion message "committer must clear a fenced coordinator
  instead of failing the task").
* Closed, unmerged PRs **#17376** and **#17450** (Sep 2026) proposed "**Fenced ≠ fatal.** A
  coordinator that terminates because it was fenced (`ProducerFencedException` /
  `InvalidProducerEpochException` / `UnknownProducerIdException`, matched across the cause chain up
  to a bounded depth) is cleared without failing the task (Connect does not auto-restart FAILED
  tasks)". The merged design (#18039) checks only the top-level class. Neither PR mentions the
  `ValidationException` path or `Tasks.throwOne`; a cause-chain walk would not see a *suppressed*
  PFE either.
* Code comments: `CoordinatorThread.java:74-77` javadoc ("Whether the coordinator terminated because
  a newer coordinator reused its transactional.id and bumped the producer epoch, fencing this one");
  `CommitterImpl.java:201` log text; `Coordinator.java:177-178` comment listing ValidationException
  among "not retryable, terminate immediately"; `Channel.java:158-170` javadoc on the validator as
  the stale-coordinator guard.
* Tests: `TestCommitterImpl.testFencedCoordinatorIsClearedWithoutFailingTask` (PFE/IPEE top-level →
  cleared), `testCommitFailurePropagatesAsNotRunningException` (other error → NotRunningException),
  `TestCoordinator.testCoordinatorCommittedOffsetValidation` (stale offsets →
  `ValidationException`). No test covers a superseded coordinator whose error is a
  `ValidationException`, nor a multi-table commit with mixed failures.
* Pipeline harness note (instrumented, smoke/recipe source only): `harness/INSTRUMENTATION.md`
  scenario `zombie_validation` describes the single-table path ("the zombie's CAS fails validation
  (CoordCasValidationFail), its host fails (CoordinatorNotRunning)") — reached with source-level
  stalls in the instrumented tree.

## Step 3 — known-status / precedent

Searched (gh CLI, 2026-11-05): PRs and issues in apache/iceberg for "isFenced", "fenced by a newer
coordinator", "NotRunningException", "zombie coordinator", "throwOne", "stale coordinator",
"coordinator fencing", "InvalidProducerEpochException", "Snapshot ancestry validation failed",
"suppressed ProducerFencedException", "stale offsets ValidationException coordinator"; all Kafka
Connect PRs updated since 2026-10-01 (incl. open #18322, #18687, #18767, #17910, #18012, #18006,
#18290, #17593, #17080 and closed #16843, #17925) were checked for diffs touching
`CoordinatorThread.java` / `isFenced` / `throwOne`: only #18322 touches the file (adds an unrelated
`assignmentChanged()` next to `isFenced()`), #16843 (closed) changes `terminate()` only.
`specula findings lookup` (isFenced, throwOne, ProducerFencedException, NotRunningException,
processControlEvents) → no record.

Result: no issue / PR / CVE / Specula record reports this mechanism (non-fenced error surfaced for a
superseded coordinator — via `throwOne` ordering or a sole `ValidationException` — making its host
task fail). #17376/#17450 are closed alternative designs, not reports of a defect at the current
site. → **No pre-filter drop; proceed to Phase 2.** Novelty: NEW.
